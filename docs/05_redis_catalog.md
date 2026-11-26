# Redis catalog

`RedisCatalog` stores current table pointers, root versions, table settings, RBAC records, staging and pipe definitions, sharing records, and engine configuration. Its data lives in ordinary Redis strings, hashes, and sets. Parquet data and historical snapshot JSON live in the selected storage backend.

Sources: [RedisCatalog](../supertable/redis_catalog.py), [RedisConnector](../supertable/redis_connector.py), [key builders](../supertable/redis_keys.py). Read [Redis layout](16_redis_layout.md) for exact keys and [locking](08_locking.md) for concurrency behavior.

## Connection setup

```python
from supertable.redis_catalog import RedisCatalog

catalog = RedisCatalog()
if not catalog.ping():
    raise ConnectionError("Redis is unavailable")
root = catalog.get_root("acme", "sales")
leaf = catalog.get_leaf("acme", "sales", "orders")
```

The main connector reads host, port, database, password, SSL, and Sentinel settings from the imported settings object. It shares clients through a process-local configuration-keyed cache. `close_all_redis_clients()` clears the cache, disconnects pools, and closes clients. Responses are always decoded to text.

`RedisOptions` derives connection attributes in `__post_init__`; its host and similar fields are not constructor override arguments. It forces `decode_responses=True` and `sentinel_strict=True`. The main connector does not consume the URL or username settings. See [configuration](02_configuration.md) for the distinction between declared settings and connector behavior.

With Sentinel enabled and at least one parsed `host:port`, the connector discovers the configured master and retries `PING` for roughly three seconds. Discovery failure raises under the forced strict mode. Enabling Sentinel without any parsed hosts falls back to the direct connection. The Sentinel construction branch does not pass the direct connection's SSL flag.

## Root and leaf records

A root is a JSON string at `meta:root` within a lake namespace:

```json
{"version": 0, "ts": 1770000000000}
```

`ensure_root()` creates that record if an existence check finds no key. Additional root flags can include `read_only`, `clone_type`, `cloned_from`, and `replica_tables`. `update_root_flags()` reads the document, merges supplied fields, and writes it back. `bump_root()` uses Lua to increment `version` and replace `ts` while retaining existing fields.

A leaf at `meta:leaf:doc:<simple>` identifies the current simple-table snapshot:

```json
{
  "version": 4,
  "ts": 1770000000000,
  "path": "acme/sales/tables/orders/snapshots/tables_example.json",
  "payload": {
    "simple_name": "orders",
    "snapshot_version": 4,
    "previous_snapshot": "acme/sales/tables/orders/snapshots/tables_previous.json",
    "resources": []
  }
}
```

The example is a reduced record. The payload produced by the table writer also includes schema, tombstone and statistics references, timestamps, location, and optional lineage. `version` is the Redis leaf update counter; `snapshot_version` is maintained in the JSON snapshot. They are separate fields, not one shared counter.

The record is stored on one line, in the order `version`, `ts`, `path`, `payload`; the example is laid out for reading. The payload is the snapshot document value for value: it is stored as the JSON text the writer serialised, and the script that stores it does not decode it. A leaf written by a release up to 3.5.7 went through Redis's `cjson` on the way in, which changed it: an empty list was stored as `{}`, an integer of 15 digits or more as a rounded float, a float without its digits past the fourteenth, and keys in another order. Readers parse both forms.

`get_leaf()` returns the decoded record, or `None` when there is no leaf; a Redis error raises. `SimpleTable.get_simple_table_snapshot()` first uses an inline payload with a list-valued `resources` field, including an accepted nested `payload.snapshot` shape. Otherwise it reads `path` from storage. The pointer must still contain a path even when the payload is inline.

## Publication and atomic operations

The methods named `set_leaf_path_cas()` and `set_leaf_payload_cas()` atomically increment and replace a leaf using Lua. They do not accept an expected version, compare against a caller's previous value, or verify a lock token. For a missing leaf, their initial result is version `0`; for an existing valid leaf, it is the old version plus one.

`commit_leaf_and_bump_root()` does verify a lock token, and is what the writer publishes with. One Lua script compares the token with the table's lock key, replaces the leaf and bumps the root, so no other writer can take the lock and publish between the check and the commit. A token that does not hold the lock raises `LockLostError` and nothing is written. The script is built from the two scripts of `set_leaf_payload_cas()` and `bump_root()`, used as they are: the leaf and the root it stores are the ones they store, and like them it compares no expected version.

`set_leaf_payload_cas()` reads the old version from the first bytes of the leaf it replaces when that leaf begins `{"version":N` and ends as a JSON object does, which every leaf it writes does; it then neither fetches nor decodes the old leaf. Redis is busy with a commit for about a quarter of the time it was: it takes the new snapshot's text and stores it, where it used to decode the old leaf, decode the new payload and encode both again (about 2 ms instead of 9 ms for a table of 1,000 files, measured with `SLOWLOG`). Any other value in the key (a leaf written by an earlier release or by `set_leaf_path_cas()`, one written by hand, one that is not JSON) is fetched and decoded as before, with the same result. What differs from the script it replaces is confined to values written by hand that begin with their version and end with `}`: the version is taken from the head, whatever stands between. The old script decoded the whole value, so it started again from `0` when the rest was not valid JSON or nested deeper than `cjson` reads, and went on from the later value when `version` was named twice.

The script calls `GETRANGE` as well as `GET` and `SET`. A Redis user restricted to named commands needs it allowed (it belongs to the `@read` and `@string` categories); without it the script fails, and the writer falls back to a path-only leaf on every commit.

| Operation | Atomic portion | Separate work |
| --- | --- | --- |
| `set_leaf_path_cas()` | Read version and replace `{version, ts, path}` in one Lua script | Does not retain an existing inline payload |
| `set_leaf_payload_cas()` | Read version and replace `{version, ts, path, payload}` in one Lua script | Payload JSON is encoded before the script and stored as that text; serialization failure supplies `{}`; a payload holding a number that is not finite (NaN, Infinity) raises `redis.ResponseError` and stores nothing |
| `bump_root()` | Read, increment, preserve flags, and replace root in one Lua script | Independent of leaf publication |
| `commit_leaf_and_bump_root()` | Compare the lock token, replace `{version, ts, path, payload}` and bump the root in one Lua script over the leaf, root and lock keys; a token that does not hold the lock writes nothing | A loss of the lock that the heartbeat recorded is asked for first, without a Redis call; a payload holding a number that is not finite raises `redis.ResponseError` before anything is sent |
| `store_schema_and_read_mirrors()` | None: a `SET` of the table's schema document, a `SADD` of its name to the lake's table names and a `GET` of the lake's mirror formats, sent in one round trip without `MULTI` | Returns the enabled formats as `get_mirrors()` does; a refused `SET` or `SADD` raises after all three were sent |
| `reserve_rowids(count)` | Redis `INCRBY` reserves a contiguous range | No rollback when subsequent file creation fails |
| `ensure_root()` | Individual `EXISTS` and `SET` commands | The check and creation are separate operations |
| `update_root_flags()` | Individual read and write commands | Concurrent updates can overwrite one another |

For positive `count`, `reserve_rowids()` returns the first ID in the reserved range. An unused sequence starts at `1`; nonpositive counts return `0` without incrementing it.

The writer's publication sequence is:

1. Acquire the simple-table lock and load the current snapshot.
2. Write new data, tombstones/statistics as needed, and a new snapshot JSON object.
3. Verify the lock, then publish the leaf with inline payload; if that call raises, attempt path-only publication.
4. Bump the super-table root version.
5. Update schema and table-name acceleration keys, then attempt mirrors.
6. Release the lock and enqueue monitoring outside the lock.

Each Lua invocation is atomic in Redis. The sequence as a whole is not a transaction: storage writes, leaf publication, root bump, acceleration keys, and mirrors are separate actions. A failed root bump can follow a successful leaf publication; files written before failed publication can remain unreferenced. Schema/table-name updates and mirror errors are handled without rolling the leaf back. Table readers should derive the current snapshot from the leaf, rather than infer a transaction from matching counters.

## Snapshot history

Every `SimpleTable.update()` sets `previous_snapshot` to the prior snapshot path, increments `snapshot_version`, and writes a new JSON snapshot. Redis retains the current leaf rather than a history list.

A caller can follow the chain through storage:

```python
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

st = SuperTable("sales", "acme", create_if_missing=False)
table = SimpleTable(st, "orders", create_if_missing=False)
snapshot, path = table.get_simple_table_snapshot()
seen = set()

while path and path not in seen:
    seen.add(path)
    print(snapshot["snapshot_version"], path)
    path = snapshot.get("previous_snapshot")
    if path:
        snapshot = st.read_simple_table_snapshot(path)
```

The snapshot chain is metadata history. Following it can raise if a previous JSON object was removed, and referenced Parquet resources must still exist to read historical data. This catalog does not provide a historical-version selector or a transaction that pins a snapshot chain and its objects.

## Replica reads

When a root has `clone_type="replica"` and a different nonempty `cloned_from`, `get_leaf()`, `leaf_exists()`, and leaf scanning read the source super-table's leaves. A nonempty list in `replica_tables` filters permitted simple names; a missing or empty list means no table-name restriction here. A Redis error reading the root raises from these calls rather than sending them to the replica's own, empty namespace.

Root retrieval remains at the requested namespace. Replica lookup is a single source redirection, not recursive source resolution. `find_readonly_clones()` scans roots in one organization and returns those whose `read_only` flag is truthy and whose `cloned_from` matches the source. These catalog primitives read and write metadata; they do not perform storage copying themselves.

## Enumeration and failure behavior

`scan_leaf_keys()` uses Redis `SCAN` with the lake's leaf pattern. `scan_leaf_items()` batches `GET` calls and yields dictionaries containing `simple`, `version`, `ts`, `path`, and `payload`. Missing or malformed entries in those batches are skipped. `SCAN` is incremental and is not a consistent snapshot of a changing namespace: a table created or dropped while a listing runs may or may not appear in it. Redis may also return a key more than once while the database is being resized; `scan_leaf_keys()` skips a key it has already yielded, so one call lists each leaf once, and `scan_leaf_items()` fetches and yields it once. `find_readonly_clones()` and the package-level lake and table listings, `list_supers()` and `list_tables()`, do the same. `SCAN` also walks every key of the Redis database whatever its pattern, so listing one lake costs time in proportion to everything stored beside it.

`get_leaf_items(org, sup, simples)` returns the same dictionaries for the tables named in `simples`, without `SCAN`: it reads the root for replica resolution and then the named leaf keys in one `MULTI`/`EXEC`. Names are matched exactly and come back in the order given, each once. A name with no leaf is left out, and so is a name `redis_keys` does not accept as a key segment, since no leaf key is built under one. Replica lakes resolve as they do for the scan, including the `replica_tables` filter. Use it when the table names are known; `DataEstimator` does, and lists the lake only for a table named like its SuperTable or when a named table has no snapshot.

Failure handling varies by method:

| Methods | Redis failure behavior |
| --- | --- |
| `root_exists()`, raw leaf existence, `ensure_root()`, leaf publication, `bump_root()`, `update_root_flags()` | Log and raise |
| `get_root()`, `get_leaf()` and raw leaf retrieval, `get_share()`, `get_linked_share()` | Log and raise; `None` means the key is absent. Malformed JSON raises the decoder's error |
| Replica resolution in `get_leaf()`, `leaf_exists()`, `get_leaf_items()`, and leaf scans | Raise; a malformed root reads as no replica |
| `get_leaf_items()` | Log and raise; a named leaf that is absent or malformed is left out of the result |
| `list_shares()`, `list_linked_shares()` | Log and raise, for the index or any one document; an indexed document that is missing or malformed is skipped |
| `commit_leaf_and_bump_root()` | Log and raise, with the leaf and the root as they were: the script reads everything it reads before its first write. `LockLostError` when the token does not hold the lock |
| `get_mirrors()`, and the read inside `store_schema_and_read_mirrors()` | Log and return no formats; an unreadable value is no formats, without a log line |
| `ping()` | Return false |
| Leaf scans and batches | Log and stop/skip the failed operation, potentially yielding partial results |
| Many metadata getters | Return `None`, an empty list, or defaults |
| Several metadata mutation helpers | Return false or a partial deletion count |

A missing result from a getter is therefore not universally proof that the resource is absent. Existence checks deliberately propagate connection errors. Callers should use the method's actual contract when deciding whether creation is appropriate.

## Table, mirror, and engine configuration

`set_table_config()` stores the supplied dictionary with `modified_ms`; `get_table_config()` reads it. The catalog does not validate writer tuning fields here. `DataWriter.configure_table()` validates positive values for its supported thresholds before calling it.

Mirror settings are a JSON object containing `formats` and `ts`. Only `DELTA`, `ICEBERG`, and `PARQUET` are retained, uppercased and deduplicated in input order. Unknown values are ignored. `enable_mirror()` and `disable_mirror()` perform a read followed by a write and can race with each other. See [mirroring](13_mirroring.md).

Engine configuration is organization-scoped. `set_engine_config()` accepts only engine `lite` and these fields: `duckdb_memory_limit`, `duckdb_io_multiplier`, `duckdb_threads`, `duckdb_http_timeout`, and `duckdb_external_cache_size`. It replaces the stored section with the supplied nonempty supported values. Memory strings are normalized. The runtime resolver can read additional shared routing fields and a `pro` section from stored data; the setter does not expose all fields understood by that resolver.

Spark cluster and plug registries are hashes keyed by cluster/plug ID, with JSON configuration values. Selection and routing are described in [query engines](09_query_engine.md).

## RBAC and authentication records

Users and roles use hash documents, set indexes, case-insensitive name-to-ID hashes, and version metadata hashes. The catalog serializes list/dictionary fields as JSON. Creation writes document/index/name entries in a Redis pipeline, then bumps version metadata separately.

Role update uses Lua to update fields, maintain the role-type set, and bump role metadata together. Role deletion uses Lua to remove the role from indexed users, delete the role and its name/type/index entries, and bump role metadata. Adding or removing a user's role also uses Lua to avoid replacing concurrent role-list changes. Higher-level authority checks are described in [RBAC](11_rbac.md).

Organization login tokens are generated with the `st_login_` prefix. Redis stores SHA-256 token IDs as hash fields and JSON metadata as values; the plaintext token is returned at creation. `validate_auth_token()` only tests whether that hash field exists. `validate_auth_token_full()` loads metadata and checks enabled/expiry state. Use the full validation contract where those controls must be enforced. Expiry is checked from `expires_ms`, not an individual Redis field TTL.

## Staging, pipes, and sharing

Staging and pipe upserts store JSON documents and add names to set indexes in a pipeline. They add scope fields if absent and replace `updated_at_ms` on every upsert. Lists primarily use those indexes. Pipe listing has a scan fallback, but its fallback name extraction splits on `:pipe:` while current keys contain `:pipes:doc:`; an absent index can therefore produce full keys rather than pipe names. Maintain the indexes through the provided upsert/delete operations.

Share and linked-share creation similarly write JSON plus index membership through pipelines. These records store supplied metadata; storage exports and refresh behavior are responsibilities of their callers.

Two Lua scripts back a provider's revocation and manifests. `delete_share_if_unchanged(org, share_id, expected_document)` removes a share's document and index entry together, and only while the stored bytes equal `expected_document` encoded the way `create_share` writes it, so a share replaced after the caller read it survives; pass the dictionary `get_share` returned. It returns false for an absent share and raises `RuntimeError` when the document changed or the document and index disagree. `allocate_share_manifest_generation(org, share_id, incarnation)` returns the next manifest generation for one share token, whose 64-hex hash is `incarnation`: Redis `TIME` in milliseconds, or one past the previous value when that is not below it, so generations strictly increase across processes and within a millisecond. The last value stays in its own key without a TTL; neither the share index nor either delete touches it.

## Deletion scope

`delete_leaf()` removes only the leaf key. `delete_simple_table()` removes the leaf and its lock; it does not remove the row-ID sequence, schema key, table-name set membership, table settings, or quality records. It returns true if its Redis delete command executes, even when no keys existed.

`delete_super_table()` scans and deletes the entire `supertable:<org>:lakes:<sup>:*` namespace in batches, keeping `meta:root` and the namespace deletion intent for one final `DEL` of both, and returns a count. It does not remove organization system keys, query jobs, monitoring partitions, or storage objects. Redis errors raise. The higher-level `SuperTable.delete()` first sets the deletion intent with `set_namespace_deletion_intent()`, a JSON string with `intent_id` and `started_at_ms`, then removes storage, then invokes it. A delete that stops part way therefore leaves the lake openable, through its root, and marked as being deleted, and running it again finishes it. dataisland-core reads the intent; the SDK's own readers and writers do not. No catalog deletion helper coordinates with active writers through a namespace-wide lock, and of two deletes of one lake running at once, the first to finish removes the intent the other still relies on.
