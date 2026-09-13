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

`get_leaf()` returns the decoded record or `None`. `SimpleTable.get_simple_table_snapshot()` first uses an inline payload with a list-valued `resources` field, including an accepted nested `payload.snapshot` shape. Otherwise it reads `path` from storage. The pointer must still contain a path even when the payload is inline.

## Publication and atomic operations

The methods named `set_leaf_path_cas()` and `set_leaf_payload_cas()` atomically increment and replace a leaf using Lua. They do not accept an expected version, compare against a caller's previous value, or verify a lock token. For a missing leaf, their initial result is version `0`; for an existing valid leaf, it is the old version plus one.

| Operation | Atomic portion | Separate work |
| --- | --- | --- |
| `set_leaf_path_cas()` | Read version and replace `{version, ts, path}` in one Lua script | Does not retain an existing inline payload |
| `set_leaf_payload_cas()` | Read version and replace `{version, ts, path, payload}` in one Lua script | Payload JSON is encoded before the script; serialization failure supplies `{}` |
| `bump_root()` | Read, increment, preserve flags, and replace root in one Lua script | Independent of leaf publication |
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

When a root has `clone_type="replica"` and a different nonempty `cloned_from`, `get_leaf()`, `leaf_exists()`, and leaf scanning read the source super-table's leaves. A nonempty list in `replica_tables` filters permitted simple names; a missing or empty list means no table-name restriction here.

Root retrieval remains at the requested namespace. Replica lookup is a single source redirection, not recursive source resolution. `find_readonly_clones()` scans roots in one organization and returns those whose `read_only` flag is truthy and whose `cloned_from` matches the source. These catalog primitives read and write metadata; they do not perform storage copying themselves.

## Enumeration and failure behavior

`scan_leaf_keys()` uses Redis `SCAN` with the lake's leaf pattern. `scan_leaf_items()` batches `GET` calls and yields dictionaries containing `simple`, `version`, `ts`, `path`, and `payload`. Missing or malformed entries in those batches are skipped. `SCAN` is incremental and is not a consistent snapshot of a changing namespace.

Failure handling varies by method:

| Methods | Redis failure behavior |
| --- | --- |
| `root_exists()`, raw leaf existence, `ensure_root()`, leaf publication, `bump_root()` | Log and raise |
| `ping()` | Return false |
| `get_root()`, raw leaf retrieval | Return `None` on Redis errors; malformed JSON is not covered by that Redis-only catch |
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

## Deletion scope

`delete_leaf()` removes only the leaf key. `delete_simple_table()` removes the leaf and its lock; it does not remove the row-ID sequence, schema key, table-name set membership, table settings, or quality records. It returns true if its Redis delete command executes, even when no keys existed.

`delete_super_table()` scans and deletes the entire `supertable:<org>:lakes:<sup>:*` namespace in batches, returning a count. It does not remove organization system keys, query jobs, monitoring partitions, or storage objects. Redis errors can produce a partial count. The higher-level `SuperTable.delete()` removes storage before invoking it. No catalog deletion helper coordinates with active writers through a namespace-wide lock.
