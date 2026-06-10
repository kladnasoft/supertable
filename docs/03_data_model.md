# 03 -- Data Model

## Business Context

SuperTable is a multi-tenant analytical data platform that organizes data into a three-level hierarchy: **Organization > SuperTable > SimpleTable**. This structure enables enterprises to isolate data by business unit (organization), group related datasets under a logical umbrella (SuperTable), and manage individual table lifecycles independently (SimpleTable). Every write to a SimpleTable produces an immutable, versioned snapshot -- giving teams point-in-time reproducibility, safe rollbacks, and concurrent read/write access without locks on the read path.

The metadata layer lives entirely in Redis for sub-millisecond lookups, while the heavy payload (Parquet data files and snapshot JSON) is persisted on pluggable object storage (local disk, S3, MinIO, Azure Blob, or GCS). This separation means the system can serve metadata-intensive operations (schema introspection, table listing, RBAC checks) at Redis speed, and defer to object storage only when actual data must be read.

---

## Three-Level Hierarchy

```
Organization (tenant boundary)
 +-- SuperTable  (logical dataset group)
      +-- SimpleTable  (individual versioned table)
      +-- SimpleTable
      +-- ...
 +-- SuperTable
      +-- ...
```

### Organization

The top-level isolation boundary. Every Redis key and every storage path is prefixed with the organization identifier, ensuring complete tenant separation. Organizations are implicit -- they are established the first time a SuperTable is created under a given org name.

### SuperTable

A logical grouping of related SimpleTables. Represented by the `SuperTable` class in `supertable/super_table.py`.

```python
class SuperTable:
    def __init__(self, super_name: str, organization: str)
```

Key responsibilities:

| Concern | Detail |
|---------|--------|
| **Storage bootstrap** | Creates the base directory `{organization}/{super_name}/super` on first use. |
| **Redis root pointer** | Ensures a `meta:root` key exists via `RedisCatalog.ensure_root()`. |
| **RBAC scaffolding** | Initializes `RoleManager` and `UserManager` on first creation. |
| **Snapshot reading** | `read_simple_table_snapshot(path)` reads the heavy JSON from storage. |
| **Deletion** | `delete(role_name)` removes both storage data and all Redis keys under the supertable prefix. |

The `identity` field is always `"super"`, used in path construction:

```
{organization}/{super_name}/super/
```

**Fast-path optimization**: If `meta:root` already exists in Redis, the constructor skips storage directory creation entirely, avoiding unnecessary I/O on repeated instantiation.

### SimpleTable

An individual versioned table within a SuperTable. Represented by the `SimpleTable` class in `supertable/simple_table.py`.

```python
class SimpleTable:
    def __init__(self, super_table: SuperTable, simple_name: str)
```

Key responsibilities:

| Concern | Detail |
|---------|--------|
| **Directory layout** | Creates `data/` and `snapshots/` subdirectories under `{org}/{super}/{tables}/{simple_name}/`. |
| **Initial snapshot** | Bootstraps version 0 with an empty resource list and writes it to storage + Redis leaf. |
| **Snapshot reading** | `get_simple_table_snapshot()` resolves the Redis leaf pointer and returns `(snapshot_dict, path)`. |
| **Snapshot writing** | `update(new_resources, sunset_files, model_df, ...)` builds a new snapshot, increments the version, and writes it to storage. |
| **Schema tracking** | Each snapshot stores a schema list (Delta-compatible `{name, type, nullable, metadata}` format) plus a `schemaString` in Spark StructType JSON. |
| **Data lineage** | Optional `lineage` dict is stored in the snapshot JSON for provenance tracking. |
| **Deletion** | `delete(role_name)` checks write access via RBAC, removes the storage folder, and deletes Redis meta. |

The `identity` field is `"tables"`, producing paths like:

```
{organization}/{super_name}/tables/{simple_name}/
  +-- data/           # Parquet data files
  +-- snapshots/      # Versioned snapshot JSON files
```

---

## Versioned Snapshots

Every write produces a new immutable snapshot file. Snapshots are never overwritten -- they form a linked list via the `previous_snapshot` field.

### Snapshot JSON Structure

```json
{
  "simple_name": "orders",
  "location": "acme/warehouse/tables/orders",
  "snapshot_version": 5,
  "last_updated_ms": 1713200000000,
  "previous_snapshot": "acme/warehouse/tables/orders/snapshots/tables_20240414_v4.json",
  "schema": [
    {"name": "order_id", "type": "long", "nullable": true, "metadata": {}},
    {"name": "amount",   "type": "double", "nullable": true, "metadata": {}}
  ],
  "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
  "resources": [
    {"file": "data/part-00001.parquet", "rows": 50000, "bytes": 1048576}
  ],
  "lineage": {
    "source": "s3://raw-bucket/orders/",
    "pipeline": "daily-etl"
  }
}
```

### Version Progression

1. **Version 0** -- Created by `SimpleTable.init_simple_table()`. Empty `schema` and `resources`.
2. **Version N+1** -- Created by `SimpleTable.update()`. Merges new resources, removes sunset files, updates schema from the model DataFrame.

### Schema Field Semantics — "Last Write Wins" (Intentional)

The snapshot's `schema` field follows a **"writer is the schema authority"**
contract. Each call to `SimpleTable.update` **unconditionally overwrites** the
new snapshot's `schema` field with the schema of the `model_df` passed in.
It does **not** union with the prior snapshot's schema, and it does **not**
preserve columns that aren't in the incoming `model_df`. This is deliberate
— it just hadn't been documented anywhere outside the code, so reproducing
it correctly without reading the source was effectively impossible.

#### Where this lives in code

`supertable/simple_table.py`, inside `SimpleTable.update` (≈ lines 290–299):

```python
schema_list = collect_schema(model_df)
if not schema_list:
    # Fallback: derive schema from Polars dtypes if helper returns empty.
    schema_list = _schema_list_from_polars_df(model_df)
last_simple_table["schema"] = schema_list                  # ← overwrite, no union
# Also store a Spark StructType JSON for downstream Delta mirrors.
try:
    last_simple_table["schemaString"] = json.dumps(
        {"type": "struct", "fields": schema_list},
        separators=(",", ":"),
    )
except Exception:
    pass
```

Step by step:

1. `collect_schema(model_df)` (from `supertable/utils/helper.py`) turns the
   incoming Polars DataFrame into a list of `{name, type, nullable, metadata}`
   field dicts.
2. If that returns empty (e.g. the helper can't introspect the dtype),
   `_schema_list_from_polars_df` (local to `simple_table.py`) falls back to
   walking `model_df.schema` and mapping each Polars dtype to a Spark/Delta
   type string.
3. The result is assigned directly onto `last_simple_table["schema"]` —
   replacing whatever was there in the previous snapshot. There is no read
   of the prior `schema` field at any point in this method.
4. A Spark `StructType` mirror is written to `schemaString` so Delta-shaped
   readers see the same fields.

Because `last_simple_table` was built by mutating the previous snapshot
dict, every other field (`resources`, `previous_snapshot`, etc.) carries
forward — only `schema` and `schemaString` are wholesale-replaced.

#### Caller contract

- Every caller of `SimpleTable.update` must pass a `model_df` whose schema
  is exactly the schema the new snapshot should record — **OR** pass
  `model_df=None` to explicitly preserve the previous snapshot's schema.
- For `DataWriter.write(...)` (non-delete), that's the incoming Arrow/Polars
  data the caller supplied. Sending a DataFrame missing columns that exist
  in older parquet files will shrink the snapshot's `schema` even though
  the columns are still present on disk.
- For `DataWriter.write(..., delete_only=True)`, the writer passes
  `model_df=None`. **Deletes never change the schema** — only update paths
  do. The incoming delete-predicate dataframe only carries the columns the
  caller used to identify rows to remove (e.g. just the primary key); using
  it as `model_df` would collapse `schema` / `schemaString` to that partial
  shape, breaking every subsequent `SELECT col` against a column not in the
  predicate. The on-disk parquet files written by `process_delete_only`
  retain the full schema; the snapshot metadata must too.
- For `DataWriter.compact(...)` and `SimpleTable.repair_schema()`, the
  internal helpers derive `model_df` by reading the actual parquet files on
  storage, so the schema reflects what's physically present.

#### Why this design (and not append-only union)

An alternative would have been "schema is the union of every column ever
written, columns can only be added, never removed" (Iceberg and Delta lean
that way). The chosen design instead lets the writer declare the canonical
schema explicitly:

| Design | Pros | Cons |
|--------|------|------|
| **"Last write wins" (the choice)** | Writer owns the schema; column drops and dtype changes are first-class operations expressed by passing a different `model_df`. No accumulating-forever metadata. Simple update semantics. | Caller must always pass a full-shape `model_df`. A partial `model_df` shrinks the snapshot metadata even though older parquet files still contain the missing columns. |
| Append-only union | Snapshot metadata always reflects every column the table has ever had. Cannot be silently shrunk. | Need an explicit "drop column" operation to ever shrink the schema. More complex update semantics. |

#### Read-time fallbacks

A `SELECT col` against a column not in the snapshot's `schema` field will
fail with `Missing required column(s):` because
`DataEstimator.get_missing_columns` validates the requested columns against
this field. That's the contract working as designed.

To keep queries working when the snapshot `schema` has drifted from the
parquet files (e.g. a caller passed a partial `model_df`, or the field was
written empty by an older buggy code path), the SDK has two defensive
fallbacks:

1. **`DataEstimator` self-heal.** When the snapshot's schema is empty or
   doesn't list a requested column, the estimator reads the first parquet
   file's schema and augments the in-memory column set. Queries succeed
   without anyone having to repair anything. A `WARNING` log line surfaces
   the drift so it's not silent.
2. **`SimpleTable.repair_schema()`.** A one-shot helper that derives the
   schema from the first parquet file and rewrites the snapshot with it.
   Use when you want to persistently fix the metadata rather than rely on
   the self-heal at every read.

These are recovery paths, not part of the contract — they keep queries
running while the caller fixes the upstream `model_df`.

### Snapshot Retention

By default the snapshot JSONs accumulate forever — every write's
`previous_snapshot` field forms an unbounded linked list back to v0,
and sunset parquet files (replaced during compaction) are left on
storage. Set `SUPERTABLE_SNAPSHOT_RETENTION=N` to keep only the **N**
most recent snapshots; set `SUPERTABLE_SUNSET_GC_ENABLED=true` to
also queue sunset parquets for deletion. In both cases the writer
pushes the deletion candidates onto a per-table Redis stream after
the leaf-CAS commit, and a separate orchestrator
(`GCCleaner.tick()` — chap. 17) physically deletes them after a
safety delay window long enough that any in-flight reader has
finished. Both flags are off by default; with neither set, behaviour
is identical to a pre-GC SDK build.

### CAS (Compare-and-Swap) Pointer Update

The Redis leaf pointer is updated atomically via a Lua script (`_LUA_LEAF_CAS_SET` or `_LUA_LEAF_PAYLOAD_CAS_SET` in `RedisCatalog`). The script reads the current version, increments it, and writes the new value in a single atomic Redis operation, preventing lost updates under concurrency.

Two CAS variants exist:

- **`set_leaf_path_cas`** -- Stores only the path to the snapshot file. Readers must fetch the snapshot from storage.
- **`set_leaf_payload_cas`** -- Stores the path *and* the full snapshot payload inline in Redis. Readers can serve metadata requests without hitting storage.

### Schema Evolution

Schema is captured from the model DataFrame at each write. The system uses a best-effort mapping from Polars dtypes to Spark/Delta type strings via `_spark_type_from_polars_dtype()`:

| Polars Type | Spark Type |
|-------------|------------|
| `Utf8`, `String` | `string` |
| `Boolean` | `boolean` |
| `Int8` | `byte` |
| `Int16` | `short` |
| `Int32` | `integer` |
| `Int64` | `long` |
| `UInt8` | `short` |
| `UInt16` | `integer` |
| `UInt32` | `long` |
| `UInt64` | `decimal(20,0)` |
| `Float32` | `float` |
| `Float64` | `double` |
| `Date` | `date` |
| `Datetime` | `timestamp` |
| `Binary` | `binary` |
| `Decimal(p,s)` | `decimal(p,s)` |

Unknown types default to `string`.

---

## Dataclass Definitions

All dataclasses are defined in `supertable/data_classes.py`. They serve as typed transfer objects between the metadata layer and the query execution engines.

### TableDefinition

```python
@dataclass
class TableDefinition:
    super_name: str
    simple_name: str
    alias: str
    columns: List[str] = field(default_factory=list)
```

Identifies a single table within a query context. The `alias` allows the SQL engine to reference the table by a user-chosen name. `columns` lists the column names available in this table.

### SuperSnapshot

```python
@dataclass
class SuperSnapshot:
    super_name: str
    simple_name: str
    simple_version: int
    files: List[str] = field(default_factory=list)
    columns: Set[str] = field(default_factory=set)
```

A resolved view of a single SimpleTable snapshot used during query planning. Contains the list of data files to scan and the column set for schema validation.

### RbacViewDef

```python
@dataclass
class RbacViewDef:
    allowed_columns: List[str] = field(default_factory=lambda: ["*"])
    where_clause: str = ""
```

Defines RBAC-based column and row filtering for a table alias. Produced by `restrict_read_access()` and consumed by query executors to create a filtered view. `["*"]` means unrestricted column access; a non-empty `where_clause` is injected as a SQL WHERE predicate.

### DedupViewDef

```python
@dataclass
class DedupViewDef:
    primary_keys: List[str] = field(default_factory=list)
    order_column: str = "__timestamp__"
    visible_columns: List[str] = field(default_factory=list)
```

Defines dedup-on-read semantics. The query engine creates a `ROW_NUMBER()` window partitioned by `primary_keys` and ordered by `order_column` DESC, keeping only the latest row per key combination. `visible_columns` controls projection; when empty or `["*"]`, all columns except `__rn__` are exposed.

### TombstoneDef

```python
@dataclass
class TombstoneDef:
    primary_keys: List[str] = field(default_factory=list)
    deleted_keys: List = field(default_factory=list)
```

Defines soft-delete filtering. The executor builds a view that excludes rows whose primary-key tuple appears in `deleted_keys`. Each entry in `deleted_keys` is a list of scalar values matching the order of `primary_keys`.

### Reflection

```python
@dataclass
class Reflection:
    storage_type: str
    reflection_bytes: int
    total_reflections: int
    supers: List[SuperSnapshot]
    freshness_ms: int = 0
    rbac_views: Dict[str, RbacViewDef] = field(default_factory=dict)
    dedup_views: Dict[str, DedupViewDef] = field(default_factory=dict)
    tombstone_views: Dict[str, TombstoneDef] = field(default_factory=dict)
```

The top-level query plan object. Aggregates all resolved SuperSnapshots with their associated RBAC, dedup, and tombstone view definitions. Key fields:

| Field | Purpose |
|-------|---------|
| `storage_type` | Backend identifier (e.g., `"LOCAL"`, `"S3"`, `"MINIO"`). |
| `reflection_bytes` | Total bytes across all data files -- used for engine auto-selection. |
| `total_reflections` | Count of data files to scan. |
| `supers` | List of `SuperSnapshot` objects, one per SimpleTable involved. |
| `freshness_ms` | Maximum `last_updated_ms` across snapshots -- helps the engine decide between caching and re-reading. |
| `rbac_views` | Alias-keyed RBAC filter definitions. |
| `dedup_views` | Alias-keyed dedup-on-read definitions. |
| `tombstone_views` | Alias-keyed tombstone (soft-delete) definitions. |

---

## Metadata-to-Redis Mapping

All metadata is stored in Redis as JSON strings. The key schema follows the
v2 hierarchy enforced by `supertable/redis_keys.py` (see
[16 Redis Key Layout](16_redis_layout.md)):

```
supertable:{organization}:lakes:{super_name}:meta:root
supertable:{organization}:lakes:{super_name}:meta:leaf:doc:{simple_name}
supertable:{organization}:lakes:{super_name}:meta:mirrors
supertable:{organization}:lakes:{super_name}:meta:table_config:doc:{simple_name}
supertable:{organization}:lakes:{super_name}:meta:table_names
```

Built by `meta_root()`, `meta_leaf()`, `meta_mirrors()`,
`meta_table_config()`, and `meta_table_names()` in
`supertable/redis_keys.py`.

### meta:root

```json
{"version": 3, "ts": 1713200000000}
```

Tracks the SuperTable's global version. Bumped on structural changes (new tables, deletes). May contain additional flags:

- `read_only` (bool) -- marks the SuperTable as a read-only clone.
- `cloned_from` (str) -- source SuperTable name for clones.
- `clone_type` (str) -- `"replica"` for live-linked clones.
- `replica_tables` (list) -- subset of tables exposed by a replica.

### meta:leaf:{simple_name}

```json
{
  "version": 5,
  "ts": 1713200000000,
  "path": "acme/warehouse/tables/orders/snapshots/tables_20240414.json",
  "payload": { ... }
}
```

Points to the current snapshot for a SimpleTable. The `payload` field (when present) holds the full snapshot data inline, allowing readers to skip the storage read.

### meta:mirrors

```json
{"formats": ["DELTA", "ICEBERG"], "ts": 1713200000000}
```

Lists enabled mirror export formats for the SuperTable. Supported values: `DELTA`, `ICEBERG`, `PARQUET`.

### meta:table_config:{simple_name}

```json
{
  "dedup_mode": "latest",
  "primary_keys": ["order_id"],
  "modified_ms": 1713200000000
}
```

Per-table configuration for dedup mode, primary keys, and other table-level settings.

---

## MetaReader

The `MetaReader` class (`supertable/meta_reader.py`) provides a read-only metadata API optimized for the query path. It wraps `SuperTable` and `RedisCatalog` to serve:

- **`get_tables(role_name)`** -- Lists all SimpleTables visible to a role (RBAC-filtered).
- **`get_table_schema(table_name, role_name)`** -- Returns the schema for a single table or an aggregate schema across all tables if querying at the SuperTable level.

MetaReader includes an in-process cache (`_SUPER_META_CACHE`) with a configurable TTL (`SUPERTABLE_SUPER_META_CACHE_TTL_S`, default 1 second) to de-duplicate bursty reflection calls. Redis leaf values are parsed and normalized through helper functions (`_try_parse_leaf_meta`, `_leaf_to_snapshot_like`, `_schema_to_dict`) that handle bytes/string decoding and various payload shapes.
