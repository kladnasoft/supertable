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
