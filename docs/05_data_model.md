# Data Island Core — Data Model

## Overview

Data Island Core organizes data in a three-level hierarchy: Organization → SuperTable → SimpleTable. An organization is a tenant namespace. A SuperTable is a virtual database that groups related tables. A SimpleTable is a versioned, append-only data table backed by Parquet files on object storage and metadata in Redis.

This hierarchy is the foundation of every operation in the platform — reads, writes, RBAC, monitoring, and audit logging all resolve to a specific point in this tree. Understanding it is the prerequisite for understanding everything else.

---

## How it works

### The three levels

**Organization** — A string identifier that scopes all data, metadata, roles, tokens, and audit events. Set globally via `SUPERTABLE_ORGANIZATION`. Every Redis key and storage path is prefixed with the organization name. A single deployment serves one organization (multi-org support is architectural, not yet multi-tenant in the security sense).

**SuperTable** — A virtual database within an organization. It has a root metadata entry in Redis (`meta:root`), a directory on storage (`{org}/{super_name}/`), and its own RBAC role and user registries. Creating a SuperTable initializes the root pointer, the storage directory, and the RBAC scaffolding. Implemented in `super_table.py`.

**SimpleTable** — A versioned data table within a SuperTable. Each write produces a new snapshot — an immutable description of which Parquet files constitute the table at that version. Old snapshots are retained (the previous snapshot path is stored in each new snapshot). Reads always see the latest committed snapshot. Implemented in `simple_table.py`.

### Storage layout

```
{org}/
  {super_name}/
    super/                           SuperTable marker directory
    tables/
      {simple_name}/
        data/
          {uuid}.parquet             Data files (one per write batch)
          {uuid}.parquet
        snapshots/
          tables_{timestamp}_{uuid}.json   Snapshot files (one per version)
```

Data files are never modified — each write appends new Parquet files and produces a new snapshot that references the complete file set. Deleted or overwritten data is handled by removing file references from the snapshot (the old Parquet files may be cleaned up later or retained for history).

### Snapshot structure

A snapshot is a JSON file on storage and a cached payload in Redis. It describes the complete state of a table at a point in time:

```json
{
  "simple_name": "orders",
  "location": "acme/example/tables/orders",
  "snapshot_version": 7,
  "last_updated_ms": 1711734000000,
  "previous_snapshot": "acme/example/tables/orders/snapshots/tables_20250328_..._abc.json",
  "schema": [
    {"name": "id", "type": "integer", "nullable": true},
    {"name": "customer", "type": "string", "nullable": true},
    {"name": "amount", "type": "double", "nullable": true}
  ],
  "resources": [
    {"file": "acme/example/tables/orders/data/a1b2c3.parquet", "rows": 5000},
    {"file": "acme/example/tables/orders/data/d4e5f6.parquet", "rows": 3200}
  ],
  "tombstones": {
    "primary_keys": ["id"],
    "deleted_keys": [[101], [202], [303]]
  }
}
```

The `resources` array is the file manifest — the union of all listed Parquet files is the table's current content. `previous_snapshot` creates a linked list of versions. `tombstones` records soft-deleted rows that must be filtered out at read time.

### Redis leaf pointer

The current snapshot for each table is tracked in Redis as a "leaf" pointer:

```
Key:   supertable:{org}:{sup}:meta:leaf:{simple_name}
Type:  Hash
Fields:
  path     — storage path to the current snapshot JSON
  payload  — the snapshot JSON itself (cached to avoid storage reads)
  version  — monotonically increasing version counter
  ts       — last update timestamp (ms)
```

When `set_leaf_payload_cas` is called, both the `path` and the full `payload` are stored. Readers check the payload first — if present, they skip the storage read entirely. This is the fast path for `MetaReader` and `DataReader`.

---

## Data classes

Defined in `supertable/data_classes.py`, these dataclasses are the shared vocabulary between the read path, write path, and query engine.

### TableDefinition

```python
@dataclass
class TableDefinition:
    super_name: str       # SuperTable name
    simple_name: str      # Table name
    alias: str            # SQL alias for the table
    columns: List[str]    # Column names
```

Used by the query engine to identify tables and their schemas when building SQL execution plans.

### SuperSnapshot

```python
@dataclass
class SuperSnapshot:
    super_name: str          # SuperTable name
    simple_name: str         # Table name
    simple_version: int      # Snapshot version number
    files: List[str]         # Parquet file paths on storage
    columns: Set[str]        # Column names in this snapshot
```

The read path's representation of a table — a list of files and their columns. Constructed from the snapshot JSON by `DataReader`.

### Reflection

```python
@dataclass
class Reflection:
    storage_type: str                          # "MINIO", "S3", etc.
    reflection_bytes: int                      # Total data size in bytes
    total_reflections: int                     # Number of tables
    supers: List[SuperSnapshot]                # All table snapshots
    freshness_ms: int                          # Most recent update timestamp
    rbac_views: Dict[str, RbacViewDef]         # RBAC filters per table
    dedup_views: Dict[str, DedupViewDef]       # Dedup config per table
    tombstone_views: Dict[str, TombstoneDef]   # Tombstone filters per table
```

The complete picture assembled by `DataReader` for query execution. Contains every snapshot, every RBAC filter, every dedup definition, and every tombstone rule — everything the query engine needs to build correct, secure SQL.

### RbacViewDef

```python
@dataclass
class RbacViewDef:
    allowed_columns: List[str]   # ["*"] for unrestricted, or explicit list
    where_clause: str            # SQL WHERE predicate, or "" for none
```

Produced by `restrict_read_access()` in the RBAC module. The query engine wraps the table in a view that projects only `allowed_columns` and applies the `where_clause`. This enforces row-level and column-level security at query time.

### DedupViewDef

```python
@dataclass
class DedupViewDef:
    primary_keys: List[str]        # Composite key columns
    order_column: str              # ORDER BY DESC column (default: __timestamp__)
    visible_columns: List[str]     # Columns to expose (["*"] = all)
```

When a table has `dedup_on_read` enabled in its config, the query engine wraps it in a `ROW_NUMBER()` window view that keeps only the latest row per primary key combination. This allows append-only writes with logical update semantics — the latest row for each key wins.

### TombstoneDef

```python
@dataclass
class TombstoneDef:
    primary_keys: List[str]    # Key columns
    deleted_keys: List          # List of key tuples to exclude
```

Tombstones represent soft-deleted rows. The query engine adds a `WHERE NOT (key_col IN (...))` filter to exclude them. Tombstones accumulate across writes and are periodically compacted.

---

## MetaReader

`meta_reader.py` provides a read-only metadata aggregation layer. It reads catalog data from Redis and assembles it into structured responses for the UI and API.

### Key methods

| Method | Returns | Description |
|---|---|---|
| `get_tables(role_name)` | `List[str]` | All table names visible to the role |
| `get_table_schema(table_name, role_name)` | `List[Dict]` | Column definitions (name, type, nullable), filtered by RBAC |
| `get_table_stats(table_name, role_name)` | `List[Dict]` | File count, total size, row count, version, last updated |
| `get_super_meta(role_name)` | `Dict` | Full SuperTable metadata: all tables, schemas, sizes, versions |

MetaReader respects RBAC — it only returns tables and columns the role is permitted to see. Internally it calls `scan_leaf_items()` on the catalog and assembles the results.

### Caching

When `SUPERTABLE_SUPER_META_CACHE_TTL_S` is set, MetaReader results are cached in-memory for the specified duration. This is recommended for read-heavy workloads where metadata doesn't change frequently.

---

## Versioning and immutability

Every write to a SimpleTable creates a new snapshot version:

1. `DataWriter` acquires a per-table lock
2. Reads the current snapshot (via the Redis leaf pointer)
3. Writes new Parquet files to storage
4. Computes the new resource list (existing files ± added/removed files)
5. Writes a new snapshot JSON to storage
6. Atomically updates the Redis leaf pointer to the new snapshot
7. Bumps the root version counter
8. Releases the lock

Old snapshots are retained on storage (linked via `previous_snapshot`). This provides a history of table states, though the platform does not currently expose point-in-time queries — reads always see the latest snapshot.

---

## Module structure

```
supertable/
  super_table.py       SuperTable entity — init, storage, Redis root (102 lines)
  simple_table.py      SimpleTable entity — init, snapshot, update, delete (269 lines)
  data_classes.py      Shared dataclasses — TableDefinition, SuperSnapshot, etc. (89 lines)
  meta_reader.py       Metadata aggregator — schemas, stats, table lists (542 lines)
```

---

## Frequently asked questions

**Can I query a previous snapshot version?**
Not through the standard API. The platform always reads the latest committed snapshot. The snapshot history exists on storage (via `previous_snapshot` links), but no endpoint exposes historical reads. This is a potential future feature.

**What happens if a write fails mid-way?**
The lock is released, and the Redis leaf pointer is unchanged (it still points to the previous snapshot). Any Parquet files written to storage are orphaned — they exist on disk but are not referenced by any snapshot. A future cleanup job could garbage-collect unreferenced files.

**How does dedup-on-read differ from dedup-on-write?**
Dedup-on-write eliminates duplicates at write time using overlap detection and overwrite columns. Dedup-on-read keeps all rows on storage and eliminates duplicates at query time using a `ROW_NUMBER()` window function. Dedup-on-read is cheaper to write but slightly more expensive to query.

**What are tombstones?**
Soft deletes. When rows are logically deleted, their primary key values are recorded in the snapshot's `tombstones` block. At read time, the query engine filters them out. Tombstones are periodically compacted — once enough accumulate, the data files are rewritten without the deleted rows.

**Is there a limit on the number of tables in a SuperTable?**
No hard limit. The practical limit is Redis memory for leaf pointers (each leaf is a small hash) and the number of files the query engine can handle in a single query. Hundreds of tables work well; thousands may require tuning.

**Can two tables have the same name in different SuperTables?**
Yes. Table names are scoped to their SuperTable. `acme/warehouse/orders` and `acme/staging_db/orders` are completely independent.
