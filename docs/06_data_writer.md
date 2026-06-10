# Data Writer

## Business Context

The Data Writer is the core write-path component of SuperTable. Every row that enters the data lake -- whether from an API upload, a staging area commit, or a pipe transformation -- flows through the `DataWriter` class. It is responsible for turning raw Arrow tables into versioned, deduplicated, compressed Parquet files while maintaining snapshot isolation, catalog consistency, and optional mirroring to downstream formats.

The write pipeline is designed around three principles:

1. **Atomicity** -- a write either succeeds completely (new snapshot, updated catalog, optional mirror) or fails without side-effects. A Redis-backed per-table lock serialises concurrent writers.
2. **Idempotency** -- the `newer_than` parameter lets callers replay the same data safely; stale or duplicate rows are silently dropped.
3. **Incremental merge** -- only files whose key ranges overlap with incoming data are rewritten. Non-overlapping small files accumulate until a compaction threshold is reached, at which point they are merged in memory-bounded chunks.

---

## Module Location

- **Primary module**: `supertable/data_writer.py`
- **Processing engine**: `supertable/processing.py`

---

## DataWriter Class

```python
class DataWriter:
    def __init__(self, super_name: str, organization: str)
```

### Constructor

Creates a writer bound to a specific SuperTable within an organization. Internally instantiates:

- `self.super_table` -- a `SuperTable(super_name, organization)` instance representing the logical dataset.
- `self.catalog` -- a `RedisCatalog()` for metadata operations (lock acquisition, leaf pointer updates, root bumps).
- `self._table_config_cache` -- an in-process dict that caches per-table dedup configuration to avoid repeated Redis round-trips.

---

## Write Pipeline

The `write()` method orchestrates the entire write pipeline. Each step is timed individually and logged at the end for performance diagnostics.

### Method Signature

```python
def write(
    self,
    role_name,
    simple_name,
    data,               # PyArrow Table
    overwrite_columns,  # list of column names forming the logical key
    compression_level=1,
    newer_than=None,
    delete_only=False,
    lineage=None,
) -> tuple[total_columns, total_rows, inserted, deleted]
```

### Pipeline Steps

```
access_check --> convert --> dedup_ts --> validate --> lock --> snapshot
    --> overlap --> newer_than --> process --> update_simple
    --> bump_root --> mirror --> unlock --> monitoring
    --> audit
```

#### 1. Access Control (`access`)

Calls `check_write_access()` from the RBAC module to verify the role has write permission on the target table. Raises immediately if denied.

#### 2. Convert Input (`convert`)

Converts the incoming PyArrow table to a Polars `DataFrame` using `polars.from_arrow(data)`. Captures `incoming_rows` and `incoming_columns` for monitoring.

#### 3. Dedup Timestamp Injection (`dedup_ts`)

If the table is configured with `dedup_on_read=True`, a `__timestamp__` column is injected (set to `datetime.now(timezone.utc)`) unless one already exists. The read side uses this column in a `ROW_NUMBER` window to return only the latest row per primary key.

#### 4. Validation (`validate`)

The `validation()` method enforces structural invariants:

| Rule | Constraint |
|---|---|
| Table name length | 1--128 characters |
| Name collision | `simple_name != super_name` |
| Name pattern | `^[A-Za-z_][A-Za-z0-9_]*$` |
| Overwrite columns type | Must be a list, not a string |
| Overwrite columns presence | All columns must exist in the DataFrame |
| Delete-only guard | `delete_only=True` requires `overwrite_columns` |
| Newer-than guard | `newer_than` must be a valid column name string and `overwrite_columns` must be set |

#### 5. Lock Acquisition (`lock`)

Acquires a per-SimpleTable Redis lock via `catalog.acquire_simple_lock()` with a 30-second TTL and 60-second timeout. The lock token is stored for release in the `finally` block. If the lock cannot be acquired, a `TimeoutError` is raised.

```python
token = self.catalog.acquire_simple_lock(
    org, super_name, simple_name,
    ttl_s=30, timeout_s=60
)
```

#### 6. Read Last Snapshot (`snapshot`)

Reads the current SimpleTable snapshot via `simple_table.get_simple_table_snapshot()`, returning a dict of resources (file paths, sizes, column stats) and tombstone metadata.

#### 7. Overlap Detection (`overlap`)

Calls `find_overlapping_files()` from `processing.py`. This function classifies every existing resource into one of three buckets:

- **`has_overlap=True`** -- the file's per-column min/max statistics indicate that at least one incoming key value falls within the file's range, OR the file has no stats (conservative assumption).
- **`has_overlap=False`** -- the file's key ranges do not overlap with incoming data but the file is small enough to be a compaction candidate.
- **Not included** -- large files with no overlap are left untouched.

The function then applies `prune_not_overlapping_files_by_threshold()` which gates compaction: non-overlapping small files are only included if either their total size exceeds `MAX_MEMORY_CHUNK_SIZE` or their count exceeds `MAX_OVERLAPPING_FILES`. These limits can be set per-table via `configure_table()` or fall back to global defaults.

#### 8. Newer-Than Filtering (`newer_than`)

When `newer_than` is specified, `filter_stale_incoming_rows()` joins incoming data against existing overlapping files on `overwrite_columns`, comparing the `newer_than` column. Rows where the existing value is >= the incoming value are dropped as stale/replayed. If all rows are stale, the write short-circuits (no file I/O), but still releases the lock and emits monitoring.

A `file_cache` dict is populated during this step so that Parquet files read here are not re-read during the processing step.

#### 9. Tombstone / Soft-Delete Logic

When `dedup_on_read` is enabled and `primary_keys` are configured:

**Delete-only with tombstones** (`delete_only=True` and `use_tombstones=True`):
- Extracts key tuples from the incoming DataFrame via `extract_key_tuples()`.
- Appends them to the existing tombstone list (deduplicated).
- No Parquet files are read or written (purely metadata).
- If the tombstone count reaches the compaction threshold (default 1000, configurable via `tombstone_compact_total`), `compact_tombstones()` physically removes the tombstoned rows from all affected Parquet files and clears the tombstone list.

**Reconciliation on insert/overwrite**:
- When incoming data has keys matching existing tombstones, those tombstone entries are removed via `reconcile_tombstones()` so the newly-written rows become visible.

**Physical delete fallback** (`delete_only=True` without `dedup_on_read`):
- Falls back to `process_delete_only()`, which rewrites each overlapping file independently with the matching rows removed.

#### 10. Processing -- Merge and Rewrite (`process`)

For non-delete writes, `process_overlapping_files()` executes a three-phase merge:

**Phase 1 -- Compaction** (`process_files_without_overlap`): Reads all `has_overlap=False` files, concatenates them with schema alignment, and flushes in memory-bounded chunks when cumulative size exceeds `MAX_MEMORY_CHUNK_SIZE`.

**Phase 2 -- Overlap merge** (`process_files_with_overlap`): For each `has_overlap=True` file, performs an anti-join on the composite overwrite key to drop rows being overwritten, then concatenates the survivors with the merged buffer. Uses `2x MAX_MEMORY_CHUNK_SIZE` as the spill threshold. Files where no rows are actually deleted (false positive from stats) are skipped and tracked separately.

**Phase 3 -- Skipped-file compaction**: When the number of skipped files (overlap=True but zero actual matches) exceeds `MAX_OVERLAPPING_FILES`, they are all merged into the output buffer to prevent small-file accumulation.

A final flush writes any remaining rows.

#### 11. Update Snapshot (`update_simple`)

Calls `simple_table.update()` which creates a new snapshot JSON on storage containing the new resource list (new files added, sunset files removed), schema, lineage, and tombstone metadata.

#### 12. Catalog Update (`bump_root`)

Two atomic Redis operations:

1. **`set_leaf_payload_cas()`** -- stores the new snapshot payload and path for the SimpleTable leaf, using compare-and-swap semantics. Per-file stats are stripped from the Redis payload to reduce size (~934 to ~172 bytes per resource).
2. **`bump_root()`** -- increments the root version timestamp so that readers see the new data.

Falls back to `set_leaf_path_cas()` if the payload CAS method is unavailable (backward compatibility).

#### 13. Schema and Table Name Registration

Stores the table schema and name in Redis as permanent metadata:

```python
# Via redis_keys.schema(org, sup, simple_name) and redis_keys.meta_table_names(org, sup):
self.catalog.r.set(RK.schema(org, sup, simple_name), schema_json)         # supertable:{org}:lakes:{sup}:schema:doc:{simple_name}
self.catalog.r.sadd(RK.meta_table_names(org, sup), simple_name)           # supertable:{org}:lakes:{sup}:meta:table_names
```

#### 14. Mirroring (`mirror`)

Calls `MirrorFormats.mirror_if_enabled()` to replicate the new snapshot into downstream formats (Delta Lake, Iceberg, Parquet) if mirroring is configured. Mirroring failures are logged but never fail the write.

#### 15. Lock Release (`finally`)

The per-table lock is released in the `finally` block via `catalog.release_simple_lock()` with the original token. Token mismatch (e.g., expired lock) is logged but does not raise.

#### 16. Monitoring (after lock release)

A `MonitoringWriter` context manager enqueues write statistics to a
daily-partitioned Redis LIST. This runs entirely outside the data lock
to avoid holding the lock during I/O. Today's partition key is
`supertable:{org}:monitor:writes:doc:{YYYY-MM-DD}` (recomputed per
ship so writes that cross midnight roll naturally — chap. 14).

```python
from supertable.monitoring.partitions import MONITORING_SINK_TABLES

# Loop-guard: writes to a monitoring sink table are deliberately not
# measured. The external orchestrator that drained the partition is
# *writing back* the metric, and re-emitting it would create a 1:1
# amplification cycle. The sink-table set is the single source of
# truth in supertable/monitoring/partitions.py.
if stats_payload is not None and simple_name not in MONITORING_SINK_TABLES:
    stats_payload["supertables"] = [self.super_table.super_name]
    with MonitoringWriter(
        organization=self.super_table.organization,
        monitor_type="writes",
    ) as monitor:
        monitor.log_metric(stats_payload)
```

`MONITORING_SINK_TABLES` =
`{"__writes__", "__reads__", "__mcp__", "__plans__"}`. Writes
targeting these tables skip the metric emission entirely.

The stats payload includes: `query_id`, `recorded_at`, `organization`,
`super_name`, `role_name`, `table_name`, `overwrite_columns`,
`compression_level`, `newer_than`, `delete_only`, `incoming_rows`,
`incoming_columns`, `inserted`, `deleted`, `total_rows`,
`total_columns`, `new_resources`, `sunset_files`, `skipped_stale`,
`lineage`, `duration`, `supertables`.

#### 17. Data Quality Notification

Calls `notify_ingest()` to set a debounced "pending" flag in Redis. The Data Quality scheduler picks it up on the next tick. This never blocks or fails the write.

#### 18. Audit Logging

Emits a `DATA_WRITE` audit event with category `DATA_MUTATION` including row counts, durations, and role information. Failures are silently ignored.

---

## Explicit Compaction — `DataWriter.compact()`

`write()` does opportunistic compaction in three places (Phase 1 small-file
roll-up, Phase 3 skipped-file roll-up, tombstone threshold breach). For
deployments that want **scheduled, manual** compaction outside the
natural write cadence, `DataWriter.compact()` is the explicit entry
point — it does the same work `write()` would do for an empty input,
without rewriting any file that doesn't need to be rewritten.

```python
dw = DataWriter("warehouse", "acme")
stats = dw.compact(
    role_name="admin",
    simple_name="orders",
    force_tombstones=True,   # default: physically clean tombstones now
    small_only=True,         # default: only touch files < max_memory_chunk_size
    compression_level=1,
)
print(stats["files_before"], "→", stats["files_after"])
```

### What the call does

1. **Access check** — `check_write_access` against the target table.
2. **Per-simple lock** — same TTL (30 s) and timeout (60 s) as `write()`,
   so concurrent writes and compactions serialise.
3. **Snapshot read** — uses `SimpleTable(..., create_if_missing=False)`
   so a missing table raises `TableNotFoundError` instead of being
   bootstrapped. Compaction never creates a table.
4. **Tombstone compaction** — runs `compact_tombstones()` to physically
   remove soft-deleted rows from affected parquet files.
   - `force_tombstones=True` (default) bypasses
     `tombstone_compact_total`: clean *now* regardless of count.
   - `force_tombstones=False` honors the natural threshold (same gate
     as the delete-only path in `write()`).
   - Skipped entirely when no primary keys / no tombstones.
5. **Small-file compaction** — calls `processing.compact_resources()`:
   - `small_only=True` (default): only files strictly smaller than
     `max_memory_chunk_size` are considered. Large files are
     left untouched.
   - `small_only=False`: rewrite every resource regardless of size
     (useful for a full-table re-encode / compression change).
6. **Schema preservation** — derives the post-compaction schema for
   `simple_table.update()` by reading the first new parquet file's
   footer. Falls back to reconstructing from the prior snapshot's
   `schema` field (compaction by definition preserves schema). This
   guards against the silent corruption that would occur if `update()`
   was handed an empty / wrong-typed model_df.
7. **Snapshot commit** — `simple_table.update()` → `set_leaf_payload_cas`
   → `bump_root`. Same atomic-CAS pattern as `write()`.
8. **Mirroring** — Delta / Iceberg / Parquet mirrors are refreshed.
9. **Monitoring + audit** — emits a `monitor_type="compact"` metric
   (own daily partition, own sink table `__compact__`) and a
   `DATA_WRITE` audit event with `operation="compact"`.

### Concurrency

Compaction takes the same per-simple Redis lock as `write()`. A
concurrent writer either runs first (compaction sees the updated
snapshot) or waits. No corruption window — the leaf-CAS + bump-root
sequence is identical.

### Short-circuit

When `compact_resources` finds nothing to merge **and** tombstones
either don't run or don't produce work, the method returns early
without writing a new snapshot. `files_before == files_after` and
no leaf-CAS / root-bump / mirror calls are made.

### Return value

A stats dict (safe to JSON-encode) with the same shape monitoring
emits:

| Key | Meaning |
|---|---|
| `query_id` | Per-compaction UUID. Correlates the monitoring/audit entries. |
| `files_before` / `files_after` | Resource count before/after the commit. |
| `files_compacted` | Number of small files that were merged. |
| `tombstone_rows_removed` | Rows physically deleted from parquets in Phase 4. |
| `tombstone_files_rewritten` | Files rewritten by tombstone compaction. |
| `new_resources` / `sunset_files` | Counts of files written / removed. |
| `total_rows_written` | Rows written into the new compacted files. |
| `duration` | Wall-clock seconds. |
| `lineage` | JSON-encoded provenance dict. |
| `supertables` | Always `[<super_name>]` — added before monitoring emit. |

### Value-preservation invariants

`processing.compact_resources()` provides these guarantees, verified
by the `test_processing_compact_resources` suite (real Parquet I/O
in a tempdir):

- **No row loss** — multiset of input rows == multiset of output rows.
- **No row duplication** — same.
- **No column loss** — every column from any source file survives.
- **No phantom columns** — no columns added that weren't in any source.
- **Schema evolution preserved** — when source files have different
  column sets, the union schema is used; missing columns become null.
- **Dtypes preserved** — Int / Float / String / Boolean / Date round-trip
  through Parquet without coercion.
- **Race-tolerant** — if a source file is sunset by another writer
  mid-compaction (`_read_parquet_safe` returns None), the file is
  **not** added to `sunset_files`, so the snapshot still references
  it and the next compaction retries. No silent data loss.

## Table Configuration

### configure_table()

```python
def configure_table(
    self,
    role_name: str,
    simple_name: str,
    primary_keys: list,
    dedup_on_read: bool = False,
    max_memory_chunk_size: int | None = None,
    max_overlapping_files: int | None = None,
    tombstone_compact_total: int | None = None,
) -> None
```

Persists table-level configuration in Redis via `catalog.set_table_config()`. The configuration controls:

| Parameter | Default | Purpose |
|---|---|---|
| `primary_keys` | (required) | Column names forming the logical primary key |
| `dedup_on_read` | `False` | Enables ROW_NUMBER dedup on read and `__timestamp__` injection on write |
| `max_memory_chunk_size` | 16 MB | Maximum in-memory buffer size before flushing a Parquet chunk |
| `max_overlapping_files` | 100 | File-count threshold that triggers compaction of small files |
| `tombstone_compact_total` | 1000 | Maximum tombstone entries before physical compaction is triggered |

Configuration is cached locally in `_table_config_cache` so that subsequent `write()` calls avoid extra Redis round-trips.

---

## Overlap Detection Details

The `find_overlapping_files()` function in `processing.py` uses per-column min/max statistics stored in each resource's `stats` dict:

```python
def find_overlapping_files(
    last_simple_table: dict,
    df: polars.DataFrame,
    overwrite_columns: List[str],
    locking: object = None,     # deprecated
    table_config: Optional[dict] = None,
) -> Set[Tuple[str, bool, int]]
```

**Algorithm**:

1. For each resource, extract per-column stats (min/max values).
2. For each overwrite column, check if any incoming unique value falls within `[min, max]`.
3. If stats are missing for a column, conservatively mark the file as overlapping.
4. Date/DateTime columns are normalized from ISO strings before comparison.
5. Non-overlapping small files (below `MAX_MEMORY_CHUNK_SIZE`) are included as compaction candidates with `has_overlap=False`.
6. The `prune_not_overlapping_files_by_threshold()` function gates inclusion of non-overlapping files: they are only merged when their total size exceeds `MAX_MEMORY_CHUNK_SIZE` or their count reaches `MAX_OVERLAPPING_FILES`.

---

## Schema Alignment

The `concat_with_union()` function in `processing.py` handles DataFrames with different schemas:

```python
def concat_with_union(a: polars.DataFrame, b: polars.DataFrame) -> polars.DataFrame
```

It computes a union schema via `_union_schema()` and aligns both DataFrames before concatenation:

- Missing columns are filled with `null`.
- Type conflicts are resolved by `_resolve_unified_dtype()`:
  - If any type is `Utf8` (string), the unified type is `Utf8`.
  - Mixed integer + float becomes `Float64`.
  - Mixed integers become `Int64`.
  - `Datetime` types unify to `Datetime("us", None)`.
  - Fallback is `Utf8`.

---

## Row-Group Optimization

All Parquet writes use a fixed row-group size defined in `processing.py`:

```python
_PARQUET_ROW_GROUP_SIZE = 122_880  # ~120K rows
```

This value sits in the recommended 100K--1M range. The trade-off:

- **Smaller groups** produce tighter min/max statistics, allowing DuckDB to skip more row groups during filtered scans.
- **Larger groups** reduce metadata overhead.
- **122,880 rows** is the balance chosen for the incremental-merge write pattern.

Before writing, data is sorted by `__timestamp__` (if present) followed by the overwrite columns. This ensures each row group covers a tight value range, maximising the effectiveness of DuckDB's zonemap-based predicate pushdown.

All Parquet files are written with:

- **Compression**: zstd at the caller-specified `compression_level` (default 1).
- **Dictionary encoding**: enabled.
- **Statistics**: enabled (write_statistics=True).
- **Partitioning**: when `__timestamp__` is present, rows are partitioned by `year/month/day` into Hive-style subdirectories.

---

## Tombstone System

Tombstones provide soft-delete semantics for tables with `dedup_on_read` enabled.

### Key Functions

| Function | Purpose |
|---|---|
| `extract_key_tuples(df, primary_keys)` | Extracts unique composite-key tuples from a DataFrame for tombstone storage |
| `reconcile_tombstones(tombstone_keys, incoming_keys)` | Removes tombstone entries that match newly-written keys (resurrection) |
| `compact_tombstones(snapshot, primary_keys, data_dir, compression_level, table_config)` | Physically removes tombstoned rows from Parquet files when the threshold is breached |
| `_tombstone_threshold(table_config)` | Returns the compaction threshold (default: `_DEFAULT_TOMBSTONE_COMPACT_TOTAL = 1000`) |
| `_tombstone_overlaps_stats(tombstone_df, primary_keys, stats)` | Uses column stats to skip files that provably cannot contain tombstoned keys |

### Lifecycle

1. **Soft delete**: Key tuples are appended to the `tombstones.deleted_keys` list in the snapshot metadata. No Parquet files are touched.
2. **Read filtering**: The read side excludes tombstoned keys when building query results.
3. **Reconciliation**: When new data arrives for a previously-deleted key, the tombstone entry is removed so the new row becomes visible.
4. **Compaction**: When `len(deleted_keys) >= tombstone_compact_total`, all affected Parquet files are rewritten with the tombstoned rows physically removed, and the tombstone list is cleared.

### Tombstone Storage Format

Tombstones are stored in the snapshot JSON:

```json
{
  "tombstones": {
    "primary_keys": ["customer_id", "order_id"],
    "deleted_keys": [
      ["CUST-001", "ORD-100"],
      ["CUST-002", "ORD-200"]
    ],
    "total_tombstones": 2
  }
}
```

---

## Lineage Tracking

Every write records lineage metadata in the monitoring payload. Callers can pass a `lineage` dict with conventional keys:

| Key | Description |
|---|---|
| `source_type` | Origin type: `staging_ingest`, `pipe_transform`, `api_upload`, `spark_job`, `backfill`, `manual` |
| `source_id` | Identifier of the upstream source |
| `source_tables` | List of upstream table names |
| `source_query` | SQL/transform that produced this data |
| `staging_name` | Staging area name (ingest path) |
| `pipe_name` | Pipe name (ingest path) |
| `job_id` | Batch job correlation ID |
| `run_id` | Batch run correlation ID |
| `source_files` | List of upstream file paths/URIs |
| `schema_version` | Version tag of the incoming schema |
| `tags` | Free-form dict for filtering/grouping |

If no lineage is provided, the writer auto-generates a minimal lineage dict with the role name, overwrite columns, and query ID.
