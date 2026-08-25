# Data Writer

## Business Context

The Data Writer is the core write-path component of SuperTable. Every row that enters the data lake -- whether from an API upload, a staging area commit, or a pipe transformation -- flows through the `DataWriter` class. It turns raw Arrow tables into versioned, compressed, immutable Parquet files and publishes snapshot-pinned deletion vectors while maintaining snapshot isolation, catalog consistency, and optional mirroring to downstream formats.

The write pipeline is designed around three principles:

1. **Atomicity** -- a write either succeeds completely (new snapshot, updated catalog, optional mirror) or fails without side-effects. A Redis-backed per-table lock serialises concurrent writers.
2. **Idempotency** -- the `newer_than` parameter lets callers replay the same data safely; stale or duplicate rows are silently dropped.
3. **Merge on read** -- ordinary deletes and upserts append immutable data and record the exact replaced physical rows in a deletion vector. Physical rewrites happen at explicit or threshold compaction, where bounded packing separately caps decoded bytes, Arrow batches/rows, and pending Polars frames/rows.

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
- `self._table_config_cache` -- an in-process copy of per-table write and deletion-vector configuration used for observability; writes refresh the authoritative configuration under the table lease.

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
access_check --> convert --> validate --> system_ts --> lock --> snapshot
    --> overlap --> newer_than --> process --> update_simple
    --> bump_root --> mirror --> unlock --> monitoring
    --> audit
```

#### 1. Access Control (`access`)

Calls `check_write_access()` from the RBAC module to verify the role has write permission on the target table. Raises immediately if denied.

#### 2. Convert Input (`convert`)

Converts the incoming PyArrow table to a Polars `DataFrame` using `polars.from_arrow(data)`. Captures `incoming_rows` and `incoming_columns` for monitoring.

#### 3. Validation (`validate`)

The `validation()` method enforces structural invariants:

| Rule | Constraint |
|---|---|
| Table name length | 1--128 characters |
| Name collision | `simple_name != super_name` |
| Name pattern | `^[A-Za-z_][A-Za-z0-9_]*$` |
| Overwrite columns type | Must be a list, not a string |
| Overwrite columns presence | All columns must exist in the DataFrame |
| Delete-only mode | With `overwrite_columns`, deletes matching rows; without them, deletes the whole table |
| Newer-than guard | `newer_than` must be a valid column name string and `overwrite_columns` must be set |

#### 4. System Timestamp Injection (`dedup_ts`)

Every non-delete write injects the system-owned `__timestamp__` using the
current UTC time. A caller-supplied value is deliberately overwritten. The
column drives date partitioning and internal layout; `newer_than` is the
supported caller-controlled conflict-resolution column. Readers do not run a
key-based `ROW_NUMBER()` collapse.

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

`resolve_overwrite_writes()` probes the overlapping candidates once. It first
applies the predecessor deletion vector, then compares live existing rows with
the incoming rows on `overwrite_columns`. When `newer_than` is specified, an
incoming row whose existing value is greater than or equal to its value is
dropped as stale/replayed. If all rows are stale, the write short-circuits (no
new data or deletion-vector artifact) but still releases the lock and emits
monitoring. The same probe returns the exact live physical rows replaced by the
surviving input.

#### 9. Deletion-Vector Resolution

Deletes are physical-row decisions, not persistent business-key markers.
`resolve_overwrite_writes()` returns the live `(file, __rowid__)` pairs matched
by each targeted delete or surviving upsert, after excluding rows already in
the predecessor vector. A pure append creates no deletion-vector entries.

- A targeted delete publishes the matched pairs and writes no data row.
- An upsert publishes those pairs and writes each surviving incoming row with a
  fresh `__rowid__` in a new immutable data file.
- A reinsertion never removes the predecessor entry: the old physical row stays
  deleted while the fresh physical identity remains visible.
- A whole-table delete sunsets every current data resource and clears the
  vector rather than constructing an O(rows) intermediate artifact.

The vector is sealed with its row count and digest. Its logical rows contain
`__file__` and `__rowid__`; readers anti-join on both values, so equal row IDs
from different files cannot hide each other.

#### 10. Immutable Publication and Compaction (`process`)

The data-file write and deletion-vector publication use disjoint locations and
may run concurrently. With no new deleted pairs, the snapshot reuses the
predecessor vector. Otherwise the writer publishes an immutable successor:
formats 1 and 3 use a direct Parquet artifact, while format 2 uses a sealed JSON
manifest plus immutable Parquet segments.

Ordinary writes do not rewrite every overlapping source file. When the vector
reaches `max_tombstone_rows` (default 1,000,000), targeted compaction physically
anti-joins its recorded row IDs from the named data files, writes survivor
files, sunsets only the successfully replaced resources, and publishes any
residual vector entries. Explicit compaction drains an active vector regardless
of that lazy write threshold.

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
4. **Deletion-vector compaction** — drains every active deletion vector before
   small-file compaction, physically removing the identified rows from affected
   Parquet files. `force_tombstones` is retained for API and lineage
   compatibility, but explicit `compact()` drains regardless of that flag or
   `max_tombstone_rows`. It is skipped only when no active vector exists.
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

When `compact_resources` finds nothing to merge **and** the deletion-vector
drain produces no work, the method returns early
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
    max_memory_chunk_size: int | None = None,
    max_decoded_compaction_bytes: int | None = None,
    max_overlapping_files: int | None = None,
    max_tombstone_rows: int | None = None,
    tombstone_compaction_workers: int | None = None,
    deletion_vector_format: int | None = None,
    *,
    confirm_dv_v2_reader_fleet: bool = False,
    confirm_dv_v3_reader_fleet: bool = False,
) -> None
```

Persists table-level configuration in Redis via `catalog.set_table_config()`. The configuration controls:

| Parameter | Default | Purpose |
|---|---|---|
| `max_memory_chunk_size` | 16 MB | Legacy name for the compressed source-byte packing/output target. Decoded memory is governed separately by `max_decoded_compaction_bytes`; one unusually wide row is its indivisible lower bound. Metadata is bounded to 4,096 Arrow batches/65,536 rows per storage emission and 128 Polars frames/1,048,576 rows per concat. |
| `max_decoded_compaction_bytes` | derived | Optional hard decoded-frame budget apportioned across the coordinator and bounded encoder slots. If unset, the writer derives a bounded budget from the encoded target (12× per retained lane), capped at 1 GiB and one quarter of the detected cgroup-v2/host-memory boundary; an unknown boundary uses a conservative 128 MiB cap. An explicit positive value remains authoritative. |
| `max_overlapping_files` | 100 | File-count threshold that triggers compaction of small files |
| `max_tombstone_rows` | 1,000,000 | Maximum deletion-vector rows before physical compaction is triggered |
| `tombstone_compaction_workers` | 2 | Bounded worker count for independent tombstone rewrites; valid range 1–8 |
| `deletion_vector_format` | legacy | `2` selects segmented manifests; `3` selects one immutable Parquet per snapshot. Both require the matching reader-fleet confirmation and writer environment gate. |

Configuration is persisted in Redis. A process-local copy is retained for
observability, but every write refreshes the authoritative configuration after
acquiring the table lease so cross-process changes cannot be hidden by stale
cache state.

### Deletion-vector v2 activation gate

DV-v2 publication is a coordinated deployment gate, not an ordinary tuning
switch. Before activating a table, deploy a v2-capable reader fleet and every
non-query consumer (recovery, mirroring, metadata, and quality), and update the
external retention/garbage collector to expand
`supertable.utils.snapshot.referenced_snapshot_artifacts()` so it retains the
JSON manifest **and every segment**. A collector that retains only the manifest
can permanently delete live deletion-vector segments.

After those checks, enable the local writer gate
`SUPERTABLE_DV_V2_WRITES_ENABLED=true` and persist the two durable table-config
fields together: `deletion_vector_format=2` and
`dv_v2_reader_fleet_confirmed=true`. Partial, coerced, or contradictory pairs
are rejected. Do not roll old readers or the old GC back into service while
any retained snapshot can reference format 2; removing the config does not
rewrite already-published snapshots.

### Immutable single-file deletion-vector v3

Format 3 retains one tombstone Parquet referenced by each snapshot. Ordinary
delete/upsert writes treat the pinned predecessor as immutable, validate only
the newly derived `(file, rowid)` delta, and encode one successor Parquet. The
normal writer reuses the overwrite resolver's proof that these exact rows were
selected after applying the predecessor vector; unproved/internal callers use
a native row-ID semi-join as a conservative fallback.
It does **not** sort, validate, or logically hash the complete million-row union
again. A SHA-256 fingerprint is taken over the Parquet byte buffer in one bulk
native call while that buffer is already in memory; this is object-identity
checking, not another logical row scan. Cold recovery verifies the pinned exact
bytes, while physical compaction paths that subtract arbitrary old entries
retain full logical validation.

Enable the process gate `SUPERTABLE_DV_V3_WRITES_ENABLED=true`, then atomically
configure the table with `deletion_vector_format=3` and
`confirm_dv_v3_reader_fleet=True`. Format 3 is a reader-fleet boundary: old
readers must not be reintroduced while a retained snapshot references it.
Legacy v1 and segmented v2 snapshots keep their existing semantics.

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

## Deletion-Vector System

The code retains `tombstone` in metadata and helper names for compatibility,
but the stored object is a physical deletion vector. Each row identifies an
exact immutable source row by `(__file__, __rowid__)`; no primary-key tuple is
persisted as a future delete rule.

### Key Functions

| Function | Purpose |
|---|---|
| `resolve_overwrite_writes(...)` | Applies the predecessor vector, filters stale input, and returns the exact live `(file, __rowid__)` pairs replaced by surviving keys |
| `identify_deleted_rowids(...)` | Conservative projected fallback that derives matching physical pairs from candidate files |
| `build_tombstone_file(...)` | Builds the legacy format-1 immutable Parquet successor |
| `build_tombstone_v2(...)` | Appends a sealed Parquet delta segment and publishes an immutable JSON manifest |
| `build_tombstone_v3(...)` | Builds one immutable single-Parquet successor from the pinned predecessor and proved delta |
| `compact_tombstones(...)` | Physically removes recorded rows from their named files and returns any entries that could not be safely consumed |

### Lifecycle

1. **Resolve**: A targeted delete or upsert maps its surviving business-key
   matches to live physical `(file, __rowid__)` pairs after applying the
   predecessor vector.
2. **Publish**: Those pairs extend an immutable deletion-vector artifact. A
   changed vector therefore performs Parquet and/or manifest storage I/O; it is
   not an inline metadata key list.
3. **Read**: The reader validates the snapshot-pinned path, row count, digest,
   format, and resource membership, then anti-joins on the composite physical
   identity.
4. **Reinsert**: New data receives a fresh row ID. The old entry remains in the
   vector and cannot hide the new physical row, even when the business key is
   identical.
5. **Compact**: `write()` drains when the vector reaches
   `max_tombstone_rows` (default 1,000,000); explicit `compact()` always drains
   an active vector. Only successfully rewritten source groups are removed
   from the vector, so failures cannot resurrect rows.
6. **Delete all**: A whole-table delete sunsets every data resource and clears
   the vector without constructing an O(rows) artifact.

### Tombstone Storage Format

The snapshot stores an immutable artifact pointer and its seals at top level:

```json
{
  "tombstone": "acme/warehouse/orders/tombstone/deleted_abcd.parquet",
  "tombstone_rows": 2,
  "tombstone_digest": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "tombstone_format": 1
}
```

For formats 1 and 3, `tombstone` names a direct Parquet object whose logical
schema is:

```json
{"__file__": "acme/warehouse/orders/data/part-0001.parquet", "__rowid__": 123}
```

Format 2 instead points to a sealed JSON manifest containing bounded immutable
Parquet segment descriptors. Every snapshot pins the exact root and segment
identities it reads; artifacts are never mutated in place.

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
