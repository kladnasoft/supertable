# Data Island Core — Data Writer

## Overview

The data writer is the pipeline that takes incoming data (a PyArrow table), validates it, resolves overlaps with existing data, writes Parquet files to storage, updates the snapshot in Redis, and triggers downstream systems (monitoring, data quality, audit logging, mirroring). Every mutation to a table — append, overwrite, delete — flows through `DataWriter.write()` in `data_writer.py`.

The write path is designed for correctness under concurrency. A per-table Redis lock ensures that only one write can modify a table at a time. The lock is held only during the critical section (snapshot read → file write → pointer update). Monitoring, quality checks, and audit logging happen after the lock is released.

---

## How it works

### Write pipeline — step by step

The `write()` method executes these steps in order:

**1. Access control** — Calls `check_write_access()` from the RBAC module. Verifies the role has write permission on the target table. Raises `PermissionError` if denied.

**2. Convert input** — Converts the PyArrow table to a Polars DataFrame for processing. Records incoming row and column counts.

**3. Dedup timestamp injection** — If the table has `dedup_on_read` enabled in its config, injects a `__timestamp__` column (current UTC time) if not already present. This column is used by the dedup-on-read `ROW_NUMBER()` window to determine which row is "newest."

**4. Validate** — Checks table name format (alphanumeric + underscore, 1–128 chars), verifies overwrite columns exist in the dataframe, validates `newer_than` and `delete_only` constraints.

**5. Acquire lock** — Acquires a per-table Redis lock (`SET NX` with 30-second TTL, 60-second timeout). If the lock cannot be acquired within the timeout, raises `TimeoutError`. Only one writer can hold the lock at a time.

**6. Read current snapshot** — Reads the current snapshot from the Redis leaf pointer (or falls back to storage if the payload is not cached). This is the table's state before the write.

**7. Detect overlaps** — Calls `find_overlapping_files()` from `processing.py`. Scans the snapshot's file manifest to find Parquet files whose key columns overlap with the incoming data. Returns a set of file paths that must be rewritten.

**8. Newer-than filtering** (optional) — If `newer_than` is set, reads the overlapping files and drops incoming rows where the `newer_than` column value is less than or equal to the existing value. This prevents replayed or stale data from overwriting newer data.

**9. Process data** — Depending on the operation:
   - **Append (no overwrite columns):** Writes the incoming data as new Parquet files. No existing files are touched.
   - **Overwrite:** Reads overlapping files, merges with incoming data (incoming rows replace existing rows on matching key columns), writes merged results as new files, and marks old files for removal.
   - **Delete-only with tombstones:** If `dedup_on_read` is enabled, records deleted key values in the snapshot's tombstone list (soft delete). No files are rewritten unless the tombstone count exceeds the compaction threshold.
   - **Delete-only without tombstones:** Reads overlapping files, removes matching rows, writes remaining rows as new files.

**10. Update snapshot** — Calls `simple_table.update()` which builds a new snapshot JSON (updated resource list, incremented version, new schema, previous snapshot link) and writes it to storage.

**11. Update Redis pointer** — Atomically updates the Redis leaf pointer to reference the new snapshot. Also caches the snapshot payload in Redis for fast reads. Bumps the root version counter.

**12. Mirror** — If mirroring is enabled for this SuperTable, writes the updated table in Delta/Iceberg/Parquet mirror format.

**13. Release lock** — The per-table Redis lock is released in a `finally` block.

**14. Post-lock operations** — After the lock is released (so writes are never blocked by downstream I/O):
   - **Monitoring:** Enqueues a metrics payload to the MonitoringWriter.
   - **Data quality:** Notifies the quality scheduler of the new data via `notify_ingest()`.
   - **Audit:** Emits a `data_write` audit event.

### Timing instrumentation

Every step is timed via the internal `mark()` helper. The complete timing breakdown is logged at INFO level:

```
Timing(s): total=1.234 | convert=0.001 | dedup_ts=0.000 | validate=0.001 |
lock=0.050 | snapshot=0.002 | overlap=0.100 | newer_than=0.000 |
process=0.800 | update_simple=0.050 | bump_root=0.005 | mirror=0.100
```

---

## Parameters

The `write()` method signature:

```python
def write(
    self,
    role_name: str,            # RBAC role for access control
    simple_name: str,          # Target table name
    data: pa.Table,            # Incoming data (PyArrow table)
    overwrite_columns: list,   # Key columns for overlap detection (empty = append)
    compression_level: int = 1,  # Parquet compression level
    newer_than: str = None,    # Column name for staleness filtering
    delete_only: bool = False, # Delete matching rows instead of inserting
    lineage: dict = None,      # Upstream origin metadata for monitoring
) -> Tuple[int, int, int, int]:
    # Returns: (total_columns, total_rows, inserted, deleted)
```

### Overwrite columns

When `overwrite_columns` is a non-empty list, the writer performs a key-based merge:

- Incoming rows with keys matching existing rows **replace** the existing rows
- Incoming rows with new keys are **appended**
- Existing rows with no matching incoming rows are **preserved**

This is the mechanism for upsert operations. The overwrite columns define the composite key.

### newer_than

The `newer_than` parameter names a column (typically a timestamp) that determines row freshness. When set, incoming rows are dropped if their `newer_than` value is less than or equal to the existing row's value for the same key. This prevents replayed data from overwriting newer updates.

Requires `overwrite_columns` to be set (needs keys to match rows).

### delete_only

When `True`, the incoming data is treated as a deletion manifest — rows matching the overwrite columns are removed from the table. Two paths exist:

- **Tombstone path** (when `dedup_on_read` is enabled): Key values are appended to the snapshot's tombstone list. No files are rewritten. Fast, O(1) regardless of table size. Tombstones are compacted when their count exceeds a configurable threshold.
- **Physical delete path** (when `dedup_on_read` is disabled): Overlapping files are read, matching rows are removed, and remaining rows are written back. Slower, proportional to overlap set size.

### lineage

An optional dict describing where the data came from. Stored in the monitoring stats payload for data lineage tracing. Common keys: `source_type`, `source_id`, `staging_name`, `pipe_name`, `job_id`.

---

## Overlap detection

`find_overlapping_files()` in `processing.py` determines which existing Parquet files contain rows that overlap with the incoming data on the overwrite columns.

The algorithm:

1. Extract distinct key column values from the incoming dataframe
2. For each file in the current snapshot's resource list, read only the key columns from the Parquet file
3. Check if any key values intersect with the incoming keys
4. Return the set of overlapping file paths

Optimization: `MAX_OVERLAPPING_FILES` (default: 100) limits how many files are checked. If a table has more files than this threshold, only the most recent files are scanned. This prevents overlap detection from becoming a bottleneck on large tables.

---

## Tombstone management

Tombstones are soft deletes for tables with `dedup_on_read` enabled. Instead of rewriting Parquet files to remove rows, the deleted key values are recorded in the snapshot metadata.

```json
"tombstones": {
  "primary_keys": ["order_id"],
  "deleted_keys": [[101], [202], [303]],
  "total_tombstones": 3
}
```

At read time, the query engine adds a `WHERE order_id NOT IN (101, 202, 303)` filter.

**Compaction:** When the tombstone count exceeds the threshold (configurable via table config, default defined in `_tombstone_threshold()`), the writer triggers compaction: all data files are rewritten without the tombstoned rows, and the tombstone list is cleared.

**Reconciliation:** When new data is written with keys that match existing tombstones, those tombstones are removed — the new rows "resurrect" the deleted keys.

---

## Validation rules

The `validation()` method enforces:

- Table name is 1–128 characters, starts with a letter or underscore, contains only alphanumeric and underscore characters
- Table name does not match the SuperTable name (would cause path conflicts)
- `overwrite_columns` is a list (not a string)
- All overwrite column names exist in the incoming dataframe
- `delete_only` requires `overwrite_columns` (needs keys to identify rows)
- `newer_than` must be a column name present in the dataframe, and requires `overwrite_columns`

---

## Locking

Each table has its own Redis lock key: `supertable:{org}:{sup}:lock:{simple}`. The lock is a Redis key set with `NX` (only if not exists) and `EX` (TTL of 30 seconds).

The lock token is a random UUID. Only the holder can release the lock — `release_simple_lock()` uses a Lua script to atomically check-and-delete the key only if the value matches the token.

If the lock holder crashes or the process is killed, the lock expires after 30 seconds (the TTL). Other writers will be able to acquire the lock after expiry.

See the Locking documentation for full details.

---

## Configuration

| Setting | Default | Description |
|---|---|---|
| `MAX_MEMORY_CHUNK_SIZE` | 16 MB | Maximum chunk size for in-memory processing |
| `MAX_OVERLAPPING_FILES` | 100 | Maximum files scanned during overlap detection |
| `DEFAULT_LOCK_DURATION_SEC` | 30 | Per-table write lock TTL |
| `DEFAULT_TIMEOUT_SEC` | 60 | Lock acquisition timeout |

Per-table configuration (stored in Redis via `set_table_config()`):

| Field | Description |
|---|---|
| `dedup_on_read` | Enable dedup-on-read (ROW_NUMBER window at query time) |
| `primary_keys` | Key columns for dedup and tombstone operations |
| `max_memory_chunk_size` | Per-table override for chunk size |
| `max_overlapping_files` | Per-table override for overlap scan limit |

---

## Module structure

```
supertable/
  data_writer.py       DataWriter class — write pipeline, validation (565 lines)
  processing.py        Overlap detection, merge, delete, tombstones, Parquet I/O (1,069 lines)
  simple_table.py      SimpleTable entity — snapshot read/update (269 lines)
  locking/
    redis_lock.py      Redis-based distributed locks
    file_lock.py       File-based fallback locks
```

---

## Frequently asked questions

**Can multiple processes write to the same table concurrently?**
No. The per-table Redis lock ensures mutual exclusion. Concurrent writes to *different* tables within the same SuperTable are fully parallel — each table has its own lock.

**What happens if the writer crashes while holding the lock?**
The lock has a 30-second TTL. After expiry, the next writer can acquire it. The Redis leaf pointer is unchanged (the last successful write's snapshot is still current), so no data corruption occurs. Orphaned Parquet files from the partial write may exist on storage.

**How large can a single write be?**
Limited by available memory. The incoming PyArrow table is converted to a Polars DataFrame in memory. For very large writes, use the ingestion pipeline (which handles chunking) rather than calling `DataWriter` directly.

**Does the writer support transactions across multiple tables?**
No. Each `write()` call is atomic for a single table. There is no cross-table transaction mechanism.

**How does the write path interact with RBAC?**
`check_write_access()` is called before any data processing. If the role doesn't have write permission on the target table, the write fails with `PermissionError` before any lock is acquired or data is read.

**What Parquet compression is used?**
Snappy by default (PyArrow's default). The `compression_level` parameter controls the compression effort but does not change the codec.
