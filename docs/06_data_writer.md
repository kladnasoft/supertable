# Data writer

`DataWriter` accepts Arrow-compatible input, writes Parquet resources, and publishes a new table snapshot through Redis. Mutations are serialized per SimpleTable using a Redis lock.

Sources: [DataWriter](../supertable/data_writer.py), [processing helpers](../supertable/processing.py), [SimpleTable](../supertable/simple_table.py).

## Public API

```python
from supertable import DataWriter

writer = DataWriter(super_name="warehouse", organization="acme")
```

Construction creates the SuperTable if it is missing. The first write can also create a missing SimpleTable after access checks and lock acquisition.

```python
writer.write(
    role_name,
    simple_name,
    data,
    overwrite_columns,
    compression_level=1,
    newer_than=None,
    delete_only=False,
    lineage=None,
)
```

`data` is passed to `polars.from_arrow`; a `pyarrow.Table` is the straightforward input. The writer returns:

```text
(total_columns, total_rows, inserted, deleted)
```

`total_columns` is the incoming column count before system columns are added. `total_rows` equals rows inserted by this call, not the table's resulting row count. A delete-only call reports zero inserted and total rows. An all-stale `newer_than` call returns `(incoming_columns, 0, 0, 0)` without publishing a new snapshot.

## Append, replace matching keys, and delete

### Append

An empty overwrite-column list appends all incoming rows:

```python
import pyarrow as pa

result = writer.write(
    role_name="superadmin",
    simple_name="orders",
    data=pa.table({"order_id": [1, 2], "amount": [12.5, 18.0]}),
    overwrite_columns=[],
)
```

### Replace matching keys

A nonempty key list retires existing rows matching any incoming key and inserts the incoming rows:

```python
result = writer.write(
    role_name="superadmin",
    simple_name="orders",
    data=pa.table({"order_id": [2], "amount": [21.0]}),
    overwrite_columns=["order_id"],
)
```

Multiple columns form a composite match. Null keys are matched as equal by the matching joins. This is not a uniqueness constraint: the writer preserves multiple incoming rows with the same key, and each call may choose its own key list. Existing matching rows are hidden with tombstones rather than updated in place.

### Accept only newer values

```python
result = writer.write(
    role_name="superadmin",
    simple_name="events",
    data=pa.table({"event_id": [7], "revision": [3], "value": ["updated"]}),
    overwrite_columns=["event_id"],
    newer_than="revision",
)
```

For each matching key, the implementation computes the maximum existing value of `newer_than`. An incoming row survives when that maximum is null or the incoming value is strictly greater. Equal values are stale. With a non-null existing maximum, a null incoming value does not pass. The comparison uses the referenced physical candidate files before already-deleted matches are excluded from the new deletion pairs, so a still-present tombstoned row can affect the maximum.

`newer_than` must name an input column and requires overwrite columns. Incoming rows are compared with existing data, not with each other.

### Delete matching rows

```python
result = writer.write(
    role_name="superadmin",
    simple_name="orders",
    data=pa.table({"order_id": [2]}),
    overwrite_columns=["order_id"],
    delete_only=True,
)
```

The input supplies keys; it is not inserted. Rows already in the deletion vector are excluded from the reported new deletion count.

With `delete_only=True` and `overwrite_columns=[]`, the implementation scans existing resources for every row ID to delete. Input rows do not narrow this operation. For example, a typed empty Arrow table can be supplied to clear visible rows while retaining table metadata:

```python
result = writer.write(
    role_name="superadmin",
    simple_name="orders",
    data=pa.table({"order_id": pa.array([], type=pa.int64())}),
    overwrite_columns=[],
    delete_only=True,
)
```

The all-row scan skips missing/unreadable resources and files without `__rowid__`. A successful return therefore does not verify that every physical source file was readable or that all rows were deleted under those failure conditions.

## Validation and permissions

Writes require the role's `WRITE` permission for the target table and pass through the read-only guard. `role_name` is a role name, not a user ID or access token.

Validation rejects an empty or overlong table name, a name equal to the SuperTable name, characters outside the table-name pattern, an overwrite-column argument that is a string, missing overwrite columns, and invalid `newer_than` arguments. Redis key construction further restricts names: ordinary SimpleTable names must be lowercase, start with a letter, contain only letters/digits/underscores, and be at most 64 characters. Internal double-underscore names use the separate form documented in [Redis layout](16_redis_layout.md). The method does not enforce a complete input type/schema contract, uniqueness, or primary-key declaration.

The writer overwrites incoming `__rowid__` and `__timestamp__` columns for inserted rows. Reserve those names for system use. See [data model](03_data_model.md) and [RBAC](11_rbac.md).

## Write sequence

1. Check access, convert Arrow input to Polars, and validate arguments.
2. Reserve table row IDs for non-delete input and assign the write timestamp. Read the cached table configuration.
3. Acquire the SimpleTable lock with a 30-second lease and up to 60 seconds of waiting. Open or create the table and read its current snapshot.
4. Identify candidate resources. With overwrite keys, use stored bounds to remove impossible candidates, then resolve existing matches and optional version comparisons.
5. Load the previous deletion vector and exclude already-retired IDs from new deletion pairs.
6. Write new data and tombstone artifacts. When data is inserted, those two branches run concurrently in a two-worker executor.
7. Reclaim fully dead files when new deletion pairs produced a combined vector. Apply tombstone and small-file compaction when their thresholds are reached.
8. Rebuild statistics for the resulting live resources and write the next snapshot JSON, predecessor pointer, lineage, and row-ID watermark.
9. Check lock ownership, publish the Redis leaf payload (falling back to a path-only leaf if publication raises), then increment the root version.
10. Refresh the schema/table-name cache and invoke configured mirrors. Release the lock in `finally`.
11. Enqueue monitoring, notify quality scheduling, and emit the audit event through separate best-effort calls.

The Redis leaf update, root increment, storage writes, mirrors, and observability calls are not one transaction. Errors before publication can leave unreferenced files. A failure after leaf publication can leave visible data even if the call raises. Mirror and observability failures are logged or suppressed after the native data update.

The lock manager renews acquired leases with a background heartbeat. The writer's final ownership check raises `LockLostError` only when it receives an explicit `False`; the normal verifier returns `False` on Redis errors. If a verifier itself raises, the writer logs the exception and publication proceeds. Ownership verification and publication are separate calls. See [locking](08_locking.md) and [catalog](05_redis_catalog.md).

## Overwrite matching and Parquet output

All existing resources begin as candidates when overwrite keys are supplied. A statistics artifact can eliminate files whose stored ranges cannot match the input. Missing or unsupported bounds retain the candidate.

If `SUPERTABLE_DUCKDB_WRITE_PROBE` is enabled, matching first tries a DuckDB scan projecting the keys, row ID, optional comparison column, and filename. It joins against distinct incoming keys and resolves storage paths, with a presigned-URL retry for selected failures. Unavailable or failed probing falls back to projected Polars reads.

New Parquet data uses Zstandard, the requested compression level, statistics, and row groups of 122,880 rows. Ordinary writes pass no overwrite sort columns to the file writer; sorting is by `__timestamp__` when present. A write does not split its incoming frame into bounded-memory chunks using `max_memory_chunk_size`: that setting chiefly controls compaction decisions and grouping.

## Tombstones and automatic compaction

Tombstones contain `(file, __rowid__)` pairs. New deletions normally add a Parquet part; when adding a part would exceed `SUPERTABLE_TOMBSTONE_MAX_PARTS`, the parts are checkpointed into one combined artifact. The default limit is 100 parts.

A resource whose deleted count reaches its physical row count is removed from the next snapshot, along with unnecessary tombstone entries. This is logical reclamation: the old data object is retained.

The small-file cutoff is `max(1, int(max_memory_chunk_size * 0.75))`. Automatic small-file compaction triggers when the count of files below that cutoff reaches `max_overlapping_files`, or their combined stored bytes exceed `max_memory_chunk_size`. Tombstone compaction triggers when the current combined deletion vector reaches `max_tombstone_rows`; the small-file trigger can also cause tombstones to be drained first.

Compaction reads and rewrites selected live rows, preserves row IDs, unions frame schemas, and removes replaced resources from the next snapshot. Grouping uses stored file sizes, so the memory setting is not a hard limit on decoded memory consumption. Small-file reads can be skipped by the helper on missing/unreadable files; source data is not universally validated as part of compaction.

## Per-table settings

```python
writer.configure_table(
    role_name="superadmin",
    simple_name="orders",
    max_memory_chunk_size=32 * 1024 * 1024,
    max_overlapping_files=64,
    max_tombstone_rows=500_000,
)
```

All supplied values must be positive. Defaults come from process settings: 16 MiB, 100 files, and 1,000,000 tombstone rows respectively. Configuration is stored in Redis and cached on each `DataWriter`. Changes made elsewhere are not automatically reloaded by an existing writer instance.

Calling `configure_table` without values still stores the instance's cached configuration, or `{}` when uncached; it is not a read-only operation. See [configuration](02_configuration.md).

## Explicit compaction

```python
summary = writer.compact(
    role_name="superadmin",
    simple_name="orders",
    force_tombstones=True,
    small_only=True,
    compression_level=1,
    lineage={"source_type": "maintenance"},
)
```

The target root and table must already exist. `small_only=False` considers all resources for rewriting. Any nonempty deletion vector is processed first; **`force_tombstones=False` does not disable that phase in the current implementation**. The argument is recorded in the result and lineage but does not control the branch.

The returned dictionary contains query/actor/table identity, requested options, `files_before`, `files_after`, `files_compacted`, `tombstone_rows_removed`, `tombstone_files_rewritten`, `new_resources`, `sunset_files`, `total_rows_written`, `duration`, `lineage`, and `timings`. `total_rows_written` comes from the resource-compaction phase, not a final logical table count. If nothing is rewritten or retired, no new snapshot is published.

## Error handling and operational boundaries

`write` and `compact` propagate their main exceptions, including validation, authorization, catalog/storage, timeout, and explicit lock-loss failures. They always attempt to release an acquired lock. Post-operation monitoring, quality notification, and audit emission do not determine success of the data commit.

These methods do not garbage-collect historic artifacts or provide multi-table transactions. Retry logic must account for uncertain outcomes after partial publication; the call has no caller-supplied idempotency key. For a complete example and handling the result tuple, see [Python SDK](15_python_sdk.md).
