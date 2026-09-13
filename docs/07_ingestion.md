# Ingestion, staging, and result streaming

The ingestion building blocks are direct table writes, Parquet staging, and Redis pipe definitions. The current package does not include a worker that executes pipe definitions or automatically transfers staged files into tables. The `streaming` package runs SQL queries and stores their results in Arrow chunks.

Implementation: [staging_area.py](../supertable/staging_area.py), [super_pipe.py](../supertable/super_pipe.py), [streaming/jobs.py](../supertable/streaming/jobs.py), and [streaming/runner.py](../supertable/streaming/runner.py).

## 1. Write directly to a table

Use `DataWriter.write(role_name, simple_name, data, overwrite_columns, compression_level=1, newer_than=None, delete_only=False, lineage=None)` to publish data immediately. Staging is optional. Writes return `(column_count, row_count, inserted, deleted)`; overwrite keys and deletion behavior are described in [writing data](06_data_writer.md).

```python
import pyarrow as pa
from supertable import DataWriter, SuperTable

lake = SuperTable(super_name="warehouse", organization="acme")
writer = DataWriter(super_name="warehouse", organization="acme")
writer.write(
    role_name="superadmin",
    simple_name="orders",
    data=pa.table({"order_id": [1, 2], "amount": [12.5, 30.0]}),
    overwrite_columns=["order_id"],
)
```

Use an existing role with write access to the target table. The examples use the built-in `superadmin` role for a locally administered lake.

## 2. Create or open staging

```python
from supertable import Staging

stages = Staging(organization="acme", super_name="warehouse")
stage = stages.open("incoming_orders")
filename = stage.save_as_parquet(
    role_name="superadmin",
    arrow_table=pa.table({"order_id": [3], "amount": [8.0]}),
    base_file_name="orders.parquet",
)
print(stage.list_files(role_name="superadmin"))
print(stages.get_directory_structure(role_name="superadmin"))
```

The constructor is keyword-only:

```text
Staging(*, organization, super_name=None, super_table=None, staging_name=None)
```

Provide a name or a `SuperTable` object. The root must already exist in Redis. Omitting `staging_name` creates a manager; call `open(name)` before saving, listing, or deleting files. Opening a named stage initializes its directory, Redis metadata, and index if absent.

The storage layout is relative to the configured storage backend:

```text
acme/warehouse/staging/
  incoming_orders/
    orders_<time_ns>.parquet
  incoming_orders_files.json
```

`save_as_parquet` returns a filename, not a full path. It accepts `source="upload"`, `duration_ms=0`, `pipe_name=""`, and `pipe_id=""`. The JSON index records filename, nanosecond write time, row count, source, rounded duration, pipe metadata, and `status="ok"`. It is the source for `list_files`; that method does not scan storage for unindexed files.

Saving and deleting require write access on `table_name="*"`; listing files and directory structure require metadata access on `"*"`. Stage initialization itself has no role parameter. Initialization, saving, and deletion use a Redis `SET NX EX` lock with a 30-second lease and token-checked release. A busy stage raises `RuntimeError`; this lock has no lease renewal. Writing a file and rewriting its JSON index are separate storage operations.

`stage.delete(role_name=...)` deletes the stage directory, its index, and staging metadata, including pipe subkeys. It does not delete destination tables.

## 3. Store a pipe definition

```python
from supertable import SuperPipe

pipes = SuperPipe(
    organization="acme",
    super_name="warehouse",
    staging_name="incoming_orders",
)
reference = pipes.create(
    role_name="superadmin",
    pipe_name="load_orders",
    simple_name="orders",
    user_hash="loader_identity",
    overwrite_columns=["order_id"],
    enabled=True,
)
print(reference)
print(pipes.read("load_orders", role_name="superadmin"))
pipes.set_enabled("load_orders", False, role_name="superadmin")
```

Construction requires the stage's Redis metadata to exist. `create` stores `staging_name`, `pipe_name`, `user_hash`, `simple_name`, `overwrite_columns`, `transformation=[]`, `updated_at_ns`, and `enabled`. It returns `redis://<organization>/<super_name>/<staging_name>/<pipe_name>` as an identifier.

Creation checks write access for both the new destination and any destination already associated with that pipe name. Changing `enabled` and deleting check write access for the stored destination; reading checks metadata access. Missing destination metadata falls back to `"*"`. Pipe mutations share the stage lock key with staging, using a 10-second lease without renewal.

`create` rejects another pipe with the same destination and an equal overwrite-column configuration. The comparison uses the supplied value before storing `None` as `[]`, so omitted and empty overwrite lists are not handled consistently by this duplicate check. Creating the same pipe name updates its definition. `delete(pipe_name, role_name)` returns a boolean; `read` and `set_enabled` raise `FileNotFoundError` for a missing pipe.

These methods manage definitions only. `enabled=True` does not start execution; no transformation execution, file acknowledgement, retry queue, or staged-file ingestion worker is implemented in this package.

## 4. Stream SQL results through stored jobs

The job runner executes `DataReader.stream` using the recorded role and writes Arrow IPC streams to storage. It is useful when consuming query results in batches across a process boundary.

```python
from supertable.streaming.jobs import JobStore
from supertable.streaming.runner import iter_job_batches, submit_and_run

store = JobStore()
job = submit_and_run(
    organization="acme",
    super_name="warehouse",
    sql="SELECT order_id, amount FROM warehouse.orders",
    role_name="superadmin",
    deadline_sec=300,
    store=store,
)
for batch in iter_job_batches("acme", job.job_id, store=store, timeout=300):
    print(batch.to_pydict())

finished = store.get("acme", job.job_id)
print(finished.state if finished else "metadata expired")
```

`submit_and_run(..., background=True, deadline_sec=None, batch_rows=0, fullscan=False, store=None)` creates Redis metadata and starts a daemon thread. With `background=False`, execution completes before returning. `JobStore.create` creates metadata without starting execution; `run_job(rec, store=None, storage=None, poll_every_batches=4, on_chunk=None)` runs a record explicitly. There is no distributed work queue or automatic ownership takeover.

Job states are `pending`, `running`, `done`, `failed`, `cancelled`, and `expired`. Metadata includes the SQL, role, owner, deadline, accumulated row/chunk/byte counts, acknowledgement count, and serialized Arrow schema encoded as hex in `schema_json`. Chunk references contain `index`, `path`, `rows`, and `bytes`. Chunk paths are:

```text
<organization>/_query_jobs/<job_id>/chunk-000000.arrow
```

| Setting | Default | Effect |
| --- | --- | --- |
| `SUPERTABLE_STREAM_BATCH_ROWS` | `65536` | Default engine batch size when `batch_rows=0`. |
| `SUPERTABLE_STREAM_CHUNK_BYTES` | `33554432` | Flush when accumulated Arrow batch memory reaches this target; one batch may overshoot. |
| `SUPERTABLE_STREAM_MAX_AHEAD_CHUNKS` | `0` | Producer waits for acknowledgements when nonzero; zero disables this limit. |
| `SUPERTABLE_STREAM_MAX_SPILL_BYTES` | `0` | Stops production after the configured spill threshold is exceeded; zero disables the check. |
| `SUPERTABLE_STREAM_DEADLINE_SEC` | `3600` | Default execution deadline; explicit `deadline_sec=0` removes it. |
| `SUPERTABLE_STREAM_JOB_TTL_SEC` | `3600` | Redis record and chunk-index expiration, refreshed by updates. |

`iter_job_batches(organization, job_id, ..., follow=True, poll_interval=0.1, timeout=None, acknowledge=True)` reads from chunk zero, polls for new chunks, and acknowledges each fully consumed chunk. `follow=False` reads available chunks and returns when caught up. The timeout is measured from iterator start, not reset after each chunk, and is checked while waiting without available chunks. Transient Redis errors have a bounded retry helper.

`store.cancel(organization, job_id)` sets a Redis cancellation flag. The runner checks deadlines while processing batches and also starts a one-second watcher that cancels the engine handle. Deadline and spill-limit cancellation set `expired`; explicit cancellation sets `cancelled`. Unflushed pending batches are discarded on cancellation. The iterator raises for `failed` after delivering published chunks, but returns normally for `cancelled` and `expired`; inspect the final job record to distinguish these outcomes.

The spill cap is checked after threshold-triggered flushes, not after the final remainder flush, and is not a strict storage quota. Acknowledgements advance a count; they do not delete chunks. Redis expiration does not delete object-storage data. Use `store.delete(organization, job_id, storage=...)` while chunk references still exist to attempt both kinds of cleanup. `reap` removes orphaned Redis index entries, but cannot recover paths after the chunk index has expired.

The low-level job store and chunk iterator do not perform role checks themselves. Query execution delegates access control to `DataReader`; applications exposing job lookup, cancellation, deletion, or chunk access must restrict those operations to the appropriate users. The module's `shutdown(timeout=5.0)` cancels tracked local jobs and is registered with `atexit`.
