# Data reader

`DataReader` executes a read against the current catalog snapshots. It accepts SQL and a role name, applies access rules and deletion vectors, and returns either a materialized Polars DataFrame or an Arrow stream handle.

Implementation: [data_reader.py](../supertable/data_reader.py), [system_query.py](../supertable/system_query.py), [Arrow result conversion](../supertable/engine/arrow_result.py), [streaming jobs](../supertable/streaming/jobs.py), and [streaming runner](../supertable/streaming/runner.py). See [Query engine](09_query_engine.md) for planning and execution details and [RBAC](11_rbac.md) for role definitions.

## 1. Execute a query

```python
from supertable.data_reader import DataReader, Status
from supertable.engine.engine_enum import Engine

reader = DataReader(
    super_name="warehouse",
    organization="example_org",
    query="SELECT order_id, total FROM orders WHERE total >= 100 ORDER BY order_id",
)

frame, status, message = reader.execute(
    role_name="sales_reader",
    engine=Engine.DUCKDB,
)
if status is Status.ERROR:
    raise RuntimeError(message)

print(frame)
```

This example assumes the SuperTable, table, columns, and named role already exist. Construction selects the configured storage implementation. It does not create a missing target table.

### Constructor

```text
DataReader(super_name: str, organization: str, query: str, source: str = "sdk")
```

`source` labels the query in monitoring. The reader exposes `timer`, `plan_stats`, and `query_plan_manager` after the corresponding execution stages have run. A query rejected before planning may leave `query_plan_manager` unset.

### Materialized execution

```text
reader.execute(
    role_name: str,
    with_scan: bool = False,
    engine: Engine = Engine.AUTO,
    fullscan: bool = False,
) -> tuple[polars.DataFrame, Status, str | None]
```

| Argument | Behavior |
| --- | --- |
| `role_name` | Name of the role to resolve in this organization and SuperTable |
| `engine` | An `Engine` enum member; the method reads its `dialect` property |
| `fullscan` | Bypasses SuperTable's statistics-based file pruning |
| `with_scan` | Accepted by the current signature but not used in the execution body |

`Status.OK.value` is `"ok"`; `Status.ERROR.value` is `"error"`. A successful ordinary query normally returns `message=None`. Execution failures caught inside the main execution block return an empty DataFrame with `Status.ERROR` and the exception text.

Authorization and parser construction occur before that broad execution handler. Callers must also handle exceptions such as `PermissionError`, invalid parser inputs, and catalog failures during preflight; an error is not always represented by the status tuple.

`execute()` does not add a row limit. Materialization consumes the complete result into memory. Choose SQL `LIMIT` or `stream()` when the expected result is large.

The current Spark executor has a return-value defect that discards its stream handle, described in [Query engine](09_query_engine.md#5-spark-execution-and-current-limitation). Explicit DuckDB avoids that path; AUTO can select Spark when a registered cluster meets the estimated size threshold.

## 2. Accepted SQL and table preflight

An unqualified name such as `orders` uses the constructor's `super_name`. A qualified name such as `archive.orders` selects another SuperTable within the same organization. The reader checks each distinct physical target in Redis before estimating files:

- Missing root: `SuperTableNotFoundError`, returned as `Status.ERROR` with its message.
- Missing leaf: `TableNotFoundError`, returned as `Status.ERROR` with its message.
- Missing source columns or absent Parquet resources: execution error during estimation.

The read gate permits one read statement and rejects writes, multiple statements, and table functions. SELECT queries may contain CTEs, joins, subqueries, and set operations subject to the parser and selected engine. A query with no named source table fails parser validation.

The SQL role is supplied directly by the caller. `DataReader` does not authenticate a username, select a user's role, or verify a bearer token. An application exposing it must resolve the caller's permitted role before invoking this API.

## 3. EXPLAIN and SHOW STATS

Use materialized `execute()` for these commands.

```python
explain_reader = DataReader(
    super_name="warehouse",
    organization="example_org",
    query="EXPLAIN SELECT order_id FROM orders WHERE total >= 100",
)
plan, status, message = explain_reader.execute(
    role_name="sales_reader",
    engine=Engine.DUCKDB,
)
```

`EXPLAIN SELECT ...` and `EXPLAIN ANALYZE SELECT ...` pass through the same table preflight and RBAC checks as the inner query. `EXPLAIN` forces DuckDB execution even if another engine was requested. `ANALYZE` executes the query while gathering its explanation. `WITH` is also accepted after the EXPLAIN prefix.

```python
stats_reader = DataReader(
    super_name="warehouse",
    organization="example_org",
    query="SHOW STATS orders",
)
stats, status, message = stats_reader.execute(role_name="sales_reader")
```

`SHOW STATS [super.]simple` checks table existence and READ authorization, then loads the latest snapshot's `stats_file`. It accepts identifier quoting with double quotes or backticks. It does not use the ordinary SQL execution engine.

Statistics contain file paths, row-group IDs, column names, physical/logical types, typed minima/maxima, null counts, row-group row counts, compressed bytes, availability, and exactness flags. If no statistics are present, the command returns `Status.OK` with an empty DataFrame whose statistics columns are UTF-8 strings.

The current implementation checks read authorization but does not apply the returned row/column restriction views to statistics. Thus a restricted role that passes READ checks can receive raw statistics for columns or rows outside its query projection. Do not treat `SHOW STATS` as a row-filtered data result.

`stream()` does not implement these command results correctly: SHOW STATS produces no stream handle, and the streaming branch does not pass the EXPLAIN flag into the executor. Use `execute()` for both commands.

## 4. Read Arrow batches

```python
reader = DataReader(
    super_name="warehouse",
    organization="example_org",
    query="SELECT order_id, total FROM orders ORDER BY order_id",
)
with reader.stream(
    role_name="sales_reader",
    engine=Engine.DUCKDB,
    batch_rows=8192,
) as handle:
    schema = handle.schema
    for batch in handle.batches():
        print(batch.num_rows)
```

```text
reader.stream(
    role_name: str,
    engine=None,
    fullscan: bool = False,
    batch_rows: int = 0,
    expose_rowid: bool = False,
)
```

`engine=None` resolves to AUTO. `batch_rows=0` uses `SUPERTABLE_STREAM_BATCH_ROWS`, default `65536`. The handle provides `schema`, `batches()`, `cancel()`, `close()`, and context-manager cleanup. `batches()` closes resources in its `finally` block; explicit context management also cleans up when a consumer stops early.

`cancel()` interrupts the active cursor; `close()` releases resources. These are separate operations. A failed stream preparation raises `RuntimeError` rather than returning a status tuple. Exceptions may also arise while batches are consumed.

Streaming uses the same table, role, share-filter, and deletion-vector preparation as materialized execution. The ordinary public view hides `__rowid__` and `__timestamp__`. `expose_rowid=True` requests visibility of `__rowid__` in DuckDB's deletion-filtered view for service-level paging; RBAC's allowed-column view can still restrict it.

If an exception escapes the leaf getter during per-table deletion-vector lookup, the reader wraps it in `DeletionVectorUnavailable` and fails the read. An exception while establishing a share predicate uses `ShareFilterUnavailable`; both derive from `ReadAccessUnavailable`. A share predicate is combined with an existing role predicate using `AND`.

`RedisCatalog` converts Redis-specific errors in its leaf/root getters to `None`, so those failures do not necessarily reach the exception handling above. A failed control lookup after successful planning can therefore look like an absent payload and skip controls.

A path-only leaf is a related limitation: the estimator can recover resources from its snapshot file, but the control lookup reads tombstones and share predicates only from the embedded leaf payload. When that payload is absent, neither executor reloads the missing controls from the snapshot. The writer can publish a path-only leaf after a payload-publication failure, so that fallback can omit deletion/share filtering on a later read. See [implementation gaps](TODO.md).

The close callback writes monitoring using the number of consumed rows. It currently records an OK stream completion even when a consumer closes early, so that metric alone does not establish that every result row was consumed.

## 5. Convert results to rows and column metadata

```python
from supertable.data_reader import query_sql

query_info = {}
columns, rows, columns_meta = query_sql(
    organization="example_org",
    super_name="warehouse",
    sql="SELECT order_id, total FROM orders",
    limit=100,
    engine=Engine.DUCKDB,
    role_name="sales_reader",
    out=query_info,
)
```

```text
query_sql(
    organization: str,
    super_name: str,
    sql: str,
    limit: int,
    engine,
    role_name: str,
    source: str = "sdk",
    out: dict | None = None,
) -> tuple[list[str], list[list], list[dict]]
```

For ordinary SELECT queries the helper appends a default LIMIT unless a trailing numeric `LIMIT` with an optional numeric `OFFSET` is already recognized. It does not cap an existing limit. Its detection is textual, not a complete rewrite of arbitrary SQL; omit a trailing semicolon when relying on the appended limit because the current helper appends to the original SQL text.

The helper converts NaN values to nulls where Polars permits it and returns rows as lists. Each column metadata entry contains `name`, the string form of its Polars `type`, and `nullable=True`; nullability is not inferred. The optional `out` dictionary receives `query_id` and `query_hash` if planning occurred. `Status.ERROR` becomes `RuntimeError("Query execution failed: ...")`.

## 6. Persist a query stream as a job

The streaming package stores job metadata and chunk references in Redis and Arrow IPC stream chunks in the configured storage.

```python
from supertable.streaming.jobs import JobStore
from supertable.streaming.runner import submit_and_run, iter_job_batches

store = JobStore()
job = submit_and_run(
    organization="example_org",
    super_name="warehouse",
    sql="SELECT order_id, total FROM orders",
    role_name="sales_reader",
    deadline_sec=600,
    batch_rows=8192,
    store=store,
)

for batch in iter_job_batches("example_org", job.job_id, store=store):
    print(batch.num_rows)

finished = store.get("example_org", job.job_id)
print(finished.state if finished else "expired metadata")
```

`submit_and_run(..., background=True, deadline_sec=None, batch_rows=0, fullscan=False, store=None)` starts a daemon thread by default. `background=False` runs in the caller. The job runner invokes `DataReader.stream()` with AUTO; this helper currently has no engine argument, so the Spark limitation also applies when AUTO selects Spark.

Job states are `pending`, `running`, `done`, `failed`, `cancelled`, and `expired`. Chunk paths are `<organization>/_query_jobs/<job_id>/chunk-000000.arrow`, with zero-based chunk indices. The record tracks rows, serialized bytes, chunk count, acknowledgement position, owner, deadline, error, and serialized Arrow schema.

| Setting | Default | Behavior |
| --- | --- | --- |
| `SUPERTABLE_STREAM_CHUNK_BYTES` | `33554432` | Flush target based on accumulated batch size; chunks can exceed it by a batch |
| `SUPERTABLE_STREAM_MAX_AHEAD_CHUNKS` | `0` | Zero disables producer waiting for acknowledgements |
| `SUPERTABLE_STREAM_MAX_SPILL_BYTES` | `0` | Zero disables the stored-byte cap |
| `SUPERTABLE_STREAM_DEADLINE_SEC` | `3600` | Default job execution deadline |
| `SUPERTABLE_STREAM_JOB_TTL_SEC` | `3600` | Redis record/chunk-reference TTL, refreshed on updates |

`iter_job_batches(..., follow=True, poll_interval=0.1, timeout=None, acknowledge=True)` reads available chunks in order and follows running jobs. It acknowledges each consumed chunk by default. Failed jobs raise after their available chunks are consumed; cancelled and expired jobs end without that failure exception, so inspect the final job record when completeness matters.

`JobStore.cancel()` sets a cancellation marker. The runner checks cancellation and deadlines while consuming results and uses a watcher to interrupt the handle. Buffered batches not yet written as a chunk are discarded on cancellation; previously written chunks remain available.

`JobStore.delete(..., storage=storage)` attempts to delete referenced chunks and removes Redis records. `reap()` removes index entries for jobs whose records have expired. Redis TTL expiration does not itself delete storage objects, and once chunk references expire the reaper may no longer know their paths. These helpers do not implement role checks for reading or cancelling stored jobs; services must enforce job ownership/access before calling them.

## 7. OData service helpers

[odata/stream.py](../supertable/odata/stream.py) provides `query_odata_sql_stream()` around the reader, and [odata/policy.py](../supertable/odata/policy.py) computes role and effective-policy fingerprints from column restrictions and row predicates.

The OData helper can accept an expected effective-policy fingerprint; a mismatch raises `ODataPolicyChanged` before streaming. It supports a maximum total row limit, caller cancellation event, timeout, and continuation boundary. Boundaries contain ordered columns, directions, values, and a row identity used as a tie breaker. Missing row identity or null sort values are rejected.

The helper compares current policy and opens a current-snapshot query. This does not pin a snapshot across pages or guarantee a stable result while underlying data changes. [row_identity.py](../supertable/odata/row_identity.py) separately checks snapshot `rowid_high_watermark` metadata and live-row counts; the stream helper itself does not perform that identity check.

Current limits are visible in the implementation: the OData helper accepts `engine` but does not pass it to `DataReader.stream()`, and its selected-engine diagnostic can fall back to `duckdb` without evidence that DuckDB executed. Its timeout checks occur during batch iteration, after stream setup. Use these as service integration helpers, with paging order, authorization, identity checks, and snapshot policy established by the calling service.
