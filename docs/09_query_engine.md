# Query engine

SuperTable reads the Parquet resources listed in current table snapshots. The read pipeline parses named SQL tables, checks access, estimates the selected files, creates engine views, and executes the rewritten SQL. The materialized public result is a Polars DataFrame; DuckDB can also return Arrow record batches.

Implementation: [executor](../supertable/engine/executor.py), [estimator](../supertable/engine/data_estimator.py), [SQL parser](../supertable/utils/sql_parser.py), [DuckDB](../supertable/engine/duckdb.py), and [Spark Thrift](../supertable/engine/spark_thrift.py). See [Data reader](10_data_reader.md) for the public API and [RBAC](11_rbac.md) for access rules.

## 1. Parse and resolve the query

`Engine` in `supertable.engine.engine_enum` has three values:

| Member | Value | Parser dialect |
| --- | --- | --- |
| `Engine.AUTO` | `auto` | DuckDB |
| `Engine.DUCKDB` | `duckdb` | DuckDB |
| `Engine.SPARK_SQL` | `spark_sql` | Spark |

The read gate first validates ordinary queries using the DuckDB SQL parser. It accepts one read statement, including SELECT queries with CTEs and set operations, and rejects writes and table functions such as direct `read_parquet(...)` calls. SQL must ultimately reference a named table: a table-free `SELECT 1` does not pass `SQLParser`.

An unqualified table name resolves inside the reader's `super_name`. `other_super.orders` resolves `orders` in `other_super`, using the same organization. CTE names are excluded from the physical table list. Repeated physical table references are merged for schema validation and resource estimation. Distinct aliases identify query views, so use explicit, distinct aliases for joins and nested references.

The parser collects source columns from projections and expressions, including predicates and join conditions. A wildcard is represented internally by an empty column list. This requests the full source schema; it does not represent an empty projection.

## 2. Read current snapshots and estimate resources

`DataEstimator(organization, storage, tables, predicate_constraints=None, plan_stats=None, fullscan=False)` scans the Redis leaf entries for each selected SuperTable. It uses a leaf's embedded snapshot payload when that payload contains a resource list; otherwise it reads the referenced snapshot file from storage. Resource entries supply Parquet paths and file sizes, and snapshot schemas supply known column names and types.

`estimate()` returns a `Reflection` containing:

- Storage implementation name, estimated bytes, total file count, and latest selected leaf update time.
- A `SuperSnapshot` for each selected table, including its version, files, and schema columns.
- Separate dictionaries to which the reader attaches RBAC and deletion-vector definitions.

Missing requested columns, no selected snapshots, or a selected table without Parquet files raise an execution error. An existing but resource-free table is therefore different from a populated table whose SQL predicate returns zero rows.

The estimator has an aggregation branch when a simple name equals its SuperTable name: it selects all non-internal leaves. The public reader still performs its ordinary root/leaf existence checks first; do not assume that a bare SuperTable name automatically forms a queryable union in every catalog state.

### Predicate pruning

Pruning is enabled by default through `SUPERTABLE_READ_PRUNING_ENABLED=true`. `fullscan=True` bypasses this SuperTable file-pruning step; the SQL engine still evaluates the original query and can perform its own scan optimizations.

The parser extracts direct column-to-literal comparisons (`=`, `<`, `<=`, `>`, `>=`), `BETWEEN`, and literal `IN` lists from conjunctions in SELECT `WHERE` clauses. An `IN` list becomes its minimum/maximum interval. Reversed comparisons are normalized. Numeric, boolean, string, and supported date/timestamp literals have separate comparison handling.

Unsupported expressions are left to SQL execution. For example, OR branches, functions applied to columns, column-to-column comparisons, and JOIN predicates do not supply these file constraints. An unqualified predicate is resolved only when its scope has one unambiguous source.

The pruner compares constraints with stored row-group statistics. It removes a file only when every table occurrence excludes all of that file's indexed row groups. A self-join or repeated CTE reference with an unrestricted occurrence prevents pruning for that physical table. Missing or unusable statistics retain the file. ASCII string ranges account for DuckDB's case-insensitive collation; non-ASCII values conservatively retain files. Timestamp comparisons widen bounds to account for session timezone uncertainty.

If pruning would remove every file, the implementation retains a subset covering the available schema. The SQL engine evaluates the original predicate against that subset and produces the empty result while still binding schema-dependent expressions. Statistics extraction, range handling, and this schema-covering fallback are implemented in [processing.py](../supertable/processing.py).

### Estimated bytes

With `SUPERTABLE_READ_PROJECTION_SIZING_ENABLED=true`, an explicit column projection reduces the size estimate. The estimator first uses selected columns' recorded `compressed_bytes`; when those are unavailable it approximates a proportion from schema type widths. Without projection sizing it sums complete surviving file sizes.

The estimate is a routing input, not measured peak memory or a billed byte count. It can differ from actual reads, particularly when system columns and columns needed only by row filters are added later.

Plan statistics include `REFLECTIONS`, `REFLECTION_SIZE`, and `REFLECTION_SIZE_RAW`. When pruning is enabled, they also include `FILES_BEFORE_PRUNE`, `FILES_PRUNED`, `FILES_KEPT`, `PRUNE_DURATION_MS`, and available pruning counters.

## 3. Select an engine

AUTO examines active Spark cluster registrations for the organization:

1. If none are active, it selects DuckDB.
2. Otherwise it compares the estimate with the minimum `min_bytes` among active clusters.
3. At or above that minimum it selects Spark; below it, DuckDB.

Spark's subsequent cluster selection also enforces each cluster's `max_bytes` when positive and selects randomly among eligible active clusters. AUTO's first decision does not check the maximum ranges, so it can select Spark and then fail because no cluster accepts that job size. It does not automatically retry the query on DuckDB.

Explicit `Engine.DUCKDB` bypasses routing. Explicit `Engine.SPARK_SQL` requests a cluster with `force=True` in both the materialized and the streaming executor, bypassing cluster byte ranges while still requiring an active cluster.

`engine_lite_max_bytes` and `engine_freshness_sec` are present in configuration but do not participate in `Executor._auto_pick()`. The current executor always uses the `lite` DuckDB configuration; it does not select a separate `pro` execution implementation.

## 4. DuckDB execution

DuckDB keeps a connection in thread-local state. Each query creates views over its selected Parquet files with `union_by_name=TRUE` and `HIVE_PARTITIONING=FALSE`, then runs on a cursor of that connection. A cursor sees the connection's views and what the connection's database holds, but none of the connection's session settings. The engine therefore configures object storage for the whole database when a query reads remote paths. The store's endpoint, region, URL style, TLS and credentials go into one temporary DuckDB secret, kept in memory, which every S3 read on any cursor signs with. The HTTP timeout is set globally, as the memory limit, thread count and caches already are. No setting holds a credential, so a query cannot read one with `current_setting()`. `duckdb_secrets()` is the only SQL that shows a secret, and it redacts the secret and session token; it is a table function, which the read gate refuses.

The view chain is:

```mermaid
flowchart LR
    P[Selected Parquet resources] --> V[Reflection view]
    V --> D[Deletion-vector anti join]
    D --> R[Allowed columns and row predicate]
    R --> Q[Rewritten user query]
    Q --> A[Arrow batches]
```

The reflection projection includes `__rowid__` and `__timestamp__` when needed for internal processing. A deletion vector removes row IDs through an anti join, and the public view normally hides both system columns. RBAC views then restrict rows and columns. Columns needed only to evaluate a row filter are available inside the filter and hidden from the resulting projection.

View names are derived from the table, snapshot version, and projection; query-specific suffixes distinguish control views. On handle closure the engine closes the Arrow reader and cursor, drops query views, and releases deletion-vector cache references. The shared connection remains available for later queries.

Deletion-vector tables are cached with a configured capacity and TTL, defaulting to 8 entries and 300 seconds. Referenced cache entries are released when the stream closes.

### Configuration

Organization engine configuration is resolved from Redis first, then environment variables, then defaults. Shared fields live at the configuration root; DuckDB fields live in the `lite` or `pro` section. `resolve_engine_config_provenance()` reports each field's source.

| Environment variable | Default | Use |
| --- | --- | --- |
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | `1GB` | DuckDB memory limit |
| `SUPERTABLE_DUCKDB_THREADS` | empty | Explicit thread count; otherwise derived from CPU, memory, and multiplier |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | `3` | Multiplier used by automatic thread sizing |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | empty | Optional positive HTTP timeout |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | `5GB` | Enables the external file cache and requests a size cap where supported |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | empty | Uses the application home's `duckdb_cache` directory where supported |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | `true` | HTTP metadata caching |
| `SUPERTABLE_DUCKDB_PRESIGNED` | `false` | Prefer storage presigned paths during estimation |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | `false` | Use constructed HTTP URLs instead of S3 URLs when resolving object-store keys |
| `SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD` | `false` | Permit installing `httpfs` if it is not already available |
| `SUPERTABLE_STREAM_BATCH_ROWS` | `65536` | Default Arrow batch row count |

Memory values accept positive numbers with units such as `MB`, `GB`, `MiB`, and `GiB`; a bare number is interpreted as GB. Invalid memory limits fall back to `1GB`. Thread derivation uses approximately one thread per 400 MB, capped by CPU count times the I/O multiplier.

Initialization sets the default collation to `nocase`, disables insertion-order preservation where supported, and configures a spill directory under the application home. Add `ORDER BY` when output order matters.

Remote S3 and HTTP reads require DuckDB's `httpfs` extension. Missing local extensions cause a clear runtime error unless online installation is enabled. Storage paths resolve through storage URL helpers where available. Certain HTTP/authentication failures during reflection creation trigger one retry using presigned paths. Optional DuckDB settings are applied only where supported, so a dedicated external-cache cap depends on the installed DuckDB build.

## 5. Spark execution

The Spark implementation uses PyHive to connect to a registered Thrift endpoint. Cluster configuration supplies `thrift_host`, `thrift_port` (default `10000`), `auth` (default `NONE`), and optional credentials. Missing PyHive or an eligible active cluster raises an error.

Each query opens its own Thrift session. The executor first SETs on it the S3 options the cluster registration carries (`s3_endpoint`, `s3_access_key`, `s3_secret_key`, `s3_region`, `s3_use_ssl`, `s3_path_style`), skipping any the server refuses; without them the server's launch configuration applies. It then sets `spark.sql.variable.substitute=false`. Spark otherwise expands `${...}` in SQL text from the session configuration, the server's environment and its JVM system properties, so `SELECT '${spark.hadoop.fs.s3a.secret.key}'` would return the object-store secret. That SET is not guarded: if the server refuses it, the query fails instead of running with substitution on.

The executor builds temporary Parquet views, combines multiple files with `UNION ALL`, applies deletion and RBAC views, rewrites SQL to Spark syntax, and attempts to capture `EXPLAIN EXTENDED`. S3 paths are converted to `s3a://`; optional presigning is controlled by `SUPERTABLE_SPARK_PRESIGNED`.

Both the materialized and the streaming executor ask Spark for a stream. `SparkThriftExecutor.execute()` then returns a `SparkStreamHandle`, which pulls rows with `fetchmany`, in batches of `SUPERTABLE_STREAM_BATCH_ROWS` or the `batch_rows` a stream asks for, and converts each batch to Arrow. The handle owns the query's cursor, Thrift session and temporary views, and drops and closes them once it is drained or closed. Called directly without a batch size, `execute()` fetches every row into a Polars DataFrame and tears down before it returns.

The handle fixes its Arrow schema from the result's metadata before the first batch, so a stream has a schema before any row arrives and every batch shares it. BOOLEAN, the integer types, FLOAT, DOUBLE, BINARY and TIMESTAMP keep their types, a TIMESTAMP as microseconds without a time zone; STRING, VARCHAR and CHAR are strings. Spark sends a DATE as text, which the handle parses into a date. A DECIMAL keeps the precision and scale the server reports for its column. PyHive leaves those out of the cursor description, so when the result has a DECIMAL the handle reads the result metadata again, and a DECIMAL column without them fails the read. A NULL-typed column, such as `SELECT NULL`, is an Arrow null column. Every other type is a string: Spark sends ARRAY, MAP and STRUCT values as JSON text and intervals as text such as `1 00:00:00.000000000`, where DuckDB returns nested and interval types.

`SUPERTABLE_SPARK_QUERY_TIMEOUT` (default 300 seconds) bounds a query once its session is open: the setup statements, the views, the `EXPLAIN`, the statement and the handle's read of the result metadata. `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` (default 120 seconds) bounds the `EXPLAIN` and the user statement, each on its own, and each batch a stream handle fetches. A stream's rows are fetched after `execute()` returns, at the consumer's pace, so only that per-batch bound applies to them; called without a batch size, `execute()` fetches inside the query timeout. When a timeout fires, the connection's socket is shut down, which fails the call in flight at once; PyHive's socket has no timeout of its own. `SUPERTABLE_SPARK_CONNECT_TIMEOUT` (default 30 seconds) is sent as the session's `hive.server2.session.check.interval`; it does not limit the client's socket.

`cancel()` is safe to call from another thread and makes no Thrift call. PyHive's client has no lock and the server answers one connection's requests in order, so a second call would wait behind the fetch in flight while both threads read the same socket. `cancel()` shuts the socket down instead: the fetch in flight, or the next one, raises a `RuntimeError` saying the stream was interrupted, which lets the stream-job runner record a cancelled or expired job as such. The server drops the session, and its temporary views with it, when the connection closes, so closing a cancelled or timed-out handle only closes its socket.

## 6. Results and monitoring

Materialization consumes Arrow batches into a Polars DataFrame and always closes the handle. Duplicate column names gain numeric suffixes. Decimal values with scale zero are normalized toward integers, with a floating-point fallback if the exact cast fails.

`QueryPlanManager` assigns a UUID query ID and a 16-character hash derived from the query plus its metadata-path input. This hash is not a snapshot version or query-result cache key. Temporary plan paths live under the application home, and old plans for the same hash are capped at 200 during initialization.

The plan extension records timings, estimated resources, status, result shape, source, query identifiers, and available engine profile in monitoring. Queries targeting monitoring sink tables skip recursive plan logging. Failures to log monitoring are non-fatal. Materialized execution records the engine; the streaming path currently does not add that engine statistic and records completion when its handle closes.
