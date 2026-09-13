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

Explicit `Engine.DUCKDB` bypasses routing. Explicit `Engine.SPARK_SQL` in the materialized executor requests a cluster with `force=True`, bypassing cluster byte ranges while still requiring an active cluster. The streaming executor does not pass that force flag.

`engine_lite_max_bytes` and `engine_freshness_sec` are present in configuration but do not participate in `Executor._auto_pick()`. The current executor always uses the `lite` DuckDB configuration; it does not select a separate `pro` execution implementation.

## 4. DuckDB execution

DuckDB keeps a connection in thread-local state. Each query creates views over its selected Parquet files with `union_by_name=TRUE` and `HIVE_PARTITIONING=FALSE`.

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

## 5. Spark execution and current limitation

The Spark implementation uses PyHive to connect to a registered Thrift endpoint. Cluster configuration supplies `thrift_host`, `thrift_port` (default `10000`), `auth` (default `NONE`), and optional credentials. Missing PyHive or an eligible active cluster raises an error.

The executor builds temporary Parquet views, combines multiple files with `UNION ALL`, applies deletion and RBAC views, rewrites SQL to Spark syntax, and attempts to capture `EXPLAIN EXTENDED`. S3 paths are converted to `s3a://`; optional presigning is controlled by `SUPERTABLE_SPARK_PRESIGNED`. Defaults are 300 seconds for query execution, 120 seconds per statement, and 30 seconds for the connection setting.

**Current return-value defect:** `SparkThriftExecutor.execute()` returns a stream handle inside its `try`, but a bare `return` in its streaming `finally` branch overrides that handle with `None`. The materialized executor currently converts this into an empty DataFrame, while `DataReader.stream()` reports that no reader was produced. Use `Engine.DUCKDB` when a working result is required until that implementation is corrected. Setting AUTO does not avoid this defect when AUTO selects Spark.

## 6. Results and monitoring

Materialization consumes Arrow batches into a Polars DataFrame and always closes the handle. Duplicate column names gain numeric suffixes. Decimal values with scale zero are normalized toward integers, with a floating-point fallback if the exact cast fails.

`QueryPlanManager` assigns a UUID query ID and a 16-character hash derived from the query plus its metadata-path input. This hash is not a snapshot version or query-result cache key. Temporary plan paths live under the application home, and old plans for the same hash are capped at 200 during initialization.

The plan extension records timings, estimated resources, status, result shape, source, query identifiers, and available engine profile in monitoring. Queries targeting monitoring sink tables skip recursive plan logging. Failures to log monitoring are non-fatal. Materialized execution records the engine; the streaming path currently does not add that engine statistic and records completion when its handle closes.
