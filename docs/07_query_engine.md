# Data Island Core — Data Reader & Query Engine

## Overview

The query engine takes a SQL query, resolves the tables it references, assembles the data files from snapshots, selects the best execution backend (DuckDB Lite, DuckDB Pro, or Spark SQL), applies RBAC filters, dedup views, and tombstone exclusions, then returns results as a Pandas DataFrame. The entire flow is orchestrated by `DataReader` in `data_reader.py`, with engine selection and execution delegated to the `Executor` in `engine/executor.py`.

---

## How it works

### Query execution flow

**1. Parse SQL** — `SQLParser` (from `utils/sql_parser.py`) parses the query using the sqlglot library. It extracts the tables referenced in the query (as `TableDefinition` objects) and determines which physical tables are accessed.

**2. RBAC check** — `restrict_read_access()` from the RBAC module verifies the role has read permission on every referenced table. It returns `RbacViewDef` objects for each table — column restrictions and row-level WHERE clauses that must be applied.

**3. Estimate** — `DataEstimator` reads snapshot metadata from Redis for every referenced table. It collects file paths, column names, file sizes, and freshness timestamps. The output is a `Reflection` object — the complete picture of what needs to be queried.

**4. Resolve dedup and tombstones** — For tables with `dedup_on_read` enabled, `DedupViewDef` objects are attached to the reflection. For tables with tombstoned rows, `TombstoneDef` objects are attached. These tell the engine what filters to apply.

**5. Select engine** — The `Executor._auto_pick()` method chooses the best backend based on total data size and freshness:

| Data size | Fresh data (< 5 min old) | Stable data (≥ 5 min old) |
|---|---|---|
| Small (< 100 MB) | DuckDB Lite | DuckDB Lite |
| Medium (100 MB – 10 GB) | DuckDB Lite (cache would churn) | DuckDB Pro (cache pays off) |
| Large (> 10 GB) | Spark SQL | Spark SQL |

All thresholds are configurable via environment variables. If Spark is not installed, large queries fall back to DuckDB Pro.

**6. Execute** — The selected engine registers the Parquet files as tables (DuckDB) or temporary views (Spark), applies RBAC column/row filters as SQL views, applies dedup-on-read window views, applies tombstone exclusion filters, then runs the user's query against these layered views.

**7. Return results** — Results are returned as a Pandas DataFrame with a status enum (`OK` or `ERROR`) and an optional error message.

---

## Engines

### DuckDB Lite

File: `engine/duckdb_lite.py`

The default engine for small to medium queries. Creates a fresh in-memory DuckDB connection per query. Parquet files are registered as views using DuckDB's httpfs extension (for cloud storage) or local file paths.

Characteristics:
- No persistent state between queries — each query starts clean
- Memory limit set by `SUPERTABLE_DUCKDB_MEMORY_LIMIT` (default: 1 GB)
- Thread count set by `SUPERTABLE_DUCKDB_THREADS` (default: auto-detect)
- Best for: ad-hoc queries, small tables, frequently-changing data

### DuckDB Pro

File: `engine/duckdb_pro.py`

An enhanced DuckDB engine that enables caching and advanced features. Uses the same DuckDB core but with external caching configured via `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` and `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR`.

Characteristics:
- External cache survives across queries — subsequent queries on the same data are faster
- Better for stable data that doesn't change frequently
- Same memory and thread configuration as Lite
- Best for: repeated queries on large stable datasets, dashboard workloads

### Spark SQL

File: `engine/spark_thrift.py`

Connects to an external Spark Thrift server via JDBC/Thrift protocol. Used for queries that exceed DuckDB's memory capacity.

Characteristics:
- Requires a running Spark Thrift server (configured separately via `infrastructure/spark_thrift/`)
- Query timeout: `SUPERTABLE_SPARK_QUERY_TIMEOUT` (default: 300 seconds)
- Statement timeout: `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` (default: 120 seconds)
- Connection timeout: `SUPERTABLE_SPARK_CONNECT_TIMEOUT` (default: 30 seconds)
- Result batching: `SUPERTABLE_SPARK_BATCH_SIZE` (default: 50 rows per fetch)
- Best for: full-table scans on 10+ GB tables, complex joins across large datasets

### Engine selection override

Users can force a specific engine via the `engine` parameter:

```python
# In the API
POST /reflection/execute
{ "engine": "duckdb_lite" }  # or "duckdb_pro", "spark_sql", "auto"

# In the MCP server
query_sql(engine="spark_sql", ...)

# In Python
reader = DataReader(super_name="example", organization="acme", query="SELECT ...")
reader.execute(role_name="analyst", engine=Engine.SPARK_SQL)
```

---

## DataEstimator

File: `engine/data_estimator.py`

The estimator collects metadata for engine selection without reading actual data. For each table referenced in the query:

1. Reads snapshot metadata from Redis (leaf pointer → payload)
2. Extracts file paths and sizes from the resource list
3. Builds `SuperSnapshot` objects (file list + column set)
4. Converts storage paths to DuckDB-readable paths (presigned URLs or `s3://` paths)
5. Sums total bytes across all tables for engine routing

The output is a `Reflection` object containing all snapshots, total size, storage type, and freshness timestamp.

---

## RBAC integration

Every query is filtered through RBAC before execution. The flow:

1. `restrict_read_access()` checks if the role can access each referenced table
2. For each table, it builds an `RbacViewDef` with:
   - `allowed_columns`: the columns the role can see (or `["*"]` for all)
   - `where_clause`: a SQL WHERE predicate from row-level filters
3. The engine wraps each table in a filtered view:
   ```sql
   CREATE VIEW filtered_orders AS
   SELECT id, customer, amount     -- column restriction
   FROM raw_orders
   WHERE region = 'EU'             -- row-level filter
   ```
4. The user's query runs against these filtered views — it never sees restricted columns or rows

---

## Dedup-on-read

When a table has `dedup_on_read` enabled and `primary_keys` configured:

1. DataReader creates a `DedupViewDef` for that table
2. The engine wraps the table in a ROW_NUMBER window view:
   ```sql
   CREATE VIEW deduped_orders AS
   SELECT * FROM (
     SELECT *, ROW_NUMBER() OVER (
       PARTITION BY order_id ORDER BY __timestamp__ DESC
     ) AS __rn__
     FROM raw_orders
   ) WHERE __rn__ = 1
   ```
3. The user's query sees only the latest row per primary key

This allows append-only writes with logical update semantics — every version of a row exists in the Parquet files, but queries see only the newest.

---

## Tombstone filtering

When a table has tombstoned (soft-deleted) rows:

1. DataReader creates a `TombstoneDef` from the snapshot metadata
2. The engine adds an exclusion filter:
   ```sql
   WHERE order_id NOT IN (101, 202, 303)
   ```
3. Deleted rows are invisible to queries

---

## Query plan management

`QueryPlanManager` in `query_plan_manager.py` tracks execution metadata:

- Query ID (unique per execution)
- Query hash (deterministic hash of the normalized SQL — identifies identical queries across runs)
- Execution plan steps (which engine was used, how many files were scanned)

`PlanStats` in `engine/plan_stats.py` collects per-query statistics (engine used, file counts, timing).

`PlanExtender` in `plan_extender.py` enriches the execution plan with timing breakdowns and result metadata after execution completes.

---

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | `1GB` | DuckDB memory limit per connection |
| `SUPERTABLE_DUCKDB_THREADS` | (auto) | DuckDB thread count |
| `SUPERTABLE_DUCKDB_PRESIGNED` | `false` | Use presigned URLs for storage reads |
| `SUPERTABLE_ENGINE_LITE_MAX_BYTES` | 100 MB | Upper bound for DuckDB Lite |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | 10 GB | Lower bound for Spark SQL |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | 300 | Age threshold for cache decision (seconds) |
| `SUPERTABLE_DEFAULT_ENGINE` | `AUTO` | Default engine selection |
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | 300 | Spark query timeout (seconds) |
| `SUPERTABLE_SPARK_CONNECT_TIMEOUT` | 30 | Spark connection timeout (seconds) |

---

## Module structure

```
supertable/
  data_reader.py              DataReader facade — parse, estimate, execute (272 lines)
  engine/
    engine_enum.py            Engine enum (AUTO, DUCKDB_LITE, DUCKDB_PRO, SPARK_SQL)
    engine_common.py          Shared engine utilities
    executor.py               Engine selector and dispatcher (200 lines)
    data_estimator.py         Snapshot metadata collector for engine routing (350 lines)
    duckdb_lite.py            DuckDB embedded — fresh connection per query
    duckdb_pro.py             DuckDB with caching and advanced features
    spark_thrift.py           Spark SQL via Thrift protocol
    plan_stats.py             Per-query execution statistics
  plan_extender.py            Execution plan enrichment
  query_plan_manager.py       Query ID, hash, and plan tracking
  utils/sql_parser.py         SQL parsing via sqlglot
```

---

## Frequently asked questions

**How does AUTO engine selection work?**
The executor sums the total bytes across all referenced tables (from snapshot metadata). Small data (< 100 MB) goes to DuckDB Lite. Medium data with stable freshness (> 5 minutes since last write) goes to DuckDB Pro (caching helps). Large data (> 10 GB) goes to Spark SQL if available, otherwise DuckDB Pro.

**Can I use JOINs across tables?**
Yes. The SQL parser resolves all referenced tables, and the engine registers all of them before executing the query. JOINs, subqueries, CTEs, and window functions work as they would in DuckDB or Spark SQL.

**Are write queries (INSERT, UPDATE, DELETE) allowed?**
No. The read path enforces read-only SQL — only `SELECT` and `WITH ... SELECT` queries are accepted. Write operations use the `DataWriter` API. The MCP server has an additional keyword blocklist that rejects DDL and DML statements.

**What SQL dialect does the query engine support?**
DuckDB SQL for Lite and Pro engines. Spark SQL for the Spark engine. The parser normalizes queries via sqlglot, which supports cross-dialect translation. Most standard SQL works with any engine.

**How does the engine handle schema differences across Parquet files?**
DuckDB's `read_parquet()` with `union_by_name=true` handles schema evolution — columns present in some files but not others are filled with NULL. The estimator collects the union of all column names from all snapshot files.

**What is the maximum query result size?**
The API enforces a 10,000 row hard limit (configurable). The MCP server enforces `SUPERTABLE_MAX_LIMIT` (default: 5,000). Both apply `LIMIT` to the SQL if the user doesn't specify one.
