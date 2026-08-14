# Query Engine

## Overview

SuperTable's query engine is a multi-backend execution layer that automatically selects the optimal SQL engine based on data size, query shape, and runtime availability. It supports three execution backends -- DuckDB, IslandDB, and Spark SQL -- unified behind a single `Executor` facade.

The engine layer lives in `supertable/engine/` and is invoked by `DataReader` after the query has been parsed, access control checked, and file lists resolved.

## Engine Selection

### The `Engine` Enum

Defined in `supertable/engine/engine_enum.py`, the `Engine` enum declares four members (AUTO plus three execution engines):

```python
class Engine(Enum):
    AUTO       = "auto"
    DUCKDB = "duckdb"
    ISLANDDB  = "islanddb"
    SPARK_SQL   = "spark_sql"
```

Each variant exposes a `dialect` property used by `SQLParser` to select the correct sqlglot grammar. `SPARK_SQL` returns `"spark"`; all others return `"duckdb"`.

### AUTO Mode

`Executor._auto_pick()` first applies correctness and availability gates, then
compares deterministic cost estimates for the remaining engines. Its primary
work signal is the sealed post-pruning row-group byte estimate when complete;
otherwise it conservatively uses the selected-file bytes. Query shape, decoded
bytes, file/row-group fanout, result size, freshness, spill advice, and scoped
historical observations also participate.

Spark is eligible only when an active registered Thrift cluster accepts the
job's size window and the SQL is inside the cross-engine equivalence subset.
IslandDB is eligible only for its statically proven SQL subset and an executable
resource plan. Missing/incomplete estimates route conservatively to DuckDB.

Organizations may store non-overlapping half-open `auto_policy` intervals in
Redis to force an engine for an estimated-scan range. Manual rules remain below
the same capability, identity, resource, and fleet gates; an unsafe rule falls
through to the adaptive model.

The shared values below tune the model/fleet fallback; they are not a fixed
three-tier routing table:

| Threshold | Environment Variable | Default |
|-----------|---------------------|---------|
| IslandDB crossover hint | `SUPERTABLE_ENGINE_ISLAND_MIN_BYTES` | 100 MiB (104,857,600 bytes) |
| Spark fallback floor | `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | 0 (active fleet `min_bytes` normally wins) |

### Freshness-Aware Routing

Freshness prevents cache thrashing. The `Reflection.freshness_ms` field carries the maximum `last_updated_ms` across all snapshots referenced by the query. The engine computes the age of the data:

```python
age_s = (time.time() * 1000 - reflection.freshness_ms) / 1000.0
data_is_fresh = age_s < freshness_threshold_s
```

| Setting | Environment Variable | Default |
|---------|---------------------|---------|
| Freshness threshold | `SUPERTABLE_ENGINE_FRESHNESS_SEC` | 300 seconds (5 minutes) |

**Routing logic for the medium tier:**

- **Fresh data** (age < threshold): routed to **DuckDB**, because the data is still being updated frequently and cached views in IslandDB would be invalidated before they pay off.
- **Stable data** (age >= threshold): routed to **IslandDB**, because the persistent connection and cached views will be reused across multiple queries, amortizing the setup cost.

When freshness is unknown (`freshness_ms == 0`), the data is assumed stable so that IslandDB gets a chance to cache.

## The Executor

The `Executor` class in `supertable/engine/executor.py` is instantiated per request and dispatches to the appropriate backend.

```python
class Executor:
    def __init__(self, storage=None, organization=""):
        self.storage = storage
        self.organization = organization
        self.duckdb_exec = DuckDB(storage=storage)
        self.island_exec = IslandDB(storage=storage)
        self.spark_exec = None  # lazily initialized

    def execute(
        self,
        engine: Engine,
        reflection: Reflection,
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer: Timer,
        plan_stats: PlanStats,
        log_prefix: str,
    ) -> Tuple[pd.DataFrame, str]:
```

Key behaviors:

- If `engine == Engine.AUTO`, calls `_auto_pick()` to resolve the actual engine.
- **DuckDB**: uses a storage- and organization-scoped shared instance so its connection caches survive across request-scoped `Executor` instances.
- **IslandDB**: executes supported Parquet query shapes with conservative
  cgroup-aware admission, bounded result collection, and hard-quota spill.
- **Spark SQL**: lazily imports `SparkThriftExecutor` (avoiding import cost when Spark is not needed). Passes `force=True` when the user explicitly requested Spark (not via AUTO).
- Records the engine used in `PlanStats` for query plan reporting.

## DuckDB

**Module**: `supertable/engine/duckdb_engine.py`
**Class**: `DuckDB`

DuckDB is the lightweight, transient execution path optimized for small datasets and frequently-changing data.

### Characteristics

- **Single persistent connection**: created lazily and reused across all queries to preserve DuckDB's HTTP metadata cache and external file cache between requests.
- **No materialized state**: VIEWs are created with unique (hashed) names and dropped in the `finally` block after each query. No TABLE state is retained between queries.
- **Thread-safe**: a lock guards connection creation and httpfs initialization only. DuckDB allows concurrent reads on the same connection, so query execution runs outside the lock.

### Cache Layers

1. **DuckDB external file cache** -- disk-level data block cache (DuckDB >= 1.3)
2. **DuckDB HTTP metadata cache** -- connection-level parquet footer cache (in-memory)
3. **ParquetMetadataCache** -- module-level Python dict, version-aware

### Execution Flow

1. Acquire (or create) the persistent connection via `_get_connection()`.
2. Configure httpfs once per connection lifetime via `_ensure_httpfs()`.
3. For each table referenced in the query:
   - Generate a hashed table name via `hashed_table_name()`.
   - Create a reflection table or view from the resolved parquet files using `create_reflection_table_with_presign_retry()`.
   - Optionally layer dedup, tombstone, and RBAC views on top.
4. Rewrite the user's SQL to reference the hashed table names via `rewrite_query_with_hashed_tables()`.
5. Execute the rewritten SQL, fetch results into a pandas DataFrame.
6. Drop all created views/tables in the `finally` block.

### Connection Recovery

If the connection encounters an unrecoverable error, `_reset_connection()` closes and discards it. The next query will create a fresh connection.

## Spark Thrift

**Module**: `supertable/engine/spark_thrift.py`
**Class**: `SparkThriftExecutor`

Spark Thrift is the distributed execution path for datasets too large for a single-node DuckDB instance.

### Characteristics

- Connects to a Spark Thrift Server via PyHive's HiveServer2 interface.
- Converts S3/HTTP paths to `s3a://` paths for Spark compatibility via `_to_s3a_path()`.
- Creates temporary parquet views using `CREATE OR REPLACE TEMPORARY VIEW ... USING parquet OPTIONS (path ...)`.
- Batches large file lists: individual file views are created per batch, unioned into batch views, then all batch views are unioned into the final view. Batch size is controlled by `SUPERTABLE_SPARK_BATCH_SIZE`.
- Intermediate views are kept alive until the final query completes (Spark's lazy view resolution requires this).

### Timeouts

| Setting | Environment Variable | Default |
|---------|---------------------|---------|
| Overall query timeout | `SUPERTABLE_SPARK_QUERY_TIMEOUT` | 300 seconds |
| Per-statement timeout | `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | 120 seconds |

### Table Naming

```python
def _spark_table_name(super_name, simple_name, version) -> str:
    key = f"{super_name}_{simple_name}"
    digest = hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]
    return f"spark_{digest}_v{version}"
```

### Verbose Logging Suppression

PyHive and Thrift libraries log every SQL statement at INFO level. The module suppresses this by setting the log level to WARNING for `pyhive`, `pyhive.hive`, `TCLIService`, `thrift`, and `thrift_sasl`.

## Data Size Estimation

**Module**: `supertable/engine/data_estimator.py`
**Class**: `DataEstimator`

The `DataEstimator` resolves which parquet files will be read for a query and calculates total byte size. This information feeds the engine auto-picker.

```python
class DataEstimator:
    def __init__(self, organization, storage, tables: List[TableDefinition]):
        self.organization = organization
        self.storage = storage
        self.tables = tables
        self.catalog = RedisCatalog()
```

### Output

The estimator produces a `Reflection` dataclass:

```python
@dataclass
class Reflection:
    storage_type: str              # storage backend identifier
    reflection_bytes: int          # total bytes across all parquet files
    total_reflections: int         # number of parquet files
    supers: List[SuperSnapshot]    # per-table file lists and metadata
    freshness_ms: int = 0          # max last_updated_ms across snapshots
    rbac_views: Dict[str, RbacViewDef] = {}
    dedup_views: Dict[str, DedupViewDef] = {}
    tombstone_views: Dict[str, TombstoneDef] = {}
```

### Column Validation

The `get_missing_columns()` function validates that columns requested by the query actually exist in the available snapshots. It performs case-insensitive matching and skips validation for `SELECT *` queries (where `columns == []`).

## View Chain Construction

Each engine constructs a layered view chain on top of the raw parquet data. The chain is built bottom-up, with each layer wrapping the previous one:

```
parquet files
    |
    v
[1] Base reflection table/view  (parquet_scan of all files)
    |
    v
[2] RBAC view                   (column + row filtering)
    |
    v
[3] Tombstone view              (exclude soft-deleted rows)
    |
    v
[4] Dedup view                  (ROW_NUMBER to keep latest per PK)
    |
    v
  User query executes against the top-most view
```

### Base Reflection Table/View

Created by `create_reflection_table()` or `create_reflection_view()` in `engine_common.py`:

```sql
CREATE TABLE st_<hash> AS
SELECT <columns>
FROM parquet_scan(['file1.parquet', 'file2.parquet', ...],
     union_by_name=TRUE, HIVE_PARTITIONING=FALSE);
```

- `union_by_name=TRUE` handles schema evolution (columns may appear in some files but not others).
- `HIVE_PARTITIONING=FALSE` disables hive-style partition inference.

### RBAC View Injection

Created by `create_rbac_view()` when the `Reflection.rbac_views` dict has an entry for the table alias:

```sql
CREATE OR REPLACE VIEW rbac_<base_table> AS
SELECT <allowed_columns>
FROM <base_table>
WHERE <where_clause>;
```

The `RbacViewDef` dataclass carries:
- `allowed_columns`: list of visible columns, or `["*"]` for unrestricted.
- `where_clause`: SQL predicate from role filters, or empty string.

View naming uses `rbac_view_name()`: `f"rbac_{base_table_name}"`.

### Tombstone View

Created by `create_tombstone_view()` when the snapshot's metadata includes a tombstones block:

```sql
CREATE OR REPLACE VIEW <view_name> AS
SELECT * FROM <source_table>
WHERE NOT EXISTS (
  SELECT 1 FROM (VALUES (k1, k2), ...) AS __tombstones__(pk1, pk2)
  WHERE <source_table>.pk1 = __tombstones__.pk1
    AND <source_table>.pk2 = __tombstones__.pk2
);
```

This anti-join pattern handles NULLs correctly and avoids column name collisions. Tombstone lists are bounded by the compaction threshold (typically <= 1000 keys).

### Dedup View

Created by `create_dedup_view()` when the table has `dedup_on_read` enabled in its config:

```sql
CREATE OR REPLACE VIEW <view_name> AS
SELECT <visible_columns> FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY <primary_keys>
    ORDER BY <order_column> DESC
  ) AS __rn__
  FROM <source_table>
) sub WHERE __rn__ = 1;
```

The `DedupViewDef` dataclass controls:
- `primary_keys`: columns forming the composite dedup key.
- `order_column`: column for ordering (default `__timestamp__`).
- `visible_columns`: columns exposed to the user query. When empty or `["*"]`, all source columns except `__rn__` are exposed using DuckDB's `EXCLUDE` syntax.

## Query Rewriting

After the view chain is built, the user's original SQL must reference the hashed physical table names instead of the logical table names. The `rewrite_query_with_hashed_tables()` function in `engine_common.py` handles this:

1. Parses the SQL using sqlglot.
2. Walks all `Table` nodes in the AST.
3. Replaces each table's physical name with the corresponding hashed name from `alias_to_table`.
4. Preserves or injects table aliases so qualified column references (e.g., `t.col`) remain valid.
5. Serializes back to DuckDB SQL dialect.

## Common Infrastructure

### Connection Initialization

The `init_connection()` function in `engine_common.py` applies standard PRAGMA settings to every DuckDB connection:

| Setting | Purpose | Default |
|---------|---------|---------|
| `memory_limit` | Cap DuckDB RAM to enable disk spilling | `SUPERTABLE_DUCKDB_MEMORY_LIMIT` or `"1GB"` |
| `temp_directory` | Absolute path for spill files | Resolved under `SUPERTABLE_HOME/tmp/` |
| `default_collation` | Case-insensitive string comparisons | `nocase` |
| `preserve_insertion_order` | Reduce memory pressure during scans | `false` |
| `threads` | Parallel execution threads | Auto-derived or `SUPERTABLE_DUCKDB_THREADS` |

### Thread Count Derivation

When `SUPERTABLE_DUCKDB_THREADS` is not set, the thread count is derived from memory and CPU:

```
io_threads   = cpu_count * SUPERTABLE_DUCKDB_IO_MULTIPLIER  (default 3)
memory_floor = max(1, memory_mb // 400)   -- ~400 MB per thread minimum
result       = min(io_threads, memory_floor)
```

This prevents OOM on large-CPU hosts with small memory limits.

### httpfs and S3 Configuration

The `configure_httpfs_and_s3()` function loads the httpfs extension and configures S3 credentials, endpoint, region, URL style, SSL, and caches. It reads from settings:

- `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY`, `STORAGE_SESSION_TOKEN`
- `STORAGE_REGION`, `STORAGE_FORCE_PATH_STYLE`, `STORAGE_USE_SSL`
- `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` (default 30 seconds)
- `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` -- parquet footer caching across queries
- `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` -- enables disk-level data block cache (DuckDB >= 1.3)
- `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` -- cache directory (defaults to `SUPERTABLE_HOME/duckdb_cache`)

### SQL Helpers

- `quote_if_needed(col)`: quotes column names containing special characters.
- `sanitize_sql_string(value)`: escapes single quotes in SQL string literals.
- `escape_parquet_path(path)`: escapes file paths for SQL string literals.
- `hashed_table_name(super_name, simple_name, version, columns)`: generates deterministic `st_<sha1_prefix>` table names.

## Configuration Reference

| Variable | Purpose | Default |
|----------|---------|---------|
| `SUPERTABLE_ENGINE_ISLAND_MIN_BYTES` | Upper bound for DuckDB engine selection | 104,857,600 (100 MB) |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | Spark fallback floor; active fleet `min_bytes` normally wins | 0 |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | Age threshold for fresh vs. stable data | 300 (5 minutes) |
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | DuckDB memory limit (shared by DuckDB and IslandDB) | `"1GB"` |
| `SUPERTABLE_DUCKDB_THREADS` | Explicit DuckDB thread count (overrides auto-derive) | Auto |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | CPU multiplier for IO thread calculation | 3 |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | httpfs HTTP timeout in seconds | 30 |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | Enable parquet footer caching | true |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | External file cache size (e.g. `"2GB"`) | Disabled |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | External file cache directory | `SUPERTABLE_HOME/duckdb_cache` |
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | Overall Spark query timeout | 300 seconds |
| `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | Per-statement Spark timeout | 120 seconds |
| `SUPERTABLE_SPARK_BATCH_SIZE` | Files per Spark view creation batch | Configurable |

## Business Context

The multi-engine architecture addresses a fundamental tradeoff in data analytics: **small queries should be fast and cheap, while large queries should be possible at all**.

- **DuckDB** handles the majority of interactive queries (dashboards, ad-hoc exploration) with sub-second latency and zero infrastructure overhead. It is the default path for datasets under 100 MB.
- **IslandDB** handles supported selective Parquet workloads with conservative
  memory admission, range reads, bounded results, and optional hard-quota spill.
- **Spark SQL** enables queries over datasets that exceed single-node memory limits. It requires a Spark Thrift Server but handles arbitrarily large data volumes.

The freshness-aware routing prevents a common failure mode: caching data that is still being actively ingested. Without this, a dashboard querying a table mid-ingestion would populate the IslandDB cache, only to invalidate it seconds later when the next batch lands.

The view chain (base, RBAC, tombstone, dedup) ensures that security filtering and data consistency are enforced at the engine level, not the application level. This means every query path -- SQL editor, API, OData, MCP -- gets identical security and consistency guarantees without duplicating logic.
