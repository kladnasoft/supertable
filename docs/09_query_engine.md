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
job's size window, the SQL is inside the cross-engine equivalence subset, and
none of the pinned snapshots has an active deletion vector. Explicit Spark
requests against active deletion vectors fail closed before cluster selection
or connection; AUTO keeps Spark out of the candidate set and routes to an
eligible single-node engine.
IslandDB is eligible only for its statically proven SQL subset and an executable
resource plan. Missing/incomplete estimates route conservatively to DuckDB.

Provider-linked share resources carrying provider-issued bearer credentials
are DuckDB-only. AUTO records IslandDB and Spark as ineligible and selects
DuckDB; explicitly requesting either ineligible backend fails before catalog,
cache, or provider I/O. The common credential-expiry admission check still runs
first, so an expired linked credential never reaches routing.

When `SUPERTABLE_DUCKDB_PRESIGNED=true`, estimation retains canonical object
paths and performs no credential I/O. Only a selected DuckDB execution mints
one deadline-bounded credential per consumer-owned resource immediately before
setup; IslandDB and Spark never pay that cost. Sealed immutable objects are
then exposed to DuckDB through a stable process-local path, allowing DuckDB's
HTTP/external cache to survive signed-URL rotation without weakening object
identity checks.

Concurrent DuckDB leases for the same stable object intentionally share that
relay route and upstream transfer. Because an inbound HTTP read cannot be
attributed to one lease, the relay uses the longest active lease deadline: a
short query that cancels closes its downstream request, while shared upstream
I/O may remain alive for a concurrent longer query. The residual resource use
is bounded by relay connection/permit limits and the longest active deadline;
it is not exact per-query upstream cancellation.

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
- **Thread-safe**: runtime PRAGMA/configuration changes and non-streaming query
  execution are serialized by a reentrant runtime lock. Streaming queries keep
  setup and execution protected while their result lifecycle remains isolated.
- **Closed SQL capability surface**: the backend reparses the original SQL,
  requires one read-only statement, rebuilds its table bindings, verifies that
  every requested relation exists in the authorized reflection, and rejects
  settings/secret catalogs, filesystem helpers, unknown functions, extension
  UDFs, qualified function calls, bare DuckDB session/catalog identity tokens,
  and unbounded collection aggregates. Quoted or table-qualified columns whose
  names happen to match an identity token remain ordinary data columns.
- **Protected storage credentials**: explicit S3 credentials live in a named
  temporary in-memory DuckDB secret. The legacy readable `s3_access_key_id`,
  `s3_secret_access_key`, and `s3_session_token` settings remain unset. An
  injected S3/MinIO backend provisions that secret from its own authorization
  context, never from broader process-global credentials. Opaque injected SDK
  credentials fail closed for direct `s3://` scans; use presigned-path mode or
  expose the documented server-side `duckdb_s3_config()` adapter contract.

### Cache Layers

1. **DuckDB external file cache** -- disk-level data block cache (DuckDB >= 1.3)
2. **DuckDB HTTP metadata cache** -- connection-level parquet footer cache (in-memory)
3. **ParquetMetadataCache** -- module-level Python dict, version-aware

### Execution Flow

1. Reparse and validate the original SQL and authorized reflection before
   opening the DuckDB connection.
2. Acquire (or create) the persistent connection via `_get_connection()` and
   configure httpfs/the temporary S3 secret via `_ensure_httpfs()`.
3. For each table referenced in the query:
   - Generate a hashed table name via `hashed_table_name()`.
   - Create a reflection table or view from the resolved parquet files using `create_reflection_table_with_presign_retry()`.
   - Create the protected projection (including the composite deletion-vector
     anti-join when active), then layer RBAC on top.
4. Rewrite the user's SQL to reference the hashed table names via `rewrite_query_with_hashed_tables()`.
5. Disable raw backend profiling, disable secret unredaction/extension
   autoloading, and execute the rewritten SQL.
6. Drop all created views/tables in the `finally` block.

Direct DuckDB and `Executor` callers receive phase-only messages for backend
connection, storage, managed-view, scan, and result-stream failures. Raw
backend causes are scrubbed before propagation because generated DuckDB SQL can
contain physical source URLs and RBAC/share predicate columns or literals.
Parser, authorization, request-limit, timeout, and cancellation failures retain
their typed public semantics.

### Connection Recovery

If the connection encounters an unrecoverable error, `_reset_connection()` closes and discards it. The next query will create a fresh connection.

## Spark Thrift

**Module**: `supertable/engine/spark_thrift.py`
**Class**: `SparkThriftExecutor`

Spark Thrift is the distributed execution path for datasets too large for a single-node DuckDB instance.

Nanosecond timestamp normalization verifies parquet footer metadata first. If a
representative footer cannot be verified, the read fails closed instead of
guessing a timestamp unit.

### Characteristics

- Connects to a Spark Thrift Server via PyHive's HiveServer2 interface.
- Converts S3/HTTP paths to `s3a://` paths for Spark compatibility via `_to_s3a_path()`.
- Requires cluster-side workload identity or a Hadoop credential provider.
  Inline access/secret/session keys and presigned source URLs are rejected.
- Disables Spark variable substitution and enforces a closed, data-only SQL
  function allowlist before cluster selection. JVM reflection, source-file
  metadata, scripts, hints, cluster UDFs, and unbounded output amplifiers fail
  closed.
- Persists only credential-safe plans: raw Catalyst plan sections are replaced
  by a fixed marker before the plan file is written.
- Rejects snapshots with an active deletion vector before parser work, fleet
  selection, or connection. Spark's resolved `s3a://`, `gs://`, `abfss://`, or
  signed source paths cannot yet be bound safely to the stable logical
  `__file__` keys stored in the vector. A row-id-only anti-join could hide a
  live row in a different file, so explicit Spark fails closed and AUTO selects
  another eligible engine until composite source-file + row-id identity is
  implemented end to end.
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
    tombstone_views: Dict[str, TombstoneDef] = {}
```

### Column Validation

The `get_missing_columns()` function validates that columns requested by the query actually exist in the available snapshots. It performs case-insensitive matching and skips validation for `SELECT *` queries (where `columns == []`).

## View Chain Construction

Successful engine executions construct a protected view chain on top of the raw
parquet data. DuckDB and IslandDB can apply active deletion vectors. Spark uses
the same public projection and RBAC contract only when no deletion vector is
active; otherwise it rejects the request before touching the fleet.

```
parquet files
    |
    v
[1] Base reflection table/view  (parquet_scan of all files)
    |
    v
[2] Protected projection       (composite deletion-vector anti-join,
                                then strip internal columns)
    |
    v
[3] RBAC view                   (column + row filtering)
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

### Protected Deletion-Vector View

The writer records obsolete/deleted physical rows in an immutable deletion
vector with the exact pair `(__file__, __rowid__)`. `create_tombstone_view()`
validates the vector's schema, pinned row count/digest, referenced snapshot
files, and source row-id integrity before exposing a protected view. Its logical
shape is:

```sql
CREATE OR REPLACE VIEW <view_name> AS
SELECT <public columns>
FROM <source_table> AS src
ANTI JOIN <validated_deletion_vector> AS dv
  ON src.__supertable_source_file__ = dv.__file__
 AND src.__rowid__ = dv.__rowid__;
```

`__supertable_source_file__` is the executor's protected canonical logical
identity for the scanned data object; it is not a user-visible column. The
composite join is required even when row IDs are normally table-global: using
`__rowid__` alone would turn a metadata or legacy collision into live-row loss.
The view strips `__rowid__`, `__timestamp__`, and all protected source-file
columns before RBAC or user SQL can observe them.

Spark does not currently create this view for an active vector. Its explicit
executor raises `Spark deletion-vector reads require composite source-file +
row-id identity and are not supported safely`; the public `query_sql` facade
returns the stable sanitized `Query execution failed` message. This is a
fail-closed capability boundary, not a fallback or an ignored tombstone.

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

The `configure_httpfs_and_s3()` function loads the httpfs extension and creates
or replaces the `supertable_s3` temporary in-memory secret. DuckDB redacts the
secret/session-token fields in its own catalog; user SQL cannot call that
catalog or the settings API. The function also configures HTTP/cache controls.
It reads from settings:

- `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY`, `STORAGE_SESSION_TOKEN`
- `STORAGE_REGION`, `STORAGE_FORCE_PATH_STYLE`, `STORAGE_USE_SSL`
- `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` (default 30 seconds)
- `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` -- parquet footer caching across queries
- `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` -- enables disk-level data block cache (DuckDB >= 1.3)
- `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` -- cache directory (defaults to `SUPERTABLE_HOME/duckdb_cache`)

Raw DuckDB JSON profiling is disabled on the untrusted path because physical
filenames can contain presigned bearer URLs. `EXPLAIN ANALYZE` is rejected, and
plain `EXPLAIN` is rejected whenever a data or deletion-vector source is a
credential-bearing URL or the query has an RBAC/share row or column policy.
The latter prevents expanded view plans from disclosing hidden predicate
columns or literal policy values.

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
| `SUPERTABLE_SPARK_PRESIGNED` | Disabled compatibility flag; must remain false | false |

## Business Context

The multi-engine architecture addresses a fundamental tradeoff in data analytics: **small queries should be fast and cheap, while large queries should be possible at all**.

- **DuckDB** handles the majority of interactive queries (dashboards, ad-hoc exploration) with sub-second latency and zero infrastructure overhead. It is the default path for datasets under 100 MB.
- **IslandDB** handles supported selective Parquet workloads with conservative
  memory admission, range reads, bounded results, and optional hard-quota spill.
- **Spark SQL** enables supported queries over datasets that exceed single-node
  memory limits. It requires a Spark Thrift Server; snapshots with active
  deletion vectors remain on DuckDB/IslandDB through AUTO and explicit Spark
  rejects them.

The freshness-aware routing prevents a common failure mode: caching data that is still being actively ingested. Without this, a dashboard querying a table mid-ingestion would populate the IslandDB cache, only to invalidate it seconds later when the next batch lands.

The protected projection and RBAC chain keeps security filtering and data
consistency in the engine layer rather than duplicating it in each API. Every
successful query path therefore receives the same policy and deletion-vector
guarantees. An engine that cannot establish those guarantees fails closed or is
excluded by AUTO; it never returns an unfiltered approximation.
