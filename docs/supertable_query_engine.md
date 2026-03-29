# Feature / Component Reverse Engineering — Supertable Query Engine

## 1. Executive Summary

The Supertable Query Engine is the core SQL execution subsystem of the Supertable product. It enables users to execute SQL queries against parquet files stored in S3-compatible object storage, abstracted through a virtual table layer backed by Redis metadata. The engine resolves logical table references to physical parquet file sets, applies a layered view chain (RBAC security → tombstone soft-delete filtering → dedup-on-read), rewrites user SQL to target internal view names, and dispatches execution to one of three execution backends: **DuckDB Lite** (ephemeral, per-request), **DuckDB Pro** (persistent singleton with version-aware caching), or **Spark Thrift** (remote Spark SQL for large datasets). Results are returned as pandas DataFrames. An MCP-facing entry point (`query_sql`) serializes results into column/row/metadata tuples for API consumers.

**Primary responsibility**: Transform user SQL + logical table names → physical parquet scan → security-filtered → deduplicated → executed → DataFrame result.

**Business purpose**: Enables a serverless analytical query layer over parquet data lakes with built-in multi-tenancy (RBAC), soft-delete semantics, and automatic engine scaling — the foundation of Supertable's query-as-a-service offering.

**Technical role**: Orchestration + execution layer sitting between the SQL parsing / RBAC subsystems and the storage / metadata subsystems.

---

## 2. Functional Overview

### Product Capabilities Enabled

| Capability | Description |
|---|---|
| **SQL-over-Parquet** | Users write standard SQL; the engine resolves table references to versioned parquet file sets in S3 and executes via DuckDB or Spark. |
| **Automatic engine selection** | The system picks the optimal execution backend based on data volume and freshness — no user intervention required. |
| **Role-based access control (RBAC)** | Column-level and row-level security filters are applied transparently as SQL views before the user query executes. |
| **Soft-delete (tombstone) filtering** | Rows marked as deleted via composite-key tombstones are excluded without physical file mutation. |
| **Dedup-on-read** | For append-only ingestion patterns, only the latest row per primary key is surfaced using `ROW_NUMBER()` windowing. |
| **MCP/API query interface** | `query_sql()` provides a JSON-serializable output format (columns, rows, metadata) for the MCP server layer. |
| **Execution plan telemetry** | Every query produces timing breakdowns, engine choice rationale, and query plan artifacts for observability. |
| **Presigned URL fallback** | Automatic retry with presigned S3 URLs on HTTP/auth errors, enabling deployment across diverse storage configurations. |

### Target Actors

- **End users**: Issue SQL queries via MCP or other API surfaces; receive tabular results filtered by their role permissions.
- **Administrators / Superadmins**: Execute unrestricted queries; manage table configurations (dedup, tombstones) via Redis catalog.
- **Platform operators**: Configure engine thresholds, memory limits, caching behavior, and Spark cluster registration via environment variables and Redis.
- **Downstream systems**: MCP server, plan extender (telemetry), query plan manager.

### Business Value

- **Core monetizable capability**: SQL analytics over customer parquet data lakes is the primary product offering.
- **Multi-tenancy**: RBAC enforcement at the engine level enables per-role data isolation within a shared infrastructure.
- **Operational efficiency**: Auto-engine selection optimizes cost/performance without manual tuning.
- **Data integrity**: Tombstone and dedup layers provide correct read semantics over immutable parquet storage without rewriting files.

---

## 3. Technical Overview

### Architectural Style

- **Facade pattern**: `DataReader` orchestrates the full query lifecycle — parse, RBAC, estimate, wire views, execute, telemetry.
- **Strategy pattern**: `Executor` selects and delegates to one of three engine implementations based on a configurable decision matrix.
- **Layered view chain**: Security and data-integrity views are composed as SQL VIEWs wrapping each other: `parquet_scan → RBAC → tombstone → dedup → user query`.
- **Singleton with ref-counting**: `DuckDBPro` maintains a persistent connection and version-aware view cache with reference counting for safe concurrent access.

### Major Control Flow

```
query_sql() or DataReader.execute()
  ├─ SQLParser: parse SQL, extract table aliases + columns, select dialect
  ├─ restrict_read_access(): RBAC gate — deny or return per-alias RbacViewDef
  ├─ DataEstimator.estimate(): resolve snapshots from Redis, collect parquet files, validate columns → Reflection
  ├─ Wire rbac_views, dedup_views, tombstone_views onto Reflection
  ├─ Executor.execute():
  │    ├─ _auto_pick(): size + freshness → Engine enum
  │    └─ Dispatch to DuckDBLite | DuckDBPro | SparkThriftExecutor
  │         ├─ Create reflection views (parquet_scan over S3 files)
  │         ├─ Create RBAC view (column + row filtering)
  │         ├─ Create tombstone view (NOT EXISTS anti-join)
  │         ├─ Create dedup view (ROW_NUMBER window)
  │         ├─ Rewrite SQL: alias → hashed view name
  │         ├─ Execute rewritten SQL
  │         └─ Return DataFrame
  ├─ extend_execution_plan(): telemetry
  └─ Return (DataFrame, Status, message)
```

### Key Data Flow

1. **Input**: organization, super_name, SQL string, role_name, engine preference
2. **Metadata resolution**: Redis → snapshot paths → parquet file lists + schema
3. **View chain construction**: parquet_scan VIEW → optional RBAC VIEW → optional tombstone VIEW → optional dedup VIEW
4. **Query rewriting**: sqlglot AST transform replacing logical table names with hashed view names
5. **Execution**: DuckDB in-process or Spark Thrift remote
6. **Output**: pandas DataFrame (or columns/rows/meta for MCP)

### Design Patterns

- **Deterministic view naming**: SHA1 hashes of `(super_name, simple_name, version, columns)` ensure cache-safe, collision-free names.
- **Per-query UUID suffixes**: RBAC/tombstone/dedup views include a UUID fragment to prevent cross-query collisions under concurrency.
- **Presigned URL retry**: On S3 HTTP errors, the engine re-attempts with presigned URLs — a resilience pattern for heterogeneous storage backends.
- **Lazy views (DuckDB)**: `CREATE VIEW` over `parquet_scan` defers all I/O to query time, enabling projection and predicate pushdown.
- **Watchdog timeout (Spark)**: Background thread forcibly closes the Thrift connection on timeout, unblocking any hung RPC.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes / Functions | Relevance |
|---|---|---|---|
| `__init__.py` | Package exports | Exports `Engine`, `Executor`, `PlanStats`, `DataEstimator` | Package entry point |
| `engine_enum.py` | Engine enum + dialect mapping | `Engine` enum with `dialect` property | Engine identity and SQL grammar selection |
| `executor.py` | Engine router + auto-pick logic | `Executor`, `_auto_pick()`, `_get_pro()` | Central dispatch, engine selection algorithm |
| `duckdb_lite.py` | Ephemeral per-request DuckDB executor | `DuckDBLite` | Low-overhead engine for small/fresh data |
| `duckdb_pro.py` | Persistent singleton DuckDB executor with view cache | `DuckDBPro`, `_ProCacheEntry` | High-performance engine for stable, repeated queries |
| `spark_thrift.py` | Remote Spark Thrift Server executor | `SparkThriftExecutor`, `_spark_create_parquet_view`, `_spark_rewrite_query` | Large-dataset engine via remote Spark |
| `engine_common.py` | Shared utilities: S3 config, view creation, query rewriting, connection init | `configure_httpfs_and_s3`, `create_rbac_view`, `create_dedup_view`, `create_tombstone_view`, `rewrite_query_with_hashed_tables`, `init_connection` | Foundation for all three engines |
| `data_estimator.py` | Snapshot discovery, file collection, column validation | `DataEstimator`, `get_missing_columns` | Pre-execution metadata resolution |
| `data_classes.py` | Core data structures | `Reflection`, `SuperSnapshot`, `RbacViewDef`, `DedupViewDef`, `TombstoneDef`, `TableDefinition` | Shared contracts between all components |
| `data_reader.py` | Facade orchestrating the full query lifecycle | `DataReader`, `query_sql()`, `_ensure_sql_limit()` | Top-level entry point |
| `plan_stats.py` | Simple stat accumulator | `PlanStats` | Telemetry support |
| `engine_documentation.md` | Internal documentation (provided as reference) | — | Architecture reference |

---

## 5. Detailed Functional Capabilities

### 5.1 SQL Query Execution over Parquet

- **Description**: Accepts a SQL string, resolves referenced tables to versioned parquet files in S3, and returns query results as a DataFrame.
- **Business purpose**: Core product capability — analytics over parquet data lakes.
- **Trigger**: `query_sql()` (MCP entry) or `DataReader.execute()`.
- **Processing**: Parse SQL → RBAC check → estimate (discover files) → construct view chain → rewrite SQL → execute → return DataFrame.
- **Output**: `(DataFrame, Status, message)` or `(columns, rows, columns_meta)` for MCP.
- **Dependencies**: `SQLParser`, `RedisCatalog`, `StorageInterface`, chosen engine.
- **Constraints**: Query must reference tables that exist as snapshots in Redis. Columns must exist in snapshot schema.
- **Risks**: Missing columns raise `RuntimeError`. No parquet files → empty result with ERROR status.
- **Confidence**: Explicit.

### 5.2 Automatic Engine Selection

- **Description**: Chooses the best execution engine (Lite, Pro, or Spark) based on total data size and data freshness.
- **Business purpose**: Optimizes cost/performance without user intervention; prevents OOM on large datasets.
- **Trigger**: `engine=Engine.AUTO` (default path).
- **Processing**: Compares `reflection_bytes` against env-configurable thresholds; checks `freshness_ms` to determine caching value.
- **Decision matrix**:
  - Small (≤100 MB): always Lite.
  - Medium (100 MB–10 GB) + fresh (<5 min): Lite (cache would churn).
  - Medium + stable (≥5 min): Pro (cache pays off).
  - Large (≥10 GB): Spark if pyspark importable, else Pro fallback.
- **Output**: `Engine` enum value.
- **Dependencies**: Environment variables `SUPERTABLE_ENGINE_LITE_MAX_BYTES`, `SUPERTABLE_ENGINE_SPARK_MIN_BYTES`, `SUPERTABLE_ENGINE_FRESHNESS_SEC`.
- **Constraints**: Spark requires pyspark on the classpath and an active Spark Thrift Server cluster in Redis.
- **Risks**: Unknown freshness (0) is treated as stable — may suboptimally route to Pro for actively-written data if `freshness_ms` is not populated.
- **Confidence**: Explicit.

### 5.3 RBAC View Enforcement (Engine-Side)

- **Description**: Before query execution, the engine creates a SQL VIEW that enforces column-level projection and row-level WHERE filtering per the user's role definition.
- **Business purpose**: Multi-tenant data isolation — restricted roles see only permitted columns and rows.
- **Trigger**: Non-empty `rbac_views` dict on the `Reflection` object (populated by `restrict_read_access()`).
- **Processing**: For each alias with an `RbacViewDef`, a `CREATE OR REPLACE VIEW rbac_<table>_<uuid>` is issued with column projection and optional WHERE clause. The user query is rewritten to target this view instead of the base table.
- **Output**: Filtered VIEW name replaces base table name in the alias map.
- **Dependencies**: `restrict_read_access()` (external RBAC module), `RbacViewDef` data class, `create_rbac_view()` in `engine_common.py`.
- **Constraints**: RBAC views must pass through columns required by downstream dedup/tombstone views (documented as `augment_rbac_columns()` in the RBAC module — not in this code set).
- **Risks**: If RBAC view omits PK/order columns needed by dedup, the dedup view will fail. Mitigation is handled externally by `augment_rbac_columns()`.
- **Confidence**: Explicit in code; augmentation logic is external (strong inference from documentation).

### 5.4 Tombstone (Soft-Delete) Filtering

- **Description**: Creates a VIEW that excludes rows matching a set of soft-deleted composite keys using a `NOT EXISTS` anti-join against a `VALUES` list.
- **Business purpose**: Supports delete semantics on immutable parquet storage without file rewriting.
- **Trigger**: Non-empty `tombstone_views` dict on `Reflection`, populated from snapshot `tombstones` block in Redis.
- **Processing**: Reads `primary_keys` and `deleted_keys` from `TombstoneDef`. Builds `CREATE VIEW ... WHERE NOT EXISTS (SELECT 1 FROM VALUES (...) AS __tombstones__ WHERE ...)`.
- **Output**: Filtered VIEW inserted in the chain after RBAC and before dedup.
- **Dependencies**: `RedisCatalog.get_leaf()` for tombstone metadata, `TombstoneDef` data class.
- **Constraints**: Tombstone list is bounded by a compaction threshold (~1000 keys per documentation). Unbounded lists could degrade query performance.
- **Risks**: If `deleted_keys` format is inconsistent (e.g., wrong tuple length vs. `primary_keys`), the generated SQL will be invalid.
- **Confidence**: Explicit.

### 5.5 Dedup-on-Read

- **Description**: Creates a VIEW using `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <order_col> DESC) = 1` to surface only the latest row per primary key.
- **Business purpose**: Supports upsert/CDC semantics on append-only parquet storage — each new version of a row is appended; only the latest is visible at query time.
- **Trigger**: Non-empty `dedup_views` dict on `Reflection`, populated from table config in Redis (`dedup_on_read: true` + `primary_keys`).
- **Processing**: Wraps the source (possibly already RBAC + tombstone filtered) in a window function view. Internal columns (`__rn__`, `__timestamp__`) are excluded from the outer projection.
- **Output**: Dedup VIEW is the final layer before the user query.
- **Dependencies**: `RedisCatalog.get_table_config()`, `DedupViewDef` data class.
- **Constraints**: Requires `__timestamp__` column in the data. Extra PK + order columns are injected into the column list at the DuckDB Lite level even if not in the user's SELECT.
- **Risks**: If `__timestamp__` is missing from parquet files, the dedup view creation will fail.
- **Confidence**: Explicit.

### 5.6 Snapshot Discovery and Column Validation

- **Description**: `DataEstimator.estimate()` scans Redis for snapshot metadata, resolves parquet file paths to DuckDB-accessible URLs, unions schema across snapshots, and validates that all requested columns exist.
- **Business purpose**: Pre-flight check ensuring query feasibility before expensive execution.
- **Trigger**: Called from `DataReader.execute()` after RBAC check.
- **Processing**: Groups tables by `super_name` → scans Redis for leaf snapshots → filters to requested `simple_name` → reads snapshot payload for resources (file list) and schema → resolves paths via `_to_duckdb_path()` → validates columns via `get_missing_columns()`.
- **Output**: `Reflection` object containing `supers`, `reflection_bytes`, `freshness_ms`, `total_reflections`.
- **Dependencies**: `RedisCatalog`, `SuperTable`, `StorageInterface`.
- **Constraints**: Missing columns or empty file lists raise `RuntimeError`.
- **Risks**: If Redis is unavailable, snapshot discovery fails entirely.
- **Confidence**: Explicit.

### 5.7 MCP Query Interface

- **Description**: `query_sql()` is the public entry point for the MCP server. It enforces a SQL LIMIT, executes via `DataReader`, and converts the result DataFrame to `(columns, rows, columns_meta)`.
- **Business purpose**: Provides the serialization boundary between the engine and the MCP API layer.
- **Trigger**: Called by the MCP server with organization, super_name, SQL, limit, engine, role_name.
- **Processing**: Appends `LIMIT` if missing → creates `DataReader` → executes → sanitizes NA/NaN to None → builds column metadata.
- **Output**: `(List[str], List[List[Any]], List[Dict])`.
- **Dependencies**: `DataReader`, `_ensure_sql_limit()`.
- **Constraints**: Default limit is appended only if no LIMIT clause is present. Sanitizes `pd.NA`, `pd.NaT`, `np.nan` to `None` for JSON safety.
- **Confidence**: Explicit.

### 5.8 DuckDB Lite Execution (Ephemeral)

- **Description**: Per-request DuckDB executor. Uses a persistent connection but creates ephemeral VIEWs per query that are dropped in a `finally` block.
- **Business purpose**: Low-overhead execution for small datasets or actively-written data where caching would churn.
- **Trigger**: Selected by auto-pick for small data or fresh data.
- **Processing**: Reuses persistent connection → creates parquet_scan VIEWs → layers RBAC/tombstone/dedup views → rewrites SQL → executes → drops all views.
- **Cache layers**: DuckDB external file cache (disk), HTTP metadata cache (connection-level), ParquetMetadataCache (removed — dead code).
- **Thread safety**: Lock guards connection creation and httpfs init only; DuckDB supports concurrent reads.
- **Confidence**: Explicit.

### 5.9 DuckDB Pro Execution (Persistent + Cached)

- **Description**: Singleton DuckDB executor with version-based reflection view caching and reference counting.
- **Business purpose**: High-performance execution for stable, repeatedly-queried datasets — avoids redundant parquet footer reads and view creation.
- **Trigger**: Selected by auto-pick for medium stable data.
- **Processing**: Checks view registry → if version matches, reuse existing view (zero cost) → else create new view and mark old as stale → RBAC/tombstone/dedup views are per-query (not cached) → reference counting ensures stale views are dropped only when no in-flight queries use them.
- **Cache layers**: Same as Lite plus version-aware view registry.
- **Thread safety**: Full lock around DDL and registry mutations.
- **Confidence**: Explicit.

### 5.10 Spark Thrift Execution (Remote)

- **Description**: Connects to a remote Spark Thrift Server via PyHive, registers parquet files as temp views (batched unions for multi-file tables), applies RBAC/dedup/tombstone views, transpiles SQL to Spark dialect, executes with per-statement timeouts, and fetches results as a DataFrame.
- **Business purpose**: Handles datasets exceeding single-node DuckDB capacity (10+ GB).
- **Trigger**: Selected by auto-pick for large data, or explicitly by user.
- **Processing**: Select cluster from Redis → connect via PyHive → configure S3 on Spark session → register parquet views (batched for multi-file) → CAST nanosecond timestamps → create view chain → transpile SQL (DuckDB → Spark dialect) → execute with timeout → fetch → cleanup.
- **Special handling**: Timestamp CAST wrappers for DuckDB-written TIMESTAMP(NANOS) columns; `nanosAsLong=true` Spark config; `EXPLAIN EXTENDED` for query plan capture.
- **Timeout**: Overall watchdog (`SUPERTABLE_SPARK_QUERY_TIMEOUT`, default 300s) + per-statement timeout (`SUPERTABLE_SPARK_STATEMENT_TIMEOUT`, default 120s).
- **Dependencies**: PyHive, remote Spark Thrift Server, Redis for cluster selection.
- **Confidence**: Explicit.

---

## 6. Classes, Functions, and Methods

### 6.1 Data Classes (`data_classes.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `TableDefinition` | dataclass | Parsed SQL table reference: super_name, simple_name, alias, columns | Critical |
| `SuperSnapshot` | dataclass | Resolved snapshot: super_name, simple_name, version, files, columns | Critical |
| `RbacViewDef` | dataclass | RBAC filter definition: allowed_columns, where_clause | Critical |
| `DedupViewDef` | dataclass | Dedup config: primary_keys, order_column, visible_columns | Critical |
| `TombstoneDef` | dataclass | Soft-delete keys: primary_keys, deleted_keys | Critical |
| `Reflection` | dataclass | Aggregated estimation result: storage_type, bytes, freshness, supers, all view defs | Critical |

### 6.2 DataReader (`data_reader.py`)

| Name | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|---|---|---|---|---|---|---|
| `DataReader` | class | Facade orchestrating parse → RBAC → estimate → execute → telemetry | super_name, organization, query | — | — | Critical |
| `DataReader.execute()` | method | Main entry: runs full query lifecycle | role_name, with_scan, engine | `(DataFrame, Status, message)` | Writes execution plan; logs timing | Critical |
| `query_sql()` | function | MCP entry: wraps DataReader, serializes result | organization, super_name, sql, limit, engine, role_name | `(columns, rows, columns_meta)` | Sanitizes NA values | Critical |
| `_ensure_sql_limit()` | function | Appends LIMIT to SQL if absent | sql, default_limit | modified SQL string | None | Important |

### 6.3 Executor (`executor.py`)

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `Executor` | class | Engine router: picks engine and dispatches | Holds lite_exec, lazy spark_exec | Critical |
| `Executor._auto_pick()` | method | Selects engine from size + freshness | 3×3 decision matrix with env-configurable thresholds | Critical |
| `Executor.execute()` | method | Dispatches to chosen engine, records engine used in plan_stats | Switch on Engine enum | Critical |
| `_get_pro()` | module function | Thread-safe singleton accessor for DuckDBPro | Double-checked locking | Important |

### 6.4 Engine Enum (`engine_enum.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `Engine` | enum | `AUTO`, `DUCKDB_LITE`, `DUCKDB_PRO`, `SPARK_SQL` | Critical |
| `Engine.dialect` | property | Returns sqlglot dialect string (`"duckdb"` or `"spark"`) | Important |

### 6.5 DuckDB Lite (`duckdb_lite.py`)

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `DuckDBLite` | class | Ephemeral executor with persistent connection | Per-query VIEWs, dropped in finally | Critical |
| `DuckDBLite.execute()` | method | Core execution: create views → rewrite → execute → cleanup | Layered view chain; presign retry; profiling PRAGMAs | Critical |
| `DuckDBLite._get_connection()` | method | Lazy persistent connection creation | init_connection with temp_dir | Important |
| `DuckDBLite._ensure_httpfs()` | method | One-time httpfs + S3 config under lock | Thread-safe; skips if already configured | Important |
| `DuckDBLite._reset_connection()` | method | Discards connection on unrecoverable error | Clears httpfs flag | Supporting |

### 6.6 DuckDB Pro (`duckdb_pro.py`)

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `DuckDBPro` | class | Persistent singleton with version-aware view cache | `_registry` maps (super, simple) → list of `_ProCacheEntry` | Critical |
| `_ProCacheEntry` | dataclass | Cache entry: table_name, version, ref_count, stale flag | Ref-counting for safe concurrent access | Important |
| `DuckDBPro._ensure_view()` | method | Create or reuse cached reflection view by version | Version match → reuse; mismatch → new view + mark old stale | Critical |
| `DuckDBPro._acquire_refs()` / `_release_refs()` | methods | Increment/decrement ref counts for in-flight queries | Prevents premature drop of views used by concurrent queries | Important |
| `DuckDBPro._drop_unreferenced_stale()` | method | Drop stale views with zero refs | Garbage collection for old versions | Important |
| `DuckDBPro.execute()` | method | Full execution with cache management | Lock for DDL; per-query RBAC/dedup/tombstone views | Critical |
| `DuckDBPro.get_cached_tables()` | method | Diagnostic snapshot of registry | For monitoring/debugging | Supporting |
| `DuckDBPro.drop_all()` | method | Drop all views, reset connection | For testing/shutdown | Supporting |

### 6.7 Spark Thrift (`spark_thrift.py`)

| Name | Type | Purpose | Key Logic | Importance |
|---|---|---|---|---|
| `SparkThriftExecutor` | class | Remote Spark SQL executor via PyHive | Cluster selection from Redis; full lifecycle management | Critical |
| `SparkThriftExecutor.execute()` | method | Connect → configure → register → view chain → transpile → execute → fetch → cleanup | Watchdog timeout; timestamp CAST wrappers; EXPLAIN capture | Critical |
| `_spark_create_parquet_view()` | function | Register parquet files as Spark temp views (batched unions) | Returns intermediate views for cleanup; lazy references prevent early drop | Critical |
| `_spark_rewrite_query()` | function | Rewrite table refs + transpile DuckDB → Spark SQL dialect | sqlglot transpile with double-quote → backtick fallback | Important |
| `_spark_create_rbac_view()` | function | RBAC view creation for Spark (backtick quoting) | Mirrors DuckDB version with Spark syntax | Important |
| `_spark_create_dedup_view()` | function | Dedup view for Spark (no EXCLUDE syntax — uses DESCRIBE introspection) | Falls back to `sub.*` if DESCRIBE fails | Important |
| `_spark_create_tombstone_view()` | function | Tombstone anti-join for Spark | Same logic as DuckDB version with backtick quoting | Important |
| `_configure_spark_s3()` | function | SET S3 config on Spark session | Only uses cluster config — does NOT fall back to host env vars | Important |
| `_execute_with_stmt_timeout()` | function | Per-statement timeout enforcement via background thread | Closes connection on timeout to unblock Thrift RPC | Important |
| `_to_s3a_path()` | function | Convert s3:// / http(s):// paths to s3a:// for Spark | Handles presigned URLs by stripping query params | Supporting |
| `_double_quotes_to_backticks()` | function | Fallback identifier quoting conversion | Character-by-character, respects string literals | Supporting |

### 6.8 Engine Common (`engine_common.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `configure_httpfs_and_s3()` | function | Load httpfs extension, configure S3 creds + caches on DuckDB connection | Critical |
| `create_reflection_view()` | function | CREATE VIEW over parquet_scan (lazy, no data read) | Critical |
| `create_reflection_table()` | function | CREATE TABLE AS SELECT from parquet_scan (eager materialization) | Important |
| `create_reflection_view_with_presign_retry()` | function | View creation with automatic presigned URL fallback | Critical |
| `create_rbac_view()` | function | CREATE VIEW with column projection + WHERE clause | Critical |
| `create_dedup_view()` | function | CREATE VIEW with ROW_NUMBER dedup window | Critical |
| `create_tombstone_view()` | function | CREATE VIEW with NOT EXISTS anti-join against VALUES list | Critical |
| `rewrite_query_with_hashed_tables()` | function | sqlglot AST transform: replace table names with hashed view names | Critical |
| `init_connection()` | function | Apply PRAGMAs: memory limit, temp dir, threads, collation, insertion order | Critical |
| `hashed_table_name()` | function | SHA1-based deterministic view name from (super, simple, version, columns) | Important |
| `pro_table_name()` | function | SHA1-based deterministic view name for Pro mode (version-scoped, all columns) | Important |
| `make_presigned_list()` | function | Presign each path via storage.presign(), fall back on failure | Important |
| `_derive_thread_count()` | function | Compute DuckDB thread count from memory limit and CPU cores | Important |
| `_parse_memory_limit_mb()` | function | Parse DuckDB memory limit string to MB | Supporting |

### 6.9 DataEstimator (`data_estimator.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `DataEstimator` | class | Resolves snapshots → files → schema; validates columns | Critical |
| `DataEstimator.estimate()` | method | Main API: returns `Reflection` | Critical |
| `DataEstimator._to_duckdb_path()` | method | Resolve storage key to DuckDB-usable URL (presign, httpfs, s3://) | Important |
| `DataEstimator._collect_snapshots_from_redis()` | method | Scan Redis leaf items for a super_name | Important |
| `DataEstimator._filter_snapshots()` | method | Filter snapshots by simple_name (or all non-system tables) | Important |
| `get_missing_columns()` | module function | Compare requested columns against available schema; return missing | Important |

### 6.10 PlanStats (`plan_stats.py`)

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `PlanStats` | class | Simple list-based stat accumulator | Supporting |
| `PlanStats.add_stat()` | method | Append a dict to stats list | Supporting |

---

## 7. End-to-End Workflows

### 7.1 Standard Query Execution (via `query_sql`)

1. **Entry**: `query_sql(organization, super_name, sql, limit, engine, role_name)` called by MCP server.
2. **Limit guard**: `_ensure_sql_limit()` appends `LIMIT <n>` if the SQL has no trailing LIMIT clause.
3. **DataReader creation**: Instantiates `DataReader` with organization, super_name, SQL.
4. **DataReader.execute()** begins:
   - 4a. Creates `SQLParser` with engine dialect → extracts `TableDefinition` list.
   - 4b. Calls `restrict_read_access()` → returns `Dict[str, RbacViewDef]` (or raises `PermissionError`).
   - 4c. Creates `Executor` with storage reference.
   - 4d. Initializes `QueryPlanManager` (query ID, hash, paths).
   - 4e. Creates `DataEstimator` → calls `estimate()`:
     - Scans Redis for snapshots per super_name.
     - Filters to requested simple_names.
     - Reads snapshot payloads for file lists and schema.
     - Resolves file paths to DuckDB URLs.
     - Validates requested columns against available schema.
     - Returns `Reflection`.
   - 4f. Wires `rbac_views` onto Reflection.
   - 4g. Looks up dedup config and tombstone metadata from Redis; wires `dedup_views` and `tombstone_views` onto Reflection.
   - 4h. If no supers found → return empty DataFrame with ERROR.
   - 4i. Calls `executor.execute(engine, reflection, parser, ...)`:
     - If `engine == AUTO`, calls `_auto_pick(reflection)` → selects engine.
     - Dispatches to chosen engine's `execute()`.
5. **Engine execution** (e.g., DuckDB Lite):
   - 5a. Get/create persistent connection.
   - 5b. Map aliases to snapshot files; compute hashed table names.
   - 5c. Configure httpfs + S3 (once per connection).
   - 5d. Create parquet_scan VIEWs per alias (with presign retry).
   - 5e. Create RBAC views (if rbac_views non-empty).
   - 5f. Create tombstone views (if tombstone_views non-empty).
   - 5g. Create dedup views (if dedup_views non-empty).
   - 5h. Rewrite SQL: alias → final view name in chain.
   - 5i. Enable profiling; execute rewritten SQL; fetch DataFrame.
   - 5j. Finally: disable profiling; drop all per-query views.
6. **Telemetry**: `extend_execution_plan()` records timing, stats, status, result shape.
7. **Return**: `(DataFrame, Status.OK, None)` on success.
8. **MCP serialization**: `query_sql()` converts DataFrame to `(columns, rows, columns_meta)` with NA sanitization.

**Failure modes**:
- RBAC denial → `PermissionError` (before execution).
- Missing columns → `RuntimeError` from `DataEstimator`.
- No parquet files → empty DataFrame + ERROR status.
- S3 HTTP errors → presigned URL retry; if retry also fails → exception.
- Spark timeout → `RuntimeError` with timeout message.
- DuckDB OOM → spills to disk (if temp_dir configured) or raises.

**Observability**:
- Structured logging with `[qid=... qh=...]` prefix.
- Timer captures per-phase durations (CONNECTING, CREATING_REFLECTION, EXECUTING_QUERY, etc.).
- PlanStats records ENGINE, REFLECTIONS, REFLECTION_SIZE.
- DuckDB JSON profiling output to `query_plan_path`.
- Spark EXPLAIN EXTENDED output saved as JSON to same path.

### 7.2 DuckDB Pro View Cache Lifecycle

1. **First query** for table (super_A, simple_B, v3):
   - `_ensure_view()`: no entry in registry → create lazy VIEW `pro_<hash>_v3` → add `_ProCacheEntry(v3, ref_count=0, stale=False)`.
2. **Subsequent queries** for same table + version:
   - `_ensure_view()`: version matches → return cached view name immediately (zero cost).
3. **Data version changes** to v4:
   - `_ensure_view()`: version mismatch → mark v3 entry as stale → create VIEW `pro_<hash>_v4`.
   - `_drop_unreferenced_stale()`: if v3 ref_count == 0 → DROP VIEW.
4. **Concurrent queries**: During execution, `_acquire_refs()` increments ref_count; on completion (in `finally`), `_release_refs()` decrements and `_drop_unreferenced_stale()` cleans up.

### 7.3 Spark Thrift Full Lifecycle

1. Select cluster from Redis based on job byte size.
2. Connect to Thrift Server via PyHive (with socket timeout).
3. Start watchdog timer thread.
4. Configure S3 credentials on Spark session from cluster config.
5. Apply timestamp nanosecond workarounds.
6. Register parquet files as temp views (batched unions for multi-file).
7. DESCRIBE each view; CAST nanosecond BIGINT columns to TIMESTAMP.
8. Create RBAC → tombstone → dedup view chain.
9. Transpile SQL from DuckDB to Spark dialect.
10. Capture query plan via EXPLAIN EXTENDED.
11. Execute user query with per-statement timeout.
12. Fetch results, build DataFrame.
13. Cleanup: drop all views, close cursor, close connection, cancel watchdog.

---

## 8. Data Model and Information Flow

### Core Entities

```
TableDefinition (from SQLParser)
  ├─ super_name: str          — logical database / namespace
  ├─ simple_name: str         — logical table name within super
  ├─ alias: str               — SQL alias used in query
  └─ columns: List[str]       — requested columns ([] = SELECT *)

SuperSnapshot (from DataEstimator)
  ├─ super_name, simple_name  — identity
  ├─ simple_version: int      — snapshot version (for cache keying)
  ├─ files: List[str]         — resolved parquet file URLs
  └─ columns: Set[str]        — available column names (lowercase)

Reflection (estimation output, engine input)
  ├─ storage_type: str        — storage backend class name
  ├─ reflection_bytes: int    — total parquet file size
  ├─ total_reflections: int   — total parquet file count
  ├─ freshness_ms: int        — max last_updated_ms across snapshots
  ├─ supers: List[SuperSnapshot]
  ├─ rbac_views: Dict[alias, RbacViewDef]
  ├─ dedup_views: Dict[alias, DedupViewDef]
  └─ tombstone_views: Dict[alias, TombstoneDef]
```

### Information Flow

```
Redis (metadata)                         S3 (data)
     │                                       │
     ▼                                       │
DataEstimator.estimate()                     │
     │                                       │
     ▼                                       │
Reflection                                   │
     │                                       │
     ▼                                       ▼
Executor → Engine.execute() ─── parquet_scan(files) ──→ DuckDB/Spark
     │                              │
     │                         VIEW chain:
     │                         base → RBAC → tombstone → dedup
     │                              │
     ▼                              ▼
PlanStats / Timer              Rewritten SQL executed
                                    │
                                    ▼
                               pd.DataFrame
                                    │
                                    ▼
                        query_sql() → (columns, rows, meta) → MCP
```

### Stateful vs Stateless

| Component | State | Lifetime |
|---|---|---|
| DuckDB Lite connection | Persistent across requests | Process lifetime |
| DuckDB Lite views | Ephemeral per-query | Single query |
| DuckDB Pro connection | Persistent singleton | Process lifetime |
| DuckDB Pro view cache | Version-scoped, ref-counted | Until version changes + zero refs |
| DuckDB Pro RBAC/dedup/tombstone views | Ephemeral per-query | Single query |
| Spark Thrift connection | Per-query | Single query |
| Spark Thrift views | Per-query | Single query |
| DataEstimator | Stateless per-call | Single estimate() |
| DataReader | Stateless per-call | Single execute() |

---

## 9. Dependencies and Integrations

### Internal Modules

| Module | Role |
|---|---|
| `supertable.utils.sql_parser.SQLParser` | SQL parsing via sqlglot; table/column extraction |
| `supertable.query_plan_manager.QueryPlanManager` | Query ID generation, plan path management |
| `supertable.plan_extender.extend_execution_plan` | Post-execution telemetry writer |
| `supertable.rbac.access_control.restrict_read_access` | RBAC gate; returns per-alias RbacViewDef dict |
| `supertable.redis_catalog.RedisCatalog` | Redis-backed metadata catalog (snapshots, table config, cluster selection) |
| `supertable.storage.storage_factory.get_storage` | Storage backend factory |
| `supertable.storage.storage_interface.StorageInterface` | Storage abstraction (presign, path resolution) |
| `supertable.super_table.SuperTable` | Snapshot reading fallback when Redis payload is incomplete |
| `supertable.utils.timer.Timer` | Per-phase timing capture |
| `supertable.utils.helper.dict_keys_to_lowercase` | Schema normalization |
| `supertable.config.defaults.logger` | Centralized logger |
| `supertable.config.homedir.get_app_home` | App home directory for temp/spill paths |

### Third-Party Packages

| Package | Role | Required By |
|---|---|---|
| `duckdb` | In-process SQL engine | DuckDB Lite, DuckDB Pro, engine_common |
| `pandas` | DataFrame result format | All engines |
| `sqlglot` | SQL parsing, AST rewriting, dialect transpilation | engine_common, spark_thrift, SQLParser |
| `pyhive` | Thrift Server connection (HiveServer2 protocol) | SparkThriftExecutor (optional; import guarded) |
| `pyspark` | Spark availability check (import only) | Executor._auto_pick (optional) |

### External Systems

| System | Role | Access Pattern |
|---|---|---|
| **Redis** | Metadata store: snapshots, table configs, tombstones, Spark clusters | Read via RedisCatalog |
| **S3-compatible storage** | Parquet file storage | Read via DuckDB httpfs or Spark s3a |
| **Spark Thrift Server** | Remote SQL execution for large datasets | TCP via PyHive (HiveServer2 protocol) |

### Environment Variables (Complete)

Documented in Section 5 and the engine_documentation.md; 25+ env vars controlling engine thresholds, DuckDB memory/threads/caching, S3 credentials, and Spark timeouts. See `engine_documentation.md` Environment Variables section for the full reference.

---

## 10. Architecture Positioning

### Upstream Components

- **MCP Server**: Calls `query_sql()` — the primary external entry point.
- **SQLParser**: Provides parsed table definitions and column lists.
- **RBAC Module** (`restrict_read_access`): Provides security view definitions.
- **Redis Catalog**: Provides snapshot metadata, table configurations, Spark cluster registry.

### Downstream Systems

- **S3 / Object Storage**: Data source (parquet files).
- **Spark Thrift Server**: Remote compute for large queries.
- **Plan Extender**: Consumes timing + stats for telemetry/observability.

### Architectural Role

The engine is the **execution core** of the Supertable product — it sits at the intersection of:
- Metadata plane (Redis) — what tables/files exist
- Data plane (S3) — where the data lives
- Security plane (RBAC) — who can see what
- Compute plane (DuckDB/Spark) — how to execute

It is a **domain orchestration layer** that:
1. Resolves logical table references to physical files
2. Enforces security policies as SQL views
3. Applies data-integrity transformations (dedup, tombstone)
4. Selects and manages compute engines
5. Produces structured execution telemetry

### Boundaries

- The engine does **not** manage data ingestion, schema evolution, or snapshot creation.
- The engine does **not** own RBAC policy definition — it consumes `RbacViewDef` from the RBAC module.
- The engine does **not** manage Spark cluster lifecycle — it consumes cluster info from Redis.
- The engine **does own** the view chain construction, SQL rewriting, and engine selection logic.

### Coupling

- **Tightly coupled** to: DuckDB (SQL dialect, VIEWs, PRAGMAs), Redis metadata schema, parquet file format.
- **Loosely coupled** to: Spark (optional, import-guarded), storage backend (via interface).

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Enables SQL analytics over parquet data lakes with multi-tenant security, soft-delete semantics, and automatic engine scaling. |
| **Revenue relevance** | Directly revenue-generating — this is the core query execution capability that customers pay for. |
| **Differentiating vs commodity** | **Differentiating**: The auto-engine selection, layered view chain (RBAC + tombstone + dedup), and seamless DuckDB ↔ Spark scaling are non-trivial; the combination is uncommon in the market. |
| **Efficiency value** | Auto-pick eliminates manual engine tuning. Presigned URL retry and caching reduce operational incidents. |
| **Compliance value** | RBAC enforcement at the engine level ensures data isolation without relying on application-layer filtering. |
| **Integration value** | MCP-facing `query_sql()` enables AI/LLM-driven analytics workflows. |
| **Impact of removal** | Product ceases to function — no query execution capability. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | High. Three execution backends, layered view chain, version-aware caching with ref-counting, cross-dialect SQL transpilation, multiple timeout mechanisms. |
| **Architectural maturity** | High. Clean separation between estimation, execution, and telemetry. Strategy pattern for engines. Facade for external consumers. |
| **Maintainability** | Good. Shared logic centralized in `engine_common.py`. Each engine is self-contained. Data classes are well-documented. |
| **Extensibility** | Good. Adding a new engine requires implementing `execute()` with the same signature and adding an enum value. View chain is composable. |
| **Operational sensitivity** | High. Memory limits, thread counts, and cache settings directly affect stability. Misconfiguration can cause OOM or degraded performance. |
| **Performance** | Tuned. Lazy VIEWs enable DuckDB pushdown. HTTP metadata cache and external file cache reduce network I/O. Pro's version-aware cache avoids redundant view creation. Thread count is derived from memory to prevent OOM. |
| **Reliability** | Good. Presigned URL retry. Connection reset on unrecoverable error. Per-query view cleanup in `finally` blocks. Spark watchdog timeout. |
| **Security** | RBAC views enforce column + row filtering at the SQL level. Per-query UUID suffixes prevent cross-query view collisions. No credential leakage in logs (credentials set via PRAGMAs, not logged). |
| **Testing** | Referenced in documentation: 198 tests in `test_engine.py`, 27 tests in `test_dedup_read.py`. Test fixtures include mock Redis and DuckDB connections. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- Three execution engines: DuckDB Lite, DuckDB Pro, Spark Thrift.
- Auto-pick algorithm based on byte size and freshness with env-configurable thresholds.
- View chain order: parquet_scan → RBAC → tombstone → dedup.
- Per-query UUID suffixes on RBAC/tombstone/dedup views for concurrency safety.
- DuckDB Pro uses version-based view caching with reference counting.
- Spark executor uses PyHive, batched union for multi-file tables, sqlglot transpilation, timestamp CAST wrappers.
- `query_sql()` sanitizes pandas NA/NaN to None for JSON serialization.
- `_ensure_sql_limit()` appends LIMIT only if no existing LIMIT clause.
- Presigned URL retry on specific HTTP error tokens.
- DuckDB connection init: memory limit, temp dir, collation, thread count, insertion order.
- Tombstone anti-join uses `NOT EXISTS` with `VALUES` list.
- Dedup uses `ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ... DESC) = 1`.

### Reasonable Inferences

- `augment_rbac_columns()` exists in the RBAC module to ensure PK/order columns pass through RBAC views for downstream dedup (referenced in documentation, not in provided code).
- The product is a multi-tenant analytical platform where organizations have independent data namespaces.
- `super_name` is a top-level organizational unit (like a database); `simple_name` is a table within it.
- The MCP server is the primary consumer of `query_sql()`, suggesting LLM/AI-driven analytics as a key use case.
- Redis is used as a fast metadata index rather than persistent storage — snapshot payloads can be read from S3 as fallback.

### Unknown / Not Visible in Provided Snippet

- Full RBAC module implementation (`restrict_read_access`, `augment_rbac_columns`, role/permission model).
- `SQLParser` internals (table/column extraction logic, dialect handling).
- `QueryPlanManager` internals (query ID generation, plan path construction).
- `extend_execution_plan` internals (what telemetry is written and where).
- `StorageInterface` implementation (which storage backends are supported).
- `RedisCatalog` implementation (Redis key schema, cluster selection algorithm).
- How `super_name` and `organization` relate in the broader data model.
- Whether there is a query result cache above this layer.
- Authentication/authorization flow before `query_sql()` is called.

---

## 14. AI Consumption Notes

- **Canonical feature name**: `Supertable Query Engine`
- **Alternative names**: engine, execution engine, query executor, data reader
- **Main responsibilities**: SQL-over-parquet execution, engine auto-selection, RBAC view enforcement, tombstone filtering, dedup-on-read, query plan telemetry
- **Important entities**: `Reflection`, `SuperSnapshot`, `TableDefinition`, `RbacViewDef`, `DedupViewDef`, `TombstoneDef`, `Engine` enum, `PlanStats`
- **Important workflows**: query_sql → DataReader.execute → estimate → execute → telemetry; auto-pick decision matrix; view chain construction; DuckDB Pro cache lifecycle
- **Integration points**: Redis (metadata), S3 (data), Spark Thrift Server (remote compute), MCP server (API consumer), RBAC module (security)
- **Business purpose keywords**: analytics, SQL, parquet, multi-tenant, RBAC, soft-delete, dedup, auto-scaling, data lake, MCP
- **Architecture keywords**: facade, strategy, singleton, view chain, ref-counting, lazy view, presigned retry, transpilation
- **Follow-up files for completeness**:
  - `supertable/utils/sql_parser.py` — SQL parsing and table/column extraction
  - `supertable/rbac/access_control.py` — RBAC enforcement logic and `augment_rbac_columns()`
  - `supertable/redis_catalog.py` — Redis metadata schema and operations
  - `supertable/query_plan_manager.py` — Query ID/hash generation and plan paths
  - `supertable/plan_extender.py` — Telemetry writing logic
  - `supertable/storage/storage_interface.py` — Storage abstraction contract
  - `supertable/rbac/filter_builder.py` — JSON filter → SQL WHERE conversion

---

## 15. Suggested Documentation Tags

`query-engine`, `sql-execution`, `parquet`, `duckdb`, `spark`, `thrift`, `auto-engine-selection`, `rbac-enforcement`, `column-security`, `row-security`, `tombstone`, `soft-delete`, `dedup-on-read`, `view-chain`, `data-lake`, `s3-storage`, `redis-metadata`, `multi-tenant`, `mcp-api`, `presigned-url`, `query-rewriting`, `sqlglot`, `execution-telemetry`, `caching`, `ref-counting`, `version-aware-cache`, `facade-pattern`, `strategy-pattern`, `singleton`, `domain-logic`, `orchestration-layer`, `core-product-feature`

---

## Merge Readiness

- **Suggested canonical name**: `Supertable Query Engine`
- **Standalone or partial**: Substantially standalone for engine documentation. Partial for full product documentation — depends on RBAC, SQL parser, Redis catalog, storage, and telemetry modules.
- **Related components to merge**:
  - RBAC module (`supertable/rbac/`) — security policy definition and `augment_rbac_columns()`
  - SQL Parser (`supertable/utils/sql_parser.py`) — query parsing contract
  - Redis Catalog (`supertable/redis_catalog.py`) — metadata schema and operations
  - Storage Layer (`supertable/storage/`) — backend abstraction
  - Plan Extender / Query Plan Manager — telemetry pipeline
- **Files that would most improve certainty**:
  - `supertable/rbac/access_control.py` (RBAC enforcement + column augmentation)
  - `supertable/redis_catalog.py` (Redis key schema, cluster selection)
  - `supertable/utils/sql_parser.py` (parsing contract, column extraction rules)
- **Recommended merge key phrases**: `query engine`, `execution engine`, `data reader`, `engine auto-pick`, `reflection`, `view chain`, `RBAC view`, `dedup view`, `tombstone view`, `DuckDB Lite`, `DuckDB Pro`, `Spark Thrift`
