# Supertable Query Engine

## Overview

The query engine executes SQL against parquet files stored in S3-compatible object storage. It resolves table references from Redis metadata, creates DuckDB or Spark views over the parquet files, applies RBAC/tombstone/dedup view chains, rewrites the user SQL to target those views, executes, and returns a pandas DataFrame.

## Execution Flow

```
query_sql() / DataReader.execute()
  │
  ├─ SQLParser            Parse SQL, extract table aliases + columns
  ├─ restrict_read_access  RBAC gate (raise on deny, return view defs)
  ├─ DataEstimator         Resolve snapshots from Redis, collect files, validate columns
  │    └─ Returns Reflection (files, bytes, freshness_ms, schema)
  ├─ Wire rbac_views onto Reflection
  ├─ Wire dedup_views + tombstone_views from Redis table config
  ├─ Executor._auto_pick   Choose engine (Lite / Pro / Spark)
  └─ Engine.execute
       ├─ Create reflection views (parquet_scan)
       ├─ Create RBAC view         (column + row filtering)
       ├─ Create tombstone view    (soft-delete anti-join)
       ├─ Create dedup view        (ROW_NUMBER latest-per-key)
       ├─ Rewrite SQL table refs → hashed view names
       └─ Execute + return DataFrame
```

## Engines

### DuckDB Lite (`duckdb_lite.py`)
Ephemeral. New instance per request — connection and all state are discarded after the query. No caching. Best for small datasets or data that is actively being written to (versions churning).

### DuckDB Pro (`duckdb_pro.py`)
Persistent singleton. Keeps a long-lived connection with version-based view caching, HTTP metadata cache (parquet footers), and optional external file cache (disk). Old views are ref-counted and dropped when stale + unreferenced. Best for stable, repeatedly-queried datasets.

### Spark Thrift (`spark_thrift.py`)
Connects to a remote Spark Thrift Server via PyHive. Registers parquet files as temp views (batched unions for multi-file tables), applies timestamp CAST wrappers for DuckDB-written nanos columns, transpiles SQL from DuckDB dialect to Spark dialect. Per-statement timeout via `_execute_with_stmt_timeout`. Best for datasets exceeding single-node DuckDB capacity (10+ GB).

## Engine Auto-Selection

`Executor._auto_pick()` chooses based on **data size** and **data freshness** (age of most recent snapshot):

```
                        Data freshness
                   FRESH (<5 min)       STABLE (≥5 min)
              ┌─────────────────────┬─────────────────────┐
  Small       │       LITE          │       LITE          │
  (≤100 MB)   │  cheap anyway       │  cheap anyway       │
              ├─────────────────────┼─────────────────────┤
  Medium      │       LITE          │       PRO           │
  (100MB–10GB)│  cache would churn  │  cache pays off     │
              ├─────────────────────┼─────────────────────┤
  Large       │       SPARK *       │       SPARK *       │
  (≥10 GB)    │  too big for DuckDB │  too big for DuckDB │
              └─────────────────────┴─────────────────────┘

* Spark only if pyspark is importable; falls back to PRO otherwise.
```

Freshness is derived from `Reflection.freshness_ms` — the max `last_updated_ms` across all snapshots involved in the query. Unknown freshness (0) is treated as stable.

## View Chain

Views are layered in this order (each wraps the previous):

```
parquet_scan → RBAC view → tombstone view → dedup view → user query
```

**RBAC view**: Column projection (`allowed_columns`) + row filter (`WHERE` clause) from role definition. Columns required by downstream tombstone/dedup views are automatically augmented via `augment_rbac_columns()` — the dedup view's outer projection hides them from the user.

**Tombstone view**: `NOT EXISTS` anti-join against a `VALUES` list of soft-deleted composite keys. Bounded by compaction threshold (~1000 keys max).

**Dedup view**: `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY <order_col> DESC) = 1`. Keeps only the latest row per primary key. Internal columns (`__rn__`, `__timestamp__`) are excluded from the outer projection.

Each view name includes a per-query UUID suffix to prevent collisions under concurrency.

## Key Data Classes

**`Reflection`** — Output of `DataEstimator.estimate()`. Carries everything the executor needs:
- `supers: List[SuperSnapshot]` — resolved file lists per table
- `reflection_bytes: int` — total parquet file size
- `freshness_ms: int` — max last_updated_ms across snapshots
- `rbac_views: Dict[str, RbacViewDef]` — per-alias RBAC filters
- `dedup_views: Dict[str, DedupViewDef]` — per-alias dedup config
- `tombstone_views: Dict[str, TombstoneDef]` — per-alias soft-delete keys

**`TableDefinition`** — From `SQLParser.get_table_tuples()`:
- `super_name, simple_name, alias` — table identity
- `columns: List[str]` — requested columns (`[]` means SELECT *)

**`RbacViewDef`** — `allowed_columns` + `where_clause`
**`DedupViewDef`** — `primary_keys` + `order_column` + `visible_columns`
**`TombstoneDef`** — `primary_keys` + `deleted_keys`

## Environment Variables

### Engine Selection
| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_ENGINE_LITE_MAX_BYTES` | `104857600` (100 MB) | Upper bound for Lite in AUTO mode |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | `10737418240` (10 GB) | Lower bound for Spark in AUTO mode |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | `300` (5 min) | Age threshold separating fresh vs stable data |

### DuckDB Configuration
| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | `1GB` | DuckDB memory cap (both Lite and Pro) |
| `SUPERTABLE_DUCKDB_THREADS` | auto-derived | Explicit thread count override |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | `3` | CPU × multiplier for IO threads (auto mode) |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | DuckDB default (30s) | HTTP timeout in seconds |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | `1` (on) | Parquet footer cache on persistent connection |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | unset (off) | External file cache size (enables disk cache) |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | unset | Disk cache directory |
| `SUPERTABLE_DUCKDB_MATERIALIZE` | `view` | `view` (lazy) or `table` (eager materialisation) |

### S3 / Storage
| Variable | Default | Description |
|---|---|---|
| `STORAGE_ENDPOINT_URL` | unset | S3-compatible endpoint |
| `STORAGE_REGION` | `us-east-1` | S3 region |
| `STORAGE_ACCESS_KEY` | unset | S3 access key |
| `STORAGE_SECRET_KEY` | unset | S3 secret key |
| `STORAGE_SESSION_TOKEN` | unset | STS session token |
| `STORAGE_BUCKET` | unset | Default bucket |
| `STORAGE_FORCE_PATH_STYLE` | `true` | Path-style vs vhost-style URLs |
| `STORAGE_USE_SSL` | unset (off) | Enable HTTPS for S3 |
| `SUPERTABLE_DUCKDB_PRESIGNED` | unset (off) | Force presigned URLs for all paths |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | unset (off) | Force HTTP URLs instead of `s3://` |

### Spark Thrift
| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | `300` (5 min) | Overall query timeout (watchdog) |
| `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | `120` (2 min) | Per-statement timeout (EXPLAIN, user query) |
| `SUPERTABLE_SPARK_CONNECT_TIMEOUT` | `30` | Socket connect timeout |
| `SUPERTABLE_SPARK_BATCH_SIZE` | `50` | Files per batch when registering views |

## File Map

```
supertable/engine/
├── __init__.py              Package exports (Engine, Executor, PlanStats, DataEstimator)
├── engine_enum.py           Engine enum (AUTO, DUCKDB_LITE, DUCKDB_PRO, SPARK_SQL) + dialect
├── engine_common.py         Shared: S3 config, httpfs, view creation (reflection/RBAC/dedup/tombstone), query rewriting, connection init, augment_rbac_columns
├── executor.py              Engine router + auto-pick logic
├── duckdb_lite.py           Ephemeral DuckDB executor (fire-and-forget)
├── duckdb_pro.py            Persistent DuckDB executor (singleton, view cache, ref-counting)
├── spark_thrift.py          Spark Thrift executor (PyHive, s3a paths, timestamp CAST, transpile)
├── data_estimator.py        Snapshot resolution, file collection, column validation
├── plan_stats.py            Simple stat accumulator for execution plans
└── tests/
    ├── conftest.py          Shared fixtures (mock Redis, DuckDB connection, clean env)
    ├── test_engine.py        198 tests covering all modules
    └── test_dedup_read.py   27 tests for dedup-on-read flow

supertable/data_classes.py   Reflection, SuperSnapshot, RbacViewDef, DedupViewDef, TombstoneDef, TableDefinition
supertable/data_reader.py    Facade: parse → RBAC → estimate → wire views → execute → extend plan
supertable/utils/sql_parser.py  SQLParser: table/column extraction via sqlglot

supertable/rbac/
├── access_control.py        restrict_read_access (returns RbacViewDef dict), check_write_access, check_meta_access
├── permissions.py           RoleType enum, Permission enum, ROLE_PERMISSIONS matrix
├── role_manager.py          CRUD for roles (Redis-backed)
├── filter_builder.py        JSON filter → SQL WHERE clause
├── row_column_security.py   Role validation + content hashing
└── user_manager.py          CRUD for users (Redis-backed)
```

## RBAC Flow

```
restrict_read_access(role_name, tables)
  ├─ Resolve role from Redis via RoleManager
  ├─ Check READ permission (raise PermissionError if denied)
  ├─ Superadmin/Admin → return {} (no filtering)
  └─ Reader/Writer with restrictions:
       ├─ Validate table access (role.tables)
       ├─ Validate column access (role.columns vs requested columns)
       ├─ Build WHERE clause from role.filters via FilterBuilder
       └─ Return {alias: RbacViewDef(allowed_columns, where_clause)}
```

The returned dict is set on `Reflection.rbac_views` before execution. All three executors create filtered views from these definitions. `augment_rbac_columns()` ensures PK and order columns needed by downstream dedup/tombstone views pass through the RBAC view — the dedup outer projection hides them from the user.

## SQL Parsing

`SQLParser(super_name, query, dialect)` uses sqlglot to extract:
- Table references with schema resolution (missing schema → default `super_name`)
- Per-alias column lists (qualified and unqualified resolution)
- Star semantics: `SELECT *` → `columns=[]` for all tables; `SELECT t.*` → `columns=[]` for alias `t`
- SELECT alias detection to avoid recording computed aliases as physical columns

Output: `List[TableDefinition]` consumed by RBAC, estimator, and executors.
