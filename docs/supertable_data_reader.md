# Feature / Component Reverse Engineering — Supertable Data Reader Subsystem

## 1. Executive Summary

This code implements the **SQL query execution pipeline** of the Supertable product — a multi-tenant analytical data platform that stores columnar data (Parquet) on object storage (MinIO/S3/local) and uses Redis as a metadata catalog and coordination layer. The `DataReader` is the central facade that receives a SQL query, resolves table metadata from Redis, estimates the data footprint, enforces role-based access control (RBAC), applies deduplication and tombstone filtering, selects an execution engine (DuckDB or Spark), and returns a Pandas DataFrame.

- **Main responsibility**: End-to-end orchestration of SQL read queries against a virtual table layer ("SuperTable") with security, dedup, tombstone, and multi-engine support.
- **Likely product capability**: MCP-compatible SQL analytics API — enables BI tools, LLM agents, and dashboards to query multi-table, multi-version, access-controlled datasets.
- **Business purpose**: Provides the read path of a lakehouse-style analytical platform; transforms raw Parquet on object storage into a queryable, governed data surface.
- **Technical role**: Facade / orchestration layer sitting between the API/MCP surface and the execution engines (DuckDB, Spark).

The surrounding files form a coherent subsystem: `SuperTable` and `SimpleTable` manage hierarchical table metadata and snapshots; `RedisCatalog` provides the metadata store and distributed locking; `RedisConnector` handles connection lifecycle with Sentinel HA; `QueryPlanManager` manages DuckDB profiling artifacts; `PlanExtender` enriches and persists query execution metrics; `Staging` manages data staging areas; and `data_classes` define the DTOs that wire these layers together.

---

## 2. Functional Overview

### What the Product Can Do Because of This Code

1. **Ad-hoc SQL analytics over versioned Parquet data** — users (human or AI agents) submit SQL queries through an MCP server or API and receive tabular results. The underlying data lives on object storage and is versioned through snapshot chains.

2. **Multi-tenant query isolation** — every query is scoped to an `organization` and `super_name`, ensuring tenants cannot access each other's data.

3. **Role-based column and row filtering** — RBAC is enforced at query time via `restrict_read_access()`, which produces per-alias `RbacViewDef` definitions that are applied as SQL views inside the execution engine.

4. **Deduplication-on-read** — tables configured with `dedup_on_read` and `primary_keys` get automatic CDC-style latest-row semantics using `ROW_NUMBER()` windowing, without requiring physical compaction.

5. **Soft-delete / tombstone filtering** — deleted key combinations stored in snapshot metadata are excluded at query time, enabling non-destructive deletes.

6. **Multi-engine execution** — an `AUTO` engine picker can select between DuckDB (in-process, low-latency) and Spark (distributed, high-volume) based on data size and freshness heuristics.

7. **Query observability and monitoring** — every query produces a profiling artifact (DuckDB JSON profile), enriched with app-level timings, and logged to a monitoring table via `MonitoringWriter`.

8. **MCP server integration** — `query_sql()` is the top-level entry point formatted specifically for Model Context Protocol consumers, returning `(columns, rows, columns_meta)`.

### Target Users / Actors

| Actor | Interaction |
|---|---|
| **LLM agents (MCP clients)** | Submit SQL via `query_sql()`, receive structured tabular results |
| **BI dashboards / API consumers** | Execute SQL queries through the API layer |
| **Data engineers** | Configure tables (dedup, primary keys, tombstones) via Redis catalog |
| **Platform operators** | Monitor query performance via plan metrics; manage Sentinel HA |
| **Security admins** | Define roles with column/row restrictions enforced at query time |

### Business Value

- **Core feature**: This is the primary read path of the entire product. Without it, no query can be answered.
- **Revenue-enabling**: Query execution is the foundational capability that makes the platform useful for analytics, BI, and AI-agent workloads.
- **Governance**: RBAC enforcement, dedup, and tombstone filtering provide compliance and data-correctness guarantees that are prerequisites for enterprise adoption.

---

## 3. Technical Overview

### Architectural Style

Facade + Pipeline pattern. `DataReader.execute()` is a linear pipeline:

```
SQL Parse → RBAC Check → Estimate → Dedup/Tombstone Enrichment → Execute → Plan Extension → Return
```

The system follows a **metadata-in-Redis, data-on-storage** split architecture:
- Redis holds pointers (leaf/root), RBAC definitions, table configs, locking tokens, and optionally snapshot payloads.
- Object storage (MinIO/S3/local via `StorageInterface`) holds Parquet data files and heavy snapshot JSON.

### Major Modules

| Module | Role |
|---|---|
| `DataReader` | Orchestration facade for query execution |
| `query_sql()` | MCP-facing entry point with LIMIT guard and result formatting |
| `SuperTable` | Root-level namespace object; ensures meta:root exists |
| `SimpleTable` | Table-level object; manages snapshot lifecycle (create/update/delete) |
| `RedisCatalog` | Redis-backed metadata catalog with CAS, locking, RBAC, staging |
| `RedisConnector` | Connection factory with Sentinel HA and fallback |
| `QueryPlanManager` | DuckDB profiling file management and cleanup |
| `extend_execution_plan()` | Post-query metric enrichment and monitoring |
| `Staging` | Pre-commit data staging area with locking |
| `data_classes` | DTOs: `Reflection`, `RbacViewDef`, `DedupViewDef`, `TombstoneDef`, etc. |

### Key Design Patterns

- **CAS (Compare-and-Swap) via Lua** — leaf pointer updates use atomic Lua scripts to increment version and swap path/payload.
- **Distributed locking** — Redis `SET NX EX` with token-based release via Lua; used for writes, staging, stats.
- **Snapshot chain** — each `SimpleTable` update creates a new snapshot JSON pointing to the previous one, forming an immutable version chain.
- **View layering** — RBAC, dedup, and tombstone definitions are encoded as view definitions (`RbacViewDef`, `DedupViewDef`, `TombstoneDef`) attached to the `Reflection` object and materialized as SQL views by the executor.
- **Sentinel with fallback** — Redis connector tries Sentinel first, falls back to direct connection if not strict, providing HA without hard dependency.

---

## 4. Files / Modules Analyzed

| File | Primary Responsibility | Notable Classes/Functions | Relevance |
|---|---|---|---|
| `data_reader.py` | Query orchestration facade | `DataReader`, `query_sql()`, `_ensure_sql_limit()` | **Core** — the central read pipeline |
| `data_classes.py` | Domain DTOs | `TableDefinition`, `SuperSnapshot`, `Reflection`, `RbacViewDef`, `DedupViewDef`, `TombstoneDef` | **Core** — data contracts between all layers |
| `redis_catalog.py` | Metadata catalog (Redis) | `RedisCatalog` (~50+ methods), Lua scripts for CAS | **Core** — the metadata backbone |
| `redis_connector.py` | Redis connection factory | `RedisOptions`, `RedisConnector`, `create_redis_client()` | **Infrastructure** — HA connectivity |
| `query_plan_manager.py` | DuckDB profile management | `QueryPlanManager` | **Supporting** — profiling/observability |
| `plan_extender.py` | Post-query metric enrichment | `extend_execution_plan()` | **Supporting** — monitoring pipeline |
| `super_table.py` | Root namespace management | `SuperTable` | **Core** — top-level entity lifecycle |
| `simple_table.py` | Table snapshot management | `SimpleTable` | **Core** — versioned table lifecycle |
| `staging_area.py` | Data staging before commit | `Staging`, `StageInfo` | **Supporting** — write-path staging |

---

## 5. Detailed Functional Capabilities

### 5.1 SQL Query Execution

- **Description**: Accepts a SQL query string, resolves referenced tables against the Redis catalog, enforces RBAC, estimates data volume, selects an engine, executes, and returns results as a DataFrame.
- **Business purpose**: The primary read path — every analytics query flows through this.
- **Trigger/input**: `DataReader(super_name, organization, query).execute(role_name, engine, with_scan)`
- **Processing behavior**:
  1. Parse SQL (dialect-aware via `SQLParser`)
  2. Extract table tuples and physical tables
  3. Call `restrict_read_access()` for RBAC column/row filtering
  4. Instantiate `Executor` with storage reference
  5. Initialize `QueryPlanManager` for profiling
  6. Run `DataEstimator.estimate()` to build a `Reflection` (file list, sizes, storage type)
  7. Wire RBAC, dedup, and tombstone view definitions onto `Reflection`
  8. Call `Executor.execute()` with the enriched `Reflection`
  9. Post-process: capture timings, extend execution plan, log metrics
- **Output/result**: `(pd.DataFrame, Status, Optional[str])` — result data, OK/ERROR status, optional error message
- **Dependencies**: `SQLParser`, `restrict_read_access`, `DataEstimator`, `Executor`, `RedisCatalog`, `QueryPlanManager`, `StorageInterface`
- **Constraints**: Engine dialect must match parser dialect; tables must exist in catalog
- **Risks**: If Redis is unreachable, dedup/tombstone enrichment is skipped (logged as warning, not fatal). If no Parquet files are found, returns empty DataFrame.
- **Confidence level**: Explicit

### 5.2 MCP Query Interface (`query_sql`)

- **Description**: Top-level function shaped for MCP server consumption. Adds a safety LIMIT, runs `DataReader.execute()`, and serializes results to `(columns, rows, columns_meta)`.
- **Business purpose**: The integration point between the Supertable engine and MCP-based AI agents / BI consumers.
- **Trigger/input**: `query_sql(organization, super_name, sql, limit, engine, role_name)`
- **Processing behavior**:
  1. `_ensure_sql_limit()` appends `LIMIT N` if the query lacks one
  2. Constructs and executes `DataReader`
  3. Sanitizes pandas NA/NaT/NaN to Python `None` for JSON safety
  4. Builds column metadata with name, dtype, nullable
- **Output/result**: `(List[str], List[List[Any]], List[Dict])` — column names, row data, column metadata
- **Constraints**: LIMIT is always enforced — unbounded queries are not allowed through MCP
- **Risks**: `_ensure_sql_limit` uses regex on trailing SQL; CTEs or subqueries with internal LIMIT are handled correctly (only appends if outermost query lacks LIMIT)
- **Confidence level**: Explicit

### 5.3 RBAC Enforcement

- **Description**: Before execution, `restrict_read_access()` is called to produce per-alias `RbacViewDef` objects that specify which columns are visible and what WHERE-clause row filters apply.
- **Business purpose**: Enterprise access control — ensures users see only the data their role permits.
- **Trigger/input**: Role name, table list, physical tables
- **Processing behavior**: Returns `Dict[str, RbacViewDef]` wired onto `Reflection.rbac_views`; executor creates filtered SQL views.
- **Dependencies**: `supertable.rbac.access_control`
- **Confidence level**: Explicit (import and call visible; implementation in external module)

### 5.4 Deduplication-on-Read

- **Description**: For tables configured with `dedup_on_read=True` and a `primary_keys` list in Redis table config, the system creates a `DedupViewDef` that the executor materializes as a `ROW_NUMBER()` window partitioned by PKs, ordered by `__timestamp__` DESC, keeping only `rn=1`.
- **Business purpose**: CDC / upsert semantics without physical compaction — enables streaming ingestion where the latest record per key is the truth.
- **Trigger/input**: Presence of `dedup_on_read` + `primary_keys` in `RedisCatalog.get_table_config()`
- **Processing behavior**: Lookup per-table config in Redis; if dedup enabled, create `DedupViewDef` with PKs, order column, and visible columns from query.
- **Dependencies**: `RedisCatalog.get_table_config()`
- **Constraints**: Only works if `primary_keys` is non-empty; `__timestamp__` must exist in data
- **Confidence level**: Explicit

### 5.5 Tombstone Filtering

- **Description**: Reads `tombstones` block from the snapshot payload stored in the Redis leaf. If present, creates a `TombstoneDef` that the executor uses to exclude soft-deleted key combinations.
- **Business purpose**: Non-destructive deletes — data is never physically removed but filtered at read time.
- **Trigger/input**: Presence of `tombstones` block with `deleted_keys` and `primary_keys` in the leaf's `payload`
- **Processing behavior**: `RedisCatalog.get_leaf()` → extract `payload.tombstones` → build `TombstoneDef`
- **Risks**: If leaf lookup fails, the error is logged at DEBUG and execution continues without tombstone filtering (graceful degradation)
- **Confidence level**: Explicit

### 5.6 Query Profiling and Monitoring

- **Description**: DuckDB writes a JSON profile to a temp file. After execution, `extend_execution_plan()` reads this file, enriches it with app timings and stats, logs a flat metric to `MonitoringWriter`, and deletes the temp file.
- **Business purpose**: Operational observability — enables query performance analysis, SLA tracking, and debugging.
- **Trigger/input**: Automatically invoked after every query execution
- **Processing behavior**: Read local JSON → build extended plan → flatten to metric payload → `MonitoringWriter.log_metric()` → delete temp file
- **Constraints**: Never raises (all exceptions caught) — monitoring must not break reads
- **Dependencies**: `MonitoringWriter`, `PlanStats`, local filesystem
- **Confidence level**: Explicit

### 5.7 Snapshot Version Chain (SimpleTable)

- **Description**: Each `SimpleTable.update()` creates a new snapshot JSON file on storage that references the previous snapshot via `previous_snapshot`, increments `snapshot_version`, and records the current schema and resources (Parquet file list).
- **Business purpose**: Immutable version history — enables time-travel, audit, and rollback capabilities.
- **Trigger/input**: Write operations that add/remove Parquet files
- **Processing behavior**: Read current snapshot → merge resources → increment version → write new snapshot → update Redis leaf pointer via CAS
- **Dependencies**: `StorageInterface`, `RedisCatalog.set_leaf_payload_cas()`
- **Confidence level**: Explicit

### 5.8 Data Staging

- **Description**: `Staging` provides a pre-commit area where Parquet files are written before being promoted to a `SimpleTable`. Supports manager mode (list/inspect stages) and stage mode (write/list/delete files).
- **Business purpose**: Transactional data ingestion — stage data, validate, then commit; prevents partial writes from corrupting tables.
- **Trigger/input**: API calls to create staging, save Parquet, list files, delete stage
- **Processing behavior**: Distributed lock → write Parquet → update file index → register in Redis
- **Dependencies**: `StorageInterface`, `RedisCatalog`, `check_write_access`, `check_meta_access`
- **Confidence level**: Explicit

### 5.9 Redis High Availability (Sentinel)

- **Description**: `RedisConnector` supports Sentinel-based HA with configurable strict/fallback mode. If Sentinel is unreachable and `SUPERTABLE_REDIS_SENTINEL_STRICT=false`, it falls back to direct Redis.
- **Business purpose**: Production resilience — prevents Redis single-point-of-failure from taking down the query path.
- **Trigger/input**: Environment variables (`SUPERTABLE_REDIS_SENTINEL`, `SUPERTABLE_REDIS_SENTINELS`, etc.)
- **Confidence level**: Explicit

---

## 6. Classes, Functions, and Methods

### 6.1 `data_reader.py`

| Name | Type | Purpose | Parameters | Returns | Side Effects | Importance |
|---|---|---|---|---|---|---|
| `DataReader` | Class | Query orchestration facade | `super_name`, `organization`, `query` | — | Initializes storage, timers | **Critical** |
| `DataReader.execute()` | Method | Run the full query pipeline | `role_name`, `with_scan`, `engine` | `(DataFrame, Status, str)` | Writes profiling files, logs metrics | **Critical** |
| `query_sql()` | Function | MCP-facing query entry point | `organization`, `super_name`, `sql`, `limit`, `engine`, `role_name` | `(columns, rows, columns_meta)` | Raises `RuntimeError` on failure | **Critical** |
| `_ensure_sql_limit()` | Function | Append LIMIT if missing | `sql`, `default_limit` | `str` (modified SQL) | None | **Important** |
| `Status` | Enum | OK / ERROR status | — | — | — | Supporting |

**`DataReader.execute()` — detailed internal logic:**

1. Creates `Timer` and `PlanStats` instances
2. Builds `SQLParser` with engine-specific dialect
3. Extracts table tuples (alias, super_name, simple_name, columns) and physical tables
4. Calls `restrict_read_access()` → `Dict[str, RbacViewDef]`
5. Creates `Executor` with storage reference
6. Creates `QueryPlanManager` → generates `query_id` (UUID) and `query_hash` (deterministic 16-char hash)
7. Runs `DataEstimator.estimate()` → `Reflection` object with supers, file lists, byte counts, storage type, freshness
8. Wires `rbac_views` onto reflection
9. Iterates over tables to look up dedup config and tombstone metadata from Redis
10. Guards: if `reflection.supers` is empty, returns empty DataFrame with error
11. Calls `Executor.execute()` → `(DataFrame, engine_used)`
12. Calls `extend_execution_plan()` for monitoring (never-fail)
13. Returns `(DataFrame, Status, message)`

### 6.2 `data_classes.py`

| Name | Type | Purpose | Key Fields | Importance |
|---|---|---|---|---|
| `TableDefinition` | Dataclass | Identifies a table in a query | `super_name`, `simple_name`, `alias`, `columns` | **Critical** |
| `SuperSnapshot` | Dataclass | Snapshot metadata for estimation | `super_name`, `simple_name`, `simple_version`, `files`, `columns` | **Critical** |
| `RbacViewDef` | Dataclass | RBAC column/row filter spec | `allowed_columns` (default `["*"]`), `where_clause` (default `""`) | **Critical** |
| `DedupViewDef` | Dataclass | Dedup-on-read configuration | `primary_keys`, `order_column` (default `__timestamp__`), `visible_columns` | **Important** |
| `TombstoneDef` | Dataclass | Soft-delete filter spec | `primary_keys`, `deleted_keys` (list of key tuples) | **Important** |
| `Reflection` | Dataclass | Aggregated estimation result | `storage_type`, `reflection_bytes`, `total_reflections`, `supers`, `freshness_ms`, `rbac_views`, `dedup_views`, `tombstone_views` | **Critical** |

### 6.3 `redis_catalog.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `RedisCatalog` | Class | Full metadata catalog over Redis | **Critical** |
| `RedisCatalog.__init__()` | Method | Connects to Redis, registers Lua scripts | **Critical** |
| `ensure_root()` | Method | Bootstrap meta:root for a SuperTable | **Important** |
| `get_leaf()` | Method | Read leaf pointer (version, path, payload) | **Critical** |
| `set_leaf_payload_cas()` | Method | Atomic leaf update with snapshot payload | **Critical** |
| `set_leaf_path_cas()` | Method | Atomic leaf update (path only) | **Important** |
| `acquire_simple_lock()` / `release_simple_lock()` | Methods | Distributed lock for table writes | **Critical** |
| `get_table_config()` | Method | Read per-table config (dedup, PKs) | **Important** |
| `delete_simple_table()` | Method | Remove table metadata from Redis | **Important** |
| `delete_super_table()` | Method | Remove all Redis keys for a SuperTable | **Important** |
| `list_stagings()` | Method | List staging area names | Supporting |
| Lua scripts (`_LUA_LEAF_CAS_SET`, `_LUA_LEAF_PAYLOAD_CAS_SET`, `_LUA_ROOT_BUMP`) | Scripts | Atomic version-bumping CAS operations | **Critical** |

### 6.4 `redis_connector.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `RedisOptions` | Frozen Dataclass | Parse Redis config from env vars | **Important** |
| `create_redis_client()` | Function | Build Redis client (direct or Sentinel) | **Critical** |
| `RedisConnector` | Class | Thin wrapper holding `redis.Redis` | Supporting |

### 6.5 `query_plan_manager.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `QueryPlanManager` | Class | Manage DuckDB profile temp files | **Important** |
| `profile_pragmas()` | Method | Generate DuckDB PRAGMA statements | Supporting |
| `_cleanup_old_plans()` | Method | Housekeeping: delete old plan files | Supporting |

### 6.6 `plan_extender.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `extend_execution_plan()` | Function | Enrich plan with timings, log metric, cleanup | **Important** |
| `_safe_json()` | Function | Never-fail JSON serializer | Supporting |
| `_read_local_json()` | Function | Read DuckDB profile from local disk | Supporting |

### 6.7 `super_table.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `SuperTable` | Class | Root namespace object | **Critical** |
| `init_super_table()` | Method | Create storage dir + Redis root | **Important** |
| `read_simple_table_snapshot()` | Method | Read heavy snapshot JSON from storage | **Important** |
| `delete()` | Method | Destructive: remove storage + Redis meta | **Important** |

### 6.8 `simple_table.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `SimpleTable` | Class | Table lifecycle (create/update/delete) | **Critical** |
| `init_simple_table()` | Method | Bootstrap dirs, initial snapshot, Redis leaf | **Important** |
| `get_simple_table_snapshot()` | Method | Read current snapshot (prefer Redis payload) | **Critical** |
| `update()` | Method | Build new snapshot with merged resources | **Critical** |
| `delete()` | Method | Remove storage + Redis meta (RBAC-checked) | **Important** |

### 6.9 `staging_area.py`

| Name | Type | Purpose | Importance |
|---|---|---|---|
| `Staging` | Class | Data staging (manager + stage mode) | **Important** |
| `save_as_parquet()` | Method | Write Parquet to staging, update index | **Important** |
| `get_directory_structure()` | Method | List all stages with metadata | Supporting |
| `delete()` | Method | Remove stage (storage + Redis) | Supporting |

---

## 7. End-to-End Workflows

### 7.1 Query Execution (Read Path)

1. **Trigger**: External caller invokes `query_sql(org, super_name, sql, limit, engine, role_name)`
2. `_ensure_sql_limit()` appends `LIMIT` if absent
3. `DataReader` is constructed with `super_name`, `organization`, `query`
4. `DataReader.execute()` begins:
   - 4a. `SQLParser` parses SQL with engine dialect
   - 4b. `restrict_read_access()` checks RBAC → returns `Dict[str, RbacViewDef]`
   - 4c. `Executor` is instantiated with `StorageInterface`
   - 4d. `QueryPlanManager` is created → generates `query_id`, `query_hash`, temp directory, plan file path
   - 4e. `DataEstimator.estimate()` scans metadata → returns `Reflection` (file list, sizes, storage type, freshness)
   - 4f. **Decision point**: If `reflection.supers` is empty → return empty DataFrame with error
   - 4g. Dedup enrichment: for each table, check `RedisCatalog.get_table_config()` for `dedup_on_read` → attach `DedupViewDef`
   - 4h. Tombstone enrichment: for each table, check `RedisCatalog.get_leaf()` for `payload.tombstones` → attach `TombstoneDef`
   - 4i. `Executor.execute()` runs the query (DuckDB or Spark) → returns `(DataFrame, engine_used)`
5. `extend_execution_plan()` reads DuckDB profile, enriches with timings, logs to `MonitoringWriter`, deletes temp file
6. `query_sql()` sanitizes NA values → converts DataFrame to `(columns, rows, columns_meta)`
7. **Success outcome**: Tuple of column names, row data, column metadata
8. **Failure modes**: `RuntimeError` raised on execution failure; dedup/tombstone lookup failures are non-fatal (graceful degradation)

### 7.2 Table Creation / Bootstrap

1. **Trigger**: `SuperTable(super_name, organization)` is constructed
2. Check `RedisCatalog.root_exists()` → if yes, skip init (fast path)
3. If no: create storage directory, call `catalog.ensure_root()` to set `meta:root` in Redis
4. Initialize RBAC scaffolding (`RoleManager`, `UserManager`)
5. `SimpleTable(super_table, simple_name)` is constructed
6. Check `catalog.leaf_exists()` → if yes, skip init (fast path)
7. If no: create data/snapshot dirs, write initial empty snapshot JSON, set Redis leaf via CAS

### 7.3 Table Update (Write Path Snapshot)

1. **Trigger**: `SimpleTable.update(new_resources, sunset_files, model_df)`
2. Read current snapshot (prefer Redis payload via `get_simple_table_snapshot()`)
3. Merge resources: remove sunset files, append new resources
4. Increment `snapshot_version`, update `last_updated_ms`, derive schema
5. Write new snapshot JSON to storage
6. Return `(snapshot_dict, snapshot_path)` — caller is responsible for updating Redis leaf via CAS

### 7.4 Staging Workflow

1. **Trigger**: `Staging(organization, super_name, staging_name)` is constructed
2. Validate SuperTable exists via `catalog.root_exists()`
3. Acquire Redis lock → create stage directory → register in Redis → ensure file index
4. `save_as_parquet()`: acquire lock → write Parquet file → append to index JSON → release lock
5. `delete()`: acquire lock → delete storage recursively → delete Redis meta → release lock

---

## 8. Data Model and Information Flow

### Core Entities

```
Organization
  └── SuperTable  (meta:root in Redis)
        ├── SimpleTable[]  (meta:leaf:{name} in Redis)
        │     ├── Snapshot (JSON on storage, optionally cached in Redis leaf payload)
        │     │     ├── resources: [{file, rows, ...}]
        │     │     ├── schema: [{name, type, nullable, metadata}]
        │     │     ├── snapshot_version: int
        │     │     ├── previous_snapshot: path
        │     │     └── tombstones: {primary_keys, deleted_keys}
        │     └── Data Files (Parquet on object storage)
        ├── Staging[]
        │     ├── Parquet files
        │     └── _files.json index
        └── RBAC (roles, users, tokens in Redis)
```

### Key DTOs and Their Flow

```
SQLParser → TableDefinition[] ──────────────────────┐
                                                     │
restrict_read_access() → Dict[alias, RbacViewDef] ──┤
                                                     │
DataEstimator.estimate() → Reflection ◄──────────────┤
  ├── supers: SuperSnapshot[]                        │
  ├── rbac_views: Dict[alias, RbacViewDef]  ◄────────┘
  ├── dedup_views: Dict[alias, DedupViewDef]  ◄── RedisCatalog.get_table_config()
  └── tombstone_views: Dict[alias, TombstoneDef] ◄── RedisCatalog.get_leaf().payload.tombstones
                    │
                    ▼
            Executor.execute() → (DataFrame, engine_used)
```

### Redis Key Schema

| Key Pattern | Type | Content |
|---|---|---|
| `supertable:{org}:{sup}:meta:root` | STRING | `{version, ts}` |
| `supertable:{org}:{sup}:meta:leaf:{simple}` | STRING | `{version, ts, path, payload?}` |
| `supertable:{org}:{sup}:lock:leaf:{simple}` | STRING | Lock token (NX EX) |
| `supertable:{org}:{sup}:meta:table_config:{simple}` | STRING | `{dedup_on_read, primary_keys, ...}` |
| `supertable:{org}:{sup}:meta:staging:{name}` | STRING | Stage metadata |
| `supertable:{org}:{sup}:meta:staging:meta` | SET | Set of staging names |
| `supertable:{org}:{sup}:rbac:*` | Various | RBAC users, roles, docs |
| `supertable:{org}:auth:tokens` | HASH | API token hashes → metadata |
| `supertable:reflection:compute:{org}__{sup}` | STRING | Compute pool config |

### Storage Layout

```
{org}/{super}/super/                     # SuperTable root directory
{org}/{super}/tables/{simple}/data/      # Parquet data files
{org}/{super}/tables/{simple}/snapshots/ # Snapshot JSON files
{org}/{super}/staging/{stage}/           # Staging Parquet files
{org}/{super}/staging/{stage}_files.json # Staging file index
```

---

## 9. Dependencies and Integrations

### External Dependencies

| Dependency | Purpose | Criticality |
|---|---|---|
| `redis` (redis-py) | Metadata store, locking, RBAC | **Critical** — system non-functional without it |
| `redis.sentinel` | Redis HA via Sentinel | **Important** — production resilience |
| `pandas` | DataFrame result container | **Critical** |
| `pyarrow` | Parquet I/O in staging | **Important** |
| `polars` | Schema type mapping (optional, best-effort) | Supporting |
| `python-dotenv` | Environment variable loading | Supporting |

### Internal Dependencies

| Module | Purpose |
|---|---|
| `supertable.storage.storage_factory` / `StorageInterface` | Abstracted object storage (MinIO, S3, local) |
| `supertable.utils.sql_parser.SQLParser` | SQL parsing, table extraction, dialect support |
| `supertable.engine.data_estimator.DataEstimator` | Data volume estimation |
| `supertable.engine.executor.Executor` | Multi-engine query execution |
| `supertable.engine.engine_enum.Engine` | Engine enum (AUTO, DUCKDB, SPARK) |
| `supertable.engine.plan_stats.PlanStats` | Query statistics collection |
| `supertable.rbac.access_control` | RBAC enforcement (`restrict_read_access`, `check_write_access`, `check_meta_access`) |
| `supertable.rbac.role_manager.RoleManager` | RBAC role scaffolding |
| `supertable.rbac.user_manager.UserManager` | RBAC user scaffolding |
| `supertable.monitoring_writer.MonitoringWriter` | Monitoring metric logging |
| `supertable.locking.redis_lock.RedisLocking` | Distributed lock primitives |
| `supertable.utils.helper` | `generate_hash_uid`, `collect_schema`, `generate_filename` |
| `supertable.config.defaults` | Logger |
| `supertable.config.homedir` | App home directory resolution |

### Integration Points

| Integration | Direction | Protocol |
|---|---|---|
| MCP Server | Upstream (caller) | Function call (`query_sql`) |
| Redis / Sentinel | Bidirectional | Redis protocol |
| Object Storage (MinIO/S3/Local) | Bidirectional | `StorageInterface` abstraction |
| DuckDB | Downstream (engine) | In-process (PRAGMA profile) |
| Spark | Downstream (engine) | Likely Thrift/JDBC (inferred from `_spark_thrifts_key`) |
| MonitoringWriter | Downstream | Internal buffered writer |

---

## 10. Architecture Positioning

### System Context

```
┌─────────────────────────────────────────────────┐
│  MCP Server / API Layer                          │
│  (calls query_sql() or DataReader.execute())     │
└────────────────┬────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────┐
│  DataReader (Facade / Orchestrator)               │
│  ├── SQLParser                                    │
│  ├── RBAC: restrict_read_access()                 │
│  ├── DataEstimator                                │
│  ├── RedisCatalog (metadata, config, tombstones)  │
│  └── Executor (DuckDB / Spark)                    │
└────────┬────────────────┬───────────────────────┘
         │                │
┌────────▼──────┐  ┌──────▼───────┐
│ Redis Cluster │  │ Object Store │
│ (Sentinel HA) │  │ (MinIO/S3)   │
└───────────────┘  └──────────────┘
```

### Boundaries

- **DataReader** is the boundary between the API surface and the engine internals. It does not know about HTTP, gRPC, or MCP protocol details.
- **Reflection** is the contract between estimation and execution — it carries everything the executor needs.
- **StorageInterface** is the abstraction boundary for storage backends.
- **RedisCatalog** is the abstraction boundary for all metadata operations.

### Coupling Considerations

- `DataReader` has moderate coupling to `RedisCatalog` (direct calls for dedup/tombstone lookup inside `execute()`). This could be extracted to a "reflection enricher" for cleaner separation.
- `SimpleTable` and `SuperTable` depend on both `StorageInterface` and `RedisCatalog`, creating a dual-dependency pattern.
- The monitoring path (`extend_execution_plan`) is fully decoupled — all exceptions are swallowed.

---

## 11. Business Value Assessment

| Dimension | Assessment |
|---|---|
| **Business problem** | Enables SQL analytics over versioned, access-controlled data stored on cheap object storage — the core value proposition of the Supertable platform. |
| **Revenue relationship** | Directly revenue-enabling. Every query that a customer or AI agent runs flows through this code. |
| **Differentiation** | The combination of dedup-on-read, tombstone filtering, multi-engine selection, and RBAC enforcement at the query layer — applied transparently over Parquet on object storage — is a differentiating capability vs. raw data lakes. |
| **What would be lost** | Without this code, the product cannot answer any query. It is the core read path. |
| **Category** | Foundational + governance + analytics. Not a commodity — the view-layering architecture (RBAC, dedup, tombstones as composable views) is a deliberate design choice that avoids physical data transformation. |
| **Strategic significance** | The `query_sql()` function is specifically shaped for MCP consumption, positioning the product as an AI-agent-native data platform. The Sentinel HA support and monitoring pipeline indicate production-grade operational maturity. |

---

## 12. Technical Depth Assessment

| Dimension | Assessment |
|---|---|
| **Complexity** | High — multi-step pipeline with RBAC, dedup, tombstone, multi-engine, and monitoring concerns composed in a single orchestration method. |
| **Architectural maturity** | Mature. Clean separation between metadata (Redis), data (storage), and execution (engine). CAS-based versioning with Lua scripts. Snapshot chain for immutability. |
| **Maintainability** | Moderate. `DataReader.execute()` is ~90 lines of linear pipeline code with multiple try/except blocks. The dedup/tombstone enrichment loop could be extracted. |
| **Extensibility** | Good. The `Reflection` dataclass is extensible (new view types can be added). The engine picker (`Engine.AUTO`) allows new engines. |
| **Operational sensitivity** | High. Redis unavailability degrades functionality (dedup/tombstones silently skipped). Object storage unavailability is fatal. |
| **Performance** | `_ensure_sql_limit()` prevents unbounded scans. `DataEstimator` pre-checks data volume. Engine auto-picker uses freshness heuristics. Redis leaf payload caching avoids storage reads for metadata. |
| **Reliability** | Defensive. Monitoring never raises. Dedup/tombstone lookups are wrapped in try/except. Lock release uses Lua CAS. Sentinel fallback prevents single-point-of-failure. |
| **Security** | RBAC is enforced before execution. Write operations check `check_write_access()`. Meta operations check `check_meta_access()`. API tokens are stored as SHA-256 hashes. |
| **Testing clues** | No test code visible. `pragma: no cover` comments on import fallbacks suggest coverage tooling is in use. |

---

## 13. Assumptions, Unknowns, and Inferences

### Explicitly Proven by Code

- `DataReader` is the main query orchestration facade.
- `query_sql()` is shaped for MCP server integration.
- RBAC is enforced via `restrict_read_access()` producing `RbacViewDef` per alias.
- Dedup-on-read uses `ROW_NUMBER()` over `__timestamp__` (column name is hardcoded).
- Tombstones are read from Redis leaf payload, not from storage.
- Redis leaf stores version, timestamp, path, and optionally the full snapshot payload.
- CAS updates use atomic Lua scripts.
- Sentinel HA with non-strict fallback is supported.
- `QueryPlanManager` manages DuckDB profile JSON files on local disk.
- `extend_execution_plan()` writes metrics to `MonitoringWriter` and never raises.
- `SimpleTable.update()` creates new snapshot files and maintains a `previous_snapshot` chain.
- `Staging` uses distributed locks (Redis SET NX EX with Lua release).
- NA/NaT/NaN sanitization is performed before returning results through MCP.

### Reasonable Inferences

- **Inference**: The `Engine.AUTO` picker likely uses `Reflection.freshness_ms` and `reflection_bytes` to decide between DuckDB (small/stable data) and Spark (large/fresh data). *Basis*: `freshness_ms` docstring explicitly mentions "engine auto-picker."
- **Inference**: `Executor.execute()` creates SQL views from `RbacViewDef`, `DedupViewDef`, and `TombstoneDef` before running the user query. *Basis*: these are described as "consumed by executors to create filtered views."
- **Inference**: The system supports Delta Lake mirroring. *Basis*: `SimpleTable.update()` generates `schemaString` in Spark StructType JSON format; `_mirrors_key` exists in Redis catalog.
- **Inference**: Spark execution uses Thrift server or "plug" connectors. *Basis*: `_spark_thrifts_key`, `_spark_plugs_key`, `_compute_pools_key` in Redis catalog.
- **Inference**: The product is a multi-tenant SaaS platform. *Basis*: organization-scoped keys, RBAC, API tokens.

### Unknown / Not Visible in Provided Snippet

- Implementation of `Executor` — how views are materialized, how engines are selected.
- Implementation of `DataEstimator` — how file lists and byte counts are derived.
- Implementation of `restrict_read_access()` — how RBAC rules are resolved from roles.
- Implementation of `SQLParser` — how dialect-aware parsing works.
- Implementation of `MonitoringWriter` — how metrics are buffered and flushed.
- Implementation of `RedisLocking` — retry/backoff strategy.
- How `StorageInterface` abstracts MinIO vs S3 vs local.
- The API/MCP layer that calls `query_sql()`.
- How the write path actually promotes staging data to `SimpleTable` (the commit step).
- Compute pool management and Spark cluster lifecycle.
- The full RBAC policy model (how roles define column/row restrictions).

---

## 14. AI Consumption Notes

- **Canonical feature name**: `Supertable Data Reader` / `Query Execution Pipeline`
- **Alternative names**: `DataReader`, `query_sql`, `read path`, `SQL execution facade`
- **Main responsibilities**: SQL query orchestration, RBAC enforcement, dedup-on-read, tombstone filtering, multi-engine execution, query monitoring
- **Important entities**: `DataReader`, `Reflection`, `RbacViewDef`, `DedupViewDef`, `TombstoneDef`, `TableDefinition`, `SuperSnapshot`, `SuperTable`, `SimpleTable`, `RedisCatalog`, `Staging`
- **Important workflows**: Query execution pipeline (parse → RBAC → estimate → enrich → execute → monitor), table bootstrap, snapshot update, staging lifecycle
- **Integration points**: MCP server (upstream), Redis/Sentinel (metadata), Object storage (data), DuckDB/Spark (engines), MonitoringWriter (observability)
- **Business purpose keywords**: SQL analytics, multi-tenant, RBAC, dedup-on-read, tombstone filtering, versioned snapshots, lakehouse, MCP, AI-agent analytics
- **Architecture keywords**: facade, pipeline, CAS versioning, distributed locking, view layering, Sentinel HA, metadata-data split, snapshot chain
- **Follow-up files for completeness**:
  - `supertable/engine/executor.py` — engine selection and view materialization
  - `supertable/engine/data_estimator.py` — file/byte estimation logic
  - `supertable/rbac/access_control.py` — RBAC policy resolution
  - `supertable/utils/sql_parser.py` — SQL parsing and table extraction
  - `supertable/monitoring_writer.py` — metric buffering and flush
  - `supertable/engine/engine_enum.py` — engine enum and dialect mapping
  - `supertable/locking/redis_lock.py` — lock primitives
  - `supertable/storage/storage_interface.py` — storage abstraction
  - API/MCP layer that invokes `query_sql()`

---

## 15. Suggested Documentation Tags

`sql-analytics`, `query-execution`, `read-path`, `multi-tenant`, `rbac`, `access-control`, `dedup-on-read`, `tombstone-filtering`, `snapshot-versioning`, `redis-catalog`, `redis-sentinel`, `distributed-locking`, `duckdb`, `spark`, `multi-engine`, `parquet`, `object-storage`, `mcp-integration`, `query-monitoring`, `profiling`, `facade-pattern`, `pipeline-pattern`, `cas-versioning`, `data-lakehouse`, `staging`, `schema-evolution`, `view-layering`, `ai-agent-analytics`, `data-governance`

---

## Merge Readiness

- **Suggested canonical name**: `Supertable Query Execution Pipeline`
- **Standalone or partial**: Partial — this covers the orchestration layer and metadata management. The execution engine internals (`Executor`, `DataEstimator`), RBAC policy model (`access_control`), SQL parser, and API/MCP layer are not included.
- **Related components to merge**:
  - `Executor` / `DataEstimator` documentation (engine internals)
  - RBAC subsystem documentation (policy model, role/user management)
  - Storage subsystem documentation (StorageInterface implementations)
  - API/MCP layer documentation (HTTP/protocol surface)
  - Monitoring subsystem documentation (MonitoringWriter, metric schema)
  - Write path documentation (data ingestion, staging → commit)
- **Files that would most improve certainty**: `executor.py`, `data_estimator.py`, `access_control.py`, `sql_parser.py`, `engine_enum.py`
- **Recommended merge key phrases**: `DataReader`, `query_sql`, `Reflection`, `query execution pipeline`, `read path`, `RBAC enforcement`, `dedup-on-read`, `tombstone filtering`, `RedisCatalog`
