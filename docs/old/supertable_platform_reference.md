# Supertable Platform — Complete Architecture & Codebase Reference

> **Purpose:** Single-document reference for any AI system working with the Supertable codebase.
> Covers product identity, architecture, every subsystem, data flows, dependency tree,
> configuration, security model, and operational patterns.

---

## 1. Product Identity

**Supertable** is a multi-tenant, versioned, columnar data platform built on Parquet files with Redis-backed metadata coordination. It functions as a managed analytical layer — a "virtual data lakehouse" — that enables SQL queries over versioned datasets stored on cheap object storage (S3, MinIO, Azure Blob, GCP Storage, or local filesystem).

The platform is designed for AI-agent-native analytics. Its MCP (Model Context Protocol) server allows LLM agents to autonomously discover schemas, execute SQL, store annotations, and build dashboards — all under strict RBAC controls.

**Core value proposition:** SQL analytics over versioned, access-controlled data stored on object storage — with dedup-on-read, soft-delete (tombstone) semantics, multi-engine execution (DuckDB + Spark), and per-column/per-row RBAC enforcement — without requiring physical data transformation or ETL.

---

## 2. System Architecture

### 2.1 High-Level Architecture

```
┌───────────────────────────────────────────────────────────────────────────┐
│                           ENTRYPOINTS                                     │
│                                                                           │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐  ┌───────┐  ┌────────────┐  │
│  │ API      │  │ MCP      │  │ Reflection │  │ OData │  │ Python SDK │  │
│  │ Server   │  │ Server   │  │ UI Server  │  │       │  │ (direct)   │  │
│  │ :8050    │  │ :8000    │  │ :8051      │  │       │  │            │  │
│  └────┬─────┘  └────┬─────┘  └──────┬─────┘  └───┬───┘  └─────┬──────┘  │
│       │              │               │            │            │          │
│       └──────────────┴───────┬───────┴────────────┴────────────┘          │
│                              │                                            │
│                    ┌─────────▼──────────┐                                 │
│                    │  config/settings   │  ← .env loaded ONCE here        │
│                    │  (frozen dataclass │                                 │
│                    │   123 attributes)  │                                 │
│                    └─────────┬──────────┘                                 │
└──────────────────────────────┼────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────┐
│                        DOMAIN / BUSINESS LOGIC                            │
│                                                                           │
│  ┌─────────────┐  ┌─────────────┐  ┌────────────┐  ┌──────────────────┐  │
│  │ DataReader  │  │ DataWriter  │  │ MetaReader │  │ HistoryCleaner   │  │
│  │ (query)     │  │ (ingest)    │  │ (catalog)  │  │ (GC)             │  │
│  └──────┬──────┘  └──────┬──────┘  └─────┬──────┘  └────────┬─────────┘  │
│         │                │               │                   │            │
│  ┌──────▼──────────────────────────────────────────────────────────────┐  │
│  │                      Shared Infrastructure                          │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ ┌────────────┐ │  │
│  │  │SQLParser │ │  RBAC    │ │ Staging  │ │Locking │ │ Monitoring │ │  │
│  │  │          │ │          │ │ + Pipes  │ │        │ │            │ │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └────────┘ └────────────┘ │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────┬────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────┐
│                        ENGINE LAYER                                       │
│                                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                    │
│  │ DuckDB Lite  │  │ DuckDB Pro   │  │ Spark Thrift │                    │
│  │ (ephemeral)  │  │ (persistent  │  │ (remote)     │                    │
│  │              │  │  + cached)   │  │              │                    │
│  └──────────────┘  └──────────────┘  └──────────────┘                    │
│                                                                           │
│  ┌──────────────────────────────────────────────────────────────────────┐ │
│  │ engine_common.py — S3 config, view chain, SQL rewriting, httpfs     │ │
│  └──────────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────┬────────────────────────────────────────────┘
                               │
┌──────────────────────────────▼────────────────────────────────────────────┐
│                      INFRASTRUCTURE LAYER                                 │
│                                                                           │
│  ┌──────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐  │
│  │ RedisCatalog     │  │ StorageInterface│  │ RedisConnector          │  │
│  │ (metadata, CAS,  │  │ (S3, MinIO,    │  │ (Sentinel HA, SSL,     │  │
│  │  locks, RBAC,    │  │  Azure, GCP,   │  │  connection factory)   │  │
│  │  staging, pipes) │  │  Local)        │  │                        │  │
│  └────────┬─────────┘  └───────┬─────────┘  └───────────┬─────────────┘  │
│           │                    │                         │                │
│     ┌─────▼─────┐      ┌──────▼──────┐           ┌──────▼──────┐        │
│     │   Redis   │      │Object Store │           │   Redis     │        │
│     │ (Sentinel)│      │(MinIO/S3/…) │           │ (Sentinel)  │        │
│     └───────────┘      └─────────────┘           └─────────────┘        │
└───────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Entrypoints

| Entrypoint | File | Default Port | Role |
|---|---|---|---|
| **API Server** | `api/application.py` | 8050 | JSON API — all 70+ endpoints (RBAC, execute, ingestion, monitoring, tables, tokens) |
| **MCP Server** | `mcp/mcp_server.py` | 8000 | Model Context Protocol — 22 tools for AI agents (list_tables, query_sql, etc.) |
| **Reflection UI** | `reflection/application.py` | 8051 | Jinja2 HTML UI + reverse proxy to API server |
| **OData** | `odata/odata.py` | (mounted on API) | OData v4 endpoint for BI tool integration |
| **Python SDK** | `data_reader.py` / `data_writer.py` | N/A | Direct Python import — no HTTP involved |
| **Notebook WS** | `infrastructure/python_worker/ws_server.py` | 8010 | WebSocket server for interactive notebook execution |
| **MCP Web Gateway** | `mcp/web_app.py` + `web_server.py` | 8099 | FastAPI gateway proxying stdio MCP via HTTP for remote clients |

### 2.3 Configuration Bootstrap

All entrypoints share a single configuration source:

```python
from supertable.config.settings import settings  # singleton, frozen dataclass, 123 attributes
```

`config/settings.py` is the **single source of truth** for every environment variable. It:
1. Calls `load_dotenv()` exactly once at module level
2. Constructs a frozen `Settings` dataclass from `os.getenv()` calls
3. Resolves all fallback chains (e.g., `SUPERTABLE_SUPERUSER_TOKEN` → `SUPERTABLE_SUPERTOKEN`)
4. Is imported by every other module — no module calls `os.getenv()` directly for fixed env vars

---

## 3. Core Data Model

### 3.1 Entity Hierarchy

```
Organization (org)
  └── SuperTable (super_name)
       ├── SimpleTable (simple_name)  ← individual table
       │    ├── data/*.parquet        ← immutable data files
       │    └── snapshots/*.json      ← versioned snapshot chain
       ├── staging/{stage}/           ← pre-commit staging areas
       └── super/                     ← SuperTable metadata
```

### 3.2 Redis Key Schema

| Key Pattern | Type | Content |
|---|---|---|
| `supertable:{org}:{sup}:meta:root` | STRING | `{"version": N, "ts": epoch_ms}` |
| `supertable:{org}:{sup}:meta:leaf:{simple}` | STRING | `{"version": N, "ts": epoch_ms, "path": "...", "payload": {...}}` |
| `supertable:{org}:{sup}:meta:table_config:{simple}` | STRING | `{"primary_keys": [...], "dedup_on_read": bool, ...}` |
| `supertable:{org}:{sup}:lock:leaf:{simple}` | STRING | Lock token (SET NX EX) |
| `supertable:{org}:{sup}:rbac:*` | Various | RBAC roles, users, permissions |
| `supertable:{org}:auth:tokens` | HASH | API token hashes → metadata |
| `supertable:{org}:{sup}:meta:staging:*` | STRING/SET | Staging area metadata |
| `supertable:reflection:compute:{org}__{sup}` | STRING | Compute pool config |
| `monitor:{org}:{sup}:{type}` | LIST | Monitoring metric payloads |

### 3.3 Storage Layout

```
{organization}/
  {super_name}/
    super/                              ← SuperTable root metadata
    tables/
      {simple_name}/
        data/
          data_{uuid}.parquet           ← Data files (immutable, zstd compressed)
        snapshots/
          tables_{uuid}.json            ← Snapshot files (immutable chain)
    staging/
      {stage_name}/
        {uuid}.parquet                  ← Staged data awaiting pipe merge
      {stage_name}_files.json           ← Staging file index
```

### 3.4 Snapshot JSON Structure

```json
{
  "simple_name": "orders",
  "location": "org/super/tables/orders",
  "snapshot_version": 42,
  "last_updated_ms": 1711612345000,
  "previous_snapshot": "org/super/tables/orders/snapshots/prev.json",
  "schema": [{"name": "id", "type": "long", "nullable": true, "metadata": {}}],
  "resources": [
    {
      "file": "org/super/tables/orders/data/data_abc123.parquet",
      "file_size": 1048576,
      "rows": 50000,
      "columns": 8,
      "stats": {"id": {"min": 1, "max": 9999}}
    }
  ],
  "tombstones": {
    "primary_keys": ["id"],
    "deleted_keys": [[42], [99]],
    "total_tombstones": 2
  }
}
```

---

## 4. Subsystem Reference

### 4.1 Query Engine (Read Path)

**Files:** `data_reader.py`, `engine/executor.py`, `engine/duckdb_lite.py`, `engine/duckdb_pro.py`, `engine/spark_thrift.py`, `engine/engine_common.py`, `engine/data_estimator.py`

**Pipeline:**
```
SQL Parse → RBAC Check → Estimate → Dedup/Tombstone Enrichment → Execute → Telemetry → Return
```

**View Chain:** Security and data-integrity views are composed as SQL VIEWs:
```
parquet_scan → RBAC view → tombstone view → dedup view → user query
```

**Three Execution Engines:**

| Engine | Selection Criteria | Characteristics |
|---|---|---|
| **DuckDB Lite** | Small data or actively-written tables | Ephemeral VIEWs per request, dropped in `finally` |
| **DuckDB Pro** | Medium stable data | Persistent singleton, version-aware view cache with ref-counting |
| **Spark Thrift** | Large datasets (10+ GB) | Remote Spark via PyHive, sqlglot transpilation, watchdog timeout |

**Auto-Pick Algorithm:** `_auto_pick()` selects engine based on data size and freshness with env-configurable thresholds (`SUPERTABLE_ENGINE_LITE_MAX_BYTES`, `SUPERTABLE_ENGINE_SPARK_MIN_BYTES`, `SUPERTABLE_ENGINE_FRESHNESS_SEC`).

**Key entry points:**
- `query_sql(organization, super_name, sql, limit, engine, role_name)` — MCP-facing, returns `(columns, rows, columns_meta)`
- `DataReader(super_name, organization, query).execute(role_name)` — returns `(DataFrame, Status, message)`

### 4.2 Write Engine (Ingest Path)

**Files:** `data_writer.py`, `processing.py`, `simple_table.py`, `super_table.py`

**Pipeline:**
```
Arrow Table → Polars DataFrame → Overlap Detection → Anti-Join Merge
  → New Parquet (zstd) → New Snapshot JSON → Redis CAS Leaf Update
  → Root Pointer Bump → Monitoring → DQ Notification → Mirror
```

**Key patterns:**
- **LSM-style storage** — append-only snapshot chain, no in-place mutation
- **CAS (Compare-and-Swap) via Lua** — atomic leaf pointer updates
- **Distributed locking** — Redis `SET NX EX` with token-based release
- **Overlap detection** — column-level min/max zonemap statistics
- **Tombstone management** — soft-delete via composite key tracking in snapshots
- **Format mirroring** — optional write to Delta Lake, Iceberg, or Parquet mirror targets

### 4.3 Metadata Reflection (MetaReader)

**File:** `meta_reader.py`

Read-only metadata service enabling catalog discovery:
- `list_supers()` — all SuperTables in an org, RBAC-filtered
- `list_tables()` — tables within a SuperTable, RBAC-filtered
- `get_table_schema()` — column names/types for one or all tables
- `get_table_stats()` — file count, row count, size, timestamps
- `get_super_meta()` — composite view with burst caching (TTL-bounded, version-keyed)

**Architecture:** Redis-first with storage fallback. In-process burst cache for `get_super_meta()`.

### 4.4 RBAC (Role-Based Access Control)

**Files:** `rbac/permissions.py`, `rbac/role_manager.py`, `rbac/user_manager.py`, `rbac/access_control.py`, `rbac/filter_builder.py`, `rbac/row_column_security.py`

**Permission Matrix:**

| Role Type | CONTROL | CREATE | WRITE | READ | META |
|---|---|---|---|---|---|
| superadmin | ✓ | ✓ | ✓ | ✓ | ✓ |
| admin | ✓ | ✓ | ✓ | ✓ | ✓ |
| writer | ✗ | ✗ | ✓ | ✓ | ✓ |
| reader | ✗ | ✗ | ✗ | ✓ | ✓ |
| meta | ✗ | ✗ | ✗ | ✗ | ✓ |

**Enforcement levels:**
- **Table-level:** which tables a role can access
- **Column-level:** which columns within a table are visible
- **Row-level:** SQL WHERE predicates defined via JSON filter specs, translated by `FilterBuilder`

**Key flow:** `restrict_read_access()` produces `Dict[alias → RbacViewDef]` consumed by the query engine to create filtered SQL VIEWs.

**Auto-bootstrap:** `superadmin` role + `superuser` account created automatically on first access with distributed locking.

### 4.5 MCP Server (AI Agent Interface)

**Files:** `mcp/mcp_server.py`, `mcp/web_app.py`, `mcp/web_client.py`, `mcp/mcp_client.py`, `mcp/web_server.py`

**22 tools** exposed via JSON-RPC 2.0 for LLM agents:

| Category | Tools |
|---|---|
| Discovery | `list_tables`, `describe_table`, `list_supers` |
| Query | `query_sql` (with LIMIT guard, engine selection) |
| Write | `insert_data`, `create_table`, `delete_table` |
| RBAC | `list_roles`, `create_role`, `list_users`, `create_user` |
| Feedback | `submit_feedback`, `list_feedback` |
| Annotations | `store_annotation`, `list_annotations` |
| App State | `store_app_state`, `get_app_state`, `list_app_state` |
| Profiling | `profile_table` |

**Transports:** stdio (Claude Desktop), Streamable HTTP (remote clients), web gateway (browser testing)

**Design patterns:**
- Concurrency limiter (AnyIO CapacityLimiter, default 6)
- Thread offloading for blocking I/O
- Consistent error envelopes (`status: "OK"/"ERROR"`)
- Eager context loading via `list_tables` system_hint
- HMAC token authentication

### 4.6 Storage Layer

**Files:** `storage/storage_interface.py`, `storage/storage_factory.py`, `storage/local_storage.py`, `storage/s3_storage.py`, `storage/minio_storage.py`, `storage/azure_storage.py`, `storage/gcp_storage.py`

**Pattern:** Strategy (ABC) + Factory with lazy imports.

| Backend | SDK | Key Features |
|---|---|---|
| LOCAL | stdlib | Atomic writes via `tempfile` + `os.replace()` + `fsync` |
| S3 | boto3 | Batch delete (1000/batch), auto-region correction |
| MinIO | minio-py | S3-compatible, force path style |
| Azure | azure-storage-blob | Connection string, SAS token, managed identity, ABFSS URI |
| GCP | google-cloud-storage | Service account JSON, ADC, project scoping |

Every backend provides `to_duckdb_path()` for direct DuckDB-from-storage queries and `presign()` for temporary download URLs.

### 4.7 Locking

**Files:** `locking/redis_lock.py`, `locking/file_lock.py`

Two backends sharing the same interface:

| Backend | Mechanism | Use Case |
|---|---|---|
| **RedisLocking** | `SET key token NX EX` + Lua compare-and-delete | Production (distributed) |
| **FileLocking** | `fcntl.LOCK_EX` + JSON lock file + `fsync` | Development (single-node) |

Both include:
- Heartbeat thread (extends lock at half-TTL)
- `atexit` cleanup
- Token-based ownership verification
- Thread-safe `_held` tracking

### 4.8 Monitoring

**Files:** `monitoring_writer.py`, `monitoring_reader.py`

**Writer:** Non-blocking async pipeline embedded in the query hot path.
- Bounded in-memory queue → daemon thread → batched Redis `RPUSH` pipeline
- Exponential backoff on Redis failure
- Graceful degradation via `NullMonitoringLogger`
- Singleton cache with LRU eviction (max 256 loggers)

**Reader:** Queries persisted monitoring Parquet files via DuckDB over time windows with RBAC enforcement.

### 4.9 Staging & Pipes

**Files:** `staging_area.py`, `super_pipe.py`

Two-layer ingestion architecture:
1. **Staging Areas** — temporary Parquet holding zones with distributed locking and file indexing
2. **Pipes** — declarative rules binding a staging area to a target table with overwrite column semantics

### 4.10 History Cleaner (Garbage Collection)

**File:** `history_cleaner.py`

Timestamp-based GC with multiple safety layers:
1. Active resources explicitly excluded
2. Current snapshot path excluded
3. Only timestamp-prefixed filenames considered
4. Stricter of two timestamps used as threshold
5. RBAC requires CONTROL access (highest level)

### 4.11 Data Quality

**Files:** `reflection/quality/scheduler.py`, `checker.py`, `anomaly.py`, `config.py`, `history.py`, `routes.py`

Rule-based data quality checking triggered after ingestion via debounced `notify_ingest()` flags in Redis.

### 4.12 Logging & Observability

**File:** `logging.py`

Structured JSON logging with:
- Correlation ID propagation (`X-Correlation-ID` header)
- Request/response middleware for FastAPI
- Configurable format (JSON/text), level, file output, color
- `NO_COLOR` standard support

---

## 5. Dependency Tree

### 5.1 Module Dependency Graph (import direction: →)

```
config/settings.py          ← ZERO supertable imports (only stdlib + dotenv)
    ↑
config/defaults.py          → config/settings
    ↑
config/homedir.py           → config/settings, config/defaults
    ↑
redis_connector.py          → config/defaults, config/settings
    ↑
redis_catalog.py            → config/defaults, redis_connector
    ↑
┌───────────────────────────────────────────────────────────────┐
│                    DOMAIN LAYER                                │
│                                                                │
│  storage/*                → config/settings                    │
│  storage/storage_factory  → config/defaults, config/settings   │
│                                                                │
│  rbac/access_control      → config/defaults, redis_catalog     │
│  rbac/role_manager        → config/defaults, redis_catalog     │
│  rbac/user_manager        → config/defaults, redis_catalog     │
│                                                                │
│  simple_table             → config/defaults, storage, redis    │
│  super_table              → config/defaults, storage, redis,   │
│                             rbac, locking                      │
│                                                                │
│  meta_reader              → config/settings, config/defaults,  │
│                             redis_catalog, storage, rbac       │
│                                                                │
│  engine/engine_common     → config/defaults, config/settings,  │
│                             config/homedir                     │
│  engine/data_estimator    → config/defaults, config/settings,  │
│                             engine_common, redis_catalog,      │
│                             storage                            │
│  engine/executor          → config/defaults, config/settings   │
│  engine/duckdb_lite       → config/defaults, engine_common     │
│  engine/duckdb_pro        → config/defaults, config/settings,  │
│                             engine_common                      │
│  engine/spark_thrift      → config/defaults, config/settings,  │
│                             engine_common                      │
│                                                                │
│  data_reader              → config/defaults, storage, rbac,    │
│                             engine/*, redis_catalog,           │
│                             sql_parser, query_plan_manager,    │
│                             plan_extender, monitoring_writer   │
│                                                                │
│  data_writer              → config/defaults, storage, redis,   │
│                             rbac, locking, simple_table,       │
│                             super_table, processing,           │
│                             monitoring_writer, mirroring       │
│                                                                │
│  monitoring_writer        → config/defaults, config/settings,  │
│                             redis_connector                    │
│  monitoring_reader        → rbac, storage, engine, duckdb      │
│                                                                │
│  history_cleaner          → config/defaults, storage,          │
│                             redis_catalog, rbac, super_table   │
│                                                                │
│  staging_area             → config/defaults, storage, redis,   │
│                             rbac, locking                      │
│  super_pipe               → config/defaults, redis_catalog     │
└───────────────────────────────────────────────────────────────┘
    ↑
┌───────────────────────────────────────────────────────────────┐
│                    API / ENTRYPOINT LAYER                       │
│                                                                │
│  api/api.py               → config/settings (as _cfg),        │
│                             reflection/common (settings),      │
│                             data_reader, data_writer,          │
│                             meta_reader, rbac, staging,        │
│                             monitoring, compute, redis_catalog │
│                                                                │
│  api/application.py       → config/settings, logging,         │
│                             reflection/common (router)         │
│  api/auth.py              → config/settings                    │
│                                                                │
│  reflection/application.py → config/settings (as _cfg),       │
│                              reflection/common (settings)      │
│  reflection/common.py     → config/settings (as _cfg),        │
│                             redis, jinja2, session             │
│  reflection/compute.py    → config/settings                    │
│                                                                │
│  mcp/mcp_server.py        → config/settings, meta_reader,     │
│                             data_reader, data_writer, rbac,    │
│                             redis_catalog, sql_parser          │
│  mcp/web_app.py           → config/settings, mcp/web_client   │
│                                                                │
│  logging.py               → config/settings                    │
└───────────────────────────────────────────────────────────────┘
```

### 5.2 External Dependencies

| Package | Used By | Purpose | Criticality |
|---|---|---|---|
| `redis` / `redis-py` | redis_connector, redis_catalog, monitoring | Metadata store, locking, monitoring transport | **Critical** |
| `duckdb` | engine/duckdb_lite, duckdb_pro, engine_common | In-process SQL engine | **Critical** |
| `pandas` | data_reader, monitoring_reader | DataFrame result format | **Critical** |
| `polars` | data_writer, processing | In-memory columnar processing for writes | **Critical** |
| `pyarrow` | data_writer, staging, storage | Parquet I/O with zstd compression | **Critical** |
| `sqlglot` | engine_common, sql_parser, spark_thrift | SQL parsing, AST rewriting, dialect transpilation | **Critical** |
| `fastapi` | api, reflection, mcp/web_app | HTTP framework for all API entrypoints | **Critical** |
| `python-dotenv` | config/settings | Environment variable loading from `.env` | Important |
| `httpx` | reflection/application | Async reverse proxy client | Important |
| `jinja2` | reflection/common | HTML template rendering | Important |
| `pyhive` | engine/spark_thrift | Spark Thrift Server connection (optional) | Optional |
| `boto3` | storage/s3_storage | AWS S3 SDK | Backend-specific |
| `minio` | storage/minio_storage | MinIO SDK | Backend-specific |
| `azure-storage-blob` | storage/azure_storage | Azure Blob SDK | Backend-specific |
| `google-cloud-storage` | storage/gcp_storage | GCP Storage SDK | Backend-specific |
| `colorlog` | config/defaults | Colored console logging | Supporting |
| `mcp` (FastMCP SDK) | mcp/mcp_server | Model Context Protocol server framework | Important |

---

## 6. Security Model

### 6.1 Authentication

| Mechanism | Env Vars | Used By |
|---|---|---|
| **API Key** | `SUPERTABLE_AUTH_MODE=api_key`, `SUPERTABLE_API_KEY` | API Server |
| **Bearer Token** | `SUPERTABLE_AUTH_MODE=bearer`, `SUPERTABLE_BEARER_TOKEN` | API Server |
| **MCP Token** | `SUPERTABLE_MCP_TOKEN` | MCP Server (HMAC comparison) |
| **Superuser Token** | `SUPERTABLE_SUPERUSER_TOKEN` | Reflection UI, MCP Web Gateway |
| **Session Cookie** | `SUPERTABLE_SESSION_SECRET` | Reflection UI login |

All secret comparisons use `hmac.compare_digest()` to prevent timing attacks.

### 6.2 Authorization (RBAC)

- Five role types with static permission matrix
- Per-table, per-column, per-row security
- Filter definitions stored as JSON in Redis, translated to SQL WHERE at query time
- Superadmin/admin bypass all read filters
- Auto-bootstrap ensures every namespace is administrable

### 6.3 Query Security

- Agent-generated SQL passes through `SQLParser` for table/column extraction
- All queries execute through the RBAC view chain — no bypass possible
- Read-only enforcement in MCP server (DML/DDL rejected)
- SQL string parameterization via the SQLParser pipeline

---

## 7. Data Flows

### 7.1 Query Flow (Read)

```
Client SQL → query_sql() → DataReader.execute()
  → SQLParser: extract tables, columns, aliases
  → restrict_read_access(): RBAC gate → Dict[alias → RbacViewDef]
  → DataEstimator: Redis snapshots → parquet file lists → Reflection
  → Wire dedup/tombstone defs from table_config
  → Executor._auto_pick(): size + freshness → engine selection
  → Engine.execute():
       CREATE VIEW (parquet_scan over S3 files)
       CREATE RBAC VIEW (column + row filter)
       CREATE TOMBSTONE VIEW (NOT EXISTS anti-join)
       CREATE DEDUP VIEW (ROW_NUMBER window)
       REWRITE SQL (alias → hashed view name)
       EXECUTE rewritten SQL
       DROP views
  → extend_execution_plan(): telemetry
  → Return DataFrame
```

### 7.2 Write Flow (Ingest)

```
Arrow Table → DataWriter.write()
  → check_write_access(): RBAC gate
  → Acquire distributed lock (Redis SET NX EX)
  → Read current snapshot from Redis/storage
  → Convert to Polars DataFrame
  → Overlap detection via column-level zonemap stats
  → Anti-join merge with existing Parquet
  → Write new Parquet (zstd compression, column stats)
  → Write new Snapshot JSON (version N+1)
  → CAS update Redis leaf pointer (Lua atomic script)
  → Bump Redis root version
  → Release lock
  → Async: monitoring emit, DQ notification, mirror if enabled
```

### 7.3 MCP Agent Flow

```
Agent → list_tables (session start)
  ← system_hint, catalog, annotations, feedback
Agent → query_sql("SELECT * FROM orders WHERE region='EU'")
  → MCP Server validates token
  → Acquires concurrency slot
  → Offloads to thread: DataReader.execute(role_name=role)
  ← {status, columns, rows, rowcount, engine, elapsed_ms}
Agent → store_annotation(category="sql", instruction="weekly means rolling 7 days")
  → Persisted to __annotations__ system table
Agent → submit_feedback(rating="thumbs_up", query="...", response_summary="...")
  → Persisted to __feedback__ system table
```

---

## 8. Configuration Reference

### 8.1 Configuration Architecture

```
.env file → load_dotenv() [once] → Settings dataclass [frozen, 123 attrs]
                                         ↓
                              Every module imports `settings`
                              (no os.getenv() in business logic)
```

### 8.2 Key Configuration Categories

| Category | Count | Examples |
|---|---|---|
| Core | 4 | `SUPERTABLE_HOME`, `SUPERTABLE_ORGANIZATION`, `SUPERTABLE_PREFIX` |
| Storage (generic) | 8 | `STORAGE_TYPE`, `STORAGE_BUCKET`, `STORAGE_REGION`, `STORAGE_ENDPOINT_URL` |
| Storage (Azure) | 6 | `AZURE_STORAGE_ACCOUNT`, `AZURE_CONTAINER`, `AZURE_SAS_TOKEN` |
| Storage (GCP) | 4 | `GCS_BUCKET`, `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_SA_JSON` |
| DuckDB Engine | 11 | `SUPERTABLE_DUCKDB_MEMORY_LIMIT`, `SUPERTABLE_DUCKDB_THREADS` |
| Spark Engine | 5 | `SUPERTABLE_SPARK_QUERY_TIMEOUT`, `SPARK_MASTER_URL` |
| Redis | 14 | `SUPERTABLE_REDIS_HOST`, `SUPERTABLE_REDIS_URL`, Sentinel vars |
| Auth | 9 | `SUPERTABLE_AUTH_MODE`, `SUPERTABLE_API_KEY`, `SUPERTABLE_MCP_TOKEN` |
| MCP | 9 | `SUPERTABLE_MCP_PORT`, `SUPERTABLE_MAX_CONCURRENCY`, `MCP_WIRE` |
| Logging | 6 | `SUPERTABLE_LOG_LEVEL`, `SUPERTABLE_LOG_FORMAT`, `SUPERTABLE_LOG_FILE` |
| Monitoring | 2 | `SUPERTABLE_MONITORING_ENABLED`, `SUPERTABLE_MONITOR_CACHE_MAX` |
| Notebook | 4 | `SUPERTABLE_NOTEBOOK_PORT`, `SUPERTABLE_NOTEBOOK_ALLOWED_IMPORTS` |
| UI | 6 | `SUPERTABLE_UI_HOST`, `SUPERTABLE_UI_PORT`, `TEMPLATES_DIR` |

### 8.3 Engine Tuning Parameters (Redis-Overridable)

These 8 parameters support a three-tier resolver: Redis → env var → hardcoded default:

| Parameter | Env Var | Default |
|---|---|---|
| `engine_lite_max_bytes` | `SUPERTABLE_ENGINE_LITE_MAX_BYTES` | 100 MB |
| `engine_spark_min_bytes` | `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | 10 GB |
| `engine_freshness_sec` | `SUPERTABLE_ENGINE_FRESHNESS_SEC` | 300s |
| `duckdb_memory_limit` | `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | 1GB |
| `duckdb_io_multiplier` | `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | 3 |
| `duckdb_threads` | `SUPERTABLE_DUCKDB_THREADS` | auto |
| `duckdb_http_timeout` | `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | (DuckDB default) |
| `duckdb_external_cache_size` | `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | (disabled) |

---

## 9. File Index

### 9.1 All Source Files (non-test, non-studio)

| Path | Role |
|---|---|
| `config/settings.py` | **Central env var loader — single source of truth** |
| `config/defaults.py` | Legacy defaults dataclass + colored logger |
| `config/homedir.py` | App home directory resolution |
| `redis_connector.py` | Redis connection factory (Sentinel HA, SSL) |
| `redis_catalog.py` | Redis-backed metadata catalog (CAS, locking, RBAC, staging) |
| `data_reader.py` | Query orchestration facade |
| `data_writer.py` | Write-path orchestrator / storage engine |
| `data_classes.py` | DTOs: Reflection, RbacViewDef, DedupViewDef, TombstoneDef, etc. |
| `meta_reader.py` | Metadata reflection (schema, stats, catalog discovery) |
| `simple_table.py` | Table-level snapshot lifecycle |
| `super_table.py` | Root-level namespace object |
| `processing.py` | Merge algorithms, overlap detection, compaction |
| `staging_area.py` | Pre-commit data staging with locking |
| `super_pipe.py` | Declarative ingestion pipe definitions |
| `history_cleaner.py` | Garbage collection of stale files |
| `monitoring_writer.py` | Non-blocking async metrics pipeline |
| `monitoring_reader.py` | Historical monitoring query via DuckDB |
| `plan_extender.py` | Post-query telemetry enrichment |
| `query_plan_manager.py` | DuckDB profiling file management |
| `logging.py` | Structured JSON logging + correlation IDs |
| `engine/engine_enum.py` | Engine enum (AUTO, DUCKDB_LITE, DUCKDB_PRO, SPARK_SQL) |
| `engine/executor.py` | Engine router + auto-pick algorithm |
| `engine/engine_common.py` | S3 config, view chain, SQL rewriting, httpfs |
| `engine/duckdb_lite.py` | Ephemeral DuckDB executor |
| `engine/duckdb_pro.py` | Persistent singleton DuckDB with view cache |
| `engine/spark_thrift.py` | Remote Spark executor via PyHive |
| `engine/data_estimator.py` | Snapshot discovery, file collection, column validation |
| `engine/plan_stats.py` | Simple stat accumulator |
| `storage/storage_interface.py` | ABC defining 15+ abstract methods |
| `storage/storage_factory.py` | Factory function with lazy imports |
| `storage/local_storage.py` | Local filesystem backend |
| `storage/s3_storage.py` | AWS S3 backend |
| `storage/minio_storage.py` | MinIO backend |
| `storage/azure_storage.py` | Azure Blob backend |
| `storage/gcp_storage.py` | GCP Storage backend |
| `rbac/permissions.py` | Permission/RoleType enums, static matrix |
| `rbac/role_manager.py` | Role CRUD with auto-bootstrap |
| `rbac/user_manager.py` | User CRUD with role assignment |
| `rbac/access_control.py` | Authorization enforcement |
| `rbac/filter_builder.py` | JSON filter → SQL WHERE translation |
| `rbac/row_column_security.py` | Role permission normalization |
| `locking/redis_lock.py` | Redis distributed locking + Lua scripts |
| `locking/file_lock.py` | File-based locking (dev mode) |
| `mcp/mcp_server.py` | MCP server (22 tools, FastMCP) |
| `mcp/web_app.py` | FastAPI gateway for MCP over HTTP |
| `mcp/web_client.py` | MCP subprocess client |
| `mcp/web_server.py` | Uvicorn runner for web gateway |
| `mcp/mcp_client.py` | CLI MCP client for testing |
| `api/api.py` | All 70+ JSON API endpoints |
| `api/application.py` | FastAPI app + router mounting |
| `api/auth.py` | API key / bearer token authentication |
| `api/session.py` | Session management |
| `reflection/application.py` | UI server + reverse proxy |
| `reflection/common.py` | Settings adapter, Redis client, Jinja2 templates |
| `reflection/compute.py` | Compute pool management helpers |
| `reflection/security.py` | OData security, token verification |
| `odata/odata.py` | OData v4 endpoint handler |
| `odata/odata_handler.py` | OData query option processing |
| `utils/sql_parser.py` | SQL parsing via sqlglot |
| `utils/helper.py` | Utility functions |
| `utils/timer.py` | Performance timer |
| `infrastructure/python_worker/ws_server.py` | Notebook WebSocket server |
| `infrastructure/python_worker/warm_pool_manager.py` | Session pool management |
| `infrastructure/spark_worker/spark_manager.py` | Spark worker management |

---

## 10. Naming Conventions & Patterns

### 10.1 File Route Convention

Every `.py` file must have a route comment as its first line:
```python
# route: supertable.config.settings
```

### 10.2 Import Alias Convention

When `config.settings` collides with `reflection/common.py` settings:
```python
from supertable.config.settings import settings as _cfg  # central config
from supertable.reflection.common import settings          # UI settings adapter
```

This collision exists in `api/api.py` and `reflection/application.py`.

### 10.3 Multi-Tenancy Scoping

All operations are scoped by `(organization, super_name)`. These two values appear as parameters on virtually every function in the codebase.

### 10.4 Error Handling

- RBAC violations raise `PermissionError`
- Missing resources raise `HTTPException(404)`
- Invalid input raises `HTTPException(400)`
- Monitoring and telemetry never raise — all exceptions swallowed
- Storage backends log and skip individual file failures

---

## 11. Key Design Decisions

| Decision | Rationale |
|---|---|
| **Metadata in Redis, data on object storage** | Redis for fast pointer lookups and atomic CAS; object storage for cheap, durable, scalable data |
| **Immutable snapshot chain** | Enables versioning, rollback, and concurrent reads without locking |
| **View chain for security/integrity** | RBAC + tombstone + dedup applied as SQL VIEWs avoids physical data transformation |
| **DuckDB as primary engine** | In-process, zero-copy, excellent Parquet support, columnar analytics |
| **Frozen Settings dataclass** | Single source of truth, no runtime mutation, import-safe |
| **MCP-native design** | AI agents are first-class consumers — `query_sql()` returns structured data, `list_tables` primes context |
| **Conservative GC** | Multiple safety layers prevent accidental data loss in history cleanup |
| **Sentinel HA with fallback** | Production Redis resilience without hard Sentinel dependency |

---

## 12. Operational Notes

### 12.1 Port Assignments

| Service | Env Var | Default |
|---|---|---|
| API Server | `SUPERTABLE_API_PORT` | 8050 |
| Reflection UI | `SUPERTABLE_UI_PORT` | 8051 |
| MCP Server | `SUPERTABLE_MCP_PORT` | 8000 |
| MCP Web Gateway | `SUPERTABLE_MCP_WEB_PORT` | 8099 |
| Notebook WS | `SUPERTABLE_NOTEBOOK_PORT` | 8010 |

### 12.2 Health Checks

- `GET /healthz` on the API server (excluded from auth)
- Redis connectivity via `RedisConnector`
- Storage backend connectivity via `get_storage()`

### 12.3 Graceful Shutdown

- Monitoring writer flushes queue on `__exit__`
- Locking backends release all held locks via `atexit`
- httpx proxy client closed in FastAPI lifespan shutdown
