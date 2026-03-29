# Data Island Core — Platform Overview

## Overview

Data Island Core is a data warehouse and cataloging platform that stores structured data on object storage (S3, MinIO, Azure Blob, GCP Cloud Storage, or local filesystem), manages metadata in Redis, and queries everything through DuckDB or Spark SQL. It is the engine behind the Data Island product line, published as the `supertable` Python package.

The platform serves three roles simultaneously. It is a storage layer that organizes data into versioned, append-only tables with snapshot isolation. It is a query engine that routes SQL to the fastest available backend (DuckDB for small-to-medium, Spark for large). And it is an operational control plane with built-in RBAC, audit logging, data quality checks, and monitoring — everything a team needs to run a production data platform without assembling a dozen tools.

Data Island Core runs as a set of FastAPI servers sharing the same Redis catalog and storage backend. There is no single monolithic process — each server can scale independently, and all state lives in Redis + object storage.

---

## Architecture

### Three servers, one catalog

Data Island Core consists of three independent server processes. All three share the same Redis instance (metadata catalog) and the same storage backend (data files). They can run on the same machine, in separate containers, or across different hosts.

**WebUI server** (`supertable/webui/application.py`) — Port 8050 by default. Serves the browser-based admin interface using Jinja2 templates. Handles login/logout and session cookies locally. All data operations are proxied to the API server via httpx.

**API server** (`supertable/api/application.py`) — Port 8051 by default. Serves all JSON endpoints: SQL execution, table management, ingestion, RBAC, monitoring, data quality, audit log. This is the only server that executes queries and writes data. The WebUI proxies to it; external tools call it directly.

**MCP server** (`supertable/mcp/mcp_server.py`) — Runs on stdio (for Claude Desktop, Cursor, etc.) or HTTP port 8000. Exposes SuperTable data as read-only MCP tools for AI clients. Supports the Model Context Protocol over stdio and streamable-HTTP transports.

The relationship is:

```
Browser → WebUI (:8050) ──httpx proxy──→ API (:8051)
AI client → MCP (stdio or :8000) ──direct import──→ DataReader / MetaReader
External tools → API (:8051) ──bearer token──→ JSON endpoints
```

The WebUI never touches Redis or storage directly for data operations — it always proxies to the API. The MCP server imports the Python modules directly (no HTTP) for performance.

### Shared infrastructure

**Redis** — Stores all metadata: table snapshots, RBAC roles and users, auth tokens, locks, staging/pipe configuration, monitoring metrics, audit streams, data quality results. Supports standalone Redis, Redis Sentinel for HA, and Redis with TLS.

**Object storage** — Stores all data files (Parquet), staging uploads, mirrored formats (Delta, Iceberg), and audit log archives. The storage backend is pluggable: MinIO (default), Amazon S3, Azure Blob Storage, Google Cloud Storage, or local filesystem.

**DuckDB** — Embedded SQL engine for queries. Uses httpfs to read Parquet files directly from object storage via presigned URLs. No data copying — DuckDB reads files in place.

**Spark SQL** (optional) — External SQL engine for large queries. Connected via Thrift server. Used when data exceeds DuckDB's memory limits.

---

## Directory structure

```
supertable/
├── __init__.py
├── server_common.py          Shared infrastructure: router, Redis client, catalog,
│                              auth guards, session helpers, discovery
├── config/                   Settings, defaults, home directory management
│   ├── settings.py           Central Settings dataclass + env var loading
│   ├── defaults.py           Default logger, constants
│   └── homedir.py            SUPERTABLE_HOME directory structure
│
├── api/                      JSON API server (standalone, port 8051)
│   ├── application.py        FastAPI app, middleware, lifespan
│   ├── api.py                All 82+ endpoint handlers
│   └── session.py            Cookie-based session management, HMAC signing
│
├── webui/                    Browser UI server (port 8050)
│   ├── application.py        FastAPI app, login/logout, template rendering, API proxy
│   ├── web_auth.py           Jinja2 templates + login page renderer
│   ├── templates/            HTML templates (sidebar, home, execute, tables, etc.)
│   └── static/               CSS, JS, images
│
├── mcp/                      Model Context Protocol server
│   ├── mcp_server.py         Tool definitions, transport config, auth
│   ├── web_app.py            Browser-based MCP tester UI
│   └── web_server.py         Standalone HTTP wrapper for testing
│
├── audit/                    Compliance audit logging (DORA / SOC 2)
│   ├── events.py             AuditEvent dataclass, categories, actions
│   ├── logger.py             Queue + background worker, singleton cache
│   ├── writer_redis.py       Redis Streams hot tier
│   ├── writer_parquet.py     Parquet warm tier with hash chaining
│   ├── middleware.py          Auto-logs 401/403/500 responses
│   └── ...                   Chain, crypto, reader, export, retention
│
├── engine/                   Query engine backends
│   ├── executor.py           Engine router (AUTO/DuckDB/Spark)
│   ├── duckdb_lite.py        DuckDB embedded (single-process)
│   ├── duckdb_pro.py         DuckDB with advanced features
│   ├── spark_thrift.py       Spark SQL via Thrift
│   └── data_estimator.py     Query cost estimation for engine selection
│
├── storage/                  Pluggable storage backends
│   ├── storage_interface.py  Abstract base class
│   ├── storage_factory.py    Backend selection by STORAGE_TYPE env var
│   ├── minio_storage.py      MinIO / S3-compatible
│   ├── s3_storage.py         Amazon S3
│   ├── azure_storage.py      Azure Blob Storage
│   ├── gcp_storage.py        Google Cloud Storage
│   └── local_storage.py      Local filesystem
│
├── rbac/                     Role-based access control
│   ├── access_control.py     Read/write access checks
│   ├── role_manager.py       Role CRUD
│   ├── user_manager.py       User CRUD
│   ├── row_column_security.py Row and column filtering
│   └── filter_builder.py     SQL WHERE clause generation from RBAC rules
│
├── services/                 Shared business logic (used by both API and WebUI)
│   ├── execute.py            SQL query helpers
│   ├── ingestion.py          Staging/pipe management helpers
│   ├── security.py           RBAC + OData endpoint helpers
│   ├── monitoring.py         Monitoring data readers
│   ├── compute.py            Engine configuration helpers
│   └── quality/              Data quality subsystem
│       ├── checker.py        Quality check execution
│       ├── config.py         Built-in check definitions
│       ├── anomaly.py        Anomaly detection algorithms
│       ├── history.py        Quality result history
│       └── scheduler.py      On-ingest quality scheduling
│
├── locking/                  Distributed locking
│   ├── redis_lock.py         Redis-based distributed locks
│   └── file_lock.py          File-based fallback locks
│
├── mirroring/                Format mirroring (export after write)
│   ├── mirror_formats.py     Mirror orchestrator
│   ├── mirror_parquet.py     Parquet export
│   ├── mirror_delta.py       Delta Lake export
│   └── mirror_iceberg.py     Apache Iceberg export
│
├── data_reader.py            Read pipeline: SQL → engine → results (with RBAC)
├── data_writer.py            Write pipeline: data → validate → lock → write → monitor
├── meta_reader.py            Metadata aggregator (schemas, stats, table lists)
├── redis_catalog.py          Redis metadata store (all catalog operations)
├── redis_connector.py        Redis connection pooling, Sentinel, TLS
├── super_table.py            SuperTable entity (virtual database)
├── simple_table.py           SimpleTable entity (versioned table within a SuperTable)
├── staging_area.py           Staging area management (file uploads)
├── super_pipe.py             Data pipe management (staging → table automation)
├── monitoring_writer.py      Metrics writer (queue + background worker → Redis)
├── processing.py             Overlap detection, dedup, tombstone management
├── plan_extender.py          Query plan extension and optimization
├── query_plan_manager.py     Query plan profiling and statistics
├── data_classes.py           Shared dataclasses (TableDefinition, SuperSnapshot, etc.)
├── logging.py                Structured JSON logging + correlation ID middleware
└── utils/
    ├── helper.py             General utilities
    ├── sql_parser.py         SQL parsing helpers
    └── timer.py              Timing utilities
```

---

## Data model

Data is organized in a three-level hierarchy:

**Organization** — A tenant namespace. All data, metadata, roles, and tokens are scoped to an organization. Set via `SUPERTABLE_ORGANIZATION`.

**SuperTable** — A virtual database within an organization. Contains multiple tables, staging areas, pipes, and quality configurations. One Redis + storage deployment can host many SuperTables.

**SimpleTable** — A versioned data table within a SuperTable. Each write creates a new snapshot (version). Snapshots are immutable — data files are never modified in place. Reads always see a consistent snapshot.

Data files are Parquet, stored on the configured object storage backend at:

```
{org}/{super_name}/{simple_name}/v{version}/{filename}.parquet
```

---

## Deployment

### Docker Compose (recommended)

Prerequisites: Docker and Docker Compose v2.

```bash
# 1. Clone the repository
git clone https://github.com/kladnasoft/supertable.git
cd supertable

# 2. Start infrastructure
cd infrastructure/redis && docker compose up -d && cd ../..
cd infrastructure/minio && docker compose up -d && cd ../..

# 3. Configure environment (create .env — see README for full example)

# 4. Start the WebUI (includes API proxy)
docker compose --profile reflection up -d
# Open http://localhost:8050
```

Available Docker Compose profiles:

| Profile | Service | Default port | Description |
|---|---|---|---|
| `reflection` | WebUI + API proxy | 8050 | Browser admin interface |
| `api` | REST API | 8090 | Standalone JSON API |
| `mcp` | MCP server | 8070, 8099 | AI tool server (stdio + HTTP + tester) |
| `mcp-http` | MCP over HTTPS | 8070 | MCP with Caddy TLS |
| `notebook` | Notebook server | 8000 | WebSocket notebook execution |
| `spark` | Spark plug | 8010 | Spark WebSocket server |
| `https` | TLS sidecar | 8443+ | Caddy HTTPS (combinable with any profile) |

### Bare metal

```bash
# Install dependencies
pip install -r requirements.txt

# Start the WebUI server
uvicorn supertable.webui.application:app --host 0.0.0.0 --port 8050

# Start the API server (separate process)
uvicorn supertable.api.application:app --host 0.0.0.0 --port 8051

# Start the MCP server (stdio)
python -u supertable/mcp/mcp_server.py
```

### Minimum viable setup

To run Data Island Core, you need:

1. **Redis** — any Redis 6+ instance (standalone or Sentinel)
2. **Object storage** — MinIO, S3, Azure Blob, GCP, or a local directory
3. **At least one server** — typically the `reflection` profile (WebUI + API proxy)

Set these environment variables:

```bash
SUPERTABLE_ORGANIZATION=my-org
SUPERTABLE_SUPERTOKEN=my-secret-token
STORAGE_TYPE=MINIO
STORAGE_ENDPOINT_URL=http://localhost:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_BUCKET=supertable
REDIS_HOST=localhost
REDIS_PORT=6379
```

---

## Authentication

Data Island Core supports two authentication modes:

**Superuser login** — Uses `SUPERTABLE_SUPERTOKEN`. Full access to all operations including RBAC management, audit log, and configuration. Intended for administrators.

**Regular user login** — Uses a username + access token (tokens are created by a superuser). Access is governed by RBAC roles assigned to the user. Row-level and column-level security filters are applied to every query.

**API token authentication** — External tools authenticate with bearer tokens created through the token management API. Tokens are SHA-256 hashed before storage — the plaintext is never persisted.

**MCP authentication** — The MCP server supports an optional shared secret (`SUPERTABLE_MCP_AUTH_TOKEN`) and per-request user hash enforcement (`SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH`).

---

## Technology stack

| Component | Technology | Purpose |
|---|---|---|
| Application framework | FastAPI (Python 3.11+) | HTTP servers, routing, middleware |
| Templates | Jinja2 | WebUI HTML rendering |
| Metadata store | Redis 6+ | Catalog, locks, sessions, metrics, audit streams |
| Query engine (primary) | DuckDB | Embedded SQL on Parquet via httpfs |
| Query engine (large) | Spark SQL (Thrift) | Distributed SQL for large datasets |
| Data format | Apache Parquet | Columnar storage on object storage |
| Object storage | MinIO / S3 / Azure / GCP / local | Data file persistence |
| Mirror formats | Delta Lake, Apache Iceberg | Optional export after writes |
| Audit storage | Redis Streams + Parquet | Compliance logging (DORA, SOC 2) |
| Container runtime | Docker + Docker Compose | Packaging and orchestration |

---

## Frequently asked questions

**What is the relationship between SuperTable (the package) and Data Island Core (the product)?**
SuperTable is the Python package name (`supertable/`). Data Island Core is the product name shown in the UI and documentation. They refer to the same codebase.

**Can I run everything in a single process?**
The WebUI server in `reflection` mode proxies API calls internally. For development, this is a single-process deployment. For production, running the API server as a separate process is recommended for independent scaling.

**Does Data Island Core require Spark?**
No. Spark is optional. DuckDB handles most workloads. Spark is only needed when individual queries scan more data than fits in DuckDB's memory (configurable via `SUPERTABLE_DUCKDB_MEMORY_LIMIT`).

**What Python version is required?**
Python 3.11 or higher.

**Can I use Data Island Core without Docker?**
Yes. Install the Python dependencies from `requirements.txt` and run the servers directly with `uvicorn`. Docker is recommended for production but not required.

**How do I connect AI tools (Claude, ChatGPT, Cursor)?**
Use the MCP server. For local tools, configure stdio transport in the tool's MCP settings. For remote tools, run the MCP server with `SUPERTABLE_MCP_TRANSPORT=streamable-http` and add the URL as a custom connector.

**Is there a Python SDK for programmatic access?**
The `supertable` package itself is the SDK. Import `SuperTable`, `SimpleTable`, `DataReader`, or `DataWriter` directly in your Python code. The REST API provides the same functionality over HTTP for non-Python clients.

**What compliance frameworks does Data Island Core support?**
The audit log module is designed for EU DORA (Regulation 2022/2554) and SOC 2 Type II. See the Audit Log documentation for full compliance mapping.

**How do I back up the platform?**
Back up Redis (RDB/AOF snapshots) and the object storage bucket. All platform state lives in these two systems. The Parquet files on storage are the system of record for data; Redis is the system of record for metadata.

**What happens if Redis goes down?**
Reads against cached metadata may still work briefly. Writes, RBAC checks, and locking will fail until Redis recovers. The platform is designed for Redis to be highly available — use Redis Sentinel in production.
