# SuperTable — Platform Overview

## What is SuperTable

SuperTable is a versioned data lake platform that integrates multiple tables into a single, cohesive framework for SQL analytics. It is designed for teams that need:

- **Versioned, append-only storage** — every write creates an immutable snapshot; no data is ever silently overwritten
- **Multi-cloud portability** — the same codebase runs on AWS S3, Azure Blob, Google Cloud Storage, MinIO, or local disk
- **SQL-first analytics** — standard SQL queries with automatic engine selection (DuckDB for small/medium, Spark for large datasets)
- **Enterprise security** — role-based access control with row-level and column-level security, plus tamper-evident audit logging
- **AI-ready** — built-in MCP server lets AI assistants (Claude Desktop) query and manage data through natural language
- **BI-tool integration** — OData 4.0 endpoint for Power BI, Excel, and Tableau

SuperTable is distributed as a pip-installable Python package (`pip install supertable`) and as Docker containers.

## Architecture

SuperTable runs as four independent servers sharing a Redis metadata catalog and a pluggable storage backend:

```
┌────────────────────────────────────────────────────────────┐
│                        Clients                              │
│  Browser   SDK/API   Power BI/Excel   Claude Desktop/AI    │
└────┬─────────┬───────────┬──────────────────┬──────────────┘
     │         │           │                  │
     ▼         ▼           ▼                  ▼
┌─────────┐ ┌─────────┐ ┌─────────┐    ┌─────────┐
│ WebUI   │ │  API    │ │ OData   │    │  MCP    │
│ :8050   │ │ :8051   │ │ :8052   │    │ :8099   │
│ FastAPI │ │ FastAPI │ │ FastAPI │    │ FastMCP │
│ Jinja2  │ │ JSON    │ │ OData4  │    │ stdio/  │
│ + proxy │ │ only    │ │ XML+JSON│    │ HTTP    │
└────┬────┘ └────┬────┘ └────┬────┘    └────┬────┘
     │           │           │              │
     └───────────┴─────┬─────┴──────────────┘
                       │
          ┌────────────┴────────────┐
          │                         │
     ┌────▼────┐            ┌──────▼──────┐
     │  Redis  │            │   Storage   │
     │ Catalog │            │  (Parquet)  │
     │ metadata│            │ S3/MinIO/   │
     │ + locks │            │ Azure/GCP/  │
     │ + audit │            │ Local       │
     └─────────┘            └─────────────┘
```

### API Server (port 8051)

The core JSON REST API. Serves all 128+ endpoints for data operations, RBAC management, ingestion, monitoring, quality checks, sharing, and administration. Stateless — multiple instances can run behind a load balancer.

- Entry point: `supertable.api.application:app`
- Middleware: structured logging, audit logging, optional rate limiting
- Starts GC scheduler as a background daemon thread

### WebUI Server (port 8050)

Browser-based management console. Renders HTML pages via Jinja2 templates and acts as a reverse proxy to the API server (via httpx). Users interact with the WebUI; it forwards all `/api/*` calls to the API server internally.

- Entry point: `supertable.webui.application:app`
- 16 pages: home, tables, SQL editor, ingestion, monitoring, security, audit, quality, sharing, engine, platform

### OData Server (port 8052)

OData 4.0 feed server for BI tools. Power BI, Excel, Tableau, and other OData consumers connect directly. Authentication uses OData-specific bearer tokens (`st_od_*` prefix) with RBAC role binding.

- Entry point: `supertable.odata.application:app`
- CORS enabled for cross-origin BI tool access
- Supports `$filter`, `$select`, `$orderby`, `$top`, `$skip`

### MCP Server (port 8099)

Model Context Protocol server for AI assistants. Exposes SuperTable operations as MCP tools that Claude Desktop and other MCP clients can invoke. Supports both stdio transport (local) and streamable HTTP (remote).

- Entry point: `supertable.mcp.web_app:app`
- Web tester UI at `:8099/web`
- Concurrency-controlled (SUPERTABLE_MAX_CONCURRENCY)

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.10+ |
| Web framework | FastAPI + Uvicorn |
| Metadata store | Redis 7 (standalone or Sentinel HA) |
| Query engines | DuckDB (embedded), Spark SQL (via Thrift) |
| Data format | Apache Parquet (via PyArrow) |
| Data processing | Polars, Pandas, NumPy |
| SQL parsing | SQLGlot |
| MCP protocol | FastMCP |
| Templating | Jinja2 |
| HTTP client | httpx (async) |
| Storage SDKs | boto3, minio, azure-storage-blob, google-cloud-storage |

## Deployment Modes

### pip install (Python SDK)

```bash
pip install supertable              # core + local storage
pip install supertable[s3]          # + AWS S3
pip install supertable[minio]       # + MinIO
pip install supertable[azure]       # + Azure Blob
pip install supertable[gcp]         # + Google Cloud Storage
pip install supertable[all]         # all cloud backends
```

The SDK package excludes server modules (api, webui, odata, mcp) — it includes only the core data engine for embedded use in Python applications and notebooks.

### Docker Compose

```bash
# Infrastructure + WebUI (most common)
docker compose --profile infra --profile webui up -d

# Add OData for BI tools
docker compose --profile infra --profile webui --profile odata up -d

# API + MCP for programmatic/AI access
docker compose --profile infra --profile api --profile mcp up -d

# HTTPS via Caddy sidecar
docker compose --profile infra --profile webui --profile https up -d
```

Profiles: `infra` (Redis + MinIO), `webui`, `api`, `odata`, `mcp`, `mcp-http`, `https`

## Package Structure

```
supertable/
├── api/                 # REST API server (128+ endpoints)
├── webui/               # Web UI server (Jinja2 templates + proxy)
├── odata/               # OData 4.0 server
├── mcp/                 # MCP server (AI integration)
├── engine/              # Query engines (DuckDB Lite/Pro, Spark)
├── storage/             # Storage backends (S3, MinIO, Azure, GCP, Local)
├── rbac/                # Role-based access control
├── audit/               # Audit logging & compliance
├── locking/             # Distributed locking (Redis, file)
├── mirroring/           # Table format export (Delta, Iceberg, Parquet)
├── services/            # Business logic services
│   ├── quality/         #   Data quality checks
│   ├── sharing.py       #   Cross-org data sharing
│   ├── publication.py   #   Data publication
│   ├── monitoring.py    #   Metrics collection
│   ├── ingestion.py     #   File ingestion & staging
│   ├── gc.py            #   Garbage collection
│   ├── gc_scheduler.py  #   GC scheduling
│   ├── security.py      #   Token & auth management
│   ├── time_travel.py   #   Point-in-time queries
│   └── ...
├── config/              # Configuration (100+ env vars)
├── utils/               # SQL parser, helpers
├── super_table.py       # Core coordination class
├── simple_table.py      # Versioned append-only table
├── data_reader.py       # Read interface
├── data_writer.py       # Write interface
├── processing.py        # Parquet processing engine
├── redis_catalog.py     # Redis metadata operations
├── redis_connector.py   # Redis connection management
├── meta_reader.py       # Metadata reading
├── logging.py           # Structured logging
├── service_registry.py  # Process heartbeat & discovery
└── data_classes.py      # Shared data structures
```

## Data Flow

```
Write Path:
  Client → API → Lock table → Validate → Write Parquet → Update Catalog → Mirror → Unlock

Read Path:
  Client → API → Resolve tables → Build view chain → Estimate size
        → Select engine (Lite/Pro/Spark) → Execute SQL → Return results

View Chain (transparent to user):
  Base Parquet files → Dedup view → Tombstone view → RBAC view → User query
```

## Service Registry

Every running process (API, WebUI, OData, MCP, SDK) registers itself in Redis with a 30-second TTL key, refreshed every 15 seconds. If a process crashes, its key expires automatically. The monitoring dashboard reads these keys to show all running instances.

Key pattern: `supertable:registry:{service_type}:{hostname}:{pid}`

## Cross-References

| Doc | Module |
|-----|--------|
| [02 Configuration](02_configuration.md) | All 100+ environment variables |
| [03 Data Model](03_data_model.md) | Organization → SuperTable → SimpleTable hierarchy |
| [04 Storage](04_storage.md) | Storage backends (S3, MinIO, Azure, GCP, Local) |
| [05 Redis Catalog](05_redis_catalog.md) | Metadata store and key naming |
| [06 Data Writer](06_data_writer.md) | Write pipeline with locking and dedup |
| [07 Ingestion](07_ingestion.md) | File upload, staging areas, pipes |
| [08 Locking](08_locking.md) | Distributed locking (Redis + file) |
| [09 Query Engine](09_query_engine.md) | DuckDB Lite/Pro, Spark SQL, engine selection |
| [10 Data Reader](10_data_reader.md) | Read facade, time travel, view chain |
| [11 RBAC](11_rbac.md) | Roles, users, row/column-level security |
| [12 Audit](12_audit.md) | Compliance logging, hash chains, SIEM |
| [13 Authentication](13_security_services.md) | Auth modes, tokens, sessions |
| [14 REST API](14_rest_api.md) | All 128+ endpoints |
| [15 OData](15_odata.md) | OData 4.0 for BI tools |
| [16 MCP](16_mcp.md) | AI assistant integration |
| [17 Web UI](17_webui.md) | Browser management console |
| [18 Sharing](18_sharing.md) | Cross-org data sharing |
| [19 Mirroring](19_mirroring.md) | Delta Lake, Iceberg, Parquet export |
| [20 Data Quality](20_data_quality.md) | Quality checks and anomaly detection |
| [21 Garbage Collection](21_garbage_collection.md) | Storage cleanup |
| [22 Monitoring](22_monitoring.md) | Metrics, logging, service registry |
| [23 Rate Limiting](23_rate_limiting.md) | API throttling |
| [24 Error Codes](24_error_codes.md) | Error catalog |
| [25 Python SDK](25_python_sdk.md) | pip package usage |
