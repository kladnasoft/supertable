# SuperTable — Platform Overview

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

## Overview

SuperTable is a data warehouse and cataloging platform that stores structured data on object storage (S3, MinIO, Azure Blob, GCP Cloud Storage, or local filesystem), manages metadata in Redis, and queries everything through DuckDB or Spark SQL. It is the engine behind the Data Island product line, published as the `supertable` Python package.

The platform serves three roles simultaneously. It is a storage layer that organizes data into versioned, append-only tables with snapshot isolation. It is a query engine that routes SQL to the fastest available backend (DuckDB for small-to-medium, Spark for large). And it is an operational control plane with built-in RBAC, audit logging, data quality checks, and monitoring — everything a team needs to run a production data platform without assembling a dozen tools.

Data Island Core runs as a set of FastAPI servers sharing the same Redis catalog and storage backend. There is no single monolithic process — each server can scale independently, and all state lives in Redis + object storage.

---

## Architecture

### Four servers, one catalog

Data Island Core consists of four independent server processes. All four share the same Redis instance (metadata catalog) and the same storage backend (data files). They can run on the same machine, in separate containers, or across different hosts.

**WebUI server** (`supertable/webui/application.py`) — Port 8050. Serves the browser-based admin interface using Jinja2 templates. Handles login/logout and session cookies. All data operations are proxied to the API server via httpx.

**API server** (`supertable/api/application.py`) — Port 8051. Serves all JSON endpoints: SQL execution, table management, ingestion, RBAC, monitoring, data quality, audit log. This is the only server that executes queries and writes data. The WebUI proxies to it; external tools call it directly.

**OData server** (`supertable/odata/application.py`) — Port 8052. Exposes SuperTable data as OData 4.0 feeds for BI tools (Excel, Power BI). Authenticates via dedicated OData bearer tokens, each scoped to an RBAC role. All data access is filtered through the same RBAC layer as the API.

**MCP server** (`supertable/mcp/mcp_server.py`) — Runs on stdio (for Claude Desktop, Cursor) or HTTP port 8070. Exposes SuperTable data as read-only MCP tools for AI clients. Supports the Model Context Protocol over stdio and streamable-HTTP transports.

```
Browser  → WebUI (:8050) ──httpx proxy──→ API (:8051)
AI tools → MCP (stdio or :8070) ──────→ DataReader / MetaReader
BI tools → OData (:8052) ─────────────→ DataReader (RBAC-scoped)
Scripts  → API (:8051) ──bearer token──→ JSON endpoints
```

### Shared infrastructure

**Redis** — Stores all metadata: table snapshots, RBAC roles and users, auth tokens, locks, staging/pipe configuration, monitoring metrics, audit streams, data quality results. Supports standalone Redis, Redis Sentinel for HA, and Redis with TLS.

**Object storage** — Stores all data files (Parquet), staging uploads, mirrored formats (Delta, Iceberg), and audit log archives. The storage backend is pluggable: MinIO (default), Amazon S3, Azure Blob Storage, Google Cloud Storage, or local filesystem.

**DuckDB** — Embedded SQL engine for queries. Uses httpfs to read Parquet files directly from object storage via presigned URLs. No data copying — DuckDB reads files in place.

**Spark SQL** (optional) — External SQL engine for large queries. Connected via Thrift server. Used when data exceeds DuckDB's memory limits.

---

## Data model

Data is organized in a three-level hierarchy: **Organization** → **SuperTable** → **SimpleTable**.

An organization is a tenant namespace. All data, metadata, roles, and tokens are scoped to an organization. A SuperTable is a virtual database within an organization — it contains multiple tables, staging areas, pipes, and quality configurations. A SimpleTable is a versioned data table within a SuperTable. Each write creates a new snapshot (version). Snapshots are immutable — data files are never modified in place. Reads always see a consistent snapshot.

Data files are Parquet, stored on the configured object storage backend at:

```
{org}/{super_name}/{simple_name}/v{version}/{filename}.parquet
```

### Write features

- **Upsert** via `overwrite_columns` — incoming rows with matching keys replace existing rows
- **Append** — omit `overwrite_columns` to append without dedup
- **Soft delete** — `delete_only=True` removes matching rows via tombstones (O(1), no file rewriting)
- **Staleness filter** — `newer_than` column prevents replayed data from overwriting newer updates
- **Per-table locking** — Redis distributed locks with heartbeat renewal prevent concurrent writes

### Query features

- DuckDB SQL on Parquet files (reads directly from object storage, no copying)
- Automatic engine routing: DuckDB Lite for small, DuckDB Pro for medium (with caching), Spark for large
- Dedup-on-read for tables with primary keys (ROW_NUMBER window function)
- Tombstone exclusion for soft-deleted rows
- RBAC: row-level and column-level filters applied automatically per role

---

## Security

### RBAC

Four role types: `superadmin` (full access), `admin` (management + read/write), `writer` (write + read), `reader` (read only). Each role defines which tables are accessible, which columns are visible, and which row-level WHERE filters are applied. Every query passes through the RBAC layer — there are no backdoors.

### Authentication

- **Superuser login** — uses `SUPERTABLE_SUPERUSER_TOKEN` for admin access
- **Regular user login** — username + access token, scoped to RBAC roles
- **API bearer tokens** — SHA-256 hashed before storage, plaintext never persisted
- **OData bearer tokens** — per-endpoint tokens scoped to an RBAC role
- **MCP shared secret** — optional `SUPERTABLE_MCP_TOKEN` for AI tool auth

### Audit logging

Every security-relevant action is recorded: logins, queries, data mutations, permission changes, configuration updates. Events are chained with SHA-256 hashes so tampering is detectable. Hot tier in Redis Streams (last 24 hours, real-time). Warm tier in Parquet (7-year retention by default). Built for EU DORA (Regulation 2022/2554) and SOC 2 Type II compliance. SIEM integration via Redis Stream consumer groups.

---

## Operations

### Monitoring

Read/write/MCP metrics captured asynchronously via background worker threads. Stored in Redis, queryable from the admin UI and API. Includes query timing, row counts, engine selection, error rates, and latency percentiles.

### Data quality

16 built-in checks (row count delta, freshness, schema drift, NULL rates, distinct count shifts, min/max breaches, mean drift, Shannon entropy, percentile tracking, and more). Statistical anomaly detection distinguishes expected variation from genuine issues. Triggered automatically on ingest with configurable schedules and cooldowns.

### Mirroring

Optional post-write export to Delta Lake, Apache Iceberg, or Parquet snapshot formats. Runs after every write, outside the lock — never blocks ingestion.

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

## Deployment

### Docker Compose (recommended)

```bash
git clone https://github.com/kladnasoft/supertable.git
cd supertable
cp .env.example .env
# Edit .env — set SUPERTABLE_SUPERUSER_TOKEN

# Start infrastructure + WebUI
docker compose --profile infra --profile webui up -d
# Open http://localhost:8050

# Add more services as needed
docker compose --profile infra --profile webui --profile odata --profile mcp up -d
```

### Bare metal

```bash
pip install -r requirements-docker.txt

uvicorn supertable.webui.application:app --host 0.0.0.0 --port 8050
uvicorn supertable.api.application:app --host 0.0.0.0 --port 8051
uvicorn supertable.odata.application:app --host 0.0.0.0 --port 8052
python -u supertable/mcp/mcp_server.py
```

### Minimum viable setup

1. **Redis** — any Redis 6+ instance (standalone or Sentinel)
2. **Object storage** — MinIO, S3, Azure Blob, GCP, or a local directory
3. **At least one server** — typically the `webui` profile

---

## FAQ

**What is the relationship between SuperTable and Data Island Core?**
SuperTable is the Python package name (`supertable/`). Data Island Core is the product name. They refer to the same codebase.

**Can I run everything in a single process?**
The WebUI server proxies API calls internally. For development, this is a single-process deployment. For production, running the API server as a separate process is recommended.

**Does Data Island Core require Spark?**
No. Spark is optional. DuckDB handles most workloads. Spark is only needed for queries exceeding DuckDB's memory limits.

**How do I connect AI tools?**
Use the MCP server. For Claude Desktop, configure stdio transport in `claude_desktop_config.json`. For remote tools, run with `SUPERTABLE_MCP_TRANSPORT=streamable-http` and add the URL as a custom connector.

**How do I connect Excel or Power BI?**
Use the OData server. Create an OData endpoint in the admin UI (scoped to an RBAC role), then connect with the OData bearer token.

**Is there a Python SDK?**
The `supertable` package itself is the SDK. Import `DataReader`, `DataWriter`, or `MetaReader` directly. The REST API provides the same functionality over HTTP for non-Python clients.

**What compliance frameworks are supported?**
The audit log is designed for EU DORA (Regulation 2022/2554) and SOC 2 Type II. See [Audit Log documentation](docs/10_audit_log.md).

**How do I back up the platform?**
Back up Redis (RDB/AOF snapshots) and the object storage bucket. All state lives in these two systems.

**What happens if Redis goes down?**
Reads against cached metadata may still work briefly. Writes, RBAC checks, and locking will fail until Redis recovers. Use Redis Sentinel in production for high availability.

---

## License

Super Table Public Use License (STPUL) v1.0 — see [LICENSE](LICENSE).

Copyright © Kladna Soft Kft. All rights reserved.
