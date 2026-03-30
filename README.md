# SuperTable

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

**SuperTable — The simplest data warehouse & cataloging system.**

SuperTable is a data warehouse and cataloging platform that stores structured data on object storage (S3, MinIO, Azure Blob, GCP Cloud Storage, or local filesystem), manages metadata in Redis, and queries everything through DuckDB or Spark SQL. It is the engine behind the Data Island product line, published as the `supertable` Python package.

The platform serves three roles simultaneously. It is a **storage layer** that organizes data into versioned, append-only tables with snapshot isolation. It is a **query engine** that routes SQL to the fastest available backend (DuckDB for small-to-medium, Spark for large). And it is an **operational control plane** with built-in RBAC, audit logging, data quality checks, and monitoring — everything a team needs to run a production data platform without assembling a dozen tools.

---

## Architecture

SuperTable runs as a set of independent FastAPI servers sharing the same Redis catalog and storage backend. There is no single monolithic process — each server can scale independently, and all state lives in Redis + object storage.

```
Browser  → WebUI (:8050) ──httpx proxy──→ API (:8051)
AI tools → MCP (stdio or :8070) ──────→ DataReader / MetaReader
BI tools → OData (:8052) ─────────────→ DataReader (RBAC-scoped)
Scripts  → API (:8051) ──bearer token──→ JSON endpoints
```

| Component | Technology | Purpose |
|---|---|---|
| Application framework | FastAPI (Python 3.11+) | HTTP servers, routing, middleware |
| Metadata store | Redis 6+ | Catalog, locks, sessions, metrics, audit streams |
| Query engine (primary) | DuckDB | Embedded SQL on Parquet via httpfs |
| Query engine (large) | Spark SQL (optional) | Distributed SQL for large datasets |
| Data format | Apache Parquet | Columnar storage on object storage |
| Object storage | MinIO / S3 / Azure / GCP / local | Data file persistence |
| Mirror formats | Delta Lake, Apache Iceberg | Optional export after writes |
| Audit storage | Redis Streams + Parquet | Compliance logging (DORA, SOC 2) |

### Data model

Data is organized in a three-level hierarchy: **Organization** → **SuperTable** → **SimpleTable**.

An organization is a tenant namespace. A SuperTable is a virtual database that groups related tables. A SimpleTable is a versioned, append-only data table backed by Parquet files on object storage and metadata in Redis. Each write creates a new immutable snapshot — reads always see a consistent view.

---

## Quick start

### Docker (recommended)

```bash
git clone https://github.com/kladnasoft/supertable.git
cd supertable
cp .env.example .env
# Edit .env — set SUPERTABLE_SUPERUSER_TOKEN to a strong value

docker compose --profile infra --profile webui up -d
# Open http://localhost:8050
```

### Python package

```bash
pip install supertable
```

See [PYPI.md](PYPI.md) for SDK usage examples.

### Docker Hub

```bash
docker pull kladnasoft/supertable:latest
```

See [DOCKERHUB.md](DOCKERHUB.md) for deployment guide.

---

## What's included

- **Versioned tables** with snapshot isolation, upsert, soft deletes, and staleness filtering
- **DuckDB query engine** — embedded, zero-copy reads from object storage
- **Spark SQL** — optional, for queries exceeding DuckDB memory limits
- **RBAC** — role types (superadmin, admin, writer, reader) with row-level and column-level security
- **Audit logging** — tamper-evident SHA-256 hash chain, 7-year Parquet retention, DORA / SOC 2 ready
- **Data quality** — 16 built-in checks with statistical anomaly detection
- **Monitoring** — read/write/MCP metrics with latency percentiles
- **Ingestion** — file upload (CSV, JSON, Parquet) with staging areas and automated pipes
- **Delta Lake & Iceberg mirroring** — optional export after every write
- **MCP server** — 16+ AI tools for Claude Desktop, Cursor, and other LLM clients
- **OData 4.0** — role-scoped feeds for Excel, Power BI, and other BI tools
- **REST API** — 82+ JSON endpoints for all data operations

---

## Documentation

| # | Document | Description |
|---|---|---|
| 01 | [Platform Overview](docs/01_platform_overview.md) | Architecture, deployment, directory structure |
| 02 | [Configuration](docs/02_configuration.md) | All 100+ environment variables |
| 03 | [Storage Backends](docs/03_storage.md) | S3, MinIO, Azure, GCP, local setup |
| 04 | [Redis Catalog](docs/04_redis_catalog.md) | Metadata store, key naming, operations |
| 05 | [Data Model](docs/05_data_model.md) | Organization → SuperTable → SimpleTable |
| 06 | [Data Writer](docs/06_data_writer.md) | Write pipeline, locking, dedup, tombstones |
| 07 | [Query Engine](docs/07_query_engine.md) | DuckDB, Spark, engine selection |
| 08 | [Ingestion](docs/08_ingestion.md) | File upload, staging areas, pipes |
| 09 | [RBAC](docs/09_rbac.md) | Roles, users, row/column security |
| 10 | [Audit Log](docs/10_audit_log.md) | DORA/SOC 2 compliance, hash chain |
| 11 | [REST API](docs/11_rest_api.md) | All 82+ JSON endpoints |
| 12 | [MCP Server](docs/12_mcp.md) | AI tool integration |
| 13 | [Monitoring](docs/13_monitoring.md) | Metrics, structured logging |
| 14 | [Data Quality](docs/14_data_quality.md) | 16 checks, anomaly detection |
| 15 | [Mirroring](docs/15_mirroring.md) | Delta Lake, Iceberg export |
| 16 | [Locking](docs/16_locking.md) | Redis distributed locks |

---

## License

Super Table Public Use License (STPUL) v1.0 — see [LICENSE](LICENSE).

Copyright © Kladna Soft Kft. All rights reserved.
