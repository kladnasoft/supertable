# SuperTable

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

**SuperTable — The simplest data warehouse & cataloging system.**
A high‑performance, lightweight transaction catalog that defaults to **Redis (catalog/locks)** + **MinIO (object storage)** via DuckDB httpfs.

> **This README was updated to correct environment variable names and document Compose profiles.**
> - Canonical storage variables are **`STORAGE_*`** (not `AWS_S3_*`), except when using real AWS S3 where `AWS_*` is expected.
> - Redis vars are **`REDIS_HOST/PORT/DB/PASSWORD`** (DB **0** recommended by default).
> - Profiles: `admin`, `mcp`, `infra` with “host services default” behavior.

---

## Contents

- [What’s new](#whats-new)
- [Architecture](#architecture)
- [Quick start (Docker Compose)](#quick-start-docker-compose)
- [Profiles & run matrix](#profiles--run-matrix)
- [Admin UI](#admin-ui)
- [MCP server (stdio)](#mcp-server-stdio)
- [Configuration](#configuration)
  - [Redis](#redis)
  - [MinIO (default)](#minio-default)
  - [Amazon S3](#amazon-s3)
  - [Azure Blob](#azure-blob)
  - [GCP Storage](#gcp-storage)
  - [DuckDB tuning](#duckdb-tuning)
  - [Security](#security)
- [Environment reference](#environment-reference)
- [Local development](#local-development)
- [Production deployment](#production-deployment)
- [FAQ](#faq)

---

## What’s new

- **Default backends:** `LOCKING_BACKEND=redis` and `STORAGE_TYPE=MINIO`.
- **Out-of-the-box Admin UI** (FastAPI + Jinja2) on port **8000**.
- **MCP stdio server** (`mcp_server.py`) with optional hash enforcement.
- **Profiles** for flexible runtime: host-backed by default, `infra` when you want built-in Redis+MinIO.

---

## Architecture

- **Catalog & Locks:** Redis keys (e.g., `supertable:<org>:<super>:meta:*`).
- **Data files:** Object storage: MinIO/S3/Azure/GCS (MinIO by default).
- **Query:** DuckDB (embedded) using httpfs.
- **Mirrors:** Optional “latest-only” writers to Delta/Iceberg.

---

## Quick start (Docker Compose)

Requirements: Docker & docker-compose.

```bash
# 1) Clone and build
git clone https://github.com/kladnasoft/supertable.git
cd supertable
docker compose build --no-cache

# 2) Create a .env next to docker-compose.yml (sample)
cat > .env <<'ENV'
# ---- Redis (DB 0 by default) ----
LOCKING_BACKEND=redis
REDIS_HOST=host.docker.internal
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# ---- MinIO (default backend) ----
STORAGE_TYPE=MINIO
STORAGE_REGION=eu-central-1
STORAGE_ENDPOINT_URL=http://host.docker.internal:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_BUCKET=supertable
STORAGE_FORCE_PATH_STYLE=true

# ---- App / DuckDB ----
SUPERTABLE_HOME=/data/supertable
LOG_LEVEL=INFO

SUPERTABLE_DUCKDB_PRESIGNED=1
SUPERTABLE_DUCKDB_THREADS=4
SUPERTABLE_DUCKDB_EXTERNAL_THREADS=2
SUPERTABLE_DUCKDB_HTTP_TIMEOUT=60
SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE=1

# ---- MCP (optional) ----
SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1
SUPERTABLE_ALLOWED_USER_HASHES=0b85b786b16d195439c0da18fd4478df

SUPERTABLE_TEST_ORG=kladna-soft
SUPERTABLE_TEST_SUPER=example
SUPERTABLE_TEST_USER_HASH=0b85b786b16d195439c0da18fd4478df
SUPERTABLE_TEST_QUERY=

# ---- Admin ----
SUPERTABLE_ADMIN_TOKEN=replace-me
ENV

# 3) Start Admin against HOST services (default)
docker compose --profile admin up -d
# Open http://localhost:8000  (login with SUPERTABLE_ADMIN_TOKEN)
```

> Linux note: this stack uses `host.docker.internal` (provided via `extra_hosts`) so containers can reach services on the host. Replace with your host IP if needed.

---

## Profiles & run matrix

**Profiles:**

- `admin` — run the Admin UI/API (port 8000)
- `mcp` — run the MCP server (stdio; no port)
- `infra` — optional Redis + MinIO inside the stack

**Defaults:** Admin/MCP point to **host** Redis/MinIO using `host.docker.internal`.  
To use in-cluster Redis/MinIO, include `infra` and override two envs.

### Host-backed (default)

```bash
# Admin only
docker compose --profile admin up -d --remove-orphans

# MCP only
docker compose --profile mcp up -d --remove-orphans

# Both
docker compose --profile admin --profile mcp up -d --remove-orphans
```

### Clean start with in-cluster infra

```bash
STORAGE_ENDPOINT_URL=http://minio:9000 REDIS_HOST=redis REDIS_DB=0 docker compose --profile infra --profile admin up -d --remove-orphans

# MCP:
STORAGE_ENDPOINT_URL=http://minio:9000 REDIS_HOST=redis REDIS_DB=0 docker compose --profile infra --profile mcp up -d --remove-orphans
```

> The in-cluster MinIO is published on host ports **9002 (S3 API)** and **9003 (console)**. Inside the network, always use `http://minio:9000`.

---

## Admin UI

- `/` → `/admin/login`
- `/admin` → overview of tenants / tables, etc.
- `/admin/config` → effective env values (secrets redacted)
- `/healthz` → `ok` when Redis is reachable

Auth uses a cookie set with `SUPERTABLE_ADMIN_TOKEN`.

---

## MCP server (stdio)

Wrapper commands in the image: `admin-server` and `mcp-server`.

Run interactively (stdio) via Compose:

```bash
docker compose --profile mcp up
# or
docker compose run --rm -i supertable-mcp mcp-server
```

Use `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1` and `SUPERTABLE_ALLOWED_USER_HASHES` to enforce access.

---

## Configuration

### Redis
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` (**0 recommended**), `REDIS_PASSWORD`.
- The app also accepts `SUPERTABLE_REDIS_URL` (e.g., `redis://host.docker.internal:6379/0`) if you prefer a single URL.

### MinIO (default)
- `STORAGE_TYPE=MINIO`
- `STORAGE_ENDPOINT_URL=http://minio:9000` (or your host/ELB)
- `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY`
- `STORAGE_BUCKET` (default: `supertable`)
- `STORAGE_FORCE_PATH_STYLE=true`
- `STORAGE_REGION` (optional)

### Amazon S3
- `STORAGE_TYPE=S3`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`
- `STORAGE_BUCKET`

### Azure Blob
- `STORAGE_TYPE=AZURE`
- `AZURE_STORAGE_CONNECTION_STRING` **or** MSI when running in Azure
- `SUPERTABLE_HOME` can be an `abfss://` path for certain flows

### GCP Storage
- `STORAGE_TYPE=GCP`
- `GOOGLE_APPLICATION_CREDENTIALS` (path) **or** inline `GCP_SA_JSON`
- `STORAGE_BUCKET`

### DuckDB tuning
- `SUPERTABLE_DUCKDB_*` variables enable presigned reads and tune concurrency.

### Security
- Set a strong `SUPERTABLE_ADMIN_TOKEN`.
- For MCP, enforce user hash if you need auditability (`SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1`).

---

## Environment reference

| Key | Default | Notes |
| --- | --- | --- |
| `LOCKING_BACKEND` | `redis` | Lock manager |
| `REDIS_HOST` | `host.docker.internal` | Set to `redis` when using `infra` |
| `REDIS_PORT` | `6379` |  |
| `REDIS_DB` | `0` | Recommended |
| `REDIS_PASSWORD` | _empty_ | Set if your Redis requires auth |
| `SUPERTABLE_REDIS_URL` | — | Optional URL style (`redis://host:6379/0`) |
| `STORAGE_TYPE` | `MINIO` | `MINIO` \| `S3` \| `AZURE` \| `GCP` \| `LOCAL` |
| `STORAGE_ENDPOINT_URL` | — | Required for MinIO/custom S3 endpoints |
| `STORAGE_ACCESS_KEY` / `STORAGE_SECRET_KEY` | — | For MinIO |
| `STORAGE_BUCKET` | `supertable` | Target bucket |
| `STORAGE_FORCE_PATH_STYLE` | `true` | Needed for MinIO-style endpoints |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_DEFAULT_REGION` | — | For `STORAGE_TYPE=S3` |
| `SUPERTABLE_HOME` | `/data/supertable` | Working dir/cache |
| `SUPERTABLE_ADMIN_TOKEN` | — | Required for Admin login |
| `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH` | `1` | Enforce MCP hash |
| `SUPERTABLE_ALLOWED_USER_HASHES` | — | Comma-separated allow list |

---

## Local development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run Admin
uvicorn supertable.admin:app --host 0.0.0.0 --port 8000

# Run MCP server (stdio)
python -u supertable/mcp_server.py
```

---

## Production deployment

### Docker Hub

Admin only:
```bash
docker run -d --name supertable-admin   -e LOCKING_BACKEND=redis   -e REDIS_HOST=your-redis -e REDIS_PORT=6379 -e REDIS_DB=0   -e STORAGE_TYPE=MINIO   -e STORAGE_ENDPOINT_URL=http://your-minio:9000   -e STORAGE_ACCESS_KEY=... -e STORAGE_SECRET_KEY=...   -e STORAGE_BUCKET=supertable -e STORAGE_FORCE_PATH_STYLE=true   -e SUPERTABLE_ADMIN_TOKEN=replace-me   -p 8000:8000   kladnasoft/supertable:latest
```

MCP (stdio):
```bash
docker run --rm -i --name supertable-mcp   -e LOCKING_BACKEND=redis   -e REDIS_HOST=your-redis -e REDIS_PORT=6379 -e REDIS_DB=0   -e STORAGE_TYPE=MINIO   -e STORAGE_ENDPOINT_URL=http://your-minio:9000   -e STORAGE_ACCESS_KEY=... -e STORAGE_SECRET_KEY=...   -e STORAGE_BUCKET=supertable -e STORAGE_FORCE_PATH_STYLE=true   kladnasoft/supertable:latest mcp-server
```

---

## FAQ

**Q: Do I need to pre-create the bucket?**  
A: With MinIO we attempt to ensure the bucket exists on first use.

**Q: Is the MCP server networked?**  
A: No — MCP uses **stdio**. Integrate with tools that spawn a process and connect via stdio.
