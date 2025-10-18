# SuperTable

![Python](https://img.shields.io/badge/python-3.10%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

**SuperTable — The simplest data warehouse & cataloging system.**  
A high-performance, lightweight transaction catalog that **now defaults to Redis (catalog/locks) + MinIO (object storage)**.  
It automatically creates and manages tables so you can start running SQL immediately—no complicated schemas or manual joins.

---

## Contents

- [What’s new](#whats-new)
- [Architecture](#architecture)
- [Quick start (Docker Compose)](#quick-start-docker-compose)
- [Admin UI](#admin-ui)
- [MCP server & client](#mcp-server--client)
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
- **Out-of-the-box Admin UI** (FastAPI + Jinja2) for inspecting tenants, tables, users, roles and mirror settings.
- **MCP stdio server** (`mcp_server.py`) and a robust local **MCP client** (`mcp_client.py`) for testing your tools.
- Production-ready Docker image and docker-compose stack (Redis + MinIO + Admin + MCP utility container).

---

## Architecture

- **Catalog & Locks:** Redis stores SuperTable metadata & locks (`supertable:{org}:{super}:meta:*`).
- **Data files:** Object storage (MinIO/S3/Azure/GCS). MinIO is the default and ships with Compose.
- **Query:** DuckDB (embedded) with S3-style httpfs; optional presigned reads.
- **Mirrors:** Delta/Iceberg “latest-only” writers when enabled (see Admin mirrors box).

---

## Quick start (Docker Compose)

Requirements: Docker & docker-compose.

```bash
# 1) Clone and build
git clone https://github.com/kladnasoft/supertable.git
cd supertable

# 2) (Optional) Create a .env next to docker-compose.yml (sample below)
cat > .env <<'ENV'
LOCKING_BACKEND=redis
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_DB=0

AWS_S3_ENDPOINT_URL=http://minio:9000
AWS_S3_FORCE_PATH_STYLE=true
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin123!
SUPERTABLE_BUCKET=supertable

STORAGE_TYPE=MINIO
SUPERTABLE_HOME=/data/supertable
LOG_LEVEL=INFO

SUPERTABLE_DUCKDB_PRESIGNED=1
SUPERTABLE_DUCKDB_THREADS=4
SUPERTABLE_DUCKDB_EXTERNAL_THREADS=2
SUPERTABLE_DUCKDB_HTTP_TIMEOUT=60
SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE=1
SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1
SUPERTABLE_ALLOWED_USER_HASHES=0b85b786b16d195439c0da18fd4478df

MCP_SERVER_PATH=/app/mcp_server.py
SUPERTABLE_TEST_ORG=kladna-soft
SUPERTABLE_TEST_SUPER=example
SUPERTABLE_TEST_USER_HASH=0b85b786b16d195439c0da18fd4478df
SUPERTABLE_TEST_QUERY=
SUPERTABLE_ADMIN_TOKEN=change-me-now
ENV

# 3) Start services
docker compose up -d

# 4) Open the Admin UI
# http://localhost:8000  (login with SUPERTABLE_ADMIN_TOKEN)
```

MinIO console is available at **http://localhost:9001** (user/pass from `AWS_ACCESS_KEY_ID/SECRET`).

---

## Admin UI

- `/` redirects to `/admin/login`.
- `/admin` lists tenants discovered in Redis, root/meta, tables, users, roles.
- `/admin/config` shows effective env & .env values (sensitive values redacted).
- `/healthz` returns `ok` when Redis is reachable.

> **Auth:** the UI uses a cookie with `SUPERTABLE_ADMIN_TOKEN`. Set it via env or `.env`.

---

## MCP server & client

The MCP server runs over **stdio**. Use the **mcp** utility container:

```bash
# List tools and run a safe sample query (falls back if SQL has no table)
docker compose run --rm mcp mcp-client --org kladna-soft --super example

# Or run the raw MCP server (blocks, waiting on stdio):
docker compose run --rm -i mcp mcp-server
```

Typical integration (VS Code / Claude Desktop / OpenAI MCP):
- Configure a “command provider” that invokes:
  ```
  docker run --rm -i     --env-file <your .env>     --network host     kladnasoft/supertable:latest mcp-server
  ```

---

## Configuration

### Redis
- Set `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, and `REDIS_PASSWORD` if needed.
- The Admin & MCP pieces discover tenants under keys like `supertable:<org>:<super>:meta:*`.

### MinIO (default)
- `STORAGE_TYPE=MINIO`
- `AWS_S3_ENDPOINT_URL=http://minio:9000`, `AWS_S3_FORCE_PATH_STYLE=true`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `SUPERTABLE_BUCKET` (default: `supertable`)

> The MinIO backend will **ensure the bucket exists** automatically.

### Amazon S3
- `STORAGE_TYPE=S3`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`

### Azure Blob
- `STORAGE_TYPE=AZURE`
- Either `AZURE_STORAGE_CONNECTION_STRING` or managed identity (when running in Azure/Synapse).
- `SUPERTABLE_HOME` can point to `abfss://container@account.dfs.core.windows.net/prefix`.

### GCP Storage
- `STORAGE_TYPE=GCP`
- `GCP_PROJECT` + `GOOGLE_APPLICATION_CREDENTIALS` (path) **or** inline `GCP_SA_JSON`.

### DuckDB tuning
- `SUPERTABLE_DUCKDB_*` variables enable presigned reads & thread counts.

### Security

- Set a strong `SUPERTABLE_ADMIN_TOKEN`.
- If `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1`, the MCP server will require a `user_hash`.
- You can whitelist hashes via `SUPERTABLE_ALLOWED_USER_HASHES` (comma-separated).

---

## Environment reference

| Key | Default | Notes |
| --- | --- | --- |
| `LOCKING_BACKEND` | `redis` | Lock manager (redis/file) |
| `STORAGE_TYPE` | `MINIO` | `LOCAL` \| `MINIO` \| `S3` \| `AZURE` \| `GCP` |
| `SUPERTABLE_HOME` | `/data/supertable` | Local root for `LOCAL` (still used for temp/derived) |
| `REDIS_*` | — | Host/Port/DB/Password |
| `AWS_*` | — | MinIO/S3 credentials & endpoint |
| `SUPERTABLE_BUCKET` | `supertable` | Target bucket |
| `SUPERTABLE_ADMIN_TOKEN` | — | Required for Admin UI login |
| `SUPERTABLE_DUCKDB_*` | — | Query performance/env knobs |
| `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH` | `1` | Enforce user hash |
| `SUPERTABLE_ALLOWED_USER_HASHES` | — | Comma-separated allow-list for quick demos |

---

## Local development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run Admin
uvicorn admin:app --host 0.0.0.0 --port 8000

# Run MCP server
python -u mcp_server.py

# Test client
python -u mcp_client.py --org kladna-soft --super example
```

---

## Production deployment

### Docker Hub

Pull the image and run just the Admin API:

```bash
docker pull kladnasoft/supertable:latest

docker run -d --name supertable-admin   -e STORAGE_TYPE=MINIO   -e LOCKING_BACKEND=redis   -e REDIS_HOST=your-redis   -e REDIS_PORT=6379   -e AWS_S3_ENDPOINT_URL=http://your-minio:9000   -e AWS_S3_FORCE_PATH_STYLE=true   -e AWS_ACCESS_KEY_ID=...   -e AWS_SECRET_ACCESS_KEY=...   -e SUPERTABLE_BUCKET=supertable   -e SUPERTABLE_ADMIN_TOKEN=replace-me   -p 8000:8000   kladnasoft/supertable:latest
```

Run the MCP server (stdio):

```bash
# Note: -i is important to provide stdio
docker run --rm -i   --env STORAGE_TYPE=MINIO   --env LOCKING_BACKEND=redis   --env REDIS_HOST=your-redis   --env AWS_S3_ENDPOINT_URL=http://your-minio:9000   --env AWS_S3_FORCE_PATH_STYLE=true   --env AWS_ACCESS_KEY_ID=...   --env AWS_SECRET_ACCESS_KEY=...   kladnasoft/supertable:latest mcp-server
```

---

## FAQ

**Q: Do I need buckets or schemas pre-created?**  
A: No—MinIO/S3 bucket creation is handled on first use. Redis keys are written lazily.

**Q: Is the MCP server networked?**  
A: No. It speaks **stdio**. Use `docker run -i … mcp-server` with tools that spawn a process.

**Q: Where are Delta/Iceberg mirrors written?**  
A: Under `<org>/<super>/delta/<table>` and `<org>/<super>/iceberg/<table>` within your object storage when enabled.
