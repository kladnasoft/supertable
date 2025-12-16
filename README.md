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
- [Reflection UI](#reflection-ui)
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
- **Out-of-the-box Reflection UI** (FastAPI + Jinja2) on port **8000**.
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

# 3) Start Reflection UI against HOST services (default)
docker compose --profile admin up -d
# Open http://localhost:8000  (redirects to /reflection/login)
```

> Linux note: this stack uses `host.docker.internal` (provided via `extra_hosts`) so containers can reach services on the host. Replace with your host IP if needed.

---

## Profiles & run matrix

**Profiles:**

- `admin` — run the Reflection UI (port 8000)
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

## Reflection UI

**Reflection** is SuperTable’s built‑in browser UI (FastAPI + Jinja2 templates) for operating a SuperTable instance:

- browse **SuperTables** and their tables/files (“leaves”)
- inspect metadata and basic stats
- manage **users/roles/tokens** (for UI/API access)
- run **read‑only SQL** in the in‑browser editor (with syntax highlighting + autocomplete)

### Routes

- `/` → redirects to `/reflection/login`
- `/reflection/login` → sign‑in page
- `/reflection/admin` → main dashboard (tenants, mirrors, users/roles/tokens, config)
- `/reflection/tables` → table/leaf browser
- `/reflection/execute` → SQL editor + results
- `/reflection/admin/config` → effective env values (secrets redacted)
- `/healthz` → `ok` when Redis is reachable

### Authentication model

Reflection supports two login modes (controlled by `SUPERTABLE_LOGIN_MASK`):

- **Superuser login** (recommended for administration)  
  Uses `SUPERTABLE_SUPERTOKEN` (or the legacy fallback `SUPERTABLE_ADMIN_TOKEN`).  
  On success it sets two cookies: `st_admin_token` (superuser token) and `st_session` (signed session).

- **Regular user login** (least privilege)  
  Uses **Username + Access Token** (tokens are created by a superuser in Reflection).  
  On success it sets only `st_session`.

### Important environment variables

- `SUPERTABLE_ORGANIZATION` — default organization used by the UI.
- `SUPERTABLE_SUPERTOKEN` — superuser token for privileged admin actions (preferred).
- `SUPERTABLE_ADMIN_TOKEN` — legacy fallback for `SUPERTABLE_SUPERTOKEN` (kept for compatibility).
- `SUPERTABLE_SUPERHASH` — user hash for the superuser identity (must exist in the underlying user registry).
- `SUPERTABLE_LOGIN_MASK` — `1` superuser only, `2` regular users only, `3` both.
- `SUPERTABLE_SESSION_SECRET` — **required in production** (signs/encrypts the `st_session` cookie).
- `SECURE_COOKIES=1` — set when serving Reflection over HTTPS.

### Production hardening (recommended)

- Put Reflection behind TLS (HTTPS) and set `SECURE_COOKIES=1`.
- Set a strong `SUPERTABLE_SESSION_SECRET` and rotate `SUPERTABLE_SUPERTOKEN` periodically.
- Prefer regular-user tokens for day‑to‑day usage; reserve the superuser token for administration.
- If exposed outside a trusted network: add IP allowlisting / VPN and consider an upstream auth proxy.

---

## MCP server (stdio)

SuperTable ships an **MCP (Model Context Protocol) server** that exposes your SuperTables as **read‑only tools** for LLM clients (Claude Desktop, Claude Code, etc.). Under the hood it speaks **JSON‑RPC 2.0** and can run in two ways:

- **stdio (local)**: the client spawns the server process and talks over stdin/stdout (newline‑delimited JSON).
- **streamable-http (remote)**: the server runs as an HTTP endpoint (POST JSON‑RPC; SSE for streams) for “Remote MCP servers”.

### What tools does SuperTable expose?

These are the MCP tools implemented by `supertable/mcp/mcp_server.py`:

- `health` / `info` (service status + policy flags)
- `whoami` (validates/normalizes `user_hash`)
- `list_supers(organization)`
- `list_tables(super_name, organization, user_hash, auth_token)`
- `describe_table(super_name, organization, table, user_hash, auth_token)`
- `get_table_stats(super_name, organization, table, user_hash, auth_token)`
- `get_super_meta(super_name, organization, user_hash, auth_token)`
- `query_sql(super_name, organization, sql, limit, engine, query_timeout_sec, user_hash, auth_token)`

**Safety defaults (important):**
- **Read‑only SQL enforcement**: only `SELECT` / `WITH … SELECT` are allowed; write/DDL keywords are rejected.
- **Concurrency limiting + per‑request timeout** to keep the server stable under parallel calls.
- Optional **shared secret token** (`auth_token`) plus optional **explicit user hash** enforcement for auditability/tenant isolation.

### Run as a local MCP server (stdio)

Wrapper commands in the image: `admin-server` and `mcp-server`.

Run interactively (stdio) via Compose:

```bash
docker compose --profile mcp up
# or
docker compose run --rm -i supertable-mcp mcp-server
```

If you run it directly (dev mode):

```bash
python -u supertable/mcp/mcp_server.py
```

### Run as a remote MCP server (streamable-http)

Set the transport to **streamable-http** and start the server (it will run an ASGI app via Uvicorn):

```bash
export SUPERTABLE_MCP_TRANSPORT=streamable-http
export SUPERTABLE_MCP_HTTP_HOST=0.0.0.0
export SUPERTABLE_MCP_HTTP_PORT=8000
export SUPERTABLE_MCP_HTTP_PATH=/mcp

python -u supertable/mcp/mcp_server.py
# Listening on http://0.0.0.0:8000/mcp
```

> **HTTP transport tip:** In MCP “streamable HTTP”, every JSON‑RPC message is a new **HTTP POST** to the MCP endpoint.

### Use with Claude Desktop

Claude Desktop supports **two** integration modes:

1) **Local MCP servers (stdio)** — configured in `claude_desktop_config.json` (Claude Desktop “Developer” settings).  
2) **Remote MCP servers (streamable-http)** — added via **Settings → Connectors → Add Custom Connector** (not by editing the JSON config directly).

#### Option A — Local (stdio) via config

Typical Claude Desktop config (paths vary by OS; Claude opens it for you via **Settings → Developer → Edit Config**):

```json
{
  "mcpServers": {
    "supertable": {
      "command": "python",
      "args": ["-u", "/absolute/path/to/supertable/mcp/mcp_server.py"],
      "env": {
        "SUPERTABLE_MCP_TRANSPORT": "stdio",
        "SUPERTABLE_REQUIRE_TOKEN": "1",
        "SUPERTABLE_MCP_AUTH_TOKEN": "replace-with-strong-token",
        "SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH": "1",
        "SUPERTABLE_ALLOWED_USER_HASHES": "0b85b786b16d195439c0da18fd4478df",
        "SUPERTABLE_ORGANIZATION": "kladna-soft"
      }
    }
  }
}
```

Notes:
- If `SUPERTABLE_REQUIRE_TOKEN=1`, Claude must include `auth_token` in tool calls. The server enforces it.
- If `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1`, each request must include `user_hash` (32/64 hex). This is recommended for multi-tenant use.

#### Option B — Remote (streamable-http) via Connectors

1. Run the server with `SUPERTABLE_MCP_TRANSPORT=streamable-http`.
2. In Claude Desktop: **Settings → Connectors → Add Custom Connector**.
3. Use the URL:

```text
http://<host>:8000/mcp
```

If you expose it on the internet:
- put it behind TLS (HTTPS),
- lock it down with an allowlist / private network,
- keep `SUPERTABLE_REQUIRE_TOKEN=1` and rotate `SUPERTABLE_MCP_AUTH_TOKEN`.

### Web tester (optional)

For quick manual testing, there is a minimal FastAPI UI (`supertable/mcp/web_app.py`) that spawns the MCP server as a stdio subprocess and exposes an authenticated browser UI + HTTP JSON‑RPC gateway.

Run it with:

```bash
python -u supertable/mcp/web_server.py
# default: http://0.0.0.0:8099/?auth=<SUPERTABLE_SUPERTOKEN>
```

### Example `.env` (as used in this project)

Below is a complete example `.env` you can use as a starting point:

```dotenv
LOG_LEVEL=INFO

# STORAGE
STORAGE_TYPE=MINIO
STORAGE_REGION=eu-central-1
STORAGE_ENDPOINT_URL=http://localhost:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_BUCKET=supertable
STORAGE_FORCE_PATH_STYLE=true

# === Redis auth ===
LOCKING_BACKEND=redis
#SUPERTABLE_REDIS_URL=redis://localhost:6379/1
#SUPERTABLE_REDIS_HOST=localhost
#SUPERTABLE_REDIS_PORT=6379
SUPERTABLE_REDIS_DB=1
SUPERTABLE_REDIS_PASSWORD=change_me_to_a_strong_password
SUPERTABLE_REDIS_DECODE_RESPONSES=true

# SENTINEL
SUPERTABLE_REDIS_SENTINEL=true
SUPERTABLE_REDIS_SENTINELS=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381
SUPERTABLE_REDIS_SENTINEL_MASTER=mymaster

# ENGINE
SUPERTABLE_DUCKDB_PRESIGNED=1
SUPERTABLE_DUCKDB_THREADS=4
SUPERTABLE_DUCKDB_EXTERNAL_THREADS=2
SUPERTABLE_DUCKDB_HTTP_TIMEOUT=60
SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE=1

# REFLECTION
SUPERTABLE_HOME=~/supertable
SUPERTABLE_ORGANIZATION=kladna-soft
SUPERTABLE_SUPERTOKEN=token
SUPERTABLE_SUPERHASH=0b85b786b16d195439c0da18fd4478df
SUPERTABLE_LOGIN_MASK=1

# MCP
SUPERTABLE_MCP_TRANSPORT='streamable-http'
SUPERTABLE_MCP_HTTP_HOST='0.0.0.0'
SUPERTABLE_MCP_HTTP_PORT=8000
SUPERTABLE_MCP_HTTP_PATH=/mcp
SUPERTABLE_REQUIRE_TOKEN=0
SUPERTABLE_MCP_AUTH_TOKEN=some-strong-random-string
SUPERTABLE_TEST_QUERY="select * from facts"
```

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

# Run Reflection UI
uvicorn supertable.admin:app --host 0.0.0.0 --port 8000

# Run MCP server (stdio)
python -u supertable/mcp_server.py
```

---

## Production deployment

### Docker Hub

Reflection UI only:
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
