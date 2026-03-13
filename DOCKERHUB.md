# SuperTable – Docker Image

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

This image bundles multiple services:

| Service | Command | Default Port | Description |
|---------|---------|--------------|-------------|
| **Reflection** | `reflection-server` | 8050 | Admin UI + REST API |
| **API** | `api-server` | 8090 | Supertable REST API |
| **MCP** | `mcp-server` | 8099 (web UI) | MCP stdio + web tester |
| **MCP-HTTP** | `mcp-http-server` | 8070 | MCP over streamable-http |
| **Notebook** | `notebook-server` | 8010 | Notebook WebSocket server |
| **Spark** | `spark-server` | 8010 | Spark plug WebSocket server |

> **Runtime:** Redis (catalog/locks) + MinIO/S3/Azure/GCS object backends via DuckDB httpfs

---

## Quick Start

### 1. Start infrastructure (Redis + MinIO)

Infrastructure runs as separate compose stacks. Start them first:

```bash
# Redis (master + replica)
cd infrastructure/redis && docker compose up -d

# MinIO (2-node cluster)
cd infrastructure/minio && docker compose up -d
```

All services communicate via the shared `supertable-net` Docker network, which is auto-created by whichever compose file starts first.

### 2. Create a `.env` file

```bash
# === Logging ===
LOG_LEVEL=INFO

# === Authentication ===
SUPERTABLE_AUTH_MODE=api_key
SUPERTABLE_API_KEY=change-me-super-secret
SUPERTABLE_AUTH_HEADER_NAME=X-API-Key

# === Storage (MinIO — uses Docker hostname on supertable-net) ===
STORAGE_TYPE=MINIO
STORAGE_REGION=eu-central-1
STORAGE_ENDPOINT_URL=http://minio:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_BUCKET=supertable
STORAGE_FORCE_PATH_STYLE=true

# === Redis ===
LOCKING_BACKEND=redis
SUPERTABLE_REDIS_DB=1
SUPERTABLE_REDIS_DECODE_RESPONSES=true

# === Reflection (Admin UI) ===
SUPERTABLE_REFLECTION_PORT=8050
SUPERTABLE_HOME=~/supertable
SUPERTABLE_ORGANIZATION=my-org
SUPERTABLE_SUPERTOKEN=change-me-strong-token
SUPERTABLE_LOGIN_MASK=1

# === API ===
SUPERTABLE_API_PORT=8090

# === Notebook ===
SUPERTABLE_NOTEBOOK_PORT=8010

# === MCP ===
SUPERTABLE_MCP_TRANSPORT=streamable-http
SUPERTABLE_MCP_PORT=8070
SUPERTABLE_MCP_HTTP_PATH=/mcp
SUPERTABLE_REQUIRE_TOKEN=0
SUPERTABLE_MCP_AUTH_TOKEN=some-strong-random-string

# === DuckDB Engine ===
SUPERTABLE_DUCKDB_PRESIGNED=1
SUPERTABLE_DUCKDB_THREADS=4
SUPERTABLE_DUCKDB_EXTERNAL_THREADS=2
SUPERTABLE_DUCKDB_HTTP_TIMEOUT=60

# === Vault (optional) ===
# SUPERTABLE_VAULT_MASTER_KEY=your-master-key
# SUPERTABLE_VAULT_FERNET_KEY=your-fernet-key
```

### 3. Run a service

```bash
# Reflection UI (Admin)
docker run -d --name supertable-reflection \
  --network supertable-net \
  --env-file .env \
  -p 8050:8050 \
  kladnasoft/supertable:latest reflection-server

# REST API
docker run -d --name supertable-api \
  --network supertable-net \
  --env-file .env \
  -p 8090:8090 \
  kladnasoft/supertable:latest api-server
```

Open **http://localhost:8050** for the Reflection UI.

---

## Services

### Reflection (Admin UI)

```bash
docker run -d --name supertable-reflection \
  --network supertable-net \
  --env-file .env \
  -e SERVICE=reflection \
  -p 8050:8050 \
  kladnasoft/supertable:latest
```

Or using the convenience wrapper:
```bash
docker run -d --network supertable-net --env-file .env -p 8050:8050 \
  kladnasoft/supertable:latest reflection-server
```

### REST API

```bash
docker run -d --network supertable-net --env-file .env -p 8090:8090 \
  kladnasoft/supertable:latest api-server
```

### MCP (stdio)

For MCP hosts that spawn processes (Claude Desktop, IDEs):

```bash
docker run --rm -i --name supertable-mcp \
  --network supertable-net \
  --env-file .env \
  -e SUPERTABLE_MCP_TRANSPORT=stdio \
  kladnasoft/supertable:latest mcp-server
```

> **Important:** Use `-i` to keep stdin open for stdio transport.

### MCP (streamable-http)

For remote/HTTP MCP clients:

```bash
docker run -d --name supertable-mcp-http \
  --network supertable-net \
  --env-file .env \
  -e SUPERTABLE_MCP_TRANSPORT=streamable-http \
  -e SUPERTABLE_MCP_PORT=8070 \
  -e SUPERTABLE_MCP_HTTP_PATH=/mcp \
  -p 8070:8070 \
  kladnasoft/supertable:latest mcp-http-server
```

MCP endpoint: `http://localhost:8070/mcp`

For the single-profile web app deployment, `docker compose --profile mcp --profile https up -d`
exposes `https://localhost:8470/mcp`, `https://localhost:8470/web`, and
`https://localhost:8470/simulation` from the same HTTPS origin.

### Notebook

```bash
docker run -d --network supertable-net --env-file .env -p 8010:8010 \
  -v ./notebooks:/app/notebooks \
  kladnasoft/supertable:latest notebook-server
```

### Spark

```bash
docker run -d --network supertable-net --env-file .env -p 8010:8010 \
  kladnasoft/supertable:latest spark-server
```

---

## Docker Compose

Start infrastructure first, then use profiles for application services:

```bash
# Infrastructure (separate compose files)
cd infrastructure/redis && docker compose up -d
cd infrastructure/minio && docker compose up -d

# Application services (from project root)
docker compose --profile reflection up -d
docker compose --profile api up -d
docker compose --profile mcp up -d
# Recommended remote HTTPS deployment
# docker compose --profile mcp --profile https up -d
docker compose --profile mcp-http up -d
docker compose --profile notebook up -d

# Multiple profiles
docker compose --profile reflection --profile api up -d

# Add HTTPS to any service
docker compose --profile reflection --profile https up -d
```

---

## Storage Backends

### MinIO (default)

```bash
STORAGE_TYPE=MINIO
STORAGE_ENDPOINT_URL=http://minio:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_BUCKET=supertable
STORAGE_FORCE_PATH_STYLE=true
```

> Inside Docker, use the service hostname `minio`. From the host, use `http://localhost:9000`.

### AWS S3

```bash
STORAGE_TYPE=S3
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=eu-central-1
STORAGE_BUCKET=my-s3-bucket
```

### S3-Compatible (Backblaze, Wasabi, etc.)

```bash
STORAGE_TYPE=MINIO
STORAGE_ENDPOINT_URL=https://s3.us-west-001.backblazeb2.com
STORAGE_ACCESS_KEY=your-key
STORAGE_SECRET_KEY=your-secret
STORAGE_BUCKET=my-bucket
STORAGE_FORCE_PATH_STYLE=true
```

---

## Redis Configuration

### Standalone Redis

```bash
LOCKING_BACKEND=redis
SUPERTABLE_REDIS_DB=1
# Optional: if not using default localhost:6379
# SUPERTABLE_REDIS_URL=redis://myredis:6379/1
```

### Redis Sentinel (HA)

```bash
LOCKING_BACKEND=redis
SUPERTABLE_REDIS_SENTINEL=true
SUPERTABLE_REDIS_SENTINELS=redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379
SUPERTABLE_REDIS_SENTINEL_MASTER=mymaster
SUPERTABLE_REDIS_SENTINEL_PASSWORD=your-sentinel-password
SUPERTABLE_REDIS_SENTINEL_STRICT=true
```

> Start Redis with the sentinel profile: `cd infrastructure/redis && docker compose --profile sentinel up -d`

---

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE` | `reflection` | Service to run: `reflection`, `api`, `mcp`, `mcp-http`, `notebook`, `spark` |
| `LOG_LEVEL` | `INFO` | Logging level |
| **Auth** |||
| `SUPERTABLE_AUTH_MODE` | `api_key` | Auth mode: `api_key` or `bearer` |
| `SUPERTABLE_API_KEY` | - | API key for authentication |
| `SUPERTABLE_SUPERTOKEN` | - | Admin superuser token |
| **Storage** |||
| `STORAGE_TYPE` | `MINIO` | Storage backend: `MINIO`, `S3`, `AZURE`, `GCS` |
| `STORAGE_ENDPOINT_URL` | - | S3/MinIO endpoint URL |
| `STORAGE_BUCKET` | `supertable` | Storage bucket name |
| `STORAGE_ACCESS_KEY` | - | Access key |
| `STORAGE_SECRET_KEY` | - | Secret key |
| **Redis** |||
| `LOCKING_BACKEND` | `redis` | Locking backend |
| `SUPERTABLE_REDIS_DB` | `0` | Redis database number |
| `SUPERTABLE_REDIS_URL` | - | Full Redis URL (overrides host/port) |
| **MCP** |||
| `SUPERTABLE_MCP_TRANSPORT` | `stdio` | Transport: `stdio` or `streamable-http` |
| `SUPERTABLE_MCP_PORT` | `8070` | HTTP transport port |
| `SUPERTABLE_MCP_HTTP_PATH` | `/mcp` | HTTP endpoint path |
| `SUPERTABLE_MCP_AUTH_TOKEN` | - | MCP authentication token |
| `SUPERTABLE_REQUIRE_TOKEN` | `1` | Require auth token for MCP |
| **Ports** |||
| `SUPERTABLE_REFLECTION_PORT` | `8050` | Reflection UI port |
| `SUPERTABLE_API_PORT` | `8090` | REST API port |
| `SUPERTABLE_NOTEBOOK_PORT` | `8010` | Notebook server port |

---

## Volumes

| Path | Description |
|------|-------------|
| `/app/notebooks` | Notebook files (mount for persistence) |
| `/app/.env` | Environment file (mount read-only) |
| `/home/supertable/.duckdb/extensions` | DuckDB extensions (pre-baked with httpfs) |

Example:
```bash
docker run -d --network supertable-net --env-file .env \
  -v $PWD/notebooks:/app/notebooks \
  -v $PWD/.env:/app/.env:ro \
  -p 8050:8050 \
  kladnasoft/supertable:latest reflection-server
```

---

## Health Checks

All HTTP services expose health endpoints:

| Service | Health Endpoint |
|---------|-----------------|
| Reflection | `GET /health` on port 8050 |
| API | `GET /health` on port 8090 |
| MCP Web | `GET /health` on port 8099 |
| MCP HTTP | `GET /health` on port 8070 |
| Notebook | `GET /health` on port 8010 |

---

## Security Best Practices

1. **Always set strong tokens:**
   ```bash
   SUPERTABLE_SUPERTOKEN=<random-64-char-string>
   SUPERTABLE_API_KEY=<random-64-char-string>
   SUPERTABLE_MCP_AUTH_TOKEN=<random-64-char-string>
   ```

2. **Enable MCP authentication:**
   ```bash
   SUPERTABLE_REQUIRE_TOKEN=1
   SUPERTABLE_REQUIRE_EXPLICIT_ROLE=1
   SUPERTABLE_ALLOWED_ROLES=reader,writer
   ```

3. **Use Redis password in production:**
   ```bash
   SUPERTABLE_REDIS_PASSWORD=<strong-password>
   ```

4. **Run as non-root:** The container runs as user `supertable` (uid 1001) by default.

---

## Troubleshooting

### Container exits immediately

Check logs:
```bash
docker logs supertable-reflection
```

Common causes:
- Missing required env vars (`SUPERTABLE_ORGANIZATION`, `SUPERTABLE_SUPERTOKEN`)
- Redis not reachable
- MinIO/S3 not reachable

### Cannot connect to infrastructure

Ensure infrastructure is running and on `supertable-net`:
```bash
docker network inspect supertable-net
```

If running services outside of compose (standalone `docker run`), add `--network supertable-net` so the container can resolve `redis-master` and `minio`.

For host-mode development without Docker networking:
```bash
-e STORAGE_ENDPOINT_URL=http://host.docker.internal:9000
-e REDIS_HOST=host.docker.internal
--add-host=host.docker.internal:host-gateway
```

### Permission denied errors

Ensure mounted volumes are writable by uid 1001:
```bash
sudo chown -R 1001:1001 ./notebooks
```