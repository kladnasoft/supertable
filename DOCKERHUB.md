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

```bash
# Using docker-compose profiles
docker compose --profile infra up -d
```

Or run separately:
```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin123! \
  minio/minio:latest server /data --console-address ":9001"
```

### 2. Create a `.env` file

```bash
# === Logging ===
LOG_LEVEL=INFO

# === Authentication ===
SUPERTABLE_AUTH_MODE=api_key
SUPERTABLE_API_KEY=change-me-super-secret
SUPERTABLE_AUTH_HEADER_NAME=X-API-Key

# === Storage (MinIO) ===
STORAGE_TYPE=MINIO
STORAGE_REGION=eu-central-1
STORAGE_ENDPOINT_URL=http://host.docker.internal:9000
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
  --env-file .env \
  -p 8050:8050 \
  kladnasoft/supertable:latest reflection-server

# REST API
docker run -d --name supertable-api \
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
  --env-file .env \
  -e SERVICE=reflection \
  -p 8050:8050 \
  kladnasoft/supertable:latest
```

Or using the convenience wrapper:
```bash
docker run -d --env-file .env -p 8050:8050 \
  kladnasoft/supertable:latest reflection-server
```

### REST API

```bash
docker run -d --env-file .env -p 8090:8090 \
  kladnasoft/supertable:latest api-server
```

### MCP (stdio)

For MCP hosts that spawn processes (Claude Desktop, IDEs):

```bash
docker run --rm -i --name supertable-mcp \
  --env-file .env \
  -e SUPERTABLE_MCP_TRANSPORT=stdio \
  kladnasoft/supertable:latest mcp-server
```

> **Important:** Use `-i` to keep stdin open for stdio transport.

### MCP (streamable-http)

For remote/HTTP MCP clients:

```bash
docker run -d --name supertable-mcp-http \
  --env-file .env \
  -e SUPERTABLE_MCP_TRANSPORT=streamable-http \
  -e SUPERTABLE_MCP_PORT=8070 \
  -e SUPERTABLE_MCP_HTTP_PATH=/mcp \
  -p 8070:8070 \
  kladnasoft/supertable:latest mcp-http-server
```

MCP endpoint: `http://localhost:8070/mcp`

### Notebook

```bash
docker run -d --env-file .env -p 8010:8010 \
  -v ./notebooks:/app/notebooks \
  kladnasoft/supertable:latest notebook-server
```

### Spark

```bash
docker run -d --env-file .env -p 8010:8010 \
  kladnasoft/supertable:latest spark-server
```

---

## Docker Compose

Use profiles to start specific services:

```bash
# Infrastructure only (Redis + MinIO)
docker compose --profile infra up -d

# Reflection UI
docker compose --profile reflection up -d

# REST API
docker compose --profile api up -d

# MCP (stdio + web tester)
docker compose --profile mcp up -d

# MCP HTTP
docker compose --profile mcp-http up -d

# Notebook
docker compose --profile notebook up -d

# Multiple profiles
docker compose --profile infra --profile reflection --profile api up -d
```

---

## Storage Backends

### MinIO (default)

```bash
STORAGE_TYPE=MINIO
STORAGE_ENDPOINT_URL=http://host.docker.internal:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_BUCKET=supertable
STORAGE_FORCE_PATH_STYLE=true
```

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
SUPERTABLE_REDIS_SENTINELS=127.0.0.1:26379,127.0.0.1:26380,127.0.0.1:26381
SUPERTABLE_REDIS_SENTINEL_MASTER=mymaster
SUPERTABLE_REDIS_SENTINEL_PASSWORD=your-sentinel-password
SUPERTABLE_REDIS_SENTINEL_STRICT=true
```

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
docker run -d --env-file .env \
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
   SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1
   SUPERTABLE_ALLOWED_USER_HASHES=hash1,hash2
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

### Cannot connect to host services

On Linux, use `host.docker.internal` or your host IP:
```bash
-e STORAGE_ENDPOINT_URL=http://172.17.0.1:9000
-e REDIS_HOST=172.17.0.1
```

Or add:
```bash
--add-host=host.docker.internal:host-gateway
```

### Permission denied errors

Ensure mounted volumes are writable by uid 1001:
```bash
sudo chown -R 1001:1001 ./notebooks
```