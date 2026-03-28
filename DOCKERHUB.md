# SuperTable – Docker Image

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

This image bundles multiple services controlled by the `SERVICE` env var or convenience commands:

| Service | Command | Default Port | Description |
|---------|---------|--------------|-------------|
| **Reflection** | `reflection-server` | 8050 | Admin UI + REST API |
| **API** | `api-server` | 8090 | Supertable REST API |
| **MCP** | `mcp-server` | 8099 (web) + 8070 (HTTP) | MCP stdio + streamable-http + web tester |
| **MCP-HTTP** | `mcp-http-server` | 8070 | Dedicated MCP over streamable-http |
| **Notebook** | `notebook-server` | 8000 | Notebook WebSocket server |
| **Spark** | `spark-server` | 8010 | Spark plug WebSocket server |

> **Runtime:** Redis (catalog/locks) + MinIO/S3/Azure/GCS object backends via DuckDB httpfs

---

## Quick Start

### 1. Start infrastructure (Redis + MinIO)

Infrastructure runs as separate compose stacks:

```bash
cd infrastructure/redis && docker compose up -d
cd infrastructure/minio && docker compose up -d
```

All services communicate via the shared `supertable-net` Docker network.

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
STORAGE_ENDPOINT_URL=http://minio:9000
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_BUCKET=supertable
STORAGE_FORCE_PATH_STYLE=true

# === Redis ===
LOCKING_BACKEND=redis
SUPERTABLE_REDIS_HOST=redis-master
SUPERTABLE_REDIS_PORT=6379
SUPERTABLE_REDIS_PASSWORD=change-me-strong-password
SUPERTABLE_REDIS_DB=1

# === Core ===
SUPERTABLE_HOME=~/supertable
SUPERTABLE_ORGANIZATION=my-org
SUPERTABLE_SUPERUSER_TOKEN=change-me-strong-token
SUPERTABLE_LOGIN_MASK=1

# === API ===
SUPERTABLE_API_PORT=8090

# === Reflection (Admin UI) ===
SUPERTABLE_REFLECTION_PORT=8050

# === MCP ===
SUPERTABLE_MCP_TRANSPORT=streamable-http
SUPERTABLE_MCP_PORT=8070
SUPERTABLE_MCP_TOKEN=some-strong-random-string

# === DuckDB Engine ===
SUPERTABLE_DUCKDB_PRESIGNED=1
SUPERTABLE_DUCKDB_THREADS=4
SUPERTABLE_DUCKDB_EXTERNAL_THREADS=2
SUPERTABLE_DUCKDB_HTTP_TIMEOUT=60
```

### 3. Run with Docker Compose (recommended)

Use profiles to start individual services:

```bash
# Admin UI
docker compose --profile reflection up -d

# REST API
docker compose --profile api up -d

# MCP server (stdio + streamable-http + web tester)
docker compose --profile mcp up -d

# Multiple services at once
docker compose --profile reflection --profile api --profile mcp up -d

# Add HTTPS to any profile
docker compose --profile mcp --profile https up -d
```

### 4. Or run standalone containers

```bash
# Reflection UI
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

---

## Docker Compose Profiles

| Profile | Service | Ports | Description |
|---------|---------|-------|-------------|
| `reflection` | `supertable-reflection` | 8050 | Admin UI + REST API |
| `api` | `supertable-api` | 8090 | REST API |
| `mcp` | `supertable-mcp` | 8099, 8070 | MCP stdio + streamable-http + web tester |
| `mcp-http` | `supertable-mcp-http` + `caddy-mcp` | 8070 (HTTPS) | Dedicated MCP HTTP with TLS via Caddy |
| `notebook` | `supertable-notebook` | 8000 | Notebook WebSocket server |
| `spark` | `supertable-spark` | 8010 | Spark plug WebSocket server |
| `https` | `caddy-https` | 8443, 8470, 8490, 9443 | Universal TLS sidecar (combinable with any profile) |

### HTTPS Ports (when using `--profile https`)

| Port | Routes to | URL |
|------|-----------|-----|
| 8443 | Reflection :8050 | `https://host:8443` |
| 8470 | MCP + web + simulation | `https://host:8470/mcp`, `/web`, `/simulation` |
| 8490 | API :8090 | `https://host:8490` |
| 9443 | MinIO S3 :9000 | `https://host:9443` |

One-time CA trust: `docker compose --profile https exec caddy-https caddy trust`

---

## Services

### MCP Server

The `mcp` profile starts a combined server that exposes:

- **stdio** transport (foreground process for Claude Desktop / IDE hosts)
- **Streamable HTTP** on port 8070 (`http://localhost:8070/mcp`)
- **Web tester UI** on port 8099 (`http://localhost:8099`)

```bash
docker compose --profile mcp up -d
```

For Claude Desktop remote connections with TLS:

```bash
docker compose --profile mcp --profile https up -d
# Endpoint: https://localhost:8470/mcp
```

### MCP-HTTP (dedicated)

Legacy profile for a dedicated HTTP-only MCP server with Caddy TLS sidecar:

```bash
docker compose --profile mcp-http up -d
# Endpoint: https://localhost:8070/mcp
```

Port 8000 (internal) is NOT exposed to the host — traffic flows through Caddy only.

### Notebook

```bash
docker compose --profile notebook up -d
```

Mount notebooks for persistence:
```bash
docker run -d --network supertable-net --env-file .env \
  -v ./notebooks:/app/notebooks \
  -p 8000:8000 \
  kladnasoft/supertable:latest notebook-server
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

### Standalone

```bash
LOCKING_BACKEND=redis
SUPERTABLE_REDIS_HOST=localhost
SUPERTABLE_REDIS_PORT=6379
SUPERTABLE_REDIS_PASSWORD=change-me-strong-password
SUPERTABLE_REDIS_DB=1
```

### Sentinel (HA)

```bash
LOCKING_BACKEND=redis
SUPERTABLE_REDIS_SENTINEL=true
SUPERTABLE_REDIS_SENTINELS=redis-sentinel-1:26379,redis-sentinel-2:26379,redis-sentinel-3:26379
SUPERTABLE_REDIS_SENTINEL_MASTER=mymaster
SUPERTABLE_REDIS_SENTINEL_PASSWORD=your-sentinel-password
```

Start with sentinel profile: `cd infrastructure/redis && docker compose --profile sentinel up -d`

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVICE` | `reflection` | Service to run: `reflection`, `api`, `mcp`, `mcp-http`, `notebook`, `spark` |
| `LOG_LEVEL` | `INFO` | Logging level |
| **Auth** |||
| `SUPERTABLE_AUTH_MODE` | `api_key` | Auth mode: `api_key` or `bearer` |
| `SUPERTABLE_API_KEY` | — | API key for authentication |
| `SUPERTABLE_SUPERUSER_TOKEN` | — | Admin superuser token (required) |
| `SUPERTABLE_LOGIN_MASK` | `1` | Login modes: `1` = superuser only, `2` = users only, `3` = both |
| **Storage** |||
| `STORAGE_TYPE` | `MINIO` | Backend: `MINIO`, `S3`, `AZURE`, `GCS` |
| `STORAGE_ENDPOINT_URL` | — | S3/MinIO endpoint URL |
| `STORAGE_BUCKET` | `supertable` | Bucket name |
| `STORAGE_ACCESS_KEY` | — | Access key |
| `STORAGE_SECRET_KEY` | — | Secret key |
| `STORAGE_FORCE_PATH_STYLE` | `true` | Force path-style S3 URLs (required for MinIO) |
| **Redis** |||
| `LOCKING_BACKEND` | `redis` | Locking backend |
| `SUPERTABLE_REDIS_HOST` | `localhost` | Redis host |
| `SUPERTABLE_REDIS_PORT` | `6379` | Redis port |
| `SUPERTABLE_REDIS_DB` | `0` | Redis database number |
| `SUPERTABLE_REDIS_PASSWORD` | — | Redis password |
| **MCP** |||
| `SUPERTABLE_MCP_TRANSPORT` | `stdio` | Transport: `stdio` or `streamable-http` |
| `SUPERTABLE_MCP_PORT` | `8070` | HTTP transport port |
| `SUPERTABLE_MCP_TOKEN` | — | MCP authentication token |
| `SUPERTABLE_ALLOWED_ROLES` | — | Comma-separated allowed roles |
| **DuckDB** |||
| `SUPERTABLE_DUCKDB_PRESIGNED` | `1` | Use presigned URLs for object storage |
| `SUPERTABLE_DUCKDB_THREADS` | `4` | DuckDB worker threads |
| `SUPERTABLE_DUCKDB_EXTERNAL_THREADS` | `2` | External threads for I/O |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | `60` | HTTP timeout for DuckDB httpfs |
| **Ports** |||
| `SUPERTABLE_REFLECTION_PORT` | `8050` | Reflection UI port |
| `SUPERTABLE_API_PORT` | `8090` | REST API port |
| `SUPERTABLE_NOTEBOOK_PORT` | `8000` | Notebook server port |

---

## Volumes

| Path | Description |
|------|-------------|
| `/app/notebooks` | Notebook files (mount for persistence) |
| `/app/.env` | Environment file (mount read-only) |
| `/home/supertable/.duckdb/extensions` | DuckDB extensions (pre-baked with httpfs) |

---

## Health Checks

| Service | Endpoint | Port |
|---------|----------|------|
| Reflection | `GET /health` | 8050 |
| API | `GET /health` | 8090 |
| MCP Web | `GET /health` | 8099 |
| MCP HTTP | `GET /health` | 8070 |
| Notebook | `GET /health` | 8000 |

---

## Security

1. **Set strong tokens:**
   ```bash
   SUPERTABLE_SUPERUSER_TOKEN=<random-64-char-string>
   SUPERTABLE_API_KEY=<random-64-char-string>
   SUPERTABLE_MCP_TOKEN=<random-64-char-string>
   ```

2. **Restrict MCP roles:**
   ```bash
   SUPERTABLE_ALLOWED_ROLES=reader,writer
   ```

3. **Use Redis password in production:**
   ```bash
   SUPERTABLE_REDIS_PASSWORD=<strong-password>
   ```

4. **Non-root container:** Runs as user `supertable` (uid 1001).

---

## Troubleshooting

### Container exits immediately

```bash
docker logs supertable-reflection
```

Common causes: missing `SUPERTABLE_ORGANIZATION` or `SUPERTABLE_SUPERUSER_TOKEN`, Redis/MinIO unreachable.

### Cannot reach infrastructure

Ensure services are on `supertable-net`:

```bash
docker network inspect supertable-net
```

For host-mode development:

```bash
docker run ... \
  -e STORAGE_ENDPOINT_URL=http://host.docker.internal:9000 \
  -e REDIS_HOST=host.docker.internal \
  --add-host=host.docker.internal:host-gateway \
  kladnasoft/supertable:latest reflection-server
```

### Permission denied on mounted volumes

```bash
sudo chown -R 1001:1001 ./notebooks
```

---

## Links

- **Source**: [github.com/kladnasoft](https://github.com/kladnasoft)
- **Website**: [kladnasoft.com](https://kladnasoft.com/kladnasoft/index/)
- **License**: Supertable Proprietary Use License (STPUL)
