# SuperTable

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

**SuperTable — The simplest data warehouse & cataloging system.**

A data warehouse and cataloging platform that stores structured data on object storage, manages metadata in Redis, and queries everything through DuckDB. This image includes four services: WebUI (admin interface), REST API, OData feed server, and MCP server (AI tool integration).

---

## Step 1 — Configure

```bash
git clone https://github.com/kladnasoft/supertable.git
cd supertable
cp .env.example .env
```

Edit `.env` and set at minimum:

```bash
SUPERTABLE_SUPERUSER_TOKEN=changeme        # Admin login token
SUPERTABLE_ORGANIZATION=my-org             # Tenant name
```

The defaults for storage (MinIO) and Redis work with the `infra` profile out of the box.

---

## Step 2 — Start infrastructure

```bash
docker compose --profile infra up -d
```

This starts Redis (`:6379`) and MinIO (`:9000`, console `:9001`), creates the default `supertable` bucket, and establishes the `supertable-net` Docker network.

Verify:

```bash
docker compose --profile infra ps
# redis, minio, minio-init should all be healthy/exited
```

---

## Step 3 — Start the WebUI

```bash
docker compose --profile infra --profile webui up -d
```

Open [http://localhost:8050](http://localhost:8050) and log in with your `SUPERTABLE_SUPERUSER_TOKEN`.

The WebUI renders the admin interface and proxies all data operations to the API server internally.

---

## Step 4 — Add services (optional)

Each service is an independent profile. Add as needed:

```bash
# REST API (standalone, for external tools and scripts)
docker compose --profile infra --profile webui --profile api up -d

# OData feed server (for Excel, Power BI)
docker compose --profile infra --profile webui --profile odata up -d

# MCP server (for Claude Desktop, Cursor, and other AI tools)
docker compose --profile infra --profile webui --profile mcp up -d

# Everything at once
docker compose --profile infra --profile webui --profile api --profile odata --profile mcp up -d
```

---

## Step 5 — Add HTTPS (optional)

```bash
docker compose --profile infra --profile webui --profile https up -d
```

Uses a Caddy TLS sidecar with self-signed certificates. Trust the CA once:

```bash
docker compose --profile https exec caddy-https caddy trust
```

---

## Profiles

| Profile | Service | Port | Description |
|---|---|---|---|
| `infra` | Redis + MinIO | 6379, 9000, 9001 | Infrastructure (required) |
| `webui` | Admin UI + API proxy | 8050 | Browser admin interface |
| `api` | REST API | 8051 | Standalone JSON API |
| `odata` | OData 4.0 feed server | 8052 | Excel / Power BI feeds |
| `mcp` | MCP server | 8070, 8099 | AI tool integration |
| `mcp-http` | MCP over HTTPS | 8070 | MCP with Caddy TLS |
| `https` | TLS sidecar | 8443, 8451, 8452, 8470 | Caddy HTTPS for all services |

### HTTPS port map

| HTTPS port | Routes to |
|---|---|
| 8443 | WebUI (:8050) |
| 8451 | API (:8051) |
| 8452 | OData (:8052) |
| 8470 | MCP web tester (:8099) |
| 9443 | MinIO S3 API (:9000) |

---

## Standalone container

Run a single service without Docker Compose:

```bash
docker run -d --name supertable-webui \
  -e SUPERTABLE_SUPERUSER_TOKEN=changeme \
  -e SUPERTABLE_ORGANIZATION=my-org \
  -e STORAGE_TYPE=MINIO \
  -e STORAGE_ENDPOINT_URL=http://minio:9000 \
  -e STORAGE_ACCESS_KEY=minioadmin \
  -e STORAGE_SECRET_KEY=minioadmin123! \
  -e STORAGE_BUCKET=supertable \
  -e SUPERTABLE_REDIS_HOST=redis \
  -e SERVICE=webui \
  -p 8050:8050 \
  kladnasoft/supertable:latest
```

Available `SERVICE` values: `webui`, `api`, `odata`, `mcp`, `mcp-http`.

Convenience wrappers are also available:

```bash
docker run -d kladnasoft/supertable:latest webui-server
docker run -d kladnasoft/supertable:latest api-server
docker run -d kladnasoft/supertable:latest odata-server
docker run -d kladnasoft/supertable:latest mcp-server
```

---

## Environment variables

### Required

| Variable | Description |
|---|---|
| `SUPERTABLE_SUPERUSER_TOKEN` | Admin login token |
| `SUPERTABLE_ORGANIZATION` | Tenant name |

### Storage — MinIO (default)

| Variable | Default | Description |
|---|---|---|
| `STORAGE_TYPE` | `MINIO` | Storage backend |
| `STORAGE_ENDPOINT_URL` | `http://minio:9000` | MinIO endpoint |
| `STORAGE_ACCESS_KEY` | `minioadmin` | Access key |
| `STORAGE_SECRET_KEY` | `minioadmin123!` | Secret key |
| `STORAGE_BUCKET` | `supertable` | Bucket name |
| `STORAGE_FORCE_PATH_STYLE` | `true` | Path-style access |

### Storage — Amazon S3

| Variable | Description |
|---|---|
| `STORAGE_TYPE` | Set to `S3` |
| `AWS_ACCESS_KEY_ID` | AWS access key |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key |
| `AWS_DEFAULT_REGION` | AWS region |
| `STORAGE_BUCKET` | S3 bucket name |

### Storage — Azure Blob

| Variable | Description |
|---|---|
| `STORAGE_TYPE` | Set to `AZURE` |
| `AZURE_STORAGE_CONNECTION_STRING` | Connection string (or use managed identity) |
| `AZURE_CONTAINER` | Container name |

### Storage — GCP Cloud Storage

| Variable | Description |
|---|---|
| `STORAGE_TYPE` | Set to `GCP` |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON |
| `GCS_BUCKET` | Bucket name |

### Redis

| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_REDIS_HOST` | `redis` | Redis hostname |
| `SUPERTABLE_REDIS_PORT` | `6379` | Redis port |
| `SUPERTABLE_REDIS_PASSWORD` | (empty) | Redis auth password |
| `SUPERTABLE_REDIS_SENTINEL` | `false` | Enable Sentinel HA |
| `SUPERTABLE_REDIS_SENTINELS` | (empty) | Sentinel addresses (`host1:port,host2:port`) |

### Server

| Variable | Default | Description |
|---|---|---|
| `SUPERTABLE_UI_PORT` | `8050` | WebUI listen port |
| `SUPERTABLE_API_PORT` | `8051` | API listen port |
| `SUPERTABLE_ODATA_PORT` | `8052` | OData listen port |
| `SUPERTABLE_SESSION_SECRET` | (empty) | Cookie signing key (set in production) |
| `SUPERTABLE_MCP_TOKEN` | (empty) | MCP shared secret |
| `SUPERTABLE_LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `SUPERTABLE_LOG_FORMAT` | `json` | `json` or `text` |

---

## Health check

All services expose `/healthz` which returns `ok` when Redis is reachable.

```bash
curl http://localhost:8050/healthz
```

---

## Links

- **GitHub**: [github.com/kladnasoft/supertable](https://github.com/kladnasoft/supertable)
- **PyPI**: [pypi.org/project/supertable](https://pypi.org/project/supertable/)
- **Documentation**: [github.com/kladnasoft/supertable/tree/master/docs](https://github.com/kladnasoft/supertable/tree/master/docs)

---

## License

Super Table Public Use License (STPUL) v1.0

Copyright © Kladna Soft Kft. All rights reserved.
