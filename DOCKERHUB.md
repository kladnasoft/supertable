# SuperTable – Docker Image (Updated)

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![License: STPUL](https://img.shields.io/badge/license-STPUL-blue)

This image bundles:
- **Admin API (FastAPI)** on port **8000**
- **MCP server** (`mcp_server.py`) over **stdio**
- SuperTable runtime (Redis catalog/locks + MINIO/S3/Azure/GCS object backends via DuckDB httpfs)

> **What changed in this revision**
> - Corrected environment variables (`STORAGE_*`, `REDIS_*`), DB **0** default.
> - Clear host-backed vs clean-start usage (Compose profiles).
> - Added wrapper commands: **`admin-server`** and **`mcp-server`**.
> - Clarified MCP is **stdio** only (no HTTP).

---

## Quick run – Admin (host Redis & MinIO)

```bash
docker run -d --name supertable-admin   -e LOCKING_BACKEND=redis   -e REDIS_HOST=host.docker.internal -e REDIS_PORT=6379 -e REDIS_DB=0   -e STORAGE_TYPE=MINIO   -e STORAGE_ENDPOINT_URL=http://host.docker.internal:9000   -e STORAGE_FORCE_PATH_STYLE=true   -e STORAGE_ACCESS_KEY=minioadmin   -e STORAGE_SECRET_KEY=minioadmin123!   -e STORAGE_BUCKET=supertable   -e SUPERTABLE_ADMIN_TOKEN=replace-me   -p 8000:8000   kladnasoft/supertable:latest admin-server
```

Open **http://localhost:8000** and log in with `SUPERTABLE_ADMIN_TOKEN`.

> Linux tip: use `host.docker.internal` (or your host IP if not available).

---

## MCP (stdio)

```bash
# Important: -i to keep stdio open
docker run --rm -i --name supertable-mcp   -e LOCKING_BACKEND=redis   -e REDIS_HOST=host.docker.internal -e REDIS_PORT=6379 -e REDIS_DB=0   -e STORAGE_TYPE=MINIO   -e STORAGE_ENDPOINT_URL=http://host.docker.internal:9000   -e STORAGE_FORCE_PATH_STYLE=true   -e STORAGE_ACCESS_KEY=minioadmin   -e STORAGE_SECRET_KEY=minioadmin123!   -e STORAGE_BUCKET=supertable   kladnasoft/supertable:latest mcp-server
```

> This image doesn’t ship an interactive “MCP client”. Integrate with your tool/editor that supports MCP and spawn this container or use Compose.

---

## Using AWS S3

```bash
docker run -d --name supertable-admin   -e LOCKING_BACKEND=redis   -e REDIS_HOST=host.docker.internal -e REDIS_PORT=6379 -e REDIS_DB=0   -e STORAGE_TYPE=S3   -e AWS_ACCESS_KEY_ID=...   -e AWS_SECRET_ACCESS_KEY=...   -e AWS_DEFAULT_REGION=eu-central-1   -e STORAGE_BUCKET=my-s3-bucket   -e SUPERTABLE_ADMIN_TOKEN=replace-me   -p 8000:8000   kladnasoft/supertable:latest admin-server
```

> For S3-compatible endpoints, keep `STORAGE_TYPE=MINIO` and set `STORAGE_ENDPOINT_URL`, `STORAGE_ACCESS_KEY/SECRET_KEY`, and `STORAGE_FORCE_PATH_STYLE=true`.

---

## Volumes

- `/data` – working directory & local cache (mount for persistence).
- To have the app read a file-based `.env` inside the container:
  ```bash
  -v $PWD/.env:/app/.env:ro
  ```

---

## Health

- Admin health endpoint: `GET /healthz` → `ok` if Redis is reachable.

---

## Security

- Always set a strong `SUPERTABLE_ADMIN_TOKEN`.
- For MCP: set `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1` and `SUPERTABLE_ALLOWED_USER_HASHES` as needed.
