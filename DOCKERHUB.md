# SuperTable – Docker Image

This image bundles:
- SuperTable runtime (Redis catalog + MinIO/S3/Azure/GCS backends)
- Admin API (FastAPI) on port **8000**
- MCP utilities: `mcp-server` (stdio) and `mcp-client`

## Quick run – Admin only

```bash
docker run -d --name supertable-admin   -e STORAGE_TYPE=MINIO   -e LOCKING_BACKEND=redis   -e REDIS_HOST=redis   -e REDIS_PORT=6379   -e AWS_S3_ENDPOINT_URL=http://minio:9000   -e AWS_S3_FORCE_PATH_STYLE=true   -e AWS_ACCESS_KEY_ID=minioadmin   -e AWS_SECRET_ACCESS_KEY=minioadmin123!   -e SUPERTABLE_BUCKET=supertable   -e SUPERTABLE_ADMIN_TOKEN=change-me-now   -p 8000:8000   kladnasoft/supertable:latest
```

Open **http://localhost:8000** and log in with `SUPERTABLE_ADMIN_TOKEN`.

## MCP (stdio)

```bash
# Provide env as above, and keep -i for stdio
docker run --rm -i   -e STORAGE_TYPE=MINIO   -e LOCKING_BACKEND=redis   -e REDIS_HOST=redis   -e REDIS_PORT=6379   -e AWS_S3_ENDPOINT_URL=http://minio:9000   -e AWS_S3_FORCE_PATH_STYLE=true   -e AWS_ACCESS_KEY_ID=minioadmin   -e AWS_SECRET_ACCESS_KEY=minioadmin123!   kladnasoft/supertable:latest mcp-server
```

For a quick smoke test:

```bash
docker run --rm   -e SUPERTABLE_TEST_ORG=kladna-soft   -e SUPERTABLE_TEST_SUPER=example   kladnasoft/supertable:latest mcp-client --no-query
```

## Volumes

- `/data` – working directory & local cache (mount for persistence)
- `/config/.env` – auto-generated unless you disable `AUTO_CONFIG`

## Health

- Admin health endpoint: `GET /healthz` → `ok` if Redis is reachable.

## Security

- Always set a strong `SUPERTABLE_ADMIN_TOKEN`.
- Consider network policies so Redis/MinIO are not exposed publicly.
