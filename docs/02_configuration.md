# Configuration Reference

## Overview

SuperTable uses a centralized, immutable configuration system. All settings are loaded once at startup from environment variables (with optional `.env` file support) into a frozen Python dataclass. Values never change at runtime — restart the process to pick up new values.

**Source of truth**: `supertable/config/settings.py`

## How Configuration Works

1. **`.env` loading** — `python-dotenv` loads the nearest `.env` file at import time (via `find_dotenv(usecwd=True)`). Existing env vars are NOT overridden (`override=False`).
2. **Type-safe parsing** — Helper functions (`_env_str`, `_env_int`, `_env_bool`, `_env_float`) parse each variable with fallback defaults. Boolean parsing accepts `1/true/yes/y/on` and `0/false/no/n/off`.
3. **Frozen dataclass** — All values are stored in a `@dataclass(frozen=True)` singleton named `settings`. Immutable after construction.
4. **Zero internal imports** — The settings module has no imports from `supertable.*`, preventing circular dependencies. It can safely be imported first.

### Adding a New Environment Variable

1. Add a field to the `Settings` dataclass with type, default, and inline comment
2. Wire it in `_build_settings()` using the `_env_*` helpers
3. Replace any `os.getenv()` call in consuming modules with `settings.YOUR_FIELD`

## Environment Variables

### Core

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_HOME` | str | `~/supertable` | Root directory for local data storage |
| `SUPERTABLE_ORGANIZATION` | str | _(empty)_ | Organization namespace |
| `SUPERTABLE_PREFIX` | str | _(empty)_ | Base prefix for all storage keys |
| `DOTENV_PATH` | str | `.env` | Path to .env file |

### Processing Defaults

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `MAX_MEMORY_CHUNK_SIZE` | int | `16777216` (16 MB) | Maximum in-memory chunk size for processing |
| `MAX_OVERLAPPING_FILES` | int | `100` | Maximum overlapping files before error |
| `DEFAULT_TIMEOUT_SEC` | int | `60` | Default operation timeout |
| `DEFAULT_LOCK_DURATION_SEC` | int | `30` | Default distributed lock TTL |
| `IS_SHOW_TIMING` | bool | `true` | Show timing information in responses |

### Storage (General)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `STORAGE_TYPE` | str | `LOCAL` | Backend: `LOCAL`, `S3`, `MINIO`, `AZURE`, `GCS`/`GCP` |
| `STORAGE_BUCKET` | str | `supertable` | Bucket/container name |
| `STORAGE_REGION` | str | `us-east-1` | Cloud region |
| `STORAGE_ENDPOINT_URL` | str | _(empty)_ | Custom endpoint (for MinIO, S3-compatible) |
| `STORAGE_ACCESS_KEY` | str | _(empty)_ | Access key ID |
| `STORAGE_SECRET_KEY` | str | _(empty)_ | Secret access key |
| `STORAGE_SESSION_TOKEN` | str | _(empty)_ | AWS session token (temporary credentials) |
| `STORAGE_FORCE_PATH_STYLE` | bool | `true` | Use path-style addressing |
| `STORAGE_USE_SSL` | bool | `false` | Enable SSL for storage connections |

### Storage — Azure

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `AZURE_STORAGE_ACCOUNT` | str | _(empty)_ | Azure storage account name |
| `AZURE_CONTAINER` | str | _(empty)_ | Azure container name (alias for STORAGE_BUCKET) |
| `AZURE_BLOB_ENDPOINT` | str | _(empty)_ | Blob endpoint URL |
| `AZURE_STORAGE_CONNECTION_STRING` | str | _(empty)_ | Full connection string (highest priority) |
| `AZURE_STORAGE_KEY` | str | _(empty)_ | Account key (alias for STORAGE_ACCESS_KEY) |
| `AZURE_SAS_TOKEN` | str | _(empty)_ | Shared Access Signature token |

### Storage — GCP

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `GCS_BUCKET` | str | _(empty)_ | GCS bucket (preferred over STORAGE_BUCKET) |
| `GOOGLE_APPLICATION_CREDENTIALS` | str | _(empty)_ | Path to service account JSON |
| `GCP_SA_JSON` | str | _(empty)_ | Raw service account JSON string |
| `GCP_PROJECT` | str | _(empty)_ | GCP project ID |

### DuckDB Engine

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | str | `1GB` | DuckDB memory limit |
| `SUPERTABLE_DUCKDB_THREADS` | str | _(empty)_ | DuckDB thread count (auto if empty) |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | int | `3` | I/O thread multiplier |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | str | _(empty)_ | HTTP timeout for remote reads |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | bool | `true` | Cache HTTP metadata |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | str | `5GB` | External cache size |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | str | _(empty)_ | External cache directory |
| `SUPERTABLE_DUCKDB_MATERIALIZE` | str | `view` | Materialization mode (`view` or `table`) |
| `SUPERTABLE_DUCKDB_PRESIGNED` | bool | `false` | Use presigned URLs for DuckDB access |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | bool | `false` | Use HTTP(S) URLs instead of native protocol |
| `SUPERTABLE_DEBUG_TIMINGS` | bool | `false` | Enable debug timing output |

### Engine Routing

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_ENGINE_LITE_MAX_BYTES` | int | `104857600` (100 MB) | Max data size for DuckDB Lite |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | int | `10737418240` (10 GB) | Min data size for Spark routing |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | int | `300` (5 min) | Freshness threshold for engine selection |
| `SUPERTABLE_DEFAULT_ENGINE` | str | `AUTO` | Engine override: `AUTO`, `LITE`, `PRO`, `SPARK` |

### Spark Engine

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | int | `300` | Total query timeout (seconds) |
| `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | int | `120` | Statement timeout (seconds) |
| `SUPERTABLE_SPARK_CONNECT_TIMEOUT` | int | `30` | Connection timeout (seconds) |
| `SUPERTABLE_SPARK_BATCH_SIZE` | int | `50` | Result fetch batch size |

### Redis

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_REDIS_URL` | str | _(empty)_ | Redis URL (takes precedence) |
| `SUPERTABLE_REDIS_HOST` | str | `localhost` | Redis host |
| `SUPERTABLE_REDIS_PORT` | int | `6379` | Redis port |
| `SUPERTABLE_REDIS_DB` | int | `0` | Redis database number |
| `SUPERTABLE_REDIS_PASSWORD` | str | _(empty)_ | Redis password |
| `SUPERTABLE_REDIS_USERNAME` | str | _(empty)_ | Redis username (ACL) |
| `SUPERTABLE_REDIS_SSL` | bool | `false` | Enable Redis SSL/TLS |
| `SUPERTABLE_REDIS_SENTINEL` | bool | `false` | Enable Sentinel mode |
| `SUPERTABLE_REDIS_SENTINELS` | str | _(empty)_ | Sentinel addresses (comma-separated) |
| `SUPERTABLE_REDIS_SENTINEL_MASTER` | str | `mymaster` | Sentinel master name |
| `SUPERTABLE_REDIS_SENTINEL_PASSWORD` | str | _(empty)_ | Sentinel password |
| `SUPERTABLE_REDIS_SENTINEL_STRICT` | str | _(empty)_ | Strict Sentinel mode |
| `SUPERTABLE_REFLECTION_REDIS_URL` | str | _(empty)_ | Override Redis URL for UI/reflection |

### API Server

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_API_HOST` | str | `0.0.0.0` | API bind address |
| `SUPERTABLE_API_PORT` | int | `8051` | API listen port |
| `UVICORN_RELOAD` | bool | `false` | Enable hot reload (development) |
| `SUPERTABLE_PROXY_TIMEOUT` | float | `60.0` | WebUI → API proxy timeout |

### Authentication

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_AUTH_MODE` | str | `api_key` | Auth mode: `api_key`, `bearer`, `session` |
| `SUPERTABLE_AUTH_HEADER_NAME` | str | `X-API-Key` | Custom auth header name |
| `SUPERTABLE_API_KEY` | str | _(empty)_ | API key value |
| `SUPERTABLE_BEARER_TOKEN` | str | _(empty)_ | Bearer token value |
| `SUPERTABLE_SUPERUSER_TOKEN` | str | _(empty)_ | Superuser admin token |
| `SUPERTABLE_SESSION_SECRET` | str | _(empty)_ | Session cookie signing secret |
| `SUPERTABLE_ROLE` | str | _(empty)_ | Default role name |
| `SUPERTABLE_LOGIN_MASK` | int | `1` | Login form configuration mask |
| `SECURE_COOKIES` | bool | `false` | Require HTTPS for session cookies |

### MCP Server

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_MCP_TOKEN` | str | _(empty)_ | MCP authentication token |
| `SUPERTABLE_MCP_PORT` | int | `8000` | MCP server port |
| `SUPERTABLE_DEFAULT_LIMIT` | int | `200` | Default query result limit |
| `SUPERTABLE_MAX_LIMIT` | int | `5000` | Maximum query result limit |
| `SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC` | float | `60.0` | Default query timeout |
| `SUPERTABLE_MAX_CONCURRENCY` | int | `6` | Max concurrent MCP operations |
| `MCP_SERVER_PATH` | str | `mcp_server.py` | MCP server script path |
| `MCP_WIRE` | str | `ndjson` | Wire format |
| `SUPERTABLE_ALLOWED_HOSTS` | str | `*` | Allowed hosts |

### MCP Web Server

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_MCP_WEB_HOST` | str | `0.0.0.0` | Web MCP bind address |
| `SUPERTABLE_MCP_WEB_PORT` | int | `8099` | Web MCP port |
| `SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS` | bool | `false` | Disable subprocess spawning |
| `SUPERTABLE_MCP_HTTP_TOKEN` | str | _(empty)_ | HTTP transport token |
| `FORWARDED_ALLOW_IPS` | str | `*` | Trusted proxy IPs |

### MCP Client (Test/CLI)

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_TEST_ORG` | str | _(empty)_ | Test organization name |
| `SUPERTABLE_TEST_SUPER` | str | _(empty)_ | Test SuperTable name |
| `SUPERTABLE_TEST_ENGINE` | str | _(empty)_ | Test engine override |
| `SUPERTABLE_TEST_TIMEOUT_SEC` | float | `0.0` | Test timeout |
| `XDG_CONFIG_HOME` | str | `~/.config` | XDG config directory |

### Reflection UI

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_UI_HOST` | str | `0.0.0.0` | WebUI bind address |
| `SUPERTABLE_UI_PORT` | int | `8050` | WebUI port |
| `SUPERTABLE_STATIC_DIR` | str | _(empty)_ | Custom static files directory |
| `SUPERTABLE_ODATA_BASE_URL` | str | `/api/v1/reflection` | OData base URL path |
| `SUPERTABLE_ODATA_HOST` | str | `0.0.0.0` | OData bind address |
| `SUPERTABLE_ODATA_PORT` | int | `8052` | OData port |
| `SUPERTABLE_REFLECTION_STATE_DIR` | str | `/tmp/supertable_reflection` | Reflection state directory |
| `TEMPLATES_DIR` | str | _(empty)_ | Custom templates directory |

### Logging

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_LOG_LEVEL` | str | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `SUPERTABLE_LOG_FORMAT` | str | `json` | Log format: `json` (stackdriver) or `text` |
| `SUPERTABLE_LOG_FILE` | str | _(empty)_ | Optional log file path |
| `SUPERTABLE_LOG_COLOR` | str | _(empty)_ | Force color output |
| `SUPERTABLE_CORRELATION_HEADER` | str | `X-Correlation-ID` | Correlation ID header name |
| `NO_COLOR` | bool | `false` | Disable colored output (standard) |

### Monitoring

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_MONITORING_ENABLED` | bool | `true` | Enable monitoring metrics |
| `SUPERTABLE_MONITOR_CACHE_MAX` | int | `256` | Max monitoring cache entries |

### Rate Limiting

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_API_RATE_LIMIT_ENABLED` | bool | `false` | Enable API rate limiting |
| `SUPERTABLE_API_RATE_LIMIT_RPM` | int | `300` | Requests per minute limit |

### Notebook

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_NOTEBOOK_PORT` | int | `8010` | Notebook worker port |

### Meta Caching

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_SUPER_META_CACHE_TTL_S` | float | `None` | SuperTable metadata cache TTL (seconds) |

### Audit

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_AUDIT_ENABLED` | bool | `true` | Enable audit logging |
| `SUPERTABLE_AUDIT_RETENTION_DAYS` | int | `2555` (~7 years) | Parquet audit log retention |
| `SUPERTABLE_AUDIT_BATCH_SIZE` | int | `1000` | Events per batch flush |
| `SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC` | int | `60` | Flush interval (seconds) |
| `SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS` | int | `24` | Redis Stream TTL |
| `SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN` | int | `100000` | Max Redis Stream length |
| `SUPERTABLE_AUDIT_HASH_CHAIN` | bool | `true` | Enable tamper-evident hash chain |
| `SUPERTABLE_AUDIT_LOG_QUERIES` | bool | `true` | Log SQL queries |
| `SUPERTABLE_AUDIT_LOG_READS` | bool | `true` | Log read operations |
| `SUPERTABLE_AUDIT_ALERT_WEBHOOK` | str | _(empty)_ | Webhook URL for alerts |
| `SUPERTABLE_AUDIT_LEGAL_HOLD` | bool | `false` | Prevent audit log deletion |
| `SUPERTABLE_AUDIT_FERNET_KEY` | str | _(empty)_ | Fernet encryption key |
| `SUPERTABLE_AUDIT_SIEM_ENABLED` | bool | `true` | Enable SIEM integration |
| `SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS` | int | `10` | Max SIEM consumer groups |

### Data Sharing

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_SHARE_PRESIGN_TTL` | int | `14400` (4 hours) | Presigned URL TTL |
| `SUPERTABLE_SHARE_REFRESH_BUFFER` | int | `600` (10 min) | Refresh buffer before expiry |

### Storage GC

Sunset parquet files (replaced during compaction) and old snapshot
JSONs are never deleted by the writer directly. When either flag is
enabled, the writer XADDs the paths onto a per-table Redis STREAM
after a successful leaf-CAS commit. A separate orchestrator
(`GCCleaner.tick()` — see chap. 17) drains entries older than the
delay window and calls `storage.delete()`. The delay avoids the race
where an in-flight reader's `parquet_scan([...])` resolves a path
just before the writer deletes it. **All defaults preserve the
pre-existing behaviour (off / unbounded).**

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SUPERTABLE_SNAPSHOT_RETENTION` | int | `0` | Number of snapshot JSONs to keep per simple table. `0` = unlimited. With `N > 0`, snapshots older than the `N`-th most recent are queued for deletion. |
| `SUPERTABLE_SUNSET_GC_ENABLED` | bool | `false` | When `true`, sunset parquet files are queued for deletion. When `false`, they remain on storage forever (pre-existing behaviour). |
| `SUPERTABLE_GC_DELAY_SEC` | int | `1800` (30 min) | Minimum age before the cleaner deletes an entry. Wide enough that any in-flight DuckDB `parquet_scan` has finished. |
| `SUPERTABLE_GC_SLEEP_SEC` | int | `60` | Cleaner-daemon loop sleep between ticks. Consulted only by the convenience daemon (`supertable.gc.daemon`); the `tick()` library primitive doesn't loop. |
| `SUPERTABLE_GC_BATCH_SIZE` | int | `500` | Max entries the cleaner processes per stream per tick. |

## Fallback Chains

Several settings have fallback logic for backward compatibility:

| Property | Fallback Chain |
|----------|---------------|
| `effective_redis_url` | `SUPERTABLE_REFLECTION_REDIS_URL` → `SUPERTABLE_REDIS_URL` → `None` |
| `effective_storage_bucket` | `STORAGE_BUCKET` → `AZURE_CONTAINER` → `"supertable"` |
| `effective_storage_endpoint` | `STORAGE_ENDPOINT_URL` → `AZURE_BLOB_ENDPOINT` |
| `effective_storage_access_key` | `STORAGE_ACCESS_KEY` → `AZURE_STORAGE_KEY` |
| `effective_gcs_bucket` | `GCS_BUCKET` → `STORAGE_BUCKET` → `"supertable"` |
| `effective_redis_sentinel_password` | `SUPERTABLE_REDIS_SENTINEL_PASSWORD` → `SUPERTABLE_REDIS_PASSWORD` |
| Superuser token | `SUPERTABLE_SUPERUSER_TOKEN` → `SUPERTABLE_SUPERTOKEN` |
| MCP token | `SUPERTABLE_MCP_TOKEN` → `SUPERTABLE_MCP_AUTH_TOKEN` |
