# Data Island Core — Configuration & Settings

## Overview

All configuration in Data Island Core flows through a single frozen dataclass in `supertable/config/settings.py`. Environment variables are read once at process startup, validated, and locked into an immutable `Settings` object. Every module imports this object rather than calling `os.getenv()` directly — there are no scattered defaults, no runtime mutation, and no mismatch bugs.

The system supports `.env` files (via `python-dotenv`, loaded automatically from the working directory) and standard environment variables. Environment variables always take precedence over `.env` values.

---

## How it works

1. At import time, `python-dotenv` loads the nearest `.env` file (if found) without overriding existing environment variables.
2. `_build_settings()` reads every env var through type-safe helper functions (`_env_str`, `_env_int`, `_env_bool`, `_env_float`).
3. A frozen `Settings` dataclass is constructed. Once built, no field can be modified — the process must be restarted to pick up new values.
4. The module exports `settings` as a singleton: `from supertable.config.settings import settings`.

Fallback chains exist for several settings where legacy env var names are still supported. For example, `SUPERTABLE_SUPERUSER_TOKEN` falls back to `SUPERTABLE_SUPERTOKEN`, and `SUPERTABLE_MCP_TOKEN` falls back to `SUPERTABLE_MCP_AUTH_TOKEN`.

---

## Settings reference

### Core

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_HOME` | str | `~/supertable` | Application home directory. Created on startup if it doesn't exist. Used for logs, cache, and temporary files. |
| `SUPERTABLE_ORGANIZATION` | str | (empty) | **Required.** Default tenant organization. Scopes all data, metadata, roles, and tokens. |
| `SUPERTABLE_PREFIX` | str | (empty) | Optional prefix for Redis keys and storage paths. Useful for shared infrastructure. |
| `DOTENV_PATH` | str | `.env` | Path to the `.env` file. Loaded automatically at startup. |

### Defaults

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `MAX_MEMORY_CHUNK_SIZE` | int | `16777216` (16 MB) | Maximum chunk size for in-memory data processing. |
| `MAX_OVERLAPPING_FILES` | int | `100` | Maximum number of overlapping files before compaction is triggered during writes. |
| `DEFAULT_TIMEOUT_SEC` | int | `60` | Default timeout for lock acquisition and network operations. |
| `DEFAULT_LOCK_DURATION_SEC` | int | `30` | Default TTL for Redis distributed locks. |
| `IS_SHOW_TIMING` | bool | `false` | Enable timing instrumentation in write operations. |

### Storage — General

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `STORAGE_TYPE` | str | `LOCAL` | Storage backend. One of: `LOCAL`, `MINIO`, `S3`, `AZURE`, `GCP`. |
| `STORAGE_BUCKET` | str | `supertable` | Bucket or container name. |
| `STORAGE_REGION` | str | `eu-central-1` | AWS/MinIO region identifier. |
| `STORAGE_ENDPOINT_URL` | str | (empty) | Custom endpoint URL. Required for MinIO (`http://minio:9000`). |
| `STORAGE_ACCESS_KEY` | str | (empty) | Access key for S3/MinIO. |
| `STORAGE_SECRET_KEY` | str | (empty) | Secret key for S3/MinIO. |
| `STORAGE_SESSION_TOKEN` | str | (empty) | Temporary session token (STS). |
| `STORAGE_FORCE_PATH_STYLE` | bool | `true` | Use path-style URLs. Required for MinIO. |
| `STORAGE_USE_SSL` | bool | `false` | Enable TLS for storage connections. |

### Storage — Azure

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `AZURE_STORAGE_ACCOUNT` | str | (empty) | Azure storage account name. |
| `AZURE_CONTAINER` | str | (empty) | Azure Blob container name. Falls back to `STORAGE_BUCKET`. |
| `AZURE_BLOB_ENDPOINT` | str | (empty) | Custom Azure Blob endpoint URL. |
| `AZURE_STORAGE_CONNECTION_STRING` | str | (empty) | Full Azure connection string. Takes precedence over individual fields. |
| `AZURE_STORAGE_KEY` | str | (empty) | Azure storage access key. |
| `AZURE_SAS_TOKEN` | str | (empty) | Azure shared access signature token. |

### Storage — GCP

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `GCS_BUCKET` | str | (empty) | GCS bucket name. Falls back to `STORAGE_BUCKET`. |
| `GOOGLE_APPLICATION_CREDENTIALS` | str | (empty) | Path to GCP service account JSON file. |
| `GCP_SA_JSON` | str | (empty) | Inline GCP service account JSON (alternative to file path). |
| `GCP_PROJECT` | str | (empty) | GCP project ID. |

### DuckDB engine

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | str | `1GB` | DuckDB memory limit per connection. |
| `SUPERTABLE_DUCKDB_THREADS` | str | (auto) | DuckDB thread count. Empty = auto-detect. |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | int | `3` | I/O parallelism multiplier for httpfs reads. |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | str | (empty) | HTTP timeout for httpfs operations (seconds). |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | bool | `true` | Cache HTTP metadata responses. |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | str | (empty) | External cache size for DuckDB. |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | str | (empty) | External cache directory path. |
| `SUPERTABLE_DUCKDB_MATERIALIZE` | str | `view` | Materialization strategy: `view` or `table`. |
| `SUPERTABLE_DUCKDB_PRESIGNED` | bool | `false` | Use presigned URLs for object storage reads. Recommended for MinIO. |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | bool | `false` | Force httpfs extension for all reads. |
| `SUPERTABLE_DEBUG_TIMINGS` | bool | `false` | Log detailed query timing breakdowns. |

### Engine routing

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_ENGINE_LITE_MAX_BYTES` | int | `104857600` (100 MB) | Maximum data size for DuckDB Lite. Queries above this use DuckDB Pro. |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | int | `10737418240` (10 GB) | Minimum data size for Spark SQL routing. |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | int | `300` | Cache TTL for data size estimates used in engine routing. |
| `SUPERTABLE_DEFAULT_ENGINE` | str | `AUTO` | Default engine: `AUTO`, `DUCKDB_LITE`, `DUCKDB_PRO`, or `SPARK_SQL`. |

### Spark engine

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | int | `300` | Overall query timeout (seconds). |
| `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | int | `120` | Individual SQL statement timeout (seconds). |
| `SUPERTABLE_SPARK_CONNECT_TIMEOUT` | int | `30` | Thrift connection timeout (seconds). |
| `SUPERTABLE_SPARK_BATCH_SIZE` | int | `50` | Result batch size for Thrift fetch. |

### Redis

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_REDIS_URL` | str | (empty) | Full Redis URL (`redis://host:6379/0`). If set, overrides host/port/db. |
| `SUPERTABLE_REDIS_HOST` | str | `localhost` | Redis host. Use `redis-master` in Docker. |
| `SUPERTABLE_REDIS_PORT` | int | `6379` | Redis port. |
| `SUPERTABLE_REDIS_DB` | int | `0` | Redis database number. |
| `SUPERTABLE_REDIS_PASSWORD` | str | (empty) | Redis auth password. |
| `SUPERTABLE_REDIS_USERNAME` | str | (empty) | Redis ACL username (Redis 6+). |
| `SUPERTABLE_REDIS_SSL` | bool | `false` | Enable TLS for Redis connections. |
| `SUPERTABLE_REDIS_SENTINEL` | bool | `false` | Enable Redis Sentinel mode. |
| `SUPERTABLE_REDIS_SENTINELS` | str | (empty) | Comma-separated Sentinel addresses (`host1:port1,host2:port2`). |
| `SUPERTABLE_REDIS_SENTINEL_MASTER` | str | `mymaster` | Sentinel master name. |
| `SUPERTABLE_REDIS_SENTINEL_PASSWORD` | str | (empty) | Sentinel auth password. Falls back to `SUPERTABLE_REDIS_PASSWORD`. |
| `SUPERTABLE_REDIS_SENTINEL_STRICT` | str | (empty) | Strict Sentinel mode flag. |
| `SUPERTABLE_REFLECTION_REDIS_URL` | str | (empty) | Override Redis URL for the WebUI server only. |

### API server

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_API_HOST` | str | `0.0.0.0` | API server bind address. |
| `SUPERTABLE_API_PORT` | int | `8050` | API server listen port. |
| `UVICORN_RELOAD` | bool | `false` | Enable hot reload (development only). |
| `SUPERTABLE_PROXY_TIMEOUT` | float | `60.0` | Timeout for WebUI → API proxy requests (seconds). |

### Authentication

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_AUTH_MODE` | str | `api_key` | Auth mode for the API: `api_key` or `bearer`. |
| `SUPERTABLE_AUTH_HEADER_NAME` | str | `X-API-Key` | HTTP header name for API key authentication. |
| `SUPERTABLE_API_KEY` | str | (empty) | API key value (when `AUTH_MODE=api_key`). |
| `SUPERTABLE_BEARER_TOKEN` | str | (empty) | Bearer token value (when `AUTH_MODE=bearer`). |
| `SUPERTABLE_SUPERUSER_TOKEN` | str | (empty) | **Required.** Superuser token for admin access. Also accepts `SUPERTABLE_SUPERTOKEN`. |
| `SUPERTABLE_SESSION_SECRET` | str | (empty) | Secret for HMAC-signing session cookies. **Required in production.** |
| `SUPERTABLE_ROLE` | str | (empty) | Default RBAC role when none is specified. |
| `SUPERTABLE_LOGIN_MASK` | int | `1` | Login modes: `1` = superuser only, `2` = regular users only, `3` = both. |
| `SECURE_COOKIES` | bool | `false` | Set `Secure` flag on session cookies. Enable when serving over HTTPS. |

### MCP server

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_MCP_TOKEN` | str | (empty) | Shared secret for MCP auth. Also accepts `SUPERTABLE_MCP_AUTH_TOKEN`. |
| `SUPERTABLE_MCP_PORT` | int | `8000` | MCP HTTP server port (streamable-HTTP transport). |
| `SUPERTABLE_DEFAULT_LIMIT` | int | `200` | Default row limit for MCP query results. |
| `SUPERTABLE_MAX_LIMIT` | int | `5000` | Maximum row limit for MCP query results. |
| `SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC` | float | `60.0` | Default query timeout for MCP tools. |
| `SUPERTABLE_MAX_CONCURRENCY` | int | `6` | Maximum concurrent MCP tool executions. |
| `MCP_SERVER_PATH` | str | `mcp_server.py` | MCP server script path (for subprocess spawning). |
| `MCP_WIRE` | str | `ndjson` | MCP wire protocol format. |
| `SUPERTABLE_ALLOWED_HOSTS` | str | `*` | Allowed CORS origins for MCP HTTP transport. |

### MCP web server

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_MCP_WEB_HOST` | str | `0.0.0.0` | MCP web tester bind address. |
| `SUPERTABLE_MCP_WEB_PORT` | int | `8099` | MCP web tester port. |
| `SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS` | bool | `false` | Run MCP in-process instead of as a subprocess. |
| `SUPERTABLE_MCP_HTTP_TOKEN` | str | (empty) | Auth token for the MCP HTTP gateway. |
| `FORWARDED_ALLOW_IPS` | str | `*` | Trusted proxy IPs for forwarded headers. |

### MCP client (test/CLI)

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_TEST_ORG` | str | (empty) | Default organization for MCP test client. |
| `SUPERTABLE_TEST_SUPER` | str | (empty) | Default SuperTable for MCP test client. |
| `SUPERTABLE_TEST_ENGINE` | str | (empty) | Default engine for MCP test client. |
| `SUPERTABLE_TEST_TIMEOUT_SEC` | float | `0.0` | Timeout override for MCP test client. |
| `XDG_CONFIG_HOME` | str | `~/.config` | XDG config directory (for Claude Desktop config path). |

### Reflection UI

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_UI_HOST` | str | `0.0.0.0` | WebUI server bind address. |
| `SUPERTABLE_UI_PORT` | int | `8051` | WebUI server port (internal, behind proxy). |
| `SUPERTABLE_STATIC_DIR` | str | (auto) | Path to static files directory. Auto-detected from package location. |
| `SUPERTABLE_ODATA_BASE_URL` | str | `/api/v1/reflection` | Base URL for OData feed endpoints. |
| `SUPERTABLE_REFLECTION_STATE_DIR` | str | `/tmp/supertable_reflection` | Temporary state directory for the WebUI. |
| `TEMPLATES_DIR` | str | (auto) | Path to Jinja2 templates directory. Auto-detected from package location. |

### Logging

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_LOG_LEVEL` | str | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. |
| `SUPERTABLE_LOG_FORMAT` | str | `json` | Log format: `json` (machine-parseable) or `text` (human-readable). |
| `SUPERTABLE_LOG_FILE` | str | (empty) | Log file path. Set to `none` to disable file logging. Default: `{SUPERTABLE_HOME}/log/st.log`. |
| `SUPERTABLE_LOG_COLOR` | str | (empty) | Force colored output. |
| `SUPERTABLE_CORRELATION_HEADER` | str | `X-Correlation-ID` | HTTP header name for request correlation ID propagation. |
| `NO_COLOR` | bool | `false` | Disable colored log output (standard `NO_COLOR` convention). |

### Monitoring

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_MONITORING_ENABLED` | bool | `true` | Enable read/write metrics collection. |
| `SUPERTABLE_MONITOR_CACHE_MAX` | int | `256` | Maximum cached monitoring writer instances (one per org/super combo). |

### Notebook

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_NOTEBOOK_PORT` | int | `8010` | Notebook/Python Worker WebSocket port. |

### Meta reader

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_SUPER_META_CACHE_TTL_S` | float | (none) | Cache TTL for MetaReader results (seconds). Empty = no caching. |

### Audit

| Environment variable | Type | Default | Description |
|---|---|---|---|
| `SUPERTABLE_AUDIT_ENABLED` | bool | `true` | Master switch for audit logging. |
| `SUPERTABLE_AUDIT_RETENTION_DAYS` | int | `2555` | Parquet retention period (~7 years). DORA requires minimum 5 years. |
| `SUPERTABLE_AUDIT_BATCH_SIZE` | int | `1000` | Maximum events per Parquet batch file. |
| `SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC` | int | `60` | Maximum seconds before flushing a partial batch. |
| `SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS` | int | `24` | Redis Stream hot tier retention (hours). |
| `SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN` | int | `100000` | Hard cap on Redis Stream entries. |
| `SUPERTABLE_AUDIT_HASH_CHAIN` | bool | `true` | Enable SHA-256 tamper-evident hash chaining. |
| `SUPERTABLE_AUDIT_LOG_QUERIES` | bool | `true` | Log SQL query execution events. |
| `SUPERTABLE_AUDIT_LOG_READS` | bool | `true` | Log table read events. Set to `false` to reduce volume outside DORA scope. |
| `SUPERTABLE_AUDIT_ALERT_WEBHOOK` | str | (empty) | Webhook URL for critical audit alerts. |
| `SUPERTABLE_AUDIT_LEGAL_HOLD` | bool | `false` | Suspend retention enforcement. No audit data is deleted when `true`. |
| `SUPERTABLE_AUDIT_FERNET_KEY` | str | (empty) | Fernet encryption key for full SQL text in audit events. If empty, SQL is stored in plaintext. |
| `SUPERTABLE_AUDIT_SIEM_ENABLED` | bool | `true` | Allow external SIEM consumer groups on the audit Redis Stream. |
| `SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS` | int | `10` | Maximum external consumer groups. |

---

## Convenience properties

The `Settings` dataclass includes computed properties that resolve fallback chains:

| Property | Returns | Fallback chain |
|---|---|---|
| `effective_api_host` | API bind address | `SUPERTABLE_API_HOST` |
| `effective_redis_url` | Redis URL or None | `SUPERTABLE_REFLECTION_REDIS_URL` → `SUPERTABLE_REDIS_URL` → None |
| `effective_redis_sentinel_password` | Sentinel password | `SUPERTABLE_REDIS_SENTINEL_PASSWORD` → `SUPERTABLE_REDIS_PASSWORD` |
| `effective_storage_bucket` | Bucket/container name | `STORAGE_BUCKET` → `AZURE_CONTAINER` → `"supertable"` |
| `effective_storage_endpoint` | Storage endpoint URL | `STORAGE_ENDPOINT_URL` → `AZURE_BLOB_ENDPOINT` |
| `effective_storage_access_key` | Storage access key | `STORAGE_ACCESS_KEY` → `AZURE_STORAGE_KEY` |
| `effective_gcs_bucket` | GCS bucket name | `GCS_BUCKET` → `STORAGE_BUCKET` → `"supertable"` |

---

## The SUPERTABLE_HOME directory

The `SUPERTABLE_HOME` directory (default: `~/supertable`) is created automatically on startup. It serves as the working directory for the application and contains:

```
~/supertable/
├── log/
│   └── st.log          Structured log file (JSON format)
└── (working files)     Temporary processing files
```

The home directory is resolved by `supertable/config/homedir.py`, which expands `~`, creates the directory, and changes the process working directory to it. The resolved path is available via `get_app_home()`.

---

## The Default dataclass

`supertable/config/defaults.py` provides a legacy `Default` dataclass that mirrors a subset of `Settings` fields. It exists for backward compatibility — new code should import from `settings` directly. The `Default` instance is mutable (unlike `Settings`) and supports `update_default()` for runtime changes to log level.

```python
from supertable.config.defaults import default

# Legacy pattern (backward compatibility)
default.MAX_MEMORY_CHUNK_SIZE  # 16 MB

# Preferred pattern
from supertable.config.settings import settings
settings.MAX_MEMORY_CHUNK_SIZE  # 16 MB (same value, immutable)
```

---

## Module structure

```
supertable/config/
  __init__.py          Empty (package marker)
  settings.py          Settings dataclass, _build_settings(), env var parsing helpers
  defaults.py          Legacy Default dataclass, colored logging setup
  homedir.py           SUPERTABLE_HOME resolution, directory creation, cwd change
```

---

## Adding a new setting

1. Add a field to the `Settings` dataclass with its type, default value, and an inline comment containing the env var name.
2. Add the corresponding `_env_*()` call in `_build_settings()`, in the matching section.
3. Replace `os.getenv()` calls in the consuming module with `settings.YOUR_FIELD`.
4. Document the setting in this file.

---

## Frequently asked questions

**Can I change settings at runtime without restarting?**
No. The `Settings` dataclass is frozen. All values are set once at import time. Restart the process to pick up changes.

**What takes precedence — .env file or environment variables?**
Environment variables. `python-dotenv` loads the `.env` file with `override=False`, so existing env vars are never overwritten.

**What happens if a required env var is missing?**
The server raises `RuntimeError` at startup with a message naming the missing variable. Required variables are `SUPERTABLE_ORGANIZATION` and `SUPERTABLE_SUPERUSER_TOKEN`.

**How do I see the effective configuration?**
Check the structured log at startup — the log level and service name are printed. For a complete dump, the WebUI admin page shows effective env values with secrets redacted.

**Can I use a .env file in production?**
Yes, but environment variables injected by your orchestrator (Docker, Kubernetes) are preferred. The `.env` file is primarily a convenience for local development.

**What is the difference between SUPERTABLE_SUPERTOKEN and SUPERTABLE_SUPERUSER_TOKEN?**
They are the same setting. `SUPERTABLE_SUPERUSER_TOKEN` is the canonical name; `SUPERTABLE_SUPERTOKEN` is a legacy alias. Both are checked in the fallback chain.

**How do I generate a session secret?**
Use any cryptographically random string of at least 32 characters: `python3 -c "import secrets; print(secrets.token_urlsafe(32))"`.
