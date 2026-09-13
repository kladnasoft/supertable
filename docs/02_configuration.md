# Configuration

The central configuration is the frozen `settings` instance in [config/settings.py](../supertable/config/settings.py). It is constructed when that module is imported. Set process environment variables before importing SuperTable.

## Load order and conversion

1. Import calls `find_dotenv(usecwd=True)` to discover a `.env` file from the current working-directory context.
2. If that file exists, `load_dotenv(..., override=False)` fills environment variables without replacing variables already present in the process.
3. `_build_settings()` reads the environment and constructs `Settings`.
4. Other modules import that shared instance. Changing the environment afterward does not rebuild it.

`DOTENV_PATH` is itself a setting; the loader does not use it to select the discovered `.env` file. Likewise, `load_defaults_from_env(env_file=None, prefer_system=True)` currently copies values from the existing settings instance: those arguments do not trigger a new file load or change precedence.

String helpers strip surrounding whitespace. Missing or empty values generally select the declared default; a whitespace-only string becomes empty after stripping. Invalid integers/floats fall back to defaults. Boolean values accept `1`, `true`, `yes`, `y`, `on` and `0`, `false`, `no`, `n`, `off`, case-insensitively; invalid values select the default. Numeric helpers do not generally validate positivity or ranges.

`SUPERTABLE_SUPER_META_CACHE_TTL_S` is special: only a positive float is retained; missing, invalid, zero, and negative values become `None`. `NO_COLOR` is true whenever the environment variable exists, including when its value is empty or `0`.

The loader uppercases `STORAGE_TYPE` and `SUPERTABLE_LOG_LEVEL`, and lowercases `SUPERTABLE_AUTH_MODE` and `SUPERTABLE_LOG_FORMAT`. Two aliases are read only when their primary values are empty:

| Primary setting | Alias |
| --- | --- |
| `SUPERTABLE_SUPERUSER_TOKEN` | `SUPERTABLE_SUPERTOKEN` |
| `SUPERTABLE_MCP_TOKEN` | `SUPERTABLE_MCP_AUTH_TOKEN` |

Constructing `Settings()` directly returns dataclass defaults and does not read the environment. The imported `settings` object is the environment-derived instance. One observable difference is `SUPERTABLE_API_HOST`: the dataclass default is empty, while `_build_settings()` supplies `0.0.0.0`.

## Start with local data and a direct Redis connection

```dotenv
SUPERTABLE_HOME=/var/lib/supertable
SUPERTABLE_ORGANIZATION=acme
STORAGE_TYPE=LOCAL
SUPERTABLE_REDIS_HOST=localhost
SUPERTABLE_REDIS_PORT=6379
SUPERTABLE_REDIS_DB=0
```

Local storage still uses Redis for metadata, locks, and access-control records. The organization is also passed explicitly to many library constructors. Authentication and role administration are described in [RBAC](11_rbac.md).

For an S3-compatible backend:

```dotenv
STORAGE_TYPE=S3
STORAGE_BUCKET=analytics
STORAGE_REGION=us-east-1
STORAGE_ENDPOINT_URL=https://objects.example.net
STORAGE_FORCE_PATH_STYLE=true
STORAGE_USE_SSL=true
SUPERTABLE_PREFIX=warehouse
```

Supply storage credentials through the backend's supported environment settings or provider credential mechanism. These examples select data access; they do not create application users or start servers. Backend-specific construction and credential precedence are in [storage](04_storage.md).

## Application home

[homedir.py](../supertable/config/homedir.py) expands `~`, makes `SUPERTABLE_HOME` absolute, creates the directory, and tests it by creating a temporary file. If it is not writable, it tries `<system-temp>/supertable`. If neither path is writable, it raises `RuntimeError`.

The resolved path is cached. Importing that module changes the process working directory to the resolved home. Local relative data paths and application-owned spill/cache paths consequently depend on this import-time behavior. `get_app_home()` returns the cached path. `change_to_app_home(path)` changes directory and logs failure instead of raising it.

`SUPERTABLE_PREFIX` is an object-storage base prefix. Redis prefixes are fixed constants, independent of this setting.

## Redis settings that the main catalog consumes

[RedisConnector](../supertable/redis_connector.py) uses `SUPERTABLE_REDIS_HOST`, `PORT`, `DB`, `PASSWORD`, `SSL`, and the Sentinel enablement/hosts/master/password settings. Its `RedisOptions` forces decoded responses and strict Sentinel discovery.

The main connector does not consume `SUPERTABLE_REDIS_URL`, `SUPERTABLE_REFLECTION_REDIS_URL`, or `SUPERTABLE_REDIS_USERNAME`. Although `SUPERTABLE_REDIS_SENTINEL_STRICT` is declared, the main options builder sets strict mode to true unconditionally. Set host/port/database/password explicitly for the catalog. The settings object's `effective_redis_url` property prefers the reflection URL and then the Redis URL, but that property is not used by this connector.

Sentinel hosts are comma-separated `host:port` entries; malformed entries are skipped. Sentinel password falls back to the Redis password. If Sentinel is enabled and only a Sentinel password is supplied, it is also used for the Redis master password. With hosts present, failed discovery raises after roughly three seconds; with no hosts, the connector falls back to direct Redis. SSL is passed on the direct branch, but not on its Sentinel branch.

## Runtime overrides

The [engine configuration resolver](../supertable/engine/engine_config.py) resolves stored Redis configuration first, then the current process environment, then built-in defaults. This resolver reads the environment at call time, unlike the central settings instance. It resolves shared routing thresholds plus per-engine DuckDB settings. `resolve_engine_config_provenance()` reports each value's source.

Memory strings support positive decimal values with optional `KB`, `MB`, `GB`, `TB`, or their `iB` forms, case-insensitively. Bare positive numbers are interpreted as GB. Invalid values select the caller's fallback. Nonpositive resolved thread counts and HTTP timeouts become `None`.

`DataWriter.configure_table()` stores positive `max_memory_chunk_size`, `max_overlapping_files`, and `max_tombstone_rows` overrides in Redis. Writer objects cache loaded table settings. The mutable `default` object in [config/defaults.py](../supertable/config/defaults.py) contains the core write thresholds, timing, log level, and storage type. Its `update_default(**kwargs)` updates known attributes and ignores unknown names; it does not update the frozen settings instance. Current writer lock calls use explicit 30-second leases and 60-second acquisition timeouts, independently of changes to the similarly named default fields.

See [writer](06_data_writer.md), [query engines](09_query_engine.md), and [reader](10_data_reader.md) for how these values are applied.

## Complete settings registry

The tables below list every central `Settings` field with its value from `_build_settings()` when no corresponding environment value is supplied. Empty means an empty string; `None` means no optional value. Sizes ending in `_BYTES` and `MAX_MEMORY_CHUNK_SIZE` are bytes; `_SEC`/`_S` and timeout fields use seconds unless the consumer explicitly documents otherwise.

These are declarations, not a guarantee that every field is consumed by every service. Backend and connector behavior above takes precedence over assumptions based on a setting's name. Service-facing fields are included because they are present in the current Python configuration class; server startup and endpoint behavior are outside this registry.

### Application and write defaults

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_HOME` | `str` | `~/supertable` |
| `SUPERTABLE_ORGANIZATION` | `str` | `empty` |
| `SUPERTABLE_PREFIX` | `str` | `empty` |
| `DOTENV_PATH` | `str` | `.env` |
| `MAX_MEMORY_CHUNK_SIZE` | `int` | `16777216` |
| `MAX_OVERLAPPING_FILES` | `int` | `100` |
| `MAX_TOMBSTONE_ROWS` | `int` | `1000000` |
| `DEFAULT_TIMEOUT_SEC` | `int` | `60` |
| `DEFAULT_LOCK_DURATION_SEC` | `int` | `30` |
| `IS_SHOW_TIMING` | `bool` | `true` |

### Storage backends

| Setting | Type | Default |
| --- | --- | --- |
| `STORAGE_TYPE` | `str` | `LOCAL` |
| `STORAGE_BUCKET` | `str` | `supertable` |
| `STORAGE_REGION` | `str` | `us-east-1` |
| `STORAGE_ENDPOINT_URL` | `str` | `empty` |
| `STORAGE_ACCESS_KEY` | `str` | `empty` |
| `STORAGE_SECRET_KEY` | `str` | `empty` |
| `STORAGE_SESSION_TOKEN` | `str` | `empty` |
| `STORAGE_FORCE_PATH_STYLE` | `bool` | `true` |
| `STORAGE_USE_SSL` | `bool` | `false` |
| `AZURE_STORAGE_ACCOUNT` | `str` | `empty` |
| `AZURE_CONTAINER` | `str` | `empty` |
| `AZURE_BLOB_ENDPOINT` | `str` | `empty` |
| `AZURE_STORAGE_CONNECTION_STRING` | `str` | `empty` |
| `AZURE_STORAGE_KEY` | `str` | `empty` |
| `AZURE_SAS_TOKEN` | `str` | `empty` |
| `GCS_BUCKET` | `str` | `empty` |
| `GOOGLE_APPLICATION_CREDENTIALS` | `str` | `empty` |
| `GCP_SA_JSON` | `str` | `empty` |
| `GCP_PROJECT` | `str` | `empty` |

### DuckDB and engine routing

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | `str` | `1GB` |
| `SUPERTABLE_DUCKDB_THREADS` | `str` | `empty` |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | `int` | `3` |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | `str` | `empty` |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | `bool` | `true` |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | `str` | `5GB` |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | `str` | `empty` |
| `SUPERTABLE_DUCKDB_MATERIALIZE` | `str` | `view` |
| `SUPERTABLE_DUCKDB_PRESIGNED` | `bool` | `false` |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | `bool` | `false` |
| `SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD` | `bool` | `false` |
| `SUPERTABLE_DUCKDB_WRITE_PROBE` | `bool` | `false` |
| `SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE` | `int` | `8` |
| `SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_TTL_SEC` | `int` | `300` |
| `SUPERTABLE_DEBUG_TIMINGS` | `bool` | `false` |
| `SUPERTABLE_ENGINE_LITE_MAX_BYTES` | `int` | `104857600` |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | `int` | `0` |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | `int` | `300` |
| `SUPERTABLE_DEFAULT_ENGINE` | `str` | `AUTO` |
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | `int` | `300` |
| `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | `int` | `120` |
| `SUPERTABLE_SPARK_CONNECT_TIMEOUT` | `int` | `30` |
| `SUPERTABLE_SPARK_BATCH_SIZE` | `int` | `50` |
| `SUPERTABLE_SPARK_PRESIGNED` | `bool` | `false` |

### Redis

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_REDIS_URL` | `str` | `empty` |
| `SUPERTABLE_REDIS_HOST` | `str` | `localhost` |
| `SUPERTABLE_REDIS_PORT` | `int` | `6379` |
| `SUPERTABLE_REDIS_DB` | `int` | `0` |
| `SUPERTABLE_REDIS_PASSWORD` | `str` | `empty` |
| `SUPERTABLE_REDIS_USERNAME` | `str` | `empty` |
| `SUPERTABLE_REDIS_SSL` | `bool` | `false` |
| `SUPERTABLE_REDIS_SENTINEL` | `bool` | `false` |
| `SUPERTABLE_REDIS_SENTINELS` | `str` | `empty` |
| `SUPERTABLE_REDIS_SENTINEL_MASTER` | `str` | `mymaster` |
| `SUPERTABLE_REDIS_SENTINEL_PASSWORD` | `str` | `empty` |
| `SUPERTABLE_REDIS_SENTINEL_STRICT` | `str` | `empty` |
| `SUPERTABLE_REFLECTION_REDIS_URL` | `str` | `empty` |

### API and authentication

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_API_HOST` | `str` | `0.0.0.0` |
| `SUPERTABLE_API_PORT` | `int` | `8051` |
| `UVICORN_RELOAD` | `bool` | `false` |
| `SUPERTABLE_PROXY_TIMEOUT` | `float` | `60.0` |
| `SUPERTABLE_AUTH_MODE` | `str` | `api_key` |
| `SUPERTABLE_AUTH_HEADER_NAME` | `str` | `X-API-Key` |
| `SUPERTABLE_API_KEY` | `str` | `empty` |
| `SUPERTABLE_BEARER_TOKEN` | `str` | `empty` |
| `SUPERTABLE_SUPERUSER_TOKEN` | `str` | `empty` |
| `SUPERTABLE_SESSION_SECRET` | `str` | `empty` |
| `SUPERTABLE_ROLE` | `str` | `empty` |
| `SUPERTABLE_LOGIN_MASK` | `int` | `1` |
| `SECURE_COOKIES` | `bool` | `false` |

### MCP, limits, and test configuration

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_MCP_TOKEN` | `str` | `empty` |
| `SUPERTABLE_MCP_PORT` | `int` | `8000` |
| `SUPERTABLE_DEFAULT_LIMIT` | `int` | `200` |
| `SUPERTABLE_MAX_LIMIT` | `int` | `5000` |
| `SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC` | `float` | `60.0` |
| `SUPERTABLE_MAX_CONCURRENCY` | `int` | `6` |
| `MCP_SERVER_PATH` | `str` | `mcp_server.py` |
| `MCP_WIRE` | `str` | `ndjson` |
| `SUPERTABLE_ALLOWED_HOSTS` | `str` | `*` |
| `SUPERTABLE_MCP_WEB_HOST` | `str` | `0.0.0.0` |
| `SUPERTABLE_MCP_WEB_PORT` | `int` | `8099` |
| `SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS` | `bool` | `false` |
| `SUPERTABLE_MCP_HTTP_TOKEN` | `str` | `empty` |
| `FORWARDED_ALLOW_IPS` | `str` | `*` |
| `SUPERTABLE_TEST_ORG` | `str` | `empty` |
| `SUPERTABLE_TEST_SUPER` | `str` | `empty` |
| `SUPERTABLE_TEST_ENGINE` | `str` | `empty` |
| `SUPERTABLE_TEST_TIMEOUT_SEC` | `float` | `0.0` |
| `XDG_CONFIG_HOME` | `str` | `~/.config (expanded home path)` |

### UI and OData

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_UI_HOST` | `str` | `0.0.0.0` |
| `SUPERTABLE_UI_PORT` | `int` | `8050` |
| `SUPERTABLE_STATIC_DIR` | `str` | `empty` |
| `SUPERTABLE_ODATA_BASE_URL` | `str` | `/api/v1/reflection` |
| `SUPERTABLE_ODATA_HOST` | `str` | `0.0.0.0` |
| `SUPERTABLE_ODATA_PORT` | `int` | `8052` |
| `SUPERTABLE_REFLECTION_STATE_DIR` | `str` | `/tmp/supertable_reflection` |
| `TEMPLATES_DIR` | `str` | `empty` |

### Logging, monitoring, and service limits

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_LOG_LEVEL` | `str` | `INFO` |
| `SUPERTABLE_LOG_FORMAT` | `str` | `json` |
| `SUPERTABLE_LOG_FILE` | `str` | `empty` |
| `SUPERTABLE_LOG_COLOR` | `str` | `empty` |
| `SUPERTABLE_CORRELATION_HEADER` | `str` | `X-Correlation-ID` |
| `NO_COLOR` | `bool` | `false` |
| `SUPERTABLE_MONITORING_ENABLED` | `bool` | `true` |
| `SUPERTABLE_MONITOR_CACHE_MAX` | `int` | `256` |
| `SUPERTABLE_API_RATE_LIMIT_ENABLED` | `bool` | `false` |
| `SUPERTABLE_API_RATE_LIMIT_RPM` | `int` | `300` |
| `SUPERTABLE_NOTEBOOK_PORT` | `int` | `8010` |

### Read caches and streaming

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_SUPER_META_CACHE_TTL_S` | `Optional[float]` | `None` |
| `SUPERTABLE_STATS_CACHE_MAX_TABLES` | `int` | `64` |
| `SUPERTABLE_TOMBSTONE_MAX_PARTS` | `int` | `100` |
| `SUPERTABLE_STREAM_BATCH_ROWS` | `int` | `65536` |
| `SUPERTABLE_STREAM_CHUNK_BYTES` | `int` | `33554432` |
| `SUPERTABLE_STREAM_MAX_AHEAD_CHUNKS` | `int` | `0` |
| `SUPERTABLE_STREAM_MAX_SPILL_BYTES` | `int` | `0` |
| `SUPERTABLE_STREAM_DEADLINE_SEC` | `int` | `3600` |
| `SUPERTABLE_STREAM_JOB_TTL_SEC` | `int` | `3600` |
| `SUPERTABLE_TOMBSTONE_CACHE_MAX_TABLES` | `int` | `64` |
| `SUPERTABLE_READ_PRUNING_ENABLED` | `bool` | `true` |
| `SUPERTABLE_READ_PROJECTION_SIZING_ENABLED` | `bool` | `true` |

### Audit and sharing

| Setting | Type | Default |
| --- | --- | --- |
| `SUPERTABLE_AUDIT_ENABLED` | `bool` | `false` |
| `SUPERTABLE_AUDIT_RETENTION_DAYS` | `int` | `2555` |
| `SUPERTABLE_AUDIT_BATCH_SIZE` | `int` | `1000` |
| `SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC` | `int` | `60` |
| `SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS` | `int` | `24` |
| `SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN` | `int` | `100000` |
| `SUPERTABLE_AUDIT_HASH_CHAIN` | `bool` | `true` |
| `SUPERTABLE_AUDIT_LOG_QUERIES` | `bool` | `true` |
| `SUPERTABLE_AUDIT_LOG_READS` | `bool` | `true` |
| `SUPERTABLE_AUDIT_ALERT_WEBHOOK` | `str` | `empty` |
| `SUPERTABLE_AUDIT_LEGAL_HOLD` | `bool` | `false` |
| `SUPERTABLE_AUDIT_FERNET_KEY` | `str` | `empty` |
| `SUPERTABLE_AUDIT_SIEM_ENABLED` | `bool` | `true` |
| `SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS` | `int` | `10` |
| `SUPERTABLE_SHARE_PRESIGN_TTL` | `int` | `14400` |
| `SUPERTABLE_SHARE_REFRESH_BUFFER` | `int` | `600` |

## Consumer-specific details

- `STORAGE_USE_SSL` is read by query-engine credential configuration; S3 and MinIO adapter construction derive their transport from the endpoint rather than passing this field as a universal TLS switch. Keep endpoint scheme and engine settings consistent.
- Azure's effective container selects `STORAGE_BUCKET` before `AZURE_CONTAINER`. The ordinary loader gives `STORAGE_BUCKET` a nonempty `supertable` default. GCS's effective bucket selects `GCS_BUCKET` first.
- `SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD=false` prevents the configured HTTPFS loader from installing an extension when loading it fails; it can still load an already installed extension.
- `SUPERTABLE_DUCKDB_WRITE_PROBE` gates an additional capability probe in the write path. Its default is false.
- `SUPERTABLE_SUPER_META_CACHE_TTL_S=None` resolves to a one-second metadata cache TTL in `MetaReader`. A cached result is reused only while both its root version matches and its expiry has not passed. The ordinary environment loader maps zero and negative values back to `None`, so those values also select the one-second fallback.
- `SUPERTABLE_STREAM_MAX_AHEAD_CHUNKS=0` and `SUPERTABLE_STREAM_MAX_SPILL_BYTES=0` disable their respective producer limits in the streaming runner.
- `SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS` controls age-based stream trimming; it is not a Redis key expiry attached to the entire stream. Audit configuration can also be overridden per organization in Redis.
- `SUPERTABLE_LOG_LEVEL` is validated against `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL` when constructing the mutable defaults; invalid values fall back to `INFO`.

See [audit](12_audit.md), [monitoring](14_monitoring.md), and [Redis layout](16_redis_layout.md) for the persisted runtime settings and state associated with these controls.
