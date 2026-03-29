# Supertable — Environment Variable Map

> Auto-generated from source scan of all `.py` files.
> Each entry lists the env var name, its default value (if any), and every source file that references it.

---

## Table of Contents

1. [Core / General](#1-core--general)
2. [API Server](#2-api-server)
3. [Authentication](#3-authentication)
4. [Redis](#4-redis)
5. [Storage (Generic S3-compatible)](#5-storage-generic-s3-compatible)
6. [Storage — Azure](#6-storage--azure)
7. [Storage — GCP](#7-storage--gcp)
8. [DuckDB Engine](#8-duckdb-engine)
9. [Spark Engine](#9-spark-engine)
10. [Engine Routing / Executor](#10-engine-routing--executor)
11. [MCP Server & Client](#11-mcp-server--client)
12. [MCP Web Server / Web App](#12-mcp-web-server--web-app)
13. [Reflection UI](#13-reflection-ui)
14. [Studio UI](#14-studio-ui)
15. [Notebook / Python Worker](#15-notebook--python-worker)
16. [Logging & Observability](#16-logging--observability)
17. [Monitoring](#17-monitoring)
18. [Vault & Encryption](#18-vault--encryption)
19. [Meta Reader / Caching](#19-meta-reader--caching)
20. [Miscellaneous](#20-miscellaneous)

---

## 1. Core / General

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_HOME` | `~/supertable` | `config/homedir.py`, `storage/azure_storage.py` |
| `STORAGE_TYPE` | `LOCAL` | `config/defaults.py`, `storage/storage_factory.py` |
| `MAX_MEMORY_CHUNK_SIZE` | `16777216` (16 MB) | `config/defaults.py` |
| `MAX_OVERLAPPING_FILES` | `100` | `config/defaults.py` |
| `DEFAULT_TIMEOUT_SEC` | `60` | `config/defaults.py` |
| `DEFAULT_LOCK_DURATION_SEC` | `30` | `config/defaults.py` |
| `IS_SHOW_TIMING` | `true` | `config/defaults.py` |
| `SUPERTABLE_PREFIX` | `""` | `storage/azure_storage.py`, `storage/gcp_storage.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |
| `DOTENV_PATH` | `.env` | `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_ORGANIZATION` | `""` | `mcp/mcp_client.py`, `reflection/common.py`, `studio/common.py` |

---

## 2. API Server

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_API_HOST` | `0.0.0.0` | `api/application.py`, `reflection/application.py` |
| `SUPERTABLE_API_PORT` | `8050` | `api/application.py`, `reflection/application.py` |
| `SUPERTABLE_HOST` | `0.0.0.0` | `api/application.py`, `mcp/mcp_server.py`, `studio/application.py` |
| `UVICORN_RELOAD` | `0` | `api/application.py`, `reflection/application.py`, `studio/application.py` |
| `SUPERTABLE_DEBUG_TIMINGS` | _(unset)_ | `api/api.py`, `meta_reader.py` |
| `SUPERTABLE_PROXY_TIMEOUT` | `60` | `reflection/application.py` |

---

## 3. Authentication

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_AUTH_MODE` | `api_key` | `api/auth.py`, `api/tests/test_api.py` |
| `SUPERTABLE_AUTH_HEADER_NAME` | `X-API-Key` | `api/auth.py`, `api/tests/test_api.py` |
| `SUPERTABLE_API_KEY` | _(required)_ | `api/auth.py`, `api/tests/test_api.py` |
| `SUPERTABLE_BEARER_TOKEN` | _(required if mode=bearer)_ | `api/auth.py` |
| `SUPERTABLE_SUPERUSER_TOKEN` | _(unset)_ | `mcp/web_app.py`, `reflection/common.py` |
| `SUPERTABLE_SUPERTOKEN` | `""` | `mcp/web_app.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_SUPERHASH` | `""` | `studio/common.py` |
| `SUPERTABLE_SESSION_SECRET` | `""` | `reflection/common.py`, `studio/common.py`, `studio/environments.py` |
| `SUPERTABLE_ADMIN_TOKEN` | _(unset)_ | `studio/common.py` |
| `SUPERTABLE_LOGIN_MASK` | `1` | `reflection/common.py`, `studio/common.py` |
| `SECURE_COOKIES` | `0` | `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_ROLE` | _(unset)_ | `mcp/mcp_server.py` |

---

## 4. Redis

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_REDIS_HOST` | `localhost` | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_PORT` | `6379` | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_DB` | `0` | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_PASSWORD` | _(unset)_ | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_SSL` | `false` | `redis_connector.py` |
| `SUPERTABLE_REDIS_URL` | _(unset)_ | `reflection/common.py`, `reflection/compute.py`, `studio/common.py`, `studio/compute.py` |
| `SUPERTABLE_REDIS_USERNAME` | _(unset)_ | `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_SENTINEL` | `false` | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_SENTINELS` | `""` | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_SENTINEL_MASTER` | `mymaster` | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_SENTINEL_PASSWORD` | _(falls back to REDIS_PASSWORD)_ | `redis_connector.py`, `reflection/common.py`, `studio/common.py` |
| `SUPERTABLE_REDIS_SENTINEL_STRICT` | _(unset)_ | `reflection/common.py`, `studio/common.py` |
| `REDIS_URL` | _(unset)_ | `reflection/compute.py`, `studio/compute.py` |
| `SUPERTABLE_REFLECTION_REDIS_URL` | _(unset)_ | `reflection/compute.py`, `studio/compute.py` |

---

## 5. Storage (Generic S3-compatible)

| Env Var | Default | Files |
|---------|---------|-------|
| `STORAGE_BUCKET` | `supertable` | `engine/data_estimator.py`, `engine/engine_common.py`, `storage/azure_storage.py`, `storage/gcp_storage.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |
| `STORAGE_ENDPOINT_URL` | _(unset)_ | `engine/data_estimator.py`, `engine/engine_common.py`, `storage/azure_storage.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |
| `STORAGE_ACCESS_KEY` | _(unset)_ | `engine/engine_common.py`, `storage/azure_storage.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |
| `STORAGE_SECRET_KEY` | _(unset)_ | `engine/engine_common.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |
| `STORAGE_SESSION_TOKEN` | _(unset)_ | `engine/engine_common.py`, `storage/s3_storage.py` |
| `STORAGE_REGION` | `us-east-1` | `engine/engine_common.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |
| `STORAGE_FORCE_PATH_STYLE` | `true` | `engine/engine_common.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |
| `STORAGE_USE_SSL` | `""` | `engine/data_estimator.py`, `engine/engine_common.py` |

---

## 6. Storage — Azure

| Env Var | Default | Files |
|---------|---------|-------|
| `AZURE_STORAGE_ACCOUNT` | `""` | `storage/azure_storage.py` |
| `AZURE_CONTAINER` | _(falls back to STORAGE_BUCKET)_ | `storage/azure_storage.py` |
| `AZURE_BLOB_ENDPOINT` | _(falls back to STORAGE_ENDPOINT_URL)_ | `storage/azure_storage.py` |
| `AZURE_STORAGE_CONNECTION_STRING` | `""` | `storage/azure_storage.py` |
| `AZURE_STORAGE_KEY` | _(falls back to STORAGE_ACCESS_KEY)_ | `storage/azure_storage.py` |
| `AZURE_SAS_TOKEN` | `""` | `storage/azure_storage.py` |

---

## 7. Storage — GCP

| Env Var | Default | Files |
|---------|---------|-------|
| `GCS_BUCKET` | _(falls back to STORAGE_BUCKET)_ | `storage/gcp_storage.py` |
| `GOOGLE_APPLICATION_CREDENTIALS` | `""` | `storage/gcp_storage.py` |
| `GCP_SA_JSON` | `""` | `storage/gcp_storage.py` |
| `GCP_PROJECT` | `""` | `storage/gcp_storage.py` |

---

## 8. DuckDB Engine

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_DUCKDB_MEMORY_LIMIT` | `1GB` | `engine/duckdb_pro.py`, `engine/engine_common.py`, `api/api.py` |
| `SUPERTABLE_DUCKDB_THREADS` | _(auto)_ | `engine/engine_common.py`, `api/api.py` |
| `SUPERTABLE_DUCKDB_IO_MULTIPLIER` | `3` | `engine/engine_common.py`, `api/api.py` |
| `SUPERTABLE_DUCKDB_HTTP_TIMEOUT` | _(unset)_ | `engine/engine_common.py`, `api/api.py` |
| `SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE` | `1` | `engine/engine_common.py` |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE` | `""` | `engine/engine_common.py`, `api/api.py` |
| `SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR` | `""` | `engine/engine_common.py` |
| `SUPERTABLE_DUCKDB_MATERIALIZE` | `view` | `engine/engine_common.py` |
| `SUPERTABLE_DUCKDB_PRESIGNED` | `""` | `engine/data_estimator.py` |
| `SUPERTABLE_DUCKDB_USE_HTTPFS` | `""` | `engine/data_estimator.py`, `engine/engine_common.py`, `storage/azure_storage.py`, `storage/gcp_storage.py`, `storage/minio_storage.py`, `storage/s3_storage.py` |

---

## 9. Spark Engine

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_SPARK_QUERY_TIMEOUT` | `300` | `engine/spark_thrift.py` |
| `SUPERTABLE_SPARK_STATEMENT_TIMEOUT` | `120` | `engine/spark_thrift.py` |
| `SUPERTABLE_SPARK_CONNECT_TIMEOUT` | `30` | `engine/spark_thrift.py` |
| `SUPERTABLE_SPARK_BATCH_SIZE` | `50` | `engine/spark_thrift.py` |
| `SPARK_MASTER_URL` | `spark://localhost:7078` | `infrastructure/spark_worker/spark_manager.py` |

---

## 10. Engine Routing / Executor

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_ENGINE_LITE_MAX_BYTES` | _(see executor.py)_ | `engine/executor.py`, `api/api.py` |
| `SUPERTABLE_ENGINE_SPARK_MIN_BYTES` | _(see executor.py)_ | `engine/executor.py`, `api/api.py` |
| `SUPERTABLE_ENGINE_FRESHNESS_SEC` | `300` | `engine/executor.py`, `api/api.py` |
| `SUPERTABLE_DEFAULT_ENGINE` | `AUTO` | `mcp/mcp_server.py` |

---

## 11. MCP Server & Client

| Env Var | Default | Files |
|---------|---------|-------|
| `MCP_SERVER_PATH` | `mcp_server.py` | `mcp/mcp_client.py`, `mcp/mcp_server.py`, `mcp/web_app.py`, `mcp/web_client.py` |
| `MCP_WIRE` | `ndjson` | `mcp/mcp_client.py` |
| `SUPERTABLE_MCP_TOKEN` | `""` | `mcp/mcp_client.py`, `mcp/mcp_server.py`, `mcp/web_app.py`, `mcp/web_client.py` |
| `SUPERTABLE_MCP_AUTH_TOKEN` | _(alias for MCP_TOKEN)_ | `mcp/mcp_client.py`, `mcp/mcp_server.py`, `mcp/web_app.py`, `mcp/web_client.py` |
| `SUPERTABLE_MCP_PORT` | `8000` | `mcp/mcp_server.py` |
| `SUPERTABLE_DEFAULT_LIMIT` | `200` | `mcp/mcp_server.py` |
| `SUPERTABLE_MAX_LIMIT` | `5000` | `mcp/mcp_server.py` |
| `SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC` | `60` | `mcp/mcp_server.py` |
| `SUPERTABLE_MAX_CONCURRENCY` | `6` | `mcp/mcp_server.py` |
| `SUPERTABLE_TEST_ORG` | `""` | `mcp/mcp_client.py` |
| `SUPERTABLE_TEST_SUPER` | `""` | `mcp/mcp_client.py` |
| `SUPERTABLE_TEST_ENGINE` | `""` | `mcp/mcp_client.py` |
| `SUPERTABLE_TEST_TIMEOUT_SEC` | `0` | `mcp/mcp_client.py` |
| `XDG_CONFIG_HOME` | `~/.config` | `mcp/mcp_client.py` |
| `_SUPERTABLE_DOTENV_LOADED` | _(internal flag)_ | `mcp/mcp_server.py` |

---

## 12. MCP Web Server / Web App

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_MCP_WEB_HOST` | `0.0.0.0` | `mcp/web_server.py` |
| `SUPERTABLE_MCP_WEB_PORT` | `8099` | `mcp/web_server.py` |
| `SUPERTABLE_MCP_HTTP_TOKEN` | _(alias for superuser token)_ | `mcp/web_app.py` |
| `SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS` | `""` | `mcp/web_app.py` |
| `SUPERTABLE_ALLOWED_HOSTS` | `*` | `mcp/web_app.py` |
| `FORWARDED_ALLOW_IPS` | `*` | `mcp/web_server.py` |

---

## 13. Reflection UI

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_UI_HOST` | `0.0.0.0` | `reflection/application.py` |
| `SUPERTABLE_UI_PORT` | `8051` | `reflection/application.py` |
| `SUPERTABLE_STATIC_DIR` | _(auto: `<pkg>/static`)_ | `reflection/application.py` |
| `SUPERTABLE_ODATA_BASE_URL` | `/api/v1/reflection` | `reflection/security.py` |
| `SUPERTABLE_REFLECTION_STATE_DIR` | `/tmp/supertable_reflection` | `reflection/compute.py`, `studio/common.py`, `studio/environments.py` |
| `TEMPLATES_DIR` | _(auto: `<pkg>/templates`)_ | `reflection/common.py`, `studio/common.py` |

---

## 14. Studio UI

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_REFLECTION_PORT` | `8050` | `studio/application.py` |
| `SUPERTABLE_DEFAULT_STAGES` | `""` | `studio/environments.py`, `studio/vault.py` |
| `SUPERTABLE_ENV_EXPORT_SECRET` | _(falls back to SESSION_SECRET)_ | `studio/environments.py` |

---

## 15. Notebook / Python Worker

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_NOTEBOOK_PORT` | `8010` | `infrastructure/python_worker/ws_server.py`, `reflection/compute.py`, `studio/compute.py` |
| `SUPERTABLE_NOTEBOOK_SESSION_TTL_SECONDS` | `""` | `infrastructure/python_worker/warm_pool_manager.py` |
| `SUPERTABLE_NOTEBOOK_STATEFUL_WS` | `1` | `infrastructure/python_worker/ws_server.py` |
| `SUPERTABLE_NOTEBOOK_ALLOWED_IMPORTS` | `math,re,json,datetime,...` | `studio/notebook.py`, `studio/studio.py` |
| `SUPERTABLE_LAB_ALLOWED_IMPORTS` | _(alias for above)_ | `studio/studio.py` |

---

## 16. Logging & Observability

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_LOG_LEVEL` | `INFO` | `config/defaults.py`, `logging.py`, `mcp/mcp_server.py` |
| `SUPERTABLE_LOG_FORMAT` | `json` | `logging.py` |
| `SUPERTABLE_LOG_FILE` | _(unset)_ | `logging.py` |
| `SUPERTABLE_LOG_COLOR` | _(unset)_ | `logging.py` |
| `SUPERTABLE_CORRELATION_HEADER` | `X-Correlation-ID` | `logging.py` |
| `NO_COLOR` | _(standard)_ | `logging.py` |

---

## 17. Monitoring

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_MONITORING_ENABLED` | `true` | `monitoring_writer.py` |
| `SUPERTABLE_MONITOR_CACHE_MAX` | `256` | `monitoring_writer.py` |

---

## 18. Vault & Encryption

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_VAULT_FERNET_KEY` | `""` | `studio/environments.py`, `studio/vault.py` |
| `SUPERTABLE_VAULT_MASTER_KEY` | `""` | `studio/environments.py`, `studio/vault.py` |

---

## 19. Meta Reader / Caching

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_SUPER_META_CACHE_TTL_S` | _(unset — no caching)_ | `meta_reader.py` |

---

## 20. Miscellaneous

| Env Var | Default | Files |
|---------|---------|-------|
| `SUPERTABLE_DUCKDB_MATERIALIZE` | `view` | `engine/engine_common.py` |
| `SUPERTABLE_DUCKDB_PRESIGNED` | `""` | `engine/data_estimator.py` |

---

## Summary Statistics

| Metric | Count |
|--------|-------|
| **Total unique env vars** | **~100** |
| **Source files referencing env vars** | **~40** (excluding tests) |
| **Sensitive / secret env vars** | `SUPERTABLE_API_KEY`, `SUPERTABLE_BEARER_TOKEN`, `SUPERTABLE_SUPERUSER_TOKEN`, `SUPERTABLE_SUPERTOKEN`, `SUPERTABLE_SUPERHASH`, `SUPERTABLE_SESSION_SECRET`, `SUPERTABLE_MCP_TOKEN`, `SUPERTABLE_MCP_AUTH_TOKEN`, `SUPERTABLE_MCP_HTTP_TOKEN`, `SUPERTABLE_ADMIN_TOKEN`, `SUPERTABLE_REDIS_PASSWORD`, `SUPERTABLE_REDIS_SENTINEL_PASSWORD`, `STORAGE_ACCESS_KEY`, `STORAGE_SECRET_KEY`, `STORAGE_SESSION_TOKEN`, `AZURE_STORAGE_CONNECTION_STRING`, `AZURE_STORAGE_KEY`, `AZURE_SAS_TOKEN`, `GCP_SA_JSON`, `SUPERTABLE_VAULT_FERNET_KEY`, `SUPERTABLE_VAULT_MASTER_KEY`, `SUPERTABLE_ENV_EXPORT_SECRET` |

> **Note:** All file paths are relative to `supertable/` root. Test files are excluded from the main map but do reference many of these vars via `patch.dict(os.environ, ...)`.
