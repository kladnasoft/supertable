# route: supertable.config.settings
"""
Centralised environment configuration for the entire Supertable platform.

This module is the **single source of truth** for every environment variable
used across all entrypoints (API, MCP, UI, OData, Python SDK, Notebook WS).

Import order safety
-------------------
This module deliberately has **zero** imports from ``supertable.*``.
It depends only on the standard library and ``python-dotenv``.  This
guarantees it can be imported first — before logging, before Redis,
before any other subsystem — without circular-import risk.

How it works
------------
1.  ``load_dotenv()`` runs exactly once at module level.
2.  A frozen ``Settings`` dataclass is constructed from ``os.getenv()``
    calls *at that moment*.  The dataclass is immutable; values never
    change at runtime (restart to pick up new values).
3.  Every other module imports ``settings`` from here instead of calling
    ``os.getenv()`` directly.  This eliminates scattered defaults and
    the mismatch bugs they cause.

Adding a new env var
--------------------
1.  Add a field to ``Settings`` with its type, default, and inline comment.
2.  Wire it in ``_build_settings()`` using one of the ``_env_*`` helpers.
3.  Replace the ``os.getenv()`` call in the consuming module with
    ``from supertable.config.settings import settings; settings.YOUR_FIELD``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv, find_dotenv

# ── .env loading (runs exactly once) ─────────────────────────────────
_dotenv_path = find_dotenv(usecwd=True)
if _dotenv_path and os.path.isfile(_dotenv_path):
    load_dotenv(_dotenv_path, override=False)

# ── Parsing helpers ──────────────────────────────────────────────────

_TRUTHY = frozenset({"1", "true", "yes", "y", "on"})
_FALSY = frozenset({"0", "false", "no", "n", "off"})


def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or default).strip()


def _env_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in _TRUTHY:
        return True
    if raw in _FALSY:
        return False
    return default


def _env_float_optional(name: str) -> Optional[float]:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return None
    try:
        val = float(raw)
        return val if val > 0 else None
    except ValueError:
        return None


# ── Settings dataclass ───────────────────────────────────────────────

@dataclass(frozen=True)
class Settings:
    """Immutable, process-wide configuration.

    Organised by subsystem. Every field maps 1:1 to an env var (listed in
    the inline comment). Defaults are chosen to match the documented
    production recommendations and to resolve all previously-identified
    mismatch bugs.
    """

    # ── Core ─────────────────────────────────────────────────────────
    SUPERTABLE_HOME: str = "~/supertable"         # SUPERTABLE_HOME
    SUPERTABLE_ORGANIZATION: str = ""            # SUPERTABLE_ORGANIZATION
    SUPERTABLE_PREFIX: str = ""                  # SUPERTABLE_PREFIX
    DOTENV_PATH: str = ".env"                    # DOTENV_PATH

    # ── Defaults (legacy config/defaults.py) ─────────────────────────
    MAX_MEMORY_CHUNK_SIZE: int = 16 * 1024 * 1024  # MAX_MEMORY_CHUNK_SIZE
    MAX_OVERLAPPING_FILES: int = 100               # MAX_OVERLAPPING_FILES
    DEFAULT_TIMEOUT_SEC: int = 60                  # DEFAULT_TIMEOUT_SEC
    DEFAULT_LOCK_DURATION_SEC: int = 30            # DEFAULT_LOCK_DURATION_SEC
    IS_SHOW_TIMING: bool = False                   # IS_SHOW_TIMING

    # ── Storage ──────────────────────────────────────────────────────
    STORAGE_TYPE: str = "LOCAL"                  # STORAGE_TYPE
    STORAGE_BUCKET: str = "supertable"           # STORAGE_BUCKET  (unified default)
    STORAGE_REGION: str = "eu-central-1"         # STORAGE_REGION  (unified default)
    STORAGE_ENDPOINT_URL: str = ""               # STORAGE_ENDPOINT_URL
    STORAGE_ACCESS_KEY: str = ""                 # STORAGE_ACCESS_KEY
    STORAGE_SECRET_KEY: str = ""                 # STORAGE_SECRET_KEY
    STORAGE_SESSION_TOKEN: str = ""              # STORAGE_SESSION_TOKEN
    STORAGE_FORCE_PATH_STYLE: bool = True        # STORAGE_FORCE_PATH_STYLE  (unified: True)
    STORAGE_USE_SSL: bool = False                # STORAGE_USE_SSL

    # ── Storage — Azure ──────────────────────────────────────────────
    AZURE_STORAGE_ACCOUNT: str = ""              # AZURE_STORAGE_ACCOUNT
    AZURE_CONTAINER: str = ""                    # AZURE_CONTAINER
    AZURE_BLOB_ENDPOINT: str = ""                # AZURE_BLOB_ENDPOINT
    AZURE_STORAGE_CONNECTION_STRING: str = ""    # AZURE_STORAGE_CONNECTION_STRING
    AZURE_STORAGE_KEY: str = ""                  # AZURE_STORAGE_KEY
    AZURE_SAS_TOKEN: str = ""                    # AZURE_SAS_TOKEN

    # ── Storage — GCP ────────────────────────────────────────────────
    GCS_BUCKET: str = ""                         # GCS_BUCKET
    GOOGLE_APPLICATION_CREDENTIALS: str = ""     # GOOGLE_APPLICATION_CREDENTIALS
    GCP_SA_JSON: str = ""                        # GCP_SA_JSON
    GCP_PROJECT: str = ""                        # GCP_PROJECT

    # ── DuckDB Engine ────────────────────────────────────────────────
    SUPERTABLE_DUCKDB_MEMORY_LIMIT: str = "1GB"    # SUPERTABLE_DUCKDB_MEMORY_LIMIT
    SUPERTABLE_DUCKDB_THREADS: str = ""            # SUPERTABLE_DUCKDB_THREADS
    SUPERTABLE_DUCKDB_IO_MULTIPLIER: int = 3       # SUPERTABLE_DUCKDB_IO_MULTIPLIER
    SUPERTABLE_DUCKDB_HTTP_TIMEOUT: str = ""        # SUPERTABLE_DUCKDB_HTTP_TIMEOUT
    SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE: bool = True  # SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE
    SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE: str = ""     # SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE
    SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR: str = ""      # SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR
    SUPERTABLE_DUCKDB_MATERIALIZE: str = "view"    # SUPERTABLE_DUCKDB_MATERIALIZE
    SUPERTABLE_DUCKDB_PRESIGNED: bool = False      # SUPERTABLE_DUCKDB_PRESIGNED
    SUPERTABLE_DUCKDB_USE_HTTPFS: bool = False     # SUPERTABLE_DUCKDB_USE_HTTPFS
    SUPERTABLE_DEBUG_TIMINGS: bool = False          # SUPERTABLE_DEBUG_TIMINGS

    # ── Engine Routing / Executor ────────────────────────────────────
    SUPERTABLE_ENGINE_LITE_MAX_BYTES: int = 100 * 1024 * 1024    # SUPERTABLE_ENGINE_LITE_MAX_BYTES (100 MB)
    SUPERTABLE_ENGINE_SPARK_MIN_BYTES: int = 10 * 1024 * 1024 * 1024  # SUPERTABLE_ENGINE_SPARK_MIN_BYTES (10 GB)
    SUPERTABLE_ENGINE_FRESHNESS_SEC: int = 300     # SUPERTABLE_ENGINE_FRESHNESS_SEC
    SUPERTABLE_DEFAULT_ENGINE: str = "AUTO"        # SUPERTABLE_DEFAULT_ENGINE

    # ── Spark Engine ─────────────────────────────────────────────────
    SUPERTABLE_SPARK_QUERY_TIMEOUT: int = 300      # SUPERTABLE_SPARK_QUERY_TIMEOUT
    SUPERTABLE_SPARK_STATEMENT_TIMEOUT: int = 120  # SUPERTABLE_SPARK_STATEMENT_TIMEOUT
    SUPERTABLE_SPARK_CONNECT_TIMEOUT: int = 30     # SUPERTABLE_SPARK_CONNECT_TIMEOUT
    SUPERTABLE_SPARK_BATCH_SIZE: int = 50          # SUPERTABLE_SPARK_BATCH_SIZE

    # ── Redis ────────────────────────────────────────────────────────
    SUPERTABLE_REDIS_URL: str = ""               # SUPERTABLE_REDIS_URL
    SUPERTABLE_REDIS_HOST: str = "localhost"      # SUPERTABLE_REDIS_HOST
    SUPERTABLE_REDIS_PORT: int = 6379             # SUPERTABLE_REDIS_PORT
    SUPERTABLE_REDIS_DB: int = 0                  # SUPERTABLE_REDIS_DB
    SUPERTABLE_REDIS_PASSWORD: str = ""           # SUPERTABLE_REDIS_PASSWORD
    SUPERTABLE_REDIS_USERNAME: str = ""           # SUPERTABLE_REDIS_USERNAME
    SUPERTABLE_REDIS_SSL: bool = False            # SUPERTABLE_REDIS_SSL
    SUPERTABLE_REDIS_SENTINEL: bool = False       # SUPERTABLE_REDIS_SENTINEL
    SUPERTABLE_REDIS_SENTINELS: str = ""          # SUPERTABLE_REDIS_SENTINELS
    SUPERTABLE_REDIS_SENTINEL_MASTER: str = "mymaster"  # SUPERTABLE_REDIS_SENTINEL_MASTER
    SUPERTABLE_REDIS_SENTINEL_PASSWORD: str = ""  # SUPERTABLE_REDIS_SENTINEL_PASSWORD
    SUPERTABLE_REDIS_SENTINEL_STRICT: str = ""    # SUPERTABLE_REDIS_SENTINEL_STRICT
    SUPERTABLE_REFLECTION_REDIS_URL: str = ""     # SUPERTABLE_REFLECTION_REDIS_URL

    # ── API Server ───────────────────────────────────────────────────
    SUPERTABLE_API_HOST: str = ""                # SUPERTABLE_API_HOST  (falls back to SUPERTABLE_HOST)
    SUPERTABLE_API_PORT: int = 8051              # SUPERTABLE_API_PORT
    UVICORN_RELOAD: bool = False                 # UVICORN_RELOAD
    SUPERTABLE_PROXY_TIMEOUT: float = 60.0       # SUPERTABLE_PROXY_TIMEOUT

    # ── Auth ─────────────────────────────────────────────────────────
    SUPERTABLE_AUTH_MODE: str = "api_key"         # SUPERTABLE_AUTH_MODE
    SUPERTABLE_AUTH_HEADER_NAME: str = "X-API-Key"  # SUPERTABLE_AUTH_HEADER_NAME
    SUPERTABLE_API_KEY: str = ""                  # SUPERTABLE_API_KEY
    SUPERTABLE_BEARER_TOKEN: str = ""             # SUPERTABLE_BEARER_TOKEN
    SUPERTABLE_SUPERUSER_TOKEN: str = ""          # SUPERTABLE_SUPERUSER_TOKEN → SUPERTABLE_SUPERTOKEN (unified)
    SUPERTABLE_SESSION_SECRET: str = ""           # SUPERTABLE_SESSION_SECRET
    SUPERTABLE_ROLE: str = ""                     # SUPERTABLE_ROLE
    SUPERTABLE_LOGIN_MASK: int = 1                # SUPERTABLE_LOGIN_MASK
    SECURE_COOKIES: bool = False                  # SECURE_COOKIES

    # ── MCP Server ───────────────────────────────────────────────────
    SUPERTABLE_MCP_TOKEN: str = ""               # SUPERTABLE_MCP_TOKEN → SUPERTABLE_MCP_AUTH_TOKEN (unified)
    SUPERTABLE_MCP_PORT: int = 8000              # SUPERTABLE_MCP_PORT
    SUPERTABLE_DEFAULT_LIMIT: int = 200          # SUPERTABLE_DEFAULT_LIMIT
    SUPERTABLE_MAX_LIMIT: int = 5000             # SUPERTABLE_MAX_LIMIT
    SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC: float = 60.0  # SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC
    SUPERTABLE_MAX_CONCURRENCY: int = 6          # SUPERTABLE_MAX_CONCURRENCY
    MCP_SERVER_PATH: str = "mcp_server.py"       # MCP_SERVER_PATH
    MCP_WIRE: str = "ndjson"                     # MCP_WIRE
    SUPERTABLE_ALLOWED_HOSTS: str = "*"          # SUPERTABLE_ALLOWED_HOSTS

    # ── MCP Web Server ───────────────────────────────────────────────
    SUPERTABLE_MCP_WEB_HOST: str = "0.0.0.0"     # SUPERTABLE_MCP_WEB_HOST
    SUPERTABLE_MCP_WEB_PORT: int = 8099          # SUPERTABLE_MCP_WEB_PORT
    SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS: bool = False  # SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS
    SUPERTABLE_MCP_HTTP_TOKEN: str = ""          # SUPERTABLE_MCP_HTTP_TOKEN
    FORWARDED_ALLOW_IPS: str = "*"               # FORWARDED_ALLOW_IPS

    # ── MCP Client (test/CLI) ────────────────────────────────────────
    SUPERTABLE_TEST_ORG: str = ""                # SUPERTABLE_TEST_ORG
    SUPERTABLE_TEST_SUPER: str = ""              # SUPERTABLE_TEST_SUPER
    SUPERTABLE_TEST_ENGINE: str = ""             # SUPERTABLE_TEST_ENGINE
    SUPERTABLE_TEST_TIMEOUT_SEC: float = 0.0     # SUPERTABLE_TEST_TIMEOUT_SEC
    XDG_CONFIG_HOME: str = ""                    # XDG_CONFIG_HOME

    # ── Reflection UI ────────────────────────────────────────────────
    SUPERTABLE_UI_HOST: str = "0.0.0.0"          # SUPERTABLE_UI_HOST
    SUPERTABLE_UI_PORT: int = 8050               # SUPERTABLE_UI_PORT
    SUPERTABLE_STATIC_DIR: str = ""              # SUPERTABLE_STATIC_DIR
    SUPERTABLE_ODATA_BASE_URL: str = "/api/v1/reflection"  # SUPERTABLE_ODATA_BASE_URL
    SUPERTABLE_ODATA_HOST: str = "0.0.0.0"               # SUPERTABLE_ODATA_HOST
    SUPERTABLE_ODATA_PORT: int = 8052                     # SUPERTABLE_ODATA_PORT
    SUPERTABLE_REFLECTION_STATE_DIR: str = "/tmp/supertable_reflection"  # SUPERTABLE_REFLECTION_STATE_DIR
    TEMPLATES_DIR: str = ""                      # TEMPLATES_DIR

    # ── Logging ──────────────────────────────────────────────────────
    SUPERTABLE_LOG_LEVEL: str = "INFO"           # SUPERTABLE_LOG_LEVEL
    SUPERTABLE_LOG_FORMAT: str = "json"          # SUPERTABLE_LOG_FORMAT
    SUPERTABLE_LOG_FILE: str = ""                # SUPERTABLE_LOG_FILE
    SUPERTABLE_LOG_COLOR: str = ""               # SUPERTABLE_LOG_COLOR
    SUPERTABLE_CORRELATION_HEADER: str = "X-Correlation-ID"  # SUPERTABLE_CORRELATION_HEADER
    NO_COLOR: bool = False                       # NO_COLOR (standard)

    # ── Monitoring ───────────────────────────────────────────────────
    SUPERTABLE_MONITORING_ENABLED: bool = True    # SUPERTABLE_MONITORING_ENABLED
    SUPERTABLE_MONITOR_CACHE_MAX: int = 256      # SUPERTABLE_MONITOR_CACHE_MAX

    # ── Notebook / Python Worker ─────────────────────────────────────
    SUPERTABLE_NOTEBOOK_PORT: int = 8010         # SUPERTABLE_NOTEBOOK_PORT  (unified: 8010)

    # ── Meta Reader / Caching ────────────────────────────────────────
    SUPERTABLE_SUPER_META_CACHE_TTL_S: Optional[float] = None  # SUPERTABLE_SUPER_META_CACHE_TTL_S

    # ── Audit ────────────────────────────────────────────────────────
    SUPERTABLE_AUDIT_ENABLED: bool = True                    # SUPERTABLE_AUDIT_ENABLED
    SUPERTABLE_AUDIT_RETENTION_DAYS: int = 2555              # SUPERTABLE_AUDIT_RETENTION_DAYS (~7 years)
    SUPERTABLE_AUDIT_BATCH_SIZE: int = 1000                  # SUPERTABLE_AUDIT_BATCH_SIZE
    SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC: int = 60            # SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC
    SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS: int = 24        # SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS
    SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN: int = 100000       # SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN
    SUPERTABLE_AUDIT_HASH_CHAIN: bool = True                 # SUPERTABLE_AUDIT_HASH_CHAIN
    SUPERTABLE_AUDIT_LOG_QUERIES: bool = True                # SUPERTABLE_AUDIT_LOG_QUERIES
    SUPERTABLE_AUDIT_LOG_READS: bool = True                  # SUPERTABLE_AUDIT_LOG_READS
    SUPERTABLE_AUDIT_ALERT_WEBHOOK: str = ""                 # SUPERTABLE_AUDIT_ALERT_WEBHOOK
    SUPERTABLE_AUDIT_LEGAL_HOLD: bool = False                # SUPERTABLE_AUDIT_LEGAL_HOLD
    SUPERTABLE_AUDIT_FERNET_KEY: str = ""                    # SUPERTABLE_AUDIT_FERNET_KEY
    SUPERTABLE_AUDIT_SIEM_ENABLED: bool = True               # SUPERTABLE_AUDIT_SIEM_ENABLED
    SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS: int = 10            # SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS


    # ── Convenience properties ───────────────────────────────────────

    @property
    def effective_api_host(self) -> str:
        """API bind address: SUPERTABLE_API_HOST."""
        return self.SUPERTABLE_API_HOST

    @property
    def effective_redis_url(self) -> Optional[str]:
        """Redis URL: SUPERTABLE_REFLECTION_REDIS_URL → SUPERTABLE_REDIS_URL → REDIS_URL."""
        return (
            self.SUPERTABLE_REFLECTION_REDIS_URL
            or self.SUPERTABLE_REDIS_URL
            or None
        )

    @property
    def effective_redis_sentinel_password(self) -> str:
        """Sentinel password: SUPERTABLE_REDIS_SENTINEL_PASSWORD → SUPERTABLE_REDIS_PASSWORD."""
        return self.SUPERTABLE_REDIS_SENTINEL_PASSWORD or self.SUPERTABLE_REDIS_PASSWORD

    @property
    def effective_storage_bucket(self) -> str:
        """For Azure: STORAGE_BUCKET → AZURE_CONTAINER → 'supertable'."""
        return self.STORAGE_BUCKET or self.AZURE_CONTAINER or "supertable"

    @property
    def effective_storage_endpoint(self) -> str:
        """For Azure: STORAGE_ENDPOINT_URL → AZURE_BLOB_ENDPOINT."""
        return self.STORAGE_ENDPOINT_URL or self.AZURE_BLOB_ENDPOINT

    @property
    def effective_storage_access_key(self) -> str:
        """For Azure: STORAGE_ACCESS_KEY → AZURE_STORAGE_KEY."""
        return self.STORAGE_ACCESS_KEY or self.AZURE_STORAGE_KEY

    @property
    def effective_gcs_bucket(self) -> str:
        """For GCP: GCS_BUCKET → STORAGE_BUCKET → 'supertable'."""
        return self.GCS_BUCKET or self.STORAGE_BUCKET or "supertable"


# ── Builder ──────────────────────────────────────────────────────────

def _build_settings() -> Settings:
    """Construct Settings from the environment (called once at import time)."""

    # Superuser token: unified fallback chain
    superuser_token = (
        _env_str("SUPERTABLE_SUPERUSER_TOKEN")
        or _env_str("SUPERTABLE_SUPERTOKEN")
        or ""
    )

    # MCP auth token: unified fallback chain
    mcp_token = (
        _env_str("SUPERTABLE_MCP_TOKEN")
        or _env_str("SUPERTABLE_MCP_AUTH_TOKEN")
        or ""
    )

    # API host fallback
    api_host = _env_str("SUPERTABLE_API_HOST") or "0.0.0.0"

    # Meta cache TTL
    meta_ttl = _env_float_optional("SUPERTABLE_SUPER_META_CACHE_TTL_S")

    # XDG fallback
    xdg = _env_str("XDG_CONFIG_HOME") or os.path.join(
        os.path.expanduser("~"), ".config"
    )


    return Settings(
        # ── Core ─────────────────────────────────────────────────────
        SUPERTABLE_HOME=_env_str("SUPERTABLE_HOME", "~/supertable"),
        SUPERTABLE_ORGANIZATION=_env_str("SUPERTABLE_ORGANIZATION"),
        SUPERTABLE_PREFIX=_env_str("SUPERTABLE_PREFIX"),
        DOTENV_PATH=_env_str("DOTENV_PATH", ".env"),

        # ── Defaults ─────────────────────────────────────────────────
        MAX_MEMORY_CHUNK_SIZE=_env_int("MAX_MEMORY_CHUNK_SIZE", 16 * 1024 * 1024),
        MAX_OVERLAPPING_FILES=_env_int("MAX_OVERLAPPING_FILES", 100),
        DEFAULT_TIMEOUT_SEC=_env_int("DEFAULT_TIMEOUT_SEC", 60),
        DEFAULT_LOCK_DURATION_SEC=_env_int("DEFAULT_LOCK_DURATION_SEC", 30),
        IS_SHOW_TIMING=_env_bool("IS_SHOW_TIMING", True),

        # ── Storage ──────────────────────────────────────────────────
        STORAGE_TYPE=_env_str("STORAGE_TYPE", "LOCAL").upper(),
        STORAGE_BUCKET=_env_str("STORAGE_BUCKET", "supertable"),
        STORAGE_REGION=_env_str("STORAGE_REGION", "us-east-1"),
        STORAGE_ENDPOINT_URL=_env_str("STORAGE_ENDPOINT_URL"),
        STORAGE_ACCESS_KEY=_env_str("STORAGE_ACCESS_KEY"),
        STORAGE_SECRET_KEY=_env_str("STORAGE_SECRET_KEY"),
        STORAGE_SESSION_TOKEN=_env_str("STORAGE_SESSION_TOKEN"),
        STORAGE_FORCE_PATH_STYLE=_env_bool("STORAGE_FORCE_PATH_STYLE", True),
        STORAGE_USE_SSL=_env_bool("STORAGE_USE_SSL", False),

        # ── Storage — Azure ──────────────────────────────────────────
        AZURE_STORAGE_ACCOUNT=_env_str("AZURE_STORAGE_ACCOUNT"),
        AZURE_CONTAINER=_env_str("AZURE_CONTAINER"),
        AZURE_BLOB_ENDPOINT=_env_str("AZURE_BLOB_ENDPOINT"),
        AZURE_STORAGE_CONNECTION_STRING=_env_str("AZURE_STORAGE_CONNECTION_STRING"),
        AZURE_STORAGE_KEY=_env_str("AZURE_STORAGE_KEY"),
        AZURE_SAS_TOKEN=_env_str("AZURE_SAS_TOKEN"),

        # ── Storage — GCP ────────────────────────────────────────────
        GCS_BUCKET=_env_str("GCS_BUCKET"),
        GOOGLE_APPLICATION_CREDENTIALS=_env_str("GOOGLE_APPLICATION_CREDENTIALS"),
        GCP_SA_JSON=_env_str("GCP_SA_JSON"),
        GCP_PROJECT=_env_str("GCP_PROJECT"),

        # ── DuckDB Engine ────────────────────────────────────────────
        SUPERTABLE_DUCKDB_MEMORY_LIMIT=_env_str("SUPERTABLE_DUCKDB_MEMORY_LIMIT", "1GB"),
        SUPERTABLE_DUCKDB_THREADS=_env_str("SUPERTABLE_DUCKDB_THREADS"),
        SUPERTABLE_DUCKDB_IO_MULTIPLIER=_env_int("SUPERTABLE_DUCKDB_IO_MULTIPLIER", 3),
        SUPERTABLE_DUCKDB_HTTP_TIMEOUT=_env_str("SUPERTABLE_DUCKDB_HTTP_TIMEOUT"),
        SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE=_env_bool("SUPERTABLE_DUCKDB_HTTP_METADATA_CACHE", True),
        SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE=_env_str("SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE"),
        SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR=_env_str("SUPERTABLE_DUCKDB_EXTERNAL_CACHE_DIR"),
        SUPERTABLE_DUCKDB_MATERIALIZE=_env_str("SUPERTABLE_DUCKDB_MATERIALIZE", "view"),
        SUPERTABLE_DUCKDB_PRESIGNED=_env_bool("SUPERTABLE_DUCKDB_PRESIGNED", False),
        SUPERTABLE_DUCKDB_USE_HTTPFS=_env_bool("SUPERTABLE_DUCKDB_USE_HTTPFS", False),
        SUPERTABLE_DEBUG_TIMINGS=_env_bool("SUPERTABLE_DEBUG_TIMINGS", False),

        # ── Engine Routing ───────────────────────────────────────────
        SUPERTABLE_ENGINE_LITE_MAX_BYTES=_env_int("SUPERTABLE_ENGINE_LITE_MAX_BYTES", 100 * 1024 * 1024),
        SUPERTABLE_ENGINE_SPARK_MIN_BYTES=_env_int("SUPERTABLE_ENGINE_SPARK_MIN_BYTES", 10 * 1024 * 1024 * 1024),
        SUPERTABLE_ENGINE_FRESHNESS_SEC=_env_int("SUPERTABLE_ENGINE_FRESHNESS_SEC", 300),
        SUPERTABLE_DEFAULT_ENGINE=_env_str("SUPERTABLE_DEFAULT_ENGINE", "AUTO"),

        # ── Spark Engine ─────────────────────────────────────────────
        SUPERTABLE_SPARK_QUERY_TIMEOUT=_env_int("SUPERTABLE_SPARK_QUERY_TIMEOUT", 300),
        SUPERTABLE_SPARK_STATEMENT_TIMEOUT=_env_int("SUPERTABLE_SPARK_STATEMENT_TIMEOUT", 120),
        SUPERTABLE_SPARK_CONNECT_TIMEOUT=_env_int("SUPERTABLE_SPARK_CONNECT_TIMEOUT", 30),
        SUPERTABLE_SPARK_BATCH_SIZE=_env_int("SUPERTABLE_SPARK_BATCH_SIZE", 50),

        # ── Redis ────────────────────────────────────────────────────
        SUPERTABLE_REDIS_URL=_env_str("SUPERTABLE_REDIS_URL"),
        SUPERTABLE_REDIS_HOST=_env_str("SUPERTABLE_REDIS_HOST", "localhost"),
        SUPERTABLE_REDIS_PORT=_env_int("SUPERTABLE_REDIS_PORT", 6379),
        SUPERTABLE_REDIS_DB=_env_int("SUPERTABLE_REDIS_DB", 0),
        SUPERTABLE_REDIS_PASSWORD=_env_str("SUPERTABLE_REDIS_PASSWORD"),
        SUPERTABLE_REDIS_USERNAME=_env_str("SUPERTABLE_REDIS_USERNAME"),
        SUPERTABLE_REDIS_SSL=_env_bool("SUPERTABLE_REDIS_SSL", False),
        SUPERTABLE_REDIS_SENTINEL=_env_bool("SUPERTABLE_REDIS_SENTINEL", False),
        SUPERTABLE_REDIS_SENTINELS=_env_str("SUPERTABLE_REDIS_SENTINELS"),
        SUPERTABLE_REDIS_SENTINEL_MASTER=_env_str("SUPERTABLE_REDIS_SENTINEL_MASTER", "mymaster"),
        SUPERTABLE_REDIS_SENTINEL_PASSWORD=_env_str("SUPERTABLE_REDIS_SENTINEL_PASSWORD"),
        SUPERTABLE_REDIS_SENTINEL_STRICT=_env_str("SUPERTABLE_REDIS_SENTINEL_STRICT"),

        SUPERTABLE_REFLECTION_REDIS_URL=_env_str("SUPERTABLE_REFLECTION_REDIS_URL"),

        # ── API Server ───────────────────────────────────────────────
        SUPERTABLE_API_HOST=api_host,
        SUPERTABLE_API_PORT=_env_int("SUPERTABLE_API_PORT", 8051),
        UVICORN_RELOAD=_env_bool("UVICORN_RELOAD", False),
        SUPERTABLE_PROXY_TIMEOUT=_env_float("SUPERTABLE_PROXY_TIMEOUT", 60.0),

        # ── Auth ─────────────────────────────────────────────────────
        SUPERTABLE_AUTH_MODE=_env_str("SUPERTABLE_AUTH_MODE", "api_key").lower(),
        SUPERTABLE_AUTH_HEADER_NAME=_env_str("SUPERTABLE_AUTH_HEADER_NAME", "X-API-Key"),
        SUPERTABLE_API_KEY=_env_str("SUPERTABLE_API_KEY"),
        SUPERTABLE_BEARER_TOKEN=_env_str("SUPERTABLE_BEARER_TOKEN"),
        SUPERTABLE_SUPERUSER_TOKEN=superuser_token,
        SUPERTABLE_SESSION_SECRET=_env_str("SUPERTABLE_SESSION_SECRET"),
        SUPERTABLE_ROLE=_env_str("SUPERTABLE_ROLE"),
        SUPERTABLE_LOGIN_MASK=_env_int("SUPERTABLE_LOGIN_MASK", 1),
        SECURE_COOKIES=_env_bool("SECURE_COOKIES", False),

        # ── MCP Server ───────────────────────────────────────────────
        SUPERTABLE_MCP_TOKEN=mcp_token,
        SUPERTABLE_MCP_PORT=_env_int("SUPERTABLE_MCP_PORT", 8000),
        SUPERTABLE_DEFAULT_LIMIT=_env_int("SUPERTABLE_DEFAULT_LIMIT", 200),
        SUPERTABLE_MAX_LIMIT=_env_int("SUPERTABLE_MAX_LIMIT", 5000),
        SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC=_env_float("SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC", 60.0),
        SUPERTABLE_MAX_CONCURRENCY=_env_int("SUPERTABLE_MAX_CONCURRENCY", 6),
        MCP_SERVER_PATH=_env_str("MCP_SERVER_PATH", "mcp_server.py"),
        MCP_WIRE=_env_str("MCP_WIRE", "ndjson"),
        SUPERTABLE_ALLOWED_HOSTS=_env_str("SUPERTABLE_ALLOWED_HOSTS", "*"),

        # ── MCP Web Server ───────────────────────────────────────────
        SUPERTABLE_MCP_WEB_HOST=_env_str("SUPERTABLE_MCP_WEB_HOST", "0.0.0.0"),
        SUPERTABLE_MCP_WEB_PORT=_env_int("SUPERTABLE_MCP_WEB_PORT", 8099),
        SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS=_env_bool("SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS", False),
        SUPERTABLE_MCP_HTTP_TOKEN=_env_str("SUPERTABLE_MCP_HTTP_TOKEN"),
        FORWARDED_ALLOW_IPS=_env_str("FORWARDED_ALLOW_IPS", "*"),

        # ── MCP Client (test/CLI) ────────────────────────────────────
        SUPERTABLE_TEST_ORG=_env_str("SUPERTABLE_TEST_ORG"),
        SUPERTABLE_TEST_SUPER=_env_str("SUPERTABLE_TEST_SUPER"),
        SUPERTABLE_TEST_ENGINE=_env_str("SUPERTABLE_TEST_ENGINE"),
        SUPERTABLE_TEST_TIMEOUT_SEC=_env_float("SUPERTABLE_TEST_TIMEOUT_SEC", 0.0),
        XDG_CONFIG_HOME=xdg,

        # ── Reflection UI ────────────────────────────────────────────
        SUPERTABLE_UI_HOST=_env_str("SUPERTABLE_UI_HOST", "0.0.0.0"),
        SUPERTABLE_UI_PORT=_env_int("SUPERTABLE_UI_PORT", 8050),
        SUPERTABLE_STATIC_DIR=_env_str("SUPERTABLE_STATIC_DIR"),
        SUPERTABLE_ODATA_BASE_URL=_env_str("SUPERTABLE_ODATA_BASE_URL", "/api/v1/reflection"),
        SUPERTABLE_ODATA_HOST=_env_str("SUPERTABLE_ODATA_HOST") or "0.0.0.0",
        SUPERTABLE_ODATA_PORT=_env_int("SUPERTABLE_ODATA_PORT", 8052),
        SUPERTABLE_REFLECTION_STATE_DIR=_env_str("SUPERTABLE_REFLECTION_STATE_DIR", "/tmp/supertable_reflection"),
        TEMPLATES_DIR=_env_str("TEMPLATES_DIR"),

        # ── Logging ──────────────────────────────────────────────────
        SUPERTABLE_LOG_LEVEL=_env_str("SUPERTABLE_LOG_LEVEL", "INFO").upper(),
        SUPERTABLE_LOG_FORMAT=_env_str("SUPERTABLE_LOG_FORMAT", "json").lower(),
        SUPERTABLE_LOG_FILE=_env_str("SUPERTABLE_LOG_FILE"),
        SUPERTABLE_LOG_COLOR=_env_str("SUPERTABLE_LOG_COLOR"),
        SUPERTABLE_CORRELATION_HEADER=_env_str("SUPERTABLE_CORRELATION_HEADER", "X-Correlation-ID"),
        NO_COLOR=os.getenv("NO_COLOR") is not None,

        # ── Monitoring ───────────────────────────────────────────────
        SUPERTABLE_MONITORING_ENABLED=_env_bool("SUPERTABLE_MONITORING_ENABLED", True),
        SUPERTABLE_MONITOR_CACHE_MAX=_env_int("SUPERTABLE_MONITOR_CACHE_MAX", 256),

        # ── Notebook / Python Worker ─────────────────────────────────
        SUPERTABLE_NOTEBOOK_PORT=_env_int("SUPERTABLE_NOTEBOOK_PORT", 8010),

        # ── Meta Reader / Caching ────────────────────────────────────
        SUPERTABLE_SUPER_META_CACHE_TTL_S=meta_ttl,

        # ── Audit ────────────────────────────────────────────────────
        SUPERTABLE_AUDIT_ENABLED=_env_bool("SUPERTABLE_AUDIT_ENABLED", True),
        SUPERTABLE_AUDIT_RETENTION_DAYS=_env_int("SUPERTABLE_AUDIT_RETENTION_DAYS", 2555),
        SUPERTABLE_AUDIT_BATCH_SIZE=_env_int("SUPERTABLE_AUDIT_BATCH_SIZE", 1000),
        SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC=_env_int("SUPERTABLE_AUDIT_FLUSH_INTERVAL_SEC", 60),
        SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS=_env_int("SUPERTABLE_AUDIT_REDIS_STREAM_TTL_HOURS", 24),
        SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN=_env_int("SUPERTABLE_AUDIT_REDIS_STREAM_MAXLEN", 100000),
        SUPERTABLE_AUDIT_HASH_CHAIN=_env_bool("SUPERTABLE_AUDIT_HASH_CHAIN", True),
        SUPERTABLE_AUDIT_LOG_QUERIES=_env_bool("SUPERTABLE_AUDIT_LOG_QUERIES", True),
        SUPERTABLE_AUDIT_LOG_READS=_env_bool("SUPERTABLE_AUDIT_LOG_READS", True),
        SUPERTABLE_AUDIT_ALERT_WEBHOOK=_env_str("SUPERTABLE_AUDIT_ALERT_WEBHOOK"),
        SUPERTABLE_AUDIT_LEGAL_HOLD=_env_bool("SUPERTABLE_AUDIT_LEGAL_HOLD", False),
        SUPERTABLE_AUDIT_FERNET_KEY=_env_str("SUPERTABLE_AUDIT_FERNET_KEY"),
        SUPERTABLE_AUDIT_SIEM_ENABLED=_env_bool("SUPERTABLE_AUDIT_SIEM_ENABLED", True),
        SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS=_env_int("SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS", 10),
    )


# ── Module-level singleton ───────────────────────────────────────────
settings: Settings = _build_settings()
