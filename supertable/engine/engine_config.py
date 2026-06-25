# route: supertable.engine.engine_config
"""
Engine runtime configuration — single source of truth for resolution.

Engine config is stored once per organization at **system scope**
(``supertable:{org}:system:config:engine``).  It is global to the org, not
per-supertable, and is resolved **live on every query** (no caching) so that
changes made in the UI take effect on the next query without a restart or a
connection reset.

Resolution precedence, per field::

    Redis (org system config)  →  environment variable  →  built-in default

Two entry points share that precedence so the engine and the UI never disagree:

  * :func:`resolve_engine_config`            → typed :class:`EngineRuntimeConfig`,
                                                consumed by the executor and the
                                                DuckDB leaf executors.
  * :func:`resolve_engine_config_provenance` → per-field ``{value, source,
                                                env_var, default}`` dict for the
                                                API / UI to display where each
                                                effective value came from.

Built-in defaults mirror the :mod:`supertable.config.settings` dataclass
defaults, so behaviour is identical whether a value flows through ``settings``
(no Redis override) or through this resolver.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


# Canonical field spec: key → (env var, built-in default as string).
# Defaults mirror the settings dataclass defaults in
# supertable/config/settings.py — keep the two in sync.
_FIELD_SPEC: Dict[str, Tuple[str, str]] = {
    "engine_lite_max_bytes":      ("SUPERTABLE_ENGINE_LITE_MAX_BYTES",      str(100 * 1024 * 1024)),
    "engine_spark_min_bytes":     ("SUPERTABLE_ENGINE_SPARK_MIN_BYTES",     str(10 * 1024 * 1024 * 1024)),
    "engine_freshness_sec":       ("SUPERTABLE_ENGINE_FRESHNESS_SEC",       "300"),
    "duckdb_memory_limit":        ("SUPERTABLE_DUCKDB_MEMORY_LIMIT",        "1GB"),
    "duckdb_io_multiplier":       ("SUPERTABLE_DUCKDB_IO_MULTIPLIER",       "3"),
    "duckdb_threads":             ("SUPERTABLE_DUCKDB_THREADS",             ""),
    "duckdb_http_timeout":        ("SUPERTABLE_DUCKDB_HTTP_TIMEOUT",        ""),
    "duckdb_external_cache_size": ("SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE", "5GB"),
}

# Public tuple of recognised field names (whitelist for callers/tests).
ENGINE_CONFIG_FIELDS: Tuple[str, ...] = tuple(_FIELD_SPEC.keys())


@dataclass(frozen=True)
class EngineRuntimeConfig:
    """Effective, typed engine configuration for a single resolution."""

    # auto-pick thresholds
    engine_lite_max_bytes: int
    engine_spark_min_bytes: int
    engine_freshness_sec: int

    # duckdb session pragmas (re-applied live per query)
    duckdb_memory_limit: str             # e.g. "1GB"
    duckdb_io_multiplier: float
    duckdb_threads: Optional[int]        # None → auto-derive from memory limit
    duckdb_http_timeout: Optional[int]   # seconds; None → leave DuckDB default
    duckdb_external_cache_size: str      # "" → external file cache disabled


def _redis_cfg(org: str, catalog: Optional[Any]) -> Dict[str, Any]:
    """Read the stored engine config for ``org`` (best effort).

    Returns an empty dict when no catalog is supplied or Redis is unreachable,
    so the resolver degrades cleanly to env vars + defaults.
    """
    if not (org and catalog is not None):
        return {}
    try:
        return catalog.get_engine_config(org) or {}
    except Exception:
        return {}


def _effective(redis_cfg: Dict[str, Any], key: str) -> Tuple[str, str]:
    """Return ``(value, source)`` for one field. source ∈ redis|env|default."""
    env_var, default = _FIELD_SPEC[key]
    redis_val = redis_cfg.get(key)
    if redis_val is not None and str(redis_val).strip() != "":
        return str(redis_val).strip(), "redis"
    env_val = (os.getenv(env_var) or "").strip()
    if env_val:
        return env_val, "env"
    return default, "default"


def _to_int(s: Any, fallback: int) -> int:
    try:
        return int(str(s).strip())
    except (TypeError, ValueError):
        return fallback


def _to_float(s: Any, fallback: float) -> float:
    try:
        return float(str(s).strip())
    except (TypeError, ValueError):
        return fallback


def resolve_engine_config(org: str, catalog: Optional[Any] = None) -> EngineRuntimeConfig:
    """Resolve effective engine config for ``org`` (Redis → env → default).

    ``catalog`` is an optional ``RedisCatalog``.  When it is None or Redis is
    unreachable the resolver falls back to environment variables and built-in
    defaults, which keeps the engine usable without Redis (e.g. in unit tests).
    """
    cfg = _redis_cfg(org, catalog)
    val = {k: _effective(cfg, k)[0] for k in _FIELD_SPEC}

    threads = _to_int(val["duckdb_threads"], 0) if val["duckdb_threads"] else 0
    http = _to_int(val["duckdb_http_timeout"], 0) if val["duckdb_http_timeout"] else 0

    return EngineRuntimeConfig(
        engine_lite_max_bytes=_to_int(val["engine_lite_max_bytes"], 100 * 1024 * 1024),
        engine_spark_min_bytes=_to_int(val["engine_spark_min_bytes"], 10 * 1024 * 1024 * 1024),
        engine_freshness_sec=_to_int(val["engine_freshness_sec"], 300),
        duckdb_memory_limit=val["duckdb_memory_limit"].strip() or "1GB",
        duckdb_io_multiplier=_to_float(val["duckdb_io_multiplier"], 3.0),
        duckdb_threads=(threads if threads > 0 else None),
        duckdb_http_timeout=(http if http > 0 else None),
        duckdb_external_cache_size=val["duckdb_external_cache_size"].strip(),
    )


def resolve_engine_config_provenance(org: str, catalog: Optional[Any] = None) -> Dict[str, Any]:
    """Per-field provenance for the UI: ``{value, source, env_var, default}``.

    ``source`` is one of ``redis`` / ``env`` / ``default``.  The extra
    ``modified_ms`` key carries the last-write timestamp (or None).
    """
    cfg = _redis_cfg(org, catalog)
    result: Dict[str, Any] = {}
    for key, (env_var, default) in _FIELD_SPEC.items():
        value, source = _effective(cfg, key)
        result[key] = {
            "value": value,
            "source": source,
            "env_var": env_var,
            "default": default,
        }
    result["modified_ms"] = cfg.get("modified_ms")
    return result
