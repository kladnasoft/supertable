# route: supertable.engine.engine_config
"""
Engine runtime configuration — single source of truth for resolution.

Engine config is stored once per organization at **system scope**
(``supertable:{org}:system:engine:duckdb``).  It is global to the org, not
per-supertable, and is resolved **live on every query** (no caching) so that
changes made in the UI take effect on the next query without a restart or a
connection reset.

Two kinds of settings live in that document:

  * **Shared auto-pick thresholds** (one value, used by the router):
    ``engine_lite_max_bytes``, ``engine_spark_min_bytes``,
    ``engine_freshness_sec``.  These describe the *boundaries* between engines
    so they cannot be per-engine.
  * **Per-engine DuckDB runtime pragmas** (separate values for Lite and Pro):
    ``duckdb_memory_limit``, ``duckdb_io_multiplier``, ``duckdb_threads``,
    ``duckdb_http_timeout``, ``duckdb_external_cache_size``.  Each DuckDB engine
    carries its own copy under a ``"lite"`` / ``"pro"`` section, so tuning Lite
    never changes Pro and vice-versa.

Stored document shape::

    {
      "engine_lite_max_bytes": "104857600",
      "engine_spark_min_bytes": "10737418240",
      "engine_freshness_sec":   "300",
      "lite": {"duckdb_memory_limit": "1GB", ...},
      "pro":  {"duckdb_memory_limit": "8GB", ...},
      "modified_ms": 1750000000000
    }

Resolution precedence, per field::

    Redis (org system config)  →  environment variable  →  built-in default

The environment variable is a single global fallback shared by both engines;
per-engine differentiation therefore only happens once a value is stored in
Redis (via the UI).

Entry points (all share that precedence so engine and UI never disagree):

  * :func:`resolve_engine_configs`           → ``{"lite": cfg, "pro": cfg}`` in a
                                                single Redis read; used by the
                                                executor.
  * :func:`resolve_engine_config`            → typed :class:`EngineRuntimeConfig`
                                                for one engine (convenience).
  * :func:`resolve_engine_config_provenance` → ``{shared, lite, pro,
                                                modified_ms}`` per-field
                                                ``{value, source, env_var,
                                                default}`` for the API / UI.

Built-in defaults mirror the :mod:`supertable.config.settings` dataclass
defaults, so behaviour is identical whether a value flows through ``settings``
(no Redis override) or through this resolver.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


# Shared auto-pick thresholds: key → (env var, built-in default as string).
_SHARED_SPEC: Dict[str, Tuple[str, str]] = {
    "engine_lite_max_bytes":  ("SUPERTABLE_ENGINE_LITE_MAX_BYTES",  str(100 * 1024 * 1024)),
    "engine_spark_min_bytes": ("SUPERTABLE_ENGINE_SPARK_MIN_BYTES", str(10 * 1024 * 1024 * 1024)),
    "engine_freshness_sec":   ("SUPERTABLE_ENGINE_FRESHNESS_SEC",   "300"),
}

# Per-engine DuckDB runtime pragmas: key → (env var, built-in default as string).
# Defaults mirror the settings dataclass defaults in supertable/config/settings.py.
_DUCKDB_SPEC: Dict[str, Tuple[str, str]] = {
    "duckdb_memory_limit":        ("SUPERTABLE_DUCKDB_MEMORY_LIMIT",        "1GB"),
    "duckdb_io_multiplier":       ("SUPERTABLE_DUCKDB_IO_MULTIPLIER",       "3"),
    "duckdb_threads":             ("SUPERTABLE_DUCKDB_THREADS",             ""),
    "duckdb_http_timeout":        ("SUPERTABLE_DUCKDB_HTTP_TIMEOUT",        ""),
    "duckdb_external_cache_size": ("SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE", "5GB"),
}

# The two DuckDB engines that each carry their own runtime section.
DUCKDB_ENGINES: Tuple[str, ...] = ("lite", "pro")

# Public whitelists (for callers / catalog / tests).
SHARED_CONFIG_FIELDS: Tuple[str, ...] = tuple(_SHARED_SPEC.keys())
DUCKDB_CONFIG_FIELDS: Tuple[str, ...] = tuple(_DUCKDB_SPEC.keys())
ENGINE_CONFIG_FIELDS: Tuple[str, ...] = SHARED_CONFIG_FIELDS + DUCKDB_CONFIG_FIELDS


@dataclass(frozen=True)
class EngineRuntimeConfig:
    """Effective, typed config for one DuckDB engine resolution.

    Carries the shared auto-pick thresholds (identical across engines) plus the
    DuckDB session pragmas for the engine it was resolved for.
    """

    # auto-pick thresholds (shared)
    engine_lite_max_bytes: int
    engine_spark_min_bytes: int
    engine_freshness_sec: int

    # duckdb session pragmas (per-engine, re-applied live per query)
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


def _effective(stored: Dict[str, Any], key: str, spec: Dict[str, Tuple[str, str]]) -> Tuple[str, str]:
    """Return ``(value, source)`` for one field. source ∈ redis|env|default.

    ``stored`` is the dict the value is looked up in directly: the top-level
    document for shared fields, or the ``lite`` / ``pro`` sub-section for
    per-engine fields.
    """
    env_var, default = spec[key]
    redis_val = stored.get(key) if isinstance(stored, dict) else None
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


def _build_runtime(redis_cfg: Dict[str, Any], engine: str) -> EngineRuntimeConfig:
    """Build the typed config for ``engine`` ("lite"/"pro") from a stored doc."""
    section = redis_cfg.get(engine) if isinstance(redis_cfg.get(engine), dict) else {}

    sv = {k: _effective(redis_cfg, k, _SHARED_SPEC)[0] for k in _SHARED_SPEC}
    dv = {k: _effective(section, k, _DUCKDB_SPEC)[0] for k in _DUCKDB_SPEC}

    threads = _to_int(dv["duckdb_threads"], 0) if dv["duckdb_threads"] else 0
    http = _to_int(dv["duckdb_http_timeout"], 0) if dv["duckdb_http_timeout"] else 0

    return EngineRuntimeConfig(
        engine_lite_max_bytes=_to_int(sv["engine_lite_max_bytes"], 100 * 1024 * 1024),
        engine_spark_min_bytes=_to_int(sv["engine_spark_min_bytes"], 10 * 1024 * 1024 * 1024),
        engine_freshness_sec=_to_int(sv["engine_freshness_sec"], 300),
        duckdb_memory_limit=dv["duckdb_memory_limit"].strip() or "1GB",
        duckdb_io_multiplier=_to_float(dv["duckdb_io_multiplier"], 3.0),
        duckdb_threads=(threads if threads > 0 else None),
        duckdb_http_timeout=(http if http > 0 else None),
        duckdb_external_cache_size=dv["duckdb_external_cache_size"].strip(),
    )


def resolve_engine_configs(org: str, catalog: Optional[Any] = None) -> Dict[str, EngineRuntimeConfig]:
    """Resolve both DuckDB engines in a single Redis read.

    Returns ``{"lite": EngineRuntimeConfig, "pro": EngineRuntimeConfig}``.  The
    shared thresholds are identical in both; only the DuckDB pragmas differ.
    """
    cfg = _redis_cfg(org, catalog)
    return {e: _build_runtime(cfg, e) for e in DUCKDB_ENGINES}


def resolve_engine_config(org: str, catalog: Optional[Any] = None, engine: str = "lite") -> EngineRuntimeConfig:
    """Resolve effective config for a single ``engine`` (Redis → env → default).

    ``catalog`` is an optional ``RedisCatalog``.  When it is None or Redis is
    unreachable the resolver falls back to environment variables and built-in
    defaults, which keeps the engine usable without Redis (e.g. in unit tests).
    """
    engine = engine if engine in DUCKDB_ENGINES else "lite"
    return _build_runtime(_redis_cfg(org, catalog), engine)


def resolve_engine_config_provenance(org: str, catalog: Optional[Any] = None) -> Dict[str, Any]:
    """Per-field provenance for the UI.

    Shape::

        {
          "shared": {field: {value, source, env_var, default}, ...},
          "lite":   {field: {value, source, env_var, default}, ...},
          "pro":    {field: {value, source, env_var, default}, ...},
          "modified_ms": <int|None>,
        }

    ``source`` is one of ``redis`` / ``env`` / ``default``.
    """
    cfg = _redis_cfg(org, catalog)

    def _block(stored: Dict[str, Any], spec: Dict[str, Tuple[str, str]]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key, (env_var, default) in spec.items():
            value, source = _effective(stored, key, spec)
            out[key] = {"value": value, "source": source, "env_var": env_var, "default": default}
        return out

    result: Dict[str, Any] = {"shared": _block(cfg, _SHARED_SPEC)}
    for engine in DUCKDB_ENGINES:
        section = cfg.get(engine) if isinstance(cfg.get(engine), dict) else {}
        result[engine] = _block(section, _DUCKDB_SPEC)
    result["modified_ms"] = cfg.get("modified_ms")
    return result
