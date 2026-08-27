# route: supertable.engine.engine_config
"""
Engine runtime configuration — single source of truth for resolution.

Engine config is stored once per organization at **system scope** in the
historically named ``supertable:{org}:system:engine:duckdb`` document. It is global to the org, not
per-supertable, and is resolved **live on every query** (no caching) so that
changes made in the UI take effect on the next query without a restart or a
connection reset.

Two kinds of settings live in that document:

  * **Shared auto-pick thresholds** (one value, used by the router):
    ``engine_island_min_bytes``, ``engine_spark_min_bytes``,
    ``engine_freshness_sec``.  These describe the *boundaries* between engines
    so they cannot be per-engine.
  * **DuckDB runtime pragmas**:
    ``duckdb_memory_limit``, ``duckdb_io_multiplier``, ``duckdb_threads``,
    ``duckdb_http_timeout``, ``duckdb_external_cache_size``.  Each DuckDB engine
    are stored under the ``"duckdb"`` section.
  * **IslandDB resource ceilings**: CPU, memory, whole-object cache, and range
    cache limits stored under ``"islanddb"``. Empty/zero means automatic.

Stored document shape::

    {
      "engine_island_min_bytes": "104857600",
      "engine_spark_min_bytes": "10737418240",
      "engine_freshness_sec":   "300",
      "duckdb": {"duckdb_memory_limit": "1GB", ...},
      "islanddb": {"island_cpu_max": "4", ...},
      "modified_ms": 1750000000000
    }

Resolution precedence, per field::

    Redis (org system config)  →  environment variable  →  built-in default

The environment variable is the global fallback until a value is stored in
Redis via the UI.

Entry points (all share that precedence so engine and UI never disagree):

  * :func:`resolve_engine_configs`           → DuckDB and IslandDB configs in a
                                                single Redis read; used by the
                                                executor.
  * :func:`resolve_engine_config`            → typed runtime config for one
                                                engine (convenience).
  * :func:`resolve_engine_config_provenance` → ``{shared, duckdb, islanddb,
                                                modified_ms}`` per-field
                                                ``{value, source, env_var,
                                                default}`` for the API / UI.

Built-in defaults mirror the :mod:`supertable.config.settings` dataclass
defaults, so behaviour is identical whether a value flows through ``settings``
(no Redis override) or through this resolver.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from supertable.engine.engine_enum import Engine


# Optional decimal number followed by an optional byte unit (decimal KB/MB/GB/TB
# or binary KiB/MiB/GiB/TiB), case-insensitive, whitespace-tolerant.
_MEM_SIZE_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([KMGT]i?B)?$", re.IGNORECASE)


def normalize_memory_size(value: Any, default: str = "1GB", *, bare_unit: str = "GB") -> str:
    """Coerce a memory-size value into a DuckDB-valid, unit-suffixed string.

    The engine UI collects a bare integer (interpreted as ``bare_unit`` — GB),
    while env vars / older Redis docs may already carry a unit.  DuckDB's
    ``PRAGMA memory_limit`` parser rejects a unit-less number
    (``Unknown unit for memory: ''``), so this guarantees a value the parser
    accepts before it is ever sent:

      * bare number            -> ``"<n>GB"``       ("2"    -> "2GB")
      * number + unit          -> canonical casing  ("2gib" -> "2GiB")
      * empty / None           -> ``default``       (""/None -> default)
      * non-positive / garbage -> ``default``       ("0", "x" -> default)

    ``default`` is returned verbatim, so pass ``""`` when "unset" is the
    desired fallback (e.g. the external file cache, where empty == disabled).
    """
    s = ("" if value is None else str(value)).strip()
    if not s:
        return default
    m = _MEM_SIZE_RE.match(s)
    if not m:
        return default
    num, unit = m.group(1), m.group(2)
    try:
        if float(num) <= 0:
            return default
    except ValueError:
        return default
    if unit:
        u = unit.upper()
        unit = (u[0] + "iB") if u.endswith("IB") else u
    else:
        unit = bare_unit
    return f"{num}{unit}"


# Shared auto-pick thresholds: key → (env var, built-in default as string).
_SHARED_SPEC: Dict[str, Tuple[str, str]] = {
    "engine_island_min_bytes":  ("SUPERTABLE_ENGINE_ISLAND_MIN_BYTES",  str(100 * 1024 * 1024)),
    "engine_spark_min_bytes": ("SUPERTABLE_ENGINE_SPARK_MIN_BYTES", str(0)),
    "engine_freshness_sec":   ("SUPERTABLE_ENGINE_FRESHNESS_SEC",   "300"),
}

# DuckDB runtime pragmas: key → (env var, built-in default as string).
# Defaults mirror the settings dataclass defaults in supertable/config/settings.py.
_DUCKDB_SPEC: Dict[str, Tuple[str, str]] = {
    "duckdb_memory_limit":        ("SUPERTABLE_DUCKDB_MEMORY_LIMIT",        "1GB"),
    "duckdb_io_multiplier":       ("SUPERTABLE_DUCKDB_IO_MULTIPLIER",       "3"),
    "duckdb_threads":             ("SUPERTABLE_DUCKDB_THREADS",             ""),
    "duckdb_http_timeout":        ("SUPERTABLE_DUCKDB_HTTP_TIMEOUT",        ""),
    "duckdb_external_cache_size": ("SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE", "5GB"),
}

# IslandDB process/admission/cache ceilings. Empty/zero means automatic: CPU
# and memory use the effective container limits; caches derive a filesystem
# budget at construction time.
_ISLAND_SPEC: Dict[str, Tuple[str, str]] = {
    "island_cpu_max": ("SUPERTABLE_ISLAND_CPU_MAX", ""),
    "island_memory_max_bytes": ("SUPERTABLE_ISLAND_MAX_MEMORY_BYTES", ""),
    "island_cache_max_bytes": ("SUPERTABLE_ISLAND_CACHE_MAX_BYTES", ""),
    "island_range_cache_max_bytes": (
        "SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES", "",
    ),
}

# Public whitelists (for callers / catalog / tests).
SHARED_CONFIG_FIELDS: Tuple[str, ...] = tuple(_SHARED_SPEC.keys())
DUCKDB_CONFIG_FIELDS: Tuple[str, ...] = tuple(_DUCKDB_SPEC.keys())
ISLAND_CONFIG_FIELDS: Tuple[str, ...] = tuple(_ISLAND_SPEC.keys())
ENGINE_CONFIG_FIELDS: Tuple[str, ...] = (
    SHARED_CONFIG_FIELDS + DUCKDB_CONFIG_FIELDS + ISLAND_CONFIG_FIELDS
)


@dataclass(frozen=True)
class AutoRoutingRule:
    """One half-open estimated-scan interval persisted in Redis."""

    min_bytes: int
    max_bytes: Optional[int]
    engine: Engine

    def matches(self, estimated_scan_bytes: int) -> bool:
        return (
            estimated_scan_bytes >= self.min_bytes
            and (self.max_bytes is None or estimated_scan_bytes < self.max_bytes)
        )

    def as_dict(self) -> Dict[str, Any]:
        return {
            "min_bytes": self.min_bytes,
            "max_bytes": self.max_bytes,
            "engine": self.engine.value,
        }


def normalize_auto_routing_policy(raw: Any) -> Tuple[AutoRoutingRule, ...]:
    """Validate a non-overlapping Redis AUTO policy.

    Intervals are half-open (``min <= estimate < max``); ``max_bytes=None`` is
    unbounded and can only be the final rule. Gaps are allowed and deliberately
    fall through to the adaptive cost model. Invalid documents fail closed by
    raising instead of being partially applied.
    """
    if raw in (None, ""):
        return ()
    if not isinstance(raw, (list, tuple)):
        raise ValueError("auto_policy must be a list of routing rules")
    rules = []
    for index, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise ValueError(f"auto_policy[{index}] must be an object")
        try:
            minimum = int(item.get("min_bytes", 0))
        except (TypeError, ValueError, OverflowError):
            raise ValueError(f"auto_policy[{index}].min_bytes is invalid") from None
        maximum_raw = item.get("max_bytes")
        try:
            maximum = None if maximum_raw in (None, "") else int(maximum_raw)
        except (TypeError, ValueError, OverflowError):
            raise ValueError(f"auto_policy[{index}].max_bytes is invalid") from None
        if minimum < 0 or (maximum is not None and maximum <= minimum):
            raise ValueError(f"auto_policy[{index}] has an empty/negative interval")
        try:
            engine = Engine(str(item.get("engine", "")).strip().lower())
        except ValueError:
            raise ValueError(f"auto_policy[{index}].engine is invalid") from None
        if engine is Engine.AUTO:
            raise ValueError("auto_policy cannot route AUTO to itself")
        rules.append(AutoRoutingRule(minimum, maximum, engine))
    rules.sort(key=lambda rule: rule.min_bytes)
    previous_max = None
    for index, rule in enumerate(rules):
        if index and (previous_max is None or rule.min_bytes < previous_max):
            raise ValueError("auto_policy intervals overlap or follow an unbounded rule")
        previous_max = rule.max_bytes
    return tuple(rules)


def match_auto_routing_policy(
    rules: Tuple[AutoRoutingRule, ...], estimated_scan_bytes: int,
) -> Optional[AutoRoutingRule]:
    estimate = max(0, int(estimated_scan_bytes))
    return next((rule for rule in rules if rule.matches(estimate)), None)


@dataclass(frozen=True)
class EngineRuntimeConfig:
    """Effective, typed config for the DuckDB executor."""

    # auto-pick thresholds (shared)
    engine_island_min_bytes: int
    engine_spark_min_bytes: int
    engine_freshness_sec: int

    # duckdb session pragmas (per-engine, re-applied live per query)
    duckdb_memory_limit: str             # e.g. "1GB"
    duckdb_io_multiplier: float
    duckdb_threads: Optional[int]        # None → auto-derive from memory limit
    duckdb_http_timeout: Optional[int]   # seconds; None → leave DuckDB default
    duckdb_external_cache_size: str      # "" → external file cache disabled


@dataclass(frozen=True)
class IslandRuntimeConfig:
    """Effective per-query Island resource ceilings; None means automatic."""

    cpu_max: Optional[int]
    memory_max_bytes: Optional[int]
    cache_max_bytes: Optional[int]
    range_cache_max_bytes: Optional[int]


def _redis_cfg(org: str, catalog: Optional[Any]) -> Dict[str, Any]:
    """Read the stored engine config for ``org`` fail closed.

    A genuinely absent document uses env vars and defaults. Backend uncertainty
    or corrupt control state must propagate instead of silently changing query
    routing and resource policy.
    """
    if not (org and catalog is not None):
        return {}
    raw = catalog.get_engine_config(org)
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise RuntimeError("Engine configuration is not a JSON object")
    return _canonicalize_legacy_24_config(raw)


def _canonicalize_legacy_24_config(raw: Mapping[str, Any]) -> Dict[str, Any]:
    """Return one current-shape view of a 2.4 engine document.

    SuperTable 2.4 persisted DuckDB pragmas below ``lite`` and ``pro``.  The
    current DuckDB implementation is the renamed Lite execution path, so Lite
    is the deterministic first choice; Pro is used only for estates that stored
    no Lite section.  A present current ``duckdb`` section is authoritative,
    even when empty, so an operator can intentionally clear old overrides.

    This is a storage compatibility read, not a revival of the removed public
    Lite/Pro engine names.
    """
    cfg = dict(raw)
    if "duckdb" not in cfg:
        legacy_section = next(
            (
                raw.get(name)
                for name in ("lite", "pro")
                if isinstance(raw.get(name), Mapping)
            ),
            None,
        )
        if legacy_section is not None:
            cfg["duckdb"] = dict(legacy_section)

    if (
        "engine_island_min_bytes" not in cfg
        and "engine_lite_max_bytes" in raw
    ):
        cfg["engine_island_min_bytes"] = raw["engine_lite_max_bytes"]

    policy = raw.get("auto_policy")
    if isinstance(policy, (list, tuple)):
        migrated_policy = []
        for item in policy:
            if not isinstance(item, Mapping):
                migrated_policy.append(item)
                continue
            migrated = dict(item)
            if str(migrated.get("engine", "")).strip().casefold() in {
                "duckdb_lite", "duckdb_pro", "lite", "pro",
            }:
                migrated["engine"] = Engine.DUCKDB.value
            migrated_policy.append(migrated)
        cfg["auto_policy"] = migrated_policy
    return cfg


def _effective(stored: Dict[str, Any], key: str, spec: Dict[str, Tuple[str, str]]) -> Tuple[str, str]:
    """Return ``(value, source)`` for one field. source ∈ redis|env|default.

    ``stored`` is the dict the value is looked up in directly.
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


def normalize_island_limit(value: Any) -> Optional[int]:
    """Parse a non-negative Island ceiling; zero/empty selects automatic."""
    if value is None or str(value).strip() == "":
        return None
    text = str(value).strip()
    if text.casefold() in {"true", "false"}:
        raise ValueError("Island resource limits must be non-negative integers")
    try:
        parsed = int(text)
    except (TypeError, ValueError, OverflowError):
        raise ValueError(
            "Island resource limits must be non-negative integers"
        ) from None
    if parsed < 0:
        raise ValueError("Island resource limits must be non-negative integers")
    return parsed or None


def _island_section(redis_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    if "islanddb" not in redis_cfg:
        return {}
    section = redis_cfg.get("islanddb")
    if not isinstance(section, Mapping):
        raise ValueError("islanddb engine configuration must be an object")
    return dict(section)


def _build_runtime(redis_cfg: Dict[str, Any]) -> EngineRuntimeConfig:
    """Build the typed DuckDB config from the stored document."""
    section = (
        redis_cfg.get("duckdb")
        if isinstance(redis_cfg.get("duckdb"), dict) else {}
    )
    sv = {k: _effective(redis_cfg, k, _SHARED_SPEC)[0] for k in _SHARED_SPEC}
    dv = {k: _effective(section, k, _DUCKDB_SPEC)[0] for k in _DUCKDB_SPEC}

    threads = _to_int(dv["duckdb_threads"], 0) if dv["duckdb_threads"] else 0
    http = _to_int(dv["duckdb_http_timeout"], 0) if dv["duckdb_http_timeout"] else 0

    return EngineRuntimeConfig(
        engine_island_min_bytes=_to_int(sv["engine_island_min_bytes"], 100 * 1024 * 1024),
        engine_spark_min_bytes=_to_int(sv["engine_spark_min_bytes"], 0),
        engine_freshness_sec=_to_int(sv["engine_freshness_sec"], 300),
        duckdb_memory_limit=normalize_memory_size(dv["duckdb_memory_limit"], default="1GB"),
        duckdb_io_multiplier=_to_float(dv["duckdb_io_multiplier"], 3.0),
        duckdb_threads=(threads if threads > 0 else None),
        duckdb_http_timeout=(http if http > 0 else None),
        duckdb_external_cache_size=normalize_memory_size(dv["duckdb_external_cache_size"], default=""),
    )


def _build_island_runtime(redis_cfg: Dict[str, Any]) -> IslandRuntimeConfig:
    section = _island_section(redis_cfg)
    values = {
        key: normalize_island_limit(
            _effective(section, key, _ISLAND_SPEC)[0]
        )
        for key in _ISLAND_SPEC
    }
    return IslandRuntimeConfig(
        cpu_max=values["island_cpu_max"],
        memory_max_bytes=values["island_memory_max_bytes"],
        cache_max_bytes=values["island_cache_max_bytes"],
        range_cache_max_bytes=values["island_range_cache_max_bytes"],
    )


def resolve_engine_configs(org: str, catalog: Optional[Any] = None) -> Dict[str, object]:
    """Resolve DuckDB and IslandDB from one stored engine document."""
    cfg = _redis_cfg(org, catalog)
    return {
        "duckdb": _build_runtime(cfg),
        "islanddb": _build_island_runtime(cfg),
    }


def resolve_auto_routing_policy(
    org: str, catalog: Optional[Any] = None,
) -> Tuple[AutoRoutingRule, ...]:
    """Resolve the live Redis policy; missing or malformed policy means AUTO."""
    cfg = _redis_cfg(org, catalog)
    try:
        return normalize_auto_routing_policy(cfg.get("auto_policy"))
    except ValueError:
        return ()


def resolve_engine_bundle(
    org: str, catalog: Optional[Any] = None,
) -> Tuple[Dict[str, object], Tuple[AutoRoutingRule, ...]]:
    """Resolve runtime configs and manual AUTO policy with one Redis GET."""
    cfg = _redis_cfg(org, catalog)
    configs = {
        "duckdb": _build_runtime(cfg),
        "islanddb": _build_island_runtime(cfg),
    }
    try:
        policy = normalize_auto_routing_policy(cfg.get("auto_policy"))
    except ValueError:
        policy = ()
    return configs, policy


def resolve_engine_config(org: str, catalog: Optional[Any] = None, engine: str = "duckdb"):
    """Resolve one effective engine config (Redis → env → default).

    ``catalog`` is optional for standalone/unit-test use.  With no catalog (or
    with a genuinely absent Redis document), environment variables and built-in
    defaults apply.  Once a catalog is supplied, backend uncertainty and corrupt
    control state propagate instead of silently changing routing or resource
    policy.
    """
    cfg = _redis_cfg(org, catalog)
    if str(engine).strip().casefold() in {"island", "islanddb"}:
        return _build_island_runtime(cfg)
    return _build_runtime(cfg)


def resolve_engine_config_provenance(org: str, catalog: Optional[Any] = None) -> Dict[str, Any]:
    """Per-field provenance for the UI.

    Shape::

        {
          "shared": {field: {value, source, env_var, default}, ...},
          "duckdb": {field: {value, source, env_var, default}, ...},
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
    section = cfg.get("duckdb") if isinstance(cfg.get("duckdb"), dict) else {}
    result["duckdb"] = _block(section, _DUCKDB_SPEC)
    island_section = _island_section(cfg)
    result["islanddb"] = _block(island_section, _ISLAND_SPEC)
    result["modified_ms"] = cfg.get("modified_ms")
    try:
        result["auto_policy"] = [
            rule.as_dict()
            for rule in normalize_auto_routing_policy(cfg.get("auto_policy"))
        ]
        result["auto_policy_valid"] = True
    except ValueError:
        result["auto_policy"] = []
        result["auto_policy_valid"] = False
    return result
