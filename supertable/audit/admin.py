# route: supertable.audit.admin
"""
Per-organization runtime audit configuration.

The Compliance tab in the WebUI (/ui/audit) calls this module via the API
to read and update per-org audit toggles WITHOUT restarting the process.

Persistence
-----------
Stored in Redis as a HASH at:

    supertable:{org}:audit:config

Fields
------
    enabled         "true" | "false"   master on/off switch (default: env)
    log_queries     "true" | "false"   record DATA_ACCESS query events
    log_reads       "true" | "false"   record DATA_ACCESS read events
    hash_chain      "true" | "false"   tamper-evident hash chaining
    siem_enabled    "true" | "false"   external SIEM consumer groups
    updated_ms      str(int)           last update timestamp
    updated_by      str                actor (username) who toggled

Audit-of-the-audit
------------------
Every change to this config emits a CONFIG_CHANGE audit event so that
turning audit OFF is itself recorded — DORA Art. 6 / SOC 2 CC8.1.

Compliance: DORA Art. 6 (information security), SOC 2 CC8.1 (change mgmt).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from supertable import redis_keys as RK

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Field schema
# ---------------------------------------------------------------------------

# Boolean fields and their env-var defaults (resolved lazily so settings is
# only imported when actually needed; avoids import-time side effects).
_BOOL_FIELDS = (
    "enabled",
    "log_queries",
    "log_reads",
    "hash_chain",
    "siem_enabled",
)

_ENV_DEFAULTS = {
    "enabled":      "SUPERTABLE_AUDIT_ENABLED",
    "log_queries":  "SUPERTABLE_AUDIT_LOG_QUERIES",
    "log_reads":    "SUPERTABLE_AUDIT_LOG_READS",
    "hash_chain":   "SUPERTABLE_AUDIT_HASH_CHAIN",
    "siem_enabled": "SUPERTABLE_AUDIT_SIEM_ENABLED",
}


def _env_default(field: str) -> bool:
    """Read the env-var default for *field*."""
    from supertable.config.settings import settings as _cfg
    attr = _ENV_DEFAULTS.get(field)
    if not attr:
        return False
    return bool(getattr(_cfg, attr, False))


def _coerce_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool):
        return v
    if v is None:
        return None
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return True
    if s in ("0", "false", "no", "n", "off"):
        return False
    return None


def _redis():
    """Lazy Redis handle.  Centralizes the import for testability."""
    from supertable.redis_catalog import RedisCatalog
    return RedisCatalog().r


# ---------------------------------------------------------------------------
# Read
# ---------------------------------------------------------------------------

def get_audit_config(org: str) -> Dict[str, Any]:
    """Return the effective audit config for *org*.

    Merges Redis overrides over env-var defaults.  Always returns every
    boolean field with a concrete True/False value plus updated_ms /
    updated_by (or empty defaults).
    """
    out: Dict[str, Any] = {field: _env_default(field) for field in _BOOL_FIELDS}
    out["updated_ms"] = 0
    out["updated_by"] = ""

    if not org:
        return out

    try:
        raw = _redis().hgetall(RK.audit_config(org)) or {}
    except Exception as e:  # pragma: no cover — non-fatal
        logger.warning("[audit-admin] get_audit_config redis error: %s", e)
        return out

    for field in _BOOL_FIELDS:
        if field in raw:
            v = _coerce_bool(raw[field])
            if v is not None:
                out[field] = v
    if "updated_ms" in raw:
        try:
            out["updated_ms"] = int(raw["updated_ms"])
        except (TypeError, ValueError):
            pass
    if "updated_by" in raw:
        out["updated_by"] = str(raw["updated_by"])

    return out


def is_audit_enabled(org: str) -> bool:
    """Convenience helper used by the audit logger."""
    return bool(get_audit_config(org).get("enabled", False))


# ---------------------------------------------------------------------------
# Write
# ---------------------------------------------------------------------------

def set_audit_config(
    org: str,
    *,
    enabled: Optional[bool] = None,
    log_queries: Optional[bool] = None,
    log_reads: Optional[bool] = None,
    hash_chain: Optional[bool] = None,
    siem_enabled: Optional[bool] = None,
    updated_by: str = "",
) -> Dict[str, Any]:
    """Update per-org audit config.  Only fields that are not None are written.

    Returns the new effective config (post-merge).
    """
    if not org:
        raise ValueError("organization is required")

    incoming: Dict[str, Any] = {
        "enabled": enabled,
        "log_queries": log_queries,
        "log_reads": log_reads,
        "hash_chain": hash_chain,
        "siem_enabled": siem_enabled,
    }
    mapping: Dict[str, str] = {}
    for field, val in incoming.items():
        if val is None:
            continue
        mapping[field] = "true" if bool(val) else "false"

    now_ms = int(time.time() * 1000)
    mapping["updated_ms"] = str(now_ms)
    if updated_by:
        mapping["updated_by"] = str(updated_by)

    try:
        _redis().hset(RK.audit_config(org), mapping=mapping)
    except Exception as e:
        logger.error("[audit-admin] set_audit_config redis error: %s", e)
        raise

    new_cfg = get_audit_config(org)

    # Emit a CONFIG_CHANGE audit event for the change itself.
    # Use a try/except so a misconfigured audit subsystem cannot block the
    # config write that just succeeded.
    try:
        from supertable.audit import emit, EventCategory, Actions, Severity, make_detail
        # Build a detail string showing only the fields that were touched.
        touched = {k: v for k, v in incoming.items() if v is not None}
        emit(
            category=EventCategory.CONFIG_CHANGE,
            action=getattr(Actions, "CONFIG_UPDATE", "config.update"),
            organization=org,
            actor_username=updated_by or "",
            actor_id="",
            resource_type="audit_config",
            resource_id="audit:config",
            detail=make_detail(**{k: ("true" if bool(v) else "false") for k, v in touched.items()}),
            severity=Severity.WARNING,
        )
    except Exception as e:  # pragma: no cover — non-fatal
        logger.debug("[audit-admin] config-change audit emit failed: %s", e)

    return new_cfg
