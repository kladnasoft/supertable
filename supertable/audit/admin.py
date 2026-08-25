# route: supertable.audit.admin
"""
Per-organization runtime audit configuration.

The Compliance tab in the WebUI (/ui/audit) calls this module via the API
to read and update per-org audit toggles WITHOUT restarting the process.

Persistence
-----------
Stored in Redis as a HASH at:

    supertable:{org}:system:audit:config

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
Every change is admitted as a CONFIG_CHANGE event whenever either the prior
or resulting general-audit policy is enabled.  Turning audit OFF is admitted
under the prior enabled policy before its worker is drained.  The general
audit lane remains bounded and best-effort; a change made while both policies
are disabled has no admitted general-audit destination.

Compliance: DORA Art. 6 (information security), SOC 2 CC8.1 (change mgmt).
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Optional

from supertable import redis_keys as RK
from supertable.audit.diagnostics import safe_audit_error_type

logger = logging.getLogger(__name__)

# Audit configuration is low-volume control-plane state.  Serialize updates so
# each call has a well-defined prior policy, persisted state, meta-event, and
# runtime-policy transition even when two administrators submit concurrently.
_CONFIG_UPDATE_LOCK = threading.RLock()


class AuditConfigDurabilityError(RuntimeError):
    """A persisted audit-policy change lacked a confirmed durable meta-event."""


class AuditConfigActivationError(RuntimeError):
    """A persisted audit policy could not be reconciled into this process."""


class AuditConfigReadError(RuntimeError):
    """The authoritative per-organization policy could not be read."""


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

def get_audit_config(org: str, *, strict: bool = False) -> Dict[str, Any]:
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
    except Exception as exc:  # pragma: no cover — non-fatal
        logger.warning(
            "[audit-admin] get_audit_config failed; error_type=%s",
            safe_audit_error_type(exc),
        )
        if strict:
            raise AuditConfigReadError(
                "authoritative audit configuration is unavailable"
            ) from None
        return out

    if not isinstance(raw, dict):
        if strict:
            raise AuditConfigReadError(
                "authoritative audit configuration is malformed"
            )
        return out

    for field in _BOOL_FIELDS:
        if field in raw:
            try:
                v = _coerce_bool(raw[field])
            except Exception:
                v = None
            if v is not None:
                out[field] = v
            elif strict:
                raise AuditConfigReadError(
                    "authoritative audit configuration is malformed"
                ) from None
    if "updated_ms" in raw:
        try:
            updated_ms = int(raw["updated_ms"])
            if updated_ms < 0 or updated_ms > (1 << 63) - 1:
                raise ValueError("updated_ms outside supported range")
            out["updated_ms"] = updated_ms
        except (TypeError, ValueError):
            if strict:
                raise AuditConfigReadError(
                    "authoritative audit configuration is malformed"
                ) from None
    if "updated_by" in raw:
        updated_by = raw["updated_by"]
        try:
            valid_updated_by = (
                isinstance(updated_by, str)
                and len(updated_by.encode("utf-8")) <= 256
            )
        except UnicodeEncodeError:
            valid_updated_by = False
        if not valid_updated_by:
            if strict:
                raise AuditConfigReadError(
                    "authoritative audit configuration is malformed"
                ) from None
        else:
            out["updated_by"] = updated_by

    return out


def is_audit_enabled(org: str) -> bool:
    """Convenience helper used by the audit logger."""
    return bool(get_audit_config(org).get("enabled", False))


def _refresh_runtime_policy(org: str, *, action: str):
    """Invalidate and immediately reconcile the process-local audit policy."""
    from supertable.audit.logger import (
        get_audit_logger,
        invalidate_audit_config_cache,
    )

    invalidate_audit_config_cache(org)
    return get_audit_logger(org, action=action)


def _emit_config_change(
    audit_logger,
    *,
    action: str,
    org: str,
    updated_by: str,
    touched: Dict[str, Any],
) -> bool:
    """Admit a config meta-event to an already policy-approved logger."""
    from supertable.audit.events import (
        AuditEvent,
        EventCategory,
        Severity,
        make_detail,
    )
    from supertable.audit.logger import NullAuditLogger

    if audit_logger is None or isinstance(audit_logger, NullAuditLogger):
        return False

    accepted = audit_logger.emit(AuditEvent(
        category=EventCategory.CONFIG_CHANGE.value,
        action=action,
        organization=org,
        actor_username=updated_by or "",
        actor_id="",
        resource_type="audit_config",
        resource_id="audit:config",
        detail=make_detail(**{
            key: "true" if bool(value) else "false"
            for key, value in touched.items()
        }),
        severity=Severity.WARNING.value,
    ))
    if accepted is False:
        raise AuditConfigDurabilityError(
            "audit config-change event was not admitted"
        )
    flush = getattr(audit_logger, "flush", None)
    if callable(flush):
        # The worker barrier confirms the Parquet system-of-record publication;
        # it never performs sink I/O on this caller thread.
        flush(timeout_s=7.0)
    return True


def _require_runtime_policy(org: str, *, action: str):
    """Reconcile now or raise a sanitized error; never report stale success."""
    try:
        return _refresh_runtime_policy(org, action=action)
    except Exception as exc:
        logger.error(
            "[audit-admin] runtime policy reconciliation failed; "
            "error_type=%s",
            safe_audit_error_type(exc),
        )
        raise AuditConfigActivationError(
            "audit runtime policy could not be activated"
        ) from None


def _try_emit_config_change(
    audit_logger,
    *,
    action: str,
    org: str,
    updated_by: str,
    touched: Dict[str, Any],
) -> bool:
    try:
        return _emit_config_change(
            audit_logger,
            action=action,
            org=org,
            updated_by=updated_by,
            touched=touched,
        )
    except Exception as exc:  # pragma: no cover - best-effort audit lane
        logger.debug(
            "[audit-admin] config-change audit admission failed; "
            "error_type=%s",
            safe_audit_error_type(exc),
        )
        return False


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

    Ordering is deliberate:

    1. Reconcile and capture the prior process-local policy.
    2. Persist the Redis override.
    3. If the prior policy was enabled, durably archive CONFIG_CHANGE through
       that logger.
    4. Invalidate and reconcile the new policy before returning.  Replacing or
       disabling a prior logger drains it, including the meta-event above.
    5. If only the resulting policy is enabled, admit through the new logger.

    Thus ON/OFF changes are visible immediately after this function returns,
    without the normal config-cache TTL.
    If either enabled policy admits the change but its durable barrier cannot
    complete, the persisted update remains visible and this call raises
    :class:`AuditConfigDurabilityError` rather than reporting false success.
    Returns the new effective config (post-merge).
    """
    if not org:
        raise ValueError("organization is required")
    # Validate every control-plane input before the Redis mutation.  A policy
    # call must never partially persist data that the strict runtime reader
    # will subsequently reject and leave the organization without a usable
    # last-known configuration.
    RK.audit_config(org)
    for field_name, value in (
        ("enabled", enabled),
        ("log_queries", log_queries),
        ("log_reads", log_reads),
        ("hash_chain", hash_chain),
        ("siem_enabled", siem_enabled),
    ):
        if value is not None and type(value) is not bool:
            raise TypeError(f"{field_name} must be a boolean or None")
    if not isinstance(updated_by, str):
        raise TypeError("updated_by must be a string")
    try:
        updated_by_bytes = updated_by.encode("utf-8")
    except UnicodeEncodeError:
        raise ValueError("updated_by must be valid UTF-8") from None
    if len(updated_by_bytes) > 256:
        raise ValueError("updated_by exceeds its byte limit")

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
        mapping[field] = "true" if val else "false"

    now_ms = int(time.time() * 1000)
    mapping["updated_ms"] = str(now_ms)
    # Persist the empty value too, so an anonymous update cannot inherit and
    # misattribute the previous administrator.
    mapping["updated_by"] = updated_by

    action = "config.update"
    touched = {key: value for key, value in incoming.items() if value is not None}

    with _CONFIG_UPDATE_LOCK:
        prior_cfg = get_audit_config(org)
        prior_enabled = bool(prior_cfg.get("enabled", False))

        # Force any stale TTL entry to the actual pre-write policy.  Capturing
        # the resulting logger gives ON→OFF a stable admitted path after the
        # Redis write but before the new policy is activated.
        prior_logger = _require_runtime_policy(org, action=action)

        try:
            redis_client = _redis()
            config_key = RK.audit_config(org)
            # WATCH prevents two independent workers from silently losing
            # one another's control-plane updates.
            make_pipeline = getattr(redis_client, "pipeline", None)
            if callable(make_pipeline):
                pipe = make_pipeline()
                pipe.watch(config_key)
                pipe.multi()
                pipe.hset(config_key, mapping=mapping)
                pipe.execute()
            else:  # lightweight test/adaptor clients; real Redis supports WATCH
                redis_client.hset(config_key, mapping=mapping)
        except Exception as exc:
            logger.error(
                "[audit-admin] set_audit_config failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            raise AuditConfigDurabilityError(
                "audit configuration could not be persisted"
            ) from None

        new_cfg = get_audit_config(org)
        new_enabled = bool(new_cfg.get("enabled", False))

        admitted = False
        if prior_enabled:
            admitted = _try_emit_config_change(
                prior_logger,
                action=action,
                org=org,
                updated_by=updated_by,
                touched=touched,
            )

        # This is the runtime-policy linearization point.  get_audit_logger()
        # stops/replaces a stale worker as needed, so ON/OFF and lane toggles
        # are effective when this call returns rather than after the 30s TTL.
        new_logger = _require_runtime_policy(org, action=action)

        if not admitted and new_enabled:
            admitted = _try_emit_config_change(
                new_logger,
                action=action,
                org=org,
                updated_by=updated_by,
                touched=touched,
            )

        if (prior_enabled or new_enabled) and not admitted:
            raise AuditConfigDurabilityError(
                "audit policy changed without a confirmed durable meta-event"
            )

        return new_cfg
