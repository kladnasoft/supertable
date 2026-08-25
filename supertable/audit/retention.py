# route: supertable.audit.retention
"""
Audit log retention policy enforcement and legal hold management.

Retention deletes Parquet partitions older than AUDIT_RETENTION_DAYS
(default: 2555 ≈ 7 years, DORA Art. 12 minimum).  Legal hold is a
global kill switch that prevents ALL audit deletions.

Legal hold state is persisted in Redis (not in the frozen Settings
dataclass) so it can be toggled at runtime without a restart.  The
Settings field ``SUPERTABLE_AUDIT_LEGAL_HOLD`` serves as the initial
default — once ``set_legal_hold()`` is called, the Redis value takes
precedence.

All deletions are recorded as audit events (meta-event: the audit log
audits its own cleanup).  Ordinary delivery failures remain best-effort,
but configured encryption failures propagate after deletion so callers
cannot mistake an unaudited protected mutation for success.

Compliance: DORA Art. 12 (5+ year retention), SOC 2 A1.2.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from supertable import redis_keys as RK
from supertable.audit.crypto import AuditEncryptionError
from supertable.audit.diagnostics import safe_audit_error_type

logger = logging.getLogger(__name__)


class AuditRetentionDurabilityError(RuntimeError):
    """A retention mutation completed without a durable audit event."""


def _legal_hold_key(org: str) -> str:
    return RK.audit_legal_hold(org)


# ---------------------------------------------------------------------------
# Partition path parsing
# ---------------------------------------------------------------------------

_PARTITION_RE = re.compile(
    r"year=(\d{4})[/\\]month=(\d{2})[/\\]day=(\d{2})/?$"
)


def _parse_partition_date(partition_path: Any) -> Optional[datetime]:
    """Extract the date from a Hive-style partition path.

    Matches paths like ``…/year=2024/month=03/day=15`` or
    ``…/year=2024/month=03/day=15/``.

    Returns a timezone-aware UTC datetime at midnight, or None if the
    path does not match the expected partition naming convention.
    """
    if not isinstance(partition_path, str):
        return None
    m = _PARTITION_RE.search(partition_path)
    if not m:
        return None
    try:
        return datetime(
            year=int(m.group(1)),
            month=int(m.group(2)),
            day=int(m.group(3)),
            tzinfo=timezone.utc,
        )
    except (ValueError, OverflowError):
        return None


# ---------------------------------------------------------------------------
# Legal hold — query
# ---------------------------------------------------------------------------

def is_legal_hold_active(organization: str) -> bool:
    """Check whether legal hold is active for this organization.

    Resolution order:
      1. Redis runtime override (set by ``set_legal_hold()``)
      2. Settings default (``SUPERTABLE_AUDIT_LEGAL_HOLD``)

    Fail-safe: if both lookups fail, returns True (hold active) so
    that data is never accidentally deleted.
    """
    # Try Redis first (runtime override).  Once runtime overrides are supported,
    # an unreadable authoritative key cannot safely be treated as "no
    # override": doing so could turn a Redis outage into destructive deletion
    # under a false environment default.
    try:
        from supertable.redis_infra import redis_client
        val = redis_client.get(_legal_hold_key(organization))
        if val is not None:
            decoded = val if isinstance(val, str) else val.decode("utf-8")
            normalized = decoded.strip().lower()
            if normalized in ("1", "true", "yes"):
                return True
            if normalized in ("0", "false", "no"):
                return False
            logger.warning(
                "[audit-retention] malformed legal_hold value; "
                "deletion disabled"
            )
            return True
    except Exception as exc:
        logger.debug(
            "[audit-retention] Redis legal_hold lookup failed; error_type=%s",
            safe_audit_error_type(exc),
        )
        return True

    # Fall back to settings default
    try:
        from supertable.config.settings import settings
        configured = settings.SUPERTABLE_AUDIT_LEGAL_HOLD
        return configured if type(configured) is bool else True
    except Exception:
        return True

    # Fail-safe: if we cannot determine, treat as hold active (never delete)
    return True  # pragma: no cover - both branches above return explicitly


# ---------------------------------------------------------------------------
# Legal hold — set
# ---------------------------------------------------------------------------

def set_legal_hold(enabled: bool, organization: str = "") -> Dict[str, Any]:
    """Activate or deactivate legal hold for an organization.

    Persists the state in Redis so it takes effect immediately without
    a process restart.  Emits a CONFIG_CHANGE audit event.

    Returns: {"ok": True, "legal_hold": bool, "organization": str}
    """
    if type(enabled) is not bool:
        return {"ok": False, "error": "enabled must be boolean"}
    if not organization:
        try:
            from supertable.config.settings import settings
            organization = settings.SUPERTABLE_ORGANIZATION or ""
        except Exception:
            pass
    if not organization:
        return {"ok": False, "error": "organization required"}
    try:
        _legal_hold_key(organization)
    except (TypeError, ValueError):
        return {"ok": False, "error": "organization is invalid"}

    # Persist to Redis
    try:
        from supertable.redis_infra import redis_client
        redis_client.set(_legal_hold_key(organization), "1" if enabled else "0")
    except Exception as exc:
        error_type = safe_audit_error_type(exc)
        logger.error(
            "[audit-retention] legal hold persistence failed; error_type=%s",
            error_type,
        )
        return {
            "ok": False,
            "error": "Redis persistence failed",
            "error_type": error_type,
        }

    logger.info(
        "[audit-retention] Legal hold %s for org=%s",
        "activated" if enabled else "deactivated",
        organization,
    )

    # Legal-hold changes are compliance mutations; never report success when
    # their audit event could not be durably admitted.
    try:
        from supertable.audit import emit as _audit, EventCategory, Actions, Severity, make_detail
        _audit(
            category=EventCategory.CONFIG_CHANGE,
            action=Actions.LEGAL_HOLD_CHANGE,
            organization=organization,
            resource_type="audit_legal_hold",
            resource_id=organization,
            severity=Severity.CRITICAL,
            detail=make_detail(
                legal_hold=enabled,
                action="activated" if enabled else "deactivated",
            ),
        )
    except AuditEncryptionError:
        raise
    except Exception as exc:
        raise AuditRetentionDurabilityError(
            "legal-hold mutation was not durably audited"
        ) from exc

    return {"ok": True, "legal_hold": enabled, "organization": organization}


# ---------------------------------------------------------------------------
# Retention enforcement
# ---------------------------------------------------------------------------

def enforce_retention(organization: str) -> Dict[str, Any]:
    """Delete audit Parquet partitions older than the configured retention period.

    Scans the storage partition tree, identifies day-level partitions
    whose date is older than ``now - AUDIT_RETENTION_DAYS``, and deletes
    them.  Each run is recorded as an audit event (meta-event).

    Skips all deletions if legal hold is active.

    Returns:
        {
            "deleted_partitions": int,
            "skipped_legal_hold": bool,
            "retention_days": int,
            "cutoff_date": str,          # ISO date
            "errors": [...],             # sanitized operation failures
            "deleted_paths": [...],      # canonical date labels, not paths
        }
    """
    result: Dict[str, Any] = {
        "deleted_partitions": 0,
        "skipped_legal_hold": False,
        "retention_days": 0,
        "cutoff_date": "",
        "errors": [],
        "deleted_paths": [],
        "organization": organization,
    }

    if not organization:
        result["errors"].append("organization is empty")
        return result
    try:
        _legal_hold_key(organization)
    except (TypeError, ValueError):
        result["organization"] = ""
        result["errors"].append("organization is invalid")
        return result

    # ── Legal hold check (fail-safe: if check fails, do not delete) ──
    if is_legal_hold_active(organization):
        logger.info("[audit-retention] Legal hold active — skipping retention for %s", organization)
        result["skipped_legal_hold"] = True
        return result

    # ── Load retention period ──
    try:
        from supertable.config.settings import settings
        retention_days = int(settings.SUPERTABLE_AUDIT_RETENTION_DAYS)
    except Exception:
        retention_days = 2555  # ~7 years default

    if retention_days <= 0:
        logger.info("[audit-retention] Retention disabled (days=%d) for %s", retention_days, organization)
        result["retention_days"] = retention_days
        return result

    result["retention_days"] = retention_days
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
    result["cutoff_date"] = cutoff.strftime("%Y-%m-%d")

    # ── Enumerate partitions ──
    try:
        from supertable.audit.writer_parquet import ParquetAuditWriter
        writer = ParquetAuditWriter()
        partitions = writer.list_partitions(organization)
    except Exception as exc:
        error_type = safe_audit_error_type(exc)
        logger.error(
            "[audit-retention] partition enumeration failed; error_type=%s",
            error_type,
        )
        result["errors"].append(
            f"partition enumeration failed; error_type={error_type}"
        )
        return result

    if not partitions:
        logger.debug("[audit-retention] No partitions found for %s", organization)
        return result

    # ── Identify and delete old partitions ──
    try:
        from supertable.storage.storage_factory import get_storage
        storage = get_storage()
    except Exception as exc:
        error_type = safe_audit_error_type(exc)
        logger.error(
            "[audit-retention] storage initialization failed; error_type=%s",
            error_type,
        )
        result["errors"].append(
            f"storage initialization failed; error_type={error_type}"
        )
        return result

    deleted_count = 0

    for partition_path in partitions:
        partition_date = _parse_partition_date(partition_path)
        if partition_date is None:
            logger.debug(
                "[audit-retention] Skipping unparseable partition entry"
            )
            continue

        if partition_date >= cutoff:
            # This partition is within retention — and since the list is
            # sorted, all subsequent partitions will also be within retention.
            break

        # Delete the partition directory
        partition_label = partition_date.strftime(
            "year=%Y/month=%m/day=%d"
        )
        try:
            storage.delete(partition_path)
            deleted_count += 1
            result["deleted_paths"].append(partition_label)
            logger.info(
                "[audit-retention] Deleted partition date=%s cutoff=%s",
                partition_date.strftime("%Y-%m-%d"), result["cutoff_date"],
            )
        except FileNotFoundError:
            # Already deleted (race condition or concurrent cleanup) — not an error
            pass
        except Exception as exc:
            error_type = safe_audit_error_type(exc)
            logger.error(
                "[audit-retention] partition deletion failed; "
                "partition=%s error_type=%s",
                partition_label, error_type,
            )
            result["errors"].append(
                "partition deletion failed "
                f"({partition_label}); error_type={error_type}"
            )

    result["deleted_partitions"] = deleted_count

    # ── Audit the retention run itself (meta-event) ──
    try:
        from supertable.audit import emit as _audit, EventCategory, Actions, Severity, make_detail
        _audit(
            category=EventCategory.SYSTEM,
            action=Actions.RETENTION_EXECUTE,
            organization=organization,
            resource_type="audit_retention",
            resource_id=organization,
            severity=Severity.WARNING if deleted_count > 0 else Severity.INFO,
            detail=make_detail(
                retention_days=retention_days,
                cutoff_date=result["cutoff_date"],
                deleted_partitions=deleted_count,
                error_count=len(result["errors"]),
            ),
        )
    except AuditEncryptionError:
        raise
    except Exception as exc:
        raise AuditRetentionDurabilityError(
            "retention mutation was not durably audited"
        ) from exc

    return result
