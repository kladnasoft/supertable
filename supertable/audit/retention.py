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
audits its own cleanup).  If audit emission fails, the deletion still
proceeds — audit failures must never block retention.

Compliance: DORA Art. 12 (5+ year retention), SOC 2 A1.2.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Redis key for legal hold runtime override
# ---------------------------------------------------------------------------

_LEGAL_HOLD_KEY_TEMPLATE = "supertable:{org}:audit:legal_hold"


def _legal_hold_key(org: str) -> str:
    return _LEGAL_HOLD_KEY_TEMPLATE.replace("{org}", org)


# ---------------------------------------------------------------------------
# Partition path parsing
# ---------------------------------------------------------------------------

_PARTITION_RE = re.compile(
    r"year=(\d{4})[/\\]month=(\d{2})[/\\]day=(\d{2})/?$"
)


def _parse_partition_date(partition_path: str) -> Optional[datetime]:
    """Extract the date from a Hive-style partition path.

    Matches paths like ``…/year=2024/month=03/day=15`` or
    ``…/year=2024/month=03/day=15/``.

    Returns a timezone-aware UTC datetime at midnight, or None if the
    path does not match the expected partition naming convention.
    """
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
    # Try Redis first (runtime override)
    try:
        from supertable.server_common import redis_client
        val = redis_client.get(_legal_hold_key(organization))
        if val is not None:
            decoded = val if isinstance(val, str) else val.decode("utf-8")
            return decoded.lower() in ("1", "true", "yes")
    except Exception as e:
        logger.debug("[audit-retention] Redis legal_hold lookup failed: %s", e)

    # Fall back to settings default
    try:
        from supertable.config.settings import settings
        return bool(settings.SUPERTABLE_AUDIT_LEGAL_HOLD)
    except Exception:
        pass

    # Fail-safe: if we cannot determine, treat as hold active (never delete)
    return True


# ---------------------------------------------------------------------------
# Legal hold — set
# ---------------------------------------------------------------------------

def set_legal_hold(enabled: bool, organization: str = "") -> Dict[str, Any]:
    """Activate or deactivate legal hold for an organization.

    Persists the state in Redis so it takes effect immediately without
    a process restart.  Emits a CONFIG_CHANGE audit event.

    Returns: {"ok": True, "legal_hold": bool, "organization": str}
    """
    if not organization:
        try:
            from supertable.config.settings import settings
            organization = settings.SUPERTABLE_ORGANIZATION or ""
        except Exception:
            pass
    if not organization:
        return {"ok": False, "error": "organization required"}

    # Persist to Redis
    try:
        from supertable.server_common import redis_client
        redis_client.set(_legal_hold_key(organization), "1" if enabled else "0")
    except Exception as e:
        logger.error("[audit-retention] Failed to persist legal hold to Redis: %s", e)
        return {"ok": False, "error": f"Redis persistence failed: {e}"}

    logger.info(
        "[audit-retention] Legal hold %s for org=%s",
        "activated" if enabled else "deactivated",
        organization,
    )

    # Audit the change (never fail the operation if audit fails)
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
    except Exception:
        pass

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
            "errors": [...],             # partition paths that failed
            "deleted_paths": [...],      # paths that were deleted
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
    except Exception as e:
        logger.error("[audit-retention] Failed to enumerate partitions: %s", e)
        result["errors"].append(f"partition enumeration failed: {e}")
        return result

    if not partitions:
        logger.debug("[audit-retention] No partitions found for %s", organization)
        return result

    # ── Identify and delete old partitions ──
    try:
        from supertable.storage.storage_factory import get_storage
        storage = get_storage()
    except Exception as e:
        logger.error("[audit-retention] Failed to get storage backend: %s", e)
        result["errors"].append(f"storage init failed: {e}")
        return result

    deleted_count = 0

    for partition_path in partitions:
        partition_date = _parse_partition_date(partition_path)
        if partition_date is None:
            logger.debug("[audit-retention] Skipping unparseable partition: %s", partition_path)
            continue

        if partition_date >= cutoff:
            # This partition is within retention — and since the list is
            # sorted, all subsequent partitions will also be within retention.
            break

        # Delete the partition directory
        try:
            storage.delete(partition_path)
            deleted_count += 1
            result["deleted_paths"].append(partition_path)
            logger.info(
                "[audit-retention] Deleted partition %s (date=%s, cutoff=%s)",
                partition_path, partition_date.strftime("%Y-%m-%d"), result["cutoff_date"],
            )
        except FileNotFoundError:
            # Already deleted (race condition or concurrent cleanup) — not an error
            pass
        except Exception as e:
            logger.error("[audit-retention] Failed to delete %s: %s", partition_path, e)
            result["errors"].append(f"{partition_path}: {e}")

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
    except Exception:
        pass

    return result
