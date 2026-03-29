# route: supertable.audit.retention
"""
Audit log retention policy enforcement and legal hold management.

Runs as a background task (registered via FastAPI lifespan hook):
  - Daily: delete Parquet partitions older than AUDIT_RETENTION_DAYS
  - Respects AUDIT_LEGAL_HOLD (global kill switch for deletion)
  - Records all deletions as audit events (meta-event)

Full implementation: Phase 10.

Compliance: DORA Art. 12 (5+ year retention), SOC 2 A1.2.
"""
from __future__ import annotations

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


def enforce_retention(organization: str) -> Dict[str, Any]:
    """Delete audit partitions older than the configured retention period.

    Returns summary: {"deleted_partitions": N, "skipped_legal_hold": bool}
    """
    try:
        from supertable.config.settings import settings
        if settings.SUPERTABLE_AUDIT_LEGAL_HOLD:
            logger.info("[audit-retention] Legal hold active — skipping retention for %s", organization)
            return {"deleted_partitions": 0, "skipped_legal_hold": True}
    except Exception:
        pass

    # TODO Phase 10: enumerate partitions, compute age, delete old ones
    return {"deleted_partitions": 0, "skipped_legal_hold": False, "status": "not_yet_implemented"}


def set_legal_hold(enabled: bool) -> None:
    """Activate or deactivate global legal hold.

    When active, no audit data is deleted regardless of retention policy.
    """
    # TODO Phase 10: persist to settings/Redis, emit config_change audit event
    logger.info("[audit-retention] Legal hold %s", "activated" if enabled else "deactivated")
