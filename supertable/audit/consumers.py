# route: supertable.audit.consumers
"""
SIEM consumer group management for external audit event consumption.

External tools (Splunk, Sentinel, ELK, Datadog) register consumer groups
on the Redis Stream and consume events independently of the internal
archival worker.

Full implementation: Phase 8.

Compliance: DORA Art. 10 (information sharing), SOC 2 CC7.1 (monitoring).
"""
from __future__ import annotations

import logging
from typing import Any, Dict, List

from supertable.audit.diagnostics import safe_audit_error_type

logger = logging.getLogger(__name__)


def create_consumer(organization: str, group_name: str, start_from: str = "$") -> Dict[str, Any]:
    """Create an external SIEM consumer group on the audit stream."""
    try:
        from supertable.redis_infra import redis_client
        from supertable.audit.admin import get_audit_config
        from supertable.audit.writer_redis import RedisAuditWriter
        from supertable.config.settings import settings

        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        config = get_audit_config(organization, strict=True)
        if not config["siem_enabled"]:
            return {
                "success": False,
                "error": "SIEM audit consumers are disabled",
            }
        maximum = settings.SUPERTABLE_AUDIT_SIEM_MAX_CONSUMERS
        ok = writer.create_consumer_group(
            group_name,
            start_from,
            max_consumers=maximum,
        )
        return {"success": ok, "group_name": group_name, "start_from": start_from}
    except Exception as exc:
        error_type = safe_audit_error_type(exc)
        logger.error(
            "[audit-consumers] create failed; error_type=%s", error_type,
        )
        return {
            "success": False,
            "error": "consumer creation failed",
            "error_type": error_type,
        }


def delete_consumer(organization: str, group_name: str) -> Dict[str, Any]:
    """Remove an external SIEM consumer group."""
    try:
        from supertable.redis_infra import redis_client
        from supertable.audit.writer_redis import RedisAuditWriter
        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        ok = writer.delete_consumer_group(group_name)
        return {"success": ok, "group_name": group_name}
    except Exception as exc:
        error_type = safe_audit_error_type(exc)
        logger.error(
            "[audit-consumers] delete failed; error_type=%s", error_type,
        )
        return {
            "success": False,
            "error": "consumer deletion failed",
            "error_type": error_type,
        }


def list_consumers(organization: str) -> List[Dict[str, Any]]:
    """List all consumer groups with lag info."""
    try:
        from supertable.redis_infra import redis_client
        from supertable.audit.writer_redis import RedisAuditWriter
        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        return writer.list_consumer_groups()
    except Exception as exc:
        logger.error(
            "[audit-consumers] list failed; error_type=%s",
            safe_audit_error_type(exc),
        )
        return []
