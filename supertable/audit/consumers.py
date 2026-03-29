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

logger = logging.getLogger(__name__)


def create_consumer(organization: str, group_name: str, start_from: str = "$") -> Dict[str, Any]:
    """Create an external SIEM consumer group on the audit stream."""
    try:
        from supertable.server_common import redis_client
        from supertable.audit.writer_redis import RedisAuditWriter
        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        ok = writer.create_consumer_group(group_name, start_from)
        return {"success": ok, "group_name": group_name, "start_from": start_from}
    except Exception as e:
        logger.error("[audit-consumers] create failed: %s", e)
        return {"success": False, "error": str(e)}


def delete_consumer(organization: str, group_name: str) -> Dict[str, Any]:
    """Remove an external SIEM consumer group."""
    try:
        from supertable.server_common import redis_client
        from supertable.audit.writer_redis import RedisAuditWriter
        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        ok = writer.delete_consumer_group(group_name)
        return {"success": ok, "group_name": group_name}
    except Exception as e:
        logger.error("[audit-consumers] delete failed: %s", e)
        return {"success": False, "error": str(e)}


def list_consumers(organization: str) -> List[Dict[str, Any]]:
    """List all consumer groups with lag info."""
    try:
        from supertable.server_common import redis_client
        from supertable.audit.writer_redis import RedisAuditWriter
        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        return writer.list_consumer_groups()
    except Exception as e:
        logger.error("[audit-consumers] list failed: %s", e)
        return []
