# route: supertable.audit.reader
"""
Audit event query interface.

Provides a unified query API that automatically routes to Redis Streams
(hot tier, last 24h) or Parquet files (warm tier, historical) based
on the requested time range.

Full implementation: Phase 7.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def query_audit_log(
    organization: str,
    *,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    category: Optional[str] = None,
    action: Optional[str] = None,
    actor_id: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    outcome: Optional[str] = None,
    severity: Optional[str] = None,
    correlation_id: Optional[str] = None,
    limit: int = 500,
    source: str = "auto",
) -> List[Dict[str, Any]]:
    """Query audit events with filters.

    source="auto": uses Redis for queries within the last 24h,
    falls back to Parquet for older ranges.
    """
    if not organization:
        return []

    now_ms = int(time.time() * 1000)
    cutoff_24h = now_ms - (24 * 3600 * 1000)

    use_redis = source in ("redis", "auto") and (start_ms is None or start_ms >= cutoff_24h)

    results: List[Dict[str, Any]] = []

    if use_redis:
        try:
            from supertable.server_common import redis_client
            from supertable.audit.writer_redis import RedisAuditWriter
            writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
            raw = writer.query(start_ms=start_ms, end_ms=end_ms, count=limit)
            results = raw
        except Exception as e:
            logger.warning("[audit-reader] Redis query failed: %s", e)

    # TODO Phase 7: Parquet query for historical data when source != "redis"
    # and results are insufficient.

    # Apply filters
    filtered = []
    for event in results:
        if category and event.get("category") != category:
            continue
        if action and event.get("action") != action:
            continue
        if actor_id and event.get("actor_id") != actor_id:
            continue
        if resource_type and event.get("resource_type") != resource_type:
            continue
        if resource_id and event.get("resource_id") != resource_id:
            continue
        if outcome and event.get("outcome") != outcome:
            continue
        if severity and event.get("severity") != severity:
            continue
        if correlation_id and event.get("correlation_id") != correlation_id:
            continue
        filtered.append(event)
        if len(filtered) >= limit:
            break

    return filtered


def verify_chain_integrity(
    organization: str,
    date: str,
) -> Dict[str, Any]:
    """Verify hash chain integrity for a specific day.

    Returns: {"valid": True/False, "batches": N, "gaps": [...]}
    """
    # TODO Phase 7: full implementation
    return {
        "valid": True,
        "date": date,
        "organization": organization,
        "batches": 0,
        "gaps": [],
        "status": "not_yet_implemented",
    }
