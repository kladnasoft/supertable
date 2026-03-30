# route: supertable.audit.reader
"""
Audit event query interface.

Provides a unified query API that automatically routes to Redis Streams
(hot tier, last 24h) or Parquet files (warm tier, historical) based
on the requested time range.

Also provides chain integrity verification: replays the SHA-256 chain
from Parquet batch files for a given day and compares against the stored
Merkle proof.

Compliance: DORA Art. 12 (record keeping), SOC 2 CC7.3 (forensic integrity).
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from supertable.audit.chain import (
    compute_batch_hash,
    compute_chain_hash,
    verify_merkle_proof,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event filtering (shared by Redis and Parquet query paths)
# ---------------------------------------------------------------------------

def _apply_filters(
    events: List[Dict[str, Any]],
    *,
    category: Optional[str] = None,
    action: Optional[str] = None,
    actor_id: Optional[str] = None,
    resource_type: Optional[str] = None,
    resource_id: Optional[str] = None,
    outcome: Optional[str] = None,
    severity: Optional[str] = None,
    correlation_id: Optional[str] = None,
    start_ms: Optional[int] = None,
    end_ms: Optional[int] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    """Apply field filters and time-range bounds to a list of event dicts."""
    filtered = []
    for event in events:
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
        # Time-range bounds (for Parquet results which are not pre-filtered)
        ts = 0
        try:
            ts = int(event.get("timestamp_ms", 0))
        except (TypeError, ValueError):
            pass
        if start_ms and ts and ts < start_ms:
            continue
        if end_ms and ts and ts > end_ms:
            continue
        filtered.append(event)
        if len(filtered) >= limit:
            break
    return filtered


# ---------------------------------------------------------------------------
# Redis (hot tier) query
# ---------------------------------------------------------------------------

def _query_redis(
    organization: str,
    start_ms: Optional[int],
    end_ms: Optional[int],
    limit: int,
) -> List[Dict[str, Any]]:
    """Query recent events from the Redis Stream."""
    try:
        from supertable.server_common import redis_client
        from supertable.audit.writer_redis import RedisAuditWriter
        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        return writer.query(start_ms=start_ms, end_ms=end_ms, count=limit)
    except Exception as e:
        logger.warning("[audit-reader] Redis query failed: %s", e)
        return []


# ---------------------------------------------------------------------------
# Parquet (warm tier) query
# ---------------------------------------------------------------------------

def _query_parquet(
    organization: str,
    start_ms: Optional[int],
    end_ms: Optional[int],
    limit: int,
) -> List[Dict[str, Any]]:
    """Query historical events from Parquet files on storage.

    Determines which day partitions to scan from the time range,
    reads the Parquet files, and returns a flat list of event dicts.
    """
    from datetime import datetime, timezone

    try:
        from supertable.audit.writer_parquet import ParquetAuditWriter
        writer = ParquetAuditWriter()
    except Exception as e:
        logger.warning("[audit-reader] Parquet writer init failed: %s", e)
        return []

    # Determine the day range to scan
    now_ms = int(time.time() * 1000)
    eff_start_ms = start_ms if start_ms else 0
    eff_end_ms = end_ms if end_ms else now_ms

    start_dt = datetime.fromtimestamp(eff_start_ms / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(eff_end_ms / 1000, tz=timezone.utc)

    events: List[Dict[str, Any]] = []
    current_date = start_dt.date()
    end_date = end_dt.date()

    # Cap at 366 days to prevent unbounded scans on wide-open queries
    from datetime import timedelta
    max_end = current_date + timedelta(days=366)
    if end_date > max_end:
        end_date = max_end

    while current_date <= end_date:
        if len(events) >= limit:
            break

        batches = writer.read_batch_events(
            organization,
            year=current_date.year,
            month=current_date.month,
            day=current_date.day,
        )
        for batch in batches:
            for event in batch.get("events", []):
                events.append(event)
                if len(events) >= limit:
                    break
            if len(events) >= limit:
                break

        current_date += timedelta(days=1)

    return events


# ---------------------------------------------------------------------------
# Unified query
# ---------------------------------------------------------------------------

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

    source="auto" (default):
      - If the query range falls entirely within the last 24h, queries
        Redis only (fast path).
      - If the range extends beyond 24h, queries Parquet for the
        historical portion and Redis for the recent portion, then
        merges the results.
    source="redis": queries Redis only (hot tier).
    source="parquet": queries Parquet only (warm tier).
    """
    if not organization:
        return []

    now_ms = int(time.time() * 1000)
    cutoff_24h = now_ms - (24 * 3600 * 1000)

    filter_kwargs = dict(
        category=category,
        action=action,
        actor_id=actor_id,
        resource_type=resource_type,
        resource_id=resource_id,
        outcome=outcome,
        severity=severity,
        correlation_id=correlation_id,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=limit,
    )

    results: List[Dict[str, Any]] = []

    if source == "redis":
        results = _query_redis(organization, start_ms, end_ms, limit)
        return _apply_filters(results, **filter_kwargs)

    if source == "parquet":
        results = _query_parquet(organization, start_ms, end_ms, limit)
        return _apply_filters(results, **filter_kwargs)

    # source == "auto"
    query_is_recent = (start_ms is None or start_ms >= cutoff_24h)

    if query_is_recent:
        # Everything falls within the Redis hot tier
        results = _query_redis(organization, start_ms, end_ms, limit)
        return _apply_filters(results, **filter_kwargs)

    # Historical query — need both tiers
    # 1) Parquet for the historical portion (start_ms → cutoff_24h)
    parquet_events = _query_parquet(organization, start_ms, cutoff_24h, limit)

    # 2) Redis for the recent portion (cutoff_24h → end_ms)
    remaining = limit - len(parquet_events)
    redis_events: List[Dict[str, Any]] = []
    if remaining > 0:
        eff_end = end_ms if end_ms else now_ms
        if eff_end > cutoff_24h:
            redis_events = _query_redis(organization, cutoff_24h, end_ms, remaining)

    # Merge — Parquet events first (older), then Redis (newer)
    combined = parquet_events + redis_events

    # Deduplicate by event_id (a batch may appear in both tiers
    # if it was written right around the 24h boundary)
    seen_ids: set = set()
    deduped: List[Dict[str, Any]] = []
    for event in combined:
        eid = event.get("event_id", "")
        if eid and eid in seen_ids:
            continue
        if eid:
            seen_ids.add(eid)
        deduped.append(event)

    return _apply_filters(deduped, **filter_kwargs)


# ---------------------------------------------------------------------------
# Chain integrity verification
# ---------------------------------------------------------------------------

def verify_chain_integrity(
    organization: str,
    date: str,
) -> Dict[str, Any]:
    """Verify hash chain integrity for a specific day.

    Reads all Parquet batch files for the given date, groups them by
    server instance, replays the SHA-256 chain for each instance, and
    cross-references against the stored daily Merkle proof.

    Args:
        organization: tenant organization name
        date: date string in ``YYYY-MM-DD`` or ``YYYYMMDD`` format

    Returns:
        {
            "valid": bool,                # overall verdict
            "date": str,
            "organization": str,
            "instances": {
                "<instance_id>": {
                    "batches": int,
                    "events": int,
                    "chain_valid": bool,
                    "gaps": [...],
                }
            },
            "merkle_proof": {             # null if no proof file
                "valid": bool,
                "computed_root": str,
                "recorded_root": str,
            },
            "total_batches": int,
            "total_events": int,
        }
    """
    from datetime import datetime as _dt

    result: Dict[str, Any] = {
        "valid": False,
        "date": date,
        "organization": organization,
        "instances": {},
        "merkle_proof": None,
        "total_batches": 0,
        "total_events": 0,
    }

    if not organization or not date:
        return result

    # Parse date
    clean_date = date.replace("-", "")
    try:
        dt = _dt.strptime(clean_date, "%Y%m%d")
        year, month, day = dt.year, dt.month, dt.day
    except ValueError:
        result["error"] = f"Invalid date format: {date}"
        return result

    # ── Read all Parquet batch files for this day ──
    try:
        from supertable.audit.writer_parquet import ParquetAuditWriter
        writer = ParquetAuditWriter()
        batches = writer.read_batch_events(organization, year, month, day)
    except Exception as e:
        logger.error("[audit-reader] Failed to read batch events for %s: %s", date, e)
        result["error"] = f"Failed to read Parquet files: {e}"
        return result

    if not batches:
        # No data for this day — technically valid (nothing to tamper with)
        result["valid"] = True
        result["info"] = "No audit data found for this date"
        return result

    # ── Group batches by instance_id and sort by timestamp ──
    instance_batches: Dict[str, List[Dict[str, Any]]] = {}
    for batch in batches:
        inst = batch.get("instance_id", "unknown")
        instance_batches.setdefault(inst, []).append(batch)

    # Sort each instance's batches by min_timestamp_ms (chronological order)
    for inst in instance_batches:
        instance_batches[inst].sort(key=lambda b: b.get("min_timestamp_ms", 0))

    # ── Replay chain for each instance ──
    all_valid = True
    total_batches = 0
    total_events = 0

    for inst_id, inst_batch_list in instance_batches.items():
        inst_result: Dict[str, Any] = {
            "batches": len(inst_batch_list),
            "events": 0,
            "chain_valid": True,
            "gaps": [],
        }

        prev_chain_hash = ""

        for i, batch in enumerate(inst_batch_list):
            event_ids = batch.get("event_ids", [])
            recorded_chain_hash = batch.get("chain_hash", "")
            event_count = batch.get("event_count", 0)

            inst_result["events"] += event_count

            if not recorded_chain_hash:
                # Hash chaining was disabled for this batch — skip verification
                continue

            # ── Bug fix 1: the logger calls chain.advance(event_ids) without
            # passing a file_hash, so batch_hash is computed with file_hash="".
            # We must match that here — NOT use the Parquet file hash.
            expected_batch_hash = compute_batch_hash(event_ids, "")

            # ── Bug fix 2: the chain is continuous across days (restored from
            # Redis on startup).  We do NOT know what the chain head was at the
            # start of this day.  For the first batch of each instance we accept
            # the recorded chain_hash as the baseline.  For subsequent batches
            # we verify that chain_hash = SHA-256(prev_recorded_chain_hash + batch_hash).
            if i == 0 or not prev_chain_hash:
                # First batch: we cannot verify the absolute starting point,
                # but we record it as the baseline for verifying subsequent batches.
                prev_chain_hash = recorded_chain_hash
                continue

            expected_chain_hash = compute_chain_hash(prev_chain_hash, expected_batch_hash)

            if expected_chain_hash != recorded_chain_hash:
                inst_result["chain_valid"] = False
                inst_result["gaps"].append({
                    "batch_index": i,
                    "file_path": batch.get("file_path", ""),
                    "expected_chain_hash": expected_chain_hash,
                    "recorded_chain_hash": recorded_chain_hash,
                    "event_count": event_count,
                })
                all_valid = False

            prev_chain_hash = recorded_chain_hash

        total_batches += inst_result["batches"]
        total_events += inst_result["events"]
        result["instances"][inst_id] = inst_result

    result["total_batches"] = total_batches
    result["total_events"] = total_events

    # ── Cross-reference with stored Merkle proof ──
    try:
        from supertable.audit.writer_parquet import ParquetAuditWriter
        proof_writer = ParquetAuditWriter()
        proof = proof_writer.load_chain_proof(organization, clean_date)
        if proof:
            merkle_result = verify_merkle_proof(proof)
            result["merkle_proof"] = merkle_result
            if not merkle_result.get("valid", False):
                all_valid = False
    except Exception as e:
        logger.warning("[audit-reader] Merkle proof check failed: %s", e)

    result["valid"] = all_valid
    return result
