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

import json
import logging
import time
from hashlib import sha256
from typing import Any, Dict, List, Optional, TypedDict

from supertable.audit.chain import (
    GENESIS_HASH,
    compute_chain_hash,
    compute_event_batch_hash,
    verify_merkle_proof,
)
from supertable.audit.diagnostics import safe_audit_error_type

logger = logging.getLogger(__name__)

_MAX_AUDIT_QUERY_LIMIT = 10_000
_MAX_AUDIT_QUERY_DAY_PARTITIONS = 31
_MAX_TIMESTAMP_MS = 253_402_300_799_999  # 9999-12-31T23:59:59.999Z
_MAX_AUDIT_QUERY_SCAN_EVENTS = 250_000
_MAX_AUDIT_QUERY_SCAN_BYTES = 256 * 1024 * 1024
_MAX_AUDIT_QUERY_RESULT_BYTES = 64 * 1024 * 1024
_AUDIT_QUERY_RETAINED_OVERHEAD = 2 * 1024
_REDIS_QUERY_PAGE = 256


class AuditQueryError(RuntimeError):
    """An admitted audit query could not be completed without partial data."""


class _FilterKwargs(TypedDict):
    category: Optional[str]
    action: Optional[str]
    actor_id: Optional[str]
    resource_type: Optional[str]
    resource_id: Optional[str]
    outcome: Optional[str]
    severity: Optional[str]
    correlation_id: Optional[str]
    start_ms: Optional[int]
    end_ms: Optional[int]
    limit: int


def _opaque_file_ref(value: Any) -> str:
    """Return a non-reversible reference without exposing a backend path."""
    if isinstance(value, bytes):
        raw = value
    elif isinstance(value, str):
        raw = value.encode("utf-8", errors="replace")
    else:
        raw = b"<unavailable>"
    return f"sha256:{sha256(raw).hexdigest()[:16]}"


def _query_event_size(event: Dict[str, Any]) -> int:
    """Bound retained query records using their canonical JSON footprint."""
    try:
        size = len(json.dumps(
            event,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8"))
    except (TypeError, ValueError, UnicodeEncodeError):
        raise AuditQueryError("audit query returned an invalid event") from None
    if size > 96 * 1024:
        raise AuditQueryError("audit query event exceeds its byte limit")
    return size + _AUDIT_QUERY_RETAINED_OVERHEAD


def _canonical_proof_bytes(proof: Any) -> bytes:
    """Serialize a proof without retaining poisoned decode failures."""
    try:
        return json.dumps(
            proof.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except Exception:
        raise AuditQueryError("audit proof is not canonical") from None


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
        if start_ms is not None and ts < start_ms:
            continue
        if end_ms is not None and ts > end_ms:
            continue
        filtered.append(event)
    # Sort by timestamp descending (newest first) then apply limit
    def _timestamp(event: Dict[str, Any]) -> int:
        try:
            value = int(event.get("timestamp_ms", 0) or 0)
        except (TypeError, ValueError, OverflowError):
            return 0
        return value

    filtered.sort(key=_timestamp, reverse=True)
    return filtered[:limit]


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
        from supertable.redis_infra import redis_client
        from supertable.audit.writer_redis import RedisAuditWriter
        writer = RedisAuditWriter(redis_client, organization, "", maxlen=0)
        results: List[Dict[str, Any]] = []
        scanned_bytes = 0
        cursor = "+"
        while True:
            remaining = _MAX_AUDIT_QUERY_SCAN_EVENTS - len(results)
            if remaining <= 0:
                probe = writer.query(
                    count=1,
                    min_stream_id="-",
                    max_stream_id=cursor,
                )
                if probe:
                    raise AuditQueryError(
                        "audit hot-tier query exceeds its scan ceiling"
                    )
                break
            page_size = min(_REDIS_QUERY_PAGE, remaining)
            page = writer.query(
                count=page_size,
                min_stream_id="-",
                max_stream_id=cursor,
            )
            for event in page:
                scanned_bytes += _query_event_size(event)
                if scanned_bytes > _MAX_AUDIT_QUERY_SCAN_BYTES:
                    raise AuditQueryError(
                        "audit hot-tier query exceeds its byte scan ceiling"
                    )
                results.append(event)
            if len(page) < page_size:
                break
            last_id = page[-1].get("_stream_id")
            if not isinstance(last_id, str):
                raise AuditQueryError("audit hot-tier cursor is invalid")
            cursor = f"({last_id}"
        return results
    except Exception as exc:
        logger.error(
            "[audit-reader] Redis query failed; error_type=%s",
            safe_audit_error_type(exc),
        )
        raise AuditQueryError("audit hot-tier query failed") from None


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

    # Determine the day range to scan
    now_ms = int(time.time() * 1000)
    # A warm-tier request without an explicit start is bounded to the most
    # recent admitted window instead of scanning from the Unix epoch.
    eff_end_ms = end_ms if end_ms is not None else now_ms
    eff_start_ms = (
        start_ms
        if start_ms is not None
        else max(0, eff_end_ms - (30 * 24 * 3600 * 1000))
    )

    try:
        start_dt = datetime.fromtimestamp(eff_start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(eff_end_ms / 1000, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        raise ValueError(
            "audit query timestamp is outside the supported range"
        ) from None

    events: List[Dict[str, Any]] = []
    scanned_bytes = 0
    current_date = start_dt.date()
    end_date = end_dt.date()

    from datetime import timedelta
    if (end_date - current_date).days >= _MAX_AUDIT_QUERY_DAY_PARTITIONS:
        raise ValueError("audit archive query exceeds the day-partition limit")

    try:
        from supertable.audit.writer_parquet import ParquetAuditWriter
        writer = ParquetAuditWriter()
    except Exception as exc:
        logger.error(
            "[audit-reader] Parquet writer init failed; error_type=%s",
            safe_audit_error_type(exc),
        )
        raise AuditQueryError("audit archive query failed") from None

    # Results are newest-first, so scan complete newer partitions before older
    # ones.  No result limit is consumed until public filters are applied.
    current_date = end_date
    while current_date >= start_dt.date():
        remaining = _MAX_AUDIT_QUERY_SCAN_EVENTS - len(events)
        if remaining <= 0:
            raise AuditQueryError(
                "audit archive query exceeds its event scan ceiling"
            )

        try:
            batches = writer.read_batch_events(
                organization,
                year=current_date.year,
                month=current_date.month,
                day=current_date.day,
                limit=remaining,
                strict=True,
            )
        except Exception as exc:
            logger.error(
                "[audit-reader] archive partition read failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            raise AuditQueryError("audit archive query failed") from None
        for batch in batches:
            for event in batch.get("events", []):
                scanned_bytes += _query_event_size(event)
                if scanned_bytes > _MAX_AUDIT_QUERY_SCAN_BYTES:
                    raise AuditQueryError(
                        "audit archive query exceeds its byte scan ceiling"
                    )
                events.append(event)

        current_date -= timedelta(days=1)

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
    from supertable import redis_keys as RK

    RK.audit_stream(organization)
    if source not in {"auto", "redis", "parquet"}:
        raise ValueError("audit query source is invalid")
    if type(limit) is not int or limit <= 0 or limit > _MAX_AUDIT_QUERY_LIMIT:
        raise ValueError("audit query limit must be between 1 and 10000")
    for label, timestamp_value in (("start", start_ms), ("end", end_ms)):
        if timestamp_value is None:
            continue
        if (
            type(timestamp_value) is not int
            or timestamp_value < 0
            or timestamp_value > _MAX_TIMESTAMP_MS
        ):
            raise ValueError(f"audit query {label} timestamp is invalid")
    if start_ms is not None and end_ms is not None and start_ms > end_ms:
        raise ValueError("audit query start must not exceed end")
    for label, filter_value in (
        ("category", category),
        ("action", action),
        ("actor_id", actor_id),
        ("resource_type", resource_type),
        ("resource_id", resource_id),
        ("outcome", outcome),
        ("severity", severity),
        ("correlation_id", correlation_id),
    ):
        if filter_value is None:
            continue
        try:
            valid = (
                isinstance(filter_value, str)
                and len(filter_value.encode("utf-8")) <= 1_024
            )
        except UnicodeEncodeError:
            valid = False
        if not valid:
            raise ValueError(f"audit query {label} filter is invalid")

    filter_kwargs: _FilterKwargs = {
        "category": category,
        "action": action,
        "actor_id": actor_id,
        "resource_type": resource_type,
        "resource_id": resource_id,
        "outcome": outcome,
        "severity": severity,
        "correlation_id": correlation_id,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "limit": limit,
    }

    if source == "redis":
        results = _query_redis(organization, start_ms, end_ms, limit)
        filtered = _apply_filters(results, **filter_kwargs)
        if sum(_query_event_size(event) for event in filtered) > _MAX_AUDIT_QUERY_RESULT_BYTES:
            raise AuditQueryError("audit query result exceeds its byte ceiling")
        return filtered

    if source == "parquet":
        results = _query_parquet(organization, start_ms, end_ms, limit)
        filtered = _apply_filters(results, **filter_kwargs)
        if sum(_query_event_size(event) for event in filtered) > _MAX_AUDIT_QUERY_RESULT_BYTES:
            raise AuditQueryError("audit query result exceeds its byte ceiling")
        return filtered

    # ``auto`` is complete by construction: Parquet is the durable system of
    # record, while Redis is a best-effort acceleration that may have gaps.
    results = _query_parquet(organization, start_ms, end_ms, limit)
    filtered = _apply_filters(results, **filter_kwargs)
    if sum(_query_event_size(event) for event in filtered) > _MAX_AUDIT_QUERY_RESULT_BYTES:
        raise AuditQueryError("audit query result exceeds its byte ceiling")
    return filtered


# ---------------------------------------------------------------------------
# Chain integrity verification
# ---------------------------------------------------------------------------

def verify_chain_integrity(
    organization: str,
    date: str,
) -> Dict[str, Any]:
    """Verify one complete, content-bound day against its stored proof.

    A day is never declared valid merely because no rows were returned.  The
    current proof is mandatory, all decoded rows are bound into the chain, and
    the starting head comes from the previous proof (or a provable genesis).
    Storage/decode limits and missing anchors fail closed.
    """
    from datetime import datetime as _dt, timedelta, timezone as _timezone
    from supertable import redis_keys as RK

    result: Dict[str, Any] = {
        "valid": False,
        "date": date,
        "organization": organization,
        "instances": {},
        "merkle_proof": None,
        "total_batches": 0,
        "total_events": 0,
    }

    if not isinstance(organization, str) or not isinstance(date, str):
        result["error"] = "Invalid audit verification request"
        return result

    try:
        # Validate the tenant before any read or proof path is constructed.
        RK.audit_stream(organization)
        if date != date.strip():
            raise ValueError("non-canonical date")
        clean_date = date.replace("-", "")
        dt = _dt.strptime(clean_date, "%Y%m%d")
        if date not in {dt.strftime("%Y%m%d"), dt.strftime("%Y-%m-%d")}:
            raise ValueError("non-canonical date")
        year, month, day = dt.year, dt.month, dt.day
    except (TypeError, ValueError):
        result["error"] = "Invalid audit verification request"
        return result

    try:
        from supertable.audit.writer_parquet import ParquetAuditWriter
        writer = ParquetAuditWriter()
        close_manifest = writer.load_day_close_manifest(
            organization, clean_date, strict=True,
        )
        if close_manifest is None:
            target_day = int(dt.replace(tzinfo=_timezone.utc).timestamp()) // 86_400
            current_day = int(time.time() * 1000) // 86_400_000
            journal_status = ""
            try:
                from supertable.audit.durable_journal import RedisAuditJournal
                from supertable.redis_infra import redis_client

                journal_status = RedisAuditJournal.inspect_day_state(
                    redis_client, organization, target_day,
                ).get("status", "")
            except Exception:
                journal_status = ""
            if journal_status == "closed":
                result["status"] = "invalid"
                result["error"] = (
                    "Closed audit day is missing its immutable manifest"
                )
            elif journal_status == "closing":
                result["status"] = "closing"
                result["error"] = "Audit day proof publication is still pending"
            elif target_day >= current_day or journal_status == "open":
                result["status"] = "open"
                result["error"] = "Audit day membership is still open"
            else:
                result["status"] = "unverifiable_unclosed"
                result["error"] = "Audit day has no immutable close manifest"
            return result
        close_day = close_manifest.get("day")
        cutover_day = close_manifest.get("cutover_day")
        if type(close_day) is not int or type(cutover_day) is not int:
            raise AuditQueryError("audit close manifest counters are invalid")
        batches = writer.read_batch_events(
            organization, year, month, day, strict=True,
        )
        proof = writer.load_chain_proof(
            organization, clean_date, strict=True,
        )
        previous_date = (dt - timedelta(days=1)).strftime("%Y%m%d")
        previous_proof = (
            writer.load_chain_proof(
                organization, previous_date, strict=True,
            )
            if close_day > cutover_day
            else None
        )
        previous_close_manifest = (
            writer.load_day_close_manifest(
                organization, previous_date, strict=True,
            )
            if previous_proof is not None
            else None
        )
    except Exception as exc:
        error_type = safe_audit_error_type(exc)
        logger.error(
            "[audit-reader] integrity input read failed; error_type=%s",
            error_type,
        )
        result["error"] = "Audit integrity inputs could not be read"
        result["error_type"] = error_type
        return result

    if proof is None:
        result["error"] = "Closed audit day proof is unavailable"
        result["status"] = "invalid"
        return result

    try:
        proof_raw = _canonical_proof_bytes(proof)
    except AuditQueryError:
        result["error"] = "Audit proof document is invalid"
        result["error_type"] = "AuditQueryError"
        result["status"] = "invalid"
        return result
    if sha256(proof_raw).hexdigest() != close_manifest.get("proof_hash"):
        result["error"] = "Audit proof differs from its closed-day manifest"
        result["status"] = "invalid"
        return result
    if previous_proof is not None:
        if previous_close_manifest is None:
            result["error"] = "Previous audit proof has no closed-day manifest"
            result["status"] = "invalid"
            return result
        try:
            previous_raw = _canonical_proof_bytes(previous_proof)
        except AuditQueryError:
            result["error"] = "Previous audit proof document is invalid"
            result["error_type"] = "AuditQueryError"
            result["status"] = "invalid"
            return result
        if sha256(previous_raw).hexdigest() != previous_close_manifest.get(
            "proof_hash"
        ):
            result["error"] = (
                "Previous audit proof differs from its closed-day manifest"
            )
            result["status"] = "invalid"
            return result

    expected_batch_ids = close_manifest.get("batch_ids")
    if not isinstance(expected_batch_ids, list):
        result["error"] = "Audit close manifest membership is unavailable"
        result["status"] = "invalid"
        return result
    selected_batches: List[Dict[str, Any]] = []
    observed_batch_ids: set[str] = set()
    expected_batch_id_set = set(expected_batch_ids)
    for batch in batches:
        path = batch.get("file_path") if isinstance(batch, dict) else None
        if not isinstance(path, str):
            result["error"] = "Audit archive membership is invalid"
            result["status"] = "invalid"
            return result
        filename = path.rsplit("/", 1)[-1]
        batch_id = filename.removesuffix(".parquet").rsplit("_", 1)[-1]
        if batch_id in expected_batch_id_set:
            if batch_id in observed_batch_ids:
                result["error"] = "Audit archive membership is duplicated"
                result["status"] = "invalid"
                return result
            observed_batch_ids.add(batch_id)
            selected_batches.append(batch)
    if observed_batch_ids != expected_batch_id_set:
        result["error"] = "Audit archive membership is incomplete"
        result["status"] = "invalid"
        return result
    batches = selected_batches

    try:
        def _canonical_hash(value: Any) -> bool:
            return (
                isinstance(value, str)
                and len(value) == 64
                and all(ch in "0123456789abcdef" for ch in value)
            )

        def _proof_date(value: Any) -> str:
            if not isinstance(value, str):
                raise ValueError("proof date is invalid")
            candidate = value.replace("-", "")
            parsed = _dt.strptime(candidate, "%Y%m%d")
            if value not in {
                parsed.strftime("%Y%m%d"), parsed.strftime("%Y-%m-%d"),
            }:
                raise ValueError("proof date is invalid")
            return parsed.strftime("%Y%m%d")

        def _proof_entries(value: Any, expected_date: str) -> Dict[str, Dict[str, Any]]:
            if _proof_date(value.date) != expected_date:
                raise ValueError("proof date does not match its partition")
            if not isinstance(value.instances, dict) or len(value.instances) > 4_096:
                raise ValueError("proof instances are invalid")
            if type(value.total_events) is not int or value.total_events < 0:
                raise ValueError("proof event total is invalid")
            if not _canonical_hash(value.merkle_root):
                raise ValueError("proof root is invalid")
            merkle = verify_merkle_proof(value)
            if not merkle.get("valid", False):
                raise ValueError("proof root does not match instance heads")
            entries: Dict[str, Dict[str, Any]] = {}
            event_total = 0
            for instance_id, entry in value.instances.items():
                RK.audit_chain_head(organization, instance_id)
                if not isinstance(entry, dict) or set(entry) != {
                    "head", "batches", "events",
                }:
                    raise ValueError("proof instance entry is invalid")
                head = entry.get("head")
                batches_count = entry.get("batches")
                events_count = entry.get("events")
                if not _canonical_hash(head):
                    raise ValueError("proof instance head is invalid")
                if type(batches_count) is not int or batches_count < 0:
                    raise ValueError("proof batch count is invalid")
                if type(events_count) is not int or events_count < 0:
                    raise ValueError("proof instance event count is invalid")
                if batches_count == 0 and head != GENESIS_HASH:
                    raise ValueError("empty proof chain has a non-genesis head")
                entries[instance_id] = {
                    "head": head,
                    "batches": batches_count,
                    "events": events_count,
                }
                event_total += events_count
            if event_total != value.total_events:
                raise ValueError("proof event total does not match its instances")
            return entries

        current_entries = _proof_entries(proof, clean_date)
        previous_entries = (
            _proof_entries(previous_proof, previous_date)
            if previous_proof is not None else {}
        )
        result["merkle_proof"] = verify_merkle_proof(proof)

        instance_batches: Dict[str, List[Dict[str, Any]]] = {}
        seen_event_ids: set[str] = set()
        total_events = 0
        for batch in batches:
            if not isinstance(batch, dict):
                raise ValueError("batch record is invalid")
            events = batch.get("events")
            if not isinstance(events, list) or not events:
                raise ValueError("batch events are missing")
            event_count = batch.get("event_count")
            if type(event_count) is not int or event_count != len(events):
                raise ValueError("batch event count is inconsistent")
            instance_id = batch.get("instance_id")
            chain_hash = batch.get("chain_hash")
            if not isinstance(instance_id, str):
                raise ValueError("batch instance identity is invalid")
            if not isinstance(chain_hash, str):
                raise ValueError("batch chain head is invalid")
            RK.audit_chain_head(organization, instance_id)
            if not _canonical_hash(chain_hash):
                raise ValueError("batch chain head is invalid")

            actual_ids: List[str] = []
            timestamps: List[int] = []
            for event in events:
                if not isinstance(event, dict):
                    raise ValueError("event record is invalid")
                event_id = event.get("event_id")
                if not isinstance(event_id, str) or not event_id:
                    raise ValueError("event ID is invalid")
                if event_id in seen_event_ids:
                    raise ValueError("event ID is duplicated")
                seen_event_ids.add(event_id)
                if (
                    event.get("organization") != organization
                    or event.get("instance_id") != instance_id
                    or event.get("chain_hash") != chain_hash
                ):
                    raise ValueError("event batch identity is inconsistent")
                timestamp_ms = event.get("timestamp_ms")
                if type(timestamp_ms) is not int or timestamp_ms < 0:
                    raise ValueError("event timestamp is invalid")
                actual_ids.append(event_id)
                timestamps.append(timestamp_ms)

            declared_ids = batch.get("event_ids")
            if (
                not isinstance(declared_ids, list)
                or declared_ids != sorted(actual_ids)
                or len(set(declared_ids)) != len(declared_ids)
            ):
                raise ValueError("batch event IDs are inconsistent")
            if batch.get("min_timestamp_ms") != min(timestamps):
                raise ValueError("batch timestamp metadata is inconsistent")
            batch["_content_hash"] = compute_event_batch_hash(events)
            instance_batches.setdefault(instance_id, []).append(batch)
            total_events += event_count

        if set(instance_batches) - set(current_entries):
            raise ValueError("observed instance is absent from proof")

        all_instances = set(current_entries) | set(previous_entries)
        for instance_id in sorted(all_instances):
            current_entry = current_entries.get(instance_id)
            previous_entry = previous_entries.get(instance_id)
            observed = list(instance_batches.get(instance_id, []))
            if current_entry is None:
                raise ValueError("prior instance is absent from current proof")

            prior_batches = previous_entry["batches"] if previous_entry else 0
            prior_head = previous_entry["head"] if previous_entry else GENESIS_HASH
            if previous_entry is None and current_entry["batches"] != len(observed):
                raise ValueError("instance predecessor anchor is unavailable")
            if current_entry["batches"] - prior_batches != len(observed):
                raise ValueError("proof batch count does not match observed batches")
            observed_events = sum(batch["event_count"] for batch in observed)
            if current_entry["events"] != observed_events:
                raise ValueError("proof event count does not match observed events")

            current_head = prior_head
            remaining = observed
            ordered: List[Dict[str, Any]] = []
            while remaining:
                candidates = [
                    batch for batch in remaining
                    if compute_chain_hash(
                        current_head, batch["_content_hash"],
                    ) == batch["chain_hash"]
                ]
                if len(candidates) != 1:
                    raise ValueError("batch chain order is missing or ambiguous")
                candidate = candidates[0]
                ordered.append(candidate)
                current_head = candidate["chain_hash"]
                remaining = [batch for batch in remaining if batch is not candidate]
            if current_head != current_entry["head"]:
                raise ValueError("proof terminal head does not match observed chain")

            result["instances"][instance_id] = {
                "batches": len(ordered),
                "events": observed_events,
                "chain_valid": True,
                "gaps": [],
                "starting_head": prior_head,
                "terminal_head": current_head,
                "file_refs": [
                    _opaque_file_ref(batch.get("file_path")) for batch in ordered
                ],
            }

        if total_events != proof.total_events:
            raise ValueError("proof total does not match observed events")
        if (
            close_manifest.get("admitted") != total_events
            or close_manifest.get("receipt_count") != len(batches)
        ):
            raise ValueError(
                "closed-day manifest does not match observed archive membership"
            )
        result["total_batches"] = len(batches)
        result["total_events"] = total_events
        result["valid"] = True
        result["status"] = "verified"
        return result
    except Exception as exc:
        logger.warning(
            "[audit-reader] integrity verification failed; error_type=%s",
            safe_audit_error_type(exc),
        )
        result["error"] = "Audit integrity verification failed"
        result["error_type"] = safe_audit_error_type(exc)
        result["status"] = "invalid"
        return result
