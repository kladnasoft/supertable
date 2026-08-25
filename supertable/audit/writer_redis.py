# route: supertable.audit.writer_redis
"""
Hot-tier audit writer using Redis Streams.

Provides real-time queryability (XRANGE), consumer groups for external
SIEM tools (Splunk, Sentinel, ELK), and TTL-based eviction.

Each organization gets its own stream:
    supertable:{org}:system:audit:stream

The internal archival consumer group ("__archival__") is created
automatically. External SIEM consumer groups are managed via the
consumers API.

Compliance: DORA Art. 10 (real-time detection), SOC 2 CC7.1 (monitoring).
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from supertable import redis_keys as RK
from supertable.audit.diagnostics import safe_audit_error_type

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Key helpers — delegate to supertable.redis_keys for namespace consistency
# ---------------------------------------------------------------------------

ARCHIVAL_GROUP = "__archival__"
_MAX_HOT_QUERY_PAGE = 1_000
_MAX_AUDIT_EVENT_FIELD_BYTES = 64 * 1024
_MAX_AUDIT_EVENT_BYTES = 96 * 1024
_MAX_CONSUMER_GROUPS_RETURNED = 101
_MAX_CONSUMER_METADATA_BYTES = 256 * 1024
_MAX_EXTERNAL_CONSUMERS = 100
_CONSUMER_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")
_STREAM_ID_RE = re.compile(r"(?:0|[1-9][0-9]{0,19})-(?:0|[1-9][0-9]{0,19})")
_MAX_EXCEPTION_ARG_BYTES = 4_096


def _exception_has_code(error: BaseException, code: str) -> bool:
    """Inspect bounded Redis error arguments without rendering an exception."""
    try:
        arguments = error.args
    except Exception:
        return False
    if not isinstance(arguments, tuple):
        return False
    for value in arguments[:4]:
        if isinstance(value, bytes):
            if len(value) > _MAX_EXCEPTION_ARG_BYTES:
                continue
            text = value.decode("ascii", errors="ignore")
        elif isinstance(value, str):
            if (
                len(value.encode("utf-8", errors="ignore"))
                > _MAX_EXCEPTION_ARG_BYTES
            ):
                continue
            text = value
        else:
            continue
        if code in text:
            return True
    return False

_CREATE_CONSUMER_GROUP_LUA = r"""
local external_count = 0
if redis.call('EXISTS', KEYS[1]) == 1 then
    local groups = redis.call('XINFO', 'GROUPS', KEYS[1])
    for _, group in ipairs(groups) do
        local name = nil
        for index = 1, #group, 2 do
            if group[index] == 'name' then
                name = group[index + 1]
                break
            end
        end
        if name == ARGV[1] then
            return 2
        end
        if name ~= ARGV[4] then
            external_count = external_count + 1
        end
    end
end
if external_count >= tonumber(ARGV[3]) then
    return -1
end
redis.call('XGROUP', 'CREATE', KEYS[1], ARGV[1], ARGV[2], 'MKSTREAM')
return 1
"""


class AuditRedisCheckpointError(RuntimeError):
    """The secondary Redis chain checkpoint was not persisted."""


class AuditRedisQueryError(RuntimeError):
    """The Redis hot tier could not return a complete query result."""


def _validate_consumer_group_name(
    group_name: str,
    *,
    allow_internal: bool = False,
) -> str:
    if allow_internal and group_name == ARCHIVAL_GROUP:
        return group_name
    if (
        not isinstance(group_name, str)
        or not _CONSUMER_ID_RE.fullmatch(group_name)
        or group_name == ARCHIVAL_GROUP
    ):
        raise ValueError("audit consumer group name is invalid")
    return group_name


def _is_bounded_stream_id(value: str) -> bool:
    if not isinstance(value, str) or not _STREAM_ID_RE.fullmatch(value):
        return False
    milliseconds, sequence = value.split("-", 1)
    return (
        int(milliseconds) <= (1 << 64) - 1
        and int(sequence) <= (1 << 64) - 1
    )


def _validate_consumer_start(start_from: str) -> str:
    if not isinstance(start_from, str) or (
        start_from != "$" and not _is_bounded_stream_id(start_from)
    ):
        raise ValueError("audit consumer start position is invalid")
    return start_from


def _bounded_text(value: Any, *, maximum_bytes: int) -> str:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str):
        raise ValueError("audit consumer metadata text is invalid")
    if len(value.encode("utf-8")) > maximum_bytes:
        raise ValueError("audit consumer metadata text is oversized")
    return value


# ---------------------------------------------------------------------------
# RedisAuditWriter
# ---------------------------------------------------------------------------

class RedisAuditWriter:
    """Write audit events to a Redis Stream and manage chain state.

    Thread-safety: this class is NOT thread-safe. The AuditLogger
    serializes access through its background worker thread.
    """

    def __init__(self, redis_client, org: str, instance_id: str, maxlen: int = 100_000):
        self._r = redis_client
        self._org = org
        self._instance_id = instance_id
        self._maxlen = maxlen
        self._stream = RK.audit_stream(org)
        # The chain-head key is lazy: read-only callers (e.g. the
        # audit reader) instantiate this class purely to use
        # ``query()`` and pass instance_id="". Building the
        # chain-head key with an empty instance_id would fail v2's
        # segment validator, so defer construction until a method
        # that actually needs it is called.
        self._ensure_stream()

    @property
    def _chain_key(self) -> str:
        if not self._instance_id:
            raise RuntimeError(
                "RedisAuditWriter._chain_key requires a non-empty "
                "instance_id; this writer was instantiated for "
                "read-only use (query) and cannot save/load chain head."
            )
        return RK.audit_chain_head(self._org, self._instance_id)

    def _ensure_stream(self) -> None:
        """Create the stream and archival consumer group if they don't exist."""
        try:
            self._r.xgroup_create(
                self._stream, ARCHIVAL_GROUP, id="0", mkstream=True,
            )
            logger.info("[audit-redis] Created archival consumer group")
        except Exception as exc:
            if _exception_has_code(exc, "BUSYGROUP"):
                pass  # Group already exists — expected on restart
            else:
                logger.warning(
                    "[audit-redis] xgroup_create failed (non-fatal); "
                    "error_type=%s",
                    safe_audit_error_type(exc),
                )

    # ── Write ──────────────────────────────────────────────

    def write_batch(self, events: List[Dict[str, Any]]) -> List[str]:
        """Write a batch of events to the Redis Stream.

        Each event dict is flattened into a Redis Stream entry.
        Returns the list of Redis Stream IDs assigned.

        Uses XADD with MAXLEN~ (approximate trimming) to keep the
        stream bounded without blocking on exact trimming.
        """
        if not events:
            return []

        ids: List[str] = []
        try:
            pipe = self._r.pipeline()
            for event in events:
                # Redis Streams require flat string values
                flat = {k: str(v) if not isinstance(v, str) else v for k, v in event.items()}
                pipe.xadd(self._stream, flat, maxlen=self._maxlen, approximate=True)
            ids = pipe.execute()
            ids = [sid.decode("utf-8") if isinstance(sid, bytes) else str(sid) for sid in ids]
        except Exception as exc:
            logger.error(
                "[audit-redis] write_batch failed (%d events); error_type=%s",
                len(events), safe_audit_error_type(exc),
            )

        return ids

    # ── Chain state persistence ────────────────────────────

    def save_chain_head(self, head: str, batch_count: int) -> None:
        """Persist the current chain head for this instance."""
        try:
            self._r.hset(self._chain_key, mapping={
                "head": head,
                "batch_count": str(batch_count),
                "updated_ms": str(int(time.time() * 1000)),
            })
        except Exception as exc:
            logger.error(
                "[audit-redis] save_chain_head failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            raise AuditRedisCheckpointError(
                "audit Redis chain checkpoint failed"
            ) from None

    def load_chain_head(self) -> Tuple[str, int]:
        """Load the persisted chain head. Returns (head_hash, batch_count)."""
        try:
            data = self._r.hgetall(self._chain_key)
            if data:
                raw_head = data.get("head", "")
                if isinstance(raw_head, bytes):
                    raw_head = raw_head.decode("utf-8")
                if not isinstance(raw_head, str) or raw_head and (
                    len(raw_head) != 64
                    or any(ch not in "0123456789abcdef" for ch in raw_head)
                ):
                    raise ValueError("audit Redis chain head is invalid")
                count_raw = data.get("batch_count", "0")
                if isinstance(count_raw, bytes):
                    count_raw = count_raw.decode("utf-8")
                if (
                    not isinstance(count_raw, str)
                    or not count_raw.isascii()
                    or not count_raw.isdigit()
                    or len(count_raw) > 19
                    or int(count_raw) > (1 << 63) - 1
                ):
                    raise ValueError("audit Redis chain count is invalid")
                return raw_head, int(count_raw)
        except Exception as exc:
            logger.warning(
                "[audit-redis] load_chain_head failed; error_type=%s",
                safe_audit_error_type(exc),
            )
        return "", 0

    # ── Query (hot tier) ───────────────────────────────────

    def query(
        self,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        count: int = 500,
        min_stream_id: Optional[str] = None,
        max_stream_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Query recent events from the Redis Stream by time range.

        Uses XRANGE with millisecond-based IDs.
        """
        if type(count) is not int or not 1 <= count <= _MAX_HOT_QUERY_PAGE:
            raise ValueError(
                f"audit Redis query count must be between 1 and {_MAX_HOT_QUERY_PAGE}"
            )
        for label, timestamp_bound in (("start", start_ms), ("end", end_ms)):
            if timestamp_bound is not None and (
                type(timestamp_bound) is not int
                or not 0 <= timestamp_bound <= (1 << 63) - 1
            ):
                raise ValueError(f"audit Redis {label} timestamp is invalid")
        if start_ms is not None and end_ms is not None and start_ms > end_ms:
            raise ValueError("audit Redis start timestamp exceeds end timestamp")
        if start_ms is not None and min_stream_id is not None:
            raise ValueError("audit Redis minimum bounds are ambiguous")
        if end_ms is not None and max_stream_id is not None:
            raise ValueError("audit Redis maximum bounds are ambiguous")
        min_id = min_stream_id if min_stream_id is not None else (
            f"{start_ms}-0" if start_ms is not None else "-"
        )
        max_id = max_stream_id if max_stream_id is not None else (
            f"{end_ms}-18446744073709551615" if end_ms is not None else "+"
        )
        for label, stream_bound in (("minimum", min_id), ("maximum", max_id)):
            if not isinstance(stream_bound, str):
                raise ValueError(f"audit Redis {label} stream ID is invalid")
            candidate = (
                stream_bound[1:]
                if stream_bound.startswith("(")
                else stream_bound
            )
            if candidate not in {"-", "+"} and not _is_bounded_stream_id(candidate):
                raise ValueError(f"audit Redis {label} stream ID is invalid")

        try:
            entries = self._r.xrevrange(self._stream, max=max_id, min=min_id, count=count)
            if not isinstance(entries, (list, tuple)) or len(entries) > count:
                raise AuditRedisQueryError(
                    "audit Redis query returned an invalid result envelope"
                )
            from supertable.audit.events import AuditEvent

            expected_fields = set(AuditEvent.__dataclass_fields__)
            results = []
            for stream_id, fields in entries:
                sid = (
                    stream_id
                    if isinstance(stream_id, str)
                    else stream_id.decode("utf-8")
                )
                if not _is_bounded_stream_id(sid) or not isinstance(fields, dict):
                    raise AuditRedisQueryError(
                        "audit Redis query returned an invalid stream entry"
                    )
                row: Dict[str, Any] = {}
                retained_bytes = 0
                for raw_key, raw_value in fields.items():
                    key = (
                        raw_key
                        if isinstance(raw_key, str)
                        else raw_key.decode("utf-8")
                    )
                    field_value = (
                        raw_value
                        if isinstance(raw_value, str)
                        else raw_value.decode("utf-8")
                    )
                    if key in row:
                        raise AuditRedisQueryError(
                            "audit Redis query returned duplicate fields"
                        )
                    value_bytes = len(field_value.encode("utf-8"))
                    if value_bytes > _MAX_AUDIT_EVENT_FIELD_BYTES:
                        raise AuditRedisQueryError(
                            "audit Redis event field exceeds its byte limit"
                        )
                    retained_bytes += len(key.encode("utf-8")) + value_bytes
                    if retained_bytes > _MAX_AUDIT_EVENT_BYTES:
                        raise AuditRedisQueryError(
                            "audit Redis event exceeds its byte limit"
                        )
                    row[key] = field_value
                if set(row) != expected_fields:
                    raise AuditRedisQueryError(
                        "audit Redis event schema is incomplete or unexpected"
                    )
                timestamp = row["timestamp_ms"]
                if (
                    not timestamp.isascii()
                    or not timestamp.isdigit()
                    or len(timestamp) > 19
                    or int(timestamp) > (1 << 63) - 1
                ):
                    raise AuditRedisQueryError(
                        "audit Redis event timestamp is invalid"
                    )
                if row["organization"] != self._org or not row["event_id"]:
                    raise AuditRedisQueryError(
                        "audit Redis event tenant or identity is inconsistent"
                    )
                RK.audit_chain_head(self._org, row["instance_id"])
                chain_hash = row["chain_hash"]
                if chain_hash and (
                    len(chain_hash) != 64
                    or any(ch not in "0123456789abcdef" for ch in chain_hash)
                ):
                    raise AuditRedisQueryError(
                        "audit Redis event chain hash is invalid"
                    )
                row["_stream_id"] = sid
                results.append(row)
            return results
        except Exception as exc:
            logger.error(
                "[audit-redis] query failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            raise AuditRedisQueryError("audit Redis query failed") from None

    # ── Stream management ──────────────────────────────────

    def stream_length(self) -> int:
        """Current number of entries in the stream."""
        try:
            return int(self._r.xlen(self._stream) or 0)
        except Exception:
            return 0

    def trim_acknowledged(self, ttl_hours: int = 24) -> int:
        """Trim events older than ttl_hours ONLY if all consumer groups
        have acknowledged them.

        Returns the number of entries trimmed.
        """
        cutoff_ms = int((time.time() - ttl_hours * 3600) * 1000)
        min_id = f"{cutoff_ms}-0"

        try:
            # Check all consumer groups for unacknowledged entries
            groups = self._r.xinfo_groups(self._stream) or []
            for group in groups:
                pending = group.get("pending", 0)
                if isinstance(pending, bytes):
                    pending = int(pending)
                if pending > 0:
                    # This group has unacknowledged entries — check if they're old
                    lag = group.get("lag")
                    if lag and int(lag) > 0:
                        logger.debug(
                            "[audit-redis] consumer group has pending events; "
                            "pending=%d; skipping trim",
                            pending,
                        )
                        return 0

            # All groups are caught up — safe to trim by time
            result = self._r.xtrim(self._stream, minid=min_id)
            return int(result or 0)
        except Exception as exc:
            logger.warning(
                "[audit-redis] trim_acknowledged failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            return 0

    # ── Consumer group management ──────────────────────────

    def create_consumer_group(
        self,
        group_name: str,
        start_from: str = "$",
        *,
        max_consumers: int = 10,
    ) -> bool:
        """Atomically create a bounded external SIEM consumer group."""
        _validate_consumer_group_name(group_name)
        _validate_consumer_start(start_from)
        if (
            type(max_consumers) is not int
            or not 1 <= max_consumers <= _MAX_EXTERNAL_CONSUMERS
        ):
            raise ValueError("audit SIEM consumer limit is invalid")
        try:
            result = self._r.eval(
                _CREATE_CONSUMER_GROUP_LUA,
                1,
                self._stream,
                group_name,
                start_from,
                max_consumers,
                ARCHIVAL_GROUP,
            )
            if type(result) is not int or result not in {-1, 1, 2}:
                raise ValueError("audit consumer creation result is invalid")
            if result == -1:
                logger.warning("[audit-redis] SIEM consumer group limit reached")
                return False
            logger.info("[audit-redis] SIEM consumer group is available")
            return True
        except Exception as exc:
            logger.error(
                "[audit-redis] create_consumer_group failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            return False

    def delete_consumer_group(self, group_name: str) -> bool:
        """Remove an external SIEM consumer group."""
        _validate_consumer_group_name(group_name)
        try:
            self._r.xgroup_destroy(self._stream, group_name)
            logger.info("[audit-redis] Deleted SIEM consumer group")
            return True
        except Exception as exc:
            logger.error(
                "[audit-redis] delete_consumer_group failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            return False

    def list_consumer_groups(self) -> List[Dict[str, Any]]:
        """List all consumer groups with lag info."""
        try:
            groups = self._r.xinfo_groups(self._stream) or []
            if (
                not isinstance(groups, (list, tuple))
                or len(groups) > _MAX_CONSUMER_GROUPS_RETURNED
            ):
                raise ValueError("audit consumer metadata envelope is invalid")
            result = []
            retained_bytes = 0
            for g in groups:
                if not isinstance(g, dict) or len(g) > 16:
                    raise ValueError("audit consumer metadata entry is invalid")
                normalized: Dict[str, Any] = {}
                for raw_key, value in g.items():
                    key = _bounded_text(raw_key, maximum_bytes=64)
                    if key in normalized:
                        raise ValueError(
                            "audit consumer metadata contains duplicate fields"
                        )
                    normalized[key] = value
                    retained_bytes += len(key.encode("utf-8")) + 64
                    if retained_bytes > _MAX_CONSUMER_METADATA_BYTES:
                        raise ValueError("audit consumer metadata is oversized")
                name = _bounded_text(normalized.get("name"), maximum_bytes=128)
                _validate_consumer_group_name(name, allow_internal=True)
                consumers = normalized.get("consumers", 0)
                pending = normalized.get("pending", 0)
                lag = normalized.get("lag", 0)
                for value in (consumers, pending):
                    if type(value) is not int or not 0 <= value <= (1 << 63) - 1:
                        raise ValueError("audit consumer counter is invalid")
                if lag is not None and (
                    type(lag) is not int or not 0 <= lag <= (1 << 63) - 1
                ):
                    raise ValueError("audit consumer lag is invalid")
                last_delivered_id = _bounded_text(
                    normalized.get("last-delivered-id"),
                    maximum_bytes=64,
                )
                if not _is_bounded_stream_id(last_delivered_id):
                    raise ValueError("audit consumer stream position is invalid")
                retained_bytes += len(name.encode("utf-8"))
                retained_bytes += len(last_delivered_id.encode("utf-8"))
                if retained_bytes > _MAX_CONSUMER_METADATA_BYTES:
                    raise ValueError("audit consumer metadata is oversized")
                result.append({
                    "name": name,
                    "consumers": consumers,
                    "pending": pending,
                    "last_delivered_id": last_delivered_id,
                    "lag": lag,
                    "is_internal": name == ARCHIVAL_GROUP,
                })
            return result
        except Exception as exc:
            logger.error(
                "[audit-redis] list_consumer_groups failed; error_type=%s",
                safe_audit_error_type(exc),
            )
            return []
