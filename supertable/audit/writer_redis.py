# route: supertable.audit.writer_redis
"""
Hot-tier audit writer using Redis Streams.

Provides real-time queryability (XRANGE), consumer groups for external
SIEM tools (Splunk, Sentinel, ELK), and TTL-based eviction.

Each organization gets its own stream:
    supertable:{org}:audit:stream

The internal archival consumer group ("__archival__") is created
automatically. External SIEM consumer groups are managed via the
consumers API.

Compliance: DORA Art. 10 (real-time detection), SOC 2 CC7.1 (monitoring).
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------

def _stream_key(org: str) -> str:
    return f"supertable:{org}:audit:stream"


def _chain_head_key(org: str, instance_id: str) -> str:
    return f"supertable:{org}:audit:chain_head:{instance_id}"


ARCHIVAL_GROUP = "__archival__"


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
        self._stream = _stream_key(org)
        self._chain_key = _chain_head_key(org, instance_id)
        self._ensure_stream()

    def _ensure_stream(self) -> None:
        """Create the stream and archival consumer group if they don't exist."""
        try:
            self._r.xgroup_create(
                self._stream, ARCHIVAL_GROUP, id="0", mkstream=True,
            )
            logger.info("[audit-redis] Created stream %s with group %s", self._stream, ARCHIVAL_GROUP)
        except Exception as e:
            err_msg = str(e).lower()
            if "busygroup" in err_msg:
                pass  # Group already exists — expected on restart
            else:
                logger.warning("[audit-redis] xgroup_create: %s (non-fatal)", e)

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
        except Exception as e:
            logger.error("[audit-redis] write_batch failed (%d events): %s", len(events), e)

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
        except Exception as e:
            logger.error("[audit-redis] save_chain_head failed: %s", e)

    def load_chain_head(self) -> Tuple[str, int]:
        """Load the persisted chain head. Returns (head_hash, batch_count)."""
        try:
            data = self._r.hgetall(self._chain_key)
            if data:
                head = data.get("head", "")
                if isinstance(head, bytes):
                    head = head.decode("utf-8")
                count_raw = data.get("batch_count", "0")
                if isinstance(count_raw, bytes):
                    count_raw = count_raw.decode("utf-8")
                return head, int(count_raw)
        except Exception as e:
            logger.warning("[audit-redis] load_chain_head failed: %s", e)
        return "", 0

    # ── Query (hot tier) ───────────────────────────────────

    def query(
        self,
        *,
        start_ms: Optional[int] = None,
        end_ms: Optional[int] = None,
        count: int = 500,
    ) -> List[Dict[str, Any]]:
        """Query recent events from the Redis Stream by time range.

        Uses XRANGE with millisecond-based IDs.
        """
        min_id = f"{start_ms}-0" if start_ms else "-"
        max_id = f"{end_ms}-18446744073709551615" if end_ms else "+"

        try:
            entries = self._r.xrevrange(self._stream, max=max_id, min=min_id, count=count)
            results = []
            for stream_id, fields in entries:
                sid = stream_id if isinstance(stream_id, str) else stream_id.decode("utf-8")
                row = {
                    k if isinstance(k, str) else k.decode("utf-8"):
                    v if isinstance(v, str) else v.decode("utf-8")
                    for k, v in fields.items()
                }
                row["_stream_id"] = sid
                results.append(row)
            return results
        except Exception as e:
            logger.error("[audit-redis] query failed: %s", e)
            return []

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
                            "[audit-redis] Consumer group %s has %d pending — skipping trim",
                            group.get("name", "?"), pending,
                        )
                        return 0

            # All groups are caught up — safe to trim by time
            result = self._r.xtrim(self._stream, minid=min_id)
            return int(result or 0)
        except Exception as e:
            logger.warning("[audit-redis] trim_acknowledged failed: %s", e)
            return 0

    # ── Consumer group management ──────────────────────────

    def create_consumer_group(self, group_name: str, start_from: str = "$") -> bool:
        """Create an external SIEM consumer group."""
        try:
            self._r.xgroup_create(self._stream, group_name, id=start_from, mkstream=True)
            logger.info("[audit-redis] Created consumer group: %s (start=%s)", group_name, start_from)
            return True
        except Exception as e:
            err_msg = str(e).lower()
            if "busygroup" in err_msg:
                logger.info("[audit-redis] Consumer group %s already exists", group_name)
                return True
            logger.error("[audit-redis] create_consumer_group failed: %s", e)
            return False

    def delete_consumer_group(self, group_name: str) -> bool:
        """Remove an external SIEM consumer group."""
        if group_name == ARCHIVAL_GROUP:
            logger.warning("[audit-redis] Cannot delete internal archival group")
            return False
        try:
            self._r.xgroup_destroy(self._stream, group_name)
            logger.info("[audit-redis] Deleted consumer group: %s", group_name)
            return True
        except Exception as e:
            logger.error("[audit-redis] delete_consumer_group failed: %s", e)
            return False

    def list_consumer_groups(self) -> List[Dict[str, Any]]:
        """List all consumer groups with lag info."""
        try:
            groups = self._r.xinfo_groups(self._stream) or []
            result = []
            for g in groups:
                name = g.get("name", "")
                if isinstance(name, bytes):
                    name = name.decode("utf-8")
                result.append({
                    "name": name,
                    "consumers": int(g.get("consumers", 0)),
                    "pending": int(g.get("pending", 0)),
                    "last_delivered_id": str(g.get("last-delivered-id", "")),
                    "lag": int(g.get("lag", 0)) if g.get("lag") is not None else None,
                    "is_internal": name == ARCHIVAL_GROUP,
                })
            return result
        except Exception as e:
            logger.error("[audit-redis] list_consumer_groups failed: %s", e)
            return []
