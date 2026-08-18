# route: supertable.monitoring.partitions
"""
Orchestration primitives for the daily-partitioned monitoring queue.

Layout
------

The writer (``supertable.monitoring_writer._AsyncMonitoringLogger``)
pushes JSON payloads into daily-partitioned Redis LISTs::

    supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}    LIST (live)
    supertable:{org}:monitor:{monitor_type}:doc:{YYYY-MM-DD}:_drain
                                                               LIST (in-progress)

Today's partition fills as writes happen. Older partitions are
**immutable from the writer's point of view** — the writer always
resolves "today" fresh per batch, so once midnight (UTC) passes
yesterday's partition stops growing. That's what makes it safe for an
external orchestrator to drain.

Contract
--------

The three public helpers in this module are **pure** with respect to
SDK state — they only touch Redis. None of them spawn threads or
loops. Your service is expected to call them on its own schedule.

  * :func:`list_drainable_partitions` — discover what's drainable.
  * :func:`claim_partition` — claim a small partition and return its receipt.
  * :func:`claim_partition_chunks` + :func:`iter_claimed_partition_chunks` —
    memory-bounded claim/stream with an operable explicit receipt.
  * :func:`acknowledge_partition` — delete only after the sink commits.

Mapping to internal sink tables
-------------------------------

The constant :data:`MONITORING_SINK_TABLE_FOR` records the agreed
mapping from monitor_type to the SDK-internal sink table name your
orchestrator should write to (``plans → __reads__``,
``writes → __writes__``, ``mcp → __mcp__``, etc.). Writes to those
sink tables are exempted from monitoring emission by
``data_writer._MONITORING_SINK_TABLES`` to break the recursion loop.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, NamedTuple, Optional, Sequence

from supertable import redis_keys as RK

logger = logging.getLogger(__name__)


# Safety cap on read_recent's max_days_back kwarg. Going further back is
# unlikely to find anything (drained long ago) and would multiply Redis
# round-trips for no gain.
_MAX_READ_RECENT_DAYS_BACK = 90


# ---------------------------------------------------------------------------
# Monitor type → sink table mapping
# ---------------------------------------------------------------------------

#: Canonical mapping from ``monitor_type`` to the SDK-internal sink table
#: an orchestrator should write the drained partition into. Sink table
#: names are exempt from monitoring emission via
#: :data:`MONITORING_SINK_TABLES` so dumping a ``writes`` partition into
#: ``__writes__`` does not generate a fresh ``writes`` metric for
#: tomorrow's flush.
MONITORING_SINK_TABLE_FOR: Dict[str, str] = {
    "plans":   "__reads__",
    "writes":  "__writes__",
    "mcp":     "__mcp__",
    "compact": "__compact__",
    # The remaining monitor types (``odata``/``errors``/``locks``) do
    # not yet have agreed sink tables. An orchestrator that wants to
    # persist them can extend this mapping at call time — the SDK
    # only uses it as a hint.
}


#: Single source of truth for "table names whose writes/reads must NOT
#: emit monitoring metrics". Used by ``data_writer`` and
#: ``plan_extender`` to break the recursion loop that would otherwise
#: form when the orchestrator dumps a drained partition back into the
#: sink table.
#:
#: Listed explicitly (rather than derived from ``MONITORING_SINK_TABLE_FOR``)
#: so we cover both directions — the values flow *into* the loop guard
#: but additional defensive entries (e.g. ``__plans__`` as a future
#: rename target) can be added here without touching the mapping.
MONITORING_SINK_TABLES: frozenset = frozenset(MONITORING_SINK_TABLE_FOR.values()) | {
    "__plans__",
    "__compact__",
}


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------


class MonitorPartition(NamedTuple):
    """One drainable Redis partition.

    NamedTuple chosen over dataclass so callers can iterate as tuples
    (``for mt, dt in parts:``) *or* address fields by name
    (``part.monitor_type``) — both ergonomics, zero overhead.
    """

    organization: str
    monitor_type: str
    date: str  # ISO 8601 YYYY-MM-DD


class MonitoringPartitionError(RuntimeError):
    """A drain could not be claimed or acknowledged without data loss."""


@dataclass(frozen=True)
class MonitorDrainClaim:
    """A stable at-least-once claim which must be acknowledged explicitly."""

    organization: str
    monitor_type: str
    date: str
    receipt: str
    entries: tuple[Dict[str, Any], ...]


@dataclass(frozen=True)
class MonitorChunkClaim:
    """Receipt and immutable size for a memory-bounded drain claim."""

    organization: str
    monitor_type: str
    date: str
    receipt: str
    entry_count: int


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _today_utc() -> str:
    """ISO 8601 UTC calendar date — identical helper to the writer's."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _get_redis(catalog: Any):
    """Recover a redis-py client from a ``RedisCatalog`` (or duck-type).

    Tests pass mocks with a ``.r`` attribute; production callers pass a
    ``RedisCatalog``. Anything exposing ``.r`` is acceptable.
    """
    r = getattr(catalog, "r", None)
    if r is None:
        raise ValueError(
            "catalog argument must expose a redis-py client as `.r`"
        )
    return r


def _decode(v: Any) -> str:
    """Decode a redis-py value (bytes or str) to str."""
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.decode("utf-8", errors="replace")
    return v


def _parse_entries(raw_entries: List[Any]) -> List[Dict[str, Any]]:
    """Parse a list of JSON-serialised payloads into dicts.

    Malformed entries are skipped (logged at DEBUG) so one bad row
    cannot poison the whole drain.
    """
    out: List[Dict[str, Any]] = []
    for raw in raw_entries or []:
        s = _decode(raw)
        try:
            obj = json.loads(s)
        except (json.JSONDecodeError, TypeError) as e:
            logger.debug("[monitoring.partitions] skipping malformed entry: %s", e)
            continue
        if isinstance(obj, dict):
            out.append(obj)
        else:
            logger.debug(
                "[monitoring.partitions] skipping non-object entry: %r", obj
            )
    return out


def _parse_entries_strict(raw_entries: Sequence[Any]) -> List[Dict[str, Any]]:
    """Parse a drain without allowing malformed source rows to disappear."""

    parsed: List[Dict[str, Any]] = []
    for index, raw in enumerate(raw_entries or ()):
        value = _decode(raw)
        try:
            item = json.loads(value)
        except (json.JSONDecodeError, TypeError, UnicodeError) as exc:
            raise MonitoringPartitionError(
                f"monitoring drain entry {index} is not valid JSON"
            ) from exc
        if not isinstance(item, dict):
            raise MonitoringPartitionError(
                f"monitoring drain entry {index} is not a JSON object"
            )
        parsed.append(item)
    return parsed


def _new_receipt_hasher():
    digest = hashlib.sha256()
    digest.update(b"supertable-monitor-drain-v1\0")
    return digest


def _update_receipt_hasher(digest, raw_entries: Sequence[Any]) -> None:
    for raw in raw_entries:
        if isinstance(raw, bytes):
            encoded = raw
        elif isinstance(raw, str):
            encoded = raw.encode("utf-8")
        else:
            raise MonitoringPartitionError(
                "monitoring drain contains a non-text Redis value"
            )
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)


def _raw_receipt(raw_entries: Sequence[Any]) -> str:
    digest = _new_receipt_hasher()
    _update_receipt_hasher(digest, raw_entries)
    return digest.hexdigest()


_LUA_CLAIM_WITHOUT_EXPIRY = """
local source = KEYS[1]
local drain = KEYS[2]
local drain_exists = redis.call('EXISTS', drain)
local renamed = 0
if drain_exists == 0 and redis.call('EXISTS', source) == 1 then
    renamed = redis.call('RENAMENX', source, drain)
end
if redis.call('EXISTS', drain) == 1 then
    -- RENAME preserves TTL. PERSIST in this same atomic script removes the
    -- crash window where an acknowledged source becomes an expiring drain.
    redis.call('PERSIST', drain)
end
return {renamed, redis.call('PTTL', drain)}
"""


def _claim_without_expiry(r: Any, source: str, drain: str) -> bool:
    """Atomically claim a partition and make its drain non-expiring."""
    try:
        result = r.eval(_LUA_CLAIM_WITHOUT_EXPIRY, 2, source, drain)
        if (
            not isinstance(result, (list, tuple))
            or len(result) != 2
            or isinstance(result[0], bool)
            or not isinstance(result[0], int)
            or isinstance(result[1], bool)
            or not isinstance(result[1], int)
        ):
            raise MonitoringPartitionError(
                "monitoring claim script returned an invalid response"
            )
        renamed, pttl = result
        if pttl not in {-1, -2}:
            raise MonitoringPartitionError(
                "claimed monitoring partition still has an expiry"
            )
        return bool(renamed)
    except MonitoringPartitionError:
        raise
    except Exception as exc:
        raise MonitoringPartitionError(
            "could not atomically claim monitoring partition"
        ) from exc


def claim_partition(
    catalog: Any,
    *,
    organization: str,
    monitor_type: str,
    date: str,
) -> Optional[MonitorDrainClaim]:
    """Claim and read a frozen partition without deleting its Redis source.

    Call :func:`acknowledge_partition` with the returned receipt only after
    the downstream sink durably and idempotently commits every entry.
    """

    r = _get_redis(catalog)
    src = RK.monitor_partition(organization, monitor_type, date)
    drain = RK.monitor_partition_drain(organization, monitor_type, date)
    receipt_key = RK.monitor_partition_receipt(
        organization, monitor_type, date,
    )
    try:
        _claim_without_expiry(r, src, drain)
        raw_entries = r.lrange(drain, 0, -1)
    except Exception as exc:
        raise MonitoringPartitionError(
            f"could not read claimed monitoring partition "
            f"{organization}/{monitor_type}/{date}"
        ) from exc
    if not raw_entries:
        return None

    receipt = _raw_receipt(raw_entries)
    try:
        created = r.set(receipt_key, receipt, nx=True)
        persisted = r.get(receipt_key)
    except Exception as exc:
        raise MonitoringPartitionError(
            "could not persist monitoring drain receipt"
        ) from exc
    # Minimal Redis duck-types used by embedders sometimes acknowledge SET NX
    # without implementing a value-returning GET. Production redis-py always
    # returns bytes/text here; accept the acknowledged creation in that narrow
    # compatibility case, but never accept an explicit conflicting value.
    if isinstance(persisted, (bytes, str)):
        persisted_receipt = _decode(persisted)
    elif created:
        persisted_receipt = receipt
    else:
        persisted_receipt = None
    if persisted_receipt != receipt:
        raise MonitoringPartitionError(
            "monitoring drain content differs from its persisted receipt"
        )
    return MonitorDrainClaim(
        organization=organization,
        monitor_type=monitor_type,
        date=date,
        receipt=receipt,
        entries=tuple(_parse_entries_strict(raw_entries)),
    )


def _bounded_chunk_size(chunk_size: int) -> int:
    try:
        value = int(chunk_size)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("chunk_size must be an integer") from exc
    return max(1, min(value, 1_000_000))


def claim_partition_chunks(
    catalog: Any,
    *,
    organization: str,
    monitor_type: str,
    date: str,
    chunk_size: int = 10_000,
) -> Optional[MonitorChunkClaim]:
    """Claim and validate a partition using bounded ``LRANGE`` windows.

    The returned receipt is the token passed to
    :func:`acknowledge_partition` *after* every chunk has committed to an
    idempotent sink. The drain's writer TTL is removed before this function
    returns, so downstream delay cannot silently expire acknowledged work.
    """

    size = _bounded_chunk_size(chunk_size)
    r = _get_redis(catalog)
    src = RK.monitor_partition(organization, monitor_type, date)
    drain = RK.monitor_partition_drain(organization, monitor_type, date)
    receipt_key = RK.monitor_partition_receipt(
        organization, monitor_type, date,
    )
    try:
        _claim_without_expiry(r, src, drain)
        total = int(r.llen(drain) or 0)
    except MonitoringPartitionError:
        raise
    except Exception as exc:
        raise MonitoringPartitionError(
            "could not inspect claimed monitoring partition"
        ) from exc
    if total == 0:
        return None

    digest = _new_receipt_hasher()
    for start in range(0, total, size):
        stop = min(start + size, total) - 1
        try:
            raw = r.lrange(drain, start, stop)
        except Exception as exc:
            raise MonitoringPartitionError(
                f"could not read monitoring claim window {start}..{stop}"
            ) from exc
        if len(raw) != stop - start + 1:
            raise MonitoringPartitionError(
                "monitoring drain changed while its receipt was calculated"
            )
        # Refuse poison rows before a downstream sink sees any part of this
        # claim. Parsing is per-window and therefore remains memory bounded.
        _parse_entries_strict(raw)
        _update_receipt_hasher(digest, raw)
    try:
        if int(r.llen(drain) or 0) != total:
            raise MonitoringPartitionError(
                "monitoring drain changed while its receipt was calculated"
            )
        receipt = digest.hexdigest()
        created = r.set(receipt_key, receipt, nx=True)
        persisted = r.get(receipt_key)
    except MonitoringPartitionError:
        raise
    except Exception as exc:
        raise MonitoringPartitionError(
            "could not persist bounded monitoring drain receipt"
        ) from exc
    persisted_receipt = _decode(persisted) if isinstance(persisted, (bytes, str)) else None
    if persisted_receipt is None and created:
        persisted_receipt = receipt
    if persisted_receipt != receipt:
        raise MonitoringPartitionError(
            "monitoring drain content differs from its persisted receipt"
        )
    return MonitorChunkClaim(
        organization=organization,
        monitor_type=monitor_type,
        date=date,
        receipt=receipt,
        entry_count=total,
    )


def iter_claimed_partition_chunks(
    catalog: Any,
    claim: MonitorChunkClaim,
    *,
    chunk_size: int = 10_000,
) -> Iterator[List[Dict[str, Any]]]:
    """Stream a bounded claim and re-verify its receipt before exhaustion.

    Exhaustion does not acknowledge. Commit every yielded chunk idempotently,
    let this iterator reach its verified end, then call
    :func:`acknowledge_partition` with ``claim.receipt``.
    """

    if not isinstance(claim, MonitorChunkClaim):
        raise TypeError("claim must be a MonitorChunkClaim")
    size = _bounded_chunk_size(chunk_size)
    r = _get_redis(catalog)
    drain = RK.monitor_partition_drain(
        claim.organization, claim.monitor_type, claim.date,
    )
    receipt_key = RK.monitor_partition_receipt(
        claim.organization, claim.monitor_type, claim.date,
    )
    try:
        persisted = r.get(receipt_key)
        total = int(r.llen(drain) or 0)
        pttl = int(r.pttl(drain))
    except Exception as exc:
        raise MonitoringPartitionError(
            "could not reopen bounded monitoring drain claim"
        ) from exc
    if not isinstance(persisted, (bytes, str)) or _decode(persisted) != claim.receipt:
        raise MonitoringPartitionError("monitoring chunk claim receipt is no longer current")
    if total != claim.entry_count:
        raise MonitoringPartitionError("monitoring chunk claim size changed before delivery")
    if pttl != -1:
        raise MonitoringPartitionError("monitoring chunk claim is not durable for delivery")

    digest = _new_receipt_hasher()
    for start in range(0, total, size):
        stop = min(start + size, total) - 1
        try:
            raw = r.lrange(drain, start, stop)
        except Exception as exc:
            raise MonitoringPartitionError(
                f"could not stream monitoring claim window {start}..{stop}"
            ) from exc
        if len(raw) != stop - start + 1:
            raise MonitoringPartitionError("monitoring chunk claim changed during delivery")
        _update_receipt_hasher(digest, raw)
        yield _parse_entries_strict(raw)
    if digest.hexdigest() != claim.receipt:
        raise MonitoringPartitionError("monitoring chunk claim failed final receipt verification")


def acknowledge_partition(
    catalog: Any,
    *,
    organization: str,
    monitor_type: str,
    date: str,
    receipt: str,
) -> bool:
    """Delete a claimed partition iff its exact receipt still matches."""

    if (
        not isinstance(receipt, str)
        or len(receipt) != 64
        or any(character not in "0123456789abcdef" for character in receipt)
    ):
        raise ValueError("receipt must be a lowercase SHA-256 digest")
    r = _get_redis(catalog)
    drain = RK.monitor_partition_drain(organization, monitor_type, date)
    receipt_key = RK.monitor_partition_receipt(
        organization, monitor_type, date,
    )
    script = """
    if redis.call('get', KEYS[2]) ~= ARGV[1] then
        return 0
    end
    local removed = redis.call('del', KEYS[1])
    if removed ~= 1 then
        return redis.error_reply('monitoring drain disappeared before acknowledgement')
    end
    redis.call('del', KEYS[2])
    return 1
    """
    try:
        return bool(r.eval(script, 2, drain, receipt_key, receipt))
    except Exception as exc:
        raise MonitoringPartitionError(
            "monitoring partition acknowledgement was not confirmed"
        ) from exc


# ---------------------------------------------------------------------------
# 1. list_drainable_partitions
# ---------------------------------------------------------------------------


def list_drainable_partitions(
    catalog: Any,
    *,
    organization: str,
    monitor_type: Optional[str] = None,
) -> List[MonitorPartition]:
    """Discover partitions whose date is strictly older than today (UTC).

    ``today`` is excluded because the writer is still appending to it.
    Anything older is frozen from the writer's perspective and safe to
    drain.

    Args:
        catalog: a ``RedisCatalog`` (or any object with a ``.r``
            redis-py client).
        organization: the org to scope the SCAN to.
        monitor_type: when set, narrow to one type
            (``plans|writes|mcp|odata|errors|locks``); otherwise return
            every type.

    Returns:
        ``List[MonitorPartition]`` sorted by ``(monitor_type, date)``.
        Empty list on Redis errors or missing args — never raises (the
        orchestrator's loop should never die on transient infra).
    """
    if not organization:
        return []

    # Validate monitor_type against the closed set so a bad input
    # returns [] cleanly instead of raising ValueError out of
    # ``RK.monitor_partition_pattern`` (which would violate the
    # "never raises" contract documented above). ``None`` means
    # "every type" and uses the wildcard pattern.
    if monitor_type is not None:
        from supertable.redis_keys import _VALID_MONITOR_TYPES
        if monitor_type not in _VALID_MONITOR_TYPES:
            return []

    try:
        r = _get_redis(catalog)
    except ValueError:
        return []

    pattern = (
        RK.monitor_partition_pattern(organization, monitor_type)
        if monitor_type
        else RK.monitor_partition_pattern_for_org(organization)
    )

    today = _today_utc()
    out: set[MonitorPartition] = set()
    try:
        for raw_key in r.scan_iter(match=pattern, count=512):
            key = _decode(raw_key)
            parsed = RK.parse_monitor_partition_key(key)
            if parsed is None:
                parsed = RK.parse_monitor_partition_drain_key(key)
            if parsed is None:
                continue
            org, mt, dt = parsed
            if dt >= today:
                # Today's partition is still being written to — skip.
                continue
            out.add(MonitorPartition(organization=org, monitor_type=mt, date=dt))
    except Exception as e:  # noqa: BLE001 — best-effort discovery
        logger.warning(
            "[monitoring.partitions] list scan failed for %s/%s: %s",
            organization, monitor_type or "*", e,
        )
        return []

    return sorted(out)


# ---------------------------------------------------------------------------
# 2. drain_partition
# ---------------------------------------------------------------------------


def drain_partition(
    catalog: Any,
    *,
    organization: str,
    monitor_type: str,
    date: str,
) -> List[Dict[str, Any]]:
    """Claim and read one partition without acknowledging its source.

    The implementation uses ``RENAMENX`` (atomic in Redis, **does not
    overwrite**) to move the source key out from under any concurrent
    writer in the rare day-boundary race where a writer started a batch
    before midnight UTC. After the rename, no writer can hit the old
    key again — they'd create a fresh empty one for the new day. We
    then ``LRANGE 0 -1`` the renamed handle. The handle is deliberately
    retained until :func:`acknowledge_partition` is called after a durable
    downstream write.

    **Crash recovery via RENAMENX.** Using ``RENAME`` here would be a
    silent-data-loss bug: a previous run crashing between LRANGE and
    DEL leaves the drain handle populated. The next call's ``RENAME``
    would overwrite the existing drain contents (Redis ``RENAME`` does
    an implicit DEL of the destination), destroying entries that were
    queued but not yet processed. ``RENAMENX`` returns 0 / False
    instead — we fall through to LRANGE the *existing* drain handle,
    so the earlier run's entries are still recovered. The fresh source
    is left in place and picked up by the next call.

    If the partition does not exist (e.g. another orchestrator drained
    it first, or no writes happened that day), returns an empty list
    silently.

    Args:
        catalog: a ``RedisCatalog`` (or any object with a ``.r``
            redis-py client).
        organization, monitor_type, date: partition coordinates.

    Returns:
        List of strictly parsed JSON-object payloads. Empty when the partition
        was already gone, held no entries, or could not be safely claimed.
        The source remains until an explicit receipt acknowledgement.

    Raises:
        ValueError: if ``catalog`` does not expose ``.r``. Other
            transient Redis errors are surfaced to the caller.
    """
    try:
        claim = claim_partition(
            catalog,
            organization=organization,
            monitor_type=monitor_type,
            date=date,
        )
    except MonitoringPartitionError as exc:
        logger.warning("[monitoring.partitions] claim failed: %s", exc)
        return []
    return list(claim.entries) if claim is not None else []


# ---------------------------------------------------------------------------
# 3. iter_partition_chunks
# ---------------------------------------------------------------------------


def iter_partition_chunks(
    catalog: Any,
    *,
    organization: str,
    monitor_type: str,
    date: str,
    chunk_size: int = 10_000,
) -> Iterator[List[Dict[str, Any]]]:
    """Compatibility replay-only chunk reader without acknowledgement.

    Same atomicity model as :func:`drain_partition` — the source key is
    ``RENAME``-d to a private drain handle up front, then sliced with
    ``LRANGE start stop``. Iterator exhaustion never deletes the drain handle;
    only an explicit receipt acknowledgement may do so after the sink commits.
    An exhausted or abandoned iterator therefore remains replayable.

    This legacy API does not expose the content receipt and therefore cannot
    safely acknowledge. New orchestrators must use
    :func:`claim_partition_chunks` followed by
    :func:`iter_claimed_partition_chunks`, then acknowledge that claim only
    after the downstream commit.

    **Resume semantics:** if your process crashes mid-iteration, the
    drain handle survives. Calling this function (or
    :func:`drain_partition`) again resumes from the handle — no data is
    lost. The trade-off is that re-reading does not skip
    already-consumed chunks; the orchestrator must keep its
    sink-table writes idempotent (e.g., include the entry's stable
    ``query_id`` so the sink can ignore replays).

    Args:
        catalog: a ``RedisCatalog`` (or any object with a ``.r``
            redis-py client).
        organization, monitor_type, date: partition coordinates.
        chunk_size: max entries per yield. Clamped to ``[1, 1_000_000]``.

    Yields:
        ``List[Dict[str, Any]]`` chunks of parsed payloads. A chunk
        may be empty only if every entry in that slice was malformed
        (rare); the iterator continues until the underlying LIST is
        exhausted.

    Raises:
        ValueError: if ``catalog`` does not expose ``.r``. Other
            transient Redis errors abort the iteration; the drain
            handle is left in place for a retry.
    """
    if chunk_size < 1:
        chunk_size = 1
    elif chunk_size > 1_000_000:
        chunk_size = 1_000_000

    r = _get_redis(catalog)

    src = RK.monitor_partition(organization, monitor_type, date)
    drain = RK.monitor_partition_drain(organization, monitor_type, date)

    # Atomic non-overwriting rename + PERSIST. Same semantics as
    # ``drain_partition``: if the drain key already exists from a
    # previous crashed run, RENAMENX returns False (does NOT overwrite),
    # we fall through to LRANGE the existing drain handle, the earlier
    # run's entries are recovered, and the new src is left in place for
    # the next call. Plain RENAME would silently destroy queued data.
    _claim_without_expiry(r, src, drain)

    # Determine how many entries we have to walk.
    try:
        total = int(r.llen(drain) or 0)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "[monitoring.partitions] chunk LLEN failed for %s/%s/%s: %s",
            organization, monitor_type, date, e,
        )
        return

    if total == 0:
        return

    start = 0
    try:
        while start < total:
            stop = min(start + chunk_size, total) - 1
            try:
                raw = r.lrange(drain, start, stop)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[monitoring.partitions] chunk LRANGE [%d..%d] failed for "
                    "%s/%s/%s: %s — leaving drain handle for retry",
                    start, stop, organization, monitor_type, date, e,
                )
                return
            chunk = _parse_entries(raw)
            yield chunk
            start = stop + 1
    finally:
        # Downstream persistence, not iterator exhaustion, is the authority
        # for deletion. Keep the exact drain available for replay.
        pass


# ---------------------------------------------------------------------------
# 4. read_recent — newest-first tail across partitions
# ---------------------------------------------------------------------------


def read_recent(
    catalog: Any,
    *,
    organization: str,
    monitor_type: str,
    limit: int = 100,
    max_days_back: int = 7,
) -> List[Dict[str, Any]]:
    """Read up to ``limit`` most-recent entries from the Redis partitions.

    Walks newest-first: today's partition tail, then yesterday's, then
    the day before, up to ``max_days_back`` calendar days (UTC). Stops
    as soon as ``limit`` entries are collected.

    Returns newest first (i.e. most recently ``RPUSH``-ed payload at
    index 0). Each entry is a parsed dict — the same shape as what
    :func:`drain_partition` returns. Malformed payloads are silently
    skipped.

    **Use this for the live tail** (monitoring UI "recent 100",
    debug introspection, etc.). For older data already drained into
    the sink tables (``__reads__``, ``__writes__``, ``__mcp__``), use
    the normal ``DataReader`` SQL path.

    **Read-only.** Unlike :func:`drain_partition` /
    :func:`iter_partition_chunks`, this function never mutates Redis
    state — no ``RENAME``, no ``DEL``. Safe to call concurrently from
    multiple processes (a UI poll, a debug session, an alert sweep)
    without coordination.

    Args:
        catalog: a ``RedisCatalog`` (or any object with a ``.r``
            redis-py client).
        organization: the org to scope reads to.
        monitor_type: closed set —
            ``plans | writes | mcp | odata | errors | locks``.
        limit: max entries to return. Clamped to ``[0, 1_000_000]``.
        max_days_back: how many calendar days to walk before giving up.
            ``1`` = today only. ``7`` (default) covers a typical
            monitoring UI's "last week" view. Clamped to
            ``[1, _MAX_READ_RECENT_DAYS_BACK]``.

    Returns:
        ``List[Dict[str, Any]]`` of length ``<= limit``, newest first.
        Returns ``[]`` on invalid input, missing catalog, or Redis
        errors — never raises (a monitoring UI must never crash on a
        transient infra hiccup).
    """
    # Input clamping.
    if limit <= 0:
        return []
    if limit > 1_000_000:
        limit = 1_000_000
    if max_days_back < 1:
        max_days_back = 1
    elif max_days_back > _MAX_READ_RECENT_DAYS_BACK:
        max_days_back = _MAX_READ_RECENT_DAYS_BACK
    # Validate the monitor_type against the same closed set
    # ``redis_keys.monitor_partition`` enforces — keeping the two in
    # sync via the import (not a hardcoded literal) means adding a new
    # type (e.g. ``"compact"``) lights up here automatically.
    from supertable.redis_keys import _VALID_MONITOR_TYPES
    if not organization or monitor_type not in _VALID_MONITOR_TYPES:
        return []

    try:
        r = _get_redis(catalog)
    except ValueError:
        return []

    today_dt = datetime.now(timezone.utc)
    collected: List[Dict[str, Any]] = []
    remaining = limit

    for offset in range(max_days_back):
        if remaining <= 0:
            break
        day = (today_dt - timedelta(days=offset)).strftime("%Y-%m-%d")
        try:
            key = RK.monitor_partition(organization, monitor_type, day)
        except ValueError:
            # Should never happen — monitor_type validated above and
            # date format is hard-coded — but defend against future
            # refactors that might break the assumption.
            continue

        try:
            # LRANGE -N -1 returns the last N items in insertion order
            # (oldest-of-N first, newest at the tail). When a key does
            # not exist Redis returns [] — no error.
            raw = r.lrange(key, -remaining, -1)
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "[monitoring.partitions] read_recent LRANGE failed for %s: %s",
                key, e,
            )
            # Skip this day but keep walking back; partial results are
            # better than empty.
            continue

        if not raw:
            # Either the partition does not exist (already drained,
            # or no writes that day) or it is empty. Keep walking.
            continue

        # Reverse so the freshest payload of this day comes first,
        # then prepend nothing — we extend collected in newest-first
        # order across days.
        parsed_reversed = _parse_entries(list(reversed(raw)))
        if not parsed_reversed:
            continue

        # Respect the overall limit (defense in depth — should not
        # over-count because we passed ``-remaining -1``).
        take = min(len(parsed_reversed), remaining)
        collected.extend(parsed_reversed[:take])
        remaining -= take

    return collected
