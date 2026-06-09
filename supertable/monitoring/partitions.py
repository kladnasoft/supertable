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
  * :func:`drain_partition` — atomic read + delete of one partition.
  * :func:`iter_partition_chunks` — memory-bounded streaming variant
    with explicit resume semantics.

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

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterator, List, NamedTuple, Optional

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
    out: List[MonitorPartition] = []
    try:
        for raw_key in r.scan_iter(match=pattern, count=512):
            key = _decode(raw_key)
            parsed = RK.parse_monitor_partition_key(key)
            if parsed is None:
                continue
            org, mt, dt = parsed
            if dt >= today:
                # Today's partition is still being written to — skip.
                continue
            out.append(MonitorPartition(organization=org, monitor_type=mt, date=dt))
    except Exception as e:  # noqa: BLE001 — best-effort discovery
        logger.warning(
            "[monitoring.partitions] list scan failed for %s/%s: %s",
            organization, monitor_type or "*", e,
        )
        return []

    out.sort()
    return out


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
    """Atomically take a snapshot of one partition and delete the source.

    The implementation uses ``RENAMENX`` (atomic in Redis, **does not
    overwrite**) to move the source key out from under any concurrent
    writer in the rare day-boundary race where a writer started a batch
    before midnight UTC. After the rename, no writer can hit the old
    key again — they'd create a fresh empty one for the new day. We
    then ``LRANGE 0 -1`` the renamed handle and ``DEL`` it.

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
        List of parsed JSON payloads. Empty when the partition was
        already gone or held no entries. Malformed entries are
        silently skipped.

    Raises:
        ValueError: if ``catalog`` does not expose ``.r``. Other
            transient Redis errors are surfaced to the caller.
    """
    r = _get_redis(catalog)

    src = RK.monitor_partition(organization, monitor_type, date)
    drain = RK.monitor_partition_drain(organization, monitor_type, date)

    # Atomic non-overwriting rename. Three outcomes:
    #   1. src exists, drain does not  →  renamenx returns truthy.
    #                                     We own the drain handle.
    #   2. src exists, drain also exists (previous run crashed mid-process)
    #      →  renamenx returns falsy. We DON'T destroy the existing
    #         drain contents. We LRANGE the drain handle to recover the
    #         earlier run's entries; the new src is left in place and
    #         the *next* call to drain_partition picks it up.
    #   3. src does not exist           →  renamenx raises ResponseError.
    #                                     LRANGE drain still works (it
    #                                     may be empty or hold prior data).
    renamed = False
    try:
        # redis-py returns True/False or 1/0 depending on encoding mode
        renamed = bool(r.renamenx(src, drain))
    except Exception:
        # ResponseError when src missing, network blip, etc. Either way
        # try the drain key — it may legitimately hold data from a
        # crashed previous run.
        renamed = False

    try:
        raw_entries = r.lrange(drain, 0, -1)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "[monitoring.partitions] drain LRANGE failed for %s/%s/%s: %s",
            organization, monitor_type, date, e,
        )
        # Leave the drain key in place so a retry can finish the job.
        return []

    if not raw_entries:
        # Nothing in the drain handle. If we did rename, delete the
        # (empty) handle to keep Redis clean.
        if renamed:
            try:
                r.delete(drain)
            except Exception:
                pass
        return []

    entries = _parse_entries(raw_entries)

    # Successful read — delete the drain handle. Failure here means a
    # retry will redeliver entries; the caller's write to the sink
    # table should be idempotent (e.g. include a stable ``query_id``).
    try:
        r.delete(drain)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "[monitoring.partitions] drain DEL failed for %s/%s/%s: %s "
            "(entries already returned; next call will redeliver)",
            organization, monitor_type, date, e,
        )

    return entries


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
    """Stream a partition in memory-bounded chunks; delete on exhaustion.

    Same atomicity model as :func:`drain_partition` — the source key is
    ``RENAME``-d to a private drain handle up front, then sliced with
    ``LRANGE start stop``. The drain handle is ``DEL``-ed when the
    iterator exhausts.

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

    # Atomic non-overwriting rename. Same semantics as
    # ``drain_partition``: if the drain key already exists from a
    # previous crashed run, RENAMENX returns False (does NOT overwrite),
    # we fall through to LRANGE the existing drain handle, the earlier
    # run's entries are recovered, and the new src is left in place for
    # the next call. Plain RENAME would silently destroy queued data.
    try:
        r.renamenx(src, drain)
    except Exception:
        pass

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
        # Either drain didn't exist or the partition was empty. Delete
        # the (possibly empty) handle to keep Redis clean.
        try:
            r.delete(drain)
        except Exception:
            pass
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
        # Iterator either exhausted naturally or the caller bailed
        # (``break``); either way attempt the cleanup. If the caller
        # bailed early we delete a partially-consumed handle —
        # acceptable because chunked drain is opt-in (orchestrator
        # chose this mode and must handle replay-on-resume anyway).
        if start >= total:
            try:
                r.delete(drain)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[monitoring.partitions] chunk DEL failed for %s/%s/%s: "
                    "%s (entries already yielded)",
                    organization, monitor_type, date, e,
                )


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
