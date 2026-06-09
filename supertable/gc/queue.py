# route: supertable.gc.queue
"""
Producer side of the deferred-deletion GC pipeline.

The writer calls :func:`enqueue_deletions` *after* the leaf-CAS commit has
succeeded. The order matters: if XADD happened first and the CAS then
failed (race retry), the stream would point at files still referenced by
the live snapshot and the cleaner would silently delete live data. The
reverse order — CAS first, XADD second — has only one failure mode (writer
crash between the two), which causes a file leak, identical to today's
behaviour.

All functions in this module are *best-effort*: GC failures must never
fail a write. Every Redis call is wrapped in a broad except so a Redis
outage degrades the system to "today's behaviour" (files leak; nothing
breaks).
"""
from __future__ import annotations

import logging
from typing import Any, Iterable, List, Optional, Set

from supertable import redis_keys as RK

logger = logging.getLogger(__name__)


# Safety cap on snapshot-chain walk depth. The walker stops earlier on
# cycles or missing JSONs; this guard exists so a corrupted chain cannot
# loop indefinitely.
_MAX_CHAIN_WALK = 10_000


def enqueue_deletions(
    catalog: Any,
    org: str,
    sup: str,
    simple: str,
    kind: str,
    paths: Iterable[str],
    write_id: Optional[str] = None,
) -> int:
    """XADD one entry per path onto the per-table GC stream.

    Args:
        catalog: a ``RedisCatalog`` (or any object exposing a ``.r``
            redis-py client). Passed in rather than constructed so tests
            and the writer share the same connection.
        org, sup, simple: per-table coordinates.
        kind: ``"parquet"`` or ``"snapshot"`` — only used by the cleaner
            and observability tooling for filtering/tracing.
        paths: iterable of storage paths to delete.
        write_id: optional correlation id (the writer's ``qid``) carried
            on each entry for tracing.

    Returns:
        Number of entries successfully enqueued (best-effort; 0 on
        Redis failure).
    """
    paths_list = [p for p in (paths or []) if p]
    if not paths_list:
        return 0

    if kind not in ("parquet", "snapshot"):
        # Defensive: don't enqueue unknown kinds, they'd just be ignored
        # by the cleaner and create noise.
        logger.warning("[gc-queue] unknown kind %r, skipping %d paths", kind, len(paths_list))
        return 0

    try:
        stream_key = RK.gc_pending(org, sup, simple)
    except ValueError as e:
        # Invalid org/sup/simple — surface to logs and bail. This should
        # never happen in practice because the same names just passed
        # validation elsewhere in the write path.
        logger.warning("[gc-queue] invalid key segment, dropping %d paths: %s", len(paths_list), e)
        return 0

    r = getattr(catalog, "r", None)
    if r is None:
        logger.debug("[gc-queue] catalog has no .r client, skipping enqueue")
        return 0

    enqueued = 0
    try:
        with r.pipeline(transaction=False) as p:
            for path in paths_list:
                fields = {"kind": kind, "path": path}
                if write_id:
                    fields["write_id"] = write_id
                # Redis Stream IDs are <ms>-<seq> natively — the cleaner
                # uses XRANGE - <cutoff_ms>-0 to enforce the delay window,
                # so we deliberately do NOT stamp our own scheduled_at.
                p.xadd(stream_key, fields)
            results = p.execute()
        enqueued = sum(1 for r in results if r)
    except Exception as e:  # noqa: BLE001 — best-effort by design
        logger.warning(
            "[gc-queue] enqueue failed for %s/%s/%s kind=%s n=%d: %s",
            org, sup, simple, kind, len(paths_list), e,
        )
        return 0

    logger.debug(
        "[gc-queue] enqueued %d %s path(s) on %s",
        enqueued, kind, stream_key,
    )
    return enqueued


def nuke_stream(catalog: Any, org: str, sup: str, simple: str) -> bool:
    """DEL the per-table GC stream. Called by ``SimpleTable.delete()``.

    Returns True on success (or if the key didn't exist), False if the
    DEL itself errored.
    """
    r = getattr(catalog, "r", None)
    if r is None:
        return False
    try:
        key = RK.gc_pending(org, sup, simple)
    except ValueError as e:
        logger.warning("[gc-queue] nuke invalid key segment %s/%s/%s: %s", org, sup, simple, e)
        return False
    try:
        r.delete(key)
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning("[gc-queue] nuke failed for %s: %s", key, e)
        return False


def collect_old_snapshot_paths(
    new_snapshot: dict,
    storage: Any,
    keep_n: int,
) -> List[str]:
    """Walk ``previous_snapshot`` from ``new_snapshot`` and return paths
    older than the retention window.

    With ``keep_n = N``, we keep the newest snapshot (the one we just
    wrote) plus the ``N − 1`` immediate predecessors. Everything from
    the ``N``-th predecessor onwards is returned as a candidate for
    deletion.

    The walk reads predecessor JSONs from storage. Missing JSONs stop
    the walk (a previous GC run already pruned them); a cycle also
    stops it. The walk is capped at ``_MAX_CHAIN_WALK`` as a defence
    against corrupted chains.

    Args:
        new_snapshot: the snapshot dict that was just written.
        storage: any object exposing ``read_json(path) -> dict``.
        keep_n: number of snapshots to keep, including the newest.
            ``keep_n <= 0`` short-circuits to ``[]`` (today's behaviour).

    Returns:
        List of snapshot JSON storage paths that may be deleted.
    """
    if keep_n <= 0:
        return []
    if not isinstance(new_snapshot, dict):
        return []

    # Walk: the newest counts as position 0. We need to follow
    # ``previous_snapshot`` (keep_n - 1) times to keep, then collect
    # everything from there on.
    keep_remaining = max(0, keep_n - 1)
    cursor = new_snapshot
    cursor_path: Optional[str] = None
    visited: Set[str] = set()
    out: List[str] = []

    for _ in range(_MAX_CHAIN_WALK):
        prev_path = cursor.get("previous_snapshot") if isinstance(cursor, dict) else None
        if not prev_path:
            break
        if prev_path in visited:
            logger.warning("[gc-queue] snapshot chain cycle at %s", prev_path)
            break
        visited.add(prev_path)

        if keep_remaining > 0:
            keep_remaining -= 1
            # Need to load this predecessor to continue the walk.
            try:
                cursor = storage.read_json(prev_path)
            except FileNotFoundError:
                # Already pruned earlier — chain is short here.
                return out
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[gc-queue] could not read predecessor %s during walk: %s",
                    prev_path, e,
                )
                return out
            cursor_path = prev_path
            continue

        # Past the retention window — collect this path and keep walking
        # without reading the JSON. We only need its predecessor pointer,
        # which we *do* need to read from storage. If the JSON is missing
        # we stop (anything older is also already gone).
        out.append(prev_path)
        try:
            cursor = storage.read_json(prev_path)
        except FileNotFoundError:
            return out
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "[gc-queue] could not read snapshot %s during walk: %s",
                prev_path, e,
            )
            return out
        cursor_path = prev_path

    if cursor_path is None:
        # silence "assigned but unused" — kept for future debug logging
        pass

    return out
