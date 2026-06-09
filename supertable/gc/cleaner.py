# route: supertable.gc.cleaner
"""
GC orchestration primitives — the **library** surface.

This module exposes :class:`GCCleaner`, a pure orchestration object
whose only state-modifying operation is :meth:`GCCleaner.tick`. One
``tick()`` is one full pass over the org's per-table GC streams: drain
entries older than the configured delay, ``storage.delete()`` each
referenced path, ``XDEL`` the processed entries, return a stats dict.

The expected usage pattern is **caller-owned scheduling** — your
service decides when to call ``tick()``:

    cleaner = GCCleaner(org="acme", catalog=catalog, storage=storage,
                       delay_sec=1800, batch_size=500)
    while not service.shutdown_requested:
        stats = cleaner.tick()
        service.publish_metrics(stats)
        service.sleep(60)

For deployments that don't already have a scheduler, the convenience
daemon in :mod:`supertable.gc.daemon` wraps this class with a
``run_forever`` loop and a CLI entrypoint. **Most deployments should
prefer calling ``tick()`` directly from their own scheduler** —
running ``run_forever`` is just here so that single-node installs
have a one-command setup path.

The delay window (``SUPERTABLE_GC_DELAY_SEC``) is the safety
guarantee: by the time the cleaner touches a file, any in-flight
reader that resolved the leaf payload before the writer committed has
long since finished. We never delete a file that's still reachable
from any reader's plan.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

from supertable import redis_keys as RK
from supertable.config.settings import settings

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _decode(v: Any) -> str:
    """Decode a redis-py value (bytes or str) to str."""
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8")
        except UnicodeDecodeError:
            return v.decode("utf-8", errors="replace")
    return v


def _decode_fields(fields: Dict[Any, Any]) -> Dict[str, str]:
    """Decode a redis-py stream-entry field dict to {str: str}."""
    return {_decode(k): _decode(v) for k, v in (fields or {}).items()}


class GCCleaner:
    """Per-org cleaner — orchestration primitive.

    Calling :meth:`tick` performs one full pass over the org's GC
    streams. It is intentionally **pure orchestration**: no loops, no
    threads, no signal handlers. The caller's service is expected to
    own scheduling, retries, and shutdown.

    For deployments that don't have an external scheduler, the
    convenience wrapper in :mod:`supertable.gc.daemon` adds a
    ``run_forever`` loop and a CLI entrypoint on top of this class.

    Parameters
    ----------
    org:
        Organisation name this cleaner is responsible for.
    catalog:
        ``RedisCatalog`` (or any object with a ``.r`` redis-py client).
    storage:
        ``StorageInterface`` instance used to delete files.
    delay_sec, sleep_sec, batch_size:
        Override the corresponding ``settings.*`` defaults. Useful for
        tests; production code should leave these as ``None`` and pick
        up the env-driven settings. ``sleep_sec`` is only consulted by
        the daemon wrapper and has no effect on ``tick()`` itself.
    """

    def __init__(
        self,
        org: str,
        catalog: Any,
        storage: Any,
        *,
        delay_sec: Optional[int] = None,
        sleep_sec: Optional[int] = None,
        batch_size: Optional[int] = None,
    ) -> None:
        if not org:
            raise ValueError("GCCleaner: org is required")
        self.org = org
        self.catalog = catalog
        self.storage = storage
        self.delay_sec = int(delay_sec if delay_sec is not None else settings.SUPERTABLE_GC_DELAY_SEC)
        self.sleep_sec = int(sleep_sec if sleep_sec is not None else settings.SUPERTABLE_GC_SLEEP_SEC)
        self.batch_size = int(batch_size if batch_size is not None else settings.SUPERTABLE_GC_BATCH_SIZE)
        if self.delay_sec < 0:
            self.delay_sec = 0
        if self.sleep_sec < 1:
            self.sleep_sec = 1
        if self.batch_size < 1:
            self.batch_size = 1

        # ``_stop`` is consulted by :meth:`tick` between streams so the
        # daemon wrapper (or a test) can interrupt a long-running tick.
        # The library surface itself never touches it after init.
        self._stop = threading.Event()

    # ── Lifecycle ────────────────────────────────────────────────────

    def stop(self) -> None:
        """Signal an in-progress :meth:`tick` to bail early.

        Used by the daemon wrapper and tests. After calling ``stop()``,
        a subsequent ``tick()`` returns as soon as the current stream
        finishes. The stop flag is not auto-reset — to re-use the
        cleaner after a stop, instantiate a new one (cheap; no
        persistent state).
        """
        self._stop.set()

    # ── One pass ─────────────────────────────────────────────────────

    def tick(self) -> Dict[str, int]:
        """Process every per-table stream once. Returns stats dict.

        Stats keys: ``streams_processed``, ``deleted``,
        ``deleted_parquet``, ``deleted_snapshot``, ``errors``.

        Idempotent: re-calling produces the same effect if no new
        entries appeared. Safe to call concurrently from multiple
        processes — the worst case is two cleaners XDEL-ing the same
        entry (Redis tolerates this) and double-attempting a
        ``storage.delete`` (also idempotent via ``FileNotFoundError``).
        """
        stats = {
            "streams_processed": 0,
            "deleted": 0,
            "deleted_parquet": 0,
            "deleted_snapshot": 0,
            "errors": 0,
        }

        cutoff_ms = _now_ms() - self.delay_sec * 1000
        if cutoff_ms < 0:
            cutoff_ms = 0
        cutoff_id = f"{cutoff_ms}-0"

        for stream_key in self._discover_streams():
            if self._stop.is_set():
                break
            try:
                processed, by_kind = self._drain_stream(stream_key, cutoff_id)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[gc-cleaner] org=%s stream=%s drain failed: %s",
                    self.org, stream_key, e,
                )
                stats["errors"] += 1
                continue
            stats["streams_processed"] += 1
            stats["deleted"] += processed
            stats["deleted_parquet"] += by_kind.get("parquet", 0)
            stats["deleted_snapshot"] += by_kind.get("snapshot", 0)

        return stats

    # ── Internals ────────────────────────────────────────────────────

    def _discover_streams(self) -> Iterable[str]:
        """SCAN the org's GC streams and yield each key as a str."""
        r = getattr(self.catalog, "r", None)
        if r is None:
            return []
        pattern = RK.gc_pending_pattern_for_org(self.org)
        try:
            return [_decode(k) for k in r.scan_iter(match=pattern, count=512)]
        except Exception as e:  # noqa: BLE001
            logger.warning("[gc-cleaner] org=%s SCAN failed: %s", self.org, e)
            return []

    def _drain_stream(
        self, stream_key: str, cutoff_id: str
    ) -> Tuple[int, Dict[str, int]]:
        """XRANGE one stream up to cutoff, delete files, XDEL entries.

        Returns ``(processed_count, by_kind)`` where ``by_kind`` maps
        ``"parquet"|"snapshot"`` → count. Re-raises Redis errors so the
        caller's per-stream error budget tracks them.
        """
        r = self.catalog.r
        # Inclusive bounds: "-" = the very first id, "<cutoff_ms>-0" =
        # the smallest id at the cutoff millisecond. Anything strictly
        # after the cutoff is not returned.
        entries = r.xrange(stream_key, min="-", max=cutoff_id, count=self.batch_size)
        if not entries:
            return 0, {}

        deleted = 0
        by_kind: Dict[str, int] = {"parquet": 0, "snapshot": 0}
        to_xdel: List[bytes] = []

        for entry_id, raw_fields in entries:
            if self._stop.is_set():
                break
            fields = _decode_fields(raw_fields)
            kind = fields.get("kind", "")
            path = fields.get("path", "")
            if not path or kind not in ("parquet", "snapshot"):
                # Malformed entry — XDEL it so it doesn't block progress
                to_xdel.append(entry_id)
                continue

            try:
                self.storage.delete(path)
                deleted += 1
                by_kind[kind] = by_kind.get(kind, 0) + 1
            except FileNotFoundError:
                # Idempotent: already gone (another cleaner, manual
                # removal, etc.) — still XDEL the entry.
                pass
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[gc-cleaner] storage.delete failed for %s (kind=%s): %s",
                    path, kind, e,
                )
                # Leave entry on the stream so we retry next tick.
                continue

            to_xdel.append(entry_id)

        if to_xdel:
            try:
                r.xdel(stream_key, *to_xdel)
            except Exception as e:  # noqa: BLE001
                logger.warning(
                    "[gc-cleaner] XDEL failed on %s for %d entries: %s",
                    stream_key, len(to_xdel), e,
                )

        return deleted, by_kind


__all__ = ["GCCleaner"]
