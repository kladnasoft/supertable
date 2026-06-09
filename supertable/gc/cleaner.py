# route: supertable.gc.cleaner
"""
Consumer side of the deferred-deletion GC pipeline.

A ``GCCleaner`` is a long-running daemon, one per organisation. On each
tick it walks every per-table GC stream in its org, drains entries older
than ``SUPERTABLE_GC_DELAY_SEC``, calls ``storage.delete()`` on each
referenced path, and ``XDEL``s the processed entries. Then it sleeps for
``SUPERTABLE_GC_SLEEP_SEC`` and starts again — the same shape as MSSQL's
CHECKPOINT or PostgreSQL's autovacuum.

The delay window is the safety guarantee: by the time the cleaner
touches a file, any in-flight reader that resolved the leaf payload
before the writer committed has long since finished. We never delete a
file that's still reachable from any reader's plan.

CLI
---
Run as a process:

    python -m supertable.gc.cleaner --org acme
    python -m supertable.gc.cleaner --all-orgs

``--all-orgs`` discovers orgs by SCANning ``gc_pending_pattern_all_orgs``
and spawns one cleaner thread per discovered org. New orgs that appear
after startup are picked up at the next discovery refresh
(``--discover-every-sec``).
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

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
    """Per-org cleaner daemon.

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
        up the env-driven settings.
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

        self._stop = threading.Event()

    # ── Lifecycle ────────────────────────────────────────────────────

    def stop(self) -> None:
        """Signal the daemon loop to exit at the next iteration."""
        self._stop.set()

    def run_forever(self) -> None:
        """Loop: tick, sleep, repeat — until ``stop()`` is called."""
        logger.info(
            "[gc-cleaner] org=%s starting (delay=%ds sleep=%ds batch=%d)",
            self.org, self.delay_sec, self.sleep_sec, self.batch_size,
        )
        while not self._stop.is_set():
            try:
                stats = self.tick()
                if stats["deleted"] or stats["streams_processed"]:
                    logger.info(
                        "[gc-cleaner] org=%s tick: streams=%d deleted=%d (parquet=%d snapshot=%d) errors=%d",
                        self.org,
                        stats["streams_processed"],
                        stats["deleted"],
                        stats["deleted_parquet"],
                        stats["deleted_snapshot"],
                        stats["errors"],
                    )
            except Exception as e:  # noqa: BLE001 — daemon must never die
                logger.error("[gc-cleaner] org=%s tick failed: %s", self.org, e)
            # Sleep in 1-second chunks so stop() interrupts promptly
            for _ in range(self.sleep_sec):
                if self._stop.is_set():
                    break
                time.sleep(1)
        logger.info("[gc-cleaner] org=%s stopped", self.org)

    # ── One pass ─────────────────────────────────────────────────────

    def tick(self) -> Dict[str, int]:
        """Process every per-table stream once. Returns stats dict.

        Stats keys: ``streams_processed``, ``deleted``,
        ``deleted_parquet``, ``deleted_snapshot``, ``errors``.
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


# ── --all-orgs runner ────────────────────────────────────────────────


def _discover_orgs(catalog: Any) -> Set[str]:
    """SCAN across every org and return the set of orgs that have at
    least one GC stream key."""
    r = getattr(catalog, "r", None)
    if r is None:
        return set()
    orgs: Set[str] = set()
    pattern = RK.gc_pending_pattern_all_orgs()
    try:
        for key in r.scan_iter(match=pattern, count=512):
            decoded = _decode(key)
            parsed = RK.parse_gc_pending_key(decoded)
            if parsed is not None:
                orgs.add(parsed[0])
    except Exception as e:  # noqa: BLE001
        logger.warning("[gc-cleaner] org discovery SCAN failed: %s", e)
    return orgs


def run_all_orgs(
    catalog: Any,
    storage: Any,
    *,
    delay_sec: Optional[int] = None,
    sleep_sec: Optional[int] = None,
    batch_size: Optional[int] = None,
    discover_every_sec: int = 300,
    stop_event: Optional[threading.Event] = None,
) -> None:
    """Spawn one ``GCCleaner`` thread per discovered org and re-scan
    periodically to pick up new orgs.

    Tenant isolation: each org gets its own thread; a slow storage
    backend on one org cannot block deletes on another.

    Args:
        catalog, storage: as for ``GCCleaner``.
        delay_sec, sleep_sec, batch_size: forwarded to each cleaner.
        discover_every_sec: how often to re-scan for new orgs.
        stop_event: external stop signal; one is created if omitted.
    """
    stop = stop_event or threading.Event()
    threads: Dict[str, Tuple[GCCleaner, threading.Thread]] = {}

    def _spawn(org: str) -> None:
        c = GCCleaner(
            org=org,
            catalog=catalog,
            storage=storage,
            delay_sec=delay_sec,
            sleep_sec=sleep_sec,
            batch_size=batch_size,
        )
        t = threading.Thread(
            target=c.run_forever,
            name=f"gc-cleaner-{org}",
            daemon=True,
        )
        threads[org] = (c, t)
        t.start()
        logger.info("[gc-cleaner-all] spawned thread for org=%s", org)

    # Initial discovery
    for org in _discover_orgs(catalog):
        if org not in threads:
            _spawn(org)

    # Periodic re-discovery
    while not stop.is_set():
        slept = 0
        while slept < discover_every_sec and not stop.is_set():
            time.sleep(1)
            slept += 1
        if stop.is_set():
            break
        for org in _discover_orgs(catalog):
            if org not in threads:
                _spawn(org)

    # Shutdown
    logger.info("[gc-cleaner-all] stopping %d cleaner thread(s)", len(threads))
    for c, _t in threads.values():
        c.stop()
    for _c, t in threads.values():
        t.join(timeout=10)


# ── CLI entrypoint ───────────────────────────────────────────────────


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m supertable.gc.cleaner",
        description=(
            "Drain Redis GC streams and physically delete sunset parquets "
            "+ pruned snapshot JSONs. Runs forever; one thread per org."
        ),
    )
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--org", help="Run a single cleaner for this organisation.")
    group.add_argument(
        "--all-orgs",
        action="store_true",
        help="Discover all orgs with pending GC entries and spawn one cleaner each.",
    )
    p.add_argument(
        "--delay-sec", type=int, default=None,
        help=f"Override SUPERTABLE_GC_DELAY_SEC (default {settings.SUPERTABLE_GC_DELAY_SEC}).",
    )
    p.add_argument(
        "--sleep-sec", type=int, default=None,
        help=f"Override SUPERTABLE_GC_SLEEP_SEC (default {settings.SUPERTABLE_GC_SLEEP_SEC}).",
    )
    p.add_argument(
        "--batch-size", type=int, default=None,
        help=f"Override SUPERTABLE_GC_BATCH_SIZE (default {settings.SUPERTABLE_GC_BATCH_SIZE}).",
    )
    p.add_argument(
        "--discover-every-sec", type=int, default=300,
        help="(--all-orgs only) Seconds between org re-discovery scans (default 300).",
    )
    p.add_argument(
        "--log-level", default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR). Default INFO.",
    )
    return p


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_arg_parser().parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    # Late imports so unit tests can monkey-patch before main() runs and
    # so the module can be imported without requiring storage credentials.
    from supertable.redis_catalog import RedisCatalog
    from supertable.storage.storage_factory import get_storage

    catalog = RedisCatalog()
    storage = get_storage()

    stop_event = threading.Event()

    def _on_signal(signum, _frame):  # noqa: ANN001
        logger.info("[gc-cleaner] received signal %s — stopping", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    if args.all_orgs:
        run_all_orgs(
            catalog=catalog,
            storage=storage,
            delay_sec=args.delay_sec,
            sleep_sec=args.sleep_sec,
            batch_size=args.batch_size,
            discover_every_sec=args.discover_every_sec,
            stop_event=stop_event,
        )
    else:
        cleaner = GCCleaner(
            org=args.org,
            catalog=catalog,
            storage=storage,
            delay_sec=args.delay_sec,
            sleep_sec=args.sleep_sec,
            batch_size=args.batch_size,
        )
        # Bridge signal → cleaner.stop
        def _on_signal_single(signum, _frame):  # noqa: ANN001
            logger.info("[gc-cleaner] received signal %s — stopping", signum)
            cleaner.stop()
            stop_event.set()
        signal.signal(signal.SIGINT, _on_signal_single)
        signal.signal(signal.SIGTERM, _on_signal_single)
        cleaner.run_forever()

    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
