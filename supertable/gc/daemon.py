# route: supertable.gc.daemon
"""
GC daemon — **convenience** wrapper around :class:`GCCleaner`.

If you already run a long-lived service that does its own scheduling,
**you don't need this module** — call :meth:`GCCleaner.tick` from your
own loop and own the lifecycle. This module exists for single-node
installs and ops teams that want a one-command "just run it"
deployment::

    python -m supertable.gc.daemon --org acme
    python -m supertable.gc.daemon --all-orgs

What this module adds on top of the library surface:

  * :func:`run_forever` — a tick / sleep / repeat loop with SIGINT and
    SIGTERM handling so the process exits cleanly when stopped.
  * :func:`run_all_orgs` — multi-org runner: discovers orgs by SCAN and
    spawns one ``GCCleaner`` thread per org. Periodically re-discovers
    so newly-active orgs are picked up without a restart.
  * :func:`main` — argparse-based CLI entry. Run as
    ``python -m supertable.gc.daemon`` from a container or systemd unit.

The orchestration semantics (delay window, batch size, per-tick
behavior) all live in :class:`supertable.gc.cleaner.GCCleaner`. This
module only adds **scheduling and lifecycle** — the kind of thing
a Kubernetes Deployment, systemd service, or supervisor process would
otherwise wrap around the library.
"""
from __future__ import annotations

import argparse
import logging
import signal
import sys
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from supertable import redis_keys as RK
from supertable.config.settings import settings
from supertable.gc.cleaner import GCCleaner, _decode

logger = logging.getLogger(__name__)


# ── Single-org daemon loop ───────────────────────────────────────────


def run_forever(cleaner: GCCleaner) -> None:
    """tick → sleep → repeat until ``cleaner.stop()`` is called.

    The loop sleeps in 1-second chunks so :meth:`GCCleaner.stop`
    interrupts within a second. Exceptions raised by ``tick()`` are
    caught and logged — the daemon must never die on a transient
    Redis or storage error.
    """
    logger.info(
        "[gc-daemon] org=%s starting (delay=%ds sleep=%ds batch=%d)",
        cleaner.org, cleaner.delay_sec, cleaner.sleep_sec, cleaner.batch_size,
    )
    while not cleaner._stop.is_set():
        try:
            stats = cleaner.tick()
            # Log when we did work OR when something errored. The
            # ``or stats["errors"]`` guard is important: a tick with
            # no work but transient SCAN/XRANGE errors must surface,
            # otherwise infra incidents stay invisible.
            if stats["deleted"] or stats["streams_processed"] or stats["errors"]:
                logger.info(
                    "[gc-daemon] org=%s tick: streams=%d deleted=%d (parquet=%d snapshot=%d) errors=%d",
                    cleaner.org,
                    stats["streams_processed"],
                    stats["deleted"],
                    stats["deleted_parquet"],
                    stats["deleted_snapshot"],
                    stats["errors"],
                )
        except Exception as e:  # noqa: BLE001 — daemon must never die
            logger.error("[gc-daemon] org=%s tick failed: %s", cleaner.org, e)
        # Sleep in 1-second chunks so stop() interrupts promptly
        for _ in range(cleaner.sleep_sec):
            if cleaner._stop.is_set():
                break
            time.sleep(1)
    logger.info("[gc-daemon] org=%s stopped", cleaner.org)


# ── Multi-org discovery + runner ─────────────────────────────────────


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
        logger.warning("[gc-daemon] org discovery SCAN failed: %s", e)
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
        catalog, storage: as for :class:`GCCleaner`.
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
            target=run_forever,
            args=(c,),
            name=f"gc-daemon-{org}",
            daemon=True,
        )
        threads[org] = (c, t)
        t.start()
        logger.info("[gc-daemon-all] spawned thread for org=%s", org)

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
    logger.info("[gc-daemon-all] stopping %d cleaner thread(s)", len(threads))
    for c, _t in threads.values():
        c.stop()
    for _c, t in threads.values():
        t.join(timeout=10)


# ── CLI entrypoint ───────────────────────────────────────────────────


def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="python -m supertable.gc.daemon",
        description=(
            "Drain Redis GC streams and physically delete sunset parquets "
            "+ pruned snapshot JSONs. Convenience daemon — most "
            "deployments should call supertable.gc.cleaner.GCCleaner.tick "
            "from their own scheduler instead."
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

    if args.all_orgs:
        def _on_signal(signum, _frame):  # noqa: ANN001
            logger.info("[gc-daemon] received signal %s — stopping", signum)
            stop_event.set()

        signal.signal(signal.SIGINT, _on_signal)
        signal.signal(signal.SIGTERM, _on_signal)

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

        def _on_signal_single(signum, _frame):  # noqa: ANN001
            logger.info("[gc-daemon] received signal %s — stopping", signum)
            cleaner.stop()
            stop_event.set()

        signal.signal(signal.SIGINT, _on_signal_single)
        signal.signal(signal.SIGTERM, _on_signal_single)
        run_forever(cleaner)

    return 0


__all__ = ["run_forever", "run_all_orgs", "main"]


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
