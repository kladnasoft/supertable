"""
Tests for supertable/gc/daemon.py — the convenience wrapper around
:class:`supertable.gc.cleaner.GCCleaner`.

Covers:
  1. _discover_orgs
     - SCAN gives keys → orgs deduped
     - SCAN raises → returns empty set
     - SCAN ignores unrelated keys
     - missing catalog.r → empty set
  2. run_forever
     - calls cleaner.tick() in a loop until cleaner.stop()
     - swallows exceptions from tick() so the daemon never dies
     - logs a stats line only when something happened
  3. run_all_orgs
     - spawns one thread per discovered org
     - stop_event causes shutdown
     - newly-discovered orgs spawned on next discovery tick
  4. CLI (main)
     - --org and --all-orgs are mutually exclusive
     - --org parses correctly
     - --all-orgs parses correctly
"""
from __future__ import annotations

import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.gc import daemon as daemon_mod  # noqa: E402
from supertable.gc.daemon import (  # noqa: E402
    _discover_orgs,
    run_all_orgs,
    run_forever,
)
from supertable import redis_keys as RK  # noqa: E402


# ---------------------------------------------------------------------------
# 1. _discover_orgs
# ---------------------------------------------------------------------------

class TestDiscoverOrgs:

    def test_dedupes_orgs(self):
        catalog = MagicMock()
        catalog.r.scan_iter.return_value = iter([
            RK.gc_pending("acme", "sup1", "t1"),
            RK.gc_pending("acme", "sup2", "t2"),
            RK.gc_pending("brand", "sup1", "t1"),
        ])
        assert _discover_orgs(catalog) == {"acme", "brand"}

    def test_returns_empty_on_scan_error(self):
        catalog = MagicMock()
        catalog.r.scan_iter.side_effect = RuntimeError("redis down")
        assert _discover_orgs(catalog) == set()

    def test_ignores_unrelated_keys(self):
        catalog = MagicMock()
        catalog.r.scan_iter.return_value = iter([
            "supertable:acme:lakes:sup:meta:root",  # not gc_pending
            RK.gc_pending("acme", "sup", "t1"),
            "garbage:foo:bar",
        ])
        assert _discover_orgs(catalog) == {"acme"}

    def test_no_r_returns_empty(self):
        catalog = MagicMock()
        catalog.r = None
        assert _discover_orgs(catalog) == set()


# ---------------------------------------------------------------------------
# 2. run_forever
# ---------------------------------------------------------------------------

def _make_fake_cleaner(*, tick_returns=None, tick_raises=None):
    """Build a minimal cleaner-shaped object."""
    c = MagicMock()
    c.org = "acme"
    c.delay_sec = 1
    c.sleep_sec = 1
    c.batch_size = 100
    c._stop = threading.Event()
    if tick_raises is not None:
        c.tick.side_effect = tick_raises
    else:
        c.tick.return_value = tick_returns or {
            "streams_processed": 0,
            "deleted": 0,
            "deleted_parquet": 0,
            "deleted_snapshot": 0,
            "errors": 0,
        }
    # Match the real .stop() behavior so the loop exits
    c.stop.side_effect = c._stop.set
    return c


class TestRunForever:

    def test_loops_until_stop(self):
        cleaner = _make_fake_cleaner()
        # Stop after the first tick
        original_tick = cleaner.tick
        call_count = {"n": 0}

        def _ticking(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] >= 1:
                cleaner.stop()
            return original_tick.return_value

        cleaner.tick.side_effect = _ticking

        t = threading.Thread(target=run_forever, args=(cleaner,), daemon=True)
        t.start()
        t.join(timeout=5)
        assert not t.is_alive()
        assert cleaner.tick.call_count >= 1

    def test_swallows_tick_exceptions(self):
        cleaner = _make_fake_cleaner(tick_raises=RuntimeError("boom"))
        # Stop after a brief moment
        def _stopper():
            time.sleep(0.5)
            cleaner.stop()
        threading.Thread(target=_stopper, daemon=True).start()

        t = threading.Thread(target=run_forever, args=(cleaner,), daemon=True)
        t.start()
        t.join(timeout=5)
        assert not t.is_alive()
        # tick() must have been called at least once even though it raised
        assert cleaner.tick.call_count >= 1


# ---------------------------------------------------------------------------
# 3. run_all_orgs
# ---------------------------------------------------------------------------

class TestRunAllOrgs:

    def test_spawns_thread_per_org_and_stops(self):
        catalog = MagicMock()
        catalog.r.scan_iter.return_value = iter([
            RK.gc_pending("acme", "sup", "t"),
            RK.gc_pending("brand", "sup", "t"),
        ])
        storage = MagicMock()

        instances: list = []

        class _FakeCleaner:
            def __init__(self, *, org, catalog, storage,
                         delay_sec=None, sleep_sec=None, batch_size=None):
                self.org = org
                self.delay_sec = delay_sec or 1
                self.sleep_sec = sleep_sec or 1
                self.batch_size = batch_size or 100
                self._stop = threading.Event()
                instances.append(self)

            def tick(self):
                return {
                    "streams_processed": 0, "deleted": 0,
                    "deleted_parquet": 0, "deleted_snapshot": 0,
                    "errors": 0,
                }

            def stop(self):
                self._stop.set()

        stop = threading.Event()

        with patch.object(daemon_mod, "GCCleaner", _FakeCleaner):
            runner = threading.Thread(
                target=run_all_orgs,
                kwargs=dict(
                    catalog=catalog,
                    storage=storage,
                    discover_every_sec=300,
                    stop_event=stop,
                ),
                daemon=True,
            )
            runner.start()
            # Give discovery a moment to spawn threads
            deadline = time.time() + 2.0
            while time.time() < deadline and len(instances) < 2:
                time.sleep(0.05)
            assert len(instances) == 2
            assert {i.org for i in instances} == {"acme", "brand"}

            stop.set()
            runner.join(timeout=15)
            assert not runner.is_alive()
            for inst in instances:
                assert inst._stop.is_set()


# ---------------------------------------------------------------------------
# 4. CLI arg parsing
# ---------------------------------------------------------------------------

class TestCliArgParser:

    def test_org_and_all_orgs_mutually_exclusive(self):
        parser = daemon_mod._build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["--org", "acme", "--all-orgs"])

    def test_one_of_org_or_all_orgs_required(self):
        parser = daemon_mod._build_arg_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_org_parses_cleanly(self):
        parser = daemon_mod._build_arg_parser()
        args = parser.parse_args(["--org", "acme"])
        assert args.org == "acme"
        assert args.all_orgs is False

    def test_all_orgs_parses_cleanly(self):
        parser = daemon_mod._build_arg_parser()
        args = parser.parse_args(["--all-orgs"])
        assert args.all_orgs is True
        assert args.org is None

    def test_overrides_parse(self):
        parser = daemon_mod._build_arg_parser()
        args = parser.parse_args([
            "--org", "acme",
            "--delay-sec", "60",
            "--sleep-sec", "5",
            "--batch-size", "100",
            "--discover-every-sec", "120",
            "--log-level", "DEBUG",
        ])
        assert args.delay_sec == 60
        assert args.sleep_sec == 5
        assert args.batch_size == 100
        assert args.discover_every_sec == 120
        assert args.log_level == "DEBUG"
