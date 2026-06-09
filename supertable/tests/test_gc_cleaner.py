"""
Tests for supertable/gc/cleaner.py — the deferred-deletion consumer daemon.

Covers:
  1. helpers
     - _decode handles bytes + str
     - _decode_fields decodes a stream-entry dict
  2. GCCleaner construction
     - rejects empty org
     - respects override args
     - falls back to settings defaults when overrides are None
     - clamps invalid values to safe minimums
  3. GCCleaner.tick
     - no streams → all-zero stats
     - entries past cutoff → storage.delete + xdel each
     - entries before cutoff → xrange returns nothing → no-op
     - storage raises FileNotFoundError → still XDELs
     - storage raises other Exception → does NOT XDEL (retry next tick)
     - malformed entry (no path / unknown kind) → XDELs anyway
     - separates parquet / snapshot counters
     - SCAN failure → tick returns gracefully
     - per-stream XRANGE failure → tick increments errors and continues
  4. _discover_orgs
     - SCAN gives keys → orgs deduped
     - SCAN raises → returns empty set
  5. run_all_orgs
     - spawns one thread per discovered org
     - stop_event causes shutdown
"""
from __future__ import annotations

import os
import threading
import time
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.gc import cleaner as cleaner_mod  # noqa: E402
from supertable.gc.cleaner import (  # noqa: E402
    GCCleaner,
    _decode,
    _decode_fields,
    _discover_orgs,
    run_all_orgs,
)
from supertable import redis_keys as RK  # noqa: E402


ORG = "acme"
SUP = "demo"
SIMPLE = "orders"


# ---------------------------------------------------------------------------
# 1. helpers
# ---------------------------------------------------------------------------

class TestHelpers:

    def test_decode_bytes(self):
        assert _decode(b"hello") == "hello"

    def test_decode_str_passthrough(self):
        assert _decode("hello") == "hello"

    def test_decode_invalid_utf8_replaces(self):
        assert _decode(b"\xff\xfe") == "��"

    def test_decode_fields_mixed(self):
        out = _decode_fields({b"kind": b"parquet", "path": "x.parquet"})
        assert out == {"kind": "parquet", "path": "x.parquet"}

    def test_decode_fields_empty(self):
        assert _decode_fields({}) == {}
        assert _decode_fields(None) == {}  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 2. GCCleaner construction
# ---------------------------------------------------------------------------

class TestGCCleanerConstruction:

    def test_rejects_empty_org(self):
        with pytest.raises(ValueError):
            GCCleaner(org="", catalog=MagicMock(), storage=MagicMock())

    def test_overrides_applied(self):
        c = GCCleaner(
            org="acme",
            catalog=MagicMock(),
            storage=MagicMock(),
            delay_sec=10,
            sleep_sec=2,
            batch_size=7,
        )
        assert c.delay_sec == 10
        assert c.sleep_sec == 2
        assert c.batch_size == 7

    def test_falls_back_to_settings_when_none(self):
        c = GCCleaner(org="acme", catalog=MagicMock(), storage=MagicMock())
        from supertable.config.settings import settings
        assert c.delay_sec == settings.SUPERTABLE_GC_DELAY_SEC
        assert c.sleep_sec == settings.SUPERTABLE_GC_SLEEP_SEC
        assert c.batch_size == settings.SUPERTABLE_GC_BATCH_SIZE

    def test_clamps_negative_delay_to_zero(self):
        c = GCCleaner(org="acme", catalog=MagicMock(), storage=MagicMock(),
                     delay_sec=-5, sleep_sec=2, batch_size=1)
        assert c.delay_sec == 0

    def test_clamps_sleep_to_at_least_one(self):
        c = GCCleaner(org="acme", catalog=MagicMock(), storage=MagicMock(),
                     delay_sec=0, sleep_sec=0, batch_size=1)
        assert c.sleep_sec == 1

    def test_clamps_batch_size_to_at_least_one(self):
        c = GCCleaner(org="acme", catalog=MagicMock(), storage=MagicMock(),
                     delay_sec=0, sleep_sec=1, batch_size=-9)
        assert c.batch_size == 1

    def test_stop_sets_event(self):
        c = GCCleaner(org="acme", catalog=MagicMock(), storage=MagicMock())
        assert not c._stop.is_set()
        c.stop()
        assert c._stop.is_set()


# ---------------------------------------------------------------------------
# 3. GCCleaner.tick
# ---------------------------------------------------------------------------

def _make_cleaner(*, delay_sec=10, batch_size=500):
    """Build a cleaner with a fully-mocked catalog & storage. The .r
    client is exposed for test inspection."""
    storage = MagicMock()
    r = MagicMock()
    catalog = MagicMock()
    catalog.r = r
    c = GCCleaner(
        org=ORG, catalog=catalog, storage=storage,
        delay_sec=delay_sec, sleep_sec=1, batch_size=batch_size,
    )
    return c, r, storage


class TestTick:

    def test_no_streams_returns_zero_stats(self):
        c, r, _ = _make_cleaner()
        r.scan_iter.return_value = iter([])
        stats = c.tick()
        assert stats == {
            "streams_processed": 0,
            "deleted": 0,
            "deleted_parquet": 0,
            "deleted_snapshot": 0,
            "errors": 0,
        }
        r.xrange.assert_not_called()

    def test_processes_eligible_parquet_entries(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        key = RK.gc_pending(ORG, SUP, SIMPLE)
        r.scan_iter.return_value = iter([key])
        # Two old parquet entries
        r.xrange.return_value = [
            (b"1000-0", {b"kind": b"parquet", b"path": b"data/a.parquet"}),
            (b"1001-0", {b"kind": b"parquet", b"path": b"data/b.parquet"}),
        ]
        stats = c.tick()
        assert stats["streams_processed"] == 1
        assert stats["deleted"] == 2
        assert stats["deleted_parquet"] == 2
        assert stats["deleted_snapshot"] == 0
        assert stats["errors"] == 0
        storage.delete.assert_any_call("data/a.parquet")
        storage.delete.assert_any_call("data/b.parquet")
        r.xdel.assert_called_once()
        assert set(r.xdel.call_args[0][1:]) == {b"1000-0", b"1001-0"}

    def test_processes_eligible_snapshot_entries(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        key = RK.gc_pending(ORG, SUP, SIMPLE)
        r.scan_iter.return_value = iter([key])
        r.xrange.return_value = [
            (b"1000-0", {b"kind": b"snapshot", b"path": b"snap/v1.json"}),
        ]
        stats = c.tick()
        assert stats["deleted_snapshot"] == 1
        assert stats["deleted_parquet"] == 0
        storage.delete.assert_called_once_with("snap/v1.json")
        r.xdel.assert_called_once_with(key, b"1000-0")

    def test_separates_parquet_and_snapshot_counters(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        r.scan_iter.return_value = iter([RK.gc_pending(ORG, SUP, SIMPLE)])
        r.xrange.return_value = [
            (b"1-0", {b"kind": b"parquet", b"path": b"a.parquet"}),
            (b"2-0", {b"kind": b"parquet", b"path": b"b.parquet"}),
            (b"3-0", {b"kind": b"snapshot", b"path": b"s.json"}),
        ]
        stats = c.tick()
        assert stats["deleted"] == 3
        assert stats["deleted_parquet"] == 2
        assert stats["deleted_snapshot"] == 1

    def test_xrange_uses_cutoff_id(self):
        c, r, _ = _make_cleaner(delay_sec=100)
        key = RK.gc_pending(ORG, SUP, SIMPLE)
        r.scan_iter.return_value = iter([key])
        r.xrange.return_value = []
        c.tick()
        # min="-", max="<cutoff_ms>-0", count=batch_size
        args, kwargs = r.xrange.call_args
        assert args[0] == key
        assert kwargs["min"] == "-"
        assert kwargs["max"].endswith("-0")
        cutoff_ms = int(kwargs["max"].split("-")[0])
        # cutoff should be ~100 seconds ago (allow generous slack on slow CI)
        now_ms = int(time.time() * 1000)
        assert (now_ms - 105_000) <= cutoff_ms <= (now_ms - 95_000)

    def test_file_not_found_still_xdels(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        r.scan_iter.return_value = iter([RK.gc_pending(ORG, SUP, SIMPLE)])
        r.xrange.return_value = [
            (b"1-0", {b"kind": b"parquet", b"path": b"already_gone.parquet"}),
        ]
        storage.delete.side_effect = FileNotFoundError("nope")
        stats = c.tick()
        # FileNotFoundError is idempotent — count does not increment but XDEL still runs
        assert stats["deleted"] == 0
        r.xdel.assert_called_once_with(RK.gc_pending(ORG, SUP, SIMPLE), b"1-0")

    def test_other_storage_error_keeps_entry_for_retry(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        r.scan_iter.return_value = iter([RK.gc_pending(ORG, SUP, SIMPLE)])
        r.xrange.return_value = [
            (b"1-0", {b"kind": b"parquet", b"path": b"problem.parquet"}),
        ]
        storage.delete.side_effect = RuntimeError("transient")
        stats = c.tick()
        assert stats["deleted"] == 0
        # Entry NOT xdeled — will be retried on next tick
        r.xdel.assert_not_called()

    def test_mixed_success_and_failure(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        r.scan_iter.return_value = iter([RK.gc_pending(ORG, SUP, SIMPLE)])
        r.xrange.return_value = [
            (b"1-0", {b"kind": b"parquet", b"path": b"a.parquet"}),  # ok
            (b"2-0", {b"kind": b"parquet", b"path": b"b.parquet"}),  # fails
            (b"3-0", {b"kind": b"parquet", b"path": b"c.parquet"}),  # ok
        ]
        def _delete(path):
            if path == "b.parquet":
                raise RuntimeError("nope")
        storage.delete.side_effect = _delete
        stats = c.tick()
        assert stats["deleted"] == 2
        # Only ok entries xdeled (a + c)
        xdel_ids = set(r.xdel.call_args[0][1:])
        assert xdel_ids == {b"1-0", b"3-0"}

    def test_malformed_entry_no_path_is_xdeled(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        r.scan_iter.return_value = iter([RK.gc_pending(ORG, SUP, SIMPLE)])
        r.xrange.return_value = [
            (b"1-0", {b"kind": b"parquet"}),  # no path
            (b"2-0", {b"path": b"a.parquet"}),  # no kind
            (b"3-0", {b"kind": b"weird", b"path": b"b.parquet"}),  # unknown kind
        ]
        stats = c.tick()
        assert stats["deleted"] == 0
        storage.delete.assert_not_called()
        # All three malformed entries XDELed so they don't block progress
        xdel_ids = set(r.xdel.call_args[0][1:])
        assert xdel_ids == {b"1-0", b"2-0", b"3-0"}

    def test_scan_failure_returns_zero(self):
        c, r, storage = _make_cleaner()
        r.scan_iter.side_effect = RuntimeError("scan broke")
        stats = c.tick()
        # _discover_streams returns [] on SCAN error → no streams processed
        assert stats["streams_processed"] == 0

    def test_per_stream_xrange_failure_records_error_and_continues(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        k1 = RK.gc_pending(ORG, SUP, "alpha")
        k2 = RK.gc_pending(ORG, SUP, "beta")
        r.scan_iter.return_value = iter([k1, k2])

        def _xrange(key, **_kwargs):
            if key == k1:
                raise RuntimeError("broken")
            return [(b"1-0", {b"kind": b"parquet", b"path": b"x.parquet"})]

        r.xrange.side_effect = _xrange
        stats = c.tick()
        assert stats["errors"] == 1
        assert stats["streams_processed"] == 1  # beta succeeded
        assert stats["deleted"] == 1

    def test_empty_stream_still_counted_as_processed(self):
        c, r, _ = _make_cleaner(delay_sec=10)
        r.scan_iter.return_value = iter([RK.gc_pending(ORG, SUP, SIMPLE)])
        r.xrange.return_value = []
        stats = c.tick()
        # ``streams_processed`` counts streams the cleaner inspected
        # without error — empty streams included (so observability can
        # tell "0 to do" from "scan failed").
        assert stats["streams_processed"] == 1
        assert stats["deleted"] == 0

    def test_stop_event_breaks_loop_between_streams(self):
        c, r, storage = _make_cleaner(delay_sec=10)
        r.scan_iter.return_value = iter([
            RK.gc_pending(ORG, SUP, "a"),
            RK.gc_pending(ORG, SUP, "b"),
        ])

        def _xrange(*_args, **_kwargs):
            c.stop()
            return [(b"1-0", {b"kind": b"parquet", b"path": b"x.parquet"})]

        r.xrange.side_effect = _xrange
        stats = c.tick()
        # First stream processed, then stop breaks the loop before the second.
        assert stats["streams_processed"] == 1
        assert r.xrange.call_count == 1


# ---------------------------------------------------------------------------
# 4. _discover_orgs
# ---------------------------------------------------------------------------

class TestDiscoverOrgs:

    def test_dedupes_orgs(self):
        catalog = MagicMock()
        # Three keys spanning two orgs and two supertables
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
# 5. run_all_orgs
# ---------------------------------------------------------------------------

class TestRunAllOrgs:

    def test_spawns_thread_per_org_and_stops(self):
        catalog = MagicMock()
        catalog.r.scan_iter.return_value = iter([
            RK.gc_pending("acme", "sup", "t"),
            RK.gc_pending("brand", "sup", "t"),
        ])
        storage = MagicMock()

        # Patch GCCleaner so run_forever returns immediately
        instances: list = []

        class _FakeCleaner:
            def __init__(self, *, org, catalog, storage, delay_sec=None, sleep_sec=None, batch_size=None):
                self.org = org
                self._stopped = threading.Event()
                instances.append(self)
            def run_forever(self):
                # Block until stop is called so the test can drive the lifecycle
                while not self._stopped.is_set():
                    time.sleep(0.01)
            def stop(self):
                self._stopped.set()

        stop = threading.Event()

        with patch.object(cleaner_mod, "GCCleaner", _FakeCleaner):
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
                assert inst._stopped.is_set()
