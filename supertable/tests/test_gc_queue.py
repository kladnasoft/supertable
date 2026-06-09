"""
Tests for supertable/gc/queue.py — the deferred-deletion producer.

Covers:
  1. enqueue_deletions
     - empty paths → no-op, returns 0
     - happy path with parquet kind → XADDs every entry, returns count
     - happy path with snapshot kind
     - unknown kind → returns 0, no XADD
     - invalid org segment → returns 0, no XADD
     - missing .r on catalog → returns 0
     - Redis pipeline raises → returns 0
     - write_id field carried when provided
  2. nuke_stream
     - calls r.delete with the gc_pending key
     - invalid segment → returns False
     - Redis raises → returns False
  3. collect_old_snapshot_paths
     - keep_n=0 → []
     - keep_n=1 → returns the entire prior chain
     - keep_n=2 → keeps newest + first predecessor, returns rest
     - non-dict input → []
     - FileNotFoundError during walk → stops with what it has
     - cycle in chain → stops, logs
     - chain shorter than keep_n → returns []
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock

import pytest

# Environment defence — settings module reads at import time
os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.gc.queue import (  # noqa: E402
    collect_old_snapshot_paths,
    enqueue_deletions,
    nuke_stream,
)
from supertable import redis_keys as RK  # noqa: E402


ORG = "acme"
SUP = "demo"
SIMPLE = "orders"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_catalog_with_pipeline(execute_returns=None, execute_raises=None):
    """Build a MagicMock catalog whose .r.pipeline() context manager
    captures XADD calls and returns ``execute_returns`` from .execute()
    (or raises ``execute_raises``)."""
    pipe = MagicMock()
    pipe.__enter__ = MagicMock(return_value=pipe)
    pipe.__exit__ = MagicMock(return_value=False)
    if execute_raises is not None:
        pipe.execute.side_effect = execute_raises
    else:
        pipe.execute.return_value = execute_returns or []
    r = MagicMock()
    r.pipeline.return_value = pipe
    catalog = MagicMock()
    catalog.r = r
    return catalog, r, pipe


# ===========================================================================
# 1. enqueue_deletions
# ===========================================================================

class TestEnqueueDeletions:

    def test_empty_paths_returns_zero(self):
        cat, r, _ = _mock_catalog_with_pipeline()
        assert enqueue_deletions(cat, ORG, SUP, SIMPLE, "parquet", []) == 0
        r.pipeline.assert_not_called()

    def test_none_paths_returns_zero(self):
        cat, r, _ = _mock_catalog_with_pipeline()
        assert enqueue_deletions(cat, ORG, SUP, SIMPLE, "parquet", None) == 0
        r.pipeline.assert_not_called()

    def test_filters_out_falsy_paths(self):
        cat, r, pipe = _mock_catalog_with_pipeline(execute_returns=[b"1-0", b"2-0"])
        # Mix of "" and valid paths — empty ones filtered out
        n = enqueue_deletions(cat, ORG, SUP, SIMPLE, "parquet", ["", "a.parquet", "", "b.parquet"])
        assert n == 2
        assert pipe.xadd.call_count == 2

    def test_happy_path_parquet(self):
        cat, r, pipe = _mock_catalog_with_pipeline(execute_returns=[b"1-0", b"2-0", b"3-0"])
        n = enqueue_deletions(
            cat, ORG, SUP, SIMPLE, "parquet",
            ["a.parquet", "b.parquet", "c.parquet"],
            write_id="qid-42",
        )
        assert n == 3
        expected_key = RK.gc_pending(ORG, SUP, SIMPLE)
        # All three XADD calls on the same stream key with parquet + write_id
        for c in pipe.xadd.call_args_list:
            args, _ = c
            assert args[0] == expected_key
            fields = args[1]
            assert fields["kind"] == "parquet"
            assert fields["write_id"] == "qid-42"
            assert "path" in fields

    def test_happy_path_snapshot_without_write_id(self):
        cat, r, pipe = _mock_catalog_with_pipeline(execute_returns=[b"1-0"])
        n = enqueue_deletions(cat, ORG, SUP, SIMPLE, "snapshot", ["snap_v1.json"])
        assert n == 1
        fields = pipe.xadd.call_args_list[0][0][1]
        assert fields["kind"] == "snapshot"
        assert fields["path"] == "snap_v1.json"
        assert "write_id" not in fields  # absent when not supplied

    def test_unknown_kind_returns_zero(self):
        cat, r, pipe = _mock_catalog_with_pipeline()
        assert enqueue_deletions(cat, ORG, SUP, SIMPLE, "garbage", ["a.parquet"]) == 0
        r.pipeline.assert_not_called()
        pipe.xadd.assert_not_called()

    def test_invalid_org_segment_returns_zero(self):
        # Sentinel name fails _safe(); enqueue must not crash, just bail.
        cat, r, _ = _mock_catalog_with_pipeline()
        assert enqueue_deletions(cat, "_apps_", SUP, SIMPLE, "parquet", ["a.parquet"]) == 0
        r.pipeline.assert_not_called()

    def test_missing_r_returns_zero(self):
        cat = MagicMock()
        del cat.r  # no .r attribute at all
        # MagicMock auto-creates attrs, so set to None explicitly
        cat.r = None
        assert enqueue_deletions(cat, ORG, SUP, SIMPLE, "parquet", ["a.parquet"]) == 0

    def test_pipeline_raises_returns_zero(self):
        cat, _r, _pipe = _mock_catalog_with_pipeline(execute_raises=RuntimeError("redis down"))
        assert enqueue_deletions(cat, ORG, SUP, SIMPLE, "parquet", ["a.parquet"]) == 0

    def test_execute_returns_some_failures(self):
        # Pipeline can return mixed results; the count reflects truthy results.
        cat, _r, _pipe = _mock_catalog_with_pipeline(execute_returns=[b"1-0", None, b"3-0"])
        n = enqueue_deletions(cat, ORG, SUP, SIMPLE, "parquet",
                              ["a.parquet", "b.parquet", "c.parquet"])
        assert n == 2


# ===========================================================================
# 2. nuke_stream
# ===========================================================================

class TestNukeStream:

    def test_calls_delete_with_gc_pending_key(self):
        r = MagicMock()
        cat = MagicMock()
        cat.r = r
        assert nuke_stream(cat, ORG, SUP, SIMPLE) is True
        r.delete.assert_called_once_with(RK.gc_pending(ORG, SUP, SIMPLE))

    def test_missing_r_returns_false(self):
        cat = MagicMock()
        cat.r = None
        assert nuke_stream(cat, ORG, SUP, SIMPLE) is False

    def test_invalid_segment_returns_false(self):
        cat = MagicMock()
        cat.r = MagicMock()
        assert nuke_stream(cat, "_sentinel_", SUP, SIMPLE) is False
        cat.r.delete.assert_not_called()

    def test_redis_error_returns_false(self):
        r = MagicMock()
        r.delete.side_effect = RuntimeError("redis down")
        cat = MagicMock()
        cat.r = r
        assert nuke_stream(cat, ORG, SUP, SIMPLE) is False


# ===========================================================================
# 3. collect_old_snapshot_paths
# ===========================================================================

class _FakeStorage:
    """Tiny storage that resolves paths from a dict, mimicking read_json."""

    def __init__(self, snapshots: dict):
        self._snapshots = snapshots
        self.reads: list[str] = []

    def read_json(self, path: str) -> dict:
        self.reads.append(path)
        if path not in self._snapshots:
            raise FileNotFoundError(path)
        return self._snapshots[path]


class TestCollectOldSnapshotPaths:

    def test_keep_n_zero_returns_empty(self):
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({"v3.json": {"previous_snapshot": None}})
        assert collect_old_snapshot_paths(new_snap, storage, keep_n=0) == []
        # No storage reads should happen at all
        assert storage.reads == []

    def test_keep_n_negative_returns_empty(self):
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({"v3.json": {"previous_snapshot": None}})
        assert collect_old_snapshot_paths(new_snap, storage, keep_n=-5) == []

    def test_non_dict_input_returns_empty(self):
        storage = _FakeStorage({})
        assert collect_old_snapshot_paths("not a dict", storage, keep_n=2) == []  # type: ignore[arg-type]
        assert collect_old_snapshot_paths(None, storage, keep_n=2) == []  # type: ignore[arg-type]

    def test_no_previous_snapshot_returns_empty(self):
        new_snap = {"previous_snapshot": None}
        storage = _FakeStorage({})
        assert collect_old_snapshot_paths(new_snap, storage, keep_n=2) == []

    def test_keep_n_one_returns_entire_prior_chain(self):
        # Newest is the in-memory new_snap (kept as #1)
        # Chain: new → v3 → v2 → v1 → None
        # keep_n=1 means keep only the newest → return [v3, v2, v1]
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({
            "v3.json": {"previous_snapshot": "v2.json"},
            "v2.json": {"previous_snapshot": "v1.json"},
            "v1.json": {"previous_snapshot": None},
        })
        out = collect_old_snapshot_paths(new_snap, storage, keep_n=1)
        assert out == ["v3.json", "v2.json", "v1.json"]

    def test_keep_n_two_keeps_newest_plus_one_predecessor(self):
        # keep_n=2 → keep newest + v3, return [v2, v1]
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({
            "v3.json": {"previous_snapshot": "v2.json"},
            "v2.json": {"previous_snapshot": "v1.json"},
            "v1.json": {"previous_snapshot": None},
        })
        out = collect_old_snapshot_paths(new_snap, storage, keep_n=2)
        assert out == ["v2.json", "v1.json"]

    def test_keep_n_three_keeps_newest_plus_two_predecessors(self):
        # keep_n=3 → keep newest + v3 + v2, return [v1]
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({
            "v3.json": {"previous_snapshot": "v2.json"},
            "v2.json": {"previous_snapshot": "v1.json"},
            "v1.json": {"previous_snapshot": None},
        })
        out = collect_old_snapshot_paths(new_snap, storage, keep_n=3)
        assert out == ["v1.json"]

    def test_chain_shorter_than_keep_n_returns_empty(self):
        # Only one predecessor exists; keep_n=5 → nothing to delete
        new_snap = {"previous_snapshot": "v1.json"}
        storage = _FakeStorage({"v1.json": {"previous_snapshot": None}})
        assert collect_old_snapshot_paths(new_snap, storage, keep_n=5) == []

    def test_file_not_found_during_walk_after_window_collects_and_stops(self):
        # keep_n=2 → keep newest + v3. v2 is past the retention window.
        # We can identify v2 as a deletion candidate from v3's
        # ``previous_snapshot`` pointer without reading v2 itself — so
        # v2 IS collected, then the attempt to follow further fails
        # gracefully (FileNotFound on v2) and the walk stops.
        # Enqueuing an already-deleted file is safe: the cleaner's
        # storage.delete() handles FileNotFoundError idempotently.
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({
            "v3.json": {"previous_snapshot": "v2.json"},
            # v2.json missing
        })
        out = collect_old_snapshot_paths(new_snap, storage, keep_n=2)
        assert out == ["v2.json"]

    def test_file_not_found_in_keep_window_stops_and_returns_empty(self):
        # keep_n=3 → keep newest + v3 + v2. We need to load v2 (it's in
        # the keep window) to continue the walk; but v2 is missing.
        # Nothing past the keep window has been collected yet, so out=[].
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({
            "v3.json": {"previous_snapshot": "v2.json"},
            # v2.json missing — within the keep window
        })
        out = collect_old_snapshot_paths(new_snap, storage, keep_n=3)
        assert out == []

    def test_file_not_found_after_collecting_returns_partial(self):
        # keep_n=1 → keep only the newest. v3 is past the window:
        # collect v3, then need v3's dict to find its predecessor v2 →
        # successfully loaded. Then collect v2, then need v2 to continue →
        # FileNotFound → stop.
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({
            "v3.json": {"previous_snapshot": "v2.json"},
            # v2.json missing — chain broken after v3
        })
        out = collect_old_snapshot_paths(new_snap, storage, keep_n=1)
        assert out == ["v3.json", "v2.json"]

    def test_cycle_stops_walk(self):
        # Self-cycle: v3 → v3
        new_snap = {"previous_snapshot": "v3.json"}
        storage = _FakeStorage({
            "v3.json": {"previous_snapshot": "v3.json"},
        })
        out = collect_old_snapshot_paths(new_snap, storage, keep_n=1)
        # Collected v3 once, then detected cycle → return [v3]
        assert out == ["v3.json"]

    def test_storage_exception_stops_walk_gracefully(self):
        class _ExplodingStorage:
            def __init__(self):
                self.calls = 0
            def read_json(self, path):
                self.calls += 1
                raise RuntimeError("storage on fire")

        new_snap = {"previous_snapshot": "v3.json"}
        # keep_n=1: collect v3, then try to read it → blows up → return [v3]
        out = collect_old_snapshot_paths(new_snap, _ExplodingStorage(), keep_n=1)
        assert out == ["v3.json"]
