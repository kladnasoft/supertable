# supertable/tests/test_resolve_overwrite_writes.py
"""
Characterization tests for resolve_overwrite_writes (the DuckDB-pushdown
write-path probe) against the polars oracle it replaces.

resolve_overwrite_writes runs ONE column-projected, row-group-skipping,
null-safe SEMI JOIN over the overlapping data files and derives BOTH the
stale-filtered incoming frame and the (file, __rowid__) delete pairs from
that single read.  The pre-existing polars implementation
(filter_stale_incoming_rows + identify_deleted_rowids, which read whole
files) remains as the fallback and is the behavioral oracle here.

Each test writes REAL local parquet files and asserts:
  1. the DuckDB path was actually exercised (profiler 'probe_files' present,
     no 'overwrite_resolve_fallback'), and
  2. its (filtered rows, delete pairs) match the polars oracle exactly.

_get_storage is patched to a real LocalStorage so both paths read the same
local files regardless of the ambient STORAGE_TYPE.
"""
from __future__ import annotations

from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

from supertable.processing import (
    resolve_overwrite_writes,
    filter_stale_incoming_rows,
    identify_deleted_rowids,
)
from supertable.storage.local_storage import LocalStorage
from supertable.utils.profiler import Profiler


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(d, name, df):
    """Write a real parquet file; return an overlapping_files tuple."""
    path = str(d / name)
    pq.write_table(df.to_arrow(), path)
    return (path, True, (d / name).stat().st_size)


def _oracle(incoming, files, keys, ntc):
    """Polars full-read reference: stale filter then delete-pair identify."""
    filt = (
        filter_stale_incoming_rows(incoming, files, keys, ntc)
        if ntc else incoming
    )
    pairs = identify_deleted_rowids(filt, files, keys)
    return filt, pairs


def _rows(df):
    """Order-independent row multiset (read path gives no order guarantee)."""
    return sorted(df.sort(df.columns).to_dicts(), key=repr)


def _used_duck(counts):
    return "probe_files" in counts and "overwrite_resolve_fallback" not in counts


def _compare(incoming, files, keys, ntc):
    """Run both paths; assert DuckDB ran and equals the polars oracle."""
    exp_filt, exp_pairs = _oracle(incoming, files, keys, ntc)
    prof = Profiler()
    got_filt, got_pairs = resolve_overwrite_writes(
        incoming_df=incoming,
        overlapping_files=files,
        overwrite_columns=keys,
        newer_than_col=ntc,
        profiler=prof,
    )
    counts = prof.emit_counts()
    assert _used_duck(counts), f"DuckDB probe not exercised; counts={counts}"
    assert _rows(got_filt) == _rows(exp_filt), "filtered rows diverge from oracle"
    assert sorted(got_pairs) == sorted(exp_pairs), "delete pairs diverge from oracle"
    return got_filt, got_pairs, counts


@pytest.fixture(autouse=True)
def _local_storage():
    """Force both probe and oracle reads through a real LocalStorage."""
    with patch("supertable.processing._get_storage", return_value=LocalStorage()):
        yield


# ---------------------------------------------------------------------------
# Single-file newer_than scenarios
# ---------------------------------------------------------------------------

class TestSingleFileNewerThan:

    def test_stale_equal_replay(self, tmp_path):
        f = _write(tmp_path, "a.parquet", pl.DataFrame(
            {"__rowid__": [1], "user_id": [5], "name": ["Alice"], "updated_at": [7]}))
        incoming = pl.DataFrame({"user_id": [5], "name": ["Alice"], "updated_at": [7]})
        filt, pairs, _ = _compare(incoming, {f}, ["user_id"], "updated_at")
        assert filt.height == 0 and pairs == []

    def test_stale_older(self, tmp_path):
        f = _write(tmp_path, "a.parquet", pl.DataFrame(
            {"__rowid__": [1], "user_id": [5], "name": ["Alice"], "updated_at": [7]}))
        incoming = pl.DataFrame({"user_id": [5], "name": ["Alice"], "updated_at": [3]})
        filt, pairs, _ = _compare(incoming, {f}, ["user_id"], "updated_at")
        assert filt.height == 0 and pairs == []

    def test_genuine_newer(self, tmp_path):
        f = _write(tmp_path, "a.parquet", pl.DataFrame(
            {"__rowid__": [1], "user_id": [5], "name": ["Alice"], "updated_at": [7]}))
        incoming = pl.DataFrame({"user_id": [5], "name": ["Bob"], "updated_at": [9]})
        filt, pairs, _ = _compare(incoming, {f}, ["user_id"], "updated_at")
        assert filt.height == 1
        assert pairs == [(f[0], 1)]

    def test_new_key_no_overlap(self, tmp_path):
        f = _write(tmp_path, "a.parquet", pl.DataFrame(
            {"__rowid__": [1], "user_id": [5], "name": ["Alice"], "updated_at": [7]}))
        incoming = pl.DataFrame({"user_id": [99], "name": ["New"], "updated_at": [1]})
        filt, pairs, _ = _compare(incoming, {f}, ["user_id"], "updated_at")
        assert filt.height == 1 and pairs == []


# ---------------------------------------------------------------------------
# Multi-file max() and composite keys
# ---------------------------------------------------------------------------

class TestMultiFileAndComposite:

    def test_max_across_files_stale(self, tmp_path):
        f1 = _write(tmp_path, "f1.parquet", pl.DataFrame(
            {"__rowid__": [1], "user_id": [5], "name": ["A"], "updated_at": [3]}))
        f2 = _write(tmp_path, "f2.parquet", pl.DataFrame(
            {"__rowid__": [2], "user_id": [5], "name": ["B"], "updated_at": [7]}))
        # incoming ts=5 is below max(3,7)=7 → stale.
        incoming = pl.DataFrame({"user_id": [5], "name": ["C"], "updated_at": [5]})
        filt, pairs, _ = _compare(incoming, {f1, f2}, ["user_id"], "updated_at")
        assert filt.height == 0 and pairs == []

    def test_max_across_files_newer_tombstones_both(self, tmp_path):
        f1 = _write(tmp_path, "f1.parquet", pl.DataFrame(
            {"__rowid__": [1], "user_id": [5], "name": ["A"], "updated_at": [3]}))
        f2 = _write(tmp_path, "f2.parquet", pl.DataFrame(
            {"__rowid__": [2], "user_id": [5], "name": ["B"], "updated_at": [7]}))
        # incoming ts=8 beats max → survives, and deletes the key in BOTH files.
        incoming = pl.DataFrame({"user_id": [5], "name": ["C"], "updated_at": [8]})
        filt, pairs, _ = _compare(incoming, {f1, f2}, ["user_id"], "updated_at")
        assert filt.height == 1
        assert sorted(pairs) == sorted([(f1[0], 1), (f2[0], 2)])

    def test_composite_key_mixed(self, tmp_path):
        f = _write(tmp_path, "c.parquet", pl.DataFrame({
            "__rowid__": [1, 2], "user_id": [5, 5],
            "day": ["2024-01-01", "2024-01-02"], "value": [100, 200], "ts_ms": [10, 20],
        }))
        # (5,'01-01') ts=15 > 10 → survives+deletes rowid 1;
        # (5,'01-02') ts=5  < 20 → stale.
        incoming = pl.DataFrame({
            "user_id": [5, 5], "day": ["2024-01-01", "2024-01-02"],
            "value": [999, 999], "ts_ms": [15, 5],
        })
        filt, pairs, _ = _compare(incoming, {f}, ["user_id", "day"], "ts_ms")
        assert filt.height == 1
        assert pairs == [(f[0], 1)]


# ---------------------------------------------------------------------------
# Legacy files (schema evolution via union_by_name)
# ---------------------------------------------------------------------------

class TestLegacyFiles:

    def _legacy_set(self, tmp_path):
        m = _write(tmp_path, "modern.parquet", pl.DataFrame(
            {"__rowid__": [1, 2], "user_id": [5, 10], "updated_at": [7, 3]}))
        lnt = _write(tmp_path, "legacy_no_ts.parquet", pl.DataFrame(
            {"__rowid__": [3], "user_id": [5]}))           # missing newer_than col
        lnr = _write(tmp_path, "legacy_no_rowid.parquet", pl.DataFrame(
            {"user_id": [10], "updated_at": [99]}))          # missing __rowid__
        return m, lnt, lnr

    def test_legacy_key_newer_than_modern(self, tmp_path):
        m, lnt, lnr = self._legacy_set(tmp_path)
        incoming = pl.DataFrame({"user_id": [10], "v": [1], "updated_at": [100]})
        _compare(incoming, {m, lnt, lnr}, ["user_id"], "updated_at")

    def test_legacy_key_stale_against_legacy_ts(self, tmp_path):
        m, lnt, lnr = self._legacy_set(tmp_path)
        # legacy_no_rowid carries updated_at=99 for user 10 → incoming 50 is stale.
        incoming = pl.DataFrame({"user_id": [10], "v": [1], "updated_at": [50]})
        filt, pairs, _ = _compare(incoming, {m, lnt, lnr}, ["user_id"], "updated_at")
        assert filt.height == 0 and pairs == []

    def test_legacy_missing_newer_than_treated_new(self, tmp_path):
        m, lnt, lnr = self._legacy_set(tmp_path)
        # user 5 has ts=7 (modern) and a legacy row lacking ts; incoming 8 > 7.
        _compare(incoming=pl.DataFrame({"user_id": [5], "v": [1], "updated_at": [8]}),
                 files={m, lnt, lnr}, keys=["user_id"], ntc="updated_at")

    def test_no_newer_than_delete_upsert(self, tmp_path):
        m, lnt, lnr = self._legacy_set(tmp_path)
        # No newer_than: every matching existing rowid is tombstoned; rows kept.
        incoming = pl.DataFrame({"user_id": [5, 10], "v": [1, 2]})
        filt, pairs, _ = _compare(incoming, {m, lnt, lnr}, ["user_id"], None)
        assert filt.height == 2  # nothing filtered without newer_than


# ---------------------------------------------------------------------------
# Null keys (typed null column — null-safe matching)
# ---------------------------------------------------------------------------

class TestNullKeys:

    def _null_file(self, tmp_path):
        return _write(tmp_path, "n.parquet", pl.DataFrame({
            "__rowid__": [1, 2],
            "user_id": pl.Series([5, None], dtype=pl.Int64),
            "updated_at": [7, 7],
        }))

    def test_null_key_newer(self, tmp_path):
        f = self._null_file(tmp_path)
        incoming = pl.DataFrame({
            "user_id": pl.Series([None], dtype=pl.Int64), "v": [1], "updated_at": [9]})
        _compare(incoming, {f}, ["user_id"], "updated_at")

    def test_null_key_stale_dropped(self, tmp_path):
        f = self._null_file(tmp_path)
        # Null-safe stale filter (R7): the incoming NULL key (updated_at=6) compares
        # against the existing NULL-group max (7) and is dropped as stale, so it
        # tombstones nothing — the newer existing NULL-keyed row is preserved.
        incoming = pl.DataFrame({
            "user_id": pl.Series([None], dtype=pl.Int64), "v": [1], "updated_at": [6]})
        filt, pairs, _ = _compare(incoming, {f}, ["user_id"], "updated_at")
        assert filt.height == 0 and pairs == []
