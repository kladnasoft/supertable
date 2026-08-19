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

import dataclasses
import os
import threading
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

import supertable.processing as st_processing
from supertable.processing import (
    resolve_overwrite_writes,
    filter_stale_incoming_rows,
    identify_deleted_rowids,
)
from supertable.config.settings import settings
from supertable.storage.local_storage import LocalStorage
from supertable.utils.profiler import Profiler


@pytest.fixture(autouse=True)
def _enable_write_probe(monkeypatch):
    """These tests validate the DuckDB pushdown probe path, which is opt-in
    (``SUPERTABLE_DUCKDB_WRITE_PROBE``, default off).  Force it on so the probe
    is actually exercised; without this the gate in ``resolve_overwrite_writes``
    would route every call to the polars fallback and the probe assertions
    (``probe_files`` present, no ``overwrite_resolve_fallback``) would be vacuous."""
    monkeypatch.setattr(
        st_processing, "settings",
        dataclasses.replace(settings, SUPERTABLE_DUCKDB_WRITE_PROBE=True),
    )


@pytest.fixture(autouse=True)
def _clear_local_integrity_cache():
    st_processing._LOCAL_ROWID_INTEGRITY_CACHE.clear()
    yield
    st_processing._LOCAL_ROWID_INTEGRITY_CACHE.clear()


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
        with pytest.raises((ValueError, Exception), match="rowid|__rowid__|column"):
            resolve_overwrite_writes(incoming, {m, lnt, lnr}, ["user_id"], "updated_at")

    def test_legacy_key_stale_against_legacy_ts(self, tmp_path):
        m, lnt, lnr = self._legacy_set(tmp_path)
        # legacy_no_rowid carries updated_at=99 for user 10 → incoming 50 is stale.
        incoming = pl.DataFrame({"user_id": [10], "v": [1], "updated_at": [50]})
        with pytest.raises((ValueError, Exception), match="rowid|__rowid__|column"):
            resolve_overwrite_writes(incoming, {m, lnt, lnr}, ["user_id"], "updated_at")

    def test_legacy_missing_newer_than_treated_new(self, tmp_path):
        m, lnt, lnr = self._legacy_set(tmp_path)
        # user 5 has ts=7 (modern) and a legacy row lacking ts; incoming 8 > 7.
        _compare(incoming=pl.DataFrame({"user_id": [5], "v": [1], "updated_at": [8]}),
                 files={m, lnt}, keys=["user_id"], ntc="updated_at")

    def test_no_newer_than_delete_upsert(self, tmp_path):
        m, lnt, lnr = self._legacy_set(tmp_path)
        # No newer_than: every matching existing rowid is tombstoned; rows kept.
        incoming = pl.DataFrame({"user_id": [5, 10], "v": [1, 2]})
        filt, pairs, _ = _compare(incoming, {m, lnt}, ["user_id"], None)
        assert filt.height == 2  # nothing filtered without newer_than

    def test_missing_key_plus_incoming_null_aborts_instead_of_false_tombstone(self, tmp_path):
        modern = _write(tmp_path, "modern.parquet", pl.DataFrame(
            {"__rowid__": [1], "user_id": pl.Series([5], dtype=pl.Int64)}))
        missing_key = _write(tmp_path, "missing_key.parquet", pl.DataFrame(
            {"__rowid__": [2], "value": ["unrelated"]}))
        incoming = pl.DataFrame({
            "user_id": pl.Series([None], dtype=pl.Int64), "value": ["new"],
        })

        with pytest.raises((ValueError, Exception), match="user_id|column"):
            resolve_overwrite_writes(
                incoming, {modern, missing_key}, ["user_id"], None
            )

    def test_missing_rowid_aborts_instead_of_partial_overwrite(self, tmp_path):
        legacy = _write(tmp_path, "missing_rowid.parquet", pl.DataFrame(
            {"user_id": [5], "value": ["old"]}))
        incoming = pl.DataFrame({"user_id": [5], "value": ["new"]})

        with pytest.raises((ValueError, Exception), match="rowid|__rowid__|column"):
            resolve_overwrite_writes(incoming, {legacy}, ["user_id"], None)


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


class TestProbeStrictEquivalence:

    def test_duplicate_source_rowid_aborts_before_emitting_tombstone(
            self, tmp_path,
    ):
        source = _write(tmp_path, "duplicate-rowid.parquet", pl.DataFrame({
            "__rowid__": pl.Series([7, 7], dtype=pl.Int64),
            "key": [1, 2],
        }))

        with pytest.raises(ValueError, match="duplicate rowids"):
            resolve_overwrite_writes(
                pl.DataFrame({"key": [1]}),
                {source},
                ["key"],
                None,
            )

    def test_dead_newer_row_cannot_make_valid_reinsertion_stale(self, tmp_path):
        f = _write(tmp_path, "dead-newer.parquet", pl.DataFrame({
            "__rowid__": [1], "key": [7], "version": [100],
        }))
        incoming = pl.DataFrame({"key": [7], "version": [50]})
        tombstones = pl.DataFrame(
            {"__file__": [f[0]], "__rowid__": [1]},
            schema={"__file__": pl.Utf8, "__rowid__": pl.Int64},
        )
        filtered, pairs = resolve_overwrite_writes(
            incoming, {f}, ["key"], "version",
            existing_tombstones=tombstones,
        )
        assert filtered.rows() == [(7, 50)]
        assert pairs == []

    def test_polars_fallback_does_not_expand_full_vector_to_python_sets(
            self, tmp_path, monkeypatch,
    ):
        f = _write(tmp_path, "dead.parquet", pl.DataFrame({
            "__rowid__": [1], "key": [7], "version": [100],
        }))
        incoming = pl.DataFrame({"key": [7], "version": [50]})
        tombstones = pl.DataFrame(
            {
                "__file__": [f[0]] + ["unrelated.parquet"] * 1_000,
                "__rowid__": list(range(1, 1_002)),
            },
            schema={"__file__": pl.Utf8, "__rowid__": pl.Int64},
        )
        monkeypatch.setattr(
            st_processing,
            "settings",
            dataclasses.replace(
                settings,
                SUPERTABLE_DUCKDB_WRITE_PROBE=False,
                SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO=False,
            ),
        )
        original_to_list = pl.Series.to_list

        def reject_large_python_expansion(series):
            if len(series) > 10:
                raise AssertionError("full deletion vector expanded to Python objects")
            return original_to_list(series)

        monkeypatch.setattr(pl.Series, "to_list", reject_large_python_expansion)
        filtered, pairs = resolve_overwrite_writes(
            incoming, {f}, ["key"], "version",
            existing_tombstones=tombstones,
        )

        assert filtered.rows() == [(7, 50)]
        assert pairs == []

    def test_noninjective_resolved_paths_fall_back_without_wrong_file_key(
            self, tmp_path, monkeypatch,
    ):
        a = _write(tmp_path, "a.parquet", pl.DataFrame({
            "__rowid__": [1], "key": [7],
        }))
        b = _write(tmp_path, "b.parquet", pl.DataFrame({
            "__rowid__": [2], "key": [7],
        }))
        monkeypatch.setattr(
            st_processing, "_storage_duckdb_path",
            lambda storage, key, force_presign=False: "same-resolved-url",
        )
        prof = Profiler()
        filtered, pairs = resolve_overwrite_writes(
            pl.DataFrame({"key": [7]}),
            {a, b},
            ["key"],
            None,
            profiler=prof,
        )

        assert filtered.rows() == [(7,)]
        assert sorted(pairs) == sorted([(a[0], 1), (b[0], 2)])
        assert prof.emit_counts().get("overwrite_resolve_fallback") == 1

    def test_string_keys_remain_case_sensitive_under_nocase_connection(self, tmp_path):
        f = _write(tmp_path, "case.parquet", pl.DataFrame(
            {"__rowid__": [1], "key": ["A"], "value": ["old"]}))
        incoming = pl.DataFrame({"key": ["a"], "value": ["new"]})
        filtered, pairs, _ = _compare(incoming, {f}, ["key"], None)
        assert filtered.height == 1
        assert pairs == []

    def test_key_type_mismatch_uses_strict_fallback(self, tmp_path):
        f = _write(tmp_path, "int32.parquet", pl.DataFrame({
            "__rowid__": [1],
            "key": pl.Series([5], dtype=pl.Int32),
        }))
        incoming = pl.DataFrame({"key": pl.Series([5], dtype=pl.Int64)})
        filtered, pairs = resolve_overwrite_writes(incoming, {f}, ["key"], None)
        assert filtered.height == 1
        assert pairs == [(f[0], 1)]


class TestLocalRowidIntegrityCache:
    @staticmethod
    def _probe(candidate, incoming, profiler, storage=None):
        storage = storage or LocalStorage()
        return st_processing._duckdb_probe_overlap_matches(
            [(candidate[0], candidate[2])],
            ["key"],
            None,
            incoming.select("key").unique(),
            incoming_schema=dict(incoming.schema),
            profiler=profiler,
            storage=storage,
        )

    def test_unchanged_identity_skips_only_the_warm_integrity_scan(self, tmp_path):
        candidate = _write(tmp_path, "warm.parquet", pl.DataFrame({
            "__rowid__": [11, 12], "key": [7, 8],
        }))
        incoming = pl.DataFrame({"key": [7]})

        cold = Profiler()
        warm = Profiler()
        assert self._probe(candidate, incoming, cold).height == 1
        assert self._probe(candidate, incoming, warm).height == 1

        cold_counts = cold.emit_counts()
        warm_counts = warm.emit_counts()
        assert cold_counts["probe_rowid_integrity_cache_misses"] == 1
        assert cold_counts["probe_rowid_integrity_scanned_files"] == 1
        assert cold_counts["io.duckdb_probe_rowid_integrity.n"] == 1
        assert warm_counts["probe_rowid_integrity_cache_hits"] == 1
        assert "probe_rowid_integrity_scanned_files" not in warm_counts
        assert "io.duckdb_probe_rowid_integrity.n" not in warm_counts
        # Schema and key projection are deliberately never cached.
        assert warm_counts["io.duckdb_probe_schema.n"] == 1
        assert warm_counts["io.duckdb_probe.n"] == 1

    @pytest.mark.parametrize("replace_inode", [False, True])
    def test_change_or_replacement_invalidates_identity(
        self, tmp_path, replace_inode,
    ):
        candidate = _write(tmp_path, "changed.parquet", pl.DataFrame({
            "__rowid__": [1], "key": [7],
        }))
        incoming = pl.DataFrame({"key": [7]})
        assert self._probe(candidate, incoming, Profiler()).height == 1

        replacement = tmp_path / ("replacement.parquet" if replace_inode else "changed.parquet")
        pq.write_table(pl.DataFrame({
            "__rowid__": [99, 100], "key": [7, 9],
        }).to_arrow(), replacement)
        if replace_inode:
            os.replace(replacement, candidate[0])
        changed = (candidate[0], True, os.stat(candidate[0]).st_size)
        profiler = Profiler()
        matched = self._probe(changed, incoming, profiler)

        assert matched is not None
        assert matched.get_column("__rowid__").to_list() == [99]
        assert profiler.emit_counts()["probe_rowid_integrity_cache_misses"] == 1

    def test_corrupt_replacement_is_rescanned_never_cached_and_falls_back(
        self, tmp_path,
    ):
        candidate = _write(tmp_path, "corrupt.parquet", pl.DataFrame({
            "__rowid__": [1, 2], "key": [7, 8],
        }))
        incoming = pl.DataFrame({"key": [7]})
        assert self._probe(candidate, incoming, Profiler()).height == 1

        replacement = tmp_path / "corrupt-replacement.parquet"
        pq.write_table(pl.DataFrame({
            "__rowid__": [5, 5], "key": [7, 8],
        }).to_arrow(), replacement)
        os.replace(replacement, candidate[0])
        corrupt = (candidate[0], True, os.stat(candidate[0]).st_size)

        for _attempt in range(2):
            profiler = Profiler()
            assert self._probe(corrupt, incoming, profiler) is None
            assert profiler.emit_counts()["probe_rowid_integrity_cache_misses"] == 1
        with pytest.raises(ValueError, match="duplicate rowids"):
            resolve_overwrite_writes(
                incoming, {corrupt}, ["key"], None, storage=LocalStorage(),
            )

    def test_lru_eviction_forces_a_rescan(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            st_processing, "_LOCAL_ROWID_INTEGRITY_CACHE_MAX_ENTRIES", 2,
        )
        incoming = pl.DataFrame({"key": [7]})
        candidates = [
            _write(tmp_path, f"evict-{index}.parquet", pl.DataFrame({
                "__rowid__": [index + 1], "key": [7],
            }))
            for index in range(3)
        ]
        for candidate in candidates:
            assert self._probe(candidate, incoming, Profiler()).height == 1

        profiler = Profiler()
        assert self._probe(candidates[0], incoming, profiler).height == 1
        assert profiler.emit_counts()["probe_rowid_integrity_cache_misses"] == 1

    def test_concurrent_cold_reservations_are_coalesced(self):
        cache = st_processing._LocalRowidIntegrityCache()
        identity = ("/immutable.parquet", 1, 2, 3, 4, 5)
        owner_ready = threading.Event()
        release_owner = threading.Event()
        results = []

        def owner():
            owned, hits = cache.reserve([identity])
            results.append((owned, hits))
            owner_ready.set()
            assert release_owner.wait(timeout=5)
            cache.finish(owned, owned)

        def waiter():
            assert owner_ready.wait(timeout=5)
            owned, hits = cache.reserve([identity])
            results.append((owned, hits))

        first = threading.Thread(target=owner)
        second = threading.Thread(target=waiter)
        first.start()
        second.start()
        assert owner_ready.wait(timeout=5)
        release_owner.set()
        first.join(timeout=5)
        second.join(timeout=5)

        assert not first.is_alive() and not second.is_alive()
        assert sum(len(owned) for owned, _hits in results) == 1
        assert sum(len(hits) for _owned, hits in results) == 1

    def test_ambiguous_local_subclass_rescans_and_matches_exact_local(self, tmp_path):
        class AmbiguousLocalStorage(LocalStorage):
            pass

        candidate = _write(tmp_path, "ambiguous.parquet", pl.DataFrame({
            "__rowid__": [1, 2], "key": [7, 8],
        }))
        incoming = pl.DataFrame({"key": [7]})
        expected = self._probe(candidate, incoming, Profiler(), LocalStorage())

        for _attempt in range(2):
            profiler = Profiler()
            actual = self._probe(
                candidate, incoming, profiler, AmbiguousLocalStorage(),
            )
            assert actual.rows() == expected.rows()
            counts = profiler.emit_counts()
            assert counts["io.duckdb_probe_rowid_integrity.n"] == 1
            assert "probe_rowid_integrity_cache_hits" not in counts
