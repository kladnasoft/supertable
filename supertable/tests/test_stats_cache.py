"""In-process cache for the external stats artifact.

The stats parquet is read on every overwrite/delete (write-path pruning) and on
every filtered read (query estimation).  ``load_stats`` keeps the *latest*
version of each table's stats in memory so those hot paths avoid a storage
round-trip, while time-travel reads of older versions stay uncached and never
evict the cached latest.

Contract under test:
  * a cache hit returns the exact same frame object without touching storage;
  * a versioned (new) path misses and reads fresh;
  * ``allow_cache=False`` reads fresh and does NOT evict the cached latest;
  * ``cache_stats`` seeds the cache so the next ``load_stats`` is a pure hit;
  * the cache is bounded (LRU) and disabled when the cap is 0;
  * keys are per-table (per stats directory), so tables don't evict each other.
"""

from __future__ import annotations

from unittest.mock import patch

import polars as pl
import pytest

from supertable import processing
from supertable.processing import (
    load_stats,
    cache_stats,
    build_stats_file,
    STATS_SCHEMA,
    _STATS_CACHE,
)

_MOD = "supertable.processing"


def _df(n: int = 1) -> pl.DataFrame:
    base = {k: [None] for k in STATS_SCHEMA}
    base["file_path"] = [f"f{n}.parquet"]
    base["column_name"] = ["a"]
    base["row_group_id"] = [0]
    base["min_bigint"] = [n]
    base["max_bigint"] = [n]
    base["stats_available"] = [True]
    return pl.DataFrame(base, schema=STATS_SCHEMA)


def _full_stats_df(file_path: str, value: int) -> pl.DataFrame:
    """A stats frame with every STATS_SCHEMA column populated (build-ready)."""
    base = {k: [None] for k in STATS_SCHEMA}
    base["file_path"] = [file_path]
    base["row_group_id"] = [0]
    base["column_name"] = ["a"]
    base["physical_type"] = ["INT64"]
    base["logical_type"] = [""]
    base["min_bigint"] = [value]
    base["max_bigint"] = [value]
    base["null_count"] = [0]
    base["row_group_rows"] = [1]
    base["stats_available"] = [True]
    base["min_is_exact"] = [True]
    base["max_is_exact"] = [True]
    return pl.DataFrame(base, schema=STATS_SCHEMA)


class _FakeStorage:
    """Minimal in-memory backend for build_stats_file's write of the new file."""

    def __init__(self):
        self.store: dict = {}

    def exists(self, path: str) -> bool:
        return path in self.store

    def makedirs(self, path: str) -> None:
        pass

    def write_bytes(self, path: str, data: bytes) -> None:
        self.store[path] = data

    def size(self, path: str) -> int:
        return len(self.store[path])


@pytest.fixture(autouse=True)
def _clear_cache():
    _STATS_CACHE.clear()
    yield
    _STATS_CACHE.clear()


class TestLoadStats:

    def test_none_path_returns_none_without_read(self):
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            assert load_stats(None) is None
            mock_read.assert_not_called()

    def test_first_read_misses_then_caches(self):
        df = _df(1)
        with patch(f"{_MOD}._read_parquet_safe", return_value=df) as mock_read:
            out1 = load_stats("t/stats/v1.parquet")
            out2 = load_stats("t/stats/v1.parquet")
        assert out1 is df and out2 is df
        # Second call served from cache → storage read happened exactly once.
        assert mock_read.call_count == 1

    def test_cache_hit_returns_same_object(self):
        df = _df(1)
        cache_stats("t/stats/v1.parquet", df)
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            out = load_stats("t/stats/v1.parquet")
            mock_read.assert_not_called()
        assert out is df

    def test_new_version_misses_and_replaces_latest(self):
        v1, v2 = _df(1), _df(2)
        cache_stats("t/stats/v1.parquet", v1)
        with patch(f"{_MOD}._read_parquet_safe", return_value=v2) as mock_read:
            out = load_stats("t/stats/v2.parquet")  # newer version → miss
            assert mock_read.call_count == 1
        assert out is v2
        # v2 is now the cached latest; v1's path no longer hits.
        with patch(f"{_MOD}._read_parquet_safe", return_value=v1) as mock_read:
            again = load_stats("t/stats/v1.parquet")
            assert mock_read.call_count == 1  # had to re-read the old version
        assert again is v1

    def test_allow_cache_false_does_not_evict_latest(self):
        latest, old = _df(2), _df(1)
        cache_stats("t/stats/v2.parquet", latest)
        with patch(f"{_MOD}._read_parquet_safe", return_value=old) as mock_read:
            # Time-travel read of an older version.
            out = load_stats("t/stats/v1.parquet", allow_cache=False)
            assert out is old and mock_read.call_count == 1
        # Latest is still cached → no read.
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            assert load_stats("t/stats/v2.parquet") is latest
            mock_read.assert_not_called()
        # And the historical version was NOT cached → reads again.
        with patch(f"{_MOD}._read_parquet_safe", return_value=old) as mock_read:
            load_stats("t/stats/v1.parquet", allow_cache=False)
            assert mock_read.call_count == 1

    def test_failed_read_not_cached(self):
        with patch(f"{_MOD}._read_parquet_safe", return_value=None) as mock_read:
            assert load_stats("t/stats/v1.parquet") is None
            assert load_stats("t/stats/v1.parquet") is None
            assert mock_read.call_count == 2  # None never cached → re-read

    def test_per_table_keys_do_not_collide(self):
        a, b = _df(1), _df(2)
        cache_stats("tableA/stats/v1.parquet", a)
        cache_stats("tableB/stats/v1.parquet", b)
        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            assert load_stats("tableA/stats/v1.parquet") is a
            assert load_stats("tableB/stats/v1.parquet") is b
            mock_read.assert_not_called()

    def test_profiler_counts_hit_and_miss(self):
        from supertable.utils.profiler import Profiler
        df = _df(1)
        prof = Profiler()
        with patch(f"{_MOD}._read_parquet_safe", return_value=df):
            load_stats("t/stats/v1.parquet", profiler=prof)  # miss
            load_stats("t/stats/v1.parquet", profiler=prof)  # hit
        counts = prof.emit_counts()
        assert counts.get("stats_cache_miss") == 1
        assert counts.get("stats_cache_hit") == 1


class TestCacheBounds:

    def test_lru_eviction_caps_table_count(self):
        with patch(f"{_MOD}.settings") as mock_settings:
            mock_settings.SUPERTABLE_STATS_CACHE_MAX_TABLES = 2
            cache_stats("t1/stats/v.parquet", _df(1))
            cache_stats("t2/stats/v.parquet", _df(2))
            cache_stats("t3/stats/v.parquet", _df(3))  # evicts t1 (LRU)
            with patch(f"{_MOD}._read_parquet_safe", return_value=_df(9)) as mock_read:
                # t1 was evicted → miss/read; t2 and t3 still cached → hits.
                load_stats("t1/stats/v.parquet")
                assert mock_read.call_count == 1
            with patch(f"{_MOD}._read_parquet_safe") as mock_read:
                load_stats("t3/stats/v.parquet")
                mock_read.assert_not_called()

    def test_zero_cap_disables_cache(self):
        with patch(f"{_MOD}.settings") as mock_settings:
            mock_settings.SUPERTABLE_STATS_CACHE_MAX_TABLES = 0
            cache_stats("t/stats/v1.parquet", _df(1))  # no-op
            with patch(f"{_MOD}._read_parquet_safe", return_value=_df(1)) as mock_read:
                load_stats("t/stats/v1.parquet")
                load_stats("t/stats/v1.parquet")
                assert mock_read.call_count == 2  # never cached


class TestBuildStatsFileUsesCache:
    """Change 1: build_stats_file reads the previous stats through the cache
    (load_stats), so a process writing in a loop needs no storage GET for the
    carry-forward once the cache is warm — seeded by the prune phase of the same
    write or by the prior write's cache_stats."""

    @patch(f"{_MOD}._get_storage")
    def test_warm_cache_prev_read_is_a_hit_no_storage_read(self, mock_gs):
        mock_gs.return_value = _FakeStorage()
        prev_path = "t/stats/prev.parquet"
        # Warm the cache exactly as the write path does (prune phase / prior write).
        cache_stats(prev_path, _full_stats_df("fileA.parquet", 1))

        with patch(f"{_MOD}._read_parquet_safe") as mock_read:
            path, combined = build_stats_file(
                stats_dir="t/stats",
                prev_stats_path=prev_path,
                new_rows=_full_stats_df("fileB.parquet", 2),
                removed_files=set(),
                compression_level=1,
            )
            # The previous stats came from memory → zero storage reads.
            mock_read.assert_not_called()

        assert path != prev_path  # a NEW versioned artifact was written
        assert set(combined["file_path"].to_list()) == {"fileA.parquet", "fileB.parquet"}

    @patch(f"{_MOD}._get_storage")
    def test_cold_cache_reads_prev_once(self, mock_gs):
        # Negative control: with an EMPTY cache the same build DOES read prev
        # exactly once — proving the warm-cache assertion above is not vacuous.
        mock_gs.return_value = _FakeStorage()
        prev_path = "t/stats/prev.parquet"
        with patch(
            f"{_MOD}._read_parquet_safe",
            return_value=_full_stats_df("fileA.parquet", 1),
        ) as mock_read:
            build_stats_file(
                stats_dir="t/stats",
                prev_stats_path=prev_path,
                new_rows=_full_stats_df("fileB.parquet", 2),
                removed_files=set(),
                compression_level=1,
            )
            assert mock_read.call_count == 1  # cold cache → exactly one GET
