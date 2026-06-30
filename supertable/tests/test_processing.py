"""
Comprehensive test suite for supertable/processing.py

Tests every public and private function independently using real Polars
DataFrames wherever possible and mocks for storage I/O.

 1. _resolve_unified_dtype
 2. _union_schema
 3. _align_to_schema
 4. concat_with_union
 5. _safe_exists
 6. _read_parquet_safe
 7. is_file_in_overlapping_files
 8. prune_not_overlapping_files_by_threshold
 9. find_and_lock_overlapping_files
11. write_parquet_and_collect_resources
12. filter_stale_incoming_rows
13. identify_deleted_rowids
14. identify_all_rowids
"""

from __future__ import annotations

import json
from datetime import datetime, date, timezone
from typing import Dict, List, Set, Tuple
from unittest.mock import MagicMock, patch, PropertyMock

import polars as pl
import pytest


_MOD = "supertable.processing"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _df(**cols) -> pl.DataFrame:
    return pl.DataFrame(cols)


# ===========================================================================
# 1. _resolve_unified_dtype
# ===========================================================================

class TestResolveUnifiedDtype:

    def test_empty_set_returns_utf8(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype(set()) == pl.Utf8

    def test_single_type_returned(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int64}) == pl.Int64

    def test_utf8_present_forces_utf8(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int64, pl.Utf8}) == pl.Utf8

    def test_mixed_ints_returns_int64(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int8, pl.Int32}) == pl.Int64

    def test_mixed_floats_returns_float64(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Float32, pl.Float64}) == pl.Float64

    def test_int_and_float_returns_float64(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int32, pl.Float32}) == pl.Float64

    def test_datetime_present_returns_datetime(self):
        from supertable.processing import _resolve_unified_dtype
        result = _resolve_unified_dtype({pl.Int64, pl.Datetime})
        assert result == pl.Datetime("us", None)

    def test_date_present_returns_date(self):
        from supertable.processing import _resolve_unified_dtype
        result = _resolve_unified_dtype({pl.Int64, pl.Date})
        assert result == pl.Date

    def test_unknown_types_return_utf8(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Boolean, pl.Binary}) == pl.Utf8


# ===========================================================================
# 2. _union_schema
# ===========================================================================

class TestUnionSchema:

    def test_same_columns(self):
        from supertable.processing import _union_schema
        a = _df(id=[1], val=["x"])
        b = _df(id=[2], val=["y"])
        schema = _union_schema(a, b)
        assert set(schema.keys()) == {"id", "val"}

    def test_disjoint_columns(self):
        from supertable.processing import _union_schema
        a = _df(a=[1])
        b = _df(b=["x"])
        schema = _union_schema(a, b)
        assert set(schema.keys()) == {"a", "b"}

    def test_overlapping_columns_preserve_order(self):
        from supertable.processing import _union_schema
        a = _df(x=[1], y=[2])
        b = _df(y=[3], z=[4])
        schema = _union_schema(a, b)
        assert list(schema.keys()) == ["x", "y", "z"]

    def test_type_mismatch_resolved(self):
        from supertable.processing import _union_schema
        a = _df(x=pl.Series([1], dtype=pl.Int32))
        b = _df(x=pl.Series([1.0], dtype=pl.Float32))
        schema = _union_schema(a, b)
        assert schema["x"] == pl.Float64


# ===========================================================================
# 3. _align_to_schema
# ===========================================================================

class TestAlignToSchema:

    def test_no_change_needed(self):
        from supertable.processing import _align_to_schema
        df = _df(id=[1])
        result = _align_to_schema(df, {"id": pl.Int64})
        assert result.shape == df.shape

    def test_adds_missing_column(self):
        from supertable.processing import _align_to_schema
        df = _df(a=[1])
        result = _align_to_schema(df, {"a": pl.Int64, "b": pl.Utf8})
        assert "b" in result.columns
        assert result["b"].null_count() == 1

    def test_casts_column(self):
        from supertable.processing import _align_to_schema
        df = _df(x=pl.Series([1], dtype=pl.Int32))
        result = _align_to_schema(df, {"x": pl.Int64})
        assert result["x"].dtype == pl.Int64

    def test_empty_schema_returns_same(self):
        from supertable.processing import _align_to_schema
        df = _df(a=[1])
        result = _align_to_schema(df, {})
        assert result.shape == df.shape


# ===========================================================================
# 4. concat_with_union
# ===========================================================================

class TestConcatWithUnion:

    def test_a_empty_returns_b(self):
        from supertable.processing import concat_with_union
        a = pl.DataFrame(schema={"id": pl.Int64})
        b = _df(id=[1, 2])
        result = concat_with_union(a, b)
        assert result.shape[0] == 2

    def test_b_empty_returns_a(self):
        from supertable.processing import concat_with_union
        a = _df(id=[1])
        b = pl.DataFrame(schema={"id": pl.Int64})
        result = concat_with_union(a, b)
        assert result.shape[0] == 1

    def test_same_schema(self):
        from supertable.processing import concat_with_union
        a = _df(id=[1], val=["a"])
        b = _df(id=[2], val=["b"])
        result = concat_with_union(a, b)
        assert result.shape == (2, 2)

    def test_type_coercion_during_union(self):
        """Same columns but different dtypes → unified and concatenated."""
        from supertable.processing import concat_with_union
        a = pl.DataFrame({"x": pl.Series([1], dtype=pl.Int32)})
        b = pl.DataFrame({"x": pl.Series([2.5], dtype=pl.Float64)})
        result = concat_with_union(a, b)
        assert result.shape == (2, 1)
        assert result["x"].dtype == pl.Float64


# ===========================================================================
# 5. _safe_exists
# ===========================================================================

class TestSafeExists:

    @patch(f"{_MOD}._get_storage")
    def test_returns_true(self, mock_gs):
        from supertable.processing import _safe_exists
        mock_gs.return_value.exists.return_value = True
        assert _safe_exists("/path") is True

    @patch(f"{_MOD}._get_storage")
    def test_returns_false(self, mock_gs):
        from supertable.processing import _safe_exists
        mock_gs.return_value.exists.return_value = False
        assert _safe_exists("/path") is False

    @patch(f"{_MOD}._get_storage")
    def test_exception_returns_false(self, mock_gs):
        from supertable.processing import _safe_exists
        mock_gs.return_value.exists.side_effect = ConnectionError("down")
        assert _safe_exists("/path") is False


# ===========================================================================
# 6. _read_parquet_safe
# ===========================================================================

class TestReadParquetSafe:

    @patch(f"{_MOD}._safe_exists", return_value=False)
    def test_file_not_exists_returns_none(self, mock_ex):
        from supertable.processing import _read_parquet_safe
        assert _read_parquet_safe("/missing") is None

    @patch(f"{_MOD}._get_storage")
    @patch(f"{_MOD}._safe_exists", return_value=True)
    def test_happy_path(self, mock_ex, mock_gs):
        import pyarrow as pa
        from supertable.processing import _read_parquet_safe
        tbl = pa.table({"id": [1, 2]})
        mock_gs.return_value.read_parquet.return_value = tbl
        result = _read_parquet_safe("/data.parquet")
        assert result is not None
        assert result.shape[0] == 2

    @patch(f"{_MOD}._get_storage")
    @patch(f"{_MOD}._safe_exists", return_value=True)
    def test_file_not_found_during_read(self, mock_ex, mock_gs):
        from supertable.processing import _read_parquet_safe
        mock_gs.return_value.read_parquet.side_effect = FileNotFoundError()
        assert _read_parquet_safe("/vanished") is None

    @patch(f"{_MOD}._get_storage")
    @patch(f"{_MOD}._safe_exists", return_value=True)
    def test_generic_exception_returns_none(self, mock_ex, mock_gs):
        from supertable.processing import _read_parquet_safe
        mock_gs.return_value.read_parquet.side_effect = RuntimeError("corrupt")
        assert _read_parquet_safe("/bad") is None


# ===========================================================================
# 7. is_file_in_overlapping_files
# ===========================================================================

class TestIsFileInOverlappingFiles:

    def test_found(self):
        from supertable.processing import is_file_in_overlapping_files
        files = {("a.parquet", True, 100), ("b.parquet", False, 200)}
        assert is_file_in_overlapping_files("a.parquet", files) is True

    def test_not_found(self):
        from supertable.processing import is_file_in_overlapping_files
        files = {("a.parquet", True, 100)}
        assert is_file_in_overlapping_files("z.parquet", files) is False

    def test_empty_set(self):
        from supertable.processing import is_file_in_overlapping_files
        assert is_file_in_overlapping_files("any", set()) is False


# ===========================================================================
# 8. prune_not_overlapping_files_by_threshold
# ===========================================================================

class TestPruneNotOverlappingFilesByThreshold:

    def test_always_keeps_true_items(self):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        files = {("a.parquet", True, 100), ("b.parquet", False, 50)}
        result = prune_not_overlapping_files_by_threshold(files)
        assert ("a.parquet", True, 100) in result

    @patch(f"{_MOD}.default")
    def test_false_items_excluded_below_threshold(self, mock_default):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        mock_default.MAX_MEMORY_CHUNK_SIZE = 1_000_000
        mock_default.MAX_OVERLAPPING_FILES = 100
        files = {("a.parquet", True, 100), ("b.parquet", False, 50)}
        result = prune_not_overlapping_files_by_threshold(files)
        # total_size=150 < 1M and total_false=1 < 100 → false excluded
        assert ("b.parquet", False, 50) not in result

    @patch(f"{_MOD}.default")
    def test_false_items_included_when_size_exceeds(self, mock_default):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        mock_default.MAX_MEMORY_CHUNK_SIZE = 100
        mock_default.MAX_OVERLAPPING_FILES = 1000
        files = {("a.parquet", True, 80), ("b.parquet", False, 50)}
        # total_size=130 > 100 → include false items
        result = prune_not_overlapping_files_by_threshold(files)
        assert ("b.parquet", False, 50) in result

    @patch(f"{_MOD}.default")
    def test_false_items_included_when_count_exceeds(self, mock_default):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        mock_default.MAX_MEMORY_CHUNK_SIZE = 10_000_000
        mock_default.MAX_OVERLAPPING_FILES = 2
        files = {
            ("a.parquet", False, 10),
            ("b.parquet", False, 10),
            ("c.parquet", True, 10),
        }
        # total_false=2 >= 2 → gate opens
        result = prune_not_overlapping_files_by_threshold(files)
        assert ("a.parquet", False, 10) in result
        assert ("b.parquet", False, 10) in result

    def test_empty_input(self):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        assert prune_not_overlapping_files_by_threshold(set()) == set()


# ===========================================================================
# 10. write_parquet_and_collect_resources
# ===========================================================================

class TestWriteParquetAndCollectResources:

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_appends_resource(self, mock_gs, mock_gen):
        from supertable.processing import write_parquet_and_collect_resources
        mock_stor = MagicMock()
        mock_stor.exists.return_value = True
        mock_stor.size.return_value = 1234
        mock_gs.return_value = mock_stor

        df = _df(id=[1, 2], val=["a", "b"])
        resources: list = []

        write_parquet_and_collect_resources(
            write_df=df,
            overwrite_columns=["id"],
            data_dir="/data",
            new_resources=resources,
            compression_level=10,
        )

        assert len(resources) == 1
        res = resources[0]
        assert res["rows"] == 2
        assert res["columns"] == 2
        assert res["file_size"] == 1234
        assert "data.parquet" in res["file"]
        assert "stats" not in res

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_creates_dir_if_missing(self, mock_gs, mock_gen):
        from supertable.processing import write_parquet_and_collect_resources
        mock_stor = MagicMock()
        mock_stor.exists.return_value = False
        mock_stor.size.return_value = 100
        mock_gs.return_value = mock_stor

        write_parquet_and_collect_resources(
            write_df=_df(x=[1]),
            overwrite_columns=[],
            data_dir="/data",
            new_resources=[],
        )

        mock_stor.makedirs.assert_called_once_with("/data")

    @patch(f"{_MOD}.generate_filename", return_value="data.parquet")
    @patch(f"{_MOD}._get_storage")
    def test_timestamp_rows_spanning_days_write_one_current_time_bucket(
        self, mock_gs, mock_gen
    ):
        """A frame whose ``__timestamp__`` rows span many days must become ONE
        file under a single Hive bucket derived from the CURRENT write time —
        not one tiny file per distinct row-day.

        This is the anti-fragmentation contract: per-row bucketing shredded a
        memory-bounded compaction chunk into one file per day, so merging small
        files never actually consolidated and the small-file gate stayed tripped.
        """
        from supertable.processing import write_parquet_and_collect_resources

        mock_stor = MagicMock()
        mock_stor.size.return_value = 4096
        mock_gs.return_value = mock_stor

        # Rows deliberately span three different calendar years.
        df = _df(
            __timestamp__=[
                datetime(2020, 1, 1, tzinfo=timezone.utc),
                datetime(2021, 6, 15, tzinfo=timezone.utc),
                datetime(2022, 12, 31, tzinfo=timezone.utc),
            ],
            id=[1, 2, 3],
        )
        resources: list = []

        fixed = datetime(2026, 6, 30, 12, 0, 0, tzinfo=timezone.utc)
        with patch(f"{_MOD}.datetime") as mock_dt:
            mock_dt.now.return_value = fixed
            write_parquet_and_collect_resources(
                write_df=df,
                overwrite_columns=["id"],
                data_dir="/data",
                new_resources=resources,
            )

        # ONE file, not three (one per row-day) -> consolidation is possible.
        assert len(resources) == 1
        path = resources[0]["file"]
        # Bucket reflects the current write time, NOT any row's __timestamp__.
        assert "year=2026" in path
        assert "month=06" in path
        assert "day=30" in path
        for foreign in ("year=2020", "year=2021", "year=2022"):
            assert foreign not in path, f"row-day {foreign} leaked into shard path {path}"

    @patch(f"{_MOD}._get_storage")
    def test_timestamp_preserved_in_body_no_partition_column_leak(
        self, mock_gs, tmp_path
    ):
        """Real round-trip: the shard folder is path-only.  ``__timestamp__``
        stays in the file body (it is the dedup ORDER BY key) and the Hive
        ``year/month/day`` keys are NEVER materialized as body columns."""
        from supertable.processing import write_parquet_and_collect_resources
        from supertable.storage.local_storage import LocalStorage

        mock_gs.return_value = LocalStorage()

        df = _df(
            __timestamp__=[
                datetime(2020, 1, 1, tzinfo=timezone.utc),
                datetime(2022, 12, 31, tzinfo=timezone.utc),
            ],
            id=[1, 2],
            val=["a", "b"],
        )
        resources: list = []
        write_parquet_and_collect_resources(
            write_df=df,
            overwrite_columns=["id"],
            data_dir=str(tmp_path),
            new_resources=resources,
        )

        assert len(resources) == 1
        written = pl.read_parquet(resources[0]["file"])
        # Body keeps __timestamp__ and the user columns ...
        assert "__timestamp__" in written.columns
        assert set(written.columns) == {"__timestamp__", "id", "val"}
        # ... but never the Hive partition keys.
        for leaked in ("year", "month", "day"):
            assert leaked not in written.columns
        # All rows survived as a single file.
        assert written.height == 2


# ===========================================================================
# 12. filter_stale_incoming_rows
# ===========================================================================

class TestFilterStaleIncomingRows:

    @patch(f"{_MOD}._read_parquet_safe")
    def test_newer_rows_kept(self, mock_read):
        from supertable.processing import filter_stale_incoming_rows

        mock_read.return_value = _df(id=[1], ts=[100])
        incoming = _df(id=[1], ts=[200])

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="ts",
        )

        assert result.shape[0] == 1

    @patch(f"{_MOD}._read_parquet_safe")
    def test_stale_rows_dropped(self, mock_read):
        from supertable.processing import filter_stale_incoming_rows

        mock_read.return_value = _df(id=[1], ts=[200])
        incoming = _df(id=[1], ts=[100])

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="ts",
        )

        assert result.shape[0] == 0

    @patch(f"{_MOD}._read_parquet_safe")
    def test_equal_rows_dropped(self, mock_read):
        from supertable.processing import filter_stale_incoming_rows

        mock_read.return_value = _df(id=[1], ts=[100])
        incoming = _df(id=[1], ts=[100])

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="ts",
        )

        assert result.shape[0] == 0

    @patch(f"{_MOD}._read_parquet_safe")
    def test_new_keys_kept(self, mock_read):
        from supertable.processing import filter_stale_incoming_rows

        mock_read.return_value = _df(id=[1], ts=[100])
        incoming = _df(id=[2], ts=[50])  # new key, regardless of ts

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="ts",
        )

        assert result.shape[0] == 1

    def test_no_overlap_files_returns_all(self):
        from supertable.processing import filter_stale_incoming_rows

        incoming = _df(id=[1], ts=[100])

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", False, 50)},  # only False entries
            overwrite_columns=["id"],
            newer_than_col="ts",
        )

        assert result.shape[0] == 1

    def test_empty_overwrite_columns_returns_all(self):
        from supertable.processing import filter_stale_incoming_rows

        incoming = _df(id=[1], ts=[100])

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=[],
            newer_than_col="ts",
        )

        assert result.shape[0] == 1

    def test_empty_newer_than_col_returns_all(self):
        from supertable.processing import filter_stale_incoming_rows

        incoming = _df(id=[1], ts=[100])

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="",
        )

        assert result.shape[0] == 1

    @patch(f"{_MOD}._read_parquet_safe")
    def test_missing_newer_than_col_in_file_keeps_rows(self, mock_read):
        from supertable.processing import filter_stale_incoming_rows

        # Existing file doesn't have 'ts' column
        mock_read.return_value = _df(id=[1], val=["x"])
        incoming = _df(id=[1], ts=[100])

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="ts",
        )

        assert result.shape[0] == 1

    @patch(f"{_MOD}._read_parquet_safe")
    def test_file_cache_populated(self, mock_read):
        from supertable.processing import filter_stale_incoming_rows

        mock_read.return_value = _df(id=[1], ts=[100])
        cache: dict = {}

        filter_stale_incoming_rows(
            incoming_df=_df(id=[1], ts=[200]),
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="ts",
            file_cache=cache,
        )

        assert "f1.parquet" in cache
        assert cache["f1.parquet"].shape[0] == 1

    @patch(f"{_MOD}._read_parquet_safe")
    def test_mixed_newer_and_stale(self, mock_read):
        from supertable.processing import filter_stale_incoming_rows

        mock_read.return_value = _df(id=[1, 2], ts=[100, 200])
        incoming = _df(id=[1, 2], ts=[150, 150])
        # id=1: incoming 150 > existing 100 → keep
        # id=2: incoming 150 < existing 200 → drop

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            newer_than_col="ts",
        )

        assert result.shape[0] == 1
        assert result["id"].to_list() == [1]


# ===========================================================================
# 15. find_and_lock_overlapping_files
# ===========================================================================

class TestFindAndLockOverlappingFiles:

    def test_no_overwrite_columns_compaction_only(self):
        from supertable.processing import find_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100},
                {"file": "f2.parquet", "file_size": 200},
            ]
        }
        df = _df(id=[1])

        result = find_overlapping_files(snap, df, [], MagicMock())

        # All small files with has_overlap=False (subject to pruning)
        for f, has_overlap, _ in result:
            assert has_overlap is False

    def test_overwrite_column_marks_every_file_overlapping(self):
        """Without per-file stats, every existing file is a delete/overwrite
        candidate (has_overlap=True) so no matching rowid is missed."""
        from supertable.processing import find_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100},
                {"file": "f2.parquet", "file_size": 200},
            ]
        }
        df = _df(id=[1])

        result = find_overlapping_files(snap, df, ["id"], MagicMock())
        matches = {f for f, overlap, _ in result if overlap}
        assert matches == {"f1.parquet", "f2.parquet"}

    def test_stale_stats_in_snapshot_are_ignored(self):
        """Legacy snapshots may still carry a 'stats' block; it must no longer
        prune files — the file is still treated as overlapping."""
        from supertable.processing import find_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100,
                 "stats": {"id": {"min": 10, "max": 20}}},  # incoming id=1 is outside
            ]
        }
        df = _df(id=[1])

        result = find_overlapping_files(snap, df, ["id"], MagicMock())
        matches = [f for f, overlap, _ in result if overlap]
        assert "f1.parquet" in matches

    def test_empty_resources(self):
        from supertable.processing import find_overlapping_files
        result = find_overlapping_files({"resources": []}, _df(id=[1]), ["id"], MagicMock())
        assert result == set()


# ===========================================================================
# 13. identify_deleted_rowids  (deletion-vector: which existing rows to tombstone)
# ===========================================================================

class TestIdentifyDeletedRowids:
    """Maps a delete/overwrite predicate to the (file, __rowid__) pairs of the
    existing rows it matches.  Real DataFrames are fed through ``file_cache`` so
    no parquet I/O is mocked."""

    def test_no_overwrite_columns_returns_empty(self):
        from supertable.processing import identify_deleted_rowids
        df = _df(id=[1])
        result = identify_deleted_rowids(df, {("f.parquet", True, 100)}, [])
        assert result == []

    def test_append_multiple_same_key_then_delete_tombstones_all(self):
        """The case the user named: three rows appended with the SAME key all
        get tombstoned when a delete/overwrite hits that key."""
        from supertable.processing import identify_deleted_rowids
        existing = _df(id=[1, 1, 1, 2], __rowid__=[10, 20, 30, 40])
        df = _df(id=[1])  # delete/overwrite predicate: key == 1
        result = identify_deleted_rowids(
            df, {("f.parquet", True, 100)}, ["id"],
            file_cache={"f.parquet": existing},
        )
        assert sorted(result) == [("f.parquet", 10), ("f.parquet", 20), ("f.parquet", 30)]

    def test_null_key_matches_null_null_safe(self):
        """nulls_equal=True: an incoming NULL key tombstones an existing NULL
        key (unlike SQL's NULL != NULL)."""
        from supertable.processing import identify_deleted_rowids
        existing = _df(id=[1, None, 2], __rowid__=[10, 20, 30])
        df = pl.DataFrame({"id": [None]}, schema={"id": pl.Int64})
        result = identify_deleted_rowids(
            df, {("f.parquet", True, 100)}, ["id"],
            file_cache={"f.parquet": existing},
        )
        assert result == [("f.parquet", 20)]

    def test_multi_column_key_with_null_component(self):
        """Composite key, one component NULL on both sides → matches null-safely."""
        from supertable.processing import identify_deleted_rowids
        existing = pl.DataFrame(
            {"a": [1, 1, 2], "b": [None, "x", "y"], "__rowid__": [10, 20, 30]},
            schema={"a": pl.Int64, "b": pl.Utf8, "__rowid__": pl.Int64},
        )
        df = pl.DataFrame({"a": [1], "b": [None]}, schema={"a": pl.Int64, "b": pl.Utf8})
        result = identify_deleted_rowids(
            df, {("f.parquet", True, 100)}, ["a", "b"],
            file_cache={"f.parquet": existing},
        )
        assert result == [("f.parquet", 10)]

    def test_no_match_returns_empty(self):
        from supertable.processing import identify_deleted_rowids
        existing = _df(id=[1, 2], __rowid__=[10, 20])
        df = _df(id=[99])
        result = identify_deleted_rowids(
            df, {("f.parquet", True, 100)}, ["id"],
            file_cache={"f.parquet": existing},
        )
        assert result == []

    def test_file_without_rowid_skipped(self):
        """Legacy data lacking __rowid__ cannot be tombstoned by id → skipped."""
        from supertable.processing import identify_deleted_rowids
        existing = _df(id=[1, 2])  # no __rowid__ column
        df = _df(id=[1])
        result = identify_deleted_rowids(
            df, {("f.parquet", True, 100)}, ["id"],
            file_cache={"f.parquet": existing},
        )
        assert result == []

    def test_non_overlapping_file_skipped(self):
        from supertable.processing import identify_deleted_rowids
        existing = _df(id=[1], __rowid__=[10])
        df = _df(id=[1])
        result = identify_deleted_rowids(
            df, {("f.parquet", False, 100)}, ["id"],
            file_cache={"f.parquet": existing},
        )
        assert result == []

    def test_predicate_column_absent_from_incoming_returns_empty(self):
        from supertable.processing import identify_deleted_rowids
        existing = _df(id=[1], __rowid__=[10])
        df = _df(other=[1])  # no 'id' column in incoming df
        result = identify_deleted_rowids(
            df, {("f.parquet", True, 100)}, ["id"],
            file_cache={"f.parquet": existing},
        )
        assert result == []

    def test_matches_across_multiple_files(self):
        from supertable.processing import identify_deleted_rowids
        f1 = _df(id=[1, 2], __rowid__=[10, 20])
        f2 = _df(id=[1, 3], __rowid__=[30, 40])
        df = _df(id=[1])
        result = identify_deleted_rowids(
            df,
            {("f1.parquet", True, 100), ("f2.parquet", True, 100)},
            ["id"],
            file_cache={"f1.parquet": f1, "f2.parquet": f2},
        )
        assert sorted(result) == [("f1.parquet", 10), ("f2.parquet", 30)]


# ===========================================================================
# 14. identify_all_rowids  (delete-all: tombstone every live row)
# ===========================================================================

class TestIdentifyAllRowids:

    def test_collects_every_rowid_across_resources(self):
        from supertable.processing import identify_all_rowids
        f1 = _df(id=[1, 2], __rowid__=[10, 20])
        f2 = _df(id=[3], __rowid__=[30])
        resources = [
            {"file": "f1.parquet", "file_size": 100},
            {"file": "f2.parquet", "file_size": 100},
        ]
        result = identify_all_rowids(
            resources, file_cache={"f1.parquet": f1, "f2.parquet": f2}
        )
        assert sorted(result) == [
            ("f1.parquet", 10), ("f1.parquet", 20), ("f2.parquet", 30),
        ]

    def test_empty_resources_returns_empty(self):
        from supertable.processing import identify_all_rowids
        assert identify_all_rowids([]) == []
        assert identify_all_rowids(None) == []

    def test_file_without_rowid_skipped(self):
        from supertable.processing import identify_all_rowids
        f1 = _df(id=[1])  # no __rowid__
        resources = [{"file": "f1.parquet", "file_size": 100}]
        result = identify_all_rowids(resources, file_cache={"f1.parquet": f1})
        assert result == []

    def test_resource_without_file_skipped(self):
        from supertable.processing import identify_all_rowids
        f1 = _df(id=[1], __rowid__=[10])
        resources = [{"file_size": 100}, {"file": "f1.parquet", "file_size": 100}]
        result = identify_all_rowids(resources, file_cache={"f1.parquet": f1})
        assert result == [("f1.parquet", 10)]

    def test_non_dict_resource_skipped(self):
        from supertable.processing import identify_all_rowids
        f1 = _df(id=[1], __rowid__=[10])
        resources = ["not-a-dict", {"file": "f1.parquet", "file_size": 100}]
        result = identify_all_rowids(resources, file_cache={"f1.parquet": f1})
        assert result == [("f1.parquet", 10)]