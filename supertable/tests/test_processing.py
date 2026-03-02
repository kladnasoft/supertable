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
10. collect_column_statistics
11. write_parquet_and_collect_resources
12. process_files_without_overlap
13. process_files_with_overlap
14. process_overlapping_files
15. process_delete_only
16. filter_stale_incoming_rows
"""

from __future__ import annotations

import json
from datetime import datetime, date
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
# 9. collect_column_statistics
# ===========================================================================

class TestCollectColumnStatistics:

    def test_basic_int_stats(self):
        from supertable.processing import collect_column_statistics
        df = _df(id=[1, 5, 3])
        stats = collect_column_statistics(df, ["id"])
        assert stats["id"]["min"] == 1
        assert stats["id"]["max"] == 5

    def test_string_stats(self):
        from supertable.processing import collect_column_statistics
        df = _df(name=["alice", "bob", "charlie"])
        stats = collect_column_statistics(df, [])
        assert stats["name"]["min"] == "alice"
        assert stats["name"]["max"] == "charlie"

    def test_date_stats_iso_format(self):
        from supertable.processing import collect_column_statistics
        df = pl.DataFrame({
            "d": [date(2024, 1, 1), date(2024, 12, 31)],
        })
        stats = collect_column_statistics(df, [])
        assert stats["d"]["min"] == "2024-01-01"
        assert stats["d"]["max"] == "2024-12-31"

    def test_datetime_stats_iso_format(self):
        from supertable.processing import collect_column_statistics
        df = pl.DataFrame({
            "ts": [datetime(2024, 1, 1, 10, 0), datetime(2024, 6, 15, 20, 30)],
        })
        stats = collect_column_statistics(df, [])
        assert "2024-01-01" in stats["ts"]["min"]
        assert "2024-06-15" in stats["ts"]["max"]

    def test_empty_df_returns_empty(self):
        from supertable.processing import collect_column_statistics
        df = pl.DataFrame(schema={"id": pl.Int64})
        stats = collect_column_statistics(df, [])
        assert stats == {}

    def test_all_columns_included(self):
        from supertable.processing import collect_column_statistics
        df = _df(a=[1], b=["x"], c=[1.5])
        stats = collect_column_statistics(df, ["a"])
        assert set(stats.keys()) == {"a", "b", "c"}

    def test_single_row(self):
        from supertable.processing import collect_column_statistics
        df = _df(x=[42])
        stats = collect_column_statistics(df, [])
        assert stats["x"]["min"] == 42
        assert stats["x"]["max"] == 42


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
        assert "stats" in res

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


# ===========================================================================
# 11. process_files_without_overlap
# ===========================================================================

class TestProcessFilesWithoutOverlap:

    @patch(f"{_MOD}._read_parquet_safe")
    @patch(f"{_MOD}.write_parquet_and_collect_resources")
    def test_compacts_false_files(self, mock_write, mock_read):
        from supertable.processing import process_files_without_overlap
        mock_read.return_value = _df(id=[1])

        overlapping = {("f1.parquet", False, 10), ("f2.parquet", True, 100)}
        sunset: set = set()
        empty = pl.DataFrame(schema={"id": pl.Int64})

        result = process_files_without_overlap(
            empty_df=empty,
            data_dir="/data",
            new_resources=[],
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            sunset_files=sunset,
            compression_level=10,
        )

        # Only False file processed
        assert "f1.parquet" in sunset
        assert "f2.parquet" not in sunset
        assert result.shape[0] == 1

    @patch(f"{_MOD}._read_parquet_safe")
    def test_skips_none_reads(self, mock_read):
        from supertable.processing import process_files_without_overlap
        mock_read.return_value = None

        overlapping = {("f1.parquet", False, 10)}
        sunset: set = set()
        empty = pl.DataFrame(schema={"id": pl.Int64})

        result = process_files_without_overlap(
            empty_df=empty,
            data_dir="/data",
            new_resources=[],
            overlapping_files=overlapping,
            overwrite_columns=[],
            sunset_files=sunset,
            compression_level=10,
        )

        assert result.shape[0] == 0
        assert len(sunset) == 0

    @patch(f"{_MOD}._read_parquet_safe")
    @patch(f"{_MOD}.write_parquet_and_collect_resources")
    @patch(f"{_MOD}.default")
    def test_flushes_on_memory_limit(self, mock_default, mock_write, mock_read):
        from supertable.processing import process_files_without_overlap
        mock_default.MAX_MEMORY_CHUNK_SIZE = 50  # tiny limit
        mock_read.return_value = _df(id=[1])

        overlapping = {
            ("f1.parquet", False, 30),
            ("f2.parquet", False, 30),
        }
        sunset: set = set()
        empty = pl.DataFrame(schema={"id": pl.Int64})

        process_files_without_overlap(
            empty_df=empty,
            data_dir="/data",
            new_resources=[],
            overlapping_files=overlapping,
            overwrite_columns=[],
            sunset_files=sunset,
            compression_level=10,
        )

        # At least one flush should have occurred
        assert mock_write.call_count >= 1


# ===========================================================================
# 12. process_files_with_overlap
# ===========================================================================

class TestProcessFilesWithOverlap:

    @patch(f"{_MOD}._read_parquet_safe")
    def test_anti_join_deletes_matching_rows(self, mock_read):
        from supertable.processing import process_files_with_overlap

        # Existing file has rows with id=1,2,3
        mock_read.return_value = _df(id=[1, 2, 3], val=["a", "b", "c"])

        incoming = _df(id=[2], val=["updated"])
        merged = incoming.clone()
        empty = pl.DataFrame(schema=incoming.schema)

        deleted, result_df, total_rows, skipped = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=empty,
            merged_df=merged,
            new_resources=[],
            overlapping_files={("f1.parquet", True, 100)},
            overwrite_columns=["id"],
            sunset_files=set(),
            total_rows=0,
            compression_level=10,
        )

        assert deleted == 1  # row id=2 deleted
        # Merged should contain incoming + kept rows (id=1,3)
        assert result_df.shape[0] == 3  # 1 incoming + 2 kept

    @patch(f"{_MOD}._read_parquet_safe")
    def test_no_matches_skips_file(self, mock_read):
        from supertable.processing import process_files_with_overlap

        mock_read.return_value = _df(id=[10, 20], val=["x", "y"])
        incoming = _df(id=[1], val=["new"])
        merged = incoming.clone()
        empty = pl.DataFrame(schema=incoming.schema)

        deleted, result_df, total_rows, skipped = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=empty,
            merged_df=merged,
            new_resources=[],
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            sunset_files=set(),
            total_rows=0,
            compression_level=10,
        )

        assert deleted == 0
        assert len(skipped) == 1
        assert skipped[0][0] == "f1.parquet"

    @patch(f"{_MOD}._read_parquet_safe")
    def test_uses_file_cache(self, mock_read):
        from supertable.processing import process_files_with_overlap

        cached_df = _df(id=[5], val=["cached"])
        cache = {"f1.parquet": cached_df}

        incoming = _df(id=[5], val=["new"])
        merged = incoming.clone()
        empty = pl.DataFrame(schema=incoming.schema)

        deleted, _, _, _ = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=empty,
            merged_df=merged,
            new_resources=[],
            overlapping_files={("f1.parquet", True, 50)},
            overwrite_columns=["id"],
            sunset_files=set(),
            total_rows=0,
            compression_level=10,
            file_cache=cache,
        )

        # Should have used cache, not called _read_parquet_safe
        mock_read.assert_not_called()
        assert deleted == 1
        # Cache entry consumed
        assert "f1.parquet" not in cache


# ===========================================================================
# 13. process_delete_only
# ===========================================================================

class TestProcessDeleteOnly:

    @patch(f"{_MOD}._read_parquet_safe")
    @patch(f"{_MOD}.write_parquet_and_collect_resources")
    def test_deletes_matching_rows(self, mock_write, mock_read):
        from supertable.processing import process_delete_only

        mock_read.return_value = _df(id=[1, 2, 3], val=["a", "b", "c"])

        delete_df = _df(id=[2, 3])
        inserted, deleted, total_rows, total_cols, new_res, sunset = process_delete_only(
            df=delete_df,
            overlapping_files={("f1.parquet", True, 100)},
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=10,
        )

        assert inserted == 0
        assert deleted == 2
        assert "f1.parquet" in sunset
        mock_write.assert_called_once()
        # Kept row is id=1
        written_df = mock_write.call_args[1]["write_df"]
        assert written_df.shape[0] == 1

    @patch(f"{_MOD}._read_parquet_safe")
    @patch(f"{_MOD}.write_parquet_and_collect_resources")
    def test_all_rows_deleted_no_write(self, mock_write, mock_read):
        from supertable.processing import process_delete_only

        mock_read.return_value = _df(id=[1], val=["a"])

        inserted, deleted, total_rows, total_cols, new_res, sunset = process_delete_only(
            df=_df(id=[1]),
            overlapping_files={("f1.parquet", True, 100)},
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=10,
        )

        assert deleted == 1
        assert "f1.parquet" in sunset
        mock_write.assert_not_called()  # no rows remain
        assert total_rows == 0

    @patch(f"{_MOD}._read_parquet_safe")
    def test_no_matches_leaves_file_untouched(self, mock_read):
        from supertable.processing import process_delete_only

        mock_read.return_value = _df(id=[10, 20], val=["x", "y"])

        inserted, deleted, total_rows, total_cols, new_res, sunset = process_delete_only(
            df=_df(id=[1]),
            overlapping_files={("f1.parquet", True, 100)},
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=10,
        )

        assert deleted == 0
        assert len(sunset) == 0

    @patch(f"{_MOD}._read_parquet_safe")
    def test_skips_false_files(self, mock_read):
        from supertable.processing import process_delete_only

        mock_read.return_value = _df(id=[1])

        inserted, deleted, total_rows, total_cols, new_res, sunset = process_delete_only(
            df=_df(id=[1]),
            overlapping_files={("f1.parquet", False, 100)},
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=10,
        )

        assert deleted == 0
        mock_read.assert_not_called()

    @patch(f"{_MOD}._read_parquet_safe")
    def test_total_columns_fallback_to_df(self, mock_read):
        from supertable.processing import process_delete_only

        mock_read.return_value = _df(id=[10])  # no match

        _, _, _, total_cols, _, _ = process_delete_only(
            df=_df(id=[1], val=["a"], extra=[True]),
            overlapping_files={("f1.parquet", True, 100)},
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=10,
        )

        # No files touched → fallback to df.shape[1]
        assert total_cols == 3

    @patch(f"{_MOD}._read_parquet_safe")
    @patch(f"{_MOD}.write_parquet_and_collect_resources")
    def test_uses_file_cache(self, mock_write, mock_read):
        from supertable.processing import process_delete_only

        cached = _df(id=[1, 2], val=["a", "b"])
        cache = {"f1.parquet": cached}

        process_delete_only(
            df=_df(id=[1]),
            overlapping_files={("f1.parquet", True, 100)},
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=10,
            file_cache=cache,
        )

        mock_read.assert_not_called()
        assert "f1.parquet" not in cache


# ===========================================================================
# 14. filter_stale_incoming_rows
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
        from supertable.processing import find_and_lock_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100},
                {"file": "f2.parquet", "file_size": 200},
            ]
        }
        df = _df(id=[1])

        result = find_and_lock_overlapping_files(snap, df, [], MagicMock())

        # All small files with has_overlap=False (subject to pruning)
        for f, has_overlap, _ in result:
            assert has_overlap is False

    def test_missing_stats_treated_as_overlap(self):
        from supertable.processing import find_and_lock_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100},  # no stats
            ]
        }
        df = _df(id=[1])

        result = find_and_lock_overlapping_files(snap, df, ["id"], MagicMock())
        matches = [f for f, overlap, _ in result if overlap]
        assert len(matches) == 1

    def test_stats_no_overlap_marked_false(self):
        from supertable.processing import find_and_lock_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100,
                 "stats": {"id": {"min": 10, "max": 20}}},
            ]
        }
        # Incoming id=1 is outside [10,20]
        df = _df(id=[1])

        result = find_and_lock_overlapping_files(snap, df, ["id"], MagicMock())
        for f, has_overlap, _ in result:
            if f == "f1.parquet":
                assert has_overlap is False

    def test_stats_overlap_marked_true(self):
        from supertable.processing import find_and_lock_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100,
                 "stats": {"id": {"min": 1, "max": 10}}},
            ]
        }
        # Incoming id=5 is within [1,10]
        df = _df(id=[5])

        result = find_and_lock_overlapping_files(snap, df, ["id"], MagicMock())
        matches = [f for f, overlap, _ in result if overlap]
        assert "f1.parquet" in matches

    def test_empty_resources(self):
        from supertable.processing import find_and_lock_overlapping_files
        result = find_and_lock_overlapping_files({"resources": []}, _df(id=[1]), ["id"], MagicMock())
        assert result == set()

    def test_null_stats_min_max_treated_as_overlap(self):
        from supertable.processing import find_and_lock_overlapping_files

        snap = {
            "resources": [
                {"file": "f1.parquet", "file_size": 100,
                 "stats": {"id": {"min": None, "max": None}}},
            ]
        }
        df = _df(id=[1])

        result = find_and_lock_overlapping_files(snap, df, ["id"], MagicMock())
        matches = [f for f, overlap, _ in result if overlap]
        assert len(matches) == 1