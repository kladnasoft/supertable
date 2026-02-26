"""
Comprehensive unit tests for:
  - data_writer.DataWriter
  - data_reader.DataReader, query_sql, Status, engine
  - super_table.SuperTable
  - simple_table.SimpleTable, _spark_type_from_polars_dtype, _schema_list_from_polars_df
  - processing (all public & internal helpers)

All external dependencies (Redis, Storage, RBAC, Monitoring, SQL parsing) are mocked
so that tests exercise the logic of each module in isolation.
"""

from __future__ import annotations

import io
import json
import os
import types
import uuid
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import (
    MagicMock,
    PropertyMock,
    call,
    patch,
    sentinel,
)

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ---------------------------------------------------------------------------
# Fixtures & helpers
# ---------------------------------------------------------------------------

def _arrow_table(data: dict) -> pa.Table:
    """Build a PyArrow Table from a dict of lists."""
    return pa.table(data)


def _polars_df(data: dict) -> pl.DataFrame:
    return pl.DataFrame(data)


def _dummy_snapshot(
    simple_name: str = "tbl",
    resources: list | None = None,
    version: int = 0,
) -> dict:
    return {
        "simple_name": simple_name,
        "location": f"org/super/tables/{simple_name}",
        "snapshot_version": version,
        "last_updated_ms": 1000,
        "previous_snapshot": None,
        "schema": [],
        "resources": resources or [],
    }


def _resource_entry(
    file: str,
    rows: int = 5,
    columns: int = 3,
    file_size: int = 1024,
    stats: dict | None = None,
) -> dict:
    return {
        "file": file,
        "file_size": file_size,
        "rows": rows,
        "columns": columns,
        "stats": stats or {},
    }


# ===========================================================================
# processing.py tests
# ===========================================================================

class TestResolveUnifiedDtype:
    """Tests for _resolve_unified_dtype."""

    def test_empty_set_returns_utf8(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype(set()) == pl.Utf8

    def test_single_dtype(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int32}) == pl.Int32

    def test_utf8_wins(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int32, pl.Utf8}) == pl.Utf8

    def test_float_wins_over_int(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int64, pl.Float64}) == pl.Float64

    def test_mixed_ints_promote_to_int64(self):
        from supertable.processing import _resolve_unified_dtype
        assert _resolve_unified_dtype({pl.Int8, pl.Int32}) == pl.Int64

    def test_datetime_wins(self):
        from supertable.processing import _resolve_unified_dtype
        result = _resolve_unified_dtype({pl.Datetime, pl.Int32})
        # Should be a Datetime variant
        assert result == pl.Datetime("us", None)

    def test_date_wins_over_numeric(self):
        from supertable.processing import _resolve_unified_dtype
        result = _resolve_unified_dtype({pl.Date, pl.Int32})
        assert result == pl.Date

    def test_mixed_floats_only(self):
        from supertable.processing import _resolve_unified_dtype
        result = _resolve_unified_dtype({pl.Float32, pl.Float64})
        assert result == pl.Float64

    def test_unrecognised_falls_to_utf8(self):
        from supertable.processing import _resolve_unified_dtype
        result = _resolve_unified_dtype({pl.Boolean, pl.Binary})
        assert result == pl.Utf8


class TestUnionSchema:
    """Tests for _union_schema."""

    def test_identical_schemas(self):
        from supertable.processing import _union_schema
        a = _polars_df({"x": [1], "y": ["a"]})
        b = _polars_df({"x": [2], "y": ["b"]})
        schema = _union_schema(a, b)
        assert set(schema.keys()) == {"x", "y"}

    def test_disjoint_schemas(self):
        from supertable.processing import _union_schema
        a = _polars_df({"x": [1]})
        b = _polars_df({"y": ["a"]})
        schema = _union_schema(a, b)
        assert set(schema.keys()) == {"x", "y"}

    def test_overlapping_different_types_promote(self):
        from supertable.processing import _union_schema
        a = _polars_df({"x": [1]})           # Int64
        b = _polars_df({"x": [1.5]})         # Float64
        schema = _union_schema(a, b)
        assert schema["x"] == pl.Float64


class TestAlignToSchema:
    """Tests for _align_to_schema."""

    def test_adds_missing_columns(self):
        from supertable.processing import _align_to_schema
        df = _polars_df({"x": [1, 2]})
        target = {"x": pl.Int64, "y": pl.Utf8}
        result = _align_to_schema(df, target)
        assert "y" in result.columns
        assert result["y"].to_list() == [None, None]

    def test_casts_existing_column(self):
        from supertable.processing import _align_to_schema
        df = _polars_df({"x": [1, 2]})
        target = {"x": pl.Float64}
        result = _align_to_schema(df, target)
        assert result["x"].dtype == pl.Float64

    def test_noop_when_already_aligned(self):
        from supertable.processing import _align_to_schema
        df = _polars_df({"x": [1, 2]})
        target = {"x": pl.Int64}
        result = _align_to_schema(df, target)
        assert result.equals(df)


class TestConcatWithUnion:
    """Tests for concat_with_union."""

    def test_empty_a_returns_b(self):
        from supertable.processing import concat_with_union
        a = pl.DataFrame(schema={"x": pl.Int64})
        b = _polars_df({"x": [1, 2]})
        result = concat_with_union(a, b)
        assert result.shape == (2, 1)

    def test_empty_b_returns_a(self):
        from supertable.processing import concat_with_union
        a = _polars_df({"x": [1, 2]})
        b = pl.DataFrame(schema={"x": pl.Int64})
        result = concat_with_union(a, b)
        assert result.shape == (2, 1)

    def test_union_different_schemas(self):
        from supertable.processing import concat_with_union
        # Both share 'x' as first column; each has a unique extra column.
        # _align_to_schema adds missing cols at the end, so order stays compatible.
        a = _polars_df({"x": [1], "y": ["a"]})
        b = _polars_df({"x": [2], "y": [None], "z": [3.0]})
        result = concat_with_union(a, b)
        assert result.shape[0] == 2
        assert "x" in result.columns
        assert "y" in result.columns
        assert "z" in result.columns

    def test_both_empty_returns_empty(self):
        from supertable.processing import concat_with_union
        a = pl.DataFrame(schema={"x": pl.Int64})
        b = pl.DataFrame(schema={"x": pl.Int64})
        result = concat_with_union(a, b)
        assert result.shape[0] == 0


class TestIsFileInOverlappingFiles:
    """Tests for is_file_in_overlapping_files."""

    def test_found(self):
        from supertable.processing import is_file_in_overlapping_files
        files: Set[Tuple[str, bool, int]] = {("a.parquet", True, 100), ("b.parquet", False, 200)}
        assert is_file_in_overlapping_files("a.parquet", files) is True

    def test_not_found(self):
        from supertable.processing import is_file_in_overlapping_files
        files: Set[Tuple[str, bool, int]] = {("a.parquet", True, 100)}
        assert is_file_in_overlapping_files("missing.parquet", files) is False

    def test_empty_set(self):
        from supertable.processing import is_file_in_overlapping_files
        assert is_file_in_overlapping_files("x", set()) is False


class TestPruneNotOverlappingFilesByThreshold:
    """Tests for prune_not_overlapping_files_by_threshold."""

    def test_always_keeps_true_items(self):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        files: Set[Tuple[str, bool, int]] = {
            ("a.parquet", True, 100),
            ("b.parquet", False, 200),
        }
        result = prune_not_overlapping_files_by_threshold(files)
        assert ("a.parquet", True, 100) in result

    def test_false_items_excluded_below_thresholds(self):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        # Total size small, count of false items small → false items excluded
        files: Set[Tuple[str, bool, int]] = {
            ("a.parquet", True, 100),
            ("b.parquet", False, 200),
        }
        result = prune_not_overlapping_files_by_threshold(files)
        # Only 1 false item, total_size=300 << MAX_MEMORY_CHUNK_SIZE
        assert ("b.parquet", False, 200) not in result

    @patch("supertable.processing.default")
    def test_false_items_included_when_count_exceeds_threshold(self, mock_default):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        mock_default.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024
        mock_default.MAX_OVERLAPPING_FILES = 2  # low threshold
        files: Set[Tuple[str, bool, int]] = {
            ("a.parquet", True, 100),
            ("b.parquet", False, 200),
            ("c.parquet", False, 300),
        }
        result = prune_not_overlapping_files_by_threshold(files)
        # 2 false items >= MAX_OVERLAPPING_FILES=2 → include them
        assert ("a.parquet", True, 100) in result
        # At least some false items should be included
        false_in_result = [f for f in result if f[1] is False]
        assert len(false_in_result) > 0

    @patch("supertable.processing.default")
    def test_false_items_included_when_size_exceeds_threshold(self, mock_default):
        from supertable.processing import prune_not_overlapping_files_by_threshold
        mock_default.MAX_MEMORY_CHUNK_SIZE = 100  # very low
        mock_default.MAX_OVERLAPPING_FILES = 1000
        files: Set[Tuple[str, bool, int]] = {
            ("a.parquet", True, 50),
            ("b.parquet", False, 200),
        }
        result = prune_not_overlapping_files_by_threshold(files)
        # total_size=250 > 100 → gate opens
        assert ("a.parquet", True, 50) in result


class TestCollectColumnStatistics:
    """Tests for collect_column_statistics."""

    def test_basic_stats(self):
        from supertable.processing import collect_column_statistics
        df = _polars_df({"x": [1, 2, 3], "y": ["a", "c", "b"]})
        stats = collect_column_statistics(df, overwrite_columns=["x"])
        assert stats["x"]["min"] == 1
        assert stats["x"]["max"] == 3
        assert stats["y"]["min"] == "a"
        assert stats["y"]["max"] == "c"

    def test_empty_df_returns_empty(self):
        from supertable.processing import collect_column_statistics
        df = pl.DataFrame(schema={"x": pl.Int64})
        stats = collect_column_statistics(df, overwrite_columns=["x"])
        assert stats == {}

    def test_date_values_are_iso_strings(self):
        from supertable.processing import collect_column_statistics
        df = _polars_df({"d": [date(2023, 1, 1), date(2023, 12, 31)]})
        stats = collect_column_statistics(df, overwrite_columns=["d"])
        assert isinstance(stats["d"]["min"], str)
        assert "2023-01-01" in stats["d"]["min"]

    def test_datetime_values_are_iso_strings(self):
        from supertable.processing import collect_column_statistics
        df = pl.DataFrame({"dt": [datetime(2023, 1, 1, 8, 0), datetime(2023, 6, 15, 12, 0)]})
        stats = collect_column_statistics(df, overwrite_columns=["dt"])
        assert isinstance(stats["dt"]["min"], str)


class TestFindAndLockOverlappingFiles:
    """Tests for find_and_lock_overlapping_files."""

    @patch("supertable.processing._storage")
    def test_no_resources_returns_empty(self, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files
        snapshot = _dummy_snapshot(resources=[])
        df = _polars_df({"day": ["2023-01-01"], "value": [1]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        assert len(result) == 0

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema", return_value={"day": "String", "value": "Int64"})
    def test_resource_with_no_stats_marked_as_overlapping(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files
        resource = _resource_entry("data/file1.parquet", stats=None)
        snapshot = _dummy_snapshot(resources=[resource])
        df = _polars_df({"day": ["2023-01-01"], "value": [1]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        overlapping = [f for f in result if f[1] is True]
        assert len(overlapping) == 1

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema", return_value={"day": "String", "value": "Int64"})
    def test_resource_with_stats_no_overlap(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files
        resource = _resource_entry(
            "data/file1.parquet",
            stats={"day": {"min": "2023-06-01", "max": "2023-06-30"}},
        )
        snapshot = _dummy_snapshot(resources=[resource])
        df = _polars_df({"day": ["2023-01-01"], "value": [1]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        # No overlap → either not in result, or has_overlap=False
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 0

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema", return_value={"day": "String", "value": "Int64"})
    def test_resource_with_stats_has_overlap(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files
        resource = _resource_entry(
            "data/file1.parquet",
            stats={"day": {"min": "2023-01-01", "max": "2023-01-31"}},
        )
        snapshot = _dummy_snapshot(resources=[resource])
        df = _polars_df({"day": ["2023-01-15"], "value": [1]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 1

    @patch("supertable.processing._storage")
    @patch("supertable.processing.default")
    def test_no_overwrite_columns_compaction_path(self, mock_default, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files
        mock_default.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024
        mock_default.MAX_OVERLAPPING_FILES = 100
        resource = _resource_entry("data/file1.parquet", file_size=1024)
        snapshot = _dummy_snapshot(resources=[resource])
        df = _polars_df({"day": ["2023-01-01"], "value": [1]})
        result = find_and_lock_overlapping_files(snapshot, df, [], locking=None)
        # Without overwrite columns → compaction path, has_overlap=False
        for item in result:
            assert item[1] is False

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema", return_value={"day": "String"})
    def test_resource_stats_missing_column_treated_as_overlapping(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files
        resource = _resource_entry(
            "data/file1.parquet",
            stats={"other_col": {"min": "a", "max": "z"}},  # day not in stats
        )
        snapshot = _dummy_snapshot(resources=[resource])
        df = _polars_df({"day": ["2023-01-15"]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 1

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema", return_value={"day": "String"})
    def test_resource_stats_none_min_max_treated_as_overlapping(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files
        resource = _resource_entry(
            "data/file1.parquet",
            stats={"day": {"min": None, "max": None}},
        )
        snapshot = _dummy_snapshot(resources=[resource])
        df = _polars_df({"day": ["2023-01-15"]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 1


class TestWriteParquetAndCollectResources:
    """Tests for write_parquet_and_collect_resources."""

    @patch("supertable.processing._storage")
    def test_writes_file_and_appends_resource(self, mock_storage):
        from supertable.processing import write_parquet_and_collect_resources
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 4096

        df = _polars_df({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        new_resources: list = []
        write_parquet_and_collect_resources(
            write_df=df,
            overwrite_columns=["x"],
            data_dir="/tmp/test_data",
            new_resources=new_resources,
            compression_level=1,
        )
        assert len(new_resources) == 1
        res = new_resources[0]
        assert res["rows"] == 3
        assert res["columns"] == 2
        assert "stats" in res
        assert "x" in res["stats"]
        assert res["file_size"] == 4096

    @patch("supertable.processing._storage")
    def test_resource_has_correct_stats_keys(self, mock_storage):
        from supertable.processing import write_parquet_and_collect_resources
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 2048

        df = _polars_df({"a": [10, 20], "b": [1.5, 2.5]})
        new_resources: list = []
        write_parquet_and_collect_resources(
            write_df=df,
            overwrite_columns=["a"],
            data_dir="/tmp/data",
            new_resources=new_resources,
            compression_level=1,
        )
        stats = new_resources[0]["stats"]
        assert "a" in stats
        assert "b" in stats
        assert stats["a"]["min"] == 10
        assert stats["a"]["max"] == 20


class TestProcessOverlappingFiles:
    """Tests for process_overlapping_files (full pipeline)."""

    @patch("supertable.processing._storage")
    def test_insert_only_no_overlapping(self, mock_storage):
        from supertable.processing import process_overlapping_files
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 1024

        df = _polars_df({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        overlapping: Set[Tuple[str, bool, int]] = set()

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = (
            process_overlapping_files(df, overlapping, ["x"], "/tmp/data", 1)
        )
        assert inserted == 3
        assert deleted == 0
        assert total_columns == 2
        assert total_rows == 3
        assert len(new_resources) == 1
        assert len(sunset_files) == 0

    @patch("supertable.processing._read_parquet_safe")
    @patch("supertable.processing._storage")
    def test_overlap_deletes_matching_rows(self, mock_storage, mock_read):
        from supertable.processing import process_overlapping_files
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 1024

        existing_df = _polars_df({"x": [1, 2, 3], "y": ["old_a", "old_b", "old_c"]})
        mock_read.return_value = existing_df

        new_df = _polars_df({"x": [2, 3, 4], "y": ["new_b", "new_c", "new_d"]})
        overlapping: Set[Tuple[str, bool, int]] = {("data/old_file.parquet", True, 1024)}

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = (
            process_overlapping_files(new_df, overlapping, ["x"], "/tmp/data", 1)
        )
        assert inserted == 3
        assert deleted == 2  # rows x=2 and x=3 deleted from existing
        assert "data/old_file.parquet" in sunset_files

    @patch("supertable.processing._read_parquet_safe")
    @patch("supertable.processing._storage")
    def test_no_overwrite_columns_compaction(self, mock_storage, mock_read):
        from supertable.processing import process_overlapping_files
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 512

        existing_df = _polars_df({"x": [10, 20]})
        mock_read.return_value = existing_df

        new_df = _polars_df({"x": [30]})
        overlapping: Set[Tuple[str, bool, int]] = {("data/compact.parquet", False, 512)}

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = (
            process_overlapping_files(new_df, overlapping, [], "/tmp/data", 1)
        )
        assert inserted == 1
        assert "data/compact.parquet" in sunset_files


class TestProcessFilesWithOverlap:
    """Tests for process_files_with_overlap."""

    @patch("supertable.processing._read_parquet_safe")
    @patch("supertable.processing._storage")
    def test_skips_file_when_read_returns_none(self, mock_storage, mock_read):
        from supertable.processing import process_files_with_overlap
        mock_read.return_value = None
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 1024

        df = _polars_df({"x": [1]})
        empty_df = pl.DataFrame(schema={"x": pl.Int64})
        merged_df = df.clone()
        new_resources: list = []
        sunset_files: set = set()

        deleted, result_merged, total_rows = process_files_with_overlap(
            data_dir="/tmp",
            deleted=0,
            df=df,
            empty_df=empty_df,
            merged_df=merged_df,
            new_resources=new_resources,
            overlapping_files={("missing.parquet", True, 1024)},
            overwrite_columns=["x"],
            sunset_files=sunset_files,
            total_rows=0,
            compression_level=1,
        )
        assert deleted == 0
        assert "missing.parquet" not in sunset_files

    @patch("supertable.processing._read_parquet_safe")
    @patch("supertable.processing._storage")
    def test_no_rows_deleted_skips_sunset(self, mock_storage, mock_read):
        from supertable.processing import process_files_with_overlap
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 1024

        existing = _polars_df({"x": [10, 20], "y": ["a", "b"]})
        mock_read.return_value = existing

        # New df has x=99 → no overlap with existing x=[10,20]
        df = _polars_df({"x": [99], "y": ["z"]})
        empty_df = pl.DataFrame(schema={"x": pl.Int64, "y": pl.Utf8})
        merged_df = df.clone()
        new_resources: list = []
        sunset_files: set = set()

        deleted, result_merged, total_rows = process_files_with_overlap(
            data_dir="/tmp",
            deleted=0,
            df=df,
            empty_df=empty_df,
            merged_df=merged_df,
            new_resources=new_resources,
            overlapping_files={("nochange.parquet", True, 1024)},
            overwrite_columns=["x"],
            sunset_files=sunset_files,
            total_rows=0,
            compression_level=1,
        )
        assert deleted == 0
        assert "nochange.parquet" not in sunset_files


class TestProcessFilesWithoutOverlap:
    """Tests for process_files_without_overlap."""

    @patch("supertable.processing._read_parquet_safe")
    @patch("supertable.processing._storage")
    def test_compacts_false_files(self, mock_storage, mock_read):
        from supertable.processing import process_files_without_overlap
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 512

        existing = _polars_df({"x": [10, 20]})
        mock_read.return_value = existing

        empty_df = pl.DataFrame(schema={"x": pl.Int64})
        new_resources: list = []
        sunset_files: set = set()

        result = process_files_without_overlap(
            empty_df=empty_df,
            data_dir="/tmp",
            new_resources=new_resources,
            overlapping_files={("compact1.parquet", False, 512)},
            overwrite_columns=["x"],
            sunset_files=sunset_files,
            compression_level=1,
        )
        assert "compact1.parquet" in sunset_files
        assert result.shape[0] == 2

    @patch("supertable.processing._read_parquet_safe")
    @patch("supertable.processing._storage")
    def test_ignores_true_files(self, mock_storage, mock_read):
        from supertable.processing import process_files_without_overlap

        empty_df = pl.DataFrame(schema={"x": pl.Int64})
        new_resources: list = []
        sunset_files: set = set()

        result = process_files_without_overlap(
            empty_df=empty_df,
            data_dir="/tmp",
            new_resources=new_resources,
            overlapping_files={("overlap.parquet", True, 512)},
            overwrite_columns=["x"],
            sunset_files=sunset_files,
            compression_level=1,
        )
        # True files are skipped by this function
        assert "overlap.parquet" not in sunset_files
        assert result.shape[0] == 0


class TestSafeExists:
    """Tests for _safe_exists."""

    @patch("supertable.processing._storage")
    def test_returns_true(self, mock_storage):
        from supertable.processing import _safe_exists
        mock_storage.exists.return_value = True
        assert _safe_exists("some/path") is True

    @patch("supertable.processing._storage")
    def test_returns_false_on_exception(self, mock_storage):
        from supertable.processing import _safe_exists
        mock_storage.exists.side_effect = Exception("connection error")
        assert _safe_exists("some/path") is False


class TestReadParquetSafe:
    """Tests for _read_parquet_safe."""

    @patch("supertable.processing._storage")
    def test_returns_none_when_not_exists(self, mock_storage):
        from supertable.processing import _read_parquet_safe
        mock_storage.exists.return_value = False
        assert _read_parquet_safe("missing.parquet") is None

    @patch("supertable.processing._storage")
    def test_returns_none_on_read_error(self, mock_storage):
        from supertable.processing import _read_parquet_safe
        mock_storage.exists.return_value = True
        mock_storage.read_parquet.side_effect = Exception("corrupt file")
        assert _read_parquet_safe("bad.parquet") is None

    @patch("supertable.processing._storage")
    def test_returns_dataframe_on_success(self, mock_storage):
        from supertable.processing import _read_parquet_safe
        mock_storage.exists.return_value = True
        arrow_tbl = pa.table({"x": [1, 2, 3]})
        mock_storage.read_parquet.return_value = arrow_tbl
        result = _read_parquet_safe("good.parquet")
        assert result is not None
        assert result.shape == (3, 1)


# ===========================================================================
# simple_table.py tests
# ===========================================================================

class TestSparkTypeFromPolarsDtype:
    """Tests for _spark_type_from_polars_dtype."""

    def test_string_types(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Utf8) == "string"
        assert _spark_type_from_polars_dtype(pl.String) == "string"

    def test_boolean(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Boolean) == "boolean"

    def test_integer_types(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Int8) == "byte"
        assert _spark_type_from_polars_dtype(pl.Int16) == "short"
        assert _spark_type_from_polars_dtype(pl.Int32) == "integer"
        assert _spark_type_from_polars_dtype(pl.Int64) == "long"

    def test_unsigned_types(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.UInt8) == "short"
        assert _spark_type_from_polars_dtype(pl.UInt16) == "integer"
        assert _spark_type_from_polars_dtype(pl.UInt32) == "long"
        assert _spark_type_from_polars_dtype(pl.UInt64) == "decimal(20,0)"

    def test_float_types(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Float32) == "float"
        assert _spark_type_from_polars_dtype(pl.Float64) == "double"

    def test_temporal_types(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Date) == "date"
        assert _spark_type_from_polars_dtype(pl.Datetime) == "timestamp"

    def test_binary(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        assert _spark_type_from_polars_dtype(pl.Binary) == "binary"

    def test_unknown_falls_to_string(self):
        from supertable.simple_table import _spark_type_from_polars_dtype
        # Pass something unusual
        assert _spark_type_from_polars_dtype("unknown_type") == "string"


class TestSchemaListFromPolarsDf:
    """Tests for _schema_list_from_polars_df."""

    def test_basic_schema(self):
        from supertable.simple_table import _schema_list_from_polars_df
        df = _polars_df({"x": [1], "y": ["a"], "z": [1.5]})
        result = _schema_list_from_polars_df(df)
        assert len(result) == 3
        names = [r["name"] for r in result]
        assert "x" in names
        assert "y" in names
        assert "z" in names
        for entry in result:
            assert "type" in entry
            assert "nullable" in entry
            assert entry["nullable"] is True
            assert "metadata" in entry

    def test_empty_df(self):
        from supertable.simple_table import _schema_list_from_polars_df
        df = pl.DataFrame(schema={"a": pl.Int64})
        result = _schema_list_from_polars_df(df)
        assert len(result) == 1
        assert result[0]["name"] == "a"

    def test_object_without_schema_returns_empty(self):
        from supertable.simple_table import _schema_list_from_polars_df
        result = _schema_list_from_polars_df(object())
        assert result == []


class TestSimpleTableInit:
    """Tests for SimpleTable.__init__ and init_simple_table."""

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.generate_filename", return_value="test_snapshot.json")
    @patch("supertable.simple_table.check_write_access")
    def test_init_when_leaf_exists_skips_bootstrap(self, mock_access, mock_gen, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()

        st = SimpleTable(mock_super, "my_table")
        assert st.simple_name == "my_table"
        # Should NOT call init_simple_table internals
        mock_super.storage.write_json.assert_not_called()

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.generate_filename", return_value="snap_init.json")
    @patch("supertable.simple_table.check_write_access")
    def test_init_when_leaf_not_exists_bootstraps(self, mock_access, mock_gen, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = False
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()
        mock_super.storage.exists.return_value = False

        st = SimpleTable(mock_super, "new_table")
        # Should write initial snapshot
        mock_super.storage.write_json.assert_called_once()
        call_args = mock_super.storage.write_json.call_args
        snapshot_data = call_args[0][1]
        assert snapshot_data["simple_name"] == "new_table"
        assert snapshot_data["snapshot_version"] == 0
        assert snapshot_data["resources"] == []


class TestSimpleTableGetSnapshot:
    """Tests for SimpleTable.get_simple_table_snapshot."""

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.check_write_access")
    def test_returns_payload_from_redis_when_available(self, mock_access, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        payload = _dummy_snapshot("tbl", resources=[_resource_entry("f.parquet")])
        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog.get_leaf.return_value = {"path": "snap.json", "payload": payload}
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()

        st = SimpleTable(mock_super, "tbl")
        snap, path = st.get_simple_table_snapshot()
        assert snap["simple_name"] == "tbl"
        assert path == "snap.json"
        # Should NOT read from storage since payload was in Redis
        mock_super.storage.read_json.assert_not_called()

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.check_write_access")
    def test_falls_back_to_storage_when_no_payload(self, mock_access, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog.get_leaf.return_value = {"path": "snap.json", "payload": None}
        mock_catalog_cls.return_value = mock_catalog

        storage_snap = _dummy_snapshot("tbl", resources=[])
        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()
        mock_super.storage.read_json.return_value = storage_snap

        st = SimpleTable(mock_super, "tbl")
        snap, path = st.get_simple_table_snapshot()
        assert snap == storage_snap
        mock_super.storage.read_json.assert_called_once_with("snap.json")

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.check_write_access")
    def test_raises_when_no_path(self, mock_access, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog.get_leaf.return_value = {"path": None}
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()

        st = SimpleTable(mock_super, "tbl")
        with pytest.raises(FileNotFoundError):
            st.get_simple_table_snapshot()

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.check_write_access")
    def test_nested_payload_snapshot_key(self, mock_access, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        inner_snap = _dummy_snapshot("tbl", resources=[_resource_entry("f.parquet")])
        payload = {"snapshot": inner_snap}

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog.get_leaf.return_value = {"path": "snap.json", "payload": payload}
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()

        st = SimpleTable(mock_super, "tbl")
        snap, path = st.get_simple_table_snapshot()
        assert snap["simple_name"] == "tbl"


class TestSimpleTableUpdate:
    """Tests for SimpleTable.update."""

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.generate_filename", return_value="new_snap.json")
    @patch("supertable.simple_table.collect_schema", return_value=[{"name": "x", "type": "long"}])
    @patch("supertable.simple_table.check_write_access")
    def test_update_adds_resources_and_removes_sunset(self, mock_access, mock_schema, mock_gen, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        existing_resource = _resource_entry("old_file.parquet")
        sunset_resource = _resource_entry("sunset_file.parquet")
        payload = _dummy_snapshot(
            "tbl",
            resources=[existing_resource, sunset_resource],
            version=1,
        )

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog.get_leaf.return_value = {"path": "snap_v1.json", "payload": payload}
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()

        st = SimpleTable(mock_super, "tbl")

        new_res = [_resource_entry("new_file.parquet")]
        sunset = {"sunset_file.parquet"}
        model_df = _polars_df({"x": [1]})

        result_snap, result_path = st.update(new_res, sunset, model_df)

        assert result_snap["snapshot_version"] == 2
        resource_files = [r["file"] for r in result_snap["resources"]]
        assert "old_file.parquet" in resource_files
        assert "new_file.parquet" in resource_files
        assert "sunset_file.parquet" not in resource_files
        assert result_snap["previous_snapshot"] == "snap_v1.json"


class TestSimpleTableDelete:
    """Tests for SimpleTable.delete."""

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.check_write_access")
    def test_delete_calls_storage_and_catalog(self, mock_access, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()
        mock_super.storage.exists.return_value = True

        st = SimpleTable(mock_super, "del_table")
        st.delete(user_hash="test_hash")

        mock_access.assert_called_once()
        mock_super.storage.delete.assert_called_once()
        mock_catalog.delete_simple_table.assert_called_once_with("org", "super", "del_table")

    @patch("supertable.simple_table.RedisCatalog")
    @patch("supertable.simple_table.check_write_access")
    def test_delete_handles_missing_storage(self, mock_access, mock_catalog_cls):
        from supertable.simple_table import SimpleTable

        mock_catalog = MagicMock()
        mock_catalog.leaf_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        mock_super = MagicMock()
        mock_super.organization = "org"
        mock_super.super_name = "super"
        mock_super.storage = MagicMock()
        mock_super.storage.exists.return_value = True
        mock_super.storage.delete.side_effect = FileNotFoundError("gone")

        st = SimpleTable(mock_super, "del_table")
        # Should not raise
        st.delete(user_hash="test_hash")
        mock_catalog.delete_simple_table.assert_called_once()


# ===========================================================================
# super_table.py tests
# ===========================================================================

class TestSuperTableInit:
    """Tests for SuperTable.__init__ and init_super_table."""

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_init_when_root_exists_skips_init(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("my_super", "my_org")
        assert st.super_name == "my_super"
        assert st.organization == "my_org"
        # Should NOT call init_super_table
        mock_storage.makedirs.assert_not_called()
        mock_catalog.ensure_root.assert_not_called()

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_init_when_root_not_exists_bootstraps(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = False
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("new_super", "org")
        mock_storage.makedirs.assert_called_once()
        mock_catalog.ensure_root.assert_called_once_with("org", "new_super")


class TestSuperTableReadSnapshot:
    """Tests for SuperTable.read_simple_table_snapshot."""

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_read_snapshot_success(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 100
        mock_storage.read_json.return_value = {"resources": []}
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("s", "o")
        result = st.read_simple_table_snapshot("snap.json")
        assert result == {"resources": []}

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_read_snapshot_not_found(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_storage.exists.return_value = False
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("s", "o")
        with pytest.raises(FileNotFoundError):
            st.read_simple_table_snapshot("missing.json")

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_read_snapshot_empty_file(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_storage.exists.return_value = True
        mock_storage.size.return_value = 0
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("s", "o")
        with pytest.raises(ValueError, match="empty"):
            st.read_simple_table_snapshot("empty.json")

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_read_snapshot_empty_path(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("s", "o")
        with pytest.raises(FileNotFoundError):
            st.read_simple_table_snapshot("")


class TestSuperTableDelete:
    """Tests for SuperTable.delete."""

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_delete_removes_storage_and_redis(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_storage.exists.return_value = True
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("s", "o")
        st.delete()
        mock_storage.delete.assert_called_once()
        mock_catalog.delete_super_table.assert_called_once_with("o", "s")

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_delete_handles_missing_storage(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_storage.exists.return_value = True
        mock_storage.delete.side_effect = FileNotFoundError("gone")
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("s", "o")
        st.delete()
        # Should still delete Redis meta
        mock_catalog.delete_super_table.assert_called_once()

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_delete_when_storage_not_exists(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_storage = MagicMock()
        mock_storage.exists.return_value = False
        mock_get_storage.return_value = mock_storage

        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("s", "o")
        st.delete()
        mock_storage.delete.assert_not_called()
        mock_catalog.delete_super_table.assert_called_once()


class TestSuperTableDirectoryLayout:
    """Tests for SuperTable directory construction."""

    @patch("supertable.super_table.UserManager")
    @patch("supertable.super_table.RoleManager")
    @patch("supertable.super_table.RedisCatalog")
    @patch("supertable.super_table.get_storage")
    def test_super_dir_path(self, mock_get_storage, mock_catalog_cls, mock_role, mock_user):
        from supertable.super_table import SuperTable

        mock_get_storage.return_value = MagicMock()
        mock_catalog = MagicMock()
        mock_catalog.root_exists.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        st = SuperTable("my_super", "my_org")
        expected = os.path.join("my_org", "my_super", "super")
        assert st.super_dir == expected


# ===========================================================================
# data_writer.py tests
# ===========================================================================

class TestDataWriterValidation:
    """Tests for DataWriter.validation method."""

    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    def _make_writer(self, mock_super_cls, mock_catalog_cls):
        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super
        mock_catalog_cls.return_value = MagicMock()
        from supertable.data_writer import DataWriter
        return DataWriter("test_super", "test_org")

    def test_empty_name_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        with pytest.raises(ValueError, match="can't be empty"):
            dw.validation(df, "", ["x"])

    def test_name_too_long_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        with pytest.raises(ValueError, match="can't be empty or longer"):
            dw.validation(df, "a" * 129, ["x"])

    def test_name_matches_super_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        with pytest.raises(ValueError, match="can't match"):
            dw.validation(df, "test_super", ["x"])

    def test_invalid_name_pattern_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        with pytest.raises(ValueError, match="Invalid table name"):
            dw.validation(df, "123bad", ["x"])

    def test_name_with_special_chars_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        with pytest.raises(ValueError, match="Invalid table name"):
            dw.validation(df, "my-table", ["x"])

    def test_name_with_spaces_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        with pytest.raises(ValueError, match="Invalid table name"):
            dw.validation(df, "my table", ["x"])

    def test_overwrite_columns_not_in_df_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1], "y": [2]})
        with pytest.raises(ValueError, match="not present"):
            dw.validation(df, "valid_table", ["x", "missing_col"])

    def test_overwrite_columns_as_string_raises(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        with pytest.raises(ValueError, match="must be list"):
            dw.validation(df, "valid_table", "x")

    def test_valid_input_passes(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1], "y": [2]})
        # Should not raise
        dw.validation(df, "valid_table", ["x"])

    def test_valid_underscore_prefix(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        dw.validation(df, "_private_table", ["x"])

    def test_valid_empty_overwrite_columns(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        dw.validation(df, "valid_table", [])

    def test_valid_name_128_chars(self):
        dw = self._make_writer()
        df = _polars_df({"x": [1]})
        dw.validation(df, "a" * 128, ["x"])


class TestDataWriterWrite:
    """Tests for DataWriter.write method."""

    @patch("supertable.data_writer.get_monitoring_logger")
    @patch("supertable.data_writer.MirrorFormats")
    @patch("supertable.data_writer.process_overlapping_files")
    @patch("supertable.data_writer.find_and_lock_overlapping_files")
    @patch("supertable.data_writer.check_write_access")
    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    @patch("supertable.data_writer.SimpleTable")
    def test_write_success_path(
        self,
        mock_simple_cls,
        mock_super_cls,
        mock_catalog_cls,
        mock_access,
        mock_find_overlap,
        mock_process_overlap,
        mock_mirror,
        mock_monitor,
    ):
        from supertable.data_writer import DataWriter

        # Setup SuperTable mock
        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        # Setup RedisCatalog mock
        mock_catalog = MagicMock()
        mock_catalog.acquire_simple_lock.return_value = "lock_token_123"
        mock_catalog.release_simple_lock.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        # Setup SimpleTable mock
        mock_simple = MagicMock()
        mock_simple.data_dir = "/tmp/data"
        snapshot = _dummy_snapshot("tbl", resources=[])
        mock_simple.get_simple_table_snapshot.return_value = (snapshot, "snap.json")
        mock_simple.update.return_value = (snapshot, "new_snap.json")
        mock_simple_cls.return_value = mock_simple

        # Setup processing mocks
        mock_find_overlap.return_value = set()
        mock_process_overlap.return_value = (3, 0, 3, 2, [_resource_entry("new.parquet")], set())

        # Setup monitoring mock
        mock_monitor_inst = MagicMock()
        mock_monitor.return_value = mock_monitor_inst

        dw = DataWriter("test_super", "test_org")
        arrow = _arrow_table({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        result = dw.write("user_hash", "my_table", arrow, ["x"])

        assert result == (2, 3, 3, 0)
        mock_access.assert_called_once()
        mock_catalog.acquire_simple_lock.assert_called_once()
        mock_catalog.release_simple_lock.assert_called_once()
        mock_catalog.bump_root.assert_called_once()

    @patch("supertable.data_writer.check_write_access")
    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    def test_write_lock_timeout_raises(self, mock_super_cls, mock_catalog_cls, mock_access):
        from supertable.data_writer import DataWriter

        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        mock_catalog = MagicMock()
        mock_catalog.acquire_simple_lock.return_value = None  # lock failed
        mock_catalog_cls.return_value = mock_catalog

        dw = DataWriter("test_super", "test_org")
        arrow = _arrow_table({"x": [1, 2, 3], "y": ["a", "b", "c"]})

        with pytest.raises(TimeoutError, match="Could not acquire lock"):
            dw.write("user_hash", "my_table", arrow, ["x"])

    @patch("supertable.data_writer.check_write_access")
    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    def test_write_validation_failure_does_not_acquire_lock(self, mock_super_cls, mock_catalog_cls, mock_access):
        from supertable.data_writer import DataWriter

        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        mock_catalog = MagicMock()
        mock_catalog_cls.return_value = mock_catalog

        dw = DataWriter("test_super", "test_org")
        arrow = _arrow_table({"x": [1]})

        with pytest.raises(ValueError):
            dw.write("user_hash", "", arrow, ["x"])

        mock_catalog.acquire_simple_lock.assert_not_called()

    @patch("supertable.data_writer.get_monitoring_logger")
    @patch("supertable.data_writer.MirrorFormats")
    @patch("supertable.data_writer.process_overlapping_files")
    @patch("supertable.data_writer.find_and_lock_overlapping_files")
    @patch("supertable.data_writer.check_write_access")
    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    @patch("supertable.data_writer.SimpleTable")
    def test_write_releases_lock_on_exception(
        self,
        mock_simple_cls,
        mock_super_cls,
        mock_catalog_cls,
        mock_access,
        mock_find_overlap,
        mock_process_overlap,
        mock_mirror,
        mock_monitor,
    ):
        from supertable.data_writer import DataWriter

        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        mock_catalog = MagicMock()
        mock_catalog.acquire_simple_lock.return_value = "token_abc"
        mock_catalog_cls.return_value = mock_catalog

        # Fail during processing
        mock_simple = MagicMock()
        mock_simple.data_dir = "/tmp/data"
        mock_simple.get_simple_table_snapshot.side_effect = RuntimeError("boom")
        mock_simple_cls.return_value = mock_simple

        mock_find_overlap.return_value = set()

        dw = DataWriter("test_super", "test_org")
        arrow = _arrow_table({"x": [1, 2], "y": ["a", "b"]})

        with pytest.raises(RuntimeError, match="boom"):
            dw.write("user_hash", "my_table", arrow, ["x"])

        # Lock MUST be released in finally block
        mock_catalog.release_simple_lock.assert_called_once()

    @patch("supertable.data_writer.get_monitoring_logger")
    @patch("supertable.data_writer.MirrorFormats")
    @patch("supertable.data_writer.process_overlapping_files")
    @patch("supertable.data_writer.find_and_lock_overlapping_files")
    @patch("supertable.data_writer.check_write_access")
    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    @patch("supertable.data_writer.SimpleTable")
    def test_write_mirror_failure_does_not_break_write(
        self,
        mock_simple_cls,
        mock_super_cls,
        mock_catalog_cls,
        mock_access,
        mock_find_overlap,
        mock_process_overlap,
        mock_mirror,
        mock_monitor,
    ):
        from supertable.data_writer import DataWriter

        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        mock_catalog = MagicMock()
        mock_catalog.acquire_simple_lock.return_value = "tok"
        mock_catalog.release_simple_lock.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        mock_simple = MagicMock()
        mock_simple.data_dir = "/tmp/data"
        snapshot = _dummy_snapshot("tbl")
        mock_simple.get_simple_table_snapshot.return_value = (snapshot, "s.json")
        mock_simple.update.return_value = (snapshot, "ns.json")
        mock_simple_cls.return_value = mock_simple

        mock_find_overlap.return_value = set()
        mock_process_overlap.return_value = (1, 0, 1, 1, [], set())

        # Mirror fails
        mock_mirror.mirror_if_enabled.side_effect = RuntimeError("mirror fail")

        mock_monitor_inst = MagicMock()
        mock_monitor.return_value = mock_monitor_inst

        dw = DataWriter("test_super", "test_org")
        arrow = _arrow_table({"x": [1]})
        result = dw.write("user_hash", "my_table", arrow, ["x"])

        # Write should still succeed
        assert result is not None

    @patch("supertable.data_writer.get_monitoring_logger")
    @patch("supertable.data_writer.MirrorFormats")
    @patch("supertable.data_writer.process_overlapping_files")
    @patch("supertable.data_writer.find_and_lock_overlapping_files")
    @patch("supertable.data_writer.check_write_access")
    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    @patch("supertable.data_writer.SimpleTable")
    def test_write_cas_fallback_to_set_leaf_path(
        self,
        mock_simple_cls,
        mock_super_cls,
        mock_catalog_cls,
        mock_access,
        mock_find_overlap,
        mock_process_overlap,
        mock_mirror,
        mock_monitor,
    ):
        from supertable.data_writer import DataWriter

        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        mock_catalog = MagicMock()
        mock_catalog.acquire_simple_lock.return_value = "tok"
        mock_catalog.release_simple_lock.return_value = True
        # set_leaf_payload_cas fails → fallback to set_leaf_path_cas
        mock_catalog.set_leaf_payload_cas.side_effect = Exception("not supported")
        mock_catalog_cls.return_value = mock_catalog

        mock_simple = MagicMock()
        mock_simple.data_dir = "/tmp/data"
        snapshot = _dummy_snapshot("tbl")
        mock_simple.get_simple_table_snapshot.return_value = (snapshot, "s.json")
        mock_simple.update.return_value = (snapshot, "ns.json")
        mock_simple_cls.return_value = mock_simple

        mock_find_overlap.return_value = set()
        mock_process_overlap.return_value = (1, 0, 1, 1, [], set())
        mock_monitor.return_value = MagicMock()

        dw = DataWriter("test_super", "test_org")
        arrow = _arrow_table({"x": [1]})
        result = dw.write("user_hash", "my_table", arrow, ["x"])

        assert result is not None
        mock_catalog.set_leaf_path_cas.assert_called_once()

    @patch("supertable.data_writer.get_monitoring_logger")
    @patch("supertable.data_writer.MirrorFormats")
    @patch("supertable.data_writer.process_overlapping_files")
    @patch("supertable.data_writer.find_and_lock_overlapping_files")
    @patch("supertable.data_writer.check_write_access")
    @patch("supertable.data_writer.RedisCatalog")
    @patch("supertable.data_writer.SuperTable")
    @patch("supertable.data_writer.SimpleTable")
    def test_write_monitoring_failure_does_not_break_result(
        self,
        mock_simple_cls,
        mock_super_cls,
        mock_catalog_cls,
        mock_access,
        mock_find_overlap,
        mock_process_overlap,
        mock_mirror,
        mock_monitor,
    ):
        from supertable.data_writer import DataWriter

        mock_super = MagicMock()
        mock_super.super_name = "test_super"
        mock_super.organization = "test_org"
        mock_super_cls.return_value = mock_super

        mock_catalog = MagicMock()
        mock_catalog.acquire_simple_lock.return_value = "tok"
        mock_catalog.release_simple_lock.return_value = True
        mock_catalog_cls.return_value = mock_catalog

        mock_simple = MagicMock()
        mock_simple.data_dir = "/tmp/data"
        snapshot = _dummy_snapshot("tbl")
        mock_simple.get_simple_table_snapshot.return_value = (snapshot, "s.json")
        mock_simple.update.return_value = (snapshot, "ns.json")
        mock_simple_cls.return_value = mock_simple

        mock_find_overlap.return_value = set()
        mock_process_overlap.return_value = (1, 0, 1, 1, [], set())

        # Monitoring fails
        mock_monitor.side_effect = RuntimeError("monitoring down")

        dw = DataWriter("test_super", "test_org")
        arrow = _arrow_table({"x": [1]})
        result = dw.write("user_hash", "my_table", arrow, ["x"])

        # Should still return result
        assert result is not None


# ===========================================================================
# data_reader.py tests
# ===========================================================================

class TestStatusEnum:
    """Tests for Status enum."""

    def test_ok_value(self):
        from supertable.data_reader import Status
        assert Status.OK.value == "ok"

    def test_error_value(self):
        from supertable.data_reader import Status
        assert Status.ERROR.value == "error"


class TestEngineEnum:
    """Tests for engine enum."""

    def test_auto_value(self):
        from supertable.data_reader import engine
        assert engine.AUTO.value == "auto"

    def test_duckdb_value(self):
        from supertable.data_reader import engine
        assert engine.DUCKDB.value == "duckdb"

    def test_spark_value(self):
        from supertable.data_reader import engine
        assert engine.SPARK.value == "spark"

    def test_to_internal(self):
        from supertable.data_reader import engine
        from supertable.executor import Engine
        assert engine.DUCKDB.to_internal() == Engine.DUCKDB
        assert engine.AUTO.to_internal() == Engine.AUTO


class TestDataReaderInit:
    """Tests for DataReader.__init__."""

    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_init_sets_attributes(self, mock_parser_cls, mock_get_storage):
        from supertable.data_reader import DataReader

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = [("super", "table1", "t1")]
        mock_parser.original_query = "SELECT * FROM table1"
        mock_parser_cls.return_value = mock_parser

        mock_get_storage.return_value = MagicMock()

        dr = DataReader("my_super", "my_org", "SELECT * FROM table1")
        assert dr.super_name == "my_super"
        assert dr.organization == "my_org"
        assert dr.timer is None
        assert dr.plan_stats is None
        assert dr.tables == [("super", "table1", "t1")]


class TestDataReaderExecute:
    """Tests for DataReader.execute."""

    @patch("supertable.data_reader.extend_execution_plan")
    @patch("supertable.data_reader.Executor")
    @patch("supertable.data_reader.DataEstimator")
    @patch("supertable.data_reader.QueryPlanManager")
    @patch("supertable.data_reader.restrict_read_access")
    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_execute_success(
        self,
        mock_parser_cls,
        mock_get_storage,
        mock_restrict,
        mock_qpm_cls,
        mock_estimator_cls,
        mock_executor_cls,
        mock_extend,
    ):
        import pandas as pd
        from supertable.data_reader import DataReader, Status

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = [("super", "tbl", "t")]
        mock_parser.original_query = "SELECT * FROM tbl"
        mock_parser_cls.return_value = mock_parser
        mock_get_storage.return_value = MagicMock()

        mock_qpm = MagicMock()
        mock_qpm.query_id = "q1"
        mock_qpm.query_hash = "h1"
        mock_qpm_cls.return_value = mock_qpm

        # Estimator returns valid reflection
        mock_reflection = MagicMock()
        mock_reflection.supers = [MagicMock()]
        mock_reflection.storage_type = "LOCAL"
        mock_reflection.total_reflections = 1
        mock_reflection.reflection_bytes = 1024
        mock_estimator = MagicMock()
        mock_estimator.estimate.return_value = mock_reflection
        mock_estimator_cls.return_value = mock_estimator

        result_df = pd.DataFrame({"x": [1, 2]})
        mock_executor = MagicMock()
        mock_executor.execute.return_value = (result_df, "duckdb")
        mock_executor_cls.return_value = mock_executor

        dr = DataReader("super", "org", "SELECT * FROM tbl")
        df, status, msg = dr.execute("user_hash")

        assert status == Status.OK
        assert msg is None
        assert len(df) == 2

    @patch("supertable.data_reader.extend_execution_plan")
    @patch("supertable.data_reader.Executor")
    @patch("supertable.data_reader.DataEstimator")
    @patch("supertable.data_reader.QueryPlanManager")
    @patch("supertable.data_reader.restrict_read_access")
    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_execute_no_files_returns_empty(
        self,
        mock_parser_cls,
        mock_get_storage,
        mock_restrict,
        mock_qpm_cls,
        mock_estimator_cls,
        mock_executor_cls,
        mock_extend,
    ):
        import pandas as pd
        from supertable.data_reader import DataReader, Status

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT * FROM tbl"
        mock_parser_cls.return_value = mock_parser
        mock_get_storage.return_value = MagicMock()

        mock_qpm = MagicMock()
        mock_qpm.query_id = "q1"
        mock_qpm.query_hash = "h1"
        mock_qpm_cls.return_value = mock_qpm

        mock_reflection = MagicMock()
        mock_reflection.supers = []  # No files
        mock_reflection.storage_type = "LOCAL"
        mock_reflection.total_reflections = 0
        mock_reflection.reflection_bytes = 0
        mock_estimator = MagicMock()
        mock_estimator.estimate.return_value = mock_reflection
        mock_estimator_cls.return_value = mock_estimator

        dr = DataReader("super", "org", "SELECT * FROM tbl")
        df, status, msg = dr.execute("user_hash")

        assert status == Status.ERROR
        assert msg == "No parquet files found"
        assert df.empty

    @patch("supertable.data_reader.extend_execution_plan")
    @patch("supertable.data_reader.Executor")
    @patch("supertable.data_reader.DataEstimator")
    @patch("supertable.data_reader.QueryPlanManager")
    @patch("supertable.data_reader.restrict_read_access")
    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_execute_exception_returns_error(
        self,
        mock_parser_cls,
        mock_get_storage,
        mock_restrict,
        mock_qpm_cls,
        mock_estimator_cls,
        mock_executor_cls,
        mock_extend,
    ):
        import pandas as pd
        from supertable.data_reader import DataReader, Status

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT * FROM tbl"
        mock_parser_cls.return_value = mock_parser
        mock_get_storage.return_value = MagicMock()

        mock_qpm = MagicMock()
        mock_qpm.query_id = "q1"
        mock_qpm.query_hash = "h1"
        mock_qpm_cls.return_value = mock_qpm

        mock_estimator = MagicMock()
        mock_estimator.estimate.side_effect = RuntimeError("estimation failed")
        mock_estimator_cls.return_value = mock_estimator

        dr = DataReader("super", "org", "SELECT * FROM tbl")
        df, status, msg = dr.execute("user_hash")

        assert status == Status.ERROR
        assert "estimation failed" in msg


class TestQuerySql:
    """Tests for the query_sql function."""

    @patch("supertable.data_reader.DataReader")
    def test_query_sql_success(self, mock_reader_cls):
        import pandas as pd
        from supertable.data_reader import query_sql, Status

        result_df = pd.DataFrame({"x": [1, 2, 3], "y": ["a", "b", "c"]})
        mock_reader = MagicMock()
        mock_reader.execute.return_value = (result_df, Status.OK, None)
        mock_reader_cls.return_value = mock_reader

        columns, rows, meta = query_sql(
            organization="org",
            super_name="super",
            sql="SELECT * FROM tbl",
            limit=100,
            engine=MagicMock(),
            user_hash="u1",
        )
        assert columns == ["x", "y"]
        assert len(rows) == 3
        assert len(meta) == 2
        assert meta[0]["name"] == "x"
        assert meta[0]["nullable"] is True

    @patch("supertable.data_reader.DataReader")
    def test_query_sql_error_raises(self, mock_reader_cls):
        import pandas as pd
        from supertable.data_reader import query_sql, Status

        mock_reader = MagicMock()
        mock_reader.execute.return_value = (pd.DataFrame(), Status.ERROR, "bad query")
        mock_reader_cls.return_value = mock_reader

        with pytest.raises(RuntimeError, match="bad query"):
            query_sql(
                organization="org",
                super_name="super",
                sql="SELECT bad",
                limit=100,
                engine=MagicMock(),
                user_hash="u1",
            )


class TestDataReaderLogPrefix:
    """Tests for DataReader._lp."""

    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_lp_prepends_context(self, mock_parser_cls, mock_storage):
        from supertable.data_reader import DataReader

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = []
        mock_parser.original_query = "SELECT 1"
        mock_parser_cls.return_value = mock_parser
        mock_storage.return_value = MagicMock()

        dr = DataReader("s", "o", "SELECT 1")
        dr._log_ctx = "[test] "
        assert dr._lp("hello") == "[test] hello"


# ===========================================================================
# data_classes.py tests
# ===========================================================================

class TestDataClasses:
    """Tests for dataclass structures."""

    def test_table_definition(self):
        from supertable.data_classes import TableDefinition
        td = TableDefinition(super_name="s", simple_name="t", alias="a", columns=["x", "y"])
        assert td.super_name == "s"
        assert td.simple_name == "t"
        assert td.alias == "a"
        assert td.columns == ["x", "y"]

    def test_table_definition_default_columns(self):
        from supertable.data_classes import TableDefinition
        td = TableDefinition(super_name="s", simple_name="t", alias="a")
        assert td.columns == []

    def test_super_snapshot(self):
        from supertable.data_classes import SuperSnapshot
        ss = SuperSnapshot(super_name="s", simple_name="t", simple_version=1, files=["f1"], columns={"c1"})
        assert ss.super_name == "s"
        assert ss.simple_version == 1
        assert ss.files == ["f1"]
        assert ss.columns == {"c1"}

    def test_super_snapshot_defaults(self):
        from supertable.data_classes import SuperSnapshot
        ss = SuperSnapshot(super_name="s", simple_name="t", simple_version=0)
        assert ss.files == []
        assert ss.columns == set()

    def test_reflection(self):
        from supertable.data_classes import Reflection, SuperSnapshot
        snap = SuperSnapshot(super_name="s", simple_name="t", simple_version=1)
        r = Reflection(storage_type="LOCAL", reflection_bytes=1024, total_reflections=5, supers=[snap])
        assert r.storage_type == "LOCAL"
        assert r.reflection_bytes == 1024
        assert len(r.supers) == 1


# ===========================================================================
# plan_stats.py tests
# ===========================================================================

class TestPlanStats:
    """Tests for PlanStats."""

    def test_init_empty(self):
        from supertable.plan_stats import PlanStats
        ps = PlanStats()
        assert ps.stats == []

    def test_add_stat(self):
        from supertable.plan_stats import PlanStats
        ps = PlanStats()
        ps.add_stat({"ENGINE": "duckdb"})
        ps.add_stat({"ROWS": 100})
        assert len(ps.stats) == 2
        assert ps.stats[0] == {"ENGINE": "duckdb"}


# ===========================================================================
# Integration-like tests (multi-component)
# ===========================================================================

class TestOverlapDetectionWithDateTypes:
    """Test overlap detection handles date/datetime string comparisons correctly."""

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema")
    def test_date_overlap_with_iso_strings(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files

        mock_schema.return_value = {"day": "Date"}
        resource = _resource_entry(
            "data/file1.parquet",
            stats={"day": {"min": "2023-01-01", "max": "2023-01-31"}},
        )
        snapshot = _dummy_snapshot(resources=[resource])
        df = pl.DataFrame({"day": [date(2023, 1, 15)]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 1

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema")
    def test_datetime_overlap_with_iso_strings(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files

        mock_schema.return_value = {"dt": "DateTime"}
        resource = _resource_entry(
            "data/file1.parquet",
            stats={"dt": {"min": "2023-01-01T00:00:00", "max": "2023-01-31T23:59:59"}},
        )
        snapshot = _dummy_snapshot(resources=[resource])
        df = pl.DataFrame({"dt": [datetime(2023, 1, 15, 12, 0)]})
        result = find_and_lock_overlapping_files(snapshot, df, ["dt"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 1


class TestMultipleOverwriteColumns:
    """Tests for overlap detection with multiple overwrite columns."""

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema")
    def test_overlap_any_column_match_triggers_overlap(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files

        mock_schema.return_value = {"day": "String", "client": "String"}
        resource = _resource_entry(
            "data/file1.parquet",
            stats={
                "day": {"min": "2023-01-01", "max": "2023-01-31"},
                "client": {"min": "clientA", "max": "clientZ"},
            },
        )
        snapshot = _dummy_snapshot(resources=[resource])
        # day is outside range BUT client is within range → overlap detected
        # because the code checks each overwrite column and breaks on first match
        df = _polars_df({"day": ["2023-06-01"], "client": ["clientB"]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day", "client"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 1

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema")
    def test_no_overlap_when_all_columns_outside_range(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files

        mock_schema.return_value = {"day": "String", "client": "String"}
        resource = _resource_entry(
            "data/file1.parquet",
            stats={
                "day": {"min": "2023-01-01", "max": "2023-01-31"},
                "client": {"min": "clientA", "max": "clientC"},
            },
        )
        snapshot = _dummy_snapshot(resources=[resource])
        # day=2023-06-01 outside [01-01,01-31], client=clientZ outside [clientA,clientC]
        df = _polars_df({"day": ["2023-06-01"], "client": ["clientZ"]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day", "client"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 0


class TestNoneValuesInOverwriteColumns:
    """Tests for handling None values in overwrite column data."""

    @patch("supertable.processing._storage")
    @patch("supertable.processing.collect_schema")
    def test_none_values_treated_as_overlap(self, mock_schema, mock_storage):
        from supertable.processing import find_and_lock_overlapping_files

        mock_schema.return_value = {"day": "String"}
        resource = _resource_entry(
            "data/file1.parquet",
            stats={"day": {"min": "2023-01-01", "max": "2023-12-31"}},
        )
        snapshot = _dummy_snapshot(resources=[resource])
        df = _polars_df({"day": [None, "2023-06-01"]})
        result = find_and_lock_overlapping_files(snapshot, df, ["day"], locking=None)
        overlapping_true = [f for f in result if f[1] is True]
        assert len(overlapping_true) == 1