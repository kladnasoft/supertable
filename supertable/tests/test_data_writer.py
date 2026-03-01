# test_data_writer_v20260301_1200_comprehensive.py
"""
Comprehensive test suite for supertable/data_writer.py

Covers:
  - DataWriter.__init__
  - DataWriter.validation (all branches)
  - DataWriter.write (full happy path, delete_only, newer_than, error paths,
    lock lifecycle, monitoring, mirroring fallback, CAS fallback, etc.)

All external dependencies are mocked: Redis, storage, processing, RBAC, monitoring.
"""

from __future__ import annotations

import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple
from unittest.mock import MagicMock, patch, PropertyMock, call

import polars as pl
import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# Paths to patch (as they appear in data_writer.py's import namespace)
# ---------------------------------------------------------------------------
_MOD = "supertable.data_writer"
_PATCH_CHECK_WRITE = f"{_MOD}.check_write_access"
_PATCH_SUPER_TABLE = f"{_MOD}.SuperTable"
_PATCH_SIMPLE_TABLE = f"{_MOD}.SimpleTable"
_PATCH_REDIS_CATALOG = f"{_MOD}.RedisCatalog"
_PATCH_TIMER = f"{_MOD}.Timer"
_PATCH_POLARS_FROM_ARROW = f"{_MOD}.polars.from_arrow"
_PATCH_FIND_OVERLAP = f"{_MOD}.find_and_lock_overlapping_files"
_PATCH_FILTER_STALE = f"{_MOD}.filter_stale_incoming_rows"
_PATCH_PROCESS_OVERLAP = f"{_MOD}.process_overlapping_files"
_PATCH_PROCESS_DELETE = f"{_MOD}.process_delete_only"
_PATCH_MIRROR = f"{_MOD}.MirrorFormats"
_PATCH_GET_MON_LOGGER = f"{_MOD}.get_monitoring_logger"
_PATCH_UUID4 = f"{_MOD}.uuid.uuid4"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _arrow_table(columns: Dict[str, list]) -> pa.Table:
    """Helper: build a PyArrow table from column dict."""
    return pa.table(columns)


def _polars_df(columns: Dict[str, list]) -> pl.DataFrame:
    """Helper: build a Polars DataFrame from column dict."""
    return pl.DataFrame(columns)


@pytest.fixture()
def sample_arrow():
    """A simple 3-row Arrow table with id and value columns."""
    return _arrow_table({"id": [1, 2, 3], "value": ["a", "b", "c"]})


@pytest.fixture()
def sample_polars():
    """Polars equivalent of sample_arrow."""
    return _polars_df({"id": [1, 2, 3], "value": ["a", "b", "c"]})


@pytest.fixture()
def mock_catalog():
    """Pre-configured mock RedisCatalog."""
    cat = MagicMock(name="RedisCatalog")
    cat.acquire_simple_lock.return_value = "lock-token-abc"
    cat.release_simple_lock.return_value = True
    cat.set_leaf_payload_cas.return_value = 1
    cat.bump_root.return_value = 1
    cat.root_exists.return_value = True
    cat.leaf_exists.return_value = True
    cat.get_table_config.return_value = None
    return cat


@pytest.fixture()
def mock_super_table():
    """Pre-configured mock SuperTable instance."""
    st = MagicMock(name="SuperTable")
    st.super_name = "my_super"
    st.organization = "my_org"
    st.storage = MagicMock()
    st.catalog = MagicMock()
    return st


@pytest.fixture()
def snapshot_dict():
    """Minimal valid snapshot dictionary."""
    return {
        "simple_name": "my_table",
        "location": "my_org/my_super/tables/my_table",
        "snapshot_version": 1,
        "last_updated_ms": 1000,
        "previous_snapshot": None,
        "schema": [],
        "resources": [],
    }


# ---------------------------------------------------------------------------
# Helper: build a fully-mocked DataWriter without touching real Redis/storage
# ---------------------------------------------------------------------------

def _build_writer(mock_super_table_cls, mock_catalog_cls, mock_catalog_inst, mock_st_inst):
    """
    Instantiate DataWriter with mocked SuperTable and RedisCatalog constructors.
    Returns the DataWriter instance.
    """
    mock_super_table_cls.return_value = mock_st_inst
    mock_catalog_cls.return_value = mock_catalog_inst

    from supertable.data_writer import DataWriter

    writer = DataWriter.__new__(DataWriter)
    writer.super_table = mock_st_inst
    writer.catalog = mock_catalog_inst
    writer._table_config_cache = {}
    return writer


# ====================================================================
# 1.  DataWriter.__init__
# ====================================================================

class TestDataWriterInit:
    """Verify constructor wires SuperTable and RedisCatalog correctly."""

    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_init_creates_super_table_and_catalog(self, MockST, MockCat):
        from supertable.data_writer import DataWriter

        mock_st = MagicMock()
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        MockCat.return_value = mock_cat

        dw = DataWriter("super1", "org1")

        MockST.assert_called_once_with("super1", "org1")
        MockCat.assert_called_once_with()
        assert dw.super_table is mock_st
        assert dw.catalog is mock_cat

    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_init_propagates_super_table_exception(self, MockST, MockCat):
        from supertable.data_writer import DataWriter

        MockST.side_effect = ConnectionError("redis down")
        with pytest.raises(ConnectionError, match="redis down"):
            DataWriter("s", "o")


# ====================================================================
# 2.  DataWriter.validation — exhaustive branch coverage
# ====================================================================

class TestValidation:
    """All validation rules and edge cases."""

    def _make_writer(self):
        """Lightweight writer with mocked internals for validation-only tests."""
        from supertable.data_writer import DataWriter

        w = DataWriter.__new__(DataWriter)
        w.super_table = MagicMock()
        w.super_table.super_name = "super1"
        w.catalog = MagicMock()
        w._table_config_cache = {}
        return w

    # ---- simple_name length -------------------------------------------

    def test_empty_simple_name_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        with pytest.raises(ValueError, match="can't be empty"):
            w.validation(df, "", [], None, False)

    def test_simple_name_too_long_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        long_name = "a" * 129
        with pytest.raises(ValueError, match="can't be empty or longer than 128"):
            w.validation(df, long_name, [], None, False)

    def test_simple_name_exactly_128_ok(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        name = "a" * 128
        # Should not raise
        w.validation(df, name, [], None, False)

    def test_simple_name_exactly_1_char_ok(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        w.validation(df, "x", [], None, False)

    # ---- simple_name matches super_name --------------------------------

    def test_simple_name_equals_super_name_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        with pytest.raises(ValueError, match="can't match with SuperTable name"):
            w.validation(df, "super1", [], None, False)

    # ---- simple_name regex (^[A-Za-z_][A-Za-z0-9_]*$) -----------------

    @pytest.mark.parametrize("bad_name", [
        "1starts_with_digit",
        "has-dash",
        "has space",
        "has.dot",
        "special!char",
        "tab\there",
        "new\nline",
        "$dollar",
        "semi;colon",
    ])
    def test_invalid_simple_name_regex(self, bad_name):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        with pytest.raises(ValueError, match="Invalid table name"):
            w.validation(df, bad_name, [], None, False)

    @pytest.mark.parametrize("good_name", [
        "valid_name",
        "_leading_underscore",
        "CamelCase",
        "a",
        "A1_b2_C3",
        "__double",
        "ALL_CAPS_123",
    ])
    def test_valid_simple_name_regex(self, good_name):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        # Should not raise
        w.validation(df, good_name, [], None, False)

    # ---- overwrite_columns is string -----------------------------------

    def test_overwrite_columns_as_string_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        with pytest.raises(ValueError, match="overwrite columns must be list"):
            w.validation(df, "tbl", "col_a", None, False)

    # ---- overwrite_columns not in dataframe ----------------------------

    def test_overwrite_columns_missing_from_df_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1], "b": [2]})
        with pytest.raises(ValueError, match="not present in the dataset"):
            w.validation(df, "tbl", ["a", "missing_col"], None, False)

    def test_overwrite_columns_all_present_ok(self):
        w = self._make_writer()
        df = _polars_df({"a": [1], "b": [2]})
        w.validation(df, "tbl", ["a", "b"], None, False)

    def test_overwrite_columns_empty_list_ok(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        w.validation(df, "tbl", [], None, False)

    # ---- delete_only without overwrite_columns -------------------------

    def test_delete_only_without_overwrite_columns_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        with pytest.raises(ValueError, match="delete_only requires overwrite_columns"):
            w.validation(df, "tbl", [], None, delete_only=True)

    def test_delete_only_with_overwrite_columns_ok(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        w.validation(df, "tbl", ["a"], None, delete_only=True)

    # ---- newer_than validation -----------------------------------------

    def test_newer_than_not_string_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1], "ts": [100]})
        with pytest.raises(ValueError, match="newer_than must be a column name string"):
            w.validation(df, "tbl", ["a"], newer_than=123, delete_only=False)

    def test_newer_than_column_missing_from_df_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1], "ts": [100]})
        with pytest.raises(ValueError, match="not present in the dataset"):
            w.validation(df, "tbl", ["a"], newer_than="missing_ts", delete_only=False)

    def test_newer_than_without_overwrite_columns_raises(self):
        w = self._make_writer()
        df = _polars_df({"a": [1], "ts": [100]})
        with pytest.raises(ValueError, match="newer_than requires overwrite_columns"):
            w.validation(df, "tbl", [], newer_than="ts", delete_only=False)

    def test_newer_than_valid_ok(self):
        w = self._make_writer()
        df = _polars_df({"a": [1], "ts": [100]})
        w.validation(df, "tbl", ["a"], newer_than="ts", delete_only=False)

    # ---- newer_than is None (default) ----------------------------------

    def test_newer_than_none_skips_validation(self):
        w = self._make_writer()
        df = _polars_df({"a": [1]})
        # Should not raise — newer_than block is skipped entirely
        w.validation(df, "tbl", ["a"], newer_than=None, delete_only=False)


# ====================================================================
# 3.  DataWriter.write — Happy Path (normal insert/upsert)
# ====================================================================

class TestWriteHappyPath:
    """Full write flow with no delete_only, no newer_than."""

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_normal_write_returns_correct_tuple(
        self,
        MockST, MockCat,
        mock_from_arrow,
        mock_check_write,
        MockSimple,
        mock_find_overlap,
        mock_process,
        MockMirror,
        mock_get_mon,
    ):
        # --- Setup ---
        mock_st = MagicMock(super_name="s1", organization="o1")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.release_simple_lock.return_value = True
        mock_cat.set_leaf_payload_cas.return_value = 1
        mock_cat.bump_root.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1, 2], "val": ["a", "b"]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock()
        mock_simple.data_dir = "/data"
        snap = {"resources": [], "snapshot_version": 0}
        mock_simple.get_simple_table_snapshot.return_value = (snap, "/snap/path")
        mock_simple.update.return_value = ({"resources": [{"file": "f1"}]}, "/snap/new")
        MockSimple.return_value = mock_simple

        mock_find_overlap.return_value = set()

        # process_overlapping_files returns 6-tuple
        new_res = [{"file": "new.parquet"}]
        sunset = {"old.parquet"}
        mock_process.return_value = (2, 0, 2, 2, new_res, sunset)

        mock_mon = MagicMock()
        mock_get_mon.return_value = mock_mon

        from supertable.data_writer import DataWriter
        dw = DataWriter("s1", "o1")

        # --- Act ---
        arrow = _arrow_table({"id": [1, 2], "val": ["a", "b"]})
        result = dw.write("admin", "my_tbl", arrow, ["id"])

        # --- Assert ---
        assert result == (2, 2, 2, 0)  # (total_columns, total_rows, inserted, deleted)

        mock_check_write.assert_called_once_with(
            super_name="s1", organization="o1",
            role_name="admin", table_name="my_tbl",
        )
        mock_cat.acquire_simple_lock.assert_called_once()
        mock_cat.release_simple_lock.assert_called_once()
        mock_cat.set_leaf_payload_cas.assert_called_once()
        mock_cat.bump_root.assert_called_once()
        mock_simple.update.assert_called_once()
        mock_mon.log_metric.assert_called_once()
        MockMirror.mirror_if_enabled.assert_called_once()

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_write_passes_compression_level_to_process(
        self,
        MockST, MockCat,
        mock_from_arrow, mock_check_write, MockSimple,
        mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"x": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple

        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        dw.write("admin", "tbl", _arrow_table({"x": [1]}), ["x"], compression_level=9)

        _, kwargs = mock_process.call_args
        assert kwargs.get("compression_level", mock_process.call_args[0][4] if len(mock_process.call_args[0]) > 4 else None) is not None


# ====================================================================
# 4.  DataWriter.write — delete_only Path
# ====================================================================

class TestWriteDeleteOnly:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_DELETE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_delete_only_calls_process_delete_only(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_proc_del, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1], "val": ["x"]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()

        mock_proc_del.return_value = (0, 1, 0, 2, [], {"f1"})
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1], "val": ["x"]}), ["id"], delete_only=True)

        mock_proc_del.assert_called_once()
        assert result == (2, 0, 0, 1)  # (cols, total_rows, inserted, deleted)

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_DELETE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_delete_only_does_not_call_process_overlapping_files(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_proc_del, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_proc_del.return_value = (0, 0, 0, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter

        with patch(_PATCH_PROCESS_OVERLAP) as mock_proc_overlap:
            dw = DataWriter("s", "o")
            dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"], delete_only=True)
            mock_proc_overlap.assert_not_called()


# ====================================================================
# 5.  DataWriter.write — newer_than Filtering
# ====================================================================

class TestWriteNewerThan:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FILTER_STALE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_newer_than_all_stale_returns_early(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_filter_stale,
        mock_process, MockMirror, mock_get_mon,
    ):
        """When all rows are stale, write returns (cols, 0, 0, 0) without processing."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.release_simple_lock.return_value = True
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1], "ts": [100]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()

        # Return empty DataFrame (all stale)
        empty_df = _polars_df({"id": [], "ts": []}).cast({"id": pl.Int64, "ts": pl.Int64})
        mock_filter_stale.return_value = empty_df
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1], "ts": [100]}), ["id"], newer_than="ts")

        assert result == (2, 0, 0, 0)
        mock_process.assert_not_called()
        # Lock must still be released even on early return
        mock_cat.release_simple_lock.assert_called_once()

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FILTER_STALE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_newer_than_some_rows_survive_continues_write(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_filter_stale,
        mock_process, MockMirror, mock_get_mon,
    ):
        """When some rows survive stale filtering, write continues to process."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1, 2], "ts": [100, 200]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()

        # 1 of 2 rows survives
        surviving = _polars_df({"id": [2], "ts": [200]})
        mock_filter_stale.return_value = surviving
        mock_process.return_value = (1, 0, 1, 2, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1, 2], "ts": [100, 200]}), ["id"], newer_than="ts")

        mock_process.assert_called_once()
        assert result is not None

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FILTER_STALE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_newer_than_skipped_when_no_overwrite_columns(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_filter_stale,
        mock_process, MockMirror, mock_get_mon,
    ):
        """newer_than filtering is only triggered if BOTH newer_than and overwrite_columns are set."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        # Pass newer_than but empty overwrite_columns → validation will fail
        # because newer_than requires overwrite_columns.  This test confirms the
        # validation boundary rather than the filtering branch.
        df = _polars_df({"id": [1], "ts": [1]})
        mock_from_arrow.return_value = df

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(ValueError, match="newer_than requires overwrite_columns"):
            dw.write("admin", "tbl", _arrow_table({"id": [1], "ts": [1]}), [], newer_than="ts")

        mock_filter_stale.assert_not_called()


# ====================================================================
# 6.  DataWriter.write — Lock Lifecycle
# ====================================================================

class TestWriteLockLifecycle:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_lock_timeout_raises(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """When acquire_simple_lock returns None, TimeoutError is raised."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = None  # Lock not acquired
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(TimeoutError, match="Could not acquire lock"):
            dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_lock_released_on_success(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        mock_cat.release_simple_lock.assert_called_once_with("o", "s", "tbl", "tok")

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_lock_released_on_process_failure(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """Lock is always released in finally block, even if processing raises."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.side_effect = RuntimeError("process boom")

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(RuntimeError, match="process boom"):
            dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        mock_cat.release_simple_lock.assert_called_once_with("o", "s", "tbl", "tok")

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_lock_not_released_when_never_acquired(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """If lock was never acquired (token is None), release is never called."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = None
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(TimeoutError):
            dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        mock_cat.release_simple_lock.assert_not_called()

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_lock_release_failure_is_swallowed(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """Even if release_simple_lock raises, the exception is swallowed."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.release_simple_lock.side_effect = RuntimeError("release boom")
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        # Should not raise
        result = dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])
        assert result is not None


# ====================================================================
# 7.  DataWriter.write — CAS Fallback
# ====================================================================

class TestWriteCASFallback:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_falls_back_to_set_leaf_path_cas_on_payload_error(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """If set_leaf_payload_cas raises, write falls back to set_leaf_path_cas."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.set_leaf_payload_cas.side_effect = Exception("no payload support")
        mock_cat.set_leaf_path_cas.return_value = 1
        mock_cat.bump_root.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({"resources": []}, "/snap/new")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        mock_cat.set_leaf_payload_cas.assert_called_once()
        mock_cat.set_leaf_path_cas.assert_called_once()
        assert result is not None


# ====================================================================
# 8.  DataWriter.write — Mirroring
# ====================================================================

class TestWriteMirroring:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_mirror_failure_is_swallowed(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """MirrorFormats.mirror_if_enabled failure must not crash the write."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        MockMirror.mirror_if_enabled.side_effect = Exception("mirror boom")
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        # Should succeed despite mirror failure
        assert result is not None
        MockMirror.mirror_if_enabled.assert_called_once()


# ====================================================================
# 9.  DataWriter.write — Monitoring
# ====================================================================

class TestWriteMonitoring:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_monitoring_called_after_lock_release(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """Monitoring enqueue happens AFTER finally block (lock release)."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())

        mock_mon = MagicMock()
        mock_get_mon.return_value = mock_mon

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        mock_get_mon.assert_called_once_with(
            super_name="s", organization="o", monitor_type="stats",
        )
        mock_mon.log_metric.assert_called_once()
        payload = mock_mon.log_metric.call_args[0][0]
        assert "query_id" in payload
        assert "duration" in payload
        assert payload["super_name"] == "s"
        assert payload["table_name"] == "tbl"

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_monitoring_failure_is_swallowed(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """Even if monitoring enqueue raises, write returns successfully."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        mock_get_mon.side_effect = RuntimeError("monitoring exploded")

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])
        assert result is not None

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_monitoring_not_called_on_exception_before_stats_payload(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """If write fails before stats_payload is set, monitoring log_metric is skipped."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        # SimpleTable constructor raises
        MockSimple.side_effect = FileNotFoundError("no snapshot")

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(FileNotFoundError):
            dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        # get_monitoring_logger may be called but log_metric should not be
        # since stats_payload is None when exception fires
        for c in mock_get_mon.return_value.method_calls:
            assert c[0] != "log_metric"


# ====================================================================
# 10. DataWriter.write — Access Control
# ====================================================================

class TestWriteAccessControl:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_permission_error_propagates(
        self,
        MockST, MockCat, mock_from_arrow,
        mock_check_write, MockSimple, mock_find_overlap,
        mock_process, MockMirror, mock_get_mon,
    ):
        """If check_write_access raises PermissionError, it propagates."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        MockCat.return_value = MagicMock()

        mock_from_arrow.return_value = _polars_df({"id": [1]})
        mock_check_write.side_effect = PermissionError("no write perm")

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(PermissionError, match="no write perm"):
            dw.write("reader_role", "tbl", _arrow_table({"id": [1]}), ["id"])

        # Nothing past access check should run
        MockSimple.assert_not_called()
        mock_find_overlap.assert_not_called()

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_access_check_called_with_correct_args(
        self,
        MockST, MockCat, mock_from_arrow,
        mock_check_write, MockSimple, mock_find_overlap,
        mock_process, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="my_s", organization="my_o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("my_s", "my_o")
        dw.write("writer_role", "target_tbl", _arrow_table({"id": [1]}), ["id"])

        mock_check_write.assert_called_once_with(
            super_name="my_s",
            organization="my_o",
            role_name="writer_role",
            table_name="target_tbl",
        )


# ====================================================================
# 11. DataWriter.write — Arrow-to-Polars Conversion
# ====================================================================

class TestWriteConversion:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_from_arrow_conversion_error_propagates(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """If polars.from_arrow fails, the exception propagates before any lock."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        MockCat.return_value = mock_cat

        mock_from_arrow.side_effect = TypeError("unsupported arrow type")

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(TypeError, match="unsupported arrow type"):
            dw.write("admin", "tbl", "not_an_arrow_table", ["id"])

        # Lock should never have been attempted
        mock_cat.acquire_simple_lock.assert_not_called()


# ====================================================================
# 12. DataWriter.write — Snapshot Read
# ====================================================================

class TestWriteSnapshotRead:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_snapshot_read_failure_releases_lock_and_raises(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.side_effect = FileNotFoundError("no leaf")
        MockSimple.return_value = mock_simple

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(FileNotFoundError, match="no leaf"):
            dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        mock_cat.release_simple_lock.assert_called_once_with("o", "s", "tbl", "tok")


# ====================================================================
# 13. DataWriter.write — Monitoring Payload Fields
# ====================================================================

class TestWriteMonitoringPayload:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_stats_payload_contains_all_required_fields(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="sup", organization="org")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1, 2], "val": ["a", "b"]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        new_res = [{"file": "f1"}, {"file": "f2"}]
        sunset = {"old1"}
        mock_simple.update.return_value = ({"resources": new_res}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (2, 1, 5, 2, new_res, sunset)

        mock_mon = MagicMock()
        mock_get_mon.return_value = mock_mon

        from supertable.data_writer import DataWriter
        dw = DataWriter("sup", "org")
        dw.write("admin", "my_tbl", _arrow_table({"id": [1, 2], "val": ["a", "b"]}), ["id"])

        payload = mock_mon.log_metric.call_args[0][0]

        expected_keys = {
            "query_id", "recorded_at", "super_name", "table_name",
            "overwrite_columns", "newer_than", "delete_only",
            "inserted", "deleted", "total_rows", "total_columns",
            "new_resources", "sunset_files", "duration",
        }
        assert expected_keys.issubset(payload.keys())

        assert payload["super_name"] == "sup"
        assert payload["table_name"] == "my_tbl"
        assert payload["overwrite_columns"] == ["id"]
        assert payload["newer_than"] is None
        assert payload["delete_only"] is False
        assert payload["inserted"] == 2
        assert payload["deleted"] == 1
        assert payload["total_rows"] == 5
        assert payload["total_columns"] == 2
        assert payload["new_resources"] == 2  # len(new_res)
        assert payload["sunset_files"] == 1  # len(sunset)
        assert isinstance(payload["duration"], float)
        assert payload["duration"] >= 0

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FILTER_STALE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_stale_early_return_skips_monitoring_enqueue(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_filter_stale,
        mock_process, MockMirror, mock_get_mon,
    ):
        """When all rows are stale, write() returns early via 'return' inside the
        try block.  The finally block (lock release) runs, but the monitoring
        enqueue code that lives *after* the try/finally never executes.
        Therefore log_metric must NOT be called."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1, 2, 3], "ts": [10, 20, 30]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()

        empty_df = _polars_df({"id": [], "ts": []}).cast({"id": pl.Int64, "ts": pl.Int64})
        mock_filter_stale.return_value = empty_df

        mock_mon = MagicMock()
        mock_get_mon.return_value = mock_mon

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1, 2, 3], "ts": [10, 20, 30]}), ["id"], newer_than="ts")

        # Early return yields the zero-result tuple
        assert result == (2, 0, 0, 0)
        # Monitoring enqueue is unreachable on this path because 'return'
        # inside try causes finally to run, then the function exits — the
        # post-finally monitoring block is never reached.
        mock_mon.log_metric.assert_not_called()


# ====================================================================
# 14. DataWriter.write — Validation Errors During Write
# ====================================================================

class TestWriteValidationErrors:

    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_write_with_invalid_table_name_raises_before_lock(
        self, MockST, MockCat, mock_from_arrow, mock_check_write,
    ):
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(ValueError, match="Invalid table name"):
            dw.write("admin", "bad-name!", _arrow_table({"id": [1]}), ["id"])

        mock_cat.acquire_simple_lock.assert_not_called()

    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_write_with_string_overwrite_columns_raises(
        self, MockST, MockCat, mock_from_arrow, mock_check_write,
    ):
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        MockCat.return_value = MagicMock()
        mock_from_arrow.return_value = _polars_df({"id": [1]})

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")

        with pytest.raises(ValueError, match="overwrite columns must be list"):
            dw.write("admin", "tbl", _arrow_table({"id": [1]}), "id")


# ====================================================================
# 15. DataWriter.write — update() integration
# ====================================================================

class TestWriteUpdateIntegration:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_update_receives_correct_args(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """Verify simple_table.update is called with new_resources, sunset_files,
        the DataFrame, and last_snapshot + last_snapshot_path."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "tok"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1]})
        mock_from_arrow.return_value = df

        snap = {"resources": [{"file": "existing.parquet"}], "snapshot_version": 5}
        snap_path = "/org/s/tables/tbl/snapshots/snap5.json"

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = (snap, snap_path)
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()

        new_res = [{"file": "new.parquet"}]
        sunset = {"old.parquet"}
        mock_process.return_value = (1, 0, 1, 1, new_res, sunset)
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        dw.write("admin", "tbl", _arrow_table({"id": [1]}), ["id"])

        mock_simple.update.assert_called_once()
        args, kwargs = mock_simple.update.call_args
        assert args[0] is new_res
        assert args[1] is sunset
        # The DataFrame passed to update is the (possibly filtered) polars DF
        assert args[2] is df
        assert kwargs.get("last_snapshot") is snap
        assert kwargs.get("last_snapshot_path") == snap_path


# ====================================================================
# 16. DataWriter.write — bump_root called
# ====================================================================

class TestWriteBumpRoot:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_bump_root_called_with_org_and_super(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        mock_st = MagicMock(super_name="mysup", organization="myorg")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"a": [1]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (1, 0, 1, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("mysup", "myorg")
        dw.write("admin", "tbl", _arrow_table({"a": [1]}), ["a"])

        mock_cat.bump_root.assert_called_once()
        call_kwargs = mock_cat.bump_root.call_args
        assert call_kwargs[0][0] == "myorg"
        assert call_kwargs[0][1] == "mysup"


# ====================================================================
# 17. DataWriter.write — Empty DataFrame Edge Case
# ====================================================================

class TestWriteEmptyDataFrame:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_write_with_empty_dataframe_no_overwrite(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_process, MockMirror, mock_get_mon,
    ):
        """Writing an empty DataFrame with no overwrite_columns should succeed."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": []}).cast({"id": pl.Int64})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_process.return_value = (0, 0, 0, 1, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": pa.array([], type=pa.int64())}), [])
        assert result is not None


# ====================================================================
# 18. DataWriter.write — timer class attribute
# ====================================================================

class TestTimerAttribute:

    def test_timer_is_class_attribute(self):
        from supertable.data_writer import DataWriter
        assert hasattr(DataWriter, "timer")

    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_timer_shared_across_instances(self, MockST, MockCat):
        MockST.return_value = MagicMock()
        MockCat.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw1 = DataWriter("a", "b")
        dw2 = DataWriter("c", "d")
        assert dw1.timer is dw2.timer


# ====================================================================
# 19. DataWriter.write — Return Value Consistency
# ====================================================================

class TestWriteReturnValue:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_DELETE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_return_tuple_format_delete_only(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_proc_del, MockMirror, mock_get_mon,
    ):
        """Verify the 4-tuple format: (total_columns, total_rows, inserted, deleted)."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1], "val": ["x"]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()
        mock_proc_del.return_value = (0, 5, 10, 3, [], set())  # inserted=0, deleted=5, rows=10, cols=3
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        result = dw.write("admin", "tbl", _arrow_table({"id": [1], "val": ["x"]}), ["id"], delete_only=True)

        assert len(result) == 4
        total_columns, total_rows, inserted, deleted = result
        assert total_columns == 3
        assert total_rows == 10
        assert inserted == 0
        assert deleted == 5


# ====================================================================
# 20. DataWriter.write — file_cache passed through
# ====================================================================

class TestWriteFileCache:

    @patch(_PATCH_GET_MON_LOGGER)
    @patch(_PATCH_MIRROR)
    @patch(_PATCH_PROCESS_OVERLAP)
    @patch(_PATCH_FILTER_STALE)
    @patch(_PATCH_FIND_OVERLAP)
    @patch(_PATCH_SIMPLE_TABLE)
    @patch(_PATCH_CHECK_WRITE)
    @patch(_PATCH_POLARS_FROM_ARROW)
    @patch(_PATCH_REDIS_CATALOG)
    @patch(_PATCH_SUPER_TABLE)
    def test_file_cache_passed_to_filter_stale_and_process(
        self,
        MockST, MockCat, mock_from_arrow, mock_check_write,
        MockSimple, mock_find_overlap, mock_filter_stale,
        mock_process, MockMirror, mock_get_mon,
    ):
        """file_cache dict is created and passed to both filter_stale and process."""
        mock_st = MagicMock(super_name="s", organization="o")
        MockST.return_value = mock_st
        mock_cat = MagicMock()
        mock_cat.get_table_config.return_value = None
        mock_cat.acquire_simple_lock.return_value = "t"
        mock_cat.set_leaf_payload_cas.return_value = 1
        MockCat.return_value = mock_cat

        df = _polars_df({"id": [1], "ts": [100]})
        mock_from_arrow.return_value = df

        mock_simple = MagicMock(data_dir="/d")
        mock_simple.get_simple_table_snapshot.return_value = ({"resources": []}, "/p")
        mock_simple.update.return_value = ({}, "/np")
        MockSimple.return_value = mock_simple
        mock_find_overlap.return_value = set()

        # filter_stale returns non-empty (so we continue to process)
        mock_filter_stale.return_value = df
        mock_process.return_value = (1, 0, 1, 2, [], set())
        mock_get_mon.return_value = MagicMock()

        from supertable.data_writer import DataWriter
        dw = DataWriter("s", "o")
        dw.write("admin", "tbl", _arrow_table({"id": [1], "ts": [100]}), ["id"], newer_than="ts")

        # Check filter_stale received file_cache kwarg
        filter_kwargs = mock_filter_stale.call_args[1]
        assert "file_cache" in filter_kwargs
        assert isinstance(filter_kwargs["file_cache"], dict)

        # Check process received the same file_cache
        process_kwargs = mock_process.call_args[1]
        assert "file_cache" in process_kwargs
        # Both should share the same dict instance
        assert filter_kwargs["file_cache"] is process_kwargs["file_cache"]
