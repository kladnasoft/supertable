"""
Tests for ``DataReader._assert_targets_exist`` — the read-path pre-flight
that refuses to bootstrap missing tables.

Covers:
  1. _assert_targets_exist (unit)
     - empty physical_tables list → no-op
     - super exists + leaf exists → no-op
     - super missing → SuperTableNotFoundError with coordinates
     - leaf missing → TableNotFoundError with coordinates
     - duplicate (super, simple) tuples deduped (one EXISTS call each)
     - blank super_name / simple_name skipped
  2. DataReader.execute (integration)
     - missing super → returns (empty_df, Status.ERROR, "SuperTable not found: …")
     - missing simple → returns (empty_df, Status.ERROR, "Table not found: …")
"""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.data_reader import DataReader, Status  # noqa: E402
from supertable.errors import (  # noqa: E402
    SuperTableNotFoundError,
    TableNotFoundError,
)


_MOD = "supertable.data_reader"
_P_GET_STORAGE = f"{_MOD}.get_storage"
_P_SQL_PARSER = f"{_MOD}.SQLParser"
_P_REDIS_CATALOG = f"{_MOD}.RedisCatalog"
_P_RESTRICT_READ = f"{_MOD}.restrict_read_access"


def _td(super_name: str = "sup", simple_name: str = "tbl"):
    """Build a TableDefinition-like object — just needs the two attrs."""
    obj = MagicMock()
    obj.super_name = super_name
    obj.simple_name = simple_name
    return obj


def _build_reader(organization: str = "org", super_name: str = "sup"):
    """Build a DataReader via __new__, skipping the real __init__."""
    r = DataReader.__new__(DataReader)
    r.organization = organization
    r.super_name = super_name
    r.query = "SELECT 1 FROM tbl"
    r._log_ctx = ""
    return r


# ===========================================================================
# 1. _assert_targets_exist — unit tests
# ===========================================================================

class TestAssertTargetsExistUnit:

    def test_empty_list_is_noop(self):
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            reader._assert_targets_exist([])
            MockCat.assert_not_called()

    def test_none_is_noop(self):
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            reader._assert_targets_exist(None)
            MockCat.assert_not_called()

    def test_existing_targets_pass(self):
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            cat = MagicMock()
            cat.root_exists.return_value = True
            cat.leaf_exists.return_value = True
            MockCat.return_value = cat

            reader._assert_targets_exist([_td("sup", "tbl")])

            cat.root_exists.assert_called_once_with("org", "sup")
            cat.leaf_exists.assert_called_once_with("org", "sup", "tbl")

    def test_missing_super_raises(self):
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            cat = MagicMock()
            cat.root_exists.return_value = False
            MockCat.return_value = cat

            with pytest.raises(SuperTableNotFoundError) as exc_info:
                reader._assert_targets_exist([_td("ghost", "tbl")])

            assert exc_info.value.organization == "org"
            assert exc_info.value.super_name == "ghost"
            # leaf_exists never reached because super check failed first
            cat.leaf_exists.assert_not_called()

    def test_missing_leaf_raises(self):
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            cat = MagicMock()
            cat.root_exists.return_value = True
            cat.leaf_exists.return_value = False
            MockCat.return_value = cat

            with pytest.raises(TableNotFoundError) as exc_info:
                reader._assert_targets_exist([_td("sup", "ghost_table")])

            assert exc_info.value.organization == "org"
            assert exc_info.value.super_name == "sup"
            assert exc_info.value.simple_name == "ghost_table"

    def test_duplicate_targets_deduped(self):
        """Aliasing in SQL can produce duplicates — we shouldn't fire two
        EXISTS calls for the same (super, simple)."""
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            cat = MagicMock()
            cat.root_exists.return_value = True
            cat.leaf_exists.return_value = True
            MockCat.return_value = cat

            reader._assert_targets_exist([
                _td("sup", "tbl"),
                _td("sup", "tbl"),  # duplicate
                _td("sup", "tbl"),  # duplicate
            ])

            assert cat.root_exists.call_count == 1
            assert cat.leaf_exists.call_count == 1

    def test_distinct_targets_each_checked(self):
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            cat = MagicMock()
            cat.root_exists.return_value = True
            cat.leaf_exists.return_value = True
            MockCat.return_value = cat

            reader._assert_targets_exist([
                _td("sup_a", "t1"),
                _td("sup_b", "t2"),
                _td("sup_a", "t3"),
            ])

            # sup_a checked twice (different simples), sup_b once
            assert cat.root_exists.call_count == 3
            assert cat.leaf_exists.call_count == 3

    def test_blank_segments_skipped(self):
        """Defensive: blank super or simple names are skipped, not
        passed to the catalog (where they'd violate _safe segments)."""
        reader = _build_reader()
        with patch(_P_REDIS_CATALOG) as MockCat:
            cat = MagicMock()
            cat.root_exists.return_value = True
            cat.leaf_exists.return_value = True
            MockCat.return_value = cat

            reader._assert_targets_exist([
                _td("", "tbl"),       # blank super
                _td("sup", ""),       # blank simple
                _td(None, "tbl"),     # None super
                _td("sup", None),     # None simple
                _td("real_sup", "real_tbl"),
            ])

            cat.root_exists.assert_called_once_with("org", "real_sup")
            cat.leaf_exists.assert_called_once_with("org", "real_sup", "real_tbl")


# ===========================================================================
# 2. DataReader.execute integration — pre-flight surfaces as Status.ERROR
# ===========================================================================

class TestExecuteSurfacesNotFoundError:

    def _patch_execute_minimal(self, *, root_exists: bool, leaf_exists: bool):
        """Context manager (via helper) — patches just enough to drive
        execute() through the pre-flight check."""
        # Note: we yield the catalog mock so the test can inspect it
        ...

    @patch(_P_RESTRICT_READ)
    @patch(_P_REDIS_CATALOG)
    @patch(_P_SQL_PARSER)
    @patch(_P_GET_STORAGE)
    def test_missing_super_returns_error_status(
        self, mock_get_storage, MockParser, MockCat, mock_restrict,
    ):
        mock_get_storage.return_value = MagicMock()
        parser = MagicMock()
        parser.get_table_tuples.return_value = []
        parser.get_physical_tables.return_value = [_td("ghost_sup", "tbl")]
        parser.original_query = "SELECT 1 FROM ghost_sup.tbl"
        MockParser.return_value = parser

        cat = MagicMock()
        cat.root_exists.return_value = False  # super missing
        MockCat.return_value = cat

        mock_restrict.return_value = {}

        from supertable.engine.engine_enum import Engine
        reader = DataReader("orig_sup", "org", "SELECT 1 FROM ghost_sup.tbl")
        df, status, message = reader.execute(role_name="r", engine=Engine.AUTO)

        assert status == Status.ERROR
        assert "SuperTable not found" in (message or "")
        assert "ghost_sup" in (message or "")
        assert df.empty

    @patch(_P_RESTRICT_READ)
    @patch(_P_REDIS_CATALOG)
    @patch(_P_SQL_PARSER)
    @patch(_P_GET_STORAGE)
    def test_missing_leaf_returns_error_status(
        self, mock_get_storage, MockParser, MockCat, mock_restrict,
    ):
        mock_get_storage.return_value = MagicMock()
        parser = MagicMock()
        parser.get_table_tuples.return_value = []
        parser.get_physical_tables.return_value = [_td("sup", "ghost_tbl")]
        parser.original_query = "SELECT 1 FROM sup.ghost_tbl"
        MockParser.return_value = parser

        cat = MagicMock()
        cat.root_exists.return_value = True
        cat.leaf_exists.return_value = False
        MockCat.return_value = cat

        mock_restrict.return_value = {}

        from supertable.engine.engine_enum import Engine
        reader = DataReader("sup", "org", "SELECT 1 FROM sup.ghost_tbl")
        df, status, message = reader.execute(role_name="r", engine=Engine.AUTO)

        assert status == Status.ERROR
        assert "Table not found" in (message or "")
        assert "ghost_tbl" in (message or "")
        assert df.empty
