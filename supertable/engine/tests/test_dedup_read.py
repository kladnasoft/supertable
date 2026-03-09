"""
Comprehensive dedup-on-read tests.

Tests are organized around these concerns:

  1. DedupViewDef dataclass — defaults and field access
  2. Reflection — dedup_views field coexists with rbac_views
  3. create_dedup_view (DuckDB) — real DuckDB connection tests:
     - dedup correctness (latest row per key wins)
     - visible_columns restricts output columns
     - SELECT * hides __rn__ and __timestamp__
     - composite primary keys
     - single PK column
     - ties broken deterministically
  4. DuckDB Pinned executor — dedup view creation, cleanup, column visibility
  5. DuckDB Transient executor — column extension for dedup, dedup view creation
  6. Spark executor — _spark_create_dedup_view, DESCRIBE introspection, cleanup
  7. DataReader — dedup config lookup from Redis, DedupViewDef population

All DuckDB SQL tests use a REAL in-memory DuckDB connection.
Executor integration tests use mocks.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Dict, List
from unittest.mock import MagicMock, patch, PropertyMock

import duckdb
import pandas as pd
import pytest

from supertable.data_classes import (
    DedupViewDef,
    Reflection,
    RbacViewDef,
    SuperSnapshot,
    TableDefinition,
)
from supertable.engine.engine_common import create_dedup_view, quote_if_needed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _duckdb_con() -> duckdb.DuckDBPyConnection:
    """Create a fresh in-memory DuckDB connection."""
    return duckdb.connect()


def _seed_table(con: duckdb.DuckDBPyConnection, table_name: str, data: dict) -> None:
    """Create a table from a dict of {col: [values]}."""
    cols_sql = []
    for col, vals in data.items():
        if isinstance(vals[0], str):
            dtype = "VARCHAR"
        elif isinstance(vals[0], int):
            dtype = "INTEGER"
        elif isinstance(vals[0], float):
            dtype = "DOUBLE"
        elif isinstance(vals[0], datetime):
            dtype = "TIMESTAMP"
        else:
            dtype = "VARCHAR"
        cols_sql.append(f"{quote_if_needed(col)} {dtype}")

    con.execute(f"CREATE TABLE {table_name} ({', '.join(cols_sql)});")
    columns = list(data.keys())
    rows = list(zip(*data.values()))
    for row in rows:
        placeholders = ", ".join("?" for _ in row)
        con.execute(f"INSERT INTO {table_name} VALUES ({placeholders});", list(row))


def _ts(hours_ago: int) -> datetime:
    """Return a UTC datetime `hours_ago` hours in the past."""
    return datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc) - timedelta(hours=hours_ago)


def _query_view(con: duckdb.DuckDBPyConnection, view_name: str) -> pd.DataFrame:
    """Fetch all rows from a view and return as a DataFrame."""
    return con.execute(f"SELECT * FROM {view_name}").fetchdf()


def _view_columns(con: duckdb.DuckDBPyConnection, view_name: str) -> List[str]:
    """Return the column names from a view."""
    df = _query_view(con, view_name)
    return list(df.columns)


# ===========================================================================
# 1. DedupViewDef dataclass
# ===========================================================================

class TestDedupViewDef:

    def test_defaults(self):
        d = DedupViewDef()
        assert d.primary_keys == []
        assert d.order_column == "__timestamp__"
        assert d.visible_columns == []

    def test_custom_fields(self):
        d = DedupViewDef(
            primary_keys=["id", "region"],
            order_column="updated_at",
            visible_columns=["id", "value"],
        )
        assert d.primary_keys == ["id", "region"]
        assert d.order_column == "updated_at"
        assert d.visible_columns == ["id", "value"]


# ===========================================================================
# 2. Reflection dedup_views field
# ===========================================================================

class TestReflectionDedupField:

    def test_dedup_views_default_empty(self):
        r = Reflection(
            storage_type="test",
            reflection_bytes=0,
            total_reflections=0,
            supers=[],
        )
        assert r.dedup_views == {}

    def test_dedup_views_coexists_with_rbac(self):
        r = Reflection(
            storage_type="test",
            reflection_bytes=0,
            total_reflections=0,
            supers=[],
            rbac_views={"t1": RbacViewDef(allowed_columns=["a"])},
            dedup_views={"t1": DedupViewDef(primary_keys=["id"])},
        )
        assert "t1" in r.rbac_views
        assert "t1" in r.dedup_views


# ===========================================================================
# 3. create_dedup_view — real DuckDB tests
# ===========================================================================

class TestCreateDedupViewBasicDedup:
    """Core dedup correctness: latest row per key wins."""

    def test_single_pk_keeps_latest(self):
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1, 2, 2],
            "value": ["old_1", "new_1", "old_2", "new_2"],
            "__timestamp__": [_ts(2), _ts(0), _ts(3), _ts(1)],
        })

        dedup = DedupViewDef(primary_keys=["id"])
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 2
        row1 = df[df["id"] == 1].iloc[0]
        row2 = df[df["id"] == 2].iloc[0]
        assert row1["value"] == "new_1"
        assert row2["value"] == "new_2"

    def test_composite_pk_keeps_latest(self):
        con = _duckdb_con()
        _seed_table(con, "src", {
            "region": ["US", "US", "EU", "EU"],
            "id": [1, 1, 1, 1],
            "value": ["us_old", "us_new", "eu_old", "eu_new"],
            "__timestamp__": [_ts(4), _ts(0), _ts(3), _ts(1)],
        })

        dedup = DedupViewDef(primary_keys=["region", "id"])
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 2
        us = df[df["region"] == "US"].iloc[0]
        eu = df[df["region"] == "EU"].iloc[0]
        assert us["value"] == "us_new"
        assert eu["value"] == "eu_new"

    def test_unique_rows_all_kept(self):
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
            "__timestamp__": [_ts(2), _ts(1), _ts(0)],
        })

        dedup = DedupViewDef(primary_keys=["id"])
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 3

    def test_three_dupes_keeps_latest_only(self):
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1, 1],
            "value": ["oldest", "middle", "newest"],
            "__timestamp__": [_ts(3), _ts(2), _ts(0)],
        })

        dedup = DedupViewDef(primary_keys=["id"])
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 1
        assert df.iloc[0]["value"] == "newest"

    def test_custom_order_column(self):
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1],
            "value": ["old", "new"],
            "updated_at": [_ts(5), _ts(0)],
        })

        dedup = DedupViewDef(primary_keys=["id"], order_column="updated_at")
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 1
        assert df.iloc[0]["value"] == "new"


class TestCreateDedupViewColumnVisibility:
    """Verify that only allowed columns are exposed."""

    def test_select_star_hides_rn_and_timestamp(self):
        """visible_columns=[] (SELECT *) should hide __rn__ and __timestamp__."""
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1],
            "value": ["old", "new"],
            "__timestamp__": [_ts(2), _ts(0)],
        })

        dedup = DedupViewDef(primary_keys=["id"], visible_columns=[])
        create_dedup_view(con, "src", "dedup_v", dedup)
        cols = _view_columns(con, "dedup_v")

        assert "id" in cols
        assert "value" in cols
        assert "__rn__" not in cols
        assert "__timestamp__" not in cols

    def test_explicit_columns_only_those_visible(self):
        """visible_columns=["value"] should expose only 'value'."""
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1, 2],
            "region": ["US", "US", "EU"],
            "value": ["old", "new", "only"],
            "__timestamp__": [_ts(2), _ts(0), _ts(1)],
        })

        dedup = DedupViewDef(
            primary_keys=["id"],
            visible_columns=["value"],
        )
        create_dedup_view(con, "src", "dedup_v", dedup)
        cols = _view_columns(con, "dedup_v")

        assert cols == ["value"]

        df = _query_view(con, "dedup_v")
        assert len(df) == 2

    def test_visible_includes_pk_and_data(self):
        """User SELECTs both a PK column and a data column."""
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1],
            "category": ["A", "A"],
            "amount": [100, 200],
            "__timestamp__": [_ts(2), _ts(0)],
        })

        dedup = DedupViewDef(
            primary_keys=["id"],
            visible_columns=["id", "amount"],
        )
        create_dedup_view(con, "src", "dedup_v", dedup)
        cols = _view_columns(con, "dedup_v")

        assert set(cols) == {"id", "amount"}

        df = _query_view(con, "dedup_v")
        assert len(df) == 1
        assert df.iloc[0]["amount"] == 200

    def test_visible_columns_excludes_extra_pk_not_requested(self):
        """PK has [id, region] but user only SELECTs [id, value].
        Region and __timestamp__ should be hidden."""
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1],
            "region": ["US", "US"],
            "value": ["old", "new"],
            "__timestamp__": [_ts(2), _ts(0)],
        })

        dedup = DedupViewDef(
            primary_keys=["id", "region"],
            visible_columns=["id", "value"],
        )
        create_dedup_view(con, "src", "dedup_v", dedup)
        cols = _view_columns(con, "dedup_v")

        assert set(cols) == {"id", "value"}
        assert "region" not in cols
        assert "__timestamp__" not in cols
        assert "__rn__" not in cols

    def test_select_star_with_custom_order_column_hides_it(self):
        """Custom order column 'updated_at' should also be hidden from SELECT *."""
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1, 1],
            "value": ["old", "new"],
            "updated_at": [_ts(2), _ts(0)],
        })

        dedup = DedupViewDef(
            primary_keys=["id"],
            order_column="updated_at",
            visible_columns=[],
        )
        create_dedup_view(con, "src", "dedup_v", dedup)
        cols = _view_columns(con, "dedup_v")

        assert "id" in cols
        assert "value" in cols
        assert "updated_at" not in cols
        assert "__rn__" not in cols


class TestCreateDedupViewEdgeCases:

    def test_empty_table(self):
        con = _duckdb_con()
        con.execute(
            "CREATE TABLE src (id INTEGER, value VARCHAR, __timestamp__ TIMESTAMP);"
        )

        dedup = DedupViewDef(primary_keys=["id"])
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 0

    def test_single_row(self):
        con = _duckdb_con()
        _seed_table(con, "src", {
            "id": [1],
            "value": ["only"],
            "__timestamp__": [_ts(0)],
        })

        dedup = DedupViewDef(primary_keys=["id"])
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 1
        assert df.iloc[0]["value"] == "only"

    def test_all_same_timestamp_keeps_one(self):
        """When all rows for a key have the same timestamp, exactly one is kept."""
        con = _duckdb_con()
        ts = _ts(0)
        _seed_table(con, "src", {
            "id": [1, 1, 1],
            "value": ["a", "b", "c"],
            "__timestamp__": [ts, ts, ts],
        })

        dedup = DedupViewDef(primary_keys=["id"])
        create_dedup_view(con, "src", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert len(df) == 1

    def test_dedup_view_on_top_of_view(self):
        """Dedup view can be layered on top of another view (simulating RBAC)."""
        con = _duckdb_con()
        _seed_table(con, "base", {
            "id": [1, 1, 2, 2],
            "value": ["old_1", "new_1", "old_2", "new_2"],
            "secret": ["s1", "s2", "s3", "s4"],
            "__timestamp__": [_ts(2), _ts(0), _ts(3), _ts(1)],
        })

        # Simulate RBAC view: hide 'secret' column
        con.execute(
            "CREATE VIEW rbac_v AS SELECT id, value, __timestamp__ FROM base;"
        )

        dedup = DedupViewDef(
            primary_keys=["id"],
            visible_columns=["id", "value"],
        )
        create_dedup_view(con, "rbac_v", "dedup_v", dedup)
        df = _query_view(con, "dedup_v")

        assert set(df.columns) == {"id", "value"}
        assert len(df) == 2
        row1 = df[df["id"] == 1].iloc[0]
        assert row1["value"] == "new_1"


# ===========================================================================
# 4. DuckDB Pinned — executor-level dedup
# ===========================================================================

class TestDuckDBPinnedDedup:
    """Test dedup view creation and cleanup in pinned executor."""

    @patch("supertable.engine.duckdb_pinned.create_dedup_view")
    @patch("supertable.engine.duckdb_pinned.create_rbac_view")
    @patch("supertable.engine.duckdb_pinned.rewrite_query_with_hashed_tables")
    @patch("supertable.engine.duckdb_pinned.create_reflection_view")
    @patch("supertable.engine.duckdb_pinned.configure_httpfs_and_s3")
    @patch("supertable.engine.duckdb_pinned.init_connection")
    def test_dedup_view_created_and_used(
        self,
        mock_init,
        mock_httpfs,
        mock_create_tbl,
        mock_rewrite,
        mock_rbac,
        mock_dedup,
    ):
        from supertable.engine.duckdb_pinned import DuckDBPinned

        # Setup: make the connection execute return a DataFrame
        mock_con = MagicMock()
        mock_con.execute.return_value.fetchdf.return_value = pd.DataFrame({"id": [1]})
        mock_con.execute.return_value.fetchall.return_value = []

        mock_rewrite.return_value = "SELECT * FROM dedup_view"

        pinned = DuckDBPinned(storage=None)
        pinned._con = mock_con
        pinned._httpfs_configured = True

        # Build reflection with one dedup-enabled table
        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="s", simple_name="t", simple_version=1,
                files=["f1.parquet"],
            )],
            dedup_views={"t": DedupViewDef(
                primary_keys=["id"],
                visible_columns=["id", "value"],
            )},
        )

        parser = MagicMock()
        parser.get_table_tuples.return_value = [
            TableDefinition(super_name="s", simple_name="t", alias="t", columns=["id", "value"]),
        ]
        parser.original_query = "SELECT id, value FROM t"

        qm = MagicMock()
        qm.temp_dir = "/tmp"
        qm.query_plan_path = "/tmp/plan.json"

        pinned.execute(
            reflection=reflection,
            parser=parser,
            query_manager=qm,
            timer_capture=lambda x: None,
        )

        # Dedup view was created
        mock_dedup.assert_called_once()
        call_args = mock_dedup.call_args
        dedup_def_arg = call_args[0][3]
        assert dedup_def_arg.primary_keys == ["id"]
        assert dedup_def_arg.visible_columns == ["id", "value"]

        # Dedup view was cleaned up (DROP VIEW called for dedup view names)
        drop_calls = [
            c for c in mock_con.execute.call_args_list
            if isinstance(c[0][0], str) and "DROP VIEW" in c[0][0] and "dedup_" in c[0][0]
        ]
        assert len(drop_calls) >= 1

    @patch("supertable.engine.duckdb_pinned.create_dedup_view")
    @patch("supertable.engine.duckdb_pinned.create_rbac_view")
    @patch("supertable.engine.duckdb_pinned.rewrite_query_with_hashed_tables")
    @patch("supertable.engine.duckdb_pinned.create_reflection_view")
    @patch("supertable.engine.duckdb_pinned.configure_httpfs_and_s3")
    @patch("supertable.engine.duckdb_pinned.init_connection")
    def test_rbac_then_dedup_layering(
        self,
        mock_init,
        mock_httpfs,
        mock_create_tbl,
        mock_rewrite,
        mock_rbac,
        mock_dedup,
    ):
        """Dedup view should be created on top of RBAC view, not raw table."""
        from supertable.engine.duckdb_pinned import DuckDBPinned

        mock_con = MagicMock()
        mock_con.execute.return_value.fetchdf.return_value = pd.DataFrame()
        mock_con.execute.return_value.fetchall.return_value = []
        mock_rewrite.return_value = "SELECT * FROM v"

        pinned = DuckDBPinned(storage=None)
        pinned._con = mock_con
        pinned._httpfs_configured = True

        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="s", simple_name="t", simple_version=1,
                files=["f1.parquet"],
            )],
            rbac_views={"t": RbacViewDef(allowed_columns=["id", "value"])},
            dedup_views={"t": DedupViewDef(
                primary_keys=["id"],
                visible_columns=["id", "value"],
            )},
        )

        parser = MagicMock()
        parser.get_table_tuples.return_value = [
            TableDefinition(super_name="s", simple_name="t", alias="t", columns=["id", "value"]),
        ]
        parser.original_query = "SELECT id, value FROM t"

        qm = MagicMock()
        qm.temp_dir = "/tmp"
        qm.query_plan_path = "/tmp/plan.json"

        pinned.execute(
            reflection=reflection,
            parser=parser,
            query_manager=qm,
            timer_capture=lambda x: None,
        )

        # RBAC view was created first
        mock_rbac.assert_called_once()
        rbac_source = mock_rbac.call_args[0][1]  # base_table_name

        # Dedup view was created on top of RBAC view (not raw table)
        mock_dedup.assert_called_once()
        dedup_source = mock_dedup.call_args[0][1]  # source_table
        assert dedup_source.startswith("rbac_")


# ===========================================================================
# 5. DuckDB Transient — column extension + dedup
# ===========================================================================

class TestDuckDBTransientDedupColumnExtension:
    """Verify the transient engine extends columns for dedup tables."""

    @patch("supertable.engine.duckdb_transient.create_dedup_view")
    @patch("supertable.engine.duckdb_transient.create_reflection_view_with_presign_retry")
    @patch("supertable.engine.duckdb_transient.configure_httpfs_and_s3")
    @patch("supertable.engine.duckdb_transient.init_connection")
    @patch("supertable.engine.duckdb_transient.rewrite_query_with_hashed_tables")
    def test_columns_extended_with_pk_and_timestamp(
        self,
        mock_rewrite,
        mock_init,
        mock_httpfs,
        mock_create_tbl,
        mock_dedup,
    ):
        from supertable.engine.duckdb_transient import DuckDBTransient

        mock_rewrite.return_value = "SELECT value FROM dedup_v"

        transient = DuckDBTransient(storage=None)

        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="s", simple_name="t", simple_version=1,
                files=["f1.parquet"],
            )],
            dedup_views={"t": DedupViewDef(
                primary_keys=["id", "region"],
                visible_columns=["value"],
            )},
        )

        parser = MagicMock()
        parser.get_table_tuples.return_value = [
            TableDefinition(super_name="s", simple_name="t", alias="t", columns=["value"]),
        ]
        parser.original_query = "SELECT value FROM t"

        qm = MagicMock()
        qm.temp_dir = "/tmp"
        qm.query_plan_path = "/tmp/plan.json"

        # The real duckdb.connect() inside execute() needs to work
        with patch("supertable.engine.duckdb_transient.duckdb") as mock_duckdb:
            mock_con = MagicMock()
            mock_con.execute.return_value.fetchdf.return_value = pd.DataFrame({"value": ["x"]})
            mock_duckdb.connect.return_value = mock_con

            transient.execute(
                reflection=reflection,
                parser=parser,
                query_manager=qm,
                timer_capture=lambda x: None,
            )

        # Verify create_reflection_table was called with extended columns
        create_call = mock_create_tbl.call_args
        # args: con, storage, table_name, files, cols, log_prefix
        cols_used = create_call[0][4] if len(create_call[0]) > 4 else create_call[1].get("cols", [])
        # The method is called positionally:
        # create_reflection_view_with_presign_retry(con, self.storage, table_name, files, cols, log_prefix)
        cols_used = create_call[0][4]

        cols_lower = {c.lower() for c in cols_used}
        assert "value" in cols_lower, "Original column preserved"
        assert "id" in cols_lower, "PK column 'id' added"
        assert "region" in cols_lower, "PK column 'region' added"
        assert "__timestamp__" in cols_lower, "Order column added"

    @patch("supertable.engine.duckdb_transient.create_dedup_view")
    @patch("supertable.engine.duckdb_transient.create_reflection_view_with_presign_retry")
    @patch("supertable.engine.duckdb_transient.configure_httpfs_and_s3")
    @patch("supertable.engine.duckdb_transient.init_connection")
    @patch("supertable.engine.duckdb_transient.rewrite_query_with_hashed_tables")
    def test_no_duplicate_columns_when_pk_already_selected(
        self,
        mock_rewrite,
        mock_init,
        mock_httpfs,
        mock_create_tbl,
        mock_dedup,
    ):
        """If user already SELECTs the PK column, it should not be added twice."""
        from supertable.engine.duckdb_transient import DuckDBTransient

        mock_rewrite.return_value = "SELECT id, value FROM dedup_v"

        transient = DuckDBTransient(storage=None)

        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="s", simple_name="t", simple_version=1,
                files=["f1.parquet"],
            )],
            dedup_views={"t": DedupViewDef(
                primary_keys=["id"],
                visible_columns=["id", "value"],
            )},
        )

        parser = MagicMock()
        parser.get_table_tuples.return_value = [
            TableDefinition(super_name="s", simple_name="t", alias="t", columns=["id", "value"]),
        ]
        parser.original_query = "SELECT id, value FROM t"

        qm = MagicMock()
        qm.temp_dir = "/tmp"
        qm.query_plan_path = "/tmp/plan.json"

        with patch("supertable.engine.duckdb_transient.duckdb") as mock_duckdb:
            mock_con = MagicMock()
            mock_con.execute.return_value.fetchdf.return_value = pd.DataFrame()
            mock_duckdb.connect.return_value = mock_con

            transient.execute(
                reflection=reflection,
                parser=parser,
                query_manager=qm,
                timer_capture=lambda x: None,
            )

        cols_used = mock_create_tbl.call_args[0][4]
        # "id" should appear only once
        cols_lower = [c.lower() for c in cols_used]
        assert cols_lower.count("id") == 1, f"PK 'id' duplicated: {cols_used}"
        # __timestamp__ should be added
        assert "__timestamp__" in cols_lower

    @patch("supertable.engine.duckdb_transient.create_dedup_view")
    @patch("supertable.engine.duckdb_transient.create_reflection_view_with_presign_retry")
    @patch("supertable.engine.duckdb_transient.configure_httpfs_and_s3")
    @patch("supertable.engine.duckdb_transient.init_connection")
    @patch("supertable.engine.duckdb_transient.rewrite_query_with_hashed_tables")
    def test_select_star_no_column_extension(
        self,
        mock_rewrite,
        mock_init,
        mock_httpfs,
        mock_create_tbl,
        mock_dedup,
    ):
        """SELECT * means td.columns=[] — no column extension needed,
        all columns are already loaded."""
        from supertable.engine.duckdb_transient import DuckDBTransient

        mock_rewrite.return_value = "SELECT * FROM dedup_v"

        transient = DuckDBTransient(storage=None)

        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="s", simple_name="t", simple_version=1,
                files=["f1.parquet"],
            )],
            dedup_views={"t": DedupViewDef(
                primary_keys=["id"],
                visible_columns=[],
            )},
        )

        parser = MagicMock()
        parser.get_table_tuples.return_value = [
            TableDefinition(super_name="s", simple_name="t", alias="t", columns=[]),
        ]
        parser.original_query = "SELECT * FROM t"

        qm = MagicMock()
        qm.temp_dir = "/tmp"
        qm.query_plan_path = "/tmp/plan.json"

        with patch("supertable.engine.duckdb_transient.duckdb") as mock_duckdb:
            mock_con = MagicMock()
            mock_con.execute.return_value.fetchdf.return_value = pd.DataFrame()
            mock_duckdb.connect.return_value = mock_con

            transient.execute(
                reflection=reflection,
                parser=parser,
                query_manager=qm,
                timer_capture=lambda x: None,
            )

        cols_used = mock_create_tbl.call_args[0][4]
        # Empty list = SELECT * — no extension
        assert cols_used == []


# ===========================================================================
# 6. Spark — _spark_create_dedup_view
# ===========================================================================

class TestSparkCreateDedupView:

    def test_explicit_visible_columns(self):
        from supertable.engine.spark_thrift import _spark_create_dedup_view

        cursor = MagicMock()
        dedup = DedupViewDef(
            primary_keys=["id"],
            visible_columns=["id", "value"],
        )

        _spark_create_dedup_view(cursor, "src_table", "dedup_v", dedup)

        sql = cursor.execute.call_args[0][0]
        assert "ROW_NUMBER()" in sql
        assert "PARTITION BY `id`" in sql
        assert "ORDER BY `__timestamp__` DESC" in sql
        assert "sub.__rn__ = 1" in sql
        # Only visible columns in outer SELECT
        assert "sub.`id`" in sql
        assert "sub.`value`" in sql
        # Internal columns NOT in outer SELECT
        assert "sub.`__timestamp__`" not in sql

    def test_select_star_uses_describe(self):
        from supertable.engine.spark_thrift import _spark_create_dedup_view

        cursor = MagicMock()
        # DESCRIBE returns column schema
        cursor.execute.return_value = None
        cursor.fetchall.return_value = [
            ("id",), ("value",), ("region",), ("__timestamp__",),
        ]

        dedup = DedupViewDef(
            primary_keys=["id"],
            visible_columns=[],
        )

        _spark_create_dedup_view(cursor, "src_table", "dedup_v", dedup)

        # First call: DESCRIBE src_table
        first_call = cursor.execute.call_args_list[0][0][0]
        assert "DESCRIBE src_table" in first_call

        # Second call: CREATE VIEW
        create_sql = cursor.execute.call_args_list[1][0][0]
        assert "sub.`id`" in create_sql
        assert "sub.`value`" in create_sql
        assert "sub.`region`" in create_sql
        # __timestamp__ and __rn__ should be excluded
        assert "sub.`__timestamp__`" not in create_sql

    def test_describe_failure_falls_back_to_star(self):
        from supertable.engine.spark_thrift import _spark_create_dedup_view

        cursor = MagicMock()
        # DESCRIBE raises an exception
        call_count = [0]
        def side_effect(sql):
            call_count[0] += 1
            if "DESCRIBE" in sql:
                raise RuntimeError("DESCRIBE not supported")
            return None
        cursor.execute.side_effect = side_effect

        dedup = DedupViewDef(
            primary_keys=["id"],
            visible_columns=[],
        )

        _spark_create_dedup_view(cursor, "src_table", "dedup_v", dedup)

        # Should fallback to sub.*
        create_sql = cursor.execute.call_args_list[-1][0][0]
        assert "sub.*" in create_sql

    def test_custom_order_column(self):
        from supertable.engine.spark_thrift import _spark_create_dedup_view

        cursor = MagicMock()
        dedup = DedupViewDef(
            primary_keys=["id"],
            order_column="updated_at",
            visible_columns=["id", "value"],
        )

        _spark_create_dedup_view(cursor, "src_table", "dedup_v", dedup)

        sql = cursor.execute.call_args[0][0]
        assert "ORDER BY `updated_at` DESC" in sql

    def test_composite_pk(self):
        from supertable.engine.spark_thrift import _spark_create_dedup_view

        cursor = MagicMock()
        dedup = DedupViewDef(
            primary_keys=["region", "id"],
            visible_columns=["value"],
        )

        _spark_create_dedup_view(cursor, "src_table", "dedup_v", dedup)

        sql = cursor.execute.call_args[0][0]
        assert "PARTITION BY `region`, `id`" in sql
        assert "sub.`value`" in sql


class TestSparkExecutorDedupIntegration:
    """Test dedup integration in SparkThriftExecutor.execute()."""

    @patch("supertable.engine.spark_thrift._spark_create_dedup_view")
    @patch("supertable.engine.spark_thrift._spark_create_rbac_view")
    @patch("supertable.engine.spark_thrift._spark_create_parquet_view")
    @patch("supertable.engine.spark_thrift._configure_spark_s3")
    @patch("supertable.engine.spark_thrift._spark_rewrite_query")
    def test_dedup_view_created_and_cleaned_up(
        self,
        mock_rewrite,
        mock_s3,
        mock_parquet,
        mock_rbac,
        mock_dedup,
    ):
        from supertable.engine.spark_thrift import SparkThriftExecutor

        mock_rewrite.return_value = "SELECT id FROM dedup_v"

        executor = SparkThriftExecutor(storage=None)
        executor.catalog = MagicMock()
        executor.catalog.select_spark_cluster.return_value = {
            "cluster_id": "c1",
            "thrift_host": "localhost",
            "thrift_port": 10000,
            "name": "test",
        }

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = [("id",)]
        mock_cursor.fetchall.return_value = [(1,)]
        mock_conn.cursor.return_value = mock_cursor

        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="s", simple_name="t", simple_version=1,
                files=["s3://bucket/f1.parquet"],
            )],
            dedup_views={"t": DedupViewDef(
                primary_keys=["id"],
                visible_columns=["id"],
            )},
        )

        parser = MagicMock()
        parser.get_table_tuples.return_value = [
            TableDefinition(super_name="s", simple_name="t", alias="t", columns=["id"]),
        ]
        parser.original_query = "SELECT id FROM t"

        qm = MagicMock()

        with patch.object(executor, "_get_connection", return_value=mock_conn):
            executor.execute(
                reflection=reflection,
                parser=parser,
                query_manager=qm,
                timer_capture=lambda x: None,
            )

        # Dedup view was created
        mock_dedup.assert_called_once()
        dedup_def_arg = mock_dedup.call_args[0][3]
        assert dedup_def_arg.primary_keys == ["id"]

        # Cleanup: DROP VIEW was called for the dedup view
        drop_calls = [
            c for c in mock_cursor.execute.call_args_list
            if isinstance(c[0][0], str) and "DROP VIEW" in c[0][0] and "dedup_" in c[0][0]
        ]
        assert len(drop_calls) >= 1


# ===========================================================================
# 7. DataReader — dedup config lookup
# ===========================================================================

class TestDataReaderDedupLookup:

    @patch("supertable.data_reader.RedisCatalog")
    @patch("supertable.data_reader.Executor")
    @patch("supertable.data_reader.DataEstimator")
    @patch("supertable.data_reader.QueryPlanManager")
    @patch("supertable.data_reader.extend_execution_plan")
    @patch("supertable.data_reader.restrict_read_access")
    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_dedup_config_populates_reflection(
        self,
        mock_parser_cls,
        mock_storage,
        mock_restrict,
        mock_extend,
        mock_qm_cls,
        mock_estimator_cls,
        mock_executor_cls,
        mock_catalog_cls,
    ):
        from supertable.data_reader import DataReader

        # Setup parser
        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = [
            TableDefinition(super_name="sup", simple_name="events", alias="events", columns=["id", "value"]),
        ]
        mock_parser.original_query = "SELECT id, value FROM events"
        mock_parser_cls.return_value = mock_parser

        # Setup estimator
        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="sup", simple_name="events", simple_version=1,
                files=["f1.parquet"],
            )],
        )
        mock_estimator = MagicMock()
        mock_estimator.estimate.return_value = reflection
        mock_estimator_cls.return_value = mock_estimator

        # Setup executor
        mock_executor = MagicMock()
        mock_executor.execute.return_value = (pd.DataFrame({"id": [1]}), "duckdb_pinned")
        mock_executor_cls.return_value = mock_executor

        # Setup catalog: table has dedup_on_read enabled
        mock_catalog = MagicMock()
        mock_catalog.get_table_config.return_value = {
            "dedup_on_read": True,
            "primary_keys": ["id"],
        }
        mock_catalog_cls.return_value = mock_catalog

        # Setup query plan manager
        mock_qm = MagicMock()
        mock_qm.query_id = "test_qid"
        mock_qm.query_hash = "test_hash"
        mock_qm_cls.return_value = mock_qm

        # Setup storage
        mock_storage.return_value = MagicMock()

        reader = DataReader("sup", "org", "SELECT id, value FROM events")
        reader.execute(role_name="admin")

        # Verify catalog was queried for table config
        mock_catalog.get_table_config.assert_called_once_with("org", "sup", "events")

        # Verify dedup_views was populated on the reflection passed to executor
        executor_call = mock_executor.execute.call_args
        refl_arg = executor_call[1].get("reflection") or executor_call[0][1]
        assert "events" in refl_arg.dedup_views
        assert refl_arg.dedup_views["events"].primary_keys == ["id"]
        assert refl_arg.dedup_views["events"].visible_columns == ["id", "value"]

    @patch("supertable.data_reader.RedisCatalog")
    @patch("supertable.data_reader.Executor")
    @patch("supertable.data_reader.DataEstimator")
    @patch("supertable.data_reader.QueryPlanManager")
    @patch("supertable.data_reader.extend_execution_plan")
    @patch("supertable.data_reader.restrict_read_access")
    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_no_dedup_config_leaves_empty(
        self,
        mock_parser_cls,
        mock_storage,
        mock_restrict,
        mock_extend,
        mock_qm_cls,
        mock_estimator_cls,
        mock_executor_cls,
        mock_catalog_cls,
    ):
        from supertable.data_reader import DataReader

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = [
            TableDefinition(super_name="sup", simple_name="events", alias="events", columns=[]),
        ]
        mock_parser.original_query = "SELECT * FROM events"
        mock_parser_cls.return_value = mock_parser

        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="sup", simple_name="events", simple_version=1,
                files=["f1.parquet"],
            )],
        )
        mock_estimator = MagicMock()
        mock_estimator.estimate.return_value = reflection
        mock_estimator_cls.return_value = mock_estimator

        mock_executor = MagicMock()
        mock_executor.execute.return_value = (pd.DataFrame(), "duckdb_pinned")
        mock_executor_cls.return_value = mock_executor

        # No dedup config
        mock_catalog = MagicMock()
        mock_catalog.get_table_config.return_value = None
        mock_catalog_cls.return_value = mock_catalog

        mock_qm = MagicMock()
        mock_qm.query_id = "test_qid"
        mock_qm.query_hash = "test_hash"
        mock_qm_cls.return_value = mock_qm

        mock_storage.return_value = MagicMock()

        reader = DataReader("sup", "org", "SELECT * FROM events")
        reader.execute(role_name="admin")

        refl_arg = mock_executor.execute.call_args[1].get("reflection") or mock_executor.execute.call_args[0][1]
        assert refl_arg.dedup_views == {}

    @patch("supertable.data_reader.RedisCatalog")
    @patch("supertable.data_reader.Executor")
    @patch("supertable.data_reader.DataEstimator")
    @patch("supertable.data_reader.QueryPlanManager")
    @patch("supertable.data_reader.extend_execution_plan")
    @patch("supertable.data_reader.restrict_read_access")
    @patch("supertable.data_reader.get_storage")
    @patch("supertable.data_reader.SQLParser")
    def test_redis_failure_degrades_gracefully(
        self,
        mock_parser_cls,
        mock_storage,
        mock_restrict,
        mock_extend,
        mock_qm_cls,
        mock_estimator_cls,
        mock_executor_cls,
        mock_catalog_cls,
    ):
        from supertable.data_reader import DataReader

        mock_parser = MagicMock()
        mock_parser.get_table_tuples.return_value = [
            TableDefinition(super_name="sup", simple_name="events", alias="events", columns=["id"]),
        ]
        mock_parser.original_query = "SELECT id FROM events"
        mock_parser_cls.return_value = mock_parser

        reflection = Reflection(
            storage_type="test",
            reflection_bytes=100,
            total_reflections=1,
            supers=[SuperSnapshot(
                super_name="sup", simple_name="events", simple_version=1,
                files=["f1.parquet"],
            )],
        )
        mock_estimator = MagicMock()
        mock_estimator.estimate.return_value = reflection
        mock_estimator_cls.return_value = mock_estimator

        mock_executor = MagicMock()
        mock_executor.execute.return_value = (pd.DataFrame({"id": [1]}), "duckdb_pinned")
        mock_executor_cls.return_value = mock_executor

        # Redis throws an exception
        mock_catalog = MagicMock()
        mock_catalog.get_table_config.side_effect = RuntimeError("Redis down")
        mock_catalog_cls.return_value = mock_catalog

        mock_qm = MagicMock()
        mock_qm.query_id = "test_qid"
        mock_qm.query_hash = "test_hash"
        mock_qm_cls.return_value = mock_qm

        mock_storage.return_value = MagicMock()

        reader = DataReader("sup", "org", "SELECT id FROM events")
        result_df, status, message = reader.execute(role_name="admin")

        # Should succeed without dedup — graceful degradation
        from supertable.data_reader import Status
        assert status == Status.OK
        refl_arg = mock_executor.execute.call_args[1].get("reflection") or mock_executor.execute.call_args[0][1]
        assert refl_arg.dedup_views == {}
