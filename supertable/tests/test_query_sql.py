# tests/test_query_sql.py

"""
Unit tests for the MCP-facing query_sql helper in supertable.data_reader.

Covers:
  1. _ensure_sql_limit — LIMIT injection logic
  2. query_sql — NAType sanitization (pandas.NA → None)
"""

import math
from unittest.mock import patch, MagicMock

import pandas as pd
import numpy as np
import pytest

from supertable.data_reader import _ensure_sql_limit


# ---------------------------------------------------------------------------
# 1. _ensure_sql_limit
# ---------------------------------------------------------------------------

class TestEnsureSqlLimitAppends:
    """Cases where a LIMIT should be appended."""

    def test_simple_select(self):
        result = _ensure_sql_limit("SELECT * FROM t", 1000)
        assert result == "SELECT * FROM t\nLIMIT 1000"

    def test_select_with_where(self):
        sql = "SELECT * FROM t WHERE x = 1"
        result = _ensure_sql_limit(sql, 500)
        assert result == f"{sql}\nLIMIT 500"

    def test_select_with_order_by(self):
        sql = "SELECT * FROM t ORDER BY x DESC"
        result = _ensure_sql_limit(sql, 100)
        assert result == f"{sql}\nLIMIT 100"

    def test_select_with_group_by(self):
        sql = "SELECT region, COUNT(*) FROM t GROUP BY region"
        result = _ensure_sql_limit(sql, 200)
        assert result == f"{sql}\nLIMIT 200"

    def test_trailing_semicolon(self):
        """Semicolons should not fool the detection — LIMIT should be appended."""
        sql = "SELECT * FROM t;"
        result = _ensure_sql_limit(sql, 1000)
        assert "LIMIT 1000" in result

    def test_trailing_whitespace(self):
        sql = "SELECT * FROM t   "
        result = _ensure_sql_limit(sql, 1000)
        assert "LIMIT 1000" in result

    def test_subquery_has_limit_but_outer_does_not(self):
        """LIMIT inside a subquery should NOT prevent appending to the outer query."""
        sql = "SELECT * FROM t WHERE id IN (SELECT id FROM u LIMIT 5)"
        result = _ensure_sql_limit(sql, 1000)
        assert result.endswith("LIMIT 1000")

    def test_derived_table_has_limit_but_outer_does_not(self):
        sql = "SELECT * FROM (SELECT * FROM t LIMIT 5) sub"
        result = _ensure_sql_limit(sql, 1000)
        assert result.endswith("LIMIT 1000")

    def test_cte_inner_limit_outer_none(self):
        sql = "WITH cte AS (SELECT * FROM t LIMIT 10) SELECT * FROM cte"
        result = _ensure_sql_limit(sql, 500)
        assert result.endswith("LIMIT 500")

    def test_limit_param_cast_to_int(self):
        """Ensure the limit value is always an integer in the output."""
        result = _ensure_sql_limit("SELECT 1", 99.9)
        assert "LIMIT 99" in result


class TestEnsureSqlLimitPreserves:
    """Cases where the existing LIMIT must be left untouched."""

    def test_explicit_limit(self):
        sql = "SELECT * FROM t LIMIT 10"
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_explicit_limit_lowercase(self):
        sql = "SELECT * FROM t limit 10"
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_explicit_limit_mixed_case(self):
        sql = "SELECT * FROM t Limit 25"
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_limit_with_offset(self):
        sql = "SELECT * FROM t LIMIT 10 OFFSET 20"
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_limit_with_offset_lowercase(self):
        sql = "select * from t limit 50 offset 10"
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_limit_trailing_semicolon(self):
        sql = "SELECT * FROM t LIMIT 10;"
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_limit_trailing_whitespace_and_semicolon(self):
        sql = "SELECT * FROM t LIMIT 10 ;  "
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_order_by_then_limit(self):
        sql = "SELECT * FROM t ORDER BY x LIMIT 100"
        assert _ensure_sql_limit(sql, 1000) == sql

    def test_complex_query_with_limit(self):
        sql = (
            "SELECT a, b, COUNT(*) AS cnt "
            "FROM t "
            "WHERE a > 10 "
            "GROUP BY a, b "
            "HAVING cnt > 1 "
            "ORDER BY cnt DESC "
            "LIMIT 50"
        )
        assert _ensure_sql_limit(sql, 1000) == sql


class TestEnsureSqlLimitEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_sql(self):
        """Empty SQL — still appends LIMIT (engine will raise on execution)."""
        result = _ensure_sql_limit("", 100)
        assert "LIMIT 100" in result

    def test_multiline_sql_no_limit(self):
        sql = "SELECT *\nFROM t\nWHERE x = 1\nORDER BY y"
        result = _ensure_sql_limit(sql, 500)
        assert result.endswith("LIMIT 500")

    def test_multiline_sql_with_limit(self):
        sql = "SELECT *\nFROM t\nWHERE x = 1\nLIMIT 25"
        assert _ensure_sql_limit(sql, 500) == sql

    def test_word_limit_in_column_name_does_not_match(self):
        """A column named 'limit_value' should not be detected as a LIMIT clause."""
        sql = "SELECT limit_value FROM t"
        result = _ensure_sql_limit(sql, 1000)
        assert result.endswith("LIMIT 1000")

    def test_limit_in_string_literal_does_not_match(self):
        """LIMIT inside a string literal isn't at the tail, so outer LIMIT is appended."""
        sql = "SELECT * FROM t WHERE name = 'LIMIT 10'"
        result = _ensure_sql_limit(sql, 1000)
        assert result.endswith("LIMIT 1000")


# ---------------------------------------------------------------------------
# 2. query_sql — NAType sanitization
# ---------------------------------------------------------------------------

class TestQuerySqlNATypeSanitization:
    """
    Verify that query_sql converts pandas NA variants to None
    so the MCP server can JSON-serialize the response.
    """

    @pytest.fixture
    def mock_reader(self):
        """Patch DataReader so we can inject a DataFrame result without real infra."""
        with patch("supertable.data_reader.DataReader") as MockReader, \
             patch("supertable.data_reader.restrict_read_access"):
            yield MockReader

    def _run_query_sql(self, mock_reader, result_df):
        """Helper: configure mock and call query_sql."""
        from supertable.data_reader import query_sql, Status

        instance = MagicMock()
        instance.execute.return_value = (result_df, Status.OK, None)
        mock_reader.return_value = instance

        columns, rows, meta = query_sql(
            organization="org",
            super_name="sup",
            sql="SELECT * FROM t",
            limit=1000,
            engine=MagicMock(),
            user_hash="u",
        )
        return columns, rows, meta

    def test_pandas_na_becomes_none(self, mock_reader):
        df = pd.DataFrame({"a": pd.array([1, pd.NA, 3], dtype="Int64")})
        columns, rows, _ = self._run_query_sql(mock_reader, df)

        assert columns == ["a"]
        # Row with pd.NA should now be Python None
        values = [r[0] for r in rows]
        assert values[0] == 1
        assert values[1] is None
        assert values[2] == 3

    def test_numpy_nan_becomes_none(self, mock_reader):
        df = pd.DataFrame({"x": [1.0, np.nan, 3.0]})
        _, rows, _ = self._run_query_sql(mock_reader, df)

        values = [r[0] for r in rows]
        assert values[1] is None

    def test_nat_becomes_none(self, mock_reader):
        df = pd.DataFrame({"ts": pd.to_datetime(["2024-01-01", pd.NaT, "2024-03-01"])})
        _, rows, _ = self._run_query_sql(mock_reader, df)

        values = [r[0] for r in rows]
        assert values[1] is None

    def test_mixed_na_types(self, mock_reader):
        """DataFrame with multiple columns, each having a different NA flavor."""
        df = pd.DataFrame({
            "int_col": pd.array([1, pd.NA], dtype="Int64"),
            "float_col": [1.0, np.nan],
            "str_col": pd.array(["a", pd.NA], dtype="string"),
        })
        _, rows, _ = self._run_query_sql(mock_reader, df)

        # Second row: all columns should be None
        assert rows[1][0] is None  # int_col
        assert rows[1][1] is None  # float_col
        assert rows[1][2] is None  # str_col

    def test_no_na_passes_through(self, mock_reader):
        """Clean DataFrame should not be altered."""
        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        _, rows, _ = self._run_query_sql(mock_reader, df)

        assert rows == [[1, "x"], [2, "y"], [3, "z"]]

    def test_columns_meta_reflects_original_dtypes(self, mock_reader):
        """Column metadata should use the original dtypes, not post-sanitization."""
        df = pd.DataFrame({"a": pd.array([1, pd.NA], dtype="Int64")})
        _, _, meta = self._run_query_sql(mock_reader, df)

        assert meta[0]["name"] == "a"
        assert "Int64" in meta[0]["type"]

    def test_limit_is_enforced_on_sql(self, mock_reader):
        """Verify that _ensure_sql_limit is applied before execution."""
        from supertable.data_reader import query_sql, Status

        instance = MagicMock()
        instance.execute.return_value = (pd.DataFrame({"a": [1]}), Status.OK, None)
        mock_reader.return_value = instance

        query_sql(
            organization="org",
            super_name="sup",
            sql="SELECT * FROM t",
            limit=500,
            engine=MagicMock(),
            user_hash="u",
        )

        # DataReader should have been constructed with the LIMIT-appended SQL
        call_kwargs = mock_reader.call_args
        constructed_query = call_kwargs[1]["query"] if "query" in (call_kwargs[1] or {}) else call_kwargs[0][2]
        assert "LIMIT 500" in constructed_query