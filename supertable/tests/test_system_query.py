# test_system_query.py
"""Unit tests for supertable/system_query.py — read-path command classifier.

Pure-function tests (no mocks): classify_query maps raw SQL text into one of
SELECT / EXPLAIN / SHOW_STATS, preserving ordinary SELECTs byte-for-byte and
raising ValueError only for recognised-but-malformed EXPLAIN/SHOW STATS forms.
"""

from __future__ import annotations

import pytest

from supertable.system_query import classify_query, CommandKind, SystemCommand


# ---------------------------------------------------------------------------
# SELECT fall-through — everything that isn't EXPLAIN/SHOW STATS
# ---------------------------------------------------------------------------

class TestSelectFallThrough:

    def test_plain_select_preserved_verbatim(self):
        raw = "SELECT id, value FROM s.t WHERE id > 5"
        cmd = classify_query(raw, "default_super")
        assert cmd.kind is CommandKind.SELECT
        assert cmd.sql == raw  # byte-identical, untouched
        assert cmd.explain is False
        assert cmd.explain_options == ""

    def test_leading_whitespace_preserved_in_sql(self):
        raw = "   \n  SELECT 1"
        cmd = classify_query(raw, "ds")
        assert cmd.kind is CommandKind.SELECT
        assert cmd.sql == raw  # raw text passed through, not stripped

    def test_lowercase_select(self):
        cmd = classify_query("select * from t", "ds")
        assert cmd.kind is CommandKind.SELECT

    def test_top_level_with_is_select(self):
        # A bare WITH...SELECT (no EXPLAIN prefix) is an ordinary read.
        raw = "WITH c AS (SELECT 1) SELECT * FROM c"
        cmd = classify_query(raw, "ds")
        assert cmd.kind is CommandKind.SELECT
        assert cmd.sql == raw

    def test_empty_string_is_select(self):
        cmd = classify_query("", "ds")
        assert cmd.kind is CommandKind.SELECT
        assert cmd.sql == ""

    def test_none_is_select(self):
        cmd = classify_query(None, "ds")  # type: ignore[arg-type]
        assert cmd.kind is CommandKind.SELECT
        assert cmd.sql == ""

    def test_whitespace_only_is_select(self):
        cmd = classify_query("   \t\n ", "ds")
        assert cmd.kind is CommandKind.SELECT

    def test_non_select_dml_falls_through_unchanged(self):
        # DELETE/INSERT are NOT special-cased here; they fall through to the
        # SELECT path and are rejected downstream exactly as before.
        raw = "DELETE FROM t WHERE id = 1"
        cmd = classify_query(raw, "ds")
        assert cmd.kind is CommandKind.SELECT
        assert cmd.sql == raw

    def test_show_statsfoo_not_misclassified(self):
        # Word-boundary guard: SHOW STATSFOO is not SHOW STATS.
        cmd = classify_query("SHOW STATSFOO", "ds")
        assert cmd.kind is CommandKind.SELECT

    def test_explain_alone_no_inner_falls_through(self):
        # "EXPLAIN" with no following statement does not match the EXPLAIN rule.
        cmd = classify_query("EXPLAIN", "ds")
        assert cmd.kind is CommandKind.SELECT


# ---------------------------------------------------------------------------
# EXPLAIN [ANALYZE] <select>
# ---------------------------------------------------------------------------

class TestExplain:

    def test_explain_select(self):
        cmd = classify_query("EXPLAIN SELECT * FROM t", "ds")
        assert cmd.kind is CommandKind.EXPLAIN
        assert cmd.explain is True
        assert cmd.explain_options == ""
        assert cmd.sql == "SELECT * FROM t"  # inner only; prefix stripped

    def test_explain_analyze_select(self):
        cmd = classify_query("EXPLAIN ANALYZE SELECT 1", "ds")
        assert cmd.kind is CommandKind.EXPLAIN
        assert cmd.explain is True
        assert cmd.explain_options == "ANALYZE"
        assert cmd.sql == "SELECT 1"

    def test_explain_lowercase(self):
        cmd = classify_query("explain select 1", "ds")
        assert cmd.kind is CommandKind.EXPLAIN
        assert cmd.explain_options == ""

    def test_explain_analyze_mixed_case(self):
        cmd = classify_query("ExPlAiN aNaLyZe SELECT 1", "ds")
        assert cmd.kind is CommandKind.EXPLAIN
        assert cmd.explain_options == "ANALYZE"

    def test_explain_inner_with_cte(self):
        cmd = classify_query("EXPLAIN WITH c AS (SELECT 1) SELECT * FROM c", "ds")
        assert cmd.kind is CommandKind.EXPLAIN
        assert cmd.sql == "WITH c AS (SELECT 1) SELECT * FROM c"

    def test_explain_analyze_with_cte(self):
        cmd = classify_query("EXPLAIN ANALYZE WITH c AS (SELECT 1) SELECT * FROM c", "ds")
        assert cmd.kind is CommandKind.EXPLAIN
        assert cmd.explain_options == "ANALYZE"
        assert cmd.sql.startswith("WITH")

    def test_explain_inner_is_trimmed(self):
        cmd = classify_query("EXPLAIN    SELECT 1   ", "ds")
        assert cmd.sql == "SELECT 1"

    def test_explain_non_select_inner_raises(self):
        with pytest.raises(ValueError, match="EXPLAIN is only supported for SELECT"):
            classify_query("EXPLAIN DELETE FROM t", "ds")

    def test_explain_format_option_rejected(self):
        # DuckDB-style "EXPLAIN (FORMAT JSON) SELECT" — inner doesn't start with
        # SELECT/WITH, so it is rejected (only ANALYZE is supported as an option).
        with pytest.raises(ValueError, match="EXPLAIN is only supported for SELECT"):
            classify_query("EXPLAIN (FORMAT JSON) SELECT 1", "ds")

    def test_explain_insert_inner_raises(self):
        with pytest.raises(ValueError):
            classify_query("EXPLAIN INSERT INTO t VALUES (1)", "ds")


# ---------------------------------------------------------------------------
# SHOW STATS [super.]simple
# ---------------------------------------------------------------------------

class TestShowStats:

    def test_qualified(self):
        cmd = classify_query("SHOW STATS mysuper.mytable", "ds")
        assert cmd.kind is CommandKind.SHOW_STATS
        assert cmd.super_name == "mysuper"
        assert cmd.simple_name == "mytable"

    def test_unqualified_uses_default_super(self):
        cmd = classify_query("SHOW STATS mytable", "default_super")
        assert cmd.kind is CommandKind.SHOW_STATS
        assert cmd.super_name == "default_super"
        assert cmd.simple_name == "mytable"

    def test_lowercase(self):
        cmd = classify_query("show stats s.t", "ds")
        assert cmd.kind is CommandKind.SHOW_STATS
        assert cmd.super_name == "s"
        assert cmd.simple_name == "t"

    def test_trailing_semicolon_stripped(self):
        cmd = classify_query("SHOW STATS s.t;", "ds")
        assert cmd.kind is CommandKind.SHOW_STATS
        assert cmd.simple_name == "t"

    def test_double_quoted_idents_unquoted(self):
        cmd = classify_query('SHOW STATS "my super"."my table"', "ds")
        assert cmd.super_name == "my super"
        assert cmd.simple_name == "my table"

    def test_backtick_idents_unquoted(self):
        cmd = classify_query("SHOW STATS `s`.`t`", "ds")
        assert cmd.super_name == "s"
        assert cmd.simple_name == "t"

    def test_quoted_simple_with_dot(self):
        # A dot inside quotes is part of the identifier, not a separator.
        cmd = classify_query('SHOW STATS "a.b"', "default_super")
        assert cmd.super_name == "default_super"
        assert cmd.simple_name == "a.b"

    def test_extra_whitespace_around_dot(self):
        cmd = classify_query("SHOW STATS  s . t ", "ds")
        assert cmd.super_name == "s"
        assert cmd.simple_name == "t"

    def test_no_table_raises(self):
        with pytest.raises(ValueError, match="SHOW STATS expects a table reference"):
            classify_query("SHOW STATS", "ds")

    def test_three_part_name_raises(self):
        with pytest.raises(ValueError, match="SHOW STATS expects a table reference"):
            classify_query("SHOW STATS a.b.c", "ds")


# ---------------------------------------------------------------------------
# SystemCommand dataclass shape
# ---------------------------------------------------------------------------

class TestSystemCommandShape:

    def test_is_frozen(self):
        cmd = SystemCommand(kind=CommandKind.SELECT)
        with pytest.raises(Exception):
            cmd.sql = "mutated"  # type: ignore[misc]

    def test_select_defaults(self):
        cmd = SystemCommand(kind=CommandKind.SELECT)
        assert cmd.sql == ""
        assert cmd.explain is False
        assert cmd.explain_options == ""
        assert cmd.super_name is None
        assert cmd.simple_name is None
