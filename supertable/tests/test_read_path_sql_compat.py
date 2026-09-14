"""Read-path SQL compatibility and row-bound handling.

Seals the query-admission findings of the 2026-09-14 LOCAL/DuckDB read-path
audit. Each class names the issue it closes and, where the behaviour is a
deliberate translation rather than a plain fix, states why the translation is
sound.

Every construct asserted here was first verified against DuckDB itself — the
queries are valid SQL that the engine executes; only the parser in front of it
refused them.
"""

from __future__ import annotations

import duckdb
import pytest
import sqlglot

from supertable.data_reader import _ensure_sql_limit, _top_level_row_bound
from supertable.system_query import CommandKind, classify_query
from supertable.utils.sql_compat import (
    normalize_read_sql,
    parse_read_one,
    parse_read_statements,
)
from supertable.utils.sql_parser import SQLParser

SUPER = "warehouse"


def _parses(sql: str) -> bool:
    try:
        sqlglot.parse(sql, read="duckdb")
        return True
    except Exception:
        return False


def _bound(sql: str):
    return _top_level_row_bound(parse_read_one(sql))


def _cols(sql: str):
    return {
        t.simple_name: t.columns
        for t in SQLParser(SUPER, sql, "duckdb").get_physical_tables()
    }


@pytest.fixture(scope="module")
def con():
    """A DuckDB connection holding the shapes these tests translate.

    The point of comparing against DuckDB is that a rewrite is only acceptable
    if the engine returns the same rows for the original and the rewritten form.
    """
    c = duckdb.connect()
    c.execute("CREATE TABLE t (region VARCHAR, status VARCHAR, flag BOOLEAN)")
    c.execute(
        "INSERT INTO t VALUES ('eu','a',TRUE),('eu','b',FALSE),"
        "('us','a',NULL),('us','b',TRUE)"
    )
    return c


# ---------------------------------------------------------------------------
# STREAD-002 / STREAD-003 — the default row bound
# ---------------------------------------------------------------------------

class TestRowBoundIsACapNotAnOverride:
    """The default limit is a ceiling; a caller's own bound must survive it."""

    @pytest.mark.parametrize("sql", [
        "SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid LIMIT 3",
        "SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid FETCH FIRST 3 ROWS ONLY",
        "SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid LIMIT (3)",
        "SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid LIMIT /* three */ 3",
    ])
    def test_a_bound_under_the_cap_is_left_alone(self, sql):
        """Every spelling of "three rows" must still mean three rows.

        The old regex recognised only ``LIMIT <digits>`` at the end of the text.
        Each of these forms therefore looked limitless, got ``LIMIT <cap>``
        appended, and the appended clause replaced the caller's bound — a query
        asking for 3 rows returned all 8. Nothing in the response said so,
        which is why this is sealed per spelling rather than in general.
        """
        assert _ensure_sql_limit(sql, 100_000) == sql

    @pytest.mark.parametrize("sql,offset", [
        ("SELECT oid FROM orders ORDER BY oid OFFSET 2 ROWS FETCH NEXT 3 ROWS ONLY", 2),
        ("SELECT oid FROM orders ORDER BY oid LIMIT (3) OFFSET (2)", 2),
    ])
    def test_offset_forms_keep_both_halves(self, sql, offset):
        """The offset variants lost the limit but kept the offset, returning 6 of 8."""
        assert _ensure_sql_limit(sql, 100_000) == sql

    def test_limit_zero_is_a_real_bound(self):
        """LIMIT 0 is a deliberate request for no rows, not a missing limit."""
        sql = "SELECT oid FROM orders LIMIT 0"
        assert _ensure_sql_limit(sql, 100) == sql
        assert _bound(sql) == 0

    def test_unbounded_query_gets_the_cap(self):
        assert _bound(_ensure_sql_limit("SELECT oid FROM orders", 100)) == 100

    def test_bound_above_the_cap_is_clamped(self):
        assert _bound(_ensure_sql_limit("SELECT oid FROM orders LIMIT 999999", 100)) == 100

    def test_bound_equal_to_the_cap_is_untouched(self):
        sql = "SELECT oid FROM orders LIMIT 100"
        assert _ensure_sql_limit(sql, 100) == sql

    def test_limit_all_means_unbounded_so_it_is_capped(self):
        """LIMIT ALL is DuckDB for "no limit", so the cap applies to it."""
        assert _bound(_ensure_sql_limit("SELECT oid FROM orders LIMIT ALL", 100)) == 100

    @pytest.mark.parametrize("sql", [
        "SELECT * FROM (SELECT oid FROM orders LIMIT 5) x",
        "WITH h AS (SELECT oid FROM orders LIMIT 5) SELECT * FROM h",
    ])
    def test_inner_bounds_are_not_the_outer_bound(self, sql):
        capped = _ensure_sql_limit(sql, 100)
        assert _bound(capped) == 100
        assert "LIMIT 5" in capped.upper()

    @pytest.mark.parametrize("sql", [
        "SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid;",
        "SELECT oid FROM orders;   \n  ",
        "SELECT oid FROM orders; -- trailing note",
        "SELECT oid FROM orders;;",
    ])
    def test_a_terminated_statement_stays_parseable(self, sql):
        """STREAD-003: the bound must land inside the statement, not after it.

        ``SELECT ...;`` used to become ``SELECT ...;\\nLIMIT 100000`` — a limit
        clause after the terminator, which does not parse. A terminal semicolon
        is how most clients send SQL; it is not a malformed query, so it must
        not be turned into one (and must not be rejected either).
        """
        out = _ensure_sql_limit(sql, 100)
        assert _parses(out)
        assert _bound(out) == 100

    def test_capping_is_idempotent(self):
        once = _ensure_sql_limit("SELECT oid FROM orders", 100)
        assert _ensure_sql_limit(once, 100) == once

    @pytest.mark.parametrize("sql", [
        "INSERT INTO orders (oid) VALUES (999)",
        "CREATE TABLE t (a INT)",
        "DROP TABLE orders",
        "DESCRIBE orders",
    ])
    def test_a_non_read_is_never_rewritten(self, sql):
        """Related to STREAD-012.

        The helper used to append a limit to anything its caller mislabelled as
        a SELECT, which turned a clean "INSERT is not permitted" into a parse
        error and "CREATE" into "COMMAND". Leaving non-reads untouched lets the
        reader classify them and report the specific reason.
        """
        assert _ensure_sql_limit(sql, 100) == sql


# ---------------------------------------------------------------------------
# STREAD-006 — IS UNKNOWN
# ---------------------------------------------------------------------------

class TestIsUnknown:

    @pytest.mark.parametrize("sql", [
        "SELECT region, flag IS UNKNOWN AS r FROM t ORDER BY region",
        "SELECT region, flag IS NOT UNKNOWN AS r FROM t ORDER BY region",
        "SELECT region FROM t WHERE flag IS UNKNOWN",
    ])
    def test_duckdb_agrees_with_the_rewrite(self, con, sql):
        """``IS UNKNOWN`` and ``IS NULL`` must return identical rows.

        They are the same predicate — UNKNOWN is SQL's three-valued NULL for
        booleans — and SQLGlot 27.x normalises the former to the latter, which
        is the rewrite performed here for the pinned 26.x range. Asserted
        against the engine rather than by inspecting the text, so the claim is
        about results and not about spelling.
        """
        normalized = normalize_read_sql(sql)
        assert _parses(normalized)
        assert con.execute(sql).fetchall() == con.execute(normalized).fetchall()

    @pytest.mark.parametrize("sql", [
        "SELECT 'IS UNKNOWN' AS lit FROM t",
        "SELECT region FROM t WHERE status = 'flag IS UNKNOWN'",
        "SELECT region FROM t -- flag IS UNKNOWN",
        "SELECT region FROM t /* flag IS UNKNOWN */",
    ])
    def test_literals_and_comments_are_not_syntax(self, sql):
        """The rewrite is token-driven precisely so these are untouched.

        A regex over raw SQL would corrupt every one of these.
        """
        assert normalize_read_sql(sql) == sql


# ---------------------------------------------------------------------------
# STREAD-005 — grouping extensions with a row bound
# ---------------------------------------------------------------------------

class TestGroupingExtensionsWithRowBounds:

    @pytest.mark.parametrize("sql", [
        "SELECT region, status, COUNT(*) n FROM t GROUP BY ROLLUP (region, status) LIMIT 100",
        "SELECT region, status, COUNT(*) n FROM t GROUP BY CUBE (region, status) LIMIT 100",
        "SELECT region, COUNT(*) n FROM t GROUP BY GROUPING SETS ((region),(status),()) LIMIT 100",
        "SELECT region, GROUPING(region) g, COUNT(*) n FROM t GROUP BY ROLLUP (region, status) LIMIT 100",
    ])
    def test_duckdb_agrees_with_the_wrap(self, con, sql):
        """The rewrite must not change which rows come back.

        SQLGlot's GROUP BY parser consumes ``LIMIT`` as another grouping
        expression and then fails on the count. This is an upstream bug in both
        26.33 and 27.29, so it is shimmed rather than waited out: the row bound
        is moved outside a wrapping subquery, which parses.
        """
        normalized = normalize_read_sql(sql)
        assert _parses(normalized)
        assert sorted(map(str, con.execute(sql).fetchall())) == \
               sorted(map(str, con.execute(normalized).fetchall()))

    @pytest.mark.parametrize("sql", [
        "SELECT region, COUNT(*) n FROM t GROUP BY ROLLUP (region) LIMIT 3 OFFSET 1",
        "SELECT region, COUNT(*) n FROM t GROUP BY CUBE (region) OFFSET 1",
    ])
    def test_offset_forms_parse_and_slice_the_same_multiset(self, con, sql):
        """OFFSET cannot be expressed re-parseably in place, so it is wrapped too.

        A row bound without ORDER BY selects an implementation-defined subset,
        so only the row count and the source multiset are asserted — not which
        rows. This is not a weakening: the parse failure only occurs when the
        bound *immediately* follows the grouping extension, and any intervening
        clause (ORDER BY included) parses fine, so a query reaching this shim
        provably had no ordering for the wrap to disturb.
        """
        normalized = normalize_read_sql(sql)
        assert _parses(normalized)
        unbounded = {
            str(r) for r in con.execute(
                sql.split(" LIMIT ")[0].split(" OFFSET ")[0]
            ).fetchall()
        }
        rows = con.execute(normalized).fetchall()
        assert len(rows) == len(con.execute(sql).fetchall())
        assert {str(r) for r in rows} <= unbounded

    @pytest.mark.parametrize("sql", [
        "SELECT region FROM t GROUP BY ROLLUP (region, status) ORDER BY region LIMIT 5",
        "SELECT region FROM t GROUP BY ROLLUP (region, status) HAVING COUNT(*) > 0 LIMIT 5",
        "SELECT region FROM t GROUP BY region LIMIT 5",
        "SELECT region FROM t GROUP BY ROLLUP (region)",
        "SELECT * FROM (SELECT region FROM t GROUP BY ROLLUP(region)) x LIMIT 2",
    ])
    def test_queries_that_already_parse_are_returned_verbatim(self, sql):
        """The wrap is a last resort, applied only after a real parse failure.

        A query that parses must come back byte-for-byte, or this shim would be
        quietly reformatting the whole read path.
        """
        assert normalize_read_sql(sql) == sql

    def test_the_grouping_shim_also_covers_the_appended_cap(self):
        """STREAD-005 x STREAD-002.

        These queries passed the four native modes and failed only through the
        public helper, because the helper's own appended limit created the
        unparseable shape. Adding the bound must therefore re-normalize.
        """
        out = _ensure_sql_limit(
            "SELECT region, COUNT(*) n FROM t GROUP BY ROLLUP (region, status)", 100
        )
        assert _parses(out)
        assert _bound(out) == 100


# ---------------------------------------------------------------------------
# STREAD-008 — trailing comments
# ---------------------------------------------------------------------------

class TestTrailingComments:

    @pytest.mark.parametrize("sql", [
        "SELECT oid FROM orders; -- terminated named-table query",
        "SELECT oid FROM orders; /* terminated */",
        "SELECT oid FROM orders;",
        "SELECT oid FROM orders;;",
        "-- leading note\nSELECT oid FROM orders",
    ])
    def test_a_comment_is_not_a_statement(self, sql):
        """A trailing comment parsed as its own Semicolon node and was counted.

        ``SELECT ...; -- note`` was refused with "found 2".
        """
        assert len(parse_read_statements(sql)) == 1
        assert classify_query(sql, SUPER).kind is CommandKind.SELECT

    @pytest.mark.parametrize("sql", [
        "SELECT oid FROM orders; SELECT qty FROM orders",
        "SELECT oid FROM orders; DROP TABLE orders",
        "SELECT oid FROM orders; -- note\nSELECT qty FROM orders",
    ])
    def test_a_real_chain_is_still_refused(self, sql):
        """The reason the count exists: one request is one statement.

        Including the case where a comment sits between two real statements —
        dropping comment nodes must not drop the statements around them.
        """
        with pytest.raises(ValueError, match="single statement"):
            classify_query(sql, SUPER)

    def test_a_ddl_prelude_does_not_smuggle_its_select_through(self):
        """A setup statement followed by a read is refused as one request.

        Worth pinning explicitly because this shape looks harmless — the DDL
        creates a scratch table and the SELECT only reads it. But admission
        judges the *request*, not each statement: it is rejected for having two
        statements, so the SELECT never executes either. Statement chaining is
        how an injected payload arrives, which is why the count exists at all.

        (The same text run against a bare DuckDB connection does work — that is
        how the GROUP BY precedence premise in
        test_group_by_alias_resolution.py is established. It is not a read-path
        query, and this test is the boundary between the two.)
        """
        chained = "CREATE TABLE t (a INT, b INT);\nSELECT a AS b, COUNT(*) FROM t GROUP BY b"
        with pytest.raises(ValueError, match="single statement"):
            classify_query(chained, SUPER)
        # And on its own the DDL is refused for what it is.
        with pytest.raises(ValueError, match="CREATE is not permitted"):
            classify_query("CREATE TABLE t (a INT, b INT)", SUPER)


# ---------------------------------------------------------------------------
# STREAD-007 — LIMIT ALL
# ---------------------------------------------------------------------------

class TestLimitAllIsNotAColumn:

    def test_limit_all_is_not_a_required_column(self):
        """``Missing required column(s): warehouse.orders: ALL``.

        SQLGlot has no node for ``LIMIT ALL`` and parses the bare word as
        ``Limit(expression=Column(ALL))``; the column sweep then demanded a
        column named ALL from the table.
        """
        assert _cols("SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid LIMIT ALL") \
            == {"orders": ["oid", "qty"]}

    @pytest.mark.parametrize("sql,expected", [
        ("SELECT oid FROM orders LIMIT 5", ["oid"]),
        ("SELECT oid FROM orders LIMIT 5 OFFSET 2", ["oid"]),
        # DuckDB's own ALL forms: SQLGlot models these properly and never emits
        # a Column for them, so they were never affected — pinned so a future
        # change to the row-bound skip cannot start swallowing real columns.
        ("SELECT region, COUNT(*) FROM orders GROUP BY ALL", ["region"]),
        ("SELECT oid, qty FROM orders ORDER BY ALL", ["oid", "qty"]),
    ])
    def test_ordinary_row_bounds_and_duckdb_all_forms(self, sql, expected):
        assert _cols(sql) == {"orders": expected}

    @pytest.mark.parametrize("sql", [
        "SELECT oid, qty FROM orders WHERE oid <= 8 ORDER BY oid LIMIT ALL",
        "SELECT oid, qty FROM orders limit all",
    ])
    def test_the_clause_is_removed_not_just_ignored(self, sql):
        """Keeping the column analysis quiet was not enough.

        ``LIMIT ALL`` means unbounded, and SQLGlot renders its ``Column(ALL)``
        node back out as ``LIMIT "ALL"`` — a *quoted identifier*. So once the
        table-hashing rewrite round-tripped the query, DuckDB failed to bind a
        column named ALL even though the estimator no longer demanded one: the
        clause is corrupted by the reparse, not by the column analysis.
        Dropping it is what ``LIMIT ALL`` asks for anyway.
        """
        normalized = normalize_read_sql(sql)
        assert "LIMIT" not in normalized.upper()
        assert _parses(normalized)
        # Round-tripping it the way the rewrite does must stay executable.
        assert '"ALL"' not in sqlglot.parse_one(normalized, read="duckdb").sql(dialect="duckdb")

    @pytest.mark.parametrize("sql", [
        # SQLGlot models these properly; they are not row bounds.
        "SELECT region, COUNT(*) FROM orders GROUP BY ALL",
        "SELECT oid, qty FROM orders ORDER BY ALL",
        # A literal that merely spells the keywords.
        "SELECT oid FROM orders WHERE note = 'LIMIT ALL'",
        # A subquery's own bound is not the top-level one.
        "SELECT * FROM (SELECT oid FROM orders LIMIT ALL) x",
        "SELECT oid FROM orders LIMIT 5",
    ])
    def test_only_a_top_level_limit_all_is_stripped(self, sql):
        assert normalize_read_sql(sql) == sql


# ---------------------------------------------------------------------------
# STREAD-013 — CTE column lineage
# ---------------------------------------------------------------------------

class TestCteColumnLineage:

    def test_a_cte_body_column_is_attributed_to_its_physical_table(self):
        """The denied column has to reach the permission check to be refused.

        A CTE name is carried in the alias map (it parses as a Table) but is not
        a real table. Counting it made every unqualified column ambiguous — two
        "tables", so no single owner — and dropped them all, leaving an empty
        list that means *all columns*. RBAC then had nothing specific to refuse
        and a denied column surfaced as a DuckDB binder error from the
        restricted view instead of the column-permission error.
        """
        assert _cols("WITH hidden AS (SELECT oid, note FROM orders) SELECT note FROM hidden") \
            == {"orders": ["note", "oid"]}

    def test_matches_the_equivalent_direct_reference(self):
        """The whole point: the CTE shape must agree with the direct shape."""
        assert _cols("WITH h AS (SELECT oid, note FROM orders) SELECT note FROM h") \
            == _cols("SELECT oid, note FROM orders")

    @pytest.mark.parametrize("sql,expected", [
        ("WITH h AS (SELECT oid FROM orders) SELECT oid FROM h", ["oid"]),
        ("WITH h(a, b) AS (SELECT oid, qty FROM orders) SELECT a FROM h", ["oid", "qty"]),
        ("WITH h AS (SELECT region, COUNT(*) AS n FROM orders GROUP BY region)"
         " SELECT region, n FROM h", ["region"]),
    ])
    def test_allowed_cte_shapes_still_resolve(self, sql, expected):
        assert _cols(sql) == {"orders": expected}

    def test_a_name_the_cte_computes_is_not_a_physical_column(self):
        """``x`` is the CTE's output, not a column of orders.

        Without this guard, resolving unqualified columns through a CTE would
        hand ``x`` to orders and report it missing — trading one wrong error for
        another.
        """
        cols = _cols("WITH h AS (SELECT oid AS x FROM orders) SELECT x FROM h")
        assert "x" not in cols["orders"]

    def test_a_derived_table_output_is_still_not_a_physical_column(self):
        """Pre-existing protection for subqueries, kept alongside the CTE one."""
        cols = _cols(
            "SELECT count(*) FROM (SELECT oid, row_number() OVER () AS rn"
            " FROM orders) w WHERE rn <= 3"
        )
        assert "rn" not in cols["orders"]

    def test_a_column_renamed_inside_a_cte_is_still_read_from_the_table(self):
        """The source of a CTE's alias must reach the reflection.

        ``amount`` is the direct value of an Alias, which the sweep skips as
        "already counted from the SELECT list" — but the projection loop scans
        the *outer* SELECT (``oid, gross``), so it was never counted. It got
        dropped from the column set, the RBAC view was built without it, and
        DuckDB failed with ``Referenced column "amount" not found in FROM
        clause``. The skip now verifies which SELECT actually counted it.
        """
        cols = _cols(
            "WITH allowed AS (SELECT oid, amount AS gross FROM orders)"
            " SELECT oid, gross FROM allowed ORDER BY oid"
        )
        assert cols == {"orders": ["amount", "oid"]}

    @pytest.mark.parametrize("sql,expected", [
        # The aliased value is an expression, so its inputs must come through.
        ("WITH h AS (SELECT qty * 2 AS dbl FROM orders) SELECT dbl FROM h", ["qty"]),
        ("WITH h AS (SELECT region, SUM(amount) AS tot FROM orders GROUP BY region)"
         " SELECT region, tot FROM h", ["amount", "region"]),
        # An alias in the outer SELECT was always counted correctly; pinned so
        # the scope check cannot regress the case it is narrowing.
        ("SELECT oid, amount AS gross FROM orders ORDER BY gross", ["amount", "oid"]),
    ])
    def test_alias_sources_resolve_at_every_level(self, sql, expected):
        assert _cols(sql) == {"orders": expected}


# ---------------------------------------------------------------------------
# STREAD-004 — the rewrite must not reinterpret the query
# ---------------------------------------------------------------------------

class TestRewritePreservesMeaning:
    """The table-hashing rewrite reparses the SQL; it must reparse as DuckDB.

    Parsing with SQLGlot's neutral dialect and *rendering* as DuckDB is not a
    round trip — it silently reinterprets dialect-sensitive functions. This is
    the most dangerous class of bug in the file because the query still runs;
    it just answers a different question.
    """

    def test_date_diff_keeps_its_arguments(self, con):
        """``DATE_DIFF('day', d, DATE '...')`` came back with its args permuted.

        The rewrite produced ``DATE_DIFF('2024-03-15', d, CAST('day' AS DATE))``
        — unit and endpoint swapped, the unit cast to a date — which DuckDB
        then rejected as an invalid date conversion. Had it not errored it
        would have returned a wrong number.
        """
        from supertable.engine.engine_common import rewrite_query_with_hashed_tables

        con.execute("CREATE TABLE events (eid INT, event_date DATE)")
        con.execute(
            "INSERT INTO events VALUES (1,'2024-03-10'),(2,'2024-03-20'),(3,'2024-03-15')"
        )
        con.execute("CREATE TABLE hashed_events AS SELECT * FROM events")

        sql = ("SELECT eid, DATE_DIFF('day', event_date, DATE '2024-03-15') AS result"
               " FROM events ORDER BY eid")
        rewritten = rewrite_query_with_hashed_tables(sql, {"events": "hashed_events"})

        assert "hashed_events" in rewritten
        assert con.execute(rewritten).fetchall() == con.execute(sql).fetchall()
        # Differences on both sides of the comparison date, per the audit.
        assert [r[1] for r in con.execute(rewritten).fetchall()] == [5, -5, 0]

    def test_a_rewrite_with_no_tables_to_map_is_a_noop(self):
        from supertable.engine.engine_common import rewrite_query_with_hashed_tables

        sql = "SELECT eid, DATE_DIFF('day', a, b) FROM events"
        assert rewrite_query_with_hashed_tables(sql, {}) == sql


# ---------------------------------------------------------------------------
# STREAD-009 — table-free SELECTs are refused, consistently and on purpose
# ---------------------------------------------------------------------------

class TestTableFreeQueriesAreAnExplicitCapabilityLimit:
    """Refused deliberately, and the message has to say that.

    Two shapes reached two different errors for one cause: a literal SELECT got
    "No tables found in SQL query." from the parser, while a literal CTE slipped
    past that (a CTE name parses as a Table) and died later in the estimator
    with "No snapshots selected." — a message about internal state.

    This is NOT a feature waiting to be switched on. Admission control requires
    every FROM/JOIN source to be a named table, but a table-free SELECT has no
    FROM, so that rule passes vacuously — and DuckDB's *scalar* file readers
    (``read_text``, ``read_blob``) live in the projection, where nothing else
    checks them. This error is what stops them today, so supporting table-free
    SELECTs requires a positive guard over projection functions first.
    """

    @pytest.mark.parametrize("sql", [
        "SELECT 42 AS answer, CAST(NULL AS INTEGER) AS missing, TRUE AS flag",
        "SELECT 42 AS answer;",
        "SELECT 1 AS a UNION ALL SELECT 2 AS a",
        "SELECT (SELECT 1) AS a",
        "WITH t AS (SELECT 1 AS a) SELECT a FROM t",
        "WITH t(a) AS (SELECT 1) SELECT a FROM t",
    ])
    def test_every_shape_reports_the_same_capability_error(self, sql):
        from supertable.utils.sql_parser import TABLE_FREE_QUERY_ERROR

        with pytest.raises(ValueError) as excinfo:
            SQLParser(SUPER, sql, "duckdb")
        assert str(excinfo.value) == TABLE_FREE_QUERY_ERROR

    def test_the_scalar_file_read_this_error_currently_blocks(self):
        """Why the check cannot simply be deleted.

        This query has no Table node at all, so the named-table rule never
        fires. If table-free SELECTs were admitted without a projection guard,
        this would reach DuckDB and read a local file with the engine's own
        credentials.
        """
        from sqlglot import exp
        from supertable.utils.sql_parser import TABLE_FREE_QUERY_ERROR

        sql = "SELECT read_text('/etc/hostname')"
        assert not list(sqlglot.parse_one(sql, read="duckdb").find_all(exp.Table))
        with pytest.raises(ValueError) as excinfo:
            SQLParser(SUPER, sql, "duckdb")
        assert str(excinfo.value) == TABLE_FREE_QUERY_ERROR

    @pytest.mark.parametrize("sql,tables", [
        ("SELECT oid FROM orders", ["orders"]),
        ("WITH h AS (SELECT oid FROM orders) SELECT * FROM h", ["orders"]),
        # A literal CTE alongside a real one must not trip the check: the query
        # does read a table, so it is admissible.
        ("WITH lit AS (SELECT 1 AS a), h AS (SELECT oid FROM orders)"
         " SELECT h.oid, lit.a FROM h, lit", ["orders"]),
        ("WITH a AS (SELECT oid FROM orders), b AS (SELECT oid FROM a)"
         " SELECT * FROM b", ["orders"]),
    ])
    def test_queries_that_do_read_a_table_are_unaffected(self, sql, tables):
        assert [t.simple_name for t in SQLParser(SUPER, sql, "duckdb").get_physical_tables()] \
            == tables


# ---------------------------------------------------------------------------
# STREAD-012 — a refused query keeps its own diagnostic
# ---------------------------------------------------------------------------

class TestRefusedQueriesKeepTheirDiagnostic:
    """query_sql must not rewrite a query whose classification already failed.

    It used to assume SELECT on a ValueError and add its default limit anyway.
    Nothing was ever executed — this was not a write or an authorization
    bypass — but the caller was told the wrong reason: INSERT, DROP and
    DESCRIBE came back as parse errors about the injected LIMIT, and CREATE
    degraded to "COMMAND is not permitted" because
    ``CREATE TABLE t (a INT)\\nLIMIT 100000`` no longer parses as a CREATE.
    """

    @pytest.mark.parametrize("sql,expected", [
        ("INSERT INTO orders (oid) VALUES (999)", "INSERT is not permitted"),
        ("CREATE TABLE t (a INT)", "CREATE is not permitted"),
        ("DROP TABLE orders", "DROP is not permitted"),
        ("DESCRIBE orders", "DESCRIBE is not permitted"),
        # Not a forbidden verb, but the same failure mode: the original reason
        # has to survive instead of being replaced by one about the rewrite.
        ("SELECT * FROM orders; DROP TABLE orders", "single statement"),
        ("SELECT FROM WHERE ((((", "could not parse"),
    ])
    def test_the_original_refusal_reaches_the_caller_unrewritten(self, sql, expected):
        from unittest.mock import patch

        from supertable.data_reader import Status, query_sql
        from supertable.engine.engine_enum import Engine

        seen = {}

        class _CapturingReader:
            """Stands in for DataReader to record the SQL it was handed."""

            def __init__(self, **kwargs):
                seen["query"] = kwargs["query"]
                self.query_plan_manager = None
                self.plan_stats = None

            def execute(self, **_kwargs):
                try:
                    classify_query(seen["query"], SUPER)
                except ValueError as err:
                    return None, Status.ERROR, str(err)
                return None, Status.ERROR, "admitted — should not happen here"

        with patch("supertable.data_reader.DataReader", _CapturingReader):
            with pytest.raises(RuntimeError, match=expected):
                query_sql("org", SUPER, sql, 100_000, Engine.AUTO, "superadmin")

        assert seen["query"] == sql, (
            "a refused query must reach the reader byte-for-byte; rewriting it "
            "is what replaced its diagnostic"
        )
