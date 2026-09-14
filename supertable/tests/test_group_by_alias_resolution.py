"""STREAD-014: GROUP BY may name a SELECT alias.

``SELECT EXTRACT(year FROM event_date) AS yr, COUNT(*) FROM t GROUP BY yr`` is
valid SQL that DuckDB runs, but ``yr`` was collected as a required physical
column and the estimator refused the query with
``Missing required column(s): warehouse.temporal_dates: yr`` before execution.

WHY THIS IS NOT JUST "ADD exp.Group TO THE ALIAS SCOPE"

GROUP BY takes an alias *and* a column, and where a name is both, DuckDB binds
the **column**. Verified against the engine:

    CREATE TABLE t (a INT, b INT);
    SELECT a AS b, COUNT(*) FROM t GROUP BY b;
    -- Binder Error: column "a" must appear in the GROUP BY clause

If ``GROUP BY b`` had resolved to the alias (``a``) that query would be legal.
It is not, which proves the physical column won. So simply skipping such names
the way ORDER BY aliases are skipped would drop a genuinely-read column from the
projection, and the reflection would be built without it.

The name is therefore neither required nor ignorable until something knows the
table's schema. The parser records it in ``TableDefinition.optional_columns``;
the missing-column check ignores it, and the executor adds it to the projection
only if the snapshot declares it.
"""

from __future__ import annotations

import duckdb
import pytest

from supertable.data_classes import SuperSnapshot, TableDefinition
from supertable.engine.data_estimator import get_missing_columns
from supertable.utils.sql_parser import SQLParser

SUPER = "warehouse"


def _table(sql, name):
    return next(
        t for t in SQLParser(SUPER, sql, "duckdb").get_physical_tables()
        if t.simple_name == name
    )


class TestDuckDbPrecedenceIsWhatWeModel:
    """Pin the engine behaviour the design rests on.

    If a DuckDB upgrade ever changed GROUP BY to prefer the alias, the
    optional-column machinery would be unnecessary — this test is what would
    tell us.

    THESE ARE NOT SUPERTABLE QUERIES. They run against a bare in-memory DuckDB
    connection, with no snapshot, no reflection and no admission control, because
    the question being asked is "what does the SQL engine do?" — the premise the
    parser's behaviour is derived from. Deriving it from SuperTable's own read
    path would make the check circular.

    That is also why DDL appears in a read-path test file. Submitted to
    SuperTable, every statement below is refused: ``CREATE`` by
    ``assert_read_only`` ("CREATE is not permitted on the read path"), and the
    CREATE-then-SELECT pair by the single-statement rule ("only a single
    statement may be submitted; found 2") — which refuses the *request*, so the
    SELECT never runs either. See test_read_path_admission.py for those.
    """

    def test_a_group_by_name_binds_the_column_not_the_alias(self):
        con = duckdb.connect()
        con.execute("CREATE TABLE t (a INT, b INT)")
        con.execute("INSERT INTO t VALUES (1,10),(1,20),(2,10),(2,20)")
        with pytest.raises(duckdb.BinderException, match="must appear in the GROUP BY"):
            con.execute("SELECT a AS b, COUNT(*) FROM t GROUP BY b").fetchall()

    def test_grouping_by_the_expression_or_ordinal_is_unambiguous(self):
        """The documented workarounds, which must keep working."""
        con = duckdb.connect()
        con.execute("CREATE TABLE t (a INT, b INT)")
        con.execute("INSERT INTO t VALUES (1,10),(1,20),(2,10),(2,20)")
        assert sorted(con.execute(
            "SELECT a AS b, COUNT(*) n FROM t GROUP BY a"
        ).fetchall()) == [(1, 2), (2, 2)]
        assert sorted(con.execute(
            "SELECT a AS b, COUNT(*) n FROM t GROUP BY 1"
        ).fetchall()) == [(1, 2), (2, 2)]


class TestTheAliasIsNoLongerDemandedAsAColumn:

    @pytest.mark.parametrize("sql,table,required", [
        # The reported date/datetime shape.
        ("SELECT EXTRACT(year FROM event_date) AS yr, COUNT(*) AS n, MIN(event_date) AS f"
         " FROM temporal_dates WHERE event_date IS NOT NULL GROUP BY yr ORDER BY yr",
         "temporal_dates", ["event_date"]),
        # A plain rename.
        ("SELECT grp AS category, COUNT(*) AS n FROM numbers GROUP BY category",
         "numbers", ["grp"]),
        # An expression alias.
        ("SELECT grp + 1 AS category, COUNT(*) AS n FROM numbers GROUP BY category",
         "numbers", ["grp"]),
        # A quoted alias, including one with a space.
        ('SELECT EXTRACT(year FROM event_date) AS "Calendar Year", COUNT(*) AS n'
         ' FROM temporal_dates GROUP BY "Calendar Year"', "temporal_dates", ["event_date"]),
        # Inside a grouping extension.
        ("SELECT grp AS category, COUNT(*) AS n FROM numbers GROUP BY ROLLUP(category)",
         "numbers", ["grp"]),
    ])
    def test_required_columns_hold_only_real_ones(self, sql, table, required):
        assert _table(sql, table).columns == sorted(required)

    @pytest.mark.parametrize("sql,table,optional", [
        ("SELECT EXTRACT(year FROM event_date) AS yr, COUNT(*) AS n FROM temporal_dates"
         " GROUP BY yr", "temporal_dates", ["yr"]),
        ("SELECT grp AS category, COUNT(*) AS n FROM numbers GROUP BY ROLLUP(category)",
         "numbers", ["category"]),
    ])
    def test_the_alias_is_recorded_as_conditional(self, sql, table, optional):
        """Recorded, not discarded — the executor still needs to consider it."""
        assert _table(sql, table).optional_columns == sorted(optional)

    def test_the_missing_column_check_ignores_conditional_names(self):
        """This check is what rejected the query."""
        definition = TableDefinition(
            super_name=SUPER, simple_name="temporal_dates", alias="temporal_dates",
            columns=["event_date"], optional_columns=["yr"],
        )
        snapshot = SuperSnapshot(
            super_name=SUPER, simple_name="temporal_dates", simple_version=1,
            files=["a.parquet"], columns={"event_date"},
        )
        assert get_missing_columns([definition], [snapshot]) == []

    def test_a_genuinely_missing_required_column_is_still_rejected(self):
        """Criterion: keep genuinely missing columns rejected."""
        definition = TableDefinition(
            super_name=SUPER, simple_name="temporal_dates", alias="temporal_dates",
            columns=["event_date", "nope"],
        )
        snapshot = SuperSnapshot(
            super_name=SUPER, simple_name="temporal_dates", simple_version=1,
            files=["a.parquet"], columns={"event_date"},
        )
        missing = get_missing_columns([definition], [snapshot])
        assert missing and missing[0][2] == {"nope"}


class TestPhysicalPrecedenceIsPreserved:

    def test_a_shadowed_name_stays_available_to_the_projection(self):
        """`SELECT grp + 1 AS grp ... GROUP BY grp` must still read `grp`.

        Here the projection references it anyway, so it is required outright —
        the case that proves the fix does not blanket-discard matching names.
        """
        definition = _table(
            "SELECT grp + 1 AS grp, COUNT(*) AS n FROM numbers GROUP BY grp ORDER BY grp",
            "numbers",
        )
        assert "grp" in definition.columns

    def test_a_shadowed_name_not_otherwise_referenced_is_conditional(self):
        """The shape DuckDB resolves to the physical column.

        `status` is projected, `grp` is not — so `grp` reaches the executor as
        conditional, and the executor adds it because the schema has it. Were
        it merely skipped, the reflection would omit it and `GROUP BY grp`
        could not bind.
        """
        definition = _table(
            "SELECT status AS grp, COUNT(*) AS n FROM orders GROUP BY grp", "orders",
        )
        assert definition.columns == ["status"]
        assert definition.optional_columns == ["grp"]


class TestUnrelatedShapesAreUnchanged:

    @pytest.mark.parametrize("sql,table,expected", [
        # GROUP BY a real column that is not an alias: still required.
        ("SELECT region, COUNT(*) AS n FROM orders GROUP BY region", "orders", ["region"]),
        # The working alternatives from the issue.
        ("SELECT EXTRACT(year FROM event_date) AS yr, COUNT(*) AS n FROM temporal_dates"
         " GROUP BY EXTRACT(year FROM event_date)", "temporal_dates", ["event_date"]),
        ("SELECT EXTRACT(year FROM event_date) AS yr, COUNT(*) AS n FROM temporal_dates"
         " GROUP BY 1", "temporal_dates", ["event_date"]),
        # DuckDB's GROUP BY ALL.
        ("SELECT region, COUNT(*) FROM orders GROUP BY ALL", "orders", ["region"]),
        # ORDER BY / HAVING aliases were already handled and must stay so.
        ("SELECT grp AS category, COUNT(*) AS n FROM numbers GROUP BY grp ORDER BY category",
         "numbers", ["grp"]),
        ("SELECT grp AS category, COUNT(*) AS n FROM numbers GROUP BY grp HAVING COUNT(*) > 1",
         "numbers", ["grp"]),
    ])
    def test_columns(self, sql, table, expected):
        assert _table(sql, table).columns == sorted(expected)

    @pytest.mark.parametrize("sql,table", [
        ("SELECT region, COUNT(*) AS n FROM orders GROUP BY region", "orders"),
        ("SELECT oid, qty FROM orders", "orders"),
        ("SELECT * FROM orders", "orders"),
    ])
    def test_nothing_conditional_when_no_alias_is_grouped(self, sql, table):
        """The field stays empty for ordinary queries, so it costs nothing."""
        assert _table(sql, table).optional_columns == []

    def test_select_star_leaves_nothing_conditional(self):
        """[] already means every column, so there is nothing left to resolve."""
        definition = _table(
            "SELECT *, grp AS category FROM numbers GROUP BY category", "numbers",
        )
        assert definition.columns == []
        assert definition.optional_columns == []
