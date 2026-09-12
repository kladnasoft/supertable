"""Row-level security must work for projected queries (audit H4).

The reflection view projects only the columns the QUERY named
(``duckdb.py`` ``stream``), while the RBAC view above it filters on a column
of the ROLE's choosing (``engine_common.create_rbac_view``).  When the two
disagree DuckDB cannot bind the filter, so a role with a row filter — or a
column mask — could only run ``SELECT *``:

    SELECT sum(amount) FROM orders
      -> Binder Error: Referenced column "region" not found in FROM clause!

It fails closed, so nothing leaked; it simply made the feature unusable.

The fix threads the filter's own columns through the reflection and strips
them again in the RBAC view — the same trick ``create_tombstone_view`` uses
for ``__rowid__``/``__timestamp__``.  Because the strip is what keeps the
widening honest, the masking tests below are as much a part of this suite as
the availability ones: a masked column must stay unreachable through every
query shape.

These run the real ``DuckDBEngine`` against real parquet; nothing about the
view chain is mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import polars as pl
import pyarrow.parquet as pq
import pytest

from supertable.data_classes import Reflection, RbacViewDef, SuperSnapshot
from supertable.engine.duckdb import DuckDBEngine, reset_shared_duckdb_state
from supertable.engine.engine_common import (
    create_rbac_view,
    row_filter_columns,
    widen_projection_for_row_filter,
)
from supertable.utils.sql_parser import SQLParser

SUPER = "s"
TABLE = "orders"
SCHEMA = {"id", "region", "amount", "secret", "__rowid__", "__timestamp__"}

#: Role that may see everything but only the EU rows.
EU_ROWS = RbacViewDef(allowed_columns=["*"], where_clause="\"region\" = 'EU'")
#: Role that may see three of the four columns, no row filter.
MASK_ONLY = RbacViewDef(allowed_columns=["id", "amount", "region"], where_clause="")
#: Role that may see two columns AND only the EU rows — ``secret`` and
#: ``region`` are both denied, and ``region`` is the filter column.
MASK_AND_ROWS = RbacViewDef(
    allowed_columns=["id", "amount"], where_clause="\"region\" = 'EU'",
)


@pytest.fixture(scope="module")
def orders_parquet(tmp_path_factory) -> str:
    path = str(tmp_path_factory.mktemp("rbac_proj") / "orders.parquet")
    pq.write_table(
        pl.DataFrame(
            {
                "id": [1, 2, 3],
                "region": ["EU", "US", "EU"],
                "amount": [10.0, 20.0, 30.0],
                "secret": ["s1", "s2", "s3"],
                "__rowid__": ["r1", "r2", "r3"],
                "__timestamp__": [1, 2, 3],
            }
        ).to_arrow(),
        path,
    )
    return path


def _run(parquet: str, sql: str, rbac: RbacViewDef | None = None):
    """Run *sql* through the real view chain; return (column names, rows)."""
    reset_shared_duckdb_state()
    parser = SQLParser(SUPER, sql, "duckdb")
    reflection = Reflection(
        storage_type="LocalStorage",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot(SUPER, TABLE, 1, [parquet], set(SCHEMA))],
    )
    if rbac is not None:
        reflection.rbac_views = {
            td.alias: rbac for td in parser.get_table_tuples()
        }
    qm = MagicMock()
    qm.temp_dir = str(parquet).rsplit("/", 1)[0]

    handle = None
    try:
        handle = DuckDBEngine(storage=None).stream(
            reflection, parser, qm, lambda _e: None,
        )
        names = list(handle.schema.names)
        rows = [row for batch in handle.batches() for row in batch.to_pylist()]
        return names, rows
    finally:
        if handle is not None:
            handle.close()
        reset_shared_duckdb_state()


# ═══════════════════════════════════════════════════════════════════
#  The regression: a row filter must not require the query to name it
# ═══════════════════════════════════════════════════════════════════


class TestRowFilterWithProjectedQuery:
    """Every one of these raised BinderException before the fix."""

    def test_projection_not_naming_the_filter_column(self, orders_parquet):
        names, rows = _run(orders_parquet, "SELECT id, amount FROM orders", EU_ROWS)
        assert names == ["id", "amount"]
        assert rows == [{"id": 1, "amount": 10.0}, {"id": 3, "amount": 30.0}]

    def test_aggregate_over_filtered_rows(self, orders_parquet):
        """The canonical restricted-analyst query."""
        names, rows = _run(orders_parquet, "SELECT sum(amount) FROM orders", EU_ROWS)
        assert rows == [{"sum(amount)": 40.0}]

    def test_aggregate_under_a_column_mask_too(self, orders_parquet):
        names, rows = _run(
            orders_parquet, "SELECT sum(amount) FROM orders", MASK_AND_ROWS,
        )
        assert rows == [{"sum(amount)": 40.0}]

    def test_merged_share_and_role_filter(self, orders_parquet):
        """data_reader ANDs a share's _row_filter onto the role's clause."""
        merged = RbacViewDef(
            allowed_columns=["*"],
            where_clause="(\"region\" = 'EU') AND (\"id\" > 1)",
        )
        _names, rows = _run(orders_parquet, "SELECT sum(amount) FROM orders", merged)
        assert rows == [{"sum(amount)": 30.0}]

    def test_cte_over_a_filtered_table(self, orders_parquet):
        _names, rows = _run(
            orders_parquet,
            "WITH c AS (SELECT id, amount FROM orders) SELECT sum(amount) FROM c",
            EU_ROWS,
        )
        assert rows == [{"sum(amount)": 40.0}]

    def test_set_operation_over_a_filtered_table(self, orders_parquet):
        _names, rows = _run(
            orders_parquet,
            "SELECT id FROM orders UNION ALL SELECT id FROM orders",
            EU_ROWS,
        )
        assert sorted(r["id"] for r in rows) == [1, 1, 3, 3]

    def test_self_join_over_a_filtered_table(self, orders_parquet):
        _names, rows = _run(
            orders_parquet,
            "SELECT o.id FROM orders o JOIN orders p ON o.id = p.id",
            EU_ROWS,
        )
        assert sorted(r["id"] for r in rows) == [1, 3]

    def test_star_and_count_still_work(self, orders_parquet):
        """The two shapes that worked before must be untouched."""
        names, rows = _run(orders_parquet, "SELECT * FROM orders", EU_ROWS)
        assert names == ["id", "region", "amount", "secret"]
        assert [r["id"] for r in rows] == [1, 3]

        _names, rows = _run(orders_parquet, "SELECT count(*) FROM orders", EU_ROWS)
        assert rows == [{"count_star()": 2}]

    def test_filter_column_named_by_the_query_is_returned(self, orders_parquet):
        """Widening must not turn a requested column into a stripped one."""
        names, rows = _run(orders_parquet, "SELECT id, region FROM orders", EU_ROWS)
        assert names == ["id", "region"]
        assert rows == [
            {"id": 1, "region": "EU"},
            {"id": 3, "region": "EU"},
        ]


class TestColumnMaskWithPartialProjection:
    """A column-masked role could not name FEWER columns than it was granted.

    ``create_rbac_view`` emitted ``SELECT <every allowed column>`` over a
    reflection narrowed to the query's columns, so ``SELECT id`` under a role
    granted ``id, amount, region`` failed to bind.  Every test here raised
    BinderException before the fix.
    """

    def test_single_allowed_column(self, orders_parquet):
        names, rows = _run(orders_parquet, "SELECT id FROM orders", MASK_ONLY)
        assert names == ["id"]
        assert [r["id"] for r in rows] == [1, 2, 3]

    def test_distinct_on_one_allowed_column(self, orders_parquet):
        _names, rows = _run(
            orders_parquet, "SELECT DISTINCT region FROM orders", MASK_ONLY,
        )
        assert sorted(r["region"] for r in rows) == ["EU", "US"]

    def test_subset_of_the_allowed_columns(self, orders_parquet):
        names, _rows = _run(orders_parquet, "SELECT id, amount FROM orders", MASK_ONLY)
        assert names == ["id", "amount"]

    def test_set_operation(self, orders_parquet):
        _names, rows = _run(
            orders_parquet,
            "SELECT id FROM orders INTERSECT SELECT id FROM orders",
            MASK_ONLY,
        )
        assert sorted(r["id"] for r in rows) == [1, 2, 3]

    def test_naming_the_full_allowed_set_is_unchanged(self, orders_parquet):
        names, _rows = _run(
            orders_parquet, "SELECT id, amount, region FROM orders", MASK_ONLY,
        )
        assert names == ["id", "amount", "region"]


# ═══════════════════════════════════════════════════════════════════
#  MUST NOT REGRESS: the column mask is the strongest control here
# ═══════════════════════════════════════════════════════════════════


class TestMaskedColumnStaysHidden:
    """Five query shapes, none of which may reach a denied column.

    ``MASK_AND_ROWS`` denies ``secret`` outright and denies ``region`` — which
    is also the row-filter column, i.e. exactly the column the fix now reads
    into the reflection.  If widening leaked, it would leak here first.
    """

    def test_star(self, orders_parquet):
        names, rows = _run(orders_parquet, "SELECT * FROM orders", MASK_AND_ROWS)
        assert names == ["id", "amount"]
        assert all("secret" not in r and "region" not in r for r in rows)

    def test_qualified_star(self, orders_parquet):
        names, _rows = _run(orders_parquet, "SELECT o.* FROM orders o", MASK_AND_ROWS)
        assert names == ["id", "amount"]

    def test_cte(self, orders_parquet):
        names, _rows = _run(
            orders_parquet,
            "WITH c AS (SELECT * FROM orders) SELECT * FROM c",
            MASK_AND_ROWS,
        )
        assert names == ["id", "amount"]

    def test_subquery(self, orders_parquet):
        names, _rows = _run(
            orders_parquet,
            "SELECT * FROM (SELECT * FROM orders) z",
            MASK_AND_ROWS,
        )
        assert names == ["id", "amount"]

    def test_set_operation(self, orders_parquet):
        names, rows = _run(
            orders_parquet,
            "SELECT * FROM orders UNION ALL SELECT * FROM orders",
            MASK_AND_ROWS,
        )
        assert names == ["id", "amount"]
        assert len(rows) == 4

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT secret FROM orders",
            "SELECT o.secret FROM orders o",
            "SELECT region FROM orders",
            "WITH c AS (SELECT secret FROM orders) SELECT * FROM c",
            "SELECT count(*) FROM orders WHERE secret = 's1'",
        ],
    )
    def test_naming_a_denied_column_fails_closed(self, orders_parquet, sql):
        """Reached only if RBAC is bypassed upstream; must still not bind."""
        with pytest.raises(Exception) as exc:
            _run(orders_parquet, sql, MASK_AND_ROWS)
        assert "secret" not in str(exc.value) or "not found" in str(exc.value)

    def test_filter_only_column_never_reaches_the_caller(self, orders_parquet):
        """Unmasked role: the widened column must still be stripped."""
        names, rows = _run(orders_parquet, "SELECT id, amount FROM orders", EU_ROWS)
        assert names == ["id", "amount"]
        assert all("region" not in r for r in rows)

    def test_filter_only_column_not_visible_through_a_derived_star(
            self, orders_parquet,
    ):
        """A star over a derived table must not pick the widened column up."""
        names, _rows = _run(
            orders_parquet,
            "SELECT o.* FROM (SELECT id, amount FROM orders) o",
            EU_ROWS,
        )
        assert names == ["id", "amount"]

    def test_system_columns_still_stripped_when_widening(self, orders_parquet):
        names, _rows = _run(orders_parquet, "SELECT id, amount FROM orders", EU_ROWS)
        assert "__rowid__" not in names
        assert "__timestamp__" not in names


class TestMisconfiguredFilterFailsLoudly:

    def test_unknown_filter_column_raises(self, orders_parquet):
        bad = RbacViewDef(allowed_columns=["*"], where_clause="\"ghost\" = 'x'")
        with pytest.raises(ValueError, match="ghost"):
            _run(orders_parquet, "SELECT id FROM orders", bad)

    def test_unparsable_filter_does_not_silently_drop_the_filter(
            self, orders_parquet,
    ):
        """A clause sqlglot cannot read widens to SELECT *, so the engine
        still applies it — and rejects it itself if it is not valid SQL."""
        broken = RbacViewDef(allowed_columns=["*"], where_clause="region ==== 'EU'")
        with pytest.raises(Exception):
            _run(orders_parquet, "SELECT id, amount FROM orders", broken)


# ═══════════════════════════════════════════════════════════════════
#  Units
# ═══════════════════════════════════════════════════════════════════


class TestRowFilterColumns:

    @pytest.mark.parametrize(
        "clause,expected",
        [
            ("\"region\" = 'EU'", ["region"]),
            ("(\"region\" = 'EU') AND (dept = 'x')", ["region", "dept"]),
            ("\"region\" IN ('EU','US')", ["region"]),
            ("\"a\" BETWEEN '1' AND '2'", ["a"]),
            ("NOT (\"x\" = 'y')", ["x"]),
            ("\"n\" IS NOT NULL", ["n"]),
            ("\"name\" ILIKE 'a%' ESCAPE '!'", ["name"]),
            ("lower(\"region\") = 'eu'", ["region"]),
            ("t.region = 'EU'", ["region"]),
            ("\"region\" = 'EU' AND \"REGION\" = 'EU'", ["region"]),
            # FilterBuilder's "reference" operand compares two columns; both
            # sides have to reach the reflection.
            ("\"amount\" > \"budget\"", ["amount", "budget"]),
        ],
    )
    def test_extracts_referenced_columns(self, clause, expected):
        assert sorted(row_filter_columns(clause)) == sorted(expected)

    def test_string_literal_is_not_a_column(self):
        assert row_filter_columns("\"region\" = 'amount'") == ["region"]

    def test_empty_clause(self):
        assert row_filter_columns("") == []

    def test_unparsable_returns_none(self):
        assert row_filter_columns("region ==== 'EU'") is None

    def test_subquery_columns_are_not_attributed_to_this_relation(self):
        """They bind to the subquery's own FROM, not to the filtered table."""
        cols = row_filter_columns("\"id\" IN (SELECT other_col FROM elsewhere)")
        assert "other_col" not in cols

    def test_result_is_memoised_but_not_shared(self):
        """The parse is cached; the list handed out must not be the cache's."""
        clause = "\"region\" = 'EU' AND \"dept\" = 'x'"
        first = row_filter_columns(clause)
        first.append("injected")
        assert row_filter_columns(clause) == ["region", "dept"]


class TestWidenProjectionForRowFilter:

    def test_star_projection_is_untouched(self):
        assert widen_projection_for_row_filter([], EU_ROWS, {"id", "region"}) == ([], [])

    def test_no_filter_is_untouched(self):
        cols = ["id", "amount"]
        assert widen_projection_for_row_filter(cols, MASK_ONLY, SCHEMA) == (cols, [])

    def test_no_rbac_at_all_is_untouched(self):
        cols = ["id", "amount"]
        assert widen_projection_for_row_filter(cols, None, SCHEMA) == (cols, [])

    def test_adds_the_filter_column(self):
        widened, extra = widen_projection_for_row_filter(
            ["id", "amount"], EU_ROWS, SCHEMA,
        )
        assert widened == ["id", "amount", "region"]
        assert extra == ["region"]

    def test_already_requested_column_is_not_duplicated(self):
        widened, extra = widen_projection_for_row_filter(
            ["id", "region"], EU_ROWS, SCHEMA,
        )
        assert widened == ["id", "region"]
        assert extra == []

    def test_case_insensitive_against_the_request(self):
        widened, extra = widen_projection_for_row_filter(
            ["id", "REGION"], EU_ROWS, SCHEMA,
        )
        assert extra == []
        assert widened == ["id", "REGION"]

    def test_unknown_column_raises(self):
        bad = RbacViewDef(allowed_columns=["*"], where_clause="\"ghost\" = 'x'")
        with pytest.raises(ValueError, match="ghost"):
            widen_projection_for_row_filter(["id"], bad, SCHEMA, "orders")

    def test_unknown_column_tolerated_when_schema_is_unknown(self):
        """Nothing to validate against — leave it to the binder."""
        bad = RbacViewDef(allowed_columns=["*"], where_clause="\"ghost\" = 'x'")
        widened, extra = widen_projection_for_row_filter(["id"], bad, set())
        assert extra == ["ghost"]
        assert widened == ["id", "ghost"]

    def test_unparsable_falls_back_to_the_full_projection(self):
        broken = RbacViewDef(allowed_columns=["*"], where_clause="region ==== 'EU'")
        assert widen_projection_for_row_filter(["id"], broken, SCHEMA) == ([], [])


class TestCreateRbacViewSql:
    """The emitted SQL must be byte-identical to the old one wherever the old
    one worked, so only the broken shapes change."""

    def _sql(self, **kwargs):
        con = MagicMock()
        create_rbac_view(con, "base", "v", **kwargs)
        return con.execute.call_args[0][0]

    def test_star_reflection_unrestricted_columns(self):
        sql = self._sql(rbac_view_def=EU_ROWS, projected_columns=[])
        assert "SELECT * FROM base WHERE \"region\" = 'EU'" in sql

    def test_star_reflection_masked_columns(self):
        sql = self._sql(rbac_view_def=MASK_AND_ROWS, projected_columns=[])
        assert "SELECT id, amount FROM base" in sql

    def test_narrowed_reflection_masked_columns_intersects(self):
        sql = self._sql(
            rbac_view_def=MASK_ONLY,
            projected_columns=["id", "__rowid__", "__timestamp__"],
        )
        assert "SELECT id FROM base" in sql

    def test_narrowed_reflection_keeps_allowed_system_column(self):
        """A role granted __rowid__ (OData) keeps it: it is in the reflection."""
        view = RbacViewDef(allowed_columns=["id", "__rowid__"], where_clause="")
        sql = self._sql(
            rbac_view_def=view,
            projected_columns=["id", "__rowid__", "__timestamp__"],
        )
        assert "__rowid__" in sql

    def test_empty_intersection_falls_back_to_the_allowed_list(self):
        """Fails closed at bind time rather than emitting no projection."""
        sql = self._sql(
            rbac_view_def=MASK_ONLY, projected_columns=["secret"],
        )
        assert "SELECT id, amount, region FROM base" in sql

    def test_filter_only_column_is_stripped(self):
        sql = self._sql(
            rbac_view_def=EU_ROWS,
            projected_columns=["id", "amount", "region", "__rowid__"],
            filter_only_columns=["region"],
        )
        assert "COLUMNS(c -> lower(c) NOT IN ('region'))" in sql
        assert "WHERE \"region\" = 'EU'" in sql

    def test_filter_only_column_excluded_from_the_allowed_intersection(self):
        sql = self._sql(
            rbac_view_def=MASK_AND_ROWS,
            projected_columns=["id", "region", "__rowid__"],
            filter_only_columns=["region"],
        )
        assert "SELECT id FROM base" in sql

    def test_no_projection_argument_behaves_as_before(self):
        """Callers that pass nothing get the pre-fix SQL exactly."""
        assert "SELECT * FROM base" in self._sql(rbac_view_def=EU_ROWS)
        assert "SELECT id, amount FROM base" in self._sql(
            rbac_view_def=MASK_AND_ROWS,
        )
