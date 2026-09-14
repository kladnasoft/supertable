# route: supertable.tests.test_show_stats_rbac
"""SHOW STATS must not disclose what a SELECT would refuse (AUDIT_BUGS M9).

A statistics row carries a column's min, max and null count — values copied out
of the data. SHOW STATS enforced only the *table* gate and returned every row
unfiltered, so the command was a second route to data the role's mask blocked:

  * a role denied ``SELECT salary`` read ``min=50000, max=250000`` from
    SHOW STATS, which on a small table is the salaries themselves and on any
    table is the range and null count; and
  * a role restricted to ``region = 'eu'`` read ``max=900000`` for a salary
    belonging to a ``us`` row — a value outside its row filter, reached through
    a column it was allowed.

The two need different treatment, which is why they are masked separately.
A column mask can drop whole rows. A row filter cannot: a min/max is an
aggregate over rows the role may not see, and no subset of it corresponds to
the rows it may — so the bounds are withheld instead of recomputed, and
everything describing shape rather than content is kept.

These tests drive the real reader against a real table, because the leak was
in what the command RETURNS, and a unit test on the masking helper alone would
not have caught that the helper was never called.
"""

from __future__ import annotations

import polars as pl
import pyarrow as pa
import pytest

VALUE_COLUMNS = (
    "min_bigint", "max_bigint", "min_double", "max_double",
    "min_timestamp", "max_timestamp", "min_string", "max_string",
)

ORG = "showstats"
SUPER = "rbac"
TABLE = "emp"

SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("region", pa.string()),
    ("salary", pa.int64()),
])

ROWS = [
    {"id": 1, "region": "eu", "salary": 40_000},
    {"id": 2, "region": "us", "salary": 900_000},
]

#: The value that must never reach a role restricted away from it.
US_SALARY = 900_000


@pytest.fixture(scope="module")
def lake():
    """A table plus the roles under test.

    Skips rather than fails when the live catalog/storage is unavailable, the
    same way the other integration suites here do.
    """
    try:
        from supertable.data_writer import DataWriter
        from supertable.rbac.role_manager import RoleManager
        from supertable.super_table import SuperTable

        SuperTable(SUPER, ORG)
        writer = DataWriter(super_name=SUPER, organization=ORG)
        writer.write("superadmin", TABLE,
                     pa.Table.from_pylist(ROWS, schema=SCHEMA), ["id"])

        roles = RoleManager(SUPER, ORG, actor_role_name="superadmin")
        for definition in (
            # Masked away from `salary` entirely.
            {"role_name": "ss_no_salary", "role": "reader",
             "tables": {TABLE: {"columns": ["id", "region"], "filters": ["*"]}}},
            # Same grant, written in a different case — the mask must not be
            # defeated by spelling.
            {"role_name": "ss_mixed_case", "role": "reader",
             "tables": {TABLE: {"columns": ["ID", "Region"], "filters": ["*"]}}},
            # Allowed every column, so nothing should be masked.
            {"role_name": "ss_all_columns", "role": "reader",
             "tables": {TABLE: {"columns": ["*"], "filters": ["*"]}}},
            # Allowed every column but restricted to a subset of ROWS.
            {"role_name": "ss_eu_only", "role": "reader",
             "tables": {TABLE: {
                 "columns": ["id", "region", "salary"],
                 "filters": {"region": {"operation": "=", "type": "value",
                                        "value": "eu"}}}}},
        ):
            try:
                roles.create_role(definition)
            except Exception:
                pass          # already present from an earlier run in-process
    except Exception as e:
        pytest.skip(f"live catalog/storage unavailable "
                    f"({type(e).__name__}: {str(e)[:120]})")
    return True


def _show_stats(role: str) -> pl.DataFrame:
    from supertable.data_reader import DataReader, Status, engine

    reader = DataReader(super_name=SUPER, organization=ORG,
                        query=f"SHOW STATS {TABLE}")
    frame, status, message = reader.execute(role_name=role, engine=engine.DUCKDB)
    assert status is Status.OK, f"SHOW STATS failed for {role}: {message}"
    return frame


def _columns_listed(frame: pl.DataFrame):
    if not frame.height:
        return []
    return sorted(set(frame.get_column("column_name").to_list()))


# ---------------------------------------------------------------------------
# the column mask
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("role", ["ss_no_salary", "ss_mixed_case"])
def test_a_masked_column_has_no_statistics_row(lake, role):
    """The leak: min/max for a column the role may not read."""
    listed = _columns_listed(_show_stats(role))
    assert "salary" not in listed, (
        f"{role} may not read 'salary' but SHOW STATS returned its statistics"
    )
    assert "id" in listed and "region" in listed, (
        f"the mask removed columns {role} IS allowed: {listed}"
    )


def test_the_masked_role_is_genuinely_denied_a_select(lake):
    """Anchors the test above: without this, 'salary' might just be absent.

    If the role could read the column anyway, hiding its statistics would prove
    nothing — so the denial the mask mirrors is asserted directly.
    """
    from supertable.data_reader import DataReader, engine

    with pytest.raises(PermissionError, match="salary"):
        DataReader(super_name=SUPER, organization=ORG,
                   query=f"SELECT salary FROM {TABLE}").execute(
            role_name="ss_no_salary", engine=engine.DUCKDB)


# ---------------------------------------------------------------------------
# the row filter
# ---------------------------------------------------------------------------

def test_a_row_filtered_role_gets_no_bounds(lake):
    """Bounds describe rows the role cannot select, so they are withheld."""
    frame = _show_stats("ss_eu_only")
    assert frame.height, "a row-filtered role should still see the stats shape"

    for column in VALUE_COLUMNS:
        if column not in frame.columns:
            continue
        values = [v for v in frame.get_column(column).to_list() if v is not None]
        assert not values, (
            f"{column} still carries values for a row-filtered role: {values[:3]}"
        )


def test_the_out_of_filter_value_is_not_reachable(lake):
    """Stated as the concrete disclosure rather than as a null check.

    The eu-only role can select exactly one row, whose salary is 40,000. The
    900,000 belongs to the us row. It must not appear anywhere in the result.
    """
    frame = _show_stats("ss_eu_only")
    flat = [v for column in frame.columns for v in frame.get_column(column).to_list()]
    assert US_SALARY not in flat, (
        f"the salary of a row outside the role's filter ({US_SALARY}) is "
        f"disclosed by SHOW STATS"
    )


def test_shape_is_kept_so_the_command_stays_useful(lake):
    """Withholding the bounds must not empty the result.

    Column names, types, row counts and sizes describe layout, not content, and
    are what make SHOW STATS worth running for diagnosing pruning.
    """
    frame = _show_stats("ss_eu_only")
    assert "salary" in _columns_listed(frame)
    row = frame.filter(pl.col("column_name") == "salary").to_dicts()[0]
    assert row["row_group_rows"] and row["row_group_rows"] > 0
    assert row["physical_type"]
    # stats_available answers "does the footer HAVE usable statistics", which is
    # a property of the file and a different question from "what are they".
    assert row["stats_available"] is True


# ---------------------------------------------------------------------------
# unrestricted roles are untouched
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("role", ["superadmin", "ss_all_columns"])
def test_an_unrestricted_role_sees_everything(lake, role):
    """The masking must cost nothing for a role with nothing masked.

    ss_all_columns is included because it reaches RBAC with an explicit
    ``["*"]`` grant rather than by being an admin, which is a different path to
    the same answer.
    """
    frame = _show_stats(role)
    listed = _columns_listed(frame)
    assert {"id", "region", "salary"} <= set(listed), listed

    row = frame.filter(pl.col("column_name") == "salary").to_dicts()[0]
    assert row["min_bigint"] == 40_000
    assert row["max_bigint"] == US_SALARY, (
        "an unrestricted role must still get real bounds"
    )
