"""STREAD-011 end-to-end: a Decimal column ingests through DataWriter and reads back.

The unit-level seal for this lives in
``supertable/tests/test_decimal_footer_stats.py``, which drives the footer
routing directly. This module asserts the acceptance criterion the audit
actually stated — *"the events table must ingest successfully through DataWriter
without the matrix fixture fallback"* — which needs a real write, a real
snapshot, and a real read.

It lives under ``tests/characterization`` because that is the only suite with
the hermetic bootstrap (LOCAL storage + fakeredis pinned before ``supertable``
is first imported) that a DataWriter -> DataReader round trip requires; see the
scope note in ``supertable/tests/test_read_pruning_integration.py``.

Polars writes a Decimal column as INT64 with a DECIMAL logical type. The footer
reports ``has_min_max = True``, but PyArrow cannot decode bounds in that
encoding, so reading ``stat.min`` raised ``ArrowNotImplementedError`` and took
the whole write down — a table with one Decimal column could not be ingested.
"""

from __future__ import annotations

import decimal
from typing import List, Tuple

import polars as pl
import pytest

from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.super_table import SuperTable

ORG = "chardec"
SUPER = "decimals"
SIMPLE = "events"
ROLE = "superadmin"

#: Negative, zero, fractional and boundary values — a decimal bug that only
#: showed on one sign would otherwise slip through.
AMOUNTS = [
    decimal.Decimal("1.500"),
    decimal.Decimal("-2.250"),
    decimal.Decimal("0.000"),
    decimal.Decimal("999999.999"),
]
EXPECTED: List[Tuple[int, decimal.Decimal]] = list(zip([1, 2, 3, 4], AMOUNTS))


def _frame() -> pl.DataFrame:
    return pl.DataFrame(
        {"eid": [1, 2, 3, 4], "amt": AMOUNTS, "note": ["a", "b", "c", "d"]},
        schema={"eid": pl.Int64, "amt": pl.Decimal(12, 3), "note": pl.String},
    )


@pytest.fixture
def written():
    """Ingest the decimal table through the real writer.

    The assertion is the write *completing*: this call is what raised
    ArrowNotImplementedError before the fix.

    Function-scoped deliberately. The hermetic fixture in ``tests/conftest.py``
    is autouse and function-scoped, so it gives each test its own storage and
    fake Redis — a module-scoped write would run *outside* it and reach the
    real Redis. Every test therefore re-writes, which also means each one
    independently proves the ingest completes.
    """
    SuperTable(SUPER, ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)
    result = writer.write(
        role_name=ROLE,
        simple_name=SIMPLE,
        data=_frame(),
        overwrite_columns=["eid"],
        lineage={"source_type": "manual", "source_id": "stread_011"},
    )
    return result


def _query(sql: str) -> pl.DataFrame:
    reader = DataReader(super_name=SUPER, organization=ORG, query=sql)
    df, status, msg = reader.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"query failed: {status} / {msg}\nSQL: {sql}"
    return df


def test_the_write_completes(written):
    """4 rows in, 4 rows inserted, nothing deleted."""
    _cols, rows, inserted, deleted = written
    assert (rows, inserted, deleted) == (4, 4, 0)


def test_the_values_read_back_exactly(written):
    """Exact comparison, not a tolerance: decimals must not round-trip as floats."""
    df = _query(f"SELECT eid, amt FROM {SIMPLE} ORDER BY eid")
    got = [(int(e), decimal.Decimal(str(a))) for e, a in df.iter_rows()]
    assert got == EXPECTED


def test_arithmetic_over_the_decimal_column(written):
    """Stats being unavailable for the column must not affect its arithmetic."""
    df = _query(f"SELECT SUM(amt) AS total FROM {SIMPLE}")
    assert decimal.Decimal(str(df.get_column("total")[0])) == sum(AMOUNTS)


def test_a_predicate_on_the_decimal_column_is_correct(written):
    """The decimal column never prunes, so every file is scanned and the
    query's own WHERE does the filtering — the result must still be right."""
    df = _query(f"SELECT eid FROM {SIMPLE} WHERE amt < 0 ORDER BY eid")
    assert [int(r[0]) for r in df.iter_rows()] == [2]


def test_the_other_columns_keep_working(written):
    """A decimal sharing the file must not cost its neighbours their stats."""
    df = _query(f"SELECT eid, note FROM {SIMPLE} WHERE eid >= 3 ORDER BY eid")
    assert [(int(e), n) for e, n in df.iter_rows()] == [(3, "c"), (4, "d")]


def test_the_stats_artifact_was_published(written):
    """The write produced a stats artifact rather than skipping it.

    Bailing out of statistics entirely would also have made the write pass, so
    this distinguishes "handled the decimal" from "gave up on stats".
    """
    df = _query(f"SHOW STATS {SUPER}.{SIMPLE}")
    assert df.height > 0
    names = set(df.get_column("column_name").to_list())
    assert {"eid", "note"} <= names
