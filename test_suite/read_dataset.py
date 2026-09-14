# route: test_suite.read_dataset
"""A deterministic dataset for the read half, written through the real writer.

DETERMINISTIC, UNLIKE THE WRITE HALF

The write half is randomized because it is hunting for an interleaving that
breaks. The read half is checking SQL semantics, where a fixed dataset is
better: expectations are stable, failures are reproducible without a seed, and
the interesting values (nulls, negatives, zero, ties, boundary dates) are
placed on purpose rather than hoped for.

THE PHYSICAL STATE IS DELIBERATELY NOT THE LOGICAL STATE

The dataset is written in several batches and then partly updated and partly
deleted, so at read time the table has superseded rows, an active deletion
vector and multiple files. Reading a pristine single-file table would exercise
almost none of the read path: the tombstone anti-join, the file pruning and the
projection all become no-ops. ``logical_rows()`` returns what the table should
contain *after* all of that, which is what the oracle uses.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, List

import pyarrow as pa

ORG = "correctness"
SUPER = "suite"
FACTS = "facts"
DIMS = "dims"
ROLE = "superadmin"

FACT_COLUMNS = ("fid", "grp", "region", "amount", "qty", "score", "note", "event_date", "event_ts")
DIM_COLUMNS = ("grp", "label", "weight")

FACT_SCHEMA = pa.schema([
    ("fid", pa.int64()),
    ("grp", pa.string()),
    ("region", pa.string()),
    ("amount", pa.float64()),
    ("qty", pa.int64()),
    ("score", pa.float64()),        # carries NULLs, so aggregate null-skipping shows
    ("note", pa.string()),          # carries NULLs and unicode
    ("event_date", pa.date32()),
    ("event_ts", pa.timestamp("us")),
])

DIM_SCHEMA = pa.schema([
    ("grp", pa.string()),
    ("label", pa.string()),
    ("weight", pa.float64()),
])

GROUPS = ("alpha", "beta", "gamma", "delta")
REGIONS = ("eu", "us", "apac")
BASE_DATE = date(2024, 1, 1)

#: 'delta' exists in the facts but NOT in dims, and 'omega' exists in dims but
#: not in facts — so LEFT/RIGHT/FULL joins and anti-joins have unmatched rows on
#: both sides instead of degenerating into inner joins.
DIM_ROWS: List[Dict[str, object]] = [
    {"grp": "alpha", "label": "Alpha group", "weight": 1.5},
    {"grp": "beta", "label": "Beta group", "weight": 2.0},
    {"grp": "gamma", "label": "Gamma group", "weight": 0.5},
    {"grp": "omega", "label": "Unused group", "weight": 9.0},
]

#: Keys updated after the initial load: their stored row is superseded, so a
#: correct read must serve only the new version.
UPDATED_KEYS = (3, 17, 42, 88, 120, 151)

#: Keys deleted after the initial load: they must not appear in any result.
DELETED_KEYS = (7, 23, 55, 99, 140, 177, 190)

ROW_COUNT = 200


def _fact_row(fid: int, revision: int = 1) -> Dict[str, object]:
    """One fact row, fully determined by *fid* and *revision*.

    No randomness: the same fid always produces the same values, so an
    expectation written down today still holds tomorrow.
    """
    group = GROUPS[fid % len(GROUPS)]
    region = REGIONS[fid % len(REGIONS)]
    # Deliberate value placement:
    #   - amount: spans negative, zero and positive
    #   - qty: includes 0 so AVG vs SUM/COUNT differ meaningfully
    #   - score: every 7th row is NULL, so aggregates must skip nulls
    #   - note: every 5th row is NULL; unicode elsewhere
    amount = round((fid * 37 % 1000) - 250 + revision * 0.5, 2)
    qty = fid % 13
    score = None if fid % 7 == 0 else round((fid * 13 % 97) / 7.0, 3)
    note = None if fid % 5 == 0 else f"note-{group}-{fid}é"
    event_date = BASE_DATE + timedelta(days=(fid * 3) % 730)
    event_ts = datetime(2024, 1, 1) + timedelta(hours=(fid * 7) % 17_520)
    return {
        "fid": fid, "grp": group, "region": region, "amount": amount,
        "qty": qty, "score": score, "note": note,
        "event_date": event_date, "event_ts": event_ts,
    }


def logical_rows() -> List[Dict[str, object]]:
    """What the facts table must contain after the load, updates and deletes.

    Computed here in plain Python — this is the oracle's input and never comes
    from reading the table back.
    """
    rows = {}
    for fid in range(1, ROW_COUNT + 1):
        rows[fid] = _fact_row(fid, revision=1)
    for fid in UPDATED_KEYS:
        rows[fid] = _fact_row(fid, revision=2)
    for fid in DELETED_KEYS:
        rows.pop(fid, None)
    return [rows[fid] for fid in sorted(rows)]


def dim_rows() -> List[Dict[str, object]]:
    return [dict(row) for row in DIM_ROWS]


def materialize() -> Dict[str, object]:
    """Write the dataset through DataWriter and return a description of it.

    Written in four batches so the table spans multiple files, then updated and
    deleted so a deletion vector is live during every read below.
    """
    from supertable.data_writer import DataWriter
    from supertable.super_table import SuperTable

    SuperTable(SUPER, ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)

    initial = [_fact_row(fid) for fid in range(1, ROW_COUNT + 1)]
    batch_size = 50
    for start in range(0, len(initial), batch_size):
        writer.write(ROLE, FACTS,
                     pa.Table.from_pylist(initial[start:start + batch_size],
                                          schema=FACT_SCHEMA), [])

    writer.write(ROLE, FACTS,
                 pa.Table.from_pylist([_fact_row(fid, revision=2) for fid in UPDATED_KEYS],
                                      schema=FACT_SCHEMA), ["fid"])

    writer.write(ROLE, FACTS,
                 pa.table({"fid": pa.array(list(DELETED_KEYS), pa.int64())}),
                 ["fid"], delete_only=True)

    writer.write(ROLE, DIMS,
                 pa.Table.from_pylist(DIM_ROWS, schema=DIM_SCHEMA), [])

    return {
        "rows_written": len(initial),
        "updated": list(UPDATED_KEYS),
        "deleted": list(DELETED_KEYS),
        "expected_rows": len(logical_rows()),
    }


def oracle_connection():
    """A bare DuckDB holding the logical rows under the same table names.

    This is the independent oracle for the SQL cases. It is *not* SuperTable:
    no reflection view, no RBAC view, no deletion-vector anti-join, no pruning,
    no engine routing — just DuckDB over the rows the dataset is defined to
    contain. Running the identical SQL against both therefore isolates
    SuperTable's read path as the only difference.

    Aggregates whose value is fixed by arithmetic are *also* asserted against
    hand-computed Python in the test module, so the read half does not rest on
    DuckDB alone.
    """
    import duckdb

    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.register("facts_src", pa.Table.from_pylist(logical_rows(), schema=FACT_SCHEMA))
    con.register("dims_src", pa.Table.from_pylist(DIM_ROWS, schema=DIM_SCHEMA))
    con.execute(f"CREATE TABLE {FACTS} AS SELECT * FROM facts_src")
    con.execute(f"CREATE TABLE {DIMS} AS SELECT * FROM dims_src")
    return con
