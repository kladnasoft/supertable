# route: test_suite.test_write_correctness
"""Write + tombstone correctness over a randomized transaction stream.

WHAT THIS PROVES

A stream of 120+ inserts, updates, deletes, no-op deletes and rejected stale
updates is applied to a real table. The expected contents are tracked
independently in ``shadow.py`` — a dict, sharing no code with the library — and
a fingerprint is recorded after every transaction. At the end the table is read
back and must match the shadow's checksum exactly.

That single equality covers the things that actually go wrong in a
copy-on-write table with a deletion vector:

  * a deleted row coming back (tombstone lost, or not applied to a new file),
  * a superseded row coming back (the old version still visible after upsert),
  * a live row disappearing (over-aggressive tombstone, or a prune that drops a
    file it could not prove empty),
  * a rejected write taking effect anyway (``newer_than`` not enforced),
  * a no-op write disturbing unrelated rows.

Compaction is part of the surface, not an interference: 120 writes cross the
auto-compaction thresholds, so tombstoned rows are physically removed and files
are merged *while* the stream runs. A compaction that resurrected or dropped a
row would show up here as a checksum mismatch.

The read-back is done twice — pruned and full-scan — because pruning may only
drop files that provably hold no matching row. If the two disagree, pruning is
unsound, which is a class of bug that is invisible to any single read.
"""

from __future__ import annotations

import json
import os

import pyarrow as pa
import pytest

from .shadow import COLUMNS, KEY, table_checksum
from .workload import WriteWorkload

ORG = "correctness"
SUPER = "suite"
TABLE = "ledger"
ROLE = "superadmin"

DEFAULT_SEED = 20260914
DEFAULT_TRANSACTIONS = 120

#: Arrow schema of the workload table. Explicit rather than inferred so a
#: column's type cannot drift between batches and quietly change what is being
#: tested.
SCHEMA = pa.schema([
    ("id", pa.int64()),
    ("name", pa.string()),
    ("amount", pa.float64()),
    ("qty", pa.int64()),
    ("grp", pa.string()),
    ("rev", pa.int64()),
    ("event_date", pa.date32()),
])


def _seed() -> int:
    return int(os.environ.get("SUPERTABLE_TEST_SEED", DEFAULT_SEED))


def _transactions() -> int:
    return int(os.environ.get("SUPERTABLE_TEST_TRANSACTIONS", DEFAULT_TRANSACTIONS))


@pytest.fixture(scope="module")
def executed():
    """Run the whole transaction stream once, then hand over the results.

    Module-scoped because the stream is the expensive part and every assertion
    below interrogates the same final state.
    """
    from supertable.data_writer import DataWriter
    from supertable.super_table import SuperTable

    SuperTable(SUPER, ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)

    def apply_fn(action: str, payload):
        if action == "append":
            writer.write(ROLE, TABLE, pa.Table.from_pylist(payload, schema=SCHEMA), [])
        elif action == "upsert":
            writer.write(ROLE, TABLE, pa.Table.from_pylist(payload, schema=SCHEMA), [KEY])
        elif action == "upsert_if_newer":
            writer.write(ROLE, TABLE, pa.Table.from_pylist(payload, schema=SCHEMA),
                         [KEY], newer_than="rev")
        elif action == "delete":
            writer.write(ROLE, TABLE, pa.table({KEY: pa.array(payload, pa.int64())}),
                         [KEY], delete_only=True)
        else:
            raise AssertionError(f"unknown action {action!r}")

    workload = WriteWorkload(seed=_seed(), transactions=_transactions())
    workload.run(apply_fn)
    return workload


def _read(sql: str, fullscan: bool = False):
    from supertable.data_reader import DataReader, Status, engine

    reader = DataReader(super_name=SUPER, organization=ORG, query=sql)
    frame, status, message = reader.execute(
        role_name=ROLE, engine=engine.DUCKDB, fullscan=fullscan,
    )
    assert status is Status.OK, f"read failed: {status} / {message}\nSQL: {sql}"
    return frame


def _rows_as_dicts(fullscan: bool = False):
    columns = ", ".join(COLUMNS)
    frame = _read(f"SELECT {columns} FROM {TABLE}", fullscan=fullscan)
    return [dict(zip(COLUMNS, row)) for row in frame.iter_rows()]


# ---------------------------------------------------------------------------
# The headline assertion
# ---------------------------------------------------------------------------

def test_the_table_matches_the_independently_computed_checksum(executed):
    """One equality covering every transaction in the stream."""
    actual = _rows_as_dicts()
    expected_checksum = executed.shadow.checksum()
    actual_checksum = table_checksum(actual)

    if expected_checksum != actual_checksum:
        # Turn "checksums differ" into something actionable before failing.
        diagnosis = executed.shadow.divergence(actual)
        stalled_at = executed.shadow.last_matching_transaction(actual_checksum)
        detail = [
            f"seed={executed.seed} (rerun with --seed {executed.seed})",
            f"expected {len(executed.shadow.rows)} rows, got {len(actual)}",
            f"divergence: {diagnosis}",
        ]
        if stalled_at is not None:
            culprit = next(
                (t for t in executed.applied if t.index == stalled_at + 1), None,
            )
            detail.append(
                f"the result matches the expected state as of transaction "
                f"{stalled_at}, so transaction {stalled_at + 1} "
                f"({culprit.kind if culprit else '?'}) did not take effect: "
                f"{culprit.detail if culprit else ''}"
            )
        detail.append(f"workload: {json.dumps(executed.summary(), default=str)}")
        pytest.fail("\n  ".join(detail))


def test_every_row_matches_its_own_checksum(executed):
    """Per-row digests, so a single corrupted column is named directly."""
    actual = {row[KEY]: row for row in _rows_as_dicts()}
    expected = executed.shadow.checksums_by_key()

    from .shadow import row_checksum
    mismatched = {
        key: (expected[key], row_checksum(actual[key]))
        for key in expected
        if key in actual and row_checksum(actual[key]) != expected[key]
    }
    assert not mismatched, (
        f"{len(mismatched)} row(s) differ from their expected checksum: "
        f"{dict(list(mismatched.items())[:5])}"
    )


def test_row_count_and_key_set_agree(executed):
    """Stated separately because it distinguishes the two failure directions."""
    actual = _rows_as_dicts()
    assert len(actual) == len(executed.shadow.rows)
    assert {r[KEY] for r in actual} == set(executed.shadow.rows)


def test_no_key_is_returned_twice(executed):
    """A duplicate key means a superseded row survived alongside its replacement.

    Worth its own assertion: the checksum would catch it, but not say why.
    """
    keys = [row[KEY] for row in _rows_as_dicts()]
    assert len(keys) == len(set(keys)), "the reader returned the same key twice"


# ---------------------------------------------------------------------------
# Pruning must not change the answer
# ---------------------------------------------------------------------------

def test_a_full_scan_returns_exactly_the_same_table(executed):
    """Pruning may only drop files that provably hold no matching row.

    A pruned read and a full scan must therefore be identical. This is the
    differential that catches an unsound prune — including one that is
    invisible at a particular timezone or data distribution.
    """
    pruned = table_checksum(_rows_as_dicts(fullscan=False))
    full = table_checksum(_rows_as_dicts(fullscan=True))
    assert pruned == full, (
        "pruned and full-scan reads disagree, so file pruning dropped a file "
        "that did contain matching rows"
    )


@pytest.mark.parametrize("predicate,description", [
    ("qty >= 25", "integer range"),
    ("amount < 0", "negative floats"),
    ("grp = 'alpha'", "string equality"),
    ("event_date >= DATE '2025-01-01'", "date lower bound"),
    ("rev > 1", "rows that have been updated at least once"),
])
def test_filtered_reads_also_agree_with_the_model(executed, predicate, description):
    """Filters engage pruning per column type; each must still match the model.

    The model applies the same predicate in Python, so this checks the filter
    *and* the pruning decision behind it.
    """
    columns = ", ".join(COLUMNS)
    frame = _read(f"SELECT {columns} FROM {TABLE} WHERE {predicate}")
    actual = [dict(zip(COLUMNS, row)) for row in frame.iter_rows()]

    import datetime as _dt
    checks = {
        "qty >= 25": lambda r: r["qty"] >= 25,
        "amount < 0": lambda r: r["amount"] < 0,
        "grp = 'alpha'": lambda r: r["grp"] == "alpha",
        "event_date >= DATE '2025-01-01'": lambda r: r["event_date"] >= _dt.date(2025, 1, 1),
        "rev > 1": lambda r: r["rev"] > 1,
    }
    expected = [r for r in executed.shadow.rows.values() if checks[predicate](r)]
    assert table_checksum(actual) == table_checksum(expected), (
        f"{description}: expected {len(expected)} rows, got {len(actual)}"
    )


# ---------------------------------------------------------------------------
# The workload itself has to be worth running
# ---------------------------------------------------------------------------

def test_the_stream_actually_exercised_every_operation(executed):
    """A guard against a silently degenerate workload.

    If a seed change or a weighting mistake stopped generating deletes, the
    checksum above would still pass while proving much less. This fails instead.
    """
    summary = executed.summary()
    by_kind = summary["by_kind"]
    for kind in ("insert", "update", "delete", "delete_missing", "stale_update"):
        assert by_kind.get(kind, 0) > 0, (
            f"the workload produced no {kind!r} transactions, so this run does "
            f"not prove what it claims: {summary}"
        )
    assert summary["transactions"] >= 100, summary
    # Deletes and updates must have actually removed/replaced rows, not just
    # been issued against an empty table.
    assert summary["final_row_count"] < summary["keys_allocated"], (
        "no key was ever removed; deletes did not take effect"
    )
