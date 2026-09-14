# route: test_suite.test_read_correctness
"""SQL read correctness: every case must agree with an independent oracle.

HOW THE EXPECTATIONS ARE INDEPENDENT

Each query runs twice: once through ``DataReader`` and once through a bare
DuckDB connection holding the same logical rows under the same table names.
The second has none of what is under test — no reflection view, no RBAC view,
no deletion-vector anti-join, no file pruning, no engine routing — so the only
difference between the two is SuperTable's read path.

Because that leaves DuckDB checking DuckDB for the pure-SQL cases, the
aggregates whose values are fixed by arithmetic are *additionally* asserted
against numbers computed in plain Python (``test_core_aggregates_match_python``).
If SuperTable and the oracle were ever wrong together, those would still fail.

WHY THE TABLE IS NOT PRISTINE

The dataset is written in four batches, then partly updated and partly deleted,
so at read time there are multiple files, superseded rows and a live deletion
vector. A correct answer therefore requires the tombstone anti-join and the
pruning decision to be right, not just the SQL.

MODES

Every case runs buffered *and* full-scan, and the two must agree: pruning may
only drop files that provably hold no matching row, so any disagreement is an
unsound prune. A representative subset also runs through streaming, AUTO and
the public ``query_sql`` helper, which are separate code paths.
"""

from __future__ import annotations

import datetime as dt
import decimal
import math
import statistics
from typing import Any, List, Sequence, Tuple

import pytest

from .read_cases import ReadCase, build_cases
from .read_dataset import (
    DIMS, FACTS, ORG, ROLE, SUPER, dim_rows, logical_rows, materialize,
    oracle_connection,
)

CASES: List[ReadCase] = build_cases()


# ---------------------------------------------------------------------------
# normalisation: compare values, not representations
# ---------------------------------------------------------------------------

def _normalise(value: Any) -> Any:
    """Reduce a cell to a form that compares across Arrow/polars/DuckDB.

    Floats are rounded because two engines may differ in the last bit of a
    division or a standard deviation — a real difference of 1e-12 is not a
    correctness failure, whereas a wrong row or a wrong aggregate is orders of
    magnitude larger. Dates and timestamps compare by their textual form so
    date32 vs datetime.date, and tz-naive vs tz-aware-at-UTC, do not matter.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, decimal.Decimal):
        # Folded to a number so a Decimal and an equal int/float compare equal
        # here, because this comparison is about values.
        #
        # The read path no longer differs on this — a scale-zero decimal (an
        # integer SUM) is int64 in both the buffered and streamed paths. That
        # equivalence is asserted on the RAW values by
        # test_an_integer_sum_has_the_same_python_type_in_every_mode, precisely
        # because this folding would otherwise hide a regression in it.
        integral = value.to_integral_value()
        return int(integral) if integral == value else round(float(value), 9)
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        return round(value, 9)
    if isinstance(value, int):
        return value
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value.isoformat(sep=" ")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return value.days * 86400 + value.seconds
    return str(value)


def _rows(raw: Sequence[Sequence[Any]]) -> List[Tuple]:
    return [tuple(_normalise(cell) for cell in row) for row in raw]


def _compare(case: ReadCase, actual: List[Tuple], expected: List[Tuple]) -> None:
    if case.ordered:
        assert actual == expected, _explain(case, actual, expected, ordered=True)
    else:
        assert sorted(actual, key=repr) == sorted(expected, key=repr), \
            _explain(case, actual, expected, ordered=False)


def _explain(case, actual, expected, ordered) -> str:
    lines = [
        f"case {case.case_id} ({case.feature}) disagreed with the oracle",
        f"  comparison: {'positional' if ordered else 'multiset'}",
        f"  sql: {case.sql}",
        f"  rows: expected {len(expected)}, got {len(actual)}",
    ]
    if ordered:
        for index, (want, got) in enumerate(zip(expected, actual)):
            if want != got:
                lines.append(f"  first difference at row {index}: "
                             f"expected {want!r}, got {got!r}")
                break
    else:
        missing = [r for r in expected if r not in actual][:3]
        extra = [r for r in actual if r not in expected][:3]
        if missing:
            lines.append(f"  missing rows (up to 3): {missing}")
        if extra:
            lines.append(f"  unexpected rows (up to 3): {extra}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def dataset():
    """Write the dataset once; every case reads the same final state."""
    return materialize()


@pytest.fixture(scope="module")
def oracle():
    con = oracle_connection()
    yield con
    con.close()


def _read(sql: str, mode: str = "pruned") -> List[Tuple]:
    """Run *sql* through the read path in *mode* and return normalised rows."""
    from supertable.data_reader import DataReader, Status, engine, query_sql

    if mode == "query_sql":
        _columns, rows, _meta = query_sql(
            ORG, SUPER, sql, 100_000, engine.DUCKDB, ROLE, source="test_suite",
        )
        return _rows(rows)

    reader = DataReader(super_name=SUPER, organization=ORG, query=sql,
                        source="test_suite")
    if mode == "stream":
        handle = reader.stream(ROLE, engine=engine.DUCKDB, batch_rows=17)
        try:
            names = list(handle.schema.names)
            collected = []
            for batch in handle.batches():
                collected.extend(tuple(row[name] for name in names)
                                 for row in batch.to_pylist())
            return _rows(collected)
        finally:
            handle.close()

    chosen = engine.AUTO if mode == "auto" else engine.DUCKDB
    frame, status, message = reader.execute(
        role_name=ROLE, engine=chosen, fullscan=(mode == "fullscan"),
    )
    assert status is Status.OK, f"read failed in {mode}: {status} / {message}\nSQL: {sql}"
    return _rows(list(frame.iter_rows()))


def _oracle_rows(oracle, sql: str) -> List[Tuple]:
    return _rows(oracle.execute(sql).fetchall())


# ---------------------------------------------------------------------------
# the dataset itself must be what the oracle assumes
# ---------------------------------------------------------------------------

def test_the_dataset_is_the_shape_the_oracle_expects(dataset):
    """Guards every other assertion in this module.

    If the load silently wrote a different number of rows, every case would be
    compared against the wrong oracle and could still 'pass'.
    """
    expected = logical_rows()
    assert dataset["expected_rows"] == len(expected)
    rows = _read(f"SELECT COUNT(*) AS n, COUNT(DISTINCT fid) AS k FROM {FACTS}")
    assert rows == [(len(expected), len(expected))], (
        "the facts table does not hold one row per expected key — the deletion "
        "vector or an upsert did not take effect"
    )
    dims = _read(f"SELECT COUNT(*) AS n FROM {DIMS}")
    assert dims == [(len(dim_rows()),)]


# ---------------------------------------------------------------------------
# the SQL surface
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("case", CASES, ids=[c.case_id for c in CASES])
def test_matches_the_independent_oracle(dataset, oracle, case):
    """Every case, buffered, against bare DuckDB over the same logical rows."""
    _compare(case, _read(case.sql, "pruned"), _oracle_rows(oracle, case.sql))


@pytest.mark.parametrize("case", CASES, ids=[c.case_id for c in CASES])
def test_a_full_scan_gives_the_same_answer(dataset, case):
    """Pruning may only drop files that provably hold no matching row.

    Comparing the two reads of the same query catches an unsound prune even
    where both agree with the oracle by luck on this data.
    """
    pruned = _read(case.sql, "pruned")
    full = _read(case.sql, "fullscan")
    _compare(case, pruned, full)


_ALL_MODE_CASES = [c for c in CASES if c.all_modes]


@pytest.mark.parametrize("mode", ["stream", "auto", "query_sql"])
@pytest.mark.parametrize("case", _ALL_MODE_CASES, ids=[c.case_id for c in _ALL_MODE_CASES])
def test_representative_cases_agree_in_every_mode(dataset, oracle, case, mode):
    """Streaming, AUTO routing and the public helper are distinct code paths.

    A representative case per feature rather than the whole catalogue: the
    point is that the modes agree, which a subset establishes, and running all
    of them would multiply the suite's runtime for little extra signal.
    """
    _compare(case, _read(case.sql, mode), _oracle_rows(oracle, case.sql))


# ---------------------------------------------------------------------------
# a Python-computed floor, so the oracle is not the only authority
# ---------------------------------------------------------------------------

def test_core_aggregates_match_python(dataset):
    """Hand-computed aggregates, independent of both SQL engines.

    Deliberately redundant with the oracle comparison. If SuperTable and DuckDB
    were ever wrong in the same direction, this is what still fails.
    """
    rows = logical_rows()
    quantities = [r["qty"] for r in rows]
    amounts = [r["amount"] for r in rows]
    scores = [r["score"] for r in rows if r["score"] is not None]

    actual = _read(f"""
        SELECT COUNT(*) AS n, SUM(qty) AS total, MIN(qty) AS lo, MAX(qty) AS hi,
               ROUND(AVG(qty), 9) AS mean, ROUND(STDDEV_SAMP(qty), 9) AS sd,
               COUNT(score) AS scored, ROUND(AVG(score), 9) AS score_mean,
               ROUND(MIN(amount), 9) AS amount_lo, ROUND(MAX(amount), 9) AS amount_hi
        FROM {FACTS}""")[0]

    expected = (
        len(rows),
        sum(quantities),
        min(quantities),
        max(quantities),
        round(statistics.fmean(quantities), 9),
        round(statistics.stdev(quantities), 9),
        len(scores),
        round(statistics.fmean(scores), 9),
        round(min(amounts), 9),
        round(max(amounts), 9),
    )
    assert actual == expected, (
        f"aggregates disagree with Python\n  expected {expected}\n  got      {actual}"
    )


def test_group_counts_match_python(dataset):
    """Per-group counts and sums, computed without SQL."""
    from collections import defaultdict

    counts, sums = defaultdict(int), defaultdict(int)
    for row in logical_rows():
        counts[row["grp"]] += 1
        sums[row["grp"]] += row["qty"]

    actual = _read(f"SELECT grp, COUNT(*) AS n, SUM(qty) AS q FROM {FACTS} "
                   f"GROUP BY grp ORDER BY grp")
    expected = [(g, counts[g], sums[g]) for g in sorted(counts)]
    assert actual == expected


def test_date_filter_matches_python(dataset):
    """A date boundary computed in Python, not by either SQL engine."""
    cutoff = dt.date(2025, 1, 1)
    expected = sum(1 for r in logical_rows() if r["event_date"] >= cutoff)
    actual = _read(f"SELECT COUNT(*) AS n FROM {FACTS} "
                   f"WHERE event_date >= DATE '2025-01-01'")
    assert actual == [(expected,)], f"expected {expected} rows on/after {cutoff}"


def test_deleted_rows_are_gone_and_updates_are_current(dataset):
    """The tombstone and upsert guarantees, stated in Python terms."""
    from .read_dataset import DELETED_KEYS, UPDATED_KEYS, _fact_row

    present = {r[0] for r in _read(f"SELECT fid FROM {FACTS}")}
    assert not (present & set(DELETED_KEYS)), (
        f"deleted keys came back: {sorted(present & set(DELETED_KEYS))}"
    )

    actual = _read(f"SELECT fid, amount FROM {FACTS} "
                   f"WHERE fid IN {tuple(UPDATED_KEYS)} ORDER BY fid")
    expected = [(fid, _normalise(_fact_row(fid, revision=2)["amount"]))
                for fid in sorted(UPDATED_KEYS)]
    assert actual == expected, (
        "an updated row is serving its superseded version"
    )


# ---------------------------------------------------------------------------
# the catalogue has to be worth running
# ---------------------------------------------------------------------------

def test_an_integer_sum_has_the_same_python_type_in_every_mode(dataset):
    """The same query must not answer with two types.

    DuckDB types an integer SUM as decimal128(38, 0). The buffered path casts
    that to int64; the streamed path used to hand the raw decimal to the
    caller, so ``execute`` returned ``int`` and ``stream`` returned ``Decimal``
    for one query — a difference decided only by how it was called.

    Asserted on the raw values rather than through ``_normalise``, which
    deliberately folds the two together so the rest of the suite compares
    values rather than representations. That folding would hide exactly this.
    """
    from supertable.data_reader import DataReader, Status, engine, query_sql

    sql = f"SELECT SUM(qty) AS total FROM {FACTS}"

    reader = DataReader(super_name=SUPER, organization=ORG, query=sql)
    frame, status, message = reader.execute(role_name=ROLE, engine=engine.DUCKDB)
    assert status is Status.OK, message
    buffered = list(frame.iter_rows())[0][0]

    streaming_reader = DataReader(super_name=SUPER, organization=ORG, query=sql)
    handle = streaming_reader.stream(ROLE, engine=engine.DUCKDB, batch_rows=17)
    try:
        declared = handle.schema.field("total").type
        streamed = [r["total"] for b in handle.batches() for r in b.to_pylist()][0]
    finally:
        handle.close()

    _columns, helper_rows, _meta = query_sql(
        ORG, SUPER, sql, 100_000, engine.DUCKDB, ROLE, source="test_suite")
    helper = helper_rows[0][0]

    expected = sum(r["qty"] for r in logical_rows())
    assert buffered == streamed == helper == expected, (
        f"integer SUM disagrees: buffered={buffered!r} streamed={streamed!r} "
        f"helper={helper!r} expected={expected!r}"
    )
    assert type(buffered) is type(streamed) is int, (
        f"an integer SUM should be int in both paths; got "
        f"buffered {type(buffered).__name__}, streamed {type(streamed).__name__} "
        f"(stream declared {declared})"
    )


def test_a_scaled_decimal_column_is_not_coerced_to_int(dataset):
    """The scale-zero rule must not touch a genuine DECIMAL.

    ``DECIMAL(12, 3)`` has a scale, so it is a real decimal and casting it to
    an integer would silently drop the fraction. Guards the blast radius of the
    scale-zero normalisation above.
    """
    import decimal as _decimal

    import pyarrow as pa

    from supertable.engine.arrow_result import normalize_arrow_schema

    schema = pa.schema([
        ("int_sum", pa.decimal128(38, 0)),     # an integer SUM -> becomes int64
        ("money", pa.decimal128(12, 3)),       # a real decimal -> untouched
        ("plain", pa.int64()),
    ])
    normalized, changed = normalize_arrow_schema(schema, for_pandas=False)
    assert changed
    assert normalized.field("int_sum").type == pa.int64()
    assert normalized.field("money").type == pa.decimal128(12, 3)
    assert normalized.field("plain").type == pa.int64()
    assert _decimal  # the type a scaled decimal must still produce


def test_the_catalogue_covers_the_features_it_claims():
    """A guard against the suite quietly shrinking."""
    features = {c.feature for c in CASES}
    required = {
        "aggregate", "group_by", "grouping_sets", "window", "date", "join",
        "set_ops", "subquery", "cte", "expression", "null_semantics",
        "string", "cast", "distinct", "row_bounds", "tombstone", "types",
    }
    assert required <= features, f"missing feature coverage: {required - features}"
    assert len(CASES) >= 80, f"only {len(CASES)} cases"
    assert len({c.case_id for c in CASES}) == len(CASES), "duplicate case ids"
