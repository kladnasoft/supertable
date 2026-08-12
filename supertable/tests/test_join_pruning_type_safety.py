"""Regression tests for executor/footer type-semantics mismatches.

These are kept-superset tests: every file shown to contain a key that joins
under DuckDB's execution semantics must survive the metadata-only optimiser.
"""

from __future__ import annotations

import io
from datetime import datetime

import duckdb
import polars
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.data_classes import JoinEdge
from supertable.engine.join_pruner import (
    plan_file_pruning_for_query,
    prune_files_across_joins,
)
from supertable.processing import STATS_SCHEMA


SUPER = "s"
A = (SUPER, "a")
B = (SUPER, "b")


def _row(
    file_path: str,
    lane: str,
    lo,
    hi,
    *,
    logical_type: str = "",
) -> dict:
    row = {name: None for name in STATS_SCHEMA}
    row.update({
        "file_path": file_path,
        "row_group_id": 0,
        "column_name": "k",
        "physical_type": "INT64",
        "logical_type": logical_type,
        "null_count": 0,
        "row_group_rows": 2,
        "compressed_bytes": 8,
        "stats_available": True,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    if lane == "bigint":
        row["min_bigint"], row["max_bigint"] = lo, hi
    elif lane == "double":
        row["physical_type"] = "DOUBLE"
        row["min_double"], row["max_double"] = lo, hi
    elif lane == "string":
        row["physical_type"] = "BYTE_ARRAY"
        row["min_string"], row["max_string"] = lo, hi
    elif lane == "timestamp":
        row["physical_type"] = (
            "INT32" if logical_type == "DATE" else "INT64"
        )
        row["min_timestamp"], row["max_timestamp"] = lo, hi
    else:  # pragma: no cover - test helper guard
        raise AssertionError(lane)
    return row


def _stats(*rows: dict) -> polars.DataFrame:
    return polars.DataFrame(list(rows), schema=STATS_SCHEMA)


def _plan(a_files, b_files, a_stats, b_stats, *, allowed_lanes=None):
    return prune_files_across_joins(
        [JoinEdge(A, "k", B, "k")],
        {},
        {A: list(a_files), B: list(b_files)},
        {A: a_stats, B: b_stats},
        allow_empty=False,
        allowed_lanes=allowed_lanes,
    )


def test_nocase_string_equality_keeps_binary_case_variant():
    con = duckdb.connect()
    con.execute("PRAGMA default_collation='nocase'")
    assert con.sql(
        "SELECT count(*) FROM (VALUES ('a')) a(k) "
        "JOIN (VALUES ('A'), ('a')) b(k) USING(k)"
    ).fetchone()[0] == 2

    plan = _plan(
        ["a"], ["upper", "lower"],
        _stats(_row("a", "string", "a", "a")),
        _stats(
            _row("upper", "string", "A", "A"),
            _row("lower", "string", "a", "a"),
        ),
    )
    assert set(plan.survivors[B]) == {"upper", "lower"}


def test_nan_hidden_from_parquet_bounds_keeps_nan_join_file():
    # This is the real footer behaviour the synthetic stats below model: NaN
    # is a joinable DuckDB value but is omitted from PyArrow's min/max.
    sink = io.BytesIO()
    pq.write_table(pa.table({"k": [2.0, float("nan")]}), sink)
    footer = pq.read_metadata(io.BytesIO(sink.getvalue()))
    stat = footer.row_group(0).column(0).statistics
    assert (stat.min, stat.max) == (2.0, 2.0)
    assert duckdb.sql(
        "SELECT 'NaN'::DOUBLE = 'NaN'::DOUBLE"
    ).fetchone()[0]

    plan = _plan(
        ["a_mixed"], ["b_mixed", "b_one"],
        _stats(_row("a_mixed", "double", 1.0, 1.0)),
        _stats(
            _row("b_mixed", "double", 2.0, 2.0),
            _row("b_one", "double", 1.0, 1.0),
        ),
    )
    assert set(plan.survivors[B]) == {"b_mixed", "b_one"}


def test_bigint_double_rounding_match_keeps_both_integer_files():
    n = 2**53
    assert duckdb.sql(
        f"SELECT {n + 1}::BIGINT = {n}::DOUBLE"
    ).fetchone()[0]

    plan = _plan(
        ["exact", "rounded"], ["double"],
        _stats(
            _row("exact", "bigint", n, n),
            _row("rounded", "bigint", n + 1, n + 1),
        ),
        _stats(_row("double", "double", float(n), float(n))),
    )
    assert set(plan.survivors[A]) == {"exact", "rounded"}


def test_date_timestamptz_session_coercion_keeps_matching_instant():
    con = duckdb.connect()
    con.execute("SET TimeZone='Europe/Budapest'")
    assert con.sql(
        "SELECT DATE '2026-01-01' = "
        "TIMESTAMPTZ '2025-12-31 23:00:00+00', "
        "DATE '2026-01-01' = "
        "TIMESTAMPTZ '2026-01-01 00:00:00+00'"
    ).fetchone() == (True, False)

    midnight = datetime(2026, 1, 1)
    match = datetime(2025, 12, 31, 23)
    plan = _plan(
        ["date"], ["match", "decoy"],
        _stats(_row(
            "date", "timestamp", midnight, midnight,
            logical_type="DATE",
        )),
        _stats(
            _row(
                "match", "timestamp", match, match,
                logical_type="TIMESTAMP_TZ_MICROS",
            ),
            _row(
                "decoy", "timestamp", midnight, midnight,
                logical_type="TIMESTAMP_TZ_MICROS",
            ),
        ),
    )
    assert set(plan.survivors[B]) == {"match", "decoy"}


def test_executor_lane_gate_treats_temporal_join_ranges_as_unknown():
    # AUTO/Spark reads Parquet NTZ values with executor/session-time-zone
    # semantics.  The reader therefore permits only the common exact numeric
    # lane; a disabled temporal range must keep every destination file.
    before_gap = datetime(2026, 3, 29, 2, 30)
    after_gap = datetime(2026, 3, 29, 3, 30)
    plan = _plan(
        ["source"], ["same_raw", "normalized_peer"],
        _stats(_row(
            "source", "timestamp", before_gap, before_gap,
            logical_type="TIMESTAMP_NTZ_MICROS",
        )),
        _stats(
            _row(
                "same_raw", "timestamp", before_gap, before_gap,
                logical_type="TIMESTAMP_NTZ_MICROS",
            ),
            _row(
                "normalized_peer", "timestamp", after_gap, after_gap,
                logical_type="TIMESTAMP_NTZ_MICROS",
            ),
        ),
        allowed_lanes={"numeric"},
    )
    assert set(plan.survivors[B]) == {"same_raw", "normalized_peer"}


def test_spark_query_wrapper_applies_temporal_join_lane_gate():
    """The public SQL wrapper must be as conservative as DataReader."""
    before_gap = datetime(2026, 3, 29, 2, 30)
    after_gap = datetime(2026, 3, 29, 3, 30)

    plan = plan_file_pruning_for_query(
        SUPER,
        "SELECT * FROM s.a a JOIN s.b b ON a.k = b.k",
        "spark",
        {A: ["source"], B: ["same_raw", "normalized_peer"]},
        {
            A: _stats(_row(
                "source", "timestamp", before_gap, before_gap,
                logical_type="TIMESTAMP_NTZ_MICROS",
            )),
            B: _stats(
                _row(
                    "same_raw", "timestamp", before_gap, before_gap,
                    logical_type="TIMESTAMP_NTZ_MICROS",
                ),
                _row(
                    "normalized_peer", "timestamp", after_gap, after_gap,
                    logical_type="TIMESTAMP_NTZ_MICROS",
                ),
            ),
        },
        allow_empty=False,
    )

    assert set(plan.survivors[B]) == {"same_raw", "normalized_peer"}
