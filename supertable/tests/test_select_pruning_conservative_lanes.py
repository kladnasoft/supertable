"""Focused safety regressions for literal and SELECT-stat pruning lanes.

SELECT file pruning is intentionally narrower than generic footer-stat routing:
only comparison domains whose executor ordering/coercion semantics are proven
identical may exclude a file.  Every unsupported or ambiguous lane must retain
the file, even when its raw min/max looks disjoint from the predicate.
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import polars
import pytest

from supertable.data_classes import PredInterval
from supertable.processing import (
    STATS_SCHEMA,
    probe_ranges_from_df,
    prune_files_by_predicates,
)
from supertable.utils.sql_parser import SQLParser


TABLE = ("s", "events")
FILES = ["events/near.parquet", "events/far.parquet"]


def _literal_constraint(sql_literal: str):
    query = f"SELECT * FROM s.events e WHERE e.k = {sql_literal}"
    occurrences = SQLParser("s", query, "duckdb").get_predicate_constraints()
    return occurrences[TABLE][0].get("k")


@pytest.mark.parametrize(
    "sql_literal, expected_lane, expected_value",
    [
        (
            "DATE '2026-01-02'",
            "date",
            datetime(2026, 1, 2),
        ),
        (
            "TIMESTAMP '2026-01-02 03:04:05'",
            "timestamp",
            datetime(2026, 1, 2, 3, 4, 5),
        ),
        (
            "TIMESTAMPTZ '2026-01-02 03:04:05+02:00'",
            "timestamptz",
            datetime(2026, 1, 2, 1, 4, 5),
        ),
    ],
    ids=["date", "timestamp", "timestamptz-explicit-offset-to-utc"],
)
def test_temporal_literals_have_distinct_lanes(
    sql_literal: str,
    expected_lane: str,
    expected_value: datetime,
):
    assert _literal_constraint(sql_literal) == PredInterval(
        expected_lane,
        expected_value,
        True,
        expected_value,
        True,
    )


def test_timezone_less_timestamptz_literal_is_unextractable():
    # Its instant depends on the executor session timezone, which the parser
    # does not know.  An empty occurrence disables pruning for this scan.
    assert _literal_constraint(
        "TIMESTAMPTZ '2026-01-02 03:04:05'"
    ) is None


def test_datetime_shaped_date_literal_uses_duckdbs_midnight_value():
    assert _literal_constraint(
        "DATE '2026-01-02 23:59:59'"
    ) == PredInterval(
        "date",
        datetime(2026, 1, 2),
        True,
        datetime(2026, 1, 2),
        True,
    )


@pytest.mark.parametrize(
    "predicate",
    [
        "e.k = DATE '1500-01-01'",
        "e.k BETWEEN DATE '1500-01-01' AND DATE '1500-01-02'",
        "e.k IN (DATE '1500-01-01', DATE '1500-01-02')",
    ],
    ids=["comparison", "between", "in"],
)
def test_spark_ancient_date_predicates_fail_open(predicate: str):
    """Spark may rebase pre-Gregorian Parquet days per file metadata.

    A literal's proleptic-Gregorian day therefore need not equal an old
    writer's raw footer day even when Spark returns the row.  Parser-side DATE
    bounds cannot safely exclude files in Spark mode.  DuckDB remains eligible
    because its footer/literal representation is the one this parser models.
    """
    query = f"SELECT * FROM s.events e WHERE {predicate}"
    assert SQLParser(
        "s", query, "spark",
    ).get_predicate_constraints()[TABLE] == [{}]
    assert SQLParser(
        "s", query, "duckdb",
    ).get_predicate_constraints()[TABLE][0]["k"].lane == "date"


@pytest.mark.parametrize(
    "sql_literal",
    [
        "TIMESTAMP_S '2026-01-02 03:04:05.123456'",
        "TIMESTAMP_MS '2026-01-02 03:04:05.123456'",
        "TIMESTAMP_NS '2026-01-02 03:04:05.1234567'",
        "TIMESTAMP(3) '2026-01-02 03:04:05.123456'",
    ],
    ids=[
        "seconds", "milliseconds", "nanoseconds",
        "parameterized-milliseconds",
    ],
)
def test_resolution_changing_timestamp_literal_is_unextractable(sql_literal):
    # DuckDB rounds/coarsens these constants.  The source text is not the value
    # compared to a Parquet TIMESTAMP_MICROS column, so it cannot be a zone-map
    # bound without exactly implementing the cast.
    assert _literal_constraint(sql_literal) is None


def test_millisecond_cast_never_drops_the_quantized_contributing_file():
    contributor = "events/contributor.parquet"
    decoy = "events/source-text.parquet"
    stats = _stats_frame([
        _stats_row(
            contributor,
            "timestamp",
            datetime(2026, 1, 2, 3, 4, 5, 123000),
            datetime(2026, 1, 2, 3, 4, 5, 123000),
            logical_type="TIMESTAMP_NTZ_MICROS",
        ),
        _stats_row(
            decoy,
            "timestamp",
            datetime(2026, 1, 2, 3, 4, 5, 123456),
            datetime(2026, 1, 2, 3, 4, 5, 123456),
            logical_type="TIMESTAMP_NTZ_MICROS",
        ),
    ])
    query = (
        "SELECT * FROM s.events e WHERE e.k = "
        "TIMESTAMP_MS '2026-01-02 03:04:05.123456'"
    )
    occurrences = SQLParser(
        "s", query, "duckdb",
    ).get_predicate_constraints()[TABLE]

    assert duckdb.sql(
        "SELECT ts FROM (VALUES "
        "(TIMESTAMP '2026-01-02 03:04:05.123000'), "
        "(TIMESTAMP '2026-01-02 03:04:05.123456')) e(ts) "
        "WHERE ts = TIMESTAMP_MS '2026-01-02 03:04:05.123456'"
    ).fetchall() == [(datetime(2026, 1, 2, 3, 4, 5, 123000),)]

    # Before the resolution gate, the parser emitted .123456, causing pruning
    # to delete the real .123000 match while retaining only the decoy.
    assert prune_files_by_predicates(
        [contributor, decoy], stats, occurrences,
    ) == [contributor, decoy]


def test_nanosecond_strict_bound_never_drops_microsecond_contributor():
    contributor = "events/contributor.parquet"
    decoy = "events/decoy.parquet"
    stats = _stats_frame([
        _stats_row(
            contributor,
            "timestamp",
            datetime(2026, 1, 2, 3, 4, 5, 123456),
            datetime(2026, 1, 2, 3, 4, 5, 123456),
            logical_type="TIMESTAMP_NTZ_MICROS",
        ),
        _stats_row(
            decoy,
            "timestamp",
            datetime(2026, 1, 2, 3, 4, 5, 999999),
            datetime(2026, 1, 2, 3, 4, 5, 999999),
            logical_type="TIMESTAMP_NTZ_MICROS",
        ),
    ])
    query = (
        "SELECT * FROM s.events e WHERE e.k < "
        "TIMESTAMP_NS '2026-01-02 03:04:05.1234567'"
    )
    occurrences = SQLParser(
        "s", query, "duckdb",
    ).get_predicate_constraints()[TABLE]

    assert duckdb.sql(
        "SELECT ts FROM (VALUES "
        "(TIMESTAMP '2026-01-02 03:04:05.123456'), "
        "(TIMESTAMP '2026-01-02 03:04:05.999999')) e(ts) "
        "WHERE ts < TIMESTAMP_NS '2026-01-02 03:04:05.1234567'"
    ).fetchall() == [(datetime(2026, 1, 2, 3, 4, 5, 123456),)]

    # Python parses the literal text as .123456.  Treating that truncated
    # value as an exclusive upper bound would delete the genuine match.
    assert prune_files_by_predicates(
        [contributor, decoy], stats, occurrences,
    ) == [contributor, decoy]


def test_integral_looking_double_cast_is_unextractable():
    # DuckDB rounds this cast to 2**53, so both adjacent BIGINT values compare
    # equal.  Treating the source text as the exact integer 2**53+1 would let
    # zone-map pruning delete a contributing 2**53 file.
    assert _literal_constraint(
        "CAST('9007199254740993' AS DOUBLE)"
    ) is None


def _stats_row(
    file_path: str,
    lane: str,
    minimum,
    maximum,
    *,
    logical_type: str = "",
) -> dict:
    row = {column: None for column in STATS_SCHEMA}
    row.update({
        "file_path": file_path,
        "row_group_id": 0,
        "column_name": "k",
        "physical_type": {
            "bigint": "INT64",
            "double": "DOUBLE",
            "timestamp": "INT64",
            "string": "BYTE_ARRAY",
        }[lane],
        "logical_type": logical_type,
        "null_count": 0,
        "row_group_rows": 100,
        "compressed_bytes": 1000,
        "stats_available": True,
        "min_is_exact": True,
        "max_is_exact": True,
        f"min_{lane}": minimum,
        f"max_{lane}": maximum,
    })
    return row


def _stats_frame(rows: list[dict]) -> polars.DataFrame:
    return polars.DataFrame(rows, schema=STATS_SCHEMA)


@pytest.mark.parametrize(
    "stats, predicate",
    [
        (
            _stats_frame([
                _stats_row(FILES[0], "string", "a", "m", logical_type="STRING"),
                _stats_row(FILES[1], "string", "x", "z", logical_type="STRING"),
            ]),
            PredInterval("string", "b", True, "b", True),
        ),
        (
            _stats_frame([
                _stats_row(FILES[0], "double", 0.0, 9.0),
                _stats_row(FILES[1], "double", 100.0, 109.0),
            ]),
            PredInterval("numeric", 5.0, True, 5.0, True),
        ),
        (
            _stats_frame([
                _stats_row(
                    FILES[0],
                    "timestamp",
                    datetime(2026, 1, 1),
                    datetime(2026, 1, 5),
                    logical_type="TIMESTAMP",
                ),
                _stats_row(
                    FILES[1],
                    "timestamp",
                    datetime(2026, 6, 1),
                    datetime(2026, 6, 5),
                    logical_type="TIMESTAMP",
                ),
            ]),
            PredInterval(
                "timestamp",
                datetime(2026, 1, 3),
                True,
                datetime(2026, 1, 3),
                True,
            ),
        ),
    ],
    ids=["string", "double", "historical-ambiguous-timestamp"],
)
def test_select_pruning_retains_unsafe_stored_lanes(
    stats: polars.DataFrame,
    predicate: PredInterval,
):
    assert prune_files_by_predicates(
        FILES,
        stats,
        [{"k": predicate}],
    ) == FILES


@pytest.mark.parametrize(
    "predicate_lane, stored_logical_type",
    [
        ("date", "TIMESTAMP_NTZ_MICROS"),
        ("date", "TIMESTAMP_TZ_MICROS"),
        ("timestamp", "DATE"),
        ("timestamp", "TIMESTAMP_TZ_MICROS"),
        ("timestamptz", "DATE"),
        ("timestamptz", "TIMESTAMP_NTZ_MICROS"),
    ],
)
def test_select_pruning_retains_mismatched_temporal_kinds(
    predicate_lane: str,
    stored_logical_type: str,
):
    stats = _stats_frame([
        _stats_row(
            FILES[0],
            "timestamp",
            datetime(2026, 1, 1),
            datetime(2026, 1, 5),
            logical_type=stored_logical_type,
        ),
        _stats_row(
            FILES[1],
            "timestamp",
            datetime(2026, 6, 1),
            datetime(2026, 6, 5),
            logical_type=stored_logical_type,
        ),
    ])
    point = datetime(2026, 1, 3)

    assert prune_files_by_predicates(
        FILES,
        stats,
        [{"k": PredInterval(predicate_lane, point, True, point, True)}],
    ) == FILES


def test_select_pruning_still_prunes_exact_signed_bigint_ranges():
    stats = _stats_frame([
        _stats_row(FILES[0], "bigint", 0, 9),
        _stats_row(FILES[1], "bigint", 100, 109),
    ])

    assert prune_files_by_predicates(
        FILES,
        stats,
        [{"k": PredInterval("numeric", 5, True, 5, True)}],
    ) == [FILES[0]]


def test_select_pruning_matches_footer_columns_case_insensitively():
    near = _stats_row(FILES[0], "bigint", 0, 9)
    far = _stats_row(FILES[1], "bigint", 100, 109)
    near["column_name"] = "K"
    far["column_name"] = "K"

    assert prune_files_by_predicates(
        FILES,
        _stats_frame([near, far]),
        [{"k": PredInterval("numeric", 5, True, 5, True)}],
    ) == [FILES[0]]


def test_case_colliding_footer_columns_fail_open_for_that_row_group():
    ambiguous_match = _stats_row(FILES[0], "bigint", 5, 5)
    ambiguous_match["column_name"] = "K"
    ambiguous_decoy = _stats_row(FILES[0], "bigint", 99, 99)
    ambiguous_decoy["column_name"] = "k"
    known_match = _stats_row(FILES[1], "bigint", 5, 5)

    kept = prune_files_by_predicates(
        FILES,
        _stats_frame([ambiguous_match, ambiguous_decoy, known_match]),
        [{"k": PredInterval("numeric", 5, True, 5, True)}],
    )

    assert kept == FILES


def test_narrow_projection_still_rejects_a_second_populated_lane():
    malformed = _stats_row(FILES[0], "bigint", 100, 100)
    malformed["min_string"] = "corrupt"
    malformed["max_string"] = "corrupt"
    decoy = _stats_row(FILES[1], "bigint", 5, 5)

    kept = prune_files_by_predicates(
        FILES,
        _stats_frame([malformed, decoy]),
        [{"k": PredInterval("numeric", 5, True, 5, True)}],
    )

    # A damaged row with two typed lanes is unknown.  The matching decoy makes
    # sure this exercises the per-file guard rather than the never-empty guard.
    assert kept == FILES


def test_zero_pruned_files_return_the_original_list_identity():
    stats = _stats_frame([
        _stats_row(FILES[0], "bigint", 0, 9),
        _stats_row(FILES[1], "bigint", 0, 9),
    ])

    kept = prune_files_by_predicates(
        FILES,
        stats,
        [{"K": PredInterval("numeric", 5, True, 5, True)}],
    )

    assert kept is FILES


@pytest.mark.parametrize(
    "frame",
    [
        polars.DataFrame({"k": [1.0, float("nan"), 2.0]}),
        polars.DataFrame(
            {"k": [2**63, 2**63 + 1]},
            schema={"k": polars.UInt64},
        ),
    ],
    ids=["float-containing-nan", "uint64"],
)
def test_probe_range_is_unknown_for_unsafe_numeric_columns(
    frame: polars.DataFrame,
):
    assert probe_ranges_from_df(frame, ["k"])["k"] is None
