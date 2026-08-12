"""Conservative boundary tests for footer statistics extraction/consumption."""
from __future__ import annotations

import io
from datetime import datetime, timezone

import polars
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from supertable import processing as processing_mod
from supertable.data_classes import PredInterval
from supertable.processing import (
    STATS_SCHEMA,
    _STATS_CACHE,
    _conform_stats_schema,
    _logical_type_name,
    _stats_rows_for_metadata,
    _stored_select_lane,
    _stored_select_lane_values,
    load_stats,
    probe_ranges_from_df,
    prune_files_by_predicates,
    prune_overlapping_files_by_stats,
)


@pytest.fixture(autouse=True)
def _isolate_stats_cache():
    _STATS_CACHE.clear()
    yield
    _STATS_CACHE.clear()


def _metadata(values, arrow_type):
    sink = io.BytesIO()
    pq.write_table(
        pa.table({"k": pa.array(values, type=arrow_type)}),
        sink,
        write_statistics=True,
    )
    return pq.read_metadata(io.BytesIO(sink.getvalue()))


def _row(
    file_path: str,
    minimum,
    maximum,
    *,
    stats_available=True,
):
    row = {name: None for name in STATS_SCHEMA}
    row.update({
        "file_path": file_path,
        "row_group_id": 0,
        "column_name": "k",
        "physical_type": "INT64",
        "logical_type": "",
        "min_bigint": minimum,
        "max_bigint": maximum,
        "null_count": 0,
        "row_group_rows": 1,
        "compressed_bytes": 8,
        "stats_available": stats_available,
        "min_is_exact": True,
        "max_is_exact": True,
    })
    return row


@pytest.mark.parametrize(
    "unit, timezone_name, marker, select_lane",
    [
        ("ms", None, "TIMESTAMP_NTZ_MILLIS", "timestamp"),
        ("us", None, "TIMESTAMP_NTZ_MICROS", "timestamp"),
        ("ms", "UTC", "TIMESTAMP_TZ_MILLIS", "timestamptz"),
        ("us", "UTC", "TIMESTAMP_TZ_MICROS", "timestamptz"),
    ],
)
def test_real_timestamp_millis_micros_preserve_selectable_markers(
    unit,
    timezone_name,
    marker,
    select_lane,
):
    tzinfo = timezone.utc if timezone_name else None
    md = _metadata(
        [datetime(2026, 1, 1, tzinfo=tzinfo), datetime(2026, 1, 2, tzinfo=tzinfo)],
        pa.timestamp(unit, tz=timezone_name),
    )
    stat = md.row_group(0).column(0).statistics
    row = _stats_rows_for_metadata("f.parquet", md)[0]

    assert _logical_type_name(stat) == marker
    assert row["logical_type"] == marker
    assert row["stats_available"] is True
    assert _stored_select_lane(row) == (
        select_lane,
        datetime(2026, 1, 1),
        datetime(2026, 1, 2),
    )


@pytest.mark.parametrize(
    "timezone_name, marker",
    [
        (None, "TIMESTAMP_NTZ_NANOS"),
        ("UTC", "TIMESTAMP_TZ_NANOS"),
    ],
)
def test_real_timestamp_nanos_are_unknown_to_select(timezone_name, marker):
    # Both bounds differ below a microsecond and collapse in STATS_SCHEMA's us
    # lane.  The resolution marker must prevent SELECT from trusting that loss.
    md = _metadata(
        [1767225600000000001, 1767225600000000999],
        pa.timestamp("ns", tz=timezone_name),
    )
    stat = md.row_group(0).column(0).statistics
    row = _stats_rows_for_metadata("f.parquet", md)[0]
    built = polars.DataFrame([row], schema=STATS_SCHEMA)
    built_row = next(built.iter_rows(named=True))

    assert _logical_type_name(stat) == marker
    assert built_row["logical_type"] == marker
    assert _stored_select_lane(built_row) is None


@pytest.mark.parametrize("value", [2**63, 2**64 - 1])
def test_real_uint64_beyond_int64_builds_schema_frame_without_raising(value):
    md = _metadata([value], pa.uint64())
    row = _stats_rows_for_metadata("u.parquet", md)[0]
    built = polars.DataFrame([row], schema=STATS_SCHEMA)

    assert built.schema == STATS_SCHEMA
    assert built["stats_available"].item() is False
    assert built["min_bigint"].item() is None
    assert built["max_bigint"].item() is None


@pytest.mark.parametrize(
    "frame",
    [
        polars.DataFrame({"k": [1.0, float("nan"), 2.0]}),
        polars.DataFrame({"k": [float("nan")]}),
    ],
    ids=["mixed", "all-nan"],
)
def test_float_nan_probe_is_unknown(frame):
    assert probe_ranges_from_df(frame, ["k"])["k"] is None


def test_real_infinite_footer_values_do_not_break_stats_frame():
    md = _metadata([float("-inf"), 0.0, float("inf")], pa.float64())
    row = _stats_rows_for_metadata("inf.parquet", md)[0]
    built = polars.DataFrame([row], schema=STATS_SCHEMA)

    assert built["min_double"].item() == float("-inf")
    assert built["max_double"].item() == float("inf")
    # SELECT deliberately never trusts double footer ranges.
    assert _stored_select_lane(next(built.iter_rows(named=True))) is None


def test_inexact_flags_do_not_invalidate_conservative_bounds():
    # Parquet permits compact non-value extrema (e.g. string B/C bounds) while
    # requiring that they still bound every value.  Exactness is informational,
    # not permission to reverse/narrow the interval.
    row = _row("f.parquet", 0, 9)
    row["min_is_exact"] = False
    row["max_is_exact"] = False
    assert _stored_select_lane(row) == ("bigint", 0, 9)


def test_ambiguous_or_untyped_legacy_lane_is_unknown():
    multiple = _row("f.parquet", 0, 9)
    multiple["min_double"], multiple["max_double"] = 0.0, 9.0
    assert _stored_select_lane(multiple) is None

    non_boolean_availability = _row("f.parquet", 0, 9)
    non_boolean_availability["stats_available"] = 1
    assert _stored_select_lane(non_boolean_availability) is None

    wrong_endpoint_type = _row("f.parquet", 0, 9)
    wrong_endpoint_type["min_bigint"] = "0"
    assert _stored_select_lane(wrong_endpoint_type) is None


@pytest.mark.parametrize(
    "values,expected",
    [
        (
            (True, "INT64", "INT", 0, 9, None, None, False),
            ("bigint", 0, 9),
        ),
        (
            (True, "INT64", "INT", 0, None, None, None, False),
            None,
        ),
        (
            (True, "INT64", "INT", 0, 9, None, None, True),
            None,
        ),
        (
            (
                True, "INT32", "DATE", None, None,
                datetime(2026, 1, 1), datetime(2026, 1, 2), False,
            ),
            (
                "date", datetime(2026, 1, 1), datetime(2026, 1, 2),
            ),
        ),
        (
            (
                True, "INT64", "TIMESTAMP_NTZ_MICROS", 0, 9,
                datetime(2026, 1, 1), datetime(2026, 1, 2), False,
            ),
            None,
        ),
    ],
    ids=["bigint", "half-lane", "other-lane", "date", "two-safe-lanes"],
)
def test_tuple_select_lane_decoder_preserves_conservative_contract(
    values,
    expected,
):
    assert _stored_select_lane_values(*values) == expected


@pytest.mark.parametrize(
    "physical_type,logical_type",
    [
        ("DOUBLE", ""),
        ("BYTE_ARRAY", ""),
        ("INT64", "DATE"),
        ("INT32", "DECIMAL"),
    ],
)
def test_bigint_select_lane_requires_consistent_footer_type_markers(
    physical_type,
    logical_type,
):
    row = _row("f.parquet", 0, 9)
    row["physical_type"] = physical_type
    row["logical_type"] = logical_type

    assert _stored_select_lane(row) is None


@pytest.mark.parametrize(
    "logical_type,physical_type",
    [
        ("DATE", "INT64"),
        ("TIMESTAMP_NTZ_MICROS", "INT32"),
        ("TIMESTAMP_TZ_MILLIS", "INT32"),
    ],
)
def test_temporal_select_lane_requires_matching_physical_type(
    logical_type,
    physical_type,
):
    row = _row("f.parquet", None, None)
    row["min_timestamp"] = datetime(2026, 1, 1)
    row["max_timestamp"] = datetime(2026, 1, 2)
    row["logical_type"] = logical_type
    row["physical_type"] = physical_type

    assert _stored_select_lane(row) is None


@pytest.mark.parametrize(
    "bad_min,bad_max",
    [
        (10, 0),
        (float("nan"), 10.0),
        (0.0, float("nan")),
    ],
    ids=["reversed", "nan-min", "nan-max"],
)
def test_malformed_stored_bounds_retain_files_on_read_and_write(bad_min, bad_max):
    bad = _row("bad.parquet", None, None)
    if isinstance(bad_min, float) or isinstance(bad_max, float):
        bad["physical_type"] = "DOUBLE"
        bad["min_bigint"] = bad["max_bigint"] = None
        bad["min_double"], bad["max_double"] = bad_min, bad_max
    else:
        bad["min_bigint"], bad["max_bigint"] = bad_min, bad_max
    decoy = _row("decoy.parquet", 100, 100)
    stats = polars.DataFrame([bad, decoy], schema=STATS_SCHEMA)

    read_survivors = prune_files_by_predicates(
        ["bad.parquet", "decoy.parquet"],
        stats,
        [{"k": PredInterval("numeric", 5, True, 5, True)}],
    )
    assert "bad.parquet" in read_survivors

    candidates = {
        ("bad.parquet", True, 1),
        ("decoy.parquet", True, 1),
    }
    write_survivors = prune_overlapping_files_by_stats(
        candidates,
        stats,
        {"k": ("bigint", 5, 5)},
    )
    assert ("bad.parquet", True, 1) in write_survivors


def test_unknown_footer_null_count_is_stored_as_unknown():
    # A minimal metadata double with a usable bound but no null_count.  Unknown
    # must not be rewritten as zero (which would falsely assert no NULLs).
    class _Stat:
        null_count = None
        logical_type = None
        has_min_max = True
        min = 1
        max = 2

    class _Col:
        path_in_schema = "k"
        physical_type = "INT64"
        is_stats_set = True
        statistics = _Stat()
        total_compressed_size = 8

    class _Group:
        num_rows = 2
        num_columns = 1

        @staticmethod
        def column(_index):
            return _Col()

    class _Metadata:
        num_row_groups = 1

        @staticmethod
        def row_group(_index):
            return _Group()

    row = _stats_rows_for_metadata("unknown-null.parquet", _Metadata())[0]
    built = polars.DataFrame([row], schema=STATS_SCHEMA)

    assert row["null_count"] is None
    assert built["null_count"].item() is None
    assert built["stats_available"].item() is True


def test_conform_corrupt_legacy_schema_casts_or_nulls_and_drops_bad_identity():
    legacy = polars.DataFrame({
        "file_path": ["good.parquet", None],
        "row_group_id": polars.Series([0, 1], dtype=polars.Int32),
        "column_name": ["k", "k"],
        "min_bigint": polars.Series([5, 6], dtype=polars.Int32),
        "max_bigint": polars.Series([9, 10], dtype=polars.Int32),
        "stats_available": polars.Series([1, 7], dtype=polars.Int8),
        "min_timestamp": [1, 2],
        "min_double": [1, 2],
        "min_string": [1, 2],
    })

    conformed = _conform_stats_schema(legacy)

    assert conformed.schema == STATS_SCHEMA
    assert conformed.height == 1
    row = next(conformed.iter_rows(named=True))
    assert row["file_path"] == "good.parquet"
    assert row["row_group_id"] == 0
    assert (row["min_bigint"], row["max_bigint"]) == (5, 9)
    assert row["stats_available"] is True
    assert row["min_timestamp"] is None
    assert row["min_double"] is None
    assert row["min_string"] is None


def test_load_stats_conforms_legacy_schema_before_caching(monkeypatch):
    legacy = polars.DataFrame({
        "file_path": ["f.parquet"],
        "row_group_id": polars.Series([0], dtype=polars.Int32),
        "column_name": ["k"],
        "min_bigint": polars.Series([5], dtype=polars.Int32),
        "max_bigint": polars.Series([9], dtype=polars.Int32),
        "stats_available": polars.Series([1], dtype=polars.Int8),
    })
    monkeypatch.setattr(
        processing_mod, "_read_parquet_safe", lambda *_args, **_kwargs: legacy,
    )

    loaded = load_stats("legacy/stats/v1.parquet")

    assert loaded.schema == STATS_SCHEMA
    assert loaded["min_bigint"].item() == 5
    assert loaded["stats_available"].item() is True
    # The second load is the same conformed cached object, not the unsafe input.
    assert load_stats("legacy/stats/v1.parquet") is loaded
