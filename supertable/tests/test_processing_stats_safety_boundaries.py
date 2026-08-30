"""Conservative boundary tests for footer statistics extraction/consumption."""
from __future__ import annotations

import io
import logging
from datetime import datetime, timezone

import polars
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from supertable import processing as processing_mod
from supertable.data_classes import PredInterval
from supertable.processing import (
    MAX_PLANNING_STATS_ROWS,
    MAX_SHOW_STATS_STRING_BYTES,
    STATS_SCHEMA,
    _STATS_CACHE,
    _conform_stats_schema,
    _logical_type_name,
    _stats_rows_for_metadata,
    _stored_select_lane,
    _stored_select_lane_values,
    load_bounded_stats_diagnostic,
    load_bounded_stats_for_planning,
    load_stats,
    probe_ranges_from_df,
    prune_files_by_predicates,
    prune_overlapping_files_by_stats,
)
from supertable.storage.local_storage import LocalStorage


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


def test_bounded_stats_diagnostic_seals_and_projects_canonical_columns(tmp_path):
    frame = polars.DataFrame([_row("data/f.parquet", 1, 9)], schema=STATS_SCHEMA)
    artifact = frame.to_arrow().append_column(
        "untrusted_extra",
        pa.array(["must-not-escape"]),
    )
    storage = LocalStorage(root=str(tmp_path))
    path = "lake/table/stats/stats-v1.parquet"
    storage.write_parquet(artifact, path)

    loaded = load_bounded_stats_diagnostic(
        path,
        expected_rows=1,
        storage=storage,
    )

    assert loaded.schema == STATS_SCHEMA
    assert loaded.height == 1
    assert loaded["file_path"].item() == "data/f.parquet"
    assert "untrusted_extra" not in loaded.columns


def test_bounded_planning_stats_reuses_only_an_exact_admitted_cache(
    monkeypatch,
):
    frame = polars.DataFrame([_row("data/f.parquet", 1, 9)], schema=STATS_SCHEMA)
    identity = "bounded-planning-cache"
    _STATS_CACHE.put(identity, frame)
    monkeypatch.setattr(
        processing_mod,
        "load_bounded_stats_diagnostic",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("an admitted cache hit reached storage")
        ),
    )

    loaded = load_bounded_stats_for_planning(
        "lake/table/stats/stats-v1.parquet",
        expected_rows=1,
        cache_identity=identity,
    )

    assert loaded is frame


@pytest.mark.parametrize(
    ("expected_rows", "max_decoded_bytes"),
    [(2, 16 * 1024 * 1024), (1, 1)],
)
def test_bounded_planning_stats_rejects_an_invalid_cache_without_decode(
    monkeypatch, expected_rows, max_decoded_bytes,
):
    frame = polars.DataFrame([_row("data/f.parquet", 1, 9)], schema=STATS_SCHEMA)
    identity = "invalid-bounded-planning-cache"
    _STATS_CACHE.put(identity, frame)
    monkeypatch.setattr(
        processing_mod,
        "load_bounded_stats_diagnostic",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("an invalid cache fell through to storage")
        ),
    )

    with pytest.raises(ValueError, match="planning seal"):
        load_bounded_stats_for_planning(
            "lake/table/stats/stats-v1.parquet",
            expected_rows=expected_rows,
            cache_identity=identity,
            max_decoded_bytes=max_decoded_bytes,
        )


def test_bounded_planning_stats_rejects_declared_rows_before_decode(monkeypatch):
    monkeypatch.setattr(
        processing_mod,
        "load_bounded_stats_diagnostic",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("oversized declared rows reached storage")
        ),
    )

    with pytest.raises(ValueError, match="row count exceeds"):
        load_bounded_stats_for_planning(
            "lake/table/stats/stats-v1.parquet",
            expected_rows=MAX_PLANNING_STATS_ROWS + 1,
        )


def test_bounded_stats_diagnostic_rejects_footer_size_before_decode(
    tmp_path, monkeypatch,
):
    frame = polars.DataFrame([_row("data/f.parquet", 1, 9)], schema=STATS_SCHEMA)
    storage = LocalStorage(root=str(tmp_path))
    path = "lake/table/stats/stats-v1.parquet"
    storage.write_parquet(frame.to_arrow(), path)
    parsed = pq.ParquetFile(tmp_path / path)

    class FooterOnlyParquet:
        metadata = parsed.metadata
        schema_arrow = parsed.schema_arrow

        @staticmethod
        def iter_batches(**_kwargs):
            raise AssertionError("oversized footer chunks reached the decoder")

    monkeypatch.setattr(
        processing_mod.pq,
        "ParquetFile",
        lambda _source, **_kwargs: FooterOnlyParquet(),
    )

    with pytest.raises(ValueError, match="decoded data exceeds"):
        load_bounded_stats_diagnostic(
            path,
            expected_rows=1,
            storage=storage,
            max_decoded_bytes=1,
        )


def test_bounded_stats_diagnostic_rejects_dictionary_amplification_before_cast(
    tmp_path, monkeypatch,
):
    repeated = "x" * MAX_SHOW_STATS_STRING_BYTES
    rows = []
    for index in range(256):
        row = _row(f"data/f-{index}.parquet", 1, 9)
        row["min_string"] = repeated
        rows.append(row)
    frame = polars.DataFrame(rows, schema=STATS_SCHEMA)
    storage = LocalStorage(root=str(tmp_path))
    path = "lake/table/stats/dictionary-amplification.parquet"
    storage.write_parquet(frame.to_arrow(), path)

    artifact_path = tmp_path / path
    metadata = pq.read_metadata(artifact_path)
    encoded_string_bytes = sum(
        metadata.row_group(row_group).column(column).total_uncompressed_size
        for row_group in range(metadata.num_row_groups)
        for column in range(metadata.num_columns)
        if metadata.row_group(row_group).column(column).path_in_schema
        == "min_string"
    )
    assert artifact_path.stat().st_size < 256 * 1024
    assert encoded_string_bytes < 1024 * 1024

    def expansion_must_not_run(*_args, **_kwargs):
        raise AssertionError("dictionary string was expanded before admission")

    monkeypatch.setattr(processing_mod.pc, "cast", expansion_must_not_run)
    with pytest.raises(ValueError, match="decoded data exceeds"):
        load_bounded_stats_diagnostic(
            path,
            expected_rows=256,
            storage=storage,
            max_decoded_bytes=1024 * 1024,
        )


def test_bounded_stats_diagnostic_rejects_oversized_dictionary_scalar(
    tmp_path, monkeypatch,
):
    row = _row("data/f.parquet", 1, 9)
    row["min_string"] = "x" * (MAX_SHOW_STATS_STRING_BYTES + 1)
    frame = polars.DataFrame([row], schema=STATS_SCHEMA)
    storage = LocalStorage(root=str(tmp_path))
    path = "lake/table/stats/oversized-string.parquet"
    storage.write_parquet(frame.to_arrow(), path)

    def expansion_must_not_run(*_args, **_kwargs):
        raise AssertionError("oversized dictionary scalar reached a cast")

    monkeypatch.setattr(processing_mod.pc, "cast", expansion_must_not_run)
    with pytest.raises(ValueError, match="string scalar exceeds"):
        load_bounded_stats_diagnostic(
            path,
            expected_rows=1,
            storage=storage,
        )


def test_bounded_stats_diagnostic_rejects_noncanonical_scalar_type(tmp_path):
    frame = polars.DataFrame([_row("data/f.parquet", 1, 9)], schema=STATS_SCHEMA)
    artifact = frame.to_arrow()
    file_path_index = artifact.schema.get_field_index("file_path")
    artifact = artifact.set_column(
        file_path_index,
        pa.field("file_path", pa.binary()),
        pa.array([b"data/f.parquet"]),
    )
    storage = LocalStorage(root=str(tmp_path))
    path = "lake/table/stats/noncanonical-scalar.parquet"
    storage.write_parquet(artifact, path)

    with pytest.raises(RuntimeError, match="unsafe scalar type"):
        load_bounded_stats_diagnostic(
            path,
            expected_rows=1,
            storage=storage,
        )


def test_load_stats_uses_explicitly_pinned_storage_for_probe_and_read(
    monkeypatch,
):
    expected = polars.DataFrame(schema=STATS_SCHEMA)

    class PinnedStorage:
        def __init__(self):
            self.calls = []

        def exists(self, path):
            self.calls.append(("exists", path))
            return True

        def read_parquet(self, path):
            self.calls.append(("read_parquet", path))
            return expected.to_arrow()

    pinned = PinnedStorage()

    def sticky_global_must_not_run():
        raise AssertionError("load_stats switched to sticky global storage")

    monkeypatch.setattr(processing_mod, "_get_storage", sticky_global_must_not_run)
    loaded = load_stats(
        "provider/stats/v1.parquet",
        storage=pinned,
        cache_identity="pinned-provider-stats-v1",
        allow_cache=False,
    )

    assert loaded is not None
    assert loaded.schema == STATS_SCHEMA
    assert pinned.calls == [
        ("exists", "provider/stats/v1.parquet"),
        ("read_parquet", "provider/stats/v1.parquet"),
    ]


def test_build_stats_uses_one_pinned_storage_for_directory_and_write(
    monkeypatch,
):
    class PinnedStorage:
        def __init__(self):
            self.directories = []
            self.objects = {}

        def makedirs(self, path):
            self.directories.append(path)

        def write_bytes(self, path, data):
            self.objects[path] = data

    pinned = PinnedStorage()

    def sticky_global_must_not_run():
        raise AssertionError("build_stats switched to sticky global storage")

    monkeypatch.setattr(processing_mod, "_get_storage", sticky_global_must_not_run)
    new_rows = polars.DataFrame(
        [_row("provider/data/part.parquet", 1, 1)],
        schema=STATS_SCHEMA,
    )

    path, combined = processing_mod.build_stats_file(
        stats_dir="provider/stats",
        prev_stats_path=None,
        new_rows=new_rows,
        removed_files=None,
        compression_level=1,
        storage=pinned,
    )

    assert path is not None
    assert combined is not None and combined.height == 1
    assert len(pinned.directories) == 1
    assert path in pinned.objects


def test_parquet_read_logs_and_required_errors_redact_storage_credentials(
    caplog,
):
    url = (
        "https://alice:password@example.invalid/private/data.parquet"
        "?X-Amz-Credential=credential-secret&X-Amz-Signature=signed-secret"
        "#fragment-secret"
    )

    class ExplodingStorage:
        def exists(self, path):
            return True

        def read_parquet(self, path):
            raise RuntimeError("backend-secret-from-exception")

    with caplog.at_level(logging.WARNING):
        assert processing_mod._read_parquet_safe(
            url, storage=ExplodingStorage(),
        ) is None
        with pytest.raises(RuntimeError) as raised:
            processing_mod._read_parquet_safe(
                url, storage=ExplodingStorage(), required=True,
            )

    observed = caplog.text + str(raised.value)
    assert "https://example.invalid/<redacted-path>" in observed
    for secret in (
        "alice",
        "password",
        "private",
        "data.parquet",
        "credential-secret",
        "signed-secret",
        "fragment-secret",
        "backend-secret-from-exception",
    ):
        assert secret not in observed
