"""Parity and conservative-selection tests for the native Parquet codec."""

from __future__ import annotations

import io
from contextlib import ExitStack
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
from polars.testing import assert_frame_equal
import pyarrow.parquet as pq
import pytest

from supertable import processing
from supertable.processing import (
    _PARQUET_ROW_GROUP_SIZE,
    resource_stats_seal,
    stats_seal_for_metadata,
    write_parquet_and_collect_resources,
)
from supertable.utils.profiler import Profiler


def _write_bytes_frame(frame: pl.DataFrame, profiler: Profiler | None = None):
    storage = MagicMock()
    uploaded = {}
    storage.write_bytes.side_effect = (
        lambda path, data: uploaded.update({path: data})
    )
    resources = []
    footer_cache = {}
    with (
        patch.object(processing, "_get_storage", return_value=storage),
        patch.object(processing, "generate_filename", return_value="data.parquet"),
    ):
        write_parquet_and_collect_resources(
            frame,
            [],
            "/table/data",
            resources,
            compression_level=1,
            profiler=profiler,
            footer_md_out=footer_cache,
        )
    resource = resources[0]
    return uploaded[resource["file"]], resource, footer_cache, storage


def _representative_frame() -> pl.DataFrame:
    return pl.DataFrame({
        "i8": pl.Series([-2, -1, None, 2], dtype=pl.Int8),
        "u64": pl.Series([0, 1, None, 2], dtype=pl.UInt64),
        "f32": pl.Series([-1.25, 0.0, None, 2.5], dtype=pl.Float32),
        "f64": pl.Series([-1.25, 0.0, None, 2.5], dtype=pl.Float64),
        "bool": pl.Series([False, True, None, False], dtype=pl.Boolean),
        "string": pl.Series(["", "é", None, "xyz"], dtype=pl.String),
        "binary": pl.Series(
            [b"", b"\x80", None, b"x" * 257], dtype=pl.Binary,
        ),
        "date": pl.Series(
            [date(2020, 1, 1), date(2021, 2, 3), None, date(2022, 4, 5)],
            dtype=pl.Date,
        ),
        "time": pl.Series(
            [time(1, 2, 3), time(2, 3, 4), None, time(4, 5, 6)],
            dtype=pl.Time,
        ),
        "datetime": pl.Series(
            [datetime(2020, 1, 1), datetime(2021, 2, 3), None,
             datetime(2022, 4, 5)],
            dtype=pl.Datetime("us"),
        ),
        "datetime_utc": pl.Series(
            [datetime(2020, 1, 1, tzinfo=timezone.utc),
             datetime(2021, 2, 3, tzinfo=timezone.utc), None,
             datetime(2022, 4, 5, tzinfo=timezone.utc)],
            dtype=pl.Datetime("us", "UTC"),
        ),
        "duration": pl.Series(
            [timedelta(seconds=1), timedelta(seconds=2), None,
             timedelta(seconds=3)],
            dtype=pl.Duration("us"),
        ),
        "decimal": pl.Series(
            [Decimal("-1.20"), Decimal("0"), None, Decimal("3.45")],
            dtype=pl.Decimal(10, 2),
        ),
        "null": pl.Series([None] * 4, dtype=pl.Null),
    })


def test_native_codec_preserves_values_schema_binary_width_and_resource_seal():
    frame = _representative_frame()
    profiler = Profiler()

    payload, resource, footer_cache, storage = _write_bytes_frame(
        frame, profiler,
    )

    assert_frame_equal(pl.read_parquet(io.BytesIO(payload)), frame)
    metadata = pq.read_metadata(io.BytesIO(payload))
    assert metadata.schema.to_arrow_schema().equals(frame.to_arrow().schema)
    expected_seal = stats_seal_for_metadata(resource["file"], metadata)
    assert resource_stats_seal(resource) == expected_seal
    assert footer_cache[resource["file"]].metadata.num_rows == frame.height
    assert resource["column_max_value_bytes"] == {"binary": 257}
    assert resource["file_size"] == len(payload)
    assert profiler.emit_counts().get("parquet_codec_polars") == 1
    assert profiler.emit_counts().get("parquet_codec_pyarrow", 0) == 0
    storage.size.assert_not_called()
    storage.delete.assert_not_called()


def test_native_codec_preserves_configured_row_group_size_and_statistics():
    rows = _PARQUET_ROW_GROUP_SIZE * 2 + 7
    frame = pl.DataFrame({
        "id": pl.int_range(0, rows, eager=True),
        "value": pl.int_range(0, rows, eager=True) * 2,
    })

    payload, resource, _, _ = _write_bytes_frame(frame)
    metadata = pq.read_metadata(io.BytesIO(payload))

    assert metadata.num_row_groups == 3
    assert [
        metadata.row_group(index).num_rows
        for index in range(metadata.num_row_groups)
    ] == [_PARQUET_ROW_GROUP_SIZE, _PARQUET_ROW_GROUP_SIZE, 7]
    for group_index in range(metadata.num_row_groups):
        group = metadata.row_group(group_index)
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            assert column.is_stats_set
            assert column.statistics is not None
            assert column.statistics.has_min_max
    assert resource["stats_rows"] == metadata.num_row_groups * 2


@pytest.mark.parametrize(
    ("frame", "reason"),
    [
        (pl.DataFrame({"x": pl.Series([1.0, float("nan")], dtype=pl.Float32)}),
         "nan"),
        (pl.DataFrame({"x": pl.Series([1.0, float("nan")], dtype=pl.Float64)}),
         "nan"),
        (pl.DataFrame({"x": ["x" * 65]}), "long_string"),
        # 33 characters but 66 UTF-8 bytes proves this is a byte-width gate.
        (pl.DataFrame({"x": ["é" * 33]}), "long_string"),
        (pl.DataFrame({
            "x": pl.Series(["a", "b"], dtype=pl.Categorical),
        }), "categorical"),
        (pl.DataFrame({
            "x": pl.Series(["a", "b"], dtype=pl.Enum(["a", "b"])),
        }), "enum"),
        (pl.DataFrame({
            "x": pl.Series([["short"], ["x" * 65]], dtype=pl.List(pl.String)),
        }), "nested"),
        (pl.DataFrame({
            "x": pl.Series([[1.0, float("nan")]], dtype=pl.Array(pl.Float64, 2)),
        }), "nested"),
        (pl.DataFrame({
            "x": pl.Series(
                [{"value": "short"}, {"value": "x" * 65}],
                dtype=pl.Struct({"value": pl.String}),
            ),
        }), "nested"),
    ],
)
def test_stats_sensitive_frames_fall_back_to_pyarrow(frame, reason):
    profiler = Profiler()
    with patch.object(
        processing,
        "_encode_parquet_polars",
        wraps=processing._encode_parquet_polars,
    ) as native_encode:
        payload, _, _, _ = _write_bytes_frame(frame, profiler)

    native_encode.assert_not_called()
    assert_frame_equal(pl.read_parquet(io.BytesIO(payload)), frame)
    counts = profiler.emit_counts()
    assert counts.get("parquet_codec_pyarrow") == 1
    assert counts.get(f"parquet_codec_pyarrow_{reason}") == 1
    assert counts.get("parquet_codec_polars", 0) == 0


def test_exactly_64_utf8_bytes_remains_native_eligible():
    profiler = Profiler()
    frame = pl.DataFrame({"x": ["é" * 32]})

    _write_bytes_frame(frame, profiler)

    assert profiler.emit_counts().get("parquet_codec_polars") == 1


def test_system_codec_uses_native_polars_for_long_object_keys_without_stats_scan():
    """Stats/DV paths are payload, not footer-pruning metadata."""
    profiler = Profiler()
    frame = pl.DataFrame({
        "__file__": ["org/super/tables/table/data/" + ("x" * 256)],
        "__rowid__": pl.Series([1], dtype=pl.Int64),
    })
    uploaded = {}
    storage = MagicMock()
    storage.write_bytes.side_effect = (
        lambda path, data: uploaded.update({path: data})
    )

    with (
        patch.object(processing, "_get_storage", return_value=storage),
        patch.object(
            processing,
            "_native_polars_parquet_eligibility",
            side_effect=AssertionError("system file used data footer gate"),
        ),
    ):
        size = processing._write_df_parquet(
            frame,
            "/table/tombstone/deleted.parquet",
            compression_level=1,
            profiler=profiler,
        )

    payload = uploaded["/table/tombstone/deleted.parquet"]
    assert size == len(payload)
    assert_frame_equal(pl.read_parquet(io.BytesIO(payload)), frame)
    metadata = pq.read_metadata(io.BytesIO(payload))
    assert all(
        not metadata.row_group(group_index).column(column_index).is_stats_set
        for group_index in range(metadata.num_row_groups)
        for column_index in range(metadata.row_group(group_index).num_columns)
    )
    counts = profiler.emit_counts()
    assert counts.get("parquet_codec_polars") == 1
    assert counts.get("parquet_codec_pyarrow", 0) == 0
    assert counts.get("write.parquet_codec_check.n", 0) == 0


def test_system_native_encode_error_falls_back_before_upload():
    profiler = Profiler()
    frame = pl.DataFrame({
        "file_path": ["x" * 128],
        "row_group_id": pl.Series([0], dtype=pl.Int64),
    })
    uploaded = {}
    storage = MagicMock()
    storage.write_bytes.side_effect = (
        lambda path, data: uploaded.update({path: data})
    )

    with (
        patch.object(processing, "_get_storage", return_value=storage),
        patch.object(
            processing,
            "_encode_system_parquet_polars",
            side_effect=RuntimeError("unsupported native system dtype"),
        ),
    ):
        processing._write_df_parquet(
            frame,
            "/table/stats/stats.parquet",
            compression_level=1,
            profiler=profiler,
        )

    assert_frame_equal(
        pl.read_parquet(io.BytesIO(uploaded["/table/stats/stats.parquet"])),
        frame,
    )
    counts = profiler.emit_counts()
    assert counts.get("parquet_codec_polars_encode_error") == 1
    assert counts.get("parquet_codec_pyarrow") == 1
    assert counts.get("parquet_codec_pyarrow_encode_error") == 1
    assert counts.get("parquet_codec_polars", 0) == 0


def test_system_parquet_only_backend_does_not_build_throwaway_encoded_bytes():
    class ParquetOnlyStorage:
        def __init__(self):
            self.table = None

        def write_parquet(self, table, _path):
            self.table = table

        def size(self, _path):
            return 123

    storage = ParquetOnlyStorage()
    profiler = Profiler()
    frame = pl.DataFrame({"__file__": ["x" * 128], "__rowid__": [1]})
    with (
        patch.object(processing, "_get_storage", return_value=storage),
        patch.object(
            processing,
            "_encode_system_parquet_pyarrow",
            side_effect=AssertionError("system frame was encoded twice"),
        ),
    ):
        size = processing._write_df_parquet(
            frame,
            "/table/tombstone/deleted.parquet",
            compression_level=1,
            profiler=profiler,
        )

    assert size == 123
    assert storage.table is not None
    assert_frame_equal(pl.from_arrow(storage.table), frame)
    counts = profiler.emit_counts()
    assert counts.get("parquet_codec_pyarrow") == 1
    assert counts.get("parquet_codec_pyarrow_backend") == 1


def test_native_encode_error_falls_back_before_upload():
    profiler = Profiler()
    frame = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    with patch.object(
        processing,
        "_encode_parquet_polars",
        side_effect=RuntimeError("unsupported native dtype"),
    ):
        payload, _, _, _ = _write_bytes_frame(frame, profiler)

    assert_frame_equal(pl.read_parquet(io.BytesIO(payload)), frame)
    counts = profiler.emit_counts()
    assert counts.get("parquet_codec_polars_encode_error") == 1
    assert counts.get("parquet_codec_pyarrow") == 1
    assert counts.get("parquet_codec_pyarrow_encode_error") == 1
    assert counts.get("parquet_codec_polars", 0) == 0


def test_parquet_only_backend_retains_arrow_codec():
    class ParquetOnlyStorage:
        def __init__(self):
            self.table = None

        def makedirs(self, _path):
            pass

        def write_parquet(self, table, _path):
            self.table = table

        def size(self, _path):
            return 123

    storage = ParquetOnlyStorage()
    profiler = Profiler()
    resources = []
    frame = pl.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    with (
        patch.object(processing, "_get_storage", return_value=storage),
        patch.object(processing, "generate_filename", return_value="data.parquet"),
        patch.object(
            processing,
            "_encode_parquet_polars",
            wraps=processing._encode_parquet_polars,
        ) as native_encode,
    ):
        write_parquet_and_collect_resources(
            frame,
            [],
            "/table/data",
            resources,
            compression_level=1,
            profiler=profiler,
        )

    native_encode.assert_not_called()
    assert storage.table is not None
    assert_frame_equal(pl.from_arrow(storage.table), frame)
    counts = profiler.emit_counts()
    assert counts.get("parquet_codec_pyarrow") == 1
    assert counts.get("parquet_codec_pyarrow_backend") == 1


_FIXED_OUTPUT = "/table/data/data-fixed-uuid.parquet"


class _TrackingBytesStorage:
    def __init__(self, *, write_error=None, delete_error=None):
        self.write_error = write_error
        self.delete_error = delete_error
        self.byte_writes = []
        self.parquet_writes = []
        self.deleted = []
        self.objects = {}

    def makedirs(self, _path):
        pass

    def write_bytes(self, path, data):
        # Persist first to model a remote PUT whose success acknowledgement is
        # lost after the immutable object became visible.
        self.byte_writes.append(path)
        self.objects[path] = data
        if self.write_error is not None:
            raise self.write_error

    def write_parquet(self, _table, path):
        self.parquet_writes.append(path)
        raise AssertionError("write_bytes failure fell back to write_parquet")

    def delete(self, path):
        self.deleted.append(path)
        self.objects.pop(path, None)
        if self.delete_error is not None:
            raise self.delete_error


class _TrackingParquetStorage:
    def __init__(self, *, write_error=None, size_error=None):
        self.write_error = write_error
        self.size_error = size_error
        self.parquet_writes = []
        self.deleted = []
        self.objects = {}

    def makedirs(self, _path):
        pass

    def write_parquet(self, table, path):
        self.parquet_writes.append(path)
        self.objects[path] = table
        if self.write_error is not None:
            raise self.write_error

    def size(self, _path):
        if self.size_error is not None:
            raise self.size_error
        return 123

    def delete(self, path):
        self.deleted.append(path)
        self.objects.pop(path, None)


def _attempt_fixed_write(storage, frame=None):
    resources = []
    footer_cache = {}
    get_storage = patch.object(
        processing, "_get_storage", return_value=storage,
    )
    fixed_name = patch.object(
        processing, "generate_filename", return_value="data-fixed-uuid.parquet",
    )
    return resources, footer_cache, get_storage, fixed_name, (
        frame if frame is not None else pl.DataFrame({"id": [1, 2]})
    )


def _exception_chain(error):
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        yield error
        error = error.__cause__ or error.__context__


def test_ambiguous_byte_put_failure_deletes_exact_path_and_preserves_error():
    upload_error = OSError("lost PUT acknowledgement")
    storage = _TrackingBytesStorage(
        write_error=upload_error,
        delete_error=RuntimeError("cleanup transport failed"),
    )
    resources, footer_cache, get_storage, fixed_name, frame = (
        _attempt_fixed_write(storage)
    )

    with get_storage as storage_lookup, fixed_name:
        with pytest.raises(OSError) as caught:
            write_parquet_and_collect_resources(
                frame, [], "/table/data", resources,
                compression_level=1, footer_md_out=footer_cache,
            )

    assert caught.value is upload_error
    assert storage.byte_writes == [_FIXED_OUTPUT]
    assert storage.parquet_writes == []
    assert storage.deleted == [_FIXED_OUTPUT]
    assert resources == []
    assert footer_cache == {}
    storage_lookup.assert_called_once_with()


@pytest.mark.parametrize(
    "failure_stage", ["object_seal", "footer", "binary", "stats"],
)
def test_byte_upload_post_put_failures_delete_exact_unpublished_path(
    failure_stage,
):
    injected = RuntimeError(f"injected {failure_stage} failure")
    storage = _TrackingBytesStorage()
    frame = (
        pl.DataFrame({"blob": pl.Series([b"value"], dtype=pl.Binary)})
        if failure_stage == "binary"
        else pl.DataFrame({"id": [1, 2]})
    )
    resources, footer_cache, get_storage, fixed_name, frame = (
        _attempt_fixed_write(storage, frame)
    )

    with ExitStack() as stack:
        storage_lookup = stack.enter_context(get_storage)
        stack.enter_context(fixed_name)
        if failure_stage == "object_seal":
            stack.enter_context(patch.object(
                processing,
                "_uploaded_resource_object_seal",
                side_effect=injected,
            ))
        elif failure_stage == "footer":
            stack.enter_context(patch.object(
                processing.pq, "read_metadata", side_effect=injected,
            ))
        elif failure_stage == "binary":
            stack.enter_context(patch.object(
                processing,
                "_native_polars_parquet_eligibility",
                return_value=(False, "test"),
            ))
            stack.enter_context(patch.object(
                processing.pc, "binary_length", side_effect=injected,
            ))
        elif failure_stage == "stats":
            stack.enter_context(patch.object(
                processing, "_stats_rows_for_metadata", side_effect=injected,
            ))

        with pytest.raises(RuntimeError) as caught:
            write_parquet_and_collect_resources(
                frame, [], "/table/data", resources,
                compression_level=1, footer_md_out=footer_cache,
            )

    assert any(error is injected for error in _exception_chain(caught.value))
    assert storage.byte_writes == [_FIXED_OUTPUT]
    assert storage.parquet_writes == []
    assert storage.deleted == [_FIXED_OUTPUT]
    assert storage.objects == {}
    assert resources == []
    assert footer_cache == {}
    storage_lookup.assert_called_once_with()


@pytest.mark.parametrize("failure_stage", ["upload", "size"])
def test_parquet_backend_failure_deletes_exact_path_without_fallback(
    failure_stage,
):
    injected = OSError(f"injected parquet {failure_stage} failure")
    storage = _TrackingParquetStorage(
        write_error=injected if failure_stage == "upload" else None,
        size_error=injected if failure_stage == "size" else None,
    )
    resources, footer_cache, get_storage, fixed_name, frame = (
        _attempt_fixed_write(storage)
    )

    with get_storage as storage_lookup, fixed_name:
        with pytest.raises(OSError) as caught:
            write_parquet_and_collect_resources(
                frame, [], "/table/data", resources,
                compression_level=1, footer_md_out=footer_cache,
            )

    assert caught.value is injected
    assert storage.parquet_writes == [_FIXED_OUTPUT]
    assert storage.deleted == [_FIXED_OUTPUT]
    assert storage.objects == {}
    assert resources == []
    assert footer_cache == {}
    storage_lookup.assert_called_once_with()
