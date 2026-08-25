# supertable/simple_table.py

from __future__ import annotations

import os
import copy
import io
import struct
import time
import uuid
from datetime import datetime

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from supertable.config.defaults import logger
from supertable.errors import TableNotFoundError
from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import ObjectMetadata
from supertable.super_table import SuperTable
from supertable.utils.helper import collect_schema, generate_filename
from supertable.utils.snapshot import (
    complete_snapshot_payload,
    snapshot_cache_payload,
)
from supertable.utils.profiler import Profiler, get_null_profiler
import json
from typing import Any, Callable, Dict, List, Optional, Sequence


_MAX_RESTORE_PARQUET_FOOTER_BYTES = 64 * 1024 * 1024
_MAX_RESTORE_ROW_GROUPS = 100_000
_MAX_RESTORE_COLUMNS = 4096
_MAX_RESTORE_SCHEMA_BYTES = 1024 * 1024
_MAX_RESTORE_TOMBSTONE_BYTES = 64 * 1024 * 1024
_MAX_RESTORE_TOMBSTONE_DECODED_BYTES = 256 * 1024 * 1024
_MAX_RESTORE_TOMBSTONE_ROWS = 1_000_000
_MAX_RESTORE_COLUMN_CHUNKS = 100_000
_MAX_RESTORE_AGGREGATE_COLUMN_CHUNKS = 100_000
_MAX_RESTORE_AGGREGATE_FOOTER_BYTES = 256 * 1024 * 1024
_MAX_RESTORE_SNAPSHOT_BYTES = 8 * 1024 * 1024
_RESTORE_JOURNAL_PREFIX = "supertable_restore_pending_"
_RESTORE_JOURNAL_PATTERN = f"{_RESTORE_JOURNAL_PREFIX}*.json"
_MAX_RESTORE_JOURNALS = 10_000
_MAX_RESTORE_JOURNAL_BYTES = 64 * 1024


def _read_sealed_json_object(
    storage: object,
    path: str,
    *,
    max_bytes: int,
    label: str,
) -> tuple[object, ObjectMetadata]:
    """Conditionally read one bounded JSON object under an immutable seal."""
    stat_object = getattr(storage, "stat_object", None)
    read_range = getattr(storage, "read_range", None)
    if not callable(stat_object) or not callable(read_range):
        raise RuntimeError(f"{label} storage lacks bounded immutable reads")
    observed = stat_object(path)
    if (
        not isinstance(observed, ObjectMetadata)
        or type(observed.size) is not int
        or not 1 <= observed.size <= max_bytes
        or not observed.identity_token()
    ):
        raise RuntimeError(f"{label} has an invalid size or identity")
    encoded = read_range(path, 0, observed.size, expected=observed)
    if (
        not isinstance(encoded, (bytes, bytearray, memoryview))
        or len(encoded) != observed.size
    ):
        raise RuntimeError(f"{label} bounded read was incomplete")
    try:
        payload = json.loads(bytes(encoded))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise RuntimeError(f"{label} is not valid JSON") from None
    resealed = stat_object(path)
    if not isinstance(resealed, ObjectMetadata) or resealed != observed:
        raise RuntimeError(f"{label} changed during validation")
    return payload, observed


def _sealed_parquet_metadata(
    storage: object,
    path: str,
    *,
    expected_size: Optional[int],
) -> tuple[ObjectMetadata, Any, int]:
    """Read and parse only an immutable Parquet footer under an object seal."""
    stat_object = getattr(storage, "stat_object", None)
    read_range = getattr(storage, "read_range", None)
    if not callable(stat_object) or not callable(read_range):
        raise RuntimeError("Restore storage lacks bounded immutable reads")
    observed = stat_object(path)
    if not isinstance(observed, ObjectMetadata):
        raise RuntimeError("Restore storage returned invalid object metadata")
    if (
        type(observed.size) is not int
        or observed.size < 12
        or (expected_size is not None and observed.size != expected_size)
        or not observed.identity_token()
    ):
        raise RuntimeError("Restored artifact size or identity is invalid")
    tail = read_range(
        path, observed.size - 8, 8, expected=observed,
    )
    if (
        not isinstance(tail, (bytes, bytearray, memoryview))
        or len(tail) != 8
        or bytes(tail)[4:] != b"PAR1"
    ):
        raise RuntimeError("Restored data artifact is not valid Parquet")
    footer_size = struct.unpack("<I", bytes(tail)[:4])[0]
    if (
        footer_size <= 0
        or footer_size > _MAX_RESTORE_PARQUET_FOOTER_BYTES
        or footer_size > observed.size - 12
    ):
        raise RuntimeError("Restored Parquet footer exceeds its safety limit")
    footer = read_range(
        path,
        observed.size - footer_size - 8,
        footer_size + 8,
        expected=observed,
    )
    if (
        not isinstance(footer, (bytes, bytearray, memoryview))
        or len(footer) != footer_size + 8
    ):
        raise RuntimeError("Restored Parquet footer read was incomplete")
    try:
        metadata = pq.read_metadata(pa.BufferReader(b"PAR1" + bytes(footer)))
    except Exception:
        raise RuntimeError(
            "Restored data artifact has invalid Parquet metadata"
        ) from None
    if (
        metadata.num_row_groups > _MAX_RESTORE_ROW_GROUPS
        or metadata.num_columns > _MAX_RESTORE_COLUMNS
        or metadata.num_row_groups * metadata.num_columns
        > _MAX_RESTORE_COLUMN_CHUNKS
    ):
        raise ValueError("Restored Parquet metadata exceeds its safety limit")
    try:
        schema_bytes = int(metadata.schema.to_arrow_schema().serialize().size)
    except Exception:
        raise RuntimeError("Restored Parquet schema is invalid") from None
    if schema_bytes > _MAX_RESTORE_SCHEMA_BYTES:
        raise ValueError("Restored Parquet schema exceeds its safety limit")
    resealed = stat_object(path)
    if not isinstance(resealed, ObjectMetadata) or resealed != observed:
        raise RuntimeError("Restored data artifact changed during validation")
    return observed, metadata, footer_size + 8


def _object_seal_document(metadata: ObjectMetadata) -> Dict[str, Any]:
    return {
        "size": metadata.size,
        "version": metadata.version,
        "etag": metadata.etag,
        "last_modified_ns": metadata.last_modified_ns,
        "checksum_sha256": metadata.checksum_sha256,
    }


def _validate_declared_object_seal(
    declared: object,
    observed: ObjectMetadata,
) -> None:
    if declared is None:
        return
    if not isinstance(declared, dict):
        raise ValueError("Restored resource object seal is invalid")
    canonical = _object_seal_document(observed)
    for field, actual in canonical.items():
        value = declared.get(field, "" if isinstance(actual, str) else 0)
        if value != actual:
            raise RuntimeError("Restored resource object seal has changed")


def _restored_schema_field_names(schema: object) -> list[str]:
    """Return an ordered, strict, case-unambiguous schema projection."""
    if isinstance(schema, dict):
        names = list(schema)
    elif isinstance(schema, list):
        names = []
        for item in schema:
            if isinstance(item, dict) and isinstance(item.get("name"), str):
                names.append(item["name"])
            elif isinstance(item, dict) and len(item) == 1:
                names.append(next(iter(item)))
            elif (
                isinstance(item, (list, tuple))
                and len(item) == 2
                and isinstance(item[0], str)
            ):
                names.append(item[0])
            else:
                raise ValueError("Restored snapshot schema is invalid")
    else:
        raise ValueError("Restored snapshot schema is invalid")
    folded: set[str] = set()
    result: list[str] = []
    for name in names:
        try:
            encoded_name = name.encode("utf-8") if isinstance(name, str) else b""
        except UnicodeEncodeError:
            raise ValueError("Restored snapshot schema is invalid") from None
        if (
            not isinstance(name, str)
            or not name
            or "\x00" in name
            or len(encoded_name) > 1024
            or name.casefold() in folded
            or name.casefold() in {
                "__rowid__", "__timestamp__", "__file__",
                "__supertable_source_file__",
                "__supertable_scan_filename__",
            }
            or name.casefold().startswith("__supertable_")
        ):
            raise ValueError("Restored snapshot schema is invalid")
        folded.add(name.casefold())
        result.append(name)
    return result


def _restored_schema_type_values(
    schema: object,
    names: list[str],
) -> dict[str, str]:
    """Extract and bound caller types for a zero-resource logical schema."""
    values: list[object]
    if isinstance(schema, dict):
        values = list(schema.values())
    elif isinstance(schema, list):
        values = []
        for item in schema:
            if isinstance(item, dict) and isinstance(item.get("name"), str):
                values.append(item.get("type"))
            elif isinstance(item, dict) and len(item) == 1:
                values.append(next(iter(item.values())))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                values.append(item[1])
            else:  # pragma: no cover - names parser rejects the same shape
                raise ValueError("Restored snapshot schema is invalid")
    else:  # pragma: no cover - names parser rejects the same shape
        raise ValueError("Restored snapshot schema is invalid")
    if len(values) != len(names):
        raise ValueError("Restored snapshot schema is invalid")

    result: dict[str, str] = {}
    for name, raw_type in zip(names, values):
        if not isinstance(raw_type, str) or not raw_type.strip():
            raise ValueError("Restored snapshot schema type is invalid")
        normalized = raw_type.strip()
        try:
            encoded_type = normalized.encode("utf-8")
        except UnicodeEncodeError:
            raise ValueError("Restored snapshot schema type is invalid") from None
        if len(encoded_type) > 4096:
            raise ValueError("Restored snapshot schema type is invalid")
        result[name] = normalized
    return result


def _polars_dtype_for_arrow_field(field: pa.Field) -> Any:
    """Convert one physical Arrow field through Polars' writer type bridge."""
    try:
        import polars as pl

        empty = pa.Table.from_arrays(
            [pa.array([], type=field.type)],
            names=[field.name],
        )
        converted = pl.from_arrow(empty)
        return converted.schema[field.name]
    except Exception:
        raise ValueError(
            "Restored column has an unsupported physical type"
        ) from None


_RESTORE_TEMPORAL_UNIT_RANK = {"s": 0, "ms": 1, "us": 2, "ns": 3}


def _restore_integer_spec(dtype: pa.DataType) -> Optional[tuple[bool, int]]:
    for predicate, signed, bits in (
        (pa.types.is_int8, True, 8),
        (pa.types.is_int16, True, 16),
        (pa.types.is_int32, True, 32),
        (pa.types.is_int64, True, 64),
        (pa.types.is_uint8, False, 8),
        (pa.types.is_uint16, False, 16),
        (pa.types.is_uint32, False, 32),
        (pa.types.is_uint64, False, 64),
    ):
        if predicate(dtype):
            return signed, bits
    return None


def _restore_integer_dtype(signed: bool, bits: int) -> pa.DataType:
    constructors = (
        (8, pa.int8 if signed else pa.uint8),
        (16, pa.int16 if signed else pa.uint16),
        (32, pa.int32 if signed else pa.uint32),
        (64, pa.int64 if signed else pa.uint64),
    )
    for available_bits, constructor in constructors:
        if bits <= available_bits:
            return constructor()
    raise ValueError("Restored integer types have no lossless common type")


def _restore_integer_decimal_digits(signed: bool, bits: int) -> int:
    return {
        (True, 8): 3,
        (True, 16): 5,
        (True, 32): 10,
        (True, 64): 19,
        (False, 8): 3,
        (False, 16): 5,
        (False, 32): 10,
        (False, 64): 20,
    }[(signed, bits)]


def _restore_decimal_dtype(precision: int, scale: int) -> pa.DataType:
    try:
        if precision <= 38:
            return pa.decimal128(precision, scale)
        if precision <= 76:
            return pa.decimal256(precision, scale)
    except (TypeError, ValueError):
        raise ValueError(
            "Restored decimal types have no lossless common type"
        ) from None
    raise ValueError("Restored decimal precision exceeds the lossless limit")


def _merge_restore_decimals(
    left_precision: int,
    left_scale: int,
    right_precision: int,
    right_scale: int,
) -> pa.DataType:
    scale = max(left_scale, right_scale)
    integer_digits = max(
        left_precision - left_scale,
        right_precision - right_scale,
    )
    return _restore_decimal_dtype(integer_digits + scale, scale)


def _lossless_restore_physical_type(
    left: pa.DataType,
    right: pa.DataType,
) -> pa.DataType:
    """Return the least physical type that represents both domains exactly.

    Restore metadata becomes live read authority, so convenience coercions
    (notably Int64 -> Float64 or arbitrary values -> Utf8) are forbidden. The
    lattice is deliberately explicit; unrelated/nested types are accepted only
    when Arrow reports them exactly equal.
    """
    if left.equals(right):
        return left
    if pa.types.is_null(left):
        return right
    if pa.types.is_null(right):
        return left

    left_integer = _restore_integer_spec(left)
    right_integer = _restore_integer_spec(right)
    if left_integer is not None and right_integer is not None:
        left_signed, left_bits = left_integer
        right_signed, right_bits = right_integer
        if left_signed == right_signed:
            return _restore_integer_dtype(
                left_signed, max(left_bits, right_bits),
            )
        signed_bits = left_bits if left_signed else right_bits
        unsigned_bits = right_bits if left_signed else left_bits
        required_signed_bits = max(signed_bits, unsigned_bits + 1)
        if required_signed_bits <= 64:
            return _restore_integer_dtype(True, required_signed_bits)
        # Int64 and UInt64 have no common fixed-width integer, but their full
        # domains fit exactly in a scale-zero Decimal128(20, 0).
        return _restore_decimal_dtype(20, 0)

    left_decimal = pa.types.is_decimal(left)
    right_decimal = pa.types.is_decimal(right)
    if left_decimal or right_decimal:
        if pa.types.is_floating(left) or pa.types.is_floating(right):
            raise ValueError(
                "Restored decimal and floating types are incompatible"
            )

        def decimal_spec(
            dtype: pa.DataType,
            integer: Optional[tuple[bool, int]],
        ) -> tuple[int, int]:
            if pa.types.is_decimal(dtype):
                return int(dtype.precision), int(dtype.scale)
            if integer is not None:
                return _restore_integer_decimal_digits(*integer), 0
            raise ValueError(
                "Restored decimal and non-numeric types are incompatible"
            )

        left_precision, left_scale = decimal_spec(left, left_integer)
        right_precision, right_scale = decimal_spec(right, right_integer)
        return _merge_restore_decimals(
            left_precision,
            left_scale,
            right_precision,
            right_scale,
        )

    if (
        pa.types.is_floating(left)
        or pa.types.is_floating(right)
    ):
        float_width = 0
        integer_significant_bits = 0
        for dtype, integer in (
            (left, left_integer),
            (right, right_integer),
        ):
            if pa.types.is_float16(dtype):
                float_width = max(float_width, 16)
            elif pa.types.is_float32(dtype):
                float_width = max(float_width, 32)
            elif pa.types.is_float64(dtype):
                float_width = 64
            elif integer is not None:
                signed, bits = integer
                integer_significant_bits = max(
                    integer_significant_bits,
                    bits - 1 if signed else bits,
                )
            else:
                raise ValueError(
                    "Restored floating and non-numeric types are incompatible"
                )
        # Widening preserves every value of the narrower IEEE lane. Integer
        # domains fit only when the destination mantissa represents every bit.
        if float_width <= 32 and integer_significant_bits <= 24:
            return pa.float32()
        if integer_significant_bits <= 53:
            return pa.float64()
        raise ValueError(
            "Restored integer and floating types have no lossless common type"
        )

    if pa.types.is_boolean(left) or pa.types.is_boolean(right):
        raise ValueError("Restored boolean types are incompatible")

    if (
        pa.types.is_string(left) or pa.types.is_large_string(left)
    ) and (
        pa.types.is_string(right) or pa.types.is_large_string(right)
    ):
        return (
            pa.large_string()
            if pa.types.is_large_string(left) or pa.types.is_large_string(right)
            else pa.string()
        )

    def binary_type(dtype: pa.DataType) -> bool:
        return bool(
            pa.types.is_binary(dtype)
            or pa.types.is_large_binary(dtype)
            or pa.types.is_fixed_size_binary(dtype)
        )

    if binary_type(left) and binary_type(right):
        if pa.types.is_large_binary(left) or pa.types.is_large_binary(right):
            return pa.large_binary()
        return pa.binary()

    if (pa.types.is_date(left) and pa.types.is_date(right)):
        # Parquet DATE is day-granular; canonicalize both Arrow date spellings
        # to the writer's logical Date lane rather than silently making date64
        # a midnight Datetime in Polars.
        return pa.date32()

    if pa.types.is_timestamp(left) and pa.types.is_timestamp(right):
        if left.tz != right.tz:
            raise ValueError(
                "Restored datetime timezones are incompatible"
            )
        unit = max(
            (left.unit, right.unit),
            key=_RESTORE_TEMPORAL_UNIT_RANK.__getitem__,
        )
        return pa.timestamp(unit, tz=left.tz)

    if pa.types.is_duration(left) and pa.types.is_duration(right):
        unit = max(
            (left.unit, right.unit),
            key=_RESTORE_TEMPORAL_UNIT_RANK.__getitem__,
        )
        return pa.duration(unit)

    if pa.types.is_time(left) and pa.types.is_time(right):
        unit = max(
            (left.unit, right.unit),
            key=_RESTORE_TEMPORAL_UNIT_RANK.__getitem__,
        )
        return pa.time32(unit) if unit in {"s", "ms"} else pa.time64(unit)

    raise ValueError(
        "Restored physical column types are not losslessly compatible"
    )


def _bounded_restored_tombstone_frame(
    storage: object,
    path: str,
    *,
    observed: ObjectMetadata,
    expected_rows: int,
    expected_digest: Optional[str],
    tombstone_format: int,
    allowed_files: set[str],
) -> object:
    """Conditionally decode a bounded v1/v3 deletion vector in batches.

    Dictionary/RLE Parquet sizes are not decoded-memory bounds.  Keep the
    string column dictionary encoded until every distinct value is proven to
    be one of this snapshot's already-bounded resource keys, then materialize
    only a frame whose worst-case logical size fits the restore budget.
    """
    read_range = getattr(storage, "read_range", None)
    stat_object = getattr(storage, "stat_object", None)
    if not callable(read_range) or not callable(stat_object):
        raise RuntimeError(
            "Restore storage lacks bounded immutable tombstone reads"
        )
    if (
        type(expected_rows) is not int
        or expected_rows < 0
        or expected_rows > _MAX_RESTORE_TOMBSTONE_ROWS
    ):
        raise ValueError("Restored deletion-vector row count is unsafe")
    longest_file = max(
        (len(file_name.encode("utf-8")) for file_name in allowed_files),
        default=0,
    )
    if expected_rows * (longest_file + 16) > (
        _MAX_RESTORE_TOMBSTONE_DECODED_BYTES
    ):
        raise ValueError("Restored deletion vector exceeds its decoded-byte limit")

    raw = read_range(path, 0, observed.size, expected=observed)
    if (
        not isinstance(raw, (bytes, bytearray, memoryview))
        or len(raw) != observed.size
    ):
        raise RuntimeError("Restored deletion-vector read was incomplete")
    exact_bytes = bytes(raw)

    from supertable.processing import (
        TOMBSTONE_FILE_COL,
        TOMBSTONE_SCHEMA,
        validate_tombstone_frame,
    )
    from supertable.tombstone_manifest_v2 import (
        TOMBSTONE_FORMAT_V3,
        tombstone_v3_artifact_digest,
    )
    try:
        parquet_file = pq.ParquetFile(
            pa.BufferReader(exact_bytes),
            read_dictionary=[TOMBSTONE_FILE_COL],
        )
        physical_schema = parquet_file.schema_arrow
        file_type = physical_schema.field(0).type
        if pa.types.is_dictionary(file_type):
            file_type = file_type.value_type
        if (
            physical_schema.names != list(TOMBSTONE_SCHEMA)
            or not (
                pa.types.is_string(file_type)
                or pa.types.is_large_string(file_type)
            )
            or not pa.types.is_int64(physical_schema.field(1).type)
        ):
            raise ValueError("Restored deletion vector has an invalid schema")

        batches: list[pa.RecordBatch] = []
        arrow_bytes = 0
        rows_seen = 0
        for batch in parquet_file.iter_batches(batch_size=65_536):
            rows_seen += batch.num_rows
            arrow_bytes += int(batch.nbytes)
            if (
                rows_seen > expected_rows
                or arrow_bytes > _MAX_RESTORE_TOMBSTONE_DECODED_BYTES
            ):
                raise ValueError(
                    "Restored deletion vector exceeds its decoded-byte limit"
                )
            file_values = batch.column(0)
            if file_values.null_count:
                raise ValueError("Restored deletion vector contains NULL file keys")
            distinct_files = set(pc.unique(file_values).to_pylist())
            if (
                any(not isinstance(value, str) or not value for value in distinct_files)
                or not distinct_files.issubset(allowed_files)
            ):
                raise ValueError(
                    "Restored deletion vector references a foreign resource"
                )
            batches.append(batch)
        if rows_seen != expected_rows:
            raise ValueError("Restored deletion-vector row count is inconsistent")

        import polars as pl

        if batches:
            frame = pl.from_arrow(pa.Table.from_batches(batches))
            if frame.schema.get(TOMBSTONE_FILE_COL) != pl.Utf8:
                frame = frame.with_columns(
                    pl.col(TOMBSTONE_FILE_COL).cast(pl.Utf8),
                )
        else:
            frame = pl.DataFrame(schema=TOMBSTONE_SCHEMA)
        frame = validate_tombstone_frame(
            frame,
            expected_rows=expected_rows,
            expected_digest=(
                None if tombstone_format == TOMBSTONE_FORMAT_V3
                else expected_digest
            ),
            allowed_files=allowed_files,
            source="restored deletion-vector",
        )
        if tombstone_format == TOMBSTONE_FORMAT_V3:
            if tombstone_v3_artifact_digest(exact_bytes) != expected_digest:
                raise ValueError(
                    "Restored format-3 deletion-vector digest is inconsistent"
                )
    except ValueError:
        raise
    except Exception:
        raise ValueError("Restored deletion vector is invalid") from None

    resealed = stat_object(path)
    if not isinstance(resealed, ObjectMetadata) or resealed != observed:
        raise RuntimeError("Restored deletion vector changed during validation")
    return frame


def _validate_physical_containment(
    storage: object,
    path: str,
    required_prefix: str,
) -> None:
    """Reject local symlinks that remain in the global root but leave a table."""
    is_local = getattr(storage, "is_local_storage", None)
    to_path = getattr(storage, "to_duckdb_path", None)
    if not callable(is_local) or is_local() is not True:
        return
    if not callable(to_path):
        raise RuntimeError("Local restore storage cannot resolve physical paths")
    physical_prefix = os.path.realpath(str(to_path(required_prefix)))
    physical_path = os.path.realpath(str(to_path(path)))
    try:
        contained = os.path.commonpath(
            (physical_prefix, physical_path),
        ) == physical_prefix
    except ValueError:
        contained = False
    if not contained or physical_path == physical_prefix:
        raise ValueError("Restored artifact escapes its physical table namespace")


def _contained_artifact_path(
    raw: Any, *, label: str, required_prefix: str,
) -> str:
    try:
        encoded_path = raw.encode("utf-8") if isinstance(raw, str) else b""
    except UnicodeEncodeError:
        raise ValueError(f"Restored {label} path is invalid") from None
    if (
        not isinstance(raw, str)
        or not raw
        or len(encoded_path) > 4096
        or "\x00" in raw
        or "\\" in raw
        or os.path.isabs(raw)
        or any(component in {"", ".", ".."} for component in raw.split("/"))
    ):
        raise ValueError(f"Restored {label} path is invalid")
    normalized = os.path.normpath(raw)
    if normalized != raw:
        raise ValueError(f"Restored {label} path is not canonical")
    absolute = os.path.abspath(normalized)
    prefix = os.path.abspath(os.path.normpath(required_prefix))
    if os.path.commonpath((prefix, absolute)) != prefix:
        raise ValueError(
            f"Restored {label} path escapes its immutable artifact prefix"
        )
    if absolute == prefix:
        raise ValueError(f"Restored {label} path does not name an object")
    return normalized


def _spark_type_from_polars_dtype(dtype: Any) -> str:
    """Best-effort mapping from Polars dtype to Spark/Delta type string."""
    try:
        import polars as pl
    except Exception:  # pragma: no cover
        return "string"

    if dtype in (pl.Utf8, pl.String):
        return "string"
    if dtype == pl.Boolean:
        return "boolean"

    if dtype == pl.Int8:
        return "byte"
    if dtype == pl.Int16:
        return "short"
    if dtype == pl.Int32:
        return "integer"
    if dtype == pl.Int64:
        return "long"

    if dtype == pl.UInt8:
        return "short"
    if dtype == pl.UInt16:
        return "integer"
    if dtype == pl.UInt32:
        return "long"
    if dtype == pl.UInt64:
        return "decimal(20,0)"

    if dtype == pl.Float32:
        return "float"
    if dtype == pl.Float64:
        return "double"

    if dtype == pl.Date:
        return "date"
    if dtype == pl.Datetime:
        return "timestamp"
    if dtype == pl.Binary:
        return "binary"

    # Decimal can be parametric; treat conservatively
    try:
        if isinstance(dtype, pl.Decimal):
            return f"decimal({dtype.precision},{dtype.scale})"
    except Exception:
        pass

    return "string"


def _schema_list_from_polars_df(model_df: Any) -> List[Dict[str, Any]]:
    """Build a Delta-friendly schema list from a Polars DataFrame."""
    try:
        schema = model_df.schema
    except Exception:
        return []

    out: List[Dict[str, Any]] = []
    for name, dtype in schema.items():
        out.append(
            {
                "name": name,
                "type": _spark_type_from_polars_dtype(dtype),
                "nullable": True,
                "metadata": {},
            }
        )
    return out
from supertable.rbac.access_control import check_control_access, check_write_access


class SimpleTable:
    """
    Simple-table layout on storage (heavy data) + Redis leaf pointer (meta).

    Args:
        super_table: Parent ``SuperTable`` (already constructed).
        simple_name: Name of this simple table.
        create_if_missing: When True (default), bootstrap the simple
            table (mkdirs, initial empty snapshot JSON, Redis ``meta:leaf``
            pointer) if it does not exist. When False, raise
            ``TableNotFoundError`` instead. Read-side callers
            (``MetaReader`` and friends) pass ``False`` so a missing
            table surfaces as an error instead of being silently
            materialized as a side effect of constructing the Python
            object.
        catalog: Existing catalog client to reuse. Writers pass the client that
            owns their table lease; ordinary callers omit it.
        _live_leaf_verified: Internal writer fast path. The caller must hold the
            table lease and have just verified the exact live leaf plus both
            deletion-intent fences. It is valid only with
            ``create_if_missing=False``.
        _pinned_leaf: Exact leaf document returned by the writer's atomic
            mutation-context read. It is consumed by
            :meth:`get_simple_table_snapshot` without another catalog lookup.
    """

    def __init__(
        self,
        super_table: SuperTable,
        simple_name: str,
        *,
        create_if_missing: bool = True,
        catalog: Optional[RedisCatalog] = None,
        _live_leaf_verified: bool = False,
        _pinned_leaf: Optional[Dict[str, Any]] = None,
        _pinned_snapshot: Optional[Dict[str, Any]] = None,
    ):
        # ``super == simple`` is the public aggregate relation that unions the
        # parent's children.  A physical child with that name is therefore
        # unaddressable on its own and would be silently folded into the
        # aggregate scan.  Enforce the invariant at the table-creation boundary
        # as well as in DataWriter.validation(), including case-only aliases.
        if (
            create_if_missing
            and str(simple_name).casefold()
            == str(super_table.super_name).casefold()
        ):
            raise ValueError("SimpleTable name can't match with SuperTable name")

        self.super_table = super_table
        self.identity = "tables"
        self.simple_name = simple_name

        # Storage is the same as SuperTable's
        self.storage = self.super_table.storage
        # Writers already own a table lease and have just fenced the namespace,
        # deletion intents, root and leaf through their catalog. Reuse that
        # exact client and proof instead of constructing another catalog and
        # repeating the same Redis lifecycle reads. Ordinary/read-side callers
        # retain the independent fail-closed checks below.
        self.catalog = catalog if catalog is not None else RedisCatalog()

        if _live_leaf_verified and create_if_missing:
            raise ValueError(
                "_live_leaf_verified requires create_if_missing=False"
            )
        if _pinned_leaf is not None and not _live_leaf_verified:
            raise ValueError("_pinned_leaf requires _live_leaf_verified=True")
        if _pinned_snapshot is not None and _pinned_leaf is None:
            raise ValueError("_pinned_snapshot requires _pinned_leaf")
        if _pinned_leaf is not None and (
            not isinstance(_pinned_leaf, dict)
            or not isinstance(_pinned_leaf.get("path"), str)
            or not _pinned_leaf["path"]
        ):
            raise ValueError("_pinned_leaf must be a valid leaf document")
        self._pinned_leaf = (
            dict(_pinned_leaf) if _pinned_leaf is not None else None
        )
        self._pinned_snapshot = _pinned_snapshot
        if not _live_leaf_verified:
            deletion_guard = getattr(
                type(self.catalog), "check_deletion_intent_absent", None,
            )
            if callable(deletion_guard):
                self.catalog.check_deletion_intent_absent(
                    self.super_table.organization,
                    self.super_table.super_name,
                    simple=self.simple_name,
                )


        # Data layout
        self.simple_dir = os.path.join(
            super_table.organization, super_table.super_name, self.identity, self.simple_name
        )
        self.data_dir = os.path.join(self.simple_dir, "data")
        self.snapshot_dir = os.path.join(self.simple_dir, "snapshots")

        if _live_leaf_verified:
            return

        # Fast path: if meta:leaf exists, don't touch storage
        if self.catalog.leaf_exists(
                self.super_table.organization, self.super_table.super_name, self.simple_name
        ):
            return

        # Read-only opt-out: refuse to bootstrap as a side effect. The
        # writer leaves the default so the first write to a new table
        # naturally creates it; readers opt out so a query against a
        # missing name fails fast rather than materializing an empty
        # table.
        if not create_if_missing:
            raise TableNotFoundError(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            )

        self.init_simple_table()

    def init_simple_table(self) -> None:
        """
        Initialize simple table:
          * If Redis meta:leaf already exists -> skip any folder checks/creations and bootstrapping.
          * Otherwise, create folders and bootstrap an initial empty snapshot and leaf pointer.
        """

        org = self.super_table.organization
        sup = self.super_table.super_name
        # Only structural creation participates in the namespace lock; writes
        # to existing tables remain concurrent and are drained by their leaf
        # locks during SuperTable deletion. Acquiring this before any storage
        # write closes the new-child write-after-prefix-verification race.
        namespace_token = self.catalog.acquire_namespace_lock(
            org, sup, ttl_s=30, timeout_s=60,
        )
        if not namespace_token:
            raise TimeoutError(
                f"Could not acquire namespace creation lock for {org}/{sup}"
            )
        try:
            # Check before either fast-path return: stale metadata recreated
            # behind a terminal tombstone must not make the object live.
            self.catalog.check_initialization_allowed(
                org,
                sup,
                namespace_token=namespace_token,
                simple=self.simple_name,
            )
            if not self.catalog.root_exists(org, sup):
                raise TableNotFoundError(org, sup, self.simple_name)
            # Another initializer may have won while this constructor waited.
            if self.catalog.leaf_exists(org, sup, self.simple_name):
                return

            # First-time initialization: ensure directories in storage.
            for p in (self.simple_dir, self.data_dir, self.snapshot_dir):
                try:
                    if not self.storage.exists(p):
                        self.storage.makedirs(p)
                except Exception:
                    # Object storage may no-op; that's fine.
                    pass

            initial_snapshot_file = generate_filename(alias=self.identity)
            new_simple_path = os.path.join(
                self.snapshot_dir, initial_snapshot_file,
            )
            snapshot_data = {
                "simple_name": self.simple_name,
                "location": self.simple_dir,
                "snapshot_version": 0,
                "last_updated_ms": int(datetime.now().timestamp() * 1000),
                "previous_snapshot": None,
                "schema": [],
                "resources": [],
                "tombstone": None,
                "tombstone_rows": 0,
                "tombstone_digest": None,
                "rowid_high_watermark": 0,
                "stats_file": None,
                "stats_rows": 0,
                "_row_filter": None,
            }
            self.storage.write_json(new_simple_path, snapshot_data)

            now_ms = int(datetime.now().timestamp() * 1000)
            payload_setter = getattr(
                self.catalog, "set_leaf_payload_cas", None,
            )
            if callable(payload_setter):
                # An ambiguous timeout may mean Redis committed the payload;
                # never delete or retry that immutable snapshot blindly.
                payload_setter(
                    org,
                    sup,
                    self.simple_name,
                    snapshot_data,
                    new_simple_path,
                    now_ms=now_ms,
                    namespace_token=namespace_token,
                )
            else:
                raise RuntimeError(
                    "Catalog does not support atomic initialize-only leaf "
                    "payloads; refusing ambiguous table creation"
                )
        finally:
            self.catalog.release_namespace_lock(
                org, sup, namespace_token,
            )

    def delete(self, role_name: str) -> str:
        """Start a new deletion, refusing any abandoned prior intent."""
        return self._delete_with_intent(role_name=role_name)

    def recover_delete(
            self,
            role_name: str,
            *,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        """Resume an abandoned deletion after external liveness proof.

        The caller must first prove that the previous process cannot resume an
        already-issued delete against this table's fixed storage prefix.  An
        ordinary :meth:`delete` never takes over a durable intent.
        """
        return self._delete_with_intent(
            role_name=role_name,
            recovery_intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
        )

    def _delete_with_intent(
            self,
            *,
            role_name: str,
            recovery_intent_id: Optional[str] = None,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        check_control_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            role_name=role_name,
            table_name=self.simple_name,
        )

        # Creation is fenced by the namespace lock, while writers are fenced
        # by the leaf lock. Acquire in the same namespace->leaf order as whole
        # SuperTable deletion and retain both through prefix verification and
        # atomic metadata cleanup so a concurrent initializer cannot recreate
        # the table inside a successful delete.
        namespace_token = self.catalog.acquire_namespace_lock(
            self.super_table.organization,
            self.super_table.super_name,
            ttl_s=30,
            timeout_s=60,
        )
        if not namespace_token:
            raise TimeoutError("Could not acquire the namespace deletion lock")
        token = self.catalog.acquire_simple_lock(
            self.super_table.organization,
            self.super_table.super_name,
            self.simple_name,
            ttl_s=30,
            timeout_s=60,
        )
        if not token:
            self.catalog.release_namespace_lock(
                self.super_table.organization,
                self.super_table.super_name,
                namespace_token,
            )
            raise TimeoutError("Could not acquire the table deletion lock")

        # The same auto-renewed leaf lock used by DataWriter covers physical
        # prefix verification and metadata removal as one mutation. A writer
        # can neither publish into the prefix mid-delete nor orphan a successor
        # between verification and leaf removal.
        simple_table_folder = os.path.join(
            self.super_table.organization, self.super_table.super_name, self.identity, self.simple_name
        )
        storage_prefixes = [
            simple_table_folder,
            *[
                os.path.join(
                    self.super_table.organization,
                    self.super_table.super_name,
                    mirror_name,
                    self.simple_name,
                )
                for mirror_name in ("delta", "iceberg", "parquet")
            ],
        ]
        try:
            # Lock acquisition may queue behind an existing writer. Re-read
            # CONTROL once both namespace and leaf fences are held, immediately
            # before publishing the durable deletion intent, so a role revoked
            # during that wait cannot begin a deletion. No irreversible storage
            # mutation has happened yet, so denial remains cleanly fail-closed.
            check_control_access(
                super_name=self.super_table.super_name,
                organization=self.super_table.organization,
                role_name=role_name,
                table_name=self.simple_name,
            )
            if recovery_intent_id is None:
                intent = self.catalog.begin_simple_deletion(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    namespace_token=namespace_token,
                    lock_token=token,
                )
            else:
                intent = self.catalog.recover_simple_deletion(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    expected_intent_id=recovery_intent_id,
                    namespace_token=namespace_token,
                    lock_token=token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
            intent_id = intent.get("intent_id") if isinstance(intent, dict) else None
            if not intent_id:
                raise RuntimeError("Catalog returned an invalid deletion intent")
            logger.info(
                "[deletion] SimpleTable cleanup started; recovery=%s",
                recovery_intent_id is not None,
            )

            # Prefixes on cloud stores usually do not have marker objects.
            # Delete and verify the full logical prefix before making it
            # undiscoverable in Redis. If this call or a lease becomes
            # ambiguous, the durable intent remains and recreation stays
            # blocked; an ordinary retry cannot clear it.
            for prefix in storage_prefixes:
                self.storage.delete_prefix(prefix)

            removed = self.catalog.delete_simple_table(
                self.super_table.organization,
                self.super_table.super_name,
                self.simple_name,
                lock_token=token,
                namespace_token=namespace_token,
                intent_id=intent_id,
            )
            if not removed:
                raise RuntimeError(
                    "Failed to remove catalog metadata after storage deletion"
                )
            if recovery_intent_id is not None:
                self.catalog.clear_simple_deletion_tombstone(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    expected_intent_id=intent_id,
                    namespace_token=namespace_token,
                    lock_token=token,
                    confirm_previous_owner_stopped=(
                        confirm_previous_owner_stopped
                    ),
                )
        finally:
            try:
                self.catalog.release_simple_lock(
                    self.super_table.organization,
                    self.super_table.super_name,
                    self.simple_name,
                    token,
                )
            finally:
                self.catalog.release_namespace_lock(
                    self.super_table.organization,
                    self.super_table.super_name,
                    namespace_token,
                )

        logger.info(
            "Deleted table storage and catalog metadata"
        )
        return str(intent_id)

    @classmethod
    def recover_pending_delete(
            cls,
            *,
            organization: str,
            super_name: str,
            simple_name: str,
            role_name: str,
            intent_id: str,
            confirm_previous_owner_stopped: bool = False,
    ) -> str:
        """Recover a deleted leaf even if its parent root is already absent."""
        catalog = RedisCatalog()
        storage = get_storage()
        parent = SuperTable.__new__(SuperTable)
        parent.identity = "super"
        parent.super_name = super_name
        parent.organization = organization
        parent.storage = storage
        parent.catalog = catalog
        parent.super_dir = os.path.join(organization, super_name, "super")
        table = cls.__new__(cls)
        table.super_table = parent
        table.identity = "tables"
        table.simple_name = simple_name
        table.storage = storage
        table.catalog = catalog
        table.simple_dir = os.path.join(
            organization, super_name, "tables", simple_name,
        )
        table.data_dir = os.path.join(table.simple_dir, "data")
        table.snapshot_dir = os.path.join(table.simple_dir, "snapshots")
        return table.recover_delete(
            role_name,
            intent_id=intent_id,
            confirm_previous_owner_stopped=confirm_previous_owner_stopped,
        )

    def get_simple_table_snapshot(self):
        """
        Read the current heavy snapshot via the Redis leaf pointer.

        If the Redis leaf stores a snapshot payload, use it to avoid storage reads.
        """
        ptr = getattr(self, "_pinned_leaf", None)
        if ptr is None:
            ptr = self.catalog.get_leaf(
                self.super_table.organization,
                self.super_table.super_name,
                self.simple_name,
            )
        if not ptr or not ptr.get("path"):
            raise FileNotFoundError("No path found in simple table leaf pointer.")
        # Preserve the exact leaf document that selected this snapshot.  The
        # writer uses its path/version as the compare-and-swap base when it
        # atomically publishes the successor.  Re-fetching later would create
        # a time-of-check/time-of-use gap after lock expiry or takeover.
        self._last_snapshot_leaf = dict(ptr)
        path = ptr["path"]

        payload = getattr(self, "_pinned_snapshot", None)
        if payload is None:
            payload = complete_snapshot_payload(
                ptr.get("payload") if isinstance(ptr, dict) else None,
                expected_version=ptr.get("version") if isinstance(ptr, dict) else None,
                require_policy_marker=True,
            )
        if payload is not None:
            return payload, path

        data = self.storage.read_json(path)
        return data, path

    def _write_restore_journal(
        self,
        *,
        snapshot_path: str,
        commit_id: str,
        snapshot_version: int,
        base_path: str,
    ) -> str:
        payload = {
            "version": 2,
            "snapshot_path": snapshot_path,
            "commit_id": commit_id,
            "snapshot_version": snapshot_version,
            "base_path": base_path,
            "created_at_ns": time.time_ns(),
        }
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
        if len(encoded) > _MAX_RESTORE_JOURNAL_BYTES:
            raise ValueError("Restore reconciliation journal is too large")
        journal_path = os.path.join(
            self.snapshot_dir,
            f"{_RESTORE_JOURNAL_PREFIX}{uuid.uuid4().hex}.json",
        )
        self.storage.write_json(journal_path, payload)
        return journal_path

    def _discard_restore_journal(self, journal_path: str) -> None:
        try:
            self.storage.delete(journal_path)
        except FileNotFoundError:
            pass
        except Exception:
            logger.warning("[restore] failed to remove reconciliation journal")

    def _cleanup_restore_candidate(
        self, *, snapshot_path: str, journal_path: str,
    ) -> None:
        try:
            self.storage.delete(snapshot_path)
        except FileNotFoundError:
            pass
        except Exception:
            # Retain the journal so a later lease holder can retry safely.
            logger.warning("[restore] failed to remove unpublished snapshot")
            return
        self._discard_restore_journal(journal_path)

    def _restore_candidate_status(
        self,
        *,
        current_leaf: Dict[str, Any],
        candidate_path: str,
        candidate_version: int,
    ) -> str:
        """Return ``published``, ``unpublished``, or ``unknown`` safely.

        A committed candidate can stop being the current leaf before a crashed
        publisher removes its journal. It is then immutable history and must
        never be collected as an orphan. Walk the monotonic predecessor chain
        until the candidate generation is reached; any incomplete/corrupt or
        excessively deep proof remains ``unknown`` and retains the object.
        """
        current_path = current_leaf.get("path")
        current_version = current_leaf.get("version")
        if not isinstance(current_path, str) or not current_path:
            return "unknown"
        if type(current_version) is not int or current_version < 0:
            return "unknown"
        if current_path == candidate_path:
            return "published"
        if current_version < candidate_version:
            return "unpublished"

        path = current_path
        version = current_version
        cached_payload: object = current_leaf.get("payload")
        visited: set[str] = set()
        for _step in range(_MAX_RESTORE_JOURNALS):
            if path in visited:
                return "unknown"
            visited.add(path)
            try:
                path = _contained_artifact_path(
                    path,
                    label="restore history snapshot",
                    required_prefix=self.snapshot_dir,
                )
                payload: object = complete_snapshot_payload(
                    cached_payload,
                    expected_version=version,
                    require_policy_marker=True,
                )
                if payload is None:
                    payload, _metadata = _read_sealed_json_object(
                        self.storage,
                        path,
                        max_bytes=_MAX_RESTORE_SNAPSHOT_BYTES,
                        label="restore history snapshot",
                    )
            except Exception:
                return "unknown"
            if (
                not isinstance(payload, dict)
                or payload.get("snapshot_version") != version
            ):
                return "unknown"
            previous = payload.get("previous_snapshot")
            if previous == candidate_path:
                return "published"
            if version <= candidate_version:
                return "unpublished"
            if not isinstance(previous, str) or not previous:
                return "unpublished" if previous is None else "unknown"
            # Every publisher increments the leaf exactly once and stores the
            # exact predecessor path. A different path at the candidate's
            # generation proves this UUID-named object was never published.
            if version - 1 == candidate_version:
                return "unpublished"
            path = previous
            version -= 1
            cached_payload = None
        return "unknown"

    def _reconcile_restore_journals(
        self,
        current_leaf: Dict[str, Any],
        *,
        confirm_inactive_candidates: bool = False,
    ) -> int:
        journal_paths = self.storage.list_files(
            self.snapshot_dir, _RESTORE_JOURNAL_PATTERN,
        )
        if len(journal_paths) > _MAX_RESTORE_JOURNALS:
            raise RuntimeError("Restore reconciliation journal fan-out is invalid")
        reconciled = 0
        for journal_path in journal_paths:
            try:
                journal_path = _contained_artifact_path(
                    journal_path,
                    label="restore reconciliation journal",
                    required_prefix=self.snapshot_dir,
                )
                payload, _journal_metadata = _read_sealed_json_object(
                    self.storage,
                    journal_path,
                    max_bytes=_MAX_RESTORE_JOURNAL_BYTES,
                    label="restore reconciliation journal",
                )
            except FileNotFoundError:
                continue
            if not isinstance(payload, dict) or payload.get("version") not in {
                1, 2,
            }:
                raise RuntimeError("Restore reconciliation journal is invalid")
            snapshot_path = _contained_artifact_path(
                payload.get("snapshot_path"),
                label="restore reconciliation snapshot",
                required_prefix=self.snapshot_dir,
            )
            commit_id = payload.get("commit_id")
            if (
                not isinstance(commit_id, str)
                or not commit_id
                or len(commit_id) > 256
            ):
                raise RuntimeError("Restore reconciliation journal is invalid")
            exact_current = (
                current_leaf.get("path") == snapshot_path
                and current_leaf.get("commit_id") == commit_id
            )
            if exact_current:
                self._discard_restore_journal(journal_path)
                reconciled += 1
                continue
            if payload.get("version") == 1:
                # The unreleased v1 journal did not record a candidate
                # generation, so a non-current object cannot safely be
                # distinguished from a committed historical ancestor.
                continue
            candidate_version = payload.get("snapshot_version")
            base_path = payload.get("base_path")
            if (
                type(candidate_version) is not int
                or candidate_version < 1
                or not isinstance(base_path, str)
                or not base_path
            ):
                raise RuntimeError("Restore reconciliation journal is invalid")
            _contained_artifact_path(
                base_path,
                label="restore reconciliation base snapshot",
                required_prefix=self.snapshot_dir,
            )
            if not self.storage.exists(snapshot_path):
                # A prior owner may still be paused just before its immutable
                # write. The new lease prevents publication, but retaining the
                # journal ensures its later write is eventually collected.
                if confirm_inactive_candidates:
                    self._discard_restore_journal(journal_path)
                    reconciled += 1
                continue
            candidate_payload, _candidate_metadata = _read_sealed_json_object(
                self.storage,
                snapshot_path,
                max_bytes=_MAX_RESTORE_SNAPSHOT_BYTES,
                label="restore reconciliation candidate",
            )
            if (
                not isinstance(candidate_payload, dict)
                or candidate_payload.get("snapshot_version")
                != candidate_version
                or candidate_payload.get("previous_snapshot") != base_path
                or candidate_payload.get("_restore_commit_id") != commit_id
            ):
                raise RuntimeError(
                    "Restore reconciliation journal does not identify its candidate"
                )
            status = self._restore_candidate_status(
                current_leaf=current_leaf,
                candidate_path=snapshot_path,
                candidate_version=candidate_version,
            )
            if status == "published":
                self._discard_restore_journal(journal_path)
                reconciled += 1
                continue
            if status != "unpublished":
                continue
            self._cleanup_restore_candidate(
                snapshot_path=snapshot_path,
                journal_path=journal_path,
            )
            reconciled += 1
        return reconciled

    def recover_pending_restore_objects(
        self,
        role_name: str,
        *,
        confirm_previous_owner_stopped: bool = False,
        authorization_callback: Optional[Callable[[], str]] = None,
    ) -> int:
        """Reconcile restore intents under the table lease.

        An absent candidate is normally retained because an expired writer can
        still be paused immediately before its immutable object write. An
        operator may discard only those absent intents after independently
        proving that the previous process cannot resume.
        """
        if type(confirm_previous_owner_stopped) is not bool:
            raise TypeError("confirm_previous_owner_stopped must be a boolean")
        org = self.super_table.organization
        sup = self.super_table.super_name
        check_control_access(
            super_name=sup,
            organization=org,
            role_name=role_name,
            table_name=self.simple_name,
        )
        token = self.catalog.acquire_simple_lock(
            org, sup, self.simple_name, ttl_s=30, timeout_s=60,
        )
        if not token:
            raise TimeoutError("Could not acquire the table lock")
        try:
            effective_role = (
                authorization_callback()
                if authorization_callback is not None else role_name
            )
            if not isinstance(effective_role, str) or not effective_role.strip():
                raise PermissionError("A current authorized role is required")
            check_control_access(
                super_name=sup,
                organization=org,
                role_name=effective_role.strip(),
                table_name=self.simple_name,
            )
            self.catalog.check_deletion_intent_absent(
                org, sup, simple=self.simple_name,
            )
            leaf = self.catalog.get_leaf(org, sup, self.simple_name)
            if (
                not isinstance(leaf, dict)
                or not isinstance(leaf.get("path"), str)
                or not leaf["path"]
                or type(leaf.get("version")) is not int
                or leaf["version"] < 0
            ):
                raise FileNotFoundError("The live table snapshot is unavailable")
            return self._reconcile_restore_journals(
                leaf,
                confirm_inactive_candidates=confirm_previous_owner_stopped,
            )
        finally:
            self.catalog.release_simple_lock(
                org, sup, self.simple_name, token,
            )

    def publish_restored_successor(
        self,
        *,
        role_name: str,
        source_snapshot: Dict[str, Any],
        lineage: Optional[Dict[str, Any]] = None,
        authorization_callback: Optional[Callable[[], str]] = None,
    ) -> Dict[str, Any]:
        """Publish restored content as a new immutable successor snapshot.

        Restore and rollback must never repoint a live leaf directly at an old
        generation: doing so makes versions non-monotonic and severs the
        history chain.  This method pins the current leaf under the renewable
        table lease, validates all restored artifacts remain inside this exact
        table, writes a new snapshot whose predecessor is the current head,
        and commits it through the SDK's atomic snapshot CAS.
        """
        check_control_access(
            super_name=self.super_table.super_name,
            organization=self.super_table.organization,
            role_name=role_name,
            table_name=self.simple_name,
        )
        if not isinstance(source_snapshot, dict):
            raise ValueError("Restored snapshot must be an object")
        if lineage is not None and not isinstance(lineage, dict):
            raise TypeError("Restore lineage must be an object")

        org = self.super_table.organization
        sup = self.super_table.super_name
        sample_authority_generation = getattr(
            type(self.catalog), "sample_write_authority_generation", None,
        )
        validate_authority_generation = getattr(
            type(self.catalog), "validate_write_authority_generation", None,
        )

        def stable_control_access(
            fallback_role: str,
        ) -> tuple[str, Optional[Sequence[int]]]:
            """Authorize inside one unchanged RBAC/root generation window."""
            def current_role() -> str:
                value = (
                    authorization_callback()
                    if authorization_callback is not None else fallback_role
                )
                if not isinstance(value, str) or not value.strip():
                    raise PermissionError("A current authorized role is required")
                return value.strip()

            def check_once(value: str) -> None:
                check_control_access(
                    super_name=sup,
                    organization=org,
                    role_name=value,
                    table_name=self.simple_name,
                )

            if not (
                callable(sample_authority_generation)
                and callable(validate_authority_generation)
            ):
                value = current_role()
                check_once(value)
                return value, None

            for _attempt in range(3):
                generation = self.catalog.sample_write_authority_generation(
                    org, sup,
                )
                if (
                    not isinstance(generation, (tuple, list))
                    or len(generation) != 4
                    or any(
                        type(component) is not int or component < 0
                        for component in generation
                    )
                ):
                    raise RuntimeError(
                        "The write-authority generation is invalid"
                    )
                value = current_role()
                check_once(value)
                if self.catalog.validate_write_authority_generation(
                    org, sup, generation,
                ) is True:
                    return value, tuple(generation)
            raise PermissionError(
                "Write authorization changed continuously during restore"
            )

        token = self.catalog.acquire_simple_lock(
            org, sup, self.simple_name, ttl_s=30, timeout_s=60,
        )
        if not token:
            raise TimeoutError("Could not acquire the table lock")
        try:
            # The lease acquisition may queue behind a long writer.  Re-read
            # the authoritative role immediately before any restore I/O so a
            # revoked CONTROL grant cannot survive the wait.
            effective_role, _initial_authority_generation = (
                stable_control_access(role_name)
            )
            self.catalog.check_deletion_intent_absent(
                org, sup, simple=self.simple_name,
            )
            leaf = self.catalog.get_leaf(org, sup, self.simple_name)
            if not isinstance(leaf, dict) or not isinstance(
                leaf.get("path"), str,
            ) or not leaf["path"]:
                raise FileNotFoundError("The live table snapshot is unavailable")
            if type(leaf.get("version")) is not int or leaf["version"] < 0:
                raise RuntimeError("The live table generation is invalid")
            self._reconcile_restore_journals(leaf)

            current_path = _contained_artifact_path(
                leaf["path"],
                label="live snapshot",
                required_prefix=self.snapshot_dir,
            )
            mirrors = self.catalog.get_mirrors(org, sup)
            if not isinstance(mirrors, list) or any(
                not isinstance(value, str) for value in mirrors
            ):
                raise RuntimeError("Mirror configuration is invalid")
            if mirrors:
                # A successor cannot claim success while configured mirror
                # formats still expose the old snapshot. Until this narrow
                # restore primitive participates in the durable mirror outbox,
                # reject before writing an orphan snapshot object.
                raise RuntimeError(
                    "Restored successors for mirror-enabled tables require "
                    "mirror reconciliation support"
                )
            current_payload: object = complete_snapshot_payload(
                leaf.get("payload"),
                expected_version=leaf["version"],
                require_policy_marker=True,
            )
            if current_payload is None:
                current_payload, _current_metadata = _read_sealed_json_object(
                    self.storage,
                    current_path,
                    max_bytes=_MAX_RESTORE_SNAPSHOT_BYTES,
                    label="live restore snapshot",
                )
            if (
                not isinstance(current_payload, dict)
                or current_payload.get("snapshot_version") != leaf["version"]
            ):
                raise RuntimeError("The live snapshot and catalog generation disagree")

            # A restore source is recovery input, not already-authoritative
            # live metadata. Carry only fields whose physical artifacts are
            # independently validated below; unknown execution, linked-share,
            # row-ID, and cache hints must never become trusted by copying an
            # arbitrary caller dictionary into the live snapshot.
            restored: Dict[str, Any] = {
                field_name: copy.deepcopy(source_snapshot[field_name])
                for field_name in (
                    "schema",
                    "resources",
                    "tombstone",
                    "tombstone_rows",
                    "tombstone_digest",
                    "tombstone_format",
                    "tombstone_object_seal",
                )
                if field_name in source_snapshot
            }
            restored["stats_file"] = None
            restored["stats_rows"] = 0
            restored["_row_filter"] = None
            resources = restored.get("resources")
            schema = restored.get("schema")
            if not isinstance(resources, list) or len(resources) > 10_000:
                raise ValueError("Restored snapshot resource fan-out is invalid")
            if not isinstance(schema, (dict, list)):
                raise ValueError("Restored snapshot schema is invalid")
            try:
                schema_size = len(json.dumps(schema, allow_nan=False).encode("utf-8"))
            except (TypeError, ValueError):
                raise ValueError("Restored snapshot schema is invalid") from None
            if schema_size > 1024 * 1024:
                raise ValueError("Restored snapshot schema exceeds its size limit")
            declared_schema_names = _restored_schema_field_names(schema)
            seen_resources: set[str] = set()
            physical_schema_folded: dict[str, str] = {}
            physical_schema_types: dict[str, pa.DataType] = {}
            total_rows = 0
            total_bytes = 0
            total_column_chunks = 0
            total_footer_bytes = 0
            from supertable.processing import stats_seal_for_metadata
            for resource_index, resource in enumerate(resources):
                if not isinstance(resource, dict):
                    raise ValueError("Restored snapshot resource is invalid")
                path = _contained_artifact_path(
                    resource.get("file"), label="resource",
                    required_prefix=self.data_dir,
                )
                if path in seen_resources:
                    raise ValueError("Restored snapshot repeats a resource")
                seen_resources.add(path)
                rows = resource.get("rows")
                file_size = resource.get("file_size")
                if (
                    type(rows) is not int
                    or rows < 0
                    or rows > 1_000_000_000
                    or type(file_size) is not int
                    or file_size <= 0
                    or file_size > 2 * 1024 * 1024 * 1024
                ):
                    raise ValueError(
                        "Restored snapshot resource bounds are invalid"
                    )
                total_rows += rows
                total_bytes += file_size
                if (
                    total_rows > 1_000_000_000
                    or total_bytes > 2 * 1024 * 1024 * 1024 * 1024
                ):
                    raise ValueError(
                        "Restored snapshot aggregate bounds are invalid"
                    )
                _validate_physical_containment(
                    self.storage, path, self.data_dir,
                )
                try:
                    (
                        object_metadata,
                        parquet_metadata,
                        footer_bytes,
                    ) = _sealed_parquet_metadata(
                        self.storage, path, expected_size=file_size,
                    )
                except FileNotFoundError:
                    raise FileNotFoundError(
                        "A restored data artifact is unavailable"
                    ) from None
                if int(parquet_metadata.num_rows) != rows:
                    raise RuntimeError(
                        "Restored Parquet row count disagrees with its snapshot"
                    )
                total_footer_bytes += footer_bytes
                if (
                    total_footer_bytes
                    > _MAX_RESTORE_AGGREGATE_FOOTER_BYTES
                ):
                    raise ValueError(
                        "Restored Parquet aggregate footer bytes exceed the "
                        "safety limit"
                    )
                total_column_chunks += (
                    int(parquet_metadata.num_row_groups)
                    * int(parquet_metadata.num_columns)
                )
                if (
                    total_column_chunks
                    > _MAX_RESTORE_AGGREGATE_COLUMN_CHUNKS
                ):
                    raise ValueError(
                        "Restored Parquet footer fan-out exceeds its safety limit"
                    )
                arrow_schema = parquet_metadata.schema.to_arrow_schema()
                footer_names: set[str] = set()
                for field in arrow_schema:
                    column_name = str(field.name)
                    folded_name = column_name.casefold()
                    if folded_name in footer_names:
                        raise ValueError(
                            "Restored physical schema repeats a column"
                        )
                    footer_names.add(folded_name)
                    prior_name = physical_schema_folded.get(folded_name)
                    if prior_name is not None and prior_name != column_name:
                        raise ValueError(
                            "Restored physical schemas contain case-colliding columns"
                        )
                    physical_schema_folded[folded_name] = column_name
                    if folded_name not in {
                        "__rowid__", "__timestamp__", "__file__",
                        "__supertable_source_file__",
                        "__supertable_scan_filename__",
                    } and not folded_name.startswith("__supertable_"):
                        dtype = field.type
                        previous_dtype = physical_schema_types.get(column_name)
                        physical_schema_types[column_name] = (
                            dtype
                            if previous_dtype is None
                            else _lossless_restore_physical_type(
                                previous_dtype, dtype,
                            )
                        )
                _validate_declared_object_seal(
                    resource.get("object_seal"), object_metadata,
                )
                exact_stats_seal = stats_seal_for_metadata(
                    path,
                    parquet_metadata,
                )
                for field_name, actual_value in (
                    ("footer_sha256", exact_stats_seal.footer_sha256),
                    ("stats_rows", exact_stats_seal.stats_rows),
                    ("stats_digest", exact_stats_seal.stats_digest),
                ):
                    if (
                        field_name in resource
                        and resource.get(field_name) != actual_value
                    ):
                        raise RuntimeError(
                            "Restored resource statistics disagree with its footer"
                        )
                resources[resource_index] = {
                    "file": path,
                    "rows": rows,
                    "file_size": file_size,
                    "columns": int(parquet_metadata.num_columns),
                    "object_seal": _object_seal_document(object_metadata),
                    "footer_sha256": exact_stats_seal.footer_sha256,
                    "stats_rows": exact_stats_seal.stats_rows,
                    "stats_digest": exact_stats_seal.stats_digest,
                }

            if resources:
                if any(
                    name not in physical_schema_types
                    for name in declared_schema_names
                ):
                    raise ValueError(
                        "Restored snapshot schema is not present in its resources"
                    )
                # Caller-provided type strings are recovery input, never an
                # authority boundary.  Preserve the declared logical names and
                # order, but derive every live type from all physical
                # occurrences using the restore-specific lossless lattice.
                restored["schema"] = {
                    name: str(_polars_dtype_for_arrow_field(pa.field(
                        name, physical_schema_types[name],
                    )))
                    for name in declared_schema_names
                }
                if physical_schema_types and not declared_schema_names:
                    raise ValueError(
                        "Restored snapshot schema omits physical user columns"
                    )
            else:
                empty_schema = _restored_schema_type_values(
                    schema, declared_schema_names,
                )
                from supertable.engine.engine_common import (
                    snapshot_duckdb_type,
                    snapshot_spark_type,
                )
                for raw_type in empty_schema.values():
                    try:
                        snapshot_duckdb_type(raw_type)
                        snapshot_spark_type(raw_type)
                    except RuntimeError:
                        raise ValueError(
                            "Restored snapshot schema type is invalid"
                        ) from None
                restored["schema"] = empty_schema

            from supertable.tombstone_manifest_v2 import (
                TOMBSTONE_FORMAT_V2,
                TombstoneManifestV2Error,
                normalize_snapshot_tombstone_state,
            )
            try:
                restored_tombstone = normalize_snapshot_tombstone_state(
                    restored,
                )
            except (TypeError, TombstoneManifestV2Error):
                raise ValueError(
                    "Restored deletion-vector state is invalid"
                ) from None
            if (
                restored_tombstone.pointer is not None
                and restored_tombstone.tombstone_format
                == TOMBSTONE_FORMAT_V2
            ):
                # A v2 root binds its immutable manifest to the source
                # snapshot_version. Reusing it under this new successor
                # generation would make every reader reject the manifest.
                # Re-encoding that root needs its own multi-object journal;
                # fail before publication until that transaction exists.
                raise RuntimeError(
                    "Active format-2 deletion vectors cannot be restored as "
                    "a successor"
                )
            if (
                restored_tombstone.pointer is not None
                and (
                    restored_tombstone.rows > total_rows
                    or restored_tombstone.rows > _MAX_RESTORE_TOMBSTONE_ROWS
                )
            ):
                raise ValueError(
                    "Restored deletion-vector row count exceeds the table bound"
                )
            tombstone_object_metadata: Optional[ObjectMetadata] = None
            for field_name, subdir in (("tombstone", "tombstone"),):
                pointer = restored.get(field_name)
                if pointer is None:
                    continue
                path = _contained_artifact_path(
                    pointer, label=field_name,
                    required_prefix=os.path.join(self.simple_dir, subdir),
                )
                required_prefix = os.path.join(self.simple_dir, subdir)
                _validate_physical_containment(
                    self.storage, path, required_prefix,
                )
                try:
                    (
                        artifact_metadata,
                        artifact_parquet,
                        tombstone_footer_bytes,
                    ) = _sealed_parquet_metadata(
                        self.storage,
                        path,
                        expected_size=None,
                    )
                    total_footer_bytes += tombstone_footer_bytes
                    if (
                        total_footer_bytes
                        > _MAX_RESTORE_AGGREGATE_FOOTER_BYTES
                    ):
                        raise ValueError(
                            "Restored Parquet aggregate footer bytes "
                            "exceed the safety limit"
                        )
                    if field_name == "tombstone":
                        tombstone_object_metadata = artifact_metadata
                        if (
                            artifact_metadata.size
                            > _MAX_RESTORE_TOMBSTONE_BYTES
                            or int(artifact_parquet.num_rows)
                            != restored_tombstone.rows
                        ):
                            raise ValueError(
                                "Restored deletion vector exceeds its safety "
                                "limit or row-count seal"
                            )
                        expanded_bytes = 0
                        for group_index in range(
                            artifact_parquet.num_row_groups
                        ):
                            group = artifact_parquet.row_group(group_index)
                            for column_index in range(group.num_columns):
                                value = int(
                                    group.column(
                                        column_index,
                                    ).total_uncompressed_size
                                    or 0
                                )
                                if value < 0:
                                    raise ValueError(
                                        "Restored deletion-vector metadata is invalid"
                                    )
                                expanded_bytes += value
                                if (
                                    expanded_bytes
                                    > _MAX_RESTORE_TOMBSTONE_BYTES
                                ):
                                    raise ValueError(
                                        "Restored deletion vector exceeds its "
                                        "decoded-byte limit"
                                    )
                except FileNotFoundError:
                    raise FileNotFoundError(
                        "A restored metadata artifact is unavailable"
                    ) from None
                _validate_declared_object_seal(
                    restored.get(f"{field_name}_object_seal"),
                    artifact_metadata,
                )
                restored[field_name] = path
                restored[f"{field_name}_object_seal"] = (
                    _object_seal_document(artifact_metadata)
                )

            if restored_tombstone.pointer is not None:
                # A compressed-size/footer bound alone does not constrain
                # dictionary/RLE expansion. Decode conditionally in bounded
                # Arrow batches, prove resource membership before strings are
                # materialized in Polars, then validate row IDs and the format
                # specific digest over that exact sealed object.
                if tombstone_object_metadata is None:
                    raise RuntimeError(
                        "Restored deletion vector has no immutable object seal"
                    )
                _bounded_restored_tombstone_frame(
                    self.storage,
                    restored_tombstone.pointer,
                    observed=tombstone_object_metadata,
                    expected_rows=int(restored_tombstone.rows),
                    expected_digest=restored_tombstone.digest,
                    tombstone_format=restored_tombstone.tombstone_format,
                    allowed_files=seen_resources,
                )
                resealed_tombstone = self.storage.stat_object(
                    restored_tombstone.pointer,
                )
                if (
                    tombstone_object_metadata is None
                    or not isinstance(resealed_tombstone, ObjectMetadata)
                    or resealed_tombstone != tombstone_object_metadata
                ):
                    raise RuntimeError(
                        "Restored deletion vector changed during validation"
                    )
            else:
                restored.pop("tombstone_object_seal", None)

            restored["simple_name"] = self.simple_name
            restored["location"] = self.simple_dir
            restored["snapshot_version"] = leaf["version"] + 1
            restored["previous_snapshot"] = current_path
            restored["last_updated_ms"] = int(time.time() * 1000)
            restored["lineage"] = dict(lineage or {
                "source_type": "snapshot_restore",
                "restored_snapshot_version": source_snapshot.get(
                    "snapshot_version"
                ),
            })
            commit_id = uuid.uuid4().hex
            restored["_restore_commit_id"] = commit_id
            restored = snapshot_cache_payload(restored)
            try:
                lineage_size = len(json.dumps(
                    restored["lineage"],
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8"))
                snapshot_size = len(json.dumps(
                    restored,
                    ensure_ascii=False,
                    allow_nan=False,
                ).encode("utf-8"))
            except (TypeError, ValueError, OverflowError):
                raise ValueError("Restored snapshot is not valid JSON") from None
            if lineage_size > 64 * 1024:
                raise ValueError("Restored snapshot lineage exceeds its size limit")
            if snapshot_size > 8 * 1024 * 1024:
                raise ValueError("Restored snapshot exceeds its size limit")

            effective_role, _prewrite_authority_generation = (
                stable_control_access(role_name)
            )
            snapshot_path = os.path.join(
                self.snapshot_dir, generate_filename(alias=self.identity),
            )
            journal_path = self._write_restore_journal(
                snapshot_path=snapshot_path,
                commit_id=commit_id,
                snapshot_version=int(restored["snapshot_version"]),
                base_path=current_path,
            )
            try:
                self.storage.write_json(snapshot_path, restored)
            except Exception:
                self._cleanup_restore_candidate(
                    snapshot_path=snapshot_path,
                    journal_path=journal_path,
                )
                raise
            try:
                _final_role, authority_generation = stable_control_access(
                    effective_role,
                )
            except Exception:
                self._cleanup_restore_candidate(
                    snapshot_path=snapshot_path,
                    journal_path=journal_path,
                )
                raise
            try:
                commit_kwargs: Dict[str, Any] = {
                    "expected_version": leaf["version"],
                    "expected_path": current_path,
                    "lock_token": token,
                    "commit_id": commit_id,
                    "now_ms": restored["last_updated_ms"],
                    "expected_mirrors": [],
                }
                if authority_generation is not None:
                    commit_kwargs["expected_write_authority_generation"] = (
                        authority_generation
                    )
                leaf_version, root_version = self.catalog.commit_snapshot(
                    org,
                    sup,
                    self.simple_name,
                    restored,
                    snapshot_path,
                    **commit_kwargs,
                )
            except Exception as commit_error:
                # A Redis timeout may arrive after the atomic script committed.
                # A successor may also advance the leaf before this readback;
                # preserve a candidate found anywhere in the immutable history.
                try:
                    observed_leaf = self.catalog.get_leaf(
                        org, sup, self.simple_name,
                    )
                    if (
                        isinstance(observed_leaf, dict)
                        and observed_leaf.get("path") == snapshot_path
                        and observed_leaf.get("commit_id") == commit_id
                    ):
                        # Never delete an object selected by the live leaf,
                        # even if the returned generation violates the commit
                        # contract.  A mismatched generation is corruption or
                        # an incompatible catalog implementation: retain the
                        # journal/object and fail closed for operator recovery.
                        if observed_leaf.get("version") != leaf["version"] + 1:
                            raise RuntimeError(
                                "Committed restore has an invalid leaf generation"
                            ) from None
                        root = self.catalog.get_root(org, sup)
                        if not isinstance(root, dict) or type(
                            root.get("version")
                        ) is not int:
                            raise RuntimeError(
                                "Committed restore has no valid root generation"
                            ) from None
                        self._discard_restore_journal(journal_path)
                        return {
                            "snapshot": restored,
                            "snapshot_path": snapshot_path,
                            "leaf_version": int(observed_leaf["version"]),
                            "root_version": int(root["version"]),
                            "from_version": int(
                                current_payload["snapshot_version"]
                            ),
                        }
                    if not isinstance(observed_leaf, dict):
                        raise RuntimeError(
                            "Restore publication outcome is unavailable"
                        ) from None
                    if observed_leaf.get("path") == snapshot_path:
                        raise RuntimeError(
                            "Restore publication identity is inconsistent"
                        ) from None
                    candidate_status = self._restore_candidate_status(
                        current_leaf=observed_leaf,
                        candidate_path=snapshot_path,
                        candidate_version=leaf["version"] + 1,
                    )
                    if candidate_status == "published":
                        root = self.catalog.get_root(org, sup)
                        if (
                            not isinstance(root, dict)
                            or type(root.get("version")) is not int
                        ):
                            raise RuntimeError(
                                "Committed restore has no valid root generation"
                            ) from None
                        self._discard_restore_journal(journal_path)
                        return {
                            "snapshot": restored,
                            "snapshot_path": snapshot_path,
                            "leaf_version": int(leaf["version"] + 1),
                            "root_version": int(root["version"]),
                            "from_version": int(
                                current_payload["snapshot_version"]
                            ),
                        }
                    if candidate_status != "unpublished":
                        raise RuntimeError(
                            "Restore publication outcome remains ambiguous"
                        ) from None
                except Exception:
                    # Outcome is still ambiguous; retain both journal and
                    # immutable candidate for a later fenced reconciliation.
                    raise RuntimeError(
                        "Restore publication outcome could not be reconciled"
                    ) from None
                self._cleanup_restore_candidate(
                    snapshot_path=snapshot_path,
                    journal_path=journal_path,
                )
                if isinstance(commit_error, PermissionError):
                    raise commit_error from None
                raise RuntimeError("Restore snapshot publication failed") from None
            self._discard_restore_journal(journal_path)
            return {
                "snapshot": restored,
                "snapshot_path": snapshot_path,
                "leaf_version": int(leaf_version),
                "root_version": int(root_version),
                "from_version": int(current_payload["snapshot_version"]),
            }
        finally:
            self.catalog.release_simple_lock(org, sup, self.simple_name, token)

    def export_to(self, target_dir: str, compression_level: int = 3, small_only: bool = False):
        """Write a standalone copy of the current data into ``target_dir``.

        Reads the current snapshot's parquet resources and re-writes them
        as memory-bounded parquet chunks (each ~``max_memory_chunk_size``,
        from the per-table config or the global default) under
        ``target_dir``.  This is a pure copy: it does NOT create a new
        snapshot, advance the Redis leaf, or touch ``data/``/``snapshots/``.

        Logically-deleted rows are physically dropped: the snapshot's
        deletion-vector (``tombstone`` pointer) is read and its
        ``__rowid__`` values are anti-joined out of the exported files, so
        a standalone export never contains tombstoned rows.

        Args:
            target_dir: destination directory for the exported parquet
                files (created if missing).  Typically an
                ``export/<timestamp>/`` folder next to ``data/``.
            compression_level: zstd level for the exported parquet.
            small_only: when False (default) every resource is read and
                re-chunked; when True only files smaller than
                ``max_memory_chunk_size`` are included.

        Returns:
            ``dict`` with ``files`` (list of written paths),
            ``files_written``, ``total_rows`` and ``total_bytes``.
        """
        from supertable.processing import (
            compact_resources,
            ROWID_COL,
            TOMBSTONE_FILE_COL,
            load_tombstone,
            load_tombstone_manifest_from_storage,
            load_tombstone_segments,
            tombstone_cache_identity,
        )
        from supertable.data_classes import TombstoneSegmentDef
        from supertable.tombstone_manifest_v2 import (
            TOMBSTONE_FORMAT_V2,
            TOMBSTONE_FORMAT_V3,
            normalize_snapshot_tombstone_state,
        )

        snapshot, _path = self.get_simple_table_snapshot()
        table_config = self.catalog.get_table_config(
            self.super_table.organization,
            self.super_table.super_name,
            self.simple_name,
        ) or {}

        # Read the deletion-vector (if any) so its rows are dropped from
        # the export rather than copied verbatim.
        dead_rowids_by_file: Optional[Dict[str, set[int]]] = None
        tombstone_state = normalize_snapshot_tombstone_state(snapshot)
        tombstone_path = tombstone_state.pointer
        tombstone_format = tombstone_state.tombstone_format
        if tombstone_path:
            allowed_files: set[str] = set()
            for resource in snapshot.get("resources") or []:
                if not isinstance(resource, dict):
                    continue
                resource_file = resource.get("file")
                if isinstance(resource_file, str) and resource_file:
                    allowed_files.add(resource_file)
            if tombstone_format == TOMBSTONE_FORMAT_V2:
                manifest_prefix = (
                    f"{self.simple_dir.rstrip('/')}/tombstone/"
                )
                if not tombstone_path.startswith(manifest_prefix):
                    raise ValueError(
                        "Deletion-vector manifest pointer escapes the pinned "
                        "simple table"
                    )
                manifest = load_tombstone_manifest_from_storage(
                    self.storage,
                    tombstone_path,
                    expected_organization=self.super_table.organization,
                    expected_super_name=self.super_table.super_name,
                    expected_simple_name=self.simple_name,
                    pinned_snapshot_version=snapshot.get("snapshot_version"),
                    expected_total_rows=tombstone_state.rows,
                    expected_digest=tombstone_state.digest,
                    expected_segment_prefix=os.path.join(
                        self.simple_dir, "tombstone",
                    ),
                )
                segment_defs = tuple(
                    TombstoneSegmentDef(
                        cache_key=segment.file,
                        tombstone_path=segment.file,
                        expected_rows=segment.rows,
                        file_size=segment.file_size,
                        tombstone_digest=segment.digest,
                    )
                    for segment in manifest.segments
                )
                manifest_cache_identity = tombstone_cache_identity(
                    tombstone_path,
                    organization=self.super_table.organization,
                    storage=self.storage,
                )
                tomb_df = load_tombstone_segments(
                    segment_defs,
                    storage=self.storage,
                    cache_identity=(
                        "export-dv-v2:"
                        f"{manifest_cache_identity}:"
                        f"{tombstone_state.digest}"
                    ),
                    expected_rows=tombstone_state.rows,
                    allowed_files=allowed_files,
                    allow_cache=False,
                )
            else:
                if tombstone_format == TOMBSTONE_FORMAT_V3:
                    tombstone_prefix = (
                        f"{self.simple_dir.rstrip('/')}/tombstone/"
                    )
                    if not tombstone_path.startswith(tombstone_prefix):
                        raise ValueError(
                            "Format-3 deletion-vector pointer escapes the "
                            "pinned simple table"
                        )
                tomb_df = load_tombstone(
                    tombstone_path,
                    cache_identity=tombstone_cache_identity(
                        tombstone_path,
                        organization=self.super_table.organization,
                        storage=self.storage,
                    ),
                    allow_cache=False,
                    required=True,
                    expected_rows=tombstone_state.rows,
                    expected_digest=tombstone_state.digest,
                    allowed_files=allowed_files,
                    tombstone_format=tombstone_format,
                    storage=self.storage,
                    artifact_key=tombstone_path,
                )
            if tomb_df is None:  # defensive: required=True must raise instead
                raise RuntimeError("Required deletion-vector could not be loaded")
            dead_rowids_by_file = {}
            for file_key, rowid in tomb_df.select(
                [TOMBSTONE_FILE_COL, ROWID_COL]
            ).iter_rows():
                dead_rowids_by_file.setdefault(str(file_key), set()).add(int(rowid))

        _considered, total_rows, new_resources, _sunset = compact_resources(
            snapshot=snapshot,
            data_dir=target_dir,
            compression_level=compression_level,
            table_config=table_config,
            small_only=small_only,
            dead_rowids_by_file=dead_rowids_by_file,
            required_reads=True,
        )

        files = [r.get("file") for r in new_resources if isinstance(r, dict) and r.get("file")]
        total_bytes = sum(
            int(r.get("file_size") or 0) for r in new_resources if isinstance(r, dict)
        )
        return {
            "files": files,
            "files_written": len(files),
            "total_rows": int(total_rows),
            "total_bytes": int(total_bytes),
        }

    def update(self, new_resources, sunset_files, model_df, last_snapshot=None, last_snapshot_path=None, lineage=None, profiler: Optional[Profiler] = None):
        """
        Build and write a new heavy snapshot on storage.
        Returns: (snapshot_dict, snapshot_path)

        If last_snapshot and last_snapshot_path are provided, skips the redundant
        snapshot read (caller already holds the data under lock).

        Args:
            model_df: Polars DataFrame whose schema becomes the new snapshot's
                ``schema`` / ``schemaString`` ("last write wins" — see
                docs/03_data_model.md "Schema Field Semantics").  Pass ``None``
                to PRESERVE the previous snapshot's schema unchanged — used by
                delete-only writes which must not alter the table shape.  Only
                update paths are allowed to change schema; deletes never are.
            lineage: Optional dict of data provenance metadata.  Stored in the
                snapshot JSON so historical versions carry their origin.
        """
        p = profiler or get_null_profiler()
        if last_snapshot is not None and last_snapshot_path is not None:
            last_simple_table = last_snapshot
            last_simple_table_path = last_snapshot_path
        else:
            # Fallback: read current snapshot (backward compatible)
            with p.span("simple_update.read_snapshot"):
                last_simple_table, last_simple_table_path = self.get_simple_table_snapshot()

        with p.span("simple_update.merge_resources"):
            current_resources = last_simple_table.get("resources", [])
            sunset_set = set(sunset_files)
            updated_resources = [res for res in current_resources if res.get("file") not in sunset_set]
            updated_resources.extend(new_resources)
            last_simple_table["resources"] = updated_resources

        # Update metadata
        # An expected-absent one-shot write uses an in-memory compatibility
        # version-zero base and an empty CAS path. Its first durable snapshot is
        # version one but has no predecessor object; ordinary updates retain the
        # exact immutable predecessor pointer.
        last_simple_table["previous_snapshot"] = last_simple_table_path or None
        last_simple_table["last_updated_ms"] = int(datetime.now().timestamp() * 1000)
        last_simple_table["snapshot_version"] = int(last_simple_table.get("snapshot_version", 0)) + 1

        # Schema policy: only "update" callers (those that supply a model_df)
        # may change the snapshot schema.  Delete-only writers pass
        # model_df=None so the previous snapshot's schema / schemaString carry
        # forward verbatim — a delete must never shrink the metadata view of
        # the table even though the delete-predicate dataframe only carries
        # the key columns.
        if model_df is not None:
            with p.span("simple_update.collect_schema"):
                schema_list: Dict[str, str] | List[Dict[str, Any]] = (
                    collect_schema(model_df)
                )
                if not schema_list:
                    # Fallback: derive schema from Polars dtypes if helper returns empty.
                    schema_list = _schema_list_from_polars_df(model_df)
                last_simple_table["schema"] = schema_list
                # Also store a Spark StructType JSON for downstream Delta mirrors.
                try:
                    last_simple_table["schemaString"] = json.dumps({"type": "struct", "fields": schema_list}, separators=(",", ":"))
                except Exception:
                    pass
        # else: leave last_simple_table["schema"] and ["schemaString"] untouched.

        # Data lineage — record provenance of this write
        if lineage and isinstance(lineage, dict):
            last_simple_table["lineage"] = lineage

        # Every newly published Redis cache must explicitly seal the linked-
        # share policy state. Preserve a valid inherited overlay; ordinary
        # unshared snapshots use JSON null as the canonical unrestricted value.
        normalized_snapshot = snapshot_cache_payload(last_simple_table)
        last_simple_table.clear()
        last_simple_table.update(normalized_snapshot)

        # Write new heavy snapshot file
        new_simple_path = os.path.join(self.snapshot_dir, generate_filename(alias=self.identity))
        with p.span("simple_update.write_json"):
            self.storage.write_json(new_simple_path, last_simple_table)

        p.add("snapshot_resources_count", len(updated_resources))
        p.add("snapshot_sunset_count", len(sunset_set))
        p.add("snapshot_new_resources_count", len(new_resources))

        return last_simple_table, new_simple_path
