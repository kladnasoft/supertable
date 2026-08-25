"""Trusted, typed OData keyset-continuation state.

The public SQL readers deliberately do not accept this state.  Core may pass
it only through ``query_odata_sql_stream`` after authenticating and decrypting
its opaque continuation token; the SDK then binds it to the exact
direct-column ``ORDER BY`` tuple before the DuckDB backend constructs a
parameterised seek predicate.
"""

from __future__ import annotations

import base64
import json
import math
import re
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

import duckdb
from sqlglot import exp

from supertable.row_identity import MAX_TABLE_ROWID, ODATA_INTERNAL_ROWID_COLUMN


_BOUNDARY_VERSION = 1
_MAX_BOUNDARY_BYTES = 64 * 1024
_MAX_ORDER_TERMS = 256
_MAX_COLUMN_BYTES = 512
_MAX_STRING_BYTES = 16 * 1024
_MAX_BINARY_BYTES = 16 * 1024
_MAX_DECIMAL_CHARS = 80
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1
_UINT64_MAX = (1 << 64) - 1
_SIGNED_INTEGER_RE = re.compile(r"-?(?:0|[1-9][0-9]*)\Z")
_UNSIGNED_INTEGER_RE = re.compile(r"(?:0|[1-9][0-9]*)\Z")
_DECIMAL_RE = re.compile(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z")
_TEMPORAL_RE = re.compile(
    r"(?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2})T"
    r"(?P<clock>[0-9]{2}:[0-9]{2}:[0-9]{2})"
    r"(?:\.(?P<fraction>[0-9]{1,9}))?"
    r"(?P<offset>Z|[+-][0-9]{2}:[0-9]{2})?\Z"
)


@dataclass(frozen=True)
class ODataBoundaryValue:
    """One validated typed seek value and its safe DuckDB parameter."""

    type: str
    value: Any


@dataclass(frozen=True)
class ODataOrderBoundary:
    column: str
    direction: str
    value: ODataBoundaryValue


@dataclass(frozen=True)
class ODataContinuationBoundary:
    version: int
    order: tuple[ODataOrderBoundary, ...]
    row_identity: int


def _exact_keys(value: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(value) != expected or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{label} has invalid fields")


def _canonical_integer(raw: Any, *, signed: bool) -> int:
    if not isinstance(raw, str):
        raise ValueError("OData continuation integer must be a decimal string")
    pattern = _SIGNED_INTEGER_RE if signed else _UNSIGNED_INTEGER_RE
    if pattern.fullmatch(raw) is None:
        raise ValueError("OData continuation integer is not canonical")
    value = int(raw, 10)
    if signed:
        if value < _INT64_MIN or value > _INT64_MAX:
            raise ValueError("OData continuation int64 is out of range")
    elif value < 0 or value > _UINT64_MAX:
        raise ValueError("OData continuation uint64 is out of range")
    return value


def _duckdb_temporal_parameter(value_type: str, raw: Any) -> object:
    """Validate a Core ISO timestamp and retain all nine fractional digits.

    Python's :class:`datetime` is microsecond-only.  Parsing a Pandas/Arrow
    ``timestamp[ns]`` boundary through it silently truncates the last three
    digits and can skip or repeat an OData page.  Validate calendar/offset
    structure with ``datetime`` but hand DuckDB the original canonical string
    through its explicitly typed values. ``TIMESTAMP_NS`` then compares at
    nanosecond precision; timezone-aware values use DuckDB's native
    ``TIMESTAMPTZ`` lane (whose physical precision is defined by DuckDB).
    """
    if not isinstance(raw, str) or len(raw) > 64:
        raise ValueError("OData continuation temporal value is invalid")
    match = _TEMPORAL_RE.fullmatch(raw)
    if match is None:
        raise ValueError("OData continuation temporal value is invalid")

    fraction = match.group("fraction") or ""
    offset = match.group("offset")
    if value_type == "datetime":
        if offset is None:
            raise ValueError("OData continuation datetime requires an offset")
    elif offset is not None:
        raise ValueError("OData continuation timestamp must not have an offset")

    # Core emits pandas.Timestamp.isoformat().  Values exactly representable
    # in microseconds use six digits; true nanosecond values use nine.  Keeping
    # this canonical form prevents multiple token encodings for one boundary.
    if fraction:
        padded = fraction.ljust(9, "0")
        canonical_length = 9 if int(padded[6:]) else 6
        if len(fraction) != canonical_length:
            raise ValueError(
                "OData continuation temporal value is not canonical"
            )

    normalized_offset = "+00:00" if offset == "Z" else (offset or "")
    microseconds = (fraction + "000000")[:6]
    validation_value = (
        f"{match.group('date')}T{match.group('clock')}"
        f"{('.' + microseconds) if fraction else ''}{normalized_offset}"
    )
    try:
        parsed = datetime.fromisoformat(validation_value)
    except ValueError:
        raise ValueError(
            "OData continuation temporal value is invalid"
        ) from None
    if value_type == "datetime":
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("OData continuation datetime requires an offset")
        # Offsets beyond ISO's valid range are rejected by fromisoformat.  A
        # zero offset may use Core's ``+00:00`` or the established compact Z.
        try:
            return duckdb.TimestampTimeZoneValue(raw)
        except Exception:
            raise ValueError(
                "OData continuation temporal value is invalid"
            ) from None
    if parsed.tzinfo is not None or parsed.utcoffset() is not None:
        raise ValueError("OData continuation timestamp must not have an offset")
    try:
        return duckdb.TimestampNanosecondValue(raw)
    except Exception:
        raise ValueError("OData continuation temporal value is invalid") from None


def _typed_value(raw: Any) -> ODataBoundaryValue:
    if not isinstance(raw, Mapping):
        raise ValueError("OData continuation value must be a mapping")
    value_type = raw.get("type")
    if not isinstance(value_type, str):
        raise ValueError("OData continuation value type is invalid")

    if value_type == "null":
        _exact_keys(raw, {"type"}, "OData continuation null")
        return ODataBoundaryValue(type="null", value=None)

    _exact_keys(raw, {"type", "value"}, "OData continuation value")
    value = raw.get("value")
    converted: Any
    if value_type == "boolean":
        if type(value) is not bool:
            raise ValueError("OData continuation boolean is invalid")
        converted = value
    elif value_type == "int64":
        converted = _canonical_integer(value, signed=True)
    elif value_type == "uint64":
        converted = _canonical_integer(value, signed=False)
    elif value_type == "float64":
        if type(value) is not float:
            raise ValueError("OData continuation float64 is invalid")
        converted = value
        if not math.isfinite(converted):
            raise ValueError("OData continuation float64 must be finite")
    elif value_type == "decimal":
        if (
            not isinstance(value, str)
            or len(value) > _MAX_DECIMAL_CHARS
            or _DECIMAL_RE.fullmatch(value) is None
        ):
            raise ValueError("OData continuation decimal is not canonical")
        digits = value.lstrip("-").replace(".", "")
        if len(digits) > 38:
            raise ValueError("OData continuation decimal exceeds precision")
        try:
            converted = Decimal(value)
        except InvalidOperation:  # defensive; the regex is stricter
            raise ValueError("OData continuation decimal is invalid") from None
        if not converted.is_finite():
            raise ValueError("OData continuation decimal must be finite")
    elif value_type == "string":
        if (
            not isinstance(value, str)
            or len(value.encode("utf-8")) > _MAX_STRING_BYTES
        ):
            raise ValueError("OData continuation string is invalid")
        converted = value
    elif value_type == "binary":
        if not isinstance(value, str) or len(value) > 4 * _MAX_BINARY_BYTES:
            raise ValueError("OData continuation binary is invalid")
        try:
            converted = base64.b64decode(value.encode("ascii"), validate=True)
        except (UnicodeEncodeError, ValueError):
            raise ValueError("OData continuation binary is invalid") from None
        if (
            len(converted) > _MAX_BINARY_BYTES
            or base64.b64encode(converted).decode("ascii") != value
        ):
            raise ValueError("OData continuation binary is not canonical")
    elif value_type == "date":
        if not isinstance(value, str) or len(value) > 10:
            raise ValueError("OData continuation date is invalid")
        try:
            converted = date.fromisoformat(value)
        except ValueError:
            raise ValueError("OData continuation date is invalid") from None
        if converted.isoformat() != value:
            raise ValueError("OData continuation date is not canonical")
    elif value_type == "time":
        if not isinstance(value, str) or len(value) > 32:
            raise ValueError("OData continuation time is invalid")
        try:
            converted = time.fromisoformat(value)
        except ValueError:
            raise ValueError("OData continuation time is invalid") from None
        if converted.tzinfo is not None or converted.isoformat() != value:
            raise ValueError("OData continuation time is not canonical")
    elif value_type in {"datetime", "timestamp"}:
        converted = _duckdb_temporal_parameter(value_type, value)
    elif value_type == "uuid":
        if not isinstance(value, str) or len(value) != 36:
            raise ValueError("OData continuation UUID is invalid")
        try:
            converted = uuid.UUID(value)
        except (ValueError, AttributeError):
            raise ValueError("OData continuation UUID is invalid") from None
        if str(converted) != value:
            raise ValueError("OData continuation UUID is not canonical")
    else:
        raise ValueError("OData continuation value type is unsupported")
    return ODataBoundaryValue(type=value_type, value=converted)


def validate_odata_continuation_boundary(
    raw: object,
) -> Optional[ODataContinuationBoundary]:
    """Parse an exact, bounded Core continuation mapping.

    This validates representation and scalar bounds only.  Binding the order
    metadata to the SQL AST is intentionally a separate mandatory step.
    """
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ValueError("OData continuation boundary must be a mapping")
    _exact_keys(raw, {"version", "order", "row_identity"}, "OData continuation")
    if type(raw.get("version")) is not int or raw["version"] != _BOUNDARY_VERSION:
        raise ValueError("OData continuation version is unsupported")
    raw_order = raw.get("order")
    if not isinstance(raw_order, list) or len(raw_order) > _MAX_ORDER_TERMS:
        raise ValueError("OData continuation order is invalid")
    row_identity = raw.get("row_identity")
    if (
        type(row_identity) is not int
        or row_identity <= 0
        or row_identity > MAX_TABLE_ROWID
    ):
        raise ValueError("OData continuation row identity is invalid")

    try:
        encoded = json.dumps(
            raw,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("OData continuation boundary is not JSON-safe") from None
    if len(encoded) > _MAX_BOUNDARY_BYTES:
        raise ValueError("OData continuation boundary exceeds its size limit")

    order: list[ODataOrderBoundary] = []
    seen: set[str] = set()
    for raw_term in raw_order:
        if not isinstance(raw_term, Mapping):
            raise ValueError("OData continuation order term is invalid")
        _exact_keys(
            raw_term,
            {"column", "direction", "value"},
            "OData continuation order term",
        )
        column = raw_term.get("column")
        if (
            not isinstance(column, str)
            or not column
            or "\x00" in column
            or len(column.encode("utf-8")) > _MAX_COLUMN_BYTES
        ):
            raise ValueError("OData continuation order column is invalid")
        folded = column.casefold()
        if folded in seen or folded == ODATA_INTERNAL_ROWID_COLUMN.casefold():
            raise ValueError("OData continuation order column is duplicated")
        seen.add(folded)
        direction = raw_term.get("direction")
        if direction not in {"asc", "desc"}:
            raise ValueError("OData continuation order direction is invalid")
        order.append(ODataOrderBoundary(
            column=column,
            direction=direction,
            value=_typed_value(raw_term.get("value")),
        ))
    return ODataContinuationBoundary(
        version=_BOUNDARY_VERSION,
        order=tuple(order),
        row_identity=row_identity,
    )


def normalized_odata_order(parsed: exp.Select) -> tuple[tuple[str, str], ...]:
    """Return the exact supported simple-column, NULLS-LAST order tuple."""
    order_node = parsed.args.get("order")
    if order_node is None:
        return ()
    if not isinstance(order_node, exp.Order):
        raise ValueError("OData continuation ORDER BY is invalid")
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for term in order_node.expressions or ():
        if (
            not isinstance(term, exp.Ordered)
            or not isinstance(term.this, exp.Column)
            or term.args.get("with_fill") is not None
            or term.args.get("nulls_first") is True
        ):
            raise ValueError(
                "OData continuation requires simple-column NULLS LAST ordering"
            )
        column = str(term.this.name or "")
        folded = column.casefold()
        if (
            not column
            or folded in seen
            or folded == ODATA_INTERNAL_ROWID_COLUMN.casefold()
        ):
            raise ValueError("OData continuation ORDER BY is ambiguous")
        seen.add(folded)
        result.append((column, "desc" if term.args.get("desc") is True else "asc"))
    if len(result) > _MAX_ORDER_TERMS:
        raise ValueError("OData continuation ORDER BY has too many terms")
    return tuple(result)


def _odata_physical_type_family(type_name: object) -> Optional[str]:
    """Map one pinned schema spelling to the continuation scalar family."""
    if not isinstance(type_name, str) or not type_name.strip():
        return None
    compact = re.sub(r"\s+", "", type_name).casefold()

    if compact in {"bool", "boolean"}:
        return "boolean"
    if compact in {
        "int8", "int16", "int32", "int64", "tinyint", "smallint",
        "integer", "int", "bigint", "byte", "short", "long",
    }:
        return "int64"
    if compact in {
        "uint8", "uint16", "uint32", "uint64", "utinyint", "usmallint",
        "uinteger", "ubigint",
    }:
        return "uint64"
    if compact in {
        "float16", "float32", "float64", "float", "real", "double",
    }:
        return "float64"
    if compact.startswith(("decimal(", "decimal128(", "decimal256(")):
        return "decimal"
    if compact == "decimal":
        return "decimal"
    if compact in {"uuid"}:
        return "uuid"
    if compact in {
        "string", "str", "utf8", "largeutf8", "varchar", "text",
    } or compact.startswith(("varchar(", "char(")):
        return "string"
    if compact in {
        "binary", "largebinary", "blob", "bytes",
    } or compact.startswith(("binary(", "fixedsizebinary(", "fixed_size_binary(")):
        return "binary"

    # Check timestamps before date/time because common spellings begin with
    # ``datetime`` or ``timestamp``. Core intentionally distinguishes naive
    # timestamp values from offset-bearing instants.
    if compact.startswith(("timestamp", "datetime")):
        timezone_aware = any(token in compact for token in (
            "timestamptz",
            "timestampwithtimezone",
            "timestamp_tz",
        ))
        for marker in ("tz=", "timezone="):
            marker_at = compact.find(marker)
            if marker_at < 0:
                continue
            timezone_value = compact[marker_at + len(marker):]
            timezone_value = timezone_value.split(",", 1)[0]
            timezone_value = timezone_value.rstrip("])").strip("'\"")
            if timezone_value not in {"", "none", "null"}:
                timezone_aware = True
        # Pandas commonly renders aware dtypes as datetime64[ns, UTC].
        if compact.startswith("datetime64[") and "," in compact:
            timezone_aware = True
        return "datetime" if timezone_aware else "timestamp"
    if compact in {"date", "date32", "date64"}:
        return "date"
    if compact in {"time"} or compact.startswith(("time32[", "time64[")):
        return "time"
    return None


def _odata_order_type_families(
    parsed: exp.Select,
    column_types: Mapping[str, object],
) -> tuple[tuple[str, str], ...]:
    """Resolve every direct ORDER BY column against one pinned schema."""
    if not isinstance(column_types, Mapping):
        raise ValueError("OData continuation physical schema is unavailable")
    folded_types: dict[str, object] = {}
    for raw_name, raw_type in column_types.items():
        if not isinstance(raw_name, str) or not raw_name:
            raise ValueError("OData continuation physical schema is invalid")
        folded = raw_name.casefold()
        if folded in folded_types:
            raise ValueError("OData continuation physical schema is ambiguous")
        folded_types[folded] = raw_type

    resolved: list[tuple[str, str]] = []
    for column, _direction in normalized_odata_order(parsed):
        family = _odata_physical_type_family(folded_types.get(column.casefold()))
        if family is None:
            raise ValueError(
                "OData continuation ORDER BY physical type is unsupported"
            )
        resolved.append((column, family))
    return tuple(resolved)


def odata_float_order_columns(
    parsed: exp.Select,
    column_types: Mapping[str, object],
) -> tuple[str, ...]:
    """Return float order keys after fail-closed physical-schema binding."""
    return tuple(
        column
        for column, family in _odata_order_type_families(parsed, column_types)
        if family == "float64"
    )


def bind_odata_continuation_boundary(
    parsed: exp.Select,
    boundary: Optional[ODataContinuationBoundary],
    *,
    column_types: Optional[Mapping[str, object]] = None,
) -> Optional[ODataContinuationBoundary]:
    """Require boundary metadata to equal the reparsed SQL order tuple."""
    if boundary is None:
        if column_types is not None:
            _odata_order_type_families(parsed, column_types)
        return None
    if parsed.args.get("offset") is not None:
        raise ValueError("OData continuation cannot be combined with OFFSET")
    if any(isinstance(node, exp.Placeholder) for node in parsed.walk()):
        raise ValueError("OData continuation SQL cannot contain parameters")
    actual = normalized_odata_order(parsed)
    physical_families = (
        dict(_odata_order_type_families(parsed, column_types))
        if column_types is not None else None
    )
    if len(actual) != len(boundary.order):
        raise ValueError("OData continuation order count does not match SQL")
    rebound: list[ODataOrderBoundary] = []
    for (actual_column, actual_direction), supplied in zip(actual, boundary.order):
        if (
            actual_column.casefold() != supplied.column.casefold()
            or actual_direction != supplied.direction
        ):
            raise ValueError("OData continuation order does not match SQL")
        if physical_families is not None:
            expected_type = physical_families[actual_column]
            if supplied.value.type not in {"null", expected_type}:
                raise ValueError(
                    "OData continuation value type does not match ORDER BY"
                )
        # Retain the spelling from the freshly parsed SQL, never from token
        # state, for backend identifier construction.
        rebound.append(ODataOrderBoundary(
            column=actual_column,
            direction=actual_direction,
            value=supplied.value,
        ))
    return ODataContinuationBoundary(
        version=boundary.version,
        order=tuple(rebound),
        row_identity=boundary.row_identity,
    )


__all__ = [
    "ODataBoundaryValue",
    "ODataContinuationBoundary",
    "ODataOrderBoundary",
    "bind_odata_continuation_boundary",
    "normalized_odata_order",
    "odata_float_order_columns",
    "validate_odata_continuation_boundary",
]
