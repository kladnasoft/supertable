"""Deterministic, bounded JSON normalization for quality-check state.

DuckDB LIST/STRUCT cells arrive through pandas as NumPy object arrays whose
nested values are frequently NumPy scalars.  ``json.dumps(default=str)`` turns
the outer array into one opaque string, destroying the D3/D4 structure.  This
module defines the single conversion boundary used before evaluation and
persistence.

The conversion is deliberately lossless for integral values: Python and NumPy
integers remain arbitrary-precision JSON integers.  Finite decimals retain
their exact textual representation instead of passing through binary float.
Non-finite numeric/null sentinels become JSON ``null``.  Ordered containers
remain ordered; tuples and ndarrays become JSON arrays.
"""
from __future__ import annotations

import math
import sys
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Set

try:  # NumPy is present with pandas, but keep configuration imports optional.
    import numpy as _np
except ImportError:  # pragma: no cover - exercised only in minimal installs
    _np = None


DEFAULT_MAX_DEPTH = 32
DEFAULT_MAX_NODES = 250_000
# D3/D4 previews are bounded to a small number of values upstream.  64 KiB
# still permits unusually wide textual values without allowing one scalar to
# dominate a Redis document.  The 16 MiB document cap accommodates legitimate
# wide-table latest results while keeping every eventual json.dumps allocation
# and Redis argument bounded.
DEFAULT_MAX_SCALAR_BYTES = 64 * 1024
DEFAULT_MAX_TOTAL_BYTES = 16 * 1024 * 1024


class JSONNormalizationError(ValueError):
    """A value cannot be represented within the quality JSON contract."""


def _is_pandas_null_sentinel(value: object) -> bool:
    """Match exact pandas sentinel classes without trusting writable names."""

    value_type = type(value)
    for module_name, class_name in (
        ("pandas._libs.missing", "NAType"),
        ("pandas._libs.tslibs.nattype", "NaTType"),
    ):
        module = sys.modules.get(module_name)
        published_type = (
            vars(module).get(class_name) if module is not None else None
        )
        if value_type is published_type:
            return True
    return False


def normalize_json_value(
    value: Any,
    *,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_nodes: int = DEFAULT_MAX_NODES,
    max_scalar_bytes: int = DEFAULT_MAX_SCALAR_BYTES,
    max_total_bytes: int = DEFAULT_MAX_TOTAL_BYTES,
) -> Any:
    """Return a bounded tree containing only strict JSON value types.

    ``max_depth``, ``max_nodes`` and the byte limits bound hostile or
    accidentally enormous object graphs before they reach Redis/history
    serialization.  ``max_scalar_bytes`` measures the raw UTF-8 bytes of each
    textual scalar; ``max_total_bytes`` conservatively accounts for the compact
    UTF-8 JSON representation, including escaping and container punctuation.
    Cycles, binary values, and unsupported scalar types fail closed rather than
    being stringified. Dictionary keys must normalize to strings, matching
    DuckDB STRUCT field names and avoiding JSON key collisions such as ``1``
    versus ``"1"``.
    """

    if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
        raise ValueError("max_depth must be a non-negative integer")
    if isinstance(max_nodes, bool) or not isinstance(max_nodes, int) or max_nodes < 1:
        raise ValueError("max_nodes must be a positive integer")
    if (
        isinstance(max_scalar_bytes, bool)
        or not isinstance(max_scalar_bytes, int)
        or max_scalar_bytes < 1
    ):
        raise ValueError("max_scalar_bytes must be a positive integer")
    if (
        isinstance(max_total_bytes, bool)
        or not isinstance(max_total_bytes, int)
        or max_total_bytes < 1
    ):
        raise ValueError("max_total_bytes must be a positive integer")

    nodes = 0
    total_bytes = 0
    active: Set[int] = set()

    def consume(depth: int) -> None:
        nonlocal nodes
        if depth > max_depth:
            raise JSONNormalizationError(
                f"quality JSON exceeds maximum depth {max_depth}"
            )
        nodes += 1
        if nodes > max_nodes:
            raise JSONNormalizationError(
                f"quality JSON exceeds maximum node count {max_nodes}"
            )

    def charge(byte_count: int) -> None:
        """Account before materializing the final JSON string."""

        nonlocal total_bytes
        total_bytes += byte_count
        if total_bytes > max_total_bytes:
            raise JSONNormalizationError(
                "quality JSON exceeds maximum total UTF-8 byte count "
                f"{max_total_bytes}"
            )

    def scalar_size(byte_count: int, scalar_type: str) -> None:
        if byte_count > max_scalar_bytes:
            raise JSONNormalizationError(
                f"quality JSON {scalar_type} exceeds maximum scalar UTF-8 "
                f"byte count {max_scalar_bytes}"
            )

    def normalize_string(text: str) -> str:
        # UTF-8 needs at least one byte per Unicode code point, so reject huge
        # ASCII/common strings in O(1) without allocating an encoded copy.
        if len(text) > max_scalar_bytes:
            scalar_size(len(text), "string")

        raw_bytes = 0
        json_bytes = 2  # surrounding quotation marks
        for character in text:
            codepoint = ord(character)
            if codepoint <= 0x7F:
                width = 1
            elif codepoint <= 0x7FF:
                width = 2
            elif 0xD800 <= codepoint <= 0xDFFF:
                raise JSONNormalizationError(
                    "quality JSON string contains an invalid UTF-8 surrogate"
                )
            elif codepoint <= 0xFFFF:
                width = 3
            else:
                width = 4
            raw_bytes += width
            if raw_bytes > max_scalar_bytes:
                scalar_size(raw_bytes, "string")

            # Match json.dumps(..., ensure_ascii=False, separators=(',', ':')).
            if character in {'"', "\\"}:
                json_bytes += 2
            elif codepoint < 0x20:
                json_bytes += 2 if character in "\b\f\n\r\t" else 6
            else:
                json_bytes += width

        charge(json_bytes)
        return text

    def integer_json_size(number: int) -> int:
        """Conservative decimal width without constructing a huge string."""

        if number == 0:
            return 1
        magnitude = -number if number < 0 else number
        # log10(2) < 0.30103, so this is an upper bound (at most one byte
        # larger than the exact decimal representation).
        digits = (magnitude.bit_length() * 30103) // 100000 + 1
        return digits + (1 if number < 0 else 0)

    def walk(current: Any, depth: int) -> Any:
        consume(depth)

        if current is None:
            charge(4)
            return None

        # pandas missing-value sentinels cannot safely participate in boolean
        # checks.  Detect them without taking a hard pandas dependency here.
        if _is_pandas_null_sentinel(current):
            charge(4)
            return None

        if isinstance(current, bool):
            charge(4 if current else 5)
            return current
        if isinstance(current, int):
            # Python JSON writes arbitrary-precision integers without first
            # passing through float, so values beyond 2**53 remain exact.
            byte_count = integer_json_size(current)
            scalar_size(byte_count, "integer")
            charge(byte_count)
            return current
        if isinstance(current, float):
            if not math.isfinite(current):
                charge(4)
                return None
            byte_count = len(repr(current))
            scalar_size(byte_count, "float")
            charge(byte_count)
            return current
        if isinstance(current, Decimal):
            # The pre-existing DQ JSON contract represented Decimal as text.
            # Keep its coefficient, exponent and trailing zeros exactly.
            if not current.is_finite():
                charge(4)
                return None
            return normalize_string(str(current))
        if isinstance(current, str):
            return normalize_string(current)

        if isinstance(current, (bytes, bytearray, memoryview)):
            # No implicit UTF-8/base64/hex conversion: each changes the value's
            # logical type and recreates the old default=str corruption under
            # a different spelling.
            raise JSONNormalizationError(
                "binary values are not supported by the quality JSON contract"
            )

        if isinstance(current, datetime):
            normalized = current
            try:
                if current.utcoffset() is not None:
                    normalized = current.astimezone(timezone.utc)
            except (OverflowError, ValueError):
                # An invalid temporal scalar is data corruption, not a value
                # that should silently acquire an implementation-specific str.
                raise JSONNormalizationError(
                    f"invalid datetime value {current!r}"
                ) from None
            return normalize_string(normalized.isoformat())
        if isinstance(current, date):
            return normalize_string(current.isoformat())

        if _np is not None and isinstance(current, _np.ndarray):
            identity = id(current)
            if identity in active:
                raise JSONNormalizationError("quality JSON contains a cycle")
            if int(current.ndim) + depth > max_depth:
                raise JSONNormalizationError(
                    f"quality JSON exceeds maximum depth {max_depth}"
                )
            # Check before tolist(), which otherwise materializes the entire
            # array before the ordinary list traversal can enforce its bound.
            if int(current.size) > max_nodes - nodes:
                raise JSONNormalizationError(
                    f"quality JSON exceeds maximum node count {max_nodes}"
                )
            active.add(identity)
            try:
                return walk(current.tolist(), depth + 1)
            finally:
                active.remove(identity)

        if isinstance(current, (list, tuple)):
            identity = id(current)
            if identity in active:
                raise JSONNormalizationError("quality JSON contains a cycle")
            if len(current) > max_nodes - nodes:
                raise JSONNormalizationError(
                    f"quality JSON exceeds maximum node count {max_nodes}"
                )
            active.add(identity)
            try:
                charge(2 + max(0, len(current) - 1))
                return [walk(item, depth + 1) for item in current]
            finally:
                active.remove(identity)

        if isinstance(current, dict):
            identity = id(current)
            if identity in active:
                raise JSONNormalizationError("quality JSON contains a cycle")
            # Every entry consumes at least a key and a value node.
            if len(current) * 2 > max_nodes - nodes:
                raise JSONNormalizationError(
                    f"quality JSON exceeds maximum node count {max_nodes}"
                )
            active.add(identity)
            try:
                # Braces, one colon per entry and commas between entries.
                charge(2 + len(current) + max(0, len(current) - 1))
                result: Dict[str, Any] = {}
                for raw_key, raw_value in current.items():
                    key = walk(raw_key, depth + 1)
                    if not isinstance(key, str):
                        raise JSONNormalizationError(
                            "quality JSON object keys must be strings"
                        )
                    if key in result:
                        raise JSONNormalizationError(
                            f"quality JSON contains duplicate key {key!r}"
                        )
                    result[key] = walk(raw_value, depth + 1)
                return result
            finally:
                active.remove(identity)

        # NumPy scalar types (including datetime64) convert losslessly through
        # item().  Guard against arbitrary objects exposing a similarly named
        # method by requiring NumPy's scalar base class.
        if _np is not None and isinstance(current, _np.generic):
            converted = current.item()
            if converted is current:
                raise JSONNormalizationError(
                    "unsupported quality JSON scalar value"
                )
            return walk(converted, depth)

        raise JSONNormalizationError(
            "unsupported quality JSON value"
        )

    return walk(value, 0)


__all__ = [
    "DEFAULT_MAX_DEPTH",
    "DEFAULT_MAX_NODES",
    "DEFAULT_MAX_SCALAR_BYTES",
    "DEFAULT_MAX_TOTAL_BYTES",
    "JSONNormalizationError",
    "normalize_json_value",
]
