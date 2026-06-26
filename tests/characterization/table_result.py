"""Canonical, engine-agnostic representation of a table read result.

A :class:`TableResult` is the unit of comparison for the golden suite.  It holds
the *logical* result of a read: ordered column names, an inferred logical type
per column, and normalised row values (timestamps as ISO-8601 UTC strings, all
null variants collapsed to ``None``, decimals as exact strings).  The
normalisation is deliberately lossless for the types SuperTable actually returns
and deterministic so a sha256 of the canonical JSON can seal the result.

Public (external) columns only: the internal ``__timestamp__`` / ``__rn__`` /
future ``__rowid`` columns are never part of a normal read output and therefore
never appear here.
"""

from __future__ import annotations

import base64
import datetime as _dt
import json
import math
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, List, Optional, Sequence

# Logical type tags (engine-agnostic).
T_INT = "int"
T_FLOAT = "float"
T_BOOL = "bool"
T_STR = "str"
T_DATE = "date"
T_TIMESTAMP = "timestamp"
T_DECIMAL = "decimal"
T_BYTES = "bytes"
T_NULL = "null"  # column with only nulls / empty


def _is_nan(v: Any) -> bool:
    return isinstance(v, float) and math.isnan(v)


def _normalize_value(v: Any) -> Any:
    """Collapse null variants to None; render temporal/decimal/bytes canonically.

    Timestamps are normalised to UTC and serialised with microsecond precision
    and a trailing ``Z``.  Naive datetimes are treated as already-UTC (SuperTable
    stores ``__timestamp__`` as UTC) and tagged the same way so tz/no-tz inputs
    that denote the same instant compare equal.
    """
    if v is None:
        return None
    # pandas NA / NaT without importing pandas hard-dependency at value level
    tname = type(v).__name__
    if tname in ("NaTType", "NAType"):
        return None
    if _is_nan(v):
        return None

    if isinstance(v, bool):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return v
    if isinstance(v, Decimal):
        return f"DEC:{format(v, 'f')}"
    if isinstance(v, (bytes, bytearray, memoryview)):
        return "B64:" + base64.b64encode(bytes(v)).decode("ascii")
    if isinstance(v, _dt.datetime):
        if v.tzinfo is not None:
            v = v.astimezone(_dt.timezone.utc).replace(tzinfo=None)
        return "TS:" + v.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
    if isinstance(v, _dt.date):
        return "D:" + v.isoformat()
    # numpy scalars expose .item()
    item = getattr(v, "item", None)
    if callable(item):
        try:
            return _normalize_value(item())
        except Exception:
            pass
    return str(v)


def _logical_type(values: Sequence[Any]) -> str:
    """Infer a single logical type tag from a column's normalised values."""
    seen = set()
    for v in values:
        if v is None:
            continue
        if isinstance(v, bool):
            seen.add(T_BOOL)
        elif isinstance(v, int):
            seen.add(T_INT)
        elif isinstance(v, float):
            seen.add(T_FLOAT)
        elif isinstance(v, str):
            if v.startswith("TS:"):
                seen.add(T_TIMESTAMP)
            elif v.startswith("D:"):
                seen.add(T_DATE)
            elif v.startswith("DEC:"):
                seen.add(T_DECIMAL)
            elif v.startswith("B64:"):
                seen.add(T_BYTES)
            else:
                seen.add(T_STR)
        else:
            seen.add(T_STR)
    if not seen:
        return T_NULL
    if seen == {T_INT}:
        return T_INT
    if seen == {T_FLOAT} or seen == {T_INT, T_FLOAT}:
        return T_FLOAT
    if seen == {T_BOOL}:
        return T_BOOL
    if seen == {T_TIMESTAMP}:
        return T_TIMESTAMP
    if seen == {T_DATE}:
        return T_DATE
    if seen == {T_DECIMAL}:
        return T_DECIMAL
    if seen == {T_BYTES}:
        return T_BYTES
    return T_STR  # mixed / textual


@dataclass
class TableResult:
    columns: List[str]
    column_types: List[str]
    rows: List[List[Any]]  # values already normalised via _normalize_value

    @property
    def row_count(self) -> int:
        return len(self.rows)

    # ---- construction -------------------------------------------------

    @classmethod
    def from_columns_rows(
        cls, columns: Sequence[str], rows: Sequence[Sequence[Any]],
        column_types: Optional[Sequence[str]] = None,
    ) -> "TableResult":
        cols = list(columns)
        norm_rows = [[_normalize_value(v) for v in row] for row in rows]
        if column_types is None:
            column_types = [
                _logical_type([r[i] for r in norm_rows]) for i in range(len(cols))
            ]
        return cls(columns=cols, column_types=list(column_types), rows=norm_rows)

    # ---- canonical serialization -------------------------------------

    def to_canonical_dict(self) -> dict:
        return {
            "columns": self.columns,
            "column_types": self.column_types,
            "row_count": self.row_count,
            "rows": self.rows,
        }

    def to_json_bytes(self) -> bytes:
        return json.dumps(
            self.to_canonical_dict(),
            ensure_ascii=False,
            sort_keys=False,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def from_canonical_dict(cls, d: dict) -> "TableResult":
        return cls(
            columns=list(d["columns"]),
            column_types=list(d["column_types"]),
            rows=[list(r) for r in d["rows"]],
        )

    # ---- golden directory I/O ----------------------------------------

    RESULT_JSON = "result.json"

    def write_golden(self, golden_dir: str | Path) -> None:
        d = Path(golden_dir)
        d.mkdir(parents=True, exist_ok=True)
        (d / self.RESULT_JSON).write_bytes(
            json.dumps(self.to_canonical_dict(), ensure_ascii=False, indent=2).encode("utf-8")
        )

    @classmethod
    def load_golden(cls, golden_dir: str | Path) -> "TableResult":
        d = Path(golden_dir)
        with open(d / cls.RESULT_JSON, "rb") as fh:
            return cls.from_canonical_dict(json.load(fh))

    def canonical_sha256(self) -> str:
        import hashlib

        return hashlib.sha256(self.to_json_bytes()).hexdigest()
