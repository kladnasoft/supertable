from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
import math


def value_equal(left, right):
    if left is None or right is None:
        return left is right
    if isinstance(left, bool) or isinstance(right, bool):
        return type(left) is type(right) and left == right
    if isinstance(left, (int, float, Decimal)) and isinstance(right, (int, float, Decimal)):
        if isinstance(left, float):
            return math.isclose(float(left), float(right), rel_tol=1e-9, abs_tol=1e-9)
        return left == right
    if isinstance(left, datetime) and isinstance(right, datetime):
        if left.tzinfo is not None:
            left = left.astimezone(timezone.utc).replace(tzinfo=None)
        if right.tzinfo is not None:
            right = right.astimezone(timezone.utc).replace(tzinfo=None)
    if isinstance(left, (list, tuple)) and isinstance(right, (list, tuple)):
        return len(left) == len(right) and all(value_equal(a, b) for a, b in zip(left, right))
    return type(left) is type(right) and left == right


def compare_result(case, columns, rows, types=None):
    if list(columns) != list(case.columns):
        return {"kind": "columns", "expected_columns": case.columns, "actual_columns": columns}
    if len(rows) != len(case.expected):
        return {"kind": "row_count", "expected_count": len(case.expected), "actual_count": len(rows)}
    if case.expected_types and types is not None:
        mismatch = {col: {"expected": dtype, "actual": types.get(col)}
                    for col, dtype in case.expected_types.items() if types.get(col) != dtype}
        if mismatch:
            return {"kind": "types", "mismatch": mismatch}
    if case.ordered:
        for index, (expected, actual) in enumerate(zip(case.expected, rows)):
            if not value_equal(expected, actual):
                return {"kind": "values", "row_index": index, "expected_row": expected, "actual_row": actual}
    else:
        unmatched = list(rows)
        for expected in case.expected:
            found = next((i for i, actual in enumerate(unmatched) if value_equal(expected, actual)), None)
            if found is None:
                return {"kind": "values", "missing_row": expected, "unmatched_actual_sample": unmatched[:5]}
            unmatched.pop(found)
    return None


def json_default(value):
    if isinstance(value, (datetime, date)):
        return {"type": type(value).__name__, "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"type": "decimal", "value": str(value)}
    if isinstance(value, set):
        return sorted(value)
    raise TypeError(f"Cannot serialize {type(value).__name__}")
