"""Canonical comparison of an actual :class:`TableResult` against a sealed golden.

Design goals (see the characterization brief):

* Schema and row-count are compared **separately** from row contents so a single
  structural drift does not drown the report in spurious per-row diffs.
* Nulls are preserved and compared explicitly (``None`` only equals ``None``).
* Output order is **not** assumed: rows are compared as a multiset unless the
  scenario explicitly seals an order (``ordered=True`` for ``ORDER BY`` queries).
* Duplicates are never collapsed — a multiset mismatch in either direction is
  reported with exact counts (we never ``DISTINCT`` away a duplicate).
* Exact comparison for int / str / bool / decimal / date / timestamp; float
  comparison applies a tolerance **only** for columns whose logical type is
  ``float``.
"""

from __future__ import annotations

import math
from collections import Counter
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

from tests.characterization.table_result import TableResult, T_FLOAT

FLOAT_REL_TOL = 1e-9
FLOAT_ABS_TOL = 1e-12


def _align_to(actual: TableResult, golden: TableResult) -> Optional[List[int]]:
    """If both have the same column *set*, return indices that reorder actual's
    columns into golden's order; else None (cannot align)."""
    if set(actual.columns) != set(golden.columns):
        return None
    pos = {c: i for i, c in enumerate(actual.columns)}
    return [pos[c] for c in golden.columns]


def _row_key(row: Sequence[Any], float_cols: Sequence[bool]) -> Tuple:
    """Hashable key for multiset bucketing.  Float cells are excluded from the
    key (rounded coarsely) so tolerance matching can run within a bucket."""
    key = []
    for i, v in enumerate(row):
        if float_cols[i] and isinstance(v, float):
            key.append(("~f", round(v, 6)))
        else:
            key.append(("v", v))
    return tuple(key)


def _rows_match(a: Sequence[Any], b: Sequence[Any], float_cols: Sequence[bool]) -> bool:
    for i, (x, y) in enumerate(zip(a, b)):
        if float_cols[i] and isinstance(x, float) and isinstance(y, float):
            if not math.isclose(x, y, rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL):
                return False
        else:
            if x != y:
                return False
    return True


def _fmt_row(columns: Sequence[str], row: Sequence[Any]) -> str:
    return "{" + ", ".join(f"{c}={v!r}" for c, v in zip(columns, row)) + "}"


def diff_table_results(
    actual: TableResult,
    golden: TableResult,
    *,
    ordered: bool = False,
) -> List[str]:
    """Return a list of human-readable difference strings (empty == match)."""
    diffs: List[str] = []

    # ---- 1. columns & column order ----
    if actual.columns != golden.columns:
        if set(actual.columns) == set(golden.columns):
            diffs.append(
                f"column-order difference: actual={actual.columns} "
                f"golden={golden.columns}"
            )
        else:
            missing_cols = [c for c in golden.columns if c not in actual.columns]
            extra_cols = [c for c in actual.columns if c not in golden.columns]
            if missing_cols:
                diffs.append(f"missing columns (in golden, absent from actual): {missing_cols}")
            if extra_cols:
                diffs.append(f"unexpected columns (in actual, absent from golden): {extra_cols}")

    # ---- 2. column types (and nullability via T_NULL) for shared columns ----
    gtypes = dict(zip(golden.columns, golden.column_types))
    atypes = dict(zip(actual.columns, actual.column_types))
    for c in golden.columns:
        if c in atypes and atypes[c] != gtypes[c]:
            diffs.append(
                f"type difference for column {c!r}: actual={atypes[c]} golden={gtypes[c]}"
            )

    # ---- 3. row count ----
    if actual.row_count != golden.row_count:
        diffs.append(
            f"row-count difference: actual={actual.row_count} golden={golden.row_count}"
        )

    # If columns cannot be aligned, row-level comparison is meaningless.
    align = _align_to(actual, golden)
    if align is None:
        return diffs

    a_rows = [[r[i] for i in align] for r in actual.rows]
    g_rows = [list(r) for r in golden.rows]
    cols = golden.columns
    float_cols = [t == T_FLOAT for t in golden.column_types]

    # ---- 4. row contents ----
    if ordered:
        for idx, (ar, gr) in enumerate(zip(a_rows, g_rows)):
            if not _rows_match(ar, gr, float_cols):
                diffs.append(
                    f"ordered row {idx} differs:\n    actual={_fmt_row(cols, ar)}\n"
                    f"    golden={_fmt_row(cols, gr)}"
                )
        return diffs

    if any(float_cols):
        diffs.extend(_diff_multiset_with_tolerance(a_rows, g_rows, cols, float_cols))
    else:
        diffs.extend(_diff_multiset_exact(a_rows, g_rows, cols))

    return diffs


def _diff_multiset_exact(a_rows, g_rows, cols) -> List[str]:
    diffs: List[str] = []
    ac = Counter(tuple(r) for r in a_rows)
    gc = Counter(tuple(r) for r in g_rows)
    for row, gn in gc.items():
        an = ac.get(row, 0)
        if an < gn:
            if an == 0:
                diffs.append(f"missing row (expected, not produced): {_fmt_row(cols, row)}")
            else:
                diffs.append(
                    f"duplicate-count difference for {_fmt_row(cols, row)}: "
                    f"actual={an} golden={gn}"
                )
    for row, an in ac.items():
        gn = gc.get(row, 0)
        if an > gn:
            if gn == 0:
                diffs.append(f"unexpected row (produced, not in golden): {_fmt_row(cols, row)}")
            else:
                diffs.append(
                    f"duplicate-count difference for {_fmt_row(cols, row)}: "
                    f"actual={an} golden={gn}"
                )
    return diffs


def _diff_multiset_with_tolerance(a_rows, g_rows, cols, float_cols) -> List[str]:
    """Greedy tolerance-aware multiset matching for results containing floats."""
    diffs: List[str] = []
    remaining = list(a_rows)
    unmatched_golden = []
    for gr in g_rows:
        hit = None
        for j, ar in enumerate(remaining):
            if _rows_match(ar, gr, float_cols):
                hit = j
                break
        if hit is None:
            unmatched_golden.append(gr)
        else:
            remaining.pop(hit)
    for gr in unmatched_golden:
        diffs.append(f"missing row (expected, not produced): {_fmt_row(cols, gr)}")
    for ar in remaining:
        diffs.append(f"unexpected row (produced, not in golden): {_fmt_row(cols, ar)}")
    return diffs


def assert_table_result_matches_golden(
    actual: TableResult,
    golden_directory: str | Path,
    *,
    ordered: bool = False,
) -> None:
    """Assert ``actual`` matches the golden sealed in ``golden_directory``.

    Raises ``AssertionError`` with a structured, multi-section report listing
    every difference (schema, types, row count, missing / unexpected / duplicate
    rows).  Never silently passes on a mismatch and never regenerates the golden.
    """
    golden_directory = Path(golden_directory)
    result_json = golden_directory / TableResult.RESULT_JSON
    if not result_json.exists():
        raise AssertionError(
            f"golden not found: {result_json}\n"
            f"Seal it first with:  python -m tests.generate_current_behavior_golden\n"
            f"(or: pytest --create-golden)"
        )
    golden = TableResult.load_golden(golden_directory)
    diffs = diff_table_results(actual, golden, ordered=ordered)
    if diffs:
        report = "\n".join(f"  - {d}" for d in diffs)
        raise AssertionError(
            f"TableResult does not match sealed golden at {golden_directory}\n"
            f"{len(diffs)} difference(s):\n{report}\n\n"
            f"If this change is INTENTIONAL, reseal deliberately with "
            f"`pytest --create-golden` and review the golden diff."
        )
