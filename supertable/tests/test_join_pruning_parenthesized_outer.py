"""Regression tests for preserved parenthesized RIGHT-join operands.

An unaliased ``(b JOIN c ...)`` is represented by sqlglot as one Subquery
operand but its physical aliases remain in the surrounding scope.  Directional
join pruning must classify both aliases as the JOIN node's own side; otherwise
a RIGHT / RIGHT ANTI join can prune the preserved RHS and lose result rows.
"""

from __future__ import annotations

from typing import Dict, List, Tuple

import polars
import pytest

from supertable.engine.join_pruner import plan_file_pruning_for_query
from supertable.processing import STATS_SCHEMA
from supertable.utils.sql_parser import SQLParser


SUPER = "s"
A = (SUPER, "a")
B = (SUPER, "b")
C = (SUPER, "c")


def _query(join_sql: str, rhs_alias: str = "b") -> str:
    return f"""
        SELECT b.k
        FROM s.a a
        {join_sql} (s.b b JOIN s.c c ON b.z = c.z)
          ON a.k = {rhs_alias}.k
    """


def _rows_for_file(
    file_path: str,
    colspecs: Dict[str, Tuple[int, int]],
) -> List[dict]:
    rows: List[dict] = []
    for column, (minimum, maximum) in colspecs.items():
        row = {name: None for name in STATS_SCHEMA}
        row.update({
            "file_path": file_path,
            "row_group_id": 0,
            "column_name": column,
            "physical_type": "INT64",
            "logical_type": "",
            "null_count": 0,
            "row_group_rows": 100,
            "compressed_bytes": 1000,
            "min_is_exact": True,
            "max_is_exact": True,
            "stats_available": True,
            "min_bigint": minimum,
            "max_bigint": maximum,
        })
        rows.append(row)
    return rows


def _stats(rows: List[dict]) -> polars.DataFrame:
    return polars.DataFrame(rows, schema=STATS_SCHEMA)


@pytest.mark.parametrize("join_sql", ["RIGHT JOIN", "RIGHT ANTI JOIN"])
@pytest.mark.parametrize("rhs_alias", ["b", "c"])
def test_parenthesized_preserved_rhs_endpoint_is_not_prunable(
    join_sql: str,
    rhs_alias: str,
):
    """Every physical alias below the grouped RHS has preserved semantics."""
    (edge,) = SQLParser(
        SUPER,
        _query(join_sql, rhs_alias),
        "duckdb",
    ).get_join_edges()

    prunable = {
        edge.left_table: edge.prune_left,
        edge.right_table: edge.prune_right,
    }
    assert prunable[A] is True
    assert prunable[(SUPER, rhs_alias)] is False


@pytest.mark.parametrize("join_sql", ["RIGHT JOIN", "RIGHT ANTI JOIN"])
def test_parenthesized_preserved_rhs_contributing_file_is_kept(join_sql: str):
    """The unmatched B file contributes to RIGHT/RIGHT ANTI and must survive."""
    table_files = {
        A: ["a/matching.parquet"],
        B: ["b/matching.parquet", "b/preserved-unmatched.parquet"],
        C: ["c/all.parquet"],
    }
    table_stats = {
        A: _stats(_rows_for_file("a/matching.parquet", {"k": (0, 9)})),
        B: _stats(
            _rows_for_file(
                "b/matching.parquet",
                {"k": (0, 9), "z": (1, 1)},
            )
            + _rows_for_file(
                "b/preserved-unmatched.parquet",
                {"k": (100, 109), "z": (1, 1)},
            )
        ),
        C: _stats(_rows_for_file("c/all.parquet", {"z": (1, 1)})),
    }

    plan = plan_file_pruning_for_query(
        SUPER,
        _query(join_sql),
        "duckdb",
        table_files,
        table_stats,
        allow_empty=False,
    )

    # RIGHT JOIN emits this file as a null-extended B row; RIGHT ANTI emits it
    # precisely because no A key matches.  The old one-alias ownership logic
    # dropped it while retaining b/matching.parquet, bypassing the non-empty
    # guard and losing the contributing row.
    assert plan.survivors[B] == table_files[B]
