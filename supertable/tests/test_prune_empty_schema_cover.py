"""When pruning proves nothing matches, hand back a schema, not the table.

``prune_files_by_predicates`` cannot return an empty list: the reflection scan
is built with ``parquet_scan([...])`` and ``create_reflection_view`` raises on
an empty one. The old remedy was to hand back *every* file, so a query the
pruner had proven returns nothing opened the whole table. Measured on the
generated corpus: 33 of 595 time-predicate queries, 792 files, 18.6% of their
wall time, to produce zero rows.

It now hands back the smallest subset exposing the same columns. The query
keeps its own WHERE clause, so those files yield no rows by themselves — the
only thing they are needed for is binding column names.

Why not simply the first file: the scan runs ``union_by_name=TRUE`` because a
lake's files can carry different schemas after an evolution. A query
projecting a column only the newer files have would fail to bind against an
arbitrary single file. That case has its own test below.
"""
from __future__ import annotations

import polars
import pytest

from supertable.processing import (
    _schema_covering_subset,
    prune_files_by_predicates,
)


def _stats(spec: dict[str, list[str]]) -> polars.DataFrame:
    """Build a stats frame from ``{file_path: [column, ...]}``."""
    rows = [{"file_path": fp, "column_name": col}
            for fp, cols in spec.items() for col in cols]
    return polars.DataFrame(rows)


# --------------------------------------------------------------------------
# The cover itself
# --------------------------------------------------------------------------

def test_uniform_schema_collapses_to_one_file():
    """The overwhelmingly common case: every file has the same columns."""
    files = [f"f{i}.parquet" for i in range(24)]
    stats = _stats({f: ["id", "ts", "amount"] for f in files})

    assert _schema_covering_subset(files, stats) == ["f0.parquet"]


def test_schema_evolution_keeps_a_file_carrying_the_new_column():
    """Exactly why ``file_keys[:1]`` would have been wrong.

    ``extra`` exists only in the last file. Returning the first alone would
    leave a query projecting ``extra`` unable to bind.
    """
    files = ["old.parquet", "mid.parquet", "new.parquet"]
    stats = _stats({
        "old.parquet": ["id", "ts"],
        "mid.parquet": ["id", "ts"],
        "new.parquet": ["id", "ts", "extra"],
    })

    out = _schema_covering_subset(files, stats)

    assert "new.parquet" in out, "the only file with 'extra' must survive"
    covered = set().union(*[set(stats.filter(polars.col("file_path") == f)
                                ["column_name"].to_list()) for f in out])
    assert covered == {"id", "ts", "extra"}


def test_disjoint_schemas_need_more_than_one_file():
    files = ["a.parquet", "b.parquet"]
    stats = _stats({"a.parquet": ["x"], "b.parquet": ["y"]})

    assert sorted(_schema_covering_subset(files, stats)) == files


def test_the_cover_is_a_subset_in_caller_order():
    """Downstream matches these against stats and resolved paths.

    A reordered or invented path would mismatch, and an unstable order would
    churn plans and seals for no reason.
    """
    files = ["z.parquet", "a.parquet", "m.parquet"]
    stats = _stats({"z.parquet": ["x"], "a.parquet": ["y"], "m.parquet": ["z"]})

    out = _schema_covering_subset(files, stats)

    assert out == [f for f in files if f in out], "must follow caller order"
    assert set(out) <= set(files), "must be a subset, never a new path"


def test_it_is_deterministic_across_calls():
    files = [f"f{i}.parquet" for i in range(8)]
    stats = _stats({f: ["id", "ts"] for f in files})

    assert len({tuple(_schema_covering_subset(files, stats))
                for _ in range(20)}) == 1


# --------------------------------------------------------------------------
# Falling back to the full list rather than guessing
# --------------------------------------------------------------------------

@pytest.mark.parametrize("stats", [None, polars.DataFrame()])
def test_no_stats_falls_back_to_everything(stats):
    files = ["a.parquet", "b.parquet"]
    assert _schema_covering_subset(files, stats) == files


def test_a_file_missing_from_the_stats_falls_back_to_everything():
    """Such a file could be hiding any column, so guessing is not allowed."""
    files = ["known.parquet", "unknown.parquet"]
    stats = _stats({"known.parquet": ["id", "ts"]})

    assert _schema_covering_subset(files, stats) == files


def test_a_stats_frame_with_no_columns_falls_back():
    files = ["a.parquet"]
    stats = polars.DataFrame({"file_path": [], "column_name": []},
                             schema={"file_path": polars.Utf8,
                                     "column_name": polars.Utf8})
    assert _schema_covering_subset(files, stats) == files


def test_a_malformed_stats_frame_falls_back_rather_than_raising():
    """Pruning is an optimisation; it must never break the query."""
    files = ["a.parquet"]
    assert _schema_covering_subset(files, polars.DataFrame({"nope": [1]})) == files


# --------------------------------------------------------------------------
# End to end through the pruner
# --------------------------------------------------------------------------

class _Pred:
    def __init__(self, lane, lo=None, hi=None):
        self.lane, self.lo, self.hi = lane, lo, hi
        self.lo_incl = self.hi_incl = True


def _full_stats(spec, lo, hi):
    """Stats with real min/max so the predicate can actually exclude.

    The lane fields are the ones ``_stored_lane`` reads — ``stats_available``
    plus a populated ``min_*``/``max_*`` pair. A row missing them yields no
    range, which means "cannot exclude", so a test built on the wrong shape
    would silently assert nothing.
    """
    rows = []
    for fp, cols in spec.items():
        for col in cols:
            rows.append({
                "file_path": fp, "row_group_id": 0, "column_name": col,
                "stats_available": True,
                "min_bigint": int(lo), "max_bigint": int(hi),
            })
    return polars.DataFrame(rows)


def test_pruning_everything_returns_a_cover_not_the_whole_table():
    files = [f"f{i}.parquet" for i in range(24)]
    stats = _full_stats({f: ["id"] for f in files}, 0, 10)
    # Every file holds id in [0, 10]; ask for id >= 1000.
    occ = [{"id": _Pred("numeric", lo=1000)}]

    out = prune_files_by_predicates(files, stats, occ)

    assert out, "never empty — the reflection scan cannot be built from one"
    assert len(out) < len(files), (
        f"proved nothing matches yet handed back {len(out)}/{len(files)} files"
    )
    assert set(out) <= set(files)


def test_a_normal_partial_prune_is_untouched():
    """The guard must not alter the ordinary path."""
    files = ["lo.parquet", "hi.parquet"]
    stats = polars.DataFrame([
        {"file_path": "lo.parquet", "row_group_id": 0, "column_name": "id",
         "stats_available": True, "min_bigint": 0, "max_bigint": 10},
        {"file_path": "hi.parquet", "row_group_id": 0, "column_name": "id",
         "stats_available": True, "min_bigint": 1000, "max_bigint": 2000},
    ])
    occ = [{"id": _Pred("numeric", lo=1500)}]

    assert prune_files_by_predicates(files, stats, occ) == ["hi.parquet"]
