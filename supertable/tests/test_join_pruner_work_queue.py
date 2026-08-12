"""Regressions for join-pruner fixpoint scheduling and profile hot paths."""

from __future__ import annotations

import random
from collections import Counter, deque

import pytest

from supertable.data_classes import JoinEdge, PredInterval
from supertable.engine import join_pruner
from supertable.engine.join_pruner import (
    _derive_join_intervals,
    prune_files_across_joins,
)
from supertable.tests.test_join_pruning import _rows_for_file, _stats_df
from supertable.utils import sql_parser as parser_module
from supertable.utils.sql_parser import SQLParser


SUPER = "queue"
A = (SUPER, "a")
B = (SUPER, "b")
C = (SUPER, "c")


def _eroding_cycle(size: int = 20):
    """Two equality edges whose ranges remove one endpoint per wave."""
    a_files = []
    b_files = []
    a_rows = []
    b_rows = []
    for value in range(size):
        a_file = f"a/{value}.parquet"
        b_file = f"b/{value}.parquet"
        a_files.append(a_file)
        b_files.append(b_file)
        a_rows += _rows_for_file(
            a_file,
            {"x": ("bigint", value, value), "y": ("bigint", value, value)},
        )
        b_rows += _rows_for_file(
            b_file,
            {
                "x": ("bigint", value, value),
                "y": ("bigint", value + 1, value + 1),
            },
        )
    return (
        [JoinEdge(A, "x", B, "x"), JoinEdge(A, "y", B, "y")],
        {A: a_files, B: b_files},
        {A: _stats_df(a_rows), B: _stats_df(b_rows)},
    )


def test_default_work_queue_reaches_fixpoint_past_the_old_heuristic_cap():
    edges, files, stats = _eroding_cycle()

    plan = prune_files_across_joins(
        edges, {}, files, stats, allow_empty=False
    )

    # The former len(edges)*2 + len(tables) + 2 cap was eight passes and
    # stopped this cycle early.  Default execution must now drain all dirty
    # directions and reach the actual finite, monotone fixpoint.
    assert plan.converged
    assert plan.iterations > 8
    assert plan.survivors[A] == ["a/9.parquet"]
    assert plan.survivors[B] == ["b/9.parquet"]


def test_eroding_cycle_repairs_witnesses_instead_of_rescanning_tables(
    monkeypatch,
):
    """One endpoint per wave must have near-linear destination-check cost."""
    size = 120
    edges, files, stats = _eroding_cycle(size)
    original = join_pruner._DestinationColumn.ranges
    checks = 0

    def counted(destination, file_key):
        nonlocal checks
        checks += 1
        return original(destination, file_key)

    monkeypatch.setattr(join_pruner._DestinationColumn, "ranges", counted)
    plan = prune_files_across_joins(
        edges, {}, files, stats, allow_empty=False
    )

    midpoint = size // 2 - 1
    assert plan.survivors[A] == [f"a/{midpoint}.parquet"]
    assert plan.survivors[B] == [f"b/{midpoint}.parquet"]
    # Four directions each inspect their destination once initially, after
    # which only files whose concrete support disappeared are revisited.  The
    # old union-and-rescan loop performed O(size**2) checks here.
    assert checks < size * 12


def _exact_cycle_oracle(edges, files, intervals, *, allow_empty):
    """Small independent exact-interval fixpoint oracle for randomized tests."""
    survivors = {table: list(keys) for table, keys in files.items()}
    directions = []
    for edge in edges:
        if edge.prune_right:
            directions.append((
                edge.left_table, edge.left_col,
                edge.right_table, edge.right_col,
            ))
        if edge.prune_left:
            directions.append((
                edge.right_table, edge.right_col,
                edge.left_table, edge.left_col,
            ))
    outgoing = {}
    for index, (src, _sc, _dst, _dc) in enumerate(directions):
        outgoing.setdefault(src, []).append(index)
    pending = deque(range(len(directions)))
    queued = set(pending)
    while pending:
        wave_size = len(pending)
        for _ in range(wave_size):
            index = pending.popleft()
            queued.remove(index)
            src, src_col, dst, dst_col = directions[index]
            source_ranges = [
                bounds
                for file_key in survivors[src]
                for bounds in intervals[src][file_key][src_col]
            ]
            if not source_ranges:
                if survivors[src] or not allow_empty:
                    continue
                new = []
            else:
                new = []
                for file_key in survivors[dst]:
                    destination_ranges = intervals[dst][file_key][dst_col]
                    if any(
                        source_lo <= destination_hi
                        and destination_lo <= source_hi
                        for source_lo, source_hi in source_ranges
                        for destination_lo, destination_hi in destination_ranges
                    ):
                        new.append(file_key)
                if not new and not allow_empty:
                    new = list(survivors[dst])
            if len(new) >= len(survivors[dst]):
                continue
            survivors[dst] = new
            for dirty in outgoing.get(dst, []):
                if dirty not in queued:
                    pending.append(dirty)
                    queued.add(dirty)
    return survivors


@pytest.mark.parametrize("allow_empty", [False, True])
def test_incremental_interval_index_matches_randomized_exact_oracle(allow_empty):
    """Witness invalidation has the same fixpoint as brute-force overlap."""
    edges = [JoinEdge(A, "x", B, "x"), JoinEdge(A, "y", B, "y")]
    for seed in range(30):
        rng = random.Random(seed)
        size = rng.randint(2, 12)
        files = {A: [], B: []}
        rows = {A: [], B: []}
        intervals = {A: {}, B: {}}
        for table, prefix in ((A, "a"), (B, "b")):
            for number in range(size):
                file_key = f"{prefix}/{number}.parquet"
                files[table].append(file_key)
                intervals[table][file_key] = {}
                specs = {}
                for column in ("x", "y"):
                    lower = rng.randint(0, size * 2)
                    upper = lower + rng.randint(0, 2)
                    specs[column] = ("bigint", lower, upper)
                    intervals[table][file_key][column] = [(lower, upper)]
                rows[table] += _rows_for_file(file_key, specs)
        stats = {table: _stats_df(table_rows) for table, table_rows in rows.items()}

        expected = _exact_cycle_oracle(
            edges, files, intervals, allow_empty=allow_empty
        )
        actual = prune_files_across_joins(
            edges, {}, files, stats, allow_empty=allow_empty
        ).survivors

        assert actual == expected, f"seed={seed}, allow_empty={allow_empty}"


def test_explicit_iteration_cap_is_safe_under_pruning():
    edges, files, stats = _eroding_cycle()

    capped = prune_files_across_joins(
        edges, {}, files, stats, allow_empty=False, max_iterations=3
    )
    complete = prune_files_across_joins(
        edges, {}, files, stats, allow_empty=False
    )

    assert not capped.converged
    assert capped.iterations == 3
    # Early termination may keep extra files, but must never discard anything
    # retained by the true fixpoint.
    assert set(complete.survivors[A]) <= set(capped.survivors[A])
    assert set(complete.survivors[B]) <= set(capped.survivors[B])


def test_profiles_for_all_join_columns_are_built_once_per_table(monkeypatch):
    files = {
        A: ["a.parquet"],
        B: ["b-hit.parquet", "b-miss.parquet"],
        C: ["c-hit.parquet", "c-miss.parquet"],
    }
    stats = {
        A: _stats_df(_rows_for_file(
            "a.parquet",
            {"customer_id": ("bigint", 10, 10), "sku": ("bigint", 50, 50)},
        )),
        B: _stats_df(
            _rows_for_file("b-hit.parquet", {"id": ("bigint", 10, 10)})
            + _rows_for_file("b-miss.parquet", {"id": ("bigint", 11, 11)})
        ),
        C: _stats_df(
            _rows_for_file("c-hit.parquet", {"id": ("bigint", 50, 50)})
            + _rows_for_file("c-miss.parquet", {"id": ("bigint", 51, 51)})
        ),
    }
    edges = [
        JoinEdge(A, "customer_id", B, "id"),
        JoinEdge(A, "sku", C, "id"),
    ]
    original = join_pruner._table_profiles
    calls = []

    def counted(frame, columns, **kwargs):
        calls.append((id(frame), frozenset(columns)))
        return original(frame, columns, **kwargs)

    monkeypatch.setattr(join_pruner, "_table_profiles", counted)
    plan = prune_files_across_joins(edges, {}, files, stats)

    assert plan.survivors[B] == ["b-hit.parquet"]
    assert plan.survivors[C] == ["c-hit.parquet"]
    assert Counter(frame_id for frame_id, _columns in calls) == Counter(
        {id(frame): 1 for frame in stats.values()}
    )
    a_call = next(columns for frame_id, columns in calls if frame_id == id(stats[A]))
    assert a_call == frozenset({"customer_id", "sku"})


def test_profile_build_skips_files_already_removed_by_where(monkeypatch):
    files = {
        A: ["a-hit.parquet", "a-miss.parquet"],
        B: ["b-hit.parquet", "b-miss.parquet"],
    }
    stats = {
        A: _stats_df(
            _rows_for_file(
                "a-hit.parquet",
                {"filter": ("bigint", 1, 1), "id": ("bigint", 10, 10)},
            )
            + _rows_for_file(
                "a-miss.parquet",
                {"filter": ("bigint", 2, 2), "id": ("bigint", 20, 20)},
            )
        ),
        B: _stats_df(
            _rows_for_file("b-hit.parquet", {"id": ("bigint", 10, 10)})
            + _rows_for_file("b-miss.parquet", {"id": ("bigint", 20, 20)})
        ),
    }
    original = join_pruner._table_profiles
    filters = {}

    def captured(frame, columns, **kwargs):
        filters[id(frame)] = kwargs.get("file_keys")
        return original(frame, columns, **kwargs)

    monkeypatch.setattr(join_pruner, "_table_profiles", captured)
    plan = prune_files_across_joins(
        [JoinEdge(A, "id", B, "id")],
        {A: [{"filter": PredInterval("numeric", 1, True, 1, True)}]},
        files,
        stats,
        allow_empty=False,
    )

    assert plan.survivors[A] == ["a-hit.parquet"]
    assert plan.survivors[B] == ["b-hit.parquet"]
    assert filters[id(stats[A])] == ["a-hit.parquet"]


def test_unchanged_export_does_not_rescan_destination(monkeypatch):
    """Losing a duplicate-range source file must not repeat destination IO."""
    files = {
        A: ["a-one.parquet", "a-two.parquet"],
        B: ["b-hit.parquet", "b-miss.parquet"],
        C: ["c.parquet"],
    }
    stats = {
        A: _stats_df(
            _rows_for_file(
                "a-one.parquet",
                {"x": ("bigint", 10, 10), "y": ("bigint", 1, 1)},
            )
            + _rows_for_file(
                "a-two.parquet",
                {"x": ("bigint", 10, 10), "y": ("bigint", 2, 2)},
            )
        ),
        B: _stats_df(
            _rows_for_file("b-hit.parquet", {"x": ("bigint", 10, 10)})
            + _rows_for_file("b-miss.parquet", {"x": ("bigint", 11, 11)})
        ),
        C: _stats_df(_rows_for_file("c.parquet", {"y": ("bigint", 2, 2)})),
    }
    original = join_pruner._DestinationColumn.ranges
    b_checks = 0

    def counted(destination, file_key):
        nonlocal b_checks
        if file_key.startswith("b-"):
            b_checks += 1
        return original(destination, file_key)

    monkeypatch.setattr(join_pruner._DestinationColumn, "ranges", counted)
    plan = prune_files_across_joins(
        [JoinEdge(A, "x", B, "x"), JoinEdge(C, "y", A, "y")],
        {}, files, stats, allow_empty=False,
    )

    assert plan.survivors[A] == ["a-two.parquet"]
    assert plan.survivors[B] == ["b-hit.parquet"]
    # Both B files are checked initially.  The duplicate active interval is a
    # containment certificate, so a-one's entire watcher set transfers to it
    # without inspecting B again (regardless of which equal tie won first).
    assert b_checks == 2


def test_empty_export_is_distinct_from_unknown_and_respects_guard():
    assert _derive_join_intervals({}, []) == []
    assert _derive_join_intervals({}, ["missing.parquet"]) is None

    files = {A: [], B: ["b.parquet"]}
    stats = {
        A: None,
        B: _stats_df(_rows_for_file("b.parquet", {"id": ("bigint", 1, 1)})),
    }
    edge = JoinEdge(A, "id", B, "id")

    may_empty = prune_files_across_joins(
        [edge], {}, files, stats, allow_empty=True
    )
    guarded = prune_files_across_joins(
        [edge], {}, files, stats, allow_empty=False
    )

    assert may_empty.survivors[B] == []
    assert guarded.survivors[B] == ["b.parquet"]


def test_streaming_interval_union_caps_to_a_conservative_hull(monkeypatch):
    monkeypatch.setattr(join_pruner, "MAX_JOIN_INTERVALS", 3)
    file_rows = {
        "f.parquet": [
            (("bigint", 8, 8), False),
            (("bigint", 0, 0), False),
            (("bigint", 4, 4), False),
            (("bigint", 12, 12), False),
        ]
    }

    intervals = _derive_join_intervals(file_rows, ["f.parquet"])

    assert intervals == [PredInterval("numeric", 0, True, 12, True)]


def test_interval_cap_applies_to_final_union_not_transient_row_order(monkeypatch):
    monkeypatch.setattr(join_pruner, "MAX_JOIN_INTERVALS", 3)
    rows = [
        (("bigint", value, value), False) for value in (0, 2, 4, 6)
    ] + [(("bigint", 0, 4), False)]

    intervals = _derive_join_intervals({"f.parquet": rows}, ["f.parquet"])
    reversed_intervals = _derive_join_intervals(
        {"f.parquet": list(reversed(rows))}, ["f.parquet"]
    )

    expected = [
        PredInterval("numeric", 0, True, 4, True),
        PredInterval("numeric", 6, True, 6, True),
    ]
    assert intervals == expected
    assert reversed_intervals == expected


def test_kernel_interval_index_keeps_more_than_cap_bands_exact(monkeypatch):
    """The fixpoint hot path need not lose precision to the reporting cap."""
    monkeypatch.setattr(join_pruner, "MAX_JOIN_INTERVALS", 3)
    source_files = []
    source_rows = []
    for value in (0, 10, 20, 30):
        file_key = f"a/{value}.parquet"
        source_files.append(file_key)
        source_rows += _rows_for_file(
            file_key, {"x": ("bigint", value, value)}
        )
    destination_files = ["b/hit.parquet", "b/gap.parquet"]
    destination_rows = (
        _rows_for_file("b/hit.parquet", {"x": ("bigint", 20, 20)})
        + _rows_for_file("b/gap.parquet", {"x": ("bigint", 15, 15)})
    )

    plan = prune_files_across_joins(
        [JoinEdge(A, "x", B, "x")],
        {},
        {A: source_files, B: destination_files},
        {A: _stats_df(source_rows), B: _stats_df(destination_rows)},
    )

    # _derive_join_intervals still returns the conservative [0,30] hull for
    # compact diagnostics at this cap, but the incremental kernel can cheaply
    # prove that 15 overlaps none of the four real source bands.
    assert plan.survivors[B] == ["b/hit.parquet"]


def test_malformed_reversed_range_fails_open():
    file_rows = {"f.parquet": [(("bigint", 9, 3), False)]}

    assert _derive_join_intervals(file_rows, ["f.parquet"]) is None


def test_predicate_and_join_analysis_share_one_scope_traversal(monkeypatch):
    original = parser_module.traverse_scope
    calls = 0

    def counted(expression):
        nonlocal calls
        calls += 1
        return original(expression)

    monkeypatch.setattr(parser_module, "traverse_scope", counted)
    parser = SQLParser(
        "queue",
        "SELECT * FROM queue.a a JOIN queue.b b ON a.id = b.id "
        "WHERE a.id >= 10",
        "duckdb",
    )

    constraints = parser.get_predicate_constraints()
    edges = parser.get_join_edges()
    # Repeated explain/debug consumers reuse the same immutable scope tuple too.
    assert parser.get_predicate_constraints() == constraints
    assert parser.get_join_edges() == edges
    assert calls == 1


def test_case_colliding_footer_columns_poison_destination_stat():
    rows = _rows_for_file(
        "f.parquet", {"ID": ("bigint", 5, 5), "id": ("bigint", 99, 99)}
    )

    index = join_pruner._build_rg_index(_stats_df(rows), ["id"])
    kept = join_pruner._prune_by_occurrences(
        ["f.parquet"],
        None,
        [{"id": PredInterval("numeric", 5, True, 5, True)}],
        allow_empty=True,
        index=index,
    )

    assert index["f.parquet"][0]["id"] is None
    assert kept == ["f.parquet"]


def test_duplicate_source_stats_row_makes_export_unknown():
    a_rows = _rows_for_file("a.parquet", {"id": ("bigint", 5, 5)})
    duplicate = dict(a_rows[0])
    duplicate["min_bigint"] = duplicate["max_bigint"] = 99
    files = {
        A: ["a.parquet"],
        B: ["b-match.parquet", "b-gap.parquet"],
    }
    stats = {
        A: _stats_df(a_rows + [duplicate]),
        B: _stats_df(
            _rows_for_file("b-match.parquet", {"id": ("bigint", 5, 5)})
            + _rows_for_file("b-gap.parquet", {"id": ("bigint", 50, 50)})
        ),
    }

    plan = prune_files_across_joins(
        [JoinEdge(A, "id", B, "id")], {}, files, stats, allow_empty=False
    )

    # Corrupt duplicate metadata cannot safely bound the partner at all.
    assert plan.survivors[B] == files[B]


def test_narrow_join_profile_rejects_a_second_populated_lane():
    a_rows = _rows_for_file("a.parquet", {"id": ("bigint", 5, 5)})
    a_rows[0]["min_string"] = a_rows[0]["max_string"] = "corrupt"
    files = {
        A: ["a.parquet"],
        B: ["b-match.parquet", "b-gap.parquet"],
    }
    stats = {
        A: _stats_df(a_rows),
        B: _stats_df(
            _rows_for_file("b-match.parquet", {"id": ("bigint", 5, 5)})
            + _rows_for_file("b-gap.parquet", {"id": ("bigint", 50, 50)})
        ),
    }

    plan = prune_files_across_joins(
        [JoinEdge(A, "id", B, "id")], {}, files, stats, allow_empty=False
    )

    assert plan.survivors[B] == files[B]
