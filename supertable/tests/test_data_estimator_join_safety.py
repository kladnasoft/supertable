"""Fail-open and transactional guards around estimator join pruning.

The pruning kernel has its own semantic tests.  These regressions exercise the
read-path trust boundary: optional stats may fail, inert/ambiguous edges must
cost nothing, and a malformed kernel result must never replace or partially
apply a table's candidate files.
"""
from __future__ import annotations

import polars

from supertable.data_classes import JoinEdge, PredInterval
from supertable.engine import data_estimator as de_mod
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.join_pruner import JoinPrunePlan
from supertable.processing import STATS_SCHEMA, prune_files_by_predicates
from supertable.tests.test_data_estimator_join_pruning import (
    SUPER,
    _flat_stats,
    _make_estimator,
    _td,
)


CATEGORY = (SUPER, "category")
PRODUCT = (SUPER, "product")


def _edge(*, prune_left: bool = True, prune_right: bool = True) -> JoinEdge:
    return JoinEdge(
        CATEGORY, "category_id", PRODUCT, "product_id",
        prune_left=prune_left, prune_right=prune_right,
    )


def _files(reflection, simple_name: str):
    return next(
        list(snapshot.files)
        for snapshot in reflection.supers
        if snapshot.simple_name == simple_name
    )


def test_join_stats_load_failure_keeps_every_candidate(monkeypatch):
    """A broken join-only stats pointer cannot turn SELECT into an error."""
    est = _make_estimator(
        monkeypatch, join_edges=[_edge()], predicate_constraints={},
    )
    attempted = []

    def unavailable(path, **_kwargs):
        attempted.append(path)
        raise OSError("stats object unavailable")

    monkeypatch.setattr(de_mod, "load_stats", unavailable)
    reflection = est.estimate()

    # Only endpoints of the one runnable edge need join stats.  Both failures
    # degrade to unknown ranges, retaining every file.
    assert set(attempted) == {
        "category/_stats.parquet", "product/_stats.parquet",
    }
    assert len(_files(reflection, "category")) == 20
    assert len(_files(reflection, "product")) == 20
    stats = _flat_stats(est.plan_stats)
    assert stats["FILES_BEFORE_PRUNE"] == 80
    assert stats["FILES_KEPT"] == 80
    assert stats["FILES_PRUNED"] == 0


def test_snapshot_row_count_rejects_truncated_join_stats(monkeypatch):
    """A readable partial stats artifact is not absence proof."""
    est = _make_estimator(
        monkeypatch, join_edges=[_edge()], predicate_constraints={},
    )
    snapshots = est._collect_snapshots_from_redis("org", SUPER)
    for snapshot in snapshots:
        # Deliberately disagree with each loaded artifact's real height.
        snapshot["payload"]["stats_rows"] = 10_000
    est._collect_snapshots_from_redis = (
        lambda organization, super_name: list(snapshots)
    )
    observed = {}

    def unchanged(_edges, _constraints, table_files, table_stats, **_kwargs):
        observed.update(table_stats)
        return JoinPrunePlan(
            survivors={key: list(files) for key, files in table_files.items()}
        )

    monkeypatch.setattr(de_mod, "prune_files_across_joins", unchanged)
    reflection = est.estimate()

    assert observed == {CATEGORY: None, PRODUCT: None}
    assert len(_files(reflection, "category")) == 20
    assert len(_files(reflection, "product")) == 20
    stats = _flat_stats(est.plan_stats)
    assert stats["FILES_PRUNED"] == 0


def test_same_height_stats_substitution_cannot_hide_a_row_group():
    """Per-file manifests catch corruption a global height check cannot.

    The genuine four-slot artifact would contain ``f/rg0``, ``f/rg1``,
    ``g/rg0`` and ``h/rg0``.  Replace f's second row group with a duplicate of
    g's slot: the table-level height remains four, but f still contains a real
    ``k=100`` row.  Trusting the partial f range ``[1,1]`` would drop that
    contributing file while g keeps the table non-empty, bypassing the legacy
    never-empty guard.
    """
    def stats_row(file_path: str, row_group_id: int, value: int) -> dict:
        row = {column: None for column in STATS_SCHEMA}
        row.update({
            "file_path": file_path,
            "row_group_id": row_group_id,
            "column_name": "k",
            "physical_type": "INT64",
            "logical_type": "",
            "min_bigint": value,
            "max_bigint": value,
            "null_count": 0,
            "row_group_rows": 1,
            "compressed_bytes": 8,
            "stats_available": True,
            "min_is_exact": True,
            "max_is_exact": True,
        })
        return row

    corrupt = polars.DataFrame([
        stats_row("f.parquet", 0, 1),
        stats_row("g.parquet", 0, 100),
        stats_row("g.parquet", 0, 100),  # substitutes for missing f/rg1
        stats_row("h.parquet", 0, 0),
    ], schema=STATS_SCHEMA)
    assert corrupt.height == 4  # the snapshot's global stats_rows still agrees

    safe = DataEstimator._stats_for_complete_files(
        corrupt,
        {"f.parquet": 2, "g.parquet": 1, "h.parquet": 1},
    )
    assert safe is not None
    assert set(safe["file_path"].to_list()) == {"h.parquet"}

    kept = prune_files_by_predicates(
        ["f.parquet", "g.parquet", "h.parquet"],
        safe,
        [{"k": PredInterval("numeric", 100, True, 100, True)}],
    )
    assert kept == ["f.parquet", "g.parquet"]


def test_malformed_projection_stats_fall_back_without_breaking_select(monkeypatch):
    """A successfully loaded but invalid stats object is still optional."""
    est = _make_estimator(
        monkeypatch, join_edges=[], predicate_constraints={},
    )
    category = _td("category")
    category.columns = ["category_id"]
    est.tables = [category]
    monkeypatch.setattr(
        de_mod, "load_stats", lambda *_args, **_kwargs: {"malformed": True},
    )

    reflection = est.estimate()

    assert len(_files(reflection, "category")) == 20
    # Schema-width fallback sees one selected column out of one user column,
    # so it conservatively retains the whole 20 x 1000-byte estimate.
    assert reflection.reflection_bytes == 20_000


def test_fully_inert_edge_loads_no_stats_and_skips_kernel(monkeypatch):
    """FULL-style false/false edges have no legal pruning direction."""
    est = _make_estimator(
        monkeypatch,
        join_edges=[_edge(prune_left=False, prune_right=False)],
        predicate_constraints={},
    )

    def should_not_run(*_args, **_kwargs):
        raise AssertionError("inert join pruning work should have been skipped")

    monkeypatch.setattr(de_mod, "load_stats", should_not_run)
    monkeypatch.setattr(de_mod, "prune_files_across_joins", should_not_run)
    reflection = est.estimate()

    assert reflection.total_reflections == 80
    stats = _flat_stats(est.plan_stats)
    assert "JOIN_EDGES" not in stats
    assert "JOIN_FILES_PRUNED" not in stats
    assert stats["FILES_PRUNED"] == 0


def test_unconstrained_or_unsafe_predicates_load_no_stats(monkeypatch):
    """Parser placeholders and untrusted lanes are guaranteed pruning no-ops."""
    est = _make_estimator(
        monkeypatch,
        join_edges=[],
        predicate_constraints={
            CATEGORY: [{}],
            PRODUCT: [{"name": PredInterval(
                "string", "a", True, "z", True,
            )}],
        },
    )

    def should_not_load(*_args, **_kwargs):
        raise AssertionError("no-op predicate must not load stats")

    monkeypatch.setattr(de_mod, "load_stats", should_not_load)
    reflection = est.estimate()

    assert reflection.total_reflections == 80
    stats = _flat_stats(est.plan_stats)
    assert stats["FILES_PRUNED"] == 0


def test_invalid_join_plan_is_rejected_atomically(monkeypatch):
    """One invalid endpoint rolls back valid-looking changes to every endpoint."""
    est = _make_estimator(
        monkeypatch, join_edges=[_edge()], predicate_constraints={},
    )

    def malformed(_edges, _constraints, table_files, _stats, **_kwargs):
        return JoinPrunePlan(
            survivors={
                CATEGORY: table_files[CATEGORY][:1],       # valid subset
                PRODUCT: ["category/f00.parquet"],        # foreign file
            },
            iterations=1,
        )

    monkeypatch.setattr(de_mod, "prune_files_across_joins", malformed)
    reflection = est.estimate()

    # The category proposal appeared first and was valid, but must not leak
    # through after product fails validation.
    assert len(_files(reflection, "category")) == 20
    assert len(_files(reflection, "product")) == 20
    stats = _flat_stats(est.plan_stats)
    assert stats["JOIN_FILES_PRUNED"] == 0
    assert stats["FILES_PRUNED"] == 0
    assert not stats.get("PRUNE_COUNTS", {}).get("read_join_pruned_files")


def test_incomplete_join_plan_is_rejected_atomically(monkeypatch):
    """A missing endpoint may hide a partially computed, unsafe fixpoint."""
    est = _make_estimator(
        monkeypatch, join_edges=[_edge()], predicate_constraints={},
    )

    def incomplete(_edges, _constraints, table_files, _stats, **_kwargs):
        # If PRODUCT had first been narrowed internally, this CATEGORY result
        # could depend on that narrower set.  Restoring PRODUCT while applying
        # CATEGORY would therefore be unsafe; reject the whole plan.
        return JoinPrunePlan(
            survivors={CATEGORY: table_files[CATEGORY][:1]},
            iterations=1,
        )

    monkeypatch.setattr(de_mod, "prune_files_across_joins", incomplete)
    reflection = est.estimate()

    assert len(_files(reflection, "category")) == 20
    assert len(_files(reflection, "product")) == 20
    stats = _flat_stats(est.plan_stats)
    assert stats["JOIN_FILES_PRUNED"] == 0
    assert stats["FILES_PRUNED"] == 0


def test_valid_join_plan_has_distinct_nonduplicated_profiler_count(monkeypatch):
    """Join removals are counted once under an explicit join-only name."""
    est = _make_estimator(
        monkeypatch, join_edges=[_edge()], predicate_constraints={},
    )

    def valid(_edges, _constraints, table_files, _stats, **_kwargs):
        return JoinPrunePlan(
            survivors={
                CATEGORY: table_files[CATEGORY][:2],
                PRODUCT: table_files[PRODUCT][:3],
            },
            iterations=2,
        )

    monkeypatch.setattr(de_mod, "prune_files_across_joins", valid)
    reflection = est.estimate()

    assert len(_files(reflection, "category")) == 2
    assert len(_files(reflection, "product")) == 3
    stats = _flat_stats(est.plan_stats)
    assert stats["JOIN_FILES_PRUNED"] == 35
    assert stats["FILES_PRUNED"] == 35
    counts = stats["PRUNE_COUNTS"]
    assert counts["read_join_pruned_files"] == 35
    assert counts.get("read_pruned_files", 0) == 0


def test_duplicate_normalized_table_key_disables_join_work(monkeypatch):
    """Case variants cannot be collapsed into one range/file map entry."""
    est = _make_estimator(
        monkeypatch, join_edges=[_edge()], predicate_constraints={},
    )
    est.tables = [_td("category"), _td("Category"), _td("product")]

    def should_not_run(*_args, **_kwargs):
        raise AssertionError("ambiguous join endpoint must be fail-open")

    monkeypatch.setattr(de_mod, "load_stats", should_not_run)
    monkeypatch.setattr(de_mod, "prune_files_across_joins", should_not_run)
    reflection = est.estimate()

    assert len(_files(reflection, "category")) == 20
    assert len(_files(reflection, "Category")) == 20
    assert len(_files(reflection, "product")) == 20
    stats = _flat_stats(est.plan_stats)
    assert "JOIN_EDGES" not in stats
    assert stats["FILES_PRUNED"] == 0
    assert stats["FILES_BEFORE_PRUNE"] == stats["FILES_KEPT"] == 60


def test_invalid_literal_pruner_output_cannot_break_file_counters(monkeypatch):
    """The estimator boundary also rejects injected files from phase one."""
    est = _make_estimator(
        monkeypatch, join_edges=[], predicate_constraints={},
    )
    def inject(_super, _simple, raw_keys, _stats, profiler=None):
        profiler.add("read_pruned_files", 999)
        return list(raw_keys) + ["foreign/file.parquet"]

    est._prune_files = inject
    reflection = est.estimate()

    assert reflection.total_reflections == 80
    stats = _flat_stats(est.plan_stats)
    assert stats["FILES_BEFORE_PRUNE"] == stats["FILES_KEPT"] == 80
    assert stats["FILES_PRUNED"] == 0
    assert not stats.get("PRUNE_COUNTS", {}).get("read_pruned_files")


def test_literal_counter_is_derived_from_validated_survivors(monkeypatch):
    est = _make_estimator(
        monkeypatch, join_edges=[], predicate_constraints={},
    )

    def silent_valid(_super, simple, raw_keys, _stats, profiler=None):
        return raw_keys[:2] if simple == "category" else raw_keys

    est._prune_files = silent_valid
    reflection = est.estimate()

    assert len(_files(reflection, "category")) == 2
    stats = _flat_stats(est.plan_stats)
    assert stats["FILES_PRUNED"] == 18
    assert stats["PRUNE_COUNTS"]["read_pruned_files"] == 18
