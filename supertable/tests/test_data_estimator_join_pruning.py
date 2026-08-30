"""Cross-table (join-aware) file pruning wired into :meth:`DataEstimator.estimate`.

The pure kernel is covered by ``test_join_pruning.py``.  This suite proves the
*wiring*: that ``estimate()`` runs the kernel between its two passes, on by
default (only the existing ``SUPERTABLE_READ_PRUNING_ENABLED`` master switch
gates it — no separate kill-switch), and that a table narrowed by its own
``WHERE`` propagates its join-key range to shrink the *other* tables' surviving
file lists that end up in the ``SuperSnapshot``s.

The estimator is built with ``__new__`` and its Redis/storage seams
(``_collect_snapshots_from_redis``, ``SuperTable``, ``load_stats``,
``_to_duckdb_path``) are stubbed so no external services are touched — only the
snapshot payloads + synthetic ``STATS_SCHEMA`` frames feed the run, mirroring the
4-table webshop scenario (category / session / product / purchase, 20 files each,
one key band per file).
"""
from __future__ import annotations

import dataclasses
import hashlib
import types
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import polars
import pytest

from supertable.config.settings import settings
from supertable.engine import data_estimator as de_mod
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.plan_stats import PlanStats
from supertable.processing import STATS_SCHEMA, stats_resource_seals
from supertable.utils.sql_parser import SQLParser

SUPER = "webshop"
TABLES = ["category", "product", "purchase", "session"]
_DAY0 = datetime(2026, 1, 1)
_BAND = 7  # 2026-01-08 == _DAY0 + 7 days

SCENARIO_QUERY = """
    SELECT p.purchase_id, s.event_time, pr.product_id, c.category_id
    FROM webshop.purchase p
    JOIN webshop.session  s  ON p.session_id  = s.session_id
    JOIN webshop.product  pr ON p.product_id  = pr.product_id
    JOIN webshop.category c  ON p.category_id = c.category_id
    WHERE s.event_time >= TIMESTAMP '2026-01-08 00:00:00'
      AND s.event_time <  TIMESTAMP '2026-01-09 00:00:00'
"""


# ---------------------------------------------------------------------------
# Synthetic STATS_SCHEMA + snapshot payload builders
# ---------------------------------------------------------------------------

def _rows_for_file(file_path: str, colspecs: Dict[str, Tuple]) -> List[dict]:
    rows: List[dict] = []
    for col, spec in colspecs.items():
        lane = spec[0]
        row = {k: None for k in STATS_SCHEMA}
        row["file_path"] = file_path
        row["footer_sha256"] = hashlib.sha256(file_path.encode()).hexdigest()
        row["row_group_id"] = 0
        row["column_name"] = col
        row["physical_type"] = "INT64"
        row["logical_type"] = ""
        row["null_count"] = 0
        row["row_group_rows"] = 100
        row["compressed_bytes"] = 1000
        row["min_is_exact"] = True
        row["max_is_exact"] = True
        row["stats_available"] = True
        if lane == "bigint":
            row["min_bigint"], row["max_bigint"] = int(spec[1]), int(spec[2])
        elif lane == "timestamp":
            row["min_timestamp"], row["max_timestamp"] = spec[1], spec[2]
            row["logical_type"] = "TIMESTAMP_NTZ_MICROS"
        else:  # pragma: no cover
            raise ValueError(f"unknown lane {lane!r}")
        rows.append(row)
    return rows


def _colspecs(table: str, k: int) -> Dict[str, Tuple]:
    d_lo = _DAY0 + timedelta(days=k)
    d_hi = d_lo + timedelta(hours=23, minutes=59, seconds=59)
    if table == "session":
        return {
            "session_id": ("bigint", k * 1000, k * 1000 + 999),
            "event_time": ("timestamp", d_lo, d_hi),
        }
    if table == "purchase":
        return {
            "purchase_id": ("bigint", k * 5000, k * 5000 + 4999),
            "session_id": ("bigint", k * 1000, k * 1000 + 999),
            "product_id": ("bigint", k * 100, k * 100 + 99),
            "category_id": ("bigint", k * 5, k * 5 + 4),
            "purchase_time": ("timestamp", d_lo, d_hi),
        }
    if table == "product":
        return {"product_id": ("bigint", k * 100, k * 100 + 99)}
    if table == "category":
        return {"category_id": ("bigint", k * 5, k * 5 + 4)}
    raise AssertionError(table)  # pragma: no cover


def _build_world(n: int = 20):
    """Return ``(snapshots, stats_by_file, schema_by_table)`` for the scenario.

    *snapshots* is a single per-super list (what ``_collect_snapshots_from_redis``
    would return); each entry's payload carries its 20 file resources + a
    per-table ``stats_file`` handle resolved by the ``load_stats`` stub.
    """
    snapshots: List[dict] = []
    stats_by_file: Dict[str, polars.DataFrame] = {}
    schema_by_table: Dict[str, Dict[str, str]] = {}

    for table in TABLES:
        resources = []
        rows: List[dict] = []
        cols: Dict[str, str] = {}
        for k in range(n):
            fp = f"{table}/f{k:02d}.parquet"
            resources.append({"file": fp, "file_size": 1000, "rows": 100})
            spec = _colspecs(table, k)
            rows += _rows_for_file(fp, spec)
            for c in spec:
                cols[c] = "BIGINT"
        stats_file = f"{table}/_stats.parquet"
        stats_frame = polars.DataFrame(rows, schema=STATS_SCHEMA)
        stats_by_file[stats_file] = stats_frame
        resource_seals = stats_resource_seals(stats_frame)
        assert resource_seals is not None
        for resource in resources:
            seal = resource_seals[resource["file"]]
            resource.update({
                "footer_sha256": seal.footer_sha256,
                "stats_rows": seal.stats_rows,
                "stats_digest": seal.stats_digest,
            })
        schema_by_table[table] = cols
        snapshots.append({
            "table_name": table,
            "last_updated_ms": 1_700_000_000_000 + len(snapshots),
            "path": f"{table}/_snapshot.json",
            "payload": {
                "snapshot_version": 3,
                "_row_filter": None,
                "schema": cols,
                "stats_file": stats_file,
                "stats_rows": stats_frame.height,
                "resources": resources,
                "tombstone": None,
                "tombstone_rows": 0,
                "tombstone_digest": None,
            },
        })
    return snapshots, stats_by_file, schema_by_table


def _td(simple_name: str):
    # Minimal TableDefinition stand-in; columns=[] => SELECT * (no projection
    # sizing, no missing-column validation — orthogonal to join pruning).
    return types.SimpleNamespace(super_name=SUPER, simple_name=simple_name, columns=[])


def _make_estimator(monkeypatch, *, join_edges, predicate_constraints, pruning=True):
    snapshots, stats_by_file, _schema = _build_world(20)

    est = DataEstimator.__new__(DataEstimator)
    est.organization = "org"
    est.storage = types.SimpleNamespace()
    est.tables = [_td(t) for t in TABLES]
    est.predicate_constraints = predicate_constraints
    est.join_edges = join_edges
    est.plan_stats = PlanStats()
    est.timer = None
    est.catalog = None

    # Seams: no Redis, no storage, no real stats IO.
    est._collect_snapshots_from_redis = lambda organization, super_name: list(snapshots)
    est._to_duckdb_path = lambda key: key  # identity — assert on raw keys

    class _DummySuper:
        def __init__(self, *a, **k):
            pass

        def read_simple_table_snapshot(self, path):  # pragma: no cover - payloads inline
            return {}

    monkeypatch.setattr(de_mod, "SuperTable", _DummySuper)
    monkeypatch.setattr(
        de_mod, "load_stats",
        lambda path, **_kwargs: stats_by_file[path],
    )
    # ``settings`` is a frozen dataclass; clone it with the pruning gate toggled
    # and rebind the estimator module's reference to the clone.
    test_settings = dataclasses.replace(
        settings,
        SUPERTABLE_READ_PRUNING_ENABLED=pruning,
        SUPERTABLE_READ_PROJECTION_SIZING_ENABLED=True,
    )
    monkeypatch.setattr(de_mod, "settings", test_settings)
    return est


def _files_by_table(reflection) -> Dict[str, List[str]]:
    return {s.simple_name: list(s.files) for s in reflection.supers}


def _flat_stats(plan_stats: PlanStats) -> Dict[str, object]:
    flat: Dict[str, object] = {}
    for d in plan_stats.stats:
        flat.update(d)
    return flat


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_estimate_join_pruning_collapses_all_four_tables():
    """The headline: a WHERE that isolates ``session`` to one file drives the
    other three tables down to their single band-matching file — through the
    real ``estimate()`` two-pass path, on by default."""
    parser = SQLParser(SUPER, SCENARIO_QUERY, "duckdb")
    join_edges = parser.get_join_edges()
    predicate_constraints = parser.get_predicate_constraints()
    assert len(join_edges) == 3  # sanity: parser found the equi-joins

    with pytest.MonkeyPatch.context() as mp:
        est = _make_estimator(
            mp, join_edges=join_edges, predicate_constraints=predicate_constraints
        )
        reflection = est.estimate()

    files = _files_by_table(reflection)
    assert files["session"] == [f"session/f{_BAND:02d}.parquet"]
    assert files["purchase"] == [f"purchase/f{_BAND:02d}.parquet"]
    assert files["product"] == [f"product/f{_BAND:02d}.parquet"]
    assert files["category"] == [f"category/f{_BAND:02d}.parquet"]

    # 4 tables x 1 surviving file each.
    assert reflection.total_reflections == 4

    stats = _flat_stats(est.plan_stats)
    assert stats["FILES_BEFORE_PRUNE"] == 80
    assert stats["FILES_KEPT"] == 4
    assert stats["FILES_PRUNED"] == 76
    # Join step removed purchase/product/category (19 each); session was already
    # collapsed by its own WHERE, so it contributes nothing to the join count.
    assert stats["JOIN_EDGES"] == 3
    assert stats["JOIN_FILES_PRUNED"] == 57
    assert stats["JOIN_PRUNE_ITERATIONS"] >= 1
    for timing_name in (
        "STATS_LOAD_DURATION_MS",
        "STATS_FILTER_DURATION_MS",
        "ROW_GROUP_PRUNE_DURATION_MS",
    ):
        assert isinstance(stats[timing_name], float)
        assert stats[timing_name] >= 0.0
    assert stats["STATS_LOAD_OCCURRENCES"] == 4
    assert stats["STATS_FILTER_OCCURRENCES"] == 4
    assert stats["ROW_GROUP_PRUNE_OCCURRENCES"] == 4
    assert stats["PREDICATE_PRUNE_OCCURRENCES"] == 4
    assert stats["JOIN_PRUNE_OCCURRENCES"] == 1


def test_estimate_without_join_edges_keeps_partners_wide():
    """Parity guard: with no join edges the WHERE still collapses ``session`` but
    the partners keep all 20 files, and no JOIN_* stats are emitted."""
    parser = SQLParser(SUPER, SCENARIO_QUERY, "duckdb")
    predicate_constraints = parser.get_predicate_constraints()

    with pytest.MonkeyPatch.context() as mp:
        est = _make_estimator(
            mp, join_edges=[], predicate_constraints=predicate_constraints
        )
        reflection = est.estimate()

    files = _files_by_table(reflection)
    assert files["session"] == [f"session/f{_BAND:02d}.parquet"]
    assert len(files["purchase"]) == 20
    assert len(files["product"]) == 20
    assert len(files["category"]) == 20

    stats = _flat_stats(est.plan_stats)
    assert "JOIN_EDGES" not in stats
    assert "JOIN_FILES_PRUNED" not in stats
    assert stats["FILES_KEPT"] == 61  # 1 session + 3 x 20 partners


def test_estimate_join_pruning_disabled_by_master_switch():
    """The read-pruning master switch also disables cross-table pruning: with it
    off, nothing is pruned (neither WHERE nor join) and no prune stats appear."""
    parser = SQLParser(SUPER, SCENARIO_QUERY, "duckdb")
    join_edges = parser.get_join_edges()
    predicate_constraints = parser.get_predicate_constraints()

    with pytest.MonkeyPatch.context() as mp:
        est = _make_estimator(
            mp, join_edges=join_edges,
            predicate_constraints=predicate_constraints, pruning=False,
        )
        reflection = est.estimate()

    files = _files_by_table(reflection)
    assert len(files["session"]) == 20
    assert len(files["purchase"]) == 20
    assert len(files["product"]) == 20
    assert len(files["category"]) == 20

    stats = _flat_stats(est.plan_stats)
    assert "FILES_BEFORE_PRUNE" not in stats
    assert "JOIN_EDGES" not in stats
