"""SOUNDNESS tests for cross-table (join-aware) file pruning.

These tests encode the superset invariant the pruner must uphold: for any query
+ data, the files KEPT for a table must be a superset of the files that contain
a row participating in the query's result.  They were written RED against the
original implementation (2026-08 adversarial review) and now seal the fixes:

* outer/anti join sides: the preserved side of a LEFT/RIGHT/FULL/ANTI join is
  never pruned (its non-matching rows appear in the result) — per-endpoint
  ``prune_left``/``prune_right`` flags on :class:`JoinEdge`;
* occurrence conflation: a physical table the query binds more than once
  (second alias, UNION branch) is never pruned — the executor scans ONE shared
  file list for all occurrences;
* db-qualified 3-part column references (DuckDB struct-field access ``a.s.id``)
  never fabricate an edge;
* the estimator degrades to "no join pruning" if the kernel raises;
* the overlap primitives compare int64 exactly (no float() collapse past 2**53)
  and honour a CAST's target lane.

Do NOT weaken these tests — under-pruning is always the safe direction.

The randomized inner-join and LEFT-join property tests at the bottom compare
the kernel against a brute-force join oracle on random data, so future changes
cannot silently over-prune in either regime.
"""

from __future__ import annotations

import dataclasses
import random
import types
from typing import Dict, List, Tuple

import polars
import pytest

from supertable.config.settings import settings
from supertable.data_classes import PredInterval
from supertable.engine import data_estimator as de_mod
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.plan_stats import PlanStats
from supertable.processing import STATS_SCHEMA
from supertable.utils.sql_parser import SQLParser
from supertable.engine.join_pruner import (
    plan_file_pruning_for_query,
    prune_files_across_joins,
)

SUPER = "s"


# ---------------------------------------------------------------------------
# Synthetic STATS_SCHEMA builders (same shape as test_join_pruning.py)
# ---------------------------------------------------------------------------

def _rows_for_file(file_path: str, colspecs: Dict[str, Tuple[int, int]]) -> List[dict]:
    rows: List[dict] = []
    for col, (mn, mx) in colspecs.items():
        row = {k: None for k in STATS_SCHEMA}
        row["file_path"] = file_path
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
        row["min_bigint"], row["max_bigint"] = int(mn), int(mx)
        rows.append(row)
    return rows


def _stats_df(rows: List[dict]) -> polars.DataFrame:
    return polars.DataFrame(rows, schema=STATS_SCHEMA)


def _banded_table(name: str, n: int, col: str, width: int = 100):
    """n files; file i holds ``col`` in band [i*width, i*width+width-1]."""
    files, rows = [], []
    for i in range(n):
        fp = f"{name}/f{i:02d}.parquet"
        files.append(fp)
        rows += _rows_for_file(fp, {col: (i * width, i * width + width - 1)})
    return files, _stats_df(rows)


# ---------------------------------------------------------------------------
# 1) Outer / anti joins must not prune the preserved side
# ---------------------------------------------------------------------------
# Layout: a = 20 banded files over k in [0,1999]; b = ONE file with k in
# [700,799].  An INNER join could legitimately prune a to its band-7 file, but
# the preserved side of an outer join must keep every file: rows of a with no
# match in b still appear (padded with NULLs) — and for ANTI they are exactly
# the result.

def _outer_world():
    a_files, a_stats = _banded_table("a", 20, "id")
    b_files, b_stats = _banded_table("b", 1, "id")  # single band [0,99]
    # move b's band to [700,799] so the overlap is band 7 of a
    b_stats = _stats_df(_rows_for_file("b/f00.parquet", {"id": (700, 799)}))
    table_files = {(SUPER, "a"): a_files, (SUPER, "b"): b_files}
    table_stats = {(SUPER, "a"): a_stats, (SUPER, "b"): b_stats}
    return table_files, table_stats


@pytest.mark.parametrize(
    "query, must_keep_all",
    [
        # LEFT: a preserved — every a file contributes.  Pruning b is fine.
        ("SELECT * FROM s.a a LEFT JOIN s.b b ON a.id = b.id", ["a"]),
        # RIGHT: b preserved (here b is 1 file anyway; flip roles instead).
        ("SELECT * FROM s.b b RIGHT JOIN s.a a ON b.id = a.id", ["a"]),
        # FULL OUTER: both sides preserved.
        ("SELECT * FROM s.a a FULL OUTER JOIN s.b b ON a.id = b.id", ["a", "b"]),
        # ANTI: result is precisely the NON-matching rows of a — pruning a to
        # b's band inverts the semantics (keeps the one file whose rows are
        # excluded, drops the 19 files whose rows are the answer).
        ("SELECT * FROM s.a a ANTI JOIN s.b b ON a.id = b.id", ["a"]),
    ],
    ids=["left", "right", "full", "anti"],
)
def test_outer_join_preserved_side_is_never_pruned(query, must_keep_all):
    table_files, table_stats = _outer_world()
    plan = plan_file_pruning_for_query(SUPER, query, "duckdb", table_files, table_stats)
    for t in must_keep_all:
        kept = plan.survivors[(SUPER, t)]
        assert len(kept) == len(table_files[(SUPER, t)]), (
            f"preserved side {t!r} was pruned to {kept} — rows that must appear "
            f"in the outer/anti join result were dropped"
        )


def test_semi_join_prunes_like_inner():
    """SEMI keeps only matching rows of a — pruning both sides IS sound, and
    the pruner should still do it (guards the fix against over-correction)."""
    table_files, table_stats = _outer_world()
    q = "SELECT * FROM s.a a SEMI JOIN s.b b ON a.id = b.id"
    plan = plan_file_pruning_for_query(SUPER, q, "duckdb", table_files, table_stats)
    assert plan.survivors[(SUPER, "a")] == ["a/f07.parquet"]


# ---------------------------------------------------------------------------
# 2) Multiple occurrences of one physical table share ONE file list
# ---------------------------------------------------------------------------

def test_union_branch_full_scan_blocks_join_pruning():
    """``a JOIN b`` in branch 1 must not prune b when branch 2 scans b in full:
    the executor registers ONE view per physical table, shared by both
    branches."""
    a_files, a_stats = _banded_table("a", 20, "k")
    b_files, b_stats = _banded_table("b", 20, "k")
    table_files = {(SUPER, "a"): a_files, (SUPER, "b"): b_files}
    table_stats = {(SUPER, "a"): a_stats, (SUPER, "b"): b_stats}
    q = (
        "SELECT a.k FROM s.a a JOIN s.b b ON a.k = b.k "
        "WHERE a.k BETWEEN 700 AND 799 "
        "UNION ALL SELECT b2.k FROM s.b b2"
    )
    plan = plan_file_pruning_for_query(SUPER, q, "duckdb", table_files, table_stats)
    assert len(plan.survivors[(SUPER, "b")]) == 20, (
        "b was join-pruned even though the UNION's second branch reads it in full"
    )


def test_union_transitive_contamination_across_scopes():
    """Branch 1 filters a and joins a-b; branch 2 joins b-c (no filter).  The
    edge graph flattens the scopes, so branch 1's filter leaks through b and
    wrongly prunes c — but branch 2 needs ALL of b x c."""
    a_files, a_stats = _banded_table("a", 20, "k")
    b_files, b_stats = _banded_table("b", 20, "k")
    c_files, c_stats = _banded_table("c", 20, "k")
    table_files = {(SUPER, "a"): a_files, (SUPER, "b"): b_files, (SUPER, "c"): c_files}
    table_stats = {(SUPER, "a"): a_stats, (SUPER, "b"): b_stats, (SUPER, "c"): c_stats}
    q = (
        "SELECT a.k FROM s.a a JOIN s.b b ON a.k = b.k "
        "WHERE a.k BETWEEN 700 AND 799 "
        "UNION ALL SELECT b2.k FROM s.b b2 JOIN s.c c ON b2.k = c.k"
    )
    plan = plan_file_pruning_for_query(SUPER, q, "duckdb", table_files, table_stats)
    assert len(plan.survivors[(SUPER, "c")]) == 20, (
        "c was pruned through b by branch 1's filter, but branch 2 joins the "
        "FULL b against c"
    )


def test_two_aliases_of_one_table_need_the_union_not_intersection():
    """b joined twice on different a-columns: b's shared file list must keep
    the UNION of what each alias needs; sequential edge application computes
    the intersection instead (under estimator semantics, allow_empty=False)."""
    # a: one file with x in band 2 and z in band 9 (of b's k bands).
    a_rows = _rows_for_file("a/f00.parquet", {"x": (250, 260), "z": (910, 920)})
    b_files, b_stats = _banded_table("b", 20, "k")
    table_files = {(SUPER, "a"): ["a/f00.parquet"], (SUPER, "b"): b_files}
    table_stats = {(SUPER, "a"): _stats_df(a_rows), (SUPER, "b"): b_stats}
    q = (
        "SELECT * FROM s.a a "
        "JOIN s.b b1 ON a.x = b1.k "
        "JOIN s.b b2 ON a.z = b2.k"
    )
    parser = SQLParser(SUPER, q, "duckdb")
    plan = prune_files_across_joins(
        parser.get_join_edges(),
        parser.get_predicate_constraints(),
        table_files,
        table_stats,
        allow_empty=False,  # estimator semantics
    )
    kept = set(plan.survivors[(SUPER, "b")])
    assert {"b/f02.parquet", "b/f09.parquet"} <= kept, (
        f"b kept {sorted(kept)} — alias b1 needs f02 AND alias b2 needs f09; "
        f"the executor scans b once for both"
    )


# ---------------------------------------------------------------------------
# 3) Column qualifiers: a 3-part reference is NOT alias.column
# ---------------------------------------------------------------------------

def test_struct_field_access_does_not_fabricate_an_edge():
    """DuckDB resolves ``a.s.id`` as struct-field ``id`` of column ``s`` of
    table ``a`` — but sqlglot parses it as Column(db=a, table=s, this=id) and
    get_join_edges drops the ``db`` qualifier, mis-binding it to table alias
    ``s`` and fabricating the edge ``b.y = s.id``.  A fabricated edge prunes b
    by an unrelated column's range: pruning invented from thin air."""
    a_files, a_stats = _banded_table("a", 1, "k")
    s_files, s_stats = _banded_table("s", 1, "k")   # also has id in [0,99]
    s_stats = _stats_df(
        _rows_for_file("s/f00.parquet", {"k": (0, 99), "id": (0, 99)})
    )
    b_files, b_stats = _banded_table("b", 20, "y")
    table_files = {
        (SUPER, "a"): a_files, (SUPER, "s"): s_files, (SUPER, "b"): b_files,
    }
    table_stats = {
        (SUPER, "a"): a_stats, (SUPER, "s"): s_stats, (SUPER, "b"): b_stats,
    }
    q = (
        "SELECT * FROM s.a a "
        "JOIN s.s s ON a.k = s.k "
        "JOIN s.b b ON a.s.id = b.y"
    )
    plan = plan_file_pruning_for_query(SUPER, q, "duckdb", table_files, table_stats)
    assert len(plan.survivors[(SUPER, "b")]) == 20, (
        "b was pruned via a fabricated edge: a.s.id is a struct-field access "
        "on table a, not column id of table s"
    )


# ---------------------------------------------------------------------------
# 4) Pre-existing overlap-primitive soundness gaps the kernel inherits
# ---------------------------------------------------------------------------

def test_pred_overlap_is_exact_for_int64_beyond_2_53():
    """``WHERE k > 2**53`` must keep a file whose only value is 2**53 + 1.
    float() coercion in _pred_overlaps_stored collapses both onto the same
    float, and the strict bound then excludes the file (pre-existing bug in the
    WHERE-pruning primitive the join kernel builds on).  Python compares
    int vs float exactly — the coercion is unnecessary."""
    from supertable.processing import _pred_overlaps_stored

    L = 2**53
    pred = PredInterval("numeric", L, False, None, True)  # k > L, strict
    assert _pred_overlaps_stored(pred, ("bigint", L + 1, L + 1)) is True


def test_cast_numeric_of_string_literal_is_not_a_string_constraint():
    """``WHERE c = CAST('1.5' AS DOUBLE)`` compares c NUMERICALLY (DuckDB casts
    the column), so a file whose c is the string '1.50' matches.  The literal
    extractor unwraps the CAST and emits a STRING-lane constraint '1.5', whose
    byte-order comparison excludes that file ('1.5' < '1.50')."""
    from supertable.processing import prune_files_by_predicates
    from supertable.utils.sql_parser import SQLParser as _P

    rows = []
    row = {k: None for k in STATS_SCHEMA}
    row.update(
        file_path="t/f00.parquet", row_group_id=0, column_name="c",
        physical_type="BYTE_ARRAY", logical_type="STRING", null_count=0,
        row_group_rows=1, compressed_bytes=100, stats_available=True,
        min_is_exact=True, max_is_exact=True,
        min_string="1.50", max_string="1.50",  # numerically == 1.5
    )
    rows.append(row)
    row2 = dict(row)
    row2.update(file_path="t/f01.parquet", min_string="1.5", max_string="1.5")
    rows.append(row2)
    stats = _stats_df(rows)

    constraints = _P(SUPER, "SELECT * FROM s.t WHERE c = CAST('1.5' AS DOUBLE)",
                     "duckdb").get_predicate_constraints()
    occurrences = constraints[(SUPER, "t")]
    kept = prune_files_by_predicates(
        ["t/f00.parquet", "t/f01.parquet"], stats, occurrences
    )
    assert "t/f00.parquet" in kept, (
        "file with c='1.50' pruned by string-order comparison of a constraint "
        "the engine evaluates numerically"
    )


# ---------------------------------------------------------------------------
# 5) Estimator-level differential: pruning ON vs OFF must not change the
#    preserved side of an outer join (mirrors the harness in
#    test_data_estimator_join_pruning.py)
# ---------------------------------------------------------------------------

def _make_estimator(monkeypatch, *, query: str, pruning: bool):
    a_files, a_stats = _banded_table("a", 20, "k")
    b_stats = _stats_df(_rows_for_file("b/f00.parquet", {"k": (700, 799)}))
    stats_by_file = {"a/_stats.parquet": a_stats, "b/_stats.parquet": b_stats}
    snapshots = [
        {
            "table_name": "a",
            "last_updated_ms": 1_700_000_000_000,
            "path": "a/_snapshot.json",
            "payload": {
                "snapshot_version": 3,
                "_row_filter": None,
                "schema": {"k": "BIGINT"},
                "stats_file": "a/_stats.parquet",
                "resources": [
                    {"file": f, "file_size": 1000, "rows": 100}
                    for f in a_files
                ],
                "tombstone": None,
                "tombstone_rows": 0,
                "tombstone_digest": None,
            },
        },
        {
            "table_name": "b",
            "last_updated_ms": 1_700_000_000_001,
            "path": "b/_snapshot.json",
            "payload": {
                "snapshot_version": 3,
                "_row_filter": None,
                "schema": {"k": "BIGINT"},
                "stats_file": "b/_stats.parquet",
                "resources": [
                    {"file": "b/f00.parquet", "file_size": 1000, "rows": 100}
                ],
                "tombstone": None,
                "tombstone_rows": 0,
                "tombstone_digest": None,
            },
        },
    ]

    parser = SQLParser(SUPER, query, "duckdb")
    est = DataEstimator.__new__(DataEstimator)
    est.organization = "org"
    est.storage = types.SimpleNamespace()
    est.tables = [
        types.SimpleNamespace(super_name=SUPER, simple_name="a", columns=[]),
        types.SimpleNamespace(super_name=SUPER, simple_name="b", columns=[]),
    ]
    est.predicate_constraints = parser.get_predicate_constraints()
    est.join_edges = parser.get_join_edges()
    est.plan_stats = PlanStats()
    est.timer = None
    est.catalog = None
    est._collect_snapshots_from_redis = lambda organization, super_name: list(snapshots)
    est._to_duckdb_path = lambda key: key
    # This fixture exercises join-propagation soundness with synthetic footer
    # frames. Resource-seal validation has its own tests; keep that independent
    # boundary from discarding these deliberately unsealed synthetic stats.
    est._stats_for_complete_files = (
        lambda stats_df, resource_rows, resource_seals=None, stats_path=None: stats_df
    )

    class _DummySuper:
        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr(de_mod, "SuperTable", _DummySuper)
    monkeypatch.setattr(
        de_mod, "load_stats",
        lambda path, allow_cache=False, cache_identity=None, profiler=None,
        storage=None: stats_by_file[path],
    )
    test_settings = dataclasses.replace(
        settings, SUPERTABLE_READ_PRUNING_ENABLED=pruning
    )
    monkeypatch.setattr(de_mod, "settings", test_settings)
    return est


def test_estimator_differential_left_join_preserved_side():
    """The file set the executor scans for the preserved side of a LEFT JOIN
    must be identical with pruning on and off."""
    q = "SELECT * FROM s.a a LEFT JOIN s.b b ON a.k = b.k"
    files: Dict[bool, Dict[str, List[str]]] = {}
    for pruning in (True, False):
        with pytest.MonkeyPatch.context() as mp:
            est = _make_estimator(mp, query=q, pruning=pruning)
            reflection = est.estimate()
        files[pruning] = {s.simple_name: sorted(s.files) for s in reflection.supers}
    assert files[True]["a"] == files[False]["a"], (
        f"pruning changed the preserved side of a LEFT JOIN: "
        f"{len(files[True]['a'])} vs {len(files[False]['a'])} files"
    )


def test_estimator_inner_join_still_prunes_the_probe_side():
    """Guard against over-correction: the same layout with an INNER join must
    still collapse ``a`` to its single band-matching file."""
    q = "SELECT * FROM s.a a JOIN s.b b ON a.k = b.k"
    with pytest.MonkeyPatch.context() as mp:
        est = _make_estimator(mp, query=q, pruning=True)
        reflection = est.estimate()
    files = {s.simple_name: list(s.files) for s in reflection.supers}
    assert files["a"] == ["a/f07.parquet"]


def test_estimator_degrades_to_no_join_pruning_when_kernel_raises():
    """A pruning failure must never break a read: if the kernel raises, the
    estimator keeps the Pass-1 survivors and completes."""
    q = "SELECT * FROM s.a a JOIN s.b b ON a.k = b.k"
    with pytest.MonkeyPatch.context() as mp:
        est = _make_estimator(mp, query=q, pruning=True)

        def _boom(*a, **k):
            raise RuntimeError("synthetic kernel failure")

        mp.setattr(de_mod, "prune_files_across_joins", _boom)
        reflection = est.estimate()
    files = {s.simple_name: list(s.files) for s in reflection.supers}
    assert len(files["a"]) == 20  # unpruned, but the read succeeded
    assert len(files["b"]) == 1


def test_wrapper_where_pruning_is_case_insensitive():
    """The public wrapper lowercases constraint keys, so a mixed-case SQL
    identifier still drives phase-1 WHERE pruning."""
    a_files, a_stats = _banded_table("a", 20, "k")
    plan = plan_file_pruning_for_query(
        SUPER,
        "SELECT * FROM s.a WHERE K BETWEEN 700 AND 799",
        "duckdb",
        {(SUPER, "a"): a_files},
        {(SUPER, "a"): a_stats},
    )
    assert plan.survivors[(SUPER, "a")] == ["a/f07.parquet"]


def test_cast_numeric_of_numeric_string_prunes_numerically():
    """Positive counterpart of the CAST red test: the constraint lands in the
    cast-numeric lane and prunes against bigint stats for explicit DuckDB."""
    from supertable.utils.sql_parser import SQLParser as _P

    constraints = _P(SUPER, "SELECT * FROM s.t WHERE c = CAST('750' AS BIGINT)",
                     "duckdb").get_predicate_constraints()
    (occ,) = constraints[(SUPER, "t")]
    pred = occ["c"]
    assert pred.lane == "numeric_cast" and pred.lo == 750 and pred.hi == 750


# ---------------------------------------------------------------------------
# 4) Randomized inner-join soundness property (PASSES today — keep it green)
# ---------------------------------------------------------------------------
# Chain a-b-c on one shared key column, random per-file value sets, random
# optional WHERE interval on a.  Brute-force the set of keys that actually
# join; every file holding such a key must survive the kernel.

def _random_table(rng: random.Random, name: str, n_files: int, domain: int):
    files, rows, values = [], [], {}
    for i in range(n_files):
        fp = f"{name}/f{i:02d}.parquet"
        vals = sorted(rng.sample(range(domain), rng.randint(1, 6)))
        files.append(fp)
        values[fp] = set(vals)
        rows += _rows_for_file(fp, {"k": (vals[0], vals[-1])})
    return files, _stats_df(rows), values


@pytest.mark.parametrize("seed", range(25))
def test_inner_join_kernel_keeps_every_contributing_file(seed):
    rng = random.Random(seed)
    world = {}
    for t in ("a", "b", "c"):
        world[t] = _random_table(rng, t, rng.randint(2, 8), domain=60)

    table_files = {(SUPER, t): world[t][0] for t in world}
    table_stats = {(SUPER, t): world[t][1] for t in world}

    # Optional WHERE interval on a.k
    constraints = {}
    lo = rng.randint(0, 50)
    hi = lo + rng.randint(0, 30)
    if rng.random() < 0.7:
        constraints[(SUPER, "a")] = [
            {"k": PredInterval("numeric", lo, True, hi, True)}
        ]
    else:
        lo, hi = 0, 10**9  # no constraint

    # Brute force: keys that produce at least one joined row.
    all_vals = {t: set().union(*world[t][2].values()) for t in world}
    joinable = {
        v for v in all_vals["a"]
        if lo <= v <= hi and v in all_vals["b"] and v in all_vals["c"]
    }

    parser = SQLParser(
        SUPER,
        "SELECT * FROM s.a a JOIN s.b b ON a.k = b.k JOIN s.c c ON b.k = c.k",
        "duckdb",
    )
    plan = prune_files_across_joins(
        parser.get_join_edges(), constraints, table_files, table_stats,
        allow_empty=True,
    )
    for t in world:
        contributing = {
            fp for fp, vals in world[t][2].items() if vals & joinable
        }
        kept = set(plan.survivors[(SUPER, t)])
        assert contributing <= kept, (
            f"seed={seed} table={t}: contributing files {sorted(contributing - kept)} "
            f"were pruned (kept={sorted(kept)}, joinable keys={sorted(joinable)})"
        )


@pytest.mark.parametrize("seed", range(25))
def test_left_join_kernel_keeps_every_contributing_file(seed):
    """LEFT-join soundness property: every a file with a row passing the WHERE
    contributes (null-extended if unmatched); a b file contributes when one of
    its keys matches such an a row."""
    rng = random.Random(seed + 1000)
    a_files, a_stats, a_vals = _random_table(rng, "a", rng.randint(2, 8), domain=60)
    b_files, b_stats, b_vals = _random_table(rng, "b", rng.randint(2, 8), domain=60)

    lo = rng.randint(0, 50)
    hi = lo + rng.randint(0, 30)
    constraints = {
        (SUPER, "a"): [{"k": PredInterval("numeric", lo, True, hi, True)}]
    }

    parser = SQLParser(
        SUPER, "SELECT * FROM s.a a LEFT JOIN s.b b ON a.k = b.k", "duckdb"
    )
    plan = prune_files_across_joins(
        parser.get_join_edges(), constraints,
        {(SUPER, "a"): a_files, (SUPER, "b"): b_files},
        {(SUPER, "a"): a_stats, (SUPER, "b"): b_stats},
        allow_empty=True,
    )

    a_pass = {v for vals in a_vals.values() for v in vals if lo <= v <= hi}
    contributing_a = {
        fp for fp, vals in a_vals.items() if any(lo <= v <= hi for v in vals)
    }
    all_b = set().union(*b_vals.values())
    contributing_b = {
        fp for fp, vals in b_vals.items() if vals & a_pass & all_b
    }
    kept_a = set(plan.survivors[(SUPER, "a")])
    kept_b = set(plan.survivors[(SUPER, "b")])
    assert contributing_a <= kept_a, (
        f"seed={seed}: preserved-side files {sorted(contributing_a - kept_a)} pruned"
    )
    assert contributing_b <= kept_b, (
        f"seed={seed}: matching b files {sorted(contributing_b - kept_b)} pruned"
    )
