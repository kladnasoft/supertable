"""Cross-table (join-aware) file pruning — the webshop star-join scenario.

Four super tables (``category``, ``session``, ``product``, ``purchase``), 20
files each, laid out as time-ordered ingestion so that surrogate keys grow with
the file (the common real-world clustering).  A ``WHERE`` on ``session`` narrows
it to a *single* file; the test proves that single file's join-key ranges
propagate transitively (session -> purchase -> product/category), collapsing
every table from 20 files to just the one that can actually join.

The stats frames are synthesised directly in ``STATS_SCHEMA`` shape — the exact
artifact ``build_stats_file`` persists — so the pruning runs against genuine
min/max zone maps without the heavyweight write path.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import polars
import pytest

from supertable.data_classes import JoinEdge, PredInterval
from supertable.processing import STATS_SCHEMA
from supertable.utils.sql_parser import SQLParser
from supertable.engine.join_pruner import (
    JoinPrunePlan,
    _derive_join_range,
    _prune_by_occurrences,
    plan_file_pruning_for_query,
    prune_files_across_joins,
)

# Table keys as the parser/estimator emit them: (super.lower(), simple.lower()).
SUPER = "webshop"
CATEGORY = (SUPER, "category")
SESSION = (SUPER, "session")
PRODUCT = (SUPER, "product")
PURCHASE = (SUPER, "purchase")
ALL_KEYS = [CATEGORY, SESSION, PRODUCT, PURCHASE]

_DAY0 = datetime(2026, 1, 1)

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
# Stats-frame builders (STATS_SCHEMA shape)
# ---------------------------------------------------------------------------

def _rows_for_file(file_path: str, colspecs: Dict[str, Tuple]) -> List[dict]:
    """One STATS_SCHEMA row (single row group) per column of one file.

    *colspecs* maps a column to ``(lane, min, max)`` with lane in
    ``bigint`` / ``double`` / ``timestamp`` / ``string`` / ``unknown`` (the last
    yields a ``stats_available=False`` row — a real column whose footer carried
    no usable statistic).
    """
    rows: List[dict] = []
    for col, spec in colspecs.items():
        lane = spec[0]
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
        if lane == "bigint":
            row["min_bigint"], row["max_bigint"] = int(spec[1]), int(spec[2])
        elif lane == "double":
            row["min_double"], row["max_double"] = float(spec[1]), float(spec[2])
        elif lane == "timestamp":
            row["min_timestamp"], row["max_timestamp"] = spec[1], spec[2]
            row["logical_type"] = "TIMESTAMP_NTZ_MICROS"
        elif lane == "string":
            row["min_string"], row["max_string"] = str(spec[1]), str(spec[2])
        elif lane == "unknown":
            row["stats_available"] = False
        else:  # pragma: no cover - guard against typos in tests
            raise ValueError(f"unknown lane {lane!r}")
        rows.append(row)
    return rows


def _stats_df(rows: List[dict]) -> polars.DataFrame:
    return polars.DataFrame(rows, schema=STATS_SCHEMA)


def _build_webshop(n: int = 20):
    """Return ``(table_files, table_stats)`` for the 4-table, n-file scenario.

    File ``k`` of every table holds the same key band ``k`` so that a filter that
    isolates band ``k`` on one table can propagate to band ``k`` on the others:

      * ``session``  file k: session_id ``[k*1000, k*1000+999]``, event_time in day k
      * ``purchase`` file k: session_id band k, product_id ``[k*100,+99]``,
                             category_id ``[k*5,+4]``
      * ``product``  file k: product_id  ``[k*100, k*100+99]``
      * ``category`` file k: category_id ``[k*5,  k*5+4]``
    """
    files: Dict[Tuple[str, str], List[str]] = {t: [] for t in ALL_KEYS}
    rows: Dict[Tuple[str, str], List[dict]] = {t: [] for t in ALL_KEYS}

    for k in range(n):
        d_lo = _DAY0 + timedelta(days=k)
        d_hi = d_lo + timedelta(hours=23, minutes=59, seconds=59)

        fp = f"session/f{k:02d}.parquet"
        files[SESSION].append(fp)
        rows[SESSION] += _rows_for_file(fp, {
            "session_id": ("bigint", k * 1000, k * 1000 + 999),
            "event_time": ("timestamp", d_lo, d_hi),
        })

        fp = f"purchase/f{k:02d}.parquet"
        files[PURCHASE].append(fp)
        rows[PURCHASE] += _rows_for_file(fp, {
            "purchase_id": ("bigint", k * 5000, k * 5000 + 4999),
            "session_id": ("bigint", k * 1000, k * 1000 + 999),
            "product_id": ("bigint", k * 100, k * 100 + 99),
            "category_id": ("bigint", k * 5, k * 5 + 4),
            "purchase_time": ("timestamp", d_lo, d_hi),
        })

        fp = f"product/f{k:02d}.parquet"
        files[PRODUCT].append(fp)
        rows[PRODUCT] += _rows_for_file(fp, {
            "product_id": ("bigint", k * 100, k * 100 + 99),
            "price": ("double", 1.0, 999.0),
        })

        fp = f"category/f{k:02d}.parquet"
        files[CATEGORY].append(fp)
        rows[CATEGORY] += _rows_for_file(fp, {
            "category_id": ("bigint", k * 5, k * 5 + 4),
        })

    stats = {t: _stats_df(rows[t]) for t in ALL_KEYS}
    return files, stats


def _edge_set(edges: List[JoinEdge]):
    """Direction-independent representation for comparison."""
    return {
        frozenset({(e.left_table, e.left_col), (e.right_table, e.right_col)})
        for e in edges
    }


# ---------------------------------------------------------------------------
# The headline scenario
# ---------------------------------------------------------------------------

def test_scenario_time_filter_prunes_all_four_tables_to_one_file():
    files, stats = _build_webshop(20)

    plan = plan_file_pruning_for_query(SUPER, SCENARIO_QUERY, "duckdb", files, stats)

    # Every table starts at 20 files.
    for t in ALL_KEYS:
        assert plan.files_before[t] == 20

    # Day 2026-01-08 == band k=7. The WHERE isolates session file 7; its
    # session_id band propagates to purchase, whose product/category bands
    # propagate onward. All four collapse to the single band-7 file.
    assert plan.survivors[SESSION] == ["session/f07.parquet"]
    assert plan.survivors[PURCHASE] == ["purchase/f07.parquet"]
    assert plan.survivors[PRODUCT] == ["product/f07.parquet"]
    assert plan.survivors[CATEGORY] == ["category/f07.parquet"]

    assert plan.iterations >= 1
    assert plan.steps  # something actually happened
    # A transitive step must appear: purchase's band drives product.
    assert any("product" in s and "join" in s for s in plan.steps)


def test_scenario_parser_extracts_three_join_edges():
    parser = SQLParser(SUPER, SCENARIO_QUERY, "duckdb")
    edges = parser.get_join_edges()

    expected = {
        frozenset({(PURCHASE, "session_id"), (SESSION, "session_id")}),
        frozenset({(PURCHASE, "product_id"), (PRODUCT, "product_id")}),
        frozenset({(PURCHASE, "category_id"), (CATEGORY, "category_id")}),
    }
    assert _edge_set(edges) == expected


def test_scenario_where_isolates_single_session_file():
    """Literal pruning alone (no joins) already reduces session to one file."""
    files, stats = _build_webshop(20)
    parser = SQLParser(SUPER, SCENARIO_QUERY, "duckdb")
    lits = parser.get_predicate_constraints()

    survivors = _prune_by_occurrences(
        files[SESSION], stats[SESSION], lits[SESSION], allow_empty=True
    )
    assert survivors == ["session/f07.parquet"]


# ---------------------------------------------------------------------------
# Join-edge extraction edge cases
# ---------------------------------------------------------------------------

def test_join_edges_from_implicit_where_join():
    q = "SELECT a.x FROM webshop.orders a, webshop.customers b WHERE a.cid = b.id"
    edges = SQLParser(SUPER, q, "duckdb").get_join_edges()
    assert _edge_set(edges) == {
        frozenset({((SUPER, "orders"), "cid"), ((SUPER, "customers"), "id")})
    }


def test_join_edges_ignore_column_literal_predicates():
    q = ("SELECT a.x FROM webshop.orders a JOIN webshop.customers b "
         "ON a.cid = b.id WHERE a.amount = 5")
    edges = SQLParser(SUPER, q, "duckdb").get_join_edges()
    # Only the a.cid=b.id link — a.amount=5 is a literal predicate, not a join.
    assert _edge_set(edges) == {
        frozenset({((SUPER, "orders"), "cid"), ((SUPER, "customers"), "id")})
    }


def test_join_edges_skip_self_join():
    q = ("SELECT a.x FROM webshop.orders a JOIN webshop.orders b "
         "ON a.parent_id = b.id")
    edges = SQLParser(SUPER, q, "duckdb").get_join_edges()
    assert edges == []


def test_join_edges_dedupe_direction():
    q1 = "SELECT a.x FROM webshop.a a JOIN webshop.b b ON a.k = b.k"
    q2 = "SELECT a.x FROM webshop.a a JOIN webshop.b b ON b.k = a.k"
    e1 = SQLParser(SUPER, q1, "duckdb").get_join_edges()
    e2 = SQLParser(SUPER, q2, "duckdb").get_join_edges()
    assert len(e1) == 1 and len(e2) == 1
    assert _edge_set(e1) == _edge_set(e2)


# ---------------------------------------------------------------------------
# Range derivation
# ---------------------------------------------------------------------------

def test_derive_range_over_surviving_files():
    files, stats = _build_webshop(20)
    # Over ALL 20 purchase files, session_id spans [0, 19999].
    rng = _derive_join_range(stats[PURCHASE], files[PURCHASE], "session_id")
    assert rng == PredInterval("numeric", 0, True, 19999, True)
    # Over a single file, the band tightens.
    rng1 = _derive_join_range(stats[PURCHASE], ["purchase/f07.parquet"], "session_id")
    assert rng1 == PredInterval("numeric", 7000, True, 7999, True)


def test_derive_range_unbounded_when_any_file_lacks_stat():
    fp_ok = "t/ok.parquet"
    fp_bad = "t/bad.parquet"
    rows = _rows_for_file(fp_ok, {"k": ("bigint", 10, 20)})
    rows += _rows_for_file(fp_bad, {"k": ("unknown",)})
    df = _stats_df(rows)
    # A surviving file whose key stat is unavailable → range unknowable → None,
    # so the partner is NOT pruned (soundness: no false negative).
    assert _derive_join_range(df, [fp_ok, fp_bad], "k") is None
    # But over just the good file the range is derivable.
    assert _derive_join_range(df, [fp_ok], "k") == PredInterval("numeric", 10, True, 20, True)


def test_derive_range_missing_column_or_empty():
    files, stats = _build_webshop(3)
    assert _derive_join_range(stats[SESSION], files[SESSION], "nope") is None
    assert _derive_join_range(stats[SESSION], [], "session_id") is None
    assert _derive_join_range(None, files[SESSION], "session_id") is None


# ---------------------------------------------------------------------------
# Kernel behaviours: range (multi-file) pruning, soundness, empties
# ---------------------------------------------------------------------------

def test_range_join_prunes_to_multiple_overlapping_files():
    """A wider key band survives several partner files, not just one."""
    # source: one file with product_id band [700, 799]
    src_rows = _rows_for_file("src/f.parquet", {"product_id": ("bigint", 700, 799)})
    # dest: 20 files, each 40-wide → band [700,799] overlaps files 17,18,19.
    dst_files, dst_rows = [], []
    for k in range(20):
        fp = f"dst/f{k:02d}.parquet"
        dst_files.append(fp)
        dst_rows += _rows_for_file(fp, {"product_id": ("bigint", k * 40, k * 40 + 39)})

    src, dst = (SUPER, "src"), (SUPER, "dst")
    edge = JoinEdge(src, "product_id", dst, "product_id")
    plan = prune_files_across_joins(
        [edge], {}, {src: ["src/f.parquet"], dst: dst_files},
        {src: _stats_df(src_rows), dst: _stats_df(dst_rows)},
    )
    assert plan.survivors[dst] == ["dst/f17.parquet", "dst/f18.parquet", "dst/f19.parquet"]


def test_soundness_uncorrelated_key_is_not_pruned():
    """When a dimension's files all span the full key range, none can be
    dropped — the pruner must not invent separation that the zone maps don't
    show (no false pruning)."""
    files, stats = _build_webshop(20)
    # Rewrite product so EVERY file spans the whole product_id domain.
    rows = []
    for k in range(20):
        rows += _rows_for_file(f"product/f{k:02d}.parquet",
                               {"product_id": ("bigint", 0, 1999)})
    stats[PRODUCT] = _stats_df(rows)

    plan = plan_file_pruning_for_query(SUPER, SCENARIO_QUERY, "duckdb", files, stats)
    # session/purchase/category still collapse; product cannot be pruned.
    assert plan.survivors[SESSION] == ["session/f07.parquet"]
    assert plan.survivors[PURCHASE] == ["purchase/f07.parquet"]
    assert len(plan.survivors[PRODUCT]) == 20


def test_empty_result_when_bands_do_not_overlap():
    """A join key range that matches no partner file yields an empty survivor
    set when allow_empty=True (the correct 'join produces nothing' answer)."""
    src_rows = _rows_for_file("src/f.parquet", {"k": ("bigint", 5000, 5100)})
    dst_files, dst_rows = [], []
    for i in range(5):
        fp = f"dst/f{i}.parquet"
        dst_files.append(fp)
        dst_rows += _rows_for_file(fp, {"k": ("bigint", i * 100, i * 100 + 99)})
    src, dst = (SUPER, "src"), (SUPER, "dst")
    edge = JoinEdge(src, "k", dst, "k")
    args = ([edge], {}, {src: ["src/f.parquet"], dst: dst_files},
            {src: _stats_df(src_rows), dst: _stats_df(dst_rows)})

    plan = prune_files_across_joins(*args, allow_empty=True)
    assert plan.survivors[dst] == []
    # With allow_empty=False the guard keeps the full list instead.
    plan2 = prune_files_across_joins(*args, allow_empty=False)
    assert len(plan2.survivors[dst]) == 5


def test_prune_by_occurrences_case_insensitive_columns():
    """SQL identifier case need not match the stored footer column case."""
    rows = _rows_for_file("f.parquet", {"Session_ID": ("bigint", 100, 200)})
    df = _stats_df(rows)
    occ = {"session_id": PredInterval("numeric", 500, True, 600, True)}
    assert _prune_by_occurrences(["f.parquet"], df, [occ], allow_empty=True) == []
    occ_hit = {"session_id": PredInterval("numeric", 150, True, 160, True)}
    assert _prune_by_occurrences(["f.parquet"], df, [occ_hit], allow_empty=True) == ["f.parquet"]


def test_plan_summary_renders():
    files, stats = _build_webshop(5)
    plan = plan_file_pruning_for_query(SUPER, SCENARIO_QUERY, "duckdb", files, stats)
    text = plan.summary()
    assert "JoinPrunePlan" in text
    assert "webshop.session" in text
    assert plan.converged


# ---------------------------------------------------------------------------
# Union-of-intervals export: disjoint survivor bands stay disjoint
# ---------------------------------------------------------------------------

def test_disjoint_survivor_bands_prune_partner_files_between_them():
    """Two disjoint surviving bands must NOT be collapsed into one hull that
    spuriously retains every partner file lying between them."""
    src_rows = _rows_for_file("src/f00.parquet", {"k": ("bigint", 0, 99)})
    src_rows += _rows_for_file("src/f19.parquet", {"k": ("bigint", 1900, 1999)})
    dst_files, dst_rows = [], []
    for i in range(20):
        fp = f"dst/f{i:02d}.parquet"
        dst_files.append(fp)
        dst_rows += _rows_for_file(fp, {"k": ("bigint", i * 100, i * 100 + 99)})
    src, dst = (SUPER, "src"), (SUPER, "dst")
    edge = JoinEdge(src, "k", dst, "k")
    plan = prune_files_across_joins(
        [edge], {},
        {src: ["src/f00.parquet", "src/f19.parquet"], dst: dst_files},
        {src: _stats_df(src_rows), dst: _stats_df(dst_rows)},
    )
    # Only the two band-matching partner files survive — not the 18 between.
    assert plan.survivors[dst] == ["dst/f00.parquet", "dst/f19.parquet"]


def test_all_null_row_group_does_not_poison_the_export():
    """An all-NULL join-key row group holds no join keys (equi-joins never
    match NULL) — it must be skipped, not turn the whole export unbounded."""
    # src file: rg0 has k in [500,599]; rg1 is entirely NULL.
    rows = _rows_for_file("src/f.parquet", {"k": ("bigint", 500, 599)})
    null_row = {k: None for k in STATS_SCHEMA}
    null_row.update({
        "file_path": "src/f.parquet", "row_group_id": 1, "column_name": "k",
        "physical_type": "INT64", "logical_type": "", "null_count": 100,
        "row_group_rows": 100, "compressed_bytes": 10,
        "min_is_exact": True, "max_is_exact": True, "stats_available": False,
    })
    rows.append(null_row)
    dst_files, dst_rows = [], []
    for i in range(10):
        fp = f"dst/f{i}.parquet"
        dst_files.append(fp)
        dst_rows += _rows_for_file(fp, {"k": ("bigint", i * 100, i * 100 + 99)})
    src, dst = (SUPER, "src"), (SUPER, "dst")
    edge = JoinEdge(src, "k", dst, "k")
    plan = prune_files_across_joins(
        [edge], {}, {src: ["src/f.parquet"], dst: dst_files},
        {src: _stats_df(rows), dst: _stats_df(dst_rows)},
    )
    assert plan.survivors[dst] == ["dst/f5.parquet"]


def test_corrupt_null_cardinality_does_not_hide_unknown_join_keys():
    """Impossible footer cardinalities are uncertainty, not all-NULL proof."""
    src_rows = _rows_for_file(
        "src/known.parquet", {"k": ("bigint", 10, 10)}
    )
    corrupt = {k: None for k in STATS_SCHEMA}
    corrupt.update({
        "file_path": "src/corrupt.parquet",
        "row_group_id": 0,
        "column_name": "k",
        "physical_type": "INT64",
        "logical_type": "",
        # This cannot describe a valid row group.  The range is unknown and
        # must remain unbounded; the former >= check mislabeled it all-NULL.
        "null_count": 101,
        "row_group_rows": 100,
        "compressed_bytes": 10,
        "min_is_exact": True,
        "max_is_exact": True,
        "stats_available": False,
    })
    src_rows.append(corrupt)
    dst_rows = (
        _rows_for_file("dst/known.parquet", {"k": ("bigint", 10, 10)})
        + _rows_for_file("dst/could-match.parquet", {"k": ("bigint", 99, 99)})
    )
    src, dst = (SUPER, "src"), (SUPER, "dst")

    plan = prune_files_across_joins(
        [JoinEdge(src, "k", dst, "k")],
        {},
        {
            src: ["src/known.parquet", "src/corrupt.parquet"],
            dst: ["dst/known.parquet", "dst/could-match.parquet"],
        },
        {src: _stats_df(src_rows), dst: _stats_df(dst_rows)},
        allow_empty=False,
    )

    assert plan.survivors[dst] == [
        "dst/known.parquet", "dst/could-match.parquet"
    ]


# ---------------------------------------------------------------------------
# USING / alias-case edge extraction
# ---------------------------------------------------------------------------

def test_join_edges_from_using_two_tables():
    q = "SELECT a.x FROM webshop.orders a JOIN webshop.customers b USING (cid)"
    edges = SQLParser(SUPER, q, "duckdb").get_join_edges()
    assert _edge_set(edges) == {
        frozenset({((SUPER, "orders"), "cid"), ((SUPER, "customers"), "cid")})
    }
    assert edges[0].prune_left and edges[0].prune_right


def test_join_edges_using_chain_binds_only_the_first_hop():
    """Deeper USING hops would need schema knowledge to bind the column to the
    right left-side table — only the unambiguous first hop is translated."""
    q = ("SELECT a.x FROM webshop.a a JOIN webshop.b b USING (k) "
         "JOIN webshop.c c USING (k)")
    edges = SQLParser(SUPER, q, "duckdb").get_join_edges()
    assert _edge_set(edges) == {
        frozenset({((SUPER, "a"), "k"), ((SUPER, "b"), "k")})
    }


def test_join_edges_left_join_using_clears_preserved_side():
    q = "SELECT a.x FROM webshop.a a LEFT JOIN webshop.b b USING (k)"
    (edge,) = SQLParser(SUPER, q, "duckdb").get_join_edges()
    prunable = {
        edge.left_table: edge.prune_left,
        edge.right_table: edge.prune_right,
    }
    assert prunable[(SUPER, "a")] is False   # preserved side
    assert prunable[(SUPER, "b")] is True    # null-supplying side


def test_join_edges_alias_case_insensitive():
    """DuckDB identifiers are case-insensitive — a mixed-case qualifier must
    still resolve to its alias."""
    q = "SELECT A.x FROM webshop.orders a JOIN webshop.customers B ON A.cid = b.id"
    edges = SQLParser(SUPER, q, "duckdb").get_join_edges()
    assert _edge_set(edges) == {
        frozenset({((SUPER, "orders"), "cid"), ((SUPER, "customers"), "id")})
    }
