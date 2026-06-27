"""Read-path file pruning (consumer 5b) — differential & predicate-extraction suite.

The read path uses the external stats artifact to drop parquet files that
*provably* cannot satisfy a query's WHERE predicates, before the executor scans
them.  Pruning must be a pure performance optimisation: for **every** query the
result over the pruned file set must be byte-identical to the result over the
full file set.

This module proves that end-to-end against a real DuckDB engine:

  * **Predicate extraction** — :meth:`SQLParser.get_predicate_constraints`
    turns WHERE conjuncts into per-table, per-occurrence interval constraints,
    handling qualified/unqualified columns, BETWEEN / IN, OR-safety, casts,
    self-joins (two occurrences) and CTE/subquery scopes.
  * **Pruning engine** — :func:`prune_files_by_predicates` narrows a raw file
    list, with every "retain on uncertainty" guard and the union-across-
    occurrences rule for self-joins.
  * **Differential identity** — for a battery of SELECTs (single table, inner /
    left joins, subselects, CTEs, self-joins, OR, functions, multi-column AND,
    timestamps, strings, empty results) plus a randomized fuzz, the pruned and
    full results match exactly — and pruning actually fires where it should.
"""

from __future__ import annotations

import random
from datetime import datetime

import duckdb
import polars as pl
import pyarrow.parquet as pq
import pytest
import pandas as pd
from pandas.testing import assert_frame_equal

from supertable.processing import (
    STATS_SCHEMA,
    _stats_rows_for_metadata,
    prune_files_by_predicates,
)
from supertable.utils.sql_parser import SQLParser

_SUPER = "maindb"


# ---------------------------------------------------------------------------
# Real-parquet helpers (footer stats are the genuine min/max, like production)
# ---------------------------------------------------------------------------

def _write(tmp_path, name: str, df: pl.DataFrame, row_group_size: int | None = None) -> str:
    path = str(tmp_path / name)
    kwargs = {"write_statistics": True}
    if row_group_size is not None:
        kwargs["row_group_size"] = row_group_size
    pq.write_table(df.to_arrow(), path, **kwargs)
    return path


def _stats_for(paths: list[str]) -> pl.DataFrame:
    rows: list[dict] = []
    for p in paths:
        md = pq.read_metadata(p)
        rows.extend(_stats_rows_for_metadata(p, md))
    if not rows:
        return pl.DataFrame(schema=STATS_SCHEMA)
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


def _duckdb_result(query: str, table_files: dict[str, list[str]]) -> pd.DataFrame:
    """Run *query* with each table view backed by exactly *table_files[t]*."""
    con = duckdb.connect()
    try:
        for tbl, files in table_files.items():
            flist = "[" + ", ".join("'" + f + "'" for f in files) + "]"
            con.execute(f'CREATE VIEW "{tbl}" AS SELECT * FROM read_parquet({flist})')
        return con.execute(query).fetchdf()
    finally:
        con.close()


def _normalise(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.reset_index(drop=True)
    return df.sort_values(by=list(df.columns)).reset_index(drop=True)


def _prune_tables(
    query: str,
    table_files: dict[str, list[str]],
    stats_df: pl.DataFrame,
) -> dict[str, list[str]]:
    parser = SQLParser(_SUPER, query, "duckdb")
    constraints = parser.get_predicate_constraints()
    pruned: dict[str, list[str]] = {}
    for tbl, files in table_files.items():
        occ = constraints.get((_SUPER.lower(), tbl.lower()))
        pruned[tbl] = prune_files_by_predicates(files, stats_df, occ) if occ else files
    return pruned


def _assert_differential(query, table_files, stats_df):
    """Result over pruned files == result over all files; returns pruned map."""
    pruned = _prune_tables(query, table_files, stats_df)
    for tbl, files in pruned.items():
        assert set(files).issubset(set(table_files[tbl])), tbl
    res_all = _normalise(_duckdb_result(query, table_files))
    res_pruned = _normalise(_duckdb_result(query, pruned))
    assert_frame_equal(res_all, res_pruned, check_dtype=False)
    return pruned


# ---------------------------------------------------------------------------
# Range-partitioned dataset:  orders in 3 files by amount, 2 row groups each.
# ---------------------------------------------------------------------------

@pytest.fixture
def orders_ds(tmp_path):
    """orders split by amount into 3 files (1-100, 101-200, 201-300)."""
    def mk(lo, hi):
        ids = list(range(lo, hi + 1))
        return pl.DataFrame({
            "id": ids,
            "amount": ids,                      # amount == id for easy reasoning
            "region": [("US" if i % 2 else "EU") for i in ids],
        })
    f1 = _write(tmp_path, "o1.parquet", mk(1, 100), row_group_size=50)
    f2 = _write(tmp_path, "o2.parquet", mk(101, 200), row_group_size=50)
    f3 = _write(tmp_path, "o3.parquet", mk(201, 300), row_group_size=50)
    files = [f1, f2, f3]
    return {"files": {"orders": files}, "stats": _stats_for(files), "f": files}


# ===========================================================================
# 1.  Predicate extraction
# ===========================================================================

class TestPredicateExtraction:

    def _c(self, query):
        return SQLParser(_SUPER, query, "duckdb").get_predicate_constraints()

    def test_simple_range_qualified(self):
        c = self._c("SELECT * FROM orders o WHERE o.amount > 100 AND o.amount <= 150")
        (occ,) = c[(_SUPER, "orders")]
        iv = occ["amount"]
        assert iv.lane == "numeric"
        assert (iv.lo, iv.lo_incl, iv.hi, iv.hi_incl) == (100, False, 150, True)

    def test_unqualified_single_table(self):
        c = self._c("SELECT * FROM orders WHERE amount = 7")
        (occ,) = c[(_SUPER, "orders")]
        assert occ["amount"].lo == 7 and occ["amount"].hi == 7

    def test_unqualified_ambiguous_multi_table_ignored(self):
        # Bare `amount` with two tables → not attributed to either.
        c = self._c("SELECT * FROM orders o JOIN lines l ON o.id=l.oid WHERE amount > 5")
        assert all(occ == {} for occ in c.get((_SUPER, "orders"), [{}]))

    def test_between_and_in(self):
        c = self._c("SELECT * FROM orders WHERE amount BETWEEN 10 AND 20")
        (occ,) = c[(_SUPER, "orders")]
        assert (occ["amount"].lo, occ["amount"].hi) == (10, 20)
        c2 = self._c("SELECT * FROM orders WHERE region IN ('EU','US','APAC')")
        (occ2,) = c2[(_SUPER, "orders")]
        assert occ2["region"].lane == "string"
        assert (occ2["region"].lo, occ2["region"].hi) == ("APAC", "US")

    def test_top_level_or_yields_no_constraint(self):
        c = self._c("SELECT * FROM orders WHERE amount > 5 OR amount < 1")
        assert c[(_SUPER, "orders")] == [{}]

    def test_function_on_column_ignored(self):
        c = self._c("SELECT * FROM orders WHERE abs(amount) > 5")
        assert c[(_SUPER, "orders")] == [{}]

    def test_self_join_two_occurrences(self):
        c = self._c(
            "SELECT a.id FROM orders a JOIN orders b ON a.id=b.id "
            "WHERE a.amount > 250 AND b.amount < 50"
        )
        occs = c[(_SUPER, "orders")]
        assert len(occs) == 2
        los = sorted([(o["amount"].lo, o["amount"].hi) for o in occs], key=lambda x: x[0] if x[0] is not None else -1)
        assert los == [(None, 50), (250, None)]

    def test_cte_attributes_to_underlying_table(self):
        c = self._c(
            "WITH s AS (SELECT * FROM orders o WHERE o.amount > 100) "
            "SELECT * FROM s WHERE s.id < 5"
        )
        # The WHERE inside the CTE constrains orders; the outer s.id is on the CTE.
        occs = c[(_SUPER, "orders")]
        assert any(o.get("amount") and o["amount"].lo == 100 for o in occs)

    def test_timestamp_cast(self):
        c = self._c("SELECT * FROM orders WHERE ts < TIMESTAMP '2021-06-01'")
        (occ,) = c[(_SUPER, "orders")]
        assert occ["ts"].lane == "timestamp"
        assert occ["ts"].hi == datetime(2021, 6, 1)

    def test_multi_column_and(self):
        c = self._c("SELECT * FROM orders WHERE amount > 10 AND region = 'US'")
        (occ,) = c[(_SUPER, "orders")]
        assert set(occ.keys()) == {"amount", "region"}


# ===========================================================================
# 2.  Differential — single table
# ===========================================================================

class TestSingleTableDifferential:

    def test_gt_prunes_lower_files(self, orders_ds):
        q = "SELECT id, amount FROM orders WHERE amount > 250"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 1  # only o3 (201-300)

    def test_lt_prunes_upper_files(self, orders_ds):
        q = "SELECT id FROM orders WHERE amount < 60"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 1  # only o1

    def test_eq_prunes_to_single_file(self, orders_ds):
        q = "SELECT * FROM orders WHERE amount = 175"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 1  # o2 (101-200)

    def test_between_middle_file(self, orders_ds):
        q = "SELECT id FROM orders WHERE amount BETWEEN 120 AND 180"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 1

    def test_in_list_hull_is_conservative(self, orders_ds):
        # IN is pruned by its [min,max] hull: (40, 260) spans o2, so all three
        # files are retained — conservative but always correct.
        q = "SELECT id FROM orders WHERE amount IN (40, 260)"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 3

    def test_in_list_within_one_file(self, orders_ds):
        q = "SELECT id FROM orders WHERE amount IN (40, 60)"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 1  # hull [40,60] ⊂ o1

    def test_combined_and_intersection(self, orders_ds):
        q = "SELECT id FROM orders WHERE amount > 100 AND amount < 130"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 1  # only o2

    def test_string_equality_prunes_nothing_here(self, orders_ds):
        # region present in every file → no file pruned, still identical.
        q = "SELECT id FROM orders WHERE region = 'US'"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 3

    def test_or_disables_pruning(self, orders_ds):
        q = "SELECT id FROM orders WHERE amount < 50 OR amount > 250"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 3  # OR → no pruning

    def test_function_disables_pruning(self, orders_ds):
        q = "SELECT id FROM orders WHERE amount + 0 > 250"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 3

    def test_empty_result_retains_all_files(self, orders_ds):
        # Would prune everything → guard keeps all files, executor returns empty.
        q = "SELECT id FROM orders WHERE amount > 9999"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 3
        assert _duckdb_result(q, pruned).empty

    def test_row_group_level_retained(self, orders_ds):
        # o2 max is 200; amount>150 matches its 2nd row group → o2 retained.
        q = "SELECT id FROM orders WHERE amount > 150"
        pruned = _assert_differential(q, orders_ds["files"], orders_ds["stats"])
        assert len(pruned["orders"]) == 2  # o2 + o3


# ===========================================================================
# 3.  Differential — joins, subselects, self-joins
# ===========================================================================

@pytest.fixture
def join_ds(tmp_path):
    """orders (by amount) + customers (by cid), each range-partitioned."""
    def orders(lo, hi):
        ids = list(range(lo, hi + 1))
        return pl.DataFrame({"id": ids, "amount": ids, "cid": [i % 10 for i in ids]})

    def customers(lo, hi):
        ids = list(range(lo, hi + 1))
        return pl.DataFrame({"cid": ids, "age": [20 + (i % 40) for i in ids]})

    of = [
        _write(tmp_path, "oj1.parquet", orders(1, 100), row_group_size=50),
        _write(tmp_path, "oj2.parquet", orders(101, 200), row_group_size=50),
        _write(tmp_path, "oj3.parquet", orders(201, 300), row_group_size=50),
    ]
    cf = [
        _write(tmp_path, "cj1.parquet", customers(0, 4)),
        _write(tmp_path, "cj2.parquet", customers(5, 9)),
    ]
    files = {"orders": of, "customers": cf}
    return {"files": files, "stats": _stats_for(of + cf)}

    # (customers small so age comparisons exercise no-prune path)

class TestJoinDifferential:

    def test_inner_join_prunes_both_sides(self, join_ds):
        q = ("SELECT o.id, c.cid FROM orders o JOIN customers c ON o.cid=c.cid "
             "WHERE o.amount > 250 AND c.cid >= 5")
        pruned = _assert_differential(q, join_ds["files"], join_ds["stats"])
        assert len(pruned["orders"]) == 1     # o3
        assert len(pruned["customers"]) == 1  # cj2 (5-9)

    def test_left_join_where_on_right(self, join_ds):
        # WHERE on the right side effectively inner-joins; pruning right is safe.
        q = ("SELECT o.id, c.age FROM orders o LEFT JOIN customers c ON o.cid=c.cid "
             "WHERE c.cid < 5 AND o.amount > 150")
        pruned = _assert_differential(q, join_ds["files"], join_ds["stats"])
        assert len(pruned["customers"]) == 1  # cj1 (0-4)
        assert len(pruned["orders"]) == 2     # o2 + o3

    def test_left_join_predicate_only_in_on_not_pruned(self, join_ds):
        # No WHERE on customers → customers not pruned, result identical.
        q = ("SELECT o.id, c.age FROM orders o LEFT JOIN customers c "
             "ON o.cid=c.cid AND c.cid < 3 WHERE o.amount > 250")
        pruned = _assert_differential(q, join_ds["files"], join_ds["stats"])
        assert len(pruned["customers"]) == 2  # untouched (ON-only)
        assert len(pruned["orders"]) == 1

    def test_subselect_in_where(self, join_ds):
        q = ("SELECT id FROM orders WHERE amount > 250 "
             "AND cid IN (SELECT cid FROM customers WHERE cid >= 5)")
        pruned = _assert_differential(q, join_ds["files"], join_ds["stats"])
        assert len(pruned["orders"]) == 1
        assert len(pruned["customers"]) == 1

    def test_cte_pipeline(self, join_ds):
        q = ("WITH big AS (SELECT * FROM orders WHERE amount > 150) "
             "SELECT b.id, c.age FROM big b JOIN customers c ON b.cid=c.cid "
             "WHERE c.cid < 5")
        pruned = _assert_differential(q, join_ds["files"], join_ds["stats"])
        assert len(pruned["orders"]) == 2     # o2 + o3
        assert len(pruned["customers"]) == 1  # cj1

    def test_self_join_union_semantics(self, join_ds):
        # a needs amount>250 (o3); b needs amount<50 (o1).  Union keeps BOTH;
        # a naive intersection would wrongly drop o1 and corrupt the result.
        q = ("SELECT a.id AS aid, b.id AS bid FROM orders a JOIN orders b "
             "ON a.cid=b.cid WHERE a.amount > 250 AND b.amount < 50")
        pruned = _assert_differential(q, join_ds["files"], join_ds["stats"])
        kept = {p.rsplit("/", 1)[-1] for p in pruned["orders"]}
        assert "oj1.parquet" in kept and "oj3.parquet" in kept
        assert "oj2.parquet" not in kept  # excluded by both occurrences

    def test_union_all_query(self, join_ds):
        q = ("SELECT id FROM orders WHERE amount < 50 "
             "UNION ALL SELECT id FROM orders WHERE amount > 250")
        # Two occurrences of orders (one per branch) → union keeps o1 and o3.
        pruned = _assert_differential(q, join_ds["files"], join_ds["stats"])
        kept = {p.rsplit("/", 1)[-1] for p in pruned["orders"]}
        assert kept == {"oj1.parquet", "oj3.parquet"}


# ===========================================================================
# 4.  Differential — timestamp & null edge cases
# ===========================================================================

class TestEdgeDifferential:

    @pytest.fixture
    def ts_ds(self, tmp_path):
        def mk(year):
            return pl.DataFrame({
                "id": list(range(year * 10, year * 10 + 5)),
                "ts": [datetime(year, m, 1) for m in range(1, 6)],
            })
        files = [
            _write(tmp_path, "t2019.parquet", mk(2019)),
            _write(tmp_path, "t2020.parquet", mk(2020)),
            _write(tmp_path, "t2021.parquet", mk(2021)),
        ]
        return {"files": {"events": files}, "stats": _stats_for(files)}

    def test_timestamp_pruning(self, ts_ds):
        q = "SELECT id FROM events WHERE ts >= TIMESTAMP '2021-01-01'"
        pruned = _assert_differential(q, ts_ds["files"], ts_ds["stats"])
        assert len(pruned["events"]) == 1  # only t2021

    def test_timestamp_between(self, ts_ds):
        q = ("SELECT id FROM events WHERE ts BETWEEN TIMESTAMP '2020-01-01' "
             "AND TIMESTAMP '2020-12-31'")
        pruned = _assert_differential(q, ts_ds["files"], ts_ds["stats"])
        assert len(pruned["events"]) == 1

    def test_nulls_in_predicate_column(self, tmp_path):
        # A file whose predicate column is entirely NULL: footer min/max are
        # absent → stats can't exclude → file retained, result still identical.
        f1 = _write(tmp_path, "n1.parquet",
                    pl.DataFrame({"id": [1, 2, 3], "v": [None, None, None]},
                                 schema={"id": pl.Int64, "v": pl.Int64}))
        f2 = _write(tmp_path, "n2.parquet",
                    pl.DataFrame({"id": [4, 5, 6], "v": [10, 20, 30]}))
        files = {"vals": [f1, f2]}
        stats = _stats_for([f1, f2])
        q = "SELECT id FROM vals WHERE v > 15"
        _assert_differential(q, files, stats)


# ===========================================================================
# 5.  Randomized fuzz — pruned result must always equal full result
# ===========================================================================

class TestFuzzDifferential:

    @pytest.fixture
    def rand_ds(self, tmp_path):
        rng = random.Random(20260627)
        files = []
        for fi in range(6):
            lo = fi * 100
            n = rng.randint(20, 60)
            vals = [rng.randint(lo, lo + 99) for _ in range(n)]
            df = pl.DataFrame({
                "id": list(range(fi * 1000, fi * 1000 + n)),
                "amount": vals,
                "score": [rng.randint(0, 50) for _ in range(n)],
            })
            files.append(_write(tmp_path, f"r{fi}.parquet", df, row_group_size=15))
        return {"files": {"t": files}, "stats": _stats_for(files)}

    def test_fuzz_single_column(self, rand_ds):
        rng = random.Random(7)
        ops = ["=", ">", ">=", "<", "<=", "BETWEEN", "IN"]
        for _ in range(60):
            op = rng.choice(ops)
            if op == "BETWEEN":
                a, b = sorted([rng.randint(-50, 650), rng.randint(-50, 650)])
                pred = f"amount BETWEEN {a} AND {b}"
            elif op == "IN":
                vals = [rng.randint(-50, 650) for _ in range(rng.randint(1, 4))]
                pred = f"amount IN ({', '.join(map(str, vals))})"
            else:
                pred = f"amount {op} {rng.randint(-50, 650)}"
            q = f"SELECT id, amount FROM t WHERE {pred}"
            _assert_differential(q, rand_ds["files"], rand_ds["stats"])

    def test_fuzz_two_columns_and(self, rand_ds):
        rng = random.Random(11)
        for _ in range(40):
            p1 = f"amount > {rng.randint(-50, 650)}"
            p2 = f"score <= {rng.randint(0, 60)}"
            q = f"SELECT id FROM t WHERE {p1} AND {p2}"
            _assert_differential(q, rand_ds["files"], rand_ds["stats"])
