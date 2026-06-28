"""Manual, result-asserting scenarios for the single-shot write + ad-hoc queries.

Two self-contained tests you can run individually and watch the asserted results
print to the screen.  Run with ``-s`` so the prints are not captured:

  # scenario 1 — the single write produces exactly the ds=1 dataset
  STORAGE_TYPE=LOCAL PYTHONPATH=. .venv/bin/python -m pytest \
      tests/characterization/test_single_write_queries.py::test_single_write_result -s

  # scenario 2 — aggregation / filter / group-by / having / distinct / ordering
  STORAGE_TYPE=LOCAL PYTHONPATH=. .venv/bin/python -m pytest \
      tests/characterization/test_single_write_queries.py::test_queries_on_single_dataset -s

Both mirror ``supertable/demo/quickstart/s02_02_write_single_data.py``: they write
``get_dummy_data(1)`` with ``overwrite_columns=["day"]`` into a fresh, empty
``facts`` table (the autouse hermetic fixture gives each test its own storage +
fake Redis, so the table always starts empty).

The ds=1 dataset under test:
    day         client   value  datastream_name
    2023-01-01  client1     11  First partition
    2023-01-02  client1     20  First partition
    2023-01-03  client1     30  First partition
    2023-01-04  client1     40  First partition
    2023-01-05  client2     50  First partition
"""

from __future__ import annotations

from typing import List, Tuple

import polars as pl

from supertable.data_reader import DataReader, engine
from supertable.data_writer import DataWriter
from supertable.demo.quickstart.dummy_data import get_dummy_data
from supertable.super_table import SuperTable

ORG = "kladna-soft"
SUPER = "demo"
SIMPLE = "facts"
ROLE = "superadmin"

# write() returns (columns, rows, inserted, deleted).
EXPECTED_WRITE_RESULT = (5, 5, 5, 0)

# The full ds=1 dataset, ordered by day.
EXPECTED_ROWS: List[Tuple[str, str, int, str]] = [
    ("2023-01-01", "client1", 11, "First partition"),
    ("2023-01-02", "client1", 20, "First partition"),
    ("2023-01-03", "client1", 30, "First partition"),
    ("2023-01-04", "client1", 40, "First partition"),
    ("2023-01-05", "client2", 50, "First partition"),
]


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _write_single():
    """Write get_dummy_data(1) exactly as s02_02_write_single_data.py does."""
    SuperTable(SUPER, ORG)  # bootstrap super table + default superadmin role/user
    dw = DataWriter(super_name=SUPER, organization=ORG)
    return dw.write(
        role_name=ROLE,
        simple_name=SIMPLE,
        data=get_dummy_data(1),
        overwrite_columns=["day"],
        lineage={
            "source_type": "manual",
            "source_id": "single_data_demo",
            "source_tables": ["dummy_data.ds=1"],
        },
    )


def _query(sql: str) -> pl.DataFrame:
    dr = DataReader(super_name=SUPER, organization=ORG, query=sql)
    df, status, msg = dr.execute(role_name=ROLE, with_scan=False, engine=engine.AUTO)
    assert str(status).endswith("OK"), f"query failed: {status} / {msg}\nSQL: {sql}"
    return df if isinstance(df, pl.DataFrame) else pl.from_pandas(df)


def _rows(df: pl.DataFrame, cols) -> List[Tuple]:
    return [tuple(r) for r in df.select(cols).iter_rows()]


def _scalar(df: pl.DataFrame, col: str):
    return df.get_column(col)[0]


# --------------------------------------------------------------------------- #
# per-query checkers — each returns (passed, got_repr)
# --------------------------------------------------------------------------- #
def _chk_count_all(df):
    return int(_scalar(df, "n")) == 5, f"n={_scalar(df, 'n')}"


def _chk_sum(df):
    return int(_scalar(df, "total")) == 151, f"total={_scalar(df, 'total')}"


def _chk_avg(df):
    v = float(_scalar(df, "mean"))
    return abs(v - 30.2) < 1e-6, f"mean={v}"


def _chk_min_max(df):
    lo, hi = int(_scalar(df, "lo")), int(_scalar(df, "hi"))
    return (lo == 11 and hi == 50), f"lo={lo} hi={hi}"


def _chk_group_by_client(df):
    d = {r[0]: (int(r[1]), int(r[2])) for r in _rows(df, ["client", "n", "total"])}
    ok = d.get("client1") == (4, 101) and d.get("client2") == (1, 50)
    return ok, str(d)


def _chk_filter_gt_25(df):
    got = [(r[0], int(r[1])) for r in _rows(df, ["day", "value"])]
    want = [("2023-01-03", 30), ("2023-01-04", 40), ("2023-01-05", 50)]
    return got == want, str(got)


def _chk_filter_client2(df):
    return int(_scalar(df, "n")) == 1, f"n={_scalar(df, 'n')}"


def _chk_top2_by_value(df):
    got = [(r[0], int(r[1])) for r in _rows(df, ["day", "value"])]
    want = [("2023-01-05", 50), ("2023-01-04", 40)]
    return got == want, str(got)


def _chk_distinct_stream(df):
    got = [r[0] for r in _rows(df, ["ds"])]
    return (df.height == 1 and got == ["First partition"]), str(got)


def _chk_having_sum(df):
    got = [(r[0], int(r[1])) for r in _rows(df, ["client", "total"])]
    return got == [("client1", 101)], str(got)


def _chk_between_dates(df):
    return int(_scalar(df, "n")) == 3, f"n={_scalar(df, 'n')}"


def _chk_arithmetic(df):
    return int(_scalar(df, "doubled")) == 302, f"doubled={_scalar(df, 'doubled')}"


def _chk_string_upper_like(df):
    return int(_scalar(df, "n")) == 5, f"n={_scalar(df, 'n')}"


def _chk_string_length(df):
    ok = df.height == 1 and int(_scalar(df, "len")) == 7
    return ok, f"len={_scalar(df, 'len')} rows={df.height}"


def _chk_null_count_value(df):
    return int(_scalar(df, "nulls")) == 0, f"nulls={_scalar(df, 'nulls')}"


def _chk_nullif_client(df):
    return int(_scalar(df, "n")) == 4, f"n={_scalar(df, 'n')}"


def _chk_conditional_agg(df):
    return int(_scalar(df, "big_sum")) == 120, f"big_sum={_scalar(df, 'big_sum')}"


def _chk_in_clause(df):
    return int(_scalar(df, "n")) == 2, f"n={_scalar(df, 'n')}"


def _chk_self_join(df):
    # client1 (4 rows) x itself = 16, client2 (1 row) x itself = 1 -> 17 pairs.
    return int(_scalar(df, "n")) == 17, f"n={_scalar(df, 'n')}"


def _chk_cte_join_region(df):
    d = {r[0]: int(r[1]) for r in _rows(df, ["region", "n"])}
    return d == {"EU": 4, "US": 1}, str(d)


def _chk_datetime_month(df):
    return int(_scalar(df, "n")) == 5, f"n={_scalar(df, 'n')}"


def _chk_datetime_distinct_months(df):
    return int(_scalar(df, "months")) == 1, f"months={_scalar(df, 'months')}"


QUERY_CASES = [
    ("count_all",
     f"SELECT count(*) AS n FROM {SIMPLE}",
     "n=5", _chk_count_all),
    ("sum_value",
     f"SELECT sum(value) AS total FROM {SIMPLE}",
     "total=151", _chk_sum),
    ("avg_value",
     f"SELECT avg(value) AS mean FROM {SIMPLE}",
     "mean=30.2", _chk_avg),
    ("min_max",
     f"SELECT min(value) AS lo, max(value) AS hi FROM {SIMPLE}",
     "lo=11 hi=50", _chk_min_max),
    ("group_by_client",
     f"SELECT client, count(*) AS n, sum(value) AS total FROM {SIMPLE} "
     f"GROUP BY client ORDER BY client",
     "client1=(4,101), client2=(1,50)", _chk_group_by_client),
    ("filter_value_gt_25",
     f"SELECT day, value FROM {SIMPLE} WHERE value > 25 ORDER BY value",
     "[(01-03,30),(01-04,40),(01-05,50)]", _chk_filter_gt_25),
    ("filter_client2",
     f"SELECT count(*) AS n FROM {SIMPLE} WHERE client = 'client2'",
     "n=1", _chk_filter_client2),
    ("top2_by_value_desc",
     f"SELECT day, value FROM {SIMPLE} ORDER BY value DESC LIMIT 2",
     "[(01-05,50),(01-04,40)]", _chk_top2_by_value),
    ("distinct_datastream",
     f"SELECT DISTINCT datastream_name AS ds FROM {SIMPLE}",
     "['First partition']", _chk_distinct_stream),
    ("having_sum_gt_60",
     f"SELECT client, sum(value) AS total FROM {SIMPLE} "
     f"GROUP BY client HAVING sum(value) > 60 ORDER BY client",
     "[(client1,101)]", _chk_having_sum),
    ("between_dates",
     f"SELECT count(*) AS n FROM {SIMPLE} "
     f"WHERE day BETWEEN '2023-01-02' AND '2023-01-04'",
     "n=3", _chk_between_dates),
    ("arithmetic_value_x2",
     f"SELECT sum(value * 2) AS doubled FROM {SIMPLE}",
     "doubled=302", _chk_arithmetic),
    # --- string functions ---
    ("string_upper_like",
     f"SELECT count(*) AS n FROM {SIMPLE} WHERE upper(client) LIKE 'CLIENT%'",
     "n=5", _chk_string_upper_like),
    ("string_length",
     f"SELECT DISTINCT length(client) AS len FROM {SIMPLE}",
     "len=7 (single row)", _chk_string_length),
    # --- NULL handling ---
    ("null_count_value",
     f"SELECT count(*) - count(value) AS nulls FROM {SIMPLE}",
     "nulls=0", _chk_null_count_value),
    ("nullif_client",
     f"SELECT count(NULLIF(client, 'client2')) AS n FROM {SIMPLE}",
     "n=4", _chk_nullif_client),
    # --- conditional aggregation ---
    ("conditional_agg_case",
     f"SELECT sum(CASE WHEN value >= 30 THEN value ELSE 0 END) AS big_sum FROM {SIMPLE}",
     "big_sum=120", _chk_conditional_agg),
    # --- IN list ---
    ("in_clause",
     f"SELECT count(*) AS n FROM {SIMPLE} WHERE value IN (11, 50)",
     "n=2", _chk_in_clause),
    # --- joins ---
    ("self_join_same_client",
     f"SELECT count(*) AS n FROM {SIMPLE} a JOIN {SIMPLE} b ON a.client = b.client",
     "n=17", _chk_self_join),
    ("cte_join_region",
     f"WITH region(client, region) AS (VALUES ('client1','EU'), ('client2','US')) "
     f"SELECT r.region, count(*) AS n FROM {SIMPLE} f JOIN region r "
     f"ON f.client = r.client GROUP BY r.region ORDER BY r.region",
     "EU=4, US=1", _chk_cte_join_region),
    # --- date/time functions ---
    ("datetime_extract_month",
     f"SELECT count(*) AS n FROM {SIMPLE} WHERE extract('month' FROM datetime) = 1",
     "n=5", _chk_datetime_month),
    ("datetime_distinct_months",
     f"SELECT count(DISTINCT date_trunc('month', datetime)) AS months FROM {SIMPLE}",
     "months=1", _chk_datetime_distinct_months),
]


# --------------------------------------------------------------------------- #
# scenario 1 — the single write
# --------------------------------------------------------------------------- #
def test_single_write_result():
    result = _write_single()
    columns, rows, inserted, deleted = result

    print("\n" + "=" * 64)
    print("scenario 1: single write (mirrors s02_02_write_single_data)")
    print("=" * 64)
    print(f"write() -> columns={columns} rows={rows} "
          f"inserted={inserted} deleted={deleted}")
    print("expected   columns=5 rows=5 inserted=5 deleted=0")

    assert result == EXPECTED_WRITE_RESULT

    df = _query(
        f"SELECT day, client, value, datastream_name FROM {SIMPLE} ORDER BY day"
    )
    print("\ndata in table after write:")
    with pl.Config(tbl_rows=20):
        print(df)

    assert df.height == 5
    got = _rows(df, ["day", "client", "value", "datastream_name"])
    got = [(d, c, int(v), s) for (d, c, v, s) in got]
    assert got == EXPECTED_ROWS
    print("\nASSERT OK: write result == (5,5,5,0) and 5 rows match ds=1 exactly")


# --------------------------------------------------------------------------- #
# scenario 2 — ad-hoc queries (aggregation / filter / ordering / ...)
# --------------------------------------------------------------------------- #
def test_queries_on_single_dataset():
    _write_single()

    print("\n" + "=" * 64)
    print("scenario 2: ad-hoc queries on the ds=1 dataset")
    print("=" * 64)

    failures: List[str] = []
    for name, sql, want, check in QUERY_CASES:
        df = _query(sql)
        passed, got = check(df)
        print("-" * 64)
        print(f"[{'PASS' if passed else 'FAIL'}] {name}")
        print(f"  SQL : {sql}")
        print(f"  want: {want}")
        print(f"  got : {got}")
        with pl.Config(tbl_rows=20, tbl_cols=10):
            print(df)
        if not passed:
            failures.append(name)

    print("=" * 64)
    print(f"{len(QUERY_CASES) - len(failures)}/{len(QUERY_CASES)} query cases passed")
    assert not failures, f"failed query cases: {failures}"
