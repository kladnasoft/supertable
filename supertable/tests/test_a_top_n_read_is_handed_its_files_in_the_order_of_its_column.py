"""
A read that takes its first rows by a column is handed its files in that order.

``SELECT ... FROM t ORDER BY ts DESC LIMIT n`` has no predicate, so no file is
pruned, and DuckDB reads a table's files in the order it is given them: the
order they were written in, oldest first.  Its top-N operator skips a row group
once it holds *n* rows that beat the group's bounds, which never happens while
every file it opens next holds newer rows than all before it.  The whole table
was read for *n* rows: 1.5 s for 1,000 of 10,000,000 rows on two threads, and
85 ms with the same files newest first.

So for that shape (``SQLParser.get_scan_order``) the estimator works out the
files in the order of the column's stored bounds
(``processing.order_files_by_stats``) and the DuckDB engine reads them so.

The order of the files never decides which rows come back: the query's own
ORDER BY does.  It is seen in one place only.  A reader of several files takes
each column's name, and for ``SELECT *`` its position, from the first file
that has it.  The files are therefore reordered only when the statistics show
that this comes out the same either way, and what the rule assumes about
DuckDB is checked here against DuckDB itself.
"""

from __future__ import annotations

import decimal
import itertools
import os
import random
import sys
from datetime import date, datetime, timedelta

import duckdb
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import supertable.processing as processing
from supertable.engine.data_estimator import DataEstimator
from supertable.processing import (
    STATS_SCHEMA,
    _columns_read_together,
    _column_sequences,
    extract_stats_rows,
    order_files_by_stats,
)
from supertable.utils.sql_parser import SQLParser

FILES = [f"acme/lake/orders/data/part-{i}.parquet" for i in range(5)]


@pytest.fixture(autouse=True)
def nothing_kept():
    processing.forget_stats_derived()
    yield
    processing.forget_stats_derived()


def _row(path, column, group=0, **fields):
    row = {"file_path": path, "row_group_id": group, "column_name": column,
           "stats_available": any(k.startswith("min_") for k in fields)}
    row.update(fields)
    return row


def _stats(files=FILES, columns=("id", "ts", "note"), groups=1) -> pl.DataFrame:
    """Files written one after another: ids and days rise from file to file."""
    rows = []
    for index, path in enumerate(files):
        for group in range(groups):
            low = index * 100 + group * 50
            for column in columns:
                if column == "id":
                    rows.append(_row(path, "id", group, min_bigint=low, max_bigint=low + 49))
                elif column == "ts":
                    day = datetime(2026, 1, 1) + timedelta(days=index, hours=group)
                    rows.append(_row(path, "ts", group, min_timestamp=day,
                                     max_timestamp=day + timedelta(minutes=59)))
                else:
                    rows.append(_row(path, column, group, min_string="a", max_string="z"))
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


def _frame(rows) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=STATS_SCHEMA)


# --------------------------------------------------------------------------
# The shape of the query
# --------------------------------------------------------------------------

@pytest.mark.parametrize("query, expected", [
    ("SELECT * FROM orders ORDER BY ts DESC LIMIT 10", ("ts", True)),
    ("SELECT * FROM orders ORDER BY ts DESC, id DESC LIMIT 1000", ("ts", True)),
    ("SELECT id, note FROM orders WHERE id > 3 ORDER BY ts LIMIT 10 OFFSET 5", ("ts", False)),
    ("SELECT id FROM orders ORDER BY ts ASC LIMIT 1", ("ts", False)),
    ("SELECT id FROM orders AS o ORDER BY o.ts DESC FETCH FIRST 5 ROWS ONLY", ("ts", True)),
    ("SELECT id FROM orders ORDER BY orders.ts DESC LIMIT 5", ("ts", True)),
    ("SELECT id FROM orders ORDER BY (ts) DESC LIMIT 5", ("ts", True)),
    ("SELECT id FROM orders ORDER BY TS DESC NULLS LAST LIMIT 5", ("TS", True)),
    # the select list names the column itself, or names it something else
    ("SELECT ts AS ts, id FROM orders ORDER BY ts LIMIT 5", ("ts", False)),
    ("SELECT ts AS happened, id FROM orders ORDER BY ts LIMIT 5", ("ts", False)),
    ("SELECT id + 1 AS next_id, upper(note) FROM orders ORDER BY id DESC LIMIT 5", ("id", True)),
    ("SELECT * EXCLUDE (note) FROM orders ORDER BY id DESC LIMIT 5", ("id", True)),
])
def test_a_query_that_takes_its_first_rows_by_a_column_names_it(query, expected):
    assert SQLParser("lake", query, "duckdb").get_scan_order() == {("lake", "orders"): expected}


def test_the_table_is_named_with_its_own_lake_in_lower_case():
    parser = SQLParser("lake", "SELECT id FROM Other.Orders ORDER BY id DESC LIMIT 5", "duckdb")
    assert parser.get_scan_order() == {("other", "orders"): ("id", True)}


@pytest.mark.parametrize("query", [
    # nothing pins the order, or every row is read anyway
    "SELECT * FROM orders",
    "SELECT * FROM orders LIMIT 5",
    "SELECT * FROM orders ORDER BY ts DESC",
    "SELECT * FROM orders WHERE id = 5",
    # the first key is not a column of the table
    "SELECT * FROM orders ORDER BY ts + 1 LIMIT 5",
    "SELECT * FROM orders ORDER BY -id LIMIT 5",
    "SELECT * FROM orders ORDER BY 1 LIMIT 5",
    "SELECT * FROM orders ORDER BY ALL LIMIT 5",
    "SELECT * FROM orders ORDER BY upper(note) LIMIT 5",
    "SELECT * FROM orders ORDER BY address.city LIMIT 5",          # a field of a struct
    "SELECT * FROM orders ORDER BY other.ts LIMIT 5",
    # the select list gives the name to something else: ORDER BY sorts by that
    "SELECT -ts AS ts FROM orders ORDER BY ts DESC LIMIT 5",
    "SELECT id AS ts FROM orders ORDER BY ts LIMIT 5",
    "SELECT ts AS happened, id AS ts FROM orders ORDER BY ts LIMIT 5",
    "SELECT * REPLACE (-ts AS ts) FROM orders ORDER BY ts DESC LIMIT 5",
    "SELECT * RENAME (id AS ts) FROM orders ORDER BY ts DESC LIMIT 5",
    "SELECT id FROM orders AS o(ts, id) ORDER BY ts LIMIT 5",
    # rows are folded together, or their answer depends on the order they arrive in
    "SELECT DISTINCT id FROM orders ORDER BY id LIMIT 5",
    "SELECT DISTINCT ON (note) id FROM orders ORDER BY id LIMIT 5",
    "SELECT id, count(*) FROM orders GROUP BY id ORDER BY id LIMIT 5",
    "SELECT max(id) FROM orders ORDER BY ts LIMIT 5",
    "SELECT first(id) FROM orders ORDER BY ts LIMIT 5",
    "SELECT id, row_number() OVER () FROM orders ORDER BY ts LIMIT 5",
    "SELECT id, sum(id) OVER (ORDER BY ts) FROM orders ORDER BY ts LIMIT 5",
    "SELECT id FROM orders QUALIFY row_number() OVER (PARTITION BY note ORDER BY ts) = 1 ORDER BY ts LIMIT 5",
    "SELECT id FROM orders USING SAMPLE 10 ROWS ORDER BY ts LIMIT 5",
    "SELECT id FROM orders TABLESAMPLE (10 PERCENT) ORDER BY ts LIMIT 5",
    # the table is read more than once, or is not the only source
    "SELECT o.id FROM orders o JOIN customers c ON o.id = c.id ORDER BY o.ts LIMIT 5",
    "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id ORDER BY a.ts LIMIT 5",
    "SELECT id FROM orders, customers ORDER BY ts LIMIT 5",
    "SELECT id FROM orders WHERE id IN (SELECT id FROM customers) ORDER BY ts LIMIT 5",
    "SELECT id FROM orders WHERE id IN (SELECT id FROM orders) ORDER BY ts LIMIT 5",
    "SELECT id, (SELECT max(id) FROM orders) FROM orders ORDER BY ts LIMIT 5",
    "SELECT id FROM (SELECT * FROM orders) x ORDER BY ts LIMIT 5",
    "WITH c AS (SELECT * FROM orders) SELECT id FROM c ORDER BY ts LIMIT 5",
    "WITH c AS (SELECT 1 AS one) SELECT id FROM orders ORDER BY ts LIMIT 5",
    "SELECT id FROM orders UNION ALL SELECT id FROM orders ORDER BY id LIMIT 5",
    "(SELECT id FROM orders ORDER BY ts LIMIT 5) UNION ALL (SELECT id FROM orders ORDER BY ts LIMIT 5)",
])
def test_every_other_query_names_nothing(query):
    assert SQLParser("lake", query, "duckdb").get_scan_order() == {}


def test_a_statement_that_cannot_be_walked_names_nothing(monkeypatch):
    parser = SQLParser("lake", "SELECT * FROM orders ORDER BY ts DESC LIMIT 5", "duckdb")
    assert parser.get_scan_order() != {}
    monkeypatch.setattr(parser, "_scan_order", lambda: 1 / 0)
    assert parser.get_scan_order() == {}


def test_what_the_shape_leans_on_in_duckdb():
    """An unqualified ORDER BY name is the select list's before it is the
    table's, and an aggregate cannot be ordered by a bare column: no function
    the parser does not know as an aggregate can pass for the shape."""
    con = duckdb.connect()
    con.execute("CREATE TABLE orders AS SELECT * FROM (VALUES (1, 10), (2, 5), (3, 1)) v(ts, id)")
    assert con.execute("SELECT -ts AS ts FROM orders ORDER BY ts DESC LIMIT 1").fetchall() == [(-1,)]
    assert con.execute("SELECT -ts AS ts FROM orders ORDER BY orders.ts DESC LIMIT 1").fetchall() == [(-3,)]
    assert con.execute(
        "SELECT * REPLACE (-ts AS ts) FROM orders ORDER BY ts DESC LIMIT 1").fetchall() == [(-1, 10)]
    assert con.execute("SELECT id FROM orders AS o(id, ts) ORDER BY ts LIMIT 1").fetchall() == [(3,)]
    for aggregate in ("list(id)", "max(id)", "product(id)", "histogram(id)"):
        with pytest.raises(duckdb.BinderException, match="GROUP BY"):
            con.execute(f"SELECT {aggregate} FROM orders ORDER BY ts LIMIT 5")


# --------------------------------------------------------------------------
# The order
# --------------------------------------------------------------------------

def test_descending_the_file_with_the_highest_value_comes_first():
    stats = _stats()
    assert order_files_by_stats(FILES, stats, "id", True, True) == FILES[::-1]
    assert order_files_by_stats(FILES, stats, "ts", True, False) == FILES[::-1]
    # whichever way they are listed
    mixed = [FILES[2], FILES[0], FILES[4], FILES[1], FILES[3]]
    assert order_files_by_stats(mixed, stats, "id", True, True) == FILES[::-1]


def test_ascending_the_file_with_the_lowest_value_comes_first():
    stats = _stats()
    mixed = [FILES[2], FILES[0], FILES[4], FILES[1], FILES[3]]
    assert order_files_by_stats(mixed, stats, "id", False, True) == FILES
    assert order_files_by_stats(mixed, stats, "ts", False, True) == FILES


def test_files_already_in_order_are_handed_back_as_they_are():
    stats = _stats()
    assert order_files_by_stats(FILES, stats, "id", False, True) is FILES
    newest_first = FILES[::-1]
    assert order_files_by_stats(newest_first, stats, "ts", True, True) is newest_first


def test_the_list_that_was_given_is_not_changed():
    stats, given = _stats(), list(FILES)
    assert order_files_by_stats(given, stats, "id", True, True) == FILES[::-1]
    assert given == FILES


def test_only_the_files_asked_about_are_ordered():
    """After pruning the list is a part of the table."""
    stats = _stats()
    kept = [FILES[1], FILES[3], FILES[4]]
    assert order_files_by_stats(kept, stats, "id", True, False) == [FILES[4], FILES[3], FILES[1]]


def test_a_column_is_found_whatever_case_the_query_names_it_in():
    stats = _stats()
    assert order_files_by_stats(FILES, stats, "ID", True, True) == FILES[::-1]
    assert order_files_by_stats(FILES, stats, "Ts", True, True) == FILES[::-1]


def test_a_files_bounds_are_those_of_all_its_row_groups():
    """Three row groups each; the last file's highest value is in its first group."""
    rows = []
    for index, path in enumerate(FILES[:3]):
        for group in range(3):
            low = index * 100 + group * 10
            rows.append(_row(path, "id", group, min_bigint=low, max_bigint=low + 9))
    rows[6] = _row(FILES[2], "id", 0, min_bigint=5, max_bigint=1_000)
    stats = _frame(rows)
    assert order_files_by_stats(FILES[:3], stats, "id", True, True) == [FILES[2], FILES[1], FILES[0]]
    # lowest value first: file 2 holds a 5, file 0 starts at 0
    assert order_files_by_stats([FILES[2], FILES[1], FILES[0]], stats, "id", False, True) == [
        FILES[0], FILES[2], FILES[1]]


def test_files_without_bounds_go_last_as_they_are_listed():
    """Only NULLs, no statistics, a type that is not routed: nothing to order by."""
    rows = [
        _row(FILES[0], "id"),                                            # no bounds
        _row(FILES[1], "id", min_bigint=100, max_bigint=199),
        _row(FILES[2], "note", min_string="a", max_string="b"),          # no such column
        _row(FILES[3], "id", min_bigint=300, max_bigint=399),
        _row(FILES[4], "id", min_double=float("nan"), max_double=float("nan")),
    ]
    for path in (FILES[0], FILES[1], FILES[3], FILES[4]):
        rows.append(_row(path, "note", min_string="a", max_string="b"))
    rows.insert(4, _row(FILES[2], "id"))
    stats = _frame(sorted(rows, key=lambda r: (r["file_path"], r["column_name"])))
    assert order_files_by_stats(FILES, stats, "id", True, True) == [
        FILES[3], FILES[1], FILES[0], FILES[2], FILES[4]]
    assert order_files_by_stats(FILES, stats, "id", False, True) == [
        FILES[1], FILES[3], FILES[0], FILES[2], FILES[4]]


def test_whole_numbers_and_fractions_order_together_and_nothing_else_does():
    rows = [_row(FILES[0], "amount", min_bigint=1, max_bigint=5),
            _row(FILES[1], "amount", min_double=7.5, max_double=9.5),
            _row(FILES[2], "amount", min_bigint=6, max_bigint=7)]
    assert order_files_by_stats(FILES[:3], _frame(rows), "amount", True, True) == [
        FILES[1], FILES[2], FILES[0]]

    rows[1] = _row(FILES[1], "amount", min_string="7", max_string="9")
    left = FILES[:3]
    assert order_files_by_stats(left, _frame(rows), "amount", True, True) is left
    rows[1] = _row(FILES[1], "amount", min_timestamp=datetime(2026, 1, 1),
                   max_timestamp=datetime(2026, 1, 2))
    assert order_files_by_stats(left, _frame(rows), "amount", True, True) is left


def test_a_file_whose_row_groups_disagree_about_the_type_has_no_bounds():
    rows = [_row(FILES[0], "amount", 0, min_bigint=1, max_bigint=5),
            _row(FILES[1], "amount", 0, min_bigint=50, max_bigint=60),
            _row(FILES[1], "amount", 1, min_string="x", max_string="y"),
            _row(FILES[2], "amount", 0, min_bigint=6, max_bigint=7)]
    assert order_files_by_stats(FILES[:3], _frame(rows), "amount", True, True) == [
        FILES[2], FILES[0], FILES[1]]


def test_a_files_bounds_are_read_as_pruning_reads_them():
    """Row by row what ``_stored_lane`` gives: the first typed pair that is
    filled, and nothing for a row whose statistics are not available."""
    day = datetime(2026, 3, 1)
    rows = [
        _row(FILES[0], "v", 0, min_bigint=1, max_bigint=2),
        _row(FILES[0], "v", 1, min_bigint=-5, max_bigint=1),
        _row(FILES[1], "v", 0, min_double=1.5, max_double=2.5),
        _row(FILES[2], "v", 0, min_timestamp=day, max_timestamp=day + timedelta(hours=1)),
        _row(FILES[3], "v", 0, min_string="a", max_string="b"),
        _row(FILES[4], "v", 0, min_bigint=7),                          # half a pair: no bounds
        _row(FILES[4], "v", 1, min_bigint=3, max_bigint=4, min_string="x", max_string="y"),
        _row(FILES[4], "V", 2, min_bigint=1, max_bigint=9),            # the same column in another case
        _row(FILES[4], "other", 0, min_bigint=100, max_bigint=200),
    ]
    rows.append(dict(_row("f5", "v", 0, min_bigint=1, max_bigint=2), stats_available=False))
    frame = _frame(rows)
    expected = {}
    for row in frame.iter_rows(named=True):
        stored = processing._stored_lane(row)
        if row["column_name"].lower() != "v" or stored is None:
            continue
        lane, low, high = stored
        seen = expected.get(row["file_path"])
        expected[row["file_path"]] = (lane, low, high) if seen is None else (
            lane, min(seen[1], low), max(seen[2], high))
    assert processing._file_bounds(frame, "V") == expected == {
        FILES[0]: ("bigint", -5, 2), FILES[1]: ("double", 1.5, 2.5),
        FILES[2]: ("timestamp", day, day + timedelta(hours=1)), FILES[3]: ("string", "a", "b"),
        FILES[4]: ("bigint", 1, 9)}
    assert processing._file_bounds(frame, "nowhere") == {}


@pytest.mark.parametrize("stats, files", [
    (None, FILES),
    (pl.DataFrame(schema=STATS_SCHEMA), FILES),
    (_stats(), FILES[:1]),
    (_stats(), []),
    (_stats(), FILES + FILES[:1]),                       # a file listed twice
    (_stats(FILES[:4]), FILES),                          # a file the statistics do not describe
    (_stats(columns=("note",)), FILES),                  # no bounds for the column at all
])
def test_with_nothing_to_order_by_the_files_are_left_as_they_are(stats, files):
    assert order_files_by_stats(files, stats, "id", True, True) is files
    assert order_files_by_stats(files, stats, "id", False, False) is files


def test_a_column_the_table_does_not_have_orders_nothing():
    files = list(FILES)
    assert order_files_by_stats(files, _stats(), "nowhere", True, True) is files


# --------------------------------------------------------------------------
# The columns a read returns must be the same either way
# --------------------------------------------------------------------------

def _files_with(columns_by_file) -> pl.DataFrame:
    """One row group a file; ids rise from file to file, columns as given."""
    rows = []
    for index, (path, columns) in enumerate(columns_by_file.items()):
        for column in columns:
            if column.lower() == "id":
                rows.append(_row(path, column, min_bigint=index * 100, max_bigint=index * 100 + 99))
            else:
                rows.append(_row(path, column, min_string="a", max_string="z"))
    return _frame(rows)


def test_files_with_the_same_columns_in_the_same_order_are_reordered():
    stats = _files_with({path: ["id", "amount", "note"] for path in FILES})
    for every_column in (True, False):
        assert order_files_by_stats(FILES, stats, "id", True, every_column) == FILES[::-1]


def test_a_column_added_later_does_not_keep_the_files_from_being_reordered():
    """Old files (id, note), new files (id, note, extra): a reader finds
    id, note, extra whichever file it opens first."""
    stats = _files_with({FILES[0]: ["id", "note"], FILES[1]: ["id", "note"],
                         FILES[2]: ["id", "note", "extra"], FILES[3]: ["id", "note", "extra"]})
    assert order_files_by_stats(FILES[:4], stats, "id", True, True) == FILES[3::-1]


@pytest.mark.parametrize("columns_by_file", [
    # a column in the middle: (id, note) then (id, extra, note) reads id, note, extra or id, extra, note
    {FILES[0]: ["id", "note"], FILES[1]: ["id", "extra", "note"]},
    # each has a column the other lacks
    {FILES[0]: ["id", "note", "old"], FILES[1]: ["id", "note", "new"]},
    # the same columns in another order
    {FILES[0]: ["id", "note", "amount"], FILES[1]: ["id", "amount", "note"]},
    # the same column spelled in another case: the reader takes the first spelling
    {FILES[0]: ["id", "Amount"], FILES[1]: ["id", "amount"]},
    {FILES[0]: ["ID", "amount"], FILES[1]: ["id", "amount"]},
    # three files, the odd one listed first: it gives the order now, and would not then
    {FILES[0]: ["note", "id"], FILES[1]: ["id", "note"], FILES[2]: ["id", "note"]},
])
def test_files_whose_columns_would_come_out_differently_are_left_as_listed(columns_by_file):
    stats, files = _files_with(columns_by_file), list(columns_by_file)
    for every_column in (True, False):
        assert order_files_by_stats(files, stats, "id", True, every_column) is files


def test_an_odd_file_that_is_not_met_first_either_way_changes_nothing():
    """(id, note), (note, id), (id, note, extra): the first file listed and the
    first file by id both give id, note; the reader finds id, note, extra."""
    by_file = {FILES[0]: ["id", "note"], FILES[1]: ["note", "id"], FILES[2]: ["id", "note", "extra"]}
    stats, files = _files_with(by_file), list(by_file)
    assert order_files_by_stats(files, stats, "id", True, True) == files[::-1]


def test_nested_columns_keep_select_star_as_listed_and_named_columns_are_reordered():
    """The statistics name a nested field ``address.city``.  A column whose
    name has a dot in it is named the same, and SELECT * shows the difference
    between a struct and two such columns, so SELECT * is left alone."""
    nested = ["id", "address.city", "address.zip", "tags.list.element"]
    stats = _files_with({path: nested for path in FILES})
    files = list(FILES)
    assert order_files_by_stats(files, stats, "id", True, every_column=True) is files
    assert order_files_by_stats(files, stats, "id", True, every_column=False) == FILES[::-1]


def test_nested_columns_that_differ_between_files_are_left_as_listed():
    stats = _files_with({FILES[0]: ["id", "address.city"],
                         FILES[1]: ["id", "address.city", "address.zip"]})
    files = FILES[:2]
    for every_column in (True, False):
        assert order_files_by_stats(files, stats, "id", True, every_column) is files


def test_a_files_columns_are_those_of_its_first_row_group_in_footer_order():
    stats = _stats(columns=("note", "id", "ts"), groups=3)
    of_file, sequences = _column_sequences(stats)
    assert sequences == [("note", "id", "ts")]
    assert of_file == {path: 0 for path in FILES}


# --------------------------------------------------------------------------
# What is worked out of the statistics is kept with them
# --------------------------------------------------------------------------

def test_the_statistics_are_worked_through_once_for_a_column(monkeypatch):
    bounds, sequences = [], []
    real_bounds, real_sequences = processing._file_bounds, processing._column_sequences
    monkeypatch.setattr(processing, "_file_bounds",
                        lambda frame, column: bounds.append(column) or real_bounds(frame, column))
    monkeypatch.setattr(processing, "_column_sequences",
                        lambda frame: sequences.append(1) or real_sequences(frame))
    stats = _stats()
    for _ in range(5):
        assert order_files_by_stats(FILES, stats, "id", True, True) == FILES[::-1]
        assert order_files_by_stats(FILES[1:4], stats, "id", True, False) == FILES[3:0:-1]
    assert order_files_by_stats(FILES, stats, "ts", True, True) == FILES[::-1]
    assert bounds == ["id", "ts"] and sequences == [1]
    # new statistics are worked through anew
    assert order_files_by_stats(FILES, _stats(), "id", True, True) == FILES[::-1]
    assert bounds == ["id", "ts", "id"] and sequences == [1, 1]


# --------------------------------------------------------------------------
# The estimator's step
# --------------------------------------------------------------------------

def _estimator() -> DataEstimator:
    return DataEstimator.__new__(DataEstimator)


def test_the_estimator_gives_the_order_or_nothing():
    stats, estimator = _stats(), _estimator()
    order = lambda files, by, every=True, frame=stats: estimator._order_files(
        "lake", "orders", files, frame, by, every)
    assert order(FILES, ("id", True)) == FILES[::-1]
    assert order(FILES, ("id", False)) is None            # already in that order
    assert order(FILES, None) is None                     # not the shape
    assert order(FILES, ("id", True), frame=None) is None
    assert order(FILES[:1], ("id", True)) is None
    assert order(FILES, ("nowhere", True)) is None


def test_a_failure_to_work_out_the_order_does_not_fail_the_read(monkeypatch):
    module = sys.modules[DataEstimator.__module__]
    monkeypatch.setattr(module, "order_files_by_stats", lambda *a, **k: 1 / 0)
    assert _estimator()._order_files("lake", "orders", FILES, _stats(), ("id", True), True) is None


# --------------------------------------------------------------------------
# Against DuckDB itself
# --------------------------------------------------------------------------

def _describe(con, files):
    listed = ", ".join("'" + f + "'" for f in files)
    return [(row[0], row[1]) for row in con.execute(
        f"DESCRIBE SELECT * FROM parquet_scan([{listed}], union_by_name=TRUE, "
        f"HIVE_PARTITIONING=FALSE)").fetchall()]


def _statistics_of(paths) -> pl.DataFrame:
    return extract_stats_rows(paths, footer_md_cache={p: pq.read_metadata(p) for p in paths})


def test_the_statistics_list_a_files_columns_as_duckdb_finds_them(tmp_path):
    """Footer order, every column, nested fields by their dotted path."""
    path = str(tmp_path / "one.parquet")
    pq.write_table(pa.table({
        "note": ["a", "b"], "id": [1, 2], "Amount": [1.5, 2.5],
        "address": [{"city": "x", "zip": "1"}, {"city": "y", "zip": "2"}],
        "tags": [["p"], ["q"]], "price": [decimal.Decimal("1.25"), decimal.Decimal("2.50")],
    }), path, row_group_size=1)                            # two row groups
    of_file, sequences = _column_sequences(_statistics_of([path]))
    assert sequences[of_file[path]] == (
        "note", "id", "Amount", "address.city", "address.zip", "tags.list.element", "price")
    assert [name for name, _ in _describe(duckdb.connect(), [path])] == [
        "note", "id", "Amount", "address", "tags", "price"]


def test_duckdb_reads_several_files_columns_in_the_order_the_rule_assumes(tmp_path):
    """Every name where it first appears, compared without case, spelled as
    first met: for every order of three files that disagree in all of it."""
    a, b, c = (str(tmp_path / f"{name}.parquet") for name in "abc")
    pq.write_table(pa.table({"id": [1], "Amount": [1.0], "d": ["x"]}), a)
    pq.write_table(pa.table({"amount": [2.0], "id": [2], "c": ["y"]}), b)
    pq.write_table(pa.table({"c": ["z"], "e": [1], "ID": [3]}), c)
    of_file, sequences = _column_sequences(_statistics_of([a, b, c]))
    con = duckdb.connect()
    found = set()
    for order in itertools.permutations([a, b, c]):
        predicted = _columns_read_together([of_file[path] for path in order], sequences)
        assert [name for name, _ in _describe(con, list(order))] == list(predicted)
        found.add(predicted)
    assert len(found) == 5                                 # the order shows: only a,b,c and a,c,b agree


@pytest.mark.parametrize("left, right", [
    (pa.array([1], pa.int32()), pa.array([2], pa.int64())),
    (pa.array([1], pa.int64()), pa.array([2.5], pa.float64())),
    (pa.array([1.5], pa.float32()), pa.array([2.5], pa.float64())),
    (pa.array([True], pa.bool_()), pa.array([2], pa.int64())),
    (pa.array([1], pa.int64()), pa.array(["x"], pa.string())),
    (pa.array([datetime(2026, 1, 1)], pa.timestamp("us")), pa.array([datetime(2026, 1, 2)], pa.timestamp("ns"))),
    (pa.array([datetime(2026, 1, 1)], pa.timestamp("us")),
     pa.array([datetime(2026, 1, 2)], pa.timestamp("us", tz="UTC"))),
    (pa.array([date(2026, 1, 1)], pa.date32()), pa.array([datetime(2026, 1, 2)], pa.timestamp("us"))),
    (pa.array([decimal.Decimal("1.25")], pa.decimal128(10, 2)),
     pa.array([decimal.Decimal("2.1234")], pa.decimal128(18, 4))),
    (pa.array([{"a": 1, "b": 2}]), pa.array([{"a": 1, "b": 2, "c": 3}])),
    (pa.array([{"a": 1, "b": 2}]), pa.array([{"b": 1, "a": 2}])),
    (pa.array([[1]]), pa.array([[1.5]])),
], ids=["int32/int64", "int64/double", "float/double", "bool/int64", "int64/text", "micros/nanos",
        "timestamp/with zone", "date/timestamp", "decimal scales", "struct grows", "struct fields swapped",
        "list of int/double"])
def test_the_types_duckdb_gives_columns_do_not_depend_on_the_order_of_the_files(tmp_path, left, right):
    """Two files that hold one column in two types: the reader's type for it,
    and the rows it reads, are the same whichever file comes first."""
    first, second = str(tmp_path / "first.parquet"), str(tmp_path / "second.parquet")
    pq.write_table(pa.table({"k": [1], "v": left}), first)
    pq.write_table(pa.table({"k": [2], "v": right}), second)
    con = duckdb.connect()

    def read(files):
        listed = ", ".join("'" + f + "'" for f in files)
        try:
            return _describe(con, files), con.execute(
                f"SELECT * FROM parquet_scan([{listed}], union_by_name=TRUE) ORDER BY k").fetchall()
        except duckdb.Error as error:
            return "refused", type(error).__name__

    assert read([first, second]) == read([second, first])


_POOL = {
    "amount": (pa.int32(), pa.int64(), pa.float64()),
    "Amount": (pa.int64(),),
    "note": (pa.string(),),
    "extra": (pa.string(), pa.int64()),
    "ts": (pa.timestamp("us"), pa.timestamp("ns")),
}


def _random_file(rng, path, index):
    """A file with ``id`` and some of the pool's columns, in some order."""
    names = [name for name in _POOL if rng.random() < 0.6]
    if "amount" in names and "Amount" in names:
        names.remove(rng.choice(["amount", "Amount"]))
    names.insert(rng.randrange(len(names) + 1) if rng.random() < 0.3 else 0, "id")
    if rng.random() < 0.25:
        rng.shuffle(names)
    columns = {}
    for name in names:
        if name == "id":
            columns[name] = pa.array([index * 10 + 1, index * 10 + 2], pa.int64())
            continue
        kind = rng.choice(_POOL[name])
        if pa.types.is_timestamp(kind):
            columns[name] = pa.array([datetime(2026, 1, 1 + index), datetime(2026, 1, 2 + index)], kind)
        elif pa.types.is_string(kind):
            columns[name] = pa.array([f"{name}{index}", f"{name}{index}b"], kind)
        elif pa.types.is_floating(kind):
            columns[name] = pa.array([index + 0.5, index + 1.5], kind)
        else:
            columns[name] = pa.array([index, index + 1], kind)
    pq.write_table(pa.table(columns), path)


def test_whenever_files_are_reordered_duckdb_reads_the_same_columns_and_rows(tmp_path):
    """Files with columns added, missing, moved, respelled and retyped, listed
    in no order.  Where the rule reorders them, DuckDB's ``SELECT *`` has the
    same names, positions, types and rows as for the listed order; and the
    columns it reads are always the ones the rule expects."""
    rng = random.Random(20261128)
    con = duckdb.connect()
    reordered = left = 0
    for case in range(60):
        folder = tmp_path / f"case{case}"
        folder.mkdir()
        count = rng.randint(2, 5)
        paths = []
        for index in rng.sample(range(count), count):      # ids in no order of listing
            path = str(folder / f"part-{index}.parquet")
            if case % 3 == 0 and paths:                    # every third: one shape, written again and again
                first = pq.read_table(paths[0])
                pq.write_table(first.set_column(
                    first.schema.get_field_index("id"), "id",
                    pa.array([index * 10 + 1, index * 10 + 2], pa.int64())), path)
            else:
                _random_file(rng, path, index)
            paths.append(path)
        stats = _statistics_of(paths)
        of_file, sequences = _column_sequences(stats)
        listed = _describe(con, paths)
        assert [name for name, _ in listed] == list(
            _columns_read_together([of_file[p] for p in paths], sequences))

        descending = rng.random() < 0.5
        ordered = order_files_by_stats(paths, stats, "id", descending, every_column=True)
        if ordered is paths:
            left += 1
            continue
        reordered += 1
        assert sorted(ordered) == sorted(paths)
        assert _describe(con, ordered) == listed

        def rows(files):
            scan = ", ".join("'" + f + "'" for f in files)
            return con.execute(
                f"SELECT * FROM parquet_scan([{scan}], union_by_name=TRUE, "
                f"HIVE_PARTITIONING=FALSE) ORDER BY id {'DESC' if descending else ''} LIMIT 3").fetchall()

        assert rows(ordered) == rows(paths)
    # the test is of both: files that are reordered, and files that are left
    assert reordered >= 15 and left >= 10, (reordered, left)


def test_a_top_n_query_returns_the_same_rows_from_the_files_in_either_order(tmp_path):
    """Six files of 2,000 rows, newer in every file, on a connection set up as
    the engine's: the rows and their order are the query's."""
    paths = []
    for index in range(6):
        ids = list(range(index * 2_000, (index + 1) * 2_000))
        path = str(tmp_path / f"part-{index}.parquet")
        pq.write_table(pa.table({
            "id": pa.array(ids, pa.int64()),
            "ts": pa.array([datetime(2026, 1, 1) + timedelta(seconds=30 * i) for i in ids], pa.timestamp("us")),
            "amount": pa.array([(i * 7919) % 1_000 for i in ids], pa.int64()),
        }), path, row_group_size=500)
        paths.append(path)
    stats = _statistics_of(paths)
    con = duckdb.connect()
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")

    def read(files, query):
        scan = ", ".join("'" + f + "'" for f in files)
        return con.execute(query.format(
            t=f"parquet_scan([{scan}], union_by_name=TRUE, HIVE_PARTITIONING=FALSE)")).fetchall()

    for column, descending, query in [
        ("ts", True, "SELECT * FROM {t} ORDER BY ts DESC, id DESC LIMIT 50"),
        ("ts", False, "SELECT id, ts FROM {t} WHERE amount < 500 ORDER BY ts, id LIMIT 50 OFFSET 20"),
        ("amount", True, "SELECT id, amount FROM {t} ORDER BY amount DESC, id LIMIT 200"),
        ("id", True, "SELECT * FROM {t} WHERE amount = 7 ORDER BY id DESC LIMIT 5"),
    ]:
        shuffled = [paths[i] for i in (3, 0, 5, 1, 4, 2)]
        ordered = order_files_by_stats(shuffled, stats, column, descending, every_column=True)
        expected = read(shuffled, query)
        assert read(ordered, query) == expected and len(expected) > 0
    newest_first = order_files_by_stats(paths, stats, "ts", True, True)
    assert [os.path.basename(p) for p in newest_first] == [f"part-{i}.parquet" for i in (5, 4, 3, 2, 1, 0)]
