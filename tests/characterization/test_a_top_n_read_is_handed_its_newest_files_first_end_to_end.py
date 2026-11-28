"""
A read that takes its first rows by a column is handed its newest files first.

``SELECT ... FROM t ORDER BY ts DESC LIMIT n`` prunes no file, and DuckDB reads
a table's files in the order it is given them: as they were written, oldest
first.  Its top-N can skip a row group only once it holds *n* rows that beat
the group's bounds, so the whole table was read for *n* rows.  For that shape
the estimator now works out the files in the order of the column's stored
bounds and the DuckDB engine reads them so.

Here the real writer writes, upserts, deletes and changes the table's columns,
and the real reader reads, over the hermetic fake Redis.  Every read of the
shape is compared with the same read under ``fullscan``, which hands every
file over as listed: the rows, their order and the columns have to be the
same.  What the engine was handed is taken from the statement it was sent.
"""

from __future__ import annotations

import dataclasses
import re
import sys
import uuid
from datetime import datetime, timedelta
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.config.settings import settings
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.engine_enum import Engine
from supertable.rbac.role_manager import RoleManager
from supertable.super_table import SuperTable
import supertable.processing as processing

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads ``id`` and ``amount`` of the rows whose ``amount`` is below 2,000.
SMALL_AMOUNTS = "small_amounts"
FILES = 5
ROWS_A_FILE = 100
ROWS = FILES * ROWS_A_FILE
DAY_ONE = datetime(2026, 1, 1)


def _rows(first: int, count: int = ROWS_A_FILE) -> pl.DataFrame:
    """Ids, and seconds, rise from row to row; ``falling`` falls."""
    ids = list(range(first, first + count))
    return pl.DataFrame({
        "id": ids,
        "ts": [DAY_ONE + timedelta(seconds=i) for i in ids],
        "amount": [i * 10 for i in ids],
        "falling": [-i for i in ids],
        "note": [f"row {i:04d}" for i in ids],
    })


class _Recording:
    """The thread's connection, recording what it is sent."""

    def __init__(self, con):
        self._con = con
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        return self._con.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._con, name)


@pytest.fixture
def lake(hermetic_fakeredis, monkeypatch):
    engine_duckdb.reset_shared_duckdb_state()
    processing._STATS_CACHE.clear()
    processing.forget_stats_derived()
    name = f"topn_{uuid.uuid4().hex[:10]}"
    SuperTable(name, ORG)
    writer = DataWriter(super_name=name, organization=ORG)
    for part in range(FILES):                                 # a file a write, ids in order
        writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                     data=_rows(part * ROWS_A_FILE))
    RoleManager(super_name=name, organization=ORG, actor_role_name=ADMIN).create_role({
        "role_name": SMALL_AMOUNTS, "role": "reader", "tables": {"orders": {
            "columns": ["id", "amount"],
            "filters": [{"amount": {"operation": "<", "type": "value", "value": 2000}}]}}})

    estimates = []
    real_estimate = DataEstimator.estimate

    def estimate(self):
        reflection = real_estimate(self)
        estimates.append(reflection)
        return reflection

    monkeypatch.setattr(DataEstimator, "estimate", estimate)

    def read(sql: str, role: str = ADMIN, fullscan: bool = False):
        """(frame, plan, the files the engine was handed, the table's snapshot as estimated)."""
        state = engine_duckdb._shared_state()
        if state.get("con") is not None and not isinstance(state["con"], _Recording):
            state["con"] = _Recording(state["con"])
        if state.get("con") is not None:
            state["con"].sql = []
        reader = DataReader(super_name=name, organization=ORG, query=sql)
        frame, status, message = reader.execute(role_name=role, engine=Engine.DUCKDB, fullscan=fullscan)
        assert status == Status.OK, message
        plan = {}
        for entry in reader.plan_stats.stats:
            if isinstance(entry, dict):
                plan.update(entry)
        con = engine_duckdb._shared_state()["con"]
        handed = None
        if isinstance(con, _Recording):
            views = [s for s in con.sql if s.startswith("CREATE OR REPLACE VIEW") and "parquet_scan(" in s]
            handed = [re.findall(r"'([^']+\.parquet)'", view.split("parquet_scan(", 1)[1].split("]", 1)[0])
                      for view in views]
        return SimpleNamespace(frame=frame, plan=plan, handed=handed, supers=estimates[-1].supers)

    read("SELECT COUNT(*) AS n FROM orders")                  # makes the thread's connection
    yield SimpleNamespace(name=name, writer=writer, read=read)
    engine_duckdb.reset_shared_duckdb_state()
    processing._STATS_CACHE.clear()
    processing.forget_stats_derived()


def _same(lake, sql: str, role: str = ADMIN):
    """The read, and the same read of the files as listed: one answer."""
    ordered, listed = lake.read(sql, role=role), lake.read(sql, role=role, fullscan=True)
    assert "FILES_ORDERED_BY" not in listed.plan
    assert listed.supers[0].files_in_read_order is None
    assert ordered.frame.columns == listed.frame.columns, sql
    assert ordered.frame.to_dicts() == listed.frame.to_dicts(), sql
    return ordered


def test_the_newest_file_is_handed_over_first(lake):
    got = lake.read("SELECT * FROM orders ORDER BY id DESC LIMIT 3")
    assert got.frame["id"].to_list() == [499, 498, 497]
    assert got.frame.columns == ["id", "ts", "amount", "falling", "note"]
    assert got.plan["FILES_ORDERED_BY"] == "orders.id DESC"
    assert got.plan["FILES_KEPT"] == FILES and got.plan["FILES_PRUNED"] == 0

    table = got.supers[0]
    assert len(table.files) == FILES
    # the engine was handed the table's files last written first, in one view
    assert got.handed == [table.files[::-1]]
    assert table.files_in_read_order == table.files[::-1]
    # what the estimate lists for every other reader is as it was
    listed = lake.read("SELECT * FROM orders ORDER BY id DESC LIMIT 3", fullscan=True)
    assert listed.supers[0].files == table.files and listed.handed == [table.files]
    assert listed.frame.to_dicts() == got.frame.to_dicts()


@pytest.mark.parametrize("sql, by", [
    ("SELECT * FROM orders ORDER BY ts DESC, id DESC LIMIT 7", "orders.ts DESC"),
    ("SELECT id, note FROM orders ORDER BY ts DESC LIMIT 7 OFFSET 150", "orders.ts DESC"),
    ("SELECT id, amount FROM orders WHERE id < 350 ORDER BY id DESC LIMIT 5", "orders.id DESC"),
    ("SELECT id FROM orders WHERE amount >= 1000 AND note <> 'row 0499' ORDER BY amount DESC LIMIT 4",
     "orders.amount DESC"),
    ("SELECT id, falling FROM orders ORDER BY falling LIMIT 6", "orders.falling ASC"),
    ("SELECT * FROM orders ORDER BY falling, id LIMIT 3", "orders.falling ASC"),
    ("SELECT note, id + 1 AS next_id FROM orders AS o ORDER BY o.note DESC LIMIT 5", "orders.note DESC"),
    ("SELECT id AS key, ts AS ts FROM orders ORDER BY ts DESC LIMIT 2", "orders.ts DESC"),
    ("SELECT ID, Amount FROM orders ORDER BY ID DESC LIMIT 3", "orders.ID DESC"),
    # already in the order of the column: nothing to do
    ("SELECT * FROM orders ORDER BY id LIMIT 5", None),
    ("SELECT id FROM orders ORDER BY ts, id LIMIT 5 OFFSET 250", None),
    ("SELECT id FROM orders ORDER BY falling DESC LIMIT 5", None),
])
def test_a_read_of_the_shape_returns_what_a_read_of_the_files_as_listed_returns(lake, sql, by):
    got = _same(lake, sql)
    assert got.plan.get("FILES_ORDERED_BY") == by
    assert got.frame.height > 0
    table = got.supers[0]
    if by is None:
        assert table.files_in_read_order is None and got.handed == [table.files]
    else:
        assert sorted(table.files_in_read_order) == sorted(table.files)
        assert got.handed == [table.files_in_read_order] != [table.files]


def test_a_pruned_read_orders_the_files_that_are_left(lake):
    got = _same(lake, "SELECT id, amount FROM orders WHERE id BETWEEN 120 AND 310 ORDER BY id DESC LIMIT 5")
    assert got.frame["id"].to_list() == [310, 309, 308, 307, 306]
    assert got.plan["FILES_KEPT"] == 3 and got.plan["FILES_PRUNED"] == 2
    table = got.supers[0]
    assert len(table.files) == 3 and table.files_in_read_order == table.files[::-1]


@pytest.mark.parametrize("sql", [
    "SELECT * FROM orders ORDER BY id DESC",                           # every row is read anyway
    "SELECT * FROM orders LIMIT 5",                                    # nothing pins the order
    "SELECT id FROM orders ORDER BY -id LIMIT 5",
    "SELECT -id AS id FROM orders ORDER BY id DESC LIMIT 5",           # sorts by the alias
    "SELECT note, COUNT(*) AS n FROM orders GROUP BY note ORDER BY note DESC LIMIT 5",
    "SELECT DISTINCT amount FROM orders ORDER BY amount DESC LIMIT 5",
    "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS n FROM orders ORDER BY id DESC LIMIT 5",
    "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id ORDER BY a.id DESC LIMIT 5",
    "SELECT id FROM orders WHERE amount = (SELECT MAX(amount) FROM orders) ORDER BY id DESC LIMIT 5",
    "WITH recent AS (SELECT * FROM orders WHERE id > 400) SELECT id FROM recent ORDER BY id DESC LIMIT 5",
])
def test_every_other_read_is_handed_the_files_as_listed(lake, sql):
    got = lake.read(sql)
    assert "FILES_ORDERED_BY" not in got.plan
    assert all(table.files_in_read_order is None for table in got.supers)
    assert got.handed and all(files == got.supers[0].files for files in got.handed)


def test_the_switch_hands_the_files_over_as_listed(lake, monkeypatch):
    sql = "SELECT id, note FROM orders ORDER BY id DESC LIMIT 4"
    on = lake.read(sql)
    assert on.plan["FILES_ORDERED_BY"] == "orders.id DESC"
    # ``settings`` is frozen: the estimator is given a copy with the switch off
    monkeypatch.setattr(sys.modules[DataEstimator.__module__], "settings",
                        dataclasses.replace(settings, SUPERTABLE_READ_FILE_ORDER_ENABLED=False))
    off = lake.read(sql)
    assert "FILES_ORDERED_BY" not in off.plan
    assert off.supers[0].files_in_read_order is None and off.handed == [off.supers[0].files]
    assert off.frame.to_dicts() == on.frame.to_dicts()


def test_an_order_is_worked_out_only_for_a_read_that_has_one(lake, monkeypatch):
    """Not for another shape, not under fullscan, not for a table of one file."""
    module = sys.modules[DataEstimator.__module__]
    asked = []
    real = module.order_files_by_stats
    monkeypatch.setattr(module, "order_files_by_stats", lambda files, frame, column, descending, every: (
        asked.append((len(files), column, descending, every)) or real(files, frame, column, descending, every)))

    lake.read("SELECT * FROM orders")
    lake.read("SELECT * FROM orders LIMIT 5")
    lake.read("SELECT id FROM orders WHERE id > 10 ORDER BY id DESC")
    lake.read("SELECT * FROM orders ORDER BY id DESC LIMIT 5", fullscan=True)
    assert asked == []
    lake.read("SELECT * FROM orders ORDER BY id DESC LIMIT 5")
    lake.read("SELECT note FROM orders WHERE id < 150 ORDER BY ts LIMIT 5")
    assert asked == [(FILES, "id", True, True), (2, "ts", False, False)]

    lake.writer.write(role_name=ADMIN, simple_name="single", overwrite_columns=[], data=_rows(0, 20))
    got = lake.read("SELECT * FROM single ORDER BY id DESC LIMIT 3")
    assert got.frame["id"].to_list() == [19, 18, 17] and "FILES_ORDERED_BY" not in got.plan
    assert len(asked) == 2


def test_writes_between_reads_are_seen(lake):
    top = "SELECT id, amount, note FROM orders ORDER BY id DESC LIMIT 3"
    assert _same(lake, top).frame["id"].to_list() == [499, 498, 497]

    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                      data=_rows(FILES * ROWS_A_FILE, 10))            # ids 500 to 509, a sixth file
    got = _same(lake, top)
    assert got.frame["id"].to_list() == [509, 508, 507]
    assert len(got.supers[0].files) == FILES + 1

    # a key far above every other, written over a row of the oldest file
    changed = _rows(7, 1).with_columns(pl.lit("changed").alias("note"))
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["id"],
                      data=pl.concat([changed, _rows(9000, 1)]))
    got = _same(lake, top)
    assert got.frame["id"].to_list() == [9000, 509, 508]
    assert _same(lake, "SELECT id, note FROM orders WHERE id < 10 ORDER BY id DESC LIMIT 3"
                 ).frame.to_dicts() == [{"id": 9, "note": "row 0009"}, {"id": 8, "note": "row 0008"},
                                        {"id": 7, "note": "changed"}]

    # the rows that were first are deleted: the next ones are
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["id"],
                      data=pl.DataFrame({"id": [9000, 509, 507]}), delete_only=True)
    got = _same(lake, top)
    assert got.frame["id"].to_list() == [508, 506, 505]
    assert got.plan["FILES_ORDERED_BY"] == "orders.id DESC"
    assert _same(lake, "SELECT COUNT(*) AS n FROM orders").frame["n"][0] == ROWS + 10 + 1 - 3


def test_a_restricted_role_reads_its_own_rows_in_the_same_order(lake):
    """Columns ``id`` and ``amount``, rows with an amount below 2,000: ids 0 to 199."""
    got = _same(lake, "SELECT id, amount FROM orders ORDER BY id DESC LIMIT 3", role=SMALL_AMOUNTS)
    assert got.frame.to_dicts() == [{"id": 199, "amount": 1990}, {"id": 198, "amount": 1980},
                                    {"id": 197, "amount": 1970}]
    assert got.plan["FILES_ORDERED_BY"] == "orders.id DESC"
    star = _same(lake, "SELECT * FROM orders ORDER BY amount DESC LIMIT 2", role=SMALL_AMOUNTS)
    assert star.frame.to_dicts() == [{"id": 199, "amount": 1990}, {"id": 198, "amount": 1980}]
    with pytest.raises(PermissionError, match="note"):
        lake.read("SELECT id FROM orders ORDER BY note DESC LIMIT 3", role=SMALL_AMOUNTS)


def test_a_column_added_later_is_read_where_it_was(lake):
    """Old files without ``extra``, a new file with it at the end: a reader
    finds the same columns whichever file it opens first, so the files are
    reordered, and SELECT * is as it was."""
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                      data=_rows(ROWS, 10).with_columns(pl.lit("new").alias("extra")))
    got = _same(lake, "SELECT * FROM orders ORDER BY id DESC LIMIT 12")
    assert got.frame.columns == ["id", "ts", "amount", "falling", "note", "extra"]
    assert got.frame["extra"].to_list() == ["new"] * 10 + [None, None]
    assert got.plan["FILES_ORDERED_BY"] == "orders.id DESC"
    named = _same(lake, "SELECT extra, id FROM orders ORDER BY id DESC LIMIT 11")
    assert named.frame.columns == ["extra", "id"] and named.plan["FILES_ORDERED_BY"] == "orders.id DESC"


def test_files_whose_columns_come_in_another_order_are_read_as_listed(lake):
    """The writer keeps a frame's columns as they come.  A file written with
    them in another order would give SELECT * that order if it were opened
    first, so the files of such a table are not reordered."""
    moved = _rows(ROWS, 10).select(["note", "falling", "id", "amount", "ts"])
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[], data=moved)
    _, sequences = processing._column_sequences(
        processing.load_stats(_stats_path(lake), allow_cache=True))
    assert sequences == [("id", "ts", "amount", "falling", "note"),
                         ("note", "falling", "id", "amount", "ts")]

    star = _same(lake, "SELECT * FROM orders ORDER BY id DESC LIMIT 12")
    assert star.frame.columns == ["id", "ts", "amount", "falling", "note"]
    assert star.frame["id"].to_list() == list(range(ROWS + 9, ROWS - 3, -1))
    assert "FILES_ORDERED_BY" not in star.plan
    assert star.supers[0].files_in_read_order is None and star.handed == [star.supers[0].files]
    for sql in ("SELECT * FROM orders ORDER BY ts DESC, id LIMIT 3 OFFSET 8",
                "SELECT note, id FROM orders ORDER BY id DESC LIMIT 12",
                "SELECT * FROM orders ORDER BY falling LIMIT 12"):
        assert "FILES_ORDERED_BY" not in _same(lake, sql).plan

    # a read that prunes the odd file away has files of one shape again
    older = _same(lake, f"SELECT * FROM orders WHERE id < {ROWS} ORDER BY id DESC LIMIT 3")
    assert older.frame["id"].to_list() == [ROWS - 1, ROWS - 2, ROWS - 3]
    assert older.plan["FILES_ORDERED_BY"] == "orders.id DESC" and older.plan["FILES_KEPT"] == FILES


def test_a_column_spelled_in_another_case_keeps_the_spelling_it_had(lake):
    """A reader of several files spells a column as the first file does."""
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                      data=_rows(ROWS, 10).rename({"amount": "Amount"}))
    for sql, columns in (("SELECT * FROM orders ORDER BY id DESC LIMIT 12",
                          ["id", "ts", "amount", "falling", "note"]),
                         ("SELECT amount, id FROM orders ORDER BY id DESC LIMIT 12", ["amount", "id"])):
        got = _same(lake, sql)
        assert got.frame.columns == columns
        assert got.frame["amount"].to_list() == [i * 10 for i in range(ROWS + 9, ROWS - 3, -1)]
        assert "FILES_ORDERED_BY" not in got.plan and got.supers[0].files_in_read_order is None


def test_a_table_with_a_nested_column_is_reordered_for_named_columns_only(lake):
    """The statistics name a nested field by a dotted path (``address.city``),
    as they would a column whose name has a dot in it.  SELECT * shows the
    difference between the two, so it reads the files as listed."""
    for part in range(3):
        ids = list(range(part * 10, part * 10 + 10))
        lake.writer.write(role_name=ADMIN, simple_name="places", overwrite_columns=[],
                          data=pl.DataFrame({"id": ids, "address": [
                              {"city": f"city {i}", "zip": f"{i:05d}"} for i in ids]}))
    _, sequences = processing._column_sequences(
        processing.load_stats(_stats_path(lake, "places"), allow_cache=True))
    assert sequences == [("id", "address.city", "address.zip")]

    star = _same(lake, "SELECT * FROM places ORDER BY id DESC LIMIT 3")
    assert star.frame.to_dicts() == [
        {"id": i, "address": {"city": f"city {i}", "zip": f"{i:05d}"}} for i in (29, 28, 27)]
    assert "FILES_ORDERED_BY" not in star.plan and star.supers[0].files_in_read_order is None
    named = _same(lake, "SELECT address, id FROM places ORDER BY id DESC LIMIT 3")
    assert named.frame.columns == ["address", "id"] and named.frame["id"].to_list() == [29, 28, 27]
    assert named.plan["FILES_ORDERED_BY"] == "places.id DESC"


def _stats_path(lake, table: str = "orders") -> str:
    from supertable.redis_catalog import RedisCatalog
    leaf = RedisCatalog().get_leaf(ORG, lake.name, table)
    payload = leaf.get("payload") if isinstance(leaf, dict) else None
    assert isinstance(payload, dict) and payload.get("stats_file")
    return payload["stats_file"]
