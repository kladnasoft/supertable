"""
A read keeps what it worked out of a table's statistics, until a write changes them.

Pruning (each file's bounds for the columns a query constrains) and projection
sizing (each file's stored bytes for the columns it names) were worked out of
the statistics frame again for every read: 6 to 9 ms and 7 to 14 ms of every
read with a predicate that names its columns.  They depend on the frame and on
the columns only, so they are now made once for a frame and kept with it.

The risk in keeping anything is answering from what is no longer true.  Here
the real writer appends, upserts and deletes between the reads of the real
reader, over the hermetic fake Redis: every read has to be right, find what
the last write changed, and report the same plan a first read reports.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import polars as pl
import pytest

import supertable.processing as processing
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.engine_enum import Engine
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
FILES = 5
ROWS_A_FILE = 100


def _rows(first: int, count: int = ROWS_A_FILE) -> pl.DataFrame:
    ids = list(range(first, first + count))
    return pl.DataFrame({"id": ids, "amount": [i * 10 for i in ids],
                         "note": [f"row {i}" for i in ids]})


@pytest.fixture
def lake(hermetic_fakeredis, monkeypatch):
    engine_duckdb.reset_shared_duckdb_state()
    processing._STATS_CACHE.clear()
    processing.forget_stats_derived()
    name = f"derived_{uuid.uuid4().hex[:10]}"
    SuperTable(name, ORG)
    writer = DataWriter(super_name=name, organization=ORG)
    for part in range(FILES):                                 # a file a write, ids in order
        writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                     data=_rows(part * ROWS_A_FILE))

    bounds, sums = [], []
    real_bounds, real_sums = processing._bounds_index, DataEstimator._sum_projected_bytes
    monkeypatch.setattr(processing, "_bounds_index",
                        lambda frame, columns: bounds.append(tuple(columns)) or real_bounds(frame, columns))
    monkeypatch.setattr(DataEstimator, "_sum_projected_bytes", staticmethod(
        lambda frame, columns: sums.append(frozenset(columns)) or real_sums(frame, columns)))

    def read(sql: str):
        reader = DataReader(super_name=name, organization=ORG, query=sql)
        frame, status, message = reader.execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status == Status.OK, message
        plan = {}
        for entry in reader.plan_stats.stats:
            if isinstance(entry, dict):
                plan.update(entry)
        return frame, plan

    yield SimpleNamespace(name=name, writer=writer, read=read, bounds=bounds, sums=sums)
    engine_duckdb.reset_shared_duckdb_state()
    processing._STATS_CACHE.clear()
    processing.forget_stats_derived()


def _plan(plan: dict) -> dict:
    """What a read reports of its files, without what differs by the clock."""
    return {key: plan.get(key) for key in (
        "REFLECTIONS", "REFLECTION_SIZE", "REFLECTION_SIZE_RAW",
        "FILES_BEFORE_PRUNE", "FILES_PRUNED", "FILES_KEPT")}


def test_reads_of_one_shape_work_through_the_statistics_once(lake):
    first, first_plan = lake.read("SELECT id, amount FROM orders WHERE id = 137")
    assert first.to_dicts() == [{"id": 137, "amount": 1370}]
    assert first_plan["FILES_KEPT"] == 1 and first_plan["FILES_PRUNED"] == FILES - 1
    assert 0 < first_plan["REFLECTION_SIZE"] < first_plan["REFLECTION_SIZE_RAW"]

    for key in (5, 499, 250, 137):                            # other values, the same columns
        frame, plan = lake.read(f"SELECT id, amount FROM orders WHERE id = {key}")
        assert frame.to_dicts() == [{"id": key, "amount": key * 10}]
        assert plan["FILES_KEPT"] == 1
    assert _plan(plan) == _plan(first_plan)                   # 137 again reports what it reported
    assert lake.bounds == [("id",)] and lake.sums == [frozenset({"id", "amount"})]

    # a range over two files, the same columns: the same kept answers
    frame, plan = lake.read("SELECT id, amount FROM orders WHERE id >= 180 AND id < 220")
    assert frame.height == 40 and plan["FILES_KEPT"] == 2
    assert lake.bounds == [("id",)] and len(lake.sums) == 1

    # other columns are worked out once more, and then kept as well
    for _ in range(2):
        frame, plan = lake.read("SELECT note FROM orders WHERE amount = 3210")
        assert frame.to_dicts() == [{"note": "row 321"}] and plan["FILES_KEPT"] == 1
    assert lake.bounds == [("id",), ("amount",)]
    assert lake.sums == [frozenset({"id", "amount"}), frozenset({"amount", "note"})]


def test_an_append_is_seen_by_the_next_read(lake):
    assert lake.read("SELECT id FROM orders WHERE id = 137")[0]["id"].to_list() == [137]
    assert lake.read("SELECT id FROM orders WHERE id = 555")[0].is_empty()    # not written yet

    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                      data=_rows(FILES * ROWS_A_FILE))        # ids 500 to 599, a sixth file
    frame, plan = lake.read("SELECT id, amount FROM orders WHERE id = 555")
    assert frame.to_dicts() == [{"id": 555, "amount": 5550}]
    assert plan["FILES_BEFORE_PRUNE"] == FILES + 1 and plan["FILES_KEPT"] == 1
    # the rows that were there are found as before
    assert lake.read("SELECT id FROM orders WHERE id = 137")[0]["id"].to_list() == [137]
    assert lake.read("SELECT COUNT(*) AS n FROM orders WHERE id >= 0")[0]["n"][0] == 600
    # the statistics before the append, and the statistics after it
    assert lake.bounds == [("id",), ("id",)]


def test_an_upsert_that_moves_a_files_bounds_is_seen(lake):
    """Keys far outside every file's range are written over a key that exists:
    the file they land in has bounds no kept index knew."""
    assert lake.read("SELECT id FROM orders WHERE id = 9000")[0].is_empty()
    assert lake.read("SELECT amount FROM orders WHERE id = 42")[0]["amount"].to_list() == [420]

    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["id"],
                      data=pl.DataFrame({"id": [42, 9000], "amount": [1, 2],
                                         "note": ["changed", "new"]}))
    assert lake.read("SELECT amount, note FROM orders WHERE id = 9000")[0].to_dicts() == [
        {"amount": 2, "note": "new"}]
    assert lake.read("SELECT amount, note FROM orders WHERE id = 42")[0].to_dicts() == [
        {"amount": 1, "note": "changed"}]
    assert lake.read("SELECT amount FROM orders WHERE id = 43")[0]["amount"].to_list() == [430]
    assert lake.read("SELECT COUNT(*) AS n FROM orders WHERE id >= 0")[0]["n"][0] == FILES * ROWS_A_FILE + 1


def test_a_delete_is_seen_and_the_rest_is_found(lake):
    assert lake.read("SELECT id FROM orders WHERE id IN (10, 11)")[0]["id"].to_list() == [10, 11]
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["id"],
                      data=pl.DataFrame({"id": [10, 310]}), delete_only=True)
    assert lake.read("SELECT id FROM orders WHERE id IN (10, 11)")[0]["id"].to_list() == [11]
    assert lake.read("SELECT id FROM orders WHERE id = 310")[0].is_empty()
    assert lake.read("SELECT id FROM orders WHERE id = 311")[0]["id"].to_list() == [311]
    assert lake.read("SELECT COUNT(*) AS n FROM orders WHERE id < 100000")[0]["n"][0] == FILES * ROWS_A_FILE - 2


def test_a_read_reports_the_plan_a_first_read_reports(lake):
    """The same query on statistics nothing was derived from yet, and on
    statistics everything was: the same files, the same sizes."""
    queries = [
        "SELECT id, amount FROM orders WHERE id = 250",
        # ordered: a read of more than one file promises no order of its own
        "SELECT note FROM orders WHERE id BETWEEN 90 AND 110 ORDER BY id",
        "SELECT SUM(amount) AS s FROM orders WHERE id >= 400",
        "SELECT * FROM orders WHERE id = 7",
        "SELECT amount FROM orders ORDER BY amount",
    ]
    warm = {}
    for _ in range(2):
        for sql in queries:
            frame, plan = lake.read(sql)
            warm[sql] = (frame.to_dicts(), _plan(plan))

    for sql in queries:
        processing.forget_stats_derived()                     # as a process that never read it
        frame, plan = lake.read(sql)
        assert (frame.to_dicts(), _plan(plan)) == warm[sql], sql
