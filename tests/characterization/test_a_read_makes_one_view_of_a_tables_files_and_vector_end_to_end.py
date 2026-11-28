"""
A read makes one view of a table's files and its deletion vector.

It used to make a view of the files (the reflection), a second view over it
that strips the system columns and anti-joins the deletion vector, and for a
restricted role a third.  DuckDB resolves a view's files when the view is
created and again whenever something is bound over it, so every read went over
every kept file three times before it read a row.  The reflection is now a
subquery of the second view: two passes, and one statement less each way.

Here the real writer writes and deletes and the real reader reads, over the
hermetic fake Redis.  What is checked is that every read is right, for the
administrator and for a role with a column list and a row filter, and what is
counted is what the reading thread's connection is sent.
"""

from __future__ import annotations

import re
import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.rbac.role_manager import RoleManager
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads ``id`` and ``amount`` of the rows whose region is ``eu``.
EU_READER = "eu_two_columns"
FILES = 4
ROWS_A_FILE = 50
ROWS = FILES * ROWS_A_FILE

ONE_VIEW = re.compile(r"st_[0-9a-f]{16}_[0-9a-f]{8}")


class _Recording:
    """The thread's connection, recording what it is sent."""

    def __init__(self, con):
        self._con = con
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        return self._con.execute(sql, *args, **kwargs)

    def sent(self, starting_with):
        """The statements since the last call that start with *starting_with*."""
        return [sql for sql in self.sql if sql.startswith(starting_with)]

    def forget(self):
        self.sql = []

    def __getattr__(self, name):
        return getattr(self._con, name)


def _region(i: int) -> str:
    return ("eu", "us", "apac")[i % 3]


@pytest.fixture
def lake(hermetic_fakeredis):
    engine_duckdb.reset_shared_duckdb_state()
    name = f"oneview_{uuid.uuid4().hex[:10]}"
    SuperTable(name, ORG)
    writer = DataWriter(super_name=name, organization=ORG)
    for table in ("orders", "customers"):
        for part in range(FILES):                             # a file a write
            ids = list(range(part * ROWS_A_FILE, (part + 1) * ROWS_A_FILE))
            writer.write(role_name=ADMIN, simple_name=table, overwrite_columns=[],
                         data=pl.DataFrame({"id": ids, "region": [_region(i) for i in ids],
                                            "amount": [i * 10 for i in ids]}))
    RoleManager(super_name=name, organization=ORG, actor_role_name=ADMIN).create_role({
        "role_name": EU_READER, "role": "reader", "tables": {"orders": {
            "columns": ["id", "amount"],
            "filters": [{"region": {"operation": "=", "type": "value", "value": "eu"}}]}}})

    def read(sql: str, role: str = ADMIN) -> pl.DataFrame:
        frame, status, message = DataReader(
            super_name=name, organization=ORG, query=sql,
        ).execute(role_name=role, engine=Engine.DUCKDB)
        assert status == Status.OK, message
        return frame

    def refused(sql: str, role: str) -> None:
        with pytest.raises(PermissionError, match="region"):
            DataReader(super_name=name, organization=ORG, query=sql).execute(
                role_name=role, engine=Engine.DUCKDB)

    def delete(table: str, keys) -> None:
        writer.write(role_name=ADMIN, simple_name=table, overwrite_columns=["id"],
                     data=pl.DataFrame({"id": list(keys)}), delete_only=True)

    yield SimpleNamespace(name=name, read=read, refused=refused, delete=delete)
    engine_duckdb.reset_shared_duckdb_state()


def _recorded(lake):
    """Make the thread's connection (a first read), then record on it."""
    assert lake.read("SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    state = engine_duckdb._shared_state()
    state["con"] = _Recording(state["con"])
    return state["con"]


def _views(con):
    return [name for (name,) in con.execute(
        "SELECT view_name FROM duckdb_views() WHERE NOT internal").fetchall()]


def test_a_read_sends_one_view_and_drops_it(lake):
    con = _recorded(lake)

    assert lake.read("SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    created, dropped = con.sent("CREATE OR REPLACE VIEW"), con.sent("DROP VIEW")
    assert len(created) == 1 and len(dropped) == 1
    view = created[0].split()[4]
    assert ONE_VIEW.fullmatch(view) and view in dropped[0]
    # every file of the table, once, and the system columns stripped in the same view
    assert created[0].count(".parquet'") == FILES
    assert "COLUMNS(c -> c NOT IN ('__rowid__', '__timestamp__'))" in created[0]
    assert _views(con) == []

    con.forget()
    assert lake.read("SELECT SUM(amount) AS s FROM orders WHERE region = 'us'")["s"][0] == sum(
        i * 10 for i in range(ROWS) if _region(i) == "us")
    assert lake.read("SELECT id, amount FROM orders WHERE id = 137").to_dicts() == [
        {"id": 137, "amount": 1370}]
    assert lake.read("SELECT * FROM orders ORDER BY id LIMIT 2").to_dicts() == [
        {"id": 0, "region": "eu", "amount": 0}, {"id": 1, "region": "us", "amount": 10}]
    assert len(con.sent("CREATE OR REPLACE VIEW")) == 3 and len(con.sent("DROP VIEW")) == 3
    assert _views(con) == []


def test_deleted_rows_stay_deleted_through_the_one_view(lake):
    con = _recorded(lake)
    gone = [3, 77, 150, 151]

    lake.delete("orders", gone[:2])
    con.forget()
    assert lake.read("SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS - 2
    # the first read after a delete builds the vector's table, then its one view
    assert len(con.sent("CREATE TABLE IF NOT EXISTS dv_")) == 1
    created = con.sent("CREATE OR REPLACE VIEW")
    assert len(created) == 1 and "ANTI JOIN dv_" in created[0] and "read_parquet(" not in created[0]

    con.forget()
    assert lake.read("SELECT id FROM orders WHERE id IN (3, 4, 77, 78) ORDER BY id")["id"].to_list() == [4, 78]
    assert con.sent("CREATE TABLE") == []                     # the table is found again
    assert len(con.sent("CREATE OR REPLACE VIEW")) == 1

    lake.delete("orders", gone[2:])                           # a second part of the vector
    assert lake.read("SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS - 4
    left = lake.read("SELECT id FROM orders ORDER BY id")["id"].to_list()
    assert left == [i for i in range(ROWS) if i not in gone]
    # the other table has no vector and is as it was
    assert lake.read("SELECT COUNT(*) AS n FROM customers")["n"][0] == ROWS
    assert _views(con) == []


def test_a_restricted_role_reads_through_its_own_view_over_the_one_view(lake):
    con = _recorded(lake)
    lake.delete("orders", [0, 3])                             # an eu row and another eu row
    eu = [i for i in range(ROWS) if _region(i) == "eu" and i not in (0, 3)]
    con.forget()

    frame = lake.read("SELECT id, amount FROM orders ORDER BY id", role=EU_READER)
    assert frame["id"].to_list() == eu and frame["amount"].to_list() == [i * 10 for i in eu]
    created = con.sent("CREATE OR REPLACE VIEW")
    assert len(created) == 2 and len(con.sent("DROP VIEW")) == 2
    below, role_view = created[0].split()[4], created[1].split()[4]
    assert ONE_VIEW.fullmatch(below) and role_view.startswith(f"rbac_{below}_")
    assert f"FROM {below} WHERE" in created[1] and "parquet_scan(" not in created[1]

    assert lake.read("SELECT COUNT(*) AS n FROM orders", role=EU_READER)["n"][0] == len(eu)
    assert lake.read("SELECT * FROM orders ORDER BY id LIMIT 1", role=EU_READER).to_dicts() == [
        {"id": eu[0], "amount": eu[0] * 10}]
    # what the role may not read it still cannot
    lake.refused("SELECT region FROM orders", role=EU_READER)
    lake.refused("SELECT id FROM orders WHERE region = 'us'", role=EU_READER)
    assert _views(con) == []
    # and the administrator reads every row that is left
    assert lake.read("SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS - 2


def test_a_join_has_a_view_for_each_table_and_a_self_join_one(lake):
    con = _recorded(lake)
    lake.delete("orders", [5])
    con.forget()

    joined = lake.read(
        "SELECT o.id, c.amount FROM orders o JOIN customers c ON o.id = c.id "
        "WHERE o.id BETWEEN 4 AND 6 ORDER BY o.id")
    assert joined.to_dicts() == [{"id": 4, "amount": 40}, {"id": 6, "amount": 60}]
    assert len(con.sent("CREATE OR REPLACE VIEW")) == 2 and len(con.sent("DROP VIEW")) == 2

    con.forget()
    twice = lake.read(
        "SELECT a.id, b.amount FROM orders a JOIN orders b ON a.id = b.id "
        "WHERE a.id BETWEEN 4 AND 6 ORDER BY a.id")
    assert twice.to_dicts() == [{"id": 4, "amount": 40}, {"id": 6, "amount": 60}]
    # the table named twice with the same columns: the same view, made once
    assert len(con.sent("CREATE OR REPLACE VIEW")) == 1
    assert _views(con) == []
