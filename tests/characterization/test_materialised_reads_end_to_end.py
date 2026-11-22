"""``SUPERTABLE_DUCKDB_MATERIALIZE=table`` behind a real reader.

With the setting a reflection is a table, not a view. It used to be created
under the name every query of that table shared and never dropped, so the
second read of a table on a connection failed with "Table ... already exists".
A table is written and read here with the real writer, reader and RBAC on
LOCAL storage, in both modes, and the two modes have to say the same.

``supertable/engine/tests/test_presign_fallback_and_materialised_reads.py``
looks at the engine's connection itself: what is created, and what is left.
"""

from __future__ import annotations

import dataclasses
import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable.config.settings import settings
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine import engine_common
from supertable.engine.engine_enum import Engine
from supertable.rbac.role_manager import RoleManager
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads ``id`` and ``region`` of the rows in ``eu``.
READER = "eu_two_columns"

QUERIES = [
    "SELECT id, region FROM emp ORDER BY id",
    "SELECT count(*) AS n FROM emp",
    "SELECT region, count(*) AS n FROM emp GROUP BY region ORDER BY region",
    "SELECT a.id FROM emp a JOIN emp b ON a.id = b.id WHERE b.id > 1 ORDER BY a.id",
    "WITH w AS (SELECT id FROM emp) SELECT count(*) AS n FROM w",
]


@pytest.fixture
def lake(hermetic_fakeredis):
    """A table with a deletion vector, and a role with a column list and a
    row filter on it."""
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"mat_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame({"id": [1, 2, 3], "region": ["eu", "us", "eu"],
                           "salary": [40_000, 900_000, 55_000]}))
    # an overwrite by key: the first row for id 2 goes into the deletion vector
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=["id"],
        data=pl.DataFrame({"id": [2], "region": ["apac"], "salary": [910_000]}))
    RoleManager(super_name=sup, organization=ORG, actor_role_name=ADMIN).create_role(
        {"role_name": READER, "role": "reader", "tables": {
            "emp": {"columns": ["id", "region"],
                    "filters": {"region": {"operation": "=", "type": "value",
                                           "value": "eu"}}}}})

    def read(sql: str, role: str = ADMIN):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=role, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return frame.to_dicts()

    yield SimpleNamespace(sup=sup, read=read)
    engine_duckdb.reset_shared_duckdb_state()


def _materialise(monkeypatch):
    monkeypatch.setattr(
        engine_common, "settings",
        dataclasses.replace(settings, SUPERTABLE_DUCKDB_MATERIALIZE="table"))
    assert engine_common.reflections_are_materialised() is True


def _left_on_the_connection():
    con = engine_duckdb._shared_state()["con"]
    tables = con.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE table_name LIKE 'st\\_%' ESCAPE '\\'").fetchone()[0]
    views = con.execute(
        "SELECT count(*) FROM duckdb_views() WHERE NOT internal").fetchone()[0]
    return tables, views


def test_every_read_says_the_same_materialised_as_it_does_through_views(lake, monkeypatch):
    through_views = [lake.read(sql) for sql in QUERIES]
    assert through_views[0] == [
        {"id": 1, "region": "eu"}, {"id": 2, "region": "apac"}, {"id": 3, "region": "eu"}]
    _materialise(monkeypatch)
    # twice: the second read of a table on the connection is the one that failed
    for _ in range(2):
        assert [lake.read(sql) for sql in QUERIES] == through_views
    assert _left_on_the_connection() == (0, 0)


def test_a_role_reads_its_own_rows_and_columns_from_a_materialised_table(lake, monkeypatch):
    _materialise(monkeypatch)
    for _ in range(2):
        assert lake.read("SELECT * FROM emp ORDER BY id", role=READER) == [
            {"id": 1, "region": "eu"}, {"id": 3, "region": "eu"}]
    with pytest.raises(PermissionError, match="salary"):
        DataReader(super_name=lake.sup, organization=ORG,
                   query="SELECT salary FROM emp").execute(
            role_name=READER, engine=Engine.DUCKDB)
    assert _left_on_the_connection() == (0, 0)


def test_a_write_between_two_materialised_reads_is_seen_by_the_second(lake, monkeypatch):
    _materialise(monkeypatch)
    assert lake.read("SELECT count(*) AS n FROM emp") == [{"n": 3}]
    DataWriter(super_name=lake.sup, organization=ORG).write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame({"id": [4], "region": ["eu"], "salary": [1]}))
    assert lake.read("SELECT count(*) AS n FROM emp") == [{"n": 4}]
