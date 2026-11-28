"""
A read sends the engine settings to DuckDB when they have changed, not always.

The organization's engine configuration (memory limit, threads, HTTP timeout,
file cache) is read from the catalog for every query, and the engine puts it in
force on the thread's connection before the query runs
(``apply_runtime_pragmas``).  It did that by sending every statement before
every query.  Sending them is cheap; what ``PRAGMA memory_limit`` does is not:
DuckDB flushes its allocator, and the query after it ran about 6 ms slower.

Here the real writer writes and the real reader reads, over the hermetic fake
Redis, with the configuration changed in the catalog the way the UI changes it.
What is counted is what the thread's connection is sent; what is checked is
that every read is right and that the configuration in force is the stored one.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import duckdb
import polars as pl
import pytest

from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
ROWS = 300
NAMES = ("memory_limit", "threads", "enable_external_file_cache")


class _Recording:
    """The thread's connection, recording what it is sent."""

    def __init__(self, con):
        self._con = con
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        return self._con.execute(sql, *args, **kwargs)

    def settings_sent(self):
        """The settings statements since the last call."""
        sent, self.sql = self.sql, []
        return [sql for sql in sent if sql.startswith(("SET ", "PRAGMA "))]

    def __getattr__(self, name):
        return getattr(self._con, name)


def _read(lake: str, query: str) -> pl.DataFrame:
    frame, status, message = DataReader(
        super_name=lake, organization=ORG, query=query,
    ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
    assert status == Status.OK, message
    return frame


def _in_force(con) -> dict:
    return dict(zip(NAMES, con.execute(
        "SELECT " + ", ".join(f"current_setting('{name}')" for name in NAMES)
    ).fetchone()))


def _as_duckdb_shows(memory_limit: str) -> str:
    reference = duckdb.connect()
    try:
        reference.execute(f"PRAGMA memory_limit='{memory_limit}'")
        return reference.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    finally:
        reference.close()


@pytest.fixture
def lake(hermetic_fakeredis):
    engine_duckdb.reset_shared_duckdb_state()
    name = f"settings_{uuid.uuid4().hex[:10]}"
    SuperTable(name, ORG)
    DataWriter(super_name=name, organization=ORG).write(
        role_name=ADMIN, simple_name="orders", overwrite_columns=[],
        data=pl.DataFrame({"id": list(range(ROWS)), "amount": [i * 10 for i in range(ROWS)]}))
    catalog = RedisCatalog()
    # the organization's configuration, stored as the UI stores it
    assert catalog.set_engine_config(ORG, "lite", {
        "duckdb_memory_limit": "256MB", "duckdb_threads": 2, "duckdb_external_cache_size": "1GB"})
    yield SimpleNamespace(name=name, catalog=catalog)
    engine_duckdb.reset_shared_duckdb_state()


def _recorded_connection(lake):
    """Make the first read, then put a recorder around the thread's connection.

    The recorder is a connection the engine has not seen, so the read after
    this is sent everything once, like the first read on any connection."""
    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    state = engine_duckdb._shared_state()
    state["con"] = _Recording(state["con"])
    return state["con"]


def test_reads_under_one_configuration_send_the_settings_once(lake):
    con = _recorded_connection(lake)
    stored = {"memory_limit": _as_duckdb_shows("256MB"), "threads": 2,
              "enable_external_file_cache": True}
    assert _in_force(con) == stored                           # the first read put it in force

    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    assert con.settings_sent()[:3] == ["PRAGMA memory_limit='256MB';", "SET threads=2;",
                                       "SET enable_external_file_cache=true;"]

    # every read after it: right, and not one settings statement
    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    assert _read(lake.name, "SELECT SUM(amount) AS s FROM orders")["s"][0] == sum(
        i * 10 for i in range(ROWS))
    assert _read(lake.name, "SELECT id FROM orders WHERE id = 7")["id"].to_list() == [7]
    assert con.settings_sent() == []
    assert _in_force(con) == stored


def test_a_configuration_changed_in_the_catalog_is_in_force_for_the_next_read(lake):
    con = _recorded_connection(lake)
    _read(lake.name, "SELECT COUNT(*) AS n FROM orders")
    con.settings_sent()

    # the memory limit and the thread count are raised; the cache is as it was
    assert lake.catalog.set_engine_config(ORG, "lite", {
        "duckdb_memory_limit": "512MB", "duckdb_threads": 3, "duckdb_external_cache_size": "1GB"})
    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    assert con.settings_sent() == ["PRAGMA memory_limit='512MB';", "SET threads=3;"]
    assert _in_force(con) == {"memory_limit": _as_duckdb_shows("512MB"), "threads": 3,
                              "enable_external_file_cache": True}

    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    assert con.settings_sent() == []

    # only the cache's size is changed: the switch is sent again, and with it
    # the size, on a DuckDB build that can cap the cache (no released one can)
    assert lake.catalog.set_engine_config(ORG, "lite", {
        "duckdb_memory_limit": "512MB", "duckdb_threads": 3, "duckdb_external_cache_size": "2GB"})
    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    sent = con.settings_sent()
    assert sent[0] == "SET enable_external_file_cache=true;"
    assert sent[1:] in ([], ["SET external_file_cache_max_size='2GB';"])
    assert _in_force(con) == {"memory_limit": _as_duckdb_shows("512MB"), "threads": 3,
                              "enable_external_file_cache": True}

    # and all of it is put back
    assert lake.catalog.set_engine_config(ORG, "lite", {
        "duckdb_memory_limit": "256MB", "duckdb_threads": 2, "duckdb_external_cache_size": "1GB"})
    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    assert con.settings_sent()[:3] == ["PRAGMA memory_limit='256MB';", "SET threads=2;",
                                       "SET enable_external_file_cache=true;"]
    assert _in_force(con) == {"memory_limit": _as_duckdb_shows("256MB"), "threads": 2,
                              "enable_external_file_cache": True}


def test_a_new_connection_is_sent_the_configuration(lake):
    """The connection is replaced (an error, a reset): the record went with it."""
    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    first = engine_duckdb._shared_state()["con"]

    engine_duckdb.reset_shared_duckdb_state()
    assert _read(lake.name, "SELECT COUNT(*) AS n FROM orders")["n"][0] == ROWS
    second = engine_duckdb._shared_state()["con"]
    assert second is not first
    assert _in_force(second) == {"memory_limit": _as_duckdb_shows("256MB"), "threads": 2,
                                 "enable_external_file_cache": True}
