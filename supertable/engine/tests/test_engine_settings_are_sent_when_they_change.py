"""
A query sends an engine setting to DuckDB only when its value has changed.

The engine configuration (memory limit, thread count, HTTP timeout, external
file cache) is resolved for every query, and ``apply_runtime_pragmas`` puts it
in force on the thread's connection before the query runs.  It used to send
every statement before every query, whatever the values: three or four
statements, and ``PRAGMA memory_limit`` also makes DuckDB flush its allocator,
so the query after it ran slower.  Measured: 4 to 20 ms of a read, 8 of a point
lookup's 50.

It now remembers, per connection, what it last put in force, and sends a
statement only for a value that differs.  What that must not cost:

* a value changed in the configuration is in force for the very next query;
* a statement DuckDB refuses is sent, and logged, again with the next query;
* whatever else sets one of these settings on the connection
  (``init_connection``, ``configure_httpfs_and_s3``) makes it send all of them
  again;
* a query cannot set them itself, so nothing changes them unseen.

The first group runs on a connection that only records, so the statements can
be compared exactly; the second on real DuckDB, where what counts is the
setting in force, on the connection and on a cursor of it.
"""

from __future__ import annotations

import dataclasses
import gc
import logging
import weakref

import duckdb
import pytest

import supertable.engine.engine_common as ec
from supertable.config.settings import settings as real_settings
from supertable.engine.engine_config import EngineRuntimeConfig
from supertable.system_query import assert_read_only

CONFIG = EngineRuntimeConfig(
    engine_lite_max_bytes=0, engine_spark_min_bytes=0, engine_freshness_sec=0,
    duckdb_memory_limit="256MB", duckdb_io_multiplier=1.0, duckdb_threads=2,
    duckdb_http_timeout=7, duckdb_external_cache_size="",
)

MEMORY = "PRAGMA memory_limit='256MB';"
THREADS = "SET threads=2;"
TIMEOUT = "SET GLOBAL http_timeout=7;"
CACHE_OFF = "SET enable_external_file_cache=false;"
CACHE_ON = "SET enable_external_file_cache=true;"
EVERYTHING = [MEMORY, THREADS, TIMEOUT, CACHE_OFF]


def _with(**changes) -> EngineRuntimeConfig:
    return dataclasses.replace(CONFIG, **changes)


class _Connection:
    """Records what it is sent; refuses the statements that start with *refuse*."""

    def __init__(self, refuse=()):
        self.refuse = tuple(refuse)
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        if self.refuse and str(sql).startswith(self.refuse):
            raise duckdb.Error(f"refused: {sql}")
        return self

    def sent(self):
        """The statements since the last call."""
        sent, self.sql = self.sql, []
        return sent


@pytest.fixture
def no_cap(monkeypatch):
    """A DuckDB build without ``external_file_cache_max_size``: every released one."""
    monkeypatch.setattr(ec, "_external_file_cache_cappable", lambda con: False)


@pytest.fixture
def with_cap(monkeypatch):
    monkeypatch.setattr(ec, "_external_file_cache_cappable", lambda con: True)


# --------------------------------------------------------------------------
# What is sent
# --------------------------------------------------------------------------

def test_the_first_query_on_a_connection_sends_every_setting(no_cap):
    con = _Connection()
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == EVERYTHING


def test_a_query_with_the_same_configuration_sends_nothing(no_cap):
    con = _Connection()
    ec.apply_runtime_pragmas(con, CONFIG)
    con.sent()

    for _ in range(3):
        ec.apply_runtime_pragmas(con, CONFIG)
    # the configuration is resolved anew for every query: an equal one is the same
    ec.apply_runtime_pragmas(con, _with())
    # ... and so is one that only says the same differently
    ec.apply_runtime_pragmas(con, _with(duckdb_memory_limit="256mb", duckdb_http_timeout=7.0))
    assert con.sent() == []


@pytest.mark.parametrize("change, statements, back", [
    ({"duckdb_memory_limit": "512MB"}, ["PRAGMA memory_limit='512MB';"], [MEMORY]),
    ({"duckdb_threads": 4}, ["SET threads=4;"], [THREADS]),
    ({"duckdb_http_timeout": 9}, ["SET GLOBAL http_timeout=9;"], [TIMEOUT]),
    ({"duckdb_external_cache_size": "1GB"}, [CACHE_ON], [CACHE_OFF]),
    ({"duckdb_memory_limit": "2", "duckdb_threads": 8},
     ["PRAGMA memory_limit='2GB';", "SET threads=8;"], [MEMORY, THREADS]),
], ids=["memory", "threads", "timeout", "cache", "memory-and-threads"])
def test_a_changed_setting_is_sent_and_nothing_else(no_cap, change, statements, back):
    con = _Connection()
    ec.apply_runtime_pragmas(con, CONFIG)
    con.sent()

    ec.apply_runtime_pragmas(con, _with(**change))
    assert con.sent() == statements
    ec.apply_runtime_pragmas(con, _with(**change))            # the query after it
    assert con.sent() == []
    # changed back: what is remembered is the value in force, not every value seen
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == back


def test_a_derived_thread_count_is_sent_when_what_it_is_derived_from_changes(no_cap, monkeypatch):
    """No explicit thread count: it follows the memory limit and the multiplier."""
    monkeypatch.setattr(
        ec, "_derive_thread_count",
        lambda memory_limit, multiplier=None: {"256MB": 1, "4GB": 10}[memory_limit] * int(multiplier),
    )
    con = _Connection()
    derived = _with(duckdb_threads=None, duckdb_http_timeout=None)

    ec.apply_runtime_pragmas(con, derived)
    assert con.sent() == [MEMORY, "SET threads=1;", CACHE_OFF]
    ec.apply_runtime_pragmas(con, derived)
    assert con.sent() == []

    ec.apply_runtime_pragmas(con, dataclasses.replace(derived, duckdb_io_multiplier=3.0))
    assert con.sent() == ["SET threads=3;"]                   # the limit is unchanged
    ec.apply_runtime_pragmas(con, dataclasses.replace(
        derived, duckdb_io_multiplier=3.0, duckdb_memory_limit="4GB"))
    assert con.sent() == ["PRAGMA memory_limit='4GB';", "SET threads=30;"]


def test_no_timeout_in_the_configuration_leaves_the_timeout_alone(no_cap):
    """``None`` is "leave DuckDB's": nothing is sent for it, before or after."""
    con = _Connection()
    ec.apply_runtime_pragmas(con, CONFIG)
    con.sent()

    ec.apply_runtime_pragmas(con, _with(duckdb_http_timeout=None))
    assert con.sent() == []
    # 7 was never taken back, so asking for 7 again has nothing to send
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == []


def test_the_cache_size_is_sent_where_the_build_can_cap_it(with_cap):
    con = _Connection()
    capped = _with(duckdb_external_cache_size="1GB")

    ec.apply_runtime_pragmas(con, capped)
    assert con.sent()[-2:] == [CACHE_ON, "SET external_file_cache_max_size='1GB';"]
    ec.apply_runtime_pragmas(con, capped)
    assert con.sent() == []

    ec.apply_runtime_pragmas(con, _with(duckdb_external_cache_size="2GB"))
    assert con.sent() == [CACHE_ON, "SET external_file_cache_max_size='2GB';"]
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == [CACHE_OFF]


def test_no_configuration_sends_nothing_and_changes_nothing(no_cap):
    con = _Connection()
    ec.apply_runtime_pragmas(con, None)                       # a caller without one
    assert con.sent() == []

    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == EVERYTHING
    ec.apply_runtime_pragmas(con, None)
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == []


# --------------------------------------------------------------------------
# What DuckDB refuses is not remembered
# --------------------------------------------------------------------------

def test_a_refused_statement_is_sent_again_with_every_query(no_cap):
    """The timeout on a connection without httpfs: refused today, every time."""
    con = _Connection(refuse=("SET GLOBAL http_timeout",))
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == EVERYTHING

    for _ in range(3):
        ec.apply_runtime_pragmas(con, CONFIG)
        assert con.sent() == [TIMEOUT]                        # that one, and only that one

    con.refuse = ()                                           # httpfs is there now
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == [TIMEOUT]
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == []


def test_a_refused_memory_limit_is_logged_with_every_query(no_cap, caplog):
    con = _Connection(refuse=("PRAGMA memory_limit",))
    with caplog.at_level(logging.WARNING, logger=ec.logger.name):
        for _ in range(3):
            ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent().count(MEMORY) == 3
    rejected = [r for r in caplog.records if "memory_limit='256MB' rejected" in r.getMessage()]
    assert len(rejected) == 3


def test_a_refused_cache_switch_is_logged_with_every_query(no_cap, caplog):
    con = _Connection(refuse=("SET enable_external_file_cache",))
    on = _with(duckdb_external_cache_size="1GB")
    with caplog.at_level(logging.WARNING, logger=ec.logger.name):
        for _ in range(2):
            ec.apply_runtime_pragmas(con, on)
    assert con.sent().count(CACHE_ON) == 2
    failed = [r for r in caplog.records if "external file cache config failed" in r.getMessage()]
    assert len(failed) == 2


def test_after_a_refused_change_the_old_value_is_sent_again(no_cap):
    """A refused statement leaves it unknown what is in force: nothing is assumed."""
    con = _Connection()
    ec.apply_runtime_pragmas(con, CONFIG)
    con.sent()

    con.refuse = ("SET threads=64",)
    ec.apply_runtime_pragmas(con, _with(duckdb_threads=64))
    assert con.sent() == ["SET threads=64;"]
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == [THREADS]


# --------------------------------------------------------------------------
# Whose record it is
# --------------------------------------------------------------------------

def test_every_connection_is_sent_its_own(no_cap):
    first, second = _Connection(), _Connection()
    ec.apply_runtime_pragmas(first, CONFIG)
    ec.apply_runtime_pragmas(second, CONFIG)
    assert first.sent() == second.sent() == EVERYTHING

    ec.apply_runtime_pragmas(second, _with(duckdb_threads=4))
    assert second.sent() == ["SET threads=4;"]
    ec.apply_runtime_pragmas(first, CONFIG)                   # the other one is as it was
    assert first.sent() == []


def test_a_connection_that_is_gone_is_not_kept(no_cap):
    gc.collect()                                              # what earlier tests left
    before = len(ec._RUNTIME_APPLIED)
    con = _Connection()
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con in ec._RUNTIME_APPLIED
    assert len(ec._RUNTIME_APPLIED) == before + 1

    gone = weakref.ref(con)
    del con
    gc.collect()
    assert gone() is None                                     # the record did not hold it
    assert len(ec._RUNTIME_APPLIED) == before


def test_a_connection_that_cannot_be_remembered_is_sent_everything(no_cap):
    """As before the change, rather than an error: no weak reference, no record."""

    class _Slotted(_Connection):
        __slots__ = ()
        __hash__ = None                                       # nor a dictionary key

    con = _Slotted()
    for _ in range(2):
        ec.apply_runtime_pragmas(con, CONFIG)
        assert con.sent() == EVERYTHING
    ec.forget_runtime_pragmas(con)                            # nothing to forget, no error


def test_forgetting_a_connection_makes_the_next_query_send_everything(no_cap):
    con = _Connection()
    ec.forget_runtime_pragmas(con)                            # never seen: no error
    ec.apply_runtime_pragmas(con, CONFIG)
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == EVERYTHING

    ec.forget_runtime_pragmas(con)
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == EVERYTHING
    ec.apply_runtime_pragmas(con, CONFIG)
    assert con.sent() == []


# --------------------------------------------------------------------------
# On real DuckDB: what is in force
# --------------------------------------------------------------------------

LOCAL = _with(duckdb_http_timeout=None)                       # no httpfs is needed for these
NAMES = ("memory_limit", "threads", "enable_external_file_cache")


class _Recording:
    """A real connection that also records what it is sent."""

    def __init__(self, con):
        self._con = con
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        return self._con.execute(sql, *args, **kwargs)

    def settings_sent(self):
        sent, self.sql = self.sql, []
        return [sql for sql in sent if sql.startswith(("SET ", "PRAGMA "))]

    def __getattr__(self, name):
        return getattr(self._con, name)


def _in_force(con, names=NAMES):
    return dict(zip(names, con.execute(
        "SELECT " + ", ".join(f"current_setting('{name}')" for name in names)
    ).fetchone()))


def _as_duckdb_shows(memory_limit: str) -> str:
    reference = duckdb.connect()
    try:
        reference.execute(f"PRAGMA memory_limit='{memory_limit}'")
        return reference.execute("SELECT current_setting('memory_limit')").fetchone()[0]
    finally:
        reference.close()


@pytest.fixture
def connection(duckdb_con, tmp_path, monkeypatch):
    """A connection as the engine makes one, with the settings' own values
    different from ``LOCAL``'s so that it shows which of the two is in force."""
    monkeypatch.setattr(ec, "settings", dataclasses.replace(
        real_settings,
        SUPERTABLE_DUCKDB_MEMORY_LIMIT="768MB",
        SUPERTABLE_DUCKDB_THREADS="3",
        SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE="1GB",
    ))
    ec.init_connection(duckdb_con, temp_dir=str(tmp_path / "spill"))
    return _Recording(duckdb_con)


def test_the_configuration_stays_in_force_over_queries_that_send_nothing(connection):
    wanted = {"memory_limit": _as_duckdb_shows("256MB"), "threads": 2,
              "enable_external_file_cache": False}
    assert _in_force(connection) != wanted                    # the settings' start differs

    ec.apply_runtime_pragmas(connection, LOCAL)
    assert _in_force(connection) == wanted
    assert connection.settings_sent() == [MEMORY, THREADS, CACHE_OFF]

    for _ in range(3):
        ec.apply_runtime_pragmas(connection, LOCAL)
        # a stream runs its query on a cursor, which is a session of its own
        cursor = connection.cursor()
        try:
            assert _in_force(cursor) == wanted
        finally:
            cursor.close()
    assert _in_force(connection) == wanted
    assert connection.settings_sent() == []


def test_a_changed_configuration_is_in_force_for_the_next_query(connection):
    ec.apply_runtime_pragmas(connection, LOCAL)
    connection.settings_sent()

    changed = dataclasses.replace(
        LOCAL, duckdb_memory_limit="512MB", duckdb_threads=4, duckdb_external_cache_size="1GB")
    ec.apply_runtime_pragmas(connection, changed)
    cursor = connection.cursor()
    try:
        assert _in_force(cursor) == {"memory_limit": _as_duckdb_shows("512MB"), "threads": 4,
                                     "enable_external_file_cache": True}
    finally:
        cursor.close()

    ec.apply_runtime_pragmas(connection, LOCAL)               # and back
    assert _in_force(connection) == {"memory_limit": _as_duckdb_shows("256MB"), "threads": 2,
                                     "enable_external_file_cache": False}


def test_init_connection_makes_the_next_query_send_everything(connection, tmp_path):
    """``init_connection`` sets the same settings from the process's settings."""
    wanted = {"memory_limit": _as_duckdb_shows("256MB"), "threads": 2,
              "enable_external_file_cache": False}
    ec.apply_runtime_pragmas(connection, LOCAL)
    assert _in_force(connection) == wanted

    ec.init_connection(connection, temp_dir=str(tmp_path / "spill"))
    assert _in_force(connection) == {"memory_limit": _as_duckdb_shows("768MB"), "threads": 3,
                                     "enable_external_file_cache": True}
    connection.settings_sent()

    ec.apply_runtime_pragmas(connection, LOCAL)               # the same configuration as before
    assert connection.settings_sent() == [MEMORY, THREADS, CACHE_OFF]
    assert _in_force(connection) == wanted


def test_configuring_object_storage_makes_the_next_query_send_everything(
        connection, monkeypatch, tmp_path):
    """``configure_httpfs_and_s3`` sets the timeout and the cache switch from
    the process's settings.  Whether it gets through or stops half-way (this
    machine may have no httpfs), the configuration is sent again after it."""
    monkeypatch.setattr(ec, "get_app_home", lambda: str(tmp_path))
    ec.reset_httpfs_configuration()
    ec.apply_runtime_pragmas(connection, LOCAL)
    connection.settings_sent()

    try:
        ec.configure_httpfs_and_s3(connection, ["s3://lake/acme/part-0.parquet"])
    except Exception:
        pass
    finally:
        ec.reset_httpfs_configuration()
    connection.settings_sent()

    ec.apply_runtime_pragmas(connection, LOCAL)
    assert connection.settings_sent() == [MEMORY, THREADS, CACHE_OFF]
    assert _in_force(connection) == {"memory_limit": _as_duckdb_shows("256MB"), "threads": 2,
                                     "enable_external_file_cache": False}


def test_local_paths_configure_nothing_and_nothing_is_sent_again(connection):
    """A read of local files: ``configure_httpfs_and_s3`` returns at once."""
    ec.apply_runtime_pragmas(connection, LOCAL)
    connection.settings_sent()

    ec.configure_httpfs_and_s3(connection, ["/data/lake/acme/part-0.parquet"])
    ec.apply_runtime_pragmas(connection, LOCAL)
    assert connection.settings_sent() == []


def test_a_setting_changed_by_hand_is_put_back_once_the_record_is_dropped(connection):
    """The record is of what this function sent.  Code that sets one of these
    settings itself has to say so, as the two functions above do."""
    ec.apply_runtime_pragmas(connection, LOCAL)
    connection.execute("SET threads=5")
    ec.forget_runtime_pragmas(connection)

    ec.apply_runtime_pragmas(connection, LOCAL)
    assert _in_force(connection)["threads"] == 2


@pytest.mark.parametrize("query", [
    "SET threads=64",
    "SET GLOBAL threads=64",
    "PRAGMA memory_limit='64GB'",
    "RESET memory_limit",
    "SELECT 1 AS one FROM orders; SET threads=64",
])
def test_a_query_cannot_set_them_itself(query):
    """What the record relies on: the read gate admits reads of named tables,
    so no query changes a setting between two calls of this function."""
    with pytest.raises(ValueError):
        assert_read_only(query)


def test_duckdb_has_no_function_that_sets_a_setting(duckdb_con):
    """... and a read cannot do it through a function either.  The gate admits
    scalar calls it does not know, so this is DuckDB's part: PostgreSQL's
    ``set_config`` does not exist in it, and nothing else of that kind does.
    A DuckDB that gains one has to be refused it by the gate, as
    ``current_setting`` is."""
    with pytest.raises(duckdb.CatalogException):
        duckdb_con.execute("SELECT set_config('threads', '64', false)")
    named_like_one = {row[0] for row in duckdb_con.execute(
        "SELECT DISTINCT function_name FROM duckdb_functions() "
        "WHERE function_type IN ('scalar', 'macro', 'aggregate') "
        "AND (function_name ILIKE '%config%' OR function_name ILIKE '%setting%' "
        "     OR function_name ILIKE 'set%')"
    ).fetchall()}
    # the visibility macro, the reader of a setting, a bit operation, the random seed
    assert named_like_one == {"pg_ts_config_is_visible", "current_setting", "set_bit", "setseed"}
