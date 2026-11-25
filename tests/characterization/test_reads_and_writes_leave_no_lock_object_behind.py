"""
A read or a write leaves no lock object behind.

Every ``RedisCatalog`` builds a ``RedisLocking``. A write builds four catalogs
(six when it also builds its ``DataWriter``) and a read builds six. Each lock
object used to register its own exit callback, and ``atexit`` keeps what it is
given until the process ends: none was ever freed. A long-running process kept
11 to 15 KB more for every read and every write, and its list of exit callbacks
grew by four to six each time.

Here the real writer and reader run over the hermetic fake Redis: appends, an
upsert, a delete, a compaction and reads, among them a restricted role's and a
refused one. Afterwards the process holds as many lock objects, and as many
exit callbacks, as it did before them; the table lock is free; and the rows
are the rows those operations should leave.
"""

from __future__ import annotations

import atexit
import gc
import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable import redis_keys as RK
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.locking import redis_lock
from supertable.locking.redis_lock import RedisLocking
from supertable.rbac.role_manager import RoleManager
from supertable.redis_catalog import RedisCatalog
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
READER = "eu_only"


def _live_lock_objects() -> int:
    gc.collect()
    return sum(1 for obj in gc.get_objects() if type(obj) is RedisLocking)


def _rows(lake: str, sql: str, role: str = ADMIN):
    frame, status, message = DataReader(
        super_name=lake, organization=ORG, query=sql,
    ).execute(role_name=role, engine=Engine.DUCKDB)
    assert status == Status.OK, message
    return frame.to_dicts()


def _batch(first: int, count: int = 4) -> pl.DataFrame:
    ids = list(range(first, first + count))
    return pl.DataFrame({"id": ids, "region": ["eu" if i % 2 else "us" for i in ids],
                         "amount": [i * 10 for i in ids]})


@pytest.fixture
def lake(hermetic_fakeredis):
    """A lake with one table, already written to and read from.

    Whatever the first operations of a process keep for good (script loads,
    engine state, caches) is kept before any test starts counting.
    """
    engine_duckdb.reset_shared_duckdb_state()
    name = f"locks_{uuid.uuid4().hex[:10]}"
    SuperTable(name, ORG)
    writer = DataWriter(super_name=name, organization=ORG)
    for first in (0, 4):
        writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                     data=_batch(first))
    writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["id"],
                 data=_batch(0, 1))
    RoleManager(super_name=name, organization=ORG, actor_role_name=ADMIN).create_role(
        {"role_name": READER, "role": "reader", "tables": {
            "orders": {"columns": ["id", "region"],
                       "filters": {"region": {"operation": "=", "type": "value",
                                              "value": "eu"}}}}})
    _rows(name, "SELECT COUNT(*) AS n FROM orders")
    _rows(name, "SELECT id FROM orders", role=READER)
    yield SimpleNamespace(name=name, writer=writer, redis=hermetic_fakeredis)
    engine_duckdb.reset_shared_duckdb_state()


def test_writes_leave_no_lock_object_behind(lake):
    objects, callbacks = _live_lock_objects(), atexit._ncallbacks()

    for first in (8, 12, 16, 20, 24):                          # appends, one writer
        lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                          data=_batch(first))
    for first in (28, 32, 36):                                 # a writer per write
        DataWriter(super_name=lake.name, organization=ORG).write(
            role_name=ADMIN, simple_name="orders", overwrite_columns=[], data=_batch(first))
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["id"],
                      data=pl.DataFrame({"id": [8, 9], "region": ["eu", "eu"],
                                         "amount": [-8, -9]}))  # upsert
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["id"],
                      data=pl.DataFrame({"id": [36, 37, 38, 39]}), delete_only=True)
    lake.writer.compact(role_name=ADMIN, simple_name="orders")
    lake.writer.write(role_name=ADMIN, simple_name="second", overwrite_columns=[],
                      data=_batch(100))                         # a table is created

    assert _live_lock_objects() == objects
    assert atexit._ncallbacks() == callbacks
    assert _rows(lake.name, "SELECT COUNT(*) AS n, SUM(amount) AS total FROM orders") == [
        {"n": 36, "total": sum(i * 10 for i in range(36)) - 80 - 90 - 8 - 9}]
    assert _rows(lake.name, "SELECT COUNT(*) AS n FROM second") == [{"n": 4}]


def test_reads_leave_no_lock_object_behind(lake):
    objects, callbacks = _live_lock_objects(), atexit._ncallbacks()

    for _ in range(4):
        assert _rows(lake.name, "SELECT COUNT(*) AS n FROM orders") == [{"n": 8}]
    assert _rows(lake.name, "SELECT id FROM orders WHERE id = 5") == [{"id": 5}]
    assert sorted(row["id"] for row in _rows(lake.name, "SELECT id FROM orders", role=READER)) \
        == [1, 3, 5, 7]
    assert _rows(lake.name, "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id "
                            "WHERE a.id = 2") == [{"id": 2}]

    assert _live_lock_objects() == objects
    assert atexit._ncallbacks() == callbacks


def test_a_refused_write_and_a_refused_read_leave_none_either(lake):
    objects, callbacks = _live_lock_objects(), atexit._ncallbacks()

    for _ in range(3):
        with pytest.raises(PermissionError):
            lake.writer.write(role_name=READER, simple_name="orders", overwrite_columns=[],
                              data=_batch(500))
        with pytest.raises(ValueError):
            lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["nope"],
                              data=_batch(500))
        _frame, status, _message = DataReader(
            super_name=lake.name, organization=ORG, query="SELECT * FROM no_such_table",
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status == Status.ERROR

    assert _live_lock_objects() == objects
    assert atexit._ncallbacks() == callbacks
    assert _rows(lake.name, "SELECT COUNT(*) AS n FROM orders") == [{"n": 8}]


def test_no_write_leaves_its_table_lock_or_a_holder(lake):
    """What the exit hook exists for must be empty after ordinary writes: every
    write released its lock, so nothing is left for the hook to release."""
    holders = set(redis_lock._holders)

    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                      data=_batch(8))
    with pytest.raises(ValueError):
        lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=["nope"],
                          data=_batch(500))

    assert set(redis_lock._holders) == holders
    assert lake.redis.get(RK.lock_leaf(ORG, lake.name, "orders")) is None


def test_a_table_lock_held_when_the_process_exits_is_released(lake):
    """The promise the per-object registration made, kept by the one hook."""
    catalog = RedisCatalog()
    token = catalog.acquire_simple_lock(ORG, lake.name, "orders", ttl_s=30, timeout_s=2)
    assert token
    key = RK.lock_leaf(ORG, lake.name, "orders")
    assert lake.redis.get(key) is not None
    del catalog                                   # its owner forgets it; the lock is still held

    redis_lock._release_held_locks()              # what atexit runs

    assert lake.redis.get(key) is None
    lake.writer.write(role_name=ADMIN, simple_name="orders", overwrite_columns=[],
                      data=_batch(8))             # the next writer does not wait out a TTL
    assert _rows(lake.name, "SELECT COUNT(*) AS n FROM orders") == [{"n": 12}]
