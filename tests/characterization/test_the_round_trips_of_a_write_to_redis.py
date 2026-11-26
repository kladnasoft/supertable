"""
The round trips a write makes to Redis, and that fewer of them change nothing.

A write to an existing table went to Redis eighteen times here (nineteen with
monitoring on, which this harness has off).  Four of those were redundant:

* the table was opened with an existence check and its snapshot then read, and
  each of the two first read the lake's root document: four round trips for
  one catalog entry, now two (``SimpleTable(..., keep_leaf=True)``);
* after the commit the schema document was stored, the table's name added to
  the lake's table names and the lake's mirror formats read, one command a
  round trip: now one for the three
  (``RedisCatalog.store_schema_and_read_mirrors``).

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage, and every round trip is recorded.  The sequence of an append is
pinned, so that a round trip added later shows up as a failure here.  The way
it was is still reachable (a table opened without ``keep_leaf``, a catalog
without the new method), and the same writes are run both ways: they must
leave the same rows, the same schema document, the same table names and the
same mirror.

``supertable/tests/test_a_table_opened_for_a_write_reads_its_entry_once.py``
and ``supertable/tests/test_what_follows_a_commit_goes_to_redis_in_one_round_trip.py``
compare the two changed pieces with the code as it was, state by state.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from types import SimpleNamespace

import polars as pl
import pytest
import redis
from redis.client import Pipeline

from supertable import data_writer as data_writer_module
from supertable import redis_keys as RK
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.mirroring.mirror_formats import MirrorFormats
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
TABLE = "orders"

# An append to a table that exists, in order.
APPEND = [
    "GET meta:root",                                    # is the lake read-only
    "EXISTS rbac:roles:meta",                           # the role
    "SMEMBERS rbac:roles:type:doc:superadmin",
    "HGET rbac:roles:name_to_id",
    "HGETALL rbac:roles:doc:<id>",
    "INCRBY meta:rowid_seq:doc:orders",                 # row ids
    "SET lock:leaf:doc:orders",                         # the table lock
    "GET meta:root",                                    # the table's entry, once
    "GET meta:leaf:doc:orders",
    "GET lock:leaf:doc:orders",                         # still locked?
    "EVALSHA meta:leaf:doc:orders",                     # the commit
    "EVALSHA meta:root",
    ["SET schema:doc:orders", "SADD meta:table_names", "GET meta:mirrors"],
    "EVALSHA lock:leaf:doc:orders",                     # unlock
]
# The same append before: the entry asked for and then read, and three trips
# after the commit.
APPEND_AS_IT_WAS = APPEND[:7] + [
    "GET meta:root", "EXISTS meta:leaf:doc:orders",
    "GET meta:root", "GET meta:leaf:doc:orders",
] + APPEND[9:12] + [
    "SET schema:doc:orders", "SADD meta:table_names", "GET meta:mirrors",
    "EVALSHA lock:leaf:doc:orders",
]


def _rows(first: int, count: int = 3) -> pl.DataFrame:
    ids = list(range(first, first + count))
    return pl.DataFrame({"id": ids, "region": ["eu" if i % 2 else "us" for i in ids],
                         "amount": [i * 10 for i in ids]})


class _OpenedAsItWas(SimpleTable):
    """A table opened the way every caller but ``DataWriter.write`` opens it."""

    def __init__(self, super_table, simple_name, *, create_if_missing=True, keep_leaf=False):
        super().__init__(super_table, simple_name, create_if_missing=create_if_missing)


def _lake(monkeypatch, as_it_was: bool = False):
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"trips_{uuid.uuid4().hex[:12]}"
    super_table = SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)
    trips: list = []
    state = {"recording": False, "fail": None}
    lake_prefix = RK.meta_root(ORG, sup)[:-len("meta:root")]
    role_doc = re.compile(r"(rbac:roles:doc:)[0-9a-f]{32}$")

    def short(key) -> str:
        key = str(key)
        key = key[len(lake_prefix):] if key.startswith(lake_prefix) else key
        return role_doc.sub(r"\1<id>", key)

    def name(args) -> str:
        command = str(args[0]).upper()
        if command == "EVALSHA":
            return "EVALSHA " + short(args[3])
        return command + (" " + short(args[1]) if len(args) > 1 else "")

    def note(trip) -> None:
        if not state["recording"]:
            return
        if trip == "SCRIPT LOAD" or (isinstance(trip, str) and trip.startswith("SCRIPT ")):
            # a script this Redis had not seen: the EVALSHA before was refused
            if trips and isinstance(trips[-1], str) and trips[-1].startswith("EVALSHA"):
                trips.pop()
            return
        trips.append(trip)
        if state["fail"] is not None and state["fail"](trip):
            raise redis.exceptions.TimeoutError("Timeout reading from socket")

    real_command, real_execute = redis.Redis.execute_command, Pipeline.execute

    def command(self, *args, **options):
        note("SCRIPT LOAD" if str(args[0]).upper() == "SCRIPT" else name(args))
        return real_command(self, *args, **options)

    def execute(self, *args, **options):
        note([name(queued[0]) for queued in self.command_stack])
        return real_execute(self, *args, **options)

    monkeypatch.setattr(redis.Redis, "execute_command", command)
    monkeypatch.setattr(Pipeline, "execute", execute)
    if as_it_was:
        monkeypatch.setattr(data_writer_module, "SimpleTable", _OpenedAsItWas)
        monkeypatch.delattr(RedisCatalog, "store_schema_and_read_mirrors")

    def write(frame: pl.DataFrame, overwrite_columns=(), **options):
        """One write, and the round trips it made."""
        trips.clear()
        state["recording"] = True
        try:
            result = writer.write(role_name=ADMIN, simple_name=TABLE,
                                  overwrite_columns=list(overwrite_columns), data=frame, **options)
        finally:
            state["recording"] = False
        return result, list(trips)

    def read() -> list:
        frame, status, message = DataReader(
            super_name=sup, organization=ORG,
            query=f"SELECT id, region, amount FROM {TABLE} ORDER BY id, amount",
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return [(row["id"], row["region"], row["amount"]) for row in frame.to_dicts()]

    def mirrored() -> list:
        folder = os.path.join(ORG, sup, "parquet", TABLE, "files")
        try:
            return sorted(os.path.basename(p) for p in super_table.storage.list_files(folder, "*"))
        except Exception:
            return []

    client = RedisCatalog().r

    def in_redis() -> dict:
        leaf = json.loads(client.get(RK.meta_leaf(ORG, sup, TABLE)) or "null")
        names = client.smembers(RK.meta_table_names(ORG, sup))
        return {
            "schema": json.loads(client.get(RK.schema(ORG, sup, TABLE)) or "null"),
            "names": sorted(n.decode() if isinstance(n, bytes) else n for n in names),
            "entry_version": leaf and leaf["version"],
            "files": leaf and len(leaf["payload"]["resources"]),
            "root_version": json.loads(client.get(RK.meta_root(ORG, sup)))["version"],
        }

    return SimpleNamespace(sup=sup, super_table=super_table, write=write, read=read,
                           mirrored=mirrored, in_redis=in_redis, state=state, client=client,
                           trips=trips)


@pytest.fixture
def lake(hermetic_fakeredis, monkeypatch):
    yield _lake(monkeypatch)
    engine_duckdb.reset_shared_duckdb_state()


def _expected(ids, amounts=None) -> list:
    amounts = amounts or {}
    return [(i, "eu" if i % 2 else "us", amounts.get(i, i * 10)) for i in sorted(ids)]


# ---------------------------------------------------------------------------
# The sequence, pinned
# ---------------------------------------------------------------------------

def test_an_append_goes_to_redis_these_fourteen_times(lake):
    lake.write(_rows(0))

    result, trips = lake.write(_rows(3))

    assert trips == APPEND
    assert result[2] == 3, "three rows inserted"
    assert lake.read() == _expected(range(6))


def test_an_upsert_makes_the_same_trips_and_a_delete_reserves_no_ids(lake):
    lake.write(_rows(0))
    lake.write(_rows(3))

    _result, upsert = lake.write(pl.DataFrame({"id": [1, 4], "region": ["eu", "us"],
                                               "amount": [-1, -4]}), overwrite_columns=["id"])
    _result, delete = lake.write(pl.DataFrame({"id": [0]}), overwrite_columns=["id"], delete_only=True)

    assert upsert == APPEND
    assert delete == [trip for trip in APPEND if trip != "INCRBY meta:rowid_seq:doc:orders"]
    assert lake.read() == _expected([1, 2, 3, 4, 5], {1: -1, 4: -4})


def test_the_entry_is_read_once_in_a_write_and_never_asked_for(lake):
    lake.write(_rows(0))

    for frame, options in ((_rows(3), {}),
                           (pl.DataFrame({"id": [1], "region": ["eu"], "amount": [7]}),
                            {"overwrite_columns": ["id"]}),
                           (pl.DataFrame({"id": [2]}), {"overwrite_columns": ["id"], "delete_only": True})):
        _result, trips = lake.write(frame, **options)
        assert trips.count("GET meta:leaf:doc:orders") == 1
        assert "EXISTS meta:leaf:doc:orders" not in trips
        assert trips.count("GET meta:root") == 2, "the read-only guard, and the open"


def test_a_first_write_asks_once_and_then_creates_the_table_as_before(lake):
    _result, trips = lake.write(_rows(0))

    start = trips.index("SET lock:leaf:doc:orders")
    assert trips[start + 1:start + 9] == [
        "GET meta:root", "GET meta:leaf:doc:orders",        # the open: there is no entry
        "GET meta:root", "EXISTS meta:leaf:doc:orders",     # the creating open confirms it
        "EVALSHA meta:leaf:doc:orders",                     # the empty table's entry
        "GET meta:root", "GET meta:leaf:doc:orders",        # its snapshot
        "GET lock:leaf:doc:orders",
    ]
    assert lake.read() == _expected(range(3))
    assert lake.in_redis()["entry_version"] == 1


# ---------------------------------------------------------------------------
# The way it was, and the same outcome
# ---------------------------------------------------------------------------

def _a_day_of_writes(lake) -> list:
    """Every kind of write, mirrors switched on and off in between."""
    seen = []

    def step(label, *args, **options):
        result, trips = lake.write(*args, **options)
        seen.append({"step": label, "result": tuple(result), "trips": len(trips),
                     "rows": lake.read(), "redis": lake.in_redis(), "mirrored": len(lake.mirrored())})

    step("the first write", _rows(0))
    step("an append", _rows(3))
    MirrorFormats.set_with_lock(lake.super_table, ["PARQUET"])
    step("an append with a mirror on", _rows(6))
    step("an upsert", pl.DataFrame({"id": [1, 4], "region": ["eu", "us"], "amount": [-1, -4]}),
         overwrite_columns=["id"])
    step("a delete", pl.DataFrame({"id": [0]}), overwrite_columns=["id"], delete_only=True)
    MirrorFormats.set_with_lock(lake.super_table, [])
    step("an append with the mirror off again", _rows(9))
    step("a write that widens the schema",
         _rows(12).with_columns(pl.lit("x").alias("note")))
    step("a delete of every row", pl.DataFrame({"id": [0]}), overwrite_columns=[], delete_only=True)
    step("a write into the emptied table", _rows(20))
    return seen


def test_the_same_writes_both_ways_leave_the_same_everything(hermetic_fakeredis, monkeypatch):
    with monkeypatch.context() as patched:
        was = _a_day_of_writes(_lake(patched, as_it_was=True))
    with monkeypatch.context() as patched:
        now = _a_day_of_writes(_lake(patched))
    engine_duckdb.reset_shared_duckdb_state()

    assert [s["step"] for s in now] == [s["step"] for s in was]
    for before, after in zip(was, now):
        label = after["step"]
        assert after["result"] == before["result"], label
        assert after["rows"] == before["rows"], label
        assert after["redis"] == before["redis"], label
        assert after["mirrored"] == before["mirrored"], label
        assert after["trips"] < before["trips"], label
    # the comparison compared something: a mirror was written and then left alone
    by_step = {s["step"]: s for s in now}
    assert by_step["an append"]["mirrored"] == 0
    assert by_step["an append with a mirror on"]["mirrored"] == 3
    assert by_step["an append with the mirror off again"]["mirrored"] == by_step["a delete"]["mirrored"]
    assert by_step["a write that widens the schema"]["redis"]["schema"].get("note") == "String"
    assert now[-1]["rows"] == _expected([20, 21, 22])
    # four round trips fewer on a table that exists, two on the write that creates it
    assert [b["trips"] - a["trips"] for b, a in zip(was, now)] == [2] + [4] * (len(now) - 1)


def test_the_way_it_was_is_the_sequence_this_change_started_from(hermetic_fakeredis, monkeypatch):
    lake = _lake(monkeypatch, as_it_was=True)
    lake.write(_rows(0))

    _result, trips = lake.write(_rows(3))
    engine_duckdb.reset_shared_duckdb_state()

    assert trips == APPEND_AS_IT_WAS
    assert len(APPEND_AS_IT_WAS) - len(APPEND) == 4


# ---------------------------------------------------------------------------
# The mirror formats come with the bookkeeping
# ---------------------------------------------------------------------------

def test_the_formats_read_after_the_commit_are_the_ones_mirrored_to(lake):
    lake.write(_rows(0))
    MirrorFormats.set_with_lock(lake.super_table, ["PARQUET"])

    _result, trips = lake.write(_rows(3))

    assert trips.count("GET meta:mirrors") == 0, "no read of its own: it came with the two writes"
    assert trips[-2] == ["SET schema:doc:orders", "SADD meta:table_names", "GET meta:mirrors"]
    assert len(lake.mirrored()) == 2, "both files of the table are in the mirror"

    MirrorFormats.set_with_lock(lake.super_table, [])
    lake.write(_rows(6))
    assert len(lake.mirrored()) == 2, "switched off: the mirror is left as it was"


def test_a_bookkeeping_trip_that_fails_does_not_fail_the_write_or_its_mirror(lake):
    """The write logs it and goes on, as it did when the SET failed; mirroring
    then reads the formats itself, as it always did."""
    lake.write(_rows(0))
    MirrorFormats.set_with_lock(lake.super_table, ["PARQUET"])
    lake.state["fail"] = lambda trip: isinstance(trip, list) and trip[0].startswith("SET schema:doc:")

    result, trips = lake.write(_rows(3))

    lake.state["fail"] = None
    assert result[2] == 3
    assert trips[-3:] == [["SET schema:doc:orders", "SADD meta:table_names", "GET meta:mirrors"],
                          "GET meta:mirrors",
                          "EVALSHA lock:leaf:doc:orders"]
    assert len(lake.mirrored()) == 2
    assert lake.read() == _expected(range(6))


# ---------------------------------------------------------------------------
# A read that fails is not a table that is missing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("failing", ["GET meta:leaf:doc:orders", "GET meta:root"])
def test_an_entry_that_cannot_be_read_fails_the_write_and_creates_nothing(lake, failing):
    """AUDIT_BUGS C3, for the open as it is now: an error on the one read must
    not be taken for a table that does not exist."""
    lake.write(_rows(0))
    lake.write(_rows(3))
    before = lake.in_redis()
    after_the_lock = {"seen": False, "failed": 0}

    def fail(trip):
        """Once: Redis answers again straight after."""
        if trip == "SET lock:leaf:doc:orders":
            after_the_lock["seen"] = True
        if after_the_lock["seen"] and trip == failing and not after_the_lock["failed"]:
            after_the_lock["failed"] += 1
            return True
        return False

    lake.state["fail"] = fail
    with pytest.raises(redis.RedisError):
        lake.write(_rows(6))
    lake.state["fail"] = None

    assert after_the_lock["failed"] == 1
    # the failed read is the last thing the write asked before it let go of the lock
    locked = lake.trips.index("SET lock:leaf:doc:orders")
    assert lake.trips[locked + 1:] == (
        ["GET meta:root", "GET meta:leaf:doc:orders"][:1 + (failing == "GET meta:leaf:doc:orders")]
        + ["EVALSHA lock:leaf:doc:orders"])
    assert lake.in_redis() == before, "no entry was written, the root was not bumped"
    assert lake.read() == _expected(range(6))
    result, trips = lake.write(_rows(6))
    assert result[2] == 3 and trips == APPEND, "the lock was released; the next write is an ordinary one"
    assert lake.read() == _expected(range(9))
