"""
The round trips a write makes to Redis, and that fewer of them change nothing.

A write to an existing table went to Redis eighteen times here (nineteen with
monitoring on, which this harness has off).  It is eight now:

* the access check read the lake's root for the read-only guard, asked twice
  whether the role store is initialised, and then read the role's id and its
  document: five round trips, now one script that reads the five
  (``RedisCatalog.rbac_access_read``, by way of
  ``RoleManager.read_for_access_check``);
* the table was opened with an existence check and its snapshot then read, and
  each of the two first read the lake's root document: four round trips for
  one catalog entry, now two (``SimpleTable(..., keep_leaf=True)``);
* the commit was three: the lock key read and compared, the leaf set, the root
  bumped.  One script does the three
  (``RedisCatalog.commit_leaf_and_bump_root``), and with it the moment between
  the lock check and the commit, in which another writer could take the lock
  and be overwritten, is gone;
* after the commit the schema document was stored, the table's name added to
  the lake's table names and the lake's mirror formats read, one command a
  round trip: now one for the three
  (``RedisCatalog.store_schema_and_read_mirrors``).

The script of the access check is sent as a read (``EVAL_RO``), so that Redis
answers it wherever it answers the five reads, and that command is Redis 7's.
The fake Redis of this harness does not know it, as a Redis before 7 does not;
it is made to run it here, as Redis 7 does, and one test leaves it as it is:
such a Redis is asked the five reads, and a write makes the twelve round trips
it made before this change.

The real writer and reader run here over the hermetic fake Redis on LOCAL
storage, and every round trip is recorded.  The sequence of an append is
pinned, so that a round trip added later shows up as a failure here.  The way
it was is still reachable (a table opened without ``keep_leaf``, a catalog
without the new methods, a role manager without its one read), and the same
writes are run both ways: they must
leave the same rows, the same catalog entry and root versions, the same schema
document, the same table names and the same mirror.

The four changed pieces are compared with the code as it was, state by state,
in ``supertable/tests``:
``test_an_access_check_reads_redis_once.py``,
``test_a_table_opened_for_a_write_reads_its_entry_once.py``,
``test_a_commit_checks_the_lock_and_bumps_the_root_in_one_script.py`` and
``test_what_follows_a_commit_goes_to_redis_in_one_round_trip.py``.
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
from supertable.errors import LockLostError
from supertable.mirroring.mirror_formats import MirrorFormats
from supertable.rbac.role_manager import RoleManager
from supertable.redis_catalog import RedisCatalog
from supertable.simple_table import SimpleTable
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
TABLE = "orders"

# The commit: is the lock still ours, the entry, the root, in one script.
COMMIT = "EVALSHA meta:leaf:doc:orders meta:root lock:leaf:doc:orders"
# The commit in three steps, as it was, and as it still is when the one script
# is not to be had.
THREE_STEPS = ["GET lock:leaf:doc:orders", "EVALSHA meta:leaf:doc:orders", "EVALSHA meta:root"]
AFTER_THE_COMMIT = ["SET schema:doc:orders", "SADD meta:table_names", "GET meta:mirrors"]

# The access check: is the role store initialised, the role's id and its
# document, and is the lake read-only, in one script sent as a read.
ACCESS = "EVAL_RO rbac:roles:meta rbac:roles:type:doc:superadmin rbac:roles:name_to_id meta:root"
# The same five reads one by one, as they were, and as they still are when the
# one script is not to be had.
FIVE_READS = [
    "GET meta:root",                                    # is the lake read-only
    "EXISTS rbac:roles:meta",                           # the role
    "SMEMBERS rbac:roles:type:doc:superadmin",
    "HGET rbac:roles:name_to_id",
    "HGETALL rbac:roles:doc:<id>",
]

# An append to a table that exists, in order.
APPEND = [
    ACCESS,                                             # may this role write here
    "INCRBY meta:rowid_seq:doc:orders",                 # row ids
    "SET lock:leaf:doc:orders",                         # the table lock
    "GET meta:root",                                    # the table's entry, once
    "GET meta:leaf:doc:orders",
    COMMIT,
    AFTER_THE_COMMIT,                                   # one round trip
    "EVALSHA lock:leaf:doc:orders",                     # unlock
]
# The same append before: five reads for the access check, the entry asked for
# and then read, the commit in three steps, and three round trips after it.
APPEND_AS_IT_WAS = FIVE_READS + APPEND[1:3] + [
    "GET meta:root", "EXISTS meta:leaf:doc:orders",
    "GET meta:root", "GET meta:leaf:doc:orders",
] + THREE_STEPS + AFTER_THE_COMMIT + ["EVALSHA lock:leaf:doc:orders"]


def _rows(first: int, count: int = 3) -> pl.DataFrame:
    ids = list(range(first, first + count))
    return pl.DataFrame({"id": ids, "region": ["eu" if i % 2 else "us" for i in ids],
                         "amount": [i * 10 for i in ids]})


def _text(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _any_of_the_three_steps(trips) -> bool:
    return any(step in trips for step in THREE_STEPS)


class _OpenedAsItWas(SimpleTable):
    """A table opened the way every caller but ``DataWriter.write`` opens it."""

    def __init__(self, super_table, simple_name, *, create_if_missing=True, keep_leaf=False):
        super().__init__(super_table, simple_name, create_if_missing=create_if_missing)


def _as_redis_7(monkeypatch) -> None:
    """Make the fake Redis run a script sent as a read (``EVAL_RO``), as
    Redis 7 does: the script is run."""
    real = redis.Redis.execute_command

    def execute_command(self, *args, **options):
        if str(args[0]).upper() == "EVAL_RO":
            args = ("EVAL",) + tuple(args[1:])
        return real(self, *args, **options)

    monkeypatch.setattr(redis.Redis, "execute_command", execute_command)


def _lake(monkeypatch, as_it_was: bool = False, redis_7: bool = True):
    engine_duckdb.reset_shared_duckdb_state()
    if redis_7:
        _as_redis_7(monkeypatch)
    # which of the two this Redis is, is asked once for each client: here,
    # before any round trip is recorded
    RedisCatalog()._runs_read_only_scripts()
    sup = f"trips_{uuid.uuid4().hex[:12]}"
    super_table = SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)
    trips: list = []
    # recording: between the start and the end of one operation.  quiet: this
    # test is reading or changing Redis itself.  before: called with every
    # round trip before it is sent.  fail: a round trip to time out instead.
    state = {"recording": False, "quiet": False, "before": None, "fail": None}
    lake_prefix = RK.meta_root(ORG, sup)[:-len("meta:root")]
    role_doc = re.compile(r"(rbac:roles:doc:)[0-9a-f]{32}$")

    def short(key) -> str:
        key = str(key)
        key = key[len(lake_prefix):] if key.startswith(lake_prefix) else key
        return role_doc.sub(r"\1<id>", key)

    def name(args) -> str:
        command = str(args[0]).upper()
        if command in ("EVALSHA", "EVAL", "EVAL_RO"):
            return command + " " + " ".join(short(key) for key in args[3:3 + int(args[2])])
        return command + (" " + short(args[1]) if len(args) > 1 else "")

    def note(trip) -> None:
        if not state["recording"] or state["quiet"]:
            return
        if isinstance(trip, str) and trip.startswith("SCRIPT"):
            # a script this Redis had not seen: the EVALSHA before was refused
            # and is sent again once the script is loaded
            if trips and isinstance(trips[-1], str) and trips[-1].startswith("EVALSHA"):
                trips.pop()
            return
        if state["before"] is not None:
            state["quiet"] = True
            try:
                state["before"](trip)
            finally:
                state["quiet"] = False
        trips.append(trip)
        if state["fail"] is not None and state["fail"](trip):
            raise redis.exceptions.TimeoutError("Timeout reading from socket")

    real_command, real_execute = redis.Redis.execute_command, Pipeline.execute

    def command(self, *args, **options):
        note(name(args))
        return real_command(self, *args, **options)

    def execute(self, *args, **options):
        note([name(queued[0]) for queued in self.command_stack])
        return real_execute(self, *args, **options)

    monkeypatch.setattr(redis.Redis, "execute_command", command)
    monkeypatch.setattr(Pipeline, "execute", execute)
    if as_it_was:
        monkeypatch.setattr(data_writer_module, "SimpleTable", _OpenedAsItWas)
        monkeypatch.delattr(RedisCatalog, "commit_leaf_and_bump_root")
        monkeypatch.delattr(RedisCatalog, "store_schema_and_read_mirrors")
        monkeypatch.delattr(RoleManager, "read_for_access_check")

    def recorded(operation):
        """Run one operation of the writer; what it returned and its round trips."""
        trips.clear()
        state["recording"] = True
        try:
            result = operation()
        finally:
            state["recording"] = False
        return result, list(trips)

    def write(frame: pl.DataFrame, overwrite_columns=(), role=ADMIN, **options):
        return recorded(lambda: writer.write(
            role_name=role, simple_name=TABLE, overwrite_columns=list(overwrite_columns),
            data=frame, **options))

    def compact():
        return recorded(lambda: writer.compact(role_name=ADMIN, simple_name=TABLE))

    def read() -> list:
        frame, status, message = DataReader(
            super_name=sup, organization=ORG,
            query=f"SELECT id, region, amount FROM {TABLE} ORDER BY id, amount",
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return [(row["id"], row["region"], row["amount"]) for row in frame.to_dicts()]

    def query(sql: str, role=ADMIN):
        """One query of the real reader, recorded: what came back, as plain
        values, and its round trips.  A refusal is what came back, too."""
        def run():
            try:
                frame, status, message = DataReader(
                    super_name=sup, organization=ORG, query=sql,
                ).execute(role_name=role, engine=Engine.DUCKDB)
            except PermissionError as refusal:
                return ("refused", str(refusal))
            if status is not Status.OK:
                return ("error", message)
            return ("rows", [tuple(row.values()) for row in frame.to_dicts()], list(frame.columns))
        return recorded(run)

    def mirrored() -> list:
        folder = os.path.join(ORG, sup, "parquet", TABLE, "files")
        try:
            return sorted(os.path.basename(p) for p in super_table.storage.list_files(folder, "*"))
        except Exception:
            return []

    client = RedisCatalog().r
    lock_key = RK.lock_leaf(ORG, sup, TABLE)

    def leaf() -> dict:
        return json.loads(client.get(RK.meta_leaf(ORG, sup, TABLE)) or "null")

    def in_redis() -> dict:
        entry = leaf()
        payload = (entry or {}).get("payload")
        names = client.smembers(RK.meta_table_names(ORG, sup))
        return {
            "schema": json.loads(client.get(RK.schema(ORG, sup, TABLE)) or "null"),
            "names": sorted(_text(n) for n in names),
            "entry_version": entry and entry["version"],
            "files": payload and len(payload["resources"]),
            "root_version": json.loads(client.get(RK.meta_root(ORG, sup)))["version"],
        }

    def take_the_lock_away_before(trip_named: str):
        """Another writer takes the table lock just before that round trip."""
        def before(trip):
            if trip == trip_named:
                client.set(lock_key, "another-writer")
        state["before"] = before

    return SimpleNamespace(sup=sup, super_table=super_table, writer=writer, write=write, query=query,
                           compact=compact, read=read, mirrored=mirrored, in_redis=in_redis,
                           leaf=leaf, state=state, client=client, trips=trips, lock_key=lock_key,
                           take_the_lock_away_before=take_the_lock_away_before)


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

def test_an_append_goes_to_redis_these_eight_times(lake):
    lake.write(_rows(0))

    result, trips = lake.write(_rows(3))

    assert trips == APPEND
    assert len(trips) == 8
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
        assert trips.count("GET meta:root") == 1, "the open; the guard's read is in the access check"
        assert trips.count(ACCESS) == 1 and not any(read in trips for read in FIVE_READS[1:])
        assert trips.count(COMMIT) == 1 and not _any_of_the_three_steps(trips)


def test_a_first_write_asks_once_and_then_creates_the_table_as_before(lake):
    _result, trips = lake.write(_rows(0))

    start = trips.index("SET lock:leaf:doc:orders")
    assert trips[start + 1:start + 9] == [
        "GET meta:root", "GET meta:leaf:doc:orders",        # the open: there is no entry
        "GET meta:root", "EXISTS meta:leaf:doc:orders",     # the creating open confirms it
        "EVALSHA meta:leaf:doc:orders",                     # the empty table's entry
        "GET meta:root", "GET meta:leaf:doc:orders",        # its snapshot
        COMMIT,
    ]
    assert lake.read() == _expected(range(3))
    assert lake.in_redis()["entry_version"] == 1


def test_a_write_still_reports_both_timings_of_its_commit(lake, monkeypatch):
    """The monitoring record of a write names the leaf commit and the root
    bump.  The one script does both, and both timings are still reported: the
    first holds the round trip, the second what is left of it, nothing."""
    profilers = []

    class _Kept(data_writer_module.Profiler):
        def __init__(self):
            super().__init__()
            profilers.append(self)

    monkeypatch.setattr(data_writer_module, "Profiler", _Kept)
    lake.write(_rows(0))
    before = len(profilers)

    lake.write(_rows(3))

    # the write's own profiler: the one its commit was timed with
    committing = [p for p in profilers[before:] if "redis.set_leaf" in p.timings]
    assert len(committing) == 1
    timings, counts = committing[0].timings, committing[0].counts
    assert "redis.bump_root" in timings
    assert (counts["redis.set_leaf.n"], counts["redis.bump_root.n"]) == (1, 1)
    assert timings["redis.bump_root"] < timings["redis.set_leaf"]
    assert "write_parquet" in timings, "it is the profiler the whole write reports from"


def test_a_compaction_publishes_in_one_script_too(lake):
    for first in (0, 3, 6, 9):
        lake.write(_rows(first))
    before = lake.in_redis()

    _result, trips = lake.compact()

    assert trips.count(COMMIT) == 1 and not _any_of_the_three_steps(trips)
    after = lake.in_redis()
    assert (after["files"], before["files"]) == (1, 4)
    assert after["entry_version"] == before["entry_version"] + 1
    assert after["root_version"] == before["root_version"] + 1
    assert lake.read() == _expected(range(12))


# ---------------------------------------------------------------------------
# The way it was, and the same outcome
# ---------------------------------------------------------------------------

def _a_day_of_writes(lake) -> list:
    """Every kind of write and a compaction, mirrors switched on and off."""
    seen = []

    def step(label, operation, *args, **options):
        result, trips = operation(*args, **options)
        if isinstance(result, dict):
            # a compaction: its counts, without the run's own id, lake name and clock
            result = {k: v for k, v in result.items() if type(v) is int}
        else:
            result = tuple(result)
        seen.append({"step": label, "result": result, "trips": len(trips), "rows": lake.read(),
                     "redis": lake.in_redis(), "mirrored": len(lake.mirrored())})

    step("the first write", lake.write, _rows(0))
    step("an append", lake.write, _rows(3))
    MirrorFormats.set_with_lock(lake.super_table, ["PARQUET"])
    step("an append with a mirror on", lake.write, _rows(6))
    step("an upsert", lake.write, pl.DataFrame({"id": [1, 4], "region": ["eu", "us"],
                                                "amount": [-1, -4]}), overwrite_columns=["id"])
    step("a delete", lake.write, pl.DataFrame({"id": [0]}), overwrite_columns=["id"], delete_only=True)
    MirrorFormats.set_with_lock(lake.super_table, [])
    step("an append with the mirror off again", lake.write, _rows(9))
    step("a write that widens the schema", lake.write,
         _rows(12).with_columns(pl.lit("x").alias("note")))
    step("a compaction", lake.compact)
    step("a delete of every row", lake.write, pl.DataFrame({"id": [0]}), overwrite_columns=[],
         delete_only=True)
    step("a write into the emptied table", lake.write, _rows(20))
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
    assert by_step["a compaction"]["redis"]["files"] == 1
    assert now[-1]["rows"] == _expected([20, 21, 22])
    # ten round trips fewer on a table that exists, eight on the write that
    # creates it, six on a compaction (which opens its table as it did)
    fewer = {after["step"]: before["trips"] - after["trips"] for before, after in zip(was, now)}
    assert fewer.pop("the first write") == 8
    assert fewer.pop("a compaction") == 6
    assert set(fewer.values()) == {10}


def test_the_way_it_was_is_the_sequence_these_changes_started_from(hermetic_fakeredis, monkeypatch):
    lake = _lake(monkeypatch, as_it_was=True)
    lake.write(_rows(0))

    _result, trips = lake.write(_rows(3))
    engine_duckdb.reset_shared_duckdb_state()

    assert trips == APPEND_AS_IT_WAS
    assert (len(APPEND_AS_IT_WAS), len(APPEND)) == (18, 8)


# ---------------------------------------------------------------------------
# The lock, checked by the script that commits
# ---------------------------------------------------------------------------

def test_a_lock_taken_at_the_last_moment_refuses_the_commit(lake):
    """Another writer holds the lock by the time the commit arrives: nothing
    of this write is published, and the other writer's lock is left alone."""
    lake.write(_rows(0))
    lake.write(_rows(3))
    before = lake.in_redis()
    lake.take_the_lock_away_before(COMMIT)

    with pytest.raises(LockLostError):
        lake.write(_rows(6))
    lake.state["before"] = None

    assert lake.trips[-2:] == [COMMIT, "EVALSHA lock:leaf:doc:orders"]
    assert lake.in_redis() == before, "the entry and the root are as they were"
    assert lake.read() == _expected(range(6))
    assert _text(lake.client.get(lake.lock_key)) == "another-writer"

    lake.client.delete(lake.lock_key)
    result, trips = lake.write(_rows(6))
    assert result[2] == 3 and trips == APPEND
    assert lake.read() == _expected(range(9))


def test_the_moment_between_the_check_and_the_commit_is_gone(hermetic_fakeredis, monkeypatch):
    """In three steps the lock was read, and then the leaf was set: a writer
    that took the lock between the two was overwritten.  That is shown here
    with the three steps, and then the same theft, at the latest moment there
    now is, is refused."""
    with monkeypatch.context() as patched:
        old = _lake(patched, as_it_was=True)
        old.write(_rows(0))
        before = old.in_redis()
        old.take_the_lock_away_before("EVALSHA meta:leaf:doc:orders")   # after the check

        result, trips = old.write(_rows(3))

        old.state["before"] = None
        assert result[2] == 3, "published, though the lock was another writer's by then"
        assert trips[trips.index("GET lock:leaf:doc:orders"):][:3] == THREE_STEPS
        assert old.in_redis()["entry_version"] == before["entry_version"] + 1
        assert _text(old.client.get(old.lock_key)) == "another-writer"

    with monkeypatch.context() as patched:
        new = _lake(patched)
        new.write(_rows(0))
        before = new.in_redis()
        new.take_the_lock_away_before(COMMIT)

        with pytest.raises(LockLostError):
            new.write(_rows(3))

        new.state["before"] = None
        assert new.in_redis() == before
        assert new.read() == _expected(range(3))
    engine_duckdb.reset_shared_duckdb_state()


def test_a_lock_the_heartbeat_reported_lost_refuses_the_commit_without_asking_redis(lake):
    lake.write(_rows(0))
    before = lake.in_redis()

    def lose(trip):
        if trip == "GET meta:leaf:doc:orders":          # the lock is held by now
            locker = lake.writer.catalog._locker
            with locker._held_lock:
                locker._lost[(lake.lock_key, _text(lake.client.get(lake.lock_key)))] = None

    lake.state["before"] = lose
    with pytest.raises(LockLostError):
        lake.write(_rows(3))
    lake.state["before"] = None

    assert COMMIT not in lake.trips and not _any_of_the_three_steps(lake.trips)
    assert lake.in_redis() == before
    assert lake.read() == _expected(range(3))


def test_a_compaction_whose_lock_was_taken_publishes_nothing(lake):
    for first in (0, 3, 6):
        lake.write(_rows(first))
    before = lake.in_redis()
    lake.take_the_lock_away_before(COMMIT)

    with pytest.raises(LockLostError):
        lake.compact()
    lake.state["before"] = None

    assert lake.in_redis() == before
    assert lake.read() == _expected(range(9))


# ---------------------------------------------------------------------------
# When the one script is not to be had, the three steps
# ---------------------------------------------------------------------------

def test_a_commit_script_that_fails_is_followed_by_the_three_steps(lake):
    lake.write(_rows(0))
    before = lake.in_redis()
    failed = {"times": 0}

    def fail(trip):
        if trip == COMMIT and not failed["times"]:
            failed["times"] += 1
            return True
        return False

    lake.state["fail"] = fail
    result, trips = lake.write(_rows(3))
    lake.state["fail"] = None

    assert result[2] == 3
    at = trips.index(COMMIT)
    assert trips[at:at + 4] == [COMMIT] + THREE_STEPS
    after = lake.in_redis()
    assert after["entry_version"] == before["entry_version"] + 1
    assert after["root_version"] == before["root_version"] + 1
    assert lake.read() == _expected(range(6))


def test_a_snapshot_the_entry_cannot_hold_is_published_in_three_steps_as_before(lake):
    """A number that is not finite in the lineage: refused before the script
    is sent, and stored as an entry without payload, which readers follow to
    the snapshot file."""
    lake.write(_rows(0))
    before = lake.in_redis()

    result, trips = lake.write(_rows(3), lineage={"source_type": "manual", "ratio": float("nan")})

    assert result[2] == 3
    assert COMMIT not in trips
    at = trips.index("GET lock:leaf:doc:orders")
    assert trips[at:at + 3] == THREE_STEPS
    entry = lake.leaf()
    assert "payload" not in entry and entry["version"] == before["entry_version"] + 1
    assert lake.read() == _expected(range(6))

    result, trips = lake.write(_rows(6))
    assert trips == APPEND and lake.read() == _expected(range(9))


def test_a_lost_lock_is_still_found_by_the_three_steps(hermetic_fakeredis, monkeypatch):
    """The fallback is the whole of what the writer did, its lock check
    included: a catalog without the one script still refuses a lost lock."""
    lake = _lake(monkeypatch, as_it_was=True)
    lake.write(_rows(0))
    before = lake.in_redis()
    lake.take_the_lock_away_before("GET lock:leaf:doc:orders")

    with pytest.raises(LockLostError):
        lake.write(_rows(3))
    lake.state["before"] = None
    engine_duckdb.reset_shared_duckdb_state()

    assert lake.in_redis() == before
    assert "EVALSHA meta:leaf:doc:orders" not in lake.trips and "EVALSHA meta:root" not in lake.trips


# ---------------------------------------------------------------------------
# The mirror formats come with the bookkeeping
# ---------------------------------------------------------------------------

def test_the_formats_read_after_the_commit_are_the_ones_mirrored_to(lake):
    lake.write(_rows(0))
    MirrorFormats.set_with_lock(lake.super_table, ["PARQUET"])

    _result, trips = lake.write(_rows(3))

    assert trips.count("GET meta:mirrors") == 0, "no read of its own: it came with the two writes"
    assert trips[-2] == AFTER_THE_COMMIT
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
    assert trips[-3:] == [AFTER_THE_COMMIT, "GET meta:mirrors", "EVALSHA lock:leaf:doc:orders"]
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


# ---------------------------------------------------------------------------
# The access check, on one read
# ---------------------------------------------------------------------------

def _refused(lake, frame, **options):
    """A write that must be refused: the refusal, and the round trips made."""
    with pytest.raises(PermissionError) as raised:
        lake.write(frame, **options)
    return str(raised.value), list(lake.trips)


def _roles(lake) -> RoleManager:
    return RoleManager(super_name=lake.sup, organization=ORG, actor_role_name=ADMIN)


def _both(monkeypatch, scenario):
    """Run *scenario* on a lake the way it was and on one the way it is."""
    with monkeypatch.context() as patched:
        was = scenario(_lake(patched, as_it_was=True))
    with monkeypatch.context() as patched:
        now = scenario(_lake(patched))
    engine_duckdb.reset_shared_duckdb_state()
    return was, now


def test_a_locked_lake_refuses_the_write_as_it_did_and_nothing_is_written(hermetic_fakeredis, monkeypatch):
    def scenario(lake):
        lake.write(_rows(0))
        before = (lake.in_redis(), lake.read())
        RedisCatalog().update_root_flags(ORG, lake.sup, {"read_only": True})
        refusal, trips = _refused(lake, _rows(3))
        assert (lake.in_redis(), lake.read()) == before
        RedisCatalog().update_root_flags(ORG, lake.sup, {"read_only": False})
        result, _trips = lake.write(_rows(3))
        return refusal, trips, tuple(result), lake.read()

    was, now = _both(monkeypatch, scenario)

    assert now[0] == was[0] == "This SuperTable is locked. Cannot write to this table."
    assert was[1] == FIVE_READS[:1], "the guard's read, and the role was never looked at"
    assert now[1] == [ACCESS]
    assert now[2:] == was[2:] and now[3] == _expected(range(6))


def test_a_role_that_may_not_write_is_refused_as_it_was(hermetic_fakeredis, monkeypatch):
    everything = {"*": {"columns": ["*"], "filters": ["*"]}}

    def scenario(lake):
        lake.write(_rows(0))
        roles = _roles(lake)
        roles.create_role({"role": "reader", "role_name": "analyst", "tables": everything})
        roles.create_role({"role": "writer", "role_name": "items_writer",
                           "tables": {"items": {"columns": ["*"], "filters": ["*"]}}})
        roles.create_role({"role": "writer", "role_name": "Loader", "tables": everything})
        before = (lake.in_redis(), lake.read())
        refusals = [_refused(lake, _rows(3), role=role) for role in ("analyst", "items_writer", "nobody")]
        assert (lake.in_redis(), lake.read()) == before
        result, trips = lake.write(_rows(3), role="LOADER")
        return refusals, tuple(result), trips, lake.read()

    was, now = _both(monkeypatch, scenario)

    assert [message for message, _trips in now[0]] == [message for message, _trips in was[0]] == [
        "You don't have permission to write to this table.",
        "You don't have permission to write to this table table 'orders'.",
        "Invalid or nonexistent role: nobody",
    ]
    assert [trips for _message, trips in was[0]] == [FIVE_READS, FIVE_READS, FIVE_READS[:4]]
    assert [trips for _message, trips in now[0]] == [[ACCESS]] * 3
    assert now[1] == was[1] and now[3] == was[3] == _expected(range(6))
    assert (was[2], now[2]) == (APPEND_AS_IT_WAS, APPEND), "and a role that may, by any spelling, writes"


def test_a_role_changed_between_two_writes_decides_the_second(hermetic_fakeredis, monkeypatch):
    """Nothing of the check is remembered from one write to the next: the role
    is read again, by the one script, every time."""
    everything = {"*": {"columns": ["*"], "filters": ["*"]}}

    def scenario(lake):
        lake.write(_rows(0))
        roles = _roles(lake)
        role_id = roles.create_role({"role": "writer", "role_name": "loader", "tables": everything})
        seen = [tuple(lake.write(_rows(3), role="loader")[0])]
        roles.update_role(role_id, {"tables": {"items": {"columns": ["*"], "filters": ["*"]}}})
        seen.append(_refused(lake, _rows(6), role="loader")[0])
        roles.update_role(role_id, {"role": "reader", "tables": everything})
        seen.append(_refused(lake, _rows(6), role="loader")[0])
        roles.update_role(role_id, {"role": "writer"})
        seen.append(tuple(lake.write(_rows(6), role="loader")[0]))
        RedisCatalog().r.hset(RK.rbac_role_doc(ORG, lake.sup, role_id), "enabled", "false")
        seen.append(_refused(lake, _rows(9), role="loader")[0])
        roles.delete_role(role_id)
        seen.append(_refused(lake, _rows(9), role="loader")[0])
        return seen, lake.read()

    was, now = _both(monkeypatch, scenario)

    assert now == was
    assert now[0][1:3] == ["You don't have permission to write to this table table 'orders'.",
                           "You don't have permission to write to this table."]
    assert now[0][4:] == ["Role 'loader' is disabled.", "Invalid or nonexistent role: loader"]
    assert now[1] == _expected(range(9))


def test_an_access_read_that_fails_is_followed_by_the_five_reads(lake):
    lake.write(_rows(0))
    failed = {"times": 0}

    def fail(trip):
        if trip == ACCESS and not failed["times"]:
            failed["times"] += 1
            return True
        return False

    lake.state["fail"] = fail
    result, trips = lake.write(_rows(3))
    lake.state["fail"] = None

    assert result[2] == 3
    assert trips == [ACCESS] + FIVE_READS + APPEND[1:]
    assert lake.read() == _expected(range(6))

    result, trips = lake.write(_rows(6))
    assert trips == APPEND and lake.read() == _expected(range(9))


def test_an_access_read_that_fails_does_not_open_a_locked_lake(lake):
    lake.write(_rows(0))
    RedisCatalog().update_root_flags(ORG, lake.sup, {"read_only": True})
    lake.state["fail"] = lambda trip: trip == ACCESS

    refusal, trips = _refused(lake, _rows(3))
    lake.state["fail"] = None

    assert refusal == "This SuperTable is locked. Cannot write to this table."
    assert trips == [ACCESS, "GET meta:root"]
    assert lake.read() == _expected(range(3))


def test_a_role_store_taken_away_under_the_writer_is_made_again_as_before(hermetic_fakeredis, monkeypatch):
    """The check that the role store is there is made at every write, inside
    the one read: a store that is gone is given to the constructor, which
    makes the default role again, as it did."""
    def scenario(lake):
        lake.write(_rows(0))
        client = RedisCatalog().r
        gone = [key for key in client.keys("*") if ":rbac:roles:" in _text(key)]
        assert len(gone) >= 5
        client.delete(*gone)
        result, trips = lake.write(_rows(3))
        again = sorted(_text(key).split(":rbac:roles:")[1].split(":")[0]
                       for key in client.keys("*") if ":rbac:roles:" in _text(key))
        return tuple(result), trips, again, lake.read()

    was, now = _both(monkeypatch, scenario)

    assert now[0] == was[0] and now[3] == was[3] == _expected(range(6))
    assert now[2] == was[2] == ["doc", "index", "meta", "name_to_id", "type"], "one role, made again"
    assert now[1][0] == ACCESS, "the one read found no role store"
    made_again = now[1][1:now[1].index("INCRBY meta:rowid_seq:doc:orders")]
    assert made_again == was[1][:was[1].index("INCRBY meta:rowid_seq:doc:orders")]
    assert len(made_again) > 5, "the constructor had work to do"


def test_a_compaction_and_a_first_write_ask_for_access_once_too(lake):
    _result, first = lake.write(_rows(0))
    for start in (3, 6, 9):
        lake.write(_rows(start))

    _result, compaction = lake.compact()

    for trips in (first, compaction):
        assert trips.count(ACCESS) == 1
        assert not any(read in trips for read in FIVE_READS[1:])


def test_a_redis_that_does_not_run_a_script_sent_as_a_read_is_written_to_as_before(hermetic_fakeredis, monkeypatch):
    """A Redis before 7: the access check makes its five reads, everything
    else is as on any other Redis, and the rows are the same rows."""
    old = _lake(monkeypatch, redis_7=False)
    assert RedisCatalog()._runs_read_only_scripts() is False
    old.write(_rows(0))

    result, trips = old.write(_rows(3))
    refusal, refused = None, None
    RedisCatalog().update_root_flags(ORG, old.sup, {"read_only": True})
    refusal, refused = _refused(old, _rows(6))
    engine_duckdb.reset_shared_duckdb_state()

    assert trips == FIVE_READS + APPEND[1:]
    assert len(trips) == 12 and result[2] == 3
    assert (refusal, refused) == ("This SuperTable is locked. Cannot write to this table.", FIVE_READS[:1])
    assert old.read() == _expected(range(6))


def test_a_redis_is_asked_once_whether_it_runs_the_script(hermetic_fakeredis, monkeypatch):
    """Once for each client, before its first access check; the answer is
    kept, and no later write asks."""
    _as_redis_7(monkeypatch)
    asked = []
    real = redis.Redis.execute_command

    def command(self, *args, **options):
        if str(args[0]).upper() == "EVAL" and int(args[2]) == 0:       # EVAL_RO, as this fake runs it
            asked.append(args[1])
        return real(self, *args, **options)

    monkeypatch.setattr(redis.Redis, "execute_command", command)
    lake = _lake(monkeypatch)
    for first in (0, 3, 6):
        lake.write(_rows(first))
    lake.compact()
    engine_duckdb.reset_shared_duckdb_state()

    assert asked == ["return 1"], "once, before the first check; three writes and a compaction did not ask again"

