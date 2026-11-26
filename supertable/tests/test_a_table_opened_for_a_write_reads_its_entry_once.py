"""
A table opened for a write reads its catalog entry once.

``DataWriter.write`` opens its table and takes the table's snapshot in two
lines, under the table lock.  The first asked the catalog whether the entry
exists, the second read it, and each began by reading the lake's root document
to learn whether the lake is a replica: four round trips to Redis for one
entry.  With ``keep_leaf=True`` the entry is read once.  That read says whether
the table exists, and the next ``get_simple_table_snapshot()`` uses what it
returned, once.

The two methods as they were are kept below, word for word, in a subclass.
The writer's two lines are run through both for every state the entry and the
root document can be in, and must end the same: the snapshot and path
returned, or the error raised.  What is sent to Redis is recorded too.
"""
from __future__ import annotations

import copy
import json
import os
from types import SimpleNamespace

import fakeredis
import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_catalog  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402
from supertable.errors import TableNotFoundError  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402
from supertable.simple_table import SimpleTable  # noqa: E402
from supertable.storage.tests.fake_object_store import FakeObjectStore  # noqa: E402

ORG, LAKE, SOURCE, TABLE = "acme", "sales", "sales_source", "orders"
PATH = f"{ORG}/{LAKE}/tables/{TABLE}/snapshots/1795000000001_ab12_tables.json"
GONE = f"{ORG}/{LAKE}/tables/{TABLE}/snapshots/1795000000009_ffff_tables.json"

SNAPSHOT = {
    "simple_name": TABLE, "location": f"{ORG}/{LAKE}/tables/{TABLE}", "snapshot_version": 4,
    "last_updated_ms": 1_795_000_000_000, "previous_snapshot": None,
    "schema": {"id": "Int64", "name": "String"},
    "resources": [{"file": f"{ORG}/{LAKE}/tables/{TABLE}/data/a.parquet", "file_size": 10,
                   "rows": 2, "columns": 2}],
    "tombstone": None, "tombstone_rows": 0, "stats_file": None, "stats_rows": 0,
}
ON_STORAGE = dict(SNAPSHOT, snapshot_version=3)      # what the file at PATH holds


# ---------------------------------------------------------------------------
# The two methods as they were at 117bcf0
# ---------------------------------------------------------------------------

class _AsItWas(SimpleTable):

    def __init__(self, super_table, simple_name, *, create_if_missing=True):
        self.super_table = super_table
        self.identity = "tables"
        self.simple_name = simple_name

        # Storage is the same as SuperTable's
        self.storage = self.super_table.storage
        self.catalog = RedisCatalog()

        # Data layout
        self.simple_dir = os.path.join(
            super_table.organization, super_table.super_name, self.identity, self.simple_name
        )
        self.data_dir = os.path.join(self.simple_dir, "data")
        self.snapshot_dir = os.path.join(self.simple_dir, "snapshots")

        # Fast path: if meta:leaf exists, don't touch storage
        if self.catalog.leaf_exists(
                self.super_table.organization, self.super_table.super_name, self.simple_name
        ):
            return

        if not create_if_missing:
            raise TableNotFoundError(
                self.super_table.organization,
                self.super_table.super_name,
                simple_name,
            )

        self.init_simple_table()

    def get_simple_table_snapshot(self):
        ptr = self.catalog.get_leaf(self.super_table.organization, self.super_table.super_name, self.simple_name)
        if not ptr or not ptr.get("path"):
            raise FileNotFoundError("No path found in simple table leaf pointer.")
        path = ptr["path"]

        payload = ptr.get("payload") if isinstance(ptr, dict) else None
        if isinstance(payload, dict):
            # Common: payload is the snapshot dict itself.
            if isinstance(payload.get("resources"), list):
                return payload, path
            # Also support nested snapshot shapes.
            snap = payload.get("snapshot") if isinstance(payload.get("snapshot"), dict) else None
            if isinstance(snap, dict) and isinstance(snap.get("resources"), list):
                return snap, path

        data = self.storage.read_json(path)
        return data, path


# ---------------------------------------------------------------------------
# A lake on a fake Redis that records what it is sent
# ---------------------------------------------------------------------------

class _Lake:
    """One fake Redis, its catalog, and a super table over a fake object store."""

    def __init__(self, monkeypatch):
        self.client = fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)
        self.sent = []
        self.fail_on = None                     # (command, key) that times out
        real = self.client.execute_command

        def recording(*args, **options):
            entry = (str(args[0]).upper(), args[1] if len(args) > 1 else None)
            self.sent.append(entry)
            if entry == self.fail_on:
                raise redis.exceptions.TimeoutError("Timeout reading from socket")
            return real(*args, **options)

        self.client.execute_command = recording
        self._monkeypatch = monkeypatch
        self.storage = FakeObjectStore()
        self.storage.write_json(PATH, ON_STORAGE)
        self.super_table = SimpleNamespace(organization=ORG, super_name=LAKE, storage=self.storage)

    def use(self) -> "_Lake":
        """Every catalog made from here on talks to this lake's Redis."""
        self._monkeypatch.setattr(
            redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=self.client))
        return self

    def put(self, root, entry, replica: bool):
        if root is not None:
            self.client.set(RK.meta_root(ORG, LAKE), root)
        if entry is not None:
            self.client.set(RK.meta_leaf(ORG, SOURCE if replica else LAKE, TABLE), entry)
        self.sent.clear()

    def reads(self):
        """What was sent, with the lake's keys shortened to their last part."""
        prefix = RK.meta_root(ORG, LAKE)[:-len(f"{LAKE}:meta:root")]
        return [(command, (key or "").replace(prefix, "")) for command, key in self.sent]


@pytest.fixture
def lake(monkeypatch):
    return _Lake(monkeypatch).use()


@pytest.fixture
def twin(monkeypatch):
    """Two lakes in the same state, for the old way and the new."""
    return _Lake(monkeypatch), _Lake(monkeypatch)


def _entry(**fields) -> str:
    return json.dumps({"version": 4, "ts": 1_795_000_000_001, **fields})


ENTRIES = {
    "no entry": None,
    "its snapshot inside": _entry(path=PATH, payload=SNAPSHOT),
    "a nested snapshot inside": _entry(path=PATH, payload={"snapshot": SNAPSHOT}),
    "a path only": _entry(path=PATH),
    "a path to a file that is gone": _entry(path=GONE),
    "a payload without resources": _entry(path=PATH, payload={"note": "x"}),
    "a payload whose resources are no list": _entry(path=PATH, payload=dict(SNAPSHOT, resources={})),
    "a payload that is a list": _entry(path=PATH, payload=[1, 2]),
    "no path": _entry(payload=SNAPSHOT),
    "an empty path": _entry(path="", payload=SNAPSHOT),
    "an empty value": "",
    "JSON null": "null",
    "JSON false": "false",
    "an empty object": "{}",
    "an empty list": "[]",
    "a number": "5",
    "a list": '["path"]',
    "text that is not JSON": "{not json",
}

_REPLICA = {"version": 1, "ts": 1, "read_only": True, "clone_type": "replica", "cloned_from": SOURCE}
# name -> (root document, whether the entry is the source lake's)
ROOTS = {
    "an ordinary lake": (json.dumps({"version": 3, "ts": 1}), False),
    "a lake without a root": (None, False),
    "a root that is not JSON": ("{not json", False),
    "a root that is a list": ("[1]", False),
    "a replica": (json.dumps(_REPLICA), True),
    "a replica of these tables": (json.dumps(dict(_REPLICA, replica_tables=[TABLE, "other"])), True),
    "a replica of other tables": (json.dumps(dict(_REPLICA, replica_tables=["other"])), True),
    "a replica of itself": (json.dumps(dict(_REPLICA, cloned_from=LAKE)), False),
}


def _the_writer_s_two_lines(cls, lake: _Lake, **first_open):
    """``DataWriter.write`` from the open to the snapshot, and how it ended."""
    lake.use()
    try:
        try:
            table = cls(lake.super_table, TABLE, create_if_missing=False, **first_open)
        except TableNotFoundError:
            table = cls(lake.super_table, TABLE, create_if_missing=True)
        snapshot, path = table.get_simple_table_snapshot()
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))
    return ("returned", snapshot, path)


def _without_what_a_bootstrap_makes_up(outcome):
    """A table created on the way gets a file name and a clock value of its own."""
    if outcome[0] != "returned":
        return outcome
    snapshot, path = copy.deepcopy(outcome[1]), outcome[2]
    if isinstance(snapshot, dict) and snapshot.get("snapshot_version") == 0:
        snapshot["last_updated_ms"] = "now"
        path = os.path.dirname(path) + "/<new>"
    return ("returned", snapshot, path)


# ---------------------------------------------------------------------------
# The same end, whatever is in Redis
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("root", sorted(ROOTS))
@pytest.mark.parametrize("entry", sorted(ENTRIES))
def test_the_writer_s_two_lines_end_as_they_did(twin, entry, root):
    was_lake, now_lake = twin
    document, replica = ROOTS[root]
    for one in twin:
        one.put(document, ENTRIES[entry], replica)

    was = _the_writer_s_two_lines(_AsItWas, was_lake)
    now = _the_writer_s_two_lines(SimpleTable, now_lake, keep_leaf=True)

    assert _without_what_a_bootstrap_makes_up(now) == _without_what_a_bootstrap_makes_up(was)
    # and each left the same in Redis and on storage
    assert sorted(now_lake.client.keys("*")) == sorted(was_lake.client.keys("*"))
    assert len(now_lake.storage.keys()) == len(was_lake.storage.keys())


def test_the_states_cover_every_way_the_two_lines_can_end(monkeypatch):
    """A snapshot returned from the entry, from storage and from a bootstrap,
    and each kind of error: so that the comparison above compares something."""
    ends = set()
    for entry in ENTRIES:
        lake = _Lake(monkeypatch)
        lake.put(ROOTS["an ordinary lake"][0], ENTRIES[entry], False)
        outcome = _the_writer_s_two_lines(_AsItWas, lake)
        ends.add(outcome[1] if outcome[0] == "raised" else
                 "snapshot v%s" % outcome[1].get("snapshot_version"))
    assert ends == {"snapshot v4", "snapshot v3", "snapshot v0", "FileNotFoundError",
                    "AttributeError", "JSONDecodeError"}


# ---------------------------------------------------------------------------
# What is sent to Redis
# ---------------------------------------------------------------------------

ROOT, LEAF = f"{LAKE}:meta:root", f"{LAKE}:meta:leaf:doc:{TABLE}"


def test_an_existing_table_costs_two_reads_where_it_cost_four(twin):
    was_lake, now_lake = twin
    for one in twin:
        one.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)

    was = _the_writer_s_two_lines(_AsItWas, was_lake)
    now = _the_writer_s_two_lines(SimpleTable, now_lake, keep_leaf=True)

    assert was_lake.reads() == [("GET", ROOT), ("EXISTS", LEAF), ("GET", ROOT), ("GET", LEAF)]
    assert now_lake.reads() == [("GET", ROOT), ("GET", LEAF)]
    assert now == was == ("returned", SNAPSHOT, PATH)


def test_a_replica_s_entry_is_read_from_its_source_once(lake):
    lake.put(ROOTS["a replica"][0], ENTRIES["its snapshot inside"], True)

    outcome = _the_writer_s_two_lines(SimpleTable, lake, keep_leaf=True)

    assert lake.reads() == [("GET", ROOT), ("GET", f"{SOURCE}:meta:leaf:doc:{TABLE}")]
    assert outcome == ("returned", SNAPSHOT, PATH)


def test_a_table_that_is_not_there_is_asked_for_once_before_it_is_created(lake):
    lake.put(ROOTS["an ordinary lake"][0], None, False)

    table = None
    with pytest.raises(TableNotFoundError):
        table = SimpleTable(lake.super_table, TABLE, create_if_missing=False, keep_leaf=True)

    assert table is None
    assert lake.reads() == [("GET", ROOT), ("GET", LEAF)]
    assert lake.storage.keys() == [PATH], "nothing was created"


# ---------------------------------------------------------------------------
# The entry is used once
# ---------------------------------------------------------------------------

def test_the_entry_serves_the_next_snapshot_and_no_other(lake):
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)
    table = SimpleTable(lake.super_table, TABLE, create_if_missing=False, keep_leaf=True)
    lake.sent.clear()

    first = table.get_simple_table_snapshot()
    assert lake.reads() == [], "the entry read at the open is the one used"
    assert first == (SNAPSHOT, PATH)
    assert "_opened_leaf" not in vars(table), "and it is let go of"

    newer = dict(SNAPSHOT, snapshot_version=5)
    lake.client.set(RK.meta_leaf(ORG, LAKE, TABLE), _entry(path=GONE, payload=newer))
    lake.sent.clear()

    second = table.get_simple_table_snapshot()
    assert lake.reads() == [("GET", ROOT), ("GET", LEAF)]
    assert second == (newer, GONE)


def test_what_the_caller_does_to_the_snapshot_stays_with_the_caller(lake):
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)
    table = SimpleTable(lake.super_table, TABLE, create_if_missing=False, keep_leaf=True)

    snapshot, _path = table.get_simple_table_snapshot()
    snapshot["resources"].append({"file": "added by the caller"})
    snapshot["rowid_watermark"] = 99

    assert table.get_simple_table_snapshot() == (SNAPSHOT, PATH)


@pytest.mark.parametrize("entry", ["an empty object", "an empty list", "JSON false"])
def test_an_entry_that_is_empty_is_still_the_entry_that_was_read(lake, entry):
    """Not ``None``, so it is kept, and not read a second time to find the
    same thing: the snapshot fails on it as it did."""
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES[entry], False)
    table = SimpleTable(lake.super_table, TABLE, create_if_missing=False, keep_leaf=True)
    lake.sent.clear()

    with pytest.raises(FileNotFoundError, match="No path found in simple table leaf pointer"):
        table.get_simple_table_snapshot()

    assert lake.reads() == []


# ---------------------------------------------------------------------------
# Where nothing changed
# ---------------------------------------------------------------------------

def test_without_the_argument_a_table_is_opened_as_before(lake):
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)

    table = SimpleTable(lake.super_table, TABLE, create_if_missing=False)

    assert lake.reads() == [("GET", ROOT), ("EXISTS", LEAF)]
    assert "_opened_leaf" not in vars(table)
    assert table.get_simple_table_snapshot() == (SNAPSHOT, PATH)
    assert lake.reads() == [("GET", ROOT), ("EXISTS", LEAF), ("GET", ROOT), ("GET", LEAF)]


def test_whether_to_create_a_table_is_still_decided_by_the_existence_check(lake):
    """``keep_leaf`` with ``create_if_missing=True`` changes nothing: an entry
    that exists but reads as nothing must not be taken for a missing table and
    have an empty one written over it."""
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES["an empty value"], False)

    table = SimpleTable(lake.super_table, TABLE, create_if_missing=True, keep_leaf=True)

    assert lake.reads() == [("GET", ROOT), ("EXISTS", LEAF)]
    assert "_opened_leaf" not in vars(table)
    assert lake.client.get(RK.meta_leaf(ORG, LAKE, TABLE)) == "", "the entry is as it was"
    assert lake.storage.keys() == [PATH], "no bootstrap snapshot was written"


def test_a_new_table_is_created_as_before(lake):
    lake.put(ROOTS["an ordinary lake"][0], None, False)

    outcome = _the_writer_s_two_lines(SimpleTable, lake, keep_leaf=True)

    assert outcome[0] == "returned"
    snapshot, path = outcome[1], outcome[2]
    assert snapshot["resources"] == [] and snapshot["snapshot_version"] == 0
    assert path.startswith(f"{ORG}/{LAKE}/tables/{TABLE}/snapshots/")
    # A script this Redis has not seen is refused, loaded and sent again.
    commands = [command for command, _key in lake.reads() if command != "SCRIPT LOAD"]
    commands = [command for i, command in enumerate(commands)
                if not (command == "EVALSHA" and i and commands[i - 1] == "EVALSHA")]
    assert commands == [
        "GET", "GET",                   # the open: root, entry (not there)
        "GET", "EXISTS",                # the creating open: root, existence
        "EVALSHA",                      # the bootstrap entry
        "GET", "GET",                   # the snapshot: root, entry
    ]


def test_an_object_made_without_its_constructor_reads_the_catalog(lake):
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)
    table = SimpleTable.__new__(SimpleTable)
    table.super_table, table.simple_name = lake.super_table, TABLE
    table.storage, table.catalog = lake.storage, RedisCatalog()

    assert table.get_simple_table_snapshot() == (SNAPSHOT, PATH)
    assert lake.reads() == [("GET", ROOT), ("GET", LEAF)]


# ---------------------------------------------------------------------------
# A read that fails is not a table that is missing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("failing", [("GET", RK.meta_root(ORG, LAKE)),
                                     ("GET", RK.meta_leaf(ORG, LAKE, TABLE))],
                         ids=["the root", "the entry"])
def test_a_redis_error_at_the_open_raises_and_creates_nothing(lake, failing):
    """As ``leaf_exists`` does (AUDIT_BUGS C3): had the error read as "no
    entry", the writer would go on to create an empty table over a live one."""
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)
    before = lake.client.get(RK.meta_leaf(ORG, LAKE, TABLE))
    lake.sent.clear()
    lake.fail_on = failing

    outcome = _the_writer_s_two_lines(SimpleTable, lake, keep_leaf=True)

    assert outcome == ("raised", "TimeoutError", "Timeout reading from socket")
    # Nothing was asked or written after the failure: no second attempt by
    # way of "the table is missing, create it".
    expected = [("GET", ROOT), ("GET", LEAF)]
    assert lake.reads() == expected[:1 + (failing[1] == RK.meta_leaf(ORG, LAKE, TABLE))]
    lake.fail_on = None
    assert lake.client.get(RK.meta_leaf(ORG, LAKE, TABLE)) == before
    assert lake.storage.keys() == [PATH]


def test_one_failed_read_is_enough_to_fail_the_open(lake):
    """The read is not tried again as an existence check: had Redis answered
    the second time, a write would have gone on after an error it never
    reported."""
    lake.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)
    failures = {"left": 1}
    real = lake.client.execute_command

    def once(*args, **options):
        if failures["left"] and (str(args[0]).upper(), args[1]) == ("GET", RK.meta_leaf(ORG, LAKE, TABLE)):
            failures["left"] -= 1
            lake.sent.append(("GET", args[1]))
            raise redis.exceptions.TimeoutError("Timeout reading from socket")
        return real(*args, **options)

    lake.client.execute_command = once

    outcome = _the_writer_s_two_lines(SimpleTable, lake, keep_leaf=True)

    assert outcome == ("raised", "TimeoutError", "Timeout reading from socket")
    assert lake.reads() == [("GET", ROOT), ("GET", LEAF)]


def test_it_fails_where_the_existence_check_failed(twin):
    was_lake, now_lake = twin
    for one in twin:
        one.put(ROOTS["an ordinary lake"][0], ENTRIES["its snapshot inside"], False)
    was_lake.fail_on = ("EXISTS", RK.meta_leaf(ORG, LAKE, TABLE))
    now_lake.fail_on = ("GET", RK.meta_leaf(ORG, LAKE, TABLE))

    was = _the_writer_s_two_lines(_AsItWas, was_lake)
    now = _the_writer_s_two_lines(SimpleTable, now_lake, keep_leaf=True)

    assert now == was == ("raised", "TimeoutError", "Timeout reading from socket")


def test_an_entry_of_the_wrong_kind_is_refused_as_before(twin):
    """Redis answers a GET of a hash with an error: the writer met it at the
    snapshot, and meets the same error at the open now."""
    was_lake, now_lake = twin
    for one in twin:
        one.put(ROOTS["an ordinary lake"][0], None, False)
        one.client.hset(RK.meta_leaf(ORG, LAKE, TABLE), "field", "value")
        one.sent.clear()

    was = _the_writer_s_two_lines(_AsItWas, was_lake)
    now = _the_writer_s_two_lines(SimpleTable, now_lake, keep_leaf=True)

    assert now == was
    assert now[:2] == ("raised", "ResponseError")
