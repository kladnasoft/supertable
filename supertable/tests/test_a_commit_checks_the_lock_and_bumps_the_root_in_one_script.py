"""
A commit checks the table lock, sets the leaf and bumps the root in one script.

``DataWriter`` published a snapshot in three round trips: ``verify_simple_lock``
(a GET of the lock key, compared in Python), ``set_leaf_payload_cas`` and
``bump_root``.  Between the first and the second another writer could take the
lock and commit, and this commit then overwrote it.
``RedisCatalog.commit_leaf_and_bump_root`` does the three in one script, which
Redis runs whole.

The script is built from the two scripts it replaces, so the comparison here is
with the three calls themselves, which are still there: for every state the
leaf and the root can be in, the one call must leave in Redis, byte for byte,
what the three calls leave, and return the versions they return.  Then the
lock: a token that does not hold the lock commits nothing.

``test_suite/test_catalog_commit.py`` repeats the comparison on a real Redis,
and ``tests/characterization/test_the_round_trips_of_a_write_to_redis.py`` runs
the real writer.
"""
from __future__ import annotations

import json
import logging
import os
from types import SimpleNamespace

import fakeredis
import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_catalog  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402
from supertable.errors import LockLostError  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402

ORG, LAKE, TABLE = "acme", "sales", "orders"
PATH = "acme/sales/tables/orders/snapshots/1795000000001_ab12_tables.json"
NOW = 1_795_000_000_001
TOKEN = "0123456789abcdef0123456789abcdef"
LEAF, ROOT, LOCK = RK.meta_leaf(ORG, LAKE, TABLE), RK.meta_root(ORG, LAKE), RK.lock_leaf(ORG, LAKE, TABLE)
LOGGER = redis_catalog.logger.name

SNAPSHOT = {
    "simple_name": TABLE, "location": "acme/sales/tables/orders", "snapshot_version": 8,
    "last_updated_ms": 1_795_000_000_000, "previous_snapshot": "acme/sales/tables/orders/snapshots/p.json",
    "schema": {"id": "Int64", "név": "String"},
    "resources": [{"file": f"acme/sales/tables/orders/data/f{i}.parquet", "file_size": 1000 + i,
                   "rows": 10 + i, "columns": 2} for i in range(3)],
    "tombstone": None, "tombstone_rows": 0, "stats_file": None, "stats_rows": 0,
    "lineage": {"source_type": "write", "overwrite_columns": [], "ratio": 0.1},
}


class _Side:
    """A catalog on a fake Redis of its own, recording its round trips."""

    def __init__(self, monkeypatch):
        self.client = fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)
        self.trips = []
        self.looking = False            # this test's own reads are not round trips of the catalog
        real = self.client.execute_command

        def command(*args, **options):
            name = str(args[0]).upper()
            if self.looking:
                pass
            elif name == "EVALSHA":
                self.trips.append(("EVALSHA", tuple(args[3:3 + int(args[2])])))
            elif name.startswith("SCRIPT"):
                # a script this Redis had not seen: the EVALSHA before it was
                # refused, and is sent again once the script is loaded
                if self.trips and self.trips[-1][0] == "EVALSHA":
                    self.trips.pop()
            else:
                self.trips.append((name, args[1] if len(args) > 1 else None))
            return real(*args, **options)

        self.client.execute_command = command
        with monkeypatch.context() as patched:
            patched.setattr(
                redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=self.client))
            self.catalog = RedisCatalog()

    def put(self, leaf=None, root=None, lock=TOKEN):
        for key, value in ((LEAF, leaf), (ROOT, root), (LOCK, lock)):
            if value is not None:
                self.client.set(key, value)
        self.trips.clear()

    def state(self) -> dict:
        self.looking = True
        try:
            return {key: self.client.get(key) for key in sorted(self.client.keys("*"))}
        finally:
            self.looking = False


@pytest.fixture
def side(monkeypatch):
    return _Side(monkeypatch)


@pytest.fixture
def twin(monkeypatch):
    return _Side(monkeypatch), _Side(monkeypatch)


def _three_calls(catalog: RedisCatalog, payload, path=PATH, now_ms=NOW, token=TOKEN):
    """``DataWriter`` publishing as it did: the lock, the leaf, the root."""
    if token and catalog.verify_simple_lock(ORG, LAKE, TABLE, token) is False:
        raise LockLostError(ORG, LAKE, TABLE)
    try:
        leaf_version = catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, payload, path, now_ms=now_ms)
    except Exception:   # noqa: BLE001 - the writer's fallback, whatever was raised
        leaf_version = catalog.set_leaf_path_cas(ORG, LAKE, TABLE, path, now_ms=now_ms)
    return leaf_version, catalog.bump_root(ORG, LAKE, now_ms=now_ms)


def _one_call(catalog: RedisCatalog, payload, path=PATH, now_ms=NOW, token=TOKEN):
    return catalog.commit_leaf_and_bump_root(ORG, LAKE, TABLE, payload, path, now_ms=now_ms,
                                             lock_token=token)


def _run(function, *args, **options):
    try:
        return ("returned", function(*args, **options))
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))


LEAVES = {
    "no leaf": None,
    "a leaf this release wrote": '{"version":41,"ts":1794000000000,"path":"x\\/y.json","payload":{"resources":[]}}',
    "a leaf an older release wrote": '{"path":"x\\/y.json","ts":1794000000000,"version":41,"payload":{"resources":{}}}',
    "a leaf without payload": '{"version":12,"ts":1794000000000,"path":"p.json"}',
    "a leaf written by hand": '{ "version": 5, "path": "p.json" }',
    "a leaf without a version": '{"path":"p.json","ts":5}',
    "text that is not JSON": "not json at all",
    "an empty value": "",
}

_FLAGS = {"version": 7, "ts": 1794000000000, "read_only": False, "cloned_from": "source",
          "clone_type": "readonly", "clone_ts": 1793000000000, "replica_tables": ["orders", "items"]}
ROOTS = {
    "no root": None,
    "a root": '{"version": 3, "ts": 1794000000000}',
    "a root as the script writes it": '{"ts":1794000000000,"version":3}',
    "a root with flags": json.dumps(_FLAGS),
    "a root at version 0": '{"version": 0, "ts": 1}',
    "a root whose version is text": '{"version": "seven", "ts": 1}',
    "a root whose version is numeric text": '{"version": "7", "ts": 1}',
    "a root without a version": '{"ts": 1, "read_only": true}',
    "a root that is a list": "[1, 2]",
    "a root that is a number": "5",
    "text that is not JSON": "{not json",
    "an empty value": "",
}

PAYLOADS = {
    "a snapshot": SNAPSHOT,
    "an empty table's snapshot": dict(SNAPSHOT, resources=[], schema=[]),
    "no payload": None,
    "an empty payload": {},
    "something that cannot be serialised": {"when": object()},
}


# ---------------------------------------------------------------------------
# The leaf and the root it stores are the ones the three calls store
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("root", sorted(ROOTS))
@pytest.mark.parametrize("leaf", sorted(LEAVES))
def test_it_leaves_in_redis_what_the_three_calls_leave(twin, leaf, root):
    was_side, now_side = twin
    for one in twin:
        one.put(LEAVES[leaf], ROOTS[root])

    was = _run(_three_calls, was_side.catalog, SNAPSHOT)
    now = _run(_one_call, now_side.catalog, SNAPSHOT)

    assert now == was
    assert now[0] == "returned" and type(now[1]) is tuple and len(now[1]) == 2
    assert now_side.state() == was_side.state(), "byte for byte"

    # and the commit after it, which meets what this one left
    was = _run(_three_calls, was_side.catalog, SNAPSHOT, path="next.json", now_ms=NOW + 1)
    now = _run(_one_call, now_side.catalog, SNAPSHOT, path="next.json", now_ms=NOW + 1)
    assert now == was
    assert now_side.state() == was_side.state()


@pytest.mark.parametrize("payload", sorted(PAYLOADS))
def test_it_stores_each_kind_of_payload_as_the_three_calls_do(twin, payload):
    was_side, now_side = twin
    for one in twin:
        one.put(LEAVES["a leaf this release wrote"], ROOTS["a root with flags"])

    was = _run(_three_calls, was_side.catalog, PAYLOADS[payload])
    now = _run(_one_call, now_side.catalog, PAYLOADS[payload])

    assert now == was == ("returned", (42, 8))
    assert now_side.state() == was_side.state()


def test_the_root_keeps_its_flags(side):
    side.put(LEAVES["a leaf this release wrote"], ROOTS["a root with flags"])

    assert _one_call(side.catalog, SNAPSHOT) == (42, 8)

    assert json.loads(side.client.get(ROOT)) == dict(_FLAGS, version=8, ts=NOW)
    leaf = json.loads(side.client.get(LEAF))
    assert (leaf["version"], leaf["ts"], leaf["path"]) == (42, NOW, PATH)
    assert leaf["payload"] == SNAPSHOT


def test_the_two_ways_take_over_from_each_other(twin):
    """A writer of an older release and one of this release on the same table."""
    was_side, mixed = twin
    for one in twin:
        one.put(LEAVES["a leaf an older release wrote"], ROOTS["a root"])

    for step in range(6):
        was = _run(_three_calls, was_side.catalog, SNAPSHOT, path=f"s{step}.json", now_ms=NOW + step)
        way = _one_call if step % 2 else _three_calls
        now = _run(way, mixed.catalog, SNAPSHOT, path=f"s{step}.json", now_ms=NOW + step)
        assert now == was == ("returned", (42 + step, 4 + step))
        assert mixed.state() == was_side.state()


# ---------------------------------------------------------------------------
# One round trip
# ---------------------------------------------------------------------------

def test_three_round_trips_became_one(twin):
    was_side, now_side = twin
    for one in twin:
        one.put(LEAVES["a leaf this release wrote"], ROOTS["a root"])

    _three_calls(was_side.catalog, SNAPSHOT)
    _one_call(now_side.catalog, SNAPSHOT)

    assert was_side.trips == [("GET", LOCK), ("EVALSHA", (LEAF,)), ("EVALSHA", (ROOT,))]
    assert now_side.trips == [("EVALSHA", (LEAF, ROOT, LOCK))]


def test_the_script_is_made_of_the_two_it_replaces():
    """Not a copy of them: a change to either is a change to this one."""
    script = RedisCatalog._LUA_LEAF_COMMIT_FENCED

    assert RedisCatalog._LUA_LEAF_PAYLOAD_CAS_SET in script
    assert RedisCatalog._LUA_ROOT_BUMP in script


# ---------------------------------------------------------------------------
# The lock
# ---------------------------------------------------------------------------

LOCKS = {
    "no lock key": None,
    "another writer's token": "ffffffffffffffffffffffffffffffff",
    "a token one character longer": TOKEN + "0",
    "a token one character shorter": TOKEN[:-1],
    "the token in upper case": TOKEN.upper(),
    "an empty value": "",
}


@pytest.mark.parametrize("held_by", sorted(LOCKS))
def test_a_token_that_does_not_hold_the_lock_commits_nothing(twin, held_by):
    was_side, now_side = twin
    for one in twin:
        one.put(LEAVES["a leaf this release wrote"], ROOTS["a root with flags"], lock=LOCKS[held_by])
    before = now_side.state()

    was = _run(_three_calls, was_side.catalog, SNAPSHOT)
    now = _run(_one_call, now_side.catalog, SNAPSHOT)

    assert now == was
    assert now[:2] == ("raised", "LockLostError")
    assert now_side.state() == before, "neither the leaf nor the root was touched"
    assert now_side.trips == [("EVALSHA", (LEAF, ROOT, LOCK))]


def test_the_error_is_the_one_the_writer_raised(side):
    side.put(LEAVES["no leaf"], ROOTS["a root"], lock=None)

    with pytest.raises(LockLostError) as raised:
        _one_call(side.catalog, SNAPSHOT)

    assert str(raised.value) == str(LockLostError(ORG, LAKE, TABLE))
    assert (raised.value.organization, raised.value.super_name, raised.value.simple_name) == (
        ORG, LAKE, TABLE)


def test_the_token_that_holds_the_lock_commits(side):
    side.put(LEAVES["no leaf"], ROOTS["no root"])

    assert _one_call(side.catalog, SNAPSHOT) == (0, 0)
    assert side.client.get(LOCK) == TOKEN, "the lock is neither released nor renewed by the commit"


def test_a_lock_the_heartbeat_reported_lost_commits_nothing_and_asks_nothing(twin):
    """The first of the two signals of ``verify_simple_lock``: the lock key
    still holds the token, but the heartbeat could not renew it and has
    stopped trying."""
    was_side, now_side = twin
    for one in twin:
        one.put(LEAVES["a leaf this release wrote"], ROOTS["a root"])
        with one.catalog._locker._held_lock:
            one.catalog._locker._lost[(LOCK, TOKEN)] = None
    before = now_side.state()

    was = _run(_three_calls, was_side.catalog, SNAPSHOT)
    now = _run(_one_call, now_side.catalog, SNAPSHOT)

    assert now == was
    assert now[:2] == ("raised", "LockLostError")
    assert now_side.trips == [], "known without a round trip"
    assert now_side.state() == before


@pytest.mark.parametrize("token", [None, ""])
def test_without_a_token_nothing_is_checked(side, token):
    side.put(LEAVES["a leaf this release wrote"], ROOTS["a root"], lock="someone else")

    assert _one_call(side.catalog, SNAPSHOT, token=token) == (42, 4)
    assert side.client.get(LOCK) == "someone else"


def test_one_table_s_lock_does_not_stand_for_another_s(side):
    side.put(LEAVES["no leaf"], ROOTS["a root"])

    with pytest.raises(LockLostError):
        side.catalog.commit_leaf_and_bump_root(ORG, LAKE, "items", SNAPSHOT, PATH, now_ms=NOW,
                                               lock_token=TOKEN)

    assert side.client.get(RK.meta_leaf(ORG, LAKE, "items")) is None
    assert json.loads(side.client.get(ROOT))["version"] == 3


# ---------------------------------------------------------------------------
# What cannot be stored, and what Redis refuses
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_a_number_that_is_not_finite_is_refused_before_anything_is_sent(side, value, caplog):
    """As ``set_leaf_payload_cas`` refuses it, so that the writer can store a
    leaf without payload instead.  Not logged here: that call logs it."""
    side.put(LEAVES["a leaf this release wrote"], ROOTS["a root"])
    before = side.state()
    payload = dict(SNAPSHOT, lineage={"ratio": value})

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(redis.ResponseError, match="not finite"):
            _one_call(side.catalog, payload)

    assert side.trips == []
    assert side.state() == before
    assert not caplog.records


@pytest.mark.parametrize("key, kind", [(LOCK, "the lock"), (LEAF, "the leaf"), (ROOT, "the root")])
def test_a_key_that_holds_no_string_fails_the_commit_before_anything_is_written(side, key, kind, caplog):
    """Everything the script reads it reads before it writes."""
    side.put(LEAVES["a leaf this release wrote"], ROOTS["a root with flags"])
    side.client.delete(key)
    side.client.hset(key, "field", "value")
    before = {k: side.client.get(k) for k in (LEAF, ROOT, LOCK) if k != key}
    side.trips.clear()

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(Exception) as raised:
            _one_call(side.catalog, SNAPSHOT)

    assert not isinstance(raised.value, LockLostError)
    assert {k: side.client.get(k) for k in before} == before, f"{kind} is no string: nothing was written"
    assert side.client.hgetall(key) == {"field": "value"}


def test_an_error_of_redis_is_logged_and_raised(side, caplog):
    side.put(LEAVES["a leaf this release wrote"], ROOTS["a root"])

    def refuse(*args, **options):
        raise redis.exceptions.ConnectionError("Connection refused")

    side.catalog._leaf_commit_fenced = refuse
    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(redis.exceptions.ConnectionError):
            _one_call(side.catalog, SNAPSHOT)

    assert any("commit_leaf_and_bump_root error: Connection refused" in r.getMessage()
               for r in caplog.records)


@pytest.mark.parametrize("reply", [None, [], [2, 1, 1], [1, 5], "OK", 7])
def test_a_reply_that_is_not_the_script_s_is_an_error(side, reply):
    side.put(LEAVES["a leaf this release wrote"], ROOTS["a root"])
    side.catalog._leaf_commit_fenced = lambda keys, args: reply

    with pytest.raises(redis.ResponseError, match="unexpected reply"):
        _one_call(side.catalog, SNAPSHOT)


# ---------------------------------------------------------------------------
# The three calls are as they were
# ---------------------------------------------------------------------------

def test_the_lock_check_is_made_of_the_same_two_signals(side):
    side.put()
    locker = side.catalog._locker

    assert locker.is_known_lost(LOCK, TOKEN) is False
    assert side.catalog.verify_simple_lock(ORG, LAKE, TABLE, TOKEN) is True
    assert side.trips == [("GET", LOCK)]

    with locker._held_lock:
        locker._lost[(LOCK, TOKEN)] = None
    side.trips.clear()
    assert locker.is_known_lost(LOCK, TOKEN) is True
    assert locker.is_known_lost(LOCK, "another token") is False
    assert side.catalog.verify_simple_lock(ORG, LAKE, TABLE, TOKEN) is False
    assert side.trips == [], "a loss the heartbeat recorded needs no round trip"


def test_the_leaf_call_still_logs_and_refuses_a_number_that_is_not_finite(side, caplog):
    side.put(LEAVES["a leaf this release wrote"], ROOTS["a root"])
    before = side.state()

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(redis.ResponseError, match="not finite"):
            side.catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, {"x": float("nan")}, PATH, now_ms=NOW)

    assert [r.getMessage() for r in caplog.records] == [
        "[redis-catalog] leaf_payload_cas_set error: leaf payload holds a number that is not "
        "finite (NaN or Infinity)"]
    assert side.state() == before and side.trips == []
