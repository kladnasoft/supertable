# route: test_suite.test_catalog_commit
"""The commit of a table's catalog entry, on a real Redis.

WHAT THIS PROVES

A commit stores the table's snapshot in Redis beside its path. The script that
does it used to decode and re-encode the snapshot with cjson, which blocked
Redis for the length of the snapshot and changed it on the way: empty lists
came out as ``{}``, integers of 15 digits or more as rounded floats, floats
without their digits past the fourteenth. It now stores the snapshot as the
text it is given and reads the old entry's version from its first bytes.

Only a real Redis has the real cjson, so only here can the two scripts be
compared where they are supposed to agree, and the damage be shown where they
are supposed to differ:

  * what a reader finds in the entry is the snapshot, value for value, and it
    is what the snapshot file holds after every kind of write;
  * the version, timestamp and path are what the script as it was stored,
    whatever was in the key before;
  * the two scripts take over from each other, as two releases writing one
    table would;
  * a commit over an entry this script wrote does not fetch that entry;
  * the one script that checks the table lock, sets the entry and bumps the
    root leaves what the three calls it replaces leave, refuses a token that
    does not hold the lock, and loses no update when locks are taken away.

The script as it was is kept below, word for word.
"""

from __future__ import annotations

import json
import uuid

import pyarrow as pa
import pytest
import redis

ORG = "correctness"
SUPER = "commits"
ROLE = "superadmin"
PATH = "correctness/commits/tables/t/snapshots/1795000000001_ab12_t.json"
NOW = 1_795_000_000_001

_SCRIPT_AS_IT_WAS = """
local key = KEYS[1]
local payload_json = ARGV[1]
local new_path = ARGV[2]
local now_ms = tonumber(ARGV[3])

local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then
    old_version = tonumber(obj['version'])
  end
end
local new_version = old_version + 1

local payload = {}
local okp, pobj = pcall(cjson.decode, payload_json)
if okp and pobj then
  payload = pobj
end

local new_val = cjson.encode({version=new_version, ts=now_ms, path=new_path, payload=payload})
redis.call('SET', key, new_val)
return new_version
"""


@pytest.fixture(scope="module")
def catalog():
    from supertable.redis_catalog import RedisCatalog
    return RedisCatalog()


@pytest.fixture(scope="module")
def as_it_was(catalog):
    return catalog.r.register_script(_SCRIPT_AS_IT_WAS)


@pytest.fixture
def key(catalog, request):
    """A key of this test's own, removed afterwards."""
    name = "supertable:test-suite:commit:" + request.node.name
    catalog.r.delete(name)
    yield name
    catalog.r.delete(name)


def _snapshot(files: int = 2, **more) -> dict:
    doc = {
        "simple_name": "t", "location": "correctness/commits/tables/t", "snapshot_version": 7,
        "last_updated_ms": 1_795_000_000_000,
        "previous_snapshot": "correctness/commits/tables/t/snapshots/prev.json",
        "schema": {"zeta": "Int64", "alpha": "String", "__rowid__": "Int64"},
        "resources": [{"file": f"correctness/commits/tables/t/data/year=2026/data_{i}.parquet",
                       "file_size": 12_582_912 + i, "rows": 1_100_000 + i, "columns": 3}
                      for i in range(files)],
        "tombstone": None, "tombstone_rows": 0,
        "stats_file": "correctness/commits/tables/t/stats/s.parquet", "stats_rows": files * 3,
        "lineage": {"source_type": "write", "overwrite_columns": ["id"], "incoming_rows": 100},
    }
    doc.update(more)
    return doc


def _same(a, b) -> bool:
    """Equal, types and key order included."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return list(a) == list(b) and all(_same(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, float):
        return repr(a) == repr(b)
    return a == b


def _commit(script, client, key, snapshot, path=PATH, now_ms=NOW):
    try:
        outcome = ("returned", int(script(keys=[key], args=[
            json.dumps(snapshot, separators=(",", ":")), path, now_ms])))
    except redis.ResponseError:
        outcome = ("raised", "ResponseError")
    raw = client.get(key)
    try:
        left = json.loads(raw) if raw else raw
    except ValueError:
        left = raw
    return outcome, left


# ---------------------------------------------------------------------------
# What the old script changed on the way, and this one does not
# ---------------------------------------------------------------------------

DAMAGED_BEFORE = {
    "the empty snapshot of a new table": (
        {"simple_name": "t", "snapshot_version": 0, "schema": [], "resources": [],
         "tombstone": None, "stats_file": None},
        {"schema": {}, "resources": {}}),
    "the lineage of an append": (
        _snapshot(1, lineage={"source_type": "write", "overwrite_columns": []}),
        {"lineage": {"source_type": "write", "overwrite_columns": {}}}),
    "an integer of 15 digits": (
        _snapshot(1, lineage={"job_id": 123456789012345}),
        {"lineage": {"job_id": 123456789012340.0}}),
    "an integer of 19 digits": (
        _snapshot(1, lineage={"run_id": 1234567890123456789}),
        {"lineage": {"run_id": 1.2345678901235e+18}}),
    "a float of 16 digits": (
        _snapshot(1, lineage={"pi": 3.141592653589793}),
        {"lineage": {"pi": 3.1415926535898}}),
    "a float that is a whole number": (
        _snapshot(1, lineage={"whole": 2.0}),
        {"lineage": {"whole": 2}}),
}


@pytest.mark.parametrize("name", sorted(DAMAGED_BEFORE))
def test_the_entry_holds_what_the_old_script_changed(catalog, as_it_was, key, name):
    snapshot, changed_to = DAMAGED_BEFORE[name]

    _outcome, was = _commit(as_it_was, catalog.r, key, snapshot)
    for field, value in changed_to.items():
        # cjson also stored the keys in an order of its own: compared sorted.
        assert json.dumps(was["payload"][field], sort_keys=True) == json.dumps(
            value, sort_keys=True), "the damage this case is about"

    catalog.r.delete(key)
    _outcome, now = _commit(catalog._leaf_payload_cas_set, catalog.r, key, snapshot)
    assert _same(now["payload"], snapshot)


UNCHANGED_BY_EITHER = {
    "a table of files": _snapshot(40),
    "a deletion vector": _snapshot(2, tombstone=["t/a.parquet", "t/b.parquet"], tombstone_rows=9),
    "text outside ASCII": _snapshot(1, lineage={"note": "árvíztűrő 日本語 🎉", "key ű": "ø"}),
    "text that needs escaping": _snapshot(1, lineage={
        "q": 'say "hi"', "b": "a\\b", "n": "a\nb", "t": "a\tb", "c": "\u0001", "s": "a/b",
        "u": " ", "tag": "</script>"}),
    "booleans, nulls and small numbers": _snapshot(1, lineage={
        "yes": True, "no": False, "nothing": None, "n": 99999999999999, "half": 0.5,
        "tiny": 1e-300, "big": 1e22, "neg": -17}),
    "a row filter": _snapshot(1, _row_filter="region = 'eu'"),
    "nested objects": _snapshot(1, lineage={"tags": {"team": {"name": "ingest"}}, "files": ["a", "b"]}),
}


@pytest.mark.parametrize("name", sorted(UNCHANGED_BY_EITHER))
def test_everything_else_reads_as_it_did(catalog, as_it_was, key, name):
    """Where cjson gave back what it was given, a reader sees no difference."""
    snapshot = UNCHANGED_BY_EITHER[name]

    was_outcome, was = _commit(as_it_was, catalog.r, key, snapshot)
    catalog.r.delete(key)
    now_outcome, now = _commit(catalog._leaf_payload_cas_set, catalog.r, key, snapshot)

    assert now_outcome == was_outcome == ("returned", 0)
    assert now == was, "the same document, read back"
    assert _same(now["payload"], snapshot)
    assert list(now) == ["version", "ts", "path", "payload"]


# ---------------------------------------------------------------------------
# The version, whatever was in the key
# ---------------------------------------------------------------------------

VALUES_BEFORE = {
    "no entry": None,
    "an entry the old script wrote":
        '{"path":"x\\/y.json","ts":1794000000000,"version":41,"payload":{"resources":{}}}',
    "an entry this script wrote":
        '{"version":41,"ts":1794000000000,"path":"x\\/y.json","payload":{"resources":[]}}',
    "an entry without payload": '{"version":12,"ts":1794000000000,"path":"p.json"}',
    "a version of 0": '{"version":0,"ts":1,"path":"p"}',
    "a version alone": '{"version":7}',
    "a version of 14 digits": '{"version":99999999999998,"path":"p"}',
    "a version of 17 digits": '{"version":12345678901234567,"path":"p"}',
    "a version of 19 digits": '{"version":1234567890123456789,"path":"p"}',
    "a version of 30 digits": '{"version":123456789012345678901234567890,"path":"p"}',
    "spaces, as a person would write it": '{ "version": 5, "path": "p.json" }',
    "a version that is a fraction": '{"version":3.5,"path":"p.json"}',
    "a version in scientific notation": '{"version":1.2345678901235e+16,"path":"p.json"}',
    "a version that is negative": '{"version":-4,"path":"p.json"}',
    "a version that is text": '{"version":"seven","path":"p.json"}',
    "a version that is numeric text": '{"version":"7","path":"p.json"}',
    "a version that is null": '{"version":null,"path":"p.json"}',
    "a version that is true": '{"version":true,"path":"p.json"}',
    "no version": '{"path":"p.json","ts":5}',
    "a payload that mentions a version first": '{"payload":{"version":99},"version":6,"path":"p"}',
    "a version inside a payload only": '{"payload":{"version":99},"path":"p"}',
    "not JSON": "not json at all",
    "a leaf cut off after its version": '{"version":8,"ts":17',
    "an empty value": "",
    "a JSON list": "[1,2,3]",
    "a JSON number": "5",
    "JSON null": "null",
    "JSON true": "true",
    "a JSON string": '"version"',
}


@pytest.mark.parametrize("name", sorted(VALUES_BEFORE))
def test_two_commits_give_what_the_script_as_it_was_gave(catalog, as_it_was, key, name):
    client, new = catalog.r, catalog._leaf_payload_cas_set
    before = VALUES_BEFORE[name]
    snapshot = UNCHANGED_BY_EITHER["a table of files"]
    was_key = key + ":was"
    try:
        for k in (key, was_key):
            client.delete(k)
            if before is not None:
                client.set(k, before)

        first_was = _commit(as_it_was, client, was_key, snapshot)
        first_now = _commit(new, client, key, snapshot)
        assert first_now == first_was

        second_was = _commit(as_it_was, client, was_key, snapshot, path="next.json", now_ms=NOW + 1)
        second_now = _commit(new, client, key, snapshot, path="next.json", now_ms=NOW + 1)
        assert second_now == second_was
    finally:
        client.delete(was_key)


@pytest.mark.parametrize("name", sorted(VALUES_BEFORE))
def test_the_two_scripts_take_over_from_each_other(catalog, as_it_was, key, name):
    """One release with the old script and one with this one, writing the same
    table in either order."""
    client, new = catalog.r, catalog._leaf_payload_cas_set
    before = VALUES_BEFORE[name]
    snapshot = UNCHANGED_BY_EITHER["a table of files"]
    keys = {k: key + ":" + k for k in ("was", "old-new", "new-old")}
    try:
        for k in keys.values():
            client.delete(k)
            if before is not None:
                client.set(k, before)
        _commit(as_it_was, client, keys["was"], snapshot)
        was = _commit(as_it_was, client, keys["was"], snapshot, path="next.json", now_ms=NOW + 1)

        _commit(as_it_was, client, keys["old-new"], snapshot)
        assert _commit(new, client, keys["old-new"], snapshot, path="next.json", now_ms=NOW + 1) == was
        _commit(new, client, keys["new-old"], snapshot)
        assert _commit(as_it_was, client, keys["new-old"], snapshot, path="next.json",
                       now_ms=NOW + 1) == was
    finally:
        client.delete(*keys.values())


# ---------------------------------------------------------------------------
# The script's arguments: path, clock value, payload text
# ---------------------------------------------------------------------------

# What the catalog never sends but the script could be handed.  The script as
# it was is the measure.
ARGUMENTS = {
    "a path with a quote": ('{"a":1}', 'p/"quoted".json', NOW),
    "a path with a backslash": ('{"a":1}', "p\\q.json", NOW),
    "a path with a line break and a tab": ('{"a":1}', "p\nq\t.json", NOW),
    "a path with a control character": ('{"a":1}', "p\x01q\x7f.json", NOW),
    "a path outside ASCII": ('{"a":1}', "árvíz/日本.json", NOW),
    "an empty path": ('{"a":1}', "", NOW),
    "a clock value with a fraction": ('{"a":1}', PATH, "1795000000001.5"),
    "a clock value of 17 digits": ('{"a":1}', PATH, "12345678901234567"),
    "a clock value in scientific notation": ('{"a":1}', PATH, "1.5e12"),
    "a clock value of zero": ('{"a":1}', PATH, 0),
    "a negative clock value": ('{"a":1}', PATH, -5),
    "a clock value that is no number": ('{"a":1}', PATH, "soon"),
    "an empty clock value": ('{"a":1}', PATH, ""),
    "an empty payload text": ("", PATH, NOW),
}


def _call(script, client, key, arguments):
    try:
        outcome = ("returned", int(script(keys=[key], args=list(arguments))))
    except redis.ResponseError:
        outcome = ("raised", "ResponseError")
    raw = client.get(key)
    try:
        left = json.loads(raw) if raw else raw
    except ValueError:
        left = ("NOT JSON", raw)
    return outcome, left


@pytest.mark.parametrize("name", sorted(ARGUMENTS))
def test_path_clock_and_payload_text_are_stored_as_the_script_as_it_was_stored_them(
        catalog, as_it_was, key, name):
    client, new = catalog.r, catalog._leaf_payload_cas_set
    snapshot = UNCHANGED_BY_EITHER["a table of files"]
    was_key = key + ":was"
    try:
        client.delete(was_key)

        was = _call(as_it_was, client, was_key, ARGUMENTS[name])
        now = _call(new, client, key, ARGUMENTS[name])

        assert was[0] == ("returned", 0)
        assert isinstance(was[1], dict), "the script as it was stored JSON"
        assert now == was

        was = _commit(as_it_was, client, was_key, snapshot, path="next.json", now_ms=NOW + 1)
        now = _commit(new, client, key, snapshot, path="next.json", now_ms=NOW + 1)
        assert now == was
        assert now[0] == ("returned", 1)
    finally:
        client.delete(was_key)


# ---------------------------------------------------------------------------
# Where the two scripts differ: entries no writer produces
# ---------------------------------------------------------------------------

# The value in the key, and what the script as it was made of it.
ONLY_BY_HAND = {
    "damaged in the middle":
        ('{"version":8,"ts":1,"path":"p",,,}', ("returned", 0)),
    "nested deeper than cjson reads":
        ('{"version":8,"x":' + "[" * 1100 + "]" * 1100 + "}", ("returned", 0)),
    "its version named twice":
        ('{"version":8,"path":"p","version":20}', ("returned", 21)),
    "its version named twice, the second not a number":
        ('{"version":8,"path":"p","version":"x"}', ("raised", "ResponseError")),
}


@pytest.mark.parametrize("name", sorted(ONLY_BY_HAND))
def test_an_entry_that_begins_with_its_version_is_taken_at_its_word(catalog, as_it_was, key, name):
    """An entry that begins ``{"version":8`` and ends with ``}`` is not decoded,
    so what the rest of it holds no longer decides the next version: it is 9.

    The old script decoded all of it.  Where it could not (the text is not
    JSON, or nests deeper than cjson reads) it started the table's versions
    again from 0; where the text names ``version`` a second time it went on
    from the later value, and failed if that was not a number.  Each of these
    takes a hand in Redis: every entry a script wrote names its version once
    and is JSON.
    """
    client, new = catalog.r, catalog._leaf_payload_cas_set
    before, as_it_was_gave = ONLY_BY_HAND[name]
    snapshot = UNCHANGED_BY_EITHER["a table of files"]
    was_key = key + ":was"
    try:
        for k in (key, was_key):
            client.set(k, before)

        was = _commit(as_it_was, client, was_key, snapshot)
        now = _commit(new, client, key, snapshot)

        assert was[0] == as_it_was_gave
        assert now[0] == ("returned", 9)
        assert _same(now[1], {"version": 9, "ts": NOW, "path": PATH, "payload": snapshot})
    finally:
        client.delete(was_key)


def test_the_commit_without_payload_reads_this_script_s_entry_too(catalog):
    """The writers' fallback, whose script still decodes the entry it finds."""
    table = "pathonly"
    try:
        assert catalog.set_leaf_payload_cas(ORG, SUPER, table, _snapshot(3), PATH, now_ms=NOW) == 0
        assert catalog.set_leaf_path_cas(ORG, SUPER, table, "p2.json", now_ms=NOW + 1) == 1
        assert catalog.get_leaf(ORG, SUPER, table) == {"version": 1, "ts": NOW + 1, "path": "p2.json"}
        assert catalog.set_leaf_payload_cas(ORG, SUPER, table, _snapshot(4), "p3.json", now_ms=NOW + 2) == 2
        leaf = catalog.get_leaf(ORG, SUPER, table)
        assert (leaf["version"], leaf["ts"], leaf["path"]) == (2, NOW + 2, "p3.json")
        assert _same(leaf["payload"], _snapshot(4))
    finally:
        catalog.delete_leaf(ORG, SUPER, table)


# ---------------------------------------------------------------------------
# A number that is not finite
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_a_number_that_is_not_finite_is_refused_and_nothing_is_stored(catalog, as_it_was, key, value):
    snapshot = _snapshot(1, lineage={"ratio": value})
    client = catalog.r
    text = json.dumps(snapshot)                     # as the old method sent it

    with pytest.raises(redis.ResponseError):        # the script as it was
        as_it_was(keys=[key], args=[text, PATH, NOW])
    assert client.get(key) is None

    table = "notfinite"
    try:
        with pytest.raises(redis.ResponseError):    # the catalog as it is
            catalog.set_leaf_payload_cas(ORG, SUPER, table, snapshot, PATH, now_ms=NOW)
        assert catalog.get_leaf(ORG, SUPER, table) is None

        catalog.set_leaf_payload_cas(ORG, SUPER, table, _snapshot(2), PATH, now_ms=NOW)
        before = catalog.get_leaf(ORG, SUPER, table)
        with pytest.raises(redis.ResponseError):
            catalog.set_leaf_payload_cas(ORG, SUPER, table, snapshot, "p2.json", now_ms=NOW + 1)
        assert catalog.get_leaf(ORG, SUPER, table) == before
    finally:
        catalog.delete_leaf(ORG, SUPER, table)


# ---------------------------------------------------------------------------
# What a commit asks of Redis
# ---------------------------------------------------------------------------

def _script_commands(client, key, do) -> dict:
    """The commands a script ran on *key* (or on any of several keys) while
    ``do()`` ran, counted by name.

    Read from ``MONITOR`` and kept only when they came from a script and name
    the key.  The server's own counters (``INFO commandstats``) count every
    client of the server, so another process on the same Redis, or a thread of
    this one, would make a commit look like it issued commands it did not.
    """
    token = "commit-test-" + uuid.uuid4().hex
    keys = {key} if isinstance(key, str) else set(key)
    # A connection of its own with a timeout: a stream that never brings the
    # marker fails the test instead of hanging it.
    watcher = redis.Redis(**{**client.connection_pool.connection_kwargs, "socket_timeout": 30})
    made: dict = {}
    try:
        with watcher.monitor() as monitor:
            do()
            client.echo(token)
            for seen in monitor.listen():
                words = seen["command"].split(" ")
                if words[-1] == token:
                    break
                if seen["client_type"] == "lua" and keys & set(words[1:2]):
                    name = words[0].lower()
                    made[name] = made.get(name, 0) + 1
    finally:
        watcher.close()
    return made


def test_a_commit_over_its_own_entry_does_not_fetch_it(catalog, key):
    client, new = catalog.r, catalog._leaf_payload_cas_set
    _commit(new, client, key, _snapshot(50))

    made = _script_commands(client, key, lambda: _commit(new, client, key, _snapshot(51)))

    assert made == {"getrange": 2, "set": 1}


def test_a_commit_over_an_older_entry_decodes_it_once_and_never_again(catalog, as_it_was, key):
    client, new = catalog.r, catalog._leaf_payload_cas_set
    _commit(as_it_was, client, key, _snapshot(50))

    first = _script_commands(client, key, lambda: _commit(new, client, key, _snapshot(51)))
    second = _script_commands(client, key, lambda: _commit(new, client, key, _snapshot(52)))

    assert first == {"getrange": 1, "get": 1, "set": 1}, "the entry is fetched and decoded"
    assert second == {"getrange": 2, "set": 1}, "and from then on it is not"
    assert json.loads(client.get(key))["version"] == 2


def test_the_script_as_it_was_fetched_the_entry_every_time(catalog, as_it_was, key):
    """What the counts above are measured against."""
    client = catalog.r
    _commit(as_it_was, client, key, _snapshot(50))

    made = _script_commands(client, key, lambda: _commit(as_it_was, client, key, _snapshot(51)))

    assert made == {"get": 1, "set": 1}


# ---------------------------------------------------------------------------
# One script for the lock check, the leaf and the root
# ---------------------------------------------------------------------------
#
# ``DataWriter`` published in three round trips: the lock key read and compared,
# the leaf set, the root bumped.  ``commit_leaf_and_bump_root`` does the three
# in one script, so that no other writer can take the lock and commit between
# the check and this commit.  The script is built from the two it replaces;
# here the one call is compared with the three on the real Redis.

RACE = "commits_fenced"
TOKEN = "0123456789abcdef0123456789abcdef"

ROOTS_BEFORE = {
    "no root": None,
    "a root": '{"version": 3, "ts": 1794000000000}',
    "a root as the script writes it": '{"ts":1794000000000,"version":3}',
    "a root with flags": json.dumps({
        "version": 7, "ts": 1794000000000, "read_only": False, "cloned_from": "source",
        "clone_type": "readonly", "clone_ts": 1793000000000, "replica_tables": ["t", "u"]}),
    "a root whose version is text": '{"version": "seven", "ts": 1}',
    "a root that is a list": "[1, 2]",
    "text that is not JSON": "{not json",
}
LEAVES_BEFORE = {name: VALUES_BEFORE[name] for name in (
    "no entry", "an entry this script wrote", "an entry the old script wrote",
    "an entry without payload", "spaces, as a person would write it", "not JSON")}


def _lake_keys(lake: str, table: str = "t"):
    from supertable import redis_keys as RK
    return RK.meta_leaf(ORG, lake, table), RK.meta_root(ORG, lake), RK.lock_leaf(ORG, lake, table)


def _publish_in_three_calls(catalog, lake, payload, path, now_ms, token=TOKEN, table="t"):
    """The writer's three steps, as ``DataWriter`` made them."""
    from supertable.errors import LockLostError
    if token and catalog.verify_simple_lock(ORG, lake, table, token) is False:
        raise LockLostError(ORG, lake, table)
    try:
        leaf_version = catalog.set_leaf_payload_cas(ORG, lake, table, payload, path, now_ms=now_ms)
    except Exception:   # noqa: BLE001 - the writer's fallback, whatever was raised
        leaf_version = catalog.set_leaf_path_cas(ORG, lake, table, path, now_ms=now_ms)
    return leaf_version, catalog.bump_root(ORG, lake, now_ms=now_ms)


def _outcome(do):
    try:
        return ("returned", do())
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))


@pytest.fixture
def two_lakes(catalog):
    """Two lakes of this test's own, in the same state: one for the three
    calls, one for the one script."""
    names = ("fenced_was_" + uuid.uuid4().hex[:8], "fenced_now_" + uuid.uuid4().hex[:8])
    yield names
    for name in names:
        catalog.r.delete(*_lake_keys(name))


@pytest.mark.parametrize("root", sorted(ROOTS_BEFORE))
@pytest.mark.parametrize("leaf", sorted(LEAVES_BEFORE))
def test_one_script_leaves_what_the_three_calls_leave(catalog, two_lakes, leaf, root):
    client = catalog.r
    snapshot = UNCHANGED_BY_EITHER["a table of files"]
    for lake in two_lakes:
        leaf_key, root_key, lock_key = _lake_keys(lake)
        for key, value in ((leaf_key, LEAVES_BEFORE[leaf]), (root_key, ROOTS_BEFORE[root]),
                           (lock_key, TOKEN)):
            if value is not None:
                client.set(key, value)
    was_lake, now_lake = two_lakes

    for step in range(2):       # the second commit meets what the first one left
        was = _outcome(lambda: _publish_in_three_calls(
            catalog, was_lake, snapshot, f"s{step}.json", NOW + step))
        now = _outcome(lambda: catalog.commit_leaf_and_bump_root(
            ORG, now_lake, "t", snapshot, f"s{step}.json", now_ms=NOW + step, lock_token=TOKEN))

        assert now == was
        assert now[0] == "returned"
        for was_key, now_key in zip(_lake_keys(was_lake)[:2], _lake_keys(now_lake)[:2]):
            assert client.get(now_key) == client.get(was_key), "byte for byte"


@pytest.mark.parametrize("held_by", [None, "ffffffffffffffffffffffffffffffff", TOKEN + "0", ""],
                         ids=["no lock key", "another writer", "a longer token", "an empty value"])
def test_a_token_that_does_not_hold_the_lock_commits_nothing(catalog, two_lakes, held_by):
    from supertable.errors import LockLostError
    client, lake = catalog.r, two_lakes[1]
    leaf_key, root_key, lock_key = _lake_keys(lake)
    client.set(leaf_key, VALUES_BEFORE["an entry this script wrote"])
    client.set(root_key, ROOTS_BEFORE["a root with flags"])
    if held_by is not None:
        client.set(lock_key, held_by)
    before = (client.get(leaf_key), client.get(root_key))

    with pytest.raises(LockLostError):
        catalog.commit_leaf_and_bump_root(ORG, lake, "t", _snapshot(2), PATH, now_ms=NOW,
                                          lock_token=TOKEN)

    assert (client.get(leaf_key), client.get(root_key)) == before
    assert client.get(lock_key) == held_by


def test_the_commit_is_one_round_trip_of_seven_commands(catalog, two_lakes):
    client, lake = catalog.r, two_lakes[1]
    keys = _lake_keys(lake)
    client.set(keys[2], TOKEN)
    catalog.commit_leaf_and_bump_root(ORG, lake, "t", _snapshot(50), PATH, now_ms=NOW, lock_token=TOKEN)

    made = _script_commands(client, keys, lambda: catalog.commit_leaf_and_bump_root(
        ORG, lake, "t", _snapshot(51), PATH, now_ms=NOW + 1, lock_token=TOKEN))

    # the lock; the root, read once before anything is written and once to
    # bump it; the entry's version from its first and last bytes; the two SETs
    assert made == {"get": 3, "getrange": 2, "set": 2}


def test_a_key_that_holds_no_string_fails_the_commit_with_nothing_written(catalog, two_lakes):
    client, lake = catalog.r, two_lakes[1]
    leaf_key, root_key, lock_key = _lake_keys(lake)
    client.set(leaf_key, VALUES_BEFORE["an entry this script wrote"])
    client.hset(root_key, "version", "3")          # a root that is no string
    client.set(lock_key, TOKEN)
    before = client.get(leaf_key)

    with pytest.raises(redis.ResponseError):
        catalog.commit_leaf_and_bump_root(ORG, lake, "t", _snapshot(2), PATH, now_ms=NOW,
                                          lock_token=TOKEN)

    assert client.get(leaf_key) == before, "the entry was not set before the root could not be read"


def test_writers_whose_lock_is_taken_away_lose_no_update(catalog):
    """Six writers take the table lock, read a counter in the entry and commit
    it plus one, while the lock key is deleted under them over and over, as
    an expiry would.  A writer whose lock went is refused by the script, so
    every commit that succeeded was made by the holder of the lock and built
    on the commit before it: the counter ends at the number of commits."""
    import random
    import threading
    import time

    from supertable.errors import LockLostError
    from supertable.redis_catalog import RedisCatalog

    lake = "fenced_race_" + uuid.uuid4().hex[:8]
    leaf_key, root_key, lock_key = _lake_keys(lake)
    client = catalog.r
    tally = {"committed": 0, "refused": 0}
    tally_lock, stop = threading.Lock(), threading.Event()
    errors = []

    def writer():
        own = RedisCatalog()
        try:
            for _ in range(40):
                token = own.acquire_simple_lock(ORG, lake, "t", ttl_s=30, timeout_s=30)
                assert token, "the lock could not be had in 30 seconds"
                try:
                    entry = own.get_leaf(ORG, lake, "t")
                    count = entry["payload"]["count"] if entry else 0
                    time.sleep(0.001)               # the write itself
                    try:
                        own.commit_leaf_and_bump_root(
                            ORG, lake, "t", {"resources": [], "count": count + 1}, PATH,
                            lock_token=token)
                    except LockLostError:
                        with tally_lock:
                            tally["refused"] += 1
                    else:
                        with tally_lock:
                            tally["committed"] += 1
                finally:
                    own.release_simple_lock(ORG, lake, "t", token)
        except Exception as error:   # noqa: BLE001 - reported by the test below
            errors.append(repr(error))

    def expiry():
        pause = random.Random(20261125)
        while not stop.is_set():
            client.delete(lock_key)
            time.sleep(pause.uniform(0.001, 0.008))     # some writes finish in time, some do not

    threads = [threading.Thread(target=writer) for _ in range(6)]
    thief = threading.Thread(target=expiry)
    try:
        thief.start()
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)
    finally:
        stop.set()
        thief.join(timeout=10)
    try:
        assert not errors, errors
        assert not any(thread.is_alive() for thread in threads)
        assert tally["committed"] + tally["refused"] == 240
        assert tally["refused"] > 0, "the lock was never taken away: nothing was put to the test"
        assert tally["committed"] > 0, "no write kept its lock long enough: nothing was put to the test"
        entry = catalog.get_leaf(ORG, lake, "t")
        assert entry["payload"]["count"] == tally["committed"], "an update was lost"
        assert entry["version"] == tally["committed"] - 1
        assert catalog.get_root(ORG, lake)["version"] == tally["committed"] - 1
    finally:
        client.delete(leaf_key, root_key, lock_key)


# ---------------------------------------------------------------------------
# The real writer
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def table():
    """A table taken through every kind of write, with what Redis and the
    snapshot file held after each."""
    from supertable.data_writer import DataWriter
    from supertable.redis_catalog import RedisCatalog
    from supertable.super_table import SuperTable

    lake = SuperTable(SUPER, ORG)
    writer = DataWriter(super_name=SUPER, organization=ORG)
    catalog = RedisCatalog()
    name = "ledger"
    seen = []

    def step(label, **write):
        writer.write(ROLE, name, **write)
        leaf = catalog.get_leaf(ORG, SUPER, name)
        seen.append((label, leaf, lake.storage.read_json(leaf["path"])))

    rows = lambda first, n=3: pa.table({"id": list(range(first, first + n)),          # noqa: E731
                                        "v": [f"r{i}" for i in range(first, first + n)]})
    step("the first write", data=rows(0), overwrite_columns=[])
    step("an append", data=rows(3), overwrite_columns=[])
    step("an upsert", data=rows(2, 2), overwrite_columns=["id"])
    step("a delete", data=pa.table({"id": [0]}), overwrite_columns=["id"], delete_only=True)
    step("a write with lineage", data=rows(10), overwrite_columns=[],
         lineage={"source_type": "api_upload", "source_files": [], "job_id": 1234567890123456789,
                  "tags": {}, "ratio": 0.1, "note": "árvíztűrő"})
    step("a delete of every row", data=pa.table({"id": [1]}), overwrite_columns=[], delete_only=True)
    step("a write into the emptied table", data=rows(20), overwrite_columns=[])
    writer.compact(ROLE, name)
    leaf = catalog.get_leaf(ORG, SUPER, name)
    seen.append(("a compaction", leaf, lake.storage.read_json(leaf["path"])))
    return seen


def test_after_every_kind_of_write_the_entry_is_the_snapshot_file(table):
    assert len(table) == 8
    for label, leaf, on_storage in table:
        assert _same(leaf["payload"], on_storage), label
        assert isinstance(leaf["payload"]["resources"], list), label


def test_the_lineage_in_the_entry_is_the_lineage_that_was_given(table):
    by_label = {label: leaf for label, leaf, _ in table}
    assert _same(by_label["an append"]["payload"]["lineage"]["overwrite_columns"], [])
    given = by_label["a write with lineage"]["payload"]["lineage"]
    assert _same(given, {"source_type": "api_upload", "source_files": [],
                         "job_id": 1234567890123456789, "tags": {}, "ratio": 0.1,
                         "note": "árvíztűrő"})


def test_the_version_went_up_by_one_with_every_commit(table):
    versions = [leaf["version"] for _label, leaf, _ in table]
    assert versions == list(range(versions[0], versions[0] + len(versions)))


def test_the_table_reads_back_what_those_writes_leave():
    from supertable.data_reader import DataReader, Status, engine

    frame, status, message = DataReader(
        super_name=SUPER, organization=ORG, query="SELECT id, v FROM ledger ORDER BY id",
    ).execute(role_name=ROLE, engine=engine.DUCKDB)
    assert status is Status.OK, message
    assert frame.to_dicts() == [{"id": i, "v": f"r{i}"} for i in (20, 21, 22)]


def test_the_real_writer_publishes_nothing_once_its_lock_is_another_writer_s(catalog, monkeypatch):
    """The whole of a write, on the real Redis: another writer takes the table
    lock just as the commit is sent, and the script refuses it.  With the
    commit in three steps that write was published over the other writer."""
    from supertable import redis_keys as RK
    from supertable.data_reader import DataReader, Status, engine
    from supertable.data_writer import DataWriter
    from supertable.errors import LockLostError

    name = "fenced"
    writer = DataWriter(super_name=SUPER, organization=ORG)
    rows = lambda first: pa.table({"id": list(range(first, first + 3)),         # noqa: E731
                                   "v": [f"r{i}" for i in range(first, first + 3)]})
    writer.write(ROLE, name, data=rows(0), overwrite_columns=[])
    before = (catalog.get_leaf(ORG, SUPER, name), catalog.get_root(ORG, SUPER))
    lock_key = RK.lock_leaf(ORG, SUPER, name)
    other = redis.Redis(**catalog.r.connection_pool.connection_kwargs)      # the other writer
    taken = []
    real = redis.Redis.execute_command

    def with_a_theft(self, *args, **options):
        if not taken and str(args[0]).upper() == "EVALSHA" and int(args[2]) == 3:
            taken.append(lock_key)
            other.set(lock_key, "another-writer")
        return real(self, *args, **options)

    monkeypatch.setattr(redis.Redis, "execute_command", with_a_theft)
    try:
        with pytest.raises(LockLostError):
            writer.write(ROLE, name, data=rows(3), overwrite_columns=[])
        monkeypatch.undo()

        assert taken == [lock_key], "the commit was reached, and the lock taken just before it"
        assert (catalog.get_leaf(ORG, SUPER, name), catalog.get_root(ORG, SUPER)) == before
        held = other.get(lock_key)
        assert (held.decode() if isinstance(held, bytes) else held) == "another-writer"

        other.delete(lock_key)
        writer.write(ROLE, name, data=rows(3), overwrite_columns=[])
        frame, status, message = DataReader(
            super_name=SUPER, organization=ORG, query=f"SELECT id, v FROM {name} ORDER BY id",
        ).execute(role_name=ROLE, engine=engine.DUCKDB)
        assert status is Status.OK, message
        assert frame.to_dicts() == [{"id": i, "v": f"r{i}"} for i in range(6)]
        assert catalog.get_leaf(ORG, SUPER, name)["version"] == before[0]["version"] + 1
    finally:
        other.delete(lock_key)
        other.close()
