"""
The catalog entry of a table holds the snapshot it was given, value for value.

A commit stores the table's snapshot in Redis beside its path, so that readers
need no storage read.  The script that stores it used to decode the entry that
was there, decode the new snapshot, and encode both again.  That blocked Redis
for 9 ms per 1,000 files on every commit, and what cjson encoded was not what
it had decoded: an empty list came out as ``{}``, an integer of 15 digits or
more as a rounded float, a float without its digits past the fourteenth.

The script now puts the entry together as text around the snapshot and reads
the old entry's version from its first bytes.  Pinned here, on a real
``RedisCatalog`` over fakeredis:

  * what a reader finds under ``payload`` is the snapshot, types included;
  * the version, the timestamp and the path are what the script as it was
    stored, whatever was in the key before: an entry of either form, one
    without a payload, one written by hand, a damaged one;
  * a snapshot holding a number that is not finite is refused with the error
    it was refused with, and the entry is left alone.

The script as it was is kept below, word for word, to compare against.
``test_suite/test_catalog_commit.py`` repeats the comparison on a real Redis,
where cjson is the real one.
"""
from __future__ import annotations

import copy
import json
import logging
from datetime import datetime
from types import SimpleNamespace

import fakeredis
import pytest
import redis

from supertable import redis_catalog
from supertable import redis_keys as RK

ORG, LAKE, TABLE = "acme", "sales", "orders"
PATH = "acme/sales/tables/orders/snapshots/1795000000001_ab12_orders.json"
NOW = 1_795_000_000_001

# The script at a6f88ae, unchanged.
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


@pytest.fixture
def client():
    return fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)


@pytest.fixture
def catalog(client, monkeypatch):
    monkeypatch.setattr(
        redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=client))
    return redis_catalog.RedisCatalog()


def _key(table: str = TABLE) -> str:
    return RK.meta_leaf(ORG, LAKE, table)


def _snapshot(files: int = 3, **more) -> dict:
    doc = {
        "simple_name": TABLE,
        "location": "acme/sales/tables/orders",
        "snapshot_version": 7,
        "last_updated_ms": 1_795_000_000_000,
        "previous_snapshot": "acme/sales/tables/orders/snapshots/prev.json",
        "schema": {"zeta": "Int64", "alpha": "String", "mid": "Float64", "__rowid__": "Int64",
                   "__timestamp__": "Datetime(time_unit='us', time_zone='UTC')"},
        "schemaString": json.dumps({"type": "struct", "fields": {"zeta": "Int64"}},
                                   separators=(",", ":")),
        "resources": [
            {"file": f"acme/sales/tables/orders/data/year=2026/month=11/day=25/data_{i:04d}.parquet",
             "file_size": 12_582_912 + i, "rows": 1_100_000 + i, "columns": 5}
            for i in range(files)
        ],
        "tombstone": None,
        "tombstone_rows": 0,
        "stats_file": "acme/sales/tables/orders/stats/s.parquet",
        "stats_rows": files * 5,
        "lineage": {"source_type": "write", "role_name": "writer", "overwrite_columns": [],
                    "delete_only": False, "incoming_rows": 100, "write_id": "0b7c9d2e"},
        "rowid_watermark": 3_300_000,
    }
    doc.update(more)
    return doc


NEW_TABLE = {"simple_name": TABLE, "location": "acme/sales/tables/orders", "snapshot_version": 0,
             "last_updated_ms": 1_795_000_000_000, "previous_snapshot": None, "schema": [],
             "resources": [], "tombstone": None, "tombstone_rows": 0, "stats_file": None,
             "stats_rows": 0}

SNAPSHOTS = {
    "the empty snapshot of a new table": NEW_TABLE,
    "an append": _snapshot(3),
    "a table of 300 files": _snapshot(300),
    "a deletion vector of two parts": _snapshot(
        2, tombstone=["t/a.parquet", "t/b.parquet"], tombstone_rows=9),
    "an upsert's lineage": _snapshot(1, lineage={
        "source_type": "write", "overwrite_columns": ["id", "day"], "tags": {"team": "ingest"},
        "source_files": ["a.csv"]}),
    "text outside ASCII": _snapshot(1, lineage={"note": "árvíztűrő 日本語 🎉", "key ű": "ø"}),
    "text that needs escaping": _snapshot(1, lineage={
        "q": 'say "hi"', "b": "a\\b", "n": "a\nb", "t": "a\tb", "c": "\u0001", "s": "a/b",
        "u": " ", "tag": "</script>"}),
    "integers of 15 to 19 digits": _snapshot(1, lineage={
        "job_id": 123456789012345, "run_id": 1234567890123456789, "small": 99999999999999}),
    "floats": _snapshot(1, lineage={
        "half": 0.5, "whole": 2.0, "tiny": 1e-300, "pi": 3.141592653589793, "neg": -0.0,
        "big": 1e22}),
    "booleans and nulls": _snapshot(1, lineage={"yes": True, "no": False, "nothing": None}),
    "empty containers, nested": _snapshot(1, lineage={
        "a": {}, "b": [], "c": [[], {}], "d": {"e": []}}),
    "the row filter of a linked share": _snapshot(1, _row_filter="region = 'eu'"),
    "a schema recorded as a list of fields": _snapshot(
        1, schema=[{"name": "id", "type": "long"}, {"name": "v", "type": "string"}]),
}


def _same(a, b) -> bool:
    """Equal, types included: ``2 == 2.0`` and ``{} == []`` do not pass."""
    if type(a) is not type(b):
        return False
    if isinstance(a, dict):
        return list(a) == list(b) and all(_same(a[k], b[k]) for k in a)
    if isinstance(a, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b))
    if isinstance(a, float):
        return repr(a) == repr(b)            # -0.0 is not 0.0
    return a == b


# ---------------------------------------------------------------------------
# What is stored
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", sorted(SNAPSHOTS))
class TestWhatAReaderFinds:

    def test_the_payload_is_the_snapshot_types_and_key_order_included(self, catalog, name):
        snapshot = SNAPSHOTS[name]
        given = copy.deepcopy(snapshot)

        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, snapshot, PATH, now_ms=NOW)

        leaf = catalog.get_leaf(ORG, LAKE, TABLE)
        assert _same(leaf["payload"], given)
        assert _same(snapshot, given), "and the caller's snapshot is not touched"

    def test_the_entry_is_version_timestamp_path_and_payload(self, catalog, client, name):
        version = catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, SNAPSHOTS[name], PATH, now_ms=NOW)

        stored = client.get(_key())
        assert version == 0
        assert list(json.loads(stored)) == ["version", "ts", "path", "payload"]
        leaf = json.loads(stored)
        assert (leaf["version"], leaf["ts"], leaf["path"]) == (0, NOW, PATH)
        assert type(leaf["version"]) is int and type(leaf["ts"]) is int
        assert stored.startswith('{"version":0,"ts":%d,"path":"' % NOW)
        assert stored.isascii()

    def test_every_reader_of_leaves_decodes_it_the_same(self, catalog, name):
        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, SNAPSHOTS[name], PATH, now_ms=NOW)

        by_name = catalog.get_leaf_items(ORG, LAKE, [TABLE])
        by_scan = list(catalog.scan_leaf_items(ORG, LAKE))
        assert by_name == by_scan == [{
            "simple": TABLE, "version": 0, "ts": NOW, "path": PATH, "payload": SNAPSHOTS[name]}]
        assert _same(by_name[0]["payload"], SNAPSHOTS[name])


class TestTheTextThatIsStored:

    def test_the_snapshot_is_sent_once_compact_and_in_ascii(self, catalog):
        sent = []
        script = catalog._leaf_payload_cas_set
        catalog._leaf_payload_cas_set = lambda keys, args: (sent.append((keys, args)),
                                                            script(keys=keys, args=args))[1]
        snapshot = SNAPSHOTS["text outside ASCII"]

        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, snapshot, PATH, now_ms=NOW)

        (keys, args), = sent
        assert keys == [_key()]
        assert args[1:] == [PATH, NOW]
        assert args[0] == json.dumps(snapshot, separators=(",", ":"))
        assert args[0].isascii() and "\n" not in args[0]

    def test_the_stored_entry_holds_that_text_unchanged(self, catalog, client):
        snapshot = SNAPSHOTS["empty containers, nested"]

        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, snapshot, PATH, now_ms=NOW)

        text = json.dumps(snapshot, separators=(",", ":"))
        assert client.get(_key()).endswith(',"payload":' + text + "}")

    def test_no_payload_is_stored_as_an_empty_one_as_before(self, catalog, client):
        for payload in (None, {}):
            client.delete(_key())
            catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, payload, PATH, now_ms=NOW)
            assert json.loads(client.get(_key())) == {
                "version": 0, "ts": NOW, "path": PATH, "payload": {}}

    def test_a_clock_value_of_nothing_is_now_as_before(self, catalog, client):
        before = int(datetime.now().timestamp() * 1000)
        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(1), PATH)
        catalog.set_leaf_payload_cas(ORG, LAKE, "other", _snapshot(1), PATH, now_ms=0)
        for table in (TABLE, "other"):
            assert before <= json.loads(client.get(_key(table)))["ts"] <= before + 60_000


# ---------------------------------------------------------------------------
# The version, whatever was in the key
# ---------------------------------------------------------------------------

AS_THE_OLD_SCRIPT_WROTE = (
    '{"path":"x\\/y.json","ts":1794000000000,"version":41,"payload":{"resources":{}}}')
AS_THIS_SCRIPT_WRITES = (
    '{"version":41,"ts":1794000000000,"path":"x\\/y.json","payload":{"resources":[]}}')

VALUES_BEFORE = {
    "no entry": None,
    "an entry the old script wrote": AS_THE_OLD_SCRIPT_WROTE,
    "an entry this script wrote": AS_THIS_SCRIPT_WRITES,
    "an entry without payload": '{"version":12,"ts":1794000000000,"path":"p.json"}',
    "a version of 0": '{"version":0,"ts":1,"path":"p"}',
    "a version alone": '{"version":7}',
    "a version of 13 digits": '{"version":9999999999999,"path":"p"}',
    "spaces, as a person would write it": '{ "version": 5, "path": "p.json" }',
    "a version that is a fraction": '{"version":3.5,"path":"p.json"}',
    "a version that is negative": '{"version":-4,"path":"p.json"}',
    "a version that is text": '{"version":"seven","path":"p.json"}',
    "a version that is numeric text": '{"version":"7","path":"p.json"}',
    "a version that is null": '{"version":null,"path":"p.json"}',
    "no version": '{"path":"p.json","ts":5}',
    "a payload that mentions a version first": '{"payload":{"version":99},"version":6,"path":"p"}',
    "a version inside a payload only": '{"payload":{"version":99},"path":"p"}',
    "not JSON": "not json at all",
    "a leaf cut off after its version": '{"version":8,"ts":17',
    "an empty value": "",
    "a JSON list": "[1,2,3]",
    "a JSON number": "5",
    "JSON null": "null",
    "a JSON string": '"version"',
    "a JSON list that looks like a leaf": '[{"version":3}]',
}

# A snapshot that fakeredis's stand-in for cjson gives back unchanged, so that
# the two scripts can be compared on everything else.
PLAIN = {"snapshot_version": 3, "resources": [{"file": "a/b.parquet", "file_size": 10, "rows": 2,
                                               "columns": 3}],
         "schema": {"id": "Int64"}, "tombstone": None, "stats_file": "a/s.parquet",
         "lineage": {"source_type": "write", "overwrite_columns": ["id"]}}


def _commit_with(script, client, key: str, snapshot: dict, path: str = PATH, now_ms: int = NOW):
    """What a commit returned, or that it raised, and what it left in the key.

    Which error fakeredis raises for a Lua error is its own affair (a real
    Redis answers every one of them with ``ResponseError``), so only the fact
    is kept.
    """
    try:
        outcome = ("returned", int(script(keys=[key], args=[
            json.dumps(snapshot, separators=(",", ":")), path, now_ms])))
    except Exception:   # noqa: BLE001 - see above
        outcome = ("raised",)
    raw = client.get(key)
    try:
        left = json.loads(raw) if raw else raw
    except ValueError:
        left = raw
    return outcome, left


@pytest.mark.parametrize("name", sorted(VALUES_BEFORE))
class TestTheVersionWhateverWasThere:

    def test_it_is_what_the_script_as_it_was_gave(self, catalog, client, name):
        as_it_was = client.register_script(_SCRIPT_AS_IT_WAS)
        before = VALUES_BEFORE[name]
        for key in ("was", "now"):
            if before is not None:
                client.set(key, before)

        was = _commit_with(as_it_was, client, "was", PLAIN)
        now = _commit_with(catalog._leaf_payload_cas_set, client, "now", PLAIN)

        assert now == was

    def test_and_so_is_the_commit_after_it(self, catalog, client, name):
        """The second commit meets what the first one left: this script's own
        entry on one side, the old script's on the other."""
        as_it_was = client.register_script(_SCRIPT_AS_IT_WAS)
        before = VALUES_BEFORE[name]
        for key in ("was", "now"):
            if before is not None:
                client.set(key, before)
        _commit_with(as_it_was, client, "was", PLAIN)
        _commit_with(catalog._leaf_payload_cas_set, client, "now", PLAIN)

        was = _commit_with(as_it_was, client, "was", PLAIN, path="next.json", now_ms=NOW + 1)
        now = _commit_with(catalog._leaf_payload_cas_set, client, "now", PLAIN,
                           path="next.json", now_ms=NOW + 1)

        assert now == was

    def test_the_scripts_take_over_from_each_other(self, catalog, client, name):
        """A release with the old script and one with this script writing the
        same table, in either order."""
        as_it_was = client.register_script(_SCRIPT_AS_IT_WAS)
        before = VALUES_BEFORE[name]
        for key in ("was", "old then new", "new then old"):
            if before is not None:
                client.set(key, before)
        new = catalog._leaf_payload_cas_set
        _commit_with(as_it_was, client, "was", PLAIN)
        was = _commit_with(as_it_was, client, "was", PLAIN, path="next.json", now_ms=NOW + 1)

        _commit_with(as_it_was, client, "old then new", PLAIN)
        assert _commit_with(new, client, "old then new", PLAIN, path="next.json",
                            now_ms=NOW + 1) == was
        _commit_with(new, client, "new then old", PLAIN)
        assert _commit_with(as_it_was, client, "new then old", PLAIN, path="next.json",
                            now_ms=NOW + 1) == was


# What the method never sends but the script could be handed: the script as it
# was is the measure.  A clock value too large for 14 digits is compared on a
# real Redis only (test_suite/test_catalog_commit.py): fakeredis's stand-in
# for cjson writes such a number its own way.
ARGUMENTS = {
    "a path with a quote": ('{"a":1}', 'p/"quoted".json', NOW),
    "a path with a backslash": ('{"a":1}', "p\\q.json", NOW),
    "a path with a line break and a tab": ('{"a":1}', "p\nq\t.json", NOW),
    "a path outside ASCII": ('{"a":1}', "árvíz/日本.json", NOW),
    "an empty path": ('{"a":1}', "", NOW),
    "a clock value with a fraction": ('{"a":1}', PATH, "1795000000001.5"),
    "a clock value of zero": ('{"a":1}', PATH, 0),
    "a negative clock value": ('{"a":1}', PATH, -5),
    "a clock value that is no number": ('{"a":1}', PATH, "soon"),
    "an empty clock value": ('{"a":1}', PATH, ""),
}


def _call_with(script, client, key: str, arguments):
    try:
        outcome = ("returned", int(script(keys=[key], args=list(arguments))))
    except Exception:   # noqa: BLE001 - see _commit_with
        outcome = ("raised",)
    raw = client.get(key)
    try:
        left = json.loads(raw) if raw else raw
    except ValueError:
        left = ("NOT JSON", raw)
    return outcome, left


@pytest.mark.parametrize("name", sorted(ARGUMENTS))
class TestTheArgumentsOfTheScript:

    def test_path_clock_and_payload_text_are_stored_as_they_were(self, catalog, client, name):
        as_it_was = client.register_script(_SCRIPT_AS_IT_WAS)

        was = _call_with(as_it_was, client, "was", ARGUMENTS[name])
        now = _call_with(catalog._leaf_payload_cas_set, client, "now", ARGUMENTS[name])

        assert was[0] == ("returned", 0)
        assert isinstance(was[1], dict), "the script as it was stored JSON"
        assert now == was

    def test_and_so_is_the_commit_over_them(self, catalog, client, name):
        as_it_was = client.register_script(_SCRIPT_AS_IT_WAS)
        _call_with(as_it_was, client, "was", ARGUMENTS[name])
        _call_with(catalog._leaf_payload_cas_set, client, "now", ARGUMENTS[name])

        was = _commit_with(as_it_was, client, "was", PLAIN, path="next.json", now_ms=NOW + 1)
        now = _commit_with(catalog._leaf_payload_cas_set, client, "now", PLAIN,
                           path="next.json", now_ms=NOW + 1)

        assert now == was == (("returned", 1), {"version": 1, "ts": NOW + 1, "path": "next.json",
                                                "payload": PLAIN})


def test_an_empty_payload_text_is_stored_as_an_empty_object(catalog, client):
    """The script as it was could not decode it and stored an empty table,
    which the real cjson writes as ``{}`` (fakeredis's stand-in writes ``[]``,
    so the two are compared on a real Redis only)."""
    outcome, left = _call_with(catalog._leaf_payload_cas_set, client, "now", ("", PATH, NOW))

    assert outcome == ("returned", 0)
    assert left == {"version": 0, "ts": NOW, "path": PATH, "payload": {}}
    assert type(left["payload"]) is dict


class TestWhereTheTwoScriptsDiffer:
    """Entries no writer produces.  One that begins with its version and ends
    as an object does is not decoded, so the rest of it no longer decides the
    next version.  ``test_suite/test_catalog_commit.py`` shows the same on a
    real Redis, with the cases only the real cjson has."""

    def test_an_entry_damaged_in_the_middle_keeps_its_version(self, catalog, client):
        """Not JSON, yet it begins with its version and ends as an object
        does.  The old script could not parse it and started the table's
        versions again from 0; this one reads the 8 at its head and stores 9.
        No writer produces such an entry: it takes a hand in Redis."""
        damaged = '{"version":8,"ts":1,"path":"p",,,}'
        as_it_was = client.register_script(_SCRIPT_AS_IT_WAS)
        for key in ("was", "now"):
            client.set(key, damaged)

        was = _commit_with(as_it_was, client, "was", PLAIN)
        now = _commit_with(catalog._leaf_payload_cas_set, client, "now", PLAIN)

        assert was[0] == ("returned", 0) and now[0] == ("returned", 9)
        assert was[1]["payload"] == now[1]["payload"] == PLAIN
        assert (now[1]["ts"], now[1]["path"]) == (was[1]["ts"], was[1]["path"]) == (NOW, PATH)

    def test_an_entry_naming_its_version_twice_goes_on_from_the_first(self, catalog, client):
        """JSON lets a name stand twice, and a decoder keeps the later value:
        the old script went on from 20.  This one reads the 8 at the head."""
        twice = '{"version":8,"path":"p","version":20}'
        as_it_was = client.register_script(_SCRIPT_AS_IT_WAS)
        for key in ("was", "now"):
            client.set(key, twice)

        was = _commit_with(as_it_was, client, "was", PLAIN)
        now = _commit_with(catalog._leaf_payload_cas_set, client, "now", PLAIN)

        assert was[0] == ("returned", 21) and now[0] == ("returned", 9)
        assert now[1] == {"version": 9, "ts": NOW, "path": PATH, "payload": PLAIN}

    def test_one_that_is_cut_off_does_not(self, catalog, client):
        """Its last byte is not a closing brace, so it is decoded whole, fails
        as it always did, and the versions start again."""
        client.set(_key(), '{"version":8,"ts":17')
        assert catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, PLAIN, PATH, now_ms=NOW) == 0


class TestTheVersionOverManyCommits:

    def test_it_counts_up_by_one(self, catalog, client):
        versions = [catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(i % 4), PATH, now_ms=NOW + i)
                    for i in range(25)]
        assert versions == list(range(25))
        assert catalog.get_leaf(ORG, LAKE, TABLE)["version"] == 24

    def test_through_the_commit_without_payload_and_back(self, catalog):
        """The writers' fallback stores a leaf without payload; the next commit
        with one carries on from its version."""
        assert catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(1), PATH, now_ms=NOW) == 0
        assert catalog.set_leaf_path_cas(ORG, LAKE, TABLE, "p2.json", now_ms=NOW + 1) == 1
        assert "payload" not in catalog.get_leaf(ORG, LAKE, TABLE)
        assert catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(2), "p3.json", now_ms=NOW + 2) == 2
        assert catalog.set_leaf_path_cas(ORG, LAKE, TABLE, "p4.json", now_ms=NOW + 3) == 3
        leaf = catalog.get_leaf(ORG, LAKE, TABLE)
        assert leaf == {"version": 3, "ts": NOW + 3, "path": "p4.json"}

    def test_tables_do_not_share_a_version(self, catalog):
        for _ in range(3):
            catalog.set_leaf_payload_cas(ORG, LAKE, "a", _snapshot(1), PATH, now_ms=NOW)
        assert catalog.set_leaf_payload_cas(ORG, LAKE, "b", _snapshot(1), PATH, now_ms=NOW) == 0
        assert catalog.get_leaf(ORG, LAKE, "a")["version"] == 2

    def test_a_key_that_holds_no_string_is_refused_as_before(self, catalog, client):
        client.hset(_key(), "a", "b")
        with pytest.raises(redis.ResponseError):
            catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(1), PATH, now_ms=NOW)
        assert client.hgetall(_key()) == {"a": "b"}


# ---------------------------------------------------------------------------
# What is refused
# ---------------------------------------------------------------------------

NOT_FINITE = {
    "NaN in the lineage": _snapshot(1, lineage={"ratio": float("nan")}),
    "infinity": _snapshot(1, lineage={"ratio": float("inf")}),
    "minus infinity, nested": _snapshot(1, lineage={"a": [{"b": float("-inf")}]}),
    "NaN at the top": {"x": float("nan")},
}


@pytest.mark.parametrize("name", sorted(NOT_FINITE))
class TestANumberThatIsNotFinite:

    def test_it_is_refused_with_the_error_it_was_refused_with(self, catalog, name, caplog):
        with caplog.at_level(logging.ERROR, logger="supertable.config.defaults"):
            with pytest.raises(redis.ResponseError):
                catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, NOT_FINITE[name], PATH, now_ms=NOW)
        assert any("leaf_payload_cas_set error" in r.getMessage() for r in caplog.records)

    def test_there_is_still_no_entry_when_there_was_none(self, catalog, client, name):
        with pytest.raises(redis.ResponseError):
            catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, NOT_FINITE[name], PATH, now_ms=NOW)
        assert client.get(_key()) is None

    def test_the_entry_that_was_there_is_left_as_it_was(self, catalog, client, name):
        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(2), PATH, now_ms=NOW)
        before = client.get(_key())

        with pytest.raises(redis.ResponseError):
            catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, NOT_FINITE[name], "p2.json", now_ms=NOW + 1)

        assert client.get(_key()) == before

    def test_the_commit_without_payload_then_stores_the_path(self, catalog, name):
        """What both writers do when the commit with payload raises."""
        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(2), PATH, now_ms=NOW)
        with pytest.raises(redis.ResponseError):
            catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, NOT_FINITE[name], "p2.json", now_ms=NOW + 1)

        assert catalog.set_leaf_path_cas(ORG, LAKE, TABLE, "p2.json", now_ms=NOW + 1) == 1
        assert catalog.get_leaf(ORG, LAKE, TABLE) == {"version": 1, "ts": NOW + 1, "path": "p2.json"}


class TestWhatCannotBeSerialisedAtAll:

    @pytest.mark.parametrize("value", [datetime(2026, 1, 1), {1, 2}, object(), b"bytes"])
    def test_it_is_stored_as_an_empty_payload_as_before(self, catalog, value):
        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(1, lineage={"x": value}), PATH, now_ms=NOW)
        assert catalog.get_leaf(ORG, LAKE, TABLE) == {
            "version": 0, "ts": NOW, "path": PATH, "payload": {}}

    def test_a_snapshot_that_contains_itself_too(self, catalog):
        snapshot = _snapshot(1)
        snapshot["lineage"]["self"] = snapshot
        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, snapshot, PATH, now_ms=NOW)
        assert catalog.get_leaf(ORG, LAKE, TABLE)["payload"] == {}

    def test_and_so_is_one_that_also_holds_a_number_that_is_not_finite(self, catalog):
        """What cannot be serialised wins over what is refused, as it did: the
        script was never reached with such a snapshot."""
        snapshot = _snapshot(1, lineage={"ratio": float("nan"), "when": datetime(2026, 1, 1)})
        catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, snapshot, PATH, now_ms=NOW)
        assert catalog.get_leaf(ORG, LAKE, TABLE)["payload"] == {}


class TestWithoutTheScript:

    def test_the_path_is_stored_without_payload_as_before(self, catalog):
        del catalog._leaf_payload_cas_set
        assert catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(1), PATH, now_ms=NOW) == 0
        assert catalog.get_leaf(ORG, LAKE, TABLE) == {"version": 0, "ts": NOW, "path": PATH}

    def test_an_error_of_redis_is_raised_as_before(self, catalog):
        def failing(keys, args):
            raise redis.ConnectionError("gone")
        catalog._leaf_payload_cas_set = failing
        with pytest.raises(redis.ConnectionError):
            catalog.set_leaf_payload_cas(ORG, LAKE, TABLE, _snapshot(1), PATH, now_ms=NOW)
