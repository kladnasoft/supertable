"""
A query asks the catalog about its tables in two round trips.

A query asked the catalog one thing at a time, a round trip each.  Before the
role was checked: is the lake there (``root_exists``), and for each table, is
it there (``leaf_exists``: the root again, for the replica the lake may be,
then the leaf).  After it: what the leaves hold (``get_leaf_items``: the root
a third time, then the leaves), with one more ``root_exists`` for each table
when its lake object was built.  Six round trips for one table, four more for
every further one.

The same commands are now sent in two transactions, each where its commands
were sent before.  ``RedisCatalog.root_and_leaves_exist`` asks, before the
role is checked, EXISTS and GET of the root and EXISTS of each leaf;
``DataReader._assert_targets_exist`` decides on it what it decided on the
separate calls.  ``RedisCatalog.get_root_and_leaf_items`` asks, after it,
EXISTS and GET of the root and GET of each leaf; ``DataEstimator`` takes its
leaves from it and tells each lake object that its root is there.  The
separate calls are still there, and are what is made whenever one of those
reads is not to be had: a catalog or a lake object that is not the library's
own, a call replaced on either, a name no key can be built from, a failure of
the read.

The comparison here is with the calls as they stand and as they were.  For
every state a root and a leaf can be in, each read answers what the calls it
stands for answer; the reader's check, kept in this file word for word as it
was, raises or passes as the new one does; and an estimate that read its lake
in one transaction hands on what an estimate that made the separate calls
hands on.  Two fake Redis servers are given the same content, one way runs on
each.

``tests/characterization/test_the_round_trips_of_a_query_to_redis.py`` runs the
real reader and pins its round trips, and ``test_suite/test_read_tables_once.py``
repeats the comparison on a real Redis.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import redis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import data_reader as data_reader_mod  # noqa: E402
from supertable import redis_catalog  # noqa: E402
from supertable import redis_keys as RK  # noqa: E402
from supertable import super_table as super_table_mod  # noqa: E402
from supertable.config.settings import settings  # noqa: E402
from supertable.data_classes import TableDefinition  # noqa: E402
from supertable.data_reader import DataReader  # noqa: E402
from supertable.engine.data_estimator import DataEstimator  # noqa: E402
from supertable.errors import SuperTableNotFoundError, TableNotFoundError  # noqa: E402
from supertable.redis_catalog import RedisCatalog  # noqa: E402
from supertable.storage.local_storage import LocalStorage  # noqa: E402
from supertable.super_table import SuperTable  # noqa: E402

# ``supertable.engine`` is shadowed by the ``Engine`` enum at the package
# level; the module is fetched by name.
data_estimator_mod = sys.modules["supertable.engine.data_estimator"]

ORG = "acme"
LAKE, SOURCE, MIRROR = "sales", "archive", "mirror"
LOGGER = redis_catalog.logger.name
ROOT = RK.meta_root(ORG, LAKE)
#: The one line the new check may log that the old did not: Redis refused or
#: failed the one read, and the separate calls followed.
READ_FAILED = "[redis-catalog] root_and_leaves_exist error: "


def _leaf_key(simple: str, lake: str = LAKE) -> str:
    return RK.meta_leaf(ORG, lake, simple)


# ---------------------------------------------------------------------------
# A fake Redis, and every round trip made to it
# ---------------------------------------------------------------------------

class _Redis:
    """A fake Redis of its own.  A command sent by itself is recorded as
    ``(name, key)``, a transaction as ``("MULTI", ((name, key), ...))``.

    ``fail``: called with each round trip before it is sent; a true answer
    times it out instead.
    """

    def __init__(self):
        server = fakeredis.FakeServer()
        self.client = fakeredis.FakeRedis(server=server, decode_responses=True)
        self.bytes = fakeredis.FakeRedis(server=server)      # this test's own look at it
        self.trips = []
        self.fail = None
        real_command, real_pipeline = self.client.execute_command, self.client.pipeline

        def sent(trip):
            self.trips.append(trip)
            if self.fail is not None and self.fail(trip):
                raise redis.exceptions.TimeoutError("Timeout reading from socket")

        def command(*args, **options):
            sent((str(args[0]).upper(), args[1] if len(args) > 1 else None))
            return real_command(*args, **options)

        def pipeline(*args, **options):
            opened = real_pipeline(*args, **options)
            real_execute = opened.execute

            def execute(*a, **k):
                sent(("MULTI", tuple((str(c[0][0]).upper(), c[0][1] if len(c[0]) > 1 else None)
                                     for c in opened.command_stack)))
                return real_execute(*a, **k)

            opened.execute = execute
            return opened

        self.client.execute_command = command
        self.client.pipeline = pipeline

    def use(self, patched) -> None:
        """Every catalog built from here on talks to this Redis."""
        patched.setattr(redis_catalog, "RedisConnector",
                        lambda options=None: SimpleNamespace(r=self.client))

    def catalog(self) -> RedisCatalog:
        with pytest.MonkeyPatch.context() as patched:
            self.use(patched)
            return RedisCatalog()

    def load(self, content: dict) -> "_Redis":
        self.bytes.flushall()
        for key, value in content.items():
            if isinstance(value, dict):
                self.bytes.hset(key, mapping=value)
            elif isinstance(value, (set, frozenset)):
                self.bytes.sadd(key, *value)
            else:
                self.bytes.set(key, value)
        self.trips.clear()
        return self

    def dump(self) -> dict:
        out = {}
        for key in sorted(self.bytes.keys("*")):
            kind = self.bytes.type(key)
            if kind == b"string":
                out[key] = self.bytes.get(key)
            elif kind == b"hash":
                out[key] = ("hash", sorted(self.bytes.hgetall(key).items()))
            else:
                out[key] = ("set", sorted(self.bytes.smembers(key)))
        return out


def _names(trips) -> list:
    """Round trips by name: ``"EXISTS"`` for a command, ``"MULTI of 4"`` for a transaction."""
    return [f"MULTI of {len(key)}" if name == "MULTI" else name for name, key in trips]


# ---------------------------------------------------------------------------
# What a lake can hold
# ---------------------------------------------------------------------------

def _snapshot(version: int, files, columns=("id", "val"), **extra) -> dict:
    payload = {
        "snapshot_version": version,
        "schema": [{"name": c, "type": "BIGINT" if c == "id" else "VARCHAR"} for c in columns],
        "resources": [{"file": f, "file_size": 100 + i} for i, f in enumerate(files)],
    }
    payload.update(extra)
    return payload


def _leaf(payload=None, *, path="snap.json", version=1, ts=1_700_000_000_000) -> str:
    doc = {"version": version, "ts": ts, "path": path}
    if payload is not None:
        doc["payload"] = payload
    return json.dumps(doc)


#: The leaves of the lake, good and bad, by table name.
LEAVES = {
    "orders": _leaf(_snapshot(3, ["o1.parquet", "o2.parquet"], tombstone=["dv-0.parquet"]), version=3),
    "items": _leaf(_snapshot(2, ["i1.parquet"], columns=("id", "sku")), version=2, ts=1_700_000_500_000),
    "elsewhere": _leaf(None, path="acme/sales/elsewhere/snap-5.json", version=5),
    "pathless": _leaf(_snapshot(1, ["p.parquet"]), path=""),
    "blank": "",
    "broken": "{not json",
    "listed": "[1, 2]",
    "unversioned": json.dumps({"path": "u.json", "payload": _snapshot(1, ["u.parquet"])}),
    "oddversion": json.dumps({"version": "x", "ts": 1, "path": "v.json"}),
    "emptytable": _leaf(_snapshot(4, []), version=4),
}
#: Read from storage for the leaf that holds a path only.
ON_DISK = {"acme/sales/elsewhere/snap-5.json": _snapshot(5, ["e1.parquet", "e2.parquet", "e3.parquet"])}
#: The leaves of the lake a replica is a replica of.
SOURCE_LEAVES = {
    "orders": _leaf(_snapshot(9, ["s-o1.parquet"]), version=9),
    "extras": _leaf(_snapshot(1, ["s-x1.parquet"]), version=1),
    "broken": "{not json either",
}
A_ROOT = '{"version": 3, "ts": 1794000000000}'


def _replica_of(source, **more) -> str:
    return json.dumps({"version": 1, "ts": 1, "clone_type": "replica", "cloned_from": source, **more})


#: Each: the text at the lake's root key (``None``: no key), and whatever else it needs.
ROOTS = {
    "a root": {ROOT: A_ROOT},
    "no root": {},
    "a root of empty text": {ROOT: ""},
    "a root that is not JSON": {ROOT: "{not json"},
    "a root that is a list": {ROOT: "[1, 2]"},
    "a root that is a number": {ROOT: "7"},
    "a root that is text": {ROOT: '"replica"'},
    "a locked root": {ROOT: '{"version": 1, "read_only": true, "clone_type": "readonly", "cloned_from": "archive"}'},
    "a replica of another lake": {ROOT: _replica_of(SOURCE)},
    "a replica given two tables": {ROOT: _replica_of(SOURCE, replica_tables=["orders", "items"])},
    "a replica given a table its source has not": {ROOT: _replica_of(SOURCE, replica_tables=["items"])},
    "a replica with an empty list of tables": {ROOT: _replica_of(SOURCE, replica_tables=[])},
    "a replica whose tables are no list": {ROOT: _replica_of(SOURCE, replica_tables="orders")},
    "a replica of itself": {ROOT: _replica_of(LAKE)},
    "a replica of nothing": {ROOT: _replica_of("")},
    "a replica of a name no key is built from": {ROOT: _replica_of("No Such Lake!")},
    "a replica of a replica": {ROOT: _replica_of(MIRROR), RK.meta_root(ORG, MIRROR): _replica_of(SOURCE),
                               RK.meta_leaf(ORG, MIRROR, "orders"): _leaf(_snapshot(7, ["m-o1.parquet"]), version=7)},
    "a replica of a lake that is not there": {ROOT: _replica_of("gone")},
}

#: The table names a query can ask for.
ASKED = {
    "one table": ["orders"],
    "two tables": ["orders", "items"],
    "a table that is not there": ["nothere"],
    "one there, one not, one there": ["orders", "nothere", "items"],
    "one table named twice": ["orders", "orders"],
    "no table": [],
    "a leaf that holds a path only": ["elsewhere"],
    "a leaf without a path": ["pathless"],
    "a leaf of empty text": ["blank"],
    "a leaf that is not JSON": ["broken"],
    "a leaf that is a list": ["listed"],
    "a leaf without a version": ["unversioned"],
    "a leaf whose version is no number": ["oddversion"],
    "every kind of leaf": list(LEAVES),
    "a table only the source has": ["extras"],
    "a name in capitals": ["Orders"],
    "a name no key is built from": ["or ders"],
    "a good name and one no key is built from": ["orders", "No!"],
}


def _content(root: str = "a root") -> dict:
    content = {_leaf_key(name): text for name, text in LEAVES.items()}
    content.update({_leaf_key(name, SOURCE): text for name, text in SOURCE_LEAVES.items()})
    content[RK.meta_root(ORG, SOURCE)] = A_ROOT
    content.update(ROOTS[root])
    return content


# ---------------------------------------------------------------------------
# The two reads, beside the calls they stand for
# ---------------------------------------------------------------------------

def _outcome(do):
    try:
        return ("returned", do())
    except Exception as error:   # noqa: BLE001 - whatever it raises is the outcome
        return ("raised", type(error).__name__, str(error))


def _existence_call_by_call(catalog: RedisCatalog, names, lake: str = LAKE):
    """What the reader's check asked, one call after another."""
    return (catalog.root_exists(ORG, lake),
            {name: catalog.leaf_exists(ORG, lake, name) for name in dict.fromkeys(names)})


def _leaves_call_by_call(catalog: RedisCatalog, names, lake: str = LAKE):
    """What the estimate asked: the leaves, and the root for each lake object."""
    items = catalog.get_leaf_items(ORG, lake, names)
    return (catalog.root_exists(ORG, lake), items)


@pytest.mark.parametrize("asked", list(ASKED))
@pytest.mark.parametrize("root", list(ROOTS))
def test_the_check_s_read_answers_what_root_exists_and_leaf_exists_answer(root, asked):
    side = _Redis().load(_content(root))
    catalog, names = side.catalog(), ASKED[asked]
    before = side.dump()

    at_once = _outcome(lambda: catalog.root_and_leaves_exist(ORG, LAKE, names))
    one_by_one = _outcome(lambda: _existence_call_by_call(catalog, names))

    assert at_once == one_by_one
    if at_once[0] == "returned":
        assert type(at_once[1]) is tuple and type(at_once[1][0]) is bool
        assert list(at_once[1][1]) == list(dict.fromkeys(names))
        assert all(type(there) is bool for there in at_once[1][1].values())
    else:
        assert at_once[1] == "ValueError", "a name no key is built from, raised where leaf_exists meets it"
    assert side.dump() == before, "nothing is written"


@pytest.mark.parametrize("asked", list(ASKED))
@pytest.mark.parametrize("root", list(ROOTS))
def test_the_estimate_s_read_answers_what_root_exists_and_get_leaf_items_answer(root, asked):
    side = _Redis().load(_content(root))
    catalog, names = side.catalog(), ASKED[asked]
    before = side.dump()

    at_once = _outcome(lambda: catalog.get_root_and_leaf_items(ORG, LAKE, names))
    one_by_one = _outcome(lambda: _leaves_call_by_call(catalog, names))

    assert at_once == one_by_one
    if at_once[0] == "returned":
        assert type(at_once[1]) is tuple and type(at_once[1][0]) is bool and type(at_once[1][1]) is list
    else:
        assert at_once[1] == "ValueError", "a replica of a name no key is built from"
    assert side.dump() == before, "nothing is written"


def test_what_the_states_cover():
    """The comparisons above are of lakes that are there and not, replicas of
    every kind, and leaves that are listed, left out and missing."""
    def exist(root, asked):
        return _Redis().load(_content(root)).catalog().root_and_leaves_exist(ORG, LAKE, ASKED[asked])

    def leaves(root, asked):
        return _Redis().load(_content(root)).catalog().get_root_and_leaf_items(ORG, LAKE, ASKED[asked])

    assert exist("a root", "two tables") == (True, {"orders": True, "items": True})
    assert [i["simple"] for i in leaves("a root", "two tables")[1]] == ["orders", "items"]
    assert exist("no root", "one table") == (False, {"orders": True})
    assert leaves("no root", "one table")[0] is False and len(leaves("no root", "one table")[1]) == 1
    assert exist("a root of empty text", "one table")[0] is True, "a key that is there, whatever it holds"
    assert exist("a root", "a table that is not there") == (True, {"nothere": False})
    assert leaves("a root", "a table that is not there") == (True, [])
    assert exist("a root", "a leaf of empty text") == (True, {"blank": True}), "there"
    assert leaves("a root", "a leaf of empty text") == (True, []), "and no leaf to list"
    assert leaves("a root", "a leaf that is not JSON") == (True, [])
    assert exist("a replica of another lake", "one table") == (True, {"orders": True})
    assert leaves("a replica of another lake", "one table")[1][0]["version"] == 9, "the source's leaf"
    assert exist("a replica of another lake", "a table only the source has")[1] == {"extras": True}
    assert exist("a replica of another lake", "two tables")[1] == {"orders": True, "items": False}
    assert exist("a replica given a table its source has not", "two tables") == (
        True, {"orders": False, "items": False})
    assert exist("a replica given a table its source has not", "a name in capitals") == (
        True, {"Orders": False}), "not asked for, whatever it is called"
    assert leaves("a replica with an empty list of tables", "one table")[1][0]["version"] == 9, \
        "an empty list gives every table, as it always did"
    assert leaves("a replica of itself", "one table")[1][0]["version"] == 3, "its own leaves"
    assert leaves("a replica of a replica", "one table")[1][0]["version"] == 7, "one level, and no further"
    assert exist("a replica of a lake that is not there", "one table") == (True, {"orders": False})
    assert leaves("a root that is a list", "one table")[1][0]["version"] == 3, "no replica: its own leaves"
    assert leaves("a root", "a name in capitals") == (True, []), "left out, as get_leaf_items leaves it out"
    with pytest.raises(ValueError):
        exist("a root", "a name in capitals")


@pytest.mark.parametrize("asked,leaves", [("one table", 1), ("two tables", 2), ("one table named twice", 1),
                                          ("no table", 0), ("every kind of leaf", len(LEAVES))])
def test_each_read_is_one_transaction_of_the_commands_that_were_sent_one_by_one(asked, leaves):
    side = _Redis().load(_content())
    names = list(dict.fromkeys(ASKED[asked]))
    catalog = side.catalog()

    catalog.root_and_leaves_exist(ORG, LAKE, ASKED[asked])
    catalog.get_root_and_leaf_items(ORG, LAKE, ASKED[asked])

    of_the_root = (("EXISTS", ROOT), ("GET", ROOT))
    assert side.trips == [
        ("MULTI", of_the_root + tuple(("EXISTS", _leaf_key(name)) for name in names)),
        ("MULTI", of_the_root + tuple(("GET", _leaf_key(name)) for name in names)),
    ]
    assert len(side.trips[0][1]) == len(side.trips[1][1]) == 2 + leaves


def test_the_calls_were_six_round_trips_for_one_table_and_four_more_for_another():
    """Three for the check, two for the leaves, one for the lake object; and
    for a second table three more for the check and one more lake object."""
    side = _Redis().load(_content())
    catalog = side.catalog()

    _existence_call_by_call(catalog, ["orders"])
    check_of_one = _names(side.trips)
    side.trips.clear()
    _existence_call_by_call(catalog, ["orders", "items"])
    check_of_two = _names(side.trips)
    side.trips.clear()
    catalog.get_leaf_items(ORG, LAKE, ["orders", "items"])

    assert check_of_one == ["EXISTS", "GET", "EXISTS"]
    assert check_of_two == ["EXISTS", "GET", "EXISTS", "GET", "EXISTS"], \
        "as this helper asks: the reader asked for the root before each table, one round trip more"
    assert _names(side.trips) == ["GET", "MULTI of 2"]


@pytest.mark.parametrize("root", ["a replica of another lake", "a replica given two tables",
                                  "a replica of a replica"])
def test_a_replica_is_read_in_two_transactions_the_second_of_its_source(root):
    side = _Redis().load(_content(root))
    source = MIRROR if root == "a replica of a replica" else SOURCE
    wanted = ["orders"] if root == "a replica given two tables" else ["orders", "extras"]
    catalog = side.catalog()

    catalog.root_and_leaves_exist(ORG, LAKE, ["orders", "extras"])
    catalog.get_root_and_leaf_items(ORG, LAKE, ["orders", "extras"])

    assert len(side.trips) == 4
    assert side.trips[0][1][:2] == side.trips[2][1][:2] == (("EXISTS", ROOT), ("GET", ROOT))
    assert side.trips[1] == ("MULTI", tuple(("EXISTS", _leaf_key(name, source)) for name in wanted))
    assert side.trips[3] == ("MULTI", tuple(("GET", _leaf_key(name, source)) for name in wanted))


def test_a_replica_given_none_of_the_tables_asked_for_is_read_once():
    side = _Redis().load(_content("a replica given a table its source has not"))
    catalog = side.catalog()

    assert catalog.root_and_leaves_exist(ORG, LAKE, ["orders", "extras"]) == (
        True, {"orders": False, "extras": False})
    assert catalog.get_root_and_leaf_items(ORG, LAKE, ["orders", "extras"]) == (True, [])
    assert len(side.trips) == 2


def _resolve_replica_info_as_it_was(self, org, sup):
    try:
        root = self.get_root(org, sup)
        if root and root.get("clone_type") == "replica":
            source = root.get("cloned_from", "")
            if source and source != sup:
                tables = root.get("replica_tables")
                allowed = tables if isinstance(tables, list) and tables else None
                return (source, allowed)
    except redis.RedisError:
        raise
    except Exception:
        pass
    return None


@pytest.mark.parametrize("root", list(ROOTS))
def test_a_replica_is_told_from_its_root_as_it_was(root):
    """The decision was moved into a function of its own, for the two reads
    to use on a root they have read already."""
    side = _Redis().load(_content(root))
    catalog = side.catalog()
    text = side.client.get(ROOT)

    was = _resolve_replica_info_as_it_was(catalog, ORG, LAKE)
    assert catalog._resolve_replica_info(ORG, LAKE) == was
    assert catalog._replica_info_of_text(text, LAKE) == was


def test_a_root_that_cannot_be_fetched_is_still_raised_by_the_replica_check():
    side = _Redis().load(_content())
    catalog = side.catalog()
    side.fail = lambda trip: trip == ("GET", ROOT)

    with pytest.raises(redis.exceptions.TimeoutError):
        catalog._resolve_replica_info(ORG, LAKE)
    with pytest.raises(redis.exceptions.TimeoutError):
        _resolve_replica_info_as_it_was(catalog, ORG, LAKE)


# ---------------------------------------------------------------------------
# A read Redis refuses, or does not answer
# ---------------------------------------------------------------------------

READS = {
    "the check's": ("root_and_leaves_exist", "[redis-catalog] root_and_leaves_exist error: "),
    "the estimate's": ("get_root_and_leaf_items", "[redis-catalog] get_root_and_leaf_items error: "),
}
OF_ANOTHER_KIND = {
    "the root is a hash": {ROOT: {"read_only": "true"}},
    "a leaf is a hash": {_leaf_key("orders"): {"version": "3"}},
    "a leaf is a set": {_leaf_key("items"): {"a", "b"}},
}


@pytest.mark.parametrize("state", list(OF_ANOTHER_KIND))
@pytest.mark.parametrize("which", list(READS))
def test_a_key_of_another_kind_fails_the_read_that_gets_it(caplog, which, state):
    """Both reads GET the root; only the estimate's GETs a leaf.  A leaf of
    another kind is there, for the check, as it is for ``leaf_exists``."""
    method, line = READS[which]
    content = _content()
    content.update(OF_ANOTHER_KIND[state])
    side = _Redis().load(content)
    catalog, before = side.catalog(), side.dump()
    refused = state == "the root is a hash" or which == "the estimate's"

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        outcome = _outcome(lambda: getattr(catalog, method)(ORG, LAKE, ["orders", "items"]))

    logged = [r.getMessage() for r in caplog.records if r.name == LOGGER]
    if refused:
        assert outcome[:2] == ("raised", "ResponseError") and "WRONGTYPE" in outcome[2]
        assert len(logged) == 1 and logged[0].startswith(line) and "WRONGTYPE" in logged[0]
    else:
        assert outcome == ("returned", (True, {"orders": True, "items": True})) and logged == []
    assert side.dump() == before


@pytest.mark.parametrize("which", list(READS))
def test_a_redis_that_does_not_answer_is_logged_and_raised(caplog, which):
    method, line = READS[which]
    side = _Redis().load(_content())
    side.fail = lambda trip: trip[0] == "MULTI"

    with caplog.at_level(logging.ERROR, logger=LOGGER):
        with pytest.raises(redis.exceptions.TimeoutError):
            getattr(side.catalog(), method)(ORG, LAKE, ["orders"])

    assert [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER] == [
        ("ERROR", line + "Timeout reading from socket")]


def _answering(side, replies) -> None:
    """Have this Redis answer every transaction with *replies*, whatever it holds."""
    real = side.client.pipeline

    def pipeline(*args, **options):
        opened = real(*args, **options)
        opened.execute = lambda *a, **k: replies
        return opened

    side.client.pipeline = pipeline


NOT_THE_REPLIES = {
    "the check's": [
        None, 0, "OK", [], [1], [1, A_ROOT], [1, A_ROOT, 1, 0],
        [True, A_ROOT, 1], ["1", A_ROOT, 1], [1, b"{}", 1], [1, 7, 1], [1, A_ROOT, None],
        [1, A_ROOT, "1"], [1, A_ROOT, True], MagicMock(),
    ],
    "the estimate's": [
        None, 0, "OK", [], [1], [1, A_ROOT], [1, A_ROOT, "{}", "{}"],
        [True, A_ROOT, "{}"], ["1", A_ROOT, "{}"], [1, b"{}", "{}"], [1, 7, "{}"], [None, A_ROOT, "{}"],
        [1, A_ROOT, {"version": 3}], [1, A_ROOT, b"{}"], [1, A_ROOT, 1], MagicMock(),
    ],
}


@pytest.mark.parametrize("which,replies", [(which, replies) for which in READS for replies in NOT_THE_REPLIES[which]],
                         ids=lambda value: value if isinstance(value, str) else repr(value)[:40])
def test_an_answer_that_is_not_those_replies_is_refused(caplog, which, replies):
    """What a stand-in for Redis answers that does not run a transaction."""
    method, _line = READS[which]
    side = _Redis().load(_content())
    catalog = side.catalog()
    _answering(side, replies)

    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        with pytest.raises(redis.ResponseError, match=f"unexpected reply to the transaction of {method}"):
            getattr(catalog, method)(ORG, LAKE, ["orders"])

    assert not [r for r in caplog.records if r.name == LOGGER], "no failure of Redis: nothing to log"


@pytest.mark.parametrize("which", list(READS))
def test_the_refusal_of_an_answer_does_not_repeat_what_was_answered(which):
    method, _line = READS[which]
    side = _Redis().load(_content())
    catalog = side.catalog()
    _answering(side, ["a secret count", "a secret root", "a secret leaf"])

    with pytest.raises(redis.ResponseError) as raised:
        getattr(catalog, method)(ORG, LAKE, ["orders"])

    assert "secret" not in str(raised.value)


def test_a_tuple_of_replies_is_as_good_as_a_list():
    side = _Redis().load(_content())
    catalog = side.catalog()
    _answering(side, (1, A_ROOT, 1))
    assert catalog.root_and_leaves_exist(ORG, LAKE, ["orders"]) == (True, {"orders": True})
    _answering(side, (1, A_ROOT, LEAVES["orders"]))
    assert catalog.get_root_and_leaf_items(ORG, LAKE, ["orders"])[1][0]["version"] == 3


# ---------------------------------------------------------------------------
# A call someone has replaced is the call that is made
# ---------------------------------------------------------------------------

STANDS_FOR = ["root_exists", "leaf_exists", "_leaf_exists_raw", "get_root", "_resolve_replica_info",
              "_replica_info_of", "get_leaf_items", "_leaf_items"]
#: Those of them that take no catalog: replaced, they are put there as they are.
NO_SELF = ("_replica_info_of", "_leaf_items")
NOT_ITS_OWN = "of RedisCatalog as it defines them"


def _both_refuse(catalog) -> None:
    with pytest.raises(NotImplementedError, match="root_and_leaves_exist stands for root_exists and leaf_exists "
                                                  + NOT_ITS_OWN):
        catalog.root_and_leaves_exist(ORG, LAKE, ["orders"])
    with pytest.raises(NotImplementedError, match="get_root_and_leaf_items stands for root_exists and "
                                                  "get_leaf_items " + NOT_ITS_OWN):
        catalog.get_root_and_leaf_items(ORG, LAKE, ["orders"])


@pytest.mark.parametrize("name", STANDS_FOR)
@pytest.mark.parametrize("where", ["on the class", "on the object"])
def test_a_catalog_with_one_of_those_calls_replaced_is_not_asked_for_either_read(monkeypatch, name, where):
    side = _Redis().load(_content())
    catalog = side.catalog()
    original = getattr(RedisCatalog, name)
    if where == "on the class":
        another = lambda *a, **k: original(*a, **k)     # noqa: E731 - the same call, by another function
        monkeypatch.setattr(RedisCatalog, name, staticmethod(another) if name in NO_SELF else another)
    else:
        bound = original if name in NO_SELF else (lambda *a, **k: original(catalog, *a, **k))
        setattr(catalog, name, bound)

    assert catalog._table_reads_are_its_own() is False
    _both_refuse(catalog)
    assert side.trips == [], "nothing was sent"


def test_a_catalog_as_the_library_defines_it_is_asked_for_them():
    side = _Redis().load(_content())

    assert side.catalog()._table_reads_are_its_own() is True
    assert sorted(redis_catalog._TABLE_READS_STAND_FOR) == sorted(STANDS_FOR)
    assert all(getattr(RedisCatalog, name) is original
               for name, original in redis_catalog._TABLE_READS_STAND_FOR.items())


class _ItsOwnCatalog(RedisCatalog):
    """A catalog a host might make: this lake is always there."""

    def root_exists(self, org, sup):
        return True


def test_a_subclass_is_not_asked_for_either_read():
    side = _Redis().load(_content("no root"))
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        catalog = _ItsOwnCatalog()

    _both_refuse(catalog)
    assert side.trips == []


@pytest.mark.parametrize("name", ["exists", "get"])
@pytest.mark.parametrize("where", ["on the class", "on the object"])
def test_a_client_with_exists_or_get_replaced_is_sent_that_command(monkeypatch, name, where):
    """What a test does to have Redis fail, or answer as the test likes.  The
    separate calls send EXISTS and GET through the client, one at a time; a
    transaction would go round what was put there."""
    side = _Redis().load(_content())
    catalog = side.catalog()
    original = getattr(redis.Redis, name)
    asked = []

    if where == "on the class":
        def watching(client, *args, **options):
            asked.append(name)
            return original(client, *args, **options)

        monkeypatch.setattr(type(side.client), name, watching)
    else:
        def watching(*args, **options):
            asked.append(name)
            return original(side.client, *args, **options)

        setattr(side.client, name, watching)

    assert catalog._table_reads_are_its_own() is False
    _both_refuse(catalog)
    assert side.trips == [] and asked == [], "nothing was sent"

    reader = _reader()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        reader._assert_targets_exist([_table("orders")])
    assert _names(side.trips) == ["EXISTS", "GET", "EXISTS"], "the check, call by call"
    assert asked == ([name] if name == "get" else [name, name])


def test_a_client_whose_pipeline_was_replaced_is_still_asked_through_it():
    """Both reads open a pipeline, as ``get_leaf_items`` does: what was put in
    its place is what is called, and nothing is gone round."""
    side = _Redis().load(_content())
    catalog = side.catalog()
    assert "pipeline" in vars(side.client), "this harness has replaced it, to record"

    assert catalog._table_reads_are_its_own() is True
    assert catalog.root_and_leaves_exist(ORG, LAKE, ["orders"]) == (True, {"orders": True})
    assert _names(side.trips) == ["MULTI of 3"]


class _HandWrittenClient:
    """A stand-in for the client, as some tests write one."""

    def register_script(self, text):
        return lambda keys=None, args=None: 0

    def exists(self, key):
        return 1

    def get(self, key):
        return None

    def pipeline(self):
        raise AssertionError("a stand-in is not sent the transaction")


@pytest.mark.parametrize("stand_in", [MagicMock, _HandWrittenClient], ids=["a mock", "a class written by hand"])
def test_a_stand_in_for_the_client_is_asked_call_by_call(stand_in):
    client = stand_in()
    with pytest.MonkeyPatch.context() as patched:
        patched.setattr(redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=client))
        catalog = RedisCatalog()

    assert catalog._table_reads_are_its_own() is False
    _both_refuse(catalog)
    if stand_in is MagicMock:
        client.pipeline.assert_not_called()


# ---------------------------------------------------------------------------
# The reader's check: on the one read, and as it was
# ---------------------------------------------------------------------------

def _reader() -> DataReader:
    reader = DataReader.__new__(DataReader)      # no storage, no query: the check needs neither
    reader.organization = ORG
    reader.super_name = LAKE
    reader.query = "SELECT 1"
    reader._log_ctx = ""
    return reader


def _table(simple, lake=LAKE, columns=()):
    return TableDefinition(super_name=lake, simple_name=simple, alias=simple, columns=list(columns))


def _assert_targets_exist_as_it_was(self, physical_tables) -> None:
    if not physical_tables:
        return
    # One catalog handle for the whole loop — cheaper than letting
    # each .exists() call open a fresh connection.
    catalog = RedisCatalog()
    # Dedup by (super, simple) — SQL may mention the same table
    # multiple times via different aliases.
    seen = set()
    for td in physical_tables:
        super_name = td.super_name
        simple_name = td.simple_name
        if not super_name or not simple_name:
            continue
        key = (super_name, simple_name)
        if key in seen:
            continue
        seen.add(key)
        if not catalog.root_exists(self.organization, super_name):
            raise SuperTableNotFoundError(self.organization, super_name)
        if not catalog.leaf_exists(self.organization, super_name, simple_name):
            raise TableNotFoundError(
                self.organization, super_name, simple_name
            )


def _logged(caplog) -> list:
    return [(r.levelname, r.getMessage()) for r in caplog.records if r.name == LOGGER]


def _checked_both_ways(caplog, content, tables):
    """The check as it was on one Redis, as it is on another holding the same."""
    old, new = _Redis().load(content), _Redis().load(content)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        caplog.clear()
        with pytest.MonkeyPatch.context() as patched:
            old.use(patched)
            was = _outcome(lambda: _assert_targets_exist_as_it_was(_reader(), tables))
        was_logged = _logged(caplog)
        caplog.clear()
        with pytest.MonkeyPatch.context() as patched:
            new.use(patched)
            reader = _reader()
            now = _outcome(lambda: reader._assert_targets_exist(tables))
        now_logged = _logged(caplog)
    assert now == was
    failed = bool(now_logged) and now_logged[0][1].startswith(READ_FAILED)
    assert (now_logged[1:] if failed else now_logged) == was_logged
    assert new.dump() == old.dump()
    return SimpleNamespace(outcome=now, old=old, new=new, read_failed=failed)


@pytest.mark.parametrize("asked", list(ASKED))
@pytest.mark.parametrize("root", list(ROOTS))
def test_the_check_passes_and_refuses_as_it_did(caplog, root, asked):
    ran = _checked_both_ways(caplog, _content(root), [_table(name) for name in ASKED[asked]])

    if ran.outcome[0] == "returned":
        assert ran.outcome == ("returned", None)


def test_the_refusals_compared_are_of_every_kind(caplog):
    def check(root, asked):
        return _checked_both_ways(caplog, _content(root), [_table(n) for n in ASKED[asked]]).outcome

    assert check("a root", "two tables") == ("returned", None)
    assert check("a root", "no table") == ("returned", None)
    assert check("no root", "one table") == ("raised", "SuperTableNotFoundError",
                                             str(SuperTableNotFoundError(ORG, LAKE)))
    assert check("a root", "one there, one not, one there") == (
        "raised", "TableNotFoundError", str(TableNotFoundError(ORG, LAKE, "nothere")))
    assert check("a replica given a table its source has not", "one table")[1] == "TableNotFoundError"
    assert check("a replica of another lake", "a table only the source has") == ("returned", None)
    assert check("a root", "a leaf of empty text") == ("returned", None), "a key that is there"
    assert check("a root", "a name in capitals")[1] == "ValueError"
    assert check("no root", "a name in capitals")[1] == "SuperTableNotFoundError", \
        "the lake is asked for before the name is looked at, as it was"
    assert check("a replica of a name no key is built from", "one table")[1] == "ValueError"


TWO_LAKES = {
    "both there": ("a root", [("orders", LAKE), ("orders", SOURCE), ("items", LAKE)]),
    "the second lake is not there": ("a root", [("orders", LAKE), ("orders", "gone"), ("nothere", LAKE)]),
    "a table of the first is missing, after the second lake": (
        "a root", [("orders", LAKE), ("extras", SOURCE), ("nothere", LAKE)]),
    "the first lake is not there, the second has a name no key is built from": (
        "no root", [("orders", LAKE), ("orders", "No Such Lake!")]),
    "a name no key is built from, in the lake named second": (
        "a root", [("orders", LAKE), ("Orders", SOURCE), ("nothere", LAKE)]),
    "blank names among them": ("a root", [("orders", LAKE), ("", LAKE), ("orders", ""), ("items", LAKE)]),
}


@pytest.mark.parametrize("case", list(TWO_LAKES))
def test_tables_of_two_lakes_are_checked_in_the_order_they_are_named(caplog, case):
    root, named = TWO_LAKES[case]

    ran = _checked_both_ways(caplog, _content(root), [_table(simple, lake) for simple, lake in named])

    expected = {
        "both there": ("returned",),
        "the second lake is not there": ("raised", "SuperTableNotFoundError"),
        "a table of the first is missing, after the second lake": ("raised", "TableNotFoundError"),
        "the first lake is not there, the second has a name no key is built from": (
            "raised", "SuperTableNotFoundError"),
        "a name no key is built from, in the lake named second": ("raised", "ValueError"),
        "blank names among them": ("returned",),
    }[case]
    assert ran.outcome[:len(expected)] == expected


def test_the_check_is_one_round_trip_for_each_lake():
    side = _Redis().load(_content())
    reader = _reader()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        reader._assert_targets_exist([_table("orders"), _table("items"), _table("orders")])
        one_lake = _names(side.trips)
        side.trips.clear()
        reader._assert_targets_exist([_table("orders"), _table("extras", SOURCE), _table("items")])

    assert one_lake == ["MULTI of 4"]
    assert _names(side.trips) == ["MULTI of 4", "MULTI of 3"]


def test_blank_names_are_passed_over_and_the_rest_is_still_one_round_trip():
    side = _Redis().load(_content())
    reader = _reader()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        reader._assert_targets_exist([_table("orders"), _table(""), _table("orders", ""), _table("items")])

    assert _names(side.trips) == ["MULTI of 4"]


def test_the_check_as_it_was_made_three_round_trips_for_each_table():
    side = _Redis().load(_content())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        _assert_targets_exist_as_it_was(_reader(), [_table("orders"), _table("items"), _table("orders")])

    assert _names(side.trips) == ["EXISTS", "GET", "EXISTS"] * 2


def test_no_table_asks_nothing_and_builds_no_catalog():
    reader = _reader()
    with patch.object(data_reader_mod, "RedisCatalog") as catalog_class:
        assert reader._assert_targets_exist([]) is None
        assert reader._assert_targets_exist(None) is None

    catalog_class.assert_not_called()


def test_the_check_leaves_nothing_on_the_reader():
    side = _Redis().load(_content())
    reader = _reader()
    before = dict(vars(reader))
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        assert reader._assert_targets_exist([_table("orders")]) is None

    assert vars(reader) == before


# --- when the one read is not to be had ------------------------------------

def test_a_root_of_another_kind_refuses_the_read_and_is_met_by_the_calls_as_it_was(caplog):
    content = _content()
    content.update(OF_ANOTHER_KIND["the root is a hash"])

    ran = _checked_both_ways(caplog, content, [_table("orders"), _table("items")])

    assert ran.read_failed and ran.outcome[:2] == ("raised", "ResponseError")
    assert ran.new.trips[0][0] == "MULTI" and ran.new.trips[1:] == ran.old.trips


@pytest.mark.parametrize("state", ["a leaf is a hash", "a leaf is a set"])
def test_a_leaf_of_another_kind_is_there_as_it_was(caplog, state):
    content = _content()
    content.update(OF_ANOTHER_KIND[state])

    ran = _checked_both_ways(caplog, content, [_table("orders"), _table("items")])

    assert ran.outcome == ("returned", None) and not ran.read_failed
    assert _names(ran.new.trips) == ["MULTI of 4"]


@pytest.mark.parametrize("also", [None, ("EXISTS", ROOT), ("GET", ROOT), ("EXISTS", _leaf_key("orders"))],
                         ids=["and then answers", "nor the first call", "nor the second", "nor the third"])
def test_a_read_redis_does_not_answer_is_followed_by_the_calls_as_they_were(caplog, also):
    """One failed round trip decides nothing: each of the three calls is still
    the call it was, and fails, or answers, its own way."""
    content, tables = _content(), [_table("orders")]
    old, new = _Redis().load(content), _Redis().load(content)
    old.fail = lambda trip: trip == also
    new.fail = lambda trip: trip[0] == "MULTI" or trip == also
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        with pytest.MonkeyPatch.context() as patched:
            old.use(patched)
            was = _outcome(lambda: _assert_targets_exist_as_it_was(_reader(), tables))
        was_logged = _logged(caplog)
        caplog.clear()
        with pytest.MonkeyPatch.context() as patched:
            new.use(patched)
            now = _outcome(lambda: _reader()._assert_targets_exist(tables))

    assert now == was
    assert now == (("returned", None) if also is None else
                   ("raised", "TimeoutError", "Timeout reading from socket"))
    assert _logged(caplog) == [("ERROR", READ_FAILED + "Timeout reading from socket")] + was_logged
    assert new.trips[1:] == old.trips


def test_a_catalog_put_in_its_place_is_asked_call_by_call():
    """How the tests of the check give it a catalog of their own."""
    reader = _reader()
    with patch.object(data_reader_mod, "RedisCatalog") as catalog_class:
        catalog = catalog_class.return_value
        catalog.root_exists.return_value = True
        catalog.leaf_exists.return_value = True
        reader._assert_targets_exist([_table("orders"), _table("items")])

    assert [call[0] for call in catalog.method_calls] == ["root_exists", "leaf_exists"] * 2, \
        "and nothing else was asked of it"


def test_a_subclass_put_in_its_place_is_asked_call_by_call():
    side = _Redis().load(_content("no root"))
    reader = _reader()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        patched.setattr(data_reader_mod, "RedisCatalog", _ItsOwnCatalog)
        reader._assert_targets_exist([_table("orders")])

    assert _names(side.trips) == ["GET", "EXISTS"], "its own answer for the root, then the leaf"


@pytest.mark.parametrize("name", STANDS_FOR)
def test_a_call_replaced_on_the_catalog_is_the_call_the_check_makes(monkeypatch, name):
    side = _Redis().load(_content())
    original = getattr(RedisCatalog, name)
    another = lambda *a, **k: original(*a, **k)     # noqa: E731
    monkeypatch.setattr(RedisCatalog, name, staticmethod(another) if name in NO_SELF else another)
    reader = _reader()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        reader._assert_targets_exist([_table("orders")])

    assert _names(side.trips) == ["EXISTS", "GET", "EXISTS"]


def test_a_library_without_the_one_read_checks_as_before(monkeypatch):
    side = _Redis().load(_content())
    monkeypatch.delattr(RedisCatalog, "root_and_leaves_exist")
    reader = _reader()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        reader._assert_targets_exist([_table("orders")])

    assert _names(side.trips) == ["EXISTS", "GET", "EXISTS"]


@pytest.mark.parametrize("answer", [
    None, MagicMock(), [True, {"orders": True}], (True,), (True, {"orders": True}, []),
    (1, {"orders": True}), (True, {"orders": 1}), (True, {"items": True}),
    (True, {"orders": True, "items": True}), (True, [("orders", True)]), (True, {}),
], ids=["nothing", "a mock", "a list", "one value", "three values", "a count for the root",
        "a count for a leaf", "another table's answer", "a table more", "pairs", "no table answered"])
def test_an_answer_that_is_not_the_read_s_is_followed_by_the_calls_as_they_were(monkeypatch, answer):
    side = _Redis().load(_content())
    monkeypatch.setattr(RedisCatalog, "root_and_leaves_exist", lambda self, org, sup, simples: answer)
    reader = _reader()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        reader._assert_targets_exist([_table("orders")])

    assert _names(side.trips) == ["EXISTS", "GET", "EXISTS"]


def test_a_table_object_that_cannot_be_read_is_met_by_the_loop_as_it_was():
    """Whatever a table definition raises when asked for its names is raised
    by the loop, where it always was."""
    class Odd:
        super_name = LAKE

        @property
        def simple_name(self):
            raise RuntimeError("no name")

    side = _Redis().load(_content())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with pytest.raises(RuntimeError, match="no name"):
            _reader()._assert_targets_exist([_table("orders"), Odd()])

    assert _names(side.trips) == ["EXISTS", "GET", "EXISTS"], "the table before it was checked, as it was"


# ---------------------------------------------------------------------------
# The estimate: its lake read in one transaction, or call by call
# ---------------------------------------------------------------------------

def _estimated(side, tables, *, together: bool, before_estimate=None) -> SimpleNamespace:
    """What an estimate hands on, and its round trips.

    Without *together* the library is one that has no
    ``get_root_and_leaf_items``: the estimate then makes the calls it always
    made.
    """
    test_settings = dataclasses.replace(settings, SUPERTABLE_DUCKDB_PRESIGNED=False, STORAGE_ENDPOINT_URL="")
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        patched.setattr(data_estimator_mod, "settings", test_settings)
        patched.setattr(super_table_mod, "get_storage", lambda: LocalStorage())
        patched.setattr(SuperTable, "read_simple_table_snapshot", lambda self, path: ON_DISK[path])
        if not together:
            patched.delattr(RedisCatalog, "get_root_and_leaf_items")
        estimator = DataEstimator(organization=ORG, storage=LocalStorage(), tables=tables)
        if before_estimate is not None:
            before_estimate(estimator, patched)
        side.trips.clear()
        try:
            reflection = estimator.estimate()
        except Exception as error:   # noqa: BLE001 - the error IS the outcome
            return SimpleNamespace(outcome={"raised": type(error).__name__, "message": str(error)},
                                   trips=_names(side.trips), estimator=estimator, reflection=None)
    outcome = {
        "tables": [{"super": s.super_name, "simple": s.simple_name, "version": s.simple_version,
                    "files": list(s.files), "columns": sorted(s.columns), "types": dict(s.column_types)}
                   for s in reflection.supers],
        "bytes": reflection.reflection_bytes,
        "files": reflection.total_reflections,
        "freshness_ms": reflection.freshness_ms,
        "storage": reflection.storage_type,
        "kept": {f"{sup}.{simple}": snapshot
                 for (sup, simple), snapshot in estimator.resolved_snapshots.items()},
        "plan": [entry for entry in estimator.plan_stats.stats if "PRUNE_DURATION_MS" not in entry],
    }
    return SimpleNamespace(outcome=outcome, trips=_names(side.trips), estimator=estimator, reflection=reflection)


def _estimated_both_ways(content, tables):
    old, new = _Redis().load(content), _Redis().load(content)
    was = _estimated(old, tables, together=False)
    now = _estimated(new, tables, together=True)
    assert now.outcome == was.outcome
    assert new.dump() == old.dump()
    return SimpleNamespace(was=was, now=now)


#: Table lists to estimate, on the lake as it is built here.
ESTIMATED = {
    "one table": ["orders"],
    "two tables": ["orders", "items"],
    "a table named twice": ["orders", "orders"],
    "a leaf that holds a path only": ["elsewhere"],
    "a table without files": ["emptytable"],
    "a leaf without a path": ["pathless"],
    "a leaf of empty text": ["blank"],
    "a leaf that is not JSON": ["broken"],
    "a leaf that is a list": ["listed"],
    "a leaf without a version": ["unversioned"],
    "a leaf whose version is no number": ["oddversion"],
    "a table that is not there": ["nothere"],
    "three tables, one of them without a snapshot": ["orders", "pathless", "items"],
    "a name in capitals": ["Orders"],
    "the lake's own name": [LAKE],
    "a table only the source has": ["extras"],
}


@pytest.mark.parametrize("asked", list(ESTIMATED))
@pytest.mark.parametrize("root", list(ROOTS))
def test_an_estimate_hands_on_what_it_did(root, asked):
    _estimated_both_ways(_content(root), [_table(name) for name in ESTIMATED[asked]])


def test_the_estimates_compared_find_files_snapshots_and_nothing():
    def estimate(asked, root="a root"):
        return _estimated_both_ways(_content(root), [_table(n) for n in asked]).now.outcome

    two = estimate(["orders", "items"])
    assert [(t["simple"], t["version"], len(t["files"])) for t in two["tables"]] == [
        ("items", 2, 1), ("orders", 3, 2)]
    assert two["kept"]["sales.orders"]["tombstone"] == ["dv-0.parquet"]
    assert estimate(["elsewhere"])["files"] == 3, "read from storage, by the path the leaf holds"
    assert estimate(["emptytable"])["tables"][0]["files"] == []
    assert "raised" in estimate(["blank"]), "a key that is there and holds no snapshot"
    assert "raised" in estimate(["nothere"])
    assert estimate(["Orders"])["tables"][0]["simple"] == "Orders", "found by its lowercase name, as it was"
    assert estimate(["orders"], "a replica of another lake")["tables"][0]["version"] == 9
    assert estimate(["orders"], "no root")["raised"] == "SuperTableNotFoundError", \
        "leaves without a root: the lake object refuses, as it did"
    assert estimate(["orders"], "a replica of a name no key is built from")["raised"] == "ValueError"


def test_an_estimate_is_one_round_trip_where_it_was_two_and_one_for_each_table():
    ran = _estimated_both_ways(_content(), [_table("orders"), _table("items")])

    assert ran.was.trips == ["GET", "MULTI of 2", "EXISTS", "EXISTS"], \
        "the root, the leaves, and the root once more for each table's lake object"
    assert ran.now.trips == ["MULTI of 4"]


def test_a_leaf_that_holds_a_path_only_is_read_from_storage_as_before():
    ran = _estimated_both_ways(_content(), [_table("elsewhere")])

    assert ran.was.trips == ["GET", "MULTI of 1", "EXISTS"]
    assert ran.now.trips == ["MULTI of 3"]
    assert ran.now.outcome["files"] == 3


def test_a_table_the_names_cannot_answer_for_is_looked_for_in_the_listing_as_before():
    """A key that is there and holds no snapshot: the names cannot answer, and
    the lake is listed, as it was."""
    ran = _estimated_both_ways(_content(), [_table("orders"), _table("pathless")])

    assert ran.was.trips[:2] == ["GET", "MULTI of 2"] and "SCAN" in ran.was.trips
    assert ran.now.trips == ["MULTI of 4"] + ran.was.trips[2:], \
        "the same listing, and after it the question each lake object asks, as it was"
    assert ran.was.trips[-2:] == ["EXISTS", "EXISTS"]


def test_a_lake_without_a_root_is_asked_for_by_the_lake_object_as_before():
    ran = _estimated_both_ways(_content("no root"), [_table("orders")])

    assert ran.was.trips == ["GET", "MULTI of 1", "EXISTS"]
    assert ran.now.trips == ["MULTI of 3", "EXISTS"], "not found in the transaction: asked for, and refused"
    assert ran.now.outcome["raised"] == "SuperTableNotFoundError"


def test_tables_of_two_lakes_are_read_in_one_transaction_for_each():
    tables = [_table("orders"), _table("extras", SOURCE), _table("items")]

    ran = _estimated_both_ways(_content(), tables)

    assert ran.was.trips == ["GET", "MULTI of 2", "EXISTS", "EXISTS", "GET", "MULTI of 1", "EXISTS"]
    assert ran.now.trips == ["MULTI of 4", "MULTI of 3"]


def test_every_estimate_reads_the_catalog_and_remembers_nothing():
    side = _Redis().load(_content())
    first = _estimated(side, [_table("orders")], together=True)
    side.bytes.set(_leaf_key("orders"), _leaf(_snapshot(4, ["o9.parquet"]), version=4))
    side.bytes.delete(ROOT)

    test_settings = dataclasses.replace(settings, SUPERTABLE_DUCKDB_PRESIGNED=False, STORAGE_ENDPOINT_URL="")
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        patched.setattr(data_estimator_mod, "settings", test_settings)
        patched.setattr(super_table_mod, "get_storage", lambda: LocalStorage())
        side.trips.clear()
        with pytest.raises(SuperTableNotFoundError):
            first.estimator.estimate()

    assert first.trips == ["MULTI of 3"] and first.estimator._roots_read == set(), \
        "the root that was there for the first estimate is not taken to be there for the second"
    assert _names(side.trips) == ["MULTI of 3", "EXISTS"]


# --- when the one read is not to be had ------------------------------------

ITS_OWN_READS = ["GET", "MULTI of 1", "EXISTS"]


@pytest.mark.parametrize("state", list(OF_ANOTHER_KIND))
def test_an_estimate_whose_read_redis_refuses_makes_its_calls_as_before(caplog, state):
    content = _content()
    content.update(OF_ANOTHER_KIND[state])
    old, new = _Redis().load(content), _Redis().load(content)
    tables = [_table("orders"), _table("items")]
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _estimated(old, tables, together=False)
        was_logged = _logged(caplog)
        caplog.clear()
        now = _estimated(new, tables, together=True)

    assert now.outcome == was.outcome and "raised" in now.outcome
    assert _logged(caplog)[0][1].startswith("[redis-catalog] get_root_and_leaf_items error: ")
    assert _logged(caplog)[1:] == was_logged
    assert now.trips == ["MULTI of 4"] + was.trips


@pytest.mark.parametrize("also", [None, "GET", "MULTI of 1", "EXISTS"],
                         ids=["and then answers", "nor the root", "nor the leaves", "nor the lake object's question"])
def test_an_estimate_whose_read_redis_does_not_answer_makes_its_calls_as_before(caplog, also):
    content, tables = _content(), [_table("orders")]
    old, new = _Redis().load(content), _Redis().load(content)
    old.fail = lambda trip: _names([trip])[0] == also
    new.fail = lambda trip: _names([trip])[0] in ("MULTI of 3", also)
    with caplog.at_level(logging.DEBUG, logger=LOGGER):
        was = _estimated(old, tables, together=False)
        was_logged = _logged(caplog)
        caplog.clear()
        now = _estimated(new, tables, together=True)

    assert now.outcome == was.outcome
    assert ("raised" in now.outcome) is (also is not None)
    assert _logged(caplog) == [("ERROR", "[redis-catalog] get_root_and_leaf_items error: "
                                         "Timeout reading from socket")] + was_logged
    assert now.trips == ["MULTI of 3"] + was.trips


def test_an_estimator_given_another_catalog_asks_that_catalog():
    """What a test does to have the estimate see leaves of its own."""
    side, other = _Redis().load(_content()), _Redis().load(_content())
    other.bytes.set(_leaf_key("orders"), _leaf(_snapshot(8, ["x.parquet"]), version=8))

    def swap(estimator, patched):
        estimator.catalog = other.catalog()

    ran = _estimated(side, [_table("orders")], together=True, before_estimate=swap)

    assert [s.simple_version for s in ran.reflection.supers] == [8], "the other catalog's leaf"
    assert _names(other.trips) == ["MULTI of 3"] and ran.trips == []


def test_an_estimator_whose_catalog_is_a_double_asks_the_double_as_before():
    side = _Redis().load(_content())
    double = MagicMock()
    double.get_leaf_items.return_value = [
        {"simple": "orders", "version": 6, "ts": 1, "path": "p.json", "payload": _snapshot(6, ["d.parquet"])}]

    def swap(estimator, patched):
        estimator.catalog = double

    ran = _estimated(side, [_table("orders")], together=True, before_estimate=swap)

    assert [s.simple_version for s in ran.reflection.supers] == [6]
    assert [call[0] for call in double.method_calls] == ["get_leaf_items"], "and nothing else was asked of it"
    assert ran.trips == ["EXISTS"], "the lake object asks for its root, as it did"


def test_an_estimator_whose_catalog_is_a_subclass_makes_its_calls_as_before():
    side = _Redis().load(_content())

    def swap(estimator, patched):
        side.use(patched)
        estimator.catalog = _ItsOwnCatalog()

    assert _estimated(side, [_table("orders")], together=True, before_estimate=swap).trips == ITS_OWN_READS


@pytest.mark.parametrize("name", STANDS_FOR)
def test_an_estimator_whose_catalog_has_a_call_replaced_makes_its_calls_as_before(name):
    side = _Redis().load(_content())
    original = getattr(RedisCatalog, name)

    def replace(estimator, patched):
        bound = original if name in NO_SELF else (lambda *a, **k: original(estimator.catalog, *a, **k))
        setattr(estimator.catalog, name, bound)

    assert _estimated(side, [_table("orders")], together=True, before_estimate=replace).trips == ITS_OWN_READS


@pytest.mark.parametrize("answer", [
    None, MagicMock(), [True, []], (True,), (True, [], []), (1, []), (True, ()), (True, ["an item"]),
    (True, {"orders": {}}),
], ids=["nothing", "a mock", "a list", "one value", "three values", "a count for the root",
        "a tuple of items", "an item that is no document", "items by name"])
def test_an_answer_that_is_not_the_read_s_is_followed_by_the_calls_as_before(answer):
    side = _Redis().load(_content())

    def replace(estimator, patched):
        patched.setattr(RedisCatalog, "get_root_and_leaf_items", lambda self, org, sup, simples: answer)

    ran = _estimated(side, [_table("orders")], together=True, before_estimate=replace)

    assert ran.trips == ITS_OWN_READS and [s.simple_version for s in ran.reflection.supers] == [3]


def test_the_collaborators_are_the_library_s_own_as_it_stands():
    assert data_estimator_mod._CATALOG_AS_DEFINED is RedisCatalog
    assert data_estimator_mod._LAKE_AS_DEFINED is SuperTable
    assert data_estimator_mod._LAKE_CONSTRUCTOR is SuperTable.__init__
    assert data_reader_mod._CATALOG_AS_DEFINED is RedisCatalog


# ---------------------------------------------------------------------------
# The lake object: told that its root is there, or asking
# ---------------------------------------------------------------------------

@pytest.fixture
def storage(monkeypatch):
    monkeypatch.setattr(super_table_mod, "get_storage", lambda: LocalStorage())


def test_a_lake_known_to_exist_is_opened_without_asking(storage):
    side = _Redis().load(_content("no root"))
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        lake = SuperTable(LAKE, ORG, create_if_missing=False, known_to_exist=True)

    assert side.trips == []
    assert (lake.super_name, lake.organization, lake.identity) == (LAKE, ORG, "super")
    assert lake.super_dir == os.path.join(ORG, LAKE, "super") and type(lake.catalog) is RedisCatalog
    assert side.dump() == _Redis().load(_content("no root")).dump(), "and nothing is made"


@pytest.mark.parametrize("known", [False, None, 1, "yes", MagicMock()],
                         ids=["false", "nothing", "one", "text", "a mock"])
def test_only_true_is_taken_for_known(storage, known):
    side = _Redis().load(_content("no root"))
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        with pytest.raises(SuperTableNotFoundError):
            SuperTable(LAKE, ORG, create_if_missing=False, known_to_exist=known)

    assert _names(side.trips) == ["EXISTS"]


def test_a_lake_is_asked_for_as_it_was_when_nothing_is_said(storage):
    side = _Redis().load(_content())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        SuperTable(LAKE, ORG, create_if_missing=False)

    assert side.trips == [("EXISTS", ROOT)]


def test_a_reserved_name_is_refused_known_or_not(storage):
    with pytest.raises(ValueError, match="is reserved"):
        SuperTable("_system_", ORG, create_if_missing=False, known_to_exist=True)


def test_a_lake_that_is_listed_is_asked_for_after_the_listing_as_it_was():
    """Found in the transaction, and then listed because a name could not
    answer: the root found before the listing is not taken for one found
    after it."""
    side = _Redis().load(_content())
    ran = _estimated(side, [_table("orders"), _table("pathless")], together=True)

    assert ran.estimator._roots_read == set()
    assert ran.trips[0] == "MULTI of 4" and "SCAN" in ran.trips and ran.trips[-2:] == ["EXISTS", "EXISTS"]


def test_the_estimator_opens_a_lake_it_has_found_without_asking_and_any_other_as_before(storage):
    side = _Redis().load(_content())
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        estimator = DataEstimator(organization=ORG, storage=LocalStorage(), tables=[_table("orders")])
        estimator._roots_read = {LAKE}
        estimator._open_lake(LAKE)
        found = _names(side.trips)
        side.trips.clear()
        estimator._open_lake(SOURCE)

    assert found == [] and side.trips == [("EXISTS", RK.meta_root(ORG, SOURCE))]


def test_a_lake_object_put_in_its_place_is_constructed_as_before():
    """How the estimator's tests give it lake objects of their own."""
    side = _Redis().load(_content())
    double = MagicMock()
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        patched.setattr(data_estimator_mod, "SuperTable", double)
        estimator = DataEstimator(organization=ORG, storage=LocalStorage(), tables=[_table("orders")])
        estimator._roots_read = {LAKE}
        estimator._open_lake(LAKE)

    double.assert_called_once_with(LAKE, ORG, create_if_missing=False)


class _NoLakeIsThere(RedisCatalog):
    """A catalog a test might put where a lake object builds its own."""

    def root_exists(self, org, sup):
        return False


def test_a_catalog_put_where_the_lake_object_builds_its_own_is_the_one_asked(storage):
    """The lake object asked the catalog its module names whether the root is
    there.  With another put there, it is the other that is asked."""
    content = _content()
    outcomes = {}
    for together in (False, True):
        side = _Redis().load(content)

        def replace(estimator, patched):
            patched.setattr(super_table_mod, "RedisCatalog", _NoLakeIsThere)

        ran = _estimated(side, [_table("orders")], together=together, before_estimate=replace)
        outcomes[together] = (ran.outcome, ran.trips)

    assert outcomes[True][0] == outcomes[False][0] and outcomes[True][0]["raised"] == "SuperTableNotFoundError"
    assert outcomes[False][1] == ["GET", "MULTI of 1"] and outcomes[True][1] == ["MULTI of 3"], \
        "the catalog put there answered for the root, and asked Redis nothing"


def test_the_lake_object_s_module_names_the_library_s_catalog_as_it_stands():
    assert data_estimator_mod._LAKE_MODULE is super_table_mod
    assert super_table_mod.RedisCatalog is RedisCatalog


def test_a_constructor_put_in_its_place_is_called_as_before(monkeypatch, storage):
    side = _Redis().load(_content())
    called = []

    def constructor(self, super_name, organization, *, create_if_missing=True):
        called.append((super_name, organization, create_if_missing))

    monkeypatch.setattr(SuperTable, "__init__", constructor)
    with pytest.MonkeyPatch.context() as patched:
        side.use(patched)
        estimator = DataEstimator(organization=ORG, storage=LocalStorage(), tables=[_table("orders")])
        estimator._roots_read = {LAKE}
        estimator._open_lake(LAKE)

    assert called == [(LAKE, ORG, False)]
