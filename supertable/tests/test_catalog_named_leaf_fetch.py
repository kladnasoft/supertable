"""``RedisCatalog.get_leaf_items``: the leaves of named tables, without SCAN.

A read used to list its whole lake to find the tables it named.
``scan_leaf_items`` runs SCAN, which walks every key of the Redis database
whatever the pattern, and then fetches and decodes every leaf of the lake; a
one-row lookup paid for all the tables of all the lakes sharing the database.
``get_leaf_items`` asks for the named keys instead.

It is only a replacement if it answers what the scan answers. That is what is
pinned here, on a real ``RedisCatalog`` over fakeredis:

  * for every name a leaf can have, the items are the scan's items for that
    name: same documents, decoded the same, replica lakes included;
  * it costs the root read and one round trip, and never a SCAN;
  * the named leaves are read together, in one transaction;
  * unlike the scan, a Redis error is raised and not read as "no such table".

How ``DataEstimator`` uses it, and when it still lists the lake, is in
``test_estimator_reads_named_tables_by_name.py``.
"""

from __future__ import annotations

import json
import random
from types import SimpleNamespace
from unittest.mock import MagicMock

import fakeredis
import pytest
import redis

from supertable import redis_catalog
from supertable import redis_keys as RK

ORG = "acme"
LAKE = "sales"


@pytest.fixture
def client():
    return fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)


@pytest.fixture
def catalog(client, monkeypatch):
    monkeypatch.setattr(
        redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=client))
    return redis_catalog.RedisCatalog()


def _leaf(version=1, files=("a.parquet",), **extra) -> str:
    doc = {
        "version": version,
        "ts": 1_700_000_000_000 + version,
        "path": f"{ORG}/{LAKE}/snap-{version}.json",
        "payload": {
            "snapshot_version": version,
            "schema": [{"name": "id", "type": "BIGINT"}],
            "resources": [{"file": f, "file_size": 10} for f in files],
        },
    }
    doc.update(extra)
    return json.dumps(doc)


def _put(client, sup: str, simple: str, document: str, org: str = ORG) -> None:
    """A leaf as every writer stores one: under the key ``redis_keys`` builds."""
    client.set(RK.meta_leaf(org, sup, simple), document)


def _put_raw(client, sup: str, name: str, document: str, org: str = ORG) -> None:
    """A leaf under a name ``redis_keys`` refuses: put there by hand, never by a writer."""
    client.set(RK.meta_leaf_pattern(org, sup)[:-1] + name, document)


def _replica(client, name: str, source: str, tables=None, org: str = ORG) -> None:
    root = {"version": 1, "ts": 1, "clone_type": "replica", "cloned_from": source}
    if tables is not None:
        root["replica_tables"] = tables
    client.set(RK.meta_root(org, name), json.dumps(root))


def _scanned(catalog, sup: str, org: str = ORG) -> dict:
    """What the scan yields for a lake, by table name."""
    by_name: dict = {}
    for item in catalog.scan_leaf_items(org, sup, count=512):
        by_name.setdefault(item["simple"], []).append(item)
    return by_name


def _expected(catalog, sup: str, names, org: str = ORG) -> list:
    """The scan's items for *names*: each name once, in the order first given."""
    by_name = _scanned(catalog, sup, org)
    out = []
    for name in dict.fromkeys(names):
        out.extend(by_name.get(name, []))
    return out


@pytest.fixture
def commands(client, monkeypatch):
    """Record every command the catalog sends, and every pipeline it opens."""
    seen = []
    real_execute = client.execute_command
    real_pipeline = client.pipeline

    def execute_command(*args, **kwargs):
        seen.append(str(args[0]).upper())
        return real_execute(*args, **kwargs)

    def pipeline(*args, **kwargs):
        seen.append(("PIPELINE", args, tuple(sorted(kwargs.items()))))
        return real_pipeline(*args, **kwargs)

    monkeypatch.setattr(client, "execute_command", execute_command)
    monkeypatch.setattr(client, "pipeline", pipeline)
    return seen


# ---------------------------------------------------------------------------
# The same answer as the scan
# ---------------------------------------------------------------------------

class TestTheSameItemsAsTheScan:

    def test_one_named_table(self, catalog, client):
        _put(client, LAKE, "orders", _leaf(3, ["o1.parquet", "o2.parquet"]))
        _put(client, LAKE, "lines", _leaf(5))

        items = catalog.get_leaf_items(ORG, LAKE, ["orders"])

        assert items == _expected(catalog, LAKE, ["orders"])
        assert [item["simple"] for item in items] == ["orders"]
        assert items[0]["version"] == 3
        assert items[0]["ts"] == 1_700_000_000_003
        assert items[0]["path"] == f"{ORG}/{LAKE}/snap-3.json"
        assert [r["file"] for r in items[0]["payload"]["resources"]] == [
            "o1.parquet", "o2.parquet"]

    def test_several_come_back_in_the_order_asked_for(self, catalog, client):
        for version, name in enumerate(["orders", "lines", "customers", "unrelated"], 1):
            _put(client, LAKE, name, _leaf(version))

        items = catalog.get_leaf_items(ORG, LAKE, ["lines", "customers", "orders"])

        assert [item["simple"] for item in items] == ["lines", "customers", "orders"]
        assert items == _expected(catalog, LAKE, ["lines", "customers", "orders"])

    def test_a_name_asked_for_twice_is_answered_once(self, catalog, client):
        _put(client, LAKE, "orders", _leaf(3))
        items = catalog.get_leaf_items(ORG, LAKE, ["orders", "orders"])
        assert [item["simple"] for item in items] == ["orders"]

    def test_a_table_that_is_not_there_is_left_out(self, catalog, client):
        _put(client, LAKE, "orders", _leaf(3))
        assert catalog.get_leaf_items(ORG, LAKE, ["nothere"]) == []
        assert [i["simple"] for i in catalog.get_leaf_items(
            ORG, LAKE, ["nothere", "orders"])] == ["orders"]

    def test_no_names_asks_redis_for_no_leaf(self, catalog, client, commands):
        _put(client, LAKE, "orders", _leaf(3))
        del commands[:]
        assert catalog.get_leaf_items(ORG, LAKE, []) == []
        assert commands == ["GET"]  # the root, for replica resolution

    def test_a_name_is_matched_whole(self, catalog, client):
        """Not the tables it is a prefix of, as a pattern would."""
        _put(client, LAKE, "orders", _leaf(1))
        _put(client, LAKE, "orders_archive", _leaf(2))
        _put(client, LAKE, "orders-2", _leaf(3))
        _put(client, LAKE, "order", _leaf(4))
        assert [i["version"] for i in catalog.get_leaf_items(ORG, LAKE, ["orders"])] == [1]
        assert [i["version"] for i in catalog.get_leaf_items(ORG, LAKE, ["order"])] == [4]

    def test_other_lakes_and_organizations_are_not_read(self, catalog, client):
        _put(client, LAKE, "orders", _leaf(1))
        _put(client, "sales-eu", "orders", _leaf(2))
        _put(client, "sales_2", "orders", _leaf(3))
        _put(client, LAKE, "orders", _leaf(4), org="globex")
        assert [i["version"] for i in catalog.get_leaf_items(ORG, LAKE, ["orders"])] == [1]
        assert [i["version"] for i in catalog.get_leaf_items(ORG, "sales-eu", ["orders"])] == [2]
        assert [i["version"] for i in catalog.get_leaf_items("globex", LAKE, ["orders"])] == [4]
        assert catalog.get_leaf_items("globex", "sales-eu", ["orders"]) == []

    def test_an_internal_table_is_fetched_by_its_name(self, catalog, client):
        _put(client, LAKE, "__data_quality__", _leaf(7))
        items = catalog.get_leaf_items(ORG, LAKE, ["__data_quality__"])
        assert items == _expected(catalog, LAKE, ["__data_quality__"])
        assert items[0]["version"] == 7

    @pytest.mark.parametrize("document", [
        json.dumps({"version": 2, "ts": 5, "path": "p.json"}),          # no inline snapshot
        json.dumps({"version": 2, "ts": 5}),                            # no path
        json.dumps({"version": 2, "ts": 5, "path": ""}),
        json.dumps({"version": "7", "ts": "9", "path": "p.json"}),      # numbers as text
        json.dumps({"path": "p.json"}),                                 # defaults
        json.dumps({"version": 2, "ts": 5, "path": "p.json", "payload": None}),
        json.dumps({"version": 2, "ts": 5, "path": "p.json", "payload": {"_row_filter": "a = 1"}}),
        json.dumps({"version": "x", "ts": 5, "path": "p.json"}),        # not a leaf: skipped
        json.dumps({"version": 2, "ts": None, "path": "p.json"}),       # not a leaf: skipped
        "{not json",
        "[1, 2]",
        "null",
        "0",
        '"text"',
        "",
    ], ids=["path-only", "no-path", "empty-path", "numbers-as-text", "defaults",
            "null-payload", "payload-with-filter", "bad-version", "null-ts",
            "malformed", "a-list", "null", "zero", "a-string", "empty"])
    def test_every_document_decodes_as_the_scan_decodes_it(self, catalog, client, document):
        _put(client, LAKE, "orders", document)
        assert catalog.get_leaf_items(ORG, LAKE, ["orders"]) == _expected(
            catalog, LAKE, ["orders"])

    def test_defaults_are_the_scan_s(self, catalog, client):
        _put(client, LAKE, "orders", json.dumps({"path": "p.json"}))
        assert catalog.get_leaf_items(ORG, LAKE, ["orders"]) == [
            {"simple": "orders", "version": -1, "ts": 0, "path": "p.json", "payload": None}]

    def test_a_lake_of_many_shapes_any_names(self, catalog, client):
        """Random lakes, random questions: always the scan's answer."""
        rng = random.Random(20261124)
        pool = [f"t{i:02d}" for i in range(24)] + [
            "orders", "orders_archive", "o", "a-b", "x_y", "__internal__", "__dq_1__", "9lives"]
        documents = [
            lambda v: _leaf(v),
            lambda v: _leaf(v, files=[]),
            lambda v: json.dumps({"version": v, "ts": v, "path": f"p{v}.json"}),
            lambda v: json.dumps({"version": v, "ts": v}),
            lambda v: json.dumps({"version": v, "ts": v, "path": ""}),
            lambda v: "{not json",
            lambda v: "[]",
            lambda v: json.dumps({"version": "x", "path": "p.json"}),
        ]
        for round_number in range(120):
            client.flushall()
            present = rng.sample(pool, rng.randint(0, len(pool)))
            for version, name in enumerate(present, 1):
                _put(client, LAKE, name, rng.choice(documents)(version))
                if rng.random() < 0.3:      # the same name in another lake
                    _put(client, "other", name, _leaf(900 + version))
            for i in range(rng.randint(0, 30)):                 # unrelated keys
                client.set(f"unrelated:{round_number}:{i}", "x")
            asked = [rng.choice(pool) for _ in range(rng.randint(0, 8))]

            assert catalog.get_leaf_items(ORG, LAKE, asked) == _expected(
                catalog, LAKE, asked), (round_number, present, asked)


# ---------------------------------------------------------------------------
# Names no leaf can have
# ---------------------------------------------------------------------------

class TestANameNoLeafCanHave:
    """Every leaf key is built by ``redis_keys``, which admits lowercase names
    only. A name it refuses has no leaf, so there is nothing to fetch."""

    @pytest.mark.parametrize("name", [
        "Orders", "ORDERS", "my table", "orders.v2", "", "-orders", "_orders_",
        "x" * 65, "ordérs", "orders*", "a:b", "orders\n",
    ])
    def test_it_is_left_out_and_not_an_error(self, catalog, client, name):
        _put(client, LAKE, "orders", _leaf(3))
        assert catalog.get_leaf_items(ORG, LAKE, [name]) == []
        assert [i["simple"] for i in catalog.get_leaf_items(
            ORG, LAKE, [name, "orders"])] == ["orders"]

    @pytest.mark.parametrize("name", [None, 7, b"orders", ("orders",)])
    def test_something_that_is_not_a_name_is_left_out(self, catalog, client, name):
        _put(client, LAKE, "orders", _leaf(3))
        assert catalog.get_leaf_items(ORG, LAKE, [name]) == []

    def test_a_pattern_is_not_expanded(self, catalog, client, commands):
        """``*`` names no key here; nothing is listed for it."""
        _put(client, LAKE, "orders", _leaf(3))
        assert catalog.get_leaf_items(ORG, LAKE, ["*", "ord*", "?rders", "[o]rders"]) == []
        assert "SCAN" not in commands and "KEYS" not in commands

    def test_a_leaf_put_there_by_hand_is_not_found_by_name(self, catalog, client):
        """The scan lists it; a named fetch cannot build its key. A reader that
        must still see it falls back to the scan (``DataEstimator`` does)."""
        _put_raw(client, LAKE, "Orders", _leaf(8))
        assert [i["simple"] for i in catalog.scan_leaf_items(ORG, LAKE)] == ["Orders"]
        assert catalog.get_leaf_items(ORG, LAKE, ["Orders"]) == []
        assert catalog.get_leaf_items(ORG, LAKE, ["orders"]) == []

    @pytest.mark.parametrize("org,sup", [
        ("Acme", LAKE), (ORG, "Sales"), (ORG, "my lake"), ("", LAKE), (ORG, ""),
        (ORG, "_reserved_"), (None, LAKE), (ORG, None),
    ])
    def test_an_organization_or_lake_no_key_can_be_built_from_raises_as_the_scan_does(
            self, catalog, org, sup):
        with pytest.raises(ValueError) as from_the_scan:
            list(catalog.scan_leaf_items(org, sup))
        with pytest.raises(ValueError) as by_name:
            catalog.get_leaf_items(org, sup, ["orders"])
        assert str(by_name.value) == str(from_the_scan.value)


# ---------------------------------------------------------------------------
# Replica lakes
# ---------------------------------------------------------------------------

class TestReplicaLakes:
    """A replica's tables are its source's, less the ones it was not given."""

    @pytest.fixture
    def source(self, client):
        for version, name in enumerate(["orders", "lines", "customers"], 1):
            _put(client, LAKE, name, _leaf(version))

    def test_a_replica_reads_its_source_s_leaves(self, catalog, client, source):
        _replica(client, "mirror", LAKE)
        asked = ["orders", "lines", "customers", "nothere"]
        items = catalog.get_leaf_items(ORG, "mirror", asked)
        assert items == _expected(catalog, "mirror", asked)
        assert [i["simple"] for i in items] == ["orders", "lines", "customers"]

    def test_a_replica_given_some_tables_reads_only_those(self, catalog, client, source):
        _replica(client, "mirror", LAKE, tables=["orders", "customers"])
        asked = ["orders", "lines", "customers"]
        items = catalog.get_leaf_items(ORG, "mirror", asked)
        assert items == _expected(catalog, "mirror", asked)
        assert [i["simple"] for i in items] == ["orders", "customers"]
        assert catalog.get_leaf_items(ORG, "mirror", ["lines"]) == []

    def test_an_empty_table_list_restricts_nothing(self, catalog, client, source):
        _replica(client, "mirror", LAKE, tables=[])
        assert [i["simple"] for i in catalog.get_leaf_items(
            ORG, "mirror", ["lines"])] == ["lines"]
        assert catalog.get_leaf_items(ORG, "mirror", ["lines"]) == _expected(
            catalog, "mirror", ["lines"])

    def test_a_replica_s_own_leaves_are_not_its_tables(self, catalog, client, source):
        _replica(client, "mirror", LAKE)
        _put(client, "mirror", "orders", _leaf(99))
        _put(client, "mirror", "local_only", _leaf(98))
        assert [i["version"] for i in catalog.get_leaf_items(ORG, "mirror", ["orders"])] == [1]
        assert catalog.get_leaf_items(ORG, "mirror", ["local_only"]) == []
        assert catalog.get_leaf_items(ORG, "mirror", ["orders", "local_only"]) == _expected(
            catalog, "mirror", ["orders", "local_only"])

    def test_the_source_itself_is_unaffected(self, catalog, client, source):
        _replica(client, "mirror", LAKE, tables=["orders"])
        assert [i["simple"] for i in catalog.get_leaf_items(
            ORG, LAKE, ["orders", "lines"])] == ["orders", "lines"]

    def test_a_replica_of_a_lake_that_is_not_there_has_no_tables(self, catalog, client):
        _replica(client, "mirror", "gone")
        _put(client, "mirror", "orders", _leaf(99))
        assert catalog.get_leaf_items(ORG, "mirror", ["orders"]) == []
        assert _expected(catalog, "mirror", ["orders"]) == []

    @pytest.mark.parametrize("root", [
        {"version": 1, "clone_type": "readonly", "cloned_from": LAKE},   # a clone, not a replica
        {"version": 1, "clone_type": "replica", "cloned_from": ""},
        {"version": 1, "clone_type": "replica", "cloned_from": "mirror"},  # of itself
        {"version": 1},
    ], ids=["readonly-clone", "no-source", "its-own-source", "plain"])
    def test_a_lake_that_is_not_a_replica_reads_its_own(self, catalog, client, source, root):
        client.set(RK.meta_root(ORG, "mirror"), json.dumps(root))
        _put(client, "mirror", "orders", _leaf(99))
        assert [i["version"] for i in catalog.get_leaf_items(ORG, "mirror", ["orders"])] == [99]
        assert catalog.get_leaf_items(ORG, "mirror", ["orders"]) == _expected(
            catalog, "mirror", ["orders"])

    def test_a_root_that_is_not_json_reads_as_no_replica(self, catalog, client, source):
        client.set(RK.meta_root(ORG, "mirror"), "{not json")
        _put(client, "mirror", "orders", _leaf(99))
        assert [i["version"] for i in catalog.get_leaf_items(ORG, "mirror", ["orders"])] == [99]

    def test_random_replicas_answer_as_the_scan_does(self, catalog, client):
        rng = random.Random(7)
        pool = [f"t{i}" for i in range(12)]
        for round_number in range(80):
            client.flushall()
            for version, name in enumerate(rng.sample(pool, rng.randint(0, 12)), 1):
                _put(client, LAKE, name, _leaf(version))
            for version, name in enumerate(rng.sample(pool, rng.randint(0, 4)), 50):
                _put(client, "mirror", name, _leaf(version))
            tables = rng.choice([None, [], rng.sample(pool, rng.randint(1, 6))])
            _replica(client, "mirror", LAKE, tables=tables)
            asked = [rng.choice(pool) for _ in range(rng.randint(1, 6))]
            assert catalog.get_leaf_items(ORG, "mirror", asked) == _expected(
                catalog, "mirror", asked), (round_number, tables, asked)


# ---------------------------------------------------------------------------
# What it costs
# ---------------------------------------------------------------------------

class TestWhatItAsksRedis:

    def test_the_root_and_one_round_trip_whatever_the_lake_holds(
            self, catalog, client, commands):
        for i in range(300):
            _put(client, LAKE, f"t{i}", _leaf(i + 1))
        for i in range(2000):
            client.set(f"unrelated:{i}", "x")
        del commands[:]

        items = catalog.get_leaf_items(ORG, LAKE, ["t7", "t11", "t299"])

        assert [i["simple"] for i in items] == ["t7", "t11", "t299"]
        assert commands == ["GET", ("PIPELINE", (), ())]
        assert "SCAN" not in commands

    def test_the_scan_it_stands_in_for_walks_the_database(self, catalog, client, commands):
        """The contrast: the listing needs SCAN, page after page."""
        for i in range(300):
            _put(client, LAKE, f"t{i}", _leaf(i + 1))
        for i in range(2000):
            client.set(f"unrelated:{i}", "x")
        del commands[:]

        list(catalog.scan_leaf_items(ORG, LAKE, count=512))

        assert commands.count("SCAN") > 1

    def test_the_named_leaves_are_read_in_one_transaction(self, catalog, client, monkeypatch):
        """As one batch of the scan is: the tables of a query, as of one moment."""
        _put(client, LAKE, "orders", _leaf(1))
        _put(client, LAKE, "lines", _leaf(2))
        opened = []
        real_pipeline = client.pipeline

        def pipeline(*args, **kwargs):
            pipe = real_pipeline(*args, **kwargs)
            opened.append(pipe)
            return pipe

        monkeypatch.setattr(client, "pipeline", pipeline)
        catalog.get_leaf_items(ORG, LAKE, ["orders", "lines"])

        assert len(opened) == 1
        # redis-py's name for "wrapped in MULTI/EXEC".
        assert opened[0].transaction is True

    def test_the_scan_still_yields_what_it_did(self, catalog, client):
        """``_fetch_batch`` now shares its decoder; its own output is unchanged."""
        _put(client, LAKE, "orders", _leaf(3, ["a.parquet"]))
        _put(client, LAKE, "broken", "{not json")
        _put(client, LAKE, "empty", "")
        assert list(catalog.scan_leaf_items(ORG, LAKE)) == [{
            "simple": "orders",
            "version": 3,
            "ts": 1_700_000_000_003,
            "path": f"{ORG}/{LAKE}/snap-3.json",
            "payload": {
                "snapshot_version": 3,
                "schema": [{"name": "id", "type": "BIGINT"}],
                "resources": [{"file": "a.parquet", "file_size": 10}],
            },
        }]


# ---------------------------------------------------------------------------
# A leaf that could not be read is not an absent one
# ---------------------------------------------------------------------------

def _bare_catalog(redis_client):
    cat = redis_catalog.RedisCatalog.__new__(redis_catalog.RedisCatalog)
    cat.r = redis_client
    return cat


def _timing_out(*args, **kwargs):
    raise redis.exceptions.TimeoutError("Timeout reading from socket")


def _pipeline_that(fails: bool, values=None):
    pipe = MagicMock(name="pipeline")
    pipe.__enter__.return_value = pipe
    pipe.__exit__.return_value = False
    if fails:
        pipe.execute.side_effect = _timing_out
    else:
        pipe.execute.return_value = values
    return pipe


class TestARedisErrorIsNotAnAbsentTable:

    def test_a_root_that_cannot_be_read_raises(self):
        client = MagicMock(name="redis")
        client.get.side_effect = _timing_out
        with pytest.raises(redis.exceptions.TimeoutError):
            _bare_catalog(client).get_leaf_items(ORG, LAKE, ["orders"])
        client.pipeline.assert_not_called()

    def test_leaves_that_cannot_be_read_raise(self):
        client = MagicMock(name="redis")
        client.get.return_value = None
        client.pipeline.return_value = _pipeline_that(fails=True)
        with pytest.raises(redis.exceptions.TimeoutError):
            _bare_catalog(client).get_leaf_items(ORG, LAKE, ["orders"])

    @pytest.mark.parametrize("error", [
        redis.exceptions.ConnectionError("down"),
        redis.exceptions.ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        redis.exceptions.BusyLoadingError("loading"),
    ], ids=["connection", "wrong-type", "loading"])
    def test_whatever_the_redis_error(self, error):
        client = MagicMock(name="redis")
        client.get.return_value = None
        pipe = _pipeline_that(fails=False)
        pipe.execute.side_effect = error
        client.pipeline.return_value = pipe
        with pytest.raises(type(error)):
            _bare_catalog(client).get_leaf_items(ORG, LAKE, ["orders"])

    def test_the_scan_still_reads_the_same_failure_as_no_tables(self):
        """Unchanged, and the reason the two differ is in ``get_leaf_items``."""
        client = MagicMock(name="redis")
        client.get.return_value = None
        client.scan.return_value = (0, [RK.meta_leaf(ORG, LAKE, "orders")])
        client.pipeline.return_value = _pipeline_that(fails=True)
        assert list(_bare_catalog(client).scan_leaf_items(ORG, LAKE)) == []

    def test_a_healthy_redis_still_reports_absence(self):
        client = MagicMock(name="redis")
        client.get.return_value = None
        client.pipeline.return_value = _pipeline_that(fails=False, values=[None])
        assert _bare_catalog(client).get_leaf_items(ORG, LAKE, ["orders"]) == []

    def test_an_error_that_is_not_redis_s_is_not_swallowed_either(self):
        client = MagicMock(name="redis")
        client.get.return_value = None
        pipe = _pipeline_that(fails=False)
        pipe.execute.side_effect = RuntimeError("not a redis error")
        client.pipeline.return_value = pipe
        with pytest.raises(RuntimeError, match="not a redis error"):
            _bare_catalog(client).get_leaf_items(ORG, LAKE, ["orders"])
