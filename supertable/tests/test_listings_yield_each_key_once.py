"""A listing built from SCAN names each key once.

Redis's SCAN promises that a full iteration returns every key that was there
throughout it. It does not promise to return each of them once: when the
keyspace is resized between two calls of one iteration, keys already returned
can come back. On a redis:7, with another client adding and deleting keys,
1 listing in 129 of a 60-table lake returned a key twice.

Four listings passed the repeat on to their callers:

  * ``RedisCatalog.scan_leaf_keys`` / ``scan_leaf_items``, the table listing.
    ``DataEstimator`` took every listed leaf for a snapshot, so a table listed
    twice had its files scanned twice and every row came back doubled, with
    ``Status.OK``. Every read listed its lake until reads began fetching their
    tables by name (``test_estimator_reads_named_tables_by_name.py``); the
    reads that still list are covered here, and dataisland-core builds its
    inventories, garbage collection and backups from the same listing.
  * ``meta_reader._get_redis_items``, behind ``list_supers`` and
    ``list_tables``: a lake or a table named twice in a listing.
  * ``RedisCatalog.find_readonly_clones``: a clone named twice.
  * ``redis_infra._FallbackCatalog.scan_leaf_keys``, the stand-in table
    listing used when the catalog cannot be built.

Each now skips a key it has already returned. What must not change is pinned
with it: every distinct key still comes through, in the order first seen, and
one listing remembers nothing for the next.

The same read through a real writer, reader and engine is in
``tests/characterization/test_named_tables_are_read_without_listing_the_lake_end_to_end.py``.
"""

from __future__ import annotations

import dataclasses
import importlib
import json
import random
import sys
from unittest.mock import MagicMock, patch

import pytest
import redis

import supertable.config.settings as cfg
from supertable import redis_keys as RK
from supertable.config.settings import settings
from supertable.data_classes import TableDefinition
from supertable.engine.data_estimator import DataEstimator
from supertable.redis_catalog import RedisCatalog
from supertable.storage.local_storage import LocalStorage

# ``supertable.engine`` is shadowed by the ``Engine`` enum at the package
# level; the module is fetched by name, as test_read_pruning_integration does.
data_estimator_mod = sys.modules["supertable.engine.data_estimator"]

ORG = "acme"
LAKE = "sales"


# ---------------------------------------------------------------------------
# A Redis whose SCAN repeats itself
# ---------------------------------------------------------------------------

def _pages(*pages):
    """A ``scan`` that answers with *pages* in turn; the last ends the iteration."""
    state = {"calls": 0}

    def scan(cursor=0, match=None, count=None):
        index = state["calls"]
        state["calls"] += 1
        if index >= len(pages):
            raise AssertionError("SCAN was continued past the end of its iteration")
        return (0 if index == len(pages) - 1 else index + 1), list(pages[index])

    scan.state = state
    return scan


class _Pipeline:
    def __init__(self, client):
        self._client = client
        self._keys = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def get(self, key):
        self._keys.append(key)

    def execute(self):
        self._client.fetched.extend(self._keys)
        return [self._client.values.get(key) for key in self._keys]


class _Redis:
    """Just enough of a client: SCAN from a script, values from a dict."""

    def __init__(self, pages, values=None):
        self.scan = _pages(*pages)
        self.values = dict(values or {})
        self.fetched = []

    def get(self, key):
        self.fetched.append(key)
        return self.values.get(key)

    def pipeline(self, *args, **kwargs):
        return _Pipeline(self)

    def leaves_fetched(self):
        return [key for key in self.fetched if ":meta:leaf:doc:" in key]


def _catalog(client) -> RedisCatalog:
    """A RedisCatalog wired to *client*, skipping connection setup."""
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = client
    return catalog


def _leaf(name: str, sup: str = LAKE) -> str:
    return RK.meta_leaf(ORG, sup, name)


def _root(sup: str) -> str:
    return RK.meta_root(ORG, sup)


def _document(version: int, files) -> str:
    return json.dumps({
        "version": version, "ts": 1_700_000_000_000 + version, "path": f"snap-{version}.json",
        "payload": {
            "snapshot_version": version,
            "schema": [{"name": "id", "type": "BIGINT"}],
            "resources": [{"file": f, "file_size": 10} for f in files],
        },
    })


ORDERS, LINES, CUSTOMERS, RETURNS = (_leaf(n) for n in ("orders", "lines", "customers", "returns"))


# ---------------------------------------------------------------------------
# The table listing
# ---------------------------------------------------------------------------

class TestTheTableListing:

    def test_a_key_returned_twice_in_one_call(self):
        catalog = _catalog(_Redis([[ORDERS, LINES, ORDERS]]))
        assert list(catalog.scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES]

    def test_a_key_returned_again_by_a_later_call(self):
        """What a resize between two calls does: keys already returned come back."""
        catalog = _catalog(_Redis([[ORDERS, LINES], [CUSTOMERS, ORDERS], [LINES, RETURNS, ORDERS]]))
        assert list(catalog.scan_leaf_keys(ORG, LAKE, count=2)) == [
            ORDERS, LINES, CUSTOMERS, RETURNS]

    def test_a_whole_page_returned_again(self):
        catalog = _catalog(_Redis([[ORDERS, LINES], [ORDERS, LINES], [ORDERS, LINES]]))
        assert list(catalog.scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES]

    def test_one_key_as_bytes_and_as_text_is_one_key(self):
        catalog = _catalog(_Redis([[ORDERS.encode(), LINES], [ORDERS, LINES.encode()]]))
        assert list(catalog.scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES]

    def test_a_listing_with_no_repeat_is_what_it_was(self):
        catalog = _catalog(_Redis([[ORDERS, LINES], [], [CUSTOMERS], [RETURNS]]))
        assert list(catalog.scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES, CUSTOMERS, RETURNS]

    def test_an_empty_lake(self):
        assert list(_catalog(_Redis([[]])).scan_leaf_keys(ORG, LAKE)) == []

    def test_every_key_comes_through_once_in_the_order_first_seen(self):
        """Whatever SCAN repeats, nothing is lost and nothing is reordered."""
        rng = random.Random(20261124)
        names = [_leaf(f"t{i:03d}") for i in range(200)]
        for _ in range(200):
            present = rng.sample(names, rng.randint(0, 60))
            returned = list(present)
            for _ in range(rng.randint(0, 40)):                # SCAN's repeats
                if present:
                    returned.insert(rng.randint(0, len(returned)), rng.choice(present))
            pages, rest = [], list(returned)
            while rest:
                size = rng.randint(0, 7)
                pages.append(rest[:size])
                rest = rest[size:]
            pages = pages or [[]]
            as_sent = [k.encode() if rng.random() < 0.3 else k for page in pages for k in page]
            sent_pages, position = [], 0
            for page in pages:
                sent_pages.append(as_sent[position:position + len(page)])
                position += len(page)

            listed = list(_catalog(_Redis(sent_pages)).scan_leaf_keys(ORG, LAKE))

            assert listed == list(dict.fromkeys(returned))
            assert sorted(listed) == sorted(present)

    def test_one_listing_remembers_nothing_for_the_next(self):
        client = _Redis([[ORDERS, LINES, ORDERS]])
        catalog = _catalog(client)
        assert list(catalog.scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES]
        client.scan = _pages([LINES, ORDERS], [CUSTOMERS])
        assert list(catalog.scan_leaf_keys(ORG, LAKE)) == [LINES, ORDERS, CUSTOMERS]

    def test_two_listings_under_way_do_not_share_what_they_have_seen(self):
        one = _catalog(_Redis([[ORDERS], [LINES]])).scan_leaf_keys(ORG, LAKE)
        two = _catalog(_Redis([[ORDERS], [LINES]])).scan_leaf_keys(ORG, LAKE)
        assert next(one) == ORDERS
        assert next(two) == ORDERS
        assert list(one) == [LINES]
        assert list(two) == [LINES]

    def test_a_scan_that_fails_part_way_still_ends_the_listing_quietly(self):
        """Unchanged: what was listed before the error is what the caller gets."""
        client = _Redis([[ORDERS, ORDERS, LINES]])
        calls = []

        def scan(cursor=0, match=None, count=None):
            calls.append(cursor)
            if len(calls) == 1:
                return 7, [ORDERS, ORDERS, LINES]
            raise redis.exceptions.TimeoutError("Timeout reading from socket")

        client.scan = scan
        assert list(_catalog(client).scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES]

    def test_the_pattern_and_the_page_size_asked_of_redis_are_unchanged(self):
        client = _Redis([[ORDERS]])
        seen = []
        real = client.scan

        def scan(cursor=0, match=None, count=None):
            seen.append((cursor, match, count))
            return real(cursor=cursor, match=match, count=count)

        client.scan = scan
        list(_catalog(client).scan_leaf_keys(ORG, LAKE, count=512))
        assert seen == [(0, RK.meta_leaf_pattern(ORG, LAKE), 512)]


class TestTheTableListingWithItsDocuments:

    def _values(self):
        return {ORDERS: _document(3, ["o.parquet"]), LINES: _document(5, ["l.parquet"]),
                CUSTOMERS: _document(7, ["c.parquet"])}

    def test_each_table_is_one_item(self):
        client = _Redis([[ORDERS, LINES], [ORDERS, CUSTOMERS, LINES]], self._values())
        items = list(_catalog(client).scan_leaf_items(ORG, LAKE))
        assert [(i["simple"], i["version"]) for i in items] == [
            ("orders", 3), ("lines", 5), ("customers", 7)]

    def test_each_leaf_is_fetched_once(self):
        client = _Redis([[ORDERS, LINES], [ORDERS, CUSTOMERS, LINES]], self._values())
        list(_catalog(client).scan_leaf_items(ORG, LAKE))
        assert client.leaves_fetched() == [ORDERS, LINES, CUSTOMERS]

    def test_a_repeat_that_falls_in_a_later_batch_is_not_fetched_again(self):
        """``count`` is also the size of a fetch batch."""
        client = _Redis([[ORDERS, LINES], [ORDERS, CUSTOMERS], [LINES, ORDERS]], self._values())
        items = list(_catalog(client).scan_leaf_items(ORG, LAKE, count=2))
        assert [i["simple"] for i in items] == ["orders", "lines", "customers"]
        assert client.leaves_fetched() == [ORDERS, LINES, CUSTOMERS]

    def test_a_listing_with_no_repeat_yields_what_it_did(self):
        client = _Redis([[ORDERS], [LINES]], self._values())
        assert list(_catalog(client).scan_leaf_items(ORG, LAKE)) == [
            {"simple": "orders", "version": 3, "ts": 1_700_000_000_003, "path": "snap-3.json",
             "payload": json.loads(_document(3, ["o.parquet"]))["payload"]},
            {"simple": "lines", "version": 5, "ts": 1_700_000_000_005, "path": "snap-5.json",
             "payload": json.loads(_document(5, ["l.parquet"]))["payload"]},
        ]


class TestAReplicaLake:

    def _replica(self, tables=None):
        root = {"version": 1, "clone_type": "replica", "cloned_from": LAKE}
        if tables is not None:
            root["replica_tables"] = tables
        return {_root("mirror"): json.dumps(root)}

    def test_its_source_s_tables_are_each_listed_once(self):
        client = _Redis([[ORDERS, LINES, ORDERS], [LINES, CUSTOMERS]], self._replica())
        assert list(_catalog(client).scan_leaf_keys(ORG, "mirror")) == [ORDERS, LINES, CUSTOMERS]

    def test_the_tables_it_was_given_still_decide_what_is_listed(self):
        client = _Redis([[ORDERS, LINES, ORDERS], [LINES, CUSTOMERS, ORDERS]],
                        self._replica(tables=["orders", "customers"]))
        assert list(_catalog(client).scan_leaf_keys(ORG, "mirror")) == [ORDERS, CUSTOMERS]

    def test_a_table_it_was_not_given_does_not_appear_however_often_it_is_returned(self):
        client = _Redis([[LINES, LINES], [LINES]], self._replica(tables=["orders"]))
        assert list(_catalog(client).scan_leaf_keys(ORG, "mirror")) == []


# ---------------------------------------------------------------------------
# A read that still lists its lake
# ---------------------------------------------------------------------------

def _estimate(client, tables):
    """The real ``estimate`` over a real catalog on *client*."""
    test_settings = dataclasses.replace(
        settings, SUPERTABLE_DUCKDB_PRESIGNED=False, STORAGE_ENDPOINT_URL="")
    with patch.object(data_estimator_mod, "RedisCatalog", return_value=_catalog(client)), \
         patch.object(data_estimator_mod, "SuperTable", MagicMock()), \
         patch.object(data_estimator_mod, "settings", test_settings):
        estimator = DataEstimator(organization=ORG, storage=LocalStorage(), tables=tables)
        return estimator, estimator.estimate()


class TestAReadThatListsItsLake:
    """Reads fetch their tables by name and never list. Two still do: a table
    named like its SuperTable (every table of the lake), and a table the named
    fetch has no snapshot for."""

    def _values(self):
        return {ORDERS: _document(3, ["o1.parquet", "o2.parquet"]),
                LINES: _document(5, ["l1.parquet"])}

    def test_every_table_of_the_lake_is_read_once(self):
        client = _Redis([[ORDERS, LINES, ORDERS], [LINES, ORDERS]], self._values())
        _, reflection = _estimate(client, [TableDefinition(LAKE, LAKE, LAKE, columns=[])])
        assert sorted(reflection.supers[0].files) == ["l1.parquet", "o1.parquet", "o2.parquet"]
        assert reflection.total_reflections == 3
        assert reflection.reflection_bytes == 30

    def test_a_lake_of_one_table_listed_twice_is_that_table_once(self):
        client = _Redis([[ORDERS, ORDERS]], {ORDERS: _document(3, ["o1.parquet"])})
        _, reflection = _estimate(client, [TableDefinition(LAKE, LAKE, LAKE, columns=[])])
        assert list(reflection.supers[0].files) == ["o1.parquet"]
        assert reflection.total_reflections == 1

    def test_a_table_found_only_by_the_listing_is_read_once(self):
        """A leaf the named fetch cannot reach (keyed by hand, in another
        case). Listed once it is one snapshot, and so the one the reader is
        handed for the deletion vector; listed twice it was two, and none."""
        by_hand = RK.meta_leaf_pattern(ORG, LAKE)[:-1] + "Orders"
        client = _Redis([[by_hand, by_hand], [by_hand]],
                        {by_hand: _document(8, ["hand-1.parquet", "hand-2.parquet"])})
        estimator, reflection = _estimate(client, [TableDefinition(LAKE, "orders", "orders", columns=[])])
        assert list(reflection.supers[0].files) == ["hand-1.parquet", "hand-2.parquet"]
        assert reflection.total_reflections == 2
        assert list(estimator.resolved_snapshots) == [(LAKE, "orders")]


# ---------------------------------------------------------------------------
# Lakes and tables, as meta_reader lists them
# ---------------------------------------------------------------------------

_META = "supertable.meta_reader"


class TestLakeAndTableListings:

    def _scanning(self, mock_catalog_class, *pages):
        mock_catalog_class.return_value.r.scan.side_effect = _pages(*pages)

    @patch(f"{_META}.RedisCatalog")
    def test_each_key_is_returned_once_in_the_order_first_seen(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        self._scanning(MockCat, [ORDERS, LINES, ORDERS], [LINES.encode(), CUSTOMERS], [ORDERS])
        assert _get_redis_items("pattern") == [ORDERS, LINES, CUSTOMERS]

    @patch(f"{_META}.RedisCatalog")
    def test_a_listing_with_no_repeat_is_what_it_was(self, MockCat):
        from supertable.meta_reader import _get_redis_items
        self._scanning(MockCat, [ORDERS, LINES], [CUSTOMERS])
        assert _get_redis_items("pattern") == [ORDERS, LINES, CUSTOMERS]

    @patch(f"{_META}.check_meta_access")
    @patch(f"{_META}.RedisCatalog")
    def test_a_table_is_named_once(self, MockCat, mock_check):
        from supertable.meta_reader import list_tables
        mock_check.return_value = None
        self._scanning(MockCat, [ORDERS, LINES, ORDERS], [LINES, CUSTOMERS, ORDERS])
        assert list_tables(ORG, LAKE, role_name="superadmin") == ["customers", "lines", "orders"]
        # And asked about once: one META check per table, not per returned key.
        assert mock_check.call_count == 3

    @patch(f"{_META}.check_meta_access")
    @patch(f"{_META}.RedisCatalog")
    def test_a_lake_is_named_once(self, MockCat, mock_check):
        from supertable.meta_reader import list_supers
        mock_check.return_value = None
        self._scanning(MockCat, [_root("sales"), _root("ops")], [_root("sales")], [_root("ops"), _root("hr")])
        assert list_supers(ORG, role_name="superadmin") == ["hr", "ops", "sales"]
        assert mock_check.call_count == 3

    @patch(f"{_META}.check_meta_access")
    @patch(f"{_META}.RedisCatalog")
    def test_a_table_the_role_may_not_see_stays_out(self, MockCat, mock_check):
        from supertable.meta_reader import list_tables

        def check(super_name, organization, role_name, table_name):
            if table_name == "lines":
                raise PermissionError("no META on lines")

        mock_check.side_effect = check
        self._scanning(MockCat, [ORDERS, LINES, LINES], [LINES, ORDERS])
        assert list_tables(ORG, LAKE, role_name="reader") == ["orders"]


# ---------------------------------------------------------------------------
# Read-only clones
# ---------------------------------------------------------------------------

class TestReadOnlyClones:

    def _values(self):
        clone = {"version": 1, "read_only": True, "cloned_from": "sales"}
        return {
            _root("sales"): json.dumps({"version": 9}),
            _root("sales_copy"): json.dumps(clone),
            _root("sales_snap"): json.dumps(clone),
            _root("of_another"): json.dumps({"version": 1, "read_only": True, "cloned_from": "ops"}),
            _root("writable"): json.dumps({"version": 1, "cloned_from": "sales"}),
        }

    def test_a_clone_is_named_once(self):
        client = _Redis(
            [[_root("sales_copy"), _root("sales"), _root("sales_copy")],
             [_root("of_another"), _root("sales_snap"), _root("sales_copy")],
             [_root("writable"), _root("sales_snap")]],
            self._values())
        assert _catalog(client).find_readonly_clones(ORG, "sales") == ["sales_copy", "sales_snap"]

    def test_each_root_is_read_once(self):
        client = _Redis([[_root("sales_copy"), _root("sales_copy")], [_root("sales_copy")]],
                        self._values())
        _catalog(client).find_readonly_clones(ORG, "sales")
        assert client.fetched == [_root("sales_copy")]

    def test_a_listing_with_no_repeat_is_what_it_was(self):
        client = _Redis(
            [[_root("sales"), _root("sales_copy")], [_root("of_another"), _root("writable")],
             [_root("sales_snap")]],
            self._values())
        assert _catalog(client).find_readonly_clones(ORG, "sales") == ["sales_copy", "sales_snap"]


# ---------------------------------------------------------------------------
# The stand-in catalog of redis_infra
# ---------------------------------------------------------------------------

@pytest.fixture
def fallback_catalog(monkeypatch):
    """``redis_infra._FallbackCatalog``, the catalog used when ``RedisCatalog``
    cannot be built.

    Importing ``redis_infra`` builds a catalog and a client, so the import
    runs against stub settings and no network (as
    ``test_redis_connection_config`` does); the module that was loaded before
    is put back afterwards.
    """
    name = "supertable.redis_infra"
    previous = sys.modules.pop(name, None)
    monkeypatch.setattr(cfg, "settings", dataclasses.replace(
        cfg.settings, SUPERTABLE_ORGANIZATION="acme", SUPERTABLE_SUPERUSER_TOKEN="t",
        SUPERTABLE_LOGIN_MASK=1, SUPERTABLE_REDIS_URL="", SUPERTABLE_REDIS_SENTINEL=False,
        SUPERTABLE_REDIS_USERNAME="", SUPERTABLE_REDIS_PASSWORD="", SUPERTABLE_REDIS_SSL=False,
    ))
    monkeypatch.setattr(redis, "Redis", MagicMock())
    try:
        yield importlib.import_module(name)._FallbackCatalog
    finally:
        sys.modules.pop(name, None)
        if previous is not None:
            sys.modules[name] = previous


class TestTheStandInCatalog:

    def test_each_table_is_listed_once(self, fallback_catalog):
        client = _Redis([[ORDERS, LINES.encode(), ORDERS], [LINES, CUSTOMERS], [ORDERS.encode()]])
        assert list(fallback_catalog(client).scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES, CUSTOMERS]

    def test_each_table_is_one_item_fetched_once(self, fallback_catalog):
        values = {ORDERS: _document(3, ["o.parquet"]), LINES: _document(5, ["l.parquet"])}
        client = _Redis([[ORDERS, LINES], [ORDERS], [LINES, ORDERS]], values)
        items = list(fallback_catalog(client).scan_leaf_items(ORG, LAKE, count=2))
        assert [(i["simple"], i["version"]) for i in items] == [("orders", 3), ("lines", 5)]
        assert client.leaves_fetched() == [ORDERS, LINES]

    def test_a_listing_with_no_repeat_is_what_it_was(self, fallback_catalog):
        client = _Redis([[ORDERS, LINES], [CUSTOMERS]])
        assert list(fallback_catalog(client).scan_leaf_keys(ORG, LAKE)) == [ORDERS, LINES, CUSTOMERS]
