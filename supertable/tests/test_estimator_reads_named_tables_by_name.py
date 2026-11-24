"""``DataEstimator`` fetches the tables a query names; it no longer lists the lake.

Every read used to begin with ``scan_leaf_items``: a SCAN of the whole Redis
database for the lake's leaves, a fetch and decode of every one of them, and
then ``_filter_snapshots`` to keep the table the query named. A one-row lookup
in a lake of 1,000 tables decoded 1,000 snapshots, and the SCAN grew with every
other lake and every unrelated key in the database.

``estimate`` now asks the catalog for the named tables' leaves
(``RedisCatalog.get_leaf_items``). The listing is still there, and still what
answers:

  * a table named like its SuperTable, the legacy way to ask for every table;
  * any table the named fetch has no snapshot for, so that what a read finds,
    and how it fails when there is nothing, is decided where it always was.

The change is only sound if nothing a caller can observe differs. So each case
here runs ``estimate`` twice over the same catalog, by name and with the named
fetch switched off (which is the code path every read took before), and
compares everything the estimate hands on: the tables, their files in order,
columns, types, versions, sizes, freshness, the snapshots kept for the reader,
the plan statistics, or the error.

The catalog is the real ``RedisCatalog`` on fakeredis.  ``get_leaf_items``
itself is pinned in ``test_catalog_named_leaf_fetch.py``; a real writer, reader
and engine run the same comparison in
``tests/characterization/test_named_tables_are_read_without_listing_the_lake_end_to_end.py``.
"""

from __future__ import annotations

import dataclasses
import json
import random
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import fakeredis
import pytest
import redis

from supertable import redis_catalog
from supertable import redis_keys as RK
from supertable.config.settings import settings
from supertable.data_classes import TableDefinition
from supertable.engine.data_estimator import DataEstimator
from supertable.storage.local_storage import LocalStorage
from supertable.utils.sql_parser import SQLParser

# ``supertable.engine`` is shadowed by the ``Engine`` enum at the package
# level; the module is fetched by name, as test_read_pruning_integration does.
data_estimator_mod = sys.modules["supertable.engine.data_estimator"]

ORG = "intorg"
LAKE = "maindb"
OTHER = "otherdb"


# ---------------------------------------------------------------------------
# A catalog to estimate against
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    return fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True)


@pytest.fixture
def catalog(client, monkeypatch):
    monkeypatch.setattr(
        redis_catalog, "RedisConnector", lambda options=None: SimpleNamespace(r=client))
    return redis_catalog.RedisCatalog()


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


def _put(client, sup: str, simple: str, document: str) -> None:
    """A leaf as every writer stores one: under the key ``redis_keys`` builds."""
    client.set(RK.meta_leaf(ORG, sup, simple), document)


def _put_raw(client, sup: str, name: str, document: str) -> None:
    """A leaf under a name ``redis_keys`` refuses: put there by hand, never by a writer."""
    client.set(RK.meta_leaf_pattern(ORG, sup)[:-1] + name, document)


def _replica(client, name: str, source: str, tables=None) -> None:
    root = {"version": 1, "ts": 1, "clone_type": "replica", "cloned_from": source}
    if tables is not None:
        root["replica_tables"] = tables
    client.set(RK.meta_root(ORG, name), json.dumps(root))


def _tables(query: str, lake: str = LAKE):
    return SQLParser(lake, query, "duckdb").get_physical_tables()


def _table(simple: str, lake: str = LAKE, columns=()) -> TableDefinition:
    return TableDefinition(super_name=lake, simple_name=simple, alias=simple,
                           columns=list(columns))


# ---------------------------------------------------------------------------
# One estimate, and everything it hands on
# ---------------------------------------------------------------------------

def _estimator(tables, *, list_the_lake=False):
    """An estimator for *tables*; build and run it inside :class:`_patched`,
    which is what gives it its catalog."""
    estimator = DataEstimator(organization=ORG, storage=LocalStorage(), tables=tables)
    if list_the_lake:
        # The path every read took before: no named fetch, the lake listed.
        estimator._named_snapshots = lambda *args, **kwargs: None
    return estimator


class _patched:
    """The estimator's seams: the catalog under test, storage snapshots from
    *on_disk*, and URL resolution pinned to local paths."""

    def __init__(self, catalog, on_disk=None):
        super_table = MagicMock()
        super_table.return_value.read_simple_table_snapshot.side_effect = (
            lambda path: (on_disk or {})[path])
        test_settings = dataclasses.replace(
            settings, SUPERTABLE_DUCKDB_PRESIGNED=False, STORAGE_ENDPOINT_URL="")
        self._patches = [
            patch.object(data_estimator_mod, "RedisCatalog", return_value=catalog),
            patch.object(data_estimator_mod, "SuperTable", super_table),
            patch.object(data_estimator_mod, "settings", test_settings),
        ]

    def __enter__(self):
        for p in self._patches:
            p.start()
        return self

    def __exit__(self, *exc):
        for p in reversed(self._patches):
            p.stop()
        return False


def _outcome(catalog, tables, on_disk=None, *, list_the_lake=False) -> dict:
    """Everything a caller can observe of one ``estimate()``."""
    with _patched(catalog, on_disk):
        estimator = _estimator(tables, list_the_lake=list_the_lake)
        try:
            reflection = estimator.estimate()
        except Exception as error:  # noqa: BLE001 - the error IS the outcome
            return {"raised": type(error).__name__, "message": str(error)}
        return {
            "tables": [
                {"super": s.super_name, "simple": s.simple_name,
                 "version": s.simple_version, "files": list(s.files),
                 "columns": sorted(s.columns), "types": dict(s.column_types)}
                for s in reflection.supers],
            "bytes": reflection.reflection_bytes,
            "files": reflection.total_reflections,
            "freshness_ms": reflection.freshness_ms,
            "storage": reflection.storage_type,
            "kept": {f"{sup}.{simple}": snapshot
                     for (sup, simple), snapshot in estimator.resolved_snapshots.items()},
            "plan": [entry for entry in estimator.plan_stats.stats
                     if "PRUNE_DURATION_MS" not in entry],
        }


def _both(catalog, tables, on_disk=None):
    """``(by name, by listing the lake)``."""
    return (_outcome(catalog, tables, on_disk),
            _outcome(catalog, tables, on_disk, list_the_lake=True))


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
        seen.append("PIPELINE")
        return real_pipeline(*args, **kwargs)

    monkeypatch.setattr(client, "execute_command", execute_command)
    monkeypatch.setattr(client, "pipeline", pipeline)
    return seen


# ---------------------------------------------------------------------------
# The cases
# ---------------------------------------------------------------------------

def _a_lake(client) -> None:
    """Three tables, an internal one, and the same names in another lake."""
    _put(client, LAKE, "orders", _leaf(
        _snapshot(7, ["o1.parquet", "o2.parquet"], tombstone=["dv-0.parquet"]),
        version=7, ts=1_700_000_000_700))
    _put(client, LAKE, "lines", _leaf(
        _snapshot(5, ["l1.parquet"], columns=("id", "qty")), version=5, ts=1_700_000_000_500))
    _put(client, LAKE, "customers", _leaf(
        _snapshot(3, ["c1.parquet", "c2.parquet", "c3.parquet"], columns=("id", "name")),
        version=3, ts=1_700_000_000_300))
    _put(client, LAKE, "__data_quality__", _leaf(
        _snapshot(2, ["dq.parquet"], columns=("id", "rule")), version=2, ts=1_700_000_000_900))
    _put(client, OTHER, "orders", _leaf(
        _snapshot(40, ["other-o.parquet"]), version=40, ts=1_700_000_004_000))
    _put(client, OTHER, "customers", _leaf(
        _snapshot(41, ["other-c.parquet"], columns=("id", "name")), version=41,
        ts=1_700_000_004_100))
    for i in range(25):
        _put(client, LAKE, f"filler_{i:02d}", _leaf(_snapshot(1, [f"f{i}.parquet"])))


def _case_one_table(client):
    _a_lake(client)
    return _tables("SELECT * FROM orders"), None


def _case_named_columns(client):
    _a_lake(client)
    return _tables("SELECT id FROM orders WHERE val = 'x'"), None


def _case_a_join(client):
    _a_lake(client)
    return _tables("SELECT o.id, l.qty FROM orders o JOIN lines l ON o.id = l.id"), None


def _case_three_tables(client):
    _a_lake(client)
    return _tables("SELECT * FROM orders o JOIN lines l ON o.id = l.id "
                   "JOIN customers c ON c.id = o.id"), None


def _case_a_table_twice(client):
    _a_lake(client)
    return _tables("SELECT a.id FROM orders a JOIN orders b ON a.id = b.id"), None


def _case_two_lakes(client):
    _a_lake(client)
    return _tables(f"SELECT * FROM orders o JOIN {OTHER}.customers c ON o.id = c.id"), None


def _case_one_name_in_two_lakes(client):
    _a_lake(client)
    return _tables(f"SELECT a.id FROM orders a JOIN {OTHER}.orders b ON a.id = b.id"), None


def _case_an_internal_table(client):
    _a_lake(client)
    return _tables("SELECT * FROM __data_quality__"), None


def _case_a_cte_over_a_table(client):
    _a_lake(client)
    return _tables("WITH recent AS (SELECT id FROM orders) SELECT * FROM recent"), None


def _case_a_leaf_with_only_a_path(client):
    on_disk = {"lake/orders/v9.json": _snapshot(9, ["disk.parquet"], tombstone=["dv-9.parquet"])}
    _put(client, LAKE, "orders", _leaf(path="lake/orders/v9.json", version=9))
    return _tables("SELECT * FROM orders"), on_disk


def _case_an_inline_snapshot_without_resources(client):
    on_disk = {"lake/orders/v9.json": _snapshot(9, ["disk.parquet"])}
    _put(client, LAKE, "orders", _leaf(
        {"stats_file": "s.parquet", "tombstone": ["stale"]}, path="lake/orders/v9.json"))
    return _tables("SELECT * FROM orders"), on_disk


def _case_an_empty_table(client):
    _put(client, LAKE, "orders", _leaf(_snapshot(4, [])))
    return _tables("SELECT id FROM orders"), None


def _case_no_files_and_no_schema(client):
    _put(client, LAKE, "orders", _leaf({"snapshot_version": 4, "schema": {}, "resources": []}))
    return _tables("SELECT * FROM orders"), None


def _case_a_table_that_is_not_there(client):
    _a_lake(client)
    return _tables("SELECT * FROM nothere"), None


def _case_an_empty_lake(client):
    return _tables("SELECT * FROM orders"), None


def _case_one_of_two_is_not_there(client):
    _a_lake(client)
    return _tables("SELECT * FROM orders o JOIN nothere n ON o.id = n.id"), None


def _case_one_of_two_is_not_there_and_its_columns_are_named(client):
    _a_lake(client)
    return _tables("SELECT o.id, n.x FROM orders o JOIN nothere n ON o.id = n.id"), None


def _case_a_column_that_is_not_there(client):
    _a_lake(client)
    return _tables("SELECT nope FROM orders"), None


def _case_a_leaf_with_no_path(client):
    _a_lake(client)
    _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o1.parquet"]), path=""))
    return _tables("SELECT * FROM orders"), None


def _case_a_leaf_that_is_not_json(client):
    _a_lake(client)
    _put(client, LAKE, "orders", "{not json")
    return _tables("SELECT * FROM orders"), None


def _case_a_leaf_that_is_not_a_document(client):
    _a_lake(client)
    _put(client, LAKE, "orders", "[1, 2]")
    return _tables("SELECT * FROM orders JOIN lines ON orders.id = lines.id"), None


def _case_numbers_stored_as_text(client):
    _put(client, LAKE, "orders", json.dumps({
        "version": "12", "ts": "1700000001200", "path": "p.json",
        "payload": _snapshot(12, ["o.parquet"])}))
    return _tables("SELECT * FROM orders"), None


def _case_a_name_in_another_case(client):
    """Only a direct caller can ask this; the reader refuses the name first."""
    _a_lake(client)
    return [_table("Orders")], None


def _case_one_table_spelled_two_ways(client):
    _a_lake(client)
    return [_table("orders", columns=["id"]), _table("ORDERS", columns=["val"])], None


def _case_a_name_no_leaf_can_have(client):
    _a_lake(client)
    return [_table("my table")], None


def _case_a_name_no_leaf_can_have_beside_a_real_one(client):
    _a_lake(client)
    return [_table("orders"), _table("order s", columns=["id"])], None


def _case_the_same_table_listed_twice(client):
    _a_lake(client)
    return [_table("orders"), _table("orders")], None


def _case_a_hand_written_leaf_alone(client):
    """No writer builds this key, but the listing matches it and so must this."""
    _a_lake(client)
    client.delete(RK.meta_leaf(ORG, LAKE, "orders"))
    _put_raw(client, LAKE, "Orders", _leaf(_snapshot(8, ["hand.parquet"]), version=8))
    return _tables("SELECT * FROM orders"), None


def _case_two_hand_written_leaves(client):
    _put_raw(client, LAKE, "Orders", _leaf(_snapshot(8, ["hand-1.parquet"]), version=8))
    _put_raw(client, LAKE, "ORDERS", _leaf(_snapshot(9, ["hand-2.parquet"]), version=9))
    return _tables("SELECT * FROM orders"), None


def _case_the_lake_s_own_name(client):
    """The legacy form: every table of the lake that is not internal."""
    _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"], tombstone=["dv.parquet"])))
    _put(client, LAKE, "lines", _leaf(_snapshot(5, ["l.parquet"], columns=("id", "qty"))))
    _put(client, LAKE, "__data_quality__", _leaf(_snapshot(2, ["dq.parquet"])))
    return _tables(f"SELECT * FROM {LAKE}"), None


def _case_the_lake_s_own_name_over_one_table(client):
    _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"], tombstone=["dv.parquet"])))
    return _tables(f"SELECT * FROM {LAKE}"), None


def _case_the_lake_s_own_name_with_a_table_of_that_name(client):
    _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"])))
    _put(client, LAKE, LAKE, _leaf(_snapshot(1, ["self.parquet"])))
    return _tables(f"SELECT * FROM {LAKE}"), None


def _case_the_lake_s_own_name_in_another_case(client):
    _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"])))
    _put(client, LAKE, "lines", _leaf(_snapshot(5, ["l.parquet"])))
    return [_table(LAKE.upper(), lake=LAKE)], None


def _case_the_lake_s_own_name_beside_a_table(client):
    _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"])))
    _put(client, LAKE, "lines", _leaf(_snapshot(5, ["l.parquet"], columns=("id", "qty"))))
    return _tables(f"SELECT * FROM {LAKE} m JOIN orders o ON m.id = o.id"), None


def _case_the_lake_s_own_name_over_an_empty_lake(client):
    return _tables(f"SELECT * FROM {LAKE}"), None


def _case_a_replica(client):
    _a_lake(client)
    _replica(client, "mirror", LAKE)
    _put(client, "mirror", "orders", _leaf(_snapshot(99, ["never.parquet"])))
    return _tables("SELECT * FROM orders o JOIN lines l ON o.id = l.id", lake="mirror"), None


def _case_a_replica_given_the_table(client):
    _a_lake(client)
    _replica(client, "mirror", LAKE, tables=["orders", "customers"])
    return _tables("SELECT * FROM orders", lake="mirror"), None


def _case_a_replica_not_given_the_table(client):
    _a_lake(client)
    _replica(client, "mirror", LAKE, tables=["orders", "customers"])
    return _tables("SELECT * FROM lines", lake="mirror"), None


def _case_a_replica_given_one_of_two(client):
    _a_lake(client)
    _replica(client, "mirror", LAKE, tables=["orders"])
    return _tables("SELECT o.id, l.qty FROM orders o JOIN lines l ON o.id = l.id",
                   lake="mirror"), None


def _case_a_replica_and_its_source_together(client):
    _a_lake(client)
    _replica(client, "mirror", LAKE, tables=["orders"])
    return _tables(f"SELECT * FROM orders a JOIN {LAKE}.lines b ON a.id = b.id",
                   lake="mirror"), None


def _case_a_replica_of_nothing(client):
    _replica(client, "mirror", "gone")
    _put(client, "mirror", "orders", _leaf(_snapshot(99, ["never.parquet"])))
    return _tables("SELECT * FROM orders", lake="mirror"), None


CASES = {name[len("_case_"):]: build for name, build in sorted(globals().items())
         if name.startswith("_case_")}


# ---------------------------------------------------------------------------
# By name, or by listing the lake: the same estimate
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("case", sorted(CASES))
def test_the_estimate_is_the_one_listing_the_lake_gave(case, catalog, client):
    tables, on_disk = CASES[case](client)
    by_name, by_listing = _both(catalog, tables, on_disk)
    assert by_name == by_listing


def test_the_cases_are_not_all_one_outcome(catalog, client):
    """A comparison of two errors proves little; most cases must really read."""
    read, refused = [], []
    for case in sorted(CASES):
        client.flushall()
        tables, on_disk = CASES[case](client)
        (read if "tables" in _outcome(catalog, tables, on_disk) else refused).append(case)
    assert len(read) >= 25, read
    assert len(refused) >= 8, refused


def test_random_lakes_and_queries(catalog, client):
    """Lakes, replicas and table lists nobody thought of: always the same estimate."""
    rng = random.Random(20261124)
    pool = [f"t{i:02d}" for i in range(16)] + ["orders", "__internal__", "a-b"]
    asked_pool = pool + ["Orders", "T03", "nothere", "no such", LAKE, "MAINDB"]
    documents = [
        lambda v: _leaf(_snapshot(v, [f"f{v}-a.parquet", f"f{v}-b.parquet"]), version=v,
                        ts=1_700_000_000_000 + v),
        lambda v: _leaf(_snapshot(v, [f"f{v}.parquet"], columns=("id", "qty")), version=v),
        lambda v: _leaf(_snapshot(v, []), version=v),
        lambda v: _leaf(path="on-disk.json", version=v),
        lambda v: _leaf(_snapshot(v, [f"f{v}.parquet"]), path=""),
        lambda v: "{not json",
    ]
    weights = [5, 3, 2, 2, 1, 1]           # mostly snapshots that can be read
    on_disk = {"on-disk.json": _snapshot(77, ["disk.parquet"])}
    agreed = {"read": 0, "refused": 0}
    set_aside = 0

    for round_number in range(250):
        client.flushall()
        present = {LAKE: rng.sample(pool, rng.randint(0, len(pool))),
                   OTHER: rng.sample(pool, rng.randint(0, 6))}
        for lake, first_version in ((LAKE, 1), (OTHER, 100)):
            for version, name in enumerate(present[lake], first_version):
                _put(client, lake, name, rng.choices(documents, weights)[0](version))
        hand_written = None
        if rng.random() < 0.15:
            hand_written = rng.choice(["Orders", "T03", "Nothere"])
            _put_raw(client, LAKE, hand_written,
                     _leaf(_snapshot(500, ["hand.parquet"]), version=500))
        if rng.random() < 0.4:
            given = rng.choice([None, [], rng.sample(pool, rng.randint(1, 8))])
            _replica(client, "mirror", LAKE, tables=given)
            present["mirror"] = [n for n in present[LAKE] if not given or n in given]

        def a_table(i):
            lake = rng.choice(sorted(present))
            if present[lake] and rng.random() < 0.75:       # a table that is there
                return TableDefinition(
                    super_name=lake, simple_name=rng.choice(present[lake]),
                    alias=f"a{i}", columns=rng.choice([[], [], ["id"]]))
            return TableDefinition(                         # anything at all
                super_name=lake, simple_name=rng.choice(asked_pool),
                alias=f"a{i}", columns=rng.choice([[], ["id"], ["id", "val"], ["nope"]]))

        tables = [a_table(i) for i in range(rng.randint(1, 4))]

        # A hand-written leaf differing only by case from a writer's is the one
        # catalog state the two disagree on (TestALeafNoWriterCouldHaveMade).
        if hand_written and hand_written.lower() in present[LAKE]:
            set_aside += 1
            continue

        by_name, by_listing = _both(catalog, tables, on_disk)
        assert by_name == by_listing, (round_number, tables)
        agreed["read" if "tables" in by_name else "refused"] += 1

    assert agreed["read"] >= 60 and agreed["refused"] >= 60, agreed
    assert set_aside < 25, set_aside


# ---------------------------------------------------------------------------
# What is kept for the reader
# ---------------------------------------------------------------------------

class TestTheSnapshotKeptForTheReader:
    """The deletion vector and a share's row filter are taken from the
    snapshot the files were listed from (see
    ``test_read_takes_its_controls_from_one_snapshot.py``). A table fetched by
    name keeps its snapshot exactly as a listed one does."""

    def test_a_named_table_keeps_the_snapshot_its_files_came_from(self, catalog, client):
        _a_lake(client)
        with _patched(catalog):
            estimator = _estimator(_tables("SELECT * FROM orders"))
            reflection = estimator.estimate()
        kept = estimator.resolved_snapshots
        assert list(kept) == [(LAKE, "orders")]
        assert kept[(LAKE, "orders")]["tombstone"] == ["dv-0.parquet"]
        assert [r["file"] for r in kept[(LAKE, "orders")]["resources"]] == list(
            reflection.supers[0].files)

    def test_each_table_of_a_join_keeps_its_own(self, catalog, client):
        _a_lake(client)
        with _patched(catalog):
            estimator = _estimator(_tables(
                "SELECT * FROM orders o JOIN lines l ON o.id = l.id"))
            estimator.estimate()
        assert sorted(estimator.resolved_snapshots) == [(LAKE, "lines"), (LAKE, "orders")]
        assert estimator.resolved_snapshots[(LAKE, "orders")]["snapshot_version"] == 7
        assert estimator.resolved_snapshots[(LAKE, "lines")]["snapshot_version"] == 5

    def test_a_snapshot_read_from_its_path_is_the_one_kept(self, catalog, client):
        on_disk = {"v9.json": _snapshot(9, ["disk.parquet"], tombstone=["dv-9.parquet"])}
        _put(client, LAKE, "orders", _leaf(path="v9.json", version=9))
        with _patched(catalog, on_disk):
            estimator = _estimator(_tables("SELECT * FROM orders"))
            estimator.estimate()
        assert estimator.resolved_snapshots[(LAKE, "orders")] is on_disk["v9.json"]

    def test_the_lake_s_own_name_still_keeps_none(self, catalog, client):
        _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"], tombstone=["dv.parquet"])))
        with _patched(catalog):
            estimator = _estimator(_tables(f"SELECT * FROM {LAKE}"))
            reflection = estimator.estimate()
        assert reflection.supers[0].simple_name == LAKE
        assert estimator.resolved_snapshots == {}


# ---------------------------------------------------------------------------
# What it asks Redis
# ---------------------------------------------------------------------------

class TestWhatAnEstimateAsksTheCatalog:

    def test_one_table_is_the_root_and_one_round_trip(self, catalog, client, commands):
        _a_lake(client)
        for i in range(500):
            client.set(f"unrelated:{i}", "x")
        del commands[:]

        outcome = _outcome(catalog, _tables("SELECT * FROM orders"))

        assert outcome["tables"][0]["files"] == ["o1.parquet", "o2.parquet"]
        assert commands == ["GET", "PIPELINE"]

    def test_the_same_read_used_to_scan_the_database(self, catalog, client, commands):
        _a_lake(client)
        for i in range(500):
            client.set(f"unrelated:{i}", "x")
        del commands[:]

        _outcome(catalog, _tables("SELECT * FROM orders"), list_the_lake=True)

        assert commands.count("SCAN") >= 1
        assert commands[0] == "GET" and commands[-1] == "PIPELINE"

    def test_a_join_fetches_its_tables_together(self, catalog, client, commands):
        """One round trip for all of them, and so one moment in the catalog."""
        _a_lake(client)
        del commands[:]
        outcome = _outcome(catalog, _tables(
            "SELECT * FROM orders o JOIN lines l ON o.id = l.id JOIN customers c ON c.id = o.id"))
        assert [t["simple"] for t in outcome["tables"]] == ["customers", "lines", "orders"]
        assert commands == ["GET", "PIPELINE"]

    def test_each_lake_of_a_query_is_asked_once(self, catalog, client, commands):
        _a_lake(client)
        del commands[:]
        outcome = _outcome(catalog, _tables(
            f"SELECT * FROM orders o JOIN {OTHER}.customers c ON o.id = c.id"))
        assert len(outcome["tables"]) == 2
        assert commands == ["GET", "PIPELINE", "GET", "PIPELINE"]

    def test_a_name_in_another_case_is_still_fetched_by_name(self, catalog, client, commands):
        """The listing matched names case-insensitively; the one leaf that can
        match is the lowercase one, and that is the key asked for."""
        _a_lake(client)
        del commands[:]
        outcome = _outcome(catalog, [_table("Orders"), _table("LINES")])
        assert [(t["simple"], t["files"]) for t in outcome["tables"]] == [
            ("LINES", ["l1.parquet"]), ("Orders", ["o1.parquet", "o2.parquet"])]
        assert commands == ["GET", "PIPELINE"]

    def test_never_more_round_trips_than_listing_the_lake(self, catalog, client, commands):
        """Even where listing is at its cheapest: a lake of one table and
        nothing else in the database."""
        _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"])))
        del commands[:]
        _outcome(catalog, _tables("SELECT * FROM orders"))
        by_name = list(commands)
        del commands[:]
        _outcome(catalog, _tables("SELECT * FROM orders"), list_the_lake=True)
        by_listing = list(commands)
        assert by_name == ["GET", "PIPELINE"]
        assert by_listing == ["GET", "SCAN", "PIPELINE"]

    def test_the_lake_s_own_name_lists_and_fetches_nothing_by_name(
            self, catalog, client, commands):
        _put(client, LAKE, "orders", _leaf(_snapshot(7, ["o.parquet"])))
        _put(client, LAKE, "lines", _leaf(_snapshot(5, ["l.parquet"])))
        del commands[:]
        outcome = _outcome(catalog, _tables(f"SELECT * FROM {LAKE}"))
        assert sorted(outcome["tables"][0]["files"]) == ["l.parquet", "o.parquet"]
        assert commands == ["GET", "SCAN", "PIPELINE"]

    def test_a_table_that_is_not_there_is_looked_for_in_the_listing_once(
            self, catalog, client, commands):
        """The listing decides what a miss means, as it always did; and it is
        listed once for the lake, not once per table."""
        _a_lake(client)
        del commands[:]
        outcome = _outcome(catalog, _tables(
            "SELECT o.id, a.x, b.y FROM orders o JOIN nothere a ON o.id = a.id "
            "JOIN neither b ON o.id = b.id"))
        assert outcome["raised"] == "RuntimeError"
        assert commands[:2] == ["GET", "PIPELINE"]          # by name: two are missing
        assert commands[2:].count("GET") == 1                # then one listing
        assert commands[2:].count("SCAN") >= 1
        assert commands[2:].count("PIPELINE") == 1

    def test_a_catalog_that_cannot_fetch_by_name_is_listed(self, catalog, client):
        """A stand-in without ``get_leaf_items`` gets the read it always got."""
        _a_lake(client)
        listing_only = SimpleNamespace(scan_leaf_items=catalog.scan_leaf_items)
        with _patched(listing_only):
            estimator = DataEstimator(
                organization=ORG, storage=LocalStorage(), tables=_tables("SELECT * FROM orders"))
            reflection = estimator.estimate()
        assert list(reflection.supers[0].files) == ["o1.parquet", "o2.parquet"]
        assert list(estimator.resolved_snapshots) == [(LAKE, "orders")]

    def test_a_named_fetch_that_answers_nothing_is_listed(self, client):
        """``MagicMock`` catalogs, as the older estimator tests use them, answer
        a named fetch with nothing; they are listed, as before."""
        stand_in = MagicMock()
        stand_in.scan_leaf_items.return_value = [
            {"simple": "orders", "path": "snap.json", "version": 1, "ts": 5,
             "payload": _snapshot(1, ["a.parquet"])}]
        outcome = _outcome(stand_in, _tables("SELECT * FROM orders"))
        assert outcome["tables"][0]["files"] == ["a.parquet"]
        stand_in.get_leaf_items.assert_called_once_with(ORG, LAKE, ["orders"])
        stand_in.scan_leaf_items.assert_called_once_with(ORG, LAKE, count=512)

    @pytest.mark.parametrize("answer", [
        [{"simple": "lines", "path": "p.json", "version": 1, "ts": 1,
          "payload": {"resources": [{"file": "wrong.parquet"}], "schema": {"id": "BIGINT"}}}],
        [{"simple": "orders", "path": "p.json", "version": 1, "ts": 1, "payload": None},
         {"simple": "orders", "path": "q.json", "version": 2, "ts": 2, "payload": None}],
        [{"simple": "ORDERS", "path": "p.json", "version": 1, "ts": 1, "payload": None}],
        [{"simple": "orders", "path": "", "version": 1, "ts": 1, "payload": None}],
    ], ids=["another-table", "the-table-twice", "another-case", "no-path"])
    def test_an_answer_that_is_not_one_snapshot_per_table_is_not_used(self, answer):
        """Only exactly the named table's one snapshot stands in for the listing."""
        listed = [{"simple": "orders", "path": "snap.json", "version": 1, "ts": 5,
                   "payload": _snapshot(1, ["listed.parquet"])}]
        stand_in = MagicMock()
        stand_in.get_leaf_items.return_value = answer
        stand_in.scan_leaf_items.return_value = listed
        outcome = _outcome(stand_in, _tables("SELECT * FROM orders"))
        assert outcome["tables"][0]["files"] == ["listed.parquet"]
        stand_in.scan_leaf_items.assert_called_once()


# ---------------------------------------------------------------------------
# Failure
# ---------------------------------------------------------------------------

class TestWhenTheCatalogCannotAnswer:

    def test_a_redis_error_fails_the_estimate_with_that_error(self, catalog, client, monkeypatch):
        """It used to be read as an empty lake: "No snapshots selected."."""
        _a_lake(client)

        def unreachable(*args, **kwargs):
            raise redis.exceptions.TimeoutError("Timeout reading from socket")

        monkeypatch.setattr(client, "pipeline", unreachable)
        outcome = _outcome(catalog, _tables("SELECT * FROM orders"))
        assert outcome == {"raised": "TimeoutError", "message": "Timeout reading from socket"}

    def test_the_listing_it_replaced_swallowed_the_same_error(self, catalog, client, monkeypatch):
        """Sealed for contrast: no read that succeeded before can fail now,
        because the listing needed the same pipeline to answer."""
        _a_lake(client)

        def unreachable(*args, **kwargs):
            raise redis.exceptions.TimeoutError("Timeout reading from socket")

        monkeypatch.setattr(client, "pipeline", unreachable)
        outcome = _outcome(catalog, _tables("SELECT * FROM orders"), list_the_lake=True)
        assert outcome == {"raised": "RuntimeError", "message": "No snapshots selected."}

    def test_a_root_that_cannot_be_read_fails_both_the_same_way(self, catalog, client, monkeypatch):
        _a_lake(client)

        def unreachable(*args, **kwargs):
            raise redis.exceptions.ConnectionError("down")

        monkeypatch.setattr(client, "get", unreachable)
        tables = _tables("SELECT * FROM orders")
        by_name, by_listing = _both(catalog, tables)
        assert by_name == by_listing == {"raised": "ConnectionError", "message": "down"}

    @pytest.mark.parametrize("lake", ["MainDB", "my lake", "_reserved_"])
    def test_a_lake_name_no_key_can_be_built_from_fails_both_the_same_way(
            self, catalog, client, lake):
        _a_lake(client)
        tables = [_table("orders", lake=lake)]
        by_name, by_listing = _both(catalog, tables)
        assert by_name == by_listing
        assert by_name["raised"] == "ValueError"


# ---------------------------------------------------------------------------
# The one state the two disagree on
# ---------------------------------------------------------------------------

class TestALeafNoWriterCouldHaveMade:
    """``_filter_snapshots`` matches names case-insensitively, so the listing
    read a leaf keyed ``Orders`` as part of the table ``orders`` and merged the
    files of the two. ``redis_keys`` has never built a leaf key with an
    uppercase letter: such a key is put into Redis by hand, cannot be read or
    written by its own name (the reader and the writer refuse the name), and is
    not a table.

    A read by name fetches ``orders`` and does not go looking for it. This is
    the one catalog state in which the estimate differs from the listing's."""

    def _lake(self, client):
        _put(client, LAKE, "orders", _leaf(_snapshot(7, ["real.parquet"]), version=7))
        _put_raw(client, LAKE, "Orders", _leaf(_snapshot(8, ["hand.parquet"]), version=8))

    def test_by_name_the_table_is_its_own_files(self, catalog, client):
        self._lake(client)
        outcome = _outcome(catalog, _tables("SELECT * FROM orders"))
        assert outcome["tables"][0]["files"] == ["real.parquet"]
        assert outcome["tables"][0]["version"] == 7
        assert list(outcome["kept"]) == [f"{LAKE}.orders"]

    def test_the_listing_merged_the_two(self, catalog, client):
        self._lake(client)
        outcome = _outcome(catalog, _tables("SELECT * FROM orders"), list_the_lake=True)
        assert sorted(outcome["tables"][0]["files"]) == ["hand.parquet", "real.parquet"]
        assert outcome["kept"] == {}

    def test_without_a_real_table_of_that_name_it_is_still_found(self, catalog, client):
        """Nothing by name, so the listing decides, and finds what it found before."""
        _put_raw(client, LAKE, "Orders", _leaf(_snapshot(8, ["hand.parquet"]), version=8))
        by_name, by_listing = _both(catalog, _tables("SELECT * FROM orders"))
        assert by_name == by_listing
        assert by_name["tables"][0]["files"] == ["hand.parquet"]
