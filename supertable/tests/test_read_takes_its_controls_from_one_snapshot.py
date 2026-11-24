"""A read takes its deletion vector and share filter from the snapshot its
files came from.

``DataEstimator.estimate`` lists a table's files from a snapshot.
``DataReader.execute`` then builds, per table alias, the deletion-vector view
and any share row filter. Both live in that same snapshot, and both were
fetched from the catalog a second time, so a commit between the two fetches
gave one read the files of one snapshot and the vector of the next
(``tests/characterization/test_read_is_one_snapshot_end_to_end.py`` shows what
that returns).

Two halves are pinned here, without Redis or an engine:

  * the estimator keeps the snapshot it listed each table's files from, and
    keeps none when the files came from more than one;
  * the reader uses that snapshot, and asks the catalog only for an alias the
    estimator kept nothing for, which is what it did for every alias before.
"""

from __future__ import annotations

import dataclasses
import sys
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from supertable.config.settings import settings
from supertable.data_classes import RbacViewDef, Reflection, SuperSnapshot
from supertable.data_reader import DataReader, Status
from supertable.engine.data_estimator import DataEstimator
from supertable.storage.local_storage import LocalStorage
from supertable.utils.sql_parser import SQLParser

# ``supertable.engine`` is shadowed by the ``Engine`` enum at the package
# level; the module is fetched by name, as test_read_pruning_integration does.
data_estimator_mod = sys.modules["supertable.engine.data_estimator"]

_ORG = "intorg"
_SUPER = "maindb"


# ---------------------------------------------------------------------------
# The estimator keeps the snapshot it listed the files from
# ---------------------------------------------------------------------------

def _snapshot(version: int, files, **extra) -> dict:
    payload = {
        "snapshot_version": version,
        "schema": [{"name": "id", "type": "BIGINT"}],
        "resources": [{"file": f, "file_size": 10} for f in files],
    }
    payload.update(extra)
    return payload


def _leaf_item(simple: str, payload=None, path="snap.json", version=1) -> dict:
    item = {"simple": simple, "path": path, "version": version, "ts": 1_700_000_000_000}
    if payload is not None:
        item["payload"] = payload
    return item


def _estimate(leaf_items, query="SELECT * FROM orders", on_disk=None):
    """Run the real ``estimate`` over *leaf_items*; returns ``(estimator, reflection)``."""
    catalog = MagicMock()
    catalog.scan_leaf_items.return_value = leaf_items
    super_table = MagicMock()
    super_table.return_value.read_simple_table_snapshot.return_value = on_disk
    test_settings = dataclasses.replace(
        settings, SUPERTABLE_DUCKDB_PRESIGNED=False, STORAGE_ENDPOINT_URL="")
    with patch.object(data_estimator_mod, "RedisCatalog", return_value=catalog), \
         patch.object(data_estimator_mod, "SuperTable", super_table), \
         patch.object(data_estimator_mod, "settings", test_settings):
        estimator = DataEstimator(
            organization=_ORG, storage=LocalStorage(),
            tables=SQLParser(_SUPER, query, "duckdb").get_physical_tables())
        return estimator, estimator.estimate()


class TestTheEstimatorKeepsTheSnapshotItListedFrom:

    def test_a_new_estimator_has_kept_nothing(self):
        with patch.object(data_estimator_mod, "RedisCatalog"):
            estimator = DataEstimator(organization=_ORG, storage=MagicMock(), tables=[])
        assert estimator.resolved_snapshots == {}

    def test_an_inline_snapshot_is_kept_as_it_was_read(self):
        payload = _snapshot(7, ["a.parquet"], tombstone=["dv-0.parquet"])
        estimator, reflection = _estimate([_leaf_item("orders", payload)])
        kept = estimator.resolved_snapshots
        assert list(kept) == [(_SUPER, "orders")]
        # The very object the files were listed from, not a second read of it.
        assert kept[(_SUPER, "orders")] is payload
        assert reflection.supers[0].simple_version == 7

    def test_a_snapshot_read_from_its_path_is_the_one_kept(self):
        """A leaf with no inline snapshot: the files come from storage, and so
        does what is kept."""
        on_disk = _snapshot(9, ["b.parquet"], tombstone=["dv-9.parquet"])
        estimator, reflection = _estimate([_leaf_item("orders")], on_disk=on_disk)
        assert estimator.resolved_snapshots[(_SUPER, "orders")] is on_disk
        assert len(reflection.supers[0].files) == 1

    def test_an_inline_snapshot_the_estimator_bypasses_is_not_the_one_kept(self):
        """An inline copy without a ``resources`` list is not what the files
        are listed from, so it is not what is kept either."""
        on_disk = _snapshot(9, ["b.parquet"], tombstone=["dv-9.parquet"])
        estimator, _ = _estimate(
            [_leaf_item("orders", {"stats_file": "s.parquet", "tombstone": ["stale"]})],
            on_disk=on_disk)
        assert estimator.resolved_snapshots[(_SUPER, "orders")] is on_disk

    def test_each_table_of_a_query_keeps_its_own(self):
        orders = _snapshot(3, ["o.parquet"])
        lines = _snapshot(5, ["l.parquet"], tombstone=["dv-l.parquet"])
        estimator, _ = _estimate(
            [_leaf_item("orders", orders), _leaf_item("lines", lines),
             _leaf_item("unrelated", _snapshot(1, ["u.parquet"]))],
            query="SELECT * FROM orders o JOIN lines l ON o.id = l.id")
        assert estimator.resolved_snapshots == {
            (_SUPER, "orders"): orders, (_SUPER, "lines"): lines}

    def test_files_listed_from_two_snapshots_keep_none(self):
        """Two leaves answer to one table name; their files are merged into one
        reflection and no single snapshot describes it. Nothing is kept, and
        the reader asks the catalog as it did before."""
        estimator, reflection = _estimate([
            _leaf_item("orders", _snapshot(3, ["a.parquet"])),
            _leaf_item("ORDERS", _snapshot(4, ["b.parquet"], tombstone=["dv.parquet"])),
        ])
        assert len(reflection.supers[0].files) == 2
        assert estimator.resolved_snapshots == {}

    def test_files_listed_from_another_table_s_snapshot_keep_none(self):
        """The legacy branch: a table named like its SuperTable is given the
        files of the other tables. With one other table that is a single
        snapshot, but not this table's, so it is not kept as this table's."""
        estimator, reflection = _estimate(
            [_leaf_item("orders", _snapshot(3, ["a.parquet"], tombstone=["dv.parquet"]))],
            query=f"SELECT * FROM {_SUPER}")
        assert reflection.supers[0].simple_name == _SUPER
        assert len(reflection.supers[0].files) == 1
        assert estimator.resolved_snapshots == {}

    def test_a_second_estimate_does_not_hand_out_the_first_ones_snapshots(self):
        catalog = MagicMock()
        first = _snapshot(1, ["a.parquet"])
        second = _snapshot(2, ["b.parquet"])
        catalog.scan_leaf_items.side_effect = [
            [_leaf_item("orders", first)], [_leaf_item("orders", second)]]
        test_settings = dataclasses.replace(
            settings, SUPERTABLE_DUCKDB_PRESIGNED=False, STORAGE_ENDPOINT_URL="")
        with patch.object(data_estimator_mod, "RedisCatalog", return_value=catalog), \
             patch.object(data_estimator_mod, "SuperTable", MagicMock()), \
             patch.object(data_estimator_mod, "settings", test_settings):
            estimator = DataEstimator(
                organization=_ORG, storage=LocalStorage(),
                tables=SQLParser(_SUPER, "SELECT * FROM orders", "duckdb").get_physical_tables())
            estimator.estimate()
            assert estimator.resolved_snapshots[(_SUPER, "orders")] is first
            estimator.estimate()
            assert estimator.resolved_snapshots == {(_SUPER, "orders"): second}


# ---------------------------------------------------------------------------
# The reader uses it
# ---------------------------------------------------------------------------

_MOD = "supertable.data_reader"

#: What the estimator listed the files from.
LISTED_FROM = {
    "resources": [{"file": "data/old.parquet", "file_size": 1}],
    "tombstone": ["tomb/dv-old-0.parquet", "tomb/dv-old-1.parquet"],
}
#: What the catalog holds by the time the views are built: a compaction has
#: rewritten the files and cleared the vector.
IN_THE_CATALOG_BY_NOW = {
    "payload": {"resources": [{"file": "data/new.parquet", "file_size": 1}],
                "tombstone": None},
    "path": "snap-new.json",
}


def _run(resolved_snapshots, leaf=IN_THE_CATALOG_BY_NOW, rbac=None,
         query="SELECT id FROM tbl", supers=None, on_execute=None):
    """Run the real ``DataReader.execute`` with the estimator, the catalog and
    the engine mocked. Returns ``(status, message, reflection, catalog)``."""
    reflection = Reflection(
        storage_type="LocalStorage", reflection_bytes=1, total_reflections=1,
        supers=supers or [SuperSnapshot(
            super_name="s1", simple_name="tbl", simple_version=1,
            files=["data/old.parquet"], columns={"id"})])
    with patch(f"{_MOD}.get_storage"), \
         patch(f"{_MOD}.QueryPlanManager"), \
         patch(f"{_MOD}.extend_execution_plan"), \
         patch(f"{_MOD}.restrict_read_access", return_value=dict(rbac or {})), \
         patch(f"{_MOD}.RedisCatalog") as catalog_class, \
         patch(f"{_MOD}.DataEstimator") as estimator_class, \
         patch(f"{_MOD}.Executor") as executor_class:
        catalog = catalog_class.return_value
        if isinstance(leaf, Exception):
            catalog.get_leaf.side_effect = leaf
        else:
            catalog.get_leaf.return_value = leaf
        estimator = estimator_class.return_value
        estimator.estimate.return_value = reflection
        estimator._to_duckdb_path.side_effect = lambda key: f"resolved/{key}"
        if resolved_snapshots is not _LEFT_AS_A_MOCK:
            estimator.resolved_snapshots = resolved_snapshots
        def execute(**_kwargs):
            if on_execute is not None:
                on_execute()
            return pl.DataFrame({"id": [1]}), "duckdb"

        executor_class.return_value.execute.side_effect = execute
        _frame, status, message = DataReader(
            super_name="s1", organization="org", query=query,
        ).execute(role_name="reader")
        return status, message, reflection, catalog


_LEFT_AS_A_MOCK = object()


class TestTheReaderUsesTheSnapshotTheFilesCameFrom:

    def test_the_vector_is_the_one_of_the_snapshot_the_files_came_from(self):
        status, message, reflection, catalog = _run({("s1", "tbl"): LISTED_FROM})
        assert status is Status.OK, message
        vector = reflection.tombstone_views["tbl"]
        assert vector.tombstone_path == [
            "resolved/tomb/dv-old-0.parquet", "resolved/tomb/dv-old-1.parquet"]
        assert vector.cache_key == str(LISTED_FROM["tombstone"])
        # The catalog's newer leaf, with its cleared vector, was not consulted.
        catalog.get_leaf.assert_not_called()

    def test_no_vector_in_that_snapshot_means_no_vector_whatever_the_catalog_says(self):
        """The other direction: an overwrite has since ADDED rows to the vector.
        The files this read has do not hold what replaced them."""
        newer = {"payload": {"resources": [], "tombstone": ["tomb/dv-new.parquet"]},
                 "path": "p"}
        status, message, reflection, catalog = _run(
            {("s1", "tbl"): {"resources": [{"file": "data/old.parquet"}]}}, leaf=newer)
        assert status is Status.OK, message
        assert reflection.tombstone_views == {}
        catalog.get_leaf.assert_not_called()

    def test_the_share_filter_is_the_one_of_that_snapshot_too(self):
        listed_from = dict(LISTED_FROM, _row_filter="region = 'eu'")
        newer = {"payload": {"resources": [], "_row_filter": "region = 'us'"}, "path": "p"}
        status, message, reflection, _ = _run({("s1", "tbl"): listed_from}, leaf=newer)
        assert status is Status.OK, message
        assert reflection.rbac_views["tbl"].where_clause == "region = 'eu'"

    def test_every_alias_of_a_table_gets_the_same_snapshot(self):
        status, message, reflection, catalog = _run(
            {("s1", "tbl"): LISTED_FROM},
            query="SELECT a.id FROM tbl a JOIN tbl b ON a.id = b.id")
        assert status is Status.OK, message
        assert set(reflection.tombstone_views) == {"a", "b"}
        assert (reflection.tombstone_views["a"].tombstone_path
                == reflection.tombstone_views["b"].tombstone_path)
        catalog.get_leaf.assert_not_called()

    def test_an_alias_with_nothing_kept_asks_the_catalog_as_before(self):
        leaf = {"payload": {"resources": [{"file": "data/old.parquet"}],
                            "tombstone": ["tomb/dv-cat.parquet"]}, "path": "p"}
        status, message, reflection, catalog = _run({}, leaf=leaf)
        assert status is Status.OK, message
        assert reflection.tombstone_views["tbl"].tombstone_path == [
            "resolved/tomb/dv-cat.parquet"]
        catalog.get_leaf.assert_called_once_with("org", "s1", "tbl")

    @pytest.mark.parametrize("kept", [_LEFT_AS_A_MOCK, None, ["not", "a", "dict"]])
    def test_an_estimator_that_kept_nothing_usable_asks_the_catalog(self, kept):
        """A stand-in estimator, or one from before this attribute existed."""
        leaf = {"payload": {"resources": [{"file": "data/old.parquet"}],
                            "tombstone": ["tomb/dv-cat.parquet"]}, "path": "p"}
        status, message, reflection, catalog = _run(kept, leaf=leaf)
        assert status is Status.OK, message
        assert reflection.tombstone_views["tbl"].tombstone_path == [
            "resolved/tomb/dv-cat.parquet"]
        catalog.get_leaf.assert_called_once()

    def test_something_kept_that_is_not_a_snapshot_asks_the_catalog(self):
        leaf = {"payload": {"resources": [{"file": "data/old.parquet"}],
                            "tombstone": ["tomb/dv-cat.parquet"]}, "path": "p"}
        status, _, reflection, catalog = _run({("s1", "tbl"): "not a snapshot"}, leaf=leaf)
        assert status is Status.OK
        assert "tbl" in reflection.tombstone_views
        catalog.get_leaf.assert_called_once()

    def test_a_catalog_that_cannot_answer_still_fails_the_read(self):
        """The lookup that remains is still inside the handler that refuses to
        serve a table whose deletion vector is unknown."""
        status, message, _, _ = _run({}, leaf=ConnectionError("redis is down"))
        assert status is Status.ERROR
        assert "cannot establish the deletion vector" in message

    def test_the_snapshots_are_let_go_before_the_query_runs(self):
        """A snapshot lists every file of its table. It is needed to build the
        controls and not while the engine runs the query."""
        kept = {("s1", "tbl"): LISTED_FROM}
        held_while_running = []
        status, message, reflection, _ = _run(
            kept, on_execute=lambda: held_while_running.append(len(kept)))
        assert status is Status.OK, message
        assert held_while_running == [0]
        assert "tbl" in reflection.tombstone_views, "and the vector was taken first"

    def test_a_role_filter_and_the_kept_share_filter_are_both_applied(self):
        listed_from = dict(LISTED_FROM, _row_filter="amount > 10")
        status, message, reflection, _ = _run(
            {("s1", "tbl"): listed_from},
            rbac={"tbl": RbacViewDef(allowed_columns=["*"], where_clause="region = 'eu'")})
        assert status is Status.OK, message
        assert reflection.rbac_views["tbl"].where_clause == "(region = 'eu') AND (amount > 10)"
