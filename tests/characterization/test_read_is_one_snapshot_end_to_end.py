"""One read is one snapshot, behind a real reader.

A read lists a table's files from the snapshot the estimator fetched, and then
builds the views those files are read through. The deletion vector (and a
share's row filter) used to be fetched from the catalog a second time for those
views, with nothing tying the two fetches to one snapshot. A commit that landed
between them paired the files of one snapshot with the vector of the next:

  * a compaction rewrites the files that hold deleted rows and CLEARS the
    vector. The old files, read with no vector, bring back every row deleted so
    far. The old files are still there to be read: compaction does not delete
    them.
  * an overwrite by key ADDS the replaced row to the vector and writes the new
    row to a new file. The old files, read with the new vector, lose the row:
    its old version is hidden and its new version is in a file this read does
    not have.

Either way the read answered with a table that never existed, and said OK.

Here a table is written with the real writer, on LOCAL storage, and read with
the real reader, RBAC and DuckDB. ``commit_mid_read`` lands a real commit at
exactly that point: after the estimator has returned the file list, before any
view is built.

The unit side, which leaf shapes are pinned and which still ask the catalog,
is in ``supertable/tests/test_read_takes_its_controls_from_one_snapshot.py``.
"""

from __future__ import annotations

import json
import uuid
from types import SimpleNamespace

import polars as pl
import pyarrow as pa
import pytest

from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.data_estimator import DataEstimator
from supertable.engine.engine_enum import Engine
from supertable.rbac.role_manager import RoleManager
from supertable.redis_catalog import RedisCatalog
from supertable.redis_keys import meta_leaf
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"
#: Reads ``id`` and ``region`` of the rows in ``eu``.
READER = "eu_two_columns"

#: What the table holds once the fixture has written it: id 2 was first written
#: in ``eu`` and then overwritten, so its first row is in the deletion vector
#: and still physically in the first file.
LIVE = [{"id": 1, "region": "eu"}, {"id": 2, "region": "apac"}, {"id": 3, "region": "us"}]


@pytest.fixture
def lake(hermetic_fakeredis):
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"snap_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=[],
        data=pl.DataFrame({"id": [1, 2, 3], "region": ["eu", "eu", "us"],
                           "salary": [40_000, 900_000, 55_000]}))
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=["id"],
        data=pl.DataFrame({"id": [2], "region": ["apac"], "salary": [910_000]}))
    RoleManager(super_name=sup, organization=ORG, actor_role_name=ADMIN).create_role(
        {"role_name": READER, "role": "reader", "tables": {
            "emp": {"columns": ["id", "region"],
                    "filters": {"region": {"operation": "=", "type": "value",
                                           "value": "eu"}}}}})

    def read(sql: str, role: str = ADMIN):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=role, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return frame.to_dicts()

    def snapshot():
        return RedisCatalog().get_leaf(ORG, sup, "emp")["payload"]

    def compact():
        DataWriter(super_name=sup, organization=ORG).compact(ADMIN, "emp")

    def overwrite(row: dict):
        DataWriter(super_name=sup, organization=ORG).write(
            role_name=ADMIN, simple_name="emp", overwrite_columns=["id"],
            data=pl.DataFrame({key: [value] for key, value in row.items()}))

    yield SimpleNamespace(sup=sup, read=read, snapshot=snapshot,
                          compact=compact, overwrite=overwrite)
    engine_duckdb.reset_shared_duckdb_state()


@pytest.fixture
def commit_mid_read(monkeypatch):
    """Land *commit* once, when the next read has its file list and no view yet."""
    real_estimate = DataEstimator.estimate

    def arm(commit):
        landed = []

        def estimate(self):
            reflection = real_estimate(self)
            if not landed:
                landed.append(True)
                commit()
            return reflection

        monkeypatch.setattr(DataEstimator, "estimate", estimate)
        return landed

    return arm


ALL_ROWS = "SELECT id, region FROM emp ORDER BY id, region"


def test_the_fixture_has_a_deleted_row_still_in_its_file(lake):
    """Without this the tests below would pass on a table with nothing to bring back."""
    before = lake.snapshot()
    assert before["tombstone"], "the overwrite must have left a deletion vector"
    assert lake.read(ALL_ROWS) == LIVE


def test_a_compaction_that_commits_mid_read_brings_no_deleted_row_back(lake, commit_mid_read):
    before = lake.snapshot()
    landed = commit_mid_read(lake.compact)

    rows = lake.read(ALL_ROWS)

    assert landed, "the compaction did not run inside the read"
    after = lake.snapshot()
    assert not after.get("tombstone"), "the compaction must have cleared the vector"
    assert ({r["file"] for r in after["resources"]}
            != {r["file"] for r in before["resources"]}), "and rewritten the files"
    # The read is the snapshot it started from: id 2 once, as overwritten.
    assert rows == LIVE


def test_the_read_after_that_compaction_says_the_same(lake, commit_mid_read):
    commit_mid_read(lake.compact)
    assert lake.read(ALL_ROWS) == LIVE
    # Not armed again: this one reads the compacted snapshot from start to end.
    assert lake.read(ALL_ROWS) == LIVE


def test_an_overwrite_that_commits_mid_read_makes_no_row_vanish(lake, commit_mid_read):
    landed = commit_mid_read(
        lambda: lake.overwrite({"id": 3, "region": "latam", "salary": 1}))

    rows = lake.read(ALL_ROWS)

    assert landed
    # The snapshot the read started from: id 3 is still there, as it was.
    assert rows == LIVE
    # And the commit is not lost: the next read has it.
    assert lake.read(ALL_ROWS) == [
        {"id": 1, "region": "eu"}, {"id": 2, "region": "apac"},
        {"id": 3, "region": "latam"}]


def test_a_delete_that_commits_mid_read_is_not_half_seen(lake, commit_mid_read):
    """A count and the rows it counts come from one snapshot."""
    commit_mid_read(lambda: lake.overwrite({"id": 1, "region": "eu", "salary": 2}))
    assert lake.read("SELECT count(*) AS n, count(DISTINCT id) AS ids FROM emp") == [
        {"n": 3, "ids": 3}]


def test_both_aliases_of_a_self_join_read_the_same_snapshot(lake, commit_mid_read):
    commit_mid_read(lake.compact)
    rows = lake.read(
        "SELECT a.id, b.region FROM emp a JOIN emp b ON a.id = b.id "
        "ORDER BY a.id, b.region")
    # With the old files and no vector, id 2 has two rows on each side: four.
    assert rows == LIVE


def test_a_role_does_not_get_a_deleted_row_through_its_filter(lake, commit_mid_read):
    """The row that would come back is one the role's own filter lets through."""
    assert lake.read("SELECT * FROM emp ORDER BY id", role=READER) == [
        {"id": 1, "region": "eu"}]
    commit_mid_read(lake.compact)
    assert lake.read("SELECT * FROM emp ORDER BY id", role=READER) == [
        {"id": 1, "region": "eu"}]


def test_a_streamed_read_is_one_snapshot_too(lake, commit_mid_read):
    commit_mid_read(lake.compact)
    handle = DataReader(
        super_name=lake.sup, organization=ORG, query="SELECT id, region FROM emp",
    ).stream(role_name=ADMIN, engine=Engine.DUCKDB)
    with handle:
        table = pa.Table.from_batches(list(handle.batches()), schema=handle.schema)
    assert sorted(table.to_pylist(), key=lambda r: r["id"]) == LIVE


def test_a_leaf_that_holds_only_a_path_is_one_snapshot_too(
        lake, commit_mid_read, hermetic_fakeredis):
    """A leaf without an inline snapshot is a shape the writer produces.

    The estimator then reads the snapshot from storage, and that is the
    snapshot the vector has to come from as well.
    """
    key = meta_leaf(ORG, lake.sup, "emp")
    leaf = json.loads(hermetic_fakeredis.get(key))
    assert leaf.pop("payload", None), "the fixture's leaf had no inline snapshot"
    hermetic_fakeredis.set(key, json.dumps(leaf))

    assert lake.read(ALL_ROWS) == LIVE
    commit_mid_read(lake.compact)
    assert lake.read(ALL_ROWS) == LIVE


def test_the_vector_is_not_asked_of_the_catalog_a_second_time(lake, monkeypatch):
    """The second fetch is what made two snapshots of one read.

    It was also a catalog round trip per table alias on every read; a read now
    makes none for it.
    """
    asked = []
    real_resolve = DataReader._resolve_snapshot
    real_get_leaf = RedisCatalog.get_leaf

    def resolve(self, catalog, super_name, simple_name):
        asked.append(("resolve", simple_name))
        return real_resolve(self, catalog, super_name, simple_name)

    def get_leaf(self, org, sup, simple):
        asked.append(("get_leaf", simple))
        return real_get_leaf(self, org, sup, simple)

    monkeypatch.setattr(DataReader, "_resolve_snapshot", resolve)
    monkeypatch.setattr(RedisCatalog, "get_leaf", get_leaf)

    assert lake.read(ALL_ROWS) == LIVE
    assert lake.read(
        "SELECT a.id FROM emp a JOIN emp b ON a.id = b.id ORDER BY a.id") == [
        {"id": 1}, {"id": 2}, {"id": 3}]

    assert asked == []


def test_a_cte_name_is_still_asked_and_reads_as_before(lake, monkeypatch):
    """An alias the estimator resolved no snapshot for keeps the old lookup."""
    asked = []
    real_resolve = DataReader._resolve_snapshot

    def resolve(self, catalog, super_name, simple_name):
        asked.append(simple_name)
        return real_resolve(self, catalog, super_name, simple_name)

    monkeypatch.setattr(DataReader, "_resolve_snapshot", resolve)

    assert lake.read("WITH w AS (SELECT id FROM emp) SELECT count(*) AS n FROM w") == [
        {"n": 3}]
    assert "emp" not in asked
