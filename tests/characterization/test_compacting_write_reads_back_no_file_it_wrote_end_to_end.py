"""A write that compacts does not read back the files it has just written.

A write can compact on its way: when the deletion vector has grown past
``max_tombstone_rows`` it rewrites the files that hold deleted rows, and when
small files have piled up to ``max_overlapping_files`` it merges them. The
write already kept the footer of the file it wrote for the incoming rows, so
that file's statistics cost no read. The files the two compaction phases wrote
were not kept the same way: each was fetched back whole for its statistics,
and, when files had been merged, once more for the snapshot's schema, all of it
with the table lock held.

A table is written here with the real writer on LOCAL storage, the two
thresholds are set low enough for a write to trip them, and every whole-object
read made during that write is counted.

``test_compact_reads_back_no_file_it_wrote_end_to_end.py`` is the same for an
explicit ``compact()``;
``supertable/tests/test_compaction_reuses_written_footers.py`` is the unit
side.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import polars as pl
import pytest

from supertable import data_writer as data_writer_module
from supertable.data_reader import DataReader, Status
from supertable.data_writer import DataWriter
from supertable.engine import duckdb as engine_duckdb
from supertable.engine.engine_enum import Engine
from supertable.processing import extract_stats_rows, load_stats
from supertable.redis_catalog import RedisCatalog
from supertable.storage.local_storage import LocalStorage
from supertable.super_table import SuperTable

ORG = "kladna-soft"
ADMIN = "superadmin"


def _rows(first: int, **more) -> pl.DataFrame:
    ids = [first, first + 1, first + 2]
    columns = {"id": ids, "region": ["eu", "us", "apac"],
               "salary": [i * 10 for i in ids]}
    columns.update({name: [value] * 3 for name, value in more.items()})
    return pl.DataFrame(columns)


@pytest.fixture
def lake(hermetic_fakeredis, monkeypatch):
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"wfoot_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)

    whole_object_reads = []
    real_read_bytes = LocalStorage.read_bytes

    def read_bytes(self, path):
        whole_object_reads.append(path)
        return real_read_bytes(self, path)

    monkeypatch.setattr(LocalStorage, "read_bytes", read_bytes)

    def snapshot() -> dict:
        leaf = RedisCatalog().get_leaf(ORG, sup, "emp")
        return leaf["payload"] if leaf else {"resources": []}

    def write(frame: pl.DataFrame, overwrite_columns=()) -> SimpleNamespace:
        """Write *frame* and say which files this write left in the snapshot
        and which whole objects it read while it ran."""
        before = snapshot()
        whole_object_reads.clear()
        DataWriter(super_name=sup, organization=ORG).write(
            role_name=ADMIN, simple_name="emp",
            overwrite_columns=list(overwrite_columns), data=frame)
        reads = list(whole_object_reads)
        after = snapshot()
        old = {r["file"] for r in before["resources"]}
        written = [r["file"] for r in after["resources"] if r["file"] not in old]
        return SimpleNamespace(before=before, after=after, reads=reads, written=written)

    def thresholds(**limits) -> None:
        DataWriter(super_name=sup, organization=ORG).configure_table(
            ADMIN, "emp", **limits)

    def rows(sql: str = "SELECT id, region, salary FROM emp ORDER BY id"):
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=sql,
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return frame.to_dicts()

    yield SimpleNamespace(sup=sup, snapshot=snapshot, write=write,
                          thresholds=thresholds, rows=rows)
    engine_duckdb.reset_shared_duckdb_state()


def _read_back(done) -> list:
    return [path for path in done.reads if path in set(done.written)]


def _recorded_statistics_are_what_the_files_hold(done) -> None:
    recorded = load_stats(done.after["stats_file"])
    assert set(recorded["file_path"].to_list()) == {
        r["file"] for r in done.after["resources"]}, "one set of rows per live file"
    order = ["file_path", "row_group_id", "column_name"]
    for path in done.written:
        of_this_file = recorded.filter(pl.col("file_path") == path).sort(order)
        held_by_the_file = extract_stats_rows([path]).sort(order)   # reads it back
        assert held_by_the_file.height > 0
        assert of_this_file.select(held_by_the_file.columns).equals(held_by_the_file)


def _schema_names(snapshot: dict) -> set:
    schema = snapshot["schema"]
    return {c["name"] for c in schema} if isinstance(schema, list) else set(schema)


# ---------------------------------------------------------------------------
# The small-file merge
# ---------------------------------------------------------------------------

def test_a_write_that_merges_small_files_reads_back_nothing_it_wrote(lake):
    lake.write(_rows(1))
    lake.thresholds(max_overlapping_files=3)
    lake.write(_rows(4))

    done = lake.write(_rows(7))                     # the third small file

    assert len(done.after["resources"]) == 1, "the write must have merged the three"
    assert len(done.written) == 1
    # Twice before: once for its statistics, once for the snapshot's schema.
    assert _read_back(done) == []
    assert [r["id"] for r in lake.rows()] == list(range(1, 10))
    _recorded_statistics_are_what_the_files_hold(done)


def test_a_column_only_an_older_file_has_survives_the_merge_in_the_schema(lake):
    """Why a compacting write takes its schema from the merged files and not
    from the incoming frame: the frame may be narrower than the table."""
    lake.write(_rows(1, bonus=5))                   # only this file has ``bonus``
    lake.thresholds(max_overlapping_files=3)
    lake.write(_rows(4))

    done = lake.write(_rows(7))

    assert "bonus" not in _schema_names(done.before), (
        "the narrower second write had left the snapshot's schema without it")
    assert len(done.after["resources"]) == 1
    assert _read_back(done) == []
    # The schema is the merged file's: taken from its footer, not read back.
    assert {"id", "region", "salary", "bonus"} <= _schema_names(done.after)
    assert set(pl.read_parquet_schema(done.written[0])) == _schema_names(done.after)
    bonus = lake.rows("SELECT id, bonus FROM emp ORDER BY id")
    assert [r["bonus"] for r in bonus] == [5, 5, 5, None, None, None, None, None, None]


# ---------------------------------------------------------------------------
# The deletion-vector drain
# ---------------------------------------------------------------------------

def test_a_write_that_drains_the_vector_reads_back_nothing_it_wrote(lake):
    lake.write(_rows(1))
    lake.thresholds(max_tombstone_rows=1)

    done = lake.write(
        pl.DataFrame({"id": [2], "region": ["latam"], "salary": [1]}),
        overwrite_columns=["id"])

    assert not done.after.get("tombstone"), "the write must have drained the vector"
    assert len(done.written) == 2, "the incoming file and the rewritten one"
    # The rewritten file was fetched back once before, for its statistics.
    assert _read_back(done) == []
    assert lake.rows() == [
        {"id": 1, "region": "eu", "salary": 10},
        {"id": 2, "region": "latam", "salary": 1},
        {"id": 3, "region": "apac", "salary": 30}]
    _recorded_statistics_are_what_the_files_hold(done)


def test_a_write_that_does_both_reads_back_nothing_it_wrote(lake):
    lake.write(_rows(1))
    lake.write(_rows(4))
    lake.thresholds(max_overlapping_files=3, max_tombstone_rows=1)

    done = lake.write(
        pl.DataFrame({"id": [5], "region": ["latam"], "salary": [1]}),
        overwrite_columns=["id"])

    assert not done.after.get("tombstone")
    assert len(done.after["resources"]) == 1, "drained first, then merged into one"
    assert _read_back(done) == []
    rows = lake.rows()
    assert [r["id"] for r in rows] == [1, 2, 3, 4, 5, 6]
    assert {"id": 5, "region": "latam", "salary": 1} in rows
    _recorded_statistics_are_what_the_files_hold(done)


# ---------------------------------------------------------------------------
# What does not change
# ---------------------------------------------------------------------------

def test_a_write_that_compacts_nothing_reads_back_nothing_as_before(lake):
    first = lake.write(_rows(1))
    second = lake.write(_rows(4))
    assert len(second.after["resources"]) == 2
    assert _read_back(first) == [] and _read_back(second) == []


def test_one_set_of_footers_reaches_every_step_of_a_compacting_write(lake, monkeypatch):
    handed = {}

    def recording(name, keyword):
        real = getattr(data_writer_module, name)

        def call(*args, **kwargs):
            handed[name] = kwargs.get(keyword)
            return real(*args, **kwargs)

        monkeypatch.setattr(data_writer_module, name, call)

    lake.write(_rows(1))
    lake.write(_rows(4))
    lake.thresholds(max_overlapping_files=3, max_tombstone_rows=1)
    recording("compact_tombstones", "footer_md_out")
    recording("compact_resources", "footer_md_out")
    recording("extract_stats_rows", "footer_md_cache")

    done = lake.write(
        pl.DataFrame({"id": [5], "region": ["latam"], "salary": [1]}),
        overwrite_columns=["id"])

    footers = handed["compact_resources"]
    assert isinstance(footers, dict)
    assert handed["compact_tombstones"] is footers
    assert handed["extract_stats_rows"] is footers
    assert set(done.written) <= set(footers)


def test_a_compacting_write_whose_footers_were_not_kept_still_reads_them_back(
        lake, monkeypatch):
    """A writer path that keeps no footer leaves the cache without the file;
    its statistics and the schema are then read from storage, as they always
    were, and the table says the same."""
    lake.write(_rows(1, bonus=5))
    lake.thresholds(max_overlapping_files=3)
    lake.write(_rows(4))

    real = data_writer_module.compact_resources

    def without_footers(*args, **kwargs):
        kwargs.pop("footer_md_out", None)
        return real(*args, **kwargs)

    monkeypatch.setattr(data_writer_module, "compact_resources", without_footers)

    done = lake.write(_rows(7))

    assert len(done.written) == 1
    assert done.reads.count(done.written[0]) == 2, "statistics, then the schema"
    assert "bonus" in _schema_names(done.after)
    assert [r["id"] for r in lake.rows()] == list(range(1, 10))
    _recorded_statistics_are_what_the_files_hold(done)
