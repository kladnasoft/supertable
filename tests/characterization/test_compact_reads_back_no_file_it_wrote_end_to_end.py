"""``compact()`` does not read back the files it has just written.

For every file it writes, compaction needs the footer statistics (for the stats
artifact) and the schema (for the new snapshot). It used to fetch each new file
from storage for the first, and fetch it again for the second: two downloads of
the whole object per output file, with the table lock held, to learn what was
in memory when the file was uploaded.

A table is written here with the real writer on LOCAL storage, and a real
``compact()`` is run with every whole-object read counted.

``supertable/tests/test_compaction_reuses_written_footers.py`` is the unit
side: what the helpers keep, and that it is what storage holds.
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

ROWS_QUERY = "SELECT id, region, salary FROM emp ORDER BY id"


@pytest.fixture
def lake(hermetic_fakeredis, monkeypatch):
    """Four small files, one of them holding a row that has been overwritten:
    both phases of compaction have work, and each writes a file."""
    engine_duckdb.reset_shared_duckdb_state()
    sup = f"foot_{uuid.uuid4().hex[:12]}"
    SuperTable(sup, ORG)
    writer = DataWriter(super_name=sup, organization=ORG)
    for first in (1, 4, 7):
        writer.write(
            role_name=ADMIN, simple_name="emp", overwrite_columns=[],
            data=pl.DataFrame({"id": [first, first + 1, first + 2],
                               "region": ["eu", "us", "apac"],
                               "salary": [first * 10, first * 20, first * 30]}))
    writer.write(
        role_name=ADMIN, simple_name="emp", overwrite_columns=["id"],
        data=pl.DataFrame({"id": [2], "region": ["latam"], "salary": [1]}))

    whole_object_reads = []
    real_read_bytes = LocalStorage.read_bytes

    def read_bytes(self, path):
        whole_object_reads.append(path)
        return real_read_bytes(self, path)

    monkeypatch.setattr(LocalStorage, "read_bytes", read_bytes)

    def snapshot() -> dict:
        return RedisCatalog().get_leaf(ORG, sup, "emp")["payload"]

    def rows():
        frame, status, message = DataReader(
            super_name=sup, organization=ORG, query=ROWS_QUERY,
        ).execute(role_name=ADMIN, engine=Engine.DUCKDB)
        assert status is Status.OK, message
        return frame.to_dicts()

    def compact() -> SimpleNamespace:
        before = snapshot()
        whole_object_reads.clear()
        result = DataWriter(super_name=sup, organization=ORG).compact(ADMIN, "emp")
        reads = list(whole_object_reads)
        after = snapshot()
        old = {r["file"] for r in before["resources"]}
        written = [r["file"] for r in after["resources"] if r["file"] not in old]
        return SimpleNamespace(before=before, after=after, reads=reads,
                               written=written, result=result)

    yield SimpleNamespace(sup=sup, snapshot=snapshot, rows=rows, compact=compact)
    engine_duckdb.reset_shared_duckdb_state()


def test_the_fixture_gives_both_phases_a_file_to_write(lake):
    before = lake.snapshot()
    assert len(before["resources"]) == 4
    assert before["tombstone"], "the overwrite must have left a deletion vector"


def test_no_file_the_compaction_wrote_is_read_back(lake):
    done = lake.compact()

    assert done.written, "the compaction must have written at least one file"
    assert not done.after.get("tombstone"), "and drained the vector"
    read_back = [path for path in done.reads if path in set(done.written)]
    # Two per written file before: one for its statistics, one for its schema.
    assert read_back == []


def test_the_table_reads_the_same_before_and_after(lake):
    before = lake.rows()
    assert len(before) == 9 and {"id": 2, "region": "latam", "salary": 1} in before
    lake.compact()
    assert lake.rows() == before


def test_the_statistics_recorded_are_the_ones_the_files_hold(lake):
    """Taken from the footers in memory, they are what reading the stored
    files back gives."""
    done = lake.compact()

    recorded = load_stats(done.after["stats_file"])
    assert set(recorded["file_path"].to_list()) == {
        r["file"] for r in done.after["resources"]}, "one set of rows per live file"

    order = ["file_path", "row_group_id", "column_name"]
    for path in done.written:
        of_this_file = recorded.filter(pl.col("file_path") == path).sort(order)
        held_by_the_file = extract_stats_rows([path]).sort(order)   # reads it back
        assert held_by_the_file.height > 0
        assert of_this_file.select(held_by_the_file.columns).equals(held_by_the_file)


def test_the_schema_recorded_is_the_table_s(lake):
    done = lake.compact()
    assert done.after["schema"] == done.before["schema"]

    # ...and it is what the schema read back from the written files is.
    writer = DataWriter(super_name=lake.sup, organization=ORG)
    read_back = writer._build_compact_model_df(
        [{"file": path} for path in done.written], {})
    in_snapshot = done.after["schema"]
    names = ({c["name"] for c in in_snapshot} if isinstance(in_snapshot, list)
             else set(in_snapshot))
    assert set(read_back.columns) == names


def test_one_set_of_footers_reaches_every_step(lake, monkeypatch):
    """Both phases put their footers into the dict the statistics and the
    schema are then read from."""
    handed = {}

    def recording(name, keyword):
        real = getattr(data_writer_module, name)

        def call(*args, **kwargs):
            handed[name] = kwargs.get(keyword)
            return real(*args, **kwargs)

        monkeypatch.setattr(data_writer_module, name, call)

    recording("compact_tombstones", "footer_md_out")
    recording("compact_resources", "footer_md_out")
    recording("extract_stats_rows", "footer_md_cache")

    done = lake.compact()

    footers = handed["compact_resources"]
    assert isinstance(footers, dict)
    assert handed["compact_tombstones"] is footers
    assert handed["extract_stats_rows"] is footers
    assert set(done.written) <= set(footers)


def test_a_compaction_whose_footers_were_not_kept_still_reads_them_back(lake, monkeypatch):
    """A writer path that keeps no footer leaves the cache empty; statistics
    and schema are then read from storage, as they always were, and the
    snapshot says the same."""
    for name in ("compact_tombstones", "compact_resources"):
        real = getattr(data_writer_module, name)

        def without_footers(*args, _real=real, **kwargs):
            kwargs.pop("footer_md_out", None)
            return _real(*args, **kwargs)

        monkeypatch.setattr(data_writer_module, name, without_footers)

    before = lake.rows()
    done = lake.compact()

    for path in done.written:
        assert done.reads.count(path) == 2, "once for statistics, once for the schema"
    assert done.after["schema"] == done.before["schema"]
    assert lake.rows() == before
    recorded = load_stats(done.after["stats_file"])
    assert set(done.written) <= set(recorded["file_path"].to_list())


def test_a_second_compaction_reads_back_nothing_it_wrote_either(lake):
    """Footers are kept per compaction: the first one's are not what the
    second one answers from, and the second reads none back itself."""
    before = lake.rows()
    lake.compact()
    again = lake.compact()
    assert [path for path in again.reads if path in set(again.written)] == []
    assert not again.after.get("tombstone")
    assert lake.rows() == before
