"""Compaction does not read back the files it has just written.

``DataWriter.compact`` needs two things about every file it writes: the footer
statistics, for the stats artifact, and the schema, for the new snapshot. Both
are in the parquet footer, and the footer is in memory when the file is
uploaded. They were read back from storage instead: each new file fetched whole
once for its statistics and once more for its schema, with the table lock held.

The compaction helpers now hand the footers back (``footer_md_out``) and the
two consumers take them from there (``footer_md_cache``). Pinned here, on a
real ``LocalStorage``:

  * the helpers keep a footer for every file they write, and only when asked;
  * the kept footer says what the stored file says, so statistics and schema
    come out the same either way;
  * a file with a kept footer is not read, and one without is read as before.

``tests/characterization/test_compact_reads_back_no_file_it_wrote_end_to_end.py``
runs a real ``compact()`` and counts the reads.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from typing import Dict, List
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc_mod  # noqa: E402
from supertable.data_writer import DataWriter  # noqa: E402
from supertable.processing import (  # noqa: E402
    ROWID_COL,
    TOMBSTONE_FILE_COL,
    compact_resources,
    compact_tombstones,
    extract_stats_rows,
)
from supertable.storage.local_storage import LocalStorage  # noqa: E402


class _CountingStorage(LocalStorage):
    """A LocalStorage that remembers every whole-object read."""

    def __init__(self):
        super().__init__()
        self.bytes_read_of: List[str] = []

    def read_bytes(self, path):
        self.bytes_read_of.append(path)
        return super().read_bytes(path)


@pytest.fixture
def lake(tmp_path):
    data_dir = str(tmp_path / "warehouse" / "orders" / "data")
    os.makedirs(data_dir, exist_ok=True)
    storage = _CountingStorage()
    proc_mod._storage = None
    with patch.object(proc_mod, "get_storage", return_value=storage):
        yield SimpleNamespace(data_dir=data_dir, storage=storage)
    proc_mod._storage = None


def _source(lake, name: str, frame: pl.DataFrame) -> Dict:
    path = os.path.join(lake.data_dir, name)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(frame.to_arrow(), path, compression="zstd", write_statistics=True)
    return {"file": path, "file_size": os.path.getsize(path), "rows": frame.height}


def _frame(first_id: int, rows: int, **more) -> pl.DataFrame:
    ids = list(range(first_id, first_id + rows))
    columns = {ROWID_COL: ids, "id": ids, "name": [f"n{i}" for i in ids]}
    columns.update({name: [value] * rows for name, value in more.items()})
    return pl.DataFrame(columns)


def _three_small_files(lake) -> dict:
    return {"resources": [
        _source(lake, "a.parquet", _frame(1, 5)),
        _source(lake, "b.parquet", _frame(6, 5)),
        _source(lake, "c.parquet", _frame(11, 5, amount=1.5)),   # one more column
    ]}


def _paths(resources) -> List[str]:
    return [r["file"] for r in resources]


# ---------------------------------------------------------------------------
# The helpers hand back the footers of what they wrote
# ---------------------------------------------------------------------------

class TestTheHelpersKeepTheFootersTheyWrote:

    def test_compact_resources_keeps_one_for_every_file_it_writes(self, lake):
        footers: dict = {}
        _, total_rows, new_resources, _ = compact_resources(
            snapshot=_three_small_files(lake), data_dir=lake.data_dir,
            compression_level=1, footer_md_out=footers)
        assert new_resources, "the three files must have been merged"
        assert set(footers) == set(_paths(new_resources))
        assert sum(md.num_rows for md in footers.values()) == total_rows == 15

    def test_one_for_each_chunk_when_the_merge_writes_several(self, lake):
        footers: dict = {}
        _, _, new_resources, _ = compact_resources(
            snapshot=_three_small_files(lake), data_dir=lake.data_dir,
            compression_level=1, table_config={"max_memory_chunk_size": 1},
            small_only=False, footer_md_out=footers)
        assert len(new_resources) == 3, "a 1-byte chunk cap flushes after every file"
        assert set(footers) == set(_paths(new_resources))

    def test_compact_tombstones_keeps_one_for_every_file_it_rewrites(self, lake):
        snapshot = _three_small_files(lake)
        a, b = snapshot["resources"][0]["file"], snapshot["resources"][1]["file"]
        vector = pl.DataFrame({TOMBSTONE_FILE_COL: [a, a, b], ROWID_COL: [1, 2, 7]})
        footers: dict = {}
        removed, new_resources, sunset = compact_tombstones(
            snapshot=snapshot, tombstone_df=vector, data_dir=lake.data_dir,
            compression_level=1, footer_md_out=footers)
        assert removed == 3 and sunset == {a, b}
        assert set(footers) == set(_paths(new_resources))
        assert sorted(md.num_rows for md in footers.values()) == [3, 4]

    def test_nothing_is_kept_and_nothing_changes_unless_it_is_asked_for(self, lake):
        """Every other caller of the two helpers passes no ``footer_md_out``."""
        snapshot = _three_small_files(lake)
        considered, total_rows, new_resources, sunset = compact_resources(
            snapshot=snapshot, data_dir=lake.data_dir, compression_level=1)
        assert (considered, total_rows, len(new_resources), len(sunset)) == (3, 15, 1, 3)

        a = snapshot["resources"][0]["file"]
        vector = pl.DataFrame({TOMBSTONE_FILE_COL: [a], ROWID_COL: [1]})
        removed, rewritten, _ = compact_tombstones(
            snapshot=_three_small_files(lake), tombstone_df=vector,
            data_dir=lake.data_dir, compression_level=1)
        assert removed == 1 and len(rewritten) == 1

    def test_the_merged_rows_are_the_same_with_and_without_it(self, lake):
        with_footers = compact_resources(
            snapshot=_three_small_files(lake), data_dir=lake.data_dir,
            compression_level=1, footer_md_out={})[2]
        without = compact_resources(
            snapshot=_three_small_files(lake), data_dir=lake.data_dir,
            compression_level=1)[2]
        read = lambda resources: pl.read_parquet(_paths(resources)[0]).sort("id")  # noqa: E731
        assert read(with_footers).equals(read(without))


# ---------------------------------------------------------------------------
# The kept footer says what the stored file says
# ---------------------------------------------------------------------------

@pytest.fixture
def compacted(lake):
    footers: dict = {}
    new_resources = compact_resources(
        snapshot=_three_small_files(lake), data_dir=lake.data_dir,
        compression_level=1, table_config={"max_memory_chunk_size": 1},
        small_only=False, footer_md_out=footers)[2]
    lake.storage.bytes_read_of.clear()
    return SimpleNamespace(resources=new_resources, footers=footers,
                           paths=_paths(new_resources))


class TestTheKeptFooterIsTheStoredFile:

    def test_it_is_the_footer_storage_holds(self, compacted):
        for path, kept in compacted.footers.items():
            stored = pq.read_metadata(path)
            assert kept.num_rows == stored.num_rows
            assert kept.num_row_groups == stored.num_row_groups
            assert kept.schema.to_arrow_schema().equals(stored.schema.to_arrow_schema())

    def test_statistics_from_it_are_the_statistics_read_back(self, lake, compacted):
        read_back = extract_stats_rows(compacted.paths)
        assert set(lake.storage.bytes_read_of) == set(compacted.paths)
        lake.storage.bytes_read_of.clear()

        from_footers = extract_stats_rows(
            compacted.paths, footer_md_cache=compacted.footers)

        assert lake.storage.bytes_read_of == [], "a kept footer is not read back"
        assert from_footers.height > 0
        assert from_footers.equals(read_back)

    def test_a_file_without_a_kept_footer_has_its_statistics_read_back(self, lake, compacted):
        kept_for_one = {compacted.paths[0]: compacted.footers[compacted.paths[0]]}
        rows = extract_stats_rows(compacted.paths, footer_md_cache=kept_for_one)
        assert sorted(lake.storage.bytes_read_of) == sorted(compacted.paths[1:])
        assert rows.equals(extract_stats_rows(compacted.paths))


# ---------------------------------------------------------------------------
# The snapshot's schema comes from it too
# ---------------------------------------------------------------------------

def _writer(storage) -> DataWriter:
    """A DataWriter with only what ``_build_compact_model_df`` touches."""
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(storage=storage)
    return writer


class TestTheSchemaComesFromTheKeptFooter:

    def test_no_new_file_is_read_for_the_schema(self, lake, compacted):
        model = _writer(lake.storage)._build_compact_model_df(
            compacted.resources, {}, footer_md_cache=compacted.footers)
        assert lake.storage.bytes_read_of == []
        assert model.height == 0
        assert set(model.columns) == {ROWID_COL, "id", "name", "amount"}

    def test_the_schema_is_the_one_read_back_from_storage(self, lake, compacted):
        writer = _writer(lake.storage)
        from_footers = writer._build_compact_model_df(
            compacted.resources, {}, footer_md_cache=compacted.footers)
        read_back = writer._build_compact_model_df(compacted.resources, {})
        assert sorted(lake.storage.bytes_read_of) == sorted(compacted.paths)
        assert from_footers.schema == read_back.schema
        assert from_footers.columns == read_back.columns

    def test_a_file_without_a_kept_footer_is_read_for_its_schema(self, lake, compacted):
        """The column only the last file has is found either way."""
        all_but_last = {p: compacted.footers[p] for p in compacted.paths[:-1]}
        writer = _writer(lake.storage)
        model = writer._build_compact_model_df(
            compacted.resources, {}, footer_md_cache=all_but_last)
        assert lake.storage.bytes_read_of == [compacted.paths[-1]]
        assert model.schema == writer._build_compact_model_df(compacted.resources, {}).schema

    @pytest.mark.parametrize("cache", [None, {}])
    def test_without_kept_footers_every_file_is_read_as_before(self, lake, compacted, cache):
        _writer(lake.storage)._build_compact_model_df(
            compacted.resources, {}, footer_md_cache=cache)
        assert sorted(lake.storage.bytes_read_of) == sorted(compacted.paths)

    def test_a_file_that_cannot_be_read_still_falls_back_to_the_old_schema(self, lake):
        """The fallback chain below the footers is untouched."""
        writer = _writer(lake.storage)
        model = writer._build_compact_model_df(
            [{"file": os.path.join(lake.data_dir, "gone.parquet")}],
            {"schema": {"id": "Int64", "name": "String"}},
            footer_md_cache={})
        assert dict(model.schema) == {"id": pl.Int64, "name": pl.String}
