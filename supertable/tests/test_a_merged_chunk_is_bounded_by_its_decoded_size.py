"""
A merged chunk is bounded by its decoded size as well as by its stored bytes.

``max_memory_chunk_size`` counts the stored bytes of the files a merge buffers,
and the merge holds them decoded.  Batches of a million rows by fifty columns,
stored in 8 MiB each, counted as "small": every third append read three of them
back, 1.2 GB of rows and 5 GB of memory, to write one file instead of three.

Two things follow from one estimate, rows x columns x 8 bytes:

  * a file whose estimate reaches three quarters of 32 times
    ``max_memory_chunk_size`` is big enough, whatever it weighs on disk: the
    gate does not count it and no merge reads it;
  * a chunk is closed once the estimate of what it buffers reaches 32 times
    ``max_memory_chunk_size``.

The multiple is high on purpose (every file costs a reader), so a table stored
in more than about a quarter of a byte a value is laid out exactly as before,
which the older suites (``test_processing_compact_resources.py``,
``test_compaction_selection.py``) keep sealing.
"""
from __future__ import annotations

import os
from typing import Dict, List
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc  # noqa: E402
from supertable.processing import (  # noqa: E402
    _COMPACTION_DECODED_RATIO,
    _decoded_size_cap,
    _decoded_size_threshold,
    _is_small_file,
    _resource_decoded_estimate,
    _small_file_threshold,
    compact_resources,
    count_small_files,
    should_compact_small_files,
)

MIB = 1024 * 1024
MAX_MEM = 16 * MIB                     # the library default
ROWID = "__rowid__"


def _res(name: str, *, size: int, rows=None, columns=None) -> Dict:
    resource = {"file": name, "file_size": size}
    if rows is not None:
        resource["rows"] = rows
    if columns is not None:
        resource["columns"] = columns
    return resource


# ---------------------------------------------------------------------------
# The estimate and the two thresholds
# ---------------------------------------------------------------------------

class TestTheEstimate:

    def test_it_is_eight_bytes_a_value(self):
        assert _resource_decoded_estimate(_res("f", size=1, rows=1000, columns=7)) == 56_000

    def test_a_list_of_column_descriptions_counts_as_that_many_columns(self):
        described = [{"name": "id", "type": "Int64"}, {"name": "v", "type": "String"}]
        assert _resource_decoded_estimate(_res("f", size=1, rows=10, columns=described)) == 160

    @pytest.mark.parametrize("rows, columns", [
        (None, 7), (1000, None), (None, None), (0, 7), (1000, 0),
        (-5, 7), ("many", 7), (1000, "wide"), (True, 7), (1000, True),
    ])
    def test_a_resource_that_records_no_usable_count_estimates_to_nothing(self, rows, columns):
        assert _resource_decoded_estimate(_res("f", size=1, rows=rows, columns=columns)) == 0

    def test_counts_stored_as_text_are_read(self):
        assert _resource_decoded_estimate(_res("f", size=1, rows="1000", columns="7")) == 56_000

    def test_the_cap_is_32_times_the_chunk_size_and_the_threshold_three_quarters_of_it(self):
        assert _COMPACTION_DECODED_RATIO == 32
        assert _decoded_size_cap(MAX_MEM) == 512 * MIB
        assert _decoded_size_threshold(MAX_MEM) == 384 * MIB
        assert _decoded_size_cap(1) == 32 and _decoded_size_threshold(1) == 24

    def test_a_chunk_closed_at_the_cap_is_above_the_threshold(self):
        """The hysteresis that keeps a merged file from being merged again."""
        for max_mem in (1, 7, 1000, 64 * 1024, MAX_MEM, 64 * MIB):
            assert _decoded_size_threshold(max_mem) <= _decoded_size_cap(max_mem)


class TestWhichFilesAreSmall:

    def test_small_on_disk_and_small_decoded_is_small(self):
        assert _is_small_file(_res("f", size=20_000, rows=5000, columns=7), MAX_MEM)

    def test_at_the_stored_size_threshold_it_is_not_as_before(self):
        edge = _small_file_threshold(MAX_MEM)
        assert _is_small_file(_res("f", size=edge - 1, rows=10, columns=2), MAX_MEM)
        assert not _is_small_file(_res("f", size=edge, rows=10, columns=2), MAX_MEM)

    def test_a_million_rows_of_fifty_columns_is_not_small_whatever_it_weighs(self):
        """416 MB decoded.  It was "small" at 8 MiB on disk, so every third
        such append merged three of them: 2 s and 5 GB instead of 0.2 s."""
        wide = _res("f", size=8 * MIB, rows=1_000_000, columns=52)
        assert not _is_small_file(wide, MAX_MEM)

    def test_five_million_rows_of_seven_columns_is_still_small_at_twelve_megabytes(self):
        """280 MB decoded: under the threshold, so such a table is merged up to
        its stored size as it always was."""
        narrow = _res("f", size=11 * MIB, rows=5_000_000, columns=7)
        assert _is_small_file(narrow, MAX_MEM)

    def test_the_decoded_threshold_is_exact(self):
        edge_rows = -(-_decoded_size_threshold(MAX_MEM) // (7 * 8))   # 7 columns, rounded up
        assert _is_small_file(_res("f", size=1000, rows=edge_rows - 1, columns=7), MAX_MEM)
        assert not _is_small_file(_res("f", size=1000, rows=edge_rows, columns=7), MAX_MEM)

    def test_counts_recorded_another_way_are_judged_the_same(self):
        """A list of column descriptions, or counts stored as text: a
        hand-built or older snapshot."""
        described = [{"name": f"c{i}", "type": "Int64"} for i in range(7)]
        assert not _is_small_file(
            _res("f", size=1000, rows=8_000_000, columns=described), MAX_MEM)
        assert _is_small_file(_res("f", size=1000, rows=1000, columns=described), MAX_MEM)
        assert not _is_small_file(_res("f", size=1000, rows="8000000", columns="7"), MAX_MEM)
        assert _is_small_file(_res("f", size=1000, rows="1000", columns="7"), MAX_MEM)

    def test_a_resource_without_counts_is_judged_on_its_stored_size_alone(self):
        """As every resource was before: an older or hand-built snapshot."""
        assert _is_small_file(_res("f", size=1000), MAX_MEM)
        assert not _is_small_file(_res("f", size=MAX_MEM), MAX_MEM)

    @pytest.mark.parametrize("rows, columns", [
        (-8_000_000, -7), (-8_000_000, 7), (8_000_000, -7), (0, 10 ** 12), (10 ** 12, 0),
    ])
    def test_counts_that_cannot_be_counts_do_not_make_a_file_big(self, rows, columns):
        assert _is_small_file(_res("f", size=1000, rows=rows, columns=columns), MAX_MEM)

    def test_the_per_table_chunk_size_scales_both_thresholds(self):
        file = _res("f", size=10_000, rows=10_000, columns=10)        # 800 KB decoded
        assert _is_small_file(file, MAX_MEM)
        assert not _is_small_file(file, 30_000)        # decoded threshold 720 KB
        assert _is_small_file(file, 40_000)            # decoded threshold 960 KB
        assert not _is_small_file(file, 12_000)        # stored threshold 9 KB


class TestTheGate:

    def _big_decoded(self, i: int) -> Dict:
        return _res(f"big{i}.parquet", size=2 * MIB, rows=8_000_000, columns=7)

    def test_files_that_are_big_decoded_do_not_open_it_by_count(self):
        resources = [self._big_decoded(i) for i in range(150)]
        assert should_compact_small_files(resources) is False
        assert count_small_files(resources) == 0

    def test_files_that_are_big_decoded_do_not_open_it_by_bytes(self):
        """Their stored bytes are well past a chunk; they are not small files."""
        resources = [self._big_decoded(i) for i in range(20)]
        assert sum(r["file_size"] for r in resources) > MAX_MEM
        assert should_compact_small_files(resources) is False

    def test_small_files_beside_them_still_do(self):
        resources = [self._big_decoded(i) for i in range(50)] + [
            _res(f"s{i}.parquet", size=20_000, rows=5000, columns=7) for i in range(100)]
        assert count_small_files(resources) == 100
        assert should_compact_small_files(resources) is True
        assert should_compact_small_files(resources[:-1]) is False

    def test_the_gate_and_the_candidates_agree_on_every_file(self, tmp_path):
        """A gate that opened on files the merge skipped would re-fire on
        every write with nothing to do."""
        mixed = [
            _res("a", size=20_000, rows=5000, columns=7),
            _res("b", size=2 * MIB, rows=8_000_000, columns=7),
            _res("c", size=13 * MIB, rows=10, columns=2),
            _res("d", size=1000),
            _res("e", size=1000, rows=5, columns=[{"name": "x"}]),
            _res(None, size=1000, rows=5, columns=1),
        ]
        small = [r["file"] for r in mixed if r["file"] and _is_small_file(r, MAX_MEM)]
        assert small == ["a", "d", "e"]
        assert count_small_files(mixed) == 3
        read: List[str] = []
        with patch.object(proc, "_read_parquet_safe", side_effect=lambda path, **k: read.append(path)):
            compact_resources(snapshot={"resources": mixed}, data_dir=str(tmp_path),
                              compression_level=1, small_only=True)
        assert read == small


# ---------------------------------------------------------------------------
# The merge, on real files
# ---------------------------------------------------------------------------

@pytest.fixture
def lake(tmp_path):
    from supertable.storage.local_storage import LocalStorage

    data_dir = str(tmp_path / "warehouse" / "orders" / "data")
    os.makedirs(data_dir, exist_ok=True)
    proc._storage = None
    reads: List[str] = []
    real_read = proc._read_parquet_safe

    def recording_read(path, *a, **k):
        reads.append(path)
        return real_read(path, *a, **k)

    with patch.object(proc, "get_storage", return_value=LocalStorage()), \
            patch.object(proc, "_read_parquet_safe", recording_read):
        yield type("Lake", (), {"data_dir": data_dir, "reads": reads})
    proc._storage = None


def _file(lake, name: str, first: int, rows: int, *, size: int | None = None,
          extra_columns: int = 0, constant_v: bool = False) -> Dict:
    """*rows* rows of ``__rowid__``, ``v`` and *extra_columns* more; a resource
    shaped as the writer shapes it (``rows`` and ``columns`` are counts)."""
    data = {ROWID: list(range(first, first + rows)),
            "v": [1.0] * rows if constant_v else [float(i) for i in range(first, first + rows)]}
    for c in range(extra_columns):
        data[f"x{c}"] = [c] * rows
    frame = pl.DataFrame(data)
    path = os.path.join(lake.data_dir, name)
    frame.write_parquet(path, compression="zstd", compression_level=1, statistics=True)
    return {"file": path, "file_size": int(size if size is not None else os.path.getsize(path)),
            "rows": rows, "columns": frame.width}


def _ids(resources) -> List[int]:
    ids: List[int] = []
    for r in resources:
        ids.extend(pq.ParquetFile(r["file"]).read().column(ROWID).to_pylist())
    return ids


# A chunk size small enough to reach the decoded bound with a few hundred rows:
# cap 80,000 estimated bytes = 10,000 values; threshold 60,000 = 7,500 values.
SMALL_CHUNK = {"max_memory_chunk_size": 80_000 // _COMPACTION_DECODED_RATIO}
assert SMALL_CHUNK == {"max_memory_chunk_size": 2500}


class TestAChunkIsClosedOnItsDecodedSize:

    def test_files_that_fit_one_chunk_on_disk_are_written_as_several(self, lake):
        """Twelve files of 500 rows by 2 columns: 8,000 estimated bytes each
        and, as recorded here, 100 stored bytes.  Stored bytes would put all
        twelve in one chunk; ten of them reach the decoded cap."""
        files = [_file(lake, f"f{i}.parquet", i * 500, 500, size=100) for i in range(12)]

        considered, rows, written, sunset = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir,
            compression_level=1, table_config=SMALL_CHUNK)

        assert (considered, rows) == (12, 6000)
        assert sunset == {f["file"] for f in files}
        assert [w["rows"] for w in written] == [5000, 1000]
        assert sorted(_ids(written)) == list(range(6000)), "every row, once"

    def test_a_file_closed_on_its_decoded_size_is_not_merged_again(self, lake):
        """Files of 5,000 rows by 20 columns, all but the row id constant:
        800,000 estimated bytes each, so four close a chunk of this table (cap
        3,200,000).  What that chunk is written as stays under the table's
        stored-size threshold, so its decoded size is the only thing that keeps
        it out of the next merge."""
        config = {"max_memory_chunk_size": 100_000}
        files = [_file(lake, f"f{i}.parquet", i * 5000, 5000, size=100, extra_columns=18,
                       constant_v=True) for i in range(9)]
        _, _, written, _ = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir,
            compression_level=1, table_config=config)
        assert [w["rows"] for w in written] == [20_000, 20_000, 5000]
        closed, remainder = written[:2], written[2]
        for w in written:
            assert w["file_size"] < _small_file_threshold(100_000), "small on disk, all three"
        assert [_is_small_file(w, 100_000) for w in written] == [False, False, True]
        lake.reads.clear()

        considered, _, again, sunset = compact_resources(
            snapshot={"resources": written}, data_dir=lake.data_dir,
            compression_level=1, table_config=config)

        assert lake.reads == [remainder["file"]], "the closed files are never opened"
        assert sunset == {remainder["file"]}
        assert considered == 1
        assert sorted(_ids(closed + again)) == list(range(45_000))
        assert should_compact_small_files(closed * 100, config) is False

    def test_the_bound_counts_columns_as_well_as_rows(self, lake):
        """500 rows of 10 columns are 40,000 estimated bytes: two reach the cap."""
        files = [_file(lake, f"w{i}.parquet", i * 500, 500, size=100, extra_columns=8)
                 for i in range(5)]

        _, rows, written, _ = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir,
            compression_level=1, table_config=SMALL_CHUNK)

        assert rows == 2500
        assert [w["rows"] for w in written] == [1000, 1000, 500]

    def test_stored_bytes_still_close_a_chunk_first_when_they_are_reached_first(self, lake):
        """Four files of 50 rows recorded at 6,000 stored bytes, in a table
        whose chunk is 10,000: the chunk closes after two, as it always did."""
        files = [_file(lake, f"b{i}.parquet", i * 50, 50, size=6000) for i in range(4)]

        _, rows, written, _ = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir,
            compression_level=1, table_config={"max_memory_chunk_size": 10_000})

        assert rows == 200
        assert [w["rows"] for w in written] == [100, 100]

    def test_under_both_bounds_everything_is_one_file_as_before(self, lake):
        files = [_file(lake, f"s{i}.parquet", i * 10, 10) for i in range(30)]

        considered, rows, written, _ = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir, compression_level=1)

        assert (considered, rows, len(written)) == (30, 300, 1)
        assert sorted(_ids(written)) == list(range(300))

    def test_a_file_that_is_big_decoded_is_left_alone_beside_small_ones(self, lake):
        """With stored sizes recorded small, only the decoded size tells them apart."""
        big = _file(lake, "big.parquet", 0, 4000, size=50)
        small = [_file(lake, f"s{i}.parquet", 4000 + i * 10, 10, size=50) for i in range(3)]

        considered, rows, written, sunset = compact_resources(
            snapshot={"resources": [small[0], big, small[1], small[2]]},
            data_dir=lake.data_dir, compression_level=1, table_config=SMALL_CHUNK)

        assert lake.reads == [s["file"] for s in small]
        assert sunset == {s["file"] for s in small}
        assert (considered, rows) == (3, 30)
        assert sorted(_ids(written)) == list(range(4000, 4030))

    def test_rewriting_every_file_is_bounded_the_same_way(self, lake):
        """``small_only=False``: an export or a full rewrite.  A file past the
        bound is still read, and closes the chunk it is in."""
        big = _file(lake, "big.parquet", 0, 6000, size=50)             # 96,000: past the cap
        small = [_file(lake, f"s{i}.parquet", 6000 + i * 100, 100, size=50) for i in range(3)]

        considered, rows, written, sunset = compact_resources(
            snapshot={"resources": [small[0], big, small[1], small[2]]},
            data_dir=lake.data_dir, compression_level=1,
            table_config=SMALL_CHUNK, small_only=False)

        assert considered == 4 and rows == 6300
        assert sunset == {big["file"]} | {s["file"] for s in small}
        assert [w["rows"] for w in written] == [6100, 200]
        assert sorted(_ids(written)) == list(range(6300))

    def test_rows_dropped_from_an_export_do_not_count_towards_the_bound(self, lake):
        """The bound is on what is buffered: with every other row deleted, twice
        as many files fit a chunk."""
        files = [_file(lake, f"f{i}.parquet", i * 500, 500, size=100) for i in range(12)]
        dead = pl.DataFrame({ROWID: list(range(0, 6000, 2))})

        _, rows, written, _ = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir, compression_level=1,
            table_config=SMALL_CHUNK, small_only=False, dead_rowids=dead)

        assert rows == 3000
        assert [w["rows"] for w in written] == [3000]
        assert sorted(_ids(written)) == list(range(1, 6000, 2))

    def test_a_file_with_no_rows_left_adds_nothing_to_the_bound(self, lake):
        files = [_file(lake, f"f{i}.parquet", i * 500, 500, size=100) for i in range(11)]
        dead = pl.DataFrame({ROWID: list(range(500))})            # all of the first file

        _, rows, written, sunset = compact_resources(
            snapshot={"resources": files}, data_dir=lake.data_dir, compression_level=1,
            table_config=SMALL_CHUNK, small_only=False, dead_rowids=dead)

        assert rows == 5000
        assert [w["rows"] for w in written] == [5000], "ten files of 500 reach the cap exactly"
        assert len(sunset) == 11
