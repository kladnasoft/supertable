"""
Rows that are already in timestamp order are not sorted again before encoding.

Every data file is sorted by ``__timestamp__`` before it is encoded, so each
row group covers a tight range.  A merge of files written one after another
concatenates rows that are in that order already, and the sort then moves
nothing while costing two more copies of the decoded rows: a merge of
2,000,000 rows held 3.7 times its decoded input with the sort and 1.7 times
without it, and spent a fifth of its time in it.

``_rows_already_in_order`` answers the question from the one column, and the
writer skips the sort when the answer is yes.  The file it writes must be the
file the sort would have produced.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import List
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import processing as proc  # noqa: E402
from supertable.processing import (  # noqa: E402
    _rows_already_in_order,
    compact_resources,
    write_parquet_and_collect_resources,
)

TS = "__timestamp__"
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _at(second: int) -> datetime:
    return T0 + timedelta(seconds=second)


def _frame(seconds: List, first_id: int = 0) -> pl.DataFrame:
    """One row per entry of *seconds* (``None`` is a null timestamp).

    Read back from Arrow, as a merge reads its files: polars then holds no
    "sorted" flag for the column and has to look at the values.
    """
    built = pl.DataFrame({
        "id": list(range(first_id, first_id + len(seconds))),
        "name": [f"row-{first_id + i}" for i in range(len(seconds))],
        TS: pl.Series([None if s is None else _at(s) for s in seconds],
                      dtype=pl.Datetime("us", "UTC")),
    })
    frame = pl.from_arrow(built.to_arrow())
    if len(seconds) > 1:
        assert not frame[TS].flags["SORTED_ASC"], "the test must not lean on polars' own flag"
    return frame


@pytest.fixture
def lake(tmp_path):
    """Real LocalStorage under tmp_path, and a count of the sorts performed."""
    from supertable.storage.local_storage import LocalStorage

    data_dir = str(tmp_path / "warehouse" / "orders" / "data")
    os.makedirs(data_dir, exist_ok=True)
    proc._storage = None
    sorts: List[List[str]] = []
    real_sort = pl.DataFrame.sort

    def counting_sort(self, by=None, *more, **options):
        sorts.append(list(by) if isinstance(by, (list, tuple)) else [by])
        return real_sort(self, by, *more, **options)

    with patch.object(proc, "get_storage", return_value=LocalStorage()), \
            patch.object(pl.DataFrame, "sort", counting_sort):
        yield type("Lake", (), {"data_dir": data_dir, "sorts": sorts})
    proc._storage = None


def _write(lake, frame: pl.DataFrame, overwrite_columns=()) -> dict:
    resources: list = []
    write_parquet_and_collect_resources(
        write_df=frame, overwrite_columns=list(overwrite_columns),
        data_dir=lake.data_dir, new_resources=resources, compression_level=1)
    assert len(resources) == 1
    return resources[0]


def _stored(resource: dict) -> pl.DataFrame:
    return pl.from_arrow(pq.ParquetFile(resource["file"]).read())


# ---------------------------------------------------------------------------
# The question
# ---------------------------------------------------------------------------

class TestWhetherRowsAreInOrder:

    @pytest.mark.parametrize("seconds", [
        [1, 2, 3, 4],
        [1, 1, 1, 1],                 # one write: every row carries one timestamp
        [1, 1, 2, 2, 2, 3],           # several writes, merged
        [None, None, 1, 2],           # nulls first is where the sort puts them
        [None, None],
        [5],
    ])
    def test_these_are(self, seconds):
        frame = _frame(seconds)
        assert _rows_already_in_order(frame, [TS]) is True
        assert frame.sort([TS]).equals(frame), "and sorting them changes nothing"

    @pytest.mark.parametrize("seconds", [
        [2, 1],
        [1, 2, 3, 2],
        [3, 2, 1],
        [1, None, 2],                 # a null after a value
        [1, 2, None],                 # nulls last: the sort would move them first
    ])
    def test_these_are_not(self, seconds):
        frame = _frame(seconds)
        assert _rows_already_in_order(frame, [TS]) is False
        assert not frame.sort([TS]).equals(frame), "the sort has work to do here"

    def test_no_rows_are_in_order(self):
        assert _rows_already_in_order(_frame([]), [TS]) is True

    def test_many_chunks_are_looked_at_as_one_column(self):
        ordered = pl.concat([_frame([1, 1]), _frame([2], 2), _frame([2, 3], 3)])
        assert ordered.n_chunks() == 3
        assert _rows_already_in_order(ordered, [TS]) is True
        swapped = pl.concat([_frame([2, 3]), _frame([1, 1], 2)])
        assert _rows_already_in_order(swapped, [TS]) is False

    @pytest.mark.parametrize("sort_cols", [
        ["id"], [TS, "id"], ["id", TS], [],
    ])
    def test_only_a_sort_by_the_timestamp_alone_is_answered(self, sort_cols):
        """Rows ordered by every column here; any other sort is still performed."""
        frame = _frame([1, 2, 3])
        assert _rows_already_in_order(frame, sort_cols) is False

    def test_a_frame_without_the_column_is_to_be_sorted(self):
        assert _rows_already_in_order(pl.DataFrame({"id": [1, 2]}), [TS]) is False

    def test_a_column_that_cannot_be_asked_is_to_be_sorted(self):
        """``False`` is the safe answer: it means "sort"."""
        frame = _frame([1, 2])
        with patch.object(pl.Series, "is_sorted", side_effect=RuntimeError("no")):
            assert _rows_already_in_order(frame, [TS]) is False


# ---------------------------------------------------------------------------
# The writer
# ---------------------------------------------------------------------------

class TestTheWriter:

    def test_rows_in_order_are_written_without_a_sort(self, lake):
        frame = _frame([1, 1, 1, 2, 2, 3])

        stored = _stored(_write(lake, frame))

        assert lake.sorts == []
        assert stored.equals(frame), "the same rows, in the order they came in"

    def test_rows_out_of_order_are_sorted_as_before(self, lake):
        frame = _frame([3, 1, 2, 1])

        stored = _stored(_write(lake, frame))

        assert lake.sorts == [[TS]]
        assert stored[TS].to_list() == [_at(1), _at(1), _at(2), _at(3)]
        assert sorted(stored["id"].to_list()) == [0, 1, 2, 3]

    def test_a_sort_that_names_other_columns_is_performed_as_before(self, lake):
        frame = _frame([1, 2, 3])                       # in order by every column

        _write(lake, frame, overwrite_columns=["id"])

        assert lake.sorts == [[TS, "id"]]

    def test_a_frame_without_a_timestamp_is_sorted_by_its_keys_as_before(self, lake):
        frame = pl.DataFrame({"id": [1, 2, 3], "v": ["a", "b", "c"]})

        stored = _stored(_write(lake, frame, overwrite_columns=["id"]))

        assert lake.sorts == [["id"]]
        assert stored["id"].to_list() == [1, 2, 3]

    def test_the_file_is_byte_for_byte_the_one_the_sort_would_have_written(self, lake):
        """What makes skipping the sort invisible: for rows already in order
        the sort is the identity, and the encoder writes the same row groups
        from many chunks as from one."""
        parts = [_frame([second] * 700, first_id=second * 700) for second in range(9)]
        frame = pl.concat(parts)
        assert frame.n_chunks() == 9

        with patch.object(proc, "_PARQUET_ROW_GROUP_SIZE", 1000):
            skipped = _write(lake, frame)
            assert lake.sorts == []
            with patch.object(proc, "_rows_already_in_order", return_value=False):
                sorted_first = _write(lake, frame)
            assert lake.sorts == [[TS]]

        with open(skipped["file"], "rb") as a, open(sorted_first["file"], "rb") as b:
            assert a.read() == b.read()
        footer = pq.read_metadata(skipped["file"])
        assert [footer.row_group(i).num_rows for i in range(footer.num_row_groups)] == [
            1000, 1000, 1000, 1000, 1000, 1000, 300]

    def test_statistics_of_a_file_written_without_the_sort_are_those_of_its_rows(self, lake):
        frame = pl.concat([_frame([s] * 400, first_id=s * 400) for s in range(5)])

        with patch.object(proc, "_PARQUET_ROW_GROUP_SIZE", 500):
            resource = _write(lake, frame)

        assert lake.sorts == []
        footer = pq.read_metadata(resource["file"])
        assert footer.num_rows == 2000
        position = footer.schema.names.index("id")
        first = 0
        for group in range(footer.num_row_groups):
            column = footer.row_group(group).column(position)
            rows = footer.row_group(group).num_rows
            assert (column.statistics.min, column.statistics.max) == (first, first + rows - 1)
            first += rows


# ---------------------------------------------------------------------------
# A merge
# ---------------------------------------------------------------------------

class TestAMerge:

    def _files(self, lake, seconds_per_file) -> list:
        resources, first = [], 0
        for seconds in seconds_per_file:
            resources.append(_write(lake, _frame(seconds, first_id=first)))
            first += len(seconds)
        lake.sorts.clear()
        return resources

    def test_files_written_one_after_another_are_merged_without_a_sort(self, lake):
        resources = self._files(lake, [[1, 1, 1], [2, 2], [3], [3, 4, 4]])

        considered, rows, merged, _sunset = compact_resources(
            snapshot={"resources": resources}, data_dir=lake.data_dir, compression_level=1)

        assert (considered, rows, len(merged)) == (4, 9, 1)
        assert lake.sorts == []
        stored = _stored(merged[0])
        assert stored["id"].to_list() == list(range(9)), "in the order the files were written"
        assert stored[TS].is_sorted()

    def test_files_listed_out_of_order_are_merged_with_the_sort(self, lake):
        resources = self._files(lake, [[1, 1, 1], [2, 2], [3]])

        _, rows, merged, _ = compact_resources(
            snapshot={"resources": list(reversed(resources))},
            data_dir=lake.data_dir, compression_level=1)

        assert rows == 6
        assert lake.sorts == [[TS]]
        stored = _stored(merged[0])
        assert stored[TS].is_sorted()
        assert sorted(stored["id"].to_list()) == list(range(6))

    def test_a_file_with_no_timestamp_among_them_is_merged_with_the_sort(self, lake):
        """Its rows get a null timestamp in the merged frame, after rows that
        have one: not where the sort puts nulls."""
        resources = self._files(lake, [[1, 1], [2]])
        legacy: list = []
        write_parquet_and_collect_resources(
            write_df=pl.DataFrame({"id": [7, 8], "name": ["x", "y"]}), overwrite_columns=[],
            data_dir=lake.data_dir, new_resources=legacy, compression_level=1)
        lake.sorts.clear()

        _, rows, merged, _ = compact_resources(
            snapshot={"resources": resources + legacy},
            data_dir=lake.data_dir, compression_level=1)

        assert rows == 5
        assert lake.sorts == [[TS]]
        stored = _stored(merged[0])
        assert stored[TS].null_count() == 2
        assert stored[TS].to_list()[:2] == [None, None], "nulls first, as the sort leaves them"
        assert sorted(stored["id"].to_list()) == [0, 1, 2, 7, 8]
