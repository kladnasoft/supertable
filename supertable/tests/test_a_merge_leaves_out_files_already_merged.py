"""
A merge that the file count triggered leaves out the files that are a large
share of it.

When a hundred small files have piled up, a write merges them.  What it writes
is usually small again, and every later merge used to take it back in: read it,
decode it and rewrite it with the hundred files that had arrived since, a
bigger file each time, until it passed the size threshold.  Over 1,400 appends
of 5,000 rows each row was rewritten five times and merges grew from 0.4 s to
0.9 s; one-row appends rewrote the whole table every hundred writes.

``select_small_files_to_merge`` picks what a write merges once the gate is
open.  A file joins a merge only when it holds at most a tenth of the rows
merged, so every rewrite of a row moves it into a file at least ten times
bigger.  Where merging everything leaves nothing to merge again, or where the
rule cannot keep the number of small files down, every small file is merged,
as before.
"""
from __future__ import annotations

import copy
import os
import random
from typing import Dict, List

import pytest

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.processing import (  # noqa: E402
    _COMPACTION_TIER_FANIN,
    _decoded_size_cap,
    _decoded_size_threshold,
    _estimated_decoded_bytes,
    _is_small_file,
    _resolve_limits,
    count_small_files,
    select_small_files_to_merge,
    should_compact_small_files,
)

MIB = 1024 * 1024
COLUMNS = 7


def _res(name: str, rows, *, size: int | None = None, columns=COLUMNS) -> Dict:
    """A resource as the writer records it.  Stored size: a footer and a few
    bytes a row, unless given."""
    resource = {"file": name, "file_size": 2000 + 3 * int(rows or 0) if size is None else size}
    if rows is not None:
        resource["rows"] = rows
    if columns is not None:
        resource["columns"] = columns
    return resource


def _fresh(count: int, rows: int = 10, start: int = 0) -> List[Dict]:
    return [_res(f"fresh_{start + i}.parquet", rows) for i in range(count)]


def _names(resources) -> List[str]:
    return [r["file"] for r in resources]


# ---------------------------------------------------------------------------
# Where every small file is merged, as before
# ---------------------------------------------------------------------------

class TestEverySmallFileIsMerged:

    def test_when_they_are_all_one_size(self):
        """The first merge of a table: a hundred appends."""
        files = _fresh(100, rows=5000)
        assert select_small_files_to_merge(files) == files

    def test_when_their_stored_bytes_fill_a_chunk(self):
        """The gate's byte trigger.  What is written is a full file, or more:
        nothing is left to merge again, whatever share one file is."""
        files = [_res("merged.parquet", 400_000, size=10 * MIB)] + [
            _res(f"f{i}.parquet", 3000, size=200_000) for i in range(40)]
        max_mem, max_files = _resolve_limits(None)
        assert sum(f["file_size"] for f in files) > max_mem and len(files) < max_files
        assert select_small_files_to_merge(files) == files

    def test_when_what_they_decode_to_is_a_big_enough_file(self):
        """Stored in 11 MiB; 7,500,000 rows of 7 columns: over the decoded
        threshold, so the file written is not merged again either."""
        files = [_res("merged.parquet", 7_000_000, size=9 * MIB)] + [
            _res(f"fresh_{i}.parquet", 5000, size=20_000) for i in range(99)]
        estimate = sum(_estimated_decoded_bytes(f["rows"], COLUMNS) for f in files)
        assert estimate >= _decoded_size_threshold(16 * MIB)
        assert sum(f["file_size"] for f in files) < 16 * MIB
        assert count_small_files(files) == 100
        assert select_small_files_to_merge(files) == files

    def test_but_not_when_it_falls_short_of_that(self):
        """The same hundred files with a million rows fewer: the merged file is
        most of them, and stays out."""
        merged = _res("merged.parquet", 6_000_000, size=9 * MIB)
        fresh = [_res(f"fresh_{i}.parquet", 5000, size=20_000) for i in range(99)]
        assert select_small_files_to_merge([merged] + fresh) == fresh

    @pytest.mark.parametrize("limit", [2, 3, 5, 10])
    def test_in_a_table_with_a_low_file_limit(self, limit):
        """Leaving one file out takes ten others to merge, which never fit
        under such a limit: every file is merged, and the existing suites keep
        their behaviour."""
        files = [_res("merged.parquet", 3 * limit)] + _fresh(limit - 1, rows=3)
        config = {"max_overlapping_files": limit}
        assert should_compact_small_files(files, config)
        assert select_small_files_to_merge(files, config) == files

    def test_when_no_file_records_its_rows(self):
        files = [_res(f"f{i}.parquet", None, size=20_000) for i in range(100)]
        assert select_small_files_to_merge(files) == files

    def test_when_leaving_files_out_would_leave_too_many(self):
        """Sixty files, each an eighth bigger than the one before, and forty
        new ones.  Every one of the sixty is more than a tenth of the files up
        to it, so the rule would take the forty and leave sixty-one small
        files in a table whose limit is a hundred: everything is merged."""
        spread = [_res(f"m{i}.parquet", int(100 * 1.125 ** i)) for i in range(60)]
        files = spread + _fresh(40, rows=10)
        assert count_small_files(files) == 100
        assert sum(f["file_size"] for f in files) < 16 * MIB
        assert select_small_files_to_merge(files) == files

    def test_but_not_while_half_the_limit_or_fewer_would_be_left(self):
        """The same forty-nine largest files and fifty-one new ones: fifty
        small files remain after the merge, which is the most it may leave."""
        spread = [_res(f"m{i}.parquet", int(100 * 1.125 ** i)) for i in range(11, 60)]
        fresh = _fresh(51, rows=10)
        assert select_small_files_to_merge(spread + fresh) == fresh
        one_more = [_res("m10.parquet", int(100 * 1.125 ** 10))] + spread
        assert select_small_files_to_merge(one_more + fresh[:50]) == one_more + fresh[:50]

    def test_when_there_is_one_or_none(self):
        assert select_small_files_to_merge([]) == []
        assert select_small_files_to_merge(None) == []
        only = _fresh(1)
        assert select_small_files_to_merge(only) == only


# ---------------------------------------------------------------------------
# Where a merged file is left for later
# ---------------------------------------------------------------------------

class TestAFileThatIsALargeShareIsLeftOut:

    def test_the_file_the_last_merge_wrote(self):
        merged = _res("merged.parquet", 1000)              # a hundred files of ten rows
        fresh = _fresh(99)
        selected = select_small_files_to_merge([merged] + fresh)
        assert selected == fresh
        assert merged not in selected

    @pytest.mark.parametrize("merged_files", [2, 3, 5, 9])
    def test_up_to_nine_of_them(self, merged_files):
        merged = [_res(f"merged_{i}.parquet", 1000 - 10 * i) for i in range(merged_files)]
        fresh = _fresh(100 - merged_files)
        assert select_small_files_to_merge(merged + fresh) == fresh

    def test_ten_of_one_size_are_merged_together_with_the_new_files(self):
        merged = [_res(f"merged_{i}.parquet", 1000) for i in range(10)]
        fresh = _fresh(90)
        files = merged + fresh
        assert select_small_files_to_merge(files) == files

    def test_a_file_one_size_up_is_left_while_the_size_below_is_merged(self):
        bigger = _res("bigger.parquet", 10_900)
        merged = [_res(f"merged_{i}.parquet", 1000) for i in range(10)]
        fresh = _fresh(89)
        selected = select_small_files_to_merge([bigger] + merged + fresh)
        assert selected == merged + fresh

    def test_the_share_is_one_row_in_ten(self):
        """Exactly a tenth joins; one row more does not."""
        assert _COMPACTION_TIER_FANIN == 10
        fresh = _fresh(90, rows=10)                        # 900 rows
        joins = _res("edge.parquet", 100)                  # 100 of 1000
        stays = _res("edge.parquet", 101)                  # 101 of 1001
        assert select_small_files_to_merge([joins] + fresh) == [joins] + fresh
        assert select_small_files_to_merge([stays] + fresh) == fresh

    def test_leaving_one_file_out_takes_ten_others_to_merge(self):
        merged = _res("merged.parquet", 30)
        ten, nine = _fresh(10, rows=3), _fresh(9, rows=3)
        assert select_small_files_to_merge(
            [merged] + ten, {"max_overlapping_files": 11}) == ten
        assert select_small_files_to_merge(
            [merged] + nine, {"max_overlapping_files": 10}) == [merged] + nine

    def test_rows_decide_not_stored_bytes(self):
        """A file of a few rows is mostly footer: a merged file of a thousand
        rows weighs little more than a new one of ten."""
        merged = _res("merged.parquet", 1000, size=2100)
        fresh = [_res(f"fresh_{i}.parquet", 10, size=2000) for i in range(99)]
        assert select_small_files_to_merge([merged] + fresh) == fresh

    def test_a_file_without_a_row_count_is_always_taken(self):
        """It weighs nothing in the comparison: what happened to every file before."""
        unknown = _res("old.parquet", None, size=50_000)
        merged = _res("merged.parquet", 1000)
        fresh = _fresh(98)
        assert select_small_files_to_merge([merged, unknown] + fresh) == [unknown] + fresh


# ---------------------------------------------------------------------------
# What is returned
# ---------------------------------------------------------------------------

class TestWhatIsReturned:

    def test_the_files_come_back_in_snapshot_order_not_in_size_order(self):
        """The merge concatenates them in this order, and rows in the order
        they were written need no sort."""
        files = _fresh(50, rows=10) + [_res("merged.parquet", 1000)] + [
            _res(f"later_{i}.parquet", 3 + i % 4) for i in range(49)]
        selected = select_small_files_to_merge(files)
        assert _names(selected) == [n for n in _names(files) if n != "merged.parquet"]

    def test_they_are_the_snapshot_s_own_entries_and_the_snapshot_is_untouched(self):
        files = [_res("merged.parquet", 1000)] + _fresh(99)
        before = copy.deepcopy(files)
        selected = select_small_files_to_merge(files)
        assert files == before
        assert all(any(s is f for f in files) for s in selected)

    def test_a_file_that_is_not_small_is_never_among_them(self):
        large_on_disk = _res("large.parquet", 100, size=13 * MIB)
        large_decoded = _res("wide.parquet", 8_000_000, size=2 * MIB)
        no_path = {"file": None, "file_size": 100, "rows": 1, "columns": 1}
        for files in (
            [large_on_disk, large_decoded, no_path] + _fresh(100),
            [large_on_disk, large_decoded, no_path, _res("merged.parquet", 1000)] + _fresh(99),
        ):
            selected = select_small_files_to_merge(files)
            assert selected, "something to merge"
            for resource in selected:
                assert resource["file"] and _is_small_file(resource, 16 * MIB)

    def test_the_per_table_limits_are_the_ones_used(self):
        files = [_res("merged.parquet", 1000)] + _fresh(99)
        assert len(select_small_files_to_merge(files)) == 99
        # A chunk of 3,000 bytes: these files are not small for that table.
        assert select_small_files_to_merge(files, {"max_memory_chunk_size": 2000}) == []
        # A file limit of 400: leaving one file out is still fine.
        assert len(select_small_files_to_merge(files, {"max_overlapping_files": 400})) == 99


# ---------------------------------------------------------------------------
# What the rule guarantees, on any set of files
# ---------------------------------------------------------------------------

class TestWhatTheRuleGuarantees:

    @pytest.mark.parametrize("limit", [20, 50, 100, 200])
    def test_on_random_tables(self, limit):
        rng = random.Random(limit)
        config = {"max_overlapping_files": limit}
        tiered = merged_everything = 0
        for round_ in range(600):
            files = []
            if round_ % 2:
                # Any sizes at all, over four orders of magnitude.
                for i in range(rng.randint(limit, limit + 15)):
                    rows = int(10 ** rng.uniform(0, rng.choice([1, 2, 4])))
                    files.append(_res(f"f{i}.parquet", rows))
            else:
                # The shape a table grows into: a few merged files, many new ones.
                for i in range(rng.randint(0, limit // 2 + 3)):
                    files.append(_res(f"m{i}.parquet", rng.randint(200, 20_000)))
                for i in range(limit - len(files) + rng.randint(0, 5)):
                    files.append(_res(f"f{i}.parquet", rng.randint(1, 20)))
                rng.shuffle(files)
            small = [f for f in files if _is_small_file(f, 16 * MIB)]
            selected = select_small_files_to_merge(files, config)

            # Never anything that is not a small file of this snapshot, in order.
            assert _names(selected) == [n for n in _names(small) if n in set(_names(selected))]
            if selected == small:
                merged_everything += 1
                continue
            tiered += 1
            rows_selected = sum(f["rows"] for f in selected)
            left = [f for f in small if f not in selected]
            # Each file merged is at most a tenth of the merge ...
            assert max(f["rows"] for f in selected) * _COMPACTION_TIER_FANIN <= rows_selected
            # ... the files left out are the largest, each too big to join ...
            assert min(f["rows"] for f in left) >= max(f["rows"] for f in selected)
            assert min(f["rows"] for f in left) * _COMPACTION_TIER_FANIN > (
                rows_selected + min(f["rows"] for f in left))
            # ... and no more than half the limit is left behind.
            assert len(left) + 1 <= max(1, limit // 2)
            assert len(selected) >= 2
        assert tiered > 50 and merged_everything > 50, "both outcomes must have been exercised"


# ---------------------------------------------------------------------------
# Over many writes: a model of the table, the real gate and the real selection
# ---------------------------------------------------------------------------

class _Table:
    """Files as (rows) with a modelled stored size; merges by the real rules.

    The gate (``should_compact_small_files``) and the selection are the
    library's.  The merge itself is modelled: the selected files become one
    file per chunk, a chunk closing as ``compact_resources`` closes it.
    """

    def __init__(self, bytes_per_row: float, *, select, limit: int = 100):
        self.bytes_per_row = bytes_per_row
        self.select = select
        self.config = {"max_overlapping_files": limit}
        self.max_mem, self.limit = _resolve_limits(self.config)
        self.files: List[Dict] = []
        self.serial = 0
        self.rows_rewritten = 0
        self.rows_appended = 0
        self.most_small_files = 0
        self.largest_merge = 0

    def _new(self, rows: int) -> Dict:
        self.serial += 1
        return _res(f"file_{self.serial}.parquet", rows,
                    size=2000 + int(rows * self.bytes_per_row))

    def append(self, rows: int) -> None:
        self.files.append(self._new(rows))
        self.rows_appended += rows
        if should_compact_small_files(self.files, self.config):
            self._merge(self.select(self.files, self.config))
        self.most_small_files = max(
            self.most_small_files, count_small_files(self.files, self.config))

    def _merge(self, chosen: List[Dict]) -> None:
        names = set(_names(chosen))
        written, rows, stored, decoded = [], 0, 0, 0
        for resource in chosen:
            rows += resource["rows"]
            stored += resource["file_size"]
            decoded += _estimated_decoded_bytes(resource["rows"], COLUMNS)
            if stored >= self.max_mem or decoded >= _decoded_size_cap(self.max_mem):
                written.append(self._new(rows))
                rows = stored = decoded = 0
        if rows:
            written.append(self._new(rows))
        merged_rows = sum(r["rows"] for r in chosen)
        self.rows_rewritten += merged_rows
        self.largest_merge = max(self.largest_merge, merged_rows)
        self.files = [f for f in self.files if f["file"] not in names] + written


def _every_small_file(resources, table_config=None):
    """What a merge took before: all of them."""
    max_mem, _ = _resolve_limits(table_config)
    return [r for r in resources if r.get("file") and _is_small_file(r, max_mem)]


class TestOverManyWrites:

    @pytest.mark.parametrize("batch_rows, bytes_per_row, writes, was_at_least", [
        (1, 40.0, 6000, 25.0),          # one-row appends
        (10, 40.0, 6000, 25.0),
        (100, 40.0, 6000, 12.0),        # small batches of ordinary rows
        (1000, 2.4, 6000, 8.0),         # rows that compress very well
    ])
    def test_a_row_is_rewritten_a_few_times_not_once_per_merge(
            self, batch_rows, bytes_per_row, writes, was_at_least):
        """``before`` differs from ``now`` in the selection alone: it takes
        every small file, as a merge did."""
        before = _Table(bytes_per_row, select=_every_small_file)
        now = _Table(bytes_per_row, select=select_small_files_to_merge)
        for _ in range(writes):
            before.append(batch_rows)
            now.append(batch_rows)

        assert before.rows_rewritten / before.rows_appended >= was_at_least
        assert now.rows_rewritten / now.rows_appended <= 3.0
        # The gate still does its job: never a hundred small files after a write.
        assert now.most_small_files < 100
        assert before.most_small_files < 100
        # And no merge reads more than the largest one did before.
        assert now.largest_merge <= max(before.largest_merge, 2 * 10 ** 6)

    def test_ordinary_batches_that_fill_a_chunk_are_merged_exactly_as_before(self):
        """5,000 rows at 40 bytes a row: eighty-four files exceed a chunk, the
        byte trigger opens the gate, and every small file is merged."""
        before = _Table(40.0, select=_every_small_file)
        now = _Table(40.0, select=select_small_files_to_merge)
        for _ in range(3000):
            before.append(5000)
            now.append(5000)
        assert [f["rows"] for f in now.files] == [f["rows"] for f in before.files]
        assert now.rows_rewritten == before.rows_rewritten

    @pytest.mark.parametrize("limit", [2, 3, 10])
    def test_a_table_with_a_low_file_limit_behaves_exactly_as_before(self, limit):
        before = _Table(40.0, select=_every_small_file, limit=limit)
        now = _Table(40.0, select=select_small_files_to_merge, limit=limit)
        for _ in range(500):
            before.append(100)
            now.append(100)
        assert [f["rows"] for f in now.files] == [f["rows"] for f in before.files]
        assert now.rows_rewritten == before.rows_rewritten
