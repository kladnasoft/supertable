"""
Unit tests for small-file compaction in supertable.processing.

Covers the three fixes introduced to restore proper compaction:

  1. prune_not_overlapping_files_by_threshold — includes ALL False files
     once the gate opens (no longer drops files at MAX_MEMORY_CHUNK_SIZE).
  2. process_files_with_overlap — returns a 4-tuple; the 4th element is the
     list of skipped (zero-match) has_overlap=True files.
  3. process_overlapping_files Phase 3 — when skipped file count reaches
     MAX_OVERLAPPING_FILES, compacts them into the merged output.

All storage and I/O is mocked.  Tests exercise only the processing logic.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple
from unittest.mock import patch, MagicMock

import polars as pl
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MOD = "supertable.processing"


def _df(data: dict, **schema_overrides) -> pl.DataFrame:
    return pl.DataFrame(data, schema_overrides=schema_overrides)


def _overlap_set(items: list) -> Set[Tuple[str, bool, int]]:
    """Build an overlapping_files set from [(file, has_overlap, size), ...]."""
    return set(items)


class _FakeStorage:
    """In-memory storage for parquet reads and writes within a single test."""

    def __init__(self):
        self.files: Dict[str, pl.DataFrame] = {}
        self.written_dfs: List[pl.DataFrame] = []
        self.written_resources: List[Dict] = []

    # -- read side --
    def read(self, path: str) -> Optional[pl.DataFrame]:
        return self.files.get(path)

    # -- write side (replaces write_parquet_and_collect_resources) --
    def fake_write(self, write_df, overwrite_columns, data_dir, new_resources,
                   compression_level=10):
        self.written_dfs.append(write_df.clone())
        new_resources.append({
            "file": f"{data_dir}/out_{len(self.written_resources)}.parquet",
            "file_size": write_df.estimated_size(),
            "rows": write_df.shape[0],
            "columns": write_df.shape[1],
            "stats": {},
        })
        self.written_resources.append(new_resources[-1])


@pytest.fixture
def store():
    return _FakeStorage()


@pytest.fixture
def patched(store):
    """Patch _read_parquet_safe and write_parquet_and_collect_resources."""
    with patch(f"{_MOD}._read_parquet_safe", side_effect=store.read), \
         patch(f"{_MOD}.write_parquet_and_collect_resources",
               side_effect=store.fake_write):
        yield store


def _all_written_rows(store: _FakeStorage) -> pl.DataFrame:
    """Concat all DataFrames that were flushed to storage."""
    if not store.written_dfs:
        return pl.DataFrame()
    return pl.concat(store.written_dfs, how="vertical_relaxed")


# ===========================================================================
# 1. prune_not_overlapping_files_by_threshold — ALL False files included
# ===========================================================================

class TestPruneIncludesAllFalseFiles:
    """After the fix, when the gate opens ALL False items must be included —
    the old code capped at MAX_MEMORY_CHUNK_SIZE and dropped the rest."""

    @patch(f"{_MOD}.default")
    def test_all_false_files_included_when_count_threshold_hit(self, mock_def):
        from supertable.processing import prune_not_overlapping_files_by_threshold

        mock_def.MAX_OVERLAPPING_FILES = 5
        mock_def.MAX_MEMORY_CHUNK_SIZE = 500  # tiny — old code would stop early

        files = _overlap_set([
            (f"/data/f{i}.parquet", False, 200) for i in range(10)
        ])

        result = prune_not_overlapping_files_by_threshold(files)

        false_in = [f for f in result if f[1] is False]
        assert len(false_in) == 10, (
            "All 10 false files must be included; old code stopped at ~2-3"
        )

    @patch(f"{_MOD}.default")
    def test_all_false_files_included_when_size_threshold_hit(self, mock_def):
        from supertable.processing import prune_not_overlapping_files_by_threshold

        mock_def.MAX_OVERLAPPING_FILES = 1000  # count gate won't trigger
        mock_def.MAX_MEMORY_CHUNK_SIZE = 100   # but size gate will

        files = _overlap_set([
            (f"/data/f{i}.parquet", False, 80) for i in range(20)
        ])

        result = prune_not_overlapping_files_by_threshold(files)

        false_in = [f for f in result if f[1] is False]
        assert len(false_in) == 20, (
            "All 20 false files must be included once size gate opens"
        )

    @patch(f"{_MOD}.default")
    def test_false_files_excluded_when_below_both_thresholds(self, mock_def):
        from supertable.processing import prune_not_overlapping_files_by_threshold

        mock_def.MAX_OVERLAPPING_FILES = 100
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        files = _overlap_set([
            ("a.parquet", True, 100),
            ("b.parquet", False, 50),
            ("c.parquet", False, 50),
        ])

        result = prune_not_overlapping_files_by_threshold(files)

        false_in = [f for f in result if f[1] is False]
        assert len(false_in) == 0, (
            "Below both thresholds: false files must NOT be pulled in"
        )
        assert ("a.parquet", True, 100) in result

    @patch(f"{_MOD}.default")
    def test_true_items_always_kept(self, mock_def):
        from supertable.processing import prune_not_overlapping_files_by_threshold

        mock_def.MAX_OVERLAPPING_FILES = 100
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        files = _overlap_set([
            ("a.parquet", True, 100),
            ("b.parquet", True, 200),
        ])

        result = prune_not_overlapping_files_by_threshold(files)
        assert ("a.parquet", True, 100) in result
        assert ("b.parquet", True, 200) in result


# ===========================================================================
# 2. process_files_with_overlap — 4-tuple return with skipped_files
# ===========================================================================

class TestProcessFilesWithOverlapReturn:
    """Verify the function returns (deleted, merged_df, total_rows, skipped_files)."""

    def test_returns_4_tuple(self, store, patched):
        from supertable.processing import process_files_with_overlap

        incoming = _df({"id": [999], "val": ["new"]})
        store.files["/data/f0.parquet"] = _df({"id": [1], "val": ["old"]})

        result = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=pl.DataFrame(schema=incoming.schema),
            merged_df=incoming.clone(),
            new_resources=[],
            overlapping_files=_overlap_set([("/data/f0.parquet", True, 100)]),
            overwrite_columns=["id"],
            sunset_files=set(),
            total_rows=0,
            compression_level=1,
        )

        assert len(result) == 4, "Must return 4-tuple"

    def test_zero_match_file_appears_in_skipped(self, store, patched):
        from supertable.processing import process_files_with_overlap

        incoming = _df({"id": [999], "val": ["new"]})
        # File has id=1 — no match with incoming id=999
        store.files["/data/f0.parquet"] = _df({"id": [1], "val": ["old"]})

        deleted, merged, total_rows, skipped = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=pl.DataFrame(schema=incoming.schema),
            merged_df=incoming.clone(),
            new_resources=[],
            overlapping_files=_overlap_set([("/data/f0.parquet", True, 100)]),
            overwrite_columns=["id"],
            sunset_files=set(),
            total_rows=0,
            compression_level=1,
        )

        assert deleted == 0
        assert len(skipped) == 1
        assert skipped[0][0] == "/data/f0.parquet"
        assert skipped[0][1] == 100

    def test_actual_match_file_not_in_skipped(self, store, patched):
        from supertable.processing import process_files_with_overlap

        incoming = _df({"id": [1], "val": ["updated"]})
        store.files["/data/f0.parquet"] = _df({"id": [1, 2], "val": ["old", "keep"]})
        sunset = set()

        deleted, merged, total_rows, skipped = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=pl.DataFrame(schema=incoming.schema),
            merged_df=incoming.clone(),
            new_resources=[],
            overlapping_files=_overlap_set([("/data/f0.parquet", True, 100)]),
            overwrite_columns=["id"],
            sunset_files=sunset,
            total_rows=0,
            compression_level=1,
        )

        assert deleted == 1
        assert len(skipped) == 0
        assert "/data/f0.parquet" in sunset

    def test_mixed_match_and_skip(self, store, patched):
        from supertable.processing import process_files_with_overlap

        incoming = _df({"id": [1], "val": ["updated"]})

        # f0 has actual match (id=1)
        store.files["/data/f0.parquet"] = _df({"id": [1, 2], "val": ["a", "b"]})
        # f1 has no match (id=50)
        store.files["/data/f1.parquet"] = _df({"id": [50], "val": ["c"]})
        # f2 has no match (id=60)
        store.files["/data/f2.parquet"] = _df({"id": [60], "val": ["d"]})

        sunset = set()
        deleted, merged, total_rows, skipped = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=pl.DataFrame(schema=incoming.schema),
            merged_df=incoming.clone(),
            new_resources=[],
            overlapping_files=_overlap_set([
                ("/data/f0.parquet", True, 100),
                ("/data/f1.parquet", True, 100),
                ("/data/f2.parquet", True, 100),
            ]),
            overwrite_columns=["id"],
            sunset_files=sunset,
            total_rows=0,
            compression_level=1,
        )

        assert deleted == 1
        assert "/data/f0.parquet" in sunset
        assert len(skipped) == 2
        skipped_paths = {s[0] for s in skipped}
        assert "/data/f1.parquet" in skipped_paths
        assert "/data/f2.parquet" in skipped_paths

    def test_file_cache_used_when_provided(self, store, patched):
        from supertable.processing import process_files_with_overlap

        incoming = _df({"id": [999], "val": ["new"]})
        cached_df = _df({"id": [1], "val": ["cached"]})

        # File NOT in store — only reachable via cache
        file_cache = {"/data/f0.parquet": cached_df}

        deleted, merged, total_rows, skipped = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=pl.DataFrame(schema=incoming.schema),
            merged_df=incoming.clone(),
            new_resources=[],
            overlapping_files=_overlap_set([("/data/f0.parquet", True, 100)]),
            overwrite_columns=["id"],
            sunset_files=set(),
            total_rows=0,
            compression_level=1,
            file_cache=file_cache,
        )

        # Cache was consumed, no match so file was skipped
        assert len(file_cache) == 0, "Cache entry should be popped"
        assert len(skipped) == 1

    def test_composite_key_and_semantics(self, store, patched):
        """Multi-column key: only exact tuple match should delete."""
        from supertable.processing import process_files_with_overlap

        incoming = _df({"region": ["US"], "id": [1], "val": ["new"]})
        # Existing has (US, 2) — region matches but id doesn't → no match
        store.files["/data/f0.parquet"] = _df({
            "region": ["US", "EU"],
            "id": [2, 1],
            "val": ["a", "b"],
        })

        sunset = set()
        deleted, merged, total_rows, skipped = process_files_with_overlap(
            data_dir="/data",
            deleted=0,
            df=incoming,
            empty_df=pl.DataFrame(schema=incoming.schema),
            merged_df=incoming.clone(),
            new_resources=[],
            overlapping_files=_overlap_set([("/data/f0.parquet", True, 100)]),
            overwrite_columns=["region", "id"],
            sunset_files=sunset,
            total_rows=0,
            compression_level=1,
        )

        # (US,1) not in file → 0 deletes → skipped
        assert deleted == 0
        assert len(skipped) == 1
        assert "/data/f0.parquet" not in sunset


# ===========================================================================
# 3. Phase 3 — compaction of skipped True files
# ===========================================================================

class TestPhase3CompactionBelowThreshold:
    """When skipped file count < MAX_OVERLAPPING_FILES, Phase 3 does NOT run."""

    @patch(f"{_MOD}.default")
    def test_no_compaction_below_threshold(self, mock_def, store, patched):
        from supertable.processing import process_overlapping_files

        mock_def.MAX_OVERLAPPING_FILES = 100
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        # 5 files, all has_overlap=True, none match incoming id=999
        for i in range(5):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i * 10 + 1],
                "val": [f"v{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(5)
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        assert deleted == 0
        # Only 1 file written (incoming data), none of the 5 compacted
        assert len(new_resources) == 1
        for i in range(5):
            assert f"/data/f{i}.parquet" not in sunset_files


class TestPhase3CompactionAtThreshold:
    """When skipped file count >= MAX_OVERLAPPING_FILES, Phase 3 compacts them."""

    @patch(f"{_MOD}.default")
    def test_all_skipped_files_compacted(self, mock_def, store, patched):
        from supertable.processing import process_overlapping_files

        n = 10
        mock_def.MAX_OVERLAPPING_FILES = n
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        for i in range(n):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i * 10 + 1],
                "val": [f"v{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(n)
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        assert deleted == 0
        # Every skipped file must be sunset (compacted into new output)
        for i in range(n):
            assert f"/data/f{i}.parquet" in sunset_files
        assert total_rows == 1 + n  # 1 incoming + 10 existing

    @patch(f"{_MOD}.default")
    def test_compacted_output_has_all_rows(self, mock_def, store, patched):
        from supertable.processing import process_overlapping_files

        n = 8
        mock_def.MAX_OVERLAPPING_FILES = n
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        for i in range(n):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i + 1],
                "val": [f"orig_{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(n)
        ])

        process_overlapping_files(
            df=incoming,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        all_written = _all_written_rows(store)
        ids = sorted(all_written["id"].to_list())

        # All original rows preserved
        for i in range(n):
            assert (i + 1) in ids
        # Incoming row preserved
        assert 999 in ids
        # No extra rows
        assert len(ids) == n + 1

    @patch(f"{_MOD}.default")
    def test_no_duplicate_rows_after_compaction(self, mock_def, store, patched):
        """Critical: compaction must not duplicate any data."""
        from supertable.processing import process_overlapping_files

        n = 6
        mock_def.MAX_OVERLAPPING_FILES = n
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        for i in range(n):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i + 1],
                "val": [f"v{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(n)
        ])

        process_overlapping_files(
            df=incoming,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        all_written = _all_written_rows(store)
        ids = all_written["id"].to_list()
        assert len(ids) == len(set(ids)), f"Duplicate rows found: {ids}"


class TestPhase3MemoryFlushing:
    """Phase 3 must respect MAX_MEMORY_CHUNK_SIZE and flush in chunks."""

    @patch(f"{_MOD}.default")
    def test_multiple_flushes_at_memory_boundary(self, mock_def, store, patched):
        from supertable.processing import process_overlapping_files

        n = 8
        mock_def.MAX_OVERLAPPING_FILES = n
        # Tiny memory limit forces multiple flushes
        mock_def.MAX_MEMORY_CHUNK_SIZE = 1

        incoming = _df({"id": [999], "val": ["new"]})

        for i in range(n):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i + 1],
                "val": [f"v{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 500) for i in range(n)
        ])

        process_overlapping_files(
            df=incoming,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        # Must have multiple write calls
        assert len(store.written_dfs) >= 2, (
            f"Expected multiple flushes, got {len(store.written_dfs)}"
        )

        # Total rows across all writes must still be correct
        all_written = _all_written_rows(store)
        assert all_written.shape[0] == n + 1

    @patch(f"{_MOD}.default")
    def test_large_memory_limit_single_flush(self, mock_def, store, patched):
        from supertable.processing import process_overlapping_files

        n = 5
        mock_def.MAX_OVERLAPPING_FILES = n
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024  # huge

        incoming = _df({"id": [999], "val": ["new"]})

        for i in range(n):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i + 1],
                "val": [f"v{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(n)
        ])

        process_overlapping_files(
            df=incoming,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        # Everything fits in memory → single flush
        assert len(store.written_dfs) == 1
        assert store.written_dfs[0].shape[0] == n + 1


# ===========================================================================
# 4. Mixed scenarios — Phase 2 overlap + Phase 3 compaction
# ===========================================================================

class TestPhase3MixedWithRealOverlap:
    """Some files have actual key matches (Phase 2), others are skipped and
    compacted in Phase 3.  Data integrity must hold across both phases."""

    @patch(f"{_MOD}.default")
    def test_phase2_deletes_and_phase3_compacts(self, mock_def, store, patched):
        from supertable.processing import process_overlapping_files

        mock_def.MAX_OVERLAPPING_FILES = 3
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [1, 999], "val": ["updated", "brand_new"]})

        # f0 has real overlap (id=1 matches incoming)
        store.files["/data/f0.parquet"] = _df({
            "id": [1, 2], "val": ["old_1", "keep_2"],
        })

        # f1..f3 have no actual overlap (stats say True, but keys don't match)
        for i in range(1, 4):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i * 100], "val": [f"no_match_{i}"],
            })

        overlapping = _overlap_set(
            [("/data/f0.parquet", True, 200)] +
            [(f"/data/f{i}.parquet", True, 100) for i in range(1, 4)]
        )

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        # Phase 2: id=1 deleted from f0
        assert deleted == 1

        # f0 sunset in Phase 2
        assert "/data/f0.parquet" in sunset_files

        # f1..f3 sunset in Phase 3 (compacted)
        for i in range(1, 4):
            assert f"/data/f{i}.parquet" in sunset_files

        # Data integrity check
        all_written = _all_written_rows(store)
        ids = sorted(all_written["id"].to_list())

        # Incoming: 1, 999
        assert 1 in ids
        assert 999 in ids
        # Surviving from f0: 2
        assert 2 in ids
        # From compacted f1..f3: 100, 200, 300
        assert 100 in ids
        assert 200 in ids
        assert 300 in ids

        # No duplicates
        assert len(ids) == len(set(ids))

    @patch(f"{_MOD}.default")
    def test_phase2_actual_overlap_not_counted_toward_skipped(self, mock_def, store, patched):
        """Files handled in Phase 2 (actual deletes) must NOT appear in
        the skipped list that drives Phase 3."""
        from supertable.processing import process_overlapping_files

        mock_def.MAX_OVERLAPPING_FILES = 2
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [1, 2], "val": ["a", "b"]})

        # Both files have actual overlap
        store.files["/data/f0.parquet"] = _df({"id": [1], "val": ["old"]})
        store.files["/data/f1.parquet"] = _df({"id": [2], "val": ["old"]})

        overlapping = _overlap_set([
            ("/data/f0.parquet", True, 100),
            ("/data/f1.parquet", True, 100),
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        assert deleted == 2  # Both files had real matches

        # Phase 2 sunset both files; Phase 3 has zero skipped → no extra compaction
        all_written = _all_written_rows(store)
        ids = all_written["id"].to_list()
        # Only incoming rows (existing rows were overwritten → deleted)
        assert sorted(ids) == [1, 2]


# ===========================================================================
# 5. Phase 1 + Phase 3 combined — False files AND skipped True files
# ===========================================================================

class TestPhase1AndPhase3Combined:
    """Both has_overlap=False (Phase 1) and skipped has_overlap=True
    (Phase 3) files should be compacted in the same write."""

    @patch(f"{_MOD}.default")
    def test_false_and_skipped_true_both_compacted(self, mock_def, store, patched):
        from supertable.processing import process_overlapping_files

        mock_def.MAX_OVERLAPPING_FILES = 3
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        # False files (Phase 1 compaction)
        for i in range(3):
            store.files[f"/data/false_{i}.parquet"] = _df({
                "id": [i + 1000], "val": [f"false_{i}"],
            })

        # True files with no actual match (Phase 3 compaction)
        for i in range(3):
            store.files[f"/data/true_{i}.parquet"] = _df({
                "id": [i + 2000], "val": [f"true_{i}"],
            })

        overlapping = _overlap_set(
            [(f"/data/false_{i}.parquet", False, 100) for i in range(3)] +
            [(f"/data/true_{i}.parquet", True, 100) for i in range(3)]
        )

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        # All 6 existing files sunset
        for i in range(3):
            assert f"/data/false_{i}.parquet" in sunset_files
            assert f"/data/true_{i}.parquet" in sunset_files

        all_written = _all_written_rows(store)
        ids = sorted(all_written["id"].to_list())

        assert 999 in ids
        for i in range(3):
            assert (i + 1000) in ids  # False files
            assert (i + 2000) in ids  # Skipped True files
        assert len(ids) == 7  # 1 incoming + 3 false + 3 true


# ===========================================================================
# 6. Edge cases
# ===========================================================================

class TestPhase3EdgeCases:

    @patch(f"{_MOD}.default")
    def test_read_failure_during_compaction_skips_file(self, mock_def, store, patched):
        """If _read_parquet_safe returns None for a has_overlap=True file,
        it is skipped in both Phase 2 and Phase 3.  The other readable
        zero-match files are still compacted when the threshold is met."""
        from supertable.processing import process_overlapping_files

        # f1 will return None → only f0 and f2 are readable and skipped
        # so set threshold to 2 to trigger Phase 3 on those two
        mock_def.MAX_OVERLAPPING_FILES = 2
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        store.files["/data/f0.parquet"] = _df({"id": [1], "val": ["a"]})
        # f1 is missing → read returns None in Phase 2, never reaches skipped_files
        store.files["/data/f2.parquet"] = _df({"id": [3], "val": ["c"]})

        overlapping = _overlap_set([
            ("/data/f0.parquet", True, 100),
            ("/data/f1.parquet", True, 100),  # will be None
            ("/data/f2.parquet", True, 100),
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        # f0 and f2 compacted in Phase 3; f1 not readable → never sunset
        assert "/data/f0.parquet" in sunset_files
        assert "/data/f2.parquet" in sunset_files
        assert "/data/f1.parquet" not in sunset_files

        all_written = _all_written_rows(store)
        ids = sorted(all_written["id"].to_list())
        assert ids == [1, 3, 999]

    @patch(f"{_MOD}.default")
    def test_empty_overlapping_set(self, mock_def, store, patched):
        """No overlapping files at all — just the incoming data."""
        from supertable.processing import process_overlapping_files

        mock_def.MAX_OVERLAPPING_FILES = 100
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [1, 2, 3], "val": ["a", "b", "c"]})

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=set(),
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        assert inserted == 3
        assert deleted == 0
        assert total_rows == 3
        assert len(sunset_files) == 0
        assert len(new_resources) == 1

    @patch(f"{_MOD}.default")
    def test_exactly_at_threshold(self, mock_def, store, patched):
        """Exactly MAX_OVERLAPPING_FILES skipped files → compaction triggers."""
        from supertable.processing import process_overlapping_files

        n = 5
        mock_def.MAX_OVERLAPPING_FILES = n
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        for i in range(n):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i + 1], "val": [f"v{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(n)
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        # Exactly at threshold → compaction should trigger
        assert len(sunset_files) == n
        all_written = _all_written_rows(store)
        assert all_written.shape[0] == n + 1

    @patch(f"{_MOD}.default")
    def test_one_below_threshold_no_compaction(self, mock_def, store, patched):
        """n-1 skipped files → just below threshold → no compaction."""
        from supertable.processing import process_overlapping_files

        n = 5
        mock_def.MAX_OVERLAPPING_FILES = n
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        for i in range(n - 1):
            store.files[f"/data/f{i}.parquet"] = _df({
                "id": [i + 1], "val": [f"v{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(n - 1)
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=["id"],
                data_dir="/data",
                compression_level=1,
            )

        # Below threshold → no compaction of skipped files
        assert len(sunset_files) == 0
        assert total_rows == 1  # only incoming

    @patch(f"{_MOD}.default")
    def test_no_overwrite_columns_skips_phase2_and_phase3(self, mock_def, store, patched):
        """Without overwrite columns, Phase 2 passes everything through
        unchanged and Phase 3 has nothing to compact."""
        from supertable.processing import process_overlapping_files

        mock_def.MAX_OVERLAPPING_FILES = 2
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"id": [999], "val": ["new"]})

        store.files["/data/f0.parquet"] = _df({"id": [1], "val": ["a"]})
        store.files["/data/f1.parquet"] = _df({"id": [2], "val": ["b"]})

        overlapping = _overlap_set([
            ("/data/f0.parquet", True, 100),
            ("/data/f1.parquet", True, 100),
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = \
            process_overlapping_files(
                df=incoming,
                overlapping_files=overlapping,
                overwrite_columns=[],
                data_dir="/data",
                compression_level=1,
            )

        # Without overwrite_columns, Phase 2 includes all existing data
        # (filtered_df == existing_df but overwrite_columns is empty so
        #  the "if nothing changed" guard is skipped)
        assert "/data/f0.parquet" in sunset_files
        assert "/data/f1.parquet" in sunset_files

        all_written = _all_written_rows(store)
        assert all_written.shape[0] == 3  # 1 incoming + 1 + 1

    @patch(f"{_MOD}.default")
    def test_multi_column_key_compaction_preserves_all_data(self, mock_def, store, patched):
        """Composite key compaction must not lose any rows."""
        from supertable.processing import process_overlapping_files

        n = 4
        mock_def.MAX_OVERLAPPING_FILES = n
        mock_def.MAX_MEMORY_CHUNK_SIZE = 512 * 1024 * 1024

        incoming = _df({"region": ["US"], "id": [1], "val": ["new"]})

        for i in range(n):
            store.files[f"/data/f{i}.parquet"] = _df({
                "region": ["EU"],
                "id": [i + 100],
                "val": [f"eu_{i}"],
            })

        overlapping = _overlap_set([
            (f"/data/f{i}.parquet", True, 100) for i in range(n)
        ])

        process_overlapping_files(
            df=incoming,
            overlapping_files=overlapping,
            overwrite_columns=["region", "id"],
            data_dir="/data",
            compression_level=1,
        )

        all_written = _all_written_rows(store)
        ids = sorted(all_written["id"].to_list())
        regions = all_written["region"].to_list()

        # Incoming (US, 1) plus 4 EU rows
        assert len(ids) == n + 1
        assert 1 in ids
        for i in range(n):
            assert (i + 100) in ids
        assert "US" in regions
        assert regions.count("EU") == n
