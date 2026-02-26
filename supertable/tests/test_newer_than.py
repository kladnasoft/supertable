# tests/test_newer_than.py
"""
Tests for the newer_than (conflict resolution) feature in data_writer / processing.

These tests exercise filter_stale_incoming_rows in isolation — no storage, Redis,
or lock infrastructure required.  We mock _read_parquet_safe so tests are fully
decoupled from the storage backend.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest
import polars as pl

from supertable.processing import filter_stale_incoming_rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_overlapping_set(paths):
    """Build an overlapping_files set with has_overlap=True for all paths."""
    return {(p, True, 1000) for p in paths}


def _mock_reader(file_map: dict):
    """Return a side_effect function for _read_parquet_safe that serves from file_map."""
    def _read(path):
        return file_map.get(path)
    return _read


# ---------------------------------------------------------------------------
# Scenario 1: All incoming rows are stale (replay) → empty result
# ---------------------------------------------------------------------------

class TestAllRowsStale:

    @patch("supertable.processing._read_parquet_safe")
    def test_single_row_older(self, mock_read):
        existing = pl.DataFrame({"user_id": [5], "name": ["Alice"], "updated_at": [7]})
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({"user_id": [5], "name": ["Alice"], "updated_at": [5]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 0

    @patch("supertable.processing._read_parquet_safe")
    def test_single_row_equal(self, mock_read):
        """Exact replay (same updated_at) → stale, should be dropped."""
        existing = pl.DataFrame({"user_id": [5], "name": ["Alice"], "updated_at": [7]})
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({"user_id": [5], "name": ["Alice"], "updated_at": [7]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 0


# ---------------------------------------------------------------------------
# Scenario 2: Incoming row is genuinely newer → kept
# ---------------------------------------------------------------------------

class TestRowIsNewer:

    @patch("supertable.processing._read_parquet_safe")
    def test_single_row_newer(self, mock_read):
        existing = pl.DataFrame({"user_id": [5], "name": ["Alice"], "updated_at": [3]})
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({"user_id": [5], "name": ["Bob"], "updated_at": [7]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 1
        assert result["name"][0] == "Bob"


# ---------------------------------------------------------------------------
# Scenario 3: New key (no existing data) → kept
# ---------------------------------------------------------------------------

class TestNewKey:

    def test_new_key_no_overlapping_files(self):
        incoming = pl.DataFrame({"user_id": [99], "name": ["New"], "updated_at": [1]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=set(),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 1

    @patch("supertable.processing._read_parquet_safe")
    def test_new_key_existing_files_have_different_keys(self, mock_read):
        existing = pl.DataFrame({"user_id": [1, 2], "name": ["A", "B"], "updated_at": [5, 5]})
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({"user_id": [99], "name": ["New"], "updated_at": [1]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 1


# ---------------------------------------------------------------------------
# Scenario 4: Mixed batch — some stale, some newer
# ---------------------------------------------------------------------------

class TestMixedBatch:

    @patch("supertable.processing._read_parquet_safe")
    def test_mixed_rows(self, mock_read):
        existing = pl.DataFrame({
            "user_id": [5, 10],
            "name": ["Alice", "Charlie"],
            "updated_at": [7, 3],
        })
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({
            "user_id": [5, 10],
            "name": ["Alice_old", "Charlie_new"],
            "updated_at": [5, 9],  # user_id=5 stale (5<7), user_id=10 newer (9>3)
        })

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 1
        assert result["user_id"][0] == 10
        assert result["name"][0] == "Charlie_new"


# ---------------------------------------------------------------------------
# Scenario 5: Existing file lacks newer_than column (legacy) → allow through
# ---------------------------------------------------------------------------

class TestLegacyFiles:

    @patch("supertable.processing._read_parquet_safe")
    def test_missing_newer_than_column(self, mock_read):
        existing = pl.DataFrame({"user_id": [5], "name": ["Alice"]})  # no updated_at
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({"user_id": [5], "name": ["Bob"], "updated_at": [1]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 1


# ---------------------------------------------------------------------------
# Scenario 6: Multiple existing files — takes max across all
# ---------------------------------------------------------------------------

class TestMultipleFiles:

    @patch("supertable.processing._read_parquet_safe")
    def test_max_across_files(self, mock_read):
        """Existing user_id=5 appears in two files with updated_at=3 and updated_at=7.
        Incoming updated_at=5 should be stale (max existing is 7)."""
        f1 = pl.DataFrame({"user_id": [5], "name": ["A"], "updated_at": [3]})
        f2 = pl.DataFrame({"user_id": [5], "name": ["B"], "updated_at": [7]})
        mock_read.side_effect = _mock_reader({
            "f1.parquet": f1,
            "f2.parquet": f2,
        })

        incoming = pl.DataFrame({"user_id": [5], "name": ["C"], "updated_at": [5]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["f1.parquet", "f2.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 0


# ---------------------------------------------------------------------------
# Scenario 7: has_overlap=False files are ignored (they don't share keys)
# ---------------------------------------------------------------------------

class TestOnlyOverlapTrueFilesRead:

    @patch("supertable.processing._read_parquet_safe")
    def test_false_overlap_ignored(self, mock_read):
        existing = pl.DataFrame({"user_id": [5], "name": ["A"], "updated_at": [99]})
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({"user_id": [5], "name": ["B"], "updated_at": [1]})

        # Mark file as has_overlap=False — should NOT be read
        overlapping = {("file1.parquet", False, 1000)}

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=overlapping,
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        # Row should pass through because no overlap=True files were examined
        assert result.height == 1
        mock_read.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 8: Composite overwrite columns
# ---------------------------------------------------------------------------

class TestCompositeKey:

    @patch("supertable.processing._read_parquet_safe")
    def test_composite_key(self, mock_read):
        existing = pl.DataFrame({
            "user_id": [5, 5],
            "day": ["2024-01-01", "2024-01-02"],
            "value": [100, 200],
            "ts_ms": [10, 20],
        })
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({
            "user_id": [5, 5],
            "day": ["2024-01-01", "2024-01-02"],
            "value": [999, 999],
            "ts_ms": [15, 5],  # first is newer (15>10), second is stale (5<20)
        })

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id", "day"],
            newer_than_col="ts_ms",
        )
        assert result.height == 1
        assert result["day"][0] == "2024-01-01"
        assert result["ts_ms"][0] == 15


# ---------------------------------------------------------------------------
# Scenario 9: No-op when newer_than or overwrite_columns not set
# ---------------------------------------------------------------------------

class TestNoOp:

    def test_no_newer_than(self):
        incoming = pl.DataFrame({"user_id": [5], "updated_at": [1]})
        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=set(),
            overwrite_columns=["user_id"],
            newer_than_col="",
        )
        assert result.height == 1

    def test_no_overwrite_columns(self):
        incoming = pl.DataFrame({"user_id": [5], "updated_at": [1]})
        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=set(),
            overwrite_columns=[],
            newer_than_col="updated_at",
        )
        assert result.height == 1


# ---------------------------------------------------------------------------
# Scenario 10: _read_parquet_safe returns None (file vanished) → allow through
# ---------------------------------------------------------------------------

class TestFileVanished:

    @patch("supertable.processing._read_parquet_safe")
    def test_vanished_file(self, mock_read):
        mock_read.return_value = None  # file disappeared

        incoming = pl.DataFrame({"user_id": [5], "name": ["Bob"], "updated_at": [1]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["gone.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 1


# ---------------------------------------------------------------------------
# Scenario 11: Duplicate keys in existing data — max is correctly computed
# ---------------------------------------------------------------------------

class TestDuplicateKeysInExisting:

    @patch("supertable.processing._read_parquet_safe")
    def test_duplicate_keys_takes_max(self, mock_read):
        """user_id=5 appears twice in same file with updated_at=3 and updated_at=7.
        Incoming updated_at=5 → stale (max is 7)."""
        existing = pl.DataFrame({
            "user_id": [5, 5],
            "name": ["old", "new"],
            "updated_at": [3, 7],
        })
        mock_read.side_effect = _mock_reader({"file1.parquet": existing})

        incoming = pl.DataFrame({"user_id": [5], "name": ["replay"], "updated_at": [5]})

        result = filter_stale_incoming_rows(
            incoming_df=incoming,
            overlapping_files=_make_overlapping_set(["file1.parquet"]),
            overwrite_columns=["user_id"],
            newer_than_col="updated_at",
        )
        assert result.height == 0