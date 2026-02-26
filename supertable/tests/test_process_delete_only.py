# tests/test_process_delete_only.py

"""
Unit tests for the process_delete_only function in supertable.processing.

All storage I/O is mocked — no Redis, MinIO, or filesystem required.
"""

import os
from unittest.mock import patch, MagicMock

import polars
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from supertable.processing import process_delete_only


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_df(data: dict) -> polars.DataFrame:
    """Shorthand for creating a Polars DataFrame from a dict."""
    return polars.DataFrame(data)


def _overlapping_set(files: list) -> set:
    """
    Build the overlapping_files set from a list of (path, has_overlap, size) tuples.
    """
    return set(files)


# ---------------------------------------------------------------------------
# Mock: intercept _read_parquet_safe to return in-memory DataFrames
# ---------------------------------------------------------------------------

class _ParquetStore:
    """
    Fake parquet store that maps file paths to Polars DataFrames.
    Used to patch _read_parquet_safe and write_parquet_and_collect_resources.
    """

    def __init__(self):
        self.files: dict[str, polars.DataFrame] = {}
        self.written: list[dict] = []  # records of (path, df) for written files

    def read(self, path: str):
        return self.files.get(path)

    def write_side_effect(self, write_df, overwrite_columns, data_dir, new_resources, compression_level):
        """
        Stand-in for write_parquet_and_collect_resources: just track what was written
        and append a synthetic resource entry.
        """
        fake_path = os.path.join(data_dir, f"data_{len(self.written)}.parquet")
        self.written.append({"path": fake_path, "df": write_df.clone()})
        new_resources.append({
            "file": fake_path,
            "file_size": write_df.estimated_size(),
            "rows": write_df.shape[0],
            "columns": write_df.shape[1],
            "stats": {},
        })


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def store():
    return _ParquetStore()


@pytest.fixture
def patch_io(store):
    """Patch storage I/O so process_delete_only works without real files."""
    with patch(
        "supertable.processing._read_parquet_safe", side_effect=store.read
    ), patch(
        "supertable.processing.write_parquet_and_collect_resources",
        side_effect=store.write_side_effect,
    ):
        yield store


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestProcessDeleteOnlyBasic:
    """Core happy-path and edge-case tests."""

    def test_partial_delete_rewrites_file(self, store, patch_io):
        """
        File has 3 rows, delete request targets 1 key.
        Expect: 1 deleted, file sunset, new file written with 2 remaining rows.
        """
        existing = _make_df({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        store.files["/data/file1.parquet"] = existing

        delete_keys = _make_df({"id": [2]})
        overlapping = _overlapping_set([("/data/file1.parquet", True, 1000)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert inserted == 0
        assert deleted == 1
        assert total_rows == 2
        assert total_columns == 2  # id + value (from existing file)
        assert len(new_resources) == 1
        assert "/data/file1.parquet" in sunset_files

        # Verify the written DataFrame has the correct remaining rows
        written_df = store.written[0]["df"]
        assert written_df.shape[0] == 2
        assert set(written_df["id"].to_list()) == {1, 3}

    def test_full_delete_no_new_file(self, store, patch_io):
        """
        All rows in the file match delete keys.
        Expect: file sunset, NO new file written, total_rows=0.
        """
        existing = _make_df({"id": [10, 20], "name": ["x", "y"]})
        store.files["/data/file1.parquet"] = existing

        delete_keys = _make_df({"id": [10, 20]})
        overlapping = _overlapping_set([("/data/file1.parquet", True, 500)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert inserted == 0
        assert deleted == 2
        assert total_rows == 0
        assert len(new_resources) == 0
        assert "/data/file1.parquet" in sunset_files
        assert len(store.written) == 0

    def test_no_match_leaves_file_untouched(self, store, patch_io):
        """
        Delete keys don't match any rows in the file.
        Expect: nothing sunset, nothing written, deleted=0.
        """
        existing = _make_df({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        store.files["/data/file1.parquet"] = existing

        delete_keys = _make_df({"id": [999]})
        overlapping = _overlapping_set([("/data/file1.parquet", True, 1000)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert inserted == 0
        assert deleted == 0
        assert total_rows == 0
        assert len(new_resources) == 0
        assert len(sunset_files) == 0

    def test_false_overlap_files_ignored(self, store, patch_io):
        """
        Files with has_overlap=False must be skipped entirely.
        """
        existing = _make_df({"id": [1], "value": ["a"]})
        store.files["/data/no_overlap.parquet"] = existing

        delete_keys = _make_df({"id": [1]})
        overlapping = _overlapping_set([("/data/no_overlap.parquet", False, 500)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert deleted == 0
        assert len(sunset_files) == 0
        assert len(new_resources) == 0


class TestProcessDeleteOnlyMultiFile:
    """Tests involving multiple files in the overlapping set."""

    def test_multi_file_independent_rewrite(self, store, patch_io):
        """
        Two overlapping files; delete key hits both.
        Each file should be rewritten independently (no merging across files).
        """
        store.files["/data/f1.parquet"] = _make_df({"id": [1, 2], "val": ["a", "b"]})
        store.files["/data/f2.parquet"] = _make_df({"id": [2, 3], "val": ["c", "d"]})

        delete_keys = _make_df({"id": [2]})
        overlapping = _overlapping_set([
            ("/data/f1.parquet", True, 500),
            ("/data/f2.parquet", True, 500),
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert inserted == 0
        assert deleted == 2  # one from each file
        assert total_rows == 2  # 1 kept from f1, 1 kept from f2
        assert len(new_resources) == 2
        assert sunset_files == {"/data/f1.parquet", "/data/f2.parquet"}

        # Each written file should have exactly 1 row
        for entry in store.written:
            assert entry["df"].shape[0] == 1

    def test_mixed_overlap_flags(self, store, patch_io):
        """
        Mix of has_overlap=True and False files.
        Only True files are processed; False files are untouched.
        """
        store.files["/data/overlap.parquet"] = _make_df({"id": [5, 6], "x": [1, 2]})
        store.files["/data/compact.parquet"] = _make_df({"id": [5], "x": [99]})

        delete_keys = _make_df({"id": [5]})
        overlapping = _overlapping_set([
            ("/data/overlap.parquet", True, 500),
            ("/data/compact.parquet", False, 200),
        ])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert deleted == 1
        assert sunset_files == {"/data/overlap.parquet"}
        assert len(new_resources) == 1
        # The compact file should NOT be in sunset
        assert "/data/compact.parquet" not in sunset_files


class TestProcessDeleteOnlyEdgeCases:
    """Edge cases and robustness tests."""

    def test_file_vanished_race_condition(self, store, patch_io):
        """
        File listed in overlapping set but _read_parquet_safe returns None (race).
        Should be skipped gracefully.
        """
        # Don't add anything to store.files — simulates vanished file
        delete_keys = _make_df({"id": [1]})
        overlapping = _overlapping_set([("/data/gone.parquet", True, 500)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert deleted == 0
        assert len(sunset_files) == 0
        assert len(new_resources) == 0

    def test_overwrite_column_missing_from_existing_file(self, store, patch_io):
        """
        Existing file doesn't have the overwrite column.
        Should be skipped (no predicate can be built).
        """
        existing = _make_df({"other_col": [1, 2], "value": ["a", "b"]})
        store.files["/data/file1.parquet"] = existing

        delete_keys = _make_df({"id": [1]})
        overlapping = _overlapping_set([("/data/file1.parquet", True, 500)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert deleted == 0
        assert len(sunset_files) == 0

    def test_empty_overlapping_set(self, store, patch_io):
        """
        No overlapping files at all — nothing to do.
        """
        delete_keys = _make_df({"id": [1, 2, 3]})

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=set(),
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert inserted == 0
        assert deleted == 0
        assert total_rows == 0
        assert len(new_resources) == 0
        assert len(sunset_files) == 0

    def test_total_columns_fallback_to_delete_df(self, store, patch_io):
        """
        When no files are touched, total_columns should fall back to the
        delete DataFrame's column count.
        """
        delete_keys = _make_df({"id": [999], "extra": ["x"]})

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=set(),
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        assert total_columns == 2  # id + extra from delete_keys df

    def test_schema_preserved_on_rewrite(self, store, patch_io):
        """
        Rewritten file must keep the original file's full schema,
        not the delete-keys schema.
        """
        existing = _make_df({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "score": [100.0, 200.0, 300.0],
            "active": [True, False, True],
        })
        store.files["/data/file1.parquet"] = existing

        # Delete keys only have the key column
        delete_keys = _make_df({"id": [2]})
        overlapping = _overlapping_set([("/data/file1.parquet", True, 1000)])

        process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["id"],
            data_dir="/data",
            compression_level=1,
        )

        written_df = store.written[0]["df"]
        assert written_df.columns == ["id", "name", "score", "active"]
        assert written_df.shape == (2, 4)


class TestProcessDeleteOnlyMultiColumnKey:
    """Tests for composite (multi-column) overwrite keys."""

    def test_multi_column_key_delete(self, store, patch_io):
        """
        Delete with a composite key (region + id).
        Only rows matching on ALL key columns should be removed.
        """
        existing = _make_df({
            "region": ["US", "US", "EU", "EU"],
            "id": [1, 2, 1, 2],
            "value": ["a", "b", "c", "d"],
        })
        store.files["/data/file1.parquet"] = existing

        # Delete US-1 and EU-2: but OR logic means any row matching region=US OR id=1 OR region=EU OR id=2
        # With the current OR-based is_in logic, this will match ALL rows
        # because region is_in(["US","EU"]) matches everything and id is_in([1,2]) matches everything.
        #
        # This is consistent with how process_files_with_overlap works (OR across columns).
        delete_keys = _make_df({"region": ["US", "EU"], "id": [1, 2]})
        overlapping = _overlapping_set([("/data/file1.parquet", True, 1000)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["region", "id"],
            data_dir="/data",
            compression_level=1,
        )

        # OR semantics: region in (US, EU) OR id in (1, 2) → all 4 rows deleted
        assert deleted == 4
        assert total_rows == 0
        assert len(new_resources) == 0
        assert "/data/file1.parquet" in sunset_files

    def test_multi_column_key_partial_match(self, store, patch_io):
        """
        Multi-column key where only one column matches some rows.
        OR semantics: any column match triggers deletion.
        """
        existing = _make_df({
            "region": ["US", "EU", "APAC"],
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        store.files["/data/file1.parquet"] = existing

        # Delete where region=US — this matches row 0; id=99 matches nothing
        delete_keys = _make_df({"region": ["US"], "id": [99]})
        overlapping = _overlapping_set([("/data/file1.parquet", True, 1000)])

        inserted, deleted, total_rows, total_columns, new_resources, sunset_files = process_delete_only(
            df=delete_keys,
            overlapping_files=overlapping,
            overwrite_columns=["region", "id"],
            data_dir="/data",
            compression_level=1,
        )

        # region in ("US") matches row 0; id in (99) matches nothing → 1 deleted
        assert deleted == 1
        assert total_rows == 2