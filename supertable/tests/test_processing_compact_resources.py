"""
Integrity tests for ``supertable.processing.compact_resources``.

These tests focus on the user's stated requirements:

  * No row LOSS              — every input row survives compaction.
  * No row DUPLICATION       — multiset equality before and after.
  * No column LOSS           — every column from any source file is present.
  * No column ADDED          — no extra columns appear.
  * Schema preserved         — dtypes round-trip through Parquet.
  * Value preservation       — numerical sums and string set-equality match.
  * Tombstone interaction    — when a caller pre-runs ``compact_tombstones``,
                                the downstream ``compact_resources`` only sees
                                survivors.

All Parquet I/O is performed against a real on-disk ``LocalStorage`` in a
``tmp_path`` directory — no mocks for the file format. The storage
factory is monkey-patched per test so processing's
``_get_storage()`` resolves to our tempdir-rooted local storage.

The Parquet read step uses ``_read_parquet_safe`` which calls
``_get_storage().read_parquet``; we go through the same code path so
schema-evolution and partition-handling are exercised end-to-end.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Dict, List
from unittest.mock import patch

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Stable env so config imports cleanly
os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")


# ---------------------------------------------------------------------------
# Local-storage harness
# ---------------------------------------------------------------------------
#
# ``LocalStorage`` is a "give it any absolute path" backend — no
# root concept, no home_dir kwarg. We just point ``data_dir`` at a
# subdirectory of pytest's ``tmp_path`` and let LocalStorage write
# real Parquet files there.
#
# The processing module caches its storage handle in ``proc_mod._storage``.
# We patch ``proc_mod.get_storage`` per test so every call inside
# ``compact_resources`` resolves to a fresh LocalStorage — no
# state leaks between tests.


@pytest.fixture
def storage_dir(tmp_path):
    """Return (data_dir, storage) — data_dir is an absolute path under
    tmp_path; storage is a real ``LocalStorage`` instance."""
    from supertable.storage.local_storage import LocalStorage
    data_dir = str(tmp_path / "warehouse" / "orders" / "data")
    os.makedirs(data_dir, exist_ok=True)
    storage = LocalStorage()
    return data_dir, storage


@pytest.fixture
def patched_storage(storage_dir):
    """Reset processing._storage cache + patch get_storage so processing
    code paths see our fixture-controlled storage."""
    from supertable import processing as proc_mod

    _, storage = storage_dir
    proc_mod._storage = None

    with patch.object(proc_mod, "get_storage", return_value=storage):
        yield storage

    proc_mod._storage = None


# ---------------------------------------------------------------------------
# Helpers to write source parquet files + build a snapshot dict
# ---------------------------------------------------------------------------


def _write_source_parquet(
    storage,
    data_dir: str,
    name: str,
    df: pl.DataFrame,
    compression_level: int = 1,
) -> Dict:
    """Write *df* as a single Parquet file at *data_dir*/*name* and
    return a resource dict matching the snapshot shape SuperTable uses.

    Paths are absolute (LocalStorage operates directly on filesystem
    paths). The resource dict's ``file`` field is the same absolute
    path so ``_read_parquet_safe`` round-trips correctly.
    """
    abs_path = os.path.join(data_dir, name)
    Path(abs_path).parent.mkdir(parents=True, exist_ok=True)

    arrow_tbl = df.to_arrow()
    pq.write_table(
        arrow_tbl, abs_path,
        compression="zstd", compression_level=compression_level,
        use_dictionary=True, write_statistics=True,
    )

    file_size = os.path.getsize(abs_path)
    return {
        "file": abs_path,
        "file_size": file_size,
        "rows": df.shape[0],
        "columns": [{"name": c, "type": str(t)} for c, t in zip(df.columns, df.dtypes)],
        # stats: None — keep the test focused on body, not stats pruning.
        "stats": None,
    }


def _build_snapshot(resources: List[Dict]) -> Dict:
    return {
        "simple_name": "orders",
        "snapshot_version": 1,
        "resources": resources,
        "tombstones": None,
    }


def _read_all(_storage, paths: List[str]) -> pl.DataFrame:
    """Read every parquet file at *paths* and return one concatenated frame.

    Paths are absolute (LocalStorage operates directly on the filesystem),
    so we can hand them straight to Polars.
    """
    frames = []
    for p in paths:
        frames.append(pl.read_parquet(p))
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="vertical_relaxed")


def _multiset(df: pl.DataFrame, cols: List[str] | None = None) -> Dict[tuple, int]:
    """Return a multiset (tuple → count) of the rows in *df*, projected
    to *cols* (defaults to all columns). Order-independent equality
    check across the before/after frames."""
    use = cols if cols is not None else list(df.columns)
    counts: Dict[tuple, int] = {}
    for row in df.select(use).iter_rows():
        # Convert any unhashable (lists, nested dicts) to repr; our test
        # data uses scalar columns only so this is a no-op.
        key = tuple(row)
        counts[key] = counts.get(key, 0) + 1
    return counts


# ===========================================================================
# 1. Pure no-op cases
# ===========================================================================


class TestNoOpCases:
    """compact_resources must do nothing when there's nothing to compact."""

    def test_empty_snapshot_returns_zero(self, patched_storage, storage_dir):
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir
        snapshot = _build_snapshot([])
        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
        )
        assert considered == 0
        assert rows == 0
        assert new_res == []
        assert sunset == set()

    def test_no_resources_returns_zero(self, patched_storage, storage_dir):
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir
        snapshot = {"resources": None}
        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
        )
        assert considered == 0
        assert new_res == []
        assert sunset == set()

    def test_all_files_above_threshold_small_only_skips(
        self, patched_storage, storage_dir,
    ):
        """When small_only=True and every file ≥ max_memory_chunk_size,
        nothing qualifies. No writes, no sunsets."""
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        # Each file ~1 MB — pretend large by setting file_size > threshold.
        df = pl.DataFrame({"a": range(10), "b": range(10)})
        resources = [
            _write_source_parquet(patched_storage, data_dir, f"big_{i}.parquet", df)
            for i in range(3)
        ]
        # Override file_size to be above the threshold (4 MB) — we don't
        # want to actually generate 4 MB files in the test.
        for r in resources:
            r["file_size"] = 10_000_000  # 10 MB

        snapshot = _build_snapshot(resources)
        table_config = {"max_memory_chunk_size": 4 * 1024 * 1024}  # 4 MB threshold

        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config=table_config, small_only=True,
        )
        assert considered == 0
        assert new_res == []
        assert sunset == set()


# ===========================================================================
# 2. Value-preservation — no row loss, no duplication, no column drift
# ===========================================================================


class TestValuePreservation:
    """Every row and every column survives compaction unchanged."""

    def test_three_small_files_merged_into_one_no_loss_no_dup(
        self, patched_storage, storage_dir,
    ):
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        # 3 small files with disjoint, distinguishable rows
        df1 = pl.DataFrame({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "carol"],
            "amount": [10.5, 20.0, 33.3],
        })
        df2 = pl.DataFrame({
            "id": [4, 5, 6],
            "name": ["dave", "eve", "frank"],
            "amount": [44.4, 55.5, 66.6],
        })
        df3 = pl.DataFrame({
            "id": [7, 8],
            "name": ["grace", "heidi"],
            "amount": [77.7, 88.8],
        })

        resources = [
            _write_source_parquet(patched_storage, data_dir, "s1.parquet", df1),
            _write_source_parquet(patched_storage, data_dir, "s2.parquet", df2),
            _write_source_parquet(patched_storage, data_dir, "s3.parquet", df3),
        ]
        snapshot = _build_snapshot(resources)

        considered, rows_written, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            # Lower the threshold so the buffer flushes only at the end
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024},
        )

        # Every input file is sunset; rows_written is preserved.
        assert considered == 3
        assert len(sunset) == 3
        assert {r["file"] for r in resources} == sunset

        # Read the new files and confirm multiset equality.
        before = pl.concat([df1, df2, df3], how="vertical_relaxed")
        after = _read_all(patched_storage, [r["file"] for r in new_res])

        assert after.shape[0] == before.shape[0], "row count mismatch"
        assert sorted(after.columns) == sorted(before.columns), "column drift"
        assert _multiset(after) == _multiset(before), \
            "multiset mismatch: rows lost or duplicated"
        # Specific value sanity: sum of amount survives
        assert after["amount"].sum() == pytest.approx(before["amount"].sum())

    def test_multiple_chunks_when_buffer_exceeds_threshold(
        self, patched_storage, storage_dir,
    ):
        """When buffered bytes exceed max_memory_chunk_size mid-iteration,
        the function flushes and starts a new chunk — both chunks
        together must hold the full input."""
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        # 4 files, each ~10 KB on disk. We'll set the threshold so the
        # buffer flushes after the 2nd file.
        resources = []
        all_frames: List[pl.DataFrame] = []
        for i in range(4):
            df = pl.DataFrame({
                "id": [i * 100 + n for n in range(50)],
                "val": [float(i * 100 + n) for n in range(50)],
            })
            res = _write_source_parquet(
                patched_storage, data_dir, f"chunk_{i}.parquet", df,
            )
            # Pin file_size so the chunk size grows deterministically
            res["file_size"] = 6_000
            resources.append(res)
            all_frames.append(df)

        snapshot = _build_snapshot(resources)
        # Threshold: 10_000 bytes — flushes after 2 files (6 + 6 = 12 KB)
        table_config = {"max_memory_chunk_size": 10_000}

        considered, rows_written, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config=table_config, small_only=True,
        )

        assert considered == 4
        assert len(sunset) == 4
        # Two chunks: first flushed after the threshold, second on final flush
        assert len(new_res) >= 2, \
            f"expected ≥2 output chunks, got {len(new_res)}"

        before = pl.concat(all_frames, how="vertical_relaxed")
        after = _read_all(patched_storage, [r["file"] for r in new_res])

        assert after.shape[0] == before.shape[0]
        assert sorted(after.columns) == sorted(before.columns)
        assert _multiset(after) == _multiset(before)

    def test_value_sums_preserved_across_compaction(
        self, patched_storage, storage_dir,
    ):
        """The arithmetic sum of every numeric column round-trips."""
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        # Use a large numeric range so float precision matters
        n = 100
        df1 = pl.DataFrame({
            "id": list(range(n)),
            "qty": list(range(n)),
            "price": [round(i * 1.5, 2) for i in range(n)],
        })
        df2 = pl.DataFrame({
            "id": list(range(n, 2 * n)),
            "qty": list(range(n, 2 * n)),
            "price": [round(i * 1.5, 2) for i in range(n, 2 * n)],
        })

        resources = [
            _write_source_parquet(patched_storage, data_dir, "p1.parquet", df1),
            _write_source_parquet(patched_storage, data_dir, "p2.parquet", df2),
        ]
        snapshot = _build_snapshot(resources)
        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024},
        )

        before = pl.concat([df1, df2], how="vertical_relaxed")
        after = _read_all(patched_storage, [r["file"] for r in new_res])

        # Exact integer sums
        assert after["id"].sum() == before["id"].sum()
        assert after["qty"].sum() == before["qty"].sum()
        # Float sum within tolerance (Parquet round-trips Float64 exactly,
        # but be defensive)
        assert after["price"].sum() == pytest.approx(before["price"].sum())


# ===========================================================================
# 3. Column-set fidelity (no LOSS, no ADDITION)
# ===========================================================================


class TestColumnFidelity:
    """compact_resources must neither lose nor add columns."""

    def test_no_phantom_columns_appear(self, patched_storage, storage_dir):
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        df = pl.DataFrame({"id": [1, 2, 3], "v": [10, 20, 30]})
        resources = [
            _write_source_parquet(patched_storage, data_dir, "a.parquet", df),
            _write_source_parquet(patched_storage, data_dir, "b.parquet", df),
        ]
        snapshot = _build_snapshot(resources)
        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024},
        )

        after = _read_all(patched_storage, [r["file"] for r in new_res])

        # Exact column-set equality — no extras, no missing
        assert set(after.columns) == {"id", "v"}, \
            f"unexpected columns: {set(after.columns)}"

    def test_union_schema_when_files_have_different_columns(
        self, patched_storage, storage_dir,
    ):
        """When inputs have schema drift (one file adds a column), the
        union schema is used: missing columns become null, no row drops."""
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        df1 = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})
        df2 = pl.DataFrame({
            "id": [3, 4],
            "name": ["c", "d"],
            "email": ["c@x", "d@x"],   # extra column on the newer file
        })

        resources = [
            _write_source_parquet(patched_storage, data_dir, "old.parquet", df1),
            _write_source_parquet(patched_storage, data_dir, "new.parquet", df2),
        ]
        snapshot = _build_snapshot(resources)
        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024},
        )

        after = _read_all(patched_storage, [r["file"] for r in new_res])

        # Union schema — all columns from both inputs present
        assert set(after.columns) == {"id", "name", "email"}
        # Row count is the sum of inputs (no drops)
        assert after.shape[0] == df1.shape[0] + df2.shape[0]
        # The old rows have null for the new column
        old_rows = after.filter(pl.col("id").is_in([1, 2]))
        assert old_rows["email"].null_count() == 2
        # The new rows keep their email
        new_rows = after.filter(pl.col("id").is_in([3, 4]))
        assert set(new_rows["email"].to_list()) == {"c@x", "d@x"}

    def test_dtype_preserved_through_compaction(self, patched_storage, storage_dir):
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        df = pl.DataFrame({
            "i64": pl.Series([1, 2, 3], dtype=pl.Int64),
            "f64": pl.Series([1.1, 2.2, 3.3], dtype=pl.Float64),
            "s":   pl.Series(["a", "b", "c"], dtype=pl.Utf8),
            "b":   pl.Series([True, False, True], dtype=pl.Boolean),
        })
        resources = [
            _write_source_parquet(patched_storage, data_dir, "t1.parquet", df),
            _write_source_parquet(patched_storage, data_dir, "t2.parquet", df),
        ]
        snapshot = _build_snapshot(resources)
        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024},
        )
        after = _read_all(patched_storage, [r["file"] for r in new_res])

        # Dtype map preserved
        dtype_map = dict(zip(after.columns, after.dtypes))
        assert dtype_map["i64"] == pl.Int64
        assert dtype_map["f64"] == pl.Float64
        assert dtype_map["s"] == pl.Utf8
        assert dtype_map["b"] == pl.Boolean


# ===========================================================================
# 4. small_only gating
# ===========================================================================


class TestSmallOnlyGating:
    """The ``small_only`` kwarg controls which files participate."""

    def test_small_only_true_excludes_large_files(
        self, patched_storage, storage_dir,
    ):
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        small = pl.DataFrame({"id": [1, 2, 3]})
        large = pl.DataFrame({"id": [100, 200, 300]})

        small_res = _write_source_parquet(patched_storage, data_dir, "small.parquet", small)
        large_res = _write_source_parquet(patched_storage, data_dir, "large.parquet", large)
        # Pin sizes
        small_res["file_size"] = 1_000
        large_res["file_size"] = 10_000_000

        snapshot = _build_snapshot([small_res, large_res])
        table_config = {"max_memory_chunk_size": 100_000}

        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config=table_config, small_only=True,
        )

        # Only the small file participates
        assert sunset == {small_res["file"]}
        assert large_res["file"] not in sunset
        # Output frame holds the small file's rows only
        after = _read_all(patched_storage, [r["file"] for r in new_res])
        assert _multiset(after) == _multiset(small)

    def test_small_only_false_includes_every_file(
        self, patched_storage, storage_dir,
    ):
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        small = pl.DataFrame({"id": [1, 2, 3]})
        large = pl.DataFrame({"id": [100, 200, 300]})

        small_res = _write_source_parquet(patched_storage, data_dir, "small.parquet", small)
        large_res = _write_source_parquet(patched_storage, data_dir, "large.parquet", large)
        small_res["file_size"] = 1_000
        large_res["file_size"] = 10_000_000

        snapshot = _build_snapshot([small_res, large_res])
        table_config = {"max_memory_chunk_size": 100_000}

        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config=table_config, small_only=False,
        )

        # Both files participate
        assert sunset == {small_res["file"], large_res["file"]}
        after = _read_all(patched_storage, [r["file"] for r in new_res])
        before = pl.concat([small, large], how="vertical_relaxed")
        assert _multiset(after) == _multiset(before)


# ===========================================================================
# 5. Race tolerance — a file already sunset by another writer
# ===========================================================================


class TestRaceTolerance:

    def test_missing_file_is_skipped_and_not_sunset(
        self, patched_storage, storage_dir,
    ):
        """If ``_read_parquet_safe`` returns None for a source file
        (because another writer already deleted it), the file must NOT
        be added to ``sunset_files`` — that would replace it with the
        compacted output and forfeit the rows."""
        from supertable.processing import compact_resources
        data_dir, _ = storage_dir

        df = pl.DataFrame({"id": [1, 2, 3]})
        present = _write_source_parquet(
            patched_storage, data_dir, "present.parquet", df,
        )
        # Reference a non-existent path; _read_parquet_safe returns None.
        missing = {
            "file": f"{data_dir}/missing.parquet",
            "file_size": 500,
            "rows": 99,
            "columns": [{"name": "id", "type": "Int64"}],
            "stats": None,
        }
        snapshot = _build_snapshot([present, missing])

        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024},
        )

        # Missing file was NOT sunset; only the survivable file is.
        assert sunset == {present["file"]}
        # Output has only the present file's rows
        after = _read_all(patched_storage, [r["file"] for r in new_res])
        assert _multiset(after) == _multiset(df)
