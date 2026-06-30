"""Compaction *selection* contract: which files each phase TOUCHES and READS.

The user's requirement (verbatim): *"make sure we touch only the tombstone
files and the ones which is below the threshold and we do not read through all
the others."*  Value-preservation (no row/column loss) is already sealed by
``test_processing_compact_resources.py``; this suite seals the **targeting** —
proven by spying on ``_read_parquet_safe`` so a file that is read at all shows
up, and asserting the read set is exactly the intended subset.

Two physical phases share one chokepoint but select differently:

  * Phase A — ``compact_tombstones``: reads ONLY the data files named in the
    deletion-vector (``__file__``).  Every other live file is invisible to it.
  * Phase B — ``compact_resources(small_only=True)``: reads ONLY files whose
    ``file_size`` is strictly below ``max_memory_chunk_size``.  Files at/above
    the threshold are skipped *before* the read — never opened.

All Parquet I/O goes through a real on-disk ``LocalStorage`` in ``tmp_path`` so
the format and the ``_read_parquet_safe`` code path are exercised end-to-end.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List, Tuple
from unittest.mock import patch

import polars as pl
import pyarrow.parquet as pq
import pytest

# Stable env so config imports cleanly even when run in isolation.
os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable.utils.profiler import Profiler  # noqa: E402

ROWID = "__rowid__"
TFILE = "__file__"


# ---------------------------------------------------------------------------
# Harness — real LocalStorage rooted in tmp_path + a read spy
# ---------------------------------------------------------------------------

@pytest.fixture
def data_dir(tmp_path):
    d = str(tmp_path / "warehouse" / "orders" / "data")
    os.makedirs(d, exist_ok=True)
    return d


@pytest.fixture
def patched_storage():
    """Point processing's storage at a fresh LocalStorage (no state leak)."""
    from supertable import processing as proc_mod
    from supertable.storage.local_storage import LocalStorage

    proc_mod._storage = None
    storage = LocalStorage()
    with patch.object(proc_mod, "get_storage", return_value=storage):
        yield storage
    proc_mod._storage = None


def _spy_reads(monkeypatch) -> List[str]:
    """Record every path handed to ``_read_parquet_safe`` (then delegate)."""
    from supertable import processing as proc_mod

    real = proc_mod._read_parquet_safe
    reads: List[str] = []

    def _rec(path, *a, **k):
        reads.append(path)
        return real(path, *a, **k)

    monkeypatch.setattr(proc_mod, "_read_parquet_safe", _rec)
    return reads


def _write_file(data_dir: str, name: str, df: pl.DataFrame, *, size: int | None = None) -> Dict:
    """Write *df* as a Parquet file and return its snapshot resource dict.

    When *size* is given it overrides the on-disk ``file_size`` so a file can be
    classified "large" without materializing megabytes (the selection logic
    keys off the resource's ``file_size``, not the real bytes).
    """
    abs_path = os.path.join(data_dir, name)
    Path(abs_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(df.to_arrow(), abs_path, compression="zstd", compression_level=1)
    return {
        "file": abs_path,
        "file_size": int(size if size is not None else os.path.getsize(abs_path)),
        "rows": df.shape[0],
        "columns": [{"name": c, "type": str(t)} for c, t in zip(df.columns, df.dtypes)],
        "stats": None,
    }


def _tomb(pairs: List[Tuple[str, int]]) -> pl.DataFrame:
    """Build a deletion-vector frame from (file_path, rowid) pairs."""
    return pl.DataFrame(
        {TFILE: [p for p, _ in pairs], ROWID: [r for _, r in pairs]}
    )


def _read(path: str) -> pl.DataFrame:
    return pl.read_parquet(path)


# ===========================================================================
# Phase A — compact_tombstones: touch ONLY deletion-vector files
# ===========================================================================

class TestTombstoneSelectionReadsOnlyDVFiles:

    def test_reads_only_files_named_in_deletion_vector(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """5 live files, DV names 2 of them → exactly those 2 are read; the
        other 3 are never opened."""
        from supertable.processing import compact_tombstones

        f0 = _write_file(data_dir, "f0.parquet", pl.DataFrame({ROWID: [1, 2], "v": ["a", "b"]}))
        f1 = _write_file(data_dir, "f1.parquet", pl.DataFrame({ROWID: [10, 11, 12], "v": ["c", "d", "e"]}))
        f2 = _write_file(data_dir, "f2.parquet", pl.DataFrame({ROWID: [20, 21], "v": ["f", "g"]}))
        f3 = _write_file(data_dir, "f3.parquet", pl.DataFrame({ROWID: [30, 31, 32], "v": ["h", "i", "j"]}))
        f4 = _write_file(data_dir, "f4.parquet", pl.DataFrame({ROWID: [40], "v": ["k"]}))
        snapshot = {"resources": [f0, f1, f2, f3, f4]}

        # Drop rowid 11 from f1 and rowid 31 from f3.
        dv = _tomb([(f1["file"], 11), (f3["file"], 31)])

        reads = _spy_reads(monkeypatch)
        removed, new_res, sunset = compact_tombstones(
            snapshot=snapshot, tombstone_df=dv, data_dir=data_dir, compression_level=1,
        )

        # Only the two DV-named files were ever read.
        assert set(reads) == {f1["file"], f3["file"]}
        for untouched in (f0["file"], f2["file"], f4["file"]):
            assert untouched not in reads, f"{untouched} was read but is not in the DV"

        # Two rows removed; exactly the two DV files sunset.
        assert removed == 2
        assert sunset == {f1["file"], f3["file"]}

        # Survivors keep their original rowids; the dead ones are gone.
        survivors = pl.concat([_read(r["file"]) for r in new_res], how="vertical_relaxed")
        assert set(survivors[ROWID].to_list()) == {10, 12, 30, 32}

    def test_files_outside_dv_are_not_sunset(
        self, patched_storage, data_dir, monkeypatch,
    ):
        keep = _write_file(data_dir, "keep.parquet", pl.DataFrame({ROWID: [1, 2], "v": ["a", "b"]}))
        hit = _write_file(data_dir, "hit.parquet", pl.DataFrame({ROWID: [3, 4], "v": ["c", "d"]}))
        snapshot = {"resources": [keep, hit]}
        dv = _tomb([(hit["file"], 3)])

        from supertable.processing import compact_tombstones
        reads = _spy_reads(monkeypatch)
        removed, new_res, sunset = compact_tombstones(
            snapshot=snapshot, tombstone_df=dv, data_dir=data_dir, compression_level=1,
        )

        assert keep["file"] not in reads
        assert keep["file"] not in sunset
        assert sunset == {hit["file"]}
        assert removed == 1

    def test_only_listed_rowids_dropped_within_a_touched_file(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """Within a touched file only the DV rowids vanish; siblings survive."""
        from supertable.processing import compact_tombstones
        f = _write_file(
            data_dir, "f.parquet",
            pl.DataFrame({ROWID: [1, 2, 3, 4, 5], "v": ["a", "b", "c", "d", "e"]}),
        )
        snapshot = {"resources": [f]}
        dv = _tomb([(f["file"], 2), (f["file"], 4)])

        _spy_reads(monkeypatch)
        removed, new_res, sunset = compact_tombstones(
            snapshot=snapshot, tombstone_df=dv, data_dir=data_dir, compression_level=1,
        )

        assert removed == 2
        out = pl.concat([_read(r["file"]) for r in new_res], how="vertical_relaxed")
        assert sorted(out[ROWID].to_list()) == [1, 3, 5]
        assert sorted(out["v"].to_list()) == ["a", "c", "e"]

    def test_dv_file_with_no_matching_rowids_is_read_but_not_rewritten(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """A DV-named file whose dead rowids are already absent is opened (we
        must check) but produces difference==0 → not sunset, not rewritten."""
        from supertable.processing import compact_tombstones
        f = _write_file(data_dir, "f.parquet", pl.DataFrame({ROWID: [1, 2, 3], "v": ["a", "b", "c"]}))
        snapshot = {"resources": [f]}
        dv = _tomb([(f["file"], 9999)])  # rowid not present

        reads = _spy_reads(monkeypatch)
        removed, new_res, sunset = compact_tombstones(
            snapshot=snapshot, tombstone_df=dv, data_dir=data_dir, compression_level=1,
        )

        assert f["file"] in reads          # it WAS examined ...
        assert removed == 0                 # ... but nothing was dead ...
        assert sunset == set()              # ... so nothing was rewritten.
        assert new_res == []

    def test_dv_file_absent_from_snapshot_is_skipped_without_reading(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """A DV entry for a file no longer in the snapshot (already sunset by a
        prior compaction) is skipped *before* any read."""
        from supertable.processing import compact_tombstones
        live = _write_file(data_dir, "live.parquet", pl.DataFrame({ROWID: [1, 2], "v": ["a", "b"]}))
        snapshot = {"resources": [live]}
        ghost = os.path.join(data_dir, "ghost.parquet")  # never written, not in resources
        dv = _tomb([(ghost, 1), (live["file"], 2)])

        reads = _spy_reads(monkeypatch)
        removed, new_res, sunset = compact_tombstones(
            snapshot=snapshot, tombstone_df=dv, data_dir=data_dir, compression_level=1,
        )

        assert ghost not in reads
        assert reads == [live["file"]]
        assert sunset == {live["file"]}
        assert removed == 1

    def test_profiler_counts_total_and_touched(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """``tombstone_files_total`` = distinct DV files; ``..._touched`` =
        files actually rewritten (the inputs to the write-path log line)."""
        from supertable.processing import compact_tombstones
        a = _write_file(data_dir, "a.parquet", pl.DataFrame({ROWID: [1, 2], "v": ["a", "b"]}))
        b = _write_file(data_dir, "b.parquet", pl.DataFrame({ROWID: [3, 4], "v": ["c", "d"]}))
        c = _write_file(data_dir, "c.parquet", pl.DataFrame({ROWID: [5, 6], "v": ["e", "f"]}))
        snapshot = {"resources": [a, b, c]}
        # a: real delete; b: no-op (rowid absent); c: not referenced at all.
        dv = _tomb([(a["file"], 1), (b["file"], 9999)])

        prof = Profiler()
        _spy_reads(monkeypatch)
        compact_tombstones(
            snapshot=snapshot, tombstone_df=dv, data_dir=data_dir, compression_level=1,
            profiler=prof,
        )

        counts = prof.emit_counts()
        assert counts.get("tombstone_files_total") == 2    # a and b (distinct DV files)
        assert counts.get("tombstone_files_touched") == 1  # only a was rewritten


# ===========================================================================
# Phase B — compact_resources(small_only=True): read ONLY sub-threshold files
# ===========================================================================

class TestSmallFileSelectionReadsOnlySmallFiles:

    def test_large_files_are_never_read(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """Files whose ``file_size`` >= max_memory_chunk_size are skipped before
        the read — proven by the spy, not just by absence from ``sunset``."""
        from supertable.processing import compact_resources

        small1 = _write_file(data_dir, "s1.parquet", pl.DataFrame({"id": [1, 2]}), size=1_000)
        small2 = _write_file(data_dir, "s2.parquet", pl.DataFrame({"id": [3, 4]}), size=1_000)
        big1 = _write_file(data_dir, "big1.parquet", pl.DataFrame({"id": [100]}), size=50_000_000)
        big2 = _write_file(data_dir, "big2.parquet", pl.DataFrame({"id": [200]}), size=50_000_000)
        snapshot = {"resources": [small1, big1, small2, big2]}
        table_config = {"max_memory_chunk_size": 10 * 1024 * 1024}  # 10 MiB

        reads = _spy_reads(monkeypatch)
        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config=table_config, small_only=True,
        )

        # Only the two small files were opened; neither big file was read.
        assert set(reads) == {small1["file"], small2["file"]}
        assert big1["file"] not in reads
        assert big2["file"] not in reads
        # And only the small files were sunset.
        assert sunset == {small1["file"], small2["file"]}
        assert considered == 2

    def test_profiler_attributes_phase_b_io(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """The profiler threading lets Phase B's reads/writes be attributed
        (this is what makes the compaction I/O visible in the write log)."""
        from supertable.processing import compact_resources

        s1 = _write_file(data_dir, "s1.parquet", pl.DataFrame({"id": [1, 2, 3]}), size=2_000)
        s2 = _write_file(data_dir, "s2.parquet", pl.DataFrame({"id": [4, 5, 6]}), size=2_000)
        snapshot = {"resources": [s1, s2]}

        prof = Profiler()
        compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024},
            small_only=True, profiler=prof,
        )

        counts = prof.emit_counts()
        assert counts.get("compact_small_candidates") == 2
        assert counts.get("files_read") == 2
        assert counts.get("bytes_read", 0) > 0       # 2 * 2_000
        assert counts.get("files_written", 0) >= 1
        assert counts.get("bytes_written", 0) > 0

    def test_no_profiler_is_still_a_no_op_path(
        self, patched_storage, data_dir, monkeypatch,
    ):
        """Omitting the profiler (export_to / compact() callers) must behave
        identically — the kwarg is purely additive instrumentation."""
        from supertable.processing import compact_resources
        s1 = _write_file(data_dir, "s1.parquet", pl.DataFrame({"id": [1, 2]}), size=2_000)
        s2 = _write_file(data_dir, "s2.parquet", pl.DataFrame({"id": [3, 4]}), size=2_000)
        snapshot = {"resources": [s1, s2]}

        considered, rows, new_res, sunset = compact_resources(
            snapshot=snapshot, data_dir=data_dir, compression_level=1,
            table_config={"max_memory_chunk_size": 10 * 1024 * 1024}, small_only=True,
        )
        assert considered == 2
        assert sunset == {s1["file"], s2["file"]}
        after = pl.concat([_read(r["file"]) for r in new_res], how="vertical_relaxed")
        assert sorted(after["id"].to_list()) == [1, 2, 3, 4]
