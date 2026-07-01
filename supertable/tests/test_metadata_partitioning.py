"""Partition-layout tests for the tombstone and stats write sites.

Every write emits a NEW versioned artifact (the previous one is retained for
snapshot/version history), so under heavy write volume a flat ``tombstone/`` or
``stats/`` folder would accumulate hundreds of thousands of files — making
directory listing and per-file creation slow on object stores and real
filesystems alike.  :func:`build_tombstone_file` and :func:`build_stats_file`
now place each new artifact under an hour-partitioned subdir via the shared
``_partitioned_new_path`` helper.

These tests pin that:
  * the returned (metadata-stored) path is nested under
    ``<base>/year=/month=/day=/hour=/`` and the partition dir is created;
  * pure carry-forward (no new rows) returns the prior path *verbatim* — so
    reads of pre-existing flat-layout artifacts are untouched and need no
    migration.
"""

from __future__ import annotations

import re

import polars as pl
from unittest.mock import MagicMock, patch

from supertable.processing import (
    STATS_SCHEMA,
    build_stats_file,
    build_tombstone_file,
    _partitioned_new_path,
)

_MOD = "supertable.processing"
_FIXED_PART = "year=2026/month=07/day=01/hour=17"
# <base>/year=YYYY/month=MM/day=DD/hour=HH/<ms>_<hex>_<alias>.parquet
_PART_RE = re.compile(r"/year=\d{4}/month=\d{2}/day=\d{2}/hour=\d{2}/[^/]+\.parquet$")


class _FakeStorage:
    """In-memory storage backend that also records makedirs() calls."""

    def __init__(self):
        self.store: dict = {}
        self.made_dirs: list = []

    def exists(self, path: str) -> bool:
        return path in self.store

    def makedirs(self, path: str) -> None:
        self.made_dirs.append(path)

    def write_bytes(self, path: str, data: bytes) -> None:
        self.store[path] = data

    def read_bytes(self, path: str) -> bytes:
        return self.store[path]

    def size(self, path: str) -> int:
        return len(self.store[path])


def _stats_row(file_path: str, value: int) -> pl.DataFrame:
    base = {k: [None] for k in STATS_SCHEMA}
    base["file_path"] = [file_path]
    base["row_group_id"] = [0]
    base["column_name"] = ["a"]
    base["physical_type"] = ["INT64"]
    base["logical_type"] = [""]
    base["min_bigint"] = [value]
    base["max_bigint"] = [value]
    base["null_count"] = [0]
    base["row_group_rows"] = [1]
    base["stats_available"] = [True]
    base["min_is_exact"] = [True]
    base["max_is_exact"] = [True]
    return pl.DataFrame(base, schema=STATS_SCHEMA)


# ---------------------------------------------------------------------------
# _partitioned_new_path — the shared helper both write sites call
# ---------------------------------------------------------------------------

class TestPartitionedNewPath:

    @patch(f"{_MOD}.hourly_partition_subpath", return_value=_FIXED_PART)
    @patch(f"{_MOD}._get_storage")
    def test_nests_under_partition_and_makedirs(self, mock_gs, _part):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path = _partitioned_new_path("t/tombstone", "deleted")
        assert path.startswith(f"t/tombstone/{_FIXED_PART}/")
        assert path.endswith("deleted.parquet")
        # the partition subdir is created idempotently before the write
        assert stor.made_dirs == [f"t/tombstone/{_FIXED_PART}"]

    @patch(f"{_MOD}.hourly_partition_subpath", return_value=_FIXED_PART)
    @patch(f"{_MOD}._get_storage")
    def test_makedirs_failure_is_swallowed(self, mock_gs, _part):
        # makedirs on an object store is a best-effort no-op that may raise;
        # the path must still be produced (dirs are implicit on object stores).
        stor = MagicMock()
        stor.makedirs.side_effect = OSError("object store has no directories")
        mock_gs.return_value = stor
        path = _partitioned_new_path("t/stats", "stats")
        assert path.startswith(f"t/stats/{_FIXED_PART}/")

    @patch(f"{_MOD}._get_storage")
    def test_real_helper_produces_hive_shape(self, mock_gs):
        # Unpatched partition helper: the live output really is 4 Hive segments.
        mock_gs.return_value = _FakeStorage()
        path = _partitioned_new_path("t/stats", "stats")
        assert path.startswith("t/stats/")
        assert _PART_RE.search(path) is not None


# ---------------------------------------------------------------------------
# build_tombstone_file
# ---------------------------------------------------------------------------

class TestTombstonePartitioning:

    @patch(f"{_MOD}._get_storage")
    def test_first_write_is_hour_partitioned(self, mock_gs):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path, combined = build_tombstone_file(
            tombstone_dir="t/tombstone",
            prev_tombstone_path=None,
            new_pairs=[("fileA.parquet", 1), ("fileA.parquet", 2)],
            compression_level=1,
        )
        assert path.startswith("t/tombstone/")
        assert _PART_RE.search(path) is not None
        assert path.endswith("deleted.parquet")
        assert path in stor.store          # a real artifact was written
        assert combined.height == 2

    @patch(f"{_MOD}.hourly_partition_subpath", return_value=_FIXED_PART)
    @patch(f"{_MOD}._get_storage")
    def test_exact_partition_path_and_dir(self, mock_gs, _part):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path, _ = build_tombstone_file(
            tombstone_dir="t/tombstone",
            prev_tombstone_path=None,
            new_pairs=[("fileA.parquet", 1)],
            compression_level=1,
        )
        assert path.startswith(f"t/tombstone/{_FIXED_PART}/")
        assert f"t/tombstone/{_FIXED_PART}" in stor.made_dirs

    @patch(f"{_MOD}._get_storage")
    def test_no_new_pairs_carries_forward_verbatim(self, mock_gs):
        # Pure carry-forward returns the prior (possibly legacy flat) path with
        # no rewrite and no partitioning — proving reads of pre-existing
        # flat-layout artifacts are untouched.
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path, combined = build_tombstone_file(
            tombstone_dir="t/tombstone",
            prev_tombstone_path="t/tombstone/legacy_flat.parquet",
            new_pairs=[],
            compression_level=1,
        )
        assert path == "t/tombstone/legacy_flat.parquet"
        assert combined is None
        assert stor.made_dirs == []        # nothing created


# ---------------------------------------------------------------------------
# build_stats_file
# ---------------------------------------------------------------------------

class TestStatsPartitioning:

    @patch(f"{_MOD}._get_storage")
    def test_first_write_is_hour_partitioned(self, mock_gs):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path, combined = build_stats_file(
            stats_dir="t/stats",
            prev_stats_path=None,
            new_rows=_stats_row("fileA.parquet", 5),
            removed_files=set(),
            compression_level=1,
        )
        assert path.startswith("t/stats/")
        assert _PART_RE.search(path) is not None
        assert path.endswith("stats.parquet")
        assert path in stor.store
        assert combined.height == 1

    @patch(f"{_MOD}.hourly_partition_subpath", return_value=_FIXED_PART)
    @patch(f"{_MOD}._get_storage")
    def test_exact_partition_path_and_dir(self, mock_gs, _part):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path, _ = build_stats_file(
            stats_dir="t/stats",
            prev_stats_path=None,
            new_rows=_stats_row("fileA.parquet", 5),
            removed_files=set(),
            compression_level=1,
        )
        assert path.startswith(f"t/stats/{_FIXED_PART}/")
        assert f"t/stats/{_FIXED_PART}" in stor.made_dirs

    @patch(f"{_MOD}._get_storage")
    def test_no_change_carries_forward_verbatim(self, mock_gs):
        stor = _FakeStorage()
        mock_gs.return_value = stor
        path, combined = build_stats_file(
            stats_dir="t/stats",
            prev_stats_path="t/stats/legacy_flat.parquet",
            new_rows=None,
            removed_files=set(),
            compression_level=1,
        )
        assert path == "t/stats/legacy_flat.parquet"
        assert combined is None
        assert stor.made_dirs == []
