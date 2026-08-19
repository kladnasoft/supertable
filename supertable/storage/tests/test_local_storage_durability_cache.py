"""Adversarial coverage for LocalStorage's durable-directory hot path."""

from __future__ import annotations

import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

import pytest

from supertable.storage.local_storage import LocalStorage


def _recording_directory_sync(calls: list[str]):
    original = LocalStorage._fsync_directory

    def sync(directory: str):
        calls.append(os.path.abspath(directory))
        return original(directory)

    return sync


def test_first_write_anchors_full_chain_then_hot_path_syncs_destination_only(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(tmp_path)
    destination = tmp_path / "table" / "hour"
    calls: list[str] = []

    with patch.object(
        LocalStorage,
        "_fsync_directory",
        side_effect=_recording_directory_sync(calls),
    ):
        storage.write_bytes("table/hour/first.bin", b"first")
        first_write = list(calls)
        calls.clear()
        storage.write_bytes("table/hour/second.bin", b"second")

    assert first_write == [
        str(destination),
        str(tmp_path / "table"),
        str(tmp_path),
    ]
    assert calls == [str(destination)]


def test_partial_ancestor_fsync_failure_is_not_cached_and_retry_reanchors(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(tmp_path)
    destination = tmp_path / "retry" / "child"
    original = LocalStorage._fsync_directory
    failed_calls: list[str] = []

    def fail_second_sync(directory: str):
        failed_calls.append(os.path.abspath(directory))
        if len(failed_calls) == 2:
            raise OSError("injected ancestor fsync failure")
        return original(directory)

    with (
        patch.object(
            LocalStorage,
            "_fsync_directory",
            side_effect=fail_second_sync,
        ),
        pytest.raises(OSError, match="injected ancestor fsync failure"),
    ):
        storage.write_bytes("retry/child/object.bin", b"visible-not-acknowledged")

    assert (destination / "object.bin").read_bytes() == b"visible-not-acknowledged"
    retry_calls: list[str] = []
    with patch.object(
        LocalStorage,
        "_fsync_directory",
        side_effect=_recording_directory_sync(retry_calls),
    ):
        storage.write_bytes("retry/child/object.bin", b"retry")

    assert retry_calls == [
        str(destination),
        str(tmp_path / "retry"),
        str(tmp_path),
    ]
    assert (destination / "object.bin").read_bytes() == b"retry"


def test_replaced_directory_inode_invalidates_cache_and_reanchors_link(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(tmp_path)
    parent = tmp_path / "replace"
    destination = parent / "child"
    storage.write_bytes("replace/child/first.bin", b"first")
    original_identity = (destination.stat().st_dev, destination.stat().st_ino)

    # Bypass LocalStorage to model another process replacing the directory.
    # The cached open handle pins the old inode, so the replacement cannot be
    # mistaken for the previous incarnation through immediate inode reuse.
    shutil.rmtree(destination)
    destination.mkdir()
    replacement_identity = (destination.stat().st_dev, destination.stat().st_ino)
    assert replacement_identity != original_identity

    calls: list[str] = []
    with patch.object(
        LocalStorage,
        "_fsync_directory",
        side_effect=_recording_directory_sync(calls),
    ):
        storage.write_bytes("replace/child/second.bin", b"second")

    # The new child entry must be persisted in its still-valid cached parent;
    # the unchanged storage root does not need another fsync.
    assert calls == [str(destination), str(parent)]
    assert (destination / "second.bin").read_bytes() == b"second"


def test_concurrent_first_writes_install_one_complete_anchor_chain(
    tmp_path: Path,
) -> None:
    storage = LocalStorage(tmp_path)
    destination = tmp_path / "concurrent" / "deep"
    worker_count = 8
    barrier = threading.Barrier(worker_count)
    calls: list[str] = []
    calls_lock = threading.Lock()
    original = LocalStorage._fsync_directory

    def record_sync(directory: str):
        with calls_lock:
            calls.append(os.path.abspath(directory))
        return original(directory)

    def publish(index: int) -> None:
        barrier.wait()
        storage.write_bytes(
            f"concurrent/deep/{index}.bin",
            str(index).encode("ascii"),
        )

    with patch.object(
        LocalStorage,
        "_fsync_directory",
        side_effect=record_sync,
    ):
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            list(executor.map(publish, range(worker_count)))

    assert calls.count(str(destination)) == worker_count
    assert calls.count(str(tmp_path / "concurrent")) == 1
    assert calls.count(str(tmp_path)) == 1
    for index in range(worker_count):
        assert (destination / f"{index}.bin").read_bytes() == str(index).encode("ascii")
