"""Large-table migration must reuse proofs without weakening source fencing."""

import gc
import hashlib
import json
import os
import sqlite3
import subprocess
from collections import Counter
from datetime import datetime, timezone

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.redis_catalog import RedisCatalog
from supertable.storage.local_storage import LocalStorage
from supertable.super_table import SuperTable
from supertable.tests.test_super_table_migration import (
    _seed_named_authentic_v2_4_table,
    _seed_v2_4_arrow_table,
)


@pytest.fixture(autouse=True)
def _collect_migration_fixture_cycles():
    # Wrappers capture LocalStorage/catalog objects whose private directory
    # handles are retained by mock cycles until collection. This autouse
    # fixture tears down after the regular monkeypatch fixture restores them.
    gc.collect()
    yield
    gc.collect()


def _migration_namespace(tmp_path):
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    client.set(RK.meta_root("org", "lake"), json.dumps({
        "version": 9, "ts": 1, "read_only": False,
    }))
    sources = {
        simple: _seed_named_authentic_v2_4_table(
            storage, client, simple, rowid_start,
        )
        for simple, rowid_start in (("alpha", 10), ("omega", 20))
    }
    table = SuperTable.__new__(SuperTable)
    table.organization = "org"
    table.super_name = "lake"
    table.storage = storage
    table.catalog = catalog
    return table, client, sources


def _capture_scans(monkeypatch, table):
    records = []
    original = table._scan_v2_4_resources

    def capture(**kwargs):
        result = original(**kwargs)
        records.append((kwargs["simple"], result[0].name, result[1]))
        return result

    monkeypatch.setattr(table, "_scan_v2_4_resources", capture)
    return records


def _assert_scans_cleaned(records):
    assert records
    for _simple, directory, connection in records:
        assert not os.path.exists(directory)
        with pytest.raises(sqlite3.ProgrammingError, match="closed database"):
            connection.execute("SELECT 1")


def _assert_locks_released(client, sources):
    assert client.get(RK.lock_namespace("org", "lake")) is None
    for simple in sources:
        assert client.get(RK.lock_leaf("org", "lake", simple)) is None


def test_v2_4_migration_reads_each_data_object_once_and_preserves_sources(
    tmp_path, monkeypatch,
):
    table, client, sources = _migration_namespace(tmp_path)
    data_paths = set()
    original_hashes = {}
    for snapshot_path in sources.values():
        snapshot = table.storage.read_json(snapshot_path)
        data_paths.update(resource["file"] for resource in snapshot["resources"])
        for path in (
            snapshot_path,
            snapshot["tombstone"],
            *(resource["file"] for resource in snapshot["resources"]),
        ):
            original_hashes[path] = hashlib.sha256(
                table.storage.read_bytes(path),
            ).hexdigest()
    records = _capture_scans(monkeypatch, table)
    data_downloads = Counter()
    original_download = table.storage.download_to_file

    def download(path, *args, **kwargs):
        if path in data_paths:
            data_downloads[path] += 1
        return original_download(path, *args, **kwargs)

    monkeypatch.setattr(table.storage, "download_to_file", download)

    result = table.migrate_legacy_metadata(confirm_system_offline=True)

    assert set(result["migrated_tables"]) == set(sources)
    assert Counter(simple for simple, _path, _db in records) == {
        "alpha": 1, "omega": 1,
    }
    assert data_downloads == Counter({path: 1 for path in data_paths})
    for path, expected_hash in original_hashes.items():
        assert hashlib.sha256(table.storage.read_bytes(path)).hexdigest() == expected_hash
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)


def test_v2_4_migration_rejects_changed_data_identity_between_passes(
    tmp_path, monkeypatch,
):
    table, client, sources = _migration_namespace(tmp_path)
    records = _capture_scans(monkeypatch, table)
    original_migrate_leaf = table._migrate_legacy_leaf
    changed = False

    def change_before_publication(**kwargs):
        nonlocal changed
        if not kwargs.get("preflight_only", False) and not changed:
            snapshot = table.storage.read_json(sources[kwargs["simple"]])
            path = tmp_path / snapshot["resources"][0]["file"]
            stat = path.stat()
            # Keep every byte/footer unchanged: identity checks themselves
            # must prohibit borrowing the previous pass's row-ID proof.
            os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
            changed = True
        return original_migrate_leaf(**kwargs)

    monkeypatch.setattr(table, "_migrate_legacy_leaf", change_before_publication)
    with pytest.raises((RuntimeError, ValueError), match="changed|identity|proof"):
        table.migrate_legacy_metadata(confirm_system_offline=True)

    assert changed
    for simple, source_path in sources.items():
        leaf = table.catalog.get_leaf("org", "lake", simple)
        assert leaf["version"] == 4
        assert leaf["path"] == source_path
    assert table.catalog.get_root("org", "lake")["version"] == 9
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)


@pytest.mark.parametrize("corruption", ["length", "digest"])
def test_v2_4_migration_rejects_corrupted_private_scan_proofs(
    tmp_path, monkeypatch, corruption,
):
    table, client, sources = _migration_namespace(tmp_path)
    records = _capture_scans(monkeypatch, table)
    original_migrate_leaf = table._migrate_legacy_leaf
    corrupted = False

    def corrupt_before_publication(**kwargs):
        nonlocal corrupted
        if not kwargs.get("preflight_only", False) and not corrupted:
            prepared = kwargs["scan_cache"][kwargs["simple"]]
            connection = sqlite3.connect(
                os.path.join(prepared.directory.name, "rowids.sqlite3"),
            )
            try:
                encoded = connection.execute(
                    "SELECT payload FROM migration_scan_proof",
                ).fetchone()[0]
                changed = (
                    encoded + b"x" if corruption == "length"
                    else bytes([encoded[0] ^ 1]) + encoded[1:]
                )
                connection.execute(
                    "UPDATE migration_scan_proof SET payload = ?", (changed,),
                )
                connection.commit()
            finally:
                connection.close()
            corrupted = True
        return original_migrate_leaf(**kwargs)

    monkeypatch.setattr(table, "_migrate_legacy_leaf", corrupt_before_publication)
    with pytest.raises(RuntimeError, match="scan proof changed"):
        table.migrate_legacy_metadata(confirm_system_offline=True)

    assert corrupted
    for simple, source_path in sources.items():
        assert table.catalog.get_leaf("org", "lake", simple)["path"] == source_path
    assert table.catalog.get_root("org", "lake")["version"] == 9
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)


def test_v2_4_migration_revalidates_tombstone_membership_when_reusing_scan(
    tmp_path, monkeypatch,
):
    import pyarrow as pa

    table, client, sources = _migration_namespace(tmp_path)
    records = _capture_scans(monkeypatch, table)
    original_migrate_leaf = table._migrate_legacy_leaf
    corrupted = False

    def corrupt_before_publication(**kwargs):
        nonlocal corrupted
        if not kwargs.get("preflight_only", False) and not corrupted:
            snapshot = table.storage.read_json(sources[kwargs["simple"]])
            vector = table.storage.read_parquet(snapshot["tombstone"])
            index = vector.column_names.index("__rowid__")
            vector = vector.set_column(
                index, vector.schema.field(index),
                pa.array([999], type=pa.int64()),
            )
            table.storage.write_parquet(vector, snapshot["tombstone"])
            corrupted = True
        return original_migrate_leaf(**kwargs)

    monkeypatch.setattr(table, "_migrate_legacy_leaf", corrupt_before_publication)
    with pytest.raises(ValueError, match="does not identify a physical row"):
        table.migrate_legacy_metadata(confirm_system_offline=True)

    assert corrupted
    assert Counter(simple for simple, _path, _db in records) == {
        "alpha": 1, "omega": 1,
    }
    for simple, source_path in sources.items():
        assert table.catalog.get_leaf("org", "lake", simple)["path"] == source_path
    assert table.catalog.get_root("org", "lake")["version"] == 9
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)


def test_v2_4_migration_preflight_interruption_discards_invocation_scan_cache(
    tmp_path, monkeypatch,
):
    table, client, sources = _migration_namespace(tmp_path)
    records = _capture_scans(monkeypatch, table)
    original_migrate_leaf = table._migrate_legacy_leaf

    def stop_before_publication(**kwargs):
        if not kwargs.get("preflight_only", False):
            raise RuntimeError("injected preflight-only interruption")
        return original_migrate_leaf(**kwargs)

    monkeypatch.setattr(table, "_migrate_legacy_leaf", stop_before_publication)
    with pytest.raises(RuntimeError, match="preflight-only interruption"):
        table.migrate_legacy_metadata(confirm_system_offline=True)

    first_attempt = list(records)
    assert Counter(simple for simple, _path, _db in first_attempt) == {
        "alpha": 1, "omega": 1,
    }
    _assert_scans_cleaned(first_attempt)
    _assert_locks_released(client, sources)
    for simple, source_path in sources.items():
        assert table.catalog.get_leaf("org", "lake", simple)["path"] == source_path

    monkeypatch.setattr(table, "_migrate_legacy_leaf", original_migrate_leaf)
    result = table.migrate_legacy_metadata(confirm_system_offline=True)

    assert set(result["migrated_tables"]) == set(sources)
    assert Counter(simple for simple, _path, _db in records) == {
        "alpha": 2, "omega": 2,
    }
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)


@pytest.mark.parametrize("failure_type", [RuntimeError, KeyboardInterrupt])
def test_v2_4_migration_cleans_all_prepared_scans_after_late_preflight_failure(
    tmp_path, monkeypatch, failure_type,
):
    table, client, sources = _migration_namespace(tmp_path)
    records = _capture_scans(monkeypatch, table)
    original_tombstone = table._migrate_legacy_tombstone
    tombstone_calls = 0

    def fail_late_preflight(**kwargs):
        nonlocal tombstone_calls
        tombstone_calls += 1
        if tombstone_calls == 2:
            raise failure_type("injected late preflight failure")
        return original_tombstone(**kwargs)

    monkeypatch.setattr(table, "_migrate_legacy_tombstone", fail_late_preflight)
    with pytest.raises(failure_type, match="late preflight failure"):
        table.migrate_legacy_metadata(confirm_system_offline=True)

    assert Counter(simple for simple, _path, _db in records) == {
        "alpha": 1, "omega": 1,
    }
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)
    for simple, source_path in sources.items():
        assert table.catalog.get_leaf("org", "lake", simple)["path"] == source_path
    assert table.catalog.get_root("org", "lake")["version"] == 9


def test_v2_4_migration_publication_failure_cleans_cache_and_rescans_unfinished_table(
    tmp_path, monkeypatch,
):
    table, client, sources = _migration_namespace(tmp_path)
    records = _capture_scans(monkeypatch, table)
    original_commit = table.catalog.commit_snapshot
    commit_calls = 0

    def fail_second_commit(*args, **kwargs):
        nonlocal commit_calls
        commit_calls += 1
        if commit_calls == 2:
            raise RuntimeError("injected second publication failure")
        return original_commit(*args, **kwargs)

    monkeypatch.setattr(table.catalog, "commit_snapshot", fail_second_commit)
    with pytest.raises(RuntimeError, match="second publication failure"):
        table.migrate_legacy_metadata(confirm_system_offline=True)

    assert Counter(simple for simple, _path, _db in records) == {
        "alpha": 1, "omega": 1,
    }
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)
    unfinished = [
        simple for simple in sources
        if table.catalog.get_leaf("org", "lake", simple)["version"] == 4
    ]
    assert len(unfinished) == 1
    monkeypatch.setattr(table.catalog, "commit_snapshot", original_commit)
    result = table.migrate_legacy_metadata(confirm_system_offline=True)

    assert result["migrated_tables"] == unfinished
    expected_counts = Counter({"alpha": 1, "omega": 1})
    expected_counts.update(unfinished)
    assert Counter(simple for simple, _path, _db in records) == expected_counts
    for simple in sources:
        assert table.catalog.get_leaf("org", "lake", simple)["version"] == 5
    _assert_scans_cleaned(records)
    _assert_locks_released(client, sources)


def test_v2_4_array_migration_handles_worker_pipe_above_select_fd_limit(
    tmp_path, monkeypatch,
):
    import errno

    import polars as pl

    resource = pytest.importorskip("resource")
    descriptor_limit, _hard_limit = resource.getrlimit(resource.RLIMIT_NOFILE)
    if descriptor_limit != resource.RLIM_INFINITY and descriptor_limit < 1200:
        pytest.skip("requires room for descriptors above select's 1024-fd ceiling")
    storage = LocalStorage(str(tmp_path))
    client = fakeredis.FakeStrictRedis(decode_responses=True)
    catalog = RedisCatalog(redis_client=client)
    frame = pl.DataFrame({
        "vector": pl.Series([[1.0, 2.0], [3.0, 4.0]], dtype=pl.Array(pl.Float64, 2)),
        "__rowid__": pl.Series([10, 11], dtype=pl.Int64),
        "__timestamp__": pl.Series(
            [datetime(2026, 1, 1, tzinfo=timezone.utc)] * 2,
            dtype=pl.Datetime("us", "UTC"),
        ),
    })
    _seed_v2_4_arrow_table(
        storage, client, catalog, frame.to_arrow(),
        {name: str(dtype) for name, dtype in frame.schema.items()},
    )
    table = SuperTable.__new__(SuperTable)
    table.organization = "org"
    table.super_name = "lake"
    table.storage = storage
    table.catalog = catalog
    original_popen = subprocess.Popen
    workers = []
    worker_output_fds = []

    def record_worker(*args, **kwargs):
        process = original_popen(*args, **kwargs)
        workers.append(process)
        if process.stdout is not None:
            worker_output_fds.append(process.stdout.fileno())
        return process

    monkeypatch.setattr(subprocess, "Popen", record_worker)
    descriptors = []
    try:
        # Reserve harmless parent descriptors until a subsequently created
        # worker pipe cannot fit select.select's fixed-size descriptor set.
        while not descriptors or descriptors[-1] < 1100:
            try:
                descriptors.append(os.open(os.devnull, os.O_RDONLY))
            except OSError as error:
                if error.errno == errno.EMFILE:
                    pytest.skip("process cannot admit the high-fd regression fixture")
                raise
        result = table.migrate_legacy_metadata(confirm_system_offline=True)
        assert result["migrated_tables"] == ["facts"]
        assert worker_output_fds and min(worker_output_fds) > 1023
        assert all(process.poll() is not None for process in workers)
        assert catalog.get_leaf("org", "lake", "facts")["version"] == 5
        _assert_locks_released(client, {"facts": None})
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        for process in workers:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=2)
