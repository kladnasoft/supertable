from types import SimpleNamespace

import pytest

from supertable import processing
from supertable.data_writer import DataWriter
from supertable.engine import file_cache as file_cache_module
from supertable.storage.local_storage import LocalStorage


def _scope(identity: str) -> str:
    return identity.split("/", 2)[1]


def _writer(storage, organization: str = "org") -> DataWriter:
    writer = DataWriter.__new__(DataWriter)
    writer.super_table = SimpleNamespace(
        organization=organization,
        storage=storage,
    )
    return writer


def test_writer_reuses_exact_local_scope_without_file_cache_or_realpath(
        tmp_path, monkeypatch,
):
    storage = LocalStorage(tmp_path / "storage")
    writer = _writer(storage)
    scope_calls = 0
    original_scope = processing._local_artifact_cache_scope

    def counted_scope(state):
        nonlocal scope_calls
        scope_calls += 1
        return original_scope(state)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("stable local identity reconstructed FileCache/realpath")

    monkeypatch.setattr(processing, "_local_artifact_cache_scope", counted_scope)
    monkeypatch.setattr(file_cache_module.FileCache, "__init__", forbidden)
    monkeypatch.setattr(file_cache_module.os.path, "realpath", forbidden)

    identities = []
    for index in range(55):
        kind = "stats" if index % 2 == 0 else "tombstone"
        path = f"org/lake/tables/t/{kind}/v{index}.parquet"
        method = (
            writer._stats_cache_identity
            if kind == "stats" else writer._tombstone_cache_identity
        )
        identities.append(method(path))

    assert scope_calls == 1
    assert len(set(identities)) == 55
    assert _scope(identities[0]) == _scope(
        processing.stats_cache_identity(
            "org/lake/tables/t/stats/direct.parquet",
            organization="org",
            storage=storage,
        )
    )


def test_writer_local_scope_recomputes_on_hostile_identity_changes(
        tmp_path, monkeypatch,
):
    storage_a = LocalStorage(tmp_path / "storage-a")
    storage_b = LocalStorage(tmp_path / "storage-b")
    writer = _writer(storage_a)
    calls = []
    original_scope = processing._local_artifact_cache_scope

    def counted_scope(state):
        calls.append(state)
        return original_scope(state)

    monkeypatch.setattr(processing, "_local_artifact_cache_scope", counted_scope)
    path = "org/lake/tables/t/stats/version.parquet"

    first = writer._stats_cache_identity(path)
    assert writer._stats_cache_identity(path) == first
    assert len(calls) == 1

    writer.super_table.organization = "other-org"
    changed_org = writer._stats_cache_identity(path)
    assert changed_org != first
    assert len(calls) == 2

    writer.super_table.storage = storage_b
    changed_storage = writer._stats_cache_identity(path)
    assert changed_storage != changed_org
    assert len(calls) == 3

    storage_b.root = str(tmp_path / "hostile-root-change")
    changed_root = writer._stats_cache_identity(path)
    assert changed_root != changed_storage
    assert len(calls) == 4

    real_pid = processing.os.getpid()
    monkeypatch.setattr(processing.os, "getpid", lambda: real_pid + 1)
    changed_process = writer._stats_cache_identity(path)
    assert changed_process != changed_root
    assert len(calls) == 5


def test_local_storage_object_replacement_revalidates_equivalent_scope(
        tmp_path, monkeypatch,
):
    root = tmp_path / "storage"
    storage_a = LocalStorage(root)
    storage_b = LocalStorage(root)
    identities = processing._WriterArtifactCacheIdentities()
    calls = []
    original_scope = processing._local_artifact_cache_scope

    def counted_scope(state):
        calls.append(state)
        return original_scope(state)

    monkeypatch.setattr(processing, "_local_artifact_cache_scope", counted_scope)
    path = "org/lake/tables/t/stats/version.parquet"
    first = identities.stats(path, organization="org", storage=storage_a)
    second = identities.stats(path, organization="org", storage=storage_b)

    assert first == second
    assert len(calls) == 2
    assert calls[0].storage_id != calls[1].storage_id


def test_remote_auth_is_resampled_through_file_cache_every_time(monkeypatch):
    class RemoteStorage:
        def __init__(self):
            self._aws_access_key_id = "principal-a"
            self.bucket = "shared"

        def cache_namespace(self):
            return {"provider": "test", "bucket": self.bucket}

        @staticmethod
        def is_local_storage():
            return False

    real_file_cache = file_cache_module.FileCache

    def legacy_identity(path, storage):
        namespace = real_file_cache(
            storage, "org", max_bytes=0, workers=1,
        )
        return processing._artifact_cache_identity(
            path,
            prefix=processing._STATS_CACHE_IDENTITY_PREFIX,
            identity_scope=(
                f"{namespace._organization_hash}{namespace._storage_hash}"
            ),
        )

    class CountingFileCache(real_file_cache):
        calls = 0

        def __init__(self, *args, **kwargs):
            type(self).calls += 1
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(file_cache_module, "FileCache", CountingFileCache)
    storage = RemoteStorage()
    identities = processing._WriterArtifactCacheIdentities()
    path = "org/lake/tables/t/stats/version.parquet"

    first = identities.stats(path, organization="org", storage=storage)
    assert first == legacy_identity(path, storage)
    assert identities.stats(path, organization="org", storage=storage) == first
    storage._aws_access_key_id = "principal-b"
    rotated = identities.stats(path, organization="org", storage=storage)
    assert rotated == legacy_identity(path, storage)
    storage.bucket = "other"
    moved = identities.stats(path, organization="org", storage=storage)
    assert moved == legacy_identity(path, storage)

    assert rotated != first
    assert moved != rotated
    assert CountingFileCache.calls == 4


def test_local_subclass_and_broken_adapter_retain_fallback(tmp_path, monkeypatch):
    class LocalSubclass(LocalStorage):
        pass

    real_file_cache = file_cache_module.FileCache

    class CountingFileCache(real_file_cache):
        calls = 0

        def __init__(self, *args, **kwargs):
            type(self).calls += 1
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(file_cache_module, "FileCache", CountingFileCache)
    identities = processing._WriterArtifactCacheIdentities()
    path = "org/lake/tables/t/stats/version.parquet"
    subclass = LocalSubclass(tmp_path / "subclass")

    assert identities.stats(path, organization="org", storage=subclass)
    assert identities.stats(path, organization="org", storage=subclass)
    assert CountingFileCache.calls == 2

    class BrokenAdapter:
        def cache_namespace(self):
            raise RuntimeError("unavailable identity adapter")

    broken = BrokenAdapter()
    first = identities.stats(path, organization="org", storage=broken)
    second = identities.stats(path, organization="org", storage=broken)
    other = identities.stats(path, organization="org", storage=BrokenAdapter())

    assert first == second
    assert other != first


@pytest.mark.parametrize("kind", ["stats", "tombstone"])
def test_pre_scoped_identity_remains_idempotent(tmp_path, kind):
    storage = LocalStorage(tmp_path / "storage")
    identities = processing._WriterArtifactCacheIdentities()
    method = getattr(identities, kind)
    path = f"org/lake/tables/t/{kind}/version.parquet"
    first = method(path, organization="org", storage=storage)

    assert method(first, organization="changed", storage=object()) == first
