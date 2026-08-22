from __future__ import annotations

import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from supertable.data_classes import Reflection, SuperSnapshot, TombstoneDef
from supertable.engine.file_cache import (
    FileCache,
    FileCacheIntegrityError,
)
from supertable.storage.local_storage import LocalStorage
from supertable.storage.storage_interface import (
    ObjectMetadata,
    StorageInterface,
    write_all,
)
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V3,
    tombstone_v3_artifact_digest,
)


def _parquet_bytes(seed: int = 1, rows: int = 8) -> bytes:
    sink = io.BytesIO()
    pq.write_table(
        pa.table({"id": list(range(rows)), "value": [f"{seed}-{i}" for i in range(rows)]}),
        sink,
        row_group_size=2,
    )
    return sink.getvalue()


def _reflection(
    resolved: str,
    raw_key: str = "org/table/data.parquet",
    *,
    tombstone: TombstoneDef | None = None,
) -> Reflection:
    return Reflection(
        storage_type="FakeRemote",
        reflection_bytes=123,
        total_reflections=1,
        supers=[SuperSnapshot(
            super_name="lake",
            simple_name="events",
            simple_version=7,
            files=[resolved],
            resource_keys=[raw_key],
            columns={"id", "value"},
        )],
        tombstone_views={"events": tombstone} if tombstone else {},
    )


def _v3_only_reflection(
    resolved: str,
    raw_key: str,
    digest: str,
) -> Reflection:
    return Reflection(
        storage_type="FakeRemote",
        reflection_bytes=0,
        total_reflections=0,
        supers=[],
        tombstone_views={
            "events": TombstoneDef(
                tombstone_path=resolved,
                cache_key=raw_key,
                expected_rows=1,
                tombstone_digest=digest,
                tombstone_format=TOMBSTONE_FORMAT_V3,
            ),
        },
    )


class FakeRemote:
    def __init__(self, objects: dict[str, bytes], *, route: str = "route-a"):
        self.objects = dict(objects)
        self.versions = {key: "v1" for key in objects}
        self.route = route
        self.downloads = 0
        self.read_bytes_calls = 0
        self.max_write = 0
        self._lock = threading.Lock()

    def cache_namespace(self):
        return {"provider": "fake", "route": self.route}

    def is_local_storage(self):
        return False

    def stat_object(self, key: str) -> ObjectMetadata:
        data = self.objects[key]
        return ObjectMetadata(
            size=len(data),
            version=self.versions[key],
            etag=f"etag-{self.versions[key]}",
        )

    def download_to_file(
        self, key, file_obj, *, expected=None, chunk_size=8 * 1024 * 1024,
    ):
        with self._lock:
            self.downloads += 1
        data = self.objects[key]
        current = self.stat_object(key)
        if expected is not None and current != expected:
            raise OSError("changed")
        total = 0
        for offset in range(0, len(data), max(1, min(chunk_size, 97))):
            chunk = data[offset:offset + max(1, min(chunk_size, 97))]
            self.max_write = max(self.max_write, len(chunk))
            total += write_all(file_obj, chunk)
        return total

    def read_bytes(self, key):
        self.read_bytes_calls += 1
        raise AssertionError("file cache must never call read_bytes")

    def replace(self, key: str, data: bytes) -> None:
        version = int(self.versions[key][1:]) + 1
        self.objects[key] = data
        self.versions[key] = f"v{version}"


class StatSpyRemote(FakeRemote):
    def __init__(self, objects: dict[str, bytes], *, route: str = "route-a"):
        super().__init__(objects, route=route)
        self.stat_calls = 0

    def stat_object(self, key: str) -> ObjectMetadata:
        self.stat_calls += 1
        return super().stat_object(key)


def test_populate_then_rotated_presign_hits_same_raw_object(tmp_path):
    key = "org/table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "acme", root=str(tmp_path), workers=1)

    original = _reflection("https://signed.example/data?token=one", key)
    localized, cold = cache.localize_reflection(original, populate=True)

    assert original.supers[0].files[0].endswith("token=one")
    local_path = localized.supers[0].files[0]
    assert os.path.isfile(local_path)
    assert pq.read_table(local_path).num_rows == 8
    assert cold.downloads == 1
    assert cold.downloaded_bytes == len(storage.objects[key])
    assert cold.coverage_ratio == 1.0
    assert storage.read_bytes_calls == 0
    assert storage.max_write <= 97

    rotated = _reflection("https://signed.example/data?token=two", key)
    warm, warm_metrics = cache.localize_reflection(rotated, populate=False)
    assert warm.supers[0].files[0] == local_path
    assert warm_metrics.hits == 1
    assert warm_metrics.downloads == 0
    assert storage.downloads == 1


def test_v3_snapshot_digest_rejects_download_before_footer_parse(
    tmp_path, monkeypatch,
):
    key = "org/lake/tables/events/tombstone/dv-v3.parquet"
    payload = _parquet_bytes()
    storage = FakeRemote({key: payload})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    reflection = _v3_only_reflection(
        "https://signed.example/dv-v3", key, "0" * 64,
    )

    monkeypatch.setattr(
        pq,
        "read_metadata",
        lambda *_args, **_kwargs: pytest.fail(
            "snapshot-mismatched v3 bytes reached the Parquet parser"
        ),
    )
    with pytest.raises(
        FileCacheIntegrityError, match="snapshot SHA-256",
    ):
        cache.localize_reflection(reflection, populate=True)


def test_v3_snapshot_digest_rejects_warm_hit_before_footer_parse(
    tmp_path, monkeypatch,
):
    key = "org/lake/tables/events/tombstone/dv-v3.parquet"
    payload = _parquet_bytes()
    storage = FakeRemote({key: payload})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    correct = _v3_only_reflection(
        "https://signed.example/dv-v3",
        key,
        tombstone_v3_artifact_digest(payload),
    )
    localized, metrics = cache.localize_reflection(correct, populate=True)
    assert metrics.downloads == 1
    assert os.path.isfile(
        localized.tombstone_views["events"].tombstone_path
    )

    mismatched = _v3_only_reflection(
        "https://signed.example/dv-v3", key, "f" * 64,
    )
    monkeypatch.setattr(
        pq,
        "read_metadata",
        lambda *_args, **_kwargs: pytest.fail(
            "snapshot-mismatched cached v3 bytes reached the Parquet parser"
        ),
    )
    with pytest.raises(
        FileCacheIntegrityError, match="snapshot SHA-256",
    ):
        cache.localize_reflection(mismatched, populate=False)


def test_local_storage_is_no_copy_and_reflection_is_cloned(tmp_path):
    source = tmp_path / "source.parquet"
    source.write_bytes(_parquet_bytes())
    original = _reflection(str(source), str(source))
    cache_root = tmp_path / "cache"
    cache = FileCache(LocalStorage(), "acme", root=str(cache_root))

    localized, metrics = cache.localize_reflection(original, populate=True)

    assert localized is not original
    assert localized.supers[0] is not original.supers[0]
    assert localized.supers[0].files == [str(source.resolve())]
    assert original.supers[0].files == [str(source)]
    assert metrics.local_no_copy == 1
    assert metrics.downloads == 0
    assert not (cache_root / "objects-v1").exists()


def test_organization_and_storage_route_are_hard_namespaces(tmp_path):
    key = "same/key.parquet"
    payload = _parquet_bytes()
    one = FakeRemote({key: payload}, route="one")
    two = FakeRemote({key: payload}, route="two")

    path_a = FileCache(one, "org-a", root=str(tmp_path), workers=1).localize_reflection(
        _reflection("remote-a", key), True,
    )[0].supers[0].files[0]
    path_b = FileCache(one, "org-b", root=str(tmp_path), workers=1).localize_reflection(
        _reflection("remote-b", key), True,
    )[0].supers[0].files[0]
    path_c = FileCache(two, "org-a", root=str(tmp_path), workers=1).localize_reflection(
        _reflection("remote-c", key), True,
    )[0].supers[0].files[0]

    assert len({path_a, path_b, path_c}) == 3
    assert one.downloads == 2
    assert two.downloads == 1


def test_storage_authorization_context_is_a_hard_namespace(tmp_path):
    key = "same/key.parquet"
    payload = _parquet_bytes()
    first = FakeRemote({key: payload}, route="same-route")
    first._access_key = "principal-a"
    first._secret_key = "secret-a"
    second = FakeRemote({key: payload}, route="same-route")
    second._access_key = "principal-b"
    second._secret_key = "secret-b"

    path_a = FileCache(
        first, "same-org", root=str(tmp_path), workers=1,
    ).localize_reflection(_reflection("remote-a", key), True)[0].supers[0].files[0]
    path_b = FileCache(
        second, "same-org", root=str(tmp_path), workers=1,
    ).localize_reflection(_reflection("remote-b", key), True)[0].supers[0].files[0]

    assert path_a != path_b
    assert first.downloads == second.downloads == 1


def test_object_version_rotation_creates_a_new_entry(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes(seed=1)})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    first, _ = cache.localize_reflection(_reflection("remote-v1", key), True)
    first_path = first.supers[0].files[0]

    storage.replace(key, _parquet_bytes(seed=2, rows=9))
    second, metrics = cache.localize_reflection(_reflection("remote-v2", key), True)

    assert second.supers[0].files[0] != first_path
    assert metrics.downloads == 1
    assert storage.downloads == 2
    assert pq.read_table(second.supers[0].files[0]).num_rows == 9


def test_snapshot_declared_size_mismatch_fails_closed(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    reflection = _reflection("remote", key)
    reflection.supers[0].resource_sizes = [len(storage.objects[key]) - 1]
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    with pytest.raises(FileCacheIntegrityError, match="snapshot-declared"):
        cache.localize_reflection(reflection, populate=True)

    assert storage.downloads == 0


def test_invalid_snapshot_declared_size_is_ineligible_for_localization(tmp_path):
    key = "table/data.parquet"
    storage = StatSpyRemote({key: _parquet_bytes()})
    reflection = _reflection("https://signed/remote", key)
    reflection.supers[0].resource_sizes = ["not-an-integer"]
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    localized, metrics = cache.localize_reflection(reflection, populate=True)

    assert localized.supers[0].files == ["https://signed/remote"]
    assert metrics.fallback_files == metrics.bypasses == 1
    assert storage.stat_calls == 0
    assert storage.downloads == 0


def test_remote_all_object_preflight_requires_positive_declared_sizes(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    reflection = _reflection("remote", key)
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    assert cache.can_populate_all(reflection) is False
    reflection.supers[0].resource_sizes = [len(storage.objects[key])]
    assert cache.can_populate_all(reflection) is True


def test_populate_false_is_hit_only_and_coverage_is_non_populating(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    original = _reflection("remote", key)

    clone, miss = cache.localize_reflection(original, populate=False)
    assert clone is original
    assert clone.supers[0].files == ["remote"]
    assert miss.misses == miss.bypasses == miss.fallback_files == 1
    assert storage.downloads == 0
    assert not (tmp_path / "objects-v1").exists()

    coverage = cache.coverage(original)
    assert coverage.coverage_ratio == 0.0
    assert storage.downloads == 0

    cache.localize_reflection(original, populate=True)
    coverage = cache.coverage(original)
    assert coverage.hits == 1
    assert coverage.coverage_ratio == 1.0


def test_hit_only_cold_key_skips_remote_stat(tmp_path):
    key = "table/data.parquet"
    storage = StatSpyRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    original = _reflection("remote", key)

    clone, miss = cache.localize_reflection(original, populate=False)
    coverage = cache.coverage(original)

    assert clone.supers[0].files == ["remote"]
    assert miss.misses == miss.bypasses == miss.fallback_files == 1
    assert coverage.misses == coverage.bypasses == 1
    assert storage.stat_calls == 0

    cache.localize_reflection(original, populate=True)
    warm_calls = storage.stat_calls
    _clone, warm = cache.localize_reflection(original, populate=False)
    assert warm.hits == 1
    assert storage.stat_calls == warm_calls + 1


class ShortRemote(FakeRemote):
    def download_to_file(self, key, file_obj, *, expected=None, chunk_size=1024):
        self.downloads += 1
        data = self.objects[key][:-8]
        return write_all(file_obj, data)


class OversizedRemote(FakeRemote):
    def stat_object(self, key: str) -> ObjectMetadata:
        actual = super().stat_object(key)
        return ObjectMetadata(size=actual.size - 1, version=actual.version, etag=actual.etag)


def test_short_download_is_integrity_failure_and_never_published(tmp_path):
    key = "table/data.parquet"
    storage = ShortRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    with pytest.raises(FileCacheIntegrityError, match="size mismatch"):
        cache.localize_reflection(_reflection("remote", key), populate=True)

    assert not list((tmp_path / "objects-v1").glob("*/*/*/*/manifest.json"))
    assert not list((tmp_path / "objects-v1").glob("*/*/*/*/data.parquet"))


def test_stream_sink_aborts_before_exceeding_sealed_size(tmp_path):
    key = "table/data.parquet"
    storage = OversizedRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    with pytest.raises(FileCacheIntegrityError, match="exceeded"):
        cache.localize_reflection(_reflection("remote", key), populate=True)

    partials = list((tmp_path / "objects-v1").glob("*/*/*/*/.download-*.part"))
    assert partials == []


def test_invalid_parquet_fails_closed(tmp_path):
    key = "table/not-parquet.parquet"
    storage = FakeRemote({key: b"this is not parquet"})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    with pytest.raises(FileCacheIntegrityError, match="Parquet footer"):
        cache.localize_reflection(_reflection("remote", key), populate=True)


def test_committed_cache_corruption_fails_closed_even_hit_only(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    localized, _ = cache.localize_reflection(_reflection("remote", key), True)
    local_path = localized.supers[0].files[0]
    size = os.path.getsize(local_path)
    with open(local_path, "r+b") as target:
        target.seek(size - 4)
        target.write(b"FAIL")

    with pytest.raises(FileCacheIntegrityError, match="seal mismatch|corrupt"):
        cache.localize_reflection(_reflection("remote", key), populate=False)


def test_duckdb_hit_only_mode_quarantines_corrupt_hit_and_uses_remote(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    localized, _ = cache.localize_reflection(_reflection("remote", key), True)
    with open(localized.supers[0].files[0], "r+b") as target:
        target.seek(-4, os.SEEK_END)
        target.write(b"FAIL")

    with cache.localized(
        _reflection("https://signed/remote", key),
        populate=False,
        tolerate_corrupt_hits=True,
    ) as (fallback, metrics):
        assert fallback.supers[0].files == ["https://signed/remote"]
        assert metrics.integrity_failures == 1
        assert metrics.fallback_files == 1

    assert not list((tmp_path / "objects-v1").glob("*/*/*/*/manifest.json"))


def test_duckdb_corrupt_hit_tolerance_does_not_hide_source_size_mismatch(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    reflection = _reflection("https://signed/remote", key)
    cache.localize_reflection(reflection, populate=True)
    reflection.supers[0].resource_sizes = [len(storage.objects[key]) - 1]

    with pytest.raises(FileCacheIntegrityError, match="snapshot-declared"):
        with cache.localized(
            reflection,
            populate=False,
            tolerate_corrupt_hits=True,
        ):
            pass

    assert list((tmp_path / "objects-v1").glob("*/*/*/*/manifest.json"))


def test_same_size_valid_parquet_cache_mutation_fails_stat_seal(tmp_path):
    key = "table/data.parquet"
    original_payload = _parquet_bytes(seed=1)
    replacement_payload = _parquet_bytes(seed=2)
    assert len(replacement_payload) == len(original_payload)
    storage = FakeRemote({key: original_payload})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    localized, _ = cache.localize_reflection(_reflection("remote", key), True)
    local_path = localized.supers[0].files[0]

    with open(local_path, "r+b") as target:
        target.write(replacement_payload)
        target.flush()
        os.fsync(target.fileno())
    assert pq.read_table(local_path).num_rows == 8

    with pytest.raises(FileCacheIntegrityError, match="seal mismatch"):
        cache.localize_reflection(_reflection("remote", key), populate=False)


class MutatingDuringDownloadRemote(FakeRemote):
    def download_to_file(self, key, file_obj, *, expected=None, chunk_size=1024):
        written = super().download_to_file(
            key, file_obj, expected=expected, chunk_size=chunk_size,
        )
        self.versions[key] = "v2"
        return written


def test_remote_version_change_during_download_fails_closed(tmp_path):
    key = "table/data.parquet"
    storage = MutatingDuringDownloadRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    with pytest.raises(FileCacheIntegrityError, match="version changed"):
        cache.localize_reflection(_reflection("remote", key), populate=True)

    assert not list((tmp_path / "objects-v1").glob("*/*/*/*/manifest.json"))


class TemporarilyUnavailableRemote(FakeRemote):
    def download_to_file(self, key, file_obj, *, expected=None, chunk_size=1024):
        self.downloads += 1
        raise ConnectionError("temporary network failure")


def test_transient_download_failure_fails_open_to_original_path(tmp_path):
    key = "table/data.parquet"
    storage = TemporarilyUnavailableRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    clone, metrics = cache.localize_reflection(
        _reflection("https://signed/remote", key), populate=True,
    )

    assert clone.supers[0].files == ["https://signed/remote"]
    assert metrics.errors == metrics.bypasses == metrics.fallback_files == 1
    assert not list((tmp_path / "objects-v1").glob("*/*/*/*/manifest.json"))


class LegacyWholeObjectStorage(LocalStorage):
    """Compatibility backend with no explicit streaming downloader."""

    download_to_file = StorageInterface.download_to_file

    def __init__(self, payload: bytes):
        self.payload = payload
        self.read_bytes_calls = 0

    def is_local_storage(self):
        return False

    def cache_namespace(self):
        return {"provider": "legacy-whole-object"}

    def stat_object(self, path):
        return ObjectMetadata(size=len(self.payload), version="v1")

    def read_bytes(self, path):
        self.read_bytes_calls += 1
        return self.payload


def test_cache_never_uses_inherited_whole_object_downloader(tmp_path):
    key = "table/data.parquet"
    storage = LegacyWholeObjectStorage(_parquet_bytes())
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)

    clone, metrics = cache.localize_reflection(_reflection("remote", key), True)

    assert clone.supers[0].files == ["remote"]
    assert metrics.bypasses == metrics.fallback_files == 1
    assert storage.read_bytes_calls == 0


def test_concurrent_cold_requests_singleflight_on_entry_lock(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes(rows=100)})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    reflection = _reflection("remote", key)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(
            lambda _: cache.localize_reflection(reflection, True)[0].supers[0].files[0],
            range(4),
        ))

    assert len(set(results)) == 1
    assert storage.downloads == 1


def test_byte_cap_evicts_lru_entry(tmp_path):
    first_key = "table/first.parquet"
    second_key = "table/second.parquet"
    first = _parquet_bytes(seed=1, rows=30)
    second = _parquet_bytes(seed=2, rows=30)
    storage = FakeRemote({first_key: first, second_key: second})
    cap = max(len(first), len(second)) + 32
    cache = FileCache(
        storage, "org", root=str(tmp_path), max_bytes=cap, workers=1,
    )

    cache.localize_reflection(_reflection("remote-1", first_key), True)
    _localized, metrics = cache.localize_reflection(
        _reflection("remote-2", second_key), True,
    )

    assert metrics.evictions == 1
    assert metrics.evicted_bytes == len(first)
    assert cache.coverage(_reflection("remote-1", first_key)).coverage_ratio == 0.0
    assert cache.coverage(_reflection("remote-2", second_key)).coverage_ratio == 1.0


def test_evicted_key_returns_to_no_stat_hit_only_fast_path(tmp_path):
    first_key = "table/first.parquet"
    second_key = "table/second.parquet"
    first = _parquet_bytes(seed=1, rows=30)
    second = _parquet_bytes(seed=2, rows=30)
    storage = StatSpyRemote({first_key: first, second_key: second})
    cache = FileCache(
        storage,
        "org",
        root=str(tmp_path),
        max_bytes=max(len(first), len(second)) + 32,
        workers=1,
    )
    cache.localize_reflection(_reflection("remote-1", first_key), True)
    cache.localize_reflection(_reflection("remote-2", second_key), True)
    calls = storage.stat_calls

    coverage = cache.coverage(_reflection("remote-1", first_key))

    assert coverage.coverage_ratio == 0.0
    assert storage.stat_calls == calls


def test_crash_orphan_is_reclaimed_before_next_version_fill(tmp_path):
    key = "table/data.parquet"
    first = _parquet_bytes(seed=1)
    second = _parquet_bytes(seed=2)
    storage = FakeRemote({key: first})
    cache = FileCache(
        storage, "org", root=str(tmp_path), max_bytes=len(first) + 32, workers=1,
    )
    localized, _ = cache.localize_reflection(_reflection("remote-v1", key), True)
    orphan = localized.supers[0].files[0]
    os.unlink(os.path.join(os.path.dirname(orphan), "manifest.json"))
    storage.replace(key, second)

    current, metrics = cache.localize_reflection(_reflection("remote-v2", key), True)

    assert os.path.isfile(current.supers[0].files[0])
    assert current.supers[0].files[0] != orphan
    assert not os.path.exists(orphan)
    assert metrics.evictions >= 1
    assert len(list((tmp_path / "objects-v1").glob("*/*/*/*/data.parquet"))) == 1


def test_active_localized_context_prevents_eviction(tmp_path):
    first_key = "table/first.parquet"
    second_key = "table/second.parquet"
    first = _parquet_bytes(seed=1, rows=30)
    second = _parquet_bytes(seed=2, rows=30)
    storage = FakeRemote({first_key: first, second_key: second})
    cache = FileCache(
        storage,
        "org",
        root=str(tmp_path),
        max_bytes=max(len(first), len(second)) + 32,
        workers=1,
    )

    with cache.localized(_reflection("remote-1", first_key), True) as (held, _):
        assert os.path.isfile(held.supers[0].files[0])
        blocked, metrics = cache.localize_reflection(
            _reflection("remote-2", second_key), True,
        )
        assert blocked.supers[0].files == ["remote-2"]
        assert metrics.bypasses == 1
        assert os.path.isfile(held.supers[0].files[0])

    admitted, metrics = cache.localize_reflection(
        _reflection("remote-2", second_key), True,
    )
    assert os.path.isfile(admitted.supers[0].files[0])
    assert metrics.downloads == 1


def test_idle_ttl_prune_removes_unleased_entry(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), ttl=0.01, workers=1)
    localized, _ = cache.localize_reflection(_reflection("remote", key), True)
    path = localized.supers[0].files[0]
    access = os.path.join(os.path.dirname(path), "access")
    old = time.time() - 60
    os.utime(access, (old, old))

    metrics = cache.prune()
    assert metrics.evictions == 1
    assert not os.path.exists(path)


def test_mismatched_resource_keys_never_reverse_parse_url(tmp_path):
    storage = FakeRemote({"unused": _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    reflection = _reflection("https://signed/path?secret=yes", "unused")
    reflection.supers[0].resource_keys = []

    clone, metrics = cache.localize_reflection(reflection, True)

    assert clone.supers[0].files == reflection.supers[0].files
    assert metrics.requested_files == 1
    assert metrics.bypasses == metrics.fallback_files == 1
    assert storage.downloads == 0


def test_tombstone_path_localizes_from_cache_key_without_url_parsing(tmp_path):
    data_key = "table/data.parquet"
    dv_key = "table/dv.parquet"
    storage = FakeRemote({data_key: _parquet_bytes(), dv_key: _parquet_bytes(seed=9)})
    tombstone = TombstoneDef(
        tombstone_path="https://signed/dv?token=secret",
        cache_key=dv_key,
    )
    cache = FileCache(storage, "org", root=str(tmp_path), workers=2)

    clone, metrics = cache.localize_reflection(
        _reflection("https://signed/data?token=secret", data_key, tombstone=tombstone),
        True,
    )

    assert metrics.requested_files == 2
    assert metrics.localized_files == 2
    assert os.path.isfile(clone.tombstone_views["events"].tombstone_path)
    assert tombstone.tombstone_path.startswith("https://")


def test_metrics_have_stable_plan_stats_keys(tmp_path):
    key = "table/data.parquet"
    storage = FakeRemote({key: _parquet_bytes()})
    cache = FileCache(storage, "org", root=str(tmp_path), workers=1)
    _clone, metrics = cache.localize_reflection(_reflection("remote", key), True)

    plan = metrics.to_plan_stats()
    assert plan["FILE_CACHE_REQUESTED_FILES"] == 1
    assert plan["FILE_CACHE_DOWNLOADS"] == 1
    assert plan["FILE_CACHE_COVERAGE_RATIO"] == 1.0
    assert cache.metrics().downloads == 1
