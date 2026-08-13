from __future__ import annotations

import io
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from supertable.engine.range_cache import (
    RangeCache,
    RangeCacheIntegrityError,
)
from supertable.storage.storage_interface import (
    ObjectIdentityMismatch,
    ObjectMetadata,
)


class FakeRangeStorage:
    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = dict(objects)
        self.versions = {key: "v1" for key in objects}
        self.range_calls: list[tuple[str, int, int, str]] = []
        self.delay = 0.0
        self._lock = threading.Lock()

    def cache_namespace(self):
        return {"provider": "fake-range", "endpoint": "isolated"}

    def is_local_storage(self):
        return False

    def stat_object(self, key: str) -> ObjectMetadata:
        value = self.objects[key]
        version = self.versions[key]
        return ObjectMetadata(
            size=len(value), version=version, etag=f"etag-{version}",
        )

    def read_range(self, key, offset, length, *, expected=None):
        with self._lock:
            current = self.stat_object(key)
            if expected is not None and current != expected:
                raise ObjectIdentityMismatch("conditional version mismatch")
            self.range_calls.append((key, offset, length, current.version))
        if self.delay:
            time.sleep(self.delay)
        with self._lock:
            current_after = self.stat_object(key)
            if expected is not None and current_after != expected:
                raise ObjectIdentityMismatch("object changed during range read")
            return self.objects[key][offset:offset + length]

    def replace(self, key: str, value: bytes) -> None:
        with self._lock:
            version = int(self.versions[key][1:]) + 1
            self.objects[key] = value
            self.versions[key] = f"v{version}"


def _parquet_bytes(rows: int = 20_000) -> bytes:
    sink = io.BytesIO()
    pq.write_table(
        pa.table({
            "id": pa.array(range(rows), type=pa.int64()),
            "payload": [f"large-payload-{i:08d}-" * 4 for i in range(rows)],
        }),
        sink,
        row_group_size=1_000,
        compression="snappy",
    )
    return sink.getvalue()


def test_narrow_range_never_downloads_whole_object(tmp_path):
    key = "table/data.parquet"
    payload = bytes(range(256)) * 4
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))

    actual, metrics = cache.read(key, 901, 17)

    assert actual == payload[901:918]
    assert storage.range_calls == [(key, 901, 17, "v1")]
    assert metrics.remote_bytes == 17
    assert metrics.remote_bytes < len(payload)


def test_overlapping_ranges_reuse_cached_prefix_and_fetch_only_gap(tmp_path):
    key = "table/data.parquet"
    payload = bytes(range(251)) * 8
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))

    first, _ = cache.read(key, 100, 100)
    second, metrics = cache.read(key, 150, 100)

    assert first == payload[100:200]
    assert second == payload[150:250]
    assert [(offset, length) for _, offset, length, _ in storage.range_calls] == [
        (100, 100),
        (200, 50),
    ]
    assert metrics.cache_hit_bytes == 50
    assert metrics.remote_bytes == 50


def test_seekable_reader_is_accepted_by_pyarrow_and_projection_is_partial(tmp_path):
    key = "table/data.parquet"
    payload = _parquet_bytes()
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    reader = cache.open(key)

    result = pq.read_table(reader, columns=["id"])

    assert result.num_rows == 20_000
    assert result.column_names == ["id"]
    assert sum(length for _, _, length, _ in storage.range_calls) < len(payload)
    assert all(length < len(payload) for _, _, length, _ in storage.range_calls)


def test_identical_concurrent_miss_is_singleflight(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(32_000)
    storage = FakeRangeStorage({key: payload})
    storage.delay = 0.08
    cache = RangeCache(storage, "org", root=str(tmp_path))
    barrier = threading.Barrier(8)

    def read_one():
        barrier.wait()
        return cache.read(key, 1_000, 4_000)[0]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _index: read_one(), range(8)))

    assert results == [payload[1_000:5_000]] * 8
    assert len(storage.range_calls) == 1


def test_stale_seal_never_reads_replacement_object(tmp_path):
    key = "table/data.parquet"
    before = b"before-" * 1_000
    after = b"after!!" * 1_000
    storage = FakeRangeStorage({key: before})
    stale = storage.stat_object(key)
    storage.replace(key, after)
    cache = RangeCache(storage, "org", root=str(tmp_path))

    with pytest.raises(RangeCacheIntegrityError, match="identity"):
        cache.read(key, 100, 50, expected=stale)

    assert storage.range_calls == []


def test_new_version_uses_distinct_ranges_without_mixing(tmp_path):
    key = "table/data.parquet"
    before = b"a" * 5_000
    after = b"b" * 5_000
    storage = FakeRangeStorage({key: before})
    cache = RangeCache(storage, "org", root=str(tmp_path))

    assert cache.read(key, 10, 100)[0] == b"a" * 100
    storage.replace(key, after)
    assert cache.read(key, 10, 100)[0] == b"b" * 100

    assert [version for *_, version in storage.range_calls] == ["v1", "v2"]


def test_corrupt_cached_range_is_never_returned_and_is_refilled(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    cache.read(key, 500, 1_000)
    data_path = next((tmp_path / "ranges-v1").glob("*/*/*/*/chunks/*/data.bin"))
    with data_path.open("r+b") as target:
        target.seek(0)
        target.write(b"X" * 50)
        target.flush()
        os.fsync(target.fileno())

    actual, metrics = cache.read(key, 500, 1_000)

    assert actual == payload[500:1_500]
    assert len(storage.range_calls) == 2
    assert metrics.corruption_repairs == 1


def test_cache_capacity_evicts_lru_range_and_never_serves_partial(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1_000)

    cache.read(key, 0, 600)
    _second, metrics = cache.read(key, 2_000, 600)
    first_again, _ = cache.read(key, 0, 600)

    assert first_again == payload[:600]
    assert metrics.evictions == 1
    assert len(storage.range_calls) == 3


def test_evicted_broad_interval_is_not_refetched_for_later_narrow_read(tmp_path):
    first_key = "table/large.parquet"
    second_key = "table/other.parquet"
    first = os.urandom(5_000)
    second = os.urandom(100)
    storage = FakeRangeStorage({first_key: first, second_key: second})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=5_000)

    cache.read(first_key, 0, len(first))
    cache.read(second_key, 0, len(second))  # evicts the broad first interval
    actual, _ = cache.read(first_key, 123, 17)

    assert actual == first[123:140]
    assert storage.range_calls[-1][1:3] == (123, 17)


def test_cache_filesystem_failure_bypasses_to_same_exact_range(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(4_000)
    storage = FakeRangeStorage({key: payload})
    invalid_root = tmp_path / "not-a-directory"
    invalid_root.write_bytes(b"occupied")
    cache = RangeCache(storage, "org", root=str(invalid_root))

    actual, metrics = cache.read(key, 123, 77)

    assert actual == payload[123:200]
    assert storage.range_calls == [(key, 123, 77, "v1")]
    assert metrics.bypass_bytes == 77
    assert metrics.errors == 1


def test_zero_length_range_does_no_provider_io(tmp_path):
    key = "table/data.parquet"
    storage = FakeRangeStorage({key: b"abc"})
    cache = RangeCache(storage, "org", root=str(tmp_path))

    assert cache.read(key, 3, 0)[0] == b""
    assert storage.range_calls == []


def test_prefetch_merges_overlap_runs_disjoint_ranges_in_parallel_and_warms(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(50_000)
    storage = FakeRangeStorage({key: payload})
    storage.delay = 0.04
    cache = RangeCache(storage, "org", root=str(tmp_path))

    metrics = cache.prefetch(
        key,
        [(1_000, 1_000), (1_500, 1_000), (10_000, 500), (20_000, 500)],
        workers=3,
    )

    assert sorted((offset, length) for _, offset, length, _ in storage.range_calls) == [
        (1_000, 1_500),
        (10_000, 500),
        (20_000, 500),
    ]
    assert metrics.remote_bytes == 2_500
    storage.range_calls.clear()
    assert cache.read(key, 1_250, 1_000)[0] == payload[1_250:2_250]
    assert cache.read(key, 10_100, 200)[0] == payload[10_100:10_300]
    assert storage.range_calls == []


def test_prefetch_reservation_never_exceeds_hard_cache_cap(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(5_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1_000)
    cache.read(key, 0, 500)

    cache.prefetch(key, [(0, 500), (2_000, 500)], workers=2)

    data_files = list((tmp_path / "ranges-v1").glob("*/*/*/*/chunks/*/data.bin"))
    assert sum(path.stat().st_size for path in data_files) <= 1_000
