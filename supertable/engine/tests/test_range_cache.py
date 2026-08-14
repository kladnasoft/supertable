from __future__ import annotations

import fcntl
import io
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import supertable.engine.range_cache as range_cache_module
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


def _range_entry_files(tmp_path):
    data = next((tmp_path / "ranges-v1").glob("*/*/*/*/chunks/*/data.bin"))
    return data, data.with_name("manifest.json"), data.with_name("access")


def _range_entry_for(tmp_path, offset: int, length: int):
    name = f"{offset:016x}-{length:016x}"
    data = next(
        (tmp_path / "ranges-v1").glob(f"*/*/*/*/chunks/{name}/data.bin")
    )
    return data, data.with_name("access")


def _refresh_manifest_stat_seal(data_path, manifest_path) -> None:
    """Model a committed inode whose full stat identity legitimately changed."""
    info = data_path.stat()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest.update({
        "data_dev": int(info.st_dev),
        "data_ino": int(info.st_ino),
        "data_mtime_ns": int(info.st_mtime_ns),
        "data_ctime_ns": int(info.st_ctime_ns),
    })
    manifest_path.write_text(
        json.dumps(manifest, sort_keys=True, separators=(",", ":")),
        encoding="utf-8",
    )


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


def test_reader_metric_sinks_are_query_local_under_concurrency(tmp_path):
    payload = b"0123456789abcdef"
    storage = FakeRangeStorage({"raw/object.bin": payload})
    cache = RangeCache(
        storage, "org", root=str(tmp_path), max_bytes=1024 * 1024,
    )
    first = range_cache_module.RangeCacheMetrics()
    second = range_cache_module.RangeCacheMetrics()
    first_lock = threading.Lock()
    second_lock = threading.Lock()

    def sink(target, lock):
        def merge(metrics):
            with lock:
                target.merge(metrics)
        return merge

    metadata = storage.stat_object("raw/object.bin")
    left = cache.open(
        "raw/object.bin", expected=metadata,
        metrics_sink=sink(first, first_lock),
    )
    right = cache.open(
        "raw/object.bin", expected=metadata,
        metrics_sink=sink(second, second_lock),
    )
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            left_result = pool.submit(left.read_at, 4, 0)
            right_result = pool.submit(right.read_at, 4, 8)
            assert left_result.result() == payload[:4]
            assert right_result.result() == payload[8:12]
    finally:
        left.close()
        right.close()

    assert first.logical_requests == 1
    assert first.requested_bytes == 4
    assert first.served_bytes == 4
    assert second.logical_requests == 1
    assert second.requested_bytes == 4
    assert second.served_bytes == 4
    assert cache.metrics().logical_requests == 2


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


def test_warm_hit_skips_hash_only_for_unchanged_committed_stat_identity(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))

    cache.read(key, 500, 1_000)
    actual, warm = cache.read(key, 500, 1_000)

    assert actual == payload[500:1_500]
    assert warm.validated_hits == 1
    assert warm.hash_validation_skips == 1
    assert warm.hash_validations == 0
    assert len(storage.range_calls) == 1


def test_new_cache_instance_rehashes_once_then_memoizes(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    RangeCache(storage, "org", root=str(tmp_path)).read(key, 500, 1_000)

    reopened = RangeCache(storage, "org", root=str(tmp_path))
    first, validated = reopened.read(key, 500, 1_000)
    second, memoized = reopened.read(key, 500, 1_000)

    assert first == second == payload[500:1_500]
    assert validated.validated_hits == 1
    assert validated.hash_validations == 1
    assert validated.hash_validation_skips == 0
    assert memoized.validated_hits == 1
    assert memoized.hash_validations == 0
    assert memoized.hash_validation_skips == 1
    assert len(storage.range_calls) == 1


def test_validation_proof_memo_is_bounded_lru(tmp_path, monkeypatch):
    key = "table/data.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    monkeypatch.setattr(range_cache_module, "_MAX_VALIDATION_PROOFS", 2)
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)

    for offset in (0, 100, 200):
        cache.read(key, offset, 100)
    paths = {
        offset: str(_range_entry_for(tmp_path, offset, 100)[0])
        for offset in (0, 100, 200)
    }
    assert list(cache._validation_proofs) == [paths[100], paths[200]]

    # A validated hit refreshes recency, so the next proof admission drops the
    # actually least-recently-used proof instead of growing the memo.
    cache.read(key, 100, 100)
    cache.read(key, 300, 100)
    assert list(cache._validation_proofs) == [paths[100], str(
        _range_entry_for(tmp_path, 300, 100)[0]
    )]
    assert len(cache._validation_proofs) == 2

    # The evicted proof changes performance only: cached bytes are still exact,
    # and the next hit safely hashes them again without a remote read.
    actual, metrics = cache.read(key, 200, 100)
    assert actual == payload[200:300]
    assert metrics.hash_validations == 1
    assert metrics.hash_validation_skips == 0
    assert len(storage.range_calls) == 4


def test_changed_stat_identity_forces_checksum_revalidation(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    cache.read(key, 500, 1_000)
    data_path, manifest_path, _access_path = _range_entry_files(tmp_path)
    prior = data_path.stat()
    os.utime(
        data_path,
        ns=(prior.st_atime_ns, prior.st_mtime_ns + 2_000_000_000),
    )
    _refresh_manifest_stat_seal(data_path, manifest_path)

    actual, metrics = cache.read(key, 500, 1_000)

    assert actual == payload[500:1_500]
    assert metrics.validated_hits == 1
    assert metrics.hash_validations == 1
    assert metrics.hash_validation_skips == 0
    assert len(storage.range_calls) == 1


def test_changed_stat_cannot_forge_a_warm_checksum_skip(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    cache.read(key, 500, 1_000)
    data_path, manifest_path, _access_path = _range_entry_files(tmp_path)
    with data_path.open("r+b") as target:
        target.seek(0)
        original = target.read(50)
        target.seek(0)
        target.write(bytes(value ^ 0xFF for value in original))
        target.flush()
        os.fsync(target.fileno())
    _refresh_manifest_stat_seal(data_path, manifest_path)

    actual, metrics = cache.read(key, 500, 1_000)

    assert actual == payload[500:1_500]
    assert metrics.corruption_repairs == 1
    assert metrics.hash_validations >= 1
    assert metrics.hash_validation_skips == 0
    assert len(storage.range_calls) == 2


def test_cache_capacity_evicts_lru_range_and_never_serves_partial(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)

    cache.read(key, 0, 600)
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()
    _second, metrics = cache.read(key, 2_000, 600)
    first_again, _ = cache.read(key, 0, 600)

    assert first_again == payload[:600]
    assert metrics.evictions == 1
    assert len(storage.range_calls) == 3


def test_aged_frequency_keeps_frequent_range_over_one_off_scan(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)

    cache.read(key, 0, 100)
    cache.read(key, 100, 100)
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()
    for _ in range(8):
        assert cache.read(key, 0, 100)[0] == payload[:100]
    _new, metrics = cache.read(key, 200, 100)
    hot_data, _ = _range_entry_for(tmp_path, 0, 100)
    cold_name = f"{100:016x}-{100:016x}"

    assert metrics.evictions == 1
    assert hot_data.is_file()
    assert not list(
        (tmp_path / "ranges-v1").glob(
            f"*/*/*/*/chunks/{cold_name}/data.bin"
        )
    )
    assert len(storage.range_calls) == 3


def test_old_frequency_decays_instead_of_making_entry_immortal(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)

    cache.read(key, 0, 100)
    cache.read(key, 100, 100)
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()
    for _ in range(8):
        cache.read(key, 0, 100)
    hot_data, hot_access = _range_entry_for(tmp_path, 0, 100)
    record = cache._read_access_record(str(hot_access))
    assert record is not None and record.frequency > 1
    old_record = type(record)(
        frequency=record.frequency,
        decay_epoch_ns=(
            time.time_ns()
            - range_cache_module._FREQUENCY_HALF_LIFE_NS
            * (range_cache_module._MAX_FREQUENCY.bit_length() + 1)
        ),
        last_access_ns=record.last_access_ns,
        refill_cost_ns=record.refill_cost_ns,
    )
    hot_access.write_bytes(cache._encode_access_record(old_record))

    _new, metrics = cache.read(key, 200, 100)

    assert metrics.evictions == 1
    assert not hot_data.exists()
    assert _range_entry_for(tmp_path, 100, 100)[0].is_file()


def test_capacity_eviction_stops_after_required_bytes_are_freed(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)
    for offset in (0, 100, 200):
        cache.read(key, offset, 100)
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()

    _new, metrics = cache.read(key, 300, 100)
    data_files = list(
        (tmp_path / "ranges-v1").glob("*/*/*/*/chunks/*/data.bin")
    )

    assert metrics.evictions == 1
    assert metrics.evicted_bytes == 100
    assert len(data_files) == 3
    assert sum(path.stat().st_size for path in data_files) == 300


def test_corrupt_access_record_is_bounded_safe_and_cold(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)
    cache.read(key, 0, 100)
    cache.read(key, 100, 100)
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()
    corrupt_data, corrupt_access = _range_entry_for(tmp_path, 0, 100)

    corrupt_access.write_bytes(b"torn-advisory-record")
    actual, warm = cache.read(key, 0, 100)
    repaired = cache._read_access_record(str(corrupt_access))
    assert actual == payload[:100]
    assert warm.cache_hit_chunks == 1
    assert repaired is not None
    assert corrupt_access.stat().st_size == range_cache_module._ACCESS_RECORD_BYTES
    assert len(storage.range_calls) == 2

    # Even with the newest file mtime, corrupt advisory state is ranked cold;
    # it never authorizes bytes and cannot poison the result path.
    corrupt_access.write_bytes(b"bad")
    _new, metrics = cache.read(key, 200, 100)
    assert metrics.evictions == 1
    assert not corrupt_data.exists()
    assert _range_entry_for(tmp_path, 100, 100)[0].is_file()


def test_concurrent_access_updates_remain_valid_and_do_not_lose_counts(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)
    cache.read(key, 0, 100)
    _data, access = _range_entry_for(tmp_path, 0, 100)
    workers = 16
    barrier = threading.Barrier(workers)

    def read_warm(_index):
        barrier.wait()
        return cache.read(key, 0, 100)[0]

    with ThreadPoolExecutor(max_workers=workers) as pool:
        results = list(pool.map(read_warm, range(workers)))
    record = cache._read_access_record(str(access))

    assert results == [payload[:100]] * workers
    assert record is not None
    assert record.frequency == workers + 1
    assert access.stat().st_size == range_cache_module._ACCESS_RECORD_BYTES
    assert len(storage.range_calls) == 1


def test_default_policy_keeps_idle_immutable_ranges_until_capacity_pressure(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    cache.read(key, 500, 1_000)
    _data_path, _manifest_path, access_path = _range_entry_files(tmp_path)
    old = time.time() - 7 * 24 * 60 * 60
    os.utime(access_path, (old, old))

    pruned = cache.prune()
    actual, warm = cache.read(key, 500, 1_000)

    assert pruned.evictions == 0
    assert actual == payload[500:1_500]
    assert warm.cache_hit_chunks == 1
    assert len(storage.range_calls) == 1


def test_explicit_positive_ttl_still_expires_idle_range(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(10_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), ttl=1)
    cache.read(key, 500, 1_000)
    data_path, _manifest_path, access_path = _range_entry_files(tmp_path)
    old = time.time() - 60
    os.utime(access_path, (old, old))

    pruned = cache.prune()

    assert pruned.evictions == 1
    assert pruned.evicted_bytes == 1_000
    assert not data_path.exists()
    assert cache.read(key, 500, 1_000)[0] == payload[500:1_500]
    assert len(storage.range_calls) == 2


def test_corrupt_future_dated_access_record_cannot_escape_explicit_ttl(tmp_path):
    key = "table/data.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), ttl=60)
    cache.read(key, 0, 100)
    data, access = _range_entry_for(tmp_path, 0, 100)
    access.write_bytes(b"invalid")
    future = time.time() + 365 * 24 * 60 * 60
    os.utime(access, (future, future))

    metrics = cache.prune()

    assert metrics.evictions == 1
    assert metrics.evicted_bytes == 100
    assert not data.exists()


def test_evicted_broad_interval_is_not_refetched_for_later_narrow_read(tmp_path):
    first_key = "table/large.parquet"
    second_key = "table/other.parquet"
    first = os.urandom(5_000)
    second = os.urandom(100)
    storage = FakeRangeStorage({first_key: first, second_key: second})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)

    cache.read(first_key, 0, len(first))
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()
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
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1024 * 1024)
    cache.read(key, 0, 500)
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()

    cache.prefetch(key, [(0, 500), (2_000, 500)], workers=2)

    data_files = list((tmp_path / "ranges-v1").glob("*/*/*/*/chunks/*/data.bin"))
    assert cache._scan_cache()[1] <= cache.max_bytes


def test_tiny_ranges_account_metadata_and_stay_under_measured_cap(tmp_path):
    key = "table/tiny-ranges.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(
        storage, "org", root=str(tmp_path), max_bytes=1024 * 1024,
    )

    cache.read(key, 0, 1)
    initial_total = cache._scan_cache()[1]
    data_bytes = sum(
        path.stat().st_size
        for path in (tmp_path / "ranges-v1").glob(
            "*/*/*/*/chunks/*/data.bin"
        )
    )
    assert data_bytes == 1
    assert initial_total > data_bytes

    # Leave exactly one allocation unit for the transient reservation file.
    # Repeated one-byte admissions must rotate complete chunk footprints rather
    # than allowing manifest/access/lock amplification beyond the cap.
    cache.max_bytes = initial_total + cache._allocation_unit()
    for offset in range(1, 40):
        actual, _metrics = cache.read(key, offset, 1)
        assert actual == payload[offset:offset + 1]
        assert cache._scan_cache()[1] <= cache.max_bytes


def test_metadata_too_large_for_cap_bypasses_without_admitting_data(tmp_path):
    key = "table/no-room-for-metadata.bin"
    payload = os.urandom(100)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path), max_bytes=1)

    actual, metrics = cache.read(key, 0, 1)

    assert actual == payload[:1]
    assert metrics.fills == 0
    assert metrics.bypass_requests == 1
    assert not os.path.exists(cache.cache_root)
    assert not list(
        (tmp_path / "ranges-v1").glob("*/*/*/*/chunks/*/data.bin")
    )


def test_warm_hit_does_not_create_unreserved_access_metadata(tmp_path):
    key = "table/missing-access.bin"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    cache.read(key, 0, 100)
    _data, access = _range_entry_for(tmp_path, 0, 100)
    access.unlink()
    before = cache._scan_cache()[1]
    cache.max_bytes = before

    actual, metrics = cache.read(key, 0, 100)

    assert actual == payload[:100]
    assert metrics.cache_hit_chunks == 1
    assert not access.exists()
    assert cache._scan_cache()[1] == before


def test_prune_reclaims_partials_and_both_torn_commit_directions(tmp_path):
    key = "table/orphans.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))

    cache.read(key, 0, 100)
    data, manifest, _access = _range_entry_files(tmp_path)
    directory = data.parent
    partial = directory / ".range-crashed.part"
    partial.write_bytes(b"uncommitted" * 100)
    cache.prune()
    assert data.is_file()
    assert manifest.is_file()
    assert not partial.exists()

    # Data without its atomic commit marker is never a usable entry.
    manifest.unlink()
    cache.prune()
    assert not directory.exists()

    cache.read(key, 0, 100)
    data, manifest, _access = _range_entry_files(tmp_path)
    directory = data.parent
    data.unlink()
    cache.prune()
    assert not directory.exists()
    assert not manifest.exists()


def test_prune_reclaims_malformed_published_reservation(tmp_path):
    storage = FakeRangeStorage({"table/data.bin": b"abc"})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    reservation_dir = os.path.join(cache.cache_root, ".reservations")
    cache._ensure_private_chain(reservation_dir)
    reservation = os.path.join(reservation_dir, "torn.json")
    with open(reservation, "wb") as target:
        target.write(b'{"future_bytes":')

    cache.prune()

    assert not os.path.exists(reservation)


def test_prune_never_reclaims_an_active_locked_orphan(tmp_path):
    key = "table/active-orphan.parquet"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(storage, "org", root=str(tmp_path))
    cache.read(key, 0, 100)
    data, manifest, _access = _range_entry_files(tmp_path)
    directory = data.parent
    lock_path = directory / "entry.lock"
    manifest.unlink()

    fd = cache._open_lock(str(lock_path), create=False)
    fcntl.flock(fd, fcntl.LOCK_EX)
    try:
        cache.prune()
        assert directory.is_dir()
        assert data.is_file()
        assert lock_path.is_file()
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)

    cache.prune()
    assert not directory.exists()


def test_interval_catalog_bounds_objects_and_intervals_lru(tmp_path, monkeypatch):
    objects = {f"table/{index}.bin": os.urandom(100) for index in range(4)}
    storage = FakeRangeStorage(objects)
    monkeypatch.setattr(range_cache_module, "_MAX_INTERVAL_CATALOG_OBJECTS", 2)
    monkeypatch.setattr(range_cache_module, "_MAX_INTERVAL_CATALOG_INTERVALS", 3)
    cache = RangeCache(storage, "org", root=str(tmp_path))

    for key in ("table/0.bin", "table/1.bin"):
        cache.read(key, 0, 10)
    first_hash = cache._object_paths(
        "table/0.bin", storage.stat_object("table/0.bin")
    ).identity_hash
    second_hash = cache._object_paths(
        "table/1.bin", storage.stat_object("table/1.bin")
    ).identity_hash
    assert list(cache._interval_catalog) == [first_hash, second_hash]

    # A hit refreshes object recency; admitting a third identity evicts the
    # actual least-recently-used identity from the process memo only.
    cache.read("table/0.bin", 0, 10)
    cache.read("table/2.bin", 0, 10)
    third_hash = cache._object_paths(
        "table/2.bin", storage.stat_object("table/2.bin")
    ).identity_hash
    assert list(cache._interval_catalog) == [first_hash, third_hash]

    for offset in (10, 20, 30, 40):
        cache.read("table/2.bin", offset, 10)
        assert len(cache._interval_catalog) <= 2
        assert cache._interval_catalog_count <= 3


def test_capacity_eviction_removes_chunk_directory_lock_and_catalog(tmp_path):
    key = "table/empty-directory-cleanup.bin"
    payload = os.urandom(1_000)
    storage = FakeRangeStorage({key: payload})
    cache = RangeCache(
        storage, "org", root=str(tmp_path), max_bytes=1024 * 1024,
    )
    cache.read(key, 0, 100)
    first_data, _first_access = _range_entry_for(tmp_path, 0, 100)
    first_directory = str(first_data.parent)
    cache.max_bytes = cache._scan_cache()[1] + cache._allocation_unit()

    cache.read(key, 200, 100)

    assert not os.path.lexists(first_directory)
    assert all(
        interval.paths.directory != first_directory
        for intervals in cache._interval_catalog.values()
        for interval in intervals
    )
