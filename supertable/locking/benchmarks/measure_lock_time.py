#!/usr/bin/env python3
# route: supertable.locking.benchmarks.measure_lock_time
"""
Single-threaded acquisition latency benchmark for the Redis distributed lock.

Measures acquire + release round-trip time with no contention,
giving the baseline cost of the locking mechanism itself.

``RedisLocking`` is the only locking backend SuperTable has; there is no
backend selection to benchmark against.

Usage:
    python3 measure_lock_time.py [--iterations 100]
"""

import os
import argparse
import tempfile
import time

from supertable.locking.redis_lock import RedisLocking
from supertable.redis_connector import create_redis_client


def _create_locker() -> RedisLocking:
    """Build a locker on the application's own Redis connection path."""
    return RedisLocking(create_redis_client())


def measure_acquire_release(locker, key: str, ttl_s: int) -> float:
    """Measure a single acquire + release round-trip (no contention)."""
    t0 = time.perf_counter()
    token = locker.acquire(key, ttl_s=ttl_s, timeout_s=5)
    if token:
        locker.release(key, token)
    t1 = time.perf_counter()
    return t1 - t0


def measure_acquire_read_release(locker, key: str, ttl_s: int, file_path: str) -> float:
    """Measure acquire + file read + release round-trip."""
    t0 = time.perf_counter()
    token = locker.acquire(key, ttl_s=ttl_s, timeout_s=5)
    if token:
        try:
            with open(file_path, "rb") as f:
                _ = f.read()
        except FileNotFoundError:
            pass
        finally:
            locker.release(key, token)
    t1 = time.perf_counter()
    return t1 - t0


def _print_stats(label: str, times: list[float]) -> None:
    avg = sum(times) / len(times)
    mn = min(times)
    mx = max(times)
    # p50 / p99
    s = sorted(times)
    p50 = s[len(s) // 2]
    p99 = s[int(len(s) * 0.99)]
    print(f"  {label}:")
    print(f"    Avg : {avg * 1000:.3f} ms")
    print(f"    Min : {mn * 1000:.3f} ms")
    print(f"    p50 : {p50 * 1000:.3f} ms")
    print(f"    p99 : {p99 * 1000:.3f} ms")
    print(f"    Max : {mx * 1000:.3f} ms")


def main():
    ap = argparse.ArgumentParser(
        description="Redis lock acquisition latency benchmark."
    )
    ap.add_argument("--datadir", type=str, default=None,
                    help="Scratch directory for the sample payload read under "
                         "the lock (default: a fresh temporary directory)")
    ap.add_argument("--ttl", type=int, default=5,
                    help="Lock TTL in seconds (default: 5)")
    ap.add_argument("--iterations", type=int, default=100,
                    help="Number of acquire/release cycles (default: 100)")
    args = ap.parse_args()

    datadir = os.path.abspath(args.datadir) if args.datadir else tempfile.mkdtemp(
        prefix="supertable-lock-bench-"
    )
    os.makedirs(datadir, exist_ok=True)
    iterations = args.iterations
    ttl = args.ttl

    # Create a small data file for the read-with-lock test
    data_file = os.path.join(datadir, "sample_data.json")
    if not os.path.exists(data_file):
        with open(data_file, "w") as f:
            f.write('{"status": "ok"}')

    locker = _create_locker()

    print("==== REDIS LOCK LATENCY BENCHMARK ====")
    print(f"  Iterations : {iterations}")
    print(f"  TTL        : {ttl}s")
    print(f"  Data dir   : {datadir}")
    print()

    # ---- Warm-up (3 cycles, discarded) ----
    for _ in range(3):
        measure_acquire_release(locker, "warmup", ttl)

    # ---- Acquire + release (no I/O) ----
    excl_times = []
    for _ in range(iterations):
        t = measure_acquire_release(locker, "bench_exclusive", ttl)
        excl_times.append(t)
    _print_stats("Acquire + release (no I/O)", excl_times)
    print()

    # ---- Acquire + read + release ----
    read_times = []
    for _ in range(iterations):
        t = measure_acquire_read_release(locker, "bench_read", ttl, data_file)
        read_times.append(t)
    _print_stats("Acquire + read + release", read_times)

    print("\nDone.")


if __name__ == "__main__":
    main()
