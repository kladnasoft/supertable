#!/usr/bin/env python3
# route: supertable.tests.measure_lock_time
"""
Single-threaded acquisition latency benchmark for distributed locking.

Measures acquire + release round-trip time with no contention,
giving the baseline cost of the locking mechanism itself.

Supports both file-based and Redis-based backends.

Usage:
    python3 measure_lock_time.py --backend file  [--iterations 100]
    python3 measure_lock_time.py --backend redis [--iterations 100]
"""

import os
import sys
import time
import argparse

# ---------------------------------------------------------------------------
# Standalone import — stub supertable.config.defaults.logger if missing
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

try:
    from supertable.config.defaults import logger
except ImportError:
    import logging
    logger = logging.getLogger("measure_lock_time")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.WARNING)
    import types
    _st = types.ModuleType("supertable")
    _cfg = types.ModuleType("supertable.config")
    _dfl = types.ModuleType("supertable.config.defaults")
    _dfl.logger = logger
    sys.modules["supertable"] = _st
    sys.modules["supertable.config"] = _cfg
    sys.modules["supertable.config.defaults"] = _dfl

from supertable.locking.file_lock import FileLocking


def _create_locker(backend: str, workdir: str):
    """Factory: return the appropriate locker instance."""
    if backend == "file":
        return FileLocking(working_dir=workdir)
    elif backend == "redis":
        try:
            from supertable.locking.redis_lock import RedisLocking
            from supertable.redis_connector import create_redis_client
            r = create_redis_client()
            return RedisLocking(r)
        except ImportError:
            import redis
            from supertable.locking.redis_lock import RedisLocking
            r = redis.Redis(host="localhost", port=6379, db=0)
            r.ping()
            return RedisLocking(r)
    else:
        raise ValueError(f"Unknown backend: {backend}")


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
    ap = argparse.ArgumentParser(description="Lock acquisition latency benchmark.")
    ap.add_argument("--backend", choices=["file", "redis"], default="file",
                    help="Locking backend (default: file)")
    ap.add_argument("--workdir", type=str, default=os.path.join(_HERE, ".locks"),
                    help="Working directory for file lock (default: .locks)")
    ap.add_argument("--ttl", type=int, default=5,
                    help="Lock TTL in seconds (default: 5)")
    ap.add_argument("--iterations", type=int, default=100,
                    help="Number of acquire/release cycles (default: 100)")
    args = ap.parse_args()

    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)
    iterations = args.iterations
    ttl = args.ttl

    # Create a small data file for the read-with-lock test
    data_file = os.path.join(workdir, "sample_data.json")
    if not os.path.exists(data_file):
        with open(data_file, "w") as f:
            f.write('{"status": "ok"}')

    locker = _create_locker(args.backend, workdir)

    print(f"==== LOCK LATENCY BENCHMARK ({args.backend.upper()}) ====")
    print(f"  Iterations : {iterations}")
    print(f"  TTL        : {ttl}s")
    print(f"  Backend    : {args.backend}")
    if args.backend == "file":
        print(f"  Working dir: {workdir}")
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

    print(f"\nDone.")


if __name__ == "__main__":
    main()
