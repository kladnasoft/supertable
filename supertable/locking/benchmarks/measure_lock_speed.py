#!/usr/bin/env python3
# route: supertable.tests.measure_lock_speed
"""
Multi-threaded contention benchmark for distributed locking.

Supports both file-based and Redis-based backends.
Standalone: works without the full supertable stack (uses stub logger).

Usage:
    python3 measure_lock_speed.py --backend file  [--threads 10] [--hold 1.0]
    python3 measure_lock_speed.py --backend redis [--threads 10] [--hold 1.0]
"""

import os
import sys
import gc
import time
import random
import argparse
import threading

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
    logger = logging.getLogger("measure_lock_speed")
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)
    # Patch into module path so file_lock / redis_lock can import
    import types
    _st = types.ModuleType("supertable")
    _cfg = types.ModuleType("supertable.config")
    _dfl = types.ModuleType("supertable.config.defaults")
    _dfl.logger = logger
    sys.modules["supertable"] = _st
    sys.modules["supertable.config"] = _cfg
    sys.modules["supertable.config.defaults"] = _dfl

from supertable.locking.file_lock import FileLocking

# ---------- Defaults ----------
NUM_THREADS_DEFAULT = 10
HOLD_TIME_DEFAULT = 1.0
RES_POOL_SIZE = 50
LOCK_TTL_DEFAULT = 30


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
            # Fallback: try bare redis on localhost
            import redis
            from supertable.locking.redis_lock import RedisLocking
            r = redis.Redis(host="localhost", port=6379, db=0)
            r.ping()
            return RedisLocking(r)
    else:
        raise ValueError(f"Unknown backend: {backend}")


def run_multithreaded_test(
    label: str,
    backend: str = "file",
    num_threads: int = NUM_THREADS_DEFAULT,
    hold_time: float = HOLD_TIME_DEFAULT,
    working_dir: str | None = None,
    lock_ttl: int = LOCK_TTL_DEFAULT,
):
    """
    Run a multi-threaded contention test.

    Each thread picks a random resource key from a pool and attempts to
    acquire it.  Threads that pick the same key will contend; threads
    that pick different keys proceed concurrently.
    """
    workdir = working_dir or os.path.join(_HERE, ".locks")
    os.makedirs(workdir, exist_ok=True)

    barrier = threading.Barrier(num_threads)
    results: list[dict] = []
    results_lock = threading.Lock()

    def worker(idx: int) -> None:
        name = f"{label}-T{idx}"
        key = f"res{random.randint(1, RES_POOL_SIZE)}"
        locker = _create_locker(backend, workdir)

        print(f"  [{name}] targeting {key}")
        barrier.wait()

        t0 = time.perf_counter()
        token = locker.acquire(key, ttl_s=lock_ttl, timeout_s=30)
        t1 = time.perf_counter()

        if token is None:
            print(f"  [{name}] FAILED to acquire {key}")
            return

        wait_time = t1 - t0
        print(f"  [{name}] acquired {key} after {wait_time:.4f}s")

        with results_lock:
            results.append({
                "name": name,
                "key": key,
                "wait": wait_time,
                "acquired_at": t1,
            })

        time.sleep(hold_time)
        locker.release(key, token)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    gc.collect()
    time.sleep(0.2)

    # --- Analysis ---
    print(f"\n{label} CONTENTION ANALYSIS:")
    for rec in sorted(results, key=lambda x: x["acquired_at"]):
        contenders = [
            o["name"] for o in results
            if o["key"] == rec["key"]
            and o["acquired_at"] < rec["acquired_at"]
        ]
        blocked_by = ", ".join(contenders) if contenders else "none"
        print(f"  {rec['name']:>15}  key={rec['key']:<6}  wait={rec['wait']:.4f}s  blocked_by: {blocked_by}")

    waits = [r["wait"] for r in results]
    if waits:
        avg = sum(waits) / len(waits)
        print(f"\n{label} SUMMARY:")
        print(f"  Backend           : {backend}")
        print(f"  Threads attempted : {num_threads}")
        print(f"  Successful locks  : {len(waits)}")
        print(f"  Avg wait          : {avg:.4f}s")
        print(f"  Min wait          : {min(waits):.4f}s")
        print(f"  Max wait          : {max(waits):.4f}s")
    else:
        print(f"\n{label}: No locks acquired!")
    print("-" * 50)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Multi-threaded lock contention benchmark."
    )
    ap.add_argument(
        "--backend", choices=["file", "redis"], default="file",
        help="Locking backend (default: file)",
    )
    ap.add_argument(
        "--threads", type=int, default=NUM_THREADS_DEFAULT,
        help=f"Number of threads (default: {NUM_THREADS_DEFAULT})",
    )
    ap.add_argument(
        "--hold", type=float, default=HOLD_TIME_DEFAULT,
        help=f"Seconds each thread holds the lock (default: {HOLD_TIME_DEFAULT})",
    )
    ap.add_argument(
        "--workdir", type=str, default=os.path.join(_HERE, ".locks"),
        help="Working directory for file lock (default: .locks)",
    )
    ap.add_argument(
        "--ttl", type=int, default=LOCK_TTL_DEFAULT,
        help=f"Lock TTL in seconds (default: {LOCK_TTL_DEFAULT})",
    )
    ap.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility",
    )
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print(f"==== LOCK CONTENTION BENCHMARK ({args.backend.upper()}) ====\n")
    run_multithreaded_test(
        label="contention",
        backend=args.backend,
        num_threads=args.threads,
        hold_time=args.hold,
        working_dir=args.workdir,
        lock_ttl=args.ttl,
    )


if __name__ == "__main__":
    main()
