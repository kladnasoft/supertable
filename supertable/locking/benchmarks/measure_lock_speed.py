#!/usr/bin/env python3
# route: supertable.locking.benchmarks.measure_lock_speed
"""
Multi-threaded contention benchmark for the Redis distributed lock.

``RedisLocking`` is the only locking backend SuperTable has; there is no
backend selection to benchmark against.

Usage:
    python3 measure_lock_speed.py [--threads 10] [--hold 1.0]
"""

import gc
import time
import random
import argparse
import threading

from supertable.locking.redis_lock import RedisLocking
from supertable.redis_connector import create_redis_client

# ---------- Defaults ----------
NUM_THREADS_DEFAULT = 10
HOLD_TIME_DEFAULT = 1.0
RES_POOL_SIZE = 50
LOCK_TTL_DEFAULT = 30


def _create_locker() -> RedisLocking:
    """Build a locker on the application's own Redis connection path."""
    return RedisLocking(create_redis_client())


def run_multithreaded_test(
    label: str,
    num_threads: int = NUM_THREADS_DEFAULT,
    hold_time: float = HOLD_TIME_DEFAULT,
    lock_ttl: int = LOCK_TTL_DEFAULT,
):
    """
    Run a multi-threaded contention test.

    Each thread picks a random resource key from a pool and attempts to
    acquire it.  Threads that pick the same key will contend; threads
    that pick different keys proceed concurrently.
    """
    barrier = threading.Barrier(num_threads)
    results: list[dict] = []
    results_lock = threading.Lock()

    def worker(idx: int) -> None:
        name = f"{label}-T{idx}"
        key = f"res{random.randint(1, RES_POOL_SIZE)}"
        locker = _create_locker()

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
        description="Multi-threaded Redis lock contention benchmark."
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

    print("==== REDIS LOCK CONTENTION BENCHMARK ====\n")
    run_multithreaded_test(
        label="contention",
        num_threads=args.threads,
        hold_time=args.hold,
        lock_ttl=args.ttl,
    )


if __name__ == "__main__":
    main()
