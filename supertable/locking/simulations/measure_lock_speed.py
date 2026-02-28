#!/usr/bin/env python3
# measure_lock_speed.py
#
# Multi-threaded contention benchmark for file-based locking.
# Standalone: no supertable dependency required.
#
# Usage:  python3 measure_lock_speed.py [--threads 10] [--hold 1.0] [--seed 42]

import os
import sys
import gc
import time
import random
import argparse
import threading

# ---------------------------------------------------------------------------
# Standalone import
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from supertable.locking.file_lock import FileLocking

# ---------- Defaults ----------
NUM_THREADS_DEFAULT = 10
HOLD_TIME_DEFAULT = 1.0      # seconds each thread holds the lock once acquired
RES_POOL_SIZE = 50            # resources are named res1..res50
PICKS_PER_THREAD = 5          # each thread picks 5 distinct resources
LOCK_DURATION_DEFAULT = 30    # TTL per lock in seconds


def run_multithreaded_test(
    label: str,
    num_threads: int = NUM_THREADS_DEFAULT,
    hold_time: float = HOLD_TIME_DEFAULT,
    working_dir: str | None = None,
    lock_duration: int = LOCK_DURATION_DEFAULT,
):
    """
    Run a multi-threaded contention test using FileLocking directly.
    Each thread picks 5 random resources from a pool of 50 and attempts
    to acquire them all atomically.
    """
    workdir = working_dir or os.path.join(_HERE, ".locks")
    os.makedirs(workdir, exist_ok=True)

    barrier = threading.Barrier(num_threads)
    acquisitions: list[dict] = []
    acquisitions_lock = threading.Lock()

    def worker(idx: int) -> None:
        # Each thread picks 5 distinct resources from 1–50
        picks = random.sample(range(1, RES_POOL_SIZE + 1), PICKS_PER_THREAD)
        resources = [f"res{n}" for n in picks]

        name = f"{label}-T{idx}"
        lock = FileLocking(identity=name, working_dir=workdir, check_interval=0.02)

        print(f"[{name}] attempting lock on {resources}")
        barrier.wait()  # sync start

        t0 = time.perf_counter()
        acquired = lock.acquire(resources, duration=lock_duration, who=name)
        t1 = time.perf_counter()

        if not acquired:
            print(f"[{name}] FAILED to acquire lock on {resources}")
            lock._stop_heartbeat()
            return

        wait_time = t1 - t0
        print(f"[{name}] acquired lock after waiting {wait_time:.4f}s")

        with acquisitions_lock:
            acquisitions.append(
                {
                    "name": name,
                    "resources": set(resources),
                    "start": t0,
                    "acquired": t1,
                    "wait": wait_time,
                }
            )

        # Hold the lock, then release all explicitly
        time.sleep(hold_time)
        lock.release(resources)
        lock._stop_heartbeat()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Let any finalizers run
    gc.collect()
    time.sleep(0.2)

    # Post-process to determine which thread waited for which
    print(f"\n{label} DETAILED WAIT ANALYSIS:")
    for rec in sorted(acquisitions, key=lambda x: x["acquired"]):
        deps = [
            other["name"]
            for other in acquisitions
            if other["acquired"] < rec["acquired"]
            and other["resources"].intersection(rec["resources"])
        ]
        dep_list = ", ".join(deps) if deps else "none"
        print(
            f"- {rec['name']} waited {rec['wait']:.4f}s; blocked by: {dep_list}"
        )

    waits = [r["wait"] for r in acquisitions]
    avg = sum(waits) / len(waits) if waits else 0.0
    mn = min(waits) if waits else 0.0
    mx = max(waits) if waits else 0.0

    print(f"\n{label} SUMMARY:")
    print(f"  Threads attempted : {num_threads}")
    print(f"  Successful locks  : {len(waits)}")
    print(f"  Avg wait          : {avg:.4f}s")
    print(f"  Min wait          : {mn:.4f}s")
    print(f"  Max wait          : {mx:.4f}s")
    print(f"  Working dir       : {workdir}")
    print("-" * 40)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Measure multi-threaded lock timing using file-based locking."
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
        help="Working directory for lock files (default: .locks)",
    )
    ap.add_argument(
        "--duration", type=int, default=LOCK_DURATION_DEFAULT,
        help=f"Lock TTL in seconds (default: {LOCK_DURATION_DEFAULT})",
    )
    ap.add_argument(
        "--seed", type=int, default=None,
        help="Random seed for reproducibility (default: None)",
    )
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print("==== FILE-BASED LOCKING SPEED TEST ====")
    run_multithreaded_test(
        "Locking",
        num_threads=args.threads,
        hold_time=args.hold,
        working_dir=args.workdir,
        lock_duration=args.duration,
    )


if __name__ == "__main__":
    main()
