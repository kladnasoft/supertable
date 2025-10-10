#!/usr/bin/env python3
import os
import sys
import gc
import time
import random
import argparse
import threading

import supertable.config.homedir  # ensure env/init
from supertable.config.defaults import logger, logging
from supertable.locking import Locking
from supertable.storage.storage_factory import get_storage

logger.setLevel(logging.INFO)

# ---------- Defaults ----------
NUM_THREADS_DEFAULT = 10
HOLD_TIME_DEFAULT = 1.0  # seconds each thread holds the lock once acquired
RES_POOL_SIZE = 50       # resources are named res1..res50
PICKS_PER_THREAD = 5     # each thread picks 5 distinct resources


def run_multithreaded_test(
    label: str,
    num_threads: int = NUM_THREADS_DEFAULT,
    hold_time: float = HOLD_TIME_DEFAULT,
    working_dir: str | None = None,
):
    """
    Run a multi-threaded contention test using the unified Locking façade.
    Works with any configured storage backend (File/MinIO/S3 + Redis locking).
    """
    storage = get_storage()

    # Default to a persistent, backend-agnostic ".locks" directory in storage
    workdir = working_dir or ".locks"
    if not storage.exists(workdir):
        storage.makedirs(workdir)

    barrier = threading.Barrier(num_threads)
    acquisitions: list[dict] = []
    acquisitions_lock = threading.Lock()

    def worker(idx: int) -> None:
        # each thread picks 5 distinct resources from 1–50
        picks = random.sample(range(1, RES_POOL_SIZE + 1), PICKS_PER_THREAD)
        # Use storage-path resources so both File and remote backends behave consistently
        resources = [os.path.join(workdir, f"res{n}") for n in picks]

        name = f"{label}-T{idx}"
        lock = Locking(identity=name, working_dir=workdir)

        print(f"[{name}] attempting lock on {resources}")
        barrier.wait()  # sync start

        t0 = time.perf_counter()
        acquired = lock.lock_resources(resources)
        t1 = time.perf_counter()

        if not acquired:
            print(f"[{name}] FAILED to acquire lock on {resources}")
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

        # hold the lock, then release all
        time.sleep(hold_time)
        lock.release_lock()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Let any finalizers run
    gc.collect()
    time.sleep(0.2)

    # post-process to determine which thread waited for which
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
    print(f"  Working dir       : {workdir} (storage)" )
    print("-" * 40)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Measure multi-threaded lock timing using storage-backed locking."
    )
    ap.add_argument(
        "--threads",
        type=int,
        default=NUM_THREADS_DEFAULT,
        help="Number of threads (default: 10)",
    )
    ap.add_argument(
        "--hold",
        type=float,
        default=HOLD_TIME_DEFAULT,
        help="Seconds each thread holds the lock (default: 1.0)",
    )
    ap.add_argument(
        "--workdir",
        type=str,
        default=".locks",
        help="Logical working directory inside the configured storage (default: .locks)",
    )
    ap.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for reproducibility (default: None)",
    )
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print("==== STORAGE-BASED LOCKING ====")
    run_multithreaded_test(
        "Locking",
        num_threads=args.threads,
        hold_time=args.hold,
        working_dir=args.workdir,
    )


if __name__ == "__main__":
    main()
