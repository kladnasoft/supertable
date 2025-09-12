#!/usr/bin/env python3
import os
import sys
import gc
import time
import shutil
import random
import argparse
import tempfile
import threading
from supertable.config.defaults import logger, logging
logger.setLevel(logging.DEBUG)


# ---- Import your locking implementation ----
try:
    from supertable.locking.file_lock import FileLocking as OriginalLocking
except Exception as e:
    print("[FATAL] Could not import supertable.locking.file_lock.FileLocking")
    raise

# ---------- Defaults ----------
NUM_THREADS_DEFAULT = 10
HOLD_TIME_DEFAULT   = 1.0   # seconds each thread holds the lock once acquired
RES_POOL_SIZE       = 50    # resources are named res1..res50
PICKS_PER_THREAD    = 5     # each thread picks 5 distinct resources


def run_multithreaded_test(
    lock_cls,
    label,
    num_threads=NUM_THREADS_DEFAULT,
    hold_time=HOLD_TIME_DEFAULT,
    working_dir=None,
    use_tmpdir=False,
    keep_dir=False,
):
    """
    Run a multi-threaded contention test against lock_cls (FileLocking).
    - If use_tmpdir=True: uses a unique temp dir (deleted unless keep_dir=True).
    - Else: uses a persistent project-local '.locks' dir (created if needed).
    """

    if use_tmpdir:
        tmpdir = tempfile.mkdtemp(prefix="locktest-")
        workdir = tmpdir
    else:
        workdir = os.path.abspath("./.locks")
        os.makedirs(workdir, exist_ok=True)
        tmpdir = None  # not used

    barrier = threading.Barrier(num_threads)
    acquisitions = []       # dicts with thread info (for analysis)
    acquisitions_lock = threading.Lock()

    def worker(idx):
        # each thread picks 5 distinct resources from 1–50
        picks = random.sample(range(1, RES_POOL_SIZE + 1), PICKS_PER_THREAD)
        resources = [f"res{n}" for n in picks]

        name = f"{label}-T{idx}"
        lock = lock_cls(identity=name, working_dir=workdir)

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

        # record acquisition info
        with acquisitions_lock:
            acquisitions.append({
                "name": name,
                "resources": set(resources),
                "start": t0,
                "acquired": t1,
                "wait": wait_time
            })

        # hold the lock
        time.sleep(hold_time)
        lock.release_lock()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Give destructors/atexit a chance to run *before* removing temp dir
    gc.collect()
    time.sleep(0.2)

    # cleanup
    if use_tmpdir and tmpdir and not keep_dir:
        shutil.rmtree(tmpdir, ignore_errors=True)

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
        print(f"- {rec['name']} waited {rec['wait']:.4f}s; "
              f"blocked by: {dep_list}")

    # summary
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
    if use_tmpdir:
        print(f"  Working dir       : {tmpdir} (temp)")
        print(f"  Kept dir?         : {'yes' if keep_dir else 'no'}")
    else:
        print(f"  Working dir       : {workdir} (persistent)")
    print("-" * 40)


def main():
    ap = argparse.ArgumentParser(description="Measure FileLocking multi-threaded lock timing.")
    ap.add_argument("--threads", type=int, default=NUM_THREADS_DEFAULT, help="Number of threads (default: 10)")
    ap.add_argument("--hold", type=float, default=HOLD_TIME_DEFAULT, help="Seconds each thread holds the lock (default: 1.0)")
    ap.add_argument("--use-tmpdir", action="store_true", help="Use a temporary working directory for the lock file")
    ap.add_argument("--keep-dir", action="store_true", help="When using --use-tmpdir, keep the directory after the run")
    ap.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility (default: None)")
    args = ap.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    print("==== LOCKING Implementation ====")
    run_multithreaded_test(
        OriginalLocking,
        "Locking",
        num_threads=args.threads,
        hold_time=args.hold,
        working_dir=os.path.abspath("./.locks"),  # <- force shared dir
        use_tmpdir=False,  # <- keep persistent dir
        keep_dir=False,
    )


if __name__ == "__main__":
    # No args needed — defaults give you a clean run with persistent ./.locks
    # Example overrides:
    #   --threads 16 --hold 0.5
    #   --use-tmpdir            (uses /tmp, removed safely)
    #   --use-tmpdir --keep-dir (inspect lock file after run)
    main()
