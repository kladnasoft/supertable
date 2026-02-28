#!/usr/bin/env python3
# escalate_lock.py
#
# Demonstrates lock escalation: one thread grabs a table-wide lock while
# workers try (and fail) to lock individual file resources. After the
# table lock is released, workers retry and succeed.
#
# Standalone: no supertable dependency required.
#
# Usage:  python3 escalate_lock.py

import os
import sys
import threading
import time
import tempfile

# ---------------------------------------------------------------------------
# Standalone import
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from supertable.locking.file_lock import FileLocking

# Demo working directory for lock file (.lock.json)
WORKDIR = os.path.join(tempfile.gettempdir(), "supertable_lock_demo")
os.makedirs(WORKDIR, exist_ok=True)

# Shorter numbers to see behavior quickly
LOCK_DURATION_SEC = 5       # how long the table lock is held by escalator
WORKER_DURATION_SEC = 10    # TTL for worker locks
RETRY_AFTER_RELEASE = 3     # when to retry after table lock is released

# NOTE: The actual acquire() has a hardcoded 30s busy-wait timeout.
# Workers will block for up to 30s waiting for the table lock to release.
# We rely on the escalator releasing before that deadline.


def escalator():
    """
    Simulates a writer that decides to escalate to a table-wide lock
    instead of locking many files individually.
    """
    lock = FileLocking(identity="ESCALATOR", working_dir=WORKDIR)
    table_resource = "__table__"

    print("[T0] requesting TABLE lock...")
    got = lock.acquire([table_resource], duration=LOCK_DURATION_SEC, who="escalator")
    print(f"[T0] table lock acquired? {got}")
    if not got:
        lock._stop_heartbeat()
        return
    try:
        print(f"[T0] holding table lock for {LOCK_DURATION_SEC}s...")
        time.sleep(LOCK_DURATION_SEC)
        print("[T0] done with critical section")
    finally:
        lock.release([table_resource])
        lock._stop_heartbeat()
        print("[T0] released table lock")


def worker(name: str, resources: list[str]):
    """
    Tries to lock specific file resources while the table lock may be held.
    The table lock (__table__) is a *different* resource from file_A/file_B,
    so workers won't be blocked by it unless we include __table__ in their
    resource list.

    To demonstrate escalation blocking, workers also request __table__
    alongside their file resources, simulating a convention where all
    writers must check the table lock.
    """
    lock = FileLocking(identity=name, working_dir=WORKDIR)

    # Convention: workers must also acquire __table__ to respect escalation
    all_resources = ["__table__"] + resources

    # First attempt: will block until escalator releases __table__
    print(f"[{name}] requesting {all_resources} (will BLOCK during table lock)...")
    t0 = time.perf_counter()
    got = lock.acquire(all_resources, duration=WORKER_DURATION_SEC, who=name)
    t1 = time.perf_counter()
    print(f"[{name}] acquired? {got} (waited {t1 - t0:.2f}s)")

    if got:
        # Hold briefly, then release
        time.sleep(0.5)
        lock.release(all_resources)
        print(f"[{name}] released (first attempt)")

    # Wait a bit, then try again (should be uncontested now)
    time.sleep(RETRY_AFTER_RELEASE)

    print(f"[{name}] retrying {resources} after table-lock release...")
    got = lock.acquire(resources, duration=5, who=f"{name}-retry")
    print(f"[{name}] acquired on retry? {got}")
    if got:
        time.sleep(0.5)
        lock.release(resources)
        print(f"[{name}] released (second attempt)")

    lock._stop_heartbeat()


def main():
    print(f"Working dir: {WORKDIR}")
    print(f"Table lock duration: {LOCK_DURATION_SEC}s")
    print()

    # Start escalator first so it grabs the table lock before workers try
    t0 = threading.Thread(target=escalator, name="T0")
    t1 = threading.Thread(target=worker, args=("W1", ["file_A", "file_B"]), name="T1")
    t2 = threading.Thread(target=worker, args=("W2", ["file_C"]), name="T2")

    t0.start()
    time.sleep(0.1)  # tiny head start for the table lock
    t1.start()
    t2.start()

    t0.join()
    t1.join()
    t2.join()

    # Verify lock file is clean
    lock_path = os.path.join(WORKDIR, ".lock.json")
    if os.path.exists(lock_path):
        try:
            import json
            with open(lock_path, "r") as f:
                data = json.load(f)
            # Purge expired
            now_ts = int(time.time())
            active = [r for r in data if int(r.get("exp", 0)) > now_ts]
            if active:
                print(f"\nWARNING: {len(active)} lock(s) still active: {active}")
            else:
                print(f"\nLock file clean (all entries expired or released).")
        except Exception as e:
            print(f"\nNote: could not verify lock file: {e}")

    print("\nDemo complete.")


if __name__ == "__main__":
    main()
