import os
import threading
import time
import tempfile
from supertable.locking.file_lock import FileLocking

# Demo working directory for lock file (.lock.json)
WORKDIR = os.path.join(tempfile.gettempdir(), "supertable_lock_demo")
os.makedirs(WORKDIR, exist_ok=True)

# Shorter numbers to see behavior quickly
LOCK_TIMEOUT_SEC = 2        # how long a waiter will try before giving up
LOCK_DURATION_SEC = 5       # how long the table lock is held by escalator
RETRY_AFTER_RELEASE = 3     # when to retry after table lock is released

def escalator():
    """
    Simulates a writer that decides to escalate to a table-wide lock
    instead of locking many files individually.
    """
    lock = FileLocking(identity="ESCALATOR", working_dir=WORKDIR)
    print("[T0] requesting TABLE lock…")
    got = lock.lock_resources(
        resources=["__table__"],
        timeout_seconds=LOCK_TIMEOUT_SEC,
        lock_duration_seconds=LOCK_DURATION_SEC,
    )
    print(f"[T0] table lock acquired? {got}")
    if not got:
        return
    try:
        print(f"[T0] holding table lock for {LOCK_DURATION_SEC}s…")
        time.sleep(LOCK_DURATION_SEC)
        print("[T0] done with critical section")
    finally:
        lock.release_lock()
        print("[T0] released table lock")

def worker(name: str, resources):
    """
    Tries to lock specific file resources while the table lock is held.
    Should fail during escalation, then succeed after release on retry.
    """
    lock = FileLocking(identity=name, working_dir=WORKDIR)

    # First attempt (expected to fail while table lock is held)
    print(f"[{name}] requesting {resources} (expect BLOCK during table lock)…")
    got = lock.lock_resources(
        resources=resources,
        timeout_seconds=LOCK_TIMEOUT_SEC,
        lock_duration_seconds=10,
    )
    print(f"[{name}] acquired? {got}")
    if got:
        # If it somehow got the lock (e.g., escalator not yet started), release it
        lock.release_lock()
        print(f"[{name}] released (first attempt)")

    # Wait for the escalator to finish, then try again
    time.sleep(RETRY_AFTER_RELEASE)
    print(f"[{name}] retrying {resources} after table-lock release…")
    got = lock.lock_resources(
        resources=resources,
        timeout_seconds=LOCK_TIMEOUT_SEC,
        lock_duration_seconds=5,
    )
    print(f"[{name}] acquired on retry? {got}")
    if got:
        # Simulate a tiny critical section
        time.sleep(0.5)
        lock.release_lock()
        print(f"[{name}] released (second attempt)")

def main():
    # Start escalator first so it likely grabs the table lock before workers try
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
    print("\nDemo complete. Check your logs for lock conflict messages as well.")

if __name__ == "__main__":
    main()
