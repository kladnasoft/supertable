#!/usr/bin/env python3
# measure_lock_time.py
#
# Measures acquisition latency for file-based locking.
# Standalone: no supertable dependency required.
#
# Usage:  python3 measure_lock_time.py [--workdir /tmp/locks]

import os
import sys
import time
import argparse

# ---------------------------------------------------------------------------
# Standalone import: use colocated file_lock.py / locking_backend.py
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from supertable.locking.file_lock import FileLocking

# ---------------------------------------------------------------------- CLI
def parse_args():
    ap = argparse.ArgumentParser(description="Measure file-lock acquisition latency.")
    ap.add_argument("--workdir", type=str, default=os.path.join(_HERE, ".locks"),
                    help="Directory for the lock file (default: .locks)")
    ap.add_argument("--duration", type=int, default=5,
                    help="Lock TTL in seconds (default: 5)")
    ap.add_argument("--iterations", type=int, default=10,
                    help="Number of acquire/release cycles to average (default: 10)")
    return ap.parse_args()


# ---------------------------------------------------------------------- logic
def measure_read_with_lock(lock: FileLocking, file_path: str, duration: int) -> float:
    """
    Measure time to acquire a lock on a file path, read it, and release.
    Mirrors the Locking.read_with_lock() convenience helper.
    """
    start = time.perf_counter()
    acquired = lock.acquire([file_path], duration=duration, who="read_with_lock")
    if acquired:
        try:
            try:
                with open(file_path, "rb") as f:
                    _ = f.read()
            except FileNotFoundError:
                pass
        finally:
            lock.release([file_path])
    end = time.perf_counter()
    return end - start


def measure_exclusive_lock(lock: FileLocking, resource: str, duration: int) -> float:
    """
    Measure time to acquire and immediately release an exclusive lock.
    """
    start = time.perf_counter()
    acquired = lock.acquire([resource], duration=duration, who="measure_exclusive")
    if acquired:
        lock.release([resource])
    end = time.perf_counter()
    return end - start


# ---------------------------------------------------------------------- run
def main():
    args = parse_args()

    workdir = os.path.abspath(args.workdir)
    os.makedirs(workdir, exist_ok=True)

    lock_file = ".lock.json"
    lock_path = os.path.join(workdir, lock_file)

    # Ensure lock file exists
    if not os.path.exists(lock_path):
        with open(lock_path, "w") as f:
            f.write("[]")

    # Create a small data file to read
    data_file = os.path.join(workdir, "sample_data.json")
    if not os.path.exists(data_file):
        with open(data_file, "w") as f:
            f.write('{"status": "ok"}')

    lock = FileLocking(
        identity="measure-locks",
        working_dir=workdir,
        lock_file_name=lock_file,
        check_interval=0.01,
    )

    iterations = args.iterations
    duration = args.duration

    print(f"Measuring lock latency ({iterations} iterations, duration={duration}s TTL)")
    print(f"Working dir: {workdir}")
    print()

    # ---- Read-with-lock timing ----
    read_times = []
    for i in range(iterations):
        t = measure_read_with_lock(lock, data_file, duration)
        read_times.append(t)

    avg_read = sum(read_times) / len(read_times)
    min_read = min(read_times)
    max_read = max(read_times)

    print(f"Read-with-lock (acquire + read + release):")
    print(f"  Avg : {avg_read:.6f}s")
    print(f"  Min : {min_read:.6f}s")
    print(f"  Max : {max_read:.6f}s")
    print()

    # ---- Exclusive lock timing ----
    excl_times = []
    for i in range(iterations):
        t = measure_exclusive_lock(lock, "exclusive_resource", duration)
        excl_times.append(t)

    avg_excl = sum(excl_times) / len(excl_times)
    min_excl = min(excl_times)
    max_excl = max(excl_times)

    print(f"Exclusive lock (acquire + release):")
    print(f"  Avg : {avg_excl:.6f}s")
    print(f"  Min : {min_excl:.6f}s")
    print(f"  Max : {max_excl:.6f}s")

    # Cleanup
    lock._stop_heartbeat()
    print("\nDone.")


if __name__ == "__main__":
    main()
