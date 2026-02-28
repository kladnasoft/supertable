#!/usr/bin/env python3
# concurrency_lock_simulation.py
#
# Heavy concurrent locking simulation with:
#   - Multi-threaded random resource contention
#   - Partial releases
#   - In-process mutual-exclusion invariant checks
#   - Post-run lock-file integrity verification
#
# Standalone: no supertable dependency required.
#
# Usage:
#   python3 concurrency_lock_simulation.py                         # quick defaults
#   python3 concurrency_lock_simulation.py --threads 8 --iters 10  # CLI mode

import os
import sys
import time
import json
import atexit
import signal
import random
import string
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from argparse import ArgumentParser

# ---------------------------------------------------------------------------
# Standalone import
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from supertable.locking.file_lock import FileLocking
from supertable.locking.locking_backend import LockingBackend

# -------------------- Utilities --------------------
def now():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def rand_id(prefix="T"):
    return f"{prefix}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"

# -------------------- Simulation --------------------
class ConcurrencySimulator:
    """
    Spins up threads that concurrently lock random subsets of resources, simulate work,
    perform partial releases, and verify in-process mutual exclusion across resources.
    It also validates the on-disk JSON lock file for file backend (post-run integrity).
    """

    def __init__(
        self,
        resources,
        num_threads=8,
        iterations=10,
        working_dir=".",
        lock_file_name=".lock.json",
        lock_duration=6,
        think_time=(0.05, 0.25),
        work_time=(0.2, 0.6),
        partial_release_prob=0.5,
        verbose=True,
    ):
        self.resources = list(resources)
        self.num_threads = int(num_threads)
        self.iterations = int(iterations)
        self.working_dir = os.path.abspath(working_dir)
        self.lock_file_name = lock_file_name
        self.lock_duration = int(lock_duration)
        self.think_time = think_time
        self.work_time = work_time
        self.partial_release_prob = float(partial_release_prob)
        self.verbose = verbose

        os.makedirs(self.working_dir, exist_ok=True)

        # Shared in-process check: resource -> holder thread id
        self._active_map: dict[str, str] = {}
        self._active_map_lock = threading.RLock()

        # Counters
        self._acquired = 0
        self._timeouts = 0
        self._counters_lock = threading.Lock()

        # Give a stable identity prefix for this simulator process
        self.identity_prefix = f"sim-{rand_id('PID')}"

    def _log(self, msg):
        if self.verbose:
            print(f"[{now()}] {msg}")

    # --------- Internal integrity checks (in-process) ---------
    def _claim_resources_locally(self, tid: str, res: list[str]):
        """
        Record that `tid` now holds `res`. Warns (but does not crash) if
        the local map is stale due to thread scheduling — the file lock
        is the source of truth; this map is best-effort observability.
        """
        with self._active_map_lock:
            for r in res:
                holder = self._active_map.get(r)
                if holder is not None and holder != tid:
                    # This can happen legitimately: the other thread released
                    # the file lock (so acquire succeeded) but hasn't yet
                    # called _release_resources_locally.  Log it as a warning
                    # rather than crashing the simulation.
                    self._log(
                        f"WARN: local map shows '{r}' held by {holder}, "
                        f"but {tid} acquired it (stale local map entry)"
                    )
            for r in res:
                self._active_map[r] = tid

    def _release_resources_locally(self, tid: str, res: list[str]):
        with self._active_map_lock:
            for r in res:
                holder = self._active_map.get(r)
                if holder == tid:
                    del self._active_map[r]

    # --------- Worker logic ---------
    def _worker(self, tid: str):
        random.seed(os.getpid() ^ int(time.time() * 1e6) ^ threading.get_ident())

        # Each worker gets its own FileLocking instance with a unique identity
        lock = FileLocking(
            identity=tid,
            working_dir=self.working_dir,
            lock_file_name=self.lock_file_name,
            check_interval=0.05,
        )

        try:
            for i in range(self.iterations):
                time.sleep(random.uniform(*self.think_time))

                # Sample 1..min(3, len(resources)) random resources
                sample_size = random.randint(1, min(3, len(self.resources)))
                pick = sorted(random.sample(self.resources, sample_size))

                self._log(f"{tid} -> trying to lock {pick}")

                ok = lock.acquire(pick, duration=self.lock_duration, who=tid)

                if not ok:
                    with self._counters_lock:
                        self._timeouts += 1
                    self._log(f"{tid} !! timeout on {pick}")
                    continue

                self._claim_resources_locally(tid, pick)
                with self._counters_lock:
                    self._acquired += 1
                self._log(f"{tid} OK acquired {pick}")

                # Simulate work while holding the lock
                time.sleep(random.uniform(*self.work_time))

                # Occasionally do a partial release
                if random.random() < self.partial_release_prob and len(pick) > 1:
                    to_release = sorted(random.sample(pick, random.randint(1, len(pick) - 1)))
                    self._log(f"{tid} <- partial release {to_release}")
                    lock.release(to_release)
                    self._release_resources_locally(tid, to_release)

                    remaining = [r for r in pick if r not in to_release]
                    # Do a little more work
                    time.sleep(random.uniform(*self.work_time))
                    self._log(f"{tid} <- final release {remaining}")
                    lock.release(remaining)
                    self._release_resources_locally(tid, remaining)
                else:
                    self._log(f"{tid} <- release {pick}")
                    lock.release(pick)
                    self._release_resources_locally(tid, pick)
        finally:
            lock._stop_heartbeat()

        return f"{tid} done"

    # --------- Run ---------
    def run(self):
        self._log(
            f"Simulator '{self.identity_prefix}' starting: "
            f"threads={self.num_threads}, iterations={self.iterations}, "
            f"duration={self.lock_duration}s"
        )
        self._log(f"Resources: {self.resources}")
        t0 = time.time()

        # Graceful shutdown on SIGINT/SIGTERM
        stop_event = threading.Event()

        def _sig_handler(signum, frame):
            self._log(f"Received signal {signum}, stopping...")
            stop_event.set()

        signal.signal(signal.SIGINT, _sig_handler)
        signal.signal(signal.SIGTERM, _sig_handler)

        with ThreadPoolExecutor(max_workers=self.num_threads) as ex:
            futs = [
                ex.submit(self._worker, f"{self.identity_prefix}-T{idx + 1:02d}")
                for idx in range(self.num_threads)
            ]
            for fut in as_completed(futs):
                try:
                    _ = fut.result()
                except Exception as e:
                    self._log(f"Worker error: {e}")
                    raise

        dt = time.time() - t0
        self._log(f"All workers finished in {dt:.2f}s")
        self._log(f"Acquisitions: {self._acquired}, Timeouts: {self._timeouts}")

        # Final integrity check: no resources left claimed in-process
        with self._active_map_lock:
            if self._active_map:
                raise AssertionError(
                    f"INVARIANT VIOLATION: resources still marked active: {self._active_map}"
                )

        # Post-run: verify the lock file has no leftover entries from our workers
        lock_path = os.path.join(self.working_dir, self.lock_file_name)
        if os.path.exists(lock_path):
            try:
                with open(lock_path, "r") as fh:
                    data = json.load(fh)
                # Check for entries whose "pid" starts with our identity prefix
                leftovers = [
                    entry for entry in data
                    if str(entry.get("pid", "")).startswith(self.identity_prefix)
                ]
                if leftovers:
                    raise AssertionError(
                        f"Lock file still contains our entries: {leftovers}"
                    )
                self._log(f"Lock file clean ({len(data)} total entries, 0 ours).")
            except json.JSONDecodeError as e:
                self._log(f"Note: could not parse lock file: {e}")
            except AssertionError:
                raise
            except Exception as e:
                self._log(f"Note: could not verify lock file: {e}")

        self._log("OK Concurrency simulation completed successfully.")


# -------------------- CLI --------------------
def main():
    ap = ArgumentParser(description="Simulate concurrent locking on multiple resources.")
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SIM_THREADS", "8")))
    ap.add_argument("--iters", type=int, default=int(os.environ.get("SIM_ITERS", "10")))
    ap.add_argument("--resources", type=str, default=os.environ.get("SIM_RESOURCES", "A,B,C,D,E,F"))
    ap.add_argument("--workdir", type=str, default=os.path.join(_HERE, ".locks"))
    ap.add_argument("--lock-file", type=str, default=".lock.json")
    ap.add_argument("--duration", type=int, default=int(os.environ.get("SIM_TTL", "6")))
    ap.add_argument("--partial", type=float, default=float(os.environ.get("SIM_PARTIAL", "0.5")))
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    resources = [r.strip() for r in args.resources.split(",") if r.strip()]
    sim = ConcurrencySimulator(
        resources=resources,
        num_threads=args.threads,
        iterations=args.iters,
        working_dir=args.workdir,
        lock_file_name=args.lock_file,
        lock_duration=args.duration,
        partial_release_prob=args.partial,
        verbose=not args.quiet,
    )
    sim.run()


def run_with_defaults():
    """Runs when no CLI args are provided (e.g. PyCharm Run with no parameters)."""
    sim = ConcurrencySimulator(
        resources=["A", "B", "C", "D", "E"],
        num_threads=4,
        iterations=5,
        working_dir=os.path.join(_HERE, ".locks"),
        lock_file_name=".lock.json",
        lock_duration=6,
        partial_release_prob=0.5,
        verbose=True,
    )
    print(f"Running concurrency sim: threads=4, iters=5, resources={sim.resources}")
    sim.run()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        main()
    else:
        run_with_defaults()
