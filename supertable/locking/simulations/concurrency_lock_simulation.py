#!/usr/bin/env python3
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

from supertable.config.defaults import logger, logging
logger.setLevel(logging.DEBUG)

# -------------------- Imports --------------------
def _try_import_locking():
    """
    Tries to import the user's Locking wrapper from their package layout.
    If that fails, tries relative fallbacks assuming the files are colocated.
    """
    try:
        from supertable.locking.locking import Locking
        from supertable.locking.locking_backend import LockingBackend
        return Locking, LockingBackend
    except Exception:
        # Fallbacks: local files in current dir or PYTHONPATH
        try:
            sys.path.insert(0, os.getcwd())
            from locking import Locking  # type: ignore
            from locking_backend import LockingBackend  # type: ignore
            return Locking, LockingBackend
        except Exception as e:
            print("[FATAL] Could not import Locking/LockingBackend. Ensure your project is on PYTHONPATH.")
            raise

Locking, LockingBackend = _try_import_locking()

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
    It also validates the on-disk JSON lock file for file backend (optional quick check).
    """

    def __init__(
        self,
        resources,
        num_threads=8,
        iterations=10,
        backend="file",
        working_dir=".",
        lock_file_name=".lock.json",
        acquire_timeout=10,
        lock_ttl=6,
        think_time=(0.05, 0.25),
        work_time=(0.2, 0.6),
        partial_release_prob=0.5,
        verbose=True,
    ):
        self.resources = list(resources)
        self.num_threads = int(num_threads)
        self.iterations = int(iterations)
        self.backend = backend.lower()
        self.working_dir = working_dir
        self.lock_file_name = lock_file_name
        self.acquire_timeout = float(acquire_timeout)
        self.lock_ttl = float(lock_ttl)
        self.think_time = think_time
        self.work_time = work_time
        self.partial_release_prob = float(partial_release_prob)
        self.verbose = verbose

        # Shared in-process check: resource -> holder thread id
        self._active_map = {}
        self._active_map_lock = threading.RLock()

        # Counters
        self._acquired = 0
        self._timeouts = 0
        self._counters_lock = threading.Lock()

        # Locking instance (shared safely across threads; it has its own internal RLock)
        if self.backend == "file":
            backend_enum = LockingBackend.FILE
        elif self.backend == "redis":
            backend_enum = LockingBackend.REDIS
        else:
            raise ValueError("backend must be 'file' or 'redis'")

        # Give a stable identity for this simulator process
        self.identity = f"sim-{rand_id('PID')}"

        self.locking = Locking(
            identity=self.identity,
            backend=backend_enum,
            working_dir=self.working_dir,
            lock_file_name=self.lock_file_name,
            check_interval=0.05,
        )

        atexit.register(self._on_exit)

    def _log(self, msg):
        if self.verbose:
            print(f"[{now()}] {msg}")

    def _on_exit(self):
        # Ensure clean release on interpreter shutdown
        try:
            self.locking.release_lock()
            self._log("Cleaned up locks at exit.")
        except Exception:
            pass

    # --------- Internal integrity checks (in-process) ---------
    def _claim_resources_locally(self, tid, res):
        with self._active_map_lock:
            for r in res:
                holder = self._active_map.get(r)
                if holder is not None and holder != tid:
                    raise AssertionError(f"INVARIANT VIOLATION: Resource '{r}' already held by {holder}, new holder {tid}")
            for r in res:
                self._active_map[r] = tid

    def _release_resources_locally(self, tid, res):
        with self._active_map_lock:
            for r in res:
                holder = self._active_map.get(r)
                if holder == tid:
                    del self._active_map[r]

    # --------- Worker logic ---------
    def _worker(self, tid):
        random.seed(os.getpid() ^ int(time.time() * 1e6) ^ threading.get_ident())
        for i in range(self.iterations):
            time.sleep(random.uniform(*self.think_time))
            # Sample 1..min(3, len(resources)) random resources
            sample_size = random.randint(1, min(3, len(self.resources)))
            pick = sorted(random.sample(self.resources, sample_size))

            self._log(f"{tid} → trying to lock {pick}")
            ok = self.locking.lock_resources(
                pick,
                timeout_seconds=self.acquire_timeout,
                lock_duration_seconds=self.lock_ttl,
            )
            if not ok:
                with self._counters_lock:
                    self._timeouts += 1
                self._log(f"{tid} ⚠ timeout on {pick}")
                continue

            self._claim_resources_locally(tid, pick)
            with self._counters_lock:
                self._acquired += 1
            self._log(f"{tid} ✓ acquired {pick}")

            # Simulate work while holding the lock
            time.sleep(random.uniform(*self.work_time))

            # Occasionally do a partial release
            if random.random() < self.partial_release_prob and len(pick) > 1:
                to_release = sorted(random.sample(pick, random.randint(1, len(pick)-1)))
                self._log(f"{tid} ↩ partial release {to_release}")
                self.locking.release_lock(to_release)
                self._release_resources_locally(tid, to_release)
                remaining = [r for r in pick if r not in to_release]
                # Do a little more work
                time.sleep(random.uniform(*self.work_time))
                self._log(f"{tid} ↩ final release {remaining}")
                self.locking.release_lock(remaining)
                self._release_resources_locally(tid, remaining)
            else:
                self._log(f"{tid} ↩ release {pick}")
                self.locking.release_lock(pick)
                self._release_resources_locally(tid, pick)

        return f"{tid} done"

    # --------- Run ---------
    def run(self):
        self._log(f"Simulator '{self.identity}' starting with backend={self.backend}, threads={self.num_threads}, iterations={self.iterations}")
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
            futs = [ex.submit(self._worker, f"T{idx+1:02d}") for idx in range(self.num_threads)]
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
                raise AssertionError(f"INVARIANT VIOLATION: resources still marked active: {self._active_map}")

        # Optional: For FILE backend, quick check that our pid entry is gone
        if self.backend == "file":
            lock_path = os.path.join(self.working_dir, self.lock_file_name)
            if os.path.exists(lock_path):
                try:
                    with open(lock_path, "r") as fh:
                        data = json.load(fh)
                    leftovers = [L for L in data if L.get("pid","") == getattr(self.locking.lock_instance, "lock_id", None)]
                    if leftovers:
                        raise AssertionError(f"Lock file still contains our entries: {leftovers}")
                except Exception as e:
                    self._log(f"Note: could not verify lock file: {e}")

        self._log("✔ Concurrency simulation completed successfully.")

# -------------------- CLI --------------------
def main():
    ap = ArgumentParser(description="Simulate concurrent locking on multiple resources.")
    ap.add_argument("--backend", choices=["file", "redis"], default=os.environ.get("SIM_BACKEND", "file"))
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SIM_THREADS", "8")))
    ap.add_argument("--iters", type=int, default=int(os.environ.get("SIM_ITERS", "10")))
    ap.add_argument("--resources", type=str, default=os.environ.get("SIM_RESOURCES", "A,B,C,D,E,F"))
    ap.add_argument("--workdir", type=str, default=os.path.abspath("./.locks"))
    ap.add_argument("--lock-file", type=str, default=".lock.json")
    ap.add_argument("--timeout", type=float, default=float(os.environ.get("SIM_TIMEOUT", "10")))
    ap.add_argument("--ttl", type=float, default=float(os.environ.get("SIM_TTL", "6")))
    ap.add_argument("--partial", type=float, default=float(os.environ.get("SIM_PARTIAL", "0.5")))
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    resources = [r.strip() for r in args.resources.split(",") if r.strip()]
    sim = ConcurrencySimulator(
        resources=resources,
        num_threads=args.threads,
        iterations=args.iters,
        backend=args.backend,
        working_dir=args.workdir,
        lock_file_name=args.lock_file,
        acquire_timeout=args.timeout,
        lock_ttl=args.ttl,
        partial_release_prob=args.partial,
        verbose=not args.quiet,
    )
    sim.run()

def run_with_defaults():
    # Runs when no CLI args are provided (PyCharm ▶ works with these defaults)
    sim = ConcurrencySimulator(
        resources=["A", "B", "C", "D", "E"],
        num_threads=4,
        iterations=5,
        backend="file",
        working_dir=os.path.abspath("./.locks"),  # <- unify here
        lock_file_name=".lock.json",
        acquire_timeout=10,
        lock_ttl=6,
        partial_release_prob=0.5,
        verbose=True,
    )
    print(f"⚙️ Running concurrency sim with backend=file, threads=4, iters=5, resources={sim.resources}")
    sim.run()

if __name__ == "__main__":
    # If any CLI args are passed → argparse/main()
    # Otherwise → run defaults (nice for PyCharm Run with no parameters)
    if len(sys.argv) > 1:
        main()
    else:
        run_with_defaults()
