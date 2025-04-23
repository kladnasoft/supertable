import threading
import time
import tempfile
import shutil
import random

from supertable.locking.file_lock import FileLocking as OriginalLocking

NUM_THREADS = 10
HOLD_TIME   = 1.0   # seconds each thread holds the lock once acquired

def run_multithreaded_test(lock_cls, label):
    barrier = threading.Barrier(NUM_THREADS)
    tmpdir = tempfile.mkdtemp()
    acquisitions = []       # will hold dicts with thread info
    acquisitions_lock = threading.Lock()

    def worker(idx):
        # each thread picks 5 distinct resources from 1–50
        picks = random.sample(range(1, 51), 5)
        resources = [f"res{n}" for n in picks]

        name = f"{label}-T{idx}"
        lock = lock_cls(identity=name, working_dir=tmpdir)

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
        time.sleep(HOLD_TIME)
        lock.release_lock()

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(NUM_THREADS)]
    for t in threads: t.start()
    for t in threads: t.join()

    # cleanup
    shutil.rmtree(tmpdir)

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
    avg = sum(waits) / len(waits) if waits else 0
    print(f"\n{label} SUMMARY:")
    print(f"  Threads attempted: {NUM_THREADS}")
    print(f"  Successful locks : {len(waits)}")
    print(f"  Avg wait         : {avg:.4f}s")
    print(f"  Min wait         : {min(waits):.4f}s")
    print(f"  Max wait         : {max(waits):.4f}s")
    print("-" * 40)

if __name__ == "__main__":
    print("==== LOCKING Implementation ====")
    run_multithreaded_test(OriginalLocking, "Locking")
