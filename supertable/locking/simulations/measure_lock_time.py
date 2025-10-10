# measure_lock_time.py
import os
import time

import supertable.config.homedir  # ensure env/init
from supertable.config.defaults import logger, logging, print_config, default
from supertable.storage.storage_factory import get_storage
from supertable.locking import Locking

logger.setLevel(logging.DEBUG)
print_config()

# ---------------------------------------------------------------------- setup
storage = get_storage()

LOCK_DIR = ".locks"
LOCK_FILE = ".lock.json"
LOCK_PATH = os.path.join(LOCK_DIR, LOCK_FILE)

# Ensure directory and lock file exist (backend-agnostic)
if not storage.exists(LOCK_DIR):
    storage.makedirs(LOCK_DIR)
if not storage.exists(LOCK_PATH):
    storage.write_text(LOCK_PATH, "[]")

# Use the unified Locking façade (FILE backend -> fcntl; otherwise -> Redis)
lock = Locking(identity="measure-locks", working_dir=LOCK_DIR)


# ---------------------------------------------------------------------- logic
def measure_shared_lock_time() -> float:
    """
    Measure time to acquire a 'shared' read lock and read the JSON content.

    Notes:
      - FILE backend: truly uses fcntl shared lock.
      - REDIS backend: uses an exclusive logical lock on the resource key
        (shared semantics are not available on Redis).
    """
    start = time.time()
    # Acquire & release via helper (per-backend semantics handled inside)
    _ = lock.lock_shared_and_read(LOCK_PATH)
    end = time.time()
    return end - start


def measure_exclusive_lock_time() -> float:
    """
    Measure time to acquire and release an exclusive lock on the same resource.
    """
    start = time.time()
    acquired = lock.lock_resources(
        [LOCK_PATH],
        timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
        lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC,
    )
    if acquired:
        try:
            # Immediately release; we only want acquisition latency
            pass
        finally:
            lock.release_lock([LOCK_PATH])
    end = time.time()
    return end - start


# ---------------------------------------------------------------------- run
try:
    shared_time = measure_shared_lock_time()
    print(f"Time to acquire shared lock: {shared_time:.6f} seconds")

    exclusive_time = measure_exclusive_lock_time()
    print(f"Time to acquire exclusive lock: {exclusive_time:.6f} seconds")

except Exception as e:
    logger.error(f"Error measuring lock times: {e}")
