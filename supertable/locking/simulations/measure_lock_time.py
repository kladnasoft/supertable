import os
import json
import fcntl
import time
import supertable.config.homedir
from supertable.config.defaults import logger, logging
logger.setLevel(logging.DEBUG)

LOCK_DIR = os.path.abspath("./.locks")
LOCK_FILE = ".lock.json"
lock_file_path = os.path.join(LOCK_DIR, LOCK_FILE)

os.makedirs(LOCK_DIR, exist_ok=True)
if not os.path.exists(lock_file_path):
    # Create an empty JSON array (what the file backend expects)
    with open(lock_file_path, "w") as f:
        f.write("[]")

def measure_lock_time(lock_type):
    # open without truncation so we don't destroy the JSON content
    with open(lock_file_path, "r+") as lock_file:
        start_time = time.time()
        fcntl.flock(lock_file, lock_type)
        end_time = time.time()
        fcntl.flock(lock_file, fcntl.LOCK_UN)
        return end_time - start_time

# Measure time to acquire a shared lock
shared_lock_time = measure_lock_time(fcntl.LOCK_SH)
print(f"Time to acquire shared lock: {shared_lock_time:.6f} seconds")

# Measure time to acquire an exclusive lock
exclusive_lock_time = measure_lock_time(fcntl.LOCK_EX)
print(f"Time to acquire exclusive lock: {exclusive_lock_time:.6f} seconds")
