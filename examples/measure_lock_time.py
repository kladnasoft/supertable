import fcntl
import time

lock_file_path =  "tests.lock"

def measure_lock_time(lock_type):
    with open(lock_file_path, "w") as lock_file:
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
