import json
import os
import secrets
import time
import fcntl
from supertable.config.defaults import default, logger

class FileLocking:
    def __init__(self, identity, working_dir, lock_file_name=".lock.json", check_interval=0.1):
        self.identity = identity
        self.lock_id = secrets.token_hex(8)
        self.lock_file_dir = working_dir if working_dir is not None else identity
        self.lock_file_path = os.path.join(self.lock_file_dir, lock_file_name)
        self.check_interval = check_interval
        logger.debug(f"lock_file_dir: {self.lock_file_dir}")
        logger.debug(f"lock_file_path: {self.lock_file_path}")
        self.init_lock_file()

    def init_lock_file(self):
        os.makedirs(self.lock_file_dir, exist_ok=True)
        if not os.path.exists(self.lock_file_path):
            with open(self.lock_file_path, "w") as lock_file:
                json.dump([], lock_file)

    def read_lock_file(self, lock_file):
        lock_file.seek(0)
        try:
            return json.load(lock_file)
        except json.JSONDecodeError as e:
            logger.error(f"Error reading lock file: {e}")
            return []

    def write_lock_file(self, lock_data, lock_file):
        lock_file.seek(0)
        lock_file.truncate()
        json.dump(lock_data, lock_file)
        lock_file.flush()
        os.fsync(lock_file.fileno())

    def remove_expired_locks(self, lock_data):
        current_time = int(time.time())
        return [lock for lock in lock_data if lock["exp"] > current_time]

    def remove_own_locks(self, lock_data):
        return [lock for lock in lock_data if lock["pid"] != self.lock_id]

    def self_lock(self, timeout_seconds=default.DEFAULT_TIMEOUT_SEC, lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC):
        resources = [self.identity]
        return self.lock_resources(resources, timeout_seconds, lock_duration_seconds)

    def lock_resources(self, resources, timeout_seconds=default.DEFAULT_TIMEOUT_SEC, lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC):
        start_time = time.time()
        expiration_time = int(time.time() + lock_duration_seconds)
        sleep_time = 0

        while time.time() - start_time < timeout_seconds:
            try:
                if sleep_time > 0:
                    logger.debug(f"Waiting {sleep_time} seconds to acquire the lock for {self.identity}")
                    time.sleep(sleep_time)

                with open(self.lock_file_path, "r+") as lock_file:
                    fcntl.flock(lock_file, fcntl.LOCK_EX)
                    try:
                        lock_data = self.read_lock_file(lock_file)
                        lock_data = self.remove_expired_locks(lock_data)
                        lock_check = self.remove_own_locks(lock_data)
                        logger.debug(f"Lock data: {lock_data}")
                        logger.debug(f"Lock check: {lock_check}")

                        if any(resource in lock["res"] for lock in lock_check for resource in resources):
                            logger.debug(f"{self.identity}: lock can't be acquired for resources {resources}")
                            sleep_time = self.check_interval
                            continue

                        lock_entry = {"pid": self.lock_id, "exp": expiration_time, "res": resources}
                        lock_data.append(lock_entry)
                        self.write_lock_file(lock_data, lock_file)
                        logger.debug(f"Identity: {self.identity}, lock acquired: {self.lock_id}")
                        return True
                    finally:
                        fcntl.flock(lock_file, fcntl.LOCK_UN)
            except Exception as e:
                logger.error(f"Error during lock acquisition: {e}")
                time.sleep(self.check_interval)
        return False

    def release_lock(self, resources=None):
        with open(self.lock_file_path, "r+") as lock_file:
            fcntl.flock(lock_file, fcntl.LOCK_EX)
            try:
                lock_data = self.read_lock_file(lock_file)
                lock_data = [lock for lock in lock_data if lock["pid"] != self.lock_id]
                self.write_lock_file(lock_data, lock_file)
            finally:
                fcntl.flock(lock_file, fcntl.LOCK_UN)

    def __enter__(self):
        if not self.self_lock():
            raise Exception(f"Unable to acquire file lock for {self.identity}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_lock()
