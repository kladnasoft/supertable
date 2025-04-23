import json
import os
import secrets
import time
import fcntl
import threading
from supertable.config.defaults import default, logger

class FileLocking:
    def __init__(self, identity, working_dir,
                 lock_file_name=".lock.json", check_interval=0.1):
        self.identity       = identity
        self.lock_id        = secrets.token_hex(8)
        self.lock_file_dir  = working_dir or identity
        self.lock_file_path = os.path.join(self.lock_file_dir, lock_file_name)
        self.check_interval = check_interval
        self._expiry_timer  = None

        os.makedirs(self.lock_file_dir, exist_ok=True)
        if not os.path.exists(self.lock_file_path):
            with open(self.lock_file_path, "w") as f:
                json.dump([], f)

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
        now = int(time.time())
        return [L for L in lock_data if L["exp"] > now]

    def remove_own_locks(self, lock_data):
        return [L for L in lock_data if L["pid"] != self.lock_id]

    def self_lock(self,
                  timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
                  lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC):
        """Convenience: lock just this identity as a single-resource lock."""
        return self.lock_resources([self.identity],
                                   timeout_seconds,
                                   lock_duration_seconds)

    def lock_resources(self, resources,
                       timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
                       lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC):
        start = time.time()
        sleep = 0
        while time.time() - start < timeout_seconds:
            if sleep:
                time.sleep(sleep)

            try:
                with open(self.lock_file_path, "r+") as f:
                    # 1) non-blocking flock
                    try:
                        fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        sleep = self.check_interval
                        continue

                    try:
                        # 2) JSON bookkeeping
                        data   = self.read_lock_file(f)
                        data   = self.remove_expired_locks(data)
                        others = self.remove_own_locks(data)

                        if any(r in lock["res"] for lock in others for r in resources):
                            sleep = self.check_interval
                            continue

                        now    = int(time.time())
                        exp_ts = now + lock_duration_seconds
                        data.append({
                            "pid": self.lock_id,
                            "exp": exp_ts,
                            "res": resources
                        })
                        self.write_lock_file(data, f)
                        logger.debug(f"{self.identity}: lock acquired, expires at {exp_ts}")

                        # schedule auto‐release after the TTL
                        if self._expiry_timer:
                            self._expiry_timer.cancel()
                        timer = threading.Timer(lock_duration_seconds, self.release_lock)
                        timer.daemon = True
                        timer.start()
                        self._expiry_timer = timer

                        return True
                    finally:
                        fcntl.flock(f, fcntl.LOCK_UN)

            except Exception as e:
                logger.error(f"{self.identity}: unexpected lock error: {e}")
                time.sleep(self.check_interval)

        return False

    def release_lock(self, resources=None):
        # cancel pending expiry callback
        if self._expiry_timer:
            self._expiry_timer.cancel()
            self._expiry_timer = None

        with open(self.lock_file_path, "r+") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            try:
                data = self.read_lock_file(f)
                data = [L for L in data if L["pid"] != self.lock_id]
                self.write_lock_file(data, f)
            finally:
                fcntl.flock(f, fcntl.LOCK_UN)

    def __enter__(self):
        if not self.self_lock():
            raise Exception(f"Unable to acquire file lock for {self.identity}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_lock()
