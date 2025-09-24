import json
import time
import fcntl
import supertable.config.homedir

from supertable.config.defaults import default
from supertable.locking.file_lock import FileLocking
from supertable.locking.locking_backend import LockingBackend

class Locking:
    def __init__(
        self,
        identity,
        backend: LockingBackend = None,
        working_dir=None,
        lock_file_name=".lock.json",
        check_interval=0.1,
        **kwargs
    ):
        """
        Parameters:
            identity: Unique identifier for the lock.
            backend:
                - LockingBackend.FILE or LockingBackend.REDIS
                - If None, auto-detect based on default.STORAGE_TYPE.
                - If default.STORAGE_TYPE == 'LOCAL', use file-based locking.
                - Otherwise, use Redis-based locking.
            working_dir: Required for file-based locking; ignored for Redis.
            lock_file_name: Name of the lock file (for file-based locking).
            check_interval: Time between retry attempts.
            kwargs: Additional parameters passed to the backend.
        """
        self.identity = identity
        self.check_interval = check_interval

        if backend is None:
            storage_type = getattr(default, "STORAGE_TYPE", "LOCAL").upper()
            backend = LockingBackend.FILE if storage_type == "LOCAL" else LockingBackend.REDIS

        self.backend = backend

        if self.backend == LockingBackend.REDIS:
            redis_options = {
                "host": getattr(default, "REDIS_HOST", "localhost"),
                "port": getattr(default, "REDIS_PORT", 6379),
                "db": getattr(default, "REDIS_DB", 0),
                "password": getattr(default, "REDIS_PASSWORD", None),
            }
            redis_options.update(kwargs)
            # Lazy import to avoid pulling Redis when not needed
            try:
                from supertable.locking.redis_lock import RedisLocking
                self.lock_instance = RedisLocking(identity, check_interval=self.check_interval, **redis_options)
            except Exception as e:
                raise RuntimeError(
                    "Redis backend selected, but redis backend could not be imported. "
                    "Install `redis` and ensure configuration is correct."
                ) from e

        elif self.backend == LockingBackend.FILE:
            self.lock_instance = FileLocking(
                identity,
                working_dir,
                lock_file_name=lock_file_name,
                check_interval=self.check_interval,
            )
        else:
            raise ValueError(f"Unsupported locking backend: {self.backend}")

    def lock_resources(
        self,
        resources,
        timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
        lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC
    ):
        return self.lock_instance.lock_resources(resources, timeout_seconds, lock_duration_seconds)

    def self_lock(
        self,
        timeout_seconds=default.DEFAULT_TIMEOUT_SEC,
        lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC
    ):
        return self.lock_instance.self_lock(timeout_seconds, lock_duration_seconds)

    def release_lock(self, resources=None):
        return self.lock_instance.release_lock(resources)

    def __enter__(self):
        if not self.self_lock():
            raise Exception(f"Unable to acquire lock for {self.identity}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_lock()

    def __del__(self):
        # Ensure backend-specific cleanup runs on GC
        try:
            self.release_lock()
        except Exception:
            pass


    def lock_shared_and_read(self, lock_file_path: str):
        """
        Acquires a shared lock and reads the file contents.
        - If STORAGE_TYPE=LOCAL, it uses local file I/O plus an fcntl-based shared lock.
        - Otherwise, it falls back to Redis-based locking, then reads from self.storage.

        Returns:
          The data (as a dict or list) read from the file if successful,
          or None if unable to lock/read within DEFAULT_TIMEOUT_SEC.
        """
        result = {}
        start_time = time.time()

        while time.time() - start_time < default.DEFAULT_TIMEOUT_SEC:
            if default.STORAGE_TYPE.upper() == "LOCAL":
                # 1. Local mode: use fcntl-based locking on a local file
                try:
                    with open(lock_file_path, "r") as local_file:
                        fcntl.flock(local_file, fcntl.LOCK_SH)
                        try:
                            result = json.load(local_file)
                            break
                        finally:
                            fcntl.flock(local_file, fcntl.LOCK_UN)
                except BlockingIOError:
                    # Could not acquire the lock; retry after a brief delay
                    time.sleep(self.check_interval)
                except FileNotFoundError:
                    # The file doesn't exist locally
                    break
                except json.JSONDecodeError:
                    # File is present but not valid JSON
                    # Depending on your preference, either break or raise an error
                    break

            else:
                # 2. Remote mode (S3, MinIO, etc.): use Redis-based locking and read from storage
                try:
                    acquired = self.redis_lock.acquire_shared_lock(
                        lock_name=lock_file_path,
                        timeout=default.DEFAULT_TIMEOUT_SEC
                    )
                    if acquired:
                        try:
                            # Use the storage interface for remote read
                            result = self.storage.read_json(lock_file_path)
                        finally:
                            self.redis_lock.release_shared_lock(lock_file_path)
                        break
                    else:
                        time.sleep(self.check_interval)
                except FileNotFoundError:
                    # Path not found in remote storage
                    break
                except json.JSONDecodeError:
                    # Remote file has invalid JSON
                    break

        return result