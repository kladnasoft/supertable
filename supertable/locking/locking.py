# locking.py

import json
import time
import fcntl
from typing import Optional

import supertable.config.homedir  # noqa: F401

from supertable.config.defaults import default
from supertable.locking.file_lock import FileLocking
from supertable.locking.locking_backend import LockingBackend


class Locking:
    """
    Unified locking façade that delegates to a concrete backend (file or Redis)
    and provides a small set of convenience helpers used across the codebase.
    """

    def __init__(
        self,
        identity: str,
        backend: Optional[LockingBackend] = None,
        working_dir: Optional[str] = None,
        lock_file_name: str = ".lock.json",
        check_interval: float = 0.1,
        **kwargs,
    ):
        """
        Parameters:
            identity: Unique identifier for the lock owner (used for logging/diagnostics).
            backend:
                - LockingBackend.FILE or LockingBackend.REDIS
                - If None, auto-detect based on default.STORAGE_TYPE.
                  If default.STORAGE_TYPE == 'LOCAL', use file-based locking.
                  Otherwise, use Redis-based locking.
            working_dir: Required for file-based locking; ignored for Redis.
            lock_file_name: Name of the lock file (for file-based locking).
            check_interval: Time between retry attempts.
            kwargs: Additional parameters passed to the backend (e.g., Redis connection).
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

                self.lock_instance = RedisLocking(
                    identity, check_interval=self.check_interval, **redis_options
                )
            except Exception as e:
                raise RuntimeError(
                    "Redis backend selected, but the Redis backend could not be imported. "
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

    # ---------------- Public proxy API ----------------

    def lock_resources(
        self,
        resources,
        timeout_seconds: int = default.DEFAULT_TIMEOUT_SEC,
        lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC,
    ):
        return self.lock_instance.lock_resources(resources, timeout_seconds, lock_duration_seconds)

    def self_lock(
        self,
        timeout_seconds: int = default.DEFAULT_TIMEOUT_SEC,
        lock_duration_seconds: int = default.DEFAULT_LOCK_DURATION_SEC,
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

    # ---------------- Convenience helpers ----------------

    def lock_shared_and_read(self, lock_file_path: str):
        """
        Acquire a shared/read lock for the given *path-like* resource and return
        the JSON content of that file (dict/list). Behavior depends on backend:

        - FILE backend:
            Use an fcntl-based shared lock directly on the local file, then read.
        - REDIS backend:
            Use the Redis locking backend to acquire a logical lock on the
            path string, then read the file via local I/O.

        Notes:
            * This helper intentionally avoids any dependency on a storage/S3
              abstraction. If remote storage is needed, call sites should be
              refactored to pass in a reader callable, or a corresponding
              storage-aware helper should be introduced. For now, this function
              reads via local filesystem paths which matches current usage.
        """
        start_time = time.time()
        result = {}

        # -------- File backend: fcntl shared lock + local read
        if self.backend == LockingBackend.FILE:
            while time.time() - start_time < default.DEFAULT_TIMEOUT_SEC:
                try:
                    with open(lock_file_path, "r") as local_file:
                        fcntl.flock(local_file, fcntl.LOCK_SH)
                        try:
                            result = json.load(local_file)
                            return result
                        finally:
                            fcntl.flock(local_file, fcntl.LOCK_UN)
                except BlockingIOError:
                    time.sleep(self.check_interval)
                except FileNotFoundError:
                    # Missing file is a valid "empty" state for callers.
                    return {}
                except json.JSONDecodeError:
                    # Corrupt/empty JSON -> treat as empty to avoid hard failure.
                    return {}
                except OSError:
                    # Transient I/O -> retry until timeout
                    time.sleep(self.check_interval)
            # Timeout -> return empty
            return {}

        # -------- Redis backend: logical lock on the path + local read
        # Use the existing backend to lock the path string atomically.
        timeout = default.DEFAULT_TIMEOUT_SEC
        duration = default.DEFAULT_LOCK_DURATION_SEC
        acquired = False

        try:
            while time.time() - start_time < timeout:
                acquired = self.lock_instance.lock_resources(
                    [lock_file_path],
                    timeout_seconds=min(1, max(0, timeout - int(time.time() - start_time))),
                    lock_duration_seconds=duration,
                )
                if acquired:
                    break
                time.sleep(self.check_interval)

            if not acquired:
                # Could not acquire within timeout; return empty to keep callers tolerant.
                return {}

            # Once locked, perform a best-effort local read (see Notes above).
            try:
                with open(lock_file_path, "r") as fh:
                    try:
                        return json.load(fh)
                    except json.JSONDecodeError:
                        return {}
                    except OSError:
                        return {}
            except FileNotFoundError:
                return {}

        finally:
            if acquired:
                # Always release only the resource we took.
                try:
                    self.lock_instance.release_lock([lock_file_path])
                except Exception:
                    # Never raise on cleanup
                    pass
