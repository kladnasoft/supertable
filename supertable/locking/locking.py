# supertable/locking/locking.py

from __future__ import annotations

import time
from typing import Optional, Iterable, Dict

import supertable.config.homedir  # ensure SUPERTABLE_HOME init

from supertable.config.defaults import default, logger
from supertable.locking.locking_backend import LockingBackend
from supertable.locking.file_lock import FileLocking


class Locking:
    """
    Facade selecting the concrete locking backend. **Defaults to Redis**.

    Override precedence (highest → lowest):
      1) explicit `backend` constructor arg
      2) ENV `LOCKING_BACKEND` (redis|file)
      3) default → REDIS
    """

    def __init__(
        self,
        identity: str,
        backend: Optional[LockingBackend] = None,
        working_dir: Optional[str] = None,
        lock_file_name: str = ".lock.json",
        check_interval: float = 0.05,
        **kwargs,
    ):
        self.identity = identity
        self.check_interval = max(0.01, float(check_interval))

        # Decide backend
        env_choice = getattr(default, "LOCKING_BACKEND", None) if hasattr(default, "LOCKING_BACKEND") else None
        chosen = backend or LockingBackend.from_str(env_choice, default=LockingBackend.REDIS)
        self.backend = chosen

        if self.backend == LockingBackend.REDIS:
            redis_options = {
                "host": getattr(default, "REDIS_HOST", "localhost"),
                "port": getattr(default, "REDIS_PORT", 6379),
                "db": getattr(default, "REDIS_DB", 0),
                "password": getattr(default, "REDIS_PASSWORD", None),
            }
            redis_options.update(kwargs)
            try:
                from supertable.locking.redis_lock import RedisLocking

                self.lock_instance = RedisLocking(
                    identity=self.identity,
                    check_interval=self.check_interval,
                    **redis_options,
                )
            except Exception as e:
                raise RuntimeError(
                    "Redis backend selected, but could not initialize RedisLocking. "
                    "Install `redis` and verify REDIS_* settings."
                ) from e

        elif self.backend == LockingBackend.FILE:
            if not working_dir:
                # fall back to SUPERTABLE_HOME if not provided
                working_dir = getattr(default, "SUPERTABLE_HOME", None)
            if not working_dir:
                raise ValueError("FILE backend requires working_dir or SUPERTABLE_HOME")
            self.lock_instance = FileLocking(
                identity=self.identity,
                working_dir=working_dir,
                lock_file_name=lock_file_name,
                check_interval=self.check_interval,
            )
        else:
            raise ValueError(f"Unsupported locking backend: {self.backend!r}")

        logger.debug(f"[locking] backend={self.backend.value}")

    # ---------------- proxy API ----------------

    def acquire(self, resources: Iterable[str], duration: int = 30, who: str = "") -> bool:
        return self.lock_instance.acquire(resources, duration=duration, who=who)

    def release(self, resources: Iterable[str]) -> None:
        self.lock_instance.release(resources)

    def who(self, resources: Iterable[str]) -> Dict[str, str]:
        return self.lock_instance.who(resources)

    # ---------------- convenience helpers ----------------

    def read_with_lock(self, file_path: str, duration: int = 5) -> bytes | None:
        """
        Small helper: lock a file path as a resource, read it, then release.
        Uses this Locking's backend (Redis/file). Only reads local paths.
        """
        acquired = False
        try:
            acquired = self.acquire([file_path], duration=duration, who="read_with_lock")
            if not acquired:
                return None
            try:
                with open(file_path, "rb") as f:
                    return f.read()
            except FileNotFoundError:
                return None
        finally:
            if acquired:
                try:
                    self.release([file_path])
                except Exception:
                    pass
