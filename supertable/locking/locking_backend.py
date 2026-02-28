# supertable/locking/locking_backend.py

from __future__ import annotations
from enum import Enum
from typing import Optional


class LockingBackend(Enum):
    FILE = "file"
    REDIS = "redis"

    @classmethod
    def from_str(cls, value: str | None, default: Optional["LockingBackend"] = None) -> "LockingBackend":
        if not value:
            return default or cls.REDIS
        v = str(value).strip().lower()
        if v == "redis":
            return cls.REDIS
        if v == "file":
            return cls.FILE
        return default or cls.REDIS
