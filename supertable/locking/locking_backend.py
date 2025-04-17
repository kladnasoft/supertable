from enum import Enum


class LockingBackend(Enum):
    FILE = "file"
    REDIS = "redis"