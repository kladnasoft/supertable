# route: supertable.locking
from supertable.locking.redis_lock import RedisLocking
from supertable.locking.file_lock import FileLocking

__all__ = ["RedisLocking", "FileLocking"]
