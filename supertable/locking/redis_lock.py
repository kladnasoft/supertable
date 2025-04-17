import time
import secrets
import redis
from supertable.config.defaults import default

class RedisLocking:
    def __init__(self, identity, check_interval=0.1, redis_client=None, host='localhost', port=6379, db=0, password=None):
        self.identity = identity
        self.lock_id = secrets.token_hex(8)
        self.check_interval = check_interval
        if redis_client:
            self.redis = redis_client
        else:
            self.redis = redis.Redis(host=host, port=port, db=db, password=password)

    def _lock_key(self, resource):
        return f"lock:{resource}"

    def lock_resources(self, resources, timeout_seconds=default.DEFAULT_TIMEOUT_SEC, lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC):
        start_time = time.time()
        expiration = lock_duration_seconds

        while time.time() - start_time < timeout_seconds:
            acquired = []
            try:
                for res in resources:
                    key = self._lock_key(res)
                    result = self.redis.set(key, self.lock_id, ex=expiration, nx=True)
                    if result:
                        acquired.append(key)
                    else:
                        break

                if len(acquired) == len(resources):
                    return True
                else:
                    for key in acquired:
                        current_value = self.redis.get(key)
                        if current_value and current_value.decode() == self.lock_id:
                            self.redis.delete(key)
                    time.sleep(self.check_interval)
            except Exception as e:
                if default.IS_DEBUG:
                    print("Redis lock acquisition error:", e)
                time.sleep(self.check_interval)
        return False

    def self_lock(self, timeout_seconds=default.DEFAULT_TIMEOUT_SEC, lock_duration_seconds=default.DEFAULT_LOCK_DURATION_SEC):
        return self.lock_resources([self.identity], timeout_seconds, lock_duration_seconds)

    def release_lock(self, resources=None):
        if resources is None:
            resources = [self.identity]
        for res in resources:
            key = self._lock_key(res)
            try:
                current_value = self.redis.get(key)
                if current_value and current_value.decode() == self.lock_id:
                    self.redis.delete(key)
            except Exception as e:
                if default.IS_DEBUG:
                    print("Redis lock release error:", e)

    def __enter__(self):
        if not self.self_lock():
            raise Exception(f"Unable to acquire Redis lock for {self.identity}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.release_lock()
