from __future__ import annotations

import os
import json
import time
import uuid
from dataclasses import dataclass, field
from typing import Dict, Iterator, List, Optional, Any
from urllib.parse import urlparse
from dotenv import load_dotenv

import redis
from redis.sentinel import Sentinel
from supertable.config.defaults import logger

load_dotenv()

def _now_ms() -> int:
    return int(time.time() * 1000)


def _root_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:root"


def _leaf_key(org: str, sup: str, simple: str) -> str:
    return f"supertable:{org}:{sup}:meta:leaf:{simple}"


def _lock_key(org: str, sup: str, simple: str) -> str:
    return f"supertable:{org}:{sup}:lock:leaf:{simple}"


def _stat_lock_key(org: str, sup: str) -> str:
    # Dedicated lock for stats updates
    return f"supertable:{org}:{sup}:lock:stat"


def _mirrors_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:mirrors"

def _users_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:users:meta"

def _roles_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:roles:meta"

def _user_hash_key(org: str, sup: str, user_hash: str) -> str:
    return f"supertable:{org}:{sup}:meta:users:{user_hash}"

def _role_hash_key(org: str, sup: str, role_hash: str) -> str:
    return f"supertable:{org}:{sup}:meta:roles:{role_hash}"

def _user_name_to_hash_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:users:name_to_hash"

def _role_type_to_hash_key(org: str, sup: str, role_type: str) -> str:
    return f"supertable:{org}:{sup}:meta:roles:type_to_hash:{role_type}"


@dataclass(frozen=True)
class RedisOptions:
    """
    Reads Redis connection options from environment variables.

    Supported:
      - SUPERTABLE_REDIS_URL (e.g. redis://:pass@host:6379/0 or rediss://:pass@host:6380/1)
      - or split vars: SUPERTABLE_REDIS_HOST, SUPERTABLE_REDIS_PORT, SUPERTABLE_REDIS_DB, SUPERTABLE_REDIS_PASSWORD

    Optional:
      - SUPERTABLE_REDIS_DECODE_RESPONSES (default: "true")

    Sentinel (optional):
      - SUPERTABLE_REDIS_SENTINEL (true/false)
      - SUPERTABLE_REDIS_SENTINELS="host1:26379,host2:26379"
      - SUPERTABLE_REDIS_SENTINEL_MASTER="mymaster"
    """
    host: str = field(init=False)
    port: int = field(init=False)
    db: int = field(init=False)
    password: Optional[str] = field(init=False)
    use_ssl: bool = field(init=False)
    decode_responses: bool = field(default=True)

    # Sentinel-related
    is_sentinel: bool = field(init=False)
    sentinel_hosts: List[tuple] = field(init=False)
    sentinel_master: str = field(init=False)

    def __post_init__(self):
        url = os.getenv("SUPERTABLE_REDIS_URL")
        if url:
            u = urlparse(url)
            host = u.hostname or "localhost"
            port = u.port or 6379
            # path like "/0"
            db = int((u.path or "/0").lstrip("/") or 0)
            password = u.password
            use_ssl = (u.scheme.lower() == "rediss")
        else:
            host = os.getenv("SUPERTABLE_REDIS_HOST", "localhost")
            port = int(os.getenv("SUPERTABLE_REDIS_PORT", "6379"))
            db = int(os.getenv("SUPERTABLE_REDIS_DB", "0"))
            password = os.getenv("SUPERTABLE_REDIS_PASSWORD")
            use_ssl = False

        decode = os.getenv("SUPERTABLE_REDIS_DECODE_RESPONSES", "true").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )

        # Sentinel mode configuration
        is_sentinel = os.getenv("SUPERTABLE_REDIS_SENTINEL", "false").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
        sentinel_raw = os.getenv("SUPERTABLE_REDIS_SENTINELS", "").strip()
        sentinels: List[tuple] = []
        if sentinel_raw:
            for part in sentinel_raw.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    h, p = part.split(":")
                    sentinels.append((h.strip(), int(p)))
                except ValueError:
                    logger.warning(f"[redis-options] Invalid sentinel spec: {part}")

        sentinel_master = os.getenv("SUPERTABLE_REDIS_SENTINEL_MASTER", "mymaster")

        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "db", db)
        object.__setattr__(self, "password", password)
        object.__setattr__(self, "use_ssl", use_ssl)
        object.__setattr__(self, "decode_responses", decode)

        object.__setattr__(self, "is_sentinel", is_sentinel)
        object.__setattr__(self, "sentinel_hosts", sentinels)
        object.__setattr__(self, "sentinel_master", sentinel_master)


class RedisCatalog:
    """
    Redis-backed catalog for SuperTable:
      * meta:root -> {"version": int, "ts": epoch_ms}
      * meta:leaf:{simple} -> {"version": int, "ts": epoch_ms, "path": ".../snapshot.json"}
      * meta:mirrors -> {"formats": [...], "ts": epoch_ms}
      * lock:leaf:{simple} -> token (SET NX EX)
      * lock:stat -> token (SET NX EX)  # for monitoring stats updates
    """

    # ------------- Lua sources -------------
    _LUA_LEAF_CAS_SET = """
local key = KEYS[1]
local new_path = ARGV[1]
local now_ms = tonumber(ARGV[2])

local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then
    old_version = tonumber(obj['version'])
  end
end
local new_version = old_version + 1
local new_val = cjson.encode({version=new_version, ts=now_ms, path=new_path})
redis.call('SET', key, new_val)
return new_version
"""

    _LUA_ROOT_BUMP = """
local key = KEYS[1]
local now_ms = tonumber(ARGV[1])

local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then
    old_version = tonumber(obj['version'])
  end
end
local new_version = old_version + 1
local new_val = cjson.encode({version=new_version, ts=now_ms})
redis.call('SET', key, new_val)
return new_version
"""

    _LUA_LOCK_RELEASE_IF_TOKEN = """
local key = KEYS[1]
local token = ARGV[1]
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('DEL', key)
  return 1
end
return 0
"""

    _LUA_LOCK_EXTEND_IF_TOKEN = """
local key = KEYS[1]
local token = ARGV[1]
local ttl_ms = tonumber(ARGV[2])
local cur = redis.call('GET', key)
if cur and cur == token then
  redis.call('PEXPIRE', key, ttl_ms)
  return 1
end
return 0
"""

    def __init__(self, options: Optional[RedisOptions] = None):
        opts = options or RedisOptions()

        # Decide between standard Redis and Sentinel-based Redis
        if opts.is_sentinel and opts.sentinel_hosts:
            logger.info(
                f"[redis-catalog] Using Redis Sentinel mode. master={opts.sentinel_master}, "
                f"sentinels={opts.sentinel_hosts}"
            )
            sentinel = Sentinel(
                opts.sentinel_hosts,
                socket_timeout=0.5,
                password=opts.password,
                decode_responses=opts.decode_responses,
            )
            self.r = sentinel.master_for(
                opts.sentinel_master,
                db=opts.db,
                password=opts.password,
                decode_responses=opts.decode_responses,
            )
        else:
            if opts.is_sentinel and not opts.sentinel_hosts:
                logger.warning(
                    "[redis-catalog] SUPERTABLE_REDIS_SENTINEL=true but no "
                    "SUPERTABLE_REDIS_SENTINELS set. Falling back to standard Redis."
                )
            logger.info(
                f"[redis-catalog] Using standard Redis mode. host={opts.host}, port={opts.port}, db={opts.db}"
            )
            self.r = redis.Redis(
                host=opts.host,
                port=opts.port,
                db=opts.db,
                password=opts.password,
                decode_responses=opts.decode_responses,
                ssl=opts.use_ssl,
            )

        # Register scripts
        self._leaf_cas_set = self.r.register_script(self._LUA_LEAF_CAS_SET)
        self._root_bump = self.r.register_script(self._LUA_ROOT_BUMP)
        self._lock_release_if_token = self.r.register_script(self._LUA_LOCK_RELEASE_IF_TOKEN)
        self._lock_extend_if_token = self.r.register_script(self._LUA_LOCK_EXTEND_IF_TOKEN)

    # ------------- Locking -------------

    def acquire_simple_lock(self, org: str, sup: str, simple: str, ttl_s: int = 30, timeout_s: int = 30) -> Optional[str]:
        """SET lock key NX EX with retry/backoff <= timeout. Returns token if acquired else None."""
        key = _lock_key(org, sup, simple)
        token = uuid.uuid4().hex
        deadline = time.time() + max(1, int(timeout_s))
        while time.time() < deadline:
            try:
                ok = self.r.set(key, token, nx=True, ex=max(1, int(ttl_s)))
                if ok:
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-lock] acquire error on {key}: {e}")
            time.sleep(0.05)
        return None

    def release_simple_lock(self, org: str, sup: str, simple: str, token: str) -> bool:
        """Compare-and-delete via Lua."""
        try:
            res = self._lock_release_if_token(keys=[_lock_key(org, sup, simple)], args=[token])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-lock] release error: {e}")
            return False

    def extend_simple_lock(self, org: str, sup: str, simple: str, token: str, ttl_ms: int) -> bool:
        """Optionally extend TTL if token matches."""
        try:
            res = self._lock_extend_if_token(keys=[_lock_key(org, sup, simple)], args=[token, int(ttl_ms)])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-lock] extend error: {e}")
            return False

    # ---- Stats lock (for monitoring _stats.json updates) ----

    def acquire_stat_lock(self, org: str, sup: str, ttl_s: int = 10, timeout_s: int = 10) -> Optional[str]:
        """Acquire stat lock: supertable:{org}:{sup}:lock:stat"""
        key = _stat_lock_key(org, sup)
        token = uuid.uuid4().hex
        deadline = time.time() + max(1, int(timeout_s))
        while time.time() < deadline:
            try:
                ok = self.r.set(key, token, nx=True, ex=max(1, int(ttl_s)))
                if ok:
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-stat-lock] acquire error on {key}: {e}")
            time.sleep(0.05)
        return None

    def release_stat_lock(self, org: str, sup: str, token: str) -> bool:
        """Release stat lock if token matches."""
        try:
            res = self._lock_release_if_token(keys=[_stat_lock_key(org, sup)], args=[token])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-stat-lock] release error: {e}")
            return False

    # ------------- Pointers (root/leaf) -------------

    def ensure_root(self, org: str, sup: str) -> None:
        """Initialize meta:root if missing with version=0."""
        key = _root_key(org, sup)
        try:
            if not self.r.exists(key):
                init = {"version": 0, "ts": _now_ms()}
                self.r.set(key, json.dumps(init))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] ensure_root failed: {e}")
            raise

    def root_exists(self, org: str, sup: str) -> bool:
        """Check existence of meta:root key."""
        try:
            return bool(self.r.exists(_root_key(org, sup)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] root_exists error: {e}")
            return False

    def leaf_exists(self, org: str, sup: str, simple: str) -> bool:
        """Check existence of meta:leaf key for a simple table."""
        try:
            return bool(self.r.exists(_leaf_key(org, sup, simple)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_exists error: {e}")
            return False

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        try:
            raw = self.r.get(_root_key(org, sup))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_root error: {e}")
            return None

    def bump_root(self, org: str, sup: str, now_ms: Optional[int] = None) -> int:
        try:
            return int(self._root_bump(keys=[_root_key(org, sup)], args=[int(now_ms or _now_ms())]) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] root_bump error: {e}")
            raise

    def get_leaf(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        try:
            raw = self.r.get(_leaf_key(org, sup, simple))
            return json.loads(raw) if raw else None
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_leaf error: {e}")
            return None

    def set_leaf_path_cas(self, org: str, sup: str, simple: str, path: str, now_ms: Optional[int] = None) -> int:
        try:
            return int(
                self._leaf_cas_set(keys=[_leaf_key(org, sup, simple)], args=[path, int(now_ms or _now_ms())]) or 0
            )
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_cas_set error: {e}")
            raise

    # ------------- Mirror formats (Redis-backed) -------------

    def get_mirrors(self, org: str, sup: str) -> List[str]:
        """Read enabled mirror formats from Redis key."""
        try:
            raw = self.r.get(_mirrors_key(org, sup))
            if not raw:
                return []
            obj = json.loads(raw)
            formats = obj.get("formats", [])
            seen = set()
            out: List[str] = []
            for f in (formats or []):
                fu = str(f).upper()
                if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in seen:
                    seen.add(fu)
                    out.append(fu)
            return out
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_mirrors error: {e}")
            return []
        except Exception:
            return []

    def set_mirrors(self, org: str, sup: str, formats: List[str], now_ms: Optional[int] = None) -> List[str]:
        """Atomically set enabled mirror formats."""
        seen = set()
        ordered: List[str] = []
        for f in formats or []:
            fu = str(f).upper()
            if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in seen:
                seen.add(fu)
                ordered.append(fu)
        try:
            payload = {"formats": ordered, "ts": int(now_ms or _now_ms())}
            self.r.set(_mirrors_key(org, sup), json.dumps(payload))
            return ordered
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] set_mirrors error: {e}")
            raise

    def enable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        if fu not in ("DELTA", "ICEBERG", "PARQUET"):
            return cur
        if fu in cur:
            return cur
        return self.set_mirrors(org, sup, cur + [fu])

    def disable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        nxt = [x for x in cur if x != fu]
        if nxt == cur:
            return cur
        return self.set_mirrors(org, sup, nxt)

    # ------------- User and Role Management -------------

    def get_users(self, org: str, sup: str) -> List[Dict[str, Any]]:
        """Get all users for organization."""
        users = []
        pattern = f"supertable:{org}:{sup}:meta:users:*"
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=100)
                for key in keys:
                    # Skip name_to_hash and meta keys
                    if "name_to_hash" in key or ("meta" in key and "users:meta" not in key):
                        continue
                    raw = self.r.get(key)
                    if raw:
                        try:
                            user_data = json.loads(raw)
                            if isinstance(user_data, dict):
                                user_hash = key.split(':')[-1]
                                users.append({
                                    "hash": user_hash,
                                    **user_data
                                })
                        except Exception:
                            continue
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_users error: {e}")
        return users

    def get_roles(self, org: str, sup: str) -> List[Dict[str, Any]]:
        """Get all roles for organization."""
        roles = []
        pattern = f"supertable:{org}:{sup}:meta:roles:*"
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=100)
                for key in keys:
                    # Skip type_to_hash and meta keys
                    if "type_to_hash" in key or ("meta" in key and "roles:meta" not in key):
                        continue
                    raw = self.r.get(key)
                    if raw:
                        try:
                            role_data = json.loads(raw)
                            if isinstance(role_data, dict):
                                role_hash = key.split(':')[-1]
                                roles.append({
                                    "hash": role_hash,
                                    **role_data
                                })
                        except Exception:
                            continue
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_roles error: {e}")
        return roles

    def get_role_details(self, org: str, sup: str, role_hash: str) -> Optional[Dict[str, Any]]:
        """Get detailed role information."""
        try:
            raw = self.r.get(_role_hash_key(org, sup, role_hash))
            if raw:
                return json.loads(raw)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_role_details error: {e}")
        return None

    def get_user_details(self, org: str, sup: str, user_hash: str) -> Optional[Dict[str, Any]]:
        """Get detailed user information."""
        try:
            raw = self.r.get(_user_hash_key(org, sup, user_hash))
            if raw:
                return json.loads(raw)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_user_details error: {e}")
        return None

    # ------------- Listings via SCAN -------------

    def scan_leaf_keys(self, org: str, sup: str, count: int = 1000) -> Iterator[str]:
        """Yields full Redis keys: supertable:{org}:{sup}:meta:leaf:*"""
        pattern = f"supertable:{org}:{sup}:meta:leaf:*"
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys:
                    yield k
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] SCAN error: {e}")
            return

    def scan_leaf_items(self, org: str, sup: str, count: int = 1000) -> Iterator[Dict]:
        """Iterates SCAN pages and fetches values in batches (pipeline)."""
        batch: List[str] = []
        for key in self.scan_leaf_keys(org, sup, count=count):
            batch.append(key)
            if len(batch) >= count:
                yield from self._fetch_batch(batch)
                batch = []
        if batch:
            yield from self._fetch_batch(batch)

    def _fetch_batch(self, keys: List[str]) -> Iterator[Dict]:
        try:
            with self.r.pipeline() as p:
                for k in keys:
                    p.get(k)
                vals = p.execute()
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] pipeline GET error: {e}")
            return

        for k, raw in zip(keys, vals):
            if not raw:
                continue
            try:
                obj = json.loads(raw)
                simple = k.rsplit("meta:leaf:", 1)[-1]
                yield {
                    "simple": simple,
                    "version": int(obj.get("version", -1)),
                    "ts": int(obj.get("ts", 0)),
                    "path": obj.get("path", ""),
                }
            except Exception:
                continue
