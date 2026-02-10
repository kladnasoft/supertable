from __future__ import annotations

import json
import time
import uuid
import hashlib
import secrets
from typing import Any, Dict, Iterator, List, Optional

import redis
from supertable.config.defaults import logger

try:
    from .redis_connector import RedisConnector, RedisOptions
except ImportError:  # pragma: no cover
    from redis_connector import RedisConnector, RedisOptions

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


def _auth_tokens_key(org: str) -> str:
    # Store hashed tokens (sha256) -> JSON metadata in a single Redis hash.
    # Value example: {"created_ms": 123, "created_by": "superuser", "label": "dev", "enabled": true}
    return f"supertable:{org}:auth:tokens"
def _role_type_to_hash_key(org: str, sup: str, role_type: str) -> str:
    return f"supertable:{org}:{sup}:meta:roles:type_to_hash:{role_type}"

# --------------------------------------------------------------------------- #
# Staging / Pipe meta keys (Redis-backed UI listing)
# --------------------------------------------------------------------------- #

def _staging_key(org: str, sup: str, staging_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}"


def _stagings_meta_key(org: str, sup: str) -> str:
    # Set of staging names for fast listing
    return f"supertable:{org}:{sup}:meta:staging:meta"


def _pipe_key(org: str, sup: str, staging_name: str, pipe_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:{pipe_name}"


def _pipes_meta_key(org: str, sup: str, staging_name: str) -> str:
    # Set of pipe names for a staging for fast listing
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:meta"




def _stage_lock_key(org: str, sup: str, stage_name: str) -> str:
    return f"supertable:{org}:{sup}:lock:stage:{stage_name}"


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


    _LUA_LEAF_PAYLOAD_CAS_SET = """
local key = KEYS[1]
local payload_json = ARGV[1]
local new_path = ARGV[2]
local now_ms = tonumber(ARGV[3])

local cur = redis.call('GET', key)
local old_version = -1
if cur then
  local ok, obj = pcall(cjson.decode, cur)
  if ok and obj and obj['version'] then
    old_version = tonumber(obj['version'])
  end
end
local new_version = old_version + 1

local payload = {}
local okp, pobj = pcall(cjson.decode, payload_json)
if okp and pobj then
  payload = pobj
end

local new_val = cjson.encode({version=new_version, ts=now_ms, path=new_path, payload=payload})
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
        self.r = RedisConnector(options).r

        # Register scripts
        self._leaf_cas_set = self.r.register_script(self._LUA_LEAF_CAS_SET)
        self._leaf_payload_cas_set = self.r.register_script(self._LUA_LEAF_PAYLOAD_CAS_SET)
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

    # ---- Stage lock (staging + pipe mutations) ----

    def acquire_stage_lock(
        self,
        org: str,
        sup: str,
        stage_name: str,
        ttl_s: int = 30,
        timeout_s: int = 30,
    ) -> Optional[str]:
        """Acquire lock for staging/pipe operations:
            supertable:{org}:{sup}:lock:stage:{stage_name}
        """
        key = _stage_lock_key(org, sup, stage_name)
        token = uuid.uuid4().hex
        deadline = time.time() + max(1, int(timeout_s))
        while time.time() < deadline:
            try:
                ok = self.r.set(key, token, nx=True, ex=max(1, int(ttl_s)))
                if ok:
                    return token
            except redis.RedisError as e:
                logger.debug(f"[redis-stage-lock] acquire error on {key}: {e}")
            time.sleep(0.05)
        return None

    def release_stage_lock(self, org: str, sup: str, stage_name: str, token: str) -> bool:
        """Release stage lock if token matches."""
        try:
            res = self._lock_release_if_token(keys=[_stage_lock_key(org, sup, stage_name)], args=[token])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-stage-lock] release error: {e}")
            return False

    def extend_stage_lock(self, org: str, sup: str, stage_name: str, token: str, ttl_ms: int) -> bool:
        """Optionally extend stage lock TTL if token matches."""
        try:
            res = self._lock_extend_if_token(keys=[_stage_lock_key(org, sup, stage_name)], args=[token, int(ttl_ms)])
            return int(res or 0) == 1
        except redis.RedisError as e:
            logger.debug(f"[redis-stage-lock] extend error: {e}")
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

    

    def set_leaf_payload_cas(
        self,
        org: str,
        sup: str,
        simple: str,
        payload: Dict[str, Any],
        path: str,
        now_ms: Optional[int] = None,
    ) -> int:
        """Atomically write a leaf pointer *and* snapshot payload (so readers avoid storage reads)."""
        try:
            payload_json = json.dumps(payload or {})
        except Exception:
            payload_json = "{}"

        try:
            return int(
                self._leaf_payload_cas_set(
                    keys=[_leaf_key(org, sup, simple)],
                    args=[payload_json, path, int(now_ms or _now_ms())],
                )
                or 0
            )
        except AttributeError:
            # Backward compatible fallback if script isn't registered for some reason.
            return self.set_leaf_path_cas(org, sup, simple, path, now_ms=now_ms)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] leaf_payload_cas_set error: {e}")
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
                    key_str = key if isinstance(key, str) else key.decode('utf-8')
                    # Skip name_to_hash and meta keys
                    if "name_to_hash" in key_str or ("meta" in key_str and "users:meta" not in key_str):
                        continue
                    raw = self.r.get(key_str)
                    if raw:
                        try:
                            user_data = json.loads(raw)
                            if isinstance(user_data, dict):
                                user_hash = key_str.split(':')[-1]
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
                    key_str = key if isinstance(key, str) else key.decode('utf-8')
                    # Skip type_to_hash and meta keys
                    if "type_to_hash" in key_str or ("meta" in key_str and "roles:meta" not in key_str):
                        continue
                    raw = self.r.get(key_str)
                    if raw:
                        try:
                            role_data = json.loads(raw)
                            if isinstance(role_data, dict):
                                role_hash = key_str.split(':')[-1]
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


    # ------------- Organization auth tokens (login tokens) -------------

    def list_auth_tokens(self, org: str) -> List[Dict[str, Any]]:
        """List auth tokens for an organization (tokens are stored hashed; only token_id is returned)."""
        key = _auth_tokens_key(org)
        out: List[Dict[str, Any]] = []
        try:
            raw_map = self.r.hgetall(key) or {}
            for token_id, raw in raw_map.items():
                token_id_str = token_id if isinstance(token_id, str) else token_id.decode('utf-8')
                try:
                    meta = json.loads(raw) if raw else {}
                except Exception:
                    if isinstance(raw, bytes):
                        meta = {"value": raw.decode('utf-8', errors='replace')}
                    else:
                        meta = {"value": raw}
                if isinstance(meta, dict):
                    meta = dict(meta)
                else:
                    meta = {"value": meta}
                meta.setdefault("token_id", token_id_str)
                out.append(meta)
            out.sort(key=lambda x: int(x.get("created_ms") or 0), reverse=True)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_auth_tokens error: {e}")
        return out

    def create_auth_token(
        self,
        org: str,
        created_by: str,
        label: Optional[str] = None,
        enabled: bool = True,
    ) -> Dict[str, Any]:
        """Create a new auth token.

        The plaintext token is returned ONLY once. Redis stores only token_id (sha256(token)).
        """
        token = secrets.token_urlsafe(24)
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        meta = {
            "token_id": token_id,
            "created_ms": _now_ms(),
            "created_by": str(created_by or ""),
            "label": (str(label).strip() if label is not None else ""),
            "enabled": bool(enabled),
        }
        try:
            self.r.hset(_auth_tokens_key(org), token_id, json.dumps(meta))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] create_auth_token error: {e}")
            raise
        return {"token": token, **meta}

    def delete_auth_token(self, org: str, token_id: str) -> bool:
        """Delete an auth token by token_id (sha256)."""
        if not token_id:
            return False
        try:
            return bool(self.r.hdel(_auth_tokens_key(org), token_id))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_auth_token error: {e}")
            return False

    def validate_auth_token(self, org: str, token: str) -> bool:
        """Validate a plaintext auth token."""
        if not token:
            return False
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        try:
            return bool(self.r.hexists(_auth_tokens_key(org), token_id))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] validate_auth_token error: {e}")
            return False


# ------------- Listings via SCAN -------------

    def scan_leaf_keys(self, org: str, sup: str, count: int = 1000) -> Iterator[str]:
        """Yields full Redis keys: supertable:{org}:{sup}:meta:leaf:*"""
        pattern = f"supertable:{org}:{sup}:meta:leaf:*"
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys:
                    yield k if isinstance(k, str) else k.decode('utf-8')
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
                    "payload": obj.get("payload"),
                }
            except Exception:
                continue


# ------------- Deletions (dangerous) -------------

    def delete_simple_table(self, org: str, sup: str, simple: str) -> bool:
        """Delete a simple table's Redis meta (leaf pointer + lock).

        This does **not** delete storage. Callers should delete storage first, then call this.
        """
        if not (org and sup and simple):
            return False
        keys = [_leaf_key(org, sup, simple), _lock_key(org, sup, simple)]
        try:
            # DEL returns number of keys removed
            self.r.delete(*keys)
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_simple_table error: {e}")
            return False

    def delete_super_table(self, org: str, sup: str, count: int = 1000) -> int:
        """Delete **all** Redis keys for a given supertable (meta + locks + RBAC, etc).

        This is implemented via SCAN to avoid blocking Redis.
        Returns the number of keys deleted (best-effort).
        """
        if not (org and sup):
            return 0
        pattern = f"supertable:{org}:{sup}:*"
        return self._delete_by_scan(pattern=pattern, count=count)

    def _delete_by_scan(self, pattern: str, count: int = 1000) -> int:
        deleted = 0
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                # redis-py may return list[str] or list[bytes]
                str_keys = [k if isinstance(k, str) else k.decode("utf-8") for k in (keys or [])]
                if str_keys:
                    try:
                        with self.r.pipeline() as p:
                            for k in str_keys:
                                p.delete(k)
                            res = p.execute()
                        # Each delete returns 0/1, sum them
                        deleted += sum(int(x or 0) for x in res)
                    except redis.RedisError as e:
                        logger.error(f"[redis-catalog] pipeline DEL error: {e}")
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] SCAN delete error: {e}")
        return deleted


# --------------------------------------------------------------------------- #
# Staging / Pipe meta (for website UI)
# --------------------------------------------------------------------------- #

    def upsert_staging_meta(self, org: str, sup: str, staging_name: str, meta: Dict[str, Any]) -> bool:
        """Upsert staging metadata and ensure it is indexed for listing."""
        if not (org and sup and staging_name):
            return False
        payload = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload["updated_at_ms"] = _now_ms()

        try:
            with self.r.pipeline() as p:
                p.set(_staging_key(org, sup, staging_name), json.dumps(payload))
                p.sadd(_stagings_meta_key(org, sup), staging_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_staging_meta error: {e}")
            raise

    def staging_exists(self, org: str, sup: str, staging_name: str) -> bool:
        """Fast existence check for a staging (Redis-backed)."""
        if not (org and sup and staging_name):
            return False
        try:
            return bool(self.r.exists(_staging_key(org, sup, staging_name)))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] staging_exists error: {e}")
            return False

    def get_staging_meta(self, org: str, sup: str, staging_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name):
            return None
        try:
            raw = self.r.get(_staging_key(org, sup, staging_name))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_staging_meta error: {e}")
            return None
        if not raw:
            return None
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def list_stagings(self, org: str, sup: str, *, count: int = 1000) -> List[str]:
        """List staging names. Prefers the staging index set; falls back to SCAN."""
        if not (org and sup):
            return []
        try:
            names = list(self.r.smembers(_stagings_meta_key(org, sup)) or [])
            if names:
                return sorted({(n if isinstance(n, str) else n.decode('utf-8')) for n in names if n})
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_stagings smembers error: {e}")

        # Fallback (migration/back-compat): scan exact staging meta keys.
        pattern = f"supertable:{org}:{sup}:meta:staging:*"
        seen = set()
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys or []:
                    kk = k if isinstance(k, str) else k.decode("utf-8")
                    if ":pipe:" in kk:
                        continue
                    if kk.endswith(":meta"):
                        continue
                    name = kk.rsplit("meta:staging:", 1)[-1]
                    if name:
                        seen.add(name)
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_stagings scan error: {e}")
        return sorted(seen)

    def delete_staging_meta(self, org: str, sup: str, staging_name: str, *, count: int = 1000) -> int:
        """Delete staging meta and *all* related keys under the staging prefix.

        This removes the staging from the staging index set, deletes the staging meta key,
        and deletes any keys matching:
            supertable:{org}:{sup}:meta:staging:{staging_name}:*
        Returns number of keys deleted (best-effort; does not include SREM).
        """
        if not (org and sup and staging_name):
            return 0

        deleted = 0
        try:
            # Remove from list index
            self.r.srem(_stagings_meta_key(org, sup), staging_name)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_staging_meta srem error: {e}")

        try:
            # Delete the base meta key
            deleted += int(self.r.delete(_staging_key(org, sup, staging_name)) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_staging_meta del error: {e}")

        # Delete everything under the staging namespace (pipes, pipe meta set, etc.)
        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:*"
        deleted += self._delete_by_scan(pattern=pattern, count=count)
        return deleted

    def upsert_pipe_meta(
        self,
        org: str,
        sup: str,
        staging_name: str,
        pipe_name: str,
        meta: Dict[str, Any],
    ) -> bool:
        """Upsert pipe metadata and ensure it is indexed for listing under a staging."""
        if not (org and sup and staging_name and pipe_name):
            return False
        payload = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload.setdefault("pipe_name", pipe_name)
        payload["updated_at_ms"] = _now_ms()

        try:
            with self.r.pipeline() as p:
                p.set(_pipe_key(org, sup, staging_name, pipe_name), json.dumps(payload))
                p.sadd(_pipes_meta_key(org, sup, staging_name), pipe_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_pipe_meta error: {e}")
            raise

    def get_pipe_meta(self, org: str, sup: str, staging_name: str, pipe_name: str) -> Optional[Dict[str, Any]]:
        if not (org and sup and staging_name and pipe_name):
            return None
        try:
            raw = self.r.get(_pipe_key(org, sup, staging_name, pipe_name))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_pipe_meta error: {e}")
            return None
        if not raw:
            return None
        try:
            obj = json.loads(raw)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None

    def get_pipe_defs(self, org: str, sup: str, staging_name: str, pipe_name: str) -> List[Dict[str, Any]]:
        """Read a pipe definition payload stored as a JSON **list** (spec).

        Back-compat: if a dict is stored, it is returned as a single-item list.
        """
        if not (org and sup and staging_name and pipe_name):
            return []
        try:
            raw = self.r.get(_pipe_key(org, sup, staging_name, pipe_name))
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] get_pipe_defs error: {e}")
            return []
        if not raw:
            return []
        try:
            obj = json.loads(raw)
        except Exception:
            return []
        if isinstance(obj, list):
            return [x for x in obj if isinstance(x, dict)]
        if isinstance(obj, dict):
            return [obj]
        return []

    def upsert_pipe_defs(
        self,
        org: str,
        sup: str,
        staging_name: str,
        pipe_name: str,
        defs: List[Dict[str, Any]],
    ) -> bool:
        """Store a pipe definition payload as a JSON **list** and index it for listing."""
        if not (org and sup and staging_name and pipe_name):
            return False
        safe_defs = [d for d in (defs or []) if isinstance(d, dict)]
        try:
            with self.r.pipeline() as p:
                p.set(_pipe_key(org, sup, staging_name, pipe_name), json.dumps(safe_defs))
                p.sadd(_pipes_meta_key(org, sup, staging_name), pipe_name)
                p.execute()
            return True
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] upsert_pipe_defs error: {e}")
            raise


    def list_pipe_metas(self, org: str, sup: str, staging_name: str, *, count: int = 1000) -> List[Dict[str, Any]]:
        """List pipe metadata objects for a staging (back-compat).

        This is primarily used by SuperPipe to check for existing pipes.
        If a pipe has no stored dict meta (e.g. only defs list exists), a minimal
        payload with at least "pipe_name" is returned.
        """
        if not (org and sup and staging_name):
            return []
        out_metas: List[Dict[str, Any]] = []
        for name in self.list_pipes(org, sup, staging_name, count=count):
            meta = self.get_pipe_meta(org, sup, staging_name, name)
            if isinstance(meta, dict) and meta:
                out_metas.append(meta)
            else:
                out_metas.append(
                    {
                        "organization": org,
                        "super_name": sup,
                        "staging_name": staging_name,
                        "pipe_name": name,
                    }
                )
        return out_metas

    def list_pipes(self, org: str, sup: str, staging_name: str, *, count: int = 1000) -> List[str]:
        """List pipe names for a staging. Prefers the pipe index set; falls back to SCAN."""
        if not (org and sup and staging_name):
            return []
        try:
            names = list(self.r.smembers(_pipes_meta_key(org, sup, staging_name)) or [])
            if names:
                return sorted({(n if isinstance(n, str) else n.decode('utf-8')) for n in names if n})
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_pipes smembers error: {e}")

        # Fallback: scan pipe definition keys.
        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:*"
        seen = set()
        cursor = 0
        try:
            while True:
                cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
                for k in keys or []:
                    kk = k if isinstance(k, str) else k.decode("utf-8")
                    if kk.endswith(":meta"):
                        continue
                    name = kk.rsplit(":pipe:", 1)[-1]
                    if name:
                        seen.add(name)
                if cursor == 0:
                    break
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] list_pipes scan error: {e}")
        return sorted(seen)

    def delete_pipe_meta(self, org: str, sup: str, staging_name: str, pipe_name: str) -> int:
        """Delete a pipe meta key and remove it from the pipe index set."""
        if not (org and sup and staging_name and pipe_name):
            return 0
        try:
            self.r.srem(_pipes_meta_key(org, sup, staging_name), pipe_name)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_pipe_meta srem error: {e}")
        try:
            return int(self.r.delete(_pipe_key(org, sup, staging_name, pipe_name)) or 0)
        except redis.RedisError as e:
            logger.error(f"[redis-catalog] delete_pipe_meta del error: {e}")
            return 0
