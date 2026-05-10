from __future__ import annotations

import logging
import json
from typing import Dict, Iterator, List, Optional, Tuple
from pathlib import Path
from urllib.parse import urlparse
import time


import redis
from redis import Sentinel
from redis.sentinel import MasterNotFoundError



from supertable.config.settings import settings as _cfg
from supertable import redis_keys as RK

logger = logging.getLogger(__name__)
# ------------------------------ Settings ------------------------------

class Settings:
    """Thin adapter that delegates to the central config.settings singleton.

    Attributes that config.settings does not carry (e.g. TEMPLATES_DIR with
    a path derived from __file__) are resolved here.
    """

    def __init__(self) -> None:
        self.SUPERTABLE_ORGANIZATION: str = _cfg.SUPERTABLE_ORGANIZATION
        self.SUPERTABLE_SUPERUSER_TOKEN: str = _cfg.SUPERTABLE_SUPERUSER_TOKEN
        self.SUPERTABLE_SESSION_SECRET: str = _cfg.SUPERTABLE_SESSION_SECRET

        self.SUPERTABLE_REDIS_URL: Optional[str] = _cfg.SUPERTABLE_REDIS_URL or None
        self.SUPERTABLE_REDIS_HOST: str = _cfg.SUPERTABLE_REDIS_HOST
        self.SUPERTABLE_REDIS_PORT: int = _cfg.SUPERTABLE_REDIS_PORT
        self.SUPERTABLE_REDIS_DB: int = _cfg.SUPERTABLE_REDIS_DB
        self.SUPERTABLE_REDIS_PASSWORD: Optional[str] = _cfg.SUPERTABLE_REDIS_PASSWORD or None
        self.SUPERTABLE_REDIS_USERNAME: Optional[str] = _cfg.SUPERTABLE_REDIS_USERNAME or None

        self.SUPERTABLE_REDIS_SENTINEL: Optional[str] = str(_cfg.SUPERTABLE_REDIS_SENTINEL) if _cfg.SUPERTABLE_REDIS_SENTINEL else None
        self.SUPERTABLE_REDIS_SENTINELS: Optional[str] = _cfg.SUPERTABLE_REDIS_SENTINELS or None
        self.SUPERTABLE_REDIS_SENTINEL_MASTER: Optional[str] = _cfg.SUPERTABLE_REDIS_SENTINEL_MASTER or None
        self.SUPERTABLE_REDIS_SENTINEL_PASSWORD: Optional[str] = _cfg.SUPERTABLE_REDIS_SENTINEL_PASSWORD or None
        self.SUPERTABLE_REDIS_SENTINEL_STRICT: Optional[str] = _cfg.SUPERTABLE_REDIS_SENTINEL_STRICT or None

        self.SUPERTABLE_LOGIN_MASK: int = _cfg.SUPERTABLE_LOGIN_MASK

        self.DOTENV_PATH: str = _cfg.DOTENV_PATH

        # TEMPLATES_DIR: use central setting if provided, otherwise derive from __file__
        self.TEMPLATES_DIR: str = (
            _cfg.TEMPLATES_DIR
            or str(Path(__file__).resolve().parent / "webui" / "templates")
        )

        self.SECURE_COOKIES: bool = _cfg.SECURE_COOKIES


settings = Settings()
if settings.SUPERTABLE_LOGIN_MASK not in (1, 2, 3):
    raise RuntimeError(
        f"Invalid SUPERTABLE_LOGIN_MASK (must be 1, 2, or 3): {settings.SUPERTABLE_LOGIN_MASK}"
    )

_missing_envs: List[str] = []
if not settings.SUPERTABLE_ORGANIZATION:
    _missing_envs.append("SUPERTABLE_ORGANIZATION")
if not (settings.SUPERTABLE_SUPERUSER_TOKEN or "").strip():
    _missing_envs.append("SUPERTABLE_SUPERUSER_TOKEN")
if _missing_envs:
    raise RuntimeError("Missing required environment variables: " + ", ".join(_missing_envs))


def _now_ms() -> int:
    from time import time as _t
    return int(_t() * 1000)


# ------------------------------ Catalog (import or fallback) ------------------------------
# All key strings come from supertable.redis_keys (RK).

class _FallbackCatalog:
    def __init__(self, r: redis.Redis):
        self.r = r

    def ensure_root(self, org: str, sup: str) -> None:
        key = RK.meta_root(org, sup)
        if not self.r.exists(key):
            self.r.set(key, json.dumps({"version": 0, "ts": _now_ms()}))

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        raw = self.r.get(RK.meta_root(org, sup))
        return json.loads(raw) if raw else None

    def get_leaf(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        raw = self.r.get(RK.meta_leaf(org, sup, simple))
        return json.loads(raw) if raw else None

    def get_mirrors(self, org: str, sup: str) -> List[str]:
        raw = self.r.get(RK.meta_mirrors(org, sup))
        if not raw:
            return []
        try:
            obj = json.loads(raw)
        except Exception:
            return []
        out = []
        for f in (obj.get("formats") or []):
            fu = str(f).upper()
            if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in out:
                out.append(fu)
        return out

    def set_mirrors(self, org: str, sup: str, formats: List[str]) -> List[str]:
        uniq = []
        for f in (formats or []):
            fu = str(f).upper()
            if fu in ("DELTA", "ICEBERG", "PARQUET") and fu not in uniq:
                uniq.append(fu)
        self.r.set(RK.meta_mirrors(org, sup), json.dumps({"formats": uniq, "ts": _now_ms()}))
        return uniq

    def enable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        if fu not in ("DELTA", "ICEBERG", "PARQUET") or fu in cur:
            return cur
        return self.set_mirrors(org, sup, cur + [fu])

    def disable_mirror(self, org: str, sup: str, fmt: str) -> List[str]:
        cur = self.get_mirrors(org, sup)
        fu = str(fmt).upper()
        nxt = [x for x in cur if x != fu]
        return self.set_mirrors(org, sup, nxt)

    def scan_leaf_keys(self, org: str, sup: str, count: int = 1000) -> Iterator[str]:
        pattern = RK.meta_leaf_pattern(org, sup)
        cursor = 0
        while True:
            cursor, keys = self.r.scan(cursor=cursor, match=pattern, count=max(1, int(count)))
            for k in keys:
                yield k if isinstance(k, str) else k.decode("utf-8")
            if cursor == 0:
                break

    def scan_leaf_items(self, org: str, sup: str, count: int = 1000) -> Iterator[Dict]:
        batch: List[str] = []
        for key in self.scan_leaf_keys(org, sup, count=count):
            batch.append(key)
            if len(batch) >= count:
                yield from self._fetch_batch(batch)
                batch = []
        if batch:
            yield from self._fetch_batch(batch)

    def _fetch_batch(self, keys: List[str]) -> Iterator[Dict]:
        pipe = self.r.pipeline()
        for k in keys:
            pipe.get(k)
        vals = pipe.execute()
        for k, raw in zip(keys, vals):
            if not raw:
                continue
            try:
                obj = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))
                simple = k.rsplit("meta:leaf:", 1)[-1]
                yield {
                    "simple": simple,
                    "version": int(obj.get("version", -1)),
                    "ts": int(obj.get("ts", 0)),
                    "path": obj.get("path", ""),
                }
            except Exception:
                continue

    # -- RBAC methods (mirrors RedisCatalog API using correct rbac: key namespace) --

    @staticmethod
    def _decode_member(m) -> str:
        return m if isinstance(m, str) else m.decode("utf-8")

    def get_users(self, org: str, sup: str) -> List[Dict]:
        users: List[Dict] = []
        try:
            index_key = RK.rbac_user_index(org, sup)
            members = self.r.smembers(index_key)
            for uid_raw in (members or []):
                uid = self._decode_member(uid_raw)
                doc_key = RK.rbac_user_doc(org, sup, uid)
                raw = self.r.hgetall(doc_key)
                if raw:
                    data: Dict = dict(raw)
                    data.setdefault("user_id", uid)
                    data.setdefault("hash", uid)
                    if "roles" in data:
                        try:
                            data["roles"] = json.loads(data["roles"])
                        except (json.JSONDecodeError, TypeError):
                            data["roles"] = []
                    users.append(data)
        except Exception as e:
            logger.warning("_FallbackCatalog.get_users error: %s", e)
        return users

    def get_roles(self, org: str, sup: str) -> List[Dict]:
        roles: List[Dict] = []
        try:
            index_key = RK.rbac_role_index(org, sup)
            members = self.r.smembers(index_key)
            for rid_raw in (members or []):
                rid = self._decode_member(rid_raw)
                doc_key = RK.rbac_role_doc(org, sup, rid)
                raw = self.r.hgetall(doc_key)
                if raw:
                    data: Dict = dict(raw)
                    data.setdefault("role_id", rid)
                    data.setdefault("hash", rid)
                    for field in ("tables", "columns", "filters"):
                        if field in data:
                            try:
                                data[field] = json.loads(data[field])
                            except (json.JSONDecodeError, TypeError):
                                pass
                    roles.append(data)
        except Exception as e:
            logger.warning("_FallbackCatalog.get_roles error: %s", e)
        return roles

    def get_user_details(self, org: str, sup: str, user_id: str) -> Optional[Dict]:
        try:
            doc_key = RK.rbac_user_doc(org, sup, user_id)
            raw = self.r.hgetall(doc_key)
            if not raw:
                return None
            data: Dict = dict(raw)
            if "roles" in data:
                try:
                    data["roles"] = json.loads(data["roles"])
                except (json.JSONDecodeError, TypeError):
                    data["roles"] = []
            return data
        except Exception as e:
            logger.warning("_FallbackCatalog.get_user_details error: %s", e)
        return None

    def get_role_details(self, org: str, sup: str, role_id: str) -> Optional[Dict]:
        try:
            doc_key = RK.rbac_role_doc(org, sup, role_id)
            raw = self.r.hgetall(doc_key)
            if not raw:
                return None
            data: Dict = dict(raw)
            for field in ("tables", "columns", "filters"):
                if field in data:
                    try:
                        data[field] = json.loads(data[field])
                    except (json.JSONDecodeError, TypeError):
                        pass
            return data
        except Exception as e:
            logger.warning("_FallbackCatalog.get_role_details error: %s", e)
        return None

    def rbac_get_user_id_by_username(self, org: str, sup: str, username: str) -> Optional[str]:
        try:
            name_map_key = RK.rbac_username_to_id(org, sup)
            val = self.r.hget(name_map_key, username.lower())
            if val is None:
                return None
            return self._decode_member(val)
        except Exception as e:
            logger.warning("_FallbackCatalog.rbac_get_user_id_by_username error: %s", e)
        return None


def _coerce_password(pw: Optional[str]) -> Optional[str]:
    if pw is None:
        return None
    v = pw.strip()
    # Treat these as "no password"
    if v in ("", "None", "none", "null", "NULL"):
        return None
    return v


def _build_redis_client() -> redis.Redis:
    """
    Build a Redis client from SUPERTABLE_* envs.

    Precedence:
      1) SUPERTABLE_REDIS_URL (parsed)
      2) SUPERTABLE_REDIS_HOST/PORT/DB/PASSWORD (overrides URL parts if provided)

    Sentinel:
      If SUPERTABLE_REDIS_SENTINEL is enabled and SUPERTABLE_REDIS_SENTINELS is set,
      use the same Sentinel connection behavior as RedisCatalog (ping-probe + optional fallback).
    """
    settings = Settings()
    url = (settings.SUPERTABLE_REDIS_URL or "").strip() or None

    host = settings.SUPERTABLE_REDIS_HOST
    port = settings.SUPERTABLE_REDIS_PORT
    db = settings.SUPERTABLE_REDIS_DB
    username = (settings.SUPERTABLE_REDIS_USERNAME or "").strip() or None
    password = _coerce_password(settings.SUPERTABLE_REDIS_PASSWORD)

    use_ssl = url.startswith("rediss://") if url else False

    if url:
        u = urlparse(url)
        if u.scheme not in ("redis", "rediss"):
            raise ValueError(f"Unsupported Redis URL scheme: {u.scheme}")
        # Extract from URL
        if u.hostname:
            host = u.hostname
        if u.port:
            port = u.port
        # db from path: "/0", "/1", ...
        if u.path and len(u.path) > 1:
            try:
                db_from_url = int(u.path.lstrip("/"))
                db = db_from_url
            except Exception:
                pass
        if u.username:
            username = u.username
        if u.password:
            password = _coerce_password(u.password)

    # Sentinel detection + options
    sentinel_enabled = (settings.SUPERTABLE_REDIS_SENTINEL or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    sentinel_strict = (settings.SUPERTABLE_REDIS_SENTINEL_STRICT or "").strip().lower() in (
        "1",
        "true",
        "yes",
        "y",
        "on",
    )
    sentinel_master = (settings.SUPERTABLE_REDIS_SENTINEL_MASTER or "").strip() or "mymaster"

    sentinel_password = _coerce_password(settings.SUPERTABLE_REDIS_SENTINEL_PASSWORD) or password

    # Single-password setup: if Sentinel is enabled and Redis password is not set, reuse Sentinel password.
    if sentinel_enabled and password is None and sentinel_password:
        password = sentinel_password

    # If username is set but password is None, drop username (ACL requires both)
    if username and not password:
        username = None

    sentinel_hosts: List[Tuple[str, int]] = []
    sentinel_raw = (settings.SUPERTABLE_REDIS_SENTINELS or "").strip()
    if sentinel_raw:
        for part in sentinel_raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                h, p = part.split(":")
                sentinel_hosts.append((h.strip(), int(p)))
            except ValueError:
                # Keep behavior non-fatal; invalid entries are ignored.
                continue

    if sentinel_enabled and sentinel_hosts:
        sentinel_kwargs: dict = {
            "socket_timeout": 0.5,
            "decode_responses": True,
            "ssl": use_ssl,
        }
        if sentinel_password:
            sentinel_kwargs["password"] = sentinel_password
        if username:
            sentinel_kwargs["username"] = username

        sentinel = Sentinel(
            sentinel_hosts,
            sentinel_kwargs=sentinel_kwargs,
            socket_timeout=0.5,
            decode_responses=True,
            ssl=use_ssl,
            username=username,
            password=password,
        )

        sentinel_client: redis.Redis = sentinel.master_for(
            sentinel_master,
            db=db,
            decode_responses=True,
            ssl=use_ssl,
            username=username,
            password=password,
        )

        # Fail-fast probe (matches RedisCatalog behavior).
        sentinel_err: Optional[BaseException] = None
        deadline = time.time() + 3.0
        while time.time() < deadline:
            try:
                sentinel_client.ping()
                sentinel_err = None
                break
            except (MasterNotFoundError, redis.RedisError, OSError) as e:
                sentinel_err = e
                time.sleep(0.2)

        if sentinel_err is None:
            return sentinel_client

        if sentinel_strict:
            raise sentinel_err

        # Non-strict fallback to standard Redis
        return redis.Redis(
            host=host,
            port=port,
            db=db,
            password=password,
            username=username,
            decode_responses=True,
            ssl=use_ssl,
        )

    # Standard Redis mode
    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        username=username,
        decode_responses=True,
        ssl=use_ssl,
    )


def _build_catalog() -> Tuple[object, redis.Redis]:
    r = _build_redis_client()
    try:
        from supertable.redis_catalog import RedisCatalog as _RC  # type: ignore
        return _RC(), r
    except Exception:
        try:
            from redis_catalog import RedisCatalog as _RC  # type: ignore
            return _RC(), r
        except Exception:
            return _FallbackCatalog(r), r


catalog, redis_client = _build_catalog()
