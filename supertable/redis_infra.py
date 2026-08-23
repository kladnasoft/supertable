from __future__ import annotations

import logging
import json
from typing import Dict, Iterator, List, Optional, Tuple
from pathlib import Path


import redis



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

def _require_runtime_env() -> None:
    """Validate the deployment identity needed to talk to Redis.

    Called lazily by code paths that open a real Redis connection or
    otherwise need the deployment's organization. Redis authentication is
    validated independently by ``RedisOptions``; the SuperTable HTTP/API
    superuser token is neither a Redis credential nor used by this module.
    Importing the SDK no longer fails when these are unset — only
    running against Redis does. This keeps the module importable in
    test, build, and inspection contexts where the runtime
    credentials aren't (and shouldn't be) present.
    """
    missing: List[str] = []
    if not settings.SUPERTABLE_ORGANIZATION:
        missing.append("SUPERTABLE_ORGANIZATION")
    if missing:
        raise RuntimeError(
            "Missing required environment variables: " + ", ".join(missing)
        )


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
                simple = k.rsplit("meta:leaf:doc:", 1)[-1]
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


def _build_redis_client() -> redis.Redis:
    """Return the process-shared, strictly validated Redis client.

    Redis endpoint parsing, TLS verification, Sentinel validation, and strict
    no-fallback behavior have one authority boundary in ``redis_connector``.
    Keeping a second parser here previously let audit consumers silently use a
    different and weaker connection policy.
    """
    _require_runtime_env()
    from supertable.redis_connector import RedisOptions, create_redis_client

    return create_redis_client(RedisOptions())


def _build_catalog() -> Tuple[object, redis.Redis]:
    r = _build_redis_client()
    try:
        from supertable.redis_catalog import RedisCatalog as _RC  # type: ignore
    except ImportError:
        try:
            from redis_catalog import RedisCatalog as _RC  # type: ignore
        except ImportError:
            return _FallbackCatalog(r), r

    # Inject the already validated shared client.  Audit consumers importing
    # ``redis_client`` and the catalog must never talk through different pools
    # or connection policies.
    catalog = _RC(redis_client=r)
    if getattr(catalog, "r", None) is not r:
        raise RuntimeError("RedisCatalog did not retain the hardened Redis client")
    return catalog, r


catalog, redis_client = _build_catalog()
