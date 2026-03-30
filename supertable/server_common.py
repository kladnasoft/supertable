from __future__ import annotations

import logging
import json
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, Tuple, Set, Any
from pathlib import Path
from urllib.parse import urlparse
import hashlib
import secrets
import time


import redis
from fastapi import APIRouter, HTTPException, Response
from redis import Sentinel
from redis.sentinel import MasterNotFoundError



from supertable.config.settings import settings as _cfg

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


# _required_token() — moved to supertable.api.session (imported above)

_missing_envs: List[str] = []
if not settings.SUPERTABLE_ORGANIZATION:
    _missing_envs.append("SUPERTABLE_ORGANIZATION")
if not (settings.SUPERTABLE_SUPERUSER_TOKEN or "").strip():
    _missing_envs.append("SUPERTABLE_SUPERUSER_TOKEN")
if _missing_envs:
    raise RuntimeError("Missing required environment variables: " + ", ".join(_missing_envs))

# Derive superuser identity hash from organization name.
# The superuser is always named "superuser" — hash = sha256("{org}:superuser").
from supertable.api.session import (  # noqa: E402
    derive_superuser_hash,
    SUPERUSER_USERNAME,
    _SESSION_COOKIE_NAME,
    _ADMIN_COOKIE_NAME,
    _SESSION_MAX_AGE_SECONDS,
    _required_token,
    _session_secret,
    _b64url_encode,
    _b64url_decode,
    _sign_payload,
    _encode_session,
    _decode_session,
    get_session,
    _set_session_cookie,
    _clear_session_cookie,
    is_logged_in,
    is_superuser,
    _is_authorized,
    _get_provided_token,
    session_context,
    inject_session_into_ctx,
    _sha256_hex,
    _user_hash,
    logged_in_guard_api,
    admin_guard_api,
)
import supertable.api.session as session  # noqa: E402

settings.SUPERTABLE_SUPERHASH = derive_superuser_hash(settings.SUPERTABLE_ORGANIZATION)

session.configure(
    superuser_token=settings.SUPERTABLE_SUPERUSER_TOKEN,
    session_secret=settings.SUPERTABLE_SESSION_SECRET,
    secure_cookies=settings.SECURE_COOKIES,
    superuser_hash=settings.SUPERTABLE_SUPERHASH,
)






# ------------------------------ Session & auth (supertable.api.session) ---------
# Session cookie, auth checks, guards, and identity helpers are implemented in
# supertable.api.session and imported above for backward compatibility.
# Moved: _SESSION_COOKIE_NAME, _ADMIN_COOKIE_NAME, _SESSION_MAX_AGE_SECONDS,
#   _session_secret, _b64url_encode, _b64url_decode, _sign_payload,
#   _encode_session, _decode_session, get_session, _set_session_cookie,
#   _clear_session_cookie, is_logged_in, is_superuser, session_context,
#   inject_session_into_ctx, _sha256_hex, _user_hash,
#   _is_authorized, _get_provided_token, logged_in_guard_api, admin_guard_api
# ---------------------------------------------------------------------------


def _now_ms() -> int:
    from time import time as _t
    return int(_t() * 1000)


# ------------------------------ Catalog (import or fallback) ------------------------------

def _root_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:root"


def _leaf_key(org: str, sup: str, simple: str) -> str:
    return f"supertable:{org}:{sup}:meta:leaf:{simple}"


def _mirrors_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:mirrors"


class _FallbackCatalog:
    def __init__(self, r: redis.Redis):
        self.r = r

    def ensure_root(self, org: str, sup: str) -> None:
        key = _root_key(org, sup)
        if not self.r.exists(key):
            self.r.set(key, json.dumps({"version": 0, "ts": _now_ms()}))

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        raw = self.r.get(_root_key(org, sup))
        return json.loads(raw) if raw else None

    def get_leaf(self, org: str, sup: str, simple: str) -> Optional[Dict]:
        raw = self.r.get(_leaf_key(org, sup, simple))
        return json.loads(raw) if raw else None

    def get_mirrors(self, org: str, sup: str) -> List[str]:
        raw = self.r.get(_mirrors_key(org, sup))
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
        self.r.set(_mirrors_key(org, sup), json.dumps({"formats": uniq, "ts": _now_ms()}))
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
        pattern = f"supertable:{org}:{sup}:meta:leaf:*"
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
            index_key = f"supertable:{org}:{sup}:rbac:users:index"
            members = self.r.smembers(index_key)
            for uid_raw in (members or []):
                uid = self._decode_member(uid_raw)
                doc_key = f"supertable:{org}:{sup}:rbac:users:doc:{uid}"
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
            index_key = f"supertable:{org}:{sup}:rbac:roles:index"
            members = self.r.smembers(index_key)
            for rid_raw in (members or []):
                rid = self._decode_member(rid_raw)
                doc_key = f"supertable:{org}:{sup}:rbac:roles:doc:{rid}"
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
            doc_key = f"supertable:{org}:{sup}:rbac:users:doc:{user_id}"
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
            doc_key = f"supertable:{org}:{sup}:rbac:roles:doc:{role_id}"
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
            name_map_key = f"supertable:{org}:{sup}:rbac:users:name_to_id"
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


# ------------------------------ Discovery & Utils ------------------------------

def discover_pairs(limit_pairs: int = 10000) -> List[Tuple[str, str]]:
    pairs: Set[Tuple[str, str]] = set()
    cursor = 0
    pattern = "supertable:*:*:meta:*"
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
        for k in keys:
            s = k if isinstance(k, str) else k.decode("utf-8")
            parts = s.split(":")
            if len(parts) >= 5 and parts[0] == "supertable" and parts[3] == "meta":
                pairs.add((parts[1], parts[2]))
                if len(pairs) >= limit_pairs:
                    break
        if cursor == 0 or len(pairs) >= limit_pairs:
            break
    return sorted(pairs)


def resolve_pair(org: Optional[str], sup: Optional[str]) -> Tuple[str, str]:
    pairs = discover_pairs()
    if org and sup:
        return org, sup
    if org and not sup:
        for o, s in pairs:
            if o == org:
                return o, s
    if sup and not org:
        for o, s in pairs:
            if s == sup:
                return o, s
    if not pairs:
        return "", ""
    return pairs[0]


def _fmt_ts(ms: int) -> str:
    if not ms:
        return "—"
    try:
        dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        return dt.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
    except Exception:
        return str(ms)


# ------------------------------ Auth helpers (supertable.api.session) ------
# _get_provided_token, _is_authorized — moved to session.py (imported above)
# ---------------------------------------------------------------------------


def _no_store(resp: Response):
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    _security_headers(resp)


def _security_headers(resp: Response) -> None:
    # Keep this strict-but-compatible (the UI uses inline styles/scripts).
    resp.headers.setdefault("X-Content-Type-Options", "nosniff")
    resp.headers.setdefault("X-Frame-Options", "DENY")
    resp.headers.setdefault("Referrer-Policy", "same-origin")
    resp.headers.setdefault("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
    resp.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline' https:; "
        "script-src 'self' 'unsafe-inline' https:; connect-src 'self' https:; font-src 'self' data: https:; base-uri 'self'; frame-ancestors 'none'",
    )



# logged_in_guard_api, admin_guard_api — moved to session.py (imported above)


# ------------------------------ Users/Roles readers ------------------------------

def _r_type(key: str) -> str:
    try:
        return redis_client.type(key)
    except Exception:
        return "none"


def _read_string_json(key: str) -> Optional[Dict]:
    raw = redis_client.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return {"value": raw}


def _read_hash(key: str) -> Optional[Dict]:
    try:
        data = redis_client.hgetall(key)
        return data or None
    except Exception:
        return None


def list_users(org: str, sup: str) -> List[Dict]:
    """Return all users via catalog.get_users() (RBAC index-based).

    Normalizes each doc to always have 'user_id' and 'hash' fields.
    """
    try:
        if hasattr(catalog, "get_users"):
            users = catalog.get_users(org, sup)
            # Normalize: ensure user_id and hash are always present
            for u in users:
                uid = u.get("user_id") or u.get("hash") or ""
                u.setdefault("user_id", uid)
                u.setdefault("hash", uid)
            return users
    except Exception as e:
        logger.warning("catalog.get_users failed (%s/%s): %s — falling back to legacy scan", org, sup, e)
    return _list_users_legacy(org, sup)


def _list_users_legacy(org: str, sup: str) -> List[Dict]:
    """Legacy Redis-scan fallback for old meta:users: namespace."""
    out: List[Dict] = []
    pattern = f"supertable:{org}:{sup}:meta:users:*"
    cursor = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
        for key in keys:
            k = key if isinstance(key, str) else key.decode("utf-8")
            tail = k.rsplit(":", 1)[-1]
            if tail in ("meta", "name_to_hash"):
                continue
            t = _r_type(k)
            doc = None
            if t == "string":
                doc = _read_string_json(k)
            elif t == "hash":
                doc = _read_hash(k)
            else:
                continue
            if doc is None:
                continue
            name = doc.get("name") if isinstance(doc, dict) else None
            roles = doc.get("roles") if isinstance(doc, dict) else None
            if isinstance(roles, str):
                try:
                    roles = json.loads(roles)
                except Exception:
                    roles = [roles]
            if roles is None:
                roles = []
            out.append({"hash": tail, **doc, "name": name, "roles": roles})
        if cursor == 0:
            break
    return out


def list_roles(org: str, sup: str) -> List[Dict]:
    """Return all roles via catalog.get_roles() (RBAC index-based)."""
    try:
        if hasattr(catalog, "get_roles"):
            return catalog.get_roles(org, sup)
    except Exception as e:
        logger.warning("catalog.get_roles failed (%s/%s): %s", org, sup, e)
    return []


def read_user(org: str, sup: str, user_id: str) -> Optional[Dict]:
    """Read a single user document by user_id."""
    try:
        if hasattr(catalog, "get_user_details"):
            doc = catalog.get_user_details(org, sup, user_id)
            if doc and isinstance(doc, dict):
                return doc
    except Exception:
        pass
    # Fallback: direct Redis lookup (legacy key namespace)
    key = f"supertable:{org}:{sup}:meta:users:{user_id}"
    t = _r_type(key)
    if t == "string":
        return _read_string_json(key)
    if t == "hash":
        return _read_hash(key)
    return None


def read_role(org: str, sup: str, role_id: str) -> Optional[Dict]:
    """Read a single role document by role_id."""
    try:
        if hasattr(catalog, "get_role_details"):
            doc = catalog.get_role_details(org, sup, role_id)
            if doc and isinstance(doc, dict):
                return doc
    except Exception:
        pass
    # Fallback: direct Redis lookup (legacy key namespace)
    key = f"supertable:{org}:{sup}:meta:roles:{role_id}"
    t = _r_type(key)
    if t == "string":
        return _read_string_json(key)
    if t == "hash":
        return _read_hash(key)
    return None


def get_user_roles(org: str, sup: str, username: str) -> List[Dict]:
    """Get resolved roles for a specific user by username.

    Flow: catalog.rbac_get_user_id_by_username() → user doc → role IDs
          → catalog.get_role_details() for each.
    """
    try:
        # Step 1: username → user_id
        user_id: Optional[str] = None
        if hasattr(catalog, "rbac_get_user_id_by_username"):
            user_id = catalog.rbac_get_user_id_by_username(org, sup, username)

        if not user_id:
            # Fallback: scan all users and match by name (case-insensitive)
            all_users = list_users(org, sup)
            uname_lower = (username or "").lower()
            for u in all_users:
                n = (u.get("username") or u.get("name") or "").lower()
                if n == uname_lower:
                    user_id = u.get("user_id") or u.get("hash") or ""
                    break
            if not user_id:
                return []

        # Step 2: user_id → user doc → role IDs
        user_doc = read_user(org, sup, user_id)
        if not user_doc or not isinstance(user_doc, dict):
            return []

        role_ids = user_doc.get("roles") or []
        if isinstance(role_ids, str):
            try:
                role_ids = json.loads(role_ids)
            except Exception:
                role_ids = [role_ids]

        # Step 3: resolve each role_id
        roles: List[Dict] = []
        for rid in role_ids:
            rid = str(rid).strip()
            if not rid:
                continue
            role_doc = read_role(org, sup, rid)
            if role_doc and isinstance(role_doc, dict):
                roles.append({"role_id": rid, **role_doc})
            else:
                roles.append({"role_id": rid, "role_name": rid})
        return roles
    except Exception as e:
        logger.warning("get_user_roles failed (%s/%s/%s): %s", org, sup, username, e)
        return []


# ------------------------------ Router ------------------------------

router = APIRouter()

# ------------------------------ Selection panel endpoints ------------------------------

# Cache MetaReader instances — the constructor is expensive (~40-470ms) due to
# Redis/storage connection setup.  Instances are stateless readers, safe to reuse.
_meta_reader_cache: Dict[Tuple[str, str], Any] = {}
_META_READER_CACHE_MAX = 32


def _get_meta_reader(organization: str, super_name: str):
    """Return a cached MetaReader instance, creating one if needed."""
    key = (organization, super_name)
    reader = _meta_reader_cache.get(key)
    if reader is not None:
        return reader
    from supertable.meta_reader import MetaReader
    reader = MetaReader(organization=organization, super_name=super_name)
    # Evict oldest if cache is full
    if len(_meta_reader_cache) >= _META_READER_CACHE_MAX:
        try:
            oldest = next(iter(_meta_reader_cache))
            del _meta_reader_cache[oldest]
        except StopIteration:
            pass
    _meta_reader_cache[key] = reader
    return reader

# ------------------------------ Admin: auth tokens -----------------------------

def _get_org_from_env_fallback() -> str:
    return (settings.SUPERTABLE_ORGANIZATION or "").strip()


def _catalog_list_tokens(org: str) -> List[Dict[str, Any]]:
    if not org:
        return []
    try:
        return catalog.list_auth_tokens(org)
    except Exception:
        try:
            raw = redis_client.hgetall(f"supertable:{org}:auth:tokens") or {}
            out: List[Dict[str, Any]] = []
            for token_id, meta_raw in raw.items():
                try:
                    meta = json.loads(meta_raw) if meta_raw else {}
                except Exception:
                    meta = {"value": meta_raw}
                if isinstance(meta, dict):
                    meta = dict(meta)
                else:
                    meta = {"value": meta}
                meta.setdefault("token_id", token_id)
                out.append(meta)
            out.sort(key=lambda x: int(x.get("created_ms") or 0), reverse=True)
            return out
        except Exception:
            return []


def _catalog_create_token(org: str, created_by: str, label: Optional[str], username: str = "", user_id: str = "") -> Dict[str, Any]:
    if not org:
        raise HTTPException(status_code=400, detail="Missing organization")
    try:
        return catalog.create_auth_token(org=org, created_by=created_by, label=label, username=username, user_id=user_id)
    except Exception:
        token = secrets.token_urlsafe(24)
        token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
        meta = {
            "token_id": token_id,
            "created_ms": int(time.time() * 1000),
            "created_by": str(created_by or ""),
            "label": (str(label).strip() if label is not None else ""),
            "enabled": True,
            "username": str(username or ""),
            "user_id": str(user_id or ""),
        }
        try:
            redis_client.hset(f"supertable:{org}:auth:tokens", token_id, json.dumps(meta))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Token creation failed: {e}")
        return {"token": token, **meta}


def _catalog_delete_token(org: str, token_id: str) -> bool:
    if not org or not token_id:
        return False
    try:
        return bool(catalog.delete_auth_token(org=org, token_id=token_id))
    except Exception:
        try:
            return bool(redis_client.hdel(f"supertable:{org}:auth:tokens", token_id))
        except Exception:
            return False

# ------------------------------ Backward-compat route aliases ------------------

def _add_reflection_alias_routes(_router: APIRouter) -> None:
    """Add backward-compatible aliases under /reflection for legacy /admin and /api routes."""
    try:
        from fastapi.routing import APIRoute  # noqa: F401
    except Exception:
        return

    existing = set()
    for r in _router.routes:
        try:
            methods = tuple(sorted(getattr(r, "methods", None) or []))
            existing.add((getattr(r, "path", None), methods))
        except Exception:
            continue

    def _try_add_alias(path: str, methods: set, endpoint, name: str) -> None:
        key = (path, tuple(sorted(methods or [])))
        if key in existing:
            return
        _router.add_api_route(
            path,
            endpoint,
            methods=list(methods or []),
            name=name,
            include_in_schema=False,
        )
        existing.add(key)

    routes = list(_router.routes)
    for r in routes:
        if not hasattr(r, "path") or not hasattr(r, "endpoint"):
            continue
        path = str(getattr(r, "path") or "")
        if not path:
            continue
        methods = set(getattr(r, "methods", None) or [])
        endpoint = getattr(r, "endpoint")
        name = str(getattr(r, "name", "") or "route")

        if path == "/admin":
            continue
        if path.startswith("/admin"):
            _try_add_alias(
                "/reflection" + path[len("/admin"):],
                methods, endpoint, name + "_reflection_alias",
            )

        if path == "/api":
            continue
        if path.startswith("/api"):
            _try_add_alias(
                "/reflection" + path[len("/api"):],
                methods, endpoint, name + "_reflection_alias",
            )



# ---------------------------------------------------------------------------
# Endpoint registration is NOT done here.
# Each application module imports supertable.api.api explicitly to register
# endpoints on the shared router.  This keeps server_common.py as pure
# infrastructure so the UI server can import utilities without pulling
# in all endpoint handlers.
#
# API server:  supertable.api.application  → imports api.api
# UI server:   supertable.webui.application  → does NOT import api.api
# ---------------------------------------------------------------------------
