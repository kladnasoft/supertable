from __future__ import annotations

import logging
import os
import json
import html
from datetime import datetime, timezone, date
from decimal import Decimal
from typing import Dict, Iterator, List, Optional, Tuple, Set, Any
from pathlib import Path
from urllib.parse import urlparse
import re
import uuid
import enum
import hashlib
import secrets
import base64
import hmac
import time


import redis
from fastapi import APIRouter, Query, HTTPException, Request, Response, Depends, Form, Body
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from redis import Sentinel
from redis.sentinel import MasterNotFoundError



# Load environment variables from .env file (optional)
try:
    from dotenv import load_dotenv, dotenv_values, set_key
    load_dotenv()
except ImportError:
    dotenv_values = None  # type: ignore[assignment]
    set_key = None        # type: ignore[assignment]
    pass

logger = logging.getLogger(__name__)
# ------------------------------ Settings ------------------------------

class Settings:
    def __init__(self) -> None:
        # SUPERTABLE_* — as requested
        self.SUPERTABLE_ORGANIZATION: str = os.getenv("SUPERTABLE_ORGANIZATION", "").strip()
        self.SUPERTABLE_SUPERTOKEN: str = os.getenv("SUPERTABLE_SUPERTOKEN", "").strip()
        # Expected user_hash for the superuser (must match the underlying SuperTable user registry)
        self.SUPERTABLE_SUPERHASH: str = os.getenv("SUPERTABLE_SUPERHASH", "").strip()
        self.SUPERTABLE_SESSION_SECRET: str = os.getenv("SUPERTABLE_SESSION_SECRET", "").strip()

        self.SUPERTABLE_REDIS_URL: Optional[str] = os.getenv("SUPERTABLE_REDIS_URL")

        self.SUPERTABLE_REDIS_HOST: str = os.getenv("SUPERTABLE_REDIS_HOST", "localhost")
        self.SUPERTABLE_REDIS_PORT: int = int(os.getenv("SUPERTABLE_REDIS_PORT", "6379"))
        self.SUPERTABLE_REDIS_DB: int = int(os.getenv("SUPERTABLE_REDIS_DB", "0"))
        self.SUPERTABLE_REDIS_PASSWORD: Optional[str] = os.getenv("SUPERTABLE_REDIS_PASSWORD")
        self.SUPERTABLE_REDIS_USERNAME: Optional[str] = os.getenv("SUPERTABLE_REDIS_USERNAME")

        # SENTINEL
        self.SUPERTABLE_REDIS_SENTINEL: Optional[str] = os.getenv("SUPERTABLE_REDIS_SENTINEL")
        self.SUPERTABLE_REDIS_SENTINELS: Optional[str] = os.getenv("SUPERTABLE_REDIS_SENTINELS")
        self.SUPERTABLE_REDIS_SENTINEL_MASTER: Optional[str] = os.getenv("SUPERTABLE_REDIS_SENTINEL_MASTER")
        self.SUPERTABLE_REDIS_SENTINEL_PASSWORD: Optional[str] = os.getenv("SUPERTABLE_REDIS_SENTINEL_PASSWORD")
        self.SUPERTABLE_REDIS_SENTINEL_STRICT: Optional[str] = os.getenv("SUPERTABLE_REDIS_SENTINEL_STRICT")

        self.SUPERTABLE_ADMIN_TOKEN: Optional[str] = os.getenv("SUPERTABLE_ADMIN_TOKEN")

        # 1 = only superuser can login, 2 = only regular users can login, 3 = both
        try:
            self.SUPERTABLE_LOGIN_MASK: int = int((os.getenv("SUPERTABLE_LOGIN_MASK", "1") or "1").strip())
        except ValueError:
            self.SUPERTABLE_LOGIN_MASK = 1

        self.DOTENV_PATH: str = os.getenv("DOTENV_PATH", ".env")

        # IMPORTANT: keep templates in the original folder (parent of /reflection)
        # This preserves behavior from before the move of this file.
        self.TEMPLATES_DIR: str = os.getenv(
            "TEMPLATES_DIR",
            str(Path(__file__).resolve().parent.parent / "reflection/templates")
        )

        # set to 1 in HTTPS environments
        self.SECURE_COOKIES: bool = os.getenv("SECURE_COOKIES", "0").strip().lower() in ("1", "true", "yes", "on")


settings = Settings()
if settings.SUPERTABLE_LOGIN_MASK not in (1, 2, 3):
    raise RuntimeError(
        f"Invalid SUPERTABLE_LOGIN_MASK (must be 1, 2, or 3): {settings.SUPERTABLE_LOGIN_MASK}"
    )


def _required_token() -> str:
    """Superuser token required for privileged admin actions."""
    return (settings.SUPERTABLE_SUPERTOKEN or settings.SUPERTABLE_ADMIN_TOKEN or "").strip()

_missing_envs: List[str] = []
if not settings.SUPERTABLE_ORGANIZATION:
    _missing_envs.append("SUPERTABLE_ORGANIZATION")
if not (settings.SUPERTABLE_SUPERTOKEN or "").strip():
    _missing_envs.append("SUPERTABLE_SUPERTOKEN")
if _missing_envs:
    raise RuntimeError("Missing required environment variables: " + ", ".join(_missing_envs))

if not re.fullmatch(r"[0-9a-fA-F]{16,128}", settings.SUPERTABLE_SUPERHASH or ""):
    raise RuntimeError("Invalid SUPERTABLE_SUPERHASH (expected hex string)")






# ------------------------------ Signed session cookie ------------------------------

_SESSION_COOKIE_NAME = "st_session"
_ADMIN_COOKIE_NAME = "st_admin_token"
_SESSION_MAX_AGE_SECONDS = 7 * 24 * 3600

def _session_secret() -> bytes:
    if settings.SUPERTABLE_SESSION_SECRET:
        return settings.SUPERTABLE_SESSION_SECRET.encode("utf-8")
    derived = hashlib.sha256(("st_session:" + _required_token()).encode("utf-8")).hexdigest()
    return derived.encode("utf-8")


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(s: str) -> bytes:
    pad = "=" * ((4 - (len(s) % 4)) % 4)
    return base64.urlsafe_b64decode((s + pad).encode("ascii"))


def _sign_payload(payload: bytes) -> str:
    sig = hmac.new(_session_secret(), payload, hashlib.sha256).digest()
    return _b64url_encode(sig)


def _encode_session(data: Dict[str, Any]) -> str:
    payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return _b64url_encode(payload) + "." + _sign_payload(payload)


def _decode_session(value: str) -> Optional[Dict[str, Any]]:
    try:
        if not value or "." not in value:
            return None
        b64_payload, b64_sig = value.split(".", 1)
        payload = _b64url_decode(b64_payload)
        expected = _sign_payload(payload)
        if not hmac.compare_digest(expected, b64_sig):
            return None
        data = json.loads(payload.decode("utf-8"))
        if not isinstance(data, dict):
            return None
        # Enforce server-side expiry in addition to cookie max-age.
        exp = data.get("exp")
        if exp is not None:
            try:
                if int(exp) < int(time.time()):
                    return None
            except Exception:
                return None
        return data
    except Exception:
        return None


def get_session(request: Request) -> Optional[Dict[str, Any]]:
    return _decode_session(request.cookies.get(_SESSION_COOKIE_NAME, ""))


def _set_session_cookie(resp: Response, data: Dict[str, Any]) -> None:
    # Ensure an expiry exists to prevent indefinite replay if the cookie is copied.
    data = dict(data)
    data.setdefault("exp", int(time.time()) + _SESSION_MAX_AGE_SECONDS)
    resp.set_cookie(
        _SESSION_COOKIE_NAME,
        _encode_session(data),
        httponly=True,
        samesite="lax",
        secure=settings.SECURE_COOKIES,
        max_age=_SESSION_MAX_AGE_SECONDS,
        path="/",
    )


def _clear_session_cookie(resp: Response) -> None:
    resp.delete_cookie(_SESSION_COOKIE_NAME, path="/")


def is_logged_in(request: Request) -> bool:
    sess = get_session(request) or {}
    return bool(sess.get("org") and sess.get("username") and sess.get("user_hash"))


def is_superuser(request: Request) -> bool:
    sess = get_session(request) or {}
    if sess.get("is_superuser") is True:
        return True
    tok = (request.cookies.get(_ADMIN_COOKIE_NAME) or "").strip()
    required = _required_token()
    return bool(tok and required and hmac.compare_digest(tok, required))


def session_context(request: Request) -> Dict[str, Any]:
    """Return template-safe session values (always present keys)."""
    sess = get_session(request) or {}
    return {
        "session_username": (sess.get("username") or "").strip(),
        "session_org": (sess.get("org") or "").strip(),
        "session_user_hash": (sess.get("user_hash") or "").strip(),
        "session_is_superuser": bool(sess.get("is_superuser") is True) or is_superuser(request),
        "session_logged_in": bool(sess.get("org") and sess.get("username") and sess.get("user_hash")),
        "session_role_name": (sess.get("role_name") or "").strip(),
        "session_roles": sess.get("roles") or [],
    }


def inject_session_into_ctx(ctx: Dict[str, Any], request: Request) -> Dict[str, Any]:
    """Mutates and returns ctx with session_* keys for Jinja templates."""
    try:
        ctx.update(session_context(request))
    except Exception:
        # Never fail template rendering due to session issues
        ctx.setdefault("session_username", "")
        ctx.setdefault("session_org", "")
        ctx.setdefault("session_user_hash", "")
        ctx.setdefault("session_is_superuser", False)
        ctx.setdefault("session_logged_in", False)
        ctx.setdefault("session_role_name", "")
        ctx.setdefault("session_roles", [])
    return ctx


def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _user_hash(org: str, username: str) -> str:
    return _sha256_hex(f"{org}:{username}")


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


def _escape(s: str) -> str:
    return html.escape(str(s or ""), quote=True)


# ------------------------------ Auth helpers ------------------------------


def _get_provided_token(request: Request) -> Optional[str]:
    cookie = request.cookies.get(_ADMIN_COOKIE_NAME)
    return cookie.strip() if isinstance(cookie, str) else None


def _is_authorized(request: Request) -> bool:
    return is_logged_in(request)


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


def _render_login(request: Request, message: Optional[str] = None, clear_cookie: bool = False) -> HTMLResponse:
    ctx = {
        "request": request,
        "message": message or "",
        "SUPERTABLE_LOGIN_MASK": settings.SUPERTABLE_LOGIN_MASK,
    }
    inject_session_into_ctx(ctx, request)
    resp = templates.TemplateResponse("login.html", ctx, status_code=200)
    if clear_cookie:
        resp.delete_cookie("st_admin_token", path="/")
    _no_store(resp)
    _security_headers(resp)
    return resp


def logged_in_guard_api(request: Request):
    if _is_authorized(request):
        return True
    raise HTTPException(status_code=401, detail="Unauthorized")


def admin_guard_api(request: Request):
    # Admin-only (superuser) guard for any privileged operations.
    if _is_authorized(request) and is_superuser(request):
        return True
    raise HTTPException(status_code=403, detail="Forbidden")


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


# ------------------------------ Router + templates ------------------------------

router = APIRouter()

# --- Reflection root redirect (NEW) ---
@router.get("/reflection", include_in_schema=False)
async def reflection_root_redirect() -> RedirectResponse:
    # Default landing page
    return RedirectResponse(url="/reflection/home", status_code=302)

templates = Jinja2Templates(directory=settings.TEMPLATES_DIR)
@router.get("/reflection/home", response_class=HTMLResponse)
def home_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    if not _is_authorized(request):
        resp = RedirectResponse("/reflection/login", status_code=302)
        _no_store(resp)
        return resp

    provided = _get_provided_token(request) or ""
    pairs = discover_pairs()
    sel_org, sel_sup = resolve_pair(org, sup)
    tenants = [{"org": o, "sup": s, "selected": bool(o == sel_org and s == sel_sup)} for o, s in pairs]

    # Lightweight stats (best-effort)
    try:
        supertables = len({s for o, s in pairs if o})
    except Exception:
        supertables = None

    tables = None  # intentionally not computed here (may be expensive)

    connectors = None
    pools = None
    try:
        state_dir = Path(os.getenv("SUPERTABLE_REFLECTION_STATE_DIR", "/tmp/supertable_reflection"))
        c_dir = state_dir / "connectors"
        p_dir = state_dir / "compute"
        if not p_dir.exists():
            p_dir = state_dir / "compute_pools"  # legacy
        connectors = len(list(c_dir.glob("*.json"))) if c_dir.exists() else 0
        pools = len(list(p_dir.glob("*.json"))) if p_dir.exists() else 0
    except Exception:
        pass

    ctx: Dict[str, Any] = {
        "request": request,
        "authorized": True,
        "token": provided,
        "tenants": tenants,
        "sel_org": sel_org,
        "sel_sup": sel_sup,
        "has_tenant": bool(sel_org and sel_sup),
        "stats": {
            "supertables": supertables,
            "tables": tables,
            "connectors": connectors,
            "pools": pools,
        },
    }
    inject_session_into_ctx(ctx, request)

    resp = templates.TemplateResponse("home.html", ctx)
    _no_store(resp)
    return resp



@router.get("/favicon.ico", include_in_schema=False)
def favicon():
    return RedirectResponse(url="/static/favicon.ico")


@router.get("/healthz", response_class=PlainTextResponse)
def healthz():
    try:
        pong = redis_client.ping()
        return "ok" if pong else "not-ok"
    except Exception as e:
        return f"error: {e}"



# ------------------------------ Admin page & auth routes ------------------------------
@router.get("/reflection/login", response_class=HTMLResponse)
def admin_login_form(request: Request):
    return _render_login(request, message=None, clear_cookie=True)


@router.post("/reflection/login")
def admin_login(
    request: Request,
    mode: str = Form("user"),
    username: str = Form(""),
    token: str = Form(""),
    supertoken: str = Form(""),
):
    org = settings.SUPERTABLE_ORGANIZATION
    mode = (mode or "").strip().lower()
    username = (username or "").strip()

    # Enforce login mask (1=superuser only, 2=regular users only, 3=both)
    if settings.SUPERTABLE_LOGIN_MASK == 1 and mode != "super":
        return _render_login(request, message="Login is restricted to superuser only.", clear_cookie=True)
    if settings.SUPERTABLE_LOGIN_MASK == 2 and mode == "super":
        return _render_login(request, message="Login is restricted to regular users only.", clear_cookie=True)

    if mode == "super":
        required = _required_token()
        provided = (supertoken or "").strip()
        if not provided or not required or not hmac.compare_digest(provided, required):
            return _render_login(request, message="Invalid superuser token.", clear_cookie=True)

        username_eff = "superuser"
        user_hash = settings.SUPERTABLE_SUPERHASH

        resp = RedirectResponse(url="/reflection/home", status_code=302)
        resp.set_cookie(
            _ADMIN_COOKIE_NAME,
            provided,
            httponly=True,
            samesite="lax",
            secure=settings.SECURE_COOKIES,
            max_age=7 * 24 * 3600,
            path="/",
        )
        _set_session_cookie(resp, {"org": org, "username": username_eff, "user_hash": user_hash, "is_superuser": True})
        _no_store(resp)
        return resp

    # Regular user: username + redis token
    if not username:
        return _render_login(request, message="Username is required.", clear_cookie=True)

    provided_token = (token or "").strip()
    if not provided_token:
        return _render_login(request, message="Token is required.", clear_cookie=True)

    try:
        ok = bool(catalog.validate_auth_token(org=org, token=provided_token))
    except Exception:
        ok = False

    if not ok:
        return _render_login(request, message="Invalid token.", clear_cookie=True)

    user_hash = _user_hash(org, username)
    resp = RedirectResponse(url="/reflection/home", status_code=302)
    resp.delete_cookie(_ADMIN_COOKIE_NAME, path="/")
    _clear_session_cookie(resp)
    _set_session_cookie(resp, {"org": org, "username": username, "user_hash": user_hash, "is_superuser": False})
    _no_store(resp)
    return resp


@router.get("/reflection/logout")
def admin_logout():
    resp = RedirectResponse("/reflection/login", status_code=302)
    resp.delete_cookie(_ADMIN_COOKIE_NAME, path="/")
    _clear_session_cookie(resp)
    _no_store(resp)
    return resp


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


@router.get("/reflection/super-meta")
def reflection_super_meta(
    request: Request,
    organization: str = Query(...),
    super_name: str = Query(...),
    role_name: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Return SuperTable metadata via MetaReader.get_super_meta(role_name).

    Used by selection.html to populate the header metrics panel.
    """
    t0 = time.perf_counter()

    organization = organization.strip()
    super_name = super_name.strip()
    if not organization or not super_name:
        raise HTTPException(status_code=400, detail="organization and super_name are required")

    # Resolve role_name: prefer explicit param, then session
    resolved_role = (role_name or "").strip()
    if not resolved_role:
        sess = get_session(request) or {}
        resolved_role = (sess.get("role_name") or "").strip()

    try:
        t1 = time.perf_counter()
        meta_reader = _get_meta_reader(organization, super_name)
        t2 = time.perf_counter()
        result = meta_reader.get_super_meta(resolved_role)
        t3 = time.perf_counter()
    except Exception as e:
        logger.warning("MetaReader.get_super_meta failed (%s/%s): %s", organization, super_name, e)
        raise HTTPException(status_code=500, detail=str(e))

    resp = JSONResponse({"meta": result})
    _no_store(resp)
    t4 = time.perf_counter()

    logger.debug(
        "super-meta %s/%s timings: total=%.1fms | meta_reader=%.1fms | get_super_meta=%.1fms | response=%.1fms",
        organization, super_name,
        (t4 - t0) * 1000,
        (t2 - t1) * 1000,
        (t3 - t2) * 1000,
        (t4 - t3) * 1000,
    )
    return resp


@router.get("/reflection/user-role-names")
def reflection_user_role_names(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Return role names for the current session user on the given SuperTable.

    Uses UserManager.get_user_hash_by_name() to get the user's role IDs,
    then RoleManager.get_role() for each to resolve role names.
    Returns only a flat list of role name strings.
    """
    sess = get_session(request) or {}
    username = (sess.get("username") or "").strip()
    if not username:
        return JSONResponse({"role_names": []})

    org_val, sup_val = resolve_pair(org, sup)
    if not org_val or not sup_val:
        return JSONResponse({"role_names": []})

    try:
        from supertable.rbac.user_manager import UserManager
        from supertable.rbac.role_manager import RoleManager

        user_manager = UserManager(super_name=sup_val, organization=org_val)
        user_data = user_manager.get_user_hash_by_name(username)

        if not user_data or not isinstance(user_data, dict):
            return JSONResponse({"role_names": []})

        role_ids = user_data.get("roles") or []
        if isinstance(role_ids, str):
            try:
                role_ids = json.loads(role_ids)
            except Exception:
                role_ids = [role_ids]

        role_names: List[str] = []
        role_manager = RoleManager(super_name=sup_val, organization=org_val)
        for role_id in role_ids:
            role_id = str(role_id).strip()
            if not role_id:
                continue
            try:
                role_doc = role_manager.get_role(role_id)
                if role_doc and isinstance(role_doc, dict):
                    name = role_doc.get("role_name") or role_doc.get("role") or role_id
                    role_names.append(str(name))
                else:
                    role_names.append(role_id)
            except Exception:
                role_names.append(role_id)

        return JSONResponse({"role_names": role_names})

    except Exception as e:
        logger.warning("reflection_user_role_names failed (%s/%s/%s): %s", org_val, sup_val, username, e)
        return JSONResponse({"role_names": []})


# ------------------------------ Sidebar role endpoints ------------------------------

@router.get("/reflection/users")
def reflection_users(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Return users for the given org/sup. Available to all logged-in users."""
    org_val, sup_val = resolve_pair(org, sup)
    if not org_val or not sup_val:
        return {"users": []}
    return {"users": list_users(org_val, sup_val)}


@router.get("/reflection/roles")
def reflection_roles(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Return all roles for the given org/sup. Available to all logged-in users."""
    org_val, sup_val = resolve_pair(org, sup)
    if not org_val or not sup_val:
        return {"roles": []}
    return {"roles": list_roles(org_val, sup_val)}


@router.get("/reflection/user-roles")
def reflection_user_roles(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    _: Any = Depends(logged_in_guard_api),
):
    """Return resolved roles for the current session user after SuperTable selection.

    Flow: catalog.rbac_get_user_id_by_username(username) → role IDs
          → catalog.get_role_details() for each.
    Auto-selects the first role and stores it in session.
    """
    sess = get_session(request) or {}
    username = (sess.get("username") or "").strip()
    if not username:
        return {"roles": [], "selected_role": ""}

    org_val, sup_val = resolve_pair(org, sup)
    if not org_val or not sup_val:
        return {"roles": [], "selected_role": ""}

    roles = get_user_roles(org_val, sup_val, username)

    # Determine which role to select (prefer existing session value, else first)
    current_role = (sess.get("role_name") or "").strip()
    role_names = [r.get("role_name") or r.get("role_id") or "" for r in roles]
    if current_role not in role_names and role_names:
        current_role = role_names[0]

    # Persist to session cookie
    sess_data = dict(sess)
    sess_data["role_name"] = current_role
    sess_data["roles"] = role_names
    sess_data["super_name"] = sup_val

    resp = JSONResponse({"roles": roles, "selected_role": current_role})
    _set_session_cookie(resp, sess_data)
    _no_store(resp)
    return resp


@router.post("/reflection/set-role")
def reflection_set_role(
    request: Request,
    body: Dict[str, Any] = Body(...),
    _: Any = Depends(logged_in_guard_api),
):
    """Persist the selected role name into the session cookie."""
    role_name = str(body.get("role_name") or "").strip()
    sess = get_session(request) or {}

    sess_data = dict(sess)
    sess_data["role_name"] = role_name

    resp = JSONResponse({"ok": True, "role_name": role_name})
    _set_session_cookie(resp, sess_data)
    _no_store(resp)
    return resp



@router.get("/", response_class=HTMLResponse)
def root_redirect():
    resp = RedirectResponse("/reflection/login", status_code=302)
    _no_store(resp)
    return resp


# ------------------------------ Tables tab (extracted to tables.py) ------------------------------

try:
    from .tables import attach_tables_routes  # type: ignore
except Exception:  # pragma: no cover
    try:
        from supertable.reflection.tables import attach_tables_routes  # type: ignore
    except Exception:
        from tables import attach_tables_routes  # type: ignore

attach_tables_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    list_users=list_users,
    fmt_ts=_fmt_ts,
    catalog=catalog,
    admin_guard_api=admin_guard_api,
    get_session=get_session,
)

# ------------------------------ Execute tab (extracted to execute.py) ------------------------------

try:
    from .execute import attach_execute_routes  # type: ignore
except Exception:  # pragma: no cover
    try:
        from supertable.reflection.execute import attach_execute_routes  # type: ignore
    except Exception:
        from execute import attach_execute_routes  # type: ignore

attach_execute_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    get_session=get_session,
    logged_in_guard_api=logged_in_guard_api,
    admin_guard_api=admin_guard_api,
)



# ------------------------------ Ingestion (extracted to ingestion.py) ------------------------------

try:
    from .ingestion import attach_ingestion_routes  # type: ignore
except Exception:  # pragma: no cover
    try:
        from supertable.reflection.ingestion import attach_ingestion_routes  # type: ignore
    except Exception:
        from ingestion import attach_ingestion_routes  # type: ignore

attach_ingestion_routes(
    router,
    templates=templates,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    get_session=get_session,
    is_superuser=is_superuser,
    logged_in_guard_api=logged_in_guard_api,
    admin_guard_api=admin_guard_api,
    redis_client=redis_client,
)

# ------------------------------ Admin (extracted to admin.py) ------------------------------

try:
    from .admin import attach_admin_routes  # type: ignore
except Exception:  # pragma: no cover
    try:
        from supertable.reflection.admin import attach_admin_routes  # type: ignore
    except Exception:
        from admin import attach_admin_routes  # type: ignore

try:
    from dotenv import dotenv_values as _dotenv_values, set_key as _set_key
except ImportError:
    _dotenv_values = None  # type: ignore[assignment]
    _set_key = None        # type: ignore[assignment]

attach_admin_routes(
    router,
    templates=templates,
    settings=settings,
    redis_client=redis_client,
    catalog=catalog,
    is_authorized=_is_authorized,
    no_store=_no_store,
    get_provided_token=_get_provided_token,
    discover_pairs=discover_pairs,
    resolve_pair=resolve_pair,
    inject_session_into_ctx=inject_session_into_ctx,
    get_session=get_session,
    list_users=list_users,
    fmt_ts=_fmt_ts,
    logged_in_guard_api=logged_in_guard_api,
    admin_guard_api=admin_guard_api,
    dotenv_values=_dotenv_values,
    set_key=_set_key,
)

# ---------------------------------------------------------------------------
# Import not_common to register additional routes on the shared router.
# This MUST be at the bottom so all common symbols are defined first.
# ---------------------------------------------------------------------------
#from supertable.reflection import not_common as _not_common  # noqa: F401, E402