# route: supertable.api.session
"""
Session management, authentication, and identity for the SuperTable web UI.

Handles:
  - Signed session cookies (HMAC-SHA256)
  - Superuser and regular user authentication checks
  - Login guards for FastAPI endpoints
  - Session context injection for Jinja templates

Usage (from common.py at import time):

    from supertable.api import session

    session.configure(
        superuser_token="...",
        admin_token="...",
        session_secret="...",
        secure_cookies=False,
        superuser_hash="...",
    )

All public functions are re-exported from common.py for backward compatibility.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional

from fastapi import HTTPException, Request, Response


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPERUSER_USERNAME = "superuser"

_SESSION_COOKIE_NAME = "st_session"
_ADMIN_COOKIE_NAME = "st_admin_token"
_SESSION_MAX_AGE_SECONDS = 7 * 24 * 3600


# ---------------------------------------------------------------------------
# Module-level configuration (set once via configure())
# ---------------------------------------------------------------------------

_superuser_token: str = ""
_session_secret_value: str = ""
_secure_cookies: bool = False
_superuser_hash: str = ""


def configure(
    *,
    superuser_token: str,
    session_secret: str = "",
    secure_cookies: bool = False,
    superuser_hash: str = "",
) -> None:
    """Inject settings into the session module.  Called once at startup by common.py."""
    global _superuser_token, _session_secret_value
    global _secure_cookies, _superuser_hash
    _superuser_token = (superuser_token or "").strip()
    _session_secret_value = (session_secret or "").strip()
    _secure_cookies = secure_cookies
    _superuser_hash = (superuser_hash or "").strip()


# ---------------------------------------------------------------------------
# Identity helpers
# ---------------------------------------------------------------------------

def _sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _user_hash(org: str, username: str) -> str:
    return _sha256_hex(f"{org}:{username}")


def derive_superuser_hash(org: str) -> str:
    """Derive the superuser's identity hash from the organization name."""
    return _user_hash(org, SUPERUSER_USERNAME)


# ---------------------------------------------------------------------------
# Token helpers
# ---------------------------------------------------------------------------

def _required_token() -> str:
    """Superuser token required for privileged admin actions."""
    return _superuser_token


# ---------------------------------------------------------------------------
# Session cookie cryptography
# ---------------------------------------------------------------------------

def _session_secret() -> bytes:
    if _session_secret_value:
        return _session_secret_value.encode("utf-8")
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


# ---------------------------------------------------------------------------
# Cookie operations
# ---------------------------------------------------------------------------

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
        secure=_secure_cookies,
        max_age=_SESSION_MAX_AGE_SECONDS,
        path="/",
    )


def _clear_session_cookie(resp: Response) -> None:
    resp.delete_cookie(_SESSION_COOKIE_NAME, path="/")


# ---------------------------------------------------------------------------
# Auth checks
# ---------------------------------------------------------------------------

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


def _is_authorized(request: Request) -> bool:
    return is_logged_in(request)


def _get_provided_token(request: Request) -> Optional[str]:
    cookie = request.cookies.get(_ADMIN_COOKIE_NAME)
    return cookie.strip() if isinstance(cookie, str) else None


# ---------------------------------------------------------------------------
# Session context for templates
# ---------------------------------------------------------------------------

def session_context(request: Request) -> Dict[str, Any]:
    """Return template-safe session values (always present keys)."""
    sess = get_session(request) or {}
    is_su = bool(sess.get("is_superuser") is True) or is_superuser(request)
    role_name = (sess.get("role_name") or "").strip().lower()
    roles_list = sess.get("roles") or []
    # Admin = superuser OR role_name is "admin" or "superadmin"
    is_admin = is_su or role_name in ("admin", "superadmin") or any(
        str(r).strip().lower() in ("admin", "superadmin") for r in roles_list
    )
    return {
        "session_username": (sess.get("username") or "").strip(),
        "session_org": (sess.get("org") or "").strip(),
        "session_user_hash": (sess.get("user_hash") or "").strip(),
        "session_is_superuser": is_su,
        "session_is_admin": is_admin,
        "session_logged_in": bool(sess.get("org") and sess.get("username") and sess.get("user_hash")),
        "session_role_name": (sess.get("role_name") or "").strip(),
        "session_roles": roles_list,
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
        ctx.setdefault("session_is_admin", False)
        ctx.setdefault("session_logged_in", False)
        ctx.setdefault("session_role_name", "")
        ctx.setdefault("session_roles", [])
    return ctx


# ---------------------------------------------------------------------------
# FastAPI guards
# ---------------------------------------------------------------------------

def logged_in_guard_api(request: Request):
    if _is_authorized(request):
        return True
    raise HTTPException(status_code=401, detail="Unauthorized")


def admin_guard_api(request: Request):
    # Admin-only (superuser) guard for any privileged operations.
    if _is_authorized(request) and is_superuser(request):
        return True
    raise HTTPException(status_code=403, detail="Forbidden")
