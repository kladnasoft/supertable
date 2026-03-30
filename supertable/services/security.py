# route: supertable.services.security
"""
Unified Security module — serves security.html and owns ALL RBAC
role/user CRUD endpoints plus OData URL generation.

Page routes:
  - GET  /reflection/security        → security.html
  - GET  /reflection/rbac            → 302 redirect to /reflection/security

Role CRUD:
  - GET    /reflection/rbac/roles
  - GET    /reflection/rbac/roles/{role_id}
  - POST   /reflection/rbac/roles
  - PUT    /reflection/rbac/roles/{role_id}
  - DELETE /reflection/rbac/roles/{role_id}

User CRUD:
  - GET    /reflection/rbac/users
  - GET    /reflection/rbac/users/{user_id}
  - POST   /reflection/rbac/users
  - PUT    /reflection/rbac/users/{user_id}
  - DELETE /reflection/rbac/users/{user_id}
"""
from __future__ import annotations

import hashlib
import json
import os

from supertable.config.settings import settings
import re
import secrets
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import quote as _url_quote

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse


# ---------------------------------------------------------------------------
# Redis key helpers — mirror the namespace used by RedisCatalog
# ---------------------------------------------------------------------------

def _role_index_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:rbac:roles:index"

def _role_doc_key(org: str, sup: str, role_id: str) -> str:
    return f"supertable:{org}:{sup}:rbac:roles:doc:{role_id}"

def _user_index_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:rbac:users:index"

def _user_doc_key(org: str, sup: str, user_id: str) -> str:
    return f"supertable:{org}:{sup}:rbac:users:doc:{user_id}"

def _user_name_map_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:rbac:users:name_to_id"


# ---------------------------------------------------------------------------
# OData URL builder
# ---------------------------------------------------------------------------

_ODATA_BASE = settings.SUPERTABLE_ODATA_BASE_URL


def _build_odata_url(org: str, sup: str, role_name: str) -> str:
    """Compute the role-scoped OData service root URL."""
    base = _ODATA_BASE.rstrip("/")
    return (
        f"{base}/{_url_quote(org, safe='')}"
        f"/{_url_quote(sup, safe='')}"
        f"/odata?role={_url_quote(role_name, safe='')}"
    )


# ---------------------------------------------------------------------------
# Role helpers
# ---------------------------------------------------------------------------

_VALID_ROLE_TYPES = {"superadmin", "admin", "writer", "reader", "meta"}
_ROLE_ID_RE = re.compile(r"^[a-f0-9]{32}$")


def _decode_role(raw: Dict, role_id: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for k, v in (raw or {}).items():
        kk = k if isinstance(k, str) else k.decode("utf-8")
        data[kk] = v if isinstance(v, str) else v.decode("utf-8")
    data["role_id"] = role_id
    for field in ("tables", "columns", "filters"):
        if field in data and isinstance(data[field], str):
            try:
                data[field] = json.loads(data[field])
            except Exception:
                pass
    return data


def _list_roles(redis_client: Any, org: str, sup: str) -> List[Dict[str, Any]]:
    index_key = _role_index_key(org, sup)
    members = redis_client.smembers(index_key) or set()
    out = []
    for m in members:
        rid = m if isinstance(m, str) else m.decode("utf-8")
        raw = redis_client.hgetall(_role_doc_key(org, sup, rid))
        if raw:
            out.append(_decode_role(raw, rid))
    out.sort(key=lambda r: (r.get("role_name") or "").lower())
    return out


def _get_role(redis_client: Any, org: str, sup: str, role_id: str) -> Optional[Dict[str, Any]]:
    raw = redis_client.hgetall(_role_doc_key(org, sup, role_id))
    if not raw:
        return None
    return _decode_role(raw, role_id)


def _create_role(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    role_name: str,
    role: str = "reader",
    tables: List[str],
    columns: List[str],
    filters: Any,
    source_query: str = "",
) -> Dict[str, Any]:
    nm = (role_name or "").strip()
    if not nm:
        raise HTTPException(status_code=400, detail="role_name is required")
    if len(nm) > 120:
        raise HTTPException(status_code=400, detail="role_name is too long")
    role_type = (role or "reader").strip().lower()
    if role_type not in _VALID_ROLE_TYPES:
        raise HTTPException(status_code=400, detail=f"role must be one of: {', '.join(sorted(_VALID_ROLE_TYPES))}")

    role_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    mapping: Dict[str, str] = {
        "role_name": nm,
        "role": role_type,
        "tables": json.dumps(tables or ["*"]),
        "columns": json.dumps(columns or ["*"]),
        "filters": json.dumps(filters if isinstance(filters, list) else [str(filters)]),
        "created_at": now,
        "updated_at": now,
    }
    if source_query:
        mapping["source_query"] = source_query.strip()

    doc_key = _role_doc_key(org, sup, role_id)
    redis_client.hset(doc_key, mapping=mapping)
    redis_client.sadd(_role_index_key(org, sup), role_id)

    return _decode_role(mapping, role_id)


def _update_role(
    redis_client: Any,
    org: str,
    sup: str,
    role_id: str,
    *,
    role_name: Optional[str] = None,
    role: Optional[str] = None,
    tables: Optional[List[str]] = None,
    columns: Optional[List[str]] = None,
    filters: Optional[Any] = None,
    source_query: Optional[str] = None,
) -> Dict[str, Any]:
    if not _ROLE_ID_RE.fullmatch(role_id or ""):
        raise HTTPException(status_code=400, detail="invalid role_id")
    doc_key = _role_doc_key(org, sup, role_id)
    if not redis_client.exists(doc_key):
        raise HTTPException(status_code=404, detail="role not found")

    mapping: Dict[str, str] = {}
    if role_name is not None:
        nm = (role_name or "").strip()
        if not nm:
            raise HTTPException(status_code=400, detail="role_name is required")
        mapping["role_name"] = nm
    if role is not None:
        rt = (role or "").strip().lower()
        if rt not in _VALID_ROLE_TYPES:
            raise HTTPException(status_code=400, detail=f"role must be one of: {', '.join(sorted(_VALID_ROLE_TYPES))}")
        mapping["role"] = rt
    if tables is not None:
        mapping["tables"] = json.dumps(tables)
    if columns is not None:
        mapping["columns"] = json.dumps(columns)
    if filters is not None:
        mapping["filters"] = json.dumps(filters if isinstance(filters, list) else [str(filters)])
    if source_query is not None:
        mapping["source_query"] = source_query.strip()

    mapping["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    redis_client.hset(doc_key, mapping=mapping)

    doc = _get_role(redis_client, org, sup, role_id)
    if not doc:
        raise HTTPException(status_code=404, detail="role not found")
    return doc


def _delete_role(redis_client: Any, org: str, sup: str, role_id: str) -> bool:
    if not _ROLE_ID_RE.fullmatch(role_id or ""):
        raise HTTPException(status_code=400, detail="invalid role_id")
    deleted = bool(redis_client.delete(_role_doc_key(org, sup, role_id)))
    redis_client.srem(_role_index_key(org, sup), role_id)
    return deleted


# ---------------------------------------------------------------------------
# User helpers
# ---------------------------------------------------------------------------

_USER_ID_RE = re.compile(r"^[a-f0-9]{32,64}$")
_VALID_USERNAME_RE = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}$")


def validate_username(username: str) -> str:
    """Validate and return a safe username string.

    Rules:
      - 1–64 characters
      - Starts with alphanumeric
      - Only alphanumeric, dots, underscores, hyphens
      - Case-preserved but uniqueness is case-insensitive
    """
    nm = (username or "").strip()
    if not nm:
        raise HTTPException(status_code=400, detail="username is required")
    if not _VALID_USERNAME_RE.fullmatch(nm):
        raise HTTPException(
            status_code=400,
            detail="Username must be 1-64 characters, start with a letter or digit, "
                   "and contain only letters, digits, dots, underscores, or hyphens."
        )
    return nm


def _decode_user(raw: Dict, user_id: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for k, v in (raw or {}).items():
        kk = k if isinstance(k, str) else k.decode("utf-8")
        data[kk] = v if isinstance(v, str) else v.decode("utf-8")
    data["user_id"] = user_id
    if "roles" in data and isinstance(data["roles"], str):
        try:
            data["roles"] = json.loads(data["roles"])
        except Exception:
            data["roles"] = []
    return data


def _list_users_redis(redis_client: Any, org: str, sup: str) -> List[Dict[str, Any]]:
    index_key = _user_index_key(org, sup)
    members = redis_client.smembers(index_key) or set()
    out = []
    for m in members:
        uid = m if isinstance(m, str) else m.decode("utf-8")
        raw = redis_client.hgetall(_user_doc_key(org, sup, uid))
        if raw:
            out.append(_decode_user(raw, uid))
    out.sort(key=lambda u: (u.get("username") or "").lower())
    return out


def _get_user(redis_client: Any, org: str, sup: str, user_id: str) -> Optional[Dict[str, Any]]:
    raw = redis_client.hgetall(_user_doc_key(org, sup, user_id))
    if not raw:
        return None
    return _decode_user(raw, user_id)


def _create_user(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    username: str,
    roles: List[str],
    display_name: str = "",
) -> Dict[str, Any]:
    nm = validate_username(username)

    # Prevent duplicate usernames
    existing_id = redis_client.hget(_user_name_map_key(org, sup), nm.lower())
    if existing_id:
        raise HTTPException(status_code=409, detail="username already exists")

    user_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    mapping = {
        "username": nm,
        "display_name": (display_name or "").strip(),
        "roles": json.dumps([str(r) for r in (roles or [])]),
        "created_at": now,
        "updated_at": now,
    }
    redis_client.hset(_user_doc_key(org, sup, user_id), mapping=mapping)
    redis_client.sadd(_user_index_key(org, sup), user_id)
    redis_client.hset(_user_name_map_key(org, sup), nm.lower(), user_id)
    return _decode_user(mapping, user_id)


def _update_user(
    redis_client: Any,
    org: str,
    sup: str,
    user_id: str,
    *,
    username: Optional[str] = None,
    roles: Optional[List[str]] = None,
    display_name: Optional[str] = None,
) -> Dict[str, Any]:
    if not _USER_ID_RE.fullmatch(user_id or ""):
        raise HTTPException(status_code=400, detail="invalid user_id")
    doc_key = _user_doc_key(org, sup, user_id)
    if not redis_client.exists(doc_key):
        raise HTTPException(status_code=404, detail="user not found")

    mapping: Dict[str, str] = {}
    if username is not None:
        nm = validate_username(username)
        # Update name map: remove old, add new
        existing = _get_user(redis_client, org, sup, user_id)
        old_nm = (existing or {}).get("username", "")
        if old_nm and old_nm.lower() != nm.lower():
            redis_client.hdel(_user_name_map_key(org, sup), old_nm.lower())
            redis_client.hset(_user_name_map_key(org, sup), nm.lower(), user_id)
        mapping["username"] = nm
    if roles is not None:
        mapping["roles"] = json.dumps([str(r) for r in roles])
    if display_name is not None:
        mapping["display_name"] = display_name.strip()

    mapping["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    redis_client.hset(doc_key, mapping=mapping)

    doc = _get_user(redis_client, org, sup, user_id)
    if not doc:
        raise HTTPException(status_code=404, detail="user not found")
    return doc


def _delete_user(redis_client: Any, org: str, sup: str, user_id: str) -> bool:
    if not _USER_ID_RE.fullmatch(user_id or ""):
        raise HTTPException(status_code=400, detail="invalid user_id")
    existing = _get_user(redis_client, org, sup, user_id)
    if existing:
        nm = (existing.get("username") or "").lower()
        if nm:
            redis_client.hdel(_user_name_map_key(org, sup), nm)
    deleted = bool(redis_client.delete(_user_doc_key(org, sup, user_id)))
    redis_client.srem(_user_index_key(org, sup), user_id)
    return deleted


# ---------------------------------------------------------------------------
# OData Endpoint helpers
# ---------------------------------------------------------------------------

_ENDPOINT_ID_RE = re.compile(r"^[a-f0-9]{32}$")
_TOKEN_PREFIX = "st_od_"


def _odata_index_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:odata:endpoints:index"

def _odata_doc_key(org: str, sup: str, endpoint_id: str) -> str:
    return f"supertable:{org}:{sup}:odata:endpoints:doc:{endpoint_id}"

def _odata_token_key(token_hash: str) -> str:
    """Global reverse lookup: token_hash → endpoint context."""
    return f"supertable:odata:token:{token_hash}"


def _generate_odata_token() -> str:
    """Generate a prefixed bearer token: st_od_ + 48 hex chars."""
    return _TOKEN_PREFIX + secrets.token_hex(24)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _decode_endpoint(raw: Dict, endpoint_id: str) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for k, v in (raw or {}).items():
        kk = k if isinstance(k, str) else k.decode("utf-8")
        data[kk] = v if isinstance(v, str) else v.decode("utf-8")
    data["endpoint_id"] = endpoint_id
    # Convert "true"/"false" strings to booleans for JSON
    if "enabled" in data:
        data["enabled"] = data["enabled"] not in ("false", "0", "")
    else:
        data["enabled"] = True
    return data


def _list_endpoints(redis_client: Any, org: str, sup: str) -> List[Dict[str, Any]]:
    index_key = _odata_index_key(org, sup)
    members = redis_client.smembers(index_key) or set()
    out = []
    for m in members:
        eid = m if isinstance(m, str) else m.decode("utf-8")
        raw = redis_client.hgetall(_odata_doc_key(org, sup, eid))
        if raw:
            out.append(_decode_endpoint(raw, eid))
    out.sort(key=lambda e: (e.get("label") or "").lower())
    return out


def _get_endpoint(redis_client: Any, org: str, sup: str, endpoint_id: str) -> Optional[Dict[str, Any]]:
    raw = redis_client.hgetall(_odata_doc_key(org, sup, endpoint_id))
    if not raw:
        return None
    return _decode_endpoint(raw, endpoint_id)


def _create_endpoint(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    label: str,
    role_id: str,
    role_name: str,
) -> Tuple[Dict[str, Any], str]:
    """Create an OData endpoint. Returns (endpoint_doc, plaintext_token)."""
    lbl = (label or "").strip()
    if not lbl:
        raise HTTPException(status_code=400, detail="label is required")
    if len(lbl) > 200:
        raise HTTPException(status_code=400, detail="label is too long")

    endpoint_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    # Generate token
    token = _generate_odata_token()
    token_hash = _hash_token(token)

    odata_url = _build_odata_url(org, sup, role_name)

    mapping: Dict[str, str] = {
        "label": lbl,
        "role_id": role_id,
        "role_name": role_name,
        "enabled": "true",
        "token_hash": token_hash,
        "token_prefix": token[:16] + "…",
        "odata_url": odata_url,
        "created_at": now,
        "updated_at": now,
        "last_accessed_at": "",
    }

    doc_key = _odata_doc_key(org, sup, endpoint_id)
    redis_client.hset(doc_key, mapping=mapping)
    redis_client.sadd(_odata_index_key(org, sup), endpoint_id)

    # Global token → endpoint lookup
    token_data = json.dumps({
        "org": org,
        "sup": sup,
        "endpoint_id": endpoint_id,
        "role_name": role_name,
    })
    redis_client.set(_odata_token_key(token_hash), token_data)

    return _decode_endpoint(mapping, endpoint_id), token


def _update_endpoint(
    redis_client: Any,
    org: str,
    sup: str,
    endpoint_id: str,
    *,
    label: Optional[str] = None,
    enabled: Optional[bool] = None,
) -> Dict[str, Any]:
    if not _ENDPOINT_ID_RE.fullmatch(endpoint_id or ""):
        raise HTTPException(status_code=400, detail="invalid endpoint_id")
    doc_key = _odata_doc_key(org, sup, endpoint_id)
    if not redis_client.exists(doc_key):
        raise HTTPException(status_code=404, detail="endpoint not found")

    mapping: Dict[str, str] = {}
    if label is not None:
        lbl = (label or "").strip()
        if not lbl:
            raise HTTPException(status_code=400, detail="label is required")
        mapping["label"] = lbl
    if enabled is not None:
        mapping["enabled"] = "true" if enabled else "false"

    mapping["updated_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    redis_client.hset(doc_key, mapping=mapping)

    doc = _get_endpoint(redis_client, org, sup, endpoint_id)
    if not doc:
        raise HTTPException(status_code=404, detail="endpoint not found")
    return doc


def _regenerate_endpoint_token(
    redis_client: Any,
    org: str,
    sup: str,
    endpoint_id: str,
) -> Tuple[Dict[str, Any], str]:
    """Revoke old token, generate new one. Returns (endpoint_doc, plaintext_token)."""
    if not _ENDPOINT_ID_RE.fullmatch(endpoint_id or ""):
        raise HTTPException(status_code=400, detail="invalid endpoint_id")
    doc_key = _odata_doc_key(org, sup, endpoint_id)
    if not redis_client.exists(doc_key):
        raise HTTPException(status_code=404, detail="endpoint not found")

    # Remove old token lookup
    old_hash = redis_client.hget(doc_key, "token_hash")
    if old_hash:
        old_hash_str = old_hash if isinstance(old_hash, str) else old_hash.decode("utf-8")
        redis_client.delete(_odata_token_key(old_hash_str))

    # Generate new token
    token = _generate_odata_token()
    token_hash = _hash_token(token)

    role_name_raw = redis_client.hget(doc_key, "role_name")
    role_name = (role_name_raw if isinstance(role_name_raw, str) else role_name_raw.decode("utf-8")) if role_name_raw else ""

    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    redis_client.hset(doc_key, mapping={
        "token_hash": token_hash,
        "token_prefix": token[:16] + "…",
        "updated_at": now,
    })

    # New global token lookup
    token_data = json.dumps({
        "org": org,
        "sup": sup,
        "endpoint_id": endpoint_id,
        "role_name": role_name,
    })
    redis_client.set(_odata_token_key(token_hash), token_data)

    doc = _get_endpoint(redis_client, org, sup, endpoint_id)
    return doc, token


def _delete_endpoint(redis_client: Any, org: str, sup: str, endpoint_id: str) -> bool:
    if not _ENDPOINT_ID_RE.fullmatch(endpoint_id or ""):
        raise HTTPException(status_code=400, detail="invalid endpoint_id")
    doc_key = _odata_doc_key(org, sup, endpoint_id)
    # Remove token lookup
    old_hash = redis_client.hget(doc_key, "token_hash")
    if old_hash:
        old_hash_str = old_hash if isinstance(old_hash, str) else old_hash.decode("utf-8")
        redis_client.delete(_odata_token_key(old_hash_str))
    deleted = bool(redis_client.delete(doc_key))
    redis_client.srem(_odata_index_key(org, sup), endpoint_id)
    return deleted


def verify_odata_bearer_token(redis_client: Any, token: str) -> Optional[Dict[str, Any]]:
    """Verify a bearer token and return endpoint context, or None.

    Returns dict with keys: org, sup, endpoint_id, role_name.
    Also checks that the endpoint is enabled and updates last_accessed_at.
    This function is importable by odata.py for auth.
    """
    token_hash = _hash_token(token)
    raw = redis_client.get(_odata_token_key(token_hash))
    if not raw:
        return None
    data = json.loads(raw if isinstance(raw, str) else raw.decode("utf-8"))

    # Check endpoint is enabled
    ep_key = _odata_doc_key(data["org"], data["sup"], data["endpoint_id"])
    enabled_raw = redis_client.hget(ep_key, "enabled")
    if enabled_raw:
        enabled_str = enabled_raw if isinstance(enabled_raw, str) else enabled_raw.decode("utf-8")
        if enabled_str == "false":
            return None

    # Update last_accessed_at
    redis_client.hset(
        ep_key,
        "last_accessed_at",
        datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    )

    return data


# ---------------------------------------------------------------------------
# Route attachment
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Route attachment — no-op stub.
# All endpoints previously registered here have moved to supertable.api.api.
# This function is preserved so existing callers do not break.
# ---------------------------------------------------------------------------
