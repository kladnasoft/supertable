from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse


# ---------------------------------------------------------------------------
# Redis key helpers — mirror the namespace used by _FallbackCatalog / RedisCatalog
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


def _list_users(redis_client: Any, org: str, sup: str) -> List[Dict[str, Any]]:
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
) -> Dict[str, Any]:
    nm = (username or "").strip()
    if not nm:
        raise HTTPException(status_code=400, detail="username is required")
    if len(nm) > 120:
        raise HTTPException(status_code=400, detail="username is too long")

    # Prevent duplicate usernames
    existing_id = redis_client.hget(_user_name_map_key(org, sup), nm.lower())
    if existing_id:
        raise HTTPException(status_code=409, detail="username already exists")

    user_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    mapping = {
        "username": nm,
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
) -> Dict[str, Any]:
    if not _USER_ID_RE.fullmatch(user_id or ""):
        raise HTTPException(status_code=400, detail="invalid user_id")
    doc_key = _user_doc_key(org, sup, user_id)
    if not redis_client.exists(doc_key):
        raise HTTPException(status_code=404, detail="user not found")

    mapping: Dict[str, str] = {}
    if username is not None:
        nm = (username or "").strip()
        if not nm:
            raise HTTPException(status_code=400, detail="username is required")
        # Update name map: remove old, add new
        existing = _get_user(redis_client, org, sup, user_id)
        old_nm = (existing or {}).get("username", "")
        if old_nm and old_nm.lower() != nm.lower():
            redis_client.hdel(_user_name_map_key(org, sup), old_nm.lower())
            redis_client.hset(_user_name_map_key(org, sup), nm.lower(), user_id)
        mapping["username"] = nm
    if roles is not None:
        mapping["roles"] = json.dumps([str(r) for r in roles])

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
# Route attachment
# ---------------------------------------------------------------------------


