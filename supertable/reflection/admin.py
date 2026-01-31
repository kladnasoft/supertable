from __future__ import annotations

import hashlib
import json
import secrets
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response


def attach_admin_routes(
    router: APIRouter,
    *,
    templates: Any,
    settings: Any,
    redis_client: Any,
    catalog: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Response], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], List[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Any],
    get_session: Callable[[Request], Optional[Dict[str, Any]]],
    list_users: Callable[[str, str], List[Dict[str, Any]]],
    fmt_ts: Callable[[int], str],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
    dotenv_values: Any,
    set_key: Any,
) -> None:
    """Attach admin.html-related endpoints to an existing router."""

    # Bind names used by the legacy code for minimal diffs
    _is_authorized = is_authorized
    _no_store = no_store
    _get_provided_token = get_provided_token

    # ------------------------------ Users/Roles readers (admin-only) ------------------------------

    def _r_type(key: str) -> str:
        try:
            return redis_client.type(key)
        except Exception:
            return "none"

    def _read_string_json(key: str) -> Optional[Dict[str, Any]]:
        raw = redis_client.get(key)
        if not raw:
            return None
        try:
            return json.loads(raw)
        except Exception:
            return {"value": raw}

    def _read_hash(key: str) -> Optional[Dict[str, Any]]:
        try:
            data = redis_client.hgetall(key)
            return data or None
        except Exception:
            return None

    def list_roles(org: str, sup: str) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        pattern = f"supertable:{org}:{sup}:meta:roles:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                if ":type_to_hash:" in k:
                    continue
                tail = k.rsplit(":", 1)[-1]
                if tail == "meta":
                    continue
                t = _r_type(k)
                doc: Optional[Dict[str, Any]] = None
                if t == "string":
                    doc = _read_string_json(k)
                elif t == "hash":
                    doc = _read_hash(k)
                else:
                    continue
                if doc is None:
                    continue
                out.append({"hash": tail, **doc})
            if cursor == 0:
                break
        return out

    def read_user(org: str, sup: str, user_hash: str) -> Optional[Dict[str, Any]]:
        k = f"supertable:{org}:{sup}:meta:users:{user_hash}"
        t = _r_type(k)
        if t == "string":
            return _read_string_json(k)
        if t == "hash":
            return _read_hash(k)
        return None

    def read_role(org: str, sup: str, role_hash: str) -> Optional[Dict[str, Any]]:
        k = f"supertable:{org}:{sup}:meta:roles:{role_hash}"
        t = _r_type(k)
        if t == "string":
            return _read_string_json(k)
        if t == "hash":
            return _read_hash(k)
        return None

    # ------------------------------ Auth tokens (admin-only) ------------------------------

    def _get_org_from_env_fallback() -> str:
        return (settings.SUPERTABLE_ORGANIZATION or "").strip()

    def _catalog_list_tokens(org: str) -> List[Dict[str, Any]]:
        if not org:
            return []
        try:
            return catalog.list_auth_tokens(org)  # type: ignore[attr-defined]
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

    def _catalog_create_token(org: str, created_by: str, label: Optional[str]) -> Dict[str, Any]:
        if not org:
            raise HTTPException(status_code=400, detail="Missing organization")
        try:
            return catalog.create_auth_token(org=org, created_by=created_by, label=label)  # type: ignore[attr-defined]
        except Exception:
            token = secrets.token_urlsafe(24)
            token_id = hashlib.sha256(token.encode("utf-8")).hexdigest()
            meta = {
                "token_id": token_id,
                "created_ms": int(time.time() * 1000),
                "created_by": str(created_by or ""),
                "label": (str(label).strip() if label is not None else ""),
                "enabled": True,
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
            return bool(catalog.delete_auth_token(org=org, token_id=token_id))  # type: ignore[attr-defined]
        except Exception:
            try:
                return bool(redis_client.hdel(f"supertable:{org}:auth:tokens", token_id))
            except Exception:
                return False

    # ------------------------------ .env helpers (admin-only) ------------------------------

    _SENSITIVE_KEY_PARTS = (
        "PASSWORD",
        "PASS",
        "SECRET",
        "TOKEN",
        "KEY",
        "ACCESS_KEY",
        "CONNECTION_STRING",
        "API_KEY",
        "CLIENT_SECRET",
    )

    def _env_file_path() -> Path:
        here = Path(__file__).resolve()
        reflection_dir = here.parent
        pkg_dir = reflection_dir.parent
        # project root is one level up from package root
        return (pkg_dir.parent / (settings.DOTENV_PATH or ".env")).resolve()

    def _is_sensitive_env_key(key: str) -> bool:
        k = (key or "").upper()
        return any(part in k for part in _SENSITIVE_KEY_PARTS)

    def _mask_secret(v: str) -> str:
        if not v:
            return ""
        if len(v) <= 4:
            return "****"
        return "****" + v[-4:]

    import re as _re
    _ENV_KEY_RE = _re.compile(r"^[A-Z0-9_]+$")

    # ------------------------------ Routes used by admin.html ------------------------------

    @router.get("/reflection/tenants")
    def api_tenants(_: Any = Depends(logged_in_guard_api)):
        pairs = discover_pairs()
        return {"tenants": [{"org": o, "sup": s} for o, s in pairs]}

    @router.get("/reflection/mirrors")
    def api_get_mirrors(
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _: Any = Depends(logged_in_guard_api),
    ):
        org, sup = resolve_pair(org, sup)
        if not org or not sup:
            return {"org": org, "sup": sup, "formats": []}
        try:
            fmts = catalog.get_mirrors(org, sup)
        except Exception:
            fmts = []
        return {"org": org, "sup": sup, "formats": fmts}

    @router.get("/reflection/users")
    def api_users(org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
        org, sup = resolve_pair(org, sup)
        if not org or not sup:
            return {"users": []}
        return {"users": list_users(org, sup)}

    @router.get("/reflection/roles")
    def api_roles(org: Optional[str] = Query(None), sup: Optional[str] = Query(None), _=Depends(admin_guard_api)):
        org, sup = resolve_pair(org, sup)
        if not org or not sup:
            return {"roles": []}
        return {"roles": list_roles(org, sup)}

    @router.get("/reflection/user/{user_hash}")
    def api_user_details(
        user_hash: str,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _=Depends(admin_guard_api),
    ):
        org, sup = resolve_pair(org, sup)
        if not org or not sup:
            raise HTTPException(404, "Tenant not found")
        obj = read_user(org, sup, user_hash)
        if not obj:
            raise HTTPException(status_code=404, detail="User not found")
        return {"hash": user_hash, "data": obj}

    @router.get("/reflection/role/{role_hash}")
    def api_role_details(
        role_hash: str,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        _=Depends(admin_guard_api),
    ):
        org, sup = resolve_pair(org, sup)
        if not org or not sup:
            raise HTTPException(404, "Tenant not found")
        obj = read_role(org, sup, role_hash)
        if not obj:
            raise HTTPException(status_code=404, detail="Role not found")
        return {"hash": role_hash, "data": obj}

    @router.get("/reflection/env")
    def admin_env_get(_=Depends(admin_guard_api)):
        if dotenv_values is None:
            raise HTTPException(status_code=500, detail="python-dotenv is not installed")

        env_path = _env_file_path()
        found = env_path.exists() and env_path.is_file()
        items: List[Dict[str, Any]] = []

        if found:
            values = dotenv_values(str(env_path)) or {}
            for k, v in values.items():
                val = "" if v is None else str(v)
                is_sensitive = _is_sensitive_env_key(k)
                items.append(
                    {
                        "key": k,
                        "value": _mask_secret(val) if is_sensitive else val,
                        "is_sensitive": is_sensitive,
                    }
                )

        return {"found": found, "path": str(env_path), "items": items}

    @router.post("/reflection/env")
    def admin_env_update(payload: Dict[str, Any] = Body(...), _=Depends(admin_guard_api)):
        if set_key is None:
            raise HTTPException(status_code=500, detail="python-dotenv is not installed")

        env_path = _env_file_path()
        env_path.touch(exist_ok=True)

        items = payload.get("items") or []
        if not isinstance(items, list):
            raise HTTPException(status_code=400, detail="items must be a list")

        for item in items:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            if not key or not _ENV_KEY_RE.fullmatch(key):
                continue
            value = item.get("value", "")
            value_str = str(value)
            if "\n" in value_str or "\r" in value_str:
                raise HTTPException(status_code=400, detail=f"Invalid value for {key}")
            if len(value_str) > 8192:
                raise HTTPException(status_code=400, detail=f"Value too long for {key}")
            set_key(str(env_path), key, value_str, quote_mode="never")

        return {"ok": True, "path": str(env_path)}

    @router.get("/reflection/tokens")
    def api_list_tokens(request: Request, org: str = Query(None), _: Any = Depends(admin_guard_api)):
        if not _is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        org_eff = (org or _get_org_from_env_fallback()).strip()
        tokens = _catalog_list_tokens(org_eff)
        return {"ok": True, "organization": org_eff, "tokens": tokens}

    @router.post("/reflection/tokens")
    def api_create_token(request: Request, org: str = Query(None), label: str = Query(""), _: Any = Depends(admin_guard_api)):
        if not _is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        org_eff = (org or _get_org_from_env_fallback()).strip()
        created = _catalog_create_token(org_eff, created_by="superuser", label=label)
        return {"ok": True, "organization": org_eff, **created}

    @router.delete("/reflection/tokens/{token_id}")
    def api_delete_token(request: Request, token_id: str, org: str = Query(None), _: Any = Depends(admin_guard_api)):
        if not _is_authorized(request):
            raise HTTPException(status_code=401, detail="Unauthorized")
        org_eff = (org or _get_org_from_env_fallback()).strip()
        ok = _catalog_delete_token(org_eff, token_id)
        if not ok:
            raise HTTPException(status_code=404, detail="Token not found")
        return {"ok": True, "organization": org_eff, "token_id": token_id}

    @router.get("/reflection/admin", response_class=HTMLResponse)
    def admin_page(request: Request, org: Optional[str] = Query(None), sup: Optional[str] = Query(None)):
        if not _is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            _no_store(resp)
            return resp

        provided = _get_provided_token(request) or ""

        pairs = discover_pairs()
        sel_org, sel_sup = resolve_pair(org, sup)

        tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

        if not sel_org or not sel_sup:
            sess = get_session(request) or {}
            resp = templates.TemplateResponse(
                "admin.html",
                {
                    "request": request,
                    "session_org": sess.get("org") or settings.SUPERTABLE_ORGANIZATION,
                    "session_username": sess.get("username") or "",
                    "session_user_hash": sess.get("user_hash") or "",
                    "session_is_superuser": bool(sess.get("is_superuser")),
                    "authorized": True,
                    "token": provided,
                    "tenants": tenants,
                    "sel_org": sel_org,
                    "sel_sup": sel_sup,
                    "has_tenant": False,
                    "default_user_hash": "",
                },
            )
            _no_store(resp)
            return resp

        try:
            root = catalog.get_root(sel_org, sel_sup) or {}
        except Exception:
            root = {}
        try:
            mirrors = catalog.get_mirrors(sel_org, sel_sup) or []
        except Exception:
            mirrors = []

        users = list_users(sel_org, sel_sup)
        roles = list_roles(sel_org, sel_sup)
        default_user_hash = users[0]["hash"] if users else ""

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": True,
            "root_version": int(root.get("version", -1)) if isinstance(root, dict) else -1,
            "root_ts": fmt_ts(int(root.get("ts", 0))) if isinstance(root, dict) else "—",
            "mirrors": mirrors,
            "users": users,
            "roles": roles,
            "default_user_hash": default_user_hash,
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("admin.html", ctx)
        _no_store(resp)
        return resp
