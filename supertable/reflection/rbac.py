from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse


_VIEW_ID_RE = re.compile(r"^[a-f0-9]{32}$")


def _now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _clean_sql_query(query: str) -> str:
    # remove -- ... and /* ... */ and trailing semicolons
    q = re.sub(r"--.*$", "", query, flags=re.MULTILINE)
    q = re.sub(r"/\*.*?\*/", "", q, flags=re.DOTALL)
    q = re.sub(r";+$", "", q)
    return q.strip()


def _validate_view_query(query: str) -> str:
    q = _clean_sql_query(str(query or "")).strip()
    if not q:
        raise HTTPException(status_code=400, detail="query is required")

    ql = q.lower().lstrip()
    if not ql.startswith(("select", "with")):
        raise HTTPException(status_code=400, detail="Only SELECT or WITH (CTE) queries are allowed")

    # Prevent multiple statements (simple guard; matches the existing Django implementation style).
    if ";" in q:
        raise HTTPException(status_code=400, detail="Only single-statement queries are allowed")

    return q


def _views_key(org: str, sup: str, view_id: str) -> str:
    return f"supertable:{org}:{sup}:meta:views:{view_id}"


def _views_pattern(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:views:*"


def _decode_redis_hash(doc: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in (doc or {}).items():
        kk = k.decode("utf-8") if isinstance(k, (bytes, bytearray)) else str(k)
        if isinstance(v, (bytes, bytearray)):
            vv: Any = v.decode("utf-8")
        else:
            vv = v
        out[kk] = vv

    # JSON fields
    roles_raw = out.get("roles")
    if isinstance(roles_raw, str) and roles_raw:
        try:
            out["roles"] = json.loads(roles_raw)
        except Exception:
            out["roles"] = [roles_raw]
    elif roles_raw is None:
        out["roles"] = []

    # int fields
    for f in ("created_ts", "updated_ts"):
        try:
            if f in out:
                out[f] = int(out[f])
        except Exception:
            pass

    return out


def list_views(redis_client: Any, org: str, sup: str, limit: int = 1000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    pattern = _views_pattern(org, sup)

    cursor = 0
    scanned = 0
    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=500)
        for key in keys:
            k = key.decode("utf-8") if isinstance(key, (bytes, bytearray)) else str(key)
            view_id = k.rsplit(":", 1)[-1]
            if not _VIEW_ID_RE.fullmatch(view_id):
                continue

            doc = redis_client.hgetall(k) or {}
            decoded = _decode_redis_hash(doc)
            decoded["id"] = view_id
            out.append(decoded)

            scanned += 1
            if scanned >= limit:
                cursor = 0
                break
        if cursor == 0:
            break

    out.sort(key=lambda d: int(d.get("updated_ts") or 0), reverse=True)
    return out


def get_view(redis_client: Any, org: str, sup: str, view_id: str) -> Optional[Dict[str, Any]]:
    key = _views_key(org, sup, view_id)
    doc = redis_client.hgetall(key)
    if not doc:
        return None
    decoded = _decode_redis_hash(doc)
    decoded["id"] = view_id
    return decoded


def create_view(
    redis_client: Any,
    org: str,
    sup: str,
    *,
    name: str,
    query: str,
    description: str = "",
    created_by: str = "",
    created_by_hash: str = "",
) -> Dict[str, Any]:
    nm = (name or "").strip()
    if not nm:
        raise HTTPException(status_code=400, detail="name is required")
    if len(nm) > 120:
        raise HTTPException(status_code=400, detail="name is too long")

    q = _validate_view_query(query)
    desc = (description or "").strip()
    if len(desc) > 2000:
        raise HTTPException(status_code=400, detail="description is too long")

    view_id = uuid.uuid4().hex
    now_ts = _now_ts()
    now_iso = _now_iso()

    key = _views_key(org, sup, view_id)
    mapping = {
        "name": nm,
        "query": q,
        "description": desc,
        "roles": json.dumps([]),
        "created_ts": str(now_ts),
        "updated_ts": str(now_ts),
        "created_at": now_iso,
        "updated_at": now_iso,
        "created_by": (created_by or "").strip(),
        "created_by_hash": (created_by_hash or "").strip(),
        "enabled": "1",
    }
    redis_client.hset(key, mapping=mapping)

    out = dict(mapping)
    out["roles"] = []
    out["id"] = view_id
    out["created_ts"] = now_ts
    out["updated_ts"] = now_ts
    return out


def update_view(
    redis_client: Any,
    org: str,
    sup: str,
    view_id: str,
    *,
    name: Optional[str] = None,
    query: Optional[str] = None,
    description: Optional[str] = None,
    enabled: Optional[bool] = None,
    roles: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if not _VIEW_ID_RE.fullmatch(view_id or ""):
        raise HTTPException(status_code=400, detail="invalid view id")

    key = _views_key(org, sup, view_id)
    if not redis_client.exists(key):
        raise HTTPException(status_code=404, detail="view not found")

    mapping: Dict[str, str] = {}

    if name is not None:
        nm = (name or "").strip()
        if not nm:
            raise HTTPException(status_code=400, detail="name is required")
        if len(nm) > 120:
            raise HTTPException(status_code=400, detail="name is too long")
        mapping["name"] = nm

    if query is not None:
        mapping["query"] = _validate_view_query(query)

    if description is not None:
        desc = (description or "").strip()
        if len(desc) > 2000:
            raise HTTPException(status_code=400, detail="description is too long")
        mapping["description"] = desc

    if enabled is not None:
        mapping["enabled"] = "1" if bool(enabled) else "0"

    if roles is not None:
        # Stored as JSON for future role assignment.
        if not isinstance(roles, list):
            raise HTTPException(status_code=400, detail="roles must be a list")
        mapping["roles"] = json.dumps([str(x) for x in roles if str(x).strip()])

    now_ts = _now_ts()
    now_iso = _now_iso()
    mapping["updated_ts"] = str(now_ts)
    mapping["updated_at"] = now_iso

    if mapping:
        redis_client.hset(key, mapping=mapping)

    doc = get_view(redis_client, org, sup, view_id)
    if not doc:
        raise HTTPException(status_code=404, detail="view not found")
    return doc


def delete_view(redis_client: Any, org: str, sup: str, view_id: str) -> bool:
    if not _VIEW_ID_RE.fullmatch(view_id or ""):
        raise HTTPException(status_code=400, detail="invalid view id")
    key = _views_key(org, sup, view_id)
    return bool(redis_client.delete(key))


def attach_rbac_routes(
    router: APIRouter,
    *,
    templates: Any,
    settings: Any,
    redis_client: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], str],
    discover_pairs: Callable[[], List[List[str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Any],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Dict[str, Any]],
    get_session: Callable[[Request], Optional[Dict[str, Any]]],
    admin_guard_api: Callable[..., Any],
) -> None:
    @router.get("/reflection/rbac", response_class=HTMLResponse)
    def rbac_page(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
    ):
        if not is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            no_store(resp)
            return resp

        provided = get_provided_token(request) or ""

        pairs = discover_pairs()
        sel_org, sel_sup = resolve_pair(org, sup)
        tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": bool(sel_org and sel_sup),
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("rbac.html", ctx)
        no_store(resp)
        return resp

    @router.get("/reflection/rbac/views")
    def rbac_views_list(
        organization: str = Query(""),
        super_name: str = Query(""),
        _=Depends(admin_guard_api),
    ):
        org = (organization or "").strip()
        sup = (super_name or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="organization and super_name are required")

        items = list_views(redis_client, org, sup)
        return JSONResponse({"ok": True, "data": {"items": items}})

    @router.get("/reflection/rbac/views/{view_id}")
    def rbac_view_get(
        view_id: str,
        organization: str = Query(""),
        super_name: str = Query(""),
        _=Depends(admin_guard_api),
    ):
        org = (organization or "").strip()
        sup = (super_name or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="organization and super_name are required")

        if not _VIEW_ID_RE.fullmatch(view_id or ""):
            raise HTTPException(status_code=400, detail="invalid view id")

        doc = get_view(redis_client, org, sup, view_id)
        if not doc:
            raise HTTPException(status_code=404, detail="view not found")

        return JSONResponse({"ok": True, "data": doc})

    @router.post("/reflection/rbac/views")
    def rbac_view_create(
        request: Request,
        payload: Dict[str, Any] = Body(...),
        _=Depends(admin_guard_api),
    ):
        org = str(payload.get("organization") or "").strip()
        sup = str(payload.get("super_name") or "").strip()
        name = str(payload.get("name") or "").strip()
        query = str(payload.get("query") or "")
        description = str(payload.get("description") or "")

        if not org or not sup:
            raise HTTPException(status_code=400, detail="organization and super_name are required")

        sess = get_session(request) or {}
        created_by = str(sess.get("username") or "").strip()
        created_by_hash = str(sess.get("user_hash") or "").strip()

        doc = create_view(
            redis_client,
            org,
            sup,
            name=name,
            query=query,
            description=description,
            created_by=created_by,
            created_by_hash=created_by_hash,
        )
        return JSONResponse({"ok": True, "data": doc})

    @router.put("/reflection/rbac/views/{view_id}")
    def rbac_view_update(
        view_id: str,
        payload: Dict[str, Any] = Body(...),
        _=Depends(admin_guard_api),
    ):
        org = str(payload.get("organization") or "").strip()
        sup = str(payload.get("super_name") or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="organization and super_name are required")

        name = payload.get("name")
        query = payload.get("query")
        description = payload.get("description")
        enabled_raw = payload.get("enabled")
        roles_raw = payload.get("roles")

        enabled: Optional[bool]
        if enabled_raw is None:
            enabled = None
        else:
            enabled = bool(enabled_raw)

        roles: Optional[List[str]]
        if roles_raw is None:
            roles = None
        elif isinstance(roles_raw, list):
            roles = [str(x) for x in roles_raw]
        else:
            raise HTTPException(status_code=400, detail="roles must be a list")

        doc = update_view(
            redis_client,
            org,
            sup,
            view_id,
            name=str(name) if name is not None else None,
            query=str(query) if query is not None else None,
            description=str(description) if description is not None else None,
            enabled=enabled,
            roles=roles,
        )
        return JSONResponse({"ok": True, "data": doc})

    @router.delete("/reflection/rbac/views/{view_id}")
    def rbac_view_delete(
        view_id: str,
        organization: str = Query(""),
        super_name: str = Query(""),
        _=Depends(admin_guard_api),
    ):
        org = (organization or "").strip()
        sup = (super_name or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="organization and super_name are required")

        deleted = delete_view(redis_client, org, sup, view_id)
        return JSONResponse({"ok": True, "data": {"deleted": deleted}})
