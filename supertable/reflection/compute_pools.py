from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def _state_dir() -> Path:
    base = Path(os.getenv("SUPERTABLE_REFLECTION_STATE_DIR", "/tmp/supertable_reflection"))
    d = base / "compute_pools"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _safe_slug(v: str) -> str:
    v = (v or "").strip()
    out = []
    for ch in v:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:200] or "x"


def _tenant_path(org: str, sup: str) -> Path:
    return _state_dir() / f"{_safe_slug(org)}__{_safe_slug(sup)}.json"


def _load(org: str, sup: str) -> Dict[str, Any]:
    p = _tenant_path(org, sup)
    if not p.exists():
        # sensible default
        return {
            "items": [
                {
                    "id": "default",
                    "name": "Default",
                    "kind": "in-process",
                    "size": "small",
                    "max_concurrency": 1,
                    "is_default": True,
                    "notes": "Current notebook execution runs in-process. Pool binding will be added later.",
                }
            ]
        }
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"items": []}


def _save(org: str, sup: str, data: Dict[str, Any]) -> None:
    p = _tenant_path(org, sup)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def attach_compute_pools_routes(
    router: APIRouter,
    *,
    templates: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], Sequence[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], None],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
) -> None:
    @router.get("/reflection/compute-pools", response_class=HTMLResponse)
    def compute_pools_page(
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
        resp = templates.TemplateResponse("compute_pools.html", ctx)
        no_store(resp)
        return resp

    @router.get("/reflection/compute-pools/list")
    def pools_list(org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        return {"ok": True, "data": _load(org, sup)}

    @router.post("/reflection/compute-pools/upsert")
    def pools_upsert(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        item = payload.get("item") or {}
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="item must be an object")

        data = _load(org, sup)
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []

        item_id = str(item.get("id") or "").strip() or os.urandom(6).hex()
        item["id"] = item_id

        # ensure default logic
        if item.get("is_default"):
            for it in items:
                if isinstance(it, dict):
                    it["is_default"] = False

        out = []
        replaced = False
        for it in items:
            if isinstance(it, dict) and str(it.get("id")) == item_id:
                out.append(item)
                replaced = True
            else:
                out.append(it)
        if not replaced:
            out.insert(0, item)

        # if none default, set first
        if not any(isinstance(it, dict) and it.get("is_default") for it in out) and out:
            if isinstance(out[0], dict):
                out[0]["is_default"] = True

        data["items"] = out
        _save(org, sup, data)
        return {"ok": True, "id": item_id}

    @router.delete("/reflection/compute-pools/{pool_id}")
    def pools_delete(pool_id: str, org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        data = _load(org, sup)
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []
        data["items"] = [it for it in items if not (isinstance(it, dict) and str(it.get("id")) == pool_id)]
        if not any(isinstance(it, dict) and it.get("is_default") for it in data["items"]) and data["items"]:
            if isinstance(data["items"][0], dict):
                data["items"][0]["is_default"] = True
        _save(org, sup, data)
        return {"ok": True}
