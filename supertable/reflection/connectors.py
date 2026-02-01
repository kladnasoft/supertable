from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Sequence, Tuple
from urllib.parse import urlparse

import requests
from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


# ---------------------------------------------------------------------------
# Persistence (simple JSON on disk)
# ---------------------------------------------------------------------------

def _state_dir() -> Path:
    base = Path(os.getenv("SUPERTABLE_REFLECTION_STATE_DIR", "/tmp/supertable_reflection"))
    d = base / "connectors"
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


def _load_saved(org: str, sup: str) -> Dict[str, Any]:
    p = _tenant_path(org, sup)
    if not p.exists():
        return {"items": []}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {"items": []}


def _save_saved(org: str, sup: str, data: Dict[str, Any]) -> None:
    p = _tenant_path(org, sup)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Airbyte API helpers
# ---------------------------------------------------------------------------

def _validate_base_url(base_url: str) -> str:
    base_url = (base_url or "").strip()
    if not base_url:
        raise HTTPException(status_code=400, detail="base_url is required")
    u = urlparse(base_url)
    if u.scheme not in ("http", "https"):
        raise HTTPException(status_code=400, detail="base_url must be http(s)")
    return base_url.rstrip("/")


def _airbyte_post(base_url: str, path: str, payload: Dict[str, Any]) -> Any:
    base_url = _validate_base_url(base_url)
    url = f"{base_url}{path}"
    try:
        r = requests.post(url, json=payload, timeout=25)
        r.raise_for_status()
        return r.json()
    except requests.HTTPError as e:
        detail = ""
        try:
            detail = r.text  # type: ignore[name-defined]
        except Exception:
            detail = str(e)
        raise HTTPException(status_code=502, detail=f"Airbyte error: {detail}") from e
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Airbyte request failed: {e}") from e


def attach_connectors_routes(
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
    @router.get("/reflection/connectors", response_class=HTMLResponse)
    def connectors_page(
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
        resp = templates.TemplateResponse("connectors.html", ctx)
        no_store(resp)
        return resp

    # --- Airbyte discovery endpoints (server-side proxy to avoid CORS) ---

    @router.post("/reflection/connectors/airbyte/workspaces/list")
    def airbyte_list_workspaces(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        base_url = str(payload.get("base_url") or "")
        return _airbyte_post(base_url, "/api/v1/workspaces/list", {})

    @router.post("/reflection/connectors/airbyte/source_definitions/list")
    def airbyte_list_source_defs(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        base_url = str(payload.get("base_url") or "")
        return _airbyte_post(base_url, "/api/v1/source_definitions/list", {})

    @router.post("/reflection/connectors/airbyte/destination_definitions/list")
    def airbyte_list_dest_defs(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        base_url = str(payload.get("base_url") or "")
        return _airbyte_post(base_url, "/api/v1/destination_definitions/list", {})

    @router.post("/reflection/connectors/airbyte/sources/create")
    def airbyte_create_source(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        base_url = str(payload.get("base_url") or "")
        body = {
            "name": payload.get("name"),
            "workspaceId": payload.get("workspaceId"),
            "sourceDefinitionId": payload.get("sourceDefinitionId"),
            "connectionConfiguration": payload.get("connectionConfiguration") or {},
        }
        return _airbyte_post(base_url, "/api/v1/sources/create", body)

    @router.post("/reflection/connectors/airbyte/destinations/create")
    def airbyte_create_destination(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        base_url = str(payload.get("base_url") or "")
        body = {
            "name": payload.get("name"),
            "workspaceId": payload.get("workspaceId"),
            "destinationDefinitionId": payload.get("destinationDefinitionId"),
            "connectionConfiguration": payload.get("connectionConfiguration") or {},
        }
        return _airbyte_post(base_url, "/api/v1/destinations/create", body)

    # --- Saved connectors (our app state) ---

    @router.get("/reflection/connectors/saved")
    def saved_list(org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org and sup required")
        return {"ok": True, "data": _load_saved(org, sup)}

    @router.post("/reflection/connectors/saved/upsert")
    def saved_upsert(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        item = payload.get("item") or {}
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="item must be an object")

        data = _load_saved(org, sup)
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []

        item_id = str(item.get("id") or "").strip() or os.urandom(8).hex()
        item["id"] = item_id

        # replace or insert
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
        data["items"] = out
        _save_saved(org, sup, data)
        return {"ok": True, "id": item_id}

    @router.delete("/reflection/connectors/saved/{item_id}")
    def saved_delete(item_id: str, org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        data = _load_saved(org, sup)
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []
        data["items"] = [it for it in items if not (isinstance(it, dict) and str(it.get("id")) == item_id)]
        _save_saved(org, sup, data)
        return {"ok": True}



    # --- PyAirbyte helpers (best-effort; APIs may vary by version) ---

    @router.get("/reflection/connectors/pyairbyte/connectors")
    def pyairbyte_connectors(_: Any = Depends(logged_in_guard_api)):
        """Best-effort listing of connectors from PyAirbyte (if installed).

        PyAirbyte's public API has changed across versions, so we try a few known entry-points.
        """
        try:
            import pyairbyte  # type: ignore
        except Exception:
            return {"ok": True, "installed": False, "connectors": [], "hint": "pip install pyairbyte"}

        # Try common entrypoints
        candidates = [
            "get_available_sources",
            "get_available_connectors",
            "available_connectors",
            "list_connectors",
            "sources",
            "connectors",
        ]
        for name in candidates:
            fn = getattr(pyairbyte, name, None)
            try:
                if callable(fn):
                    res = fn()
                    # normalize list of names
                    if isinstance(res, dict):
                        keys = list(res.keys())
                        return {"ok": True, "installed": True, "api": name, "connectors": keys}
                    if isinstance(res, (list, tuple, set)):
                        out = []
                        for it in res:
                            if isinstance(it, str):
                                out.append(it)
                            elif isinstance(it, dict) and "name" in it:
                                out.append(it["name"])
                            else:
                                out.append(str(it))
                        return {"ok": True, "installed": True, "api": name, "connectors": out}
                    return {"ok": True, "installed": True, "api": name, "connectors": [str(res)]}
            except Exception:
                continue

        return {
            "ok": True,
            "installed": True,
            "api": "unknown",
            "connectors": [],
            "hint": "PyAirbyte is installed but no known list API found. Upgrade/downgrade PyAirbyte or rely on Airbyte API listing on this page.",
            "module_attrs": [a for a in dir(pyairbyte) if not a.startswith('_')][:60],
        }

    @router.post("/reflection/connectors/pyairbyte/create")
    def pyairbyte_create(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        """Best-effort creation of a connector object via PyAirbyte (if installed).

        This does NOT start a sync by itself; it primarily validates config and returns a normalized payload
        that you can save in SuperTable (and/or later bind to an Airbyte server).
        """
        try:
            import pyairbyte  # type: ignore
        except Exception as e:
            raise HTTPException(status_code=400, detail="PyAirbyte not installed. pip install pyairbyte") from e

        kind = str(payload.get("kind") or "source").strip().lower()
        connector = str(payload.get("connector") or "").strip()
        config = payload.get("config") or {}

        if not connector:
            raise HTTPException(status_code=400, detail="connector is required")
        if not isinstance(config, dict):
            raise HTTPException(status_code=400, detail="config must be an object")

        # Try common factories
        factories = []
        if kind == "source":
            factories = ["get_source", "source", "Source", "get_connector"]
        else:
            factories = ["get_destination", "destination", "Destination", "get_connector"]

        last_err: Optional[str] = None
        for name in factories:
            fn = getattr(pyairbyte, name, None)
            if not callable(fn):
                continue
            try:
                obj = fn(connector, config)  # type: ignore[misc]
                # We don't serialize the object; we return validated payload.
                return {"ok": True, "kind": kind, "connector": connector, "config": config, "pyairbyte_factory": name}
            except Exception as e:
                last_err = str(e)
                continue

        raise HTTPException(
            status_code=400,
            detail=f"PyAirbyte installed, but could not create connector via known factories. Last error: {last_err}",
        )


    # --- Optional PyAirbyte hint endpoint (non-fatal if missing) ---
    @router.get("/reflection/connectors/pyairbyte/status")
    def pyairbyte_status(_: Any = Depends(logged_in_guard_api)):
        try:
            import pyairbyte  # type: ignore  # noqa: F401
            return {"ok": True, "installed": True}
        except Exception:
            return {"ok": True, "installed": False, "hint": "pip install pyairbyte"}
