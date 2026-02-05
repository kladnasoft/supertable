from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


KINDS = ("in-process", "spark", "kubernetes")
SIZES = ("small", "medium", "large")

def _notebook_port() -> int:
    raw = str(os.getenv("SUPERTABLE_NOTEBOOK_PORT", "8010") or "8010").strip()
    try:
        port = int(raw)
    except Exception:
        port = 8010
    if port < 1 or port > 65535:
        port = 8010
    return port


def _default_ws_url() -> str:
    return f"ws://0.0.0.0:{_notebook_port()}/ws/execute"


def _redis_url() -> Optional[str]:
    url = os.getenv("SUPERTABLE_REFLECTION_REDIS_URL") or os.getenv("SUPERTABLE_REDIS_URL") or os.getenv("REDIS_URL")
    url = str(url or "").strip()
    return url or None


def _redis_client():
    url = _redis_url()
    if not url:
        return None
    try:
        import redis  # type: ignore
    except Exception:
        return None
    try:
        return redis.Redis.from_url(  # type: ignore[attr-defined]
            url,
            decode_responses=True,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
        )
    except Exception:
        return None


def _redis_key(org: str, sup: str) -> str:
    return f"supertable:reflection:compute:{_safe_slug(org)}__{_safe_slug(sup)}"


def _read_state(org: str, sup: str) -> Optional[Dict[str, Any]]:
    client = _redis_client()
    if not client:
        return None
    try:
        raw = client.get(_redis_key(org, sup))
    except Exception:
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _write_state(org: str, sup: str, data: Dict[str, Any]) -> bool:
    client = _redis_client()
    if not client:
        return False
    try:
        client.set(_redis_key(org, sup), json.dumps(data, ensure_ascii=False, indent=2))
        return True
    except Exception:
        return False



def _state_dir() -> Path:
    base = Path(os.getenv("SUPERTABLE_REFLECTION_STATE_DIR", "/tmp/supertable_reflection"))
    new_dir = base / "compute"
    old_dir = base / "compute"

    # Ensure the new directory exists.
    new_dir.mkdir(parents=True, exist_ok=True)

    # Best-effort migration: if legacy files exist under compute_pools/, copy them once.
    try:
        if old_dir.exists() and old_dir.is_dir():
            for p in old_dir.glob("*.json"):
                dest = new_dir / p.name
                if not dest.exists():
                    shutil.copy2(p, dest)
    except Exception:
        # Never fail requests due to migration issues.
        pass

    return new_dir


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


def _default_data() -> Dict[str, Any]:
    return {
        "items": [
            {
                "id": "default",
                "name": "Default",
                "kind": "in-process",
                "size": "small",
                "max_concurrency": 1,
                "has_internet": False,
                "ws_url": _default_ws_url(),
                "is_default": True,
            }
        ]
    }


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _validate_kind(v: Any) -> str:
    s = str(v or "").strip().lower()
    if not s:
        return "in-process"
    if s not in KINDS:
        raise HTTPException(status_code=400, detail=f"invalid kind (allowed: {', '.join(KINDS)})")
    return s


def _validate_size(v: Any) -> str:
    s = str(v or "").strip().lower()
    if not s:
        return "small"
    if s not in SIZES:
        raise HTTPException(status_code=400, detail=f"invalid size (allowed: {', '.join(SIZES)})")
    return s


def _validate_max_concurrency(v: Any) -> int:
    try:
        n = int(v)
    except Exception:
        n = 1
    if n < 1:
        n = 1
    if n > 10_000:
        raise HTTPException(status_code=400, detail="max_concurrency too large")
    return n


def _validate_ws_url(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return _default_ws_url()
    if len(s) > 2048:
        raise HTTPException(status_code=400, detail="ws_url too long")
    parts = urlsplit(s)
    if parts.scheme not in {"ws", "wss"} or not parts.netloc:
        raise HTTPException(status_code=400, detail="ws_url must be a ws:// or wss:// URL")
    path = parts.path or "/ws/execute"
    if not path.startswith("/"):
        path = "/" + path
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))



def _sanitize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize any incoming item to the recommended schema:

      id, name, kind, size, max_concurrency, has_internet, ws_url, is_default

    Unknown fields are discarded.
    """
    pid = str(item.get("id") or "").strip()
    name = str(item.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    out: Dict[str, Any] = {
        "id": pid,
        "name": name,
        "kind": _validate_kind(item.get("kind")),
        "size": _validate_size(item.get("size")),
        "max_concurrency": _validate_max_concurrency(item.get("max_concurrency", 1)),
        "has_internet": _coerce_bool(item.get("has_internet", item.get("internet", False))),
        "ws_url": _validate_ws_url(item.get("ws_url", item.get("ws", item.get("websocket_url")))),
        "is_default": _coerce_bool(item.get("is_default", False)),
    }

    # Back-compat: if an older client sends "profile", derive has_internet from it (unless explicitly set).
    if "profile" in item and "has_internet" not in item and "internet" not in item:
        prof = str(item.get("profile") or "").strip().lower()
        if prof in {"internet", "high", "on", "true", "1"}:
            out["has_internet"] = True
        elif prof in {"no-internet", "low", "off", "false", "0"}:
            out["has_internet"] = False

    return out



def _load(org: str, sup: str) -> Dict[str, Any]:
    raw: Optional[Dict[str, Any]] = _read_state(org, sup)

    loaded_from_file = False
    if raw is None:
        p = _tenant_path(org, sup)
        if not p.exists():
            return _default_data()

        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            loaded_from_file = True
        except Exception:
            return _default_data()

    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return _default_data()

    out_items = []
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            # tolerate legacy items (e.g. missing fields)
            sanitized = {
                "id": str(it.get("id") or "").strip(),
                "name": str(it.get("name") or "").strip() or "Pool",
                "kind": (str(it.get("kind") or "").strip().lower() or "in-process"),
                "size": (str(it.get("size") or "").strip().lower() or "small"),
                "max_concurrency": it.get("max_concurrency", 1),
                "has_internet": it.get("has_internet", it.get("internet", False)),
                "ws_url": it.get("ws_url", it.get("ws", it.get("websocket_url"))),
                "is_default": it.get("is_default", False),
                "profile": it.get("profile"),
            }
            sanitized = _sanitize_item(sanitized)
        except HTTPException:
            continue
        if not sanitized.get("id"):
            sanitized["id"] = os.urandom(6).hex()
        out_items.append(sanitized)

    if not out_items:
        out_items = _default_data()["items"]

    # ensure exactly one default (or pick first)
    if not any(isinstance(it, dict) and it.get("is_default") for it in out_items) and out_items:
        out_items[0]["is_default"] = True
    else:
        # if multiple defaults, keep the first as default
        seen = False
        for it in out_items:
            if it.get("is_default") and not seen:
                seen = True
            elif it.get("is_default") and seen:
                it["is_default"] = False

    data = {"items": out_items}

    # Best-effort migration: if the state came from disk, persist into Redis (if configured).
    if loaded_from_file:
        _write_state(org, sup, data)

    return data



def _save(org: str, sup: str, data: Dict[str, Any]) -> None:
    # Prefer Redis (shared, durable), with a disk fallback for environments without Redis.
    if _write_state(org, sup, data):
        return
    p = _tenant_path(org, sup)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")



def attach_compute_routes(
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
    @router.get("/reflection/compute", response_class=HTMLResponse)
    def compute_page(
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
            "notebook_port": _notebook_port(),
            "notebook_ws_default": _default_ws_url(),
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("compute.html", ctx)
        no_store(resp)
        return resp

    @router.get("/reflection/compute/list")
    def pools_list(org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        return {"ok": True, "data": _load(org, sup), "kinds": list(KINDS), "sizes": list(SIZES)}

    @router.post("/reflection/compute/upsert")
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

        sanitized = _sanitize_item(item)

        item_id = str(sanitized.get("id") or "").strip() or os.urandom(6).hex()
        sanitized["id"] = item_id

        # ensure default logic
        if sanitized.get("is_default"):
            for it in items:
                if isinstance(it, dict):
                    it["is_default"] = False

        out = []
        replaced = False
        for it in items:
            if isinstance(it, dict) and str(it.get("id")) == item_id:
                out.append(sanitized)
                replaced = True
            else:
                out.append(it)

        if not replaced:
            out.insert(0, sanitized)

        # if none default, set first
        if not any(isinstance(it, dict) and it.get("is_default") for it in out) and out:
            if isinstance(out[0], dict):
                out[0]["is_default"] = True

        data["items"] = out
        _save(org, sup, data)
        return {"ok": True, "id": item_id}

    @router.delete("/reflection/compute/{pool_id}")
    def pools_delete(pool_id: str, org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
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
