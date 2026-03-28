# path: supertable/reflection/env.py
"""
Route module for the standalone .env configuration page.

Registers:
  GET  /reflection/env-page  — renders env.html
  GET  /reflection/env       — API: read .env variables
  POST /reflection/env       — API: write .env variables

The /reflection/env API routes are also registered by admin.py (for the
existing /reflection/admin page). FastAPI silently skips duplicate
path+method registrations on a shared router — whichever module is
attached first wins. Both modules carry identical implementations so the
result is the same either way.
"""
from __future__ import annotations

import logging
import re as _re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, Response

logger = logging.getLogger(__name__)


def attach_env_routes(
    router: APIRouter,
    *,
    templates: Any,
    settings: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Response], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], List[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[str, str]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Any],
    admin_guard_api: Any,
    dotenv_values: Any,
    set_key: Any,
) -> None:
    """Attach /reflection/env-page and /reflection/env API to router."""

    # ── .env helpers ─────────────────────────────────────────────────────────

    _SENSITIVE_KEY_PARTS = (
        "PASSWORD", "PASS", "SECRET", "TOKEN", "KEY",
        "ACCESS_KEY", "CONNECTION_STRING", "API_KEY", "CLIENT_SECRET",
    )
    _ENV_KEY_RE = _re.compile(r"^[A-Z0-9_]+$")

    def _env_file_path() -> Path:
        here = Path(__file__).resolve()
        reflection_dir = here.parent
        pkg_dir = reflection_dir.parent
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

    # ── HTML page ─────────────────────────────────────────────────────────────

    @router.get("/reflection/env-page", response_class=HTMLResponse)
    def env_page(
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
        tenants = [
            {"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)}
            for o, s in pairs
        ]

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("env.html", ctx)
        no_store(resp)
        return resp

    # ── API: read .env ────────────────────────────────────────────────────────

    @router.get("/reflection/env")
    def env_get(_: Any = Depends(admin_guard_api)):
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
                items.append({
                    "key": k,
                    "value": _mask_secret(val) if is_sensitive else val,
                    "is_sensitive": is_sensitive,
                })

        return {"found": found, "path": str(env_path), "items": items}

    # ── API: write .env ───────────────────────────────────────────────────────

    @router.post("/reflection/env")
    def env_update(
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(admin_guard_api),
    ):
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
