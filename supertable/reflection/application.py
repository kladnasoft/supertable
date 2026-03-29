# route: supertable.reflection.application
"""
SuperTable UI Server — template rendering + reverse proxy to API.

Serves HTML pages (Jinja2 templates), static files, and login/session
management locally.  All JSON API calls (/reflection/*, /api/*) are
reverse-proxied to the API server via httpx.

Architecture:
    Browser ──→ UI Server (:8050) ──httpx──→ API Server (:8051)
                 │
                 ├── GET  pages     → Jinja2 template rendering (local)
                 ├── POST login     → session cookie creation   (local)
                 ├── GET  logout    → session cookie deletion   (local)
                 ├── GET  static/*  → StaticFiles               (local)
                 └── *    else      → reverse proxy to API      (httpx)

Usage:
    python -m supertable.reflection.application
    uvicorn supertable.reflection.application:app --port 8050

Environment:
    SUPERTABLE_API_URL              — API server base URL  (default: http://localhost:8051)
    SUPERTABLE_REFLECTION_PORT      — UI listen port       (default: 8050)
    SUPERTABLE_HOST                 — bind address         (default: 0.0.0.0)
    SUPERTABLE_PROXY_TIMEOUT        — proxy timeout secs   (default: 60)
    UVICORN_RELOAD                  — hot reload           (default: 0)
"""
from __future__ import annotations

import hmac
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, Form, Query, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

# ---------------------------------------------------------------------------
# Structured logging — must be configured before any other import logs
# ---------------------------------------------------------------------------
from supertable.config.settings import settings as _cfg
from supertable.logging import configure_logging, RequestLoggingMiddleware, CORRELATION_HEADER

configure_logging(service="ui")

logger = logging.getLogger(__name__)
_proxy_logger = logging.getLogger("supertable.ui.proxy")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_BASE_URL = f"http://{_cfg.effective_api_host}:{_cfg.SUPERTABLE_API_PORT}".rstrip("/")

# ---------------------------------------------------------------------------
# Import infrastructure from common.py — NO endpoint handlers are imported.
# common.py no longer auto-imports api.py, so this is safe and lightweight.
# ---------------------------------------------------------------------------

from supertable.reflection.common import (                  # noqa: E402
    settings,
    templates,
    catalog,
    _is_authorized,
    _no_store,
    _get_provided_token,
    _render_login,
    _required_token,
    _set_session_cookie,
    _clear_session_cookie,
    _ADMIN_COOKIE_NAME,
    _user_hash,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    get_session,
)

# ---------------------------------------------------------------------------
# Proxy client — persistent httpx.AsyncClient for connection pooling
# ---------------------------------------------------------------------------

_PROXY_TIMEOUT = _cfg.SUPERTABLE_PROXY_TIMEOUT
_http_client: Optional[httpx.AsyncClient] = None


# ---------------------------------------------------------------------------
# Lifespan — manages the httpx proxy client lifecycle
# ---------------------------------------------------------------------------

@asynccontextmanager
async def _lifespan(application: FastAPI):
    # Startup: nothing to do (client is created lazily on first request)
    yield
    # Shutdown: close the persistent httpx client
    global _http_client
    if _http_client and not _http_client.is_closed:
        await _http_client.aclose()
        _http_client = None


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

app = FastAPI(
    title="SuperTable UI",
    version="1.0.0",
    description="SuperTable Reflection UI — template rendering + API proxy",
    lifespan=_lifespan,
)

app.add_middleware(RequestLoggingMiddleware, service="ui")

# Static files
_STATIC_DIR = _cfg.SUPERTABLE_STATIC_DIR or str(Path(__file__).resolve().parent / "static")
if Path(_STATIC_DIR).is_dir():
    app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")


# ---------------------------------------------------------------------------
# Shared page context helper
# ---------------------------------------------------------------------------

def _page_context(
    request: Request,
    org: Optional[str] = None,
    sup: Optional[str] = None,
    **extra: Any,
) -> Optional[Dict[str, Any]]:
    """Build the standard template context for any page route.

    Returns None if the request is not authorized (caller should redirect
    to login).
    """
    if not _is_authorized(request):
        return None

    provided = _get_provided_token(request) or ""
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
        "has_tenant": bool(sel_org and sel_sup),
    }
    ctx.update(extra)
    inject_session_into_ctx(ctx, request)
    return ctx


def _redirect_to_login() -> RedirectResponse:
    resp = RedirectResponse("/reflection/login", status_code=302)
    _no_store(resp)
    return resp


# ═══════════════════════════════════════════════════════════════════════════
#   Root / Utility routes
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
def root_redirect():
    resp = RedirectResponse("/reflection/login", status_code=302)
    _no_store(resp)
    return resp


@app.get("/reflection", include_in_schema=False)
async def reflection_root_redirect() -> RedirectResponse:
    return RedirectResponse(url="/reflection/home", status_code=302)


@app.get("/favicon.ico", include_in_schema=False)
def favicon():
    return RedirectResponse(url="/static/favicon.ico")


@app.get("/healthz")
async def healthz():
    """Proxy health check to API server."""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.get(f"{API_BASE_URL}/healthz")
            return Response(content=r.text, media_type="text/plain")
    except Exception as e:
        return Response(content=f"api-unreachable: {e}", media_type="text/plain")


# ═══════════════════════════════════════════════════════════════════════════
#   Auth — handled locally (session cookies, NOT proxied)
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/reflection/login", response_class=HTMLResponse)
def login_page(request: Request):
    return _render_login(request, message=None, clear_cookie=True)


@app.post("/reflection/login")
def login_post(
    request: Request,
    mode: str = Form("user"),
    username: str = Form(""),
    token: str = Form(""),
    supertoken: str = Form(""),
):
    """Authenticate locally and set session cookies.

    This runs entirely in the UI server — no proxy to the API server.
    Session cookies are HMAC-signed and verifiable by both servers
    (they share SUPERTABLE_SESSION_SECRET / SUPERTABLE_SUPERTOKEN).
    """
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
        user_hash_val = settings.SUPERTABLE_SUPERHASH

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
        _set_session_cookie(resp, {
            "org": org,
            "username": username_eff,
            "user_hash": user_hash_val,
            "is_superuser": True,
        })
        _no_store(resp)
        return resp

    # Regular user: username + token
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

    user_hash_val = _user_hash(org, username)
    resp = RedirectResponse(url="/reflection/home", status_code=302)
    resp.delete_cookie(_ADMIN_COOKIE_NAME, path="/")
    _clear_session_cookie(resp)
    _set_session_cookie(resp, {
        "org": org,
        "username": username,
        "user_hash": user_hash_val,
        "is_superuser": False,
    })
    _no_store(resp)
    return resp


@app.get("/reflection/logout")
def logout(request: Request):
    resp = RedirectResponse("/reflection/login", status_code=302)
    resp.delete_cookie(_ADMIN_COOKIE_NAME, path="/")
    _clear_session_cookie(resp)
    _no_store(resp)
    return resp


# ═══════════════════════════════════════════════════════════════════════════
#   Page routes — template rendering only, NO business logic
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/reflection/home", response_class=HTMLResponse)
def home_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    ctx = _page_context(request, org, sup, stats={
        "supertables": None, "tables": None, "connectors": None, "pools": None,
    })
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("home.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/tables", response_class=HTMLResponse)
def tables_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=5, le=500),
):
    ctx = _page_context(
        request, org, sup,
        total=0, page=page, pages=1, items=[],
        root_version=None, root_ts=None, default_user_hash="",
    )
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("tables.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/execute", response_class=HTMLResponse)
def execute_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
    leaf: Optional[str] = Query(None),
):
    sess = get_session(request) or {}
    ctx = _page_context(
        request, org, sup,
        initial_leaf=leaf,
        default_role_name=sess.get("role_name", "") or "",
    )
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("execute.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/ingestion", response_class=HTMLResponse)
def ingestion_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    ctx = _page_context(request, org, sup)
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("ingestion.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/engine", response_class=HTMLResponse)
def engine_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    ctx = _page_context(request, org, sup)
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("engine.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/monitoring", response_class=HTMLResponse)
def monitoring_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    ctx = _page_context(request, org, sup)
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("monitoring.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/security", response_class=HTMLResponse)
def security_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    ctx = _page_context(request, org, sup)
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("security.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/quality", response_class=HTMLResponse)
def quality_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    from supertable.reflection.quality.config import BUILTIN_CHECKS

    sel_org, sel_sup = resolve_pair(org, sup)
    ctx = _page_context(
        request, org, sup,
        selected_org=sel_org or "",
        selected_sup=sel_sup or "",
        builtin_checks=BUILTIN_CHECKS,
    )
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("quality.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/admin", response_class=HTMLResponse)
def admin_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    ctx = _page_context(
        request, org, sup,
        root_version=-1, root_ts="\u2014",
        mirrors=[], users=[], roles=[],
    )
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("admin.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/compute", response_class=HTMLResponse)
def compute_page(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    from supertable.reflection.compute import _notebook_port, _default_ws_url

    ctx = _page_context(
        request, org, sup,
        notebook_port=_notebook_port(),
        notebook_ws_default=_default_ws_url(),
    )
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("compute.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/users-page", response_class=HTMLResponse)
def users_page_html(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    from supertable.reflection.common import list_users as _lu, list_roles as _lr
    import json as _json

    sel_org, sel_sup = resolve_pair(org, sup)
    users_data = []
    roles_data = []
    if sel_org and sel_sup:
        users_raw = _lu(sel_org, sel_sup)
        for u in users_raw:
            uid = u.get("user_id") or u.get("hash") or ""
            u.setdefault("user_id", uid)
            u.setdefault("hash", uid)
            roles = u.get("roles") or []
            if isinstance(roles, str):
                try:
                    roles = _json.loads(roles)
                except Exception:
                    roles = [roles]
            u["roles"] = roles
            uname = str(u.get("username") or u.get("name") or "").strip()
            u["is_superuser"] = uname.lower() == sel_sup.lower()
        users_data = users_raw
        roles_data = _lr(sel_org, sel_sup)

    ctx = _page_context(
        request, org, sup,
        users=users_data,
        roles=roles_data,
    )
    if ctx is None:
        return _redirect_to_login()
    resp = templates.TemplateResponse("users.html", ctx)
    _no_store(resp)
    return resp


@app.get("/reflection/rbac", response_class=HTMLResponse)
def rbac_redirect(
    request: Request,
    org: Optional[str] = Query(None),
    sup: Optional[str] = Query(None),
):
    url = "/reflection/security"
    params = []
    if org:
        params.append(f"org={org}")
    if sup:
        params.append(f"sup={sup}")
    if params:
        url += "?" + "&".join(params)
    resp = RedirectResponse(url, status_code=302)
    _no_store(resp)
    return resp


# ═══════════════════════════════════════════════════════════════════════════
#   Reverse proxy — forward all JSON API calls to the API server
# ═══════════════════════════════════════════════════════════════════════════

async def _get_client() -> httpx.AsyncClient:
    global _http_client
    if _http_client is None or _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            base_url=API_BASE_URL,
            timeout=httpx.Timeout(_PROXY_TIMEOUT, connect=5.0),
            follow_redirects=False,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )
    return _http_client


_HOP_BY_HOP = frozenset({
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host",
})

_SKIP_RESPONSE_HEADERS = frozenset({
    "content-encoding", "content-length", "transfer-encoding",
    "connection", "keep-alive",
})


async def _proxy_request(request: Request, url: str) -> Response:
    """Forward a request to the API server and return the response."""
    # Tag this request so the logging middleware can distinguish local vs proxied
    request.state.proxied = True

    client = await _get_client()

    if request.url.query:
        url = f"{url}?{request.url.query}"

    # Forward headers, injecting correlation ID for cross-service tracing
    fwd_headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP
    }
    correlation_id = getattr(request.state, "correlation_id", None) or ""
    if correlation_id:
        fwd_headers[CORRELATION_HEADER] = correlation_id

    body = await request.body()
    t0 = time.perf_counter()

    try:
        api_resp = await client.request(
            method=request.method,
            url=url,
            headers=fwd_headers,
            content=body,
        )
    except httpx.RequestError as e:
        duration_ms = round((time.perf_counter() - t0) * 1000, 1)
        _proxy_logger.error(
            f"proxy failed: {request.method} {url}",
            extra={
                "event": "proxy_error",
                "correlation_id": correlation_id,
                "method": request.method,
                "path": url,
                "proxy_target": API_BASE_URL,
                "duration_ms": duration_ms,
                "error": str(e),
            },
        )
        return Response(
            content=f'{{"error": "API server unreachable: {e}"}}',
            status_code=502,
            media_type="application/json",
        )

    duration_ms = round((time.perf_counter() - t0) * 1000, 1)

    _proxy_logger.debug(
        f"proxy: {request.method} {url} → {api_resp.status_code} ({duration_ms}ms)",
        extra={
            "event": "proxy",
            "correlation_id": correlation_id,
            "method": request.method,
            "path": url,
            "proxy_target": API_BASE_URL,
            "proxy_status": api_resp.status_code,
            "duration_ms": duration_ms,
        },
    )

    resp_headers = {
        k: v for k, v in api_resp.headers.items()
        if k.lower() not in _SKIP_RESPONSE_HEADERS
    }

    resp = Response(
        content=api_resp.content,
        status_code=api_resp.status_code,
        headers=resp_headers,
    )

    # Forward Set-Cookie headers (multi-valued)
    for cookie_header in api_resp.headers.get_list("set-cookie"):
        resp.headers.append("set-cookie", cookie_header)

    return resp


@app.api_route(
    "/reflection/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
    include_in_schema=False,
)
async def proxy_reflection(request: Request, path: str):
    """Reverse-proxy any /reflection/* request not matched by a page route.

    This catches all JSON API calls (execute, schema, rbac, monitoring,
    quality, ingestion, etc.) and forwards them to the API server,
    preserving method, query params, headers, cookies, and request body.
    """
    return await _proxy_request(request, f"/reflection/{path}")


@app.api_route(
    "/api/{path:path}",
    methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    include_in_schema=False,
)
async def proxy_api_prefix(request: Request, path: str):
    """Proxy /api/* requests (OData, etc.) to the API server."""
    return await _proxy_request(request, f"/api/{path}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    host = _cfg.SUPERTABLE_UI_HOST
    port = _cfg.SUPERTABLE_UI_PORT
    reload_flag = _cfg.UVICORN_RELOAD

    uvicorn.run(
        "supertable.reflection.application:app",
        host=host,
        port=port,
        reload=reload_flag,
    )
