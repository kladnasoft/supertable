#!/usr/bin/env python3
# web_app.py — minimal FastAPI UI for exercising the MCP server
from __future__ import annotations

import contextlib
import hmac
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, Optional

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.middleware.trustedhost import TrustedHostMiddleware

from supertable.mcp.web_client import MCPWebClient

logger = logging.getLogger("supertable.mcp.web_app")

# ---------- MCP SDK Streamable HTTP mount ----------
# Import the FastMCP instance from mcp_server so we can mount its
# streamable-http ASGI app at /mcp. This is the endpoint Claude Desktop
# (and any MCP SDK client) connects to.
_mcp_sdk_available = False
_mcp_instance = None
_mcp_streamable_app = None
_mcp_mount_path = "/mcp"

try:
    from supertable.mcp.mcp_server import mcp as _mcp_instance  # noqa: F811

    _mcp_sdk_available = True
except Exception as exc:
    logger.warning("Could not import mcp instance from mcp_server: %s", exc)


def _build_mcp_streamable_app() -> Optional[Any]:
    """Build the ASGI sub-app for MCP Streamable HTTP transport.

    The SDK sub-app is built with its default path ("/mcp") and NOT mounted
    via Starlette's Mount (which would introduce a 307 trailing-slash redirect
    that breaks MCP SDK clients).  Instead, we use a lightweight ASGI
    middleware that routes /mcp* requests directly to the SDK sub-app and
    everything else to the FastAPI app.
    """

    global _mcp_mount_path, _mcp_streamable_app
    if _mcp_instance is None:
        return None

    app_factory = None
    try:
        if hasattr(_mcp_instance, "streamable_http_app"):
            app_factory = _mcp_instance.streamable_http_app
        elif hasattr(_mcp_instance, "http_app"):
            app_factory = _mcp_instance.http_app

        if app_factory is None:
            return None

        # Build the SDK app with its default internal path ("/mcp").
        # We do NOT pass path="/" because the SDK may not handle it
        # correctly in all versions, and it avoids Starlette Mount issues.
        _mcp_streamable_app = app_factory()
        _mcp_mount_path = "/mcp"
    except Exception as exc:
        logger.warning("Failed to build MCP streamable HTTP app: %s", exc)
        _mcp_streamable_app = None
        _mcp_mount_path = "/mcp"

    return _mcp_streamable_app


def _read_html(filename: str) -> str:
    return (Path(__file__).parent / filename).read_text(encoding="utf-8")


def _configured_tool_auth_token() -> str:
    return (os.getenv("SUPERTABLE_MCP_AUTH_TOKEN") or "").strip()




def _allowed_hosts() -> list[str]:
    raw = (os.getenv("SUPERTABLE_ALLOWED_HOSTS") or "*").strip()
    if not raw:
        return ["*"]
    hosts = [item.strip() for item in raw.split(",") if item.strip()]
    return hosts or ["*"]


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start the MCP SDK session manager (required for streamable-http transport).
    async with contextlib.AsyncExitStack() as stack:
        if _mcp_instance is not None and hasattr(_mcp_instance, "session_manager"):
            try:
                await stack.enter_async_context(_mcp_instance.session_manager.run())
                logger.info("MCP session_manager started (streamable-http ready)")
            except Exception as exc:
                logger.warning(
                    "MCP session_manager.run() failed (streamable-http will not work): %s",
                    exc,
                )

        # Start the stdio subprocess client for the web tester / /api/* endpoints.
        await _startup()
        app.state.mcp_client = _client
        try:
            yield
        finally:
            app.state.mcp_client = None
            await _shutdown()


app = FastAPI(title="Supertable MCP Web Tester", lifespan=lifespan)
app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allowed_hosts())

_client: Optional[MCPWebClient] = None


def _client_or_raise() -> MCPWebClient:
    c = getattr(app.state, "mcp_client", None) or _client
    if c is None:
        raise HTTPException(status_code=503, detail="MCP client not initialized")
    return c


def _parse_bearer(auth_header: str) -> str:
    v = (auth_header or "").strip()
    if not v:
        return ""
    if v.lower().startswith("bearer "):
        return v.split(" ", 1)[1].strip()
    return v


def _expected_gateway_token() -> str:
    """
    Token required to access the HTTP gateway + web API.

    By default we use SUPERTABLE_SUPERTOKEN (same token you already use for the web UI),
    and fall back to SUPERTABLE_MCP_HTTP_TOKEN for deployments that prefer a dedicated
    gateway secret.
    """

    return (os.getenv("SUPERTABLE_SUPERTOKEN") or os.getenv("SUPERTABLE_MCP_HTTP_TOKEN") or "").strip()


def _require_gateway_auth(req: Request) -> None:
    """Require a shared-secret token for all browser-facing HTTP access.

    Supported ways to pass the token:
      - Authorization: Bearer <token>
      - X-Auth-Code: <token>
      - ?auth=<token> (useful for loading the UI in a browser without extensions)

    Notes:
      - This protects the HTTP surface area. The stdio MCP server has its own tool auth
        (auth_token) as an additional layer.
    """

    expected = _expected_gateway_token()
    if not expected:
        raise HTTPException(
            status_code=500,
            detail="SUPERTABLE_SUPERTOKEN (or SUPERTABLE_MCP_HTTP_TOKEN) must be set to protect the web gateway",
        )

    got = _parse_bearer(req.headers.get("authorization", ""))
    if not got:
        got = (req.headers.get("x-auth-code") or "").strip()
    if not got:
        got = (req.query_params.get("auth") or "").strip()

    if not got:
        raise HTTPException(
            status_code=401,
            detail="Missing auth token (Authorization / X-Auth-Code / ?auth=)",
        )
    if not hmac.compare_digest(got, expected):
        raise HTTPException(status_code=403, detail="Invalid token")


async def _startup() -> None:
    global _client
    if os.getenv("SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS", "").strip().lower() in {
        "1",
        "true",
        "yes",
    }:
        return
    if _client is None:
        _client = MCPWebClient(
            server_path=os.getenv("MCP_SERVER_PATH"),
            auth_token=_configured_tool_auth_token(),
        )
        await _client.start()


async def _shutdown() -> None:
    global _client
    if _client is not None:
        await _client.close()
        _client = None


@app.get("/health")
async def health_check() -> JSONResponse:
    """Kubernetes liveness + readiness probe — no auth required."""

    c = getattr(app.state, "mcp_client", None) or _client
    return JSONResponse(
        {
            "status": "ok",
            "mcp_client": "ready" if c is not None else "not_initialized",
            "mcp_streamable_http": "mounted" if _mcp_streamable_app is not None else "not_mounted",
            "mcp_mount_path": _mcp_mount_path if _mcp_streamable_app is not None else None,
        }
    )


@app.get("/", response_class=HTMLResponse)
@app.get("/web", response_class=HTMLResponse)
async def home(req: Request) -> str:
    _require_gateway_auth(req)
    return _read_html("web_tester.html")


@app.get("/simulator", response_class=HTMLResponse)
@app.get("/simulation", response_class=HTMLResponse)
async def simulator(req: Request) -> str:
    _require_gateway_auth(req)
    return _read_html("mcp_simulator.html")


@app.post("/mcp_v1")
async def mcp_http_gateway_v1(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    """Browser-friendly JSON-RPC proxy to the stdio MCP server.

    Accepts JSON-RPC 2.0 requests and forwards them to the stdio MCP server.

    Headers supported:
      - Authorization: Bearer <gateway token>  (required for gateway access)
      - X-Role: <role>                         (optional convenience injection)
      - X-MCP-Auth-Token: <tool auth token>    (optional tool auth override)

    If X-MCP-Auth-Token is not provided, the server falls back to
    SUPERTABLE_MCP_AUTH_TOKEN when forwarding ``tools/call`` requests.
    """

    _require_gateway_auth(req)

    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Invalid JSON-RPC payload")

    method = (payload.get("method") or "").strip()
    params = payload.get("params")
    external_id = payload.get("id")

    if not method:
        raise HTTPException(status_code=400, detail="Missing JSON-RPC method")
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="JSON-RPC params must be an object")

    header_role = (req.headers.get("x-role") or "").strip()
    header_tool_auth = (req.headers.get("x-mcp-auth-token") or "").strip()

    if method == "tools/call" and isinstance(params.get("arguments"), dict):
        args = params["arguments"]
        if header_role and not args.get("role"):
            args["role"] = header_role
        if header_tool_auth and not args.get("auth_token"):
            args["auth_token"] = header_tool_auth
        elif _configured_tool_auth_token() and not args.get("auth_token"):
            args["auth_token"] = _configured_tool_auth_token()

    c = _client_or_raise()
    try:
        resp = await c.jsonrpc(method, params, external_id=external_id)
    except Exception as exc:
        # Keep JSON-RPC shape on failures.
        return JSONResponse(
            {
                "jsonrpc": "2.0",
                "id": external_id,
                "error": {"code": -32000, "message": str(exc)},
            }
        )

    # Ensure JSON-RPC 2.0 field exists (Claude expects it).
    if isinstance(resp, dict) and "jsonrpc" not in resp:
        resp["jsonrpc"] = "2.0"
    return JSONResponse(resp)


@app.get("/api/health")
async def api_health(req: Request) -> JSONResponse:
    _require_gateway_auth(req)
    c = _client_or_raise()
    resp = await c.tool("health", {})
    return JSONResponse(resp)


@app.get("/api/info")
async def api_info(req: Request) -> JSONResponse:
    _require_gateway_auth(req)
    c = _client_or_raise()
    resp = await c.tool("info", {})
    return JSONResponse(resp)


@app.get("/api/events")
async def api_events(req: Request) -> JSONResponse:
    _require_gateway_auth(req)
    c = _client_or_raise()
    tail = c.events[-200:]
    return JSONResponse(
        {
            "ts_unix": time.time(),
            "count": len(tail),
            "events": [{"dir": e.direction, "payload": e.payload} for e in tail],
        }
    )


@app.post("/api/list_supers")
async def api_list_supers(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    role = (payload.get("role") or "").strip()
    if not org:
        raise HTTPException(status_code=400, detail="organization required")
    c = _client_or_raise()
    resp = await c.tool("list_supers", {"organization": org, "role": role})
    return JSONResponse(resp)


@app.post("/api/list_tables")
async def api_list_tables(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    role = (payload.get("role") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name required")
    c = _client_or_raise()
    resp = await c.tool("list_tables", {"organization": org, "super_name": sup, "role": role})
    return JSONResponse(resp)


@app.post("/api/describe_table")
async def api_describe_table(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    tbl = (payload.get("table") or "").strip()
    role = (payload.get("role") or "").strip()
    if not org or not sup or not tbl:
        raise HTTPException(status_code=400, detail="organization, super_name, table required")
    c = _client_or_raise()
    resp = await c.tool(
        "describe_table",
        {"organization": org, "super_name": sup, "table": tbl, "role": role},
    )
    return JSONResponse(resp)


@app.post("/api/get_table_stats")
async def api_get_table_stats(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    tbl = (payload.get("table") or "").strip()
    role = (payload.get("role") or "").strip()
    if not org or not sup or not tbl:
        raise HTTPException(status_code=400, detail="organization, super_name, table required")
    c = _client_or_raise()
    resp = await c.tool(
        "get_table_stats",
        {"organization": org, "super_name": sup, "table": tbl, "role": role},
    )
    return JSONResponse(resp)


@app.post("/api/get_super_meta")
async def api_get_super_meta(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    role = (payload.get("role") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name required")
    c = _client_or_raise()
    resp = await c.tool("get_super_meta", {"organization": org, "super_name": sup, "role": role})
    return JSONResponse(resp)


@app.post("/api/sample_data")
async def api_sample_data(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    tbl = (payload.get("table") or "").strip()
    role = (payload.get("role") or "").strip()
    if not org or not sup or not tbl:
        raise HTTPException(status_code=400, detail="organization, super_name, table required")
    args: Dict[str, Any] = {"organization": org, "super_name": sup, "table": tbl, "role": role}
    if payload.get("limit") is not None:
        args["limit"] = payload["limit"]
    c = _client_or_raise()
    resp = await c.tool("sample_data", args)
    return JSONResponse(resp)


@app.post("/api/query_sql")
async def api_query_sql(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    sql = (payload.get("sql") or "").strip()
    role = (payload.get("role") or "").strip()

    if not org or not sup or not sql:
        raise HTTPException(status_code=400, detail="organization, super_name, sql required")

    args: Dict[str, Any] = {"organization": org, "super_name": sup, "sql": sql, "role": role}
    if payload.get("limit") is not None:
        args["limit"] = payload["limit"]
    if payload.get("engine"):
        args["engine"] = payload["engine"]
    if payload.get("query_timeout_sec") is not None:
        args["query_timeout_sec"] = payload["query_timeout_sec"]

    c = _client_or_raise()
    resp = await c.tool("query_sql", args)
    return JSONResponse(resp)


_build_mcp_streamable_app()
if _mcp_streamable_app is not None:
    # Do NOT use Starlette's app.mount("/mcp", ...) — it introduces a 307
    # trailing-slash redirect for the bare /mcp path that breaks MCP SDK
    # clients (Claude Desktop, mcp-remote, etc.) which POST to /mcp.
    #
    # Instead, wrap the FastAPI app in a thin ASGI middleware that intercepts
    # requests starting with /mcp and forwards them directly to the SDK
    # sub-app (which handles /mcp internally as its default path).
    # All other paths pass through to FastAPI normally.
    _fastapi_app = app  # capture the real FastAPI instance

    class _McpRouter:
        """ASGI middleware: route /mcp* to MCP SDK, everything else to FastAPI."""

        def __init__(self, fastapi_app, mcp_app):
            self._fastapi = fastapi_app
            self._mcp = mcp_app

        async def __call__(self, scope, receive, send):
            if scope["type"] in {"http", "websocket"}:
                path = scope.get("path", "")
                if path == "/mcp" or path.startswith("/mcp/"):
                    # Pass directly to the SDK sub-app with path unchanged.
                    # The SDK expects to see /mcp (its default internal path).
                    #
                    # Strip the Origin header: the MCP SDK validates Origin
                    # for CSRF protection and rejects browser origins (e.g.
                    # https://host:8470) that don't match its expectations.
                    # Browser-based access (/simulation) is already protected
                    # by gateway auth; real MCP clients don't send Origin.
                    headers = scope.get("headers", [])
                    if headers:
                        scope = dict(scope)
                        scope["headers"] = [
                            (k, v) for k, v in headers
                            if k.lower() != b"origin"
                        ]
                    await self._mcp(scope, receive, send)
                    return
            await self._fastapi(scope, receive, send)

        # Proxy attribute access to the FastAPI app so that uvicorn's
        # lifespan handling and any code that accesses app.state still works.
        def __getattr__(self, name):
            return getattr(self._fastapi, name)

    app = _McpRouter(_fastapi_app, _mcp_streamable_app)
    logger.info("Mounted MCP Streamable HTTP transport at %s (ASGI router)", _mcp_mount_path)
else:
    logger.warning(
        "MCP Streamable HTTP transport NOT mounted at %s — "
        "install/upgrade the MCP SDK (pip install --upgrade mcp) "
        "to enable Claude Desktop remote connections",
        _mcp_mount_path,
    )
