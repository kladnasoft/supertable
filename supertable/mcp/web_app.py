#!/usr/bin/env python3
# web_app.py — minimal FastAPI UI for exercising the MCP server
from __future__ import annotations

import contextlib
import hmac
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from supertable.mcp.web_client import MCPWebClient

logger = logging.getLogger("supertable.mcp.web_app")

# ---------- MCP SDK Streamable HTTP mount ----------
# Import the FastMCP instance from mcp_server so we can mount its
# streamable-http ASGI app at /mcp.  This is the endpoint Claude Desktop
# (and any MCP SDK client) connects to.
_mcp_sdk_available = False
_mcp_instance = None
_mcp_streamable_app = None

try:
    from supertable.mcp.mcp_server import mcp as _mcp_instance  # noqa: F811
    _mcp_sdk_available = True
except Exception as exc:
    logger.warning("Could not import mcp instance from mcp_server: %s", exc)

def _build_mcp_streamable_app():
    """Build the ASGI sub-app for MCP Streamable HTTP transport.

    The SDK's streamable_http_app() creates an internal route at /mcp by default.
    When we mount this sub-app, we need to avoid a double-path (/mcp/mcp).

    Strategy:
    - If the SDK supports ``path`` parameter: set path="/" and mount at /mcp.
    - If it doesn't: mount at "/" (root) so the SDK's internal /mcp works directly.
    """
    global _mcp_streamable_app, _mcp_mount_path
    if _mcp_instance is None:
        return None
    try:
        import inspect
        if hasattr(_mcp_instance, "streamable_http_app"):
            sig = inspect.signature(_mcp_instance.streamable_http_app)
            if "path" in sig.parameters:
                _mcp_streamable_app = _mcp_instance.streamable_http_app(path="/")
                _mcp_mount_path = "/mcp"
            else:
                # SDK doesn't support path= ; it will create /mcp internally.
                # Mount at root so final URL is /mcp (not /mcp/mcp).
                _mcp_streamable_app = _mcp_instance.streamable_http_app()
                _mcp_mount_path = "/"
        elif hasattr(_mcp_instance, "http_app"):
            sig = inspect.signature(_mcp_instance.http_app)
            if "path" in sig.parameters:
                _mcp_streamable_app = _mcp_instance.http_app(path="/")
                _mcp_mount_path = "/mcp"
            else:
                _mcp_streamable_app = _mcp_instance.http_app()
                _mcp_mount_path = "/"
    except Exception as exc:
        logger.warning("Failed to build MCP streamable HTTP app: %s", exc)
    return _mcp_streamable_app

# Where to mount the streamable-http sub-app (depends on SDK version).
_mcp_mount_path = "/mcp"


@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Start the MCP SDK session manager (required for streamable-http transport).
    async with contextlib.AsyncExitStack() as stack:
        if _mcp_instance is not None and hasattr(_mcp_instance, "session_manager"):
            try:
                await stack.enter_async_context(_mcp_instance.session_manager.run())
                logger.info("MCP session_manager started (streamable-http ready)")
            except Exception as exc:
                logger.warning("MCP session_manager.run() failed (streamable-http will not work): %s", exc)

        # Start the stdio subprocess client for the web tester / /api/* endpoints.
        await _startup()
        try:
            yield
        finally:
            await _shutdown()

app = FastAPI(title="Supertable MCP Web Tester", lifespan=lifespan)

# Mount the MCP SDK streamable-http transport.
# Claude Desktop connects to http://host:8099/mcp.
_build_mcp_streamable_app()
if _mcp_streamable_app is not None:
    app.mount(_mcp_mount_path, _mcp_streamable_app)
    logger.info("Mounted MCP Streamable HTTP transport at %s (endpoint: /mcp)", _mcp_mount_path)
else:
    logger.warning(
        "MCP Streamable HTTP transport NOT mounted at /mcp — "
        "install/upgrade the MCP SDK (pip install --upgrade mcp) "
        "to enable Claude Desktop remote connections"
    )

_client: Optional[MCPWebClient] = None

def _client_or_raise() -> MCPWebClient:
    c = getattr(app.state, 'mcp_client', None) or _client
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
    """Require a shared-secret token for ALL HTTP access (robust by default).

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
        raise HTTPException(status_code=401, detail="Missing auth token (Authorization / X-Auth-Code / ?auth=)")
    if not hmac.compare_digest(got, expected):
        raise HTTPException(status_code=403, detail="Invalid token")

async def _startup() -> None:
    global _client
    if os.getenv("SUPERTABLE_MCP_WEB_DISABLE_SUBPROCESS", "").strip().lower() in {"1", "true", "yes"}:
        return
    if _client is None:
        _client = MCPWebClient(
            server_path=os.getenv("MCP_SERVER_PATH"),
            auth_token=os.getenv("SUPERTABLE_MCP_AUTH_TOKEN", ""),
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
    c = getattr(app.state, 'mcp_client', None) or _client
    return JSONResponse({
        "status": "ok",
        "mcp_client": "ready" if c is not None else "not_initialized",
        "mcp_streamable_http": "mounted" if _mcp_streamable_app is not None else "not_mounted",
    })

@app.get("/", response_class=HTMLResponse)
async def home(req: Request) -> str:
    _require_gateway_auth(req)
    html_file = Path(__file__).parent / "web_tester.html"
    return html_file.read_text(encoding="utf-8")


@app.post("/mcp_v1")
async def mcp_http_gateway_v1(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    """Claude/Desktop-friendly MCP over HTTP.

    Accepts JSON-RPC 2.0 requests and forwards them to the stdio MCP server.

    Headers supported:
      - Authorization: Bearer <token>  (optional gateway auth; see env vars)
      - X-Role: <role>                 (optional convenience injection)

    Env vars:
      - SUPERTABLE_MCP_HTTP_REQUIRE_TOKEN=true|false (default false)
      - SUPERTABLE_MCP_HTTP_TOKEN=<shared secret>
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

    # Convenience: inject auth_token and role from headers for tools/call requests.
    # - Authorization: Bearer <token>  -> tool auth_token (if not provided)
    # - X-Role: <value>                 -> tool role (if not provided)
    bearer = _parse_bearer(req.headers.get("authorization", ""))
    header_role = (req.headers.get("x-role") or "").strip()
    if method == "tools/call" and isinstance(params.get("arguments"), dict):
        args = params["arguments"]
        if bearer and not args.get("auth_token"):
            args["auth_token"] = bearer
        if header_role:
            args["role"] = header_role

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
    # keep it lightweight for the browser
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
    r = (payload.get("role") or "").strip()
    if not org:
        raise HTTPException(status_code=400, detail="organization required")
    c = _client_or_raise()
    resp = await c.tool("list_supers", {"organization": org, "role": r})
    return JSONResponse(resp)

@app.post("/api/list_tables")
async def api_list_tables(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    r = (payload.get("role") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name required")
    c = _client_or_raise()
    resp = await c.tool("list_tables", {"organization": org, "super_name": sup, "role": r})
    return JSONResponse(resp)

@app.post("/api/describe_table")
async def api_describe_table(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    tbl = (payload.get("table") or "").strip()
    r = (payload.get("role") or "").strip()
    if not org or not sup or not tbl:
        raise HTTPException(status_code=400, detail="organization, super_name, table required")
    c = _client_or_raise()
    resp = await c.tool("describe_table", {"organization": org, "super_name": sup, "table": tbl, "role": r})
    return JSONResponse(resp)

@app.post("/api/get_table_stats")
async def api_get_table_stats(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    tbl = (payload.get("table") or "").strip()
    r = (payload.get("role") or "").strip()
    if not org or not sup or not tbl:
        raise HTTPException(status_code=400, detail="organization, super_name, table required")
    c = _client_or_raise()
    resp = await c.tool("get_table_stats", {"organization": org, "super_name": sup, "table": tbl, "role": r})
    return JSONResponse(resp)

@app.post("/api/get_super_meta")
async def api_get_super_meta(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    r = (payload.get("role") or "").strip()
    if not org or not sup:
        raise HTTPException(status_code=400, detail="organization and super_name required")
    c = _client_or_raise()
    resp = await c.tool("get_super_meta", {"organization": org, "super_name": sup, "role": r})
    return JSONResponse(resp)

@app.post("/api/sample_data")
async def api_sample_data(req: Request, payload: Dict[str, Any] = Body(...)) -> JSONResponse:
    _require_gateway_auth(req)
    org = (payload.get("organization") or "").strip()
    sup = (payload.get("super_name") or "").strip()
    tbl = (payload.get("table") or "").strip()
    r = (payload.get("role") or "").strip()
    if not org or not sup or not tbl:
        raise HTTPException(status_code=400, detail="organization, super_name, table required")
    args: Dict[str, Any] = {"organization": org, "super_name": sup, "table": tbl, "role": r}
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
    r = (payload.get("role") or "").strip()

    if not org or not sup or not sql:
        raise HTTPException(status_code=400, detail="organization, super_name, sql required")

    args: Dict[str, Any] = {"organization": org, "super_name": sup, "sql": sql, "role": r}
    if payload.get("limit") is not None:
        args["limit"] = payload["limit"]
    if payload.get("engine"):
        args["engine"] = payload["engine"]
    if payload.get("query_timeout_sec") is not None:
        args["query_timeout_sec"] = payload["query_timeout_sec"]

    c = _client_or_raise()
    resp = await c.tool("query_sql", args)
    return JSONResponse(resp)