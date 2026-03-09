#!/usr/bin/env python3
# web_app.py — minimal FastAPI UI for exercising the MCP server
from __future__ import annotations

import hmac
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

from pathlib import Path

from fastapi import Body, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from supertable.mcp.web_client import MCPWebClient

@asynccontextmanager
async def lifespan(_app: FastAPI):
    # Replaces deprecated @app.on_event startup/shutdown.
    await _startup()
    try:
        yield
    finally:
        await _shutdown()

app = FastAPI(title="Supertable MCP Web Tester", lifespan=lifespan)

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


@app.post("/mcp")
async def mcp_http_gateway(req: Request, body: Dict[str, Any] = Body(...)) -> JSONResponse:
    """Claude Desktop compatible MCP-over-HTTP endpoint.

    This is a thin JSON-RPC pass-through into the persistent stdio MCP subprocess.
    """
    _require_gateway_auth(req)

    method = (body.get("method") or "").strip()
    if not method:
        raise HTTPException(status_code=400, detail="json-rpc method required")

    params = body.get("params")
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="json-rpc params must be an object")

    # Optional header-based injection so Claude can authenticate without embedding secrets in prompts.
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
    # Preserve Claude's ids (may be string) while still using integer ids on the stdio side.
    external_id = body.get("id")
    resp = await c.jsonrpc(method, params, external_id=external_id)
    return JSONResponse(resp)

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