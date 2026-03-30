# route: supertable.odata.odata_server
"""
OData server — FastAPI route handlers for the independent OData service.

Endpoints:
  GET  /{org}/{sup}/odata              — Service root (list entity sets)
  GET  /{org}/{sup}/odata/$metadata    — CSDL 4.0 schema XML
  GET  /{org}/{sup}/odata/{entity_set} — Query entity set with OData options

Authentication:
  Every request requires a valid OData bearer token (st_od_*).
  The token is created via the API's /reflection/odata/endpoints management
  endpoints and carries an RBAC role_name.  All data access is scoped to
  that role — the same RBAC enforcement as the REST API.

Audit:
  Every data query emits an ODATA_ACCESS audit event.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Path, Query, Request
from fastapi.responses import JSONResponse, Response

from supertable.config.settings import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Redis client (lazy singleton — same pattern as server_common.py)
# ---------------------------------------------------------------------------

_redis_client = None


def _get_redis():
    """Return the shared Redis client, creating it on first call."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    try:
        from supertable.redis_connector import RedisConnector
        _redis_client = RedisConnector().r
    except Exception as e:
        logger.error("OData: Redis connection failed: %s", e)
        raise HTTPException(status_code=503, detail="Service unavailable: Redis not reachable")
    return _redis_client


# ---------------------------------------------------------------------------
# Auth dependency — OData bearer token
# ---------------------------------------------------------------------------

def _odata_auth(request: Request) -> Dict[str, Any]:
    """Validate the OData bearer token and return the endpoint context.

    Returns a dict with keys: org, sup, endpoint_id, role_name.
    Raises 401 if the token is missing/invalid, 403 if the endpoint is disabled.
    """
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(
            status_code=401,
            detail="Missing or invalid Authorization header. Use: Bearer <token>",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = auth_header[7:].strip()
    if not token or not token.startswith("st_od_"):
        raise HTTPException(
            status_code=401,
            detail="Invalid token format. OData tokens start with st_od_",
            headers={"WWW-Authenticate": "Bearer"},
        )

    rc = _get_redis()
    try:
        from supertable.services.security import verify_odata_bearer_token
        ctx = verify_odata_bearer_token(rc, token)
    except Exception as e:
        logger.error("OData: token verification failed: %s", e)
        raise HTTPException(status_code=500, detail="Token verification error")

    if ctx is None:
        raise HTTPException(
            status_code=401,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return ctx


def _enforce_scope(ctx: Dict[str, Any], org: str, sup: str) -> None:
    """Ensure the bearer token is scoped to the requested org/sup."""
    if ctx.get("org") != org or ctx.get("sup") != sup:
        raise HTTPException(
            status_code=403,
            detail="Token is not valid for this organization/SuperTable",
        )


# ---------------------------------------------------------------------------
# Audit helper
# ---------------------------------------------------------------------------

def _audit_odata(request: Request, ctx: Dict[str, Any], **detail_kwargs) -> None:
    """Emit an ODATA_ACCESS audit event.  Never raises."""
    try:
        from supertable.audit import (
            emit as _audit, EventCategory, Actions, Severity, make_detail,
        )
        _audit(
            category=EventCategory.EXPORT,
            action=Actions.ODATA_ACCESS,
            organization=ctx.get("org", ""),
            super_name=ctx.get("sup", ""),
            resource_type="odata",
            resource_id=ctx.get("endpoint_id", ""),
            severity=Severity.INFO,
            detail=make_detail(
                role_name=ctx.get("role_name", ""),
                client_ip=request.client.host if request.client else "",
                **detail_kwargs,
            ),
        )
    except Exception:
        pass  # Never fail a request due to audit


# ---------------------------------------------------------------------------
# Response headers
# ---------------------------------------------------------------------------

_JSON_HEADERS = {
    "Content-Type": "application/json;odata.metadata=minimal;odata.streaming=true;"
                    "IEEE754Compatible=false;charset=utf-8",
    "OData-Version": "4.0",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}

_XML_HEADERS = {
    "Content-Type": "application/xml;charset=utf-8",
    "OData-Version": "4.0",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
}


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter()


@router.get("/{organization}/{super_name}/odata")
def odata_service_root(
    request: Request,
    organization: str = Path(...),
    super_name: str = Path(...),
    ctx: Dict[str, Any] = Depends(_odata_auth),
):
    """OData service root — list available entity sets."""
    _enforce_scope(ctx, organization, super_name)
    role_name = ctx.get("role_name", "")

    from supertable.odata.odata_handler import get_tables
    tables, _ = get_tables(super_name, organization, role_name)

    base_url = str(request.base_url).rstrip("/")
    context_url = f"{base_url}/{organization}/{super_name}/odata"

    entity_sets = [
        {"name": table, "kind": "EntitySet", "url": table}
        for table in tables
    ]

    response_data = {
        "@odata.context": f"{context_url}/$metadata",
        "value": entity_sets,
    }

    _audit_odata(request, ctx, operation="service_root", tables_count=len(tables))
    return JSONResponse(content=response_data, headers=_JSON_HEADERS)


@router.get("/{organization}/{super_name}/odata/$metadata")
def odata_metadata(
    request: Request,
    organization: str = Path(...),
    super_name: str = Path(...),
    ctx: Dict[str, Any] = Depends(_odata_auth),
):
    """OData $metadata — CSDL 4.0 schema XML."""
    _enforce_scope(ctx, organization, super_name)
    role_name = ctx.get("role_name", "")

    from supertable.odata.odata_handler import get_schemas, create_metadata_xml
    schemas = get_schemas(super_name, organization, role_name)
    metadata_xml = create_metadata_xml(super_name, schemas)

    _audit_odata(request, ctx, operation="metadata", tables_count=len(schemas))
    return Response(content=metadata_xml, media_type="application/xml", headers=_XML_HEADERS)


@router.get("/{organization}/{super_name}/odata/{entity_set_name}")
def odata_query(
    request: Request,
    organization: str = Path(...),
    super_name: str = Path(...),
    entity_set_name: str = Path(...),
    ctx: Dict[str, Any] = Depends(_odata_auth),
    select: Optional[str] = Query(None, alias="$select"),
    filter_: Optional[str] = Query(None, alias="$filter"),
    top: Optional[int] = Query(None, alias="$top"),
    apply: Optional[str] = Query(None, alias="$apply"),
):
    """Query an entity set with OData query options ($select, $filter, $top, $apply)."""
    _enforce_scope(ctx, organization, super_name)
    role_name = ctx.get("role_name", "")

    # Build safe SQL from OData query options
    from supertable.odata.odata_handler import build_query, format_odata_response

    query_params: Dict[str, str] = {}
    for key, value in request.query_params.items():
        if key.startswith("$"):
            query_params[key] = value

    try:
        sql = build_query(entity_set_name, query_params)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid query: {e}")

    # Execute through DataReader with full RBAC enforcement
    from supertable.data_reader import DataReader, Status

    t0 = time.perf_counter()
    data_reader = DataReader(
        super_name=super_name,
        organization=organization,
        query=sql,
    )
    result, status, message = data_reader.execute(
        role_name=role_name,
        with_scan=False,
    )
    duration_ms = (time.perf_counter() - t0) * 1000.0

    if status != Status.OK:
        _audit_odata(
            request, ctx,
            operation="query", entity_set=entity_set_name,
            status="error", error=message or "Query failed",
        )
        raise HTTPException(status_code=400, detail=message or "Query failed")

    # Format OData response
    base_url = str(request.base_url).rstrip("/")
    context_url = f"{base_url}/{organization}/{super_name}/odata"
    odata_response = format_odata_response(entity_set_name, result, context_url)

    row_count = len(odata_response.get("value", []))
    _audit_odata(
        request, ctx,
        operation="query", entity_set=entity_set_name,
        row_count=row_count, duration_ms=round(duration_ms),
    )

    return JSONResponse(content=odata_response, headers=_JSON_HEADERS)


@router.get("/healthz")
def odata_health():
    """Health check endpoint."""
    try:
        rc = _get_redis()
        rc.ping()
        return {"status": "ok", "service": "odata"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Unhealthy: {e}")
