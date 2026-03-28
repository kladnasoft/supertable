import hashlib
import json as _json
import logging
import pandas as pd
from fastapi import (
    Path,
    Query,
    Request as fastapiRequest,
    Body,
    APIRouter,
    Response,
    Depends,
    HTTPException,
)
from supertable.data_reader import DataReader, Status
from reflection.odata_handler import (
    get_tables,
    get_schemas,
    create_schema_xml,
    apply_odata_query_options,
    create_odata_response,
)
from reflection.security import get_current_username
from reflection.application import SERVER_IP, SERVER_PORT
from fastapi.responses import JSONResponse

_logger = logging.getLogger(__name__)


def _get_redis():
    """Lazy import of Redis client for bearer token verification."""
    try:
        from supertable.reflection.common import redis_client
        return redis_client
    except ImportError:
        try:
            from reflection.common import redis_client
            return redis_client
        except ImportError:
            return None


def _verify_bearer(request: fastapiRequest):
    """Check for OData bearer token. Returns context dict or None."""
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("bearer "):
        return None
    token = auth[7:].strip()
    if not token or not token.startswith("st_od_"):
        return None
    rc = _get_redis()
    if not rc:
        return None
    try:
        from supertable.reflection.security import verify_odata_bearer_token
    except ImportError:
        try:
            from reflection.security import verify_odata_bearer_token
        except ImportError:
            return None
    return verify_odata_bearer_token(rc, token)


async def _odata_auth(request: fastapiRequest):
    """Combined auth: try bearer token first, fall back to basic auth."""
    bearer = _verify_bearer(request)
    if bearer:
        # Stash on request state so handlers can read org/sup/role
        request.state.odata_bearer = bearer
        return bearer
    # Fall back to basic auth — will raise 401 if not authenticated
    request.state.odata_bearer = None
    return get_current_username(request)


router = APIRouter(prefix="/api/v1/reflection")


JSON_HEADER = {
    "Content-Type": "application/json;odata.metadata=minimal;odata.streaming=true;IEEE754Compatible=false;charset=utf-8",
    "OData-Version": "4.0",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, OData-Version, Authorization, Accept",
    "OData-MaxVersion": "4.0",
}

XML_HEADER = {
    "Content-Type": "application/xml;charset=utf-8",
    "OData-Version": "4.0",
    "Cache-Control": "no-cache, no-store, must-revalidate",
    "Pragma": "no-cache",
    "Expires": "0",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type, OData-Version, Authorization, Accept",
    "OData-MaxVersion": "4.0",
}


@router.get(
    "/{organization}/{super_name}/service/odata",
    operation_id="list_tables",
    dependencies=[Depends(_odata_auth)],
)
async def list_tables(
    organization: str = Path(...),
    super_name: str = Path(...),
    role: str = Query("", alias="role"),
):
    tables, _ = get_tables(super_name, organization=organization)

    entity_sets = [
        {"name": table, "kind": "EntitySet", "url": table} for table in tables
    ]

    response_data = {
        "@odata.context": f"https://{SERVER_IP}:{SERVER_PORT}/api/v1/reflection/{organization}/{super_name}/odata/$metadata",
        "value": entity_sets,
    }

    return JSONResponse(content=response_data, headers=JSON_HEADER)


@router.get(
    "/{organization}/{super_name}/odata/$metadata",
    operation_id="get_metadata",
    dependencies=[Depends(_odata_auth)],
)
async def get_metadata(
    organization: str = Path(...),
    super_name: str = Path(...),
    role: str = Query("", alias="role"),
):
    schemas = get_schemas(super_name, organization=organization)
    metadata_xml = create_schema_xml(super_name, schemas)
    return Response(
        content=metadata_xml, media_type="application/xml", headers=XML_HEADER
    )


@router.get(
    "/{organization}/{super_name}/odata/{entity_set_name}",
    operation_id="query_entity_set",
    dependencies=[Depends(_odata_auth)],
)
async def query_entity_set(
    organization: str,
    super_name: str,
    entity_set_name: str,
    request: fastapiRequest,
    role: str = Query("", alias="role"),
    select: str = Query(None, alias="$select"),
    filter_: str = Query(None, alias="$filter"),
    top: int = Query(None, alias="$top"),
    apply: str = Query(None, alias="$apply"),
):
    query_params = request.query_params
    query = apply_odata_query_options(entity_set_name, query_params)

    # Bearer token carries the role; query param is fallback for basic auth
    bearer = getattr(request.state, "odata_bearer", None)
    if bearer and isinstance(bearer, dict):
        role_name = bearer.get("role_name") or None
        if bearer.get("org") != organization or bearer.get("sup") != super_name:
            raise HTTPException(
                status_code=403,
                detail="Token is not valid for this organization/super_name",
            )
    else:
        role_name = (role or "").strip() or None

    data_reader = DataReader(
        super_name=super_name,
        organization=organization,
        query=query,
    )
    result, status, message = data_reader.execute(
        role_name=role_name,
        with_scan=False,
    )

    if status != Status.OK:
        return JSONResponse(content={"error": message}, status_code=400)

    odata_response = create_odata_response(entity_set_name, result)

    return JSONResponse(content=odata_response, headers=JSON_HEADER)
