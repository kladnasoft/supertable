# route: supertable.reflection.engine
"""
SQL Engine page — focused view of spark-thrift compute pools.

Serves:
  GET /reflection/engine  — SQL Engine dashboard page

All CRUD operations reuse the existing /reflection/compute/* endpoints.
The frontend filters to kind=spark-thrift only.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def attach_engine_routes(
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
    """Register GET /reflection/engine on the shared router."""

    @router.get("/reflection/engine", response_class=HTMLResponse)
    def engine_page(
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
            "has_tenant": bool(sel_org and sel_sup),
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("engine.html", ctx)
        no_store(resp)
        return resp
