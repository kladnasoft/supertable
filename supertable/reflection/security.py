# route: supertable.reflection.security
"""
Unified Security page — serves security.html which combines
RBAC (roles) and Users (users + tokens) management.

All API endpoints remain in their original modules:
  - rbac.py:  /reflection/rbac/roles/*, /reflection/rbac/users/*
  - users.py: /reflection/users-page/*, /reflection/tokens/*

This module only adds the HTML page route at /reflection/security.
"""
from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def attach_security_routes(
    router: APIRouter,
    *,
    templates: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], List],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[str, str]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Any],
    list_users: Callable[[str, str], List[Dict[str, Any]]],
    list_roles: Callable[[str, str], List[Dict[str, Any]]],
) -> None:
    """Attach /reflection/security page route."""

    @router.get("/reflection/security", response_class=HTMLResponse)
    def security_page(
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
        resp = templates.TemplateResponse("security.html", ctx)
        no_store(resp)
        return resp
