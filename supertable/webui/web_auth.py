# route: supertable.webui.web_auth
"""
WebUI-only authentication and template rendering.

This module contains the Jinja2 template engine and login page rendering
that are exclusive to the browser UI server.  The API server never imports
this module.
"""
from __future__ import annotations

from typing import Optional

from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from supertable.server_common import (
    settings,
    _no_store,
    _security_headers,
    inject_session_into_ctx,
)

# ------------------------------ Jinja2 Templates ------------------------------

templates = Jinja2Templates(directory=settings.TEMPLATES_DIR)

# ------------------------------ Login page rendering --------------------------


def _render_login(
    request: Request,
    message: Optional[str] = None,
    clear_cookie: bool = False,
) -> HTMLResponse:
    ctx = {
        "request": request,
        "message": message or "",
        "SUPERTABLE_LOGIN_MASK": settings.SUPERTABLE_LOGIN_MASK,
    }
    inject_session_into_ctx(ctx, request)
    resp = templates.TemplateResponse("login.html", ctx, status_code=200)
    if clear_cookie:
        resp.delete_cookie("st_admin_token", path="/")
    _no_store(resp)
    _security_headers(resp)
    return resp
