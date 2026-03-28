# route: supertable.reflection.users
"""
Users page — user management via UserManager/RoleManager.

All endpoint handlers previously registered by attach_users_routes()
have moved to supertable.api.api.  This function is preserved as a
no-op so existing callers do not break.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Request
from fastapi.responses import Response

logger = logging.getLogger(__name__)


def attach_users_routes(
    router: APIRouter,
    *,
    templates: Any,
    settings: Any,
    redis_client: Any,
    catalog: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Response], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], List[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[str, str]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Any],
    list_users: Callable[[str, str], List[Dict[str, Any]]],
    list_roles: Callable[[str, str], List[Dict[str, Any]]],
    read_user: Callable[[str, str, str], Optional[Dict[str, Any]]],
    read_role: Callable[[str, str, str], Optional[Dict[str, Any]]],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
) -> None:
    """No-op — endpoints moved to supertable.api.api."""
    pass
