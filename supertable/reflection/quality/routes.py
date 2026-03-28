# route: supertable.reflection.quality.routes
# supertable/reflection/quality/routes.py
"""
FastAPI routes for Data Quality module.

DEPRECATED: All endpoints previously registered by attach_quality_routes()
have moved to supertable.api.api. This function is preserved as a no-op
so existing callers do not break.
"""
from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Request

logger = logging.getLogger(__name__)


def attach_quality_routes(
    router: APIRouter,
    *,
    templates: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], str],
    discover_pairs: Callable[[], List[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Dict[str, Any]],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
    catalog: Any = None,
    redis_client: Any = None,
    get_session: Any = None,
) -> None:
    """No-op — endpoints moved to supertable.api.api."""
    pass
