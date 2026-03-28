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
    router,
    *,
    templates,
    is_authorized,
    no_store,
    get_provided_token,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    logged_in_guard_api,
    admin_guard_api,
):
    """No-op — endpoints moved to supertable.api.api."""
    pass
