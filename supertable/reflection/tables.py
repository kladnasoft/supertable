from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage

# Prefer installed package; fallback to local module for dev
try:
    from supertable.meta_reader import MetaReader  # type: ignore
except Exception:  # pragma: no cover
    from meta_reader import MetaReader  # type: ignore


logger = logging.getLogger(__name__)



# ---------------------------------------------------------------------------
# Route attachment — no-op stub.
# All endpoints and helper closures previously defined here have moved
# to supertable.api.api. This function is preserved so existing callers
# (routes.py) do not break.
# ---------------------------------------------------------------------------

def attach_tables_routes(
    router,
    *,
    templates,
    is_authorized,
    no_store,
    get_provided_token,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    list_users,
    fmt_ts,
    catalog,
    admin_guard_api,
    get_session,
):
    """No-op — endpoints moved to supertable.api.api."""
    pass
