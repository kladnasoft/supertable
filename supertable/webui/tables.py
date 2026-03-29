from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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
