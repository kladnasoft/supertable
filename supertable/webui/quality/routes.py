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
