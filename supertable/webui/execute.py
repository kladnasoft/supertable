# route: supertable.reflection.execute
"""
Execute tab: SQL execution page, query API, and schema autocomplete API.
"""
from __future__ import annotations

import enum
import json
import re
import uuid
import logging
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from fastapi import Query, HTTPException, Request, Depends, Body
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

logger = logging.getLogger(__name__)


# ────────────────────────────────────────────────────────────────────
# SQL helpers
# ────────────────────────────────────────────────────────────────────

def _clean_sql_query(query: str) -> str:
    """Remove comments and trailing semicolons."""
    q = re.sub(r'--.*$', '', query, flags=re.MULTILINE)
    q = re.sub(r'/\*.*?\*/', '', q, flags=re.DOTALL)
    q = re.sub(r';+$', '', q)
    return q.strip()


def _apply_limit_safely(query: str, max_rows: int) -> str:
    """Ensure a LIMIT is present and not above *max_rows + 1*."""
    limit_pattern = r'(?<!\w)(limit)\s+(\d+)(?!\w)(?=[^;]*$|;)'
    m = re.search(limit_pattern, query, re.IGNORECASE)
    if m:
        cur = int(m.group(2))
        if cur > max_rows + 1:
            return re.sub(limit_pattern, f'LIMIT {max_rows + 1}', query,
                          flags=re.IGNORECASE, count=1)
        return query
    return f"{query.rstrip(';').strip()} LIMIT {max_rows + 1}"


# ────────────────────────────────────────────────────────────────────
# JSON sanitizer (Decimal, datetime, numpy, Enum, …)
# ────────────────────────────────────────────────────────────────────

def _sanitize_for_json(obj: Any) -> Any:
    """Convert arbitrary objects into JSON-safe structures."""
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    if isinstance(obj, uuid.UUID):
        return str(obj)
    if isinstance(obj, bytes):
        try:
            return obj.decode("utf-8", errors="ignore")
        except Exception:
            return str(obj)
    if isinstance(obj, enum.Enum):
        return getattr(obj, "name", str(obj))

    # numpy scalars (without importing numpy)
    np_names = (
        "int8", "int16", "int32", "int64",
        "uint8", "uint16", "uint32", "uint64",
        "float16", "float32", "float64",
    )
    if obj.__class__.__name__ in np_names:
        try:
            return obj.item()
        except Exception:
            return float(obj) if "float" in obj.__class__.__name__ else int(obj)

    # Containers
    if isinstance(obj, dict):
        return {_sanitize_for_json(k): _sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_sanitize_for_json(x) for x in obj]
    if isinstance(obj, set):
        return [_sanitize_for_json(x) for x in obj]

    # Objects with dict-like view
    for attr in ("_asdict", "dict", "__dict__"):
        if hasattr(obj, attr):
            try:
                return _sanitize_for_json(getattr(obj, attr)())
            except Exception:
                pass

    return str(obj)


# ────────────────────────────────────────────────────────────────────
# Route attachment
# ────────────────────────────────────────────────────────────────────


# ---------------------------------------------------------------------------
# Route attachment — no-op stub.
# All endpoints previously registered here have moved to supertable.api.api.
# This function is preserved so existing callers do not break.
# ---------------------------------------------------------------------------
