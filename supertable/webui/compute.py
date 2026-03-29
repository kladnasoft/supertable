# route: supertable.reflection.compute
"""
Compute pool management — helpers and validation.

All endpoint handlers previously registered by attach_compute_routes()
have moved to supertable.api.api.  This module retains the helper
functions (_load, _save, _sanitize_item, etc.) used by api.py.
"""
from __future__ import annotations

import json
import os

from supertable.config.settings import settings
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


KINDS = ("in-process", "spark", "spark-thrift", "spark-plug", "kubernetes")
SIZES = ("small", "medium", "large")

def _notebook_port() -> int:
    raw = str(settings.SUPERTABLE_NOTEBOOK_PORT)
    try:
        port = int(raw)
    except Exception:
        port = 8010
    if port < 1 or port > 65535:
        port = 8010
    return port


def _default_ws_url() -> str:
    return f"ws://0.0.0.0:{_notebook_port()}/ws/execute"


def _redis_url() -> Optional[str]:
    url = settings.effective_redis_url
    url = str(url or "").strip()
    return url or None


def _redis_client():
    url = _redis_url()
    if not url:
        return None
    try:
        import redis  # type: ignore
    except Exception:
        return None
    try:
        return redis.Redis.from_url(
            url, decode_responses=True, socket_timeout=2.0, socket_connect_timeout=2.0,
        )
    except Exception:
        return None


def _safe_slug(v: str) -> str:
    v = (v or "").strip()
    out = []
    for ch in v:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:200] or "x"


def _redis_key(org: str, sup: str) -> str:
    return f"supertable:reflection:compute:{_safe_slug(org)}__{_safe_slug(sup)}"


def _read_state(org: str, sup: str) -> Optional[Dict[str, Any]]:
    client = _redis_client()
    if not client:
        return None
    try:
        raw = client.get(_redis_key(org, sup))
    except Exception:
        return None
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except Exception:
        return None
    return parsed if isinstance(parsed, dict) else None


def _write_state(org: str, sup: str, data: Dict[str, Any]) -> bool:
    client = _redis_client()
    if not client:
        return False
    try:
        client.set(_redis_key(org, sup), json.dumps(data, ensure_ascii=False, indent=2))
        return True
    except Exception:
        return False


def _state_dir() -> Path:
    base = Path(settings.SUPERTABLE_REFLECTION_STATE_DIR)
    new_dir = base / "compute"
    old_dir = base / "compute"
    new_dir.mkdir(parents=True, exist_ok=True)
    try:
        if old_dir.exists() and old_dir.is_dir():
            for p in old_dir.glob("*.json"):
                dest = new_dir / p.name
                if not dest.exists():
                    shutil.copy2(p, dest)
    except Exception:
        pass
    return new_dir


def _tenant_path(org: str, sup: str) -> Path:
    return _state_dir() / f"{_safe_slug(org)}__{_safe_slug(sup)}.json"


def _default_data() -> Dict[str, Any]:
    return {
        "items": [
            {
                "id": "default", "name": "Default", "kind": "in-process",
                "size": "small", "max_concurrency": 1, "has_internet": False,
                "ws_url": _default_ws_url(), "is_default": True,
            }
        ]
    }


def _coerce_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    s = str(v or "").strip().lower()
    return s in ("1", "true", "yes", "y", "on")


def _validate_kind(v: Any) -> str:
    s = str(v or "").strip().lower()
    if not s:
        return "in-process"
    if s not in KINDS:
        raise HTTPException(status_code=400, detail=f"invalid kind (allowed: {', '.join(KINDS)})")
    return s


def _validate_size(v: Any) -> str:
    s = str(v or "").strip().lower()
    if not s:
        return "small"
    if s not in SIZES:
        raise HTTPException(status_code=400, detail=f"invalid size (allowed: {', '.join(SIZES)})")
    return s


def _validate_max_concurrency(v: Any) -> int:
    try:
        n = int(v)
    except Exception:
        n = 1
    if n < 1:
        n = 1
    if n > 10_000:
        raise HTTPException(status_code=400, detail="max_concurrency too large")
    return n


def _validate_ws_url(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return _default_ws_url()
    if len(s) > 2048:
        raise HTTPException(status_code=400, detail="ws_url too long")
    parts = urlsplit(s)
    if parts.scheme not in {"ws", "wss"} or not parts.netloc:
        raise HTTPException(status_code=400, detail="ws_url must be a ws:// or wss:// URL")
    path = parts.path or "/ws/execute"
    if not path.startswith("/"):
        path = "/" + path
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, ""))


def _validate_status(v: Any) -> str:
    s = str(v or "").strip().lower()
    if s not in ("active", "draining", "offline"):
        return "active"
    return s


def _sanitize_item(item: Dict[str, Any]) -> Dict[str, Any]:
    pid = str(item.get("id") or "").strip()
    name = str(item.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    kind = _validate_kind(item.get("kind"))
    ws_raw = item.get("ws_url", item.get("ws", item.get("websocket_url")))
    if kind == "in-process":
        ws_url = str(ws_raw or "").strip()
    elif kind == "spark-plug":
        ws_url = str(ws_raw or "ws://localhost:8010/ws/spark").strip()
    else:
        ws_url = _validate_ws_url(ws_raw)

    out: Dict[str, Any] = {
        "id": pid, "name": name, "kind": kind,
        "size": _validate_size(item.get("size")),
        "max_concurrency": _validate_max_concurrency(item.get("max_concurrency", 1)),
        "has_internet": _coerce_bool(item.get("has_internet", item.get("internet", False))),
        "ws_url": ws_url,
        "is_default": _coerce_bool(item.get("is_default", False)),
        "status": _validate_status(item.get("status", "active")),
    }
    if kind == "spark-thrift":
        out["thrift_host"] = str(item.get("thrift_host") or "").strip()
        try:
            out["thrift_port"] = max(1, min(65535, int(item.get("thrift_port") or 10000)))
        except (ValueError, TypeError):
            out["thrift_port"] = 10000
        out["s3_enabled"] = _coerce_bool(item.get("s3_enabled", True))
        out["s3_endpoint"] = str(item.get("s3_endpoint") or "").strip()
        out["min_bytes"] = max(0, int(item.get("min_bytes") or 0))
        out["max_bytes"] = max(0, int(item.get("max_bytes") or 0))
    elif kind == "spark-plug":
        out["spark_master"] = str(item.get("spark_master") or "spark://localhost:7077").strip()
        out["webui_url"] = str(item.get("webui_url") or "").strip()

    if "profile" in item and "has_internet" not in item and "internet" not in item:
        prof = str(item.get("profile") or "").strip().lower()
        if prof in {"internet", "high", "on", "true", "1"}:
            out["has_internet"] = True
        elif prof in {"no-internet", "low", "off", "false", "0"}:
            out["has_internet"] = False

    return out


def _load(org: str, sup: str) -> Dict[str, Any]:
    raw: Optional[Dict[str, Any]] = _read_state(org, sup)
    loaded_from_file = False
    if raw is None:
        p = _tenant_path(org, sup)
        if not p.exists():
            return _default_data()
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            loaded_from_file = True
        except Exception:
            return _default_data()

    items = raw.get("items") if isinstance(raw, dict) else None
    if not isinstance(items, list):
        return _default_data()

    out_items = []
    for it in items:
        if not isinstance(it, dict):
            continue
        try:
            sanitized = {
                "id": str(it.get("id") or "").strip(),
                "name": str(it.get("name") or "").strip() or "Pool",
                "kind": (str(it.get("kind") or "").strip().lower() or "in-process"),
                "size": (str(it.get("size") or "").strip().lower() or "small"),
                "max_concurrency": it.get("max_concurrency", 1),
                "has_internet": it.get("has_internet", it.get("internet", False)),
                "ws_url": it.get("ws_url", it.get("ws", it.get("websocket_url"))),
                "is_default": it.get("is_default", False),
                "status": it.get("status", "active"),
                "profile": it.get("profile"),
            }
            for extra_key in (
                "thrift_host", "thrift_port", "s3_enabled", "s3_endpoint",
                "min_bytes", "max_bytes", "spark_master", "webui_url",
            ):
                if extra_key in it:
                    sanitized[extra_key] = it[extra_key]
            sanitized = _sanitize_item(sanitized)
        except HTTPException:
            continue
        if not sanitized.get("id"):
            sanitized["id"] = os.urandom(6).hex()
        out_items.append(sanitized)

    if not out_items:
        out_items = _default_data()["items"]

    if not any(isinstance(it, dict) and it.get("is_default") for it in out_items) and out_items:
        out_items[0]["is_default"] = True
    else:
        seen = False
        for it in out_items:
            if it.get("is_default") and not seen:
                seen = True
            elif it.get("is_default") and seen:
                it["is_default"] = False

    data = {"items": out_items}
    if loaded_from_file:
        _write_state(org, sup, data)
    return data


def _save(org: str, sup: str, data: Dict[str, Any]) -> None:
    if _write_state(org, sup, data):
        return
    p = _tenant_path(org, sup)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ---------------------------------------------------------------------------
# Route attachment — no-op stub.
# All endpoints have moved to supertable.api.api.
# ---------------------------------------------------------------------------
