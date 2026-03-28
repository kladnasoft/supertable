from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


KINDS = ("in-process", "spark", "spark-thrift", "spark-plug", "kubernetes")
SIZES = ("small", "medium", "large")

def _notebook_port() -> int:
    raw = str(os.getenv("SUPERTABLE_NOTEBOOK_PORT", "8010") or "8010").strip()
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
    url = os.getenv("SUPERTABLE_REFLECTION_REDIS_URL") or os.getenv("SUPERTABLE_REDIS_URL") or os.getenv("REDIS_URL")
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
        return redis.Redis.from_url(  # type: ignore[attr-defined]
            url,
            decode_responses=True,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
        )
    except Exception:
        return None


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
    base = Path(os.getenv("SUPERTABLE_REFLECTION_STATE_DIR", "/tmp/supertable_reflection"))
    new_dir = base / "compute"
    old_dir = base / "compute"

    # Ensure the new directory exists.
    new_dir.mkdir(parents=True, exist_ok=True)

    # Best-effort migration: if legacy files exist under compute_pools/, copy them once.
    try:
        if old_dir.exists() and old_dir.is_dir():
            for p in old_dir.glob("*.json"):
                dest = new_dir / p.name
                if not dest.exists():
                    shutil.copy2(p, dest)
    except Exception:
        # Never fail requests due to migration issues.
        pass

    return new_dir


def _safe_slug(v: str) -> str:
    v = (v or "").strip()
    out = []
    for ch in v:
        if ch.isalnum() or ch in ("-", "_", "."):
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)[:200] or "x"


def _tenant_path(org: str, sup: str) -> Path:
    return _state_dir() / f"{_safe_slug(org)}__{_safe_slug(sup)}.json"


def _default_data() -> Dict[str, Any]:
    return {
        "items": [
            {
                "id": "default",
                "name": "Default",
                "kind": "in-process",
                "size": "small",
                "max_concurrency": 1,
                "has_internet": False,
                "ws_url": _default_ws_url(),
                "is_default": True,
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
    """
    Normalize any incoming item to the unified pool schema.

    Common fields (all kinds):
      id, name, kind, size, max_concurrency, has_internet, ws_url, is_default, status

    Kind-specific extra fields (preserved alongside common fields):
      spark-thrift: thrift_host, thrift_port, s3_enabled, s3_endpoint, min_bytes, max_bytes
      spark-plug:   spark_master, ws_url (overloaded), webui_url

    Unknown fields are discarded.
    """
    pid = str(item.get("id") or "").strip()
    name = str(item.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")

    kind = _validate_kind(item.get("kind"))

    # For in-process, skip ws_url validation (no external endpoint).
    ws_raw = item.get("ws_url", item.get("ws", item.get("websocket_url")))
    if kind == "in-process":
        ws_url = str(ws_raw or "").strip()
    elif kind == "spark-plug":
        # spark-plug ws_url uses ws://…/ws/spark, allow it through
        ws_url = str(ws_raw or "ws://localhost:8010/ws/spark").strip()
    else:
        ws_url = _validate_ws_url(ws_raw)

    out: Dict[str, Any] = {
        "id": pid,
        "name": name,
        "kind": kind,
        "size": _validate_size(item.get("size")),
        "max_concurrency": _validate_max_concurrency(item.get("max_concurrency", 1)),
        "has_internet": _coerce_bool(item.get("has_internet", item.get("internet", False))),
        "ws_url": ws_url,
        "is_default": _coerce_bool(item.get("is_default", False)),
        "status": _validate_status(item.get("status", "active")),
    }

    # Kind-specific extra fields
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

    # Back-compat: if an older client sends "profile", derive has_internet from it (unless explicitly set).
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
            # tolerate legacy items (e.g. missing fields)
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
            # Preserve kind-specific fields for _sanitize_item
            for extra_key in (
                "thrift_host", "thrift_port", "s3_enabled", "s3_endpoint",
                "min_bytes", "max_bytes",
                "spark_master", "webui_url",
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

    # ensure exactly one default (or pick first)
    if not any(isinstance(it, dict) and it.get("is_default") for it in out_items) and out_items:
        out_items[0]["is_default"] = True
    else:
        # if multiple defaults, keep the first as default
        seen = False
        for it in out_items:
            if it.get("is_default") and not seen:
                seen = True
            elif it.get("is_default") and seen:
                it["is_default"] = False

    data = {"items": out_items}

    # Best-effort migration: if the state came from disk, persist into Redis (if configured).
    if loaded_from_file:
        _write_state(org, sup, data)

    return data



def _save(org: str, sup: str, data: Dict[str, Any]) -> None:
    # Prefer Redis (shared, durable), with a disk fallback for environments without Redis.
    if _write_state(org, sup, data):
        return
    p = _tenant_path(org, sup)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")



def attach_compute_routes(
    router: APIRouter,
    *,
    templates: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], Sequence[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], None],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
) -> None:
    @router.get("/reflection/compute", response_class=HTMLResponse)
    def compute_page(
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
        tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": bool(sel_org and sel_sup),
            "notebook_port": _notebook_port(),
            "notebook_ws_default": _default_ws_url(),
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("compute.html", ctx)
        no_store(resp)
        return resp

    @router.get("/reflection/compute/list")
    def pools_list(org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        return {"ok": True, "data": _load(org, sup), "kinds": list(KINDS), "sizes": list(SIZES)}

    @router.post("/reflection/compute/upsert")
    def pools_upsert(payload: Dict[str, Any] = Body(...), _: Any = Depends(logged_in_guard_api)):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        item = payload.get("item") or {}
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")
        if not isinstance(item, dict):
            raise HTTPException(status_code=400, detail="item must be an object")

        data = _load(org, sup)
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []

        sanitized = _sanitize_item(item)

        item_id = str(sanitized.get("id") or "").strip() or os.urandom(6).hex()
        sanitized["id"] = item_id

        # ensure default logic
        if sanitized.get("is_default"):
            for it in items:
                if isinstance(it, dict):
                    it["is_default"] = False

        out = []
        replaced = False
        for it in items:
            if isinstance(it, dict) and str(it.get("id")) == item_id:
                out.append(sanitized)
                replaced = True
            else:
                out.append(it)

        if not replaced:
            out.insert(0, sanitized)

        # if none default, set first
        if not any(isinstance(it, dict) and it.get("is_default") for it in out) and out:
            if isinstance(out[0], dict):
                out[0]["is_default"] = True

        data["items"] = out
        _save(org, sup, data)

        # Sync kind-specific data to RedisCatalog for thrift/plug pools.
        _sync_pool_to_catalog(org, sanitized)

        return {"ok": True, "id": item_id}

    @router.delete("/reflection/compute/{pool_id}")
    def pools_delete(pool_id: str, org: str, sup: str, _: Any = Depends(logged_in_guard_api)):
        org = str(org or "").strip()
        sup = str(sup or "").strip()
        if not org or not sup:
            raise HTTPException(status_code=400, detail="org/sup required")

        data = _load(org, sup)
        items = data.get("items") or []
        if not isinstance(items, list):
            items = []

        data["items"] = [it for it in items if not (isinstance(it, dict) and str(it.get("id")) == pool_id)]
        if not any(isinstance(it, dict) and it.get("is_default") for it in data["items"]) and data["items"]:
            if isinstance(data["items"][0], dict):
                data["items"][0]["is_default"] = True

        _save(org, sup, data)
        return {"ok": True}

    # ── Catalog sync helper ────────────────────────────────────────

    def _sync_pool_to_catalog(org: str, pool: Dict[str, Any]) -> None:
        """
        Best-effort sync of kind-specific pool data into RedisCatalog.
        spark-thrift pools are also stored as thrift clusters.
        spark-plug pools are also stored as plug entries.
        This keeps the catalog consistent for routing/selection logic.
        """
        kind = str(pool.get("kind") or "").strip()
        pool_id = str(pool.get("id") or "").strip()
        if not org or not pool_id:
            return
        try:
            catalog = _get_catalog()
        except Exception:
            return

        if kind == "spark-thrift":
            config = {
                "name": pool.get("name", ""),
                "thrift_host": pool.get("thrift_host", ""),
                "thrift_port": pool.get("thrift_port", 10000),
                "status": pool.get("status", "active"),
                "min_bytes": pool.get("min_bytes", 0),
                "max_bytes": pool.get("max_bytes", 0),
                "s3_enabled": pool.get("s3_enabled", True),
                "s3_endpoint": pool.get("s3_endpoint", ""),
            }
            try:
                catalog.register_spark_cluster(org, pool_id, config)
            except Exception:
                pass
        elif kind == "spark-plug":
            config = {
                "name": pool.get("name", ""),
                "spark_master": pool.get("spark_master", "spark://localhost:7077"),
                "ws_url": pool.get("ws_url", "ws://localhost:8010/ws/spark"),
                "webui_url": pool.get("webui_url", ""),
                "status": pool.get("status", "active"),
            }
            try:
                catalog.register_spark_plug(org, pool_id, config)
            except Exception:
                pass

    # ── Test connection endpoint ───────────────────────────────────

    @router.post("/reflection/compute/test-connection")
    def test_connection(
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        """
        Test connectivity for a pool before saving.
        Checks vary by kind:
          spark-thrift: TCP connect to thrift_host:thrift_port
          spark-plug:   WebSocket upgrade handshake to ws_url
        """
        import socket

        kind = str(payload.get("kind") or "").strip().lower()
        if kind == "in-process":
            return {"ok": True, "message": "In-process — no connection needed"}

        if kind == "spark-thrift":
            host = str(payload.get("thrift_host") or "").strip()
            try:
                port = int(payload.get("thrift_port") or 10000)
            except (ValueError, TypeError):
                port = 10000
            label = f"thrift {host}:{port}"
            if not host:
                raise HTTPException(status_code=400, detail="thrift_host is required for testing")

            try:
                sock = socket.create_connection((host, port), timeout=5.0)
                sock.close()
                return {"ok": True, "message": f"Connected to {label}"}
            except socket.timeout:
                return {"ok": False, "message": f"Timeout connecting to {label}"}
            except OSError as e:
                return {"ok": False, "message": f"Cannot reach {label}: {e}"}

        # spark-plug: real WebSocket upgrade handshake
        ws_raw = str(payload.get("ws_url") or "").strip()
        if not ws_raw:
            raise HTTPException(status_code=400, detail="ws_url is required for testing")

        parts = urlsplit(ws_raw)
        host = parts.hostname or ""
        use_ssl = parts.scheme in ("wss", "https")
        port = parts.port or (443 if use_ssl else 80)
        path = parts.path or "/"
        label = f"ws {host}:{port}{path}"
        if not host:
            raise HTTPException(status_code=400, detail="Cannot parse host from ws_url")

        import hashlib
        import base64

        try:
            sock = socket.create_connection((host, port), timeout=5.0)
        except socket.timeout:
            return {"ok": False, "message": f"Timeout connecting to {label}"}
        except OSError as e:
            return {"ok": False, "message": f"Cannot reach {label}: {e}"}

        try:
            if use_ssl:
                import ssl
                ctx_ssl = ssl.create_default_context()
                sock = ctx_ssl.wrap_socket(sock, server_hostname=host)

            # WebSocket upgrade request (RFC 6455)
            ws_key = base64.b64encode(os.urandom(16)).decode()
            upgrade_req = (
                f"GET {path} HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                f"Upgrade: websocket\r\n"
                f"Connection: Upgrade\r\n"
                f"Sec-WebSocket-Key: {ws_key}\r\n"
                f"Sec-WebSocket-Version: 13\r\n"
                f"\r\n"
            )
            sock.sendall(upgrade_req.encode())

            # Read response (up to 4KB is plenty for headers)
            sock.settimeout(5.0)
            resp_data = b""
            while b"\r\n\r\n" not in resp_data and len(resp_data) < 4096:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                resp_data += chunk

            resp_text = resp_data.decode("utf-8", errors="replace")
            first_line = resp_text.split("\r\n", 1)[0] if resp_text else ""

            if "101" in first_line:
                return {"ok": True, "message": f"WebSocket handshake OK — {label}"}
            elif first_line:
                return {"ok": False, "message": f"Server responded: {first_line[:80]}"}
            else:
                return {"ok": False, "message": f"No response from {label} (connection closed)"}
        except socket.timeout:
            return {"ok": False, "message": f"Timeout during WebSocket handshake to {label}"}
        except OSError as e:
            return {"ok": False, "message": f"WebSocket handshake failed for {label}: {e}"}
        finally:
            try:
                sock.close()
            except Exception:
                pass

    # ── Spark Thrift cluster management (org-scoped) ────────────────

    def _get_catalog():
        """Lazily import and instantiate RedisCatalog."""
        try:
            from supertable.redis_catalog import RedisCatalog
        except Exception:
            from redis_catalog import RedisCatalog  # type: ignore
        return RedisCatalog()

    @router.get("/reflection/compute/spark/thrifts")
    def spark_thrifts_list(org: str, _: Any = Depends(logged_in_guard_api)):
        org = str(org or "").strip()
        if not org:
            raise HTTPException(status_code=400, detail="org is required")
        try:
            catalog = _get_catalog()
            clusters = catalog.list_spark_clusters(org)
            return {"ok": True, "clusters": clusters}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to list Spark thrifts: {e}")

    @router.post("/reflection/compute/spark/thrifts")
    def spark_thrift_upsert(
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(admin_guard_api),
    ):
        org = str(payload.get("org") or "").strip()
        cluster_id = str(payload.get("cluster_id") or "").strip()
        name = str(payload.get("name") or "").strip()
        thrift_host = str(payload.get("thrift_host") or "").strip()

        if not org:
            raise HTTPException(status_code=400, detail="org is required")
        if not cluster_id:
            raise HTTPException(status_code=400, detail="cluster_id is required")
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
        if not thrift_host:
            raise HTTPException(status_code=400, detail="thrift_host is required")

        thrift_port = 10000
        try:
            thrift_port = int(payload.get("thrift_port") or 10000)
        except (ValueError, TypeError):
            pass
        if thrift_port < 1 or thrift_port > 65535:
            thrift_port = 10000

        status_val = str(payload.get("status") or "active").strip().lower()
        if status_val not in ("active", "draining", "offline"):
            status_val = "active"

        min_bytes = max(0, int(payload.get("min_bytes") or 0))
        max_bytes = max(0, int(payload.get("max_bytes") or 0))

        config = {
            "name": name,
            "thrift_host": thrift_host,
            "thrift_port": thrift_port,
            "status": status_val,
            "min_bytes": min_bytes,
            "max_bytes": max_bytes,
            "s3_enabled": bool(payload.get("s3_enabled", True)),
        }
        for field_name in ("auth", "username", "password"):
            val = payload.get(field_name)
            if val is not None:
                config[field_name] = str(val)

        try:
            catalog = _get_catalog()
            catalog.register_spark_cluster(org, cluster_id, config)
            return {"ok": True, "cluster_id": cluster_id}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to register thrift: {e}")

    @router.post("/reflection/compute/spark/thrifts/status")
    def spark_thrift_status_update(
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(admin_guard_api),
    ):
        org = str(payload.get("org") or "").strip()
        cluster_id = str(payload.get("cluster_id") or "").strip()
        status_val = str(payload.get("status") or "").strip().lower()

        if not org or not cluster_id:
            raise HTTPException(status_code=400, detail="org and cluster_id are required")
        if status_val not in ("active", "draining", "offline"):
            raise HTTPException(status_code=400, detail="status must be: active, draining, offline")

        try:
            catalog = _get_catalog()
            ok = catalog.update_spark_cluster_status(org, cluster_id, status_val)
            if not ok:
                raise HTTPException(status_code=404, detail="Cluster not found")
            return {"ok": True}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update status: {e}")

    @router.delete("/reflection/compute/spark/thrifts/{cluster_id}")
    def spark_thrift_delete(cluster_id: str, org: str, _: Any = Depends(admin_guard_api)):
        org = str(org or "").strip()
        cluster_id = str(cluster_id or "").strip()
        if not org or not cluster_id:
            raise HTTPException(status_code=400, detail="org and cluster_id are required")
        try:
            catalog = _get_catalog()
            ok = catalog.deregister_spark_cluster(org, cluster_id)
            if not ok:
                raise HTTPException(status_code=404, detail="Cluster not found")
            return {"ok": True}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to deregister thrift: {e}")

    # ── Spark Plug management (org-scoped) ──────────────────────────

    @router.get("/reflection/compute/spark/plugs")
    def spark_plugs_list(org: str, _: Any = Depends(logged_in_guard_api)):
        org = str(org or "").strip()
        if not org:
            raise HTTPException(status_code=400, detail="org is required")
        try:
            catalog = _get_catalog()
            plugs = catalog.list_spark_plugs(org)
            return {"ok": True, "plugs": plugs}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to list Spark plugs: {e}")

    @router.post("/reflection/compute/spark/plugs")
    def spark_plug_upsert(
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(admin_guard_api),
    ):
        org = str(payload.get("org") or "").strip()
        plug_id = str(payload.get("plug_id") or "").strip()
        name = str(payload.get("name") or "").strip()

        if not org:
            raise HTTPException(status_code=400, detail="org is required")
        if not plug_id:
            raise HTTPException(status_code=400, detail="plug_id is required")
        if not name:
            raise HTTPException(status_code=400, detail="name is required")

        status_val = str(payload.get("status") or "active").strip().lower()
        if status_val not in ("active", "draining", "offline"):
            status_val = "active"

        config = {
            "name": name,
            "spark_master": str(payload.get("spark_master") or "spark://localhost:7077").strip(),
            "ws_url": str(payload.get("ws_url") or "ws://localhost:8010/ws/spark").strip(),
            "webui_url": str(payload.get("webui_url") or "").strip(),
            "status": status_val,
        }

        try:
            catalog = _get_catalog()
            catalog.register_spark_plug(org, plug_id, config)
            return {"ok": True, "plug_id": plug_id}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to register plug: {e}")

    @router.post("/reflection/compute/spark/plugs/status")
    def spark_plug_status_update(
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(admin_guard_api),
    ):
        org = str(payload.get("org") or "").strip()
        plug_id = str(payload.get("plug_id") or "").strip()
        status_val = str(payload.get("status") or "").strip().lower()

        if not org or not plug_id:
            raise HTTPException(status_code=400, detail="org and plug_id are required")
        if status_val not in ("active", "draining", "offline"):
            raise HTTPException(status_code=400, detail="status must be: active, draining, offline")

        try:
            catalog = _get_catalog()
            ok = catalog.update_spark_plug_status(org, plug_id, status_val)
            if not ok:
                raise HTTPException(status_code=404, detail="Plug not found")
            return {"ok": True}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to update plug status: {e}")

    @router.delete("/reflection/compute/spark/plugs/{plug_id}")
    def spark_plug_delete(plug_id: str, org: str, _: Any = Depends(admin_guard_api)):
        org = str(org or "").strip()
        plug_id = str(plug_id or "").strip()
        if not org or not plug_id:
            raise HTTPException(status_code=400, detail="org and plug_id are required")
        try:
            catalog = _get_catalog()
            ok = catalog.deregister_spark_plug(org, plug_id)
            if not ok:
                raise HTTPException(status_code=404, detail="Plug not found")
            return {"ok": True}
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Failed to deregister plug: {e}")