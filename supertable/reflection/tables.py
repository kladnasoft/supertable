from __future__ import annotations

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


def attach_tables_routes(
    router: APIRouter,
    *,
    templates: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], str],
    discover_pairs: Callable[[], List[Tuple[str, str]]],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], Dict[str, Any]],
    list_users: Callable[[str, str], List[Dict[str, Any]]],
    fmt_ts: Callable[[int], str],
    list_leaves: Callable[..., Dict[str, Any]],
    catalog: Any,
    admin_guard_api: Any,
) -> None:
    """
    Register everything used by templates/tables.html.

    tables.html calls:
      - GET    /reflection/supers?organization=...
      - GET    /reflection/super?organization=...&super_name=...&user_hash=...
      - GET    /reflection/schema?organization=...&super_name=...&table=...&user_hash=...
      - GET    /reflection/stats?organization=...&super_name=...&table=...&user_hash=...
      - DELETE /reflection/table?organization=...&super_name=...&table=...
    """

    # ------------------------------ Redis helpers (SuperTables discovery) ------------------------------

    def _get_redis_items(pattern: str) -> List[str]:
        """
        Get Redis keys by SCAN pattern via RedisCatalog's redis client.
        """
        rc = RedisCatalog()
        try:
            items: List[str] = []
            cursor = 0
            while True:
                cursor, keys = rc.r.scan(cursor=cursor, match=pattern, count=1000)
                for key in keys:
                    if isinstance(key, (bytes, bytearray)):
                        items.append(key.decode("utf-8"))
                    else:
                        items.append(str(key))
                if cursor == 0:
                    break
            return items
        except Exception as e:
            logger.error("Error scanning Redis keys for pattern %s: %s", pattern, e)
            return []

    def list_supers(organization: str) -> List[str]:
        """
        List SuperTables for an organization by scanning Redis root meta keys.

        Root key pattern: supertable:{org}:{sup}:meta:root
        """
        organization = (organization or "").strip()
        if not organization:
            return []

        pattern = f"supertable:{organization}:*:meta:root"
        items = _get_redis_items(pattern)

        supers: List[str] = []
        for item in items:
            parts = str(item).split(":")
            # expected: supertable:{org}:{sup}:meta:root  => sup at index 2
            if len(parts) >= 3:
                supers.append(parts[2])

        return sorted({s for s in supers if s})

    # ------------------------------ Page: /reflection/tables ------------------------------

    @router.get("/reflection/tables", response_class=HTMLResponse)
    def tables_page(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=5, le=500),
    ):
        """
        Tables view: shows Redis leaves as logical tables.
        """
        if not is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            no_store(resp)
            return resp

        provided = (get_provided_token(request) or "").strip()

        # same tenant selection UX as /admin
        pairs = discover_pairs()
        sel_org, sel_sup = resolve_pair(org, sup)
        tenants = [{"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)} for o, s in pairs]

        has_tenant = bool(sel_org and sel_sup)
        total = 0
        items: List[Dict[str, Any]] = []
        root_version: Optional[int] = None
        root_ts: Optional[str] = None
        pages = 1

        if has_tenant:
            listing = list_leaves(org=sel_org, sup=sel_sup, q=None, page=page, page_size=page_size)
            try:
                total = int(listing.get("total", 0) or 0)
            except Exception:
                total = 0
            try:
                page = int(listing.get("page", page) or page)
            except Exception:
                pass
            try:
                page_size = int(listing.get("page_size", page_size) or page_size)
            except Exception:
                pass

            raw_items = listing.get("items") or []
            for it in raw_items:
                ts_val = it.get("ts")
                if isinstance(ts_val, (int, float)):
                    ts_iso = fmt_ts(int(ts_val))
                else:
                    ts_iso = str(ts_val) if ts_val is not None else ""
                new_it = dict(it)
                new_it["ts_iso"] = ts_iso
                items.append(new_it)

            pages = max(1, (total + page_size - 1) // page_size)

            try:
                root = catalog.get_root(sel_org, sel_sup)
                if isinstance(root, dict) and root:
                    rv = root.get("version")
                    root_version = int(rv) if rv is not None else None
                    ts = root.get("ts", 0)
                    root_ts = fmt_ts(int(ts)) if isinstance(ts, (int, float)) else str(ts)
            except Exception:
                root_version = None
                root_ts = None

        # Used by tables.html to default USER_HASH for meta/schema/stats calls
        users = list_users(sel_org, sel_sup) if has_tenant else []
        default_user_hash = users[0]["hash"] if users else ""

        ctx: Dict[str, Any] = {
            "request": request,
            "authorized": True,
            "token": provided,
            "tenants": tenants,
            "sel_org": sel_org,
            "sel_sup": sel_sup,
            "has_tenant": has_tenant,
            "total": total,
            "page": page,
            "pages": pages,
            "items": items,
            "root_version": root_version,
            "root_ts": root_ts,
            "default_user_hash": default_user_hash,
        }
        inject_session_into_ctx(ctx, request)
        resp = templates.TemplateResponse("tables.html", ctx)
        no_store(resp)
        return resp

    # ------------------------------ JSON endpoints used by tables.html ------------------------------

    @router.get("/reflection/supers")
    def api_list_supers(
        request: Request,
        organization: str = Query(..., description="Organization identifier"),
        _: Any = Depends(admin_guard_api),
    ):
        # tables.html calls this on load; keep UX-friendly redirect to login if session expired
        if not is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            no_store(resp)
            return resp

        try:
            return {"ok": True, "organization": organization, "supers": list_supers(organization=organization)}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"List supers failed: {e}")

    @router.get("/reflection/super")
    def api_get_super_meta(
        request: Request,
        organization: str = Query(...),
        super_name: str = Query(...),
        user_hash: str = Query(...),
        _: Any = Depends(admin_guard_api),
    ):
        if not is_authorized(request):
            resp = RedirectResponse("/reflection/login", status_code=302)
            no_store(resp)
            return resp

        debug_timings = (os.getenv("SUPERTABLE_DEBUG_TIMINGS") or "").strip() == "1"
        t0 = time.perf_counter()
        try:
            mr = MetaReader(organization=organization, super_name=super_name)
            t1 = time.perf_counter()
            meta = mr.get_super_meta(user_hash)
            t2 = time.perf_counter()

            payload = {"ok": True, "meta": meta}
            if not debug_timings:
                return payload

            mr_ms = (t1 - t0) * 1000.0
            get_ms = (t2 - t1) * 1000.0
            total_ms = (t2 - t0) * 1000.0

            client_host = getattr(getattr(request, "client", None), "host", None) or "-"
            logger.info(
                "[timing][reflection/super] total_ms=%.2f mr_ms=%.2f get_super_meta_ms=%.2f org=%s super=%s user_hash=%s client=%s",
                total_ms,
                mr_ms,
                get_ms,
                organization,
                super_name,
                (user_hash or "")[:12],
                client_host,
            )

            resp = JSONResponse(payload)
            resp.headers["Server-Timing"] = (
                f"meta_reader;dur={mr_ms:.2f},get_super_meta;dur={get_ms:.2f},total;dur={total_ms:.2f}"
            )
            return resp
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Get super meta failed: {e}")

    @router.get("/reflection/schema")
    def api_get_table_schema(
        organization: str = Query(...),
        super_name: str = Query(...),
        table: str = Query(..., description="Table simple name"),
        user_hash: str = Query(...),
        _: Any = Depends(admin_guard_api),
    ):
        """Return schema for one table (by simple name)."""
        try:
            mr = MetaReader(organization=organization, super_name=super_name)
            schema = mr.get_table_schema(table, user_hash)
            return {"ok": True, "schema": schema}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Get table schema failed: {e}")

    @router.get("/reflection/stats")
    def api_get_table_stats(
        organization: str = Query(...),
        super_name: str = Query(...),
        table: str = Query(..., description="Table simple name"),
        user_hash: str = Query(...),
        _: Any = Depends(admin_guard_api),
    ):
        """Return stats for one table (by simple name)."""
        try:
            mr = MetaReader(organization=organization, super_name=super_name)
            stats = mr.get_table_stats(table, user_hash)
            return {"ok": True, "stats": stats}
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Get table stats failed: {e}")

    @router.delete("/reflection/table")
    def api_delete_table(
        request: Request,
        organization: str = Query(..., description="Organization identifier"),
        super_name: str = Query(..., description="SuperTable name"),
        table: Optional[str] = Query(None, description="Simple table name (preferred query param: table)"),
        table_name: Optional[str] = Query(None, description="Simple table name (compat)"),
        _: Any = Depends(admin_guard_api),
    ):
        """Delete a simple table (leaf) from Redis and its folder from storage (destructive)."""
        simple = (table or table_name or "").strip()
        if not organization or not super_name or not simple:
            raise HTTPException(status_code=400, detail="organization, super_name and table are required")

        storage = get_storage()
        rcatalog = RedisCatalog()

        simple_folder = os.path.join(organization, super_name, "tables", simple)

        # Delete storage first; if it fails (other than missing), do not remove Redis meta.
        try:
            if storage.exists(simple_folder):
                storage.delete(simple_folder)
        except FileNotFoundError:
            pass
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Storage delete failed: {e}")

        try:
            rcatalog.delete_simple_table(organization, super_name, simple)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Redis delete failed: {e}")

        return {"ok": True, "organization": organization, "super_name": super_name, "table": simple}
