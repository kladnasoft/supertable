# path: supertable/reflection/ingestion.py
from __future__ import annotations

import json
import os
import re
import time
import asyncio
import tempfile
import uuid
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, File, Form, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from supertable.storage.storage_factory import get_storage

# Prefer installed package; fallback to local module for dev
try:
    from supertable.meta_reader import MetaReader  # type: ignore
except Exception:  # pragma: no cover
    from meta_reader import MetaReader  # type: ignore

logger = logging.getLogger(__name__)


# ------------------------------ Staging / Pipes helpers ------------------------------

_STAGING_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


def _staging_base_dir(org: str, sup: str) -> str:
    return os.path.join(org, sup, "staging")


def _staging_index_path(org: str, sup: str) -> str:
    return os.path.join(_staging_base_dir(org, sup), "_staging.json")


def _pipe_index_path(org: str, sup: str) -> str:
    return os.path.join(_staging_base_dir(org, sup), "_pipe.json")


def _staging_key(org: str, sup: str, staging_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}"


def _staging_index_key(org: str, sup: str) -> str:
    # Index of staging names for website/UI listing.
    return f"supertable:{org}:{sup}:meta:staging:meta"


def _pipe_key(org: str, sup: str, staging_name: str, pipe_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:{pipe_name}"


def _pipe_index_key(org: str, sup: str, staging_name: str) -> str:
    # Index of pipe names for a given staging for website/UI listing.
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:meta"


def _redis_json_load(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
        return {"value": obj}
    except Exception:
        return {"value": raw}


def _read_json_if_exists(storage: Any, path: str) -> Optional[Dict[str, Any]]:
    try:
        if not storage.exists(path):
            return None
    except Exception:
        return None
    try:
        data = storage.read_json(path)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _write_json_atomic(storage: Any, path: str, data: Dict[str, Any]) -> None:
    # Storage interface is expected to handle overwrite atomically enough for our UI use.
    base = os.path.dirname(path)
    try:
        if base and not storage.exists(base):
            storage.makedirs(base)
    except Exception:
        # best effort; write_json might still create prefixes on object stores
        pass
    storage.write_json(path, data)


def _flatten_tree_leaves(prefix: str, node: Any) -> List[str]:
    out: List[str] = []

    def dfs(p: str, n: Any) -> None:
        if n is None:
            out.append(p)
            return
        if not isinstance(n, dict):
            return
        for k, v in n.items():
            dfs(os.path.join(p, k), v)

    dfs(prefix, node)
    return out


def _scan_staging_names(storage: Any, org: str, sup: str) -> List[str]:
    base = _staging_base_dir(org, sup)
    try:
        if not storage.exists(base):
            return []
        tree = storage.get_directory_structure(base)
    except Exception:
        return []
    if not isinstance(tree, dict):
        return []
    names: List[str] = []
    for name, child in tree.items():
        if not isinstance(name, str):
            continue
        if name.startswith("_"):
            continue
        # staging folders appear as dict nodes
        if isinstance(child, dict):
            names.append(name)
    return sorted(set(names))


def _get_staging_names(storage: Any, org: str, sup: str) -> List[str]:
    idx_path = _staging_index_path(org, sup)
    data = _read_json_if_exists(storage, idx_path)
    names: List[str] = []
    if data:
        raw = data.get("staging_names")
        if isinstance(raw, list):
            names = [str(x) for x in raw if isinstance(x, (str, int, float))]
    # Merge with a best-effort scan in case index is missing/stale.
    scanned = _scan_staging_names(storage, org, sup)
    merged = sorted(set(names) | set(scanned))
    if merged and (not data or sorted(set(names)) != merged):
        _write_json_atomic(storage, idx_path, {"staging_names": merged, "updated_at_ns": time.time_ns()})
    return merged


def _load_pipe_index(storage: Any, org: str, sup: str) -> List[Dict[str, Any]]:
    idx_path = _pipe_index_path(org, sup)
    data = _read_json_if_exists(storage, idx_path)
    pipes: List[Dict[str, Any]] = []
    if data:
        raw = data.get("pipes")
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict) and item.get("pipe_name") and item.get("staging_name"):
                    pipes.append(item)
    return pipes


def _scan_pipes(storage: Any, org: str, sup: str, staging_names: List[str]) -> List[Dict[str, Any]]:
    pipes: List[Dict[str, Any]] = []
    from supertable.super_pipe import SuperPipe  # noqa: WPS433

    for stg in staging_names:
        try:
            sp = SuperPipe(organization=org, super_name=sup, staging_name=stg)
        except Exception:
            continue
        try:
            tree = storage.get_directory_structure(os.path.join(_staging_base_dir(org, sup), stg, "pipes"))
        except Exception:
            continue
        for path in _flatten_tree_leaves(os.path.join(_staging_base_dir(org, sup), stg, "pipes"), tree):
            if not path.endswith(".json"):
                continue
            try:
                data = storage.read_json(path)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            # normalize minimal keys
            if not data.get("pipe_name"):
                data["pipe_name"] = os.path.splitext(os.path.basename(path))[0]
            if not data.get("staging_name"):
                data["staging_name"] = stg
            pipes.append(data)
    return pipes


def _get_pipes(storage: Any, org: str, sup: str, staging_names: List[str]) -> List[Dict[str, Any]]:
    pipes = _load_pipe_index(storage, org, sup)
    if pipes:
        # best-effort freshness: merge with scan if index seems incomplete
        scanned = _scan_pipes(storage, org, sup, staging_names)
        if scanned:
            existing_keys = {(p.get("staging_name"), p.get("pipe_name")) for p in pipes}
            for p in scanned:
                key = (p.get("staging_name"), p.get("pipe_name"))
                if key not in existing_keys:
                    pipes.append(p)
            # persist merged index
            _write_json_atomic(
                storage,
                _pipe_index_path(org, sup),
                {"pipes": pipes, "updated_at_ns": time.time_ns()},
            )
        return pipes

    scanned = _scan_pipes(storage, org, sup, staging_names)
    if scanned:
        _write_json_atomic(
            storage,
            _pipe_index_path(org, sup),
            {"pipes": scanned, "updated_at_ns": time.time_ns()},
        )
    return scanned


# ------------------------------ Redis helpers (require redis_client) ------------------------------

def _make_redis_helpers(redis_client: Any) -> Dict[str, Any]:
    """Build redis helper closures bound to the given redis_client instance."""

    def redis_get_staging_meta(org: str, sup: str, staging_name: str) -> Optional[Dict[str, Any]]:
        try:
            return _redis_json_load(redis_client.get(_staging_key(org, sup, staging_name)))
        except Exception:
            return None

    def redis_get_pipe_meta(org: str, sup: str, staging_name: str, pipe_name: str) -> Optional[Dict[str, Any]]:
        try:
            return _redis_json_load(redis_client.get(_pipe_key(org, sup, staging_name, pipe_name)))
        except Exception:
            return None

    def redis_list_stagings(org: str, sup: str) -> List[str]:
        idx = _staging_index_key(org, sup)
        try:
            names = redis_client.smembers(idx) or set()
            if names:
                return sorted({str(x) for x in names if str(x).strip() and str(x) != "meta"})
        except Exception:
            pass

        # Fallback for older data: scan for staging meta keys.
        pattern = f"supertable:{org}:{sup}:meta:staging:*"
        cursor = 0
        out: Set[str] = set()
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                tail = k.rsplit("meta:staging:", 1)[-1]
                if not tail or tail == "meta":
                    continue
                # staging meta key has no further ":" segments (pipe keys do)
                if ":" in tail:
                    continue
                out.add(tail)
            if cursor == 0:
                break
        return sorted(out)

    def redis_list_pipes(org: str, sup: str, staging_name: str) -> List[str]:
        idx = _pipe_index_key(org, sup, staging_name)
        try:
            names = redis_client.smembers(idx) or set()
            if names:
                return sorted({str(x) for x in names if str(x).strip() and str(x) != "meta"})
        except Exception:
            pass

        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:*"
        cursor = 0
        out: Set[str] = set()
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                tail = k.rsplit(":pipe:", 1)[-1]
                if not tail or tail == "meta":
                    continue
                if ":" in tail:
                    continue
                out.add(tail)
            if cursor == 0:
                break
        return sorted(out)

    def redis_upsert_staging_meta(org: str, sup: str, staging_name: str, meta: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload["updated_at_ms"] = int(time.time() * 1000)

        try:
            redis_client.set(_staging_key(org, sup, staging_name), json.dumps(payload, ensure_ascii=False))
            redis_client.sadd(_staging_index_key(org, sup), staging_name)
        except Exception:
            # UI should not fail hard if Redis is unavailable
            pass

    def redis_upsert_pipe_meta(
        org: str,
        sup: str,
        staging_name: str,
        pipe_name: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload.setdefault("pipe_name", pipe_name)
        payload["updated_at_ms"] = int(time.time() * 1000)

        try:
            redis_client.set(_pipe_key(org, sup, staging_name, pipe_name), json.dumps(payload, ensure_ascii=False))
            redis_client.sadd(_pipe_index_key(org, sup, staging_name), pipe_name)
            redis_client.sadd(_staging_index_key(org, sup), staging_name)
        except Exception:
            pass

    def redis_delete_pipe_meta(org: str, sup: str, staging_name: str, pipe_name: str) -> None:
        try:
            redis_client.srem(_pipe_index_key(org, sup, staging_name), pipe_name)
        except Exception:
            pass
        try:
            redis_client.delete(_pipe_key(org, sup, staging_name, pipe_name))
        except Exception:
            pass

    def redis_delete_staging_cascade(org: str, sup: str, staging_name: str) -> None:
        # Remove from index + delete the base meta key.
        try:
            redis_client.srem(_staging_index_key(org, sup), staging_name)
        except Exception:
            pass
        try:
            redis_client.delete(_staging_key(org, sup, staging_name))
        except Exception:
            pass

        # Delete everything under the staging namespace (pipes + indices + any future keys).
        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                try:
                    pipe = redis_client.pipeline()
                    for k in keys:
                        pipe.delete(k)
                    pipe.execute()
                except Exception:
                    # best effort
                    pass
            if cursor == 0:
                break

        try:
            redis_client.delete(_pipe_index_key(org, sup, staging_name))
        except Exception:
            pass

    return {
        "redis_get_staging_meta": redis_get_staging_meta,
        "redis_get_pipe_meta": redis_get_pipe_meta,
        "redis_list_stagings": redis_list_stagings,
        "redis_list_pipes": redis_list_pipes,
        "redis_upsert_staging_meta": redis_upsert_staging_meta,
        "redis_upsert_pipe_meta": redis_upsert_pipe_meta,
        "redis_delete_pipe_meta": redis_delete_pipe_meta,
        "redis_delete_staging_cascade": redis_delete_staging_cascade,
    }


# ------------------------------ Route attachment ------------------------------

def attach_ingestion_routes(
    router: APIRouter,
    *,
    templates: Any,
    is_authorized: Callable[[Request], bool],
    no_store: Callable[[Any], None],
    get_provided_token: Callable[[Request], Optional[str]],
    discover_pairs: Callable[[], list],
    resolve_pair: Callable[[Optional[str], Optional[str]], Tuple[Optional[str], Optional[str]]],
    inject_session_into_ctx: Callable[[Dict[str, Any], Request], None],
    get_session: Callable[[Request], Optional[Dict[str, Any]]],
    is_superuser: Callable[[Request], bool],
    logged_in_guard_api: Any,
    admin_guard_api: Any,
    redis_client: Any,
) -> None:
    """Register Ingestion UI + API routes onto an existing router.

    All staging/pipe helpers are defined in this module; only shared
    auth/session/template objects are injected from common.py.
    """

    # Build redis helper closures bound to the injected redis_client.
    rh = _make_redis_helpers(redis_client)
    redis_list_stagings = rh["redis_list_stagings"]
    redis_get_staging_meta = rh["redis_get_staging_meta"]
    redis_list_pipes = rh["redis_list_pipes"]
    redis_get_pipe_meta = rh["redis_get_pipe_meta"]
    redis_upsert_staging_meta = rh["redis_upsert_staging_meta"]
    redis_upsert_pipe_meta = rh["redis_upsert_pipe_meta"]
    redis_delete_pipe_meta = rh["redis_delete_pipe_meta"]
    redis_delete_staging_cascade = rh["redis_delete_staging_cascade"]

    # ---------------------------- Ingestion page ----------------------------

    @router.get("/reflection/ingestion", response_class=HTMLResponse)
    def ingestion_page(
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
        }
        inject_session_into_ctx(ctx, request)

        resp = templates.TemplateResponse("ingestion.html", ctx)
        no_store(resp)
        return resp

    # ---------------------------- Staging page ----------------------------

    @router.get("/reflection/staging", response_class=HTMLResponse)
    def staging_page(
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
            "staging_names": [],
            "staging_folders": [],
            "pipes_by_staging": {},
            "pipes_flat": [],
        }
        inject_session_into_ctx(ctx, request)

        if not sel_org or not sel_sup:
            resp = templates.TemplateResponse("staging.html", ctx)
            no_store(resp)
            return resp

        storage = get_storage()
        staging_names = _get_staging_names(storage, sel_org, sel_sup)
        pipes = _get_pipes(storage, sel_org, sel_sup, staging_names)

        # group pipes by staging
        by_staging: Dict[str, List[Dict[str, Any]]] = {}
        for p in pipes:
            stg = str(p.get("staging_name") or "")
            if not stg:
                continue
            by_staging.setdefault(stg, []).append(p)
        for stg, arr in by_staging.items():
            arr.sort(key=lambda x: str(x.get("pipe_name") or ""))

        # staging folders + files (best-effort)
        folders: List[Dict[str, Any]] = []
        base = _staging_base_dir(sel_org, sel_sup)
        for stg in staging_names:
            files_dir = os.path.join(base, stg, "files")
            files: List[str] = []
            try:
                if storage.exists(files_dir):
                    tree = storage.get_directory_structure(files_dir)
                    leaves = _flatten_tree_leaves(files_dir, tree)
                    # make paths relative to files_dir
                    files = [os.path.relpath(p, files_dir) for p in leaves if p.endswith(".parquet")]
                    files.sort()
            except Exception:
                files = []
            folders.append({"name": stg, "files": files})

        ctx.update(
            {
                "staging_names": staging_names,
                "staging_folders": folders,
                "pipes_by_staging": by_staging,
                "pipes_flat": sorted(
                    pipes,
                    key=lambda x: (str(x.get("staging_name") or ""), str(x.get("pipe_name") or "")),
                ),
            }
        )

        resp = templates.TemplateResponse("staging.html", ctx)
        no_store(resp)
        return resp

    # ----------------------------- Ingestion APIs ----------------------------

    @router.get("/reflection/ingestion/stagings")
    def api_ingestion_list_stagings(
        org: str = Query(...),
        sup: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        names = redis_list_stagings(org, sup)
        return {"staging_names": names}

    @router.get("/reflection/ingestion/tables")
    def api_ingestion_list_tables(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
        organization: Optional[str] = Query(None),
        super_name: Optional[str] = Query(None),
        role_name: Optional[str] = Query(None),
        _: Any = Depends(logged_in_guard_api),
    ):
        """Return table names for ingestion UI autocomplete.

        Accepts both (org, sup) and (organization, super_name) query params for compatibility.
        """
        org_val = (org or organization or "").strip()
        sup_val = (sup or super_name or "").strip()
        role = (role_name or "").strip()

        if not org_val or not sup_val or not role:
            return {"ok": True, "tables": []}

        # Prevent using another user's role unless the requester is a superuser.
        sess = get_session(request) or {}
        sess_role = (sess.get("role_name") or "").strip()
        if sess_role and sess_role != role and not is_superuser(request):
            raise HTTPException(status_code=403, detail="Forbidden")

        try:
            mr = MetaReader(organization=org_val, super_name=sup_val)
            meta = mr.get_super_meta(role) or {}
        except Exception as e:
            logger.warning("Failed to fetch super meta for ingestion tables (%s/%s): %s", org_val, sup_val, e)
            return {"ok": True, "tables": []}

        super_meta = meta.get("super", {}) if isinstance(meta, dict) else {}
        raw_tables = []
        try:
            raw_tables = super_meta.get("tables") if isinstance(super_meta, dict) else None
        except Exception:
            raw_tables = None

        names: List[str] = []
        if isinstance(raw_tables, list):
            for t in raw_tables:
                if isinstance(t, str):
                    name = t.strip()
                elif isinstance(t, dict):
                    name = str(t.get("name") or "").strip()
                else:
                    name = str(t or "").strip()
                if name and name not in names:
                    names.append(name)

        return {"ok": True, "tables": names}

    @router.get("/reflection/ingestion/staging/meta")
    def api_ingestion_get_staging_meta(
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        meta = redis_get_staging_meta(org, sup, staging_name) or {}
        return {"meta": meta}

    @router.get("/reflection/ingestion/staging/files")
    def api_ingestion_list_staging_files(
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        offset: int = Query(0, ge=0),
        limit: int = Query(50, ge=1, le=500),
        _: Any = Depends(logged_in_guard_api),
    ):
        """Paged read of staging/{staging_name}_files.json for UI (lazy-load)."""
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")

        storage = get_storage()
        base_dir = os.path.join(org, sup, "staging")
        index_path = os.path.join(base_dir, f"{staging_name}_files.json")

        if not storage.exists(index_path):
            return {"items": [], "total": 0, "offset": offset, "limit": limit}

        data = storage.read_json(index_path) or []
        if not isinstance(data, list):
            data = []

        total = len(data)
        items = data[offset : offset + limit]
        return {"items": items, "total": total, "offset": offset, "limit": limit}

    @router.get("/reflection/ingestion/pipes")
    def api_ingestion_list_pipes(
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")

        pipe_names = redis_list_pipes(org, sup, staging_name)
        if not pipe_names:
            return {"items": []}

        keys = [_pipe_key(org, sup, staging_name, pn) for pn in pipe_names]
        try:
            pl = redis_client.pipeline()
            for k in keys:
                pl.get(k)
            raws = pl.execute()
        except Exception:
            raws = [redis_client.get(k) for k in keys]

        items: List[Dict[str, Any]] = []
        for pn, raw in zip(pipe_names, raws):
            meta = _redis_json_load(raw) or {}
            items.append(
                {
                    "pipe_name": pn,
                    "simple_name": str(meta.get("simple_name") or ""),
                    "enabled": bool(meta.get("enabled")),
                }
            )

        items.sort(key=lambda x: str(x.get("pipe_name") or ""))
        return {"items": items}

    @router.get("/reflection/ingestion/pipe/meta")
    def api_ingestion_get_pipe_meta(
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        pipe_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not _STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid pipe_name")

        meta = redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {}
        return {"meta": meta}

    # ------------------------ Staging CRUD (admin) --------------------------

    @router.post("/reflection/staging/create")
    def api_create_staging(
        request: Request,
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")

        # Create staging folders (and validate super exists) via the canonical interface
        from supertable.staging_area import Staging  # noqa: WPS433

        try:
            Staging(organization=org, super_name=sup, staging_name=staging_name)  # side effects: ensure folders
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Create staging failed: {e}")

        # Persist index at super-level for fast listing (object stores don't list empty folders reliably)
        storage = get_storage()
        names = _get_staging_names(storage, org, sup)
        if staging_name not in names:
            names = sorted(set(names + [staging_name]))
            _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

        redis_upsert_staging_meta(org, sup, staging_name, {"staging_name": staging_name})
        return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name}

    @router.post("/reflection/staging/delete")
    def api_delete_staging(
        request: Request,
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")

        storage = get_storage()
        target = os.path.join(_staging_base_dir(org, sup), staging_name)

        try:
            if storage.exists(target):
                storage.delete(target)
        except FileNotFoundError:
            pass
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Delete staging failed: {e}")

        # Update indices
        names = _get_staging_names(storage, org, sup)
        if staging_name in names:
            names = [n for n in names if n != staging_name]
            _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

        pipes = _load_pipe_index(storage, org, sup)
        if pipes:
            pipes2 = [p for p in pipes if str(p.get("staging_name") or "") != staging_name]
            if pipes2 != pipes:
                _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes2, "updated_at_ns": time.time_ns()})

        redis_delete_staging_cascade(org, sup, staging_name)
        return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name}

    # -------------------------- Pipes CRUD (admin) ---------------------------

    def _parse_overwrite_columns(raw: Optional[str]) -> List[str]:
        if raw is None:
            # Default from docs/examples
            return ["day"]
        s = (raw or "").strip()
        if not s:
            return []
        cols = [c.strip() for c in s.split(",")]
        return [c for c in cols if c]

    @router.post("/reflection/pipes/save")
    def api_save_pipe(
        request: Request,
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Invalid JSON body")

        org = str(payload.get("organization") or payload.get("org") or "").strip()
        sup = str(payload.get("super_name") or payload.get("sup") or "").strip()
        staging_name = str(payload.get("staging_name") or "").strip()
        pipe_name = str(payload.get("pipe_name") or "").strip()

        if not org or not sup:
            raise HTTPException(status_code=400, detail="organization and super_name are required")
        if not _STAGING_NAME_RE.fullmatch(staging_name):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not _STAGING_NAME_RE.fullmatch(pipe_name):
            raise HTTPException(status_code=400, detail="Invalid pipe_name")

        storage = get_storage()
        stg_dir = os.path.join(_staging_base_dir(org, sup), staging_name)
        try:
            if not storage.exists(stg_dir):
                raise HTTPException(status_code=404, detail="Staging not found")
        except HTTPException:
            raise
        except Exception:
            # best-effort: proceed; object stores may not list prefixes reliably
            pass

        # Canonicalize and persist the pipe definition.
        pipe_def: Dict[str, Any] = dict(payload)
        pipe_def["organization"] = org
        pipe_def["super_name"] = sup
        pipe_def["staging_name"] = staging_name
        pipe_def["pipe_name"] = pipe_name
        pipe_def.setdefault("enabled", True)
        pipe_def.setdefault("overwrite_columns", ["day"])
        pipe_def.setdefault("meta", {})
        pipe_def["updated_at_ns"] = time.time_ns()

        pipe_path = os.path.join(stg_dir, "pipes", f"{pipe_name}.json")
        try:
            _write_json_atomic(storage, pipe_path, pipe_def)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Save pipe failed: {e}")

        # Ensure indices remain consistent for legacy pages.
        names = _get_staging_names(storage, org, sup)
        if staging_name not in names:
            names = sorted(set(names + [staging_name]))
            _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

        pipes = _load_pipe_index(storage, org, sup)
        pipes = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name)]
        overwrite_cols = pipe_def.get("overwrite_columns")
        if not isinstance(overwrite_cols, list):
            overwrite_cols = []
        pipes.append(
            {
                "pipe_name": pipe_name,
                "organization": org,
                "super_name": sup,
                "staging_name": staging_name,
                "role_name": str(pipe_def.get("role_name") or "").strip(),
                "simple_name": str(pipe_def.get("simple_name") or "").strip(),
                "overwrite_columns": overwrite_cols,
                "enabled": bool(pipe_def.get("enabled")),
                "path": pipe_path,
                "updated_at_ns": time.time_ns(),
            }
        )
        _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

        # Mirror to Redis for website/UI listing.
        redis_upsert_staging_meta(org, sup, staging_name, {"staging_name": staging_name})
        redis_upsert_pipe_meta(org, sup, staging_name, pipe_name, pipe_def)

        return {
            "ok": True,
            "organization": org,
            "super_name": sup,
            "staging_name": staging_name,
            "pipe_name": pipe_name,
            "path": pipe_path,
        }

    @router.post("/reflection/pipes/create")
    def api_create_pipe(
        request: Request,
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        pipe_name: str = Query(...),
        role_name: str = Query(""),
        simple_name: str = Query(...),
        overwrite_columns: Optional[str] = Query(None),
        enabled: bool = Query(True),
        _: Any = Depends(logged_in_guard_api),
    ):
        role = (role_name or "").strip()
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not _STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid pipe_name")

        from supertable.super_pipe import SuperPipe  # noqa: WPS433

        overwrite_cols = _parse_overwrite_columns(overwrite_columns)

        try:
            pipe = SuperPipe(organization=org, super_name=sup, staging_name=staging_name)
            path = pipe.create(
                pipe_name=pipe_name.strip(),
                user_hash=role.strip(),
                simple_name=simple_name.strip(),
                overwrite_columns=overwrite_cols,
                enabled=bool(enabled),
            )
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except FileExistsError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except ValueError as e:
            raise HTTPException(status_code=409, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Create pipe failed: {e}")

        storage = get_storage()

        # ensure staging index includes this staging (nice UX for dropdowns)
        names = _get_staging_names(storage, org, sup)
        if staging_name not in names:
            names = sorted(set(names + [staging_name]))
            _write_json_atomic(storage, _staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

        # update pipe index
        pipes = _load_pipe_index(storage, org, sup)
        pipes = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name.strip())]
        pipes.append(
            {
                "pipe_name": pipe_name.strip(),
                "organization": org,
                "super_name": sup,
                "staging_name": staging_name,
                "role_name": role.strip(),
                "simple_name": simple_name.strip(),
                "overwrite_columns": overwrite_cols,
                "enabled": bool(enabled),
                "path": path,
                "updated_at_ns": time.time_ns(),
            }
        )
        _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

        redis_upsert_pipe_meta(
            org,
            sup,
            staging_name,
            pipe_name.strip(),
            {
                "pipe_name": pipe_name.strip(),
                "organization": org,
                "super_name": sup,
                "staging_name": staging_name,
                "role_name": role.strip(),
                "simple_name": simple_name.strip(),
                "overwrite_columns": overwrite_cols,
                "enabled": bool(enabled),
                "path": path,
            },
        )
        redis_upsert_staging_meta(org, sup, staging_name, {"staging_name": staging_name})
        return {
            "ok": True,
            "organization": org,
            "super_name": sup,
            "staging_name": staging_name,
            "pipe_name": pipe_name.strip(),
            "path": path,
        }

    @router.post("/reflection/pipes/delete")
    def api_delete_pipe(
        request: Request,
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        pipe_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not _STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not _STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid pipe_name")

        storage = get_storage()
        path = os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")

        try:
            if storage.exists(path):
                storage.delete(path)
        except FileNotFoundError:
            pass
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Delete pipe failed: {e}")

        pipes = _load_pipe_index(storage, org, sup)
        if pipes:
            pipes2 = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name.strip())]
            if pipes2 != pipes:
                _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes2, "updated_at_ns": time.time_ns()})

        redis_delete_pipe_meta(org, sup, staging_name, pipe_name.strip())
        return {"ok": True, "organization": org, "super_name": sup, "staging_name": staging_name, "pipe_name": pipe_name.strip()}

    @router.post("/reflection/pipes/enable")
    def api_enable_pipe(
        request: Request,
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        pipe_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        from supertable.super_pipe import SuperPipe  # noqa: WPS433

        try:
            pipe = SuperPipe(organization=org, super_name=sup, staging_name=staging_name)
            pipe.set_enabled(pipe_name=pipe_name, enabled=True)
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Enable pipe failed: {e}")

        storage = get_storage()
        pipes = _load_pipe_index(storage, org, sup)
        if pipes:
            for p in pipes:
                if p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name:
                    p["enabled"] = True
                    p["updated_at_ns"] = time.time_ns()
            _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

        p_path = os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")
        meta = _read_json_if_exists(storage, p_path) or (redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {})
        if isinstance(meta, dict):
            meta["enabled"] = True
        redis_upsert_pipe_meta(org, sup, staging_name, pipe_name, meta if isinstance(meta, dict) else {})
        return {"ok": True}

    @router.post("/reflection/pipes/disable")
    def api_disable_pipe(
        request: Request,
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        pipe_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        from supertable.super_pipe import SuperPipe  # noqa: WPS433

        try:
            pipe = SuperPipe(organization=org, super_name=sup, staging_name=staging_name)
            pipe.set_enabled(pipe_name=pipe_name, enabled=False)
        except FileNotFoundError as e:
            raise HTTPException(status_code=404, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Disable pipe failed: {e}")

        storage = get_storage()
        pipes = _load_pipe_index(storage, org, sup)
        if pipes:
            for p in pipes:
                if p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name:
                    p["enabled"] = False
                    p["updated_at_ns"] = time.time_ns()
            _write_json_atomic(storage, _pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

        p_path = os.path.join(_staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")
        meta = _read_json_if_exists(storage, p_path) or (redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {})
        if isinstance(meta, dict):
            meta["enabled"] = False
        redis_upsert_pipe_meta(org, sup, staging_name, pipe_name, meta if isinstance(meta, dict) else {})
        return {"ok": True}

    # ----------------------------- Load (file upload) -----------------------------

    @router.post("/reflection/ingestion/load/upload")
    async def api_ingestion_load_upload(
        request: Request,
        org: str = Form(...),
        sup: str = Form(...),
        role_name: str = Form(""),
        mode: str = Form(...),  # "table" | "staging"
        table_name: Optional[str] = Form(None),
        staging_name: Optional[str] = Form(None),
        overwrite_columns: Optional[str] = Form(None),
        file: UploadFile = File(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        """Upload a file and load it either into a table or into a staging area.

        The UI always posts to this endpoint; behavior is selected by `mode`:
          - mode="staging": use `supertable.staging_area.Staging.save_as_parquet`
          - mode="table": use `supertable.data_writer.DataWriter.write`
        """
        t0 = time.perf_counter()

        org_eff = (org or "").strip()
        sup_eff = (sup or "").strip()
        if not org_eff or not sup_eff:
            raise HTTPException(status_code=400, detail="Missing org or sup")

        # Resolve role_name from form field or session
        sess = None
        try:
            sess = get_session(request) or {}
        except Exception:
            sess = None

        sess_org = str((sess or {}).get("org") or "")
        sess_role = str((sess or {}).get("role_name") or "")
        if sess_org and sess_org != org_eff:
            raise HTTPException(status_code=403, detail="Forbidden")

        role_eff = (role_name or sess_role or "").strip()
        if not role_eff:
            raise HTTPException(status_code=400, detail="Missing role_name")

        mode_eff = (mode or "").strip().lower()
        if mode_eff not in ("table", "staging"):
            raise HTTPException(status_code=400, detail="Invalid mode (expected 'table' or 'staging')")

        if mode_eff == "table":
            table_eff = (table_name or "").strip()
            if not table_eff:
                raise HTTPException(status_code=400, detail="Missing table_name")
            # Avoid path traversal / weird separators in table names.
            if any(x in table_eff for x in ("/", "\\", "\x00")) or ".." in table_eff:
                raise HTTPException(status_code=400, detail="Invalid table_name")
        else:
            stg_eff = (staging_name or "").strip()
            if not stg_eff:
                raise HTTPException(status_code=400, detail="Missing staging_name")
            if not _STAGING_NAME_RE.match(stg_eff):
                raise HTTPException(status_code=400, detail="Invalid staging_name")

        filename = (file.filename or "upload").strip()
        ext = Path(filename).suffix.lower().lstrip(".")
        if ext in ("", None):
            ext = ""

        # Save upload to a temp file (avoid large in-memory buffers).
        def _persist_upload_to_temp(upload: UploadFile) -> str:
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=(".%s" % ext) if ext else "")
            tmp_path = tmp.name
            try:
                with tmp:
                    while True:
                        chunk = upload.file.read(1024 * 1024)
                        if not chunk:
                            break
                        tmp.write(chunk)
            finally:
                try:
                    upload.file.close()
                except Exception:
                    pass
            return tmp_path

        tmp_path = await asyncio.to_thread(_persist_upload_to_temp, file)

        def _read_arrow_table(path: str, file_ext: str) -> Any:
            try:
                import pyarrow as pa  # noqa: WPS433
                import pyarrow.csv as pa_csv  # noqa: WPS433
                import pyarrow.json as pa_json  # noqa: WPS433
                import pyarrow.parquet as pa_parquet  # noqa: WPS433
            except Exception as e:
                raise RuntimeError(f"pyarrow import failed: {e}")

            e = (file_ext or "").lower()
            if e in ("csv", "tsv"):
                delimiter = "\t" if e == "tsv" else ","
                read_opts = pa_csv.ReadOptions(autogenerate_column_names=False)
                parse_opts = pa_csv.ParseOptions(delimiter=delimiter)
                convert_opts = pa_csv.ConvertOptions(strings_can_be_null=True)
                return pa_csv.read_csv(path, read_options=read_opts, parse_options=parse_opts, convert_options=convert_opts)

            if e in ("parquet", "pq"):
                return pa_parquet.read_table(path)

            if e in ("jsonl", "ndjson"):
                return pa_json.read_json(path)

            if e == "json":
                # Try pyarrow JSON first (works for JSON-lines).
                try:
                    return pa_json.read_json(path)
                except Exception:
                    # Fallback: attempt to load a JSON array of objects.
                    import json as json_mod  # noqa: WPS433
                    with open(path, "r", encoding="utf-8") as fh:
                        data = json_mod.load(fh)
                    if isinstance(data, list):
                        return pa.Table.from_pylist(data)
                    raise

            raise RuntimeError("Unsupported file type. Supported: csv, tsv, json/jsonl, parquet")

        try:
            arrow_table = await asyncio.to_thread(_read_arrow_table, tmp_path, ext)
        except HTTPException:
            raise
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to parse file: {e}")
        finally:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass

        job_uuid = str(uuid.uuid4())

        if mode_eff == "staging":
            stg_eff = (staging_name or "").strip()
            base_name = Path(filename).stem or "upload"
            base_name = re.sub(r"[^A-Za-z0-9._-]+", "_", base_name).strip("._-") or "upload"

            try:
                from supertable.staging_area import Staging  # noqa: WPS433
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Staging import failed: {e}")

            def _do_stage() -> str:
                stg = Staging(organization=org_eff, super_name=sup_eff, staging_name=stg_eff)
                return stg.save_as_parquet(arrow_table=arrow_table, base_file_name=base_name)

            try:
                saved = await asyncio.to_thread(_do_stage)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Staging save failed: {e}")

            dt_ms = (time.perf_counter() - t0) * 1000.0
            rows = getattr(arrow_table, "num_rows", None)
            return {
                "ok": True,
                "mode": "staging",
                "organization": org_eff,
                "super_name": sup_eff,
                "staging_name": stg_eff,
                "saved_file_name": saved,
                "rows": rows if rows is not None else 0,
                "file_type": ext or "unknown",
                "job_uuid": job_uuid,
                "server_duration_ms": dt_ms,
            }

        # mode == "table"
        table_eff = (table_name or "").strip()
        overwrite_cols: List[str] = []
        if overwrite_columns:
            raw = str(overwrite_columns)
            overwrite_cols = [c.strip() for c in re.split(r"[\n,]+", raw) if c.strip()]

        try:
            from supertable.data_writer import DataWriter  # noqa: WPS433
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"DataWriter import failed: {e}")

        def _do_write() -> Any:
            dw = DataWriter(super_name=sup_eff, organization=org_eff)
            return dw.write(
                role_name=role_eff,
                simple_name=table_eff,
                data=arrow_table,
                overwrite_columns=overwrite_cols,
            )

        try:
            cols, rows, inserted, deleted = await asyncio.to_thread(_do_write)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Write failed: {e}")

        dt_ms = (time.perf_counter() - t0) * 1000.0
        return {
            "ok": True,
            "mode": "table",
            "organization": org_eff,
            "super_name": sup_eff,
            "table_name": table_eff,
            "rows": rows,
            "inserted": inserted,
            "deleted": deleted,
            "file_type": ext or "unknown",
            "job_uuid": job_uuid,
            "server_duration_ms": dt_ms,
        }