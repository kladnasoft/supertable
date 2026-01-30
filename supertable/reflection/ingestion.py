from __future__ import annotations

import os
import time
from typing import Any, Callable, Dict, List, Optional, Pattern, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse


def attach_ingestion_routes(
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
    STAGING_NAME_RE: Pattern[str],
    redis_list_stagings: Callable[[str, str], List[str]],
    redis_get_staging_meta: Callable[[str, str, str], Optional[Dict[str, Any]]],
    redis_list_pipes: Callable[[str, str, str], List[str]],
    pipe_key: Callable[[str, str, str, str], str],
    redis_json_load: Callable[[Any], Any],
    redis_client: Any,
    redis_get_pipe_meta: Callable[[str, str, str, str], Optional[Dict[str, Any]]],
    staging_base_dir: Callable[[str, str], str],
    get_staging_names: Callable[[Any, str, str], List[str]],
    write_json_atomic: Callable[[Any, str, Any], None],
    staging_index_path: Callable[[str, str], str],
    load_pipe_index: Callable[[Any, str, str], List[Dict[str, Any]]],
    pipe_index_path: Callable[[str, str], str],
    redis_upsert_staging_meta: Callable[[str, str, str, Dict[str, Any]], None],
    redis_delete_staging_cascade: Callable[[str, str, str], None],
    read_json_if_exists: Callable[[Any, str], Any],
    redis_upsert_pipe_meta: Callable[[str, str, str, str, Dict[str, Any]], None],
    redis_delete_pipe_meta: Callable[[str, str, str, str], None],
    get_storage: Callable[[], Any],
) -> None:
    """Register Ingestion UI + API routes onto an existing router.

    This keeps `ui.py` smaller without changing runtime behavior or paths.
    """

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
        org: str = Query(...),
        sup: str = Query(...),
        user_hash: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        """List table (simple) names for autocomplete.

        Uses MetaReader super metadata as the source of truth.
        """
        u = (user_hash or "").strip()
        if not u:
            raise HTTPException(status_code=400, detail="Missing user_hash")
        try:
            from supertable.meta_reader import MetaReader  # noqa: WPS433
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MetaReader import failed: {e}")

        try:
            mr = MetaReader(organization=org, super_name=sup)
            meta = mr.get_super_meta(u)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"List tables failed: {e}")

        tables: List[str] = []
        if isinstance(meta, dict):
            # common shapes: {"tables": [...]} or {"meta": {"tables": [...]}}
            raw = meta.get("tables")
            if raw is None and isinstance(meta.get("meta"), dict):
                raw = meta["meta"].get("tables")
            if isinstance(raw, dict):
                raw = raw.keys()
            if isinstance(raw, (list, tuple, set)):
                tables = [str(x) for x in raw if str(x).strip()]
        tables = sorted(set(tables))
        return {"tables": tables}


    @router.get("/reflection/ingestion/staging/meta")
    def api_ingestion_get_staging_meta(
        org: str = Query(...),
        sup: str = Query(...),
        staging_name: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
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
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
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
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")

        pipe_names = redis_list_pipes(org, sup, staging_name)
        if not pipe_names:
            return {"items": []}

        keys = [pipe_key(org, sup, staging_name, pn) for pn in pipe_names]
        try:
            pl = redis_client.pipeline()
            for k in keys:
                pl.get(k)
            raws = pl.execute()
        except Exception:
            raws = [redis_client.get(k) for k in keys]

        items: List[Dict[str, Any]] = []
        for pn, raw in zip(pipe_names, raws):
            meta = redis_json_load(raw) or {}
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
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
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
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
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
        names = get_staging_names(storage, org, sup)
        if staging_name not in names:
            names = sorted(set(names + [staging_name]))
            write_json_atomic(storage, staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

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
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")

        storage = get_storage()
        target = os.path.join(staging_base_dir(org, sup), staging_name)

        try:
            if storage.exists(target):
                storage.delete(target)
        except FileNotFoundError:
            pass
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Delete staging failed: {e}")

        # Update indices
        names = get_staging_names(storage, org, sup)
        if staging_name in names:
            names = [n for n in names if n != staging_name]
            write_json_atomic(storage, staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

        pipes = load_pipe_index(storage, org, sup)
        if pipes:
            pipes2 = [p for p in pipes if str(p.get("staging_name") or "") != staging_name]
            if pipes2 != pipes:
                write_json_atomic(storage, pipe_index_path(org, sup), {"pipes": pipes2, "updated_at_ns": time.time_ns()})

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
    # legacy duplicate decorator removed (was: router.post("/pipes/save"))
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
        if not STAGING_NAME_RE.fullmatch(staging_name):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not STAGING_NAME_RE.fullmatch(pipe_name):
            raise HTTPException(status_code=400, detail="Invalid pipe_name")

        storage = get_storage()
        stg_dir = os.path.join(staging_base_dir(org, sup), staging_name)
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
            write_json_atomic(storage, pipe_path, pipe_def)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Save pipe failed: {e}")

        # Ensure indices remain consistent for legacy pages.
        names = get_staging_names(storage, org, sup)
        if staging_name not in names:
            names = sorted(set(names + [staging_name]))
            write_json_atomic(storage, staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

        pipes = load_pipe_index(storage, org, sup)
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
                "user_hash": str(pipe_def.get("user_hash") or "").strip(),
                "simple_name": str(pipe_def.get("simple_name") or "").strip(),
                "overwrite_columns": overwrite_cols,
                "enabled": bool(pipe_def.get("enabled")),
                "path": pipe_path,
                "updated_at_ns": time.time_ns(),
            }
        )
        write_json_atomic(storage, pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

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
        user_hash: str = Query(...),
        simple_name: str = Query(...),
        overwrite_columns: Optional[str] = Query(None),
        enabled: bool = Query(True),
        _: Any = Depends(logged_in_guard_api),
    ):
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid pipe_name")

        from supertable.super_pipe import SuperPipe  # noqa: WPS433

        overwrite_cols = _parse_overwrite_columns(overwrite_columns)

        try:
            pipe = SuperPipe(organization=org, super_name=sup, staging_name=staging_name)
            path = pipe.create(
                pipe_name=pipe_name.strip(),
                user_hash=user_hash.strip(),
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
        names = get_staging_names(storage, org, sup)
        if staging_name not in names:
            names = sorted(set(names + [staging_name]))
            write_json_atomic(storage, staging_index_path(org, sup), {"staging_names": names, "updated_at_ns": time.time_ns()})

        # update pipe index
        pipes = load_pipe_index(storage, org, sup)
        pipes = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name.strip())]
        pipes.append(
            {
                "pipe_name": pipe_name.strip(),
                "organization": org,
                "super_name": sup,
                "staging_name": staging_name,
                "user_hash": user_hash.strip(),
                "simple_name": simple_name.strip(),
                "overwrite_columns": overwrite_cols,
                "enabled": bool(enabled),
                "path": path,
                "updated_at_ns": time.time_ns(),
            }
        )
        write_json_atomic(storage, pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

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
                "user_hash": user_hash.strip(),
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
        if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid staging_name")
        if not STAGING_NAME_RE.fullmatch((pipe_name or "").strip()):
            raise HTTPException(status_code=400, detail="Invalid pipe_name")

        storage = get_storage()
        path = os.path.join(staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")

        try:
            if storage.exists(path):
                storage.delete(path)
        except FileNotFoundError:
            pass
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Delete pipe failed: {e}")

        pipes = load_pipe_index(storage, org, sup)
        if pipes:
            pipes2 = [p for p in pipes if not (p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name.strip())]
            if pipes2 != pipes:
                write_json_atomic(storage, pipe_index_path(org, sup), {"pipes": pipes2, "updated_at_ns": time.time_ns()})

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
        pipes = load_pipe_index(storage, org, sup)
        if pipes:
            for p in pipes:
                if p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name:
                    p["enabled"] = True
                    p["updated_at_ns"] = time.time_ns()
            write_json_atomic(storage, pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

        p_path = os.path.join(staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")
        meta = read_json_if_exists(storage, p_path) or (redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {})
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
        pipes = load_pipe_index(storage, org, sup)
        if pipes:
            for p in pipes:
                if p.get("staging_name") == staging_name and p.get("pipe_name") == pipe_name:
                    p["enabled"] = False
                    p["updated_at_ns"] = time.time_ns()
            write_json_atomic(storage, pipe_index_path(org, sup), {"pipes": pipes, "updated_at_ns": time.time_ns()})

        p_path = os.path.join(staging_base_dir(org, sup), staging_name, "pipes", f"{pipe_name.strip()}.json")
        meta = read_json_if_exists(storage, p_path) or (redis_get_pipe_meta(org, sup, staging_name, pipe_name) or {})
        if isinstance(meta, dict):
            meta["enabled"] = False
        redis_upsert_pipe_meta(org, sup, staging_name, pipe_name, meta if isinstance(meta, dict) else {})
        return {"ok": True}