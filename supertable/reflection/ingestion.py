from __future__ import annotations

import os
import re
import time
from typing import Any, Callable, Dict, List, Optional, Pattern, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, File, Form, UploadFile
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
        return {"staging_names": names, "items": names}

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

    # ----------------------------- Load Data APIs ----------------------------

    @router.get("/reflection/ingestion/tables")
    def api_ingestion_list_tables(
        org: str = Query(...),
        sup: str = Query(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        """Best-effort list of existing table names for UI suggestions."""
        try:
            from supertable.super_table import SuperTable  # noqa: WPS433
        except Exception as e:  # pragma: no cover
            return {"tables": [], "warning": f"SuperTable import failed: {e}"}

        try:
            st = SuperTable(super_name=sup, organization=org)
        except Exception as e:
            return {"tables": [], "warning": f"SuperTable init failed: {e}"}

        out: List[str] = []

        # Try a handful of common APIs across versions, but never fail hard.
        for attr in (
            "list_tables",
            "list_table_names",
            "get_table_names",
            "list_simple_names",
            "tables",
        ):
            if not hasattr(st, attr):
                continue

            try:
                val = getattr(st, attr)
                res = val() if callable(val) else val
            except Exception:
                continue

            if isinstance(res, dict) and "tables" in res:
                res = res["tables"]

            if isinstance(res, (list, tuple, set)):
                out = [str(x).strip() for x in res if str(x).strip()]
                break

        out = sorted(set(out))
        return {"tables": out}
    def _validate_simple_name(val: str) -> str:
        v = (val or "").strip()
        # conservative: letters, numbers, underscore; must start with letter; 1..64 chars
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{0,63}", v):
            raise HTTPException(status_code=400, detail="Invalid table name")
        return v

    def _convert_upload_to_arrow(file_name: str, file_bytes: bytes, table_name: str):
        """Convert upload to a PyArrow table and attach system columns."""
        try:
            import uuid  # noqa: WPS433
            import pandas as pd  # noqa: WPS433
            import pyarrow as pa  # noqa: WPS433
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Missing conversion deps (pandas/pyarrow): {e}")

        raw_name = (file_name or "").lower()
        suffix = os.path.splitext(raw_name)[1]

        # Read bytes once (already buffered)
        data = file_bytes

        # Heuristics by extension
        if suffix in {".csv", ".tsv", ".txt"}:
            sep = "\t" if suffix == ".tsv" else ","
            df = pd.read_csv(pa.BufferReader(data), sep=sep, low_memory=False)
            file_type = suffix.lstrip(".") or "csv"
        elif suffix in {".json", ".jsonl", ".ndjson"}:
            # Try jsonl first if looks line-based
            text_head = data[:2048].lstrip()
            if text_head.startswith(b"{") and b"\n" in text_head and suffix in {".jsonl", ".ndjson"}:
                df = pd.read_json(pa.BufferReader(data), lines=True)
            else:
                try:
                    df = pd.read_json(pa.BufferReader(data), lines=True)
                except Exception:
                    df = pd.read_json(pa.BufferReader(data))
            file_type = "json"
        elif suffix in {".parquet"}:
            import pyarrow.parquet as pq  # noqa: WPS433

            t = pq.read_table(pa.BufferReader(data))
            file_type = "parquet"
            df = None
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type (use csv/tsv/json/jsonl/parquet)")

        job_uuid = str(uuid.uuid4())

        if df is not None:
            t = pa.Table.from_pandas(df, preserve_index=False)

        # Attach system columns
        row_ids = pa.array([str(uuid.uuid4()) for _ in range(t.num_rows)])
        job_uuids = pa.array([job_uuid] * t.num_rows)
        table_names = pa.array([table_name] * t.num_rows)

        t = t.append_column("_sys_row_id", row_ids)
        t = t.append_column("_sys_job_uuid", job_uuids)
        t = t.append_column("_sys_table_name", table_names)

        return t, job_uuid, file_type

    @router.post("/reflection/ingestion/load/upload")
    async def api_ingestion_load_upload(
        org: str = Form(...),
        sup: str = Form(...),
        user_hash: str = Form(...),
        mode: str = Form(...),
        table_name: Optional[str] = Form(None),
        staging_name: Optional[str] = Form(None),
        overwrite_columns: Optional[str] = Form(None),
        file: UploadFile = File(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        """Upload a file and load to a table or stage it."""
        t0 = time.perf_counter()

        uhash = (user_hash or "").strip()
        if not uhash:
            raise HTTPException(status_code=400, detail="Missing user_hash")

        m = (mode or "").strip().lower()
        if m not in {"table", "staging"}:
            raise HTTPException(status_code=400, detail="Invalid mode")

        if m == "table":
            if not table_name:
                raise HTTPException(status_code=400, detail="Missing table_name")
            simple_name = _validate_simple_name(table_name)
        else:
            if not staging_name:
                raise HTTPException(status_code=400, detail="Missing staging_name")
            if not STAGING_NAME_RE.fullmatch((staging_name or "").strip()):
                raise HTTPException(status_code=400, detail="Invalid staging_name")
            simple_name = _validate_simple_name(table_name or "data")

        # Convert file
        file_bytes = await file.read()
        arrow_table, job_uuid, file_type = _convert_upload_to_arrow(file.filename or "", file_bytes, simple_name)

        # Execute target
        storage = get_storage()
        written_at_ns = time.time_ns()

        if m == "staging":
            # Ensure staging folder exists via canonical interface
            from supertable.staging_area import Staging  # noqa: WPS433

            try:
                Staging(organization=org, super_name=sup, staging_name=staging_name.strip())  # side effects
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Create staging failed: {e}")

            # Save parquet via StagingArea if available; otherwise fall back to storage write
            stage_file = None
            try:
                from supertable.super_table import SuperTable  # noqa: WPS433
                from supertable.staging_area import StagingArea  # noqa: WPS433
                import pyarrow.parquet as pq  # noqa: WPS433
                import pyarrow as pa  # noqa: WPS433

                st = SuperTable(super_name=sup, organization=org)
                stg = StagingArea(super_table=st, organization=org)
                # save_as_parquet returns a path like ".../staging/<file>"
                stage_path = stg.save_as_parquet(
                    arrow_table=arrow_table,
                    table_name=simple_name,
                    file_name=file.filename or "upload",
                )
                stage_file = str(stage_path).split("staging/")[-1]
            except Exception:
                # Fallback: write parquet directly via storage into staging folder
                try:
                    import pyarrow.parquet as pq  # noqa: WPS433
                    import pyarrow as pa  # noqa: WPS433
                    import io  # noqa: WPS433

                    buf = io.BytesIO()
                    pq.write_table(arrow_table, buf)
                    buf.seek(0)
                    stage_file = f"{simple_name}_{job_uuid}.parquet"
                    base_dir = os.path.join(org, sup, "staging")
                    p = os.path.join(base_dir, staging_name.strip(), stage_file)
                    if hasattr(storage, "write_bytes"):
                        storage.write_bytes(p, buf.read())
                    elif hasattr(storage, "write"):
                        storage.write(p, buf.read())
                    else:
                        raise RuntimeError("Storage backend does not support byte writes")
                except Exception as e:
                    raise HTTPException(status_code=500, detail=f"Stage write failed: {e}")

            # Update staging index (used by /staging/files)
            base_dir = os.path.join(org, sup, "staging")
            index_path = os.path.join(base_dir, f"{staging_name.strip()}_files.json")
            items = storage.read_json(index_path) if storage.exists(index_path) else []
            if not isinstance(items, list):
                items = []

            items.append(
                {
                    "file": stage_file,
                    "rows": int(getattr(arrow_table, "num_rows", 0) or 0),
                    "written_at_ns": str(written_at_ns),
                    "job_uuid": job_uuid,
                    "file_type": file_type,
                    "table": simple_name,
                }
            )
            write_json_atomic(storage, index_path, items)

            inserted = int(getattr(arrow_table, "num_rows", 0) or 0)
            deleted = 0
        else:
            try:
                from supertable.data_writer import DataWriter  # noqa: WPS433
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"DataWriter import failed: {e}")

            overwrite_cols: List[str] = []
            raw_overwrite = (overwrite_columns or "").strip()
            if raw_overwrite:
                parts = re.split(r"[\s,]+", raw_overwrite)
                for p in parts:
                    col = (p or "").strip()
                    if not col:
                        continue
                    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", col):
                        raise HTTPException(status_code=400, detail=f"Invalid overwrite column: {col}")
                    overwrite_cols.append(col)
                # de-duplicate preserving order
                overwrite_cols = list(dict.fromkeys(overwrite_cols))

            try:
                writer = DataWriter(super_name=sup, organization=org)
                cols, rows, inserted, deleted = writer.write(
                    user_hash=uhash,
                    simple_name=simple_name,
                    data=arrow_table,
                    overwrite_columns=overwrite_cols,
                    compression_level=2,
                )
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        t1 = time.perf_counter()
        server_ms = (t1 - t0) * 1000.0

        return {
            "ok": True,
            "mode": m,
            "org": org,
            "sup": sup,
            "table_name": simple_name,
            "staging_name": staging_name.strip() if staging_name else None,
            "file_name": file.filename,
            "file_type": file_type,
            "rows": int(getattr(arrow_table, "num_rows", 0) or 0),
            "job_uuid": job_uuid,
            "inserted": int(inserted),
            "deleted": int(deleted),
            "written_at_ns": str(written_at_ns),
            "server_duration_ms": round(server_ms, 3),
            "server_duration_s": round(server_ms / 1000.0, 3),
        }
