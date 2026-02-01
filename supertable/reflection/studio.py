from __future__ import annotations

import asyncio
import contextlib
import io
import os
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# In-memory job queue (per-process). This mimics a "central queue" pattern
# without adding external infrastructure.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _JobKey:
    job_id: str


class _Job(BaseModel):
    job_id: str
    status: str = "queued"  # queued | running | done | error
    created_at_ns: int
    started_at_ns: Optional[int] = None
    finished_at_ns: Optional[int] = None
    duration_ms: Optional[float] = None
    stdout: str = ""
    stderr: str = ""
    error: Optional[str] = None


_JOBS: Dict[str, _Job] = {}
_JOB_QUEUE: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
_JOB_LOCK = asyncio.Lock()
_WORKER_STARTED = False

# "Kernel" state to mimic notebook behavior (variables persist across cells)
_KERNELS: Dict[str, Dict[str, Any]] = {}
_KERNEL_LOCK = asyncio.Lock()


class LabJobCreate(BaseModel):
    org: str = Field(..., min_length=1, max_length=256)
    sup: str = Field(..., min_length=1, max_length=256)
    user_hash: str = Field(..., min_length=1, max_length=512)
    code: str = Field(..., min_length=1, max_length=200_000)
    session_id: str = Field(..., min_length=1, max_length=128)


def _now_ns() -> int:
    return time.time_ns()


def _safe_importer(allow: Sequence[str]) -> Callable[..., Any]:
    allowed = {a.strip() for a in allow if a.strip()}

    def _imp(name: str, globals: Any = None, locals: Any = None, fromlist: Any = (), level: int = 0) -> Any:
        root = (name or "").split(".", 1)[0]
        if root and root not in allowed:
            raise ImportError(f"Import blocked in notebook sandbox: {root}")
        return __import__(name, globals, locals, fromlist, level)

    return _imp


def _build_kernel_globals(*, org: str, sup: str, user_hash: str) -> Dict[str, Any]:
    """Create a kernel namespace with built-in connectors.

    This runs inside the API process (still isolated per "session_id") and
    provides a Synapse-like experience (stateful cells) without requiring
    external services.
    """
    allow_imports_raw = (
        os.getenv("SUPERTABLE_LAB_ALLOWED_IMPORTS")
        or os.getenv("SUPERTABLE_NOTEBOOK_ALLOWED_IMPORTS")
        or "math,re,json,datetime,statistics,random,decimal,collections,itertools,functools,typing,pandas,pyarrow,numpy,supertable"
    )
    allow_imports = allow_imports_raw.split(",")

    b = dict(__builtins__=dict(__builtins__))  # type: ignore[arg-type]
    # Replace __import__ with a allowlist import function
    try:
        b["__builtins__"]["__import__"] = _safe_importer(allow_imports)  # type: ignore[index]
    except Exception:
        pass

    ns: Dict[str, Any] = {}
    ns.update(b)

    # Convenience vars
    ns["ORG"] = org
    ns["SUPER"] = sup
    ns["USER_HASH"] = user_hash

    # Connectors (best-effort; import only when called to keep startup fast)
    def st_sql(query: str, *, with_scan: bool = False, limit: Optional[int] = None) -> Any:
        q = str(query or "")
        if limit is not None:
            try:
                lim = int(limit)
                if lim > 0:
                    q = f"{q.rstrip().rstrip(';')} limit {lim}"
            except Exception:
                pass
        from supertable.data_reader import DataReader, engine  # noqa: WPS433

        dr = DataReader(super_name=sup, organization=org, query=q)
        # Returns (df, status, message) in examples; pass through as-is.
        return dr.execute(user_hash=user_hash, with_scan=with_scan, engine=engine.DUCKDB)

    def st_read_table(table: str, *, limit: int = 50) -> Any:
        t = str(table or "").strip()
        if not t:
            raise ValueError("table is required")
        q = f"select * from {t} limit {int(limit)}"
        return st_sql(q, with_scan=False)

    def st_write_table(
        table: str,
        rows: Any,
        *,
        overwrite_columns: Optional[Sequence[str]] = None,
        compression_level: int = 2,
    ) -> Any:
        t = str(table or "").strip()
        if not t:
            raise ValueError("table is required")

        from supertable.data_writer import DataWriter  # noqa: WPS433
        import pyarrow as pa  # noqa: WPS433
        import pandas as pd  # noqa: WPS433

        ow = [str(c).strip() for c in (overwrite_columns or []) if str(c).strip()]

        if isinstance(rows, pa.Table):
            tbl = rows
        elif isinstance(rows, pd.DataFrame):
            tbl = pa.Table.from_pandas(rows)
        elif isinstance(rows, (list, tuple)):
            # list[dict] or list[list] -> DataFrame
            tbl = pa.Table.from_pandas(pd.DataFrame(list(rows)))
        elif isinstance(rows, dict):
            tbl = pa.Table.from_pandas(pd.DataFrame([rows]))
        else:
            raise ValueError("rows must be a PyArrow Table, pandas DataFrame, list, or dict")

        dw = DataWriter(super_name=sup, organization=org)
        return dw.write(
            user_hash=user_hash,
            simple_name=t,
            data=tbl,
            overwrite_columns=list(ow),
            compression_level=int(compression_level),
        )

    ns["st_sql"] = st_sql
    ns["st_read_table"] = st_read_table
    ns["st_write_table"] = st_write_table

    return ns


def _exec_like_jupyter(code: str, ns: Dict[str, Any]) -> str:
    """Exec code; if last statement is an expression, also eval and print it."""
    import ast  # noqa: WPS433

    src = str(code or "")
    tree = ast.parse(src, mode="exec")
    if not tree.body:
        return ""

    last = tree.body[-1]
    if isinstance(last, ast.Expr):
        # execute everything except last expr, then eval last and print repr
        pre = ast.Module(body=tree.body[:-1], type_ignores=[])
        expr = ast.Expression(body=last.value)
        exec(compile(pre, "<notebook>", "exec"), ns, ns)
        val = eval(compile(expr, "<notebook>", "eval"), ns, ns)
        if val is not None:
            print(repr(val))
        return ""
    exec(compile(tree, "<notebook>", "exec"), ns, ns)
    return ""


def _run_code_in_kernel(*, session_key: str, org: str, sup: str, user_hash: str, code: str) -> Tuple[str, str]:
    # Acquire/initialize kernel
    ns = _KERNELS.get(session_key)
    if ns is None:
        ns = _build_kernel_globals(org=org, sup=sup, user_hash=user_hash)
        _KERNELS[session_key] = ns

    out = io.StringIO()
    err = io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        _exec_like_jupyter(code, ns)
    return out.getvalue(), err.getvalue()


async def _job_worker() -> None:
    while True:
        job_payload = await _JOB_QUEUE.get()
        job_id = str(job_payload.get("job_id") or "")
        async with _JOB_LOCK:
            job = _JOBS.get(job_id)
            if not job:
                _JOB_QUEUE.task_done()
                continue
            job.status = "running"
            job.started_at_ns = _now_ns()
            _JOBS[job_id] = job

        started = time.perf_counter()
        try:
            async with _KERNEL_LOCK:
                stdout, stderr = await asyncio.to_thread(
                    _run_code_in_kernel,
                    session_key=str(job_payload["session_key"]),
                    org=str(job_payload["org"]),
                    sup=str(job_payload["sup"]),
                    user_hash=str(job_payload["user_hash"]),
                    code=str(job_payload["code"]),
                )
            duration_ms = (time.perf_counter() - started) * 1000.0
            async with _JOB_LOCK:
                job = _JOBS[job_id]
                job.status = "done"
                job.finished_at_ns = _now_ns()
                job.duration_ms = float(duration_ms)
                job.stdout = stdout
                job.stderr = stderr
                _JOBS[job_id] = job
        except Exception as e:
            duration_ms = (time.perf_counter() - started) * 1000.0
            tb = traceback.format_exc()
            async with _JOB_LOCK:
                job = _JOBS.get(job_id)
                if job:
                    job.status = "error"
                    job.finished_at_ns = _now_ns()
                    job.duration_ms = float(duration_ms)
                    job.error = str(e)
                    job.stderr = (job.stderr or "") + "\n" + tb
                    _JOBS[job_id] = job
        finally:
            _JOB_QUEUE.task_done()


async def _ensure_worker_started(router: APIRouter) -> None:
    global _WORKER_STARTED  # noqa: PLW0603
    if _WORKER_STARTED:
        return
    _WORKER_STARTED = True
    asyncio.create_task(_job_worker())


def attach_studio_routes(
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
    """Register Studio UI + API routes onto an existing router."""

    @router.on_event("startup")
    async def _studio_startup() -> None:  # pragma: no cover
        await _ensure_worker_started(router)
    @router.get("/reflection/studio", response_class=HTMLResponse)
    def studio_page(
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

        resp = templates.TemplateResponse("studio.html", ctx)
        no_store(resp)
        return resp

    @router.get("/reflection/lab", include_in_schema=False)
    def lab_redirect(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
    ):
        # Backward-compat: old /reflection/lab URL now redirects to /reflection/studio
        q = []
        if org:
            q.append(f"org={org}")
        if sup:
            q.append(f"sup={sup}")
        url = "/reflection/studio" + (("?" + "&".join(q)) if q else "")
        resp = RedirectResponse(url, status_code=302)
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

        resp = templates.TemplateResponse("studio.html", ctx)
        no_store(resp)
        return resp
    @router.get("/reflection/notebooks", include_in_schema=False)
    def notebooks_redirect(
        request: Request,
        org: Optional[str] = Query(None),
        sup: Optional[str] = Query(None),
    ):
        # Preserve query params for bookmarks
        q = []
        if org:
            q.append(f"org={org}")
        if sup:
            q.append(f"sup={sup}")
        url = "/reflection/studio" + (("?" + "&".join(q)) if q else "")
        resp = RedirectResponse(url, status_code=302)
        no_store(resp)
        return resp


    @router.post("/reflection/notebooks/jobs")
    @router.post("/reflection/lab/jobs")
    @router.post("/reflection/studio/jobs")
    async def api_create_job(
        payload: LabJobCreate = Body(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        # Basic validation
        if not payload.user_hash.strip():
            raise HTTPException(status_code=400, detail="Missing user_hash")
        if not payload.org.strip() or not payload.sup.strip():
            raise HTTPException(status_code=400, detail="Missing org/sup")
        if not payload.session_id.strip():
            raise HTTPException(status_code=400, detail="Missing session_id")

        job_id = str(uuid.uuid4())
        job = _Job(job_id=job_id, created_at_ns=_now_ns())

        session_key = f"{payload.org}:{payload.sup}:{payload.user_hash}:{payload.session_id}"

        async with _JOB_LOCK:
            _JOBS[job_id] = job

        await _JOB_QUEUE.put(
            {
                "job_id": job_id,
                "session_key": session_key,
                "org": payload.org,
                "sup": payload.sup,
                "user_hash": payload.user_hash,
                "code": payload.code,
            }
        )

        return {"ok": True, "job_id": job_id}

    @router.get("/reflection/notebooks/jobs/{job_id}")
    @router.get("/reflection/lab/jobs/{job_id}")
    @router.get("/reflection/studio/jobs/{job_id}")
    async def api_job_status(
        job_id: str,
        _: Any = Depends(logged_in_guard_api),
    ):
        async with _JOB_LOCK:
            job = _JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return {"ok": True, "job": job.model_dump()}

    @router.post("/reflection/notebooks/kernel/reset")
    @router.post("/reflection/lab/kernel/reset")
    @router.post("/reflection/studio/kernel/reset")
    async def api_kernel_reset(
        payload: Dict[str, Any] = Body(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        org = str(payload.get("org") or "").strip()
        sup = str(payload.get("sup") or "").strip()
        user_hash = str(payload.get("user_hash") or "").strip()
        session_id = str(payload.get("session_id") or "").strip()
        if not (org and sup and user_hash and session_id):
            raise HTTPException(status_code=400, detail="Missing org/sup/user_hash/session_id")

        session_key = f"{org}:{sup}:{user_hash}:{session_id}"
        async with _KERNEL_LOCK:
            _KERNELS.pop(session_key, None)
        return {"ok": True}

# Backward-compatible aliases
attach_lab_routes = attach_studio_routes
attach_notebook_routes = attach_studio_routes
NotebookJobCreate = LabJobCreate
