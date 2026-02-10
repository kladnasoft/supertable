from __future__ import annotations

import asyncio
import datetime as _dt
import re
import uuid
from typing import Any, Callable, Dict, Optional, Sequence, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, Field

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore[assignment]


_CRON_RE = re.compile(r"^\s*([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s+([^\s]+)\s*$")
_POOL_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")


class ScheduledNotebookCreate(BaseModel):
    org: str = Field(..., min_length=1, max_length=256)
    sup: str = Field(..., min_length=1, max_length=256)
    user_hash: str = Field(..., min_length=1, max_length=512)

    name: str = Field(..., min_length=1, max_length=256)
    notebook_id: str = Field(..., min_length=1, max_length=256)
    notebook_name: str = Field(..., min_length=1, max_length=256)

    # Stored for provenance + future UI/exports; execution uses `code`.
    notebook: Dict[str, Any] = Field(default_factory=dict)

    # Executed server-side via Studio's in-process job worker.
    code: str = Field(..., min_length=1, max_length=200_000)

    # Backward-compatible cron input (optional when schedule_type is used)
    cron: Optional[str] = Field(default=None, max_length=128)

    # Simplified schedule builder
    schedule_type: Optional[str] = Field(default=None, max_length=32)
    schedule_values: Optional[str] = Field(default=None, max_length=512)

    timezone: str = Field(default="UTC", min_length=1, max_length=64)

    compute_pool_id: Optional[str] = Field(default=None, max_length=128)


class _ScheduledJob(BaseModel):
    schedule_id: str
    org: str
    sup: str
    user_hash: str

    name: str
    notebook_id: str
    notebook_name: str
    notebook: Dict[str, Any]
    code: str

    # Schedule definition
    cron: Optional[str] = None
    schedule_type: Optional[str] = None
    schedule_values: Optional[str] = None
    schedule_parsed: Dict[str, Any] = Field(default_factory=dict)

    timezone: str
    compute_pool_id: Optional[str] = None

    created_at_iso: str
    updated_at_iso: str

    last_run_iso: Optional[str] = None
    last_job_id: Optional[str] = None
    last_status: Optional[str] = None
    last_error: Optional[str] = None

    # Prevent duplicate triggers in the same minute.
    last_trigger_minute: Optional[str] = None


_SCHEDULES: Dict[str, _ScheduledJob] = {}
_SCHEDULES_LOCK = asyncio.Lock()

_SCHED_TASK: Optional[asyncio.Task[None]] = None
_SCHED_STOP: Optional[asyncio.Event] = None


def _now_utc() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.timezone.utc)


def _iso(dt: _dt.datetime) -> str:
    return dt.astimezone(_dt.timezone.utc).isoformat()


def _get_tz(tz_name: str) -> _dt.tzinfo:
    name = (tz_name or "").strip() or "UTC"
    if name.upper() == "UTC":
        return _dt.timezone.utc
    if ZoneInfo is None:
        raise ValueError("Timezone support not available in this Python build")
    try:
        return ZoneInfo(name)
    except Exception as e:
        raise ValueError(f"Invalid timezone: {name}") from e


def _parse_field(field: str, *, min_v: int, max_v: int) -> set[int]:
    f = (field or "").strip()
    if f == "*":
        return set(range(min_v, max_v + 1))

    out: set[int] = set()
    for part in f.split(","):
        part = part.strip()
        if not part:
            continue

        step = 1
        if "/" in part:
            base, step_s = part.split("/", 1)
            base = base.strip()
            step = int(step_s.strip())
            if step <= 0:
                raise ValueError("Invalid cron step")
        else:
            base = part

        if base == "*":
            lo, hi = min_v, max_v
        elif "-" in base:
            lo_s, hi_s = base.split("-", 1)
            lo, hi = int(lo_s.strip()), int(hi_s.strip())
        else:
            v = int(base.strip())
            lo, hi = v, v

        if lo < min_v or hi > max_v or lo > hi:
            raise ValueError("Cron value out of range")
        for v in range(lo, hi + 1, step):
            out.add(v)

    if not out:
        raise ValueError("Empty cron field")
    return out


def _parse_cron(expr: str) -> tuple[set[int], set[int], str, set[int], str]:
    # Standard 5-field cron: minute hour dom month dow
    m = _CRON_RE.match(expr or "")
    if not m:
        raise ValueError("Invalid cron expression")
    minute_s, hour_s, dom_s, mon_s, dow_s = (x.strip() for x in m.groups())
    minutes = _parse_field(minute_s, min_v=0, max_v=59)
    hours = _parse_field(hour_s, min_v=0, max_v=23)

    # Keep dom/dow as raw fields so we can apply OR semantics when both are restricted.
    dom_raw = dom_s
    mon = _parse_field(mon_s, min_v=1, max_v=12)
    dow_raw = dow_s
    return minutes, hours, dom_raw, mon, dow_raw


def _cron_match(dt_local: _dt.datetime, expr: str) -> bool:
    minutes, hours, dom_raw, mon, dow_raw = _parse_cron(expr)

    if dt_local.minute not in minutes:
        return False
    if dt_local.hour not in hours:
        return False
    if dt_local.month not in mon:
        return False

    # day-of-week: cron typically uses 0=Sun..6=Sat (also allows 7=Sun; we map 7->0)
    dow_val = (dt_local.weekday() + 1) % 7  # Mon->1 ... Sat->6, Sun->0

    dom_is_any = dom_raw.strip() == "*"
    dow_is_any = dow_raw.strip() == "*"

    dom_ok = True
    dow_ok = True

    if not dom_is_any:
        dom_set = _parse_field(dom_raw, min_v=1, max_v=31)
        dom_ok = dt_local.day in dom_set

    if not dow_is_any:
        # allow 0-7, map 7 -> 0
        dow_set: set[int] = set()
        for v in _parse_field(dow_raw, min_v=0, max_v=7):
            dow_set.add(0 if v == 7 else v)
        dow_ok = dow_val in dow_set

    if dom_is_any and dow_is_any:
        return True
    if dom_is_any:
        return dow_ok
    if dow_is_any:
        return dom_ok

    # Vixie cron semantics: dom and dow are ORed when both are restricted.
    return dom_ok or dow_ok



def _dump_model(m: BaseModel) -> Dict[str, Any]:
    # Pydantic v1: .dict(); Pydantic v2: .model_dump()
    if hasattr(m, "model_dump"):
        return m.model_dump()  # type: ignore[no-any-return]
    return m.dict()  # type: ignore[no-any-return]


_SCHEDULE_TYPES = {
    "every",
    "minute",
    "day",
    "week",
    "second_week",
    "month",
    "year",
    "cron",
}

_WEEKDAY_ALIASES: Dict[str, int] = {
    "mon": 0,
    "monday": 0,
    "tue": 1,
    "tues": 1,
    "tuesday": 1,
    "wed": 2,
    "weds": 2,
    "wednesday": 2,
    "thu": 3,
    "thur": 3,
    "thurs": 3,
    "thursday": 3,
    "fri": 4,
    "friday": 4,
    "sat": 5,
    "saturday": 5,
    "sun": 6,
    "sunday": 6,
}


def _split_values(raw: Optional[str]) -> list[str]:
    return [p.strip() for p in (raw or "").split(",") if p.strip()]


def _parse_time_token(tok: str) -> tuple[int, int]:
    t = (tok or "").strip()
    if not t:
        raise ValueError("Missing time")
    if ":" in t:
        hh_s, mm_s = t.split(":", 1)
        hh = int(hh_s.strip())
        mm = int(mm_s.strip())
    else:
        hh = int(t)
        mm = 0
    if hh < 0 or hh > 23:
        raise ValueError("Hour out of range (0-23)")
    if mm < 0 or mm > 59:
        raise ValueError("Minute out of range (0-59)")
    return hh, mm


def _parse_weekday_token(tok: str) -> int:
    t = (tok or "").strip().lower()
    if not t:
        raise ValueError("Missing weekday")
    if t in _WEEKDAY_ALIASES:
        return _WEEKDAY_ALIASES[t]
    if t.isdigit():
        v = int(t)
        if 0 <= v <= 6:
            return v
        if 1 <= v <= 7:
            return v - 1  # Mon=1..Sun=7
    raise ValueError("Invalid weekday (use Mon..Sun or 0..6 or 1..7)")


def _time_key(h: int, m: int) -> str:
    return f"{h:02d}:{m:02d}"


def _week_index(d: _dt.date) -> int:
    # Monday-based continuous week index.
    epoch = _dt.date(1970, 1, 5)  # Monday
    return int((d - epoch).days // 7)


def _parse_schedule_spec(
    *,
    schedule_type: str,
    schedule_values: Optional[str],
    now_local: _dt.datetime,
) -> Dict[str, Any]:
    st = (schedule_type or "").strip()
    if st not in _SCHEDULE_TYPES or st == "cron":
        raise ValueError("Invalid schedule_type")

    vals = _split_values(schedule_values)
    parsed: Dict[str, Any] = {"type": st, "raw": (schedule_values or "").strip()}

    if st == "every":
        if not vals:
            raise ValueError("Value(s) required for 'every' (minutes)")
        if len(vals) != 1:
            raise ValueError("For 'every', provide a single integer (minutes)")
        if not vals[0].isdigit():
            raise ValueError("For 'every', value must be an integer (minutes)")
        n = int(vals[0])
        if n <= 0 or n > 525600:
            raise ValueError("For 'every', minutes must be between 1 and 525600")
        parsed["interval_minutes"] = n
        parsed["anchor_minute_utc"] = int(_now_utc().timestamp() // 60)
        return parsed

    if st == "minute":
        # If empty: every minute
        minutes: set[int] = set()
        for v in vals:
            if not v.isdigit():
                raise ValueError("Minute values must be integers (0-59)")
            mv = int(v)
            if mv < 0 or mv > 59:
                raise ValueError("Minute value out of range (0-59)")
            minutes.add(mv)
        parsed["minutes"] = sorted(minutes)
        return parsed

    if st == "day":
        # Allow empty => 00:00
        if not vals:
            vals = ["00:00"]
        times: set[str] = set()
        for v in vals:
            hh, mm = _parse_time_token(v)
            times.add(_time_key(hh, mm))
        parsed["times"] = sorted(times)
        return parsed

    if st in ("week", "second_week"):
        if not vals:
            raise ValueError("Value(s) required for weekly schedules")
        by_day: Dict[int, set[str]] = {}
        for v in vals:
            token = v.strip()
            day_part = ""
            time_part = ""
            if "@" in token:
                day_part, time_part = token.split("@", 1)
            else:
                # allow "Mon 13:00"
                parts = token.split()
                if len(parts) == 2:
                    day_part, time_part = parts[0], parts[1]
                else:
                    # If only weekday provided, default midnight.
                    day_part, time_part = token, "00:00"
            wd = _parse_weekday_token(day_part)
            hh, mm = _parse_time_token(time_part)
            by_day.setdefault(wd, set()).add(_time_key(hh, mm))
        parsed["by_day"] = {str(k): sorted(v) for k, v in by_day.items()}
        if st == "second_week":
            parsed["anchor_week_index"] = _week_index(now_local.date())
        return parsed

    if st == "month":
        if not vals:
            raise ValueError("Value(s) required for monthly schedules")
        by_dom: Dict[int, set[str]] = {}
        for v in vals:
            token = v.strip()
            if "@" in token:
                dom_s, time_s = token.split("@", 1)
            else:
                dom_s, time_s = token, "00:00"
            if not dom_s.strip().isdigit():
                raise ValueError("Month day must be an integer (1-31)")
            dom = int(dom_s.strip())
            if dom < 1 or dom > 31:
                raise ValueError("Month day out of range (1-31)")
            hh, mm = _parse_time_token(time_s)
            by_dom.setdefault(dom, set()).add(_time_key(hh, mm))
        parsed["by_dom"] = {str(k): sorted(v) for k, v in by_dom.items()}
        return parsed

    if st == "year":
        if not vals:
            raise ValueError("Value(s) required for yearly schedules")
        by_md: Dict[str, set[str]] = {}
        for v in vals:
            token = v.strip()
            if "@" in token:
                md_s, time_s = token.split("@", 1)
            else:
                md_s, time_s = token, "00:00"
            md_s = md_s.strip().replace("/", "-")
            if "-" not in md_s:
                raise ValueError("Yearly value must be MM-DD (or MM/DD)")
            mm_s, dd_s = md_s.split("-", 1)
            if not (mm_s.strip().isdigit() and dd_s.strip().isdigit()):
                raise ValueError("Yearly month/day must be integers")
            mm = int(mm_s.strip())
            dd = int(dd_s.strip())
            if mm < 1 or mm > 12:
                raise ValueError("Month out of range (1-12)")
            if dd < 1 or dd > 31:
                raise ValueError("Day out of range (1-31)")
            hh, mi = _parse_time_token(time_s)
            key = f"{mm:02d}-{dd:02d}"
            by_md.setdefault(key, set()).add(_time_key(hh, mi))
        parsed["by_md"] = {k: sorted(v) for k, v in by_md.items()}
        return parsed

    raise ValueError("Unsupported schedule_type")


def _schedule_match(*, now_local: _dt.datetime, now_utc: _dt.datetime, s: _ScheduledJob) -> bool:
    st = (s.schedule_type or "").strip()
    if not st or st == "cron":
        expr = (s.cron or "").strip()
        if not expr:
            return False
        return _cron_match(now_local, expr)

    parsed = s.schedule_parsed or {}
    if parsed.get("type") != st:
        # Best-effort: do not trigger if spec is inconsistent.
        return False

    if st == "every":
        try:
            n = int(parsed.get("interval_minutes") or 0)
            anchor = int(parsed.get("anchor_minute_utc") or 0)
        except Exception:
            return False
        if n <= 0:
            return False
        cur = int(now_utc.timestamp() // 60)
        if cur < anchor:
            return False
        return (cur - anchor) % n == 0

    if st == "minute":
        mins = parsed.get("minutes")
        if not mins:
            return True  # every minute
        try:
            return int(now_local.minute) in {int(x) for x in mins}
        except Exception:
            return False

    time_k = _time_key(int(now_local.hour), int(now_local.minute))

    if st == "day":
        times = parsed.get("times") or []
        return time_k in set(times)

    if st in ("week", "second_week"):
        by_day = parsed.get("by_day") or {}
        try:
            times = set(by_day.get(str(int(now_local.weekday()))) or [])
        except Exception:
            times = set()
        if time_k not in times:
            return False
        if st == "week":
            return True
        try:
            anchor_w = int(parsed.get("anchor_week_index") or 0)
        except Exception:
            return False
        cur_w = _week_index(now_local.date())
        if cur_w < anchor_w:
            return False
        return (cur_w - anchor_w) % 2 == 0

    if st == "month":
        by_dom = parsed.get("by_dom") or {}
        try:
            times = set(by_dom.get(str(int(now_local.day))) or [])
        except Exception:
            times = set()
        return time_k in times

    if st == "year":
        by_md = parsed.get("by_md") or {}
        key = f"{int(now_local.month):02d}-{int(now_local.day):02d}"
        times = set(by_md.get(key) or [])
        return time_k in times

    return False



async def _enqueue_studio_job(
    *,
    org: str,
    sup: str,
    user_hash: str,
    code: str,
    session_id: str,
    compute_pool_id: Optional[str],
    router: APIRouter,
) -> str:
    # Import lazily to avoid import-time coupling / circulars.
    from supertable.reflection import studio as _studio  # type: ignore

    await _studio._ensure_worker_started(router)

    compute = (compute_pool_id or "").strip() or None
    if compute and not _POOL_RE.fullmatch(compute):
        raise ValueError("Invalid compute_pool_id")

    sess = (session_id or "").strip()
    if not sess:
        raise ValueError("Missing session_id")
    if len(sess) > 128:
        sess = sess[:128]

    c = (code or "").strip()
    if not c:
        raise ValueError("Missing code")
    if len(c) > 200_000:
        raise ValueError("Code too large")

    job_id = str(uuid.uuid4())
    job = _studio._Job(job_id=job_id, created_at_ns=_studio._now_ns(), compute_pool_id=compute)
    session_key = f"{org}:{sup}:{user_hash}:{sess}"

    async with _studio._JOB_LOCK:
        _studio._JOBS[job_id] = job

    await _studio._JOB_QUEUE.put(
        {
            "job_id": job_id,
            "session_key": session_key,
            "org": org,
            "sup": sup,
            "user_hash": user_hash,
            "code": c,
            "compute_pool_id": compute,
        }
    )

    return job_id


async def _scheduler_loop(router: APIRouter) -> None:
    global _SCHED_STOP  # noqa: PLW0603
    stop = _SCHED_STOP
    if stop is None:
        stop = asyncio.Event()
        _SCHED_STOP = stop

    # tick often enough to catch the exact minute boundary even if there's jitter
    tick_s = 5

    while not stop.is_set():
        try:
            now_utc = _now_utc()
            async with _SCHEDULES_LOCK:
                schedules = list(_SCHEDULES.values())

            for s in schedules:
                try:
                    tz = _get_tz(s.timezone)
                    now_local = now_utc.astimezone(tz)

                    minute_key = now_local.strftime("%Y-%m-%dT%H:%M")
                    if s.last_trigger_minute == minute_key:
                        continue

                    if _schedule_match(now_local=now_local, now_utc=now_utc, s=s):
                        # Mark before running to avoid duplicate triggers if execution is slow.
                        async with _SCHEDULES_LOCK:
                            cur = _SCHEDULES.get(s.schedule_id)
                            if cur and cur.last_trigger_minute != minute_key:
                                cur.last_trigger_minute = minute_key
                                cur.updated_at_iso = _iso(now_utc)
                                _SCHEDULES[s.schedule_id] = cur

                        await _run_schedule(schedule_id=s.schedule_id, router=router, reason="scheduled")
                except Exception:
                    # Do not crash the scheduler loop for a single schedule.
                    continue
        finally:
            try:
                await asyncio.wait_for(stop.wait(), timeout=tick_s)
            except asyncio.TimeoutError:
                pass


async def _run_schedule(*, schedule_id: str, router: APIRouter, reason: str) -> str:
    async with _SCHEDULES_LOCK:
        s = _SCHEDULES.get(schedule_id)
    if not s:
        raise KeyError("Schedule not found")

    now_utc = _now_utc()
    session_id = f"sched:{schedule_id}:{int(now_utc.timestamp())}:{reason}"

    try:
        job_id = await _enqueue_studio_job(
            org=s.org,
            sup=s.sup,
            user_hash=s.user_hash,
            code=s.code,
            session_id=session_id,
            compute_pool_id=s.compute_pool_id,
            router=router,
        )
        async with _SCHEDULES_LOCK:
            cur = _SCHEDULES.get(schedule_id)
            if cur:
                cur.last_run_iso = _iso(now_utc)
                cur.last_job_id = job_id
                cur.last_status = "queued"
                cur.last_error = None
                cur.updated_at_iso = _iso(now_utc)
                _SCHEDULES[schedule_id] = cur
        return job_id
    except Exception as e:
        async with _SCHEDULES_LOCK:
            cur = _SCHEDULES.get(schedule_id)
            if cur:
                cur.last_run_iso = _iso(now_utc)
                cur.last_job_id = None
                cur.last_status = "error"
                cur.last_error = str(e)
                cur.updated_at_iso = _iso(now_utc)
                _SCHEDULES[schedule_id] = cur
        raise


def attach_jobs_routes(
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
    """Register Jobs UI routes onto an existing router."""

    @router.get("/reflection/jobs", response_class=HTMLResponse)
    def jobs_page(
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

        resp = templates.TemplateResponse("jobs.html", ctx)
        no_store(resp)
        return resp

    # ---------------------------------------------------------------------
    # Scheduled notebook jobs (in-memory, per-process)
    # ---------------------------------------------------------------------

    @router.post("/reflection/jobs/scheduled")
    async def api_create_scheduled_job(
        payload: ScheduledNotebookCreate = Body(...),
        _: Any = Depends(logged_in_guard_api),
    ):
        schedule_type = (payload.schedule_type or "").strip() or None
        schedule_values = (payload.schedule_values or "").strip() or None
        cron_expr = (payload.cron or "").strip() or None
        now = _now_utc()

        try:
            tz = _get_tz(payload.timezone)
            now_local = now.astimezone(tz)

            parsed: Dict[str, Any] = {}
            if schedule_type:
                parsed = _parse_schedule_spec(
                    schedule_type=schedule_type,
                    schedule_values=schedule_values,
                    now_local=now_local,
                )
            else:
                if not cron_expr:
                    raise ValueError("Schedule is required")
                _parse_cron(cron_expr)
                schedule_type = "cron"
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e)) from e

        compute = (payload.compute_pool_id or "").strip() or None
        if compute and not _POOL_RE.fullmatch(compute):
            raise HTTPException(status_code=400, detail="Invalid compute_pool_id")

        schedule_id = str(uuid.uuid4())
        # now already computed above
        item = _ScheduledJob(
            schedule_id=schedule_id,
            org=payload.org.strip(),
            sup=payload.sup.strip(),
            user_hash=payload.user_hash.strip(),
            name=payload.name.strip(),
            notebook_id=payload.notebook_id.strip(),
            notebook_name=payload.notebook_name.strip(),
            notebook=payload.notebook or {},
            code=payload.code,
            cron=cron_expr,
            schedule_type=schedule_type,
            schedule_values=schedule_values,
            schedule_parsed=parsed,
            timezone=payload.timezone.strip(),
            compute_pool_id=compute,
            created_at_iso=_iso(now),
            updated_at_iso=_iso(now),
        )

        async with _SCHEDULES_LOCK:
            _SCHEDULES[schedule_id] = item

        return {"ok": True, "schedule_id": schedule_id}

    @router.get("/reflection/jobs/scheduled")
    async def api_list_scheduled_jobs(
        org: str = Query(..., min_length=1, max_length=256),
        sup: str = Query(..., min_length=1, max_length=256),
        user_hash: str = Query(..., min_length=1, max_length=512),
        _: Any = Depends(logged_in_guard_api),
    ):
        # Best-effort: refresh last status from Studio's in-memory job store.
        try:
            from supertable.reflection import studio as _studio  # type: ignore
        except Exception:
            _studio = None  # type: ignore

        async with _SCHEDULES_LOCK:
            items = [
                s
                for s in _SCHEDULES.values()
                if s.org == org and s.sup == sup and s.user_hash == user_hash
            ]

        if _studio is not None:
            for s in items:
                if not s.last_job_id:
                    continue
                async with _studio._JOB_LOCK:
                    job = _studio._JOBS.get(s.last_job_id)
                if job:
                    s.last_status = job.status
                    s.last_error = job.error

        return {"ok": True, "items": [_dump_model(s) for s in items]}

    @router.post("/reflection/jobs/scheduled/{schedule_id}/run_now")
    async def api_run_scheduled_job_now(
        schedule_id: str,
        org: str = Query(..., min_length=1, max_length=256),
        sup: str = Query(..., min_length=1, max_length=256),
        user_hash: str = Query(..., min_length=1, max_length=512),
        _: Any = Depends(logged_in_guard_api),
    ):
        async with _SCHEDULES_LOCK:
            s = _SCHEDULES.get(schedule_id)
        if not s or s.org != org or s.sup != sup or s.user_hash != user_hash:
            raise HTTPException(status_code=404, detail="Schedule not found")

        try:
            job_id = await _run_schedule(schedule_id=schedule_id, router=router, reason="manual")
            return {"ok": True, "job_id": job_id}
        except KeyError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e)) from e

    @router.delete("/reflection/jobs/scheduled/{schedule_id}")
    async def api_delete_scheduled_job(
        schedule_id: str,
        org: str = Query(..., min_length=1, max_length=256),
        sup: str = Query(..., min_length=1, max_length=256),
        user_hash: str = Query(..., min_length=1, max_length=512),
        _: Any = Depends(logged_in_guard_api),
    ):
        async with _SCHEDULES_LOCK:
            s = _SCHEDULES.get(schedule_id)
            if not s or s.org != org or s.sup != sup or s.user_hash != user_hash:
                raise HTTPException(status_code=404, detail="Schedule not found")
            _SCHEDULES.pop(schedule_id, None)
        return {"ok": True}

    @router.on_event("startup")
    async def _jobs_startup() -> None:  # pragma: no cover
        global _SCHED_TASK, _SCHED_STOP  # noqa: PLW0603
        if _SCHED_TASK and not _SCHED_TASK.done():
            return
        if _SCHED_STOP is None:
            _SCHED_STOP = asyncio.Event()
        _SCHED_TASK = asyncio.create_task(_scheduler_loop(router))

    @router.on_event("shutdown")
    async def _jobs_shutdown() -> None:  # pragma: no cover
        global _SCHED_TASK, _SCHED_STOP  # noqa: PLW0603
        if _SCHED_STOP is not None:
            _SCHED_STOP.set()
        if _SCHED_TASK:
            try:
                await asyncio.wait_for(_SCHED_TASK, timeout=2.0)
            except Exception:
                pass