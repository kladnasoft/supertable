# route: supertable.quality.scheduler
# supertable/quality/scheduler.py
"""
Durable, bounded background scheduler for Data Quality checks.

Runs a coordinator and bounded table-worker pool inside the host process. Cron
phase/outcomes and ingest/history work are persisted in Redis so a restart does
not reset scheduling state or acknowledge a lost history row. Each scheduled
mode runs in a clean spawned process. Deadline, shutdown, and lease-loss fences
terminate that process group, so a native driver that ignores cancellation
cannot permanently consume a worker slot.

Post-ingest trigger uses a three-layer protection:

  1. DEBOUNCE — ingest replaces a generation token (not immediate execution).
     Independent quick/deep/custom companion keys prevent one mode consuming
     another. A compare-and-delete preserves an ingest racing with execution.

  2. LOCK — only one check can run per table at a time.
     If a check is already running, the pending flag stays for the next tick.

  3. COOLDOWN — after a mode completes, that mode's cooldown is set (default
     5 min). Failures use a shorter retry key and never receive a success
     cooldown, so pending work remains retryable without blocking other modes.

Redis keys used:

  ...:quality:pending:{table}             — compatibility/debounce marker
  ...:quality:pending_mode:{table}:{mode} — persistent generation per mode
  ...:quality:running:{table}             — renewable, ownership-fenced lock
  ...:quality:cooldown:{table}:{mode}      — success cooldown per mode
  ...:quality:retry:{table}:{mode}         — failure retry backoff per mode
"""
from __future__ import annotations

import json
import logging
import math
import multiprocessing
import os
import signal
import threading
import time
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from supertable.quality.serialization import normalize_json_value

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

DEFAULT_COOLDOWN_SECONDS = 300       # 5 minutes between checks on same table
DEFAULT_PENDING_TTL_SECONDS = 600    # pending flag expires after 10 min
DEFAULT_RUNNING_TTL_SECONDS = 300    # safety: running lock expires after 5 min
DEFAULT_RETRY_BACKOFF_SECONDS = 60   # failed checks retry separately from success cooldown
TICK_INTERVAL_SECONDS = 60           # scheduler wakes up every 60s
DEFAULT_MAX_WORKERS = max(
    1, min(32, int(os.environ.get("SUPERTABLE_DQ_MAX_WORKERS", "4")))
)
DEFAULT_JOB_DEADLINE_SECONDS = max(
    1, int(os.environ.get("SUPERTABLE_DQ_JOB_DEADLINE_SECONDS", "300"))
)
_PROCESS_POLL_SECONDS = 0.02
_PROCESS_TERMINATE_GRACE_SECONDS = 0.25
_PROCESS_RESULT_EXIT_GRACE_SECONDS = 0.25

QUALITY_MODES = ("quick", "deep", "custom")


class _LeaseLostError(RuntimeError):
    """Raised when a scheduler worker no longer owns its table lease."""


@dataclass
class _LeaseGuard:
    """Fail-closed ownership fence shared by a runner and its renewer.

    A failed renewal is deliberately treated as lease loss even when the
    failure may be transient.  Continuing to publish after an ambiguous Redis
    response is unsafe: another process may already own the table lease.
    """

    redis: Any
    key: str
    token: str
    lost: threading.Event = field(default_factory=threading.Event)
    fence_lock: Any = field(default_factory=threading.RLock)

    def mark_lost(self) -> None:
        with self.fence_lock:
            self.lost.set()

    def owns_lease(self) -> bool:
        with self.fence_lock:
            if self.lost.is_set():
                return False
            try:
                current = self.redis.get(self.key)
            except Exception:
                self.lost.set()
                return False
            if current is None or not _redis_values_equal(current, self.token):
                self.lost.set()
                return False
            return True

    def assert_owned(self) -> None:
        if not self.owns_lease():
            raise _LeaseLostError(
                "quality execution lease was lost before publication"
            )


def _assert_lease_owned(lease_guard: Optional[_LeaseGuard]) -> None:
    """Fence a publication when invoked by the scheduler.

    Direct/manual runner calls have no distributed lease and retain their
    historical behavior. Scheduler calls always supply a guard.
    """

    if lease_guard is not None:
        lease_guard.assert_owned()


@dataclass(frozen=True)
class CheckRunOutcome:
    """Truthful result of one scheduler attempt.

    ``success`` means the complete requested mode was evaluated and its durable
    result was published.  A lock/cooldown skip and an execution failure are
    deliberately different states: only success is eligible for cooldown and
    for consuming a post-ingest generation.
    """

    mode: str
    state: str
    evaluated: int = 0
    passed: int = 0
    warnings: int = 0
    critical: int = 0
    errors: int = 0
    skipped: int = 0
    message: str = ""
    details: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def successful(self) -> bool:
        return self.state == "success"

    @property
    def executed(self) -> bool:
        return self.state in {"success", "failed"}

    def __bool__(self) -> bool:
        # Preserve the historical truth-test contract of _try_run_check while
        # exposing enough state for callers to distinguish skip from failure.
        return self.successful


@dataclass(frozen=True)
class _IsolatedProcessResult:
    """Terminal state of one killable quality subprocess."""

    state: str
    payload: Any = None
    message: str = ""
    pid: Optional[int] = None
    exitcode: Optional[int] = None
    force_terminated: bool = False
    alive_after_cleanup: bool = False


def _success(
    mode: str,
    *,
    evaluated: int = 0,
    passed: int = 0,
    warnings: int = 0,
    critical: int = 0,
    skipped: int = 0,
    details: Optional[List[Dict[str, Any]]] = None,
) -> CheckRunOutcome:
    return CheckRunOutcome(
        mode=mode,
        state="success",
        evaluated=evaluated,
        passed=passed,
        warnings=warnings,
        critical=critical,
        skipped=skipped,
        details=tuple(details or ()),
    )


def _failed(mode: str, message: str, *, errors: int = 1) -> CheckRunOutcome:
    return CheckRunOutcome(mode=mode, state="failed", errors=errors, message=message)


def _skipped(mode: str, message: str) -> CheckRunOutcome:
    return CheckRunOutcome(mode=mode, state="skipped", message=message)

# Singleton state
_scheduler_thread: Optional[threading.Thread] = None
_scheduler_executor: Optional[ThreadPoolExecutor] = None
_scheduler_lock = threading.Lock()
_scheduler_stop = threading.Event()
_active_jobs_lock = threading.Lock()
_active_jobs_condition = threading.Condition(_active_jobs_lock)
_active_jobs: Dict[str, Any] = {}
_scheduler_slots: Optional[threading.BoundedSemaphore] = None
_scheduler_fair_cursor = 0
_scheduler_stats_lock = threading.Lock()
_scheduler_stats: Dict[str, Any] = {
    "running": False,
    "ticks": 0,
    "tick_failures": 0,
    "deadline_exceeded": 0,
    "jobs_submitted": 0,
    "jobs_completed": 0,
    "jobs_failed": 0,
    "jobs_cancelled": 0,
    "jobs_force_terminated": 0,
    "jobs_skipped_active": 0,
    "jobs_skipped_capacity": 0,
    "active_jobs": 0,
    "last_tick_started_at": None,
    "last_tick_completed_at": None,
}


# ──────────────────────────────────────────────────────────────────────
# Public API — called from ingest code
# ──────────────────────────────────────────────────────────────────────

def notify_ingest(r, org: str, sup: str, table_name: str) -> None:
    """
    Called by the ingest/write path after data is loaded into a table.

    Does NOT run a quality check — just sets a "pending" flag in Redis.
    The scheduler loop will pick it up on the next tick, respecting
    debounce, lock, and cooldown.

    This is safe to call at any frequency — 1000 calls/sec will still
    result in at most 1 quality check per cooldown period.
    """
    # Skip system tables — they are written by quality checks themselves.
    if table_name.startswith("__") and table_name.endswith("__"):
        return

    generation = f"{time.time_ns()}:{uuid.uuid4().hex}"
    unresolved_key = _unresolved_pending_key(org, sup, table_name)
    try:
        # Persist uncertainty *before* reading schedule configuration. A
        # transient config GET failure can then delay mode resolution but can
        # never silently lose this ingest generation.
        if not r.set(unresolved_key, generation):
            logger.warning(
                "[dq-ingest] Could not persist unresolved generation for %s/%s/%s",
                org,
                sup,
                table_name,
            )
            return
        from supertable.quality.config import DQConfig
        dqc = DQConfig(r, org, sup)
        schedule = dqc.get_schedule()
        table_schedule = dqc.get_table_schedule(table_name) or {}
        modes = (
            _post_ingest_modes(schedule, table_schedule)
            if schedule.get("enabled", True)
            and table_schedule.get("enabled", True)
            else ()
        )
        if not modes:
            _consume_pending_generation(r, unresolved_key, generation)
            return

        # One immutable token identifies the ingest generation.  Each mode has
        # an independent pending key so a successful quick profile can never
        # erase a failed/deferred custom or deep profile.  Replacing the value
        # is the debounce operation; compare-and-delete in the scheduler makes
        # an ingest racing with a running check impossible to lose.
        try:
            pipe = r.pipeline(transaction=True)
            # Keep the historical scalar marker for observers and rolling
            # upgrades.  The scheduler migrates/consumes it into the
            # authoritative per-mode keys below.
            pipe.set(
                _pending_key(org, sup, table_name),
                generation,
                ex=DEFAULT_PENDING_TTL_SECONDS,
            )
            for mode in modes:
                # Authoritative work must outlive an arbitrarily long
                # cooldown.  It is removed only by compare-and-delete after a
                # fenced successful run.  The compatibility scalar below may
                # retain its bounded TTL.
                pipe.set(
                    _pending_key(org, sup, table_name, mode),
                    generation,
                )
            results = pipe.execute()
            if not all(results):
                raise RuntimeError("pending generation transaction was not acknowledged")
        except (AttributeError, NotImplementedError):
            # Small Redis-compatible test/embedded clients need not implement
            # pipelines.  Independent SETs remain safe because modes are never
            # consumed as a group.
            if not r.set(
                _pending_key(org, sup, table_name),
                generation,
                ex=DEFAULT_PENDING_TTL_SECONDS,
            ):
                raise RuntimeError("scalar pending generation was not acknowledged")
            for mode in modes:
                if not r.set(
                    _pending_key(org, sup, table_name, mode),
                    generation,
                ):
                    raise RuntimeError(
                        f"{mode} pending generation was not acknowledged"
                    )
        _consume_pending_generation(r, unresolved_key, generation)
        logger.debug(
            "[dq-ingest] Pending generation set for %s/%s/%s modes=%s",
            org,
            sup,
            table_name,
            ",".join(modes),
        )
    except Exception as e:
        # Never fail ingest itself. The persistent unresolved marker remains
        # for the scheduler to resolve after configuration/backend recovery.
        logger.warning(f"[dq-ingest] Deferred pending-mode resolution: {e}")


# ──────────────────────────────────────────────────────────────────────
# Scheduler thread
# ──────────────────────────────────────────────────────────────────────


def _stats_update(**changes: Any) -> None:
    with _scheduler_stats_lock:
        for key, value in changes.items():
            _scheduler_stats[key] = value


def _stats_increment(key: str, amount: int = 1) -> None:
    with _scheduler_stats_lock:
        _scheduler_stats[key] = int(_scheduler_stats.get(key, 0)) + int(amount)


def scheduler_health() -> Dict[str, Any]:
    """Return a thread-safe operational snapshot for probes and metrics."""
    with _scheduler_stats_lock:
        result = dict(_scheduler_stats)
    with _scheduler_lock:
        result["thread_alive"] = bool(
            _scheduler_thread is not None and _scheduler_thread.is_alive()
        )
    with _active_jobs_lock:
        result["active_jobs"] = len(_active_jobs)
    result["running"] = bool(result["thread_alive"] or result["active_jobs"])
    result["max_workers"] = DEFAULT_MAX_WORKERS
    result["job_deadline_seconds"] = DEFAULT_JOB_DEADLINE_SECONDS
    return result


def _job_done(job_key: str, future: Any, slots: threading.BoundedSemaphore) -> None:
    try:
        error = future.exception()
    except Exception as exc:  # cancelled/broken future
        error = exc
    with _active_jobs_lock:
        _active_jobs.pop(job_key, None)
        active = len(_active_jobs)
        _active_jobs_condition.notify_all()
    slots.release()
    _stats_increment("jobs_completed")
    if error is not None:
        _stats_increment("jobs_failed")
        logger.error(
            "[dq-scheduler] table job %s failed: %s", job_key, error,
            exc_info=(type(error), error, error.__traceback__),
        )
    _stats_update(active_jobs=active)


def _submit_table_job(
    executor: ThreadPoolExecutor,
    slots: threading.BoundedSemaphore,
    job_key: str,
    function,
    *args: Any,
) -> bool:
    """Bound queue growth and suppress duplicate work for one table."""
    with _active_jobs_lock:
        if job_key in _active_jobs:
            _stats_increment("jobs_skipped_active")
            return False
        if not slots.acquire(blocking=False):
            _stats_increment("jobs_skipped_capacity")
            return False
        try:
            future = executor.submit(function, *args)
        except Exception:
            slots.release()
            raise
        _active_jobs[job_key] = future
        active = len(_active_jobs)
    future.add_done_callback(lambda completed: _job_done(job_key, completed, slots))
    _stats_increment("jobs_submitted")
    _stats_update(active_jobs=active)
    return True


def _quality_process_context():
    """Return the only supported execution context for quality isolation.

    The scheduler is already multi-threaded when a check starts.  Forking that
    process would inherit locks and live Redis/engine connections in an
    unknowable state, so each check uses a clean ``spawn`` interpreter.
    """

    return multiprocessing.get_context("spawn")


def _isolated_process_bootstrap(send_connection, target, target_args) -> None:
    """Create a private process group, then invoke an isolated child target."""

    if os.name == "posix":
        try:
            os.setsid()
        except OSError:
            # Parent-side termination still targets this exact PID.  ``setsid``
            # only broadens cleanup to any engine descendants the child starts.
            pass
    try:
        target(send_connection, *target_args)
    except BaseException as exc:
        try:
            send_connection.send(("error", f"{type(exc).__name__}: {exc}"))
        except Exception:
            pass
    finally:
        try:
            send_connection.close()
        except Exception:
            pass


def _signal_isolated_process(process, sig: int) -> None:
    """Signal an exact child, and its private process group when available."""

    pid = process.pid
    if pid is None:
        return
    if os.name == "posix":
        try:
            process_group = os.getpgid(pid)
        except (OSError, ProcessLookupError):
            return
        try:
            if process_group == pid:
                os.killpg(process_group, sig)
            else:
                # The child may not have reached setsid() yet. Never signal the
                # host's process group; target only the child in that race.
                os.kill(pid, sig)
            return
        except (OSError, ProcessLookupError):
            return
    try:
        if sig == getattr(signal, "SIGKILL", None):
            process.kill()
        else:
            process.terminate()
    except (OSError, ProcessLookupError):
        pass


def _terminate_isolated_process(process) -> bool:
    """Bounded TERM/KILL escalation. Return only after observing child exit."""

    if not process.is_alive():
        process.join(timeout=0)
        return True
    _signal_isolated_process(process, signal.SIGTERM)
    process.join(timeout=_PROCESS_TERMINATE_GRACE_SECONDS)
    if process.is_alive():
        hard_signal = getattr(signal, "SIGKILL", signal.SIGTERM)
        _signal_isolated_process(process, hard_signal)
        process.join(timeout=_PROCESS_TERMINATE_GRACE_SECONDS)
    return not process.is_alive()


def _run_killable_subprocess(
    target,
    target_args: Tuple[Any, ...],
    *,
    deadline_monotonic: Optional[float],
    cancel_event: Optional[Any],
    lease_lost_event: Optional[Any],
    process_name: str,
) -> _IsolatedProcessResult:
    """Run one callable in a spawned process with a hard terminal bound.

    A target sends ``("result", payload)`` on the supplied one-way connection.
    Deadline, scheduler shutdown, or lease loss terminates the whole private
    process group.  This function never returns while its child is known alive,
    so the enclosing ThreadPool slot is genuinely reusable.
    """

    if deadline_monotonic is None and cancel_event is None:
        return _IsolatedProcessResult(
            state="start_failed",
            message="isolated quality execution requires a deadline or shutdown fence",
        )
    context = _quality_process_context()
    receive_connection, send_connection = context.Pipe(duplex=False)
    process = context.Process(
        target=_isolated_process_bootstrap,
        args=(send_connection, target, target_args),
        name=process_name[:120],
        daemon=False,
    )
    try:
        process.start()
    except Exception as exc:
        try:
            receive_connection.close()
            send_connection.close()
            process.close()
        except Exception:
            pass
        return _IsolatedProcessResult(
            state="start_failed",
            message=f"could not start isolated quality process: {exc}",
        )
    finally:
        # Once started, the parent must not retain the writer end or EOF would
        # remain hidden after an abnormal child exit.
        if process.pid is not None:
            try:
                send_connection.close()
            except Exception:
                pass

    pid = process.pid
    payload: Any = None
    payload_received = False
    child_error = ""
    payload_received_at: Optional[float] = None
    terminal_reason: Optional[str] = None
    force_terminated = False

    def receive_available() -> None:
        nonlocal payload, payload_received, child_error, payload_received_at
        try:
            while receive_connection.poll(0):
                message = receive_connection.recv()
                if (
                    isinstance(message, tuple)
                    and len(message) == 2
                    and message[0] == "result"
                ):
                    payload = message[1]
                    payload_received = True
                    payload_received_at = time.monotonic()
                elif (
                    isinstance(message, tuple)
                    and len(message) == 2
                    and message[0] == "error"
                ):
                    child_error = str(message[1])
                else:
                    child_error = "isolated quality process returned an invalid envelope"
        except (EOFError, OSError):
            pass

    try:
        while True:
            now = time.monotonic()
            if cancel_event is not None and cancel_event.is_set():
                terminal_reason = "shutdown"
                break
            if deadline_monotonic is not None and now >= deadline_monotonic:
                terminal_reason = "deadline"
                break
            if lease_lost_event is not None and lease_lost_event.is_set():
                terminal_reason = "lease_lost"
                break

            receive_available()
            process.join(timeout=_PROCESS_POLL_SECONDS)
            if not process.is_alive():
                receive_available()
                break
            if (
                payload_received_at is not None
                and time.monotonic() - payload_received_at
                >= _PROCESS_RESULT_EXIT_GRACE_SECONDS
            ):
                # A valid runner result is not permission for leaked child
                # threads/processes to outlive the bounded worker slot.
                terminal_reason = "result_cleanup_timeout"
                break

        if terminal_reason is not None:
            force_terminated = process.is_alive()
            exited = _terminate_isolated_process(process)
            receive_available()
            if not exited:
                return _IsolatedProcessResult(
                    state="termination_failed",
                    payload=payload if payload_received else None,
                    message=(
                        f"isolated quality process did not exit after {terminal_reason}"
                    ),
                    pid=pid,
                    exitcode=process.exitcode,
                    force_terminated=True,
                    alive_after_cleanup=True,
                )
            if terminal_reason == "result_cleanup_timeout" and payload_received:
                terminal_reason = "completed"

        exitcode = process.exitcode
        if terminal_reason in {"deadline", "shutdown", "lease_lost"}:
            return _IsolatedProcessResult(
                state=terminal_reason,
                message=f"isolated quality process stopped after {terminal_reason}",
                pid=pid,
                exitcode=exitcode,
                force_terminated=force_terminated,
            )
        if child_error:
            return _IsolatedProcessResult(
                state="crashed",
                message=child_error,
                pid=pid,
                exitcode=exitcode,
                force_terminated=force_terminated,
            )
        if not payload_received:
            return _IsolatedProcessResult(
                state="crashed",
                message=(
                    "isolated quality process exited without a result "
                    f"(exitcode={exitcode})"
                ),
                pid=pid,
                exitcode=exitcode,
                force_terminated=force_terminated,
            )
        if exitcode not in (0, None) and not force_terminated:
            return _IsolatedProcessResult(
                state="crashed",
                message=f"isolated quality process exited with code {exitcode}",
                pid=pid,
                exitcode=exitcode,
            )
        return _IsolatedProcessResult(
            state="completed",
            payload=payload,
            pid=pid,
            exitcode=exitcode,
            force_terminated=force_terminated,
        )
    finally:
        try:
            receive_connection.close()
        except Exception:
            pass
        if not process.is_alive():
            try:
                process.close()
            except Exception:
                pass


def _quality_mode_process_entry(
    send_connection,
    org: str,
    sup: str,
    table_name: str,
    mode: str,
    running_key: str,
    lock_token: str,
) -> None:
    """Recreate process-local clients and execute one fenced quality mode."""

    from supertable.quality.config import DQConfig
    from supertable.redis_connector import (
        close_all_redis_clients,
        create_redis_client,
    )

    outcome: CheckRunOutcome
    try:
        redis_client = create_redis_client()
        config = DQConfig(redis_client, org, sup)
        guard = _LeaseGuard(redis_client, running_key, lock_token)
        runners = {
            "quick": _run_quick_check,
            "deep": _run_deep_check,
            "custom": _run_custom_check,
        }
        outcome = runners[mode](
            redis_client,
            org,
            sup,
            table_name,
            config,
            lease_guard=guard,
        )
    except BaseException as exc:
        outcome = _failed(
            mode,
            f"isolated quality execution failed: {type(exc).__name__}: {exc}",
        )
    finally:
        try:
            close_all_redis_clients()
        except Exception:
            pass
    send_connection.send(("result", outcome))


def _run_isolated_mode_check(
    *,
    org: str,
    sup: str,
    table_name: str,
    mode: str,
    running_key: str,
    lock_token: str,
    deadline_monotonic: Optional[float],
    cancel_event: Optional[Any],
    lease_guard: _LeaseGuard,
) -> CheckRunOutcome:
    """Run one scheduled check behind a killable OS-process boundary."""

    result = _run_killable_subprocess(
        _quality_mode_process_entry,
        (org, sup, table_name, mode, running_key, lock_token),
        deadline_monotonic=deadline_monotonic,
        cancel_event=cancel_event,
        lease_lost_event=lease_guard.lost,
        process_name=f"supertable-dq-{mode}-{table_name}",
    )
    if result.force_terminated:
        _stats_increment("jobs_force_terminated")
    if result.state == "completed" and isinstance(result.payload, CheckRunOutcome):
        return result.payload
    messages = {
        "deadline": "quality job deadline exceeded",
        "shutdown": "quality scheduler shut down during execution",
        "lease_lost": "quality execution lease was lost during execution",
        "termination_failed": "quality subprocess could not be terminated safely",
    }
    return _failed(mode, messages.get(result.state, result.message or result.state))


def _scheduler_loop() -> None:
    """Coordinate bounded fair table jobs until bounded shutdown."""
    global _scheduler_executor, _scheduler_slots
    if _scheduler_stop.wait(10.0):
        return
    last_quick_run: Dict[str, float] = {}
    last_deep_run: Dict[str, float] = {}
    last_custom_run: Dict[str, float] = {}
    executor = ThreadPoolExecutor(
        max_workers=DEFAULT_MAX_WORKERS,
        thread_name_prefix="supertable-dq-job",
    )
    slots = threading.BoundedSemaphore(DEFAULT_MAX_WORKERS * 2)
    with _scheduler_lock:
        _scheduler_executor = executor
        _scheduler_slots = slots
    _stats_update(running=True)
    try:
        while not _scheduler_stop.is_set():
            started = datetime.now(timezone.utc).isoformat()
            _stats_update(last_tick_started_at=started)
            _stats_increment("ticks")
            try:
                _scheduler_tick(
                    last_quick_run,
                    last_deep_run,
                    last_custom_run,
                    executor=executor,
                    slots=slots,
                )
            except Exception:
                _stats_increment("tick_failures")
                logger.error(
                    f"[dq-scheduler] Error in tick:\n{traceback.format_exc()}"
                )
            _stats_update(
                last_tick_completed_at=datetime.now(timezone.utc).isoformat(),
            )
            if _scheduler_stop.wait(TICK_INTERVAL_SECONDS):
                break
    finally:
        # Queued futures have not acquired a table lease yet and are safe to
        # cancel. Running checks observe the stop/deadline fence before any
        # result publication; do not hold host shutdown forever on a driver.
        executor.shutdown(wait=False, cancel_futures=True)
        with _scheduler_lock:
            _scheduler_executor = None
            _scheduler_slots = None
        _stats_update(running=False)


def start_scheduler() -> bool:
    """Start the Data Quality background scheduler (idempotent).

    Launches :func:`_scheduler_loop` in a single daemon thread.  The thread
    drains post-ingest "pending" flags and runs cron-based checks, respecting
    the debounce / lock / cooldown rules documented at the top of this module.

    Call this once during host-application startup (e.g. a FastAPI lifespan
    hook).  Calling it again is a safe no-op while a thread is already alive.

    Returns:
        ``True`` if this call started the thread, ``False`` if one was already
        running.
    """
    global _scheduler_thread
    with _scheduler_lock:
        if _scheduler_thread is not None and _scheduler_thread.is_alive():
            logger.debug("[dq-scheduler] Already running; start_scheduler() is a no-op")
            return False
        with _active_jobs_lock:
            if _active_jobs:
                logger.error(
                    "[dq-scheduler] Refusing restart while %d prior table job(s) "
                    "remain active",
                    len(_active_jobs),
                )
                return False
        _scheduler_stop.clear()
        with _scheduler_stats_lock:
            for key in tuple(_scheduler_stats):
                if key in {"running"}:
                    _scheduler_stats[key] = False
                elif key.startswith("last_"):
                    _scheduler_stats[key] = None
                else:
                    _scheduler_stats[key] = 0
        t = threading.Thread(
            target=_scheduler_loop,
            name="supertable-dq-scheduler",
            daemon=True,
        )
        _scheduler_thread = t
        t.start()
        logger.info("[dq-scheduler] Background scheduler started")
        return True


def stop_scheduler(timeout_s: float = 10.0) -> bool:
    """Request shutdown and await coordinator *and* jobs within ``timeout_s``.

    The return value is deliberately strict: ``True`` means no scheduler table
    job remains. Scheduled runners that ignore cooperative cancellation are
    terminated at their process boundary; ``False`` means shutdown could not
    confirm coordinator/job exit within the caller's bound.
    """
    global _scheduler_thread
    if isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float)):
        raise ValueError("timeout_s must be a non-negative finite number")
    timeout = float(timeout_s)
    if not math.isfinite(timeout) or timeout < 0:
        raise ValueError("timeout_s must be a non-negative finite number")
    deadline = time.monotonic() + timeout
    _scheduler_stop.set()
    with _scheduler_lock:
        thread = _scheduler_thread
    if thread is not None and thread is not threading.current_thread():
        thread.join(timeout=max(0.0, deadline - time.monotonic()))
    coordinator_stopped = thread is None or not thread.is_alive()
    with _active_jobs_condition:
        while _active_jobs:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            _active_jobs_condition.wait(timeout=remaining)
        jobs_stopped = not _active_jobs
    stopped = coordinator_stopped and jobs_stopped
    if stopped:
        with _scheduler_lock:
            if _scheduler_thread is thread:
                _scheduler_thread = None
    _stats_update(running=not stopped)
    return stopped


def _scheduler_tick(
    last_quick_run: Dict[str, float],
    last_deep_run: Dict[str, float],
    last_custom_run: Dict[str, float],
    *,
    executor: Optional[ThreadPoolExecutor] = None,
    slots: Optional[threading.BoundedSemaphore] = None,
) -> None:
    """Single scheduler tick — process cron-based and pending (post-ingest) checks."""
    try:
        from supertable.quality.config import DQConfig, DQConfigReadError
        from supertable.redis_connector import create_redis_client
    except ImportError:
        logger.debug("[dq-scheduler] Dependencies not available yet, skipping tick")
        return

    try:
        r = create_redis_client()
    except Exception as e:
        logger.warning(f"[dq-scheduler] Cannot connect to Redis: {e}")
        return

    pairs = _discover_dq_pairs(r)
    if not pairs:
        return

    now = time.time()
    jobs: List[Tuple[Any, ...]] = []
    for org, sup in sorted(pairs):
        dqc = DQConfig(r, org, sup)
        _drain_history_outbox(r, org, sup)
        try:
            schedule = dqc.get_schedule()
            cooldown_sec = int(
                schedule.get("cooldown_seconds", DEFAULT_COOLDOWN_SECONDS)
            )
        except (DQConfigReadError, TypeError, ValueError, OverflowError) as exc:
            message = f"Global quality schedule could not be read safely: {exc}"
            for table_name in _list_tables(r, org, sup):
                _record_tick_config_failure(
                    r,
                    org,
                    sup,
                    table_name,
                    dqc,
                    QUALITY_MODES,
                    DEFAULT_COOLDOWN_SECONDS,
                    message,
                )
            continue

        quick_cron = schedule.get("quick_cron", "0 */4 * * *")
        deep_cron = schedule.get("deep_cron", "0 2 * * *")
        custom_cron = schedule.get("custom_cron", "0 */6 * * *")
        tables = _list_tables(r, org, sup)

        if not schedule.get("enabled", True):
            for table_name in tables:
                _resolve_unresolved_pending(
                    r, org, sup, table_name, (),
                )
            continue
        for table_name in tables:
            jobs.append((
                r, org, sup, table_name, dqc, schedule, cooldown_sec,
                quick_cron, deep_cron, custom_cron, now,
                last_quick_run, last_deep_run, last_custom_run,
            ))

    # Rotate the already pair/table-sorted list every tick. Under sustained
    # capacity pressure, no tenant/table remains permanently at the tail.
    global _scheduler_fair_cursor
    if jobs:
        offset = _scheduler_fair_cursor % len(jobs)
        jobs = jobs[offset:] + jobs[:offset]
        _scheduler_fair_cursor = (offset + 1) % len(jobs)
    for job in jobs:
        job_key = f"{job[1]}:{job[2]}:{job[3]}"
        deadline = time.monotonic() + DEFAULT_JOB_DEADLINE_SECONDS
        if executor is None:
            _process_table_job(*job, deadline, None, False)
        else:
            if slots is None:
                raise RuntimeError("bounded scheduler slots are required")
            _submit_table_job(
                executor,
                slots,
                job_key,
                _process_table_job,
                *job,
                deadline,
                _scheduler_stop,
                True,
            )


def _process_table_job(
    r,
    org: str,
    sup: str,
    table_name: str,
    dqc,
    schedule: Dict[str, Any],
    cooldown_sec: int,
    quick_cron: str,
    deep_cron: str,
    custom_cron: str,
    now: float,
    last_quick_run: Dict[str, float],
    last_deep_run: Dict[str, float],
    last_custom_run: Dict[str, float],
    deadline_monotonic: float,
    cancel_event: Optional[threading.Event],
    isolate_runners: bool = False,
) -> None:
    """Run one table's serial modes inside the bounded table worker pool."""
    from supertable.quality.config import DQConfigReadError

    tkey = f"{org}:{sup}:{table_name}"
    if cancel_event is not None and cancel_event.is_set():
        _stats_increment("jobs_cancelled")
        return
    try:
        ts = dqc.get_table_schedule(table_name)
        table_schedule = ts or {}
        table_cooldown_sec = int(
            table_schedule.get("cooldown_seconds", cooldown_sec)
        )
    except (DQConfigReadError, TypeError, ValueError, OverflowError) as exc:
        _record_tick_config_failure(
            r, org, sup, table_name, dqc, QUALITY_MODES, cooldown_sec,
            f"Table quality schedule could not be read safely: {exc}",
        )
        return
    if ts and not ts.get("enabled", True):
        _resolve_unresolved_pending(r, org, sup, table_name, ())
        return
    timezone_name = str(
        table_schedule.get("timezone") or schedule.get("timezone") or "UTC"
    )
    post_ingest_modes = _post_ingest_modes(schedule, table_schedule)
    _resolve_unresolved_pending(r, org, sup, table_name, post_ingest_modes)
    _migrate_legacy_pending(r, org, sup, table_name, post_ingest_modes)
    pending_generations = {
        mode: r.get(_pending_key(org, sup, table_name, mode))
        for mode in QUALITY_MODES
    }
    handled_pending = {
        mode for mode, generation in pending_generations.items()
        if generation is not None
    }
    last_run_maps = {
        "quick": last_quick_run,
        "deep": last_deep_run,
        "custom": last_custom_run,
    }
    for mode in QUALITY_MODES:
        if cancel_event is not None and cancel_event.is_set():
            _stats_increment("jobs_cancelled")
            return
        generation = pending_generations[mode]
        if generation is None:
            continue
        outcome = _try_run_check(
            r, org, sup, table_name, mode, dqc, table_cooldown_sec,
            pending_generation=generation,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            isolate_runner=isolate_runners,
        )
        if outcome.successful:
            last_run_maps[mode][tkey] = now

    table_quick_cron = (ts or {}).get("quick_cron") or quick_cron
    quick_due, quick_state = _cron_schedule_state(
        r, org, sup, table_name, "quick", table_quick_cron,
        timezone_name, int(now * 1000),
    )
    if "quick" not in handled_pending and quick_due:
        outcome = _try_run_check(
            r, org, sup, table_name, "quick", dqc, table_cooldown_sec,
            deadline_monotonic=deadline_monotonic,
            cancel_event=cancel_event,
            isolate_runner=isolate_runners,
        )
        if outcome.executed:
            _record_cron_outcome(
                r, org, sup, table_name, "quick", quick_state,
                outcome, int(time.time() * 1000),
            )
        if outcome.successful:
            last_quick_run[tkey] = now

    table_deep_cron = (ts or {}).get("deep_cron") or deep_cron
    deep_enabled = (ts or {}).get("deep_enabled", True)
    deep_due, deep_state = _cron_schedule_state(
        r, org, sup, table_name, "deep", table_deep_cron,
        timezone_name, int(now * 1000),
    )
    if deep_enabled and "deep" not in handled_pending and deep_due:
        try:
            eff_config = dqc.get_effective_config(table_name)
            has_deep = any(
                value.get("enabled")
                for key, value in (eff_config.get("checks") or {}).items()
                if key.startswith("D")
            )
            stale_deep = _has_stale_mode_result(dqc, table_name, "deep")
        except DQConfigReadError as exc:
            _record_tick_config_failure(
                r, org, sup, table_name, dqc, ("deep",),
                table_cooldown_sec,
                f"Deep quality config could not be read safely: {exc}",
            )
            has_deep = stale_deep = False
        if has_deep or stale_deep:
            outcome = _try_run_check(
                r, org, sup, table_name, "deep", dqc, table_cooldown_sec,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                isolate_runner=isolate_runners,
            )
            if outcome.executed:
                _record_cron_outcome(
                    r, org, sup, table_name, "deep", deep_state,
                    outcome, int(time.time() * 1000),
                )
            if outcome.successful:
                last_deep_run[tkey] = now

    table_custom_cron = (ts or {}).get("custom_cron") or custom_cron
    custom_enabled = (ts or {}).get("custom_enabled", True)
    custom_due, custom_state = _cron_schedule_state(
        r, org, sup, table_name, "custom", table_custom_cron,
        timezone_name, int(now * 1000),
    )
    if custom_enabled and "custom" not in handled_pending and custom_due:
        try:
            has_rules = bool(dqc.list_rules_for_table(table_name))
            stale_custom = _has_stale_mode_result(dqc, table_name, "custom")
        except DQConfigReadError as exc:
            _record_tick_config_failure(
                r, org, sup, table_name, dqc, ("custom",),
                table_cooldown_sec,
                f"Custom quality rules could not be read safely: {exc}",
            )
            has_rules = stale_custom = False
        if has_rules or stale_custom:
            outcome = _try_run_check(
                r, org, sup, table_name, "custom", dqc, table_cooldown_sec,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                isolate_runner=isolate_runners,
            )
            if outcome.executed:
                _record_cron_outcome(
                    r, org, sup, table_name, "custom", custom_state,
                    outcome, int(time.time() * 1000),
                )
            if outcome.successful:
                last_custom_run[tkey] = now


def _record_tick_config_failure(
    r,
    org: str,
    sup: str,
    table_name: str,
    dqc,
    modes: Tuple[str, ...],
    cooldown_sec: int,
    message: str,
) -> None:
    """Turn a tick-time config uncertainty into normal failed attempts."""

    for mode in modes:
        try:
            _try_run_check(
                r,
                org,
                sup,
                table_name,
                mode,
                dqc,
                cooldown_sec,
                forced_failure=message,
            )
        except Exception as exc:
            logger.error(
                "[dq-scheduler] Could not record %s config failure for %s: %s",
                mode,
                table_name,
                exc,
            )


def _has_stale_mode_result(dqc, table_name: str, mode: str) -> bool:
    """Whether an empty mode run is needed to retire a prior result."""

    latest = dqc.get_latest(table_name) or {}
    mode_results = latest.get("mode_results")
    if isinstance(mode_results, dict):
        record = mode_results.get(mode)
        if isinstance(record, dict):
            if (
                record.get("disabled") is True
                and record.get("total_checks", 0) in (None, 0, "0")
            ):
                return False
            return bool(
                record.get("checked_at")
                or record.get("total_checks")
                or record.get("outcomes")
                or record.get("anomalies")
                or record.get("rule_results")
            )
    # Rolling-upgrade documents predate mode_results.
    if mode == "custom":
        return bool(latest.get("custom_checked_at") or latest.get("rule_results"))
    if mode == "deep":
        return bool(latest.get("deep_checked_at"))
    return False


def _publish_failure_and_retry(
    r,
    dqc,
    table_name: str,
    outcome: CheckRunOutcome,
    retry_key: str,
    lock_token: str,
    lease_guard: _LeaseGuard,
    cooldown_sec: int,
    *,
    completion_event: Optional[threading.Event] = None,
) -> None:
    """Record a failed attempt when safe, but never overwrite on read doubt."""

    try:
        published = _publish_mode_attempt(
            dqc,
            table_name,
            outcome,
            lease_guard=lease_guard,
        )
        if not published:
            logger.error(
                "[dq-scheduler] Failed-attempt telemetry was not persisted for %s",
                table_name,
            )
    except _LeaseLostError:
        raise
    except Exception as exc:
        # In particular, a strict get_latest failure means the prior merged
        # document is unknowable. Never replace it with a synthetic failure
        # document; the retry key remains safe and does not touch that state.
        logger.error(
            "[dq-scheduler] Could not safely publish failed-attempt telemetry "
            "for %s: %s",
            table_name,
            exc,
        )
    _set_retry_if_owned(
        r,
        retry_key,
        lock_token,
        lease_guard,
        cooldown_sec,
        completion_event=completion_event,
    )


def _expire_job_if_owned(
    r,
    *,
    running_key: str,
    retry_key: str,
    lock_token: str,
    cooldown_sec: int,
    reason: str,
) -> bool:
    """Atomically make an unfinished job replayable and release its lease.

    There is intentionally no GET/DEL compatibility fallback. An ambiguous
    Redis response must never delete a successor's lease. In that case the
    current worker is still fenced locally and Redis' running-key TTL remains
    the final recovery bound.
    """

    retry_seconds = max(
        1,
        min(DEFAULT_RETRY_BACKOFF_SECONDS, max(1, int(cooldown_sec))),
    )
    script = """
    if redis.call('get', KEYS[1]) ~= ARGV[1] then
        return 0
    end
    redis.call('set', KEYS[2], ARGV[2], 'EX', ARGV[3])
    redis.call('del', KEYS[1])
    return 1
    """
    try:
        return bool(r.eval(
            script,
            2,
            running_key,
            retry_key,
            lock_token,
            f"{_now_iso()}:{reason}",
            retry_seconds,
        ))
    except Exception:
        return False


def _try_run_check(
    r,
    org: str,
    sup: str,
    table_name: str,
    mode: str,
    dqc,
    cooldown_sec: int,
    *,
    pending_generation: Any = None,
    forced_failure: Optional[str] = None,
    deadline_monotonic: Optional[float] = None,
    cancel_event: Optional[threading.Event] = None,
    isolate_runner: bool = False,
) -> CheckRunOutcome:
    """
    Attempt to run a quality check with lock + cooldown protection.

    Returns a structured outcome.  It is truthy only for complete success.
    """
    if mode not in QUALITY_MODES:
        return _failed(mode, f"unknown quality mode: {mode}")
    if deadline_monotonic is not None:
        if (
            isinstance(deadline_monotonic, bool)
            or not isinstance(deadline_monotonic, (int, float))
            or not math.isfinite(float(deadline_monotonic))
        ):
            return _failed(mode, "quality job deadline is invalid")
        if time.monotonic() >= float(deadline_monotonic):
            _stats_increment("deadline_exceeded")
            return _failed(mode, "quality job deadline expired before execution")
    if cancel_event is not None and cancel_event.is_set():
        _stats_increment("jobs_cancelled")
        return _failed(mode, "quality scheduler is shutting down")

    running_key = _running_key(org, sup, table_name)
    cooldown_key = _cooldown_key(org, sup, table_name, mode)
    retry_key = _retry_key(org, sup, table_name, mode)

    # ── Check cooldown ────────────────────────────────────────
    if forced_failure is None and r.exists(cooldown_key):
        ttl = r.ttl(cooldown_key)
        logger.debug(
            f"[dq-scheduler] Skipping {table_name} ({mode}): "
            f"cooldown active, {ttl}s remaining"
        )
        return _skipped(mode, "success cooldown active")

    if r.exists(retry_key):
        ttl = r.ttl(retry_key)
        logger.debug(
            f"[dq-scheduler] Skipping {table_name} ({mode}): "
            f"retry backoff active, {ttl}s remaining"
        )
        return _skipped(mode, "failure retry backoff active")

    # ── Acquire lock (SET NX = only if not exists) ────────────
    lock_token = f"{_now_iso()}:{uuid.uuid4().hex}"
    acquired = r.set(
        running_key,
        lock_token,
        nx=True,                          # only set if key does not exist
        ex=DEFAULT_RUNNING_TTL_SECONDS,   # safety TTL in case of crash
    )
    if not acquired:
        logger.debug(
            f"[dq-scheduler] Skipping {table_name} ({mode}): "
            f"another check is already running"
        )
        return _skipped(mode, "table execution lock busy")

    lease_stop = threading.Event()
    lease_guard = _LeaseGuard(r, running_key, lock_token)
    lease_thread: Optional[threading.Thread] = None
    completion_event = threading.Event()
    deadline_fired = threading.Event()
    deadline_timer: Optional[threading.Timer] = None
    cancel_watch_stop = threading.Event()
    cancel_watch_thread: Optional[threading.Thread] = None

    def expire_for(reason: str, *, deadline: bool) -> None:
        with lease_guard.fence_lock:
            if completion_event.is_set() or lease_guard.lost.is_set():
                return
            released = _expire_job_if_owned(
                r,
                running_key=running_key,
                retry_key=retry_key,
                lock_token=lock_token,
                cooldown_sec=cooldown_sec,
                reason=reason,
            )
            # Even an ambiguous Redis response is terminal locally. A late
            # worker can no longer publish, and the original lease TTL safely
            # resolves uncertainty without risking a successor lease.
            lease_guard.lost.set()
            if deadline:
                deadline_fired.set()
                _stats_increment("deadline_exceeded")
            else:
                _stats_increment("jobs_cancelled")
            logger.warning(
                "[dq-scheduler] %s fenced for %s/%s/%s (%s; lease_released=%s)",
                mode,
                org,
                sup,
                table_name,
                reason,
                released,
            )
    try:
        lease_thread = threading.Thread(
            target=_renew_running_lease,
            args=(
                r,
                running_key,
                lock_token,
                lease_stop,
                lease_guard.lost,
                lease_guard.fence_lock,
            ),
            name=f"dq-lock-renew-{mode}-{table_name}"[:120],
            daemon=True,
        )
        lease_thread.start()
        if deadline_monotonic is not None:
            remaining = max(0.0, float(deadline_monotonic) - time.monotonic())
            deadline_timer = threading.Timer(
                remaining,
                expire_for,
                kwargs={"reason": "deadline exceeded", "deadline": True},
            )
            deadline_timer.name = f"dq-deadline-{mode}-{table_name}"[:120]
            deadline_timer.daemon = True
            deadline_timer.start()
        if cancel_event is not None:
            def watch_cancellation() -> None:
                while not cancel_watch_stop.wait(0.05):
                    if cancel_event.is_set():
                        expire_for("scheduler shutdown", deadline=False)
                        return

            cancel_watch_thread = threading.Thread(
                target=watch_cancellation,
                name=f"dq-cancel-{mode}-{table_name}"[:120],
                daemon=True,
            )
            cancel_watch_thread.start()
    except Exception as exc:
        # Do not execute without an active renewer: the query may outlive the
        # initial TTL. Release only our token and expose the failed attempt.
        completion_event.set()
        if deadline_timer is not None:
            deadline_timer.cancel()
        cancel_watch_stop.set()
        cancel_join = getattr(cancel_watch_thread, "join", None)
        if callable(cancel_join):
            cancel_join(timeout=0.2)
        lease_stop.set()
        lease_join = getattr(lease_thread, "join", None)
        if callable(lease_join):
            lease_join(timeout=1.0)
        outcome = _failed(mode, f"could not start lease renewer: {exc}")
        try:
            _publish_failure_and_retry(
                r,
                dqc,
                table_name,
                outcome,
                retry_key,
                lock_token,
                lease_guard,
                cooldown_sec,
            )
        except _LeaseLostError:
            pass
        _delete_if_value(r, running_key, lock_token)
        return outcome

    # ── Execute the check ─────────────────────────────────────
    try:
        logger.info(f"[dq-scheduler] Running {mode} check: {org}/{sup}/{table_name}")

        if forced_failure is not None:
            outcome = _failed(mode, forced_failure)
        elif isolate_runner:
            outcome = _run_isolated_mode_check(
                org=org,
                sup=sup,
                table_name=table_name,
                mode=mode,
                running_key=running_key,
                lock_token=lock_token,
                deadline_monotonic=deadline_monotonic,
                cancel_event=cancel_event,
                lease_guard=lease_guard,
            )
        else:
            runners = {
                "quick": _run_quick_check,
                "deep": _run_deep_check,
                "custom": _run_custom_check,
            }
            outcome = runners[mode](
                r,
                org,
                sup,
                table_name,
                dqc,
                lease_guard=lease_guard,
            )
        if cancel_event is not None and cancel_event.is_set():
            expire_for("scheduler shutdown", deadline=False)
            return _failed(mode, "quality scheduler shut down during execution")
        if (
            deadline_monotonic is not None
            and time.monotonic() >= float(deadline_monotonic)
        ):
            expire_for("deadline exceeded", deadline=True)
            return _failed(mode, "quality job deadline exceeded")
        if deadline_fired.is_set():
            return _failed(mode, "quality job deadline exceeded")
        if not isinstance(outcome, CheckRunOutcome):
            outcome = _failed(
                mode,
                "quality runner returned no structured completion outcome",
            )

        if outcome.successful:
            lease_guard.assert_owned()
            # Publish/clear the mode's attempt marker before success can earn
            # cooldown or consume pending work.  If ownership disappears after
            # finalization there must not be a stale prior failure left in
            # ``latest`` while the generation has already been removed.
            if not _publish_mode_attempt(
                dqc,
                table_name,
                outcome,
                lease_guard=lease_guard,
            ):
                failed_outcome = _failed(
                    mode,
                    "quality result completed but success state could not be published",
                )
                _set_retry_if_owned(
                    r, retry_key, lock_token, lease_guard, cooldown_sec,
                    completion_event=completion_event,
                )
                return failed_outcome

            # Ownership check, success cooldown, retry deletion and optional
            # pending consumption are one Redis operation in production.
            # Thus an expired/reacquired worker cannot announce success or
            # erase a newer ingest generation.
            if not _finalize_success_if_owned(
                r,
                lease_guard,
                cooldown_key,
                retry_key,
                cooldown_sec,
                pending_key=(
                    _pending_key(org, sup, table_name, mode)
                    if pending_generation is not None else None
                ),
                pending_generation=pending_generation,
                completion_event=completion_event,
            ):
                lease_guard.mark_lost()
                return _failed(
                    mode,
                    "quality execution lease was lost before success finalization",
                )
        elif outcome.state == "failed":
            lease_guard.assert_owned()
            _publish_failure_and_retry(
                r,
                dqc,
                table_name,
                outcome,
                retry_key,
                lock_token,
                lease_guard,
                cooldown_sec,
                completion_event=completion_event,
            )
            logger.error(
                "[dq-scheduler] %s check failed for %s: %s",
                mode,
                table_name,
                outcome.message,
            )
        return outcome

    except _LeaseLostError as exc:
        logger.error(
            "[dq-scheduler] %s check lost its lease for %s: %s",
            mode,
            table_name,
            exc,
        )
        # Fail closed: a worker with ambiguous ownership may not publish an
        # attempt, set cooldown/backoff, or consume pending work.
        return _failed(mode, str(exc))
    except Exception as exc:
        logger.error(
            f"[dq-scheduler] {mode} check failed for {table_name}:\n"
            f"{traceback.format_exc()}"
        )
        outcome = _failed(mode, str(exc))
        try:
            lease_guard.assert_owned()
            _publish_failure_and_retry(
                r,
                dqc,
                table_name,
                outcome,
                retry_key,
                lock_token,
                lease_guard,
                cooldown_sec,
                completion_event=completion_event,
            )
        except _LeaseLostError:
            pass
        return outcome

    finally:
        # Stop renewal before releasing.  Both renewal and release are
        # ownership-checked, so an expired/reacquired lease is never damaged.
        lease_stop.set()
        # Mark all non-deadline terminal paths complete while holding the same
        # fence used by the timer, then cancel it. This prevents a timer queued
        # at the boundary from creating a retry after successful finalization.
        with lease_guard.fence_lock:
            completion_event.set()
        if deadline_timer is not None:
            deadline_timer.cancel()
        cancel_watch_stop.set()
        cancel_join = getattr(cancel_watch_thread, "join", None)
        if callable(cancel_join):
            cancel_join(timeout=0.2)
        lease_join = getattr(lease_thread, "join", None)
        if callable(lease_join):
            lease_join(timeout=1.0)
        _delete_if_value(r, running_key, lock_token)


# ──────────────────────────────────────────────────────────────────────
# Check execution
# ──────────────────────────────────────────────────────────────────────


def _execute_quality_statement(
    org: str,
    sup: str,
    sql: str,
    *,
    allow_bounded_collection_aggregates: bool = False,
):
    """Execute through the AUTO certification boundary used by all DQ SQL."""

    from supertable.quality.execution import execute_quality_sql

    return execute_quality_sql(
        organization=org,
        super_name=sup,
        sql=sql,
        role_name="superadmin",
        allow_bounded_collection_aggregates=(
            allow_bounded_collection_aggregates
        ),
    )


def _summary_from_outcomes(outcomes: List[Dict[str, Any]]) -> Dict[str, int]:
    """Count mutually exclusive outcome states without inventing passes."""

    try:
        from supertable.quality.anomaly import summarize_check_outcomes

        authoritative = summarize_check_outcomes(outcomes)
        return {
            "total_checks": int(authoritative["configured"]),
            "evaluated": int(authoritative["passed"])
            + int(authoritative["warnings"])
            + int(authoritative["critical"]),
            "passed": int(authoritative["passed"]),
            "warnings": int(authoritative["warnings"]),
            "critical": int(authoritative["critical"]),
            "errors": int(authoritative["errors"]),
            "skipped": int(authoritative["skipped"]),
            "not_applicable": int(authoritative["not_applicable"]),
        }
    except (ImportError, KeyError, TypeError, ValueError):
        # Keep the scheduler importable during rolling upgrades.  This fallback
        # uses the same documented invariant as summarize_check_outcomes.
        pass

    counts = {
        "passed": 0,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
    }
    for outcome in outcomes:
        status = str(outcome.get("status", "error")).lower()
        evaluated = outcome.get("evaluated", outcome.get("applicable", True)) is True
        if status == "error":
            counts["errors"] += 1
        elif not evaluated or status in {"skipped", "not_applicable"}:
            counts["skipped"] += 1
        elif status == "ok":
            counts["passed"] += 1
        elif status == "warning":
            counts["warnings"] += 1
        elif status == "critical":
            counts["critical"] += 1
        else:
            # An unknown severity/configuration is not a pass.
            counts["errors"] += 1
    counts["evaluated"] = (
        counts["passed"] + counts["warnings"] + counts["critical"]
    )
    counts["total_checks"] = (
        counts["evaluated"] + counts["errors"] + counts["skipped"]
    )
    counts["not_applicable"] = sum(
        1 for outcome in outcomes
        if outcome.get("status") == "not_applicable"
        or outcome.get("applicable") is False
    )
    return counts


def _outcome_to_anomaly(outcome: Dict[str, Any], *, prefix: str = "") -> Dict[str, Any]:
    return {
        "check_id": f"{prefix}{outcome.get('check_id', '')}",
        "check_name": outcome.get("check_name") or outcome.get("check_id", "Quality check"),
        "column": outcome.get("column"),
        "severity": outcome.get("status", "warning"),
        "message": outcome.get("message") or outcome.get("detail", ""),
        "value": outcome.get("value"),
        "threshold": outcome.get("threshold"),
        "detected_at": _now_iso(),
    }


_ANOMALY_CHECK_IDS = {
    "A1": "T1",
    "A_T3": "T3",
    "A2": "C1",
    "A4": "C2",
    "A5": "C3",
    "A5_C5": "C5",
    "A3": "C6",
}


def _quick_profile_outcomes(
    parsed: Dict[str, Any],
    previous: Optional[Dict[str, Any]],
    columns: List[Tuple[str, str]],
    checks: Dict[str, Any],
    anomalies: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Describe every enabled SQL-backed quick evaluation truthfully.

    T1/T3 are table evaluations.  C1-C6 are column evaluations: a usable
    baseline on one column must never certify, pass, or warn every other
    column.  Checks with no eligible visible column retain one explicit N/A
    sentinel so the configured inventory and counters remain reconcilable.
    """

    # Reuse anomaly comparison semantics so readiness cannot label a corrupt
    # date boundary as an evaluated/passable C3 baseline.
    from supertable.quality.anomaly import _exact_number, _parse_instant
    from supertable.quality.checker import _col_category

    previous_parsed = (previous or {}).get("parsed")
    previous_columns = (
        previous_parsed.get("columns", {})
        if isinstance(previous_parsed, dict)
        else {}
    )
    current_columns = parsed.get("columns", {}) or {}
    visible: List[Tuple[str, str]] = []
    for column_name, column_type in columns:
        # The sealed visible schema determines applicability.  A malformed or
        # missing parsed entry must become a skipped current metric, not make
        # an otherwise applicable column disappear into the N/A sentinel.
        visible.append((column_name, _col_category(column_type)))

    def valid_number(value: Any, *, minimum: Optional[float] = None) -> bool:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return False
        try:
            numeric = float(value)
        except (TypeError, ValueError, OverflowError):
            return False
        if not math.isfinite(numeric):
            return False
        return minimum is None or numeric >= minimum

    def valid_rate(value: Any) -> bool:
        return valid_number(value, minimum=0) and float(value) <= 100

    def valid_count(value: Any) -> bool:
        if not valid_number(value, minimum=0):
            return False
        return float(value).is_integer()

    def valid_extremum(value: Any, category: str) -> bool:
        if value is None or isinstance(value, bool):
            return False
        if category == "numeric":
            return _exact_number(value) is not None
        if category == "date":
            return _parse_instant(value) is not None
        return False

    matching: Dict[Tuple[str, Optional[str]], List[Dict[str, Any]]] = {}
    for anomaly in anomalies:
        check_id = _ANOMALY_CHECK_IDS.get(str(anomaly.get("check_id", "")))
        if check_id:
            column = anomaly.get("column")
            matching.setdefault((check_id, column), []).append(anomaly)

    def evaluated_outcome(
        check_id: str,
        config: Dict[str, Any],
        column: Optional[str],
    ) -> Dict[str, Any]:
        issues = matching.get((check_id, column), [])
        status = "ok"
        if any(issue.get("severity") == "critical" for issue in issues):
            status = "critical"
        elif issues:
            status = "warning"
        subject = f" for {column}" if column is not None else ""
        return {
            "check_id": check_id,
            "status": status,
            "applicable": True,
            "evaluated": True,
            "message": "; ".join(
                str(issue.get("message", "")) for issue in issues
            ) if issues else f"{check_id} passed{subject}",
            "threshold": config.get("threshold"),
            "column": column,
        }

    def skipped_outcome(
        check_id: str,
        config: Dict[str, Any],
        column: Optional[str],
        reason: str,
    ) -> Dict[str, Any]:
        subject = f" on {column}" if column is not None else ""
        if reason == "uncertified_precision":
            message = (
                f"{check_id}{subject} skipped because numeric moment "
                "precision is not certified"
            )
        elif reason == "no_finite_values":
            message = f"{check_id}{subject} has no finite values to compare"
        elif reason == "missing_current_metric":
            message = f"{check_id}{subject} has no valid current metric"
        else:
            message = (
                f"Baseline recorded for {check_id}{subject}; comparison "
                "starts on the next run"
            )
        return {
            "check_id": check_id,
            "status": "skipped",
            "applicable": True,
            "evaluated": False,
            "message": message,
            "threshold": config.get("threshold"),
            "column": column,
            "reason": reason,
        }

    def not_applicable_outcome(
        check_id: str,
        config: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "check_id": check_id,
            "status": "not_applicable",
            "applicable": False,
            "evaluated": False,
            "message": f"{check_id} has no applicable visible columns",
            "threshold": config.get("threshold"),
            "column": None,
            "reason": "unsupported_column_type",
        }

    outcomes: List[Dict[str, Any]] = []

    # Table checks remain one outcome each.
    for check_id in ("T1", "T3"):
        config = checks.get(check_id)
        if not isinstance(config, dict) or not config.get("enabled"):
            continue
        if check_id == "T1":
            if not valid_count(parsed.get("total")):
                outcomes.append(skipped_outcome(
                    check_id, config, None, "missing_current_metric",
                ))
                continue
            baseline_ready = bool(
                previous_parsed
                and valid_count(previous_parsed.get("total"))
                and float(previous_parsed["total"]) > 0
            )
        else:
            # An empty public schema is a valid, observed baseline.  Presence
            # of the schema field—not its truthiness—determines readiness.
            baseline_ready = isinstance((previous or {}).get("schema"), list)
        if not baseline_ready:
            outcomes.append(skipped_outcome(
                check_id, config, None, "baseline_unavailable",
            ))
            continue
        outcomes.append(evaluated_outcome(check_id, config, None))

    applicable_categories = {
        "C1": None,
        "C2": None,
        "C3": {"numeric", "date"},
        "C4": None,
        "C5": {"numeric"},
        "C6": {"numeric"},
    }
    for check_id in ("C1", "C2", "C3", "C4", "C5", "C6"):
        config = checks.get(check_id)
        if not isinstance(config, dict) or not config.get("enabled"):
            continue
        supported = applicable_categories[check_id]
        eligible = [
            (column_name, category)
            for column_name, category in visible
            if supported is None or category in supported
        ]
        if not eligible:
            outcomes.append(not_applicable_outcome(check_id, config))
            continue

        for column_name, category in eligible:
            current = current_columns.get(column_name)
            prior = previous_columns.get(column_name)
            reason: Optional[str] = None
            if not isinstance(current, dict):
                reason = "missing_current_metric"
            elif check_id == "C1":
                if not valid_rate(current.get("null_rate")):
                    reason = "missing_current_metric"
                elif not isinstance(prior, dict) or not valid_rate(prior.get("null_rate")):
                    reason = "baseline_unavailable"
            elif check_id == "C2":
                if not valid_count(current.get("distinct")):
                    reason = "missing_current_metric"
                elif (
                    not isinstance(prior, dict)
                    or not valid_count(prior.get("distinct"))
                    or float(prior["distinct"]) <= 0
                ):
                    reason = "baseline_unavailable"
            elif check_id == "C3":
                if current.get("category") != category:
                    reason = "missing_current_metric"
                elif (
                    not valid_extremum(current.get("min"), category)
                    or not valid_extremum(current.get("max"), category)
                ):
                    reason = "no_finite_values"
                elif (
                    not isinstance(prior, dict)
                    or prior.get("category") != category
                    or not valid_extremum(prior.get("min"), category)
                    or not valid_extremum(prior.get("max"), category)
                ):
                    reason = "baseline_unavailable"
            elif check_id == "C4":
                if not valid_rate(current.get("uniqueness")):
                    reason = "missing_current_metric"
            elif check_id == "C5":
                if (
                    current.get("category") != "numeric"
                    or not valid_rate(current.get("zero_rate"))
                    or not valid_rate(current.get("negative_rate"))
                ):
                    reason = "missing_current_metric"
                elif (
                    not isinstance(prior, dict)
                    or prior.get("category") != "numeric"
                    or not valid_rate(prior.get("zero_rate"))
                    or not valid_rate(prior.get("negative_rate"))
                ):
                    reason = "baseline_unavailable"
            elif check_id == "C6":
                if current.get("category") != "numeric":
                    reason = "missing_current_metric"
                elif not bool(current.get("moments_certified", True)):
                    reason = "uncertified_precision"
                elif not valid_number(current.get("avg")):
                    reason = "no_finite_values"
                elif (
                    not isinstance(prior, dict)
                    or prior.get("category") != "numeric"
                    or not bool(prior.get("moments_certified", True))
                    or not valid_number(prior.get("avg"))
                    or not valid_number(prior.get("stddev"), minimum=0)
                    or float(prior["stddev"]) <= 0
                ):
                    reason = "baseline_unavailable"

            if reason is not None:
                outcomes.append(skipped_outcome(
                    check_id, config, column_name, reason,
                ))
            else:
                outcomes.append(evaluated_outcome(
                    check_id, config, column_name,
                ))
    return outcomes


def _table_snapshot_metadata_with_presence(
    mr,
    table_name: str,
) -> Tuple[bool, Any, Optional[int], Optional[int]]:
    """Return sealed snapshot timestamp, physical bytes, and live rows.

    Size and row completeness are independent.  In particular, a valid table
    may have no public columns, so its row count must come from the pinned
    resource metadata rather than from an invalid empty-column SQL view.  A
    deletion vector is subtracted only when its persisted row-count seal is
    complete and internally consistent.  The leading flag distinguishes a
    real table with an empty public schema from a missing table; both have an
    empty schema mapping after metadata sanitization.
    """

    try:
        records = mr.get_table_stats(table_name, "superadmin") or []
    except Exception:
        return False, None, None, None
    if not records or not isinstance(records[0], dict):
        return False, None, None, None
    snapshot = records[0]
    resources = snapshot.get("resources")
    if not isinstance(resources, list):
        return True, snapshot.get("last_updated_ms"), None, None

    total_size = 0
    total_rows = 0
    size_complete = True
    rows_complete = True
    for resource in resources:
        if not isinstance(resource, dict):
            size_complete = False
            rows_complete = False
            continue

        size = resource.get("file_size")
        if isinstance(size, bool):
            size_complete = False
        else:
            try:
                numeric_size = int(size)
            except (TypeError, ValueError, OverflowError):
                size_complete = False
            else:
                if numeric_size < 0:
                    size_complete = False
                else:
                    total_size += numeric_size

        rows = resource.get("rows")
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
            rows_complete = False
        else:
            total_rows += rows

    if rows_complete:
        tombstone = snapshot.get("tombstone")
        tombstone_rows = snapshot.get("tombstone_rows")
        if tombstone:
            if (
                isinstance(tombstone_rows, bool)
                or not isinstance(tombstone_rows, int)
                or tombstone_rows <= 0
                or tombstone_rows > total_rows
            ):
                rows_complete = False
            else:
                total_rows -= tombstone_rows
        elif tombstone_rows not in (None, 0):
            # A count without an active deletion-vector pointer is ambiguous.
            rows_complete = False

    return (
        True,
        snapshot.get("last_updated_ms"),
        total_size if size_complete else None,
        total_rows if rows_complete else None,
    )


def _table_snapshot_metadata(
    mr,
    table_name: str,
) -> Tuple[Any, Optional[int], Optional[int]]:
    """Compatibility view of sealed metadata without the presence flag."""

    _, modified, size, rows = _table_snapshot_metadata_with_presence(
        mr, table_name,
    )
    return modified, size, rows


def _table_metadata(mr, table_name: str) -> Tuple[Any, Optional[int]]:
    """Compatibility wrapper returning timestamp and complete physical size."""

    modified, size, _rows = _table_snapshot_metadata(mr, table_name)
    return modified, size


def _legacy_mode_results(latest: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Lift the old flat latest document into mode records once."""

    raw = latest.get("mode_results")
    if isinstance(raw, dict):
        return {str(mode): dict(record) for mode, record in raw.items()
                if isinstance(record, dict)}
    if not latest:
        return {}

    results: Dict[str, Dict[str, Any]] = {}
    rule_results = list(latest.get("rule_results") or [])
    anomalies = list(latest.get("anomalies") or [])
    custom_anomalies = [
        anomaly for anomaly in anomalies
        if str(anomaly.get("check_id", "")).startswith("R_")
    ]
    builtin_anomalies = [a for a in anomalies if a not in custom_anomalies]

    total = max(0, int(latest.get("total_checks", 0) or 0) - len(rule_results))
    warnings = sum(1 for a in builtin_anomalies if a.get("severity") == "warning")
    critical = sum(1 for a in builtin_anomalies if a.get("severity") == "critical")
    results["quick"] = {
        "checked_at": latest.get("checked_at"),
        "total_checks": total,
        "evaluated": total,
        "passed": max(0, total - warnings - critical),
        "warnings": warnings,
        "critical": critical,
        "errors": 0,
        "skipped": 0,
        "anomalies": builtin_anomalies,
        "outcomes": [],
    }
    if rule_results or latest.get("custom_checked_at"):
        custom_warnings = sum(
            1 for result in rule_results if result.get("status") == "warning"
        )
        custom_critical = sum(
            1 for result in rule_results if result.get("status") == "critical"
        )
        custom_errors = sum(
            1 for result in rule_results if result.get("status") == "error"
        )
        custom_total = len(rule_results)
        results["custom"] = {
            "checked_at": latest.get("custom_checked_at"),
            "total_checks": custom_total,
            "evaluated": max(0, custom_total - custom_errors),
            "passed": max(
                0,
                custom_total - custom_warnings - custom_critical - custom_errors,
            ),
            "warnings": custom_warnings,
            "critical": custom_critical,
            "errors": custom_errors,
            "skipped": 0,
            "not_applicable": 0,
            "anomalies": custom_anomalies,
            "outcomes": rule_results,
            "rule_results": rule_results,
        }
    return results


_MODE_COUNTER_FIELDS = (
    "total_checks",
    "evaluated",
    "passed",
    "warnings",
    "critical",
    "errors",
    "skipped",
    "not_applicable",
)


def _failed_attempt_modes(latest: Dict[str, Any]) -> List[str]:
    attempts = latest.get("mode_attempts")
    if not isinstance(attempts, dict):
        return []
    return sorted(
        str(mode)
        for mode, attempt in attempts.items()
        if isinstance(attempt, dict) and attempt.get("state") == "failed"
    )


def _status_from_counters(
    counters: Dict[str, Any],
    *,
    failed_modes: Optional[List[str]] = None,
) -> str:
    """Compute a truthful aggregate status, including unevaluated checks."""

    if failed_modes or int(counters.get("errors", 0) or 0):
        return "error"
    if int(counters.get("critical", 0) or 0):
        return "critical"
    if int(counters.get("warnings", 0) or 0):
        return "warning"
    skipped = int(counters.get("skipped", 0) or 0)
    not_applicable = int(counters.get("not_applicable", 0) or 0)
    if skipped > not_applicable:
        return "partial"
    return "ok"


def _apply_attempt_status(latest: Dict[str, Any]) -> None:
    failed_modes = _failed_attempt_modes(latest)
    latest["failed_modes"] = failed_modes
    latest["results_stale"] = bool(failed_modes)
    latest["status"] = _status_from_counters(
        latest,
        failed_modes=failed_modes,
    )


def _attempt_record(outcome: CheckRunOutcome) -> Dict[str, Any]:
    return {
        "attempted_at": _now_iso(),
        "state": outcome.state,
        "evaluated": int(outcome.evaluated or 0),
        "passed": int(outcome.passed or 0),
        "warnings": int(outcome.warnings or 0),
        "critical": int(outcome.critical or 0),
        "errors": int(outcome.errors or 0),
        "skipped": int(outcome.skipped or 0),
        "message": str(outcome.message or ""),
    }


def _publish_dqc_documents(
    dqc,
    documents: List[Tuple[Tuple[str, ...], Any]],
    fallback_writers: List[Any],
    *,
    lease_guard: Optional[_LeaseGuard],
) -> Optional[List[Any]]:
    """Publish a Redis result bundle under one ownership-checked Lua fence.

    Real :class:`DQConfig` instances expose their Redis client and key helper,
    allowing column staging, table completion and anomalies to become visible
    atomically.  Tiny in-memory test doubles use the supplied writers, with
    explicit checks around every write.
    """

    _assert_lease_owned(lease_guard)
    # Validate and normalize the complete bundle before any Redis mutation.
    # In particular, pandas represents DuckDB LIST<STRUCT> cells as NumPy
    # ndarrays/scalars; default=str would irreversibly flatten those arrays.
    if len(documents) != len(fallback_writers):
        raise ValueError("quality publication documents/writers are inconsistent")
    # Normalize one envelope, not each document independently.  The total-byte
    # cap therefore bounds the complete atomic Redis argument bundle and the
    # subsequent list of json.dumps strings, even for a very wide table with
    # thousands of column documents.
    normalized_values = normalize_json_value([
        value for _, value in documents
    ])
    normalized_documents = [
        (parts, value)
        for (parts, _), value in zip(documents, normalized_values)
    ]
    redis_client = getattr(dqc, "r", None)
    key_builder = getattr(dqc, "_key", None)
    if lease_guard is not None and redis_client is not None and callable(key_builder):
        keys = [key_builder(*parts) for parts, _ in normalized_documents]
        payloads = [
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            for _, value in normalized_documents
        ]
        script = """
        if redis.call('get', KEYS[1]) ~= ARGV[1] then
            return 0
        end
        for i = 2, #KEYS do
            redis.call('set', KEYS[i], ARGV[i])
        end
        return 1
        """
        with lease_guard.fence_lock:
            lease_guard.assert_owned()
            try:
                published = bool(redis_client.eval(
                    script,
                    len(keys) + 1,
                    lease_guard.key,
                    *keys,
                    lease_guard.token,
                    *payloads,
                ))
            except Exception as exc:
                lease_guard.lost.set()
                raise _LeaseLostError(
                    f"quality publication fence could not be verified: {exc}"
                ) from exc
            if not published:
                lease_guard.lost.set()
                raise _LeaseLostError(
                    "quality execution lease was lost before atomic publication"
                )
        return normalized_values

    for writer, value in zip(fallback_writers, normalized_values):
        _assert_lease_owned(lease_guard)
        if not writer(value):
            return None
    _assert_lease_owned(lease_guard)
    return normalized_values


def _publish_mode_attempt(
    dqc,
    table_name: str,
    outcome: CheckRunOutcome,
    *,
    lease_guard: Optional[_LeaseGuard] = None,
) -> bool:
    """Publish attempt state without overwriting the last successful result."""

    _assert_lease_owned(lease_guard)
    existing = dqc.get_latest(table_name) or {}
    latest = dict(existing)
    raw_attempts = existing.get("mode_attempts")
    if not isinstance(raw_attempts, dict):
        raw_attempts = {}
    attempts = {
        str(mode): dict(attempt)
        for mode, attempt in raw_attempts.items()
        if isinstance(attempt, dict)
    }
    attempt = _attempt_record(outcome)
    attempts[outcome.mode] = attempt
    latest["mode_attempts"] = attempts
    latest["last_attempt_at"] = attempt["attempted_at"]
    latest["last_attempt_mode"] = outcome.mode
    latest["last_attempt_state"] = outcome.state
    if outcome.state == "failed":
        latest["last_attempt_error"] = attempt["message"]
    else:
        latest.pop("last_attempt_error", None)
    for field_name in _MODE_COUNTER_FIELDS:
        latest.setdefault(field_name, 0)
    latest.setdefault("configured_checks", int(latest.get("total_checks", 0) or 0))
    _apply_attempt_status(latest)
    published = _publish_dqc_documents(
        dqc,
        [(('latest', table_name), latest)],
        [lambda value: dqc.set_latest(table_name, value)],
        lease_guard=lease_guard,
    )
    return published is not None


def _mode_history_document(
    latest: Dict[str, Any],
    mode: str,
) -> Dict[str, Any]:
    """Project merged latest state into the mode that produced one row."""

    projected = dict(latest)
    record = (latest.get("mode_results") or {}).get(mode) or {}
    projected["check_type"] = mode
    projected["checked_at"] = record.get("checked_at") or latest.get("checked_at")
    for field_name in _MODE_COUNTER_FIELDS:
        projected[field_name] = int(record.get(field_name, 0) or 0)
    projected["configured_checks"] = projected["total_checks"]
    projected["anomalies"] = list(record.get("anomalies") or [])
    projected["rule_results"] = (
        list(record.get("rule_results") or []) if mode == "custom" else []
    )
    projected["status"] = _status_from_counters(projected)
    return projected


def _write_mode_history(
    org: str,
    sup: str,
    table_name: str,
    mode: str,
    latest: Dict[str, Any],
    elapsed_ms: int,
    *,
    lease_guard: Optional[_LeaseGuard] = None,
) -> bool:
    """Durably queue history before attempting the Parquet sink.

    Scheduler calls have a lease (and therefore a Redis client). They may only
    advance cooldown/pending state after the immutable payload is accepted by
    the outbox hash. Sink failures leave that payload replayable. Direct/manual
    calls without a lease retain the historical best-effort behaviour.
    """

    try:
        from supertable.quality.history import write_history

        history_document = _mode_history_document(latest, mode)
        _assert_lease_owned(lease_guard)
        history_id = uuid.uuid4().hex
        payload = {
            "history_id": history_id,
            "organization": org,
            "super_name": sup,
            "table_name": table_name,
            "check_type": mode,
            "latest": history_document,
            "execution_ms": int(elapsed_ms),
        }
        encoded = json.dumps(
            normalize_json_value(payload),
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        if lease_guard is not None:
            outbox_key = _history_outbox_key(org, sup)
            if not _queue_history_outbox_if_owned(
                lease_guard,
                outbox_key,
                history_id,
                encoded,
            ):
                return False
            _assert_lease_owned(lease_guard)
        wrote = write_history(
            org,
            sup,
            table_name,
            mode,
            history_document,
            elapsed_ms,
            history_id=history_id,
        )
        _assert_lease_owned(lease_guard)
        if wrote and lease_guard is not None:
            _ack_history_outbox(
                lease_guard.redis, outbox_key, history_id, encoded,
            )
        # A durable queued payload is sufficient for scheduler success; the
        # replay worker will finish a failed/ambiguous Parquet delivery.
        return bool(wrote or lease_guard is not None)
    except _LeaseLostError:
        raise
    except Exception as exc:
        logger.error(
            "[dq-scheduler] History durability failed for %s %s: %s",
            mode,
            table_name,
            exc,
        )
        return False


def _empty_mode_record(mode: str, checked_at: str) -> Dict[str, Any]:
    return {
        "checked_at": checked_at,
        "total_checks": 0,
        "evaluated": 0,
        "passed": 0,
        "warnings": 0,
        "critical": 0,
        "errors": 0,
        "skipped": 0,
        "not_applicable": 0,
        "outcomes": [],
        "anomalies": [],
        "rule_results": [] if mode == "custom" else [],
        "disabled": True,
    }


def _cleared_mode_columns(
    dqc,
    table_name: str,
    columns: List[Tuple[str, str]],
    mode: str,
) -> Dict[str, Dict[str, Any]]:
    """Remove stale column-level state when a mode becomes empty/disabled."""

    keys_by_mode = {
        "deep": ("deep", "deep_checked_at"),
        "custom": ("custom", "custom_checked_at", "custom_rule_results"),
    }
    keys = keys_by_mode.get(mode, ())
    updates: Dict[str, Dict[str, Any]] = {}
    for column_name, _ in columns:
        existing = dqc.get_latest_column(table_name, column_name) or {}
        cleaned = dict(existing)
        changed = False
        for key in keys:
            if key in cleaned:
                cleaned.pop(key, None)
                changed = True
        if changed:
            updates[column_name] = cleaned
    return updates


def _publish_mode_latest(
    dqc,
    table_name: str,
    mode: str,
    mode_record: Dict[str, Any],
    *,
    base_updates: Optional[Dict[str, Any]] = None,
    column_updates: Optional[Dict[str, Dict[str, Any]]] = None,
    lease_guard: Optional[_LeaseGuard] = None,
) -> Optional[Dict[str, Any]]:
    """Merge one completed mode without deleting other modes' latest data."""

    _assert_lease_owned(lease_guard)
    existing = dqc.get_latest(table_name) or {}
    latest = dict(existing)
    if base_updates:
        latest.update(base_updates)

    mode_results = _legacy_mode_results(existing)
    mode_results[mode] = dict(mode_record)
    latest["mode_results"] = mode_results

    counters = {
        name: sum(int(record.get(name, 0) or 0) for record in mode_results.values())
        for name in _MODE_COUNTER_FIELDS
    }
    latest.update(counters)
    latest["configured_checks"] = counters["total_checks"]

    combined_anomalies: List[Dict[str, Any]] = []
    for record in mode_results.values():
        combined_anomalies.extend(list(record.get("anomalies") or []))
    latest["anomalies"] = combined_anomalies

    parsed = latest.get("parsed")
    if isinstance(parsed, dict):
        try:
            from supertable.quality.checker import compute_quality_score

            latest["quality_score"] = compute_quality_score(
                parsed.get("columns", {}),
                combined_anomalies,
            )
        except Exception:
            pass

    custom_record = mode_results.get("custom") or {}
    latest["rule_results"] = list(custom_record.get("rule_results") or [])
    if custom_record.get("checked_at"):
        latest["custom_checked_at"] = custom_record["checked_at"]
    deep_record = mode_results.get("deep") or {}
    if deep_record.get("checked_at"):
        latest["deep_checked_at"] = deep_record["checked_at"]

    # Attempt success is recorded only after cooldown/pending finalization in
    # _try_run_check.  Until then, preserve any prior failure marker rather
    # than claiming scheduler success prematurely.
    _apply_attempt_status(latest)

    documents: List[Tuple[Tuple[str, ...], Any]] = [
        (("latest", table_name), latest),
        (("anomalies", table_name), combined_anomalies),
    ]
    fallback_writers: List[Any] = [
        lambda value: dqc.set_latest(table_name, value),
        lambda value: dqc.set_anomalies(table_name, value),
    ]
    for column_name, document in (column_updates or {}).items():
        documents.append((("latest", table_name, column_name), document))
        fallback_writers.append(
            lambda value, column_name=column_name: dqc.set_latest_column(
                table_name, column_name, value,
            )
        )
    published = _publish_dqc_documents(
        dqc,
        documents,
        fallback_writers,
        lease_guard=lease_guard,
    )
    if published is None:
        return None
    return published[0]

def _run_quick_check(
    r,
    org: str,
    sup: str,
    table_name: str,
    dqc,
    *,
    lease_guard: Optional[_LeaseGuard] = None,
) -> CheckRunOutcome:
    """Execute and atomically publish a complete quick profile."""
    _check_start_ms = int(time.time() * 1000)

    from supertable.quality.checker import (
        build_quick_sql,
        compute_quality_score,
        filter_visible_columns,
        parse_quick_result,
        quality_table_fqn,
    )
    from supertable.quality.anomaly import (
        detect_anomalies,
        detect_schema_drift,
        evaluate_table_metadata_checks,
    )

    try:
        from supertable.meta_reader import MetaReader
    except ImportError as exc:
        return _failed("quick", f"MetaReader not available: {exc}")

    eff = dqc.get_effective_config(table_name)
    checks = eff.get("checks", {})
    previous = dqc.get_latest(table_name)

    # Read the public schema and snapshot metadata once.  Hidden storage
    # columns are never queryable by the quality SQL path.
    try:
        mr = MetaReader(super_name=sup, organization=org)
        schema_raw = mr.get_table_schema(table_name, "superadmin")
        if not schema_raw:
            return _failed("quick", f"No schema for {table_name}")
        schema_dict = schema_raw[0]
        columns = filter_visible_columns(list(schema_dict.items()))
        (
            snapshot_exists,
            last_modified_at,
            current_size_bytes,
            metadata_row_count,
        ) = _table_snapshot_metadata_with_presence(mr, table_name)
        if not schema_dict and not snapshot_exists:
            return _failed("quick", f"No schema for {table_name}")
    except Exception as e:
        return _failed("quick", f"Schema read failed for {table_name}: {e}")

    # Build and execute quick SQL
    try:
        table_fqn = quality_table_fqn(sup, table_name)
    except ValueError as exc:
        return _failed("quick", str(exc))
    # Row-level incremental quality is deliberately disabled.  A timestamp
    # alone is not a lossless cursor: ``>`` loses equal/late rows and ``>=``
    # double-counts the boundary. DQConfig sanitizes legacy configuration, and
    # this execution boundary independently enforces a complete logical scan
    # so an alternate config provider cannot reactivate the unsafe path.

    if columns:
        sql = build_quick_sql(table_fqn, columns)

        try:
            execution = _execute_quality_statement(org, sup, sql)
            if not execution.ok:
                return _failed(
                    "quick",
                    f"Quick SQL failed with status={execution.status}: "
                    f"{execution.message or 'no error message'}",
                )
            result_df = execution.require_success()
            if result_df.empty:
                return _failed(
                    "quick",
                    f"Quick SQL returned no aggregate row for {table_name}",
                )
            row = result_df.to_dict(orient="records")[0]
        except Exception as e:
            return _failed("quick", f"Quick SQL execution failed for {table_name}: {e}")
    else:
        # Constructing the public read relation for a zero-column table makes
        # both DuckDB and IslandDB reject an empty COLUMNS projection.  The
        # sealed resource metadata is the authoritative row-count source for
        # this shape, and every column check is reported not applicable below.
        if metadata_row_count is None:
            return _failed(
                "quick",
                f"Complete snapshot row metadata is unavailable for zero-column "
                f"table {table_name}",
            )
        row = {"__total": metadata_row_count}

    try:
        parsed = parse_quick_result(row, columns)
    except Exception as exc:
        return _failed("quick", f"Quick result parsing failed for {table_name}: {exc}")

    # Anomaly detection
    prev_parsed = previous.get("parsed") if previous else None
    anomalies = detect_anomalies(parsed, prev_parsed, checks)

    # Schema drift
    prev_schema = previous.get("schema") if previous else None
    if isinstance(prev_schema, list):
        prev_schema_tuples = [tuple(s) for s in prev_schema]
        schema_anomalies = detect_schema_drift(columns, prev_schema_tuples, checks)
        anomalies.extend(schema_anomalies)

    previous_size_bytes = previous.get("table_size_bytes") if previous else None
    metadata_outcomes = evaluate_table_metadata_checks(
        checks,
        last_modified_at=last_modified_at,
        current_size_bytes=current_size_bytes,
        previous_size_bytes=previous_size_bytes,
    )
    profile_outcomes = _quick_profile_outcomes(
        parsed,
        previous,
        columns,
        checks,
        anomalies,
    )
    outcomes = profile_outcomes + metadata_outcomes
    for outcome in metadata_outcomes:
        if outcome.get("status") in {"warning", "critical"}:
            anomalies.append(_outcome_to_anomaly(outcome))

    summary = _summary_from_outcomes(outcomes)
    if summary["errors"]:
        messages = [
            str(outcome.get("message") or outcome.get("check_id"))
            for outcome in outcomes
            if outcome.get("status") == "error"
        ]
        return _failed(
            "quick",
            "Quick check evaluation failed: " + "; ".join(messages),
            errors=summary["errors"],
        )

    score = compute_quality_score(parsed.get("columns", {}), anomalies)
    checked_at = _now_iso()
    mode_record = {
        "checked_at": checked_at,
        **summary,
        "outcomes": outcomes,
        "anomalies": anomalies,
    }
    base_updates = {
        "checked_at": checked_at,
        "check_type": "quick",
        "row_count": parsed.get("total", 0),
        "quality_score": score,
        "parsed": parsed,
        "schema": [list(c) for c in columns],
        "table_last_modified_at": last_modified_at,
        "table_size_bytes": current_size_bytes,
        # Clear state written by the former unsafe timestamp-watermark path.
        "incremental_column": None,
        "incremental_watermarks": {},
        "incremental_comparison_ready": False,
    }

    # Column documents are staged first and retain any completed deep profile.
    # The table-level latest write below is the completion marker.
    pending_columns: Dict[str, Dict[str, Any]] = {}
    for col_name, col_data in parsed.get("columns", {}).items():
        merged_col = dqc.get_latest_column(table_name, col_name) or {}
        merged_col.update(col_data)
        merged_col["checked_at"] = checked_at
        col_issues = [a for a in anomalies if a.get("column") == col_name]
        merged_col["status"] = "critical" if any(a["severity"] == "critical" for a in col_issues) \
            else ("warning" if col_issues else "ok")
        merged_col["issues"] = [a.get("message", "") for a in col_issues]
        merged_col.pop("data_watermark", None)
        pending_columns[col_name] = merged_col

    latest = _publish_mode_latest(
        dqc,
        table_name,
        "quick",
        mode_record,
        base_updates=base_updates,
        column_updates=pending_columns,
        lease_guard=lease_guard,
    )
    if latest is None:
        return _failed("quick", f"Could not publish quick result for {table_name}")

    logger.info(
        f"[dq-scheduler] Quick check done: {table_name} — "
        f"score={score}, anomalies={len(anomalies)}"
    )

    # ── Write to __data_quality__ history table ───────────────
    _check_elapsed_ms = int(time.time() * 1000) - _check_start_ms
    if not _write_mode_history(
        org,
        sup,
        table_name,
        "quick",
        latest,
        _check_elapsed_ms,
        lease_guard=lease_guard,
    ):
        return _failed("quick", "quality history could not be durably queued")

    return _success(
        "quick",
        evaluated=summary["evaluated"],
        passed=summary["passed"],
        warnings=summary["warnings"],
        critical=summary["critical"],
        skipped=summary["skipped"],
        details=outcomes,
    )


def _run_deep_check(
    r,
    org: str,
    sup: str,
    table_name: str,
    dqc,
    *,
    lease_guard: Optional[_LeaseGuard] = None,
) -> CheckRunOutcome:
    """Execute a complete deep profile and publish only after all SQL succeeds."""
    _deep_start_ms = int(time.time() * 1000)

    from supertable.quality.checker import (
        _col_category,
        build_deep_numeric_sql,
        build_deep_string_sql,
        filter_visible_columns,
        quality_table_fqn,
    )
    from supertable.quality.anomaly import evaluate_deep_checks

    try:
        from supertable.meta_reader import MetaReader
    except ImportError as exc:
        return _failed("deep", f"MetaReader not available: {exc}")

    eff = dqc.get_effective_config(table_name)
    checks = eff.get("checks", {})
    deep_enabled = any(v.get("enabled") for k, v in checks.items() if k.startswith("D"))

    try:
        mr = MetaReader(super_name=sup, organization=org)
        schema_raw = mr.get_table_schema(table_name, "superadmin")
        if not schema_raw:
            return _failed("deep", f"No schema for {table_name}")
        schema_dict = schema_raw[0]
        if (
            not schema_dict
            and not _table_snapshot_metadata_with_presence(mr, table_name)[0]
        ):
            return _failed("deep", f"No schema for {table_name}")
        columns = filter_visible_columns(list(schema_dict.items()))
    except Exception as e:
        return _failed("deep", f"Deep schema read failed for {table_name}: {e}")

    if not deep_enabled:
        checked_at = _now_iso()
        mode_record = _empty_mode_record("deep", checked_at)
        existing_latest = dqc.get_latest(table_name) or {}
        base_updates: Dict[str, Any] = {}
        if not existing_latest:
            base_updates = {
                "checked_at": checked_at,
                "check_type": "deep",
                "row_count": 0,
                "parsed": {"total": 0, "columns": {}},
                "schema": [list(c) for c in columns],
            }
        latest = _publish_mode_latest(
            dqc,
            table_name,
            "deep",
            mode_record,
            base_updates=base_updates,
            column_updates=_cleared_mode_columns(
                dqc, table_name, columns, "deep",
            ),
            lease_guard=lease_guard,
        )
        if latest is None:
            return _failed("deep", f"Could not clear disabled deep result for {table_name}")
        if not _write_mode_history(
            org,
            sup,
            table_name,
            "deep",
            latest,
            int(time.time() * 1000) - _deep_start_ms,
            lease_guard=lease_guard,
        ):
            return _failed("deep", "quality history could not be durably queued")
        return _success("deep")

    try:
        table_fqn = quality_table_fqn(sup, table_name)
    except ValueError as exc:
        return _failed("deep", str(exc))
    pending_columns: Dict[str, Dict[str, Any]] = {}
    outcomes: List[Dict[str, Any]] = []
    applicable_by_category = {
        "numeric": {"D2", "D3", "D4", "D5"},
        "string": {"D1", "D2", "D3", "D4", "D7"},
    }

    if not columns:
        # Deep checks are column-scoped.  Preserve one truthful outcome for
        # every enabled definition instead of silently publishing 0 checks.
        outcomes.extend(
            evaluate_deep_checks(
                {},
                None,
                "other",
                checks,
                "<no visible columns>",
            )
        )

    for col_name, col_type in columns:
        cat = _col_category(col_type)
        previous_column = dqc.get_latest_column(table_name, col_name) or {}
        previous_deep = previous_column.get("deep")
        enabled_for_category = any(
            isinstance(checks.get(check_id), dict)
            and checks[check_id].get("enabled")
            for check_id in applicable_by_category.get(cat, set())
        )

        if not enabled_for_category:
            outcomes.extend(
                evaluate_deep_checks({}, previous_deep, cat, checks, col_name)
            )
            continue

        try:
            sql = (
                build_deep_numeric_sql(table_fqn, col_name, col_type)
                if cat == "numeric"
                else build_deep_string_sql(table_fqn, col_name)
            )
            execution = _execute_quality_statement(
                org,
                sup,
                sql,
                allow_bounded_collection_aggregates=True,
            )
            if not execution.ok:
                return _failed(
                    "deep",
                    f"Deep SQL failed for {table_name}.{col_name} with "
                    f"status={execution.status}: {execution.message or 'no error message'}",
                )
            result_df = execution.require_success()
            if result_df.empty:
                return _failed(
                    "deep",
                    f"Deep SQL returned no aggregate row for {table_name}.{col_name}",
                )
            deep_result = result_df.to_dict(orient="records")[0]
            deep_result["check_type"] = "deep"
            deep_result["checked_at"] = _now_iso()
            deep_result["column_name"] = col_name
            deep_result["column_type"] = col_type
            deep_result["category"] = cat
            # Normalize before evaluation as well as publication.  D3/D4
            # outcomes must carry ordered lists of STRUCT objects, never the
            # string representation of a NumPy object array.
            deep_result = normalize_json_value(deep_result)
            outcomes.extend(
                evaluate_deep_checks(
                    deep_result,
                    previous_deep,
                    cat,
                    checks,
                    col_name,
                )
            )
            updated_column = dict(previous_column)
            updated_column["deep"] = deep_result
            updated_column["deep_checked_at"] = deep_result["checked_at"]
            pending_columns[col_name] = updated_column
        except Exception as e:
            return _failed(
                "deep",
                f"Deep check failed for {table_name}.{col_name}: {e}",
            )

    summary = _summary_from_outcomes(outcomes)
    if summary["errors"]:
        messages = [
            str(outcome.get("message") or outcome.get("check_id"))
            for outcome in outcomes
            if outcome.get("status") == "error"
        ]
        return _failed(
            "deep",
            "Deep check evaluation failed: " + "; ".join(messages),
            errors=summary["errors"],
        )

    checked_at = _now_iso()
    anomalies = [
        _outcome_to_anomaly(outcome)
        for outcome in outcomes
        if outcome.get("status") in {"warning", "critical"}
    ]
    mode_record = {
        "checked_at": checked_at,
        **summary,
        "outcomes": outcomes,
        "anomalies": anomalies,
    }
    existing_latest = dqc.get_latest(table_name) or {}
    base_updates: Dict[str, Any] = {}
    if not existing_latest:
        base_updates = {
            "checked_at": checked_at,
            "check_type": "deep",
            "row_count": 0,
            "parsed": {"total": 0, "columns": {}},
            "schema": [list(c) for c in columns],
        }
    latest = _publish_mode_latest(
        dqc,
        table_name,
        "deep",
        mode_record,
        base_updates=base_updates,
        column_updates=pending_columns,
        lease_guard=lease_guard,
    )
    if latest is None:
        return _failed("deep", f"Could not publish deep result for {table_name}")

    logger.info(f"[dq-scheduler] Deep check done: {table_name}")

    # ── Write to __data_quality__ history table ───────────────
    _deep_elapsed_ms = int(time.time() * 1000) - _deep_start_ms
    if not _write_mode_history(
        org,
        sup,
        table_name,
        "deep",
        latest,
        _deep_elapsed_ms,
        lease_guard=lease_guard,
    ):
        return _failed("deep", "quality history could not be durably queued")

    return _success(
        "deep",
        evaluated=summary["evaluated"],
        passed=summary["passed"],
        warnings=summary["warnings"],
        critical=summary["critical"],
        skipped=summary["skipped"],
        details=outcomes,
    )


def _run_custom_check(
    r,
    org: str,
    sup: str,
    table_name: str,
    dqc,
    *,
    lease_guard: Optional[_LeaseGuard] = None,
) -> CheckRunOutcome:
    """Execute custom rules for one table on their own schedule.

    Runs all enabled custom rules, evaluates results, merges into the
    existing latest (from Redis), recomputes score, and stores back.
    """
    _custom_start_ms = int(time.time() * 1000)

    from supertable.quality.checker import (
        build_custom_rule_sql,
        evaluate_custom_rule,
        filter_visible_columns,
        quality_table_fqn,
        validate_custom_rule,
        validate_quality_table_name,
    )

    try:
        from supertable.meta_reader import MetaReader
    except ImportError as exc:
        return _failed("custom", f"MetaReader not available: {exc}")

    custom_rules = dqc.list_rules_for_table(table_name)

    try:
        mr = MetaReader(super_name=sup, organization=org)
        schema_raw = mr.get_table_schema(table_name, "superadmin")
        if not schema_raw:
            return _failed("custom", f"No schema for {table_name}")
        schema_dict = schema_raw[0]
        columns = filter_visible_columns(list(schema_dict.items()))
        (
            snapshot_exists,
            _last_modified,
            _table_size,
            metadata_row_count,
        ) = _table_snapshot_metadata_with_presence(mr, table_name)
        if not schema_dict and not snapshot_exists:
            return _failed("custom", f"No schema for {table_name}")
    except Exception as exc:
        return _failed("custom", f"Custom-rule schema read failed for {table_name}: {exc}")

    try:
        table_fqn = quality_table_fqn(sup, table_name)
    except ValueError as exc:
        return _failed("custom", str(exc))

    if not custom_rules:
        checked_at = _now_iso()
        mode_record = _empty_mode_record("custom", checked_at)
        existing_latest = dqc.get_latest(table_name) or {}
        base_updates: Dict[str, Any] = {}
        if not existing_latest:
            base_updates = {
                "checked_at": checked_at,
                "check_type": "custom",
                "row_count": 0,
                "parsed": {"total": 0, "columns": {}},
                "schema": [list(c) for c in columns],
            }
        latest = _publish_mode_latest(
            dqc,
            table_name,
            "custom",
            mode_record,
            base_updates=base_updates,
            column_updates=_cleared_mode_columns(
                dqc, table_name, columns, "custom",
            ),
            lease_guard=lease_guard,
        )
        if latest is None:
            return _failed("custom", f"Could not clear disabled custom result for {table_name}")
        if not _write_mode_history(
            org,
            sup,
            table_name,
            "custom",
            latest,
            int(time.time() * 1000) - _custom_start_ms,
            lease_guard=lease_guard,
        ):
            return _failed("custom", "quality history could not be durably queued")
        return _success("custom")

    validated_sql: List[Tuple[Dict[str, Any], Optional[str]]] = []

    for rule in custom_rules:
        table_validation = validate_quality_table_name(
            rule.get("table_name"),
            super_name=sup,
            allow_wildcard=rule.get("rule_type") != "custom_sql",
        )
        if not table_validation.valid:
            return _failed(
                "custom",
                f"Invalid custom rule {rule.get('rule_id', '<unknown>')} "
                f"({table_validation.code}): {table_validation.message}",
            )
        validation = validate_custom_rule(
            rule,
            columns,
            table_fqn=table_fqn,
        )
        if not validation.valid:
            return _failed(
                "custom",
                f"Invalid custom rule {rule.get('rule_id', '<unknown>')} "
                f"({validation.code}): {validation.message}",
            )
        if rule.get("rule_type") == "row_count_min" and not columns:
            if metadata_row_count is None:
                return _failed(
                    "custom",
                    f"Complete snapshot row metadata is unavailable for zero-column "
                    f"table {table_name}",
                )
            rule_sql = None
        else:
            rule_sql = build_custom_rule_sql(rule, table_fqn, columns)
        if not rule_sql:
            if not (
                rule.get("rule_type") == "row_count_min"
                and not columns
                and metadata_row_count is not None
            ):
                return _failed(
                    "custom",
                    f"Custom rule {rule.get('rule_id', '<unknown>')} produced no SQL",
                )
        validated_sql.append((rule, rule_sql))

    rule_results: List[Dict[str, Any]] = []
    rule_anomalies: List[Dict[str, Any]] = []
    for rule, rule_sql in validated_sql:
        try:
            if rule_sql is None:
                r_result = [{"row_count": metadata_row_count}]
            else:
                execution = _execute_quality_statement(org, sup, rule_sql)
                if not execution.ok:
                    return _failed(
                        "custom",
                        f"Custom rule {rule['rule_id']} SQL failed with "
                        f"status={execution.status}: "
                        f"{execution.message or 'no error message'}",
                    )
                r_df = execution.require_success()
                r_result = (
                    r_df.to_dict(orient="records")
                    if r_df is not None and not r_df.empty
                    else []
                )
            eval_result = evaluate_custom_rule(rule, r_result)
            outcome = {
                "rule_id": rule["rule_id"],
                "check_id": f"R_{rule['rule_id']}",
                "rule_type": rule["rule_type"],
                "description": rule.get("description", ""),
                "applicable": True,
                "evaluated": eval_result.get("evaluated", True),
                **eval_result,
            }
            rule_results.append(outcome)
            if outcome.get("status") == "error":
                return _failed(
                    "custom",
                    f"Custom rule {rule['rule_id']} could not be evaluated: "
                    f"{outcome.get('detail', 'invalid result')}",
                )
            if eval_result["status"] != "ok":
                rule_anomalies.append({
                    "check_id": f"R_{rule['rule_id']}",
                    "check_name": f"Custom: {rule.get('description', rule['rule_type'])}",
                    "column": rule.get("column_name"),
                    "severity": rule.get("severity", "warning"),
                    "message": eval_result.get("detail", ""),
                    "value": eval_result.get("value"),
                    "detected_at": _now_iso(),
                })
        except Exception as e:
            return _failed("custom", f"Custom rule {rule['rule_id']} failed: {e}")

    summary = _summary_from_outcomes(rule_results)
    checked_at = _now_iso()
    mode_record = {
        "checked_at": checked_at,
        **summary,
        "outcomes": rule_results,
        "anomalies": rule_anomalies,
        "rule_results": rule_results,
    }
    existing = dqc.get_latest(table_name) or {}
    base_updates: Dict[str, Any] = {}
    if not existing:
        base_updates = {
            "checked_at": checked_at,
            "check_type": "custom",
            "row_count": 0,
            "parsed": {"total": 0, "columns": {}},
            "schema": [list(c) for c in columns],
        }
    latest = _publish_mode_latest(
        dqc,
        table_name,
        "custom",
        mode_record,
        base_updates=base_updates,
        lease_guard=lease_guard,
    )
    if latest is None:
        return _failed("custom", f"Could not publish custom result for {table_name}")

    logger.info(
        f"[dq-scheduler] Custom rules check done: {table_name} — "
        f"{len(custom_rules)} rules, {len(rule_anomalies)} issues, "
        f"score={latest.get('quality_score', 0)}"
    )

    # ── Write to __data_quality__ history table ───────────────
    _custom_elapsed_ms = int(time.time() * 1000) - _custom_start_ms
    if not _write_mode_history(
        org,
        sup,
        table_name,
        "custom",
        latest,
        _custom_elapsed_ms,
        lease_guard=lease_guard,
    ):
        return _failed("custom", "quality history could not be durably queued")

    return _success(
        "custom",
        evaluated=summary["evaluated"],
        passed=summary["passed"],
        warnings=summary["warnings"],
        critical=summary["critical"],
        skipped=summary["skipped"],
        details=rule_results,
    )


# ──────────────────────────────────────────────────────────────────────
# Redis key helpers
# ──────────────────────────────────────────────────────────────────────

from supertable import redis_keys as RK


def _pending_key(
    org: str,
    sup: str,
    table: str,
    mode: Optional[str] = None,
) -> str:
    if mode:
        # A sibling namespace preserves the historical ``pending:*`` scalar
        # key contract (including scans that expect one key per table).
        return RK.quality_prefix(org, sup) + f"pending_mode:{table}:{mode}"
    return RK.quality_prefix(org, sup) + f"pending:{table}"


def _unresolved_pending_key(org: str, sup: str, table: str) -> str:
    return RK.quality_prefix(org, sup) + f"pending_unresolved:{table}"


def _running_key(org: str, sup: str, table: str) -> str:
    return RK.quality_prefix(org, sup) + f"running:{table}"


def _cooldown_key(
    org: str,
    sup: str,
    table: str,
    mode: Optional[str] = None,
) -> str:
    key = RK.quality_prefix(org, sup) + f"cooldown:{table}"
    return f"{key}:{mode}" if mode else key


def _retry_key(org: str, sup: str, table: str, mode: str) -> str:
    return RK.quality_prefix(org, sup) + f"retry:{table}:{mode}"


def _history_outbox_key(org: str, sup: str) -> str:
    return RK.quality_prefix(org, sup) + "history_outbox"


def _queue_history_outbox_if_owned(
    lease_guard: _LeaseGuard,
    key: str,
    history_id: str,
    payload: str,
) -> bool:
    """Atomically fence and insert one immutable history payload.

    The lease comparison and ``HSETNX`` must share one Redis linearization
    point. A pre-check followed by a normal HSET lets a deadline remove the
    lease between those commands while the expired worker still queues a row.
    Existing IDs are idempotent only when their exact canonical payload agrees;
    a collision or attempted overwrite fails closed.
    """

    script = """
    if redis.call('get', KEYS[1]) ~= ARGV[1] then
        return 0
    end
    if redis.call('hsetnx', KEYS[2], ARGV[2], ARGV[3]) == 1 then
        return 1
    end
    if redis.call('hget', KEYS[2], ARGV[2]) == ARGV[3] then
        return 2
    end
    return -1
    """
    try:
        result = int(lease_guard.redis.eval(
            script,
            2,
            lease_guard.key,
            key,
            lease_guard.token,
            history_id,
            payload,
        ))
    except Exception as exc:
        # The script may have committed before an ambiguous transport error.
        # Fence this worker locally so no cooldown/pending transition follows.
        lease_guard.mark_lost()
        raise _LeaseLostError(
            "quality history outbox ownership could not be verified"
        ) from exc
    if result == 0:
        lease_guard.mark_lost()
        raise _LeaseLostError(
            "quality execution lease was lost before history enqueue"
        )
    if result not in (1, 2):
        logger.error(
            "[dq-scheduler] Refusing conflicting history outbox payload %s",
            history_id,
        )
        return False
    try:
        persisted = lease_guard.redis.hget(key, history_id)
    except Exception as exc:
        lease_guard.mark_lost()
        raise _LeaseLostError(
            "quality history outbox read-back could not be verified"
        ) from exc
    if persisted is None or not _redis_values_equal(persisted, payload):
        lease_guard.mark_lost()
        raise _LeaseLostError(
            "quality history outbox read-back did not match its payload"
        )
    return True


def _ack_history_outbox(r, key: str, history_id: str, payload: str) -> bool:
    script = """
    if redis.call('hget', KEYS[1], ARGV[1]) ~= ARGV[2] then
        return 0
    end
    return redis.call('hdel', KEYS[1], ARGV[1])
    """
    try:
        return bool(r.eval(script, 1, key, history_id, payload))
    except Exception:
        # Ambiguous acknowledgement deliberately leaves a replayable record.
        return False


def _drain_history_outbox(r, org: str, sup: str, *, limit: int = 100) -> int:
    """Retry a bounded number of durable history deliveries."""

    from supertable.quality.history import write_history_payload

    key = _history_outbox_key(org, sup)
    try:
        _cursor, raw_items = r.hscan(key, cursor=0, count=max(1, min(limit, 1000)))
    except Exception as exc:
        logger.warning("[dq-scheduler] history outbox scan failed: %s", exc)
        return 0
    delivered = 0
    for raw_id, raw_payload in list((raw_items or {}).items())[:limit]:
        history_id = (
            raw_id.decode("utf-8") if isinstance(raw_id, bytes) else str(raw_id)
        )
        encoded = (
            raw_payload.decode("utf-8")
            if isinstance(raw_payload, bytes)
            else str(raw_payload)
        )
        try:
            payload = json.loads(encoded)
            if not isinstance(payload, dict) or payload.get("history_id") != history_id:
                raise ValueError("history outbox identity mismatch")
            if not write_history_payload(payload):
                continue
            if _ack_history_outbox(r, key, history_id, encoded):
                delivered += 1
        except Exception as exc:
            logger.error(
                "[dq-scheduler] history outbox delivery failed for %s: %s",
                history_id,
                exc,
            )
    return delivered


def _cron_state_key(org: str, sup: str, table: str, mode: str) -> str:
    return RK.quality_prefix(org, sup) + f"cron_state:{table}:{mode}"


def _cron_schedule_state(
    r,
    org: str,
    sup: str,
    table: str,
    mode: str,
    expression: str,
    timezone_name: str,
    now_ms: int,
) -> Tuple[bool, Dict[str, Any]]:
    """Return persisted cron due state, initializing the next phase safely."""

    from supertable.quality.cron import CronSchedule

    schedule = CronSchedule.parse(expression, timezone_name)
    key = _cron_state_key(org, sup, table, mode)
    # Cron has minute resolution. If this process first observes a schedule
    # after its wall-clock boundary within the same minute, execute that phase
    # once and persist it; otherwise retain the exact next calendar boundary.
    current_minute_start_ms = now_ms - (now_ms % 60_000)
    first_due_ms = schedule.next_after_ms(current_minute_start_ms - 1)
    raw = r.get(key)
    state: Dict[str, Any]
    if raw is None:
        state = {
            "schema_version": 1,
            "expression": schedule.expression,
            "timezone": schedule.timezone_name,
            "status": "scheduled",
            "next_due_ms": first_due_ms,
            "last_scheduled_ms": None,
            "last_started_ms": None,
            "last_completed_ms": None,
            "last_outcome": None,
        }
        payload = json.dumps(state, sort_keys=True, separators=(",", ":"))
        if not r.set(key, payload, nx=True):
            raw = r.get(key)
            if raw is None:
                raise RuntimeError("cron state initialization was not acknowledged")
        else:
            return now_ms >= first_due_ms, state
    if raw is not None:
        try:
            state = json.loads(raw)
        except (TypeError, ValueError, UnicodeError) as exc:
            raise RuntimeError("persisted quality cron state is malformed") from exc
        if not isinstance(state, dict) or state.get("schema_version") != 1:
            raise RuntimeError("persisted quality cron state has the wrong shape")
    if (
        state.get("expression") != schedule.expression
        or state.get("timezone") != schedule.timezone_name
    ):
        # Configuration changes start a fresh calendar phase. They never reuse
        # a fixed interval derived from the previous expression/timezone.
        state = {
            "schema_version": 1,
            "expression": schedule.expression,
            "timezone": schedule.timezone_name,
            "status": "scheduled",
            "next_due_ms": first_due_ms,
            "last_scheduled_ms": None,
            "last_started_ms": None,
            "last_completed_ms": None,
            "last_outcome": None,
        }
        if not r.set(key, json.dumps(state, sort_keys=True, separators=(",", ":"))):
            raise RuntimeError("updated quality cron state was not persisted")
        return now_ms >= first_due_ms, state
    next_due = state.get("next_due_ms")
    if isinstance(next_due, bool) or not isinstance(next_due, int) or next_due < 0:
        raise RuntimeError("persisted quality cron next_due_ms is invalid")
    return now_ms >= next_due, state


def _record_cron_outcome(
    r,
    org: str,
    sup: str,
    table: str,
    mode: str,
    state: Dict[str, Any],
    outcome: CheckRunOutcome,
    now_ms: int,
) -> None:
    """Persist the exact phase and last outcome before the next tick."""

    from supertable.quality.cron import CronSchedule

    scheduled_ms = int(state["next_due_ms"])
    schedule = CronSchedule.parse(
        str(state["expression"]), str(state["timezone"]),
    )
    next_due = schedule.next_after_ms(scheduled_ms)
    # Coalesce missed phases rather than launching an unbounded catch-up storm.
    for _ in range(100_000):
        if next_due > now_ms:
            break
        next_due = schedule.next_after_ms(next_due)
    else:
        raise RuntimeError("cron catch-up exceeded its safety bound")
    updated = dict(state)
    updated.update({
        "status": "scheduled" if outcome.successful else "retry",
        "next_due_ms": next_due if outcome.successful else scheduled_ms,
        "last_scheduled_ms": scheduled_ms,
        "last_started_ms": updated.get("last_started_ms") or now_ms,
        "last_completed_ms": now_ms,
        "last_outcome": outcome.state,
        "last_message": outcome.message[:4096],
    })
    key = _cron_state_key(org, sup, table, mode)
    if not r.set(key, json.dumps(updated, sort_keys=True, separators=(",", ":"))):
        raise RuntimeError("quality cron outcome was not persisted")


def _post_ingest_modes(
    schedule: Dict[str, Any],
    table_schedule: Optional[Dict[str, Any]] = None,
) -> Tuple[str, ...]:
    """Return modes requested for one ingest, honoring table overrides."""

    override = table_schedule or {}
    post_ingest = override.get(
        "post_ingest",
        schedule.get("post_ingest", True),
    )
    if post_ingest is False:
        return ()
    defaults = {
        "quick": True,
        "custom": True,
        "deep": False,
    }
    modes: List[str] = []
    for mode in QUALITY_MODES:
        field_name = f"post_ingest_{mode}"
        enabled = override.get(
            field_name,
            schedule.get(field_name, defaults[mode]),
        )
        if enabled:
            modes.append(mode)
    return tuple(modes)


def _redis_values_equal(left: Any, right: Any) -> bool:
    def normalise(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, bytearray):
            return bytes(value)
        return str(value).encode("utf-8")

    return normalise(left) == normalise(right)


def _delete_if_value(r, key: str, expected: Any) -> bool:
    """Atomically delete *key* only while it still belongs to *expected*."""

    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """
    try:
        return bool(r.eval(script, 1, key, expected))
    except (AttributeError, NotImplementedError):
        # Compatibility fallback only for clients that genuinely do not
        # implement scripts. An operational Redis/script error is ambiguous:
        # falling back to GET+DEL could delete a successor that reacquired the
        # lease between those two non-atomic operations.
        try:
            current = r.get(key)
            if current is not None and _redis_values_equal(current, expected):
                return bool(r.delete(key))
        except Exception:
            return False
    except Exception:
        return False
    return False


def _expire_if_value(r, key: str, expected: Any, ttl_seconds: int) -> bool:
    script = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('expire', KEYS[1], ARGV[2])
    end
    return 0
    """
    try:
        return bool(r.eval(script, 1, key, expected, int(ttl_seconds)))
    except (AttributeError, NotImplementedError):
        # Compatibility only for clients that genuinely do not implement
        # scripts. Operational Redis errors are ambiguous and fail closed.
        try:
            current = r.get(key)
            if current is not None and _redis_values_equal(current, expected):
                return bool(r.expire(key, int(ttl_seconds)))
        except Exception:
            return False
    except Exception:
        return False
    return False


def _renew_running_lease(
    r,
    key: str,
    token: str,
    stop: threading.Event,
    lost: Optional[threading.Event] = None,
    fence_lock: Any = None,
) -> None:
    interval = max(1.0, DEFAULT_RUNNING_TTL_SECONDS / 3.0)
    while not stop.wait(interval):
        lock = fence_lock or threading.RLock()
        with lock:
            if lost is not None and lost.is_set():
                return
            if not _expire_if_value(r, key, token, DEFAULT_RUNNING_TTL_SECONDS):
                # Lost ownership *or* an ambiguous Redis response is terminal.
                # Never let the query silently publish after a transient renewal
                # failure, because another scheduler may acquire the expired key.
                if lost is not None:
                    lost.set()
                return


def _consume_pending_generation(r, key: str, generation: Any) -> bool:
    return _delete_if_value(r, key, generation)


def _set_retry_if_owned(
    r,
    retry_key: str,
    _lock_token: str,
    lease_guard: _LeaseGuard,
    cooldown_sec: int,
    *,
    completion_event: Optional[threading.Event] = None,
) -> bool:
    retry_seconds = max(
        1,
        min(DEFAULT_RETRY_BACKOFF_SECONDS, max(1, cooldown_sec)),
    )
    script = """
    if redis.call('get', KEYS[1]) ~= ARGV[1] then
        return 0
    end
    redis.call('set', KEYS[2], ARGV[2], 'EX', ARGV[3])
    return 1
    """
    with lease_guard.fence_lock:
        lease_guard.assert_owned()
        try:
            written = bool(r.eval(
                script,
                2,
                lease_guard.key,
                retry_key,
                lease_guard.token,
                _now_iso(),
                retry_seconds,
            ))
        except Exception as exc:
            lease_guard.lost.set()
            raise _LeaseLostError(
                f"retry ownership fence could not be verified: {exc}"
            ) from exc
        if not written:
            lease_guard.lost.set()
            raise _LeaseLostError(
                "quality execution lease was lost before retry publication"
            )
        if completion_event is not None:
            completion_event.set()
    return True


def _finalize_success_if_owned(
    r,
    lease_guard: _LeaseGuard,
    cooldown_key: str,
    retry_key: str,
    cooldown_sec: int,
    *,
    pending_key: Optional[str] = None,
    pending_generation: Any = None,
    completion_event: Optional[threading.Event] = None,
) -> bool:
    """Atomically fence success bookkeeping and exact-generation consumption."""

    keys = [lease_guard.key, cooldown_key, retry_key]
    has_pending = pending_key is not None and pending_generation is not None
    if has_pending:
        keys.append(str(pending_key))
    script = """
    if redis.call('get', KEYS[1]) ~= ARGV[1] then
        return 0
    end
    if tonumber(ARGV[3]) > 0 then
        redis.call('set', KEYS[2], ARGV[2], 'EX', ARGV[3])
    end
    redis.call('del', KEYS[3])
    if ARGV[4] == '1' and redis.call('get', KEYS[4]) == ARGV[5] then
        redis.call('del', KEYS[4])
    end
    return 1
    """
    with lease_guard.fence_lock:
        if lease_guard.lost.is_set():
            return False
        try:
            finalized = bool(r.eval(
                script,
                len(keys),
                *keys,
                lease_guard.token,
                _now_iso(),
                max(0, int(cooldown_sec)),
                "1" if has_pending else "0",
                pending_generation if has_pending else "",
            ))
        except Exception:
            lease_guard.lost.set()
            return False
        if not finalized:
            lease_guard.lost.set()
        elif completion_event is not None:
            completion_event.set()
        return finalized


def _migrate_legacy_pending(
    r,
    org: str,
    sup: str,
    table: str,
    modes: Tuple[str, ...],
) -> None:
    """Move a pre/per-mode scalar pending marker into independent mode keys."""

    legacy_key = _pending_key(org, sup, table)
    try:
        generation = r.get(legacy_key)
    except Exception:
        return
    if generation is None:
        return
    if not modes:
        return
    all_ready = True
    for mode in modes:
        mode_key = _pending_key(org, sup, table, mode)
        try:
            current = r.get(mode_key)
            if current is None:
                # Migrated authoritative mode work is persistent, exactly like
                # work produced by notify_ingest in the current release.
                r.set(mode_key, generation, nx=True)
                current = r.get(mode_key)
            if current is None or not _redis_values_equal(current, generation):
                # A newer/different generation already owns this mode. Keep
                # the scalar so the older generation can be migrated after it
                # drains; deleting it here would lose pending work.
                all_ready = False
        except Exception:
            # Do not delete the scalar marker unless every requested mode had a
            # matching durable pending generation.
            all_ready = False
            break
    if all_ready:
        _consume_pending_generation(r, legacy_key, generation)


def _resolve_unresolved_pending(
    r,
    org: str,
    sup: str,
    table: str,
    modes: Tuple[str, ...],
) -> None:
    """Resolve a pre-config ingest marker after strict schedule reads recover."""

    unresolved_key = _unresolved_pending_key(org, sup, table)
    try:
        generation = r.get(unresolved_key)
    except Exception:
        return
    if generation is None:
        return
    if not modes:
        _consume_pending_generation(r, unresolved_key, generation)
        return

    try:
        pipe = r.pipeline(transaction=True)
        pipe.set(
            _pending_key(org, sup, table),
            generation,
            ex=DEFAULT_PENDING_TTL_SECONDS,
        )
        for mode in modes:
            pipe.set(_pending_key(org, sup, table, mode), generation)
        results = pipe.execute()
        if not all(results):
            return
    except Exception:
        # The persistent unresolved key is the retry record. Never consume it
        # after an ambiguous/partial resolution attempt.
        return
    _consume_pending_generation(r, unresolved_key, generation)


# ──────────────────────────────────────────────────────────────────────
# General helpers
# ──────────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _discover_dq_pairs(r) -> List[Tuple[str, str]]:
    """Find all org:sup pairs that have root meta keys."""
    from supertable import redis_keys as RK
    pairs = []
    try:
        cursor = 0
        while True:
            cursor, keys = r.scan(
                cursor=cursor,
                match=RK.meta_root_pattern_all_orgs(),
                count=500,
            )
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                parsed = RK.parse_lake_key(k)
                if parsed is not None:
                    pairs.append(parsed)
            if cursor == 0:
                break
    except Exception as e:
        logger.warning(f"[dq-scheduler] discover pairs failed: {e}")
    return list(set(pairs))


def _list_tables(r, org: str, sup: str) -> List[str]:
    """List all table names for an org:sup."""
    from supertable import redis_keys as RK
    tables = []
    try:
        pattern = RK.meta_leaf_pattern(org, sup)
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                simple = k.rsplit("meta:leaf:doc:", 1)[-1]
                if simple and not simple.startswith("__"):
                    tables.append(simple)
            if cursor == 0:
                break
    except Exception as e:
        logger.warning(f"[dq-scheduler] list_tables failed: {e}")
    return sorted(set(tables))
