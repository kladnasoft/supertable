# route: supertable.quality.scheduler
# supertable/quality/scheduler.py
"""
Lightweight background scheduler for Data Quality checks.

Runs as a daemon thread inside the FastAPI process — no external service needed.
Supports cron expressions for quick and deep profiles.

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
import threading
import time
import traceback
import uuid
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
_scheduler_lock = threading.Lock()


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


def _scheduler_loop() -> None:
    """Main loop — checks every 60s if any cron expression is due."""
    # Wait for the app to fully initialize
    time.sleep(10)

    last_quick_run: Dict[str, float] = {}  # "org:sup:table" -> epoch
    last_deep_run: Dict[str, float] = {}
    last_custom_run: Dict[str, float] = {}

    while True:
        try:
            _scheduler_tick(last_quick_run, last_deep_run, last_custom_run)
        except Exception:
            logger.error(f"[dq-scheduler] Error in tick:\n{traceback.format_exc()}")
        time.sleep(TICK_INTERVAL_SECONDS)


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
        t = threading.Thread(
            target=_scheduler_loop,
            name="supertable-dq-scheduler",
            daemon=True,
        )
        _scheduler_thread = t
        t.start()
        logger.info("[dq-scheduler] Background scheduler started")
        return True


def _scheduler_tick(
    last_quick_run: Dict[str, float],
    last_deep_run: Dict[str, float],
    last_custom_run: Dict[str, float],
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

    for org, sup in pairs:
        dqc = DQConfig(r, org, sup)
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
            tkey = f"{org}:{sup}:{table_name}"

            # Table-specific schedule override
            try:
                ts = dqc.get_table_schedule(table_name)
                table_schedule = ts or {}
                table_cooldown_sec = int(
                    table_schedule.get("cooldown_seconds", cooldown_sec)
                )
            except (DQConfigReadError, TypeError, ValueError, OverflowError) as exc:
                _record_tick_config_failure(
                    r,
                    org,
                    sup,
                    table_name,
                    dqc,
                    QUALITY_MODES,
                    cooldown_sec,
                    f"Table quality schedule could not be read safely: {exc}",
                )
                continue
            if ts and not ts.get("enabled", True):
                _resolve_unresolved_pending(
                    r, org, sup, table_name, (),
                )
                continue

            # ── POST-INGEST PENDING CHECKS ───────────────────────
            # Pending work runs before cron work.  Otherwise the first cron
            # run in a new process starts with an empty in-memory last-run map,
            # sets cooldown, and needlessly postpones the ingest generation.
            post_ingest_modes = _post_ingest_modes(schedule, table_schedule)
            _resolve_unresolved_pending(
                r,
                org,
                sup,
                table_name,
                post_ingest_modes,
            )
            _migrate_legacy_pending(
                r,
                org,
                sup,
                table_name,
                post_ingest_modes,
            )
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
                generation = pending_generations[mode]
                if generation is None:
                    continue
                outcome = _try_run_check(
                    r,
                    org,
                    sup,
                    table_name,
                    mode,
                    dqc,
                    table_cooldown_sec,
                    pending_generation=generation,
                )
                if outcome.successful:
                    last_run_maps[mode][tkey] = now

            table_quick_cron = (ts or {}).get("quick_cron") or quick_cron

            # ── CRON-BASED QUICK CHECK ────────────────────────────
            quick_interval = _cron_to_seconds(table_quick_cron)
            last_q = last_quick_run.get(tkey, 0)

            if "quick" not in handled_pending and now - last_q >= quick_interval:
                if _try_run_check(r, org, sup, table_name, "quick", dqc, table_cooldown_sec):
                    last_quick_run[tkey] = now

            # ── CRON-BASED DEEP CHECK ─────────────────────────────
            table_deep_cron = (ts or {}).get("deep_cron") or deep_cron
            deep_interval = _cron_to_seconds(table_deep_cron)
            last_d = last_deep_run.get(tkey, 0)
            deep_enabled = (ts or {}).get("deep_enabled", True)

            if deep_enabled and "deep" not in handled_pending and now - last_d >= deep_interval:
                try:
                    eff_config = dqc.get_effective_config(table_name)
                    has_deep = any(
                        v.get("enabled")
                        for k, v in (eff_config.get("checks") or {}).items()
                        if k.startswith("D")
                    )
                    stale_deep = _has_stale_mode_result(
                        dqc, table_name, "deep"
                    )
                except DQConfigReadError as exc:
                    _record_tick_config_failure(
                        r,
                        org,
                        sup,
                        table_name,
                        dqc,
                        ("deep",),
                        table_cooldown_sec,
                        f"Deep quality config could not be read safely: {exc}",
                    )
                    has_deep = False
                    stale_deep = False
                if has_deep or stale_deep:
                    if _try_run_check(r, org, sup, table_name, "deep", dqc, table_cooldown_sec):
                        last_deep_run[tkey] = now

            # ── CRON-BASED CUSTOM RULES CHECK ─────────────────────
            table_custom_cron = (ts or {}).get("custom_cron") or custom_cron
            custom_interval = _cron_to_seconds(table_custom_cron)
            last_c = last_custom_run.get(tkey, 0)
            custom_enabled = (ts or {}).get("custom_enabled", True)

            if custom_enabled and "custom" not in handled_pending and now - last_c >= custom_interval:
                try:
                    has_rules = bool(dqc.list_rules_for_table(table_name))
                    stale_custom = _has_stale_mode_result(
                        dqc, table_name, "custom"
                    )
                except DQConfigReadError as exc:
                    _record_tick_config_failure(
                        r,
                        org,
                        sup,
                        table_name,
                        dqc,
                        ("custom",),
                        table_cooldown_sec,
                        f"Custom quality rules could not be read safely: {exc}",
                    )
                    has_rules = False
                    stale_custom = False
                if has_rules or stale_custom:
                    if _try_run_check(r, org, sup, table_name, "custom", dqc, table_cooldown_sec):
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
    )


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
) -> CheckRunOutcome:
    """
    Attempt to run a quality check with lock + cooldown protection.

    Returns a structured outcome.  It is truthy only for complete success.
    """
    if mode not in QUALITY_MODES:
        return _failed(mode, f"unknown quality mode: {mode}")

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
    except Exception as exc:
        # Do not execute without an active renewer: the query may outlive the
        # initial TTL. Release only our token and expose the failed attempt.
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
            )
        except _LeaseLostError:
            pass
        return outcome

    finally:
        # Stop renewal before releasing.  Both renewal and release are
        # ownership-checked, so an expired/reacquired lease is never damaged.
        lease_stop.set()
        if lease_thread is not None:
            lease_thread.join(timeout=1.0)
        _delete_if_value(r, running_key, lock_token)


# ──────────────────────────────────────────────────────────────────────
# Check execution
# ──────────────────────────────────────────────────────────────────────


def _execute_quality_statement(
    org: str,
    sup: str,
    sql: str,
):
    """Execute through the AUTO certification boundary used by all DQ SQL."""

    from supertable.quality.execution import execute_quality_sql

    return execute_quality_sql(
        organization=org,
        super_name=sup,
        sql=sql,
        role_name="superadmin",
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


def _table_snapshot_metadata(
    mr,
    table_name: str,
) -> Tuple[Any, Optional[int], Optional[int]]:
    """Return sealed snapshot timestamp, physical bytes, and live rows.

    Size and row completeness are independent.  In particular, a valid table
    may have no public columns, so its row count must come from the pinned
    resource metadata rather than from an invalid empty-column SQL view.  A
    deletion vector is subtracted only when its persisted row-count seal is
    complete and internally consistent.
    """

    try:
        records = mr.get_table_stats(table_name, "superadmin") or []
    except Exception:
        return None, None, None
    if not records or not isinstance(records[0], dict):
        return None, None, None
    snapshot = records[0]
    resources = snapshot.get("resources")
    if not isinstance(resources, list):
        return snapshot.get("last_updated_ms"), None, None

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
        snapshot.get("last_updated_ms"),
        total_size if size_complete else None,
        total_rows if rows_complete else None,
    )


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
) -> None:
    """Best-effort history write using only the completed mode's counters."""

    try:
        from supertable.quality.history import write_history, write_history_via_sql

        history_document = _mode_history_document(latest, mode)
        _assert_lease_owned(lease_guard)
        wrote = write_history(
            org,
            sup,
            table_name,
            mode,
            history_document,
            elapsed_ms,
        )
        _assert_lease_owned(lease_guard)
        if not wrote:
            write_history_via_sql(
                org,
                sup,
                table_name,
                mode,
                history_document,
                elapsed_ms,
            )
            _assert_lease_owned(lease_guard)
    except _LeaseLostError:
        raise
    except Exception as exc:
        logger.debug(
            "[dq-scheduler] History write skipped for %s %s: %s",
            mode,
            table_name,
            exc,
        )


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
        if not schema_raw or not schema_raw[0]:
            return _failed("quick", f"No schema for {table_name}")
        schema_dict = schema_raw[0]
        columns = filter_visible_columns(list(schema_dict.items()))
        (
            last_modified_at,
            current_size_bytes,
            metadata_row_count,
        ) = _table_snapshot_metadata(mr, table_name)
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
    _write_mode_history(
        org,
        sup,
        table_name,
        "quick",
        latest,
        _check_elapsed_ms,
        lease_guard=lease_guard,
    )

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
        if not schema_raw or not schema_raw[0]:
            return _failed("deep", f"No schema for {table_name}")
        schema_dict = schema_raw[0]
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
        _write_mode_history(
            org,
            sup,
            table_name,
            "deep",
            latest,
            int(time.time() * 1000) - _deep_start_ms,
            lease_guard=lease_guard,
        )
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
            execution = _execute_quality_statement(org, sup, sql)
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
    _write_mode_history(
        org,
        sup,
        table_name,
        "deep",
        latest,
        _deep_elapsed_ms,
        lease_guard=lease_guard,
    )

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
        if not schema_raw or not schema_raw[0]:
            return _failed("custom", f"No schema for {table_name}")
        columns = filter_visible_columns(list(schema_raw[0].items()))
        _last_modified, _table_size, metadata_row_count = (
            _table_snapshot_metadata(mr, table_name)
        )
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
        _write_mode_history(
            org,
            sup,
            table_name,
            "custom",
            latest,
            int(time.time() * 1000) - _custom_start_ms,
            lease_guard=lease_guard,
        )
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
    _write_mode_history(
        org,
        sup,
        table_name,
        "custom",
        latest,
        _custom_elapsed_ms,
        lease_guard=lease_guard,
    )

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


def _cron_to_seconds(cron_expr: str) -> int:
    """
    Simplified cron-to-interval converter.
    Handles common patterns: */N hours, daily, etc.
    Falls back to 4 hours for complex expressions.
    """
    parts = cron_expr.strip().split()
    if len(parts) != 5:
        return 4 * 3600

    minute, hour, dom, month, dow = parts

    if hour.startswith("*/"):
        try:
            return int(hour[2:]) * 3600
        except ValueError:
            pass

    if minute.startswith("*/"):
        try:
            return int(minute[2:]) * 60
        except ValueError:
            pass

    if dom == "*" and month == "*" and dow == "*" and not hour.startswith("*"):
        return 24 * 3600

    if hour == "*/1":
        return 3600

    return 4 * 3600
