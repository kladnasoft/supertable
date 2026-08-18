from __future__ import annotations

import json
import signal
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import fakeredis
import pytest

from supertable.quality import scheduler
from supertable.quality.cron import CronSchedule


ORG = "quality-org"
SUPER = "quality-lake"
TABLE = "facts"


def _ignore_process_cancellation(_send_connection, entered) -> None:
    """Spawn-safe stand-in for a native driver that never returns."""

    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, lambda *_args: None)
    entered.set()
    while True:
        time.sleep(0.05)


class _MemoryDQConfig:
    def __init__(self):
        self.latest = None

    def get_latest(self, _table):
        return self.latest

    def set_latest(self, _table, value):
        self.latest = value
        return True


def _wait_for(predicate, timeout: float = 2.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.005)
    return bool(predicate())


def _latest_quick() -> dict:
    return {
        "checked_at": "2026-08-18T10:00:00+00:00",
        "mode_results": {
            "quick": {
                "checked_at": "2026-08-18T10:00:00+00:00",
                "total_checks": 1,
                "evaluated": 1,
                "passed": 1,
                "warnings": 0,
                "critical": 0,
                "errors": 0,
                "skipped": 0,
                "not_applicable": 0,
                "anomalies": [],
                "outcomes": [],
            }
        },
    }


def test_cron_preserves_timezone_phase_and_both_fall_back_folds():
    zone = ZoneInfo("Europe/Budapest")
    schedule = CronSchedule.parse("30 2 * * *", "Europe/Budapest")
    base = datetime(2026, 10, 24, 2, 30, tzinfo=zone)

    first_ms = schedule.next_after_ms(int(base.timestamp() * 1000))
    second_ms = schedule.next_after_ms(first_ms)
    first = datetime.fromtimestamp(first_ms / 1000, zone)
    second = datetime.fromtimestamp(second_ms / 1000, zone)

    assert (first.hour, first.minute, first.fold) == (2, 30, 0)
    assert (second.hour, second.minute, second.fold) == (2, 30, 1)
    assert second_ms - first_ms == 60 * 60 * 1000

    spring = datetime(2026, 3, 28, 2, 30, tzinfo=zone)
    spring_next = datetime.fromtimestamp(
        schedule.next_after_ms(int(spring.timestamp() * 1000)) / 1000,
        zone,
    )
    # 02:30 does not exist on this date; the calendar trigger advances to the
    # corresponding first real instant instead of using a fixed 24h interval.
    assert (spring_next.month, spring_next.day, spring_next.hour, spring_next.minute) == (
        3,
        29,
        3,
        30,
    )


def test_table_timezone_is_validated_even_when_crons_are_inherited():
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    config = DQConfig(redis_client, ORG, SUPER)
    assert config.set_table_schedule(TABLE, {"timezone": "Mars/Olympus"}) is False
    assert config.get_table_schedule(TABLE) is None


def test_cron_next_due_and_last_outcome_survive_scheduler_restart():
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    now_ms = int(
        datetime(2026, 8, 18, 10, 5, tzinfo=ZoneInfo("UTC")).timestamp()
        * 1000
    )
    due, initial = scheduler._cron_schedule_state(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        "15 * * * *",
        "UTC",
        now_ms,
    )
    assert due is False
    scheduled_ms = initial["next_due_ms"]

    # A fresh call with no process-local last-run state sees the persisted due.
    due_after_restart, recovered = scheduler._cron_schedule_state(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        "15 * * * *",
        "UTC",
        scheduled_ms,
    )
    assert due_after_restart is True
    assert recovered == initial

    scheduler._record_cron_outcome(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        recovered,
        scheduler._success("quick", evaluated=1, passed=1),
        scheduled_ms + 1,
    )
    stored = json.loads(redis_client.get(
        scheduler._cron_state_key(ORG, SUPER, TABLE, "quick")
    ))
    assert stored["last_scheduled_ms"] == scheduled_ms
    assert stored["last_outcome"] == "success"
    assert stored["next_due_ms"] > scheduled_ms

    # A failure is persisted truthfully and keeps the exact phase retryable.
    _, next_state = scheduler._cron_schedule_state(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        "15 * * * *",
        "UTC",
        stored["next_due_ms"],
    )
    scheduler._record_cron_outcome(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        next_state,
        scheduler._failed("quick", "engine unavailable"),
        stored["next_due_ms"] + 1,
    )
    failed = json.loads(redis_client.get(
        scheduler._cron_state_key(ORG, SUPER, TABLE, "quick")
    ))
    assert failed["status"] == "retry"
    assert failed["last_outcome"] == "failed"
    assert failed["last_message"] == "engine unavailable"
    assert failed["next_due_ms"] == stored["next_due_ms"]


def test_history_outbox_retries_same_id_and_acks_only_after_sink(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    redis_client.set(running_key, "owner")
    guard = scheduler._LeaseGuard(redis_client, running_key, "owner")
    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        lambda *_args, **_kwargs: False,
    )

    # A failed Parquet write still succeeds from the scheduler's perspective
    # only because the exact immutable payload is durably queued first.
    assert scheduler._write_mode_history(
        ORG,
        SUPER,
        TABLE,
        "quick",
        _latest_quick(),
        12,
        lease_guard=guard,
    )
    outbox_key = scheduler._history_outbox_key(ORG, SUPER)
    queued = redis_client.hgetall(outbox_key)
    assert len(queued) == 1
    history_id, encoded = next(iter(queued.items()))
    assert json.loads(encoded)["history_id"] == history_id

    attempts = []
    monkeypatch.setattr(
        "supertable.quality.history.write_history_payload",
        lambda payload: attempts.append(payload["history_id"]) and False,
    )
    assert scheduler._drain_history_outbox(redis_client, ORG, SUPER) == 0
    assert redis_client.hget(outbox_key, history_id) == encoded

    def deliver(payload):
        attempts.append(payload["history_id"])
        return True

    monkeypatch.setattr(
        "supertable.quality.history.write_history_payload",
        deliver,
    )
    assert scheduler._drain_history_outbox(redis_client, ORG, SUPER) == 1
    assert not redis_client.exists(outbox_key)
    assert attempts == [history_id, history_id]


def test_history_outbox_enqueue_is_atomic_with_exact_lease(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    outbox_key = scheduler._history_outbox_key(ORG, SUPER)
    redis_client.set(running_key, "owner")

    class LeaseLossAtQueue:
        """Remove the lease immediately before Redis evaluates the script."""

        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if "hsetnx" in script.casefold():
                redis_client.delete(running_key)
            return redis_client.eval(script, *args)

    guard = scheduler._LeaseGuard(LeaseLossAtQueue(), running_key, "owner")
    sink_calls = []
    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        lambda *_args, **_kwargs: sink_calls.append(True) or True,
    )

    with pytest.raises(scheduler._LeaseLostError):
        scheduler._write_mode_history(
            ORG,
            SUPER,
            TABLE,
            "quick",
            _latest_quick(),
            12,
            lease_guard=guard,
        )

    assert guard.lost.is_set()
    assert redis_client.hlen(outbox_key) == 0
    assert sink_calls == []


def test_history_outbox_id_is_idempotent_but_never_overwritten():
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    outbox_key = scheduler._history_outbox_key(ORG, SUPER)
    redis_client.set(running_key, "owner")
    guard = scheduler._LeaseGuard(redis_client, running_key, "owner")

    assert scheduler._queue_history_outbox_if_owned(
        guard, outbox_key, "history-1", '{"value":1}',
    )
    assert scheduler._queue_history_outbox_if_owned(
        guard, outbox_key, "history-1", '{"value":1}',
    )
    assert not scheduler._queue_history_outbox_if_owned(
        guard, outbox_key, "history-1", '{"value":2}',
    )
    assert redis_client.hget(outbox_key, "history-1") == '{"value":1}'


def test_deadline_releases_exact_lease_and_fences_late_publication(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    dqc = _MemoryDQConfig()
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "generation-1")
    entered = threading.Event()
    release = threading.Event()
    result = []

    def blocked_runner(*_args, **_kwargs):
        entered.set()
        release.wait(2)
        return scheduler._success("quick", evaluated=1, passed=1)

    monkeypatch.setattr(scheduler, "_run_quick_check", blocked_runner)
    worker = threading.Thread(
        target=lambda: result.append(scheduler._try_run_check(
            redis_client,
            ORG,
            SUPER,
            TABLE,
            "quick",
            dqc,
            30,
            pending_generation="generation-1",
            deadline_monotonic=time.monotonic() + 0.05,
        )),
    )
    worker.start()
    assert entered.wait(1)

    retry_key = scheduler._retry_key(ORG, SUPER, TABLE, "quick")
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    assert _wait_for(lambda: bool(redis_client.exists(retry_key)))
    assert not redis_client.exists(running_key)
    assert redis_client.get(pending_key) == "generation-1"
    assert worker.is_alive()  # driver is still blocked, but cannot publish

    release.set()
    worker.join(1)
    assert result and result[0].state == "failed"
    assert "deadline" in result[0].message
    assert dqc.latest is None
    assert not redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )


def test_noncooperative_process_is_killed_at_deadline_and_capacity_recovers():
    scheduler._scheduler_stop.clear()
    context = scheduler._quality_process_context()
    entered = threading.Event()
    process_entered = context.Event()
    result = []
    replacement_ran = threading.Event()
    executor = ThreadPoolExecutor(max_workers=1)
    slots = threading.BoundedSemaphore(1)
    with scheduler._active_jobs_lock:
        scheduler._active_jobs.clear()

    def isolated_job():
        entered.set()
        result.append(scheduler._run_killable_subprocess(
            _ignore_process_cancellation,
            (process_entered,),
            deadline_monotonic=time.monotonic() + 2.0,
            cancel_event=scheduler._scheduler_stop,
            lease_lost_event=None,
            process_name="dq-test-deadline",
        ))

    try:
        assert scheduler._submit_table_job(
            executor, slots, "noncooperative", isolated_job,
        )
        assert entered.wait(1)
        assert process_entered.wait(3)
        assert _wait_for(lambda: not scheduler._active_jobs, timeout=3)
        assert result and result[0].state == "deadline"
        assert result[0].force_terminated is True
        assert result[0].alive_after_cleanup is False

        # The same one-worker executor and one-slot admission gate can execute
        # new work; the ignored cancellation did not strand either resource.
        assert scheduler._submit_table_job(
            executor, slots, "replacement", replacement_ran.set,
        )
        assert replacement_ran.wait(1)
        assert _wait_for(lambda: not scheduler._active_jobs)
    finally:
        scheduler._scheduler_stop.clear()
        executor.shutdown(wait=True)


def test_shutdown_force_terminates_noncooperative_scheduled_process():
    scheduler.stop_scheduler(timeout_s=1)
    scheduler._scheduler_stop.clear()
    context = scheduler._quality_process_context()
    process_entered = context.Event()
    result = []
    executor = ThreadPoolExecutor(max_workers=1)
    slots = threading.BoundedSemaphore(1)
    with scheduler._active_jobs_lock:
        scheduler._active_jobs.clear()

    def isolated_job():
        result.append(scheduler._run_killable_subprocess(
            _ignore_process_cancellation,
            (process_entered,),
            deadline_monotonic=time.monotonic() + 30,
            cancel_event=scheduler._scheduler_stop,
            lease_lost_event=None,
            process_name="dq-test-shutdown",
        ))

    try:
        assert scheduler._submit_table_job(
            executor, slots, "noncooperative-shutdown", isolated_job,
        )
        assert process_entered.wait(2)
        assert scheduler.stop_scheduler(timeout_s=2) is True
        assert result and result[0].state == "shutdown"
        assert result[0].force_terminated is True
        assert result[0].alive_after_cleanup is False
        assert scheduler.scheduler_health()["active_jobs"] == 0
    finally:
        scheduler._scheduler_stop.clear()
        executor.shutdown(wait=True)


def test_worker_pool_is_bounded_concurrent_and_suppresses_duplicate_tables():
    started = threading.Event()
    release = threading.Event()
    state_lock = threading.Lock()
    running = 0
    max_running = 0

    def job():
        nonlocal running, max_running
        with state_lock:
            running += 1
            max_running = max(max_running, running)
            if running == 2:
                started.set()
        release.wait(2)
        with state_lock:
            running -= 1

    with scheduler._active_jobs_lock:
        scheduler._active_jobs.clear()
    executor = ThreadPoolExecutor(max_workers=2)
    slots = threading.BoundedSemaphore(2)
    try:
        assert scheduler._submit_table_job(executor, slots, "a", job)
        assert scheduler._submit_table_job(executor, slots, "b", job)
        assert started.wait(1)
        assert not scheduler._submit_table_job(executor, slots, "a", job)
        assert not scheduler._submit_table_job(executor, slots, "c", job)
        assert max_running == 2
    finally:
        release.set()
        executor.shutdown(wait=True)
    assert _wait_for(lambda: not scheduler._active_jobs)


def test_scheduler_rotates_submission_order_under_capacity_pressure(monkeypatch):
    class Config:
        def __init__(self, *_args):
            pass

        def get_schedule(self):
            return {
                "enabled": True,
                "cooldown_seconds": 30,
                "timezone": "UTC",
                "quick_cron": "0 * * * *",
                "deep_cron": "0 2 * * *",
                "custom_cron": "0 * * * *",
            }

    class InlineExecutor:
        def submit(self, function, *args):
            future = Future()
            try:
                future.set_result(function(*args))
            except Exception as exc:  # pragma: no cover - diagnostic path
                future.set_exception(exc)
            return future

    order = []
    monkeypatch.setattr(
        "supertable.redis_connector.create_redis_client", lambda: object(),
    )
    monkeypatch.setattr("supertable.quality.config.DQConfig", Config)
    monkeypatch.setattr(scheduler, "_discover_dq_pairs", lambda _r: [(ORG, SUPER)])
    monkeypatch.setattr(scheduler, "_list_tables", lambda *_args: ["a", "b", "c"])
    monkeypatch.setattr(scheduler, "_drain_history_outbox", lambda *_args: 0)
    monkeypatch.setattr(
        scheduler,
        "_process_table_job",
        lambda *args: order.append((args[3], args[-1])),
    )
    monkeypatch.setattr(scheduler, "_scheduler_fair_cursor", 0)
    slots = threading.BoundedSemaphore(10)
    executor = InlineExecutor()

    scheduler._scheduler_tick({}, {}, {}, executor=executor, slots=slots)
    first = list(order)
    order.clear()
    scheduler._scheduler_tick({}, {}, {}, executor=executor, slots=slots)

    assert first == [("a", True), ("b", True), ("c", True)]
    assert order == [("b", True), ("c", True), ("a", True)]


def test_start_stop_health_api_has_bounded_graceful_join():
    # The initial ten-second warm-up uses Event.wait, so shutdown interrupts it
    # immediately rather than sleeping through an application lifespan exit.
    scheduler.stop_scheduler(timeout_s=1)
    assert scheduler.start_scheduler() is True
    assert scheduler.stop_scheduler(timeout_s=1) is True
    health = scheduler.scheduler_health()
    assert health["thread_alive"] is False
    assert health["active_jobs"] == 0
    assert health["running"] is False
    assert health["max_workers"] >= 1
