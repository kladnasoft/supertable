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

from supertable import redis_keys as RK
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


@pytest.mark.parametrize(
    ("root_payload", "leaf_payload"),
    [
        (None, '{"version":1,"ts":1,"path":"snapshot.json"}'),
        ('{"version":1,"ts":1}', '{"version":1,"path":"snapshot.json"}'),
        (
            '{"version":9007199254740992,"ts":1}',
            '{"version":1,"ts":1,"path":"snapshot.json"}',
        ),
    ],
)
def test_config_failure_fallback_rejects_orphan_or_invalid_catalog_identity(
    root_payload,
    leaf_payload,
):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    if root_payload is not None:
        redis_client.set(RK.meta_root(ORG, SUPER), root_payload)
    redis_client.set(RK.meta_leaf(ORG, SUPER, TABLE), leaf_payload)

    scheduler._record_tick_config_failure(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        DQConfig(redis_client, ORG, SUPER),
        ("quick",),
        1,
        "injected malformed schedule",
    )

    assert not redis_client.exists(scheduler._running_key(ORG, SUPER, TABLE))
    assert not redis_client.exists(
        scheduler._retry_key(ORG, SUPER, TABLE, "quick")
    )
    assert DQConfig(redis_client, ORG, SUPER).get_latest(TABLE) is None


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


def test_cron_phase_rejects_adjacent_lua_unsafe_integer_identities(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    unsafe_due = 1 << 53
    stale_state = {
        "schema_version": 1,
        "expression": "* * * * *",
        "timezone": "UTC",
        "status": "scheduled",
        "next_due_ms": unsafe_due,
        "last_scheduled_ms": None,
        "last_started_ms": None,
        "last_completed_ms": None,
        "last_outcome": None,
    }
    current_state = dict(stale_state, next_due_ms=unsafe_due + 1)
    redis_client.set(
        scheduler._cron_state_key(ORG, SUPER, TABLE, "quick"),
        json.dumps(current_state),
    )
    executions = []
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (
            executions.append(True)
            or scheduler._success("quick", evaluated=1, passed=1)
        ),
    )

    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        _MemoryDQConfig(),
        0,
        lease_admission=(
            scheduler._cron_state_key(ORG, SUPER, TABLE, "quick"),
            1,
            "* * * * *",
            "UTC",
            unsafe_due,
        ),
        cron_state=stale_state,
    )

    assert outcome.state == "failed"
    assert "invalid" in outcome.message
    assert executions == []


def test_noncanonical_valid_cron_json_is_admitted_and_advanced(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    now_ms = int(
        datetime(2026, 8, 18, 10, 15, tzinfo=ZoneInfo("UTC")).timestamp()
        * 1000
    )
    state = {
        "next_due_ms": now_ms,
        "timezone": "UTC",
        "expression": "* * * * *",
        "schema_version": 1,
        "status": "scheduled",
        "last_scheduled_ms": None,
        "last_started_ms": None,
        "last_completed_ms": None,
        "last_outcome": None,
    }
    key = scheduler._cron_state_key(ORG, SUPER, TABLE, "quick")
    noncanonical = json.dumps(state, indent=2, sort_keys=False)
    redis_client.set(key, noncanonical)

    due, recovered = scheduler._cron_schedule_state(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        "* * * * *",
        "UTC",
        now_ms,
    )
    assert due is True
    assert redis_client.get(key) == noncanonical

    executions = []
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (
            executions.append(True)
            or scheduler._success("quick", evaluated=1, passed=1)
        ),
    )
    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        _MemoryDQConfig(),
        0,
        lease_admission=scheduler._cron_state_admission(
            ORG, SUPER, TABLE, "quick", recovered,
        ),
        owned_completion=lambda completed, guard: scheduler._record_cron_outcome(
            redis_client,
            ORG,
            SUPER,
            TABLE,
            "quick",
            recovered,
            completed,
            now_ms + 1,
            lease_guard=guard,
        ),
    )

    assert outcome.successful
    assert executions == [True]
    advanced = json.loads(redis_client.get(key))
    assert advanced["last_outcome"] == "success"
    assert advanced["next_due_ms"] > now_ms


def test_history_outbox_retries_same_id_and_acks_only_after_sink(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    redis_client.set(running_key, "owner")
    guard = scheduler._LeaseGuard(redis_client, running_key, "owner")
    direct_sink_calls = []
    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        lambda *_args, **_kwargs: direct_sink_calls.append(True) or False,
    )

    prepared = scheduler._write_mode_history(
        ORG,
        SUPER,
        TABLE,
        "quick",
        _latest_quick(),
        12,
        lease_guard=guard,
    )
    assert isinstance(prepared, scheduler._PreparedHistory)
    outbox_key = scheduler._history_outbox_key(ORG, SUPER)
    assert redis_client.hlen(outbox_key) == 0
    assert redis_client.get(prepared.prepared_key) == prepared.payload
    assert direct_sink_calls == []

    assert scheduler._finalize_success_if_owned(
        redis_client,
        guard,
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick"),
        scheduler._retry_key(ORG, SUPER, TABLE, "quick"),
        0,
        prepared_history=prepared,
    )
    history_id = prepared.history_id
    encoded = prepared.payload
    assert redis_client.get(prepared.prepared_key) is None
    assert redis_client.hget(outbox_key, history_id) == encoded

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


def test_scheduler_history_sink_is_not_called_before_success_commit(
    monkeypatch,
):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    redis_client.set(running_key, "owner")
    guard = scheduler._LeaseGuard(redis_client, running_key, "owner")

    def raise_from_sink(*_args, **_kwargs):
        raise RuntimeError("injected parquet sink exception")

    monkeypatch.setattr(
        "supertable.quality.history.write_history",
        raise_from_sink,
    )

    prepared = scheduler._write_mode_history(
        ORG,
        SUPER,
        TABLE,
        "quick",
        _latest_quick(),
        12,
        lease_guard=guard,
    )
    assert isinstance(prepared, scheduler._PreparedHistory)
    assert redis_client.get(prepared.prepared_key) == prepared.payload
    assert redis_client.hlen(scheduler._history_outbox_key(ORG, SUPER)) == 0
    assert guard.owns_lease()


def test_rejected_success_commit_cannot_deliver_phantom_history(monkeypatch):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        '{"version":1,"ts":1}',
    )
    redis_client.set(
        RK.meta_leaf(ORG, SUPER, TABLE),
        '{"version":1,"ts":1,"path":"old.json"}',
    )
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None

    class ReplaceLeafAtSuccessCommit:
        def __init__(self, inner):
            self.inner = inner
            self.replaced = False

        def __getattr__(self, name):
            return getattr(self.inner, name)

        def eval(self, script, *args):
            if "atomic quality success document publication" in script:
                self.replaced = True
                self.inner.set(
                    RK.meta_leaf(ORG, SUPER, TABLE),
                    '{"version":2,"ts":2,"path":"new.json"}',
                )
            return self.inner.eval(script, *args)

    raced = ReplaceLeafAtSuccessCommit(redis_client)
    sink_calls = []
    monkeypatch.setattr(
        "supertable.quality.history.write_history_payload",
        lambda payload: sink_calls.append(payload) or True,
    )

    def successful_runner(
        _r, org, sup, table, _dqc, *, lease_guard=None,
    ):
        prepared = scheduler._write_mode_history(
            org,
            sup,
            table,
            "quick",
            _latest_quick(),
            1,
            lease_guard=lease_guard,
        )
        assert isinstance(prepared, scheduler._PreparedHistory)
        return scheduler._success(
            "quick",
            publication=[(("latest", table), _latest_quick())],
            prepared_history=prepared,
        )

    monkeypatch.setattr(scheduler, "_run_quick_check", successful_runner)
    outcome = scheduler._try_run_check(
        raced,
        ORG,
        SUPER,
        TABLE,
        "quick",
        DQConfig(raced, ORG, SUPER),
        0,
        table_admission=admission,
    )

    assert outcome.state == "failed"
    assert raced.replaced
    assert sink_calls == []
    assert redis_client.hlen(scheduler._history_outbox_key(ORG, SUPER)) == 0
    assert not redis_client.keys(
        scheduler._history_prepared_key(ORG, SUPER, TABLE, "*")
    )
    assert DQConfig(redis_client, ORG, SUPER).get_latest(TABLE) is None


def test_huge_cooldown_is_rejected_before_success_baseline_mutation():
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        '{"version":1,"ts":1}',
    )
    redis_client.set(
        RK.meta_leaf(ORG, SUPER, TABLE),
        '{"version":1,"ts":1,"path":"snapshot.json"}',
    )
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None
    config = DQConfig(redis_client, ORG, SUPER)
    assert config.set_latest(TABLE, {"baseline": "old"})
    old_latest = redis_client.get(config._key("latest", TABLE))
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    redis_client.set(running_key, "owner")
    guard = scheduler._LeaseGuard(
        redis_client,
        running_key,
        "owner",
        table_admission=admission,
    )

    with pytest.raises(ValueError, match="Redis-safe integer"):
        scheduler._commit_success_if_owned(
            config,
            org=ORG,
            sup=SUPER,
            table_name=TABLE,
            outcome=scheduler._success(
                "quick",
                publication=[
                    (("latest", TABLE), {"baseline": "new"}),
                    (("anomalies", TABLE), []),
                ],
            ),
            lease_guard=guard,
            cooldown_key=scheduler._cooldown_key(ORG, SUPER, TABLE, "quick"),
            retry_key=scheduler._retry_key(ORG, SUPER, TABLE, "quick"),
            cooldown_sec=1 << 31,
            pending_key=None,
            pending_generation=None,
            cron_state=None,
            table_admission=admission,
            completion_event=threading.Event(),
        )

    assert redis_client.get(config._key("latest", TABLE)) == old_latest
    assert redis_client.get(config._key("anomalies", TABLE)) is None
    assert redis_client.get(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    ) is None
    assert redis_client.get(running_key) == "owner"


def test_wrong_type_pending_key_cannot_partially_commit_success(monkeypatch):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        '{"version":1,"ts":1}',
    )
    redis_client.set(
        RK.meta_leaf(ORG, SUPER, TABLE),
        '{"version":1,"ts":1,"path":"snapshot.json"}',
    )
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "generation")

    def successful_runner(
        _r, org, sup, table, _dqc, *, lease_guard=None,
    ):
        prepared = scheduler._write_mode_history(
            org,
            sup,
            table,
            "quick",
            _latest_quick(),
            1,
            lease_guard=lease_guard,
        )
        assert isinstance(prepared, scheduler._PreparedHistory)
        redis_client.delete(pending_key)
        redis_client.rpush(pending_key, "poison")
        return scheduler._success(
            "quick",
            publication=[
                (("latest", table), _latest_quick()),
                (("anomalies", table), []),
            ],
            prepared_history=prepared,
        )

    monkeypatch.setattr(scheduler, "_run_quick_check", successful_runner)
    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        DQConfig(redis_client, ORG, SUPER),
        30,
        pending_generation="generation",
        table_admission=admission,
    )

    assert outcome.state == "failed"
    config = DQConfig(redis_client, ORG, SUPER)
    assert config.get_latest(TABLE) is None
    assert config.get_anomalies(TABLE) == []
    assert redis_client.get(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    ) is None
    assert redis_client.get(
        scheduler._retry_key(ORG, SUPER, TABLE, "quick")
    ) is None
    assert redis_client.lrange(pending_key, 0, -1) == ["poison"]
    assert redis_client.hlen(scheduler._history_outbox_key(ORG, SUPER)) == 0
    assert not redis_client.keys(
        scheduler._history_prepared_key(ORG, SUPER, TABLE, "*")
    )


def test_committed_success_delivers_only_after_atomic_result_publication(
    monkeypatch,
):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        '{"version":1,"ts":1}',
    )
    redis_client.set(
        RK.meta_leaf(ORG, SUPER, TABLE),
        '{"version":1,"ts":1,"path":"snapshot.json"}',
    )
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None
    observed = []

    def deliver(payload):
        latest = DQConfig(redis_client, ORG, SUPER).get_latest(TABLE)
        observed.append((payload["history_id"], latest is not None))
        return True

    monkeypatch.setattr(
        "supertable.quality.history.write_history_payload",
        deliver,
    )

    def successful_runner(
        _r, org, sup, table, _dqc, *, lease_guard=None,
    ):
        prepared = scheduler._write_mode_history(
            org,
            sup,
            table,
            "quick",
            _latest_quick(),
            1,
            lease_guard=lease_guard,
        )
        assert isinstance(prepared, scheduler._PreparedHistory)
        assert observed == []
        return scheduler._success(
            "quick",
            publication=[(("latest", table), _latest_quick())],
            prepared_history=prepared,
        )

    monkeypatch.setattr(scheduler, "_run_quick_check", successful_runner)
    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        DQConfig(redis_client, ORG, SUPER),
        0,
        table_admission=admission,
    )

    assert outcome.successful
    assert observed and observed[0][1] is True
    assert redis_client.hlen(scheduler._history_outbox_key(ORG, SUPER)) == 0
    assert not redis_client.keys(
        scheduler._history_prepared_key(ORG, SUPER, TABLE, "*")
    )


def test_history_preparation_still_fences_lease_loss(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    redis_client.set(running_key, "owner")
    guard = scheduler._LeaseGuard(redis_client, running_key, "owner")

    class ReplaceOwnerAtPreparation:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if "prepare an immutable" in script:
                redis_client.set(running_key, "successor")
            return redis_client.eval(script, *args)

    guard.redis = ReplaceOwnerAtPreparation()

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
    assert redis_client.hlen(scheduler._history_outbox_key(ORG, SUPER)) == 0


def test_history_outbox_cursor_progresses_beyond_poison_first_page(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    outbox_key = scheduler._history_outbox_key(ORG, SUPER)
    for index in range(100):
        history_id = f"history-{index:03d}"
        redis_client.hset(
            outbox_key,
            history_id,
            json.dumps({"history_id": history_id}),
        )

    expected_cursor, first_page = redis_client.hscan(
        outbox_key, cursor=0, count=10,
    )
    poison = set(first_page)
    assert expected_cursor != 0
    attempted = []

    def deliver(payload):
        history_id = payload["history_id"]
        attempted.append(history_id)
        return history_id not in poison

    monkeypatch.setattr(
        "supertable.quality.history.write_history_payload",
        deliver,
    )

    assert scheduler._drain_history_outbox(
        redis_client, ORG, SUPER, limit=10,
    ) == 0
    assert set(attempted) == poison
    assert redis_client.get(
        scheduler._history_outbox_cursor_key(ORG, SUPER)
    ) == str(expected_cursor)

    attempted.clear()
    assert scheduler._drain_history_outbox(
        redis_client, ORG, SUPER, limit=10,
    ) > 0
    assert attempted
    assert not set(attempted) & poison
    assert all(not redis_client.hexists(outbox_key, item) for item in attempted)


def test_delayed_concurrent_outbox_drain_cannot_rewind_cursor(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    outbox_key = scheduler._history_outbox_key(ORG, SUPER)
    cursor_key = scheduler._history_outbox_cursor_key(ORG, SUPER)
    for index in range(200):
        history_id = f"history-{index:03d}"
        redis_client.hset(
            outbox_key,
            history_id,
            json.dumps({"history_id": history_id}),
        )

    first_cursor, _ = redis_client.hscan(outbox_key, cursor=0, count=10)
    second_cursor, _ = redis_client.hscan(
        outbox_key, cursor=first_cursor, count=10,
    )
    assert first_cursor != 0 and second_cursor not in (0, first_cursor)

    delayed_at_cas = threading.Event()
    release_delayed = threading.Event()
    delayed_thread_id = []

    class DelayFirstCursorCAS:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if (
                "quality history outbox cursor compare-and-set" in script
                and delayed_thread_id
                and threading.get_ident() == delayed_thread_id[0]
            ):
                delayed_at_cas.set()
                assert release_delayed.wait(timeout=3)
            return redis_client.eval(script, *args)

    wrapped = DelayFirstCursorCAS()
    monkeypatch.setattr(
        "supertable.quality.history.write_history_payload",
        lambda _payload: False,
    )

    def delayed_drain():
        delayed_thread_id.append(threading.get_ident())
        scheduler._drain_history_outbox(wrapped, ORG, SUPER, limit=10)

    delayed = threading.Thread(target=delayed_drain)
    delayed.start()
    assert delayed_at_cas.wait(timeout=2)

    # Both of these hosts start and finish after the delayed host's HSCAN.
    # They advance two pages while every poison payload remains retryable.
    scheduler._drain_history_outbox(wrapped, ORG, SUPER, limit=10)
    assert redis_client.get(cursor_key) == str(first_cursor)
    scheduler._drain_history_outbox(wrapped, ORG, SUPER, limit=10)
    assert redis_client.get(cursor_key) == str(second_cursor)

    release_delayed.set()
    delayed.join(timeout=2)
    assert not delayed.is_alive()
    assert redis_client.get(cursor_key) == str(second_cursor)


def test_zero_cooldown_cron_state_advances_before_execution_lease_release(
    monkeypatch,
):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        '{"version":1,"ts":1}',
    )
    redis_client.set(
        RK.meta_leaf(ORG, SUPER, TABLE),
        '{"version":1,"ts":1,"path":"snapshot.json"}',
    )

    class CronConfig(_MemoryDQConfig):
        def get_table_schedule(self, _table):
            return {
                "enabled": True,
                "timezone": "UTC",
                "post_ingest": False,
                "deep_enabled": False,
                "custom_enabled": False,
                "cooldown_seconds": 0,
            }

        def get_effective_config(self, _table):
            return {"checks": {}}

        def list_rules_for_table(self, _table):
            return []

    dqc = CronConfig()
    executions = []
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (
            executions.append(threading.current_thread().name)
            or scheduler._success("quick", evaluated=1, passed=1)
        ),
    )

    entered_record = threading.Event()
    release_record = threading.Event()
    original_record = scheduler._record_cron_outcome

    def blocked_record(*args, **kwargs):
        assert kwargs.get("lease_guard") is not None
        entered_record.set()
        assert release_record.wait(2)
        return original_record(*args, **kwargs)

    monkeypatch.setattr(scheduler, "_record_cron_outcome", blocked_record)
    second_has_stale_phase = threading.Event()
    release_second = threading.Event()
    original_try = scheduler._try_run_check

    def delay_second_after_due_read(*args, **kwargs):
        if threading.current_thread().name == "second-cron-worker":
            # _process_table_job has already read the old due state and built
            # the exact admission value passed in kwargs at this boundary.
            second_has_stale_phase.set()
            assert release_second.wait(2)
        return original_try(*args, **kwargs)

    monkeypatch.setattr(scheduler, "_try_run_check", delay_second_after_due_read)
    now = time.time()

    def process_table():
        scheduler._process_table_job(
            redis_client,
            ORG,
            SUPER,
            TABLE,
            dqc,
            {"timezone": "UTC", "post_ingest": False},
            0,
            "* * * * *",
            "0 0 1 1 *",
            "0 0 1 1 *",
            now,
            {},
            {},
            {},
            time.monotonic() + 5,
            None,
            False,
        )

    first = threading.Thread(target=process_table, name="first-cron-worker")
    first.start()
    second = None
    try:
        assert entered_record.wait(2)
        running_key = scheduler._running_key(ORG, SUPER, TABLE)
        assert redis_client.exists(running_key)

        # With a zero-second cooldown, the persisted cron phase is the only
        # post-completion dedupe.  Force a second scheduler to read the old due
        # phase, then let it attempt acquisition only after the first worker has
        # advanced that phase and released its execution lease.
        second = threading.Thread(target=process_table, name="second-cron-worker")
        second.start()
        assert second_has_stale_phase.wait(2)
        release_record.set()
        first.join(3)
        assert not first.is_alive()
        assert not redis_client.exists(running_key)
        release_second.set()
        second.join(3)
        assert not second.is_alive()
    finally:
        release_record.set()
        release_second.set()
        first.join(3)
        if second is not None:
            second.join(3)

    assert executions == ["first-cron-worker"]
    assert not redis_client.exists(scheduler._running_key(ORG, SUPER, TABLE))
    stored = json.loads(redis_client.get(
        scheduler._cron_state_key(ORG, SUPER, TABLE, "quick")
    ))
    assert stored["last_outcome"] == "success"
    assert stored["next_due_ms"] > int(now * 1000)


def test_cooldown_check_and_execution_lease_admission_are_atomic(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    first_entered_admission = threading.Event()
    release_first_admission = threading.Event()

    class PauseFirstAdmission:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if (
                threading.current_thread().name == "stale-admission"
                and "ARGV[8] == '0'" in script
                and not first_entered_admission.is_set()
            ):
                first_entered_admission.set()
                assert release_first_admission.wait(2)
            return redis_client.eval(script, *args)

    wrapped = PauseFirstAdmission()
    dqc = _MemoryDQConfig()
    executions = []
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (
            executions.append(threading.current_thread().name)
            or scheduler._success("quick", evaluated=1, passed=1)
        ),
    )
    first_result = []
    first = threading.Thread(
        name="stale-admission",
        target=lambda: first_result.append(scheduler._try_run_check(
            wrapped, ORG, SUPER, TABLE, "quick", dqc, 30,
        )),
    )
    first.start()
    assert first_entered_admission.wait(2)

    winner = scheduler._try_run_check(
        wrapped, ORG, SUPER, TABLE, "quick", dqc, 30,
    )
    assert winner.successful
    release_first_admission.set()
    first.join(2)

    assert not first.is_alive()
    assert first_result[0].state == "skipped"
    assert first_result[0].message == "success cooldown active"
    assert executions == [threading.current_thread().name]


def test_retry_check_and_execution_lease_admission_are_atomic(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    first_entered_admission = threading.Event()
    release_first_admission = threading.Event()

    class PauseFirstAdmission:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if (
                threading.current_thread().name == "stale-retry-admission"
                and "ARGV[8] == '0'" in script
                and not first_entered_admission.is_set()
            ):
                first_entered_admission.set()
                assert release_first_admission.wait(2)
            return redis_client.eval(script, *args)

    wrapped = PauseFirstAdmission()
    dqc = _MemoryDQConfig()
    executions = []
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (
            executions.append(threading.current_thread().name)
            or scheduler._failed("quick", "injected engine failure")
        ),
    )
    first_result = []
    first = threading.Thread(
        name="stale-retry-admission",
        target=lambda: first_result.append(scheduler._try_run_check(
            wrapped, ORG, SUPER, TABLE, "quick", dqc, 30,
        )),
    )
    first.start()
    assert first_entered_admission.wait(2)

    winner = scheduler._try_run_check(
        wrapped, ORG, SUPER, TABLE, "quick", dqc, 30,
    )
    assert winner.state == "failed"
    release_first_admission.set()
    first.join(2)

    assert not first.is_alive()
    assert first_result[0].state == "skipped"
    assert first_result[0].message == "failure retry backoff active"
    assert executions == [threading.current_thread().name]


def test_stale_pending_generation_cannot_enter_after_zero_cooldown_success(
    monkeypatch,
):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "generation-1")
    stale_entered_admission = threading.Event()
    release_stale_admission = threading.Event()

    class PauseStaleAdmission:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if (
                threading.current_thread().name == "stale-pending-admission"
                and "ARGV[9] == '1'" in script
                and not stale_entered_admission.is_set()
            ):
                stale_entered_admission.set()
                assert release_stale_admission.wait(2)
            return redis_client.eval(script, *args)

    wrapped = PauseStaleAdmission()
    dqc = _MemoryDQConfig()
    executions = []
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (
            executions.append(threading.current_thread().name)
            or scheduler._success("quick", evaluated=1, passed=1)
        ),
    )
    stale_result = []
    stale = threading.Thread(
        name="stale-pending-admission",
        target=lambda: stale_result.append(scheduler._try_run_check(
            wrapped,
            ORG,
            SUPER,
            TABLE,
            "quick",
            dqc,
            0,
            pending_generation="generation-1",
        )),
    )
    stale.start()
    assert stale_entered_admission.wait(2)

    winner = scheduler._try_run_check(
        wrapped,
        ORG,
        SUPER,
        TABLE,
        "quick",
        dqc,
        0,
        pending_generation="generation-1",
    )
    assert winner.successful
    assert not redis_client.exists(pending_key)
    release_stale_admission.set()
    stale.join(2)

    assert not stale.is_alive()
    assert stale_result[0].state == "skipped"
    assert stale_result[0].message == (
        "pending generation already consumed or changed"
    )
    assert executions == [threading.current_thread().name]


def test_deleted_table_cannot_admit_stale_enumerated_worker(monkeypatch):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    leaf_key = RK.meta_leaf(ORG, SUPER, TABLE)
    redis_client.set(leaf_key, '{"version":1,"path":"old.json"}')
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None

    redis_client.delete(leaf_key)
    redis_client.set(
        RK.meta_simple_deletion_intent(ORG, SUPER, TABLE),
        '{"intent_id":"delete-1"}',
    )
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("deleted table runner must not execute")
        ),
    )

    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        DQConfig(redis_client, ORG, SUPER),
        0,
        table_admission=admission,
    )

    assert outcome.state == "skipped"
    assert "incarnation" in outcome.message
    assert not redis_client.exists(scheduler._running_key(ORG, SUPER, TABLE))
    assert DQConfig(redis_client, ORG, SUPER).get_latest(TABLE) is None


def test_delete_recreate_aba_cannot_admit_old_table_incarnation(monkeypatch):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    leaf_key = RK.meta_leaf(ORG, SUPER, TABLE)
    redis_client.set(leaf_key, '{"version":7,"path":"old.json"}')
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None

    # The deletion intent has already been explicitly cleared and the same
    # logical name recreated; EXISTS-only fencing would admit this stale tick.
    redis_client.set(leaf_key, '{"version":0,"path":"new.json"}')
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("old incarnation runner must not execute")
        ),
    )

    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        DQConfig(redis_client, ORG, SUPER),
        0,
        table_admission=admission,
    )

    assert outcome.state == "skipped"
    assert "incarnation" in outcome.message
    assert DQConfig(redis_client, ORG, SUPER).get_latest(TABLE) is None


def test_leaf_mutation_during_run_rejects_stale_success_bundle(monkeypatch):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        json.dumps({"version": 0, "ts": 1}),
    )
    leaf_key = RK.meta_leaf(ORG, SUPER, TABLE)
    redis_client.set(leaf_key, '{"version":1,"ts":1,"path":"one.json"}')
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None
    pending_key = scheduler._pending_key(ORG, SUPER, TABLE, "quick")
    redis_client.set(pending_key, "generation-1")

    def mutate_then_complete(*_args, **_kwargs):
        redis_client.set(leaf_key, '{"version":2,"ts":2,"path":"two.json"}')
        return scheduler._success(
            "quick",
            evaluated=1,
            passed=1,
            publication=[
                (("latest", TABLE), _latest_quick()),
                (("anomalies", TABLE), []),
            ],
        )

    monkeypatch.setattr(scheduler, "_run_quick_check", mutate_then_complete)
    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        DQConfig(redis_client, ORG, SUPER),
        30,
        pending_generation="generation-1",
        table_admission=admission,
    )

    assert outcome.state == "failed"
    assert "success commit" in outcome.message
    assert redis_client.get(pending_key) == "generation-1"
    assert not redis_client.exists(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick")
    )
    assert not redis_client.exists(
        scheduler._retry_key(ORG, SUPER, TABLE, "quick")
    )
    assert DQConfig(redis_client, ORG, SUPER).get_latest(TABLE) is None


def test_runner_exception_after_leaf_mutation_cannot_overwrite_new_state(
    monkeypatch,
):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    redis_client.set(
        RK.meta_root(ORG, SUPER),
        json.dumps({"version": 0, "ts": 1}),
    )
    leaf_key = RK.meta_leaf(ORG, SUPER, TABLE)
    redis_client.set(leaf_key, '{"version":1,"ts":1,"path":"one.json"}')
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None
    config = DQConfig(redis_client, ORG, SUPER)
    preserved = {"checked_at": "preserve-me", "status": "ok"}
    assert config.set_latest(TABLE, preserved)

    def mutate_then_raise(*_args, **_kwargs):
        redis_client.set(leaf_key, '{"version":2,"ts":2,"path":"two.json"}')
        raise RuntimeError("runner failed after a newer snapshot committed")

    monkeypatch.setattr(scheduler, "_run_quick_check", mutate_then_raise)
    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        config,
        30,
        table_admission=admission,
    )

    assert outcome.state == "failed"
    assert config.get_latest(TABLE) == preserved
    assert not redis_client.exists(
        scheduler._retry_key(ORG, SUPER, TABLE, "quick")
    )
    assert not redis_client.exists(scheduler._running_key(ORG, SUPER, TABLE))


def test_cron_admission_atomically_honors_active_cooldown(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    now_ms = int(
        datetime(2026, 8, 18, 10, 15, tzinfo=ZoneInfo("UTC")).timestamp()
        * 1000
    )
    state = {
        "schema_version": 1,
        "expression": "* * * * *",
        "timezone": "UTC",
        "status": "scheduled",
        "next_due_ms": now_ms,
        "last_scheduled_ms": None,
        "last_started_ms": None,
        "last_completed_ms": None,
        "last_outcome": None,
    }
    redis_client.set(
        scheduler._cron_state_key(ORG, SUPER, TABLE, "quick"),
        json.dumps(state, separators=(",", ":")),
    )
    redis_client.set(
        scheduler._cooldown_key(ORG, SUPER, TABLE, "quick"),
        "winner",
        ex=30,
    )
    monkeypatch.setattr(
        scheduler,
        "_run_quick_check",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("cron runner must not execute during cooldown")
        ),
    )

    outcome = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        _MemoryDQConfig(),
        30,
        lease_admission=scheduler._cron_state_admission(
            ORG, SUPER, TABLE, "quick", state,
        ),
        cron_state=state,
    )

    assert outcome.state == "skipped"
    assert outcome.message == "success cooldown active"
    assert not redis_client.exists(scheduler._running_key(ORG, SUPER, TABLE))


def test_cron_configuration_reset_cas_does_not_overwrite_newer_schedule():
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    now_ms = int(
        datetime(2026, 8, 18, 10, 15, tzinfo=ZoneInfo("UTC")).timestamp()
        * 1000
    )
    scheduler._cron_schedule_state(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        "* * * * *",
        "UTC",
        now_ms,
    )
    stale_entered_cas = threading.Event()
    release_stale_cas = threading.Event()

    class PauseStaleReset:
        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if (
                threading.current_thread().name == "stale-cron-config"
                and args
                and args[0] == 1
                and "ARGV[2]" in script
                and not stale_entered_cas.is_set()
            ):
                stale_entered_cas.set()
                assert release_stale_cas.wait(2)
            return redis_client.eval(script, *args)

    wrapped = PauseStaleReset()
    stale_error = []

    def reset_stale_schedule():
        try:
            scheduler._cron_schedule_state(
                wrapped,
                ORG,
                SUPER,
                TABLE,
                "quick",
                "*/2 * * * *",
                "UTC",
                now_ms,
            )
        except Exception as exc:
            stale_error.append(exc)

    stale = threading.Thread(
        target=reset_stale_schedule,
        name="stale-cron-config",
    )
    stale.start()
    assert stale_entered_cas.wait(2)
    scheduler._cron_schedule_state(
        wrapped,
        ORG,
        SUPER,
        TABLE,
        "quick",
        "*/3 * * * *",
        "UTC",
        now_ms,
    )
    release_stale_cas.set()
    stale.join(2)

    assert not stale.is_alive()
    assert len(stale_error) == 1
    stored = json.loads(redis_client.get(
        scheduler._cron_state_key(ORG, SUPER, TABLE, "quick")
    ))
    assert stored["expression"] == "*/3 * * * *"


def test_atomic_cron_success_survives_ambiguous_commit_response(monkeypatch):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    now_ms = int(
        datetime(2026, 8, 18, 10, 15, tzinfo=ZoneInfo("UTC")).timestamp()
        * 1000
    )
    cron_key = scheduler._cron_state_key(ORG, SUPER, TABLE, "quick")
    state = {
        "schema_version": 1,
        "expression": "* * * * *",
        "timezone": "UTC",
        "status": "scheduled",
        "next_due_ms": now_ms,
        "last_scheduled_ms": None,
        "last_started_ms": None,
        "last_completed_ms": None,
        "last_outcome": None,
    }
    redis_client.set(cron_key, json.dumps(state, separators=(",", ":")))

    class RaiseAfterFirstSuccessCommit:
        def __init__(self):
            self.raised = False

        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            result = redis_client.eval(script, *args)
            if "atomic quality success document publication" in script and not self.raised:
                self.raised = True
                raise ConnectionError("injected lost success-commit response")
            return result

    wrapped = RaiseAfterFirstSuccessCommit()
    dqc = DQConfig(wrapped, ORG, SUPER)
    executions = []

    def completed_runner(*_args, **_kwargs):
        executions.append(True)
        latest = _latest_quick()
        return scheduler._success(
            "quick",
            evaluated=1,
            passed=1,
            publication=[
                (("latest", TABLE), latest),
                (("anomalies", TABLE), []),
            ],
        )

    monkeypatch.setattr(scheduler, "_run_quick_check", completed_runner)
    admission = scheduler._cron_state_admission(
        ORG, SUPER, TABLE, "quick", state,
    )
    ambiguous = scheduler._try_run_check(
        wrapped,
        ORG,
        SUPER,
        TABLE,
        "quick",
        dqc,
        0,
        lease_admission=admission,
        cron_state=state,
    )

    assert ambiguous.state == "failed"
    assert wrapped.raised
    latest = DQConfig(redis_client, ORG, SUPER).get_latest(TABLE)
    assert latest["mode_attempts"]["quick"]["state"] == "success"
    advanced = json.loads(redis_client.get(cron_key))
    assert advanced["last_outcome"] == "success"
    assert advanced["next_due_ms"] > now_ms

    replay = scheduler._try_run_check(
        redis_client,
        ORG,
        SUPER,
        TABLE,
        "quick",
        DQConfig(redis_client, ORG, SUPER),
        0,
        lease_admission=admission,
        cron_state=state,
    )
    assert replay.state == "skipped"
    assert executions == [True]


def test_history_preparation_is_atomic_with_exact_lease(monkeypatch):
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    running_key = scheduler._running_key(ORG, SUPER, TABLE)
    outbox_key = scheduler._history_outbox_key(ORG, SUPER)
    redis_client.set(running_key, "owner")

    class LeaseLossAtQueue:
        """Remove the lease immediately before Redis evaluates the script."""

        def __getattr__(self, name):
            return getattr(redis_client, name)

        def eval(self, script, *args):
            if "prepare an immutable" in script:
                redis_client.delete(running_key)
            return redis_client.eval(script, *args)

    guard = scheduler._LeaseGuard(LeaseLossAtQueue(), running_key, "owner")
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
    assert not redis_client.keys(
        scheduler._history_prepared_key(ORG, SUPER, TABLE, "*")
    )


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


def test_deadline_after_leaf_mutation_releases_without_retry_or_attempt(
    monkeypatch,
):
    from supertable.quality.config import DQConfig

    redis_client = fakeredis.FakeRedis(decode_responses=True)
    leaf_key = RK.meta_leaf(ORG, SUPER, TABLE)
    redis_client.set(leaf_key, '{"version":1,"path":"one.json"}')
    admission = scheduler._snapshot_table_lease_admission(
        redis_client, ORG, SUPER, TABLE,
    )
    assert admission is not None
    entered = threading.Event()
    release = threading.Event()
    result = []

    def blocked_runner(*_args, **_kwargs):
        entered.set()
        release.wait(2)
        return scheduler._success("quick", evaluated=1, passed=1)

    monkeypatch.setattr(scheduler, "_run_quick_check", blocked_runner)
    config = DQConfig(redis_client, ORG, SUPER)
    worker = threading.Thread(
        target=lambda: result.append(scheduler._try_run_check(
            redis_client,
            ORG,
            SUPER,
            TABLE,
            "quick",
            config,
            30,
            deadline_monotonic=time.monotonic() + 0.08,
            table_admission=admission,
        )),
    )
    worker.start()
    assert entered.wait(timeout=1)
    redis_client.set(leaf_key, '{"version":2,"path":"two.json"}')
    assert _wait_for(
        lambda: not redis_client.exists(
            scheduler._running_key(ORG, SUPER, TABLE)
        ),
    )
    release.set()
    worker.join(timeout=2)

    assert not worker.is_alive()
    assert result and result[0].state == "failed"
    assert not redis_client.exists(
        scheduler._retry_key(ORG, SUPER, TABLE, "quick")
    )
    assert config.get_latest(TABLE) is None


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
