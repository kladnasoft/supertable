"""``run_check`` says what happened: success, failed or skipped.

It used to return a bool that was False both for a skipped check (cooldown
active, or another check running) and for a failed one, so a caller had to
guess. And a caller asking for a run without a cooldown, as dataisland-core's
manual "Run checks now" does, could not have one: the check ran, then
``SET ... EX 0`` raised "invalid expire time", and the run was reported failed.

The checks themselves are stubbed; the lock, the cooldown and the outcome are
the real ones, on fakeredis. tests/characterization/test_quality_run_check_real.py
runs the real checks.
"""
from __future__ import annotations

import json
import os

import pytest
from fakeredis import FakeRedis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_keys as RK  # noqa: E402
from supertable.quality import scheduler as sched  # noqa: E402
from supertable.quality.config import DQConfig  # noqa: E402

ORG = "test_org"
SUP = "test_super"
TABLE = "orders"


@pytest.fixture()
def r():
    return FakeRedis(decode_responses=True)


@pytest.fixture()
def runs(monkeypatch):
    """Stub the three checks, recording each run as its mode."""
    ran = []
    for mode in ("quick", "deep", "custom"):
        monkeypatch.setattr(
            sched, f"_run_{mode}_check",
            lambda r, org, sup, table, dqc, mode=mode: ran.append(mode))
    return ran


def _run(r, mode="quick", cooldown=300):
    return sched.run_check(r, ORG, SUP, TABLE, mode, DQConfig(r, ORG, SUP), cooldown)


def _cooldown(r):
    return sched._cooldown_key(ORG, SUP, TABLE)


def _running(r):
    return sched._running_key(ORG, SUP, TABLE)


def test_a_check_that_runs_succeeds_and_sets_its_cooldown(r, runs):
    outcome = _run(r, cooldown=300)

    assert (outcome.mode, outcome.state) == ("quick", "success")
    assert outcome and outcome.successful
    assert runs == ["quick"]
    assert 0 < r.ttl(_cooldown(r)) <= 300
    assert not r.exists(_running(r)), "the running lock must be released"


def test_a_zero_cooldown_runs_and_leaves_none(r, runs):
    outcome = _run(r, cooldown=0)

    assert outcome.state == "success"
    assert runs == ["quick"]
    assert not r.exists(_cooldown(r))
    assert not r.exists(_running(r))
    assert _run(r, cooldown=0).state == "success", "nothing holds the next run back"


def test_a_check_that_raises_fails_without_its_text(r, monkeypatch):
    def broken(*args):
        raise RuntimeError("secret column ssn does not exist")

    monkeypatch.setattr(sched, "_run_quick_check", broken)
    outcome = _run(r)

    assert outcome.state == "failed"
    assert not outcome and not outcome.successful
    assert "ssn" not in outcome.message
    assert not r.exists(_cooldown(r))
    assert not r.exists(_running(r))


def test_an_active_cooldown_skips_without_running(r, runs):
    r.set(_cooldown(r), "2026-11-06T00:00:00+00:00", ex=300)
    outcome = _run(r, cooldown=0)

    assert outcome.state == "skipped" and not outcome
    assert runs == []


def test_a_check_already_running_skips_without_running(r, runs):
    r.set(_running(r), "another-worker", ex=300)
    outcome = _run(r)

    assert outcome.state == "skipped" and not outcome
    assert runs == []
    assert r.get(_running(r)) == "another-worker", "the holder's lock is untouched"


@pytest.mark.parametrize("cooldown", [-1, True, 1.5, "300", None])
def test_an_invalid_cooldown_fails_before_anything_runs(r, runs, cooldown):
    outcome = _run(r, cooldown=cooldown)

    assert outcome.state == "failed" and not outcome
    assert runs == []
    assert r.keys("*") == []


@pytest.mark.parametrize("mode", ["quick", "deep", "custom"])
def test_each_mode_runs_its_own_check(r, runs, mode):
    assert _run(r, mode=mode).mode == mode
    assert runs == [mode]


@pytest.mark.parametrize("state", ["success", "failed", "skipped"])
def test_only_a_success_is_truthy(state):
    assert bool(sched.CheckRunOutcome("quick", state)) is (state == "success")


def test_the_package_exports_the_entry_point():
    import supertable.quality as quality

    assert quality.run_check is sched.run_check
    assert quality.CheckRunOutcome is sched.CheckRunOutcome


# --------------------------------------------------------------------------
# The scheduler's own tick: a cron run that ran is recorded, success or
# failure, and only a success clears a pending flag
# --------------------------------------------------------------------------

@pytest.fixture()
def lake(r, monkeypatch):
    """One enabled lake with one table, as the tick discovers them."""
    monkeypatch.setattr("supertable.redis_connector.create_redis_client", lambda: r)
    r.set(RK.meta_root(ORG, SUP), json.dumps({"version": 1, "ts": 0}))
    r.set(RK.meta_leaf(ORG, SUP, TABLE), json.dumps({"version": 0, "path": "p"}))

    def schedule(cooldown):
        DQConfig(r, ORG, SUP).set_schedule({
            "enabled": True, "post_ingest": True, "cooldown_seconds": cooldown,
            "quick_cron": "0 */4 * * *", "deep_cron": "0 2 * * *"})
    return schedule


def _tick(state):
    sched._scheduler_tick(*state)


def test_a_tick_records_a_run_and_its_cooldown_holds_the_pending_check(r, runs, lake):
    """The bookkeeping a positive cooldown gives, unchanged by the outcome."""
    lake(300)
    r.set(sched._pending_key(ORG, SUP, TABLE), "now")
    state = ({}, {}, {})

    _tick(state)
    assert runs == ["quick"]
    assert f"{ORG}:{SUP}:{TABLE}" in state[0], "the cron run is recorded"
    assert r.exists(sched._pending_key(ORG, SUP, TABLE)), "held back by the cooldown"

    r.delete(_cooldown(r))
    _tick(state)
    assert runs == ["quick", "quick"], "only the pending check was due"
    assert not r.exists(sched._pending_key(ORG, SUP, TABLE))


def test_a_tick_with_a_zero_cooldown_records_its_runs(r, runs, lake):
    """Each run used to fail on EX 0, so none was recorded: every tick ran the
    cron check again, and the pending flag was never cleared."""
    lake(0)
    r.set(sched._pending_key(ORG, SUP, TABLE), "now")
    state = ({}, {}, {})

    for _ in range(3):
        _tick(state)

    assert runs == ["quick", "quick"], "the cron run, then the pending one"
    assert f"{ORG}:{SUP}:{TABLE}" in state[0]
    assert not r.exists(sched._pending_key(ORG, SUP, TABLE))
    assert not r.exists(_cooldown(r))


@pytest.fixture()
def failing_quick(runs, monkeypatch):
    """A quick check that fails every time, as a broken table's does."""
    def broken(*args):
        runs.append("quick")
        raise RuntimeError("statement failed")

    monkeypatch.setattr(sched, "_run_quick_check", broken)
    return runs


def test_a_failed_cron_run_waits_for_its_next_interval(r, failing_quick, lake):
    """Unrecorded, a check that fails every time ran on every tick."""
    lake(300)
    state = ({}, {}, {})

    for _ in range(3):
        _tick(state)

    assert failing_quick == ["quick"]
    assert f"{ORG}:{SUP}:{TABLE}" in state[0]
    assert not r.exists(_cooldown(r)), "a failure sets no cooldown"


def test_a_failed_pending_run_keeps_its_flag_for_the_next_tick(r, failing_quick, lake):
    lake(300)
    r.set(sched._pending_key(ORG, SUP, TABLE), "now")
    state = ({}, {}, {})

    _tick(state)
    assert failing_quick == ["quick", "quick"], "the cron run, then the pending one"
    _tick(state)
    assert failing_quick == ["quick", "quick", "quick"], "the pending one again, alone"
    assert r.exists(sched._pending_key(ORG, SUP, TABLE))
