"""``stop_scheduler`` ends the Data Quality daemon thread within a bound.

The thread used to loop on ``while True`` and ``time.sleep``, with no way to
end it, and ``supertable.quality`` exported no stop: a host shutting down
(dataisland-core's API lifespan calls ``supertable.quality.stop_scheduler``)
failed its import and left the thread running. A check already running is
still not interrupted, so a stop can outlast its deadline; the thread then
exits once that check is done.
"""
from __future__ import annotations

import json
import math
import os
import threading
import time

import pytest
from fakeredis import FakeRedis

os.environ.setdefault("SUPERTABLE_ORGANIZATION", "test_org")
os.environ.setdefault("SUPERTABLE_SUPERUSER_TOKEN", "test_token")

from supertable import redis_keys as RK  # noqa: E402
from supertable.quality import scheduler as sched  # noqa: E402
from supertable.quality.config import DQConfig  # noqa: E402


@pytest.fixture(autouse=True)
def _no_scheduler_left(monkeypatch):
    """Each test starts with no scheduler thread and leaves none behind."""
    monkeypatch.setattr(sched, "_scheduler_thread", None)
    yield
    assert sched.stop_scheduler(timeout_s=15) is True
    assert not sched._scheduler_stop.is_set()


def _thread():
    return sched._scheduler_thread


def test_the_package_exports_stop_scheduler():
    import supertable.quality as quality

    assert quality.stop_scheduler is sched.stop_scheduler


def test_stopping_a_scheduler_never_started_is_a_true_no_op():
    assert sched.stop_scheduler() is True
    assert not sched._scheduler_stop.is_set(), "a later tick is not cut short"


@pytest.mark.parametrize("timeout", [-1, math.nan, math.inf, True, "10", None])
def test_an_invalid_timeout_raises_and_stops_nothing(timeout):
    assert sched.start_scheduler() is True
    with pytest.raises(ValueError):
        sched.stop_scheduler(timeout_s=timeout)
    assert not sched._scheduler_stop.is_set()
    assert _thread().is_alive()


def test_a_stop_during_the_startup_wait_returns_at_once():
    assert sched.start_scheduler() is True
    thread = _thread()

    started = time.monotonic()
    assert sched.stop_scheduler(timeout_s=5) is True
    assert time.monotonic() - started < 2, "the startup wait is not slept through"
    assert not thread.is_alive()
    assert _thread() is None


def test_a_stopped_scheduler_starts_again():
    assert sched.start_scheduler() is True
    first = _thread()
    assert sched.stop_scheduler(timeout_s=5) is True

    assert sched.start_scheduler() is True
    assert _thread() is not first and _thread().is_alive()
    assert sched.stop_scheduler(timeout_s=5) is True


def test_a_stop_during_a_long_check_outlasts_its_deadline_then_ends(monkeypatch):
    monkeypatch.setattr(sched, "STARTUP_DELAY_SECONDS", 0)
    in_tick, release, ticks = threading.Event(), threading.Event(), []

    def long_tick(*state):
        ticks.append(state)
        in_tick.set()
        release.wait(10)

    monkeypatch.setattr(sched, "_scheduler_tick", long_tick)
    assert sched.start_scheduler() is True
    thread = _thread()
    assert in_tick.wait(5)

    assert sched.stop_scheduler(timeout_s=0.2) is False
    assert sched.start_scheduler() is False, "the old thread is still finishing"

    release.set()
    thread.join(5)
    assert not thread.is_alive()
    assert len(ticks) == 1, "no tick after the stop"
    assert sched.stop_scheduler(timeout_s=1) is True
    assert _thread() is None


def test_a_tick_stops_at_the_next_table(monkeypatch):
    """The stop lands while the first table is checked; that check finishes,
    and no other table or lake is checked."""
    r = FakeRedis(decode_responses=True)
    monkeypatch.setattr("supertable.redis_connector.create_redis_client", lambda: r)
    for sup in ("lake_a", "lake_b"):
        r.set(RK.meta_root("org", sup), json.dumps({"version": 1, "ts": 0}))
        DQConfig(r, "org", sup).set_schedule({"enabled": True, "post_ingest": False})
        for table in ("t1", "t2"):
            r.set(RK.meta_leaf("org", sup, table), json.dumps({"version": 0, "path": "p"}))

    checked = []

    def check(r, org, sup, table_name, mode, dqc, cooldown_sec):
        checked.append((sup, table_name))
        sched._scheduler_stop.set()  # what stop_scheduler does mid-check
        return sched.CheckRunOutcome(mode, "success")

    monkeypatch.setattr(sched, "run_check", check)
    sched._scheduler_tick({}, {}, {})

    assert len(checked) == 1
