from __future__ import annotations

from types import SimpleNamespace

import fakeredis
import pytest

from supertable import monitoring_writer as mw


class _DownRedis:
    def eval(self, *_args, **_kwargs):
        raise ConnectionError("redis down")

    def pipeline(self):
        return self

    def rpush(self, *_args, **_kwargs):
        raise ConnectionError("redis down")

    def expireat(self, *_args, **_kwargs):
        raise ConnectionError("redis down")

    def execute(self):
        raise ConnectionError("redis down")


def _stop_all():
    with mw._MONITORS_LOCK:
        monitors = list(mw._MONITORS.values())
        mw._MONITORS.clear()
    for monitor in monitors:
        stop = getattr(monitor, "_stop", None)
        if stop is not None:
            stop.set()
        thread = getattr(monitor, "_thread", None)
        if thread is not None:
            thread.join(timeout=1)


@pytest.fixture(autouse=True)
def _clean_monitor_cache(monkeypatch, tmp_path):
    monkeypatch.setattr(mw, "_monitor_spool_dir", lambda: str(tmp_path / "spool"))
    _stop_all()
    yield
    _stop_all()


def test_return_from_log_metric_means_redis_accepted_record(monkeypatch):
    monkeypatch.setattr(mw, "_monitoring_enabled", lambda: True)
    redis_client = fakeredis.FakeRedis(decode_responses=True)
    monitor = mw.get_monitoring_logger(
        organization="acme",
        monitor_type="writes",
        redis_connector=SimpleNamespace(r=redis_client),
    )

    monitor.log_metric({"query_id": "q-1"})
    key = mw._MonitorKey("acme", "writes").redis_list_key_today()
    assert redis_client.llen(key) == 1
    assert monitor.queue_stats["total_processed"] == 1


def test_redis_outage_is_durably_spooled_and_flush_is_explicit(monkeypatch):
    monkeypatch.setattr(mw, "_monitoring_enabled", lambda: True)
    monitor = mw.get_monitoring_logger(
        organization="acme",
        monitor_type="writes",
        redis_connector=SimpleNamespace(r=_DownRedis()),
    )
    assert not isinstance(monitor, mw.NullMonitoringLogger)

    # Redis outage is safe: return means the bounded local WAL fsync completed.
    monitor.log_metric({"query_id": "q-1"})
    assert [item["query_id"] for item in monitor._retry_batch] == ["q-1"]
    assert monitor.queue_stats["total_processed"] == 0
    assert monitor.queue_stats["total_dropped"] == 0
    with pytest.raises(mw.MonitoringDeliveryError, match="remain unaccepted"):
        monitor.request_flush()


def test_cache_pressure_can_evict_logger_with_durable_wal(monkeypatch):
    monkeypatch.setattr(mw, "_monitoring_enabled", lambda: True)
    monkeypatch.setattr(mw, "_MONITORS_MAX", 1)
    first = mw.get_monitoring_logger(
        organization="acme",
        monitor_type="writes",
        redis_connector=SimpleNamespace(r=_DownRedis()),
    )
    first.log_metric({"query_id": "q-1"})

    second = mw.get_monitoring_logger(
        organization="beta",
        monitor_type="writes",
        redis_connector=SimpleNamespace(
            r=fakeredis.FakeRedis(decode_responses=True),
        ),
    )
    assert mw._MONITORS["beta/writes"] is second
    assert "acme/writes" not in mw._MONITORS
    assert first._retry_batch


def test_enabled_constructor_failure_does_not_return_null(monkeypatch):
    monkeypatch.setattr(mw, "_monitoring_enabled", lambda: True)

    class BrokenConnector:
        def __init__(self):
            raise ConnectionError("cannot connect")

    monkeypatch.setattr(mw, "RedisConnector", BrokenConnector)
    monitor = mw.get_monitoring_logger(
        organization="acme", monitor_type="writes",
    )
    assert not isinstance(monitor, mw.NullMonitoringLogger)
    monitor.log_metric({"query_id": "q-1"})
    assert [item["query_id"] for item in monitor._retry_batch] == ["q-1"]
