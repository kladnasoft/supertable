from __future__ import annotations

import json
import os
import time
from types import SimpleNamespace

import fakeredis
import pyarrow as pa
import pytest

from supertable import monitoring_writer as mw


class _DownRedis:
    def eval(self, *_args, **_kwargs):
        raise ConnectionError("redis unavailable")


def _logger(path, redis, *, max_records=100, max_bytes=1_000_000):
    return mw._AsyncMonitoringLogger(
        mw._MonitorKey("acme", "writes"),
        redis_connector=SimpleNamespace(r=redis),
        spool_dir=str(path),
        spool_max_records=max_records,
        spool_max_bytes=max_bytes,
        start_worker=False,
    )


def test_outage_backlog_survives_restart_and_drains_without_new_metric(tmp_path):
    spool = tmp_path / "monitor-wal"
    first = _logger(spool, _DownRedis())
    first.log_metric({"query_id": "q-restart"})
    assert [item["query_id"] for item in first._retry_batch] == ["q-restart"]

    redis = fakeredis.FakeRedis(decode_responses=True)
    delivered = mw.drain_monitoring_spool(
        redis_connector=SimpleNamespace(r=redis),
        spool_dir=str(spool),
    )
    assert delivered == 1
    key = mw._MonitorKey("acme", "writes").redis_list_key_today()
    assert redis.llen(key) == 1
    assert json.loads(redis.lindex(key, 0))["query_id"] == "q-restart"
    assert first._spool.count() == 0


def test_lost_redis_reply_retries_idempotently(tmp_path):
    redis = fakeredis.FakeRedis(decode_responses=True)

    class LostFirstReply:
        def __init__(self):
            self.first = True

        def eval(self, *args, **kwargs):
            result = redis.eval(*args, **kwargs)
            if self.first:
                self.first = False
                raise ConnectionError("reply lost after Redis committed")
            return result

    monitor = _logger(tmp_path / "wal", LostFirstReply())
    monitor.log_metric({"query_id": "q-once"})
    assert monitor._spool.count() == 1

    monitor.request_flush()
    key = mw._MonitorKey("acme", "writes").redis_list_key_today()
    assert redis.llen(key) == 1
    assert monitor._spool.count() == 0


def test_record_cap_applies_explicit_backpressure_without_memory_fallback(tmp_path):
    monitor = _logger(
        tmp_path / "wal", _DownRedis(), max_records=1, max_bytes=1_000_000,
    )
    monitor.log_metric({"query_id": "q-1"})

    with pytest.raises(mw.MonitoringBackpressureError, match="record cap"):
        monitor.log_metric({"query_id": "q-2"})

    assert [item["query_id"] for item in monitor._retry_batch] == ["q-1"]
    assert monitor.queue_stats["total_dropped"] == 0


def test_relative_spool_path_is_rejected_instead_of_binding_to_cwd():
    with pytest.raises(mw.MonitoringDurabilityError, match="must be absolute"):
        mw._DurableMonitoringSpool(
            "relative-monitor-wal", max_records=10, max_bytes=10_000,
        )


def test_new_spool_directory_fsyncs_every_new_parent_entry(tmp_path, monkeypatch):
    root = tmp_path / "new-parent" / "monitor-wal"
    fsynced = []
    monkeypatch.setattr(
        mw._DurableMonitoringSpool,
        "_fsync_directory",
        staticmethod(lambda path: fsynced.append(path)),
    )

    mw._DurableMonitoringSpool(
        str(root), max_records=10, max_bytes=10_000,
    )

    assert fsynced[:3] == [str(root), str(root.parent), str(tmp_path)]


def test_preexisting_spool_directory_still_fsyncs_parent(tmp_path, monkeypatch):
    root = tmp_path / "monitor-wal"
    root.mkdir()
    fsynced = []
    monkeypatch.setattr(
        mw._DurableMonitoringSpool,
        "_fsync_directory",
        staticmethod(lambda path: fsynced.append(path)),
    )

    mw._DurableMonitoringSpool(
        str(root), max_records=10, max_bytes=10_000,
    )

    assert fsynced[:2] == [str(root), str(tmp_path)]


def test_valid_fsynced_temp_is_recovered_but_partial_temp_is_discarded(tmp_path):
    monitor = _logger(tmp_path / "wal", _DownRedis())
    monitor.log_metric({"query_id": "q-valid"})
    record = monitor._spool.pending()[0]
    final_name = os.path.basename(record.path)
    temp_path = monitor._spool._safe_path(f".{final_name}.tmp")
    os.replace(record.path, temp_path)
    partial = monitor._spool._safe_path(
        ".000000000000000000000000-00000000000000000000-"
        "00000000000000000000000000000000.monitor.json.tmp"
    )
    with open(partial, "wb"):
        pass
    invalid = monitor._spool._safe_path(
        ".111111111111111111111111-00000000000000000001-"
        "11111111111111111111111111111111.monitor.json.tmp"
    )
    with open(invalid, "wb") as handle:
        handle.write(b'{"version":1,"truncated":')

    restarted = _logger(tmp_path / "wal", _DownRedis())
    assert [item["query_id"] for item in restarted._retry_batch] == ["q-valid"]
    assert not os.path.exists(partial)
    assert not os.path.exists(invalid)


def test_redis_time_refuses_delivery_at_partition_expiry_and_retains_wal(
    tmp_path, monkeypatch,
):
    redis = fakeredis.FakeRedis(decode_responses=True)
    monitor = _logger(tmp_path / "wal", _DownRedis())
    monitor.log_metric({"query_id": "q-expired"})
    record = monitor._spool.pending()[0]

    # Keep Python's preflight just before expiry while Redis TIME is at expiry;
    # the authoritative Lua check must refuse before HSET/RPUSH.
    monkeypatch.setattr(mw.time, "time", lambda: record.envelope["partition_expires_at"] - 0.1)
    lua = mw._IDEMPOTENT_ENQUEUE_LUA.replace(
        "local now = redis.call('TIME')",
        f"local now = {{{record.envelope['partition_expires_at']}, 0}}",
    )
    monkeypatch.setattr(mw, "_IDEMPOTENT_ENQUEUE_LUA", lua)
    monitor._redis = SimpleNamespace(r=redis)

    with pytest.raises(mw.MonitoringExpiredRecordError, match="Redis refused"):
        monitor.request_flush()
    assert monitor._spool.count() == 1
    assert redis.llen(record.envelope["partition_key"]) == 0


def test_post_commit_error_is_explicit_and_preserves_core_result():
    cause = mw.MonitoringBackpressureError("spool full")
    error = mw.MonitoringPostCommitError(
        organization="acme",
        super_name="sales",
        table_name="orders",
        operation="write",
        core_result=(3, 10, 2, 0),
        cause=cause,
    )
    assert error.core_committed is True
    assert error.core_result == (3, 10, 2, 0)
    assert error.cause is cause


def test_noncanonical_payload_is_a_durability_error_not_generic_type_error(
    tmp_path,
):
    monitor = _logger(tmp_path / "wal", _DownRedis())
    with pytest.raises(mw.MonitoringDurabilityError, match="canonical JSON"):
        monitor.log_metric({"not_json": object()})
    assert monitor._spool.count() == 0


def test_stream_monitoring_finalizes_on_exhaustion_with_measured_rows_bytes():
    from supertable.data_reader import _MonitoredResultStream, Status

    batches = [
        pa.record_batch({"id": [1, 2]}),
        pa.record_batch({"id": [3]}),
    ]

    class Inner:
        schema = batches[0].schema

        def __init__(self):
            self.iterator = iter(batches)
            self.closed = False

        def __iter__(self):
            return self

        def __next__(self):
            try:
                return next(self.iterator)
            except StopIteration:
                self.closed = True
                raise

        def close(self):
            self.closed = True

    outcomes = []
    stream = _MonitoredResultStream(
        Inner(), lambda status, message, rows, size: outcomes.append(
            (status, message, rows, size)
        ),
    )
    observed = list(stream)
    stream.close()

    assert sum(batch.num_rows for batch in observed) == 3
    assert outcomes == [(
        Status.OK.value,
        None,
        3,
        sum(batch.nbytes for batch in batches),
    )]


def test_stream_monitoring_backpressure_closes_and_surfaces_completed_outcome():
    from supertable.data_reader import _MonitoredResultStream

    batch = pa.record_batch({"id": [1]})

    class Inner:
        schema = batch.schema

        def __init__(self):
            self.iterator = iter([batch])
            self.closed = False
            self.cancelled = False

        def __next__(self):
            return next(self.iterator)

        def cancel(self):
            self.cancelled = True
            self.closed = True

        def close(self):
            self.closed = True

    cause = mw.MonitoringBackpressureError("spool full")

    def fail_monitor(status, _message, _rows, _size):
        raise mw.MonitoringPostExecutionError(
            organization="acme",
            super_name="sales",
            query_id="q-stream",
            status=status,
            cause=cause,
        )

    inner = Inner()
    stream = _MonitoredResultStream(inner, fail_monitor)
    assert next(stream).num_rows == 1
    with pytest.raises(mw.MonitoringPostExecutionError) as raised:
        next(stream)
    assert raised.value.execution_completed is True
    assert inner.closed is True
    # A caller abort instead cancels the engine before surfacing monitoring.
    second_inner = Inner()
    second = _MonitoredResultStream(second_inner, fail_monitor)
    with pytest.raises(mw.MonitoringPostExecutionError):
        second.cancel()
    assert second_inner.cancelled is True
    assert second_inner.closed is True
