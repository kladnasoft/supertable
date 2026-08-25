# route: supertable.audit.tests.test_query_integration
"""End-to-end query execution to protected audit-event regressions."""
from __future__ import annotations

import hashlib
import importlib
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

import supertable.audit as audit_pkg
import supertable.data_reader as data_reader
from supertable.audit import crypto
from supertable.audit.events import Actions, AuditEvent, Outcome
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.engine.engine_enum import Engine


class _CollectingAuditLogger:
    def __init__(self) -> None:
        self.events: list[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        self.events.append(event)


class _OneBatchStream:
    def __init__(self) -> None:
        self.schema = pa.schema([("id", pa.int64())])
        self.closed = False
        self._batch = pa.record_batch({"id": [1, 2]})

    def __iter__(self):
        return self

    def __next__(self):
        if self.closed:
            raise StopIteration
        self.closed = True
        return self._batch

    def close(self) -> None:
        self.closed = True


def _install_successful_execution(
    monkeypatch: pytest.MonkeyPatch,
    *,
    streaming: bool,
) -> _CollectingAuditLogger:
    parser = MagicMock()
    parser.original_query = "SELECT secret FROM events LIMIT 10000"
    parser.get_table_tuples.return_value = []
    parser.get_physical_tables.return_value = []
    monkeypatch.setattr(data_reader, "SQLParser", MagicMock(return_value=parser))
    monkeypatch.setattr(data_reader, "get_storage", lambda: MagicMock())
    monkeypatch.setattr(data_reader, "restrict_read_access", lambda **_kwargs: {})
    monkeypatch.setattr(
        data_reader, "validate_rbac_binding_stability", lambda *_args: None,
    )

    observation_module = importlib.import_module(
        "supertable.engine.query_observations"
    )
    monkeypatch.setattr(
        observation_module,
        "QueryObservationStore",
        lambda _org: SimpleNamespace(enabled=False),
    )
    query_manager = SimpleNamespace(
        query_id="query-123",
        query_hash="hash-456",
        source_type="",
        organization="acme",
        super_name="shop",
    )
    monkeypatch.setattr(
        data_reader, "QueryPlanManager", lambda **_kwargs: query_manager,
    )
    monkeypatch.setattr(data_reader, "PlanStats", MagicMock)
    monkeypatch.setattr(
        data_reader,
        "Timer",
        lambda: MagicMock(timings=[]),
    )
    reflection = Reflection(
        "local",
        1,
        1,
        [SuperSnapshot("shop", "events", 1, ["f.parquet"], {"id"})],
    )
    estimator = MagicMock()
    estimator.estimate.return_value = reflection
    monkeypatch.setattr(
        data_reader, "DataEstimator", MagicMock(return_value=estimator),
    )
    executor = MagicMock()
    if streaming:
        executor.execute_stream.return_value = (_OneBatchStream(), "duckdb")
    else:
        executor.execute.return_value = (
            pd.DataFrame({"id": [1, 2]}),
            "duckdb",
        )
    monkeypatch.setattr(
        data_reader, "Executor", MagicMock(return_value=executor),
    )
    monkeypatch.setattr(data_reader, "extend_execution_plan", MagicMock())

    import supertable.config.settings as settings_module

    monkeypatch.setattr(
        settings_module,
        "settings",
        SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=""),
        raising=True,
    )
    monkeypatch.setattr(crypto, "_fernet_instance", None)
    monkeypatch.setattr(crypto, "_fernet_loaded", False)
    collector = _CollectingAuditLogger()
    monkeypatch.setattr(
        audit_pkg,
        "get_audit_logger",
        lambda _organization, *, action=None: collector,
        raising=True,
    )
    return collector


@pytest.mark.parametrize("streaming", [False, True])
def test_successful_query_execution_emits_one_protected_event(
    monkeypatch: pytest.MonkeyPatch,
    streaming: bool,
) -> None:
    collector = _install_successful_execution(
        monkeypatch, streaming=streaming,
    )
    plaintext = "SELECT secret FROM events"
    reader = data_reader.DataReader(
        "shop", "acme", plaintext, source="api",
    )

    if streaming:
        stream, status, message = reader.execute_stream(
            "reporting-role", engine=Engine.DUCKDB,
        )
        assert [batch.num_rows for batch in stream] == [2]
    else:
        frame, status, message = reader.execute(
            "reporting-role", engine=Engine.DUCKDB,
        )
        assert frame["id"].tolist() == [1, 2]

    assert status is data_reader.Status.OK
    assert message is None
    assert len(collector.events) == 1
    event = collector.events[0]
    assert event.action == Actions.QUERY_EXECUTE
    assert event.outcome == Outcome.SUCCESS
    assert event.organization == "acme"
    assert event.super_name == "shop"
    assert event.resource_type == "query"
    assert event.resource_id == "query-123"
    assert plaintext not in event.detail
    detail = json.loads(event.detail)
    assert detail == {
        "sql_sha256": hashlib.sha256(
            plaintext.encode("utf-8")
        ).hexdigest(),
        "sql_redacted": True,
        "query_id": "query-123",
        "query_hash": "hash-456",
        "authorization_role": "reporting-role",
        "source": "api",
        "engine": "duckdb",
            "row_count": 2,
            "column_count": 1,
            "outcome": "success",
        }


def test_abandoned_query_stream_does_not_emit_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = _install_successful_execution(monkeypatch, streaming=True)
    reader = data_reader.DataReader(
        "shop", "acme", "SELECT secret FROM events", source="api",
    )

    stream, status, _message = reader.execute_stream(
        "reporting-role", engine=Engine.DUCKDB,
    )
    assert status is data_reader.Status.OK
    stream.close()

    assert collector.events == []


def test_disabled_query_lane_skips_protection_on_data_reader_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    collector = _install_successful_execution(monkeypatch, streaming=False)
    logger_module = importlib.import_module("supertable.audit.logger")
    monkeypatch.setattr(
        logger_module,
        "_resolve_config_for",
        lambda _organization: logger_module.AuditConfig(
            enabled=True,
            log_queries=False,
            fernet_key="deliberately-invalid-key",
        ),
    )
    monkeypatch.setattr(logger_module, "_LOGGERS", {})
    monkeypatch.setattr(
        audit_pkg,
        "get_audit_logger",
        logger_module.get_audit_logger,
        raising=True,
    )
    protection_calls = 0

    def fail_if_protection_runs(_detail, *, action):
        nonlocal protection_calls
        protection_calls += 1
        raise AssertionError(f"suppressed action was protected: {action}")

    monkeypatch.setattr(
        audit_pkg, "protect_sensitive_detail", fail_if_protection_runs,
    )
    reader = data_reader.DataReader(
        "shop", "acme", "SELECT secret FROM events", source="api",
    )

    frame, status, message = reader.execute(
        "reporting-role", engine=Engine.DUCKDB,
    )

    assert frame["id"].tolist() == [1, 2]
    assert status is data_reader.Status.OK
    assert message is None
    assert protection_calls == 0
    assert collector.events == []
    assert isinstance(
        logger_module._LOGGERS["acme"], logger_module.NullAuditLogger,
    )
