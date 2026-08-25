from __future__ import annotations

from typing import Any

from supertable.audit import export


def test_dora_export_uses_reader_safe_limit(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_query(_organization: str, **kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr("supertable.audit.reader.query_audit_log", fake_query)
    export.export_dora_incident_report("acme", "inc-1", 1, 2)
    assert calls == [{"start_ms": 1, "end_ms": 2, "limit": 10_000}]


def test_soc2_export_uses_reader_safe_limit(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def fake_query(_organization: str, **kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr("supertable.audit.reader.query_audit_log", fake_query)
    export.export_soc2_evidence("acme", "CC6.1", 1, 2)
    assert calls == [{
        "start_ms": 1,
        "end_ms": 2,
        "category": "authentication",
        "limit": 10_000,
    }]


def test_dora_export_filters_to_incident_id(monkeypatch) -> None:
    monkeypatch.setattr(
        "supertable.audit.reader.query_audit_log",
        lambda *_args, **_kwargs: [
            {"event_id": "a", "incident_id": "inc-1"},
            {"event_id": "b", "incident_id": "inc-2"},
        ],
    )
    payload = export.export_dora_incident_report("acme", "inc-1", 1, 2)
    assert b'"event_id":"a"' in payload
    assert b'"event_id":"b"' not in payload


def test_soc2_export_rejects_unknown_criteria() -> None:
    import pytest

    with pytest.raises(ValueError, match="unsupported SOC 2 criterion"):
        export.export_soc2_evidence("acme", "UNKNOWN", 1, 2)
