# route: supertable.audit.tests.test_emit
"""Tests for the public ``supertable.audit.emit`` and ``audit_context``.

The real ``AuditLogger`` spawns a worker thread and talks to Redis; for these
tests we replace ``get_audit_logger`` with a fake that just records the
``AuditEvent`` instances it receives. That is enough to verify the contract of
``emit()`` (parameter shaping, enum coercion, dict-detail serialization,
non-blocking on bad input).
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import List

import pytest

import supertable.audit as audit_pkg
from supertable.audit.events import (
    Actions,
    ActorType,
    AuditEvent,
    EventCategory,
    Outcome,
    Severity,
)


class FakeAuditLogger:
    """Drop-in replacement that just collects emitted events."""

    def __init__(self) -> None:
        self.events: List[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        self.events.append(event)


@pytest.fixture
def fake_logger(monkeypatch: pytest.MonkeyPatch) -> FakeAuditLogger:
    fake = FakeAuditLogger()
    monkeypatch.setattr(audit_pkg, "get_audit_logger", lambda org: fake, raising=True)
    return fake


# ---------------------------------------------------------------------------
# emit()
# ---------------------------------------------------------------------------


class TestEmit:
    def test_no_organization_is_silently_dropped(
        self, fake_logger: FakeAuditLogger
    ) -> None:
        audit_pkg.emit(
            category=EventCategory.RBAC_CHANGE,
            action=Actions.ROLE_CREATE,
            organization="",
        )
        assert fake_logger.events == []

    def test_minimal_call_is_recorded(self, fake_logger: FakeAuditLogger) -> None:
        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
        )
        assert len(fake_logger.events) == 1
        ev = fake_logger.events[0]
        assert ev.organization == "acme"
        assert ev.category == EventCategory.DATA_ACCESS.value
        assert ev.action == Actions.QUERY_EXECUTE
        # Defaults
        assert ev.severity == Severity.INFO.value
        assert ev.outcome == Outcome.SUCCESS.value
        assert ev.actor_type == ActorType.SYSTEM.value

    def test_enum_coercion_for_category_severity_outcome_actor(
        self, fake_logger: FakeAuditLogger
    ) -> None:
        audit_pkg.emit(
            category=EventCategory.AUTHENTICATION,
            action=Actions.LOGIN_SUCCESS,
            organization="acme",
            actor_type=ActorType.USER,
            severity=Severity.WARNING,
            outcome=Outcome.DENIED,
        )
        ev = fake_logger.events[0]
        assert ev.category == "authentication"
        assert ev.actor_type == "user"
        assert ev.severity == "warning"
        assert ev.outcome == "denied"

    def test_string_passthrough_when_not_an_enum(
        self, fake_logger: FakeAuditLogger
    ) -> None:
        audit_pkg.emit(
            category="custom_category",
            action="custom_action",
            organization="acme",
            actor_type="api_token",
            severity="critical",
            outcome="failure",
        )
        ev = fake_logger.events[0]
        assert ev.category == "custom_category"
        assert ev.actor_type == "api_token"
        assert ev.severity == "critical"
        assert ev.outcome == "failure"

    def test_dict_detail_is_serialized_via_make_detail(
        self, fake_logger: FakeAuditLogger
    ) -> None:
        audit_pkg.emit(
            category=EventCategory.DATA_MUTATION,
            action=Actions.DATA_WRITE,
            organization="acme",
            detail={"rows": 42, "table": "facts", "drop": None},
        )
        ev = fake_logger.events[0]
        # make_detail drops None and emits compact JSON
        assert '"drop"' not in ev.detail
        assert '"rows":42' in ev.detail
        assert '"table":"facts"' in ev.detail

    def test_string_detail_passes_through(
        self, fake_logger: FakeAuditLogger
    ) -> None:
        audit_pkg.emit(
            category=EventCategory.SYSTEM,
            action=Actions.SERVICE_START,
            organization="acme",
            detail="raw-string-detail",
        )
        assert fake_logger.events[0].detail == "raw-string-detail"

    def test_other_detail_is_stringified(
        self, fake_logger: FakeAuditLogger
    ) -> None:
        audit_pkg.emit(
            category=EventCategory.SYSTEM,
            action=Actions.AUDIT_GAP,
            organization="acme",
            detail=12345,
        )
        assert fake_logger.events[0].detail == "12345"

    def test_user_agent_truncated_to_256(
        self, fake_logger: FakeAuditLogger
    ) -> None:
        ua = "x" * 1000
        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
            actor_user_agent=ua,
        )
        assert len(fake_logger.events[0].actor_user_agent) == 256

    def test_swallows_logger_exceptions(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class BoomLogger:
            def emit(self, event: AuditEvent) -> None:
                raise RuntimeError("logger blew up")

        monkeypatch.setattr(
            audit_pkg, "get_audit_logger", lambda org: BoomLogger(), raising=True
        )
        # Must not raise
        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
        )


# ---------------------------------------------------------------------------
# audit_context()
# ---------------------------------------------------------------------------


class _FakeRequest:
    def __init__(
        self,
        client_host: str | None = "1.2.3.4",
        user_agent: str = "Mozilla/5.0",
        state: object | None = None,
    ):
        self.client = SimpleNamespace(host=client_host) if client_host else None
        self.headers = {"user-agent": user_agent}
        self.state = state


class TestAuditContext:
    def test_anonymous_request(self) -> None:
        req = _FakeRequest(state=None)
        ctx = audit_pkg.audit_context(req)
        assert ctx["actor_ip"] == "1.2.3.4"
        assert ctx["actor_user_agent"] == "Mozilla/5.0"
        assert ctx["actor_type"] == ActorType.SYSTEM
        assert ctx["correlation_id"] == ""

    def test_user_session(self) -> None:
        state = SimpleNamespace(
            correlation_id="corr-1",
            session_username="alice",
            session_user_hash="abc",
            session_id="sid-1",
            session_is_superuser=False,
        )
        ctx = audit_pkg.audit_context(_FakeRequest(state=state))
        assert ctx["correlation_id"] == "corr-1"
        assert ctx["actor_username"] == "alice"
        assert ctx["actor_id"] == "abc"
        assert ctx["session_id"] == "sid-1"
        assert ctx["actor_type"] == ActorType.USER

    def test_superuser_session(self) -> None:
        state = SimpleNamespace(
            correlation_id="corr-1",
            session_username="root",
            session_user_hash="abc",
            session_id="sid-1",
            session_is_superuser=True,
        )
        ctx = audit_pkg.audit_context(_FakeRequest(state=state))
        assert ctx["actor_type"] == ActorType.SUPERUSER

    def test_user_agent_is_truncated_to_256(self) -> None:
        long_ua = "z" * 5000
        ctx = audit_pkg.audit_context(_FakeRequest(user_agent=long_ua))
        assert len(ctx["actor_user_agent"]) == 256

    def test_missing_client_does_not_crash(self) -> None:
        ctx = audit_pkg.audit_context(_FakeRequest(client_host=None))
        assert ctx["actor_ip"] == ""

    def test_emit_with_audit_context(self, fake_logger: FakeAuditLogger) -> None:
        state = SimpleNamespace(
            correlation_id="cid-9",
            session_username="alice",
            session_user_hash="hash-1",
            session_id="sess",
            session_is_superuser=False,
        )
        ctx = audit_pkg.audit_context(_FakeRequest(state=state))
        audit_pkg.emit(
            **ctx,
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
        )

        ev = fake_logger.events[0]
        assert ev.actor_username == "alice"
        assert ev.actor_id == "hash-1"
        assert ev.actor_ip == "1.2.3.4"
        assert ev.session_id == "sess"
        assert ev.correlation_id == "cid-9"
        assert ev.actor_type == ActorType.USER.value
