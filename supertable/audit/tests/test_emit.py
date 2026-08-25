# route: supertable.audit.tests.test_emit
"""Tests for the public ``supertable.audit.emit`` and ``audit_context``.

The real ``AuditLogger`` spawns a worker thread and talks to Redis; for these
tests we replace ``get_audit_logger`` with a fake that just records the
``AuditEvent`` instances it receives. That is enough to verify the contract of
``emit()`` (parameter shaping, enum coercion, dict-detail serialization,
non-blocking on bad input).
"""
from __future__ import annotations

import hashlib
import importlib
import json
import logging
import queue
import sys
import threading
from types import SimpleNamespace
from typing import List

import pytest
from cryptography.fernet import Fernet

import supertable.audit as audit_pkg
from supertable.audit import crypto
from supertable.audit.events import (
    Actions,
    ActorType,
    AuditEvent,
    EventCategory,
    Outcome,
    Severity,
)
from supertable.audit.logger import AuditConfig, AuditLogger


class FakeAuditLogger:
    """Drop-in replacement that just collects emitted events."""

    def __init__(self) -> None:
        self.events: List[AuditEvent] = []

    def emit(self, event: AuditEvent) -> None:
        self.events.append(event)


@pytest.fixture
def fake_logger(monkeypatch: pytest.MonkeyPatch) -> FakeAuditLogger:
    fake = FakeAuditLogger()
    monkeypatch.setattr(
        audit_pkg,
        "get_audit_logger",
        lambda org, *, action=None: fake,
        raising=True,
    )
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

    def test_list_detail_is_canonical_json_and_sensitive_values_are_protected(
        self,
        fake_logger: FakeAuditLogger,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import supertable.config.settings as settings_module

        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=""),
            raising=True,
        )
        crypto._fernet_instance = None
        crypto._fernet_loaded = False
        crypto._fernet_key = None

        audit_pkg.emit(
            category=EventCategory.DATA_MUTATION,
            action=Actions.DATA_WRITE,
            organization="acme",
            detail=[{"sql": "SELECT nested_secret", "rows": 1}],
        )

        rendered = fake_logger.events[0].detail
        assert "nested_secret" not in rendered
        assert json.loads(rendered) == [{
            "sql_sha256": hashlib.sha256(
                b"SELECT nested_secret"
            ).hexdigest(),
            "sql_redacted": True,
            "rows": 1,
        }]

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
            audit_pkg,
            "get_audit_logger",
            lambda org, *, action=None: BoomLogger(),
            raising=True,
        )
        # Must not raise
        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
        )

    def test_logger_resolution_failure_bounds_exception_class_name(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        secret_name = "SignedUrlSecret-DO-NOT-LOG"
        secret_message = "redis://admin:top-secret@example.test/0"
        hostile_error = type(secret_name, (RuntimeError,), {})

        def fail_resolution(_organization: str, *, action=None):
            raise hostile_error(secret_message)

        monkeypatch.setattr(
            audit_pkg,
            "get_audit_logger",
            fail_resolution,
            raising=True,
        )
        caplog.set_level(logging.ERROR, logger="supertable.audit")

        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
        )

        assert "logger resolution failed: RuntimeError" in caplog.text
        assert secret_name not in caplog.text
        assert "top-secret" not in caplog.text
        assert "redis://" not in caplog.text

        class HostileLogger:
            def emit(self, _event: AuditEvent) -> None:
                raise hostile_error(secret_message)

        monkeypatch.setattr(
            audit_pkg,
            "get_audit_logger",
            lambda _organization, *, action=None: HostileLogger(),
            raising=True,
        )
        caplog.clear()

        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
        )

        assert "emit failed: RuntimeError" in caplog.text
        assert secret_name not in caplog.text
        assert "top-secret" not in caplog.text
        assert "redis://" not in caplog.text

    def test_configured_query_sql_is_encrypted_before_enqueue(
        self,
        fake_logger: FakeAuditLogger,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import supertable.config.settings as settings_module

        key = Fernet.generate_key().decode("utf-8")
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=key),
            raising=True,
        )
        monkeypatch.setattr(crypto, "_fernet_instance", None)
        monkeypatch.setattr(crypto, "_fernet_loaded", False)
        plaintext = "SELECT * FROM patients WHERE ssn = 'very-secret'"

        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
            detail={"sql": plaintext, "row_count": 3},
        )

        assert len(fake_logger.events) == 1
        event = fake_logger.events[0]
        assert plaintext not in event.detail
        payload = json.loads(event.detail)
        assert "sql" not in payload
        assert payload["sql_sha256"] == hashlib.sha256(
            plaintext.encode("utf-8")
        ).hexdigest()
        assert crypto.decrypt_field(payload["sql_encrypted"]) == plaintext
        assert payload["row_count"] == 3

    def test_unconfigured_query_sql_is_redacted_before_enqueue(
        self,
        fake_logger: FakeAuditLogger,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import supertable.config.settings as settings_module

        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=""),
            raising=True,
        )
        monkeypatch.setattr(crypto, "_fernet_instance", None)
        monkeypatch.setattr(crypto, "_fernet_loaded", False)
        plaintext = "SELECT card_number FROM payments"

        audit_pkg.emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization="acme",
            detail=json.dumps({"sql": plaintext, "row_count": 2}),
        )

        event = fake_logger.events[0]
        assert plaintext not in event.detail
        payload = json.loads(event.detail)
        assert payload == {
            "sql_sha256": hashlib.sha256(
                plaintext.encode("utf-8")
            ).hexdigest(),
            "sql_redacted": True,
            "row_count": 2,
        }

    def test_configured_encryption_failure_never_enqueues_plaintext(
        self,
        fake_logger: FakeAuditLogger,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class BrokenFernet:
            def encrypt(self, _value: bytes) -> bytes:
                raise RuntimeError("simulated encryption failure")

        monkeypatch.setattr(crypto, "_fernet_instance", BrokenFernet())
        monkeypatch.setattr(crypto, "_fernet_loaded", True)
        import supertable.config.settings as settings_module

        key = Fernet.generate_key().decode("utf-8")
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=key),
            raising=True,
        )
        monkeypatch.setattr(crypto, "_fernet_key", key)

        with pytest.raises(
            crypto.AuditEncryptionError,
            match="configured audit encryption failed",
        ):
            audit_pkg.emit(
                category=EventCategory.DATA_ACCESS,
                action=Actions.QUERY_EXECUTE,
                organization="acme",
                detail={"sql": "SELECT 'must-not-leak'"},
            )
        assert fake_logger.events == []

    def test_admitted_logger_validation_fails_closed_on_invalid_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import supertable.config.settings as settings_module

        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(
                SUPERTABLE_AUDIT_ENABLED=True,
                SUPERTABLE_AUDIT_FERNET_KEY="invalid-key",
            ),
            raising=True,
        )
        monkeypatch.setattr(crypto, "_fernet_instance", None)
        monkeypatch.setattr(crypto, "_fernet_loaded", False)

        config = AuditConfig.from_settings()
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="failed to initialize configured audit encryption",
        ):
            AuditLogger("acme", config)

    def test_encryption_error_from_logger_resolution_is_not_swallowed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def unavailable(_organization: str, *, action=None):
            raise crypto.AuditEncryptionError("configured encryption unavailable")

        monkeypatch.setattr(
            audit_pkg, "get_audit_logger", unavailable, raising=True,
        )
        with pytest.raises(
            crypto.AuditEncryptionError,
            match="configured encryption unavailable",
        ):
            audit_pkg.emit(
                category=EventCategory.SYSTEM,
                action=Actions.SERVICE_START,
                organization="acme",
            )

    def test_direct_logger_event_cannot_bypass_query_protection(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import supertable.config.settings as settings_module

        key = Fernet.generate_key().decode("utf-8")
        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=key),
            raising=True,
        )
        monkeypatch.setattr(crypto, "_fernet_instance", None)
        monkeypatch.setattr(crypto, "_fernet_loaded", False)
        plaintext = "SELECT private_value FROM secrets"
        audit_logger = AuditLogger.__new__(AuditLogger)
        audit_logger._org = "acme"
        audit_logger._queue = queue.Queue(maxsize=4)
        audit_logger._admission_lock = threading.Lock()
        audit_logger._accepting = True
        audit_logger._stats_lock = threading.Lock()
        audit_logger._stats = {
            "total_emitted": 0,
            "total_written": 0,
            "total_dropped": 0,
            "batches_written": 0,
        }

        audit_logger.emit(AuditEvent(
            organization="acme",
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            detail=plaintext,
        ))

        queued = audit_logger._queue.get_nowait()
        assert queued is not None
        assert plaintext not in queued.event.detail
        protected = json.loads(queued.event.detail)
        assert crypto.decrypt_field(protected["sql_encrypted"]) == plaintext

    def test_direct_logger_protects_json_sql_for_non_query_action(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import supertable.config.settings as settings_module

        monkeypatch.setattr(
            settings_module,
            "settings",
            SimpleNamespace(SUPERTABLE_AUDIT_FERNET_KEY=""),
            raising=True,
        )
        monkeypatch.setattr(crypto, "_fernet_instance", None)
        monkeypatch.setattr(crypto, "_fernet_loaded", False)
        plaintext = "DELETE FROM private_table"
        audit_logger = AuditLogger.__new__(AuditLogger)
        audit_logger._org = "acme"
        audit_logger._queue = queue.Queue(maxsize=4)
        audit_logger._admission_lock = threading.Lock()
        audit_logger._accepting = True
        audit_logger._stats_lock = threading.Lock()
        audit_logger._stats = {
            "total_emitted": 0,
            "total_written": 0,
            "total_dropped": 0,
            "batches_written": 0,
        }

        audit_logger.emit(AuditEvent(
            organization="acme",
            category=EventCategory.DATA_MUTATION,
            action=Actions.DATA_DELETE,
            detail=json.dumps({
                "statement": plaintext,
                "resource": "private_table",
            }),
        ))

        queued = audit_logger._queue.get_nowait()
        assert queued is not None
        assert plaintext not in queued.event.detail
        protected = json.loads(queued.event.detail)
        assert protected == {
            "statement_sha256": hashlib.sha256(
                plaintext.encode("utf-8")
            ).hexdigest(),
            "statement_redacted": True,
            "resource": "private_table",
        }

    def test_direct_logger_honours_disabled_query_toggle(self) -> None:
        audit_logger = AuditLogger.__new__(AuditLogger)
        audit_logger._config = AuditConfig(log_queries=False)
        audit_logger._queue = queue.Queue(maxsize=4)
        audit_logger._stats_lock = threading.Lock()
        audit_logger._stats = {
            "total_emitted": 0,
            "total_written": 0,
            "total_dropped": 0,
            "batches_written": 0,
        }

        audit_logger.emit(AuditEvent(
            organization="acme",
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            detail="SELECT must_not_be_enqueued",
        ))

        assert audit_logger._queue.empty()
        assert audit_logger._stats["total_emitted"] == 0

    @pytest.mark.parametrize("failure_mode", ["status", "exception"])
    def test_webhook_credentials_never_reach_logs(
        self,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
        failure_mode: str,
    ) -> None:
        logger_module = importlib.import_module("supertable.audit.logger")
        secret_url = (
            "https://audit-user:secret-password@alerts.example/"
            "hooks/private-token?api_key=top-secret"
        )

        class FakeClient:
            def __init__(self, **_kwargs) -> None:
                pass

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                return None

            def stream(self, method: str, url: str, **_kwargs):
                assert method == "POST"
                assert url == secret_url
                if failure_mode == "exception":
                    raise RuntimeError(f"request failed for {secret_url}")
                return FakeResponse()

        class FakeResponse:
            status_code = 503

            def __enter__(self):
                return self

            def __exit__(self, *_args) -> None:
                return None

        fake_httpx = SimpleNamespace(
            Client=FakeClient,
            Timeout=lambda *_args, **_kwargs: object(),
        )
        monkeypatch.setitem(sys.modules, "httpx", fake_httpx)

        class ImmediateThread:
            def __init__(self, *, target, **_kwargs) -> None:
                self._target = target

            def start(self) -> None:
                self._target()

        monkeypatch.setattr(logger_module.threading, "Thread", ImmediateThread)
        audit_logger = AuditLogger.__new__(AuditLogger)
        audit_logger._org = "acme"
        audit_logger._config = AuditConfig(alert_webhook=secret_url)

        with caplog.at_level(logging.WARNING, logger=logger_module.__name__):
            audit_logger._fire_webhook(AuditEvent(
                organization="acme",
                category=EventCategory.SECURITY_ALERT,
                action=Actions.UNUSUAL_ACCESS_PATTERN,
                severity=Severity.CRITICAL,
            ))

        assert secret_url not in caplog.text
        assert "secret-password" not in caplog.text
        assert "private-token" not in caplog.text
        assert "top-secret" not in caplog.text
        if failure_mode == "status":
            assert "POST returned 503" in caplog.text
        else:
            assert "POST failed: RuntimeError" in caplog.text


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
