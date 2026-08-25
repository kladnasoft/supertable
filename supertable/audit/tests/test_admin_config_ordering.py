# route: supertable.audit.tests.test_admin_config_ordering
"""Ordered runtime semantics for Redis-backed audit configuration changes."""
from __future__ import annotations

import importlib
import logging
import threading
from types import SimpleNamespace

import pytest

import supertable.audit as audit_pkg
from supertable.audit import admin
from supertable.audit.events import Actions, EventCategory


class _ConfigRedis:
    def __init__(
        self,
        *,
        enabled: bool,
        log_queries: bool = True,
        timeline: list[tuple] | None = None,
    ) -> None:
        self.data = {
            "enabled": "true" if enabled else "false",
            "log_queries": "true" if log_queries else "false",
            "log_reads": "false",
            "hash_chain": "false",
            "siem_enabled": "false",
        }
        self.timeline = timeline if timeline is not None else []

    def hgetall(self, _key: str) -> dict[str, str]:
        return dict(self.data)

    def hset(self, _key: str, *, mapping: dict[str, str]) -> None:
        self.timeline.append(("persist", dict(mapping)))
        self.data.update(mapping)


@pytest.mark.parametrize(
    ("kwargs", "error_type"),
    [
        ({"enabled": 1}, TypeError),
        ({"log_queries": "true"}, TypeError),
        ({"updated_by": 7}, TypeError),
        ({"updated_by": "x" * 257}, ValueError),
        ({"updated_by": "\ud800"}, ValueError),
    ],
)
def test_invalid_config_inputs_never_reach_redis(
    monkeypatch: pytest.MonkeyPatch,
    kwargs: dict,
    error_type: type[Exception],
) -> None:
    redis = _ConfigRedis(enabled=False)
    monkeypatch.setattr(admin, "_redis", lambda: redis)

    with pytest.raises(error_type):
        admin.set_audit_config("acme", **kwargs)

    assert redis.timeline == []


def test_invalid_organization_never_reaches_redis(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    redis = _ConfigRedis(enabled=False)
    monkeypatch.setattr(admin, "_redis", lambda: redis)

    with pytest.raises((TypeError, ValueError)):
        admin.set_audit_config("../other-tenant", enabled=True)

    assert redis.timeline == []


def _install_runtime(
    monkeypatch: pytest.MonkeyPatch,
    *,
    enabled: bool,
    log_queries: bool = True,
    flush_error: bool = False,
    stop_error: bool = False,
):
    logger_module = importlib.import_module("supertable.audit.logger")
    settings_module = importlib.import_module("supertable.config.settings")
    timeline: list[tuple] = []
    redis = _ConfigRedis(
        enabled=enabled,
        log_queries=log_queries,
        timeline=timeline,
    )
    created = []

    class FakeLogger:
        def __init__(self, organization, config) -> None:
            self.organization = organization
            self._config = config
            self.events = []
            self.stop_calls = 0
            self.flush_calls = 0
            created.append(self)
            timeline.append(
                ("create", config.enabled, config.log_queries)
            )

        def emit(self, event) -> None:
            self.events.append(event)
            timeline.append(("emit", event.action))

        def flush(self, *, timeout_s: float) -> None:
            self.flush_calls += 1
            timeline.append(("flush", timeout_s))
            if flush_error:
                raise RuntimeError("s3://audit-secret@example.invalid")

        def stop(self) -> None:
            self.stop_calls += 1
            timeline.append(
                ("stop", self._config.enabled, self._config.log_queries)
            )
            if stop_error:
                raise RuntimeError("s3://stop-secret@example.invalid")

    monkeypatch.setattr(admin, "_redis", lambda: redis)
    monkeypatch.setattr(
        settings_module,
        "settings",
        SimpleNamespace(
            SUPERTABLE_AUDIT_ENABLED=False,
            SUPERTABLE_AUDIT_LOG_QUERIES=False,
            SUPERTABLE_AUDIT_LOG_READS=False,
            SUPERTABLE_AUDIT_HASH_CHAIN=False,
            SUPERTABLE_AUDIT_SIEM_ENABLED=False,
            SUPERTABLE_AUDIT_FERNET_KEY="",
        ),
        raising=True,
    )
    monkeypatch.setattr(logger_module, "AuditLogger", FakeLogger)
    monkeypatch.setattr(logger_module, "_LOGGERS", {})
    monkeypatch.setattr(logger_module, "_ORG_CFG_CACHE", {})
    return logger_module, redis, timeline, created


def test_on_to_off_records_under_prior_policy_then_disables_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module, _redis, timeline, created = _install_runtime(
        monkeypatch, enabled=True,
    )

    result = admin.set_audit_config(
        "acme", enabled=False, updated_by="security-admin",
    )

    assert result["enabled"] is False
    assert len(created) == 1
    prior_logger = created[0]
    assert prior_logger.stop_calls == 1
    assert len(prior_logger.events) == 1
    change = prior_logger.events[0]
    assert change.category == EventCategory.CONFIG_CHANGE.value
    assert change.action == "config.update"
    assert change.actor_username == "security-admin"
    assert change.detail == '{"enabled":"false"}'
    assert [entry[0] for entry in timeline] == [
        "create", "persist", "emit", "flush", "stop",
    ]
    assert isinstance(
        logger_module._LOGGERS["acme"], logger_module.NullAuditLogger,
    )
    assert logger_module._ORG_CFG_CACHE["acme"][0].enabled is False

    audit_pkg.emit(
        category=EventCategory.SYSTEM,
        action=Actions.DATA_WRITE,
        organization="acme",
    )
    assert len(prior_logger.events) == 1


def test_off_to_on_activates_then_records_and_admits_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module, _redis, timeline, created = _install_runtime(
        monkeypatch, enabled=False,
    )

    result = admin.set_audit_config("acme", enabled=True)

    assert result["enabled"] is True
    assert len(created) == 1
    active_logger = created[0]
    assert [entry[0] for entry in timeline] == [
        "persist", "create", "emit", "flush",
    ]
    assert active_logger.events[0].detail == '{"enabled":"true"}'
    assert logger_module._LOGGERS["acme"] is active_logger
    assert logger_module._ORG_CFG_CACHE["acme"][0].enabled is True

    audit_pkg.emit(
        category=EventCategory.SYSTEM,
        action=Actions.DATA_WRITE,
        organization="acme",
    )
    assert len(active_logger.events) == 2


def test_enabled_lane_change_replaces_prior_logger_before_return(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module, _redis, _timeline, created = _install_runtime(
        monkeypatch, enabled=True, log_queries=True,
    )

    result = admin.set_audit_config("acme", log_queries=False)

    assert result["log_queries"] is False
    assert len(created) == 2
    prior_logger, replacement = created
    assert prior_logger.stop_calls == 1
    assert len(prior_logger.events) == 1
    assert replacement._config.log_queries is False
    assert logger_module._LOGGERS["acme"] is replacement

    audit_pkg.emit(
        category=EventCategory.DATA_ACCESS,
        action=Actions.QUERY_EXECUTE,
        organization="acme",
        detail={"sql": "SELECT private_value"},
    )
    assert replacement.events == []


def test_disabled_to_disabled_has_no_admitted_general_audit_lane(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module, _redis, _timeline, created = _install_runtime(
        monkeypatch, enabled=False,
    )

    result = admin.set_audit_config("acme", log_reads=True)

    assert result["enabled"] is False
    assert result["log_reads"] is True
    assert created == []
    assert isinstance(
        logger_module._LOGGERS["acme"], logger_module.NullAuditLogger,
    )


def test_enabled_change_never_reports_success_without_durable_meta_event(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _logger_module, redis, timeline, _created = _install_runtime(
        monkeypatch, enabled=False, flush_error=True,
    )

    with pytest.raises(admin.AuditConfigDurabilityError):
        admin.set_audit_config("acme", enabled=True)

    # The control-plane update is not falsely rolled back after Redis already
    # acknowledged it, but the caller receives an explicit durability failure.
    assert redis.data["enabled"] == "true"
    assert [entry[0] for entry in timeline] == [
        "persist", "create", "emit", "flush",
    ]


def test_runtime_reconciliation_error_logs_only_exception_type(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    _logger_module, _redis, _timeline, _created = _install_runtime(
        monkeypatch, enabled=True,
    )
    secret = "redis://admin:top-secret@example.test/0"

    class SecretBackendError(RuntimeError):
        pass

    def fail_refresh(*_args, **_kwargs):
        raise SecretBackendError(secret)

    monkeypatch.setattr(admin, "_refresh_runtime_policy", fail_refresh)
    caplog.set_level(logging.ERROR, logger="supertable.audit.admin")

    with pytest.raises(admin.AuditConfigActivationError):
        admin.set_audit_config("acme", enabled=True)
    assert "error_type=RuntimeError" in caplog.text
    assert secret not in caplog.text
    assert "top-secret" not in caplog.text


def test_replacement_stop_failure_cannot_return_stale_runtime_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module, redis, timeline, created = _install_runtime(
        monkeypatch,
        enabled=True,
        log_queries=True,
        stop_error=True,
    )

    with pytest.raises(admin.AuditConfigActivationError):
        admin.set_audit_config("acme", log_queries=False)

    assert redis.data["log_queries"] == "false"
    assert len(created) == 1
    assert logger_module._LOGGERS["acme"] is created[0]
    assert created[0].stop_calls == 1
    assert [entry[0] for entry in timeline] == [
        "create", "persist", "emit", "flush", "stop",
    ]


def test_expired_refresh_failure_preserves_last_known_enabled_policy(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    known = logger_module.AuditConfig(enabled=True, log_queries=True)
    monkeypatch.setattr(
        logger_module, "_ORG_CFG_CACHE", {"acme": (known, 0.0)},
    )
    secret = "redis://policy-secret@example.invalid"

    class PolicyReadError(RuntimeError):
        pass

    def fail_read(_org: str, *, strict: bool = False):
        assert strict is True
        raise PolicyReadError(secret)

    monkeypatch.setattr(admin, "get_audit_config", fail_read)
    caplog.set_level(logging.WARNING, logger="supertable.audit.logger")

    resolved = logger_module._resolve_config_for("acme")

    assert resolved is known
    assert logger_module._ORG_CFG_CACHE["acme"][0] is known
    assert logger_module._ORG_CFG_CACHE["acme"][1] > 0
    assert "error_type=RuntimeError" in caplog.text
    assert secret not in caplog.text


def test_first_authoritative_policy_failure_does_not_install_env_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    monkeypatch.setattr(logger_module, "_ORG_CFG_CACHE", {})
    monkeypatch.setattr(
        admin,
        "get_audit_config",
        lambda _org, *, strict=False: (_ for _ in ()).throw(
            RuntimeError("credential-bearing Redis failure")
        ),
    )

    with pytest.raises(logger_module.AuditConfigUnavailable):
        logger_module._resolve_config_for("acme")

    assert "acme" not in logger_module._ORG_CFG_CACHE


def test_malformed_authoritative_policy_preserves_last_known_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module = importlib.import_module("supertable.audit.logger")
    known = logger_module.AuditConfig(enabled=True, log_queries=True)
    monkeypatch.setattr(
        logger_module, "_ORG_CFG_CACHE", {"acme": (known, 0.0)},
    )

    class CorruptConfigRedis:
        def hgetall(self, _key):
            return {"enabled": "definitely-not-a-boolean"}

    monkeypatch.setattr(admin, "_redis", lambda: CorruptConfigRedis())

    assert logger_module._resolve_config_for("acme") is known
    assert logger_module._ORG_CFG_CACHE["acme"][0] is known


def test_invalidation_cannot_be_overtaken_by_blocked_old_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger_module, redis, _timeline, created = _install_runtime(
        monkeypatch, enabled=False,
    )
    real_get_config = admin.get_audit_config
    stale_read_entered = threading.Barrier(2)
    release_stale_read = threading.Event()
    refresh_started = threading.Event()
    refresh_done = threading.Event()
    outcomes: dict[str, object] = {}
    errors: list[BaseException] = []

    def blocked_get_config(org: str, **kwargs):
        snapshot = real_get_config(org, **kwargs)
        if threading.current_thread().name == "stale-audit-resolver":
            stale_read_entered.wait(timeout=2.0)
            if not release_stale_read.wait(timeout=2.0):
                raise AssertionError("stale config read was not released")
        return snapshot

    monkeypatch.setattr(admin, "get_audit_config", blocked_get_config)

    def resolve_stale() -> None:
        try:
            outcomes["stale"] = logger_module.get_audit_logger(
                "acme", action=Actions.DATA_WRITE,
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    def activate_new_policy() -> None:
        try:
            refresh_started.set()
            logger_module.invalidate_audit_config_cache("acme")
            outcomes["fresh"] = logger_module.get_audit_logger(
                "acme", action=Actions.DATA_WRITE,
            )
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)
        finally:
            refresh_done.set()

    stale_thread = threading.Thread(
        target=resolve_stale, name="stale-audit-resolver",
    )
    stale_thread.start()
    stale_read_entered.wait(timeout=2.0)

    # The Redis state changes while the first resolver is paused after reading
    # the old value.  Invalidation must serialize behind that resolution and
    # then win; it must not complete first and be overwritten by stale state.
    redis.data["enabled"] = "true"
    refresh_thread = threading.Thread(target=activate_new_policy)
    refresh_thread.start()
    assert refresh_started.wait(timeout=1.0)
    assert refresh_done.wait(timeout=0.05) is False

    release_stale_read.set()
    stale_thread.join(timeout=2.0)
    refresh_thread.join(timeout=2.0)

    assert stale_thread.is_alive() is False
    assert refresh_thread.is_alive() is False
    assert errors == []
    assert isinstance(outcomes["stale"], logger_module.NullAuditLogger)
    assert outcomes["fresh"] is created[0]
    assert created[0]._config.enabled is True
    assert logger_module._LOGGERS["acme"] is created[0]
    assert logger_module._ORG_CFG_CACHE["acme"][0].enabled is True
