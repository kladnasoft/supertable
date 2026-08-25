from __future__ import annotations

import json
import logging
import traceback

import fakeredis
import pytest

from supertable import redis_keys as RK
from supertable.quality import history
from supertable.quality.config import DQConfig, DQConfigReadError
from supertable.quality.cron import CronSchedule, CronValidationError


_SECRET = (
    "https://objects.example/private/customer/export.parquet?"
    "X-Amz-Signature=QUALITY_SECRET_7f31; "
    "SELECT password FROM tenant_credentials"
)


class _SecretBackendError(RuntimeError):
    pass


def _seed_live_catalog(redis_client, table: str = "facts") -> None:
    redis_client.set(
        RK.meta_root("org", "lake"),
        json.dumps({"version": 0, "ts": 1}),
    )
    redis_client.set(
        RK.meta_leaf("org", "lake", table),
        json.dumps({
            "version": 0,
            "ts": 1,
            "path": f"org/lake/{table}/snapshot.json",
        }),
    )


def _log_text(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(record.getMessage() for record in caplog.records)


def _public_error_text(error: BaseException) -> str:
    return "\n".join((
        str(error),
        repr(error),
        "".join(traceback.format_exception(error)),
    ))


def _assert_no_secret(text: str) -> None:
    assert "QUALITY_SECRET_7f31" not in text
    assert "tenant_credentials" not in text
    assert "private/customer/export.parquet" not in text


def test_config_backend_read_failure_has_safe_log_and_public_wrapper(caplog):
    class FailingRedis:
        def get(self, _key):
            raise _SecretBackendError(_SECRET)

    config = DQConfig(FailingRedis(), "org", "lake")
    caplog.set_level(logging.ERROR, logger="supertable.quality.config")

    with pytest.raises(DQConfigReadError) as caught:
        config.get_global_config()

    _assert_no_secret(_log_text(caplog))
    _assert_no_secret(_public_error_text(caught.value))
    assert caught.value.__cause__ is None
    assert all(record.exc_info is None for record in caplog.records)
    assert "error_type=_SecretBackendError" in _log_text(caplog)


def test_config_dynamic_backend_class_name_is_not_reflected(caplog):
    class_secret = "QualityRedis_DYNAMIC_CLASS_SECRET"
    hostile_error = type(class_secret, (RuntimeError,), {})(_SECRET)

    class FailingRedis:
        def get(self, _key):
            raise hostile_error

    config = DQConfig(FailingRedis(), "org", "lake")
    caplog.set_level(logging.ERROR, logger="supertable.quality.config")

    with pytest.raises(DQConfigReadError):
        config.get_global_config()

    assert class_secret not in _log_text(caplog)
    assert "error_type=RuntimeError" in _log_text(caplog)


@pytest.mark.parametrize(
    "error_type",
    [_SecretBackendError, ValueError],
)
def test_config_mutation_failure_has_safe_log_and_public_wrapper(
    caplog,
    error_type,
):
    class FailingRedis:
        def pipeline(self):
            raise error_type(_SECRET)

    config = DQConfig(FailingRedis(), "org", "lake")
    caplog.set_level(logging.ERROR, logger="supertable.quality.config")

    with pytest.raises(RuntimeError) as caught:
        config.create_rule({
            "rule_id": "confidentiality-test",
            "table_name": "facts",
            "rule_type": "row_count_min",
            "threshold": 1,
        })

    _assert_no_secret(_log_text(caplog))
    _assert_no_secret(_public_error_text(caught.value))
    assert caught.value.__cause__ is None
    assert all(record.exc_info is None for record in caplog.records)
    assert f"error_type={error_type.__name__}" in _log_text(caplog)


def test_history_datawriter_failure_never_logs_backend_message(
    monkeypatch,
    caplog,
):
    import supertable.data_writer as data_writer

    class FailingWriter:
        def __init__(self, *args, **kwargs):
            pass

        def write(self, *args, **kwargs):
            raise _SecretBackendError(_SECRET)

    monkeypatch.setattr(data_writer, "DataWriter", FailingWriter)
    caplog.set_level(logging.WARNING, logger="supertable.quality.history")

    assert not history.write_history(
        "org",
        "lake",
        "facts",
        "quick",
        {"checked_at": "2026-08-25T00:00:00+00:00", "parsed": {}},
        history_id="history-confidentiality-test",
    )

    _assert_no_secret(_log_text(caplog))
    assert all(record.exc_info is None for record in caplog.records)
    assert "parquet_write_failed" in _log_text(caplog)
    assert "error_type=_SecretBackendError" in _log_text(caplog)


def test_history_redis_failure_never_logs_backend_message(monkeypatch, caplog):
    class FailingRedis:
        def lpush(self, *_args, **_kwargs):
            raise _SecretBackendError(_SECRET)

        def ltrim(self, *_args, **_kwargs):
            raise AssertionError("ltrim must not follow a failed lpush")

    monkeypatch.setattr(
        "supertable.redis_connector.create_redis_client",
        lambda: FailingRedis(),
    )
    caplog.set_level(logging.WARNING, logger="supertable.quality.history")

    assert not history.write_history_via_sql(
        "org",
        "lake",
        "facts",
        "quick",
        {"checked_at": "2026-08-25T00:00:00+00:00", "parsed": {}},
        history_id="history-confidentiality-test",
    )

    _assert_no_secret(_log_text(caplog))
    assert all(record.exc_info is None for record in caplog.records)
    assert "redis_write_failed" in _log_text(caplog)
    assert "error_type=_SecretBackendError" in _log_text(caplog)


@pytest.mark.parametrize(
    ("expression", "timezone_name", "expected"),
    [
        (
            f"* * * * {_SECRET}",
            "UTC",
            "invalid five-field cron expression; error_type=ValueError",
        ),
        (
            "* * * * *",
            f"Europe/{_SECRET}",
            "unknown IANA timezone",
        ),
    ],
)
def test_cron_validation_never_reflects_persisted_config(
    expression,
    timezone_name,
    expected,
):
    with pytest.raises(CronValidationError) as caught:
        CronSchedule.parse(expression, timezone_name)

    assert str(caught.value) == expected
    assert caught.value.__cause__ is None
    _assert_no_secret(_public_error_text(caught.value))
