"""Hostile confidentiality checks for core SDK diagnostic boundaries."""

from __future__ import annotations

import logging
import time
import traceback
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import redis

from supertable.config import homedir
from supertable.locking.redis_lock import RedisLocking
from supertable.monitoring import partitions as monitoring_partitions
from supertable import monitoring_writer
from supertable import odata_continuation
from supertable import simple_table
from supertable import staging_area
from supertable.rbac import access_control
from supertable.rbac.permissions import Permission
from supertable.storage.storage_interface import ObjectMetadata
from supertable.super_pipe import SuperPipe


def _rendered(error: BaseException) -> str:
    return "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )


def test_application_home_diagnostics_never_render_paths_or_backend_text(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "/private/TOP_SECRET_HOME?access_token=sentinel"
    monkeypatch.setattr(
        homedir,
        "settings",
        SimpleNamespace(SUPERTABLE_HOME=secret),
    )
    monkeypatch.setattr(homedir, "_is_writable_dir", lambda _path: False)
    homedir._resolved_home = None
    caplog.set_level(logging.DEBUG, logger=homedir.__name__)

    with pytest.raises(RuntimeError) as caught:
        homedir._resolve_app_home()

    assert secret not in _rendered(caught.value)
    assert secret not in caplog.text

    caplog.clear()

    def fail_chdir(_path: str) -> None:
        raise OSError(f"backend rejected {secret}")

    monkeypatch.setattr(homedir.os, "chdir", fail_chdir)
    homedir.change_to_app_home(secret)
    assert secret not in caplog.text


def test_redis_lock_logs_neither_lock_key_nor_unsafe_exception_name(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "redis://user:TOP_SECRET_LOCK@host/private"
    unsafe_error = type(f"RedisBackend_{secret}", (redis.RedisError,), {})

    class BrokenRedis:
        def register_script(self, _body: str):
            def fail(*_args, **_kwargs):
                raise unsafe_error(secret)

            return fail

    locker = RedisLocking(BrokenRedis())  # type: ignore[arg-type]
    caplog.set_level(logging.DEBUG)
    try:
        assert locker.release(f"lock:{secret}", "token") is False
    finally:
        locker._on_exit()

    assert secret not in caplog.text
    assert "error_type=RedisError" in caplog.text


def test_poisoned_role_type_is_absent_from_logs_and_exception_traceback(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "https://idp.invalid/private?access_token=RBAC_SENTINEL"
    manager = MagicMock()
    manager.get_role_by_name.return_value = {
        "role_id": "role-1",
        "role": secret,
        "tables": {"*": {"columns": ["*"], "filters": ["*"]}},
    }
    monkeypatch.setattr(access_control, "RoleManager", lambda **_kwargs: manager)
    caplog.set_level(logging.DEBUG)

    with pytest.raises(PermissionError) as caught:
        access_control.resolve_role_access_context(
            "lake", "tenant", "caller-role", Permission.READ, "read data",
        )

    assert secret not in _rendered(caught.value)
    assert secret not in caplog.text
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None


def test_odata_poison_value_is_absent_from_formatted_validation_traceback() -> None:
    secret = "https://host/private/TOP_SECRET_ODATA?sig=sentinel"
    boundary = {
        "version": 1,
        "order": [{
            "column": "id",
            "direction": "asc",
            "value": {"type": "binary", "value": secret},
        }],
        "row_identity": 1,
    }

    with pytest.raises(ValueError) as caught:
        odata_continuation.validate_odata_continuation_boundary(boundary)

    assert secret not in _rendered(caught.value)
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "reader",
    [simple_table._read_sealed_json_object, staging_area._read_sealed_json_object],
)
def test_poisoned_control_json_is_absent_from_formatted_tracebacks(reader) -> None:
    secret = "https://object.invalid/private/TOP_SECRET_JSON?sig=sentinel"
    encoded = f'{{"credential":"{secret}"'.encode()
    observed = ObjectMetadata(size=len(encoded), version="immutable-v1")

    class PoisonStorage:
        def stat_object(self, _path: str) -> ObjectMetadata:
            return observed

        def read_range(self, *_args, **_kwargs) -> bytes:
            return encoded

    with pytest.raises(RuntimeError) as caught:
        reader(
            PoisonStorage(),
            "tenant/private/control.json",
            max_bytes=4096,
            label="Control document",
        )

    assert secret not in _rendered(caught.value)
    assert caught.value.__cause__ is None


def test_poisoned_persisted_pipe_name_is_not_echoed_by_duplicate_error() -> None:
    secret = "https://catalog.invalid/private/TOP_SECRET_PIPE?sig=sentinel"
    pipe = SuperPipe.__new__(SuperPipe)
    pipe.organization = "tenant"
    pipe.super_name = "lake"
    pipe.staging_name = "uploads"
    pipe.catalog = MagicMock()
    pipe.catalog.acquire_stage_lock.return_value = "lease"
    pipe.catalog.get_pipe_meta.return_value = None
    pipe.catalog.list_pipe_metas.return_value = [{
        "simple_name": "target",
        "overwrite_columns": [],
        "pipe_name": secret,
    }]
    pipe._check_target_access = MagicMock()  # type: ignore[method-assign]

    with pytest.raises(ValueError) as caught:
        pipe.create(
            role_name="writer",
            pipe_name="new-pipe",
            simple_name="target",
            user_hash="user-1",
            overwrite_columns=[],
        )

    assert secret not in _rendered(caught.value)


def _monitor_envelope(secret: str) -> dict:
    return {
        "organization": secret,
        "monitor_type": "writes",
        "partition_date": "2099-01-01",
        "partition_key": "partition",
        "partition_expires_at": int(time.time()) + 3600,
        "receipt_key": "receipt",
        "receipt_expires_at": int(time.time()) + 7200,
        "delivery_id": "0" * 32,
        "payload_sha256": "1" * 64,
        "payload_json": "{}",
    }


def test_monitor_delivery_does_not_render_or_log_backend_and_tenant_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "https://tenant.invalid/private/TOP_SECRET_MONITOR?sig=sentinel"

    class UnrenderableBackendError(RuntimeError):
        def __str__(self) -> str:
            raise AssertionError("backend exceptions must not be rendered")

    class BrokenRedis:
        def eval(self, *_args, **_kwargs):
            raise UnrenderableBackendError(secret)

    record = SimpleNamespace(envelope=_monitor_envelope(secret))
    assert monitoring_writer._deliver_spool_record(
        record, SimpleNamespace(r=BrokenRedis()), ship_to_redis=True,
    ) is False

    caplog.set_level(logging.DEBUG)
    assert monitoring_writer._deliver_spool_record(
        record, None, ship_to_redis=False,
    ) is True
    assert secret not in caplog.text


def test_monitor_delivery_recognizes_exact_collision_without_string_rendering() -> None:
    class CollisionRedis:
        def eval(self, *_args, **_kwargs):
            raise RuntimeError("monitoring delivery id collision")

    record = SimpleNamespace(envelope=_monitor_envelope("tenant"))
    with pytest.raises(
        monitoring_writer.MonitoringDurabilityError,
        match="receipt conflicts",
    ):
        monitoring_writer._deliver_spool_record(
            record, SimpleNamespace(r=CollisionRedis()), ship_to_redis=True,
        )


def test_monitoring_post_errors_keep_identifiers_out_of_formatted_text() -> None:
    secret = "https://tenant.invalid/private/TOP_SECRET_POST?sig=sentinel"
    cause = monitoring_writer.MonitoringBackpressureError(secret)
    errors = [
        monitoring_writer.MonitoringPostCommitError(
            organization=secret,
            super_name=secret,
            table_name=secret,
            operation=secret,
            core_result=True,
            cause=cause,
        ),
        monitoring_writer.MonitoringPostExecutionError(
            organization=secret,
            super_name=secret,
            query_id=secret,
            status=secret,
            cause=cause,
        ),
    ]

    for error in errors:
        assert secret not in str(error)
        assert secret not in str(error.cause)


def test_monitor_partition_scan_log_omits_tenant_and_backend_text(
    caplog: pytest.LogCaptureFixture,
) -> None:
    tenant = "tenant-secret-monitor"
    secret = "redis://user:TOP_SECRET_SCAN@host/0"

    class BrokenRedis:
        def scan_iter(self, **_kwargs):
            raise RuntimeError(secret)

    caplog.set_level(logging.WARNING, logger=monitoring_partitions.__name__)
    assert monitoring_partitions.list_drainable_partitions(
        SimpleNamespace(r=BrokenRedis()), organization=tenant,
    ) == []
    assert tenant not in caplog.text
    assert secret not in caplog.text
