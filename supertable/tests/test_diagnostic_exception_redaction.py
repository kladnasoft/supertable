"""Hostile exception metadata regressions for shipped diagnostic surfaces."""

from __future__ import annotations

import json
import logging
import traceback

import pytest

from supertable.data_reader import _caller_deadline
from supertable.engine.data_estimator import _trusted_storage_type
from supertable.locking.file_lock import _safe_error_type as file_lock_error_type
from supertable.locking.redis_lock import _safe_error_type as redis_lock_error_type
from supertable.logging import JSONFormatter, TextFormatter
from supertable.meta_reader import _safe_error_type as meta_reader_error_type
from supertable.mirroring.failure_safety import mirror_error_type
from supertable.monitoring.partitions import (
    _safe_error_type as partition_error_type,
)
from supertable.monitoring_writer import monitoring_error_type
from supertable.processing import _checked_tombstone_expected_rows
from supertable.quality.config import _safe_error_type as quality_config_error_type
from supertable.quality.history import _safe_error_type as quality_history_error_type
from supertable.quality.scheduler import (
    _safe_error_type as quality_scheduler_error_type,
)
from supertable.quality.serialization import (
    JSONNormalizationError,
    normalize_json_value,
)
from supertable.rbac.user_manager import _safe_error_type as user_manager_error_type
from supertable.recovery.redis_rebuild import (
    _safe_error_type as recovery_error_type,
)
from supertable.redis_catalog import RedisCatalog
from supertable.redis_keys import _safe
from supertable.storage.storage_interface import StorageInterface, storage_error_type
from supertable.utils.diagnostic_redaction import safe_exception_type


_CLASS_SECRET = "CredentialTokenInDynamicExceptionName"
_MESSAGE_SECRET = "Authorization: Bearer EXCEPTION_MESSAGE_SECRET"


class PublishedDiagnosticError(RuntimeError):
    """A normal module-published exception retains useful taxonomy."""


def _hostile_error(base: type[Exception] = RuntimeError) -> Exception:
    rendered = {"called": False}

    def hostile_str(_self) -> str:
        rendered["called"] = True
        raise AssertionError("diagnostics must not invoke exception __str__")

    error_type = type(
        _CLASS_SECRET,
        (base,),
        {"__str__": hostile_str},
    )
    error = error_type(_MESSAGE_SECRET)
    error._rendered = rendered  # type: ignore[attr-defined]
    return error


def _hostile_value() -> object:
    value_type = type(
        _CLASS_SECRET,
        (),
        {
            "__repr__": lambda _self: _MESSAGE_SECRET,
            "__str__": lambda _self: _MESSAGE_SECRET,
        },
    )
    return value_type()


def _formatted(error: BaseException) -> str:
    return "".join(traceback.format_exception(error))


def test_safe_exception_type_rejects_runtime_class_name_without_rendering() -> None:
    error = _hostile_error()

    assert safe_exception_type(error) == "RuntimeError"
    assert error._rendered["called"] is False  # type: ignore[attr-defined]
    assert safe_exception_type(PublishedDiagnosticError("fixed")) == (
        "PublishedDiagnosticError"
    )


@pytest.mark.parametrize(
    "extractor",
    [
        file_lock_error_type,
        redis_lock_error_type,
        meta_reader_error_type,
        mirror_error_type,
        partition_error_type,
        monitoring_error_type,
        quality_config_error_type,
        quality_history_error_type,
        quality_scheduler_error_type,
        user_manager_error_type,
        recovery_error_type,
        storage_error_type,
    ],
)
def test_non_audit_error_type_helpers_use_published_bounded_taxonomy(
    extractor,
) -> None:
    error = _hostile_error()

    assert extractor(error) == "RuntimeError"
    assert error._rendered["called"] is False  # type: ignore[attr-defined]


def test_logging_formatters_never_render_error_field_or_exception_prose() -> None:
    error = _hostile_error()
    record = logging.LogRecord(
        "diagnostic-test",
        logging.ERROR,
        __file__,
        1,
        "fixed failure",
        (),
        (type(error), error, None),
    )
    record.event = "proxy_error"
    record.method = "GET"
    record.path = "<request-path>"
    record.error = _MESSAGE_SECRET

    json_rendered = JSONFormatter(service="test").format(record)
    text_formatter = TextFormatter(service="test")
    text_formatter._color = False
    text_rendered = text_formatter.format(record)
    payload = json.loads(json_rendered)

    combined = json_rendered + text_rendered
    assert _CLASS_SECRET not in combined
    assert _MESSAGE_SECRET not in combined
    assert payload["error"] == "request_failed"
    assert payload["exception"] == "error_type=RuntimeError"
    assert error._rendered["called"] is False  # type: ignore[attr-defined]


def test_public_value_type_failures_never_reflect_dynamic_class_metadata() -> None:
    value = _hostile_value()

    with pytest.raises(JSONNormalizationError) as quality_error:
        normalize_json_value(value)
    with pytest.raises(RuntimeError) as redis_error:
        RedisCatalog._redis_key_text(value)
    with pytest.raises(ValueError) as key_error:
        _safe("organization", value)  # type: ignore[arg-type]

    rendered = " ".join(
        str(error.value)
        for error in (quality_error, redis_error, key_error)
    )
    assert _CLASS_SECRET not in rendered
    assert _MESSAGE_SECRET not in rendered
    assert _trusted_storage_type(value) == "custom"

    forged_pandas_sentinel = type("NAType", (), {})()
    with pytest.raises(JSONNormalizationError):
        normalize_json_value(forged_pandas_sentinel)


def test_storage_default_capability_error_does_not_reflect_adapter_name() -> None:
    adapter = _hostile_value()

    with pytest.raises(
        NotImplementedError,
        match=r"\AStorage adapter does not implement create_bytes_if_absent\(\)\Z",
    ) as caught:
        StorageInterface.create_bytes_if_absent(adapter, "ignored", b"")  # type: ignore[arg-type]

    rendered = _formatted(caught.value)
    assert _CLASS_SECRET not in rendered
    assert _MESSAGE_SECRET not in rendered


@pytest.mark.parametrize(
    "invoke",
    [
        lambda error: _caller_deadline(_FloatFailure(error)),
        lambda error: _checked_tombstone_expected_rows(
            _IntFailure(error), source="tombstone metadata",
        ),
    ],
)
def test_fixed_validation_wrappers_suppress_hostile_formatted_causes(invoke) -> None:
    error = _hostile_error(ValueError)

    with pytest.raises(ValueError) as caught:
        invoke(error)

    rendered = _formatted(caught.value)
    assert caught.value.__cause__ is None
    assert "During handling of the above exception" not in rendered
    assert _CLASS_SECRET not in rendered
    assert _MESSAGE_SECRET not in rendered
    assert error._rendered["called"] is False  # type: ignore[attr-defined]


class _FloatFailure:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __float__(self) -> float:
        raise self.error


class _IntFailure:
    def __init__(self, error: Exception) -> None:
        self.error = error

    def __int__(self) -> int:
        raise self.error
