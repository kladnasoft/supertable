from __future__ import annotations

import logging
import traceback
from dataclasses import replace

import pytest

from supertable.config import defaults
from supertable.config import settings as settings_module


def test_strict_integer_config_error_does_not_reflect_environment_value(monkeypatch):
    secret = "redis-password-DO-NOT-LOG"
    monkeypatch.setenv("CONFIDENTIAL_INTEGER", secret)

    with pytest.raises(ValueError) as caught:
        settings_module._env_int_strict(
            "CONFIDENTIAL_INTEGER",
            1,
            minimum=1,
            maximum=10,
        )

    rendered = "".join(
        traceback.format_exception(
            type(caught.value), caught.value, caught.value.__traceback__,
        )
    )
    assert str(caught.value) == "CONFIDENTIAL_INTEGER must be an integer"
    assert secret not in rendered


def test_invalid_log_level_warning_does_not_reflect_environment_value(
    monkeypatch,
    caplog,
):
    secret = "authorization-bearer-DO-NOT-LOG"
    monkeypatch.setattr(
        defaults,
        "settings",
        replace(defaults.settings, SUPERTABLE_LOG_LEVEL=secret),
    )
    caplog.set_level(logging.WARNING, logger=defaults.logger.name)

    configured = defaults.load_defaults_from_env()

    assert configured.LOG_LEVEL == "INFO"
    assert secret not in caplog.text
