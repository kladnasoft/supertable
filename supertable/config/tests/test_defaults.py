# route: supertable.config.tests.test_defaults
"""Tests for ``supertable.config.defaults``.

The module exposes:
  - A `colorlog`-backed module-level ``logger``
  - A ``Default`` dataclass with mutable convenience setter
  - A ``load_defaults_from_env`` factory that copies values out of
    ``settings`` and validates the log level
  - A module-level ``default`` instance constructed at import time

We test:
  - Default field values
  - ``update_default`` contract: only known fields, log-level side effect
  - ``load_defaults_from_env`` reads from settings and falls back on
    invalid log levels
"""
from __future__ import annotations

import logging
from types import SimpleNamespace

import pytest

from supertable.config import defaults as defaults_mod
from supertable.config.defaults import Default, default, load_defaults_from_env


class TestDefaultDataclass:
    def test_documented_default_values(self) -> None:
        d = Default()
        assert d.MAX_MEMORY_CHUNK_SIZE == 16 * 1024 * 1024
        assert d.MAX_OVERLAPPING_FILES == 100
        assert d.DEFAULT_TIMEOUT_SEC == 60
        assert d.DEFAULT_LOCK_DURATION_SEC == 30
        assert d.LOG_LEVEL == "INFO"
        assert d.IS_SHOW_TIMING is True
        assert d.STORAGE_TYPE == "LOCAL"

    def test_module_level_default_is_a_default_instance(self) -> None:
        assert isinstance(default, Default)

    def test_uses_slots(self) -> None:
        d = Default()
        with pytest.raises(AttributeError):
            d.brand_new_attribute = "x"  # type: ignore[attr-defined]


class TestUpdateDefault:
    def test_updates_known_fields(self) -> None:
        d = Default()
        d.update_default(MAX_OVERLAPPING_FILES=42, DEFAULT_TIMEOUT_SEC=99)
        assert d.MAX_OVERLAPPING_FILES == 42
        assert d.DEFAULT_TIMEOUT_SEC == 99

    def test_silently_ignores_unknown_fields(self) -> None:
        d = Default()
        # No error, no attribute created (slots prevent it anyway)
        d.update_default(NOT_A_REAL_FIELD="x")
        assert not hasattr(d, "NOT_A_REAL_FIELD")

    def test_log_level_change_propagates_to_root_logger(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        d = Default()
        original = logging.getLogger().level
        try:
            d.update_default(LOG_LEVEL="DEBUG")
            assert logging.getLogger().level == logging.DEBUG
        finally:
            logging.getLogger().setLevel(original)


class TestLoadDefaultsFromEnv:
    def test_copies_values_from_settings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_settings = SimpleNamespace(
            SUPERTABLE_LOG_LEVEL="WARNING",
            MAX_MEMORY_CHUNK_SIZE=12345,
            MAX_OVERLAPPING_FILES=7,
            MAX_TOMBSTONE_ROWS=999,
            DEFAULT_TIMEOUT_SEC=11,
            DEFAULT_LOCK_DURATION_SEC=22,
            IS_SHOW_TIMING=False,
            STORAGE_TYPE="S3",
        )
        monkeypatch.setattr(defaults_mod, "settings", fake_settings, raising=True)

        d = load_defaults_from_env()
        assert d.LOG_LEVEL == "WARNING"
        assert d.MAX_MEMORY_CHUNK_SIZE == 12345
        assert d.MAX_OVERLAPPING_FILES == 7
        assert d.MAX_TOMBSTONE_ROWS == 999
        assert d.DEFAULT_TIMEOUT_SEC == 11
        assert d.DEFAULT_LOCK_DURATION_SEC == 22
        assert d.IS_SHOW_TIMING is False
        assert d.STORAGE_TYPE == "S3"

    def test_invalid_log_level_falls_back_to_info(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        fake_settings = SimpleNamespace(
            SUPERTABLE_LOG_LEVEL="LOUDEST",
            MAX_MEMORY_CHUNK_SIZE=1,
            MAX_OVERLAPPING_FILES=1,
            MAX_TOMBSTONE_ROWS=1,
            DEFAULT_TIMEOUT_SEC=1,
            DEFAULT_LOCK_DURATION_SEC=1,
            IS_SHOW_TIMING=False,
            STORAGE_TYPE="LOCAL",
        )
        monkeypatch.setattr(defaults_mod, "settings", fake_settings, raising=True)

        d = load_defaults_from_env()
        assert d.LOG_LEVEL == "INFO"

    @pytest.mark.parametrize(
        "level", ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    )
    def test_all_documented_log_levels_accepted(
        self, monkeypatch: pytest.MonkeyPatch, level: str
    ) -> None:
        fake_settings = SimpleNamespace(
            SUPERTABLE_LOG_LEVEL=level,
            MAX_MEMORY_CHUNK_SIZE=1,
            MAX_OVERLAPPING_FILES=1,
            MAX_TOMBSTONE_ROWS=1,
            DEFAULT_TIMEOUT_SEC=1,
            DEFAULT_LOCK_DURATION_SEC=1,
            IS_SHOW_TIMING=False,
            STORAGE_TYPE="LOCAL",
        )
        monkeypatch.setattr(defaults_mod, "settings", fake_settings, raising=True)

        original = logging.getLogger().level
        try:
            d = load_defaults_from_env()
            assert d.LOG_LEVEL == level
            assert logging.getLogger().level == getattr(logging, level)
        finally:
            logging.getLogger().setLevel(original)


class TestModuleLogger:
    def test_logger_is_configured_with_a_streamhandler(self) -> None:
        # The module configures the root logger at import time. We don't
        # care which exact handler subclass — just that something is wired
        # up so logging calls don't drop on the floor.
        root = logging.getLogger()
        assert root.handlers, "Root logger must have at least one handler"
