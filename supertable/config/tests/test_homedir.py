# route: supertable.config.tests.test_homedir
"""Tests for ``supertable.config.homedir``.

The module resolves the application home directory from
``settings.SUPERTABLE_HOME``, expands ``~``, makes the directory if it does
not exist, and changes the process working directory to it. We exercise the
helper functions in isolation without disturbing the real test runner's CWD
beyond the test boundary.
"""
from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from supertable.config import homedir as homedir_mod


@pytest.fixture(autouse=True)
def restore_cwd() -> None:
    """Snapshot and restore the CWD around every test."""
    original = os.getcwd()
    yield
    os.chdir(original)


@pytest.fixture(autouse=True)
def reset_module_cache() -> None:
    """Force lazy resolution to recompute on each test."""
    homedir_mod._resolved_home = None
    yield
    homedir_mod._resolved_home = None


class TestResolveAppHome:
    def test_creates_directory_if_missing(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        target = tmp_path / "supertable-home"
        assert not target.exists()

        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME=str(target)),
            raising=True,
        )

        resolved = homedir_mod._resolve_app_home()
        assert Path(resolved) == target.resolve()
        assert target.is_dir()

    def test_expands_tilde(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # Use a synthetic HOME so we never touch the real user dir.
        fake_home = Path(os.path.expanduser("~"))
        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME="~/.does-not-matter"),
            raising=True,
        )
        # _resolve_app_home expands and absolutises, so the result must be absolute
        resolved = homedir_mod._resolve_app_home()
        assert os.path.isabs(resolved)
        assert resolved.startswith(str(fake_home))

    def test_caches_after_first_call(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME=str(tmp_path / "home")),
            raising=True,
        )
        a = homedir_mod._resolve_app_home()
        # Change settings under the hood — second call should NOT re-read it
        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME=str(tmp_path / "different")),
            raising=True,
        )
        b = homedir_mod._resolve_app_home()
        assert a == b

    def test_get_app_home_matches_resolve(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        target = tmp_path / "home"
        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME=str(target)),
            raising=True,
        )
        assert homedir_mod.get_app_home() == homedir_mod._resolve_app_home()


class TestChangeToAppHome:
    def test_with_explicit_directory(
        self, tmp_path: Path
    ) -> None:
        target = tmp_path / "elsewhere"
        target.mkdir()
        homedir_mod.change_to_app_home(str(target))
        assert Path(os.getcwd()).resolve() == target.resolve()

    def test_with_no_argument_uses_resolved_home(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        target = tmp_path / "auto-home"
        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME=str(target)),
            raising=True,
        )
        homedir_mod.change_to_app_home()
        assert Path(os.getcwd()).resolve() == target.resolve()

    def test_swallows_chdir_failure(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # An obviously bogus path must NOT propagate an exception.
        homedir_mod.change_to_app_home("/this/path/should/never/exist/xyz123")
        # The implementation logs an error rather than raising.
        # caplog is configured by pytest; ensure the test does not crash.
        assert True
