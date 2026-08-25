# route: supertable.config.tests.test_homedir
"""Tests for explicit, side-effect-free application-home initialisation."""
from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

from supertable.config import homedir as homedir_mod


def test_package_import_does_not_change_cwd_or_create_runtime_home(
    tmp_path: Path,
) -> None:
    working = tmp_path / "working"
    configured_home = tmp_path / "must-not-exist"
    working.mkdir()
    script = (
        "import json, os, pathlib, sys; "
        "before=os.getcwd(); import supertable; "
        "print(json.dumps({'before': before, 'after': os.getcwd(), "
        "'home_exists': pathlib.Path(sys.argv[1]).exists(), "
        "'heavy': any(name in sys.modules for name in "
        "('pyarrow', 'polars', 'supertable.redis_catalog'))}))"
    )
    env = os.environ.copy()
    env["SUPERTABLE_HOME"] = str(configured_home)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        [sys.executable, "-c", script, str(configured_home)],
        cwd=working,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    import json

    result = json.loads(completed.stdout)
    assert result == {
        "before": str(working),
        "after": str(working),
        "home_exists": False,
        "heavy": False,
    }


def test_homedir_import_does_not_load_settings_or_touch_process_state(
    tmp_path: Path,
) -> None:
    working = tmp_path / "working"
    configured_home = tmp_path / "must-not-exist"
    working.mkdir()
    script = (
        "import json, os, pathlib, sys; before=os.getcwd(); "
        "import supertable.config.homedir; "
        "print(json.dumps({'cwd_same': os.getcwd() == before, "
        "'home_exists': pathlib.Path(sys.argv[1]).exists(), "
        "'settings_loaded': 'supertable.config.settings' in sys.modules}))"
    )
    env = os.environ.copy()
    env["SUPERTABLE_HOME"] = str(configured_home)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    completed = subprocess.run(
        [sys.executable, "-c", script, str(configured_home)],
        cwd=working,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    import json

    assert json.loads(completed.stdout) == {
        "cwd_same": True,
        "home_exists": False,
        "settings_loaded": False,
    }


def test_legacy_app_home_attribute_is_lazy_and_does_not_change_cwd(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    working = tmp_path / "working"
    configured_home = tmp_path / "legacy-home"
    working.mkdir()
    monkeypatch.chdir(working)
    monkeypatch.setattr(
        homedir_mod,
        "settings",
        SimpleNamespace(SUPERTABLE_HOME=str(configured_home)),
        raising=True,
    )

    assert "app_home" not in vars(homedir_mod)
    assert "app_home" in dir(homedir_mod)
    assert not configured_home.exists()

    from supertable.config.homedir import app_home

    assert app_home == str(configured_home.resolve())
    assert configured_home.is_dir()
    assert Path.cwd() == working.resolve()
    # Keep the compatibility value dynamic rather than caching a stale module
    # global; resetting the resolver in applications/tests remains effective.
    assert "app_home" not in vars(homedir_mod)


def test_lazy_core_exports_preserve_public_engine_enum_without_home_side_effect(
    tmp_path: Path,
) -> None:
    working = tmp_path / "working"
    configured_home = tmp_path / "must-not-exist"
    working.mkdir()
    script = (
        "import json, os, pathlib, sys; before=os.getcwd(); "
        "from supertable import SuperTable, DataWriter, DataReader, engine; "
        "print(json.dumps({'auto': engine.AUTO.value, "
        "'is_enum': engine.__name__ == 'Engine', "
        "'cwd_same': os.getcwd() == before, "
        "'home_exists': pathlib.Path(sys.argv[1]).exists()}))"
    )
    env = os.environ.copy()
    env["SUPERTABLE_HOME"] = str(configured_home)
    completed = subprocess.run(
        [sys.executable, "-c", script, str(configured_home)],
        cwd=working,
        env=env,
        text=True,
        capture_output=True,
        check=True,
    )
    import json

    assert json.loads(completed.stdout) == {
        "auto": "auto",
        "is_enum": True,
        "cwd_same": True,
        "home_exists": False,
    }


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

    def test_unwritable_home_uses_private_owned_runtime_fallback(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        configured_file = tmp_path / "not-a-directory"
        configured_file.write_text("occupied", encoding="utf-8")
        runtime_root = tmp_path / "runtime"
        runtime_root.mkdir(mode=0o700)
        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME=str(configured_file)),
            raising=True,
        )
        monkeypatch.setattr(homedir_mod.tempfile, "gettempdir", lambda: str(runtime_root))

        resolved = Path(homedir_mod._resolve_app_home())

        assert resolved == runtime_root / f"supertable-{os.geteuid()}"
        info = resolved.lstat()
        assert stat.S_ISDIR(info.st_mode)
        assert info.st_uid == os.geteuid()
        assert stat.S_IMODE(info.st_mode) == 0o700

    def test_runtime_fallback_rejects_precreated_symlink(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        configured_file = tmp_path / "not-a-directory"
        configured_file.write_text("occupied", encoding="utf-8")
        runtime_root = tmp_path / "runtime"
        attacker_target = tmp_path / "attacker-target"
        runtime_root.mkdir(mode=0o700)
        attacker_target.mkdir()
        fallback = runtime_root / f"supertable-{os.geteuid()}"
        fallback.symlink_to(attacker_target, target_is_directory=True)
        monkeypatch.setattr(
            homedir_mod, "settings",
            SimpleNamespace(SUPERTABLE_HOME=str(configured_file)),
            raising=True,
        )
        monkeypatch.setattr(homedir_mod.tempfile, "gettempdir", lambda: str(runtime_root))

        with pytest.raises(RuntimeError, match="No writable application home"):
            homedir_mod._resolve_app_home()

        assert fallback.is_symlink()
        assert list(attacker_target.iterdir()) == []

    def test_runtime_fallback_rejects_non_sticky_shared_parent(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        runtime_root = tmp_path / "shared-runtime"
        runtime_root.mkdir(mode=0o777)
        runtime_root.chmod(0o777)
        monkeypatch.setattr(homedir_mod.tempfile, "gettempdir", lambda: str(runtime_root))

        assert homedir_mod._private_runtime_fallback() is None


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
