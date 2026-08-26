from __future__ import annotations

import os
from pathlib import Path
import shutil
import subprocess
import sys


_ROOT = Path(__file__).resolve().parents[1]


def _run(*args: str, cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def _release_checkout(tmp_path: Path, *, init_version: str = "2.5.3") -> Path:
    checkout = tmp_path / "checkout"
    (checkout / "supertable").mkdir(parents=True)
    for name in ("push-pypi.sh", "pyproject.toml", "setup.py"):
        shutil.copy2(_ROOT / name, checkout / name)
    (checkout / "supertable" / "__init__.py").write_text(
        f'__version__ = "{init_version}"\n',
        encoding="utf-8",
    )
    _run("git", "init", "-q", cwd=checkout)
    _run("git", "config", "user.name", "Release Test", cwd=checkout)
    _run(
        "git", "config", "user.email", "release-test@example.invalid",
        cwd=checkout,
    )
    _run("git", "add", ".", cwd=checkout)
    _run(
        "git", "-c", "commit.gpgsign=false", "commit", "-qm", "release source",
        cwd=checkout,
    )
    return checkout


def _invoke_release_helper(checkout: Path) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["SUPERTABLE_RELEASE_PYTHON"] = sys.executable
    return subprocess.run(
        ("bash", "push-pypi.sh", "--push"),
        cwd=checkout,
        env=environment,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def test_release_helper_accepts_metadata_free_setup_shim(tmp_path: Path) -> None:
    checkout = _release_checkout(tmp_path)

    result = _invoke_release_helper(checkout)

    assert result.returncode == 6
    assert "--push requires the existing signed local tag v2.5.3" in result.stderr
    assert "version mismatch" not in result.stderr
    assert "setup.py" not in result.stderr


def test_release_helper_rejects_init_version_mismatch(tmp_path: Path) -> None:
    checkout = _release_checkout(tmp_path, init_version="9.9.9")

    result = _invoke_release_helper(checkout)

    assert result.returncode == 1
    assert "version mismatch" in result.stderr
    assert "pyproject=2.5.3" in result.stderr
    assert "__init__=['9.9.9']" in result.stderr
