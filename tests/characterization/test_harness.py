"""Regression tests for the characterization test-session bootstrap."""

from __future__ import annotations

import os
import subprocess
import sys

import pytest

from tests.characterization.harness import REPO_ROOT


def test_late_bootstrap_rebinds_frozen_settings_to_local_storage(tmp_path):
    """Whole-tree collection must not retain an earlier external backend.

    Run in a fresh interpreter because this test session was correctly
    bootstrapped before collection.  The child deliberately imports the frozen
    production settings and resolves the old home first, exactly reproducing
    the ordering that occurs when pytest collects a sibling test tree before
    reaching ``tests/conftest.py``.
    """
    external_home = tmp_path / "external-home"
    hermetic_home = tmp_path / "hermetic-home"
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": os.pathsep.join(
                filter(None, (str(REPO_ROOT), env.get("PYTHONPATH", "")))
            ),
            "STORAGE_TYPE": "MINIO",
            "STORAGE_ENDPOINT_URL": "http://192.168.168.130:9000",
            "STORAGE_BUCKET": "external",
            "SUPERTABLE_HOME": str(external_home),
            "HERMETIC_TEST_HOME": str(hermetic_home),
        }
    )

    code = """
import os

import supertable.config.settings as config_module
from supertable.config import defaults, homedir
from supertable.storage import storage_factory

old_settings = config_module.settings
assert old_settings.STORAGE_TYPE == "MINIO"
assert storage_factory.settings is old_settings
assert defaults.default.STORAGE_TYPE == "MINIO"
assert homedir.get_app_home() == os.environ["SUPERTABLE_HOME"]

from tests.characterization.harness import bootstrap_hermetic_env

expected_home = os.environ["HERMETIC_TEST_HOME"]
bootstrap_hermetic_env(expected_home)

assert config_module.settings is not old_settings
assert config_module.settings.STORAGE_TYPE == "LOCAL"
assert storage_factory.settings is config_module.settings
assert defaults.settings is config_module.settings
assert defaults.default.STORAGE_TYPE == "LOCAL"
storage = storage_factory.get_storage()
assert type(storage).__name__ == "LocalStorage"
assert storage.root == expected_home
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert result.returncode == 0, (
        f"late bootstrap child failed\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_fakeredis_teardown_releases_local_storage_descriptors():
    """Repeated characterization fixtures must fit a modest descriptor limit."""
    if os.name != "posix":
        pytest.skip("RLIMIT_NOFILE is a POSIX descriptor-limit regression")
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        filter(None, (str(REPO_ROOT), env.get("PYTHONPATH", "")))
    )
    # Keep developer/CI pytest defaults from changing the focused child run.
    env.pop("PYTEST_ADDOPTS", None)
    env.pop("PYTEST_PLUGINS", None)

    code = """
import resource
import pytest

_soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
if hard != resource.RLIM_INFINITY and hard < 64:
    raise SystemExit(77)
resource.setrlimit(resource.RLIMIT_NOFILE, (64, hard))
raise SystemExit(pytest.main([
    "-q",
    "-p", "no:cacheprovider",
    "--keep-duplicates",
    "tests/characterization/test_quality_nonfinite_numeric.py",
    "tests/characterization/test_quality_nonfinite_numeric.py",
]))
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=REPO_ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    if result.returncode == 77:
        pytest.skip("hard RLIMIT_NOFILE is below the regression limit")
    assert result.returncode == 0, (
        "bounded-descriptor characterization child failed\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
