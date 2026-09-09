"""Exercise direct-script startup before immutable SDK settings are imported."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


_RUN_SCRIPT = Path(__file__).resolve().parents[1] / "demo/medcenter/run.py"
_STARTUP_PROBE = """
import contextlib
import io
import json
import os
import runpy
import sys

script, run_name = sys.argv[1:]
sys.argv = [script, '--help']
assert 'supertable.config.settings' not in sys.modules
with contextlib.redirect_stdout(io.StringIO()):
    try:
        runpy.run_path(script, run_name=run_name)
    except SystemExit as exc:
        assert exc.code == 0
from supertable.config.settings import settings
print(json.dumps({
    'dotenv': os.environ.get('SUPERTABLE_DOTENV_PATH'),
    'organization': settings.SUPERTABLE_ORGANIZATION,
    'password': settings.SUPERTABLE_REDIS_PASSWORD,
}))
"""


def _probe(tmp_path, *, explicit_path=None, exported_password=None,
           run_name="__main__", checkout_marker=True, checkout_env=True):
    checkout = tmp_path / "checkout"
    script = checkout / "supertable/demo/medcenter/run.py"
    script.parent.mkdir(parents=True)
    script.write_text(_RUN_SCRIPT.read_text())
    if checkout_marker:
        (checkout / "pyproject.toml").write_text('[project]\nname="supertable"\n')
    if checkout_env:
        (checkout / ".env").write_text(
            "SUPERTABLE_ORGANIZATION=checkout-org\n"
            "SUPERTABLE_REDIS_PASSWORD=checkout-password\n"
        )

    # An IDE may launch from the script directory, or somewhere unrelated.
    # Neither that directory nor its parents may choose the credentials.
    working = tmp_path / "unrelated" / "working"
    working.mkdir(parents=True)
    for directory in (working, working.parent):
        (directory / ".env").write_text(
            "SUPERTABLE_ORGANIZATION=unrelated-org\n"
            "SUPERTABLE_REDIS_PASSWORD=unrelated-password\n"
        )
    env = {
        key: value for key, value in os.environ.items()
        if not key.startswith(("SUPERTABLE_", "STORAGE_"))
    }
    env["SUPERTABLE_HOME"] = str(tmp_path / "runtime")
    if explicit_path is not None:
        env["SUPERTABLE_DOTENV_PATH"] = explicit_path
    if exported_password is not None:
        env["SUPERTABLE_REDIS_PASSWORD"] = exported_password
    result = subprocess.run(
        [sys.executable, "-c", _STARTUP_PROBE, str(script), run_name],
        cwd=working, env=env, capture_output=True, text=True,
        check=True, timeout=30,
    )
    return json.loads(result.stdout)


def test_direct_script_loads_checkout_env_before_sdk_settings(tmp_path):
    assert _probe(tmp_path) == {
        "dotenv": str(tmp_path / "checkout/.env"),
        "organization": "checkout-org",
        "password": "checkout-password",
    }


def test_direct_script_preserves_exported_credentials(tmp_path):
    result = _probe(tmp_path, exported_password="exported-password")
    assert result["organization"] == "checkout-org"
    assert result["password"] == "exported-password"


def test_direct_script_honors_explicit_dotenv(tmp_path):
    chosen = tmp_path / "chosen.env"
    chosen.write_text(
        "SUPERTABLE_ORGANIZATION=explicit-org\n"
        "SUPERTABLE_REDIS_PASSWORD=explicit-password\n"
    )
    assert _probe(tmp_path, explicit_path=str(chosen)) == {
        "dotenv": str(chosen),
        "organization": "explicit-org",
        "password": "explicit-password",
    }


@pytest.mark.parametrize("explicit_path", ["", "/missing-medcenter-config.env"])
def test_direct_script_does_not_replace_explicit_dotenv_setting(
    tmp_path, explicit_path,
):
    assert _probe(tmp_path, explicit_path=explicit_path) == {
        "dotenv": explicit_path,
        "organization": "",
        "password": "",
    }


@pytest.mark.parametrize(
    "options",
    [
        {"run_name": "imported_demo"},
        {"checkout_marker": False},
        {"checkout_env": False},
    ],
)
def test_import_or_absent_checkout_does_not_discover_dotenv(tmp_path, options):
    assert _probe(tmp_path, **options) == {
        "dotenv": None,
        "organization": "",
        "password": "",
    }
