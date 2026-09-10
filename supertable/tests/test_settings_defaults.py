# route: supertable.tests.test_settings_defaults
"""The declared default of a setting must be the default you actually get.

``Settings`` is a frozen dataclass whose field defaults are inert: every field
is passed explicitly by ``_build_settings()``, so a declaration that disagrees
with the builder is documentation that lies — and worse, it hides real bugs.

That is exactly what happened with ``SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE``:
declared ``"5GB"``, but the builder passed no default, so an unset env var
yielded ``""`` — and ``configure_httpfs_and_s3`` reads that as "no cache
configured" and *explicitly* sets ``enable_external_file_cache=false``,
overriding DuckDB's own default of ``true``. Any deployment that did not set
the variable silently lost a cache it would otherwise have had.

The comparison runs in a subprocess with a working directory that has no
``.env`` anywhere above it. ``settings`` calls ``find_dotenv(usecwd=True)`` at
import, so running in-process from the repo would compare against the repo's
``.env`` rather than against the declared defaults.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]

# Fields whose builder default is deliberately COMPUTED rather than copied from
# the declaration.  Each needs a reason; anything else showing up here is a bug.
_INTENTIONAL = {
    # `_env_str(...) or "0.0.0.0"` — bind-all fallback for an unset host.
    "SUPERTABLE_API_HOST",
    # Derived from the user's home (`~/.config`), which cannot be a literal.
    "XDG_CONFIG_HOME",
}

_PROBE = """
import dataclasses, json
import supertable.config.settings as S
built = S._build_settings()
print("@@" + json.dumps([
    [f.name, repr(f.default), repr(getattr(built, f.name))]
    for f in dataclasses.fields(S.Settings)
    if f.default is not dataclasses.MISSING
    and getattr(built, f.name) != f.default
]))
"""


def _divergences() -> list[list[str]]:
    with tempfile.TemporaryDirectory(dir="/tmp") as cwd:
        proc = subprocess.run(
            [sys.executable, "-c", _PROBE],
            cwd=cwd,                                  # no .env above this
            env={"PATH": "/usr/bin:/bin", "HOME": cwd,
                 "PYTHONPATH": str(_REPO_ROOT)},
            capture_output=True, text=True, timeout=180,
        )
    if proc.returncode != 0:
        pytest.fail(f"settings probe failed:\n{proc.stderr[-2000:]}")
    line = next((l for l in proc.stdout.splitlines() if l.startswith("@@")), None)
    if line is None:
        pytest.fail(f"probe produced no result:\n{proc.stdout[-2000:]}")
    return json.loads(line[2:])


def test_declared_defaults_match_built_defaults():
    """No setting may declare one default and build another."""
    unexpected = [d for d in _divergences() if d[0] not in _INTENTIONAL]
    assert not unexpected, (
        "these settings declare a default the builder does not produce, so the "
        "declaration is misleading and the real default is whatever the builder "
        "passes:\n"
        + "\n".join(f"  {n}: declared={d} built={b}" for n, d, b in unexpected)
    )


def test_intentional_exceptions_are_still_divergent():
    """Guard the guard.

    If a computed default is ever replaced by a literal, its entry in
    ``_INTENTIONAL`` becomes dead and would start masking a genuine
    contradiction on that field.
    """
    diverging = {d[0] for d in _divergences()}
    stale = _INTENTIONAL - diverging
    assert not stale, (
        f"these no longer diverge and should be dropped from _INTENTIONAL: {stale}"
    )


def test_external_file_cache_default_is_not_empty():
    """The specific regression: an empty size force-disables the cache.

    ``configure_httpfs_and_s3`` treats a falsy size as "caching off" and sets
    ``enable_external_file_cache=false``, which is *stronger* than leaving
    DuckDB alone — its own default is ``true``.
    """
    from supertable.config.settings import Settings

    declared = Settings.__dataclass_fields__[
        "SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE"
    ].default
    assert declared, "a falsy default here disables the cache outright"
