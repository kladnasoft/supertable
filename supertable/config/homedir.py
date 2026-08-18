"""Explicit application-home initialisation.

Importing :mod:`supertable` (or this module) must not create directories,
probe the filesystem, mutate ``sys.path``, or change the process working
directory.  Call :func:`initialize_app_home` at an application boundary when
the writable runtime directories are actually needed.  The historical
``app_home`` attribute remains available through a lazy module attribute: it
resolves the home only when explicitly accessed and never changes the CWD.
"""

from __future__ import annotations

import logging
import os
import tempfile
from typing import Any


# Test/application override hook.  Keeping this sentinel does not load the
# settings module; callers that need to inject settings may assign an object
# with ``SUPERTABLE_HOME`` before initialisation.
settings: Any = None

# ---------- lazy home directory resolution ----------
_resolved_home: str | None = None

def _is_writable_dir(path: str) -> bool:
    """Create *path* if needed and verify we can actually write a file in it.

    ``os.access(..., W_OK)`` is unreliable under containers, ACLs and
    root-squashed mounts, so probe with a real create+unlink: this is the
    difference between a home that merely *resolves* and one DuckDB can root
    its temp/spill, cache and extension dirs under.
    """
    try:
        os.makedirs(path, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=path):
            pass
        return True
    except OSError:
        return False

def _resolve_app_home() -> str:
    """
    Resolve, expand, and normalise the application home directory once.

    The home must be *writable*, not just resolvable: DuckDB roots its
    temp/spill, external-cache and extension directories here, so a
    non-writable home silently breaks every write (the probe fails with
    ``errno 13`` and falls back to the slow full-read path).  We therefore
    verify writability and, when the configured home is not usable, fall back
    to ``<tempdir>/supertable`` with a loud warning rather than returning a
    path that only looks valid.
    """
    global _resolved_home
    if _resolved_home is not None:
        return _resolved_home

    # Keep settings (and its dotenv handling) out of the import path.  Runtime
    # configuration is loaded only when a caller explicitly asks for a home.
    runtime_settings = settings
    if runtime_settings is None:
        from supertable.config.settings import settings as loaded_settings

        runtime_settings = loaded_settings

    raw = runtime_settings.SUPERTABLE_HOME
    expanded = os.path.abspath(os.path.expanduser(raw))

    if _is_writable_dir(expanded):
        logging.getLogger(__name__).debug(
            "Ensured app home directory exists: %s", expanded
        )
        _resolved_home = expanded
        return _resolved_home

    fallback = os.path.join(tempfile.gettempdir(), "supertable")
    if _is_writable_dir(fallback):
        logging.getLogger(__name__).warning(
            "SUPERTABLE_HOME=%r is not writable; falling back to %r. Set "
            "SUPERTABLE_HOME to a writable directory to silence this — "
            "DuckDB temp/spill, cache and extensions live under it.",
            expanded,
            fallback,
        )
        _resolved_home = fallback
        return _resolved_home

    raise RuntimeError(
        f"No writable application home: tried SUPERTABLE_HOME={expanded!r} and "
        f"fallback {fallback!r}. Set SUPERTABLE_HOME to a writable directory."
    )

def change_to_app_home(home_dir: str | None = None) -> None:
    """
    Attempts to change the current working directory to `home_dir`.
    If home_dir is not provided, uses the resolved app home.
    Logs the outcome.
    """
    target = home_dir if home_dir else _resolve_app_home()
    expanded_dir = os.path.expanduser(target)
    try:
        os.chdir(expanded_dir)
        logging.getLogger(__name__).debug(
            "Changed working directory to %s", expanded_dir
        )
    except Exception as e:
        logging.getLogger(__name__).error(
            "Failed to change working directory to %s: %s", expanded_dir, e
        )


def initialize_app_home(*, change_cwd: bool = False) -> str:
    """Create/probe the configured runtime home and optionally enter it.

    ``change_cwd`` defaults to ``False`` because libraries must use absolute
    paths and must not silently change process-global state.  The opt-in flag
    remains for command-line applications that deliberately want the legacy
    working-directory behaviour.
    """
    resolved = _resolve_app_home()
    if change_cwd:
        change_to_app_home(resolved)
    return resolved


def get_app_home() -> str:
    """Return an initialised, writable, absolute application-home path."""
    return initialize_app_home()


def __getattr__(name: str) -> Any:
    """Resolve the legacy ``app_home`` attribute on explicit access only."""
    if name == "app_home":
        return get_app_home()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Advertise the lazy compatibility attribute to introspection tools."""
    return sorted(set(globals()) | {"app_home"})
