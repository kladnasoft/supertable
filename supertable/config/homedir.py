import os
import sys
import tempfile

from supertable.config.settings import settings
from supertable.config.defaults import logger

# If this file is located in a subdirectory, adjust the path logic as needed.
# Currently appending ".." from __file__ to add the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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

    raw = settings.SUPERTABLE_HOME
    expanded = os.path.abspath(os.path.expanduser(raw))

    if _is_writable_dir(expanded):
        logger.debug(f"Ensured app home directory exists: {expanded}")
        _resolved_home = expanded
        return _resolved_home

    fallback = os.path.join(tempfile.gettempdir(), "supertable")
    if _is_writable_dir(fallback):
        logger.warning(
            f"SUPERTABLE_HOME={expanded!r} is not writable; falling back to "
            f"{fallback!r}. Set SUPERTABLE_HOME to a writable directory to "
            f"silence this — DuckDB temp/spill, cache and extensions live under it."
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
        logger.debug(f"Changed working directory to {expanded_dir}")
    except Exception as e:
        logger.error(f"Failed to change working directory to {expanded_dir}: {e}")

# ---------- eager init (preserves original import-time behaviour) ----------
_app_home = _resolve_app_home()
change_to_app_home(_app_home)
logger.debug(f"Current working directory: {os.getcwd()}")

# ---------- public API ----------

# Kept for backward compatibility; prefer get_app_home() for the expanded path.
app_home = _app_home

def get_app_home() -> str:
    """Return the fully expanded, absolute application home directory."""
    return _resolve_app_home()