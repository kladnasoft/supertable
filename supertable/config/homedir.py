import os
import sys

from supertable.config.defaults import default, logger

# If this file is located in a subdirectory, adjust the path logic as needed.
# Currently appending ".." from __file__ to add the project root directory
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# ---------- lazy home directory resolution ----------
_resolved_home: str | None = None

def _resolve_app_home() -> str:
    """
    Resolve, expand, and normalise the application home directory once.
    Creates the directory if it does not exist.
    """
    global _resolved_home
    if _resolved_home is not None:
        return _resolved_home

    raw = os.getenv("SUPERTABLE_HOME", "~/supertable")
    expanded = os.path.expanduser(raw)
    expanded = os.path.abspath(expanded)

    try:
        os.makedirs(expanded, exist_ok=True)
        logger.debug(f"Ensured app home directory exists: {expanded}")
    except OSError as e:
        logger.error(f"Failed to create app home directory {expanded}: {e}")

    _resolved_home = expanded
    return _resolved_home

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
app_home = os.getenv("SUPERTABLE_HOME", "~/supertable")

def get_app_home() -> str:
    """Return the fully expanded, absolute application home directory."""
    return _resolve_app_home()