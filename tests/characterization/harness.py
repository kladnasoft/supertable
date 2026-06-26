"""Hermetic test harness for SuperTable characterization tests.

This module deliberately imports **no** ``supertable`` package at module top so
that :func:`bootstrap_hermetic_env` can run *before* any ``supertable`` import.
The production ``supertable.config.settings`` module calls
``load_dotenv(find_dotenv(usecwd=True))`` at import time, which would otherwise
pull the developer's real ``.env`` (MinIO endpoint, Sentinel Redis, presign,
``STORAGE_TYPE=MINIO``) into the test process.  We neutralise python-dotenv and
pin a fully explicit, local, hermetic environment instead.

The other job of this module is to swap the single Redis client factory
(``supertable.redis_connector.create_redis_client``) for a process-local
``fakeredis`` instance so the genuine ``RedisCatalog`` (Lua scripts included)
runs without a live Redis server.  ``fakeredis`` executes the catalog's Lua via
``lupa``; both are required test dependencies.
"""

from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path
from typing import Optional

# Repo root = .../supertable  (this file is tests/characterization/harness.py)
REPO_ROOT = Path(__file__).resolve().parents[2]

# Fixed, deterministic clock anchor for all hand-authored fixtures.  Never
# generated from the wall clock so golden bytes are reproducible.
FIXED_NOW_MS = 1_700_000_000_000  # 2023-11-14T22:13:20Z, arbitrary but frozen

_ENV_BOOTSTRAPPED = False


def bootstrap_hermetic_env(home: Optional[str] = None) -> str:
    """Pin a hermetic environment.  MUST run before importing ``supertable``.

    Idempotent: the first call wins (so a test-session temp home is stable).
    Returns the resolved ``SUPERTABLE_HOME``.
    """
    global _ENV_BOOTSTRAPPED

    # 1) Make the local dev tree win over any installed ``supertable`` wheel.
    repo = str(REPO_ROOT)
    if sys.path and sys.path[0] != repo:
        # remove if present elsewhere, then prepend
        sys.path[:] = [p for p in sys.path if p != repo]
        sys.path.insert(0, repo)

    # 2) Neutralise python-dotenv so the repo .env never leaks deployment config.
    #    settings.py does ``from dotenv import load_dotenv, find_dotenv`` at import
    #    time, so patching the dotenv module here (before that import) is enough.
    try:
        import dotenv

        dotenv.find_dotenv = lambda *a, **k: ""  # type: ignore[assignment]
        dotenv.load_dotenv = lambda *a, **k: False  # type: ignore[assignment]
    except Exception:
        pass

    if _ENV_BOOTSTRAPPED:
        return os.environ["SUPERTABLE_HOME"]

    if home is None:
        home = tempfile.mkdtemp(prefix="st_char_home_")
    os.makedirs(home, exist_ok=True)

    # 3) Fully explicit, local, hermetic settings.  ``override=False`` in the
    #    (now neutralised) dotenv loader means these win regardless.
    hermetic = {
        "SUPERTABLE_HOME": home,
        "STORAGE_TYPE": "LOCAL",
        "STORAGE_ENDPOINT_URL": "",        # avoid S3 endpoint detection path
        "STORAGE_BUCKET": "supertable",
        "SUPERTABLE_DUCKDB_PRESIGNED": "0",  # LocalStorage has no presign()
        "SUPERTABLE_REDIS_SENTINEL": "false",
        "SUPERTABLE_REDIS_SENTINELS": "",
        "SUPERTABLE_REDIS_HOST": "localhost",
        "SUPERTABLE_REDIS_DB": "0",
        "SUPERTABLE_ORGANIZATION": "",
        "SUPERTABLE_MONITORING_ENABLED": "false",  # no background dequeue threads
        "SUPERTABLE_LOG_LEVEL": "WARNING",
        "LOCKING_BACKEND": "redis",
    }
    for k, v in hermetic.items():
        os.environ[k] = v

    _ENV_BOOTSTRAPPED = True
    return home


def require_lua_redis() -> None:
    """Fail loudly (not silently skip) if the Lua-capable fake Redis stack is
    missing.  The whole characterization approach depends on running the real
    catalog Lua scripts, so an absent dependency is a setup error worth surfacing.
    """
    try:
        import fakeredis  # noqa: F401
    except Exception as e:  # pragma: no cover - environment guard
        raise RuntimeError(
            "fakeredis is required for SuperTable characterization tests "
            "(`pip install fakeredis`)"
        ) from e
    try:
        import lupa  # noqa: F401
    except Exception as e:  # pragma: no cover - environment guard
        raise RuntimeError(
            "lupa is required so fakeredis can execute the catalog's Lua scripts "
            "(`pip install lupa`)"
        ) from e


def new_fake_redis():
    """Return a fresh, process-local fake Redis with decode_responses=True."""
    require_lua_redis()
    import fakeredis

    return fakeredis.FakeStrictRedis(decode_responses=True)


def reset_engine_singletons() -> None:
    """Drop cached engine/connection singletons so each test starts clean.

    The Lite executor keeps a persistent DuckDB connection *per Executor
    instance* (created fresh per request), but the Pro executor and the Redis
    client factory keep process-level singletons that must be reset between
    tests pointed at different fake-Redis instances.
    """
    # Redis client cache (keyed by options; our patch bypasses it, but clear
    # anyway so a stale real client can never be returned).
    try:
        import supertable.redis_connector as rc

        rc._CLIENT_CACHE.clear()
    except Exception:
        pass

    # DuckDB Pro persistent connection singleton.
    try:
        import supertable.engine.executor as ex

        if getattr(ex, "_pro_singleton", None) is not None:
            try:
                ex._pro_singleton._con and ex._pro_singleton._con.close()
            except Exception:
                pass
            ex._pro_singleton = None
    except Exception:
        pass


def install_fake_redis():
    """Patch the single Redis client factory to a fresh fake instance and reset
    engine singletons.  Returns the fake client (shared for the whole process
    until the next call).
    """
    fake = new_fake_redis()

    import supertable.redis_connector as rc

    rc._CLIENT_CACHE.clear()
    rc.create_redis_client = lambda options=None: fake  # type: ignore[assignment]

    reset_engine_singletons()
    return fake
