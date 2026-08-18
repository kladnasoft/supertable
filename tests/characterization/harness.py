"""Hermetic test harness for SuperTable characterization tests.

This module deliberately imports **no** ``supertable`` package at module top so
that :func:`bootstrap_hermetic_env` normally runs *before* any ``supertable``
import.  The production ``supertable.config.settings`` module calls
``load_dotenv(find_dotenv(usecwd=True))`` at import time, which would otherwise
pull the developer's real ``.env`` (MinIO endpoint, Sentinel Redis, presign,
``STORAGE_TYPE=MINIO``) into the test process.  We neutralise python-dotenv and
pin a fully explicit, local, hermetic environment instead.  A defensive refresh
also covers whole-repository pytest collection, where a sibling test tree can
import the frozen settings singleton before pytest reaches ``tests/conftest.py``.

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
import hashlib
import json
from pathlib import Path
from typing import Optional

# Repo root = .../supertable  (this file is tests/characterization/harness.py)
REPO_ROOT = Path(__file__).resolve().parents[2]

# Fixed, deterministic clock anchor for all hand-authored fixtures.  Never
# generated from the wall clock so golden bytes are reproducible.
FIXED_NOW_MS = 1_700_000_000_000  # 2023-11-14T22:13:20Z, arbitrary but frozen

_ENV_BOOTSTRAPPED = False


def _refresh_loaded_supertable_configuration() -> None:
    """Rebuild an already-imported settings singleton from the pinned env.

    ``Settings`` is deliberately frozen for production.  During a whole-tree
    pytest collection, however, tests under ``supertable/`` may import it before
    pytest reaches the characterization conftest.  Merely changing
    ``os.environ`` then leaves every ``from ...settings import settings`` binding
    pointed at the developer's deployment configuration.  Refresh those exact
    object bindings here, in the test harness only, and preserve the identity of
    the legacy mutable ``default`` object imported by older modules.
    """
    settings_module = sys.modules.get("supertable.config.settings")
    if settings_module is None:
        return

    old_settings = getattr(settings_module, "settings", None)
    settings_type = getattr(settings_module, "Settings", None)
    build_settings = getattr(settings_module, "_build_settings", None)
    if (
        not isinstance(settings_type, type)
        or not isinstance(old_settings, settings_type)
        or not callable(build_settings)
    ):
        raise RuntimeError("loaded supertable settings module has an invalid shape")

    fresh_settings = build_settings()
    settings_module.settings = fresh_settings

    # Production modules import the singleton under several aliases
    # (``settings``, ``_settings``, ``_cfg``).  Identity-based replacement is
    # intentionally narrower than matching attribute names or values.
    for module_name, module in tuple(sys.modules.items()):
        if module is None or not (
            module_name == "supertable" or module_name.startswith("supertable.")
        ):
            continue
        namespace = getattr(module, "__dict__", None)
        if not isinstance(namespace, dict):
            continue
        for attribute, value in tuple(namespace.items()):
            if value is old_settings:
                namespace[attribute] = fresh_settings

    defaults_module = sys.modules.get("supertable.config.defaults")
    if defaults_module is not None:
        legacy_default = getattr(defaults_module, "default", None)
        update_default = getattr(legacy_default, "update_default", None)
        if callable(update_default):
            update_default(
                MAX_MEMORY_CHUNK_SIZE=fresh_settings.MAX_MEMORY_CHUNK_SIZE,
                MAX_OVERLAPPING_FILES=fresh_settings.MAX_OVERLAPPING_FILES,
                MAX_TOMBSTONE_ROWS=fresh_settings.MAX_TOMBSTONE_ROWS,
                TOMBSTONE_COMPACTION_WORKERS=fresh_settings.TOMBSTONE_COMPACTION_WORKERS,
                DEFAULT_TIMEOUT_SEC=fresh_settings.DEFAULT_TIMEOUT_SEC,
                DEFAULT_LOCK_DURATION_SEC=fresh_settings.DEFAULT_LOCK_DURATION_SEC,
                LOG_LEVEL=fresh_settings.SUPERTABLE_LOG_LEVEL,
                IS_SHOW_TIMING=fresh_settings.IS_SHOW_TIMING,
                STORAGE_TYPE=fresh_settings.STORAGE_TYPE,
            )

    # ``homedir`` caches the resolved path separately from Settings.  It can be
    # populated by an eager sibling import, so invalidate that cache as well.
    homedir_module = sys.modules.get("supertable.config.homedir")
    if homedir_module is not None:
        homedir_module.settings = fresh_settings
        homedir_module._resolved_home = None

    # Processing lazily caches a storage instance.  Collection should not
    # create one, but clearing it prevents a side-effectful sibling import from
    # retaining an external client after the hermetic settings refresh.
    processing_module = sys.modules.get("supertable.processing")
    if processing_module is not None:
        processing_module._storage = None


def bootstrap_hermetic_env(home: Optional[str] = None) -> str:
    """Pin a hermetic environment before normal ``supertable`` use.

    Idempotent: the first call wins (so a test-session temp home is stable).
    If whole-tree collection imported ``supertable`` first, refresh its frozen
    configuration bindings after pinning the environment.
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
        "STORAGE_REGION": "us-east-1",
        "STORAGE_ACCESS_KEY": "",
        "STORAGE_SECRET_KEY": "",
        "STORAGE_SESSION_TOKEN": "",
        "STORAGE_FORCE_PATH_STYLE": "true",
        "STORAGE_USE_SSL": "false",
        "SUPERTABLE_DUCKDB_PRESIGNED": "0",  # LocalStorage has no presign()
        "SUPERTABLE_DUCKDB_USE_HTTPFS": "0",
        "SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD": "0",
        "SUPERTABLE_REDIS_URL": "",
        "SUPERTABLE_REDIS_SENTINEL": "false",
        "SUPERTABLE_REDIS_SENTINELS": "",
        "SUPERTABLE_REDIS_HOST": "localhost",
        "SUPERTABLE_REDIS_PORT": "6379",
        "SUPERTABLE_REDIS_DB": "0",
        "SUPERTABLE_REDIS_PASSWORD": "",
        "SUPERTABLE_REDIS_USERNAME": "",
        "SUPERTABLE_REDIS_SSL": "false",
        "SUPERTABLE_ORGANIZATION": "",
        "SUPERTABLE_MONITORING_ENABLED": "false",  # no background dequeue threads
        "SUPERTABLE_MONITOR_SPOOL_MAX_BYTES": str(256 * 1024 * 1024),
        "SUPERTABLE_MONITOR_SPOOL_MAX_RECORDS": "100000",
        "SUPERTABLE_LOG_LEVEL": "WARNING",
        "LOCKING_BACKEND": "redis",
    }
    for k, v in hermetic.items():
        os.environ[k] = v

    _refresh_loaded_supertable_configuration()
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


def install_privileged_activation(redis_client, organization: str) -> None:
    """Install the canonical privileged-audit activation for a test estate.

    Characterization tests intentionally run the real RBAC mutation scripts.
    Those scripts now fail closed until the deployment's existing estate has a
    pinned activation baseline.  Build and verify that baseline with the same
    production helpers used by the worker, then let the production attestation
    Lua install the immutable anchor.  This is test infrastructure, not a
    direct Redis-key bypass.

    The operation is idempotent for a Redis instance that is already anchored:
    the existing anchor is reconstructed as a report and re-attested through
    the production comparison path.
    """
    if not isinstance(organization, str) or not organization:
        raise ValueError("organization is required")

    from supertable import redis_keys as RK
    from supertable.audit import get_privileged_audit_outbox
    from supertable.audit.privileged_worker import (
        ActivationBaselineReport,
        attest_activation_baseline,
        compute_privileged_state_sha256,
        verify_activation_baseline,
    )

    outbox = get_privileged_audit_outbox(
        organization,
        redis_client=redis_client,
    )
    existing = redis_client.get(RK.audit_privileged_activation(organization))
    if existing is not None:
        document = json.loads(existing)
        report = ActivationBaselineReport(
            organization=document["organization"],
            activation_id=document["activation_id"],
            created_ms=document["created_ms"],
            state_sha256=document["state_sha256"],
            artifact_sha256=document["artifact_sha256"],
        )
        attest_activation_baseline(outbox, report)
        return

    state_sha256 = compute_privileged_state_sha256(outbox, organization)
    document = {
        "version": 1,
        "kind": "supertable_privileged_activation_baseline",
        "organization": organization,
        "activation_id": "characterization-" + hashlib.sha256(
            organization.encode("utf-8")
        ).hexdigest()[:32],
        "created_ms": FIXED_NOW_MS,
        "state_sha256": state_sha256,
    }
    payload = json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    pin = hashlib.sha256(payload).hexdigest()
    descriptor, baseline_path = tempfile.mkstemp(
        prefix="supertable-characterization-activation-",
        suffix=".json",
    )
    try:
        with os.fdopen(descriptor, "wb") as baseline_file:
            baseline_file.write(payload)
            baseline_file.flush()
            os.fsync(baseline_file.fileno())
        report = verify_activation_baseline(
            baseline_path,
            expected_sha256=pin,
            organization=organization,
        )
        attest_activation_baseline(outbox, report)
    finally:
        try:
            os.unlink(baseline_path)
        except FileNotFoundError:
            pass


def reset_engine_singletons() -> None:
    """Drop cached engine/connection singletons so each test starts clean.

    Lite and DuckDB keep organization/storage-scoped persistent connections, and
    the Redis client factory is process-global.  All must be reset between
    tests pointed at different fake-Redis instances and storage homes.
    """
    # Redis client cache (keyed by options; our patch bypasses it, but clear
    # anyway so a stale real client can never be returned).
    try:
        import supertable.redis_connector as rc

        rc._CLIENT_CACHE.clear()
    except Exception:
        pass

    # DuckDB persistent connection singletons.  Both engines are scoped and
    # shared across per-request Executor instances; characterization swaps the
    # entire Redis catalog and storage home per test, so no connection/cache
    # may survive that boundary.
    try:
        import supertable.engine.executor as ex

        engines = {
            *list(getattr(ex, "_duckdb_singletons", {}).values()),
            *list(getattr(ex, "_lite_singletons", {}).values()),
        }
        for engine in engines:
            try:
                if hasattr(engine, "drop_all"):
                    engine.drop_all()
                else:
                    engine._reset_connection()
            except Exception:
                pass
        getattr(ex, "_duckdb_singletons", {}).clear()
        getattr(ex, "_lite_singletons", {}).clear()
        ex._duckdb_singleton = None
        ex._lite_singleton = None
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
