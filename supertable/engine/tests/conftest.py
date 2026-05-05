# supertable/engine/tests/conftest.py
"""
Pytest conftest for supertable.engine tests.

Auto-discovered by pytest — no explicit import needed.
Provides shared fixtures and ensures external service dependencies
(Redis, Spark, Hive) are safely mocked for unit testing.
"""

from __future__ import annotations

import logging
import os
import sys
from unittest.mock import MagicMock, patch

import duckdb
import pytest

# Pre-import the engine submodules so they live in sys.modules. We then
# restore `supertable.engine` to the real subpackage, because
# supertable/__init__.py re-exports the lowercase name `engine` (an Engine
# Enum alias from data_reader) which shadows the `supertable.engine`
# subpackage attribute on the `supertable` module. That shadow makes every
# string-path patch like `patch("supertable.engine.data_estimator.RedisCatalog")`
# fail with `AttributeError: data_estimator` from `enum.__getattr__`.
import supertable  # noqa: F401  (ensure top-level package is imported)
import supertable.engine  # noqa: F401  (the actual subpackage)
import supertable.engine.data_estimator  # noqa: F401  (ensure in sys.modules)
import supertable.engine.duckdb_lite  # noqa: F401  (ensure in sys.modules)
import supertable.engine.duckdb_pro  # noqa: F401  (ensure in sys.modules)
import supertable.engine.engine_common  # noqa: F401  (ensure in sys.modules)
import supertable.engine.spark_thrift  # noqa: F401  (ensure in sys.modules)
import supertable.super_table  # noqa: F401  (ensure in sys.modules)

# Restore `supertable.engine` attribute to the subpackage (overwriting the
# Engine Enum re-export). Tests in this folder do not use
# `from supertable import engine` to access the Enum, but they do rely on
# string-path mock.patch lookups walking through `supertable.engine.<mod>`.
sys.modules["supertable"].engine = sys.modules["supertable.engine"]


@pytest.fixture(autouse=True, scope="session")
def _suppress_test_log_noise():
    """
    Suppress INFO/WARNING log output during tests.

    Engine code emits expected warnings on error-path tests (presign
    fallback, missing columns, connection reset, etc.).  These are
    correct behaviour — but noisy in test output.
    """
    for name in ("supertable", "supertable.engine"):
        logging.getLogger(name).setLevel(logging.CRITICAL)
    yield
    for name in ("supertable", "supertable.engine"):
        logging.getLogger(name).setLevel(logging.NOTSET)


@pytest.fixture(autouse=True)
def _mock_redis_catalog():
    """
    Prevent RedisCatalog from being instantiated in engine modules.

    Both DataEstimator and SparkThriftExecutor create a RedisCatalog()
    in __init__, which attempts to connect to Redis.  SuperTable also
    creates one during __init__ and calls get_storage().  Mock everything.
    """
    de_mod = sys.modules["supertable.engine.data_estimator"]
    st_mod = sys.modules["supertable.engine.spark_thrift"]
    su_mod = sys.modules["supertable.super_table"]
    with (
        patch.object(de_mod, "RedisCatalog") as MockDE,
        patch.object(st_mod, "RedisCatalog") as MockST,
        patch.object(su_mod, "RedisCatalog") as MockSuper,
        patch.object(su_mod, "get_storage") as MockStorage,
        patch.object(su_mod, "RoleManager"),
        patch.object(su_mod, "UserManager"),
    ):
        MockDE.return_value = MagicMock()
        MockST.return_value = MagicMock()
        MockStorage.return_value = MagicMock()
        # SuperTable needs root_exists to return True (fast-path, skip storage mkdirs)
        mock_catalog_inst = MagicMock()
        mock_catalog_inst.root_exists.return_value = True
        MockSuper.return_value = mock_catalog_inst
        yield


@pytest.fixture()
def duckdb_con():
    """Provide a real in-memory DuckDB connection, closed after each test."""
    con = duckdb.connect()
    yield con
    try:
        con.close()
    except Exception:
        pass


@pytest.fixture()
def mock_storage():
    """Generic mock storage object with presign support."""
    storage = MagicMock()
    storage.presign.side_effect = lambda key: f"https://presigned/{key}"
    return storage


@pytest.fixture()
def clean_env(monkeypatch):
    """Remove all STORAGE_* and SUPERTABLE_* env vars for a clean slate."""
    for key in list(os.environ):
        if key.startswith(("STORAGE_", "SUPERTABLE_")):
            monkeypatch.delenv(key, raising=False)
