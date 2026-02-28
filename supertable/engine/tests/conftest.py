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
from unittest.mock import MagicMock, patch

import duckdb
import pytest


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
    in __init__, which attempts to connect to Redis.  Mock it everywhere.
    """
    with (
        patch("supertable.engine.data_estimator.RedisCatalog") as MockDE,
        patch("supertable.engine.spark_thrift.RedisCatalog") as MockST,
    ):
        MockDE.return_value = MagicMock()
        MockST.return_value = MagicMock()
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
