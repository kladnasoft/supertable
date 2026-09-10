# route: supertable.tests.test_capability_probe_cache
"""The external-file-cache capability probe is a fact about the DuckDB build.

``_external_file_cache_cappable`` asks whether this DuckDB binary exposes the
``external_file_cache_max_size`` setting. That cannot change while the process
runs, but ``apply_runtime_pragmas`` called it on every query and the probe is a
scan of ``duckdb_settings()`` — 8.4ms per query re-learning the same answer.

It was invisible while every query built its own connection. Once the
connection became persistent it stood out as the second-largest avoidable cost
in the read path, which is why it is memoized now.
"""

from __future__ import annotations

from importlib import import_module

import duckdb
import pytest

ec = import_module("supertable.engine.engine_common")


@pytest.fixture(autouse=True)
def _clear_cache():
    ec._EXTERNAL_FILE_CACHE_CAPPABLE.clear()
    yield
    ec._EXTERNAL_FILE_CACHE_CAPPABLE.clear()


class _CountingCon:
    """Wraps a real connection so the probe still gets a truthful answer."""

    def __init__(self):
        self._con = duckdb.connect()
        self.queries = 0

    def execute(self, sql, *a, **k):
        self.queries += 1
        return self._con.execute(sql, *a, **k)


def test_probe_runs_once_per_build():
    con = _CountingCon()
    first = ec._external_file_cache_cappable(con)
    for _ in range(50):
        assert ec._external_file_cache_cappable(con) == first
    assert con.queries == 1, f"probed {con.queries}x; the build cannot change"


def test_answer_is_shared_across_connections():
    """The cache is keyed by build, so a second connection reuses the answer."""
    a, b = _CountingCon(), _CountingCon()
    assert ec._external_file_cache_cappable(a) == ec._external_file_cache_cappable(b)
    assert (a.queries, b.queries) == (1, 0)


def test_cache_is_keyed_by_duckdb_version():
    ec._external_file_cache_cappable(_CountingCon())
    assert list(ec._EXTERNAL_FILE_CACHE_CAPPABLE) == [duckdb.__version__]


class _BrokenCon:
    def execute(self, *a, **k):
        raise RuntimeError("connection is closed")


def test_failure_is_not_cached():
    """A probe failure may be about the connection, not the build.

    Caching False here would permanently disable a cap the build does support,
    for every later connection, because one connection happened to be broken.
    """
    assert ec._external_file_cache_cappable(_BrokenCon()) is False
    assert not ec._EXTERNAL_FILE_CACHE_CAPPABLE, "a failure must not be cached"

    good = _CountingCon()
    ec._external_file_cache_cappable(good)
    assert good.queries == 1, "a real connection must still be able to probe"
