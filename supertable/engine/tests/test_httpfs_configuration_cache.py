# route: supertable.engine.tests.test_httpfs_configuration_cache
"""``configure_httpfs_and_s3`` must configure once per connection, not per alias.

The read path calls it from ``_build_view_chain`` once for every table a query
names, outside the thread-local gate ``DuckDBEngine`` keeps for exactly this
purpose.  Measured on an object-store deployment that was ~16 ms per call —
~50 ms of pure repetition on a three-table join, on every query forever.

These tests pin the contract the fix rests on, in both directions:
  * a second call with the same configuration must do no SQL at all, and
  * a configuration change must still be applied on the very next call,
because a cache that never invalidates would silently serve stale credentials.
"""

from __future__ import annotations

import dataclasses

import duckdb
import pytest

from supertable.config.settings import settings as real_settings
import supertable.engine.engine_common as ec


S3_FILES = ["s3://bucket/facts/part-0.parquet"]
S3_FILES_OTHER = ["s3://bucket/customers/part-0.parquet"]


@pytest.fixture(autouse=True)
def _cold_cache():
    ec.reset_httpfs_configuration()
    yield
    ec.reset_httpfs_configuration()


class _SpyConnection:
    """Wraps a real connection and records every statement executed."""

    def __init__(self, con):
        self._con = con
        self.sql = []

    def execute(self, sql, *a, **k):
        self.sql.append(str(sql))
        return self._con.execute(sql, *a, **k)

    def __getattr__(self, name):
        return getattr(self._con, name)


def _spy(duckdb_con):
    return _SpyConnection(duckdb_con)


def test_second_call_same_config_executes_no_sql(duckdb_con):
    """The per-alias repeat is the whole finding: it must become free."""
    con = _spy(duckdb_con)
    ec.configure_httpfs_and_s3(con, S3_FILES)
    assert con.sql, "first call must actually configure"

    con.sql.clear()
    ec.configure_httpfs_and_s3(con, S3_FILES)
    assert con.sql == []


def test_a_different_alias_is_still_the_same_configuration(duckdb_con):
    """``for_paths`` decides *whether* to configure, never *what*."""
    con = _spy(duckdb_con)
    ec.configure_httpfs_and_s3(con, S3_FILES)
    con.sql.clear()
    ec.configure_httpfs_and_s3(con, S3_FILES_OTHER)
    assert con.sql == []


def test_settings_change_is_applied_on_the_next_call(duckdb_con):
    con = _spy(duckdb_con)
    ec.configure_httpfs_and_s3(con, S3_FILES)

    original = ec.settings
    ec.settings = dataclasses.replace(real_settings, STORAGE_REGION="ap-south-7")
    try:
        con.sql.clear()
        ec.configure_httpfs_and_s3(con, S3_FILES)
    finally:
        ec.settings = original

    assert any("s3_region='ap-south-7'" in s for s in con.sql)
    region = duckdb_con.execute(
        "SELECT value FROM duckdb_settings() WHERE name='s3_region'"
    ).fetchone()[0]
    assert region == "ap-south-7"


def test_cache_is_per_connection(duckdb_con):
    """A second connection has none of the first one's settings."""
    first = _spy(duckdb_con)
    ec.configure_httpfs_and_s3(first, S3_FILES)

    other = duckdb.connect()
    try:
        second = _spy(other)
        ec.configure_httpfs_and_s3(second, S3_FILES)
        assert second.sql, "a fresh connection must be configured from scratch"
    finally:
        other.close()


def test_local_paths_never_configure(duckdb_con):
    con = _spy(duckdb_con)
    ec.configure_httpfs_and_s3(con, ["/var/data/part-0.parquet"])
    assert con.sql == []
    ec.configure_httpfs_and_s3(con, [])
    assert con.sql == []


def test_failed_configuration_is_not_remembered(duckdb_con, monkeypatch):
    """A connection that failed PART WAY THROUGH must be retried, not skipped.

    The failure is injected inside ``set_if_supported`` so httpfs is already
    loaded and the capability scan has already run — i.e. the connection is
    genuinely half-configured, which is the state a cache must not record.
    """
    con = _spy(duckdb_con)

    def explode(*_a, **_k):
        raise RuntimeError("credential lookup failed")

    monkeypatch.setattr(ec, "sanitize_sql_string", explode)
    with pytest.raises(RuntimeError):
        ec.configure_httpfs_and_s3(con, S3_FILES)
    monkeypatch.undo()

    con.sql.clear()
    ec.configure_httpfs_and_s3(con, S3_FILES)
    assert any("SET s3_" in s for s in con.sql), \
        "the retry must configure, not return from the cache"


def test_capability_scan_runs_once_per_process(duckdb_con):
    """The 160-row duckdb_settings() scan is a third of the call's cost."""
    first = _spy(duckdb_con)
    ec.configure_httpfs_and_s3(first, S3_FILES)
    assert sum("duckdb_settings" in s for s in first.sql) == 1

    other = duckdb.connect()
    try:
        second = _spy(other)
        ec.configure_httpfs_and_s3(second, S3_FILES)
        assert sum("duckdb_settings" in s for s in second.sql) == 0
    finally:
        other.close()
