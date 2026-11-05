# route: supertable.engine.tests.test_stream_cursor_configuration
"""A stream's cursor must reach object storage the way its connection does.

``DuckDBEngine.stream`` builds the view chain on the shared connection and
runs the query on ``con.cursor()``. DuckDB gives that cursor a session of its
own: it sees the connection's views and global settings (memory_limit,
threads), but every ``s3_*`` setting and ``http_timeout`` start at DuckDB's
defaults. 3.5.1 configured only the connection, so against a deployment's own
object store the scan the cursor ran went to AWS, without credentials, over
TLS and with vhost URLs, instead of to the store the views were bound against.

These tests run the engine's real stream and read the settings back through
that cursor: the query selects ``current_setting(...)`` over the table's view
chain. The reflection view itself is a local one-row relation rather than a
scan of the S3 paths, so nothing here, on this code or on 3.5.1, opens a
connection to any store; the endpoint is a closed local port regardless.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pytest

import supertable.engine.duckdb as engine_duckdb
import supertable.engine.engine_common as ec
from supertable.config.settings import settings as real_settings
from supertable.data_classes import Reflection, SuperSnapshot
from supertable.engine.engine_config import EngineRuntimeConfig
from supertable.utils.sql_parser import SQLParser


S3_FILES = ["s3://lake/acme/invoices/data/part-0.parquet"]

#: The object-storage settings, as DuckDB reports them once configured.
STORAGE = {
    "s3_endpoint": "127.0.0.1:1",
    "s3_region": "eu-central-1",
    "s3_url_style": "path",
    "s3_use_ssl": False,
    "s3_access_key_id": "test-key",
    "s3_secret_access_key": "test-secret",
    "s3_session_token": "test-token",
}

RUNTIME = EngineRuntimeConfig(
    engine_lite_max_bytes=0, engine_spark_min_bytes=0, engine_freshness_sec=0,
    duckdb_memory_limit="256MB", duckdb_io_multiplier=1.0, duckdb_threads=2,
    duckdb_http_timeout=7, duckdb_external_cache_size="",
)

SETTINGS = [*STORAGE, "http_timeout", "enable_http_metadata_cache",
            "enable_external_file_cache", "memory_limit", "threads"]


def _one_row_view(con, view_name, files, columns=None, column_types=None):
    """The reflection view as a local relation: no scan, so no store."""
    con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT 1 AS amount")


@pytest.fixture
def engine(tmp_path, monkeypatch):
    """A DuckDB engine configured for an S3-compatible store of its own.

    The engine pins DuckDB's home directory to the application home and never
    downloads an extension, so this build's httpfs is linked into a temporary
    application home from wherever this machine keeps it.
    """
    found = None
    for root in (Path.home() / ".duckdb" / "extensions",
                 Path(ec.get_app_home()) / ".duckdb" / "extensions"):
        found = next(iter(sorted(
            root.glob(f"v{duckdb.__version__}/*/httpfs.duckdb_extension"))), None)
        if found is not None:
            break
    if found is None:
        pytest.skip("DuckDB's httpfs extension is not installed locally")
    extensions = tmp_path / ".duckdb" / "extensions"
    extensions.mkdir(parents=True)
    (extensions / found.parent.parent.name).symlink_to(found.parent.parent)

    monkeypatch.setattr(ec, "get_app_home", lambda: str(tmp_path))
    monkeypatch.setattr(ec, "settings", dataclasses.replace(
        real_settings,
        STORAGE_ENDPOINT_URL="http://127.0.0.1:1",
        STORAGE_REGION="eu-central-1",
        STORAGE_FORCE_PATH_STYLE=True,
        STORAGE_USE_SSL=False,
        STORAGE_ACCESS_KEY="test-key",
        STORAGE_SECRET_KEY="test-secret",
        STORAGE_SESSION_TOKEN="test-token",
        SUPERTABLE_DUCKDB_HTTP_TIMEOUT="3",
        SUPERTABLE_DUCKDB_EXTERNAL_CACHE_SIZE="",
        SUPERTABLE_DUCKDB_MATERIALIZE="view",
        SUPERTABLE_DUCKDB_ALLOW_EXTENSION_DOWNLOAD=False,
    ))
    monkeypatch.setattr(ec, "create_reflection_view", _one_row_view)
    engine_duckdb.reset_shared_duckdb_state()
    ec.reset_httpfs_configuration()
    yield engine_duckdb.DuckDBEngine()
    engine_duckdb.reset_shared_duckdb_state()
    ec.reset_httpfs_configuration()


def _stream(engine, tmp_path, query, engine_config):
    snapshot = SuperSnapshot(
        super_name="lake", simple_name="invoices", simple_version=1,
        files=list(S3_FILES), columns={"amount"},
    )
    reflection = Reflection(
        storage_type="MINIO", reflection_bytes=0, total_reflections=1,
        supers=[snapshot],
    )
    return engine.stream(
        reflection, SQLParser("lake", query, "duckdb"),
        SimpleNamespace(temp_dir=str(tmp_path / "spill")), lambda *_: None,
        engine_config=engine_config, batch_rows=1,
    )


def _settings_query():
    return "SELECT " + ", ".join(
        f"current_setting('{name}') AS {name}" for name in SETTINGS
    ) + " FROM invoices"


def _query_views(con) -> int:
    return con.execute(
        "SELECT count(*) FROM duckdb_views() "
        "WHERE view_name LIKE 'st_%' OR view_name LIKE 'tomb_%'"
    ).fetchone()[0]


@pytest.mark.parametrize("engine_config, http_timeout", [
    (RUNTIME, 7),   # the runtime config, applied after the storage settings
    (None, 3),      # no resolved config: the storage settings' timeout
])
def test_the_cursor_reads_with_the_connections_settings(
        engine, tmp_path, engine_config, http_timeout):
    """Every setting the connection was given, the cursor reports too.

    Two streams, two cursors: a fresh cursor is a fresh session, so each one
    has to be configured, not only the first.
    """
    for _ in range(2):
        handle = _stream(engine, tmp_path, _settings_query(), engine_config)
        assert handle._cursor is not handle._con
        on_cursor = pa.Table.from_batches(
            list(handle.batches()), schema=handle.schema,
        ).to_pylist()
        assert len(on_cursor) == 1

        con = engine_duckdb._shared_state()["con"]
        on_connection = dict(zip(SETTINGS, con.execute(
            "SELECT " + ", ".join(f"current_setting('{n}')" for n in SETTINGS)
        ).fetchone()))
        assert {k: on_connection[k] for k in STORAGE} == STORAGE
        assert on_connection["http_timeout"] == http_timeout
        assert on_cursor[0] == on_connection
        assert _query_views(con) == 0


def test_runtime_pragmas_reach_the_cursor(engine, tmp_path):
    """The org's runtime config, not the settings-based start, is in force."""
    reference = duckdb.connect()
    try:
        reference.execute("PRAGMA memory_limit='256MB'")
        memory_limit = reference.execute(
            "SELECT current_setting('memory_limit')").fetchone()[0]
    finally:
        reference.close()

    handle = _stream(engine, tmp_path, _settings_query(), RUNTIME)
    on_cursor = pa.Table.from_batches(
        list(handle.batches()), schema=handle.schema,
    ).to_pylist()[0]
    assert on_cursor["memory_limit"] == memory_limit
    assert on_cursor["threads"] == 2
    assert on_cursor["http_timeout"] == 7
    assert on_cursor["enable_external_file_cache"] is False


def test_a_failure_configuring_the_cursor_tears_the_query_down(
        engine, tmp_path, monkeypatch):
    """Configuring the cursor happens inside the stream's cleanup guard.

    If it raises, the caller gets the error and no handle, so nothing else
    would ever drop the query's views or close its cursor.
    """
    real_configure = engine_duckdb.configure_httpfs_and_s3

    def failing_for_the_cursor(con, paths):
        if con is not engine_duckdb._shared_state()["con"]:
            raise RuntimeError("cursor configuration failed")
        return real_configure(con, paths)

    monkeypatch.setattr(engine_duckdb, "configure_httpfs_and_s3",
                        failing_for_the_cursor)
    with pytest.raises(RuntimeError, match="cursor configuration failed"):
        _stream(engine, tmp_path, "SELECT amount FROM invoices", RUNTIME)
    assert _query_views(engine_duckdb._shared_state()["con"]) == 0
