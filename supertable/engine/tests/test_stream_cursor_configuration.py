# route: supertable.engine.tests.test_stream_cursor_configuration
"""A stream's cursor reaches object storage as its connection does, and its
query cannot read the store's credentials.

``DuckDBEngine.stream`` builds the view chain on the shared connection and
runs the query on ``con.cursor()``. DuckDB gives that cursor a session of its
own: it sees the connection's views and everything the database holds (its
secrets, its global settings) but none of the connection's session settings.
3.5.1 SET the store's ``s3_*`` settings and ``http_timeout`` in the
connection's session, so against a deployment's own object store the scan the
cursor ran went to AWS, without credentials, over TLS and with vhost URLs.
3.5.2's first build SET them on the cursor as well, which put the store's key
and secret in the session that runs the caller's query, where
``current_setting('s3_secret_access_key')``, a scalar call the read gate
admits, returned them.

``configure_httpfs_and_s3`` keeps the store in a temporary secret and SETs
``http_timeout`` GLOBAL, as ``apply_runtime_pragmas`` does, so the cursor reads
with the database's configuration, needs none of its own, and no setting holds
a credential.

These tests run the engine's real stream. The reflection view itself is a local
one-row relation rather than a scan of the S3 paths, so nothing here opens a
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
from supertable.system_query import assert_read_only
from supertable.utils.sql_parser import SQLParser


S3_FILES = ["s3://lake/acme/invoices/data/part-0.parquet"]
LOCAL_FILES = ["/data/lake/acme/invoices/data/part-0.parquet"]

#: The secret a read of ``S3_FILES`` signs with, as DuckDB shows it.
SECRET = {
    "name": ec._S3_SECRET_NAME,
    "persistent": "TEMPORARY",
    "storage": "memory",
    "endpoint": "127.0.0.1:1",
    "region": "eu-central-1",
    "url_style": "path",
    "use_ssl": "false",
    "key_id": "test-key",
    "secret": "redacted",
    "session_token": "redacted",
}

#: The settings 3.5.1 and 3.5.2's first build kept the store's credentials in.
CREDENTIAL_SETTINGS = ("s3_access_key_id", "s3_secret_access_key",
                       "s3_session_token")

RUNTIME = EngineRuntimeConfig(
    engine_lite_max_bytes=0, engine_spark_min_bytes=0, engine_freshness_sec=0,
    duckdb_memory_limit="256MB", duckdb_io_multiplier=1.0, duckdb_threads=2,
    duckdb_http_timeout=7, duckdb_external_cache_size="",
)

SETTINGS = ["http_timeout", "enable_http_metadata_cache",
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


def _stream(engine, tmp_path, query, engine_config, files=S3_FILES):
    snapshot = SuperSnapshot(
        super_name="lake", simple_name="invoices", simple_version=1,
        files=list(files), columns={"amount"},
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


def _rows(handle):
    return pa.Table.from_batches(
        list(handle.batches()), schema=handle.schema,
    ).to_pylist()


def _settings_query():
    return "SELECT " + ", ".join(
        f"current_setting('{name}') AS {name}" for name in SETTINGS
    ) + " FROM invoices"


def _current_settings(con, names):
    return dict(zip(names, con.execute(
        "SELECT " + ", ".join(f"current_setting('{n}')" for n in names)
    ).fetchone()))


def _signing_secret(con, path):
    """The secret DuckDB would sign a read of *path* on *con* with, if any."""
    found = con.execute(f"FROM which_secret('{path}', 's3')").fetchall()
    if not found:
        return None
    ((name, persistent, storage),) = found
    ((shown,),) = con.execute(
        f"SELECT secret_string FROM duckdb_secrets() WHERE name = '{name}'"
    ).fetchall()
    fields = dict(item.split("=", 1) for item in shown.split(";"))
    return {"name": name, "persistent": persistent, "storage": storage,
            **{key: fields.get(key) for key in SECRET
               if key not in ("name", "persistent", "storage")}}


def _query_views(con) -> int:
    return con.execute(
        "SELECT count(*) FROM duckdb_views() "
        "WHERE view_name LIKE 'st_%' OR view_name LIKE 'tomb_%'"
    ).fetchone()[0]


@pytest.mark.parametrize("engine_config, http_timeout", [
    (RUNTIME, 7),   # the runtime config, applied after the storage settings
    (None, 3),      # no resolved config: the storage settings' timeout
])
def test_the_cursor_reads_with_the_connections_configuration(
        engine, tmp_path, engine_config, http_timeout):
    """The cursor signs with the store's secret and has the connection's settings.

    Two streams, two cursors: a fresh cursor is a fresh session, so what it
    reads with has to be the database's, not something given to one cursor.
    """
    for _ in range(2):
        handle = _stream(engine, tmp_path, _settings_query(), engine_config)
        try:
            assert handle._cursor is not handle._con
            on_cursor = handle.reader.read_all().to_pylist()
            assert len(on_cursor) == 1
            assert _signing_secret(handle._cursor, S3_FILES[0]) == SECRET
        finally:
            handle.close()

        con = engine_duckdb._shared_state()["con"]
        on_connection = _current_settings(con, SETTINGS)
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

    on_cursor = _rows(_stream(engine, tmp_path, _settings_query(), RUNTIME))[0]
    assert on_cursor["memory_limit"] == memory_limit
    assert on_cursor["threads"] == 2
    assert on_cursor["http_timeout"] == 7
    assert on_cursor["enable_external_file_cache"] is False


@pytest.mark.parametrize("query, expected", [
    ("SELECT current_setting('s3_access_key_id') AS k, "
     "current_setting('s3_secret_access_key') AS s, "
     "current_setting('s3_session_token') AS t FROM invoices",
     [{"k": None, "s": None, "t": None}]),
    # No row has to be readable: the projection is evaluated regardless.
    ("SELECT current_setting('s3_secret_access_key') AS s, count(*) AS n "
     "FROM invoices WHERE 1 = 0",
     [{"s": None, "n": 0}]),
])
def test_an_admitted_query_cannot_read_the_stores_credentials(
        engine, tmp_path, query, expected):
    """No setting holds a credential, on the cursor or on its connection.

    The read gate admits these queries: their one source is a named table.
    On 3.5.2's first build the cursor returned the store's key, secret and
    token, which for a deployment that hands its object store's root
    credentials to the engine is write and delete on the whole store.
    """
    assert_read_only(query)
    assert _rows(_stream(engine, tmp_path, query, RUNTIME)) == expected

    con = engine_duckdb._shared_state()["con"]
    assert _current_settings(con, CREDENTIAL_SETTINGS) == dict.fromkeys(
        CREDENTIAL_SETTINGS)


@pytest.mark.parametrize("query", [
    "SELECT secret_string FROM duckdb_secrets()",
    "SELECT (SELECT secret_string FROM duckdb_secrets()) AS s FROM invoices",
    f"SELECT * FROM which_secret('{S3_FILES[0]}', 's3')",
])
def test_the_read_gate_refuses_what_shows_the_secret(query):
    """What keeps the secret from a query once no setting holds it.

    duckdb_secrets() is the only SQL that shows what a secret holds (it
    redacts the secret and the token, not the key ID), and it is a table
    function, which the read gate refuses wherever in the query it sits.
    """
    with pytest.raises(ValueError, match="only named tables"):
        assert_read_only(query)


class _RecordingCursor:
    """A stream's cursor, recording every statement run on it."""

    def __init__(self, cursor):
        self._cursor = cursor
        self.sql = []

    def execute(self, sql, *args, **kwargs):
        self.sql.append(str(sql))
        return self._cursor.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _RecordingConnection:
    """The engine's connection, handing out recording cursors."""

    def __init__(self, con):
        self._con = con
        self.cursors = []

    def cursor(self):
        cursor = _RecordingCursor(self._con.cursor())
        self.cursors.append(cursor)
        return cursor

    def __getattr__(self, name):
        return getattr(self._con, name)


@pytest.mark.parametrize("files", [S3_FILES, LOCAL_FILES], ids=["s3", "local"])
def test_the_cursor_runs_the_query_and_nothing_else(engine, tmp_path, files):
    """No stream configures its cursor: the database's configuration is enough.

    3.5.2's first build re-ran the connection's configuration on every
    stream's cursor before the query: 16 statements for an S3 stream and 4
    for a local one. The cursor has all of it from its database now.
    """
    state = engine_duckdb._shared_state()
    state["con"] = _RecordingConnection(
        engine._get_connection(temp_dir=str(tmp_path / "spill")))

    for _ in range(2):
        handle = _stream(engine, tmp_path, "SELECT amount FROM invoices",
                         RUNTIME, files)
        assert _rows(handle) == [{"amount": 1}]

    cursors = state["con"].cursors
    assert [len(cursor.sql) for cursor in cursors] == [1, 1]
    assert all(cursor.sql[0].lstrip().upper().startswith("SELECT ")
               for cursor in cursors)
