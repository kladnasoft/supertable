"""Adversarial regressions for the untrusted DuckDB read boundary."""

from dataclasses import replace
from pathlib import Path
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import polars as pl
import pytest

import supertable.data_reader as data_reader_module
import supertable.engine.duckdb_engine as duckdb_engine_module
import supertable.engine.engine_common as engine_common
from supertable.data_classes import (
    Reflection,
    RbacViewDef,
    SuperSnapshot,
    TombstoneDef,
)
from supertable.data_reader import DataReader, Status
from supertable.engine.duckdb_engine import (
    DuckDB,
    _DuckDBArrowBatchIterator,
    _harden_user_query_connection,
)
from supertable.engine.engine_common import (
    configure_httpfs_and_s3,
    create_rbac_view,
    redact_url_credentials,
)
from supertable.engine.engine_enum import Engine
from supertable.storage.s3_storage import S3Storage
from supertable.utils.sql_parser import SQLParser


@pytest.mark.parametrize(
    "expression",
    [
        "current_setting('s3_access_key_id')",
        "current_setting('s3_secret_access_key')",
        "current_setting('s3_session_token')",
        "duckdb_settings()",
        "duckdb_secrets()",
        "getvariable('credential')",
        "getenv('AWS_SECRET_ACCESS_KEY')",
        "input_file_name()",
        "query_table('foreign')",
        "read_text('/etc/passwd')",
        "glob('/etc/*')",
        "version()",
    ],
)
def test_parser_rejects_settings_secrets_files_and_unknown_functions(expression):
    with pytest.raises(ValueError, match="not allowed"):
        SQLParser("lake", f"SELECT {expression} FROM lake.cards", "duckdb")


@pytest.mark.parametrize(
    "expression",
    [
        "evil.sum(amount)",
        '"Pg_Catalog"."AbS"(amount)',
        "(extensions.upper(name))",
    ],
)
def test_parser_rejects_qualified_calls_even_when_function_name_is_safe(expression):
    with pytest.raises(ValueError, match="not allowed"):
        SQLParser("lake", f"SELECT {expression} FROM lake.cards", "duckdb")


@pytest.mark.parametrize(
    "expression",
    ["USER", "SESSION_USER", "CURRENT_ROLE", "CURRENT_CATALOG", "CURRENT_SCHEMA"],
)
def test_parser_rejects_bare_duckdb_session_identity_columns(expression):
    # DuckDB executes these tokens as connection metadata even though SQLGlot
    # 26.x models them as ordinary unqualified Column nodes.
    with pytest.raises(ValueError, match="session identity"):
        SQLParser("lake", f"SELECT {expression} FROM lake.cards", "duckdb")


def test_parser_preserves_quoted_qualified_columns_and_clock_expressions():
    SQLParser(
        "lake",
        'SELECT "USER", cards.USER, LOCALTIME, LOCALTIMESTAMP '
        "FROM lake.cards AS cards",
        "duckdb",
    )


def test_real_duckdb_confirms_bare_session_tokens_are_not_data_columns():
    con = duckdb.connect()
    try:
        for expression in (
            "USER", "SESSION_USER", "CURRENT_ROLE", "CURRENT_CATALOG",
            "CURRENT_SCHEMA",
        ):
            assert isinstance(con.execute(f"SELECT {expression}").fetchone()[0], str)
        clock_row = con.execute("SELECT LOCALTIME, LOCALTIMESTAMP").fetchone()
        assert all(value is not None for value in clock_row)
    finally:
        con.close()


def test_parser_preserves_ordinary_analytics():
    parser = SQLParser(
        "lake",
        """
        SELECT category,
               COUNT(*) AS n,
               SUM(amount) AS total,
               AVG(amount) AS mean,
               MIN(amount) AS lo,
               MAX(amount) AS hi,
               UPPER(name) AS upper_name,
               COALESCE(NULLIF(name, ''), 'unknown') AS normalized,
               TRY_CAST(amount AS DOUBLE) AS numeric_amount,
               CASE WHEN amount > 0 THEN 1 ELSE 0 END AS positive,
               EXTRACT(MONTH FROM created_at) AS month_no,
               DATE_TRUNC('month', created_at) AS month_start,
               ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount) AS rn,
               LAG(amount) OVER (PARTITION BY category ORDER BY amount) AS prev
        FROM lake.cards
        GROUP BY category, name, amount, created_at
        """,
        "duckdb",
    )
    assert [(table.super_name, table.simple_name) for table in parser.get_physical_tables()] == [
        ("lake", "cards")
    ]


@pytest.mark.parametrize(
    "query",
    [
        "SELECT LIST(id) FROM lake.cards",
        "SELECT ARRAY_AGG(id) FROM lake.cards",
        "SELECT STRING_AGG(name, ',') FROM lake.cards",
        "SELECT LIST(id) FROM (SELECT id FROM lake.cards LIMIT 10) bounded",
    ],
)
def test_public_parser_rejects_collection_result_amplifiers(query):
    with pytest.raises(ValueError, match="not allowed"):
        SQLParser("lake", query, "duckdb")


def test_internal_collection_capability_requires_direct_literal_limit_at_most_ten():
    allowed = SQLParser(
        "lake",
        "SELECT LIST(id) FROM (SELECT id FROM lake.cards LIMIT 10) bounded",
        "duckdb",
        allow_bounded_collection_aggregates=True,
    )
    assert allowed.get_physical_tables()[0].simple_name == "cards"

    for limit in ("11", "ALL"):
        with pytest.raises(ValueError, match="not allowed"):
            SQLParser(
                "lake",
                f"SELECT LIST(id) FROM (SELECT id FROM lake.cards LIMIT {limit}) bounded",
                "duckdb",
                allow_bounded_collection_aggregates=True,
            )


def test_backend_revalidates_before_connection_or_view_setup(monkeypatch):
    parser = SQLParser("lake", "SELECT id FROM lake.cards", "duckdb")
    parser.original_query = (
        "SELECT current_setting('s3_secret_access_key') FROM lake.cards"
    )
    engine = DuckDB()
    connection = MagicMock(side_effect=AssertionError("connection was opened"))
    monkeypatch.setattr(engine, "_get_connection", connection)

    with pytest.raises(ValueError, match="current_setting"):
        engine.execute(
            Reflection("local", 0, 0, []),
            parser,
            SimpleNamespace(temp_dir="/tmp", query_plan_path="/tmp/unused.json"),
            lambda _event: None,
        )
    connection.assert_not_called()


def test_backend_rejects_bare_session_identity_before_connection(monkeypatch):
    parser = SQLParser("lake", "SELECT id FROM lake.cards", "duckdb")
    parser.original_query = "SELECT USER FROM lake.cards"
    engine = DuckDB()
    connection = MagicMock(side_effect=AssertionError("connection was opened"))
    monkeypatch.setattr(engine, "_get_connection", connection)

    with pytest.raises(ValueError, match="session identity"):
        engine.execute(
            Reflection("local", 0, 0, []),
            parser,
            SimpleNamespace(temp_dir="/tmp", query_plan_path="/tmp/unused.json"),
            lambda _event: None,
        )
    connection.assert_not_called()


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id INTO leaked FROM lake.cards",
        "SELECT * FROM lake.cards FOR UPDATE",
        "SELECT * FROM lake.cards FOR SHARE",
        "SELECT * FROM lake.cards LOCK IN SHARE MODE",
    ],
)
def test_parser_rejects_select_into_and_locking_reads(query):
    with pytest.raises(ValueError, match="read-only"):
        SQLParser("lake", query, "duckdb")


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id FROM lake.cards; PRAGMA database_list",
        "SELECT id FROM lake.cards; SELECT current_setting('s3_secret_access_key')",
        "SELECT 1; CREATE TABLE leaked(id INTEGER)",
    ],
)
def test_parser_rejects_multiple_substantive_statements(query):
    with pytest.raises(ValueError, match="Exactly one SQL statement"):
        SQLParser("lake", query, "duckdb")


def test_parser_allows_trailing_semicolon_comment():
    SQLParser("lake", "SELECT id FROM lake.cards; -- ordinary comment", "duckdb")


def test_backend_rejects_tableless_multi_statement_custom_parser_before_setup(
    monkeypatch,
):
    parser = SimpleNamespace(
        original_query="SELECT 1; PRAGMA database_list",
        default_super_name="lake",
        get_table_tuples=lambda: [],
    )
    engine = DuckDB()
    connection = MagicMock(side_effect=AssertionError("connection was opened"))
    monkeypatch.setattr(engine, "_get_connection", connection)
    with pytest.raises(ValueError, match="Exactly one SQL statement"):
        engine.execute(
            Reflection("local", 0, 0, []),
            parser,
            SimpleNamespace(temp_dir="/tmp", query_plan_path="/tmp/unused.json"),
            lambda _event: None,
        )
    connection.assert_not_called()


def test_backend_rejects_requested_table_missing_from_reflection_before_setup(
    monkeypatch,
):
    parser = SQLParser("lake", "SELECT id FROM lake.denied", "duckdb")
    allowed_snapshot = SuperSnapshot(
        "lake", "cards", 1, ["/tmp/allowed.parquet"], {"id"}, ["raw/allowed"],
        column_types={"id": "Int64"},
    )
    engine = DuckDB()
    connection = MagicMock(side_effect=AssertionError("connection was opened"))
    monkeypatch.setattr(engine, "_get_connection", connection)
    with pytest.raises(PermissionError, match="authorize"):
        engine.execute(
            Reflection("local", 1, 1, [allowed_snapshot]),
            parser,
            SimpleNamespace(temp_dir="/tmp", query_plan_path="/tmp/unused.json"),
            lambda _event: None,
        )
    connection.assert_not_called()


def test_s3_credentials_live_only_in_redacted_memory_secret(monkeypatch):
    safe_settings = replace(
        engine_common.settings,
        STORAGE_ACCESS_KEY="sandbox-access-id",
        STORAGE_SECRET_KEY="sandbox-secret-value",
        STORAGE_SESSION_TOKEN="sandbox-session-value",
        STORAGE_ENDPOINT_URL="http://127.0.0.1:9000",
        STORAGE_REGION="us-east-1",
        STORAGE_FORCE_PATH_STYLE=True,
        STORAGE_USE_SSL=False,
    )
    monkeypatch.setattr(engine_common, "settings", safe_settings)
    con = duckdb.connect()
    try:
        configure_httpfs_and_s3(con, ["s3://sandbox-bucket/object.parquet"])
        row = con.execute(
            "SELECT persistent, storage, secret_string "
            "FROM duckdb_secrets() WHERE name='supertable_s3'"
        ).fetchone()
        assert row is not None
        assert row[0] is False
        assert row[1] == "memory"
        assert "sandbox-secret-value" not in row[2]
        assert "sandbox-session-value" not in row[2]
        assert "secret=redacted" in row[2]
        assert "session_token=redacted" in row[2]
        for setting_name in (
            "s3_access_key_id",
            "s3_secret_access_key",
            "s3_session_token",
        ):
            assert con.execute(
                "SELECT current_setting(?)", [setting_name]
            ).fetchone()[0] is None
    finally:
        con.close()


def test_s3_secret_uses_injected_storage_auth_not_ambient_settings(monkeypatch):
    safe_settings = replace(
        engine_common.settings,
        STORAGE_ACCESS_KEY="broader-ambient-access",
        STORAGE_SECRET_KEY="broader-ambient-secret",
        STORAGE_SESSION_TOKEN="broader-ambient-token",
        STORAGE_ENDPOINT_URL="https://ambient.invalid",
        STORAGE_REGION="us-west-2",
        STORAGE_FORCE_PATH_STYLE=False,
        STORAGE_USE_SSL=True,
    )
    monkeypatch.setattr(engine_common, "settings", safe_settings)
    storage = S3Storage(
        bucket_name="tenant-bucket",
        client=object(),
        endpoint_url="http://127.0.0.1:9000",
        region="eu-central-1",
        url_style="path",
        secure=False,
        aws_access_key_id="tenant-access",
        aws_secret_access_key="tenant-secret",
        aws_session_token="tenant-token",
    )
    con = duckdb.connect()
    try:
        configure_httpfs_and_s3(
            con, ["s3://tenant-bucket/object.parquet"], storage=storage,
        )
        secret = con.execute(
            "SELECT secret_string FROM duckdb_secrets() "
            "WHERE name='supertable_s3'"
        ).fetchone()[0]
        assert "tenant-access" in secret
        assert "broader-ambient-access" not in secret
        assert "127.0.0.1:9000" in secret
        assert "ambient.invalid" not in secret
        assert "tenant-secret" not in secret
        assert "tenant-token" not in secret
    finally:
        con.close()


def test_executor_threads_selected_storage_into_s3_provisioning(monkeypatch):
    storage = object()
    captured = []

    def configure(con, paths, *, storage=None):
        captured.append((con, list(paths), storage))

    monkeypatch.setattr(
        duckdb_engine_module, "configure_httpfs_and_s3", configure,
    )
    con = object()
    engine = DuckDB(storage=storage)
    engine._ensure_httpfs(con, ["s3://tenant-bucket/object.parquet"])
    assert captured == [
        (con, ["s3://tenant-bucket/object.parquet"], storage),
    ]


def test_s3_secret_rejects_opaque_injected_auth_instead_of_using_ambient(
    monkeypatch,
):
    safe_settings = replace(
        engine_common.settings,
        STORAGE_ACCESS_KEY="broader-ambient-access",
        STORAGE_SECRET_KEY="broader-ambient-secret",
    )
    monkeypatch.setattr(engine_common, "settings", safe_settings)
    storage = S3Storage(
        bucket_name="tenant-bucket",
        client=object(),
        endpoint_url="https://s3.example.test",
        region="eu-central-1",
    )
    con = duckdb.connect()
    try:
        with pytest.raises(RuntimeError, match="protected in-memory S3 access"):
            configure_httpfs_and_s3(
                con, ["s3://tenant-bucket/object.parquet"], storage=storage,
            )
        assert con.execute(
            "SELECT COUNT(*) FROM duckdb_secrets() "
            "WHERE name='supertable_s3'"
        ).fetchone()[0] == 0
    finally:
        con.close()


def test_query_connection_disables_secret_and_extension_escape_hatches():
    con = duckdb.connect()
    try:
        _harden_user_query_connection(con)
        for setting_name in (
            "allow_unredacted_secrets",
            "autoload_known_extensions",
            "autoinstall_known_extensions",
            "allow_community_extensions",
        ):
            assert con.execute(
                "SELECT current_setting(?)", [setting_name]
            ).fetchone()[0] is False
        # lock_configuration is intentionally not used: DuckDB applies it to
        # every cursor on the shared database and it is irreversible.
        assert con.execute(
            "SELECT current_setting('lock_configuration')"
        ).fetchone()[0] is False
    finally:
        con.close()


@pytest.mark.parametrize(
    "predicate",
    [
        "current_setting('s3_secret_access_key') IS NULL",
        "USER = 'policy-owner'",
        "EXISTS (SELECT 1 FROM denied_table)",
        "evil.abs(id) > 0",
        "SUM(id) > 0",
    ],
)
def test_rbac_predicate_channel_rejects_before_any_connection_sql(predicate):
    con = MagicMock()
    definition = RbacViewDef(allowed_columns=["*"], where_clause=predicate)
    with pytest.raises(ValueError):
        create_rbac_view(con, "base", "secured", definition)
    con.execute.assert_not_called()


def test_rbac_predicate_is_canonical_table_local_scalar_sql():
    con = duckdb.connect()
    try:
        con.execute(
            "CREATE TABLE base(id INTEGER, tenant VARCHAR, name VARCHAR);"
        )
        con.execute(
            "INSERT INTO base VALUES (1, 'acme', 'Alice'), (2, 'other', 'Alice')"
        )
        create_rbac_view(
            con,
            "base",
            "secured",
            RbacViewDef(
                allowed_columns=["id"],
                where_clause="tenant = 'acme' AND UPPER(name) = 'ALICE'",
            ),
        )
        assert con.execute("SELECT id FROM secured").fetchall() == [(1,)]
    finally:
        con.close()


def _local_reflection(tmp_path: Path):
    source = tmp_path / "server-private-source-name.parquet"
    pl.DataFrame({
        "id": [1, 2],
        "__rowid__": [1, 2],
        "__timestamp__": [1, 1],
    }).write_parquet(source)
    snapshot = SuperSnapshot(
        "lake",
        "cards",
        1,
        [str(source)],
        {"id", "__rowid__", "__timestamp__"},
        ["raw/cards/one"],
        column_types={
            "id": "Int64",
            "__rowid__": "Int64",
            "__timestamp__": "Int64",
        },
    )
    return source, Reflection("local", source.stat().st_size, 1, [snapshot])


def test_explain_analyze_rejected_without_returning_local_source_path(tmp_path):
    source, reflection = _local_reflection(tmp_path)
    engine = DuckDB()
    with pytest.raises(ValueError) as exc_info:
        engine.execute(
            reflection,
            SQLParser("lake", "SELECT id FROM lake.cards", "duckdb"),
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "profile.json"),
            ),
            lambda _event: None,
            explain=True,
            explain_options="ANALYZE",
        )
    assert str(source) not in str(exc_info.value)
    assert not (tmp_path / "profile.json").exists()


def test_plain_explain_rejects_signed_source_without_returning_bearer_url(tmp_path):
    signed = "https://user:password@example.test/data.parquet?signature=token"
    snapshot = SuperSnapshot(
        "lake", "cards", 1, [signed], {"id"}, ["raw/cards/one"],
        column_types={"id": "Int64"},
    )
    with pytest.raises(ValueError) as exc_info:
        DuckDB().execute(
            Reflection("http", 1, 1, [snapshot]),
            SQLParser("lake", "SELECT id FROM lake.cards", "duckdb"),
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "profile.json"),
            ),
            lambda _event: None,
            explain=True,
        )
    message = str(exc_info.value)
    assert "password" not in message
    assert "signature" not in message
    assert "token" not in message
    assert not (tmp_path / "profile.json").exists()


def test_plain_explain_rejects_signed_tombstone_without_returning_bearer_url(
    tmp_path,
):
    _source, base = _local_reflection(tmp_path)
    signed_dv = "https://example.test/dv.parquet?signature=dv-bearer-token"
    reflection = Reflection(
        base.storage_type,
        base.reflection_bytes,
        base.total_reflections,
        base.supers,
        tombstone_views={
            "cards": TombstoneDef(signed_dv, "raw/dv", 1),
        },
    )
    with pytest.raises(ValueError) as exc_info:
        DuckDB().execute(
            reflection,
            SQLParser("lake", "SELECT id FROM lake.cards", "duckdb"),
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "profile.json"),
            ),
            lambda _event: None,
            explain=True,
        )
    message = str(exc_info.value)
    assert "signature" not in message
    assert "dv-bearer-token" not in message
    assert not (tmp_path / "profile.json").exists()


def test_explain_rejects_rbac_policy_without_returning_hidden_literal(tmp_path):
    _source, reflection = _local_reflection(tmp_path)
    hidden_literal = "TOP_SECRET_POLICY_LITERAL"
    reflection.rbac_views = {
        "cards": RbacViewDef(
            allowed_columns=["id"],
            where_clause=f"tenant_secret = '{hidden_literal}'",
        ),
    }
    engine = DuckDB()
    with pytest.raises(ValueError) as exc_info:
        engine.execute(
            reflection,
            SQLParser("lake", "SELECT id FROM lake.cards", "duckdb"),
            SimpleNamespace(
                temp_dir=str(tmp_path),
                query_plan_path=str(tmp_path / "profile.json"),
            ),
            lambda _event: None,
            explain=True,
        )
    assert hidden_literal not in str(exc_info.value)
    assert "tenant_secret" not in str(exc_info.value)
    assert engine._con is None
    assert not (tmp_path / "profile.json").exists()


def test_user_duckdb_execution_does_not_persist_raw_profiles(tmp_path):
    _source, reflection = _local_reflection(tmp_path)
    profile = tmp_path / "profile.json"
    result = DuckDB().execute(
        reflection,
        SQLParser("lake", "SELECT id FROM lake.cards ORDER BY id", "duckdb"),
        SimpleNamespace(temp_dir=str(tmp_path), query_plan_path=str(profile)),
        lambda _event: None,
    )
    assert result["id"].tolist() == [1, 2]
    assert not profile.exists()


def test_public_error_redaction_removes_url_userinfo_query_and_fragment():
    raw = (
        "failed https://alice:password@example.test/path/data.parquet"
        "?signature=bearer-token#fragment"
    )
    redacted = redact_url_credentials(raw)
    assert "alice" not in redacted
    assert "password" not in redacted
    assert "bearer-token" not in redacted
    assert "fragment" not in redacted
    assert "example.test/path/data.parquet" in redacted


def _exception_chain_messages(exc):
    messages = []
    pending = [exc]
    seen = set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        messages.append(str(current))
        for linked in (current.__cause__, current.__context__):
            if isinstance(linked, BaseException):
                pending.append(linked)
    return "\n".join(messages)


def test_direct_duckdb_executor_redacts_backend_url_and_secret_causes(
    tmp_path, monkeypatch,
):
    signed = (
        "https://alice:password@example.test/path/data.parquet"
        "?signature=bearer-token#fragment"
    )
    snapshot = SuperSnapshot(
        "lake", "cards", 1, [signed], {"id"}, ["raw/cards/one"],
        column_types={"id": "Int64"},
    )
    engine = DuckDB()
    monkeypatch.setattr(engine, "_ensure_httpfs", lambda _con, _paths: None)

    def fail_scan(*_args, **_kwargs):
        try:
            raise RuntimeError("credential=deep-cause-secret")
        except RuntimeError as cause:
            raise duckdb.IOException(
                "IO Error in parquet_scan(['" + signed + "']) "
                "WHERE tenant_secret = 'TOP_SECRET_POLICY_LITERAL' "
                "token=echoed-token"
            ) from cause

    monkeypatch.setattr(
        duckdb_engine_module,
        "create_reflection_view_with_presign_retry",
        fail_scan,
    )
    try:
        with pytest.raises(RuntimeError) as exc_info:
            engine.execute(
                Reflection("http", 1, 1, [snapshot]),
                SQLParser("lake", "SELECT id FROM lake.cards", "duckdb"),
                SimpleNamespace(
                    temp_dir=str(tmp_path),
                    query_plan_path=str(tmp_path / "profile.json"),
                ),
                lambda _event: None,
            )
        public_chain = _exception_chain_messages(exc_info.value)
        assert str(exc_info.value) == "DuckDB managed query setup failed"
        for secret in (
            "alice", "password", "signature", "bearer-token", "fragment",
            "deep-cause-secret", "echoed-token",
            "tenant_secret", "TOP_SECRET_POLICY_LITERAL",
            "example.test/path/data.parquet", "parquet_scan",
        ):
            assert secret not in public_chain
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__suppress_context__ is True
        if exc_info.value.__context__ is not None:
            assert exc_info.value.__context__.__traceback__ is None
    finally:
        engine._reset_connection()


def test_late_duckdb_stream_error_uses_same_safe_boundary():
    signed = "https://user:pass@example.test/part.parquet?token=stream-secret"

    class FailingReader:
        def __iter__(self):
            return self

        def __next__(self):
            raise RuntimeError(f"stream failed at {signed}")

        def close(self):
            return None

    iterator = _DuckDBArrowBatchIterator(
        FailingReader(),
        MagicMock(),
        timed_out=threading.Event(),
        timeout_value=10,
    )
    with pytest.raises(RuntimeError) as exc_info:
        next(iterator)
    public_chain = _exception_chain_messages(exc_info.value)
    assert str(exc_info.value) == "DuckDB result stream failed"
    for secret in (
        "user", "pass", "token", "stream-secret",
        "example.test/part.parquet",
    ):
        assert secret not in public_chain
    assert exc_info.value.__cause__ is None


def test_data_reader_returns_clean_status_for_function_policy_rejection(monkeypatch):
    monkeypatch.setattr(data_reader_module, "get_storage", MagicMock())
    reader = DataReader(
        super_name="lake",
        organization="org",
        query=(
            "SELECT current_setting('s3_secret_access_key') "
            "FROM lake.cards"
        ),
    )
    frame, status, message = reader.execute(
        role_name="reader",
        engine=Engine.DUCKDB,
    )
    assert frame.empty
    assert status is Status.ERROR
    assert "current_setting" in message
