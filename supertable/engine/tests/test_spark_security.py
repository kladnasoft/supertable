"""Adversarial regressions for the Spark Thrift security boundary."""

from __future__ import annotations

import json
import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import sqlglot

import supertable.engine.spark_thrift as spark_thrift_module
from supertable.data_classes import Reflection, RbacViewDef, SuperSnapshot
from supertable.engine.spark_thrift import (
    SparkThriftExecutor,
    _configure_spark_s3,
    _configure_spark_session_security,
    _redact_spark_plan_text,
    _redact_spark_sensitive_text,
    _spark_create_parquet_view,
    _spark_create_rbac_view,
    _spark_create_tombstone_view,
    _spark_table_name,
    _revalidate_spark_parser,
    _spark_string_literal,
    _validate_spark_user_functions,
)
from supertable.redis_catalog import RedisCatalog
from supertable.utils.sql_parser import SQLParser


_STORAGE_CREDENTIAL_KEY_VARIANTS = [
    "s3_access_key",
    "s3_secret_key",
    "s3_session_token",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "S3_SECRET_KEY",
    "AWS_SECRET_ACCESS_KEY",
    "spark.hadoop.fs.s3a.secret.key",
    "fs.s3a.access.key",
    "fs.azure.account.key.account.dfs.core.windows.net",
    "google_application_credentials",
    "s3SecretKey",
    "awsAccessKeyId",
    "fs.gs.auth.service.account.json.keyfile",
]


class _IntVersionSubclass(int):
    pass


@pytest.mark.parametrize(
    "version",
    [
        True,
        False,
        1.0,
        "1); DROP VIEW protected_data; --",
        _IntVersionSubclass(1),
        -1,
        9_007_199_254_740_992,
    ],
)
def test_spark_table_name_rejects_non_exact_or_out_of_range_version(version):
    with pytest.raises(
        RuntimeError,
        match=r"^Spark snapshot version is invalid$",
    ):
        _spark_table_name("s", "t", version)


@pytest.mark.parametrize("version", [0, 9_007_199_254_740_991])
def test_spark_table_name_accepts_catalog_version_bounds(version):
    assert _spark_table_name("s", "t", version).endswith(f"_v{version}")


def test_malformed_snapshot_version_fails_before_cluster_connection_or_sql():
    payload = "1); DROP VIEW protected_data; --"
    snapshot = SuperSnapshot("s", "t", 1, [], {"id"})
    setattr(snapshot, "simple_version", payload)
    reflection = Reflection(
        storage_type="mock",
        reflection_bytes=1,
        total_reflections=1,
        supers=[snapshot],
    )
    cursor = MagicMock()
    connection = MagicMock()
    connection.cursor.return_value = cursor
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()
    executor._get_connection = MagicMock(return_value=connection)

    with pytest.raises(
        RuntimeError,
        match=r"^Spark snapshot version is invalid$",
    ) as error:
        executor.execute(reflection, MagicMock(), None, lambda _event: None)

    assert payload not in str(error.value)
    executor._select_cluster.assert_not_called()
    executor._get_connection.assert_not_called()
    cursor.execute.assert_not_called()


@pytest.mark.parametrize(
    "expression",
    [
        "reflect('java.lang.System', 'getenv', 'AWS_SECRET_ACCESS_KEY')",
        "try_reflect('java.lang.System', 'getenv', 'AWS_SECRET_ACCESS_KEY')",
        "java_method('java.lang.System', 'getenv', 'AWS_SECRET_ACCESS_KEY')",
        "input_file_name()",
        "input_file_block_start()",
        "input_file_block_length()",
        "current_user()",
        "version()",
        "cluster_installed_udf(id)",
        "evil.abs(id)",
        "repeat(name, 1000000000)",
        "space(1000000000)",
        "array_insert(array(1), 1000000000, 2)",
        "format_number(1, 1000000000)",
        "approx_count_distinct(id, 0.000000000001)",
        "approx_percentile(id, 0.5, 1000000000)",
        "percentile_approx(id, 0.5, 1000000000)",
        "sequence(1, 1000000000)",
        "explode(sequence(1, 1000000000))",
        "collect_list(name)",
        "array_repeat(name, 1000000000)",
        "from_json(name, 'a STRING')",
        "xpath(name, '//*')",
    ],
)
@pytest.mark.parametrize("dialect", ["duckdb", "spark"])
def test_untrusted_spark_functions_fail_closed(expression, dialect):
    query = f"SELECT {expression} FROM t"
    parser = SimpleNamespace(
        original_query=query,
        dialect=dialect,
        _parsed=sqlglot.parse_one(query, read=dialect),
    )
    with pytest.raises(ValueError, match="function is not allowed"):
        _validate_spark_user_functions(parser)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT '${env:AWS_SECRET_ACCESS_KEY}' FROM t",
        "SELECT '${system:user.home}' FROM t",
        "SELECT '${hiveconf:hive.metastore.uris}' FROM t",
        "SELECT '${sparkconf:spark.hadoop.fs.s3a.secret.key}' FROM t",
    ],
)
def test_variable_substitution_rejected_before_cluster_lookup(query):
    parser = SimpleNamespace(
        original_query=query,
        dialect="duckdb",
        _parsed=sqlglot.parse_one("SELECT id FROM t", read="duckdb"),
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()

    with pytest.raises(ValueError, match="variable substitution"):
        executor.execute(
            SimpleNamespace(reflection_bytes=1),
            parser,
            None,
            lambda _event: None,
        )

    executor._select_cluster.assert_not_called()


@pytest.mark.parametrize(
    "identity_expression",
    [
        "USER",
        "SESSION_USER",
        "SYSTEM_USER",
        "CURRENT_ROLE",
        "CURRENT_CATALOG",
        "CURRENT_DATABASE",
        "CURRENT_SCHEMA",
        "CURRENT_NAMESPACE",
        "CURRENT_VERSION",
        "VERSION",
    ],
)
def test_bare_spark_session_identity_rejected_before_cluster_lookup(
    identity_expression,
):
    query = f"SELECT {identity_expression} FROM t"
    parser = SimpleNamespace(
        original_query=query,
        dialect="spark",
        default_super_name="s",
        # Prove the backend reparses the original SQL instead of trusting a
        # caller-supplied benign tree.
        _parsed=sqlglot.parse_one("SELECT id FROM t", read="spark"),
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()

    with pytest.raises(ValueError, match="session identity expression"):
        executor.execute(
            Reflection(
                storage_type="mock",
                reflection_bytes=1,
                total_reflections=1,
                supers=[SuperSnapshot("s", "t", 1, [], {"id", "user"})],
            ),
            parser,
            None,
            lambda _event: None,
        )

    executor._select_cluster.assert_not_called()


def test_qualified_and_quoted_session_named_data_columns_remain_allowed():
    parser = SQLParser(
        "s",
        "SELECT t.user, `session_user`, t.localtimestamp, "
        "`localtimestamp` FROM t",
        "spark",
    )
    _validate_spark_user_functions(parser)


@pytest.mark.parametrize(
    "query",
    [
        "DELETE FROM t",
        "SELECT * FROM parquet.`s3://bucket/private.parquet`",
        "SELECT id FROM t; SET spark.sql.variable.substitute=true",
        "SELECT id FROM t FOR UPDATE",
        "SELECT id FROM denied",
    ],
)
def test_backend_reasserts_complete_query_policy_before_cluster_lookup(query):
    parser = SimpleNamespace(
        original_query=query,
        dialect="spark",
        default_super_name="s",
        _parsed=sqlglot.parse_one("SELECT id FROM t", read="spark"),
    )
    reflection = Reflection(
        storage_type="mock",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1, [], {"id"})],
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()

    with pytest.raises(ValueError):
        executor.execute(reflection, parser, None, lambda _event: None)

    executor._select_cluster.assert_not_called()


def test_backend_accepts_one_statement_with_trailing_semicolon_and_comment():
    parser = SimpleNamespace(
        original_query="SELECT id FROM t; -- trailing comment",
        dialect="spark",
        default_super_name="s",
        _parsed=sqlglot.parse_one("SELECT id FROM t", read="spark"),
    )
    reflection = Reflection(
        storage_type="mock",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1, [], {"id"})],
    )

    validated = _revalidate_spark_parser(parser, reflection)

    assert validated.get_table_tuples()[0].simple_name == "t"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT TRANSFORM (id, name) USING 'cat' AS (id, name) FROM t",
        "SELECT /*+ SET_VAR(spark.sql.shuffle.partitions=1) */ id FROM t",
    ],
)
def test_spark_scripts_and_hints_rejected_before_cluster_lookup(query):
    parser = SimpleNamespace(
        original_query=query,
        dialect="spark",
        _parsed=sqlglot.parse_one(query, read="spark"),
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()

    with pytest.raises(ValueError, match="scripts.*query hints"):
        executor.execute(
            SimpleNamespace(reflection_bytes=1),
            parser,
            None,
            lambda _event: None,
        )

    executor._select_cluster.assert_not_called()


def test_common_analytics_remain_allowed_for_auto_route():
    parser = SQLParser(
        "s",
        "SELECT COUNT(*), SUM(id), UPPER(name), DATE_TRUNC('day', ts), "
        "ROW_NUMBER() OVER (ORDER BY id), CAST(id AS STRING), "
        "COALESCE(id, 0), CASE WHEN id = 1 THEN 1 ELSE 0 END FROM t",
        "duckdb",
    )
    _validate_spark_user_functions(parser)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT to_timestamp_ntz('2026-01-01') FROM t",
        "SELECT make_timestamp_ntz(2026, 1, 1, 0, 0, 0) FROM t",
        "SELECT try_make_timestamp_ntz(2026, 1, 1, 0, 0, 0) FROM t",
        "SELECT CAST('2026-01-01' AS TIMESTAMP_NTZ) FROM t",
        "SELECT localtimestamp() FROM t",
        "SELECT localtimestamp FROM t",
    ],
)
def test_timestamp_ntz_surface_rejected_before_cluster_lookup(query):
    parser = SimpleNamespace(
        original_query=query,
        dialect="spark",
        default_super_name="s",
        # The backend must reparse the original text rather than trusting this
        # caller-supplied benign tree.
        _parsed=sqlglot.parse_one("SELECT id FROM t", read="spark"),
    )
    reflection = Reflection(
        storage_type="mock",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1, [], {"id"})],
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()

    with pytest.raises(ValueError, match="TIMESTAMP_NTZ"):
        executor.execute(reflection, parser, None, lambda _event: None)

    executor._select_cluster.assert_not_called()


def test_internal_bounded_collection_aggregate_rejects_spark_pre_cluster():
    parser = SQLParser(
        "s",
        "SELECT list(id) FROM (SELECT id FROM t LIMIT 10) bounded",
        "duckdb",
        allow_bounded_collection_aggregates=True,
    )
    reflection = Reflection(
        storage_type="mock",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1, [], {"id"})],
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()

    with pytest.raises(ValueError, match="require the DuckDB quality route"):
        executor.execute(reflection, parser, None, lambda _event: None)

    executor._select_cluster.assert_not_called()


@pytest.mark.parametrize(
    "predicate",
    [
        "reflect('java.lang.System', 'getenv', 'AWS_SECRET_ACCESS_KEY') = 'x'",
        "'${env:AWS_SECRET_ACCESS_KEY}' = 'x'",
        "EXISTS (SELECT 1 FROM denied_table)",
    ],
)
def test_rbac_predicates_use_same_spark_capability_fence(predicate):
    cursor = MagicMock()
    with pytest.raises(RuntimeError, match="protected Spark predicate"):
        _spark_create_rbac_view(
            cursor,
            "base",
            "secured",
            RbacViewDef(allowed_columns=["*"], where_clause=predicate),
        )
    cursor.execute.assert_not_called()


@pytest.mark.parametrize(
    "predicate",
    [
        "id = 1; SELECT 1",
        "COUNT(*) > 0",
        "ROW_NUMBER() OVER () = 1",
        "id = ?",
        "id = $tenant",
        "id = INTERVAL '1' DAY",
    ],
)
def test_rbac_predicate_is_exactly_one_closed_scalar_expression(predicate):
    cursor = MagicMock()

    with pytest.raises(RuntimeError, match="protected Spark predicate"):
        _spark_create_rbac_view(
            cursor,
            "base",
            "secured",
            RbacViewDef(allowed_columns=["*"], where_clause=predicate),
        )

    cursor.execute.assert_not_called()


def test_rbac_predicate_accepts_one_scalar_with_trailing_comment():
    cursor = MagicMock()
    _spark_create_rbac_view(
        cursor,
        "base",
        "secured",
        RbacViewDef(
            allowed_columns=["*"],
            where_clause="tenant_id = 7 AND active = TRUE; -- policy comment",
        ),
    )

    sql = cursor.execute.call_args.args[0]
    assert "tenant_id = 7 AND active = TRUE" in sql


def test_rbac_backend_error_never_echoes_protected_predicate_sql():
    sentinel = "TENANT-POLICY-SENTINEL"
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError(
        "failed CREATE VIEW with secret_col and " + sentinel
    )

    with pytest.raises(RuntimeError) as captured:
        _spark_create_rbac_view(
            cursor,
            "base",
            "secured",
            RbacViewDef(
                allowed_columns=["*"],
                where_clause=f"tenant_key = '{sentinel}'",
            ),
        )

    assert str(captured.value) == "Spark protected RBAC view creation failed"
    assert sentinel not in str(captured.value)
    assert "secret_col" not in str(captured.value)
    assert captured.value.__cause__ is None


def test_spark_protected_projection_strips_every_reserved_system_column():
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        ("id", "bigint"),
        ("__rowid__", "bigint"),
        ("__timestamp__", "timestamp"),
        ("__file__", "string"),
    ]

    _spark_create_tombstone_view(cursor, "source", "protected", None)

    projection = cursor.execute.call_args_list[-1].args[0]
    assert "src.`id`" in projection
    assert "__rowid__" not in projection
    assert "__timestamp__" not in projection
    assert "__file__" not in projection


_MANDATORY_SPARK_SESSION_SQL = [
    "SET spark.sql.variable.substitute=false",
    "SET spark.sql.session.timeZone=UTC",
    "SET spark.sql.timestampType=TIMESTAMP_LTZ",
    "SET spark.sql.parquet.inferTimestampNTZ.enabled=false",
]


def test_session_security_and_utc_ltz_contract_are_mandatory():
    cursor = MagicMock()
    _configure_spark_session_security(cursor)

    assert [
        item.args[0] for item in cursor.execute.call_args_list
    ] == _MANDATORY_SPARK_SESSION_SQL


@pytest.mark.parametrize("rejected_sql", _MANDATORY_SPARK_SESSION_SQL)
def test_mandatory_spark_session_setting_failure_is_fail_closed(rejected_sql):
    cursor = MagicMock()

    def execute(statement):
        if statement == rejected_sql:
            raise RuntimeError("server refused")

    cursor.execute.side_effect = execute
    with pytest.raises(RuntimeError, match="session security configuration"):
        _configure_spark_session_security(cursor)

    issued = [item.args[0] for item in cursor.execute.call_args_list]
    assert issued[-1] == rejected_sql
    assert issued == _MANDATORY_SPARK_SESSION_SQL[: len(issued)]


def test_spark_string_literal_round_trips_quote_and_backslash():
    path = "s3a://bucket/folder/a\\'b.parquet"
    rendered = _spark_string_literal(path, "test path")
    parsed = sqlglot.parse_one(f"SELECT {rendered}", read="spark")

    assert parsed.expressions[0].this == path
    assert "\\\\" in rendered
    assert "\\'" in rendered


def test_parquet_view_uses_spark_literal_renderer_for_metadata_path():
    path = "s3a://bucket/folder/a\\'b.parquet"
    cursor = MagicMock()
    cursor.fetchall.return_value = [("id", "bigint")]

    _spark_create_parquet_view(cursor, "tbl", [path])

    create_source = cursor.execute.call_args_list[0].args[0]
    assert _spark_string_literal(path, "test path") in create_source


@pytest.mark.parametrize(
    "path",
    [
        "s3a://bucket/line\nbreak.parquet",
        "s3a://bucket/tab\tname.parquet",
        "s3a://bucket/${env:AWS_SECRET_ACCESS_KEY}.parquet",
    ],
)
def test_parquet_metadata_control_or_substitution_fails_before_sql(path):
    cursor = MagicMock()

    with pytest.raises(RuntimeError):
        _spark_create_parquet_view(cursor, "tbl", [path])

    cursor.execute.assert_not_called()


def test_disabled_presigned_mode_fails_before_cluster_or_connection(monkeypatch):
    from supertable.engine import spark_thrift

    monkeypatch.setattr(
        spark_thrift,
        "settings",
        SimpleNamespace(SUPERTABLE_SPARK_PRESIGNED=True),
    )
    executor = SparkThriftExecutor.__new__(SparkThriftExecutor)
    executor._select_cluster = MagicMock()
    executor._get_connection = MagicMock()
    parser = SQLParser("s", "SELECT id FROM t", "spark")
    reflection = Reflection(
        storage_type="mock",
        reflection_bytes=1,
        total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1, [], {"id"})],
    )

    with pytest.raises(RuntimeError, match="SPARK_PRESIGNED is disabled"):
        executor.execute(reflection, parser, None, lambda _event: None)

    executor._select_cluster.assert_not_called()
    executor._get_connection.assert_not_called()


@pytest.mark.parametrize(
    "credential_key",
    _STORAGE_CREDENTIAL_KEY_VARIANTS,
)
def test_inline_storage_credentials_never_reach_spark_session(credential_key):
    cursor = MagicMock()
    with pytest.raises(RuntimeError, match="Inline Spark object-store credentials"):
        _configure_spark_s3(
            cursor,
            {credential_key: "DO-NOT-SEND", "s3_endpoint": "https://s3.example"},
        )
    cursor.execute.assert_not_called()


@pytest.mark.parametrize(
    "overrides",
    [
        {"s3_endpoint": "https://s3.example; SET x=y"},
        {"s3_endpoint": "https://s3.example/\nSET x=y"},
        {"s3_endpoint": "https://user:pass@s3.example"},
        {"s3_endpoint": "https://s3.example/?token=secret"},
        {"s3_region": "eu-west-1; SET x=y"},
        {"s3_use_ssl": "false; SET x=y"},
        {"s3_path_style": "true\nSET x=y"},
    ],
)
def test_non_secret_storage_settings_cannot_inject_sql(overrides):
    cursor = MagicMock()
    with pytest.raises(RuntimeError):
        _configure_spark_s3(cursor, overrides)
    cursor.execute.assert_not_called()


def test_storage_setting_logs_never_include_value_or_backend_echo(caplog):
    endpoint = "https://private-storage.example:9443"
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError(
        f"failed SQL SET spark.hadoop.fs.s3a.endpoint={endpoint}"
    )

    caplog.set_level(logging.DEBUG)
    with pytest.raises(RuntimeError, match="session configuration failed"):
        _configure_spark_s3(cursor, {"s3_endpoint": endpoint})

    assert endpoint not in caplog.text


def test_public_spark_error_redacts_url_userinfo_query_and_assignments():
    rendered = _redact_spark_sensitive_text(
        "GET https://alice:open-sesame@storage.example/PRIVATE_PATH_TOKEN "
        "and s3a://user:password@bucket.example/OBJECT_PATH_TOKEN"
        "?sig=SIGNED#FRAGMENT_TOKEN; "
        "spark.hadoop.fs.s3a.secret.key=STATIC-SECRET"
    )

    for secret in (
        "alice", "open-sesame", "password", "PRIVATE_PATH_TOKEN",
        "OBJECT_PATH_TOKEN", "SIGNED", "FRAGMENT_TOKEN", "STATIC-SECRET",
    ):
        assert secret not in rendered
    assert "storage.example" not in rendered
    assert "bucket.example" not in rendered
    assert rendered.startswith("Spark query failed; error_type=str;")
    assert "diagnostic_id=" in rendered
    assert "diagnostic_bytes=" in rendered


@pytest.mark.parametrize(
    "backend_detail, secret",
    [
        ("Authorization: Bearer SPARK_AUTH_SECRET", "SPARK_AUTH_SECRET"),
        ("Cookie: session=SPARK_COOKIE_SECRET", "SPARK_COOKIE_SECRET"),
        ("X-Api-Key: SPARK_API_SECRET", "SPARK_API_SECRET"),
        ('{"access_token":"SPARK_BODY_SECRET"}', "SPARK_BODY_SECRET"),
    ],
)
def test_spark_backend_diagnostic_never_preserves_header_or_body_secrets(
    backend_detail, secret,
):
    rendered = _redact_spark_sensitive_text(RuntimeError(backend_detail))

    assert secret not in rendered
    assert backend_detail not in rendered
    assert "error_type=RuntimeError" in rendered
    assert "diagnostic_id=" in rendered


def test_plan_redactor_removes_paths_and_literal_payloads():
    rendered = _redact_spark_plan_text(
        "FileScan parquet s3a://bucket/private/data.parquet "
        "https://user:pass@storage/object?sig=SIGNED "
        "spark.hadoop.fs.s3a.secret.key=STATIC filter='PRIVATE-LITERAL'"
    )
    for secret in (
        "s3a://",
        "bucket/private",
        "user",
        "pass",
        "SIGNED",
        "STATIC",
        "PRIVATE-LITERAL",
    ):
        assert secret not in rendered
    assert rendered == "<spark-plan-redacted>"


@pytest.mark.parametrize("credential_key", _STORAGE_CREDENTIAL_KEY_VARIANTS)
def test_cluster_registration_rejects_storage_credentials_before_redis(
    credential_key,
):
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = MagicMock()

    with pytest.raises(ValueError, match="Inline Spark object-store credentials"):
        catalog.register_spark_cluster(
            "org",
            "cluster",
            {
                "thrift_host": "spark.internal",
                credential_key: "DO-NOT-PERSIST",
            },
        )

    catalog.r.hset.assert_not_called()


def test_cluster_registration_keeps_thrift_auth_distinct_from_storage_keys():
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = MagicMock()
    catalog.register_spark_cluster(
        "org",
        "cluster",
        {"thrift_host": "spark.internal", "password": "thrift-transport"},
    )

    stored = json.loads(catalog.r.hset.call_args.args[2])
    assert stored["password"] == "thrift-transport"


def test_legacy_cluster_credentials_are_quarantined_and_never_returned():
    catalog = RedisCatalog.__new__(RedisCatalog)
    catalog.r = MagicMock()
    catalog.r.hgetall.return_value = {
        "cluster": json.dumps(
            {
                "cluster_id": "cluster",
                "status": "active",
                "thrift_host": "spark.internal",
                "spark.hadoop.fs.s3a.secret.key": "LEGACY-SECRET",
            }
        )
    }

    clusters = catalog.list_spark_clusters("org")
    assert clusters == [
        {
            "cluster_id": "cluster",
            "status": "offline",
            "thrift_host": "spark.internal",
            "security_error": "inline_object_store_credentials",
        }
    ]
    assert "LEGACY-SECRET" not in repr(clusters)


@patch.object(SparkThriftExecutor, "_select_cluster")
@patch.object(SparkThriftExecutor, "_get_connection")
@patch("supertable.engine.spark_thrift._configure_spark_s3")
@patch("supertable.engine.spark_thrift._spark_create_parquet_view", return_value=[])
@patch("supertable.engine.spark_thrift._spark_create_tombstone_view")
@patch(
    "supertable.engine.spark_thrift._spark_rewrite_query",
    return_value="SELECT id FROM protected_view",
)
def test_persisted_spark_plan_contains_no_source_or_secret(
    _rewrite,
    _tombstone,
    _view,
    _s3,
    get_connection,
    select_cluster,
    tmp_path,
):
    select_cluster.return_value = {
        "cluster_id": "c1",
        "thrift_host": "spark.internal",
    }
    cursor = MagicMock()
    cursor.description = [
        ("id", "BIGINT_TYPE", None, None, None, None, True),
    ]
    cursor.fetchall.side_effect = [
        [("id", "bigint")],
        [
            ("== Physical Plan ==",),
            (
                "FileScan parquet s3a://bucket/private/data.parquet "
                "https://user:pass@storage/object?sig=SIGNED "
                "spark.hadoop.fs.s3a.secret.key=STATIC "
                "filter='PRIVATE-LITERAL'",
            ),
        ],
        [(1,)],
    ]
    connection = MagicMock()
    connection.cursor.return_value = cursor
    get_connection.return_value = connection

    parser = SQLParser("s", "SELECT id FROM t", "spark")
    reflection = Reflection(
        storage_type="mock",
        reflection_bytes=1,
        total_reflections=1,
        supers=[
            SuperSnapshot(
                "s", "t", 1, ["s3://bucket/private/data.parquet"], {"id"}
            )
        ],
    )
    plan_path = tmp_path / "APP_HOME_TOKEN" / "SPARK_PLAN_TOKEN.json"
    executor = SparkThriftExecutor(storage=MagicMock(), organization="org")
    with patch.object(spark_thrift_module.logger, "debug") as debug_log:
        executor.execute(
            reflection,
            parser,
            SimpleNamespace(query_plan_path=str(plan_path)),
            lambda _event: None,
        )

    persisted = plan_path.read_text(encoding="utf-8")
    for secret in (
        "s3a://",
        "bucket/private",
        "user",
        "pass",
        "SIGNED",
        "STATIC",
        "PRIVATE-LITERAL",
    ):
        assert secret not in persisted
    assert "<spark-plan-redacted>" in persisted
    rendered_log_calls = repr(debug_log.call_args_list)
    assert "APP_HOME_TOKEN" not in rendered_log_calls
    assert "SPARK_PLAN_TOKEN" not in rendered_log_calls
    assert str(plan_path) not in rendered_log_calls
    assert "path_sha256=" in rendered_log_calls
