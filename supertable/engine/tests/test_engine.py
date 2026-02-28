# supertable/engine/tests/test_engine.py
"""
Comprehensive test suite for the supertable.engine package.

Covers:
  - engine_common: SQL helpers, table naming, env detection, presign,
                   reflection table creation, query rewriting, init_connection,
                   RBAC view creation
  - plan_stats:    PlanStats accumulation
  - data_estimator: get_missing_columns, DataEstimator.estimate
  - executor:      Engine enum, Executor routing (transient / pinned / spark / auto)
  - duckdb_transient: DuckDBTransient.execute
  - duckdb_pinned:    DuckDBPinned lifecycle, table caching, ref counting, staleness
  - spark_thrift:     SparkThriftExecutor helpers & execute flow
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from supertable.data_classes import (
    Reflection,
    RbacViewDef,
    SuperSnapshot,
    TableDefinition,
)
from supertable.engine.plan_stats import PlanStats
from supertable.engine.engine_common import (
    quote_if_needed,
    sanitize_sql_string,
    escape_parquet_path,
    hashed_table_name,
    pinned_table_name,
    normalize_endpoint_for_s3,
    detect_endpoint,
    detect_region,
    detect_url_style,
    detect_ssl,
    detect_creds,
    detect_bucket,
    url_to_key,
    make_presigned_list,
    rewrite_query_with_hashed_tables,
    init_connection,
    create_rbac_view,
    rbac_view_name,
    create_reflection_table,
    create_reflection_table_with_presign_retry,
    configure_httpfs_and_s3,
)
from supertable.engine.data_estimator import DataEstimator, get_missing_columns
from supertable.engine.executor import Engine, Executor, _get_pinned
from supertable.engine.duckdb_transient import DuckDBTransient
from supertable.engine.duckdb_pinned import DuckDBPinned, _PinnedEntry
from supertable.engine.spark_thrift import (
    _spark_table_name,
    _spark_create_parquet_view,
    _spark_create_rbac_view,
    _spark_rewrite_query,
    _configure_spark_s3,
    SparkThriftExecutor,
)


# ═══════════════════════════════════════════════════════════
#  plan_stats
# ═══════════════════════════════════════════════════════════


class TestPlanStats:

    def test_initial_state(self):
        ps = PlanStats()
        assert ps.stats == []

    def test_add_single_stat(self):
        ps = PlanStats()
        ps.add_stat({"ENGINE": "duckdb"})
        assert len(ps.stats) == 1
        assert ps.stats[0] == {"ENGINE": "duckdb"}

    def test_add_multiple_stats(self):
        ps = PlanStats()
        ps.add_stat({"A": 1})
        ps.add_stat({"B": 2})
        ps.add_stat({"C": 3})
        assert len(ps.stats) == 3
        assert ps.stats[1] == {"B": 2}

    def test_add_non_dict_stat(self):
        ps = PlanStats()
        ps.add_stat("raw_string")
        ps.add_stat(42)
        assert ps.stats == ["raw_string", 42]


# ═══════════════════════════════════════════════════════════
#  engine_common — SQL helpers
# ═══════════════════════════════════════════════════════════


class TestQuoteIfNeeded:

    def test_star_passthrough(self):
        assert quote_if_needed("*") == "*"

    def test_simple_column(self):
        assert quote_if_needed("age") == "age"

    def test_column_with_underscore(self):
        assert quote_if_needed("first_name") == "first_name"

    def test_column_with_space(self):
        assert quote_if_needed("first name") == '"first name"'

    def test_column_with_dash(self):
        assert quote_if_needed("my-col") == '"my-col"'

    def test_column_with_embedded_quotes(self):
        assert quote_if_needed('col"x') == '"col""x"'

    def test_strips_whitespace(self):
        assert quote_if_needed("  age  ") == "age"

    def test_numeric_column(self):
        assert quote_if_needed("123") == "123"

    def test_empty_string(self):
        assert quote_if_needed("") == ""


class TestSanitizeSqlString:

    def test_quoted_string(self):
        assert sanitize_sql_string("'hello'") == "'hello'"

    def test_quoted_string_with_internal_quotes(self):
        assert sanitize_sql_string("'it's'") == "'it''s'"

    def test_unquoted_passthrough(self):
        assert sanitize_sql_string("TRUE") == "TRUE"

    def test_numeric_passthrough(self):
        assert sanitize_sql_string("42") == "42"


class TestEscapeParquetPath:

    def test_no_quotes(self):
        assert escape_parquet_path("/data/file.parquet") == "/data/file.parquet"

    def test_single_quote(self):
        assert escape_parquet_path("file'name.parquet") == "file''name.parquet"

    def test_multiple_quotes(self):
        assert escape_parquet_path("a'b'c") == "a''b''c"


# ═══════════════════════════════════════════════════════════
#  engine_common — Table naming
# ═══════════════════════════════════════════════════════════


class TestHashedTableName:

    def test_deterministic(self):
        n1 = hashed_table_name("super", "simple", 1, ["a", "b"])
        n2 = hashed_table_name("super", "simple", 1, ["a", "b"])
        assert n1 == n2

    def test_prefix(self):
        assert hashed_table_name("s", "t", 1).startswith("st_")

    def test_columns_sorted(self):
        n1 = hashed_table_name("s", "t", 1, ["b", "a"])
        n2 = hashed_table_name("s", "t", 1, ["a", "b"])
        assert n1 == n2

    def test_no_columns_uses_star(self):
        n1 = hashed_table_name("s", "t", 1, None)
        n2 = hashed_table_name("s", "t", 1, [])
        assert n1 == n2

    def test_different_version_different_name(self):
        assert hashed_table_name("s", "t", 1) != hashed_table_name("s", "t", 2)

    def test_different_columns_different_name(self):
        assert hashed_table_name("s", "t", 1, ["a"]) != hashed_table_name("s", "t", 1, ["b"])

    def test_hash_length(self):
        name = hashed_table_name("super", "simple", 99, ["x"])
        assert len(name) == 3 + 16  # "st_" + 16 hex


class TestPinnedTableName:

    def test_deterministic(self):
        assert pinned_table_name("s", "t", 5) == pinned_table_name("s", "t", 5)

    def test_prefix_and_version_suffix(self):
        name = pinned_table_name("s", "t", 7)
        assert name.startswith("pin_")
        assert name.endswith("_v7")

    def test_different_version_different_name(self):
        assert pinned_table_name("s", "t", 1) != pinned_table_name("s", "t", 2)

    def test_same_key_different_version_share_prefix(self):
        n1 = pinned_table_name("s", "t", 1)
        n2 = pinned_table_name("s", "t", 2)
        assert n1.rsplit("_v", 1)[0] == n2.rsplit("_v", 1)[0]


# ═══════════════════════════════════════════════════════════
#  engine_common — env detection
# ═══════════════════════════════════════════════════════════


class TestNormalizeEndpointForS3:

    def test_empty(self):
        assert normalize_endpoint_for_s3("") == ""

    def test_with_scheme(self):
        assert normalize_endpoint_for_s3("http://minio:9000") == "minio:9000"

    def test_without_scheme(self):
        assert normalize_endpoint_for_s3("minio:9000") == "minio:9000"

    def test_https(self):
        assert normalize_endpoint_for_s3("https://s3.amazonaws.com") == "s3.amazonaws.com"

    def test_no_port(self):
        assert normalize_endpoint_for_s3("http://localhost") == "localhost"


class TestDetectEndpoint:

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("STORAGE_ENDPOINT_URL", "http://minio:9000")
        assert detect_endpoint() == "minio:9000"

    def test_missing(self, monkeypatch, clean_env):
        assert detect_endpoint() is None


class TestDetectRegion:

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("STORAGE_REGION", "eu-west-1")
        assert detect_region() == "eu-west-1"

    def test_default(self, monkeypatch, clean_env):
        assert detect_region() == "us-east-1"


class TestDetectUrlStyle:

    def test_path_style(self, monkeypatch):
        monkeypatch.setenv("STORAGE_FORCE_PATH_STYLE", "true")
        assert detect_url_style() == "path"

    def test_vhost_style(self, monkeypatch):
        monkeypatch.setenv("STORAGE_FORCE_PATH_STYLE", "false")
        assert detect_url_style() == "vhost"

    def test_default_is_path(self, monkeypatch, clean_env):
        assert detect_url_style() == "path"


class TestDetectSsl:

    def test_true(self, monkeypatch):
        monkeypatch.setenv("STORAGE_USE_SSL", "true")
        assert detect_ssl() is True

    def test_one(self, monkeypatch):
        monkeypatch.setenv("STORAGE_USE_SSL", "1")
        assert detect_ssl() is True

    def test_empty(self, monkeypatch):
        monkeypatch.setenv("STORAGE_USE_SSL", "")
        assert detect_ssl() is False

    def test_missing(self, monkeypatch, clean_env):
        assert detect_ssl() is False


class TestDetectCreds:

    def test_all_present(self, monkeypatch):
        monkeypatch.setenv("STORAGE_ACCESS_KEY", "ak")
        monkeypatch.setenv("STORAGE_SECRET_KEY", "sk")
        monkeypatch.setenv("STORAGE_SESSION_TOKEN", "st")
        ak, sk, st = detect_creds()
        assert (ak, sk, st) == ("ak", "sk", "st")

    def test_all_missing(self, monkeypatch, clean_env):
        ak, sk, st = detect_creds()
        assert ak is None and sk is None and st is None


class TestDetectBucket:

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("STORAGE_BUCKET", "my-bucket")
        assert detect_bucket() == "my-bucket"

    def test_missing(self, monkeypatch, clean_env):
        assert detect_bucket() is None


# ═══════════════════════════════════════════════════════════
#  engine_common — url_to_key / presign
# ═══════════════════════════════════════════════════════════


class TestUrlToKey:

    def test_s3_url(self):
        assert url_to_key("s3://bucket/path/file.parquet", "bucket") == "path/file.parquet"

    def test_s3_url_no_bucket(self):
        assert url_to_key("s3://bucket/key", None) == "key"

    def test_http_path_style(self):
        assert url_to_key("http://minio:9000/mybucket/data/f.parquet", "mybucket") == "data/f.parquet"

    def test_http_vhost_style(self):
        assert url_to_key("https://mybucket.s3.amazonaws.com/data/f.parquet", "mybucket") == "data/f.parquet"

    def test_http_no_bucket(self):
        assert url_to_key("http://host/some/path", None) == "some/path"

    def test_file_scheme_returns_none(self):
        assert url_to_key("file:///tmp/data.parquet", None) is None

    def test_bare_path_returns_none(self):
        assert url_to_key("/local/path/file.parquet", None) is None


class TestMakePresignedList:

    def test_no_storage(self):
        paths = ["s3://bucket/a", "s3://bucket/b"]
        assert make_presigned_list(None, paths) == paths

    def test_no_presign_method(self):
        storage = MagicMock(spec=[])
        assert make_presigned_list(storage, ["s3://bucket/a"]) == ["s3://bucket/a"]

    def test_presign_success(self, monkeypatch):
        monkeypatch.setenv("STORAGE_BUCKET", "bucket")
        storage = MagicMock()
        storage.presign.side_effect = lambda key: f"https://presigned/{key}"
        paths = ["s3://bucket/data/f1.parquet", "s3://bucket/data/f2.parquet"]
        assert make_presigned_list(storage, paths) == [
            "https://presigned/data/f1.parquet",
            "https://presigned/data/f2.parquet",
        ]

    def test_presign_partial_failure(self, monkeypatch):
        monkeypatch.setenv("STORAGE_BUCKET", "bucket")
        storage = MagicMock()
        storage.presign.side_effect = ["https://presigned/a", Exception("network error")]
        paths = ["s3://bucket/a", "s3://bucket/b"]
        result = make_presigned_list(storage, paths)
        assert result[0] == "https://presigned/a"
        assert result[1] == "s3://bucket/b"

    def test_non_url_paths_pass_through(self):
        storage = MagicMock()
        storage.presign.return_value = "https://signed"
        assert make_presigned_list(storage, ["/local/file.parquet"]) == ["/local/file.parquet"]


# ═══════════════════════════════════════════════════════════
#  engine_common — rewrite_query_with_hashed_tables
# ═══════════════════════════════════════════════════════════


class TestRewriteQuery:

    def test_empty_alias_map_returns_original(self):
        sql = "SELECT * FROM t"
        assert rewrite_query_with_hashed_tables(sql, {}) == sql

    def test_with_aliases_rewrites(self):
        result = rewrite_query_with_hashed_tables(
            "SELECT * FROM my_table AS t", {"t": "st_abc123"}
        )
        assert "st_abc123" in result

    def test_unaliased_table_rewritten_by_name(self):
        result = rewrite_query_with_hashed_tables(
            "SELECT * FROM orders", {"orders": "st_xyz"}
        )
        assert "st_xyz" in result

    def test_unknown_alias_untouched(self):
        result = rewrite_query_with_hashed_tables(
            "SELECT * FROM keep_me", {"other": "st_abc"}
        )
        assert "keep_me" in result


# ═══════════════════════════════════════════════════════════
#  engine_common — init_connection
# ═══════════════════════════════════════════════════════════


class TestInitConnection:

    def test_basic_pragmas(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        # Verify connection is usable after init
        result = duckdb_con.execute("SELECT 1 AS val").fetchdf()
        assert result["val"][0] == 1

    def test_with_profile_path(self, duckdb_con, tmp_path):
        profile = str(tmp_path / "profile.json")
        init_connection(duckdb_con, temp_dir=str(tmp_path), profile_path=profile)
        result = duckdb_con.execute("SELECT 1 AS val").fetchdf()
        assert result["val"][0] == 1

    def test_custom_memory_limit(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path), memory_limit="512MB")
        result = duckdb_con.execute("SELECT 1 AS val").fetchdf()
        assert result["val"][0] == 1

    def test_threads_env(self, duckdb_con, tmp_path, monkeypatch):
        monkeypatch.setenv("SUPERTABLE_DUCKDB_THREADS", "2")
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        result = duckdb_con.execute("SELECT 1 AS val").fetchdf()
        assert result["val"][0] == 1


# ═══════════════════════════════════════════════════════════
#  engine_common — RBAC views
# ═══════════════════════════════════════════════════════════


class TestRbacViewName:

    def test_prefix(self):
        assert rbac_view_name("st_abc123") == "rbac_st_abc123"


class TestCreateRbacView:

    def test_all_columns_no_where(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        duckdb_con.execute("CREATE TABLE base_t (id INT, name VARCHAR)")
        duckdb_con.execute("INSERT INTO base_t VALUES (1, 'a'), (2, 'b')")
        view_def = RbacViewDef(allowed_columns=["*"], where_clause="")
        create_rbac_view(duckdb_con, "base_t", "rbac_v", view_def)
        df = duckdb_con.execute("SELECT * FROM rbac_v").fetchdf()
        assert len(df) == 2

    def test_specific_columns_with_where(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        duckdb_con.execute("CREATE TABLE base_t2 (id INT, name VARCHAR, org INT)")
        duckdb_con.execute("INSERT INTO base_t2 VALUES (1, 'a', 5), (2, 'b', 6)")
        view_def = RbacViewDef(allowed_columns=["id", "name"], where_clause="org = 5")
        create_rbac_view(duckdb_con, "base_t2", "rbac_v2", view_def)
        df = duckdb_con.execute("SELECT * FROM rbac_v2").fetchdf()
        assert len(df) == 1
        assert list(df.columns) == ["id", "name"]


# ═══════════════════════════════════════════════════════════
#  engine_common — create_reflection_table
# ═══════════════════════════════════════════════════════════


class TestCreateReflectionTable:

    def test_no_files_raises(self, duckdb_con):
        with pytest.raises(ValueError, match="No files"):
            create_reflection_table(duckdb_con, "tbl", [])

    def test_sql_structure_with_mock(self):
        con = MagicMock()
        create_reflection_table(con, "tbl", ["/data/f.parquet"])
        sql = con.execute.call_args[0][0]
        assert "CREATE TABLE tbl" in sql
        assert "SELECT *" in sql
        assert "parquet_scan" in sql

    def test_multiple_files_in_sql(self):
        con = MagicMock()
        create_reflection_table(con, "tbl", ["/a.parquet", "/b.parquet"])
        sql = con.execute.call_args[0][0]
        assert "/a.parquet" in sql
        assert "/b.parquet" in sql

    def test_specific_columns_in_sql(self):
        con = MagicMock()
        create_reflection_table(con, "tbl", ["/f.parquet"], columns=["id", "name"])
        sql = con.execute.call_args[0][0]
        assert "id" in sql
        assert "name" in sql
        assert "SELECT *" not in sql

    def test_escapes_quotes_in_paths(self):
        con = MagicMock()
        create_reflection_table(con, "tbl", ["path'with'quotes.parquet"])
        sql = con.execute.call_args[0][0]
        assert "path''with''quotes.parquet" in sql


class TestCreateReflectionTableWithPresignRetry:

    @patch("supertable.engine.engine_common.configure_httpfs_and_s3")
    @patch("supertable.engine.engine_common.create_reflection_table")
    def test_success_no_retry(self, mock_create, mock_httpfs):
        result = create_reflection_table_with_presign_retry(
            MagicMock(), None, "tbl", ["/f.parquet"]
        )
        assert result is False
        mock_create.assert_called_once()

    @patch("supertable.engine.engine_common.configure_httpfs_and_s3")
    @patch("supertable.engine.engine_common.create_reflection_table")
    @patch("supertable.engine.engine_common.make_presigned_list")
    def test_http_error_triggers_presign_retry(self, mock_presign, mock_create, mock_httpfs):
        mock_create.side_effect = [Exception("HTTP Error 403 AccessDenied"), None]
        mock_presign.return_value = ["https://presigned/f.parquet"]
        result = create_reflection_table_with_presign_retry(
            MagicMock(), MagicMock(), "tbl", ["/f.parquet"]
        )
        assert result is True
        assert mock_create.call_count == 2
        mock_presign.assert_called_once()

    @patch("supertable.engine.engine_common.configure_httpfs_and_s3")
    @patch("supertable.engine.engine_common.create_reflection_table")
    def test_non_http_error_propagates(self, mock_create, mock_httpfs):
        mock_create.side_effect = RuntimeError("disk full")
        with pytest.raises(RuntimeError, match="disk full"):
            create_reflection_table_with_presign_retry(MagicMock(), None, "tbl", ["/f.parquet"])

    @patch("supertable.engine.engine_common.configure_httpfs_and_s3")
    @patch("supertable.engine.engine_common.create_reflection_table")
    @patch("supertable.engine.engine_common.make_presigned_list")
    def test_301_triggers_retry(self, mock_presign, mock_create, mock_httpfs):
        mock_create.side_effect = [Exception("301 Moved Permanently"), None]
        mock_presign.return_value = ["https://presigned/f.parquet"]
        result = create_reflection_table_with_presign_retry(
            MagicMock(), MagicMock(), "tbl", ["/f.parquet"]
        )
        assert result is True


# ═══════════════════════════════════════════════════════════
#  engine_common — configure_httpfs_and_s3
# ═══════════════════════════════════════════════════════════


class TestConfigureHttpfsAndS3:

    def test_empty_paths_skips(self):
        con = MagicMock()
        configure_httpfs_and_s3(con, [])
        con.execute.assert_not_called()

    def test_local_paths_installs_httpfs_but_no_s3_config(self):
        con = MagicMock()
        configure_httpfs_and_s3(con, ["/local/file.parquet"])
        calls = [str(c) for c in con.execute.call_args_list]
        assert any("INSTALL httpfs" in s for s in calls)
        assert not any("s3_endpoint" in s for s in calls)


# ═══════════════════════════════════════════════════════════
#  data_estimator — get_missing_columns
# ═══════════════════════════════════════════════════════════


def _snap(super_name, simple_name, cols):
    return SuperSnapshot(
        super_name=super_name, simple_name=simple_name,
        simple_version=1, files=["f.parquet"], columns=set(cols),
    )


def _tdef(super_name, simple_name, cols):
    return TableDefinition(
        super_name=super_name, simple_name=simple_name,
        alias=f"{super_name}.{simple_name}", columns=cols,
    )


class TestGetMissingColumns:

    def test_no_tables(self):
        assert get_missing_columns([], []) == []

    def test_select_star_skipped(self):
        assert get_missing_columns([_tdef("s", "t", [])], [_snap("s", "t", ["a"])]) == []

    def test_all_columns_present(self):
        assert get_missing_columns(
            [_tdef("s", "t", ["a", "b"])], [_snap("s", "t", ["a", "b", "c"])]
        ) == []

    def test_missing_column(self):
        result = get_missing_columns(
            [_tdef("s", "t", ["a", "b", "missing"])], [_snap("s", "t", ["a", "b"])]
        )
        assert len(result) == 1
        assert result[0][0] == "s"
        assert result[0][1] == "t"
        assert "missing" in result[0][2]

    def test_case_insensitive(self):
        assert get_missing_columns([_tdef("S", "T", ["ID"])], [_snap("s", "t", ["id"])]) == []

    def test_no_snapshot_all_missing(self):
        result = get_missing_columns([_tdef("s", "t", ["a", "b"])], [])
        assert len(result) == 1
        assert result[0][2] == {"a", "b"}

    def test_multiple_snapshots_union_columns(self):
        assert get_missing_columns(
            [_tdef("s", "t", ["a", "b"])],
            [_snap("s", "t", ["a"]), _snap("s", "t", ["b"])],
        ) == []

    def test_multiple_tables_mixed_results(self):
        result = get_missing_columns(
            [_tdef("s", "t1", ["a"]), _tdef("s", "t2", ["x", "y"])],
            [_snap("s", "t1", ["a"]), _snap("s", "t2", ["x"])],
        )
        assert len(result) == 1
        assert result[0][1] == "t2"
        assert "y" in result[0][2]


# ═══════════════════════════════════════════════════════════
#  data_estimator — DataEstimator
# ═══════════════════════════════════════════════════════════


def _make_estimator(tables, storage=None):
    storage = storage or MagicMock()
    est = DataEstimator("org1", storage, tables)
    est.catalog = MagicMock()
    return est


class TestDataEstimator:

    def test_schema_to_dict_from_dict(self):
        assert _make_estimator([])._schema_to_dict({"a": "int"}) == {"a": "int"}

    def test_schema_to_dict_from_list(self):
        result = _make_estimator([])._schema_to_dict([{"name": "a", "type": "int"}, {"name": "b"}])
        assert result == {"a": "int", "b": ""}

    def test_schema_to_dict_from_bad_input(self):
        est = _make_estimator([])
        assert est._schema_to_dict("garbage") == {}
        assert est._schema_to_dict(42) == {}
        assert est._schema_to_dict(None) == {}

    def test_get_supertable_map(self):
        tables = [
            TableDefinition("s1", "t1", "a1"),
            TableDefinition("s1", "t2", "a2"),
            TableDefinition("s2", "t3", "a3"),
        ]
        result = dict(_make_estimator(tables)._get_supertable_map())
        assert sorted(result["s1"]) == ["t1", "t2"]
        assert result["s2"] == ["t3"]

    def test_filter_snapshots_specific_table(self):
        snapshots = [{"table_name": "t1", "path": "p1"}, {"table_name": "t2", "path": "p2"}]
        result = _make_estimator([])._filter_snapshots("super", "t1", snapshots)
        assert len(result) == 1
        assert result[0]["table_name"] == "t1"

    def test_filter_snapshots_wildcard_excludes_dunder(self):
        snapshots = [
            {"table_name": "t1", "path": "p1"},
            {"table_name": "__meta__", "path": "p2"},
        ]
        result = _make_estimator([])._filter_snapshots("super", "super", snapshots)
        assert len(result) == 1
        assert result[0]["table_name"] == "t1"

    def test_estimate_no_snapshots_raises(self):
        est = _make_estimator([TableDefinition("s", "t", "a", columns=["x"])])
        est.catalog.scan_leaf_items.return_value = []
        with pytest.raises(RuntimeError, match="No snapshots"):
            est.estimate()

    def test_estimate_missing_columns_raises(self):
        est = _make_estimator([TableDefinition("s", "t", "a", columns=["missing_col"])])
        est.catalog.scan_leaf_items.return_value = [
            {"simple": "t", "path": "/snap.json", "version": 1, "ts": 0,
             "payload": {
                 "snapshot_version": 1,
                 "schema": {"existing_col": "string"},
                 "resources": [{"file": "/data.parquet", "file_size": 100}],
             }},
        ]
        with pytest.raises(RuntimeError, match="Missing required column"):
            est.estimate()

    def test_estimate_success(self):
        est = _make_estimator([TableDefinition("s", "t", "a", columns=["col_a"])])
        est.catalog.scan_leaf_items.return_value = [
            {"simple": "t", "path": "/snap.json", "version": 1, "ts": 0,
             "payload": {
                 "snapshot_version": 1,
                 "schema": {"col_a": "string"},
                 "resources": [{"file": "/data.parquet", "file_size": 500}],
             }},
        ]
        reflection = est.estimate()
        assert isinstance(reflection, Reflection)
        assert reflection.reflection_bytes == 500
        assert reflection.total_reflections == 1
        assert len(reflection.supers) == 1

    def test_estimate_no_files_raises(self):
        est = _make_estimator([TableDefinition("s", "t", "a", columns=["col_a"])])
        est.catalog.scan_leaf_items.return_value = [
            {"simple": "t", "path": "/snap.json", "version": 1, "ts": 0,
             "payload": {
                 "snapshot_version": 1,
                 "schema": {"col_a": "string"},
                 "resources": [],
             }},
        ]
        with pytest.raises(RuntimeError, match="No parquet files"):
            est.estimate()

    def test_to_duckdb_path_empty(self):
        assert _make_estimator([])._to_duckdb_path("") == ""

    def test_to_duckdb_path_already_url(self):
        assert _make_estimator([])._to_duckdb_path("s3://bucket/key") == "s3://bucket/key"

    def test_to_duckdb_path_presign(self, monkeypatch):
        monkeypatch.setenv("SUPERTABLE_DUCKDB_PRESIGNED", "1")
        storage = MagicMock()
        storage.presign.return_value = "https://presigned/key"
        assert _make_estimator([], storage)._to_duckdb_path("some/key") == "https://presigned/key"

    def test_to_duckdb_path_presign_failure_falls_back(self, monkeypatch):
        monkeypatch.setenv("SUPERTABLE_DUCKDB_PRESIGNED", "1")
        storage = MagicMock()
        storage.presign.side_effect = Exception("fail")
        # Falls through to further resolution attempts
        result = _make_estimator([], storage)._to_duckdb_path("some/key")
        assert isinstance(result, str)

    def test_to_duckdb_path_storage_helper(self):
        storage = MagicMock(spec=["to_duckdb_path"])
        storage.to_duckdb_path.return_value = "s3://resolved/key"
        assert _make_estimator([], storage)._to_duckdb_path("some/key") == "s3://resolved/key"

    def test_to_duckdb_path_http_construction(self, monkeypatch, clean_env):
        monkeypatch.setenv("STORAGE_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("STORAGE_BUCKET", "bkt")
        monkeypatch.setenv("SUPERTABLE_DUCKDB_USE_HTTPFS", "1")
        storage = MagicMock(spec=[])
        result = _make_estimator([], storage)._to_duckdb_path("data/file.parquet")
        assert result == "http://minio:9000/bkt/data/file.parquet"

    def test_to_duckdb_path_https_when_ssl(self, monkeypatch, clean_env):
        monkeypatch.setenv("STORAGE_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("STORAGE_BUCKET", "bkt")
        monkeypatch.setenv("STORAGE_USE_SSL", "true")
        monkeypatch.setenv("SUPERTABLE_DUCKDB_USE_HTTPFS", "1")
        storage = MagicMock(spec=[])
        assert _make_estimator([], storage)._to_duckdb_path("data/f.parquet").startswith("https://")

    def test_to_duckdb_path_s3_when_not_httpfs(self, monkeypatch, clean_env):
        monkeypatch.setenv("STORAGE_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("STORAGE_BUCKET", "bkt")
        storage = MagicMock(spec=[])
        assert _make_estimator([], storage)._to_duckdb_path("data/f.parquet") == "s3://bkt/data/f.parquet"

    def test_to_duckdb_path_fallback(self, clean_env):
        storage = MagicMock(spec=[])
        assert _make_estimator([], storage)._to_duckdb_path("bare/key") == "bare/key"

    def test_detect_endpoint_from_storage(self):
        storage = MagicMock()
        storage.endpoint_url = "http://minio:9000"
        assert _make_estimator([], storage)._detect_endpoint() == "minio:9000"

    def test_detect_endpoint_from_host_port(self):
        storage = MagicMock(spec=["host", "port"])
        storage.host = "myhost"
        storage.port = "9000"
        result = _make_estimator([], storage)._detect_endpoint()
        assert result == "myhost:9000"

    def test_detect_bucket_from_storage(self):
        storage = MagicMock()
        storage.bucket = "my-bucket"
        assert _make_estimator([], storage)._detect_bucket() == "my-bucket"

    def test_detect_ssl_from_storage(self):
        storage = MagicMock()
        storage.secure = True
        assert _make_estimator([], storage)._detect_ssl() is True


# ═══════════════════════════════════════════════════════════
#  executor — Engine enum
# ═══════════════════════════════════════════════════════════


class TestEngineEnum:

    def test_values(self):
        assert Engine.AUTO.value == "auto"
        assert Engine.DUCKDB_TRANSIENT.value == "duckdb_transient"
        assert Engine.DUCKDB_PINNED.value == "duckdb_pinned"
        assert Engine.SPARK.value == "spark"

    def test_backward_compat_alias(self):
        assert Engine.DUCKDB.value == "duckdb_pinned"

    def test_from_string(self):
        assert Engine("auto") == Engine.AUTO
        assert Engine("spark") == Engine.SPARK


# ═══════════════════════════════════════════════════════════
#  executor — Executor routing
# ═══════════════════════════════════════════════════════════


def _exec_fixtures():
    reflection = Reflection(
        storage_type="mock", reflection_bytes=100, total_reflections=1,
        supers=[SuperSnapshot("s", "t", 1, ["f.parquet"], {"col"})],
    )
    parser = MagicMock()
    parser.original_query = "SELECT * FROM t"
    qm = MagicMock()
    qm.temp_dir = "/tmp"
    qm.query_plan_path = "/tmp/plan.json"
    timer = MagicMock()
    ps = PlanStats()
    return reflection, parser, qm, timer, ps


class TestExecutor:

    @patch.object(DuckDBTransient, "execute", return_value=pd.DataFrame({"a": [1]}))
    def test_route_transient(self, mock_exec):
        r, p, qm, t, ps = _exec_fixtures()
        _, used = Executor().execute(Engine.DUCKDB_TRANSIENT, r, p, qm, t, ps, "test")
        assert used == "duckdb_transient"
        mock_exec.assert_called_once()

    @patch.object(DuckDBPinned, "execute", return_value=pd.DataFrame({"a": [1]}))
    def test_route_pinned(self, mock_exec):
        import supertable.engine.executor as mod
        mod._pinned_singleton = None
        _, used = Executor().execute(Engine.DUCKDB_PINNED, *_exec_fixtures()[:-1], PlanStats(), "test")
        assert used == "duckdb_pinned"
        mock_exec.assert_called_once()

    @patch.object(DuckDBPinned, "execute", return_value=pd.DataFrame())
    def test_route_duckdb_alias(self, mock_exec):
        import supertable.engine.executor as mod
        mod._pinned_singleton = None
        _, used = Executor().execute(Engine.DUCKDB, *_exec_fixtures()[:-1], PlanStats(), "test")
        assert used == "duckdb_pinned"

    @patch("supertable.engine.spark_thrift.SparkThriftExecutor")
    def test_route_spark(self, MockSpark):
        instance = MagicMock()
        instance.execute.return_value = pd.DataFrame()
        MockSpark.return_value = instance
        r, p, qm, t, ps = _exec_fixtures()
        _, used = Executor().execute(Engine.SPARK, r, p, qm, t, ps, "test")
        assert used == "spark_thrift"
        _, kwargs = instance.execute.call_args
        assert kwargs.get("force") is True

    @patch.object(DuckDBPinned, "execute", return_value=pd.DataFrame())
    def test_auto_picks_pinned_for_small_data(self, mock_exec):
        import supertable.engine.executor as mod
        mod._pinned_singleton = None
        r, p, qm, t, ps = _exec_fixtures()
        r.reflection_bytes = 1000
        _, used = Executor().execute(Engine.AUTO, r, p, qm, t, ps, "test")
        assert used == "duckdb_pinned"

    @patch.object(DuckDBTransient, "execute", return_value=pd.DataFrame())
    def test_plan_stats_records_engine(self, mock_exec):
        r, p, qm, t, ps = _exec_fixtures()
        Executor().execute(Engine.DUCKDB_TRANSIENT, r, p, qm, t, ps, "test")
        assert any(s.get("ENGINE") == "duckdb_transient" for s in ps.stats)


class TestGetPinned:

    def test_returns_duckdb_pinned_instance(self):
        import supertable.engine.executor as mod
        mod._pinned_singleton = None
        assert isinstance(_get_pinned(), DuckDBPinned)

    def test_singleton_returns_same_instance(self):
        import supertable.engine.executor as mod
        mod._pinned_singleton = None
        a = _get_pinned()
        b = _get_pinned()
        assert a is b


# ═══════════════════════════════════════════════════════════
#  duckdb_transient
# ═══════════════════════════════════════════════════════════


class TestDuckDBTransient:

    def test_init(self):
        dt = DuckDBTransient(storage=MagicMock())
        assert dt.storage is not None

    @patch("supertable.engine.duckdb_transient.duckdb")
    @patch("supertable.engine.duckdb_transient.init_connection")
    @patch("supertable.engine.duckdb_transient.hashed_table_name", return_value="st_abc")
    @patch("supertable.engine.duckdb_transient.create_reflection_table_with_presign_retry", return_value=False)
    @patch("supertable.engine.duckdb_transient.rewrite_query_with_hashed_tables", return_value="SELECT 1")
    def test_execute_flow(self, mock_rewrite, mock_create, mock_hash, mock_init, mock_duckdb):
        fake_con = MagicMock()
        fake_con.execute.return_value.fetchdf.return_value = pd.DataFrame({"x": [1]})
        mock_duckdb.connect.return_value = fake_con

        parser = MagicMock()
        parser.original_query = "SELECT * FROM t"
        parser.get_table_tuples.return_value = [
            TableDefinition("s", "t", "t_alias", columns=["x"])
        ]
        reflection = Reflection(
            storage_type="mock", reflection_bytes=100, total_reflections=1,
            supers=[SuperSnapshot("s", "t", 1, ["f.parquet"], {"x"})],
        )
        qm = MagicMock()
        qm.temp_dir = "/tmp"
        qm.query_plan_path = "/tmp/plan.json"

        captures = []
        DuckDBTransient(storage=MagicMock()).execute(
            reflection, parser, qm, lambda e: captures.append(e)
        )
        assert "CONNECTING" in captures
        assert "CREATING_REFLECTION" in captures
        mock_init.assert_called_once()
        mock_create.assert_called_once()
        fake_con.close.assert_called_once()

    @patch("supertable.engine.duckdb_transient.duckdb")
    @patch("supertable.engine.duckdb_transient.init_connection")
    def test_connection_closed_on_error(self, mock_init, mock_duckdb):
        fake_con = MagicMock()
        mock_duckdb.connect.return_value = fake_con
        mock_init.side_effect = RuntimeError("init failed")

        parser = MagicMock()
        parser.get_table_tuples.return_value = []
        with pytest.raises(RuntimeError):
            DuckDBTransient().execute(Reflection("m", 0, 0, []), parser, MagicMock(), lambda e: None)
        fake_con.close.assert_called_once()


# ═══════════════════════════════════════════════════════════
#  duckdb_pinned
# ═══════════════════════════════════════════════════════════


class TestDuckDBPinned:

    def _make(self):
        return DuckDBPinned(storage=MagicMock())

    def test_init(self):
        p = self._make()
        assert p._con is None
        assert p._httpfs_configured is False
        assert p._registry == {}

    @patch("supertable.engine.duckdb_pinned.duckdb")
    @patch("supertable.engine.duckdb_pinned.init_connection")
    def test_get_connection_creates_once(self, mock_init, mock_duckdb):
        fake_con = MagicMock()
        mock_duckdb.connect.return_value = fake_con
        p = self._make()
        assert p._get_connection("/tmp") is p._get_connection("/tmp")
        mock_duckdb.connect.assert_called_once()

    def test_reset_connection(self):
        p = self._make()
        p._con = MagicMock()
        p._httpfs_configured = True
        p._registry = {("s", "t"): [MagicMock()]}
        p._reset_connection()
        assert p._con is None
        assert p._httpfs_configured is False
        assert p._registry == {}

    def test_reset_connection_handles_close_error(self):
        p = self._make()
        p._con = MagicMock()
        p._con.close.side_effect = RuntimeError("close failed")
        p._reset_connection()  # should not raise
        assert p._con is None

    @patch("supertable.engine.duckdb_pinned.create_reflection_table")
    @patch("supertable.engine.duckdb_pinned.configure_httpfs_and_s3")
    def test_ensure_table_creates_new(self, mock_httpfs, mock_create):
        p = self._make()
        name = p._ensure_table(MagicMock(), "s", "t", 1, ["f.parquet"])
        assert name.startswith("pin_")
        assert "_v1" in name
        mock_create.assert_called_once()

    @patch("supertable.engine.duckdb_pinned.create_reflection_table")
    @patch("supertable.engine.duckdb_pinned.configure_httpfs_and_s3")
    def test_ensure_table_reuses_cached(self, mock_httpfs, mock_create):
        p = self._make()
        con = MagicMock()
        n1 = p._ensure_table(con, "s", "t", 1, ["f.parquet"])
        n2 = p._ensure_table(con, "s", "t", 1, ["f.parquet"])
        assert n1 == n2
        mock_create.assert_called_once()

    @patch("supertable.engine.duckdb_pinned.create_reflection_table")
    @patch("supertable.engine.duckdb_pinned.configure_httpfs_and_s3")
    def test_ensure_table_new_version_marks_old_stale(self, mock_httpfs, mock_create):
        p = self._make()
        con = MagicMock()
        name1 = p._ensure_table(con, "s", "t", 1, ["f1.parquet"])
        p._acquire_refs({name1})  # keep it alive
        name2 = p._ensure_table(con, "s", "t", 2, ["f2.parquet"])
        assert name1 != name2
        assert mock_create.call_count == 2
        stale = [e for e in p._registry[("s", "t")] if e.stale]
        assert len(stale) == 1
        assert stale[0].version == 1

    @patch("supertable.engine.duckdb_pinned.create_reflection_table")
    @patch("supertable.engine.duckdb_pinned.configure_httpfs_and_s3")
    def test_ensure_table_new_version_drops_unreferenced_stale(self, mock_httpfs, mock_create):
        p = self._make()
        con = MagicMock()
        name1 = p._ensure_table(con, "s", "t", 1, ["f1.parquet"])
        p._ensure_table(con, "s", "t", 2, ["f2.parquet"])
        entries = p._registry[("s", "t")]
        assert len(entries) == 1
        assert entries[0].version == 2
        assert not entries[0].stale
        drop_calls = [c[0][0] for c in con.execute.call_args_list if "DROP TABLE" in str(c)]
        assert any(name1 in s for s in drop_calls)

    def test_acquire_and_release_refs(self):
        p = self._make()
        entry = _PinnedEntry("tbl_v1", "s", "t", 1)
        p._registry[("s", "t")] = [entry]
        p._acquire_refs({"tbl_v1"})
        assert entry.ref_count == 1
        p._acquire_refs({"tbl_v1"})
        assert entry.ref_count == 2
        p._release_refs({"tbl_v1"})
        assert entry.ref_count == 1
        p._release_refs({"tbl_v1"})
        assert entry.ref_count == 0
        p._release_refs({"tbl_v1"})
        assert entry.ref_count == 0  # no negative

    def test_drop_unreferenced_stale(self):
        p = self._make()
        con = MagicMock()
        stale_zero = _PinnedEntry("old_v1", "s", "t", 1, ref_count=0, stale=True)
        stale_busy = _PinnedEntry("old_v2", "s", "t", 2, ref_count=1, stale=True)
        current = _PinnedEntry("cur_v3", "s", "t", 3, ref_count=0, stale=False)
        p._registry[("s", "t")] = [stale_zero, stale_busy, current]
        p._drop_unreferenced_stale(con)
        drop_calls = [c[0][0] for c in con.execute.call_args_list]
        assert any("old_v1" in s for s in drop_calls)
        remaining = p._registry[("s", "t")]
        assert len(remaining) == 2
        assert {e.table_name for e in remaining} == {"old_v2", "cur_v3"}

    def test_get_cached_tables(self):
        p = self._make()
        p._registry[("s", "t")] = [
            _PinnedEntry("tbl1", "s", "t", 1, ref_count=0, stale=False),
            _PinnedEntry("tbl2", "s", "t", 2, ref_count=1, stale=True),
        ]
        tables = p.get_cached_tables()
        assert len(tables) == 2
        assert tables[0]["table_name"] == "tbl1"
        assert tables[0]["stale"] is False
        assert tables[1]["stale"] is True

    def test_drop_all(self):
        p = self._make()
        p._con = MagicMock()
        p._registry[("s", "t")] = [_PinnedEntry("tbl1", "s", "t", 1)]
        p.drop_all()
        assert p._con is None
        assert p._registry == {}

    @patch("supertable.engine.duckdb_pinned.create_reflection_table")
    @patch("supertable.engine.duckdb_pinned.make_presigned_list")
    @patch("supertable.engine.duckdb_pinned.configure_httpfs_and_s3")
    def test_ensure_table_presign_fallback(self, mock_httpfs, mock_presign, mock_create):
        p = self._make()
        mock_create.side_effect = [Exception("HTTP Error 403"), None]
        mock_presign.return_value = ["https://presigned/f.parquet"]
        name = p._ensure_table(MagicMock(), "s", "t", 1, ["s3://bucket/f.parquet"])
        assert name.startswith("pin_")
        assert mock_create.call_count == 2
        mock_presign.assert_called_once()

    def test_current_entry_returns_non_stale(self):
        p = self._make()
        p._registry[("s", "t")] = [
            _PinnedEntry("t1", "s", "t", 1, stale=True),
            _PinnedEntry("t2", "s", "t", 2, stale=False),
        ]
        assert p._current_entry(("s", "t")).table_name == "t2"

    def test_current_entry_none_when_all_stale(self):
        p = self._make()
        p._registry[("s", "t")] = [_PinnedEntry("t1", "s", "t", 1, stale=True)]
        assert p._current_entry(("s", "t")) is None

    def test_current_entry_none_when_empty(self):
        p = self._make()
        assert p._current_entry(("s", "t")) is None


# ═══════════════════════════════════════════════════════════
#  spark_thrift — helpers
# ═══════════════════════════════════════════════════════════


class TestSparkTableName:

    def test_deterministic(self):
        assert _spark_table_name("s", "t", 1) == _spark_table_name("s", "t", 1)

    def test_prefix_and_version(self):
        name = _spark_table_name("s", "t", 3)
        assert name.startswith("spark_")
        assert name.endswith("_v3")

    def test_different_version(self):
        assert _spark_table_name("s", "t", 1) != _spark_table_name("s", "t", 2)


class TestSparkCreateParquetView:

    def test_single_file(self):
        cursor = MagicMock()
        _spark_create_parquet_view(cursor, "tbl", ["/data/f.parquet"])
        cursor.execute.assert_called_once()
        sql = cursor.execute.call_args[0][0]
        assert "CREATE OR REPLACE TEMPORARY VIEW tbl" in sql
        assert "USING parquet" in sql

    def test_multiple_files_creates_union(self):
        cursor = MagicMock()
        _spark_create_parquet_view(cursor, "tbl", ["/a.parquet", "/b.parquet"])
        all_sql = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("UNION ALL" in s for s in all_sql)

    def test_multiple_files_drops_parts(self):
        cursor = MagicMock()
        _spark_create_parquet_view(cursor, "tbl", ["/a.parquet", "/b.parquet"])
        all_sql = [c[0][0] for c in cursor.execute.call_args_list]
        assert len([s for s in all_sql if "DROP VIEW" in s]) == 2


class TestSparkCreateRbacView:

    def test_all_columns(self):
        cursor = MagicMock()
        _spark_create_rbac_view(cursor, "base", "rbac_v", RbacViewDef(allowed_columns=["*"]))
        assert "SELECT *" in cursor.execute.call_args[0][0]

    def test_specific_columns_with_where(self):
        cursor = MagicMock()
        _spark_create_rbac_view(
            cursor, "base", "rbac_v",
            RbacViewDef(allowed_columns=["id", "name"], where_clause="org = 1"),
        )
        sql = cursor.execute.call_args[0][0]
        assert "`id`" in sql
        assert "`name`" in sql
        assert "WHERE org = 1" in sql


class TestSparkRewriteQuery:

    def test_delegates_to_common(self):
        result = _spark_rewrite_query("SELECT * FROM orders", {"orders": "st_abc"})
        assert "st_abc" in result

    def test_empty_alias(self):
        assert _spark_rewrite_query("SELECT 1", {}) == "SELECT 1"


class TestConfigureSparkS3:

    def test_sets_all_configs(self, monkeypatch, clean_env):
        monkeypatch.setenv("STORAGE_ENDPOINT_URL", "http://minio:9000")
        monkeypatch.setenv("STORAGE_ACCESS_KEY", "ak")
        monkeypatch.setenv("STORAGE_SECRET_KEY", "sk")
        monkeypatch.setenv("STORAGE_REGION", "eu-west-1")
        cursor = MagicMock()
        _configure_spark_s3(cursor)
        all_sql = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("fs.s3a.endpoint" in s for s in all_sql)
        assert any("fs.s3a.access.key" in s for s in all_sql)

    def test_missing_env_still_sets_defaults(self, monkeypatch, clean_env):
        cursor = MagicMock()
        _configure_spark_s3(cursor)
        all_sql = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("ssl.enabled" in s for s in all_sql)
        assert any("s3a.impl" in s for s in all_sql)

    def test_set_failure_does_not_raise(self):
        cursor = MagicMock()
        cursor.execute.side_effect = Exception("not supported")
        _configure_spark_s3(cursor)  # must not raise


class TestSparkThriftExecutor:

    def test_init(self):
        assert SparkThriftExecutor(storage=MagicMock()).storage is not None

    @patch.object(SparkThriftExecutor, "_select_cluster")
    @patch.object(SparkThriftExecutor, "_get_connection")
    @patch("supertable.engine.spark_thrift._configure_spark_s3")
    @patch("supertable.engine.spark_thrift._spark_create_parquet_view")
    @patch("supertable.engine.spark_thrift._spark_rewrite_query", return_value="SELECT 1")
    def test_execute_flow(self, mock_rewrite, mock_view, mock_s3, mock_conn, mock_cluster):
        mock_cluster.return_value = {"cluster_id": "c1", "thrift_host": "h", "thrift_port": 10000}
        fake_cursor = MagicMock()
        fake_cursor.description = [("col1",)]
        fake_cursor.fetchall.return_value = [(42,)]
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        mock_conn.return_value = fake_conn

        parser = MagicMock()
        parser.original_query = "SELECT * FROM t"
        parser.get_table_tuples.return_value = [TableDefinition("s", "t", "t_alias")]
        reflection = Reflection(
            storage_type="mock", reflection_bytes=100, total_reflections=1,
            supers=[SuperSnapshot("s", "t", 1, ["s3://bucket/f.parquet"], {"col"})],
        )
        captures = []
        SparkThriftExecutor(storage=MagicMock()).execute(
            reflection, parser, MagicMock(), lambda e: captures.append(e),
        )
        assert "CONNECTING" in captures
        assert "CREATING_REFLECTION" in captures
        fake_cursor.close.assert_called_once()
        fake_conn.close.assert_called_once()

    @patch.object(SparkThriftExecutor, "_select_cluster")
    @patch.object(SparkThriftExecutor, "_get_connection")
    @patch("supertable.engine.spark_thrift._configure_spark_s3")
    @patch("supertable.engine.spark_thrift._spark_create_parquet_view")
    @patch("supertable.engine.spark_thrift._spark_rewrite_query", return_value="SELECT 1")
    def test_s3_to_s3a_conversion(self, mock_rewrite, mock_view, mock_s3, mock_conn, mock_cluster):
        mock_cluster.return_value = {"cluster_id": "c1", "thrift_host": "h"}
        fake_cursor = MagicMock()
        fake_cursor.description = []
        fake_cursor.fetchall.return_value = []
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        mock_conn.return_value = fake_conn

        parser = MagicMock()
        parser.original_query = "SELECT 1"
        parser.get_table_tuples.return_value = [TableDefinition("s", "t", "t_alias")]
        reflection = Reflection(
            storage_type="mock", reflection_bytes=100, total_reflections=1,
            supers=[SuperSnapshot("s", "t", 1, ["s3://bucket/key"], set())],
        )
        SparkThriftExecutor().execute(reflection, parser, MagicMock(), lambda e: None)
        files_arg = mock_view.call_args[0][2]
        assert all(f.startswith("s3a://") for f in files_arg)

    def test_select_cluster_no_cluster_raises(self):
        ste = SparkThriftExecutor()
        ste.catalog = MagicMock()
        ste.catalog.select_spark_cluster.return_value = None
        with pytest.raises(RuntimeError, match="No active Spark cluster"):
            ste._select_cluster(1000)


# ═══════════════════════════════════════════════════════════
#  engine __init__ — public API exports
# ═══════════════════════════════════════════════════════════


class TestEnginePackageExports:

    def test_exports_engine(self):
        from supertable.engine import Engine
        assert Engine is not None

    def test_exports_executor(self):
        from supertable.engine import Executor
        assert Executor is not None

    def test_exports_plan_stats(self):
        from supertable.engine import PlanStats
        assert PlanStats is not None

    def test_exports_data_estimator(self):
        from supertable.engine import DataEstimator
        assert DataEstimator is not None
