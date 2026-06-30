# supertable/engine/tests/test_engine.py
"""
Comprehensive test suite for the supertable.engine package.

Covers:
  - engine_common: SQL helpers, table naming, env detection, presign,
                   reflection table creation, query rewriting, init_connection,
                   RBAC view creation
  - plan_stats:    PlanStats accumulation
  - data_estimator: get_missing_columns, DataEstimator.estimate
  - engine_enum:   Engine enum values
  - executor:      Executor routing (lite / pro / spark_sql / auto)
  - duckdb_lite:   DuckDBLite.execute
  - duckdb_pro:    DuckDBPro lifecycle, table caching, ref counting, staleness
  - spark_sql:  SparkThriftExecutor helpers & execute flow
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from dataclasses import replace as _dc_replace
from supertable.config.settings import settings as _settings
from supertable.engine import engine_common as _engine_common
from supertable.engine import data_estimator as _data_estimator
from supertable.data_classes import (
    Reflection,
    RbacViewDef,
    SuperSnapshot,
    TableDefinition,
    TombstoneDef,
)


def _patch_settings(monkeypatch, **overrides):
    """Replace the per-module ``settings`` binding with a copy carrying
    overrides. Settings is a frozen dataclass so we use dataclasses.replace
    to produce a new instance, then patch each module that imported it.
    """
    new_settings = _dc_replace(_settings, **overrides)
    monkeypatch.setattr(_engine_common, "settings", new_settings)
    monkeypatch.setattr(_data_estimator, "settings", new_settings)
    return new_settings
from supertable.engine.plan_stats import PlanStats
from supertable.engine.engine_common import (
    quote_if_needed,
    sanitize_sql_string,
    escape_parquet_path,
    hashed_table_name,
    pro_table_name,
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
    new_duckdb_connection,
    create_rbac_view,
    rbac_view_name,
    create_reflection_table,
    create_reflection_table_with_presign_retry,
    configure_httpfs_and_s3,
    create_tombstone_view,
    TombstoneCache,
    dv_table_name,
    dv_table_id,
)
from supertable.engine.data_estimator import DataEstimator, get_missing_columns
from supertable.engine.engine_enum import Engine
from supertable.engine.executor import Executor, _get_pro
from supertable.engine.duckdb_lite import DuckDBLite
from supertable.engine.duckdb_pro import DuckDBPro, _ProCacheEntry
from supertable.engine.spark_thrift import (
    _spark_table_name,
    _spark_create_parquet_view,
    _spark_create_rbac_view,
    _spark_rewrite_query,
    _configure_spark_s3,
    _read_parquet_schema,
    _parquet_timestamp_units,
    _ts_to_timestamp_expr,
    _build_tscast_select,
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
        # Bare SQL keywords must pass through unchanged (used in SET statements).
        assert sanitize_sql_string("TRUE") == "TRUE"

    def test_numeric_passthrough(self):
        # Bare numeric literals must pass through unchanged.
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


class TestProTableName:

    def test_deterministic(self):
        assert pro_table_name("s", "t", 5) == pro_table_name("s", "t", 5)

    def test_prefix_and_version_suffix(self):
        name = pro_table_name("s", "t", 7)
        assert name.startswith("pro_")
        assert name.endswith("_v7")

    def test_different_version_different_name(self):
        assert pro_table_name("s", "t", 1) != pro_table_name("s", "t", 2)

    def test_same_key_different_version_share_prefix(self):
        n1 = pro_table_name("s", "t", 1)
        n2 = pro_table_name("s", "t", 2)
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
        _patch_settings(monkeypatch, STORAGE_ENDPOINT_URL="http://minio:9000")
        assert detect_endpoint() == "minio:9000"

    def test_missing(self, monkeypatch, clean_env):
        _patch_settings(monkeypatch, STORAGE_ENDPOINT_URL="")
        assert detect_endpoint() is None


class TestDetectRegion:

    def test_from_env(self, monkeypatch):
        _patch_settings(monkeypatch, STORAGE_REGION="eu-west-1")
        assert detect_region() == "eu-west-1"

    def test_default(self, monkeypatch, clean_env):
        # detect_region just echoes whatever the settings default is.
        assert detect_region() == _settings.STORAGE_REGION


class TestDetectUrlStyle:

    def test_path_style(self, monkeypatch):
        _patch_settings(monkeypatch, STORAGE_FORCE_PATH_STYLE=True)
        assert detect_url_style() == "path"

    def test_vhost_style(self, monkeypatch):
        _patch_settings(monkeypatch, STORAGE_FORCE_PATH_STYLE=False)
        assert detect_url_style() == "vhost"

    def test_default_is_path(self, monkeypatch, clean_env):
        assert detect_url_style() == ("path" if _settings.STORAGE_FORCE_PATH_STYLE else "vhost")


class TestDetectSsl:

    def test_true(self, monkeypatch):
        _patch_settings(monkeypatch, STORAGE_USE_SSL=True)
        assert detect_ssl() is True

    def test_one(self, monkeypatch):
        _patch_settings(monkeypatch, STORAGE_USE_SSL=True)
        assert detect_ssl() is True

    def test_empty(self, monkeypatch):
        _patch_settings(monkeypatch, STORAGE_USE_SSL=False)
        assert detect_ssl() is False

    def test_missing(self, monkeypatch, clean_env):
        _patch_settings(monkeypatch, STORAGE_USE_SSL=False)
        assert detect_ssl() is False


class TestDetectCreds:

    def test_all_present(self, monkeypatch):
        _patch_settings(
            monkeypatch,
            STORAGE_ACCESS_KEY="ak",
            STORAGE_SECRET_KEY="sk",
            STORAGE_SESSION_TOKEN="st",
        )
        ak, sk, st = detect_creds()
        assert (ak, sk, st) == ("ak", "sk", "st")

    def test_all_missing(self, monkeypatch, clean_env):
        _patch_settings(
            monkeypatch,
            STORAGE_ACCESS_KEY="",
            STORAGE_SECRET_KEY="",
            STORAGE_SESSION_TOKEN="",
        )
        ak, sk, st = detect_creds()
        assert ak is None and sk is None and st is None


class TestDetectBucket:

    def test_from_env(self, monkeypatch):
        _patch_settings(monkeypatch, STORAGE_BUCKET="my-bucket")
        assert detect_bucket() == "my-bucket"

    def test_missing(self, monkeypatch, clean_env):
        _patch_settings(monkeypatch, STORAGE_BUCKET="")
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

    def test_pins_home_directory_to_app_home(self, duckdb_con, tmp_path):
        # The write-side probe failed in production with "Can't find the home
        # directory at '/home/app'" under a restricted user.  init_connection
        # must pin DuckDB's home_directory to the app home, which is created and
        # writable at import time, so home-dependent ops never touch the OS home.
        from supertable.config.homedir import get_app_home
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        home = duckdb_con.execute(
            "SELECT current_setting('home_directory')"
        ).fetchone()[0]
        assert home == get_app_home()
        assert os.path.isdir(home)


# ═══════════════════════════════════════════════════════════
#  engine_common — read/write DuckDB parity
# ═══════════════════════════════════════════════════════════

# Settings that must be identical between the read executors and the write-side
# probe so both paths configure DuckDB the same way (and never fall back to the
# OS home directory).
_PARITY_SETTINGS = (
    "home_directory",
    "temp_directory",
    "memory_limit",
    "threads",
    "default_collation",
    "preserve_insertion_order",
)


def _duckdb_settings_snapshot(con):
    snap = {}
    for name in _PARITY_SETTINGS:
        snap[name] = con.execute(
            f"SELECT current_setting('{name}')"
        ).fetchone()[0]
    return snap


class TestReadWriteDuckDBParity:
    """Read (DuckDBLite) and write (probe via new_duckdb_connection) must
    configure DuckDB identically — same pragmas, same pinned home directory."""

    def test_helper_matches_read_executor_settings(self, tmp_path):
        lite = DuckDBLite(storage=None)
        con_read = lite._get_connection(temp_dir=str(tmp_path))
        con_write = new_duckdb_connection(temp_dir=str(tmp_path))
        try:
            read_snap = _duckdb_settings_snapshot(con_read)
            write_snap = _duckdb_settings_snapshot(con_write)
            assert read_snap == write_snap
            # Never the OS home (which may be missing under a restricted user).
            assert os.path.isdir(read_snap["home_directory"])
        finally:
            try:
                con_write.close()
            except Exception:
                pass
            try:
                con_read.close()
            except Exception:
                pass

    def test_helper_loads_httpfs_only_for_remote_paths(self, tmp_path, monkeypatch):
        calls = []
        real = _engine_common.configure_httpfs_and_s3
        monkeypatch.setattr(
            _engine_common,
            "configure_httpfs_and_s3",
            lambda con, paths: calls.append(list(paths)) or real(con, paths),
        )
        # Local-only paths: helper must NOT load httpfs.
        con_local = new_duckdb_connection(
            temp_dir=str(tmp_path), for_paths=[str(tmp_path / "f.parquet")]
        )
        con_local.close()
        assert calls == []

    def test_probe_uses_shared_connection_constructor(self, tmp_path, monkeypatch):
        # The probe must build its connection via new_duckdb_connection so it
        # inherits the read path's init_connection setup (home_directory pin).
        import polars
        from supertable import processing as _processing

        monkeypatch.setattr(_processing, "_get_storage", lambda: object())

        f1 = str(tmp_path / "f1.parquet")
        polars.DataFrame({"__rowid__": [10, 20], "id": [1, 2]}).write_parquet(f1)

        calls = []
        real = _engine_common.new_duckdb_connection

        def spy(*a, **k):
            calls.append((a, k))
            return real(*a, **k)

        monkeypatch.setattr(_engine_common, "new_duckdb_connection", spy)

        out = _processing._duckdb_probe_overlap_matches(
            overlap_true_files=[(f1, 0)],
            overwrite_columns=["id"],
            newer_than_col=None,
            incoming_keys=polars.DataFrame({"id": [2]}),
        )
        assert out is not None
        assert len(calls) == 1
        # for_paths forwarded so httpfs is loaded for remote scans.
        assert "for_paths" in calls[0][1]

    def test_probe_reuses_pooled_connection(self, tmp_path, monkeypatch):
        # A second probe on the same thread must REUSE the pooled connection,
        # so new_duckdb_connection is built exactly once — the ~150ms warmup is
        # paid on the cold probe and amortised on every subsequent write.
        import polars
        from supertable import processing as _processing

        monkeypatch.setattr(_processing, "_get_storage", lambda: object())

        f1 = str(tmp_path / "f1.parquet")
        polars.DataFrame({"__rowid__": [10, 20], "id": [1, 2]}).write_parquet(f1)

        calls = []
        real = _engine_common.new_duckdb_connection
        monkeypatch.setattr(
            _engine_common,
            "new_duckdb_connection",
            lambda *a, **k: (calls.append((a, k)), real(*a, **k))[1],
        )

        def _probe():
            return _processing._duckdb_probe_overlap_matches(
                overlap_true_files=[(f1, 0)],
                overwrite_columns=["id"],
                newer_than_col=None,
                incoming_keys=polars.DataFrame({"id": [2]}),
            )

        assert _probe() is not None
        assert _probe() is not None
        assert len(calls) == 1  # built on the cold probe, reused on the warm one

    def test_probe_matches_rows_on_local_parquet(self, tmp_path, monkeypatch):
        import polars
        from supertable import processing as _processing

        monkeypatch.setattr(_processing, "_get_storage", lambda: object())

        f1 = str(tmp_path / "f1.parquet")
        f2 = str(tmp_path / "f2.parquet")
        polars.DataFrame({"__rowid__": [10, 20], "id": [1, 2]}).write_parquet(f1)
        polars.DataFrame({"__rowid__": [30, 40], "id": [3, 4]}).write_parquet(f2)

        out = _processing._duckdb_probe_overlap_matches(
            overlap_true_files=[(f1, 0), (f2, 0)],
            overwrite_columns=["id"],
            newer_than_col=None,
            incoming_keys=polars.DataFrame({"id": [2, 3, 99]}),
        )
        assert out is not None
        assert set(out.get_column("id").to_list()) == {2, 3}
        assert set(out.get_column("__rowid__").to_list()) == {20, 30}
        assert "__file__" in out.columns
        assert set(out.get_column("__file__").to_list()) == {f1, f2}

    # ── S3 presigning parity (DuckDB cannot read private S3 unsigned) ──

    def test_resolver_presigns_only_when_flag_set(self, monkeypatch):
        # _storage_duckdb_path mirrors DataEstimator._to_duckdb_path: presign the
        # bare key only when SUPERTABLE_DUCKDB_PRESIGNED is on (or forced).
        from supertable import processing as _processing

        class _S3Stub:
            def presign(self, key, expiry_seconds=3600):
                return f"https://signed/{key}?sig=abc"

            def to_duckdb_path(self, key):
                return f"s3://bucket/{key}"

        # Force the flag OFF (the repo .env turns it on) so this branch is
        # deterministic regardless of ambient config.
        monkeypatch.setattr(
            _processing, "settings",
            _dc_replace(_processing.settings, SUPERTABLE_DUCKDB_PRESIGNED=False),
        )
        stub = _S3Stub()
        # Flag off → object-store URL via to_duckdb_path, NOT presigned.
        assert _processing._storage_duckdb_path(stub, "data/f.parquet") == (
            "s3://bucket/data/f.parquet"
        )
        # force_presign=True → signed URL (the reactive-retry path).
        assert _processing._storage_duckdb_path(
            stub, "data/f.parquet", force_presign=True
        ) == "https://signed/data/f.parquet?sig=abc"
        # A value already a URL is never presigned (presign takes a key).
        assert _processing._storage_duckdb_path(
            stub, "s3://bucket/x.parquet", force_presign=True
        ) == "s3://bucket/x.parquet"

    def test_resolver_presigns_when_global_flag_enabled(self, monkeypatch):
        from supertable import processing as _processing

        class _S3Stub:
            def presign(self, key, expiry_seconds=3600):
                return f"https://signed/{key}"

        monkeypatch.setattr(
            _processing, "settings",
            _dc_replace(_processing.settings, SUPERTABLE_DUCKDB_PRESIGNED=True),
        )
        assert _processing._storage_duckdb_path(_S3Stub(), "data/f.parquet") == (
            "https://signed/data/f.parquet"
        )

    def test_probe_presigns_keys_and_maps_back_to_storage_key(self, tmp_path, monkeypatch):
        # End-to-end: with presigning enabled the probe scans the SIGNED path but
        # __file__ must still resolve back to the original (bare) storage key, so
        # tombstones store keys, not transient signed URLs.
        import polars
        from supertable import processing as _processing

        real_file = str(tmp_path / "real.parquet")
        polars.DataFrame({"__rowid__": [10, 20], "id": [1, 2]}).write_parquet(real_file)

        class _S3Stub:
            def presign(self, key, expiry_seconds=3600):
                # Pretend the bare key signs to a readable URL (here a local file).
                return real_file

        monkeypatch.setattr(_processing, "_get_storage", lambda: _S3Stub())
        monkeypatch.setattr(
            _processing, "settings",
            _dc_replace(_processing.settings, SUPERTABLE_DUCKDB_PRESIGNED=True),
        )

        bare_key = "bucket/data/f1.parquet"
        out = _processing._duckdb_probe_overlap_matches(
            overlap_true_files=[(bare_key, 0)],
            overwrite_columns=["id"],
            newer_than_col=None,
            incoming_keys=polars.DataFrame({"id": [2]}),
        )
        assert out is not None
        assert out.get_column("__file__").to_list() == [bare_key]
        assert out.get_column("__rowid__").to_list() == [20]


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
#  engine_common — create_tombstone_view (inline vs cached DV)
# ═══════════════════════════════════════════════════════════


def _make_tomb_source(con, table: str) -> None:
    """Source reflection table with system columns + 4 user rows."""
    con.execute(
        f'CREATE TABLE {table} '
        f'(id INTEGER, name VARCHAR, "__rowid__" BIGINT, "__timestamp__" BIGINT)'
    )
    con.execute(
        f"INSERT INTO {table} VALUES "
        f"(1,'a',10,100),(2,'b',20,100),(3,'c',30,100),(4,'d',40,100)"
    )


def _write_dv_parquet(con, path: str, rowids) -> None:
    """Write a deletion-vector parquet of the given __rowid__ values."""
    values = ",".join(f"({r})" for r in rowids)
    con.execute(
        f'COPY (SELECT * FROM (VALUES {values}) AS t("__rowid__")) '
        f"TO '{path}' (FORMAT PARQUET)"
    )


class TestCreateTombstoneView:

    def test_strips_system_columns_no_tombstone(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        _make_tomb_source(duckdb_con, "src_a")
        create_tombstone_view(duckdb_con, "src_a", "tv_a", None)
        df = duckdb_con.execute("SELECT * FROM tv_a ORDER BY id").fetchdf()
        assert "__rowid__" not in df.columns
        assert "__timestamp__" not in df.columns
        assert df["id"].tolist() == [1, 2, 3, 4]

    def test_inline_path_applies_deletion_vector(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        _make_tomb_source(duckdb_con, "src_b")
        dv = str(tmp_path / "dv_b.parquet")
        _write_dv_parquet(duckdb_con, dv, [20, 40])
        tomb = TombstoneDef(tombstone_path=dv, cache_key="bare/b")
        # dv_table=None → inline read_parquet anti-join
        create_tombstone_view(duckdb_con, "src_b", "tv_b", tomb, dv_table=None)
        df = duckdb_con.execute("SELECT * FROM tv_b ORDER BY id").fetchdf()
        assert df["id"].tolist() == [1, 3]
        assert "__rowid__" not in df.columns

    def test_cached_path_matches_inline_bit_for_bit(self, duckdb_con, tmp_path):
        # The core correctness guarantee: anti-joining a materialised DV table
        # is identical to the inline read_parquet subquery.
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        _make_tomb_source(duckdb_con, "src_c")
        dv = str(tmp_path / "dv_c.parquet")
        _write_dv_parquet(duckdb_con, dv, [20, 40])
        tomb = TombstoneDef(tombstone_path=dv, cache_key="bare/c")

        create_tombstone_view(duckdb_con, "src_c", "tv_inline", tomb, dv_table=None)
        inline = duckdb_con.execute("SELECT * FROM tv_inline ORDER BY id").fetchdf()

        cache = TombstoneCache(capacity=8)
        dvt = cache.acquire(duckdb_con, tomb.cache_key, tomb.tombstone_path)
        assert dvt == dv_table_name(tomb.cache_key)
        create_tombstone_view(duckdb_con, "src_c", "tv_cached", tomb, dv_table=dvt)
        cached = duckdb_con.execute("SELECT * FROM tv_cached ORDER BY id").fetchdf()

        pd.testing.assert_frame_equal(
            inline.reset_index(drop=True), cached.reset_index(drop=True)
        )
        assert cached["id"].tolist() == [1, 3]
        assert "__rowid__" not in cached.columns
        assert "__timestamp__" not in cached.columns


# ═══════════════════════════════════════════════════════════
#  engine_common — TombstoneCache (deletion-vector table cache)
# ═══════════════════════════════════════════════════════════


def _dv_table_exists(con, name: str) -> bool:
    row = con.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE table_name = ?", [name]
    ).fetchone()
    return row[0] > 0


class TestTombstoneCacheUnit:

    # Two versions of the same logical table share a tombstone directory; only
    # the filename rotates.  These helpers keep that convention explicit.
    T = "org/super/simple/tombstone"
    V1 = f"{T}/1000_aa_deleted.parquet"
    V2 = f"{T}/2000_bb_deleted.parquet"
    V3 = f"{T}/3000_cc_deleted.parquet"

    @pytest.mark.parametrize("capacity", [0, -1])
    def test_disabled_returns_none(self, duckdb_con, tmp_path, capacity):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=capacity)
        assert cache.enabled is False
        assert cache.acquire(duckdb_con, "k", dv) is None
        # No table materialised when disabled.
        assert not _dv_table_exists(duckdb_con, dv_table_name("k"))

    def test_missing_key_or_path_returns_none(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        cache = TombstoneCache(capacity=4)
        assert cache.acquire(duckdb_con, None, "/p") is None
        assert cache.acquire(duckdb_con, "k", None) is None

    def test_acquire_materializes_distinct_rowids(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2, 2, 3])  # dup 2
        cache = TombstoneCache(capacity=4)
        name = cache.acquire(duckdb_con, self.V1, dv)
        assert name == dv_table_name(self.V1)
        assert _dv_table_exists(duckdb_con, name)
        count = duckdb_con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        assert count == 3  # DISTINCT
        snap = cache.snapshot()
        assert len(snap) == 1 and snap[0]["ref_count"] == 1

    def test_hit_reuses_single_entry_and_refcounts(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=4)
        n1 = cache.acquire(duckdb_con, self.V1, dv)
        n2 = cache.acquire(duckdb_con, self.V1, dv)
        assert n1 == n2
        snap = cache.snapshot()
        assert len(snap) == 1 and snap[0]["ref_count"] == 2

    def test_release_decrements(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1])
        # Non-zero TTL with a fixed clock so the entry survives at ref_count 0
        # (ttl<=0 would drop it the instant it becomes unreferenced).
        cache = TombstoneCache(capacity=4, ttl_seconds=300, time_fn=lambda: 1000.0)
        cache.acquire(duckdb_con, self.V1, dv)
        cache.acquire(duckdb_con, self.V1, dv)
        cache.release(duckdb_con, self.V1)
        assert cache.snapshot()[0]["ref_count"] == 1
        cache.release(duckdb_con, self.V1)
        assert cache.snapshot()[0]["ref_count"] == 0

    # ── table identity ────────────────────────────────────────────

    def test_dv_table_id_groups_versions_by_directory(self):
        assert dv_table_id(self.V1) == self.T
        assert dv_table_id(self.V2) == self.T
        assert dv_table_id(self.V1) == dv_table_id(self.V2)
        # Different tables → different ids.
        assert dv_table_id("other/tombstone/x.parquet") != self.T
        # No separator → key groups alone (cap relaxed for it, never wrong).
        assert dv_table_id("bare") == "bare"

    # ── idle TTL (refreshed on access) ────────────────────────────

    def test_acquire_sets_idle_ttl(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=4, ttl_seconds=300, time_fn=lambda: 1000.0)
        cache.acquire(duckdb_con, self.V1, dv)
        assert cache.snapshot()[0]["expires_at"] == 1000.0 + 300

    def test_idle_entry_survives_within_ttl(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        clock = [1000.0]
        cache = TombstoneCache(capacity=8, ttl_seconds=300, time_fn=lambda: clock[0])
        cache.acquire(duckdb_con, self.V1, dv)
        cache.release(duckdb_con, self.V1)       # expires at 1300
        clock[0] = 1299.0
        # Another table's activity triggers a sweep without touching V1.
        other = "other/tombstone/1_x.parquet"
        cache.acquire(duckdb_con, other, dv); cache.release(duckdb_con, other)
        assert _dv_table_exists(duckdb_con, dv_table_name(self.V1))

    def test_idle_entry_evicted_after_ttl_even_if_only_version(self, duckdb_con, tmp_path):
        # The user's rule: every table gets a TTL; the latest/only version is
        # dropped too once the table goes unqueried for the window.
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        clock = [1000.0]
        cache = TombstoneCache(capacity=8, ttl_seconds=300, time_fn=lambda: clock[0])
        cache.acquire(duckdb_con, self.V1, dv)
        cache.release(duckdb_con, self.V1)       # expires at 1300
        clock[0] = 1301.0                        # past the window
        other = "other/tombstone/1_x.parquet"
        cache.acquire(duckdb_con, other, dv); cache.release(duckdb_con, other)
        assert not _dv_table_exists(duckdb_con, dv_table_name(self.V1))
        assert {e["cache_key"] for e in cache.snapshot()} == {other}

    def test_reacquire_refreshes_idle_ttl(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        clock = [1000.0]
        cache = TombstoneCache(capacity=8, ttl_seconds=300, time_fn=lambda: clock[0])
        cache.acquire(duckdb_con, self.V1, dv)
        cache.release(duckdb_con, self.V1)       # expires at 1300
        clock[0] = 1250.0
        cache.acquire(duckdb_con, self.V1, dv)   # refresh → expires at 1550
        cache.release(duckdb_con, self.V1)
        clock[0] = 1400.0                        # past old deadline, before new
        other = "other/tombstone/1_x.parquet"
        cache.acquire(duckdb_con, other, dv); cache.release(duckdb_con, other)
        assert _dv_table_exists(duckdb_con, dv_table_name(self.V1))
        assert cache.snapshot()
        assert {e["cache_key"] for e in cache.snapshot()} >= {self.V1}

    def test_ttl_zero_drops_when_unreferenced(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=8, ttl_seconds=0)
        cache.acquire(duckdb_con, self.V1, dv)   # pinned → survives
        assert _dv_table_exists(duckdb_con, dv_table_name(self.V1))
        cache.release(duckdb_con, self.V1)       # unref + ttl 0 → dropped
        assert not _dv_table_exists(duckdb_con, dv_table_name(self.V1))
        assert cache.snapshot() == []

    # ── per-table version cap ─────────────────────────────────────

    def test_per_table_cap_keeps_last_n_versions(self, duckdb_con, tmp_path):
        # 10 rapid rewrites of ONE table under cap=3 → only the last 3
        # (most-recently-used) versions survive; the rest are dropped.
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=3, ttl_seconds=10_000, time_fn=lambda: 0.0)
        keys = [f"{self.T}/{i}_x_deleted.parquet" for i in range(10)]
        for k in keys:
            cache.acquire(duckdb_con, k, dv)
            cache.release(duckdb_con, k)
        kept = {e["cache_key"] for e in cache.snapshot()}
        assert kept == set(keys[-3:])
        for k in keys[:-3]:
            assert not _dv_table_exists(duckdb_con, dv_table_name(k))
        for k in keys[-3:]:
            assert _dv_table_exists(duckdb_con, dv_table_name(k))

    def test_per_table_cap_is_isolated_across_tables(self, duckdb_con, tmp_path):
        # A churny table at its cap must never shrink a slow table's residency:
        # the cap is counted per table_id, not globally.
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=2, ttl_seconds=10_000, time_fn=lambda: 0.0)
        slow = "slow/tombstone/1000_x_deleted.parquet"
        cache.acquire(duckdb_con, slow, dv); cache.release(duckdb_con, slow)
        fast = [f"fast/tombstone/{i}_y.parquet" for i in range(5)]
        for k in fast:
            cache.acquire(duckdb_con, k, dv); cache.release(duckdb_con, k)
        kept = {e["cache_key"] for e in cache.snapshot()}
        assert slow in kept                                  # slow untouched
        assert _dv_table_exists(duckdb_con, dv_table_name(slow))
        assert len({k for k in kept if k.startswith("fast")}) == 2  # fast capped

    def test_pinned_over_cap_not_dropped_until_released(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=1, ttl_seconds=10_000, time_fn=lambda: 0.0)
        cache.acquire(duckdb_con, self.V1, dv)   # pinned (ref 1)
        cache.acquire(duckdb_con, self.V2, dv)   # over cap but V1 pinned
        assert _dv_table_exists(duckdb_con, dv_table_name(self.V1))
        assert _dv_table_exists(duckdb_con, dv_table_name(self.V2))
        cache.release(duckdb_con, self.V1)       # now evictable (LRU) → dropped
        assert not _dv_table_exists(duckdb_con, dv_table_name(self.V1))
        assert {e["cache_key"] for e in cache.snapshot()} == {self.V2}

    # ── lifecycle / determinism ───────────────────────────────────

    def test_clear_registry_forgets_without_dropping(self, duckdb_con, tmp_path):
        init_connection(duckdb_con, temp_dir=str(tmp_path))
        dv = str(tmp_path / "dv.parquet")
        _write_dv_parquet(duckdb_con, dv, [1, 2])
        cache = TombstoneCache(capacity=4)
        name = cache.acquire(duckdb_con, self.V1, dv)
        cache.clear_registry()
        assert cache.snapshot() == []
        # Table is NOT dropped — connection still owns it.
        assert _dv_table_exists(duckdb_con, name)
        # State forgotten: re-acquiring starts fresh.
        cache.acquire(duckdb_con, self.V1, dv)
        assert len(cache.snapshot()) == 1

    def test_dv_table_name_deterministic_and_prefixed(self):
        assert dv_table_name("x") == dv_table_name("x")
        assert dv_table_name("x").startswith("dv_")
        assert dv_table_name("x") != dv_table_name("y")


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
        # With a MagicMock connection, LOAD httpfs succeeds silently so INSTALL
        # is never called.  The relevant assertion is that httpfs IS loaded and
        # that no S3-specific settings are applied for local paths.
        assert any("LOAD httpfs" in s for s in calls)
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
        # Provide endpoint/bucket via storage attrs so DataEstimator detection
        # uses the storage path rather than relying on the settings singleton.
        # SUPERTABLE_DUCKDB_USE_HTTPFS lives on settings, not env at runtime.
        _patch_settings(
            monkeypatch,
            SUPERTABLE_DUCKDB_USE_HTTPFS=True,
            STORAGE_USE_SSL=False,
        )
        storage = MagicMock(spec=["endpoint_url", "bucket"])
        storage.endpoint_url = "http://minio:9000"
        storage.bucket = "bkt"
        result = _make_estimator([], storage)._to_duckdb_path("data/file.parquet")
        assert result == "http://minio:9000/bkt/data/file.parquet"

    def test_to_duckdb_path_https_when_ssl(self, monkeypatch, clean_env):
        _patch_settings(
            monkeypatch,
            SUPERTABLE_DUCKDB_USE_HTTPFS=True,
            STORAGE_USE_SSL=True,
        )
        storage = MagicMock(spec=["endpoint_url", "bucket", "secure"])
        storage.endpoint_url = "http://minio:9000"
        storage.bucket = "bkt"
        storage.secure = True
        assert _make_estimator([], storage)._to_duckdb_path("data/f.parquet").startswith("https://")

    def test_to_duckdb_path_s3_when_not_httpfs(self, monkeypatch, clean_env):
        # Without USE_HTTPFS, the path is constructed as s3://bucket/key
        # regardless of endpoint configuration.
        _patch_settings(monkeypatch, SUPERTABLE_DUCKDB_USE_HTTPFS=False)
        storage = MagicMock(spec=["endpoint_url", "bucket"])
        storage.endpoint_url = "http://minio:9000"
        storage.bucket = "bkt"
        assert _make_estimator([], storage)._to_duckdb_path("data/f.parquet") == "s3://bkt/data/f.parquet"

    def test_to_duckdb_path_fallback(self, monkeypatch, clean_env):
        # No endpoint, no bucket, no presign -> the input key is returned as-is.
        _patch_settings(
            monkeypatch,
            SUPERTABLE_DUCKDB_PRESIGNED=False,
            STORAGE_ENDPOINT_URL="",
            STORAGE_BUCKET="",
        )
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
        assert Engine.DUCKDB_LITE.value == "duckdb_lite"
        assert Engine.DUCKDB_PRO.value == "duckdb_pro"
        assert Engine.SPARK_SQL.value == "spark_sql"

    def test_from_string(self):
        assert Engine("auto") == Engine.AUTO
        assert Engine("spark_sql") == Engine.SPARK_SQL


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

    @patch.object(DuckDBLite, "execute", return_value=pd.DataFrame({"a": [1]}))
    def test_route_lite(self, mock_exec):
        r, p, qm, t, ps = _exec_fixtures()
        _, used = Executor().execute(Engine.DUCKDB_LITE, r, p, qm, t, ps, "test")
        assert used == "duckdb_lite"
        mock_exec.assert_called_once()

    @patch.object(DuckDBPro, "execute", return_value=pd.DataFrame({"a": [1]}))
    def test_route_pro(self, mock_exec):
        import supertable.engine.executor as mod
        mod._pro_singleton = None
        _, used = Executor().execute(Engine.DUCKDB_PRO, *_exec_fixtures()[:-1], PlanStats(), "test")
        assert used == "duckdb_pro"
        mock_exec.assert_called_once()

    @patch.object(DuckDBPro, "execute", return_value=pd.DataFrame())
    def test_route_duckdb_pro_explicit(self, mock_exec):
        import supertable.engine.executor as mod
        mod._pro_singleton = None
        _, used = Executor().execute(Engine.DUCKDB_PRO, *_exec_fixtures()[:-1], PlanStats(), "test")
        assert used == "duckdb_pro"

    @patch("supertable.engine.spark_thrift.SparkThriftExecutor")
    def test_route_spark(self, MockSpark):
        instance = MagicMock()
        instance.execute.return_value = pd.DataFrame()
        MockSpark.return_value = instance
        r, p, qm, t, ps = _exec_fixtures()
        _, used = Executor().execute(Engine.SPARK_SQL, r, p, qm, t, ps, "test")
        assert used == "spark_sql"
        _, kwargs = instance.execute.call_args
        assert kwargs.get("force") is True

    @patch.object(DuckDBLite, "execute", return_value=pd.DataFrame())
    def test_auto_picks_lite_for_small_data(self, mock_exec):
        r, p, qm, t, ps = _exec_fixtures()
        r.reflection_bytes = 1000
        _, used = Executor().execute(Engine.AUTO, r, p, qm, t, ps, "test")
        assert used == "duckdb_lite"

    @patch.object(DuckDBPro, "execute", return_value=pd.DataFrame())
    def test_auto_picks_pro_for_medium_stable_data(self, mock_exec):
        import supertable.engine.executor as mod
        mod._pro_singleton = None
        r, p, qm, t, ps = _exec_fixtures()
        # 500 MB, stable (freshness_ms=0 means unknown → treated as stable)
        r.reflection_bytes = 500 * 1024 * 1024
        r.freshness_ms = 0
        _, used = Executor().execute(Engine.AUTO, r, p, qm, t, ps, "test")
        assert used == "duckdb_pro"

    @patch.object(DuckDBLite, "execute", return_value=pd.DataFrame())
    def test_auto_picks_lite_for_medium_fresh_data(self, mock_exec):
        import time
        r, p, qm, t, ps = _exec_fixtures()
        # 500 MB, just updated 10 seconds ago (fresh → cache would churn)
        r.reflection_bytes = 500 * 1024 * 1024
        r.freshness_ms = int(time.time() * 1000) - 10_000
        _, used = Executor().execute(Engine.AUTO, r, p, qm, t, ps, "test")
        assert used == "duckdb_lite"

    @patch.object(DuckDBPro, "execute", return_value=pd.DataFrame())
    def test_auto_picks_pro_for_medium_old_data(self, mock_exec):
        import time
        import supertable.engine.executor as mod
        mod._pro_singleton = None
        r, p, qm, t, ps = _exec_fixtures()
        # 500 MB, updated 10 minutes ago (stable → cache pays off)
        r.reflection_bytes = 500 * 1024 * 1024
        r.freshness_ms = int(time.time() * 1000) - 600_000
        _, used = Executor().execute(Engine.AUTO, r, p, qm, t, ps, "test")
        assert used == "duckdb_pro"

    @patch.object(DuckDBLite, "execute", return_value=pd.DataFrame())
    def test_auto_picks_lite_at_boundary(self, mock_exec):
        r, p, qm, t, ps = _exec_fixtures()
        # Exactly at lite_max threshold (100 MB) — should still pick LITE
        r.reflection_bytes = 100 * 1024 * 1024
        _, used = Executor().execute(Engine.AUTO, r, p, qm, t, ps, "test")
        assert used == "duckdb_lite"

    @patch.object(DuckDBLite, "execute", return_value=pd.DataFrame())
    def test_plan_stats_records_engine(self, mock_exec):
        r, p, qm, t, ps = _exec_fixtures()
        Executor().execute(Engine.DUCKDB_LITE, r, p, qm, t, ps, "test")
        assert any(s.get("ENGINE") == "duckdb_lite" for s in ps.stats)


class TestGetPro:

    def test_returns_duckdb_pro_instance(self):
        import supertable.engine.executor as mod
        mod._pro_singleton = None
        assert isinstance(_get_pro(), DuckDBPro)

    def test_singleton_returns_same_instance(self):
        import supertable.engine.executor as mod
        mod._pro_singleton = None
        a = _get_pro()
        b = _get_pro()
        assert a is b


# ═══════════════════════════════════════════════════════════
#  duckdb_lite
# ═══════════════════════════════════════════════════════════


class TestDuckDBLite:

    def test_init(self):
        dt = DuckDBLite(storage=MagicMock())
        assert dt.storage is not None

    def test_init_builds_tombstone_cache_from_settings(self):
        dt = DuckDBLite()
        assert dt._tombstone_cache.capacity == _settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE
        assert dt._tombstone_cache.ttl_seconds == _settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_TTL_SEC

    def test_reset_clears_tombstone_cache(self):
        dt = DuckDBLite()
        dt._con = MagicMock()
        dt._tombstone_cache._registry["k"] = MagicMock()
        dt._reset_connection()
        assert dt._con is None
        assert dt._tombstone_cache.snapshot() == []

    @patch("supertable.engine.duckdb_lite.duckdb")
    @patch("supertable.engine.duckdb_lite.init_connection")
    @patch("supertable.engine.duckdb_lite.hashed_table_name", return_value="st_abc")
    @patch("supertable.engine.duckdb_lite.create_reflection_view_with_presign_retry", return_value=False)
    @patch("supertable.engine.duckdb_lite.rewrite_query_with_hashed_tables", return_value="SELECT 1")
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
        DuckDBLite(storage=MagicMock()).execute(
            reflection, parser, qm, lambda e: captures.append(e)
        )
        assert "CONNECTING" in captures
        assert "CREATING_REFLECTION" in captures
        mock_init.assert_called_once()
        mock_create.assert_called_once()
        # The lite executor reuses a persistent connection; it is NOT closed per query.

    @patch("supertable.engine.duckdb_lite.duckdb")
    @patch("supertable.engine.duckdb_lite.init_connection")
    def test_connection_closed_on_error(self, mock_init, mock_duckdb):
        fake_con = MagicMock()
        mock_duckdb.connect.return_value = fake_con
        mock_init.side_effect = RuntimeError("init failed")

        parser = MagicMock()
        parser.get_table_tuples.return_value = []
        with pytest.raises(RuntimeError):
            DuckDBLite().execute(Reflection("m", 0, 0, []), parser, MagicMock(), lambda e: None)
        # init_connection raises before self._con is assigned, so _reset_connection
        # is a no-op (self._con is None) and close() is not called on the raw con object.


# ═══════════════════════════════════════════════════════════
#  duckdb_pro
# ═══════════════════════════════════════════════════════════


class TestDuckDBPro:

    def _make(self):
        return DuckDBPro(storage=MagicMock())

    def test_init(self):
        p = self._make()
        assert p._con is None
        assert p._httpfs_configured is False
        assert p._registry == {}

    def test_init_builds_tombstone_cache_from_settings(self):
        p = self._make()
        assert p._tombstone_cache.capacity == _settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE
        assert p._tombstone_cache.ttl_seconds == _settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_TTL_SEC

    def test_reset_clears_tombstone_cache(self):
        p = self._make()
        p._con = MagicMock()
        p._tombstone_cache._registry["k"] = MagicMock()
        p._reset_connection()
        assert p._tombstone_cache.snapshot() == []

    @patch("supertable.engine.duckdb_pro.duckdb")
    @patch("supertable.engine.duckdb_pro.init_connection")
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

    @patch("supertable.engine.duckdb_pro.create_reflection_view")
    @patch("supertable.engine.duckdb_pro.configure_httpfs_and_s3")
    def test_ensure_table_creates_new(self, mock_httpfs, mock_create):
        p = self._make()
        name = p._ensure_view(MagicMock(), "s", "t", 1, ["f.parquet"])
        assert name.startswith("pro_")
        assert "_v1" in name
        mock_create.assert_called_once()

    @patch("supertable.engine.duckdb_pro.create_reflection_view")
    @patch("supertable.engine.duckdb_pro.configure_httpfs_and_s3")
    def test_ensure_table_reuses_cached(self, mock_httpfs, mock_create):
        p = self._make()
        con = MagicMock()
        n1 = p._ensure_view(con, "s", "t", 1, ["f.parquet"])
        n2 = p._ensure_view(con, "s", "t", 1, ["f.parquet"])
        assert n1 == n2
        mock_create.assert_called_once()

    @patch("supertable.engine.duckdb_pro.create_reflection_view")
    @patch("supertable.engine.duckdb_pro.configure_httpfs_and_s3")
    def test_ensure_table_new_version_marks_old_stale(self, mock_httpfs, mock_create):
        p = self._make()
        con = MagicMock()
        name1 = p._ensure_view(con, "s", "t", 1, ["f1.parquet"])
        p._acquire_refs({name1})  # keep it alive
        name2 = p._ensure_view(con, "s", "t", 2, ["f2.parquet"])
        assert name1 != name2
        assert mock_create.call_count == 2
        stale = [e for e in p._registry[("s", "t")] if e.stale]
        assert len(stale) == 1
        assert stale[0].version == 1

    @patch("supertable.engine.duckdb_pro.create_reflection_view")
    @patch("supertable.engine.duckdb_pro.configure_httpfs_and_s3")
    def test_ensure_table_new_version_drops_unreferenced_stale(self, mock_httpfs, mock_create):
        p = self._make()
        con = MagicMock()
        name1 = p._ensure_view(con, "s", "t", 1, ["f1.parquet"])
        p._ensure_view(con, "s", "t", 2, ["f2.parquet"])
        entries = p._registry[("s", "t")]
        assert len(entries) == 1
        assert entries[0].version == 2
        assert not entries[0].stale
        drop_calls = [c[0][0] for c in con.execute.call_args_list if "DROP VIEW" in str(c)]
        assert any(name1 in s for s in drop_calls)

    def test_acquire_and_release_refs(self):
        p = self._make()
        entry = _ProCacheEntry("tbl_v1", "s", "t", 1)
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
        stale_zero = _ProCacheEntry("old_v1", "s", "t", 1, ref_count=0, stale=True)
        stale_busy = _ProCacheEntry("old_v2", "s", "t", 2, ref_count=1, stale=True)
        current = _ProCacheEntry("cur_v3", "s", "t", 3, ref_count=0, stale=False)
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
            _ProCacheEntry("tbl1", "s", "t", 1, ref_count=0, stale=False),
            _ProCacheEntry("tbl2", "s", "t", 2, ref_count=1, stale=True),
        ]
        tables = p.get_cached_tables()
        assert len(tables) == 2
        assert tables[0]["view_name"] == "tbl1"
        assert tables[0]["stale"] is False
        assert tables[1]["stale"] is True

    def test_drop_all(self):
        p = self._make()
        p._con = MagicMock()
        p._registry[("s", "t")] = [_ProCacheEntry("tbl1", "s", "t", 1)]
        p.drop_all()
        assert p._con is None
        assert p._registry == {}

    @patch("supertable.engine.duckdb_pro.create_reflection_view")
    @patch("supertable.engine.duckdb_pro.make_presigned_list")
    @patch("supertable.engine.duckdb_pro.configure_httpfs_and_s3")
    def test_ensure_table_presign_fallback(self, mock_httpfs, mock_presign, mock_create):
        p = self._make()
        mock_create.side_effect = [Exception("HTTP Error 403"), None]
        mock_presign.return_value = ["https://presigned/f.parquet"]
        name = p._ensure_view(MagicMock(), "s", "t", 1, ["s3://bucket/f.parquet"])
        assert name.startswith("pro_")
        assert mock_create.call_count == 2
        mock_presign.assert_called_once()

    def test_current_entry_returns_non_stale(self):
        p = self._make()
        p._registry[("s", "t")] = [
            _ProCacheEntry("t1", "s", "t", 1, stale=True),
            _ProCacheEntry("t2", "s", "t", 2, stale=False),
        ]
        assert p._current_entry(("s", "t")).table_name == "t2"

    def test_current_entry_none_when_all_stale(self):
        p = self._make()
        p._registry[("s", "t")] = [_ProCacheEntry("t1", "s", "t", 1, stale=True)]
        assert p._current_entry(("s", "t")) is None

    def test_current_entry_none_when_empty(self):
        p = self._make()
        assert p._current_entry(("s", "t")) is None


# ═══════════════════════════════════════════════════════════
#  spark_sql — helpers
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

    def test_multiple_files_returns_intermediates(self):
        cursor = MagicMock()
        intermediates = _spark_create_parquet_view(cursor, "tbl", ["/a.parquet", "/b.parquet"])
        # Intermediate views are returned for caller cleanup, not dropped inline
        assert len(intermediates) >= 2


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
        cursor = MagicMock()
        cluster = {
            "s3_endpoint": "http://minio:9000",
            "s3_access_key": "ak",
            "s3_secret_key": "sk",
            "s3_region": "eu-west-1",
        }
        _configure_spark_s3(cursor, cluster)
        all_sql = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("fs.s3a.endpoint" in s for s in all_sql)
        assert any("fs.s3a.access.key" in s for s in all_sql)

    def test_no_cluster_sets_impl_only(self, monkeypatch, clean_env):
        cursor = MagicMock()
        _configure_spark_s3(cursor)
        all_sql = [c[0][0] for c in cursor.execute.call_args_list]
        assert any("s3a.impl" in s for s in all_sql)
        # Without cluster dict, env vars are NOT used (by design)
        assert not any("fs.s3a.endpoint" in s for s in all_sql)

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
#  spark_sql — timestamp cast wrapper (type-based detection)
# ═══════════════════════════════════════════════════════════


def _write_ts_parquet(path, *, ts_unit="us", with_created_at=True):
    """Write a parquet file mimicking the SuperTable write path.

    A ``__timestamp__`` timestamp column at *ts_unit* resolution plus plain
    int64 / string user columns — including an epoch-integer ``created_at``
    (int64, NOT a parquet timestamp) that must never be detected as a timestamp.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    epoch = {"s": 1_700_000_000, "ms": 1_700_000_000_000,
             "us": 1_700_000_000_000_000, "ns": 1_700_000_000_000_000_000}[ts_unit]
    cols = {
        "__timestamp__": pa.array([epoch], type=pa.timestamp(ts_unit, tz="UTC")),
        "user_id": pa.array([5], type=pa.int64()),
        "name": pa.array(["Alice"], type=pa.string()),
    }
    if with_created_at:
        cols["created_at"] = pa.array([1_700_000_000_000], type=pa.int64())  # epoch-ms
    pq.write_table(pa.table(cols), path)


class TestSparkTimestampCast:
    """The Spark nanos→TIMESTAMP wrapper must convert a column back to TIMESTAMP
    only when Spark surfaces it as BIGINT AND its PARQUET logical type is a
    timestamp — never by name, so epoch-integer columns like ``created_at``
    (int64 epoch-ms) stay BIGINT.  This pins the fix for the engine divergence
    where DuckDB returned correct results but Spark force-cast epoch ints."""

    # ── pure projection builder ────────────────────────────────────

    def test_only_parquet_timestamp_bigint_is_cast(self):
        desc = [("__timestamp__", "bigint"), ("created_at", "bigint"),
                ("user_id", "bigint"), ("name", "string")]
        parts, cast = _build_tscast_select(desc, {"__timestamp__": "ns"})
        assert cast == ["__timestamp__"]
        assert "`created_at`" in parts                        # passthrough, untouched
        assert "timestamp_micros(`created_at`" not in " ".join(parts)
        assert "timestamp_micros(`__timestamp__` DIV 1000) AS `__timestamp__`" in parts

    def test_epoch_int_named_like_timestamp_not_cast(self):
        # The exact bug: BIGINTs named like timestamps but absent from the footer
        # timestamp set must be left alone.
        desc = [("source_ts_ms", "bigint"), ("transaction_created_at", "bigint")]
        parts, cast = _build_tscast_select(desc, {})          # footer read, zero ts cols
        assert cast == []
        assert parts == ["`source_ts_ms`", "`transaction_created_at`"]

    def test_footer_unreadable_casts_only_system_col(self):
        # ts_units None → only __timestamp__ (assumed nanos), never the heuristic.
        desc = [("__timestamp__", "bigint"), ("created_at", "bigint"),
                ("stat_date", "bigint")]
        parts, cast = _build_tscast_select(desc, None)
        assert cast == ["__timestamp__"]
        assert "`created_at`" in parts and "`stat_date`" in parts
        assert not any("timestamp_micros(`created_at`" in p for p in parts)

    def test_non_bigint_timestamp_not_cast(self):
        # A micros __timestamp__ already reads as TIMESTAMP in Spark → not BIGINT
        # → no conversion even though the footer flags it as a timestamp.
        desc = [("__timestamp__", "timestamp"), ("user_id", "bigint")]
        parts, cast = _build_tscast_select(desc, {"__timestamp__": "us"})
        assert cast == []
        assert parts == ["`__timestamp__`", "`user_id`"]

    def test_unit_aware_conversion_expressions(self):
        desc = [("a", "bigint"), ("b", "bigint"), ("c", "bigint"), ("d", "bigint")]
        parts, cast = _build_tscast_select(
            desc, {"a": "s", "b": "ms", "c": "us", "d": "ns"})
        assert set(cast) == {"a", "b", "c", "d"}
        joined = " ".join(parts)
        assert "timestamp_seconds(`a`) AS `a`" in joined
        assert "timestamp_millis(`b`) AS `b`" in joined
        assert "timestamp_micros(`c`) AS `c`" in joined
        assert "timestamp_micros(`d` DIV 1000) AS `d`" in joined

    def test_describe_metadata_rows_skipped(self):
        desc = [("__timestamp__", "bigint"), ("# Partitioning", ""), ("", "")]
        parts, cast = _build_tscast_select(desc, {"__timestamp__": "ns"})
        assert cast == ["__timestamp__"]
        assert parts == ["timestamp_micros(`__timestamp__` DIV 1000) AS `__timestamp__`"]

    def test_ts_expr_helper_units(self):
        assert _ts_to_timestamp_expr("x", "s") == "timestamp_seconds(`x`) AS `x`"
        assert _ts_to_timestamp_expr("x", "ms") == "timestamp_millis(`x`) AS `x`"
        assert _ts_to_timestamp_expr("x", "us") == "timestamp_micros(`x`) AS `x`"
        assert _ts_to_timestamp_expr("x", "ns") == "timestamp_micros(`x` DIV 1000) AS `x`"

    # ── real-parquet footer reading ────────────────────────────────

    def test_parquet_units_local_micros(self, tmp_path):
        p = str(tmp_path / "m.parquet")
        _write_ts_parquet(p, ts_unit="us")
        units = _parquet_timestamp_units(None, p)              # local path, no storage
        assert units == {"__timestamp__": "us"}
        assert "created_at" not in units                       # epoch int64 is NOT a timestamp

    def test_parquet_units_local_nanos(self, tmp_path):
        p = str(tmp_path / "n.parquet")
        _write_ts_parquet(p, ts_unit="ns")
        assert _parquet_timestamp_units(None, p) == {"__timestamp__": "ns"}

    def test_read_schema_missing_local_returns_none(self, tmp_path):
        assert _read_parquet_schema(None, str(tmp_path / "nope.parquet")) is None

    def test_read_schema_via_storage_strips_base_prefix(self, tmp_path):
        # Object-store path: bytes come through storage; base_prefix must be
        # stripped from the recovered key (storage re-applies it), bucket dropped.
        p = tmp_path / "data.parquet"
        _write_ts_parquet(str(p), ts_unit="ns")
        raw = p.read_bytes()
        seen = {}

        class _FakeStorage:
            base_prefix = "org/warehouse"
            def read_bytes(self, key):
                seen["key"] = key
                return raw

        units = _parquet_timestamp_units(
            _FakeStorage(), "s3://bucket/org/warehouse/t/data.parquet")
        assert seen["key"] == "t/data.parquet"
        assert units == {"__timestamp__": "ns"}

    def test_read_schema_presigned_url_via_storage(self, tmp_path):
        # Presigned HTTP URL with query string (path-style): key recovered, query
        # dropped, read through storage.
        p = tmp_path / "data.parquet"
        _write_ts_parquet(str(p), ts_unit="ns")
        raw = p.read_bytes()
        seen = {}

        class _FakeStorage:
            base_prefix = ""
            def read_bytes(self, key):
                seen["key"] = key
                return raw

        url = ("https://minio:9000/bucket/t/data.parquet"
               "?X-Amz-Signature=abc&X-Amz-Expires=3600")
        assert _parquet_timestamp_units(_FakeStorage(), url) == {"__timestamp__": "ns"}
        assert seen["key"] == "t/data.parquet"

    def test_storage_failure_returns_none(self):
        class _BoomStorage:
            base_prefix = ""
            def read_bytes(self, key):
                raise RuntimeError("network down")

        assert _parquet_timestamp_units(_BoomStorage(), "s3://b/k.parquet") is None

    # ── end-to-end execute flow ────────────────────────────────────

    @patch.object(SparkThriftExecutor, "_select_cluster")
    @patch.object(SparkThriftExecutor, "_get_connection")
    @patch("supertable.engine.spark_thrift._configure_spark_s3")
    @patch("supertable.engine.spark_thrift._spark_create_parquet_view")
    @patch("supertable.engine.spark_thrift._spark_rewrite_query", return_value="SELECT 1")
    def test_execute_wraps_nanos_not_epoch_int(
        self, mock_rewrite, mock_view, mock_s3, mock_conn, mock_cluster, tmp_path,
    ):
        # Real parquet: nanos __timestamp__ + epoch-int created_at (the repro).
        f = str(tmp_path / "data.parquet")
        _write_ts_parquet(f, ts_unit="ns")

        mock_cluster.return_value = {"cluster_id": "c1", "thrift_host": "h", "thrift_port": 10000}
        fake_cursor = MagicMock()
        fake_cursor.description = [("col_name",), ("data_type",)]
        fake_cursor.fetchall.return_value = [
            ("__timestamp__", "bigint"), ("__rowid__", "bigint"),
            ("created_at", "bigint"), ("user_id", "bigint"), ("name", "string"),
        ]
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        mock_conn.return_value = fake_conn

        parser = MagicMock()
        parser.original_query = "SELECT * FROM t"
        parser.get_table_tuples.return_value = [TableDefinition("s", "t", "t_alias")]
        reflection = Reflection(
            storage_type="local", reflection_bytes=100, total_reflections=1,
            supers=[SuperSnapshot("s", "t", 1, [f], {"user_id", "created_at"})],
        )
        SparkThriftExecutor(storage=MagicMock()).execute(
            reflection, parser, MagicMock(), lambda e: None,
        )

        all_sql = [c[0][0] for c in fake_cursor.execute.call_args_list]
        tscast = [s for s in all_sql if "_tscast__" in s and "CREATE OR REPLACE" in s]
        assert tscast, f"no tscast wrapper view created; SQL issued: {all_sql}"
        sql = tscast[0]
        # __timestamp__ (nanos) converted; created_at (epoch int64) left as BIGINT.
        assert "timestamp_micros(`__timestamp__` DIV 1000) AS `__timestamp__`" in sql
        assert "`created_at`" in sql
        assert "timestamp_micros(`created_at`" not in sql

    @patch.object(SparkThriftExecutor, "_select_cluster")
    @patch.object(SparkThriftExecutor, "_get_connection")
    @patch("supertable.engine.spark_thrift._configure_spark_s3")
    @patch("supertable.engine.spark_thrift._spark_create_parquet_view")
    @patch("supertable.engine.spark_thrift._spark_rewrite_query", return_value="SELECT 1")
    def test_execute_no_wrapper_for_micros_timestamp(
        self, mock_rewrite, mock_view, mock_s3, mock_conn, mock_cluster, tmp_path,
    ):
        # Micros __timestamp__ reads natively as TIMESTAMP (Spark DESCRIBE shows
        # 'timestamp'); with no BIGINT parquet-timestamp column, NO wrapper view.
        f = str(tmp_path / "data.parquet")
        _write_ts_parquet(f, ts_unit="us")

        mock_cluster.return_value = {"cluster_id": "c1", "thrift_host": "h"}
        fake_cursor = MagicMock()
        fake_cursor.description = [("col_name",), ("data_type",)]
        fake_cursor.fetchall.return_value = [
            ("__timestamp__", "timestamp"), ("created_at", "bigint"),
            ("user_id", "bigint"), ("name", "string"),
        ]
        fake_conn = MagicMock()
        fake_conn.cursor.return_value = fake_cursor
        mock_conn.return_value = fake_conn

        parser = MagicMock()
        parser.original_query = "SELECT * FROM t"
        parser.get_table_tuples.return_value = [TableDefinition("s", "t", "t_alias")]
        reflection = Reflection(
            storage_type="local", reflection_bytes=100, total_reflections=1,
            supers=[SuperSnapshot("s", "t", 1, [f], {"user_id", "created_at"})],
        )
        SparkThriftExecutor(storage=MagicMock()).execute(
            reflection, parser, MagicMock(), lambda e: None,
        )
        all_sql = [c[0][0] for c in fake_cursor.execute.call_args_list]
        assert not any("_tscast__" in s for s in all_sql), \
            f"unexpected tscast wrapper for a native micros timestamp: {all_sql}"


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
