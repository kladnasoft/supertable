# route: supertable.config.tests.test_settings
"""Tests for ``supertable.config.settings``.

The module is built once at import time; it is structured around small,
pure parsing helpers (``_env_str``, ``_env_int``, ``_env_float``,
``_env_bool``, ``_env_float_optional``) plus a single immutable
``Settings`` dataclass that aggregates them. We test:

  - Each helper across the documented fallback chain
  - The ``Settings`` dataclass: immutability, expected default values
  - The ``_build_settings`` function across env permutations:
      * Core env vars
      * Storage type uppercasing
      * Truthy/falsy bool parsing
      * Token fallback chains (SUPERTABLE_SUPERUSER_TOKEN / _SUPERTOKEN,
        SUPERTABLE_MCP_TOKEN / _MCP_AUTH_TOKEN)
      * API host fallback default
      * Meta cache TTL "must be > 0" rule
  - The ``Settings.effective_*`` convenience properties
"""
from __future__ import annotations

import dataclasses

import pytest

from supertable.config import settings as settings_module
from supertable.config.settings import (
    Settings,
    _build_settings,
    _env_bool,
    _env_float,
    _env_float_optional,
    _env_int,
    _env_str,
    settings,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def fresh_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """Clear every environment variable that ``_build_settings`` reads.

    Callers can then set only the variables they want to exercise.
    """
    keys = [
        # Core
        "SUPERTABLE_HOME",
        "SUPERTABLE_ORGANIZATION",
        "SUPERTABLE_PREFIX",
        "DOTENV_PATH",
        # Defaults
        "MAX_MEMORY_CHUNK_SIZE",
        "MAX_OVERLAPPING_FILES",
        "MAX_TOMBSTONE_ROWS",
        "TOMBSTONE_COMPACTION_WORKERS",
        "SUPERTABLE_DV_V2_WRITES_ENABLED",
        "SUPERTABLE_DV_V3_WRITES_ENABLED",
        "DEFAULT_TIMEOUT_SEC",
        "DEFAULT_LOCK_DURATION_SEC",
        "IS_SHOW_TIMING",
        # Storage
        "STORAGE_TYPE",
        "STORAGE_BUCKET",
        "STORAGE_REGION",
        "STORAGE_ENDPOINT_URL",
        "STORAGE_ACCESS_KEY",
        "STORAGE_SECRET_KEY",
        "STORAGE_USE_SSL",
        "STORAGE_FORCE_PATH_STYLE",
        # Azure / GCP
        "AZURE_STORAGE_ACCOUNT",
        "AZURE_CONTAINER",
        "AZURE_BLOB_ENDPOINT",
        "GCS_BUCKET",
        "GCP_PROJECT",
        # DuckDB write probe
        "SUPERTABLE_DUCKDB_WRITE_PROBE",
        "SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO",
        # Tokens
        "SUPERTABLE_SUPERUSER_TOKEN",
        "SUPERTABLE_SUPERTOKEN",
        "SUPERTABLE_MCP_TOKEN",
        "SUPERTABLE_MCP_AUTH_TOKEN",
        # Query observation history
        "SUPERTABLE_QUERY_OBSERVATIONS_ENABLED",
        "SUPERTABLE_QUERY_OBSERVATION_MAX_SAMPLES",
        "SUPERTABLE_QUERY_OBSERVATION_TTL_DAYS",
        "SUPERTABLE_QUERY_OBSERVATION_MIN_SAMPLES",
        "SUPERTABLE_QUERY_OBSERVATION_MAX_SIGNATURES",
        # IslandDB / shared cache
        "SUPERTABLE_ISLAND_CACHE_ENABLED",
        "SUPERTABLE_ISLAND_CACHE_DIR",
        "SUPERTABLE_ISLAND_CACHE_MAX_BYTES",
        "SUPERTABLE_ISLAND_CACHE_TTL_SEC",
        "SUPERTABLE_ISLAND_CACHE_WORKERS",
        "SUPERTABLE_ISLAND_AUTO_ENABLED",
        "SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED",
        "SUPERTABLE_ISLAND_RANGE_CACHE_DIR",
        "SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES",
        "SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC",
        "SUPERTABLE_ISLAND_MEMORY_FRACTION",
        "SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION",
        "SUPERTABLE_ISLAND_MAX_MEMORY_BYTES",
        "SUPERTABLE_ISLAND_MAX_RESULT_BYTES",
        "SUPERTABLE_ISLAND_CPU_MAX",
        "SUPERTABLE_ISLAND_IO_WORKERS_MAX",
        "SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC",
        "SUPERTABLE_ISLAND_SPILL_ENABLED",
        "SUPERTABLE_ISLAND_SPILL_DIR",
        "SUPERTABLE_ISLAND_SPILL_MAX_BYTES",
        "SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES",
        # API + Redis
        "SUPERTABLE_API_HOST",
        "SUPERTABLE_API_PORT",
        "SUPERTABLE_REDIS_URL",
        "SUPERTABLE_REDIS_HOST",
        "SUPERTABLE_REDIS_PORT",
        "SUPERTABLE_REDIS_DB",
        "SUPERTABLE_REDIS_SSL",
        "SUPERTABLE_REDIS_SSL_CA_CERTS",
        "SUPERTABLE_REDIS_SENTINEL",
        "SUPERTABLE_REDIS_SENTINELS",
        "SUPERTABLE_REDIS_SENTINEL_MASTER",
        "SUPERTABLE_REDIS_SENTINEL_PASSWORD",
        # Misc
        "SUPERTABLE_SUPER_META_CACHE_TTL_S",
        "XDG_CONFIG_HOME",
    ]
    for k in keys:
        monkeypatch.delenv(k, raising=False)
    return monkeypatch


# ---------------------------------------------------------------------------
# _env_str
# ---------------------------------------------------------------------------


class TestEnvStr:
    def test_returns_value(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "bar")
        assert _env_str("FOO") == "bar"

    def test_strips_whitespace(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "  bar  ")
        assert _env_str("FOO") == "bar"

    def test_returns_default_when_unset(self, fresh_env: pytest.MonkeyPatch) -> None:
        assert _env_str("FOO", "fallback") == "fallback"

    def test_returns_default_when_empty(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "")
        assert _env_str("FOO", "fallback") == "fallback"


# ---------------------------------------------------------------------------
# _env_int
# ---------------------------------------------------------------------------


class TestEnvInt:
    def test_parses_integer(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "42")
        assert _env_int("FOO", 0) == 42

    def test_default_when_unset(self, fresh_env: pytest.MonkeyPatch) -> None:
        assert _env_int("FOO", 7) == 7

    def test_default_when_empty(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "")
        assert _env_int("FOO", 7) == 7

    def test_default_on_garbage(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "not-an-int")
        assert _env_int("FOO", 7) == 7

    def test_negative_value_is_kept(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "-5")
        assert _env_int("FOO", 0) == -5


# ---------------------------------------------------------------------------
# _env_float
# ---------------------------------------------------------------------------


class TestEnvFloat:
    def test_parses_float(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "1.5")
        assert _env_float("FOO", 0.0) == 1.5

    def test_default_on_garbage(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "x")
        assert _env_float("FOO", 9.9) == 9.9


# ---------------------------------------------------------------------------
# _env_bool
# ---------------------------------------------------------------------------


class TestEnvBool:
    @pytest.mark.parametrize("v", ["1", "true", "TRUE", "yes", "Y", "on"])
    def test_truthy(self, fresh_env: pytest.MonkeyPatch, v: str) -> None:
        fresh_env.setenv("FOO", v)
        assert _env_bool("FOO", False) is True

    @pytest.mark.parametrize("v", ["0", "false", "FALSE", "no", "n", "off"])
    def test_falsy(self, fresh_env: pytest.MonkeyPatch, v: str) -> None:
        fresh_env.setenv("FOO", v)
        assert _env_bool("FOO", True) is False

    def test_default_when_empty(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "")
        assert _env_bool("FOO", True) is True
        assert _env_bool("FOO", False) is False

    def test_default_on_unknown(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "maybe")
        assert _env_bool("FOO", True) is True


# ---------------------------------------------------------------------------
# _env_float_optional
# ---------------------------------------------------------------------------


class TestEnvFloatOptional:
    def test_returns_positive(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "2.5")
        assert _env_float_optional("FOO") == 2.5

    def test_returns_none_for_zero_or_negative(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("FOO", "0")
        assert _env_float_optional("FOO") is None
        fresh_env.setenv("FOO", "-1")
        assert _env_float_optional("FOO") is None

    def test_returns_none_when_unset_or_invalid(self, fresh_env: pytest.MonkeyPatch) -> None:
        assert _env_float_optional("FOO") is None
        fresh_env.setenv("FOO", "bad")
        assert _env_float_optional("FOO") is None


# ---------------------------------------------------------------------------
# Settings dataclass
# ---------------------------------------------------------------------------


class TestSettingsDataclass:
    def test_is_frozen(self) -> None:
        with pytest.raises(dataclasses.FrozenInstanceError):
            settings.STORAGE_BUCKET = "other"  # type: ignore[misc]

    def test_module_singleton_is_a_settings_instance(self) -> None:
        assert isinstance(settings, Settings)

    def test_documented_defaults(self) -> None:
        # Pin the values that downstream code relies on.
        s = Settings()
        assert s.SUPERTABLE_HOME == "~/supertable"
        assert s.STORAGE_TYPE == "LOCAL"
        assert s.STORAGE_BUCKET == "supertable"
        assert s.SUPERTABLE_REDIS_HOST == "localhost"
        assert s.SUPERTABLE_REDIS_PORT == 6379
        assert s.SUPERTABLE_API_PORT == 8051
        assert s.SUPERTABLE_DEFAULT_LIMIT == 10000
        assert s.SUPERTABLE_MAX_LIMIT == 10000
        assert s.SUPERTABLE_DEFAULT_ENGINE == "AUTO"
        assert s.SUPERTABLE_DUCKDB_WRITE_PROBE is False
        assert s.SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO is True
        assert s.SUPERTABLE_QUERY_OBSERVATIONS_ENABLED is True
        assert s.SUPERTABLE_QUERY_OBSERVATION_MAX_SAMPLES == 64
        assert s.SUPERTABLE_QUERY_OBSERVATION_TTL_DAYS == 30
        assert s.SUPERTABLE_QUERY_OBSERVATION_MIN_SAMPLES == 3
        assert s.SUPERTABLE_QUERY_OBSERVATION_MAX_SIGNATURES == 4096
        assert s.SUPERTABLE_ISLAND_CACHE_ENABLED is True
        assert s.SUPERTABLE_ISLAND_CACHE_MAX_BYTES == 20 * 1024 * 1024 * 1024
        assert s.SUPERTABLE_ISLAND_CACHE_TTL_SEC == 0
        assert s.SUPERTABLE_ISLAND_AUTO_ENABLED is True
        assert s.SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED is True
        assert s.SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC == 0
        assert s.SUPERTABLE_ISLAND_MEMORY_FRACTION == 0.60
        assert s.SUPERTABLE_ISLAND_MAX_RESULT_BYTES == 512 * 1024 * 1024
        assert s.SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC == 300.0
        assert s.SUPERTABLE_ISLAND_SPILL_ENABLED is True
        assert s.MAX_MEMORY_CHUNK_SIZE == 16 * 1024 * 1024
        assert s.MAX_OVERLAPPING_FILES == 100
        assert s.MAX_TOMBSTONE_ROWS == 1_000_000
        assert s.TOMBSTONE_COMPACTION_WORKERS == 2
        assert s.SUPERTABLE_DV_V2_WRITES_ENABLED is False
        assert s.SUPERTABLE_DV_V3_WRITES_ENABLED is False
        assert s.DEFAULT_TIMEOUT_SEC == 60
        assert s.DEFAULT_LOCK_DURATION_SEC == 30

    def test_island_query_timeout_environment_override(
        self, fresh_env: pytest.MonkeyPatch,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC", "12.5")

        built = _build_settings()

        assert built.SUPERTABLE_ISLAND_QUERY_TIMEOUT_SEC == 12.5

    def test_query_observation_environment_overrides(
        self, fresh_env: pytest.MonkeyPatch,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_QUERY_OBSERVATIONS_ENABLED", "false")
        fresh_env.setenv("SUPERTABLE_QUERY_OBSERVATION_MAX_SAMPLES", "29")
        fresh_env.setenv("SUPERTABLE_QUERY_OBSERVATION_TTL_DAYS", "11")
        fresh_env.setenv("SUPERTABLE_QUERY_OBSERVATION_MIN_SAMPLES", "5")
        fresh_env.setenv("SUPERTABLE_QUERY_OBSERVATION_MAX_SIGNATURES", "900")

        built = _build_settings()

        assert built.SUPERTABLE_QUERY_OBSERVATIONS_ENABLED is False
        assert built.SUPERTABLE_QUERY_OBSERVATION_MAX_SAMPLES == 29
        assert built.SUPERTABLE_QUERY_OBSERVATION_TTL_DAYS == 11
        assert built.SUPERTABLE_QUERY_OBSERVATION_MIN_SAMPLES == 5
        assert built.SUPERTABLE_QUERY_OBSERVATION_MAX_SIGNATURES == 900

    def test_island_cache_environment_overrides(
        self, fresh_env: pytest.MonkeyPatch,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_ISLAND_CACHE_ENABLED", "false")
        fresh_env.setenv("SUPERTABLE_ISLAND_CACHE_DIR", "/tmp/island-cache")
        fresh_env.setenv("SUPERTABLE_ISLAND_CACHE_MAX_BYTES", "123456")
        fresh_env.setenv("SUPERTABLE_ISLAND_CACHE_TTL_SEC", "77")
        fresh_env.setenv("SUPERTABLE_ISLAND_CACHE_WORKERS", "3")
        fresh_env.setenv("SUPERTABLE_ISLAND_AUTO_ENABLED", "true")
        fresh_env.setenv("SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED", "false")
        fresh_env.setenv("SUPERTABLE_ISLAND_RANGE_CACHE_DIR", "/tmp/island-ranges")
        fresh_env.setenv("SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES", "456789")
        fresh_env.setenv("SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC", "88")
        fresh_env.setenv("SUPERTABLE_ISLAND_MEMORY_FRACTION", "0.55")
        fresh_env.setenv("SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION", "0.75")
        fresh_env.setenv("SUPERTABLE_ISLAND_MAX_MEMORY_BYTES", "999999")
        fresh_env.setenv("SUPERTABLE_ISLAND_MAX_RESULT_BYTES", "111111")
        fresh_env.setenv("SUPERTABLE_ISLAND_CPU_MAX", "4")
        fresh_env.setenv("SUPERTABLE_ISLAND_IO_WORKERS_MAX", "6")
        fresh_env.setenv("SUPERTABLE_ISLAND_SPILL_ENABLED", "false")
        fresh_env.setenv("SUPERTABLE_ISLAND_SPILL_DIR", "/tmp/island-spill")
        fresh_env.setenv("SUPERTABLE_ISLAND_SPILL_MAX_BYTES", "222222")
        fresh_env.setenv("SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES", "333333")

        built = _build_settings()

        assert built.SUPERTABLE_ISLAND_CACHE_ENABLED is False
        assert built.SUPERTABLE_ISLAND_CACHE_DIR == "/tmp/island-cache"
        assert built.SUPERTABLE_ISLAND_CACHE_MAX_BYTES == 123456
        assert built.SUPERTABLE_ISLAND_CACHE_TTL_SEC == 77
        assert built.SUPERTABLE_ISLAND_CACHE_WORKERS == 3
        assert built.SUPERTABLE_ISLAND_AUTO_ENABLED is True
        assert built.SUPERTABLE_ISLAND_RANGE_CACHE_ENABLED is False
        assert built.SUPERTABLE_ISLAND_RANGE_CACHE_DIR == "/tmp/island-ranges"
        assert built.SUPERTABLE_ISLAND_RANGE_CACHE_MAX_BYTES == 456789
        assert built.SUPERTABLE_ISLAND_RANGE_CACHE_TTL_SEC == 88
        assert built.SUPERTABLE_ISLAND_MEMORY_FRACTION == 0.55
        assert built.SUPERTABLE_ISLAND_GLOBAL_MEMORY_FRACTION == 0.75
        assert built.SUPERTABLE_ISLAND_MAX_MEMORY_BYTES == 999999
        assert built.SUPERTABLE_ISLAND_MAX_RESULT_BYTES == 111111
        assert built.SUPERTABLE_ISLAND_CPU_MAX == 4
        assert built.SUPERTABLE_ISLAND_IO_WORKERS_MAX == 6
        assert built.SUPERTABLE_ISLAND_SPILL_ENABLED is False
        assert built.SUPERTABLE_ISLAND_SPILL_DIR == "/tmp/island-spill"
        assert built.SUPERTABLE_ISLAND_SPILL_MAX_BYTES == 222222
        assert built.SUPERTABLE_ISLAND_SPILL_MIN_FREE_BYTES == 333333


# ---------------------------------------------------------------------------
# Convenience properties
# ---------------------------------------------------------------------------


class TestEffectiveProperties:
    def test_effective_redis_url_prefers_reflection_then_redis(self) -> None:
        s = Settings(
            SUPERTABLE_REFLECTION_REDIS_URL="redis://reflection:6379/0",
            SUPERTABLE_REDIS_URL="redis://main:6379/0",
        )
        assert s.effective_redis_url == "redis://reflection:6379/0"

        s = Settings(SUPERTABLE_REDIS_URL="redis://main:6379/0")
        assert s.effective_redis_url == "redis://main:6379/0"

        s = Settings()
        assert s.effective_redis_url is None

    def test_effective_storage_bucket_for_azure(self) -> None:
        s = Settings(STORAGE_BUCKET="b1", AZURE_CONTAINER="c1")
        assert s.effective_storage_bucket == "b1"

        s = Settings(STORAGE_BUCKET="", AZURE_CONTAINER="c1")
        assert s.effective_storage_bucket == "c1"

        # Both blank -> documented default
        s = Settings(STORAGE_BUCKET="", AZURE_CONTAINER="")
        assert s.effective_storage_bucket == "supertable"

    def test_effective_storage_endpoint_for_azure(self) -> None:
        s = Settings(STORAGE_ENDPOINT_URL="https://s3", AZURE_BLOB_ENDPOINT="https://blob")
        assert s.effective_storage_endpoint == "https://s3"
        s = Settings(AZURE_BLOB_ENDPOINT="https://blob")
        assert s.effective_storage_endpoint == "https://blob"

    def test_effective_storage_access_key_for_azure(self) -> None:
        s = Settings(STORAGE_ACCESS_KEY="k1", AZURE_STORAGE_KEY="kazure")
        assert s.effective_storage_access_key == "k1"
        s = Settings(AZURE_STORAGE_KEY="kazure")
        assert s.effective_storage_access_key == "kazure"

    def test_effective_gcs_bucket(self) -> None:
        s = Settings(GCS_BUCKET="g1", STORAGE_BUCKET="b1")
        assert s.effective_gcs_bucket == "g1"
        s = Settings(STORAGE_BUCKET="b1")
        assert s.effective_gcs_bucket == "b1"
        s = Settings()
        assert s.effective_gcs_bucket == "supertable"


# ---------------------------------------------------------------------------
# _build_settings
# ---------------------------------------------------------------------------


class TestBuildSettings:
    def test_default_construction(self, fresh_env: pytest.MonkeyPatch) -> None:
        s = _build_settings()
        assert s.SUPERTABLE_HOME == "~/supertable"
        assert s.STORAGE_TYPE == "LOCAL"
        # API host falls back to 0.0.0.0 when SUPERTABLE_API_HOST unset
        assert s.SUPERTABLE_API_HOST == "0.0.0.0"

    def test_storage_type_is_uppercased(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("STORAGE_TYPE", "minio")
        s = _build_settings()
        assert s.STORAGE_TYPE == "MINIO"

    def test_int_and_bool_parsing(self, fresh_env: pytest.MonkeyPatch) -> None:
        fresh_env.setenv("MAX_OVERLAPPING_FILES", "37")
        fresh_env.setenv("TOMBSTONE_COMPACTION_WORKERS", "6")
        fresh_env.setenv("SUPERTABLE_DV_V2_WRITES_ENABLED", "yes")
        fresh_env.setenv("SUPERTABLE_DV_V3_WRITES_ENABLED", "true")
        fresh_env.setenv("IS_SHOW_TIMING", "no")
        fresh_env.setenv("STORAGE_FORCE_PATH_STYLE", "yes")
        fresh_env.setenv("STORAGE_USE_SSL", "true")
        fresh_env.setenv("SUPERTABLE_REDIS_PORT", "6380")

        s = _build_settings()
        assert s.MAX_OVERLAPPING_FILES == 37
        assert s.TOMBSTONE_COMPACTION_WORKERS == 6
        assert s.SUPERTABLE_DV_V2_WRITES_ENABLED is True
        assert s.SUPERTABLE_DV_V3_WRITES_ENABLED is True
        assert s.IS_SHOW_TIMING is False
        assert s.STORAGE_FORCE_PATH_STYLE is True
        assert s.STORAGE_USE_SSL is True
        assert s.SUPERTABLE_REDIS_PORT == 6380

    def test_dv_v2_write_gate_rejects_ambiguous_boolean(
        self, fresh_env: pytest.MonkeyPatch,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_DV_V2_WRITES_ENABLED", "enabled")

        with pytest.raises(
            ValueError, match="SUPERTABLE_DV_V2_WRITES_ENABLED must be one of"
        ):
            _build_settings()

    def test_dv_v3_write_gate_rejects_ambiguous_boolean(
        self, fresh_env: pytest.MonkeyPatch,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_DV_V3_WRITES_ENABLED", "enabled")

        with pytest.raises(
            ValueError, match="SUPERTABLE_DV_V3_WRITES_ENABLED must be one of"
        ):
            _build_settings()

    def test_duckdb_write_probe_selection_overrides(
        self, fresh_env: pytest.MonkeyPatch,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_DUCKDB_WRITE_PROBE", "true")
        fresh_env.setenv("SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO", "false")

        s = _build_settings()

        assert s.SUPERTABLE_DUCKDB_WRITE_PROBE is True
        assert s.SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO is False

    def test_duckdb_local_write_probe_escape_hatch(
        self, fresh_env: pytest.MonkeyPatch,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_DUCKDB_WRITE_PROBE", "false")
        fresh_env.setenv("SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO", "false")

        s = _build_settings()

        assert s.SUPERTABLE_DUCKDB_WRITE_PROBE is False
        assert s.SUPERTABLE_DUCKDB_WRITE_PROBE_LOCAL_AUTO is False

    def test_garbage_redis_port_is_rejected(
        self, fresh_env: pytest.MonkeyPatch
    ) -> None:
        fresh_env.setenv("SUPERTABLE_REDIS_PORT", "not-a-number")
        with pytest.raises(ValueError, match="SUPERTABLE_REDIS_PORT"):
            _build_settings()

    @pytest.mark.parametrize("value", ["0", "-1", "65536"])
    def test_out_of_range_redis_port_is_rejected(
        self, fresh_env: pytest.MonkeyPatch, value: str,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_REDIS_PORT", value)
        with pytest.raises(ValueError, match="SUPERTABLE_REDIS_PORT"):
            _build_settings()

    @pytest.mark.parametrize("name", [
        "SUPERTABLE_REDIS_SSL",
        "SUPERTABLE_REDIS_SENTINEL",
    ])
    def test_garbage_redis_security_boolean_is_rejected(
        self, fresh_env: pytest.MonkeyPatch, name: str,
    ) -> None:
        fresh_env.setenv(name, "maybe")
        with pytest.raises(ValueError, match=name):
            _build_settings()

    @pytest.mark.parametrize("value", ["not-a-number", "-1"])
    def test_invalid_redis_database_is_rejected(
        self, fresh_env: pytest.MonkeyPatch, value: str,
    ) -> None:
        fresh_env.setenv("SUPERTABLE_REDIS_DB", value)
        with pytest.raises(ValueError, match="SUPERTABLE_REDIS_DB"):
            _build_settings()

    def test_superuser_token_fallback_chain(
        self, fresh_env: pytest.MonkeyPatch
    ) -> None:
        # Primary wins
        fresh_env.setenv("SUPERTABLE_SUPERUSER_TOKEN", "primary")
        fresh_env.setenv("SUPERTABLE_SUPERTOKEN", "secondary")
        assert _build_settings().SUPERTABLE_SUPERUSER_TOKEN == "primary"

        # Fallback to secondary
        fresh_env.delenv("SUPERTABLE_SUPERUSER_TOKEN", raising=False)
        assert _build_settings().SUPERTABLE_SUPERUSER_TOKEN == "secondary"

        # Both unset
        fresh_env.delenv("SUPERTABLE_SUPERTOKEN", raising=False)
        assert _build_settings().SUPERTABLE_SUPERUSER_TOKEN == ""

    def test_mcp_token_fallback_chain(
        self, fresh_env: pytest.MonkeyPatch
    ) -> None:
        fresh_env.setenv("SUPERTABLE_MCP_TOKEN", "primary-mcp")
        fresh_env.setenv("SUPERTABLE_MCP_AUTH_TOKEN", "fallback-mcp")
        assert _build_settings().SUPERTABLE_MCP_TOKEN == "primary-mcp"

        fresh_env.delenv("SUPERTABLE_MCP_TOKEN", raising=False)
        assert _build_settings().SUPERTABLE_MCP_TOKEN == "fallback-mcp"

    def test_api_host_explicit_override(
        self, fresh_env: pytest.MonkeyPatch
    ) -> None:
        fresh_env.setenv("SUPERTABLE_API_HOST", "127.0.0.1")
        assert _build_settings().SUPERTABLE_API_HOST == "127.0.0.1"

    def test_xdg_fallback_when_unset(self, fresh_env: pytest.MonkeyPatch) -> None:
        # Only assert the structural shape: XDG_CONFIG_HOME defaults to a
        # ".config" directory under HOME. Don't pin the exact path because
        # tests run on developers' machines.
        s = _build_settings()
        assert s.XDG_CONFIG_HOME != ""

    def test_returns_a_settings_instance(self, fresh_env: pytest.MonkeyPatch) -> None:
        assert isinstance(_build_settings(), Settings)
