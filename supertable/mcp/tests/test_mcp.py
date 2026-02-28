# tests/test_mcp_server.py

"""
Comprehensive test suite for the Supertable MCP server stack.

Covers:
  1. Pure helper/validator functions (no async, no mocking)
  2. Auth helpers (_check_token, _resolve_role, _allowed_role)
  3. Config & transport normalization
  4. log_tool decorator behaviour
  5. All tool endpoints (health, info, whoami, list_supers, list_tables,
     describe_table, get_table_stats, get_super_meta, query_sql, sample_data)
  6. _exec_query_sync envelope
  7. web_client._AUTH_TOOLS coverage
  8. web_app header injection (/mcp, /mcp_v1)

All Supertable backend calls (MetaReader, data_query_sql, list_supers_fn,
list_tables_fn) are mocked — no Redis/storage/network required.
"""

from __future__ import annotations

import importlib
import hmac
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Import the module under test
# ---------------------------------------------------------------------------

def _import_mcp():
    try:
        return importlib.import_module("supertable.mcp.mcp_server")
    except Exception:
        return importlib.import_module("mcp_server")


@pytest.fixture(scope="module")
def mod():
    return _import_mcp()


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

VALID_ROLE = "my_admin_role"
VALID_ROLE_UUID = "a1b2c3d4e5f6" * 2  # 24-char hex-like role id
VALID_TOKEN = "test-secret-token-123"


@pytest.fixture
def cfg_no_token(mod, monkeypatch):
    """Config with token requirement disabled."""
    fake = MagicMock()
    fake.require_token = False
    fake.require_explicit_role = True
    fake.allowed_roles = None
    fake.default_engine = "AUTO"
    fake.default_limit = 200
    fake.max_limit = 5000
    fake.default_query_timeout_sec = 60
    fake.max_concurrency = 6
    fake.shared_token = ""
    monkeypatch.setattr(mod, "CFG", fake)
    return fake


@pytest.fixture
def cfg_with_token(mod, monkeypatch):
    """Config with token requirement enabled and a known shared token."""
    fake = MagicMock()
    fake.require_token = True
    fake.shared_token = VALID_TOKEN
    fake.require_explicit_role = True
    fake.allowed_roles = None
    fake.default_engine = "AUTO"
    fake.default_limit = 200
    fake.max_limit = 5000
    fake.default_query_timeout_sec = 60
    fake.max_concurrency = 6
    monkeypatch.setattr(mod, "CFG", fake)
    return fake


@pytest.fixture
def cfg_no_explicit_role(mod, monkeypatch):
    """Config where explicit role is not required (fallback to env)."""
    fake = MagicMock()
    fake.require_token = False
    fake.require_explicit_role = False
    fake.allowed_roles = None
    fake.default_engine = "AUTO"
    fake.default_limit = 200
    fake.max_limit = 5000
    fake.default_query_timeout_sec = 60
    fake.max_concurrency = 6
    fake.shared_token = ""
    monkeypatch.setattr(mod, "CFG", fake)
    return fake


# ---------------------------------------------------------------------------
# 1. Pure helpers / validators
# ---------------------------------------------------------------------------

class TestSafeId:
    def test_valid_simple(self, mod):
        assert mod._safe_id("my_org", "org") == "my_org"

    def test_valid_with_dots_dashes(self, mod):
        assert mod._safe_id("org-1.test", "org") == "org-1.test"

    def test_rejects_slash(self, mod):
        with pytest.raises(ValueError, match="Invalid"):
            mod._safe_id("org/evil", "org")

    def test_rejects_backslash(self, mod):
        with pytest.raises(ValueError, match="Invalid"):
            mod._safe_id("org\\evil", "org")

    def test_rejects_dotdot(self, mod):
        with pytest.raises(ValueError, match="Invalid"):
            mod._safe_id("org..evil", "org")

    def test_rejects_empty(self, mod):
        with pytest.raises(ValueError, match="Invalid"):
            mod._safe_id("", "org")

    def test_rejects_too_long(self, mod):
        with pytest.raises(ValueError, match="Invalid"):
            mod._safe_id("a" * 129, "org")

    def test_rejects_non_string(self, mod):
        with pytest.raises(ValueError, match="Invalid"):
            mod._safe_id(123, "org")

    def test_rejects_spaces(self, mod):
        with pytest.raises(ValueError, match="Invalid"):
            mod._safe_id("my org", "org")


class TestValidateRole:
    def test_valid_alphanumeric(self, mod):
        assert mod._validate_role("admin_role") == "admin_role"

    def test_valid_with_dots_dashes(self, mod):
        assert mod._validate_role("role-v2.1") == "role-v2.1"

    def test_valid_uuid_like(self, mod):
        assert mod._validate_role(VALID_ROLE_UUID) == VALID_ROLE_UUID

    def test_strips_whitespace(self, mod):
        assert mod._validate_role("  admin  ") == "admin"

    def test_rejects_empty(self, mod):
        with pytest.raises(ValueError):
            mod._validate_role("")

    def test_rejects_spaces_inside(self, mod):
        with pytest.raises(ValueError):
            mod._validate_role("bad role")

    def test_rejects_non_string(self, mod):
        with pytest.raises(ValueError):
            mod._validate_role(12345)


class TestReadOnlySql:
    def test_allows_simple_select(self, mod):
        mod._read_only_sql("SELECT * FROM t LIMIT 10")

    def test_allows_with_cte(self, mod):
        mod._read_only_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_blocks_insert(self, mod):
        with pytest.raises(ValueError, match="write/DDL"):
            mod._read_only_sql("SELECT 1; INSERT INTO t VALUES (1)")

    def test_blocks_drop(self, mod):
        with pytest.raises(ValueError, match="write/DDL"):
            mod._read_only_sql("SELECT 1; DROP TABLE t")

    def test_blocks_pure_update(self, mod):
        with pytest.raises(ValueError, match="Only SELECT"):
            mod._read_only_sql("UPDATE t SET x = 1")

    def test_allows_table_named_update_log(self, mod):
        mod._read_only_sql("SELECT * FROM update_log")

    def test_allows_column_named_created_at(self, mod):
        mod._read_only_sql("SELECT created_at FROM t")

    def test_allows_table_named_deleted_records(self, mod):
        mod._read_only_sql("SELECT * FROM user_deleted_records")

    def test_allows_column_named_grant_amount(self, mod):
        mod._read_only_sql("SELECT grant_amount FROM t")

    def test_blocks_real_delete_keyword(self, mod):
        with pytest.raises(ValueError, match="write/DDL"):
            mod._read_only_sql("SELECT 1; DELETE FROM t WHERE 1=1")

    def test_blocks_merge(self, mod):
        with pytest.raises(ValueError, match="write/DDL"):
            mod._read_only_sql("SELECT 1; MERGE INTO t USING s ON t.id = s.id")

    def test_rejects_empty(self, mod):
        with pytest.raises(ValueError, match="Only SELECT"):
            mod._read_only_sql("")


class TestClampLimit:
    def test_default_when_none(self, mod):
        assert mod._clamp_limit(None, 200, 5000) == 200

    def test_clamp_to_max(self, mod):
        assert mod._clamp_limit(99999, 200, 5000) == 5000

    def test_clamp_to_min(self, mod):
        assert mod._clamp_limit(-10, 200, 5000) == 1

    def test_explicit_value(self, mod):
        assert mod._clamp_limit(50, 200, 5000) == 50

    def test_invalid_string_falls_to_default(self, mod):
        assert mod._clamp_limit("abc", 200, 5000) == 200


class TestNormalizeTransport:
    def test_stdio(self, mod):
        assert mod._normalize_transport_value("stdio") == "stdio"

    def test_http_alias(self, mod):
        assert mod._normalize_transport_value("http") == "streamable-http"

    def test_streamable_http(self, mod):
        assert mod._normalize_transport_value("streamable-http") == "streamable-http"

    def test_streamable_underscore(self, mod):
        assert mod._normalize_transport_value("streamable_http") == "streamable-http"

    def test_streamablehttp(self, mod):
        assert mod._normalize_transport_value("streamablehttp") == "streamable-http"

    def test_sse(self, mod):
        assert mod._normalize_transport_value("sse") == "sse"

    def test_none_defaults(self, mod):
        assert mod._normalize_transport_value(None, default="stdio") == "stdio"

    def test_empty_defaults(self, mod):
        assert mod._normalize_transport_value("", default="stdio") == "stdio"

    def test_unknown_passthrough(self, mod):
        assert mod._normalize_transport_value("grpc") == "grpc"


class TestEnvBool:
    def test_true_values(self, mod):
        for v in ("1", "true", "yes", "y", "on", "TRUE", "  Yes "):
            with patch.dict(os.environ, {"TEST_FLAG": v}):
                assert mod._env_bool("TEST_FLAG", False) is True

    def test_false_values(self, mod):
        for v in ("0", "false", "no", "off", ""):
            with patch.dict(os.environ, {"TEST_FLAG": v}):
                assert mod._env_bool("TEST_FLAG", True) is False

    def test_missing_uses_default(self, mod):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TEST_FLAG_MISSING", None)
            assert mod._env_bool("TEST_FLAG_MISSING", True) is True
            assert mod._env_bool("TEST_FLAG_MISSING", False) is False


class TestSummarizeRows:
    def test_basic(self, mod):
        rows = [[1, "a"], [2, "b"], [3, "c"], [4, "d"]]
        s = mod._summarize_rows(rows)
        assert "rows=4" in s
        assert "head=" in s

    def test_empty(self, mod):
        assert "rows=0" in mod._summarize_rows([])


# ---------------------------------------------------------------------------
# 2. Auth helpers
# ---------------------------------------------------------------------------

class TestCheckToken:
    def test_skipped_when_not_required(self, mod, cfg_no_token):
        mod._check_token(None)
        mod._check_token("")

    def test_required_but_missing(self, mod, cfg_with_token):
        with pytest.raises(PermissionError, match="auth_token is required"):
            mod._check_token(None)

    def test_required_but_empty(self, mod, cfg_with_token):
        with pytest.raises(PermissionError, match="auth_token is required"):
            mod._check_token("")

    def test_required_and_invalid(self, mod, cfg_with_token):
        with pytest.raises(PermissionError, match="auth_token invalid"):
            mod._check_token("wrong-token")

    def test_required_and_valid(self, mod, cfg_with_token):
        mod._check_token(VALID_TOKEN)


class TestResolveRole:
    def test_explicit_required_and_valid(self, mod, cfg_no_token):
        result = mod._resolve_role(VALID_ROLE)
        assert result == VALID_ROLE

    def test_explicit_required_but_missing(self, mod, cfg_no_token):
        with pytest.raises(PermissionError, match="role is required"):
            mod._resolve_role(None)

    def test_explicit_required_but_invalid_format(self, mod, cfg_no_token):
        with pytest.raises(ValueError):
            mod._resolve_role("has spaces")

    def test_allowed_roles_permits(self, mod, cfg_no_token):
        cfg_no_token.allowed_roles = {"admin", "reader"}
        assert mod._resolve_role("admin") == "admin"

    def test_allowed_roles_blocks(self, mod, cfg_no_token):
        cfg_no_token.allowed_roles = {"admin", "reader"}
        with pytest.raises(PermissionError, match="not permitted"):
            mod._resolve_role("hacker")

    def test_fallback_from_env(self, mod, cfg_no_explicit_role, monkeypatch):
        monkeypatch.setenv("SUPERTABLE_ROLE", "env_role")
        result = mod._resolve_role(None)
        assert result == "env_role"


class TestAllowedRole:
    def test_none_means_all_allowed(self, mod, cfg_no_token):
        assert mod._allowed_role("anything") is True

    def test_restricted_set(self, mod, cfg_no_token):
        cfg_no_token.allowed_roles = {"aaa", "bbb"}
        assert mod._allowed_role("aaa") is True
        assert mod._allowed_role("ccc") is False


# ---------------------------------------------------------------------------
# 3. _resolve_engine
# ---------------------------------------------------------------------------

class TestResolveEngine:
    def test_default_auto(self, mod, cfg_no_token):
        with patch.object(mod, "_ensure_imports", side_effect=Exception("no imports")):
            result = mod._resolve_engine(None)
            assert result == "AUTO"

    def test_explicit_engine_name(self, mod, cfg_no_token):
        with patch.object(mod, "_ensure_imports", side_effect=Exception("no imports")):
            result = mod._resolve_engine("duckdb")
            assert result == "DUCKDB"


# ---------------------------------------------------------------------------
# 4. log_tool decorator
# ---------------------------------------------------------------------------

class TestLogTool:
    @pytest.mark.asyncio
    async def test_query_sql_error_returns_rich_envelope(self, mod):
        @mod.log_tool
        async def query_sql(**kwargs):
            raise RuntimeError("boom")

        result = await query_sql()
        assert result["status"] == "ERROR"
        assert "boom" in result["message"]
        assert result["columns"] == []
        assert result["rows"] == []

    @pytest.mark.asyncio
    async def test_non_query_tool_error_returns_simple_envelope(self, mod):
        @mod.log_tool
        async def describe_table(**kwargs):
            raise ValueError("bad input")

        result = await describe_table()
        assert result["status"] == "ERROR"
        assert "bad input" in result["message"]
        assert result["result"] is None

    @pytest.mark.asyncio
    async def test_success_passthrough(self, mod):
        @mod.log_tool
        async def health():
            return {"result": "ok"}

        result = await health()
        assert result == {"result": "ok"}


# ---------------------------------------------------------------------------
# 5. _exec_query_sync envelope
# ---------------------------------------------------------------------------

class TestExecQuerySync:
    def test_returns_correct_envelope(self, mod):
        fake_query_sql = MagicMock(return_value=(
            ["id", "name"],
            [[1, "alice"], [2, "bob"], [3, "carol"]],
            [{"name": "id", "type": "int64", "nullable": True},
             {"name": "name", "type": "object", "nullable": True}],
        ))
        with patch.object(mod, "data_query_sql", fake_query_sql), \
             patch.object(mod, "_ensure_imports"):
            result = mod._exec_query_sync("sup", "org", "SELECT 1", 2, "AUTO", "admin")

        assert result["status"] == "OK"
        assert result["columns"] == ["id", "name"]
        assert len(result["rows"]) == 2  # limit_n=2 enforced
        assert result["rowcount"] == 2
        assert result["limit_applied"] == 2
        assert isinstance(result["elapsed_ms"], float)

    def test_limit_slices_rows(self, mod):
        fake_query_sql = MagicMock(return_value=(
            ["x"], [[1], [2], [3], [4], [5]], [],
        ))
        with patch.object(mod, "data_query_sql", fake_query_sql), \
             patch.object(mod, "_ensure_imports"):
            result = mod._exec_query_sync("s", "o", "SELECT x", 3, "AUTO", "reader")

        assert result["rowcount"] == 3
        assert result["rows"] == [[1], [2], [3]]


# ---------------------------------------------------------------------------
# 6. Tool endpoints (async)
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_backend_only(mod, monkeypatch):
    """Mock supertable backend imports only (no config override)."""
    mock_meta = MagicMock()
    mock_meta.return_value.get_tables.return_value = ["table_a", "table_b"]
    mock_meta.return_value.get_table_schema.return_value = {"col1": "int", "col2": "str"}
    mock_meta.return_value.get_table_stats.return_value = {"rows": 100, "files": 5}
    mock_meta.return_value.get_super_meta.return_value = {"name": "test_super"}

    monkeypatch.setattr(mod, "MetaReader", mock_meta)
    monkeypatch.setattr(mod, "engine_enum", MagicMock())
    monkeypatch.setattr(mod, "data_query_sql", MagicMock(return_value=(["a"], [[1]], [])))
    monkeypatch.setattr(mod, "list_supers_fn", MagicMock(return_value=["super_a", "super_b"]))
    monkeypatch.setattr(mod, "list_tables_fn", MagicMock(return_value=["t1", "t2"]))

    # Reset limiter so it gets created fresh inside the async test context
    monkeypatch.setattr(mod, "_LIMITER", None)

    return mock_meta


@pytest.fixture
def mock_backend(mod, cfg_no_token, mock_backend_only):
    """Backend mocks + token-disabled config (most common combo)."""
    return mock_backend_only


class TestToolHealth:
    @pytest.mark.asyncio
    async def test_returns_ok(self, mod):
        result = await mod.health()
        assert result["result"] == "ok"


class TestToolInfo:
    @pytest.mark.asyncio
    async def test_returns_config(self, mod):
        result = await mod.info()
        r = result["result"]
        assert "name" in r
        assert "version" in r
        assert "max_concurrency" in r
        assert "require_token" in r
        assert "require_explicit_role" in r


class TestToolWhoami:
    @pytest.mark.asyncio
    async def test_returns_role(self, mod, cfg_no_token):
        result = await mod.whoami(role=VALID_ROLE)
        assert result["result"]["role"] == VALID_ROLE

    @pytest.mark.asyncio
    async def test_missing_role_returns_error(self, mod, cfg_no_token):
        result = await mod.whoami(role=None)
        assert result["status"] == "ERROR"


class TestToolListSupers:
    @pytest.mark.asyncio
    async def test_returns_list(self, mod, mock_backend):
        result = await mod.list_supers(
            organization="test_org", role=VALID_ROLE,
        )
        assert result["result"] == ["super_a", "super_b"]

    @pytest.mark.asyncio
    async def test_invalid_org_returns_error(self, mod, mock_backend):
        result = await mod.list_supers(
            organization="evil/path", role=VALID_ROLE,
        )
        assert result["status"] == "ERROR"

    @pytest.mark.asyncio
    async def test_requires_auth(self, mod, cfg_with_token, mock_backend_only):
        result = await mod.list_supers(
            organization="org", role=VALID_ROLE, auth_token=None,
        )
        assert result["status"] == "ERROR"
        assert "auth_token" in result["message"]


class TestToolListTables:
    @pytest.mark.asyncio
    async def test_returns_list(self, mod, mock_backend):
        result = await mod.list_tables(
            super_name="sup", organization="org", role=VALID_ROLE,
        )
        assert result["result"] == ["table_a", "table_b"]

    @pytest.mark.asyncio
    async def test_invalid_super_returns_error(self, mod, mock_backend):
        result = await mod.list_tables(
            super_name="evil/path", organization="org", role=VALID_ROLE,
        )
        assert result["status"] == "ERROR"


class TestToolDescribeTable:
    @pytest.mark.asyncio
    async def test_returns_schema(self, mod, mock_backend):
        result = await mod.describe_table(
            super_name="sup", organization="org", table="tbl",
            role=VALID_ROLE,
        )
        assert result["result"] == {"col1": "int", "col2": "str"}

    @pytest.mark.asyncio
    async def test_invalid_table_returns_error(self, mod, mock_backend):
        result = await mod.describe_table(
            super_name="sup", organization="org", table="../etc/passwd",
            role=VALID_ROLE,
        )
        assert result["status"] == "ERROR"


class TestToolGetTableStats:
    @pytest.mark.asyncio
    async def test_returns_stats(self, mod, mock_backend):
        result = await mod.get_table_stats(
            super_name="sup", organization="org", table="tbl",
            role=VALID_ROLE,
        )
        assert result["result"] == {"rows": 100, "files": 5}


class TestToolGetSuperMeta:
    @pytest.mark.asyncio
    async def test_returns_meta(self, mod, mock_backend):
        result = await mod.get_super_meta(
            super_name="sup", organization="org", role=VALID_ROLE,
        )
        assert result["result"] == {"name": "test_super"}


class TestToolQuerySql:
    @pytest.mark.asyncio
    async def test_success(self, mod, mock_backend):
        result = await mod.query_sql(
            super_name="sup", organization="org",
            sql="SELECT * FROM t LIMIT 10",
            role=VALID_ROLE,
        )
        assert result["status"] == "OK"
        assert "columns" in result

    @pytest.mark.asyncio
    async def test_blocks_write_sql(self, mod, mock_backend):
        result = await mod.query_sql(
            super_name="sup", organization="org",
            sql="INSERT INTO t VALUES (1)",
            role=VALID_ROLE,
        )
        assert result["status"] == "ERROR"
        assert "Only SELECT" in result["message"]

    @pytest.mark.asyncio
    async def test_invalid_org_returns_error(self, mod, mock_backend):
        result = await mod.query_sql(
            super_name="sup", organization="../evil",
            sql="SELECT 1", role=VALID_ROLE,
        )
        assert result["status"] == "ERROR"


class TestToolSampleData:
    @pytest.mark.asyncio
    async def test_success(self, mod, mock_backend):
        result = await mod.sample_data(
            super_name="sup", organization="org", table="tbl",
            role=VALID_ROLE,
        )
        assert result["status"] == "OK"
        assert "columns" in result

    @pytest.mark.asyncio
    async def test_invalid_table_returns_error(self, mod, mock_backend):
        result = await mod.sample_data(
            super_name="sup", organization="org", table="evil/path",
            role=VALID_ROLE,
        )
        assert result["status"] == "ERROR"

    @pytest.mark.asyncio
    async def test_default_limit_is_10(self, mod, mock_backend):
        result = await mod.sample_data(
            super_name="sup", organization="org", table="tbl",
            role=VALID_ROLE,
        )
        # sample_data defaults to limit=10
        assert result["limit_applied"] <= 10


# ---------------------------------------------------------------------------
# 7. web_client._AUTH_TOOLS
# ---------------------------------------------------------------------------

class TestWebClientAuthTools:
    def test_all_data_tools_present(self):
        try:
            from supertable.mcp.web_client import _AUTH_TOOLS
        except ImportError:
            from web_client import _AUTH_TOOLS

        expected = {"whoami", "list_supers", "list_tables", "describe_table",
                    "get_table_stats", "get_super_meta", "query_sql", "sample_data"}
        assert expected == _AUTH_TOOLS


# ---------------------------------------------------------------------------
# 8. web_app header injection
# ---------------------------------------------------------------------------

class TestWebAppParseBearerHelper:
    def test_bearer_prefix(self):
        try:
            from supertable.mcp.web_app import _parse_bearer
        except ImportError:
            from web_app import _parse_bearer

        assert _parse_bearer("Bearer abc123") == "abc123"

    def test_no_prefix(self):
        try:
            from supertable.mcp.web_app import _parse_bearer
        except ImportError:
            from web_app import _parse_bearer

        assert _parse_bearer("raw_token") == "raw_token"

    def test_empty(self):
        try:
            from supertable.mcp.web_app import _parse_bearer
        except ImportError:
            from web_app import _parse_bearer

        assert _parse_bearer("") == ""
        assert _parse_bearer(None) == ""


# ---------------------------------------------------------------------------
# 9. web_client subprocess env
# ---------------------------------------------------------------------------

class TestWebClientSubprocessEnv:
    def test_forces_stdio_transport(self, monkeypatch):
        monkeypatch.setenv("SUPERTABLE_MCP_TRANSPORT", "streamable-http")

        try:
            from supertable.mcp.web_client import MCPWebClient
        except ImportError:
            from web_client import MCPWebClient

        import tempfile
        c = MCPWebClient(server_path=tempfile.mktemp(suffix=".py"))
        env = c._subprocess_env()
        assert env["SUPERTABLE_MCP_TRANSPORT"] == "stdio"


# ---------------------------------------------------------------------------
# 10. Config env toggle (transport)
# ---------------------------------------------------------------------------

class TestConfigTransportEnv:
    def test_http_normalizes_to_streamable_http(self, monkeypatch):
        monkeypatch.delenv("_SUPERTABLE_DOTENV_LOADED", raising=False)
        monkeypatch.setenv("SUPERTABLE_MCP_TRANSPORT", "http")
        mod = _import_mcp()
        mod = importlib.reload(mod)
        assert mod.CFG.transport == "streamable-http"

    def test_stdio_default(self, monkeypatch):
        monkeypatch.delenv("SUPERTABLE_MCP_TRANSPORT", raising=False)
        mod = _import_mcp()
        mod = importlib.reload(mod)
        assert mod.CFG.transport == "stdio"


class TestConfigRequireToken:
    def test_enabled(self, monkeypatch):
        monkeypatch.delenv("_SUPERTABLE_DOTENV_LOADED", raising=False)
        monkeypatch.setenv("SUPERTABLE_REQUIRE_TOKEN", "1")
        mod = _import_mcp()
        mod = importlib.reload(mod)
        assert mod.CFG.require_token is True

    def test_disabled(self, monkeypatch):
        monkeypatch.delenv("_SUPERTABLE_DOTENV_LOADED", raising=False)
        monkeypatch.setenv("SUPERTABLE_REQUIRE_TOKEN", "0")
        mod = _import_mcp()
        mod = importlib.reload(mod)
        assert mod.CFG.require_token is False
