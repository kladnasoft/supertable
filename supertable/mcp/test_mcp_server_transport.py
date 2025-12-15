import importlib

import pytest


def _import_target():
    # Prefer the package import used by the project; fall back to local module name.
    try:
        return importlib.import_module("supertable.mcp.mcp_server")
    except Exception:
        return importlib.import_module("mcp_server")


def test_normalize_transport_value():
    mod = _import_target()
    assert mod._normalize_transport_value("stdio") == "stdio"
    assert mod._normalize_transport_value("sse") == "sse"

    # Streamable HTTP aliases
    assert mod._normalize_transport_value("http") == "streamable-http"
    assert mod._normalize_transport_value("streamable-http") == "streamable-http"
    assert mod._normalize_transport_value("streamable_http") == "streamable-http"
    assert mod._normalize_transport_value("streamablehttp") == "streamable-http"

    # Defaulting
    assert mod._normalize_transport_value(None, default="stdio") == "stdio"


def test_require_token_env_toggle(monkeypatch):
    # Enabled
    monkeypatch.setenv("SUPERTABLE_REQUIRE_TOKEN", "1")
    mod = _import_target()
    mod = importlib.reload(mod)
    assert mod.CFG.require_token is True

    # Disabled
    monkeypatch.setenv("SUPERTABLE_REQUIRE_TOKEN", "0")
    mod = importlib.reload(mod)
    assert mod.CFG.require_token is False


def test_transport_env_normalization(monkeypatch):
    monkeypatch.setenv("SUPERTABLE_MCP_TRANSPORT", "http")
    mod = _import_target()
    mod = importlib.reload(mod)
    assert mod.CFG.transport == "streamable-http"
