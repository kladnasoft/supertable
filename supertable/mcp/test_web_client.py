import os

from supertable.mcp.web_client import MCPWebClient


def test_subprocess_env_forces_stdio(monkeypatch, tmp_path):
    # If the parent process is configured for remote Streamable HTTP, the web UI
    # stdio client must still spawn the MCP server in stdio mode.
    monkeypatch.setenv("SUPERTABLE_MCP_TRANSPORT", "streamable-http")

    c = MCPWebClient(server_path=str(tmp_path / "mcp_server.py"))
    env = c._subprocess_env()

    assert env.get("SUPERTABLE_MCP_TRANSPORT") == "stdio"
