#!/usr/bin/env python3
# web_server.py — uvicorn runner for the MCP web test app
from __future__ import annotations

import os

import uvicorn


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    host = os.getenv("SUPERTABLE_MCP_WEB_HOST", "0.0.0.0")
    port = int(os.getenv("SUPERTABLE_MCP_WEB_PORT", "8099"))
    uvicorn.run(
        "supertable.mcp.web_app:app",
        host=host,
        port=port,
        reload=_env_bool("RELOAD"),
        proxy_headers=_env_bool("UVICORN_PROXY_HEADERS", True),
        forwarded_allow_ips=os.getenv("FORWARDED_ALLOW_IPS", "*"),
    )


if __name__ == "__main__":
    main()
