#!/usr/bin/env python3
# web_server.py — uvicorn runner for the MCP web test app
from __future__ import annotations

import os

from supertable.config.settings import settings

import uvicorn


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def main() -> None:
    host = settings.SUPERTABLE_MCP_WEB_HOST
    port = settings.SUPERTABLE_MCP_WEB_PORT
    uvicorn.run(
        "supertable.mcp.web_app:app",
        host=host,
        port=port,
        reload=_env_bool("RELOAD"),
        proxy_headers=_env_bool("UVICORN_PROXY_HEADERS", True),
        forwarded_allow_ips=settings.FORWARDED_ALLOW_IPS,
    )


if __name__ == "__main__":
    main()
