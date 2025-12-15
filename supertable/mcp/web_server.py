#!/usr/bin/env python3
# web_server.py — uvicorn runner for the MCP web test app
from __future__ import annotations

import os

import uvicorn

def main() -> None:
    host = os.getenv("SUPERTABLE_MCP_WEB_HOST", "0.0.0.0")
    port = int(os.getenv("SUPERTABLE_MCP_WEB_PORT", "8099"))
    uvicorn.run("supertable.mcp.web_app:app", host=host, port=port, reload=bool(os.getenv("RELOAD")))

if __name__ == "__main__":
    main()
