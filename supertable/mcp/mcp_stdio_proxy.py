#!/usr/bin/env python3
"""
mcp_stdio_proxy.py — stdio-to-HTTP bridge for remote MCP servers.

Claude Desktop speaks stdio to this script; this script forwards
everything to your remote MCP server over Streamable HTTP and
relays responses back via stdout.

Usage in claude_desktop_config.json:

    {
      "mcpServers": {
        "supertable": {
          "command": "python3",
          "args": ["/path/to/mcp_stdio_proxy.py", "--url", "https://192.168.168.130:8470/mcp"]
        }
      }
    }

Or with --insecure for self-signed certs:

    {
      "mcpServers": {
        "supertable": {
          "command": "python3",
          "args": ["/path/to/mcp_stdio_proxy.py", "--url", "https://192.168.168.130:8470/mcp", "--insecure"]
        }
      }
    }

Requirements: Python 3.8+, no external dependencies (stdlib only).
"""
from __future__ import annotations

import argparse
import json
import ssl
import sys
import urllib.request
import urllib.error
from typing import Any, Dict, Optional


def make_ssl_context(insecure: bool) -> Optional[ssl.SSLContext]:
    """Build an SSL context; optionally skip certificate verification."""
    if not insecure:
        return None  # use default verification
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def post_json(
    url: str,
    body: Dict[str, Any],
    session_id: Optional[str],
    ssl_ctx: Optional[ssl.SSLContext],
) -> tuple[Optional[str], Optional[Dict[str, Any]]]:
    """POST a JSON-RPC message and return (session_id, parsed_response)."""
    data = json.dumps(body).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        resp = urllib.request.urlopen(req, context=ssl_ctx)
    except urllib.error.HTTPError as e:
        # Read the error body and wrap it in a JSON-RPC error
        err_body = e.read().decode("utf-8", errors="replace")
        error_resp = {
            "jsonrpc": "2.0",
            "id": body.get("id"),
            "error": {"code": -32000, "message": f"HTTP {e.code}: {err_body}"},
        }
        return session_id, error_resp

    new_sid = resp.headers.get("Mcp-Session-Id") or session_id
    content_type = resp.headers.get("Content-Type", "")
    raw = resp.read().decode("utf-8")

    if not raw.strip():
        # Notification response (202 empty body) — no reply to stdout
        return new_sid, None

    if "text/event-stream" in content_type:
        # Parse SSE: extract data: lines
        for chunk in raw.split("\n\n"):
            for line in chunk.split("\n"):
                if line.startswith("data: "):
                    try:
                        return new_sid, json.loads(line[6:])
                    except json.JSONDecodeError:
                        pass
        # Fallback: try parsing the whole body
        try:
            return new_sid, json.loads(raw)
        except json.JSONDecodeError:
            return new_sid, {
                "jsonrpc": "2.0",
                "id": body.get("id"),
                "error": {"code": -32000, "message": f"Unparseable SSE: {raw[:500]}"},
            }

    try:
        return new_sid, json.loads(raw)
    except json.JSONDecodeError:
        return new_sid, {
            "jsonrpc": "2.0",
            "id": body.get("id"),
            "error": {"code": -32000, "message": f"Invalid JSON response: {raw[:500]}"},
        }


def main() -> None:
    parser = argparse.ArgumentParser(description="MCP stdio-to-HTTP proxy")
    parser.add_argument(
        "--url",
        required=True,
        help="Remote MCP server URL (e.g. https://192.168.168.130:8470/mcp)",
    )
    parser.add_argument(
        "--insecure",
        action="store_true",
        help="Skip TLS certificate verification (for self-signed certs)",
    )
    args = parser.parse_args()

    url = args.url.rstrip("/")
    ssl_ctx = make_ssl_context(args.insecure)
    session_id: Optional[str] = None

    # Read NDJSON from stdin, forward to HTTP, write responses to stdout
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            continue

        session_id, response = post_json(url, msg, session_id, ssl_ctx)

        if response is not None:
            sys.stdout.write(json.dumps(response, separators=(",", ":")) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
