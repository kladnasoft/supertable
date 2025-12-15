#!/usr/bin/env python3
# web_client.py — async MCP stdio client for FastAPI/web tooling
from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional
from dotenv import load_dotenv
load_dotenv()

_AUTH_TOOLS = {
    "whoami",
    "list_tables",
    "describe_table",
    "get_table_stats",
    "get_super_meta",
    "query_sql",
}

def _default_server_path() -> str:
    here = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(here, "mcp_server.py")

@dataclass
class McpEvent:
    direction: str  # "->" or "<-"
    payload: Dict[str, Any]

class MCPWebClient:
    """
    A small, robust, persistent stdio client intended to be shared by a web app.

    - Uses asyncio subprocess pipes (non-blocking).
    - Serializes writes and matches responses by id.
    - Keeps an in-memory event log for debugging UI.
    """
    def __init__(
        self,
        server_path: Optional[str] = None,
        python_exe: Optional[str] = None,
        auth_token: Optional[str] = None,
    ) -> None:
        self.server_path = os.path.abspath(server_path or os.getenv("MCP_SERVER_PATH", _default_server_path()))
        self.python_exe = python_exe or sys.executable
        self.auth_token = (auth_token or os.getenv("SUPERTABLE_MCP_AUTH_TOKEN", os.getenv("SUPERTABLE_MCP_TOKEN", ""))).strip()

        self._proc: Optional[asyncio.subprocess.Process] = None
        self._reader_task: Optional[asyncio.Task[None]] = None
        self._write_lock = asyncio.Lock()
        self._id = 0
        self._pending: Dict[int, asyncio.Future[Dict[str, Any]]] = {}
        self.events: list[McpEvent] = []


    def _subprocess_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        # Ensure the spawned server uses stdio transport even if the parent process
        # is configured for streamable-http (remote server mode).
        env["SUPERTABLE_MCP_TRANSPORT"] = "stdio"
        return env


    async def start(self) -> None:
        if self._proc:
            return
        if not os.path.exists(self.server_path):
            raise FileNotFoundError(f"MCP server script not found: {self.server_path}")

        self._proc = await asyncio.create_subprocess_exec(
            self.python_exe,
            "-u",
            self.server_path,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=sys.stderr,
            env=self._subprocess_env(),
        )
        if not self._proc.stdin or not self._proc.stdout:
            raise RuntimeError("Failed to open stdio pipes to MCP server process")

        self._reader_task = asyncio.create_task(self._reader_loop())

        # initialize handshake
        init_resp = await self._request(
            "initialize",
            {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "web-mcp-client", "version": "1.0"},
            },
            is_tool=False,
        )
        self._log("<-", init_resp)

        await self._notify("notifications/initialized", {})

    async def close(self) -> None:
        proc = self._proc
        self._proc = None

        if self._reader_task:
            self._reader_task.cancel()
            self._reader_task = None

        # fail pending futures
        for fut in list(self._pending.values()):
            if not fut.done():
                fut.set_exception(RuntimeError("MCP client closed"))
        self._pending.clear()

        if proc:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def _log(self, direction: str, payload: Dict[str, Any]) -> None:
        # keep small-ish; UI can poll this
        self.events.append(McpEvent(direction=direction, payload=payload))
        if len(self.events) > 500:
            self.events = self.events[-500:]

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    async def _write_json(self, msg: Dict[str, Any]) -> None:
        assert self._proc and self._proc.stdin
        data = json.dumps(msg, separators=(",", ":")).encode("utf-8") + b"\n"
        async with self._write_lock:
            self._proc.stdin.write(data)
            await self._proc.stdin.drain()

    async def _reader_loop(self) -> None:
        assert self._proc and self._proc.stdout
        try:
            while True:
                line = await self._proc.stdout.readline()
                if not line:
                    raise RuntimeError("MCP server closed stdout")
                line = line.strip()
                if not line:
                    continue
                try:
                    msg = json.loads(line.decode("utf-8"))
                except json.JSONDecodeError:
                    # ignore garbage (shouldn't happen, but don't kill the webapp)
                    continue

                self._log("<-", msg)

                msg_id = msg.get("id")
                if isinstance(msg_id, int) and msg_id in self._pending:
                    fut = self._pending.pop(msg_id)
                    if not fut.done():
                        fut.set_result(msg)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            # fail all pending
            for fut in list(self._pending.values()):
                if not fut.done():
                    fut.set_exception(exc)
            self._pending.clear()

    async def _request(self, method: str, params: Dict[str, Any], *, is_tool: bool) -> Dict[str, Any]:
        if not self._proc:
            await self.start()

        req_id = self._next_id()
        msg = {"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}
        self._log("->", msg)

        fut: asyncio.Future[Dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        await self._write_json(msg)
        return await fut

    async def _notify(self, method: str, params: Dict[str, Any]) -> None:
        if not self._proc:
            await self.start()
        msg = {"jsonrpc": "2.0", "method": method, "params": params}
        self._log("->", msg)
        await self._write_json(msg)

    async def tool(self, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        args = dict(arguments or {})
        if self.auth_token and name in _AUTH_TOOLS and "auth_token" not in args:
            args["auth_token"] = self.auth_token
        return await self._request("tools/call", {"name": name, "arguments": args}, is_tool=True)

    async def jsonrpc(self, method: str, params: Dict[str, Any], *, external_id: Any = None) -> Dict[str, Any]:
        """Send an arbitrary JSON-RPC request to the MCP server.

        The MCP stdio server expects integer ids; we always generate a local integer id and
        then (optionally) rewrite the response id back to the caller-provided external id.
        """
        resp = await self._request(method, params, is_tool=(method == "tools/call"))
        if external_id is not None and "id" in resp:
            # Preserve caller's id (Claude Desktop expects its own ids echoed back).
            resp["id"] = external_id
        return resp
