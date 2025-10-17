#!/usr/bin/env python3
# mcp_client.py — robust local MCP stdio client for Supertable
#
# New since last version:
#   • Prints every SQL before sending (attempt + fallback).
#   • Pre-detects table-less SQL (no FROM/JOIN) and auto-falls back to a sample query.
#   • Keeps server-error-based fallback (e.g., "No table found in the query").
#   • Default --sql is empty; if env still sets SELECT 1, we’ll still fall back.

from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
from typing import Any, Dict, Optional, Tuple, List

# ------------------------------------------------------------------------------
# Diagnostics
# ------------------------------------------------------------------------------

_LOADED_ENV_PATHS: List[str] = []
_LOADED_JSON_PATHS: List[str] = []

# ------------------------------------------------------------------------------
# Lightweight config loaders (.env and .json)
# ------------------------------------------------------------------------------

def _load_env_file(path: str) -> None:
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                if s.startswith("export "):
                    s = s[len("export "):].strip()
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = k.strip()
                v = v.strip()
                if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                    v = v[1:-1]
                os.environ.setdefault(k, v)
        _LOADED_ENV_PATHS.append(os.path.abspath(path))
    except Exception:
        pass  # best-effort only


def _load_json_config(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                _LOADED_JSON_PATHS.append(os.path.abspath(path))
                return data
    except Exception:
        pass
    return {}


def _find_upwards(start_dir: str, filenames: List[str]) -> List[str]:
    seen = set()
    found: List[str] = []
    try:
        cur = os.path.abspath(start_dir)
    except Exception:
        return found

    root = os.path.abspath(os.sep)
    while True:
        for fn in filenames:
            p = os.path.join(cur, fn)
            if p in seen:
                continue
            seen.add(p)
            if os.path.exists(p):
                found.append(p)
        if cur == root:
            break
        cur = os.path.dirname(cur)
    return found


def _paths_candidates(server_path: str) -> Dict[str, List[str]]:
    home = os.path.expanduser("~")
    xdg = os.getenv("XDG_CONFIG_HOME", os.path.join(home, ".config"))
    cfgdir = os.path.join(xdg, "supertable")

    env_names = [".env", ".env.local", "mcp.local.env"]

    anchors = [
        os.getcwd(),
        os.path.dirname(os.path.abspath(server_path)),
        os.path.dirname(os.path.abspath(__file__)),
    ]

    upward_envs: List[str] = []
    for anchor in anchors:
        upward_envs.extend(_find_upwards(anchor, env_names))

    env_static = [os.path.join(cfgdir, "mcp.local.env")]
    json_static = [
        os.path.join(cfgdir, "mcp.local.json"),
        ".mcp.local.json",
        "mcp.local.json",
    ]
    hash_files = [
        os.path.join(cfgdir, "user_hash"),
        ".mcp.user_hash",
        ".supertable_user_hash",
    ]

    return {"json": json_static, "env": env_static + upward_envs, "hash_files": hash_files}


def load_config(server_path: str, verbose: bool) -> Dict[str, Any]:
    paths = _paths_candidates(server_path)

    json_cfg: Dict[str, Any] = {}
    for p in paths["json"]:
        json_cfg.update(_load_json_config(p))
    for p in paths["env"]:
        _load_env_file(p)

    cfg: Dict[str, Any] = {}
    cfg["server_path"] = os.getenv("MCP_SERVER_PATH", json_cfg.get("server_path", server_path or "mcp_server.py"))
    cfg["wire"] = os.getenv("MCP_WIRE", json_cfg.get("wire", "ndjson"))
    cfg["org"] = os.getenv("SUPERTABLE_TEST_ORG", json_cfg.get("org", ""))
    cfg["super"] = os.getenv("SUPERTABLE_TEST_SUPER", json_cfg.get("super", ""))
    cfg["user_hash"] = os.getenv(
        "SUPERTABLE_TEST_USER_HASH",
        os.getenv("SUPERTABLE_SYSADMIN_USER_HASH", json_cfg.get("user_hash", "")),
    )
    # Default SQL now empty; if env provides something, we use it (and may fallback)
    cfg["sql"] = os.getenv("SUPERTABLE_TEST_QUERY", json_cfg.get("sql", ""))
    cfg["hash_files"] = paths["hash_files"]

    if verbose:
        def _redact(h: str) -> str:
            h = (h or "").strip()
            if len(h) >= 8:
                return f"{h[:4]}…{h[-4:]}"
            return h or "(empty)"
        print(
            "mcp_client config:\n"
            f"  loaded .env files: { _LOADED_ENV_PATHS or 'none' }\n"
            f"  loaded .json files: { _LOADED_JSON_PATHS or 'none' }\n"
            f"  org={cfg['org'] or '(empty)'}  super={cfg['super'] or '(empty)'}  user_hash={_redact(cfg['user_hash'])}\n",
            flush=True,
        )
    return cfg

# ------------------------------------------------------------------------------
# Wire (NDJSON + LSP autodetect)
# ------------------------------------------------------------------------------

class Wire:
    def __init__(self, proc: subprocess.Popen, mode: Optional[str] = None) -> None:
        self.proc = proc
        sel = (mode or "").strip().lower()
        self.mode = sel if sel in {"ndjson", "lsp"} else "ndjson"
        self._reader = io.BufferedReader(proc.stdout)  # type: ignore[arg-type]

    def send(self, msg: Dict[str, Any]) -> None:
        data = json.dumps(msg, separators=(",", ":")).encode("utf-8")
        if self.mode == "lsp":
            header = f"Content-Length: {len(data)}\r\n\r\n".encode("ascii")
            self.proc.stdin.write(header)  # type: ignore[arg-type]
            self.proc.stdin.write(data)    # type: ignore[arg-type]
        else:
            self.proc.stdin.write(data + b"\n")  # type: ignore[arg-type]
        self.proc.stdin.flush()  # type: ignore[union-attr]

    def _read_headers_lsp(self) -> int:
        headers = bytearray()
        while b"\r\n\r\n" not in headers:
            ch = self._reader.read(1)
            if not ch:
                raise RuntimeError("Server closed pipe while reading LSP headers")
            headers += ch
        for line in headers.decode("ascii", errors="replace").split("\r\n"):
            if line.lower().startswith("content-length:"):
                return int(line.split(":", 1)[1].strip())
        raise RuntimeError(f"Missing Content-Length in headers: {headers!r}")

    def _read_lsp_message(self) -> Dict[str, Any]:
        length = self._read_headers_lsp()
        body = self._reader.read(length)
        if not body or len(body) < length:
            raise RuntimeError("Truncated LSP body from server")
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON from server (LSP): {body!r}") from exc

    def _read_ndjson_message(self) -> Dict[str, Any]:
        line = self._reader.readline()
        if not line:
            raise RuntimeError("Server closed pipe while reading NDJSON line")
        while line.strip() == b"":
            line = self._reader.readline()
            if not line:
                raise RuntimeError("Server closed pipe while reading NDJSON line")
        if line.lower().startswith(b"content-length:"):
            rest = self._reader.readline()
            while rest.strip() != b"":
                rest = self._reader.readline()
            try:
                length = int(line.split(b":", 1)[1].strip())
            except Exception:
                raise RuntimeError(f"Bad LSP header line: {line!r}")
            body = self._reader.read(length)
            if not body or len(body) < length:
                raise RuntimeError("Truncated LSP body after autodetect")
            self.mode = "lsp"
            try:
                return json.loads(body.decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Invalid JSON from server (autodetect LSP): {body!r}") from exc
        try:
            return json.loads(line.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Invalid JSON from server (NDJSON): {line!r}") from exc

    def read_message(self) -> Dict[str, Any]:
        if self.mode == "lsp":
            return self._read_lsp_message()
        return self._read_ndjson_message()

# ------------------------------------------------------------------------------
# RPC helpers
# ------------------------------------------------------------------------------

class RpcIds:
    def __init__(self) -> None:
        self._i = 0

    def next(self) -> int:
        self._i += 1
        return self._i


def pretty(label: str, msg: Dict[str, Any]) -> None:
    print(f"{label} -> {json.dumps(msg, indent=2)}", flush=True)


def send_request(wire: Wire, ids: RpcIds, method: str, params: Dict[str, Any]) -> int:
    req_id = ids.next()
    wire.send({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params})
    return req_id


def wait_for_response(wire: Wire, req_id: int) -> Dict[str, Any]:
    while True:
        msg = wire.read_message()
        if "id" in msg and msg.get("id") == req_id:
            return msg
        if "method" in msg and "id" not in msg:
            print(f"notification <- {json.dumps(msg, indent=2)}", flush=True)
        else:
            print(f"other <- {json.dumps(msg, indent=2)}", flush=True)

# ------------------------------------------------------------------------------
# Tool-call convenience
# ------------------------------------------------------------------------------

def call_tool(wire: Wire, ids: RpcIds, name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
    rid = send_request(wire, ids, "tools/call", {"name": name, "arguments": arguments})
    return wait_for_response(wire, rid)


def extract_structured_result(resp: Dict[str, Any]) -> Tuple[bool, Any]:
    result = resp.get("result", {})
    if not isinstance(result, dict):
        return False, result
    if result.get("isError") is True:
        return True, result
    sc = result.get("structuredContent")
    if isinstance(sc, dict) and "result" in sc:
        return False, sc["result"]
    content = result.get("content")
    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict) and first.get("type") == "text":
            try:
                return False, json.loads(first.get("text") or "")
            except Exception:
                return False, first.get("text")
    return False, result


def print_query_result(obj: Any) -> None:
    if not isinstance(obj, dict):
        print(obj, flush=True)
        return
    cols = obj.get("columns") or []
    rows = obj.get("rows") or []
    status = obj.get("status")
    message = obj.get("message")
    print(f"status={status} rows={len(rows)} cols={len(cols)}", flush=True)
    if message:
        print(f"message: {message}", flush=True)
    if cols and rows:
        widths = [max(len(str(c)), *(len(str(r[i])) for r in rows[:10])) for i, c in enumerate(cols)]
        header = " | ".join(str(c).ljust(widths[i]) for i, c in enumerate(cols))
        print(header, flush=True)
        print("-+-".join("-" * w for w in widths), flush=True)
        for r in rows[:10]:
            print(" | ".join(str(r[i]).ljust(widths[i]) for i in range(len(cols))), flush=True)
        if len(rows) > 10:
            print(f"... ({len(rows)-10} more rows)", flush=True)

# ------------------------------------------------------------------------------
# Input helpers
# ------------------------------------------------------------------------------

def resolve_path(p: str) -> str:
    if not os.path.isabs(p):
        p = os.path.abspath(p)
    return p


def _read_first_existing(paths: List[str]) -> str:
    for p in paths:
        try:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    s = f.read().strip()
                    if s:
                        return s
        except Exception:
            continue
    return ""


def _prompt_from_tty(prompt: str) -> str:
    if sys.stdin.isatty():
        try:
            return input(prompt).strip()
        except EOFError:
            return ""
    try:
        with open("/dev/tty", "r", encoding="utf-8") as tty:
            print(prompt, end="", flush=True)
            return tty.readline().strip()
    except Exception:
        return ""


def resolve_user_hash(seed: str, hash_files: List[str], require: bool, verbose: bool) -> str:
    u = (seed or "").strip()
    if u:
        if verbose:
            print(f"resolved user_hash from env/config: {u[:4]}…{u[-4:]}", flush=True)
        return u
    u = _read_first_existing(hash_files)
    if u:
        if verbose:
            print(f"resolved user_hash from file: {u[:4]}…{u[-4:]}", flush=True)
        return u
    u = _prompt_from_tty("Enter user_hash (32/64 hex): ")
    if u:
        return u
    if require:
        print(
            "ERROR: server requires user_hash but none was provided.\n"
            f"  Searched .env files: { _LOADED_ENV_PATHS or 'none' }\n"
            "  Supply --hash or set SUPERTABLE_TEST_USER_HASH in one of the above files,\n"
            "  or create ~/.config/supertable/user_hash containing the hash.\n",
            file=sys.stderr,
            flush=True,
        )
        sys.exit(2)
    return ""

# ------------------------------------------------------------------------------
# Fallback query logic
# ------------------------------------------------------------------------------

def _looks_tableless(sql: str) -> bool:
    s = (sql or "").strip().lower()
    if not s:
        return True
    # very simple heuristic: no FROM/JOIN tokens -> likely tableless
    return (" from " not in f" {s} ") and (" join " not in f" {s} ")


def _get_first_table(wire: Wire, ids: RpcIds, org: str, super_name: str, user_hash: str) -> Optional[str]:
    lt = call_tool(
        wire, ids, "list_tables",
        {"super_name": super_name, "organization": org, "user_hash": user_hash}
    )
    pretty("tools/call list_tables (fallback)", lt)
    _err, payload = extract_structured_result(lt)
    if isinstance(payload, dict) and "result" in payload:
        val = payload["result"]
    else:
        val = payload
    if isinstance(val, list) and val:
        return val[0]
    return None


def _is_no_table_error(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    msg = (payload.get("message") or "").lower()
    return "no table found" in msg


def _print_sql(label: str, sql: str) -> None:
    print(f"{label} SQL: {sql}", flush=True)


def safe_query_with_fallback(
    wire: Wire,
    ids: RpcIds,
    org: str,
    super_name: str,
    sql: str,
    user_hash: str,
    engine: str,
    timeout: float,
) -> Tuple[Dict[str, Any], Any]:
    # 0) If SQL looks tableless, skip straight to fallback sampling
    if _looks_tableless(sql):
        print("Detected table-less SQL (or empty). Using fallback sampler.", flush=True)
    else:
        # 1) Attempt original query first
        q_args: Dict[str, Any] = {
            "super_name": super_name,
            "organization": org,
            "sql": sql.strip(),
            "limit": 10,
            "user_hash": user_hash,
        }
        if engine:
            q_args["engine"] = engine
        if timeout > 0:
            q_args["query_timeout_sec"] = timeout

        _print_sql("Attempt", q_args["sql"])
        q_resp = call_tool(wire, ids, "query_sql", q_args)
        pretty("tools/call query_sql", q_resp)
        _err, payload = extract_structured_result(q_resp)
        if not (isinstance(payload, dict) and payload.get("status") == "ERROR" and _is_no_table_error(payload)):
            return q_resp, payload
        print("Server rejected SQL as table-less; falling back to sample.", flush=True)

    # 2) Fallback: sample first table
    first_tbl = _get_first_table(wire, ids, org, super_name, user_hash)
    if not first_tbl:
        return {"result": {"status": "SKIP", "message": "No tables found for fallback"}}, {"status": "SKIP", "message": "No tables found"}

    sample_sql = f'SELECT * FROM "{first_tbl}" LIMIT 10'
    _print_sql("Fallback", sample_sql)

    q_args: Dict[str, Any] = {
        "super_name": super_name,
        "organization": org,
        "sql": sample_sql,
        "limit": 10,
        "user_hash": user_hash,
    }
    if engine:
        q_args["engine"] = engine
    if timeout > 0:
        q_args["query_timeout_sec"] = timeout

    q_resp = call_tool(wire, ids, "query_sql", q_args)
    pretty("tools/call query_sql (fallback sample)", q_resp)
    _err, payload = extract_structured_result(q_resp)
    return q_resp, payload

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Local MCP stdio client")
    parser.add_argument("--server", dest="server_path", default=os.getenv("MCP_SERVER_PATH", "mcp_server.py"))
    parser.add_argument("--wire", dest="wire", default=os.getenv("MCP_WIRE", "ndjson"), choices=["ndjson", "lsp"])
    parser.add_argument("--org", dest="org", default=os.getenv("SUPERTABLE_TEST_ORG", ""))
    parser.add_argument("--super", dest="super_name", default=os.getenv("SUPERTABLE_TEST_SUPER", ""))
    parser.add_argument("--hash", dest="user_hash",
                        default=os.getenv("SUPERTABLE_TEST_USER_HASH", os.getenv("SUPERTABLE_SYSADMIN_USER_HASH", "")))
    # Default SQL may be empty; if env gives SELECT 1, we will show it and then fallback
    parser.add_argument("--sql", dest="sql", default=os.getenv("SUPERTABLE_TEST_QUERY", ""))
    parser.add_argument("--engine", dest="engine", default=os.getenv("SUPERTABLE_TEST_ENGINE", ""))
    parser.add_argument("--timeout", dest="timeout", type=float, default=float(os.getenv("SUPERTABLE_TEST_TIMEOUT_SEC", "0") or 0))
    parser.add_argument("--list-tables", action="store_true", help="Call list_tables after list_supers")
    parser.add_argument("--describe", dest="describe_table", default="", help="Call describe_table for TABLE")
    parser.add_argument("--stats", dest="stats_table", default="", help="Call get_table_stats for TABLE")
    parser.add_argument("--get-meta", action="store_true", help="Call get_super_meta for the chosen super")
    parser.add_argument("--no-query", action="store_true", help="Skip the query_sql call")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose config diagnostics")

    args = parser.parse_args()

    server_path = resolve_path(args.server_path)
    if not os.path.exists(server_path):
        raise SystemExit(f"Server script not found: {server_path}")

    cfg = load_config(server_path, verbose=args.verbose)

    # CLI overrides
    org = (args.org or cfg["org"]).strip()
    super_name = (args.super_name or cfg["super"]).strip()
    user_hash_seed = (args.user_hash or cfg["user_hash"]).strip()
    wire_mode = args.wire or cfg["wire"]
    sql = (args.sql or cfg["sql"]).strip()  # may be empty or table-less
    engine = (args.engine or "").strip()
    timeout = float(args.timeout or 0)

    # Start server process (stdio)
    cmd = [sys.executable, "-u", server_path]
    proc = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=sys.stderr,
        bufsize=0,
    )
    if proc.stdin is None or proc.stdout is None:
        raise SystemExit("Failed to open stdio pipes to server process")

    wire = Wire(proc, mode=wire_mode)
    ids = RpcIds()

    # initialize
    init_id = send_request(
        wire,
        ids,
        "initialize",
        {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "local-mcp-client", "version": "1.3"},
        },
    )
    init_resp = wait_for_response(wire, init_id)
    pretty("initialize", init_resp)

    # initialized notification
    wire.send({"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})

    # list tools
    tools_resp = wait_for_response(wire, send_request(wire, ids, "tools/list", {}))
    pretty("tools/list", tools_resp)

    # health/info
    health_resp = call_tool(wire, ids, "health", {})
    pretty("tools/call health", health_resp)

    info_resp = call_tool(wire, ids, "info", {})
    pretty("tools/call info", info_resp)
    _err, info_payload = extract_structured_result(info_resp)
    require_hash = bool(info_payload.get("require_explicit_user_hash", False)) if isinstance(info_payload, dict) else False

    # Resolve user_hash
    user_hash = resolve_user_hash(user_hash_seed, cfg["hash_files"], require_hash, verbose=args.verbose)

    # whoami
    whoami_resp = call_tool(wire, ids, "whoami", {"user_hash": user_hash})
    pretty("tools/call whoami", whoami_resp)

    # organization
    if not org:
        org = _prompt_from_tty("Enter organization: ").strip()
    if not org:
        print("ERROR: missing organization. Supply --org or SUPERTABLE_TEST_ORG.", file=sys.stderr, flush=True)
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()
        sys.exit(2)

    # list_supers
    ls_resp = call_tool(wire, ids, "list_supers", {"organization": org})
    pretty("tools/call list_supers", ls_resp)

    # If any action needs a super, prompt if empty
    need_super = args.list_tables or bool(args.describe_table) or bool(args.stats_table) or args.get_meta or (not args.no_query)
    if need_super and not super_name:
        super_name = _prompt_from_tty("Enter super name: ").strip()

    # optional helpers
    if args.list_tables and super_name:
        lt_resp = call_tool(
            wire,
            ids,
            "list_tables",
            {"super_name": super_name, "organization": org, "user_hash": user_hash},
        )
        pretty("tools/call list_tables", lt_resp)

    if args.describe_table and super_name:
        dt_resp = call_tool(
            wire,
            ids,
            "describe_table",
            {"super_name": super_name, "organization": org, "table": args.describe_table, "user_hash": user_hash},
        )
        pretty("tools/call describe_table", dt_resp)

    if args.stats_table and super_name:
        st_resp = call_tool(
            wire,
            ids,
            "get_table_stats",
            {"super_name": super_name, "organization": org, "table": args.stats_table, "user_hash": user_hash},
        )
        pretty("tools/call get_table_stats", st_resp)

    if args.get_meta and super_name:
        gm_resp = call_tool(
            wire,
            ids,
            "get_super_meta",
            {"super_name": super_name, "organization": org, "user_hash": user_hash},
        )
        pretty("tools/call get_super_meta", gm_resp)

    # query_sql (unless disabled)
    if not args.no_query and super_name:
        q_resp, payload = safe_query_with_fallback(
            wire=wire,
            ids=ids,
            org=org,
            super_name=super_name,
            sql=sql,                   # may be empty or table-less
            user_hash=user_hash,
            engine=engine,
            timeout=timeout,
        )
        if isinstance(payload, dict) and ("columns" in payload or "status" in payload):
            print_query_result(payload)

    # Clean shutdown
    try:
        proc.terminate()
    finally:
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()


if __name__ == "__main__":
    main()
