# Supertable MCP — Docker + Claude Desktop Setup

## Architecture

This deployment keeps a **single MCP profile** (`mcp`) and uses the HTTPS sidecar only for TLS. The externally documented endpoints are:

- `/mcp`
- `/web`
- `/simulation`

`/simulator` remains available as a backward-compatible alias.


```
┌──────────────────────────────────────────────────────────┐
│  Docker container — single process, single port (:8099)  │
│                                                          │
│  entrypoint.sh (SERVICE=mcp)                             │
│    └─▶ web_server.py ──▶ uvicorn ──▶ web_app.py          │
│                                                          │
│  Endpoints:                                              │
│    /mcp        Streamable HTTP transport (MCP SDK)       │
│    / and /web  Web tester UI (web_tester.html)           │
│    /simulation Claude-like MCP simulator UI              │
│    /mcp_v1     JSON-RPC proxy (used by web tester)       │
│    /api/*      REST endpoints (used by web tester)       │
│    /health     Liveness probe (no auth)                  │
│                                                          │
│  Transport: Streamable HTTP (default, recommended).      │
│  The FastMCP instance is created with stateless_http=True │
│  and json_response=True for production scalability.      │
│  The session manager lifecycle is handled via the        │
│  SDK-recommended session_manager.run() pattern.          │
│                                                          │
│  Internal: web_client.py spawns mcp_server.py as a       │
│  stdio subprocess to power /web and /api/* endpoints.    │
└──────────────────────────────────────────────────────────┘
         ▲                              ▲
         │                              │
   Claude Desktop                  Browser
   (via mcp_stdio_proxy.py)    http://host:8099/?auth=TOKEN
   → https://host:8470/mcp
```

### How Claude Desktop connects

Claude Desktop only speaks **stdio** to local processes. It cannot connect
directly to a remote HTTP server via `claude_desktop_config.json`.

To bridge the gap, use `mcp_stdio_proxy.py` — a zero-dependency Python script
that runs locally on your machine, reads JSON-RPC from stdin, forwards it over
HTTPS to the remote `/mcp` endpoint, and relays responses back via stdout.

```
Claude Desktop ←stdio→ mcp_stdio_proxy.py ←HTTPS→ Docker :8470/mcp
```

> **Note:** Claude Desktop's Settings → Integrations can connect to remote
> servers directly, but only if the URL is publicly accessible. For local
> networks, Docker, or self-signed certs, use the proxy.

---

## Files

| File | Purpose |
|------|---------|
| `mcp_server.py` | MCP server — registers all tools, talks to Supertable backend. Default transport: `streamable-http` with `stateless_http=True`, `json_response=True`. |
| `web_app.py` | FastAPI app — mounts MCP transport at `/mcp` using `streamable_http_app()` + `session_manager.run()`, serves web tester, exposes `/api/*` |
| `web_server.py` | Uvicorn runner — starts `web_app.py` on the configured port |
| `web_client.py` | Async stdio client — spawns `mcp_server.py` as subprocess for the web tester proxy |
| `web_tester.html` | Browser UI — form-based dev tool for calling MCP tools |
| `mcp_simulator.html` | Browser UI — Claude-like MCP simulator for a pasted MCP URL |
| `mcp_stdio_proxy.py` | stdio-to-HTTP bridge — runs locally, forwards Claude Desktop traffic to a remote MCP server over HTTPS. Zero external dependencies (Python stdlib only). |

---

## 1. `.env` file

```env
# ── Required ──────────────────────────────────────────
SUPERTABLE_SUPERTOKEN=change-me-gateway-secret     # protects /, /web, /simulation, /api/*
SUPERTABLE_MCP_AUTH_TOKEN=change-me-tool-secret     # protects MCP tool calls

# ── Infrastructure ────────────────────────────────────
REDIS_HOST=redis-master
REDIS_PORT=6379
REDIS_PASSWORD=change_me_to_a_strong_password
STORAGE_ENDPOINT_URL=http://minio:9000
STORAGE_BUCKET=supertable
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin123!
STORAGE_FORCE_PATH_STYLE=true

# ── Optional (defaults shown) ────────────────────────
# SUPERTABLE_MCP_TRANSPORT=streamable-http           # default; set to "stdio" for local-only mode
# SUPERTABLE_MCP_WEB_PORT=8099
# SUPERTABLE_HOST=0.0.0.0
# SUPERTABLE_REQUIRE_EXPLICIT_ROLE=true
# SUPERTABLE_ALLOWED_ROLES=                          # comma-separated; empty = allow all
# SUPERTABLE_DEFAULT_LIMIT=200
# SUPERTABLE_MAX_LIMIT=5000
# SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC=60
# SUPERTABLE_MAX_CONCURRENCY=6
# LOG_LEVEL=INFO
```

---

## 2. Start the container

```bash
# Build and start the single MCP profile plus HTTPS sidecar
docker compose --profile mcp --profile https up -d --build

# Check it's running
docker compose --profile mcp --profile https ps
```

Verify the health endpoint:

```bash
curl http://localhost:8099/health
curl -k https://localhost:8470/health
```

Expected response:

```json
{"status": "ok", "mcp_client": "ready", "mcp_streamable_http": "mounted"}
```

Both `mcp_client: ready` and `mcp_streamable_http: mounted` must show. If `mcp_streamable_http` is `not_mounted`, the MCP SDK is missing or too old — run `pip install --upgrade mcp` inside the container.

---

## 3. Connect Claude Desktop

The MCP SDK's Streamable HTTP transport is mounted at `/mcp` by `web_app.py`.
Claude Desktop (and any MCP SDK client) connects to this endpoint.

### Method A: `mcp_stdio_proxy.py` (recommended for local / Docker / self-signed)

Copy `mcp_stdio_proxy.py` to your local machine (it has zero external
dependencies — Python 3.8+ stdlib only).

Edit `claude_desktop_config.json`:

| OS | Path |
|----|------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |

```json
{
  "mcpServers": {
    "supertable": {
      "command": "python3",
      "args": [
        "/path/to/mcp_stdio_proxy.py",
        "--url", "https://192.168.168.130:8470/mcp",
        "--insecure"
      ],
      "env": {
        "SUPERTABLE_ORGANIZATION": "your-org-name",
        "SUPERTABLE_ROLE": "superadmin"
      }
    }
  }
}
```

On Windows, use `"command": "py"` (or the full path to `python.exe`) and
backslash paths in `args`:

```json
{
  "mcpServers": {
    "supertable": {
      "command": "py",
      "args": [
        "C:\\TMP\\mcp_stdio_proxy.py",
        "--url", "https://192.168.168.130:8470/mcp",
        "--insecure"
      ],
      "env": {
        "SUPERTABLE_ORGANIZATION": "your-org-name",
        "SUPERTABLE_ROLE": "superadmin"
      }
    }
  }
}
```

Replace `192.168.168.130` with your server's IP or hostname.

**`--insecure`** skips TLS certificate verification — use it for self-signed
certs (Caddy sidecar default). Remove the flag if you have a valid CA-signed
certificate.

**`env` block:** These environment variables are passed to the proxy subprocess
and are visible to Claude Desktop as context. Claude will use
`SUPERTABLE_ORGANIZATION` and `SUPERTABLE_ROLE` as default values when calling
MCP tools, so you don't have to repeat them in every tool invocation.

Restart Claude Desktop after editing the config.

### Method B: Claude Desktop Integrations (remote / publicly accessible only)

If your MCP server is publicly accessible (not behind NAT, not self-signed),
you can connect directly without the proxy:

1. Open Claude Desktop → **Settings → Integrations**
2. Click **Add custom connector**
3. Enter the URL: `https://<public-host>/mcp`

> **Important:** Claude Desktop will not connect to remote servers configured
> via `claude_desktop_config.json` — only locally spawned processes. Remote
> servers must either use the proxy (Method A) or be added via
> Settings → Integrations.

### Method C: `mcp-remote` npm bridge (alternative to the proxy)

If you prefer a Node.js-based bridge instead of the Python proxy, you can use
the `mcp-remote` npm package:

```json
{
  "mcpServers": {
    "supertable": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "https://192.168.168.130:8470/mcp"
      ]
    }
  }
}
```

> **Note:** `mcp-remote` does not support `--insecure` for self-signed certs.
> For self-signed deployments, use `mcp_stdio_proxy.py` (Method A) instead.

### Method D: HTTPS via Caddy TLS sidecar

The Caddy sidecar provides TLS termination on port 8470. Start it alongside
the MCP profile:

```bash
docker compose --profile mcp --profile https up -d --build
```

One-time CA trust (so your OS accepts the self-signed cert):

```bash
docker compose --profile https exec caddy-https caddy trust
```

Then use the single HTTPS origin:
- `https://<host>:8470/mcp`
- `https://<host>:8470/web?auth=<gateway-token>`
- `https://<host>:8470/simulation?auth=<gateway-token>`

### Claude.ai Custom Connectors (remote MCP)

You can also connect via Claude.ai:

1. Go to **Settings → Connectors → Add custom connector**
2. Enter the URL: `https://your-host:8470/mcp`
3. Complete authentication if configured

---

## 4. Open the Web Tester

```
https://localhost:8470/web?auth=change-me-gateway-secret
```

The root path `/` still serves the same tester for backward compatibility.

Open the simulator:

```
https://localhost:8470/simulation?auth=change-me-gateway-secret
```

The `?auth=` value is your `SUPERTABLE_SUPERTOKEN`. The page stores it in localStorage so you only pass it once.

---

## 5. Endpoint summary

| Path | Auth | Used by |
|------|------|---------|
| `/mcp` | None at transport layer (tool-level auth via `SUPERTABLE_MCP_AUTH_TOKEN`) | Claude Desktop / any MCP client |
| `/` and `/web` | `SUPERTABLE_SUPERTOKEN` | Browser (web tester UI) |
| `/simulation` | `SUPERTABLE_SUPERTOKEN` | Browser (Claude-like MCP simulator UI) |
| `/simulator` | `SUPERTABLE_SUPERTOKEN` | Browser (backward-compatible alias) |
| `/mcp_v1` | `SUPERTABLE_SUPERTOKEN` | Web tester (JSON-RPC proxy) |
| `/api/*` | `SUPERTABLE_SUPERTOKEN` | Web tester (REST endpoints) |
| `/health` | none | Kubernetes / Docker healthcheck |

---

## 6. Port summary

| Port | Protocol | Service |
|------|----------|---------|
| 8099 | HTTP | MCP web app (`/mcp`, `/web`, `/simulation`) |
| 8470 | HTTPS | MCP web app (`/mcp`, `/web`, `/simulation`) via Caddy |

---

## 7. Transport details

The default transport is **Streamable HTTP** (`SUPERTABLE_MCP_TRANSPORT=streamable-http`), which is the MCP SDK's recommended transport for production deployments. The server is created with:

- `stateless_http=True` — no server-side session state, optimal for horizontal scaling
- `json_response=True` — JSON responses instead of SSE streams for simple request/response patterns
- `streamable_http_path="/"` — the SDK handles requests at the mount root; the host app mounts at `/mcp`

The session manager is started via `session_manager.run()` in the FastAPI lifespan, following the official SDK pattern.

To use stdio transport (e.g. for local development or the mcp_client.py test tool):

```bash
SUPERTABLE_MCP_TRANSPORT=stdio python -m supertable.mcp.mcp_server
```

---

## Troubleshooting

### Claude Desktop says "failed to connect"

1. Check the container: `docker compose --profile mcp ps`
2. Check health: `curl http://localhost:8099/health`
3. HTTPS check: `curl -k https://localhost:8470/health`
4. Check logs: `docker compose --profile mcp logs supertable-mcp`
5. If `mcp_streamable_http: not_mounted` — upgrade the MCP SDK inside the container (`pip install --upgrade mcp`)
6. If using the proxy, verify the proxy script path is correct and Python is available on PATH

### Proxy works but returns errors

1. Check that the remote server is reachable: `curl -k https://<host>:8470/health`
2. If you see `HTTP 403` or `HTTP 401`, the `/mcp` endpoint itself has no gateway auth — this likely means Caddy or a reverse proxy is blocking the request
3. If you see SSL errors without `--insecure`, either add the flag or trust the CA cert (`caddy trust`)

### Web tester works but Claude Desktop doesn't

The web tester uses `/mcp_v1` (custom JSON-RPC proxy via stdio subprocess) which works independently. Claude Desktop needs `/mcp` (Streamable HTTP transport). Confirm `mcp_streamable_http: mounted` in `/health`.

### "auth_token is required by server policy"

Each MCP tool requires `auth_token` as a parameter. Options:
- Set `SUPERTABLE_REQUIRE_TOKEN=false` if the gateway auth is sufficient for your setup
- Configure the MCP client to pass the token in tool arguments

### "role is required by server policy"

The server has `SUPERTABLE_REQUIRE_EXPLICIT_ROLE=true` (the default). Either:
- Pass `role` in every tool call (Claude will do this automatically if `SUPERTABLE_ROLE` is set in the proxy's `env` block)
- Set `SUPERTABLE_REQUIRE_EXPLICIT_ROLE=false` on the server and provide a default via `SUPERTABLE_ROLE` in the server's `.env`

### "Task group is not initialized"

This means the MCP session manager's lifespan was not started. The `web_app.py` lifespan handler calls `session_manager.run()` automatically. Ensure:
- The MCP SDK is installed and up to date (`pip install --upgrade mcp`)
- The `mcp_server.py` module imports correctly (check container logs)

### Port already in use

```env
SUPERTABLE_MCP_WEB_PORT=9099
```

Update `docker-compose.yml` ports and Claude Desktop URL accordingly.

### Container keeps restarting

Check `docker compose --profile mcp logs supertable-mcp`. Common causes:
- Old image — rebuild with `docker compose --profile mcp build --no-cache`
- Missing env vars — ensure `.env` has `SUPERTABLE_SUPERTOKEN` set
- Redis/MinIO not running — start infrastructure first
