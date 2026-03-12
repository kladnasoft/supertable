# Supertable MCP — Docker + Claude Desktop Setup

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│  Docker container — single process, single port (:8099)  │
│                                                          │
│  entrypoint.sh (SERVICE=mcp)                             │
│    └─▶ web_server.py ──▶ uvicorn ──▶ web_app.py          │
│                                                          │
│  Endpoints:                                              │
│    /mcp      Streamable HTTP transport (MCP SDK)         │
│    /         Web tester UI (web_tester.html)             │
│    /mcp_v1   JSON-RPC proxy (used by web tester)         │
│    /api/*    REST endpoints (used by web tester)         │
│    /health   Liveness probe (no auth)                    │
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
   http://host:8099/mcp        http://host:8099/?auth=TOKEN
```

---

## Files

| File | Purpose |
|------|---------|
| `mcp_server.py` | MCP server — registers all tools, talks to Supertable backend. Default transport: `streamable-http` with `stateless_http=True`, `json_response=True`. |
| `web_app.py` | FastAPI app — mounts MCP transport at `/mcp` using `streamable_http_app()` + `session_manager.run()`, serves web tester, exposes `/api/*` |
| `web_server.py` | Uvicorn runner — starts `web_app.py` on the configured port |
| `web_client.py` | Async stdio client — spawns `mcp_server.py` as subprocess for the web tester proxy |
| `web_tester.html` | Browser UI — form-based dev tool for calling MCP tools |

---

## 1. `.env` file

```env
# ── Required ──────────────────────────────────────────
SUPERTABLE_SUPERTOKEN=change-me-gateway-secret     # protects web UI, /web, /api/*
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
# Build and start (uses the mcp profile from docker-compose.yml)
docker compose --profile mcp up -d --build

# Check it's running
docker compose --profile mcp ps
```

Verify the health endpoint:

```bash
curl http://localhost:8099/health
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

### Method A: Claude Desktop Integrations (recommended for remote)

1. Open Claude Desktop → **Settings → Integrations**
2. Click **Add custom connector**
3. Enter the URL: `http://<host>:8099/mcp` (or `https://<host>:8470/mcp` with TLS)
4. No additional config needed — the server is authless at the MCP transport layer

> **Note:** Claude Desktop will not connect to remote servers configured via
> `claude_desktop_config.json`. Remote servers must be added via Settings → Integrations.

### Method B: `mcp-remote` bridge (stdio → HTTP)

If your Claude Desktop version doesn't support Integrations, use the `mcp-remote`
npm package as a stdio-to-HTTP bridge.

Edit `claude_desktop_config.json`:

| OS | Path |
|----|------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |

```json
{
  "mcpServers": {
    "supertable": {
      "command": "npx",
      "args": [
        "mcp-remote",
        "http://192.168.168.130:8099/mcp"
      ]
    }
  }
}
```

Replace `192.168.168.130` with your server's IP or hostname.

### Method C: HTTPS via Caddy TLS sidecar

```bash
docker compose --profile mcp --profile https up -d --build
```

One-time CA trust (so your OS accepts the self-signed cert):

```bash
docker compose --profile https exec caddy-https caddy trust
```

Then use `https://<host>:8470/mcp` in any of the methods above.

### Claude.ai Custom Connectors (remote MCP)

You can also connect via Claude.ai:

1. Go to **Settings → Connectors → Add custom connector**
2. Enter the URL: `https://your-host:8470/mcp`
3. Complete authentication if configured

Restart Claude Desktop after editing the config.

---

## 4. Open the Web Tester

```
http://localhost:8099/?auth=change-me-gateway-secret
```

The `?auth=` value is your `SUPERTABLE_SUPERTOKEN`. The page stores it in localStorage so you only pass it once.

With HTTPS via Caddy:

```
https://localhost:8470/?auth=change-me-gateway-secret
```

---

## 5. Endpoint summary

| Path | Auth | Used by |
|------|------|---------|
| `/mcp` | MCP SDK auth (`SUPERTABLE_MCP_AUTH_TOKEN`) | Claude Desktop / any MCP client |
| `/` | `SUPERTABLE_SUPERTOKEN` | Browser (web tester UI) |
| `/mcp_v1` | `SUPERTABLE_SUPERTOKEN` | Web tester (JSON-RPC proxy) |
| `/api/*` | `SUPERTABLE_SUPERTOKEN` | Web tester (REST endpoints) |
| `/health` | none | Kubernetes / Docker healthcheck |

---

## 6. Port summary

| Port | Protocol | Service |
|------|----------|---------|
| 8099 | HTTP | MCP server + web tester (direct) |
| 8470 | HTTPS | MCP server + web tester (via Caddy) |

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
3. Check logs: `docker compose --profile mcp logs supertable-mcp`
4. If `mcp_streamable_http: not_mounted` — upgrade the MCP SDK inside the container (`pip install --upgrade mcp`)

### Web tester works but Claude Desktop doesn't

The web tester uses `/mcp_v1` (custom JSON-RPC proxy via stdio subprocess) which works independently. Claude Desktop needs `/mcp` (Streamable HTTP transport). Confirm `mcp_streamable_http: mounted` in `/health`.

### "auth_token is required by server policy"

Each MCP tool requires `auth_token` as a parameter. Options:
- Set `SUPERTABLE_REQUIRE_TOKEN=false` if the gateway auth is sufficient for your setup
- Configure the MCP client to pass the token in tool arguments

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
