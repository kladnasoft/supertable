# Data Island Core — MCP Server

## Overview

Data Island Core ships a Model Context Protocol (MCP) server that exposes platform data as read-only tools for AI clients — Claude Desktop, Claude Code, Cursor, ChatGPT, and any MCP-compatible tool. The server implements MCP over two transports: stdio (local, client spawns the server process) and streamable-HTTP (remote, runs as an HTTP endpoint).

The MCP server is the primary integration point for AI-powered data workflows. It provides tools for listing tables, describing schemas, querying data via SQL, profiling columns, managing annotations, and storing application state — everything an AI agent needs to explore and analyze data autonomously.

---

## How it works

### Transport modes

**stdio** — The client (Claude Desktop, Cursor) spawns the MCP server as a subprocess and communicates over stdin/stdout using newline-delimited JSON-RPC 2.0. This is the default transport.

```bash
# Start in stdio mode
python -u supertable/mcp/mcp_server.py
```

**streamable-HTTP** — The server runs as an HTTP endpoint. Each JSON-RPC message is a POST request. Server-sent events (SSE) handle streaming responses.

```bash
export SUPERTABLE_MCP_TRANSPORT=streamable-http
export SUPERTABLE_MCP_HTTP_HOST=0.0.0.0
export SUPERTABLE_MCP_HTTP_PORT=8000
export SUPERTABLE_MCP_HTTP_PATH=/mcp
python -u supertable/mcp/mcp_server.py
```

### Authentication

The MCP server supports two layers of authentication:

**Shared secret token** — Set `SUPERTABLE_REQUIRE_TOKEN=1` and `SUPERTABLE_MCP_AUTH_TOKEN=<secret>`. Every tool call must include `auth_token` matching this value. Optional but recommended.

**User hash enforcement** — Set `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1`. Every tool call must include a `user_hash` (SHA-256 hex string). The hash identifies the calling user for RBAC and audit purposes. Allowed hashes can be restricted via `SUPERTABLE_ALLOWED_USER_HASHES`.

### Concurrency control

The server limits concurrent tool executions via `anyio.CapacityLimiter`, configured by `SUPERTABLE_MAX_CONCURRENCY` (default: 6). Each query has a timeout set by `SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC` (default: 60 seconds).

---

## Available tools

### Data discovery

| Tool | Description |
|---|---|
| `health` | Service status check |
| `info` | Service info and policy flags |
| `whoami` | Validate/normalize user hash |
| `list_supers(organization)` | List all SuperTables in the organization |
| `list_tables(super_name, organization)` | List tables with schemas, stats, and catalog/annotation metadata |
| `describe_table(super_name, organization, table)` | Get table schema (columns, types) |
| `get_table_stats(super_name, organization, table)` | Get table statistics (rows, files, size, version) |
| `get_super_meta(super_name, organization)` | Get full SuperTable metadata |
| `data_freshness(super_name, organization)` | Check last-update timestamps for all tables |
| `auto_discover(super_name, organization)` | Discover table relationships, patterns, and join candidates |

### Querying

| Tool | Description |
|---|---|
| `query_sql(super_name, organization, sql, ...)` | Execute a read-only SQL query. Returns JSON results. |
| `sample_data(super_name, organization, table, ...)` | Get a sample of rows from a table |
| `validate_sql(super_name, organization, sql)` | Validate SQL syntax without executing |
| `profile_column(super_name, organization, table, column)` | Statistical profile of a column (min, max, nulls, distinct, distribution) |
| `search_columns(super_name, organization, pattern)` | Search for columns matching a name pattern across all tables |

The `query_sql` tool enforces read-only SQL — only `SELECT` and `WITH ... SELECT` queries are accepted. DDL and DML keywords are rejected by a blocklist. Results are limited by `SUPERTABLE_MAX_LIMIT` (default: 5,000 rows).

### Annotations and state

| Tool | Description |
|---|---|
| `store_annotation(super_name, organization, ...)` | Store an annotation (rule) for SQL, terminology, visualization, scope, or domain |
| `get_annotations(super_name, organization)` | Retrieve all annotations |
| `delete_annotation(super_name, organization, annotation_id)` | Delete an annotation |
| `store_app_state(super_name, organization, namespace, key, value)` | Store application state (widgets, saved queries, dashboards) |
| `get_app_state(super_name, organization, namespace, key)` | Retrieve application state |
| `delete_app_state(super_name, organization, namespace, key)` | Delete application state |
| `store_catalog(super_name, organization, catalog)` | Store catalog-level metadata |
| `submit_feedback(super_name, organization, rating, ...)` | Submit user feedback (thumbs up/down) |

Annotations are stored as SQL tables within the SuperTable via `DataWriter`. They persist across sessions and are returned by `list_tables` as operator instructions that the AI client should follow.

### Common parameters

Most tools accept these standard parameters:

| Parameter | Type | Description |
|---|---|---|
| `super_name` | str | SuperTable name |
| `organization` | str | Organization name |
| `role` | str | RBAC role name (optional, falls back to `SUPERTABLE_ROLE`) |
| `auth_token` | str | Shared secret token (required when `SUPERTABLE_REQUIRE_TOKEN=1`) |
| `user_hash` | str | User identity hash (required when `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH=1`) |

---

## Connecting AI clients

### Claude Desktop (local, stdio)

Add to `claude_desktop_config.json` (Settings → Developer → Edit Config):

```json
{
  "mcpServers": {
    "supertable": {
      "command": "python",
      "args": ["-u", "/path/to/supertable/mcp/mcp_server.py"],
      "env": {
        "SUPERTABLE_MCP_TRANSPORT": "stdio",
        "SUPERTABLE_ORGANIZATION": "acme",
        "REDIS_HOST": "localhost",
        "STORAGE_TYPE": "MINIO",
        "STORAGE_ENDPOINT_URL": "http://localhost:9000",
        "STORAGE_ACCESS_KEY": "minioadmin",
        "STORAGE_SECRET_KEY": "minioadmin123!"
      }
    }
  }
}
```

### Claude Desktop (remote, streamable-HTTP)

1. Start the MCP server with `SUPERTABLE_MCP_TRANSPORT=streamable-http`
2. In Claude Desktop: Settings → Connectors → Add Custom Connector
3. URL: `http://<host>:8000/mcp`

### Docker

```bash
# stdio (interactive)
docker compose --profile mcp run --rm -i supertable-mcp mcp-server

# HTTP (background)
docker compose --profile mcp up -d
# MCP endpoint: http://localhost:8070/mcp
```

---

## Web tester

`supertable/mcp/web_app.py` provides a browser-based testing UI for MCP tools. It spawns the MCP server as a stdio subprocess and exposes an HTTP gateway.

```bash
python -u supertable/mcp/web_server.py
# http://localhost:8099/?auth=<SUPERTABLE_SUPERTOKEN>
```

The web tester provides an interactive form for each MCP tool, displays JSON responses, and logs the JSON-RPC message exchange.

---

## Configuration

| Environment variable | Default | Description |
|---|---|---|
| `SUPERTABLE_MCP_TRANSPORT` | `stdio` | Transport: `stdio` or `streamable-http` |
| `SUPERTABLE_MCP_HTTP_HOST` | `0.0.0.0` | HTTP bind address |
| `SUPERTABLE_MCP_HTTP_PORT` | `8000` | HTTP listen port |
| `SUPERTABLE_MCP_HTTP_PATH` | `/mcp` | HTTP endpoint path |
| `SUPERTABLE_MCP_AUTH_TOKEN` | (empty) | Shared secret for tool authentication |
| `SUPERTABLE_REQUIRE_TOKEN` | `0` | Require auth_token on every tool call |
| `SUPERTABLE_REQUIRE_EXPLICIT_USER_HASH` | `0` | Require user_hash on every tool call |
| `SUPERTABLE_ALLOWED_USER_HASHES` | (empty) | Comma-separated allowed user hashes |
| `SUPERTABLE_DEFAULT_LIMIT` | `200` | Default row limit for query results |
| `SUPERTABLE_MAX_LIMIT` | `5000` | Maximum row limit |
| `SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC` | `60` | Query timeout (seconds) |
| `SUPERTABLE_MAX_CONCURRENCY` | `6` | Maximum concurrent tool executions |
| `SUPERTABLE_DEFAULT_ENGINE` | `AUTO` | Default SQL engine |

---

## Module structure

```
supertable/mcp/
  __init__.py            Package marker
  mcp_server.py          Tool definitions, transport config, auth, main entry (2,301 lines)
  mcp_client.py          CLI test client for MCP tools (709 lines)
  mcp_stdio_proxy.py     Subprocess management for stdio transport (155 lines)
  web_app.py             Browser-based MCP tester UI (497 lines)
  web_client.py          HTTP client wrapper for MCP JSON-RPC (239 lines)
  web_server.py          Standalone HTTP server for web tester (33 lines)
```

---

## Frequently asked questions

**Is the MCP server read-only?**
Yes for data queries. The server rejects DDL and DML SQL statements. However, it can write annotations, feedback, and application state via dedicated tools — these are stored as internal tables, not user data.

**Can I use the MCP server without Redis?**
No. The MCP server imports `DataReader`, `MetaReader`, and `RedisCatalog` directly. Redis must be reachable.

**What happens if a query times out?**
The query is cancelled and the tool returns an error. The timeout is per-tool-call, set by `SUPERTABLE_DEFAULT_QUERY_TIMEOUT_SEC`.

**Can I expose the MCP server on the internet?**
Yes, using streamable-HTTP transport behind TLS. Set `SUPERTABLE_REQUIRE_TOKEN=1`, use a strong `SUPERTABLE_MCP_AUTH_TOKEN`, and put it behind a reverse proxy with HTTPS. The `mcp-http` Docker profile includes Caddy for TLS.

**Which AI clients are compatible?**
Any MCP-compatible client: Claude Desktop, Claude Code, Cursor, ChatGPT (via plugins), and custom integrations using the MCP SDK.
