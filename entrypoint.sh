#!/usr/bin/env sh
set -eu

# ---------------------------------------------------------------------------
# Default HOME for anonymous/non-root runtimes (e.g. random UID on k8s).
# ---------------------------------------------------------------------------
: "${HOME:=/home/supertable}"

# Expand "~/" in DUCKDB_EXTENSION_DIRECTORY (docker-compose env often uses this form).
if [ "${DUCKDB_EXTENSION_DIRECTORY:-}" != "" ]; then
  case "$DUCKDB_EXTENSION_DIRECTORY" in
    "~/"*) DUCKDB_EXTENSION_DIRECTORY="${HOME}/${DUCKDB_EXTENSION_DIRECTORY#\~/}" ;;
  esac
fi

: "${DUCKDB_EXTENSION_DIRECTORY:=${HOME}/.duckdb/extensions}"
export DUCKDB_EXTENSION_DIRECTORY

# Ensure baked runtime dirs exist even if HOME changes or a volume is mounted.
mkdir -p "${DUCKDB_EXTENSION_DIRECTORY}" "${HOME}/supertable"

HOST="${HOST:-0.0.0.0}"

# ---------------------------------------------------------------------------
# webui — Supertable admin UI + API proxy (supertable.webui.application)
#          Default port: 8050  (SUPERTABLE_UI_PORT)
# ---------------------------------------------------------------------------
_run_webui() {
  PORT="${SUPERTABLE_UI_PORT:-8050}"
  exec python -u -m supertable.webui.application
}

# ---------------------------------------------------------------------------
# api — Supertable REST API (supertable.api.application)
#        Default port: 8051  (SUPERTABLE_API_PORT)
# ---------------------------------------------------------------------------
_run_api() {
  PORT="${SUPERTABLE_API_PORT:-8051}"
  exec python -u -m supertable.api.application
}

# ---------------------------------------------------------------------------
# odata — Supertable OData 4.0 feed server (supertable.odata.application)
#          Default port: 8052  (SUPERTABLE_ODATA_PORT)
# ---------------------------------------------------------------------------
_run_odata() {
  PORT="${SUPERTABLE_ODATA_PORT:-8052}"
  exec python -u -m supertable.odata.application
}

# ---------------------------------------------------------------------------
# mcp — MCP stdio server (foreground) + HTTP + web tester (background)
# ---------------------------------------------------------------------------
_run_mcp_stdio() {
  exec python -u /app/supertable/mcp/mcp_server.py
}

_run_mcp_http_bg() {
  SUPERTABLE_MCP_TRANSPORT=streamable-http   SUPERTABLE_MCP_PORT="${SUPERTABLE_MCP_PORT:-8070}"   python -u /app/supertable/mcp/mcp_server.py >> /tmp/mcp_http.log 2>&1 &
  MCP_HTTP_PID=$!
  echo "MCP streamable-http server started (PID $MCP_HTTP_PID) on port ${SUPERTABLE_MCP_PORT:-8070}"
}

_start_mcp_web_bg() {
  PYTHONPATH=/app python -u /app/supertable/mcp/web_server.py &
}

# ---------------------------------------------------------------------------
# mcp-http — MCP server over streamable-http transport
# ---------------------------------------------------------------------------
_run_mcp_http() {
  export SUPERTABLE_MCP_TRANSPORT=streamable-http
  exec python -u /app/supertable/mcp/mcp_server.py
}

# ---------------------------------------------------------------------------
# SERVICE dispatch
# ---------------------------------------------------------------------------
SERVICE="${SERVICE:-webui}"

case "$SERVICE" in

  webui|reflection)
    # Supertable admin UI + API proxy  →  :8050
    # "reflection" accepted for backward compatibility
    _run_webui
    ;;

  api)
    # Supertable REST API  →  :8051
    _run_api
    ;;

  odata)
    # Supertable OData 4.0 feed server  →  :8052
    _run_odata
    ;;

  mcp)
    # Starts three processes:
    #   1. MCP streamable-http server  →  :8070
    #   2. MCP web tester UI           →  :8099
    #   3. MCP stdio server            →  foreground

    _run_mcp_http_bg
    HTTP_PID=$MCP_HTTP_PID

    sleep 3
    if ! kill -0 "$HTTP_PID" 2>/dev/null; then
      echo "ERROR: MCP http server failed to start. Log:" >&2
      cat /tmp/mcp_http.log >&2
      exit 1
    fi
    echo "MCP http server alive (PID $HTTP_PID)"

    _start_mcp_web_bg
    WEB_PID=$!

    sleep 3
    if ! kill -0 "$WEB_PID" 2>/dev/null; then
      echo "ERROR: MCP web UI failed to start." >&2
      exit 1
    fi
    echo "MCP web UI alive (PID $WEB_PID)"

    _run_mcp_stdio
    ;;

  mcp-http)
    # MCP server over streamable-http  →  :8000
    _run_mcp_http
    ;;

  *)
    echo "Unknown SERVICE=$SERVICE" >&2
    echo "Valid values: webui | api | odata | mcp | mcp-http" >&2
    exit 64
    ;;
esac
