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
# reflection — Supertable admin REST + UI (supertable.reflection.application)
#              Default port: 8050  (SUPERTABLE_REFLECTION_PORT)
# ---------------------------------------------------------------------------
_run_reflection() {
  PORT="${SUPERTABLE_REFLECTION_PORT:-8050}"
  exec python -u -m supertable.reflection.application
}

# ---------------------------------------------------------------------------
# api — Supertable REST API (supertable.api.application)
#        Default port: 8090  (SUPERTABLE_API_PORT)
# ---------------------------------------------------------------------------
_run_api_rest() {
  PORT="${SUPERTABLE_API_PORT:-8090}"
  exec python -u -m supertable.api.application
}

# ---------------------------------------------------------------------------
# mcp — MCP stdio server (foreground) + MCP web tester UI (background, :8099)
# ---------------------------------------------------------------------------
_run_mcp() {
  exec python -u /app/supertable/mcp/mcp_server.py
}

_start_mcp_web_bg() {
  python -u /app/supertable/mcp/web_server.py &
}

# ---------------------------------------------------------------------------
# mcp-http — MCP server over streamable-http transport (:8000)
# ---------------------------------------------------------------------------
_run_mcp_http() {
  export SUPERTABLE_MCP_TRANSPORT=streamable-http
  exec python -u /app/supertable/mcp/mcp_server.py
}

# ---------------------------------------------------------------------------
# notebook — Supertable notebook WebSocket server (supertable.notebook.ws_server)
#            Default port: 8000  (SUPERTABLE_NOTEBOOK_PORT)
# ---------------------------------------------------------------------------
_run_notebook() {
  exec python -u /app/supertable/notebook/ws_server.py
}

# ---------------------------------------------------------------------------
# spark — Spark plug WebSocket server (supertable.spark_plug.ws_server)
#         Default port: 8010  (hardcoded in ws_server.py)
# ---------------------------------------------------------------------------
_run_spark() {
  exec python -u /app/supertable/spark_plug/ws_server.py
}

# ---------------------------------------------------------------------------
# SERVICE dispatch
# ---------------------------------------------------------------------------
SERVICE="${SERVICE:-reflection}"

case "$SERVICE" in

  reflection)
    # Supertable admin reflection UI + REST API  →  :8050
    _run_reflection
    ;;

  api)
    # Supertable REST API  →  :8090
    _run_api_rest
    ;;

  mcp)
    # MCP stdio server (foreground) + web tester UI on :8099 (background).
    # If the web UI exits unexpectedly, the container shuts down.
    _start_mcp_web_bg
    WEB_PID=$!

    _watch_web() {
      wait "$WEB_PID" || true
      echo "ERROR: MCP web UI process (PID $WEB_PID) exited unexpectedly — shutting down." >&2
      kill -TERM 1 2>/dev/null || true
    }
    _watch_web &

    _run_mcp
    ;;

  mcp-http)
    # MCP server over streamable-http  →  :8000  (for remote/HTTP MCP clients)
    _run_mcp_http
    ;;

  notebook)
    # Supertable notebook WebSocket server  →  :${SUPERTABLE_NOTEBOOK_PORT:-8000}
    _run_notebook
    ;;

  spark)
    # Spark plug WebSocket server  →  :8010
    _run_spark
    ;;

  *)
    echo "Unknown SERVICE=$SERVICE" >&2
    echo "Valid values: reflection | api | mcp | mcp-http | notebook | spark" >&2
    exit 64
    ;;
esac