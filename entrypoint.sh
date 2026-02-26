#!/usr/bin/env sh
set -eu

# Default HOME for anonymous/non-root runtimes (e.g. random UID)
: "${HOME:=/home/supertable}"

# Expand "~/" in DUCKDB_EXTENSION_DIRECTORY (docker-compose env often uses this form).
if [ "${DUCKDB_EXTENSION_DIRECTORY:-}" != "" ]; then
  case "$DUCKDB_EXTENSION_DIRECTORY" in
    "~/"*) DUCKDB_EXTENSION_DIRECTORY="${HOME}/${DUCKDB_EXTENSION_DIRECTORY#~/}" ;;
  esac
fi

: "${DUCKDB_EXTENSION_DIRECTORY:=${HOME}/.duckdb/extensions}"
export DUCKDB_EXTENSION_DIRECTORY

# Ensure baked runtime dirs exist even if HOME changes or a volume is mounted.
mkdir -p "${DUCKDB_EXTENSION_DIRECTORY}" "${HOME}/supertable"

SERVICE="${SERVICE:-admin}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

_run_web() {
  # Prefer dedicated web_server.py if present; fall back to the existing FastAPI app.
  if [ -f "/app/web_server.py" ]; then
    exec python -u /app/web_server.py
  fi
  if [ -f "/app/supertable/web_server.py" ]; then
    exec python -u /app/supertable/web_server.py
  fi
  if [ -f "/app/supertable/reflection/web_server.py" ]; then
    exec python -u /app/supertable/reflection/web_server.py
  fi
  exec uvicorn supertable.reflection.application:app --host "$HOST" --port "$PORT"
}

_start_web_bg() {
  if [ -f "/app/web_server.py" ]; then
    python -u /app/web_server.py &
    return
  fi
  if [ -f "/app/supertable/web_server.py" ]; then
    python -u /app/supertable/web_server.py &
    return
  fi
  if [ -f "/app/supertable/reflection/web_server.py" ]; then
    python -u /app/supertable/reflection/web_server.py &
    return
  fi
  uvicorn supertable.reflection.application:app --host "$HOST" --port "$PORT" &
}

_run_mcp() {
  # Prefer dedicated mcp_server.py if present; fall back to the packaged server.
  if [ -f "/app/mcp_server.py" ]; then
    exec python -u /app/mcp_server.py
  fi
  if [ -f "/app/supertable/mcp_server.py" ]; then
    exec python -u /app/supertable/mcp_server.py
  fi
  if [ -f "/app/supertable/mcp/mcp_server.py" ]; then
    exec python -u /app/supertable/mcp/mcp_server.py
  fi
  exec python -u supertable/mcp/mcp_server.py
}

case "$SERVICE" in
  admin)
    # App is inside the package: supertable/reflection/application.py -> supertable.reflection.application:app
    exec uvicorn supertable.reflection.application:app --host "$HOST" --port "$PORT"
    ;;
  web)
    _run_web
    ;;
  mcp)
    _run_mcp
    ;;
  both)
    _start_web_bg
    _run_mcp
    ;;
  *)
    echo "Unknown SERVICE=$SERVICE (use: admin|web|mcp|both)" >&2
    exit 64
    ;;
esac
