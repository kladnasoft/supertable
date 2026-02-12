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

case "$SERVICE" in
  admin)
    # App is inside the package: supertable/reflection/application.py -> supertable.reflection.application:app
    exec uvicorn supertable.reflection.application:app --host "$HOST" --port "$PORT"
    ;;
  mcp)
    exec python -u supertable/mcp/mcp_server.py
    ;;
  both)
    uvicorn supertable.reflection.application:app --host "$HOST" --port "$PORT" &
    exec python -u supertable/mcp/mcp_server.py
    ;;
  *)
    echo "Unknown SERVICE=$SERVICE (use: admin|mcp|both)" >&2
    exit 64
    ;;
esac
