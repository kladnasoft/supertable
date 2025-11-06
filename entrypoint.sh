#!/usr/bin/env sh
set -eu

SERVICE="${SERVICE:-admin}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

case "$SERVICE" in
  admin)
    # App is inside the package: supertable/rest/application.py -> supertable.rest.application:app
    exec uvicorn supertable.rest.application:app --host "$HOST" --port "$PORT"
    ;;
  mcp)
    exec python -u supertable/mcp_server.py
    ;;
  both)
    uvicorn supertable.rest.application:app --host "$HOST" --port "$PORT" &
    exec python -u supertable/mcp_server.py
    ;;
  *)
    echo "Unknown SERVICE=$SERVICE (use: admin|mcp|both)" >&2
    exit 64
    ;;
esac
