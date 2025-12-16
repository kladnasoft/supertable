#!/usr/bin/env sh
set -eu

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
