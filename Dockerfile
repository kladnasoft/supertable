# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

LABEL org.opencontainers.image.title="SuperTable"
LABEL org.opencontainers.image.description="Supertable admin (FastAPI) and MCP stdio server in one image"
LABEL org.opencontainers.image.source="https://github.com/kladnasoft/supertable"

WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY supertable/ ./supertable/
# Keep your original entrypoint behavior (SERVICE=admin|mcp|both)
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Convenience wrapper commands so you can: `docker run … mcp-server` or `admin-server`
RUN printf '%s\n' '#!/bin/sh' 'SERVICE=mcp exec /entrypoint.sh "$@"' > /usr/local/bin/mcp-server \ && printf '%s\n' '#!/bin/sh' 'SERVICE=admin exec /entrypoint.sh "$@"' > /usr/local/bin/admin-server \ && chmod +x /usr/local/bin/mcp-server /usr/local/bin/admin-server

# Optional: quick way to install redis-cli inside the image (commented; enable if you want it permanent)
# RUN apt-get update -o Acquire::Retries=3 \
#  && apt-get install -y --no-install-recommends redis-tools \
#  && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

# We rely on docker compose `init: true` for proper signal handling.
# Default command defers to entrypoint that looks at $SERVICE (admin|mcp|both)
CMD ["/entrypoint.sh"]
