# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

LABEL org.opencontainers.image.title="SuperTable"
LABEL org.opencontainers.image.description="Supertable WebUI, API, OData, and MCP services"
LABEL org.opencontainers.image.source="https://github.com/kladnasoft/supertable"

ENV PYTHONUNBUFFERED=1
ENV HOME=/home/supertable
ENV DUCKDB_EXTENSION_DIRECTORY=/home/supertable/.duckdb/extensions

WORKDIR /app

# Minimal system deps
RUN apt-get update -o Acquire::Retries=3 \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Create a non-root user that will own all runtime dirs
RUN groupadd --gid 1001 supertable \
 && useradd --uid 1001 --gid 1001 --home "${HOME}" --shell /sbin/nologin --no-create-home supertable

# Install Python deps first for layer caching
COPY requirements-docker.txt ./requirements-docker.txt
RUN pip install --no-cache-dir -r requirements-docker.txt

# Bake common runtime dirs + DuckDB httpfs extension (offline-friendly).
RUN mkdir -p "${HOME}/.duckdb/extensions" "${HOME}/supertable" \
 && DUCKDB_EXTENSION_DIRECTORY="${HOME}/.duckdb/extensions" \
    python -c "import duckdb; con=duckdb.connect(); con.execute('INSTALL httpfs;'); con.execute('LOAD httpfs;'); con.close()" \
 && test -n "$(find "${HOME}/.duckdb/extensions" -type f -name 'httpfs.duckdb_extension' -print -quit)" \
 && chown -R 1001:1001 "${HOME}" \
 && chmod -R 0755 "${HOME}"

# Copy the codebase
COPY supertable/ ./supertable/

# Entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Convenience wrappers — one per SERVICE value
RUN printf '#!/bin/sh\nSERVICE=webui    exec /entrypoint.sh\n' > /usr/local/bin/webui-server \
 && printf '#!/bin/sh\nSERVICE=api      exec /entrypoint.sh\n' > /usr/local/bin/api-server \
 && printf '#!/bin/sh\nSERVICE=odata    exec /entrypoint.sh\n' > /usr/local/bin/odata-server \
 && printf '#!/bin/sh\nSERVICE=mcp      exec /entrypoint.sh\n' > /usr/local/bin/mcp-server \
 && printf '#!/bin/sh\nSERVICE=mcp-http exec /entrypoint.sh\n' > /usr/local/bin/mcp-http-server \
 && chmod +x \
      /usr/local/bin/webui-server \
      /usr/local/bin/api-server \
      /usr/local/bin/odata-server \
      /usr/local/bin/mcp-server \
      /usr/local/bin/mcp-http-server

# Expose all service ports
# 8050 = WebUI  |  8051 = API  |  8052 = OData
# 8070 = MCP streamable-http  |  8099 = MCP web tester
EXPOSE 8050 8051 8052 8070 8099

# Switch to non-root
USER supertable

# Health check — probes /healthz on whichever port the running service uses
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -sf "http://localhost:${SUPERTABLE_UI_PORT:-8050}/healthz" \
   || curl -sf "http://localhost:${SUPERTABLE_API_PORT:-8051}/healthz" \
   || curl -sf "http://localhost:${SUPERTABLE_ODATA_PORT:-8052}/healthz" \
   || curl -sf "http://localhost:8099/healthz" \
   || exit 1

ENTRYPOINT ["/entrypoint.sh"]
