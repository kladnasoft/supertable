# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

LABEL org.opencontainers.image.title="SuperTable"
LABEL org.opencontainers.image.description="Supertable reflection, API, MCP, notebook, and Spark services"
LABEL org.opencontainers.image.source="https://github.com/kladnasoft/supertable"

# Unbuffered output from the very first RUN that invokes Python
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
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Bake common runtime dirs + DuckDB httpfs extension (offline-friendly).
# 0755 dirs — only the supertable user needs write access at runtime.
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
RUN printf '#!/bin/sh\nSERVICE=reflection exec /entrypoint.sh\n' > /usr/local/bin/reflection-server \
 && printf '#!/bin/sh\nSERVICE=api        exec /entrypoint.sh\n' > /usr/local/bin/api-server \
 && printf '#!/bin/sh\nSERVICE=mcp        exec /entrypoint.sh\n' > /usr/local/bin/mcp-server \
 && printf '#!/bin/sh\nSERVICE=mcp-http   exec /entrypoint.sh\n' > /usr/local/bin/mcp-http-server \
 && printf '#!/bin/sh\nSERVICE=notebook   exec /entrypoint.sh\n' > /usr/local/bin/notebook-server \
 && printf '#!/bin/sh\nSERVICE=spark      exec /entrypoint.sh\n' > /usr/local/bin/spark-server \
 && chmod +x \
      /usr/local/bin/reflection-server \
      /usr/local/bin/api-server \
      /usr/local/bin/mcp-server \
      /usr/local/bin/mcp-http-server \
      /usr/local/bin/notebook-server \
      /usr/local/bin/spark-server

# Expose all service ports
# 8050 = reflection  |  8090 = api  |  8099 = mcp web UI
# 8000 = mcp-http / notebook  |  8010 = spark
EXPOSE 8050 8090 8099 8000 8010

# Switch to non-root for all runtime operations
USER supertable

# Health-check probes the port that matches the running service.
# Covers all HTTP services; harmless for stdio (mcp).
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f "http://localhost:${SUPERTABLE_REFLECTION_PORT:-8050}/health" \
   || curl -f "http://localhost:${SUPERTABLE_API_PORT:-8090}/health" \
   || curl -f "http://localhost:8099/health" \
   || curl -f "http://localhost:${SUPERTABLE_NOTEBOOK_PORT:-8000}/health" \
   || curl -f "http://localhost:8010/health" \
   || exit 1

ENTRYPOINT ["/entrypoint.sh"]