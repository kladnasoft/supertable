# syntax=docker/dockerfile:1.7
FROM python:3.11-slim

LABEL org.opencontainers.image.title="SuperTable"
LABEL org.opencontainers.image.description="Supertable REST (FastAPI) and MCP stdio server"
LABEL org.opencontainers.image.source="https://github.com/kladnasoft/supertable"

WORKDIR /app

# Minimal system deps
RUN apt-get update -o Acquire::Retries=3 \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Install Python deps first for layer caching
COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Default HOME for non-root (and arbitrary UID) runtimes
ENV HOME=/home/supertable

# Bake common runtime dirs + DuckDB httpfs extension (offline-friendly)
# - ${HOME}/.duckdb/extensions will contain the downloaded extension binary
# - ${HOME}/supertable is a convenience workspace under HOME
RUN mkdir -p "${HOME}/.duckdb/extensions" "${HOME}/supertable" \
 && chmod -R 0777 "${HOME}" "${HOME}/.duckdb" "${HOME}/supertable" \
 && DUCKDB_EXTENSION_DIRECTORY="${HOME}/.duckdb/extensions" python -c "import duckdb; con=duckdb.connect(); con.execute('INSTALL httpfs;'); con.execute('LOAD httpfs;'); con.close()" \
 && test -n "$(find "${HOME}/.duckdb/extensions" -type f -name 'httpfs.duckdb_extension' -print -quit)"

# Keep DuckDB extensions in the user-home cache by default
ENV DUCKDB_EXTENSION_DIRECTORY=/home/supertable/.duckdb/extensions

# Copy the codebase (includes supertable/reflection)
COPY supertable/ ./supertable/


# Entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Optional convenience wrappers
RUN printf '#!/bin/sh\nSERVICE=admin exec /entrypoint.sh\n' > /usr/local/bin/admin-server \
 && printf '#!/bin/sh\nSERVICE=mcp exec /entrypoint.sh\n'   > /usr/local/bin/mcp-server \
 && chmod +x /usr/local/bin/admin-server /usr/local/bin/mcp-server

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
