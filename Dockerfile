FROM python:3.12-slim AS base

ARG SUPERTABLE_VERSION=""
ARG SUPERTABLE_EXTRAS=""

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SUPERTABLE_ENV_FILE=/config/.env

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

SHELL ["/bin/bash","-o","pipefail","-c"]

# Install SuperTable with optional extras and optional pinned version
RUN set -eux; \
    PACKAGE="supertable"; \
    if [[ -n "$SUPERTABLE_EXTRAS" ]]; then PACKAGE="supertable[$SUPERTABLE_EXTRAS]"; fi; \
    if [[ -n "$SUPERTABLE_VERSION" ]]; then PACKAGE="${PACKAGE}==${SUPERTABLE_VERSION}"; fi; \
    python -m pip install --upgrade pip; \
    pip install "$PACKAGE"

# Non-root user
RUN useradd -m -u 10001 appuser
USER appuser

# Config directory and entrypoint
RUN mkdir -p /config
COPY --chown=appuser:appuser docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Default entrypoint runs 'supertable config' if AUTO_CONFIG=true (default), then execs CMD
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["bash"]
