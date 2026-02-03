# Dockerfile_v20260201_2045_add-net-tools
FROM python:3.10-slim

# Install common notebook libraries and networking tools for debugging
RUN apt-get update && apt-get install -y --no-install-recommends \
    iputils-ping \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir requests

# Create a non-privileged user
RUN useradd -m sandbox
USER sandbox
WORKDIR /home/sandbox

ENV PYTHONUNBUFFERED=1