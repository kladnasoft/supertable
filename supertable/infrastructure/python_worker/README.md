# Python Worker — Sandboxed Code Execution

FastAPI WebSocket server that executes Python code in isolated Docker containers. Supports both stateless one-shot execution and stateful notebook-like sessions where variables persist across cell runs.

## Architecture

- **python-worker** — FastAPI server (`ws_server.py`) that manages sandbox containers via the Docker API
- **code-runner-slim** — Lightweight Python sandbox image (built from `Dockerfile`), used as throwaway containers for code execution

The server spawns short-lived `code-runner-slim` containers for each execution. A warm pool keeps pre-created containers ready for fast startup.

## Quick Start

```bash
# First time — build the sandbox image
docker compose --profile build up --build code-runner-slim

# Start the server
docker compose up -d

# Rebuild server after code changes
docker compose up -d --build
```

## Stopping

```bash
docker compose down
```

## Ports

| Port | Service |
|------|---------|
| 8010 | WebSocket server (`/ws/execute`) |

## Network

- `supertable-net` — The server joins this network. When using the `internet` profile (HIGH_TIER), sandbox containers also join `supertable-net` and can reach MinIO, Redis, and external APIs.

## Execution Profiles

| Profile | Memory | CPU | Network | Use Case |
|---------|--------|-----|---------|----------|
| `no-internet` (default) | 128 MB | 20% | Disabled | Safe sandboxed execution |
| `internet` | 1 GB | 200% | `supertable-net` | Access to external APIs, MinIO, Redis |

## WebSocket Protocol

Connect to `ws://localhost:8010/ws/execute` and send JSON messages:

```json
// Execute code
{"code": "print('hello')", "profile": "no-internet"}

// Execute in a persistent session
{"code": "x = 42", "session_id": "my-notebook", "profile": "no-internet"}

// Reset a session
{"op": "reset", "session_id": "my-notebook"}
```

Responses:

```json
{"status": "start", "message": "Executing...", "profile": "no-internet", "session_id": "..."}
{"status": "output", "data": "hello\n"}
{"status": "complete", "session_id": "..."}
```

## Files

| File | Purpose |
|------|---------|
| `Dockerfile` | Builds `code-runner-slim` sandbox image |
| `Dockerfile.server` | Builds the FastAPI server image |
| `docker-compose.yml` | Orchestrates the server and sandbox image build |
| `ws_server.py` | FastAPI WebSocket server, manages pools and sessions |
| `warm_pool_manager.py` | Warm pool of pre-created containers + stateful session management |
| `worker.py` | Simple synchronous execution worker (used by `execute.py`) |
| `resource_config.py` | Resource limits and tier definitions (LOW_TIER, HIGH_TIER) |
| `execute.py` | CLI test script for cold worker and warm pool |
| `index.html` | Browser-based notebook UI |
| `frontend.js` | Minimal WebSocket client for testing |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SUPERTABLE_NOTEBOOK_PORT` | `8010` | Server port |
| `SUPERTABLE_NOTEBOOK_STATEFUL_WS` | `1` | Enable stateful sessions (`0` for legacy one-shot mode) |
| `SUPERTABLE_NOTEBOOK_SESSION_TTL_SECONDS` | `1800` | Idle session timeout (seconds) before container cleanup |

## Security Notes

- Sandbox containers run with `no-new-privileges` security option
- `no-internet` containers have networking completely disabled
- The server container mounts Docker socket read-only (`/var/run/docker.sock:ro`) — this grants privileged access to Docker and should be hardened in production
- Idle sessions are automatically reaped after the TTL expires
- Orphaned containers are cleaned up by a background maintenance thread
