# Spark Worker — Interactive PySpark Execution

Standalone Apache Spark cluster with a FastAPI WebSocket server for interactive PySpark execution. Provides a browser-based interface to run Spark SQL and DataFrame operations against a local Spark cluster.

## Architecture

- **spark-master** (`spark-plug-master`) — Spark master node, manages cluster resources
- **spark-worker** (`spark-plug-worker`) — Spark worker node, executes tasks
- **spark-ws-server** — FastAPI WebSocket server that creates a SparkSession and executes user PySpark code

## Quick Start

```bash
# Start everything
docker compose up -d

# Rebuild after code changes
docker compose up -d --build
```

## Stopping

```bash
docker compose down
```

## Ports

| Port | Service |
|------|---------|
| 7078 | Spark master RPC (host) → 7077 (container) |
| 8010 | WebSocket server (`/ws/spark`) |
| 8082 | Spark master Web UI |

## Network

- `spark-network` — Internal communication between Spark master, worker, and ws-server
- `supertable-net` — Shared network, `spark-plug-master` and `spark-ws-server` are reachable by other services

## WebSocket Protocol

Connect to `ws://localhost:8010/ws/spark` and send JSON messages:

```json
// Start/connect to Spark cluster
{"action": "START"}

// Execute PySpark code
{"action": "EXECUTE", "code": "df = spark.createDataFrame([(1, 'a')], ['id', 'val'])\ndf.show()"}
```

Responses:

```json
{"status": "progress", "message": "Initializing Spark session (Attempt 1/3)..."}
{"status": "ready"}
{"status": "running"}
{"status": "result", "data": {"status": "success", "output": "..."}}
```

## Files

| File | Purpose |
|------|---------|
| `docker-compose.yml` | Orchestrates Spark cluster and WebSocket server |
| `Dockerfile.server` | Builds the ws-server image (Python + Java + PySpark) |
| `ws_server.py` | FastAPI WebSocket server for interactive Spark execution |
| `spark_manager.py` | SparkSession lifecycle management and code execution |
| `spark_interface.html` | Browser-based Spark terminal UI |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_WS_PORT` | `8010` | WebSocket server host port |
| `SPARK_MASTER_URL` | `spark://spark-plug-master:7077` | Spark master URL (auto-set in compose) |

## Running Alongside Spark Thrift

This cluster can run in parallel with `spark_thrift`. Port conflicts are avoided:

| | Spark Worker | Spark Thrift |
|---|---|---|
| Master RPC (host) | 7078 | 7077 |
| Master Web UI | 8082 | 8180 |
| Container names | `spark-plug-master` | `spark-thrift-master` |

## Browser UI

Open `spark_interface.html` in a browser for an interactive Spark terminal. It connects to `ws://localhost:8010/ws/spark`. Click "Start/Connect Cluster" first, then run PySpark commands.
