# Spark Thrift — JDBC/ODBC SQL Server

Spark Thrift Server (HiveServer2) that Supertable uses for executing large SQL queries against parquet files stored on MinIO/S3. Connects via standard JDBC/ODBC protocol.

## Architecture

- **spark-thrift-master** — Spark master node
- **spark-thrift-worker-1** — Spark worker node (scalable)
- **spark-thrift-server** — HiveServer2 Thrift Server, accepts JDBC/ODBC connections on port 10000

## Quick Start

```bash
# Start with 1 worker (default)
docker compose -f docker-compose.spark-thrift.yml up -d

# Start with 3 workers for more parallelism
docker compose -f docker-compose.spark-thrift.yml up -d --scale spark-thrift-worker-1=3
```

## Stopping

```bash
docker compose -f docker-compose.spark-thrift.yml down

# Also remove volumes (warehouse + metastore)
docker compose -f docker-compose.spark-thrift.yml down -v
```

## Ports

| Port  | Service |
|-------|---------|
| 7077  | Spark master RPC |
| 8180  | Spark master Web UI |
| 10000 | Thrift JDBC/ODBC endpoint |
| 4040  | Spark Thrift Web UI (active queries) |

## Network

- `supertable-net` — All services join this network. The Thrift Server reaches MinIO at `http://minio:9000` via this network.

## Persistence

- `spark-warehouse` — Named volume for Hive warehouse and Derby metastore at `/opt/spark/warehouse`

## S3/MinIO Configuration

The Thrift Server connects to MinIO for reading parquet files. Configuration is passed via environment variables to avoid bash escaping issues with special characters in passwords.

| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_SPARK_ENDPOINT_URL` | `http://minio:9000` | S3 endpoint (use Docker hostname) |
| `STORAGE_ACCESS_KEY` | `minioadmin` | S3 access key |
| `STORAGE_SECRET_KEY` | `minioadmin123!` | S3 secret key |
| `STORAGE_REGION` | `eu-central-1` | S3 region |
| `STORAGE_FORCE_PATH_STYLE` | `true` | Use path-style S3 access (required for MinIO) |
| `STORAGE_USE_SSL` | `false` | Enable SSL for S3 |

## Port Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `SPARK_THRIFT_PORT` | `10000` | Thrift JDBC port |
| `SPARK_MASTER_WEBUI_PORT` | `8180` | Master Web UI port |
| `SPARK_THRIFT_WEBUI_PORT` | `4040` | Thrift Web UI port |
| `SPARK_WORKER_MEMORY` | `2G` | Worker memory |
| `SPARK_WORKER_CORES` | `2` | Worker CPU cores |

## Connecting from Supertable

```python
from supertable.redis_catalog import RedisCatalog

catalog = RedisCatalog()
catalog.register_spark_cluster("spark-local", {
    "name": "Local Spark Thrift",
    "thrift_host": "spark-thrift-server",  # Docker hostname on supertable-net
    "thrift_port": 10000,
    "min_bytes": 10_000_000_000,           # 10 GB threshold
    "max_bytes": 0,                        # unlimited
    "status": "active",
    "s3_enabled": True,
})
```

## Running Alongside Spark Worker

This cluster runs in parallel with `spark_worker`. No port conflicts — see the `spark_worker` README for the port mapping comparison.

## Scaling

```bash
# Add more workers
docker compose -f docker-compose.spark-thrift.yml up -d --scale spark-thrift-worker-1=5

# Increase worker memory
SPARK_WORKER_MEMORY=4G docker compose -f docker-compose.spark-thrift.yml up -d
```

## Troubleshooting

- **Connection refused on 10000** — Thrift Server takes 30–60s to start. Check `docker logs spark-thrift-server`.
- **AccessDenied from MinIO** — Verify `STORAGE_ACCESS_KEY` and `STORAGE_SECRET_KEY` match MinIO credentials.
- **Slow queries** — Add workers, increase memory, check Spark UI at `http://localhost:4040`.

For the full setup guide, see `SPARK_THRIFT_SETUP.md`.
