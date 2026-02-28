# Spark Thrift Server Setup Guide

## Overview

This sets up a Spark Thrift Server (HiveServer2) that Supertable can use
for executing large queries against parquet files on MinIO/S3.

**Architecture:**
```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Supertable  │────▶│ Spark Thrift │────▶│    MinIO      │
│  (PyHive)    │:10000  (HiveServer2) │     │  (S3 parquet) │
└──────────────┘     └──────┬───────┘     └──────────────┘
                            │
                     ┌──────▼───────┐
                     │ Spark Master │
                     │    :7077     │
                     └──────┬───────┘
                            │
                  ┌─────────┼─────────┐
                  ▼         ▼         ▼
            ┌─────────┐┌─────────┐┌─────────┐
            │ Worker 1 ││ Worker 2 ││ Worker N │
            └─────────┘└─────────┘└─────────┘
```

## Prerequisites

- Docker and Docker Compose
- Your MinIO instance running (typically on port 9000)
- Python with `pyhive[hive]` and `thrift` packages installed

```bash
pip install pyhive[hive] thrift
```

## Step 1: Create the Docker network

If your MinIO and Redis already run in Docker, they likely have a network.
Create the shared network if it doesn't exist:

```bash
docker network create supertable-net
```

If your MinIO/Redis containers are on a different network, either:
- Add them to `supertable-net`, or
- Change `supertable-net` in the compose file to match your existing network

## Step 2: Configure environment

Create a `.env` file next to the compose file (or export these):

```bash
# S3/MinIO credentials (must match your MinIO setup)
STORAGE_ACCESS_KEY=minioadmin
STORAGE_SECRET_KEY=minioadmin
STORAGE_ENDPOINT_URL=http://minio:9000
STORAGE_REGION=us-east-1
STORAGE_FORCE_PATH_STYLE=true
STORAGE_USE_SSL=false

# Port mapping (optional, defaults shown)
SPARK_THRIFT_PORT=10000
SPARK_MASTER_WEBUI_PORT=8180
SPARK_THRIFT_WEBUI_PORT=4040
```

**Important:** `STORAGE_ENDPOINT_URL` must use the Docker hostname for MinIO
(e.g., `http://minio:9000`), not `localhost`, because Spark runs inside Docker.

## Step 3: Start the cluster

```bash
# Start with 1 worker (default)
docker compose -f docker-compose.spark-thrift.yml up -d

# Or start with 3 workers for more parallelism
docker compose -f docker-compose.spark-thrift.yml up -d --scale spark-worker=3
```

## Step 4: Verify the cluster

1. **Spark Master UI**: http://localhost:8180
   - You should see the master and workers listed

2. **Spark Thrift UI**: http://localhost:4040
   - Shows active/completed queries

3. **Test connection with beeline** (optional):
```bash
docker exec -it spark-thrift /opt/bitnami/spark/bin/beeline \
  -u "jdbc:hive2://localhost:10000"
```

Then run:
```sql
SELECT 1;
```

## Step 5: Register the cluster in Supertable

```python
from supertable.redis_catalog import RedisCatalog

catalog = RedisCatalog()
catalog.register_spark_cluster("spark-local", {
    "name": "Local Spark Thrift",
    "thrift_host": "localhost",       # use "spark-thrift" if supertable is in Docker too
    "thrift_port": 10000,
    "min_bytes": 10_000_000_000,      # 10 GB — only use Spark for large jobs
    "max_bytes": 0,                   # 0 = unlimited
    "status": "active",
    "s3_enabled": True,
})

# Verify
print(catalog.list_spark_clusters())
```

## Step 6: Test with Supertable

```python
from supertable.data_reader import DataReader, engine

reader = DataReader(
    super_name="example",
    organization="my-org",
    query="SELECT * FROM my_table LIMIT 10"
)

# Force Spark engine
result, status, message = reader.execute(
    role_name="superuser",
    engine=engine.SPARK
)
print(result)

# Or use AUTO — will pick Spark for datasets > 10GB
result, status, message = reader.execute(
    role_name="superuser",
    engine=engine.AUTO
)
```

## Scaling

### Add more workers
```bash
docker compose -f docker-compose.spark-thrift.yml up -d --scale spark-worker=5
```

### Increase memory
Edit the compose file or override with environment:
```bash
SPARK_WORKER_MEMORY=4G docker compose -f docker-compose.spark-thrift.yml up -d
```

### Multiple clusters
You can run multiple compose files with different ports and register each:

```python
catalog.register_spark_cluster("spark-large", {
    "name": "Large Jobs Cluster",
    "thrift_host": "spark-thrift-large",
    "thrift_port": 10001,
    "min_bytes": 100_000_000_000,     # 100 GB+
    "max_bytes": 0,
    "status": "active",
})
```

Supertable's `select_spark_cluster()` picks the tightest fit automatically.

## Stopping

```bash
docker compose -f docker-compose.spark-thrift.yml down

# To also remove volumes (JARs cache + warehouse):
docker compose -f docker-compose.spark-thrift.yml down -v
```

## Troubleshooting

### "Connection refused" on port 10000
The Thrift Server takes 30-60 seconds to start. Check logs:
```bash
docker logs spark-thrift
```

### "ClassNotFoundException: S3AFileSystem"
The hadoop-aws JAR wasn't loaded. Check the init container:
```bash
docker logs <spark-jars-init-container-id>
```

### "AccessDenied" from MinIO
Verify your `STORAGE_ACCESS_KEY` and `STORAGE_SECRET_KEY` match MinIO's
root credentials, and that MinIO is reachable from the Spark network.

### Thrift Server crashes on startup
Check if the port is already in use:
```bash
lsof -i :10000
```

### Slow queries
- Add more workers: `--scale spark-worker=N`
- Increase memory in compose file
- Check Spark UI at http://localhost:4040 for bottlenecks
