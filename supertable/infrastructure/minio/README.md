# MinIO — S3-Compatible Object Storage

Distributed MinIO cluster providing S3-compatible object storage for Supertable. Used for storing parquet files, staging data, and any blob storage needs.

## Architecture

- **minio1** — Primary node, exposes S3 API (`:9000`) and web console (`:9001`), aliased as `minio` on `supertable-net`
- **minio2** — Second node, always runs with minio1
- **minio3, minio4** — Optional nodes for full erasure coding HA (profile: `all-nodes`)
- **setup** — One-shot init container that runs `wait-for-minio.sh` and `setup-keys.sh` from `./scripts/`

## Quick Start

```bash
# 2-node cluster (default)
docker compose up -d

# 4-node cluster with erasure coding
MINIO_NODES=4 docker compose --profile all-nodes up -d
```

## Stopping

```bash
docker compose down

# 4-node
docker compose --profile all-nodes down
```

## Ports

| Port | Service |
|------|---------|
| 9000 | S3 API |
| 9001 | MinIO Console (Web UI) |

## Network

- `minio-cluster` — Internal cluster communication between MinIO nodes
- `supertable-net` — Shared network, minio1 is reachable as `minio` by other services

## Persistence

All data and config use Docker named volumes (`minio1-data`, `minio1-config`, etc.), managed under `/var/lib/docker/volumes/`. Data survives container recreation and `docker compose down`. To wipe data:

```bash
docker compose down -v
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MINIO_NODES` | `2` | Number of cluster nodes (used in server command) |
| `MINIO_ROOT_USER` | `minioadmin` | Root access key (set in compose) |
| `MINIO_ROOT_PASSWORD` | `minioadmin123!` | Root secret key (set in compose) |
| `MINIO_REGION` | `eu-central-1` | S3 region |

## Scripts

Place setup scripts in `./scripts/`:
- `wait-for-minio.sh` — Waits for the cluster to be healthy
- `setup-keys.sh` — Creates buckets, policies, and access keys

## Connecting from Other Services

Other services on `supertable-net` can reach MinIO at `http://minio:9000`. From the host, use `http://localhost:9000`.
