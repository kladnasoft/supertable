# Redis — Sentinel HA Cluster

Redis master-replica setup with optional Sentinel for automatic failover, used by Supertable as the catalog store, locking backend, and caching layer.

## Architecture

- **redis-master** — Primary Redis instance, accepts reads and writes
- **redis-replica-1** — Read replica, replicates from master
- **redis-sentinel-1, 2, 3** — Sentinel nodes for automatic failover (profile: `sentinel`), quorum of 2
- **redisinsight** — Web UI for inspecting Redis data (profile: `insights`)

## Quick Start

```bash
# Redis only (master + replica)
docker compose up -d

# Redis + Sentinel HA
docker compose --profile sentinel up -d

# Redis + RedisInsight UI
docker compose --profile insights up -d

# Redis + Sentinel + RedisInsight
docker compose --profile sentinel --profile insights up -d
```

## Stopping

```bash
docker compose down

# With profiles
docker compose --profile sentinel --profile insights down
```

## Ports

| Port  | Service |
|-------|---------|
| 6379  | Redis master |
| 26379 | Sentinel 1 |
| 26380 | Sentinel 2 |
| 26381 | Sentinel 3 |
| 5540  | RedisInsight Web UI |

## Network

- `redis-net` — Internal communication between Redis nodes, sentinels, and RedisInsight
- `supertable-net` — Shared network, `redis-master` and `redis-replica-1` are reachable by other services

## Persistence

Redis master and replica use Docker named volumes mounted at `/bitnami/redis/data`. The Bitnami image writes RDB snapshots and AOF logs here by default. Data survives container recreation. To wipe:

```bash
docker compose down -v
```

## Sentinel Failover

When the `sentinel` profile is active, three Sentinel nodes monitor the master. If the master goes down, Sentinels promote the replica to master (quorum: 2 out of 3 must agree). When the old master container restarts, Sentinel automatically reconfigures it as a replica of the new master.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_PASSWORD` | `change_me_to_a_strong_password` | Password for Redis and Sentinel authentication |

## Connecting from Other Services

Services on `supertable-net` can connect to `redis-master:6379`. When using Sentinel, connect to sentinels and ask for `mymaster`:

```python
from redis.sentinel import Sentinel

sentinel = Sentinel(
    [("redis-sentinel-1", 26379), ("redis-sentinel-2", 26379), ("redis-sentinel-3", 26379)],
    password="change_me_to_a_strong_password",
    sentinel_kwargs={"password": "change_me_to_a_strong_password"},
)
master = sentinel.master_for("mymaster", db=1)
```

From the host, use `localhost:6379`.

## RedisInsight

When started with `--profile insights`, RedisInsight is available at `http://localhost:5540`. The pre-configured database connection is loaded from `redisinsight_databases.json`.
