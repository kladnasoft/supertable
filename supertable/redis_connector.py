from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import urlparse

from dotenv import load_dotenv

import redis
from redis.sentinel import MasterNotFoundError, Sentinel
from supertable.config.defaults import logger

load_dotenv()


@dataclass(frozen=True)
class RedisOptions:
    """
    Reads Redis connection options from environment variables.

    Supported:
      - SUPERTABLE_REDIS_URL (e.g. redis://:pass@host:6379/0 or rediss://:pass@host:6380/1)
      - or split vars: SUPERTABLE_REDIS_HOST, SUPERTABLE_REDIS_PORT, SUPERTABLE_REDIS_DB, SUPERTABLE_REDIS_PASSWORD

    Optional:
      - SUPERTABLE_REDIS_DECODE_RESPONSES (default: "false")

    Sentinel (optional):
      - SUPERTABLE_REDIS_SENTINEL (true/false)
      - SUPERTABLE_REDIS_SENTINELS="host1:26379,host2:26379"
      - SUPERTABLE_REDIS_SENTINEL_MASTER="mymaster"
      - SUPERTABLE_REDIS_SENTINEL_PASSWORD (optional; defaults to SUPERTABLE_REDIS_PASSWORD; also used as fallback for SUPERTABLE_REDIS_PASSWORD in Sentinel mode)
      - SUPERTABLE_REDIS_SENTINEL_STRICT (true/false, default: false)  # if true, do not fall back when Sentinel is unavailable
    """
    host: str = field(init=False)
    port: int = field(init=False)
    db: int = field(init=False)
    password: Optional[str] = field(init=False)
    use_ssl: bool = field(init=False)
    decode_responses: bool = field(default=False)

    # Sentinel-related
    is_sentinel: bool = field(init=False)
    sentinel_hosts: List[tuple] = field(init=False)
    sentinel_master: str = field(init=False)
    sentinel_password: Optional[str] = field(init=False)
    sentinel_strict: bool = field(init=False)

    def __post_init__(self):
        url = os.getenv("SUPERTABLE_REDIS_URL")
        if url:
            u = urlparse(url)
            host = u.hostname or "localhost"
            port = u.port or 6379
            # path like "/0"
            db = int((u.path or "/0").lstrip("/") or 0)
            password = u.password
            use_ssl = (u.scheme.lower() == "rediss")
        else:
            host = os.getenv("SUPERTABLE_REDIS_HOST", "localhost")
            port = int(os.getenv("SUPERTABLE_REDIS_PORT", "6379"))
            db = int(os.getenv("SUPERTABLE_REDIS_DB", "0"))
            password = os.getenv("SUPERTABLE_REDIS_PASSWORD")
            use_ssl = False

        decode = os.getenv("SUPERTABLE_REDIS_DECODE_RESPONSES", "false").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )

        # Sentinel mode configuration
        is_sentinel = os.getenv("SUPERTABLE_REDIS_SENTINEL", "false").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )
        sentinel_raw = os.getenv("SUPERTABLE_REDIS_SENTINELS", "").strip()
        sentinels: List[tuple] = []
        if sentinel_raw:
            for part in sentinel_raw.split(","):
                part = part.strip()
                if not part:
                    continue
                try:
                    h, p = part.split(":")
                    sentinels.append((h.strip(), int(p)))
                except ValueError:
                    logger.warning(f"[redis-options] Invalid sentinel spec: {part}")

        sentinel_master = os.getenv("SUPERTABLE_REDIS_SENTINEL_MASTER", "mymaster")

        sentinel_password = os.getenv("SUPERTABLE_REDIS_SENTINEL_PASSWORD") or password

        # In many deployments the Sentinel and Redis server share the same password.
        # Allow a single-password setup by reusing SUPERTABLE_REDIS_SENTINEL_PASSWORD
        # for Redis auth when SUPERTABLE_REDIS_PASSWORD is not set.
        if is_sentinel and password is None and sentinel_password:
            password = sentinel_password

        sentinel_strict = os.getenv("SUPERTABLE_REDIS_SENTINEL_STRICT", "false").strip().lower() in (
            "1",
            "true",
            "yes",
            "y",
            "on",
        )

        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "db", db)
        object.__setattr__(self, "password", password)
        object.__setattr__(self, "use_ssl", use_ssl)
        object.__setattr__(self, "decode_responses", decode)

        object.__setattr__(self, "is_sentinel", is_sentinel)
        object.__setattr__(self, "sentinel_hosts", sentinels)
        object.__setattr__(self, "sentinel_master", sentinel_master)
        object.__setattr__(self, "sentinel_password", sentinel_password)
        object.__setattr__(self, "sentinel_strict", sentinel_strict)


def create_redis_client(options: Optional[RedisOptions] = None) -> redis.Redis:
    opts = options or RedisOptions()
    # Response decoding:
    # If SUPERTABLE_REDIS_DECODE_RESPONSES=true, redis-py returns str instead of bytes.
    # Some older call sites may assume bytes; callers should handle both bytes and str.
    if opts.decode_responses:
        logger.debug("[redis-connector] SUPERTABLE_REDIS_DECODE_RESPONSES=true; redis responses will be decoded to str")
    decode_responses = bool(opts.decode_responses)

    # Decide between standard Redis and Sentinel-based Redis
    if opts.is_sentinel and opts.sentinel_hosts:
        logger.debug(
            f"[redis-catalog] Using Redis Sentinel mode. master={opts.sentinel_master}, "
            f"sentinels={opts.sentinel_hosts}"
        )
        sentinel = Sentinel(
            opts.sentinel_hosts,
            sentinel_kwargs={
                "socket_timeout": 0.5,
                "password": opts.sentinel_password,
                "decode_responses": decode_responses,
            },
            socket_timeout=0.5,
            password=opts.password,
            decode_responses=decode_responses,
        )

        sentinel_client = sentinel.master_for(
            opts.sentinel_master,
            db=opts.db,
            password=opts.password,
            decode_responses=decode_responses,
        )

        # Fail-fast probe: in some deployments (e.g. standalone Redis without Sentinel),
        # redis-py will only raise on first command. We probe here so we can provide a
        # clearer error (or fall back to direct Redis if configured to do so).
        sentinel_err: Optional[BaseException] = None
        deadline = time.time() + 3.0
        while time.time() < deadline:
            try:
                sentinel_client.ping()
                sentinel_err = None
                break
            except (MasterNotFoundError, redis.RedisError, OSError) as e:
                sentinel_err = e
                time.sleep(0.2)

        if sentinel_err is None:
            return sentinel_client

        if opts.sentinel_strict:
            logger.error(f"[redis-catalog] Sentinel unavailable (strict mode): {sentinel_err}")
            raise sentinel_err

        logger.warning(
            "[redis-catalog] Sentinel unavailable; falling back to standard Redis. "
            f"master={opts.sentinel_master}, sentinels={opts.sentinel_hosts}, "
            f"fallback={opts.host}:{opts.port}/{opts.db}. err={sentinel_err}"
        )
        r = redis.Redis(
            host=opts.host,
            port=opts.port,
            db=opts.db,
            password=opts.password,
            decode_responses=decode_responses,
            ssl=opts.use_ssl,
        )
        try:
            r.ping()
        except (redis.RedisError, OSError) as e:
            logger.error(f"[redis-catalog] Standard Redis fallback ping failed: {e}")
            # Both Sentinel and the configured direct Redis fallback are unavailable.
            # Raise a single actionable error that includes both root causes.
            raise redis.ConnectionError(
                "Redis Sentinel is enabled but unavailable, and the configured direct Redis "
                "fallback is also unreachable. "
                f"sentinels={opts.sentinel_hosts}, master={opts.sentinel_master}, "
                f"fallback={opts.host}:{opts.port}/{opts.db}. "
                f"sentinel_err={sentinel_err!r}. fallback_err={e!r}. "
                "If you are running the app in Docker, do not use localhost—use the Redis "
                "service name (e.g. redis-master) or set SUPERTABLE_REDIS_URL accordingly. "
                "Also ensure your Sentinel containers are actually running Redis Sentinel "
                "(Bitnami uses a separate redis-sentinel image)."
            ) from e
        return r

    if opts.is_sentinel and not opts.sentinel_hosts:
        logger.warning(
            "[redis-catalog] SUPERTABLE_REDIS_SENTINEL=true but no "
            "SUPERTABLE_REDIS_SENTINELS set. Falling back to standard Redis."
        )
    logger.info(
        f"[redis-catalog] Using standard Redis mode. host={opts.host}, port={opts.port}, db={opts.db}"
    )
    return redis.Redis(
        host=opts.host,
        port=opts.port,
        db=opts.db,
        password=opts.password,
        decode_responses=decode_responses,
        ssl=opts.use_ssl,
    )


class RedisConnector:
    """Creates and holds a Redis client connection based on RedisOptions."""

    def __init__(self, options: Optional[RedisOptions] = None):
        self.r = create_redis_client(options)
