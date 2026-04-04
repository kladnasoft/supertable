# route: supertable.service_registry
"""
Lightweight service registry — every running process announces itself to Redis.

Used by: API server, WebUI, MCP server, OData server, SDK (best-effort).
Read by: Monitoring Infra tab, cost calculation, capacity planning.

Design principles:
  - Zero coupling between services.  Each process imports this module
    independently and registers itself.  No service knows about any other.
  - Non-fatal.  Registration failure never crashes the calling process.
    The SDK passes fail_silent=True so it doesn't even log warnings.
  - Two modes: threaded (for MCP, SDK, any non-async process) and
    async (for FastAPI lifespan).  Same Redis key, same payload.

Redis key pattern:
    supertable:registry:{service_type}:{hostname}:{pid}

TTL: 30 s, refreshed every 15 s.  If a process crashes, the key
expires automatically after two missed heartbeats.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_TTL_S: int = 30
_INTERVAL_S: int = 15
_KEY_PREFIX: str = "supertable:registry"


def _default_identity() -> Dict[str, Any]:
    return {
        "instance_id": uuid.uuid4().hex[:12],
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "started_at": time.time(),
    }


def _registry_key(service_type: str, hostname: str, pid: int) -> str:
    return f"{_KEY_PREFIX}:{service_type}:{hostname}:{pid}"


# ---------------------------------------------------------------------------
# ServiceRegistry
# ---------------------------------------------------------------------------

class ServiceRegistry:
    """
    Announce this process to Redis so it appears in the Monitoring Infra tab.

    Usage (threaded — MCP, SDK, standalone scripts):

        from supertable.service_registry import ServiceRegistry

        reg = ServiceRegistry("mcp", metadata={"transport": "stdio"})
        reg.start()          # starts daemon thread, returns immediately
        ...
        reg.stop()           # clean deregister on shutdown

    Usage (async — FastAPI lifespan):

        reg = ServiceRegistry("api", metadata={"port": 8051})
        async with reg.lifespan():
            yield

    Usage (SDK — best-effort, silent):

        reg = ServiceRegistry("sdk", fail_silent=True)
        reg.start()          # if Redis is unreachable, silently no-ops
    """

    def __init__(
        self,
        service_type: str,
        *,
        metadata: Optional[Dict[str, Any]] = None,
        fail_silent: bool = False,
        version: str = "1.0.0",
    ):
        ident = _default_identity()
        self.service_type = service_type
        self.instance_id = ident["instance_id"]
        self.hostname = ident["hostname"]
        self.pid = ident["pid"]
        self.started_at = ident["started_at"]
        self.version = version
        self.fail_silent = fail_silent
        self._key = _registry_key(service_type, self.hostname, self.pid)

        payload = {
            "instance_id": self.instance_id,
            "hostname": self.hostname,
            "pid": self.pid,
            "started_at": self.started_at,
            "service_type": service_type,
            "version": version,
        }
        if metadata:
            payload.update(metadata)
        self._payload = json.dumps(payload)

        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._rc: Any = None  # lazy Redis client

    # ---- Redis (lazy, reconnect-safe) ----

    def _redis(self) -> Any:
        if self._rc is None:
            from supertable.redis_catalog import RedisCatalog
            self._rc = RedisCatalog().r
        return self._rc

    def _ping(self) -> None:
        """Single heartbeat write.  Raises on failure."""
        self._redis().set(self._key, self._payload, ex=_TTL_S)

    # ---- Threaded mode ----

    def start(self) -> None:
        """Start background heartbeat thread.  Non-blocking, non-fatal."""
        try:
            self._ping()  # register immediately
        except Exception as exc:
            if self.fail_silent:
                return
            logger.warning("[registry] initial registration failed for %s: %s", self.service_type, exc)
            self._rc = None

        self._thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"registry-{self.service_type}",
        )
        self._thread.start()
        if not self.fail_silent:
            logger.info(
                "[registry] %s registered: id=%s host=%s pid=%d",
                self.service_type, self.instance_id, self.hostname, self.pid,
            )

    def stop(self) -> None:
        """Stop heartbeat and deregister key.  Non-fatal."""
        self._stop_event.set()
        try:
            self._redis().delete(self._key)
            if not self.fail_silent:
                logger.info("[registry] %s deregistered: %s", self.service_type, self._key)
        except Exception:
            pass

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            self._stop_event.wait(_INTERVAL_S)
            if self._stop_event.is_set():
                break
            try:
                self._ping()
            except Exception as exc:
                if not self.fail_silent:
                    logger.warning("[registry] heartbeat failed for %s: %s", self.service_type, exc)
                self._rc = None  # force reconnect next cycle

    # ---- Async mode (FastAPI) ----

    async def astart(self) -> None:
        """Register immediately (async).  Non-fatal."""
        try:
            self._ping()
            logger.info(
                "[registry] %s registered: id=%s host=%s pid=%d",
                self.service_type, self.instance_id, self.hostname, self.pid,
            )
        except Exception as exc:
            logger.warning("[registry] initial registration failed for %s: %s", self.service_type, exc)
            self._rc = None

    async def aheartbeat(self) -> None:
        """Async heartbeat loop — run as an asyncio.create_task."""
        while True:
            try:
                self._ping()
            except Exception as exc:
                logger.warning("[registry] heartbeat failed for %s: %s", self.service_type, exc)
                self._rc = None
            await asyncio.sleep(_INTERVAL_S)

    def astop(self) -> None:
        """Deregister (sync, called from finally blocks).  Non-fatal."""
        self.stop()

    # ---- Class methods for reading ----

    @staticmethod
    def scan(redis_client: Any) -> List[Dict[str, Any]]:
        """
        Read all registered services from Redis.

        Returns a list of instance dicts with ttl_ms, alive, last_seen_ms
        computed from the key's remaining TTL.  Used by the monitoring
        instances endpoint.
        """
        pattern = f"{_KEY_PREFIX}:*"
        instances: List[Dict[str, Any]] = []
        now_ms = int(time.time() * 1000)

        try:
            cursor = 0
            while True:
                cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=200)
                for key in keys:
                    try:
                        k = key if isinstance(key, str) else key.decode("utf-8")
                        raw = redis_client.get(k)
                        if not raw:
                            continue
                        s = raw if isinstance(raw, str) else raw.decode("utf-8")
                        data = json.loads(s)
                        ttl_ms = int(redis_client.pttl(k) or -1)
                        data["ttl_ms"] = ttl_ms
                        data["alive"] = ttl_ms > 0
                        data["last_seen_ms"] = now_ms - (_TTL_S * 1000 - max(0, ttl_ms))
                        # Infer service_type from key if missing in payload
                        if "service_type" not in data:
                            parts = k.split(":")
                            if len(parts) >= 3:
                                data["service_type"] = parts[2]
                        instances.append(data)
                    except Exception:
                        pass
                if cursor == 0:
                    break
        except Exception as exc:
            logger.warning("[registry] scan failed: %s", exc)

        instances.sort(key=lambda x: (x.get("service_type", ""), x.get("hostname", "")))
        return instances
