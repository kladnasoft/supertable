# route: supertable.services.rate_limit
"""
Redis-backed sliding-window rate limiter for the API server.

Uses a single Redis key per client IP per minute bucket.  The key is
incremented on every request and auto-expires after 60 seconds.

When the limit is exceeded, returns 429 Too Many Requests with a
Retry-After header.

The limiter is opt-in: disabled by default.  Enable via:
    SUPERTABLE_API_RATE_LIMIT_ENABLED=true
    SUPERTABLE_API_RATE_LIMIT_RPM=300     # requests per minute

Excluded paths: /healthz (load balancers must not be rate-limited).

Compliance: Defence-in-depth against denial-of-service.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

# Paths that are never rate-limited
_EXCLUDED_PATHS = frozenset({"/healthz", "/health"})


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Per-IP sliding-window rate limiter backed by Redis INCR + EXPIRE.

    Thread-safety: relies on Redis atomic INCR.  No local state.
    """

    def __init__(self, app, *, rpm: int = 300, redis_client=None):
        super().__init__(app)
        self._rpm = max(1, rpm)
        self._redis = redis_client

    def _get_redis(self):
        """Lazy-resolve Redis client to avoid import-time dependency."""
        if self._redis is not None:
            return self._redis
        try:
            from supertable.server_common import redis_client
            self._redis = redis_client
            return self._redis
        except Exception:
            return None

    def _client_ip(self, request: Request) -> str:
        """Extract client IP, respecting X-Forwarded-For behind a proxy."""
        xff = request.headers.get("x-forwarded-for")
        if xff:
            return xff.split(",")[0].strip()
        client = getattr(request, "client", None)
        return getattr(client, "host", "unknown") if client else "unknown"

    async def dispatch(self, request: Request, call_next) -> Response:
        # Skip excluded paths
        if request.url.path in _EXCLUDED_PATHS:
            return await call_next(request)

        r = self._get_redis()
        if r is None:
            # Redis unavailable — fail open (allow request)
            return await call_next(request)

        client_ip = self._client_ip(request)
        minute_bucket = int(time.time()) // 60
        key = f"supertable:rate:{client_ip}:{minute_bucket}"

        try:
            current = r.incr(key)
            if current == 1:
                # First request in this bucket — set 60s expiry
                r.expire(key, 60)

            if current > self._rpm:
                logger.warning(
                    "[rate-limit] Client %s exceeded %d RPM (count=%d)",
                    client_ip, self._rpm, current,
                )
                return JSONResponse(
                    status_code=429,
                    content={
                        "detail": "Rate limit exceeded",
                        "retry_after_seconds": 60 - (int(time.time()) % 60),
                    },
                    headers={"Retry-After": str(60 - (int(time.time()) % 60))},
                )
        except Exception as e:
            # Redis error — fail open
            logger.debug("[rate-limit] Redis error (failing open): %s", e)

        return await call_next(request)
