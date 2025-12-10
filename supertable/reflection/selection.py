# selection.py
from __future__ import annotations

import os
from typing import List, Tuple, Optional, Dict, Any, Set
from urllib.parse import urlparse

import redis


# ------------------------------ Settings (Redis) ------------------------------

class Settings:
    """
    Minimal settings needed for SuperTable selection:
    - Redis connection info (SUPERTABLE_* envs)
    """

    def __init__(self) -> None:
        # Full URL, if provided (takes precedence)
        self.SUPERTABLE_REDIS_URL: Optional[str] = os.getenv("SUPERTABLE_REDIS_URL")

        # Host/port/db/password fallback
        self.SUPERTABLE_REDIS_HOST: str = os.getenv("SUPERTABLE_REDIS_HOST", "localhost")
        self.SUPERTABLE_REDIS_PORT: int = int(os.getenv("SUPERTABLE_REDIS_PORT", "6379"))
        self.SUPERTABLE_REDIS_DB: int = int(os.getenv("SUPERTABLE_REDIS_DB", "0"))
        self.SUPERTABLE_REDIS_PASSWORD: Optional[str] = os.getenv("SUPERTABLE_REDIS_PASSWORD")
        self.SUPERTABLE_REDIS_USERNAME: Optional[str] = os.getenv("SUPERTABLE_REDIS_USERNAME")


settings = Settings()


def _coerce_password(pw: Optional[str]) -> Optional[str]:
    """
    Normalize password env so values like "None"/"null" behave as no password.
    """
    if pw is None:
        return None
    v = pw.strip()
    if v in ("", "None", "none", "null", "NULL"):
        return None
    return v


def _build_redis_client() -> redis.Redis:
    """
    Build a Redis client from SUPERTABLE_* envs.

    Precedence:
      1) SUPERTABLE_REDIS_URL (parsed)
      2) SUPERTABLE_REDIS_HOST/PORT/DB/PASSWORD (override URL parts if provided)
    """
    url = (settings.SUPERTABLE_REDIS_URL or "").strip() or None

    host = settings.SUPERTABLE_REDIS_HOST
    port = settings.SUPERTABLE_REDIS_PORT
    db = settings.SUPERTABLE_REDIS_DB
    username = (settings.SUPERTABLE_REDIS_USERNAME or "").strip() or None
    password = _coerce_password(settings.SUPERTABLE_REDIS_PASSWORD)

    if url:
        u = urlparse(url)
        if u.scheme not in ("redis", "rediss"):
            raise ValueError(f"Unsupported Redis URL scheme: {u.scheme}")
        if u.hostname:
            host = u.hostname
        if u.port:
            port = u.port
        if u.path and len(u.path) > 1:
            try:
                db_from_url = int(u.path.lstrip("/"))
                db = db_from_url
            except Exception:
                pass
        if u.username:
            username = u.username
        if u.password:
            password = _coerce_password(u.password)

    if username and not password:
        # ACL requires both; if only username is set, drop it
        username = None

    return redis.Redis(
        host=host,
        port=port,
        db=db,
        password=password,
        username=username,
        decode_responses=True,
        ssl=url.startswith("rediss://") if url else False,
    )


redis_client = _build_redis_client()


# ------------------------------ Core selection helpers ------------------------------

def discover_pairs(limit_pairs: int = 10000) -> List[Tuple[str, str]]:
    """
    Scan Redis for SuperTable tenant pairs (org, sup).

    Matches keys of the form:
      supertable:{org}:{sup}:meta:...

    Returns a sorted list of (org, sup) tuples.
    """
    pairs: Set[Tuple[str, str]] = set()
    cursor = 0
    pattern = "supertable:*:*:meta:*"

    while True:
        cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
        for k in keys:
            s = k if isinstance(k, str) else k.decode("utf-8")
            parts = s.split(":")
            if len(parts) >= 5 and parts[0] == "supertable" and parts[3] == "meta":
                pairs.add((parts[1], parts[2]))
                if len(pairs) >= limit_pairs:
                    break
        if cursor == 0 or len(pairs) >= limit_pairs:
            break

    return sorted(pairs)


def resolve_pair(org: Optional[str], sup: Optional[str]) -> Tuple[str, str]:
    """
    Resolve an (org, sup) pair using optional query params, falling back to:
    - first matching org or sup in Redis
    - or the first discovered pair
    - or ("", "") if none exist.
    """
    pairs = discover_pairs()

    if org and sup:
        return org, sup

    if org and not sup:
        for o, s in pairs:
            if o == org:
                return o, s

    if sup and not org:
        for o, s in pairs:
            if s == sup:
                return o, s

    if not pairs:
        return "", ""

    return pairs[0]


def build_selection_context(
    org: Optional[str] = None,
    sup: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build the exact context structure that selection.html expects on the backend side.

    Returns:
      {
        "tenants": [
          {"org": "...", "sup": "...", "selected": bool},
          ...
        ],
        "sel_org": "...",
        "sel_sup": "...",
        "has_tenant": bool,
      }
    """
    pairs = discover_pairs()
    sel_org, sel_sup = resolve_pair(org, sup)

    tenants = [
        {"org": o, "sup": s, "selected": (o == sel_org and s == sel_sup)}
        for (o, s) in pairs
    ]

    has_tenant = bool(sel_org and sel_sup)

    return {
        "tenants": tenants,
        "sel_org": sel_org,
        "sel_sup": sel_sup,
        "has_tenant": has_tenant,
    }
