from __future__ import annotations

import os
import json
from typing import Dict, List, Optional, Tuple, Set
from pathlib import Path
from urllib.parse import urlparse

import redis
from fastapi import APIRouter, Query

# ------------------------------ Settings ------------------------------


class Settings:
    def __init__(self) -> None:
        # SUPERTABLE_* — connection to Redis
        self.SUPERTABLE_REDIS_URL: Optional[str] = os.getenv("SUPERTABLE_REDIS_URL")

        self.SUPERTABLE_REDIS_HOST: str = os.getenv("SUPERTABLE_REDIS_HOST", "localhost")
        self.SUPERTABLE_REDIS_PORT: int = int(os.getenv("SUPERTABLE_REDIS_PORT", "6379"))
        self.SUPERTABLE_REDIS_DB: int = int(os.getenv("SUPERTABLE_REDIS_DB", "0"))
        self.SUPERTABLE_REDIS_PASSWORD: Optional[str] = os.getenv("SUPERTABLE_REDIS_PASSWORD")
        self.SUPERTABLE_REDIS_USERNAME: Optional[str] = os.getenv("SUPERTABLE_REDIS_USERNAME")


settings = Settings()


def _now_ms() -> int:
    from time import time as _t
    return int(_t() * 1000)


# ------------------------------ Catalog (minimal for selection) ------------------------------


def _root_key(org: str, sup: str) -> str:
    return f"supertable:{org}:{sup}:meta:root"


class _FallbackCatalog:
    """
    Minimal fallback catalog implementation providing just what the
    selection widget needs: ensure_root + get_root.
    """

    def __init__(self, r: redis.Redis):
        self.r = r

    def ensure_root(self, org: str, sup: str) -> None:
        key = _root_key(org, sup)
        if not self.r.exists(key):
            self.r.set(key, json.dumps({"version": 0, "ts": _now_ms()}))

    def get_root(self, org: str, sup: str) -> Optional[Dict]:
        raw = self.r.get(_root_key(org, sup))
        return json.loads(raw) if raw else None


def _coerce_password(pw: Optional[str]) -> Optional[str]:
    if pw is None:
        return None
    v = pw.strip()
    # Treat these as "no password"
    if v in ("", "None", "none", "null", "NULL"):
        return None
    return v


def _build_redis_client() -> redis.Redis:
    """
    Build a Redis client from SUPERTABLE_* envs.
    Precedence:
      1) SUPERTABLE_REDIS_URL (parsed)
      2) SUPERTABLE_REDIS_HOST/PORT/DB/PASSWORD (overrides URL parts if provided)
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
        # Extract from URL
        if u.hostname:
            host = u.hostname
        if u.port:
            port = u.port
        # db from path: "/0", "/1", ...
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

    # If username is set but password is None, drop username (ACL requires both)
    if username and not password:
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


def _build_catalog() -> tuple[object, redis.Redis]:
    """
    Prefer the real RedisCatalog if available, otherwise fall back to a
    minimal implementation that still supports selection needs.
    """
    r = _build_redis_client()
    try:
        from supertable.redis_catalog import RedisCatalog as _RC  # type: ignore
        return _RC(), r
    except Exception:
        try:
            from redis_catalog import RedisCatalog as _RC  # type: ignore
            return _RC(), r
        except Exception:
            return _FallbackCatalog(r), r


catalog, redis_client = _build_catalog()


# ------------------------------ Discovery helpers ------------------------------


def discover_pairs(limit_pairs: int = 10000) -> List[Tuple[str, str]]:
    """
    Scan Redis for existing (org, sup) pairs.
    This is used by the selection widget to populate the SuperTable dropdown.
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
    Resolve a (org, sup) pair, allowing the client to omit one side.
    This mirrors the behavior in admin_app.py so the same UI logic works.
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


# ------------------------------ Router + minimal JSON API ------------------------------

router = APIRouter()


@router.get("/api/tenants")
def api_tenants():
    """
    List all discovered (org, sup) pairs as tenants.
    Intended for populating the <select id="pairSel"> in selection.html.
    """
    pairs = discover_pairs()
    return {"tenants": [{"org": o, "sup": s} for o, s in pairs]}


@router.get("/api/root")
def api_get_root(org: Optional[str] = Query(None), sup: Optional[str] = Query(None)):
    """
    Return the 'root' metadata for a given (org, sup) SuperTable.

    The selection widget can use this to show aggregated metrics like:
    - number of files
    - row counts
    - total byte size
    - last updated timestamp

    Exact fields depend on how your catalog stores them in Redis.
    """
    org, sup = resolve_pair(org, sup)
    if not org or not sup:
        return {"org": org, "sup": sup, "root": None}

    if hasattr(catalog, "ensure_root"):
        try:
            catalog.ensure_root(org, sup)
        except Exception:
            # Best-effort; still attempt to read root
            pass

    try:
        root = catalog.get_root(org, sup)
    except Exception:
        root = None

    return {"org": org, "sup": sup, "root": root}
