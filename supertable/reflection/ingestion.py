# path: supertable/reflection/ingestion.py
from __future__ import annotations

import json
import os
import re
import time
import asyncio
import tempfile
import uuid
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request, File, Form, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse

from supertable.storage.storage_factory import get_storage

# Prefer installed package; fallback to local module for dev
try:
    from supertable.meta_reader import MetaReader  # type: ignore
except Exception:  # pragma: no cover
    from meta_reader import MetaReader  # type: ignore

logger = logging.getLogger(__name__)


# ------------------------------ Staging / Pipes helpers ------------------------------

_STAGING_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


def _staging_base_dir(org: str, sup: str) -> str:
    return os.path.join(org, sup, "staging")


def _staging_index_path(org: str, sup: str) -> str:
    return os.path.join(_staging_base_dir(org, sup), "_staging.json")


def _pipe_index_path(org: str, sup: str) -> str:
    return os.path.join(_staging_base_dir(org, sup), "_pipe.json")


def _staging_key(org: str, sup: str, staging_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}"


def _staging_index_key(org: str, sup: str) -> str:
    # Index of staging names for website/UI listing.
    return f"supertable:{org}:{sup}:meta:staging:meta"


def _pipe_key(org: str, sup: str, staging_name: str, pipe_name: str) -> str:
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:{pipe_name}"


def _pipe_index_key(org: str, sup: str, staging_name: str) -> str:
    # Index of pipe names for a given staging for website/UI listing.
    return f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:meta"


def _redis_json_load(raw: Optional[str]) -> Optional[Dict[str, Any]]:
    if not raw:
        return None
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
        return {"value": obj}
    except Exception:
        return {"value": raw}


def _read_json_if_exists(storage: Any, path: str) -> Optional[Dict[str, Any]]:
    try:
        if not storage.exists(path):
            return None
    except Exception:
        return None
    try:
        data = storage.read_json(path)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _write_json_atomic(storage: Any, path: str, data: Dict[str, Any]) -> None:
    # Storage interface is expected to handle overwrite atomically enough for our UI use.
    base = os.path.dirname(path)
    try:
        if base and not storage.exists(base):
            storage.makedirs(base)
    except Exception:
        # best effort; write_json might still create prefixes on object stores
        pass
    storage.write_json(path, data)


def _flatten_tree_leaves(prefix: str, node: Any) -> List[str]:
    out: List[str] = []

    def dfs(p: str, n: Any) -> None:
        if n is None:
            out.append(p)
            return
        if not isinstance(n, dict):
            return
        for k, v in n.items():
            dfs(os.path.join(p, k), v)

    dfs(prefix, node)
    return out


def _scan_staging_names(storage: Any, org: str, sup: str) -> List[str]:
    base = _staging_base_dir(org, sup)
    try:
        if not storage.exists(base):
            return []
        tree = storage.get_directory_structure(base)
    except Exception:
        return []
    if not isinstance(tree, dict):
        return []
    names: List[str] = []
    for name, child in tree.items():
        if not isinstance(name, str):
            continue
        if name.startswith("_"):
            continue
        # staging folders appear as dict nodes
        if isinstance(child, dict):
            names.append(name)
    return sorted(set(names))


def _get_staging_names(storage: Any, org: str, sup: str) -> List[str]:
    idx_path = _staging_index_path(org, sup)
    data = _read_json_if_exists(storage, idx_path)
    names: List[str] = []
    if data:
        raw = data.get("staging_names")
        if isinstance(raw, list):
            names = [str(x) for x in raw if isinstance(x, (str, int, float))]
    # Merge with a best-effort scan in case index is missing/stale.
    scanned = _scan_staging_names(storage, org, sup)
    merged = sorted(set(names) | set(scanned))
    if merged and (not data or sorted(set(names)) != merged):
        _write_json_atomic(storage, idx_path, {"staging_names": merged, "updated_at_ns": time.time_ns()})
    return merged


def _load_pipe_index(storage: Any, org: str, sup: str) -> List[Dict[str, Any]]:
    idx_path = _pipe_index_path(org, sup)
    data = _read_json_if_exists(storage, idx_path)
    pipes: List[Dict[str, Any]] = []
    if data:
        raw = data.get("pipes")
        if isinstance(raw, list):
            for item in raw:
                if isinstance(item, dict) and item.get("pipe_name") and item.get("staging_name"):
                    pipes.append(item)
    return pipes


def _scan_pipes(storage: Any, org: str, sup: str, staging_names: List[str]) -> List[Dict[str, Any]]:
    pipes: List[Dict[str, Any]] = []
    from supertable.super_pipe import SuperPipe  # noqa: WPS433

    for stg in staging_names:
        try:
            sp = SuperPipe(organization=org, super_name=sup, staging_name=stg)
        except Exception:
            continue
        try:
            tree = storage.get_directory_structure(os.path.join(_staging_base_dir(org, sup), stg, "pipes"))
        except Exception:
            continue
        for path in _flatten_tree_leaves(os.path.join(_staging_base_dir(org, sup), stg, "pipes"), tree):
            if not path.endswith(".json"):
                continue
            try:
                data = storage.read_json(path)
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            # normalize minimal keys
            if not data.get("pipe_name"):
                data["pipe_name"] = os.path.splitext(os.path.basename(path))[0]
            if not data.get("staging_name"):
                data["staging_name"] = stg
            pipes.append(data)
    return pipes


def _get_pipes(storage: Any, org: str, sup: str, staging_names: List[str]) -> List[Dict[str, Any]]:
    pipes = _load_pipe_index(storage, org, sup)
    if pipes:
        # best-effort freshness: merge with scan if index seems incomplete
        scanned = _scan_pipes(storage, org, sup, staging_names)
        if scanned:
            existing_keys = {(p.get("staging_name"), p.get("pipe_name")) for p in pipes}
            for p in scanned:
                key = (p.get("staging_name"), p.get("pipe_name"))
                if key not in existing_keys:
                    pipes.append(p)
            # persist merged index
            _write_json_atomic(
                storage,
                _pipe_index_path(org, sup),
                {"pipes": pipes, "updated_at_ns": time.time_ns()},
            )
        return pipes

    scanned = _scan_pipes(storage, org, sup, staging_names)
    if scanned:
        _write_json_atomic(
            storage,
            _pipe_index_path(org, sup),
            {"pipes": scanned, "updated_at_ns": time.time_ns()},
        )
    return scanned


# ------------------------------ Redis helpers (require redis_client) ------------------------------

def _make_redis_helpers(redis_client: Any) -> Dict[str, Any]:
    """Build redis helper closures bound to the given redis_client instance."""

    def redis_get_staging_meta(org: str, sup: str, staging_name: str) -> Optional[Dict[str, Any]]:
        try:
            return _redis_json_load(redis_client.get(_staging_key(org, sup, staging_name)))
        except Exception:
            return None

    def redis_get_pipe_meta(org: str, sup: str, staging_name: str, pipe_name: str) -> Optional[Dict[str, Any]]:
        try:
            return _redis_json_load(redis_client.get(_pipe_key(org, sup, staging_name, pipe_name)))
        except Exception:
            return None

    def redis_list_stagings(org: str, sup: str) -> List[str]:
        idx = _staging_index_key(org, sup)
        try:
            names = redis_client.smembers(idx) or set()
            if names:
                return sorted({str(x) for x in names if str(x).strip() and str(x) != "meta"})
        except Exception:
            pass

        # Fallback for older data: scan for staging meta keys.
        pattern = f"supertable:{org}:{sup}:meta:staging:*"
        cursor = 0
        out: Set[str] = set()
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                tail = k.rsplit("meta:staging:", 1)[-1]
                if not tail or tail == "meta":
                    continue
                # staging meta key has no further ":" segments (pipe keys do)
                if ":" in tail:
                    continue
                out.add(tail)
            if cursor == 0:
                break
        return sorted(out)

    def redis_list_pipes(org: str, sup: str, staging_name: str) -> List[str]:
        idx = _pipe_index_key(org, sup, staging_name)
        try:
            names = redis_client.smembers(idx) or set()
            if names:
                return sorted({str(x) for x in names if str(x).strip() and str(x) != "meta"})
        except Exception:
            pass

        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:pipe:*"
        cursor = 0
        out: Set[str] = set()
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            for key in keys:
                k = key if isinstance(key, str) else key.decode("utf-8")
                tail = k.rsplit(":pipe:", 1)[-1]
                if not tail or tail == "meta":
                    continue
                if ":" in tail:
                    continue
                out.add(tail)
            if cursor == 0:
                break
        return sorted(out)

    def redis_upsert_staging_meta(org: str, sup: str, staging_name: str, meta: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload["updated_at_ms"] = int(time.time() * 1000)

        try:
            redis_client.set(_staging_key(org, sup, staging_name), json.dumps(payload, ensure_ascii=False))
            redis_client.sadd(_staging_index_key(org, sup), staging_name)
        except Exception:
            # UI should not fail hard if Redis is unavailable
            pass

    def redis_upsert_pipe_meta(
        org: str,
        sup: str,
        staging_name: str,
        pipe_name: str,
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        payload: Dict[str, Any] = dict(meta or {})
        payload.setdefault("organization", org)
        payload.setdefault("super_name", sup)
        payload.setdefault("staging_name", staging_name)
        payload.setdefault("pipe_name", pipe_name)
        payload["updated_at_ms"] = int(time.time() * 1000)

        try:
            redis_client.set(_pipe_key(org, sup, staging_name, pipe_name), json.dumps(payload, ensure_ascii=False))
            redis_client.sadd(_pipe_index_key(org, sup, staging_name), pipe_name)
            redis_client.sadd(_staging_index_key(org, sup), staging_name)
        except Exception:
            pass

    def redis_delete_pipe_meta(org: str, sup: str, staging_name: str, pipe_name: str) -> None:
        try:
            redis_client.srem(_pipe_index_key(org, sup, staging_name), pipe_name)
        except Exception:
            pass
        try:
            redis_client.delete(_pipe_key(org, sup, staging_name, pipe_name))
        except Exception:
            pass

    def redis_delete_staging_cascade(org: str, sup: str, staging_name: str) -> None:
        # Remove from index + delete the base meta key.
        try:
            redis_client.srem(_staging_index_key(org, sup), staging_name)
        except Exception:
            pass
        try:
            redis_client.delete(_staging_key(org, sup, staging_name))
        except Exception:
            pass

        # Delete everything under the staging namespace (pipes + indices + any future keys).
        pattern = f"supertable:{org}:{sup}:meta:staging:{staging_name}:*"
        cursor = 0
        while True:
            cursor, keys = redis_client.scan(cursor=cursor, match=pattern, count=1000)
            if keys:
                try:
                    pipe = redis_client.pipeline()
                    for k in keys:
                        pipe.delete(k)
                    pipe.execute()
                except Exception:
                    # best effort
                    pass
            if cursor == 0:
                break

        try:
            redis_client.delete(_pipe_index_key(org, sup, staging_name))
        except Exception:
            pass

    return {
        "redis_get_staging_meta": redis_get_staging_meta,
        "redis_get_pipe_meta": redis_get_pipe_meta,
        "redis_list_stagings": redis_list_stagings,
        "redis_list_pipes": redis_list_pipes,
        "redis_upsert_staging_meta": redis_upsert_staging_meta,
        "redis_upsert_pipe_meta": redis_upsert_pipe_meta,
        "redis_delete_pipe_meta": redis_delete_pipe_meta,
        "redis_delete_staging_cascade": redis_delete_staging_cascade,
    }


# ------------------------------ Route attachment ------------------------------


# ---------------------------------------------------------------------------
# Route attachment — no-op stub.
# All endpoints previously registered here have moved to supertable.api.api.
# This function is preserved so existing callers do not break.
# ---------------------------------------------------------------------------

def attach_ingestion_routes(
    router,
    *,
    templates,
    is_authorized,
    no_store,
    get_provided_token,
    discover_pairs,
    resolve_pair,
    inject_session_into_ctx,
    get_session,
    is_superuser,
    logged_in_guard_api,
    admin_guard_api,
    redis_client,
):
    """No-op — endpoints moved to supertable.api.api."""
    pass
