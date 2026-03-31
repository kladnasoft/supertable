# route: supertable.services.sharing
"""
Data sharing service — cross-organization read-only data sharing.

Provider side: create shares (grant access to specific tables for a
partner organization), revoke shares, and serve manifests containing
presigned URLs to the underlying parquet files.

Consumer side: link to an external share (register a provider manifest
endpoint), refresh cached manifests (lazy, on query), unlink.

The protocol is simple:
  1. Provider creates a share → gets a share_id + bearer token.
  2. Provider gives the manifest URL + token to the consumer (out of band).
  3. Consumer registers the linked share in their local instance.
  4. On query, the consumer's engine fetches the manifest (presigned URLs)
     and reads parquet directly from the provider's object storage via httpfs.

No data is copied.  The provider controls access by revoking shares.
Presigned URLs expire after SUPERTABLE_SHARE_PRESIGN_TTL seconds (default: 4h).
The consumer refreshes SUPERTABLE_SHARE_REFRESH_BUFFER seconds before expiry.
"""
from __future__ import annotations

import hashlib
import json
import logging
import secrets
import time
import uuid
from typing import Any, Dict, List, Optional

from supertable.config.settings import settings
from supertable.redis_catalog import RedisCatalog
from supertable.storage.storage_factory import get_storage

logger = logging.getLogger(__name__)


def _now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------
# Provider side — share management
# ---------------------------------------------------------------------------

def create_share(
    organization: str,
    super_name: str,
    tables: List[str],
    grantee_org: str,
    created_by: str,
    label: str = "",
) -> Dict[str, Any]:
    """Create a share definition and return the share_id + bearer token.

    The bearer token is given to the consumer to authenticate manifest requests.
    """
    catalog = RedisCatalog()

    if not catalog.root_exists(organization, super_name):
        raise FileNotFoundError(f"SuperTable '{super_name}' not found")

    # Validate tables exist
    for t in tables:
        if not catalog.leaf_exists(organization, super_name, t):
            raise FileNotFoundError(f"Table '{t}' not found in '{super_name}'")

    share_id = f"sh_{uuid.uuid4().hex[:12]}"
    bearer_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(bearer_token.encode("utf-8")).hexdigest()

    share_doc = {
        "share_id": share_id,
        "organization": organization,
        "super_name": super_name,
        "tables": tables,
        "grantee_org": grantee_org,
        "token_hash": token_hash,
        "created_by": created_by,
        "created_ms": _now_ms(),
        "enabled": True,
        "label": label or f"Share to {grantee_org}",
    }

    catalog.create_share(organization, share_id, share_doc)

    return {
        "ok": True,
        "share_id": share_id,
        "token": bearer_token,
        "tables": tables,
        "grantee_org": grantee_org,
    }


def revoke_share(organization: str, share_id: str) -> Dict[str, Any]:
    """Revoke (delete) a share. Consumers lose access immediately."""
    catalog = RedisCatalog()
    doc = catalog.get_share(organization, share_id)
    if not doc:
        raise FileNotFoundError(f"Share '{share_id}' not found")

    catalog.delete_share(organization, share_id)
    return {"ok": True, "share_id": share_id}


def list_org_shares(organization: str) -> List[Dict[str, Any]]:
    """List all shares for an organization (provider view)."""
    catalog = RedisCatalog()
    shares = catalog.list_shares(organization)
    # Strip token_hash from the response
    for s in shares:
        s.pop("token_hash", None)
    return shares


def get_share_manifest(
    organization: str,
    share_id: str,
    bearer_token: str,
) -> Dict[str, Any]:
    """Generate a manifest with presigned URLs for the shared tables.

    Authenticates using the bearer token provided at share creation time.
    Returns snapshot metadata + presigned parquet URLs for each table.
    """
    catalog = RedisCatalog()
    storage = get_storage()

    share_doc = catalog.get_share(organization, share_id)
    if not share_doc:
        raise PermissionError("Share not found or revoked")

    if not share_doc.get("enabled"):
        raise PermissionError("Share is disabled")

    # Authenticate
    expected_hash = share_doc.get("token_hash", "")
    provided_hash = hashlib.sha256(bearer_token.encode("utf-8")).hexdigest()
    if not expected_hash or provided_hash != expected_hash:
        raise PermissionError("Invalid share token")

    sup = share_doc["super_name"]
    tables = share_doc.get("tables", [])
    presign_ttl = settings.SUPERTABLE_SHARE_PRESIGN_TTL

    manifest_tables: List[Dict[str, Any]] = []

    for table_name in tables:
        leaf = catalog.get_leaf(organization, sup, table_name)
        if not leaf:
            continue

        # Get snapshot
        snapshot = None
        payload = leaf.get("payload") if isinstance(leaf, dict) else None
        if isinstance(payload, dict) and isinstance(payload.get("resources"), list):
            snapshot = payload
        else:
            path = leaf.get("path", "")
            if path:
                try:
                    snapshot = storage.read_json(path)
                except Exception:
                    continue

        if not snapshot:
            continue

        # Generate presigned URLs for each parquet file
        presign_fn = getattr(storage, "presign", None)
        presigned_resources = []
        for res in snapshot.get("resources", []):
            file_path = res.get("file", "")
            if not file_path:
                continue

            presigned_url = file_path  # fallback: raw path
            if callable(presign_fn):
                try:
                    # presign() expects the key relative to the bucket
                    from supertable.engine.engine_common import url_to_key, detect_bucket
                    bucket = detect_bucket()
                    key = url_to_key(file_path, bucket)
                    if key:
                        presigned_url = presign_fn(key, expiry_seconds=presign_ttl)
                    else:
                        presigned_url = presign_fn(file_path, expiry_seconds=presign_ttl)
                except Exception as e:
                    logger.warning("presign failed for %s: %s", file_path, e)

            presigned_resources.append({
                "file": presigned_url,
                "rows": res.get("rows", 0),
                "columns": res.get("columns"),
            })

        manifest_tables.append({
            "table": table_name,
            "schema": snapshot.get("schema", []),
            "snapshot_version": snapshot.get("snapshot_version", 0),
            "last_updated_ms": snapshot.get("last_updated_ms", 0),
            "resources": presigned_resources,
            "resource_count": len(presigned_resources),
            "total_rows": sum(r.get("rows", 0) for r in presigned_resources),
        })

    now = _now_ms()
    return {
        "share_id": share_id,
        "provider_org": organization,
        "super_name": sup,
        "grantee_org": share_doc.get("grantee_org", ""),
        "tables": manifest_tables,
        "generated_ms": now,
        "expires_ms": now + (presign_ttl * 1000),
        "presign_ttl_seconds": presign_ttl,
    }


# ---------------------------------------------------------------------------
# Consumer side — linked share management
# ---------------------------------------------------------------------------

def link_share(
    organization: str,
    super_name: str,
    provider_url: str,
    provider_token: str,
    alias_prefix: str = "",
    label: str = "",
) -> Dict[str, Any]:
    """Register an external share as a linked share in this instance.

    The provider_url should be the full manifest endpoint URL, e.g.:
    https://acme.data-island.io/api/v1/shares/sh_abc123/manifest

    On registration, immediately fetches the manifest to verify access
    and cache the initial data.
    """
    catalog = RedisCatalog()

    if not catalog.root_exists(organization, super_name):
        raise FileNotFoundError(f"SuperTable '{super_name}' not found")

    # Fetch manifest to verify connectivity and access
    manifest = _fetch_manifest(provider_url, provider_token)

    link_id = f"lnk_{uuid.uuid4().hex[:10]}"
    now = _now_ms()

    link_doc = {
        "link_id": link_id,
        "provider_url": provider_url,
        "provider_token": provider_token,
        "alias_prefix": alias_prefix,
        "label": label or f"Share from {manifest.get('provider_org', 'unknown')}",
        "provider_org": manifest.get("provider_org", ""),
        "provider_super": manifest.get("super_name", ""),
        "share_id": manifest.get("share_id", ""),
        "created_ms": now,
        "last_refreshed_ms": now,
        "expires_ms": manifest.get("expires_ms", 0),
        "cached_manifest": manifest,
    }

    catalog.create_linked_share(organization, super_name, link_id, link_doc)

    tables = [t.get("table", "") for t in manifest.get("tables", [])]
    return {
        "ok": True,
        "link_id": link_id,
        "tables": tables,
        "provider_org": manifest.get("provider_org", ""),
    }


def unlink_share(organization: str, super_name: str, link_id: str) -> Dict[str, Any]:
    """Remove a linked share from this instance."""
    catalog = RedisCatalog()
    doc = catalog.get_linked_share(organization, super_name, link_id)
    if not doc:
        raise FileNotFoundError(f"Linked share '{link_id}' not found")
    catalog.delete_linked_share(organization, super_name, link_id)
    return {"ok": True, "link_id": link_id}


def list_linked_shares(organization: str, super_name: str) -> List[Dict[str, Any]]:
    """List all linked shares for a supertable (consumer view)."""
    catalog = RedisCatalog()
    links = catalog.list_linked_shares(organization, super_name)
    # Strip provider_token from the response
    for l in links:
        l.pop("provider_token", None)
        # Strip full cached manifest (too large for listing)
        manifest = l.pop("cached_manifest", None)
        if manifest:
            l["cached_tables"] = [t.get("table", "") for t in manifest.get("tables", [])]
            l["cached_total_rows"] = sum(
                t.get("total_rows", 0) for t in manifest.get("tables", [])
            )
    return links


def refresh_linked_share(
    organization: str,
    super_name: str,
    link_id: str,
    force: bool = False,
) -> Dict[str, Any]:
    """Refresh the cached manifest for a linked share.

    If *force* is False, only refreshes if the cached manifest is within
    SUPERTABLE_SHARE_REFRESH_BUFFER seconds of expiring.
    """
    catalog = RedisCatalog()
    doc = catalog.get_linked_share(organization, super_name, link_id)
    if not doc:
        raise FileNotFoundError(f"Linked share '{link_id}' not found")

    now = _now_ms()
    expires_ms = doc.get("expires_ms", 0)
    buffer_ms = settings.SUPERTABLE_SHARE_REFRESH_BUFFER * 1000

    if not force and expires_ms > 0 and (expires_ms - now) > buffer_ms:
        return {"ok": True, "refreshed": False, "reason": "Cache still valid"}

    manifest = _fetch_manifest(doc["provider_url"], doc["provider_token"])

    doc["cached_manifest"] = manifest
    doc["last_refreshed_ms"] = now
    doc["expires_ms"] = manifest.get("expires_ms", 0)

    catalog.update_linked_share(organization, super_name, link_id, doc)

    return {"ok": True, "refreshed": True, "expires_ms": doc["expires_ms"]}


def get_shared_table_files(
    organization: str,
    super_name: str,
    link_id: str,
    table_name: str,
) -> List[str]:
    """Get presigned file URLs for a shared table, refreshing if needed.

    Called by the query engine when it encounters a linked-share table.
    Returns a list of presigned parquet URLs ready for DuckDB httpfs.
    """
    catalog = RedisCatalog()
    doc = catalog.get_linked_share(organization, super_name, link_id)
    if not doc:
        return []

    # Lazy refresh if close to expiry
    now = _now_ms()
    expires_ms = doc.get("expires_ms", 0)
    buffer_ms = settings.SUPERTABLE_SHARE_REFRESH_BUFFER * 1000

    if expires_ms > 0 and (expires_ms - now) <= buffer_ms:
        try:
            result = refresh_linked_share(organization, super_name, link_id, force=True)
            if result.get("refreshed"):
                doc = catalog.get_linked_share(organization, super_name, link_id)
                if not doc:
                    return []
        except Exception as e:
            logger.warning("Lazy refresh failed for %s: %s", link_id, e)

    manifest = doc.get("cached_manifest", {})
    for t in manifest.get("tables", []):
        if t.get("table") == table_name:
            return [r.get("file", "") for r in t.get("resources", []) if r.get("file")]

    return []


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _fetch_manifest(provider_url: str, provider_token: str) -> Dict[str, Any]:
    """Fetch a share manifest from a remote provider."""
    import httpx

    try:
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(
                provider_url,
                headers={"Authorization": f"Bearer {provider_token}"},
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status == 401 or status == 403:
            raise PermissionError(f"Access denied by provider (HTTP {status})")
        raise ConnectionError(f"Provider returned HTTP {status}")
    except httpx.RequestError as e:
        raise ConnectionError(f"Cannot reach provider: {e}")
