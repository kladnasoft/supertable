# route: supertable.services.sharing
"""
Data sharing service — cross-organization read-only data sharing.

Provider side: create shares (grant access to specific tables for a
partner organization), revoke shares, and serve manifests containing
presigned URLs to the underlying parquet files.

Consumer side: link to an external share (register a provider manifest
endpoint), refresh cached manifests (lazy, on query), unlink.
Virtual leaf injection: when a share is linked, virtual leaf pointers
are created so the query engine reads shared tables transparently.

The protocol is simple:
  1. Provider creates a share → gets a share_id + bearer token.
  2. Provider gives the manifest URL + token to the consumer (out of band).
  3. Consumer registers the linked share in their local instance.
  4. Virtual leaf pointers are created from the manifest — the query
     engine reads shared tables as if they were local.
  5. Presigned URLs expire; the consumer refreshes before expiry.
     Leaf pointers are updated with fresh URLs on each refresh.

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
    columns: Optional[Dict[str, List[str]]] = None,
    row_filter: Optional[Dict[str, str]] = None,
    at_timestamp: Optional[int] = None,
) -> Dict[str, Any]:
    """Create a share definition and return the share_id + bearer token.

    Args:
        columns: Optional per-table column filter. {table_name: [col1, col2, ...]}
                 None or empty = all columns.
        row_filter: Optional per-table row filter. {table_name: "WHERE clause"}
                    None or empty = all rows.
        at_timestamp: Optional epoch-ms for point-in-time share.
                      If set, the share always serves data as of that timestamp.
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

    share_doc: Dict[str, Any] = {
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

    if columns:
        share_doc["columns"] = columns
    if row_filter:
        share_doc["row_filter"] = row_filter
    if at_timestamp is not None:
        share_doc["at_timestamp"] = at_timestamp

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


def _resolve_snapshot_for_share(
    catalog: RedisCatalog,
    storage,
    organization: str,
    super_name: str,
    table_name: str,
    at_timestamp: Optional[int],
) -> Optional[Dict[str, Any]]:
    """Resolve the snapshot to serve — latest or point-in-time."""
    if at_timestamp is not None:
        try:
            from supertable.services.time_travel import list_snapshot_versions, get_snapshot_at_version
            versions = list_snapshot_versions(organization, super_name, table_name, limit=500)
            for v in versions:
                if v.get("last_updated_ms", 0) <= at_timestamp:
                    return get_snapshot_at_version(organization, super_name, table_name, v["version"])
        except Exception as e:
            logger.warning("[sharing] time-travel resolve failed for %s: %s", table_name, e)
        return None

    # Latest snapshot
    leaf = catalog.get_leaf(organization, super_name, table_name)
    if not leaf:
        return None
    payload = leaf.get("payload") if isinstance(leaf, dict) else None
    if isinstance(payload, dict) and isinstance(payload.get("resources"), list):
        return payload
    path = leaf.get("path", "")
    if path:
        try:
            return storage.read_json(path)
        except Exception:
            pass
    return None


def get_share_manifest(
    organization: str,
    share_id: str,
    bearer_token: str,
) -> Dict[str, Any]:
    """Generate a manifest with presigned URLs for the shared tables.

    Authenticates using the bearer token provided at share creation time.
    Returns snapshot metadata + presigned parquet URLs for each table.
    Applies column/row filters and point-in-time resolution if configured.
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
    col_filters = share_doc.get("columns") or {}
    row_filters = share_doc.get("row_filter") or {}
    at_timestamp = share_doc.get("at_timestamp")

    manifest_tables: List[Dict[str, Any]] = []

    for table_name in tables:
        snapshot = _resolve_snapshot_for_share(
            catalog, storage, organization, sup, table_name, at_timestamp,
        )
        if not snapshot:
            continue

        # Apply column filter to schema
        allowed_cols = col_filters.get(table_name)
        schema = snapshot.get("schema", [])
        if allowed_cols:
            schema = [s for s in schema if s.get("name") in allowed_cols]

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

        tbl_entry: Dict[str, Any] = {
            "table": table_name,
            "schema": schema,
            "snapshot_version": snapshot.get("snapshot_version", 0),
            "last_updated_ms": snapshot.get("last_updated_ms", 0),
            "resources": presigned_resources,
            "resource_count": len(presigned_resources),
            "total_rows": sum(r.get("rows", 0) for r in presigned_resources),
        }

        # Include row filter in manifest so consumer can apply it
        table_filter = row_filters.get(table_name)
        if table_filter:
            tbl_entry["row_filter"] = table_filter

        # Include column filter so consumer knows which columns are allowed
        if allowed_cols:
            tbl_entry["allowed_columns"] = allowed_cols

        manifest_tables.append(tbl_entry)

    now = _now_ms()
    manifest: Dict[str, Any] = {
        "share_id": share_id,
        "provider_org": organization,
        "super_name": sup,
        "grantee_org": share_doc.get("grantee_org", ""),
        "tables": manifest_tables,
        "generated_ms": now,
        "expires_ms": now + (presign_ttl * 1000),
        "presign_ttl_seconds": presign_ttl,
    }

    if at_timestamp is not None:
        manifest["at_timestamp"] = at_timestamp

    return manifest


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

    On registration, immediately fetches the manifest to verify access,
    caches it, and creates virtual leaf pointers so the query engine
    can read shared tables transparently.
    """
    catalog = RedisCatalog()

    if not catalog.root_exists(organization, super_name):
        raise FileNotFoundError(f"SuperTable '{super_name}' not found")

    # Replicas resolve leaf reads to the source — virtual leaves we create
    # here would be invisible to the query engine.  Block this.
    root = catalog.get_root(organization, super_name)
    if root and root.get("clone_type") == "replica":
        raise ValueError(
            f"Cannot link shares to replica '{super_name}'. "
            "Link to the source SuperTable instead, or promote the replica first."
        )

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

    # Create virtual leaf pointers so query engine can read shared tables
    _materialize_linked_share_leaves(catalog, organization, super_name, link_id, manifest, alias_prefix)

    tables = [t.get("table", "") for t in manifest.get("tables", [])]
    return {
        "ok": True,
        "link_id": link_id,
        "tables": tables,
        "provider_org": manifest.get("provider_org", ""),
    }


def unlink_share(organization: str, super_name: str, link_id: str) -> Dict[str, Any]:
    """Remove a linked share and its virtual leaf pointers."""
    catalog = RedisCatalog()
    doc = catalog.get_linked_share(organization, super_name, link_id)
    if not doc:
        raise FileNotFoundError(f"Linked share '{link_id}' not found")

    # Remove virtual leaf pointers
    _remove_linked_share_leaves(catalog, organization, super_name, link_id, doc)

    catalog.delete_linked_share(organization, super_name, link_id)
    return {"ok": True, "link_id": link_id}


def list_linked_shares(organization: str, super_name: str) -> List[Dict[str, Any]]:
    """List all linked shares for a supertable (consumer view)."""
    catalog = RedisCatalog()
    links = catalog.list_linked_shares(organization, super_name)
    now = _now_ms()
    for l in links:
        l.pop("provider_token", None)
        # Strip full cached manifest (too large for listing)
        manifest = l.pop("cached_manifest", None)
        if manifest:
            l["cached_tables"] = [t.get("table", "") for t in manifest.get("tables", [])]
            l["cached_total_rows"] = sum(
                t.get("total_rows", 0) for t in manifest.get("tables", [])
            )
        # Health status
        expires_ms = l.get("expires_ms", 0)
        if expires_ms <= 0:
            l["status"] = "unknown"
        elif expires_ms < now:
            l["status"] = "expired"
        elif (expires_ms - now) < (settings.SUPERTABLE_SHARE_REFRESH_BUFFER * 1000 * 2):
            l["status"] = "expiring_soon"
        else:
            l["status"] = "healthy"
        l["presign_expires_in_s"] = max(0, int((expires_ms - now) / 1000)) if expires_ms > 0 else 0
    return links


def refresh_linked_share(
    organization: str,
    super_name: str,
    link_id: str,
    force: bool = False,
) -> Dict[str, Any]:
    """Refresh the cached manifest for a linked share.

    Also updates virtual leaf pointers with fresh presigned URLs.
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

    start = time.monotonic()
    manifest = _fetch_manifest(doc["provider_url"], doc["provider_token"])
    latency_ms = int((time.monotonic() - start) * 1000)

    old_manifest = doc.get("cached_manifest", {})
    alias_prefix = doc.get("alias_prefix", "")

    doc["cached_manifest"] = manifest
    doc["last_refreshed_ms"] = now
    doc["expires_ms"] = manifest.get("expires_ms", 0)
    doc["last_latency_ms"] = latency_ms

    catalog.update_linked_share(organization, super_name, link_id, doc)

    # Update virtual leaves: create/update new ones FIRST (atomic per-key),
    # then remove leaves for tables that were in the old manifest but not the
    # new one.  This avoids a race window where a concurrent query sees a
    # table as missing between remove and re-create.
    _materialize_linked_share_leaves(catalog, organization, super_name, link_id, manifest, alias_prefix)

    if old_manifest:
        old_tables = {t.get("table", "") for t in old_manifest.get("tables", []) if t.get("table")}
        new_tables = {t.get("table", "") for t in manifest.get("tables", []) if t.get("table")}
        removed_tables = old_tables - new_tables
        if removed_tables:
            # Build a synthetic doc with only the removed tables for cleanup
            removed_manifest = {"tables": [{"table": t} for t in removed_tables]}
            _remove_linked_share_leaves(catalog, organization, super_name, link_id,
                                        {"cached_manifest": removed_manifest, "alias_prefix": alias_prefix})

    return {"ok": True, "refreshed": True, "expires_ms": doc["expires_ms"], "latency_ms": latency_ms}


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
# Virtual leaf injection — make linked shares queryable
# ---------------------------------------------------------------------------

def _materialize_linked_share_leaves(
    catalog: RedisCatalog,
    organization: str,
    super_name: str,
    link_id: str,
    manifest: Dict[str, Any],
    alias_prefix: str = "",
) -> int:
    """Create virtual leaf pointers from a linked share's manifest.

    These leaves look identical to local leaves but their resources
    contain presigned URLs instead of local storage paths.  The query
    engine reads them transparently via _to_duckdb_path() which passes
    URLs through as-is.

    Safety: refuses to overwrite a local (non-virtual) leaf — the admin
    must use an alias_prefix to avoid name collisions.

    Returns the number of tables materialized.
    """
    now = _now_ms()
    count = 0

    for tbl in manifest.get("tables", []):
        table_name = tbl.get("table", "")
        if not table_name:
            continue

        local_name = f"{alias_prefix}{table_name}" if alias_prefix else table_name

        # Safety: do NOT overwrite a local (non-virtual) leaf, and do NOT
        # overwrite a virtual leaf belonging to a different linked share.
        existing = catalog._get_leaf_raw(organization, super_name, local_name)
        if existing:
            existing_payload = existing.get("payload") if isinstance(existing, dict) else None
            if isinstance(existing_payload, dict):
                existing_share = existing_payload.get("_linked_share")
                if not existing_share:
                    # Local (non-virtual) leaf — never overwrite
                    logger.warning(
                        "[sharing] Skipping virtual leaf for '%s' — local table exists. "
                        "Use an alias_prefix to avoid collisions.", local_name,
                    )
                    continue
                if existing_share != link_id:
                    # Virtual leaf from a different linked share — don't overwrite
                    logger.warning(
                        "[sharing] Skipping virtual leaf for '%s' — already provided by "
                        "linked share '%s'. Use an alias_prefix.", local_name, existing_share,
                    )
                    continue

        snapshot: Dict[str, Any] = {
            "simple_name": local_name,
            "snapshot_version": tbl.get("snapshot_version", 0),
            "last_updated_ms": tbl.get("last_updated_ms", now),
            "schema": tbl.get("schema", []),
            "resources": tbl.get("resources", []),
            "_linked_share": link_id,
            "_provider_org": manifest.get("provider_org", ""),
        }

        # Carry row filter from manifest if present
        row_filter = tbl.get("row_filter")
        if row_filter:
            snapshot["_row_filter"] = row_filter

        # Carry allowed columns if present
        allowed_cols = tbl.get("allowed_columns")
        if allowed_cols:
            snapshot["_allowed_columns"] = allowed_cols

        virtual_path = f"__linked_share__/{link_id}/{table_name}"

        try:
            catalog.set_leaf_payload_cas(
                organization, super_name, local_name,
                snapshot, virtual_path, now_ms=now,
            )
            count += 1
        except Exception as e:
            logger.warning("[sharing] Failed to create virtual leaf for %s: %s", local_name, e)

    return count


def _remove_linked_share_leaves(
    catalog: RedisCatalog,
    organization: str,
    super_name: str,
    link_id: str,
    link_doc: Dict[str, Any],
) -> int:
    """Remove virtual leaf pointers created by a linked share.

    Safety: only deletes leaves whose payload ``_linked_share`` matches
    this link_id.  Never touches local (non-virtual) leaves.
    """
    manifest = link_doc.get("cached_manifest", {})
    alias_prefix = link_doc.get("alias_prefix", "")
    count = 0

    for tbl in manifest.get("tables", []):
        table_name = tbl.get("table", "")
        if not table_name:
            continue
        local_name = f"{alias_prefix}{table_name}" if alias_prefix else table_name

        try:
            # Only delete if the leaf is actually a virtual share leaf belonging to us
            leaf = catalog._get_leaf_raw(organization, super_name, local_name)
            if not leaf:
                continue
            payload = leaf.get("payload") if isinstance(leaf, dict) else None
            if isinstance(payload, dict) and payload.get("_linked_share") == link_id:
                catalog.delete_leaf(organization, super_name, local_name)
                count += 1
        except Exception as e:
            logger.warning("[sharing] Failed to remove virtual leaf for %s: %s", local_name, e)

    return count


# ---------------------------------------------------------------------------
# Share-to-clone conversion (materialize linked share)
# ---------------------------------------------------------------------------

def materialize_linked_share(
    organization: str,
    super_name: str,
    link_id: str,
    target_super_name: str,
) -> Dict[str, Any]:
    """Download all data from a linked share into a new local SuperTable.

    Force-refreshes the manifest to get fresh presigned URLs, then
    reuses the publication download infrastructure to do the physical copy.
    """
    catalog = RedisCatalog()
    doc = catalog.get_linked_share(organization, super_name, link_id)
    if not doc:
        raise FileNotFoundError(f"Linked share '{link_id}' not found")

    if catalog.root_exists(organization, target_super_name):
        raise ValueError(f"SuperTable '{target_super_name}' already exists")

    # Force-refresh to get fresh presigned URLs
    refresh_linked_share(organization, super_name, link_id, force=True)
    doc = catalog.get_linked_share(organization, super_name, link_id)
    manifest = doc.get("cached_manifest", {})

    if not manifest or not manifest.get("tables"):
        raise ValueError("No tables in linked share manifest")

    from supertable.services.publication import download_manifest_tables
    return download_manifest_tables(
        organization, target_super_name, manifest,
        source_label=f"materialized from linked share {link_id}",
    )


# ---------------------------------------------------------------------------
# Share health dashboard (provider side)
# ---------------------------------------------------------------------------

def get_share_dashboard(organization: str) -> List[Dict[str, Any]]:
    """Enriched share list with access stats for provider dashboard.

    Reads recent SHARE_MANIFEST_ACCESS audit events to compute
    per-share access frequency.
    """
    shares = list_org_shares(organization)

    # Try to read access stats from audit stream
    access_stats: Dict[str, Dict[str, Any]] = {}
    try:
        catalog = RedisCatalog()
        stream_key = f"supertable:{organization}:audit:stream"
        entries = catalog.r.xrevrange(stream_key, count=1000)
        now = _now_ms()
        seven_days_ago = now - (7 * 24 * 60 * 60 * 1000)

        for entry_id, fields in entries:
            try:
                raw = fields.get(b"data") or fields.get("data")
                if not raw:
                    continue
                event = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
                if event.get("action") != "share_manifest_access":
                    continue
                rid = event.get("resource_id", "")
                if not rid:
                    continue
                ts = int(event.get("timestamp_ms", 0))

                if rid not in access_stats:
                    access_stats[rid] = {"last_accessed_ms": 0, "access_count_7d": 0}

                if ts > access_stats[rid]["last_accessed_ms"]:
                    access_stats[rid]["last_accessed_ms"] = ts

                if ts >= seven_days_ago:
                    access_stats[rid]["access_count_7d"] += 1

            except Exception:
                continue
    except Exception as e:
        logger.debug("[sharing] Audit stream read failed (non-fatal): %s", e)

    # Enrich shares with access stats
    for s in shares:
        sid = s.get("share_id", "")
        stats = access_stats.get(sid, {})
        s["last_accessed_ms"] = stats.get("last_accessed_ms", 0)
        s["access_count_7d"] = stats.get("access_count_7d", 0)

    return shares


# ---------------------------------------------------------------------------
# Auto-refresh — batch check and refresh expiring shares
# ---------------------------------------------------------------------------

def refresh_expiring_shares(
    organization: str,
    super_name: str,
) -> Dict[str, Any]:
    """Check all linked shares and refresh any that are close to expiry.

    Called on page load (fire-and-forget) to proactively keep
    presigned URLs fresh without a background daemon.
    """
    catalog = RedisCatalog()
    links = catalog.list_linked_shares(organization, super_name)
    now = _now_ms()
    buffer_ms = settings.SUPERTABLE_SHARE_REFRESH_BUFFER * 1000

    refreshed = 0
    failed = 0

    for l in links:
        link_id = l.get("link_id", "")
        expires_ms = l.get("expires_ms", 0)

        if not link_id or expires_ms <= 0:
            continue

        # Refresh if within 2x buffer of expiry (slightly more aggressive)
        if (expires_ms - now) > (buffer_ms * 2):
            continue

        try:
            result = refresh_linked_share(organization, super_name, link_id, force=True)
            if result.get("refreshed"):
                refreshed += 1
        except Exception as e:
            logger.warning("[sharing] Auto-refresh failed for %s: %s", link_id, e)
            failed += 1

    return {"ok": True, "checked": len(links), "refreshed": refreshed, "failed": failed}


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
