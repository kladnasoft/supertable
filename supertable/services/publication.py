# route: supertable.services.publication
"""
Cross-organization clone via publish + accept protocol.

The publication service enables one-time data transfer between
organizations.  Unlike data sharing (which provides live query
access via presigned URLs), publications produce a full physical
copy of the data in the consumer's storage.

Protocol:
  1. Provider publishes a SuperTable (or table subset) → gets
     a publication_id + bearer token + manifest URL.
  2. Provider sends the manifest URL + token to the consumer
     (out of band — email, Slack, etc.).
  3. Consumer calls accept_publication() with the manifest URL
     and token.  This:
       a. Fetches the manifest (presigned URLs for parquet files)
       b. Downloads every file into the consumer's local storage
       c. Creates snapshot JSONs with local file paths
       d. Sets leaf pointers in Redis
       e. Creates a root with import metadata
  4. After acceptance, the consumer owns the data independently.
     The provider can revoke the publication to prevent re-acceptance,
     but already-accepted data is not affected.

Publications reuse the existing share infrastructure (Redis keys,
manifest generation, bearer token auth) but with a separate key
namespace (pub_ prefix) and the accept flow adds physical copy.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
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
# Provider side — create and manage publications
# ---------------------------------------------------------------------------

def create_publication(
    organization: str,
    super_name: str,
    tables: Optional[List[str]] = None,
    label: str = "",
    created_by: str = "",
) -> Dict[str, Any]:
    """Create a publication and return the publication_id + bearer token.

    The bearer token is given to the consumer to authenticate the
    manifest request.  It is shown once and never stored in plaintext.

    Args:
        tables: Optional list of table names.  None = all tables.
    """
    catalog = RedisCatalog()

    if not catalog.root_exists(organization, super_name):
        raise FileNotFoundError(f"SuperTable '{super_name}' not found")

    # Resolve table list
    if tables:
        for t in tables:
            if not catalog.leaf_exists(organization, super_name, t):
                raise FileNotFoundError(f"Table '{t}' not found in '{super_name}'")
    else:
        tables = []
        for leaf_item in catalog.scan_leaf_items(organization, super_name):
            simple = leaf_item.get("simple", "")
            if simple and not (simple.startswith("__") and simple.endswith("__")):
                tables.append(simple)

    pub_id = f"pub_{uuid.uuid4().hex[:12]}"
    bearer_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(bearer_token.encode("utf-8")).hexdigest()

    pub_doc = {
        "publication_id": pub_id,
        "organization": organization,
        "super_name": super_name,
        "tables": tables,
        "token_hash": token_hash,
        "created_by": created_by,
        "created_ms": _now_ms(),
        "enabled": True,
        "label": label or f"Publication of {super_name}",
        "accepted_count": 0,
    }

    # Store using the share infrastructure with pub_ prefix
    catalog.create_share(organization, pub_id, pub_doc)

    return {
        "ok": True,
        "publication_id": pub_id,
        "token": bearer_token,
        "tables": tables,
        "table_count": len(tables),
    }


def revoke_publication(organization: str, publication_id: str) -> Dict[str, Any]:
    """Revoke a publication.  Prevents future acceptance but does not
    affect already-accepted data."""
    catalog = RedisCatalog()
    doc = catalog.get_share(organization, publication_id)
    if not doc:
        raise FileNotFoundError(f"Publication '{publication_id}' not found")

    catalog.delete_share(organization, publication_id)
    return {"ok": True, "publication_id": publication_id}


def list_publications(organization: str) -> List[Dict[str, Any]]:
    """List all publications for an organization."""
    catalog = RedisCatalog()
    all_shares = catalog.list_shares(organization)
    pubs = [s for s in all_shares if s.get("publication_id", "").startswith("pub_")]
    for p in pubs:
        p.pop("token_hash", None)
    return pubs


def get_publication_manifest(
    organization: str,
    publication_id: str,
    bearer_token: str,
) -> Dict[str, Any]:
    """Generate a manifest with presigned URLs for the published tables.

    Reuses the sharing manifest infrastructure — same auth, same
    presigned URL generation.
    """
    from supertable.services.sharing import get_share_manifest
    # Publications use the same Redis key structure as shares,
    # so get_share_manifest works directly.
    manifest = get_share_manifest(organization, publication_id, bearer_token)
    manifest["publication_id"] = publication_id
    return manifest


# ---------------------------------------------------------------------------
# Shared helper — download manifest tables into local storage
# ---------------------------------------------------------------------------

def download_manifest_tables(
    organization: str,
    target_super_name: str,
    manifest: Dict[str, Any],
    source_label: str = "",
) -> Dict[str, Any]:
    """Download all tables from a manifest into a new local SuperTable.

    Creates the SuperTable root, downloads Parquet files, creates snapshots,
    sets leaf pointers, and initializes RBAC.  Used by both accept_publication()
    and materialize_linked_share().
    """
    import httpx

    catalog = RedisCatalog()
    storage = get_storage()

    if catalog.root_exists(organization, target_super_name):
        raise ValueError(f"SuperTable '{target_super_name}' already exists")

    provider_org = manifest.get("provider_org", "")
    provider_super = manifest.get("super_name", "")
    manifest_tables = manifest.get("tables", [])

    if not manifest_tables:
        raise ValueError("Manifest contains no tables")

    now_ms = _now_ms()
    catalog.ensure_root(organization, target_super_name)
    catalog.update_root_flags(organization, target_super_name, {
        "read_only": True,
        "imported_from_org": provider_org,
        "imported_from_super": provider_super,
        "import_ts": now_ms,
        "import_label": source_label or f"Import from {provider_org}/{provider_super}",
        "import_publication_id": manifest.get("publication_id", manifest.get("share_id", "")),
    })

    tables_imported = 0
    files_downloaded = 0
    bytes_downloaded = 0
    errors: List[str] = []

    with httpx.Client(timeout=120.0) as dl_client:
        for tbl in manifest_tables:
            table_name = tbl.get("table", "")
            if not table_name:
                continue

            schema = tbl.get("schema", [])
            resources = tbl.get("resources", [])

            for subdir in ("data", "snapshots"):
                d = os.path.join(organization, target_super_name, "tables", table_name, subdir)
                try:
                    storage.makedirs(d)
                except Exception:
                    pass

            local_resources = []
            for res in resources:
                presigned_url = res.get("file", "")
                if not presigned_url:
                    continue

                file_id = uuid.uuid4().hex[:12]
                local_path = os.path.join(
                    organization, target_super_name, "tables", table_name,
                    "data", f"import_{file_id}.parquet",
                )

                try:
                    dl_resp = dl_client.get(presigned_url)
                    dl_resp.raise_for_status()
                    file_bytes = dl_resp.content

                    storage.write_bytes(local_path, file_bytes)
                    files_downloaded += 1
                    bytes_downloaded += len(file_bytes)

                    local_resources.append({
                        "file": local_path,
                        "rows": res.get("rows", 0),
                        "columns": res.get("columns"),
                    })
                except Exception as e:
                    errors.append(f"{table_name}/{presigned_url}: {e}")
                    logger.warning("[publication] Download failed for %s: %s", table_name, e)

            if not local_resources:
                errors.append(f"{table_name}: no files downloaded")
                continue

            snap_id = uuid.uuid4().hex[:12]
            snap_path = os.path.join(
                organization, target_super_name, "tables", table_name,
                "snapshots", f"import_{snap_id}.json",
            )
            snapshot = {
                "simple_name": table_name,
                "location": os.path.join(organization, target_super_name, "tables", table_name),
                "snapshot_version": 0,
                "last_updated_ms": now_ms,
                "previous_snapshot": None,
                "schema": schema,
                "resources": local_resources,
                "lineage": {
                    "source_type": "import",
                    "provider_org": provider_org,
                    "provider_super": provider_super,
                    "label": source_label,
                },
            }

            storage.write_json(snap_path, snapshot)

            try:
                catalog.set_leaf_payload_cas(
                    organization, target_super_name, table_name,
                    snapshot, snap_path, now_ms=now_ms,
                )
            except Exception:
                catalog.set_leaf_path_cas(
                    organization, target_super_name, table_name,
                    snap_path, now_ms=now_ms,
                )

            tables_imported += 1

    try:
        from supertable.rbac.role_manager import RoleManager
        from supertable.rbac.user_manager import UserManager
        RoleManager(super_name=target_super_name, organization=organization)
        UserManager(super_name=target_super_name, organization=organization)
    except Exception:
        pass

    return {
        "ok": True,
        "target": target_super_name,
        "provider_org": provider_org,
        "provider_super": provider_super,
        "tables_imported": tables_imported,
        "files_downloaded": files_downloaded,
        "bytes_downloaded": bytes_downloaded,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Consumer side — accept a publication (physical copy)
# ---------------------------------------------------------------------------

def accept_publication(
    organization: str,
    target_super_name: str,
    manifest_url: str,
    bearer_token: str,
    label: str = "",
) -> Dict[str, Any]:
    """Accept a publication by downloading all data into local storage."""
    import httpx

    # 1. Fetch manifest
    try:
        with httpx.Client(timeout=60.0) as client:
            resp = client.get(
                manifest_url,
                headers={"Authorization": f"Bearer {bearer_token}"},
            )
            resp.raise_for_status()
            manifest = resp.json()
    except httpx.HTTPStatusError as e:
        status = e.response.status_code
        if status in (401, 403):
            raise PermissionError(f"Access denied by provider (HTTP {status})")
        raise ConnectionError(f"Provider returned HTTP {status}")
    except httpx.RequestError as e:
        raise ConnectionError(f"Cannot reach provider: {e}")

    # 2. Download into local storage
    return download_manifest_tables(
        organization, target_super_name, manifest,
        source_label=label or f"Publication from {manifest.get('provider_org', 'unknown')}",
    )
