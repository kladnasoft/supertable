# route: supertable.mirroring.mirror_parquet
"""
Parquet mirror (latest-only projection)

Behavior:
- Copies current snapshot parquet files into a co-located table folder:
    <org>/<super>/parquet/<table>/files/
- Removes any previously co-located files that are not part of the current snapshot.
- Does **not** write any transaction logs or JSON metadata.
- No-op if there are no file changes (but still verifies dirs exist).

This mirrors the copy/delete semantics used by the Delta writer, minus _delta_log.
"""

from __future__ import annotations

import os
import hashlib
from typing import Any, Dict, List, Set, Tuple

from supertable.config.defaults import logger


def _binary_copy_if_possible(storage, src_path: str, dst_path: str) -> bool:
    """Copy bytes from src_path to dst_path as efficiently as the backend allows.

    The storage adapter owns physical-prefix translation and may implement
    ``copy`` server-side. Falls back to read_bytes/write_bytes.
    """

    # --- Generic backend copy ------------------------------------------------
    if hasattr(storage, "copy"):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.copy(src_path, dst_path)
            return True
        except Exception as e:
            logger.warning(f"[mirror] storage.copy failed ({src_path} -> {dst_path}): {e}")

    # --- Byte copy fallback --------------------------------------------------
    read_bytes = getattr(storage, "read_bytes", None)
    write_bytes = getattr(storage, "write_bytes", None)
    if callable(read_bytes) and callable(write_bytes):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.write_bytes(dst_path, read_bytes(src_path))
            return True
        except Exception as e:
            logger.warning(f"[mirror] byte copy failed ({src_path} -> {dst_path}): {e}")

    return False


def _list_co_located_paths(storage, table_files_dir: str) -> Set[str]:
    """
    Return set of already present co-located file relative names as 'files/<name>'.

    Tries storage.ls -> storage.listdir -> storage.list_files (StorageInterface
    canonical method) so that standard backends always succeed.
    """
    rels: Set[str] = set()
    if hasattr(storage, "ls"):
        entries = storage.ls(table_files_dir) or []
    elif hasattr(storage, "listdir"):
        entries = [
            "/".join((table_files_dir.rstrip("/"), e))
            for e in (storage.listdir(table_files_dir) or [])
        ]
    elif hasattr(storage, "list_files"):
        entries = storage.list_files(table_files_dir, "*") or []
    else:
        raise RuntimeError(
            "Parquet mirroring requires a reliable directory-listing method"
        )
    for p in entries:
        fn = p.rstrip("/").split("/")[-1]
        if fn:
            rels.add("/".join(("files", fn)))
    return rels


def _co_locate_or_reuse_path(storage, table_files_dir: str, catalog_file_path: str) -> str:
    """
    Copy parquet into table_files_dir, return the relative path 'files/<hash>_<basename>'.
    Skips copy when the destination already exists from a prior mirror run.
    """
    base_name = catalog_file_path.rstrip("/").split("/")[-1]  # robust basename for URIs
    h = hashlib.md5(
        catalog_file_path.encode("utf-8"), usedforsecurity=False
    ).hexdigest()[:8]
    rel_name = f"{h}_{base_name}"
    rel_path = "/".join(("files", rel_name))
    dst_path = os.path.join(table_files_dir, rel_name)
    # Skip copy if destination already exists from a prior mirror run (perf)
    if hasattr(storage, "exists"):
        try:
            if storage.exists(dst_path):
                return rel_path
        except Exception:
            pass
    ok = _binary_copy_if_possible(storage, catalog_file_path, dst_path)
    if not ok:
        raise RuntimeError(f"Failed to copy data file into Parquet table dir: {catalog_file_path}")
    return rel_path


def write_parquet_table(super_table, table_name: str, simple_snapshot: Dict[str, Any]) -> None:
    """
    Materialize a latest-only Parquet mirror for `table_name`:

      base = <org>/<super>/parquet/<table>
        files/

    Uses `simple_snapshot["resources"]` with entries like:
      { "file": "<uri>", "file_size": <int>, "rows": <int>, ... }
    """
    base = os.path.join(super_table.organization, super_table.super_name, "parquet", table_name)
    files_dir = os.path.join(base, "files")

    # Ensure target dirs exist
    super_table.storage.makedirs(files_dir)

    resources: List[Dict[str, Any]] = simple_snapshot.get("resources", [])

    # Previously co-located files (relative 'files/<name>')
    prev_paths = _list_co_located_paths(super_table.storage, files_dir)

    # Build current file paths by co-locating under table dir
    current_paths: List[str] = []
    for r in resources:
        src_file = r["file"]
        used_rel_path = _co_locate_or_reuse_path(super_table.storage, files_dir, src_file)
        destination = os.path.join(base, used_rel_path)
        source_size, source_digest = super_table.storage.content_sha256(src_file)
        declared_size = int(r.get("file_size") or 0)
        if declared_size and source_size != declared_size:
            raise RuntimeError(
                f"Parquet mirror source size changed: {src_file!r}"
            )
        try:
            mirror_size, mirror_digest = (
                super_table.storage.content_sha256(destination)
            )
        except Exception:
            mirror_size, mirror_digest = -1, ""
        if mirror_size != source_size or mirror_digest != source_digest:
            # Existing-by-name is only a cache hint, never integrity evidence.
            # Repair a truncated/stale object and verify its visible bytes.
            if not _binary_copy_if_possible(
                super_table.storage, src_file, destination,
            ):
                raise RuntimeError(
                    f"Failed to repair Parquet mirror file: {src_file!r}"
                )
            mirror_size, mirror_digest = (
                super_table.storage.content_sha256(destination)
            )
        if mirror_size != source_size or mirror_digest != source_digest:
            raise RuntimeError(
                f"Parquet mirror copy failed its content seal: {src_file!r}"
            )
        current_paths.append(used_rel_path)

    current_set = set(current_paths)

    # Files to remove = those present before but not now
    to_remove = sorted(list(prev_paths - current_set))

    # Newly added files = those in current set but not previously present
    to_add = sorted(list(current_set - prev_paths))

    # If nothing changes, we're done
    if not to_remove and not to_add:
        logger.info(f"[mirror][parquet] no-op for '{table_name}' (no add/remove)")
        return

    # Delete obsolete co-located files (physically remove unused parquet from the mirror folder)
    for rp in to_remove:
        rel = rp
        if rel.startswith("abfss://"):
            rel = "/".join(("files", rel.rstrip("/").split("/")[-1]))
        if not rel.startswith("files/"):
            rel = "/".join(("files", rel.rstrip("/").split("/")[-1]))
        abs_path = os.path.join(base, rel)
        if hasattr(super_table.storage, "exists") and not super_table.storage.exists(abs_path):
            # Fallback: try raw filename under files_dir
            alt_abs = os.path.join(files_dir, rel.split("/")[-1])
            if super_table.storage.exists(alt_abs):
                abs_path = alt_abs
        super_table.storage.delete(abs_path)
        if (
            hasattr(super_table.storage, "exists")
            and super_table.storage.exists(abs_path)
        ):
            raise RuntimeError(
                f"Obsolete Parquet mirror file is still visible after delete: {abs_path}"
            )

    # Some storage adapters expose ``delete`` without ``exists`` and may
    # return success even when an object was not removed.  Reuse the same
    # mandatory listing primitive that produced ``prev_paths`` so a flat
    # Parquet mirror never reports success while obsolete row-bearing files
    # remain visible.
    remaining_obsolete = (
        set(to_remove) & _list_co_located_paths(super_table.storage, files_dir)
        if to_remove else set()
    )
    if remaining_obsolete:
        raise RuntimeError(
            "Obsolete Parquet mirror files remain visible after delete: "
            f"{sorted(remaining_obsolete)!r}"
        )

    logger.info(
        f"[mirror][parquet] updated '{table_name}' (files now={len(current_paths)}, removed={len(to_remove)})"
    )


def verify_parquet_table(
        super_table, table_name: str, simple_snapshot: Dict[str, Any],
) -> None:
    """Verify that the latest-only Parquet projection is exact."""
    files_dir = os.path.join(
        super_table.organization, super_table.super_name,
        "parquet", table_name, "files",
    )
    expected: Set[str] = set()
    source_by_relative: Dict[str, Dict[str, Any]] = {}
    for resource in simple_snapshot.get("resources", []) or []:
        source = str(resource.get("file") or "")
        if not source:
            continue
        digest = hashlib.md5(
            source.encode("utf-8"), usedforsecurity=False,
        ).hexdigest()[:8]
        relative = f"files/{digest}_{source.rstrip('/').split('/')[-1]}"
        expected.add(relative)
        source_by_relative[relative] = resource
    actual = _list_co_located_paths(super_table.storage, files_dir)
    if actual != expected:
        raise RuntimeError(
            "Parquet mirror does not match the committed snapshot: "
            f"actual={sorted(actual)!r}, expected={sorted(expected)!r}"
        )
    base = os.path.dirname(files_dir)
    for relative, resource in source_by_relative.items():
        source = str(resource["file"])
        source_size, source_digest = super_table.storage.content_sha256(source)
        mirror_size, mirror_digest = super_table.storage.content_sha256(
            os.path.join(base, relative)
        )
        declared_size = int(resource.get("file_size") or 0)
        if (
            (declared_size and source_size != declared_size)
            or mirror_size != source_size
            or mirror_digest != source_digest
        ):
            raise RuntimeError(
                f"Parquet mirrored artifact failed its content seal: "
                f"{relative!r}"
            )
