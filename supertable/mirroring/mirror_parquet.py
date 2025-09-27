# supertable/mirroring/mirror_parquet.py
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
    # Prefer native copy when available
    if hasattr(storage, "copy"):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.copy(src_path, dst_path)
            return True
        except Exception as e:
            logger.warning(f"[mirror][parquet] storage.copy failed ({src_path} -> {dst_path}): {e}")

    read_bytes = getattr(storage, "read_bytes", None)
    write_bytes = getattr(storage, "write_bytes", None)
    if callable(read_bytes) and callable(write_bytes):
        try:
            storage.makedirs(os.path.dirname(dst_path))
            storage.write_bytes(dst_path, read_bytes(src_path))
            return True
        except Exception as e:
            logger.warning(f"[mirror][parquet] byte copy failed ({src_path} -> {dst_path}): {e}")

    return False


def _list_co_located_paths(storage, table_files_dir: str) -> Set[str]:
    """
    Return set of already present co-located file relative names as 'files/<name>'.
    """
    rels: Set[str] = set()
    try:
        entries: List[str] = []
        if hasattr(storage, "ls"):
            entries = storage.ls(table_files_dir) or []
        elif hasattr(storage, "listdir"):
            entries = ["/".join((table_files_dir.rstrip("/"), e)) for e in (storage.listdir(table_files_dir) or [])]
        for p in entries:
            fn = p.rstrip("/").split("/")[-1]
            if fn:
                rels.add("/".join(("files", fn)))
    except Exception:
        pass
    return rels


def _co_locate_or_reuse_path(storage, table_files_dir: str, catalog_file_path: str) -> str:
    """
    Copy parquet into table_files_dir, return the relative path 'files/<hash>_<basename>'.
    """
    base_name = catalog_file_path.rstrip("/").split("/")[-1]  # robust basename for URIs
    h = hashlib.md5(catalog_file_path.encode("utf-8")).hexdigest()[:8]
    rel_name = f"{h}_{base_name}"
    rel_path = "/".join(("files", rel_name))
    dst_path = os.path.join(table_files_dir, rel_name)
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
        current_paths.append(used_rel_path)

    current_set = set(current_paths)

    # Files to remove = those present before but not now
    to_remove = sorted(list(prev_paths - current_set))

    # If nothing changes, we’re done
    if not to_remove and len(current_paths) == len(prev_paths):
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
        try:
            if hasattr(super_table.storage, "exists") and not super_table.storage.exists(abs_path):
                # Fallback: try raw filename under files_dir
                alt_abs = os.path.join(files_dir, rel.split("/")[-1])
                if super_table.storage.exists(alt_abs):
                    abs_path = alt_abs
            super_table.storage.delete(abs_path)
        except Exception as e:
            logger.warning(f"[mirror][parquet] failed to delete obsolete {rel}: {e}")

    logger.info(
        f"[mirror][parquet] updated '{table_name}' (files now={len(current_paths)}, removed={len(to_remove)})"
    )
