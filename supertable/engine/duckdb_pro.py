# supertable/engine/duckdb_pro.py

from __future__ import annotations

import os
import threading
import uuid as _uuid
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import duckdb
import pandas as pd

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.data_classes import Reflection

from supertable.engine.engine_common import (
    pro_table_name,
    hashed_table_name,
    configure_httpfs_and_s3,
    create_reflection_view,
    make_presigned_list,
    rewrite_query_with_hashed_tables,
    init_connection,
    create_rbac_view,
    create_dedup_view,
    create_tombstone_view,
    rbac_view_name,
)


# =========================================================
# Table registry entry
# =========================================================

@dataclass
class _ProCacheEntry:
    """Tracks a single cached reflection view."""
    table_name: str          # DuckDB view name (e.g. pro_a3f8c1_v5)
    super_name: str
    simple_name: str
    version: int
    ref_count: int = 0      # number of in-flight queries using this view
    stale: bool = False      # marked for removal once ref_count hits 0


# =========================================================
# Pro executor (singleton connection + table cache)
# =========================================================

class DuckDBPro:
    """
    Persistent DuckDB executor with version-based reflection view caching.

    Views are created on first access and reused across queries as long as
    the data version is unchanged.  Because views are lazy (no data is
    materialised at creation time), DuckDB applies full projection and
    predicate pushdown on every query — only the columns and row groups
    actually needed are read from remote storage.  Repeated reads of the
    same row groups are served from the external file cache (disk) or the
    HTTP metadata cache (parquet footer, in-memory), both configured in
    configure_httpfs_and_s3.

    When a new version is detected, a new view is created alongside the old
    one.  Old views are dropped as soon as their reference count reaches zero.

    Thread-safe: all DDL and registry mutations are guarded by a lock.
    """

    def __init__(self, storage: Optional[object] = None):
        self.storage = storage
        self._lock = threading.Lock()
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._httpfs_configured = False

        # Registry: (super_name, simple_name) → list of _ProCacheEntry
        # Multiple entries per key when old version still has in-flight queries.
        self._registry: Dict[Tuple[str, str], List[_ProCacheEntry]] = {}

        # Temp dir for spill — set on first query
        self._temp_dir: Optional[str] = None

    # ---------------------------------------------------------
    # Connection lifecycle
    # ---------------------------------------------------------

    def _get_connection(self, temp_dir: str) -> duckdb.DuckDBPyConnection:
        """Return the persistent connection, creating it if needed."""
        if self._con is not None:
            return self._con

        self._temp_dir = temp_dir
        # Memory limit is shared with the transient executor via the single
        # SUPERTABLE_DUCKDB_MEMORY_LIMIT env var.  The "1GB" fallback is only
        # used when the env var is absent.
        memory_limit = settings.SUPERTABLE_DUCKDB_MEMORY_LIMIT
        con = duckdb.connect()
        init_connection(con, temp_dir=temp_dir, memory_limit=memory_limit)
        # httpfs (and both cache settings) are configured lazily on the first
        # query via _ensure_httpfs → configure_httpfs_and_s3.  They cannot be
        # applied here because the httpfs extension is not loaded yet.
        self._con = con
        self._httpfs_configured = False
        logger.info("[duckdb.pro] persistent connection created")
        return con

    def _ensure_httpfs(self, con: duckdb.DuckDBPyConnection, paths: List[str]) -> None:
        """Configure httpfs once per connection lifetime."""
        if not self._httpfs_configured:
            configure_httpfs_and_s3(con, paths)
            self._httpfs_configured = True

    def _reset_connection(self) -> None:
        """Close and discard the connection (e.g. on unrecoverable error)."""
        if self._con is not None:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None
            self._httpfs_configured = False
            self._registry.clear()
            logger.warning("[duckdb.pro] connection reset — all cached views lost")

    # ---------------------------------------------------------
    # Table registry management
    # ---------------------------------------------------------

    def _current_entry(self, key: Tuple[str, str]) -> Optional[_ProCacheEntry]:
        """Return the latest (non-stale) entry for a table key, if any."""
        entries = self._registry.get(key, [])
        for entry in reversed(entries):
            if not entry.stale:
                return entry
        return None

    def _ensure_view(
            self,
            con: duckdb.DuckDBPyConnection,
            super_name: str,
            simple_name: str,
            version: int,
            files: List[str],
            log_prefix: str = "",
    ) -> str:
        """
        Ensure a reflection VIEW exists for (super, simple, version).
        Returns the DuckDB view name to use.

        If the version matches the cached entry, returns it immediately —
        the view is already registered on the connection and costs nothing.
        Otherwise creates a new lazy view (no data read at creation time)
        and marks old entries as stale for deferred cleanup.
        """
        key = (super_name, simple_name)
        current = self._current_entry(key)

        if current is not None and current.version == version:
            return current.table_name

        # New version needed
        view_name = pro_table_name(super_name, simple_name, version)

        # Check if this exact view already exists (e.g. race between threads)
        entries = self._registry.get(key, [])
        for entry in entries:
            if entry.table_name == view_name and not entry.stale:
                return entry.table_name

        # Mark all existing entries for this key as stale.
        for entry in entries:
            if not entry.stale:
                entry.stale = True
                logger.debug(
                    f"{log_prefix}[duckdb.pro] marked stale: {entry.table_name} "
                    f"(v{entry.version}, refs={entry.ref_count})"
                )

        # Create new lazy view — no data is read from remote storage here.
        # DuckDB will apply projection and predicate pushdown at query time.
        self._ensure_httpfs(con, files)
        try:
            create_reflection_view(con, view_name, files)
        except Exception as e:
            msg = str(e)
            if any(tok in msg for tok in (
                    "HTTP Error", "HTTP GET error", "301", "Moved Permanently",
                    "AccessDenied", "SignatureDoesNotMatch", "403", "400",
            )):
                logger.warning(f"{log_prefix}[duckdb.pro] presign fallback for {view_name}: {msg}")
                presigned_files = make_presigned_list(self.storage, files)
                self._ensure_httpfs(con, presigned_files)
                create_reflection_view(con, view_name, presigned_files)
            else:
                raise

        new_entry = _ProCacheEntry(
            table_name=view_name,
            super_name=super_name,
            simple_name=simple_name,
            version=version,
        )

        if key not in self._registry:
            self._registry[key] = []
        self._registry[key].append(new_entry)

        logger.info(
            f"{log_prefix}[duckdb.pro] created view {view_name} "
            f"(super={super_name}, simple={simple_name}, v{version}, files={len(files)})"
        )

        # Eagerly drop stale views with zero refs
        self._drop_unreferenced_stale(con, log_prefix)

        return view_name

    def _acquire_refs(self, table_names: Set[str]) -> None:
        """Increment ref_count for each table being used by a query."""
        for entries in self._registry.values():
            for entry in entries:
                if entry.table_name in table_names:
                    entry.ref_count += 1

    def _release_refs(self, table_names: Set[str]) -> None:
        """Decrement ref_count for each table after query completes."""
        for entries in self._registry.values():
            for entry in entries:
                if entry.table_name in table_names:
                    entry.ref_count = max(0, entry.ref_count - 1)

    def _drop_unreferenced_stale(
            self, con: duckdb.DuckDBPyConnection, log_prefix: str = ""
    ) -> None:
        """DROP all stale views with ref_count == 0."""
        for key, entries in list(self._registry.items()):
            to_keep = []
            for entry in entries:
                if entry.stale and entry.ref_count == 0:
                    try:
                        con.execute(f"DROP VIEW IF EXISTS {entry.table_name};")
                        logger.info(
                            f"{log_prefix}[duckdb.pro] dropped stale view: {entry.table_name} (v{entry.version})"
                        )
                    except Exception as e:
                        logger.warning(
                            f"{log_prefix}[duckdb.pro] failed to drop view {entry.table_name}: {e}"
                        )
                        to_keep.append(entry)
                else:
                    to_keep.append(entry)
            if to_keep:
                self._registry[key] = to_keep
            else:
                del self._registry[key]

    # ---------------------------------------------------------
    # Core execution
    # ---------------------------------------------------------

    def execute(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
    ) -> pd.DataFrame:
        tables_used: Set[str] = set()

        with self._lock:
            try:
                con = self._get_connection(temp_dir=query_manager.temp_dir)
            except Exception:
                # Connection corrupted — reset and retry once
                self._reset_connection()
                con = self._get_connection(temp_dir=query_manager.temp_dir)

            timer_capture("CONNECTING")

            # Resolve tables
            snapshots_by_key = {
                (sup.super_name, sup.simple_name): sup
                for sup in reflection.supers
            }
            table_defs = parser.get_table_tuples()
            alias_to_table_name = {}

            for td in table_defs:
                key = (td.super_name, td.simple_name)
                sup = snapshots_by_key.get(key)
                if not sup:
                    continue

                table_name = self._ensure_view(
                    con, sup.super_name, sup.simple_name,
                    sup.simple_version, list(sup.files), log_prefix,
                )
                alias_to_table_name[td.alias] = table_name
                tables_used.add(table_name)

            # Acquire refs while still under lock
            self._acquire_refs(tables_used)

        timer_capture("CREATING_REFLECTION")

        # Both lists declared before the try block so the finally clause can
        # always reference them, even if an exception fires before the inner
        # assignments are reached (which would cause a NameError otherwise).
        rbac_view_names: List[str] = []
        dedup_view_names: List[str] = []
        tombstone_view_names: List[str] = []
        try:
            # Create per-query RBAC views if filtering is required
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            query_alias_to_name = dict(alias_to_table_name)

            if rbac_views:
                # RBAC views are per-query (role-specific), not cached.
                # Use a unique suffix to avoid collisions between concurrent queries.
                query_suffix = _uuid.uuid4().hex[:8]
                for alias, table_name in alias_to_table_name.items():
                    view_def = rbac_views.get(alias)
                    if view_def:
                        view = f"rbac_{table_name}_{query_suffix}"
                        with self._lock:
                            create_rbac_view(con, table_name, view, view_def)
                        rbac_view_names.append(view)
                        query_alias_to_name[alias] = view

            # Create per-query tombstone views if soft-deleted keys exist
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            if tombstone_views:
                if not rbac_views:
                    query_suffix = _uuid.uuid4().hex[:8]
                for alias in list(query_alias_to_name.keys()):
                    tomb_def = tombstone_views.get(alias)
                    if tomb_def and tomb_def.deleted_keys:
                        source = query_alias_to_name[alias]
                        view = f"tomb_{source}_{query_suffix}"
                        with self._lock:
                            create_tombstone_view(con, source, view, tomb_def)
                        tombstone_view_names.append(view)
                        query_alias_to_name[alias] = view

            # Create per-query dedup views if dedup-on-read is configured
            dedup_views = getattr(reflection, "dedup_views", None) or {}
            if dedup_views:
                if not rbac_views and not tombstone_views:
                    query_suffix = _uuid.uuid4().hex[:8]
                for alias in list(query_alias_to_name.keys()):
                    dedup_def = dedup_views.get(alias)
                    if dedup_def:
                        source = query_alias_to_name[alias]
                        view = f"dedup_{source}_{query_suffix}"
                        with self._lock:
                            create_dedup_view(con, source, view, dedup_def)
                        dedup_view_names.append(view)
                        query_alias_to_name[alias] = view

            executing_query = rewrite_query_with_hashed_tables(
                parser.original_query, query_alias_to_name,
            )
            parser.executing_query = executing_query

            logger.debug(f"{log_prefix}[duckdb.pro] executing: {executing_query}")

            # Profiling PRAGMAs are connection-level state.  Under concurrent
            # queries the last SET wins — one query's profile may land in the
            # wrong file.  This is acceptable: profiling is best-effort
            # diagnostics, and query_plan_path is already unique per query
            # (contains query_id) so profiles never overwrite on disk.
            try:
                con.execute("PRAGMA enable_profiling='json';")
                con.execute(f"PRAGMA profile_output='{query_manager.query_plan_path}';")
            except Exception:
                pass

            result = con.execute(executing_query).fetchdf()
            return result

        finally:
            # Disable profiling so cleanup DDL is not captured.
            try:
                con.execute("PRAGMA disable_profiling;")
            except Exception:
                pass
            # Drop per-query RBAC views
            if rbac_view_names:
                with self._lock:
                    for view in rbac_view_names:
                        try:
                            con.execute(f"DROP VIEW IF EXISTS {view};")
                        except Exception:
                            pass

            # Drop per-query tombstone views
            if tombstone_view_names:
                with self._lock:
                    for view in tombstone_view_names:
                        try:
                            con.execute(f"DROP VIEW IF EXISTS {view};")
                        except Exception:
                            pass

            # Drop per-query dedup views
            if dedup_view_names:
                with self._lock:
                    for view in dedup_view_names:
                        try:
                            con.execute(f"DROP VIEW IF EXISTS {view};")
                        except Exception:
                            pass

            # Release refs and drop stale tables
            with self._lock:
                self._release_refs(tables_used)
                self._drop_unreferenced_stale(con, log_prefix)

    # ---------------------------------------------------------
    # Diagnostics
    # ---------------------------------------------------------

    def get_cached_tables(self) -> List[Dict]:
        """Return a snapshot of the view registry for diagnostics."""
        with self._lock:
            result = []
            for entries in self._registry.values():
                for entry in entries:
                    result.append({
                        "view_name": entry.table_name,
                        "super_name": entry.super_name,
                        "simple_name": entry.simple_name,
                        "version": entry.version,
                        "ref_count": entry.ref_count,
                        "stale": entry.stale,
                    })
            return result

    def drop_all(self) -> None:
        """Drop all cached views and reset the connection. For testing/shutdown."""
        with self._lock:
            if self._con is not None:
                for entries in self._registry.values():
                    for entry in entries:
                        try:
                            self._con.execute(f"DROP VIEW IF EXISTS {entry.table_name};")
                        except Exception:
                            pass
            self._reset_connection()
