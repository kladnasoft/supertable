# supertable/engine/duckdb_pro.py

from __future__ import annotations

import os
import hashlib
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
    apply_runtime_pragmas,
    create_rbac_view,
    create_tombstone_view,
    rbac_view_name,
    TombstoneCache,
    create_typed_empty_view,
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
    # Stable logical survivor identity.  Prefer raw snapshot resource keys so
    # alternating predicates can reuse their exact views across requests.
    file_signature: str = ""
    # Exact concrete paths embedded in the DuckDB view.  Presigned URLs are
    # executable capabilities with an expiry, so a fresh URL for the same raw
    # resource must never reuse a view that still contains the old literal.
    resolved_signature: str = ""
    ref_count: int = 0      # number of in-flight queries using this view
    stale: bool = False      # marked for removal once ref_count hits 0


def _paths_signature(paths: List[str]) -> str:
    """Hash an ordered path list without retaining or logging URL secrets."""
    signature_hash = hashlib.sha256()
    for path in paths:
        encoded = str(path).encode("utf-8")
        signature_hash.update(len(encoded).to_bytes(8, "big"))
        signature_hash.update(encoded)
    return signature_hash.hexdigest()


def _view_signature(file_signature: str, resolved_signature: str) -> str:
    """Bind a Pro view name to both logical files and embedded path literals."""
    return _paths_signature([file_signature, resolved_signature])


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

        # Shared deletion-vector table cache: per-table eviction (idle TTL +
        # per-table version cap), bounded by config. Tables live on the
        # persistent connection and are forgotten when it resets.
        self._tombstone_cache = TombstoneCache(
            settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE,
            settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_TTL_SEC,
            settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_ENTRIES,
        )

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
        if not any(
            str(path).lower().startswith(("s3://", "s3a://", "http://", "https://"))
            for path in paths
        ):
            return
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
            # DV tables died with the connection — just forget the registry.
            self._tombstone_cache.clear_registry()
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
            column_types: Optional[Dict[str, str]] = None,
            resource_keys: Optional[List[str]] = None,
    ) -> str:
        """
        Ensure a reflection VIEW exists for (super, simple, version).
        Returns the DuckDB view name to use.

        A cached view is reusable only when the catalog version, logical
        survivor set, and exact concrete paths embedded in the view all match.
        The latter matters for presigned URLs: their raw resource keys stay
        stable while the executable URL rotates or expires.
        """
        key = (super_name, simple_name)
        identity_paths = (
            resource_keys
            if resource_keys and len(resource_keys) == len(files)
            else files
        )
        file_signature = _paths_signature(identity_paths)
        requested_resolved_signature = _paths_signature(files)
        entries = self._registry.get(key, [])

        # Search all retained survivor signatures, not only the newest one:
        # alternating predicates at one version deliberately keep a small MRU
        # set.  Concrete-path equality is mandatory because the view stores
        # those literals and DuckDB will keep using them after a presign rotates.
        for entry in reversed(entries):
            if (
                not entry.stale
                and entry.version == version
                and entry.file_signature == file_signature
                and entry.resolved_signature == requested_resolved_signature
            ):
                return entry.table_name

        # Give each concrete path generation its own ownership boundary.  This
        # avoids CREATE OR REPLACE touching an in-flight view when a presigned
        # URL rotates but the raw key and catalog version remain unchanged.
        view_name = pro_table_name(
            super_name,
            simple_name,
            version,
            file_signature=_view_signature(
                file_signature, requested_resolved_signature,
            ),
        )

        # A generated identifier may already be owned by a stale/in-flight view
        # (or a test may force a collision).  Never CREATE OR REPLACE it.
        for entry in entries:
            if entry.table_name != view_name:
                continue
            if (
                not entry.stale
                and entry.version == version
                and entry.file_signature == file_signature
                and entry.resolved_signature == requested_resolved_signature
            ):
                return entry.table_name
            # The generated identifier is already owned by another exact
            # signature (possible under monkeypatching/corruption and formerly
            # under the truncated hash). Allocate a private name; never replace
            # an in-flight view or reuse its wrong survivor set.
            view_name = f"{view_name}_{_uuid.uuid4().hex}"
            break

        # A table version may have several logical survivor signatures as WHERE
        # predicates alternate.  Retain those views for reuse.  Concrete path
        # generations of the *same* survivor set are replacements, however:
        # mark the old generation stale while preserving it until in-flight
        # readers release their reference.
        for entry in entries:
            replaced_resolution = (
                entry.version == version
                and entry.file_signature == file_signature
                and entry.resolved_signature != requested_resolved_signature
            )
            if not entry.stale and (
                entry.version != version or replaced_resolution
            ):
                entry.stale = True
                logger.debug(
                    f"{log_prefix}[duckdb.pro] marked stale: {entry.table_name} "
                    f"(v{entry.version}, refs={entry.ref_count})"
                )

        # Create new lazy view — no data is read from remote storage here.
        # DuckDB will apply projection and predicate pushdown at query time.
        embedded_files = files
        self._ensure_httpfs(con, embedded_files)
        try:
            if embedded_files:
                create_reflection_view(
                    con, view_name, embedded_files, resource_keys=resource_keys,
                )
            else:
                create_typed_empty_view(con, view_name, dict(column_types or {}))
        except Exception as e:
            msg = str(e)
            if any(tok in msg for tok in (
                    "HTTP Error", "HTTP GET error", "301", "Moved Permanently",
                    "AccessDenied", "SignatureDoesNotMatch", "403", "400",
            )):
                logger.warning(f"{log_prefix}[duckdb.pro] presign fallback for {view_name}: {msg}")
                presigned_files = make_presigned_list(self.storage, files)
                self._ensure_httpfs(con, presigned_files)
                create_reflection_view(
                    con, view_name, presigned_files, resource_keys=resource_keys,
                )
                embedded_files = presigned_files
            else:
                raise

        new_entry = _ProCacheEntry(
            table_name=view_name,
            super_name=super_name,
            simple_name=simple_name,
            version=version,
            file_signature=file_signature,
            resolved_signature=_paths_signature(embedded_files),
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

        # Bound alternate survivor signatures within one catalog version.  An
        # old predicate view is cheap but not free; keep a small MRU working
        # set and never evict an in-flight view.
        live_same_version = [
            entry for entry in self._registry.get(key, [])
            if not entry.stale and entry.version == version
        ]
        max_signatures = 8
        if len(live_same_version) > max_signatures:
            excess = len(live_same_version) - max_signatures
            for entry in live_same_version:
                if excess <= 0:
                    break
                if entry is not new_entry and entry.ref_count == 0:
                    entry.stale = True
                    excess -= 1
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
            engine_config=None,
    ) -> pd.DataFrame:
        return self._execute_serialized(
            reflection=reflection,
            parser=parser,
            query_manager=query_manager,
            timer_capture=timer_capture,
            log_prefix=log_prefix,
            engine_config=engine_config,
        )

    def _execute_serialized(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
            engine_config=None,
    ) -> pd.DataFrame:
        tables_used: Set[str] = set()

        with self._lock:
            try:
                root_con = self._get_connection(temp_dir=query_manager.temp_dir)
            except Exception:
                # Connection corrupted — reset and retry once
                self._reset_connection()
                root_con = self._get_connection(temp_dir=query_manager.temp_dir)

            # Each request gets an independent result handle while sharing the
            # persistent catalog/cache.  Using the root connection directly
            # lets concurrent execute()/fetchdf() calls swap result state;
            # serialising all queries would fix that but destroy concurrency.
            con = root_con.cursor()

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
                    column_types=dict(getattr(sup, "column_types", {}) or {}),
                    resource_keys=list(getattr(sup, "resource_keys", ()) or ()),
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
        tombstone_view_names: List[str] = []
        # Deletion-vector cache keys acquired this query — released in finally.
        acquired_dv_keys: List[str] = []
        try:
            query_alias_to_name = dict(alias_to_table_name)
            # Per-query suffix so concurrent queries never collide on a shared
            # view name (CREATE OR REPLACE would corrupt a sibling's view).
            # Request-private DDL must retain the full UUID.  A 32-bit suffix
            # can collide and CREATE OR REPLACE another in-flight tombstone or
            # RBAC view, changing its rows rather than merely its cache hit rate.
            query_suffix = _uuid.uuid4().hex

            # Tombstone / system-column view — created for EVERY alias so the
            # system columns (__rowid__, __timestamp__) are always stripped and
            # the deletion-vector (when present) is anti-joined out.  Built on
            # the reflection view directly, before RBAC.
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            for alias in list(query_alias_to_name.keys()):
                source = query_alias_to_name[alias]
                tomb_def = tombstone_views.get(alias)
                view = f"tomb_{source}_{query_suffix}"
                # Reuse a materialised deletion-vector table when the cache is
                # enabled and the alias has a stable key; otherwise fall back to
                # the inline read_parquet path (dv_table=None). All DDL — the DV
                # CREATE TABLE inside acquire() and the view creation — runs
                # under the connection lock, matching Pro's serialised model.
                cache_key = getattr(tomb_def, "cache_key", None) if tomb_def else None
                tomb_path = getattr(tomb_def, "tombstone_path", None) if tomb_def else None
                expected_rows = getattr(tomb_def, "expected_rows", None) if tomb_def else None
                expected_digest = getattr(tomb_def, "tombstone_digest", None) if tomb_def else None
                with self._lock:
                    dv_table = self._tombstone_cache.acquire(
                        con, cache_key, tomb_path, expected_rows=expected_rows,
                        expected_digest=expected_digest,
                    )
                    if dv_table:
                        acquired_dv_keys.append(cache_key)
                    create_tombstone_view(con, source, view, tomb_def, dv_table=dv_table)
                tombstone_view_names.append(view)
                query_alias_to_name[alias] = view

            # RBAC views (column + row filtering) on top of the stripped data.
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            if rbac_views:
                for alias in list(query_alias_to_name.keys()):
                    view_def = rbac_views.get(alias)
                    if view_def:
                        source = query_alias_to_name[alias]
                        view = f"rbac_{source}_{query_suffix}"
                        with self._lock:
                            create_rbac_view(con, source, view, view_def)
                        rbac_view_names.append(view)
                        query_alias_to_name[alias] = view

            executing_query = rewrite_query_with_hashed_tables(
                parser.original_query, query_alias_to_name,
            )
            parser.executing_query = executing_query

            logger.debug(f"{log_prefix}[duckdb.pro] executing: {executing_query}")

            # Re-apply live engine config (memory/threads/http/cache) so UI
            # changes take effect on this persistent connection per query.
            apply_runtime_pragmas(con, engine_config)

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

            # Release deletion-vector refs now their views are gone; this may
            # evict + DROP unreferenced DV tables over capacity.
            if acquired_dv_keys:
                with self._lock:
                    for cache_key in acquired_dv_keys:
                        try:
                            self._tombstone_cache.release(con, cache_key)
                        except Exception:
                            pass

            # Release refs and drop stale tables
            with self._lock:
                self._release_refs(tables_used)
                self._drop_unreferenced_stale(con, log_prefix)
            try:
                con.close()
            except Exception:
                pass

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
                        "file_signature": entry.file_signature,
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
