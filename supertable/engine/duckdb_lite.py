# supertable/engine/duckdb_lite.py

from __future__ import annotations

import threading
import uuid as _uuid
from typing import Optional, List

import duckdb
import pandas as pd

from supertable.config.defaults import logger
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.data_classes import Reflection

from supertable.engine.engine_common import (
    hashed_table_name,
    configure_httpfs_and_s3,
    create_reflection_table_with_presign_retry,
    create_reflection_view_with_presign_retry,
    rewrite_query_with_hashed_tables,
    init_connection,
    create_rbac_view,
    create_dedup_view,
    create_tombstone_view,
)


class DuckDBLite:
    """
    Per-query DuckDB executor backed by a single persistent connection.

    The connection is created once (lazily) and reused across all queries so
    that DuckDB's HTTP metadata cache, external file cache, and httpfs
    configuration survive between requests.  This eliminates the per-query
    overhead of re-fetching parquet footer metadata from remote storage.

    Query isolation is preserved: VIEWs are created with unique names and
    dropped in the finally block after each query.  No materialised TABLE
    state is retained between queries (contrast with DuckDBPro).

    Cache layers (innermost to outermost):
      1. DuckDB external file cache  -- disk-level data block cache (DuckDB >= 1.3)
      2. DuckDB HTTP metadata cache  -- connection-level parquet footer cache
      3. ParquetMetadataCache        -- module-level Python dict, version-aware

    Thread safety:
      A lock guards connection creation and httpfs initialisation only.
      DuckDB allows concurrent reads on the same connection so query execution
      runs outside the lock.
    """

    def __init__(self, storage: Optional[object] = None):
        self.storage = storage
        self._lock = threading.Lock()
        self._con: Optional[duckdb.DuckDBPyConnection] = None
        self._httpfs_configured = False

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _get_connection(self, temp_dir: str) -> duckdb.DuckDBPyConnection:
        """Return the persistent connection, creating and configuring it once."""
        if self._con is not None:
            return self._con

        con = duckdb.connect()
        init_connection(con, temp_dir=temp_dir)
        # httpfs (and both cache settings) are configured lazily on the first
        # query via _ensure_httpfs → configure_httpfs_and_s3.  They cannot be
        # applied here because the httpfs extension is not loaded yet.
        self._con = con
        self._httpfs_configured = False
        logger.info("[duckdb.lite] persistent connection created")
        return con

    def _ensure_httpfs(self, con: duckdb.DuckDBPyConnection, paths: List[str]) -> None:
        """Configure httpfs once per connection lifetime, under the lock."""
        with self._lock:
            if not self._httpfs_configured:
                configure_httpfs_and_s3(con, paths)
                self._httpfs_configured = True

    def _reset_connection(self) -> None:
        """Close and discard the connection on unrecoverable error."""
        if self._con is not None:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None
            self._httpfs_configured = False
            logger.warning("[duckdb.lite] connection reset")

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def execute(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
    ) -> pd.DataFrame:
        tried_presign = False

        with self._lock:
            try:
                con = self._get_connection(temp_dir=query_manager.temp_dir)
            except Exception:
                self._reset_connection()
                con = self._get_connection(temp_dir=query_manager.temp_dir)

        timer_capture("CONNECTING")

        snapshots_by_key = {
            (sup.super_name, sup.simple_name): sup
            for sup in reflection.supers
        }
        table_defs = parser.get_table_tuples()

        alias_to_table_name = {}
        alias_to_files = {}
        alias_to_columns = {}

        dedup_views = getattr(reflection, "dedup_views", None) or {}

        for td in table_defs:
            key = (td.super_name, td.simple_name)
            sup = snapshots_by_key.get(key)
            if not sup:
                continue

            cols = list(td.columns or [])

            # Dedup views require PK + order columns even when not SELECTed.
            dedup_def = dedup_views.get(td.alias)
            if dedup_def and cols:
                extra = []
                for c in dedup_def.primary_keys:
                    if c.lower() not in {x.lower() for x in cols}:
                        extra.append(c)
                if dedup_def.order_column.lower() not in {x.lower() for x in cols}:
                    extra.append(dedup_def.order_column)
                cols = cols + extra

            name = hashed_table_name(
                sup.super_name, sup.simple_name, sup.simple_version, cols,
            )
            alias_to_table_name[td.alias] = name
            alias_to_files[td.alias] = list(sup.files)
            alias_to_columns[td.alias] = cols

        # Ensure httpfs is configured on the persistent connection (once only).
        all_files = [f for files in alias_to_files.values() for f in files]
        self._ensure_httpfs(con, all_files)

        # Create per-query VIEWs. Dropped in finally regardless of outcome.
        created_views: List[str] = []
        try:
            for alias, table_name in alias_to_table_name.items():
                files = alias_to_files[alias]
                cols = alias_to_columns[alias]

                # Use VIEW (lazy, default). Set SUPERTABLE_DUCKDB_MATERIALIZE=table to revert.
                used_presign = create_reflection_view_with_presign_retry(
                    con, self.storage, table_name, files, cols, log_prefix,
                )
                created_views.append(table_name)
                if used_presign:
                    tried_presign = True

            timer_capture("CREATING_REFLECTION")

            # RBAC views
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            query_alias_to_name = dict(alias_to_table_name)
            if rbac_views:
                # Use a per-query suffix so concurrent requests on the same table
                # do not collide on the same view name (CREATE OR REPLACE would
                # silently corrupt the sibling query's view mid-execution).
                query_suffix = _uuid.uuid4().hex[:8]
                for alias, table_name in alias_to_table_name.items():
                    view_def = rbac_views.get(alias)
                    if view_def:
                        view = f"rbac_{table_name}_{query_suffix}"
                        create_rbac_view(con, table_name, view, view_def)
                        created_views.append(view)
                        query_alias_to_name[alias] = view

            # Tombstone views (soft-delete filtering)
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            if tombstone_views:
                if not rbac_views:
                    query_suffix = _uuid.uuid4().hex[:8]
                for alias in list(query_alias_to_name.keys()):
                    tomb_def = tombstone_views.get(alias)
                    if tomb_def and tomb_def.deleted_keys:
                        source = query_alias_to_name[alias]
                        view = f"tomb_{source}_{query_suffix}"
                        create_tombstone_view(con, source, view, tomb_def)
                        created_views.append(view)
                        query_alias_to_name[alias] = view

            # Dedup views
            if dedup_views:
                if not rbac_views and not tombstone_views:
                    query_suffix = _uuid.uuid4().hex[:8]
                for alias in list(query_alias_to_name.keys()):
                    dedup_def = dedup_views.get(alias)
                    if dedup_def:
                        source = query_alias_to_name[alias]
                        view = f"dedup_{source}_{query_suffix}"
                        create_dedup_view(con, source, view, dedup_def)
                        created_views.append(view)
                        query_alias_to_name[alias] = view

            executing_query = rewrite_query_with_hashed_tables(
                parser.original_query, query_alias_to_name,
            )
            parser.executing_query = executing_query

            # Profiling PRAGMAs are connection-level state.  Under concurrent
            # queries the last SET wins — one query's profile may land in the
            # wrong file.  This is acceptable: profiling is best-effort
            # diagnostics, and query_plan_path is already unique per query
            # (contains query_id) so profiles never overwrite on disk.
            # A lock here would serialise execution on a shared connection
            # and destroy DuckDB's concurrent-read capability.
            try:
                con.execute("PRAGMA enable_profiling='json';")
                con.execute(f"PRAGMA profile_output='{query_manager.query_plan_path}';")
            except Exception:
                pass

            logger.debug(f"{log_prefix}[duckdb.lite] executing: {executing_query}")
            result = con.execute(executing_query).fetchdf()

            if tried_presign:
                logger.debug(f"{log_prefix}[duckdb.lite] presigned fallback succeeded")

            return result

        finally:
            # Disable profiling so cleanup DDL is not captured.
            try:
                con.execute("PRAGMA disable_profiling;")
            except Exception:
                pass
            # Drop all per-query VIEWs in reverse creation order.
            for view in reversed(created_views):
                try:
                    con.execute(f"DROP VIEW IF EXISTS {view};")
                except Exception:
                    pass

