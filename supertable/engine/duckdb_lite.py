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
    apply_runtime_pragmas,
    create_rbac_view,
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
            engine_config=None,
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

        for td in table_defs:
            key = (td.super_name, td.simple_name)
            sup = snapshots_by_key.get(key)
            if not sup:
                continue

            cols = list(td.columns or [])

            # When specific columns are requested, also pull the system
            # columns (__rowid__/__timestamp__) so the tombstone view can
            # anti-join on __rowid__ and then statically EXCLUDE both from
            # the output. Every written file carries both columns, so they
            # are always threaded in. A bare SELECT * (cols == []) already
            # carries them.
            if cols:
                lower = {x.lower() for x in cols}
                for c in ("__rowid__", "__timestamp__"):
                    if c not in lower:
                        cols.append(c)

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

            # Per-query suffix so concurrent requests on the same table do not
            # collide on a shared view name (CREATE OR REPLACE would silently
            # corrupt a sibling query's view mid-execution).
            query_suffix = _uuid.uuid4().hex[:8]
            query_alias_to_name = dict(alias_to_table_name)

            # Tombstone / system-column view — created for EVERY alias so the
            # system columns (__rowid__, __timestamp__) are always stripped and
            # the deletion-vector (when present) is anti-joined out.  Sits on
            # the reflection table directly, before RBAC.
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            for alias in list(query_alias_to_name.keys()):
                source = query_alias_to_name[alias]
                tomb_def = tombstone_views.get(alias)
                view = f"tomb_{source}_{query_suffix}"
                create_tombstone_view(con, source, view, tomb_def)
                created_views.append(view)
                query_alias_to_name[alias] = view

            # RBAC views (column + row filtering) on top of stripped data.
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            if rbac_views:
                for alias in list(query_alias_to_name.keys()):
                    view_def = rbac_views.get(alias)
                    if view_def:
                        source = query_alias_to_name[alias]
                        view = f"rbac_{source}_{query_suffix}"
                        create_rbac_view(con, source, view, view_def)
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

            # Re-apply live engine config (memory/threads/http/cache) so UI
            # changes take effect on this persistent connection per query.
            apply_runtime_pragmas(con, engine_config)

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

