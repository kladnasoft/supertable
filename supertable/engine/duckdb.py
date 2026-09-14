# supertable/engine/duckdb.py

from __future__ import annotations

import threading
import uuid as _uuid
from typing import Optional, List

import duckdb

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.data_classes import Reflection
from supertable.processing import ROWID_COL as ROWID_SYSTEM_COL, TIMESTAMP_COL as TIMESTAMP_SYSTEM_COL

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
    widen_projection_for_row_filter,
    TombstoneCache,
)


# Shared per-thread engine state.
#
# `data_reader` builds a fresh Executor — and therefore a fresh DuckDBEngine —
# for every query, which is fine in itself: the engine object is cheap. What is
# NOT fine is holding the expensive, reusable state on that object. Cached on
# `self`, the connection was rebuilt on every single query (five queries, five
# connections, while the log said "persistent connection created" each time)
# at a measured 51.6ms/query — 18% of read wall — and the deletion-vector
# table cache was thrown away with it.
#
# So the state lives here instead, keyed per thread. Thread-local rather than
# process-global because a DuckDB connection is not safe to share across
# threads; this mirrors the write path's pooled probe connection. The stats
# artifact cache already works this way (a module-level cache in processing),
# which is why it never had this problem.
_SHARED = threading.local()


def _shared_state():
    """Per-thread holder for the connection and the deletion-vector cache."""
    st = getattr(_SHARED, "state", None)
    if st is None:
        st = {"con": None, "httpfs": False, "tombstone_cache": None}
        _SHARED.state = st
    return st


def reset_shared_duckdb_state() -> None:
    """Drop this thread's connection and caches (tests / eviction hook)."""
    st = _shared_state()
    con = st.get("con")
    st["con"] = None
    st["httpfs"] = False
    st["tombstone_cache"] = None
    if con is not None:
        try:
            con.close()
        except Exception:
            pass


class DuckDBEngine:
    """
    Per-query DuckDB executor backed by a single persistent connection.

    The connection is created once (lazily) and reused across all queries so
    that DuckDB's HTTP metadata cache, external file cache, and httpfs
    configuration survive between requests.  This eliminates the per-query
    overhead of re-fetching parquet footer metadata from remote storage.

    Query isolation is preserved: VIEWs are created with unique names and
    dropped in the finally block after each query.  No materialised TABLE
    state is retained between queries.

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
        # Shared deletion-vector table cache: per-table eviction (idle TTL +
        # per-table version cap), bounded by config. Tables live on the
        # persistent connection and are forgotten when it resets.
        st = _shared_state()
        if st["tombstone_cache"] is None:
            st["tombstone_cache"] = TombstoneCache(
                settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE,
                settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_TTL_SEC,
            )
        self._tombstone_cache = st["tombstone_cache"]

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    def _get_connection(self, temp_dir: str) -> duckdb.DuckDBPyConnection:
        """Return the persistent connection, creating and configuring it once."""
        st = _shared_state()
        if st["con"] is not None:
            return st["con"]

        con = duckdb.connect()
        init_connection(con, temp_dir=temp_dir)
        # httpfs (and both cache settings) are configured lazily on the first
        # query via _ensure_httpfs → configure_httpfs_and_s3.  They cannot be
        # applied here because the httpfs extension is not loaded yet.
        st["con"] = con
        st["httpfs"] = False
        logger.info("[duckdb] persistent connection created")
        return con

    def _ensure_httpfs(self, con: duckdb.DuckDBPyConnection, paths: List[str]) -> None:
        """Configure httpfs once per connection lifetime, under the lock."""
        st = _shared_state()
        with self._lock:
            if not st["httpfs"]:
                configure_httpfs_and_s3(con, paths)
                st["httpfs"] = True

    def _reset_connection(self) -> None:
        """Close and discard the connection on unrecoverable error."""
        st = _shared_state()
        if st["con"] is not None:
            try:
                st["con"].close()
            except Exception:
                pass
            st["con"] = None
            st["httpfs"] = False
            # Tables died with the connection — just forget the registry.
            self._tombstone_cache.clear_registry()
            logger.warning("[duckdb] connection reset")

    # ------------------------------------------------------------------
    # Core execution
    # ------------------------------------------------------------------

    def _build_view_chain(
            self,
            con,
            reflection: Reflection,
            parser: SQLParser,
            alias_to_table_name: dict,
            alias_to_files: dict,
            alias_to_columns: dict,
            alias_to_column_types: dict,
            created_views: List[str],
            acquired_dv_keys: List[str],
            timer_capture,
            log_prefix: str = "",
            explain: bool = False,
            explain_options: str = "",
            expose_rowid: bool = False,
            alias_to_filter_only: Optional[dict] = None,
    ):
        """Build reflection -> tombstone -> RBAC views and rewrite the query.

        Extracted so ``execute`` and ``stream`` share one definition of what a
        query actually reads. A streamed read must see exactly the chain a
        buffered read sees — the same deletion-vector anti-join, the same RBAC
        column and row filtering — otherwise streaming would quietly become a
        way to bypass both.

        ``created_views`` and ``acquired_dv_keys`` are appended in place so the
        CALLER owns teardown: a buffered query tears down when it returns, a
        stream only when its reader is closed.

        ``alias_to_filter_only`` names, per alias, the columns that are in the
        reflection ONLY so the RBAC/share row filter can bind. The RBAC view
        drops them again, so a widened projection never reaches the caller.

        Returns ``(executing_query, tried_presign)``.
        """
        tried_presign = False
        for alias, table_name in alias_to_table_name.items():
            files = alias_to_files[alias]
            cols = alias_to_columns[alias]

            # Use VIEW (lazy, default). Set SUPERTABLE_DUCKDB_MATERIALIZE=table to revert.
            used_presign = create_reflection_view_with_presign_retry(
                con, self.storage, table_name, files, cols, log_prefix,
                column_types=alias_to_column_types.get(alias),
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
            # Reuse a materialised deletion-vector table when the cache is
            # enabled and the alias has a stable key; otherwise the call
            # falls back to the inline read_parquet path (dv_table=None).
            cache_key = getattr(tomb_def, "cache_key", None) if tomb_def else None
            tomb_path = getattr(tomb_def, "tombstone_path", None) if tomb_def else None
            dv_table = self._tombstone_cache.acquire(con, cache_key, tomb_path)
            if dv_table:
                acquired_dv_keys.append(cache_key)
            create_tombstone_view(con, source, view, tomb_def, dv_table=dv_table,
                                  expose_rowid=expose_rowid)
            created_views.append(view)
            query_alias_to_name[alias] = view

        # RBAC views (column + row filtering) on top of stripped data.
        rbac_views = getattr(reflection, "rbac_views", None) or {}
        filter_only = alias_to_filter_only or {}
        if rbac_views:
            for alias in list(query_alias_to_name.keys()):
                view_def = rbac_views.get(alias)
                if view_def:
                    source = query_alias_to_name[alias]
                    view = f"rbac_{source}_{query_suffix}"
                    create_rbac_view(
                        con, source, view, view_def,
                        projected_columns=alias_to_columns.get(alias),
                        filter_only_columns=filter_only.get(alias),
                    )
                    created_views.append(view)
                    query_alias_to_name[alias] = view

        executing_query = rewrite_query_with_hashed_tables(
            parser.original_query, query_alias_to_name,
        )
        # EXPLAIN [ANALYZE] wrapper: ask DuckDB for the plan of the rewritten
        # query (over the reflection/tombstone/RBAC view chain) instead of
        # the rows. The prefix is applied to the final SQL so the plan
        # reflects exactly what a real read would execute.
        if explain:
            _opts = (explain_options or "").strip()
            executing_query = (
                f"EXPLAIN {(_opts + ' ') if _opts else ''}{executing_query}"
            )
        parser.executing_query = executing_query

        return executing_query, tried_presign



    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------
    #
    # There is no buffered execute() here any more. It called fetchdf(),
    # which built the whole result in memory before the caller saw a row —
    # the reason unbounded queries needed a LIMIT — and it was a SECOND
    # implementation of the view chain that could drift from the streamed
    # one. Buffering is now a consumer: Executor.execute drains stream()
    # through arrow_result.materialize.
    #
    # Removing it also removed fetchdf's float64 coercion of DuckDB's
    # decimal128 sums, which silently corrupted integer totals above 2**53.

    # ------------------------------------------------------------------
    # Streaming execution
    # ------------------------------------------------------------------

    def stream(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
            engine_config=None,
            batch_rows: int = 0,
            explain: bool = False,
            explain_options: str = "",
            expose_rowid: bool = False,
    ) -> "StreamHandle":
        """Execute and return an Arrow reader instead of a materialised frame.

        ``execute`` calls ``fetchdf()``, which builds the WHOLE result in memory
        before the caller sees a row — the reason unbounded queries needed a
        LIMIT. This returns a ``pyarrow.RecordBatchReader`` that DuckDB fills
        incrementally: measured on a 5M-row query, the first batch arrived in
        13.7ms out of 541ms total.

        The query runs on a dedicated CURSOR, not on the shared connection.
        A cursor sees views created on its parent, so the view chain is built
        once on the shared connection and read from the cursor; meanwhile the
        shared connection stays free for other queries, and ``interrupt()`` on
        the cursor cancels THIS stream without touching anything else. Both are
        verified in test_streaming_engine.

        The caller must close the returned handle. Views and deletion-vector
        references stay alive until it does — they are what the reader is
        reading through.
        """
        with self._lock:
            try:
                con = self._get_connection(temp_dir=query_manager.temp_dir)
            except Exception:
                self._reset_connection()
                con = self._get_connection(temp_dir=query_manager.temp_dir)

        timer_capture("CONNECTING")

        snapshots_by_key = {
            (sup.super_name, sup.simple_name): sup for sup in reflection.supers
        }
        rbac_views = getattr(reflection, "rbac_views", None) or {}
        alias_to_table_name, alias_to_files, alias_to_columns = {}, {}, {}
        alias_to_column_types: dict = {}
        alias_to_filter_only: dict = {}
        for td in parser.get_table_tuples():
            sup = snapshots_by_key.get((td.super_name, td.simple_name))
            if not sup:
                continue
            cols = list(td.columns or [])
            if cols:
                # Conditional references — a GROUP BY identifier shadowed by a
                # projected alias — are columns only if this table declares
                # one. DuckDB binds the real column over the alias
                # (`SELECT a AS b ... GROUP BY b` groups by the physical b), so
                # a name that does exist must be read or the bind fails; a name
                # that does not is the alias and must not be demanded. The
                # snapshot schema is the first place that can tell them apart,
                # which is why the parser deferred the decision to here.
                known = {str(c).lower() for c in (sup.columns or [])}
                have = {x.lower() for x in cols}
                _rbac = rbac_views.get(td.alias)
                _allowed = [str(c) for c in (getattr(_rbac, "allowed_columns", None) or ["*"])]
                _allowed_lower = None if "*" in _allowed else {c.lower() for c in _allowed}
                for candidate in (td.optional_columns or []):
                    low = candidate.lower()
                    if low not in have and low in known:
                        # The name IS a column of this table, so DuckDB would
                        # group by it. If the role may not read it, that is a
                        # permission failure and has to say so — dropping it
                        # instead would surface as a confusing binder error,
                        # which is precisely the STREAD-013 defect.
                        if _allowed_lower is not None and low not in _allowed_lower:
                            raise PermissionError(
                                f"You don't have permission to columns: "
                                f"{{'{candidate}'}} in table '{td.simple_name}'"
                            )
                        cols.append(candidate)
                        have.add(low)

                lower = {x.lower() for x in cols}
                for c in (ROWID_SYSTEM_COL, TIMESTAMP_SYSTEM_COL):
                    if c not in lower:
                        cols.append(c)
                # The RBAC/share row filter is applied ABOVE this projection,
                # on a relation that only has what we read here — so a filter
                # on a column the query never named could not bind and every
                # projected query under a row-filtered role failed. Read it
                # too, then let create_rbac_view drop it again.
                cols, extra = widen_projection_for_row_filter(
                    cols, rbac_views.get(td.alias), sup.columns, td.alias,
                )
                if extra:
                    alias_to_filter_only[td.alias] = extra
                    logger.debug(
                        f"{log_prefix}[rbac] projection widened for "
                        f"'{td.alias}' with row-filter column(s) {extra}"
                    )
            alias_to_table_name[td.alias] = hashed_table_name(
                sup.super_name, sup.simple_name, sup.simple_version, cols,
            )
            alias_to_files[td.alias] = list(sup.files)
            alias_to_columns[td.alias] = cols
            # Only consulted when the file list is empty: an existing
            # table with no resources has no footer to take a schema from.
            alias_to_column_types[td.alias] = dict(sup.column_types or {})

        self._ensure_httpfs(
            con, [f for files in alias_to_files.values() for f in files],
        )

        created_views: List[str] = []
        acquired_dv_keys: List[str] = []
        cursor = None
        try:
            executing_query, _ = self._build_view_chain(
                con, reflection, parser, alias_to_table_name, alias_to_files,
                alias_to_columns, alias_to_column_types, created_views, acquired_dv_keys,
                timer_capture, log_prefix, explain, explain_options,
                expose_rowid, alias_to_filter_only,
            )
            apply_runtime_pragmas(con, engine_config)

            cursor = con.cursor()
            rows = int(batch_rows or settings.SUPERTABLE_STREAM_BATCH_ROWS)
            logger.debug(f"{log_prefix}[duckdb.stream] {executing_query}")
            result = cursor.execute(executing_query)
            reader = (result.to_arrow_reader(rows)
                      if hasattr(result, "to_arrow_reader")
                      else result.fetch_record_batch(rows))
            return StreamHandle(
                reader=reader, cursor=cursor, connection=con,
                views=created_views, dv_keys=acquired_dv_keys,
                cache=self._tombstone_cache,
            )
        except Exception:
            # Nothing will close the handle if we never return one.
            StreamHandle(
                reader=None, cursor=cursor, connection=con,
                views=created_views, dv_keys=acquired_dv_keys,
                cache=self._tombstone_cache,
            ).close()
            raise


class StreamHandle:
    """An open Arrow stream plus everything that must outlive it.

    A streamed result is not self-contained: it reads through per-query views
    and a materialised deletion-vector table. Dropping those while the reader is
    still open would fail the query mid-flight, so teardown is deferred to
    ``close`` instead of happening when the producing call returns.

    ``cancel`` is safe from another thread — that is the whole point. It
    interrupts the cursor, which raises inside whichever ``read_next_batch``
    is in flight.
    """

    def __init__(self, reader, cursor, connection, views, dv_keys, cache):
        self.reader = reader
        self._cursor = cursor
        self._con = connection
        self._views = views
        self._dv_keys = dv_keys
        self._cache = cache
        self._closed = False
        # Guards cancel/close against each other. Without it a watcher thread
        # can call interrupt() on a cursor the main thread has already closed,
        # which is a use-after-free in DuckDB's C++ layer and aborts the
        # process with "terminate called without an active exception".
        self._teardown_lock = threading.Lock()
        #: Rows handed to the consumer so far. A streamed query has no row
        #: count until it ends, so monitoring is written on close, not on
        #: execute — see DataReader.stream.
        self.rows_streamed = 0
        #: Called once with (rows, columns) when the stream is closed.
        self.on_close = None

    @property
    def schema(self):
        return self.reader.schema if self.reader is not None else None

    def batches(self):
        """Yield record batches until exhausted, then release resources."""
        if self.reader is None:
            return
        try:
            for batch in self.reader:
                self.rows_streamed += batch.num_rows
                yield batch
        finally:
            self.close()

    def cancel(self) -> None:
        """Interrupt the in-flight read. Safe to call from another thread.

        A no-op once closed: interrupting a cursor that has been torn down
        reaches freed C++ state rather than a live query.
        """
        with self._teardown_lock:
            if self._closed or self._cursor is None:
                return
            try:
                self._cursor.interrupt()
            except Exception:
                pass

    def close(self) -> None:
        with self._teardown_lock:
            if self._closed:
                return
            # Set INSIDE the lock so a concurrent cancel() either interrupts a
            # live cursor or sees closed — never interrupts one being freed.
            self._closed = True
        for obj in (self.reader, self._cursor):
            try:
                if obj is not None and hasattr(obj, "close"):
                    obj.close()
            except Exception:
                pass
        # Views last: the reader was reading through them.
        for view in reversed(self._views):
            try:
                self._con.execute(f"DROP VIEW IF EXISTS {view};")
            except Exception:
                pass
        for key in self._dv_keys:
            try:
                self._cache.release(self._con, key)
            except Exception:
                pass
        if self.on_close is not None:
            try:
                ncols = len(self.reader.schema) if self.reader is not None else 0
                self.on_close(self.rows_streamed, ncols)
            except Exception:
                # Monitoring must never break a query that already succeeded.
                pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
        return False
