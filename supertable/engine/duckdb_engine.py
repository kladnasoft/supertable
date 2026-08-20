# supertable/engine/duckdb_engine.py

from __future__ import annotations

import threading
import math
import re
import uuid as _uuid
from typing import Any, Optional, List
from urllib.parse import urlsplit

import duckdb
import pandas as pd

from supertable.config.defaults import logger
from supertable.config.settings import settings
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
    TombstoneCache,
    create_typed_empty_view,
    redact_url_credentials,
    _validated_rbac_predicate_sql,
    tombstone_data_paths,
    validate_rbac_binding_stability,
)
from supertable.engine.island_resources import (
    ArrowBatchStream,
    ResourceReservationCancelled,
)


def _fresh_validated_duckdb_parser(parser: SQLParser) -> SQLParser:
    """Reparse original SQL and rebuild bindings at the backend boundary."""
    original_query = getattr(parser, "original_query", None)
    if not isinstance(original_query, str) or not original_query.strip():
        raise RuntimeError("DuckDB execution requires a parsed SQL query")

    default_super = getattr(parser, "default_super_name", None)
    if not isinstance(default_super, str) or not default_super:
        default_super = None
        try:
            supplied_tables = parser.get_table_tuples()
        except Exception:
            supplied_tables = []
        for table in supplied_tables or ():
            candidate = getattr(table, "super_name", None)
            if isinstance(candidate, str) and candidate:
                default_super = candidate
                break
    if default_super is None:
        default_super = "__supertable_default__"

    # Never trust parser._parsed or its cached bindings: a custom/mutable parser
    # can carry a safe AST beside malicious original SQL, or vice versa.
    return SQLParser(
        default_super,
        original_query,
        "duckdb",
        allow_bounded_collection_aggregates=(
            getattr(parser, "allow_bounded_collection_aggregates", False) is True
        ),
    )


def _harden_user_query_connection(con: duckdb.DuckDBPyConnection) -> None:
    """Apply and verify security-critical options before executing user SQL.

    DuckDB's ``lock_configuration`` is database-global (including across
    ``cursor()`` handles) and irreversible for this in-memory database. Using it
    here would let one request break concurrent setup and every later live
    runtime update. The AST gate rejects SET/PRAGMA/extension calls, while these
    verified backend settings provide independent defence in depth.
    """
    required_false = (
        "allow_unredacted_secrets",
        "autoload_known_extensions",
        "autoinstall_known_extensions",
        "allow_community_extensions",
    )
    try:
        for setting_name in required_false:
            con.execute(f"SET {setting_name}=false;")
            row = con.execute(
                "SELECT current_setting(?)", [setting_name]
            ).fetchone()
            if not row or bool(row[0]):
                raise RuntimeError("DuckDB security setting was not applied")

    except Exception:
        # Backend messages may include configured values. Expose only a stable,
        # non-sensitive failure at the user boundary.
        raise RuntimeError(
            "DuckDB user-query security configuration could not be enforced"
        ) from None


def _path_contains_bearer_credentials(path: object) -> bool:
    """Conservatively identify signed URLs or URL user-info credentials."""
    text = str(path or "").strip()
    try:
        parsed = urlsplit(text)
    except Exception:
        return text.lower().startswith(("http://", "https://")) and (
            "?" in text or "@" in text.split("/", 3)[2]
        )
    return (
        parsed.scheme.casefold() in {"http", "https"}
        and bool(parsed.query or parsed.fragment or parsed.username or parsed.password)
    )


def _tombstone_source_paths(reflection: Reflection) -> List[str]:
    """Collect every Parquet DV source and exclude v2 JSON tripwires."""
    return [
        path
        for view_def in (
            getattr(reflection, "tombstone_views", None) or {}
        ).values()
        for path in tombstone_data_paths(view_def)
    ]


_SENSITIVE_ERROR_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(authorization|password|passwd|secret(?:_access_key)?|"
    r"aws_secret_access_key|access_key(?:_id)?|aws_access_key_id|"
    r"session_token|access_token|token|credential)\b(\s*(?:=|:)\s*)"
    r"(?:'[^']*'|\"[^\"]*\"|[^\s,;)]+)"
)
_SENSITIVE_SECRET_SQL_RE = re.compile(
    r"(?i)\b(KEY_ID|SECRET|SESSION_TOKEN)\s+(?:'[^']*'|\"[^\"]*\")"
)
_MAX_PUBLIC_DUCKDB_ERROR_CHARS = 4096


def _redact_duckdb_backend_message(value: object) -> str:
    """Return a bounded backend diagnostic with credential material removed."""
    message = redact_url_credentials(value)
    message = _SENSITIVE_ERROR_ASSIGNMENT_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[REDACTED]",
        message,
    )
    message = _SENSITIVE_SECRET_SQL_RE.sub(
        lambda match: f"{match.group(1)} '[REDACTED]'",
        message,
    )
    if len(message) > _MAX_PUBLIC_DUCKDB_ERROR_CHARS:
        message = message[:_MAX_PUBLIC_DUCKDB_ERROR_CHARS] + "…"
    return message or "DuckDB backend error"


def _scrub_exception_chain(
    exc: BaseException,
    *,
    replacement: Optional[str] = None,
) -> None:
    """Remove secret-bearing strings and traceback locals from a cause chain."""
    pending = [exc]
    seen = set()
    while pending:
        current = pending.pop()
        identity = id(current)
        if identity in seen:
            continue
        seen.add(identity)
        safe_message = replacement or _redact_duckdb_backend_message(current)
        try:
            current.args = (safe_message,)
        except Exception:
            pass
        try:
            current.__traceback__ = None
        except Exception:
            pass
        for linked in (current.__cause__, current.__context__):
            if isinstance(linked, BaseException):
                pending.append(linked)


def _safe_duckdb_backend_exception(
    exc: BaseException,
    *,
    phase: str,
) -> BaseException:
    """Build a public exception without retaining raw backend diagnostics."""
    if not isinstance(exc, Exception):
        return exc
    if isinstance(exc, TimeoutError):
        _scrub_exception_chain(exc, replacement="DuckDB backend detail redacted")
        return TimeoutError(f"DuckDB {phase} timed out")
    if isinstance(exc, ResourceReservationCancelled):
        _scrub_exception_chain(exc, replacement="DuckDB backend detail redacted")
        return ResourceReservationCancelled(
            f"DuckDB {phase} was cancelled"
        )
    # Managed view DDL can contain RBAC predicates and physical source paths.
    # Even a URL-redacted backend message could expose hidden policy columns or
    # literal values, so arbitrary backend failures get a phase-only message.
    _scrub_exception_chain(exc, replacement="DuckDB backend detail redacted")
    return RuntimeError(f"DuckDB {phase} failed")


def _assert_reflection_covers_requested_tables(
    parser: SQLParser,
    reflection: Reflection,
) -> None:
    """Reject parser/reflection mismatches before opening the DuckDB session."""
    requested = {
        (str(table.super_name).casefold(), str(table.simple_name).casefold())
        for table in parser.get_physical_tables()
        if table.super_name and table.simple_name
    }
    reflected = {
        (str(snapshot.super_name).casefold(), str(snapshot.simple_name).casefold())
        for snapshot in reflection.supers
        if snapshot.super_name and snapshot.simple_name
    }
    missing = requested - reflected
    if missing:
        raise PermissionError(
            "DuckDB reflection does not authorize every requested relation"
        )


class _DuckDBArrowBatchIterator:
    """Adapt DuckDB's Arrow reader to cancellable stream semantics."""

    def __init__(
        self,
        reader,
        connection,
        *,
        timed_out: threading.Event,
        timeout_value: float,
    ):
        self._reader = reader
        self._iterator = iter(reader)
        self._connection = connection
        self._timed_out = timed_out
        self._timeout_value = timeout_value
        self._cancelled = threading.Event()
        self._closed = False
        self._close_lock = threading.Lock()

    def __iter__(self):
        return self

    def _raise_if_stopped(self) -> None:
        if self._timed_out.is_set():
            raise TimeoutError(
                f"DuckDB query timed out after {self._timeout_value:g} seconds"
            )
        if self._cancelled.is_set():
            raise ResourceReservationCancelled(
                "DuckDB Arrow result stream was cancelled"
            )

    def __next__(self):
        self._raise_if_stopped()
        try:
            batch = next(self._iterator)
        except StopIteration:
            raise
        except BaseException as exc:
            if not isinstance(exc, Exception):
                raise
            if self._timed_out.is_set():
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise TimeoutError(
                    f"DuckDB query timed out after "
                    f"{self._timeout_value:g} seconds"
                ) from None
            if self._cancelled.is_set():
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise ResourceReservationCancelled(
                    "DuckDB Arrow result stream was cancelled"
                ) from None
            safe_error = _safe_duckdb_backend_exception(
                exc, phase="result stream",
            )
            raise safe_error from None
        self._raise_if_stopped()
        return batch

    def cancel(self) -> None:
        # ``interrupt`` is DuckDB's documented cross-thread cancellation path.
        # The outer ArrowBatchStream defers ``close`` until any active next()
        # unwinds, so the Arrow reader is never closed concurrently with fetch.
        self._cancelled.set()
        try:
            self._connection.interrupt()
        except Exception:
            pass

    def close(self) -> None:
        with self._close_lock:
            if self._closed:
                return
            self._closed = True
        self._reader.close()


def _result_stream_batch_rows(max_batch_rows: Optional[int] = None) -> int:
    """Return a hard-bounded Arrow fetch size for public result streams.

    A row is the indivisible lower bound, but limiting batch cardinality keeps
    a wide result from turning the streaming API into a 64K-row materialization
    step before its serialized-byte consumer can reject it.
    """
    try:
        configured = int(
            getattr(settings, "SUPERTABLE_RESULT_STREAM_BATCH_ROWS", 256)
        )
    except (TypeError, ValueError, OverflowError):
        configured = 256
    configured = max(1, min(configured, 4096))
    if max_batch_rows is None:
        return configured
    if isinstance(max_batch_rows, bool) or not isinstance(max_batch_rows, int):
        raise ValueError("max_batch_rows must be a positive integer")
    if max_batch_rows < 1:
        raise ValueError("max_batch_rows must be a positive integer")
    return min(configured, max_batch_rows)


class DuckDB:
    """
    Per-query DuckDB executor backed by a single persistent connection.

    The connection is created once (lazily) and reused across all queries so
    that DuckDB's HTTP metadata cache, external file cache, and httpfs
    configuration survive between requests.  This eliminates the per-query
    overhead of re-fetching parquet footer metadata from remote storage.

    Query isolation is preserved: VIEWs are created with unique names and
    dropped in the finally block after each query. No materialised table state
    is retained between queries.

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
        self._s3_secret_configured = False
        # Shared deletion-vector table cache: per-table eviction (idle TTL +
        # per-table version cap), bounded by config. Tables live on the
        # persistent connection and are forgotten when it resets.
        self._tombstone_cache = TombstoneCache(
            settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_PER_TABLE,
            settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_TTL_SEC,
            settings.SUPERTABLE_DUCKDB_TOMBSTONE_CACHE_MAX_ENTRIES,
            storage=self.storage,
        )

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
        self._s3_secret_configured = False
        logger.info("[duckdb] persistent connection created")
        return con

    def _ensure_httpfs(self, con: duckdb.DuckDBPyConnection, paths: List[str]) -> None:
        """Configure every query-private cursor under the setup lock.

        DuckDB cursor handles share database settings and the temporary secret
        catalog. Track HTTP initialisation separately from S3 secret creation so
        an initial HTTPS-only query cannot cause a later S3 query to skip its
        credential setup.
        """
        if not any(
            str(path).lower().startswith(("s3://", "s3a://", "http://", "https://"))
            for path in paths
        ):
            return
        needs_s3 = any(
            str(path).lower().startswith(("s3://", "s3a://")) for path in paths
        )
        with self._lock:
            # Rebind the temporary secret for every S3 query. Credential
            # providers can rotate a session token while this persistent
            # connection remains alive; retaining an earlier secret would no
            # longer match the selected storage authorization context.
            if not self._httpfs_configured or needs_s3:
                configure_httpfs_and_s3(
                    con, paths, storage=self.storage,
                )
                self._httpfs_configured = True
                self._s3_secret_configured = (
                    self._s3_secret_configured or needs_s3
                )

    def _reset_connection(self) -> None:
        """Close and discard the connection on unrecoverable error."""
        if self._con is not None:
            try:
                self._con.close()
            except Exception:
                pass
            self._con = None
            self._httpfs_configured = False
            self._s3_secret_configured = False
            # Tables died with the connection — just forget the registry.
            self._tombstone_cache.clear_registry()
            logger.warning("[duckdb] connection reset")

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
            explain: bool = False,
            explain_options: str = "",
            timeout_sec: Optional[float] = None,
            *,
            _streaming: bool = False,
            _stream_batch_rows: Optional[int] = None,
    ) -> Any:
        if explain and str(explain_options or "").strip().casefold() == "analyze":
            raise ValueError(
                "EXPLAIN ANALYZE is not available on the untrusted read path"
            )
        # This is an execution-boundary invariant, not merely a parser
        # convenience. Reparse original SQL and rebuild table bindings before a
        # connection, secret, or managed view is provisioned.
        caller_parser = parser
        parser = _fresh_validated_duckdb_parser(caller_parser)
        validated_ast = parser._parsed
        _assert_reflection_covers_requested_tables(parser, reflection)
        validate_rbac_binding_stability(
            parser,
            getattr(reflection, "rbac_views", None) or {},
        )
        if _streaming:
            # Keep request validation outside the backend-error boundary so a
            # malformed public batch cap remains a meaningful ValueError.
            resolved_stream_batch_rows = _result_stream_batch_rows(
                _stream_batch_rows
            )
        else:
            resolved_stream_batch_rows = None
        for view_def in (
            getattr(reflection, "rbac_views", None) or {}
        ).values():
            # Validate policy SQL before a connection, secret, or managed view
            # exists. create_rbac_view repeats this immediately before DDL as a
            # defence against mutation between these two points.
            _validated_rbac_predicate_sql(
                getattr(view_def, "where_clause", "")
            )
        if explain and any(
            (
                list(getattr(view_def, "allowed_columns", None) or []) != ["*"]
                or bool(getattr(view_def, "excluded_columns", None))
                or bool(str(getattr(view_def, "where_clause", "") or "").strip())
            )
            for view_def in (
                getattr(reflection, "rbac_views", None) or {}
            ).values()
        ):
            # DuckDB expands nested views into its plan text.  That text can
            # expose hidden predicate columns and literal policy values even
            # though result rows obey the view.  Reject before opening a
            # connection or creating any credential-bearing relation.
            raise ValueError(
                "EXPLAIN is unavailable for access-controlled queries"
            )
        tombstone_paths = _tombstone_source_paths(reflection)
        initial_source_paths = [
            path
            for snapshot in reflection.supers
            for path in (getattr(snapshot, "files", None) or [])
        ] + tombstone_paths
        if explain and any(
            _path_contains_bearer_credentials(path)
            for path in initial_source_paths
        ):
            raise ValueError(
                "EXPLAIN is unavailable for credential-bearing remote sources"
            )
        tried_presign = False

        with self._lock:
            try:
                root_con = self._get_connection(temp_dir=query_manager.temp_dir)
            except Exception as initial_error:
                self._reset_connection()
                try:
                    root_con = self._get_connection(
                        temp_dir=query_manager.temp_dir
                    )
                except BaseException as retry_error:
                    if not isinstance(retry_error, Exception):
                        raise
                    # The first and retry failures can both retain connection
                    # configuration in their traceback locals/cause chain.
                    retry_error.__context__ = initial_error
                    safe_error = _safe_duckdb_backend_exception(
                        retry_error, phase="connection setup",
                    )
                    raise safe_error from None
            try:
                con = root_con.cursor()
            except BaseException as cursor_error:
                if not isinstance(cursor_error, Exception):
                    raise
                safe_error = _safe_duckdb_backend_exception(
                    cursor_error, phase="connection setup",
                )
                raise safe_error from None

        timer_capture("CONNECTING")

        snapshots_by_key = {
            (sup.super_name, sup.simple_name): sup
            for sup in reflection.supers
        }
        table_defs = parser.get_table_tuples()
        ambiguity_getter = getattr(parser, "get_group_alias_ambiguities", None)
        group_alias_ambiguities = (
            ambiguity_getter() if callable(ambiguity_getter) else {}
        )
        ambiguous_aliases = {
            str(alias).casefold() for alias in group_alias_ambiguities
        }
        # Every relation in a Lite query is request-private.  The persistent
        # root connection is shared across Executor instances, so using only
        # the snapshot hash here lets concurrent queries CREATE OR REPLACE and
        # DROP each other's reflection view before either reaches execution.
        # Keep the full 128-bit request id.  A truncated 32-bit suffix reaches
        # birthday-collision probability under realistic concurrency; because
        # the DDL below uses CREATE OR REPLACE, a collision can swap another
        # query's reflected files or deletion-vector view mid-execution.
        query_suffix = _uuid.uuid4().hex

        alias_to_table_name = {}
        alias_to_files = {}
        alias_to_resource_keys = {}
        alias_to_columns = {}

        for td in table_defs:
            key = (td.super_name, td.simple_name)
            sup = snapshots_by_key.get(key)
            if not sup:
                continue

            cols = list(td.columns or [])
            if td.alias.casefold() in ambiguous_aliases:
                # DuckDB gives an input column precedence over a same-named
                # SELECT alias in GROUP BY.  Only the backend has the real
                # schema, so retain the full protected input for that binding.
                cols = []
            # Row/share filters may depend on columns that the user did not
            # project.  Load the complete pinned relation for those queries;
            # the RBAC view reapplies the column policy before user SQL runs.
            view_def = (getattr(reflection, "rbac_views", None) or {}).get(td.alias)
            if view_def is not None and getattr(view_def, "where_clause", ""):
                cols = []

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
            name = f"{name}_{query_suffix}"
            alias_to_table_name[td.alias] = name
            alias_to_files[td.alias] = list(sup.files)
            alias_to_resource_keys[td.alias] = list(
                getattr(sup, "resource_keys", ()) or ()
            )
            alias_to_columns[td.alias] = cols

        # Ensure httpfs is configured on the persistent connection (once only).
        all_files = [f for files in alias_to_files.values() for f in files]
        sensitive_source_paths = any(
            _path_contains_bearer_credentials(path)
            for path in [*all_files, *tombstone_paths]
        )
        if explain and sensitive_source_paths:
            raise ValueError(
                "EXPLAIN is unavailable for credential-bearing remote sources"
            )
        try:
            self._ensure_httpfs(
                con,
                [
                    path
                    for path in [*all_files, *tombstone_paths]
                    if path
                ],
            )
        except BaseException as exc:
            # No view owns this query-private handle yet, so the main cleanup
            # block has not started. Do not leak cursors on a fail-closed auth
            # or extension-configuration rejection.
            try:
                con.close()
            except Exception:
                pass
            if not isinstance(exc, Exception):
                raise
            safe_error = _safe_duckdb_backend_exception(
                exc, phase="storage setup",
            )
            raise safe_error from None

        # Create per-query VIEWs. Dropped in finally regardless of outcome.
        created_views: List[str] = []
        # Deletion-vector cache keys acquired this query — released in finally.
        acquired_dv_keys: List[str] = []
        timed_out = threading.Event()
        watchdog = None
        try:
            timeout_value = float(timeout_sec) if timeout_sec is not None else 0.0
        except (TypeError, ValueError, OverflowError):
            timeout_value = 0.0
        if math.isfinite(timeout_value) and timeout_value > 0:
            def interrupt_query() -> None:
                timed_out.set()
                try:
                    # A cursor is a query-private DuckDB connection handle. Use
                    # it instead of the shared root so one deadline does not
                    # cancel unrelated concurrent readers.
                    con.interrupt()
                except Exception:
                    pass

            watchdog = threading.Timer(timeout_value, interrupt_query)
            watchdog.daemon = True
            watchdog.start()

        def check_deadline() -> None:
            if timed_out.is_set():
                raise TimeoutError(
                    f"DuckDB query timed out after {timeout_value:g} seconds"
                )

        cleanup_lock = threading.Lock()
        cleanup_complete = False

        def cleanup_query() -> None:
            nonlocal cleanup_complete
            with cleanup_lock:
                if cleanup_complete:
                    return
                cleanup_complete = True
            if watchdog is not None:
                watchdog.cancel()
                join_watchdog = getattr(watchdog, "join", None)
                if (
                    callable(join_watchdog)
                    and watchdog is not threading.current_thread()
                ):
                    # ``Timer.cancel`` does not wait for a callback that has
                    # already begun. Ensure a racing interrupt is finished
                    # before issuing cleanup DDL on the same cursor.
                    join_watchdog(timeout=1.0)
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
            # Release deletion-vector refs now the views referencing them are
            # gone; this may evict + DROP unreferenced DV tables over capacity.
            for cache_key in acquired_dv_keys:
                try:
                    with self._lock:
                        self._tombstone_cache.release(con, cache_key)
                except Exception:
                    pass
            try:
                con.close()
            except Exception:
                pass

        stream_owns_cleanup = False
        backend_phase = "managed query setup"
        try:
            check_deadline()
            for alias, table_name in alias_to_table_name.items():
                check_deadline()
                files = alias_to_files[alias]
                cols = alias_to_columns[alias]

                if not files:
                    td = next(t for t in table_defs if t.alias == alias)
                    sup = snapshots_by_key[(td.super_name, td.simple_name)]
                    create_typed_empty_view(
                        con, table_name, dict(getattr(sup, "column_types", {}) or {}), cols,
                    )
                    created_views.append(table_name)
                    continue

                # Use VIEW (lazy, default). Set SUPERTABLE_DUCKDB_MATERIALIZE=table to revert.
                used_presign = create_reflection_view_with_presign_retry(
                    con, self.storage, table_name, files, cols, log_prefix,
                    resource_keys=alias_to_resource_keys[alias],
                )
                created_views.append(table_name)
                if used_presign:
                    tried_presign = True

            if explain and tried_presign:
                raise ValueError(
                    "EXPLAIN is unavailable for credential-bearing remote sources"
                )

            timer_capture("CREATING_REFLECTION")

            # Per-query suffix so concurrent requests on the same table do not
            # collide on a shared view name (CREATE OR REPLACE would silently
            # corrupt a sibling query's view mid-execution).
            query_alias_to_name = dict(alias_to_table_name)

            # Tombstone / system-column view — created for EVERY alias so the
            # system columns (__rowid__, __timestamp__) are always stripped and
            # the deletion-vector (when present) is anti-joined out.  Sits on
            # the reflection table directly, before RBAC.
            tombstone_views = getattr(reflection, "tombstone_views", None) or {}
            for alias in list(query_alias_to_name.keys()):
                check_deadline()
                source = query_alias_to_name[alias]
                tomb_def = tombstone_views.get(alias)
                view = f"tomb_{source}_{query_suffix}"
                # Reuse a materialised deletion-vector table when the cache is
                # enabled and the alias has a stable key; otherwise the call
                # falls back to the inline read_parquet path (dv_table=None).
                cache_key = getattr(tomb_def, "cache_key", None) if tomb_def else None
                tomb_path = getattr(tomb_def, "tombstone_path", None) if tomb_def else None
                expected_rows = getattr(tomb_def, "expected_rows", None) if tomb_def else None
                expected_digest = getattr(tomb_def, "tombstone_digest", None) if tomb_def else None
                raw_snapshot_keys = (
                    getattr(tomb_def, "snapshot_resource_keys", None)
                    if tomb_def else None
                )
                selected_keys = (
                    list(getattr(tomb_def, "resource_keys", ()) or ())
                    if tomb_def else []
                )
                allowed_dv_files = (
                    list(raw_snapshot_keys)
                    if raw_snapshot_keys is not None
                    else (selected_keys or None)
                )
                with self._lock:
                    dv_table = self._tombstone_cache.acquire(
                        con, cache_key, tomb_path, expected_rows=expected_rows,
                        expected_digest=expected_digest,
                        tombstone_def=tomb_def,
                        allowed_files=allowed_dv_files,
                    )
                if dv_table:
                    acquired_dv_keys.append(
                        getattr(dv_table, "cache_key", cache_key)
                    )
                create_tombstone_view(con, source, view, tomb_def, dv_table=dv_table)
                created_views.append(view)
                query_alias_to_name[alias] = view

            # RBAC views (column + row filtering) on top of stripped data.
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            if rbac_views:
                for alias in list(query_alias_to_name.keys()):
                    check_deadline()
                    view_def = rbac_views.get(alias)
                    if view_def:
                        source = query_alias_to_name[alias]
                        view = f"rbac_{source}_{query_suffix}"
                        create_rbac_view(con, source, view, view_def)
                        created_views.append(view)
                        query_alias_to_name[alias] = view

            executing_query = rewrite_query_with_hashed_tables(
                parser.original_query,
                query_alias_to_name,
                parsed_expression=validated_ast,
                default_super_name=parser.default_super_name,
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
            try:
                setattr(caller_parser, "executing_query", executing_query)
            except Exception:
                pass

            # Profiling PRAGMAs are connection-level state.  Under concurrent
            # queries the last SET wins — one query's profile may land in the
            # wrong file.  This is acceptable: profiling is best-effort
            # diagnostics, and query_plan_path is already unique per query
            # (contains query_id) so profiles never overwrite on disk.
            # A lock here would serialise execution on a shared connection
            # and destroy DuckDB's concurrent-read capability.
            # Raw DuckDB profiles contain physical Filename(s). Because profile
            # settings are database-global across cursor handles, enabling them
            # for a sibling local query can race with a presigned query and write
            # its bearer URL to disk. User-query profiling is therefore disabled
            # unconditionally; bounded plan/timing stats remain available.
            try:
                con.execute("PRAGMA disable_profiling;")
            except Exception:
                raise RuntimeError(
                    "DuckDB user-query profiling could not be disabled"
                ) from None

            # Re-apply live engine config (memory/threads/http/cache) so UI
            # changes take effect on this persistent connection per query.
            check_deadline()
            apply_runtime_pragmas(con, engine_config)
            _harden_user_query_connection(con)

            logger.debug(f"{log_prefix}[duckdb] executing: {executing_query}")
            check_deadline()
            backend_phase = "query execution"
            query_result = con.execute(executing_query)
            check_deadline()

            if _streaming:
                to_arrow_reader = getattr(query_result, "to_arrow_reader", None)
                if callable(to_arrow_reader):
                    arrow_reader = to_arrow_reader(
                        batch_size=resolved_stream_batch_rows
                    )
                else:
                    # DuckDB 1.1 compatibility; newer versions retain this as
                    # a deprecated alias for ``to_arrow_reader``.
                    arrow_reader = query_result.fetch_record_batch(
                        rows_per_batch=resolved_stream_batch_rows,
                    )
                check_deadline()
                producer = _DuckDBArrowBatchIterator(
                    arrow_reader,
                    con,
                    timed_out=timed_out,
                    timeout_value=timeout_value,
                )
                result = ArrowBatchStream(
                    arrow_reader.schema,
                    producer,
                    close_callback=cleanup_query,
                )
                # From this point the stream owns the cursor, views, watchdog,
                # and DV references. The finally block must not invalidate them
                # before the caller consumes or closes the first batch.
                stream_owns_cleanup = True
            else:
                result = query_result.fetchdf()
                check_deadline()

            if tried_presign:
                logger.debug(f"{log_prefix}[duckdb] presigned fallback succeeded")

            return result

        except BaseException as exc:
            if not isinstance(exc, Exception):
                raise
            if timed_out.is_set() and not isinstance(exc, TimeoutError):
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise TimeoutError(
                    f"DuckDB query timed out after {timeout_value:g} seconds"
                ) from None
            safe_error = _safe_duckdb_backend_exception(
                exc, phase=backend_phase,
            )
            raise safe_error from None

        finally:
            if not stream_owns_cleanup:
                cleanup_query()

    def execute_stream(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
            engine_config=None,
            timeout_sec: Optional[float] = None,
            max_batch_rows: Optional[int] = None,
    ) -> ArrowBatchStream:
        """Return a bounded-batch Arrow stream owning all query resources."""
        return self.execute(
            reflection=reflection,
            parser=parser,
            query_manager=query_manager,
            timer_capture=timer_capture,
            log_prefix=log_prefix,
            engine_config=engine_config,
            timeout_sec=timeout_sec,
            _streaming=True,
            _stream_batch_rows=max_batch_rows,
        )
