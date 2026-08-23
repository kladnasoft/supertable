# supertable/engine/duckdb_engine.py

from __future__ import annotations

import threading
import math
import re
import time
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
    ByteBoundedArrowBatchIterator,
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


class DuckDBPresignRefreshRequired(RuntimeError):
    """A remote credential failed before any result batch was exposed.

    The exception deliberately carries no backend message or URL.  Executor is
    the only layer with both the pinned reflection identity and the storage
    authorization context required to perform one safe credential refresh.
    """


def _is_refreshable_remote_auth_error(exc: BaseException) -> bool:
    """Recognize storage-auth failures for the pre-first-row retry boundary."""
    try:
        message = str(exc).casefold().replace("_", "")
    except Exception:
        return False
    compact = " ".join(message.split())
    named_failures = (
        "accessdenied",
        "signaturedoesnotmatch",
        "expiredtoken",
        "request has expired",
        "authorizationqueryparameterserror",
        "invalidaccesskeyid",
    )
    if any(token in compact for token in named_failures):
        return True
    http_failure = any(token in compact for token in (
        "http error", "http get error", "http status", "status code",
    ))
    return http_failure and any(token in compact for token in (
        " 400", "(400", " 403", "(403",
    ))


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
        cancel_event: Optional[threading.Event] = None,
    ):
        self._reader = reader
        self._iterator = iter(reader)
        self._connection = connection
        self._timed_out = timed_out
        self._timeout_value = timeout_value
        self._external_cancel_event = cancel_event
        self._cancelled = threading.Event()
        self._closed = False
        self._close_lock = threading.Lock()
        self._batches_emitted = 0

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
        if (
            self._external_cancel_event is not None
            and self._external_cancel_event.is_set()
        ):
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
            if self._cancelled.is_set() or (
                self._external_cancel_event is not None
                and self._external_cancel_event.is_set()
            ):
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise ResourceReservationCancelled(
                    "DuckDB Arrow result stream was cancelled"
                ) from None
            if (
                self._batches_emitted == 0
                and _is_refreshable_remote_auth_error(exc)
            ):
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise DuckDBPresignRefreshRequired(
                    "DuckDB remote authorization expired before result delivery"
                ) from None
            safe_error = _safe_duckdb_backend_exception(
                exc, phase="result stream",
            )
            raise safe_error from None
        self._raise_if_stopped()
        self._batches_emitted += 1
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


class _DuckDBSetupInterruptGuard:
    """Interrupt one query-owned setup handle at cancel/deadline.

    The persistent root connection can serve sibling cursors, so this guard is
    deliberately never pointed at an existing shared root.  It observes a root
    only while this query is creating it under the setup lock, then switches to
    the query-private cursor before httpfs/storage setup.
    """

    def __init__(
        self,
        *,
        deadline_monotonic: Optional[float],
        timeout_value: float,
        cancel_event: Optional[threading.Event],
    ) -> None:
        self._deadline = deadline_monotonic
        self._timeout_value = timeout_value
        self._cancel_event = cancel_event
        self._timed_out = threading.Event()
        self._cancelled = threading.Event()
        self._stop = threading.Event()
        self._target_lock = threading.Lock()
        self._target = None
        self._timer = None
        self._watcher = None

    def _interrupt(self) -> None:
        with self._target_lock:
            target = self._target
        if target is not None:
            try:
                target.interrupt()
            except Exception:
                pass

    def set_target(self, target) -> None:
        with self._target_lock:
            self._target = target
            should_interrupt = (
                self._timed_out.is_set()
                or self._cancelled.is_set()
                or (
                    self._cancel_event is not None
                    and self._cancel_event.is_set()
                )
            )
        if should_interrupt:
            self._interrupt()

    def start(self) -> None:
        if self._deadline is not None:
            remaining = max(0.0, self._deadline - time.monotonic())

            def expire() -> None:
                self._timed_out.set()
                self._interrupt()

            if remaining <= 0:
                expire()
            else:
                self._timer = threading.Timer(remaining, expire)
                self._timer.daemon = True
                self._timer.start()

        if self._cancel_event is not None:
            def watch() -> None:
                while not self._stop.wait(0.05):
                    if self._cancel_event.is_set():
                        self._cancelled.set()
                        self._interrupt()
                        return

            self._watcher = threading.Thread(
                target=watch,
                name="supertable-duckdb-setup-cancel",
                daemon=True,
            )
            self._watcher.start()

    def raise_if_stopped(self) -> None:
        if self._cancelled.is_set() or (
            self._cancel_event is not None and self._cancel_event.is_set()
        ):
            raise ResourceReservationCancelled("DuckDB query was cancelled")
        if self._timed_out.is_set() or (
            self._deadline is not None
            and time.monotonic() >= self._deadline
        ):
            raise TimeoutError(
                f"DuckDB query timed out after {self._timeout_value:g} seconds"
            )

    def close(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            join_timer = getattr(self._timer, "join", None)
            if (
                callable(join_timer)
                and self._timer is not threading.current_thread()
            ):
                join_timer(timeout=1.0)
        self._stop.set()
        if (
            self._watcher is not None
            and self._watcher is not threading.current_thread()
        ):
            self._watcher.join(timeout=1.0)
        with self._target_lock:
            self._target = None


_DUCKDB_CONNECT_MAX_IN_FLIGHT = 8
_duckdb_connect_slots = threading.BoundedSemaphore(
    _DUCKDB_CONNECT_MAX_IN_FLIGHT
)


def _bounded_duckdb_connect(
    *,
    temp_dir: str,
    setup_check,
    setup_target_callback=None,
):
    """Create and initialize one connection behind a bounded orphan gate.

    A native ``duckdb.connect()`` call cannot be interrupted before it returns a
    handle. The request thread therefore stops waiting at its deadline/cancel
    boundary while a daemon worker retains the global slot. If that worker
    returns late, it closes its connection instead of publishing it to an engine
    cache. At most a small fixed number of abandoned native calls can coexist.
    """
    check = setup_check if callable(setup_check) else (lambda: None)
    slots = _duckdb_connect_slots
    while True:
        check()
        if slots.acquire(timeout=0.05):
            break
    try:
        check()
    except BaseException:
        slots.release()
        raise

    done = threading.Event()
    state_lock = threading.Lock()
    state: dict[str, object] = {"abandoned": False}

    def clear_target() -> None:
        if not callable(setup_target_callback):
            return
        try:
            setup_target_callback(None)
        except BaseException:
            # Clearing a target is cleanup; preserve the request's original
            # stop/backend exception rather than replacing it here.
            pass

    def close_connection(connection) -> None:
        try:
            connection.close()
        except Exception:
            pass

    def invoke() -> None:
        connection = None
        published = False
        try:
            connection = duckdb.connect()
            with state_lock:
                abandoned = bool(state["abandoned"])
            if abandoned:
                return
            if callable(setup_target_callback):
                published = True
                setup_target_callback(connection)
            init_connection(connection, temp_dir=temp_dir)
            with state_lock:
                if bool(state["abandoned"]):
                    abandoned = True
                else:
                    state["connection"] = connection
                    connection = None
                    abandoned = False
            if abandoned:
                return
        except BaseException as exc:
            with state_lock:
                if not bool(state["abandoned"]):
                    state["error"] = exc
        finally:
            if connection is not None:
                close_connection(connection)
            if published and connection is not None:
                clear_target()
            try:
                slots.release()
            finally:
                done.set()

    worker = threading.Thread(
        target=invoke,
        name="supertable-duckdb-connect",
        daemon=True,
    )
    try:
        worker.start()
    except BaseException:
        slots.release()
        raise

    def abandon() -> None:
        connection = None
        with state_lock:
            state["abandoned"] = True
            connection = state.pop("connection", None)
        if connection is not None:
            close_connection(connection)
            clear_target()

    try:
        while not done.wait(0.05):
            check()
        check()
        with state_lock:
            error = state.pop("error", None)
            connection = state.pop("connection", None)
        if isinstance(error, BaseException):
            raise error
        if connection is None:
            raise RuntimeError("DuckDB connection setup returned no handle")
        return connection
    except BaseException:
        abandon()
        raise


class _DuckDBResultLifecycleStream:
    """Finalize an idle DuckDB stream when its request stops being live.

    ``ArrowBatchStream.close`` is cooperative while ``next()`` is active: it
    records a close request and defers cursor/view cleanup until the producer
    unwinds.  Driving that existing state machine from the deadline/cancel
    watchers therefore reclaims an idle stream promptly without ever closing a
    DuckDB cursor underneath an active Arrow fetch.

    The terminal event is retained after cleanup so a later consumer call gets
    the typed timeout/cancellation rather than mistaking the auto-closed stream
    for successful exhaustion.
    """

    def __init__(
        self,
        inner: ArrowBatchStream,
        *,
        deadline_monotonic: Optional[float],
        timeout_value: float,
        cancel_event: Optional[threading.Event],
    ) -> None:
        self._inner = inner
        self.schema = inner.schema
        self._deadline = deadline_monotonic
        self._timeout_value = timeout_value
        self._cancel_event = cancel_event
        self._timed_out = threading.Event()
        self._cancelled = threading.Event()
        self._stop = threading.Event()
        self._watcher = None
        self._start_monitors()

    def __iter__(self):
        return self

    @property
    def closed(self) -> bool:
        return bool(getattr(self._inner, "closed", False))

    def _start_monitors(self) -> None:
        if self._deadline is None and self._cancel_event is None:
            return

        def watch() -> None:
            while not self._stop.is_set():
                if (
                    self._cancel_event is not None
                    and self._cancel_event.is_set()
                ):
                    self._cancelled.set()
                    try:
                        # cancel() uses DuckDB interrupt for an active fetch
                        # and the same deferred-close path for its resources.
                        self._inner.cancel()
                    finally:
                        self._stop.set()
                    return

                if (
                    self._deadline is not None
                    and time.monotonic() >= self._deadline
                ):
                    self._timed_out.set()
                    try:
                        # Safe for both states: idle streams finalize now; an
                        # active next() only records close_requested and
                        # finalizes after that producer unwinds.
                        self._inner.close()
                    finally:
                        self._stop.set()
                    return

                if self._deadline is None:
                    wait_for = 0.05
                else:
                    remaining = max(0.0, self._deadline - time.monotonic())
                    wait_for = (
                        min(0.05, remaining)
                        if self._cancel_event is not None else remaining
                    )
                self._stop.wait(wait_for)

        self._watcher = threading.Thread(
            target=watch,
            name="supertable-duckdb-result-lifecycle",
            daemon=True,
        )
        self._watcher.start()

    def _stop_monitors(self) -> None:
        self._stop.set()
        if (
            self._watcher is not None
            and self._watcher is not threading.current_thread()
        ):
            self._watcher.join(timeout=1.0)

    def _raise_if_stopped(self) -> None:
        if self._cancelled.is_set() or (
            self._cancel_event is not None and self._cancel_event.is_set()
        ):
            self._cancelled.set()
            # The watcher normally owns this call.  Calling it here closes the
            # polling race when a consumer arrives immediately after set().
            self._inner.cancel()
            self._stop_monitors()
            raise ResourceReservationCancelled(
                "DuckDB Arrow result stream was cancelled"
            )
        if self._timed_out.is_set() or (
            self._deadline is not None
            and time.monotonic() >= self._deadline
        ):
            self._timed_out.set()
            self._inner.close()
            self._stop_monitors()
            raise TimeoutError(
                f"DuckDB query timed out after {self._timeout_value:g} seconds"
            )

    def __next__(self):
        self._raise_if_stopped()
        try:
            batch = next(self._inner)
        except StopIteration:
            self._stop_monitors()
            self._raise_if_stopped()
            raise
        except BaseException:
            self._stop_monitors()
            self._raise_if_stopped()
            raise
        try:
            self._raise_if_stopped()
        except BaseException:
            # Never expose a batch that completed after cancel/deadline.
            raise
        return batch

    def cancel(self) -> None:
        self._cancelled.set()
        try:
            self._inner.cancel()
        finally:
            self._stop_monitors()

    def close(self) -> None:
        self._stop_monitors()
        self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc is not None:
            self.cancel()
        else:
            self.close()

    def __del__(self):
        try:
            self.close()
        except Exception:
            pass


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


def _result_stream_batch_bytes(max_batch_bytes: Optional[int] = None) -> int:
    try:
        configured = int(
            getattr(
                settings,
                "SUPERTABLE_RESULT_STREAM_BATCH_BYTES",
                4 * 1024 * 1024,
            )
        )
    except (TypeError, ValueError, OverflowError):
        configured = 4 * 1024 * 1024
    if configured <= 0:
        configured = 4 * 1024 * 1024
    if max_batch_bytes is None:
        return configured
    if (
        isinstance(max_batch_bytes, bool)
        or not isinstance(max_batch_bytes, int)
        or max_batch_bytes < 1
    ):
        raise ValueError("max_batch_bytes must be a positive integer")
    return min(configured, max_batch_bytes)


def _duckdb_fixed_result_row_bytes(description) -> Optional[int]:
    """Return a conservative fixed-width row charge, or ``None``.

    DuckDB chooses Arrow reader batches by row count only. Variable-width and
    nested outputs therefore use a small configurable upstream fetch cap;
    fixed-width results can safely retain the higher public row cap while the
    final Arrow iterator enforces the exact logical-byte boundary.
    """
    widths = {
        "BOOLEAN": 1,
        "TINYINT": 1,
        "UTINYINT": 1,
        "SMALLINT": 2,
        "USMALLINT": 2,
        "INTEGER": 4,
        "UINTEGER": 4,
        "FLOAT": 4,
        "DATE": 4,
        "BIGINT": 8,
        "UBIGINT": 8,
        "DOUBLE": 8,
        "TIME": 8,
        "TIME WITH TIME ZONE": 8,
        "TIMESTAMP": 8,
        "TIMESTAMP WITH TIME ZONE": 8,
        "TIMESTAMP_NS": 8,
        "TIMESTAMP_MS": 8,
        "TIMESTAMP_S": 8,
        "HUGEINT": 16,
        "UHUGEINT": 16,
        "UUID": 16,
        "INTERVAL": 16,
    }
    total = 0
    try:
        fields = tuple(description or ())
    except TypeError:
        return None
    for field in fields:
        try:
            type_name = str(field[1]).strip().upper()
        except (IndexError, TypeError):
            return None
        if type_name.startswith("DECIMAL("):
            width = 16
        else:
            width = widths.get(type_name)
        if width is None:
            return None
        # A full byte for validity is deliberately conservative relative to
        # Arrow's one-bit bitmap and keeps the division stable for tiny types.
        total += width + 1
    return max(1, total)


def _duckdb_stream_fetch_rows(
    *,
    max_batch_rows: int,
    max_batch_bytes: int,
    description,
) -> int:
    fixed_row_bytes = _duckdb_fixed_result_row_bytes(description)
    if fixed_row_bytes is not None:
        return max(
            1,
            min(max_batch_rows, max_batch_bytes // fixed_row_bytes),
        )
    try:
        variable_cap = int(
            getattr(
                settings,
                "SUPERTABLE_RESULT_STREAM_VARIABLE_FETCH_ROWS",
                16,
            )
        )
    except (TypeError, ValueError, OverflowError):
        variable_cap = 16
    if variable_cap <= 0:
        variable_cap = 16
    variable_cap = min(variable_cap, 4096)
    # A valid MCP row can itself approach the page cap. Keeping only this many
    # unknown-width rows in DuckDB's Arrow conversion bounds that transient to
    # a small, configurable multiple of the response batch budget. The final
    # iterator below still rejects/splits by actual Arrow bytes.
    return max(1, min(max_batch_rows, variable_cap))


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
        self._lifecycle_lock = threading.Lock()
        self._active_queries = 0
        self._cache_eviction_requested = False
        self._setup_context = threading.local()
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

    def _get_connection(
        self,
        temp_dir: str,
        setup_target_callback=None,
        setup_check_callback=None,
    ) -> duckdb.DuckDBPyConnection:
        """Return the persistent connection, creating and configuring it once."""
        if self._con is not None:
            return self._con

        con = _bounded_duckdb_connect(
            temp_dir=temp_dir,
            setup_check=setup_check_callback,
            setup_target_callback=setup_target_callback,
        )
        try:
            if callable(setup_check_callback):
                setup_check_callback()
        except BaseException:
            try:
                con.close()
            finally:
                if callable(setup_target_callback):
                    try:
                        setup_target_callback(None)
                    except BaseException:
                        pass
            raise
        # Only the request thread may publish a fully initialized, still-live
        # connection into the persistent cache. A late orphan worker never
        # reaches this assignment.
        self._con = con
        # httpfs (and both cache settings) are configured lazily on the first
        # query via _ensure_httpfs → configure_httpfs_and_s3.  They cannot be
        # applied here because the httpfs extension is not loaded yet.
        self._httpfs_configured = False
        self._s3_secret_configured = False
        logger.info("[duckdb] persistent connection created")
        return con

    def _acquire_setup_lock(self, setup_check=None) -> None:
        """Acquire the setup lock without waiting past cancel/deadline."""
        if not callable(setup_check):
            self._lock.acquire()
            return
        setup_check()
        while not self._lock.acquire(timeout=0.05):
            setup_check()
        try:
            setup_check()
        except BaseException:
            self._lock.release()
            raise

    def _ensure_httpfs(
        self,
        con: duckdb.DuckDBPyConnection,
        paths: List[str],
    ) -> None:
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
        setup_check = getattr(self._setup_context, "check", None)
        self._acquire_setup_lock(setup_check)
        try:
            if callable(setup_check):
                setup_check()
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
            if callable(setup_check):
                setup_check()
        finally:
            self._lock.release()

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

    def _begin_query_use(self) -> None:
        """Pin this engine while setup, execution, or stream delivery is live."""
        with self._lifecycle_lock:
            self._active_queries += 1

    def _finish_query_use(self) -> None:
        with self._lifecycle_lock:
            self._active_queries = max(0, self._active_queries - 1)
            reset = (
                self._active_queries == 0
                and self._cache_eviction_requested
            )
            if reset:
                # Keep the lifecycle gate closed until reset completes. A new
                # query therefore cannot increment active_queries in the gap
                # between the last stream release and connection close.
                with self._lock:
                    self._reset_connection()

    def request_cache_eviction(self) -> bool:
        """Close an idle evicted engine, or defer close through its last stream.

        Returns ``True`` when the connection was idle and could be closed
        immediately.  No caller should use this as an authorization decision;
        it is cache telemetry only.
        """
        with self._lifecycle_lock:
            self._cache_eviction_requested = True
            idle = self._active_queries == 0
            if idle:
                # Atomic with respect to _begin_query_use; otherwise a query
                # could start after the idle check and have its cursor closed.
                with self._lock:
                    self._reset_connection()
        return idle

    def cache_state(self) -> dict[str, object]:
        """Return data-free lifecycle state for query telemetry."""
        with self._lifecycle_lock:
            active = self._active_queries
            eviction_pending = self._cache_eviction_requested
        with self._lock:
            connection_open = self._con is not None
        return {
            "connection_open": connection_open,
            "active_queries": active,
            "eviction_pending": eviction_pending,
        }

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
            cancel_event: Optional[threading.Event] = None,
            deadline_monotonic: Optional[float] = None,
            *,
            _streaming: bool = False,
            _stream_batch_rows: Optional[int] = None,
            _stream_batch_bytes: Optional[int] = None,
    ) -> Any:
        """Execute while holding a lifecycle lease through stream close."""
        lifecycle_started = time.monotonic()
        lifecycle_deadline = deadline_monotonic
        lifecycle_timeout_value = 0.0
        if lifecycle_deadline is None:
            try:
                supplied_timeout = (
                    float(timeout_sec) if timeout_sec is not None else 0.0
                )
            except (TypeError, ValueError, OverflowError):
                supplied_timeout = 0.0
            if math.isfinite(supplied_timeout) and supplied_timeout > 0:
                lifecycle_timeout_value = supplied_timeout
                lifecycle_deadline = lifecycle_started + supplied_timeout
        else:
            try:
                parsed_deadline = float(lifecycle_deadline)
            except (TypeError, ValueError, OverflowError):
                # _execute_unleased owns the public validation/error wording.
                parsed_deadline = None
            if parsed_deadline is not None and math.isfinite(parsed_deadline):
                lifecycle_deadline = parsed_deadline
                lifecycle_timeout_value = max(
                    0.0, parsed_deadline - lifecycle_started,
                )

        self._begin_query_use()
        stream_owns_lease = False
        try:
            result = self._execute_unleased(
                reflection=reflection,
                parser=parser,
                query_manager=query_manager,
                timer_capture=timer_capture,
                log_prefix=log_prefix,
                engine_config=engine_config,
                explain=explain,
                explain_options=explain_options,
                timeout_sec=timeout_sec,
                cancel_event=cancel_event,
                deadline_monotonic=lifecycle_deadline,
                _streaming=_streaming,
                _stream_batch_rows=_stream_batch_rows,
                _stream_batch_bytes=_stream_batch_bytes,
            )
            if not _streaming:
                return result

            inner = result

            def release_stream_lease() -> None:
                try:
                    inner.close()
                finally:
                    self._finish_query_use()

            wrapped = ArrowBatchStream(
                inner.schema,
                inner,
                close_callback=release_stream_lease,
                cancel_event=cancel_event,
            )
            # From here the inner stream callback owns the lifecycle lease even
            # if monitor construction itself fails.
            stream_owns_lease = True
            try:
                return _DuckDBResultLifecycleStream(
                    wrapped,
                    deadline_monotonic=(
                        float(lifecycle_deadline)
                        if lifecycle_deadline is not None else None
                    ),
                    timeout_value=lifecycle_timeout_value,
                    cancel_event=cancel_event,
                )
            except BaseException:
                wrapped.close()
                raise
        finally:
            if not stream_owns_lease:
                self._finish_query_use()

    def _execute_unleased(
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
            cancel_event: Optional[threading.Event] = None,
            deadline_monotonic: Optional[float] = None,
            *,
            _streaming: bool = False,
            _stream_batch_rows: Optional[int] = None,
            _stream_batch_bytes: Optional[int] = None,
    ) -> Any:
        started_monotonic = time.monotonic()
        try:
            timeout_value = float(timeout_sec) if timeout_sec is not None else 0.0
        except (TypeError, ValueError, OverflowError):
            timeout_value = 0.0
        if deadline_monotonic is None:
            deadline_value = (
                started_monotonic + timeout_value
                if math.isfinite(timeout_value) and timeout_value > 0
                else None
            )
        else:
            try:
                deadline_value = float(deadline_monotonic)
            except (TypeError, ValueError, OverflowError) as exc:
                raise ValueError("query deadline must be finite") from exc
            if not math.isfinite(deadline_value):
                raise ValueError("query deadline must be finite")
            timeout_value = max(0.0, deadline_value - started_monotonic)

        def check_request_boundary() -> None:
            if cancel_event is not None and cancel_event.is_set():
                raise ResourceReservationCancelled("DuckDB query was cancelled")
            if (
                deadline_value is not None
                and time.monotonic() >= deadline_value
            ):
                raise TimeoutError(
                    f"DuckDB query timed out after {timeout_value:g} seconds"
                )

        # Cancellation and one absolute deadline cover parser/RBAC validation,
        # connection creation, extension setup, query execution, and delivery.
        check_request_boundary()
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
        check_request_boundary()
        if _streaming:
            # Keep request validation outside the backend-error boundary so a
            # malformed public batch cap remains a meaningful ValueError.
            resolved_stream_batch_rows = _result_stream_batch_rows(
                _stream_batch_rows
            )
            resolved_stream_batch_bytes = _result_stream_batch_bytes(
                _stream_batch_bytes
            )
        else:
            resolved_stream_batch_rows = None
            resolved_stream_batch_bytes = None
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

        connection_setup_guard = _DuckDBSetupInterruptGuard(
            deadline_monotonic=deadline_value,
            timeout_value=timeout_value,
            cancel_event=cancel_event,
        )

        def publish_connection_setup_target(target) -> None:
            connection_setup_guard.set_target(target)
            connection_setup_guard.raise_if_stopped()

        connection_setup_guard.start()
        try:
            self._acquire_setup_lock(
                connection_setup_guard.raise_if_stopped,
            )
            try:
                connection_setup_guard.raise_if_stopped()
                try:
                    root_con = self._get_connection(
                        temp_dir=query_manager.temp_dir,
                        setup_target_callback=publish_connection_setup_target,
                        setup_check_callback=(
                            connection_setup_guard.raise_if_stopped
                        ),
                    )
                except Exception as initial_error:
                    connection_setup_guard.raise_if_stopped()
                    self._reset_connection()
                    try:
                        root_con = self._get_connection(
                            temp_dir=query_manager.temp_dir,
                            setup_target_callback=publish_connection_setup_target,
                            setup_check_callback=(
                                connection_setup_guard.raise_if_stopped
                            ),
                        )
                    except BaseException as retry_error:
                        if not isinstance(retry_error, Exception):
                            raise
                        connection_setup_guard.raise_if_stopped()
                        # The first and retry failures can both retain connection
                        # configuration in their traceback locals/cause chain.
                        retry_error.__context__ = initial_error
                        safe_error = _safe_duckdb_backend_exception(
                            retry_error, phase="connection setup",
                        )
                        raise safe_error from None
                try:
                    con = root_con.cursor()
                    connection_setup_guard.set_target(con)
                except BaseException as cursor_error:
                    if not isinstance(cursor_error, Exception):
                        raise
                    connection_setup_guard.raise_if_stopped()
                    safe_error = _safe_duckdb_backend_exception(
                        cursor_error, phase="connection setup",
                    )
                    raise safe_error from None
            finally:
                self._lock.release()
            connection_setup_guard.raise_if_stopped()
        finally:
            connection_setup_guard.close()

        check_request_boundary()

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
        setup_paths = [
            path
            for path in [*all_files, *tombstone_paths]
            if path
        ]
        remote_setup = any(
            str(path).lower().startswith(
                ("s3://", "s3a://", "http://", "https://")
            )
            for path in setup_paths
        )
        storage_setup_guard = (
            _DuckDBSetupInterruptGuard(
                deadline_monotonic=deadline_value,
                timeout_value=timeout_value,
                cancel_event=cancel_event,
            )
            if remote_setup else None
        )
        if storage_setup_guard is not None:
            storage_setup_guard.set_target(con)
            storage_setup_guard.start()
        try:
            if storage_setup_guard is not None:
                storage_setup_guard.raise_if_stopped()
            else:
                check_request_boundary()
            if storage_setup_guard is not None:
                self._setup_context.check = (
                    storage_setup_guard.raise_if_stopped
                )
            self._ensure_httpfs(con, setup_paths)
            if storage_setup_guard is not None:
                storage_setup_guard.raise_if_stopped()
            else:
                check_request_boundary()
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
            if storage_setup_guard is not None:
                storage_setup_guard.raise_if_stopped()
            else:
                check_request_boundary()
            safe_error = _safe_duckdb_backend_exception(
                exc, phase="storage setup",
            )
            raise safe_error from None
        finally:
            try:
                del self._setup_context.check
            except AttributeError:
                pass
            if storage_setup_guard is not None:
                storage_setup_guard.close()

        # Create per-query VIEWs. Dropped in finally regardless of outcome.
        created_views: List[str] = []
        # Deletion-vector cache keys acquired this query — released in finally.
        acquired_dv_keys: List[str] = []
        timed_out = threading.Event()
        externally_cancelled = threading.Event()
        cancel_watcher_stop = threading.Event()
        cancel_watcher = None
        watchdog = None
        watchdog_seconds = (
            max(0.0, deadline_value - time.monotonic())
            if deadline_value is not None else 0.0
        )
        check_request_boundary()
        if watchdog_seconds > 0:
            def interrupt_query() -> None:
                timed_out.set()
                try:
                    # A cursor is a query-private DuckDB connection handle. Use
                    # it instead of the shared root so one deadline does not
                    # cancel unrelated concurrent readers.
                    con.interrupt()
                except Exception:
                    pass

            watchdog = threading.Timer(watchdog_seconds, interrupt_query)
            watchdog.daemon = True
            watchdog.start()

        if cancel_event is not None:
            def watch_for_cancellation() -> None:
                while not cancel_watcher_stop.wait(0.05):
                    if cancel_event.is_set():
                        externally_cancelled.set()
                        try:
                            con.interrupt()
                        except Exception:
                            pass
                        return

            cancel_watcher = threading.Thread(
                target=watch_for_cancellation,
                name="supertable-duckdb-cancel",
                daemon=True,
            )
            cancel_watcher.start()

        def check_deadline() -> None:
            if (
                externally_cancelled.is_set()
                or (cancel_event is not None and cancel_event.is_set())
            ):
                raise ResourceReservationCancelled(
                    "DuckDB query was cancelled"
                )
            if timed_out.is_set():
                raise TimeoutError(
                    f"DuckDB query timed out after {timeout_value:g} seconds"
                )
            if (
                deadline_value is not None
                and time.monotonic() >= deadline_value
            ):
                timed_out.set()
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
            cancel_watcher_stop.set()
            if (
                cancel_watcher is not None
                and cancel_watcher is not threading.current_thread()
            ):
                cancel_watcher.join(timeout=1.0)
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
                assert resolved_stream_batch_rows is not None
                assert resolved_stream_batch_bytes is not None
                producer_fetch_rows = _duckdb_stream_fetch_rows(
                    max_batch_rows=resolved_stream_batch_rows,
                    max_batch_bytes=resolved_stream_batch_bytes,
                    description=getattr(query_result, "description", None),
                )
                to_arrow_reader = getattr(query_result, "to_arrow_reader", None)
                if callable(to_arrow_reader):
                    arrow_reader = to_arrow_reader(
                        batch_size=producer_fetch_rows
                    )
                else:
                    # DuckDB 1.1 compatibility; newer versions retain this as
                    # a deprecated alias for ``to_arrow_reader``.
                    arrow_reader = query_result.fetch_record_batch(
                        rows_per_batch=producer_fetch_rows,
                    )
                check_deadline()
                duckdb_producer = _DuckDBArrowBatchIterator(
                    arrow_reader,
                    con,
                    timed_out=timed_out,
                    timeout_value=timeout_value,
                    cancel_event=cancel_event,
                )
                producer = ByteBoundedArrowBatchIterator(
                    duckdb_producer,
                    schema=arrow_reader.schema,
                    max_batch_rows=resolved_stream_batch_rows,
                    max_batch_bytes=resolved_stream_batch_bytes,
                )
                result = ArrowBatchStream(
                    arrow_reader.schema,
                    producer,
                    close_callback=cleanup_query,
                    cancel_event=cancel_event,
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
            if externally_cancelled.is_set() or (
                cancel_event is not None and cancel_event.is_set()
            ):
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise ResourceReservationCancelled(
                    "DuckDB query was cancelled"
                ) from None
            if timed_out.is_set() and not isinstance(exc, TimeoutError):
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise TimeoutError(
                    f"DuckDB query timed out after {timeout_value:g} seconds"
                ) from None
            if _is_refreshable_remote_auth_error(exc):
                _scrub_exception_chain(
                    exc, replacement="DuckDB backend detail redacted"
                )
                raise DuckDBPresignRefreshRequired(
                    "DuckDB remote authorization expired before result delivery"
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
            max_batch_bytes: Optional[int] = None,
            cancel_event: Optional[threading.Event] = None,
            deadline_monotonic: Optional[float] = None,
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
            cancel_event=cancel_event,
            deadline_monotonic=deadline_monotonic,
            _streaming=True,
            _stream_batch_rows=max_batch_rows,
            _stream_batch_bytes=max_batch_bytes,
        )
