# route: supertable.data_reader

from __future__ import annotations

import hashlib
import hmac
import json
import math
import re
import threading
import time
from datetime import date, datetime, timezone
from enum import Enum
from typing import Callable, Optional, Tuple, Any, List, Dict, Iterable, Mapping

import pandas as pd
import polars as pl
import pyarrow as pa
import sqlglot
from sqlglot import exp

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.errors import SuperTableNotFoundError, TableNotFoundError
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.utils.timer import Timer
from supertable.utils.diagnostic_redaction import safe_exception_type
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.utils.snapshot import (
    collect_share_row_filters,
    complete_snapshot_payload,
)
from supertable.plan_extender import extend_execution_plan
from supertable.monitoring_writer import (
    MonitoringDurabilityError,
    MonitoringPostExecutionError,
)
from supertable.engine.plan_stats import PlanStats
from supertable.engine.engine_common import (
    redact_url_credentials,
    validate_rbac_binding_stability,
)
from supertable.rbac.access_control import (
    restrict_read_access,  # noqa: F401
    validate_policy_fingerprint,
)

from supertable.engine.data_estimator import DataEstimator
from supertable.engine.executor import Executor
from supertable.engine.engine_enum import Engine as engine
from supertable.engine.island_resources import ResourceReservationCancelled
from supertable.data_classes import (
    MAX_TOMBSTONE_PROVIDER_IDENTITY_BYTES,
    RbacViewDef,
    TombstoneDef,
    TombstoneSegmentDef,
)
from supertable.processing import load_tombstone_manifest_from_storage
from supertable.redis_catalog import RedisCatalog
from supertable.system_query import classify_query, CommandKind
from supertable.tombstone_manifest_v2 import (
    TOMBSTONE_FORMAT_V1,
    TOMBSTONE_FORMAT_V2,
    TOMBSTONE_FORMAT_V3,
    validate_snapshot_tombstone_state,
)
from supertable.row_identity import ODATA_INTERNAL_ROWID_COLUMN
from supertable.odata_continuation import (
    ODataContinuationBoundary,
    bind_odata_continuation_boundary,
    normalized_odata_order,
    validate_odata_continuation_boundary,
)


class Status(Enum):
    OK = "ok"
    ERROR = "error"


_QUERY_VALIDATION_ERROR = "Query is invalid or unsupported"
_QUERY_AGGREGATE_ERROR = "Query aggregate resolution failed"
_QUERY_TARGET_ERROR = "Query target is unavailable"
_QUERY_EXECUTION_ERROR = "Query execution failed"
_QUERY_STREAM_ERROR = "Query result stream failed"


def _log_query_phase_failure(phase: str, exc: BaseException) -> None:
    """Log a useful phase and exception class without attacker-controlled text."""
    logger.warning(
        "%s failed; error_type=%s",
        phase,
        safe_exception_type(exc),
    )


# Process-local capability used only by ``query_odata_sql_stream``.  The raw
# boundary is deliberately absent from ordinary SQL reader signatures; the
# private plumbing below also refuses a boundary that did not enter through
# that trusted wrapper.
_ODATA_CONTINUATION_CAPABILITY = object()


def _odata_identity_binding(parser: SQLParser) -> str:
    """Validate the deliberately narrow trusted OData SELECT shape."""
    parsed = getattr(parser, "_parsed", None)
    if not isinstance(parsed, exp.Select):
        raise ValueError("OData identity requires one direct SELECT")
    if any(parsed.args.get(name) is not None for name in (
        "with_", "group", "having", "qualify", "distinct",
    )):
        raise ValueError("OData identity does not support transformed rows")
    if parsed.args.get("joins"):
        raise ValueError("OData identity does not support joins")
    if any(isinstance(node, (exp.Subquery, exp.SetOperation)) for node in parsed.walk()):
        raise ValueError("OData identity requires one direct table")
    if any(isinstance(node, exp.AggFunc) for node in parsed.walk()):
        raise ValueError("OData identity does not support aggregate rows")
    if any(isinstance(node, exp.Placeholder) for node in parsed.walk()):
        raise ValueError("OData identity SQL cannot contain parameters")
    projections = list(parsed.expressions or ())
    if not projections or any(not isinstance(item, exp.Column) for item in projections):
        raise ValueError("OData identity requires a direct column projection")
    for column in parsed.find_all(exp.Column):
        if str(column.name).casefold() == ODATA_INTERNAL_ROWID_COLUMN.casefold():
            raise ValueError("OData internal identity cannot be requested in SQL")
    # OData paging is deterministic only over direct columns.  Validate this
    # even on the first page so a later continuation cannot reinterpret a
    # previously accepted expression or NULLS-FIRST order.
    normalized_odata_order(parsed)

    table_defs = parser.get_table_tuples()
    physical = parser.get_physical_tables()
    if len(table_defs) != 1 or len(physical) != 1:
        raise ValueError("OData identity requires exactly one physical table")
    table_def = table_defs[0]
    if (
        str(table_def.super_name).casefold()
        == str(table_def.simple_name).casefold()
    ):
        raise ValueError("OData identity is unavailable for aggregate relations")
    return str(table_def.alias)


def _cancel_and_close_stream(stream: Any) -> None:
    """Terminate a live result stream that cannot be returned to its caller."""
    cancel = getattr(stream, "cancel", None)
    try:
        if callable(cancel):
            cancel()
    except Exception as exc:
        logger.warning(
            "Arrow result cancellation failed; error_type=%s",
            safe_exception_type(exc),
        )
    finally:
        close = getattr(stream, "close", None)
        try:
            if callable(close):
                close()
        except Exception as exc:
            logger.warning(
                "Arrow result close failed; error_type=%s",
                safe_exception_type(exc),
            )


class _MonitoredResultStream:
    """Arrow stream wrapper that finalizes monitoring at the real outcome.

    Creating a stream is not a successful read: production can fail, the
    serialized-byte consumer can cancel, or a caller can close early.  This
    wrapper counts yielded rows/Arrow bytes and emits exactly one monitoring
    outcome on exhaustion, cancellation, close, or producer failure.
    """

    def __init__(
        self,
        inner: Any,
        finalize: Callable[[str, Optional[str], int, int], None],
    ) -> None:
        self._inner = inner
        self._finalize_callback = finalize
        self.schema = inner.schema
        self._finalize_condition = threading.Condition()
        self._rows = 0
        self._bytes = 0
        self._finalized = False
        self._finalize_complete = False
        self._finalizing_thread: Optional[int] = None
        self._finalize_error: BaseException | None = None
        add_terminal_callback = getattr(inner, "add_terminal_callback", None)
        if callable(add_terminal_callback):
            add_terminal_callback(self._inner_terminal)

    def __iter__(self):
        return self

    def _finish(self, status: str, message: Optional[str]) -> None:
        owner = threading.get_ident()
        with self._finalize_condition:
            if self._finalized:
                # A concurrent terminal path must observe completion of the
                # one callback that won.  Callback re-entry on its own thread
                # returns immediately instead of deadlocking.
                while (
                    not self._finalize_complete
                    and self._finalizing_thread != owner
                ):
                    self._finalize_condition.wait()
                if self._finalize_error is not None:
                    raise self._finalize_error
                return
            self._finalized = True
            self._finalizing_thread = owner
            rows = self._rows
            size = self._bytes

        try:
            self._finalize_callback(status, message, rows, size)
        except BaseException as exc:
            with self._finalize_condition:
                self._finalize_error = exc
                self._finalize_complete = True
                self._finalizing_thread = None
                self._finalize_condition.notify_all()
            raise
        else:
            with self._finalize_condition:
                self._finalize_complete = True
                self._finalizing_thread = None
                self._finalize_condition.notify_all()

    def _record_batch(self, batch: Any) -> None:
        with self._finalize_condition:
            while (
                self._finalized
                and not self._finalize_complete
                and self._finalizing_thread != threading.get_ident()
            ):
                self._finalize_condition.wait()
            if self._finalized:
                if self._finalize_error is not None:
                    raise self._finalize_error
                raise RuntimeError(
                    "Result stream finalized while producing an Arrow batch"
                )
            self._rows += max(0, int(getattr(batch, "num_rows", 0)))
            self._bytes += max(0, int(getattr(batch, "nbytes", 0)))

    def _is_finalized(self) -> bool:
        with self._finalize_condition:
            return self._finalized

    def _inner_terminal(self, kind: str) -> None:
        if kind == "completed":
            status, message = Status.OK.value, None
        elif kind == "timed_out":
            status, message = Status.ERROR.value, "result stream timed out"
        elif kind == "cancelled":
            status, message = Status.ERROR.value, "result stream cancelled"
        elif kind == "failed":
            status, message = Status.ERROR.value, _QUERY_STREAM_ERROR
        else:
            status = Status.ERROR.value
            message = "result stream closed before exhaustion"
        try:
            self._finish(status, message)
        except BaseException as exc:
            # A watcher-thread callback cannot raise into the request thread.
            # _finish retains the error so any later stream operation observes
            # it; protected failure audit emission has already been attempted.
            logger.error(
                "stream terminal monitoring failed; error_type=%s",
                safe_exception_type(exc),
            )

    def __next__(self):
        try:
            batch = next(self._inner)
        except StopIteration:
            close = getattr(self._inner, "close", None)
            try:
                if callable(close):
                    close()
            except BaseException as exc:
                try:
                    self._finish(Status.ERROR.value, _QUERY_STREAM_ERROR)
                except BaseException as monitoring_exc:
                    setattr(
                        monitoring_exc, "stream_error_type", safe_exception_type(exc),
                    )
                    raise monitoring_exc from None
                if isinstance(exc, Exception):
                    raise RuntimeError(_QUERY_STREAM_ERROR) from None
                raise
            self._finish(Status.OK.value, None)
            raise
        except BaseException as exc:
            try:
                self._finish(Status.ERROR.value, _QUERY_STREAM_ERROR)
            except BaseException as monitoring_exc:
                setattr(
                    monitoring_exc, "stream_error_type", safe_exception_type(exc),
                )
                raise monitoring_exc from None
            if isinstance(exc, Exception):
                raise RuntimeError(_QUERY_STREAM_ERROR) from None
            raise
        self._record_batch(batch)
        if bool(getattr(self._inner, "successful_completion", False)):
            # A bounded export reaching its exact authorized row budget is a
            # completed query, not an abandoned stream. Finalize now so a
            # caller that closes immediately after consuming the last batch
            # cannot turn the read/AUTO observation into a false failure.
            self._finish(Status.OK.value, None)
        return batch

    def cancel(self) -> None:
        cancel = getattr(self._inner, "cancel", None)
        backend_error: BaseException | None = None
        try:
            if callable(cancel):
                cancel()
            else:
                close = getattr(self._inner, "close", None)
                if callable(close):
                    close()
        except BaseException as exc:
            backend_error = exc
        if backend_error is not None:
            try:
                self._finish(Status.ERROR.value, _QUERY_STREAM_ERROR)
            except BaseException as monitoring_exc:
                setattr(
                    monitoring_exc,
                    "stream_error_type",
                    safe_exception_type(backend_error),
                )
                raise monitoring_exc from None
            if isinstance(backend_error, Exception):
                raise RuntimeError(_QUERY_STREAM_ERROR) from None
            raise backend_error
        self._finish(Status.ERROR.value, "result stream cancelled")

    def close(self) -> None:
        close = getattr(self._inner, "close", None)
        backend_error: BaseException | None = None
        try:
            if callable(close):
                close()
        except BaseException as exc:
            backend_error = exc
        if backend_error is not None:
            try:
                self._finish(Status.ERROR.value, _QUERY_STREAM_ERROR)
            except BaseException as monitoring_exc:
                setattr(
                    monitoring_exc,
                    "stream_error_type",
                    safe_exception_type(backend_error),
                )
                raise monitoring_exc from None
            if isinstance(backend_error, Exception):
                raise RuntimeError(_QUERY_STREAM_ERROR) from None
            raise backend_error
        self._finish(
            Status.ERROR.value,
            "result stream closed before exhaustion",
        )

    @property
    def closed(self) -> bool:
        return bool(getattr(self._inner, "closed", self._is_finalized()))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc is not None and not self._is_finalized():
            try:
                self.cancel()
            except Exception:
                if not isinstance(exc, MonitoringPostExecutionError):
                    raise
        else:
            self.close()

    def __del__(self):
        try:
            if not self._is_finalized():
                self.cancel()
        except Exception:
            pass


class _RowBudgetResultStream:
    """Defence-in-depth row ceiling around an already SQL-bounded stream."""

    def __init__(self, inner: Any, max_total_rows: int) -> None:
        if (
            isinstance(max_total_rows, bool)
            or not isinstance(max_total_rows, int)
            or max_total_rows <= 0
        ):
            raise ValueError("max_total_rows must be a positive integer")
        self._inner = inner
        self._remaining = max_total_rows
        self._closed = False
        self._successful_completion = False
        self.schema = inner.schema

    def __iter__(self):
        return self

    def __next__(self):
        if self._closed or self._remaining <= 0:
            raise StopIteration
        batch = next(self._inner)
        rows = max(0, int(getattr(batch, "num_rows", 0)))
        if rows <= self._remaining:
            self._remaining -= rows
            if self._remaining == 0:
                self._inner.close()
                self._closed = True
                self._successful_completion = True
            return batch

        # The outer SQL LIMIT should make this unreachable.  If a backend ever
        # violates it, expose only the authorized budget and stop the producer
        # before another batch can be fetched.
        bounded = batch.slice(0, self._remaining)
        self._remaining = 0
        cancel = getattr(self._inner, "cancel", None)
        if callable(cancel):
            cancel()
        else:
            self._inner.close()
        self._closed = True
        self._successful_completion = True
        return bounded

    def cancel(self) -> None:
        if self._closed:
            return
        self._closed = True
        cancel = getattr(self._inner, "cancel", None)
        if callable(cancel):
            cancel()
        else:
            self._inner.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._inner.close()

    @property
    def closed(self) -> bool:
        # ``_closed`` means this wrapper has requested termination. An
        # ArrowBatchStream with an active next() does not become quiescent until
        # that call unwinds, so consumers that own an external lease must observe
        # the inner stream's stronger lifecycle signal when it is available.
        try:
            inner_closed = getattr(self._inner, "closed")
        except AttributeError:
            return self._closed
        return bool(inner_closed)

    @property
    def successful_completion(self) -> bool:
        return self._successful_completion

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class _QueryOutResultStream:
    """Refresh a caller-owned metadata dict whenever a stream terminates."""

    def __init__(self, inner: Any, refresh: Callable[[], None]) -> None:
        self._inner = inner
        self._refresh = refresh
        self.schema = inner.schema

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self._inner)
        except BaseException:
            self._refresh()
            raise

    def cancel(self) -> None:
        try:
            cancel = getattr(self._inner, "cancel", None)
            if callable(cancel):
                cancel()
            else:
                self._inner.close()
        finally:
            self._refresh()

    def close(self) -> None:
        try:
            self._inner.close()
        finally:
            self._refresh()

    @property
    def closed(self) -> bool:
        return bool(getattr(self._inner, "closed", False))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if exc is not None:
            self.cancel()
        else:
            self.close()


_SPARK_UNSAFE_PREDICATE_LANES = frozenset({
    "numeric_cast", "date", "timestamp", "timestamptz",
})

# These are predicate-lane names exported by the join kernel.  Explicit
# DuckDB preserves the DATE / timestamp-with(out)-timezone distinctions in the
# stats artifact.  Spark deliberately reads Parquet NTZ timestamps as LTZ and
# also supports legacy date/timestamp rebasing; AUTO may choose Spark only
# after estimation.  Raw temporal footer ordering is therefore not proof for
# AUTO/Spark, while integral equality/order is common to both executors.
_DUCKDB_JOIN_PRUNING_LANES = frozenset({
    "numeric", "date", "timestamp", "timestamptz",
})
_COMMON_JOIN_PRUNING_LANES = frozenset({"numeric"})

def _positive_budget(name: str, fallback: int) -> int:
    try:
        value = int(getattr(settings, name, fallback))
    except (TypeError, ValueError, OverflowError):
        value = fallback
    return value if value > 0 else fallback


def _configured_result_stream_batch_rows() -> int:
    """Return the public Arrow batch cap shared by interactive/export reads."""
    try:
        value = int(
            getattr(settings, "SUPERTABLE_RESULT_STREAM_BATCH_ROWS", 256)
        )
    except (TypeError, ValueError, OverflowError):
        value = 256
    return max(1, min(value, 4096))


def _configured_result_stream_batch_bytes() -> int:
    """Return the hard logical-byte cap for one public Arrow batch."""
    try:
        value = int(
            getattr(
                settings,
                "SUPERTABLE_RESULT_STREAM_BATCH_BYTES",
                4 * 1024 * 1024,
            )
        )
    except (TypeError, ValueError, OverflowError):
        value = 4 * 1024 * 1024
    return value if value > 0 else 4 * 1024 * 1024


def _caller_deadline(timeout_sec: Optional[float]) -> Optional[float]:
    """Validate a caller timeout and convert it to an absolute deadline."""
    if timeout_sec is None:
        return None
    if isinstance(timeout_sec, bool):
        raise ValueError("timeout_sec must be a finite positive number")
    try:
        timeout = float(timeout_sec)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("timeout_sec must be a finite positive number") from None
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout_sec must be a finite positive number")
    return time.monotonic() + timeout


def _validate_query_text_size(sql: str) -> None:
    """Cheap guard that must run before SQLGlot sees attacker-sized input."""
    if len(str(sql).encode("utf-8")) > _positive_budget(
        "SUPERTABLE_MAX_QUERY_BYTES", 64 * 1024,
    ):
        raise ValueError("SQL text exceeds the configured query-size budget")


def _ensure_request_active(
    deadline_monotonic: Optional[float],
    cancel_event: Optional[threading.Event],
) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise ResourceReservationCancelled("query was cancelled")
    if deadline_monotonic is None:
        return
    try:
        deadline = float(deadline_monotonic)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("query deadline must be finite") from None
    if not math.isfinite(deadline):
        raise ValueError("query deadline must be finite")
    if time.monotonic() >= deadline:
        raise TimeoutError("Query deadline expired")


def _validate_query_complexity(sql: str) -> None:
    """Reject oversized/deep SELECT syntax before planning or execution."""
    _validate_query_text_size(sql)
    try:
        statements = [
            statement
            for statement in sqlglot.parse(sql, read="duckdb")
            if statement is not None
        ]
    except Exception:
        raise ValueError("SQL query is invalid") from None
    if len(statements) != 1 or statements[0] is None:
        raise ValueError("Exactly one SQL statement is required")

    max_nodes = _positive_budget("SUPERTABLE_MAX_QUERY_AST_NODES", 4096)
    max_joins = _positive_budget("SUPERTABLE_MAX_QUERY_JOINS", 32)
    max_depth = _positive_budget("SUPERTABLE_MAX_QUERY_NESTING", 32)
    node_count = 0
    join_count = 0
    stack = [(statements[0], 1)]
    while stack:
        node, depth = stack.pop()
        node_count += 1
        if node_count > max_nodes:
            raise ValueError("SQL query exceeds the AST-node budget")
        if depth > max_depth:
            raise ValueError("SQL query exceeds the nesting budget")
        if isinstance(node, exp.Join):
            join_count += 1
            if join_count > max_joins:
                raise ValueError("SQL query exceeds the join budget")
        stack.extend((child, depth + 1) for child in node.iter_expressions())


def _validated_share_row_filter(raw_filter: object) -> str:
    """Return one canonical, table-local DuckDB predicate or fail closed.

    Linked-share filters are persisted SQL rather than the structured RBAC
    filter grammar.  They still execute inside the protected view, so they may
    not contain statement separators, subqueries, table sources, or mutation
    nodes.  Parsing the complete wrapper also makes later Spark transpilation
    operate on an AST-derived predicate instead of concatenated raw text.
    """
    if not isinstance(raw_filter, str) or not raw_filter.strip():
        raise RuntimeError("Linked-share row filter is invalid")
    if len(raw_filter.encode("utf-8")) > 64 * 1024:
        raise RuntimeError("Linked-share row filter exceeds the size limit")
    try:
        statements = sqlglot.parse(
            f"SELECT 1 WHERE {raw_filter}", read="duckdb"
        )
    except Exception:
        raise RuntimeError("Linked-share row filter is invalid") from None
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise RuntimeError("Linked-share row filter is invalid")
    select = statements[0]
    where = select.args.get("where")
    if not isinstance(where, exp.Where):
        raise RuntimeError("Linked-share row filter is invalid")
    forbidden = tuple(
        node_type
        for name in (
            "Table", "Subquery", "Insert", "Update", "Delete", "Merge",
            "Create", "Drop", "Alter", "Command", "Copy", "Transaction",
            "Grant", "TruncateTable",
        )
        if isinstance((node_type := getattr(exp, name, None)), type)
    )
    if forbidden and any(isinstance(node, forbidden) for node in where.walk()):
        raise RuntimeError("Linked-share row filter must be table-local")
    allowed_function_types = tuple(
        node_type
        for name in (
            # SQLGlot models boolean conjunction/disjunction as Func nodes even
            # though they are operators rather than callable SQL functions.
            "And", "Or",
            "Cast", "TryCast",
            "Abs", "Ceil", "Floor", "Round",
            "Lower", "Upper", "Trim", "LTrim", "RTrim", "Length",
            "Substring", "Coalesce", "IfNull", "Nullif",
        )
        if isinstance((node_type := getattr(exp, name, None)), type)
    )
    for node in where.walk():
        if isinstance(node, exp.Func) and not isinstance(
            node, allowed_function_types,
        ):
            # Unknown/anonymous functions include DuckDB settings, filesystem,
            # extension, sequence, and table-function surfaces. A persisted
            # authorization predicate is not a general SQL execution context.
            raise RuntimeError(
                "Linked-share row filter uses an unavailable function"
            )
    return where.this.sql(dialect="duckdb")


def _effective_read_policy_fingerprint(
    role_policy_fingerprints: Dict[str, str],
    reflection: Any,
) -> str:
    """Seal the exact data-free authorization state used by one reflection."""
    namespaces = []
    for raw_namespace, raw_fingerprint in sorted(
        role_policy_fingerprints.items(), key=lambda item: str(item[0]).casefold(),
    ):
        fingerprint = validate_policy_fingerprint(
            raw_fingerprint,
            label="resolved role policy fingerprint",
        )
        assert fingerprint is not None
        namespaces.append({
            "namespace": str(raw_namespace).casefold(),
            "role_policy_fingerprint": fingerprint,
        })

    snapshots = []
    for snapshot in sorted(
        (getattr(reflection, "supers", None) or []),
        key=lambda item: (
            str(getattr(item, "super_name", "")).casefold(),
            str(getattr(item, "simple_name", "")).casefold(),
        ),
    ):
        share_policy_fingerprint = getattr(
            snapshot, "share_policy_fingerprint", None,
        )
        if share_policy_fingerprint is not None:
            share_policy_fingerprint = validate_policy_fingerprint(
                share_policy_fingerprint,
                label="linked-share policy fingerprint",
            )
        snapshots.append({
            "namespace": str(getattr(snapshot, "super_name", "")).casefold(),
            "table": str(getattr(snapshot, "simple_name", "")).casefold(),
            "share_policy_fingerprint": share_policy_fingerprint or "",
            "share_row_filter": str(
                getattr(snapshot, "share_row_filter", None) or ""
            ),
        })

    protected_views = []
    for raw_alias, view in sorted(
        (getattr(reflection, "rbac_views", None) or {}).items(),
        key=lambda item: str(item[0]).casefold(),
    ):
        allowed = list(getattr(view, "allowed_columns", None) or [])
        excluded = list(getattr(view, "excluded_columns", None) or [])
        protected_views.append({
            "alias": str(raw_alias).casefold(),
            "allowed_columns": (
                ["*"] if allowed == ["*"]
                else sorted(str(column).casefold() for column in allowed)
            ),
            "excluded_columns": sorted(
                str(column).casefold() for column in excluded
            ),
            "where_clause": str(getattr(view, "where_clause", "") or ""),
        })

    identity = {
        "version": 1,
        "namespaces": namespaces,
        "snapshots": snapshots,
        "protected_views": protected_views,
    }
    return hashlib.sha256(json.dumps(
        identity,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")).hexdigest()


def _redact_storage_credentials(message: object) -> str:
    """Remove query/fragment and URL user-info from public read errors."""
    return redact_url_credentials(message)


def _engine_safe_join_pruning_lanes(requested_engine: engine):
    if requested_engine in (engine.AUTO, engine.SPARK_SQL):
        return _COMMON_JOIN_PRUNING_LANES
    return _DUCKDB_JOIN_PRUNING_LANES


def _engine_safe_predicate_constraints(
    predicate_constraints: Dict,
    requested_engine: engine,
) -> Dict:
    """Remove footer comparisons whose semantics depend on the executor.

    SparkThriftExecutor sets ``spark.sql.parquet.inferTimestampNTZ.enabled``
    to false so PyArrow/Polars ``TIMESTAMP(..., isAdjustedToUTC=false)`` files
    are read as Spark TIMESTAMP_LTZ.  A timezone-less Spark TIMESTAMP literal
    is interpreted in ``spark.sql.session.timeZone``; its UTC instant can
    therefore match a footer wall time with a *different* clock value.  Raw
    footer-vs-literal comparison could drop that contributing file.  Spark's
    ANSI-off narrowing integer casts can also wrap on overflow; a cast-derived
    bound must not be treated as its pre-cast Python integer.

    AUTO may choose Spark only after estimation, so it needs the same guard.
    Explicit DuckDB reads retain these constraints: DuckDB preserves the
    DATE/TIMESTAMP/TIMESTAMPTZ distinctions used by the stats lanes.  DATE is
    also gated for AUTO/Spark because legacy Parquet calendar rebasing can make
    a pre-1582 footer day number differ from the executor's logical date.

    Empty occurrences are omitted at the table level.  They mean at least one
    physical occurrence is unconstrained, so the shared file list cannot be
    pruned by its own WHERE anyway; omitting the key also avoids loading stats
    solely for a guaranteed no-op.
    """
    if requested_engine not in (engine.AUTO, engine.SPARK_SQL):
        return predicate_constraints

    filtered: Dict = {}
    try:
        for table_key, occurrences in predicate_constraints.items():
            safe_occurrences = [
                {
                    column: predicate
                    for column, predicate in occurrence.items()
                    if getattr(predicate, "lane", None)
                    not in _SPARK_UNSAFE_PREDICATE_LANES
                }
                for occurrence in occurrences
            ]
            if safe_occurrences and all(safe_occurrences):
                filtered[table_key] = safe_occurrences
    except Exception:
        # Analysis is an optional optimisation.  A malformed/custom result is
        # not allowed to make an AUTO/Spark read narrower or fail.
        return {}
    return filtered


from collections import defaultdict
from typing import List, Tuple



class DataReader:
    """
    Facade — preserves the original interface; now delegates:
      - Estimation to DataEstimator
      - Execution to Executor (DuckDB/Spark)
    """

    def __init__(
        self,
        super_name: str,
        organization: str,
        query: str,
        source: str = "sdk",
        *,
        _allow_bounded_collection_aggregates: bool = False,
        audit_actor_id: str = "",
        audit_actor_username: str = "",
    ):
        self.super_name = super_name
        self.organization = organization
        self.query = query
        # Query origin surfaced in the reads monitoring tab. "sdk" is the
        # default for direct SDK callers; the API/OData/MCP entry points
        # pass "api"/"odata"/"mcp" so each query records where it came from.
        self.source = source
        self.audit_actor_id = str(audit_actor_id or "")
        self.audit_actor_username = str(audit_actor_username or "")
        self._allow_bounded_collection_aggregates = bool(
            _allow_bounded_collection_aggregates
        )

        self.storage: StorageInterface = get_storage()

        self.timer: Optional[Timer] = None
        self.plan_stats: Optional[PlanStats] = None
        self.query_plan_manager: Optional[QueryPlanManager] = None
        self.role_policy_fingerprint = ""
        self.effective_policy_fingerprint = ""

        self._log_ctx = ""

    def _lp(self, msg: str) -> str:
        return f"{self._log_ctx}{msg}"

    def _emit_successful_query_audit(
        self,
        *,
        role_name: str,
        engine_used: Any,
        result_rows: int,
        result_columns: int,
        outcome: str = "success",
    ) -> None:
        """Record one protected event after a query has actually succeeded.

        The read facade has an authorization role but no authenticated-user
        identity, so the role stays in structured detail instead of being
        misrepresented as a user.  The public audit boundary removes raw SQL
        before any queue or backend can observe the event.
        """
        from supertable.audit import (
            Actions,
            ActorType,
            EventCategory,
            Outcome,
            emit,
        )

        query_manager = self.query_plan_manager
        query_id = str(getattr(query_manager, "query_id", "") or "")[:128]
        query_hash = str(
            getattr(query_manager, "query_hash", "") or ""
        )[:128]
        actual_engine = str(
            getattr(engine_used, "value", engine_used) or ""
        )[:64]
        audit_outcome = (
            Outcome.SUCCESS if outcome == "success" else Outcome.FAILURE
        )
        emit(
            category=EventCategory.DATA_ACCESS,
            action=Actions.QUERY_EXECUTE,
            organization=self.organization,
            actor_type=ActorType.SYSTEM,
            actor_id=self.audit_actor_id,
            actor_username=self.audit_actor_username,
            resource_type="query",
            resource_id=query_id,
            super_name=self.super_name,
            detail={
                "sql": self.query,
                "query_id": query_id,
                "query_hash": query_hash,
                "authorization_role": role_name,
                "source": self.source,
                "engine": actual_engine,
                "row_count": max(0, int(result_rows)),
                "column_count": max(0, int(result_columns)),
                "outcome": outcome,
            },
            outcome=audit_outcome,
        )

    def _assert_targets_exist(self, physical_tables) -> None:
        """Fail fast if any referenced (super, simple) is missing in Redis.

        The read path must never create catalog entries as a side effect
        of resolving a query. ``SuperTable`` / ``SimpleTable``
        constructors used to do exactly that for callers that didn't pass
        ``create_if_missing=False`` — this guard is the SDK-level
        invariant that says "reads cannot mint tables".

        Raises:
            SuperTableNotFoundError: when the supertable's
                ``meta:root`` pointer is missing.
            TableNotFoundError: when the simple table's
                ``meta:leaf:doc:{simple}`` pointer is missing.
        """
        if not physical_tables:
            return
        # One catalog handle for the whole loop — cheaper than letting
        # each .exists() call open a fresh connection.
        catalog = RedisCatalog()
        # Dedup by (super, simple) — SQL may mention the same table
        # multiple times via different aliases.
        seen = set()
        for td in physical_tables:
            super_name = td.super_name
            simple_name = td.simple_name
            if not super_name or not simple_name:
                continue
            key = (super_name, simple_name)
            if key in seen:
                continue
            seen.add(key)
            if not catalog.root_exists(self.organization, super_name):
                raise SuperTableNotFoundError(self.organization, super_name)
            # ``super.super`` is the intentional aggregate relation. It is
            # backed by the parent's leaf set rather than by a same-named leaf.
            if str(super_name).casefold() == str(simple_name).casefold():
                continue
            if not catalog.leaf_exists(self.organization, super_name, simple_name):
                raise TableNotFoundError(
                    self.organization, super_name, simple_name
                )

    def _resolve_aggregate_children(self, physical_tables):
        """Pin the child names behind every aggregate relation.

        The resulting map is shared by RBAC and the estimator.  This prevents a
        child created after authorization from entering the independently run
        estimator SCAN, and makes every expanded relation an explicit policy
        target before any files are selected.
        """
        aggregate_keys = {
            (str(td.super_name).casefold(), str(td.simple_name).casefold()): (
                str(td.super_name), str(td.simple_name)
            )
            for td in physical_tables
            if td.super_name
            and td.simple_name
            and str(td.super_name).casefold() == str(td.simple_name).casefold()
        }
        if not aggregate_keys:
            return {}

        catalog = RedisCatalog()
        resolved = {}
        for key, (super_name, simple_name) in aggregate_keys.items():
            children = []
            seen = set()
            for item in catalog.scan_leaf_items(
                self.organization, super_name, count=512,
            ):
                child = str(item.get("simple") or "")
                if not child or (
                    child.startswith("__") and child.endswith("__")
                ):
                    continue
                folded = child.casefold()
                if folded in seen:
                    raise RuntimeError(
                        f"Catalog contains case-colliding children for "
                        f"{super_name}.{simple_name}"
                    )
                seen.add(folded)
                children.append(child)
            resolved[key] = tuple(children)
        return resolved

    def _resolve_latest_stats_context(
        self,
        super_name: str,
        simple_name: str,
        *,
        include_bounds: bool = False,
    ) -> Any:
        """Return the latest stats pointer, bounds, and share-filter state.

        Prefers the leaf payload (already in Redis); falls back to reading the
        snapshot JSON from storage.  The filter marker is read from the same
        pinned leaf/snapshot context as the pointer so ``SHOW STATS`` cannot
        authorize one version and then expose another version's raw artifact.

        The compatibility default remains ``(stats_file, filtered)``. The
        diagnostic caller requests ``(stats_file, stats_rows, filtered)`` so
        the immutable artifact's row bound is pinned by that same document.

        A malformed non-null ``_row_filter`` is treated as present.  Corrupt
        authorization metadata must disable raw statistics rather than silently
        widening access.
        """
        def has_row_filter(document: object) -> bool:
            try:
                return bool(collect_share_row_filters(document))
            except RuntimeError:
                # SHOW STATS needs only the deny/allow decision.  Preserve its
                # generic denial path while treating corrupt authorization
                # metadata exactly like a present restriction.
                return True

        def result(document: Mapping[str, Any], filtered: bool) -> Any:
            stats_file = document.get("stats_file")
            if include_bounds:
                return stats_file, document.get("stats_rows"), filtered
            return stats_file, filtered

        catalog = RedisCatalog()
        leaf = catalog.get_leaf(self.organization, super_name, simple_name)
        if not isinstance(leaf, dict):
            return (None, None, False) if include_bounds else (None, False)
        payload = leaf.get("payload")
        filtered = has_row_filter(leaf) or has_row_filter(payload)
        complete_payload = complete_snapshot_payload(
            payload,
            expected_version=leaf.get("version"),
            require_policy_marker=True,
        )
        if complete_payload is not None:
            return result(
                complete_payload,
                filtered or has_row_filter(complete_payload),
            )
        path = leaf.get("path")
        if not path:
            return (None, None, filtered) if include_bounds else (None, filtered)
        from supertable.super_table import SuperTable
        snapshot = SuperTable(
            super_name, self.organization, create_if_missing=False,
        ).read_simple_table_snapshot(path)
        if not isinstance(snapshot, dict):
            return (None, None, filtered) if include_bounds else (None, filtered)
        return result(snapshot, filtered or has_row_filter(snapshot))

    def _resolve_latest_stats_file(
        self, super_name: str, simple_name: str,
    ) -> Optional[str]:
        """Compatibility wrapper returning only the latest stats pointer."""
        return self._resolve_latest_stats_context(super_name, simple_name)[0]

    def _execute_show_stats(
        self, command, role_name: str,
    ) -> Tuple[pd.DataFrame, Status, Optional[str]]:
        """Return role-visible rows from a table's latest statistics parquet.

        Reads-never-create and table-level RBAC are enforced (the same gates a
        SELECT hits); rows for hidden or denied columns are removed before the
        frame leaves this method. When the table exists but has no stats artifact, an empty
        frame with the stats schema columns is returned (success, not error).
        """
        from supertable.data_classes import TableDefinition
        from supertable.processing import (
            load_bounded_stats_diagnostic,
            STATS_SCHEMA,
        )

        super_name = command.super_name
        simple_name = command.simple_name
        td = TableDefinition(
            super_name=super_name,
            simple_name=simple_name,
            alias=simple_name,
            columns=[],
        )

        # Reads never create catalog entries.
        try:
            self._assert_targets_exist([td])
        except (SuperTableNotFoundError, TableNotFoundError) as e:
            _log_query_phase_failure("show-stats target resolution", e)
            return pd.DataFrame(), Status.ERROR, _QUERY_TARGET_ERROR

        # RBAC raises when the table is denied and returns the effective view
        # definition used below to remove column-level statistics.
        views = restrict_read_access(
            super_name=super_name,
            organization=self.organization,
            role_name=role_name,
            tables=[td],
            physical_tables=[td],
        )

        matching_views = [
            view
            for alias, view in (views.items() if isinstance(views, dict) else ())
            if str(alias).casefold() == simple_name.casefold()
        ]
        if len(matching_views) > 1:
            # Case-colliding policy aliases are ambiguous.  Never guess which
            # effective predicate governs a raw metadata artifact.
            raise PermissionError(
                "SHOW STATS is unavailable under the effective access policy"
            )
        view_def = matching_views[0] if matching_views else None
        if view_def is not None and bool(
            str(getattr(view_def, "where_clause", "") or "").strip()
        ):
            # Persisted row-group statistics describe the complete physical
            # snapshot and cannot be soundly filtered by a row predicate.
            raise PermissionError(
                "SHOW STATS is unavailable under the effective access policy"
            )

        try:
            (
                stats_file,
                expected_stats_rows,
                share_row_filtered,
            ) = self._resolve_latest_stats_context(
                super_name,
                simple_name,
                include_bounds=True,
            )
        except Exception as e:
            # Resolving the linked-share overlay may require the immutable
            # snapshot document.  Backend failures can contain its physical
            # key or a presigned URL. Log only the exception class; even a
            # trusted sink must not become a credential disclosure channel.
            # An unreadable overlay cannot prove that raw full-table
            # statistics are safe for this caller.
            logger.error(self._lp(
                "[show-stats] policy resolution failed; "
                f"error_type={safe_exception_type(e)}"
            ))
            raise PermissionError(
                "SHOW STATS is unavailable under the effective access policy"
            ) from None

        if share_row_filtered:
            # Linked-share predicates live in the atomic leaf/snapshot
            # metadata, not in role RBAC.  Deny before loading the artifact
            # so neither physical paths nor full-snapshot counts can escape.
            raise PermissionError(
                "SHOW STATS is unavailable under the effective access policy"
            )

        try:
            stats_df = (
                load_bounded_stats_diagnostic(
                    stats_file,
                    expected_rows=expected_stats_rows,
                    storage=self.storage,
                )
                if stats_file else None
            )
        except Exception as e:
            logger.error(self._lp(
                "[show-stats] failed to load stats; "
                f"error_type={safe_exception_type(e)}"
            ))
            return (
                pd.DataFrame(),
                Status.ERROR,
                "SHOW STATS artifact is unavailable",
            )

        if stats_df is None:
            return pd.DataFrame(columns=list(STATS_SCHEMA.keys())), Status.OK, None
        allowed = ["*"] if view_def is None else list(view_def.allowed_columns)
        excluded = {
            "__rowid__",
            "__timestamp__",
            "__file__",
            "__supertable_source_file__",
            "__supertable_scan_filename__",
        }
        if view_def is not None:
            excluded.update(
                str(name).casefold() for name in view_def.excluded_columns
            )
        actual_names = [
            str(name) for name in stats_df.get_column("column_name").unique()
            if name is not None
        ]
        allowed_folded = (
            None if allowed == ["*"]
            else {str(name).casefold() for name in allowed}
        )
        visible_names = [
            name for name in actual_names
            if name.casefold() not in excluded
            and (allowed_folded is None or name.casefold() in allowed_folded)
        ]
        stats_df = stats_df.filter(pl.col("column_name").is_in(visible_names))
        return stats_df.to_pandas(), Status.OK, None

    def execute(
        self,
        role_name: str,
        with_scan: bool = False,
        engine: engine = engine.AUTO,
        *,
        _streaming: bool = False,
        _stream_batch_rows: Optional[int] = None,
        _stream_batch_bytes: Optional[int] = None,
        _stream_row_limit: Optional[int] = None,
        _materialized_row_limit: Optional[int] = None,
        _materialized_result_bytes: Optional[int] = None,
        _deadline_monotonic: Optional[float] = None,
        _cancel_event: Optional[threading.Event] = None,
        expected_role_policy_fingerprint: Optional[str] = None,
        expected_effective_policy_fingerprint: Optional[str] = None,
        _policy_fingerprint_only: bool = False,
        _odata_identity: bool = False,
        _odata_continuation_boundary: Optional[
            ODataContinuationBoundary
        ] = None,
        _odata_continuation_capability: object = None,
    ) -> Tuple[Any, Status, Optional[str]]:
        expected_role_policy_fingerprint = validate_policy_fingerprint(
            expected_role_policy_fingerprint,
            label="expected_role_policy_fingerprint",
        )
        expected_effective_policy_fingerprint = validate_policy_fingerprint(
            expected_effective_policy_fingerprint,
            label="expected_effective_policy_fingerprint",
        )
        if not isinstance(_policy_fingerprint_only, bool):
            raise TypeError("_policy_fingerprint_only must be boolean")
        if not isinstance(_odata_identity, bool):
            raise TypeError("_odata_identity must be boolean")
        if _odata_continuation_boundary is not None:
            if (
                not _odata_identity
                or _odata_continuation_capability
                is not _ODATA_CONTINUATION_CAPABILITY
                or not isinstance(
                    _odata_continuation_boundary,
                    ODataContinuationBoundary,
                )
            ):
                raise ValueError(
                    "OData continuation requires the trusted OData stream"
                )
        if _odata_identity:
            from supertable.engine.engine_enum import Engine as _EngineEnum
            if not _streaming or engine is not _EngineEnum.DUCKDB:
                raise ValueError(
                    "OData identity requires explicit DuckDB result streaming"
                )
        if _materialized_row_limit is not None:
            if _streaming:
                raise ValueError(
                    "a materialized row budget cannot be used with streaming"
                )
            if (
                isinstance(_materialized_row_limit, bool)
                or not isinstance(_materialized_row_limit, int)
                or _materialized_row_limit <= 0
            ):
                raise ValueError(
                    "materialized_row_limit must be a positive integer"
                )
            # Interactive reads normally stop at SUPERTABLE_MAX_LIMIT.  One
            # additional row is permitted solely so a bounded service can
            # distinguish an exact-size result from a truncated response.
            configured_limit = _positive_budget(
                "SUPERTABLE_MAX_LIMIT", 10000,
            )
            if _materialized_row_limit > configured_limit + 1:
                raise ValueError(
                    "materialized_row_limit exceeds the interactive detection "
                    "ceiling"
                )
            if getattr(engine, "value", None) != "spark_sql":
                raise ValueError(
                    "materialized result budgets are only available for Spark SQL"
                )
        if _materialized_result_bytes is not None:
            if _materialized_row_limit is None:
                raise ValueError(
                    "materialized_result_bytes requires a materialized row budget"
                )
            if (
                isinstance(_materialized_result_bytes, bool)
                or not isinstance(_materialized_result_bytes, int)
                or _materialized_result_bytes <= 0
            ):
                raise ValueError(
                    "materialized_result_bytes must be a positive integer"
                )
            _materialized_result_bytes = min(
                _materialized_result_bytes,
                _positive_budget(
                    "SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES",
                    16 * 1024 * 1024,
                ),
            )
        self.role_policy_fingerprint = ""
        self.effective_policy_fingerprint = ""
        status = Status.ERROR
        message: Optional[str] = None
        typed_failure: Optional[BaseException] = None
        self.timer = Timer()
        self.plan_stats = PlanStats()
        _ensure_request_active(_deadline_monotonic, _cancel_event)

        # Classify into an allowed read-path command. Ordinary SELECTs fall
        # through unchanged; EXPLAIN/SHOW STATS are the two diagnostic
        # extensions. A recognised-but-malformed command (e.g. SHOW STATS with
        # no table) returns a clean error rather than raising.
        try:
            command = classify_query(self.query, self.super_name)
        except ValueError as e:
            _log_query_phase_failure("query classification", e)
            return pd.DataFrame(), Status.ERROR, _QUERY_VALIDATION_ERROR

        # DuckDB's EXPLAIN ANALYZE result includes physical Filename(s). Those
        # can be local server paths or presigned bearer URLs used by a managed
        # reflection fallback. Plain EXPLAIN remains available and describes
        # the logical managed-view plan without executing/scanning sources.
        if command.explain and command.explain_options.strip().casefold() == "analyze":
            message = "EXPLAIN ANALYZE is not available on the untrusted read path"
            logger.warning(self._lp(f"rejected query: {message}"))
            return pd.DataFrame(), Status.ERROR, message

        if command.kind is not CommandKind.SHOW_STATS:
            try:
                _validate_query_complexity(command.sql)
            except ValueError as e:
                _log_query_phase_failure("query complexity validation", e)
                return pd.DataFrame(), Status.ERROR, _QUERY_VALIDATION_ERROR
        _ensure_request_active(_deadline_monotonic, _cancel_event)

        bounded_sql = command.sql
        if command.kind is CommandKind.SELECT:
            result_limit = _positive_budget("SUPERTABLE_MAX_LIMIT", 10000)
            if _stream_row_limit is not None:
                if not _streaming:
                    raise ValueError(
                        "an export row budget requires result streaming"
                    )
                if (
                    isinstance(_stream_row_limit, bool)
                    or not isinstance(_stream_row_limit, int)
                    or _stream_row_limit <= 0
                ):
                    raise ValueError(
                        "max_total_rows must be a positive integer"
                    )
                result_limit = _stream_row_limit
            elif _materialized_row_limit is not None:
                result_limit = _materialized_row_limit
            # Enforce the server ceiling at the DataReader boundary too.  SDK
            # callers can instantiate DataReader directly and must not bypass
            # query_sql()'s convenience LIMIT injection.  The separate export
            # path supplies its own explicit positive ceiling and remains
            # incrementally streamed rather than materialized.
            bounded_sql = _ensure_sql_limit(
                command.sql,
                result_limit,
                maximum_limit=result_limit,
            )

        # SHOW STATS short-circuits the engine entirely — it returns the raw
        # statistics artifact, no reflection/estimation/execution.
        if command.kind is CommandKind.SHOW_STATS:
            return self._execute_show_stats(command, role_name)

        # Build parser with the correct dialect for the chosen engine. For
        # EXPLAIN, parse only the inner SELECT so estimation/RBAC/reflection
        # behave exactly as for the equivalent plain SELECT.
        try:
            parser = SQLParser(
                super_name=self.super_name,
                query=bounded_sql,
                dialect=engine.dialect,
                allow_bounded_collection_aggregates=(
                    self._allow_bounded_collection_aggregates
                ),
            )
        except ValueError as e:
            _log_query_phase_failure("query parsing", e)
            return pd.DataFrame(), Status.ERROR, _QUERY_VALIDATION_ERROR
        try:
            tables = parser.get_table_tuples()
            physical_tables = parser.get_physical_tables()
            odata_identity_alias = (
                _odata_identity_binding(parser) if _odata_identity else None
            )
            odata_continuation_boundary = bind_odata_continuation_boundary(
                parser._parsed,
                _odata_continuation_boundary,
            ) if _odata_identity else None
        except Exception as exc:
            _log_query_phase_failure("query parse binding", exc)
            return pd.DataFrame(), Status.ERROR, _QUERY_VALIDATION_ERROR

        try:
            aggregate_children = self._resolve_aggregate_children(
                physical_tables
            )
        except Exception as e:
            _log_query_phase_failure("query aggregate expansion", e)
            return pd.DataFrame(), Status.ERROR, _QUERY_AGGREGATE_ERROR

        # Read-path policy: reads never create. Verify every referenced
        # (super, simple) exists in the Redis catalog **before** anything
        # downstream — RBAC, estimator, or the executor — gets a chance
        # to side-effect-bootstrap them.
        #
        # ORDERING MATTERS: ``restrict_read_access`` (called next) builds
        # ``RoleManager(super_name=..., organization=...)`` which boots
        # RBAC role storage in Redis for the supertable if it doesn't
        # exist. Running the RBAC check first against a missing
        # supertable would silently mint the RBAC scaffold before our
        # existence check fires. Pre-flight FIRST.
        #
        # The check runs in its own try block so SuperTable/TableNotFound
        # convert to the standard (empty_df, Status.ERROR, message)
        # return — we don't want to raise these into the caller, but we
        # DO want to keep ``restrict_read_access``'s PermissionError
        # raising naturally (legacy behaviour API layers depend on for
        # 403 translation).
        try:
            self._assert_targets_exist(physical_tables)
        except (SuperTableNotFoundError, TableNotFoundError) as e:
            _log_query_phase_failure("query target resolution", e)
            return pd.DataFrame(), Status.ERROR, _QUERY_TARGET_ERROR

        # RBAC check — also returns per-alias column/row filter definitions.
        # PermissionError propagates to the caller (legacy behaviour).
        role_policy_fingerprints: Dict[str, str] = {}
        rbac_views = restrict_read_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            tables=tables,
            physical_tables=physical_tables,
            aggregate_children=aggregate_children or None,
            expected_role_policy_fingerprint=expected_role_policy_fingerprint,
            policy_fingerprints_out=role_policy_fingerprints,
        )
        self.role_policy_fingerprint = role_policy_fingerprints.get(
            str(self.super_name).casefold(), "",
        )
        validate_rbac_binding_stability(parser, rbac_views)

        try:
            observation_store = None
            history_provider = None
            try:
                from supertable.engine.query_observations import (
                    QueryObservationStore,
                )
                observation_store = QueryObservationStore(self.organization)
                if observation_store.enabled:
                    history_provider = observation_store.history_provider
            except Exception as observation_error:
                logger.debug(
                    self._lp(
                        "[engine.auto] observation history unavailable: "
                        f"error_type={safe_exception_type(observation_error)}"
                    )
                )
            # Initialize plan manager and query id/hash (same as before)
            self.query_plan_manager = QueryPlanManager(
                super_name=self.super_name,
                organization=self.organization,
                current_meta_path="redis://meta/root",
                query=parser.original_query,
            )
            self.query_plan_manager.requested_engine = getattr(
                engine, "value", str(engine),
            )
            self.query_plan_manager.engine_forced = (
                self.query_plan_manager.requested_engine != "auto"
            )
            # Reuse the exact bounded store used by AUTO's history provider;
            # plan extension records the successful observation after execution.
            self.query_plan_manager.query_observation_store = observation_store
            # Stamp the call origin so plan_extender records it on the read
            # monitoring entry (defaults to "api" downstream if unset).
            self.query_plan_manager.source_type = self.source
            self._log_ctx = f"[qid={self.query_plan_manager.query_id} qh={self.query_plan_manager.query_hash}] "
            self.query_plan_manager.original_table = ", ".join(t.simple_name for t in physical_tables) if physical_tables else ""

            predicate_constraints = {}
            join_edges = []
            if settings.SUPERTABLE_READ_PRUNING_ENABLED:
                # Derive per-table WHERE constraints and equi-join links only
                # when the estimator can consume them.  Both analyses walk the
                # sqlglot scope tree, so skipping them under the master switch
                # avoids pure overhead on every pruning-disabled read.
                try:
                    predicate_constraints = parser.get_predicate_constraints()
                except Exception as pc_err:
                    logger.debug(self._lp(
                        "[prune] predicate extraction failed; "
                        f"error_type={safe_exception_type(pc_err)}"))
                predicate_constraints = _engine_safe_predicate_constraints(
                    predicate_constraints, engine,
                )
                try:
                    join_edges = parser.get_join_edges()
                except Exception as je_err:
                    logger.debug(self._lp(
                        "[prune] join-edge extraction failed; "
                        f"error_type={safe_exception_type(je_err)}"))

            # 1) ESTIMATE — use physical_tables so CTE aliases are excluded
            estimator_kwargs = dict(
                organization=self.organization,
                storage=self.storage,
                tables=physical_tables,
                predicate_constraints=predicate_constraints,
                join_edges=join_edges,
                join_pruning_lanes=_engine_safe_join_pruning_lanes(engine),
                plan_stats=self.plan_stats,
                require_odata_identity=(_odata_identity is True),
                require_bounded_resource_estimates=(
                    getattr(engine, "value", None) in {"auto", "islanddb"}
                ),
            )
            if aggregate_children:
                estimator_kwargs["aggregate_children"] = aggregate_children
            estimator = DataEstimator(**estimator_kwargs)
            reflection = estimator.estimate()
            # Retain the data-free, snapshot-pinned estimate for public
            # preflight callers. It contains executor paths internally, so the
            # public helper below allowlists aggregate fields rather than
            # returning this object itself.
            self.last_reflection = reflection
            _ensure_request_active(_deadline_monotonic, _cancel_event)

            logger.info(self._lp(f"[estimate] storage={reflection.storage_type} | files={reflection.total_reflections} | bytes={reflection.reflection_bytes}"))

            # Wire RBAC column/row filter definitions onto the reflection so
            # executors create filtered views for restricted roles.
            reflection.rbac_views = rbac_views

            # --- Tombstone/share policy from the estimator-pinned snapshot ---
            # Never re-read the Redis leaf here.  A writer can commit between
            # estimation and execution; combining old files with a new DV would
            # hide the old row while omitting its replacement file.
            snapshots_by_key = {
                (sup.super_name.lower(), sup.simple_name.lower()): sup
                for sup in reflection.supers
            }
            resolved_tombstones: Dict[
                Tuple[str, str],
                Tuple[str, Tuple[TombstoneSegmentDef, ...]],
            ] = {}

            def _resolve_required_tombstone_artifact(
                    raw_key: str, *, super_name: str, simple_name: str,
            ) -> str:
                try:
                    resolved = estimator._to_duckdb_path(raw_key)
                except Exception:
                    raise RuntimeError(
                        f"Unable to resolve required deletion-vector for "
                        f"{super_name}.{simple_name}"
                    ) from None
                if not isinstance(resolved, str) or not resolved:
                    raise RuntimeError(
                        f"Unable to resolve required deletion-vector for "
                        f"{super_name}.{simple_name}"
                    )
                # A bare relative key is a valid LOCAL path, but for an
                # object-store reflection it means every URL/presign resolver
                # failed. Do not let an executor read a same-named local file.
                storage_type = (reflection.storage_type or "").lower()
                if (
                    "://" not in resolved
                    and not resolved.startswith("/")
                    and "local" not in storage_type
                ):
                    raise RuntimeError(
                        f"Unable to resolve required deletion-vector for "
                        f"{super_name}.{simple_name}"
                    )
                return resolved

            for td in tables:
                table_key = (td.super_name.lower(), td.simple_name.lower())
                sup = snapshots_by_key.get(table_key)
                if sup is None:
                    # CTE aliases have no physical snapshot of their own.
                    continue

                tombstone_key = getattr(sup, "tombstone_key", None)
                tombstone_rows = getattr(sup, "tombstone_rows", None)
                tombstone_digest = getattr(sup, "tombstone_digest", None)
                raw_tombstone_format = getattr(
                    sup, "tombstone_format", None,
                )
                if tombstone_key or raw_tombstone_format is not None:
                    try:
                        tombstone_format = validate_snapshot_tombstone_state(
                            tombstone_key,
                            tombstone_rows,
                            tombstone_digest,
                            format_present=(raw_tombstone_format is not None),
                            tombstone_format=raw_tombstone_format,
                        )
                    except (TypeError, ValueError):
                        if tombstone_key and not (
                            isinstance(tombstone_rows, int)
                            and not isinstance(tombstone_rows, bool)
                            and tombstone_rows > 0
                        ):
                            raise RuntimeError(
                                f"Snapshot for {td.super_name}."
                                f"{td.simple_name} references a deletion "
                                "vector without a positive row count"
                            ) from None
                        raise RuntimeError(
                            f"Invalid deletion-vector state for "
                            f"{td.super_name}.{td.simple_name}"
                        ) from None
                else:
                    # Legacy/direct Reflection callers may omit the canonical
                    # empty-state count. Production estimator snapshots always
                    # carry the fully validated normalized format.
                    tombstone_format = TOMBSTONE_FORMAT_V1
                if tombstone_key and not (
                    isinstance(tombstone_rows, int)
                    and not isinstance(tombstone_rows, bool)
                    and tombstone_rows > 0
                ):
                    raise RuntimeError(
                        f"Snapshot for {td.super_name}.{td.simple_name} references "
                        "a deletion vector without an exact positive row count"
                    )
                if tombstone_key and not (
                    isinstance(tombstone_digest, str)
                    and re.fullmatch(r"[0-9a-f]{64}", tombstone_digest)
                ):
                    raise RuntimeError(
                        f"Snapshot for {td.super_name}.{td.simple_name} references "
                        "a deletion vector without a valid SHA-256 digest"
                    )
                if not tombstone_key and tombstone_rows and tombstone_rows > 0:
                    raise RuntimeError(
                        f"Snapshot for {td.super_name}.{td.simple_name} records "
                        f"tombstoned rows but has no deletion-vector pointer"
                    )

                if tombstone_key:
                    if tombstone_format in (
                        TOMBSTONE_FORMAT_V2,
                        TOMBSTONE_FORMAT_V3,
                    ):
                        tombstone_prefix = (
                            f"{self.organization}/{sup.super_name}/tables/"
                            f"{sup.simple_name}/tombstone/"
                        )
                        if not tombstone_key.startswith(tombstone_prefix):
                            pointer_kind = (
                                "manifest pointer"
                                if tombstone_format == TOMBSTONE_FORMAT_V2
                                else "pointer"
                            )
                            raise RuntimeError(
                                f"Deletion-vector {pointer_kind} escapes the "
                                f"pinned table {sup.super_name}."
                                f"{sup.simple_name}"
                            )
                    resolved_entry = resolved_tombstones.get(table_key)
                    if resolved_entry is None:
                        resolved_tombstone = (
                            _resolve_required_tombstone_artifact(
                                tombstone_key,
                                super_name=td.super_name,
                                simple_name=td.simple_name,
                            )
                        )
                        resolved_segments: Tuple[
                            TombstoneSegmentDef, ...
                        ] = ()
                        if tombstone_format == TOMBSTONE_FORMAT_V2:
                            manifest = load_tombstone_manifest_from_storage(
                                self.storage,
                                tombstone_key,
                                expected_organization=self.organization,
                                expected_super_name=sup.super_name,
                                expected_simple_name=sup.simple_name,
                                pinned_snapshot_version=sup.simple_version,
                                expected_total_rows=tombstone_rows,
                                expected_digest=tombstone_digest,
                                expected_segment_prefix=(
                                    f"{self.organization}/{sup.super_name}/"
                                    f"tables/{sup.simple_name}/tombstone"
                                ),
                            )
                            segment_defs = []
                            for segment in manifest.segments:
                                try:
                                    metadata = self.storage.stat_object(
                                        segment.file
                                    )
                                    observed_size = getattr(
                                        metadata, "size", None,
                                    )
                                    identity_fn = getattr(
                                        metadata, "identity_token", None,
                                    )
                                    provider_identity = (
                                        identity_fn()
                                        if callable(identity_fn) else None
                                    )
                                except Exception:
                                    raise RuntimeError(
                                        "Unable to observe required "
                                        "deletion-vector segment"
                                    ) from None
                                if (
                                    not isinstance(observed_size, int)
                                    or isinstance(observed_size, bool)
                                    or observed_size != segment.file_size
                                ):
                                    raise RuntimeError(
                                        "Deletion-vector segment size does not "
                                        "match the manifest"
                                    )
                                if (
                                    not isinstance(provider_identity, str)
                                    or not provider_identity
                                    or "\x00" in provider_identity
                                    or len(provider_identity.encode("utf-8"))
                                    > MAX_TOMBSTONE_PROVIDER_IDENTITY_BYTES
                                ):
                                    raise RuntimeError(
                                        "Deletion-vector segment has no stable "
                                        "provider identity"
                                    )
                                segment_defs.append(TombstoneSegmentDef(
                                    cache_key=segment.file,
                                    tombstone_path=(
                                        _resolve_required_tombstone_artifact(
                                            segment.file,
                                            super_name=td.super_name,
                                            simple_name=td.simple_name,
                                        )
                                    ),
                                    expected_rows=segment.rows,
                                    file_size=segment.file_size,
                                    tombstone_digest=segment.digest,
                                    provider_identity=provider_identity,
                                ))
                            resolved_segments = tuple(segment_defs)
                        resolved_entry = (
                            resolved_tombstone,
                            resolved_segments,
                        )
                        resolved_tombstones[table_key] = resolved_entry
                    resolved_tombstone, resolved_segments = resolved_entry
                    reflection.tombstone_views[td.alias] = TombstoneDef(
                        tombstone_path=resolved_tombstone,
                        cache_key=tombstone_key,
                        expected_rows=tombstone_rows,
                        tombstone_digest=tombstone_digest,
                        resource_keys=tuple(getattr(sup, "resource_keys", ()) or ()),
                        snapshot_resource_keys=(
                            None
                            if getattr(sup, "snapshot_resource_keys", None) is None
                            else tuple(getattr(sup, "snapshot_resource_keys"))
                        ),
                        tombstone_format=(
                            raw_tombstone_format
                            if raw_tombstone_format is not None
                            else None
                        ),
                        segments=resolved_segments,
                    )

                # Linked-share policy is pinned alongside the same resources.
                # Its provider/schema column projection is an authorization
                # boundary of its own and must be intersected with local RBAC.
                share_policy_fingerprint = getattr(
                    sup, "share_policy_fingerprint", None,
                )
                share_allowed_columns = getattr(
                    sup, "share_allowed_columns", None,
                )
                if share_policy_fingerprint is None:
                    if share_allowed_columns is not None:
                        raise RuntimeError(
                            "Linked-share column policy has no authoritative seal"
                        )
                else:
                    validate_policy_fingerprint(
                        share_policy_fingerprint,
                        label="linked-share policy fingerprint",
                    )
                    if not isinstance(share_allowed_columns, list) or not (
                        share_allowed_columns
                    ):
                        raise RuntimeError(
                            "Linked-share column policy is unavailable"
                        )
                    share_columns_by_folded: Dict[str, str] = {}
                    for raw_column in share_allowed_columns:
                        if not isinstance(raw_column, str) or not raw_column:
                            raise RuntimeError(
                                "Linked-share column policy is invalid"
                            )
                        folded = raw_column.casefold()
                        if folded in share_columns_by_folded:
                            raise RuntimeError(
                                "Linked-share column policy is ambiguous"
                            )
                        share_columns_by_folded[folded] = raw_column

                    existing_rbac = reflection.rbac_views.get(td.alias)
                    if existing_rbac is None:
                        existing_rbac = RbacViewDef(
                            allowed_columns=sorted(
                                share_columns_by_folded.values(),
                                key=str.casefold,
                            ),
                        )
                        reflection.rbac_views[td.alias] = existing_rbac
                    else:
                        role_allowed = list(
                            getattr(existing_rbac, "allowed_columns", None) or []
                        )
                        if role_allowed == ["*"]:
                            effective_columns = dict(share_columns_by_folded)
                        else:
                            role_allowed_folded = {
                                str(column).casefold() for column in role_allowed
                            }
                            effective_columns = {
                                folded: name
                                for folded, name in share_columns_by_folded.items()
                                if folded in role_allowed_folded
                            }
                        excluded = {
                            str(column).casefold()
                            for column in (
                                getattr(existing_rbac, "excluded_columns", None)
                                or []
                            )
                        }
                        if not set(effective_columns).difference(excluded):
                            raise PermissionError(
                                f"You don't have permission to read any columns in "
                                f"'{td.simple_name}'."
                            )
                        existing_rbac.allowed_columns = sorted(
                            effective_columns.values(), key=str.casefold,
                        )

                share_row_filter = getattr(sup, "share_row_filter", None)
                if share_row_filter:
                    share_row_filter = _validated_share_row_filter(
                        share_row_filter
                    )
                    sup.share_row_filter = share_row_filter
                    existing_rbac = reflection.rbac_views.get(td.alias)
                    if existing_rbac:
                        if existing_rbac.where_clause:
                            existing_rbac.where_clause = (
                                f"({existing_rbac.where_clause}) AND "
                                f"({share_row_filter})"
                            )
                        else:
                            existing_rbac.where_clause = share_row_filter
                    else:
                        reflection.rbac_views[td.alias] = RbacViewDef(
                            allowed_columns=["*"],
                            where_clause=share_row_filter,
                        )

            if _odata_identity:
                if (
                    odata_identity_alias is None
                    or len(reflection.supers) != 1
                ):
                    raise RuntimeError(
                        "OData stable identity is unavailable for this relation"
                    )
                identity_snapshot = reflection.supers[0]
                if (
                    getattr(identity_snapshot, "stable_rowid_contract", False)
                    is not True
                    or getattr(
                        identity_snapshot, "share_policy_fingerprint", None,
                    ) is not None
                ):
                    raise RuntimeError(
                        "OData stable identity is unavailable for this snapshot"
                    )
                matching_aliases = [
                    str(td.alias)
                    for td in tables
                    if str(td.alias).casefold()
                    == str(odata_identity_alias).casefold()
                ]
                if len(matching_aliases) != 1:
                    raise RuntimeError(
                        "OData stable identity binding is ambiguous"
                    )
                reflection.odata_identity_aliases = {
                    matching_aliases[0]: ODATA_INTERNAL_ROWID_COLUMN,
                }
                reflection.odata_continuation_boundary = (
                    odata_continuation_boundary
                )

            if not reflection.supers:
                message = "No parquet files found"
                return pd.DataFrame(), status, message

            if not role_policy_fingerprints:
                if (
                    expected_role_policy_fingerprint is not None
                    or expected_effective_policy_fingerprint is not None
                    or _policy_fingerprint_only
                ):
                    raise PermissionError(
                        "Authoritative role policy fingerprint is unavailable"
                    )
            else:
                self.effective_policy_fingerprint = (
                    _effective_read_policy_fingerprint(
                        role_policy_fingerprints, reflection,
                    )
                )

            if expected_effective_policy_fingerprint is not None and not (
                self.effective_policy_fingerprint
                and hmac.compare_digest(
                    self.effective_policy_fingerprint,
                    expected_effective_policy_fingerprint,
                )
            ):
                raise PermissionError(
                    "Effective read policy changed before query execution"
                )

            if _policy_fingerprint_only:
                return pd.DataFrame(), Status.OK, None

            # Construct the executor only after every submitting-policy pin has
            # matched the exact protected reflection it will receive.
            executor = Executor(
                storage=self.storage,
                organization=self.organization,
                auto_history_provider=history_provider,
            )

            # 2) EXECUTE.  EXPLAIN is pinned to DuckDB so the plan is
            # produced cheaply and uniformly (no Pro materialisation / Spark
            # round trip) and prefixed onto the final rewritten query.
            exec_engine = engine
            if command.explain:
                from supertable.engine.engine_enum import Engine as _EngineEnum
                exec_engine = _EngineEnum.DUCKDB
            if _streaming:
                if command.explain:
                    raise ValueError("EXPLAIN does not support result streaming")
                _ensure_request_active(_deadline_monotonic, _cancel_event)
                result_value, _engine_used = executor.execute_stream(
                    engine=exec_engine,
                    reflection=reflection,
                    parser=parser,
                    query_manager=self.query_plan_manager,
                    timer=self.timer,
                    plan_stats=self.plan_stats,
                    log_prefix=self._lp(""),
                    max_batch_rows=_stream_batch_rows,
                    max_batch_bytes=_stream_batch_bytes,
                    deadline_monotonic=_deadline_monotonic,
                    cancel_event=_cancel_event,
                )
                if _stream_row_limit is not None:
                    result_value = _RowBudgetResultStream(
                        result_value, _stream_row_limit,
                    )
                result_shape = (0, len(result_value.schema))
                self.plan_stats.add_stat({
                    "RESULT_ROWS": None,
                    "RESULT_COLUMNS": result_shape[1],
                    "RESULT_MODE": "arrow_stream",
                })
            else:
                result_value, _engine_used = executor.execute(
                    engine=exec_engine,
                    reflection=reflection,
                    parser=parser,
                    query_manager=self.query_plan_manager,
                    timer=self.timer,
                    plan_stats=self.plan_stats,
                    log_prefix=self._lp(""),
                    explain=command.explain,
                    explain_options=command.explain_options,
                    deadline_monotonic=_deadline_monotonic,
                    cancel_event=_cancel_event,
                    materialized_row_limit=_materialized_row_limit,
                    materialized_result_bytes=_materialized_result_bytes,
                )
                _ensure_request_active(_deadline_monotonic, _cancel_event)
                result_shape = result_value.shape
                try:
                    result_bytes = int(
                        result_value.memory_usage(index=False, deep=True).sum()
                    )
                except Exception:
                    result_bytes = 0
                self.plan_stats.add_stat({
                    "RESULT_BYTES": max(0, result_bytes),
                    "RESULT_ROWS": max(0, int(result_shape[0])),
                    "RESULT_COLUMNS": max(0, int(result_shape[1])),
                })
            status = Status.OK
        except PermissionError:
            # Authorization pins and linked-share policy validation are security
            # decisions, not ordinary backend failures. Preserve their type for
            # service layers and never turn them into an empty successful result.
            raise
        except (ResourceReservationCancelled, TimeoutError) as e:
            message = (
                "Query was cancelled"
                if isinstance(e, ResourceReservationCancelled)
                else "Query timed out"
            )
            typed_failure = type(e)(message)
            _log_query_phase_failure("query execution", e)
            result_value = pd.DataFrame()
            result_shape = result_value.shape
        except Exception as e:
            message = _QUERY_EXECUTION_ERROR
            logger.error(
                self._lp(
                    "query execution failed; "
                    f"error_type={safe_exception_type(e)}"
                )
            )
            result_value = pd.DataFrame()
            result_shape = result_value.shape

        # A created Arrow stream is not yet a successful read. Defer the plan
        # metric until exhaustion/cancel/error so status, rows and bytes describe
        # what the consumer actually observed and the engine profile is final.
        if _streaming and status is Status.OK:
            stream_timer = self.timer
            stream_plan_stats = self.plan_stats
            stream_qpm = self.query_plan_manager
            if (
                stream_timer is None
                or stream_plan_stats is None
                or stream_qpm is None
            ):
                _cancel_and_close_stream(result_value)
                raise RuntimeError(
                    "stream execution completed without monitoring context"
                )
            result_columns = max(0, int(result_shape[1]))

            def _finalize_stream_monitoring(
                final_status: str,
                final_message: Optional[str],
                result_rows: int,
                result_bytes: int,
            ) -> None:
                stream_plan_stats.add_stat({
                    "RESULT_BYTES": max(0, int(result_bytes)),
                    "RESULT_ROWS": max(0, int(result_rows)),
                    "RESULT_COLUMNS": result_columns,
                    "RESULT_MODE": "arrow_stream_final",
                })
                stream_timer.capture_and_reset_timing(event="EXECUTING_QUERY")
                stream_timer.capture_duration(event="TOTAL_EXECUTE")
                monitoring_error: Optional[MonitoringPostExecutionError] = None
                try:
                    extend_execution_plan(
                        query_plan_manager=stream_qpm,
                        role_name=role_name,
                        timing=stream_timer.timings,
                        plan_stats=stream_plan_stats,
                        status=final_status,
                        message=(
                            _redact_storage_credentials(final_message)
                            if final_message else None
                        ),
                        result_shape=(result_rows, result_columns),
                    )
                except MonitoringDurabilityError as exc:
                    safe_cause = MonitoringDurabilityError(
                        "monitoring durability failure"
                    )
                    monitoring_error = MonitoringPostExecutionError(
                        organization=stream_qpm.organization,
                        super_name=stream_qpm.super_name,
                        query_id=str(getattr(stream_qpm, "query_id", "")),
                        status=final_status,
                        cause=safe_cause,
                    )
                finally:
                    stream_timer.capture_and_reset_timing(event="EXTENDING_PLAN")
                audit_engine = _actual_engine_from_plan_stats(
                    stream_plan_stats,
                    fallback=_engine_used,
                )
                try:
                    if final_status == Status.OK.value:
                        self._emit_successful_query_audit(
                            role_name=role_name,
                            engine_used=audit_engine,
                            result_rows=result_rows,
                            result_columns=result_columns,
                        )
                    else:
                        self._emit_successful_query_audit(
                            role_name=role_name,
                            engine_used=audit_engine,
                            result_rows=result_rows,
                            result_columns=result_columns,
                            outcome="failure",
                        )
                except Exception:
                    if final_status == Status.OK.value and monitoring_error is None:
                        raise
                    # Preserve the producer/cancellation or monitoring error
                    # just as the materialized failure path does.
                    logger.debug(
                        self._lp("failed to emit query failure audit"),
                        exc_info=True,
                    )
                if monitoring_error is not None:
                    raise monitoring_error

            return (
                _MonitoredResultStream(result_value, _finalize_stream_monitoring),
                status,
                message,
            )

        # Materialized/error outcomes are final here. Capture end-to-end query
        # latency before monitoring itself so Redis/WAL latency does not distort
        # engine feedback.
        self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")
        self.timer.capture_duration(event="TOTAL_EXECUTE")
        try:
            extend_execution_plan(
                query_plan_manager=self.query_plan_manager,
                role_name=role_name,
                timing=self.timer.timings,
                plan_stats=self.plan_stats,
                status=str(status.value),
                message=message,
                result_shape=result_shape,
            )
        except MonitoringDurabilityError:
            if _streaming:
                _cancel_and_close_stream(result_value)
            qpm = self.query_plan_manager
            safe_cause = MonitoringDurabilityError(
                "monitoring durability failure"
            )
            raise MonitoringPostExecutionError(
                organization=str(getattr(qpm, "organization", self.organization)),
                super_name=str(getattr(qpm, "super_name", self.super_name)),
                query_id=str(getattr(qpm, "query_id", "")),
                status=str(status.value),
                cause=safe_cause,
            ) from None
        except Exception as e:
            logger.error(self._lp(
                "extend_execution_plan failed; "
                f"error_type={safe_exception_type(e)}"
            ))

        self.timer.capture_and_reset_timing(event="EXTENDING_PLAN")
        if typed_failure is not None:
            try:
                self._emit_successful_query_audit(
                    role_name=role_name, engine_used=_engine_used,
                    result_rows=0, result_columns=0, outcome="failure",
                )
            except Exception:
                logger.debug(self._lp("failed to emit query failure audit"), exc_info=True)
            raise typed_failure
        if status is Status.OK:
            self._emit_successful_query_audit(
                role_name=role_name,
                engine_used=_engine_used,
                result_rows=int(result_shape[0]),
                result_columns=int(result_shape[1]),
            )
        else:
            try:
                self._emit_successful_query_audit(
                    role_name=role_name, engine_used=_engine_used,
                    result_rows=0, result_columns=0, outcome="failure",
                )
            except Exception:
                logger.debug(self._lp("failed to emit query failure audit"), exc_info=True)
        return result_value, status, message

    def execute_stream(
        self,
        role_name: str,
        engine: engine = engine.ISLANDDB,
        *,
        max_batch_rows: Optional[int] = None,
        max_batch_bytes: Optional[int] = None,
        timeout_sec: Optional[float] = None,
        cancel_event: Optional[threading.Event] = None,
        _deadline_monotonic: Optional[float] = None,
        expected_role_policy_fingerprint: Optional[str] = None,
        expected_effective_policy_fingerprint: Optional[str] = None,
    ) -> Tuple[Any, Status, Optional[str]]:
        """Execute through the normal preflight/RBAC path as an Arrow stream.

        Streaming is intentionally explicit and never disguises a materialized
        fallback. DuckDB and IslandDB return cancellable Arrow batches (AUTO
        may safely select either); unsupported Spark requests return the
        ordinary ``Status.ERROR`` result without running user SQL.
        """
        deadline = (
            _deadline_monotonic
            if _deadline_monotonic is not None
            else _caller_deadline(timeout_sec)
        )
        _ensure_request_active(deadline, cancel_event)
        return self.execute(
            role_name=role_name,
            engine=engine,
            with_scan=False,
            _streaming=True,
            _stream_batch_rows=max_batch_rows,
            _stream_batch_bytes=max_batch_bytes,
            _deadline_monotonic=deadline,
            _cancel_event=cancel_event,
            expected_role_policy_fingerprint=(
                expected_role_policy_fingerprint
            ),
            expected_effective_policy_fingerprint=(
                expected_effective_policy_fingerprint
            ),
        )

    def execute_export_stream(
        self,
        role_name: str,
        engine: engine = engine.AUTO,
        *,
        max_total_rows: int,
        timeout_sec: float,
        max_batch_rows: Optional[int] = None,
        max_batch_bytes: Optional[int] = None,
        cancel_event: Optional[threading.Event] = None,
        _deadline_monotonic: Optional[float] = None,
        expected_role_policy_fingerprint: Optional[str] = None,
        expected_effective_policy_fingerprint: Optional[str] = None,
        _odata_identity: bool = False,
        _odata_continuation_boundary: Optional[
            ODataContinuationBoundary
        ] = None,
        _odata_continuation_capability: object = None,
    ) -> Tuple[Any, Status, Optional[str]]:
        """Execute a large RBAC-filtered export as a bounded Arrow stream.

        This is deliberately separate from :meth:`execute_stream`, whose
        interactive contract remains capped by ``SUPERTABLE_MAX_LIMIT``.  An
        export cannot run without both an explicit positive total-row budget
        and an explicit finite positive caller timeout.
        """
        if (
            isinstance(max_total_rows, bool)
            or not isinstance(max_total_rows, int)
            or max_total_rows <= 0
        ):
            raise ValueError("max_total_rows must be a positive integer")
        deadline = (
            _deadline_monotonic
            if _deadline_monotonic is not None
            else _caller_deadline(timeout_sec)
        )
        _ensure_request_active(deadline, cancel_event)
        assert deadline is not None
        return self.execute(
            role_name=role_name,
            engine=engine,
            with_scan=False,
            _streaming=True,
            _stream_batch_rows=max_batch_rows,
            _stream_batch_bytes=max_batch_bytes,
            _stream_row_limit=max_total_rows,
            _deadline_monotonic=deadline,
            _cancel_event=cancel_event,
            expected_role_policy_fingerprint=(
                expected_role_policy_fingerprint
            ),
            expected_effective_policy_fingerprint=(
                expected_effective_policy_fingerprint
            ),
            _odata_identity=_odata_identity,
            _odata_continuation_boundary=_odata_continuation_boundary,
            _odata_continuation_capability=_odata_continuation_capability,
        )


def _constant_limit_value(expression: exp.Expression) -> int:
    """Evaluate a deliberately small, exact integer LIMIT grammar.

    DuckDB evaluates constant LIMIT expressions before scanning.  Replacing an
    expression we cannot prove would risk widening a valid small bound or
    hiding an overflow/binder error.  This evaluator therefore accepts only
    integer literals, parentheses, signed integer casts, unary negation, and
    overflow-checked ``+``, ``-`` and ``*``.
    """
    signed_cast_bits = {
        exp.DataType.Type.TINYINT: 8,
        exp.DataType.Type.SMALLINT: 16,
        exp.DataType.Type.MEDIUMINT: 32,
        exp.DataType.Type.INT: 32,
        exp.DataType.Type.BIGINT: 64,
        exp.DataType.Type.INT128: 128,
    }

    def checked(value: int, bits: int) -> Tuple[int, int]:
        lower = -(1 << (bits - 1))
        upper = (1 << (bits - 1)) - 1
        if value < lower or value > upper:
            raise ValueError("LIMIT constant expression overflows its integer type")
        return value, bits

    def evaluate(node: exp.Expression) -> Tuple[int, int]:
        while isinstance(node, exp.Paren):
            node = node.this

        if isinstance(node, exp.Literal) and not node.is_string:
            raw = str(node.this)
            if not raw.isdigit():
                raise ValueError(
                    "LIMIT row count must be a supported integer constant"
                )
            value = int(raw)
            if value <= (1 << 31) - 1:
                return value, 32
            if value <= (1 << 63) - 1:
                return value, 64
            if value <= (1 << 127) - 1:
                return value, 128
            raise ValueError("LIMIT integer literal is outside DuckDB's exact range")

        if isinstance(node, exp.Cast):
            target = node.args.get("to")
            target_type = target.this if isinstance(target, exp.DataType) else None
            bits = signed_cast_bits.get(target_type)
            if bits is None:
                raise ValueError(
                    "LIMIT casts must target a supported signed integer type"
                )
            value, _ = evaluate(node.this)
            return checked(value, bits)

        if isinstance(node, exp.Neg):
            value, bits = evaluate(node.this)
            return checked(-value, bits)

        if isinstance(node, (exp.Add, exp.Sub, exp.Mul)):
            left, left_bits = evaluate(node.this)
            right, right_bits = evaluate(node.expression)
            bits = max(left_bits, right_bits)
            if isinstance(node, exp.Add):
                value = left + right
            elif isinstance(node, exp.Sub):
                value = left - right
            else:
                value = left * right
            return checked(value, bits)

        raise ValueError("LIMIT row count must be a supported integer constant")

    value, _ = evaluate(expression)
    if value < 0:
        raise ValueError("LIMIT row count must be non-negative")
    if value > (1 << 63) - 1:
        # DuckDB's LIMIT binder ultimately converts the constant to signed
        # INT64 even when its expression is represented as INT128. Clamping a
        # larger value would hide that conversion error.
        raise ValueError("LIMIT row count exceeds DuckDB's signed integer range")
    return value


def _is_unbounded_limit(expression: exp.Expression) -> bool:
    original = expression
    while isinstance(expression, exp.Paren):
        expression = expression.this
    if isinstance(expression, exp.Null):
        return True
    # DuckDB accepts parentheses around NULL but not around the LIMIT ALL
    # keyword. Do not normalize invalid ``LIMIT (ALL)`` into a valid query.
    if expression is not original:
        return False
    if not isinstance(expression, exp.Column) or expression.table:
        return False
    identifier = expression.this
    return bool(
        isinstance(identifier, exp.Identifier)
        and not identifier.args.get("quoted")
        and identifier.name.casefold() == "all"
    )


def _ensure_sql_limit(
    sql: str,
    default_limit: int,
    *,
    maximum_limit: Optional[int] = None,
) -> str:
    """
    If the outermost query has no LIMIT clause, append one.

    Only appends when the SQL does not already end with a LIMIT (ignoring
    trailing whitespace/semicolons).  This avoids breaking queries that
    already specify their own LIMIT, subqueries that contain LIMIT internally,
    or CTEs.
    """
    try:
        requested = int(default_limit)
    except (TypeError, ValueError, OverflowError):
        raise ValueError("Query limit must be an integer") from None
    requested = max(0, requested)
    if maximum_limit is None:
        maximum = _positive_budget("SUPERTABLE_MAX_LIMIT", 10000)
    else:
        if (
            isinstance(maximum_limit, bool)
            or not isinstance(maximum_limit, int)
            or maximum_limit <= 0
        ):
            raise ValueError("maximum query limit must be a positive integer")
        maximum = maximum_limit
    enforced = min(requested, maximum)

    # Inspect the actual outer query node. Regex-only detection mistakes nested
    # LIMITs for response bounds and cannot clamp a client-supplied huge limit.
    stripped_for_parse = sql.rstrip()
    while stripped_for_parse.endswith(";"):
        stripped_for_parse = stripped_for_parse[:-1].rstrip()
    try:
        statements = sqlglot.parse(stripped_for_parse, read="duckdb")
        root = statements[0] if len(statements) == 1 else None
    except Exception:
        root = None
    if root is not None:
        limit_node = root.args.get("limit")
        if limit_node is not None:
            if isinstance(limit_node, exp.Fetch):
                count = limit_node.args.get("count")
                # SQL permits ``FETCH FIRST ROW ONLY`` with an omitted count;
                # it means exactly one row.
                current = 1 if count is None else None
                unbounded = (
                    isinstance(count, exp.Expression)
                    and _is_unbounded_limit(count)
                )
                if isinstance(count, exp.Expression):
                    if not unbounded:
                        current = _constant_limit_value(count)

                options = limit_node.args.get("limit_options")
                percent = bool(
                    isinstance(options, exp.Expression)
                    and options.args.get("percent")
                )
                with_ties = bool(
                    isinstance(options, exp.Expression)
                    and options.args.get("with_ties")
                )

                if percent or with_ties:
                    # Neither modifier is a hard row bound: WITH TIES can emit
                    # every input row and PERCENT scales with relation size.
                    # An outer SELECT/LIMIT would rename duplicate output
                    # columns and truncate tie groups, while sqlglot's DuckDB
                    # generator silently drops the modifiers.  Reject rather
                    # than execute different SQL or bypass the response ceiling.
                    raise ValueError(
                        "FETCH PERCENT and WITH TIES are unavailable on the "
                        "bounded read path"
                    )

                if unbounded:
                    bounded = root.copy()
                    bounded_fetch = bounded.args.get("limit")
                    bounded_fetch.set("count", exp.Literal.number(enforced))
                    return bounded.sql(dialect="duckdb")

                if current is not None and current >= 0:
                    if current <= enforced:
                        # In particular, FETCH 0 must remain zero; widening it
                        # to the response ceiling changes an empty result into a
                        # data-bearing one.
                        return sql
                    bounded = root.copy()
                    bounded_fetch = bounded.args.get("limit")
                    bounded_fetch.set("count", exp.Literal.number(enforced))
                    # Emit the same dialect that was parsed.  The generic
                    # generator can rewrite DuckDB-specific table functions
                    # (for example RANGE -> GENERATE_SERIES) while retaining a
                    # column reference that no longer binds.  The modifiers
                    # DuckDB's generator cannot preserve were rejected above,
                    # so its FETCH-to-LIMIT normalization is semantics-safe.
                    return bounded.sql(dialect="duckdb")

                raise ValueError("FETCH row count must be a supported integer constant")

            expression = getattr(limit_node, "expression", None)
            if not isinstance(expression, exp.Expression):
                raise ValueError("LIMIT row count is missing")
            if _is_unbounded_limit(expression):
                return root.limit(enforced, copy=True).sql(dialect="duckdb")
            current = _constant_limit_value(expression)
            if current is not None and 0 <= current <= enforced:
                return sql
            # A proven integer bound over the server ceiling can be clamped at
            # the outer AST node without widening or masking an evaluation
            # error. Unsupported and invalid expressions failed above.
            return root.limit(enforced, copy=True).sql(dialect="duckdb")

    # Preserve familiar formatting for the no-LIMIT path while removing
    # trailing semicolons so the injected clause remains part of the statement.
    stripped = sql.rstrip()
    while stripped.endswith(";"):
        stripped = stripped[:-1].rstrip()
    return f"{stripped}\nLIMIT {enforced}"


def _json_safe_result_value(
    value: Any, *, assume_naive_datetime_utc: bool = False,
) -> Any:
    """Return the exact JSON-safe value retained in a public result row.

    Arrow can produce nested values, datetimes, decimals, bytes and non-finite
    floats. Byte accounting must encode the same object that ``query_sql``
    returns; using ``default=str`` only while measuring would under-specify the
    downstream payload and leave raw unserializable objects in ``rows``.
    """
    if value is None:
        return None
    # The overwhelmingly common Arrow result cells need no conversion. Keep
    # this exact-type path before pandas/numpy/temporal inspection; subclasses
    # still flow through the specialized handling below.
    if type(value) in (str, int, bool):
        return value
    if value is pd.NA or value is pd.NaT:
        return None
    if type(value).__module__.startswith("numpy"):
        item = getattr(value, "item", None)
        if callable(item):
            try:
                return _json_safe_result_value(
                    item(),
                    assume_naive_datetime_utc=assume_naive_datetime_utc,
                )
            except (TypeError, ValueError, OverflowError):
                return str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        if value.tzinfo is None and assume_naive_datetime_utc:
            # Spark's HiveServer2 protocol returns naive Python datetimes even
            # for TIMESTAMP_LTZ. The executor marks only fields proven to come
            # from a mandatory UTC/LTZ session; unmarked SQL TIMESTAMP values
            # remain wall-clock values and deliberately receive no ``Z``.
            value = value.replace(tzinfo=timezone.utc)
        if value.tzinfo is not None:
            # DuckDB materializes TIMESTAMPTZ in the connection/session zone.
            # Public rows and their serialized-byte accounting must be stable
            # across hosts, so represent the instant canonically in UTC.
            return (
                value.astimezone(timezone.utc)
                .isoformat(timespec="microseconds")
                .replace("+00:00", "Z")
            )
        # A timezone-free SQL TIMESTAMP is a wall-clock value, not an instant.
        # Preserve that distinction while making its representation explicit.
        return value.isoformat(timespec="microseconds")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {
            str(key): _json_safe_result_value(
                item,
                assume_naive_datetime_utc=assume_naive_datetime_utc,
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [
            _json_safe_result_value(
                item,
                assume_naive_datetime_utc=assume_naive_datetime_utc,
            )
            for item in value
        ]
    # These are the complete scalar domains accepted by JSONEncoder after the
    # special temporal/numpy/float handling above. Avoid invoking json.dumps
    # for every ordinary Arrow cell merely to rediscover that fact.
    if isinstance(value, (str, int, bool)):
        return value
    return str(value)


# Keep Spark's public column metadata in the pandas-style vocabulary returned
# before the lossless object-frame fix.  The mapping is now stable and logical:
# nullable integral values remain ``int64`` instead of being mislabeled as the
# float dtype pandas previously used after coercing them.
_SPARK_PUBLIC_RESULT_TYPE_NAMES = {
    "BOOLEAN_TYPE": "bool",
    "TINYINT_TYPE": "int64",
    "SMALLINT_TYPE": "int64",
    "INT_TYPE": "int64",
    "BIGINT_TYPE": "int64",
    "FLOAT_TYPE": "float64",
    "DOUBLE_TYPE": "float64",
    "STRING_TYPE": "object",
    "TIMESTAMP_TYPE": "datetime64[ns]",
    "BINARY_TYPE": "object",
    "ARRAY_TYPE": "object",
    "MAP_TYPE": "object",
    "STRUCT_TYPE": "object",
    "UNION_TYPE": "object",
    "USER_DEFINED_TYPE": "object",
    "DECIMAL_TYPE": "object",
    "NULL_TYPE": "object",
    "DATE_TYPE": "object",
    "VARCHAR_TYPE": "object",
    "CHAR_TYPE": "object",
    "INTERVAL_YEAR_MONTH_TYPE": "object",
    "INTERVAL_DAY_TIME_TYPE": "object",
}


_JSON_STRING_ESCAPE_RE = re.compile(r'["\\\x00-\x1f]')


def _json_wire_size(value: Any) -> int:
    """Exact compact UTF-8 JSON size without constructing discarded JSON.

    ``query_sql`` returns Python rows, so the service still performs the real
    response serialization once. The previous guard serialized every row a
    first time and immediately discarded those bytes. This counter implements
    the exact ``ensure_ascii=False``/compact-separator contract used by that
    guard, including control escaping and UTF-8 validation.
    """
    if value is None:
        return 4
    if value is True:
        return 4
    if value is False:
        return 5
    if isinstance(value, str):
        encoded_size = len(value.encode("utf-8"))
        if _JSON_STRING_ESCAPE_RE.search(value) is None:
            return encoded_size + 2
        size = 2
        for character in value:
            codepoint = ord(character)
            if character in {'"', "\\"} or character in {
                "\b", "\f", "\n", "\r", "\t",
            }:
                size += 2
            elif codepoint <= 0x1F:
                size += 6
            else:
                size += len(character.encode("utf-8"))
        return size
    if isinstance(value, int):
        return len(str(value))
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite float is unavailable in JSON")
        # JSONEncoder deliberately calls the base float repr for subclasses.
        return len(float.__repr__(value))
    if isinstance(value, list):
        return 2 + max(0, len(value) - 1) + sum(
            _json_wire_size(item) for item in value
        )
    if isinstance(value, dict):
        size = 2 + max(0, len(value) - 1)
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("JSON object keys must be strings")
            size += _json_wire_size(key) + 1 + _json_wire_size(item)
        return size
    raise TypeError("unsupported JSON result value type")


def _public_arrow_type_name(field_type: Any) -> str:
    """Return stable metadata for the JSON-safe public Arrow result.

    Arrow's TIMESTAMPTZ type names include the DuckDB session timezone even
    though the public values above are normalized to UTC. Expose the normalized
    value contract rather than deployment-local connection state.
    """
    source_timezone = getattr(field_type, "tz", None)
    unit = getattr(field_type, "unit", None)
    if source_timezone is not None and unit:
        return f"timestamp[{unit}, tz=UTC]"
    return str(field_type)


def _arrow_batch_preserves_values_through_pandas(batch: Any) -> bool:
    """Whether bounded ``to_pandas`` rows preserve the public scalar domain.

    Native per-column ``to_pylist`` conversion is surprisingly expensive for
    wide Arrow batches. Pandas performs a vectorized conversion, but nullable
    integers would become floats and nested values can change representation.
    Admit only primitive/temporal types proven equivalent after
    ``_json_safe_result_value``; every other batch retains the conservative
    Arrow scalar path.
    """
    if not batch.num_columns:
        return False
    for column in batch.columns:
        field_type = column.type
        if pa.types.is_integer(field_type):
            if column.null_count:
                return False
            continue
        if any((
            pa.types.is_floating(field_type),
            pa.types.is_boolean(field_type),
            pa.types.is_string(field_type),
            pa.types.is_large_string(field_type),
            pa.types.is_binary(field_type),
            pa.types.is_large_binary(field_type),
            pa.types.is_fixed_size_binary(field_type),
            pa.types.is_decimal(field_type),
            pa.types.is_date32(field_type),
            pa.types.is_timestamp(field_type),
            pa.types.is_null(field_type),
        )):
            continue
        return False
    return True


def _latest_plan_stat(stats: Tuple[dict, ...], key: str) -> Any:
    for entry in reversed(stats):
        if isinstance(entry, dict) and key in entry:
            return entry[key]
    return None


def _engine_name(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().casefold()


def _actual_engine_from_plan_stats(plan_stats: Any, *, fallback: Any) -> str:
    """Resolve a stream's engine only after deferred delivery has finished."""
    try:
        stats = tuple(
            entry for entry in (getattr(plan_stats, "stats", ()) or ())
            if isinstance(entry, dict)
        )
        outcome = _latest_plan_stat(stats, "AUTO_ROUTING_OUTCOME")
        if isinstance(outcome, dict):
            actual = _engine_name(outcome.get("actual_engine"))
            if actual:
                return actual
        recorded = _engine_name(_latest_plan_stat(stats, "ENGINE"))
        if recorded:
            return recorded
        for entry in reversed(stats):
            attempt = entry.get("ENGINE_ATTEMPT")
            if isinstance(attempt, dict):
                attempted = _engine_name(attempt.get("engine"))
                if attempted:
                    return attempted
    except Exception:
        pass
    return _engine_name(fallback)


def _update_query_out(
    reader: DataReader,
    out: Optional[Dict[str, Any]],
    *,
    requested_engine: Any,
) -> None:
    """Expose data-free routing/cache facts to the calling service."""
    if out is None:
        return
    qpm = getattr(reader, "query_plan_manager", None)
    query_id = getattr(qpm, "query_id", "") if qpm is not None else ""
    query_hash = getattr(qpm, "query_hash", "") if qpm is not None else ""
    out["query_id"] = query_id if isinstance(query_id, str) else ""
    out["query_hash"] = query_hash if isinstance(query_hash, str) else ""
    role_policy_fingerprint = getattr(
        reader, "role_policy_fingerprint", "",
    )
    effective_policy_fingerprint = getattr(
        reader, "effective_policy_fingerprint", "",
    )
    if isinstance(role_policy_fingerprint, str) and role_policy_fingerprint:
        out["role_policy_fingerprint"] = role_policy_fingerprint
    if (
        isinstance(effective_policy_fingerprint, str)
        and effective_policy_fingerprint
    ):
        out["effective_policy_fingerprint"] = effective_policy_fingerprint

    plan_stats = getattr(reader, "plan_stats", None)
    stats = tuple(
        entry for entry in (getattr(plan_stats, "stats", ()) or ())
        if isinstance(entry, dict)
    )
    request = _latest_plan_stat(stats, "ENGINE_REQUEST")
    routing = _latest_plan_stat(stats, "AUTO_ROUTING")
    outcome = _latest_plan_stat(stats, "AUTO_ROUTING_OUTCOME")
    recorded_engine = _latest_plan_stat(stats, "ENGINE")
    presign_refresh = _latest_plan_stat(stats, "DUCKDB_PRESIGN_REFRESH")
    engine_failure = _latest_plan_stat(stats, "ENGINE_FAILURE")
    connection_cache = _latest_plan_stat(stats, "DUCKDB_CONNECTION_CACHE")
    engine_attempts = [
        dict(entry["ENGINE_ATTEMPT"])
        for entry in stats
        if isinstance(entry.get("ENGINE_ATTEMPT"), dict)
    ]
    if not any((
        request, routing, outcome, recorded_engine, presign_refresh,
        engine_failure, connection_cache, engine_attempts,
    )) and not any(
        any(str(key).startswith("FILE_CACHE_") or key == "ISLAND_CACHE" for key in entry)
        for entry in stats
    ):
        # Preserve the historical identity-only out contract for custom/legacy
        # DataReader implementations that do not publish engine plan stats.
        return

    requested = _engine_name(requested_engine)
    selected = ""
    if isinstance(request, dict):
        requested = _engine_name(request.get("requested_engine")) or requested
        selected = _engine_name(request.get("selected_engine"))
    if not selected and isinstance(routing, dict):
        selected = _engine_name(routing.get("selected_engine"))
    if not selected and isinstance(outcome, dict):
        selected = _engine_name(outcome.get("selected_engine"))

    actual = _actual_engine_from_plan_stats(plan_stats, fallback="")
    fallback = False
    if isinstance(outcome, dict):
        fallback = bool(outcome.get("fallback", False))
    selected = selected or actual or requested
    if actual and selected and not isinstance(outcome, dict):
        fallback = actual != selected

    cache: Dict[str, Any] = {}
    for entry in stats:
        for key, value in entry.items():
            if str(key).startswith("FILE_CACHE_"):
                cache[str(key)] = value
    island_cache = _latest_plan_stat(stats, "ISLAND_CACHE")
    if isinstance(island_cache, dict):
        cache["ISLAND_CACHE"] = dict(island_cache)
    if isinstance(connection_cache, dict):
        cache["DUCKDB_CONNECTION_CACHE"] = dict(connection_cache)

    out.update({
        "requested_engine": requested,
        "selected_engine": selected,
        "actual_engine": actual,
        "engine_fallback": fallback,
        "engine_attempts": engine_attempts,
        "engine_failure": (
            dict(engine_failure) if isinstance(engine_failure, dict) else {}
        ),
        "routing": dict(routing) if isinstance(routing, dict) else {},
        "cache": cache,
        "presign_refresh": (
            dict(presign_refresh)
            if isinstance(presign_refresh, dict) else {}
        ),
    })


def query_sql(
        organization: str,
        super_name: str,
        sql: str,
        limit: int,
        engine: Any,
        role_name: str,
        source: str = "sdk",
        out: Optional[Dict[str, Any]] = None,
        timeout_sec: Optional[float] = None,
        cancel_event: Optional[threading.Event] = None,
) -> Tuple[List[str], List[List[Any]], List[Dict[str, Any]]]:
    """
    Execute SQL query and return results in the format expected by MCP server.
    Returns: (columns, rows, columns_meta)

    ``source`` tags the query origin on the read monitoring entry
    (defaults to "sdk"; the MCP server passes "mcp"). When an ``out``
    dict is supplied it is populated with query identity plus data-free engine
    routing, fallback, cache, and credential-refresh metadata so the caller can
    correlate its own audit log to this read record.
    """
    request_deadline = _caller_deadline(timeout_sec)
    _validate_query_text_size(sql)
    _ensure_request_active(request_deadline, cancel_event)
    # Safety guard: ensure a LIMIT is present so unbounded queries don't
    # overwhelm the MCP response payload. Only plain SELECTs take an appended
    # LIMIT — EXPLAIN output is tiny and SHOW STATS does not accept a LIMIT.
    try:
        is_select = classify_query(sql, super_name).kind is CommandKind.SELECT
    except ValueError:
        is_select = True
    if is_select:
        sql = _ensure_sql_limit(sql, default_limit=limit)

    reader = DataReader(
        organization=organization, super_name=super_name, query=sql, source=source,
    )

    max_serialized_bytes = _positive_budget(
        "SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES", 16 * 1024 * 1024,
    )

    # SELECT responses use the Arrow stream in the normal API path.  Applying
    # the JSON byte budget after ``fetchdf()`` is too late: a few very wide
    # cells can consume unbounded pandas memory despite the outer row LIMIT.
    # Spark has no Arrow result stream, but its Thrift executor has a separate
    # bounded incremental materialization contract for this inline facade.
    from supertable.engine.engine_enum import Engine as _EngineEnum

    spark_materialized_response = bool(
        is_select
        and isinstance(engine, _EngineEnum)
        and engine is _EngineEnum.SPARK_SQL
    )
    stream_response = bool(
        is_select
        and isinstance(engine, _EngineEnum)
        and not spark_materialized_response
    )
    effective_inline_limit: Optional[int] = None
    if spark_materialized_response:
        # _ensure_sql_limit() above already validated/coerced ``limit``.  The
        # private materialization budget must remain positive even for LIMIT 0;
        # the postcondition below still requires the public result to be empty.
        effective_inline_limit = min(
            max(0, int(limit)),
            _positive_budget("SUPERTABLE_MAX_LIMIT", 10000),
        )
        try:
            result_value, status, message = reader.execute(
                role_name=role_name,
                engine=engine,
                with_scan=False,
                _materialized_row_limit=max(1, effective_inline_limit),
                _materialized_result_bytes=max_serialized_bytes,
                _deadline_monotonic=request_deadline,
                _cancel_event=cancel_event,
            )
        except BaseException:
            _update_query_out(reader, out, requested_engine=engine)
            raise
    elif stream_response:
        try:
            result_value, status, message = reader.execute_stream(
                role_name=role_name,
                engine=engine,
                max_batch_rows=_configured_result_stream_batch_rows(),
                max_batch_bytes=_configured_result_stream_batch_bytes(),
                timeout_sec=timeout_sec,
                cancel_event=cancel_event,
                _deadline_monotonic=request_deadline,
            )
        except BaseException:
            _update_query_out(reader, out, requested_engine=engine)
            raise
    else:
        # Diagnostic commands are intrinsically bounded; the non-enum branch
        # also preserves duck-typed compatibility for custom/test executors.
        try:
            result_value, status, message = reader.execute(
                role_name=role_name,
                engine=engine,
                with_scan=False,
            )
        except BaseException:
            _update_query_out(reader, out, requested_engine=engine)
            raise

    _update_query_out(reader, out, requested_engine=engine)

    if status == Status.ERROR:
        # ``message`` crosses several storage and execution boundaries and may
        # contain paths, signed URLs, or provider response bodies.  DataReader
        # records a bounded type-only diagnostic; the public facade exposes one
        # stable error contract and never repeats backend prose.
        raise RuntimeError(_QUERY_EXECUTION_ERROR)

    if spark_materialized_response:
        try:
            materialized_rows = int(result_value.shape[0])
        except (AttributeError, IndexError, TypeError, ValueError):
            raise RuntimeError(_QUERY_EXECUTION_ERROR) from None
        if (
            effective_inline_limit is None
            or materialized_rows > effective_inline_limit
        ):
            raise RuntimeError(_QUERY_EXECUTION_ERROR)

    if stream_response:
        stream = result_value
        schema = getattr(stream, "schema", None)
        if schema is None:
            close = getattr(stream, "close", None)
            if callable(close):
                close()
            raise RuntimeError("Query execution returned an invalid Arrow stream")
        columns = [str(name) for name in schema.names]
        columns_meta = [
            {
                "name": str(field.name),
                "type": _public_arrow_type_name(field.type),
                "nullable": bool(field.nullable),
            }
            for field in schema
        ]
        rows: List[List[Any]] = []
        try:
            serialized_bytes = _json_wire_size({
                "columns": columns,
                "rows": [],
                "columns_meta": columns_meta,
            })
        except (TypeError, ValueError):
            stream.close()
            raise RuntimeError(
                "Query result is not JSON serializable"
            ) from None
        if serialized_bytes > max_serialized_bytes:
            stream.close()
            raise RuntimeError(
                "Query result exceeds SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES"
            )
        try:
            for batch in stream:
                # Convert each bounded Arrow column in one native call. The
                # former scalar ``as_py`` loop crossed the Python/C boundary
                # once per cell. At the configured 256-row/4-MiB batch limits,
                # temporary conversion state stays bounded and the complete
                # batch is admitted atomically by the exact wire-size guard.
                raw_rows: Optional[Iterable[Tuple[Any, ...]]] = None
                if _arrow_batch_preserves_values_through_pandas(batch):
                    try:
                        pandas_batch = batch.to_pandas()
                    except Exception:
                        # Extremely wide/out-of-range temporal domains may not
                        # be representable by the installed pandas version.
                        # Preserve the Arrow scalar contract in that case.
                        pass
                    else:
                        raw_rows = pandas_batch.itertuples(
                            index=False, name=None,
                        )
                if raw_rows is None:
                    if batch.num_columns:
                        python_columns = [
                            batch.column(column_index).to_pylist()
                            for column_index in range(batch.num_columns)
                        ]
                        raw_rows = zip(*python_columns)
                    else:
                        raw_rows = (() for _ in range(batch.num_rows))
                batch_rows = [
                    [
                        _json_safe_result_value(value)
                        for value in raw_row
                    ]
                    for raw_row in raw_rows
                ]
                try:
                    encoded_batch = json.dumps(
                        batch_rows,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    ).encode("utf-8")
                except (TypeError, ValueError):
                    raise RuntimeError(
                        "Query result is not JSON serializable"
                    ) from None
                # Remove the batch's surrounding ``[]``. The response's rows
                # array already contributes those two bytes in the skeleton;
                # only a comma between non-empty batches remains to add.
                additional_bytes = len(encoded_batch) - 2
                if rows and batch_rows:
                    additional_bytes += 1
                if serialized_bytes + additional_bytes > max_serialized_bytes:
                    cancel = getattr(stream, "cancel", None)
                    try:
                        if callable(cancel):
                            cancel()
                    except Exception:
                        pass
                    raise RuntimeError(
                        "Query result exceeds "
                        "SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES"
                    )
                serialized_bytes += additional_bytes
                rows.extend(batch_rows)
        finally:
            stream.close()
            _update_query_out(reader, out, requested_engine=engine)
        return columns, rows, columns_meta

    # Convert DataFrame to the expected format
    result_df = result_value
    columns = list(result_df.columns)
    result_dtypes = list(result_df.dtypes)
    spark_type_codes: Tuple[str, ...] = ()
    spark_utc_timestamp_indexes: frozenset[int] = frozenset()
    if spark_materialized_response:
        from supertable.engine.spark_thrift import (
            SPARK_RESULT_TYPE_CODES_ATTR,
            SPARK_THRIFT_TYPE_CODES,
            SPARK_UTC_TIMESTAMP_INDEXES_ATTR,
        )

        attrs = getattr(result_df, "attrs", {})
        raw_type_codes = (
            attrs.get(SPARK_RESULT_TYPE_CODES_ATTR)
            if isinstance(attrs, dict)
            else None
        )
        raw_utc_indexes = (
            attrs.get(SPARK_UTC_TIMESTAMP_INDEXES_ATTR)
            if isinstance(attrs, dict)
            else None
        )
        if (
            type(raw_type_codes) is not tuple
            or len(raw_type_codes) != len(columns)
            or not raw_type_codes
            or any(
                type(code) is not str or code not in SPARK_THRIFT_TYPE_CODES
                for code in raw_type_codes
            )
            or type(raw_utc_indexes) is not tuple
            or any(type(index) is not int for index in raw_utc_indexes)
        ):
            raise RuntimeError(_QUERY_EXECUTION_ERROR)
        spark_type_codes = raw_type_codes
        expected_utc_indexes = tuple(
            index
            for index, code in enumerate(spark_type_codes)
            if code == "TIMESTAMP_TYPE"
        )
        if raw_utc_indexes != expected_utc_indexes:
            raise RuntimeError(_QUERY_EXECUTION_ERROR)
        spark_utc_timestamp_indexes = frozenset(raw_utc_indexes)
    columns_meta = [
        {
            "name": col,
            "type": (
                _SPARK_PUBLIC_RESULT_TYPE_NAMES[spark_type_codes[index]]
                if spark_type_codes
                else str(result_dtypes[index])
            ),
            "nullable": True
        }
        for index, col in enumerate(columns)
    ]

    # Sanitize pandas NA variants (pd.NA, pd.NaT, np.nan) to Python None
    # so downstream JSON serialization does not choke on NAType.
    # Note: DataFrame.where() + .values.tolist() does NOT fully sanitize
    # nullable dtypes (Int64, string) or np.nan in float columns.
    # We must sanitize the final Python objects after .tolist().
    rows = []
    try:
        serialized_bytes = _json_wire_size({
            "columns": columns,
            "rows": [],
            "columns_meta": columns_meta,
        })
    except (TypeError, ValueError):
        raise RuntimeError("Query result is not JSON serializable") from None
    if serialized_bytes > max_serialized_bytes:
        raise RuntimeError(
            "Query result exceeds SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES"
        )
    result_batch_rows = _configured_result_stream_batch_rows()
    pending_rows: List[List[Any]] = []

    def admit_pending_rows() -> None:
        nonlocal serialized_bytes
        if not pending_rows:
            return
        try:
            encoded_batch = json.dumps(
                pending_rows,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError):
            raise RuntimeError("Query result is not JSON serializable") from None
        additional_bytes = len(encoded_batch) - 2
        if rows:
            additional_bytes += 1
        if serialized_bytes + additional_bytes > max_serialized_bytes:
            raise RuntimeError(
                "Query result exceeds SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES"
            )
        serialized_bytes += additional_bytes
        rows.extend(pending_rows)
        pending_rows.clear()

    for raw_row in result_df.itertuples(index=False, name=None):
        pending_rows.append([
            _json_safe_result_value(
                value,
                assume_naive_datetime_utc=(
                    column_index in spark_utc_timestamp_indexes
                ),
            )
            for column_index, value in enumerate(raw_row)
        ])
        if len(pending_rows) >= result_batch_rows:
            admit_pending_rows()
    admit_pending_rows()

    return columns, rows, columns_meta


def query_sql_policy_fingerprint(
    organization: str,
    super_name: str,
    sql: str,
    engine: Any,
    role_name: str,
    *,
    timeout_sec: float,
    source: str = "sdk",
    out: Optional[Dict[str, Any]] = None,
    cancel_event: Optional[threading.Event] = None,
    expected_role_policy_fingerprint: Optional[str] = None,
) -> str:
    """Resolve the exact current query policy through the normal read preflight.

    This runs parser, target-existence, RBAC, and estimator/snapshot pinning, but
    deliberately stops before constructing an executor. Services may bind the
    returned value to an artifact and pass it back to ``query_sql_stream`` as
    ``expected_effective_policy_fingerprint`` to close a queued-execution gap.
    """
    request_deadline = _caller_deadline(timeout_sec)
    assert request_deadline is not None
    expected_role_policy_fingerprint = validate_policy_fingerprint(
        expected_role_policy_fingerprint,
        label="expected_role_policy_fingerprint",
    )
    _validate_query_text_size(sql)
    _ensure_request_active(request_deadline, cancel_event)
    try:
        command = classify_query(sql, super_name)
    except ValueError:
        raise ValueError(
            "policy query must be one valid SELECT statement"
        ) from None
    if command.kind is not CommandKind.SELECT:
        raise ValueError("policy query must be a SELECT statement")

    reader = DataReader(
        organization=organization,
        super_name=super_name,
        query=command.sql,
        source=source,
    )
    try:
        _result, status, message = reader.execute(
            role_name=role_name,
            engine=engine,
            with_scan=False,
            _deadline_monotonic=request_deadline,
            _cancel_event=cancel_event,
            expected_role_policy_fingerprint=(
                expected_role_policy_fingerprint
            ),
            _policy_fingerprint_only=True,
        )
    except BaseException:
        _update_query_out(reader, out, requested_engine=engine)
        raise
    _update_query_out(reader, out, requested_engine=engine)
    if status is Status.ERROR:
        raise RuntimeError(f"Policy preflight failed: {message}")
    fingerprint = validate_policy_fingerprint(
        reader.effective_policy_fingerprint,
        label="effective policy fingerprint",
    )
    if fingerprint is None:
        raise PermissionError("Effective read policy fingerprint is unavailable")
    return fingerprint


def estimate_query_sql(
    organization: str,
    super_name: str,
    sql: str,
    engine: Any,
    role_name: str,
    *,
    timeout_sec: float,
    source: str = "sdk",
    cancel_event: Optional[threading.Event] = None,
    expected_role_policy_fingerprint: Optional[str] = None,
) -> Dict[str, Any]:
    """Return a data-free, RBAC-aware query estimate without execution.

    Parser validation, table existence, RBAC, linked-share policy, immutable
    snapshot pinning, statistics seals, and pruning all run through the same
    path as execution. Only aggregate counts/bytes and completeness flags are
    exposed; storage paths, SQL text, predicates, credentials, and row values
    are never returned.
    """
    request_deadline = _caller_deadline(timeout_sec)
    assert request_deadline is not None
    expected_role_policy_fingerprint = validate_policy_fingerprint(
        expected_role_policy_fingerprint,
        label="expected_role_policy_fingerprint",
    )
    _validate_query_text_size(sql)
    _ensure_request_active(request_deadline, cancel_event)
    try:
        command = classify_query(sql, super_name)
    except ValueError:
        raise ValueError(
            "estimate query must be one valid SELECT statement"
        ) from None
    if command.kind is not CommandKind.SELECT:
        raise ValueError("estimate query must be a SELECT statement")
    reader = DataReader(
        organization=organization,
        super_name=super_name,
        query=command.sql,
        source=source,
    )
    _result, status, message = reader.execute(
        role_name=role_name,
        engine=engine,
        with_scan=False,
        _deadline_monotonic=request_deadline,
        _cancel_event=cancel_event,
        expected_role_policy_fingerprint=expected_role_policy_fingerprint,
        _policy_fingerprint_only=True,
    )
    if status is Status.ERROR:
        raise RuntimeError(f"Query estimate failed: {message}")
    reflection = getattr(reader, "last_reflection", None)
    if reflection is None:
        raise RuntimeError("Query estimate did not produce a reflection")
    supers = list(getattr(reflection, "supers", ()) or ())
    candidate_rows_complete = bool(supers) and all(
        bool(getattr(item, "candidate_rows_complete", False)) for item in supers
    )
    candidate_rows = sum(
        max(0, int(getattr(item, "candidate_rows", 0) or 0)) for item in supers
    )
    candidate_row_groups_complete = bool(supers) and all(
        bool(getattr(item, "candidate_row_groups_complete", False))
        for item in supers
    )
    candidate_row_groups = sum(
        max(0, int(getattr(item, "candidate_row_groups", 0) or 0))
        for item in supers
    )
    qpm = getattr(reader, "query_plan_manager", None)
    return {
        "version": 1,
        "requested_engine": _engine_name(engine),
        "recommended_request_engine": "auto",
        "storage_type": str(getattr(reflection, "storage_type", "") or "")[:32],
        "table_count": len(supers),
        "file_count": max(0, int(getattr(reflection, "total_reflections", 0) or 0)),
        "estimated_scan_bytes": max(
            0, int(getattr(reflection, "reflection_bytes", 0) or 0)
        ),
        "source_bytes": max(0, int(getattr(reflection, "source_bytes", 0) or 0)),
        "source_bytes_complete": bool(
            getattr(reflection, "source_bytes_complete", False)
        ),
        "row_group_scan_bytes": max(
            0, int(getattr(reflection, "row_group_scan_bytes", 0) or 0)
        ),
        "row_group_scan_bytes_complete": bool(
            getattr(reflection, "row_group_scan_bytes_complete", False)
        ),
        "decoded_bytes": max(0, int(getattr(reflection, "decoded_bytes", 0) or 0)),
        "decoded_bytes_complete": bool(
            getattr(reflection, "decoded_bytes_complete", False)
        ),
        "candidate_rows": candidate_rows,
        "candidate_rows_complete": candidate_rows_complete,
        "candidate_row_groups": candidate_row_groups,
        "candidate_row_groups_complete": candidate_row_groups_complete,
        "has_active_tombstone": bool(
            getattr(reflection, "tombstone_views", {})
        ),
        "query_id": str(getattr(qpm, "query_id", "") or "")[:128],
        "query_hash": str(getattr(qpm, "query_hash", "") or "")[:128],
    }


def query_sql_stream(
    organization: str,
    super_name: str,
    sql: str,
    engine: Any,
    role_name: str,
    *,
    max_total_rows: int,
    timeout_sec: float,
    source: str = "sdk",
    out: Optional[Dict[str, Any]] = None,
    max_batch_rows: Optional[int] = None,
    max_batch_bytes: Optional[int] = None,
    cancel_event: Optional[threading.Event] = None,
    expected_role_policy_fingerprint: Optional[str] = None,
    expected_effective_policy_fingerprint: Optional[str] = None,
    _odata_identity: bool = False,
    _odata_continuation_boundary: Optional[
        ODataContinuationBoundary
    ] = None,
    _odata_continuation_capability: object = None,
) -> Any:
    """Return a bounded, RBAC-filtered Arrow export stream without Python rows.

    Unlike :func:`query_sql`, this helper may exceed
    ``SUPERTABLE_MAX_LIMIT``. It cannot be called without explicit positive
    row and time budgets, and the ordinary DataReader path remains capped.
    Consumers must exhaust, cancel, or close the returned stream.
    """
    request_deadline = _caller_deadline(timeout_sec)
    assert request_deadline is not None
    expected_role_policy_fingerprint = validate_policy_fingerprint(
        expected_role_policy_fingerprint,
        label="expected_role_policy_fingerprint",
    )
    expected_effective_policy_fingerprint = validate_policy_fingerprint(
        expected_effective_policy_fingerprint,
        label="expected_effective_policy_fingerprint",
    )
    if _odata_continuation_boundary is not None and (
        not _odata_identity
        or _odata_continuation_capability
        is not _ODATA_CONTINUATION_CAPABILITY
        or not isinstance(
            _odata_continuation_boundary,
            ODataContinuationBoundary,
        )
    ):
        raise ValueError("OData continuation requires the trusted OData stream")
    _validate_query_text_size(sql)
    _ensure_request_active(request_deadline, cancel_event)
    if (
        isinstance(max_total_rows, bool)
        or not isinstance(max_total_rows, int)
        or max_total_rows <= 0
    ):
        raise ValueError("max_total_rows must be a positive integer")
    try:
        command = classify_query(sql, super_name)
    except ValueError:
        raise ValueError(
            "export query must be one valid SELECT statement"
        ) from None
    if command.kind is not CommandKind.SELECT:
        raise ValueError("export query must be a SELECT statement")

    bounded_sql = _ensure_sql_limit(
        command.sql,
        max_total_rows,
        maximum_limit=max_total_rows,
    )
    configured_batch_rows = _configured_result_stream_batch_rows()
    if max_batch_rows is None:
        resolved_batch_rows = configured_batch_rows
    else:
        if (
            isinstance(max_batch_rows, bool)
            or not isinstance(max_batch_rows, int)
            or max_batch_rows <= 0
        ):
            raise ValueError("max_batch_rows must be a positive integer")
        resolved_batch_rows = min(max_batch_rows, configured_batch_rows)
    configured_batch_bytes = _configured_result_stream_batch_bytes()
    if max_batch_bytes is None:
        resolved_batch_bytes = configured_batch_bytes
    else:
        if (
            isinstance(max_batch_bytes, bool)
            or not isinstance(max_batch_bytes, int)
            or max_batch_bytes <= 0
        ):
            raise ValueError("max_batch_bytes must be a positive integer")
        resolved_batch_bytes = min(max_batch_bytes, configured_batch_bytes)

    reader = DataReader(
        organization=organization,
        super_name=super_name,
        query=bounded_sql,
        source=source,
    )
    try:
        stream, status, message = reader.execute_export_stream(
            role_name=role_name,
            engine=engine,
            max_total_rows=max_total_rows,
            timeout_sec=timeout_sec,
            max_batch_rows=resolved_batch_rows,
            max_batch_bytes=resolved_batch_bytes,
            cancel_event=cancel_event,
            _deadline_monotonic=request_deadline,
            expected_role_policy_fingerprint=(
                expected_role_policy_fingerprint
            ),
            expected_effective_policy_fingerprint=(
                expected_effective_policy_fingerprint
            ),
            _odata_identity=_odata_identity,
            _odata_continuation_boundary=_odata_continuation_boundary,
            _odata_continuation_capability=_odata_continuation_capability,
        )
    except BaseException:
        _update_query_out(reader, out, requested_engine=engine)
        raise
    _update_query_out(reader, out, requested_engine=engine)
    if status == Status.ERROR:
        _cancel_and_close_stream(stream)
        raise RuntimeError(_QUERY_EXECUTION_ERROR)
    return _QueryOutResultStream(
        stream,
        lambda: _update_query_out(reader, out, requested_engine=engine),
    )


def query_odata_sql_stream(
    organization: str,
    super_name: str,
    sql: str,
    role_name: str,
    *,
    max_total_rows: int,
    timeout_sec: float,
    source: str = "odata",
    out: Optional[Dict[str, Any]] = None,
    max_batch_rows: Optional[int] = None,
    max_batch_bytes: Optional[int] = None,
    cancel_event: Optional[threading.Event] = None,
    expected_role_policy_fingerprint: Optional[str] = None,
    expected_effective_policy_fingerprint: Optional[str] = None,
    continuation_boundary: Optional[Mapping[str, Any]] = None,
) -> Any:
    """Trusted keyed OData stream over one proven local table.

    The returned Arrow schema contains the fixed private
    ``__supertable_odata_rowid__`` column after deletion-vector and RBAC
    filtering.  Core must replace it with a deployment-keyed opaque identifier
    before returning a response.  This path is intentionally DuckDB-only and
    never changes ordinary SDK SELECT visibility.
    """
    from supertable.engine.engine_enum import Engine

    validated_boundary = validate_odata_continuation_boundary(
        continuation_boundary
    )

    return query_sql_stream(
        organization=organization,
        super_name=super_name,
        sql=sql,
        engine=Engine.DUCKDB,
        role_name=role_name,
        max_total_rows=max_total_rows,
        timeout_sec=timeout_sec,
        source=source,
        out=out,
        max_batch_rows=max_batch_rows,
        max_batch_bytes=max_batch_bytes,
        cancel_event=cancel_event,
        expected_role_policy_fingerprint=expected_role_policy_fingerprint,
        expected_effective_policy_fingerprint=(
            expected_effective_policy_fingerprint
        ),
        _odata_identity=True,
        _odata_continuation_boundary=validated_boundary,
        _odata_continuation_capability=_ODATA_CONTINUATION_CAPABILITY,
    )
