# route: supertable.data_reader

from __future__ import annotations

import json
import math
import re
import threading
from datetime import date, datetime, timezone
from enum import Enum
from typing import Callable, Optional, Tuple, Any, List, Dict

import pandas as pd
import polars as pl
import sqlglot
from sqlglot import exp

from supertable.config.defaults import logger
from supertable.config.settings import settings
from supertable.errors import SuperTableNotFoundError, TableNotFoundError
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.utils.timer import Timer
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
from supertable.rbac.access_control import restrict_read_access  # noqa: F401

from supertable.engine.data_estimator import DataEstimator
from supertable.engine.executor import Executor
from supertable.engine.engine_enum import Engine as engine
from supertable.data_classes import TombstoneDef, RbacViewDef
from supertable.redis_catalog import RedisCatalog
from supertable.system_query import classify_query, CommandKind


class Status(Enum):
    OK = "ok"
    ERROR = "error"


def _cancel_and_close_stream(stream: Any) -> None:
    """Terminate a live result stream that cannot be returned to its caller."""
    cancel = getattr(stream, "cancel", None)
    try:
        if callable(cancel):
            cancel()
    except Exception as exc:
        logger.warning(
            "Arrow result cancellation during monitoring failure: %s", exc,
        )
    finally:
        close = getattr(stream, "close", None)
        try:
            if callable(close):
                close()
        except Exception as exc:
            logger.warning(
                "Arrow result close during monitoring failure: %s", exc,
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
                    self._finish(Status.ERROR.value, str(exc))
                except BaseException as monitoring_exc:
                    setattr(monitoring_exc, "stream_error", exc)
                    raise monitoring_exc from exc
                raise
            self._finish(Status.OK.value, None)
            raise
        except BaseException as exc:
            try:
                self._finish(Status.ERROR.value, str(exc))
            except BaseException as monitoring_exc:
                setattr(monitoring_exc, "stream_error", exc)
                raise monitoring_exc from exc
            raise
        self._record_batch(batch)
        return batch

    def cancel(self) -> None:
        cancel = getattr(self._inner, "cancel", None)
        try:
            if callable(cancel):
                cancel()
            else:
                close = getattr(self._inner, "close", None)
                if callable(close):
                    close()
        finally:
            self._finish(Status.ERROR.value, "result stream cancelled")

    def close(self) -> None:
        close = getattr(self._inner, "close", None)
        try:
            if callable(close):
                close()
        finally:
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


def _validate_query_complexity(sql: str) -> None:
    """Reject oversized/deep SELECT syntax before planning or execution."""
    if len(str(sql).encode("utf-8")) > _positive_budget(
        "SUPERTABLE_MAX_QUERY_BYTES", 64 * 1024,
    ):
        raise ValueError("SQL text exceeds the configured query-size budget")
    try:
        statements = [
            statement
            for statement in sqlglot.parse(sql, read="duckdb")
            if statement is not None
        ]
    except Exception as exc:
        raise ValueError("SQL query is invalid") from exc
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
    except Exception as exc:
        raise RuntimeError("Linked-share row filter is invalid") from exc
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
    return where.this.sql(dialect="duckdb")


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
    ):
        self.super_name = super_name
        self.organization = organization
        self.query = query
        # Query origin surfaced in the reads monitoring tab. "sdk" is the
        # default for direct SDK callers; the API/OData/MCP entry points
        # pass "api"/"odata"/"mcp" so each query records where it came from.
        self.source = source
        self._allow_bounded_collection_aggregates = bool(
            _allow_bounded_collection_aggregates
        )

        self.storage: StorageInterface = get_storage()

        self.timer: Optional[Timer] = None
        self.plan_stats: Optional[PlanStats] = None
        self.query_plan_manager: Optional[QueryPlanManager] = None

        self._log_ctx = ""

    def _lp(self, msg: str) -> str:
        return f"{self._log_ctx}{msg}"

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
        self, super_name: str, simple_name: str,
    ) -> Tuple[Optional[str], bool]:
        """Return the latest stats pointer and whether a share filter exists.

        Prefers the leaf payload (already in Redis); falls back to reading the
        snapshot JSON from storage.  The filter marker is read from the same
        pinned leaf/snapshot context as the pointer so ``SHOW STATS`` cannot
        authorize one version and then expose another version's raw artifact.

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

        catalog = RedisCatalog()
        leaf = catalog.get_leaf(self.organization, super_name, simple_name)
        if not isinstance(leaf, dict):
            return None, False
        payload = leaf.get("payload")
        filtered = has_row_filter(leaf) or has_row_filter(payload)
        complete_payload = complete_snapshot_payload(
            payload,
            expected_version=leaf.get("version"),
            require_policy_marker=True,
        )
        if complete_payload is not None:
            return (
                complete_payload.get("stats_file"),
                filtered or has_row_filter(complete_payload),
            )
        path = leaf.get("path")
        if not path:
            return None, filtered
        from supertable.super_table import SuperTable
        snapshot = SuperTable(
            super_name, self.organization, create_if_missing=False,
        ).read_simple_table_snapshot(path)
        if not isinstance(snapshot, dict):
            return None, filtered
        return snapshot.get("stats_file"), filtered or has_row_filter(snapshot)

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
            load_stats,
            stats_cache_identity,
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
            logger.warning(self._lp(f"[show-stats] target missing: {e}"))
            return pd.DataFrame(), Status.ERROR, str(e)

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
            stats_file, share_row_filtered = self._resolve_latest_stats_context(
                super_name, simple_name,
            )
        except Exception as e:
            # Resolving the linked-share overlay may require the immutable
            # snapshot document.  Backend failures can contain its physical
            # key or a presigned URL, so retain details only in trusted logs.
            # More importantly, an unreadable overlay cannot prove that raw
            # full-table statistics are safe for this caller.
            logger.error(self._lp(f"[show-stats] policy resolution failed: {e}"))
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
                load_stats(
                    stats_file,
                    allow_cache=True,
                    cache_identity=stats_cache_identity(
                        stats_file,
                        organization=self.organization,
                        storage=self.storage,
                    ),
                )
                if stats_file else None
            )
        except Exception as e:
            logger.error(self._lp(f"[show-stats] failed to load stats: {e}"))
            return pd.DataFrame(), Status.ERROR, str(e)

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
    ) -> Tuple[Any, Status, Optional[str]]:
        status = Status.ERROR
        message: Optional[str] = None
        self.timer = Timer()
        self.plan_stats = PlanStats()

        # Classify into an allowed read-path command. Ordinary SELECTs fall
        # through unchanged; EXPLAIN/SHOW STATS are the two diagnostic
        # extensions. A recognised-but-malformed command (e.g. SHOW STATS with
        # no table) returns a clean error rather than raising.
        try:
            command = classify_query(self.query, self.super_name)
        except ValueError as e:
            logger.warning(self._lp(f"rejected query: {e}"))
            return pd.DataFrame(), Status.ERROR, str(e)

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
                logger.warning(self._lp(f"rejected query: {e}"))
                return pd.DataFrame(), Status.ERROR, str(e)

        bounded_sql = command.sql
        if command.kind is CommandKind.SELECT:
            # Enforce the server ceiling at the DataReader boundary too.  SDK
            # callers can instantiate DataReader directly and must not bypass
            # query_sql()'s convenience LIMIT injection.
            bounded_sql = _ensure_sql_limit(
                command.sql,
                _positive_budget("SUPERTABLE_MAX_LIMIT", 5000),
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
            logger.warning(self._lp(f"rejected query: {e}"))
            return pd.DataFrame(), Status.ERROR, str(e)
        tables = parser.get_table_tuples()
        physical_tables = parser.get_physical_tables()

        try:
            aggregate_children = self._resolve_aggregate_children(
                physical_tables
            )
        except Exception as e:
            logger.warning(self._lp(f"aggregate expansion failed: {e}"))
            return pd.DataFrame(), Status.ERROR, str(e)

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
            logger.warning(self._lp(f"target missing: {e}"))
            return pd.DataFrame(), Status.ERROR, str(e)

        # RBAC check — also returns per-alias column/row filter definitions.
        # PermissionError propagates to the caller (legacy behaviour).
        rbac_kwargs = {
            "super_name": self.super_name,
            "organization": self.organization,
            "role_name": role_name,
            "tables": tables,
            "physical_tables": physical_tables,
        }
        if aggregate_children:
            rbac_kwargs["aggregate_children"] = aggregate_children
        rbac_views = restrict_read_access(**rbac_kwargs)
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
                        f"{observation_error}"
                    )
                )
            # Make executor aware of storage for presign retry
            executor = Executor(
                storage=self.storage,
                organization=self.organization,
                auto_history_provider=history_provider,
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
                        f"[prune] predicate extraction failed: {pc_err}"))
                predicate_constraints = _engine_safe_predicate_constraints(
                    predicate_constraints, engine,
                )
                try:
                    join_edges = parser.get_join_edges()
                except Exception as je_err:
                    logger.debug(self._lp(
                        f"[prune] join-edge extraction failed: {je_err}"))

            # 1) ESTIMATE — use physical_tables so CTE aliases are excluded
            estimator_kwargs = dict(
                organization=self.organization,
                storage=self.storage,
                tables=physical_tables,
                predicate_constraints=predicate_constraints,
                join_edges=join_edges,
                join_pruning_lanes=_engine_safe_join_pruning_lanes(engine),
                plan_stats=self.plan_stats,
            )
            if aggregate_children:
                estimator_kwargs["aggregate_children"] = aggregate_children
            estimator = DataEstimator(**estimator_kwargs)
            reflection = estimator.estimate()

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
            resolved_tombstones = {}
            for td in tables:
                table_key = (td.super_name.lower(), td.simple_name.lower())
                sup = snapshots_by_key.get(table_key)
                if sup is None:
                    # CTE aliases have no physical snapshot of their own.
                    continue

                tombstone_key = getattr(sup, "tombstone_key", None)
                tombstone_rows = getattr(sup, "tombstone_rows", None)
                tombstone_digest = getattr(sup, "tombstone_digest", None)
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
                    resolved_tombstone = resolved_tombstones.get(table_key)
                    if resolved_tombstone is None:
                        try:
                            resolved_tombstone = estimator._to_duckdb_path(
                                tombstone_key
                            )
                        except Exception as resolve_err:
                            raise RuntimeError(
                                f"Unable to resolve required deletion-vector for "
                                f"{td.super_name}.{td.simple_name}"
                            ) from resolve_err
                        if (
                            not isinstance(resolved_tombstone, str)
                            or not resolved_tombstone
                        ):
                            raise RuntimeError(
                                f"Unable to resolve required deletion-vector for "
                                f"{td.super_name}.{td.simple_name}"
                            )
                        # A bare relative key is a valid LOCAL path, but for an
                        # object-store reflection it means every URL/presign
                        # resolver failed.  Do not let DuckDB accidentally read
                        # a same-named local file and apply a foreign DV.
                        storage_type = (reflection.storage_type or "").lower()
                        if (
                            "://" not in resolved_tombstone
                            and not resolved_tombstone.startswith("/")
                            and "local" not in storage_type
                        ):
                            raise RuntimeError(
                                f"Unable to resolve required deletion-vector for "
                                f"{td.super_name}.{td.simple_name}"
                            )
                        resolved_tombstones[table_key] = resolved_tombstone
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
                    )

                # Linked-share policy is pinned alongside the same resources.
                share_row_filter = getattr(sup, "share_row_filter", None)
                if share_row_filter:
                    share_row_filter = _validated_share_row_filter(
                        share_row_filter
                    )
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

            if not reflection.supers:
                message = "No parquet files found"
                return pd.DataFrame(), status, message

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
                result_value, _engine_used = executor.execute_stream(
                    engine=exec_engine,
                    reflection=reflection,
                    parser=parser,
                    query_manager=self.query_plan_manager,
                    timer=self.timer,
                    plan_stats=self.plan_stats,
                    log_prefix=self._lp(""),
                    max_batch_rows=_stream_batch_rows,
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
                )
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
        except Exception as e:
            message = _redact_storage_credentials(e)
            logger.error(self._lp(f"Exception: {message}"))
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
                    raise MonitoringPostExecutionError(
                        organization=stream_qpm.organization,
                        super_name=stream_qpm.super_name,
                        query_id=str(getattr(stream_qpm, "query_id", "")),
                        status=final_status,
                        cause=exc,
                    ) from exc
                finally:
                    stream_timer.capture_and_reset_timing(event="EXTENDING_PLAN")

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
        except MonitoringDurabilityError as e:
            if _streaming:
                _cancel_and_close_stream(result_value)
            qpm = self.query_plan_manager
            raise MonitoringPostExecutionError(
                organization=str(getattr(qpm, "organization", self.organization)),
                super_name=str(getattr(qpm, "super_name", self.super_name)),
                query_id=str(getattr(qpm, "query_id", "")),
                status=str(status.value),
                cause=e,
            ) from e
        except Exception as e:
            logger.error(self._lp(f"extend_execution_plan exception: {e}"))

        self.timer.capture_and_reset_timing(event="EXTENDING_PLAN")
        return result_value, status, message

    def execute_stream(
        self,
        role_name: str,
        engine: engine = engine.ISLANDDB,
        *,
        max_batch_rows: Optional[int] = None,
    ) -> Tuple[Any, Status, Optional[str]]:
        """Execute through the normal preflight/RBAC path as an Arrow stream.

        Streaming is intentionally explicit and never disguises a materialized
        fallback. DuckDB and IslandDB return cancellable Arrow batches (AUTO
        may safely select either); unsupported Spark requests return the
        ordinary ``Status.ERROR`` result without running user SQL.
        """
        return self.execute(
            role_name=role_name,
            engine=engine,
            with_scan=False,
            _streaming=True,
            _stream_batch_rows=max_batch_rows,
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


def _ensure_sql_limit(sql: str, default_limit: int) -> str:
    """
    If the outermost query has no LIMIT clause, append one.

    Only appends when the SQL does not already end with a LIMIT (ignoring
    trailing whitespace/semicolons).  This avoids breaking queries that
    already specify their own LIMIT, subqueries that contain LIMIT internally,
    or CTEs.
    """
    try:
        requested = int(default_limit)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("Query limit must be an integer") from exc
    requested = max(0, requested)
    maximum = _positive_budget("SUPERTABLE_MAX_LIMIT", 5000)
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


def _json_safe_result_value(value: Any) -> Any:
    """Return the exact JSON-safe value retained in a public result row.

    Arrow can produce nested values, datetimes, decimals, bytes and non-finite
    floats. Byte accounting must encode the same object that ``query_sql``
    returns; using ``default=str`` only while measuring would under-specify the
    downstream payload and leave raw unserializable objects in ``rows``.
    """
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if type(value).__module__.startswith("numpy"):
        item = getattr(value, "item", None)
        if callable(item):
            try:
                return _json_safe_result_value(item())
            except (TypeError, ValueError, OverflowError):
                return str(value)
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
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
            str(key): _json_safe_result_value(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_safe_result_value(item) for item in value]
    try:
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError):
        return str(value)
    return value


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


def query_sql(
        organization: str,
        super_name: str,
        sql: str,
        limit: int,
        engine: Any,
        role_name: str,
        source: str = "sdk",
        out: Optional[Dict[str, Any]] = None,
) -> Tuple[List[str], List[List[Any]], List[Dict[str, Any]]]:
    """
    Execute SQL query and return results in the format expected by MCP server.
    Returns: (columns, rows, columns_meta)

    ``source`` tags the query origin on the read monitoring entry
    (defaults to "sdk"; the MCP server passes "mcp"). When an ``out``
    dict is supplied it is populated with ``query_id``/``query_hash`` so
    the caller can correlate its own audit log to this read record.
    """
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

    # SELECT responses use the Arrow stream in the normal API path.  Applying
    # the JSON byte budget after ``fetchdf()`` is too late: a few very wide
    # cells can consume unbounded pandas memory despite the outer row LIMIT.
    # Spark does not expose a cancellable incremental result contract here, so
    # fail explicitly instead of silently falling back to materialization.
    from supertable.engine.engine_enum import Engine as _EngineEnum

    stream_response = bool(is_select and isinstance(engine, _EngineEnum))
    if stream_response and engine is _EngineEnum.SPARK_SQL:
        raise RuntimeError(
            "Bounded query_sql responses do not support Spark SQL streaming"
        )
    if stream_response:
        result_value, status, message = reader.execute_stream(
            role_name=role_name,
            engine=engine,
            # Serialized-byte accounting begins after a native Arrow batch is
            # produced. One row per producer batch prevents multiple
            # arbitrary-width cells from accumulating ahead of that guard.
            max_batch_rows=1,
        )
    else:
        # Diagnostic commands are intrinsically bounded; the non-enum branch
        # also preserves duck-typed compatibility for custom/test executors.
        result_value, status, message = reader.execute(
            role_name=role_name,
            engine=engine,
            with_scan=False,
        )

    # Expose the query identity so the caller (e.g. the MCP audit log) can
    # link back to this read's monitoring entry. Populated even on error,
    # since the QueryPlanManager is created before execution.
    if out is not None:
        qpm = reader.query_plan_manager
        if qpm is not None:
            out["query_id"] = qpm.query_id
            out["query_hash"] = qpm.query_hash

    if status == Status.ERROR:
        raise RuntimeError(f"Query execution failed: {message}")

    max_serialized_bytes = _positive_budget(
        "SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES", 16 * 1024 * 1024,
    )

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
        serialized_bytes = len(json.dumps(
            {"columns": columns, "rows": [], "columns_meta": columns_meta},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8"))
        if serialized_bytes > max_serialized_bytes:
            stream.close()
            raise RuntimeError(
                "Query result exceeds SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES"
            )
        try:
            for batch in stream:
                # Convert one row at a time. ``RecordBatch.to_pylist()`` would
                # allocate a second copy of the complete batch before the byte
                # guard gets a chance to stop consumption.
                for row_index in range(batch.num_rows):
                    row = [
                        _json_safe_result_value(
                            batch.column(column_index)[row_index].as_py()
                        )
                        for column_index in range(batch.num_columns)
                    ]
                    try:
                        encoded = json.dumps(
                            row,
                            ensure_ascii=False,
                            allow_nan=False,
                            separators=(",", ":"),
                        ).encode("utf-8")
                    except (TypeError, ValueError) as exc:
                        raise RuntimeError(
                            "Query result is not JSON serializable"
                        ) from exc
                    serialized_bytes += len(encoded) + (1 if rows else 0)
                    if serialized_bytes > max_serialized_bytes:
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
                    rows.append(row)
        finally:
            stream.close()
        return columns, rows, columns_meta

    # Convert DataFrame to the expected format
    result_df = result_value
    columns = list(result_df.columns)
    columns_meta = [
        {
            "name": col,
            "type": str(result_df[col].dtype),
            "nullable": True
        }
        for col in columns
    ]

    # Sanitize pandas NA variants (pd.NA, pd.NaT, np.nan) to Python None
    # so downstream JSON serialization does not choke on NAType.
    # Note: DataFrame.where() + .values.tolist() does NOT fully sanitize
    # nullable dtypes (Int64, string) or np.nan in float columns.
    # We must sanitize the final Python objects after .tolist().
    rows = []
    serialized_bytes = len(json.dumps(
        {"columns": columns, "rows": [], "columns_meta": columns_meta},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8"))
    if serialized_bytes > max_serialized_bytes:
        raise RuntimeError(
            "Query result exceeds SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES"
        )
    for raw_row in result_df.itertuples(index=False, name=None):
        row = [_json_safe_result_value(value) for value in raw_row]
        try:
            encoded = json.dumps(
                row,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Query result is not JSON serializable") from exc
        serialized_bytes += len(encoded) + (1 if rows else 0)
        if serialized_bytes > max_serialized_bytes:
            raise RuntimeError(
                "Query result exceeds SUPERTABLE_MAX_SERIALIZED_RESULT_BYTES"
            )
        rows.append(row)

    return columns, rows, columns_meta
