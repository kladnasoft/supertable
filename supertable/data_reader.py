# route: supertable.data_reader

from __future__ import annotations

import math
import re
from enum import Enum
from typing import Optional, Tuple, Any, List, Dict

import polars as pl

from supertable.config.defaults import logger
from supertable.errors import SuperTableNotFoundError, TableNotFoundError
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.utils.timer import Timer
from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser
from supertable.plan_extender import extend_execution_plan
from supertable.engine.plan_stats import PlanStats
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


from collections import defaultdict
from typing import List, Tuple



class ReadAccessUnavailable(RuntimeError):
    """A control the read depends on could not be established.

    Raised instead of returning rows when the deletion vector or a share row
    filter cannot be resolved. Both are enforced by views the reader builds, so
    failing to build one does not degrade the result — it removes the control
    entirely, and the rows it would have hidden are exactly the rows nobody is
    supposed to see.

    A subclass per control so a caller can tell "stale deletes may be visible"
    from "another tenant's rows may be visible"; the second is the more urgent
    page.
    """


class DeletionVectorUnavailable(ReadAccessUnavailable):
    """The deletion vector could not be read; deleted rows would reappear."""


class ShareFilterUnavailable(ReadAccessUnavailable):
    """A share row filter could not be read; the full table would be served."""


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
    ):
        self.super_name = super_name
        self.organization = organization
        self.query = query
        # Query origin surfaced in the reads monitoring tab. "sdk" is the
        # default for direct SDK callers; the API/OData/MCP entry points
        # pass "api"/"odata"/"mcp" so each query records where it came from.
        self.source = source

        self.storage: StorageInterface = get_storage()

        self.timer: Optional[Timer] = None
        self.plan_stats: Optional[PlanStats] = None
        self.query_plan_manager: Optional[QueryPlanManager] = None

        self._log_ctx = ""
        # Set by stream(); when present, execute() hands back an open Arrow
        # reader through it instead of a materialised frame.
        self._stream_out: Optional[Dict[str, Any]] = None

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
            if not catalog.leaf_exists(self.organization, super_name, simple_name):
                raise TableNotFoundError(
                    self.organization, super_name, simple_name
                )

    def _resolve_latest_stats_file(
        self, super_name: str, simple_name: str,
    ) -> Optional[str]:
        """Return the ``stats_file`` pointer of the table's latest snapshot.

        Prefers the leaf payload (already in Redis); falls back to reading the
        snapshot JSON from storage. ``None`` when the table has no stats artifact
        yet (never written, or written before stats existed).
        """
        catalog = RedisCatalog()
        leaf = catalog.get_leaf(self.organization, super_name, simple_name)
        if not isinstance(leaf, dict):
            return None
        payload = leaf.get("payload")
        if isinstance(payload, dict) and payload.get("stats_file"):
            return payload["stats_file"]
        path = leaf.get("path")
        if not path:
            return None
        from supertable.super_table import SuperTable
        snapshot = SuperTable(
            super_name, self.organization, create_if_missing=False,
        ).read_simple_table_snapshot(path)
        return snapshot.get("stats_file") if isinstance(snapshot, dict) else None

    def _execute_show_stats(
        self, command, role_name: str,
    ) -> Tuple[pl.DataFrame, Status, Optional[str]]:
        """Return the raw contents of a table's latest statistics parquet.

        Reads-never-create and table-level RBAC are enforced (the same gates a
        SELECT hits); the statistics rows/columns themselves are returned
        unfiltered. When the table exists but has no stats artifact yet, an empty
        frame with the stats schema columns is returned (success, not error).
        """
        from supertable.data_classes import TableDefinition
        from supertable.processing import load_stats, STATS_SCHEMA

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
            return pl.DataFrame(), Status.ERROR, str(e)

        # Table-level RBAC: raises PermissionError if the role cannot read the
        # table at all. columns=[] means "all columns", which skips column-level
        # denial — we don't filter the stats output, only gate table access.
        restrict_read_access(
            super_name=super_name,
            organization=self.organization,
            role_name=role_name,
            tables=[td],
            physical_tables=[td],
        )

        try:
            stats_file = self._resolve_latest_stats_file(super_name, simple_name)
            stats_df = load_stats(stats_file, allow_cache=True) if stats_file else None
        except Exception as e:
            logger.error(self._lp(f"[show-stats] failed to load stats: {e}"))
            return pl.DataFrame(), Status.ERROR, str(e)

        if stats_df is None:
            return pl.DataFrame(schema={k: pl.Utf8 for k in STATS_SCHEMA}), Status.OK, None
        return stats_df, Status.OK, None

    def execute(
        self,
        role_name: str,
        with_scan: bool = False,
        engine: engine = engine.AUTO,
        fullscan: bool = False,
    ) -> Tuple[pl.DataFrame, Status, Optional[str]]:
        """Run the query.

        *fullscan* disables read-path file pruning, so every file in the
        snapshot is scanned. Pruning may only ever drop files that provably
        hold no matching row, which means a fullscan result and a pruned
        result must be identical — this switch exists so a test can assert
        that equality rather than assume it. It is a correctness escape
        hatch, not a performance knob.
        """
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
            return pl.DataFrame(), Status.ERROR, str(e)

        # SHOW STATS short-circuits the engine entirely — it returns the raw
        # statistics artifact, no reflection/estimation/execution.
        if command.kind is CommandKind.SHOW_STATS:
            return self._execute_show_stats(command, role_name)

        # Build parser with the correct dialect for the chosen engine. For
        # EXPLAIN, parse only the inner SELECT so estimation/RBAC/reflection
        # behave exactly as for the equivalent plain SELECT.
        # Reuse the AST admission already built, but ONLY for the dialect it
        # was parsed with. Admission always parses as duckdb; handing that to a
        # spark-dialect parser would silently reinterpret the query.
        parser = SQLParser(
            super_name=self.super_name,
            query=command.sql,
            dialect=engine.dialect,
            parsed=(command.parsed
                    if getattr(engine, "dialect", None) == "duckdb" else None),
        )
        tables = parser.get_table_tuples()
        physical_tables = parser.get_physical_tables()

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
            return pl.DataFrame(), Status.ERROR, str(e)

        # RBAC check — also returns per-alias column/row filter definitions.
        # PermissionError propagates to the caller (legacy behaviour).
        rbac_views = restrict_read_access(
            super_name=self.super_name,
            organization=self.organization,
            role_name=role_name,
            tables=tables,
            physical_tables=physical_tables,
        )

        try:
            # Make executor aware of storage for presign retry
            executor = Executor(storage=self.storage, organization=self.organization)

            # Initialize plan manager and query id/hash (same as before)
            self.query_plan_manager = QueryPlanManager(
                super_name=self.super_name,
                organization=self.organization,
                current_meta_path="redis://meta/root",
                query=parser.original_query,
            )
            # Stamp the call origin so plan_extender records it on the read
            # monitoring entry (defaults to "api" downstream if unset).
            self.query_plan_manager.source_type = self.source
            self._log_ctx = f"[qid={self.query_plan_manager.query_id} qh={self.query_plan_manager.query_hash}] "
            self.query_plan_manager.original_table = ", ".join(t.simple_name for t in physical_tables) if physical_tables else ""

            # Derive per-table WHERE constraints so the estimator can prune
            # files via the stats artifact.  Never let this break a read.
            try:
                predicate_constraints = parser.get_predicate_constraints()
            except Exception as pc_err:
                logger.debug(self._lp(f"[prune] predicate extraction failed: {pc_err}"))
                predicate_constraints = {}

            # 1) ESTIMATE — use physical_tables so CTE aliases are excluded
            estimator = DataEstimator(
                organization=self.organization,
                storage=self.storage,
                tables=physical_tables,
                predicate_constraints=predicate_constraints,
                plan_stats=self.plan_stats,
                fullscan=fullscan,
            )
            reflection = estimator.estimate()

            logger.info(self._lp(f"[estimate] storage={reflection.storage_type} | files={reflection.total_reflections} | bytes={reflection.reflection_bytes}"))

            # Wire RBAC column/row filter definitions onto the reflection so
            # executors create filtered views for restricted roles.
            reflection.rbac_views = rbac_views

            # --- Tombstone (deletion-vector): look up snapshot metadata ------
            try:
                catalog = RedisCatalog()
                for td in tables:
                    # Tombstone filtering: read the deletion-vector pointer from
                    # the snapshot payload in the Redis leaf.  When present, the
                    # executor anti-joins the data on __rowid__ against it.
                    payload = None
                    try:
                        leaf = catalog.get_leaf(
                            self.organization, td.super_name, td.simple_name,
                        )
                        payload = (leaf or {}).get("payload") if isinstance(leaf, dict) else None
                        if isinstance(payload, dict):
                            tomb_path = payload.get("tombstone")
                            if tomb_path:
                                # Resolve the deletion-vector key exactly like the
                                # data files (estimator._to_duckdb_path, see
                                # data_estimator.py:426): the catalog stores a bare
                                # object key, which DuckDB/Spark cannot read against
                                # an object store and must be presigned. LOCAL
                                # storage returns the key unchanged.
                                # The vector is a list of parts (older
                                # snapshots hold one string).  EVERY part must
                                # reach the reader — dropping one resurrects
                                # exactly the rows it recorded.
                                _parts = ([tomb_path] if isinstance(tomb_path, str)
                                          else [x for x in (tomb_path or []) if x])
                                reflection.tombstone_views[td.alias] = TombstoneDef(
                                    tombstone_path=[
                                        estimator._to_duckdb_path(x) for x in _parts
                                    ],
                                    # Bare key (pre-presign) is stable across
                                    # appends → safe deletion-vector cache key.
                                    cache_key=str(tomb_path),
                                )
                    except Exception as te:
                        # FAIL THE READ. This lookup establishes the deletion
                        # vector; without it the executor has nothing to
                        # anti-join and every deleted row comes back. Swallowing
                        # it returned resurrected rows under Status.OK with a
                        # DEBUG line nobody reads — a Redis hiccup silently
                        # undid every delete the table had ever recorded.
                        #
                        # "Could not determine the deletion vector" is not
                        # "there is no deletion vector".
                        raise DeletionVectorUnavailable(
                            f"cannot establish the deletion vector for "
                            f"{td.super_name}.{td.simple_name}: {te}"
                        ) from te

                    # Linked-share row filter: the provider may have set a
                    # row_filter on the share.  Inject it as a synthetic RBAC
                    # WHERE clause so the executor enforces it automatically.
                    try:
                        if isinstance(payload, dict):
                            share_row_filter = payload.get("_row_filter")
                            if share_row_filter and isinstance(share_row_filter, str):
                                existing_rbac = reflection.rbac_views.get(td.alias)
                                if existing_rbac:
                                    # Merge: AND the share filter with existing RBAC filter
                                    if existing_rbac.where_clause:
                                        existing_rbac.where_clause = f"({existing_rbac.where_clause}) AND ({share_row_filter})"
                                    else:
                                        existing_rbac.where_clause = share_row_filter
                                else:
                                    reflection.rbac_views[td.alias] = RbacViewDef(
                                        allowed_columns=["*"],
                                        where_clause=share_row_filter,
                                    )
                    except Exception as rf_err:
                        # FAIL THE READ, for the same reason and more sharply:
                        # a share row filter is what keeps one tenant's rows out
                        # of another's result. Dropping it on an exception
                        # served the full table. odata/policy.py calls this
                        # "the one direction this must never fail in" — the
                        # fingerprint obeyed that; this did not.
                        raise ShareFilterUnavailable(
                            f"cannot establish the share row filter for "
                            f"{td.super_name}.{td.simple_name}: {rf_err}"
                        ) from rf_err

            except ReadAccessUnavailable:
                # A control could not be established. The two handlers inside
                # this block raise deliberately; catching them here would
                # restore exactly the fail-open behaviour they exist to remove
                # — the reason H2 needed THREE handlers fixed, not two.
                raise
            except Exception as e:
                # Everything else here is genuinely optional (dedup config),
                # so degrading is correct: it changes which rows are shown
                # only in ways the caller asked for, not which rows are
                # ALLOWED to be shown.
                logger.warning(self._lp(f"[dedup] config lookup failed, skipping dedup: {e}"))

            if not reflection.supers:
                message = "No parquet files found"
                return pl.DataFrame(), status, message

            # 2) EXECUTE.  EXPLAIN is pinned to DuckDB-lite so the plan is
            # produced cheaply and uniformly (no Pro materialisation / Spark
            # round trip) and prefixed onto the final rewritten query.
            exec_engine = engine
            if command.explain:
                from supertable.engine.engine_enum import Engine as _EngineEnum
                exec_engine = _EngineEnum.DUCKDB
            if self._stream_out is not None:
                # Streaming shares every step above — RBAC, dedup, share
                # filters, pruning — and diverges only at the final fetch.
                # Anything else would make a streamed read a different query
                # from a buffered one.
                self._stream_out["handle"] = executor.stream(
                    reflection=reflection,
                    parser=parser,
                    query_manager=self.query_plan_manager,
                    timer=self.timer,
                    log_prefix=self._lp(""),
                    engine=exec_engine,
                    batch_rows=self._stream_out.get("batch_rows", 0),
                    expose_rowid=self._stream_out.get("expose_rowid", False),
                )
                self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")

                # Monitoring for a streamed read is written when the stream
                # CLOSES, not here: the row count does not exist yet. Without
                # this a streaming query would be invisible to monitoring —
                # exactly the long-running read you most want to see.
                handle = self._stream_out["handle"]
                qpm, timer, stats = (self.query_plan_manager, self.timer,
                                     self.plan_stats)
                lp = self._lp

                def _record(rows: int, cols: int) -> None:
                    try:
                        extend_execution_plan(
                            query_plan_manager=qpm,
                            role_name=role_name,
                            timing=timer.timings,
                            plan_stats=stats,
                            status=str(Status.OK.value),
                            message="streamed",
                            result_shape=(rows, cols),
                        )
                    except Exception as e:
                        logger.error(lp(f"extend_execution_plan (stream): {e}"))

                if handle is not None:
                    handle.on_close = _record
                return pl.DataFrame(), Status.OK, ""
            result_df, engine_used = executor.execute(
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
            status = Status.OK
        except Exception as e:
            message = str(e)
            logger.error(self._lp(f"Exception: {e}"))
            result_df = pl.DataFrame()

        # Extend plan + timings
        self.timer.capture_and_reset_timing(event="EXECUTING_QUERY")
        try:
            extend_execution_plan(
                query_plan_manager=self.query_plan_manager,
                role_name=role_name,
                timing=self.timer.timings,
                plan_stats=self.plan_stats,
                status=str(status.value),
                message=message,
                result_shape=result_df.shape,
            )
        except Exception as e:
            logger.error(self._lp(f"extend_execution_plan exception: {e}"))

        self.timer.capture_and_reset_timing(event="EXTENDING_PLAN")
        self.timer.capture_duration(event="TOTAL_EXECUTE")
        return result_df, status, message

    def stream(
            self,
            role_name: str,
            engine: Any = None,
            fullscan: bool = False,
            batch_rows: int = 0,
            expose_rowid: bool = False,
    ):
        """Run the query and return an open Arrow stream.

        No LIMIT is applied, ever. ``execute`` materialises through
        ``fetchdf()``, which is why callers needed one; a stream hands back
        batches as DuckDB produces them, so ``SELECT *`` over a large table is
        bounded by the consumer's appetite rather than by memory.

        The result is a ``StreamHandle``: iterate ``.batches()``, and close it
        (or use it as a context manager). It holds the per-query views open, so
        leaking it leaks catalog objects on the shared connection.

        Every step before the fetch is identical to ``execute`` — RBAC, the
        deletion-vector anti-join, dedup, share filters, pruning — so a
        streamed read returns exactly what a buffered read would.
        """
        from supertable.engine.engine_enum import Engine as _Engine

        self._stream_out = {"handle": None, "batch_rows": batch_rows,
                            "expose_rowid": expose_rowid}
        try:
            _, status, message = self.execute(
                role_name=role_name,
                engine=engine if engine is not None else _Engine.AUTO,
                with_scan=False,
                fullscan=fullscan,
            )
            if not str(status).endswith("OK"):
                raise RuntimeError(f"stream failed: {message}")
            handle = self._stream_out.get("handle")
            if handle is None:
                raise RuntimeError(f"stream produced no reader: {message}")
            return handle
        finally:
            self._stream_out = None


def _ensure_sql_limit(sql: str, default_limit: int) -> str:
    """
    If the outermost query has no LIMIT clause, append one.

    Only appends when the SQL does not already end with a LIMIT (ignoring
    trailing whitespace/semicolons).  This avoids breaking queries that
    already specify their own LIMIT, subqueries that contain LIMIT internally,
    or CTEs.
    """
    # Strip trailing whitespace and optional semicolons for inspection
    stripped = sql.rstrip().rstrip(";").rstrip()

    # Check if the query already ends with LIMIT <number> (possibly with OFFSET)
    # Pattern: LIMIT <digits> [OFFSET <digits>] at the very end
    if re.search(r'\bLIMIT\s+\d+\s*(?:OFFSET\s+\d+\s*)?$', stripped, re.IGNORECASE):
        return sql

    return f"{sql}\nLIMIT {int(default_limit)}"


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

    # Execute the query
    result_df, status, message = reader.execute(
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

    # Convert the frame to the expected format.
    columns = list(result_df.columns)

    # polars.rows() yields Python tuples with real None for nulls, so the
    # pandas NA sanitisation that used to live here is gone: pd.NA, pd.NaT and
    # np.nan only ever appeared because pandas cannot represent a null inside a
    # numeric column. `.values.tolist()` also forced every row through a shared
    # numpy dtype, which upcast integers to float for exactly the same reason.
    # polars keeps NaN and null distinct, where pandas conflated them. Nulls
    # already come out as None, but a float NaN would survive into the payload
    # and `NaN` is not valid JSON — so it is folded into null here, which is
    # what the old pandas sanitisation did for a different reason.
    try:
        result_df = result_df.fill_nan(None)
    except Exception:
        pass                                # no float columns to fill
    # map() rather than a comprehension: same rows, ~14% less time on a
    # 96k-row result, because the per-row list() call is dispatched in C
    # instead of through a Python loop.
    #
    # Two faster-looking options were measured and REJECTED:
    #
    #   df.rows() alone is 2.04x faster, but returns tuples and this function
    #   is documented to return List[List[Any]]. Breaking that for external
    #   callers is not worth 180ms.
    #
    #   df.to_numpy().tolist() is 1.76x faster and CORRUPTS DATA. numpy widens
    #   an integer column containing nulls to float64, so 9007199254740993
    #   comes back as 9007199254740992.0 and None comes back as nan — the same
    #   >2^53 coercion the pruner was just fixed for, and the null/NaN
    #   distinction this function deliberately preserves. It looks identical on
    #   a frame with no nulls and no large integers, which is how it passes a
    #   naive check.
    rows = list(map(list, result_df.rows()))

    # Create basic column metadata
    columns_meta = [
        {
            "name": col,
            "type": str(result_df[col].dtype),
            "nullable": True
        }
        for col in columns
    ]

    return columns, rows, columns_meta