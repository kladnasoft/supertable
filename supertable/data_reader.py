# route: supertable.data_reader

from __future__ import annotations

import math
import re
from enum import Enum
from typing import Optional, Tuple, Any, List, Dict

import pandas as pd

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
from supertable.data_classes import DedupViewDef, TombstoneDef, RbacViewDef
from supertable.redis_catalog import RedisCatalog


class Status(Enum):
    OK = "ok"
    ERROR = "error"


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

    def execute(
        self,
        role_name: str,
        with_scan: bool = False,
        engine: engine = engine.AUTO,
    ) -> Tuple[pd.DataFrame, Status, Optional[str]]:
        status = Status.ERROR
        message: Optional[str] = None
        self.timer = Timer()
        self.plan_stats = PlanStats()

        # Build parser with the correct dialect for the chosen engine.
        parser = SQLParser(
            super_name=self.super_name,
            query=self.query,
            dialect=engine.dialect,
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
            return pd.DataFrame(), Status.ERROR, str(e)

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

            # 1) ESTIMATE — use physical_tables so CTE aliases are excluded
            estimator = DataEstimator(
                organization=self.organization,
                storage=self.storage,
                tables=physical_tables
            )
            reflection = estimator.estimate()

            logger.info(self._lp(f"[estimate] storage={reflection.storage_type} | files={reflection.total_reflections} | bytes={reflection.reflection_bytes}"))

            # Wire RBAC column/row filter definitions onto the reflection so
            # executors create filtered views for restricted roles.
            reflection.rbac_views = rbac_views

            # --- Dedup-on-read & tombstones: look up table configs and snapshot metadata ---
            try:
                catalog = RedisCatalog()
                for td in tables:
                    tbl_cfg = catalog.get_table_config(
                        self.organization, td.super_name, td.simple_name,
                    )
                    if tbl_cfg and tbl_cfg.get("dedup_on_read"):
                        pk = tbl_cfg.get("primary_keys", [])
                        if pk:
                            # visible_columns: what the user actually asked for.
                            # Empty list (SELECT *) means "all user columns" — the
                            # executor will expose everything except __rn__.
                            reflection.dedup_views[td.alias] = DedupViewDef(
                                primary_keys=pk,
                                order_column="__timestamp__",
                                visible_columns=list(td.columns or []),
                            )

                    # Tombstone filtering: read from snapshot payload in Redis leaf
                    payload = None
                    try:
                        leaf = catalog.get_leaf(
                            self.organization, td.super_name, td.simple_name,
                        )
                        payload = (leaf or {}).get("payload") if isinstance(leaf, dict) else None
                        if isinstance(payload, dict):
                            tomb_block = payload.get("tombstones")
                            if isinstance(tomb_block, dict):
                                tomb_keys = tomb_block.get("deleted_keys") or []
                                tomb_pk = tomb_block.get("primary_keys") or []
                                if tomb_keys and tomb_pk:
                                    reflection.tombstone_views[td.alias] = TombstoneDef(
                                        primary_keys=tomb_pk,
                                        deleted_keys=tomb_keys,
                                    )
                    except Exception as te:
                        logger.debug(self._lp(f"[tombstone] leaf lookup failed for {td.alias}: {te}"))

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
                        logger.debug(self._lp(f"[share-filter] row filter injection failed for {td.alias}: {rf_err}"))

            except Exception as e:
                logger.warning(self._lp(f"[dedup] config lookup failed, skipping dedup: {e}"))

            if not reflection.supers:
                message = "No parquet files found"
                return pd.DataFrame(), status, message

            # 2) EXECUTE
            result_df, engine_used = executor.execute(
                engine=engine,
                reflection=reflection,
                parser=parser,
                query_manager=self.query_plan_manager,
                timer=self.timer,
                plan_stats=self.plan_stats,
                log_prefix=self._lp(""),
            )
            status = Status.OK
        except Exception as e:
            message = str(e)
            logger.error(self._lp(f"Exception: {e}"))
            result_df = pd.DataFrame()

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
    # overwhelm the MCP response payload.
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

    # Convert DataFrame to the expected format
    columns = list(result_df.columns)

    # Sanitize pandas NA variants (pd.NA, pd.NaT, np.nan) to Python None
    # so downstream JSON serialization does not choke on NAType.
    # Note: DataFrame.where() + .values.tolist() does NOT fully sanitize
    # nullable dtypes (Int64, string) or np.nan in float columns.
    # We must sanitize the final Python objects after .tolist().
    rows = result_df.values.tolist()
    for row in rows:
        for i, val in enumerate(row):
            if val is pd.NA or val is pd.NaT:
                row[i] = None
            elif isinstance(val, float) and math.isnan(val):
                row[i] = None

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