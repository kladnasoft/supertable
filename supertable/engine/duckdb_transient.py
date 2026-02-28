# supertable/engine/duckdb_transient.py

from __future__ import annotations

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
    rewrite_query_with_hashed_tables,
    init_connection,
    create_rbac_view,
    rbac_view_name,
)


class DuckDBTransient:
    """
    Per-query DuckDB executor.

    Creates a fresh connection for each query, materializes reflection tables,
    executes the query, and closes the connection. No state persists between calls.
    """

    def __init__(self, storage: Optional[object] = None):
        self.storage = storage

    def execute(
            self,
            reflection: Reflection,
            parser: SQLParser,
            query_manager: QueryPlanManager,
            timer_capture,
            log_prefix: str = "",
    ) -> pd.DataFrame:
        con = duckdb.connect()
        tried_presign = False

        try:
            timer_capture("CONNECTING")

            init_connection(
                con,
                temp_dir=query_manager.temp_dir,
                profile_path=query_manager.query_plan_path,
            )

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

                name = hashed_table_name(
                    sup.super_name, sup.simple_name, sup.simple_version, td.columns,
                )
                alias_to_table_name[td.alias] = name
                alias_to_files[td.alias] = list(sup.files)
                alias_to_columns[td.alias] = list(td.columns or [])

            for alias, table_name in alias_to_table_name.items():
                files = alias_to_files[alias]
                cols = alias_to_columns[alias]

                used_presign = create_reflection_table_with_presign_retry(
                    con, self.storage, table_name, files, cols, log_prefix,
                )
                if used_presign:
                    tried_presign = True

            timer_capture("CREATING_REFLECTION")

            # Create RBAC views if filtering is required
            rbac_views = getattr(reflection, "rbac_views", None) or {}
            if rbac_views:
                for alias, table_name in alias_to_table_name.items():
                    view_def = rbac_views.get(alias)
                    if view_def:
                        view = rbac_view_name(table_name)
                        create_rbac_view(con, table_name, view, view_def)
                        alias_to_table_name[alias] = view

            executing_query = rewrite_query_with_hashed_tables(
                parser.original_query, alias_to_table_name,
            )
            parser.executing_query = executing_query

            logger.debug(f"{log_prefix}[duckdb.transient] executing: {executing_query}")
            result = con.execute(executing_query).fetchdf()

            if tried_presign:
                logger.debug(f"{log_prefix}[duckdb.transient] presigned fallback succeeded")

            return result

        finally:
            try:
                con.close()
            except Exception:
                pass