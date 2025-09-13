from __future__ import annotations

import argparse
import json
from typing import List, Dict, Any, Set

from supertable.config.defaults import logger
from supertable.utils.timer import Timer
from supertable.plan_stats import PlanStats
from supertable.rbac.access_control import restrict_read_access

from data_reader import DataReader  # provided by your project


def _lower_set(items) -> Set[str]:
    return {str(x).lower() for x in items}


def get_selected_parquet_files(
    super_name: str,
    organization: str,
    query: str,
    user_hash: str,
    with_scan: bool = False,
    as_string: bool = False,
) -> List[str] | str:
    """
    Determine selected parquet files for the query without running DuckDB.
    Ensures RBAC access is validated before returning the list.
    """
    reader = DataReader(super_name, organization, query)
    reader.timer = Timer()
    reader.plan_stats = PlanStats()

    # Load metadata
    super_table_data, super_table_path, super_table_meta = (
        reader.super_table.get_super_table_and_path_with_shared_lock()
    )
    reader.timer.capture_and_reset_timing(event="META")

    # Filter snapshots and select files
    snapshots = reader.filter_snapshots(
        super_table_data=super_table_data, super_table_meta=super_table_meta
    )
    parquet_files, schema = reader.process_snapshots(
        snapshots=snapshots, with_scan=with_scan
    )

    # Validate requested columns (same semantics as execute())
    missing_columns: Set[str] = set()
    if reader.parser.columns_csv != "*":
        requested = _lower_set(reader.parser.columns_list)
        missing_columns = requested - schema

    if len(snapshots) == 0 or missing_columns or not parquet_files:
        msg = (
            f"Missing column(s): {', '.join(sorted(missing_columns))}"
            if missing_columns
            else ("No parquet files found" if not parquet_files else "No snapshots found")
        )
        logger.warning(msg)
        raise RuntimeError(msg)

    # RBAC check before returning file list
    restrict_read_access(
        super_name=reader.super_table.super_name,
        organization=reader.super_table.organization,
        user_hash=user_hash,
        table_name=reader.parser.reflection_table,
        table_schema=schema,
        parsed_columns=reader.parser.columns_list,
        parser=reader.parser,
    )
    reader.timer.capture_and_reset_timing(event="FILTERING")

    if as_string:
        header = f"Processed Parquet Files: {len(parquet_files)}"
        # Join on newlines for readability
        body = "\n".join(parquet_files)
        return f"{header}\n{body}"

    return parquet_files
