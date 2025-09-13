from __future__ import annotations
from typing import Dict, Any, List, Set

from supertable.config.defaults import logger
from supertable.utils.timer import Timer
from supertable.plan_stats import PlanStats
from supertable.rbac.access_control import restrict_read_access

# Import from the provided module (must be alongside this file in your project)
from data_reader import DataReader, Status  # noqa: E402


def _lower_set(items) -> Set[str]:
    return {str(x).lower() for x in items}


def estimate_data_read(
    super_name: str,
    organization: str,
    query: str,
    user_hash: str,
    with_scan: bool = False,
    as_string: bool = True,
) -> str | Dict[str, Any]:
    """
    Compute estimated read stats (selected parquet count, total rows, total bytes)
    based on snapshot/resource selection. Does NOT run the DuckDB query.
    """
    reader = DataReader(super_name, organization, query)
    reader.timer = Timer()
    reader.plan_stats = PlanStats()

    # --- Load super table metadata -------------------------------------------
    super_table_data, super_table_path, super_table_meta = (
        reader.super_table.get_super_table_and_path_with_shared_lock()
    )
    reader.timer.capture_and_reset_timing(event="META")

    # --- Select snapshots & files --------------------------------------------
    snapshots = reader.filter_snapshots(
        super_table_data=super_table_data, super_table_meta=super_table_meta
    )

    parquet_files, schema = reader.process_snapshots(
        snapshots=snapshots, with_scan=with_scan
    )

    # Validate requested columns (same semantics as data_reader.execute)
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

    # RBAC parity with execute() (will raise if unauthorized)
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

    # --- Aggregate totals from SELECTED files only ----------------------------
    files_set = set(parquet_files)
    total_rows = 0
    total_size = 0
    for snapshot in snapshots:
        snap_data = reader.super_table.read_simple_table_snapshot(snapshot["path"])
        resources = snap_data.get("resources", []) or []
        for res in resources:
            if res.get("file") in files_set:
                try:
                    total_rows += int(res.get("rows", 0))
                except Exception:
                    pass
                try:
                    total_size += int(res.get("file_size", 0))
                except Exception:
                    pass

    result = {
        "selected_files": len(parquet_files),
        "total_rows": total_rows,
        "estimated_bytes": total_size,
    }

    if as_string:
        return (
            f"selected_files: {result['selected_files']} | "
            f"total_rows={result['total_rows']} | estimated_bytes={result['estimated_bytes']}"
        )
    return result
