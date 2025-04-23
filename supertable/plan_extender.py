import json
import logging
import uuid
import datetime

import polars as pl
from typing import Optional

from supertable.config.defaults import logger
from supertable.query_plan_manager import QueryPlanManager
from supertable.storage.storage_interface import StorageInterface
from supertable.storage.storage_factory import get_storage
from supertable.data_writer import DataWriter
from supertable.super_table import SuperTable
from supertable.rbac.user_manager import UserManager


class PlanStats:
    def __init__(self):
        self.stats = []

    def add_stat(self, stat):
        self.stats.append(stat)


def extend_execution_plan(
    super_table: SuperTable,
    user_hash: str,
    query_plan_manager: QueryPlanManager,
    timing,
    plan_stats: PlanStats,
    status: str,
    message: str,
    result_shape: tuple,
    storage: Optional[StorageInterface] = None,
):
    """
    Reads a JSON execution plan, extends it with the given timing and PlanStats,
    then writes it back using the provided storage interface (or default storage if none is passed).
    """
    simple_name = "__query_stats__"
    return

    if query_plan_manager.original_table == simple_name:
        return

    original_level = logger.getEffectiveLevel()
    logging.getLogger('supertable').setLevel(logging.INFO)

    # If no storage is passed, create one via the storage factory
    if storage is None:
        storage = get_storage()

    # Read the existing plan JSON from the storage
    try:
        current_plan = storage.read_json(query_plan_manager.query_plan_path)
        logger.debug(f"Plan: {current_plan}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warn(f"Warning: Could not read query plan - {str(e)}")
        current_plan = {}  # or whatever default value makes sense

    user_manager = UserManager(super_table.super_name, super_table.organization)
    user_data = user_manager.get_user_hash_by_name("superuser")
    superuser_hash = user_data.get('hash', user_hash)
    logger.debug(f"User Data:- {str(user_data)}")

    # Build the new extended plan
    new_plan = {
        "execution_timings": timing,
        "profile_overview": plan_stats.stats,
        "query_profile": current_plan,
    }

    df = pl.DataFrame({
            "query_id": query_plan_manager.query_id,
            "query_hash": query_plan_manager.query_hash,
            "recorded_at": [datetime.datetime.utcnow()],
            "user_hash": user_hash,
            "table_name": query_plan_manager.original_table,
            "status": status,
            "message": message,
            "result_rows": result_shape[0],
            "result_columns": result_shape[1],
            "execution_timings": [json.dumps(new_plan["execution_timings"])],
            "profile_overview": [json.dumps(new_plan["profile_overview"])],
            "query_profile": [json.dumps(new_plan["query_profile"])]
    })

    arrow_table = df.to_arrow()
    logger.debug(f"Data to Write: {arrow_table}")
    logger.debug(f"Writing data to {simple_name}")

    data_writer = DataWriter(super_name=super_table.super_name, organization=super_table.organization)

    columns, rows, inserted, deleted = data_writer.write(
        user_hash=superuser_hash,
        simple_name=simple_name,
        data=arrow_table,
        overwrite_columns=["query_id"],
    )

    logging.getLogger('supertable').setLevel(original_level)
    logger.debug(f"Query Plan Saved")
    storage.delete(query_plan_manager.query_plan_path)
