# supertable/execute_spark.py

from __future__ import annotations

from typing import List

import pandas as pd

from supertable.query_plan_manager import QueryPlanManager
from supertable.utils.sql_parser import SQLParser


class SparkExecutor:
    """
    Executes the provided SQL against the list of Parquet files using PySpark.
    """

    def __init__(self):
        try:
            from pyspark.sql import SparkSession  # noqa: F401
        except Exception as e:
            raise RuntimeError(
                "PySpark is required for SPARK engine but is not available. "
                "Install pyspark or choose DUCKDB/AUTO."
            ) from e

    def _get_spark(self):
        from pyspark.sql import SparkSession
        return (
            SparkSession.builder
            .appName("supertable-query")
            .config("spark.sql.caseSensitive", "false")
            .getOrCreate()
        )

    def execute(
        self,
        selected: List[str],
        parser: SQLParser,
        query_manager: QueryPlanManager,
        timer_capture,
        log_prefix: str = "",
    ) -> pd.DataFrame:
        spark = self._get_spark()
        try:
            timer_capture("CONNECTING")
            df = spark.read.option("mergeSchema", "true").parquet(*parquet_files)
            df.createOrReplaceTempView(parser.reflection_table)

            # Safe RBAC view creation
            view_sql = parser.view_definition.strip() if getattr(parser, "view_definition", None) else ""
            if not view_sql:
                view_sql = f"SELECT * FROM {parser.reflection_table}"
            spark.sql(f"CREATE OR REPLACE TEMP VIEW {parser.rbac_view} AS {view_sql}")

            timer_capture("CREATING_REFLECTION")
            return spark.sql(parser.executing_query).toPandas()
        finally:
            try:
                spark.catalog.clearCache()
            except Exception:
                pass
