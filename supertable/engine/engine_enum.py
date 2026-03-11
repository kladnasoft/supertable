# supertable/engine/engine_enum.py

from enum import Enum


class Engine(Enum):
    AUTO = "auto"
    DUCKDB_LITE = "duckdb_lite"
    DUCKDB_PRO = "duckdb_pro"
    SPARK_SQL = "spark_sql"
