# supertable/engine/__init__.py
#
# Query-engine subpackage.
# Groups execution engines (DuckDB, IslandDB, Spark Thrift),
# the engine router (Executor), data estimation, and plan statistics.

from supertable.engine.executor import Engine, Executor  # noqa: F401
from supertable.engine.plan_stats import PlanStats  # noqa: F401
from supertable.engine.data_estimator import DataEstimator  # noqa: F401
from supertable.engine.duckdb_engine import DuckDB  # noqa: F401
from supertable.engine.islanddb import IslandDB  # noqa: F401
from supertable.engine.island_resources import ArrowBatchStream  # noqa: F401

__all__ = [
    "Engine",
    "Executor",
    "PlanStats",
    "DataEstimator",
    "DuckDB",
    "IslandDB",
    "ArrowBatchStream",
]
