# supertable/engine/__init__.py
#
# Query-engine subpackage.
# Groups execution engines (DuckDB transient & pinned, Spark Thrift),
# the engine router (Executor), data estimation, and plan statistics.

from supertable.engine.executor import Engine, Executor  # noqa: F401
from supertable.engine.plan_stats import PlanStats  # noqa: F401
from supertable.engine.data_estimator import DataEstimator  # noqa: F401

__all__ = [
    "Engine",
    "Executor",
    "PlanStats",
    "DataEstimator",
]