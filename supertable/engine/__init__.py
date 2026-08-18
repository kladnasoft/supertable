"""Lazy query-engine package exports.

Keeping this initializer lightweight lets the top-level package expose the
historical ``engine`` enum alias without importing Arrow, Polars, DuckDB, Redis,
or the application configuration graph.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any


_LAZY_EXPORTS = {
    "Engine": ("supertable.engine.engine_enum", "Engine"),
    "Executor": ("supertable.engine.executor", "Executor"),
    "PlanStats": ("supertable.engine.plan_stats", "PlanStats"),
    "DataEstimator": ("supertable.engine.data_estimator", "DataEstimator"),
    "DuckDB": ("supertable.engine.duckdb_engine", "DuckDB"),
    "IslandDB": ("supertable.engine.islanddb", "IslandDB"),
    "ArrowBatchStream": (
        "supertable.engine.island_resources",
        "ArrowBatchStream",
    ),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute = target
    value = getattr(import_module(module_name), attribute)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_LAZY_EXPORTS))


if TYPE_CHECKING:
    from supertable.engine.data_estimator import DataEstimator
    from supertable.engine.duckdb_engine import DuckDB
    from supertable.engine.engine_enum import Engine
    from supertable.engine.executor import Executor
    from supertable.engine.island_resources import ArrowBatchStream
    from supertable.engine.islanddb import IslandDB
    from supertable.engine.plan_stats import PlanStats

__all__ = [
    "Engine",
    "Executor",
    "PlanStats",
    "DataEstimator",
    "DuckDB",
    "IslandDB",
    "ArrowBatchStream",
]
