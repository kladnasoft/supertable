# route: supertable.engine.engine_enum

from enum import Enum


class Engine(Enum):
    """Engines a query can run on.

    There is ONE DuckDB engine. It previously came in a "lite" and a "pro"
    flavour, the latter materialising reflection tables and routing on data
    freshness; that split is gone and DUCKDB behaves as lite always did.
    """

    AUTO = "auto"
    DUCKDB = "duckdb"
    SPARK_SQL = "spark_sql"

    # There is no ``dialect`` here any more. A query has one reading — DuckDB's,
    # the one admission judges — whatever engine runs it; the Spark executor
    # writes that reading out as Spark SQL. A per-engine parser dialect is how
    # the tables a query was authorized for came apart from the tables it ran
    # over (see DataReader.execute).
