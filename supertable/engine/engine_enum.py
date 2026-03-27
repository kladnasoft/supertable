# route: supertable.engine.engine_enum

from enum import Enum


class Engine(Enum):
    AUTO = "auto"
    DUCKDB_LITE = "duckdb_lite"
    DUCKDB_PRO = "duckdb_pro"
    SPARK_SQL = "spark_sql"

    @property
    def dialect(self) -> str:
        """Return the sqlglot dialect string for this engine.

        Used by SQLParser to select the correct grammar.  AUTO defaults
        to ``"duckdb"`` because that is the most common resolution path
        and DuckDB's grammar is a superset of what Spark SQL accepts
        for the standard SELECT/JOIN/GROUP BY patterns used in queries.
        """
        if self == Engine.SPARK_SQL:
            return "spark"
        return "duckdb"
