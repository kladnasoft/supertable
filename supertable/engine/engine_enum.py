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
