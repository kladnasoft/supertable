# route: supertable.engine.engine_enum

from enum import Enum


class Engine(Enum):
    """Engines a query can run on.

    There is ONE DuckDB engine. It previously came in a "lite" and a "pro"
    flavour, the latter materialising reflection tables and routing on data
    freshness; that split is gone, and DUCKDB behaves as lite always did.
    ``DUCKDB_LITE`` and ``DUCKDB_PRO`` remain as aliases so existing callers
    and stored engine preferences keep resolving — in Python both are simply
    other names for ``DUCKDB``.
    """

    AUTO = "auto"
    DUCKDB = "duckdb"
    SPARK_SQL = "spark_sql"

    # Back-compat aliases: Enum maps equal values onto the same member.
    DUCKDB_LITE = "duckdb"
    DUCKDB_PRO = "duckdb"

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
