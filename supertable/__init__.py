"""SuperTable — versioned data lake library for SQL analytics.

SuperTable stores structured data as versioned Parquet snapshots on object
storage (S3, MinIO, Azure Blob, GCP Cloud Storage, or local disk), keeps
metadata and locks in Redis, and queries everything through DuckDB or
IslandDB, with Spark SQL for fleet-scale workloads.

Quick reference
---------------

>>> from supertable import SuperTable, DataWriter, DataReader, engine
>>> st = SuperTable(super_name="example", organization="my-org")
>>> dw = DataWriter(super_name="example", organization="my-org")
>>> _, _, inserted, deleted = dw.write(
...     role_name="superadmin",
...     simple_name="facts",
...     data=arrow_table,
...     overwrite_columns=["day"],
... )
>>> dr = DataReader(super_name="example", organization="my-org",
...                 query="SELECT count(*) FROM facts")
>>> df, status, message = dr.execute(role_name="superadmin", engine=engine.AUTO)

See the ``supertable.demo`` package for runnable end-to-end demos and the
project documentation for the full API surface.
"""

__version__ = "2.5.3"

from importlib import import_module
from typing import TYPE_CHECKING, Any

# This public alias predates the ``supertable.engine`` implementation package.
# Importing the tiny enum after the now-lazy package is initialized ensures
# later imports of engine submodules cannot replace ``supertable.engine`` with
# the package object and break ``engine.AUTO``.
from supertable.engine.engine_enum import Engine as engine


# Importing the package is intentionally cheap and side-effect-free.  The
# public convenience exports are loaded on first use (PEP 562) so importing
# ``supertable`` does not import PyArrow/Polars, connect to Redis, probe the
# filesystem, or initialise an application home.
_LAZY_EXPORTS = {
    "SuperTable": ("supertable.super_table", "SuperTable"),
    "SimpleTable": ("supertable.simple_table", "SimpleTable"),
    "DataWriter": ("supertable.data_writer", "DataWriter"),
    "DataReader": ("supertable.data_reader", "DataReader"),
    "query_sql_stream": ("supertable.data_reader", "query_sql_stream"),
    "query_odata_sql_stream": (
        "supertable.data_reader", "query_odata_sql_stream",
    ),
    "query_sql_policy_fingerprint": (
        "supertable.data_reader", "query_sql_policy_fingerprint",
    ),
    "MetaReader": ("supertable.meta_reader", "MetaReader"),
    "list_supers": ("supertable.meta_reader", "list_supers"),
    "list_tables": ("supertable.meta_reader", "list_tables"),
    "Staging": ("supertable.staging_area", "Staging"),
    "SuperPipe": ("supertable.super_pipe", "SuperPipe"),
    "RedisCatalog": ("supertable.redis_catalog", "RedisCatalog"),
    "RoleManager": ("supertable.rbac.role_manager", "RoleManager"),
    "UserManager": ("supertable.rbac.user_manager", "UserManager"),
    "SupertableLookupError": ("supertable.errors", "SupertableLookupError"),
    "SuperTableNotFoundError": ("supertable.errors", "SuperTableNotFoundError"),
    "TableNotFoundError": ("supertable.errors", "TableNotFoundError"),
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
    from supertable.data_reader import (
        DataReader,
        query_odata_sql_stream,
        query_sql_policy_fingerprint,
        query_sql_stream,
    )
    from supertable.data_writer import DataWriter
    from supertable.errors import (
        SuperTableNotFoundError,
        SupertableLookupError,
        TableNotFoundError,
    )
    from supertable.meta_reader import MetaReader, list_supers, list_tables
    from supertable.rbac.role_manager import RoleManager
    from supertable.rbac.user_manager import UserManager
    from supertable.redis_catalog import RedisCatalog
    from supertable.simple_table import SimpleTable
    from supertable.staging_area import Staging
    from supertable.super_pipe import SuperPipe
    from supertable.super_table import SuperTable

__all__ = [
    "__version__",
    "SuperTable",
    "SimpleTable",
    "DataWriter",
    "DataReader",
    "query_sql_stream",
    "query_odata_sql_stream",
    "query_sql_policy_fingerprint",
    "engine",
    "MetaReader",
    "list_supers",
    "list_tables",
    "Staging",
    "SuperPipe",
    "RedisCatalog",
    "RoleManager",
    "UserManager",
    "SupertableLookupError",
    "SuperTableNotFoundError",
    "TableNotFoundError",
]
