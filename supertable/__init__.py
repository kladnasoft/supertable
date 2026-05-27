"""SuperTable — versioned data lake library for SQL analytics.

SuperTable stores structured data as versioned Parquet snapshots on object
storage (S3, MinIO, Azure Blob, GCP Cloud Storage, or local disk), keeps
metadata and locks in Redis, and queries everything through DuckDB or
Spark SQL.

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

__version__ = "2.2.0"

# Re-export the core public surface so users can do ``from supertable import …``
# instead of remembering submodule paths.
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable
from supertable.data_writer import DataWriter
from supertable.data_reader import DataReader, engine
from supertable.meta_reader import MetaReader, list_supers, list_tables
from supertable.staging_area import Staging
from supertable.super_pipe import SuperPipe
from supertable.redis_catalog import RedisCatalog
from supertable.rbac.role_manager import RoleManager
from supertable.rbac.user_manager import UserManager

__all__ = [
    "__version__",
    "SuperTable",
    "SimpleTable",
    "DataWriter",
    "DataReader",
    "engine",
    "MetaReader",
    "list_supers",
    "list_tables",
    "Staging",
    "SuperPipe",
    "RedisCatalog",
    "RoleManager",
    "UserManager",
]
