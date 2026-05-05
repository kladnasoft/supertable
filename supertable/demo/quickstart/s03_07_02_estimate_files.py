"""Pre-flight estimate exposing the per-SuperTable file breakdown."""
from supertable.engine.data_estimator import DataEstimator
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.utils.sql_parser import SQLParser

from supertable.demo.quickstart.defaults import super_name, simple_name, organization


query = f"select * from {simple_name} limit 10"

storage: StorageInterface = get_storage()
parser = SQLParser(super_name=super_name, query=query)
tables = parser.get_table_tuples()

estimator = DataEstimator(
    organization=organization,
    storage=storage,
    tables=tables,
)
reflection = estimator.estimate()

print("-" * 60)
print(f"supers: {reflection.supers}")
print("-" * 60)
