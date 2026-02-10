from supertable.data_classes import Reflection
from supertable.data_estimator import DataEstimator
from examples.defaults import super_name, user_hash, simple_name, organization
from supertable.storage.storage_factory import get_storage
from supertable.storage.storage_interface import StorageInterface
from supertable.utils.sql_parser import SQLParser

query = f"select * as cnt from {simple_name} where 1=1 limit 10"

storage: StorageInterface = get_storage()
parser = SQLParser(super_name=super_name, query=query)
tables = parser.get_table_tuples()

estimator = DataEstimator(
    organization=organization,
    storage=storage,
    tables=tables
)
reflection = estimator.estimate()

print("-" * 52)
print(reflection.reflection_bytes)
print("-" * 52)
