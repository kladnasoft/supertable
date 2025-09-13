
from supertable.data_estimate_files import get_selected_parquet_files
from examples.defaults import super_name, user_hash, simple_name, organization

query = f"select * as cnt from {simple_name} where 1=1 limit 10"


files = get_selected_parquet_files(super_name=super_name, organization=organization, query=query, user_hash=user_hash)

print("-" * 52)
print(files)
print("-" * 52)
