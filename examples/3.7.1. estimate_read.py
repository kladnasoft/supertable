
from supertable.data_estimate_read import estimate_data_read
from examples.defaults import super_name, user_hash, simple_name, organization

query = f"select * as cnt from {simple_name} where 1=1 limit 10"

result = estimate_data_read(super_name=super_name, organization=organization, query=query, user_hash=user_hash, as_string=True)

print("-" * 52)
print(result)
print("-" * 52)
