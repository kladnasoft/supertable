from supertable.data_reader import DataReader
from examples.defaults import super_name, user_hash, simple_name, organization, role_name

query = f"select * as cnt from {super_name} where 1=1 limit 10"

dr = DataReader(super_name=super_name, organization=organization, query=query)
result = dr.execute(role_name=role_name, with_scan=False)
print("-" * 52)
print("Rows: ", result[0].shape[0], ", Columns: ", result[0].shape[1], ", " , result[1], ", Message: ", result[2])
print("-" * 52)
print(dr.timer.timings)
print("-" * len(str(dr.timer.timings)))
print(dr.plan_stats.stats)
print("-" * len(str(dr.plan_stats.stats)))