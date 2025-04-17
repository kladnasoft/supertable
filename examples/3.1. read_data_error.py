from supertable.data_reader import DataReader
from examples.defaults import super_name, simple_name, organization

user_hash="0d261765d0f88e3bdede483c3d41d125"
query = f"select count(*) as cnt from {simple_name}"

dr = DataReader(super_name=super_name, organization=organization, query=query)
result = dr.execute(user_hash=user_hash, with_scan=False)
print("-" * 85)
print("Rows: ", result[0].shape[0], ", Columns: ", result[0].shape[1], ", " , result[1], ", Message: ", result[2])
print("-" * 85)
print(dr.timer.timings)
print("-" * len(str(dr.timer.timings)))
print(dr.plan_stats.stats)
print("-" * len(str(dr.plan_stats.stats)))