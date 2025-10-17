from supertable.data_reader import DataReader, engine
from examples.defaults import super_name, user_hash, simple_name, organization

query = f"select * as cnt from {simple_name} where 1=1 limit 10"

dr = DataReader(super_name=super_name, organization=organization, query=query)
result = dr.execute(user_hash=user_hash, with_scan=False, engine=engine.DUCKDB)
print("-" * 52)
print("Rows: ", result[0].shape[0], ", Columns: ", result[0].shape[1], ", " , result[1], ", Message: ", result[2])
print("-" * 52)
print(dr.timer.timings)
print("-" * len(str(dr.timer.timings)))
print(dr.plan_stats.stats)
print("-" * len(str(dr.plan_stats.stats)))