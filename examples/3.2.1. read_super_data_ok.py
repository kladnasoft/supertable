from supertable.data_reader import DataReader, engine
from examples.defaults import super_name, user_hash, simple_name, organization, role_name

query = f"select * as cnt from {super_name} where 1=1 limit 10"

dr = DataReader(super_name=super_name, organization=organization, query=query)
result = dr.execute(role_name=role_name, with_scan=False, engine=engine.SPARK)

print("-" * 52)
print("Rows: ", result[0].shape[0], ", Columns: ", result[0].shape[1], ", " , result[1], ", Message: ", result[2])
print("-" * 52)
print(dr.timer.timings)
print("-" * len(str(dr.timer.timings)))
print(dr.plan_stats.stats)
print("-" * len(str(dr.plan_stats.stats)))


dr2 = DataReader(super_name=super_name, organization=organization, query=query)
result2 = dr2.execute(role_name=role_name, with_scan=False, engine=engine.AUTO)
print("-" * 52)
print("Rows: ", result2[0].shape[0], ", Columns: ", result2[0].shape[1], ", " , result2[1], ", Message: ", result2[2])
print("-" * 52)
print(dr2.timer.timings)
print("-" * len(str(dr2.timer.timings)))
print(dr2.plan_stats.stats)
print("-" * len(str(dr2.plan_stats.stats)))