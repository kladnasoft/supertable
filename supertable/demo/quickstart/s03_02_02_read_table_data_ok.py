"""Aggregation query against the ``facts`` SimpleTable created by 2.1."""
from supertable.data_reader import DataReader, engine
from supertable.demo.quickstart.defaults import super_name, role_name, simple_name, organization


query = f"""
SELECT day, client, sum(value) AS total
FROM {simple_name}
GROUP BY day, client
ORDER BY day
LIMIT 10
"""

dr = DataReader(super_name=super_name, organization=organization, query=query)
df, status, message = dr.execute(role_name=role_name, with_scan=False, engine=engine.AUTO)

bar = "-" * 60
print(bar)
print(f"rows={df.shape[0]} cols={df.shape[1]} status={status} msg={message}")
print(bar)
print(f"timings: {dr.timer.timings}")
print(f"plan_stats: {dr.plan_stats.stats}")
print(df)
