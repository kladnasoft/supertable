"""Run the same query against two engines and inspect timing/plan stats.

Demonstrates ``engine.SPARK_SQL`` vs ``engine.AUTO``.
"""
from supertable.data_reader import DataReader, engine
from supertable.demo.quickstart.defaults import super_name, simple_name, organization, role_name


query = f"select count(*) as cnt from {simple_name}"


def run(label, eng):
    dr = DataReader(super_name=super_name, organization=organization, query=query)
    df, status, message = dr.execute(role_name=role_name, with_scan=False, engine=eng)
    bar = "-" * 60
    print(bar)
    print(f"[{label}] rows={df.shape[0]} cols={df.shape[1]} status={status} msg={message}")
    print(bar)
    print(f"timings: {dr.timer.timings}")
    print(f"plan_stats: {dr.plan_stats.stats}")


run("SPARK_SQL", engine.SPARK_SQL)
run("AUTO", engine.AUTO)
