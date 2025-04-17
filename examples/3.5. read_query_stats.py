from supertable.data_reader import DataReader
from examples.defaults import super_name, user_hash, query_plan_table_name, organization

query = f"SELECT * FROM {query_plan_table_name} LIMIT 100"

dr = DataReader(super_name=super_name, organization=organization, query=query)
result = dr.execute(user_hash=user_hash, with_scan=False)
print("-" * 52)
print("Rows: ", result[0].shape[0], ", Columns: ", result[0].shape[1], ", " , result[1], ", Message: ", result[2])
print("-" * 52)
print(dr.timer.timings)
print("-" * len(str(dr.timer.timings)))
print(dr.plan_stats.stats)
print("-" * len(str(dr.plan_stats.stats)))

import pandas as pd

# Adjust display options so that long strings and all columns are fully shown
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.width', None)

# Now print the DataFrame fully
print(result[0].to_string())