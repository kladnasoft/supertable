import os
import pyarrow.parquet as pq

from examples.defaults import simple_name, super_name, user_hash, organization
from supertable.super_table import SuperTable
from supertable.storage.storage_factory import get_storage

st = SuperTable(super_name, organization)
storage = get_storage()

working_dir = os.path.join(st.organization, st.super_name, "simple", simple_name, "data")
print(working_dir)
files = storage.list_files(working_dir, "*")

parquet_file_path = files[0]


# Open the Parquet file
parquet_file = pq.ParquetFile(parquet_file_path)

# Get the metadata
metadata = parquet_file.metadata

# Iterate through each row group and each column to get the statistics
for row_group_index in range(metadata.num_row_groups):
    row_group = metadata.row_group(row_group_index)

    print(f"Row Group {row_group_index}:")
    for column_index in range(row_group.num_columns):
        column = row_group.column(column_index)
        column_name = column.path_in_schema
        column_stats = column.statistics

        if column_stats:
            print(f"  Column: {column_name}")
            print(f"    Min: {column_stats.min}")
            print(f"    Max: {column_stats.max}")
            print(f"    Null Count: {column_stats.null_count}")
            print(f"    Distinct Count: {column_stats.distinct_count}")
        else:
            print(f"  Column: {column_name} (No statistics available)")
