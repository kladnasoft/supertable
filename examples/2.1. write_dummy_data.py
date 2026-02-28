import polars as pl
import glob, os

from examples.dummy_data import get_dummy_data
from examples.defaults import super_name, role_name, simple_name, organization
from supertable.data_writer import DataWriter
from supertable.simple_table import SimpleTable

overwrite_columns = ["day", "client"]
dw = DataWriter(super_name=super_name, organization=organization)
st = SimpleTable(dw.super_table, simple_name)
data_dir = st.data_dir

for ds in [1, 2, 6, 7, 3, 4, 5]:
    table_name, arrow = get_dummy_data(ds)
    print(f"\n=== Running write(ds={ds}) === Table: {simple_name} ===")
    _, _, ins, del_ = dw.write(
        role_name=role_name,
        simple_name=simple_name,
        data=arrow,
        overwrite_columns=overwrite_columns,
    )
    print(f"inserted={ins}, deleted={del_}")

