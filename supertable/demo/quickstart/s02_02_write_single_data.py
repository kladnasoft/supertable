"""Single-shot write demonstrating the lineage payload.

``lineage`` is a dict — see ``DataWriter.write`` docstring in
``supertable/data_writer.py`` for the full conventional key list.
"""
from supertable.demo.quickstart.dummy_data import get_dummy_data
from supertable.demo.quickstart.defaults import super_name, role_name, simple_name, organization

from supertable.data_writer import DataWriter


data = get_dummy_data(1)

dw = DataWriter(super_name=super_name, organization=organization)

columns, rows, inserted, deleted = dw.write(
    role_name=role_name,
    simple_name=simple_name,
    data=data,
    overwrite_columns=["day"],
    lineage={
        "source_type": "manual",
        "source_id": "single_data_demo",
        "source_tables": ["dummy_data.ds=1"],
    },
)
print(f"columns={columns} rows={rows} inserted={inserted} deleted={deleted}")
