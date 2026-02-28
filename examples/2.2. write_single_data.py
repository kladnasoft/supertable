from examples.dummy_data import get_dummy_data
from examples.defaults import super_name, role_name, simple_name, organization
from supertable.data_writer import DataWriter

overwrite_columns = ["day"]
data = get_dummy_data(1)[1]

data_writer = DataWriter(super_name=super_name, organization=organization)

columns, rows, inserted, deleted = data_writer.write(
    role_name=role_name,
    simple_name=simple_name,
    data=data,
    overwrite_columns=overwrite_columns,
)