from supertable.config.defaults import logger
from supertable.data_writer import DataWriter
from examples.dummy_data import get_dummy_data
from examples.defaults import super_name, user_hash, organization

overwrite_columns = ["day", "client"]

super_table = DataWriter(super_name=super_name, organization=organization)

for i in range(1, 5):
    simple_name, new_data = get_dummy_data(i)

    columns, rows, inserted, deleted = super_table.write(
        user_hash=user_hash,
        simple_name=simple_name,
        data=new_data,
        overwrite_columns=overwrite_columns
    )
    logger.info(f"Result: columns: {columns}, rows: {rows}, inserted: {inserted}, deleted: {deleted}")
