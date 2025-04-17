from examples.dummy_data import get_dummy_data
from examples.defaults import super_name, user_hash, simple_name, organization
from supertable.super_table import SuperTable
from supertable.staging_area import StagingArea

super_table = SuperTable(super_name=super_name, organization=organization)

staging_area = StagingArea(super_table=super_table, organization=organization)


staging_area.save_as_parquet(arrow_table=get_dummy_data(1)[1],
                             table_name = simple_name,
                             file_name = "dummy_file_01.parquet" )