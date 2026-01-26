from examples.dummy_data import get_dummy_data
from examples.defaults import super_name, organization, staging_name

from supertable.staging_area import Staging



staging = Staging(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

saved_file_name = staging.save_as_parquet(
    arrow_table=get_dummy_data(1)[1],
    base_file_name="dummy_file_01",
)

print(saved_file_name)
