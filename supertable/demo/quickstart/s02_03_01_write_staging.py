"""Save an Arrow table into a staging area as Parquet.

The staging area is the source files for a SuperPipe (see 2.3.2).
``Staging.save_as_parquet`` is keyword-only and requires ``role_name``.
"""
from supertable.demo.quickstart.dummy_data import get_dummy_data
from supertable.demo.quickstart.defaults import (
    super_name, organization, staging_name, role_name,
)
from supertable.staging_area import Staging


staging = Staging(
    organization=organization,
    super_name=super_name,
    staging_name=staging_name,
)

saved_file_name = staging.save_as_parquet(
    role_name=role_name,
    arrow_table=get_dummy_data(1),
    base_file_name="dummy_file_01",
)

print(f"saved staging file: {saved_file_name}")
