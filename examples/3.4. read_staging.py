from examples.defaults import super_name, user_hash, simple_name, organization
from supertable.super_table import SuperTable
from supertable.staging_area import Staging
from supertable.config.defaults import logger

super_table = SuperTable(super_name=super_name, organization=organization)

staging_area = Staging(super_table=super_table, organization=organization)


staging = staging_area.get_directory_structure()
logger.info(staging)