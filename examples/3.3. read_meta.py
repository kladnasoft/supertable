import os
from supertable.config.defaults import logger
from supertable.meta_reader import MetaReader, list_supers, list_tables
from examples.defaults import super_name, role_name, simple_name, organization


current_working_directory = os.getcwd()
# Print the current working directory
logger.info(f"Current working directory: {current_working_directory}")


result = list_supers(organization=organization)
logger.info(f"supers: {result}")

result = list_tables(organization=organization, super_name=super_name)
logger.info(f"tables: {result}")



meta_reader = MetaReader(organization=organization, super_name=super_name)

result = meta_reader.get_super_meta(role_name)
logger.info(f"meta_reader.result: {result}")


result = meta_reader.get_table_schema(super_name, role_name)
logger.info(f"super_name.schema.result: {result}")


result = meta_reader.get_table_schema(simple_name, role_name)
logger.info(f"simple_name.schema.result: {result}")

result = meta_reader.get_table_stats(super_name, role_name)
logger.info(f"super_name.stats.result: {result}")

result = meta_reader.get_table_stats(simple_name, role_name)
logger.info(f"simple_name.stats.result: {result}")
