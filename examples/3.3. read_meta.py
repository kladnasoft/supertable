import os
from supertable.config.defaults import logger
from supertable.meta_reader import MetaReader, find_tables
from examples.defaults import super_name, user_hash, simple_name, organization


current_working_directory = os.getcwd()
# Print the current working directory
logger.info(f"Current working directory: {current_working_directory}")


result = find_tables(organization=organization)
logger.info(f"result: {result}")



logger.info(f"SuperTable: {super_name}")
meta_reader = MetaReader(super_name=super_name,organization=organization)


result = meta_reader.get_super_meta(user_hash)
logger.info(f"meta_reader.result: {result}")


tables = result.get("super").get("tables")
logger.info(f"Tables: {tables}")

result = meta_reader.get_table_schema(super_name, user_hash)
logger.info(f"super_name.schema.result: {result}")


result = meta_reader.get_table_schema(simple_name, user_hash)
logger.info(f"simple_name.schema.result: {result}")

result = meta_reader.get_table_stats(super_name, user_hash)
logger.info(f"super_name.stats.result: {result}")

result = meta_reader.get_table_stats(simple_name, user_hash)
logger.info(f"simple_name.stats.result: {result}")
