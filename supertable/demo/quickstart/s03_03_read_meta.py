"""Inspect SuperTable / SimpleTable metadata via the MetaReader facade."""
from supertable.config.defaults import logger
from supertable.meta_reader import MetaReader, list_supers, list_tables

from supertable.demo.quickstart.defaults import super_name, role_name, simple_name, organization


logger.info(f"supers in {organization}: {list_supers(organization=organization)}")
logger.info(
    f"tables in {organization}/{super_name}: "
    f"{list_tables(organization=organization, super_name=super_name)}"
)

mr = MetaReader(organization=organization, super_name=super_name)

logger.info(f"super_meta: {mr.get_super_meta(role_name)}")
logger.info(f"{simple_name}.schema: {mr.get_table_schema(simple_name, role_name)}")
logger.info(f"{simple_name}.stats: {mr.get_table_stats(simple_name, role_name)}")
