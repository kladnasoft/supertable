from supertable.super_table import SuperTable
from examples.defaults import super_name, organization
from supertable.config.defaults import logger

st = SuperTable(super_name, organization)
logger.info(f"Created SuperTable: {st.super_name}")