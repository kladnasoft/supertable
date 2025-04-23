import logging

from supertable.config import defaults
logging.getLogger('supertable').setLevel(logging.DEBUG)

defaults.default.IS_SHOW_TIMING = True

user_hash = "0b85b786b16d195439c0da18fd4478df"
super_name = "example"
simple_name = "facts"
query_plan_table_name = '__query_stats__'
organization = "kladna-soft"
overwrite_columns = ["Day"]
generated_data_dir = "generated_data"