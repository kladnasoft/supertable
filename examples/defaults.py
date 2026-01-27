import logging
from enum import Enum

from supertable.config import defaults
logging.getLogger('supertable').setLevel(logging.INFO)

defaults.default.IS_SHOW_TIMING = True

user_hash = "0b85b786b16d195439c0da18fd4478df"
super_name = "example"
simple_name = "facts"
organization = "kladna-soft"
overwrite_columns = ["partition"]
generated_data_dir = "generated_data"
staging_name = "new1"

class MonitorType(Enum):
    PLANS = "plans"
    STATS = "stats"
    METRICS = "metrics"