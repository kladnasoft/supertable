import time

from supertable.monitoring_logger import MonitoringLogger
import random
import os


with MonitoringLogger(super_name="example", organization="kladna-soft", monitor_type="plans") as monitor:
    query_id = random.randint(100000, 999999)

    stats = {
        "query_id": f"query_{query_id}",
        "rows_read": random.randint(100, 10000),
        "rows_processed": random.randint(100, 10000),
        # Note: execution_time will be added automatically if missing
        "query_hash": random.randint(100000, 999999)
    }
    monitor.log_metric(stats)