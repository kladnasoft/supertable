import time

from supertable.monitoring_logger import MonitoringLogger
from concurrent.futures import ThreadPoolExecutor
import random
import os

print("Current working directory:", os.getcwd())

# Example with 'stats' metric
with MonitoringLogger(super_name="example", organization="kladna-soft", monitor_type="plans") as monitor:
    def simulate_query_stats(query_id: int):
        stats = {
            "query_id": f"query_{query_id}",
            "rows_read": random.randint(100, 10000),
            "rows_processed": random.randint(100, 10000),
            # Note: execution_time will be added automatically if missing
            "query_hash": random.randint(100000, 999999)
        }
        monitor.log_metric(stats)


    # Simulate high-volume writes
    with ThreadPoolExecutor(max_workers=8) as executor:
        for i in range(10000):
            executor.submit(simulate_query_stats, i)
            time.sleep(0.001)  # ~1000 metrics per second