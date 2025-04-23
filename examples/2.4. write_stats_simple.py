import time
import random
import os

from supertable.monitoring_logger import MonitoringLogger

# Use the MonitoringLogger in a context manager to ensure proper setup and teardown
with MonitoringLogger(
    super_name="example",
    organization="kladna-soft",
    monitor_type="plans"
) as monitor:

    # Generate a unique ID for this query run
    query_id = random.randint(100000, 999999)

    # Start a high-resolution timer
    start_time = time.perf_counter()

    # --- Place your actual work here ---
    # For demo purposes, we’re just creating some random metrics
    stats = {
        "query_id": f"query_{query_id}",
        "rows_read": random.randint(100, 10000),
        "rows_processed": random.randint(100, 10000),
        "query_hash": random.randint(100000, 999999)
    }
    monitor.log_metric(stats)
    # --- Work complete ---

    # Stop the timer
    end_time = time.perf_counter()

    # Calculate and print the elapsed time in seconds
    elapsed = end_time - start_time
    print(f"Total execution time: {elapsed:.4f} seconds")
