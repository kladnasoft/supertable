import time
import random

from supertable.monitoring_writer import get_monitoring_logger
from examples.defaults import super_name, organization


def main():
    # Get the monitoring logger (singleton)
    monitor = get_monitoring_logger(
        super_name=super_name,
        organization=organization,
        monitor_type="metrics"
    )

    # Generate a unique ID for this query run
    query_id = random.randint(100000, 999999)

    # Start a high-resolution timer
    start_time = time.perf_counter()

    # --- Place your actual work here ---
    # For demo purposes, we're just creating some random metrics
    stats = {
        "query_id": f"query_{query_id}",
        "rows_read": random.randint(100, 10000),
        "rows_processed": random.randint(100, 10000),
        "query_hash": random.randint(100000, 999999),
        "table_name": "test_table"  # Required for proper partitioning
    }
    monitor.log_metric(stats)
    # --- Work complete ---

    # Stop the timer
    end_time = time.perf_counter()

    # Calculate and print the elapsed time in seconds
    elapsed = end_time - start_time
    print(f"Total execution time: {elapsed:.4f} seconds")

    # Request flush to ensure data is written
    monitor.request_flush()

    # Small delay to allow processing
    time.sleep(0.5)


if __name__ == "__main__":
    main()