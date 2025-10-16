import threading
import time
import random
import os

from concurrent.futures import ThreadPoolExecutor, wait
from supertable.monitoring_writer import get_monitoring_logger
from examples.defaults import super_name, organization


def print_monitor_stats(monitor):
    """Print current monitor statistics."""
    stats = monitor.queue_stats
    with monitor.queue_stats_lock:
        current_stats = stats.copy()

    processed = current_stats.get("total_processed", 0)
    received = current_stats.get("total_received", 0)
    queue_size = current_stats.get("current_size", 0)

    print(f"\nProcessed: {processed}/{received} | Queue: {queue_size}")


def worker(monitor, query_id):
    """Worker function that logs metrics."""
    stats = {
        "query_id": f"query_{query_id}",
        "rows_read": random.randint(100, 10000),
        "rows_processed": random.randint(100, 10000),
        "query_hash": random.randint(100000, 999999),
        "table_name": f"table_{query_id % 5}"  # Distribute across 5 tables
    }
    monitor.log_metric(stats)
    return query_id


def main():
    print("Current working directory:", os.getcwd())

    # Get the monitoring logger (singleton)
    monitor = get_monitoring_logger(
        super_name=super_name,
        organization=organization,
        monitor_type="metrics"
    )

    # Start monitoring thread
    stop_monitoring = threading.Event()

    def monitor_loop():
        while not stop_monitoring.is_set():
            print_monitor_stats(monitor)
            time.sleep(0.5)

    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()

    try:
        # Submit work
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(worker, monitor, i) for i in range(1000)]
            wait(futures)  # Wait for all tasks to complete

        print("\nAll tasks submitted. Waiting for queue to drain...")

        # Wait for processing to complete
        for _ in range(30):  # 30 second timeout
            with monitor.queue_stats_lock:
                processed = monitor.queue_stats.get("total_processed", 0)
                queue_size = monitor.queue.qsize()
                batch_size = len(monitor.current_batch)

            if processed >= 1000 and queue_size == 0 and batch_size == 0:
                break
            time.sleep(0.5)
            print_monitor_stats(monitor)

        # Final verification
        with monitor.queue_stats_lock:
            final_processed = monitor.queue_stats.get("total_processed", 0)

        if final_processed >= 1000:
            print(f"\nSUCCESS: Processed all {final_processed} messages")
        else:
            print(f"\nWARNING: Only processed {final_processed}/1000 messages")

    finally:
        # Stop monitoring thread
        stop_monitoring.set()
        monitor_thread.join(timeout=1.0)

        # Request final flush
        monitor.request_flush()
        time.sleep(1.0)  # Allow time for final processing


if __name__ == "__main__":
    main()