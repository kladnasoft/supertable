import threading
import time
import random
import os

from concurrent.futures import ThreadPoolExecutor, wait
from supertable.monitoring_writer import MonitoringLogger
from examples.defaults import super_name, organization, MonitorType

def print_monitor_stats(monitor):
    stats = monitor.get_queue_stats()
    print(f"\nProcessed: {stats['total_processed']}/{stats['total_received']} | "
          f"Queue: {stats['current_size']} | "
          f"Rate: {stats['processing_rate']:.1f} msg/s")


def verify_output(monitor, expected_count):
    # Wait for processing to complete
    for _ in range(20):  # 20 second timeout
        stats = monitor.get_queue_stats()
        if stats['total_processed'] >= expected_count:
            break
        time.sleep(1)

    # Verify final output
    if stats['total_processed'] < expected_count:
        print(f"\nWARNING: Only processed {stats['total_processed']}/{expected_count} messages")
    else:
        print(f"\nSUCCESS: Processed all {expected_count} messages")

    # Print final file stats
    catalog = monitor.storage.read_json(monitor.catalog_path)
    print(f"\nFinal Catalog Stats:")
    print(f"Files: {catalog['file_count']}")
    print(f"Rows: {catalog['total_rows']}")
    print(f"Version: {catalog['version']}")


print("Current working directory:", os.getcwd())

with MonitoringLogger(
        super_name=super_name,
        organization=organization,
        monitor_type=MonitorType.METRICS.value,
        max_rows_per_file=500,
        flush_interval=0.1
) as monitor:
    def worker(query_id):
        stats = {
            "query_id": f"query_{query_id}",
            "rows_read": random.randint(100, 10000),
            "rows_processed": random.randint(100, 10000),
            "query_hash": random.randint(100000, 999999)
        }
        monitor.log_metric(stats)
        return query_id


    # Start monitoring thread
    def monitor_loop():
        while not monitor.stop_event.is_set():
            print_monitor_stats(monitor)
            time.sleep(0.5)


    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()

    # Submit work
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(worker, i) for i in range(1001)]
        wait(futures)  # Wait for all tasks to be submitted

    # Ensure queue is drained
    print("\nWaiting for queue to drain...")
    while monitor.queue.qsize() > 0 or len(monitor.current_batch) > 0:
        time.sleep(0.1)

    # Verify all messages were processed
    verify_output(monitor, 1000)

    # Extra safety wait
    time.sleep(1)