# examples/3.5.2. read_write_stats.py
import os
import time
from datetime import datetime, timezone, timedelta

from supertable.monitoring_reader import MonitoringReader
from supertable.monitoring_writer import get_monitoring_logger
from examples.defaults import super_name, organization, MonitorType


def setup_test_data():
    """Write some test monitoring data first"""
    print("Setting up test monitoring data...")

    writer = get_monitoring_logger(
        super_name=super_name,
        organization=organization,
        monitor_type=MonitorType.STATS.value
    )

    # Write sample statistics data with more realistic timestamps
    base_time = int(datetime.now(timezone.utc).timestamp() * 1000)

    sample_stats = [
        {
            "table_name": "users",
            "operation": "select",
            "execution_time": base_time - 3600000,  # 1 hour ago
            "rows_processed": 1500,
            "duration_ms": 45,
            "memory_usage_mb": 12,
            "cache_hit_ratio": 0.85
        },
        {
            "table_name": "orders",
            "operation": "insert",
            "execution_time": base_time - 7200000,  # 2 hours ago
            "rows_processed": 500,
            "duration_ms": 120,
            "memory_usage_mb": 8,
            "cache_hit_ratio": 0.92
        },
        {
            "table_name": "products",
            "operation": "update",
            "execution_time": base_time - 10800000,  # 3 hours ago
            "rows_processed": 200,
            "duration_ms": 80,
            "memory_usage_mb": 6,
            "cache_hit_ratio": 0.78
        },
        {
            "table_name": "customers",
            "operation": "delete",
            "execution_time": base_time - 14400000,  # 4 hours ago
            "rows_processed": 50,
            "duration_ms": 30,
            "memory_usage_mb": 4,
            "cache_hit_ratio": 0.95
        },
        {
            "table_name": "inventory",
            "operation": "select",
            "execution_time": base_time - 18000000,  # 5 hours ago
            "rows_processed": 3000,
            "duration_ms": 200,
            "memory_usage_mb": 25,
            "cache_hit_ratio": 0.67
        }
    ]

    for stat in sample_stats:
        writer.log_metric(stat)
        print(f"Logged stat: {stat['table_name']} - {stat['operation']} at {stat['execution_time']}")

    # Force flush and wait for processing
    writer.request_flush()
    print("Waiting for data to be written and files to be available...")

    # Wait longer and verify files actually exist
    time.sleep(10)  # Give more time for background processing

    # Properly close the writer with forced flush
    writer.close(force_flush=True)

    # Additional wait to ensure all files are committed
    time.sleep(2)
    print("Test data setup complete")


def wait_for_files_available(stats_path, max_wait_seconds=30):
    """Wait until all parquet files listed in stats are actually available in storage"""
    from supertable.storage.storage_factory import get_storage
    storage = get_storage()

    if not storage.exists(stats_path):
        print(f"Stats file doesn't exist yet: {stats_path}")
        return False

    start_time = time.time()
    while time.time() - start_time < max_wait_seconds:
        try:
            stats = storage.read_json(stats_path)
            files = stats.get('files', [])

            if not files:
                print("No files listed in stats yet...")
                time.sleep(2)
                continue

            # Check if all files exist
            missing_files = []
            for file_info in files:
                file_path = file_info.get("path") or file_info.get("file")
                if file_path and not storage.exists(file_path):
                    missing_files.append(file_path)

            if not missing_files:
                print(f"All {len(files)} parquet files are available in storage")
                return True
            else:
                print(f"Waiting for {len(missing_files)} files to be available...")
                if len(missing_files) <= 5:  # Don't spam too much
                    for missing in missing_files[:3]:  # Show first few
                        print(f"  - {missing}")
                    if len(missing_files) > 3:
                        print(f"  ... and {len(missing_files) - 3} more")

        except Exception as e:
            print(f"Error checking files: {e}")

        time.sleep(2)

    print(f"Timeout waiting for files after {max_wait_seconds} seconds")
    return False


def check_stats_file():
    """Check if the stats file was created and files are available"""
    from supertable.storage.storage_factory import get_storage
    storage = get_storage()
    stats_path = os.path.join(organization, super_name, "_stats.json")

    if storage.exists(stats_path):
        stats = storage.read_json(stats_path)
        print(f"Stats file exists: {stats_path}")
        print(f"File count: {stats.get('file_count', 0)}")
        print(f"Row count: {stats.get('row_count', 0)}")

        # Check if files actually exist
        available_files = 0
        for file_info in stats.get('files', []):
            file_path = file_info.get("path") or file_info.get("file")
            if file_path and storage.exists(file_path):
                available_files += 1

        print(f"Files available in storage: {available_files}/{len(stats.get('files', []))}")
        return available_files > 0
    else:
        print(f"Stats file does not exist: {stats_path}")
        return False


def main():
    # First, ensure we have some data to read
    setup_test_data()

    stats_path = os.path.join(organization, super_name, "_stats.json")

    # Wait for files to be actually available in storage
    print("\nWaiting for parquet files to be available...")
    if not wait_for_files_available(stats_path):
        print("Files not available within timeout. Cannot proceed with reading.")
        return

    # Check if stats file was created and files are available
    if not check_stats_file():
        print("No available files found. Cannot proceed with reading.")
        return

    # Now read the data
    reader = MonitoringReader(
        super_name=super_name,
        organization=organization,
        monitor_type=MonitorType.STATS.value
    )

    # Read the last 24h of data (default)
    print("\nReading last 24 hours of data...")
    try:
        df_last_day = reader.read()
        print(f"Fetched {len(df_last_day)} rows from the last day")
        if len(df_last_day) > 0:
            print("Columns:", df_last_day.columns.tolist())
            print("Data sample:")
            print(df_last_day.head())
        else:
            print("No data found for the last 24 hours")
    except Exception as e:
        print(f"Error reading data: {e}")

    # Read a custom window - use a wider time range to catch our test data
    print("\nReading custom time window (last 6 hours)...")
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1_000)
    from_ms = now_ms - int(timedelta(hours=6).total_seconds() * 1_000)

    try:
        df_window = reader.read(
            from_ts_ms=from_ms,
            to_ts_ms=now_ms,
            limit=1000
        )
        print(f"Fetched {len(df_window)} rows between {from_ms} and {now_ms}")
        if len(df_window) > 0:
            print("Data sample:")
            print(df_window.head())
    except Exception as e:
        print(f"Error reading custom window: {e}")


if __name__ == "__main__":
    main()