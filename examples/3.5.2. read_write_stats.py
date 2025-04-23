import os
from datetime import datetime, timezone, timedelta

from supertable.monitoring_reader import MonitoringReader
from examples.defaults import super_name, organization, MonitorType

def main():
    # 1) Instantiate for the “plans” monitor
    reader = MonitoringReader(
        super_name=super_name,
        organization=organization,
        monitor_type=MonitorType.STATS.value
    )

    # 2) Read the last 24h of data (default)
    df_last_day = reader.read()
    print(f"Fetched {len(df_last_day)} rows from the last day")
    print(df_last_day.head())


if __name__ == "__main__":
    main()
