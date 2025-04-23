import os
from datetime import datetime, timezone, timedelta

from supertable.monitoring_reader import MonitoringReader
from examples.defaults import super_name, organization, MonitorType

def main():
    # 1) Instantiate for the “plans” monitor
    reader = MonitoringReader(
        super_name=super_name,
        organization=organization,
        monitor_type=MonitorType.PLANS.value
    )

    # 2) Read the last 24h of data (default)
    df_last_day = reader.read()
    print(f"Fetched {len(df_last_day)} rows from the last day")
    print(df_last_day.head())

    # 3) Read a custom window:
    #    from 48h ago up to 24h ago, limit 500 rows
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1_000)
    from_ms = now_ms - int(timedelta(days=2).total_seconds() * 1_000)
    to_ms   = now_ms - int(timedelta(days=1).total_seconds() * 1_000)

    df_window = reader.read(
        from_ts_ms=from_ms,
        to_ts_ms=to_ms,
        limit=500
    )
    print(f"Fetched {len(df_window)} rows between {from_ms} and {to_ms}")
    print(df_window.head())

if __name__ == "__main__":
    main()
