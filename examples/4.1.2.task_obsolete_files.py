#!/usr/bin/env python3
import time
from supertable.history_cleaner import HistoryCleaner
from examples.defaults import super_name, user_hash, organization


def main():
    history_cleaner = HistoryCleaner(
        super_name=super_name,
        organization=organization,
    )

    while True:
        try:
            history_cleaner.clean(role_name=user_hash)
            print("[INFO] History cleaned successfully.")
        except Exception as e:
            print(f"[ERROR] Failed to clean history: {e}")
        # wait 60 seconds before next run
        time.sleep(60)


if __name__ == "__main__":
    main()
