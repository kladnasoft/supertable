"""Run every numbered quickstart in order.

Each step is launched as a fresh subprocess via ``python -m
supertable.demo.quickstart.<module>`` so each script gets a clean interpreter
state. Destructive (s05_*) scripts stay commented out.

Invoke with::

    python -m supertable.demo.quickstart
"""
import subprocess
import sys


PACKAGE = "supertable.demo.quickstart"


STEPS = [
    # Setup
    "s01_01_01_create_super_table",
    "s01_01_02_enable_mirroring_formats",
    "s01_02_create_roles",
    "s01_03_create_users",
    # Write path
    "s02_01_write_dummy_data",
    "s02_02_write_single_data",
    "s02_03_01_write_staging",
    "s02_03_02_create_pipe",
    "s02_04_01_write_monitoring_simple",
    "s02_04_02_write_monitoring_parallel",
    "s02_05_write_tombstone",
    # Read path
    "s03_01_read_data_error",
    "s03_02_01_read_super_data_ok",
    "s03_02_02_read_table_data_ok",
    "s03_03_read_meta",
    "s03_04_read_staging",
    "s03_06_01_read_roles",
    "s03_06_02_read_user",
    "s03_07_01_estimate_read",
    "s03_07_02_estimate_files",
    "s03_08_read_snapshot_history",
    # Maintenance
    "s04_01_03_delete_pipe",
    # Destructive — keep commented unless you really mean it.
    # "s05_01_delete_table",
    # "s05_02_delete_super_table",
]


def run(module_name: str) -> bool:
    """Run a single step. Returns True on success."""
    full = f"{PACKAGE}.{module_name}"
    print(f"\nRunning {full}...")
    try:
        result = subprocess.run(
            [sys.executable, "-m", full],
            check=True,
            capture_output=True,
            text=True,
        )
        print(f"OK   {full}")
        if result.stdout:
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print(f"FAIL {full}")
        print(e.stderr)
        return False


def main() -> None:
    print("Starting SuperTable quickstart suite...")
    for step in STEPS:
        run(step)
    print("\nDone.")


if __name__ == "__main__":
    main()
