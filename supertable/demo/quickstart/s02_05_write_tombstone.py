"""Soft-delete rows via DataWriter.write(delete_only=True).

When ``delete_only=True``, the writer does NOT add new parquet files; instead
it records tombstones for every row in the input whose primary-key tuple
matches an existing row. Subsequent reads transparently skip those rows
through the tombstone view layer (see docs/06_data_writer.md and
docs/10_data_reader.md).

This script:
  1. Re-uses dummy fixture ds=1 to derive primary-key tuples to delete.
  2. Calls write(delete_only=True) and prints how many rows were tombstoned.
  3. Runs a follow-up COUNT(*) so you can see the row count drop.
"""
from supertable.demo.quickstart.data_writer_helpers import count_rows  # local helper below

from supertable.demo.quickstart.defaults import super_name, simple_name, organization, role_name
from supertable.demo.quickstart.dummy_data import get_dummy_data
from supertable.data_writer import DataWriter


def main():
    arrow = get_dummy_data(1)
    print(f"Tombstoning {arrow.num_rows} rows by (day, client) from {simple_name}...")

    dw = DataWriter(super_name=super_name, organization=organization)
    columns, rows, inserted, deleted = dw.write(
        role_name=role_name,
        simple_name=simple_name,
        data=arrow,
        overwrite_columns=["day", "client"],
        delete_only=True,
        lineage={
            "source_type": "manual",
            "source_id": "tombstone_demo",
        },
    )
    print(
        f"columns={columns} rows={rows} inserted={inserted} deleted={deleted}"
    )

    print(f"Row count after tombstone: {count_rows(super_name, organization, simple_name, role_name)}")


if __name__ == "__main__":
    main()
