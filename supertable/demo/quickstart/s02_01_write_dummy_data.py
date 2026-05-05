"""Write all 7 dummy fixtures into the same SimpleTable.

This exercises:
- Initial create + first non-overlapping append (ds=1, 2)
- Schema evolution: extra column (ds=6) and dropped column (ds=7)
- Overlap + upsert via ``overwrite_columns`` (ds=3, 4, 5)

The order is intentional: 1, 2 establish the baseline; 6, 7 trigger schema
changes; 3, 4, 5 demonstrate overlap handling against existing snapshots.
"""
from supertable.demo.quickstart.dummy_data import get_dummy_data
from supertable.demo.quickstart.defaults import (
    super_name, role_name, simple_name, organization, overwrite_columns,
)
from supertable.data_writer import DataWriter


dw = DataWriter(super_name=super_name, organization=organization)

for ds in [1, 2, 6, 7, 3, 4, 5]:
    arrow = get_dummy_data(ds)
    print(f"\n=== write(ds={ds}) into {simple_name} ===")
    _, _, ins, dele = dw.write(
        role_name=role_name,
        simple_name=simple_name,
        data=arrow,
        overwrite_columns=overwrite_columns,
    )
    print(f"inserted={ins}, deleted={dele}")
