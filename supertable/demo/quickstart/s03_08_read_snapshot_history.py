"""Walk the SimpleTable snapshot linked list (point-in-time inspection).

Every write creates a new snapshot whose ``previous_snapshot`` field points
at the predecessor path on storage. Redis only stores the current leaf
pointer; older snapshots are JSON files we can pull directly through
``SuperTable.read_simple_table_snapshot``.

This script:
  1. Resolves the current snapshot via SimpleTable.get_simple_table_snapshot.
  2. Walks ``previous_snapshot`` until it hits None or a cycle.
  3. Prints version, timestamp, resource count and total rows for each.
  4. As a bonus, lists the parquet files referenced by the *oldest* snapshot
     so you can see exactly what data was visible at that point in time.
"""
from typing import Set

from supertable.demo.quickstart.defaults import super_name, simple_name, organization
from supertable.super_table import SuperTable
from supertable.simple_table import SimpleTable


_MAX_VERSIONS = 200  # safety cap, matches the value used internally


def main():
    st = SuperTable(super_name=super_name, organization=organization)
    simple = SimpleTable(super_table=st, simple_name=simple_name)

    snapshot, path = simple.get_simple_table_snapshot()

    visited: Set[str] = set()
    chain = []
    while snapshot is not None and path and path not in visited and len(chain) < _MAX_VERSIONS:
        visited.add(path)
        chain.append((path, snapshot))
        prev_path = snapshot.get("previous_snapshot")
        if not prev_path:
            break
        try:
            snapshot = st.read_simple_table_snapshot(prev_path)
            path = prev_path
        except FileNotFoundError:
            print(f"  (previous snapshot pruned: {prev_path})")
            break

    print(f"Snapshot chain length: {len(chain)} (newest → oldest)")
    for idx, (snap_path, snap) in enumerate(chain):
        resources = snap.get("resources") or []
        total_rows = sum((r.get("rows") or 0) for r in resources)
        last_updated = snap.get("last_updated_ms") or snap.get("last_updated")
        print(
            f"  v={len(chain) - idx:>3}  "
            f"resources={len(resources):>3}  "
            f"rows={total_rows:>8}  "
            f"updated={last_updated}  "
            f"path={snap_path}"
        )

    if chain:
        oldest_path, oldest_snap = chain[-1]
        print("\nFiles referenced by the oldest snapshot:")
        for r in (oldest_snap.get("resources") or [])[:10]:
            print(f"  {r.get('file') or r.get('path')}")


if __name__ == "__main__":
    main()
