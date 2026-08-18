"""Idempotent load of the medcenter demo fixtures into SuperTable.

Each ``<data_dir>/<table_name>/`` folder maps to one raw table; every file
(semicolon CSV for file-based feeds, parquet for API feeds) is upserted
with the table's natural key as ``overwrite_columns``, so re-running the
load overwrites the same rows instead of duplicating them — the response
line makes that visible (2nd run: inserted == deleted).
"""

import argparse
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from supertable.config.homedir import initialize_app_home
from supertable.data_writer import DataWriter
from supertable.demo.medcenter.defaults import (
    generated_data_dir,
    organization,
    overwrite_columns_by_table,
    role_name,
    super_name,
)

RAW_TABLES = [
    name for name in overwrite_columns_by_table if name.startswith("raw_")
]


def read_file_as_table(file_path: str) -> pa.Table:
    if file_path.endswith(".parquet"):
        return pq.read_table(file_path)
    df = pd.read_csv(file_path, sep=";", low_memory=False)
    # Keep raw as-is, but normalise object columns to clean strings
    # (NaN from empty CSV fields would otherwise stringify to "nan").
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].fillna("").astype(str)
    return pa.Table.from_pandas(df)


def load(data_dir: str = generated_data_dir) -> dict:
    """Load every file of every raw-table folder. Returns per-table stats."""
    data_writer = DataWriter(super_name, organization)
    stats: dict[str, dict[str, int]] = {}

    for table_name in RAW_TABLES:
        table_dir = os.path.join(data_dir, table_name)
        if not os.path.isdir(table_dir):
            raise FileNotFoundError(
                f"Missing fixture folder {table_dir!r} — run "
                f"supertable-demo-medcenter-generate first."
            )
        table_stats = {"files": 0, "rows": 0, "inserted": 0, "deleted": 0}
        for file_name in sorted(os.listdir(table_dir)):
            file_path = os.path.join(table_dir, file_name)
            if not os.path.isfile(file_path):
                continue
            if not file_name.endswith((".csv", ".parquet")):
                continue
            table = read_file_as_table(file_path)
            columns, rows, inserted, deleted = data_writer.write(
                role_name=role_name,
                simple_name=table_name,
                data=table,
                overwrite_columns=overwrite_columns_by_table[table_name],
            )
            table_stats["files"] += 1
            table_stats["rows"] += rows
            table_stats["inserted"] += inserted
            table_stats["deleted"] += deleted
            print(
                f"[{table_name}] {file_name}: columns={columns} rows={rows} "
                f"inserted={inserted} deleted={deleted}"
            )
        stats[table_name] = table_stats

    return stats


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Load medcenter demo fixtures into SuperTable (idempotent)"
    )
    ap.add_argument("--data-dir", default=generated_data_dir)
    return ap.parse_args()


def main() -> None:
    initialize_app_home(change_cwd=True)
    args = parse_args()
    stats = load(data_dir=args.data_dir)
    print("\nLoad complete:")
    for table_name, s in stats.items():
        print(
            f"- {table_name}: {s['files']} file(s), {s['rows']} rows, "
            f"inserted={s['inserted']}, deleted={s['deleted']}"
        )


if __name__ == "__main__":
    main()
