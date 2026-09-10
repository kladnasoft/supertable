"""Append-load telemetry harness for the SuperTable write path.

Drives N appends into one table and captures the full ``Profiler`` payload
that ``DataWriter.write`` hands to monitoring — per-stage timings plus the
I/O counters — without touching the writer.  ``MonitoringWriter`` is swapped
for a recorder, which is the only interception point needed: the payload
already carries ``profiler.emit_timings()`` and ``profiler.emit_counts()``.

Everything downstream is polars.

    python scripts/write_telemetry_audit.py --batches 100 --rows 100000
"""
from __future__ import annotations

import argparse
import os
import time
import uuid

os.environ.setdefault("STORAGE_TYPE", "LOCAL")

import polars as pl
import pyarrow as pa


# --------------------------------------------------------------------------
# Telemetry capture
# --------------------------------------------------------------------------

CAPTURED: list[dict] = []


class _Recorder:
    """Stand-in for MonitoringWriter that keeps the payload in memory."""

    def __init__(self, *_, **__):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def log_metric(self, payload):
        CAPTURED.append(payload)


def install_recorder():
    import supertable.data_writer as dw

    dw.MonitoringWriter = _Recorder


# --------------------------------------------------------------------------
# Data
# --------------------------------------------------------------------------

def make_batch(batch_idx: int, rows: int) -> pa.Table:
    """A realistically shaped batch: ints, floats, strings, a timestamp.

    Built with polars and handed over as Arrow, which is what write() takes.
    """
    base = batch_idx * rows
    df = pl.DataFrame({
        "id": pl.arange(base, base + rows, eager=True),
        "key": pl.arange(base, base + rows, eager=True) % 1_000_003,
        "category": pl.Series(
            [f"cat_{i % 32}" for i in range(rows)], dtype=pl.Utf8
        ),
        "name": pl.Series(
            [f"entity-{(base + i) % 250_000:08d}" for i in range(rows)],
            dtype=pl.Utf8,
        ),
        "value": (pl.arange(0, rows, eager=True).cast(pl.Float64) * 1.5) % 9973.0,
        "amount": (pl.arange(0, rows, eager=True).cast(pl.Float64) * 0.017) % 101.0,
        "event_date": pl.Series(
            [f"2026-{((i % 12) + 1):02d}-{((i % 28) + 1):02d}" for i in range(rows)],
            dtype=pl.Utf8,
        ),
    })
    return df.to_arrow()


# --------------------------------------------------------------------------
# Snapshot inspection
# --------------------------------------------------------------------------

def snapshot_state(super_table, table: str) -> dict:
    from supertable.simple_table import SimpleTable

    snap, _ = SimpleTable(super_table, table).get_simple_table_snapshot()
    resources = snap.get("resources") or []
    sizes = [int(r.get("file_size") or 0) for r in resources]
    return {
        "files": len(resources),
        "bytes": sum(sizes),
        "rows": sum(int(r.get("rows") or 0) for r in resources),
        "tombstone_rows": int(snap.get("tombstone_rows") or 0),
        "stats_rows": int(snap.get("stats_rows") or 0),
        "snapshot_version": int(snap.get("snapshot_version") or 0),
    }


# --------------------------------------------------------------------------
# Run
# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batches", type=int, default=100)
    ap.add_argument("--rows", type=int, default=100_000)
    ap.add_argument("--out", default="/tmp/write_telemetry.parquet")
    ap.add_argument("--table", default="audit_facts")
    ap.add_argument("--keep", action="store_true",
                    help="leave the table behind for inspection")
    args = ap.parse_args()

    install_recorder()

    from supertable.data_writer import DataWriter
    from supertable.redis_catalog import RedisCatalog

    suffix = uuid.uuid4().hex[:8]
    org = f"tele{suffix}"
    sup = f"telesup{suffix}"
    role = "superadmin"

    print(f"org={org} super={sup} table={args.table}")
    print(f"appending {args.batches} batches x {args.rows:,} rows "
          f"= {args.batches * args.rows:,} rows\n")

    writer = DataWriter(super_name=sup, organization=org)
    catalog = RedisCatalog()

    per_write: list[dict] = []
    t_start = time.perf_counter()

    try:
        for i in range(args.batches):
            batch = make_batch(i, args.rows)
            before = len(CAPTURED)
            t0 = time.perf_counter()
            result = writer.write(role, args.table, batch, [])
            wall_ms = (time.perf_counter() - t0) * 1000.0

            payload = CAPTURED[before] if len(CAPTURED) > before else {}
            row = {
                "write_idx": i,
                "wall_ms": wall_ms,
                "inserted": (result or (0, 0, 0, 0))[2],
                "new_resources": payload.get("new_resources", 0),
                "sunset_files": payload.get("sunset_files", 0),
            }
            for stage, secs in (payload.get("timings") or {}).items():
                row[f"t.{stage}"] = float(secs) * 1000.0
            for name, val in (payload.get("counts") or {}).items():
                row[f"c.{name}"] = int(val)
            per_write.append(row)

            if (i + 1) % 10 == 0:
                st = snapshot_state(writer.super_table, args.table)
                print(f"  [{i + 1:3d}/{args.batches}] "
                      f"wall={wall_ms:7.1f}ms  files={st['files']:3d}  "
                      f"rows={st['rows']:>10,}  "
                      f"bytes={st['bytes'] / 1048576:7.1f}MiB  "
                      f"stats_rows={st['stats_rows']:>7,}")

        total_s = time.perf_counter() - t_start
        final = snapshot_state(writer.super_table, args.table)

        print(f"\ntotal {total_s:.1f}s")
        print("final:", final)

        # One row per write; every column is either t.<stage> ms or c.<counter>.
        df = pl.DataFrame(per_write, infer_schema_length=None)
        df.write_parquet(args.out)
        print(f"\nwrote telemetry -> {args.out}  ({df.height} rows, {df.width} cols)")
        return 0
    finally:
        if not args.keep:
            try:
                catalog.delete_super_table(org, sup)
            except Exception as e:
                print(f"cleanup warning: {e}")


if __name__ == "__main__":
    raise SystemExit(main())
