"""Delete-load telemetry harness for the SuperTable write path.

Builds a table, then deletes from it in batches until a target row count is
gone, capturing the full ``Profiler`` payload of every delete.  This is the
workload the append audit could not measure: an append-only load tombstones
nothing, so ``build_tombstone`` / ``compact_tombstones`` / the deletion-vector
carry-forward never do any work there.

Deletes go through ``write(..., overwrite_columns=[key], delete_only=True)``
with a frame carrying only the key column.

Keys are sampled at random across the whole table on purpose: that is the
worst case for stats pruning (the probe range spans every file, so no file can
be excluded) and it is what "delete these specific records" looks like. A
range-shaped delete would prune and is a different measurement.

    python scripts/delete_telemetry_audit.py --rows 10000000 --target-deleted 5000000
"""
from __future__ import annotations

import argparse
import os
import time
import uuid

os.environ.setdefault("STORAGE_TYPE", "LOCAL")

import numpy as np
import polars as pl
import pyarrow as pa

CAPTURED: list[dict] = []


class _Recorder:
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


def make_batch(batch_idx: int, rows: int) -> pa.Table:
    base = batch_idx * rows
    return pl.DataFrame({
        "id": pl.arange(base, base + rows, eager=True),
        "category": pl.Series([f"cat_{i % 32}" for i in range(rows)], dtype=pl.Utf8),
        "name": pl.Series(
            [f"entity-{(base + i) % 250_000:08d}" for i in range(rows)], dtype=pl.Utf8),
        "value": (pl.arange(0, rows, eager=True).cast(pl.Float64) * 1.5) % 9973.0,
        "amount": (pl.arange(0, rows, eager=True).cast(pl.Float64) * 0.017) % 101.0,
    }).to_arrow()


def snapshot_state(super_table, table: str) -> dict:
    from supertable.simple_table import SimpleTable

    snap, _ = SimpleTable(super_table, table).get_simple_table_snapshot()
    res = snap.get("resources") or []
    return {
        "files": len(res),
        "bytes": sum(int(r.get("file_size") or 0) for r in res),
        "physical_rows": sum(int(r.get("rows") or 0) for r in res),
        "tombstone_rows": int(snap.get("tombstone_rows") or 0),
        "stats_rows": int(snap.get("stats_rows") or 0),
    }


def _row(idx: int, kind: str, wall_ms: float, payload: dict, keys: int, st: dict) -> dict:
    row = {
        "op_idx": idx,
        "kind": kind,
        "wall_ms": wall_ms,
        "keys_in_batch": keys,
        "deleted": payload.get("deleted", 0),
        "new_resources": payload.get("new_resources", 0),
        "sunset_files": payload.get("sunset_files", 0),
        "files_after": st["files"],
        "bytes_after": st["bytes"],
        "physical_rows_after": st["physical_rows"],
        "tombstone_rows_after": st["tombstone_rows"],
    }
    for stage, secs in (payload.get("timings") or {}).items():
        row[f"t.{stage}"] = float(secs) * 1000.0
    for name, val in (payload.get("counts") or {}).items():
        row[f"c.{name}"] = int(val)
    return row


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    ap.add_argument("--batch-rows", type=int, default=100_000)
    ap.add_argument("--target-deleted", type=int, default=5_000_000)
    ap.add_argument("--min-del", type=int, default=10_000)
    ap.add_argument("--max-del", type=int, default=200_000)
    ap.add_argument("--seed", type=int, default=1234)
    ap.add_argument("--out", default="/tmp/delete_telemetry.parquet")
    ap.add_argument("--table", default="del_facts")
    args = ap.parse_args()

    install_recorder()
    from supertable.data_writer import DataWriter
    from supertable.redis_catalog import RedisCatalog

    sfx = uuid.uuid4().hex[:8]
    org, sup, role = f"del{sfx}", f"delsup{sfx}", "superadmin"
    writer = DataWriter(super_name=sup, organization=org)
    catalog = RedisCatalog()
    per_op: list[dict] = []

    try:
        # ---- build -------------------------------------------------------
        n_batches = args.rows // args.batch_rows
        print(f"building {args.rows:,} rows in {n_batches} appends...")
        t0 = time.perf_counter()
        for i in range(n_batches):
            writer.write(role, args.table, make_batch(i, args.batch_rows), [])
        st = snapshot_state(writer.super_table, args.table)
        print(f"  built in {time.perf_counter() - t0:.1f}s -> {st}\n")
        CAPTURED.clear()

        # ---- delete ------------------------------------------------------
        rng = np.random.default_rng(args.seed)
        order = rng.permutation(args.rows)      # sample keys without replacement
        cursor = 0
        total_deleted = 0
        op = 0
        print(f"deleting until {args.target_deleted:,} rows are gone "
              f"(batches of {args.min_del:,}-{args.max_del:,}, random keys)\n")
        t_del = time.perf_counter()

        while total_deleted < args.target_deleted:
            size = int(rng.integers(args.min_del, args.max_del + 1))
            size = min(size, args.target_deleted - total_deleted, args.rows - cursor)
            if size <= 0:
                break
            keys = order[cursor:cursor + size]
            cursor += size

            batch = pl.DataFrame({"id": pl.Series(keys, dtype=pl.Int64)}).to_arrow()
            before = len(CAPTURED)
            t = time.perf_counter()
            result = writer.write(role, args.table, batch, ["id"], delete_only=True)
            wall_ms = (time.perf_counter() - t) * 1000.0

            payload = CAPTURED[before] if len(CAPTURED) > before else {}
            st = snapshot_state(writer.super_table, args.table)
            per_op.append(_row(op, "delete", wall_ms, payload, size, st))
            total_deleted += (result or (0, 0, 0, 0))[3]
            op += 1

            print(f"  [{op:3d}] keys={size:>7,} deleted={(result or (0,0,0,0))[3]:>7,} "
                  f"total={total_deleted:>9,}  wall={wall_ms:8.1f}ms  "
                  f"files={st['files']:3d} dv_rows={st['tombstone_rows']:>9,} "
                  f"phys={st['physical_rows']:>10,}")

        print(f"\ndeleted {total_deleted:,} rows in {op} ops, "
              f"{time.perf_counter() - t_del:.1f}s")
        print("final:", snapshot_state(writer.super_table, args.table))

        df = pl.DataFrame(per_op, infer_schema_length=None)
        df.write_parquet(args.out)
        print(f"\nwrote telemetry -> {args.out}  ({df.height} rows, {df.width} cols)")
        return 0
    finally:
        try:
            catalog.delete_super_table(org, sup)
        except Exception as e:
            print(f"cleanup warning: {e}")


if __name__ == "__main__":
    raise SystemExit(main())
