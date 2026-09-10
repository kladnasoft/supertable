"""Deterministic read-benchmark dataset.

Shape is chosen so the required selectivities fall out of the data instead of
being asserted about it:

* the rows span exactly ``SPAN_DAYS`` (300) ending at a **fixed** anchor, so a
  30-day window is 10% of the table and a 24-hour window is 1/300 (~0.33%);
* each file covers one contiguous slice of that span, so a time predicate is
  prunable and the number of surviving files is predictable — a 30-day window
  can only touch 10 of 100 files.

The anchor is a constant, never ``now()``.  A wall-clock anchor would move the
window between runs and make every seal unreproducible.

Generation is seeded per file, so the same scale always produces byte-identical
content regardless of how many files are built in parallel or in what order.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pyarrow as pa

from benchmarks import _harness
from benchmarks._harness import BENCH_ORG, BENCH_ROLE, RESULTS_ROOT

# Fixed end of the data's time range.  Everything is relative to this.
ANCHOR = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
SPAN_DAYS = 300

COUNTRIES = ["AT", "DE", "CH", "HU", "SK", "CZ", "PL", "IT", "FR", "ES", "NL", "BE"]
CHANNELS = ["web", "mobile", "partner", "retail", "api"]
DEVICES = ["desktop", "phone", "tablet", "kiosk"]
STATUSES = ["paid", "open", "refunded", "cancelled"]

KEY_COLUMN = "event_id"
TIME_COLUMN = "event_ts"


# Auto-compaction merges files that are "small" — smaller than the table's
# ``max_memory_chunk_size`` — and at the specified shape every generated file
# is well under the 16 MiB default, so 100 writes collapse into a handful of
# files and the benchmark stops measuring the layout it claims to.  Configuring
# the table below the per-file size keeps every file "big", so the gate never
# opens and the file count is the one the suite asked for.  This value only
# affects the write/compaction path; the read path never consults it, so it
# cannot skew what the read scenarios measure.
NO_COMPACT_CHUNK_BYTES = 64 * 1024


@dataclass(frozen=True)
class Scale:
    name: str
    rows: int
    files: int

    @property
    def rows_per_file(self) -> int:
        return self.rows // self.files

    @property
    def days_per_file(self) -> float:
        return SPAN_DAYS / self.files

    @property
    def table(self) -> str:
        return f"perf_read_{self.name}"


SCALES: Dict[str, Scale] = {
    # The specified shape: 10 million rows across 100 underlying files.
    "full": Scale(name="full", rows=10_000_000, files=100),
    # Same time-shape and selectivities, small enough to validate the harness.
    "smoke": Scale(name="smoke", rows=200_000, files=20),
}


def window_start(days: float) -> datetime:
    """Start of a look-back window measured from the fixed anchor."""
    return ANCHOR - timedelta(days=days)


def sql_ts(value: datetime) -> str:
    """Render a timestamp as a SQL literal."""
    return value.strftime("%Y-%m-%d %H:%M:%S")


def fingerprint(scale: Scale) -> str:
    """Identity of the generated content.

    Any change to shape, schema or seeding must change this, because a result
    file measured against different data is not comparable.
    """
    import hashlib

    spec = {
        "rows": scale.rows,
        "files": scale.files,
        "span_days": SPAN_DAYS,
        "anchor": ANCHOR.isoformat(),
        "columns": [
            KEY_COLUMN, TIME_COLUMN, "dim_country", "dim_channel", "dim_device",
            "status", "user_id", "measure_amount", "measure_qty",
        ],
        "generator": "v1",
    }
    return hashlib.sha256(
        json.dumps(spec, sort_keys=True).encode()
    ).hexdigest()[:16]


def build_file_batch(scale: Scale, file_index: int) -> pa.Table:
    """Generate one file's rows.

    Seeded from the file index alone so any file can be regenerated on its own
    and the whole dataset is order-independent.
    """
    rng = np.random.default_rng(20260101 + file_index)
    n = scale.rows_per_file
    start_id = file_index * n

    # This file owns a contiguous slice of the span, which is what makes a
    # time predicate prunable down to a known number of files.
    day_lo = file_index * scale.days_per_file
    day_hi = (file_index + 1) * scale.days_per_file
    span_start = ANCHOR - timedelta(days=SPAN_DAYS - day_lo)
    seconds = int((day_hi - day_lo) * 86400)

    offsets = rng.integers(0, max(1, seconds), size=n, dtype=np.int64)
    offsets.sort()                                # in-file time order, stable
    base_us = int(span_start.timestamp() * 1_000_000)
    timestamps = base_us + offsets * 1_000_000

    event_id = np.arange(start_id, start_id + n, dtype=np.int64)
    country = rng.integers(0, len(COUNTRIES), size=n)
    channel = rng.integers(0, len(CHANNELS), size=n)
    device = rng.integers(0, len(DEVICES), size=n)
    status = rng.integers(0, len(STATUSES), size=n)
    # High cardinality on purpose: COUNT(DISTINCT) is a different cost shape
    # from the low-cardinality group-bys.
    user_id = rng.integers(0, 250_000, size=n, dtype=np.int64)
    amount = np.round(rng.gamma(shape=2.0, scale=45.0, size=n), 2)
    qty = rng.integers(1, 25, size=n, dtype=np.int64)

    return pa.table({
        KEY_COLUMN: pa.array(event_id, type=pa.int64()),
        TIME_COLUMN: pa.array(timestamps, type=pa.timestamp("us", tz="UTC")),
        "dim_country": pa.array([COUNTRIES[i] for i in country], type=pa.string()),
        "dim_channel": pa.array([CHANNELS[i] for i in channel], type=pa.string()),
        "dim_device": pa.array([DEVICES[i] for i in device], type=pa.string()),
        "status": pa.array([STATUSES[i] for i in status], type=pa.string()),
        "user_id": pa.array(user_id, type=pa.int64()),
        "measure_amount": pa.array(amount, type=pa.float64()),
        "measure_qty": pa.array(qty, type=pa.int64()),
    })


def _marker_path(profile: str, scale: Scale) -> Path:
    RESULTS_ROOT.mkdir(parents=True, exist_ok=True)
    return RESULTS_ROOT / f".dataset-{profile}-{scale.name}.json"


def existing_dataset(profile: str, scale: Scale) -> Optional[Dict[str, Any]]:
    """Return a previously built dataset only if it still matches in full.

    The fingerprint covers the *requested* shape, not the shape that was
    actually achieved, and those can differ: a dataset built before the
    no-compaction config was in place ends up merged into far fewer files.
    Reusing one of those silently measures a layout the read scenarios are not
    written against, so the achieved file count is verified too.
    """
    marker = _marker_path(profile, scale)
    if not marker.is_file():
        return None
    try:
        record = json.loads(marker.read_text())
    except Exception:
        return None
    if record.get("fingerprint") != fingerprint(scale):
        return None
    if record.get("files_live") != scale.files:
        return None
    if record.get("rows_in_snapshot") != scale.rows:
        return None
    return record


def storage_shape(scale: Scale) -> Dict[str, Any]:
    """Physical shape of the built table, straight from its snapshot.

    The brief asks for a specific number of underlying files, so the suite
    reports what it actually produced rather than assuming one write became
    one file — auto-compaction is free to merge, and that would change what
    the pruning scenarios are measuring.
    """
    try:
        from supertable.simple_table import SimpleTable
        from supertable.super_table import SuperTable

        super_table = SuperTable(super_name=_harness.BENCH_SUPER, organization=BENCH_ORG)
        snapshot, _ = SimpleTable(
            super_table=super_table, simple_name=scale.table,
        ).get_simple_table_snapshot()
        resources = snapshot.get("resources") or []
        return {
            "files_live": len(resources),
            "rows_in_snapshot": sum(int(r.get("rows") or 0) for r in resources),
            "bytes_total": sum(int(r.get("file_size") or 0) for r in resources),
            "snapshot_version": snapshot.get("snapshot_version"),
        }
    except Exception as exc:
        return {"files_live": None, "error": f"{type(exc).__name__}: {exc}"}


def build(profile: str, scale: Scale, *, rebuild: bool = False,
          log=print) -> Dict[str, Any]:
    """Create the read dataset, or reuse an identical one already present.

    Reuse is what keeps repeat runs cheap: the 10-million-row table is built
    once per machine and every later run starts measuring in seconds.
    """
    record = None if rebuild else existing_dataset(profile, scale)
    if record is not None:
        log(f"dataset {scale.table}: reusing existing build "
            f"({record['rows']:,} rows, {record.get('files_written')} writes, "
            f"fingerprint {record['fingerprint']})")
        record["reused"] = True
        return record

    from supertable.data_writer import DataWriter
    from supertable.super_table import SuperTable

    SuperTable(super_name=_harness.BENCH_SUPER, organization=BENCH_ORG)
    writer = DataWriter(super_name=_harness.BENCH_SUPER, organization=BENCH_ORG)

    # Any path that reaches here is building from scratch — either the caller
    # asked to, or the existing dataset was rejected as unusable.  Dropping is
    # not optional: the writes below are appends, so building on top of a
    # partial or mismatched table silently doubles the row count instead of
    # replacing it.
    _drop_table(scale.table, log=log)

    # Keep every generated file above the "small file" line so auto-compaction
    # leaves the layout alone and the table really has the file count the read
    # scenarios are written against.
    writer.configure_table(
        role_name=BENCH_ROLE,
        simple_name=scale.table,
        max_memory_chunk_size=NO_COMPACT_CHUNK_BYTES,
    )

    log(f"dataset {scale.table}: building {scale.rows:,} rows "
        f"in {scale.files} writes of {scale.rows_per_file:,}")

    import time as _time

    started = _time.perf_counter()
    written_rows = 0
    for i in range(scale.files):
        batch = build_file_batch(scale, i)
        # Append-only: no overwrite columns, so each write lands its own file.
        writer.write(
            role_name=BENCH_ROLE,
            simple_name=scale.table,
            data=batch,
            overwrite_columns=[],
        )
        written_rows += batch.num_rows
        if (i + 1) % max(1, scale.files // 10) == 0:
            log(f"  ... {i + 1}/{scale.files} writes, {written_rows:,} rows")

    elapsed = _time.perf_counter() - started
    record = {
        "table": scale.table,
        "scale": scale.name,
        "rows": written_rows,
        "files_written": scale.files,
        "span_days": SPAN_DAYS,
        "anchor": ANCHOR.isoformat(),
        "fingerprint": fingerprint(scale),
        "build_seconds": round(elapsed, 2),
        "reused": False,
    }
    record.update(storage_shape(scale))
    record["layout_as_requested"] = record.get("files_live") == scale.files
    _marker_path(profile, scale).write_text(json.dumps(record, indent=2) + "\n")
    log(f"dataset {scale.table}: built in {elapsed:.1f}s — "
        f"{record.get('files_live')} live files, "
        f"{record.get('rows_in_snapshot'):,} rows, "
        f"{(record.get('bytes_total') or 0) / 1024 ** 2:.1f} MiB")
    if not record["layout_as_requested"]:
        # Not fatal, but the pruning scenarios are written against a known file
        # count — say so loudly rather than quietly measuring something else.
        log(f"  WARNING: asked for {scale.files} files, snapshot exposes "
            f"{record.get('files_live')}. Auto-compaction has merged the "
            f"layout; pruning results describe the merged shape.")
    return record


def _drop_table(table: str, log=print) -> None:
    try:
        from supertable.simple_table import SimpleTable
        from supertable.super_table import SuperTable

        super_table = SuperTable(super_name=_harness.BENCH_SUPER, organization=BENCH_ORG)
        SimpleTable(super_table=super_table, simple_name=table).delete(
            role_name=BENCH_ROLE
        )
        log(f"  dropped {table}")
    except Exception as exc:
        log(f"  drop {table} skipped: {type(exc).__name__}")


def sample_ids(scale: Scale, count: int, *, seed: int = 7) -> List[int]:
    """A reproducible scatter of primary keys across the whole table.

    Deliberately spread over the full id range so the lookup cannot be served
    by one file — this is the random-access case, not a range scan.
    """
    rng = np.random.default_rng(seed)
    return sorted(int(v) for v in rng.choice(scale.rows, size=count, replace=False))
