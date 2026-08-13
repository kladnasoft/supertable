"""Reproducible benchmark for the primary immutable Parquet write path.

The ``before`` case includes both costs removed by the audit: sorting a
batch-constant timestamp and the post-upload ``size()`` call. The ``after``
case skips that no-op sort and uses the exact uploaded payload length.
Use ``--size-latency-ms`` to model the object-store HEAD latency measured in
the deployment being evaluated; zero measures local codec overhead only.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from unittest.mock import patch

import polars as pl

from supertable.processing import write_parquet_and_collect_resources


class BenchmarkStorage:
    def __init__(self, size_latency_s: float):
        self.size_latency_s = size_latency_s
        self.objects: dict[str, bytes] = {}
        self.size_calls = 0

    def makedirs(self, _path: str) -> None:
        pass

    def write_bytes(self, path: str, data: bytes) -> None:
        self.objects[path] = data

    def size(self, path: str) -> int:
        self.size_calls += 1
        time.sleep(self.size_latency_s)
        return len(self.objects[path])


def _frame(rows: int, columns: int) -> pl.DataFrame:
    values = pl.arange(0, rows, eager=True)
    frame = pl.DataFrame({f"column_{i}": values for i in range(columns)})
    return frame.with_columns(
        pl.lit(1_700_000_000_000_000).cast(pl.Datetime("us")).alias("__timestamp__")
    )


def run(rows: int, columns: int, repeats: int, size_latency_ms: float) -> dict:
    frame = _frame(rows, columns)
    samples = {"before": [], "after": []}
    calls = {"before": [], "after": []}
    encoded_bytes = 0

    # Alternate ordering so thermal/cache drift does not favor one case.
    for iteration in range(repeats):
        order = ("before", "after") if iteration % 2 == 0 else ("after", "before")
        for case in order:
            storage = BenchmarkStorage(size_latency_ms / 1000.0)
            resources: list[dict] = []
            started = time.perf_counter()
            write_frame = (
                frame.sort("__timestamp__") if case == "before" else frame
            )
            with patch("supertable.processing._get_storage", return_value=storage):
                write_parquet_and_collect_resources(
                    write_frame, [], "/benchmark", resources, compression_level=1
                )
                if case == "before":
                    # Exact operation removed from the historical implementation.
                    resources[0]["file_size"] = storage.size(resources[0]["file"])
            samples[case].append(time.perf_counter() - started)
            calls[case].append(storage.size_calls)
            encoded_bytes = resources[0]["file_size"]

    before = statistics.median(samples["before"])
    after = statistics.median(samples["after"])
    return {
        "rows": rows,
        "columns": columns + 1,
        "compression": "zstd:1",
        "encoded_bytes": encoded_bytes,
        "size_latency_ms": size_latency_ms,
        "repeats": repeats,
        "before": {
            "median_seconds": before,
            "runs_seconds": samples["before"],
            "size_calls_per_write": calls["before"],
        },
        "after": {
            "median_seconds": after,
            "runs_seconds": samples["after"],
            "size_calls_per_write": calls["after"],
        },
        "improvement": {
            "seconds": before - after,
            "percent": ((before - after) / before * 100.0) if before else 0.0,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=250_000)
    parser.add_argument("--columns", type=int, default=8)
    parser.add_argument("--repeats", type=int, default=7)
    parser.add_argument("--size-latency-ms", type=float, default=10.0)
    args = parser.parse_args()
    print(json.dumps(run(args.rows, args.columns, args.repeats, args.size_latency_ms), indent=2))


if __name__ == "__main__":
    main()
