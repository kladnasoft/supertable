#!/usr/bin/env python3
# route: supertable.tests.simulate
"""
Run all lock benchmarks and produce a summary comparison table.

Usage from PyCharm: right-click → Run 'simulate'
Usage from terminal: python3 simulate.py
"""

import subprocess
import sys
import os
import re

_HERE = os.path.dirname(os.path.abspath(__file__))


def run_capture(label: str, cmd: list[str]) -> str:
    """Run a command, print progress, return stdout."""
    print(f"  Running: {label} ...", end="", flush=True)
    result = subprocess.run(cmd, cwd=_HERE, capture_output=True, text=True)
    if result.returncode != 0:
        print(f" FAILED (exit {result.returncode})")
        if result.stderr:
            print(f"    stderr: {result.stderr.strip()}")
        return ""
    print(" done")
    return result.stdout


def parse_latency(output: str) -> dict:
    """Extract Avg/Min/p50/p99/Max from the 'Acquire + release (no I/O)' block."""
    metrics = {}
    block = re.search(r"Acquire \+ release \(no I/O\):\s*\n((?:\s+\w.+\n)+)", output)
    if not block:
        return metrics
    for line in block.group(1).strip().splitlines():
        m = re.match(r"\s*(\w+)\s*:\s*([\d.]+)\s*ms", line)
        if m:
            metrics[m.group(1).lower()] = float(m.group(2))
    return metrics


def parse_contention(output: str) -> dict:
    """Extract Avg/Min/Max wait and thread counts from the SUMMARY block."""
    metrics = {}
    for key, pattern in [
        ("threads", r"Threads attempted\s*:\s*(\d+)"),
        ("success", r"Successful locks\s*:\s*(\d+)"),
        ("avg", r"Avg wait\s*:\s*([\d.]+)s"),
        ("min", r"Min wait\s*:\s*([\d.]+)s"),
        ("max", r"Max wait\s*:\s*([\d.]+)s"),
    ]:
        m = re.search(pattern, output)
        if m:
            metrics[key] = float(m.group(1))
    return metrics


def fmt_ms(val: float | None) -> str:
    return f"{val:.1f} ms" if val is not None else "--"


def fmt_s(val: float | None) -> str:
    return f"{val * 1000:.1f} ms" if val is not None else "--"


def fmt_ratio(file_val: float | None, redis_val: float | None) -> str:
    if file_val and redis_val and redis_val > 0:
        return f"{file_val / redis_val:.1f}x faster"
    return "--"


def print_table(headers: list[str], rows: list[list[str]]) -> None:
    """Print a clean ASCII table."""
    col_widths = [
        max(len(h), max((len(r[i]) for r in rows), default=0)) + 2
        for i, h in enumerate(headers)
    ]

    def row_line(cells):
        return "|" + "|".join(c.center(w) for c, w in zip(cells, col_widths)) + "|"

    sep = "+" + "+".join("-" * w for w in col_widths) + "+"

    print(sep)
    print(row_line(headers))
    print(sep)
    for r in rows:
        print(row_line(r))
    print(sep)


def main() -> None:
    py = sys.executable

    print()
    print("=" * 55)
    print("  LOCK BENCHMARK SUITE")
    print("=" * 55)
    print()

    # --- Run all four benchmarks ---
    file_lat_out = run_capture("File latency", [
        py, "measure_lock_time.py", "--backend", "file", "--iterations", "100",
    ])
    file_con_out = run_capture("File contention", [
        py, "measure_lock_speed.py", "--backend", "file", "--threads", "12", "--hold", "0.5",
    ])
    redis_lat_out = run_capture("Redis latency", [
        py, "measure_lock_time.py", "--backend", "redis", "--iterations", "100",
    ])
    redis_con_out = run_capture("Redis contention", [
        py, "measure_lock_speed.py", "--backend", "redis", "--threads", "12", "--hold", "0.5",
    ])

    # --- Parse ---
    fl = parse_latency(file_lat_out)
    rl = parse_latency(redis_lat_out)
    fc = parse_contention(file_con_out)
    rc = parse_contention(redis_con_out)

    # --- Latency table ---
    print()
    print("  LATENCY (no contention, 100 iterations)")
    print()
    latency_rows = []
    for key in ["avg", "p50", "p99", "min", "max"]:
        f_val = fl.get(key)
        r_val = rl.get(key)
        latency_rows.append([
            key.upper(),
            fmt_ms(f_val),
            fmt_ms(r_val),
            fmt_ratio(f_val, r_val),
        ])
    print_table(["Metric", "File", "Redis", "Redis advantage"], latency_rows)

    # --- Contention table ---
    print()
    print("  CONTENTION (12 threads, 0.5s hold)")
    print()
    contention_rows = [
        ["Threads", str(int(fc.get("threads", 0))), str(int(rc.get("threads", 0))), ""],
        ["Successful", str(int(fc.get("success", 0))), str(int(rc.get("success", 0))), ""],
        ["Avg wait", fmt_s(fc.get("avg")), fmt_s(rc.get("avg")), fmt_ratio(fc.get("avg"), rc.get("avg"))],
        ["Min wait", fmt_s(fc.get("min")), fmt_s(rc.get("min")), fmt_ratio(fc.get("min"), rc.get("min"))],
        ["Max wait", fmt_s(fc.get("max")), fmt_s(rc.get("max")), fmt_ratio(fc.get("max"), rc.get("max"))],
    ]
    print_table(["Metric", "File", "Redis", "Redis advantage"], contention_rows)

    # --- Verdict ---
    print()
    avg_f = fl.get("avg")
    avg_r = rl.get("avg")
    if avg_f and avg_r and avg_r > 0:
        ratio = avg_f / avg_r
        if ratio > 1:
            print(f"  Result: Redis is ~{ratio:.1f}x faster per acquire/release cycle.")
        else:
            print(f"  Result: File is ~{1/ratio:.1f}x faster per acquire/release cycle.")
    else:
        print("  Result: could not compute (missing data).")
    print()


if __name__ == "__main__":
    main()
