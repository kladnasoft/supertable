from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--zones", nargs="+", default=["UTC", "Europe/Budapest", "America/New_York",
        "Asia/Kathmandu", "Australia/Lord_Howe", "Pacific/Apia", "Pacific/Kiritimati"])
    parser.add_argument("--groups", nargs="+", default=["temporal", "timezones", "temporal_coercion"])
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        raise ValueError("Choose a fresh output directory")
    output.mkdir(parents=True)
    repo = Path(__file__).resolve().parents[2]

    def run(zone):
        name = zone.replace("/", "_")
        directory = output / name
        command = [sys.executable, "-m", "scripts.sql_read_matrix", "--groups", *args.groups,
            "--skip-lifecycle", "--session-timezone", zone, "--output", str(directory)]
        print(f"Starting {zone}", flush=True)
        with (output / (name + ".log")).open("w") as log:
            result = subprocess.run(command, cwd=repo, env=dict(os.environ, TZ=zone), stdout=log, stderr=subprocess.STDOUT)
        report = directory / "results.json"
        summary = json.loads(report.read_text())["summary"] if report.exists() else None
        record = {"timezone": zone, "output": str(directory), "exit_code": result.returncode, "summary": summary}
        print(json.dumps(record), flush=True)
        return record

    records = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for future in as_completed([pool.submit(run, zone) for zone in args.zones]):
            records.append(future.result())
            (output / "workers.json").write_text(json.dumps(records, indent=2) + "\n")
    return int(any(row["exit_code"] for row in records))


if __name__ == "__main__":
    raise SystemExit(main())
