from __future__ import annotations

import argparse
from datetime import datetime, timezone
import importlib
import importlib.metadata
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import traceback

from .compare import compare_result
from .dataset import build_dataset, arrow_schemas
from .models import QueryCase, load_saved_cases
from .report import write_json, write_report
from .runtime import ORG, SUPER, isolated_redis, configure_process, prepare_tables, reference_connections


def execute_case(case, mode):
    from supertable.data_reader import DataReader, Status, engine, query_sql
    reader = DataReader(SUPER, ORG, case.sql, source="sql_matrix")
    started = time.perf_counter()
    run = {"mode": mode, "status": "fail"}
    run["session_timezone"] = getattr(case, "session_timezone", "UTC")
    try:
        if mode in ("stream", "post_compact_stream"):
            handle = reader.stream(case.role, engine=engine.DUCKDB, batch_rows=17)
            try:
                columns = list(handle.schema.names)
                import polars as pl
                types = {k: str(v) for k, v in pl.from_arrow(handle.schema.empty_table()).schema.items()}
                rows = []
                batches = 0
                for batch in handle.batches():
                    rows.extend(tuple(row[col] for col in columns) for row in batch.to_pylist())
                    batches += 1
                run["batches"] = batches
            finally:
                handle.close()
        elif mode == "query_sql":
            columns, rows, column_meta = query_sql(ORG, SUPER, case.sql, 100000, engine.DUCKDB, case.role, source="sql_matrix")
            types = {item["name"]: item["type"] for item in column_meta}
        else:
            chosen = engine.AUTO if mode == "auto" else engine.DUCKDB
            frame, status, message = reader.execute(case.role, engine=chosen, fullscan=mode == "fullscan")
            if status is not Status.OK:
                raise RuntimeError(message)
            columns, rows = frame.columns, frame.rows()
            types = {k: str(v) for k, v in frame.schema.items()}
        run.update(row_count=len(rows), columns=columns, types=types)
        if case.error_contains:
            run["mismatch"] = {"kind": "unexpected_success", "expected_error_contains": case.error_contains}
        else:
            run["mismatch"] = compare_result(case, columns, rows, types)
            stats = {}
            for entry in getattr(reader.plan_stats, "stats", []) or []:
                if isinstance(entry, dict):
                    stats.update(entry)
            run["plan_stats"] = stats
            if not run["mismatch"] and mode == "pruned" and case.min_pruned_files:
                actual = stats.get("FILES_PRUNED", 0)
                if actual < case.min_pruned_files:
                    run["mismatch"] = {"kind": "pruning", "minimum": case.min_pruned_files, "actual": actual}
        if run.get("mismatch") is None:
            run["status"] = "pass"
        else:
            run["actual_rows"] = rows
    except Exception as exc:
        run["error"] = f"{type(exc).__name__}: {exc}"
        if case.error_contains and any(fragment.lower() in str(exc).lower() for fragment in case.error_contains):
            run["status"] = "pass"
        else:
            run["mismatch"] = {"kind": "unexpected_error", "expected_error_contains": case.error_contains}
            run["traceback"] = traceback.format_exc(limit=8)
    run["duration_ms"] = round((time.perf_counter() - started) * 1000, 3)
    return run


def reference_case(case, connections):
    if case.error_contains:
        return {"status": "not_applicable", "reason": "expected native admission/authorization rejection"}
    con = connections[case.role]
    try:
        result = con.execute(case.sql)
        columns = [col[0] for col in result.description]
        rows = result.fetchall()
        mismatch = compare_result(case, columns, rows)
        out = {"status": "fail" if mismatch else "pass", "row_count": len(rows)}
        if mismatch:
            out.update(mismatch=mismatch, actual_rows=rows, columns=columns)
        return out
    except Exception as exc:
        return {"status": "fail", "error": f"{type(exc).__name__}: {exc}"}


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("audit/sql_read_matrix"))
    parser.add_argument("--groups", nargs="+", default=["scalar", "relational", "advanced", "controls", "syntax"])
    parser.add_argument("--case", action="append", dest="case_ids")
    parser.add_argument("--expectations-from", type=Path, help="Use a saved case book to detect changed test expectations")
    parser.add_argument("--skip-lifecycle", action="store_true", help="Omit the additional mutation and fault scenarios")
    parser.add_argument("--session-timezone", default="UTC", help="Run cases for this zone in a fresh process with TZ set before DuckDB import")
    parser.add_argument("--modes", nargs="+", default=["pruned", "fullscan", "stream", "auto", "query_sql"],
                        choices=["pruned", "fullscan", "stream", "auto", "query_sql"])
    args = parser.parse_args(argv)
    if os.environ.get("TZ") != args.session_timezone:
        environment = dict(os.environ, TZ=args.session_timezone)
        arguments = sys.argv[1:] if argv is None else list(argv)
        os.execve(sys.executable, [sys.executable, "-m", "scripts.sql_read_matrix", *arguments], environment)
    output = args.output.resolve()
    if (output / "dataset.json").exists():
        raise ValueError("Output already contains a dataset; choose a fresh directory to preserve the previous run")
    output.mkdir(parents=True, exist_ok=True)
    repo = Path(__file__).resolve().parents[2]
    expanded = any(name in ("temporal", "timezones", "temporal_coercion", "alias_followup") for name in args.groups)
    data = build_dataset(include_temporal=expanded)
    cases = []
    for name in args.groups:
        cases.extend(importlib.import_module(f"scripts.sql_read_matrix.cases_{name}").build_cases(data))
    if args.expectations_from:
        cases = load_saved_cases(args.expectations_from)
    if len({c.case_id for c in cases}) != len(cases):
        raise ValueError("Duplicate case IDs")
    cases = [case for case in cases if case.session_timezone == args.session_timezone]
    if not cases:
        raise ValueError("No cases selected for this session timezone")
    if args.case_ids:
        wanted = set(args.case_ids)
        cases = [c for c in cases if c.case_id in wanted]
        if {c.case_id for c in cases} != wanted:
            raise ValueError("Unknown case ID")
    for case in cases:
        if any(len(row) != len(case.columns) for row in case.expected):
            raise ValueError(f"Expected-row width mismatch in {case.case_id}")
    write_json(output / "dataset.json", data)
    write_json(output / "expectations.json", [c.__dict__ for c in cases])
    metadata = {
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo, text=True).strip(),
        "dirty_paths": subprocess.check_output(["git", "status", "--porcelain"], cwd=repo, text=True).splitlines(),
        "versions": {name: importlib.metadata.version(name) for name in ["duckdb", "polars", "pyarrow", "redis"]},
        "sqlglot_version": importlib.metadata.version("sqlglot"),
        "seed": "arithmetic dataset v1; no randomness", "storage": "LOCAL", "spark_tested": False,
        "python": platform.python_version(), "platform": platform.platform(),
        "groups": args.groups, "selected_case_ids": args.case_ids, "modes": args.modes,
        "session_timezone": args.session_timezone,
        "expectations_from": str(args.expectations_from.resolve()) if args.expectations_from else None,
        "dataset_sha256": hashlib.sha256((output / "dataset.json").read_bytes()).hexdigest(),
        "expectations_sha256": hashlib.sha256((output / "expectations.json").read_bytes()).hexdigest(),
        "harness_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                           for p in sorted(Path(__file__).parent.glob("*.py"))},
    }
    from .source_fingerprint import capture
    metadata["source_before"] = capture(repo)
    write_json(output / "source_fingerprint.json", metadata["source_before"])
    from zoneinfo import ZoneInfo
    zone_probe = QueryCase("session_timezone_probe", "session_verification",
        "SELECT current_setting('TimeZone') AS zone, "
        "CAST(TIMESTAMPTZ '2024-01-01 00:00:00+00' AS DATE) AS local_day FROM orders LIMIT 1",
        ("zone", "local_day"), [(args.session_timezone,
            datetime(2024, 1, 1, tzinfo=timezone.utc).astimezone(ZoneInfo(args.session_timezone)).date())],
        session_timezone=args.session_timezone)
    write_json(output / "session_expectation.json", zone_probe.__dict__)
    print(f"Prepared {len(cases)} independently asserted cases before execution", flush=True)
    results = []
    from .ingest_probes import run_probes
    run_probes(output)
    with isolated_redis() as (port, redis_meta):
        metadata["redis"] = redis_meta
        metadata["package_version"] = configure_process(output / "fixtures", port, output / "engine.log", args.session_timezone)
        schemas = arrow_schemas(include_temporal=expanded)
        writer, catalog, setup_checks = prepare_tables(data, schemas)
        manifest = {name: catalog.get_leaf(ORG, SUPER, name) for name in data}
        write_json(output / "fixture_manifest.json", {"tables": manifest, "setup_checks": setup_checks})
        print("Wrote real Parquet dataset, mutations, schema evolution, and roles", flush=True)
        connections = reference_connections(data, schemas, args.session_timezone)
        session_checks = []
        try:
            for index, case in enumerate(cases, 1):
                result = {"case_id": case.case_id, "reference": reference_case(case, connections), "runs": []}
                for mode in args.modes:
                    result["runs"].append(execute_case(case, mode))
                results.append(result)
                if index == 1:
                    for mode in args.modes:
                        check = execute_case(zone_probe, mode)
                        session_checks.append({"case_id": "session_timezone_" + mode, **check})
                    from supertable.engine.duckdb import _shared_state
                    base = _shared_state().get("con")
                    observed = {"base": None, "cursor": None}
                    if base is not None:
                        observed["base"] = base.execute("SELECT current_setting('TimeZone')").fetchone()[0]
                        cursor = base.cursor()
                        try:
                            observed["cursor"] = cursor.execute("SELECT current_setting('TimeZone')").fetchone()[0]
                        finally:
                            cursor.close()
                    metadata["observed_session_timezones"] = observed
                    session_checks.append({"case_id": "session_base_and_cursor", "observed": observed,
                        "status": "pass" if all(value == args.session_timezone for value in observed.values()) else "fail"})
                    write_json(output / "session_verification.json", session_checks)
                if index % 25 == 0 or index == len(cases):
                    write_json(output / "progress.json", {"completed": index, "results": results})
                    failures = sum(r["status"] == "fail" for item in results for r in item["runs"])
                    oracle_issues = sum(item["reference"]["status"] == "fail" for item in results)
                    print(f"{index}/{len(cases)} cases; native failures={failures}; oracle disagreements={oracle_issues}", flush=True)
            compact_cases = [c for c in cases if not c.error_contains and any(t in c.sql.lower() for t in ("ledger", "evolving"))]
            extra = [*setup_checks, *session_checks]
            if not args.skip_lifecycle:
                from .lifecycle import run_checks
                print("Running mutation, cache refresh, and read-metadata fault checks", flush=True)
                extra.extend(run_checks(writer, catalog, output, execute_case))
            if compact_cases:
                for name in ("ledger", "evolving"):
                    try:
                        summary = writer.compact("superadmin", name, small_only=False)
                        extra.append({"case_id": "compact_" + name, "status": "pass", "summary": summary})
                    except Exception as exc:
                        extra.append({"case_id": "compact_" + name, "status": "fail", "error": f"{type(exc).__name__}: {exc}"})
                by_id = {r["case_id"]: r for r in results}
                for case in compact_cases:
                    for mode in ("post_compact", "post_compact_stream"):
                        by_id[case.case_id]["runs"].append(execute_case(case, mode))
            metadata["finished_utc"] = datetime.now(timezone.utc).isoformat()
            metadata["source_after"] = capture(repo)
            extra.append({"case_id": "source_stability", "status": "pass" if metadata["source_before"] == metadata["source_after"] else "fail"})
            summary = write_report(output, metadata, cases, results, extra)
            print(json.dumps(summary, indent=2), flush=True)
            print(f"Report: {output / 'REPORT.md'}", flush=True)
            return int(bool(summary["failed"] or summary["independent_oracle_disagreements"] or any(c["status"] == "fail" for c in extra)))
        finally:
            for con in connections.values():
                con.close()
            from supertable.engine.duckdb import reset_shared_duckdb_state
            from supertable.engine.engine_common import reset_pooled_duckdb_connections
            from supertable.redis_connector import close_all_redis_clients
            reset_shared_duckdb_state()
            reset_pooled_duckdb_connections()
            close_all_redis_clients()


if __name__ == "__main__":
    raise SystemExit(main())
