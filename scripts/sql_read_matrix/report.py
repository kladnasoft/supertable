from __future__ import annotations

from collections import Counter, defaultdict
import csv
import json
from pathlib import Path

from .compare import json_default


def write_json(path, value):
    Path(path).write_text(json.dumps(value, default=json_default, ensure_ascii=False, indent=2) + "\n")


def write_report(output, metadata, cases, results, extra=None):
    by_id = {case.case_id: case for case in cases}
    executions = [run for result in results for run in result["runs"]]
    failed = [result for result in results if any(run["status"] != "pass" for run in result["runs"])]
    oracle_issues = [result for result in results if result.get("reference", {}).get("status") == "fail"]
    summary = {
        "query_cases": len(cases), "distinct_sql": len({c.sql for c in cases}),
        "expected_success_cases": sum(not c.error_contains for c in cases),
        "expected_rejection_cases": sum(bool(c.error_contains) for c in cases),
        "executions": len(executions), "passed": sum(r["status"] == "pass" for r in executions),
        "failed": sum(r["status"] == "fail" for r in executions),
        "failing_query_cases": len(failed), "independent_oracle_disagreements": len(oracle_issues),
        "additional_checks": len(extra or []),
        "additional_check_passes": sum(r["status"] == "pass" for r in extra or []),
        "additional_check_failures": sum(r["status"] == "fail" for r in extra or []),
        "categories": len({c.category for c in cases}),
        "modes": dict(Counter(r["mode"] for r in executions)),
        "categories_with_failures": dict(Counter(by_id[r["case_id"]].category for r in failed)),
        "positive_query_failures": sum(not by_id[r["case_id"]].error_contains for r in failed),
        "rejection_message_mismatches": sum(bool(by_id[item["case_id"]].error_contains) and
            run["status"] == "fail" and "error" in run for item in results for run in item["runs"]),
        "unexpected_rejection_case_successes": sum(bool(by_id[item["case_id"]].error_contains) and
            run.get("mismatch", {}).get("kind") == "unexpected_success" for item in results for run in item["runs"]),
    }
    write_json(output / "results.json", {"metadata": metadata, "summary": summary, "results": results, "additional_checks": extra or []})
    fail_dir = output / "failures"
    fail_dir.mkdir(exist_ok=True)
    for result in failed + [r for r in oracle_issues if r not in failed]:
        case = by_id[result["case_id"]]
        write_json(fail_dir / (case.case_id + ".json"), {
            "case": case.__dict__, "reference": result.get("reference"), "runs": result["runs"],
            "dataset": "../dataset.json", "fixture_manifest": "../fixture_manifest.json",
            "evidence_run": result.get("evidence_run"),
        })
    with (output / "case_results.csv").open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["case_id", "category", "role", "sql", "expected_rows", "reference", "mode", "status", "actual_rows", "error", "comparison"])
        for result in results:
            case = by_id[result["case_id"]]
            for run in result["runs"]:
                writer.writerow([case.case_id, case.category, case.role, case.sql, len(case.expected),
                    result.get("reference", {}).get("status", "not_run"), run["mode"], run["status"],
                    run.get("row_count"), run.get("error", ""), json.dumps(run.get("mismatch"), default=json_default)])
    lines = ["# SQL engine and read-path validation", "", f"Source revision: `{metadata['revision']}`. Package: `{metadata['package_version']}`.", "",
        "## Method", "", "The dataset and every expected row were constructed before queries ran. Expected results use plain Python arithmetic, grouping, joins, and explicit SQL null rules. They are not golden outputs captured from SuperTable. A direct DuckDB connection over the logical Arrow input independently checks each positive oracle; disagreements are reported separately and must be reviewed before treating a failure as a product defect.", "",
        "Data ingestion was attempted through DataWriter into multiple real local Parquet files, with real Redis metadata and locks in an isolated disposable container. Setup failures are retained as failures, and affected tables use an explicit PyArrow fixture fallback to allow read coverage to continue; see the fixture manifest and additional checks. Ledger replacement/deletion and schema evolution were performed through the writer. Native queries ran with pruning, full scans, Arrow batches, and AUTO routing without Spark registrations. Ordered queries compare complete row order; other queries compare duplicate-preserving multisets. Column names/order are checked, with explicit dtype checks where declared. Floats use absolute and relative tolerances of 1e-9; integers and decimals remain exact.", "",
        "## Results", "", "| Measure | Count |", "| --- | ---: |"]
    if (output / "FINDINGS.md").exists():
        lines[4:4] = ["Read the [reviewed findings and priorities](FINDINGS.md) for the confirmed failure groups and working behavior.", ""]
    for key, value in summary.items():
        if isinstance(value, int):
            lines.append(f"| {key.replace('_', ' ')} | {value} |")
    if summary["rejection_message_mismatches"]:
        lines += ["", f"Of the failed executions, {summary['rejection_message_mismatches']} rejected the query with a different error than expected. These are diagnostic inconsistencies, separately counted from forbidden queries that unexpectedly return results."]
    lines += ["", "## Coverage by category", "", "| Category | Queries | Failing queries |", "| --- | ---: | ---: |"]
    for category, count in sorted(Counter(c.category for c in cases).items()):
        lines.append(f"| {category} | {count} | {summary['categories_with_failures'].get(category, 0)} |")
    lines += ["", "## Reproducible failures", "", "Each linked record includes SQL, the complete expected result, actual failing results, role, mode, and comparison detail.", "",
        "| Case | Category | Failing modes | First failure |", "| --- | --- | --- | --- |"]
    for result in failed:
        case = by_id[result["case_id"]]
        runs = [r for r in result["runs"] if r["status"] == "fail"]
        detail = runs[0].get("error") or json.dumps(runs[0].get("mismatch"), default=json_default, ensure_ascii=False)
        detail = str(detail).replace("|", "\\|").replace("\n", " ")[:220]
        lines.append(f"| [{case.case_id}](failures/{case.case_id}.json) | {case.category} | {', '.join(r['mode'] for r in runs)} | {detail} |")
    if not failed:
        lines.append("| None | | | |")
    lines += ["", "## Oracle review", ""]
    if oracle_issues:
        for result in oracle_issues:
            lines.append(f"- [{result['case_id']}](failures/{result['case_id']}.json): direct DuckDB disagreed with the declared Python expectation. Review before assigning a product defect.")
    else:
        lines.append("Every positive Python oracle agreed with direct DuckDB over the predefined logical input.")
    if metadata.get("evidence_runs"):
        lines += ["", "## Evidence and reviewed reruns", "",
            "The counts above use the latest observation of each query case. Original attempts are preserved below; each result identifies its evidence run. No unchanged passing queries were discarded.", ""]
        for run in metadata["evidence_runs"]:
            lines.append(f"- [{run['path']}]({run['path']}/REPORT.md): {run['summary']['query_cases']} cases, {run['summary']['executions']} executions.")
        lines += ["", *metadata.get("review_notes", []), ""]
    lines += ["", "## Scope and reproduction", "", "Run from the repository root:", "", "```bash", "python -m scripts.sql_read_matrix --output /tmp/supertable-sql-read-results", "```", "",
        "Use `--case CASE_ID --skip-lifecycle` to reproduce one query across modes. Choose a fresh output directory on each run. Docker must be running; the runner creates and removes its own loopback-bound Redis container. The report records the Redis image ID, dependency versions, and dataset/expectation hashes. Fixtures and all expectations are saved before execution. A nonzero exit indicates native failures, oracle disagreements, or additional-check failures.", "",
        "This run covers LOCAL storage and DuckDB. AUTO had no registered Spark cluster and therefore exercises DuckDB routing. It does not validate Spark execution, cloud/object-store protocols, concurrent writers, network fault recovery, or production-scale performance. No production application code is changed by the matrix.", "",
        "Artifacts: [dataset](dataset.json), [expected results](expectations.json), [fixture manifest](fixture_manifest.json), [full results](results.json), [CSV](case_results.csv).", ""]
    if (output / "ingest_probes.json").exists():
        lines += ["Decimal ingestion was also isolated across five Parquet encodings: [probe details](ingest_probes.json). These diagnostic probes are separate from the query-execution counts.", ""]
    if extra:
        lines += ["## Additional lifecycle checks", "", "| Check | Status | Detail |", "| --- | --- | --- |"]
        for item in extra:
            detail = str(item.get("error") or item.get("mismatch") or "").replace("|", "\\|").replace("\n", " ")[:240]
            label = f"[{item['case_id']}]({item['evidence_run']}/results.json)" if item.get("evidence_run") else item['case_id']
            lines.append(f"| {label} | {item['status']} | {detail} |")
        lines += [""]
    (output / "REPORT.md").write_text("\n".join(lines))
    return summary
