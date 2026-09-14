from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import shutil

from .compare import json_default
from .models import load_saved_cases
from .report import write_json, write_report


SEMANTIC_FIELDS = (
    "sql", "columns", "expected", "ordered", "role", "error_contains",
    "expected_types", "min_pruned_files", "session_timezone",
)


def _read(path):
    return json.loads(Path(path).read_text())


def _canonical(value):
    return json.dumps(value, default=json_default, sort_keys=True, ensure_ascii=False)


def _changed_fields(before, after):
    return [field for field in SEMANTIC_FIELDS
            if _canonical(getattr(before, field)) != _canonical(getattr(after, field))]


def _load_run(path):
    path = Path(path).resolve()
    result = _read(path / "results.json")
    cases = load_saved_cases(path / "expectations.json")
    case_ids = {case.case_id for case in cases}
    result_ids = [item["case_id"] for item in result["results"]]
    if len(case_ids) != len(cases) or len(set(result_ids)) != len(result_ids):
        raise ValueError(f"Duplicate query case IDs in {path}")
    if case_ids != set(result_ids) or len(cases) != result["summary"]["query_cases"]:
        raise ValueError(f"Incomplete case results in {path}")
    executions = sum(len(item["runs"]) for item in result["results"])
    if executions != result["summary"]["executions"]:
        raise ValueError(f"Execution count does not match saved summary in {path}")
    for item in result["results"]:
        modes = [run["mode"] for run in item["runs"]]
        if not modes or len(modes) != len(set(modes)):
            raise ValueError(f"Missing or duplicate execution modes in {path}: {item['case_id']}")
        if any(run.get("status") not in ("pass", "fail") for run in item["runs"]):
            raise ValueError(f"Nonfinal execution status in {path}: {item['case_id']}")
    return {"path": path, "saved": result, "cases": {c.case_id: c for c in cases}}


def _check_source(run, allow_unavailable=False):
    metadata = run["saved"]["metadata"]
    before, after = metadata.get("source_before"), metadata.get("source_after")
    if before is None and after is None and allow_unavailable:
        audited_path = run["path"] / "source_start_audited.json"
        return {"status": "unavailable_baseline", "source_before": None, "source_after": None,
                "separate_audited_capture": _read(audited_path) if audited_path.exists() else None,
                "note": "Baseline did not record paired source fingerprints; its separately audited capture cannot establish within-run stability."}
    if before is None or after is None:
        raise ValueError(f"Missing paired source fingerprints in {run['path']}")
    if before != after:
        raise ValueError(f"Production source changed during {run['path']}")
    if not before.get("combined_sha256"):
        raise ValueError(f"Missing combined source fingerprint in {run['path']}")
    return {"status": "stable", "source_before": before, "source_after": after}


def _copy_run(source, target):
    shutil.copytree(source, target, ignore=shutil.ignore_patterns("progress.json", "__pycache__"))


def _issue_verification(historical, failure_map, cases, results, extra, frozen):
    old_cases = {c.case_id: c for c in load_saved_cases(historical / "expectations.json")}
    old_results = _read(historical / "results.json")
    old_extra = {item["case_id"]: item for item in old_results.get("additional_checks", [])}
    current_results = {(item["case_id"], run["mode"]): (item, run)
                       for item in results for run in item["runs"]}
    current_extra = {item["original_case_id"]: item for item in extra}
    frozen_results = {(item["case_id"], run["mode"]): (item, run)
                      for item in frozen["saved"]["results"] for run in item["runs"]}
    records = []
    seen = set()
    for original in failure_map["failures"]:
        cid = original["case_id"]
        key = (original["kind"], cid, original.get("mode"))
        if key in seen:
            raise ValueError(f"Historical failure is mapped more than once: {key}")
        seen.add(key)
        item = {"issue": original["issue"], "kind": original["kind"], "case_id": cid,
                "mode": original.get("mode"), "historical_evidence": original.get("evidence"),
                "historical_observed": original.get("observed")}
        if original["kind"] == "query":
            changed = _changed_fields(old_cases[cid], cases[cid]) if cid in cases else ["missing_current_case"]
            item["current_expectation_changed_fields"] = changed
            frozen_pair = frozen_results.get((cid, original["mode"]))
            if frozen_pair:
                if _changed_fields(old_cases[cid], frozen["cases"][cid]):
                    raise ValueError(f"Frozen review changed historical expectation: {cid}")
                result, observation = frozen_pair
                item["evidence_run"] = "historical_original_expectations"
            else:
                pair = current_results.get((cid, original["mode"]))
                if pair is None:
                    item["status"] = "not_retested"
                    records.append(item)
                    continue
                result, observation = pair
                item["evidence_run"] = result["evidence_run"]
            item["observation"] = observation
            item["reference"] = result.get("reference")
            if changed and not frozen_pair:
                item["status"] = "changed_expectation_unverified"
            elif result.get("reference", {}).get("status") == "fail":
                item["status"] = "oracle_disagreement"
            elif observation["status"] == "pass":
                item["status"] = "pass"
            elif "reads no table" in observation.get("error", "").lower() and not old_cases[cid].error_contains:
                item["status"] = "unsupported_original_expectation"
                if original["issue"] == "STREAD-003":
                    item["blocked_by"] = "STREAD-009"
                    item["note"] = "The semicolon query now reaches the table-free-query rejection; its original positive expectation remains unmet."
            else:
                item["status"] = "fail"
        else:
            observation = current_extra.get(cid)
            if observation is None:
                item["status"] = "not_retested"
            else:
                prior = old_extra[cid]
                changed = [field for field in ("sql", "columns", "expected_rows", "expected_error_contains")
                           if _canonical(prior.get(field)) != _canonical(observation.get(field))]
                item.update(observation=observation, evidence_run=observation["evidence_run"],
                            current_expectation_changed_fields=changed,
                            status="changed_expectation_unverified" if changed else observation["status"])
        records.append(item)
    issues = []
    for issue in failure_map["issues"]:
        related = [r for r in records if r["issue"] == issue["id"]]
        counts = Counter(r["status"] for r in related)
        if counts and set(counts) == {"pass"}:
            status = "verified_fixed"
        elif counts.get("unsupported_original_expectation") and set(counts) <= {"pass", "unsupported_original_expectation"}:
            status = "partially_verified_remaining_case_blocked" if counts.get("pass") else "unsupported_original_expectations"
        else:
            status = "not_fully_verified"
        issues.append({"id": issue["id"], "title": issue["title"], "status": status,
                       "original_failure_count": len(related), "outcomes": dict(counts)})
    query_counts = Counter(r["status"] for r in records if r["kind"] == "query")
    extra_counts = Counter(r["status"] for r in records if r["kind"] != "query")
    return {"historical_audit": str(historical), "original_query_failures": sum(query_counts.values()),
            "original_additional_failures": sum(extra_counts.values()),
            "query_outcomes": dict(query_counts), "additional_outcomes": dict(extra_counts),
            "frozen_review_summary": frozen["saved"]["summary"], "issues": issues, "failures": records}


def _zone_coverage(cases, results, evidence):
    by_zone = defaultdict(lambda: {"query_cases": 0, "executions": 0, "passed": 0,
                                  "failed": 0, "cases_with_files_pruned": 0, "max_files_pruned": 0})
    for result in results:
        zone = cases[result["case_id"]].session_timezone
        record = by_zone[zone]
        record["query_cases"] += 1
        record["executions"] += len(result["runs"])
        record["passed"] += sum(run["status"] == "pass" for run in result["runs"])
        record["failed"] += sum(run["status"] == "fail" for run in result["runs"])
        maximum = max((run.get("plan_stats", {}).get("FILES_PRUNED", 0) or 0
                       for run in result["runs"]), default=0)
        record["cases_with_files_pruned"] += int(maximum > 0)
        record["max_files_pruned"] = max(record["max_files_pruned"], maximum)
    timezone_only = defaultdict(lambda: {"query_cases": 0, "executions": 0, "passed": 0, "failed": 0})
    for result in results:
        if not result["case_id"].startswith(("tz_", "coercion_")):
            continue
        record = timezone_only[cases[result["case_id"]].session_timezone]
        record["query_cases"] += 1
        record["executions"] += len(result["runs"])
        record["passed"] += sum(run["status"] == "pass" for run in result["runs"])
        record["failed"] += sum(run["status"] == "fail" for run in result["runs"])
    return {"sessions_scope": "All query groups classified by session timezone; UTC includes baseline and general date/alias cases.",
        "sessions": dict(sorted(by_zone.items())),
        "timezone_and_coercion_cases": dict(sorted(timezone_only.items())), "worker_session_observations": [
        {"evidence_run": item["path"], "requested": item["metadata"].get("session_timezone", "UTC"),
         "observed": item["metadata"].get("observed_session_timezones"),
         "note": "Base/cursor timezone was not recorded." if not item["metadata"].get("observed_session_timezones") else ""}
        for item in evidence]}


def aggregate(runs, historical, frozen_review, output, issue_map):
    output = Path(output).resolve()
    if output.exists():
        raise ValueError(f"Use a fresh output directory: {output}")
    loaded = [_load_run(path) for path in runs]
    if not loaded:
        raise ValueError("At least one completed current-expectation run is required")
    if len({run["path"] for run in loaded}) != len(loaded):
        raise ValueError("The same evidence run was supplied more than once")
    frozen = _load_run(frozen_review)
    historical = Path(historical).resolve()
    failure_map = _read(issue_map)
    cases, chosen, dataset, manifests = {}, {}, {}, {}
    extra, evidence, superseded, source_checks = [], [], [], []
    common_fingerprint = None
    for index, run in enumerate([*loaded, frozen]):
        source = _check_source(run, allow_unavailable=index == 0)
        fingerprint = source.get("source_before")
        if fingerprint:
            if common_fingerprint is not None and common_fingerprint != fingerprint:
                raise ValueError(f"Production-source fingerprints differ between completed workers: {run['path']}")
            common_fingerprint = fingerprint
        source_checks.append({"source_path": str(run["path"]), **source})
    for check in source_checks:
        audited = check.get("separate_audited_capture")
        if audited is not None and common_fingerprint is not None:
            check["separate_audited_matches_stable_workers"] = audited == common_fingerprint
            if audited != common_fingerprint:
                raise ValueError("The baseline's separate audited capture differs from the instrumented workers")
    for index, run in enumerate(loaded, 1):
        label = f"{index:02d}_" + re.sub(r"[^A-Za-z0-9_.-]", "_", run["path"].name)
        relative = "runs/" + label
        run["label"], run["relative"] = label, relative
        saved = run["saved"]
        for name, rows in _read(run["path"] / "dataset.json").items():
            if name in dataset and dataset[name] != rows:
                raise ValueError(f"Logical fixture {name!r} differs in {run['path']}")
            dataset[name] = rows
        manifests[relative] = _read(run["path"] / "fixture_manifest.json")
        for case_id, case in run["cases"].items():
            if case_id in cases and _changed_fields(cases[case_id], case):
                raise ValueError(f"Current evidence runs disagree on case expectations: {case_id}")
            cases[case_id] = case
        for result in saved["results"]:
            result = deepcopy(result)
            result["evidence_run"] = relative
            if result["case_id"] in chosen:
                superseded.append({"case_id": result["case_id"],
                                   "previous": chosen[result["case_id"]]["evidence_run"], "chosen": relative})
            chosen[result["case_id"]] = result
        for check in saved.get("additional_checks", []):
            check = deepcopy(check)
            check.update(original_case_id=check["case_id"], case_id=label + "__" + check["case_id"], evidence_run=relative)
            extra.append(check)
        evidence.append({"path": relative, "original_path": str(run["path"]),
                         "summary": saved["summary"], "metadata": saved["metadata"],
                         "source_verification": source_checks[index - 1]})
    results = list(chosen.values())
    verification = _issue_verification(historical, failure_map, cases, results, extra, frozen)
    coverage = _zone_coverage(cases, results, evidence)
    old_cases = {case.case_id: case for case in load_saved_cases(historical / "expectations.json")}
    changed = [{"case_id": cid, "changed_fields": _changed_fields(old_cases[cid], case),
                "old_error_contains": old_cases[cid].error_contains, "current_error_contains": case.error_contains}
               for cid, case in cases.items() if cid in old_cases and _changed_fields(old_cases[cid], case)]
    metadata = deepcopy(loaded[0]["saved"]["metadata"])
    metadata.update(
        aggregated_utc=datetime.now(timezone.utc).isoformat(),
        started_utc=min(run["saved"]["metadata"]["started_utc"] for run in loaded),
        finished_utc=max(run["saved"]["metadata"]["finished_utc"] for run in loaded),
        groups=sorted({group for run in loaded for group in run["saved"]["metadata"].get("groups", [])}),
        evidence_runs=evidence, superseded_observations=superseded,
        source_verification=source_checks, stable_worker_source=common_fingerprint,
        session_timezones=sorted(coverage["sessions"]), session_timezone="multiple",
        historical_expectation_changes=changed,
        frozen_original_expectations={"path": "historical_original_expectations",
                                      "summary": frozen["saved"]["summary"], "excluded_from_current_headline": True},
        review_notes=[
            f"Current-expectation results are separated from the original acceptance requirements. {len(changed)} historical cases have changed expectations; the saved changes are recorded in results.json.",
            f"The six table-free SELECT/CTE tests now accept a 'reads no table' rejection. Re-running their original positive expectations produced {frozen['saved']['summary']['failed']} failed executions; those results are preserved in [historical_original_expectations](historical_original_expectations/REPORT.md) and are excluded from the current-expectation headline.",
            f"Of the original {verification['original_query_failures']} failed executions, {verification['query_outcomes'].get('pass', 0)} now meet their original expectations and {verification['query_outcomes'].get('unsupported_original_expectation', 0)} remain unsupported. See [original issue verification](original_issue_verification.json).",
            "Paired executable-source fingerprints are required and checked for the instrumented workers. The earlier baseline lacks paired fingerprints; its separate audited capture is retained without claiming within-run source stability.",
            "Each worker retains its own logical dataset, expectations, physical fixtures, dependency versions, and metadata. The aggregate logical dataset is a conflict-checked union; the fixture manifest is indexed by evidence run. Physical paths inside copied metadata retain their original recorded values.",
        ],
    )
    output.mkdir(parents=True)
    for run in loaded:
        _copy_run(run["path"], output / run["relative"])
    _copy_run(frozen["path"], output / "historical_original_expectations")
    shutil.copyfile(historical / "expectations.json", output / "original_expectations.json")
    write_json(output / "original_failure_map.json", failure_map)
    write_json(output / "dataset.json", dataset)
    write_json(output / "expectations.json", [case.__dict__ for case in cases.values()])
    write_json(output / "fixture_manifest.json", {"evidence_runs": manifests,
        "note": "Physical paths inside each original manifest are unchanged. The copied fixtures are under the corresponding runs directory."})
    write_json(output / "original_issue_verification.json", verification)
    write_json(output / "timezone_coverage.json", coverage)
    write_json(output / "source_verification.json", source_checks)
    for name in ("dataset", "expectations"):
        metadata[name + "_sha256"] = hashlib.sha256((output / (name + ".json")).read_bytes()).hexdigest()
    summary = write_report(output, metadata, list(cases.values()), results, extra)
    lines = ["", "## Session timezone and pruning coverage", "",
             "Each session runs in its own worker process. The case oracle uses Python datetime/ZoneInfo rules, and the worker separately verifies its actual session timezone.", "",
             "The table below includes all query groups; UTC includes baseline and alias tests. The timezone/coercion-only subset is reported separately in timezone_coverage.json.", "",
             "| Session timezone | Cases | Executions | Failed executions | Cases with files pruned | Maximum files pruned |",
             "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for zone, counts in coverage["sessions"].items():
        lines.append(f"| {zone} | {counts['query_cases']} | {counts['executions']} | {counts['failed']} | {counts['cases_with_files_pruned']} | {counts['max_files_pruned']} |")
    lines += ["", "Pruning counts include cases where at least one observed execution reported a positive FILES_PRUNED value. They report actual file selection, not an assumption that every temporal predicate is prunable.", "",
              "| Evidence run | Requested timezone | Observed base | Observed new cursor |",
              "| --- | --- | --- | --- |"]
    for worker in coverage["worker_session_observations"]:
        observed = worker["observed"] or {}
        lines.append(f"| [{worker['evidence_run']}]({worker['evidence_run']}/REPORT.md) | {worker['requested']} | {observed.get('base', 'not recorded')} | {observed.get('cursor', 'not recorded')} |")
    lines += ["", "Full details: [timezone coverage](timezone_coverage.json), [original issue verification](original_issue_verification.json).", ""]
    with (output / "REPORT.md").open("a") as handle:
        handle.write("\n".join(lines))
    return summary


def main():
    parser = argparse.ArgumentParser(description="Consolidate completed SQL read-path rechecks without hiding original acceptance failures.")
    parser.add_argument("--runs", nargs="+", type=Path, required=True)
    parser.add_argument("--historical", type=Path, required=True)
    parser.add_argument("--frozen-review", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--issue-map", type=Path,
                        default=Path(__file__).resolve().parents[2] / "issues" / "failure-map.json")
    args = parser.parse_args()
    summary = aggregate(args.runs, args.historical, args.frozen_review, args.output, args.issue_map)
    print(json.dumps(summary, indent=2))
    print(f"Report: {args.output.resolve() / 'REPORT.md'}")
    return int(bool(summary["failed"] or summary["independent_oracle_disagreements"] or summary["additional_check_failures"]))


if __name__ == "__main__":
    raise SystemExit(main())
