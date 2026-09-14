from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib
import json
from pathlib import Path
import shutil

from .compare import json_default
from .dataset import build_dataset
from .report import write_json, write_report


def normalized(value):
    result = json.loads(json.dumps(value, default=json_default))
    if isinstance(result, dict) and "expected" in result and not result.get("ordered"):
        result["expected"].sort(key=lambda row: json.dumps(row, sort_keys=True, ensure_ascii=False))
    return result


def consolidate(inputs, output, groups):
    output = Path(output).resolve()
    if output.exists():
        raise ValueError("Choose a fresh consolidated output directory")
    cases = []
    for name in groups:
        cases.extend(importlib.import_module(f"scripts.sql_read_matrix.cases_{name}").build_cases(build_dataset()))
    current = {case.case_id: normalized(case.__dict__) for case in cases}
    selected, expected, extras, provenance, manifests = {}, {}, {}, [], {}
    base = None
    dataset_digest = None
    for index, source in enumerate(inputs, 1):
        source = Path(source).resolve()
        raw = json.loads((source / "results.json").read_text())
        saved_expected = {case["case_id"]: case for case in json.loads((source / "expectations.json").read_text())}
        digest = hashlib.sha256((source / "dataset.json").read_bytes()).hexdigest()
        meta = raw["metadata"]
        if base is None:
            base, dataset_digest = meta, digest
        elif digest != dataset_digest or any(meta[key] != base[key] for key in ("revision", "package_version", "versions")):
            raise ValueError("Runs must use identical datasets, source revisions, package and dependency versions")
        relative = f"runs/{index:02d}_{source.name}"
        provenance.append({"path": relative, "metadata": meta, "summary": raw["summary"]})
        manifests[relative] = json.loads((source / "fixture_manifest.json").read_text())
        for result in raw["results"]:
            key = result["case_id"]
            if key not in current:
                continue
            result["evidence_run"] = relative
            selected[key] = result
            expected[key] = normalized(saved_expected[key])
        for check in raw.get("additional_checks", []):
            check["evidence_run"] = relative
            extras[check["case_id"]] = check
    if set(selected) != set(current):
        raise ValueError(f"Missing observations for: {sorted(set(current) - set(selected))}")
    for key, case in current.items():
        if expected[key] != case:
            raise ValueError(f"Latest observed expectation differs from current case: {key}")
    output.mkdir(parents=True)
    for source, run in zip(inputs, provenance):
        shutil.copytree(source, output / run["path"], ignore=shutil.ignore_patterns("progress.json"))
    shutil.copyfile(Path(inputs[0]) / "dataset.json", output / "dataset.json")
    write_json(output / "expectations.json", [case.__dict__ for case in cases])
    write_json(output / "fixture_manifest.json", {"evidence_runs": manifests})
    for source in reversed(inputs):
        if (Path(source) / "ingest_probes.json").exists():
            shutil.copyfile(Path(source) / "ingest_probes.json", output / "ingest_probes.json")
            break
    metadata = dict(base)
    metadata.update(
        finished_utc=max(run["metadata"]["finished_utc"] for run in provenance),
        consolidated_utc=datetime.now(timezone.utc).isoformat(),
        dataset_sha256=dataset_digest,
        expectations_sha256=hashlib.sha256((output / "expectations.json").read_bytes()).hexdigest(),
        groups=groups,
        sqlglot_version=next((run["metadata"]["sqlglot_version"] for run in reversed(provenance)
                            if "sqlglot_version" in run["metadata"]), None),
        evidence_runs=provenance,
        raw_execution_attempts=sum(run["summary"]["executions"] for run in provenance),
        review_notes=[
            "Latest observations replace earlier observations of the same case; all original runs are retained.",
            "The EU SELECT-star oracle and reference projection were corrected to the explicit canonical sorted role allowlist. The entire controls group was rerun with the corrected predefined expectation.",
            "Exact integer/decimal expectations were strengthened to reject rounded floats. Decimal AVG was explicitly represented as a floating expectation; all five cases with exact Python expectations and reported floating native columns were rerun. Other passing cases had exact native numeric types or already used floating expectations.",
        ],
    )
    summary = write_report(output, metadata, cases, [selected[case.case_id] for case in cases], list(extras.values()))
    print(json.dumps(summary, indent=2))
    print(f"Report: {output / 'REPORT.md'}")
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runs", type=Path, nargs="+", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--groups", nargs="+", default=["scalar", "relational", "advanced", "controls", "syntax"])
    args = parser.parse_args()
    consolidate(args.runs, args.output, args.groups)


if __name__ == "__main__":
    main()
