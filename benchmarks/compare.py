"""Compare two saved runs.

The comparison reports two independent kinds of difference, and treats them
very differently:

* **Correctness drift** — a scenario's seal changed, an expectation that used
  to hold now fails, or a scenario that used to run now errors.  This is a
  behaviour change and is reported first regardless of timing.  A faster run
  that returns different rows is not an improvement.

* **Performance drift** — p50/p95 moved by more than a threshold.  Reported
  only for scenarios whose seals still match, because a timing delta on a
  scenario that changed behaviour is meaningless.

Runs measured on different backends or machines are not comparable; the tool
says so loudly rather than printing a confident number.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from benchmarks._harness import comparability_warnings, load_run

DEFAULT_THRESHOLD_PCT = 10.0


def _by_id(run: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {s["id"]: s for s in run.get("scenarios", [])}


def _pct_delta(base: Optional[float], cand: Optional[float]) -> Optional[float]:
    if not base or cand is None:
        return None
    return (cand - base) / base * 100.0


def _seal_digest(scenario: Dict[str, Any]) -> Optional[str]:
    seal = scenario.get("seal")
    return seal.get("digest") if isinstance(seal, dict) else None


def _failed_expectations(scenario: Dict[str, Any]) -> List[str]:
    return sorted(
        key for key, value in (scenario.get("expectations") or {}).items()
        if isinstance(value, dict) and value.get("ok") is False
    )


def compare(
    baseline: Dict[str, Any],
    candidate: Dict[str, Any],
    *,
    threshold_pct: float = DEFAULT_THRESHOLD_PCT,
) -> Dict[str, Any]:
    base_scenarios = _by_id(baseline)
    cand_scenarios = _by_id(candidate)

    correctness: List[Dict[str, Any]] = []
    performance: List[Dict[str, Any]] = []
    missing = sorted(set(base_scenarios) - set(cand_scenarios))
    added = sorted(set(cand_scenarios) - set(base_scenarios))

    for scenario_id in sorted(set(base_scenarios) & set(cand_scenarios)):
        base = base_scenarios[scenario_id]
        cand = cand_scenarios[scenario_id]

        base_seal = _seal_digest(base)
        cand_seal = _seal_digest(cand)
        seal_changed = (
            base_seal is not None and cand_seal is not None and base_seal != cand_seal
        )
        new_failures = sorted(
            set(_failed_expectations(cand)) - set(_failed_expectations(base))
        )
        newly_broken = base.get("status") == "ok" and cand.get("status") != "ok"

        if seal_changed or new_failures or newly_broken:
            correctness.append({
                "id": scenario_id,
                "seal_changed": seal_changed,
                "baseline_seal": base_seal,
                "candidate_seal": cand_seal,
                "baseline_rows": base.get("rows"),
                "candidate_rows": cand.get("rows"),
                "new_expectation_failures": new_failures,
                "baseline_status": base.get("status"),
                "candidate_status": cand.get("status"),
                "candidate_error": cand.get("error"),
            })
            continue                              # timing on changed behaviour is noise

        base_t = base.get("timings_ms") or {}
        cand_t = cand.get("timings_ms") or {}
        entry = {
            "id": scenario_id,
            "p50_baseline_ms": base_t.get("p50"),
            "p50_candidate_ms": cand_t.get("p50"),
            "p50_delta_pct": _pct_delta(base_t.get("p50"), cand_t.get("p50")),
            "p95_baseline_ms": base_t.get("p95"),
            "p95_candidate_ms": cand_t.get("p95"),
            "p95_delta_pct": _pct_delta(base_t.get("p95"), cand_t.get("p95")),
        }
        entry["throughput"] = _throughput_delta(base, cand)

        # A verdict must clear the baseline's own run-to-run spread, not just a
        # fixed percentage.  Short scenarios on a busy machine swing by tens of
        # percent between identical runs; calling that a regression trains the
        # reader to ignore the tool.
        noise_pct = _noise_band_pct(base_t)
        single_sample = (base_t.get("n") or 0) <= 1 or (cand_t.get("n") or 0) <= 1
        # With one sample there is no spread to measure, so nothing separates a
        # real change from a noisy machine; widen the bar rather than guess.
        effective = max(threshold_pct, noise_pct,
                        threshold_pct * 2.5 if single_sample else 0.0)
        entry["noise_band_pct"] = round(noise_pct, 2)
        entry["effective_threshold_pct"] = round(effective, 2)
        entry["single_sample"] = single_sample

        # A time-boxed phase runs for a fixed number of seconds by
        # construction, so its wall time says nothing about speed — how much
        # work it completed in that window does.  Judge those on throughput.
        throughput = entry.get("throughput")
        if throughput is not None:
            entry["judged_on"] = throughput["metric"]
            delta = -throughput["delta_pct"]      # fewer rows/s == slower
        else:
            entry["judged_on"] = "p50"
            delta = entry["p50_delta_pct"]

        entry["verdict"] = (
            "unchanged" if delta is None else
            "regressed" if delta > effective else
            "improved" if delta < -effective else
            "unchanged"
        )
        if entry["verdict"] == "unchanged" and delta is not None \
                and abs(delta) > threshold_pct:
            entry["note"] = "within measurement noise"
        performance.append(entry)

    return {
        "threshold_pct": threshold_pct,
        "comparability_warnings": comparability_warnings(
            baseline.get("environment", {}), candidate.get("environment", {}),
        ),
        "baseline": _identity(baseline),
        "candidate": _identity(candidate),
        "correctness_drift": correctness,
        "performance": performance,
        "missing_scenarios": missing,
        "added_scenarios": added,
    }


def _noise_band_pct(timings: Dict[str, Any]) -> float:
    """Two standard deviations of the baseline, as a percentage of its p50.

    This is the band inside which a difference is indistinguishable from the
    same code measured twice, so it is the floor for calling anything a change.
    """
    p50 = timings.get("p50")
    stdev = timings.get("stdev")
    if not p50 or not stdev:
        return 0.0
    return (2.0 * float(stdev)) / float(p50) * 100.0


def _throughput_delta(base: Dict[str, Any], cand: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Write phases report rows/second; higher is better, so invert the sign."""
    keys = ("total_rows_per_second", "rows_per_second")
    for key in keys:
        b = (base.get("metrics") or {}).get(key)
        c = (cand.get("metrics") or {}).get(key)
        if isinstance(b, (int, float)) and isinstance(c, (int, float)) and b:
            return {
                "metric": key,
                "baseline": b,
                "candidate": c,
                "delta_pct": round((c - b) / b * 100.0, 2),
            }
    return None


def _identity(run: Dict[str, Any]) -> Dict[str, Any]:
    env = run.get("environment", {})
    return {
        "suite": run.get("suite"),
        "profile": run.get("profile"),
        "scale": run.get("scale"),
        "version": env.get("supertable_version"),
        "git": env.get("git_describe"),
        "storage": env.get("storage_type"),
        "dataset_fingerprint": (run.get("dataset") or {}).get("fingerprint"),
    }


def render(report: Dict[str, Any]) -> str:
    lines: List[str] = []
    base, cand = report["baseline"], report["candidate"]
    lines.append(
        f"{base['suite']} suite — {base['version']} ({base['git']}) "
        f"-> {cand['version']} ({cand['git']})   "
        f"profile={base['profile']} scale={base['scale']}"
    )

    if base.get("dataset_fingerprint") != cand.get("dataset_fingerprint"):
        lines.append(
            f"  !! dataset differs: {base.get('dataset_fingerprint')} -> "
            f"{cand.get('dataset_fingerprint')} — timings are NOT comparable"
        )
    for warning in report["comparability_warnings"]:
        lines.append(f"  !! {warning}")

    drift = report["correctness_drift"]
    lines.append("")
    if drift:
        lines.append(f"CORRECTNESS DRIFT — {len(drift)} scenario(s) changed behaviour:")
        for item in drift:
            lines.append(f"  {item['id']}")
            if item["seal_changed"]:
                lines.append(
                    f"    seal:  {item['baseline_seal'][:16]}... -> "
                    f"{item['candidate_seal'][:16]}...  "
                    f"(rows {item['baseline_rows']} -> {item['candidate_rows']})"
                )
            if item["new_expectation_failures"]:
                lines.append(
                    f"    newly failing checks: "
                    f"{', '.join(item['new_expectation_failures'])}"
                )
            if item["candidate_status"] != item["baseline_status"]:
                lines.append(
                    f"    status: {item['baseline_status']} -> "
                    f"{item['candidate_status']}  {item.get('candidate_error') or ''}"
                )
    else:
        lines.append("CORRECTNESS — all seals and checks match the baseline.")

    lines.append("")
    lines.append(f"PERFORMANCE (threshold ±{report['threshold_pct']:g}%, "
                 f"+ = slower):")
    lines.append(f"  {'scenario':<34} {'p50 base':>10} {'p50 cand':>10} "
                 f"{'Δp50':>8} {'Δp95':>8}  verdict")
    for entry in sorted(
        report["performance"],
        key=lambda e: -((e.get("throughput") or {}).get("delta_pct", 0) * -1
                        if e.get("throughput") else (e["p50_delta_pct"] or 0)),
    ):
        d50 = entry["p50_delta_pct"]
        d95 = entry["p95_delta_pct"]
        lines.append(
            f"  {entry['id']:<34} "
            f"{entry['p50_baseline_ms'] or 0:>10.2f} "
            f"{entry['p50_candidate_ms'] or 0:>10.2f} "
            f"{(f'{d50:+.1f}%' if d50 is not None else '   n/a'):>8} "
            f"{(f'{d95:+.1f}%' if d95 is not None else '   n/a'):>8}"
            f"  {entry['verdict']}"
            f"{'  (noise ±' + format(entry.get('noise_band_pct', 0.0), '.0f') + '%)' if entry.get('note') else ''}"
        )
        if entry.get("throughput"):
            tp = entry["throughput"]
            lines.append(
                f"      {tp['metric']}: {tp['baseline']:,.0f} -> "
                f"{tp['candidate']:,.0f} rows/s ({tp['delta_pct']:+.1f}%)"
            )

    if report["missing_scenarios"]:
        lines.append(f"\n  scenarios absent from candidate: "
                     f"{', '.join(report['missing_scenarios'])}")
    if report["added_scenarios"]:
        lines.append(f"  scenarios new in candidate: "
                     f"{', '.join(report['added_scenarios'])}")

    regressed = [e for e in report["performance"] if e["verdict"] == "regressed"]
    lines.append("")
    lines.append(
        f"SUMMARY: {len(drift)} correctness change(s), "
        f"{len(regressed)} performance regression(s) beyond "
        f"±{report['threshold_pct']:g}%"
    )
    return "\n".join(lines)


def exit_code(report: Dict[str, Any]) -> int:
    """Non-zero when something regressed, so CI can gate on it."""
    if report["correctness_drift"]:
        return 2
    if any(e["verdict"] == "regressed" for e in report["performance"]):
        return 1
    return 0


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="benchmarks.compare",
        description="Compare two SuperTable benchmark result files",
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--threshold-pct", type=float, default=DEFAULT_THRESHOLD_PCT)
    parser.add_argument("--json", action="store_true", help="emit the raw report")
    args = parser.parse_args(argv)

    report = compare(
        load_run(args.baseline), load_run(args.candidate),
        threshold_pct=args.threshold_pct,
    )
    print(json.dumps(report, indent=2) if args.json else render(report))
    return exit_code(report)


if __name__ == "__main__":
    raise SystemExit(main())
