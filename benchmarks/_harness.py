"""Shared machinery for the SuperTable performance suites.

Three jobs:

* **Fingerprint the run** — a result file is only meaningful next to the
  version, backend and machine that produced it, so every run records them.
* **Summarise timings** — one number per scenario hides everything that
  matters, so each scenario reports a distribution.
* **Seal correctness** — a scenario that got faster by returning the wrong
  rows is not faster.  Every read scenario and the write lifecycle hash their
  logical result, so a later version that changes behaviour is caught even
  when its timings look fine.

Nothing here imports ``supertable`` at module scope: the storage backend is
chosen with an environment variable that must be set *before* the settings
singleton is built.
"""
from __future__ import annotations

import hashlib
import json
import os
import platform
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

RESULT_SCHEMA_VERSION = 2

BENCH_ROOT = Path(__file__).resolve().parent
RESULTS_ROOT = BENCH_ROOT / "results"

# Everything the suite creates lives under one organization so a run can never
# be confused with real data and teardown can be exact.
BENCH_ORG = "perf_bench"
BENCH_ROLE = "superadmin"

# The supertable name is per-profile.  Redis is shared across backends while
# the data is not, so LOCAL and MINIO must never share a catalog namespace —
# otherwise one profile's pointers would reference paths that only exist in
# the other's storage.  ``configure_profile`` is called once by the CLI before
# any suite runs.
BENCH_SUPER = "perf_unset"


def configure_profile(profile: str) -> str:
    """Bind the catalog namespace to a storage profile. Returns the name."""
    global BENCH_SUPER
    BENCH_SUPER = f"perf_{profile}"
    return BENCH_SUPER

# The writer materialises these into every data file; both are write-time or
# allocation-order dependent, so neither can take part in a stable seal.
UNSEALABLE_COLUMNS = ("__rowid__", "__timestamp__")


# ──────────────────────────────────────────────────────────────────────
# Environment fingerprint
# ──────────────────────────────────────────────────────────────────────

def _git(*args: str) -> str:
    try:
        out = subprocess.run(
            ["git", *args],
            cwd=str(BENCH_ROOT.parent),
            capture_output=True,
            text=True,
            timeout=15,
        )
        return out.stdout.strip() if out.returncode == 0 else ""
    except Exception:
        return ""


def _module_version(name: str) -> str:
    try:
        mod = __import__(name)
        return str(getattr(mod, "__version__", "") or "")
    except Exception:
        return ""


def _total_ram_gb() -> Optional[float]:
    try:
        pages = os.sysconf("SC_PHYS_PAGES")
        page_size = os.sysconf("SC_PAGE_SIZE")
        return round(pages * page_size / (1024 ** 3), 1)
    except Exception:
        return None


def environment_fingerprint(profile: str) -> Dict[str, Any]:
    """Everything needed to decide whether two result files are comparable."""
    from supertable.config.settings import settings

    return {
        "profile": profile,
        "supertable_version": _module_version("supertable"),
        "git_commit": _git("rev-parse", "HEAD"),
        "git_describe": _git("describe", "--tags", "--always", "--dirty"),
        "git_dirty": bool(_git("status", "--porcelain")),
        "python": platform.python_version(),
        "duckdb": _module_version("duckdb"),
        "polars": _module_version("polars"),
        "pyarrow": _module_version("pyarrow"),
        "numpy": _module_version("numpy"),
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count(),
        "total_ram_gb": _total_ram_gb(),
        "storage_type": settings.STORAGE_TYPE,
    }


def comparability_warnings(base: Dict[str, Any], cand: Dict[str, Any]) -> List[str]:
    """Reasons two runs should not be compared at face value.

    Timings only mean something against the same backend on the same machine;
    a differing CPU or storage type makes a delta noise, not signal.
    """
    warnings: List[str] = []
    for key, label in (
        ("profile", "storage profile"),
        ("storage_type", "storage backend"),
        ("processor", "CPU"),
        ("cpu_count", "core count"),
        ("platform", "platform"),
    ):
        if base.get(key) != cand.get(key):
            warnings.append(
                f"{label} differs: {base.get(key)!r} -> {cand.get(key)!r}"
            )
    if base.get("git_dirty") or cand.get("git_dirty"):
        warnings.append(
            "one side was measured on a dirty working tree; it does not "
            "correspond to any commit"
        )
    return warnings


# ──────────────────────────────────────────────────────────────────────
# Timing
# ──────────────────────────────────────────────────────────────────────

def summarize_ms(samples: Sequence[float]) -> Dict[str, float]:
    """Report the shape of a latency sample, not just its middle.

    p95 is what a regression usually shows up in first; a mean alone hides it.
    """
    if not samples:
        return {}
    ordered = sorted(samples)

    def pct(p: float) -> float:
        if len(ordered) == 1:
            return round(ordered[0], 3)
        idx = min(len(ordered) - 1, max(0, int(round((len(ordered) - 1) * p))))
        return round(ordered[idx], 3)

    return {
        "n": len(ordered),
        "min": round(ordered[0], 3),
        "p50": pct(0.50),
        "p95": pct(0.95),
        "max": round(ordered[-1], 3),
        "mean": round(statistics.fmean(ordered), 3),
        "stdev": round(statistics.stdev(ordered), 3) if len(ordered) > 1 else 0.0,
    }


class StopWatch:
    """Monotonic timer in milliseconds."""

    def __init__(self) -> None:
        self._t0 = 0.0
        self.elapsed_ms = 0.0

    def __enter__(self) -> "StopWatch":
        self._t0 = time.perf_counter()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.elapsed_ms = (time.perf_counter() - self._t0) * 1000.0


# ──────────────────────────────────────────────────────────────────────
# Correctness seal
# ──────────────────────────────────────────────────────────────────────

def _canonical_scalar(value: Any) -> str:
    """Render one cell so equivalent values from different versions agree.

    The read path is documented to coerce integers to floats through numpy, so
    ``3`` and ``3.0`` must hash identically or the seal would fire on a type
    change that carries no information.  Floats are rounded because the last
    binary digit of a sum depends on aggregation order, which is not a
    behavioural guarantee.
    """
    if value is None:
        return "\x00null"
    if isinstance(value, bool):
        return f"b:{int(value)}"
    if isinstance(value, int):
        return f"n:{value}"
    if isinstance(value, float):
        if value != value:                      # NaN
            return "n:nan"
        if value in (float("inf"), float("-inf")):
            return f"n:{value}"
        # Round BEFORE deciding integrality, or a value that only becomes whole
        # after rounding would take the decimal branch while the equal integer
        # took the integer branch, and two equal numbers would hash apart.
        rounded = round(value, 6)
        if rounded.is_integer() and abs(rounded) < 2 ** 53:
            return f"n:{int(rounded)}"
        return f"n:{rounded:.6f}"
    if isinstance(value, (bytes, bytearray)):
        return "x:" + bytes(value).hex()
    if isinstance(value, datetime):
        return "t:" + value.astimezone(timezone.utc).isoformat()
    return "s:" + str(value)


def seal_rows(
    column_names: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    ignore_columns: Sequence[str] = UNSEALABLE_COLUMNS,
) -> Dict[str, Any]:
    """Hash a result set independently of row and column order.

    Reads carry no ordering guarantee, so an unordered query returning the same
    rows in a different sequence is not a behaviour change and must not break
    the seal.  Scenarios that *do* assert an order seal the ordered form
    separately via ``seal_ordered_rows``.
    """
    keep = [
        (i, name) for i, name in enumerate(column_names)
        if name not in ignore_columns
    ]
    keep.sort(key=lambda pair: pair[1])

    encoded = [
        "\x1f".join(_canonical_scalar(row[i]) for i, _ in keep)
        for row in rows
    ]
    encoded.sort()

    digest = hashlib.sha256()
    digest.update(("|".join(name for _, name in keep) + "\x1e").encode())
    for line in encoded:
        digest.update(line.encode())
        digest.update(b"\x1e")
    return {
        "algorithm": "sha256/unordered-v2",
        "columns": [name for _, name in keep],
        "row_count": len(encoded),
        "digest": digest.hexdigest(),
    }


def seal_ordered_rows(
    column_names: Sequence[str],
    rows: Sequence[Sequence[Any]],
    *,
    ignore_columns: Sequence[str] = UNSEALABLE_COLUMNS,
) -> Dict[str, Any]:
    """Hash a result set *including* row order.

    Used only where the query pins an order (``ORDER BY ... LIMIT``), which is
    exactly where a planner change could silently return a different top-N.
    """
    keep = [
        (i, name) for i, name in enumerate(column_names)
        if name not in ignore_columns
    ]
    keep.sort(key=lambda pair: pair[1])

    digest = hashlib.sha256()
    digest.update(("|".join(name for _, name in keep) + "\x1e").encode())
    for row in rows:
        digest.update("\x1f".join(_canonical_scalar(row[i]) for i, _ in keep).encode())
        digest.update(b"\x1e")
    return {
        "algorithm": "sha256/ordered-v2",
        "columns": [name for _, name in keep],
        "row_count": len(rows),
        "digest": digest.hexdigest(),
    }


def seal_dataframe(df: Any, *, ordered: bool = False) -> Dict[str, Any]:
    """Seal a polars or pandas frame returned by ``DataReader.execute``."""
    if df is None:
        return {"algorithm": "none", "columns": [], "row_count": 0, "digest": ""}
    if hasattr(df, "to_pandas") and not hasattr(df, "iloc"):
        columns = list(df.columns)
        rows = df.rows()
    else:                                        # pandas
        columns = [str(c) for c in df.columns]
        rows = df.itertuples(index=False, name=None)
    sealer = seal_ordered_rows if ordered else seal_rows
    return sealer(columns, list(rows))


# ──────────────────────────────────────────────────────────────────────
# Results
# ──────────────────────────────────────────────────────────────────────

@dataclass
class ScenarioResult:
    id: str
    description: str
    status: str = "ok"
    error: Optional[str] = None
    timings_ms: Dict[str, float] = field(default_factory=dict)
    rows: Optional[int] = None
    seal: Optional[Dict[str, Any]] = None
    plan: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    expectations: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SuiteRun:
    suite: str
    profile: str
    scale: str
    started_at: str
    finished_at: str = ""
    duration_s: float = 0.0
    environment: Dict[str, Any] = field(default_factory=dict)
    dataset: Dict[str, Any] = field(default_factory=dict)
    scenarios: List[ScenarioResult] = field(default_factory=list)
    schema_version: int = RESULT_SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["scenarios"] = [asdict(s) if not isinstance(s, dict) else s
                                for s in self.scenarios]
        return payload


def result_path(suite: str, profile: str, version: str, scale: str) -> Path:
    directory = RESULTS_ROOT / profile
    directory.mkdir(parents=True, exist_ok=True)
    return directory / f"{suite}-{version}-{scale}.json"


def save_run(run: SuiteRun) -> Path:
    version = run.environment.get("supertable_version") or "unknown"
    path = result_path(run.suite, run.profile, version, run.scale)
    path.write_text(json.dumps(run.to_dict(), indent=2, sort_keys=False) + "\n")
    return path


def load_run(path: Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text())


# ──────────────────────────────────────────────────────────────────────
# Scenario driver
# ──────────────────────────────────────────────────────────────────────

def run_scenario(
    scenario_id: str,
    description: str,
    body: Callable[[], Dict[str, Any]],
    *,
    iterations: int = 1,
    warmup: int = 0,
    log: Callable[[str], None] = print,
) -> ScenarioResult:
    """Time ``body`` and fold whatever it reports into one scenario result.

    ``body`` returns a dict that may carry ``rows``/``seal``/``plan``/
    ``metrics``/``expectations``.  Warmup iterations are executed and thrown
    away so the first-call cost of extension loading and connection setup does
    not land in the sample.
    """
    result = ScenarioResult(id=scenario_id, description=description)
    samples: List[float] = []
    try:
        for _ in range(max(0, warmup)):
            body()
        payload: Dict[str, Any] = {}
        for _ in range(max(1, iterations)):
            with StopWatch() as watch:
                payload = body() or {}
            samples.append(watch.elapsed_ms)

        result.timings_ms = summarize_ms(samples)
        result.rows = payload.get("rows")
        result.seal = payload.get("seal")
        result.plan = payload.get("plan", {}) or {}
        result.metrics = payload.get("metrics", {}) or {}
        result.expectations = payload.get("expectations", {}) or {}

        failed = [k for k, v in result.expectations.items()
                  if isinstance(v, dict) and v.get("ok") is False]
        if failed:
            result.status = "expectation-failed"
            result.error = f"unmet expectations: {', '.join(sorted(failed))}"
        log(f"  {scenario_id:<28} {result.status:<19} "
            f"p50={result.timings_ms.get('p50', 0):>9.2f}ms  rows={result.rows}")
    except Exception as exc:                     # a broken scenario must not abort the suite
        result.status = "error"
        result.error = f"{type(exc).__name__}: {exc}"
        log(f"  {scenario_id:<28} ERROR  {result.error}")
    return result


def expect(label: str, ok: bool, actual: Any, expected: Any) -> Dict[str, Any]:
    """Record a checked claim so a result file states what it verified."""
    return {label: {"ok": bool(ok), "actual": actual, "expected": expected}}


def new_run(suite: str, profile: str, scale: str) -> SuiteRun:
    return SuiteRun(
        suite=suite,
        profile=profile,
        scale=scale,
        started_at=datetime.now(timezone.utc).isoformat(),
        environment=environment_fingerprint(profile),
    )


def finish_run(run: SuiteRun, started_perf: float) -> SuiteRun:
    run.finished_at = datetime.now(timezone.utc).isoformat()
    run.duration_s = round(time.perf_counter() - started_perf, 2)
    return run
