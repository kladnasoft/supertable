"""Container entry point for one parity/timing engine series."""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
import sys
import traceback
from pathlib import Path
from typing import Any, Sequence

from .container_runner import _cgroup_v2_extended_telemetry
from .runner import run_engine_series_in_process


DEPENDENCY_DISTRIBUTIONS = (
    "supertable",
    "numpy",
    "pandas",
    "pyarrow",
    "polars",
    "duckdb",
    "sqlglot",
    "redis",
)


def runtime_provenance() -> dict[str, Any]:
    dependencies: dict[str, str | None] = {}
    for name in DEPENDENCY_DISTRIBUTIONS:
        try:
            dependencies[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            dependencies[name] = None
    try:
        import supertable

        module_version = getattr(supertable, "__version__", None)
        module_path = str(Path(supertable.__file__).resolve())
    except Exception as exc:  # pragma: no cover - runner import already loads it
        module_version = None
        module_path = f"unavailable:{type(exc).__name__}:{exc}"
    affinity = (
        sorted(int(cpu) for cpu in os.sched_getaffinity(0))
        if hasattr(os, "sched_getaffinity")
        else None
    )
    return {
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "python_executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
        "logical_cpu_count": os.cpu_count(),
        "cpu_affinity": affinity,
        "supertable_module_version": module_version,
        "supertable_module_path": module_path,
        "dependencies": dependencies,
        "cgroup_v2": _cgroup_v2_extended_telemetry(),
    }


def worker_main(request_path: str | Path, response_path: str | Path) -> int:
    request_file = Path(request_path)
    response_file = Path(response_path)
    before = runtime_provenance()
    try:
        request = json.loads(request_file.read_text(encoding="utf-8"))
        result = run_engine_series_in_process(request)
        response: dict[str, Any] = {
            "ok": True,
            "result": result,
            "worker_provenance": {
                "before": before,
                "after": runtime_provenance(),
            },
        }
        code = 0
    except Exception as exc:  # noqa: BLE001 - preserve complete worker failure
        response = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
            "worker_provenance": {
                "before": before,
                "after": runtime_provenance(),
            },
        }
        code = 1
    temporary = response_file.with_name(f".{response_file.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(response, indent=2, sort_keys=True, allow_nan=False) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, response_file)
    return code


def main(argv: Sequence[str] | None = None) -> int:
    values = list(sys.argv[1:] if argv is None else argv)
    if len(values) != 2:
        print(
            "usage: python -m supertable.engine.benchmarks.container_worker "
            "REQUEST RESPONSE",
            file=sys.stderr,
        )
        return 2
    return worker_main(values[0], values[1])


if __name__ == "__main__":
    raise SystemExit(main())
