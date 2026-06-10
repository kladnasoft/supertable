# utils/profiler.py
"""
Lightweight profiler for accumulating per-stage timings and IO counters
across the write / update / delete pipeline.

Design:
    - One Profiler instance is created at the top of DataWriter.write() and
      passed down through the processing helpers.
    - Each helper records its sub-phases via `with profiler.span("name"):`
      and bumps counters via `profiler.add("bytes_read", n)`.
    - The accumulated dicts are shipped in the monitoring stats payload so
      Redis-side dashboards can see where time and IO went per write.

Performance:
    time.perf_counter() ~ 50 ns; span overhead is negligible compared to
    file IO or any Polars operation.  Use NullProfiler() (or pass None and
    let callers default to it) to disable instrumentation in hot loops
    where every nanosecond matters.

Conventions:
    - Span names use dotted hierarchy:  "process.phase2.read"
    - Counter names use snake_case:     "bytes_read", "files_written"
    - `<span>.n` is auto-incremented to record how many times each span
      fired (useful to distinguish "1 slow read" from "100 fast reads").
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Dict, Iterator


class Profiler:
    """Accumulates cumulative span durations and integer counters."""

    __slots__ = ("timings", "counts")

    def __init__(self) -> None:
        self.timings: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}

    @contextmanager
    def span(self, name: str) -> Iterator[None]:
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self.timings[name] = self.timings.get(name, 0.0) + dt
            self.counts[name + ".n"] = self.counts.get(name + ".n", 0) + 1

    def mark(self, name: str, t0: float) -> float:
        """Record (perf_counter() - t0) under *name* without using a span."""
        dt = time.perf_counter() - t0
        self.timings[name] = self.timings.get(name, 0.0) + dt
        self.counts[name + ".n"] = self.counts.get(name + ".n", 0) + 1
        return dt

    def add(self, name: str, value: int = 1) -> None:
        """Increment a counter (bytes, rows, files, …)."""
        if not value:
            return
        self.counts[name] = self.counts.get(name, 0) + int(value)

    def merge(self, other: "Profiler") -> None:
        for k, v in other.timings.items():
            self.timings[k] = self.timings.get(k, 0.0) + v
        for k, v in other.counts.items():
            self.counts[k] = self.counts.get(k, 0) + v

    def emit_timings(self) -> Dict[str, float]:
        """Return timings rounded to microseconds for the monitoring payload."""
        return {k: round(v, 6) for k, v in self.timings.items()}

    def emit_counts(self) -> Dict[str, int]:
        return dict(self.counts)


class NullProfiler:
    """No-op profiler — drop-in replacement when instrumentation is off."""

    __slots__ = ("timings", "counts")

    def __init__(self) -> None:
        self.timings: Dict[str, float] = {}
        self.counts: Dict[str, int] = {}

    @contextmanager
    def span(self, name: str) -> Iterator[None]:
        yield

    def mark(self, name: str, t0: float) -> float:
        return 0.0

    def add(self, name: str, value: int = 1) -> None:
        return None

    def merge(self, other) -> None:
        return None

    def emit_timings(self) -> Dict[str, float]:
        return {}

    def emit_counts(self) -> Dict[str, int]:
        return {}


# Shared null instance — avoids allocating a NullProfiler per call when a
# caller doesn't supply one.
_NULL_PROFILER: "NullProfiler" = NullProfiler()


def get_null_profiler() -> NullProfiler:
    """Return the shared NullProfiler singleton."""
    return _NULL_PROFILER
