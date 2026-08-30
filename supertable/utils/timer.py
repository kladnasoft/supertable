# route: supertable.utils.timer
import json
import math
import threading
import time
import pyarrow
from functools import wraps
from typing import Any, Callable, Dict
from supertable.config.defaults import default

class Timer:
    """
    A flexible class for capturing execution time. It can be used:
      1) As a decorator to measure function execution time.
      2) As a context manager to measure code blocks.
      3) Via manual calls to `capture_and_reset_timing` and `capture_duration`.

    Usage as a decorator:
        timer = Timer()

        @timer
        def my_func(...):
            ...

    Usage as a context manager:
        with Timer() as t:
            # code block
        # check t.timings for captured durations

    Usage for event captures (anytime):
        t = Timer()
        # do some work
        t.capture_and_reset_timing("first_event")
        # do more work
        t.capture_and_reset_timing("second_event")
        ...
        t.capture_duration("total_run")
    """

    def __init__(self, show_timing: bool = None) -> None:
        # If show_timing is None, use default.IS_SHOW_TIMING
        self.show_timing = default.IS_SHOW_TIMING if show_timing is None else show_timing

        # List of timing dictionaries, e.g. [{"my_func": 1.2345}, {"context_block": 0.5432}, ...]
        self.timings: list[Dict[str, float]] = []

        # Streaming result fetches can number in the millions.  Keep their
        # exact-duration telemetry in one O(1) accumulator instead of growing
        # ``timings`` once per attempted fetch (including exhaustion/errors).
        self._aggregated_timing_indexes: Dict[str, int] = {}
        self._aggregated_timing_totals: Dict[str, float] = {}
        self._aggregated_timing_occurrences: Dict[str, int] = {}
        self._aggregated_timing_lock = threading.Lock()

        # Used for the decorator/context manager
        self._start_time = None

        # For manual measuring
        # fix_time is a "fixed" reference point (for capture_duration)
        self.fix_time = time.time()
        # _capture_start_time is reset each time we capture timing
        self._capture_start_time = time.time()

    def __call__(self, func: Callable) -> Callable:
        """
        When used as a decorator, measures the execution time of `func`.
        """

        @wraps(func)
        def wrapper(*args, **kwargs):
            # If not showing timing, just call the function without measuring
            if not self.show_timing:
                return func(*args, **kwargs)

            # Example: if there is a PyArrow Table in kwargs, show schema in JSON form
            formatted_kwargs: Dict[str, Any] = {}
            for key, value in kwargs.items():
                if isinstance(value, pyarrow.Table):
                    schema_dict = {field.name: str(field.type) for field in value.schema}
                    formatted_kwargs[key] = json.dumps(schema_dict)
                else:
                    formatted_kwargs[key] = value

            start_time = time.time()
            result = func(*args, **kwargs)
            elapsed_time = time.time() - start_time

            self.timings.append({func.__name__: round(elapsed_time, 6)})
            light_blue = "\033[94m"
            reset_color = "\033[0m"
            print(
                f"Function '{func.__name__}' took "
                f"{light_blue}{elapsed_time:.4f}{reset_color} seconds to execute."
            )
            return result

        return wrapper

    def __enter__(self) -> "Timer":
        """
        Allows the Timer to be used as a context manager to measure a code block.
        """
        if self.show_timing:
            self._start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Completes timing for the code block (context manager usage).
        """
        if self.show_timing and self._start_time is not None:
            elapsed_time = time.time() - self._start_time
            self.timings.append({"context_block": round(elapsed_time, 6)})

            light_blue = "\033[94m"
            reset_color = "\033[0m"
            print(
                f"Block took {light_blue}{elapsed_time:.4f}{reset_color} seconds to execute."
            )

    def capture_and_reset_timing(self, event: str) -> None:
        """
        Captures the time since the last capture (or since initialization),
        appends it as {event: elapsed_time}, and resets the timer for subsequent calls.
        """
        elapsed_time = round(time.time() - self._capture_start_time, 6)
        self.timings.append({event: elapsed_time})
        self._capture_start_time = time.time()

    def capture_duration(self, event: str) -> None:
        """
        Captures the time since this Timer was created (self.fix_time) without resetting it.
        Appends the duration as {event: elapsed_time}.
        """
        elapsed_time = round(time.time() - self.fix_time, 6)
        self.timings.append({event: elapsed_time})

    @staticmethod
    def _validated_elapsed(event: str, elapsed_seconds: float) -> float:
        """Return a validated exact duration shared by capture methods."""

        if not isinstance(event, str) or not event or len(event) > 64:
            raise ValueError("event must be a non-empty string of at most 64 characters")
        if (
            isinstance(elapsed_seconds, bool)
            or not isinstance(elapsed_seconds, (int, float))
            or not math.isfinite(float(elapsed_seconds))
            or float(elapsed_seconds) < 0
        ):
            raise ValueError("elapsed_seconds must be finite and nonnegative")
        return float(elapsed_seconds)

    def capture_timing(self, event: str, elapsed_seconds: float) -> None:
        """Record an already measured monotonic duration without resetting.

        Engine phases overlap legacy wall-clock boundaries, so forcing them
        through ``capture_and_reset_timing`` would make phase labels depend on
        whichever callback happened previously.  Callers measure with
        ``time.perf_counter`` and provide only finite nonnegative seconds.
        """

        elapsed = self._validated_elapsed(event, elapsed_seconds)
        self.timings.append({event: round(elapsed, 6)})

    def capture_aggregated_timing(
        self, event: str, elapsed_seconds: float,
    ) -> None:
        """Accumulate a repeated exact-duration event in constant space."""

        elapsed = self._validated_elapsed(event, elapsed_seconds)
        with self._aggregated_timing_lock:
            index = self._aggregated_timing_indexes.get(event)
            total = self._aggregated_timing_totals.get(event, 0.0) + elapsed
            # Adding two finite floats can overflow.  Saturating keeps the
            # diagnostic state finite; the profile layer independently drops
            # durations outside its public 24-hour bound.
            if not math.isfinite(total):
                total = float.fromhex("0x1.fffffffffffffp+1023")
            self._aggregated_timing_totals[event] = total
            self._aggregated_timing_occurrences[event] = (
                self._aggregated_timing_occurrences.get(event, 0) + 1
            )
            if index is None:
                self._aggregated_timing_indexes[event] = len(self.timings)
                self.timings.append({event: round(total, 6)})
            else:
                self.timings[index][event] = round(total, 6)

    def timing_occurrences(self, event: str) -> int:
        """Return the number of calls folded into an aggregated event."""

        with self._aggregated_timing_lock:
            return self._aggregated_timing_occurrences.get(event, 0)
