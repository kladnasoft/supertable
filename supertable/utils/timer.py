import json
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
        self.timings = []

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
