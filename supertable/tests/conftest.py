"""Fixtures every test in this directory runs under."""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _no_key_values_from_another_test():
    """Start every test with a process that knows no data file.

    The write path keeps the key values of small data files for the life of
    the process, by path and size (``processing._FILE_KEY_VALUES``).  A real
    file name is never used twice; the names tests make up (``file1.parquet``,
    size 1000) are, with other rows behind them, and what one test taught the
    process would drop a file from another test's candidates.
    """
    from supertable import processing

    processing._FILE_KEY_VALUES.clear()
    yield
    processing._FILE_KEY_VALUES.clear()
