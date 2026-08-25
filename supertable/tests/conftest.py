"""Isolation fixtures for tests that exercise process-global storage access."""

import pytest


@pytest.fixture(autouse=True)
def reset_processing_storage():
    """Prevent one test's LocalStorage root from leaking into the next test."""
    import supertable.processing as processing

    processing._storage = None
    yield
    processing._storage = None
