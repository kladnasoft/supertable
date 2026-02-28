# conftest.py — shared pytest configuration for Dockerfile integration tests.
# Place this file alongside test_dockerfile.py (e.g. in tests/docker/ or project root).

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--image",
        default="supertable:test",
        help="Docker image tag to run integration tests against (default: supertable:test)",
    )


@pytest.fixture(scope="session")
def image(request) -> str:
    return request.config.getoption("--image")