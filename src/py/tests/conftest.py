"""Test configuration and fixtures for Vertebra."""

from __future__ import annotations

import contextlib
import os
import sys
from pathlib import Path

import pytest

# Set test environment before any other imports.
os.environ.update({"VERTEBRA_DEBUG": "true", "VERTEBRA_DEV_MODE": "true", "VERTEBRA_LOG_LEVEL": "40"})

# Ensure repo root is importable for tools/ helpers.
ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

pytest_plugins = ["tests.database"]


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--integration", action="store_true", default=False, help="Run integration tests that require external services"
    )


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: Execute integration tests (HTTP endpoints with full stack)")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not config.getoption("--integration"):
        skip_integration = pytest.mark.skip(reason="need --integration to run")
        for item in items:
            if item.get_closest_marker("integration"):
                item.add_marker(skip_integration)

    for item in items:
        fixturenames = getattr(item, "fixturenames", [])
        if item.get_closest_marker("postgres") or "postgres_service" in fixturenames:
            item.add_marker(pytest.mark.xdist_group(name="postgres"))
        if item.get_closest_marker("mysql") or "mysql_service" in fixturenames:
            item.add_marker(pytest.mark.xdist_group(name="mysql"))
        if item.get_closest_marker("redis") or "redis_service" in fixturenames:
            item.add_marker(pytest.mark.xdist_group(name="redis"))


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    return "asyncio"


def _cleanup_shared_memory() -> None:
    """Clean up any stale shared memory objects from previous test runs."""
    # Clean up /dev/shm entries (Linux)
    shm_dir = Path("/dev/shm")
    if shm_dir.is_dir():
        patterns = ["test-*", "*vertebra*", "*.control", "*.req", "*.resp"]
        for pattern in patterns:
            for path in shm_dir.glob(pattern):
                with contextlib.suppress(OSError):
                    path.unlink()

    # Try to unlink named shared memory objects via shm_unlink
    try:
        from vertebra._internal.shm import shm_unlink
    except ImportError:
        return

    # Common test shm names that might be left over
    test_names = ["/test-ring", "/test-region", "/vertebra-broker.control"]
    for name in test_names:
        with contextlib.suppress(Exception):
            shm_unlink(name)


@pytest.fixture(scope="session", autouse=True)
def cleanup_shared_memory_session() -> None:
    """Clean shared memory at session start and end."""
    _cleanup_shared_memory()
    yield  # type: ignore[misc]
    _cleanup_shared_memory()


@pytest.fixture(autouse=True)
def cleanup_shared_memory_per_test() -> None:
    """Clean shared memory before each test for crash recovery."""
    _cleanup_shared_memory()
    yield  # type: ignore[misc]
