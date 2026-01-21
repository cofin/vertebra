"""Test configuration and fixtures for Vertebra."""

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

# Ensure src/py is importable so pytest plugins can resolve tests.database.
TESTS_ROOT = ROOT / "src" / "py"
if str(TESTS_ROOT) not in sys.path:
    sys.path.insert(0, str(TESTS_ROOT))

pytest_plugins = ["tests.database"]


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: Execute integration tests (HTTP endpoints with full stack)")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    # Group tests by database type for pytest-xdist parallel execution
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
    """Clean up any stale shared memory objects from previous test runs.

    This function is intentionally aggressive in its cleanup patterns to
    ensure no shared memory from previous test runs interferes with
    current tests. It uses multiple strategies:

    1. File-based cleanup via /dev/shm (Linux)
    2. shm_unlink for known test names (cross-platform)

    The function silently ignores errors, as shared memory may not exist
    or may be in use by another process.
    """
    # Clean up /dev/shm entries (Linux)
    shm_dir = Path("/dev/shm")
    if shm_dir.is_dir():
        # Comprehensive patterns to catch all naming conventions
        patterns = ["test-*", "*vertebra*", "*.control", "*.req", "*.resp", "*asgi*", "*broker*"]
        for pattern in patterns:
            for path in shm_dir.glob(pattern):
                with contextlib.suppress(OSError):
                    path.unlink()

    # Try to unlink named shared memory objects via shm_unlink
    try:
        from vertebra.typing import shm_unlink
    except ImportError:
        return

    # Common test shared memory names that might be left over
    # These are the hardcoded names used before we introduced unique names
    test_names = [
        "/test-ring",
        "/test-region",
        "/vertebra-broker.control",
        "/vertebra-broker-test.control",
        "/vertebra-broker-test.client.test-client.req",
        "/vertebra-broker-test.client.test-client.resp",
        "/vertebra-asgi-test.asgi.0.control",
        "/vertebra-asgi-test.asgi.0.client.test-client.req",
        "/vertebra-asgi-test.asgi.0.client.test-client.resp",
        "/vertebra-asgi-test.asgi.1.control",
    ]
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
    """Clean shared memory before AND after each test.

    This ensures proper cleanup even when tests fail mid-execution.
    """
    _cleanup_shared_memory()
    yield  # type: ignore[misc]
    _cleanup_shared_memory()
