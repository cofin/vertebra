"""Test configuration and fixtures for Vertebra."""

from __future__ import annotations

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
