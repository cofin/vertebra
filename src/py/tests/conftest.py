"""Pytest configuration for Vertebra test suite.

AnyIO is the async backend — no pytest-asyncio. Use @pytest.mark.anyio
for async tests. All async tests run under asyncio by default.
"""

import contextlib
import glob
import importlib.util
import os
import uuid
from typing import Any

import pytest

_CANDIDATE_DB_PLUGINS = (
    "pytest_databases.docker.postgres",
    "pytest_databases.docker.mysql",
    "pytest_databases.docker.mariadb",
    "pytest_databases.docker.oracle",
    "pytest_databases.docker.mssql",
    "pytest_databases.docker.redis",
    "pytest_databases.docker.valkey",
    "pytest_databases.docker.rustfs",
    "pytest_databases.docker.minio",
    "pytest_databases.docker.floci",
    "pytest_databases.docker.azure_blob",
)

pytest_plugins = [
    plugin
    for plugin in _CANDIDATE_DB_PLUGINS
    if importlib.util.find_spec("pytest_databases") is not None and importlib.util.find_spec(plugin) is not None
]


@pytest.fixture(params=["asyncio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    """AnyIO backend fixture — asyncio only (trio not used)."""
    return request.param


@pytest.fixture()
def shm_name() -> str:
    """Unique shared memory name to avoid test collisions."""
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
def _cleanup_shm() -> Any:
    """Clean up stale shared memory after each test."""
    yield
    for pattern in ("/dev/shm/test-*", "/dev/shm/vtb-*", "/dev/shm/*vertebra*"):
        for path in glob.glob(pattern):
            with contextlib.suppress(OSError):
                os.unlink(path)
