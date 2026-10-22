"""Pytest configuration for Vertebra test suite.

AnyIO is the async backend — no pytest-asyncio. Use @pytest.mark.anyio
for async tests. All async tests run under asyncio by default.
"""

import contextlib
import glob
import os
import uuid
from typing import Any

import pytest


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
