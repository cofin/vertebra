"""Pytest configuration for Vertebra test suite.

AnyIO is the async backend — no pytest-asyncio. Use @pytest.mark.anyio
for async tests. All async tests run under asyncio by default.
"""

import uuid
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest


# ---------------------------------------------------------------------------
# AnyIO configuration
# ---------------------------------------------------------------------------

@pytest.fixture(params=["asyncio"])
def anyio_backend(request: pytest.FixtureRequest) -> str:
    """AnyIO backend fixture — asyncio only (trio not used)."""
    return request.param


# ---------------------------------------------------------------------------
# Shared path fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def project_dir(tmp_path: Path) -> Path:
    """Temporary project directory with minimal structure."""
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "test-app"\n')
    return tmp_path


@pytest.fixture()
def codegen_dir(tmp_path: Path) -> Path:
    """Temporary codegen output directory with stub type files."""
    d = tmp_path / "codegen"
    d.mkdir()
    (d / "types.d.ts").write_text("export type Stub = string;")
    (d / "schemas.zod.ts").write_text("// zod stubs")
    (d / "routes.ts").write_text("// route stubs")
    (d / "manifest.json").write_text("{}")
    return d


# ---------------------------------------------------------------------------
# Shared memory cleanup
# ---------------------------------------------------------------------------

@pytest.fixture()
def shm_name() -> str:
    """Unique shared memory name to avoid test collisions."""
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture(autouse=True)
def _cleanup_shm() -> Any:
    """Clean up stale shared memory after each test."""
    yield
    import glob
    import os

    for pattern in ("/dev/shm/test-*", "/dev/shm/*vertebra*"):
        for path in glob.glob(pattern):
            try:
                os.unlink(path)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# ASGI test helpers
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_asgi_app() -> AsyncMock:
    """Mock ASGI application callable."""
    return AsyncMock()


@pytest.fixture()
def asgi_send() -> AsyncMock:
    """Mock ASGI send callable that records messages."""
    send = AsyncMock()
    send.messages: list[dict] = []  # type: ignore[attr-defined]

    async def _send(message: dict[str, Any]) -> None:
        send.messages.append(message)  # type: ignore[attr-defined]

    send.side_effect = _send
    return send


@pytest.fixture()
def asgi_receive() -> AsyncMock:
    """Mock ASGI receive callable."""
    return AsyncMock(return_value={"type": "http.request", "body": b""})
