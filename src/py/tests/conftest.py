"""Pytest configuration for Vertebra test suite.

AnyIO is the async backend — no pytest-asyncio. Use @pytest.mark.anyio
for async tests. All async tests run under asyncio by default.
"""

import glob
import importlib.util
import os
import uuid
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any, Final

import pytest

_SHM_ROOT: Final[Path] = Path("/dev/shm")
_BROAD_SHM_PATTERNS: Final[tuple[str, ...]] = ("test-*", "vtb-*", "*vertebra*")

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
    return str(request.param)


def _is_xdist_worker(environ: Mapping[str, str] | None = None) -> bool:
    """Return True when running inside a pytest-xdist worker process."""
    env = os.environ if environ is None else environ
    return "PYTEST_XDIST_WORKER" in env


def _post_test_patterns(issued: Sequence[str], *, is_worker: bool) -> tuple[str, ...]:
    """Return shared-memory glob patterns to sweep after a single test."""
    if is_worker:
        return tuple(f"{glob.escape(name)}*" for name in issued)
    return _BROAD_SHM_PATTERNS


def _sweep_shm(root: Path, patterns: Iterable[str]) -> int:
    """Remove matching shared-memory entries under root and return the removed count."""
    if not root.is_dir():
        return 0
    removed = 0
    for pattern in patterns:
        for entry in root.glob(pattern):
            try:
                entry.unlink()
            except OSError:
                continue
            removed += 1
    return removed


@pytest.fixture()
def _issued_shm_names() -> list[str]:
    """Track shared-memory names issued by shm_name during a single test."""
    return []


@pytest.fixture()
def shm_name(_issued_shm_names: list[str]) -> str:
    """Unique shared memory name to avoid test collisions."""
    name = f"test-{uuid.uuid4().hex[:8]}"
    _issued_shm_names.append(name)
    return name


@pytest.fixture(autouse=True)
def _cleanup_shm(_issued_shm_names: list[str]) -> Any:
    """Clean up stale shared memory after each test."""
    yield
    _sweep_shm(
        _SHM_ROOT,
        _post_test_patterns(_issued_shm_names, is_worker=_is_xdist_worker()),
    )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """Sweep remaining test shared-memory segments when the controller session ends."""
    del session, exitstatus
    if not _is_xdist_worker():
        _sweep_shm(_SHM_ROOT, _BROAD_SHM_PATTERNS)
