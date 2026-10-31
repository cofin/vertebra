"""Unit tests for Vertebra litestar-queues integration."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from vertebra.extensions.litestar.queues import (
    SHMExecutionBackend,
    SHMQueueBackend,
    create_shm_queue_config,
    create_sqlspec_queue_config,
)
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module


def test_shm_queue_backend_adaptive_clamping_bounds() -> None:
    """Verify adaptive clamping bounds clamping behavior."""
    backend = SHMQueueBackend(min_wakeup_timeout=0.01, max_wakeup_timeout=1.0)
    assert backend.min_wakeup_timeout == 0.01
    assert backend.max_wakeup_timeout == 1.0


@pytest.mark.anyio
async def test_shm_queue_backend_wait_for_wakeups_timeout() -> None:
    """Verify wait_for_wakeups returns False on timeout."""
    backend = SHMQueueBackend(min_wakeup_timeout=0.001, max_wakeup_timeout=0.005)
    result = await backend.wait_for_wakeups(timeout=0.002)
    assert result is False


@pytest.mark.anyio
async def test_shm_queue_backend_wait_for_wakeups_notification() -> None:
    """Verify wait_for_wakeups returns True when notified."""
    backend = SHMQueueBackend(min_wakeup_timeout=0.001, max_wakeup_timeout=1.0)

    async def notify_later() -> None:
        await asyncio.sleep(0.005)
        backend._notification_event.set()

    task = asyncio.create_task(notify_later())
    result = await backend.wait_for_wakeups(timeout=0.5)
    await task
    assert result is True


@pytest.mark.anyio
async def test_shm_execution_backend_execute() -> None:
    """Verify SHMExecutionBackend executes records directly via the queue service."""
    backend = SHMExecutionBackend()
    service_mock = MagicMock()
    record_mock = MagicMock()
    service_mock.execute_record = AsyncMock(return_value=record_mock)

    executed = await backend.execute(service_mock, record_mock, worker_id="test-worker")
    assert executed is record_mock
    service_mock.execute_record.assert_awaited_once_with(record_mock, worker_id="test-worker")


def test_create_shm_queue_config() -> None:
    """Verify create_shm_queue_config produces a valid QueueConfig."""
    cfg = create_shm_queue_config()
    assert cfg.queue_backend == "vertebra.extensions.litestar.queues.backend.SHMQueueBackend"
    assert cfg.execution_backend == "vertebra.extensions.litestar.queues.backend.SHMExecutionBackend"

    resolved_queue = cfg.get_queue_backend()
    assert isinstance(resolved_queue, SHMQueueBackend)
    resolved_exec = cfg.get_execution_backend()
    assert isinstance(resolved_exec, SHMExecutionBackend)


def test_create_sqlspec_queue_config() -> None:
    """Verify create_sqlspec_queue_config constructs SQLSpecBackendConfig and channels events."""
    sqlspec_cfg_mock = MagicMock()
    cfg = create_sqlspec_queue_config(sqlspec_config=sqlspec_cfg_mock)
    assert cfg.events is not None
    assert cfg.worker is not None


def test_missing_dependency_error_on_missing_queues(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError with extra='queues' when litestar_queues is absent."""
    monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError) as exc_info:
        ensure_module("litestar_queues", package="litestar-queues", extra="queues")
    assert "queues" in exc_info.value.install_package
