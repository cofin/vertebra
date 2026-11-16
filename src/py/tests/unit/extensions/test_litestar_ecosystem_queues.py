"""Unit tests for Vertebra litestar-queues integration."""

from __future__ import annotations

import ast
import asyncio
from collections.abc import Sequence
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from litestar_queues.models import HeartbeatTouch

from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry
from vertebra.extensions.litestar.queues import (
    ContainerTaskDependencyProvider,
    SHMExecutionBackend,
    SHMQueueBackend,
    SHMQueueBackendConfig,
    StoreQueueBackend,
    StoreQueueBackendConfig,
    create_shm_queue_config,
    create_sqlspec_queue_config,
    create_store_queue_config,
)
from vertebra.framework.di import Container
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module


class InMemoryAsyncQueueStore(AsyncQueueStore):
    """In-memory AsyncQueueStore implementation for testing StoreQueueBackend."""

    def __init__(self) -> None:
        self.entries: dict[str, QueueEntry] = {}
        self.claimed: dict[str, str] = {}
        self.completed: set[str] = set()
        self.closed: bool = False

    async def enqueue(self, entry: QueueEntry) -> str:
        self.entries[entry.job_id] = entry
        return entry.job_id

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        for entry in entries:
            self.entries[entry.job_id] = entry
        return [entry.job_id for entry in entries]

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        claimed_entries: list[QueueEntry] = []
        for entry in list(self.entries.values()):
            if entry.queue in queues and entry.job_id not in self.claimed and entry.job_id not in self.completed:
                self.claimed[entry.job_id] = worker_id
                claimed_entries.append(entry)
                if len(claimed_entries) >= limit:
                    break
        return claimed_entries

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        self.claimed.pop(job_id, None)
        self.completed.add(job_id)
        return True

    async def nack(
        self,
        job_id: str,
        attempt: int,
        error: str | None = None,
        retry_delay_s: float | None = None,
    ) -> bool:
        self.claimed.pop(job_id, None)
        return True

    async def cancel(self, job_id: str) -> bool:
        if job_id in self.entries:
            self.entries.pop(job_id, None)
            self.claimed.pop(job_id, None)
            return True
        return False

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        count = 0
        for jid in job_ids:
            if jid in self.claimed and self.claimed[jid] == worker_id:
                count += 1
        return count

    async def reap_stale(self, lease_seconds: float = 300.0) -> int:
        reaped = len(self.claimed)
        self.claimed.clear()
        return reaped

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        return self.entries.get(job_id)

    async def list_entries(
        self,
        queue: str,
        *,
        status: str | None = "ready",
        limit: int = 100,
    ) -> list[QueueEntry]:
        results: list[QueueEntry] = []
        for entry in self.entries.values():
            if entry.queue != queue:
                continue
            if (
                status == "ready"
                and entry.job_id not in self.claimed
                and entry.job_id not in self.completed
                or status == "completed"
                and entry.job_id in self.completed
            ):
                results.append(entry)
            if len(results) >= limit:
                break
        return results

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        return None

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        return 0

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        return True

    async def release_unique_key(self, key: str) -> bool:
        return True

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        return True

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        return True

    async def ensure_schema(self) -> None:
        pass

    async def aclose(self) -> None:
        self.closed = True


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
    assert isinstance(cfg.queue_backend, SHMQueueBackendConfig)
    assert cfg.execution_backend == "vertebra.extensions.litestar.queues.backend.SHMExecutionBackend"

    resolved_queue = cfg.get_queue_backend()
    assert isinstance(resolved_queue, SHMQueueBackend)
    resolved_exec = cfg.get_execution_backend()
    assert isinstance(resolved_exec, SHMExecutionBackend)


def test_create_sqlspec_queue_config() -> None:
    """Verify create_sqlspec_queue_config constructs SQLSpecBackendConfig and channels events."""
    sqlspec_cfg_mock = MagicMock()
    cfg = create_sqlspec_queue_config(
        sqlspec_config=sqlspec_cfg_mock,
        queue_store_class=MagicMock,
        events_store_class=MagicMock,
    )
    assert cfg.events is not None
    assert cfg.worker is not None


def test_create_store_queue_config() -> None:
    """Verify create_store_queue_config produces a valid StoreQueueBackend configuration."""
    store = InMemoryAsyncQueueStore()
    cfg = create_store_queue_config(store=store, terminal_ttl=timedelta(minutes=5))
    assert isinstance(cfg.queue_backend, StoreQueueBackendConfig)
    assert cfg.queue_backend.backend_name == "vertebra_store"
    assert cfg.queue_backend.terminal_ttl == timedelta(minutes=5)

    backend = cfg.get_queue_backend()
    assert isinstance(backend, StoreQueueBackend)
    assert backend.store is store
    assert backend.terminal_ttl == timedelta(minutes=5)


@pytest.mark.anyio
async def test_store_queue_backend_lifecycle() -> None:
    """Verify enqueue, claim, complete, fail, and lifecycle methods on StoreQueueBackend."""
    store = InMemoryAsyncQueueStore()
    backend = StoreQueueBackend(store=store)

    assert backend.capabilities.supports_maintenance is True

    record = await backend.enqueue(
        task_name="send_email",
        args=("user@example.com",),
        kwargs={"subject": "Welcome"},
        queue="emails",
        max_retries=2,
    )
    assert record.task_name == "send_email"
    assert record.queue == "emails"
    assert record.max_retries == 2

    claimed = await backend.claim_many(limit=5, queues=("emails",))
    assert len(claimed) == 1
    assert claimed[0].id == record.id
    assert claimed[0].task_name == "send_email"

    heartbeats = await backend.touch_heartbeats([HeartbeatTouch(task_id=record.id, expected_retry_count=0)])
    assert record.id in heartbeats.touched_task_ids

    await backend.complete_task(record.id)
    assert str(record.id) in store.completed

    rec2 = await backend.enqueue(task_name="failing_task", queue="default")
    await backend.fail_task(rec2.id, error="failed deliberately", retry=True)

    recovery = await backend.requeue_stale_running()
    assert recovery.requeued == 0

    await backend.close()
    assert store.closed is True


def test_store_queue_backend_no_vendor_imports() -> None:
    """Verify StoreQueueBackend in backend.py has zero vendor database imports."""
    backend_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "vertebra"
        / "extensions"
        / "litestar"
        / "queues"
        / "backend.py"
    )
    source = backend_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(backend_path))

    forbidden_prefixes = (
        "asyncpg",
        "psycopg",
        "psqlpy",
        "oracledb",
        "aiomysql",
        "asyncmy",
        "mysqlconnector",
        "sqlite3",
        "aiosqlite",
        "pymssql",
        "mssql_python",
    )

    imported_names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported_names.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_names.append(node.module)

    for imp in imported_names:
        for prefix in forbidden_prefixes:
            assert not imp.startswith(prefix), f"Forbidden vendor import {imp} found in backend.py"


@pytest.mark.anyio
async def test_container_task_dependency_provider() -> None:
    """Verify ContainerTaskDependencyProvider resolves dependencies within Container.scope()."""

    class ConfigService:
        def __init__(self) -> None:
            self.app_name = "VertebraApp"

    container = Container(singletons={"config": ConfigService()})

    provider = ContainerTaskDependencyProvider(container)

    def sample_task(config: ConfigService) -> str:
        return config.app_name

    task_mock = MagicMock()
    task_mock.fn = sample_task
    record_mock = MagicMock()
    record_mock.id = uuid4()

    async with provider(task_mock, record_mock, context=None) as resolved_deps:
        assert "config" in resolved_deps
        assert isinstance(resolved_deps["config"], ConfigService)
        assert resolved_deps["config"].app_name == "VertebraApp"


def test_missing_dependency_error_on_missing_queues(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify MissingDependencyError with extra='queues' when litestar_queues is absent."""
    monkeypatch.setattr("vertebra.utils.module_loader.import_optional", lambda name: None)
    with pytest.raises(MissingDependencyError) as exc_info:
        ensure_module("litestar_queues", package="litestar-queues", extra="queues")
    assert "queues" in exc_info.value.install_package

    with pytest.raises(MissingDependencyError) as shm_cfg_exc:
        create_shm_queue_config()
    assert shm_cfg_exc.value.package == "litestar-queues"
    assert shm_cfg_exc.value.install_package == "vertebra-core[queues]"

    with pytest.raises(MissingDependencyError) as shm_exec_exc:
        SHMExecutionBackend()
    assert shm_exec_exc.value.package == "litestar-queues"
    assert shm_exec_exc.value.install_package == "vertebra-core[queues]"
