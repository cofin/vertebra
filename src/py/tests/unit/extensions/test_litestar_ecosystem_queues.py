"""Unit tests for Vertebra litestar-queues integration."""

from __future__ import annotations

import ast
import asyncio
import multiprocessing
import time
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import msgspec
import pytest
from litestar_queues import QueueConfig, QueueService, Worker, WorkerConfig, task
from litestar_queues.backends.base import BaseQueueBackend
from litestar_queues.backends.memory import InMemoryQueueBackend
from litestar_queues.models import HeartbeatTouch, TaskRequest, TaskReservation

from vertebra._vertebra import ShmQueueRing
from vertebra.db.driver.capabilities import AsyncQueueStore, QueueEntry, QueueReservation
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
from vertebra.framework.queues.backends import SHMQueueBackend as FrameworkSHMQueueBackend
from vertebra.framework.queues.base import QueueConfig as FrameworkQueueConfig
from vertebra.framework.queues.base import TaskEnvelope
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module


@task("vertebra_test_cross_process_sum")
def _cross_process_sum(a: int, b: int) -> int:
    return a + b


def _spawn_producer_enqueue(ring_name: str, result_queue: Any) -> None:
    async def _run() -> str:
        backend = SHMQueueBackend(ring_name=ring_name)
        record = await backend.enqueue("vertebra_test_cross_process_sum", args=(19, 23))
        await backend.close()
        return str(record.id)

    task_id = asyncio.run(_run())
    result_queue.put(task_id)


def _spawn_consumer_worker(ring_name: str, task_id_str: str, result_queue: Any) -> None:
    async def _run() -> tuple[int, str, Any]:
        backend = SHMQueueBackend(ring_name=ring_name)
        service = QueueService(QueueConfig(), queue_backend=backend)
        worker = Worker(service)
        processed = await worker.run_once()
        await worker.stop()
        record = await backend.get_task(UUID(task_id_str))
        await backend.close()
        assert record is not None
        return processed, record.status, record.result

    result_queue.put(asyncio.run(_run()))


def _spawn_concurrent_claimer(ring_name: str, worker_tag: str, result_queue: Any) -> None:
    async def _run() -> list[str]:
        backend = SHMQueueBackend(ring_name=ring_name)
        claimed_ids: list[str] = []
        while True:
            batch = await backend.claim_many(limit=2)
            if not batch:
                break
            for rec in batch:
                assigned = await backend.assign_worker(
                    rec.id,
                    worker_id=worker_tag,
                    expected_retry_count=rec.retry_count,
                )
                if assigned is not None:
                    claimed_ids.append(str(assigned.id))
        await backend.close()
        return claimed_ids

    result_queue.put(asyncio.run(_run()))


class InMemoryAsyncQueueStore(AsyncQueueStore):
    """In-memory AsyncQueueStore implementation for testing StoreQueueBackend."""

    def __init__(self) -> None:
        self.entries: dict[str, QueueEntry] = {}
        self.statuses: dict[str, str] = {}
        self.claimed: dict[str, str] = {}
        self.completed: set[str] = set()
        self.reservations: dict[str, QueueReservation] = {}
        self.unique_keys: set[str] = set()
        self.leases: dict[str, tuple[str, float]] = {}
        self.enqueue_many_calls: int = 0
        self.closed: bool = False

    async def enqueue(self, entry: QueueEntry) -> str:
        self.entries[entry.job_id] = entry
        self.statuses[entry.job_id] = "ready"
        return entry.job_id

    async def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]:
        self.enqueue_many_calls += 1
        for entry in entries:
            self.entries[entry.job_id] = entry
            self.statuses[entry.job_id] = "ready"
        return [entry.job_id for entry in entries]

    async def claim(
        self,
        queues: Sequence[str],
        *,
        limit: int = 1,
        worker_id: str,
        lease_seconds: float = 30.0,
    ) -> list[QueueEntry]:
        now_ms = int(time.time() * 1000)
        lease_ms = now_ms + int(lease_seconds * 1000)
        claimed_entries: list[QueueEntry] = []
        for jid, entry in list(self.entries.items()):
            if entry.queue not in queues:
                continue
            status = self.statuses.get(jid, "ready")
            if status != "ready":
                continue
            if entry.scheduled_at_ms is not None and entry.scheduled_at_ms > now_ms:
                continue
            if entry.expires_at_ms is not None and entry.expires_at_ms <= now_ms:
                continue
            updated = msgspec.structs.replace(entry, lease_until_ms=lease_ms, worker_id=worker_id)
            self.entries[jid] = updated
            self.statuses[jid] = "claimed"
            self.claimed[jid] = worker_id
            claimed_entries.append(updated)
            if len(claimed_entries) >= limit:
                break
        return claimed_entries

    async def claim_job(
        self,
        job_id: str,
        *,
        worker_id: str,
        lease_seconds: float = 30.0,
        expected_attempt: int | None = None,
    ) -> QueueEntry | None:
        entry = self.entries.get(job_id)
        if entry is None or self.statuses.get(job_id) != "ready":
            return None
        if expected_attempt is not None and entry.attempt != expected_attempt:
            return None
        now_ms = int(time.time() * 1000)
        if entry.scheduled_at_ms is not None and entry.scheduled_at_ms > now_ms:
            return None
        updated = msgspec.structs.replace(
            entry,
            lease_until_ms=now_ms + int(lease_seconds * 1000),
            worker_id=worker_id,
        )
        self.entries[job_id] = updated
        self.statuses[job_id] = "claimed"
        self.claimed[job_id] = worker_id
        return updated

    async def ack(self, job_id: str, *, attempt: int) -> bool:
        entry = self.entries.get(job_id)
        if entry is None or self.statuses.get(job_id) != "claimed" or entry.attempt != attempt:
            return False
        now_ms = int(time.time() * 1000)
        completed_ms = entry.completed_at_ms if entry.completed_at_ms is not None else now_ms
        self.entries[job_id] = msgspec.structs.replace(entry, lease_until_ms=0, completed_at_ms=completed_ms)
        self.statuses[job_id] = "completed"
        self.claimed.pop(job_id, None)
        self.completed.add(job_id)
        return True

    async def nack(
        self,
        job_id: str,
        *,
        attempt: int,
        error: str,
        retry_delay_s: float | None = None,
    ) -> bool:
        entry = self.entries.get(job_id)
        if entry is None or self.statuses.get(job_id) != "claimed" or entry.attempt != attempt:
            return False
        self.claimed.pop(job_id, None)
        now_ms = int(time.time() * 1000)
        if retry_delay_s is None or entry.attempt >= entry.max_attempts:
            self.entries[job_id] = msgspec.structs.replace(
                entry,
                lease_until_ms=0,
                completed_at_ms=entry.completed_at_ms if entry.completed_at_ms is not None else now_ms,
                error=error,
            )
            self.statuses[job_id] = "dead_letter"
            return True
        sched_ms = now_ms + int(retry_delay_s * 1000) if retry_delay_s > 0 else None
        self.entries[job_id] = msgspec.structs.replace(
            entry,
            attempt=entry.attempt + 1,
            lease_until_ms=0,
            scheduled_at_ms=sched_ms,
            worker_id=None,
            error=error,
        )
        self.statuses[job_id] = "ready"
        return True

    async def cancel(self, job_id: str) -> bool:
        entry = self.entries.get(job_id)
        if entry is None or self.statuses.get(job_id) not in {"ready", "claimed"}:
            return False
        now_ms = int(time.time() * 1000)
        self.entries[job_id] = msgspec.structs.replace(
            entry,
            lease_until_ms=0,
            completed_at_ms=entry.completed_at_ms if entry.completed_at_ms is not None else now_ms,
        )
        self.statuses[job_id] = "cancelled"
        self.claimed.pop(job_id, None)
        return True

    async def heartbeat(
        self,
        job_ids: Sequence[str],
        *,
        worker_id: str,
        lease_seconds: float,
    ) -> int:
        now_ms = int(time.time() * 1000)
        new_lease = now_ms + int(lease_seconds * 1000)
        count = 0
        for jid in job_ids:
            if jid in self.claimed and self.claimed[jid] == worker_id and jid in self.entries:
                self.entries[jid] = msgspec.structs.replace(self.entries[jid], lease_until_ms=new_lease)
                count += 1
        return count

    async def reap_stale(self, *, lease_seconds: float = 300.0) -> int:
        _ = lease_seconds
        reaped = len(self.claimed)
        for jid in list(self.claimed):
            if jid in self.entries:
                entry = self.entries[jid]
                self.entries[jid] = msgspec.structs.replace(entry, attempt=entry.attempt + 1, lease_until_ms=0)
                self.statuses[jid] = "ready"
        self.claimed.clear()
        return reaped

    async def get_entry(self, job_id: str) -> QueueEntry | None:
        return self.entries.get(job_id)

    async def get_entry_status(self, job_id: str) -> str | None:
        return self.statuses.get(job_id)

    async def list_entries(
        self,
        queue: str | None = None,
        *,
        status: str | None = "ready",
        limit: int = 100,
    ) -> list[QueueEntry]:
        results: list[QueueEntry] = []
        for jid, entry in self.entries.items():
            if queue is not None and entry.queue != queue:
                continue
            row_status = self.statuses.get(jid, "ready")
            if status is None or row_status == status:
                results.append(entry)
            if len(results) >= limit:
                break
        return results

    async def cleanup_terminal(self, *, before_ms: int, limit: int | None = None) -> int:
        terminal_statuses = {"completed", "dead_letter", "cancelled", "expired", "interrupted"}
        deleted = 0
        for jid in list(self.entries):
            if limit is not None and deleted >= limit:
                break
            entry = self.entries[jid]
            row_status = self.statuses.get(jid)
            if (
                row_status in terminal_statuses
                and entry.completed_at_ms is not None
                and entry.completed_at_ms < before_ms
            ):
                del self.entries[jid]
                self.statuses.pop(jid, None)
                self.completed.discard(jid)
                deleted += 1
        return deleted

    async def get_entry_by_key(self, key: str, *, queue: str | None = None) -> QueueEntry | None:
        for entry in self.entries.values():
            if queue is not None and entry.queue != queue:
                continue
            if entry.headers.get("key") == key or entry.headers.get("idempotency_key") == key:
                return entry
        return None

    async def reserve_identity(
        self,
        key: str,
        *,
        job_id: str,
        task_name: str,
        ttl_seconds: float | None = None,
    ) -> QueueReservation | None:
        _ = ttl_seconds
        existing = self.reservations.get(key)
        if existing is not None:
            return existing
        self.reservations[key] = QueueReservation(
            key=key,
            job_id=job_id,
            task_name=task_name,
            created_at_ms=int(time.time() * 1000),
        )
        return None

    async def get_reservation(self, key: str) -> QueueReservation | None:
        return self.reservations.get(key)

    async def reset_identity(self, key: str, *, expected_job_id: str | None = None) -> bool:
        existing = self.reservations.get(key)
        if existing is None:
            return False
        if expected_job_id is not None and existing.job_id != expected_job_id:
            return False
        del self.reservations[key]
        return True

    async def assign_worker(self, job_id: str, *, worker_id: str, attempt: int) -> bool:
        entry = self.entries.get(job_id)
        if entry is None or self.statuses.get(job_id) != "claimed" or entry.attempt != attempt:
            return False
        self.entries[job_id] = msgspec.structs.replace(entry, worker_id=worker_id)
        self.claimed[job_id] = worker_id
        return True

    async def touch_heartbeats(self, job_ids: Sequence[str], *, heartbeat_at_ms: int) -> int:
        touched = 0
        for jid in job_ids:
            entry = self.entries.get(jid)
            if entry is not None and self.statuses.get(jid) == "claimed":
                self.entries[jid] = msgspec.structs.replace(entry, heartbeat_at_ms=heartbeat_at_ms)
                touched += 1
        return touched

    async def null_heartbeats(self, job_ids: Sequence[str]) -> int:
        cleared = 0
        for jid in job_ids:
            entry = self.entries.get(jid)
            if entry is not None and entry.heartbeat_at_ms is not None:
                self.entries[jid] = msgspec.structs.replace(entry, heartbeat_at_ms=None)
                cleared += 1
        return cleared

    async def interrupt_task(self, job_id: str, *, error: str | None = None) -> bool:
        entry = self.entries.get(job_id)
        if entry is None or self.statuses.get(job_id) not in {"ready", "claimed"}:
            return False
        self.entries[job_id] = msgspec.structs.replace(
            entry,
            attempt=entry.attempt + 1,
            lease_until_ms=0,
            worker_id=None,
            heartbeat_at_ms=None,
            error=error,
        )
        self.statuses[job_id] = "interrupted"
        self.claimed.pop(job_id, None)
        return True

    async def expire_overdue(self, *, now_ms: int, limit: int | None = None) -> int:
        expired = 0
        for jid, entry in list(self.entries.items()):
            if limit is not None and expired >= limit:
                break
            if self.statuses.get(jid) == "ready" and entry.expires_at_ms is not None and entry.expires_at_ms <= now_ms:
                self.entries[jid] = msgspec.structs.replace(
                    entry,
                    lease_until_ms=0,
                    completed_at_ms=now_ms,
                )
                self.statuses[jid] = "expired"
                expired += 1
        return expired

    async def next_due_delay(self, queues: Sequence[str]) -> float | None:
        now_ms = int(time.time() * 1000)
        scheds = [
            entry.scheduled_at_ms
            for jid, entry in self.entries.items()
            if self.statuses.get(jid) == "ready"
            and entry.queue in queues
            and entry.scheduled_at_ms is not None
            and entry.scheduled_at_ms > now_ms
        ]
        if not scheds:
            return None
        return max(0.0, (min(scheds) - now_ms) / 1000.0)

    async def promote_scheduled(self, *, limit: int = 100) -> int:
        return 0

    async def reserve_unique_key(self, key: str, *, ttl_seconds: float) -> bool:
        _ = ttl_seconds
        if key in self.unique_keys:
            return False
        self.unique_keys.add(key)
        return True

    async def release_unique_key(self, key: str) -> bool:
        if key in self.unique_keys:
            self.unique_keys.remove(key)
            return True
        return False

    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool:
        now = time.time()
        existing = self.leases.get(lease_key)
        if existing is None or existing[1] <= now or existing[0] == owner_id:
            self.leases[lease_key] = (owner_id, now + ttl_seconds)
            return True
        return False

    async def release_lease(self, lease_key: str, *, owner_id: str) -> bool:
        existing = self.leases.get(lease_key)
        if existing is not None and existing[0] == owner_id:
            del self.leases[lease_key]
            return True
        return False

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


def test_shm_queue_backend_inherits_base_queue_backend_directly() -> None:
    """Verify SHMQueueBackend subclasses BaseQueueBackend and never InMemoryQueueBackend."""
    assert issubclass(SHMQueueBackend, BaseQueueBackend)
    assert not issubclass(SHMQueueBackend, InMemoryQueueBackend)


def test_shm_queue_ring_bounds_and_futex_wait_wake() -> None:
    """Verify ShmQueueRing payload/capacity errors, idle wait timeout, and prompt push wakeup."""
    ring_name = f"vertebra-ring-bounds-{uuid4().hex}-{'x' * 36}"
    ring = ShmQueueRing(ring_name, capacity=2, slot_bytes=16)
    try:
        assert ring.name == ring_name
        assert ring.wait(timeout_ms=20) is False

        with pytest.raises(ValueError):
            ring.push(b"x" * 17)

        assert ring.push(b"first") == 1
        assert ring.push(b"second") == 2
        with pytest.raises(OverflowError):
            ring.push(b"third")

        assert ring.pop() == b"first"
        assert ring.pop() == b"second"
        assert ring.pop() is None
        assert ring.wait(timeout_ms=20) is False

        ring.wake(wake_all_waiters=False)
        ring.push(b"awake")
        assert ring.wait(timeout_ms=200) is True
        assert ring.pop() == b"awake"
    finally:
        ring.close()
        ShmQueueRing.unlink(ring_name)


def test_shm_queue_backend_multiprocess_producer_exit_and_worker_execution() -> None:
    """Verify producer exit persistence and consumer Worker.run_once execution over a long ring name."""
    ring_name = f"vertebra-long-cross-process-ring-{uuid4().hex}-{'z' * 24}"
    ctx = multiprocessing.get_context("spawn")
    producer_q: Any = ctx.Queue()
    consumer_q: Any = ctx.Queue()
    try:
        producer = ctx.Process(target=_spawn_producer_enqueue, args=(ring_name, producer_q))
        producer.start()
        producer.join(timeout=15)
        assert producer.exitcode == 0
        task_id_str = producer_q.get(timeout=5)

        consumer = ctx.Process(target=_spawn_consumer_worker, args=(ring_name, task_id_str, consumer_q))
        consumer.start()
        consumer.join(timeout=15)
        assert consumer.exitcode == 0
        processed, status, result = consumer_q.get(timeout=5)
        assert processed == 1
        assert status == "completed"
        assert result == 42
    finally:
        ShmQueueRing.unlink(ring_name)


@pytest.mark.anyio
async def test_shm_queue_backend_multiprocess_concurrent_claims_no_double_claim() -> None:
    """Verify two spawned worker processes racing claim_many never double-claim a task."""
    ring_name = f"vertebra-race-claim-ring-{uuid4().hex}-{'r' * 24}"
    ctx = multiprocessing.get_context("spawn")
    backend = SHMQueueBackend(ring_name=ring_name)
    try:
        expected_ids: set[str] = set()
        for idx in range(12):
            rec = await backend.enqueue("vertebra_test_cross_process_sum", args=(idx, 1))
            expected_ids.add(str(rec.id))
        await backend.close()

        q1: Any = ctx.Queue()
        q2: Any = ctx.Queue()
        p1 = ctx.Process(target=_spawn_concurrent_claimer, args=(ring_name, "w-1", q1))
        p2 = ctx.Process(target=_spawn_concurrent_claimer, args=(ring_name, "w-2", q2))
        p1.start()
        p2.start()
        p1.join(timeout=15)
        p2.join(timeout=15)
        assert p1.exitcode == 0
        assert p2.exitcode == 0

        claimed_1 = q1.get(timeout=5)
        claimed_2 = q2.get(timeout=5)
        assert set(claimed_1).isdisjoint(set(claimed_2))
        assert set(claimed_1) | set(claimed_2) == expected_ids
    finally:
        await backend.close()
        ShmQueueRing.unlink(ring_name)


@pytest.mark.anyio
async def test_shm_queue_backend_full_base_contract_surface() -> None:
    """Exercise the complete BaseQueueBackend method surface on SHMQueueBackend."""
    ring_name = f"vertebra-full-surface-ring-{uuid4().hex}-{'s' * 20}"
    backend = SHMQueueBackend(ring_name=ring_name)
    try:
        assert await backend.open() is True
        assert backend.capabilities.supports_worker_wakeups is True
        assert backend.capabilities.supports_maintenance is True

        rec_batch = await backend.enqueue_many(
            [
                TaskRequest(task_name="job_a", args=(1,), queue="q1", priority=5, key="dedup-a"),
                TaskRequest(task_name="job_a", args=(2,), queue="q1", priority=5, key="dedup-a"),
                TaskRequest(
                    task_name="job_future",
                    queue="q1",
                    scheduled_at=datetime.now(UTC) + timedelta(seconds=30),
                ),
            ]
        )
        assert len(rec_batch) == 3
        assert rec_batch[0].id == rec_batch[1].id

        by_key = await backend.get_task_by_key("dedup-a")
        assert by_key is not None
        assert by_key.id == rec_batch[0].id
        assert await backend.time_until_next_due(queues=("q1",)) is not None

        pending = await backend.list_pending(limit=10, queue="q1")
        assert [r.id for r in pending] == [rec_batch[0].id]

        claimed, expired = await backend.claim_many_with_expired(limit=2, queues=("q1",))
        assert [r.id for r in claimed] == [rec_batch[0].id]
        assert expired == []

        assigned = await backend.assign_worker(rec_batch[0].id, worker_id="w-alpha", expected_retry_count=0)
        assert assigned is not None
        assert assigned.worker_id == "w-alpha"

        hb_res = await backend.touch_heartbeats([HeartbeatTouch(task_id=rec_batch[0].id, expected_retry_count=0)])
        assert rec_batch[0].id in hb_res.touched_task_ids
        await backend.null_heartbeats([rec_batch[0].id], expected_retry_count=0)

        stale_recovery = await backend.requeue_stale_running(stale_after=timedelta(seconds=0))
        assert stale_recovery.failed == 1

        interrupted_rec = await backend.enqueue("interruptible", queue="q2", max_retries=1)
        claimed_int, _ = await backend.claim_task_with_expired(interrupted_rec.id)
        assert claimed_int is not None
        await backend.assign_worker(interrupted_rec.id, worker_id="w-beta", expected_retry_count=0)
        requeued_int = await backend.interrupt_task(
            interrupted_rec.id,
            expected_retry_count=0,
            worker_id="w-beta",
            queued_at=datetime.now(UTC),
        )
        assert requeued_int is not None
        assert requeued_int.status == "pending"
        assert requeued_int.retry_count == 1

        ext_rec = await backend.enqueue("ext_job", queue="q3", execution_backend="cloud_run")
        repair = await backend.list_dispatch_repair_candidates("cloud_run", limit=5)
        assert len(repair.records) == 1
        reserved_sched = await backend.reserve_scheduled_execution_ref(
            ext_rec.id,
            "cloud_run",
            "exec-1",
            expected_retry_count=0,
            expected_execution_ref=None,
        )
        assert reserved_sched is not None
        replaced = await backend.replace_execution_ref(ext_rec.id, 0, "exec-1", "exec-2")
        assert replaced is not None
        assert len(await backend.list_running_external()) == 1
        cleared = await backend.clear_execution_ref(ext_rec.id, 0, "exec-2")
        assert cleared is not None and cleared.execution_ref is None

        overdue_rec = await backend.enqueue(
            "overdue_job",
            queue="q4",
            expires_at=datetime.now(UTC) - timedelta(seconds=1),
        )
        expired_list = await backend.expire_overdue()
        assert [r.id for r in expired_list] == [overdue_rec.id]

        done_rec = await backend.enqueue("done_job", queue="q5")
        await backend.claim_task(done_rec.id)
        await backend.complete_task(done_rec.id, result={"ok": True}, expected_retry_count=0)
        assert await backend.wait_for_completion(done_rec.id, timeout=0.1) is True
        completed_list = await backend.list_completed_by_task("done_job")
        assert [r.id for r in completed_list] == [done_rec.id]

        cancel_rec = await backend.enqueue("cancel_target", queue="q6", metadata={"env": "test"})
        assert await backend.cancel_tasks(task_name="cancel_target", queue="q6", metadata={"env": "test"}) == 1
        cancelled = await backend.get_task(cancel_rec.id)
        assert cancelled is not None and cancelled.status == "cancelled"

        assert await backend.reserve_identity("forever-k", task_id=done_rec.id, task_name="done_job") is None
        assert await backend.has_identity("forever-k") is not None
        assert await backend.reset_identity("forever-k", expected_task_id=done_rec.id) is True

        assert await backend.acquire_maintenance("sweep", "tok-1", ttl=timedelta(seconds=5)) is True
        assert await backend.acquire_maintenance("sweep", "tok-2", ttl=timedelta(seconds=5)) is False
        assert await backend.release_maintenance("sweep", "tok-1") is True

        cleaned = await backend.cleanup_terminal(datetime.now(UTC) + timedelta(seconds=1))
        assert cleaned >= 3
    finally:
        await backend.close()
        ShmQueueRing.unlink(ring_name)


@pytest.mark.anyio
async def test_framework_shm_queue_backend_cross_handle_persistence() -> None:
    """Verify framework SHMQueueBackend persists tasks, leases, and identities in ShmQueueRing."""
    ring_name = f"vertebra-fw-shm-{uuid4().hex}-{'f' * 20}"
    cfg = FrameworkQueueConfig(backend_url=f"shm://{ring_name}", visibility_timeout_seconds=0.05)
    producer = FrameworkSHMQueueBackend(cfg)
    consumer = FrameworkSHMQueueBackend(cfg)
    try:
        env = TaskEnvelope(task_id="fw-task-1", task_name="compute", lane="cpu", args=(1,))
        await producer.enqueue(env)
        await producer.close()

        claimed = await consumer.dequeue(["cpu"], batch_size=1, timeout_seconds=0.1)
        assert len(claimed) == 1
        assert claimed[0].task_id == "fw-task-1"
        assert claimed[0].receipt_handle is not None
        assert await consumer.heartbeat(claimed[0], lease_seconds=1.0) is True
        await consumer.ack(claimed[0])

        assert await consumer.reserve_identity("fw-id", owner_id="worker-a") is True
        assert await consumer.reserve_identity("fw-id", owner_id="worker-b") is False
        assert await consumer.release_identity("fw-id", owner_id="worker-a") is True

        lease = await consumer.acquire_lease("cron:1", "worker-a", 5.0, slot="2026-01-01T00:00Z")
        assert lease is not None
        assert await consumer.acquire_lease("cron:1", "worker-b", 5.0) is None
        renewed = await consumer.renew_lease("cron:1", "worker-a", 10.0)
        assert renewed is not None
    finally:
        await consumer.close()
        ShmQueueRing.unlink(ring_name)


@task("vertebra_test_store_worker_record")
async def _store_worker_record_task(label: str, amount: int) -> dict[str, Any]:
    return {"label": label, "amount": amount * 2}


@pytest.mark.anyio
async def test_store_queue_backend_enqueue_many_preserves_fields_and_empty_noop() -> None:
    """Verify enqueue_many([]) is a no-op and batch TaskRequest fields survive StoreQueueBackend round-trips."""
    store = InMemoryAsyncQueueStore()
    backend = StoreQueueBackend(store=store)

    assert await backend.enqueue_many([]) == []
    assert store.enqueue_many_calls == 0

    expires_at = datetime.now(UTC) + timedelta(minutes=15)
    scheduled_at = datetime.now(UTC) - timedelta(seconds=1)
    requests = [
        TaskRequest(
            task_name="vertebra_test_store_worker_record",
            args=("invoice", 10),
            kwargs={},
            queue="billing",
            priority=9,
            max_retries=3,
            scheduled_at=scheduled_at,
            expires_at=expires_at,
            key="bill-key-1",
            execution_backend="local",
            execution_profile="high-mem",
            metadata={"tenant": "acme", "step": 1},
        ),
        TaskRequest(
            task_name="vertebra_test_store_worker_record",
            args=("welcome", 5),
            kwargs={},
            queue="emails",
            priority=4,
            max_retries=1,
            expires_at=expires_at,
            key="email-key-1",
            execution_backend="local",
            execution_profile="fast",
            metadata={"tenant": "beta", "step": 2},
        ),
    ]
    records = await backend.enqueue_many(requests)
    assert store.enqueue_many_calls == 1
    assert len(records) == 2
    assert records[0].id != records[1].id

    for req, rec in zip(requests, records, strict=True):
        assert rec.task_name == req.task_name
        assert rec.queue == req.queue
        assert rec.priority == req.priority
        assert rec.max_retries == req.max_retries
        assert rec.expires_at == req.expires_at
        assert rec.execution_backend == req.execution_backend
        assert rec.execution_profile == req.execution_profile
        assert rec.metadata == req.metadata
        assert rec.key == req.key

        loaded = await backend.get_task_by_key(req.key or "")
        assert loaded is not None
        assert loaded.id == rec.id
        assert loaded.queue == req.queue
        assert loaded.priority == req.priority
        assert loaded.expires_at == req.expires_at
        assert loaded.execution_backend == req.execution_backend
        assert loaded.execution_profile == req.execution_profile
        assert loaded.metadata == req.metadata


@pytest.mark.anyio
async def test_store_queue_backend_real_worker_execution_across_queues() -> None:
    """Drive real litestar_queues.Worker execution over StoreQueueBackend across non-default queues."""
    store = InMemoryAsyncQueueStore()
    backend = StoreQueueBackend(store=store)
    service = QueueService(
        QueueConfig(worker=WorkerConfig(batch_size=10, max_concurrency=4)),
        queue_backend=backend,
    )
    worker = Worker(service)

    enqueued = await backend.enqueue_many(
        [
            TaskRequest(
                task_name="vertebra_test_store_worker_record",
                args=("billing-1", 3),
                queue="billing",
                priority=10,
                key="worker-bill-1",
                metadata={"channel": "invoice"},
            ),
            TaskRequest(
                task_name="vertebra_test_store_worker_record",
                args=("email-1", 4),
                queue="emails",
                priority=5,
                key="worker-email-1",
                metadata={"channel": "smtp"},
            ),
        ]
    )
    assert len(await backend.list_pending(limit=10)) == 2
    assert len(await backend.list_pending(limit=10, queue="billing")) == 1

    processed = await worker.run_once()
    await worker.stop()
    assert processed == 2

    completed = await backend.list_completed_by_task("vertebra_test_store_worker_record", limit=10)
    assert {rec.id for rec in completed} == {rec.id for rec in enqueued}
    assert {rec.queue for rec in completed} == {"billing", "emails"}

    billing_rec = await backend.get_task_by_key("worker-bill-1")
    email_rec = await backend.get_task_by_key("worker-email-1")
    assert billing_rec is not None and billing_rec.status == "completed"
    assert email_rec is not None and email_rec.status == "completed"

    running_only = await backend.enqueue(
        "vertebra_test_store_worker_record",
        args=("running-1", 1),
        queue="billing",
        key="running-bill-key",
    )
    claimed = await backend.claim_task(running_only.id)
    assert claimed is not None
    running_external = await backend.list_running_external()
    assert [rec.id for rec in running_external] == [running_only.id]
    assert running_external[0].queue == "billing"


@pytest.mark.anyio
async def test_store_queue_backend_worker_start_loop_and_cleanup() -> None:
    """Run Worker.start() loop against StoreQueueBackend with maintenance and forever reservations."""
    store = InMemoryAsyncQueueStore()
    backend = StoreQueueBackend(store=store)
    service = QueueService(
        QueueConfig(worker=WorkerConfig(batch_size=10, max_concurrency=4)),
        queue_backend=backend,
    )
    worker = Worker(service)

    forever_id = uuid4()
    assert await backend.reserve_identity("forever-invoice", task_id=forever_id, task_name="bill") is None
    contender = await backend.reserve_identity("forever-invoice", task_id=uuid4(), task_name="bill")
    assert isinstance(contender, TaskReservation)
    assert contender.task_id == forever_id

    existing_res = await backend.has_identity("forever-invoice")
    assert isinstance(existing_res, TaskReservation)
    assert existing_res.task_id == forever_id
    assert existing_res.task_name == "bill"
    assert await backend.has_identity("missing-reservation") is None

    await backend.enqueue_many(
        [
            TaskRequest(
                task_name="vertebra_test_store_worker_record",
                args=("loop-a", 7),
                queue="billing",
            ),
            TaskRequest(
                task_name="vertebra_test_store_worker_record",
                args=("loop-b", 8),
                queue="emails",
            ),
        ]
    )

    worker_task = asyncio.create_task(worker.start())
    try:
        completed: list[Any] = []
        for _ in range(50):
            completed = await backend.list_completed_by_task("vertebra_test_store_worker_record", limit=10)
            if len(completed) == 2:
                break
            await asyncio.sleep(0.02)
        assert len(completed) == 2
    finally:
        await worker.stop()
        await worker_task

    deleted = await backend.cleanup_terminal(datetime.now(UTC) + timedelta(seconds=5), limit=10)
    assert deleted == 2
    assert await backend.list_completed_by_task("vertebra_test_store_worker_record", limit=10) == []

    retained_res = await backend.has_identity("forever-invoice")
    assert isinstance(retained_res, TaskReservation)
    assert retained_res.task_id == forever_id

    assert await backend.reset_identity("forever-invoice", expected_task_id=uuid4()) is False
    assert await backend.reset_identity("forever-invoice", expected_task_id=forever_id) is True
    assert await backend.has_identity("forever-invoice") is None


@pytest.mark.parametrize(
    ("wrong_worker", "wrong_retry"),
    [
        ("other-worker", 0),
        ("worker-alpha", 1),
    ],
)
@pytest.mark.anyio
async def test_store_queue_backend_worker_control_fencing_and_expiry(
    wrong_worker: str,
    wrong_retry: int,
) -> None:
    """Verify assign_worker, interrupt_task, null_heartbeats, cancel_tasks, and expire_overdue."""
    store = InMemoryAsyncQueueStore()
    backend = StoreQueueBackend(store=store)

    rec = await backend.enqueue(
        "vertebra_test_store_worker_record",
        args=("ctrl", 1),
        queue="billing",
        metadata={"env": "staging"},
    )
    claimed = await backend.claim_task(rec.id)
    assert claimed is not None

    assert await backend.assign_worker(rec.id, worker_id="worker-alpha", expected_retry_count=99) is None
    assigned = await backend.assign_worker(rec.id, worker_id="worker-alpha", expected_retry_count=0)
    assert assigned is not None
    assert assigned.worker_id == "worker-alpha"

    await backend.touch_heartbeats([HeartbeatTouch(task_id=rec.id, expected_retry_count=0)])
    hb_entry = await store.get_entry(str(rec.id))
    assert hb_entry is not None and hb_entry.heartbeat_at_ms is not None

    await backend.null_heartbeats([rec.id], expected_retry_count=99)
    still_hb = await store.get_entry(str(rec.id))
    assert still_hb is not None and still_hb.heartbeat_at_ms is not None

    await backend.null_heartbeats([rec.id], expected_retry_count=0)
    cleared_hb = await store.get_entry(str(rec.id))
    assert cleared_hb is not None and cleared_hb.heartbeat_at_ms is None

    assert (
        await backend.interrupt_task(
            rec.id,
            expected_retry_count=wrong_retry,
            worker_id=wrong_worker,
            queued_at=datetime.now(UTC),
        )
        is None
    )
    interrupted = await backend.interrupt_task(
        rec.id,
        expected_retry_count=0,
        worker_id="worker-alpha",
        queued_at=datetime.now(UTC),
    )
    assert interrupted is not None
    assert interrupted.status == "pending"

    overdue = await backend.enqueue(
        "vertebra_test_store_worker_record",
        args=("late", 2),
        queue="emails",
        expires_at=datetime.now(UTC) - timedelta(seconds=5),
    )
    expired = await backend.expire_overdue(limit=10)
    assert [item.id for item in expired] == [overdue.id]
    assert expired[0].status == "expired"

    to_cancel = await backend.enqueue(
        "vertebra_test_store_worker_record",
        args=("cancel-me", 3),
        queue="emails",
        metadata={"tag": "drop"},
    )
    assert await backend.cancel_tasks(metadata={"tag": "keep"}) == 0
    assert await backend.cancel_tasks(metadata={"tag": "drop"}) == 1
    cancelled = await backend.get_task(to_cancel.id)
    assert cancelled is not None and cancelled.status == "cancelled"


def test_queues_config_has_no_function_scoped_imports() -> None:
    """Verify src/py/vertebra/extensions/litestar/queues/config.py has zero function-scoped imports."""
    config_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "vertebra"
        / "extensions"
        / "litestar"
        / "queues"
        / "config.py"
    )
    tree = ast.parse(config_path.read_text(encoding="utf-8"), filename=str(config_path))
    nested_imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in ast.walk(node):
                if isinstance(child, (ast.Import, ast.ImportFrom)):
                    nested_imports.append(ast.dump(child))
    assert nested_imports == []
