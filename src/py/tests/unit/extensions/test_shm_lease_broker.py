"""Tests for cross-process shared-memory connection broker."""

from __future__ import annotations

import inspect
import multiprocessing as mp
import os
import signal
import time
import warnings
from pathlib import Path
from typing import Any

import pytest

from vertebra._vertebra import LeaseBroker, StatementCache
from vertebra.db.driver import InterfaceError
from vertebra.extensions import alchemy as alchemy_pkg
from vertebra.extensions.alchemy import broker as alchemy_broker
from vertebra.extensions.alchemy.broker import AsyncSHMSessionBroker, SyncSHMSessionBroker
from vertebra.extensions.broker import (
    AsyncSHMBrokerTransport,
    BrokerAttachError,
    BrokerConfig,
    BrokerSegmentInUseError,
    LeaseQuarantinedError,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.broker.transport import AsyncBrokerLeaseSource, SyncBrokerLeaseSource
from vertebra.utils.module_loader import import_optional


def _worker_contention_loop(name: str, iterations: int, start_barrier: mp.synchronize.Barrier) -> None:
    """Worker process contending for connection leases from a shared named broker.

    Args:
        name: Name of the POSIX shared-memory segment.
        iterations: Number of acquire-release cycles to perform.
        start_barrier: Barrier synchronizing worker start for contention.
    """
    broker = LeaseBroker.open_named(name, pool_size=3, max_overflow=1)
    pid = os.getpid()
    start_barrier.wait()
    for _ in range(iterations):
        grant = broker.acquire(worker_pid=pid, ttl_ns=0, timeout_ms=2000, bind_key_hash=0)
        if grant is not None:
            broker.release(grant.slot_index, grant.generation)


def _worker_hold_and_pause(
    name: str,
    ready_event: mp.synchronize.Event,
    stop_event: mp.synchronize.Event,
) -> None:
    """Worker process acquiring leases and pausing until killed.

    Args:
        name: Name of the POSIX shared-memory segment.
        ready_event: Event to notify parent when leases are held.
        stop_event: Event waited on until parent terminates the worker.
    """
    broker = LeaseBroker.open_named(name, pool_size=4, max_overflow=2)
    pid = os.getpid()
    g1 = broker.acquire(worker_pid=pid, ttl_ns=0, timeout_ms=2000, bind_key_hash=0)
    g2 = broker.acquire(worker_pid=pid, ttl_ns=0, timeout_ms=2000, bind_key_hash=0)
    if g1 is not None and g2 is not None:
        ready_event.set()
        stop_event.wait()


def test_native_lease_broker_named_cross_process_cap() -> None:
    """Verify cross-process lease contention adheres to pool_size and max_overflow."""
    name = f"/vtb-test-py-contention-{os.getpid()}-{int(time.time())}"
    broker = LeaseBroker.create_named(name, pool_size=3, max_overflow=1)
    ctx = mp.get_context("spawn")
    start_barrier = ctx.Barrier(4)

    workers = [ctx.Process(target=_worker_contention_loop, args=(name, 40, start_barrier)) for _ in range(4)]
    for w in workers:
        w.start()
    for w in workers:
        w.join(timeout=30)
        assert w.exitcode == 0

    stats = broker.stats()
    assert stats.total_acquired >= 160
    assert stats.active_leases == 0


def test_native_lease_broker_sigkill_worker_reclamation() -> None:
    """Verify child worker crash reclamation recovers slots and resets active lease gauge."""
    name = f"/vtb-test-py-kill-{os.getpid()}-{int(time.time())}"
    broker = LeaseBroker.create_named(name, pool_size=4, max_overflow=2)
    ctx = mp.get_context("spawn")

    ready_event = ctx.Event()
    stop_event = ctx.Event()
    proc = ctx.Process(target=_worker_hold_and_pause, args=(name, ready_event, stop_event))
    proc.start()

    assert ready_event.wait(timeout=10)

    stats = broker.stats()
    assert stats.active_leases == 2

    assert proc.pid is not None
    os.kill(proc.pid, signal.SIGKILL)
    proc.join(timeout=10)

    reclaimed = broker.reclaim_worker(proc.pid)
    assert reclaimed == 2
    assert broker.stats().active_leases == 0


def test_native_lease_broker_from_env_fd_inheritance() -> None:
    """Verify from_env handles unset environment variables and from_fd handles invalid FDs."""
    os.environ.pop("VERTEBRA_BROKER_SHM_FD", None)
    os.environ.pop("VERTEBRA_BROKER_SHM_SIZE", None)
    assert LeaseBroker.from_env() is None

    broker = LeaseBroker(pool_size=4, max_overflow=2)
    assert broker.pool_size == 4
    assert broker.max_overflow == 2
    assert broker.total_capacity == 6


def test_open_or_create_named_broker_and_statement_cache() -> None:
    """Verify open_or_create_named reports created status, owner_pid, and clean unlink."""
    broker_name = f"/vtb-py-ooc-brk-{os.getpid()}-{int(time.time() * 1000)}"
    cache_name = f"/vtb-py-ooc-sc-{os.getpid()}-{int(time.time() * 1000)}"

    broker1, created1 = LeaseBroker.open_or_create_named(broker_name, pool_size=3, max_overflow=1, stmt_capacity=16)
    assert created1 is True
    assert broker1.owner_pid == os.getpid()

    broker2, created2 = LeaseBroker.open_or_create_named(broker_name, pool_size=3, max_overflow=1, stmt_capacity=16)
    assert created2 is False
    assert broker2.owner_pid == os.getpid()
    assert broker1.unlink() is True
    assert not Path(f"/dev/shm/{broker_name.lstrip('/')}").exists()

    sc1, sc_created1 = StatementCache.open_or_create_named(cache_name, capacity=16)
    assert sc_created1 is True
    assert sc1.owner_pid == os.getpid()
    sc2, sc_created2 = StatementCache.open_or_create_named(cache_name, capacity=16)
    assert sc_created2 is False
    assert sc2.owner_pid == os.getpid()
    assert sc1.unlink() is True


@pytest.mark.parametrize("case", ["enoent", "capacity_mismatch", "worker_missing_name"])
def test_worker_role_attach_failures_raise_without_fallback(case: str) -> None:
    """Verify worker role never falls back to create_named and raises typed attach errors."""
    if case == "worker_missing_name":
        with pytest.raises(BrokerAttachError):
            SyncSHMBrokerTransport(BrokerConfig(role="worker"))
        return

    name = f"/vtb-worker-err-{case}-{os.getpid()}-{int(time.time() * 1000)}"
    shm_path = Path(f"/dev/shm/{name.lstrip('/')}")

    if case == "enoent":
        with pytest.raises(BrokerAttachError):
            SyncSHMBrokerTransport(BrokerConfig(shm_name=name, role="worker"))
        assert not shm_path.exists()
        return

    owner = SyncSHMBrokerTransport(
        BrokerConfig(pool_size=2, max_overflow=0, statement_cache_capacity=16, shm_name=name, role="owner")
    )
    try:
        with pytest.raises(BrokerSegmentInUseError) as exc_info:
            SyncSHMBrokerTransport(
                BrokerConfig(pool_size=6, max_overflow=2, statement_cache_capacity=16, shm_name=name, role="worker")
            )
        assert exc_info.value.segment_name == name
        assert exc_info.value.owner_pid == os.getpid()
    finally:
        owner.unlink()


def test_owner_and_worker_roles_share_pool_capacity() -> None:
    """Verify owner and worker role transports share a single underlying LeaseBroker capacity."""
    name = f"/vtb-role-share-{os.getpid()}-{int(time.time() * 1000)}"
    owner = SyncSHMBrokerTransport(
        BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50, shm_name=name, role="owner")
    )
    worker = SyncSHMBrokerTransport(
        BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50, shm_name=name, role="worker")
    )
    try:
        assert owner.created is True
        assert worker.created is False
        assert worker.owner_pid == os.getpid()
        with owner.acquire(worker_id=1), worker.acquire(worker_id=2), pytest.raises(TimeoutError):
            worker.acquire(worker_id=3, timeout_ms=20)
        assert owner.metrics().active_leases == 0
    finally:
        owner.unlink()


def test_size_one_quarantine_blocks_until_disposal_and_release_raises() -> None:
    """Verify quarantining a slot blocks reuse until disposed and rejecting direct release."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    part_id = broker.register_partition(0xABCDEF01, 1, 0)
    grant = broker.acquire_in(part_id, worker_pid=51001, ttl_ns=0, timeout_ms=50, bind_key_hash=0)
    assert grant is not None

    broker.quarantine(grant.slot_index, grant.generation)
    assert broker.quarantined_count() == 1
    stats = broker.stats()
    assert stats.quarantined == 1
    assert stats.total_quarantined == 1
    assert broker.try_acquire_in(part_id, worker_pid=51002, ttl_ns=0, bind_key_hash=0) is None

    with pytest.raises(LeaseQuarantinedError):
        broker.release(grant.slot_index, grant.generation)

    broker.dispose_quarantined(grant.slot_index, grant.generation)
    assert broker.quarantined_count() == 0
    assert broker.stats().total_disposed == 1

    reused = broker.try_acquire_in(part_id, worker_pid=51002, ttl_ns=0, bind_key_hash=0)
    assert reused is not None
    assert broker.release(reused.slot_index, reused.generation) is True


def test_reclaim_worker_leases_isolates_dead_pid_without_disturbing_live_worker() -> None:
    """Verify reclaim_worker_leases quarantines only dead_pid slots and bumps generations."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    dead_grant = broker.try_acquire(worker_pid=62001, ttl_ns=0, bind_key_hash=0)
    live_grant = broker.try_acquire(worker_pid=62002, ttl_ns=0, bind_key_hash=0)
    assert dead_grant is not None and live_grant is not None

    reclaimed = broker.reclaim_worker_leases(62001)
    assert len(reclaimed) == 1
    assert reclaimed[0].slot_index == dead_grant.slot_index
    assert reclaimed[0].generation == dead_grant.generation + 1
    assert broker.quarantined_count() == 1
    assert broker.try_acquire(worker_pid=62003, ttl_ns=0, bind_key_hash=0) is None

    broker.dispose_quarantined(reclaimed[0].slot_index, reclaimed[0].generation)
    assert broker.quarantined_count() == 0
    assert broker.release(live_grant.slot_index, live_grant.generation) is True
    assert broker.stats().active_leases == 0


def test_alchemy_broker_uses_shared_lease_source_protocols() -> None:
    """Verify duplicate transport protocols are removed and shared lease source protocols are imported."""
    assert not hasattr(alchemy_broker, "SyncBrokerTransport")
    assert not hasattr(alchemy_broker, "AsyncBrokerTransport")
    assert not hasattr(alchemy_pkg, "SyncBrokerTransport")
    assert not hasattr(alchemy_pkg, "AsyncBrokerTransport")
    assert alchemy_broker.SyncBrokerLeaseSource is SyncBrokerLeaseSource
    assert alchemy_broker.AsyncBrokerLeaseSource is AsyncBrokerLeaseSource


class _MinimalSyncLease:
    def __init__(self, key: str) -> None:
        self.lease_id = key
        self.released = False

    def release(self) -> None:
        self.released = True


class _MinimalSyncLeaseSource:
    def __init__(self) -> None:
        self.leases: list[_MinimalSyncLease] = []
        self.quarantined: list[Any] = []

    def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> _MinimalSyncLease:
        _ = (bind_key, worker_id)
        lease = _MinimalSyncLease(f"min-sync-{len(self.leases)}")
        self.leases.append(lease)
        return lease

    def quarantine(self, lease: Any) -> None:
        self.quarantined.append(lease)


class _MinimalAsyncLease:
    def __init__(self, key: str) -> None:
        self.lease_id = key
        self.released = False

    async def release(self) -> None:
        self.released = True


class _MinimalAsyncLeaseSource:
    def __init__(self) -> None:
        self.leases: list[_MinimalAsyncLease] = []
        self.quarantined: list[Any] = []

    async def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> _MinimalAsyncLease:
        _ = (bind_key, worker_id)
        lease = _MinimalAsyncLease(f"min-async-{len(self.leases)}")
        self.leases.append(lease)
        return lease

    async def quarantine(self, lease: Any) -> None:
        self.quarantined.append(lease)


@pytest.mark.parametrize("use_minimal_source", [False, True])
def test_sync_session_broker_transport_delegation_and_non_underflow(use_minimal_source: bool) -> None:
    """Verify SyncSHMSessionBroker delegates acquire/release/in_use without underflowing or requiring transport.release."""
    if use_minimal_source:
        source = _MinimalSyncLeaseSource()
        assert isinstance(source, SyncBrokerLeaseSource)
        broker = SyncSHMSessionBroker(transport=source, capacity=2)
        assert broker.in_use == 0
        assert broker.acquire() is True
        assert broker.in_use == 1
        broker.release()
        assert source.leases[0].released is True
        assert broker.in_use == 0
        broker.release()
        assert broker.in_use == 0

        lease = broker.acquire_lease()
        assert broker.in_use == 1
        assert broker.release_lease(lease) is True
        assert lease.released is True
        assert broker.in_use == 0
        return

    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    broker = SyncSHMSessionBroker(transport=transport, capacity=1)
    assert broker.in_use == 0
    assert broker.acquire() is True
    assert transport.metrics().active_leases == 1
    assert broker.in_use == 1
    assert broker.acquire(timeout=0.01) is False
    assert broker.in_use == 1
    broker.release()
    assert transport.metrics().active_leases == 0
    assert broker.in_use == 0
    broker.release()
    assert transport.metrics().active_leases == 0
    assert broker.in_use == 0


@pytest.mark.anyio
@pytest.mark.parametrize("use_minimal_source", [False, True])
async def test_async_session_broker_transport_delegation_and_sync_hook_error(use_minimal_source: bool) -> None:
    """Verify AsyncSHMSessionBroker delegates async acquire/release/in_use and rejects sync pool hooks."""
    if use_minimal_source:
        source = _MinimalAsyncLeaseSource()
        assert isinstance(source, AsyncBrokerLeaseSource)
        broker = AsyncSHMSessionBroker(transport=source, capacity=2)
        assert broker.in_use == 0
        assert await broker.acquire() is True
        assert broker.in_use == 1
        await broker.release()
        assert source.leases[0].released is True
        assert broker.in_use == 0
        await broker.release()
        assert broker.in_use == 0

        lease = await broker.acquire_lease()
        assert broker.in_use == 1
        assert await broker.release_lease(lease) is True
        assert lease.released is True
        assert broker.in_use == 0
    else:
        transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
        broker = AsyncSHMSessionBroker(transport=transport, capacity=1)
        assert broker.in_use == 0
        assert await broker.acquire() is True
        assert transport.metrics().active_leases == 1
        assert broker.in_use == 1
        assert await broker.acquire(timeout=0.01) is False
        assert broker.in_use == 1
        await broker.release()
        assert transport.metrics().active_leases == 0
        assert broker.in_use == 0
        await broker.release()
        assert transport.metrics().active_leases == 0
        assert broker.in_use == 0

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        with pytest.raises(InterfaceError):
            broker.on_checkout(object())
        with pytest.raises(InterfaceError):
            broker.on_checkin(object())

    runtime_warnings = [w for w in caught if issubclass(w.category, RuntimeWarning)]
    assert runtime_warnings == []


def test_sync_session_broker_single_index_checkout_and_checkin() -> None:
    """Verify on_checkout indexes each lease once in _active_leases and on_checkin clears both maps."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    broker = SyncSHMSessionBroker(transport=transport, capacity=2)
    conn = object()

    broker.on_checkout(conn)
    assert len(broker._active_leases) == 1
    assert len(broker._conn_leases) == 1
    assert id(conn) in broker._conn_leases
    assert transport.metrics().active_leases == 1

    broker.on_checkin(conn)
    assert broker._active_leases == {}
    assert broker._conn_leases == {}
    assert transport.metrics().active_leases == 0
    assert transport.metrics().total_released == 1


@pytest.mark.anyio
async def test_session_broker_attach_and_acquire_avoid_placeholder_compile_and_signature_inspection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify attach_to_engine never compiles placeholder SQL and acquire_lease avoids inspect.signature."""
    sync_transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    compile_calls = 0

    def _fail_if_called(*_args: Any, **_kwargs: Any) -> Any:
        nonlocal compile_calls
        compile_calls += 1
        raise AssertionError("get_or_compile_statement must not be invoked by attach_to_engine")

    monkeypatch.setattr(sync_transport, "get_or_compile_statement", _fail_if_called)

    sync_broker = SyncSHMSessionBroker(transport=sync_transport, capacity=2)
    assert not hasattr(sync_broker, "on_before_cursor_execute")

    sa_mod = import_optional("sqlalchemy")
    if sa_mod is not None:
        engine = sa_mod.create_engine("sqlite:///:memory:")
        try:
            sync_broker.attach_to_engine(engine)
            with engine.connect() as conn:
                assert conn.execute(sa_mod.text("SELECT 1")).scalar() == 1
        finally:
            engine.dispose()
    assert compile_calls == 0

    def _forbid_signature(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("inspect.signature must not be called during lease acquisition")

    monkeypatch.setattr(inspect, "signature", _forbid_signature)

    sync_lease = sync_broker.acquire_lease()
    assert sync_broker.release_lease(sync_lease) is True

    async_broker = AsyncSHMSessionBroker(
        transport=AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0)),
        capacity=2,
    )
    async_lease = await async_broker.acquire_lease()
    assert await async_broker.release_lease(async_lease) is True


@pytest.mark.anyio
async def test_broker_lease_plan_releases_async_connection_lease_via_release_sync() -> None:
    """Verify _BrokerLeasePlan and _release_lease release AsyncConnectionLease via release_sync()."""
    from vertebra.db.driver._common import _BrokerLeasePlan, _release_lease

    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    plan = _BrokerLeasePlan(transport)
    lease = await transport.acquire(worker_id=1)
    assert transport.metrics().active_leases == 1
    assert plan.lease_release_method(lease) == "release_sync"
    _release_lease(transport, plan, lease)
    assert transport.metrics().active_leases == 0


@pytest.mark.anyio
async def test_async_broker_release_bypasses_sync_pool_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify AsyncSHMBrokerTransport.release executes inline without queuing behind SyncPool waiters."""
    from vertebra.extensions.broker import transport as transport_mod

    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    lease = await transport.acquire(worker_id=1)
    assert transport.metrics().active_leases == 1

    class _ForbidSyncPool:
        @classmethod
        def default(cls) -> _ForbidSyncPool:
            raise AssertionError("release() must not queue behind SyncPool.default()")

    monkeypatch.setattr(transport_mod, "SyncPool", _ForbidSyncPool)
    assert await transport.release(lease.lease_id) is True
    assert transport.metrics().active_leases == 0
