"""Tests for cross-process shared-memory connection broker."""

from __future__ import annotations

import asyncio
import contextlib
import gc
import inspect
import multiprocessing as mp
import os
import signal
import threading
import time
import warnings
import weakref
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

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
from vertebra.extensions.broker import transport as transport_mod
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


def _worker_hold_and_release_named(
    name: str,
    ready_event: mp.synchronize.Event,
    release_event: mp.synchronize.Event,
) -> None:
    """Acquire the sole lease in a named SHM broker and release it once signaled."""
    broker = LeaseBroker.open_named(name, pool_size=1, max_overflow=0)
    grant = broker.acquire(worker_pid=os.getpid(), ttl_ns=0, timeout_ms=2000, bind_key_hash=0)
    assert grant is not None
    ready_event.set()
    release_event.wait(timeout=10.0)
    assert broker.release(grant.slot_index, grant.generation) is True


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
    """Verify _BrokerLeasePlan, _release_lease, and _release_pid_lease release AsyncConnectionLease via release_sync()."""
    from vertebra.db.driver._common import CommonDriverMixin, _BrokerLeasePlan, _release_lease

    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    plan = _BrokerLeasePlan(transport)
    lease = await transport.acquire(worker_id=1)
    assert transport.metrics().active_leases == 1
    assert plan.lease_release_method(lease) == "release_sync"
    _release_lease(transport, plan, lease)
    assert transport.metrics().active_leases == 0

    class _DummyDriver(CommonDriverMixin[Any]):
        def __init__(self, broker: Any) -> None:
            self._broker = broker
            self._cached_lease_plan = None

    dummy = _DummyDriver(transport)
    lease_two = await transport.acquire(worker_id=2)
    assert transport.metrics().active_leases == 1
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        dummy._release_pid_lease(lease_two)
    assert transport.metrics().active_leases == 0
    assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []


@pytest.mark.anyio
async def test_async_broker_release_bypasses_sync_pool_waiters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify AsyncSHMBrokerTransport.release executes inline without queuing behind SyncPool waiters."""
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


@pytest.mark.anyio
async def test_async_broker_c128_contention_bypasses_sync_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify c=128 concurrent async acquires over 10 slots complete without SyncPool submissions."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=10, max_overflow=0, acquire_timeout_ms=5000))

    class _ForbidSyncPool:
        @classmethod
        def default(cls) -> _ForbidSyncPool:
            raise AssertionError("acquire() must not submit contended waits to SyncPool.default()")

    monkeypatch.setattr(transport_mod, "SyncPool", _ForbidSyncPool)

    completed = 0

    async def _worker(idx: int) -> None:
        nonlocal completed
        lease = await transport.acquire(worker_id=idx + 1, timeout_ms=5000)
        try:
            await asyncio.sleep(0.001)
            completed += 1
        finally:
            assert await transport.release(lease.lease_id) is True

    await asyncio.gather(*(_worker(i) for i in range(128)))
    assert completed == 128
    assert transport.metrics().active_leases == 0
    assert transport.metrics().total_acquired == 128
    assert transport.metrics().total_released == 128


@pytest.mark.anyio
async def test_async_broker_leader_cancel_promotes_follower() -> None:
    """Verify cancelling the oldest waiter still wakes remaining followers on remote/native release."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))
    held = await transport.acquire(worker_id=1)

    first_waiter = asyncio.create_task(transport.acquire(worker_id=2, timeout_ms=2000))
    await asyncio.sleep(0.005)

    completed: list[int] = []

    async def _follower(idx: int) -> None:
        lease = await transport.acquire(worker_id=3 + idx, timeout_ms=2000)
        try:
            completed.append(idx)
        finally:
            assert transport._native.release(lease.slot_index, lease.generation) is True

    followers = [asyncio.create_task(_follower(idx)) for idx in range(10)]
    await asyncio.sleep(0.005)

    first_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_waiter

    assert transport._native.release(held.slot_index, held.generation) is True

    await asyncio.wait_for(asyncio.gather(*followers), timeout=2.0)
    assert sorted(completed) == list(range(10))
    assert transport.metrics().active_leases == 0


@pytest.mark.anyio
async def test_async_broker_multi_slot_remote_release_wakes_all_waiters() -> None:
    """Verify remote multi-slot releases wake queued waiters without losing the cross-process watcher."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=3, max_overflow=0, acquire_timeout_ms=2000))
    held = [await transport.acquire(worker_id=idx + 1) for idx in range(3)]

    w1 = asyncio.create_task(transport.acquire(worker_id=11, timeout_ms=2000))
    w2 = asyncio.create_task(transport.acquire(worker_id=12, timeout_ms=2000))
    w3 = asyncio.create_task(transport.acquire(worker_id=13, timeout_ms=2000))
    await asyncio.sleep(0.005)

    assert transport._native.release(held[0].slot_index, held[0].generation) is True
    assert transport._native.release(held[1].slot_index, held[1].generation) is True
    l1, l2 = await asyncio.wait_for(asyncio.gather(w1, w2), timeout=1.0)
    assert not w3.done()

    started = time.monotonic()
    assert transport._native.release(held[2].slot_index, held[2].generation) is True
    l3 = await asyncio.wait_for(w3, timeout=1.0)
    assert (time.monotonic() - started) * 1000.0 < 200.0

    for lease in (l1, l2, l3):
        assert await transport.release(lease.lease_id) is True
    assert transport.metrics().active_leases == 0


@pytest.mark.anyio
async def test_async_broker_sustained_local_contention_fifo_no_starvation() -> None:
    """Verify oldest queued waiter acquires in FIFO order without starvation under continuous local churn."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))
    held = await transport.acquire(worker_id=1)

    order: list[str] = []

    async def _named_waiter(label: str) -> None:
        lease = await transport.acquire(worker_id=10, timeout_ms=2000)
        try:
            order.append(label)
            await asyncio.sleep(0)
        finally:
            assert await transport.release(lease.lease_id) is True

    oldest = asyncio.create_task(_named_waiter("oldest"))
    await asyncio.sleep(0.005)
    followers = [asyncio.create_task(_named_waiter(f"follower-{idx}")) for idx in range(16)]
    await asyncio.sleep(0.005)

    assert await transport.release(held.lease_id) is True
    await asyncio.wait_for(asyncio.gather(oldest, *followers), timeout=2.0)
    assert order[0] == "oldest"
    assert len(order) == 17
    assert transport.metrics().active_leases == 0


@pytest.mark.anyio
async def test_async_broker_cross_thread_release_sync_wakes_other_loop() -> None:
    """Verify release_sync() from another OS thread wakes a waiting coroutine via call_soon_threadsafe."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))
    held = await transport.acquire(worker_id=1)

    waiter = asyncio.create_task(transport.acquire(worker_id=2, timeout_ms=2000))
    await asyncio.sleep(0.005)
    assert not waiter.done()

    started = time.monotonic()

    def _release_from_thread() -> None:
        held.release_sync()

    releaser = threading.Thread(target=_release_from_thread)
    releaser.start()
    lease = await asyncio.wait_for(waiter, timeout=1.0)
    releaser.join(timeout=2.0)
    assert (time.monotonic() - started) * 1000.0 < 200.0
    assert await transport.release(lease.lease_id) is True
    assert transport.metrics().active_leases == 0


def test_async_broker_weakref_evicts_closed_event_loop() -> None:
    """Verify closed event loops that contended on AsyncSHMBrokerTransport are garbage-collected."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))

    async def _contend_once() -> None:
        held = await transport.acquire(worker_id=1)
        try:
            with pytest.raises(TimeoutError):
                await transport.acquire(worker_id=2, timeout_ms=15)
        finally:
            assert await transport.release(held.lease_id) is True

    loop = asyncio.new_event_loop()
    loop_ref = weakref.ref(loop)
    try:
        loop.run_until_complete(_contend_once())
        loop.run_until_complete(asyncio.sleep(0))
    finally:
        loop.close()
    del loop
    gc.collect()
    assert loop_ref() is None


@pytest.mark.anyio
async def test_async_broker_mock_native_fallback_uses_sync_pool() -> None:
    """Verify AsyncSHMBrokerTransport falls back to SyncPool when _native lacks wait_for_wake_async."""
    grant = SimpleNamespace(
        slot_index=0,
        generation=1,
        worker_pid=1,
        is_overflow=False,
        acquired_ns=10,
        expiry_ns=0,
        bind_key_hash=0,
    )
    calls: list[str] = []

    class _MockNative:
        def statement_cache(self) -> None:
            return None

        def lookup_partition(self, _key_hash: int) -> None:
            return None

        def try_acquire(self, **_kwargs: Any) -> None:
            calls.append("try_acquire")
            return None

        def acquire(self, *_args: Any, **_kwargs: Any) -> Any:
            calls.append("acquire")
            return grant

        def release(self, _slot: int, _gen: int) -> bool:
            calls.append("release")
            return True

    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0), native=cast("Any", _MockNative()))
    lease = await transport.acquire(worker_id=1, timeout_ms=100)
    assert lease.slot_index == 0
    assert await transport.release(lease.lease_id) is True
    assert calls == ["try_acquire", "acquire", "release"]


@pytest.mark.anyio
async def test_async_broker_release_sync_wakes_local_waiter() -> None:
    """Verify lease.release_sync() wakes a queued async waiter directly on the same loop."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))
    held = await transport.acquire(worker_id=1)

    waiter_one = asyncio.create_task(transport.acquire(worker_id=2, timeout_ms=2000))
    waiter_two = asyncio.create_task(transport.acquire(worker_id=3, timeout_ms=2000))
    await asyncio.sleep(0.005)

    started = time.monotonic()
    held.release_sync()

    done, pending = await asyncio.wait({waiter_one, waiter_two}, return_when=asyncio.FIRST_COMPLETED, timeout=1.0)
    elapsed_ms = (time.monotonic() - started) * 1000.0
    assert len(done) == 1
    assert elapsed_ms < 200.0

    first_lease = next(iter(done)).result()
    first_lease.release_sync()
    second_lease = await asyncio.wait_for(next(iter(pending)), timeout=1.0)
    second_lease.release_sync()
    assert transport.metrics().active_leases == 0


@pytest.mark.anyio
async def test_async_broker_cross_process_wake_via_named_shm() -> None:
    """Verify a remote OS process releasing a named SHM slot wakes AsyncSHMBrokerTransport promptly."""
    name = f"/vtb-test-async-wake-{os.getpid()}-{time.monotonic_ns()}"
    transport = AsyncSHMBrokerTransport(
        BrokerConfig(pool_size=1, max_overflow=0, shm_name=name, role="owner", acquire_timeout_ms=2000)
    )
    ctx = mp.get_context("spawn")
    ready_event = ctx.Event()
    release_event = ctx.Event()
    proc = ctx.Process(target=_worker_hold_and_release_named, args=(name, ready_event, release_event))
    try:
        proc.start()
        assert ready_event.wait(timeout=10.0)
        waiter = asyncio.create_task(transport.acquire(worker_id=os.getpid(), timeout_ms=3000))
        await asyncio.sleep(0.01)
        assert not waiter.done()

        started = time.monotonic()
        release_event.set()
        lease = await asyncio.wait_for(waiter, timeout=2.0)
        elapsed_ms = (time.monotonic() - started) * 1000.0
        assert elapsed_ms < 500.0
        assert await transport.release(lease.lease_id) is True
        proc.join(timeout=10.0)
        assert proc.exitcode == 0
        assert transport.metrics().active_leases == 0
    finally:
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5.0)
        with contextlib.suppress(Exception):
            transport.unlink()


@pytest.mark.anyio
async def test_async_broker_multi_partition_waiters_no_starvation() -> None:
    """Verify releasing partition A wakes a waiter blocked on global capacity in partition B."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))
    part_a = transport.register_partition("pool-a", pool_size=1, max_overflow=0)
    part_b = transport.register_partition("pool-b", pool_size=1, max_overflow=0)

    lease_a = await transport.acquire(worker_id=1, bind_key="pool-a", partition=part_a)
    waiter_b = asyncio.create_task(transport.acquire(worker_id=2, bind_key="pool-b", partition=part_b, timeout_ms=2000))
    await asyncio.sleep(0.01)
    assert not waiter_b.done()

    assert await transport.release(lease_a.lease_id) is True
    lease_b = await asyncio.wait_for(waiter_b, timeout=1.0)
    assert lease_b.pool_id == part_b
    assert await transport.release(lease_b.lease_id) is True
    assert transport.metrics().active_leases == 0


@pytest.mark.anyio
async def test_async_broker_zero_and_short_timeout_cleanup() -> None:
    """Verify zero-timeout fails immediately and short timeouts leave zero leaked waiters or leases."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))
    held = await transport.acquire(worker_id=1)

    with pytest.raises(TimeoutError, match="Connection broker pool exhausted"):
        await transport.acquire(worker_id=2, timeout_ms=0)
    with pytest.raises(TimeoutError, match="Connection broker pool exhausted"):
        await transport.acquire(worker_id=3, timeout=0.0)

    results = await asyncio.gather(
        *(transport.acquire(worker_id=10 + idx, timeout_ms=20) for idx in range(5)),
        return_exceptions=True,
    )
    assert all(isinstance(exc, TimeoutError) for exc in results)

    loop = asyncio.get_running_loop()
    await asyncio.sleep(0)
    queue = transport._waiter_queues[loop][None]
    assert queue.watcher_active is False
    assert queue.watcher_task is None
    assert len(queue.waiters) == 0
    assert transport.metrics().total_timeouts == 7

    assert await transport.release(held.lease_id) is True
    assert transport.metrics().active_leases == 0


@pytest.mark.anyio
async def test_async_broker_cross_thread_release_no_stale_local_wake_debt() -> None:
    """Verify late _on_slot_freed after watcher advances seq never swallows a subsequent remote release."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=2000))
    held1 = await transport.acquire(worker_id=1)
    held2 = await transport.acquire(worker_id=2)

    w1 = asyncio.create_task(transport.acquire(worker_id=11, timeout_ms=2000))
    w2 = asyncio.create_task(transport.acquire(worker_id=12, timeout_ms=2000))
    await asyncio.sleep(0.005)

    assert transport._native.release(held1.slot_index, held1.generation) is True
    l1 = await asyncio.wait_for(w1, timeout=1.0)
    assert not w2.done()

    transport._on_slot_freed(None)
    await asyncio.sleep(0.005)
    assert not w2.done()

    loop = asyncio.get_running_loop()
    assert transport._waiter_queues[loop][None].local_wakes == 0

    started = time.monotonic()
    assert transport._native.release(held2.slot_index, held2.generation) is True
    l2 = await asyncio.wait_for(w2, timeout=1.0)
    assert (time.monotonic() - started) * 1000.0 < 200.0

    for lease in (l1, l2):
        assert await transport.release(lease.lease_id) is True
    assert transport.metrics().active_leases == 0


def test_async_broker_eager_task_factory_no_deadlock() -> None:
    """Verify contended acquire never deadlocks _waiter_lock when loop uses asyncio.eager_task_factory."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=4, max_overflow=0, acquire_timeout_ms=2000))

    async def _run() -> None:
        loop = asyncio.get_running_loop()
        loop.set_task_factory(asyncio.eager_task_factory)

        async def _worker(idx: int) -> None:
            lease = await transport.acquire(worker_id=idx + 1, timeout_ms=2000)
            try:
                await asyncio.sleep(0.001)
            finally:
                lease.release_sync()

        await asyncio.wait_for(asyncio.gather(*(_worker(i) for i in range(64))), timeout=5.0)

    asyncio.run(_run())
    assert transport.metrics().active_leases == 0
    assert transport.metrics().total_acquired == 64
    assert transport.metrics().total_released == 64
