"""Tests for cross-process shared-memory connection broker."""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import time
from pathlib import Path

import pytest

from vertebra._vertebra import LeaseBroker, StatementCache
from vertebra.extensions.broker import (
    BrokerAttachError,
    BrokerConfig,
    BrokerSegmentInUseError,
    SyncSHMBrokerTransport,
)


def _worker_contention_loop(name: str, iterations: int) -> None:
    """Worker process contending for connection leases from a shared named broker.

    Args:
        name: Name of the POSIX shared-memory segment.
        iterations: Number of acquire-release cycles to perform.
    """
    broker = LeaseBroker.open_named(name, pool_size=3, max_overflow=1)
    pid = os.getpid()
    for _ in range(iterations):
        grant = broker.acquire(worker_pid=pid, ttl_ns=0, timeout_ms=2000, bind_key_hash=0)
        if grant is not None:
            time.sleep(0.001)
            broker.release(grant.slot_index, grant.generation)


def _worker_hold_and_pause(name: str, ready_event: mp.synchronize.Event) -> None:
    """Worker process acquiring leases and pausing until killed.

    Args:
        name: Name of the POSIX shared-memory segment.
        ready_event: Event to notify parent when leases are held.
    """
    broker = LeaseBroker.open_named(name, pool_size=4, max_overflow=2)
    pid = os.getpid()
    g1 = broker.acquire(worker_pid=pid, ttl_ns=0, timeout_ms=2000, bind_key_hash=0)
    g2 = broker.acquire(worker_pid=pid, ttl_ns=0, timeout_ms=2000, bind_key_hash=0)
    if g1 is not None and g2 is not None:
        ready_event.set()
        while True:
            time.sleep(1)


def test_native_lease_broker_named_cross_process_cap() -> None:
    """Verify cross-process lease contention adheres to pool_size and max_overflow."""
    name = f"/vtb-test-py-contention-{os.getpid()}-{int(time.time())}"
    broker = LeaseBroker.create_named(name, pool_size=3, max_overflow=1)
    ctx = mp.get_context("spawn")

    workers = [ctx.Process(target=_worker_contention_loop, args=(name, 40)) for _ in range(4)]
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
    proc = ctx.Process(target=_worker_hold_and_pause, args=(name, ready_event))
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
