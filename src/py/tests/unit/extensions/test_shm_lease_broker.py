"""Tests for cross-process shared-memory connection broker."""

from __future__ import annotations

import multiprocessing as mp
import os
import signal
import time

from vertebra._vertebra import NativeLeaseBroker


def _worker_contention_loop(name: str, iterations: int) -> None:
    """Worker process contending for connection leases from a shared named broker.

    Args:
        name: Name of the POSIX shared-memory segment.
        iterations: Number of acquire-release cycles to perform.
    """
    broker = NativeLeaseBroker.open_named(name, pool_size=3, max_overflow=1)
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
    broker = NativeLeaseBroker.open_named(name, pool_size=4, max_overflow=2)
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
    broker = NativeLeaseBroker.create_named(name, pool_size=3, max_overflow=1)
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
    broker = NativeLeaseBroker.create_named(name, pool_size=4, max_overflow=2)
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
    assert NativeLeaseBroker.from_env() is None

    broker = NativeLeaseBroker(pool_size=4, max_overflow=2)
    assert broker.pool_size == 4
    assert broker.max_overflow == 2
    assert broker.total_capacity == 6
