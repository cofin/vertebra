"""Unit and cross-process tests for co-located ShmStatementCache, lease reclamation, and SHM channel rings."""

from __future__ import annotations

import multiprocessing as mp
import os
import time
from pathlib import Path

from vertebra.extensions.broker import (
    BrokerConfig,
    InMemoryBrokerTransport,
    SHMBrokerTransport,
)


def _child_publish_and_compile(shm_name: str, pool_size: int, max_overflow: int, stmt_cap: int) -> None:
    """Attach to a named SHMBrokerTransport in a child process, compile a statement, and publish a channel message.

    Args:
        shm_name: Name of the POSIX shared-memory segment.
        pool_size: Primary pool capacity.
        max_overflow: Overflow slot capacity.
        stmt_cap: Statement cache slot capacity.
    """
    cfg = BrokerConfig(
        pool_size=pool_size,
        max_overflow=max_overflow,
        statement_cache_capacity=stmt_cap,
        shm_name=shm_name,
    )
    transport = SHMBrokerTransport(cfg)
    plan = transport.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        lambda _sql: ("SHOULD_NOT_COMPILE", ()),
    )
    assert plan.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert plan.param_names == ("id",)

    transport.publish("invalidation", b"evict:users:42", lease_id="0:1")


def test_shm_broker_colocated_statement_cache_single_region_and_fd() -> None:
    """Verify SHMBrokerTransport co-locates statement cache in a single ShmRegion and shares across FD attachment."""
    name = f"/vtb-colocated-{os.getpid()}-{int(time.time() * 1000)}"
    cfg = BrokerConfig(
        pool_size=4,
        max_overflow=2,
        statement_cache_capacity=32,
        shm_name=name,
    )
    primary = SHMBrokerTransport(cfg)
    assert primary.stmt_capacity == 32
    assert primary.stmt_offset > 0
    assert primary.shm_size > primary.stmt_offset

    separate_stmt_path = Path(f"/dev/shm/{name.lstrip('/')}-stmt")
    assert not separate_stmt_path.exists()

    compile_count = 0

    def compile_sql(sql: str) -> tuple[str, tuple[str, ...]]:
        nonlocal compile_count
        compile_count += 1
        return (sql.replace(":id", "$1"), ("id",))

    plan1 = primary.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        compile_sql,
    )
    assert plan1.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert plan1.param_names == ("id",)
    assert compile_count == 1

    assert primary.shm_fd is not None
    fd_attached = SHMBrokerTransport.from_fd(primary.shm_fd, primary.shm_size, cfg)
    plan2 = fd_attached.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        compile_sql,
    )
    assert plan2.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert plan2.param_names == ("id",)
    assert compile_count == 1
    assert fd_attached.metrics().statement_cache_hits >= 1

    fd_attached.invalidate_statements()
    plan3 = primary.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        compile_sql,
    )
    assert plan3.compiled_sql == "SELECT id, email FROM users WHERE id = $1"
    assert compile_count == 2


def test_shm_broker_active_leases_for_worker_and_reclaim() -> None:
    """Verify active_leases_for_worker and reclaim_worker on SHMBrokerTransport and InMemoryBrokerTransport."""
    cfg = BrokerConfig(pool_size=3, max_overflow=1, statement_cache_capacity=16)
    shm_transport = SHMBrokerTransport(cfg)

    l1 = shm_transport.acquire(worker_id=4100, bind_key="writer")
    l2 = shm_transport.acquire(worker_id=4100, bind_key="reader")
    l3 = shm_transport.acquire(worker_id=4200, bind_key="writer")

    worker_4100_leases = shm_transport.active_leases_for_worker(4100)
    assert len(worker_4100_leases) == 2
    slots_4100 = {entry[1] for entry in worker_4100_leases}
    assert slots_4100 == {l1.slot_index, l2.slot_index}

    worker_4200_leases = shm_transport.active_leases_for_worker(4200)
    assert len(worker_4200_leases) == 1
    assert worker_4200_leases[0][1] == l3.slot_index

    reclaimed = shm_transport.reclaim_worker(4100)
    assert reclaimed == 2
    assert shm_transport.active_leases_for_worker(4100) == []
    assert shm_transport.metrics().active_leases == 1
    assert shm_transport.metrics().total_reclaimed == 2

    assert shm_transport.release(l3.lease_id) is True
    assert shm_transport.metrics().active_leases == 0

    mem_transport = InMemoryBrokerTransport(cfg)
    ml1 = mem_transport.acquire(worker_id=5100, bind_key="primary")
    assert len(mem_transport.active_leases_for_worker(5100)) == 1
    assert mem_transport.reclaim_worker(5100) == 1
    assert mem_transport.active_leases_for_worker(5100) == []
    assert mem_transport.release(ml1.lease_id) is False


def test_shm_broker_cross_process_publish_and_drain_channel() -> None:
    """Verify publish and drain_channel communicate across processes via the shared-memory channel ring."""
    name = f"/vtb-chan-{os.getpid()}-{int(time.time() * 1000)}"
    cfg = BrokerConfig(
        pool_size=4,
        max_overflow=2,
        statement_cache_capacity=32,
        shm_name=name,
    )
    parent = SHMBrokerTransport(cfg)
    parent.get_or_compile_statement(
        "postgresql",
        "SELECT id, email FROM users WHERE id = :id",
        lambda sql: (sql.replace(":id", "$1"), ("id",)),
    )

    ctx = mp.get_context("spawn")
    proc = ctx.Process(
        target=_child_publish_and_compile,
        args=(name, cfg.pool_size, cfg.max_overflow, cfg.statement_cache_capacity),
    )
    proc.start()
    proc.join(timeout=15)
    assert proc.exitcode == 0

    messages = parent.drain_channel("invalidation")
    assert len(messages) == 1
    assert messages[0].channel == "invalidation"
    assert messages[0].payload == b"evict:users:42"
    assert messages[0].lease_id == "0:1"
    assert messages[0].timestamp_ns > 0
    assert parent.drain_channel("invalidation") == []


def test_shm_broker_channel_isolation_and_ring_wraparound() -> None:
    """Verify multi-channel isolation and bounded ring wraparound on SHMBrokerTransport."""
    cfg = BrokerConfig(pool_size=2, max_overflow=1, statement_cache_capacity=16)
    primary = SHMBrokerTransport(cfg)
    assert primary.shm_fd is not None
    secondary = SHMBrokerTransport.from_fd(primary.shm_fd, primary.shm_size, cfg)

    primary.publish("chan.alpha", b"a1", lease_id="0:1")
    secondary.publish("chan.beta", b"b1", lease_id="1:1")
    primary.publish("chan.alpha", b"a2")

    beta_msgs = primary.drain_channel("chan.beta")
    assert [m.payload for m in beta_msgs] == [b"b1"]
    assert beta_msgs[0].lease_id == "1:1"

    alpha_msgs = secondary.drain_channel("chan.alpha")
    assert [m.payload for m in alpha_msgs] == [b"a1", b"a2"]
    assert alpha_msgs[0].lease_id == "0:1"
    assert alpha_msgs[1].lease_id is None

    for idx in range(70):
        primary.publish("chan.wrap", f"msg-{idx}".encode())

    wrapped = secondary.drain_channel("chan.wrap")
    assert len(wrapped) == 64
    assert wrapped[0].payload == b"msg-6"
    assert wrapped[-1].payload == b"msg-69"
