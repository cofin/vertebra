"""Unit tests for ``SyncPostgreSQLDriver`` and ``AsyncPostgreSQLDriver`` native execution and lifecycle."""

from __future__ import annotations

import asyncio
import inspect

import pytest

from tests.unit.db.postgres_ports import (
    FakeAsyncPostgresPool,
    FakePostgresCluster,
    FakeSyncPostgresPool,
)
from vertebra._vertebra import InterfaceError, OperationalError
from vertebra.db.adapters.postgres import _async as async_pg_mod
from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.adapters.postgres.types import PgJson
from vertebra.db.rls import AsyncTenantConnectionScope, TenantConnectionScope, TenantScopeConfig
from vertebra.typing import ImproperConfigurationError


@pytest.fixture
def anyio_backend() -> str:
    """Restrict async adapter tests cleanly to asyncio."""
    return "asyncio"


def _seed_items_table(cluster: FakePostgresCluster) -> None:
    """Create the sample transactional items table inside the simulated cluster."""
    cluster._conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)")


@pytest.mark.parametrize("dsn", ["memory://", "MEMORY://local", "memory://test"])
def test_drivers_reject_memory_dsn(dsn: str) -> None:
    """Both native drivers must fail fast on unsupported ``memory://`` URIs."""
    cfg = PostgreSQLConfig(dsn=dsn)
    with pytest.raises(ImproperConfigurationError):
        SyncPostgreSQLDriver(cfg)
    with pytest.raises(ImproperConfigurationError):
        AsyncPostgreSQLDriver(cfg)


def test_drivers_reject_memory_direct_dsn() -> None:
    """Reject ``memory://`` configured via ``direct_dsn`` on construction."""
    cfg = PostgreSQLConfig(dsn="postgresql://localhost:5432/app", direct_dsn="memory://direct")
    with pytest.raises(ImproperConfigurationError):
        SyncPostgreSQLDriver(cfg)
    with pytest.raises(ImproperConfigurationError):
        AsyncPostgreSQLDriver(cfg)


def test_sync_query_compilation_projection_and_pool_reuse() -> None:
    """Sync driver compiles binds, unwraps ``PgJson(None)``, forwards timeouts, and reuses pooled leases."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)

    with SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool) as drv:
        count = drv.execute_many(
            "INSERT INTO items (id, name) VALUES (%s, %s)",
            [(1, "alpha"), (2, "beta")],
            timeout=2.5,
        )
        assert count == 2
        assert len(pool.active) == 0
        assert len(pool.idle) == 1

        rows, cols = drv.execute_with_description(
            "SELECT id, name FROM items WHERE name = %(label)s ORDER BY id",
            {"label": "beta"},
            timeout=1.25,
        )
        assert cols == ("id", "name")
        assert rows == [(2, "beta")]
        assert cluster.recorded_queries[-1][2] == 1.25

        null_json_rows = drv.execute_tuples("SELECT $1 AS j", (PgJson(None),))
        assert null_json_rows == [(None,)]

    assert pool.closed is True
    assert drv.closed is True
    with pytest.raises(InterfaceError):
        drv.execute("SELECT 1")


def test_sync_transaction_savepoints_and_rollback_eviction() -> None:
    """Sync transactions support savepoints, atomic rollbacks, and evict clients on failed rollback."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    with drv.transaction():
        drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (1, "kept"))
        sp = drv.savepoint()
        drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (2, "discarded"))
        drv.rollback_to_savepoint(sp)
        drv.release_savepoint(sp)

    assert drv.execute_tuples("SELECT id, name FROM items ORDER BY id") == [(1, "kept")]
    assert len(pool.active) == 0
    assert len(pool.idle) == 1

    with pytest.raises(RuntimeError), drv.transaction():
        drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (3, "aborted"))
        raise RuntimeError("boom")
    assert drv.execute_tuples("SELECT COUNT(*) FROM items") == [(1,)]

    cluster.rollback_failures_remaining = 1
    drv.begin()
    drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (4, "evict-me"))
    with pytest.raises(OperationalError):
        drv.rollback()
    assert len(pool.evicted) == 1
    assert len(pool.active) == 0
    assert drv.in_transaction is False
    drv.close()


def test_sync_tenant_scope_and_reset_failure_eviction() -> None:
    """``TenantConnectionScope`` pins a session GUC and evicts the physical connection if reset fails."""
    cluster = FakePostgresCluster()
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")

    with TenantConnectionScope(drv, "tenant-a", scope_cfg):
        assert drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-a",)]
        assert len(pool.active) == 1
    assert len(pool.active) == 0
    assert drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("",)]

    cluster.set_config_reset_failures_remaining = 1
    with pytest.raises(OperationalError), TenantConnectionScope(drv, "tenant-b", scope_cfg):
        assert drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-b",)]
    assert len(pool.evicted) == 1
    assert len(pool.active) == 0
    drv.close()


def test_sync_copy_and_cancel() -> None:
    """Sync driver executes binary COPY FROM / COPY TO and forwards ``cancel()``."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=2)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    inserted = drv.copy_from(
        "COPY items (id, name) FROM STDIN (FORMAT BINARY)",
        [(10, "row-a"), (11, "row-b")],
        column_oids=(23, 25),
    )
    assert inserted == 2
    with drv.copy_to("COPY (SELECT id, name FROM items ORDER BY id) TO STDOUT (FORMAT BINARY)") as out:
        copied = list(out)
    assert copied == [(b"10", b"row-a"), (b"11", b"row-b")]

    drv.begin()
    drv.cancel()
    assert pool.active[0].cancel_calls == 1
    drv.rollback()
    drv.close()


def test_async_driver_has_zero_thread_offload() -> None:
    """``_async.py`` must execute natively with zero ``asyncio.to_thread`` wrappers."""
    source = inspect.getsource(async_pg_mod)
    assert "to_thread" not in source


@pytest.mark.anyio
async def test_async_query_transaction_savepoints_and_pool_lifecycle() -> None:
    """Async driver executes parameterized queries, transactions, savepoints, and cleanup cleanly."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)

    async with AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool) as drv:
        count = await drv.execute_many(
            "INSERT INTO items (id, name) VALUES (%(id)s, %(name)s)",
            [{"id": 1, "name": "one"}, {"id": 2, "name": "two"}],
            timeout=3.0,
        )
        assert count == 2
        assert len(pool.active) == 0

        async with drv.transaction():
            sp = await drv.savepoint(7)
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (3, "temp"))
            await drv.rollback_to_savepoint(sp)
            await drv.release_savepoint(sp)
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (4, "kept"))

        rows, cols = await drv.execute_with_description("SELECT id, name FROM items ORDER BY id")
        assert cols == ("id", "name")
        assert rows == [(1, "one"), (2, "two"), (4, "kept")]

    assert pool.closed is True
    assert drv.closed is True
    with pytest.raises(InterfaceError):
        await drv.execute("SELECT 1")


@pytest.mark.anyio
async def test_async_cancellation_shields_rollback_and_evicts_tainted_clients() -> None:
    """Cancelling an active async query or transaction shields cleanup and evicts tainted sessions."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)

    cluster.query_started_event = asyncio.Event()
    cluster.query_release_event = asyncio.Event()

    async def _run_tx() -> None:
        async with drv.transaction():
            await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (99, "cancelled"))

    task = asyncio.create_task(_run_tx())
    await cluster.query_started_event.wait()
    await drv.cancel()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert drv.in_transaction is False
    assert len(pool.active) == 0
    cluster.query_started_event = None
    cluster.query_release_event = None
    assert await drv.execute_tuples("SELECT COUNT(*) FROM items") == [(0,)]

    cluster.rollback_failures_remaining = 1
    await drv.begin()
    await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (50, "dirty"))
    with pytest.raises(OperationalError):
        await drv.rollback()
    assert len(pool.evicted) == 1
    assert len(pool.active) == 0
    await drv.close()


@pytest.mark.anyio
async def test_async_tenant_scope_copy_and_close_rollback() -> None:
    """Async driver supports ``AsyncTenantConnectionScope``, binary COPY, and rollback-on-close."""
    cluster = FakePostgresCluster()
    _seed_items_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=2)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    scope_cfg = TenantScopeConfig(setting_name="app.tenant_id")

    async with AsyncTenantConnectionScope(drv, "tenant-async", scope_cfg):
        assert await drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-async",)]
    assert len(pool.active) == 0

    cluster.set_config_reset_failures_remaining = 1
    with pytest.raises(OperationalError):
        async with AsyncTenantConnectionScope(drv, "tenant-broken", scope_cfg):
            assert await drv.execute_tuples("SELECT current_setting($1)", ("app.tenant_id",)) == [("tenant-broken",)]
    assert len(pool.evicted) == 1

    inserted = await drv.copy_from(
        "COPY items (id, name) FROM STDIN (FORMAT BINARY)",
        [(20, "async-a"), (21, "async-b")],
        column_oids=(23, 25),
    )
    assert inserted == 2
    async with drv.copy_to("COPY (SELECT id, name FROM items ORDER BY id) TO STDOUT (FORMAT BINARY)") as out:
        copied = [row async for row in out]
    assert copied == [(b"20", b"async-a"), (b"21", b"async-b")]

    await drv.begin()
    await drv.execute("INSERT INTO items (id, name) VALUES ($1, $2)", (22, "uncommitted"))
    await drv.close()
    assert pool.closed is True
    assert cluster._conn.execute("SELECT COUNT(*) FROM items").fetchone()[0] == 2
