"""Unit tests for sync and async ``SyncPostgreSQLDriver.pipeline()`` / ``AsyncPostgreSQLDriver.pipeline()``."""

from __future__ import annotations

import pytest

from tests.unit.db.postgres_ports import (
    FakeAsyncPostgresPool,
    FakePostgresCluster,
    FakeSyncPostgresPool,
)
from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.config import PostgreSQLConfig
from vertebra.db.driver.capabilities import AsyncPipelineCapable, SyncPipelineCapable


@pytest.fixture
def anyio_backend() -> str:
    """Restrict async pipeline tests cleanly to asyncio."""
    return "asyncio"


def _seed_accounts_table(cluster: FakePostgresCluster) -> None:
    """Create a sample accounts table for pipeline batch tests."""
    cluster._conn.execute("CREATE TABLE accounts (id INTEGER PRIMARY KEY, owner TEXT, balance INTEGER)")


def test_sync_pipeline_pooled_and_transactional_scopes() -> None:
    """Sync pipeline uses one pooled lease when idle and reuses the pinned tx connection when active."""
    cluster = FakePostgresCluster()
    _seed_accounts_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=1)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    assert isinstance(drv, SyncPipelineCapable)

    pipe = drv.pipeline()
    assert len(pool.active) == 0
    with pipe as active:
        active.execute("INSERT INTO accounts (id, owner, balance) VALUES (%s, %s, %s)", (1, "ada", 100))
        active.execute(
            "INSERT INTO accounts (id, owner, balance) VALUES (%(id)s, %(owner)s, %(bal)s)",
            {"id": 2, "owner": "bob", "bal": 250},
        )
        active.select("SELECT id, owner, balance FROM accounts ORDER BY id")
        results = active.results()

    assert results == [1, 1, [{"id": 1, "owner": "ada", "balance": 100}, {"id": 2, "owner": "bob", "balance": 250}]]
    assert len(pool.active) == 0
    assert len(pool.idle) == 1

    with drv.transaction():
        assert len(pool.active) == 1
        drv.execute("UPDATE accounts SET balance = balance + 50 WHERE id = $1", (1,))
        with pytest.raises(RuntimeError), drv.pipeline() as tx_pipe:
            tx_pipe.execute("UPDATE accounts SET balance = balance + 500 WHERE id = $1", (1,))
            tx_pipe.results()
            raise RuntimeError("abort inner pipeline savepoint")
        with drv.pipeline() as tx_pipe_ok:
            tx_pipe_ok.select("SELECT balance FROM accounts WHERE id = $1", (1,))
            ok_results = tx_pipe_ok.results()
        assert ok_results == [[{"balance": 150}]]

    assert len(pool.active) == 0
    drv.close()


@pytest.mark.anyio
async def test_async_pipeline_pooled_and_transactional_scopes() -> None:
    """Async pipeline binds natively over a single lease or the active pinned transaction."""
    cluster = FakePostgresCluster()
    _seed_accounts_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=1)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    assert isinstance(drv, AsyncPipelineCapable)

    pipe = drv.pipeline()
    assert pool.acquire_calls == 0
    async with pipe as active:
        active.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "ada", 100))
        active.select("SELECT id, owner, balance FROM accounts WHERE id = $1", (1,))
        results = await active.results()

    assert results == [1, [{"id": 1, "owner": "ada", "balance": 100}]]
    assert pool.acquire_calls == 1
    assert len(pool.active) == 0
    assert len(pool.idle) == 1

    async with drv.transaction():
        assert pool.acquire_calls == 2
        with pytest.raises(RuntimeError):
            async with drv.pipeline() as inner:
                inner.execute("UPDATE accounts SET balance = 999 WHERE id = $1", (1,))
                await inner.results()
                raise RuntimeError("rollback savepoint only")
        async with drv.pipeline() as verify_pipe:
            verify_pipe.select("SELECT balance FROM accounts WHERE id = $1", (1,))
            verified = await verify_pipe.results()
        assert verified == [[{"balance": 100}]]
        assert pool.acquire_calls == 2

    assert len(pool.active) == 0
    await drv.close()
