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


@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
def test_sync_postgres_pipeline_exit_contract(scenario: str) -> None:
    """SyncPostgresPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    cluster = FakePostgresCluster()
    _seed_accounts_table(cluster)
    pool = FakeSyncPostgresPool(cluster, max_size=1)
    drv = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    try:
        if scenario == "auto_flush":
            with drv.pipeline() as pipe:
                pipe.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "ada", 100))
            assert drv.in_transaction is False
            assert drv.select("SELECT id, owner, balance FROM accounts ORDER BY id") == [
                {"id": 1, "owner": "ada", "balance": 100},
            ]
        elif scenario == "caller_tx_rollback":
            drv.begin()
            drv.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "outer", 50))
            with drv.pipeline() as pipe:
                pipe.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (2, "inner", 75))
            assert drv.in_transaction is True
            assert drv.select("SELECT id FROM accounts ORDER BY id") == [{"id": 1}, {"id": 2}]
            drv.rollback()
            assert drv.select("SELECT id FROM accounts") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"), drv.pipeline() as pipe:
                pipe.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "discarded", 100))
                raise RuntimeError("abort")
            assert drv.in_transaction is False
            assert drv.select("SELECT id FROM accounts") == []
        else:
            drv.begin()
            drv.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "outer_kept", 50))
            with pytest.raises(RuntimeError, match="abort_inner"), drv.pipeline() as pipe:
                pipe.execute(
                    "INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (2, "inner_discarded", 99)
                )
                raise RuntimeError("abort_inner")
            assert drv.in_transaction is True
            drv.commit()
            assert drv.select("SELECT id, owner, balance FROM accounts ORDER BY id") == [
                {"id": 1, "owner": "outer_kept", "balance": 50},
            ]
    finally:
        assert len(pool.active) == 0
        drv.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
async def test_async_postgres_pipeline_exit_contract(scenario: str) -> None:
    """AsyncPostgresPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    cluster = FakePostgresCluster()
    _seed_accounts_table(cluster)
    pool = FakeAsyncPostgresPool(cluster, max_size=1)
    drv = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=pool)
    try:
        if scenario == "auto_flush":
            async with drv.pipeline() as pipe:
                pipe.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "ada", 100))
            assert drv.in_transaction is False
            assert await drv.select("SELECT id, owner, balance FROM accounts ORDER BY id") == [
                {"id": 1, "owner": "ada", "balance": 100},
            ]
        elif scenario == "caller_tx_rollback":
            await drv.begin()
            await drv.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "outer", 50))
            async with drv.pipeline() as pipe:
                pipe.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (2, "inner", 75))
            assert drv.in_transaction is True
            assert await drv.select("SELECT id FROM accounts ORDER BY id") == [{"id": 1}, {"id": 2}]
            await drv.rollback()
            assert await drv.select("SELECT id FROM accounts") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"):
                async with drv.pipeline() as pipe:
                    pipe.execute(
                        "INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)",
                        (1, "discarded", 100),
                    )
                    raise RuntimeError("abort")
            assert drv.in_transaction is False
            assert await drv.select("SELECT id FROM accounts") == []
        else:
            await drv.begin()
            await drv.execute("INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)", (1, "outer_kept", 50))
            with pytest.raises(RuntimeError, match="abort_inner"):
                async with drv.pipeline() as pipe:
                    pipe.execute(
                        "INSERT INTO accounts (id, owner, balance) VALUES ($1, $2, $3)",
                        (2, "inner_discarded", 99),
                    )
                    raise RuntimeError("abort_inner")
            assert drv.in_transaction is True
            await drv.commit()
            assert await drv.select("SELECT id, owner, balance FROM accounts ORDER BY id") == [
                {"id": 1, "owner": "outer_kept", "balance": 50},
            ]
    finally:
        assert len(pool.active) == 0
        await drv.close()
