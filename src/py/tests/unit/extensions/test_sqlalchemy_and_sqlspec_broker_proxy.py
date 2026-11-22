"""Unit tests for BrokerPool, SQLSpec providers, Proxy*Driver isolation, contention, and cancellation."""

from __future__ import annotations

import ast
import asyncio
import os
import threading
import warnings
from pathlib import Path
from typing import Any, Literal, cast

import pyarrow as pa
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import AsyncAdaptedQueuePool, NullPool, QueuePool
from sqlalchemy.util.concurrency import greenlet_spawn
from sqlspec import SQL, StatementStack, sql
from sqlspec.core.filters import LimitOffsetFilter
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.result import ArrowResult
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, AsyncRowStream, SyncDriverAdapterBase, SyncRowStream
from sqlspec.exceptions import ImproperConfigurationError as SQLSpecImproperConfigurationError
from sqlspec.exceptions import SQLSpecError, StackExecutionError

from vertebra._vertebra import LeaseBroker
from vertebra.db import DatabaseRegistry
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, SyncPostgreSQLDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
from vertebra.db.dbapi import (
    AsyncDBAPIConnection,
    InterfaceError,
    SyncDBAPIConnection,
    SyncDBAPIModule,
)
from vertebra.db.driver import InterfaceError as DriverInterfaceError
from vertebra.extensions.alchemy import (
    create_async_alchemy_config,
    create_sync_alchemy_config,
)
from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.extensions.broker.transport import AsyncSHMBrokerTransport, BrokerConfig, SyncSHMBrokerTransport
from vertebra.extensions.sqlalchemy import (
    SyncPostgreSQLDialect,
)
from vertebra.extensions.sqlalchemy.async_adapter import (
    AsyncAdapt_vertebra_connection,
    SQLAlchemyAsyncDBAPI,
)
from vertebra.extensions.sqlalchemy.dialects import (
    AsyncPostgreSQLDialect,
)
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, BrokerPool, broker_creator
from vertebra.extensions.sqlspec.plugin import ProxyAsyncConfig, ProxySyncConfig
from vertebra.extensions.sqlspec.provider import (
    AsyncBrokerLeaseSource,
    BrokerRpcRequest,
    BrokerRpcResponse,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
    SyncBrokerLeaseSource,
)
from vertebra.extensions.sqlspec.proxy import ProxyAsyncDriver, ProxySyncDriver
from vertebra.typing import ImproperConfigurationError


def test_sync_dbapi_connection_driver_ownership_and_broker_isolation() -> None:
    """Shared registry drivers survive connection close while dedicated and custom-broker drivers close."""
    url = "postgresql+vtb://localhost:5432/ownership_sync_db"
    shared_conn = SyncDBAPIConnection(url, share_pool=True)
    shared_driver = shared_conn.driver
    assert isinstance(shared_driver, SyncPostgreSQLDriver)
    assert shared_driver is DatabaseRegistry.default().get_shared_sync_driver(shared_conn.config)
    shared_conn.close()
    assert shared_driver._closed is False

    owned_conn = SyncDBAPIConnection(url, share_pool=False)
    owned_driver = owned_conn.driver
    assert isinstance(owned_driver, SyncPostgreSQLDriver)
    assert owned_driver is not shared_driver
    owned_conn.close()
    assert owned_driver._closed is True
    assert shared_driver._closed is False

    custom_broker = LeaseBroker(pool_size=4, max_overflow=2)
    broker_conn = SyncDBAPIConnection(url, broker=custom_broker, share_pool=True)
    broker_driver = broker_conn.driver
    assert isinstance(broker_driver, SyncPostgreSQLDriver)
    assert broker_driver is not shared_driver
    assert broker_driver.config.broker is custom_broker
    broker_conn.close()
    assert broker_driver._closed is True
    assert shared_driver._closed is False


@pytest.mark.anyio
async def test_async_dbapi_connection_driver_ownership_broker_isolation_and_close_settlement(
    tmp_path: Path,
) -> None:
    """Async DBAPI connections keep shared drivers open, isolate custom brokers, and settle active transactions."""
    url = "postgresql+vtb_async://localhost:5432/ownership_async_db"
    shared_conn = AsyncDBAPIConnection(url, share_pool=True)
    shared_driver = shared_conn.driver
    assert isinstance(shared_driver, AsyncPostgreSQLDriver)
    assert shared_driver is DatabaseRegistry.default().get_shared_driver(shared_conn.config)
    await shared_conn.close()
    assert shared_driver._closed is False

    owned_conn = AsyncDBAPIConnection(url, share_pool=False)
    owned_driver = owned_conn.driver
    assert isinstance(owned_driver, AsyncPostgreSQLDriver)
    assert owned_driver is not shared_driver
    await owned_conn.close()
    assert owned_driver._closed is True
    assert shared_driver._closed is False

    custom_broker = LeaseBroker(pool_size=4, max_overflow=2)
    broker_conn = AsyncDBAPIConnection(url, broker=custom_broker, share_pool=True)
    broker_driver = broker_conn.driver
    assert isinstance(broker_driver, AsyncPostgreSQLDriver)
    assert broker_driver is not shared_driver
    assert broker_driver.config.broker is custom_broker
    await broker_conn.close()
    assert broker_driver._closed is True
    assert shared_driver._closed is False

    sqlite_url = f"sqlite+vtb_async:///{tmp_path / 'close_tx.db'}"
    setup_conn = AsyncDBAPIConnection(sqlite_url, share_pool=True)
    sqlite_drv = setup_conn.driver
    await setup_conn.cursor().execute("CREATE TABLE close_tx_items (id INTEGER)")
    await setup_conn.commit()
    await setup_conn.close()

    tx_conn = AsyncDBAPIConnection(sqlite_url, share_pool=True)
    assert tx_conn.driver is sqlite_drv
    await tx_conn.cursor().execute("INSERT INTO close_tx_items VALUES (?)", (1,))
    await tx_conn.close()
    assert sqlite_drv._sync._closed is False
    assert await sqlite_drv.select_value("SELECT COUNT(*) FROM close_tx_items") == 0
    await sqlite_drv.close()


def test_sync_dbapi_connection_rejects_unparsable_url() -> None:
    """SyncDBAPIConnection and SyncDBAPIModule.connect raise PEP 249 InterfaceError on unparsable URLs."""
    with pytest.raises(
        InterfaceError,
        match=r"Cannot parse Vertebra database URL: Unsupported database URL scheme: 'nonsense\+vtb'",
    ) as exc_info:
        SyncDBAPIConnection("nonsense+vtb://host/db", share_pool=False)
    assert isinstance(exc_info.value.__cause__, ValueError)

    assert SyncDBAPIModule.InterfaceError is InterfaceError
    with pytest.raises(InterfaceError):
        SyncDBAPIModule().connect("nonsense+vtb://host/db")

    with pytest.raises(InterfaceError) as empty_exc:
        SyncDBAPIConnection("", share_pool=False)
    assert isinstance(empty_exc.value.__cause__, ValueError)
    assert str(empty_exc.value.__cause__) == "Unsupported database URL scheme: ''"

    driver = SyncSQLiteDriver()
    try:
        with pytest.raises(InterfaceError):
            SyncDBAPIConnection("nonsense+vtb://host/db", driver=driver, share_pool=False)
    finally:
        driver.close()


def test_async_dbapi_connection_rejects_unparsable_url() -> None:
    """AsyncDBAPIConnection and AsyncDBAPIModule.connect raise PEP 249 InterfaceError on unparsable URLs."""
    with pytest.raises(
        InterfaceError,
        match=r"'nonsense\+vtb_async'",
    ) as exc_info:
        AsyncDBAPIConnection("nonsense+vtb_async://host/db", share_pool=False)
    assert isinstance(exc_info.value.__cause__, ValueError)

    assert cast("Any", SQLAlchemyAsyncDBAPI.InterfaceError) is InterfaceError
    with pytest.raises(InterfaceError):
        SQLAlchemyAsyncDBAPI().connect("nonsense+vtb_async://host/db")


@pytest.mark.parametrize("failure_mode", ["rollback_error", "exit_error", "quarantine_error"])
@pytest.mark.anyio
async def test_sqlspec_provider_quarantines_without_release_on_sync_and_async_failures(failure_mode: str) -> None:
    """Verify sync/async provider and proxy session release quarantines without release and preserves context."""

    class RecordingLease:
        def __init__(self, slot: int, gen: int) -> None:
            self.slot_index = slot
            self.generation = gen
            self.released = 0
            self.quarantined = False

        def release(self) -> None:
            self.released += 1

    class SyncBrokerDouble:
        def __init__(self) -> None:
            self.leases: list[RecordingLease] = []
            self.quarantined_calls = 0

        def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> RecordingLease:
            _ = (bind_key, worker_id)
            lease = RecordingLease(2, len(self.leases) + 1)
            self.leases.append(lease)
            return lease

        def quarantine(self, lease: RecordingLease) -> None:
            self.quarantined_calls += 1
            lease.quarantined = True
            if failure_mode == "quarantine_error":
                raise OSError("quarantine backend unreachable")

    class SyncFailingSession:
        def __init__(self) -> None:
            self.exit_called = False

        def __enter__(self) -> SyncFailingSession:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.exit_called = True
            if failure_mode == "exit_error":
                raise RuntimeError("session context exit failure")

        def execute(self, sql: str, *parameters: Any) -> int:
            _ = (sql, parameters)
            return 1

        def rollback(self) -> None:
            if failure_mode in ("rollback_error", "quarantine_error"):
                raise ValueError("dirty rollback failure")

    sync_broker = SyncBrokerDouble()
    assert isinstance(sync_broker, SyncBrokerLeaseSource)
    sync_sessions: list[SyncFailingSession] = []

    def _build_sync_session() -> SyncFailingSession:
        sess = SyncFailingSession()
        sync_sessions.append(sess)
        return sess

    sync_provider = SQLSpecSyncProvider(_build_sync_session, broker=sync_broker)
    sync_driver = ProxySyncDriver(sync_provider, broker=sync_broker)
    sync_driver.begin()

    with pytest.raises(LeaseQuarantinedError) as exc_info:
        sync_driver.release_lease()
    assert sync_sessions[0].exit_called is True
    if failure_mode == "quarantine_error":
        assert "quarantine backend unreachable" in str(exc_info.value.__cause__)
        assert "dirty rollback failure" in str(exc_info.value.__cause__)
    assert sync_broker.quarantined_calls == 1
    assert sync_broker.leases[0].released == 0

    class AsyncBrokerDouble:
        def __init__(self) -> None:
            self.leases: list[RecordingLease] = []
            self.quarantined_calls = 0

        async def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> RecordingLease:
            _ = (bind_key, worker_id)
            lease = RecordingLease(3, len(self.leases) + 1)
            self.leases.append(lease)
            return lease

        async def quarantine(self, lease: RecordingLease) -> None:
            self.quarantined_calls += 1
            lease.quarantined = True
            if failure_mode == "quarantine_error":
                raise OSError("async quarantine failure")

    class AsyncFailingSession:
        def __init__(self) -> None:
            self.exit_called = False

        async def __aenter__(self) -> AsyncFailingSession:
            return self

        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.exit_called = True
            if failure_mode == "exit_error":
                raise RuntimeError("async context exit failure")

        async def execute(self, sql: str, *parameters: Any) -> int:
            _ = (sql, parameters)
            return 1

        async def rollback(self) -> None:
            if failure_mode in ("rollback_error", "quarantine_error"):
                raise ValueError("async rollback failure")

    async_broker = AsyncBrokerDouble()
    assert isinstance(async_broker, AsyncBrokerLeaseSource)
    async_sessions: list[AsyncFailingSession] = []

    def _build_async_session() -> AsyncFailingSession:
        sess = AsyncFailingSession()
        async_sessions.append(sess)
        return sess

    async_provider = SQLSpecAsyncProvider(_build_async_session, broker=async_broker)
    async_driver = ProxyAsyncDriver(async_provider, broker=async_broker)
    await async_driver.begin()

    with pytest.raises(LeaseQuarantinedError) as a_exc_info:
        await async_driver.release_lease()
    assert async_sessions[0].exit_called is True
    if failure_mode == "quarantine_error":
        assert "async quarantine failure" in str(a_exc_info.value.__cause__)
        assert "async rollback failure" in str(a_exc_info.value.__cause__)
    assert async_broker.quarantined_calls == 1
    assert async_broker.leases[0].released == 0


@pytest.mark.parametrize(
    ("dialect_name", "style"),
    [
        ("postgres", ParameterStyle.NUMERIC),
        ("oracle", ParameterStyle.NAMED_COLON),
        ("sqlite", ParameterStyle.QMARK),
    ],
)
def test_proxy_drivers_subclass_sqlspec_bases_and_preserve_statement_config(
    dialect_name: str,
    style: ParameterStyle,
) -> None:
    """Verify ProxySyncDriver/ProxyAsyncDriver inherit adapter bases, preserve statement_config, and stay reflect-free."""
    stmt_cfg = StatementConfig(
        dialect=dialect_name,
        parameter_config=ParameterStyleConfig(default_parameter_style=style, supported_parameter_styles={style}),
    )

    class WrappedCfg:
        dialect = dialect_name
        bind_key = "primary"
        statement_config = stmt_cfg
        driver_features: dict[str, Any] = {}

        def provide_session(self) -> Any:
            return self

    proxy_sync_cfg = ProxySyncConfig(WrappedCfg())
    proxy_async_cfg = ProxyAsyncConfig(WrappedCfg())
    sync_driver = ProxySyncDriver(SQLSpecSyncProvider(proxy_sync_cfg), statement_config=proxy_sync_cfg.statement_config)
    async_driver = ProxyAsyncDriver(
        SQLSpecAsyncProvider(proxy_async_cfg), statement_config=proxy_async_cfg.statement_config
    )
    assert isinstance(sync_driver, SyncDriverAdapterBase)
    assert isinstance(async_driver, AsyncDriverAdapterBase)
    assert sync_driver.statement_config is stmt_cfg
    assert async_driver.statement_config is stmt_cfg
    assert sync_driver.dialect == dialect_name
    assert async_driver.dialect == dialect_name

    assert BrokerRpcRequest.__struct_config__.gc is True
    assert BrokerRpcResponse.__struct_config__.gc is True
    assert sync_driver.data_dictionary is None
    assert async_driver.data_dictionary is None
    assert sync_driver.provider is not None
    assert sync_driver.provider.resolve_upstream_optional("primary") is not None


@pytest.mark.parametrize(
    "factory",
    [
        lambda: BrokerPool(),
        lambda: AsyncBrokerPool(),
        lambda: BrokerPool(broker=SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))),
        lambda: broker_creator("sqlite+vtb:///:memory:", broker=None),
        lambda: broker_creator("", broker=SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))),
    ],
)
def test_broker_pool_and_creator_reject_unconfigured_inputs(factory: Any) -> None:
    """Verify BrokerPool, AsyncBrokerPool, and broker_creator raise ImproperConfigurationError when unconfigured."""
    with pytest.raises(ImproperConfigurationError):
        factory()


def test_sync_broker_pool_checkout_rollback_quarantine_and_recreate(tmp_path: Path) -> None:
    """Verify BrokerPool acquires partition leases, releases healthy checkins, and quarantines rollback errors."""
    url = f"sqlite+vtb:///{tmp_path / 'broker_pool.db'}"
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    pool = BrokerPool(broker=transport, url=url)
    pool_key_tuple = DatabaseRegistry.parse_url(url, broker=transport).pool_key()
    assert pool_key_tuple is not None
    expected_partition = "|".join(str(part) for part in pool_key_tuple)
    assert pool.partition_key == expected_partition

    engine = create_engine(url, pool=pool)
    try:
        try:
            with engine.connect() as conn:
                row = conn.exec_driver_sql("SELECT 7").fetchone()
                assert row == (7,)
                assert transport.metrics().active_leases == 1
            assert transport.metrics().active_leases == 0
            assert transport.metrics().total_released == 1
            assert transport.quarantined_count() == 0

            recreated = pool.recreate()
            try:
                assert isinstance(recreated, BrokerPool)
                assert recreated.partition_key == expected_partition
            finally:
                recreated.dispose()
        finally:
            engine.dispose()

        def _boom_rollback(_conn: Any) -> None:
            raise RuntimeError("dirty connection reset error")

        fail_pool = BrokerPool(broker=transport, url=url, on_rollback=_boom_rollback)
        fail_engine = create_engine(url, pool=fail_pool)
        try:
            with fail_engine.connect() as conn:
                lease = conn.connection.info["vtb_broker_lease"]
                raw_dbapi = conn.connection.dbapi_connection
                conn.exec_driver_sql("SELECT 1").fetchone()
            assert raw_dbapi is not None and raw_dbapi.closed is True
            assert transport.quarantined_count() == 1
            assert transport.metrics().total_quarantined == 1
            assert transport.metrics().total_released == 1
            transport.dispose_quarantined(lease.slot_index, lease.generation)
            assert transport.quarantined_count() == 0
        finally:
            fail_engine.dispose()
    finally:
        transport.close()


@pytest.mark.anyio
async def test_async_broker_pool_checkout_rollback_and_quarantine() -> None:
    """Verify AsyncBrokerPool coordinates greenlet checkouts with AsyncSHMBrokerTransport and quarantines failures."""
    url = "sqlite+vtb_async:///:memory:"
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    pool = AsyncBrokerPool(broker=transport, url=url)
    async_engine = create_async_engine(url, pool=pool)
    try:
        try:
            async with async_engine.connect() as conn:
                res = await conn.exec_driver_sql("SELECT 9")
                assert res.fetchone() == (9,)
                assert transport.metrics().active_leases == 1
            assert transport.metrics().active_leases == 0
            assert transport.metrics().total_released == 1
            assert transport.quarantined_count() == 0
        finally:
            await async_engine.dispose()

        def _boom_async_rollback(_conn: Any) -> None:
            raise RuntimeError("async dirty rollback error")

        fail_pool = AsyncBrokerPool(broker=transport, url=url, on_rollback=_boom_async_rollback)
        fail_engine = create_async_engine(url, pool=fail_pool)
        try:
            async with fail_engine.connect() as conn:
                raw_fairy = await conn.get_raw_connection()
                lease = raw_fairy.info["vtb_broker_lease"]
                raw_dbapi = raw_fairy.driver_connection
                await conn.exec_driver_sql("SELECT 1")
            assert raw_dbapi is not None and raw_dbapi.closed is True
            assert transport.quarantined_count() == 1
            assert transport.metrics().total_quarantined == 1
            await transport.dispose_quarantined(lease.slot_index, lease.generation)
        finally:
            await fail_engine.dispose()
    finally:
        await transport.close()


@pytest.mark.parametrize("is_async", [False, True])
def test_alchemy_plugin_wires_broker_pool_classes(is_async: bool) -> None:
    """Verify create_sync_alchemy_config and create_async_alchemy_config wire BrokerPool/AsyncBrokerPool."""
    broker = (
        AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
        if is_async
        else SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    )
    try:
        if is_async:
            async_cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:", broker=broker)
            assert isinstance(async_cfg.engine_config.poolclass, type)
            assert issubclass(async_cfg.engine_config.poolclass, AsyncBrokerPool)
        else:
            sync_cfg = create_sync_alchemy_config("sqlite+vtb:///:memory:", broker=broker)
            assert isinstance(sync_cfg.engine_config.poolclass, type)
            assert issubclass(sync_cfg.engine_config.poolclass, BrokerPool)
    finally:
        if isinstance(broker, AsyncSHMBrokerTransport):
            asyncio.run(broker.close())
        else:
            broker.close()


@pytest.mark.anyio
@pytest.mark.parametrize("count_with_window", [False, True])
async def test_proxy_drivers_compile_sql_filters_stacks_and_common_driver_params(count_with_window: bool) -> None:
    """Verify proxy drivers compile SQL/filters/styles, dispatch stacks/totals, bind single strings, and unwrap configs."""
    pg_cfg = StatementConfig(
        dialect="postgres",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC,
            supported_parameter_styles={ParameterStyle.NUMERIC},
        ),
    )
    pg_provider = SQLSpecSyncProvider(SyncSQLiteDriver())
    pg_proxy = ProxySyncDriver(pg_provider, statement_config=pg_cfg)
    try:
        compiled_sql, compiled_params, _ = pg_proxy.compile_with_broker(
            pg_proxy,
            pg_proxy.broker,
            "postgres",
            SQL("SELECT id FROM items WHERE id = :id", id=42),
        )
        assert compiled_sql.strip() == "SELECT id FROM items WHERE id = $1"
        assert compiled_params == [42]
    finally:
        pg_proxy.close()
        pg_provider.close()

    sqlite_cfg = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
        ),
    )
    sync_driver = SyncSQLiteDriver()
    sync_provider = SQLSpecSyncProvider(sync_driver)
    async_driver = AsyncSQLiteDriver()
    async_provider = SQLSpecAsyncProvider(async_driver)
    try:
        proxy_sync = ProxySyncDriver(sync_provider, statement_config=sqlite_cfg)
        try:
            proxy_sync.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            proxy_sync.execute("INSERT INTO items VALUES (?, ?)", 1, "a")
            proxy_sync.execute("INSERT INTO items VALUES (?, ?)", 2, "b")
            proxy_sync.execute("INSERT INTO items VALUES (?, ?)", 3, "c")

            qb_sql, qb_params, _ = proxy_sync.compile_with_broker(
                proxy_sync,
                proxy_sync.broker,
                "sqlite",
                sql.select("id", "name").from_("items").where_eq("id", 2),
                (LimitOffsetFilter(limit=2, offset=1),),
            )
            assert "WHERE" in qb_sql.upper()
            assert "LIMIT" in qb_sql.upper()
            assert qb_params == [2, 2, 1]

            assert proxy_sync.select_value("SELECT ?", "hello") == "hello"
            assert proxy_sync.select_value_or_none("SELECT name FROM items WHERE id = ?", 2) == "b"
            assert proxy_sync.select_value_or_none("SELECT name FROM items WHERE id = ?", 999) is None

            rows, total = proxy_sync.select_with_total(
                "SELECT id, name FROM items ORDER BY id",
                LimitOffsetFilter(limit=2, offset=1),
                count_with_window=count_with_window,
            )
            assert total == 3
            assert [row["id"] for row in rows] == [2, 3]

            ok_stack = (
                StatementStack()
                .push_execute("INSERT INTO items VALUES (?, ?)", 4, "d")
                .push_execute("INSERT INTO items VALUES (?, ?)", 5, "e")
            )
            ok_results = proxy_sync.execute_stack(ok_stack)
            assert len(ok_results) == 2
            assert ok_results[0].result is not None
            assert ok_results[0].result.operation_type == "INSERT"
            assert proxy_sync.select_value("SELECT COUNT(*) FROM items") == 5

            fail_stack = (
                StatementStack()
                .push_execute("INSERT INTO items VALUES (?, ?)", 6, "f")
                .push_execute("INSERT INTO items VALUES (?, ?)", 1, "dup")
            )
            with pytest.raises(StackExecutionError):
                proxy_sync.execute_stack(fail_stack)
            assert proxy_sync.select_value("SELECT COUNT(*) FROM items") == 5

            cont_stack = (
                StatementStack()
                .push_execute("INSERT INTO items VALUES (?, ?)", 1, "dup")
                .push_execute("INSERT INTO items VALUES (?, ?)", 7, "g")
            )
            cont_results = proxy_sync.execute_stack(cont_stack, continue_on_error=True)
            assert len(cont_results) == 2
            assert isinstance(cont_results[0].error, StackExecutionError)
            assert cont_results[1].error is None
            assert proxy_sync.select_value("SELECT COUNT(*) FROM items") == 6
        finally:
            proxy_sync.close()

        proxy_async = ProxyAsyncDriver(async_provider, statement_config=sqlite_cfg)
        try:
            await proxy_async.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
            await proxy_async.execute("INSERT INTO items VALUES (?, ?)", 1, "a")
            await proxy_async.execute("INSERT INTO items VALUES (?, ?)", 2, "b")
            await proxy_async.execute("INSERT INTO items VALUES (?, ?)", 3, "c")

            assert await proxy_async.select_value("SELECT ?", "hello") == "hello"
            assert await proxy_async.select_value_or_none("SELECT name FROM items WHERE id = ?", 2) == "b"
            assert await proxy_async.select_value_or_none("SELECT name FROM items WHERE id = ?", 999) is None

            arows, atotal = await proxy_async.select_with_total(
                "SELECT id, name FROM items ORDER BY id",
                LimitOffsetFilter(limit=2, offset=1),
                count_with_window=count_with_window,
            )
            assert atotal == 3
            assert [row["id"] for row in arows] == [2, 3]

            aok_results = await proxy_async.execute_stack(ok_stack)
            assert len(aok_results) == 2
            assert aok_results[0].result is not None
            assert aok_results[0].result.operation_type == "INSERT"
            assert await proxy_async.select_value("SELECT COUNT(*) FROM items") == 5

            with pytest.raises(StackExecutionError):
                await proxy_async.execute_stack(fail_stack)
            assert await proxy_async.select_value("SELECT COUNT(*) FROM items") == 5

            acont_results = await proxy_async.execute_stack(cont_stack, continue_on_error=True)
            assert len(acont_results) == 2
            assert isinstance(acont_results[0].error, StackExecutionError)
            assert acont_results[1].error is None
            assert await proxy_async.select_value("SELECT COUNT(*) FROM items") == 6
        finally:
            await proxy_async.close()

        class WrappedCfg:
            dialect = "sqlite"
            bind_key = "primary"
            statement_config = sqlite_cfg
            driver_features: dict[str, Any] = {}

            def provide_session(self) -> Any:
                return self

        proxy_sync_cfg = ProxySyncConfig(WrappedCfg(), provider=sync_provider)
        proxy_async_cfg = ProxyAsyncConfig(WrappedCfg(), provider=async_provider)
        with proxy_sync_cfg.provide_proxied_session() as sess:
            assert isinstance(sess, ProxySyncDriver)
            assert sess.select_value("SELECT ?", 11) == 11
        async with proxy_async_cfg.provide_proxied_session() as asess:
            assert isinstance(asess, ProxyAsyncDriver)
            assert await asess.select_value("SELECT ?", 12) == 12
    finally:
        sync_provider.close()
        sync_driver.close()
        await async_provider.close()
        await async_driver.close()


@pytest.mark.anyio
async def test_broker_pool_partitions_timeouts_real_rollback_invalidate_and_alchemy_execution(tmp_path: Path) -> None:
    """Verify partition isolation, pool_size=1 timeouts, real rollback quarantine, recreate/invalidate, and Alchemy SQL."""
    db1 = tmp_path / "one.db"
    db2 = tmp_path / "two.db"
    sync_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    p1 = BrokerPool(broker=sync_t, url=f"sqlite+vtb:///{db1}", pool_size=1, max_overflow=0, acquire_timeout_ms=50)
    p2 = BrokerPool(broker=sync_t, url=f"sqlite+vtb:///{db2}", pool_size=1, max_overflow=0, acquire_timeout_ms=50)
    mem_pool = BrokerPool(broker=sync_t, url="sqlite+vtb:///:memory:", pool_size=1, max_overflow=0)
    try:
        try:
            assert p1.partition_key != p2.partition_key
            assert mem_pool.partition_key is not None
            assert "None" not in mem_pool.partition_key
            assert mem_pool.partition_key.endswith(f"#pool-{id(mem_pool)}")

            c1 = p1.connect()
            c2 = p2.connect()
            try:
                with pytest.raises(TimeoutError):
                    p1.connect()
            finally:
                c2.close()
            try:
                with pytest.raises(TimeoutError):
                    p1.connect()
            finally:
                c1.close()

            recreated = p1.recreate()
            try:
                assert isinstance(recreated, BrokerPool)
                inv_conn = recreated.connect()
                inv_conn.invalidate()
                assert sync_t.metrics().active_leases == 0
            finally:
                recreated.dispose()
        finally:
            mem_pool.dispose()
            p2.dispose()
            p1.dispose()

        creator_sync = broker_creator("sqlite+vtb:///:memory:", broker=sync_t)
        raw_sync = creator_sync()
        try:
            assert raw_sync.broker is sync_t
        finally:
            raw_sync.close()
    finally:
        sync_t.close()

    rollback_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=50))
    rollback_db = tmp_path / "rollback.db"
    should_fail_rollback = True

    class FailingRollbackConnection(SyncDBAPIConnection):
        def rollback(self) -> None:
            if should_fail_rollback:
                raise RuntimeError("real rollback failed")
            super().rollback()

    rollback_pool = BrokerPool(
        creator=lambda: FailingRollbackConnection(driver=SyncSQLiteDriver(), broker=rollback_t, lease_managed=True),
        broker=rollback_t,
        pool_key=f"sqlite://{rollback_db}",
        pool_size=1,
        max_overflow=0,
        acquire_timeout_ms=50,
        reset_on_return="rollback",
    )
    try:
        try:
            conn = rollback_pool.connect()
            lease = conn.info["vtb_broker_lease"]
            poisoned_dbapi = conn.dbapi_connection
            conn.close()
            assert poisoned_dbapi is not None and poisoned_dbapi.closed is True
            assert rollback_t.quarantined_count() == 1
            with pytest.raises(TimeoutError):
                rollback_pool.connect()
            should_fail_rollback = False
            rollback_t.dispose_quarantined(lease.slot_index, lease.generation)
            assert rollback_t.quarantined_count() == 0
            recovered = rollback_pool.connect()
            assert recovered.dbapi_connection is not poisoned_dbapi
            recovered.close()
        finally:
            rollback_pool.dispose()
    finally:
        rollback_t.close()

    async_t = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    try:
        creator_async = broker_creator("sqlite+vtb_async:///:memory:", broker=async_t, is_async=True)
        raw_async = await greenlet_spawn(creator_async)
        try:
            assert raw_async._connection.broker is async_t
        finally:
            await greenlet_spawn(raw_async.close)
    finally:
        await async_t.close()

    alchemy_sync_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    try:
        sync_cfg = create_sync_alchemy_config("sqlite+vtb:///:memory:", broker=alchemy_sync_t)
        sync_engine = sync_cfg.get_engine()
        try:
            with sync_engine.connect() as sa_conn:
                assert sa_conn.exec_driver_sql("SELECT 1").fetchone() == (1,)
                raw_sync_fairy = sa_conn.connection
                assert raw_sync_fairy is not None
                assert raw_sync_fairy.driver_connection is not None
                assert raw_sync_fairy.driver_connection.broker is alchemy_sync_t
            assert alchemy_sync_t.metrics().active_leases == 0
            assert alchemy_sync_t.metrics().total_released == 1
        finally:
            sync_engine.dispose()
    finally:
        alchemy_sync_t.close()

    alchemy_async_t = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    try:
        async_cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:", broker=alchemy_async_t)
        async_engine = async_cfg.get_engine()
        try:
            async with async_engine.connect() as asa_conn:
                res = await asa_conn.exec_driver_sql("SELECT 1")
                assert res.fetchone() == (1,)
                raw_async_fairy = await asa_conn.get_raw_connection()
                assert raw_async_fairy.driver_connection is not None
                assert raw_async_fairy.driver_connection.broker is alchemy_async_t
            assert alchemy_async_t.metrics().active_leases == 0
            assert alchemy_async_t.metrics().total_released == 1
        finally:
            await async_engine.dispose()
    finally:
        await alchemy_async_t.close()


def _assert_no_inspect_signature(source_path: Path) -> None:
    """Assert that a Python source module does not reference inspect or inspect.signature."""
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert alias.name != "inspect"
        elif isinstance(node, ast.ImportFrom):
            assert node.module != "inspect"
        elif (
            isinstance(node, ast.Attribute)
            and node.attr == "signature"
            and isinstance(node.value, ast.Name)
            and node.value.id == "inspect"
        ):
            raise AssertionError(f"Unexpected inspect.signature in {source_path}")


@pytest.mark.parametrize("filename", ["provider.py", "proxy.py"])
def test_sqlspec_proxy_and_provider_have_no_inspect_signature(filename: str) -> None:
    """Verify extensions/sqlspec provider and proxy modules avoid runtime inspect.signature calls."""
    sqlspec_dir = Path(__file__).resolve().parents[3] / "vertebra" / "extensions" / "sqlspec"
    _assert_no_inspect_signature(sqlspec_dir / filename)


def test_broker_pool_shared_contention_dispose_creator_failure_bind_class_and_non_sqlite_url(
    tmp_path: Path,
) -> None:
    """Verify shared file pool contention, active dispose, creator failure quarantine, bind_class, and non-SQLite URL."""
    shared_db = tmp_path / "shared_sync.db"
    shared_url = f"sqlite+vtb:///{shared_db}"
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    e1 = create_engine(
        shared_url,
        pool=BrokerPool(broker=transport, url=shared_url, pool_size=1, max_overflow=0, acquire_timeout_ms=50),
    )
    e2 = create_engine(
        shared_url,
        pool=BrokerPool(broker=transport, url=shared_url, pool_size=1, max_overflow=0, acquire_timeout_ms=50),
    )
    try:
        try:
            with e1.connect() as c1:
                c1.exec_driver_sql("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)")
                with pytest.raises(TimeoutError):
                    e2.connect()
            with e2.connect() as c2:
                assert c2.exec_driver_sql("SELECT 1").fetchone() == (1,)

            with e1.connect() as inv_conn:
                inv_lease = inv_conn.info["vtb_broker_lease"]
                inv_conn.invalidate(RuntimeError("boom"))
            assert transport.quarantined_count() == 1
            transport.dispose_quarantined(inv_lease.slot_index, inv_lease.generation)
            assert transport.quarantined_count() == 0
            e1.dispose()
        finally:
            e1.dispose()
            e2.dispose()
    finally:
        transport.close()

    dispose_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    dispose_pool = BrokerPool(
        broker=dispose_t,
        url=f"sqlite+vtb:///{tmp_path / 'dispose.db'}",
        pool_size=2,
        max_overflow=0,
        acquire_timeout_ms=50,
    )
    try:
        active_conn = dispose_pool.connect()
        idle_conn = dispose_pool.connect()
        idle_dbapi = idle_conn.dbapi_connection
        idle_conn.close()
        assert idle_dbapi is not None and idle_dbapi.closed is False
        dispose_pool.dispose()
        assert idle_dbapi.closed is True
        assert dispose_t.quarantined_count() == 0
        assert dispose_t.metrics().active_leases == 1
        active_dbapi = active_conn.dbapi_connection
        assert active_dbapi is not None and active_dbapi.closed is False
        active_conn.close()
        assert active_dbapi.closed is True
        assert dispose_t.metrics().active_leases == 0
    finally:
        dispose_t.close()

    def _failing_creator() -> Any:
        raise RuntimeError("creator boom")

    fail_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    fail_pool = BrokerPool(
        creator=_failing_creator,
        broker=fail_t,
        pool_key="failing-creator",
        pool_size=1,
        max_overflow=0,
        acquire_timeout_ms=50,
    )
    try:
        try:
            with pytest.raises(RuntimeError, match="creator boom"):
                fail_pool.connect()
            assert fail_t.quarantined_count() == 1
        finally:
            fail_pool.dispose()

        pg_cfg = create_sync_alchemy_config(
            "postgresql+vtb://user:secret@db.internal:5432/orders?shm_broker=true",
            broker=fail_t,
        )
        pg_engine = pg_cfg.get_engine()
        try:
            assert pg_cfg.connection_string == "postgresql+vtb://user:secret@db.internal:5432/orders?shm_broker=true"
            assert cast("Any", SyncPostgreSQLDialect)().create_connect_args(pg_engine.url) == (
                [],
                {"url": "postgresql+vtb://user:secret@db.internal:5432/orders"},
            )
        finally:
            pg_engine.dispose()
    finally:
        fail_t.close()

    with pytest.raises(ImproperConfigurationError):
        BrokerPool.bind_class(broker=None)

    async def _sample_coro() -> int:
        return 1

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        coro = _sample_coro()
        with pytest.raises(ImproperConfigurationError):
            BrokerPool._await_if_needed(coro)
    assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []


@pytest.mark.anyio
async def test_async_broker_pool_shared_contention_dispose_and_real_rollback_failure(
    tmp_path: Path,
) -> None:
    """Verify shared async contention, fake double dispose without quarantine_sync, and async rollback quarantine."""
    async_db = tmp_path / "shared_async.db"
    async_url = f"sqlite+vtb_async:///{async_db}"
    async_t = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    e1 = create_async_engine(
        async_url,
        pool=AsyncBrokerPool(broker=async_t, url=async_url, pool_size=1, max_overflow=0, acquire_timeout_ms=50),
    )
    e2 = create_async_engine(
        async_url,
        pool=AsyncBrokerPool(broker=async_t, url=async_url, pool_size=1, max_overflow=0, acquire_timeout_ms=50),
    )
    try:
        try:
            async with e1.connect() as c1:
                await c1.exec_driver_sql("CREATE TABLE IF NOT EXISTS items (id INTEGER PRIMARY KEY)")
                with pytest.raises(TimeoutError):
                    await e2.connect()
            async with e2.connect() as c2:
                res = await c2.exec_driver_sql("SELECT 2")
                assert res.fetchone() == (2,)
        finally:
            await e1.dispose()
            await e2.dispose()
    finally:
        await async_t.close()

    class MinimalAsyncLease:
        def __init__(self, slot: int, gen: int) -> None:
            self.slot_index = slot
            self.generation = gen

        async def release(self) -> None:
            return None

    class MinimalAsyncBrokerDouble:
        def __init__(self) -> None:
            self.next_slot = 0
            self.quarantined: list[int] = []

        async def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> MinimalAsyncLease:
            _ = (bind_key, worker_id)
            self.next_slot += 1
            return MinimalAsyncLease(self.next_slot, 1)

        async def quarantine(self, lease: MinimalAsyncLease) -> None:
            self.quarantined.append(lease.slot_index)

    fake_broker = MinimalAsyncBrokerDouble()
    assert isinstance(fake_broker, AsyncBrokerLeaseSource)
    fake_pool = AsyncBrokerPool(
        creator=lambda: SyncDBAPIConnection(driver=SyncSQLiteDriver(), share_pool=False),
        broker=fake_broker,
        pool_key="minimal-async-double",
        pool_size=2,
        max_overflow=0,
    )
    try:
        conn_a = await greenlet_spawn(fake_pool.connect)
        conn_b = await greenlet_spawn(fake_pool.connect)
        b_dbapi = conn_b.dbapi_connection
        await greenlet_spawn(conn_b.close)
        assert b_dbapi is not None and b_dbapi.closed is False
        await greenlet_spawn(fake_pool.dispose)
        assert b_dbapi.closed is True
        assert fake_broker.quarantined == []
        a_dbapi = conn_a.dbapi_connection
        assert a_dbapi is not None and a_dbapi.closed is False
        await greenlet_spawn(conn_a.close)
        assert a_dbapi.closed is True
    finally:
        await greenlet_spawn(fake_pool.dispose)

    rollback_t = AsyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=50))
    try:
        should_fail = True

        class FailingAsyncRollbackConnection(AsyncDBAPIConnection):
            async def rollback(self) -> None:
                if should_fail:
                    raise RuntimeError("real async rollback failed")
                await super().rollback()

        async_dbapi = SQLAlchemyAsyncDBAPI(dialect="sqlite")

        def _create_failing_async() -> Any:
            return AsyncAdapt_vertebra_connection(
                async_dbapi,
                cast(
                    "Any",
                    FailingAsyncRollbackConnection(
                        driver=AsyncSQLiteDriver(),
                        broker=rollback_t,
                        lease_managed=True,
                    ),
                ),
            )

        rollback_pool = AsyncBrokerPool(
            creator=_create_failing_async,
            broker=rollback_t,
            pool_key="async-real-rollback",
            pool_size=1,
            max_overflow=0,
            acquire_timeout_ms=50,
            reset_on_return="rollback",
        )
        try:
            bad_conn = await greenlet_spawn(rollback_pool.connect)
            lease = bad_conn.info["vtb_broker_lease"]
            bad_driver_conn = bad_conn.driver_connection._connection
            await greenlet_spawn(bad_conn.close)
            assert bad_driver_conn is not None and bad_driver_conn.closed is True
            assert rollback_t.quarantined_count() == 1
            with pytest.raises(TimeoutError):
                await greenlet_spawn(rollback_pool.connect)
            should_fail = False
            await rollback_t.dispose_quarantined(lease.slot_index, lease.generation)
            ok_conn = await greenlet_spawn(rollback_pool.connect)
            assert ok_conn.driver_connection._connection is not bad_driver_conn
            await greenlet_spawn(ok_conn.close)
        finally:
            await greenlet_spawn(rollback_pool.dispose)
    finally:
        await rollback_t.close()


@pytest.mark.anyio
@pytest.mark.parametrize("return_format", ["table", "batch", "batches", "reader"])
async def test_proxy_drivers_streams_arrow_formats_stacks_nested_tx_and_rpc_type_error(
    return_format: Literal["table", "batch", "batches", "reader"],
) -> None:
    """Verify streams, Arrow formats/native_only, named/mapping params, nested tx, worker quarantine, and _send_rpc."""
    sqlite_cfg = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        ),
    )
    sync_driver = SyncSQLiteDriver()
    sync_provider = SQLSpecSyncProvider(sync_driver, arrow_threshold_rows=1)
    proxy_sync = ProxySyncDriver(sync_provider, statement_config=sqlite_cfg)
    try:
        proxy_sync.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        proxy_sync.execute("INSERT INTO items VALUES (:id, :name)", {"id": 1, "name": "named-1"})
        assert proxy_sync.select_value("SELECT name FROM items WHERE id = :id", {"id": 1}) == "named-1"

        proxy_sync.begin()
        assert proxy_sync._connection_in_transaction() is True
        with proxy_sync.transaction():
            proxy_sync.execute("INSERT INTO items VALUES (?, ?)", 2, "nested-2")
        assert proxy_sync._connection_in_transaction() is True
        proxy_sync.commit()
        assert proxy_sync._connection_in_transaction() is False
        assert proxy_sync.select_value("SELECT COUNT(*) FROM items") == 2

        stack = (
            StatementStack()
            .push_execute_many("INSERT INTO items VALUES (?, ?)", [(3, "many-3"), (4, "many-4")])
            .push_execute_script("INSERT INTO items VALUES (5, 'script-5'); INSERT INTO items VALUES (6, 'script-6');")
            .push_execute_arrow("SELECT id, name FROM items ORDER BY id")
        )
        stack_results = proxy_sync.execute_stack(stack)
        assert len(stack_results) == 3
        assert isinstance(stack_results[2].result, ArrowResult)
        assert stack_results[2].result.num_rows == 6

        with pytest.raises(TypeError):
            proxy_sync.execute_stack(cast("Any", "not-a-stack"))
        with pytest.raises(ValueError):
            proxy_sync.execute_stack(StatementStack())

        with pytest.raises(ValueError, match="execute_many payload cannot be empty"):
            StatementStack().push_execute_many("INSERT INTO items VALUES (?, ?)", [])
        empty_args_op = type(
            "BadOp", (), {"method": "execute_many", "statement": "SELECT 1", "arguments": (), "keyword_arguments": {}}
        )()
        with pytest.raises(ValueError, match="parameter sets"):
            proxy_sync._execute_stack_operation_sync(empty_args_op)

        direct_stream = proxy_sync.dispatch_select_stream(SQL("SELECT id, name FROM items ORDER BY id"), chunk_size=2)
        assert isinstance(direct_stream, SyncRowStream)
        with direct_stream as open_direct_stream:
            assert [row["id"] for row in open_direct_stream] == [1, 2, 3, 4, 5, 6]
        with proxy_sync.select_stream(
            SQL("SELECT id, name FROM items ORDER BY id"), chunk_size=2, native_only=True
        ) as native_stream:
            assert [row["id"] for row in native_stream] == [1, 2, 3, 4, 5, 6]
        with proxy_sync.select_stream(
            SQL("SELECT id, name FROM items ORDER BY id"), chunk_size=2, native_only=False
        ) as open_stream:
            streamed_rows = list(open_stream)
        assert [row["id"] for row in streamed_rows] == [1, 2, 3, 4, 5, 6]

        arrow_res = proxy_sync.select_to_arrow(
            "SELECT id, name FROM items ORDER BY id",
            native_only=True,
            return_format=return_format,
        )
        assert isinstance(arrow_res, ArrowResult)
        assert arrow_res.rows_affected == 6
        arrow_payload = arrow_res.get_data()
        if return_format == "table":
            assert isinstance(arrow_payload, pa.Table)
            assert arrow_payload.num_rows == 6
        elif return_format == "batch":
            assert isinstance(arrow_payload, pa.RecordBatch)
            assert arrow_payload.num_rows == 6
        elif return_format == "batches":
            assert sum(batch.num_rows for batch in arrow_payload) == 6
        else:
            assert isinstance(arrow_payload, pa.RecordBatchReader)
            assert arrow_payload.read_all().num_rows == 6

        with pytest.raises(ValueError, match="Unsupported Arrow return_format"):
            proxy_sync.select_to_arrow("SELECT id FROM items ORDER BY id", return_format=cast("Any", "invalid"))
    finally:
        proxy_sync.close()
        sync_provider.close()
        sync_driver.close()

    non_shm_proxy = ProxySyncDriver(
        lambda req: BrokerRpcResponse(
            request_id=req.request_id,
            ok=True,
            slot_idx=req.slot_idx,
            generation=max(1, req.generation),
            rows=[[1]] if req.op == "select_to_arrow" else [],
            columns=["id"] if req.op == "select_to_arrow" else [],
            is_arrow_shm=False,
        ),
        statement_config=sqlite_cfg,
    )
    try:
        with pytest.raises(SQLSpecImproperConfigurationError, match="native SHM Arrow"):
            non_shm_proxy.select_to_arrow("SELECT id FROM items ORDER BY id", native_only=True)
    finally:
        non_shm_proxy.close()

    call_count = 0

    def _bad_rpc_transport(_req: BrokerRpcRequest) -> int:
        nonlocal call_count
        call_count += 1
        return 42

    bad_proxy = ProxySyncDriver(cast("Any", _bad_rpc_transport))
    try:
        with pytest.raises(TypeError, match="Unexpected RPC transport response type"):
            bad_proxy.acquire_lease()
        assert call_count == 1
    finally:
        bad_proxy.close()

    class WorkerBrokerDouble:
        def __init__(self) -> None:
            self.quarantined = 0

        def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> tuple[int, int]:
            _ = (bind_key, worker_id)
            return (1, 1)

        def quarantine(self, lease: Any) -> None:
            _ = lease
            self.quarantined += 1

    worker_broker = WorkerBrokerDouble()
    err_driver = ProxySyncDriver(
        lambda req: BrokerRpcResponse(
            request_id=req.request_id,
            ok=req.op != "release",
            slot_idx=1,
            generation=1,
            error_type="RuntimeError" if req.op == "release" else "",
            error_message="release boom" if req.op == "release" else "",
        ),
        broker=worker_broker,
    )
    try:
        err_driver.acquire_lease()
        with pytest.raises(LeaseQuarantinedError):
            err_driver.release_lease()
        assert worker_broker.quarantined == 1
    finally:
        err_driver.close()

    async_upstream = AsyncSQLiteDriver()
    async_provider = SQLSpecAsyncProvider(async_upstream, arrow_threshold_rows=1)
    proxy_async = ProxyAsyncDriver(async_provider, statement_config=sqlite_cfg)
    try:
        await proxy_async.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
        await proxy_async.execute("INSERT INTO items VALUES (:id, :name)", {"id": 1, "name": "async-named"})
        await proxy_async.begin()
        assert proxy_async._connection_in_transaction() is True
        async with proxy_async.transaction():
            await proxy_async.execute("INSERT INTO items VALUES (?, ?)", 2, "async-nested")
        assert proxy_async._connection_in_transaction() is True
        await proxy_async.commit()
        assert proxy_async._connection_in_transaction() is False

        direct_astream = proxy_async.dispatch_select_stream(SQL("SELECT id, name FROM items ORDER BY id"), chunk_size=1)
        assert isinstance(direct_astream, AsyncRowStream)
        async with direct_astream as open_direct_astream:
            assert [row["id"] async for row in open_direct_astream] == [1, 2]
        async with proxy_async.select_stream(
            SQL("SELECT id, name FROM items ORDER BY id"), chunk_size=1, native_only=True
        ) as native_astream:
            assert [row["id"] async for row in native_astream] == [1, 2]
        async with proxy_async.select_stream(
            SQL("SELECT id, name FROM items ORDER BY id"), chunk_size=1, native_only=False
        ) as open_astream:
            astreamed = [row async for row in open_astream]
        assert [row["id"] for row in astreamed] == [1, 2]

        async_arrow = await proxy_async.select_to_arrow(
            "SELECT id, name FROM items ORDER BY id",
            native_only=True,
            return_format=return_format,
        )
        assert isinstance(async_arrow, ArrowResult)
        assert async_arrow.rows_affected == 2
        async_payload = async_arrow.get_data()
        if return_format == "table":
            assert isinstance(async_payload, pa.Table)
            assert async_payload.num_rows == 2
        elif return_format == "batch":
            assert isinstance(async_payload, pa.RecordBatch)
            assert async_payload.num_rows == 2
        elif return_format == "batches":
            assert sum(batch.num_rows for batch in async_payload) == 2
        else:
            assert isinstance(async_payload, pa.RecordBatchReader)
            assert async_payload.read_all().num_rows == 2
    finally:
        await proxy_async.close()
        await async_provider.close()
        await async_upstream.close()


@pytest.mark.anyio
async def test_proxy_async_driver_contended_sync_shm_broker_does_not_block_event_loop() -> None:
    """Verify ProxyAsyncDriver offloads contended SyncSHMBrokerTransport and SyncBrokerLeaseSource without blocking."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=2000))
    async_upstream = AsyncSQLiteDriver()
    async_provider = SQLSpecAsyncProvider(async_upstream)
    proxy_async = ProxyAsyncDriver(async_provider, broker=transport, pool_name="shared")
    held_lease: Any = None
    try:
        transport.register_partition("shared", pool_size=1, max_overflow=0)
        held_lease = transport.acquire(worker_id=os.getpid(), bind_key="shared", timeout_ms=500)
        select_task = asyncio.create_task(proxy_async.select_value("SELECT 42"))
        ticks = 0
        while not select_task.done() and (transport.metrics().waiting_threads < 1 or ticks < 3):
            ticks += 1
            await asyncio.sleep(0)
        assert ticks >= 3
        assert transport.metrics().waiting_threads == 1
        held_lease.release()
        held_lease = None
        assert await select_task == 42
        assert transport.metrics().active_leases == 0
        assert async_provider.active_leases == 0
    finally:
        if held_lease is not None:
            held_lease.release()
        await proxy_async.close()
        await async_provider.close()
        await async_upstream.close()
        transport.close()

    started = threading.Event()
    release_gate = threading.Event()

    class _BlockingSyncLeaseSource:
        def __init__(self) -> None:
            self.quarantined = 0

        def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> tuple[int, int]:
            _ = (bind_key, worker_id)
            started.set()
            assert release_gate.wait(timeout=5.0)
            return (0, 1)

        def quarantine(self, lease: Any) -> None:
            _ = lease
            self.quarantined += 1

    blocking_double = _BlockingSyncLeaseSource()
    assert isinstance(blocking_double, SyncBrokerLeaseSource)
    double_upstream = AsyncSQLiteDriver()
    double_provider = SQLSpecAsyncProvider(double_upstream)
    double_proxy = ProxyAsyncDriver(double_provider, broker=blocking_double, pool_name="shared")
    try:
        double_task = asyncio.create_task(double_proxy.select_value("SELECT 7"))
        loop_ticks = 0
        while not double_task.done() and (not started.is_set() or loop_ticks < 3):
            loop_ticks += 1
            await asyncio.sleep(0)
        assert started.is_set()
        assert loop_ticks >= 3
        release_gate.set()
        assert await double_task == 7
    finally:
        release_gate.set()
        await double_proxy.close()
        await double_provider.close()
        await double_upstream.close()


@pytest.mark.anyio
async def test_proxy_and_provider_post_failed_commit_transaction_release_and_rollback() -> None:
    """Verify failed commit inside sync/async transaction() releases active provider leases cleanly."""
    sqlite_cfg = StatementConfig(
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK, ParameterStyle.NAMED_COLON},
        )
    )

    sync_upstream = SyncSQLiteDriver()
    sync_provider = SQLSpecSyncProvider(sync_upstream)

    def sync_failing_commit(req: BrokerRpcRequest) -> BrokerRpcResponse:
        if req.op == "commit":
            return BrokerRpcResponse(
                request_id=req.request_id,
                ok=False,
                slot_idx=req.slot_idx,
                generation=req.generation,
                error_type="RuntimeError",
                error_message="simulated sync commit failure",
            )
        return sync_provider.handle_request(req)

    sync_proxy = ProxySyncDriver(sync_failing_commit, statement_config=sqlite_cfg)
    try:
        sync_proxy.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        with pytest.raises(SQLSpecError, match="simulated sync commit failure"), sync_proxy.transaction():
            sync_proxy.execute("INSERT INTO items VALUES (1)")
        assert sync_provider.active_leases == 0
        assert sync_proxy.is_leased is False
        assert sync_proxy._connection_in_transaction() is False
        assert sync_proxy.select_value("SELECT COUNT(*) FROM items") == 0
    finally:
        sync_proxy.close()
        sync_provider.close()
        sync_upstream.close()

    async_upstream = AsyncSQLiteDriver()
    async_provider = SQLSpecAsyncProvider(async_upstream)

    async def async_failing_commit(req: BrokerRpcRequest) -> BrokerRpcResponse:
        if req.op == "commit":
            return BrokerRpcResponse(
                request_id=req.request_id,
                ok=False,
                slot_idx=req.slot_idx,
                generation=req.generation,
                error_type="RuntimeError",
                error_message="simulated async commit failure",
            )
        return await async_provider.handle_request(req)

    async_proxy = ProxyAsyncDriver(async_failing_commit, statement_config=sqlite_cfg)
    try:
        await async_proxy.execute("CREATE TABLE items (id INTEGER PRIMARY KEY)")
        with pytest.raises(SQLSpecError, match="simulated async commit failure"):
            async with async_proxy.transaction():
                await async_proxy.execute("INSERT INTO items VALUES (1)")
        assert async_provider.active_leases == 0
        assert async_proxy.is_leased is False
        assert async_proxy._connection_in_transaction() is False
        assert await async_proxy.select_value("SELECT COUNT(*) FROM items") == 0
    finally:
        await async_proxy.close()
        await async_provider.close()
        await async_upstream.close()


@pytest.mark.anyio
async def test_cancelled_error_propagates_unwrapped_from_release_lease_and_provider() -> None:
    """Verify asyncio.CancelledError propagates unwrapped while still quarantining broker leases."""

    class _CancelBrokerDouble:
        def __init__(self) -> None:
            self.quarantined = 0

        def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> tuple[int, int]:
            _ = (bind_key, worker_id)
            return (0, 1)

        def quarantine(self, lease: Any) -> None:
            _ = lease
            self.quarantined += 1

    async def _cancel_on_release_transport(req: BrokerRpcRequest) -> BrokerRpcResponse:
        if req.op == "release":
            raise asyncio.CancelledError("loop cancelled")
        return BrokerRpcResponse(request_id=req.request_id, ok=True, slot_idx=0, generation=1)

    proxy_broker = _CancelBrokerDouble()
    proxy_driver = ProxyAsyncDriver(_cancel_on_release_transport, broker=proxy_broker)
    try:
        await proxy_driver.acquire_lease()
        with pytest.raises(asyncio.CancelledError):
            await proxy_driver.release_lease()
        assert proxy_broker.quarantined == 1
    finally:
        await proxy_driver.close()

    class _CancellingRollbackUpstream:
        async def begin(self) -> None:
            return None

        async def rollback(self) -> None:
            raise asyncio.CancelledError("upstream rollback cancelled")

        async def close(self) -> None:
            return None

    provider_broker = _CancelBrokerDouble()
    upstream = _CancellingRollbackUpstream()
    provider = SQLSpecAsyncProvider(upstream, broker=provider_broker)
    try:
        acq_resp = await provider.handle_request(
            BrokerRpcRequest(request_id=1, op="acquire", pool_name="default", worker_pid=os.getpid())
        )
        assert acq_resp.ok is True
        begin_resp = await provider.handle_request(
            BrokerRpcRequest(
                request_id=2,
                op="begin",
                pool_name="default",
                worker_pid=os.getpid(),
                slot_idx=acq_resp.slot_idx,
                generation=acq_resp.generation,
            )
        )
        assert begin_resp.ok is True
        with pytest.raises(asyncio.CancelledError):
            await provider.handle_request(
                BrokerRpcRequest(
                    request_id=3,
                    op="release",
                    pool_name="default",
                    worker_pid=os.getpid(),
                    slot_idx=acq_resp.slot_idx,
                    generation=acq_resp.generation,
                    in_tx=True,
                )
            )
        assert provider_broker.quarantined == 1
        assert provider.active_leases == 0
    finally:
        await provider.close()


def test_sync_shm_broker_across_pool_proxy_and_async_transport_rejects_sync() -> None:
    """Verify SyncSHMBrokerTransport across pool/proxy/driver and InterfaceError on sync AsyncSHMBrokerTransport use."""
    sync_transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=100))
    async_transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=100))
    pool: BrokerPool | None = None
    async_pool: BrokerPool | None = None
    sqlite_driver = SyncSQLiteDriver(broker=sync_transport)
    sync_provider: SQLSpecSyncProvider | None = None
    proxy_sync: ProxySyncDriver | None = None
    try:
        lease = sync_transport.acquire(bind_key="direct", worker_id=os.getpid())
        try:
            assert lease.lease_id
            assert sync_transport.metrics().active_leases == 1
        finally:
            lease.release()
        assert sync_transport.metrics().active_leases == 0

        pool = BrokerPool(broker=sync_transport, url="sqlite+vtb:///%3Amemory%3A", pool_size=1, max_overflow=0)
        assert pool.partition_key is not None
        assert pool.partition_key.endswith(f"#pool-{id(pool)}")
        conn = pool.connect()
        try:
            assert sync_transport.metrics().active_leases == 1
        finally:
            conn.close()
        assert sync_transport.metrics().active_leases == 0

        rows, _cols = sqlite_driver.execute_with_description("SELECT 99")
        assert rows == [(99,)]
        assert sync_transport.metrics().active_leases == 0

        sync_provider = SQLSpecSyncProvider(sqlite_driver, broker=sync_transport)
        proxy_sync = ProxySyncDriver(sync_provider, broker=sync_transport, pool_name="sync_via_shm")
        assert proxy_sync.select_value("SELECT 55") == 55
        assert sync_transport.metrics().active_leases == 0

        async_pool = BrokerPool(broker=async_transport, url="sqlite+vtb:///%3Amemory%3A", pool_size=1, max_overflow=0)
        with pytest.raises(DriverInterfaceError):
            async_pool.connect()
        async_sqlite = SyncSQLiteDriver(broker=async_transport)
        try:
            with pytest.raises(DriverInterfaceError):
                async_sqlite._acquire_broker_lease()
        finally:
            async_sqlite.close()

        async_proxy = ProxySyncDriver(sync_provider, broker=async_transport, pool_name="async_rejected")
        try:
            with pytest.raises(DriverInterfaceError):
                async_proxy._acquire_broker_lease()
        finally:
            async_proxy.close()

        async def _exercise_async_quarantine() -> None:
            q_lease = await async_transport.acquire(bind_key="qasync")
            await async_transport.quarantine(q_lease)
            assert async_transport.quarantined_count() == 1
            await async_transport.dispose_quarantined(q_lease)
            assert async_transport.quarantined_count() == 0

        asyncio.run(_exercise_async_quarantine())
    finally:
        if proxy_sync is not None:
            proxy_sync.close()
        if sync_provider is not None:
            sync_provider.close()
        sqlite_driver.close()
        if pool is not None:
            pool.dispose()
        if async_pool is not None:
            async_pool.dispose()
        sync_transport.close()
        asyncio.run(async_transport.close())


@pytest.mark.anyio
async def test_sqlspec_provider_idle_commit_cancelled_quarantine_and_stack_cancel() -> None:
    """Verify idle commit fails clearly, quarantine cancellation unwraps, and execute_stack rolls back on BaseException."""
    sync_provider = SQLSpecSyncProvider(SyncSQLiteDriver())
    try:
        sync_commit = sync_provider.handle_request(
            BrokerRpcRequest(request_id=1, op="commit", pool_name="default", worker_pid=os.getpid())
        )
        assert sync_commit.ok is False
        assert "Cannot commit without an active leased session" in sync_commit.error_message
    finally:
        sync_provider.close()

    class _CancelQuarantineBroker:
        def acquire(self, *, bind_key: str | None = "default", worker_id: int | None = None) -> tuple[int, int]:
            _ = (bind_key, worker_id)
            return (0, 1)

        def quarantine(self, lease: Any) -> None:
            _ = lease
            raise asyncio.CancelledError("quarantine cancelled")

    class _BoomRollbackUpstream:
        async def begin(self) -> None:
            return None

        async def rollback(self) -> None:
            raise ValueError("rollback exploded")

        async def close(self) -> None:
            return None

    async_provider = SQLSpecAsyncProvider(_BoomRollbackUpstream(), broker=_CancelQuarantineBroker())
    try:
        idle_resp = await async_provider.handle_request(
            BrokerRpcRequest(request_id=2, op="commit", pool_name="default", worker_pid=os.getpid())
        )
        assert idle_resp.ok is False
        assert "Cannot commit without an active leased session" in idle_resp.error_message

        acq = await async_provider.handle_request(
            BrokerRpcRequest(request_id=3, op="acquire", pool_name="default", worker_pid=os.getpid())
        )
        assert acq.ok is True
        beg = await async_provider.handle_request(
            BrokerRpcRequest(
                request_id=4,
                op="begin",
                pool_name="default",
                worker_pid=os.getpid(),
                slot_idx=acq.slot_idx,
                generation=acq.generation,
            )
        )
        assert beg.ok is True
        with pytest.raises(asyncio.CancelledError):
            await async_provider.handle_request(
                BrokerRpcRequest(
                    request_id=5,
                    op="release",
                    pool_name="default",
                    worker_pid=os.getpid(),
                    slot_idx=acq.slot_idx,
                    generation=acq.generation,
                    in_tx=True,
                )
            )
    finally:
        await async_provider.close()

    sync_ops: list[str] = []

    def _sync_cancel_transport(req: BrokerRpcRequest) -> BrokerRpcResponse:
        sync_ops.append(req.op)
        if req.op == "select":
            raise KeyboardInterrupt("interrupted mid-stack")
        return BrokerRpcResponse(request_id=req.request_id, ok=True, slot_idx=0, generation=1)

    stack_proxy = ProxySyncDriver(_sync_cancel_transport)
    try:
        with pytest.raises(KeyboardInterrupt):
            stack_proxy.execute_stack(
                StatementStack().push_execute("SELECT 1"),
                continue_on_error=True,
            )
        assert sync_ops == ["acquire", "begin", "select", "rollback", "release"]
        assert stack_proxy.is_leased is False
        assert stack_proxy._in_tx is False
    finally:
        stack_proxy.close()

    async_ops: list[str] = []

    async def _async_cancel_transport(req: BrokerRpcRequest) -> BrokerRpcResponse:
        async_ops.append(req.op)
        if req.op == "select":
            raise asyncio.CancelledError("cancelled mid-stack")
        return BrokerRpcResponse(request_id=req.request_id, ok=True, slot_idx=0, generation=1)

    async_stack_proxy = ProxyAsyncDriver(_async_cancel_transport)
    try:
        with pytest.raises(asyncio.CancelledError):
            await async_stack_proxy.execute_stack(
                StatementStack().push_execute("SELECT 1"),
                continue_on_error=True,
            )
        assert async_ops == ["acquire", "begin", "select", "rollback", "release"]
        assert async_stack_proxy.is_leased is False
        assert async_stack_proxy._in_tx is False
    finally:
        await async_stack_proxy.close()


@pytest.mark.parametrize(
    ("is_async", "expected_default_pool", "expected_broker_base"),
    [
        (False, QueuePool, BrokerPool),
        (True, AsyncAdaptedQueuePool, AsyncBrokerPool),
    ],
)
def test_get_pool_class_broker_query_param(
    monkeypatch: pytest.MonkeyPatch,
    is_async: bool,
    expected_default_pool: type[Any],
    expected_broker_base: type[BrokerPool],
) -> None:
    """Verify ?broker=true resolves DatabaseRegistry.default_broker() or raises ImproperConfigurationError."""
    dialect_cls: Any = AsyncPostgreSQLDialect if is_async else SyncPostgreSQLDialect
    scheme = "postgresql+vtb_async" if is_async else "postgresql+vtb"
    assert dialect_cls.get_pool_class(make_url(f"{scheme}://localhost/app")) is expected_default_pool
    assert dialect_cls.get_pool_class(make_url(f"{scheme}://localhost/app?shm_broker=true")) is NullPool

    fake_broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    try:
        monkeypatch.setattr(DatabaseRegistry, "default_broker", classmethod(lambda cls: fake_broker))
        bound_cls = dialect_cls.get_pool_class(make_url(f"{scheme}://localhost/app?broker=true"))
        assert issubclass(bound_cls, expected_broker_base)
        assert bound_cls._bound_broker is fake_broker

        monkeypatch.setattr(DatabaseRegistry, "default_broker", classmethod(lambda cls: None))
        with pytest.raises(ImproperConfigurationError, match="default broker"):
            dialect_cls.get_pool_class(make_url(f"{scheme}://localhost/app?broker=true"))
    finally:
        fake_broker.close()


def test_broker_pool_reuses_connections_across_checkins(tmp_path: Path) -> None:
    """Verify BrokerPool keeps checked-in DBAPI connections open and reuses them across checkouts."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=4, max_overflow=0, acquire_timeout_ms=50))
    db_path = tmp_path / "reuse.db"
    url = f"sqlite+vtb:///{db_path}"
    created_conns: list[SyncDBAPIConnection] = []

    def _creator() -> SyncDBAPIConnection:
        conn = SyncDBAPIConnection(url, broker=transport, share_pool=False, lease_managed=True)
        created_conns.append(conn)
        return conn

    pool = BrokerPool(creator=_creator, broker=transport, url=url, pool_size=2, max_overflow=0)
    try:
        for idx in range(4):
            conn = pool.connect()
            try:
                cur = conn.cursor()
                cur.execute("SELECT ?", (idx,))
                assert cur.fetchone() == (idx,)
            finally:
                conn.close()
            assert created_conns[0].closed is False
        assert len(created_conns) == 1

        engine = create_engine(
            url,
            poolclass=BrokerPool.bind_class(broker=transport, url=url),
            pool_size=2,
            max_overflow=0,
        )
        try:
            assert isinstance(engine.pool, BrokerPool)
            assert engine.pool._pool_size == 2
            assert engine.pool._max_overflow == 0
        finally:
            engine.dispose()

        with pytest.raises(ImproperConfigurationError):
            BrokerPool(broker=transport, url=url, pool_size=-1)
        with pytest.raises(ImproperConfigurationError):
            BrokerPool(broker=transport, url=url, max_overflow=-2)
    finally:
        pool.dispose()
        transport.close()


def test_broker_pool_single_reset_on_return_and_quarantine(tmp_path: Path) -> None:
    """Verify Pool delegates reset_on_return once per checkin and quarantines poisoned slots on failure."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=50))
    url = f"sqlite+vtb:///{tmp_path / 'reset_once.db'}"
    rollback_calls = 0
    fail_next_rollback = False
    created: list[SyncDBAPIConnection] = []

    class _CountingConnection(SyncDBAPIConnection):
        def rollback(self) -> None:
            nonlocal rollback_calls
            rollback_calls += 1
            if fail_next_rollback:
                raise RuntimeError("reset failure")
            super().rollback()

    def _creator() -> _CountingConnection:
        conn = _CountingConnection(driver=SyncSQLiteDriver(), broker=transport, lease_managed=True)
        created.append(conn)
        return conn

    pool = BrokerPool(
        creator=_creator,
        broker=transport,
        url=url,
        pool_size=1,
        max_overflow=0,
        acquire_timeout_ms=50,
        reset_on_return="rollback",
    )
    try:
        c1 = pool.connect()
        c1.close()
        assert rollback_calls == 1
        assert created[0].closed is False

        c2 = pool.connect()
        lease = c2.info["vtb_broker_lease"]
        fail_next_rollback = True
        c2.close()
        assert rollback_calls == 2
        assert created[0].closed is True
        assert transport.quarantined_count() == 1

        fail_next_rollback = False
        transport.dispose_quarantined(lease.slot_index, lease.generation)
        c3 = pool.connect()
        assert len(created) == 2
        assert c3.driver_connection is created[1]
        c3.close()
    finally:
        pool.dispose()
        transport.close()


def test_broker_pool_dispose_keeps_checked_out_and_closes_idle(tmp_path: Path) -> None:
    """Verify pool.dispose() closes idle connections immediately while checked-out connections finish cleanly."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=4, max_overflow=0, acquire_timeout_ms=50))
    url = f"sqlite+vtb:///{tmp_path / 'dispose_idle.db'}"
    pool = BrokerPool(broker=transport, url=url, pool_size=2, max_overflow=0)
    try:
        conn_a = pool.connect()
        conn_b = pool.connect()
        raw_a = conn_a.dbapi_connection
        raw_b = conn_b.dbapi_connection
        assert raw_a is not None and raw_b is not None

        conn_b.close()
        assert raw_b.closed is False

        pool.dispose()
        assert raw_b.closed is True
        assert raw_a.closed is False
        assert transport.metrics().active_leases == 1

        cur_a = conn_a.cursor()
        cur_a.execute("SELECT 99")
        assert cur_a.fetchone() == (99,)
        cur_a.close()

        conn_a.close()
        assert raw_a.closed is True
        assert transport.metrics().active_leases == 0
    finally:
        pool.dispose()
        transport.close()


@pytest.mark.anyio
async def test_async_broker_pool_in_greenlet_and_unawaited_guard(tmp_path: Path) -> None:
    """Verify AsyncBrokerPool reuses connections in greenlets and emits zero RuntimeWarnings outside greenlet."""
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    url = f"sqlite+vtb_async:///{tmp_path / 'async_guard.db'}"
    created: list[Any] = []
    async_dbapi = SQLAlchemyAsyncDBAPI(dialect="sqlite")

    def _creator() -> AsyncAdapt_vertebra_connection:
        conn = AsyncDBAPIConnection(driver=AsyncSQLiteDriver(), broker=transport, lease_managed=True)
        adapted = AsyncAdapt_vertebra_connection(async_dbapi, cast("Any", conn))
        created.append(conn)
        return adapted

    pool = AsyncBrokerPool(
        creator=_creator,
        broker=transport,
        url=url,
        pool_size=2,
        max_overflow=0,
        acquire_timeout_ms=50,
    )
    try:
        c1 = await greenlet_spawn(pool.connect)
        await greenlet_spawn(c1.close)
        assert created[0].closed is False

        c2 = await greenlet_spawn(pool.connect)
        assert len(created) == 1
        await greenlet_spawn(c2.close)

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", RuntimeWarning)
            with pytest.raises((ImproperConfigurationError, DriverInterfaceError)):
                pool.connect()
            with pytest.raises((ImproperConfigurationError, DriverInterfaceError)):
                pool._release_lease(type("_AsyncLease", (), {"release": _creator})())
            pool.dispose()
        assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []
        assert created[0].closed is True
    finally:
        await greenlet_spawn(pool.dispose)
        await transport.close()
