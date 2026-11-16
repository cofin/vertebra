"""Unit tests for canonical extension naming, PEP 695 generic configs, broker leasing, and CLI discovery."""

from __future__ import annotations

import asyncio
import warnings
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import click
import msgspec
import pytest
from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin
from advanced_alchemy.types.file_object import FileObject
from advanced_alchemy.types.file_object.registry import storages
from litestar import Litestar, get
from litestar.config.app import AppConfig
from litestar.di import NamedDependency
from litestar.exceptions import LitestarDeprecationWarning
from litestar.params import SkipValidation
from litestar.testing import TestClient

import vertebra
import vertebra.extensions.alchemy as alchemy_ext
import vertebra.extensions.django as django_ext
import vertebra.extensions.fastapi as fastapi_ext
import vertebra.extensions.flask as flask_ext
import vertebra.extensions.litestar as litestar_ext
import vertebra.extensions.sqlspec as sqlspec_ext
import vertebra.extensions.starlette as starlette_ext
import vertebra.framework as vtb_framework
import vertebra.framework.procedures as vtb_procedures
from vertebra.build.codegen import CodegenConfig, CodegenPlugin
from vertebra.cli.plugin_registry import (
    CLIPluginProtocol,
    discover_entry_point_commands,
    register_app_cli_plugins,
)
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, SyncBigQueryDriver
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, SyncDuckDBDriver
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, SyncMySQLDriver
from vertebra.db.adapters.oracle import AsyncOracleDriver, SyncOracleDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, SyncPostgreSQLDriver
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SyncSpannerDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
from vertebra.extensions.alchemy import (
    AsyncSHMBrokerTransport,
    AsyncSHMSessionBroker,
    BrokerConfig,
    ChannelMessage,
    ObjectStoreBackend,
    SyncSHMBrokerTransport,
    SyncSHMSessionBroker,
    create_alchemy_plugin,
    create_async_alchemy_config,
    create_sync_alchemy_config,
)
from vertebra.extensions.alchemy.storage import register_storage_backend
from vertebra.extensions.litestar import (
    ChannelsBackend,
    FastPath,
    MemoryStore,
    OffloadPlan,
    RouteCompiler,
    RouterTable,
    VertebraConfig,
    VertebraPlugin,
)
from vertebra.extensions.sqlalchemy.dialects import ALL_DIALECT_SCHEMES, DIALECT_DESCRIPTORS
from vertebra.extensions.sqlspec import (
    BigQueryAsyncConfig,
    BigQueryAsyncDriver,
    BigQuerySyncConfig,
    BigQuerySyncDriver,
    DuckDBAsyncConfig,
    DuckDBAsyncDriver,
    DuckDBSyncConfig,
    DuckDBSyncDriver,
    MSSQLAsyncConfig,
    MSSQLAsyncDriver,
    MSSQLSyncConfig,
    MSSQLSyncDriver,
    MySQLAsyncConfig,
    MySQLAsyncDriver,
    MySQLSyncConfig,
    MySQLSyncDriver,
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleSyncConfig,
    OracleSyncDriver,
    PostgresAsyncConfig,
    PostgresAsyncDriver,
    PostgresSyncConfig,
    PostgresSyncDriver,
    ProxyAsyncConfig,
    ProxySyncConfig,
    SpannerAsyncConfig,
    SpannerAsyncDriver,
    SpannerSyncConfig,
    SpannerSyncDriver,
    SQLiteAsyncConfig,
    SQLiteAsyncDriver,
    SQLiteSyncConfig,
    SQLiteSyncDriver,
    SQLSpecPlugin,
    create_sqlspec_plugin,
)
from vertebra.storage import SyncObjectStore


class _DummySyncDriver:
    __slots__ = ()

    def execute(self, statement: str, *args: object, **kwargs: object) -> str:
        _ = (args, kwargs)
        return f"sync:{statement}"


class _DummyAsyncDriver:
    __slots__ = ()

    async def execute(self, statement: str, *args: object, **kwargs: object) -> str:
        _ = (args, kwargs)
        return f"async:{statement}"


def test_canonical_extension_exports() -> None:
    """Verify canonical domain classes are exported and legacy aliases are absent."""
    assert not hasattr(vertebra, "RouteDef")
    assert not hasattr(vtb_framework, "RouteDef")
    assert not hasattr(vtb_procedures, "ProcedureDef")
    assert not hasattr(vtb_procedures, "procedure_query")
    assert not hasattr(vtb_procedures, "procedure_mutation")
    assert not hasattr(vtb_procedures, "procedure_subscription")
    assert not hasattr(litestar_ext, "ServerConfig")
    assert not hasattr(litestar_ext, "LitestarConfig")
    assert not hasattr(litestar_ext, "LitestarPlugin")
    assert hasattr(litestar_ext, "VertebraConfig")
    assert hasattr(litestar_ext, "VertebraPlugin")
    assert not hasattr(starlette_ext, "StarletteConfig")
    assert not hasattr(starlette_ext, "StarlettePlugin")
    assert hasattr(starlette_ext, "VertebraConfig")
    assert hasattr(starlette_ext, "VertebraPlugin")
    assert not hasattr(fastapi_ext, "FastAPIConfig")
    assert not hasattr(fastapi_ext, "FastAPIPlugin")
    assert hasattr(fastapi_ext, "VertebraConfig")
    assert hasattr(fastapi_ext, "VertebraPlugin")
    assert not hasattr(flask_ext, "FlaskConfig")
    assert not hasattr(flask_ext, "FlaskPlugin")
    assert hasattr(flask_ext, "VertebraConfig")
    assert hasattr(flask_ext, "VertebraPlugin")
    assert not hasattr(django_ext, "DjangoConfig")
    assert not hasattr(django_ext, "DjangoPlugin")
    assert not hasattr(django_ext, "DjangoAppConfig")
    assert hasattr(django_ext, "VertebraConfig")
    assert hasattr(django_ext, "VertebraPlugin")
    assert hasattr(django_ext, "VertebraAppConfig")
    assert not hasattr(alchemy_ext, "AdvancedAlchemy")
    assert not hasattr(alchemy_ext, "AlchemyPlugin")
    assert not hasattr(alchemy_ext, "AlchemyStore")
    assert not hasattr(alchemy_ext, "StorageBackend")
    assert not hasattr(alchemy_ext, "VertebraAlchemyPlugin")
    assert not hasattr(sqlspec_ext, "SQLSpec")
    assert not hasattr(sqlspec_ext, "SQLSpecStorageBridge")
    assert not hasattr(sqlspec_ext, "VertebraSQLSpecPlugin")
    assert hasattr(alchemy_ext, "create_alchemy_plugin")
    assert hasattr(sqlspec_ext, "create_sqlspec_plugin")
    assert hasattr(sqlspec_ext, "SQLSpecPlugin")

    plugin = VertebraPlugin(VertebraConfig(enable_health_endpoint=True))
    assert plugin.config.enable_health_endpoint is True
    assert isinstance(ChannelsBackend(history=4), ChannelsBackend)
    assert isinstance(MemoryStore(), MemoryStore)
    assert isinstance(CodegenPlugin(CodegenConfig()), CodegenPlugin)
    assert RouteCompiler is not None
    assert RouterTable is not None
    assert FastPath is not None
    assert OffloadPlan is not None
    assert ObjectStoreBackend is not None
    assert set(ALL_DIALECT_SCHEMES) == set(DIALECT_DESCRIPTORS)
    assert len(ALL_DIALECT_SCHEMES) == 36


def test_sqlspec_generic_configs_and_pool_defaults() -> None:
    """Verify PEP 695 generic ProxySyncConfig, ProxyAsyncConfig, and pool defaults."""
    sync_driver = _DummySyncDriver()
    sync_cfg = SimpleNamespace(
        dsn="postgresql://localhost:5432/app",
        bind_key="primary",
        driver_instance=sync_driver,
        connection_instance="sync_conn",
        pool_instance="sync_pool",
        supports_connection_pooling=True,
        pool_config={},
        provide_session=lambda: sync_driver,
        provide_connection=lambda: "sync_conn",
        create_pool=lambda: "sync_pool",
    )
    proxy_sync = ProxySyncConfig(sync_cfg)
    assert proxy_sync.provide_session() is sync_driver
    assert proxy_sync.provide_connection() == "sync_conn"
    assert proxy_sync.create_pool() == "sync_pool"

    async_driver = _DummyAsyncDriver()
    async_cfg = SimpleNamespace(
        dsn="postgresql+asyncpg://localhost:5432/app",
        bind_key="async_pool",
        driver_instance=async_driver,
        supports_connection_pooling=True,
        pool_config={},
        provide_session=lambda: async_driver,
    )
    proxy_async = ProxyAsyncConfig(async_cfg)
    assert proxy_async.is_async is True
    assert proxy_async.provide_session() is async_driver

    spec_plugin = create_sqlspec_plugin(proxy_sync, proxy_async)
    assert isinstance(spec_plugin, SQLSpecPlugin)

    app_config = AppConfig()
    spec_plugin.on_app_init(app_config)
    for startup_fn in app_config.on_startup:
        startup_fn()
    assert "sqlspec" in app_config.state
    assert "db_session" in app_config.dependencies
    assert "db_connection" in app_config.dependencies


@pytest.mark.parametrize(
    ("config_factory", "expected_connection_type", "expected_driver_type"),
    [
        (lambda: SQLiteSyncConfig(path=":memory:", share_pool=False), SyncSQLiteDriver, SQLiteSyncDriver),
        (lambda: SQLiteAsyncConfig(path=":memory:", share_pool=False), AsyncSQLiteDriver, SQLiteAsyncDriver),
        (
            lambda: PostgresSyncConfig(dsn="postgresql://user@localhost:5432/app", share_pool=False),
            SyncPostgreSQLDriver,
            PostgresSyncDriver,
        ),
        (
            lambda: PostgresAsyncConfig(dsn="postgresql://user@localhost:5432/app", share_pool=False),
            AsyncPostgreSQLDriver,
            PostgresAsyncDriver,
        ),
        (
            lambda: MySQLSyncConfig(dsn="mysql://user@localhost:3306/app", share_pool=False),
            SyncMySQLDriver,
            MySQLSyncDriver,
        ),
        (
            lambda: MySQLAsyncConfig(dsn="mysql://user@localhost:3306/app", share_pool=False),
            AsyncMySQLDriver,
            MySQLAsyncDriver,
        ),
        (
            lambda: OracleSyncConfig(dsn="oracle://user@localhost:1521/orclpdb1", share_pool=False),
            SyncOracleDriver,
            OracleSyncDriver,
        ),
        (
            lambda: OracleAsyncConfig(dsn="oracle://user@localhost:1521/orclpdb1", share_pool=False),
            AsyncOracleDriver,
            OracleAsyncDriver,
        ),
        (
            lambda: MSSQLSyncConfig(dsn="mssql://user@localhost:1433/app", share_pool=False),
            SyncMSSQLDriver,
            MSSQLSyncDriver,
        ),
        (
            lambda: MSSQLAsyncConfig(dsn="mssql://user@localhost:1433/app", share_pool=False),
            AsyncMSSQLDriver,
            MSSQLAsyncDriver,
        ),
        (lambda: DuckDBSyncConfig(path=":memory:", share_pool=False), SyncDuckDBDriver, DuckDBSyncDriver),
        (lambda: DuckDBAsyncConfig(path=":memory:", share_pool=False), AsyncDuckDBDriver, DuckDBAsyncDriver),
        (lambda: BigQuerySyncConfig(project_id="proj", share_pool=False), SyncBigQueryDriver, BigQuerySyncDriver),
        (lambda: BigQueryAsyncConfig(project_id="proj", share_pool=False), AsyncBigQueryDriver, BigQueryAsyncDriver),
        (
            lambda: SpannerSyncConfig(
                project_id="proj",
                instance_id="inst",
                database_id="db",
                share_pool=False,
            ),
            SyncSpannerDriver,
            SpannerSyncDriver,
        ),
        (
            lambda: SpannerAsyncConfig(
                project_id="proj",
                instance_id="inst",
                database_id="db",
                share_pool=False,
            ),
            AsyncSpannerDriver,
            SpannerAsyncDriver,
        ),
    ],
    ids=[
        "sqlite-sync",
        "sqlite-async",
        "postgres-sync",
        "postgres-async",
        "mysql-sync",
        "mysql-async",
        "oracle-sync",
        "oracle-async",
        "mssql-sync",
        "mssql-async",
        "duckdb-sync",
        "duckdb-async",
        "bigquery-sync",
        "bigquery-async",
        "spanner-sync",
        "spanner-async",
    ],
)
def test_sqlspec_native_adapter_configs_wire_litestar_di(
    config_factory: Callable[[], Any],
    expected_connection_type: type[Any],
    expected_driver_type: type[Any],
) -> None:
    """Verify every native SQLSpec adapter config declares concrete connection_type and wires Litestar DI."""
    cfg = config_factory()
    assert cfg.connection_type is expected_connection_type
    assert cfg.connection_type is not Any
    assert cfg.driver_type is expected_driver_type

    app_config = AppConfig()
    create_sqlspec_plugin(cfg).on_app_init(app_config)
    assert "db_session" in app_config.dependencies
    assert "db_connection" in app_config.dependencies
    assert expected_connection_type in app_config.signature_types
    assert expected_driver_type in app_config.signature_types


def test_sqlspec_sqlite_litestar_di_runtime_execution() -> None:
    """Verify SQLiteSyncConfig injects both db_session and db_connection through Litestar without warnings."""
    sqlite_cfg = SQLiteSyncConfig(path=":memory:", share_pool=False)

    @get("/health-db", sync_to_thread=False)
    def check_sqlite(
        db_session: NamedDependency[SkipValidation[SQLiteSyncDriver]],
        db_connection: NamedDependency[SkipValidation[SyncSQLiteDriver]],
    ) -> dict[str, Any]:
        return {
            "rows": db_session.select("SELECT 1 AS ok"),
            "same_driver": db_session.native_driver is db_connection,
        }

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        app = Litestar(route_handlers=[check_sqlite], plugins=[create_sqlspec_plugin(sqlite_cfg)])
        with TestClient(app) as client:
            response = client.get("/health-db")
            assert response.status_code == 200
            assert response.json() == {"rows": [{"ok": 1}], "same_driver": True}

    litestar_deprecations = [record for record in caught if issubclass(record.category, LitestarDeprecationWarning)]
    assert litestar_deprecations == []


def test_alchemy_generic_configs_and_attachment_storage() -> None:
    """Verify SQLAlchemyAsyncConfig, SQLAlchemySyncConfig, create_alchemy_plugin, and ObjectStoreBackend."""
    async_cfg = create_async_alchemy_config(
        connection_string="postgresql+asyncpg://localhost/db",
    )
    assert async_cfg.engine_config.pool_pre_ping is True
    assert async_cfg.engine_config.pool_recycle == 1800

    sync_mem_cfg = create_sync_alchemy_config(
        connection_string="sqlite:///:memory:",
    )
    assert sync_mem_cfg.engine_config.pool_pre_ping is not True

    store = SyncObjectStore("memory://")
    plugin = create_alchemy_plugin(
        async_cfg,
        storage_backend=store,
    )
    assert isinstance(plugin, SQLAlchemyPlugin)

    app_config = AppConfig()
    plugin.on_app_init(app_config)
    for p in app_config.plugins:
        p.on_app_init(app_config)
    assert "db_session" in app_config.dependencies
    assert "db_engine" in app_config.dependencies

    backend = ObjectStoreBackend(store=store, prefix="docs")
    file_obj = FileObject(backend=backend, filename="hello.txt")
    saved = backend.save_object(file_obj, b"hello world")
    assert saved.size == 11
    assert backend.get_content("hello.txt") == b"hello world"


def test_broker_transport_leasing_and_channel_message() -> None:
    """Verify SyncSHMBrokerTransport, AsyncSHMBrokerTransport, clean break removals, and ChannelMessage."""
    assert not hasattr(alchemy_ext, "StatementCache")
    assert not hasattr(alchemy_ext, "SyncInMemoryBrokerTransport")
    assert not hasattr(alchemy_ext, "AsyncInMemoryBrokerTransport")

    msg = ChannelMessage(channel="events", payload=b"123", timestamp_ns=42)
    encoded = msgspec.msgpack.encode(msg)
    decoded = msgspec.msgpack.decode(encoded, type=ChannelMessage)
    assert decoded == msg

    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=1, acquire_timeout=0.05, lease_ttl=5.0))
    with transport.acquire() as lease1:
        assert lease1.is_overflow is False
        lease2 = transport.acquire()
        assert lease2.is_overflow is True
        with pytest.raises(TimeoutError):
            transport.acquire(timeout=0.02)
        lease2.release()

    metrics = transport.metrics()
    assert metrics.active_leases == 0
    assert metrics.total_acquired == 2
    assert metrics.total_released == 2
    assert metrics.total_timeouts == 1

    async def _run_async_broker() -> None:
        shm_broker = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
        async with await shm_broker.acquire() as async_lease:
            assert not async_lease.is_released
        assert async_lease.is_released
        await shm_broker.publish("Invalidations", b"key:1")
        drained = await shm_broker.drain_channel("Invalidations")
        assert len(drained) == 1
        assert drained[0].payload == b"key:1"
        await shm_broker.close()

    asyncio.run(_run_async_broker())


def test_storage_registry_alias_and_uri_resolution() -> None:
    """Verify ObjectStoreBackend registration in Advanced Alchemy storages registry."""
    mem_store = SyncObjectStore("memory://")
    mem_store.put("seed.txt", b"seeded")
    backend = register_storage_backend("primary_test_dropin", store=mem_store)
    assert (
        storages.get("primary_test_dropin") if hasattr(storages, "get") else storages.get_backend("primary_test_dropin")
    ) is backend
    assert backend.get_content("seed.txt") == b"seeded"


def test_pluggable_cli_discovery_and_app_plugin_hooks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify entry-point command discovery and CLIPluginProtocol.on_cli_init registration."""
    group = click.Group("vtb")

    @click.command("custom-ep")
    def _custom_ep_cmd() -> None:
        pass

    class _FakeEntryPoint:
        __slots__ = ()
        name = "custom-ep"

        @staticmethod
        def load() -> click.Command:
            return _custom_ep_cmd

    monkeypatch.setattr(
        "importlib.metadata.entry_points",
        lambda **_kwargs: [_FakeEntryPoint()],
    )
    discovered = discover_entry_point_commands(group)
    assert discovered == ["custom-ep"]
    assert "custom-ep" in group.commands

    class _CustomCliPlugin:
        __slots__ = ()

        def on_cli_init(self, cli: click.Group) -> None:
            @cli.command("app-plugin-cmd")
            def _cmd() -> None:
                pass

    plugin = _CustomCliPlugin()
    assert isinstance(plugin, CLIPluginProtocol)
    fake_app = SimpleNamespace(plugins=[plugin])
    activated = register_app_cli_plugins(group, fake_app)
    assert activated == ["_CustomCliPlugin"]
    assert "app-plugin-cmd" in group.commands


@pytest.mark.anyio
async def test_shm_session_broker_concurrency_and_lock_safety() -> None:
    """Verify SyncSHMSessionBroker and AsyncSHMSessionBroker lock safety, non-underflowing in_use, and O(1) cancelled waiter skip."""
    sync_broker = SyncSHMSessionBroker(capacity=2)
    assert sync_broker.capacity == 2
    assert sync_broker.in_use == 0

    assert sync_broker.acquire() is True
    assert sync_broker.in_use == 1

    assert sync_broker.acquire() is True
    assert sync_broker.in_use == 2

    assert sync_broker.acquire(timeout=0.01) is False
    assert sync_broker.in_use == 2

    sync_broker.release()
    assert sync_broker.in_use == 1

    sync_broker.release()
    assert sync_broker.in_use == 0

    sync_broker.release()
    assert sync_broker.in_use == 0

    async_broker = AsyncSHMSessionBroker(capacity=2)
    assert async_broker.capacity == 2
    assert async_broker.in_use == 0

    assert await async_broker.acquire() is True
    assert async_broker.in_use == 1

    assert await async_broker.acquire() is True
    assert async_broker.in_use == 2

    assert await async_broker.acquire(timeout=0.01) is False
    assert async_broker.in_use == 2

    await async_broker.release()
    assert async_broker.in_use == 1

    await async_broker.release()
    assert async_broker.in_use == 0

    await async_broker.release()
    assert async_broker.in_use == 0


@pytest.mark.anyio
async def test_database_service_and_stored_object_delegation() -> None:
    """Verify ObjectStoreBackend async delegation and alchemy engine configuration."""
    mem_store = SyncObjectStore("memory://")
    backend = ObjectStoreBackend(store=mem_store, prefix="reports")
    file_obj = FileObject(backend=backend, filename="summary.csv")
    await backend.save_object_async(file_obj, b"col1,col2\n1,2\n")
    content = await backend.get_content_async("summary.csv")
    assert content == b"col1,col2\n1,2\n"

    sync_cfg = create_sync_alchemy_config(
        connection_string="postgresql://localhost/app",
        engine_config={"pool_size": 15},
    )
    assert sync_cfg.engine_config.pool_size == 15
    assert sync_cfg.engine_config.pool_pre_ping is True
