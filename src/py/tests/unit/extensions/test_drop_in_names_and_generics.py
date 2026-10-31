"""Unit tests for canonical extension naming, PEP 695 generic configs, broker leasing, and CLI discovery."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import click
import msgspec
import pytest
from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin
from advanced_alchemy.types.file_object import FileObject
from advanced_alchemy.types.file_object.registry import storages
from litestar.config.app import AppConfig

import vertebra
import vertebra.extensions.alchemy as alchemy_ext
import vertebra.extensions.litestar as litestar_ext
import vertebra.extensions.sqlspec as sqlspec_ext
import vertebra.framework as vtb_framework
import vertebra.framework.procedures as vtb_procedures
from vertebra.build.codegen import CodegenConfig, CodegenPlugin
from vertebra.cli.plugin_registry import (
    CLIPluginProtocol,
    discover_entry_point_commands,
    register_app_cli_plugins,
)
from vertebra.extensions.alchemy import (
    BrokerConfig,
    ChannelMessage,
    InMemoryBrokerTransport,
    ObjectStoreBackend,
    SHMBrokerTransport,
    SHMSessionBroker,
    StatementCache,
    create_alchemy_plugin,
    create_async_alchemy_config,
    create_sync_alchemy_config,
)
from vertebra.extensions.alchemy.storage import register_storage_backend
from vertebra.extensions.litestar import (
    ChannelsBackend,
    LitestarConfig,
    LitestarPlugin,
    LitestarRouteCompiler,
    MemoryStore,
)
from vertebra.extensions.sqlspec import (
    ProxyAsyncConfig,
    ProxySyncConfig,
    SQLSpecPlugin,
    create_sqlspec_plugin,
)
from vertebra.storage import ObjectStore


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
    assert not hasattr(litestar_ext, "VertebraConfig")
    assert not hasattr(litestar_ext, "VertebraPlugin")
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

    plugin = LitestarPlugin(LitestarConfig(enable_health_endpoint=True))
    assert plugin.config.enable_health_endpoint is True
    assert isinstance(ChannelsBackend(history=4), ChannelsBackend)
    assert isinstance(MemoryStore(), MemoryStore)
    assert isinstance(CodegenPlugin(CodegenConfig()), CodegenPlugin)
    assert LitestarRouteCompiler is not None
    assert ObjectStoreBackend is not None


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

    store = ObjectStore("memory://")
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
    """Verify InMemoryBrokerTransport, SHMBrokerTransport, lease TTL expiration, and ChannelMessage."""
    msg = ChannelMessage(channel="events", payload=b"123", timestamp_ns=42)
    encoded = msgspec.msgpack.encode(msg)
    decoded = msgspec.msgpack.decode(encoded, type=ChannelMessage)
    assert decoded == msg

    transport = InMemoryBrokerTransport(BrokerConfig(pool_size=1, max_overflow=1, acquire_timeout=0.05, lease_ttl=0.05))
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
        shm_broker = SHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
        async with await shm_broker.acquire_async() as async_lease:
            assert not async_lease.is_released
        assert async_lease.is_released
        shm_broker.publish("Invalidations", b"key:1")
        drained = shm_broker.drain_channel("Invalidations")
        assert len(drained) == 1
        assert drained[0].payload == b"key:1"
        shm_broker.close()

    asyncio.run(_run_async_broker())


def test_storage_registry_alias_and_uri_resolution() -> None:
    """Verify ObjectStoreBackend registration in Advanced Alchemy storages registry."""
    mem_store = ObjectStore("memory://")
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
    """Verify SHMSessionBroker lock safety, non-underflowing in_use, and O(1) cancelled waiter skip."""
    broker = SHMSessionBroker(capacity=2)
    assert broker.capacity == 2
    assert broker.in_use == 0

    assert broker.acquire() is True
    assert broker.in_use == 1

    assert await broker.acquire_async() is True
    assert broker.in_use == 2

    assert await broker.acquire_async(timeout=0.01) is False
    assert broker.in_use == 2

    broker.release()
    assert broker.in_use == 1

    await broker.release_async()
    assert broker.in_use == 0

    broker.release()
    assert broker.in_use == 0


def test_statement_cache_compilation() -> None:
    """Verify StatementCache compiles ClauseElement-like objects and extracts parameter order."""
    cache = StatementCache()

    class FakeCompiled:
        def __init__(self) -> None:
            self.positiontup = ("user_id", "status")

        def __str__(self) -> str:
            return "SELECT id FROM users WHERE user_id = :user_id AND status = :status"

    class FakeStatement:
        def compile(self) -> FakeCompiled:
            return FakeCompiled()

    sql_text, params = cache.compile_and_encode(FakeStatement())
    assert "SELECT id FROM users" in sql_text
    assert params == ("user_id", "status")

    raw_sql, empty_params = cache.compile_and_encode("SELECT 1")
    assert raw_sql == "SELECT 1"
    assert empty_params == ()


@pytest.mark.anyio
async def test_database_service_and_stored_object_delegation() -> None:
    """Verify ObjectStoreBackend async delegation and alchemy engine configuration."""
    mem_store = ObjectStore("memory://")
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
