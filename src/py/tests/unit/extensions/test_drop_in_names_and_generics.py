"""Unit tests for canonical extension naming, PEP 695 generic configs, broker leasing, and CLI discovery."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import click
import msgspec
import pytest

import vertebra.extensions.alchemy as alchemy_ext
import vertebra.extensions.litestar as litestar_ext
import vertebra.extensions.sqlspec as sqlspec_ext
from vertebra.build.codegen import CodegenConfig, CodegenPlugin
from vertebra.cli.plugin_registry import (
    CLIPluginProtocol,
    discover_entry_point_commands,
    register_app_cli_plugins,
)
from vertebra.extensions.alchemy import (
    AlchemyPlugin,
    BrokerConfig,
    BrokerPlugin,
    ChannelMessage,
    FileAttachment,
    InMemoryBrokerTransport,
    ObjectStoreBackend,
    ShmBrokerTransport,
    SQLAlchemyAsyncConfig,
    SQLAlchemySyncConfig,
)
from vertebra.extensions.litestar import (
    ChannelsBackend,
    LitestarConfig,
    LitestarPlugin,
    LitestarRouteCompiler,
    MemoryStore,
    MiddlewareOffloadCompiler,
)
from vertebra.extensions.sqlspec import (
    AsyncDatabaseConfig,
    ProxyAsyncConfig,
    ProxySyncConfig,
    SQLSpecConfig,
    SQLSpecPlugin,
    SQLSpecStore,
    SyncDatabaseConfig,
)
from vertebra.storage import (
    ObjectStore,
    StorageRegistry,
    get_default_storage_registry,
)


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
    assert not hasattr(litestar_ext, "ServerConfig")
    assert not hasattr(litestar_ext, "VertebraConfig")
    assert not hasattr(litestar_ext, "VertebraPlugin")
    assert not hasattr(alchemy_ext, "AdvancedAlchemy")
    assert not hasattr(alchemy_ext, "SQLAlchemyPlugin")
    assert not hasattr(alchemy_ext, "AlchemyStore")
    assert not hasattr(alchemy_ext, "StorageBackend")
    assert not hasattr(sqlspec_ext, "SQLSpec")
    assert not hasattr(sqlspec_ext, "SQLSpecStorageBridge")
    assert not hasattr(sqlspec_ext, "VertebraSQLSpecPlugin")

    plugin = LitestarPlugin(LitestarConfig(enable_health_endpoint=True))
    assert plugin.config.enable_health_endpoint is True
    assert isinstance(ChannelsBackend(history=4), ChannelsBackend)
    assert isinstance(MemoryStore(), MemoryStore)
    assert isinstance(CodegenPlugin(CodegenConfig()), CodegenPlugin)
    assert LitestarRouteCompiler is not None
    assert MiddlewareOffloadCompiler is not None
    assert ObjectStoreBackend is not None
    assert SQLSpecStore is not None


def test_sqlspec_generic_configs_and_pool_defaults() -> None:
    """Verify PEP 695 generic SQLSpecConfig, ProxySyncConfig, ProxyAsyncConfig, and pool defaults."""
    sync_driver = _DummySyncDriver()
    sync_cfg = SyncDatabaseConfig[str, str, _DummySyncDriver](
        dsn="postgresql://localhost:5432/app",
        bind_key="primary",
        driver_instance=sync_driver,
        connection_instance="sync_conn",
        pool_instance="sync_pool",
    )
    assert sync_cfg.pool_config["pool_pre_ping"] is True
    assert sync_cfg.pool_config["pool_recycle"] == 1800

    in_mem_cfg = SyncDatabaseConfig[str, str, _DummySyncDriver](
        dsn="sqlite:///:memory:",
        driver_instance=sync_driver,
    )
    assert "pool_pre_ping" not in in_mem_cfg.pool_config

    wrapped = SQLSpecConfig.wrap(sync_cfg)
    assert isinstance(wrapped, SQLSpecConfig)
    assert wrapped.is_async is False
    assert wrapped.provide_session() is sync_driver

    proxy_sync = ProxySyncConfig(sync_cfg)
    assert proxy_sync.provide_session() is sync_driver
    assert proxy_sync.provide_connection() == "sync_conn"
    assert proxy_sync.create_pool() == "sync_pool"

    async_driver = _DummyAsyncDriver()
    async_cfg = AsyncDatabaseConfig[str, str, _DummyAsyncDriver](
        dsn="postgresql+asyncpg://localhost:5432/app",
        driver_instance=async_driver,
    )
    assert async_cfg.pool_config["pool_pre_ping"] is True
    proxy_async = ProxyAsyncConfig(async_cfg)
    assert proxy_async.is_async is True
    assert proxy_async.provide_session() is async_driver

    spec_plugin: SQLSpecPlugin[object] = SQLSpecPlugin(
        config=[proxy_sync, proxy_async],
        store=ObjectStore("memory://"),
    )
    assert spec_plugin.get_session(proxy_sync) is sync_driver
    assert spec_plugin.get_session(proxy_async) is async_driver

    app_config = SimpleNamespace(state={}, dependencies={})
    spec_plugin.on_app_init(app_config)
    assert "sqlspec" in app_config.state
    assert "db_session" in app_config.dependencies
    assert "db_connection" in app_config.dependencies


def test_alchemy_generic_configs_and_attachment_storage() -> None:
    """Verify SQLAlchemyAsyncConfig, SQLAlchemySyncConfig, AlchemyPlugin, and ObjectStoreBackend."""
    async_cfg = SQLAlchemyAsyncConfig[str, str, object](
        connection_string="postgresql+asyncpg://localhost/db",
        engine_instance="async_engine",
        session_maker_instance=lambda: "async_session",
    )
    assert async_cfg.engine_config["pool_pre_ping"] is True
    assert async_cfg.engine_config["pool_recycle"] == 1800
    assert async_cfg.provide_engine() == "async_engine"
    assert async_cfg.provide_session() == "async_session"

    sync_mem_cfg = SQLAlchemySyncConfig[str, str, object](
        connection_string="sqlite:///:memory:",
        engine_instance="sync_engine",
        session_maker_instance=lambda: "sync_session",
    )
    assert "pool_pre_ping" not in sync_mem_cfg.engine_config

    custom_cfg = SimpleNamespace(connection_string="postgresql://localhost/db", engine_config={})
    AlchemyPlugin.wrap_config(custom_cfg)
    assert custom_cfg.engine_config["pool_pre_ping"] is True

    store = ObjectStore("memory://")
    plugin = AlchemyPlugin(
        config=async_cfg,
        store=store,
        broker=BrokerPlugin(BrokerConfig(pool_size=2, max_overflow=1)),
    )
    app_config = SimpleNamespace(state={}, dependencies={})
    plugin.on_app_init(app_config)

    aa_state = app_config.state["advanced_alchemy"]
    assert aa_state.storage_backend is not None
    assert aa_state.broker is not None
    assert "db_session" in app_config.dependencies
    assert "db_engine" in app_config.dependencies
    assert "db_broker" in app_config.dependencies
    assert "db_broker_metrics" in app_config.dependencies

    attachment = FileAttachment(path="docs/hello.txt", filename="hello.txt")
    saved = aa_state.storage_backend.save_object(attachment, b"hello world")
    assert saved.size == 11
    assert aa_state.storage_backend.get_content("docs/hello.txt") == b"hello world"


def test_broker_transport_leasing_and_channel_message() -> None:
    """Verify InMemoryBrokerTransport, ShmBrokerTransport, lease TTL expiration, and ChannelMessage."""
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
        shm_broker = ShmBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
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
    """Verify StorageRegistry resolves aliases and URI schemes with caching."""
    registry = StorageRegistry()
    mem_store = ObjectStore("memory://")
    mem_store.put("seed.txt", b"seeded")
    registry.register_store("primary", mem_store)
    registry.register_alias("exports", "memory://")

    assert registry.resolve_store("primary") is mem_store
    assert registry.resolve_store("primary").get("seed.txt") == b"seeded"
    exports_store = registry.resolve_store("exports")
    exports_store.put("out.json", b"{}")
    assert registry.get("exports") is exports_store
    assert set(registry.aliases()) == {"exports", "primary"}

    default_reg = get_default_storage_registry()
    assert isinstance(default_reg, StorageRegistry)


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
