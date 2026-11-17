"""Unit tests for canonical extension naming, PEP 695 generic configs, broker leasing, and CLI discovery."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import subprocess
import sys
import warnings
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast, get_args, get_origin

import click
import msgspec
import pytest
from advanced_alchemy._listeners import (
    AsyncCacheListener,
    AsyncFileObjectListener,
    SyncCacheListener,
    SyncFileObjectListener,
    touch_updated_timestamp,
)
from advanced_alchemy.base import BigIntAuditBase
from advanced_alchemy.config import RoutingConfig
from advanced_alchemy.exceptions import DuplicateKeyError, ForeignKeyError, wrap_sqlalchemy_exception
from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin
from advanced_alchemy.extensions.litestar.store import SQLAlchemyStore, StoreModelMixin
from advanced_alchemy.filters import RelationshipFilter as UpstreamRelationshipFilter
from advanced_alchemy.filterset import FilterSet as UpstreamFilterSet
from advanced_alchemy.repository import SQLAlchemySyncRepository
from advanced_alchemy.routing import RoutingAsyncSessionMaker, RoutingSyncSessionMaker
from advanced_alchemy.service import (
    SchemaDumpConfig as UpstreamSchemaDumpConfig,
)
from advanced_alchemy.service import (
    ServiceGroup as UpstreamServiceGroup,
)
from advanced_alchemy.service import (
    SQLAlchemySyncRepositoryService,
    schema_dump,
)
from advanced_alchemy.types.file_object import FileObject
from advanced_alchemy.types.file_object.registry import storages
from litestar import Litestar, get
from litestar.config.app import AppConfig
from litestar.di import NamedDependency
from litestar.exceptions import LitestarDeprecationWarning
from litestar.params import SkipValidation
from litestar.plugins import InitPluginProtocol
from litestar.testing import TestClient
from sqlalchemy import ForeignKey, String, create_engine, event, select
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship
from sqlalchemy.pool import StaticPool

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
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, BigQueryConfig, SyncBigQueryDriver
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import (
    AsyncOracleDriver,
    AsyncOraclePool,
    OracleConfig,
    SyncOracleDriver,
    SyncOraclePool,
)
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SpannerConfig, SyncSpannerDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.extensions.alchemy import (
    AsyncSHMBrokerTransport,
    AsyncSHMSessionBroker,
    BrokerConfig,
    ChannelMessage,
    FilterSet,
    ObjectStoreBackend,
    RelationshipFilter,
    SchemaDumpConfig,
    ServiceGroup,
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
    AsyncAdapterConfig,
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
    SyncAdapterConfig,
    create_sqlspec_plugin,
)
from vertebra.extensions.sqlspec.adapters.bigquery.core import BigQueryConnectionShim
from vertebra.extensions.sqlspec.adapters.spanner.core import SpannerConnectionShim
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
        cast("Callable[[], Any]", startup_fn)()
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


@pytest.mark.parametrize(
    ("config_cls", "expected_base", "expected_args"),
    [
        (
            PostgresSyncConfig,
            SyncAdapterConfig,
            (PostgreSQLConfig, SyncPostgreSQLDriver, SyncPostgreSQLDriver, PostgresSyncDriver),
        ),
        (
            PostgresAsyncConfig,
            AsyncAdapterConfig,
            (PostgreSQLConfig, AsyncPostgreSQLDriver, AsyncPostgreSQLDriver, PostgresAsyncDriver),
        ),
        (
            OracleSyncConfig,
            SyncAdapterConfig,
            (OracleConfig, SyncOracleDriver, SyncOraclePool | SyncOracleDriver, OracleSyncDriver),
        ),
        (
            OracleAsyncConfig,
            AsyncAdapterConfig,
            (OracleConfig, AsyncOracleDriver, AsyncOraclePool | AsyncOracleDriver, OracleAsyncDriver),
        ),
        (SQLiteSyncConfig, SyncAdapterConfig, (SQLiteConfig, SyncSQLiteDriver, SyncSQLiteDriver, SQLiteSyncDriver)),
        (
            SQLiteAsyncConfig,
            AsyncAdapterConfig,
            (SQLiteConfig, AsyncSQLiteDriver, AsyncSQLiteDriver, SQLiteAsyncDriver),
        ),
        (MySQLSyncConfig, SyncAdapterConfig, (MySQLConfig, SyncMySQLDriver, SyncMySQLDriver, MySQLSyncDriver)),
        (MySQLAsyncConfig, AsyncAdapterConfig, (MySQLConfig, AsyncMySQLDriver, AsyncMySQLDriver, MySQLAsyncDriver)),
        (MSSQLSyncConfig, SyncAdapterConfig, (MSSQLConfig, SyncMSSQLDriver, SyncMSSQLDriver, MSSQLSyncDriver)),
        (MSSQLAsyncConfig, AsyncAdapterConfig, (MSSQLConfig, AsyncMSSQLDriver, AsyncMSSQLDriver, MSSQLAsyncDriver)),
        (DuckDBSyncConfig, SyncAdapterConfig, (DuckDBConfig, SyncDuckDBDriver, SyncDuckDBDriver, DuckDBSyncDriver)),
        (
            DuckDBAsyncConfig,
            AsyncAdapterConfig,
            (DuckDBConfig, AsyncDuckDBDriver, AsyncDuckDBDriver, DuckDBAsyncDriver),
        ),
        (
            BigQuerySyncConfig,
            SyncAdapterConfig,
            (BigQueryConfig, BigQueryConnectionShim, SyncBigQueryDriver, BigQuerySyncDriver),
        ),
        (
            BigQueryAsyncConfig,
            AsyncAdapterConfig,
            (BigQueryConfig, BigQueryConnectionShim, AsyncBigQueryDriver, BigQueryAsyncDriver),
        ),
        (
            SpannerSyncConfig,
            SyncAdapterConfig,
            (SpannerConfig, SpannerConnectionShim, SyncSpannerDriver, SpannerSyncDriver),
        ),
        (
            SpannerAsyncConfig,
            AsyncAdapterConfig,
            (SpannerConfig, SpannerConnectionShim, AsyncSpannerDriver, SpannerAsyncDriver),
        ),
    ],
    ids=[
        "postgres-sync",
        "postgres-async",
        "oracle-sync",
        "oracle-async",
        "sqlite-sync",
        "sqlite-async",
        "mysql-sync",
        "mysql-async",
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
def test_sqlspec_adapter_configs_specialize_four_arg_generics(
    config_cls: type[Any],
    expected_base: type[Any],
    expected_args: tuple[Any, ...],
) -> None:
    """Verify every adapter config specializes 4-arg SyncAdapterConfig or AsyncAdapterConfig."""
    assert len(expected_base.__type_params__) == 4
    matching_bases = [base for base in config_cls.__orig_bases__ if get_origin(base) is expected_base]
    assert len(matching_bases) == 1
    assert get_args(matching_bases[0]) == expected_args


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
        if isinstance(p, InitPluginProtocol):
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

    transport = SyncSHMBrokerTransport(
        BrokerConfig(pool_size=1, max_overflow=1, acquire_timeout_ms=50, lease_ttl_ms=5_000)
    )
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
    assert storages.get_backend("primary_test_dropin") is backend
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


class _R209Author(BigIntAuditBase):
    __tablename__ = "r209_authors"

    slug: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(128))
    bio: Mapped[str | None] = mapped_column(String(256), default=None, nullable=True)
    books: Mapped[list[_R209Book]] = relationship(back_populates="author")


class _R209Book(BigIntAuditBase):
    __tablename__ = "r209_books"

    title: Mapped[str] = mapped_column(String(128))
    author_id: Mapped[int] = mapped_column(ForeignKey("r209_authors.id"))
    author: Mapped[_R209Author] = relationship(back_populates="books")


class _R209StoreItem(StoreModelMixin):
    __tablename__ = "r209_store_items"


class _R209AuthorRepo(SQLAlchemySyncRepository[_R209Author]):
    model_type = _R209Author


class _R209BookRepo(SQLAlchemySyncRepository[_R209Book]):
    model_type = _R209Book


class _R209AuthorService(SQLAlchemySyncRepositoryService[_R209Author, _R209AuthorRepo]):
    repository_type = _R209AuthorRepo


class _R209BookService(SQLAlchemySyncRepositoryService[_R209Book, _R209BookRepo]):
    repository_type = _R209BookRepo


class _R209CatalogServices(ServiceGroup):
    authors: _R209AuthorService
    books: _R209BookService


class _R209AuthorFilterSet(FilterSet[_R209Author]):
    filter_fields = ("name", "books.title")


class _R209AuthorPatch(msgspec.Struct, kw_only=True):
    slug: str
    name: str
    bio: str | None | msgspec.UnsetType = msgspec.UNSET
    rank: int = 0


def test_r209_upstream_alchemy_exports_identity() -> None:
    """Verify R2.09 upstream Advanced Alchemy symbols are exported with exact identity."""
    assert FilterSet is UpstreamFilterSet
    assert RelationshipFilter is UpstreamRelationshipFilter
    assert SchemaDumpConfig is UpstreamSchemaDumpConfig
    assert ServiceGroup is UpstreamServiceGroup
    for name in ("FilterSet", "RelationshipFilter", "SchemaDumpConfig", "ServiceGroup"):
        assert name in alchemy_ext.__all__


def test_r209_service_group_get_or_none_upsert_many_and_schema_dump_config() -> None:
    """Verify ServiceGroup shared session, get_or_none, upsert_many with no_merge, and SchemaDumpConfig."""
    engine = create_engine("sqlite:///:memory:")
    BigIntAuditBase.metadata.create_all(engine)

    unset_patch = _R209AuthorPatch(slug="ada", name="Ada Lovelace")
    dumped_filtered = schema_dump(
        unset_patch,
        config=SchemaDumpConfig(
            exclude_unset=True,
            exclude_none=True,
            exclude_defaults=True,
            exclude_sentinels=True,
        ),
    )
    assert dumped_filtered == {"slug": "ada", "name": "Ada Lovelace"}

    dumped_full = schema_dump(
        _R209AuthorPatch(slug="ada", name="Ada Lovelace", bio=None, rank=0),
        config=SchemaDumpConfig(
            exclude_unset=False,
            exclude_none=False,
            exclude_defaults=False,
            exclude_sentinels=False,
        ),
    )
    assert dumped_full == {"slug": "ada", "name": "Ada Lovelace", "bio": None, "rank": 0}

    with Session(engine) as session:
        services = _R209CatalogServices(session)
        assert services.authors.repository.session is session
        assert services.books.repository.session is session
        assert services.authors.get_or_none(999_999) is None

        created = services.authors.upsert_many(
            [
                _R209AuthorPatch(slug="ada", name="Ada", bio="Initial bio"),
                _R209AuthorPatch(slug="grace", name="Grace Hopper"),
            ],
            match_fields="slug",
            no_merge=True,
            auto_commit=True,
            schema_dump_config=SchemaDumpConfig(exclude_unset=True, exclude_sentinels=True),
        )
        assert len(created) == 2

        fetched = services.authors.get_or_none(created[0].id)
        assert fetched is not None
        assert fetched.name == "Ada"

        updated = services.authors.upsert_many(
            [_R209AuthorPatch(slug="ada", name="Ada Lovelace")],
            match_fields="slug",
            no_merge=True,
            auto_commit=True,
            schema_dump_config=SchemaDumpConfig(exclude_unset=True, exclude_sentinels=True),
        )
        assert len(updated) == 1
        assert updated[0].name == "Ada Lovelace"
        assert updated[0].bio == "Initial bio"


def test_r209_filterset_and_relationship_filter() -> None:
    """Verify FilterSet query parsing and RelationshipFilter existence predicates."""
    engine = create_engine("sqlite:///:memory:")
    BigIntAuditBase.metadata.create_all(engine)

    with Session(engine) as session:
        ada = _R209Author(slug="ada", name="Ada Lovelace")
        grace = _R209Author(slug="grace", name="Grace Hopper")
        session.add_all([ada, grace])
        session.flush()
        session.add_all(
            [
                _R209Book(title="Notes on the Analytical Engine", author_id=ada.id),
                _R209Book(title="COBOL Manual", author_id=grace.id),
            ]
        )
        session.commit()

        fs = _R209AuthorFilterSet(
            name__eq="Ada Lovelace",
            books__title__eq="Notes on the Analytical Engine",
        )
        fs_stmt = fs.append_to_statement(select(_R209Author), _R209Author)
        fs_rows = list(session.scalars(fs_stmt))
        assert [author.slug for author in fs_rows] == ["ada"]

        rel_filter = RelationshipFilter(_R209Author.books, [_R209Book.title == "COBOL Manual"])
        rel_stmt = rel_filter.append_to_statement(select(_R209Author), _R209Author)
        assert "EXISTS" in str(rel_stmt)
        rel_rows = list(session.scalars(rel_stmt))
        assert [author.slug for author in rel_rows] == ["grace"]

        neg_filter = RelationshipFilter(
            _R209Author.books,
            [_R209Book.title == "COBOL Manual"],
            negate=True,
        )
        neg_rows = list(session.scalars(neg_filter.append_to_statement(select(_R209Author), _R209Author)))
        assert [author.slug for author in neg_rows] == ["ada"]


@pytest.mark.anyio
async def test_r209_sqlalchemy_store_delete_expired() -> None:
    """Verify SQLAlchemyStore.delete_expired purges expired rows while preserving live and foreign-namespace rows."""
    sync_cfg = create_sync_alchemy_config(
        connection_string="sqlite:///:memory:",
        engine_config={"poolclass": StaticPool},
    )
    with sync_cfg.get_engine().begin() as conn:
        _R209StoreItem.metadata.create_all(conn)

    store = SQLAlchemyStore(config=sync_cfg, model=_R209StoreItem, namespace="r209")
    other_store = SQLAlchemyStore(config=sync_cfg, model=_R209StoreItem, namespace="other")

    await store.set("expired-key", b"stale", expires_in=datetime.timedelta(seconds=-10))
    await store.set("live-key", b"fresh", expires_in=3600)
    await other_store.set("other-expired", b"isolated", expires_in=datetime.timedelta(seconds=-10))

    await store.delete_expired()

    assert await store.get("live-key") == b"fresh"
    with Session(sync_cfg.get_engine()) as session:
        remaining = list(session.scalars(select(_R209StoreItem.key).order_by(_R209StoreItem.key)))
        assert remaining == ["live-key", "other-expired"]


def test_r209_routing_session_maker_listeners() -> None:
    """Verify RoutingSyncSessionMaker and RoutingAsyncSessionMaker attach listeners and update timestamps."""
    sync_cfg = create_sync_alchemy_config(
        routing_config=RoutingConfig(primary_connection_string="sqlite:///:memory:"),
    )
    sync_maker = sync_cfg.create_session_maker()
    assert isinstance(sync_maker, RoutingSyncSessionMaker)
    assert event.contains(sync_maker.session_maker, "before_flush", touch_updated_timestamp)
    assert event.contains(sync_maker.session_maker, "before_flush", SyncFileObjectListener.before_flush)
    assert event.contains(sync_maker.session_maker, "after_commit", SyncCacheListener.after_commit)

    with sync_maker() as session:
        BigIntAuditBase.metadata.create_all(session.connection())
        author = _R209Author(slug="barbara", name="Barbara Liskov")
        session.add(author)
        session.flush()
        assert author.created_at is not None
        assert author.updated_at is not None
        initial_updated_at = author.updated_at

        author.updated_at = initial_updated_at - datetime.timedelta(seconds=60)
        session.flush()
        older_updated_at = author.updated_at

        author.name = "Barbara H. Liskov"
        session.flush()
        assert author.updated_at > older_updated_at
        session.commit()

    async_cfg = create_async_alchemy_config(
        routing_config=RoutingConfig(primary_connection_string="sqlite+vtb_async:///:memory:"),
    )
    async_maker = async_cfg.create_session_maker()
    assert isinstance(async_maker, RoutingAsyncSessionMaker)
    async_sync_maker = async_maker._session_config["sync_session_class"]
    assert event.contains(async_sync_maker, "before_flush", touch_updated_timestamp)
    assert event.contains(async_sync_maker, "before_flush", AsyncFileObjectListener.before_flush)
    assert event.contains(async_sync_maker, "after_commit", AsyncCacheListener.after_commit)


class AlreadyExists(Exception):
    pass


@pytest.mark.parametrize(
    ("dialect_name", "orig_factory", "expected_error"),
    [
        ("spanner", lambda: AlreadyExists("row exists"), DuplicateKeyError),
        (
            "spanner+spanner",
            lambda: type("_GrpcOrig", (Exception,), {"grpc_status_code": SimpleNamespace(name="ALREADY_EXISTS")})(
                "duplicate"
            ),
            DuplicateKeyError,
        ),
        ("spanner+vtb", lambda: AlreadyExists("row exists"), DuplicateKeyError),
        (
            "cockroach",
            lambda: type("_CrdbUniqueOrig", (Exception,), {"sqlstate": "23505"})("duplicate key"),
            DuplicateKeyError,
        ),
        (
            "cockroachdb",
            lambda: type("_CrdbFkOrig", (Exception,), {"sqlstate": "23503"})("foreign key violation"),
            ForeignKeyError,
        ),
    ],
    ids=[
        "spanner-already-exists",
        "spanner+spanner-grpc-status",
        "spanner+vtb-already-exists",
        "cockroach-23505-duplicate",
        "cockroachdb-23503-fk",
    ],
)
def test_r209_spanner_and_cockroach_integrity_classification(
    dialect_name: str,
    orig_factory: Callable[[], BaseException],
    expected_error: type[Exception],
) -> None:
    """Verify wrap_sqlalchemy_exception classifies Spanner and CockroachDB IntegrityError variants."""
    with pytest.raises(expected_error), wrap_sqlalchemy_exception(dialect_name=dialect_name):
        raise SQLAlchemyIntegrityError("INSERT INTO items VALUES (1)", {}, orig_factory())


def test_alchemy_lazy_exports_dir_and_resolution() -> None:
    """Verify alchemy.__all__, dir(), _EXPORT_MAP, and submodule attributes resolve consistently."""
    exported = set(alchemy_ext.__all__)
    expected_subset = {
        "AsyncCacheRegion",
        "AsyncConnectionLease",
        "AsyncSHMBrokerTransport",
        "AsyncSHMSessionBroker",
        "BrokerConfig",
        "BrokerMetrics",
        "BrokerPlugin",
        "CacheConfig",
        "ChannelMessage",
        "FilterSet",
        "ObjectStoreBackend",
        "RelationshipFilter",
        "SchemaDumpConfig",
        "ServiceGroup",
        "StatementPlan",
        "SyncCacheRegion",
        "SyncConnectionLease",
        "SyncSHMBrokerTransport",
        "SyncSHMSessionBroker",
        "ZeroCopyServiceMixin",
        "create_alchemy_plugin",
        "create_async_alchemy_config",
        "create_cache_config",
        "create_fastapi_alchemy",
        "create_flask_alchemy",
        "create_starlette_alchemy",
        "create_sync_alchemy_config",
        "msgspec_model_deserializer",
        "msgspec_model_serializer",
        "normalize_alchemy_url",
        "register_storage_backend",
        "setup_cache_listeners",
    }
    assert expected_subset <= exported
    assert set(alchemy_ext._EXPORT_MAP) == exported
    assert exported <= set(dir(alchemy_ext))

    for name, (mod_path, attr_name) in alchemy_ext._EXPORT_MAP.items():
        resolved = getattr(alchemy_ext, name)
        mod = importlib.import_module(mod_path)
        assert resolved is getattr(mod, attr_name)

    broker_mod = importlib.import_module("vertebra.extensions.alchemy.broker")
    assert alchemy_ext.SyncSHMSessionBroker is broker_mod.SyncSHMSessionBroker
    assert alchemy_ext.AsyncSHMSessionBroker is broker_mod.AsyncSHMSessionBroker


def test_alchemy_broker_import_isolated_from_cache_and_orm() -> None:
    """Verify vertebra.extensions.alchemy.broker imports and runs with advanced_alchemy and sqlalchemy blocked."""
    script = """
import sys
for mod_name in (
    "advanced_alchemy",
    "advanced_alchemy.cache",
    "advanced_alchemy.filters",
    "advanced_alchemy.filterset",
    "advanced_alchemy.service",
    "sqlalchemy",
    "sqlalchemy.event",
):
    sys.modules[mod_name] = None

import vertebra.extensions.alchemy as alchemy_pkg
import vertebra.extensions.alchemy.broker as broker_mod
from vertebra.extensions.alchemy import (
    AsyncConnectionLease,
    AsyncSHMSessionBroker,
    BrokerConfig,
    BrokerMetrics,
    BrokerPlugin,
    StatementPlan,
    SyncConnectionLease,
    SyncSHMSessionBroker,
)

assert SyncSHMSessionBroker is broker_mod.SyncSHMSessionBroker
assert AsyncSHMSessionBroker is broker_mod.AsyncSHMSessionBroker
assert BrokerConfig is broker_mod.BrokerConfig
assert BrokerMetrics is broker_mod.BrokerMetrics
assert BrokerPlugin is broker_mod.BrokerPlugin
assert StatementPlan is broker_mod.StatementPlan
assert SyncConnectionLease is broker_mod.SyncConnectionLease
assert AsyncConnectionLease is broker_mod.AsyncConnectionLease

sync_broker = SyncSHMSessionBroker(capacity=1)
async_broker = AsyncSHMSessionBroker(capacity=1)
assert sync_broker.acquire() is True
assert sync_broker.in_use == 1
sync_broker.release()
assert sync_broker.in_use == 0
assert async_broker.capacity == 1

for forbidden in (
    "vertebra.extensions.alchemy.cache",
    "vertebra.extensions.alchemy.plugin",
    "vertebra.extensions.alchemy.storage",
):
    assert forbidden not in sys.modules, f"Unexpectedly imported {forbidden}"
assert "vertebra.extensions.alchemy.broker" in sys.modules
assert hasattr(alchemy_pkg, "SyncSHMSessionBroker")
"""
    proc = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True)
    assert proc.returncode == 0, f"Subprocess failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"


@pytest.mark.parametrize("symbol_name", ["ServiceGroup", "create_cache_config", "ObjectStoreBackend"])
def test_alchemy_lazy_lookup_raises_when_optional_deps_blocked(symbol_name: str) -> None:
    """Verify accessing ORM/cache/storage exports raises ImportError or MissingDependencyError when blocked."""
    script = f"""
import sys
for mod_name in (
    "advanced_alchemy",
    "advanced_alchemy.cache",
    "advanced_alchemy.filters",
    "advanced_alchemy.filterset",
    "advanced_alchemy.service",
    "advanced_alchemy.types",
    "advanced_alchemy.types.file_object",
    "sqlalchemy",
):
    sys.modules[mod_name] = None

import vertebra.extensions.alchemy as alchemy_pkg
from vertebra.typing import MissingDependencyError

try:
    getattr(alchemy_pkg, {symbol_name!r})
except (MissingDependencyError, ImportError):
    pass
else:
    raise AssertionError("Expected MissingDependencyError or ImportError")
"""
    proc = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True)
    assert proc.returncode == 0, f"Subprocess failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"


@pytest.mark.parametrize(
    ("adapter_name", "prefix"),
    [
        ("bigquery", "BigQuery"),
        ("duckdb", "DuckDB"),
        ("mssql", "MSSQL"),
        ("mysql", "MySQL"),
        ("oracle", "Oracle"),
        ("postgres", "Postgres"),
        ("spanner", "Spanner"),
        ("sqlite", "SQLite"),
    ],
)
def test_sqlspec_typing_and_adapter_exports(adapter_name: str, prefix: str) -> None:
    """Verify _typing contexts and TypedDict schemas are lazily exported on sqlspec and sqlspec.adapters."""
    adapters_pkg = importlib.import_module("vertebra.extensions.sqlspec.adapters")
    typing_mod = importlib.import_module(f"vertebra.extensions.sqlspec.adapters.{adapter_name}._typing")

    expected_names = [
        f"{prefix}SyncConnectionContext",
        f"{prefix}AsyncConnectionContext",
        f"{prefix}SyncSessionContext",
        f"{prefix}AsyncSessionContext",
        f"{prefix}ConnectionParams",
        f"{prefix}PoolParams",
        f"{prefix}DriverFeatures",
    ]
    if adapter_name == "oracle":
        expected_names.extend(
            [
                "OracleSyncSessionlessTransactionContext",
                "OracleAsyncSessionlessTransactionContext",
            ]
        )

    assert set(sqlspec_ext._EXPORT_MAP) == set(sqlspec_ext.__all__)
    assert set(adapters_pkg._EXPORT_MAP) == set(adapters_pkg.__all__)

    for name in expected_names:
        assert name in sqlspec_ext.__all__
        assert name in adapters_pkg.__all__
        assert name in dir(sqlspec_ext)
        assert name in dir(adapters_pkg)
        expected_obj = getattr(typing_mod, name)
        assert getattr(sqlspec_ext, name) is expected_obj
        assert getattr(adapters_pkg, name) is expected_obj


def test_sqlspec_lazy_typing_exports_do_not_import_adk_or_pyarrow() -> None:
    """Verify accessing _typing exports on sqlspec and sqlspec.adapters does not eagerly import pyarrow or adk."""
    script = """
import sys
import vertebra.extensions.sqlspec as sqlspec_pkg
import vertebra.extensions.sqlspec.adapters as adapters_pkg

for name in (
    "BigQuerySyncConnectionContext",
    "BigQueryDriverFeatures",
    "SpannerAsyncSessionContext",
    "SpannerPoolParams",
    "DuckDBConnectionParams",
    "SQLiteSyncSessionContext",
    "PostgresAsyncConnectionContext",
    "OracleSyncSessionlessTransactionContext",
    "MySQLDriverFeatures",
    "MSSQLAsyncSessionContext",
):
    assert getattr(sqlspec_pkg, name) is getattr(adapters_pkg, name)

assert "pyarrow" not in sys.modules
assert "google.adk" not in sys.modules
"""
    proc = subprocess.run([sys.executable, "-c", script], check=False, capture_output=True, text=True)
    assert proc.returncode == 0, f"Subprocess failed:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
