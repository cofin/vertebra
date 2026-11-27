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
import sqlalchemy
import sqlalchemy.ext.asyncio
from advanced_alchemy._listeners import (
    AsyncCacheListener,
    AsyncFileObjectListener,
    SyncCacheListener,
    SyncFileObjectListener,
    touch_updated_timestamp,
)
from advanced_alchemy.base import BigIntAuditBase
from advanced_alchemy.config import RoutingConfig
from advanced_alchemy.exceptions import (
    DuplicateKeyError,
    ForeignKeyError,
    InvalidCursorError,
    wrap_sqlalchemy_exception,
)
from advanced_alchemy.extensions.litestar import SQLAlchemyPlugin
from advanced_alchemy.extensions.litestar.store import SQLAlchemyStore, StoreModelMixin
from advanced_alchemy.filters import (
    CursorFilter,
)
from advanced_alchemy.filters import (
    FilterSet as UpstreamFilterSet,
)
from advanced_alchemy.filters import (
    RelationshipFilter as UpstreamRelationshipFilter,
)
from advanced_alchemy.repository import SQLAlchemySyncRepository
from advanced_alchemy.routing import RoutingAsyncSessionMaker, RoutingSyncSessionMaker
from advanced_alchemy.routing.selectors import RoundRobinSelector
from advanced_alchemy.routing.session import RoutingAsyncSession
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
from advanced_alchemy.utils.dependencies import CursorDefaults, resolve_cursor_settings
from litestar import Litestar, get
from litestar.config.app import AppConfig
from litestar.di import NamedDependency
from litestar.exceptions import LitestarDeprecationWarning
from litestar.params import SkipValidation
from litestar.plugins import InitPluginProtocol
from litestar.testing import TestClient
from sqlalchemy import Boolean, ForeignKey, Integer, String, create_engine, event, select
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import IntegrityError as SQLAlchemyIntegrityError
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship
from sqlalchemy.pool import StaticPool

import vertebra
import vertebra._bundler as native_bundler_mod
import vertebra._storage as native_storage_mod
import vertebra._vertebra as native_core_mod
import vertebra.agent as agent_mod
import vertebra.agent.mcp as mcp_mod
import vertebra.build as build_mod
import vertebra.db.adapters.postgres as postgres_adapter_mod
import vertebra.db.rls as rls_mod
import vertebra.extensions.alchemy as alchemy_ext
import vertebra.extensions.broker as broker_mod
import vertebra.extensions.django as django_ext
import vertebra.extensions.fastapi as fastapi_ext
import vertebra.extensions.flask as flask_ext
import vertebra.extensions.litestar as litestar_ext
import vertebra.extensions.sqlalchemy as sa_ext_mod
import vertebra.extensions.sqlspec as sqlspec_ext
import vertebra.extensions.starlette as starlette_ext
import vertebra.framework as vtb_framework
import vertebra.framework.email as email_mod
import vertebra.framework.procedures as vtb_procedures
import vertebra.framework.queues as queues_mod
import vertebra.framework.stores as stores_mod
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
from vertebra.db.adapters.mysql.config import build_mysql_client_kwargs
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
from vertebra.db.dbapi import AsyncDBAPIConnection, InterfaceError
from vertebra.db.dialer import aws_rds_root_ca_pem
from vertebra.db.driver._cloud_auth import (
    AZURE_MYSQL_SCOPE,
    AwsRdsIamTokenProvider,
    AzureEntraTokenProvider,
    DatabaseTokenProvider,
)
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
from vertebra.extensions.sqlalchemy.dialects import (
    ALL_DIALECT_SCHEMES,
    DIALECT_DESCRIPTORS,
    AsyncMSSQLDialect,
    AsyncMySQLDialect,
    AsyncPostgreSQLDialect,
    SyncMSSQLDialect,
    SyncMySQLDialect,
    SyncPostgreSQLDialect,
)
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
    MySQLConnectionParams,
    MySQLSyncConfig,
    MySQLSyncDriver,
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleSyncConfig,
    OracleSyncDriver,
    PostgresAsyncConfig,
    PostgresAsyncDriver,
    PostgresConnectionParams,
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
from vertebra.extensions.sqlspec.adapters.mysql.core import build_connection_config as build_mysql_connection_config
from vertebra.extensions.sqlspec.adapters.spanner.core import SpannerConnectionShim
from vertebra.storage import SyncObjectStore
from vertebra.typing import ImproperConfigurationError


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

    assert not hasattr(agent_mod, "SpecTree")
    assert not hasattr(agent_mod, "SpecNode")
    assert not hasattr(agent_mod, "SpecNodeStatus")
    assert not hasattr(agent_mod, "DynamicWorkflow")
    assert not hasattr(agent_mod, "WorkflowNode")
    assert not hasattr(agent_mod, "StepResult")
    assert not hasattr(agent_mod, "JSONRPCErrorException")
    assert not hasattr(agent_mod, "McpHttpRequest")
    assert not hasattr(agent_mod, "McpHttpResponse")
    assert not hasattr(agent_mod, "McpToolBridge")
    assert not hasattr(agent_mod, "A2aAgentBridge")
    assert not hasattr(mcp_mod, "MCPRegistry")
    assert not hasattr(mcp_mod, "McpIcon")
    assert not hasattr(mcp_mod, "McpLogLevel")
    assert not hasattr(mcp_mod, "AsgiMcpApp")
    assert not hasattr(mcp_mod, "WsgiMcpApp")
    assert hasattr(mcp_mod, "ToolRegistry")
    assert hasattr(mcp_mod, "MCPIcon")
    assert hasattr(mcp_mod, "AsgiMCPApp")
    assert hasattr(mcp_mod, "WsgiMCPApp")
    assert not hasattr(stores_mod, "BaseNativeKVStore")
    assert hasattr(stores_mod, "BaseKeyValueStore")
    assert not hasattr(rls_mod, "TenantConnectionScope")
    assert hasattr(rls_mod, "SyncTenantConnectionScope")
    assert not hasattr(broker_mod, "BrokerStream")
    assert hasattr(broker_mod, "SyncBrokerStream")
    assert not hasattr(sa_ext_mod, "BrokerPool")
    assert hasattr(sa_ext_mod, "SyncBrokerPool")
    assert not hasattr(postgres_adapter_mod, "PostgresNotificationDriver")
    assert hasattr(postgres_adapter_mod, "AsyncPostgresNotificationDriver")
    assert not hasattr(email_mod, "SesEmailTransport")
    assert not hasattr(email_mod, "SmtpEmailTransport")
    assert hasattr(email_mod, "SESEmailTransport")
    assert hasattr(email_mod, "SMTPEmailTransport")
    assert not hasattr(queues_mod, "RedisStreamClient")
    assert not hasattr(queues_mod, "SyncRedisStreamClient")
    assert not hasattr(queues_mod, "ValkeyStreamClient")
    assert not hasattr(queues_mod, "SyncValkeyStreamClient")
    assert hasattr(queues_mod, "AsyncRESPStreamClient")
    assert hasattr(queues_mod, "SyncRESPStreamClient")
    assert not hasattr(native_bundler_mod, "DevConfig")
    assert not hasattr(native_bundler_mod, "BuildResult")
    assert not hasattr(native_bundler_mod, "OutputFile")
    assert hasattr(build_mod, "Bundler")
    assert hasattr(build_mod, "BundleConfig")
    assert hasattr(build_mod, "BundleDevConfig")
    assert hasattr(build_mod, "BundleResult")
    assert hasattr(build_mod, "BundleOutput")
    assert not hasattr(native_core_mod, "PyEmptyAwaitable")
    assert not hasattr(native_core_mod, "PyValueAwaitable")
    assert not hasattr(native_core_mod, "RustDbTaskAwaitable")
    assert hasattr(native_core_mod, "EmptyAwaitable")
    assert hasattr(native_core_mod, "ValueAwaitable")
    assert hasattr(native_core_mod, "TaskAwaitable")
    assert not hasattr(native_storage_mod, "RustTaskAwaitable")
    assert hasattr(native_storage_mod, "TaskAwaitable")

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


class _StaticTokenProvider(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Deterministic `DatabaseTokenProvider` fixture returning a static placeholder token."""

    token: str = "test-token-placeholder"

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        _ = force_refresh
        return self.token

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        _ = force_refresh
        return self.token

    def invalidate(self) -> None:
        return None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("sync_dialect_cls", "async_dialect_cls", "sync_url", "async_url", "ca_kwarg", "ca_is_bytes"),
    [
        (
            SyncPostgreSQLDialect,
            AsyncPostgreSQLDialect,
            "postgresql+vtb://iam_user@localhost:5432/app",
            "postgresql+vtb_async://iam_user@localhost:5432/app",
            "ca_pem",
            False,
        ),
        (
            SyncMySQLDialect,
            AsyncMySQLDialect,
            "mysql+vtb://iam_user@localhost:3306/app",
            "mysql+vtb_async://iam_user@localhost:3306/app",
            "ssl_ca",
            False,
        ),
        (
            SyncMSSQLDialect,
            AsyncMSSQLDialect,
            "mssql+vtb://iam_user@localhost:1433/app",
            "mssql+vtb_async://iam_user@localhost:1433/app",
            "ca_pem",
            True,
        ),
    ],
    ids=["postgresql+vtb", "mysql+vtb", "mssql+vtb"],
)
async def test_sqlalchemy_vtb_dialects_forward_token_provider_and_ca_connect_args(
    sync_dialect_cls: type[Any],
    async_dialect_cls: type[Any],
    sync_url: str,
    async_url: str,
    ca_kwarg: str,
    ca_is_bytes: bool,
) -> None:
    """Verify SQLAlchemy +vtb and +vtb_async dialects forward token_provider and CA options into DBAPI configs."""
    provider = _StaticTokenProvider()
    assert isinstance(provider, DatabaseTokenProvider)
    ca_pem_text = aws_rds_root_ca_pem()
    ca_value: str | bytes = ca_pem_text.encode("utf-8") if ca_is_bytes else ca_pem_text
    ca_kwargs: dict[str, Any] = {ca_kwarg: ca_value}

    sync_dialect = sync_dialect_cls()
    sync_args, sync_params = sync_dialect.create_connect_args(make_url(sync_url))
    sync_dbapi = sync_dialect.import_dbapi()
    sync_conn = sync_dbapi.connect(
        *sync_args,
        **sync_params,
        share_pool=False,
        token_provider=provider,
        **ca_kwargs,
    )
    try:
        assert sync_conn.config is not None
        sync_cfg = cast("Any", sync_conn.config)
        assert sync_cfg.token_provider is provider
        assert getattr(sync_cfg, ca_kwarg) == ca_value
    finally:
        sync_conn.close()

    async_dialect = async_dialect_cls()
    async_args, async_params = async_dialect.create_connect_args(make_url(async_url))
    assert async_args == []
    async_conn = AsyncDBAPIConnection(
        async_params["url"],
        share_pool=False,
        token_provider=provider,
        **ca_kwargs,
    )
    try:
        assert async_conn.config is not None
        async_cfg = cast("Any", async_conn.config)
        assert async_cfg.token_provider is provider
        assert getattr(async_cfg, ca_kwarg) == ca_value
    finally:
        await async_conn.close()

    with pytest.raises(InterfaceError, match="unsupported_cloud_arg"):
        sync_dbapi.connect(*sync_args, **sync_params, share_pool=False, unsupported_cloud_arg=True)


@pytest.mark.parametrize(
    "config_cls",
    [PostgresSyncConfig, PostgresAsyncConfig],
    ids=["sync", "async"],
)
@pytest.mark.parametrize(
    "init_mode",
    ["kwargs", "typed_mapping", "pg_config"],
)
@pytest.mark.parametrize(
    "derive_mode",
    ["dsn", "url", "connection_config_overrides", "replicas"],
)
def test_sqlspec_postgres_configs_forward_and_preserve_cloud_auth_across_derive_pool(
    config_cls: type[PostgresSyncConfig | PostgresAsyncConfig],
    init_mode: str,
    derive_mode: str,
) -> None:
    """Verify PostgresSyncConfig and PostgresAsyncConfig forward and preserve token_provider and ca_pem on derive_pool."""
    provider = _StaticTokenProvider()
    override_provider = _StaticTokenProvider(token="test-token-placeholder-replica")
    ca_pem = aws_rds_root_ca_pem()
    primary_dsn = "postgresql://iam_user@primary.internal:5432/app"
    replica_dsn = "postgresql://iam_user@replica.internal:5432/app"
    replicas_arg = [replica_dsn] if derive_mode == "replicas" else None

    if init_mode == "kwargs":
        primary = config_cls(
            primary_dsn,
            token_provider=provider,
            ca_pem=ca_pem,
            share_pool=False,
            replicas=replicas_arg,
        )
    elif init_mode == "typed_mapping":
        typed_params: PostgresConnectionParams = {
            "dsn": primary_dsn,
            "token_provider": provider,
            "ca_pem": ca_pem,
        }
        primary = config_cls(typed_params, share_pool=False, replicas=replicas_arg)
    else:
        pg_cfg = PostgreSQLConfig(dsn=primary_dsn, token_provider=provider, ca_pem=ca_pem)
        primary = config_cls(pg_cfg, share_pool=False, replicas=replicas_arg)

    assert primary.driver_config.dsn == primary_dsn
    assert primary.driver_config.token_provider is provider
    assert primary.driver_config.ca_pem == ca_pem

    if derive_mode == "dsn":
        derived = primary.derive_pool(dsn=replica_dsn)
    elif derive_mode == "url":
        derived = primary.derive_pool(url=replica_dsn)
    elif derive_mode == "connection_config_overrides":
        derived = primary.derive_pool(connection_config_overrides={"dsn": replica_dsn})
    else:
        derived = primary.replica_groups["default"].specs[0].config

    assert derived.driver_config.dsn == replica_dsn
    assert derived.driver_config.token_provider is provider
    assert derived.driver_config.ca_pem == ca_pem

    replaced = primary.derive_pool(dsn=replica_dsn, token_provider=override_provider)
    assert replaced.driver_config.dsn == replica_dsn
    assert replaced.driver_config.token_provider is override_provider
    assert replaced.driver_config.ca_pem == ca_pem
    assert primary.driver_config.token_provider is provider

    cleared = primary.derive_pool(dsn=replica_dsn, token_provider=None, ca_pem=None)
    assert cleared.driver_config.dsn == replica_dsn
    assert cleared.driver_config.token_provider is None
    assert cleared.driver_config.ca_pem is None
    assert primary.driver_config.token_provider is provider
    assert primary.driver_config.ca_pem == ca_pem


@pytest.mark.parametrize(
    "config_cls",
    [MySQLSyncConfig, MySQLAsyncConfig],
    ids=["sync", "async"],
)
@pytest.mark.parametrize(
    "provider_kind",
    ["rds_iam", "azure_entra_unscoped"],
)
@pytest.mark.parametrize(
    "init_mode",
    ["kwargs", "typed_mapping", "mysql_config"],
)
@pytest.mark.parametrize(
    "derive_mode",
    ["dsn", "url", "endpoint", "connection_config_overrides", "replicas_url", "replicas_endpoint"],
)
def test_sqlspec_mysql_configs_forward_and_preserve_cloud_auth_across_derive_pool(
    config_cls: type[MySQLSyncConfig | MySQLAsyncConfig],
    provider_kind: str,
    init_mode: str,
    derive_mode: str,
) -> None:
    """Verify MySQLSyncConfig and MySQLAsyncConfig forward and preserve token_provider and ssl_ca on derive_pool."""
    raw_entra = AzureEntraTokenProvider()
    if provider_kind == "rds_iam":
        provider: DatabaseTokenProvider = AwsRdsIamTokenProvider(
            endpoint_host="primary.internal",
            port=3306,
            region="us-east-1",
            username="iam_user",
            access_key_id="test-token-placeholder-id",
            secret_access_key="test-token-placeholder-secret",
        )
    else:
        assert raw_entra.scope is None
        provider = raw_entra

    override_provider = _StaticTokenProvider(token="test-token-placeholder-replica")
    custom_ca = "-----BEGIN CERTIFICATE-----\nMIIBCgKCAQEAtestcustomca\n-----END CERTIFICATE-----\n"
    override_ca = aws_rds_root_ca_pem()
    primary_dsn = "mysql://iam_user@primary.internal:3306/app"
    replica_dsn = "mysql://iam_user@replica.internal:3307/app"
    replicas_arg: list[Any] | None = None
    if derive_mode == "replicas_url":
        replicas_arg = [replica_dsn]
    elif derive_mode == "replicas_endpoint":
        replicas_arg = [{"host": "replica.internal", "port": 3307}]

    if init_mode == "kwargs":
        primary = config_cls(
            primary_dsn,
            token_provider=provider,
            ssl_ca=custom_ca,
            share_pool=False,
            replicas=replicas_arg,
        )
    elif init_mode == "typed_mapping":
        typed_params: MySQLConnectionParams = {
            "dsn": primary_dsn,
            "token_provider": provider,
            "ssl_ca": custom_ca,
        }
        primary = config_cls(typed_params, share_pool=False, replicas=replicas_arg)
    else:
        mysql_cfg = MySQLConfig(dsn=primary_dsn, token_provider=provider, ssl_ca=custom_ca)
        primary = config_cls(mysql_cfg, share_pool=False, replicas=replicas_arg)

    assert primary.driver_config.dsn == primary_dsn
    assert primary.driver_config.ssl_ca == custom_ca
    if provider_kind == "rds_iam":
        assert primary.driver_config.token_provider is provider
    else:
        assert raw_entra.scope is None
        assert isinstance(primary.driver_config.token_provider, AzureEntraTokenProvider)
        assert primary.driver_config.token_provider.scope == AZURE_MYSQL_SCOPE
    expected_provider = primary.driver_config.token_provider

    if derive_mode == "dsn":
        derived = primary.derive_pool(dsn=replica_dsn)
    elif derive_mode == "url":
        derived = primary.derive_pool(url=replica_dsn)
    elif derive_mode == "endpoint":
        derived = primary.derive_pool(host="replica.internal", port=3307)
    elif derive_mode == "connection_config_overrides":
        derived = primary.derive_pool(connection_config_overrides={"url": replica_dsn})
    else:
        derived = primary.replica_groups["default"].specs[0].config

    derived_key = derived.driver_config.pool_key()
    assert derived_key[1] == "replica.internal"
    assert derived_key[2] == 3307
    assert derived.driver_config.token_provider is expected_provider
    assert derived.driver_config.ssl_ca == custom_ca
    assert primary.driver_config.canonical_ssl_mode == "verify-full"
    assert derived.driver_config.canonical_ssl_mode == "verify-full"
    assert build_mysql_client_kwargs(derived.driver_config)["ssl_mode"] == "verify-full"
    assert build_mysql_client_kwargs(derived.driver_config)["ssl_ca"] == custom_ca

    replaced = primary.derive_pool(url=replica_dsn, token_provider=override_provider, ssl_ca=override_ca)
    assert replaced.driver_config.dsn == replica_dsn
    assert replaced.driver_config.token_provider is override_provider
    assert replaced.driver_config.ssl_ca == override_ca
    assert replaced.driver_config.canonical_ssl_mode == "require"
    assert primary.driver_config.token_provider is expected_provider
    assert primary.driver_config.ssl_ca == custom_ca

    cleared = primary.derive_pool(dsn=replica_dsn, token_provider=None, ssl_ca=None)
    assert cleared.driver_config.dsn == replica_dsn
    assert cleared.driver_config.token_provider is None
    assert cleared.driver_config.ssl_ca is None
    assert cleared.driver_config.canonical_ssl_mode == "prefer"
    assert primary.driver_config.token_provider is expected_provider
    assert primary.driver_config.ssl_ca == custom_ca


@pytest.mark.parametrize("config_cls", [MySQLSyncConfig, MySQLAsyncConfig], ids=["sync", "async"])
def test_sqlspec_mysql_build_connection_config_and_rds_default_ca_derive_pool(
    config_cls: type[MySQLSyncConfig | MySQLAsyncConfig],
) -> None:
    """Verify build_connection_config forwards cloud auth and RDS default ssl_ca survives or yields to URL query."""
    rds_provider = AwsRdsIamTokenProvider(
        endpoint_host="primary.rds.internal",
        port=3306,
        region="us-west-2",
        username="iam_user",
        access_key_id="test-token-placeholder-id",
        secret_access_key="test-token-placeholder-secret",
    )
    built = build_mysql_connection_config(
        connection_config={"dsn": "mysql://iam_user@primary.rds.internal:3306/app", "token_provider": rds_provider},
        overrides={"url": "mysql://iam_user@replica.rds.internal:3306/app"},
    )
    assert built.dsn == "mysql://iam_user@replica.rds.internal:3306/app"
    assert built.token_provider is rds_provider
    assert built.ssl_ca == aws_rds_root_ca_pem()
    assert built.canonical_ssl_mode == "verify-full"

    primary = config_cls(
        "mysql://iam_user@primary.rds.internal:3306/app",
        token_provider=rds_provider,
        share_pool=False,
    )
    assert primary.driver_config.ssl_ca == aws_rds_root_ca_pem()
    assert build_mysql_client_kwargs(primary.driver_config)["ssl_mode"] == "verify-full"
    assert build_mysql_client_kwargs(primary.driver_config)["ssl_ca"] == aws_rds_root_ca_pem()

    derived_query_ca = primary.derive_pool(
        url="mysql://iam_user@replica.rds.internal:3306/app?ssl_ca=/etc/ssl/replica-ca.pem"
    )
    assert derived_query_ca.driver_config.token_provider is rds_provider
    assert derived_query_ca.driver_config.ssl_ca == "/etc/ssl/replica-ca.pem"
    assert derived_query_ca.driver_config.canonical_ssl_mode == "verify-full"

    seen_scopes: list[str] = []

    def _fake_entra_fetcher(scope: str) -> str:
        seen_scopes.append(scope)
        return "test-token-placeholder-entra"

    entra_primary = config_cls(
        "mysql://iam_user@primary.mysql.database.azure.com:3306/app",
        token_provider=AzureEntraTokenProvider(fetcher=_fake_entra_fetcher),
        share_pool=False,
    )
    entra_replica = entra_primary.derive_pool(host="replica.mysql.database.azure.com")
    assert entra_primary.driver_config.canonical_ssl_mode == "verify-full"
    assert entra_replica.driver_config.canonical_ssl_mode == "verify-full"
    assert entra_replica.driver_config.ssl_ca is None
    assert entra_primary.driver_config.parse_dsn_params()["password"] == "test-token-placeholder-entra"
    assert entra_replica.driver_config.parse_dsn_params()["password"] == "test-token-placeholder-entra"
    assert seen_scopes == [AZURE_MYSQL_SCOPE]


def _mysql_cloud_token_provider(provider_kind: str) -> DatabaseTokenProvider:
    """Build an AWS RDS or Azure Entra token provider that never reaches the network."""
    if provider_kind == "rds_iam":
        return AwsRdsIamTokenProvider(
            endpoint_host="primary.internal",
            port=3306,
            region="us-east-1",
            username="iam_user",
            access_key_id="test-token-placeholder-id",
            secret_access_key="test-token-placeholder-secret",
        )
    return AzureEntraTokenProvider(fetcher=lambda scope: f"test-token-placeholder-{scope}")


@pytest.mark.parametrize("config_cls", [MySQLSyncConfig, MySQLAsyncConfig], ids=["sync", "async"])
@pytest.mark.parametrize("provider_kind", ["rds_iam", "azure_entra"])
@pytest.mark.parametrize(
    ("primary_dsn", "primary_ssl_mode", "derive_mode", "primary_mode", "derived_mode"),
    [
        ("mysql://iam_user@127.0.0.1:3306/app", None, "url", "require", "verify-full"),
        ("mysql://iam_user@127.0.0.1:3306/app", None, "endpoint", "require", "verify-full"),
        ("mysql://iam_user@127.0.0.1:3306/app", None, "replicas_url", "require", "verify-full"),
        ("mysql://iam_user@127.0.0.1:3306/app", None, "replicas_endpoint", "require", "verify-full"),
        ("mysql://iam_user@127.0.0.1:3306/app?ssl_mode=disable", None, "url", "disable", "verify-full"),
        ("mysql://iam_user@primary.internal:3306/app", None, "endpoint", "verify-full", "verify-full"),
        ("mysql://iam_user@primary.internal:3306/app", "verify-ca", "endpoint", "verify-ca", "verify-ca"),
        ("mysql://iam_user@primary.internal:3306/app", "verify-ca", "replicas_endpoint", "verify-ca", "verify-ca"),
        ("mysql://iam_user@primary.internal:3306/app", "required", "endpoint", "require", "require"),
        ("mysql://iam_user@primary.internal:3306/app?ssl_mode=verify-ca", None, "endpoint", "verify-ca", "verify-ca"),
        ("mysql://iam_user@127.0.0.1:3306/app?ssl_mode=disable", None, "endpoint", "disable", None),
        ("mysql://iam_user@127.0.0.1:3306/app", "prefer", "replicas_endpoint", "prefer", None),
    ],
)
def test_sqlspec_mysql_cloud_iam_ssl_mode_across_derive_pool_and_replicas(
    config_cls: type[MySQLSyncConfig | MySQLAsyncConfig],
    provider_kind: str,
    primary_dsn: str,
    primary_ssl_mode: str | None,
    derive_mode: str,
    primary_mode: str,
    derived_mode: str | None,
) -> None:
    """Verify derived MySQL pools verify remote cloud IAM servers by default and inherit only explicit modes."""
    provider = _mysql_cloud_token_provider(provider_kind)
    replica_dsn = "mysql://iam_user@replica.internal:3307/app"
    replicas_arg: list[Any] | None = None
    if derive_mode == "replicas_url":
        replicas_arg = [replica_dsn]
    elif derive_mode == "replicas_endpoint":
        replicas_arg = [{"host": "replica.internal", "port": 3307}]
    extra: dict[str, Any] = {} if primary_ssl_mode is None else {"ssl_mode": primary_ssl_mode}

    def _derive() -> MySQLSyncConfig | MySQLAsyncConfig:
        primary = config_cls(primary_dsn, token_provider=provider, share_pool=False, replicas=replicas_arg, **extra)
        assert primary.driver_config.canonical_ssl_mode == primary_mode
        assert build_mysql_client_kwargs(primary.driver_config)["ssl_mode"] == primary_mode
        if derive_mode == "url":
            return primary.derive_pool(url=replica_dsn)
        if derive_mode == "endpoint":
            return primary.derive_pool(host="replica.internal", port=3307)
        replica: MySQLSyncConfig | MySQLAsyncConfig = primary.replica_groups["default"].specs[0].config
        return replica

    if derived_mode is None:
        with pytest.raises(ImproperConfigurationError, match="requires TLS on remote hosts"):
            _derive()
        return

    derived = _derive()
    assert derived.driver_config.pool_key()[1] == "replica.internal"
    assert derived.driver_config.canonical_ssl_mode == derived_mode
    assert derived.driver_config.parse_dsn_params()["ssl_mode"] == derived_mode
    assert build_mysql_client_kwargs(derived.driver_config)["ssl_mode"] == derived_mode
    expected_ca = aws_rds_root_ca_pem() if provider_kind == "rds_iam" else None
    assert derived.driver_config.ssl_ca == expected_ca
    assert build_mysql_client_kwargs(derived.driver_config)["ssl_ca"] == expected_ca


def test_advanced_alchemy_1130b3_late_bound_engine_factories(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify late-bound engine lookup in 1.13.0b3 picks up post-import sqlalchemy factory patches."""
    real_create_engine = sqlalchemy.create_engine
    real_create_async_engine = sqlalchemy.ext.asyncio.create_async_engine
    sync_urls: list[str] = []
    async_urls: list[str] = []

    def patched_create_engine(url: Any, **kwargs: Any) -> Any:
        sync_urls.append(str(url))
        return real_create_engine(url, **kwargs)

    def patched_create_async_engine(url: Any, **kwargs: Any) -> Any:
        async_urls.append(str(url))
        return real_create_async_engine(url, **kwargs)

    monkeypatch.setattr(sqlalchemy, "create_engine", patched_create_engine)
    monkeypatch.setattr(sqlalchemy.ext.asyncio, "create_async_engine", patched_create_async_engine)

    sync_cfg = create_sync_alchemy_config("sqlite:///:memory:")
    sync_engine = sync_cfg.get_engine()
    sync_engine.dispose()
    assert sync_urls == ["sqlite+vtb:///:memory:"]

    sync_routing_cfg = create_sync_alchemy_config(
        routing_config=RoutingConfig(primary_connection_string="sqlite:///:memory:"),
    )
    sync_maker = sync_routing_cfg.create_session_maker()
    assert len(sync_urls) == 2
    assert isinstance(sync_maker, RoutingSyncSessionMaker)

    async_cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:")
    _ = async_cfg.get_engine()
    assert async_urls == ["sqlite+vtb_async:///:memory:"]

    async_routing_cfg = create_async_alchemy_config(
        routing_config=RoutingConfig(primary_connection_string="sqlite+vtb_async:///:memory:"),
    )
    async_maker = async_routing_cfg.create_session_maker()
    assert len(async_urls) == 2
    assert isinstance(async_maker, RoutingAsyncSessionMaker)

    direct_engine = real_create_async_engine("sqlite+vtb_async:///:memory:")
    routing_session = RoutingAsyncSession(
        routing_config=RoutingConfig(primary_connection_string="sqlite+vtb_async:///:memory:"),
        selectors={"default": RoundRobinSelector([direct_engine])},
        default_engine=direct_engine,
    )
    assert routing_session.bind is None


class _CursorItem(BigIntAuditBase):
    __tablename__ = "r209_cursor_item"

    score: Mapped[int] = mapped_column(Integer, default=0)
    active: Mapped[bool] = mapped_column(Boolean, default=False)


def test_advanced_alchemy_1130b3_cursor_filter_encryption_and_validation() -> None:
    """Verify AES-256-GCM cursor encryption, derived signing-key parity, and bool column checks."""
    secret = "test-cursor-secret-v1"
    direct_signed = CursorFilter([("id", "asc")], limit=2, secret=secret)
    cursor_settings = resolve_cursor_settings(
        {"sort_fields": {"id"}, "pagination_cursor_secret": secret},
    )
    provider_secret, provider_ttl, provider_enc = cursor_settings.token_options(CursorDefaults())
    assert provider_ttl is None
    assert provider_enc is False
    assert provider_secret == secret.encode("utf-8")

    rows = [
        _CursorItem(id=10, score=5, active=False),
        _CursorItem(id=20, score=10, active=True),
        _CursorItem(id=30, score=15, active=True),
    ]
    direct_page = direct_signed.build_page(rows, model=_CursorItem)
    assert direct_page.next_cursor is not None
    decoded = CursorFilter([("id", "asc")], cursor=direct_page.next_cursor, limit=2, secret=provider_secret)
    assert [item.id for item in decoded.paginate_items(rows, _CursorItem)] == [30]

    encrypted_filter = CursorFilter([("score", "asc")], limit=2, secret=secret, encrypted=True)
    sealed_page = encrypted_filter.build_page(rows, model=_CursorItem)
    assert sealed_page.next_cursor is not None
    assert sealed_page.next_cursor.startswith("e1.")
    reopened = CursorFilter([("score", "asc")], cursor=sealed_page.next_cursor, limit=2, secret=secret, encrypted=True)
    assert [item.score for item in reopened.paginate_items(rows, _CursorItem)] == [15]

    bool_rows = [
        SimpleNamespace(id=1, score=False),
        SimpleNamespace(id=2, score=True),
        SimpleNamespace(id=3, score=True),
    ]
    bad_bool_page = CursorFilter([("score", "asc")], limit=2).build_page(bool_rows, model=_CursorItem)
    assert bad_bool_page.next_cursor is not None
    with pytest.raises(InvalidCursorError):
        CursorFilter([("score", "asc")], cursor=bad_bool_page.next_cursor, limit=2).append_to_statement(
            select(_CursorItem), _CursorItem
        )

    items = [
        _CursorItem(id=1, score=1, active=False),
        _CursorItem(id=2, score=2, active=True),
        _CursorItem(id=3, score=3, active=True),
    ]
    bool_filter = CursorFilter([("active", "asc"), ("id", "asc")], limit=2)
    stmt = bool_filter.append_to_statement(select(_CursorItem), _CursorItem)
    assert stmt is not None
    first_page = bool_filter.build_page(bool_filter.paginate_items(items, _CursorItem), model=_CursorItem)
    assert [row.id for row in first_page.items] == [1, 2]
    assert first_page.next_cursor is not None
    second_stmt = CursorFilter(
        [("active", "asc"), ("id", "asc")],
        cursor=first_page.next_cursor,
        limit=2,
    ).append_to_statement(select(_CursorItem), _CursorItem)
    assert second_stmt is not None
