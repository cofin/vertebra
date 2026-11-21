"""Unit tests for native database drivers, Cloud SQL / AlloyDB dialers, ChannelBus, and ArrowCursor."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import importlib
import sys
import threading
import time
import tomllib
from collections.abc import AsyncIterator, Callable, Iterator
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlsplit

import pytest
from anyio import Event, create_task_group

import vertebra
import vertebra._vertebra as native_vertebra
import vertebra.db as vertebra_db
import vertebra.db.driver as vertebra_db_driver
from tests.unit.db.test_db_capabilities_and_purity_gates import (
    _ConformingAsyncChangeSource,
    _ConformingNotificationSource,
)
from vertebra.db import (
    AlloyDBAuthType,
    AlloyDBClusterURI,
    AlloyDBConfig,
    ArrowCursor,
    AsyncAlloyDBDialer,
    AsyncCloudSQLDialer,
    CDCConfig,
    ChangeEvent,
    ChangeFeed,
    CloudSQLConfig,
    CloudSQLInstanceURI,
    DatabaseRegistry,
    DataError,
    OperationalError,
    SyncAlloyDBDialer,
    SyncCloudSQLDialer,
)
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, SyncBigQueryDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.notify import AsyncPostgresNotificationSource
from vertebra.db.adapters.redis import RedisConfig, SyncRedisDriver
from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    SpannerConfig,
    SyncSpannerDriver,
)
from vertebra.db.adapters.spanner.change_stream import AsyncSpannerChangeStreamSource
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.adapters.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
from vertebra.db.arrow import read_arrow_shm, write_arrow_shm
from vertebra.db.driver import (
    AZURE_POSTGRES_SCOPE,
    AwsAuroraDsqlIamTokenProvider,
    AwsRdsIamTokenProvider,
    AzureEntraTokenProvider,
)
from vertebra.db.driver._cloud_auth import is_loopback_host
from vertebra.db.driver.capabilities import (
    NotificationEvent,
)
from vertebra.extensions.litestar import ChannelsBackend, MemoryStore
from vertebra.framework.channels import ChannelBus
from vertebra.server._channels import ChannelBus as ServerChannelBus
from vertebra.storage import GcpTokenSnapshot
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, VertebraError, reset_dependency_cache
from vertebra.utils.serializers import (
    decode_json,
    encode_json_bytes,
)
from vertebra.utils.sync_tools import SyncPool

if TYPE_CHECKING:
    from tests.unit.conftest import Resp3FakeServer


def test_sync_transaction_rolls_back_base_exception() -> None:
    """An interrupted transaction discards its writes and permits the next transaction."""
    with SyncSQLiteDriver() as driver:
        driver.execute("CREATE TABLE interrupted (value INTEGER)")
        with pytest.raises(KeyboardInterrupt), driver.transaction():
            driver.execute("INSERT INTO interrupted VALUES (?)", (1,))
            raise KeyboardInterrupt
        assert driver.select_value("SELECT COUNT(*) FROM interrupted") == 0
        with driver.transaction():
            driver.execute("INSERT INTO interrupted VALUES (?)", (2,))
        assert driver.execute_tuples("SELECT value FROM interrupted") == [(2,)]


@pytest.mark.anyio
async def test_async_transaction_rolls_back_cancelled_task() -> None:
    """Cancelling a task rolls back its transaction before another task reuses the driver."""
    async with AsyncSQLiteDriver() as driver:
        await driver.execute("CREATE TABLE interrupted (value INTEGER)")
        inserted = asyncio.Event()

        async def insert_until_cancelled() -> None:
            async with driver.transaction():
                await driver.execute("INSERT INTO interrupted VALUES (?)", (1,))
                inserted.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(insert_until_cancelled())
        await inserted.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await driver.select_value("SELECT COUNT(*) FROM interrupted") == 0
        async with driver.transaction():
            await driver.execute("INSERT INTO interrupted VALUES (?)", (2,))
        assert await driver.execute_tuples("SELECT value FROM interrupted") == [(2,)]


@pytest.mark.anyio
async def test_async_transaction_rolls_back_anyio_cancel_scope() -> None:
    """Level cancellation cannot interrupt rollback before another transaction starts."""
    async with AsyncSQLiteDriver() as driver:
        await driver.execute("CREATE TABLE interrupted (value INTEGER)")
        inserted = Event()

        async def insert_until_cancelled() -> None:
            async with driver.transaction():
                await driver.execute("INSERT INTO interrupted VALUES (?)", (1,))
                inserted.set()
                await Event().wait()

        async with create_task_group() as group:
            group.start_soon(insert_until_cancelled)
            await inserted.wait()
            group.cancel_scope.cancel()
        assert await driver.select_value("SELECT COUNT(*) FROM interrupted") == 0
        async with driver.transaction():
            await driver.execute("INSERT INTO interrupted VALUES (?)", (2,))
        assert await driver.execute_tuples("SELECT value FROM interrupted") == [(2,)]


_ADAPTER_ENGINES: tuple[str, ...] = (
    "postgres",
    "mysql",
    "sqlite",
    "oracle",
    "mssql",
    "duckdb",
    "bigquery",
    "spanner",
    "redis",
    "valkey",
    "datafusion",
)

_EXPECTED_DB_ERROR_DEFAULTS: tuple[tuple[str, str | None, int | None, bool], ...] = (
    ("DatabaseError", None, None, False),
    ("InterfaceError", None, None, False),
    ("DataError", None, None, False),
    ("NotFoundError", None, None, False),
    ("OperationalError", None, None, False),
    ("ConnectionFailure", None, None, True),
    ("PoolExhaustedError", None, None, True),
    ("QueryCancelledError", "57014", None, False),
    ("SerializationFailure", "40001", None, True),
    ("IntegrityError", None, None, False),
    ("InternalError", None, None, False),
    ("ProgrammingError", None, None, False),
    ("NotSupportedError", None, None, False),
)

_EXPECTED_DB_EXPORTS: frozenset[str] = frozenset(
    {
        "ARROW_SHM_THRESHOLD_ROWS",
        "AlloyDBAuthType",
        "AlloyDBClusterURI",
        "AlloyDBConfig",
        "ArrowCursor",
        "AsyncAlloyDBDialer",
        "AsyncAwsAuroraDsqlDialer",
        "AsyncAwsRdsDialer",
        "AsyncAzureEntraDialer",
        "AsyncChangeSource",
        "AsyncCloudSQLDialer",
        "AsyncDatabaseDriver",
        "AsyncPipeline",
        "AsyncPipelineCapable",
        "AsyncQueueStore",
        "BaseDatabaseConfig",
        "CDCConfig",
        "ChangeEvent",
        "ChangeFeed",
        "CloudSQLConfig",
        "CloudSQLInstanceURI",
        "CommonDriverMixin",
        "ConnectionFailure",
        "DataError",
        "DatabaseError",
        "DatabaseRegistry",
        "EmbeddingPushdown",
        "IntegrityError",
        "InterfaceError",
        "InternalError",
        "NotFoundError",
        "NotSupportedError",
        "NotificationEvent",
        "AsyncNotificationSource",
        "SyncNotificationSource",
        "OperationalError",
        "PoolExhaustedError",
        "ProgrammingError",
        "QueryCancelledError",
        "QueueEntry",
        "SerializationFailure",
        "SyncAlloyDBDialer",
        "SyncAwsAuroraDsqlDialer",
        "SyncAwsRdsDialer",
        "SyncAzureEntraDialer",
        "SyncChangeSource",
        "SyncCloudSQLDialer",
        "SyncDatabaseDriver",
        "SyncPipeline",
        "SyncPipelineCapable",
        "SyncQueueStore",
        "TenantPredicate",
        "TenantScopeConfig",
        "ThreadOffloadAsyncDatabaseDriver",
        "aws_rds_root_ca_pem",
        "dbapi",
        "provide_driver",
    }
)


@pytest.mark.parametrize("engine", _ADAPTER_ENGINES)
def test_adapter_package_imports_and_legacy_db_paths_removed(engine: str) -> None:
    """Verify each engine lives under vertebra.db.adapters.<engine> and legacy vertebra.db.<engine> raises."""
    adapter_mod = importlib.import_module(f"vertebra.db.adapters.{engine}")
    assert adapter_mod.__all__
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(f"vertebra.db.{engine}")


@pytest.mark.parametrize("legacy_module", ["vertebra.db._arrow", "vertebra.db._registry"])
def test_private_arrow_and_registry_modules_renamed(legacy_module: str) -> None:
    """Verify vertebra.db.arrow and vertebra.db.registry replace underscored module names cleanly."""
    assert importlib.import_module("vertebra.db.arrow") is not None
    assert importlib.import_module("vertebra.db.registry") is not None
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(legacy_module)


def test_vertebra_db_lazy_exports_and_curated_engine_agnostic_all() -> None:
    """Verify vertebra.db exposes only engine-agnostic exports and loads zero adapters at import time."""
    assert frozenset(vertebra_db.__all__) == _EXPECTED_DB_EXPORTS

    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra.db" or name.startswith("vertebra.db.")
    }
    try:
        fresh_db = importlib.import_module("vertebra.db")
        assert frozenset(fresh_db.__all__) == _EXPECTED_DB_EXPORTS
        loaded_adapters = [
            name for name in sys.modules if name == "vertebra.db.adapters" or name.startswith("vertebra.db.adapters.")
        ]
        assert loaded_adapters == []
        assert fresh_db.BaseDatabaseConfig is not None
        assert fresh_db.ArrowCursor is not None
        assert fresh_db.dbapi is not None
        loaded_after_agnostic = [
            name for name in sys.modules if name == "vertebra.db.adapters" or name.startswith("vertebra.db.adapters.")
        ]
        assert loaded_after_agnostic == []
    finally:
        for name in list(sys.modules):
            if name == "vertebra.db" or name.startswith("vertebra.db."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)
        if "vertebra.db" in saved:
            vertebra.__dict__["db"] = saved["vertebra.db"]


def test_cloudsql_and_alloydb_dialers() -> None:
    """Verify Cloud SQL and AlloyDB URI parsing, Admin API endpoints, and metadata exchange framing."""
    csql_uri = CloudSQLInstanceURI.parse("my-proj:us-central1:my-pg")
    assert csql_uri.project == "my-proj"
    assert csql_uri.region == "us-central1"
    assert csql_uri.instance == "my-pg"
    assert csql_uri.expected_server_cn == "my-proj:my-pg"
    assert "generateEphemeralCert" in csql_uri.generate_ephemeral_cert_url()

    csql_dialer = SyncCloudSQLDialer(
        CloudSQLConfig(
            instance_connection_name="my-proj:us-central1:my-pg",
            enable_iam_auth=True,
            refresh_lead_seconds=240,
        )
    )
    assert csql_dialer.needs_refresh() is True
    csql_dialer.cache_iam_token("iam-token-123", expires_in_seconds=3600)
    assert csql_dialer.needs_refresh() is False
    assert csql_dialer.port == 3307
    tls_ctx = csql_dialer.build_tls_context(ca_pem=None)
    assert tls_ctx is not None

    alloy_uri = AlloyDBClusterURI.parse("projects/p1/locations/us-central1/clusters/c1/instances/i1")
    assert alloy_uri.project == "p1"
    assert alloy_uri.instance == "i1"

    frame = SyncAlloyDBDialer.encode_metadata_exchange("vertebra-db/0.1", AlloyDBAuthType.IAM, "oauth-tok")
    assert len(frame) > 4
    assert int.from_bytes(frame[:4], "big") == len(frame) - 4

    alloy_dialer = SyncAlloyDBDialer(
        AlloyDBConfig(instance_uri="projects/p1/locations/us-central1/clusters/c1/instances/i1")
    )
    assert alloy_dialer.port == 5433


@pytest.mark.anyio
async def test_cloudsql_and_alloydb_dialers_unwrap_gcp_token_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify Sync/Async CloudSQL and AlloyDB dialers unwrap GcpTokenSnapshot access_token and cache it."""
    snapshot = GcpTokenSnapshot(
        access_token="snap-oauth-token-xyz",
        expires_at_unix_ms=int((time.time() + 3600) * 1000),
        generation=1,
    )

    class _SyncFakeTokenCache:
        def get_token(self) -> GcpTokenSnapshot:
            return snapshot

    class _AsyncFakeTokenCache:
        async def get_token(self) -> GcpTokenSnapshot:
            return snapshot

    captured_auth_headers: list[str] = []

    def _fake_csql_http(_url: str, _payload: bytes, headers: dict[str, str]) -> bytes:
        captured_auth_headers.append(headers["Authorization"])
        return b'{"ephemeralCert":{"cert":"CERT"},"serverCaCert":{"cert":"CA"},"ipAddress":"10.0.0.1"}'

    def _fake_alloy_http(_url: str, _payload: bytes, headers: dict[str, str]) -> bytes:
        captured_auth_headers.append(headers["Authorization"])
        return b'{"clientCert":"CERT","caCert":"CA"}'

    csql_cfg = CloudSQLConfig(instance_connection_name="my-proj:us-central1:my-pg")
    sync_csql = SyncCloudSQLDialer(csql_cfg, token_cache=_SyncFakeTokenCache())
    bundle_sync_csql = sync_csql.refresh_certificate(
        public_key_pem="PUB",
        private_key_pem="PRIV",
        http_transport=_fake_csql_http,
    )
    assert bundle_sync_csql.server_ip == "10.0.0.1"
    assert sync_csql._cached_token == "snap-oauth-token-xyz"

    async_csql = AsyncCloudSQLDialer(csql_cfg, token_cache=_AsyncFakeTokenCache())
    bundle_async_csql = await async_csql.refresh_certificate(
        public_key_pem="PUB",
        private_key_pem="PRIV",
        http_transport=_fake_csql_http,
    )
    assert bundle_async_csql.server_ip == "10.0.0.1"
    assert async_csql._cached_token == "snap-oauth-token-xyz"

    alloy_cfg = AlloyDBConfig(instance_uri="projects/p1/locations/us-central1/clusters/c1/instances/i1")
    sync_alloy = SyncAlloyDBDialer(alloy_cfg, token_cache=_SyncFakeTokenCache())
    bundle_sync_alloy = sync_alloy.refresh_client_cert(
        public_key_pem="PUB",
        private_key_pem="PRIV",
        http_transport=_fake_alloy_http,
    )
    assert bundle_sync_alloy.client_cert_pem == "CERT"
    assert sync_alloy._cached_token == "snap-oauth-token-xyz"

    async_alloy = AsyncAlloyDBDialer(alloy_cfg, token_cache=_AsyncFakeTokenCache())
    bundle_async_alloy = await async_alloy.refresh_client_cert(
        public_key_pem="PUB",
        private_key_pem="PRIV",
        http_transport=_fake_alloy_http,
    )
    assert bundle_async_alloy.client_cert_pem == "CERT"
    assert async_alloy._cached_token == "snap-oauth-token-xyz"

    assert captured_auth_headers == [
        "Bearer snap-oauth-token-xyz",
        "Bearer snap-oauth-token-xyz",
        "Bearer snap-oauth-token-xyz",
        "Bearer snap-oauth-token-xyz",
    ]

    sent_frames: list[bytes] = []

    class _FakeTlsSock:
        def sendall(self, data: bytes) -> None:
            sent_frames.append(data)

        def recv(self, n: int) -> bytes:
            resp = SyncAlloyDBDialer.encode_metadata_exchange_response(1)
            return resp[:4] if n == 4 else resp[4:]

        def close(self) -> None:
            pass

    class _FakeSslCtx:
        def wrap_socket(self, _sock: object) -> _FakeTlsSock:
            return _FakeTlsSock()

    monkeypatch.setattr("socket.create_connection", lambda *_a, **_kw: _FakeTlsSock())
    sync_alloy_connect = SyncAlloyDBDialer(alloy_cfg, token_cache=_SyncFakeTokenCache())
    sync_alloy_connect.connect("127.0.0.1", ssl_context=cast("Any", _FakeSslCtx()))
    assert sync_alloy_connect._cached_token == "snap-oauth-token-xyz"
    assert SyncAlloyDBDialer.decode_metadata_exchange(sent_frames[-1])["oauth2_token"] == "snap-oauth-token-xyz"

    class _FakeReader:
        async def readexactly(self, n: int) -> bytes:
            resp = AsyncAlloyDBDialer.encode_metadata_exchange_response(1)
            return resp[:4] if n == 4 else resp[4:]

    class _FakeWriter:
        def write(self, data: bytes) -> None:
            sent_frames.append(data)

        async def drain(self) -> None:
            pass

        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            pass

    async def _fake_open_connection(*_a: object, **_kw: object) -> tuple[_FakeReader, _FakeWriter]:
        return _FakeReader(), _FakeWriter()

    monkeypatch.setattr("asyncio.open_connection", _fake_open_connection)
    async_alloy_connect = AsyncAlloyDBDialer(alloy_cfg, token_cache=_AsyncFakeTokenCache())
    await async_alloy_connect.connect("127.0.0.1", ssl_context=cast("Any", _FakeSslCtx()))
    assert async_alloy_connect._cached_token == "snap-oauth-token-xyz"
    assert AsyncAlloyDBDialer.decode_metadata_exchange(sent_frames[-1])["oauth2_token"] == "snap-oauth-token-xyz"


def test_postgres_pooler_modes_and_jsonb_strip() -> None:
    """Verify PgBouncer/PgCat/pgdog pooler modes, direct_dsn fallback, and binary JSONB stripping."""
    pg_cfg = PostgreSQLConfig(
        dsn="postgresql://app:secret@pgbouncer:6432/prod",
        direct_dsn="postgresql://app:secret@pg-primary:5432/prod",
        pooler_mode="transaction",
    )
    assert pg_cfg.effective_statement_cache_size == 0
    assert pg_cfg.effective_direct_dsn == "postgresql://app:secret@pg-primary:5432/prod"
    assert bytes(SyncPostgreSQLDriver.strip_jsonb_header(b'\x01{"ok":true}')) == b'{"ok":true}'
    assert (
        PostgreSQLConfig.compute_worker_pool_budget(
            100, replica_count=3, workers_per_replica=3, reserved_admin_connections=10
        )
        == 10
    )
    with pytest.raises((MissingDependencyError, Exception)):
        SyncPostgreSQLDriver().execute_tuples("SELECT 1")
    with pytest.raises((MissingDependencyError, Exception)):
        SyncMySQLDriver().execute_tuples("SELECT 1")
    with pytest.raises((MissingDependencyError, OSError, OperationalError)):
        SyncRedisDriver(RedisConfig(url="redis://127.0.0.1:1/0")).get("k")
    with pytest.raises((MissingDependencyError, OSError, OperationalError)):
        SyncValkeyDriver(ValkeyConfig(url="valkey://127.0.0.1:1/0")).set("k", "v")
    with pytest.raises((MissingDependencyError, OSError, OperationalError)):
        SyncRedisDriver(RedisConfig(url="redis://127.0.0.1:1/0")).publish("chan", b"msg")


@pytest.mark.parametrize(
    "factory",
    [
        pytest.param(lambda: SyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://")), id="sync-postgres"),
        pytest.param(lambda: AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="MEMORY://")), id="async-postgres"),
        pytest.param(lambda: SyncMySQLDriver(MySQLConfig(dsn="memory://")), id="sync-mysql"),
        pytest.param(lambda: AsyncMySQLDriver(MySQLConfig(dsn="memory://")), id="async-mysql"),
    ],
)
def test_postgres_and_mysql_drivers_reject_memory_dsn(factory: Callable[[], object]) -> None:
    """PostgreSQL and MySQL sync and async drivers reject memory:// DSNs at construction."""
    with pytest.raises(ImproperConfigurationError, match="memory://"):
        factory()


def test_postgres_pool_without_execution_surface_raises() -> None:
    """SyncPostgreSQLDriver raises ImproperConfigurationError when its pool exposes no execution method."""
    driver = SyncPostgreSQLDriver(PostgreSQLConfig(), pool=object())
    with pytest.raises(ImproperConfigurationError, match="pool must provide"):
        driver.execute_with_description("SELECT 1")


@pytest.mark.anyio
async def test_async_postgres_pool_without_execution_surface_raises() -> None:
    """AsyncPostgreSQLDriver raises ImproperConfigurationError when its pool exposes no execution method."""
    driver = AsyncPostgreSQLDriver(PostgreSQLConfig(), pool=object())
    with pytest.raises(ImproperConfigurationError, match="pool must provide"):
        await driver.execute_with_description("SELECT 1")


@pytest.mark.anyio
async def test_sqlite_plain_tuple_fetching_for_sqlalchemy_21() -> None:
    """Verify SQLite WAL configuration, explicit commit/rollback, and plain-tuple row fetching."""
    with pytest.raises(ValueError, match="Invalid synchronous mode"):
        SQLiteConfig(synchronous=cast("Any", "INVALID"))

    driver = AsyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    await driver.execute_tuples("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    await driver.begin()
    await driver.execute_tuples("INSERT INTO items (id, name) VALUES (?, ?)", (1, "alpha"))
    await driver.commit()
    await driver.begin()
    await driver.execute_tuples("INSERT INTO items (id, name) VALUES (?, ?)", (2, "rolled_back"))
    await driver.rollback()
    rows = await driver.execute_tuples("SELECT id, name FROM items")
    assert rows == [(1, "alpha")]
    assert type(rows[0]) is tuple
    await driver.close()


@pytest.mark.anyio
async def test_valkey_resp3_client_tracking_and_litestar_integration(resp3_server: Resp3FakeServer) -> None:
    """Verify Valkey RESP3 handshake, client-side cache invalidation push, and Litestar Store/Channels."""
    vcfg = ValkeyConfig(
        url="valkeys://cache.internal:6379/0",
        enable_client_side_caching=True,
        client_tracking_prefixes=("user:", "session:"),
    )
    assert vcfg.uses_tls is True
    cmds = vcfg.build_handshake_commands()
    assert len(cmds) == 2
    assert b"HELLO" in cmds[0]
    assert b"TRACKING" in cmds[1]
    assert b"BCAST" in cmds[1]

    mem_vcfg = ValkeyConfig(
        url=resp3_server.valkey_url,
        enable_client_side_caching=True,
        client_tracking_prefixes=("user:", "session:"),
    )
    vdriver = AsyncValkeyDriver(mem_vcfg)
    await vdriver.set("user:1", b"alice")
    assert vdriver.cached_keys == ("user:1",)
    assert await vdriver.get("user:1") == b"alice"
    evicted = vdriver.handle_invalidation_push(["user:1"])
    assert evicted == 1
    assert not vdriver.cached_keys
    await vdriver.close()

    store = MemoryStore.from_valkey(resp3_server.valkey_url)
    await store.set("k1", "v1", expires_in=60)
    assert await store.get("k1") == b"v1"
    if store.driver is not None:
        await store.driver.close()

    channels = ChannelsBackend(history=5)
    await channels.on_startup()
    await channels.publish(b"hello", ["topic-a"])
    assert await channels.get_history("topic-a") == [b"hello"]
    await channels.on_shutdown()


def test_database_registry_url_schemes() -> None:
    """Verify URL scheme parsing across PostgreSQL, MySQL, SQLite, Redis, Valkey, Cloud SQL, and AlloyDB."""
    registry = DatabaseRegistry()
    assert isinstance(registry.register("pg", "postgresql+vtb://u:p@localhost:5432/db"), PostgreSQLConfig)
    assert isinstance(registry.register("valkey", "valkeys://localhost:6379/0"), ValkeyConfig)
    assert isinstance(DatabaseRegistry.parse_url("cloudsql://proj:us-central1:inst"), CloudSQLConfig)
    assert isinstance(
        DatabaseRegistry.parse_url("alloydb://projects/p/locations/r/clusters/c/instances/i"),
        AlloyDBConfig,
    )
    registry.register("sqlite_mem", "sqlite:///:memory:")
    sync_sqlite = registry.get_sync_driver("sqlite_mem")
    assert isinstance(sync_sqlite, SyncSQLiteDriver)
    sync_sqlite.close()


def test_channel_bus_clean_break_imports() -> None:
    """Framework and server expose one bus and remove the database backend."""
    assert ServerChannelBus is ChannelBus
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("vertebra.db.channels")
    assert not hasattr(vertebra_db, "DatabaseChannelBackend")


@pytest.mark.anyio
async def test_channel_bus_fanout_history_and_lifecycle() -> None:
    """Subscriptions fan out bytes, bound replay, detach topics, and stop on shutdown."""
    bus = ChannelBus(history=2)
    assert bus.backend is bus
    await bus.subscribe(iter(["orders"]))
    await bus.on_startup()
    streams = [bus.stream_events(), bus.stream_events()]
    pending = [asyncio.create_task(anext(stream)) for stream in streams]
    await asyncio.sleep(0)
    await bus.publish("first", ["orders"])
    assert await asyncio.gather(*pending) == [("orders", b"first")] * 2
    await bus.unsubscribe(iter(["orders"]))
    await bus.publish(b"ignored", ["orders"])
    await bus.subscribe(["orders", "other"])
    pending = [asyncio.create_task(anext(stream)) for stream in streams]
    await asyncio.sleep(0)
    await bus.publish(b"last", ["orders"])
    assert await asyncio.gather(*pending) == [("orders", b"last")] * 2
    assert await bus.get_history("orders") == [b"ignored", b"last"]
    assert await bus.get_history("orders", limit=1) == [b"last"]
    assert await bus.get_history("missing") == []
    assert bus.channels == ("orders", "other")
    pending = [asyncio.create_task(anext(stream)) for stream in streams]
    await asyncio.sleep(0)
    await bus.on_shutdown()
    results = await asyncio.gather(*pending, return_exceptions=True)
    assert all(isinstance(result, StopAsyncIteration) for result in results)
    await bus.on_startup()
    await bus.subscribe(["orders"])
    stream = bus.stream_events()
    next_event = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    await bus.publish(b"restart", ["orders"])
    assert await next_event == ("orders", b"restart")
    await stream.aclose()
    await bus.on_shutdown()


@pytest.mark.anyio
async def test_channel_bus_notification_without_feed_keeps_local_fanout() -> None:
    """Absent feeds do not suppress local publications and protocols receive subscriptions."""
    source = _ConformingNotificationSource()
    bus = ChannelBus(notification_source=source, history=4)
    await bus.on_startup()
    await bus.subscribe(iter(["orders"]))
    assert bus.notification_source is source
    stream = bus.stream_events()
    next_event = asyncio.create_task(anext(stream))
    await asyncio.sleep(0)
    await bus.publish(b"local", ["orders"])
    assert await asyncio.wait_for(next_event, 1) == ("orders", b"local")
    assert await bus.get_history("orders") == [b"local"]
    assert [event.payload async for event in source.listen()] == [b"local"]
    await bus.unsubscribe(iter(["orders"]))
    await source.publish("orders", b"unsubscribed")
    assert [event.payload async for event in source.listen()] == [b"local"]
    await stream.aclose()
    await bus.on_shutdown()


@pytest.mark.anyio
async def test_channel_bus_shared_feed_publish_and_notification_hydration() -> None:
    """Shared PostgreSQL feeds deliver local bytes once and hydrate remote spill envelopes."""

    class Driver:
        def __init__(self) -> None:
            self.parameters: Any = None

        async def _publish_notification(self, channel: str, payload: str) -> None:
            self.parameters = (channel, payload)

    driver = Driver()
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=16))
    source = AsyncPostgresNotificationSource(cast(Any, driver), change_feed=feed)
    bus = ChannelBus(notification_source=source, change_feed=feed, history=4)
    payload = b"\x00\xff" * 5000
    await bus.on_startup()
    await bus.subscribe(["orders"])
    await bus.publish(payload, ["orders"])
    assert await bus.get_history("orders") == [payload]
    assert bus.change_feed is feed
    remote = _ConformingNotificationSource()
    await remote.subscribe(["orders"])
    await remote.publish("orders", driver.parameters[1])
    await bus.pump_notifications(remote)
    assert await bus.get_history("orders") == [payload, payload]
    remote = _ConformingNotificationSource()
    await remote.subscribe(["orders"])
    await remote.publish("orders", b"b64:" + base64.b64encode(b"\xffbinary"))
    await bus.pump_notifications(remote)
    assert await bus.get_history("orders", 1) == [b"\xffbinary"]
    await bus.on_shutdown()


@pytest.mark.anyio
async def test_channel_bus_change_feed_and_async_source() -> None:
    """Feed listeners and async source pumps preserve serialized CDC and PUBLISH bytes."""
    feed = ChangeFeed(CDCConfig(channel="orders"))
    source = _ConformingAsyncChangeSource()
    bus = ChannelBus(change_source=source, change_feed=feed, history=5)
    assert bus.change_source is source
    await bus.on_startup()
    await bus.pump_change_source(channel="override")
    assert decode_json((await bus.get_history("override"))[0])["operation"] == "UPDATE"
    await bus.publish(b"feed-local", ["orders"])
    assert await bus.get_history("orders") == [b"feed-local"]
    feed.publish_event("PUBLISH", {"channel": "orders", "data_b64": base64.b64encode(b"feed").decode()})
    assert await bus.get_history("orders") == [b"feed-local", b"feed"]
    await bus.on_shutdown()


class _SpannerChangeStreamCursor:
    """Async cursor double that records one change-stream read on its owning client double."""

    description: tuple[tuple[Any, ...], ...] | None
    rowcount: int
    lastrowid: Any | None
    arraysize: int

    def __init__(
        self,
        calls: list[tuple[str, str | None]],
        param_encoder: Callable[[Any], str | None] | None,
    ) -> None:
        self._calls = calls
        self._param_encoder = param_encoder
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self.arraysize = 1
        self._rows: list[tuple[Any, ...]] = []

    async def execute(self, sql: str, params: Any = ()) -> _SpannerChangeStreamCursor:
        """Record the change-stream query and buffer one synthetic change-record row."""
        encoded = self._param_encoder(params) if self._param_encoder is not None else None
        self._calls.append((sql, encoded))
        self.description = (("data_change_record", None, None, None, None, None, None),)
        self._rows = [
            (
                [
                    {
                        "table_name": "orders",
                        "mod_type": "INSERT",
                        "mods": [{"keys": {"id": 1}}],
                    }
                ],
            )
        ]
        self.rowcount = 1
        return self

    async def executemany(self, sql: str, seq_of_params: Any) -> _SpannerChangeStreamCursor:
        """Clear buffered rows and record the parameter batch size."""
        _ = sql
        self._rows = []
        self.description = None
        self.rowcount = len(seq_of_params)
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Pop and return the next buffered row, or `None` when exhausted."""
        if not self._rows:
            return None
        return self._rows.pop(0)

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Pop and return up to `size` buffered rows."""
        limit = self.arraysize if size is None else size
        batch = self._rows[:limit]
        del self._rows[:limit]
        return batch

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Return and clear all remaining buffered rows."""
        rows = list(self._rows)
        self._rows.clear()
        return rows

    async def close(self) -> None:
        """Release any remaining buffered rows."""
        self._rows.clear()


@pytest.mark.anyio
@pytest.mark.parametrize("channel", [None, "orders", "override"])
@pytest.mark.parametrize("shared_feed", [False, True], ids=["independent-feed", "shared-feed"])
async def test_channel_bus_spanner_source_delivers_once_per_output_channel(
    channel: str | None, shared_feed: bool
) -> None:
    """Real Spanner CDC preserves feed observation and routes each output once."""

    class Client:
        def __init__(self) -> None:
            self.calls: list[tuple[str, str | None]] = []

        def close(self) -> None:
            pass

        def cursor(
            self,
            *,
            staleness_mode: str = "strong",
            staleness_value: int = 0,
            request_tag: str | None = None,
            transaction_tag: str | None = None,
            param_encoder: Callable[[Any], str | None] | None = None,
            lazy_begin: bool = False,
        ) -> _SpannerChangeStreamCursor:
            """Create an async change-stream cursor bound to this client double."""
            _ = (staleness_mode, staleness_value, request_tag, transaction_tag, lazy_begin)
            return _SpannerChangeStreamCursor(self.calls, param_encoder)

    client = Client()
    driver = AsyncSpannerDriver(SpannerConfig(), client=cast(Any, client))
    feed = ChangeFeed(CDCConfig(channel="orders"))
    source = AsyncSpannerChangeStreamSource(
        driver, "orders_stream", change_feed=feed, start_timestamp="2026-11-02T00:00:00Z"
    )
    bus = ChannelBus(change_source=source, change_feed=feed if shared_feed else None, history=5)
    try:
        await bus.pump_change_source(channel=channel)
        assert len(client.calls) == 1
        assert "READ_orders_stream" in client.calls[0][0]
        raw_params = client.calls[0][1]
        assert raw_params is not None
        assert decode_json(raw_params)["start_ts"] == "2026-11-02T00:00:00Z"
        event = feed.replay_since(0)[0]
        output = encode_json_bytes(event)
        expected_original = shared_feed or channel in (None, "orders")
        assert await bus.get_history("orders") == ([output] if expected_original else [])
        assert await bus.get_history("override") == ([output] if channel == "override" else [])
    finally:
        await driver.close()


@pytest.mark.anyio
async def test_channel_bus_change_source_without_feeds_keeps_local_delivery() -> None:
    """A source without a feed still forwards events when the bus also has no feed."""
    bus = ChannelBus(change_source=_ConformingAsyncChangeSource(), history=5)
    await bus.pump_change_source()
    history = await bus.get_history("orders")
    assert len(history) == 1
    assert decode_json(history[0])["operation"] == "UPDATE"


@pytest.mark.anyio
async def test_channel_bus_pumps_decode_binary_and_preserve_raw_payloads() -> None:
    """Notification and CDC pumps decode wrappers without treating plain JSON as a feed."""
    source = _ConformingNotificationSource()
    bus = ChannelBus(notification_source=source, history=8)
    await bus.subscribe(["orders"])
    await source.publish("orders", b"b64:" + base64.b64encode(b"\x00\xff"))
    await source.publish("orders", b"raw")
    await bus.pump_notifications()
    assert await bus.get_history("orders") == [b"\x00\xff", b"raw"]
    feed_bus = ChannelBus(change_feed=ChangeFeed(), history=8)
    remote = _ConformingNotificationSource()
    await remote.subscribe(["orders"])
    await remote.publish("orders", b'{"id":1}')
    await remote.publish("orders", b"{raw")
    await feed_bus.pump_notifications(remote)
    assert await feed_bus.get_history("orders") == [b'{"id":1}', b"{raw"]

    class PublishSource:
        async def stream(self) -> AsyncIterator[ChangeEvent]:
            yield ChangeEvent(
                event_id=1,
                channel="cdc",
                operation="PUBLISH",
                payload={"channel": "orders", "data_b64": base64.b64encode(b"\xffcdc").decode()},
            )

    await bus.pump_change_source(PublishSource())
    assert await bus.get_history("orders", 1) == [b"\xffcdc"]


@pytest.mark.anyio
async def test_channel_bus_pumps_propagate_failure_and_cancellation() -> None:
    """Explicit pumps expose transport failures and release cancelled source generators."""

    class FailingSource(_ConformingNotificationSource):
        async def listen(self) -> AsyncIterator[NotificationEvent]:
            raise RuntimeError("notification reader failed")
            yield

    class BlockingSource:
        def __init__(self) -> None:
            self.started = asyncio.Event()
            self.closed = asyncio.Event()

        async def stream(self) -> AsyncIterator[ChangeEvent]:
            try:
                self.started.set()
                await asyncio.Event().wait()
                yield ChangeEvent(event_id=1, channel="orders", operation="INSERT", payload={})
            finally:
                self.closed.set()

    bus = ChannelBus()
    with pytest.raises(RuntimeError, match="notification reader failed"):
        await bus.pump_notifications(FailingSource())
    source = BlockingSource()
    pending = asyncio.create_task(bus.pump_change_source(source))
    await asyncio.wait_for(source.started.wait(), 1)
    pending.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert source.closed.is_set()
    feed = ChangeFeed(CDCConfig(spill_threshold_bytes=1, max_memory_spills=1))
    wire, _ = feed.publish_event("PUBLISH", {"channel": "orders", "data_b64": "YQ=="})
    feed.publish_event("PUBLISH", {"channel": "orders", "data_b64": "Yg=="})
    remote = _ConformingNotificationSource()
    await remote.subscribe(["orders"])
    await remote.publish("orders", wire)
    with pytest.raises(KeyError, match="Evicted or unknown"):
        await ChannelBus(change_feed=feed).pump_notifications(remote)
    await bus.pump_notifications()
    await bus.pump_change_source()


@pytest.mark.anyio
async def test_sync_pool_submit_async_coalesced_loop_wakeups() -> None:
    """Verify SyncPool.submit_async coalesces concurrent worker thread completions via a per-loop completion queue."""
    pool = SyncPool(max_workers=8)
    loop = asyncio.get_running_loop()
    orig_csts = loop.call_soon_threadsafe
    csts_calls = 0

    first_wave = threading.Barrier(2, timeout=2.0)
    coalesce_gate = threading.Event()
    completed = 0
    completed_lock = threading.Lock()

    def gated_csts(callback: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal csts_calls
        csts_calls += 1
        if csts_calls == 1:

            def _gated_callback(*cb_args: Any, **cb_kwargs: Any) -> Any:
                assert coalesce_gate.wait(timeout=2.0)
                return callback(*cb_args, **cb_kwargs)

            return orig_csts(_gated_callback, *args, **kwargs)
        return orig_csts(callback, *args, **kwargs)

    def _work(idx: int) -> int:
        nonlocal completed
        if idx < 2:
            first_wave.wait()
        with completed_lock:
            completed += 1
            if completed >= 32:
                coalesce_gate.set()
        return idx * 2

    csts_attr = "call_soon_threadsafe"
    setattr(loop, csts_attr, gated_csts)
    try:
        results = await asyncio.gather(*(pool.submit_async(_work, i) for i in range(64)))
        assert results == [i * 2 for i in range(64)]
    finally:
        setattr(loop, csts_attr, orig_csts)
        pool.shutdown()

    assert csts_calls < 32


@pytest.mark.parametrize(
    ("cls_name", "expected_sqlstate", "expected_vendor_code", "expected_transient"),
    _EXPECTED_DB_ERROR_DEFAULTS,
)
def test_database_error_exports_and_defaults(
    cls_name: str,
    expected_sqlstate: str | None,
    expected_vendor_code: int | None,
    expected_transient: bool,
) -> None:
    """Verify every DatabaseError class is exported on _vertebra, db.driver, and db with exact defaults."""
    native_cls = getattr(native_vertebra, cls_name)
    driver_cls = getattr(vertebra_db_driver, cls_name)
    db_cls = getattr(vertebra_db, cls_name)

    assert native_cls is driver_cls
    assert native_cls is db_cls
    assert issubclass(native_cls, native_vertebra.DatabaseError)
    assert issubclass(native_cls, VertebraError)
    assert native_cls.sqlstate == expected_sqlstate
    assert native_cls.vendor_code == expected_vendor_code
    assert native_cls.is_transient is expected_transient

    instance = native_cls("failure")
    assert str(instance) == "failure"
    assert instance.sqlstate == expected_sqlstate
    assert instance.vendor_code == expected_vendor_code
    assert instance.is_transient is expected_transient


def test_database_error_multi_inheritance_with_value_error_and_gcp_errors() -> None:
    """Verify NotFoundError, SpannerError, SpannerAbortedError, BigQueryError, and BigQueryRowError MRO."""
    db_err = native_vertebra.DatabaseError
    data_err = native_vertebra.DataError
    not_found = native_vertebra.NotFoundError
    serialization_err = native_vertebra.SerializationFailure
    gc_err = native_vertebra.GoogleCloudError
    spanner_err = native_vertebra.SpannerError
    spanner_aborted = native_vertebra.SpannerAbortedError
    bq_err = native_vertebra.BigQueryError
    bq_row_err = native_vertebra.BigQueryRowError

    assert issubclass(not_found, data_err)
    assert issubclass(not_found, db_err)
    assert issubclass(not_found, VertebraError)
    assert issubclass(not_found, ValueError)
    with pytest.raises(ValueError, match="missing row"):
        raise not_found("missing row")
    with pytest.raises(db_err, match="missing row"):
        raise not_found("missing row")

    assert issubclass(spanner_err, gc_err)
    assert issubclass(spanner_err, db_err)
    assert issubclass(spanner_aborted, spanner_err)
    assert issubclass(spanner_aborted, serialization_err)
    assert spanner_aborted.code == 10
    assert spanner_aborted.code_name == "ABORTED"
    assert spanner_aborted.sqlstate == "40001"
    assert spanner_aborted.is_transient is True

    assert issubclass(bq_err, gc_err)
    assert issubclass(bq_err, db_err)
    assert issubclass(bq_row_err, bq_err)
    assert issubclass(bq_row_err, data_err)
    assert bq_row_err.code == 3
    assert bq_row_err.code_name == "INVALID_ARGUMENT"


def test_arrow_cursor_from_shm_capsule_zero_copy_and_unlink_on_release() -> None:
    """Verify ArrowCursor._from_shm_capsule transfers zero-copy SHM ownership and unlinks on release."""
    payload = b"VTBARROW-CAPSULE-IPC-PAYLOAD-0123456789"
    shm_raw_name = f"/vtb-cap-test-{int(time.time_ns() % 1_000_000_000)}"
    norm_name, byte_len, capsule = native_vertebra.ArrowCursor._allocate_shm_capsule(
        shm_raw_name,
        payload,
    )
    assert byte_len == len(payload)
    cursor = native_vertebra.ArrowCursor._from_shm_capsule(norm_name, byte_len, 3, capsule)
    assert cursor.shm_name == norm_name
    assert cursor.byte_len == len(payload)
    assert cursor.row_count == 3

    with pytest.raises(ValueError, match="consumed"):
        native_vertebra.ArrowCursor._from_shm_capsule(norm_name, byte_len, 3, capsule)

    mv = memoryview(cursor)
    assert mv.readonly is True
    assert bytes(mv) == payload

    assert cursor.release() is True
    if sys.platform.startswith("linux") and not norm_name.startswith("/tmp/"):
        assert not Path(f"/dev/shm{norm_name}").exists()
    assert bytes(mv) == payload
    assert cursor.release() is False
    mv.release()


def test_standalone_gcp_driver_modules_and_vertebra_clean_break() -> None:
    """Verify vertebra._bigquery and vertebra._spanner expose native clients and _vertebra omits them."""
    bq_mod = importlib.import_module("vertebra._bigquery")
    spanner_mod = importlib.import_module("vertebra._spanner")

    assert bq_mod.GIL_USED is False
    assert hasattr(bq_mod, "SyncBigQueryClient")
    assert hasattr(bq_mod, "AsyncBigQueryClient")
    assert not hasattr(native_vertebra, "SyncBigQueryClient")
    assert not hasattr(native_vertebra, "AsyncBigQueryClient")

    assert spanner_mod.GIL_USED is False
    assert hasattr(spanner_mod, "SyncSpannerClient")
    assert hasattr(spanner_mod, "AsyncSpannerClient")
    assert not hasattr(native_vertebra, "SyncSpannerClient")
    assert not hasattr(native_vertebra, "AsyncSpannerClient")


def test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify Sync/Async BigQuery and Spanner drivers raise MissingDependencyError naming extras when wheel absent."""
    monkeypatch.setitem(sys.modules, "vertebra._bigquery", None)
    monkeypatch.setitem(sys.modules, "vertebra._spanner", None)
    reset_dependency_cache("vertebra._bigquery")
    reset_dependency_cache("vertebra._spanner")
    try:
        sync_bq = SyncBigQueryDriver(project_id="test-proj")
        async_bq = AsyncBigQueryDriver(project_id="test-proj")
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[bigquery\]"):
            sync_bq._ensure_client()
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[bigquery\]"):
            async_bq._ensure_client()

        sync_spanner = SyncSpannerDriver(project_id="test-proj", instance_id="test-inst", database_id="test-db")
        async_spanner = AsyncSpannerDriver(project_id="test-proj", instance_id="test-inst", database_id="test-db")
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[spanner\]"):
            sync_spanner._ensure_client()
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[spanner\]"):
            async_spanner._ensure_client()
    finally:
        reset_dependency_cache("vertebra._bigquery")
        reset_dependency_cache("vertebra._spanner")


def test_cargo_manifests_exclude_unconditional_gcp_and_tonic_dependencies() -> None:
    """Verify vertebra-py and default vertebra-db Cargo manifests do not enable google-cloud-* or tonic."""
    repo_root = Path(__file__).resolve().parents[5]
    py_manifest = tomllib.loads((repo_root / "src/rs/vertebra-py/Cargo.toml").read_text(encoding="utf-8"))
    db_manifest = tomllib.loads((repo_root / "src/rs/vertebra-db/Cargo.toml").read_text(encoding="utf-8"))

    py_deps = py_manifest.get("dependencies", {})
    assert not any(name.startswith("google-cloud-") or name == "tonic" for name in py_deps)

    db_features = db_manifest.get("features", {})
    assert db_features.get("default", []) == []
    assert "gcp" in db_features

    db_deps = db_manifest.get("dependencies", {})
    assert "google-cloud-bigquery" not in db_deps
    assert "google-cloud-spanner" not in db_deps
    assert "tonic" not in db_deps
    for name, spec in db_deps.items():
        if name.startswith("google-cloud-"):
            assert isinstance(spec, dict) and spec.get("optional") is True


@pytest.mark.parametrize(
    ("operation", "payload", "expected_exc"),
    [
        ("read_arrow_shm", b"VTBARROW123", ValueError),
        ("read_arrow_shm", b"VTBARROW" + (128).to_bytes(8, "little") + b"abc", ValueError),
        ("read_arrow_shm", b"not-arrow", DataError),
        ("read_arrow_shm", b'[{"id": 1}]', DataError),
        ("write_arrow_shm", b"not-arrow", DataError),
        ("write_arrow_shm", b'[{"id": 1}]', DataError),
        ("to_arrow_table", b"not-arrow", DataError),
        ("to_polars", b'[{"id": 1}]', DataError),
    ],
)
def test_arrow_cursor_and_shm_helpers_fail_loud(
    operation: str,
    payload: bytes,
    expected_exc: type[Exception],
) -> None:
    """ArrowCursor and SHM helpers reject truncated frames with ValueError and non-IPC payloads with DataError."""
    with pytest.raises(expected_exc):
        if operation == "read_arrow_shm":
            read_arrow_shm(payload)
        elif operation == "write_arrow_shm":
            write_arrow_shm(payload)
        elif operation == "to_arrow_table":
            ArrowCursor(payload).to_arrow_table()
        else:
            ArrowCursor(payload).to_polars()


def _clear_ambient_cloud_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear ambient AWS, Azure, and HTTP proxy variables for isolated credential-chain tests."""
    for env_key in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_ROLE_ARN",
        "AWS_ROLE_SESSION_NAME",
        "AWS_ENDPOINT_URL_STS",
        "AWS_ENDPOINT_URL",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        "AWS_EC2_METADATA_DISABLED",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT",
        "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
        "AZURE_FEDERATED_TOKEN_FILE",
        "AZURE_AUTHORITY_HOST",
        "IDENTITY_ENDPOINT",
        "IDENTITY_HEADER",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "http_proxy",
        "https_proxy",
    ):
        monkeypatch.delenv(env_key, raising=False)


@pytest.mark.parametrize(
    ("provider_kind", "token_ttl_seconds", "refresh_margin_seconds", "should_succeed"),
    [
        ("rds", 900, 60.0, True),
        ("rds", 901, 60.0, False),
        ("rds", 0, 0.0, False),
        ("dsql", 900, 60.0, True),
        ("dsql", 86_400, 120.0, True),
        ("dsql", 604_800, 300.0, True),
        ("dsql", 604_801, 300.0, False),
        ("dsql", 0, 0.0, False),
    ],
)
def test_aws_rds_and_dsql_ttl_bounds_and_ambient_region_fallback(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    provider_kind: str,
    token_ttl_seconds: int,
    refresh_margin_seconds: float,
    should_succeed: bool,
) -> None:
    """Verify RDS (<=900s) vs DSQL (<=604800s) TTL ceilings, DBUser omission on DSQL, and ambient region fallback."""
    _clear_ambient_cloud_env(monkeypatch)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "missing-credentials"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "missing-config"))
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")

    def _build() -> AwsRdsIamTokenProvider | AwsAuroraDsqlIamTokenProvider:
        if provider_kind == "rds":
            return AwsRdsIamTokenProvider(
                endpoint_host="rds.internal",
                username="app_user",
                access_key_id="AKIATESTPLACEHOLDER",
                secret_access_key="test-token-placeholder-secret",
                token_ttl_seconds=token_ttl_seconds,
                refresh_margin_seconds=refresh_margin_seconds,
                clock=lambda: 1_794_873_600.0,
            )
        return AwsAuroraDsqlIamTokenProvider(
            hostname="cluster.dsql.us-west-2.on.aws",
            admin=True,
            access_key_id="AKIATESTPLACEHOLDER",
            secret_access_key="test-token-placeholder-secret",
            token_ttl_seconds=token_ttl_seconds,
            refresh_margin_seconds=refresh_margin_seconds,
            clock=lambda: 1_794_873_600.0,
        )

    if not should_succeed:
        with pytest.raises(ImproperConfigurationError):
            _build()
        return

    provider = _build()
    assert provider.region == "us-west-2"
    signed = provider.resolve_sync()
    qs = parse_qs(urlsplit(f"https://{signed}").query)
    assert qs["X-Amz-Expires"] == [str(token_ttl_seconds)]
    assert f"us-west-2/{'rds-db' if provider_kind == 'rds' else 'dsql'}/aws4_request" in qs["X-Amz-Credential"][0]
    if provider_kind == "rds":
        assert qs["Action"] == ["connect"]
        assert qs["DBUser"] == ["app_user"]
    else:
        assert qs["Action"] == ["DbConnectAdmin"]
        assert "DBUser" not in qs


@pytest.mark.anyio
@pytest.mark.parametrize("exec_mode", ["sync", "async"])
@pytest.mark.parametrize("profile_name", ["default", "analytics"])
async def test_aws_shared_credentials_and_config_profiles_and_precedence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exec_mode: str,
    profile_name: str,
) -> None:
    """Verify ~/.aws/credentials + ~/.aws/config profile parsing, % escaping, and explicit/env precedence."""
    _clear_ambient_cloud_env(monkeypatch)
    creds_file = tmp_path / "credentials"
    config_file = tmp_path / "config"
    creds_file.write_text(
        "[default]\n"
        "aws_access_key_id = AKIADEFAULTPROF\n"
        "aws_secret_access_key = test-token-placeholder-default-sk\n"
        "aws_session_token = test-token-placeholder-default%25sess\n\n"
        "[analytics]\n"
        "aws_access_key_id = AKIAANALYTICSPROF\n"
        "aws_secret_access_key = test-token-placeholder-analytics-sk\n"
        "aws_session_token = test-token-placeholder-analytics%25sess\n",
        encoding="utf-8",
    )
    config_file.write_text(
        "[default]\n"
        "region = us-east-2\n\n"
        "[profile analytics]\n"
        "region = eu-central-1\n"
        "aws_access_key_id = AKIACONFIGFALLBACK\n"
        "aws_secret_access_key = test-token-placeholder-overridden\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(creds_file))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_file))
    if profile_name != "default":
        monkeypatch.setenv("AWS_PROFILE", profile_name)

    provider = AwsRdsIamTokenProvider(
        endpoint_host="db.profile.internal",
        username="iam_reader",
        clock=lambda: 1_794_873_600.0,
    )
    expected_region = "us-east-2" if profile_name == "default" else "eu-central-1"
    expected_ak = "AKIADEFAULTPROF" if profile_name == "default" else "AKIAANALYTICSPROF"
    expected_sess = (
        "test-token-placeholder-default%25sess"
        if profile_name == "default"
        else "test-token-placeholder-analytics%25sess"
    )
    assert provider.region == expected_region

    token = provider.resolve_sync() if exec_mode == "sync" else await provider.resolve_async()
    qs = parse_qs(urlsplit(f"https://{token}").query)
    assert qs["X-Amz-Credential"][0].startswith(f"{expected_ak}/")
    assert f"/{expected_region}/rds-db/aws4_request" in qs["X-Amz-Credential"][0]
    assert qs["X-Amz-Security-Token"] == [expected_sess]

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIAENVOVERRIDE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-token-placeholder-env-sk")
    provider.invalidate()
    env_token = provider.resolve_sync() if exec_mode == "sync" else await provider.resolve_async()
    env_qs = parse_qs(urlsplit(f"https://{env_token}").query)
    assert env_qs["X-Amz-Credential"][0].startswith("AKIAENVOVERRIDE/")

    explicit_provider = AwsRdsIamTokenProvider(
        endpoint_host="db.profile.internal",
        username="iam_reader",
        region="ap-south-1",
        access_key_id="AKIAEXPLICIT",
        secret_access_key="test-token-placeholder-explicit-sk",
        clock=lambda: 1_794_873_600.0,
    )
    exp_token = explicit_provider.resolve_sync() if exec_mode == "sync" else await explicit_provider.resolve_async()
    exp_qs = parse_qs(urlsplit(f"https://{exp_token}").query)
    assert exp_qs["X-Amz-Credential"][0].startswith("AKIAEXPLICIT/")


@pytest.mark.anyio
@pytest.mark.parametrize("exec_mode", ["sync", "async"])
@pytest.mark.parametrize("provider_kind", ["rds", "dsql"])
async def test_aws_web_identity_sts_rotation_expiry_clamping_and_off_loop_async(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    exec_mode: str,
    provider_kind: str,
) -> None:
    """Verify STS WebIdentity XML parsing, fresh JWT re-read, credential expiry clamping, and off-loop async I/O."""
    _clear_ambient_cloud_env(monkeypatch)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-creds"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")

    jwt_path = tmp_path / "web-identity-token.jwt"
    jwt_path.write_text("test-token-placeholder-jwt-v1\n", encoding="utf-8")

    now = [1_794_873_600.0]
    cred_exp_epoch = now[0] + 200.0
    cred_exp_iso = datetime.fromtimestamp(cred_exp_epoch, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    sts_calls: list[dict[str, Any]] = []

    class _StsHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            form = parse_qs(self.rfile.read(length).decode("utf-8"), keep_blank_values=True)
            call_num = len(sts_calls) + 1
            sts_calls.append(
                {
                    "form": form,
                    "thread_id": threading.get_ident(),
                }
            )
            xml_body = (
                '<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">'
                "<AssumeRoleWithWebIdentityResult>"
                "<Credentials>"
                f"<AccessKeyId>ASIAWEBIDCALL{call_num}</AccessKeyId>"
                "<SecretAccessKey>test-token-placeholder-sts-secret</SecretAccessKey>"
                f"<SessionToken>test-token-placeholder-sts-sess-{call_num}</SessionToken>"
                f"<Expiration>{cred_exp_iso}</Expiration>"
                "</Credentials>"
                "</AssumeRoleWithWebIdentityResult>"
                "</AssumeRoleWithWebIdentityResponse>"
            ).encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/xml")
            self.send_header("Content-Length", str(len(xml_body)))
            self.end_headers()
            self.wfile.write(xml_body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _StsHandler)
    port = server.server_address[1]
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    try:
        monkeypatch.setenv("AWS_REGION", "us-east-1")
        monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(jwt_path))
        monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/vertebra-db-role")
        monkeypatch.setenv("AWS_ENDPOINT_URL_STS", f"http://127.0.0.1:{port}/")

        provider: AwsRdsIamTokenProvider | AwsAuroraDsqlIamTokenProvider
        if provider_kind == "rds":
            provider = AwsRdsIamTokenProvider(
                endpoint_host="rds.sts.internal",
                username="sts_user",
                token_ttl_seconds=900,
                refresh_margin_seconds=60.0,
                clock=lambda: now[0],
            )
        else:
            provider = AwsAuroraDsqlIamTokenProvider(
                hostname="cluster.dsql.us-east-1.on.aws",
                token_ttl_seconds=3600,
                refresh_margin_seconds=60.0,
                clock=lambda: now[0],
            )

        loop_thread_id = threading.get_ident()
        first = provider.resolve_sync() if exec_mode == "sync" else await provider.resolve_async()
        assert len(sts_calls) == 1
        assert sts_calls[0]["form"]["Action"] == ["AssumeRoleWithWebIdentity"]
        assert sts_calls[0]["form"]["RoleArn"] == ["arn:aws:iam::123456789012:role/vertebra-db-role"]
        assert sts_calls[0]["form"]["WebIdentityToken"] == ["test-token-placeholder-jwt-v1"]
        if exec_mode == "async":
            assert sts_calls[0]["thread_id"] != loop_thread_id

        first_qs = parse_qs(urlsplit(f"https://{first}").query)
        assert first_qs["X-Amz-Credential"][0].startswith("ASIAWEBIDCALL1/")
        assert first_qs["X-Amz-Security-Token"] == ["test-token-placeholder-sts-sess-1"]

        now[0] = cred_exp_epoch - 61.0
        cached = provider.resolve_sync() if exec_mode == "sync" else await provider.resolve_async()
        assert cached == first
        assert len(sts_calls) == 1

        jwt_path.write_text("test-token-placeholder-jwt-v2\n", encoding="utf-8")
        cred_exp_iso = datetime.fromtimestamp(now[0] + 1200.0, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        now[0] = cred_exp_epoch - 60.0
        second = provider.resolve_sync() if exec_mode == "sync" else await provider.resolve_async()
        assert len(sts_calls) == 2
        assert sts_calls[1]["form"]["WebIdentityToken"] == ["test-token-placeholder-jwt-v2"]
        second_qs = parse_qs(urlsplit(f"https://{second}").query)
        assert second_qs["X-Amz-Credential"][0].startswith("ASIAWEBIDCALL2/")
        assert second_qs["X-Amz-Security-Token"] == ["test-token-placeholder-sts-sess-2"]
    finally:
        server.shutdown()
        server.server_close()
        srv_thread.join(timeout=2.0)


@pytest.mark.parametrize(
    ("uri_env_name", "uri_value"),
    [
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://evil.example.com/v1/credentials"),
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://10.0.0.25/v1/credentials"),
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://169.254.169.254/latest/meta-data/"),
        ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "ftp://127.0.0.1/v1/credentials"),
        ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "//evil.example.com/v1/credentials"),
        ("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "v2/missing-leading-slash"),
    ],
)
def test_aws_ecs_container_credentials_rejects_disallowed_uris(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    uri_env_name: str,
    uri_value: str,
) -> None:
    """Verify ECS container credential provider rejects non-HTTPS remote hosts and malformed relative paths."""
    _clear_ambient_cloud_env(monkeypatch)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-creds"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))
    monkeypatch.setenv(uri_env_name, uri_value)

    provider = AwsRdsIamTokenProvider(
        endpoint_host="rds.ecs.internal",
        region="us-east-1",
        username="ecs_user",
    )
    with pytest.raises(ImproperConfigurationError):
        provider.resolve_sync()


@pytest.mark.parametrize(
    "allowed_endpoint",
    [
        "http://127.0.0.1:8080",
        "http://localhost:8080",
        "http://[::1]:8080",
        "http://169.254.170.2",
        "http://169.254.170.23",
        "http://[fd00:ec2::23]",
        "https://ecs-creds.internal.example",
    ],
)
def test_aws_ecs_endpoint_allowlist_accepts_documented_hosts(allowed_endpoint: str) -> None:
    """Verify documented loopback, 169.254.170.2, fd00:ec2::/32, and HTTPS ECS endpoints pass validation."""
    provider = AwsRdsIamTokenProvider(
        endpoint_host="rds.ecs.internal",
        region="us-east-1",
        username="ecs_user",
        ecs_endpoint=allowed_endpoint,
    )
    assert provider.ecs_endpoint == allowed_endpoint.rstrip("/")


@pytest.mark.anyio
@pytest.mark.parametrize("uri_mode", ["relative", "full"])
@pytest.mark.parametrize("exec_mode", ["sync", "async"])
async def test_aws_ecs_and_imdsv2_credential_chains_and_clamping(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    uri_mode: str,
    exec_mode: str,
) -> None:
    """Verify ECS relative/full URIs with auth token files and EC2 IMDSv2 2-step PUT/GET token exchange."""
    _clear_ambient_cloud_env(monkeypatch)
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-creds"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")

    now = [1_794_873_600.0]
    exp_iso = datetime.fromtimestamp(now[0] + 180.0, tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    auth_file = tmp_path / "ecs-auth-token"
    auth_file.write_text("test-token-placeholder-ecs-bearer\n", encoding="utf-8")
    recorded: list[dict[str, Any]] = []

    class _AwsMetadataHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_PUT(self) -> None:
            ttl_hdr = self.headers.get("X-aws-ec2-metadata-token-ttl-seconds")
            recorded.append({"method": "PUT", "path": self.path, "ttl_hdr": ttl_hdr})
            if self.path != "/latest/api/token" or not ttl_hdr:
                self.send_response(400)
                self.end_headers()
                return
            body = b"test-token-placeholder-imdsv2-session"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            recorded.append(
                {
                    "method": "GET",
                    "path": self.path,
                    "auth": self.headers.get("Authorization"),
                    "imds_token": self.headers.get("X-aws-ec2-metadata-token"),
                }
            )
            if self.path == "/v2/credentials/task":
                payload = encode_json_bytes(
                    {
                        "AccessKeyId": "ASIAECSTESTKEY",
                        "SecretAccessKey": "test-token-placeholder-ecs-sk",
                        "Token": "test-token-placeholder-ecs-sess",
                        "Expiration": exp_iso,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.headers.get("X-aws-ec2-metadata-token") != "test-token-placeholder-imdsv2-session":
                self.send_response(401)
                self.end_headers()
                return

            if self.path == "/latest/meta-data/iam/security-credentials/":
                role_bytes = b"rds-instance-role\n"
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(role_bytes)))
                self.end_headers()
                self.wfile.write(role_bytes)
                return

            if self.path == "/latest/meta-data/iam/security-credentials/rds-instance-role":
                payload = encode_json_bytes(
                    {
                        "Code": "Success",
                        "AccessKeyId": "ASIAIMDSV2KEY",
                        "SecretAccessKey": "test-token-placeholder-imds-sk",
                        "Token": "test-token-placeholder-imds-sess",
                        "Expiration": exp_iso,
                    }
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            self.send_response(404)
            self.end_headers()

    server = ThreadingHTTPServer(("127.0.0.1", 0), _AwsMetadataHandler)
    port = server.server_address[1]
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    try:
        monkeypatch.setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", str(auth_file))
        if uri_mode == "relative":
            monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "/v2/credentials/task")
            ecs_provider = AwsRdsIamTokenProvider(
                endpoint_host="rds.ecs.internal",
                region="us-east-1",
                username="ecs_user",
                ecs_endpoint=f"http://127.0.0.1:{port}",
                token_ttl_seconds=900,
                refresh_margin_seconds=60.0,
                clock=lambda: now[0],
            )
        else:
            monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", f"http://127.0.0.1:{port}/v2/credentials/task")
            ecs_provider = AwsRdsIamTokenProvider(
                endpoint_host="rds.ecs.internal",
                region="us-east-1",
                username="ecs_user",
                token_ttl_seconds=900,
                refresh_margin_seconds=60.0,
                clock=lambda: now[0],
            )

        ecs_tok = ecs_provider.resolve_sync() if exec_mode == "sync" else await ecs_provider.resolve_async()
        ecs_qs = parse_qs(urlsplit(f"https://{ecs_tok}").query)
        assert ecs_qs["X-Amz-Credential"][0].startswith("ASIAECSTESTKEY/")
        assert ecs_qs["X-Amz-Security-Token"] == ["test-token-placeholder-ecs-sess"]
        assert recorded[-1]["auth"] == "test-token-placeholder-ecs-bearer"

        for key in (
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
        ):
            monkeypatch.delenv(key, raising=False)
        recorded.clear()

        monkeypatch.setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", f"http://127.0.0.1:{port}")
        imds_provider = AwsRdsIamTokenProvider(
            endpoint_host="rds.imds.internal",
            region="us-west-2",
            username="ec2_user",
            token_ttl_seconds=900,
            refresh_margin_seconds=60.0,
            clock=lambda: now[0],
        )
        imds_tok = imds_provider.resolve_sync() if exec_mode == "sync" else await imds_provider.resolve_async()
        imds_qs = parse_qs(urlsplit(f"https://{imds_tok}").query)
        assert imds_qs["X-Amz-Credential"][0].startswith("ASIAIMDSV2KEY/")
        assert imds_qs["X-Amz-Security-Token"] == ["test-token-placeholder-imds-sess"]
        assert [r["method"] for r in recorded] == ["PUT", "GET", "GET"]

        now[0] = 1_794_873_600.0 + 119.0
        cached_imds = imds_provider.resolve_sync() if exec_mode == "sync" else await imds_provider.resolve_async()
        assert cached_imds == imds_tok
        assert len(recorded) == 3

        now[0] = 1_794_873_600.0 + 120.0
        _ = imds_provider.resolve_sync() if exec_mode == "sync" else await imds_provider.resolve_async()
        assert len(recorded) == 6
    finally:
        server.shutdown()
        server.server_close()
        srv_thread.join(timeout=2.0)


@pytest.mark.anyio
@pytest.mark.parametrize("auth_chain", ["workload_identity", "app_service_msi", "ambient_client_secret"])
@pytest.mark.parametrize("exec_mode", ["sync", "async"])
async def test_azure_entra_ambient_chains_rotation_and_off_loop_async(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    auth_chain: str,
    exec_mode: str,
) -> None:
    """Verify Azure Workload Identity token file rotation, IDENTITY_ENDPOINT MSI, and ambient AZURE_CLIENT_SECRET."""
    _clear_ambient_cloud_env(monkeypatch)
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")

    now = [1000.0]
    fed_token_file = tmp_path / "azure-federated-token.jwt"
    fed_token_file.write_text("test-token-placeholder-azure-jwt-v1\n", encoding="utf-8")
    recorded_requests: list[dict[str, Any]] = []

    class _AzureAmbientHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            form = parse_qs(self.rfile.read(length).decode("utf-8"), keep_blank_values=True)
            seq = len(recorded_requests) + 1
            recorded_requests.append(
                {
                    "method": "POST",
                    "path": self.path,
                    "form": form,
                    "thread_id": threading.get_ident(),
                }
            )
            payload = encode_json_bytes(
                {
                    "access_token": f"test-token-placeholder-entra-post-{seq}",
                    "expires_in": 600,
                    "token_type": "Bearer",
                }
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            qs = parse_qs(parsed.query, keep_blank_values=True)
            seq = len(recorded_requests) + 1
            recorded_requests.append(
                {
                    "method": "GET",
                    "path": parsed.path,
                    "query": qs,
                    "identity_header": self.headers.get("X-IDENTITY-HEADER"),
                    "thread_id": threading.get_ident(),
                }
            )
            payload = encode_json_bytes(
                {
                    "access_token": f"test-token-placeholder-entra-msi-{seq}",
                    "expires_on": str(int(now[0] + 600)),
                    "token_type": "Bearer",
                }
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _AzureAmbientHandler)
    port = server.server_address[1]
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    try:
        if auth_chain == "workload_identity":
            monkeypatch.setenv("AZURE_TENANT_ID", "11111111-2222-3333-4444-555555555555")
            monkeypatch.setenv("AZURE_CLIENT_ID", "workload-client-id")
            monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", str(fed_token_file))
            monkeypatch.setenv("AZURE_AUTHORITY_HOST", f"http://127.0.0.1:{port}")
        elif auth_chain == "app_service_msi":
            monkeypatch.setenv("IDENTITY_ENDPOINT", f"http://127.0.0.1:{port}/msi/token")
            monkeypatch.setenv("IDENTITY_HEADER", "test-token-placeholder-msi-secret")
            monkeypatch.setenv("AZURE_CLIENT_ID", "uami-app-service-id")
        else:
            monkeypatch.setenv("AZURE_TENANT_ID", "contoso.onmicrosoft.com")
            monkeypatch.setenv("AZURE_CLIENT_ID", "sp-client-id")
            monkeypatch.setenv("AZURE_CLIENT_SECRET", "test-token-placeholder-sp-secret")
            monkeypatch.setenv("AZURE_AUTHORITY_HOST", f"http://127.0.0.1:{port}")

        provider = AzureEntraTokenProvider(
            scope=AZURE_POSTGRES_SCOPE,
            refresh_margin_seconds=120.0,
            clock=lambda: now[0],
        )

        loop_thread_id = threading.get_ident()
        first = provider.resolve_sync() if exec_mode == "sync" else await provider.resolve_async()
        assert len(recorded_requests) == 1
        if exec_mode == "async":
            assert recorded_requests[0]["thread_id"] != loop_thread_id

        if auth_chain == "workload_identity":
            assert first == "test-token-placeholder-entra-post-1"
            assert recorded_requests[0]["path"] == "/11111111-2222-3333-4444-555555555555/oauth2/v2.0/token"
            form = recorded_requests[0]["form"]
            assert form["grant_type"] == ["client_credentials"]
            assert form["client_id"] == ["workload-client-id"]
            assert form["client_assertion_type"] == ["urn:ietf:params:oauth:client-assertion-type:jwt-bearer"]
            assert form["client_assertion"] == ["test-token-placeholder-azure-jwt-v1"]
            assert form["scope"] == [AZURE_POSTGRES_SCOPE]

            fed_token_file.write_text("test-token-placeholder-azure-jwt-v2\n", encoding="utf-8")
            now[0] = 1480.0
            second = provider.resolve_sync() if exec_mode == "sync" else await provider.resolve_async()
            assert second == "test-token-placeholder-entra-post-2"
            assert recorded_requests[1]["form"]["client_assertion"] == ["test-token-placeholder-azure-jwt-v2"]
        elif auth_chain == "app_service_msi":
            assert first == "test-token-placeholder-entra-msi-1"
            assert recorded_requests[0]["path"] == "/msi/token"
            assert recorded_requests[0]["identity_header"] == "test-token-placeholder-msi-secret"
            assert recorded_requests[0]["query"]["api-version"] == ["2019-08-01"]
            assert recorded_requests[0]["query"]["resource"] == ["https://ossrdbms-aad.database.windows.net"]
            assert recorded_requests[0]["query"]["client_id"] == ["uami-app-service-id"]
        else:
            assert first == "test-token-placeholder-entra-post-1"
            assert recorded_requests[0]["path"] == "/contoso.onmicrosoft.com/oauth2/v2.0/token"
            form = recorded_requests[0]["form"]
            assert form["grant_type"] == ["client_credentials"]
            assert form["client_id"] == ["sp-client-id"]
            assert form["client_secret"] == ["test-token-placeholder-sp-secret"]
            assert form["scope"] == [AZURE_POSTGRES_SCOPE]
    finally:
        server.shutdown()
        server.server_close()
        srv_thread.join(timeout=2.0)


@pytest.mark.parametrize(
    ("env_updates", "provider_kwargs"),
    [
        (
            {
                "AZURE_TENANT_ID": "contoso.onmicrosoft.com",
                "AZURE_CLIENT_ID": "cid",
                "AZURE_CLIENT_SECRET": "test-token-placeholder",
                "AZURE_AUTHORITY_HOST": "http://login.evil.example",
            },
            {"scope": AZURE_POSTGRES_SCOPE},
        ),
        (
            {"IDENTITY_ENDPOINT": "http://127.0.0.1:8080/msi/token"},
            {"scope": AZURE_POSTGRES_SCOPE},
        ),
        (
            {"AZURE_FEDERATED_TOKEN_FILE": "/nonexistent/token.jwt", "AZURE_TENANT_ID": "contoso.onmicrosoft.com"},
            {"scope": AZURE_POSTGRES_SCOPE},
        ),
    ],
)
def test_azure_entra_ambient_validation_errors(
    monkeypatch: pytest.MonkeyPatch,
    env_updates: dict[str, str],
    provider_kwargs: dict[str, Any],
) -> None:
    """Verify partial Managed Identity, missing client_id, and non-HTTPS remote authority hosts fail closed."""
    _clear_ambient_cloud_env(monkeypatch)
    for k, v in env_updates.items():
        monkeypatch.setenv(k, v)
    provider = AzureEntraTokenProvider(**provider_kwargs)
    with pytest.raises(ImproperConfigurationError):
        provider.resolve_sync()


@contextlib.contextmanager
def _loopback_http_server(handler: type[BaseHTTPRequestHandler]) -> Iterator[int]:
    """Serve `handler` on an ephemeral 127.0.0.1 port and yield the bound port."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    srv_thread = threading.Thread(target=server.serve_forever, daemon=True)
    srv_thread.start()
    try:
        yield server.server_address[1]
    finally:
        server.shutdown()
        server.server_close()
        srv_thread.join(timeout=2.0)


def _build_credential_chain_provider(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    chain: str,
    port: int,
) -> AwsRdsIamTokenProvider | AzureEntraTokenProvider:
    """Configure one ambient credential chain against a loopback endpoint and return its provider."""
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-creds"))
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "no-config"))
    base = f"http://127.0.0.1:{port}"
    if chain.startswith("aws_"):
        if chain == "aws_ecs":
            monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", f"{base}/v2/credentials/task")
            monkeypatch.setenv("AWS_CONTAINER_AUTHORIZATION_TOKEN", "test-token-placeholder-ecs-bearer")
        elif chain == "aws_sts":
            jwt_path = tmp_path / "web-identity.jwt"
            jwt_path.write_text("test-token-placeholder-jwt\n", encoding="utf-8")
            monkeypatch.setenv("AWS_WEB_IDENTITY_TOKEN_FILE", str(jwt_path))
            monkeypatch.setenv("AWS_ROLE_ARN", "arn:aws:iam::123456789012:role/vertebra-db-role")
            monkeypatch.setenv("AWS_ENDPOINT_URL_STS", f"{base}/")
        else:
            monkeypatch.setenv("AWS_EC2_METADATA_SERVICE_ENDPOINT", base)
        return AwsRdsIamTokenProvider(
            endpoint_host="rds.redirect.internal",
            region="us-east-1",
            username="iam_user",
            clock=lambda: 1_794_873_600.0,
        )
    if chain == "entra_token_endpoint":
        return AzureEntraTokenProvider(
            client_id="sp-client-id",
            client_secret="test-token-placeholder-sp-secret",
            token_endpoint=f"{base}/oauth2/v2.0/token",
            scope=AZURE_POSTGRES_SCOPE,
            clock=lambda: 1000.0,
        )
    if chain == "entra_app_service":
        monkeypatch.setenv("IDENTITY_ENDPOINT", f"{base}/msi/token")
        monkeypatch.setenv("IDENTITY_HEADER", "test-token-placeholder-msi-secret")
        return AzureEntraTokenProvider(scope=AZURE_POSTGRES_SCOPE, clock=lambda: 1000.0)
    return AzureEntraTokenProvider(
        imds_endpoint=f"{base}/metadata/identity/oauth2/token",
        scope=AZURE_POSTGRES_SCOPE,
        clock=lambda: 1000.0,
    )


_CREDENTIAL_RESPONSE_FIELDS: dict[str, Any] = {
    "AccessKeyId": "ASIAREDIRECTSINK",
    "SecretAccessKey": "test-token-placeholder-sink-sk",
    "Token": "test-token-placeholder-sink-sess",
    "access_token": "test-token-placeholder-sink-entra",
    "expires_in": 600,
}


@pytest.mark.parametrize(
    "chain",
    ["aws_ecs", "aws_sts", "aws_imds", "entra_token_endpoint", "entra_app_service", "entra_vm_imds"],
)
def test_cloud_credential_requests_refuse_redirects_without_forwarding_secrets(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    chain: str,
) -> None:
    """Verify every credential and token HTTP request refuses a 302 and never contacts the redirect target."""
    _clear_ambient_cloud_env(monkeypatch)
    sink_requests: list[dict[str, str | None]] = []
    sink_payload = encode_json_bytes(_CREDENTIAL_RESPONSE_FIELDS)

    class _SinkHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def _record(self) -> None:
            sink_requests.append(
                {
                    "authorization": self.headers.get("Authorization"),
                    "identity_header": self.headers.get("X-IDENTITY-HEADER"),
                    "imds_token": self.headers.get("X-aws-ec2-metadata-token"),
                }
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(sink_payload)))
            self.end_headers()
            self.wfile.write(sink_payload)

        do_GET = _record
        do_POST = _record
        do_PUT = _record

    with _loopback_http_server(_SinkHandler) as sink_port:

        class _RedirectHandler(BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:
                return

            def _redirect(self) -> None:
                length = int(self.headers.get("Content-Length", "0"))
                if length:
                    self.rfile.read(length)
                self.send_response(302)
                self.send_header("Location", f"http://127.0.0.1:{sink_port}/stolen")
                self.send_header("Content-Length", "0")
                self.end_headers()

            do_GET = _redirect
            do_POST = _redirect
            do_PUT = _redirect

        with _loopback_http_server(_RedirectHandler) as redirect_port:
            provider = _build_credential_chain_provider(monkeypatch, tmp_path, chain, redirect_port)
            with pytest.raises(ImproperConfigurationError, match="redirect") as exc_info:
                provider.resolve_sync()

    assert sink_requests == []
    assert "test-token-placeholder" not in str(exc_info.value)
    assert str(sink_port) not in str(exc_info.value)


@pytest.mark.parametrize("chain", ["aws_ecs", "aws_sts", "entra_token_endpoint", "entra_vm_imds"])
def test_cloud_credential_requests_reject_oversized_responses(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    chain: str,
) -> None:
    """Verify credential and token responses larger than the 1 MiB cap fail instead of being buffered."""
    _clear_ambient_cloud_env(monkeypatch)
    payload = encode_json_bytes(_CREDENTIAL_RESPONSE_FIELDS)
    oversized = payload + b" " * (1024 * 1024 + 1 - len(payload))

    class _OversizedHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def _respond(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            if length:
                self.rfile.read(length)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(oversized)))
            self.end_headers()
            with contextlib.suppress(OSError):
                self.wfile.write(oversized)

        do_GET = _respond
        do_POST = _respond

    with _loopback_http_server(_OversizedHandler) as port:
        provider = _build_credential_chain_provider(monkeypatch, tmp_path, chain, port)
        with pytest.raises(ImproperConfigurationError, match="exceeds"):
            provider.resolve_sync()


def test_token_provider_shared_across_event_loops_completes_with_single_flight_per_loop() -> None:
    """Verify one provider refreshed concurrently from two event loops completes without cross-loop lock misuse."""
    both_loops_fetching = threading.Barrier(2)
    state_lock = threading.Lock()
    fetch_counts: dict[int, int] = {}
    in_flight: dict[int, int] = {}
    max_in_flight: dict[int, int] = {}

    async def _fetch(scope: str) -> dict[str, Any]:
        ident = threading.get_ident()
        with state_lock:
            fetch_counts[ident] = fetch_counts.get(ident, 0) + 1
            in_flight[ident] = in_flight.get(ident, 0) + 1
            max_in_flight[ident] = max(max_in_flight.get(ident, 0), in_flight[ident])
            is_first_fetch = fetch_counts[ident] == 1
        try:
            if is_first_fetch:
                await asyncio.to_thread(both_loops_fetching.wait, 2.0)
            else:
                await asyncio.sleep(0)
            return {"access_token": f"test-token-placeholder-{scope}", "expires_in": 600}
        finally:
            with state_lock:
                in_flight[ident] -= 1

    provider = AzureEntraTokenProvider(scope=AZURE_POSTGRES_SCOPE, fetcher=_fetch, clock=lambda: 1000.0)
    results: dict[str, list[str]] = {}
    errors: dict[str, BaseException] = {}

    async def _refresh_three_times() -> list[str]:
        return await asyncio.wait_for(
            asyncio.gather(*(provider.resolve_async(force_refresh=True) for _ in range(3))),
            timeout=5.0,
        )

    def _run_loop(name: str) -> None:
        try:
            results[name] = asyncio.run(_refresh_three_times())
        except BaseException as exc:
            errors[name] = exc

    threads = [threading.Thread(target=_run_loop, args=(name,), daemon=True) for name in ("loop-a", "loop-b")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10.0)

    assert errors == {}
    assert [thread.is_alive() for thread in threads] == [False, False]
    expected = [f"test-token-placeholder-{AZURE_POSTGRES_SCOPE}"] * 3
    assert results == {"loop-a": expected, "loop-b": expected}
    assert sorted(max_in_flight.values()) == [1, 1]
    assert provider.resolve_sync() == expected[0]


@pytest.mark.anyio
async def test_token_provider_sync_refresh_does_not_stall_event_loop() -> None:
    """Verify an in-flight `resolve_sync` network fetch on a worker thread never blocks `resolve_async` on the loop."""
    sync_fetch_started = threading.Event()
    release_sync_fetch = threading.Event()
    sync_refresh_done = threading.Event()
    loop_ident = threading.get_ident()
    sync_results: list[str] = []

    def _fetch(scope: str) -> dict[str, Any]:
        if threading.get_ident() == loop_ident:
            return {"access_token": "test-token-placeholder-loop", "expires_in": 600}
        sync_fetch_started.set()
        release_sync_fetch.wait(2.0)
        return {"access_token": "test-token-placeholder-worker", "expires_in": 600}

    provider = AzureEntraTokenProvider(scope=AZURE_POSTGRES_SCOPE, fetcher=_fetch, clock=lambda: 1000.0)

    def _sync_refresh() -> None:
        sync_results.append(provider.resolve_sync(force_refresh=True))
        sync_refresh_done.set()

    worker = threading.Thread(target=_sync_refresh, daemon=True)
    worker.start()
    try:
        assert await asyncio.to_thread(sync_fetch_started.wait, 2.0)
        loop_token = await provider.resolve_async()
        stalled_until_sync_finished = sync_refresh_done.is_set()
    finally:
        release_sync_fetch.set()
        await asyncio.to_thread(worker.join, 5.0)

    assert loop_token == "test-token-placeholder-loop"
    assert stalled_until_sync_finished is False
    assert sync_results == ["test-token-placeholder-worker"]
    assert await provider.resolve_async() in {"test-token-placeholder-loop", "test-token-placeholder-worker"}


@pytest.mark.parametrize(
    ("token_endpoint", "accepted"),
    [
        ("http://login.evil.example/tenant/oauth2/v2.0/token", False),
        ("http://10.0.0.25/tenant/oauth2/v2.0/token", False),
        ("http://localhost%evil.example.com/tenant/oauth2/v2.0/token", False),
        ("http://127.0.0.1:8080/tenant/oauth2/v2.0/token", True),
        ("http://[::1]:8080/tenant/oauth2/v2.0/token", True),
        ("http://localhost:8080/tenant/oauth2/v2.0/token", True),
        ("https://login.example.com/tenant/oauth2/v2.0/token", True),
    ],
)
def test_azure_entra_token_endpoint_requires_https_or_loopback(token_endpoint: str, accepted: bool) -> None:
    """Verify `token_endpoint` follows the same HTTPS-or-loopback rule as `authority_host`."""
    kwargs: dict[str, Any] = {
        "client_id": "sp-client-id",
        "client_secret": "test-token-placeholder-sp-secret",
        "token_endpoint": token_endpoint,
        "scope": AZURE_POSTGRES_SCOPE,
    }
    if accepted:
        assert AzureEntraTokenProvider(**kwargs).token_endpoint == token_endpoint
        return
    with pytest.raises(ImproperConfigurationError, match="token_endpoint must use HTTPS") as exc_info:
        AzureEntraTokenProvider(**kwargs)
    assert "test-token-placeholder" not in str(exc_info.value)


@pytest.mark.parametrize(
    "profile_body",
    [
        "role_arn = arn:aws:iam::123456789012:role/assumed\nsource_profile = base\n",
        "role_arn = arn:aws:iam::123456789012:role/assumed\ncredential_source = Ec2InstanceMetadata\n",
    ],
    ids=["source_profile", "credential_source"],
)
def test_aws_assume_role_profile_falls_through_to_next_credential_source(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile_body: str,
) -> None:
    """Verify an unsupported `role_arn` assume-role profile shape is skipped instead of raising a Web Identity error."""
    _clear_ambient_cloud_env(monkeypatch)
    config_file = tmp_path / "config"
    config_file.write_text(f"[default]\nregion = us-east-1\n{profile_body}", encoding="utf-8")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-creds"))
    payload = encode_json_bytes(
        {
            "AccessKeyId": "ASIAECSFALLTHROUGH",
            "SecretAccessKey": "test-token-placeholder-ecs-sk",
            "Token": "test-token-placeholder-ecs-sess",
        }
    )

    class _EcsHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    with _loopback_http_server(_EcsHandler) as port:
        monkeypatch.setenv("AWS_CONTAINER_CREDENTIALS_FULL_URI", f"http://127.0.0.1:{port}/v2/credentials/task")
        provider = AwsRdsIamTokenProvider(
            endpoint_host="rds.profile.internal",
            username="iam_user",
            clock=lambda: 1_794_873_600.0,
        )
        token = provider.resolve_sync()

    qs = parse_qs(urlsplit(f"https://{token}").query)
    assert qs["X-Amz-Credential"][0].startswith("ASIAECSFALLTHROUGH/")


@pytest.mark.parametrize(
    "profile_body",
    [
        "web_identity_token_file = /var/run/secrets/token.jwt\n",
        "web_identity_token_file = /var/run/secrets/token.jwt\nsource_profile = base\n",
        "role_arn = arn:aws:iam::123456789012:role/assumed\n",
    ],
    ids=["token_file_only", "token_file_with_source_profile", "role_arn_only"],
)
def test_aws_incomplete_web_identity_profile_still_raises(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    profile_body: str,
) -> None:
    """Verify a profile declaring half of a Web Identity pair fails closed with the Web Identity error."""
    _clear_ambient_cloud_env(monkeypatch)
    config_file = tmp_path / "config"
    config_file.write_text(f"[default]\nregion = us-east-1\n{profile_body}", encoding="utf-8")
    monkeypatch.setenv("AWS_CONFIG_FILE", str(config_file))
    monkeypatch.setenv("AWS_SHARED_CREDENTIALS_FILE", str(tmp_path / "no-creds"))
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    provider = AwsRdsIamTokenProvider(endpoint_host="rds.profile.internal", username="iam_user")
    with pytest.raises(ImproperConfigurationError, match="Web Identity requires both"):
        provider.resolve_sync()


@pytest.mark.parametrize(
    ("host", "expected"),
    [
        ("localhost", True),
        ("LOCALHOST.", True),
        ("127.0.0.1", True),
        ("[::1]", True),
        ("::1%lo", True),
        ("[::1%lo0]", True),
        ("::ffff:127.0.0.1", True),
        ("localhost%evil.example.com", False),
        ("127.0.0.1%evil.example.com", False),
        ("localhost.%25evil.example.com", False),
        ("fe80::1%eth0", False),
        ("10.0.0.42", False),
        ("pg.postgres.database.azure.com", False),
    ],
)
def test_is_loopback_host_strips_zone_only_for_ipv6_literals(host: str, expected: bool) -> None:
    """Verify a `%zone` suffix is honoured only on IPv6 literals and never turns a hostname into loopback."""
    assert is_loopback_host(host) is expected
