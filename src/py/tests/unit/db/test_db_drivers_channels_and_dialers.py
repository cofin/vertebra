"""Unit tests for native database drivers, Cloud SQL / AlloyDB dialers, ChannelBus, and ArrowCursor."""

from __future__ import annotations

import asyncio
import base64
import importlib
import sys
import threading
import time
import tomllib
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

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
        "SyncChangeSource",
        "SyncCloudSQLDialer",
        "SyncDatabaseDriver",
        "SyncPipeline",
        "SyncPipelineCapable",
        "SyncQueueStore",
        "TenantPredicate",
        "TenantScopeConfig",
        "ThreadOffloadAsyncDatabaseDriver",
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

        async def query(
            self, sql: str, params_json: str | None, staleness_mode: str, staleness_ms: int
        ) -> dict[str, Any]:
            self.calls.append((sql, params_json))
            return {
                "columns": ["data_change_record"],
                "rows": [
                    [
                        [
                            {
                                "table_name": "orders",
                                "mod_type": "INSERT",
                                "mods": [{"keys": {"id": 1}}],
                            }
                        ]
                    ]
                ],
            }

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
