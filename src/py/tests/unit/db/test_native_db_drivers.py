"""Unit tests for native database drivers, Cloud SQL / AlloyDB dialers, and Valkey/Redis."""

from __future__ import annotations

import ast
import asyncio
import datetime
import decimal
import gc
import importlib
import re
import sqlite3
import sys
import time
import tomllib
import uuid
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path
from typing import Any, Self, cast

import pytest

import vertebra
import vertebra._vertebra as native_vertebra
import vertebra.db as vertebra_db
import vertebra.db.driver as vertebra_db_driver
from vertebra._vertebra import VectorBatchView, VectorView
from vertebra.db import (
    AlloyDBAuthType,
    AlloyDBClusterURI,
    AlloyDBConfig,
    ArrowCursor,
    AsyncAlloyDBDialer,
    AsyncCloudSQLDialer,
    AsyncDatabaseDriver,
    CDCConfig,
    ChangeEvent,
    ChangeFeed,
    CloudSQLConfig,
    CloudSQLInstanceURI,
    DatabaseError,
    DatabaseRegistry,
    IntegrityError,
    NotFoundError,
    OperationalError,
    ProgrammingError,
    SyncAlloyDBDialer,
    SyncCloudSQLDialer,
    SyncDatabaseDriver,
)
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, BigQueryConfig, SyncBigQueryDriver
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.redis import RedisConfig, SyncRedisDriver
from vertebra.db.adapters.spanner import (
    AsyncSpannerDriver,
    AsyncSpannerTransaction,
    Mutation,
    SpannerConfig,
    SyncSpannerDriver,
    SyncSpannerTransaction,
)
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.adapters.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
from vertebra.db.channels import DatabaseChannelBackend
from vertebra.db.driver.capabilities import (
    AsyncChangeSource,
    AsyncPipeline,
    AsyncPipelineCapable,
    ClaimedJob,
    EmbeddingPushdown,
    NotificationEvent,
    NotificationSource,
    QueueStore,
    SyncChangeSource,
    SyncPipeline,
    SyncPipelineCapable,
)
from vertebra.extensions.litestar import ChannelsBackend, MemoryStore
from vertebra.storage import GcpTokenSnapshot, SyncObjectStore
from vertebra.typing import MissingDependencyError, VertebraError, reset_dependency_cache
from vertebra.utils.serializers import (
    decode_json,
    decode_msgpack,
    encode_json,
    encode_json_bytes,
    encode_msgpack_bytes,
)
from vertebra.utils.sync_tools import SyncPool

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
        "BaseDatabaseConfig",
        "CDCConfig",
        "ChangeEvent",
        "ChangeFeed",
        "ClaimedJob",
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
        "NotificationSource",
        "OperationalError",
        "PoolExhaustedError",
        "ProgrammingError",
        "QueryCancelledError",
        "QueueStore",
        "SerializationFailure",
        "SyncAlloyDBDialer",
        "SyncChangeSource",
        "SyncCloudSQLDialer",
        "SyncDatabaseDriver",
        "SyncPipeline",
        "SyncPipelineCapable",
        "TenantPredicate",
        "TenantScopeConfig",
        "ThreadOffloadAsyncDatabaseDriver",
        "provide_bigquery_driver",
        "provide_spanner_driver",
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
    with pytest.raises((MissingDependencyError, OSError)):
        SyncRedisDriver(RedisConfig(url="redis://127.0.0.1:1/0")).get("k")
    with pytest.raises((MissingDependencyError, OSError)):
        SyncValkeyDriver(ValkeyConfig(url="valkey://127.0.0.1:1/0")).set("k", "v")
    with pytest.raises((MissingDependencyError, OSError)):
        SyncRedisDriver(RedisConfig(url="redis://127.0.0.1:1/0")).publish("chan", b"msg")


@pytest.mark.anyio
async def test_sqlite_plain_tuple_fetching_for_sqlalchemy_21() -> None:
    """Verify SQLite WAL configuration, explicit commit/rollback, and plain-tuple row fetching."""
    with pytest.raises(ValueError, match="Invalid synchronous mode"):
        SQLiteConfig(synchronous=cast("Any", "INVALID"))

    driver = AsyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    await driver.execute_tuples("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    await driver.execute_tuples("INSERT INTO items (id, name) VALUES (?, ?)", (1, "alpha"))
    await driver.commit()
    await driver.execute_tuples("INSERT INTO items (id, name) VALUES (?, ?)", (2, "rolled_back"))
    await driver.rollback()
    rows = await driver.execute_tuples("SELECT id, name FROM items")
    assert rows == [(1, "alpha")]
    assert type(rows[0]) is tuple
    await driver.close()


@pytest.mark.anyio
async def test_valkey_resp3_client_tracking_and_litestar_integration() -> None:
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
        url="memory://",
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

    store = MemoryStore.from_valkey("memory://")
    await store.set("k1", "v1", expires_in=60)
    assert await store.get("k1") == b"v1"

    channels = ChannelsBackend.from_valkey("memory://", history=5)
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


@pytest.mark.anyio
async def test_database_channel_backend_multi_engine_and_object_store_spill() -> None:
    """Verify DatabaseChannelBackend supports PostgreSQL LISTEN/NOTIFY, Oracle AQ, CDC, and ObjectStore spill."""
    pg_cfg = PostgreSQLConfig(
        dsn="postgresql://app:secret@pgbouncer:6432/prod",
        direct_dsn="postgresql://app:secret@pg-primary:5432/prod",
        pooler_mode="transaction",
    )
    store = SyncObjectStore("memory://")
    pg_bus = DatabaseChannelBackend.from_postgres(pg_cfg, history=10, object_store=store)
    assert pg_bus.postgres_driver is not None
    assert pg_bus.postgres_driver.config.pooler_mode == "session"
    assert pg_bus.postgres_driver.config.dsn == "postgresql://app:secret@pg-primary:5432/prod"

    await pg_bus.on_startup()
    await pg_bus.subscribe(["orders"])

    large_payload = b"X" * 8000
    await pg_bus.publish(large_payload, ["orders"])
    assert await pg_bus.get_history("orders") == [large_payload]

    pg_bus.ingest_pg_notification("orders", "direct-pg-event")
    assert await pg_bus.get_history("orders", limit=1) == [b"direct-pg-event"]
    await pg_bus.on_shutdown()

    ora_bus = DatabaseChannelBackend.from_oracle(OracleConfig(dsn="localhost:1521/FREEPDB1"), history=5)
    assert ora_bus.oracle_driver is not None

    feed = ChangeFeed(CDCConfig(channel="cdc:orders"), store=store)
    cdc_bus = DatabaseChannelBackend.from_changefeed(feed, history=5)
    await cdc_bus.on_startup()
    await cdc_bus.subscribe(["cdc:orders"])
    feed.publish_event(
        "INSERT",
        {"id": 1, "status": "paid"},
    )
    history = await cdc_bus.get_history("cdc:orders")
    assert len(history) == 1
    assert b'"operation":"INSERT"' in history[0]
    await cdc_bus.on_shutdown()


@pytest.mark.anyio
async def test_sync_pool_submit_async_coalesced_loop_wakeups() -> None:
    """Verify SyncPool.submit_async coalesces concurrent worker thread completions via a per-loop completion queue."""
    pool = SyncPool(max_workers=8)
    loop = asyncio.get_running_loop()
    orig_csts = loop.call_soon_threadsafe
    csts_calls = 0

    def tracked_csts(callback: Any, *args: Any, **kwargs: Any) -> Any:
        nonlocal csts_calls
        csts_calls += 1
        return orig_csts(callback, *args, **kwargs)

    def _work(idx: int) -> int:
        if idx < 16:
            time.sleep(0.002)
        return idx * 2

    csts_attr = "call_soon_threadsafe"
    setattr(loop, csts_attr, tracked_csts)
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


_SQL_CONTRACT_ENGINES: tuple[str, ...] = (
    "sqlite",
    "duckdb",
    "oracle",
    "mssql",
    "postgres",
    "mysql",
    "bigquery",
    "spanner",
)

_LEGACY_DRIVER_ALIASES: tuple[str, ...] = (
    "executemany",
    "fetch",
    "fetchrow",
    "fetchval",
    "fetch_arrow",
    "insert_arrow",
)


class _ContractDBAPICursor:
    """SQLite-backed DB-API cursor double for PostgreSQL and MySQL contract tests."""

    __slots__ = ("_conn", "_rows", "description", "rowcount")

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._rows: list[tuple[Any, ...]] = []
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = 0

    def _run(self, sql: str, params: Any = None) -> None:
        rewritten = sql.replace("%s", "?")
        bound: Any = () if params is None else params
        cur = self._conn.execute(rewritten, bound)
        self.rowcount = max(0, int(cur.rowcount or 0))
        if cur.description:
            self.description = tuple((d[0], None, None, None, None, None, None) for d in cur.description)
            self._rows = [tuple(r) for r in cur.fetchall()]
        else:
            self.description = None
            self._rows = []

    def execute(self, sql: str, params: Any = None) -> None:
        self._run(sql, params)

    def fetchall(self) -> list[tuple[Any, ...]]:
        return list(self._rows)

    def close(self) -> None:
        return None

    def __enter__(self) -> _ContractDBAPICursor:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _ContractAsyncDBAPICursor:
    """Async SQLite-backed DB-API cursor double for PostgreSQL and MySQL contract tests."""

    __slots__ = ("_sync", "description", "rowcount")

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._sync = _ContractDBAPICursor(conn)
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = 0

    async def execute(self, sql: str, params: Any = None) -> None:
        self._sync.execute(sql, params)
        self.description = self._sync.description
        self.rowcount = self._sync.rowcount

    async def fetchall(self) -> list[tuple[Any, ...]]:
        return self._sync.fetchall()

    async def close(self) -> None:
        return None

    async def __aenter__(self) -> _ContractAsyncDBAPICursor:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _ContractDBAPIConnection:
    """Sync/Async DB-API connection double wrapping an in-memory SQLite connection."""

    __slots__ = ("_async_mode", "_conn")

    def __init__(self, conn: sqlite3.Connection, *, async_mode: bool = False) -> None:
        self._conn = conn
        self._async_mode = async_mode

    def cursor(self) -> Any:
        if self._async_mode:
            return _ContractAsyncDBAPICursor(self._conn)
        return _ContractDBAPICursor(self._conn)

    def commit(self) -> Any:
        self._conn.commit()
        if self._async_mode:

            async def _noop() -> None:
                return None

            return _noop()
        return None

    def rollback(self) -> Any:
        self._conn.rollback()
        if self._async_mode:

            async def _noop() -> None:
                return None

            return _noop()
        return None

    def close(self) -> None:
        return None

    def __enter__(self) -> _ContractDBAPIConnection:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    async def __aenter__(self) -> _ContractDBAPIConnection:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _ContractDBAPIPool:
    """Connection pool double exposing sync `.connection()` and async `.acquire()`."""

    __slots__ = ("_async_conn", "_conn", "_sync_conn")

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn
        self._sync_conn = _ContractDBAPIConnection(conn, async_mode=False)
        self._async_conn = _ContractDBAPIConnection(conn, async_mode=True)

    @contextmanager
    def connection(self) -> Iterator[_ContractDBAPIConnection]:
        yield self._sync_conn

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[_ContractDBAPIConnection]:
        yield self._async_conn

    def close(self) -> None:
        return None


class _ContractBigQueryClient:
    """In-memory SQLite-backed BigQuery client double supporting sync and async wire calls."""

    __slots__ = ("_async_mode", "_conn", "_in_tx")

    def __init__(self, conn: sqlite3.Connection, *, async_mode: bool = False) -> None:
        self._conn = conn
        self._async_mode = async_mode
        self._in_tx = False

    @staticmethod
    def _strip_leading_trace_comment(sql: str) -> str:
        stripped = sql.lstrip()
        while stripped.startswith("/*"):
            end_idx = stripped.find("*/")
            if end_idx == -1:
                break
            stripped = stripped[end_idx + 2 :].lstrip()
        return stripped

    def _decode_bq_params(self, sql: str, params_json: str | None) -> tuple[str, Any]:
        clean_sql = self._strip_leading_trace_comment(sql)
        if not params_json:
            return clean_sql, ()
        parsed = decode_json(params_json)
        if not isinstance(parsed, list):
            return clean_sql, ()
        named_values: dict[str, Any] = {}
        positional_values: list[Any] = []
        for entry in parsed:
            if "value" in entry:
                raw_val = entry.get("value")
                param_type = entry.get("type", "STRING")
            else:
                raw_val = entry.get("parameterValue", {}).get("value")
                param_type = entry.get("parameterType", {}).get("type", "STRING")
            val: Any = int(raw_val) if (param_type == "INT64" and raw_val is not None) else raw_val
            name = entry.get("name")
            if name:
                named_values[str(name)] = val
            else:
                positional_values.append(val)
        if named_values:
            rewritten = re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", r":\1", clean_sql)
            return rewritten, named_values
        return clean_sql, tuple(positional_values)

    def _query_sync(
        self,
        sql: str,
        params_json: str | None = None,
        session_id: str | None = None,
        create_session: bool = False,
        *_extra: Any,
    ) -> dict[str, Any]:
        adapted, bound = self._decode_bq_params(sql, params_json)
        upper = adapted.strip().upper()
        sid = session_id or ("bq-contract-sess" if create_session else None)
        if upper.startswith("BEGIN"):
            if not self._conn.in_transaction:
                self._conn.execute("BEGIN")
            self._in_tx = True
            return {"columns": [], "rows": [], "total_rows": 0, "session_id": sid}
        if upper.startswith("COMMIT"):
            self._conn.commit()
            self._in_tx = False
            return {"columns": [], "rows": [], "total_rows": 0, "session_id": sid}
        if upper.startswith("ROLLBACK"):
            self._conn.rollback()
            self._in_tx = False
            return {"columns": [], "rows": [], "total_rows": 0, "session_id": sid}
        cur = self._conn.execute(adapted, bound)
        if cur.description:
            cols = [str(d[0]) for d in cur.description]
            rows = [list(r) for r in cur.fetchall()]
            return {
                "columns": cols,
                "rows": rows,
                "total_rows": len(rows),
                "session_id": sid,
            }
        affected = max(0, int(cur.rowcount or 0))
        if not self._in_tx:
            self._conn.commit()
        return {
            "columns": [],
            "rows": [],
            "total_rows": 0,
            "num_dml_affected_rows": affected,
            "session_id": sid,
        }

    def query(self, sql: str, *args: Any) -> Any:
        result = self._query_sync(sql, *args)
        if self._async_mode:

            async def _coro() -> dict[str, Any]:
                return result

            return _coro()
        return result


class _ContractSpannerClient:
    """In-memory SQLite-backed Spanner client double supporting sync and async wire calls."""

    __slots__ = ("_async_mode", "_conn")

    def __init__(self, conn: sqlite3.Connection, *, async_mode: bool = False) -> None:
        self._conn = conn
        self._async_mode = async_mode

    @staticmethod
    def _adapt_sql(sql: str, params_json: str | None) -> tuple[str, Any]:
        stripped = _ContractBigQueryClient._strip_leading_trace_comment(sql)
        if not params_json:
            return stripped, ()
        decoded = decode_json(params_json)
        if isinstance(decoded, dict):
            values = decoded.get("values", decoded)
            if isinstance(values, dict):
                rewritten = re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", r":\1", stripped)
                coerced = {k: (int(v) if isinstance(v, str) and v.isdigit() else v) for k, v in values.items()}
                return rewritten, coerced
        return stripped, ()

    def _wrap(self, value: Any) -> Any:
        if self._async_mode:

            async def _coro() -> Any:
                return value

            return _coro()
        return value

    def query(self, sql: str, params_json: str | None = None, *_extra: Any) -> Any:
        adapted, bound = self._adapt_sql(sql, params_json)
        cur = self._conn.execute(adapted, bound)
        cols = [str(d[0]) for d in (cur.description or ())]
        rows = [list(r) for r in cur.fetchall()]
        return self._wrap({"columns": cols, "rows": rows, "read_timestamp_micros": 1_700_000_000_000_000})

    def execute_dml(self, sql: str, params_json: str | None = None, *_extra: Any) -> Any:
        adapted, bound = self._adapt_sql(sql, params_json)
        cur = self._conn.execute(adapted, bound)
        self._conn.commit()
        return self._wrap(max(0, int(cur.rowcount or 0)))

    def execute_batch_dml(self, batch_json: str, *_extra: Any) -> Any:
        decoded = decode_json(batch_json) if batch_json else []
        counts: list[int] = []
        for entry in decoded:
            stmt_sql = str(entry.get("sql", ""))
            stmt_params = entry.get("params")
            params_str = encode_json({"values": stmt_params}) if isinstance(stmt_params, dict) else None
            adapted, bound = self._adapt_sql(stmt_sql, params_str)
            cur = self._conn.execute(adapted, bound)
            counts.append(max(0, int(cur.rowcount or 0)))
        self._conn.commit()
        return self._wrap(counts)

    def apply_mutations(self, mutations_json: str, *_extra: Any) -> Any:
        decoded = decode_json(mutations_json) if mutations_json else []
        count = 0
        for item in decoded:
            table = str(item.get("table", ""))
            cols = [str(c) for c in item.get("columns", ())]
            vals = item.get("values", ())
            placeholders = ", ".join("?" for _ in cols)
            col_list = ", ".join(cols)
            for row in vals:
                self._conn.execute(f"INSERT OR REPLACE INTO {table} ({col_list}) VALUES ({placeholders})", list(row))
                count += 1
        self._conn.commit()
        return self._wrap({"commit_timestamp_micros": 1_700_000_000_000_000, "mutation_count": count})

    def update_ddl(self, statements: Sequence[str]) -> Any:
        for stmt in statements:
            self._conn.execute(stmt)
        self._conn.commit()
        return self._wrap("op-ddl-1")

    def read_arrow(self, *_args: Any) -> Any:
        msg = "UNIMPLEMENTED: fallback to query"
        raise RuntimeError(msg)


def _make_contract_driver_pair(engine: str) -> tuple[Any, Any, str, Any, str, Any]:
    """Create a seeded `(sync_driver, async_driver, insert_sql, insert_fn, by_id_sql, by_id_fn)` tuple."""
    ddl = "CREATE TABLE contract_items (id INTEGER PRIMARY KEY, name TEXT, note TEXT)"
    if engine == "sqlite":
        sync_drv = SyncSQLiteDriver(SQLiteConfig(database=":memory:"))
        async_drv = AsyncSQLiteDriver(SQLiteConfig(database=":memory:"))
        sync_drv.execute(ddl)
        sync_drv.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", (1, "seed", None))
        async_drv._sync.execute(ddl)
        async_drv._sync.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", (1, "seed", None))
        return (
            sync_drv,
            async_drv,
            "INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = ?",
            lambda row_id: (row_id,),
        )
    if engine == "duckdb":
        sync_dd = SyncDuckDBDriver(DuckDBConfig(database=":memory:"))
        async_dd = AsyncDuckDBDriver(DuckDBConfig(database=":memory:"))
        sync_dd.execute(ddl)
        sync_dd.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", [1, "seed", None])
        async_dd._sync.execute(ddl)
        async_dd._sync.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", [1, "seed", None])
        return (
            sync_dd,
            async_dd,
            "INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)",
            lambda row_id, name: [row_id, name, None],
            "SELECT id, name FROM contract_items WHERE id = ?",
            lambda row_id: [row_id],
        )
    if engine == "oracle":
        sync_ora = SyncOracleDriver(OracleConfig(dsn=":memory:"))
        async_ora = AsyncOracleDriver(OracleConfig(dsn=":memory:"))
        sync_ora.execute(ddl)
        sync_ora.execute(
            "INSERT INTO contract_items (id, name, note) VALUES (:id, :name, :note)",
            {"id": 1, "name": "seed", "note": None},
        )
        async_ora._sync.execute(ddl)
        async_ora._sync.execute(
            "INSERT INTO contract_items (id, name, note) VALUES (:id, :name, :note)",
            {"id": 1, "name": "seed", "note": None},
        )
        return (
            sync_ora,
            async_ora,
            "INSERT INTO contract_items (id, name, note) VALUES (:id, :name, :note)",
            lambda row_id, name: {"id": row_id, "name": name, "note": None},
            "SELECT id, name FROM contract_items WHERE id = :id",
            lambda row_id: {"id": row_id},
        )
    if engine == "mssql":
        sync_ms = SyncMSSQLDriver(MSSQLConfig(dsn=":memory:"))
        async_ms = AsyncMSSQLDriver(MSSQLConfig(dsn=":memory:"))
        sync_ms.execute(ddl)
        sync_ms.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", (1, "seed", None))
        async_ms._sync.execute(ddl)
        async_ms._sync.execute("INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)", (1, "seed", None))
        return (
            sync_ms,
            async_ms,
            "INSERT INTO contract_items (id, name, note) VALUES (?, ?, ?)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = ?",
            lambda row_id: (row_id,),
        )
    if engine == "postgres":
        sync_conn = sqlite3.connect(":memory:", check_same_thread=False)
        sync_conn.execute(ddl)
        sync_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_conn.commit()
        async_conn = sqlite3.connect(":memory:", check_same_thread=False)
        async_conn.execute(ddl)
        async_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_conn.commit()
        sync_pg = SyncPostgreSQLDriver(
            PostgreSQLConfig(dsn="postgresql://localhost/test"), pool=_ContractDBAPIPool(sync_conn)
        )
        async_pg = AsyncPostgreSQLDriver(
            PostgreSQLConfig(dsn="postgresql://localhost/test"), pool=_ContractDBAPIPool(async_conn)
        )
        return (
            sync_pg,
            async_pg,
            "INSERT INTO contract_items (id, name, note) VALUES (%s, %s, %s)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = %s",
            lambda row_id: (row_id,),
        )
    if engine == "mysql":
        sync_my_conn = sqlite3.connect(":memory:", check_same_thread=False)
        sync_my_conn.execute(ddl)
        sync_my_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_my_conn.commit()
        async_my_conn = sqlite3.connect(":memory:", check_same_thread=False)
        async_my_conn.execute(ddl)
        async_my_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_my_conn.commit()
        sync_wrapped = _ContractDBAPIConnection(sync_my_conn, async_mode=False)
        sync_my = SyncMySQLDriver(MySQLConfig(dsn="mysql://localhost/test"), connect_factory=lambda **_kw: sync_wrapped)
        async_my = AsyncMySQLDriver(MySQLConfig(dsn="mysql://localhost/test"), pool=_ContractDBAPIPool(async_my_conn))
        return (
            sync_my,
            async_my,
            "INSERT INTO contract_items (id, name, note) VALUES (%s, %s, %s)",
            lambda row_id, name: (row_id, name, None),
            "SELECT id, name FROM contract_items WHERE id = %s",
            lambda row_id: (row_id,),
        )
    if engine == "bigquery":
        sync_bq_conn = sqlite3.connect(":memory:", check_same_thread=False)
        sync_bq_conn.execute(ddl)
        sync_bq_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_bq_conn.commit()
        async_bq_conn = sqlite3.connect(":memory:", check_same_thread=False)
        async_bq_conn.execute(ddl)
        async_bq_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_bq_conn.commit()
        bq_cfg = BigQueryConfig(project_id="test-proj", dataset_id="test_ds")
        sync_bq = SyncBigQueryDriver(
            bq_cfg, client=cast("Any", _ContractBigQueryClient(sync_bq_conn, async_mode=False))
        )
        async_bq = AsyncBigQueryDriver(
            bq_cfg, client=cast("Any", _ContractBigQueryClient(async_bq_conn, async_mode=True))
        )
        return (
            sync_bq,
            async_bq,
            "INSERT INTO contract_items (id, name, note) VALUES (@id, @name, @note)",
            lambda row_id, name: {"id": row_id, "name": name, "note": None},
            "SELECT id, name FROM contract_items WHERE id = @id",
            lambda row_id: {"id": row_id},
        )
    if engine == "spanner":
        sync_sp_conn = sqlite3.connect(":memory:", check_same_thread=False)
        sync_sp_conn.execute(ddl)
        sync_sp_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        sync_sp_conn.commit()
        async_sp_conn = sqlite3.connect(":memory:", check_same_thread=False)
        async_sp_conn.execute(ddl)
        async_sp_conn.execute("INSERT INTO contract_items (id, name, note) VALUES (1, 'seed', NULL)")
        async_sp_conn.commit()
        sp_cfg = SpannerConfig(project_id="test-proj", instance_id="test-inst", database_id="test-db")
        sync_sp = SyncSpannerDriver(sp_cfg, client=cast("Any", _ContractSpannerClient(sync_sp_conn, async_mode=False)))
        async_sp = AsyncSpannerDriver(
            sp_cfg, client=cast("Any", _ContractSpannerClient(async_sp_conn, async_mode=True))
        )
        return (
            sync_sp,
            async_sp,
            "INSERT INTO contract_items (id, name, note) VALUES (@id, @name, @note)",
            lambda row_id, name: {"id": row_id, "name": name, "note": None},
            "SELECT id, name FROM contract_items WHERE id = @id",
            lambda row_id: {"id": row_id},
        )
    msg = f"Unsupported engine {engine}"
    raise ValueError(msg)


@pytest.mark.anyio
@pytest.mark.parametrize("engine", _SQL_CONTRACT_ENGINES)
async def test_unified_driver_contract_across_all_eight_engines(engine: str) -> None:
    """Verify R6 unified signatures, return types, NotFoundError, Arrow, transactions, and alias removal."""
    for alias in _LEGACY_DRIVER_ALIASES:
        assert not hasattr(SyncDatabaseDriver, alias)
        assert not hasattr(AsyncDatabaseDriver, alias)

    sync_drv, async_drv, insert_sql, insert_params, by_id_sql, by_id_params = _make_contract_driver_pair(engine)
    try:
        for alias in _LEGACY_DRIVER_ALIASES:
            assert not hasattr(sync_drv, alias), f"{engine} sync driver still exposes {alias}"
            assert not hasattr(async_drv, alias), f"{engine} async driver still exposes {alias}"

        affected_sync = sync_drv.execute(insert_sql, parameters=insert_params(2, "two"))
        assert type(affected_sync) is int
        assert affected_sync == 1

        affected_async = await async_drv.execute(insert_sql, parameters=insert_params(2, "two"))
        assert type(affected_async) is int
        assert affected_async == 1

        desc_res_sync = sync_drv.execute_with_description(by_id_sql, parameters=by_id_params(1))
        assert isinstance(desc_res_sync, tuple) and len(desc_res_sync) == 2
        rows_sync, cols_sync = desc_res_sync
        assert rows_sync == [(1, "seed")]
        assert cols_sync == ("id", "name")

        desc_res_async = await async_drv.execute_with_description(by_id_sql, parameters=by_id_params(1))
        assert isinstance(desc_res_async, tuple) and len(desc_res_async) == 2
        rows_async, cols_async = desc_res_async
        assert rows_async == [(1, "seed")]
        assert cols_async == ("id", "name")

        arrow_sync = sync_drv.select_to_arrow(by_id_sql, parameters=by_id_params(1), threshold_rows=512)
        assert isinstance(arrow_sync, ArrowCursor)
        assert arrow_sync.row_count == 1

        arrow_async = await async_drv.select_to_arrow(by_id_sql, parameters=by_id_params(1), threshold_rows=512)
        assert isinstance(arrow_async, ArrowCursor)
        assert arrow_async.row_count == 1

        with pytest.raises(NotFoundError) as sync_one_exc:
            sync_drv.select_one(by_id_sql, parameters=by_id_params(999))
        assert isinstance(sync_one_exc.value, (DatabaseError, ValueError))

        with pytest.raises(NotFoundError) as async_one_exc:
            await async_drv.select_one(by_id_sql, parameters=by_id_params(999))
        assert isinstance(async_one_exc.value, (DatabaseError, ValueError))

        assert sync_drv.select_one_or_none(by_id_sql, parameters=by_id_params(999)) is None
        assert await async_drv.select_one_or_none(by_id_sql, parameters=by_id_params(999)) is None

        with pytest.raises(NotFoundError):
            sync_drv.select_value(by_id_sql, parameters=by_id_params(999))
        with pytest.raises(NotFoundError):
            await async_drv.select_value(by_id_sql, parameters=by_id_params(999))

        null_sql = "SELECT note FROM contract_items WHERE id = 1"
        assert sync_drv.select_value(null_sql) is None
        assert await async_drv.select_value(null_sql) is None

        with sync_drv.transaction():
            sync_drv.execute(insert_sql, parameters=insert_params(10, "tx_committed"))
        assert sync_drv.select_one(by_id_sql, parameters=by_id_params(10))["name"] == "tx_committed"

        with pytest.raises(RuntimeError, match="abort_sync_tx"), sync_drv.transaction():
            sync_drv.execute(insert_sql, parameters=insert_params(11, "tx_rolled_back"))
            raise RuntimeError("abort_sync_tx")
        assert sync_drv.select_one_or_none(by_id_sql, parameters=by_id_params(11)) is None

        async with async_drv.transaction():
            await async_drv.execute(insert_sql, parameters=insert_params(10, "tx_committed"))
        assert (await async_drv.select_one(by_id_sql, parameters=by_id_params(10)))["name"] == "tx_committed"

        with pytest.raises(RuntimeError, match="abort_async_tx"):
            async with async_drv.transaction():
                await async_drv.execute(insert_sql, parameters=insert_params(11, "tx_rolled_back"))
                raise RuntimeError("abort_async_tx")
        assert await async_drv.select_one_or_none(by_id_sql, parameters=by_id_params(11)) is None

        sync_batches = list(sync_drv.stream_arrow_batches("SELECT id, name FROM contract_items", batch_size=2))
        assert len(sync_batches) >= 1
        assert sum(len(b) for b in sync_batches) == 3

        async_batches = [
            batch async for batch in async_drv.stream_arrow_batches("SELECT id, name FROM contract_items", batch_size=2)
        ]
        assert len(async_batches) >= 1
        assert sum(len(b) for b in async_batches) == 3

        assert sync_drv.cancel() is None
        assert await async_drv.cancel() is None
    finally:
        sync_drv.close()
        await async_drv.close()


def test_sqlite_driver_error_translation_and_parameter_normalization() -> None:
    """Verify SyncSQLiteDriver maps sqlite3 errors to DatabaseError and normalizes rich bind types."""
    driver = SyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    try:
        driver.execute(
            "CREATE TABLE typed_items (id INTEGER PRIMARY KEY, price TEXT, created_at TEXT, ref_id TEXT, meta TEXT, tags TEXT)"
        )
        ts = datetime.datetime(2026, 11, 4, 17, 30, tzinfo=datetime.UTC)
        ref = uuid.UUID("12345678-1234-5678-1234-567812345678")
        inserted = driver.execute(
            "INSERT INTO typed_items (id, price, created_at, ref_id, meta, tags) VALUES (?, ?, ?, ?, ?, ?)",
            (1, decimal.Decimal("42.75"), ts, ref, {"tier": "gold"}, ["a", "b"]),
        )
        assert inserted == 1

        row = driver.select_one("SELECT price, created_at, ref_id, meta, tags FROM typed_items WHERE id = ?", (1,))
        assert row["price"] == "42.75"
        assert row["created_at"] == ts.isoformat()
        assert row["ref_id"] == str(ref)
        assert decode_json(row["meta"]) == {"tier": "gold"}
        assert decode_json(row["tags"]) == ["a", "b"]

        with pytest.raises(IntegrityError) as dup_exc:
            driver.execute(
                "INSERT INTO typed_items (id, price, created_at, ref_id, meta, tags) VALUES (?, ?, ?, ?, ?, ?)",
                (1, decimal.Decimal("1.00"), ts, ref, {}, []),
            )
        assert isinstance(dup_exc.value, DatabaseError)

        with pytest.raises((OperationalError, ProgrammingError)) as sql_exc:
            driver.execute("SELECT * FROM missing_table_xyz")
        assert isinstance(sql_exc.value, DatabaseError)
    finally:
        driver.close()


@pytest.mark.anyio
async def test_spanner_snapshots_and_transactions_not_found_and_buffered_mutations() -> None:
    """Verify Spanner snapshot/transaction NotFoundError, select_one_or_none, select_to_arrow, and mutation buffering."""
    sync_drv, async_drv, _, _, by_id_sql, by_id_params = _make_contract_driver_pair("spanner")
    try:
        with sync_drv.snapshot() as sync_snap:
            assert not hasattr(sync_snap, "fetch_arrow")
            snap_cursor = sync_snap.select_to_arrow(by_id_sql, by_id_params(1), threshold_rows=256)
            assert isinstance(snap_cursor, ArrowCursor)
            assert snap_cursor.row_count == 1
            assert sync_snap.select_one_or_none(by_id_sql, by_id_params(999)) is None
            with pytest.raises(NotFoundError):
                sync_snap.select_one(by_id_sql, by_id_params(999))
            with pytest.raises(NotFoundError):
                sync_snap.query_one(by_id_sql, by_id_params(999))

        async with async_drv.snapshot() as async_snap:
            assert not hasattr(async_snap, "fetch_arrow")
            async_cursor = await async_snap.select_to_arrow(by_id_sql, by_id_params(1), threshold_rows=256)
            assert isinstance(async_cursor, ArrowCursor)
            assert async_cursor.row_count == 1
            assert await async_snap.select_one_or_none(by_id_sql, by_id_params(999)) is None
            with pytest.raises(NotFoundError):
                await async_snap.select_one(by_id_sql, by_id_params(999))
            with pytest.raises(NotFoundError):
                await async_snap.query_one(by_id_sql, by_id_params(999))

        sync_tx = SyncSpannerTransaction(sync_drv)
        assert sync_tx.select_one_or_none(by_id_sql, by_id_params(999)) is None
        with pytest.raises(NotFoundError):
            sync_tx.select_one(by_id_sql, by_id_params(999))
        with pytest.raises(NotFoundError):
            sync_tx.query_one(by_id_sql, by_id_params(999))

        async_tx = AsyncSpannerTransaction(async_drv)
        assert await async_tx.select_one_or_none(by_id_sql, by_id_params(999)) is None
        with pytest.raises(NotFoundError):
            await async_tx.select_one(by_id_sql, by_id_params(999))
        with pytest.raises(NotFoundError):
            await async_tx.query_one(by_id_sql, by_id_params(999))

        with sync_drv.transaction():
            sync_drv.apply_mutations(
                [Mutation.insert_or_update("contract_items", ("id", "name", "note"), [(50, "mut_ok", None)])]
            )
        assert sync_drv.select_one(by_id_sql, by_id_params(50))["name"] == "mut_ok"

        with pytest.raises(RuntimeError, match="abort_mut"), sync_drv.transaction():
            sync_drv.apply_mutations(
                [Mutation.insert_or_update("contract_items", ("id", "name", "note"), [(51, "mut_abort", None)])]
            )
            raise RuntimeError("abort_mut")
        assert sync_drv.select_one_or_none(by_id_sql, by_id_params(51)) is None
    finally:
        sync_drv.close()
        await async_drv.close()


def test_zero_type_ignore_override_annotations_in_db_drivers() -> None:
    """Verify zero `# type: ignore[override]` annotations remain in vertebra/db/driver and adapters."""
    repo_root = Path(__file__).resolve().parents[5]
    db_root = repo_root / "src/py/vertebra/db"
    forbidden = f"{chr(35)} type: ignore[override]"
    targets = [*sorted((db_root / "driver").glob("*.py")), *sorted((db_root / "adapters").glob("*/driver.py"))]
    assert targets
    offenders = [str(path.relative_to(repo_root)) for path in targets if forbidden in path.read_text(encoding="utf-8")]
    assert offenders == []


_CAPABILITY_EXPORT_NAMES: tuple[str, ...] = (
    "AsyncChangeSource",
    "AsyncPipeline",
    "AsyncPipelineCapable",
    "ClaimedJob",
    "EmbeddingPushdown",
    "NotificationEvent",
    "NotificationSource",
    "QueueStore",
    "SyncChangeSource",
    "SyncPipeline",
    "SyncPipelineCapable",
)


class _ConformingSyncPipeline:
    """Minimal conforming stand-in for `SyncPipeline`."""

    __slots__ = ("_ops",)

    def __init__(self) -> None:
        self._ops: list[tuple[str, str, Any]] = []

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("execute", sql, parameters))

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("select", sql, parameters))

    def results(self) -> list[Any]:
        return list(self._ops)


class _ConformingAsyncPipeline:
    """Minimal conforming stand-in for `AsyncPipeline`."""

    __slots__ = ("_ops",)

    def __init__(self) -> None:
        self._ops: list[tuple[str, str, Any]] = []

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None

    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("execute", sql, parameters))

    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None:
        self._ops.append(("select", sql, parameters))

    async def results(self) -> list[Any]:
        return list(self._ops)


class _ConformingSyncPipelineDriver:
    """Minimal conforming stand-in for `SyncPipelineCapable`."""

    def pipeline(self) -> SyncPipeline:
        return _ConformingSyncPipeline()


class _ConformingAsyncPipelineDriver:
    """Minimal conforming stand-in for `AsyncPipelineCapable`."""

    def pipeline(self) -> AsyncPipeline:
        return _ConformingAsyncPipeline()


class _ConformingNotificationSource:
    """Minimal conforming stand-in for `NotificationSource`."""

    __slots__ = ("_events", "_subscribed")

    def __init__(self) -> None:
        self._subscribed: set[str] = set()
        self._events: list[NotificationEvent] = []

    async def publish(self, channel: str, payload: bytes | str) -> None:
        raw = payload.encode("utf-8") if isinstance(payload, str) else payload
        if channel in self._subscribed:
            self._events.append(NotificationEvent(channel=channel, payload=raw, sender_id="standin"))

    async def subscribe(self, channels: Sequence[str]) -> None:
        self._subscribed.update(channels)

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        self._subscribed.difference_update(channels)

    async def listen(self) -> AsyncIterator[NotificationEvent]:
        for event in tuple(self._events):
            yield event


class _ConformingSyncChangeSource:
    """Minimal conforming stand-in for `SyncChangeSource`."""

    def stream(self) -> Iterator[ChangeEvent]:
        yield ChangeEvent(event_id=1, channel="orders", operation="INSERT", payload={"id": 1})


class _ConformingAsyncChangeSource:
    """Minimal conforming stand-in for `AsyncChangeSource`."""

    async def stream(self) -> AsyncIterator[ChangeEvent]:
        yield ChangeEvent(event_id=2, channel="orders", operation="UPDATE", payload={"id": 1})


class _ConformingQueueStore:
    """Minimal conforming stand-in for `QueueStore`."""

    __slots__ = ("_jobs", "_ready")

    def __init__(self) -> None:
        self._ready = False
        self._jobs: dict[str, ClaimedJob] = {}

    async def ensure_schema(self) -> None:
        self._ready = True

    async def enqueue(
        self,
        queue: str,
        task_name: str,
        payload: bytes,
        *,
        job_id: str | None = None,
        scheduled_at_ms: int | None = None,
        max_attempts: int = 3,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        _ = scheduled_at_ms
        jid = job_id or f"job-{len(self._jobs) + 1}"
        self._jobs[jid] = ClaimedJob(
            job_id=jid,
            queue=queue,
            task_name=task_name,
            payload=payload,
            attempt=1,
            max_attempts=max_attempts,
            lease_until_ms=0,
            headers=dict(headers) if headers is not None else {},
        )
        return jid

    async def enqueue_many(
        self,
        queue: str,
        items: Sequence[tuple[str, bytes]],
        *,
        scheduled_at_ms: int | None = None,
        max_attempts: int = 3,
    ) -> list[str]:
        ids: list[str] = []
        for task_name, payload in items:
            ids.append(
                await self.enqueue(
                    queue,
                    task_name,
                    payload,
                    scheduled_at_ms=scheduled_at_ms,
                    max_attempts=max_attempts,
                )
            )
        return ids

    async def claim(
        self,
        queue: str,
        *,
        limit: int = 1,
        lease_ms: int = 30_000,
        worker_id: str = "",
    ) -> list[ClaimedJob]:
        _ = worker_id
        claimed: list[ClaimedJob] = []
        for jid, job in list(self._jobs.items()):
            if job.queue == queue and len(claimed) < limit:
                leased = ClaimedJob(
                    job_id=job.job_id,
                    queue=job.queue,
                    task_name=job.task_name,
                    payload=job.payload,
                    attempt=job.attempt,
                    max_attempts=job.max_attempts,
                    lease_until_ms=lease_ms,
                    headers=dict(job.headers),
                )
                self._jobs[jid] = leased
                claimed.append(leased)
        return claimed

    async def heartbeat(self, job_id: str, attempt: int, *, extend_ms: int = 30_000) -> bool:
        _ = extend_ms
        job = self._jobs.get(job_id)
        return job is not None and job.attempt == attempt

    async def complete(self, job_id: str, attempt: int) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.attempt != attempt:
            return False
        self._jobs.pop(job_id, None)
        return True

    async def fail(self, job_id: str, attempt: int, *, error: str, retry_delay_ms: int | None = None) -> None:
        _ = (attempt, error, retry_delay_ms)
        self._jobs.pop(job_id, None)

    async def promote_scheduled(self, queue: str | None = None, *, now_ms: int | None = None, limit: int = 100) -> int:
        _ = (queue, now_ms, limit)
        return 0


class _ConformingEmbeddingPushdown:
    """Minimal conforming stand-in for `EmbeddingPushdown`."""

    async def embed_documents(
        self,
        texts: Sequence[str],
        *,
        task: str = "document",
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorBatchView:
        _ = task
        dim = dimensions or 3
        rows = [[float(idx + 1)] * dim for idx, _ in enumerate(texts)]
        return VectorBatchView.from_f32_rows(rows, normalize=normalize)

    async def embed_query(
        self,
        text: str,
        *,
        dimensions: int | None = None,
        normalize: bool = True,
    ) -> VectorView:
        _ = text
        dim = dimensions or 3
        return VectorView.from_f32([1.0] * dim, normalize=normalize)

    async def similarity_search(
        self,
        query_vector: VectorView | Sequence[float],
        *,
        table: str,
        embedding_column: str = "embedding",
        top_k: int = 10,
        metric: str = "cosine",
    ) -> list[dict[str, Any]]:
        vec_len = len(query_vector)
        return [
            {
                "table": table,
                "column": embedding_column,
                "top_k": top_k,
                "metric": metric,
                "dimensions": vec_len,
            }
        ]


@pytest.mark.parametrize(
    ("record_cls", "default_kwargs", "full_kwargs", "mutate_attr", "mutate_val", "dict_attr"),
    [
        (
            NotificationEvent,
            {"channel": "orders", "payload": b"\x00\x01notify"},
            {
                "channel": "orders",
                "payload": b"\x00\x01notify",
                "sender_id": "pg-backend-42",
                "metadata": {"lsn": "0/16B3748", "attempt": 1},
            },
            "channel",
            "mutated",
            "metadata",
        ),
        (
            ClaimedJob,
            {
                "job_id": "job-101",
                "queue": "emails",
                "task_name": "send_receipt",
                "payload": b"\x80\x02payload",
                "attempt": 1,
                "max_attempts": 5,
                "lease_until_ms": 1_700_000_030_000,
            },
            {
                "job_id": "job-101",
                "queue": "emails",
                "task_name": "send_receipt",
                "payload": b"\x80\x02payload",
                "attempt": 2,
                "max_attempts": 5,
                "lease_until_ms": 1_700_000_060_000,
                "headers": {"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
            },
            "attempt",
            99,
            "headers",
        ),
    ],
)
def test_capability_records_frozen_gc_untracked_and_serializer_roundtrip(
    record_cls: type[Any],
    default_kwargs: dict[str, Any],
    full_kwargs: dict[str, Any],
    mutate_attr: str,
    mutate_val: Any,
    dict_attr: str,
) -> None:
    """Verify NotificationEvent and ClaimedJob are frozen, GC-untracked, isolated on defaults, and round-trip."""
    first = record_cls(**default_kwargs)
    second = record_cls(**default_kwargs)
    populated = record_cls(**full_kwargs)

    assert not gc.is_tracked(first)
    assert not gc.is_tracked(populated)
    assert getattr(first, dict_attr) == {}
    assert getattr(first, dict_attr) is not getattr(second, dict_attr)
    if record_cls is NotificationEvent:
        assert first.sender_id is None

    with pytest.raises(AttributeError):
        setattr(populated, mutate_attr, mutate_val)

    json_roundtrip = decode_json(encode_json_bytes(populated), type=record_cls)
    assert json_roundtrip == populated
    assert not gc.is_tracked(json_roundtrip)

    msgpack_roundtrip = decode_msgpack(encode_msgpack_bytes(populated), type=record_cls)
    assert msgpack_roundtrip == populated
    assert not gc.is_tracked(msgpack_roundtrip)


@pytest.mark.parametrize(
    ("standin_factory", "expected_protocol"),
    [
        (_ConformingSyncPipeline, SyncPipeline),
        (_ConformingAsyncPipeline, AsyncPipeline),
        (_ConformingSyncPipelineDriver, SyncPipelineCapable),
        (_ConformingAsyncPipelineDriver, AsyncPipelineCapable),
        (_ConformingNotificationSource, NotificationSource),
        (_ConformingSyncChangeSource, SyncChangeSource),
        (_ConformingAsyncChangeSource, AsyncChangeSource),
        (_ConformingQueueStore, QueueStore),
        (_ConformingEmbeddingPushdown, EmbeddingPushdown),
    ],
)
def test_capability_protocols_runtime_isinstance_and_sqlite_negative_checks(
    standin_factory: Any,
    expected_protocol: Any,
) -> None:
    """Verify conforming stand-ins satisfy runtime isinstance while SQLite drivers evaluate False."""
    instance = standin_factory()
    assert isinstance(instance, expected_protocol)
    assert not isinstance(object(), expected_protocol)

    sqlite_sync = SyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    sqlite_async = AsyncSQLiteDriver(SQLiteConfig(database=":memory:"))
    try:
        assert isinstance(sqlite_sync, SyncPipelineCapable) is False
        assert isinstance(sqlite_sync, AsyncPipelineCapable) is False
        assert isinstance(sqlite_sync, expected_protocol) is False
        assert isinstance(sqlite_async, SyncPipelineCapable) is False
        assert isinstance(sqlite_async, AsyncPipelineCapable) is False
        assert isinstance(sqlite_async, expected_protocol) is False
    finally:
        sqlite_sync.close()


@pytest.mark.anyio
async def test_capability_protocols_static_subtyping_and_end_to_end_behavior() -> None:
    """Verify static structural typing and end-to-end execution across all capability protocols."""
    sync_capable: SyncPipelineCapable = _ConformingSyncPipelineDriver()
    async_capable: AsyncPipelineCapable = _ConformingAsyncPipelineDriver()
    notif_source: NotificationSource = _ConformingNotificationSource()
    sync_cdc: SyncChangeSource = _ConformingSyncChangeSource()
    async_cdc: AsyncChangeSource = _ConformingAsyncChangeSource()
    queue_store: QueueStore = _ConformingQueueStore()
    pushdown: EmbeddingPushdown = _ConformingEmbeddingPushdown()

    raw_sync_pipe = sync_capable.pipeline()
    assert isinstance(raw_sync_pipe, SyncPipeline)
    assert not isinstance(raw_sync_pipe, AsyncPipeline)
    with raw_sync_pipe as sync_pipe:
        sync_pipe.execute("INSERT INTO items VALUES (1)")
        sync_pipe.select("SELECT 1")
        assert len(sync_pipe.results()) == 2

    raw_async_pipe = async_capable.pipeline()
    assert isinstance(raw_async_pipe, AsyncPipeline)
    assert not isinstance(raw_async_pipe, SyncPipeline)
    async with raw_async_pipe as async_pipe:
        async_pipe.execute("INSERT INTO items VALUES (2)")
        async_pipe.select("SELECT 2")
        assert len(await async_pipe.results()) == 2

    await notif_source.subscribe(["events"])
    await notif_source.publish("events", "hello")
    received = [evt async for evt in notif_source.listen()]
    assert received == [NotificationEvent(channel="events", payload=b"hello", sender_id="standin")]
    await notif_source.unsubscribe(["events"])

    sync_events = list(sync_cdc.stream())
    assert len(sync_events) == 1 and sync_events[0].operation == "INSERT"
    async_events = [evt async for evt in async_cdc.stream()]
    assert len(async_events) == 1 and async_events[0].operation == "UPDATE"

    await queue_store.ensure_schema()
    jid = await queue_store.enqueue("default", "work", b"body", headers={"k": "v"})
    bulk_ids = await queue_store.enqueue_many("default", [("work2", b"b2")])
    assert len(bulk_ids) == 1
    claimed = await queue_store.claim("default", limit=2, lease_ms=15_000, worker_id="w1")
    assert len(claimed) == 2 and claimed[0].job_id == jid
    assert await queue_store.heartbeat(jid, 1, extend_ms=30_000) is True
    assert await queue_store.complete(jid, 1) is True
    await queue_store.fail(bulk_ids[0], 1, error="boom", retry_delay_ms=500)
    assert await queue_store.promote_scheduled("default", now_ms=1_000, limit=10) == 0

    batch_vec = await pushdown.embed_documents(["doc-a", "doc-b"], dimensions=4)
    assert batch_vec.count == 2 and batch_vec.dimensions == 4
    query_vec = await pushdown.embed_query("find docs", dimensions=4)
    assert query_vec.dimensions == 4
    hits = await pushdown.similarity_search(query_vec, table="docs", top_k=5)
    assert hits[0]["table"] == "docs" and hits[0]["dimensions"] == 4


@pytest.mark.parametrize("symbol_name", _CAPABILITY_EXPORT_NAMES)
def test_capability_exports_on_driver_and_lazy_db_root_with_zero_upward_imports(symbol_name: str) -> None:
    """Verify capability symbols export on db.driver and lazy db root without upward imports."""
    cap_mod = importlib.import_module("vertebra.db.driver.capabilities")
    assert symbol_name in cap_mod.__all__
    assert symbol_name in vertebra_db_driver.__all__
    assert symbol_name in vertebra_db.__all__
    assert getattr(vertebra_db_driver, symbol_name) is getattr(cap_mod, symbol_name)
    assert getattr(vertebra_db, symbol_name) is getattr(cap_mod, symbol_name)

    repo_root = Path(__file__).resolve().parents[5]
    cap_path = repo_root / "src/py/vertebra/db/driver/capabilities.py"
    tree = ast.parse(cap_path.read_text(encoding="utf-8"), filename=str(cap_path))
    imported_targets: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_targets.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_targets.append(node.module)
    forbidden_prefixes = ("vertebra.embeddings", "vertebra.framework", "vertebra.agent", "vertebra.server")
    assert not any(
        mod == prefix or mod.startswith(f"{prefix}.") for mod in imported_targets for prefix in forbidden_prefixes
    )

    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "vertebra.db"
        or name.startswith("vertebra.db.")
        or name == "vertebra.embeddings"
        or name.startswith("vertebra.embeddings.")
    }
    try:
        fresh_db = importlib.import_module("vertebra.db")
        assert "vertebra.db.driver.capabilities" not in sys.modules
        resolved = getattr(fresh_db, symbol_name)
        assert resolved is not None
        assert "vertebra.db.driver.capabilities" in sys.modules
        assert not any(name == "vertebra.embeddings" or name.startswith("vertebra.embeddings.") for name in sys.modules)
    finally:
        for name in list(sys.modules):
            if (
                name == "vertebra.db"
                or name.startswith("vertebra.db.")
                or name == "vertebra.embeddings"
                or name.startswith("vertebra.embeddings.")
            ):
                sys.modules.pop(name, None)
        sys.modules.update(saved)
        if "vertebra.db" in saved:
            vertebra.__dict__["db"] = saved["vertebra.db"]
