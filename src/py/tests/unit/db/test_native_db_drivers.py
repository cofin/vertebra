"""Unit tests for native database drivers, Cloud SQL / AlloyDB dialers, and Valkey/Redis."""

from __future__ import annotations

import asyncio
import importlib
import sys
import time
import tomllib
from pathlib import Path
from typing import Any, cast

import pytest

import vertebra
import vertebra._vertebra as native_vertebra
import vertebra.db as vertebra_db
import vertebra.db.driver as vertebra_db_driver
from vertebra.db import (
    AlloyDBAuthType,
    AlloyDBClusterURI,
    AlloyDBConfig,
    AsyncAlloyDBDialer,
    AsyncCloudSQLDialer,
    CDCConfig,
    ChangeFeed,
    CloudSQLConfig,
    CloudSQLInstanceURI,
    DatabaseRegistry,
    SyncAlloyDBDialer,
    SyncCloudSQLDialer,
)
from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, SyncBigQueryDriver
from vertebra.db.adapters.mysql import SyncMySQLDriver
from vertebra.db.adapters.oracle import OracleConfig
from vertebra.db.adapters.postgres import PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.redis import RedisConfig, SyncRedisDriver
from vertebra.db.adapters.spanner import AsyncSpannerDriver, SyncSpannerDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.adapters.valkey import AsyncValkeyDriver, SyncValkeyDriver, ValkeyConfig
from vertebra.db.channels import DatabaseChannelBackend
from vertebra.extensions.litestar import ChannelsBackend, MemoryStore
from vertebra.storage import GcpTokenSnapshot, SyncObjectStore
from vertebra.typing import MissingDependencyError, VertebraError, reset_dependency_cache
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
        "AsyncCloudSQLDialer",
        "AsyncDatabaseDriver",
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
        "IntegrityError",
        "InterfaceError",
        "InternalError",
        "NotFoundError",
        "NotSupportedError",
        "OperationalError",
        "PoolExhaustedError",
        "ProgrammingError",
        "QueryCancelledError",
        "SerializationFailure",
        "SyncAlloyDBDialer",
        "SyncCloudSQLDialer",
        "SyncDatabaseDriver",
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
    sync_alloy_connect.connect("127.0.0.1", ssl_context=_FakeSslCtx())
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
    await async_alloy_connect.connect("127.0.0.1", ssl_context=_FakeSslCtx())
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
