"""Unit tests for native database drivers, Cloud SQL / AlloyDB dialers, and Valkey/Redis."""

from __future__ import annotations

import asyncio
import time
from typing import Any, cast

import pytest

from vertebra.db import (
    AlloyDBAuthType,
    AlloyDBClusterURI,
    AlloyDBConfig,
    AsyncAlloyDBDialer,
    AsyncCloudSQLDialer,
    AsyncSQLiteDriver,
    AsyncValkeyDriver,
    CDCConfig,
    ChangeFeed,
    CloudSQLConfig,
    CloudSQLInstanceURI,
    DatabaseChannelBackend,
    DatabaseRegistry,
    OracleConfig,
    PostgreSQLConfig,
    RedisConfig,
    SQLiteConfig,
    SyncAlloyDBDialer,
    SyncCloudSQLDialer,
    SyncMySQLDriver,
    SyncPostgreSQLDriver,
    SyncRedisDriver,
    SyncSQLiteDriver,
    SyncValkeyDriver,
    ValkeyConfig,
)
from vertebra.extensions.litestar import ChannelsBackend, MemoryStore
from vertebra.storage import GcpTokenSnapshot, SyncObjectStore
from vertebra.typing import MissingDependencyError
from vertebra.utils.sync_tools import SyncPool


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
