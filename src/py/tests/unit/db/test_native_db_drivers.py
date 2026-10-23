"""Unit tests for native database drivers, Cloud SQL / AlloyDB dialers, and Valkey/Redis."""

from __future__ import annotations

import pytest

from vertebra.db import (
    AlloyDbAuthType,
    AlloyDbClusterUri,
    AlloyDbConfig,
    AlloyDbDialer,
    AsyncSqliteDriver,
    AsyncValkeyDriver,
    CloudSqlConfig,
    CloudSqlDialer,
    CloudSqlInstanceUri,
    DatabaseRegistry,
    PostgresConfig,
    RedisConfig,
    SqliteConfig,
    SyncMysqlDriver,
    SyncPostgresDriver,
    SyncRedisDriver,
    SyncSqliteDriver,
    SyncValkeyDriver,
    ValkeyConfig,
)
from vertebra.extensions.litestar import ChannelsBackend, MemoryStore
from vertebra.typing import MissingDependencyError


def test_cloudsql_and_alloydb_dialers() -> None:
    """Verify Cloud SQL and AlloyDB URI parsing, Admin API endpoints, and metadata exchange framing."""
    csql_uri = CloudSqlInstanceUri.parse("my-proj:us-central1:my-pg")
    assert csql_uri.project == "my-proj"
    assert csql_uri.region == "us-central1"
    assert csql_uri.instance == "my-pg"
    assert csql_uri.expected_server_cn == "my-proj:my-pg"
    assert "generateEphemeralCert" in csql_uri.generate_ephemeral_cert_url()

    csql_dialer = CloudSqlDialer(
        CloudSqlConfig(
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

    alloy_uri = AlloyDbClusterUri.parse("projects/p1/locations/us-central1/clusters/c1/instances/i1")
    assert alloy_uri.project == "p1"
    assert alloy_uri.instance == "i1"

    frame = AlloyDbDialer.encode_metadata_exchange("vertebra-db/0.1", AlloyDbAuthType.IAM, "oauth-tok")
    assert len(frame) > 4
    assert int.from_bytes(frame[:4], "big") == len(frame) - 4

    alloy_dialer = AlloyDbDialer(
        AlloyDbConfig(instance_uri="projects/p1/locations/us-central1/clusters/c1/instances/i1")
    )
    assert alloy_dialer.port == 5433


def test_postgres_pooler_modes_and_jsonb_strip() -> None:
    """Verify PgBouncer/PgCat/pgdog pooler modes, direct_dsn fallback, and binary JSONB stripping."""
    pg_cfg = PostgresConfig(
        dsn="postgresql://app:secret@pgbouncer:6432/prod",
        direct_dsn="postgresql://app:secret@pg-primary:5432/prod",
        pooler_mode="transaction",
    )
    assert pg_cfg.effective_statement_cache_size == 0
    assert pg_cfg.effective_direct_dsn == "postgresql://app:secret@pg-primary:5432/prod"
    assert bytes(SyncPostgresDriver.strip_jsonb_header(b'\x01{"ok":true}')) == b'{"ok":true}'
    assert (
        PostgresConfig.compute_worker_pool_budget(
            100, replica_count=3, workers_per_replica=3, reserved_admin_connections=10
        )
        == 10
    )
    with pytest.raises((MissingDependencyError, Exception)):
        SyncPostgresDriver().execute_tuples("SELECT 1")
    with pytest.raises((MissingDependencyError, Exception)):
        SyncMysqlDriver().execute_tuples("SELECT 1")
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
        SqliteConfig(synchronous="INVALID")  # type: ignore[arg-type]

    driver = AsyncSqliteDriver(SqliteConfig(database=":memory:"))
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
    assert isinstance(registry.register("pg", "postgresql+vtb://u:p@localhost:5432/db"), PostgresConfig)
    assert isinstance(registry.register("valkey", "valkeys://localhost:6379/0"), ValkeyConfig)
    assert isinstance(DatabaseRegistry.parse_url("cloudsql://proj:us-central1:inst"), CloudSqlConfig)
    assert isinstance(
        DatabaseRegistry.parse_url("alloydb://projects/p/locations/r/clusters/c/instances/i"),
        AlloyDbConfig,
    )
    registry.register("sqlite_mem", "sqlite:///:memory:")
    sync_sqlite = registry.get_sync_driver("sqlite_mem")
    assert isinstance(sync_sqlite, SyncSqliteDriver)
    sync_sqlite.close()
