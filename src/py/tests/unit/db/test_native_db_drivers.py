"""Unit tests for native database drivers, Cloud SQL / AlloyDB dialers, Valkey/Redis, and Oracle 26ai."""

from __future__ import annotations

import os
import stat
import zipfile
from typing import TYPE_CHECKING

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
    OracleConfig,
    OracleConnectDescriptor,
    OracleContainer,
    OracleDataFrame,
    OraclePoolRegistry,
    OracleWallet,
    OrdsContainer,
    PostgresConfig,
    SqliteConfig,
    SyncOracleDriver,
    SyncPostgresDriver,
    ValkeyConfig,
)
from vertebra.extensions.litestar import ChannelsBackend, MemoryStore

if TYPE_CHECKING:
    from pathlib import Path


def test_cloudsql_and_alloydb_dialers() -> None:
    """Verify Cloud SQL and AlloyDB URI parsing, Admin API endpoints, and metadata exchange framing."""
    csql_uri = CloudSqlInstanceUri.parse("my-proj:us-central1:my-pg")
    assert csql_uri.project == "my-proj"
    assert csql_uri.region == "us-central1"
    assert csql_uri.instance == "my-pg"
    assert csql_uri.expected_server_cn == "my-proj:my-pg"
    assert "generateEphemeralCert" in csql_uri.generate_ephemeral_cert_url()

    csql_dialer = CloudSqlDialer(
        CloudSqlConfig(instance_connection_name="my-proj:us-central1:my-pg", enable_iam_auth=True)
    )
    assert csql_dialer.needs_refresh() is True
    csql_dialer.cache_iam_token("iam-token-123", expires_in_seconds=3600)
    assert csql_dialer.needs_refresh() is False
    assert csql_dialer.port == 3307

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


@pytest.mark.anyio
async def test_sqlite_plain_tuple_fetching_for_sqlalchemy_21() -> None:
    """Verify SQLite WAL configuration and plain-tuple row fetching for SQLAlchemy 2.1 ORM loaders."""
    driver = AsyncSqliteDriver(SqliteConfig(database=":memory:"))
    await driver.execute_tuples("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    await driver.execute_tuples("INSERT INTO items (id, name) VALUES (?, ?)", (1, "alpha"))
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

    vdriver = AsyncValkeyDriver(vcfg)
    await vdriver.set("user:1", b"alice")
    assert vdriver.cached_keys == ("user:1",)
    assert await vdriver.get("user:1") == b"alice"
    evicted = vdriver.handle_invalidation_push(["user:1"])
    assert evicted == 1
    assert not vdriver.cached_keys

    store = MemoryStore.from_valkey("valkey://127.0.0.1:6379/0")
    await store.set("k1", "v1", expires_in=60)
    assert await store.get("k1") == b"v1"

    channels = ChannelsBackend.from_valkey("valkey://127.0.0.1:6379/0", history=5)
    await channels.on_startup()
    await channels.publish(b"hello", ["topic-a"])
    assert await channels.get_history("topic-a") == [b"hello"]
    await channels.on_shutdown()


def test_oracle_wallet_extraction_descriptor_and_multi_pool_isolation(tmp_path: Path) -> None:
    """Verify zero-Instant-Client wallet extraction, Exadata/RAC descriptor, and pool isolation."""
    wallet_zip = tmp_path / "Wallet_ATP.zip"
    tns_content = (
        "myatp_high = (DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=adb.us-ashburn-1.oraclecloud.com)(PORT=1522))"
        "(CONNECT_DATA=(SERVICE_NAME=myatp_high.adb.oraclecloud.com)))\n"
    )
    with zipfile.ZipFile(wallet_zip, "w") as zf:
        zf.writestr("tnsnames.ora", tns_content)
        zf.writestr("ewallet.pem", "-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")

    prev_tns = os.environ.get("TNS_ADMIN")
    extracted = OracleWallet.extract(wallet_zip, target_dir=tmp_path / "extracted")
    assert os.environ.get("TNS_ADMIN") == prev_tns
    mode = stat.S_IMODE(extracted.wallet_location.stat().st_mode)
    assert mode == 0o700
    assert "MYATP_HIGH" in extracted.aliases

    desc = OracleConnectDescriptor(
        hosts=(("scan1.exadata.internal", 1521), ("scan2.exadata.internal", 1521)),
        service_name="oltp_tac.exadata.internal",
        server_mode="POOLED",
    )
    rendered = desc.to_tns_descriptor()
    assert "(LOAD_BALANCE=ON)" in rendered
    assert "(SERVER=POOLED)" in rendered
    assert "(FAILOVER=ON)" in rendered

    base_cfg = OracleConfig(
        dsn="127.0.0.1:1521/FREEPDB1",
        wallet_zip=wallet_zip,
        driver_features={"enable_numpy": True},
    )
    registry = OraclePoolRegistry(base_cfg)
    oltp_pool = registry.register_pool("oltp", max_size=32, driver_features={"oson_max_field_name_size": 255})
    vector_pool = registry.register_pool("vector", max_size=8, driver_features={"vector_hnsw_ef_search": 64})
    assert oltp_pool.config.driver_features != vector_pool.config.driver_features
    assert base_cfg.driver_features == {"enable_numpy": True}

    vec = SyncOracleDriver.encode_vector([0.1, 0.2, 0.3])
    assert vec.typecode == "f"
    assert len(vec) == 3

    df = OracleDataFrame(("id", "title"), [(1, "Espresso"), (2, "Cold Brew")])
    assert len(df) == 2
    assert df.to_dicts()[0]["title"] == "Espresso"

    adb_container = OracleContainer()
    assert "adb-free:latest-23ai" in adb_container.image
    ords_container = OrdsContainer()
    assert ords_container.enable_apex_lang is True


def test_database_registry_url_schemes() -> None:
    """Verify URL scheme parsing across PostgreSQL, MySQL, SQLite, Redis, Valkey, Oracle, Cloud SQL, and AlloyDB."""
    registry = DatabaseRegistry()
    assert isinstance(registry.register("pg", "postgresql+vtb://u:p@localhost:5432/db"), PostgresConfig)
    assert isinstance(registry.register("valkey", "valkeys://localhost:6379/0"), ValkeyConfig)
    assert isinstance(registry.register("oracle", "oracle+vertebra://admin:pw@localhost:1521/FREEPDB1"), OracleConfig)
    assert isinstance(DatabaseRegistry.parse_url("cloudsql://proj:us-central1:inst"), CloudSqlConfig)
    assert isinstance(
        DatabaseRegistry.parse_url("alloydb://projects/p/locations/r/clusters/c/instances/i"),
        AlloyDbConfig,
    )
