"""Unit tests for Rust-native SHM database drivers, Oracle/MSSQL/DuckDB codecs, SQLSpec adapters, and +vtb dialects."""

from __future__ import annotations

import io
import os
import struct
import zipfile
from typing import TYPE_CHECKING, Any

import msgspec
import pyarrow as pa
import pytest
from sqlalchemy.dialects import registry
from sqlalchemy.engine.url import URL

import vertebra.db as db_pkg
import vertebra.db.adapters.duckdb as db_duckdb_mod
import vertebra.db.adapters.mssql as db_mssql_mod
import vertebra.db.adapters.mysql as db_mysql_mod
import vertebra.db.adapters.oracle as db_oracle_mod
import vertebra.db.adapters.postgres as db_postgres_mod
import vertebra.db.adapters.sqlite as db_sqlite_mod
import vertebra.db.arrow as db_arrow_mod
from vertebra._vertebra import ArrowCursor as NativeArrowCursor
from vertebra.db import (
    ARROW_SHM_THRESHOLD_ROWS,
    AlloyDBAuthType,
    AlloyDBConfig,
    ArrowCursor,
    CloudSQLConfig,
    DatabaseRegistry,
    NotSupportedError,
    SyncAlloyDBDialer,
    SyncCloudSQLDialer,
)
from vertebra.db.adapters.duckdb import (
    AsyncDuckDBDriver,
    DuckDBConfig,
    DuckDBSecretConfig,
    SyncDuckDBDriver,
    duckdb_bootstrap_statements,
)
from vertebra.db.adapters.mssql import (
    TDS_PKT_PRE_LOGIN,
    TDS_PKT_SQL_BATCH,
    AsyncMSSQLDriver,
    MSSQLConfig,
    SyncMSSQLDriver,
    decode_tds_packet,
    encode_tds_packet,
    encode_tds_sql_batch,
    rewrite_mssql_placeholders,
)
from vertebra.db.adapters.mysql import (
    AsyncMySQLDriver,
    MySQLConfig,
)
from vertebra.db.adapters.oracle import (
    OSON_MAGIC,
    AsyncOracleDriver,
    OracleConfig,
    OracleConnectDescriptor,
    OracleDriverFeatures,
    OracleEmbeddingPushdown,
    OracleTopologyConfig,
    OracleWallet,
    SyncOracleDriver,
    decode_oson,
    decode_tns_packet,
    decode_ttc_session_attrs,
    decode_vector_binary,
    decode_vector_f32,
    decode_vector_f64,
    decode_vector_i8,
    encode_oson,
    encode_tns_connect_packet,
    encode_tns_data_packet,
    encode_ttc_session_attrs,
    encode_vector_binary,
    encode_vector_f32,
    encode_vector_f64,
    encode_vector_i8,
    parse_tnsnames_ora,
    resolve_tns_alias,
)
from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
    decode_pgcopy_rows,
    encode_pg_jsonb,
    encode_pgcopy_rows,
    read_arrow_shm,
    strip_pg_jsonb_header,
    write_arrow_shm,
)
from vertebra.db.adapters.sqlite import (
    AsyncSQLiteDriver,
    SQLiteConfig,
    SyncSQLiteDriver,
)
from vertebra.extensions.sqlalchemy.dialects import (
    ALL_DIALECT_SCHEMES,
    NullPool,
    SyncDBAPIConnection,
    SyncDBAPICursor,
    SyncDuckDBDialect,
    SyncMSSQLDialect,
)
from vertebra.extensions.sqlspec.adapters import (
    DuckDBAsyncConfig,
    DuckDBSyncConfig,
    MSSQLAsyncConfig,
    MSSQLSyncConfig,
    MySQLAsyncConfig,
    MySQLSyncConfig,
    OracleAsyncConfig,
    OracleAsyncDriver,
    OracleSyncConfig,
    OracleSyncDriver,
    PostgreSQLAsyncConfig,
    PostgreSQLAsyncDriver,
    PostgreSQLSyncConfig,
    PostgreSQLSyncDriver,
    SQLiteAsyncConfig,
    SQLiteSyncConfig,
    TenantPredicate,
)
from vertebra.typing import ImproperConfigurationError, VertebraError

if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.anyio
async def test_duckdb_execute_many_reports_affected_rows() -> None:
    """Batch counts reflect matching rows for sync and async UPDATE and DELETE."""
    sync_driver = SyncDuckDBDriver()
    async_driver = AsyncDuckDBDriver()
    try:
        sync_driver.execute("CREATE TABLE affected_rows (value INTEGER)")
        sync_driver.execute_many("INSERT INTO affected_rows VALUES (?)", [(1,), (2,), (3,)])
        async_driver.sync_driver.execute("CREATE TABLE affected_rows (value INTEGER)")
        async_driver.sync_driver.execute_many("INSERT INTO affected_rows VALUES (?)", [(1,), (2,), (3,)])
        assert (
            sync_driver.execute_many("UPDATE affected_rows SET value = value + 1 WHERE value >= ?", [(0,), (0,)]) == 6
        )
        assert (
            await async_driver.execute_many("UPDATE affected_rows SET value = value + 1 WHERE value >= ?", [(0,), (0,)])
            == 6
        )
        assert sync_driver.execute_many("DELETE FROM affected_rows WHERE value >= ?", [(99,)]) == 0
        assert await async_driver.execute_many("DELETE FROM affected_rows WHERE value >= ?", [(99,)]) == 0
        assert sync_driver.execute_many("DELETE FROM affected_rows WHERE value >= ?", [(0,), (0,)]) == 3
        assert await async_driver.execute_many("DELETE FROM affected_rows WHERE value >= ?", [(0,), (0,)]) == 3
    finally:
        sync_driver.close()
        await async_driver.close()


class _MockLeaseGrant:
    __slots__ = ("generation", "slot_index")

    def __init__(self, slot_index: int = 0, generation: int = 1) -> None:
        self.slot_index = slot_index
        self.generation = generation


class _MockLeaseTable:
    def __init__(self) -> None:
        self.acquired: int = 0
        self.released: int = 0

    def acquire(self, worker_pid: int) -> _MockLeaseGrant:
        _ = worker_pid
        self.acquired += 1
        return _MockLeaseGrant(slot_index=0, generation=self.acquired)

    def release(self, slot_index: int, generation: int) -> None:
        _ = (slot_index, generation)
        self.released += 1


class _MockStatementEntry:
    __slots__ = ("compiled_sql", "param_count", "portal_name")

    def __init__(self, param_count: int, compiled_sql: bytes, portal_name: bytes) -> None:
        self.param_count = param_count
        self.compiled_sql = compiled_sql
        self.portal_name = portal_name


class _MockStatementCache:
    def __init__(self) -> None:
        self.entries: dict[tuple[int, str], _MockStatementEntry] = {}
        self.hits: int = 0
        self.misses: int = 0

    def lookup(self, dialect_tag: int, sql: str) -> _MockStatementEntry | None:
        key = (dialect_tag, sql)
        if key in self.entries:
            self.hits += 1
            return self.entries[key]
        self.misses += 1
        return None

    def insert(
        self,
        dialect_tag: int,
        sql: str,
        param_count: int,
        compiled_sql: bytes,
        portal_name: bytes,
    ) -> None:
        self.entries[(dialect_tag, sql)] = _MockStatementEntry(param_count, compiled_sql, portal_name)


class _MockBroker:
    def __init__(self) -> None:
        self.lease_table = _MockLeaseTable()
        self.statement_cache = _MockStatementCache()


def test_postgres_jsonb_pgcopy_and_arrow_shm_cursor() -> None:
    """Verify PostgreSQL JSONB wire helpers, PGCOPY binary framing, and zero-copy Arrow SHM cursor."""
    encoded_jsonb = encode_pg_jsonb({"tenant": "acme", "active": True})
    assert encoded_jsonb[:1] == b"\x01"
    stripped = strip_pg_jsonb_header(memoryview(encoded_jsonb))
    assert bytes(stripped).startswith(b"{")

    rows = [(1, "alpha", b"\x00\xff"), (2, None, "beta")]
    copy_buf = encode_pgcopy_rows(rows)
    decoded_rows = decode_pgcopy_rows(copy_buf)
    assert len(decoded_rows) == 2
    assert decoded_rows[0][0] == b"1"
    assert decoded_rows[0][1] == b"alpha"
    assert decoded_rows[1][1] is None

    dict_rows = [{"id": idx, "name": f"row-{idx}"} for idx in range(10)]
    region_view = write_arrow_shm(dict_rows)
    cursor = read_arrow_shm(region_view, len(region_view))
    assert isinstance(cursor, ArrowCursor)
    assert len(cursor) == 10
    assert len(cursor.buffer_view()) == len(region_view)
    assert cursor.to_pylist() == dict_rows


def test_sqlite_initialization_pragmas_and_broker_coordination(tmp_path: Path) -> None:
    """Verify SQLite WAL initialization pragmas, broker lease coordination, and Arrow SHM threshold."""
    db_file = tmp_path / "test_shm.sqlite3"
    cfg = SQLiteConfig(path=str(db_file), mmap_size=67_108_864, busy_timeout_ms=3000)
    pragmas = cfg.initialization_pragmas()
    assert "PRAGMA journal_mode = WAL" in pragmas
    assert "PRAGMA synchronous = NORMAL" in pragmas
    assert "PRAGMA foreign_keys = ON" in pragmas
    assert "PRAGMA busy_timeout = 3000" in pragmas
    assert "PRAGMA mmap_size = 67108864" in pragmas

    broker = _MockBroker()
    driver = SyncSQLiteDriver(cfg, broker=broker)
    driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
    driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "first"))
    driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (2, "second"))

    rows = driver.select("SELECT id, label FROM items ORDER BY id")
    assert rows == [{"id": 1, "label": "first"}, {"id": 2, "label": "second"}]
    assert broker.lease_table.acquired == 4
    assert broker.lease_table.released == 4

    arrow_cur = driver.select_to_arrow("SELECT id, label FROM items ORDER BY id", threshold_rows=1)
    assert arrow_cur.shm_region is not None
    assert arrow_cur.to_pylist() == rows
    driver.close()


def test_cloudsql_and_alloydb_dialers_and_ephemeral_keys() -> None:
    """Verify Cloud SQL and AlloyDB dialer URI parsing, MetadataExchange protobuf encoding, and RSA key generation."""
    csql_cfg = CloudSQLConfig(
        instance_connection_name="my-proj:us-central1:my-inst",
        enable_iam_auth=True,
    )
    csql_dialer = SyncCloudSQLDialer(csql_cfg)
    assert csql_dialer.uri.project == "my-proj"
    assert csql_dialer.uri.region == "us-central1"
    assert csql_dialer.uri.instance == "my-inst"
    pub_pem, priv_pem = csql_dialer.generate_ephemeral_keypair()
    assert "BEGIN PUBLIC KEY" in pub_pem or "BEGIN RSA PUBLIC KEY" in pub_pem
    assert "PRIVATE KEY" in priv_pem

    alloy_cfg = AlloyDBConfig(
        instance_uri="projects/p1/locations/us-east1/clusters/c1/instances/i1",
        enable_iam_auth=True,
    )
    alloy_dialer = SyncAlloyDBDialer(alloy_cfg)
    wire = alloy_dialer.encode_metadata_exchange("Vertebra/1.0", AlloyDBAuthType.IAM, "tok-123")
    total_len = struct.unpack(">I", wire[:4])[0]
    assert total_len == len(wire) - 4
    decoded = alloy_dialer.decode_metadata_exchange(wire)
    assert decoded["user_agent"] == "Vertebra/1.0"
    assert decoded["auth_type"] == AlloyDBAuthType.IAM


def test_oracle_tns_descriptor_and_wallet_extraction(tmp_path: Path) -> None:
    """Verify Oracle TNS connect descriptor formatting, tnsnames.ora parsing, and in-memory Wallet zip loading."""
    desc = OracleConnectDescriptor(
        host="adb.us-ashburn-1.oraclecloud.com",
        port=1522,
        service_name="g12345_mydb_high.adb.oraclecloud.com",
        security_cert_dn="CN=adb.us-ashburn-1.oraclecloud.com",
        retry_count=3,
        retry_delay=2,
        transport_connect_timeout=5,
        protocol="TCPS",
    )
    tns_str = desc.to_tns_descriptor()
    assert "(PROTOCOL=TCPS)" in tns_str
    assert "(HOST=adb.us-ashburn-1.oraclecloud.com)" in tns_str
    assert "(PORT=1522)" in tns_str
    assert "(SERVICE_NAME=g12345_mydb_high.adb.oraclecloud.com)" in tns_str
    assert '(SSL_SERVER_CERT_DN="CN=adb.us-ashburn-1.oraclecloud.com")' in tns_str

    tnsnames_content = f"""
    mydb_high = {tns_str}
    mydb_low = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.0.5)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=low_svc)))
    """
    parsed = parse_tnsnames_ora(tnsnames_content)
    assert "MYDB_HIGH" in parsed
    assert "MYDB_LOW" in parsed

    resolved_high = resolve_tns_alias(tnsnames_content, "mydb_high")
    assert resolved_high is not None
    assert resolved_high.host == "adb.us-ashburn-1.oraclecloud.com"
    assert resolved_high.port == 1522
    assert resolved_high.protocol == "TCPS"
    assert resolved_high.security_cert_dn == "CN=adb.us-ashburn-1.oraclecloud.com"

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w") as zf:
        zf.writestr("tnsnames.ora", tnsnames_content)
        zf.writestr("sqlnet.ora", "WALLET_LOCATION = (SOURCE = (METHOD = file))")
        zf.writestr("ewallet.pem", b"-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----")
        zf.writestr("cwallet.sso", b"SSO_BINARY_BYTES")

    wallet_zip_bytes = zip_buf.getvalue()
    wallet = OracleWallet.from_zip_bytes(wallet_zip_bytes)
    assert b"BEGIN CERTIFICATE" in wallet.ewallet_pem
    assert wallet.cwallet_sso == b"SSO_BINARY_BYTES"
    wallet_desc = wallet.resolve_alias("mydb_low")
    assert wallet_desc is not None
    assert wallet_desc.host == "10.0.0.5"
    assert wallet_desc.service_name == "low_svc"

    zip_file = tmp_path / "Wallet_mydb.zip"
    zip_file.write_bytes(wallet_zip_bytes)
    ora_cfg = OracleConfig(wallet_path=str(zip_file), tns_alias="mydb_high", user="admin", password="pw")
    assert ora_cfg.connect_descriptor().host == "adb.us-ashburn-1.oraclecloud.com"

    with pytest.raises(ImproperConfigurationError):
        OracleWallet.from_zip_bytes(b"not-a-zip")


def test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs() -> None:
    """Verify Oracle 26ai VECTOR (FLOAT32/FLOAT64/INT8/BINARY), OSON, TNS packets, and TTC session piggybacking."""
    vec_f32 = [0.25, -0.5, 1.0, 3.5]
    enc_f32 = encode_vector_f32(vec_f32)
    assert decode_vector_f32(enc_f32) == pytest.approx(vec_f32)

    vec_f64 = [1.125, -2.5, 42.0]
    enc_f64 = encode_vector_f64(vec_f64)
    assert decode_vector_f64(enc_f64) == pytest.approx(vec_f64)

    vec_i8 = [-128, -1, 0, 1, 127]
    enc_i8 = encode_vector_i8(vec_i8)
    assert decode_vector_i8(enc_i8) == vec_i8

    vec_bin = bytes([0b1010_0101, 0b1111_0000])
    enc_bin = encode_vector_binary(vec_bin)
    assert decode_vector_binary(enc_bin) == vec_bin

    with pytest.raises(VertebraError):
        decode_vector_f32(b"\x00\x01\x02\x00\x00\x00\x00\x01\x00\x00\x00\x00")

    payload_obj = {"doc_id": 101, "title": "Oracle 26ai Duality View", "tags": ["json", "oson"]}
    oson_bytes = encode_oson(payload_obj)
    assert oson_bytes[:3] == OSON_MAGIC
    assert decode_oson(oson_bytes) == payload_obj

    with pytest.raises(VertebraError):
        decode_oson(b"invalid")

    tns_desc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))"
    connect_pkt = encode_tns_connect_packet(tns_desc)
    pkt_type, flags, pkt_body = decode_tns_packet(connect_pkt)
    assert pkt_type == 1
    assert flags == 0
    assert pkt_body == tns_desc.encode("utf-8")

    data_pkt = encode_tns_data_packet(0x0040, b"TTC_QUERY")
    d_type, d_flags, d_body = decode_tns_packet(data_pkt)
    assert d_type == 6
    assert d_flags == 0x0040
    assert d_body == b"TTC_QUERY"

    ttc_frame = encode_ttc_session_attrs(
        client_identifier="user-42",
        module="vertebra-api",
        action="GET /orders",
        ecid="00af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    )
    assert ttc_frame[:3] == b"\x11\x6e\x04"
    decoded_attrs = decode_ttc_session_attrs(ttc_frame)
    assert decoded_attrs["client_identifier"] == "user-42"
    assert decoded_attrs["module"] == "vertebra-api"
    assert decoded_attrs["action"] == "GET /orders"
    assert decoded_attrs["ecid"] == "00af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

    broker = _MockBroker()
    ora_driver = SyncOracleDriver(
        OracleConfig(client_identifier="tenant-7", module="checkout", action="pay"),
        broker=broker,
    )
    ora_driver.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
    assert ora_driver.last_piggyback_frame.startswith(b"\x11\x6e")
    ora_driver.execute("INSERT INTO orders (id, total) VALUES (:1, :2)", (10, 250))
    assert ora_driver.select("SELECT id, total FROM orders WHERE id = :id", {"id": 10}) == [{"id": 10, "total": 250}]
    assert broker.statement_cache.misses >= 1
    ora_driver.close()


def test_mssql_tds_framing_and_placeholder_rewriter() -> None:
    """Verify TDS 7.4 packet framing, SQLBatch ALL_HEADERS encoding, and single-pass @P1 placeholder rewriting."""
    pkt = encode_tds_packet(TDS_PKT_PRE_LOGIN, 1, True, b"\x00\x00\x1a\x00\x06\x04")
    pkt_type, is_eom, pkt_id, payload = decode_tds_packet(pkt)
    assert pkt_type == TDS_PKT_PRE_LOGIN
    assert is_eom is True
    assert pkt_id == 1
    assert payload == b"\x00\x00\x1a\x00\x06\x04"

    batch = encode_tds_sql_batch("SELECT 1", 0x1122_3344_5566_7788)
    b_type, b_eom, _, b_payload = decode_tds_packet(batch)
    assert b_type == TDS_PKT_SQL_BATCH
    assert b_eom is True
    assert struct.unpack("<I", b_payload[0:4])[0] == 22
    assert struct.unpack("<Q", b_payload[10:18])[0] == 0x1122_3344_5566_7788
    assert b_payload[22:].decode("utf-16-le") == "SELECT 1"

    sql = (
        "SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t "
        "WHERE a = ? AND b = $2 -- ? $3 comment\n AND /* ? $4 */ c = ?"
    )
    rewritten, count = rewrite_mssql_placeholders(sql)
    assert rewritten == (
        "SELECT 'a?b''$1', \"col?name\", [col$2?], * FROM t "
        "WHERE a = @P1 AND b = @P2 -- ? $3 comment\n AND /* ? $4 */ c = @P3"
    )
    assert count == 3

    broker = _MockBroker()
    mssql_driver = SyncMSSQLDriver(MSSQLConfig(), broker=broker)
    mssql_driver.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, code TEXT)")
    mssql_driver.execute("INSERT INTO ledger (id, code) VALUES (?, ?)", (7, "USD"))
    assert mssql_driver.last_rewritten_sql == "INSERT INTO ledger (id, code) VALUES (@P1, @P2)"
    assert mssql_driver.select("SELECT id, code FROM ledger WHERE id = ?", (7,)) == [{"id": 7, "code": "USD"}]
    assert broker.statement_cache.hits >= 0
    mssql_driver.close()


def test_duckdb_post_init_bootstrap_and_arrow_shm_streaming() -> None:
    """Verify DuckDB post-init SET statements (`parquet_metadata_cache`) and zero-copy Arrow SHM streaming."""
    cfg = DuckDBConfig(
        path=":memory:",
        memory_limit_mb=1024,
        threads=4,
        parquet_metadata_cache=True,
        httpfs_connection_caching=True,
        enable_http_metadata_cache=True,
    )
    stmts = duckdb_bootstrap_statements(cfg)
    assert "SET memory_limit = '1024MB'" in stmts
    assert "SET threads = 4" in stmts
    assert "SET parquet_metadata_cache = true" in stmts
    assert "SET http_keep_alive = true" in stmts
    assert "SET enable_http_metadata_cache = true" in stmts

    broker = _MockBroker()
    driver = SyncDuckDBDriver(cfg, broker=broker)
    assert "SET parquet_metadata_cache = true" in driver.executed_bootstrap_statements
    driver.execute("CREATE TABLE metrics (id INTEGER, score DOUBLE)")
    driver.execute("INSERT INTO metrics VALUES (1, 98.5), (2, 87.25)")
    arrow_cur = driver.select_to_arrow("SELECT id, score FROM metrics ORDER BY id", threshold_rows=1)
    assert arrow_cur.shm_region is not None
    assert arrow_cur.to_pylist() == [{"id": 1, "score": 98.5}, {"id": 2, "score": 87.25}]
    driver.close()


@pytest.mark.parametrize(
    ("sync_cfg_cls", "async_cfg_cls", "async_drv_cls"),
    [
        (PostgreSQLSyncConfig, PostgreSQLAsyncConfig, AsyncPostgreSQLDriver),
        (MySQLSyncConfig, MySQLAsyncConfig, AsyncMySQLDriver),
        (SQLiteSyncConfig, SQLiteAsyncConfig, AsyncSQLiteDriver),
        (OracleSyncConfig, OracleAsyncConfig, AsyncOracleDriver),
        (MSSQLSyncConfig, MSSQLAsyncConfig, AsyncMSSQLDriver),
        (DuckDBSyncConfig, DuckDBAsyncConfig, AsyncDuckDBDriver),
    ],
)
@pytest.mark.anyio
async def test_sqlspec_adapter_config_flags_and_async_driver_close(
    sync_cfg_cls: type[Any],
    async_cfg_cls: type[Any],
    async_drv_cls: type[Any],
) -> None:
    """Verify Sync and Async SQLSpec adapter flags and async driver teardown across all 6 engines."""
    scfg = sync_cfg_cls()
    assert scfg.is_async is False
    assert scfg.arrow_threshold_rows == ARROW_SHM_THRESHOLD_ROWS
    acfg = async_cfg_cls()
    assert acfg.is_async is True
    async_drv = async_drv_cls()
    await async_drv.close()


@pytest.mark.anyio
async def test_sqlspec_adapters_and_async_drivers() -> None:
    """Verify DatabaseRegistry resolution and SQLite/DuckDB SQLSpec session round trips."""
    reg = DatabaseRegistry()
    assert isinstance(reg.parse_url("postgres://localhost/db"), PostgreSQLConfig)
    assert isinstance(reg.parse_url("mysql://localhost/db"), MySQLConfig)
    assert isinstance(reg.parse_url("sqlite:///:memory:"), SQLiteConfig)
    assert isinstance(reg.parse_url("oracle://localhost:1521/FREEPDB1"), OracleConfig)
    assert isinstance(reg.parse_url("mssql://localhost:1433/master"), MSSQLConfig)
    assert isinstance(reg.parse_url("duckdb:///:memory:"), DuckDBConfig)

    sqlite_sync = SQLiteSyncConfig(path=":memory:")
    with sqlite_sync.provide_session() as session:
        session.execute("CREATE TABLE t_sync (k INTEGER PRIMARY KEY, v TEXT)")
        session.execute("INSERT INTO t_sync (k, v) VALUES (?, ?)", (1, "one"))
        assert session.select_one("SELECT k, v FROM t_sync WHERE k = ?", (1,)) == {"k": 1, "v": "one"}
        assert session.select_value("SELECT v FROM t_sync WHERE k = ?", (1,)) == "one"
    sqlite_sync.close()

    duck_async = DuckDBAsyncConfig(path=":memory:")
    async with duck_async.provide_session() as asession:
        await asession.execute("CREATE TABLE t_async (k INTEGER, v TEXT)")
        await asession.execute("INSERT INTO t_async VALUES (42, 'answer')")
        assert await asession.select_value("SELECT v FROM t_async WHERE k = 42") == "answer"
        arrow_res = await asession.select_to_arrow("SELECT k, v FROM t_async", threshold_rows=1)
        assert arrow_res.data.to_pylist() == [{"k": 42, "v": "answer"}]
    await duck_async.close()


@pytest.mark.parametrize(
    "dialect_name",
    [
        "postgresql.vtb",
        "mysql.vtb",
        "sqlite.vtb",
        "oracle.vtb",
        "mssql.vtb",
        "duckdb.vtb",
    ],
)
def test_sqlalchemy_vtb_dialect_nullpool_selection(dialect_name: str) -> None:
    """Verify SQLAlchemy +vtb dialects select NullPool when shm_broker=true is enabled."""
    assert len(ALL_DIALECT_SCHEMES) == 36
    assert registry.load("mssql.vtb") is SyncMSSQLDialect
    assert registry.load("duckdb.vtb") is SyncDuckDBDialect
    dialect_cls = registry.load(dialect_name)
    broker_url = URL.create(dialect_name.replace(".", "+"), host="localhost", query={"shm_broker": "true"})
    assert dialect_cls.get_pool_class(broker_url) is NullPool


@pytest.mark.parametrize(
    "url",
    [
        "sqlite+vtb:///:memory:",
        "oracle+vtb://localhost:1521/FREEPDB1",
        "mssql+vtb://localhost:1433/master",
        "duckdb+vtb:///:memory:",
    ],
)
def test_sqlalchemy_dbapi_broker_forwarding(url: str) -> None:
    """Verify SyncDBAPIConnection forwards DBAPI operations through the SHM broker."""
    broker = _MockBroker()
    conn = SyncDBAPIConnection(url, broker=broker)
    cur = conn.cursor()
    assert isinstance(cur, SyncDBAPICursor)
    cur.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    cur.execute("INSERT INTO items VALUES (1, 'vtb')")
    cur.execute("SELECT id, name FROM items")
    assert cur.fetchone() == (1, "vtb")
    cur.close()
    conn.close()
    assert broker.lease_table.acquired >= 2
    assert broker.lease_table.released >= 2


class _LedgerRow(msgspec.Struct, frozen=True, gc=False):
    """Typed row schema for verifying C-API vectorcall hydration across database drivers."""

    id: int
    label: str


def test_driver_schema_hydration_load_from_arrow_and_topologies() -> None:
    """Verify schema_type C-API hydration, load_from_arrow, derive_direct_config, OracleTopologyConfig, and DuckDBSecretConfig."""
    pg_cfg = PostgreSQLConfig(
        dsn="postgresql://localhost:6432/app",
        direct_dsn="postgresql://localhost:5432/app",
        pooler_mode="transaction",
        pgbouncer=True,
    )
    assert pg_cfg.effective_statement_cache_size == 0
    direct_pg = pg_cfg.derive_direct_config()
    assert direct_pg.dsn == "postgresql://localhost:5432/app"
    assert direct_pg.pooler_mode == "session"
    assert direct_pg.pgbouncer is False

    with pytest.raises(ImproperConfigurationError):
        PostgreSQLConfig(
            cloud_sql_instance="p:r:i",
            alloydb_instance_uri="projects/p/locations/r/clusters/c/instances/i",
        )

    pg_sync_adapter = PostgreSQLSyncConfig(pg_cfg)
    direct_sync_adapter = pg_sync_adapter.derive_direct_config()
    assert direct_sync_adapter.driver_config.pooler_mode == "session"

    topo = OracleTopologyConfig(
        hosts=("rac1.internal", "rac2.internal"),
        port=1521,
        service_name="PROD_SVC",
        load_balance=True,
        failover=True,
    )
    tns_topo = topo.to_tns_descriptor()
    assert "(LOAD_BALANCE=ON)" in tns_topo
    assert "(FAILOVER=ON)" in tns_topo
    assert "(HOST=rac1.internal)" in tns_topo
    assert "(HOST=rac2.internal)" in tns_topo

    ora_cfg = OracleConfig(
        topology=topo,
        driver_features=OracleDriverFeatures(enable_lowercase_column_names=True),
    )
    derived_ora = ora_cfg.derive_pool(service_name="REPORT_SVC", edition="V2")
    assert derived_ora.service_name == "REPORT_SVC"
    assert derived_ora.edition == "V2"

    secret = DuckDBSecretConfig(
        name="s3_prod",
        secret_type="S3",
        key_id="key1",
        secret="sec1",
        region="us-west-2",
        endpoint="127.0.0.1:9000",
    )
    duck_cfg = DuckDBConfig(secrets=(secret,))
    stmts = duck_cfg.bootstrap_statements()
    assert any("CREATE OR REPLACE SECRET s3_prod" in s for s in stmts)

    sqlite_drv = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    sqlite_drv.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
    inserted = sqlite_drv.load_from_arrow(
        "items",
        [{"id": 1, "label": "alpha"}, {"id": 2, "label": "beta"}],
    )
    assert inserted == 2
    hydrated = sqlite_drv.select(
        "SELECT id, label FROM items ORDER BY id",
        schema_type=_LedgerRow,
    )
    assert hydrated == [_LedgerRow(id=1, label="alpha"), _LedgerRow(id=2, label="beta")]
    assert sqlite_drv.select_one(
        "SELECT id, label FROM items WHERE id = ?",
        (1,),
        schema_type=_LedgerRow,
    ) == _LedgerRow(id=1, label="alpha")
    sqlite_drv.close()


def test_native_arrow_cursor_pep3118_zero_copy_survives_shm_unlink() -> None:
    """Verify PEP 3118 memoryview and ArrowCursor.to_pyarrow() survive cursor.release() unlink."""
    batch_a = pa.RecordBatch.from_pydict({"id": [1, 2], "name": ["alpha", "beta"]})
    batch_b = pa.RecordBatch.from_pydict({"id": [3, 4], "name": ["gamma", "delta"]})
    table = pa.Table.from_batches([batch_a, batch_b])
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_batch(batch_a)
        writer.write_batch(batch_b)
    ipc_bytes = sink.getvalue().to_pybytes()

    shm_name = f"/vtb-test-pep3118-{os.getpid()}"
    cursor = NativeArrowCursor.write_ipc_to_shm(shm_name, ipc_bytes, 4)
    assert cursor.row_count == 4
    assert cursor.byte_len == len(ipc_bytes)

    mv = memoryview(cursor)
    view = cursor.buffer_view()
    assert mv.readonly
    assert len(mv) == len(ipc_bytes)
    assert bytes(view) == ipc_bytes

    assert cursor.release() is True
    assert cursor.release() is False

    decoded_from_mv = pa.ipc.open_stream(pa.py_buffer(mv)).read_all()
    assert decoded_from_mv.to_pylist() == [
        {"id": 1, "name": "alpha"},
        {"id": 2, "name": "beta"},
        {"id": 3, "name": "gamma"},
        {"id": 4, "name": "delta"},
    ]
    assert cursor.to_pyarrow().to_pylist() == decoded_from_mv.to_pylist()
    assert cursor.to_arrow_table().num_rows == 4
    del decoded_from_mv
    mv.release()
    view.release()


def test_unified_db_arrow_cursor_identity_and_native_delegation() -> None:
    """Verify vertebra.db.ArrowCursor identity across drivers and native _vertebra.ArrowCursor delegation."""
    assert db_pkg.ArrowCursor is db_arrow_mod.ArrowCursor
    assert db_postgres_mod.ArrowCursor is db_pkg.ArrowCursor
    assert db_duckdb_mod.ArrowCursor is db_pkg.ArrowCursor
    assert db_mysql_mod.ArrowCursor is db_pkg.ArrowCursor
    assert db_oracle_mod.ArrowCursor is db_pkg.ArrowCursor
    assert db_sqlite_mod.ArrowCursor is db_pkg.ArrowCursor
    assert db_mssql_mod.ArrowCursor is db_pkg.ArrowCursor

    tbl = pa.Table.from_pydict({"k": [10, 20, 30], "v": ["x", "y", "z"]})
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as writer:
        writer.write_table(tbl)
    raw_ipc = sink.getvalue().to_pybytes()

    native_cur = NativeArrowCursor.write_ipc_to_shm(f"/vtb-test-wrap-{os.getpid()}", raw_ipc, 3)
    with db_pkg.ArrowCursor(native_cur) as wrapped:
        assert wrapped.used_shm is True
        assert len(wrapped) == 3
        assert wrapped.row_count == 3
        assert bytes(wrapped.buffer_view()) == raw_ipc
        assert wrapped.to_pyarrow().num_rows == 3
        assert wrapped.to_arrow_table().num_rows == 3
        assert wrapped.to_pylist() == [
            {"k": 10, "v": "x"},
            {"k": 20, "v": "y"},
            {"k": 30, "v": "z"},
        ]


def test_sync_tenant_predicate_raises_when_guc_set_fails() -> None:
    """A failing tenant GUC SET propagates instead of returning an unenforced predicate."""
    config = SQLiteSyncConfig(path=":memory:")
    predicate = TenantPredicate(table_alias="t", tenant_value="acme")
    try:
        with config.provide_session() as session, pytest.raises(NotSupportedError):
            session.apply_tenant_predicate(predicate)
    finally:
        config.close()


@pytest.mark.anyio
async def test_async_tenant_predicate_raises_when_guc_set_fails() -> None:
    """The async driver propagates a failing tenant GUC SET."""
    config = SQLiteAsyncConfig(path=":memory:")
    predicate = TenantPredicate(table_alias="t", tenant_value="acme")
    try:
        async with config.provide_session() as session:
            with pytest.raises(NotSupportedError):
                await session.apply_tenant_predicate(predicate)
    finally:
        await config.close()


@pytest.mark.parametrize("engine", ["postgres", "oracle"])
def test_sync_tenant_session_setting_preserves_failure_identity(engine: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Supported adapter session-setting errors escape unchanged."""
    native = SyncPostgreSQLDriver() if engine == "postgres" else SyncOracleDriver()
    failure = RuntimeError("session setting refused")

    def fail(*args: object, **kwargs: object) -> int:
        raise failure

    monkeypatch.setattr(type(native), "execute", fail)
    adapter = PostgreSQLSyncDriver(native) if engine == "postgres" else OracleSyncDriver(native)
    try:
        with pytest.raises(RuntimeError) as raised:
            adapter.apply_tenant_predicate(TenantPredicate(table_alias="t", tenant_value="acme"))
        assert raised.value is failure
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("engine", ["postgres", "oracle"])
async def test_async_tenant_session_setting_preserves_failure_identity(
    engine: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Async supported adapter session-setting errors escape unchanged."""
    native = AsyncPostgreSQLDriver() if engine == "postgres" else AsyncOracleDriver()
    failure = RuntimeError("session setting refused")

    async def fail(*args: object, **kwargs: object) -> int:
        raise failure

    monkeypatch.setattr(type(native), "execute", fail)
    adapter = PostgreSQLAsyncDriver(native) if engine == "postgres" else OracleAsyncDriver(native)
    try:
        with pytest.raises(RuntimeError) as raised:
            await adapter.apply_tenant_predicate(TenantPredicate(table_alias="t", tenant_value="acme"))
        assert raised.value is failure
    finally:
        await native.close()


@pytest.mark.parametrize("config_defaults", [False, True])
def test_postgresql_tenant_session_prepares_psycopg_binds(config_defaults: bool) -> None:
    """The native sync port receives psycopg binds and the exact session values."""

    class Port:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        def execute(self, sql: str, parameters: Any) -> list[Any]:
            assert sql.casefold() == "select set_config(%s, %s, %s)"
            self.calls.append((sql, tuple(parameters)))
            return []

    port = Port()
    native = SyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://"), pool=port)
    config = PostgreSQLSyncConfig(native.config, broker=object(), share_pool=False)
    adapter = PostgreSQLSyncDriver(native, statement_config=config.statement_config if config_defaults else None)
    try:
        result = adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))
        assert result == "o.tenant_id = $1"
        assert len(port.calls) == 1
        assert port.calls[0][1] == ("app.tenant_id", "acme", True)
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("config_defaults", [False, True])
async def test_postgresql_tenant_session_prepares_asyncpg_binds(config_defaults: bool) -> None:
    """The async native port receives numeric binds and the exact session values."""

    class Port:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        async def execute_tuples(self, sql: str, parameters: Any) -> list[Any]:
            assert sql.upper() == "SELECT SET_CONFIG($1, $2, $3)"
            self.calls.append((sql, tuple(parameters)))
            return []

    port = Port()
    native = AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://"), pool=port)
    config = PostgreSQLAsyncConfig(native.config, broker=object(), share_pool=False)
    adapter = PostgreSQLAsyncDriver(native, statement_config=config.statement_config if config_defaults else None)
    try:
        result = await adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))
        assert result == "o.tenant_id = $1"
        assert len(port.calls) == 1
        assert port.calls[0][1] == ("app.tenant_id", "acme", True)
    finally:
        await native.close()


def test_oracle_tenant_context_stand_in_sets_session_attribute() -> None:
    """The Oracle stand-in records named context binds without claiming live VPD support."""
    native = SyncOracleDriver()
    try:
        adapter = OracleSyncDriver(native)
        assert adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme")) == (
            "o.tenant_id = :tenant_id"
        )
        assert native._session_context == {("VERTEBRA_CTX", "tenant_id"): "acme"}
    finally:
        native.close()


@pytest.mark.anyio
async def test_async_oracle_tenant_context_stand_in_sets_session_attribute() -> None:
    """The async Oracle wrapper delegates named context binds to the same stand-in."""
    native = AsyncOracleDriver()
    try:
        adapter = OracleAsyncDriver(native)
        assert await adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme")) == (
            "o.tenant_id = :tenant_id"
        )
        assert native.sync_driver._session_context == {("VERTEBRA_CTX", "tenant_id"): "acme"}
    finally:
        await native.close()


@pytest.mark.parametrize(
    "driver_cls",
    [SyncOracleDriver, SyncMSSQLDriver, SyncDuckDBDriver],
    ids=["oracle-sync", "mssql-sync", "duckdb-sync"],
)
@pytest.mark.parametrize(
    ("table_name", "columns"),
    [
        ("items; DROP TABLE items--", ("id", "label")),
        ("items", ("id", "label) VALUES (1, 'x');--")),
        ("schema..items", ("id",)),
        ("items", ("schema.col",)),
    ],
    ids=["unsafe-table", "unsafe-column", "empty-dot-table", "dotted-column"],
)
def test_enterprise_and_olap_sync_load_from_arrow_rejects_unsafe_identifiers(
    driver_cls: type[Any],
    table_name: str,
    columns: tuple[str, ...],
) -> None:
    """SyncOracleDriver, SyncMSSQLDriver, and SyncDuckDBDriver reject unsafe table and column identifiers."""
    driver = driver_cls()
    try:
        with pytest.raises(ValueError, match="Invalid"):
            driver.load_from_arrow(table_name, [{"id": 1, "label": "ok"}], columns=columns)
    finally:
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "driver_cls",
    [AsyncOracleDriver, AsyncMSSQLDriver, AsyncDuckDBDriver],
    ids=["oracle-async", "mssql-async", "duckdb-async"],
)
@pytest.mark.parametrize(
    ("table_name", "columns"),
    [
        ("items; DROP TABLE items--", ("id", "label")),
        ("items", ("id", "label) VALUES (1, 'x');--")),
    ],
    ids=["unsafe-table", "unsafe-column"],
)
async def test_enterprise_and_olap_async_load_from_arrow_rejects_unsafe_identifiers(
    driver_cls: type[Any],
    table_name: str,
    columns: tuple[str, ...],
) -> None:
    """AsyncOracleDriver, AsyncMSSQLDriver, and AsyncDuckDBDriver reject unsafe table and column identifiers."""
    driver = driver_cls()
    try:
        with pytest.raises(ValueError, match="Invalid"):
            await driver.load_from_arrow(table_name, [{"id": 1, "label": "ok"}], columns=columns)
    finally:
        await driver.close()


@pytest.mark.anyio
async def test_oracle_embedding_pushdown_batches_via_utl_to_embeddings() -> None:
    """OracleEmbeddingPushdown batches multi-text calls into a single DBMS_VECTOR.UTL_TO_EMBEDDINGS query."""

    class BatchOraclePort:
        def __init__(self) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []

        def select(self, sql: str, parameters: dict[str, Any]) -> list[dict[str, Any]]:
            self.calls.append((sql, dict(parameters)))
            return [
                {"embedding": encode_vector_f32([3.0, 4.0, 0.0, 0.0])},
                {"embedding": encode_vector_f32([0.0, 5.0, 12.0, 0.0])},
                {"embedding": encode_vector_f32([8.0, 0.0, 15.0, 0.0])},
            ]

        def execute(self, sql: str, parameters: dict[str, Any]) -> None:
            self.calls.append((sql, dict(parameters)))

    port = BatchOraclePort()
    pushdown = OracleEmbeddingPushdown(port, provider="local", dimensions=4, normalize=True)
    batch = await pushdown.embed_batch(["doc-1", "doc-2", "doc-3"])
    assert batch.count == 3
    assert len(port.calls) == 1
    sql, params = port.calls[0]
    assert "DBMS_VECTOR.UTL_TO_EMBEDDINGS" in sql
    assert "doc-1" in params["texts"]
    assert batch[0].to_list() == pytest.approx([0.6, 0.8, 0.0, 0.0])
    assert batch[1].to_list() == pytest.approx([0.0, 5.0 / 13.0, 12.0 / 13.0, 0.0])
    assert batch[2].to_list() == pytest.approx([8.0 / 17.0, 0.0, 15.0 / 17.0, 0.0])
