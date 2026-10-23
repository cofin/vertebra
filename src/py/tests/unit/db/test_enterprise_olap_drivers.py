"""Unit tests for Rust-native SHM database drivers, Oracle/MSSQL/DuckDB codecs, SQLSpec adapters, and +vtb dialects."""

from __future__ import annotations

import io
import struct
import zipfile
from typing import TYPE_CHECKING

import pytest

from vertebra.db import (
    ARROW_SHM_THRESHOLD_ROWS,
    AlloyDbAuthType,
    AlloyDbConfig,
    AlloyDbDialer,
    ArrowCursor,
    AsyncDuckdbDriver,
    AsyncMssqlDriver,
    AsyncMysqlDriver,
    AsyncOracleDriver,
    AsyncPostgresDriver,
    AsyncSqliteDriver,
    CloudSqlConfig,
    CloudSqlDialer,
    DatabaseRegistry,
    DuckdbConfig,
    MssqlConfig,
    MysqlConfig,
    OracleConfig,
    OracleConnectDescriptor,
    OracleWallet,
    PostgresConfig,
    SqliteConfig,
    SyncDuckdbDriver,
    SyncMssqlDriver,
    SyncOracleDriver,
    SyncSqliteDriver,
    decode_oson,
    decode_vector_binary,
    decode_vector_f32,
    decode_vector_f64,
    decode_vector_i8,
    duckdb_bootstrap_statements,
    encode_oson,
    encode_tds_sql_batch,
    encode_ttc_session_attrs,
    encode_vector_binary,
    encode_vector_f32,
    encode_vector_f64,
    encode_vector_i8,
    parse_tnsnames_ora,
    resolve_tns_alias,
    rewrite_mssql_placeholders,
)
from vertebra.db.mssql import (
    TDS_PKT_PRE_LOGIN,
    TDS_PKT_SQL_BATCH,
    decode_tds_packet,
    encode_tds_packet,
)
from vertebra.db.oracle import (
    OSON_MAGIC,
    decode_tns_packet,
    decode_ttc_session_attrs,
    encode_tns_connect_packet,
    encode_tns_data_packet,
)
from vertebra.db.postgres import (
    decode_pgcopy_rows,
    encode_pg_jsonb,
    encode_pgcopy_rows,
    read_arrow_shm,
    strip_pg_jsonb_header,
    write_arrow_shm,
)
from vertebra.extensions.sqlalchemy.dialects import (
    DBAPIConnection,
    DBAPICursor,
    DialectDescriptor,
    DuckdbDialect,
    MssqlDialect,
    MysqlDialect,
    NullPool,
    OracleDialect,
    PostgresDialect,
    SqliteDialect,
)
from vertebra.extensions.sqlspec.adapters import (
    DuckdbAsyncConfig,
    DuckdbSyncConfig,
    MssqlAsyncConfig,
    MssqlSyncConfig,
    MysqlAsyncConfig,
    MysqlSyncConfig,
    OracleAsyncConfig,
    OracleSyncConfig,
    PostgresAsyncConfig,
    PostgresSyncConfig,
    SqliteAsyncConfig,
    SqliteSyncConfig,
)
from vertebra.typing import ImproperConfigurationError, VertebraError

if TYPE_CHECKING:
    from pathlib import Path


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
    cfg = SqliteConfig(path=str(db_file), mmap_size=67_108_864, busy_timeout_ms=3000)
    pragmas = cfg.initialization_pragmas()
    assert "PRAGMA journal_mode = WAL" in pragmas
    assert "PRAGMA synchronous = NORMAL" in pragmas
    assert "PRAGMA foreign_keys = ON" in pragmas
    assert "PRAGMA busy_timeout = 3000" in pragmas
    assert "PRAGMA mmap_size = 67108864" in pragmas

    broker = _MockBroker()
    driver = SyncSqliteDriver(cfg, broker=broker)
    driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)")
    driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (1, "first"))
    driver.execute("INSERT INTO items (id, label) VALUES (?, ?)", (2, "second"))

    rows = driver.select("SELECT id, label FROM items ORDER BY id")
    assert rows == [{"id": 1, "label": "first"}, {"id": 2, "label": "second"}]
    assert broker.lease_table.acquired == 4
    assert broker.lease_table.released == 4

    arrow_cur = driver.select_to_arrow("SELECT id, label FROM items ORDER BY id", shm_threshold_rows=1)
    assert arrow_cur.shm_region is not None
    assert arrow_cur.to_pylist() == rows
    driver.close()


def test_cloudsql_and_alloydb_dialers_and_ephemeral_keys() -> None:
    """Verify Cloud SQL and AlloyDB dialer URI parsing, MetadataExchange protobuf encoding, and RSA key generation."""
    csql_cfg = CloudSqlConfig(
        instance_connection_name="my-proj:us-central1:my-inst",
        enable_iam_auth=True,
    )
    csql_dialer = CloudSqlDialer(csql_cfg)
    assert csql_dialer.uri.project == "my-proj"
    assert csql_dialer.uri.region == "us-central1"
    assert csql_dialer.uri.instance == "my-inst"
    pub_pem, priv_pem = csql_dialer.generate_ephemeral_keypair()
    assert "BEGIN PUBLIC KEY" in pub_pem or "BEGIN RSA PUBLIC KEY" in pub_pem
    assert "PRIVATE KEY" in priv_pem

    alloy_cfg = AlloyDbConfig(
        instance_uri="projects/p1/locations/us-east1/clusters/c1/instances/i1",
        enable_iam_auth=True,
    )
    alloy_dialer = AlloyDbDialer(alloy_cfg)
    wire = alloy_dialer.encode_metadata_exchange("Vertebra/1.0", AlloyDbAuthType.IAM, "tok-123")
    total_len = struct.unpack(">I", wire[:4])[0]
    assert total_len == len(wire) - 4
    decoded = alloy_dialer.decode_metadata_exchange(wire)
    assert decoded["user_agent"] == "Vertebra/1.0"
    assert decoded["auth_type"] == AlloyDbAuthType.IAM


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
    mssql_driver = SyncMssqlDriver(MssqlConfig(), broker=broker)
    mssql_driver.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, code TEXT)")
    mssql_driver.execute("INSERT INTO ledger (id, code) VALUES (?, ?)", (7, "USD"))
    assert mssql_driver.last_rewritten_sql == "INSERT INTO ledger (id, code) VALUES (@P1, @P2)"
    assert mssql_driver.select("SELECT id, code FROM ledger WHERE id = ?", (7,)) == [{"id": 7, "code": "USD"}]
    assert broker.statement_cache.hits >= 0
    mssql_driver.close()


def test_duckdb_post_init_bootstrap_and_arrow_shm_streaming() -> None:
    """Verify DuckDB post-init SET statements (`parquet_metadata_cache`) and zero-copy Arrow SHM streaming."""
    cfg = DuckdbConfig(
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
    driver = SyncDuckdbDriver(cfg, broker=broker)
    assert "SET parquet_metadata_cache = true" in driver.executed_bootstrap_statements
    driver.execute("CREATE TABLE metrics (id INTEGER, score DOUBLE)")
    driver.execute("INSERT INTO metrics VALUES (1, 98.5), (2, 87.25)")
    arrow_cur = driver.select_to_arrow("SELECT id, score FROM metrics ORDER BY id")
    assert arrow_cur.shm_region is not None
    assert arrow_cur.to_pylist() == [{"id": 1, "score": 98.5}, {"id": 2, "score": 87.25}]
    driver.close()


@pytest.mark.anyio
async def test_sqlspec_adapters_and_async_drivers() -> None:
    """Verify all 12 Sync and Async SQLSpec adapter configurations and DatabaseRegistry resolution."""
    reg = DatabaseRegistry()
    assert isinstance(reg.parse_url("postgres://localhost/db"), PostgresConfig)
    assert isinstance(reg.parse_url("mysql://localhost/db"), MysqlConfig)
    assert isinstance(reg.parse_url("sqlite:///:memory:"), SqliteConfig)
    assert isinstance(reg.parse_url("oracle://localhost:1521/FREEPDB1"), OracleConfig)
    assert isinstance(reg.parse_url("mssql://localhost:1433/master"), MssqlConfig)
    assert isinstance(reg.parse_url("duckdb:///:memory:"), DuckdbConfig)

    sync_configs = [
        PostgresSyncConfig(),
        MysqlSyncConfig(),
        SqliteSyncConfig(),
        OracleSyncConfig(),
        MssqlSyncConfig(),
        DuckdbSyncConfig(),
    ]
    for scfg in sync_configs:
        assert scfg.is_async is False
        assert scfg.arrow_threshold_rows == ARROW_SHM_THRESHOLD_ROWS

    async_configs = [
        PostgresAsyncConfig(),
        MysqlAsyncConfig(),
        SqliteAsyncConfig(),
        OracleAsyncConfig(),
        MssqlAsyncConfig(),
        DuckdbAsyncConfig(),
    ]
    for acfg in async_configs:
        assert acfg.is_async is True

    sqlite_sync = SqliteSyncConfig(path=":memory:")
    with sqlite_sync.provide_session() as session:
        session.execute("CREATE TABLE t_sync (k INTEGER PRIMARY KEY, v TEXT)")
        session.execute("INSERT INTO t_sync (k, v) VALUES (?, ?)", (1, "one"))
        assert session.select_one("SELECT k, v FROM t_sync WHERE k = ?", (1,)) == {"k": 1, "v": "one"}
        assert session.select_value("SELECT v FROM t_sync WHERE k = ?", (1,)) == "one"
    sqlite_sync.close()

    duck_async = DuckdbAsyncConfig(path=":memory:")
    async with duck_async.provide_session() as asession:
        await asession.execute("CREATE TABLE t_async (k INTEGER, v TEXT)")
        await asession.execute("INSERT INTO t_async VALUES (42, 'answer')")
        assert await asession.select_value("SELECT v FROM t_async WHERE k = 42") == "answer"
        arrow_res = await asession.select_to_arrow("SELECT k, v FROM t_async", shm_threshold_rows=1)
        assert arrow_res.to_pylist() == [{"k": 42, "v": "answer"}]
    await duck_async.close()

    for async_drv in (
        AsyncPostgresDriver(),
        AsyncMysqlDriver(),
        AsyncSqliteDriver(),
        AsyncOracleDriver(),
        AsyncMssqlDriver(),
        AsyncDuckdbDriver(),
    ):
        await async_drv.close()


def test_sqlalchemy_vtb_dialects_nullpool_and_dbapi_broker_forwarding() -> None:
    """Verify all 6 SQLAlchemy +vtb dialects use NullPool and forward DBAPI operations through the SHM broker."""
    vtb_map = DialectDescriptor.dialects()
    assert set(vtb_map.keys()) == {
        "postgresql+vtb",
        "mysql+vtb",
        "sqlite+vtb",
        "oracle+vtb",
        "mssql+vtb",
        "duckdb+vtb",
    }
    for dialect_cls in (
        PostgresDialect,
        MysqlDialect,
        SqliteDialect,
        OracleDialect,
        MssqlDialect,
        DuckdbDialect,
    ):
        assert dialect_cls.poolclass is NullPool
        assert dialect_cls.get_pool_class() is NullPool

    all_registered = DialectDescriptor.register_all(include_enterprise_olap=True)
    assert len(all_registered) == 12
    assert all_registered["mssql.vtb"] is MssqlDialect
    assert all_registered["duckdb.vtb"] is DuckdbDialect

    broker = _MockBroker()
    for url in (
        "sqlite+vtb:///:memory:",
        "oracle+vtb://localhost:1521/FREEPDB1",
        "mssql+vtb://localhost:1433/master",
        "duckdb+vtb:///:memory:",
    ):
        conn = DBAPIConnection(url, broker=broker)
        cur = conn.cursor()
        assert isinstance(cur, DBAPICursor)
        cur.execute("CREATE TABLE items (id INTEGER, name TEXT)")
        cur.execute("INSERT INTO items VALUES (1, 'vtb')")
        cur.execute("SELECT id, name FROM items")
        assert cur.fetchone() == (1, "vtb")
        cur.close()
        conn.close()
    assert broker.lease_table.acquired >= 12
    assert broker.lease_table.released >= 12
