"""Unit tests for Rust-native SHM database drivers, Oracle/MSSQL/DuckDB codecs, SQLSpec adapters, and +vtb dialects."""

from __future__ import annotations

import gc
import importlib
import io
import mmap
import os
import re
import sqlite3
import struct
import sys
import zipfile
from typing import TYPE_CHECKING, Any

import msgspec
import pyarrow as pa
import pytest
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, delete, insert, select
from sqlalchemy.dialects import registry
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import create_async_engine

import vertebra.db as db_pkg
import vertebra.db.adapters.duckdb as db_duckdb_mod
import vertebra.db.adapters.mssql as db_mssql_mod
import vertebra.db.adapters.mysql as db_mysql_mod
import vertebra.db.adapters.oracle as db_oracle_mod
import vertebra.db.adapters.postgres as db_postgres_mod
import vertebra.db.adapters.sqlite as db_sqlite_mod
import vertebra.db.arrow as db_arrow_mod
from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra._vertebra import ArrowCursor as NativeArrowCursor
from vertebra._vertebra import LeaseBroker, ProgrammingError
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
    DuckDBCore,
    DuckDBSecretConfig,
    SyncDuckDBDriver,
    duckdb_bootstrap_statements,
)
from vertebra.db.adapters.mssql import (
    AsyncMSSQLDriver,
    MSSQLConfig,
    MSSQLCore,
    SyncMSSQLDriver,
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
    OracleCore,
    OracleDriverFeatures,
    OracleEmbeddingPushdown,
    OracleTopologyConfig,
    OracleWallet,
    SyncOracleDriver,
    parse_tnsnames_ora,
    resolve_tns_alias,
)
from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PostgreSQLConfig,
    PostgreSQLCore,
    SyncPostgreSQLDriver,
    read_arrow_shm,
    write_arrow_shm,
)
from vertebra.db.adapters.sqlite import (
    AsyncSQLiteDriver,
    SQLiteConfig,
    SyncSQLiteDriver,
)
from vertebra.db.dbapi import (
    AsyncDBAPIConnection,
    InterfaceError,
    SyncDBAPIConnection,
    SyncDBAPICursor,
)
from vertebra.db.driver._common import (
    DUCKDB_DIALECT_TAG,
    MSSQL_DIALECT_TAG,
    ORACLE_DIALECT_TAG,
    CommonDriverMixin,
)
from vertebra.extensions.sqlalchemy.dialects import (
    ALL_DIALECT_SCHEMES,
    AsyncDuckDBDialect,
    AsyncMSSQLDialect,
    NullPool,
    SyncDuckDBDialect,
    SyncMSSQLDialect,
)
from vertebra.extensions.sqlalchemy.dialects.mssql import MSExecutionContext_vtb
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
    PostgresAsyncConfig,
    PostgresAsyncDriver,
    PostgresSyncConfig,
    PostgresSyncDriver,
    SQLiteAsyncConfig,
    SQLiteSyncConfig,
    TenantPredicate,
)
from vertebra.typing import (
    ImproperConfigurationError,
    MissingDependencyError,
    VertebraError,
    module_available,
    reset_dependency_cache,
)

if TYPE_CHECKING:
    from pathlib import Path

_DUCKDB_SKIP_REASON = "vertebra._duckdb native extension is not built"


@pytest.mark.anyio
async def test_duckdb_execute_many_reports_affected_rows() -> None:
    """Batch counts reflect matching rows for sync and async UPDATE and DELETE."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    sync_driver = SyncDuckDBDriver()
    async_driver = AsyncDuckDBDriver()
    try:
        sync_driver.execute("CREATE TABLE affected_rows (value INTEGER)")
        sync_driver.execute_many("INSERT INTO affected_rows VALUES (?)", [(1,), (2,), (3,)])
        await async_driver.execute("CREATE TABLE affected_rows (value INTEGER)")
        await async_driver.execute_many("INSERT INTO affected_rows VALUES (?)", [(1,), (2,), (3,)])
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
    __slots__ = ("compiled_sql", "param_count", "param_plan")

    def __init__(self, param_count: int, compiled_sql: bytes, param_plan: bytes) -> None:
        self.param_count = param_count
        self.compiled_sql = compiled_sql
        self.param_plan = param_plan


class _MockStatementCache:
    def __init__(self) -> None:
        self.entries: dict[tuple[int, bytes], _MockStatementEntry] = {}
        self.hits: int = 0
        self.misses: int = 0

    def lookup(self, dialect_tag: int, raw_sql: bytes) -> _MockStatementEntry | None:
        if not isinstance(raw_sql, bytes):
            msg = "raw_sql must be bytes"
            raise TypeError(msg)
        key = (dialect_tag, raw_sql)
        if key in self.entries:
            self.hits += 1
            return self.entries[key]
        self.misses += 1
        return None

    def insert(
        self,
        dialect_tag: int,
        raw_sql: bytes,
        param_count: int,
        compiled_sql: bytes,
        param_plan: bytes,
    ) -> bool:
        if not isinstance(raw_sql, bytes):
            msg = "raw_sql must be bytes"
            raise TypeError(msg)
        self.entries[(dialect_tag, raw_sql)] = _MockStatementEntry(param_count, compiled_sql, param_plan)
        return True


class _MockBroker:
    def __init__(self) -> None:
        self.lease_table = _MockLeaseTable()
        self._statement_cache = _MockStatementCache()

    def acquire(
        self,
        worker_pid: int,
        ttl_ns: int = 0,
        timeout_ms: int = 5000,
        bind_key_hash: int = 0,
    ) -> _MockLeaseGrant:
        _ = (ttl_ns, timeout_ms, bind_key_hash)
        return self.lease_table.acquire(worker_pid)

    def release(self, slot_index: int, generation: int) -> bool:
        self.lease_table.release(slot_index, generation)
        return True

    def statement_cache(self) -> _MockStatementCache:
        return self._statement_cache


class _OracleSessionPort:
    """Native port stub recording Oracle session calls for SQLSpec adapter tests."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any = None) -> int:
        """Record the statement and its parameters and report zero affected rows."""
        self.calls.append((sql, parameters))
        return 0

    def close(self) -> None:
        """Release nothing; the stub holds no resources."""


class _AsyncOracleSessionPort:
    """Async native port stub recording Oracle session calls for SQLSpec adapter tests."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    async def execute(self, sql: str, parameters: Any = None) -> int:
        """Record the statement and its parameters and report zero affected rows."""
        self.calls.append((sql, parameters))
        return 0

    async def close(self) -> None:
        """Release nothing; the stub holds no resources."""


class _MockMSSQLClient:
    """In-memory SQLite-backed SyncMSSQLClient test double for broker and DBAPI forwarding checks."""

    def __init__(self) -> None:
        self._conn = sqlite3.connect(":memory:", check_same_thread=False)

    @staticmethod
    def _adapt(sql: str, params: Any) -> tuple[str, tuple[Any, ...]]:
        if not params:
            return re.sub(r"@P\d+", "?", sql), ()
        if isinstance(params, dict):
            ordered: list[Any] = []

            def _rep(m: re.Match[str]) -> str:
                key = m.group(1)
                ordered.append(params.get(key, params.get(f"@{key}")))
                return "?"

            return re.sub(r"@([A-Za-z_][A-Za-z0-9_]*)", _rep, sql), tuple(ordered)
        return re.sub(r"@P\d+", "?", sql), tuple(params)

    def begin(self, isolation_level: str | None = None) -> None:
        _ = isolation_level

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    def execute(self, sql: str, params: Any = None) -> int:
        adapted, bound = self._adapt(sql, params)
        cur = self._conn.execute(adapted, bound)
        self._conn.commit()
        return max(0, int(cur.rowcount or 0))

    def query(self, sql: str, params: Any = None) -> dict[str, Any]:
        adapted, bound = self._adapt(sql, params)
        cur = self._conn.execute(adapted, bound)
        cols = [str(c[0]) for c in (cur.description or ())]
        rows = [tuple(r) for r in cur.fetchall()] if cur.description is not None else []
        if cur.description is None:
            self._conn.commit()
        return {"columns": cols, "rows": rows, "affected_rows": max(0, int(cur.rowcount or 0))}

    def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        raw = self.query(sql, params)
        cols = raw["columns"]
        return [dict(zip(cols, row, strict=False)) for row in raw["rows"]]

    def close(self) -> None:
        self._conn.close()


def test_postgres_jsonb_pgcopy_and_arrow_shm_cursor() -> None:
    """Verify PostgreSQL JSONB wire helpers, PGCOPY binary framing, and zero-copy Arrow SHM cursor."""
    encoded_jsonb = PostgreSQLCore.encode_jsonb({"tenant": "acme", "active": True})
    assert encoded_jsonb[:1] == b"\x01"
    stripped = PostgreSQLCore.strip_jsonb_header(memoryview(encoded_jsonb))
    assert bytes(stripped).startswith(b"{")

    rows = [(1, "alpha", b"\x00\xff"), (2, None, "beta")]
    copy_buf = PostgreSQLCore.encode_pgcopy_rows(rows)
    decoded_rows = PostgreSQLCore.decode_pgcopy_rows(copy_buf)
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
    enc_f32 = OracleCore.encode_vector_f32(vec_f32)
    assert OracleCore.decode_vector_f32(enc_f32) == pytest.approx(vec_f32)

    vec_f64 = [1.125, -2.5, 42.0]
    enc_f64 = OracleCore.encode_vector_f64(vec_f64)
    assert OracleCore.decode_vector_f64(enc_f64) == pytest.approx(vec_f64)

    vec_i8 = [-128, -1, 0, 1, 127]
    enc_i8 = OracleCore.encode_vector_i8(vec_i8)
    assert OracleCore.decode_vector_i8(enc_i8) == vec_i8

    vec_bin = bytes([0b1010_0101, 0b1111_0000])
    enc_bin = OracleCore.encode_vector_binary(vec_bin)
    assert OracleCore.decode_vector_binary(enc_bin) == vec_bin

    with pytest.raises(VertebraError):
        OracleCore.decode_vector_f32(b"\x00\x01\x02\x00\x00\x00\x00\x01\x00\x00\x00\x00")

    payload_obj = {"doc_id": 101, "title": "Oracle 26ai Duality View", "tags": ["json", "oson"]}
    oson_bytes = OracleCore.encode_oson(payload_obj)
    assert oson_bytes[:3] == OSON_MAGIC
    assert OracleCore.decode_oson(oson_bytes) == payload_obj

    with pytest.raises(VertebraError):
        OracleCore.decode_oson(b"invalid")

    tns_desc = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=FREEPDB1)))"
    connect_pkt = OracleCore.encode_tns_connect_packet(tns_desc)
    pkt_type, flags, pkt_body = OracleCore.decode_tns_packet(connect_pkt)
    assert pkt_type == 1
    assert flags == 0
    assert pkt_body == tns_desc.encode("utf-8")

    data_pkt = OracleCore.encode_tns_data_packet(0x0040, b"TTC_QUERY")
    d_type, d_flags, d_body = OracleCore.decode_tns_packet(data_pkt)
    assert d_type == 6
    assert d_flags == 0x0040
    assert d_body == b"TTC_QUERY"

    ttc_frame = OracleCore.encode_ttc_session_attrs(
        client_identifier="user-42",
        module="vertebra-api",
        action="GET /orders",
        ecid="00af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
    )
    assert ttc_frame[:3] == b"\x11\x6e\x04"
    decoded_attrs = OracleCore.decode_ttc_session_attrs(ttc_frame)
    assert decoded_attrs["client_identifier"] == "user-42"
    assert decoded_attrs["module"] == "vertebra-api"
    assert decoded_attrs["action"] == "GET /orders"
    assert decoded_attrs["ecid"] == "00af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"


def test_mssql_tds_framing_and_placeholder_rewriter() -> None:
    """Verify single-pass @P1 placeholder rewriting and broker statement cache integration."""
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
    mssql_driver = SyncMSSQLDriver(MSSQLConfig(), broker=broker, client=_MockMSSQLClient())
    mssql_driver.execute("CREATE TABLE ledger (id INTEGER PRIMARY KEY, code TEXT)")
    mssql_driver.execute("INSERT INTO ledger (id, code) VALUES (?, ?)", (7, "USD"))
    assert mssql_driver.last_rewritten_sql == "INSERT INTO ledger (id, code) VALUES (@P1, @P2)"
    assert mssql_driver.select("SELECT id, code FROM ledger WHERE id = ?", (7,)) == [{"id": 7, "code": "USD"}]
    assert broker.statement_cache().hits >= 0
    mssql_driver.close()


def test_duckdb_post_init_bootstrap_and_arrow_shm_streaming() -> None:
    """Verify DuckDB post-init SET statements (`parquet_metadata_cache`) and zero-copy Arrow SHM streaming."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
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
        (PostgresSyncConfig, PostgresAsyncConfig, AsyncPostgreSQLDriver),
        (MySQLSyncConfig, MySQLAsyncConfig, AsyncMySQLDriver),
        (SQLiteSyncConfig, SQLiteAsyncConfig, AsyncSQLiteDriver),
        (MSSQLSyncConfig, MSSQLAsyncConfig, AsyncMSSQLDriver),
        pytest.param(
            DuckDBSyncConfig,
            DuckDBAsyncConfig,
            AsyncDuckDBDriver,
            marks=pytest.mark.skipif(not module_available("vertebra._duckdb"), reason=_DUCKDB_SKIP_REASON),
        ),
    ],
)
@pytest.mark.anyio
async def test_sqlspec_adapter_config_flags_and_async_driver_close(
    sync_cfg_cls: type[Any],
    async_cfg_cls: type[Any],
    async_drv_cls: type[Any],
) -> None:
    """Verify Sync and Async SQLSpec adapter flags and async driver teardown."""
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

    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
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
        "mssql+vtb://localhost:1433/master",
        "duckdb+vtb:///:memory:",
    ],
)
def test_sqlalchemy_dbapi_broker_forwarding(url: str) -> None:
    """Verify SyncDBAPIConnection forwards DBAPI operations through the SHM broker."""
    if url.startswith("duckdb"):
        pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    broker = _MockBroker()
    injected_driver = (
        SyncMSSQLDriver(MSSQLConfig.from_url(url), broker=broker, client=_MockMSSQLClient())
        if url.startswith("mssql+vtb")
        else None
    )
    conn = SyncDBAPIConnection(url, driver=injected_driver, broker=broker)
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

    pg_sync_adapter = PostgresSyncConfig(pg_cfg)
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
        secret_type="s3",
        value={
            "key_id": "key1",
            "secret": "sec1",
            "region": "us-west-2",
            "endpoint": "127.0.0.1:9000",
        },
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


def _arrow_ipc_stream_bytes(table: pa.Table) -> bytes:
    """Serialize `table` into a complete Arrow IPC stream."""
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return bytes(sink.getvalue().to_pybytes())


def _raise_on_row_materialization(*_args: Any, **_kwargs: Any) -> Any:
    """Fail when a code path materializes per-row dictionaries."""
    raise AssertionError("per-row to_pylist() materialization is not allowed on this path")


@pytest.mark.parametrize("framing", ["raw_ipc", "vtbarrow_header"])
def test_read_arrow_shm_zero_copy_lazy_len_survives_dropped_region(
    framing: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`read_arrow_shm` maps Arrow buffers directly over the SHM region and keeps it alive after the caller drops it."""
    table = pa.Table.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    ipc_bytes = _arrow_ipc_stream_bytes(table)
    region: Any
    if framing == "vtbarrow_header":
        mapping = mmap.mmap(-1, 4096)
        mapping[:8] = b"VTBARROW"
        mapping[8:16] = struct.pack("<Q", len(ipc_bytes))
        mapping[16 : 16 + len(ipc_bytes)] = ipc_bytes
        region = memoryview(mapping)
        payload_offset = 16
        del mapping
    else:
        region = write_arrow_shm(table)
        payload_offset = 0
    region_address = pa.py_buffer(region).address
    region_size = len(region)

    cursor = read_arrow_shm(region)
    del region
    gc.collect()

    assert pa.py_buffer(cursor.buffer_view()).address == region_address + payload_offset
    assert len(cursor.buffer_view()) == len(ipc_bytes)
    monkeypatch.setattr(db_arrow_mod.ArrowCursor, "to_pylist", _raise_on_row_materialization)
    assert len(cursor) == table.num_rows
    assert cursor.row_count == table.num_rows
    assert cursor._rows == []
    monkeypatch.undo()

    decoded = cursor.to_pyarrow()
    value_buffer_address = decoded.column("id").chunks[0].buffers()[1].address
    assert region_address <= value_buffer_address < region_address + region_size
    assert decoded.equals(table)
    assert cursor.to_pylist() == [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]


def test_read_arrow_shm_rejects_truncated_vtbarrow_frame() -> None:
    """A `VTBARROW` header declaring more payload bytes than the region holds raises `ValueError`."""
    ipc_bytes = _arrow_ipc_stream_bytes(pa.Table.from_pydict({"id": [1]}))
    frame = b"VTBARROW" + struct.pack("<Q", len(ipc_bytes) + 1) + ipc_bytes
    with pytest.raises(ValueError, match="VTBARROW"):
        read_arrow_shm(memoryview(frame))


def test_read_arrow_shm_empty_ipc_table_reports_zero_rows_and_schema() -> None:
    """A `0`-row Arrow IPC stream yields `len(cursor) == 0` and schema-derived column names."""
    empty = pa.table({"id": pa.array([], pa.int64()), "name": pa.array([], pa.string())})
    cursor = read_arrow_shm(_arrow_ipc_stream_bytes(empty))
    assert len(cursor) == 0
    assert db_arrow_mod._table_to_rows_and_cols(cursor) == ([], ("id", "name"))


@pytest.mark.parametrize(
    ("table", "columns", "expected"),
    [
        (
            pa.table({"id": pa.array([], pa.int64()), "name": pa.array([], pa.string())}),
            None,
            ([], ("id", "name")),
        ),
        (
            pa.Table.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]}),
            None,
            ([(1, "a"), (2, "b"), (3, "c")], ("id", "name")),
        ),
        (pa.table({}), None, ([], ())),
        (
            pa.Table.from_pydict({"id": [1, 2], "name": ["a", "b"]}),
            ("name", "missing", "id"),
            ([("a", None, 1), ("b", None, 2)], ("name", "missing", "id")),
        ),
        (
            pa.RecordBatch.from_pydict({"id": [7, 8], "name": ["x", "y"]}),
            None,
            ([(7, "x"), (8, "y")], ("id", "name")),
        ),
    ],
    ids=["empty-typed", "populated", "zero-columns", "column-subset", "record-batch"],
)
def test_table_to_rows_and_cols_transposes_arrow_tables_columnar(
    table: Any, columns: tuple[str, ...] | None, expected: tuple[list[tuple[Any, ...]], tuple[str, ...]]
) -> None:
    """`_table_to_rows_and_cols` returns tuple rows and column names for Arrow tables and record batches."""
    assert db_arrow_mod._table_to_rows_and_cols(table, columns) == expected


def test_table_to_rows_and_cols_reads_ipc_cursor_without_row_dicts(monkeypatch: pytest.MonkeyPatch) -> None:
    """An IPC-backed `ArrowCursor` converts column-wise without calling `ArrowCursor.to_pylist()`."""
    table = pa.Table.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    cursor = read_arrow_shm(write_arrow_shm(table))
    monkeypatch.setattr(db_arrow_mod.ArrowCursor, "to_pylist", _raise_on_row_materialization)
    assert db_arrow_mod._table_to_rows_and_cols(cursor) == ([(1, "a"), (2, "b"), (3, "c")], ("id", "name"))


def test_table_to_rows_and_cols_collapses_duplicate_join_columns_to_last_field() -> None:
    """Duplicate field names from SQL joins resolve to the last field, matching `Table.to_pylist()` dictionaries."""
    table = pa.Table.from_arrays(
        [pa.array([1, 2]), pa.array(["a", "b"]), pa.array([10, 20])], names=["id", "name", "id"]
    )
    rows, cols = db_arrow_mod._table_to_rows_and_cols(table)
    assert (rows, cols) == ([(10, "a"), (20, "b")], ("id", "name"))
    assert [dict(zip(cols, row, strict=True)) for row in rows] == table.to_pylist()
    assert db_arrow_mod._table_to_rows_and_cols(table, ("name", "id")) == ([("a", 10), ("b", 20)], ("name", "id"))


def test_arrow_cursor_decodes_ipc_stream_once(monkeypatch: pytest.MonkeyPatch) -> None:
    """`len()`, `to_arrow_table()`, `to_pylist()`, and row extraction share one decoded IPC table."""
    table = pa.Table.from_pydict({"id": [1, 2, 3], "name": ["a", "b", "c"]})
    cursor = read_arrow_shm(_arrow_ipc_stream_bytes(table))
    opened: list[Any] = []
    original_open_stream = pa.ipc.open_stream

    def counting_open_stream(source: Any, *args: Any, **kwargs: Any) -> Any:
        opened.append(source)
        return original_open_stream(source, *args, **kwargs)

    monkeypatch.setattr(pa.ipc, "open_stream", counting_open_stream)
    assert len(cursor) == 3
    assert cursor.to_arrow_table().equals(table)
    assert cursor.to_pylist() == table.to_pylist()
    assert db_arrow_mod._table_to_rows_and_cols(cursor) == ([(1, "a"), (2, "b"), (3, "c")], ("id", "name"))
    assert len(opened) == 1


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
    native: Any = SyncPostgreSQLDriver() if engine == "postgres" else _OracleSessionPort()
    failure = RuntimeError("session setting refused")

    def fail(*args: object, **kwargs: object) -> int:
        raise failure

    monkeypatch.setattr(type(native), "execute", fail)
    adapter = PostgresSyncDriver(native) if engine == "postgres" else OracleSyncDriver(native)
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
    native: Any = AsyncPostgreSQLDriver() if engine == "postgres" else _AsyncOracleSessionPort()
    failure = RuntimeError("session setting refused")

    async def fail(*args: object, **kwargs: object) -> int:
        raise failure

    monkeypatch.setattr(type(native), "execute", fail)
    adapter = PostgresAsyncDriver(native) if engine == "postgres" else OracleAsyncDriver(native)
    try:
        with pytest.raises(RuntimeError) as raised:
            await adapter.apply_tenant_predicate(TenantPredicate(table_alias="t", tenant_value="acme"))
        assert raised.value is failure
    finally:
        await native.close()


@pytest.mark.parametrize("config_defaults", [False, True])
def test_postgresql_tenant_session_prepares_numeric_binds(config_defaults: bool) -> None:
    """The native sync port receives $N numeric binds and the exact session values."""
    cluster = FakePostgresCluster()
    native = SyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost:5432/app"),
        pool=FakeSyncPostgresPool(cluster),
    )
    config = PostgresSyncConfig(native.config, broker=object(), share_pool=False)
    adapter = PostgresSyncDriver(native, statement_config=config.statement_config if config_defaults else None)
    try:
        result = adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))
        assert result == "o.tenant_id = $1"
        assert len(cluster.recorded_queries) == 1
        sql, params, _ = cluster.recorded_queries[0]
        assert sql.casefold() == "select set_config($1, $2, $3)"
        assert params == ("app.tenant_id", "acme", True)
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("config_defaults", [False, True])
async def test_postgresql_tenant_session_prepares_async_numeric_binds(config_defaults: bool) -> None:
    """The async native port receives numeric binds and the exact session values."""
    cluster = FakePostgresCluster()
    native = AsyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost:5432/app"),
        pool=FakeAsyncPostgresPool(cluster),
    )
    config = PostgresAsyncConfig(native.config, broker=object(), share_pool=False)
    adapter = PostgresAsyncDriver(native, statement_config=config.statement_config if config_defaults else None)
    try:
        result = await adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))
        assert result == "o.tenant_id = $1"
        assert len(cluster.recorded_queries) == 1
        sql, params, _ = cluster.recorded_queries[0]
        assert sql.casefold() == "select set_config($1, $2, $3)"
        assert params == ("app.tenant_id", "acme", True)
    finally:
        await native.close()


def test_oracle_tenant_predicate_sets_session_identifier_through_native_port() -> None:
    """OracleSyncDriver executes DBMS_SESSION.SET_IDENTIFIER against its native port."""
    port = _OracleSessionPort()
    adapter = OracleSyncDriver(port)
    assert adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme")) == (
        "o.tenant_id = :tenant_id"
    )
    assert port.calls == [
        (
            "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;",
            {"value": "acme"},
        )
    ]


@pytest.mark.anyio
async def test_async_oracle_tenant_predicate_sets_session_identifier_through_native_port() -> None:
    """OracleAsyncDriver executes DBMS_SESSION.SET_IDENTIFIER against its async native port."""
    port = _AsyncOracleSessionPort()
    adapter = OracleAsyncDriver(port)
    assert await adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme")) == (
        "o.tenant_id = :tenant_id"
    )
    assert port.calls == [
        (
            "BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;",
            {"value": "acme"},
        )
    ]


@pytest.mark.parametrize(
    "driver_cls",
    [SyncMSSQLDriver, SyncDuckDBDriver],
    ids=["mssql-sync", "duckdb-sync"],
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
    """SyncMSSQLDriver and SyncDuckDBDriver reject unsafe table and column identifiers."""
    if driver_cls is SyncDuckDBDriver:
        pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    driver = driver_cls()
    try:
        with pytest.raises(ValueError, match="Invalid"):
            driver.load_from_arrow(table_name, [{"id": 1, "label": "ok"}], columns=columns)
    finally:
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "driver_cls",
    [AsyncMSSQLDriver, AsyncDuckDBDriver],
    ids=["mssql-async", "duckdb-async"],
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
    """AsyncMSSQLDriver and AsyncDuckDBDriver reject unsafe table and column identifiers."""
    if driver_cls is AsyncDuckDBDriver:
        pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
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
                {"embedding": OracleCore.encode_vector_f32([3.0, 4.0, 0.0, 0.0])},
                {"embedding": OracleCore.encode_vector_f32([0.0, 5.0, 12.0, 0.0])},
                {"embedding": OracleCore.encode_vector_f32([8.0, 0.0, 15.0, 0.0])},
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


def test_oracle_drivers_raise_missing_dependency_when_native_module_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Oracle sync/async drivers, registry, SQLSpec sync session, and DB-API raise MissingDependencyError."""
    monkeypatch.setitem(sys.modules, "vertebra._oracle", None)
    reset_dependency_cache("vertebra._oracle")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            SyncOracleDriver(OracleConfig.from_url("oracle://u:p@db:1521/svc"))
        assert exc_info.value.package == "vertebra-core[oracle]"
        assert exc_info.value.hint == "Install it with: uv add vertebra-core[oracle]"
        assert isinstance(exc_info.value, ImportError)

        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[oracle\]"):
            AsyncOracleDriver()

        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[oracle\]"):
            DatabaseRegistry().get_shared_driver(OracleConfig())

        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[oracle\]"):
            DatabaseRegistry().get_shared_sync_driver("oracle://scott:tiger@localhost:1521/FREEPDB1")

        cfg = OracleSyncConfig()
        assert cfg.is_async is False
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[oracle\]"), cfg.provide_session():
            pass

        with pytest.raises(InterfaceError, match=r"vertebra-core\[oracle\]") as dbapi_exc:
            SyncDBAPIConnection("oracle+vtb://localhost:1521/FREEPDB1", broker=_MockBroker())
        assert isinstance(dbapi_exc.value.__cause__, MissingDependencyError)
    finally:
        reset_dependency_cache("vertebra._oracle")


@pytest.mark.anyio
async def test_oracle_async_sqlspec_session_raises_missing_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """OracleAsyncConfig.provide_session raises MissingDependencyError when vertebra._oracle is absent."""
    monkeypatch.setitem(sys.modules, "vertebra._oracle", None)
    reset_dependency_cache("vertebra._oracle")
    try:
        cfg = OracleAsyncConfig()
        assert cfg.is_async is True
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[oracle\]"):
            async with cfg.provide_session():
                pass
    finally:
        reset_dependency_cache("vertebra._oracle")


def test_mssql_drivers_raise_missing_dependency_when_native_module_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MSSQL sync/async drivers, registry, SQLSpec sync session, and DB-API raise MissingDependencyError."""
    monkeypatch.setitem(sys.modules, "vertebra._mssql", None)
    reset_dependency_cache("vertebra._mssql")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            SyncMSSQLDriver(MSSQLConfig.from_url("mssql://sa:secret@localhost:1433/master"))
        assert exc_info.value.package == "vertebra-core[mssql]"
        assert exc_info.value.hint == "Install it with: uv add vertebra-core[mssql]"
        assert isinstance(exc_info.value, ImportError)

        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            AsyncMSSQLDriver()

        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            DatabaseRegistry().get_shared_driver(MSSQLConfig())

        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            DatabaseRegistry().get_shared_sync_driver("sqlserver://sa:secret@localhost:1433/master")

        cfg = MSSQLSyncConfig()
        assert cfg.is_async is False
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"), cfg.provide_session():
            pass

        with pytest.raises(InterfaceError, match=r"vertebra-core\[mssql\]") as dbapi_exc:
            SyncDBAPIConnection("mssql+vtb://localhost:1433/master", broker=_MockBroker())
        assert isinstance(dbapi_exc.value.__cause__, MissingDependencyError)
    finally:
        reset_dependency_cache("vertebra._mssql")


@pytest.mark.anyio
async def test_mssql_async_sqlspec_session_raises_missing_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """MSSQLAsyncConfig.provide_session raises MissingDependencyError when vertebra._mssql is absent."""
    monkeypatch.setitem(sys.modules, "vertebra._mssql", None)
    reset_dependency_cache("vertebra._mssql")
    try:
        cfg = MSSQLAsyncConfig()
        assert cfg.is_async is True
        with pytest.raises(MissingDependencyError, match=r"vertebra-core\[mssql\]"):
            async with cfg.provide_session():
                pass
    finally:
        reset_dependency_cache("vertebra._mssql")


def test_duckdb_drivers_raise_missing_dependency_when_duckdb_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SyncDuckDBDriver and AsyncDuckDBDriver raise MissingDependencyError naming vertebra-core[duckdb] when absent."""
    monkeypatch.setitem(sys.modules, "vertebra._duckdb", None)
    reset_dependency_cache("vertebra._duckdb")
    try:
        with pytest.raises(MissingDependencyError) as exc_info:
            SyncDuckDBDriver()
        assert exc_info.value.package == "vertebra-core[duckdb]"
        assert exc_info.value.install_package == "vertebra-core[duckdb]"
        assert exc_info.value.hint == "Install it with: uv add vertebra-core[duckdb]"

        with pytest.raises(MissingDependencyError) as async_exc:
            AsyncDuckDBDriver(DuckDBConfig(path=":memory:"))
        assert async_exc.value.package == "vertebra-core[duckdb]"
    finally:
        reset_dependency_cache("vertebra._duckdb")


def test_duckdb_driver_executes_on_duckdb_engine() -> None:
    """SyncDuckDBDriver executes DuckDB-specific SQL and honors explicit transaction commit/rollback."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    driver = SyncDuckDBDriver()
    try:
        assert driver.select("SELECT list_value(1, 2, 3) AS xs") == [{"xs": [1, 2, 3]}]
        assert driver.execute("CREATE TABLE tx_items (v INTEGER)") == 0
        assert driver.execute("INSERT INTO tx_items VALUES (1), (2)") == 2

        driver.begin()
        assert driver.execute("INSERT INTO tx_items VALUES (?)", [7]) == 1
        driver.rollback()
        assert driver.execute_tuples("SELECT count(*) FROM tx_items") == [(2,)]

        driver.begin()
        assert driver.execute("INSERT INTO tx_items VALUES (?)", [9]) == 1
        driver.commit()
        assert driver.execute_tuples("SELECT max(v) FROM tx_items") == [(9,)]

        driver.commit()
        driver.rollback()
    finally:
        driver.close()


@pytest.mark.parametrize("core_cls", [OracleCore, DuckDBCore], ids=["oracle-core", "duckdb-core"])
@pytest.mark.parametrize("method_name", ["acquire_broker_lease", "release_broker_lease"])
def test_core_probe_helpers_removed(core_cls: type[Any], method_name: str) -> None:
    """OracleCore and DuckDBCore no longer carry duplicate broker probe helpers."""
    assert not hasattr(core_cls, method_name)


class _SyncOracleLeaseTestClient:
    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        _ = (parameters, kwargs)
        if sql == "RAISE":
            msg = "simulated oracle execution failure"
            raise RuntimeError(msg)
        return 1

    def close(self) -> None:
        return None


class _AsyncOracleLeaseTestClient:
    async def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        _ = (parameters, kwargs)
        if sql == "RAISE":
            msg = "simulated async oracle execution failure"
            raise RuntimeError(msg)
        return 1

    async def close(self) -> None:
        return None


def test_sync_oracle_driver_uses_common_mixin_broker_and_bytes_statement_cache() -> None:
    """SyncOracleDriver caches bytes-keyed StatementPlan entries and releases leases on error."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = SyncOracleDriver(OracleConfig(), broker=broker, client=_SyncOracleLeaseTestClient())
    try:
        sql = "INSERT INTO items (id) VALUES (:1)"
        assert driver.execute(sql, [1]) == 1
        sc = broker.statement_cache()
        assert sc is not None
        stats_after_first = sc.stats()
        assert stats_after_first.misses == 1
        assert stats_after_first.hits == 0
        assert stats_after_first.inserts == 1

        assert driver.execute(sql, [2]) == 1
        stats_after_second = sc.stats()
        assert stats_after_second.hits == 1
        plan = sc.lookup(ORACLE_DIALECT_TAG, sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 1
        assert plan.compiled_sql == sql.encode("utf-8")
        assert plan.param_plan == b"\x00\x01"

        with pytest.raises(RuntimeError, match="simulated oracle execution failure"):
            driver.execute("RAISE")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 3
        assert broker_stats.total_released == 3
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_oracle_driver_uses_common_mixin_broker_and_bytes_statement_cache() -> None:
    """AsyncOracleDriver caches bytes-keyed StatementPlan entries and releases leases on error."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = AsyncOracleDriver(OracleConfig(), broker=broker, client=_AsyncOracleLeaseTestClient())
    try:
        sql = "INSERT INTO items (id) VALUES (:1)"
        assert await driver.execute(sql, [1]) == 1
        assert await driver.execute(sql, [2]) == 1
        sc = broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits == 1
        plan = sc.lookup(ORACLE_DIALECT_TAG, sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 1

        with pytest.raises(RuntimeError, match="simulated async oracle execution failure"):
            await driver.execute("RAISE")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 3
        assert broker_stats.total_released == 3
    finally:
        await driver.close()


def test_sync_duckdb_driver_uses_real_lease_broker_and_releases_on_error() -> None:
    """SyncDuckDBDriver routes through CommonDriverMixin bytes StatementCache and LeaseBroker."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = SyncDuckDBDriver(DuckDBConfig(path=":memory:"), broker=broker)
    try:
        driver.execute("CREATE TABLE items (id INTEGER)")
        insert_sql = "INSERT INTO items VALUES (?)"
        assert driver.execute(insert_sql, (1,)) == 1
        assert driver.execute(insert_sql, (2,)) == 1

        sc = broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits >= 1
        plan = sc.lookup(DUCKDB_DIALECT_TAG, insert_sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 1

        with pytest.raises(ProgrammingError, match="nonexistent_table_for_lease_test"):
            driver.execute_tuples("SELECT * FROM nonexistent_table_for_lease_test")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 4
        assert broker_stats.total_released == 4
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_duckdb_driver_uses_real_lease_broker_and_releases_on_error() -> None:
    """AsyncDuckDBDriver routes through CommonDriverMixin bytes StatementCache and LeaseBroker."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = AsyncDuckDBDriver(DuckDBConfig(path=":memory:"), broker=broker)
    try:
        await driver.execute("CREATE TABLE items (id INTEGER)")
        insert_sql = "INSERT INTO items VALUES (?)"
        assert await driver.execute(insert_sql, (1,)) == 1
        assert await driver.execute(insert_sql, (2,)) == 1

        sc = broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits >= 1
        plan = sc.lookup(DUCKDB_DIALECT_TAG, insert_sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 1

        with pytest.raises(ProgrammingError, match="nonexistent_table_for_lease_test"):
            await driver.execute_tuples("SELECT * FROM nonexistent_table_for_lease_test")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 4
        assert broker_stats.total_released == 4
    finally:
        await driver.close()


def test_common_driver_mixin_statement_cache_propagates_invalid_dialect_tag() -> None:
    """CommonDriverMixin._lookup_or_cache_statement does not swallow native dialect errors."""

    class _BadDialectDriver(CommonDriverMixin[DuckDBConfig]):
        dialect_tag = -1

    broker = LeaseBroker(pool_size=1, max_overflow=0, stmt_capacity=8)
    driver = _BadDialectDriver(DuckDBConfig(path=":memory:"), broker=broker)
    with pytest.raises(OverflowError):
        driver._lookup_or_cache_statement("SELECT 1")


class _AsyncMSSQLBrokerTestClient:
    """Async MSSQL client double delegating to an in-memory SQLite connection."""

    def __init__(self) -> None:
        self._sync = _MockMSSQLClient()

    async def execute(self, sql: str, params: Any = None) -> int:
        return self._sync.execute(sql, params)

    async def query(self, sql: str, params: Any = None) -> dict[str, Any]:
        return self._sync.query(sql, params)

    async def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        return self._sync.fetch_all(sql, params)

    async def close(self) -> None:
        self._sync.close()


@pytest.mark.parametrize(
    ("target", "method_name"),
    [
        (MSSQLCore, "prepare_and_lease"),
        (MSSQLCore, "release_broker_lease"),
        (SyncSQLiteDriver, "_acquire_sqlite_broker_lease"),
        (SyncSQLiteDriver, "_release_sqlite_broker_lease"),
        (AsyncSQLiteDriver, "_acquire_sqlite_broker_lease"),
        (AsyncSQLiteDriver, "_release_sqlite_broker_lease"),
    ],
)
def test_mssql_and_sqlite_probe_helpers_removed(target: type[Any], method_name: str) -> None:
    """MSSQLCore and SQLite drivers no longer carry broker/statement-cache probe helpers."""
    assert not hasattr(target, method_name)


def test_sync_mssql_driver_uses_common_mixin_broker_and_bytes_statement_cache() -> None:
    """SyncMSSQLDriver caches rewritten @P1 bytes plans and releases leases on error."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = SyncMSSQLDriver(MSSQLConfig(), broker=broker, client=_MockMSSQLClient())
    try:
        driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT)")
        insert_sql = "INSERT INTO items (id, code) VALUES (?, ?)"
        assert driver.execute(insert_sql, (1, "USD")) == 1
        assert driver.last_rewritten_sql == "INSERT INTO items (id, code) VALUES (@P1, @P2)"
        assert driver.execute(insert_sql, (2, "EUR")) == 1
        assert driver.last_rewritten_sql == "INSERT INTO items (id, code) VALUES (@P1, @P2)"

        sc = broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits >= 1
        plan = sc.lookup(MSSQL_DIALECT_TAG, insert_sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 2
        assert plan.compiled_sql == b"INSERT INTO items (id, code) VALUES (@P1, @P2)"

        with pytest.raises(sqlite3.OperationalError):
            driver.select("SELECT * FROM nonexistent_mssql_table")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 4
        assert broker_stats.total_released == 4
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_mssql_driver_uses_common_mixin_broker_and_bytes_statement_cache() -> None:
    """AsyncMSSQLDriver caches rewritten @P1 bytes plans and releases leases on error."""
    broker = LeaseBroker(pool_size=2, max_overflow=0, stmt_capacity=16)
    driver = AsyncMSSQLDriver(MSSQLConfig(), broker=broker, client=_AsyncMSSQLBrokerTestClient())
    try:
        await driver.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, code TEXT)")
        insert_sql = "INSERT INTO items (id, code) VALUES (?, ?)"
        assert await driver.execute(insert_sql, (1, "USD")) == 1
        assert await driver.execute(insert_sql, (2, "EUR")) == 1
        assert driver.last_rewritten_sql == "INSERT INTO items (id, code) VALUES (@P1, @P2)"

        sc = broker.statement_cache()
        assert sc is not None
        assert sc.stats().hits >= 1
        plan = sc.lookup(MSSQL_DIALECT_TAG, insert_sql.encode("utf-8"))
        assert plan is not None
        assert plan.param_count == 2
        assert plan.compiled_sql == b"INSERT INTO items (id, code) VALUES (@P1, @P2)"

        with pytest.raises(sqlite3.OperationalError):
            await driver.select("SELECT * FROM nonexistent_mssql_table")
        broker_stats = broker.stats()
        assert broker_stats.active_leases == 0
        assert broker_stats.total_acquired == 4
        assert broker_stats.total_released == 4
    finally:
        await driver.close()


@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
def test_sync_duckdb_pipeline_exit_contract(scenario: str) -> None:
    """SyncDuckDBPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    driver = SyncDuckDBDriver(DuckDBConfig(path=":memory:"))
    try:
        driver.execute("CREATE TABLE pipe_items (id INTEGER PRIMARY KEY, label VARCHAR)")
        if scenario == "auto_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "auto"))
            assert driver.in_transaction is False
            assert driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [(1, "auto")]
        elif scenario == "tail_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "first"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "tail"))
            assert driver.in_transaction is False
            assert driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [
                (1, "first"),
                (2, "tail"),
            ]
        elif scenario == "caller_tx_rollback":
            driver.begin()
            driver.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "outer"))
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "inner"))
            assert driver.in_transaction is True
            assert driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [
                (1, "outer"),
                (2, "inner"),
            ]
            driver.rollback()
            assert driver.execute_tuples("SELECT id, label FROM pipe_items") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "first"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "discarded"))
                raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert driver.execute_tuples("SELECT id, label FROM pipe_items") == []
        else:
            driver.begin()
            driver.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "outer_kept"))
            with pytest.raises(RuntimeError, match="abort_inner"), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "inner_flushed"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (3, "inner_discarded"))
                raise RuntimeError("abort_inner")
            assert driver.in_transaction is True
            driver.commit()
            assert driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [(1, "outer_kept")]
    finally:
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
async def test_async_duckdb_pipeline_exit_contract(scenario: str) -> None:
    """AsyncDuckDBPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    driver = AsyncDuckDBDriver(DuckDBConfig(path=":memory:"))
    try:
        await driver.execute("CREATE TABLE pipe_items (id INTEGER PRIMARY KEY, label VARCHAR)")
        if scenario == "auto_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "auto"))
            assert driver.in_transaction is False
            assert await driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [(1, "auto")]
        elif scenario == "tail_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "first"))
                assert await pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "tail"))
            assert driver.in_transaction is False
            assert await driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [
                (1, "first"),
                (2, "tail"),
            ]
        elif scenario == "caller_tx_rollback":
            await driver.begin()
            await driver.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "outer"))
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "inner"))
            assert driver.in_transaction is True
            assert await driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [
                (1, "outer"),
                (2, "inner"),
            ]
            await driver.rollback()
            assert await driver.execute_tuples("SELECT id, label FROM pipe_items") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "first"))
                    assert await pipe.results() == [1]
                    pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "discarded"))
                    raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert await driver.execute_tuples("SELECT id, label FROM pipe_items") == []
        else:
            await driver.begin()
            await driver.execute("INSERT INTO pipe_items VALUES (?, ?)", (1, "outer_kept"))
            with pytest.raises(RuntimeError, match="abort_inner"):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (2, "inner_flushed"))
                    assert await pipe.results() == [1]
                    pipe.execute("INSERT INTO pipe_items VALUES (?, ?)", (3, "inner_discarded"))
                    raise RuntimeError("abort_inner")
            assert driver.in_transaction is True
            await driver.commit()
            assert await driver.execute_tuples("SELECT id, label FROM pipe_items ORDER BY id") == [(1, "outer_kept")]
    finally:
        await driver.close()


class _FakeMSSQLProtocolError(Exception):
    """Error double carrying sqlstate and vendor_code for dialect tests."""

    def __init__(self, message: str, *, sqlstate: str | None = None, vendor_code: int | None = None) -> None:
        super().__init__(message)
        self.sqlstate = sqlstate
        self.vendor_code = vendor_code


class _FakeMSSQLSyncProtocolDriver:
    """Protocol-fake sync SQL Server driver supporting metadata, OUTPUT, and nextset()."""

    def __init__(
        self,
        *,
        fail_serverproperty: bool = False,
        server_version_info: tuple[int, ...] | None = None,
        leading_empty_sets: int = 0,
        spin_forever: bool = False,
    ) -> None:
        self.fail_serverproperty = fail_serverproperty
        if server_version_info is not None:
            self.server_version_info = server_version_info
        self.leading_empty_sets = leading_empty_sets
        self.spin_forever = spin_forever
        self.nextset_calls = 0
        self._pending_sets: list[tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None]] = []
        self.queries: list[str] = []

    def begin(self, isolation_level: str | None = None) -> None:
        _ = isolation_level

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None

    def nextset(self) -> Any:
        self.nextset_calls += 1
        if self.spin_forever:
            return ([], None)
        if not self._pending_sets:
            return False
        return self._pending_sets.pop(0)

    def execute_with_description(
        self,
        sql: str,
        parameters: Any = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None]:
        _ = parameters
        self.queries.append(sql)
        upper = sql.upper()
        if "SERVERPROPERTY" in upper:
            if self.fail_serverproperty:
                raise ProgrammingError("SERVERPROPERTY unsupported")
            return [("16.0.1000.6",)], (("version", None, None, None, None, None, None),)
        if "@@VERSION" in upper:
            return (
                [("Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4115.5 (X64)",)],
                (("version", None, None, None, None, None, None),),
            )
        if "CAST('TEST MAX SUPPORT'" in upper:
            return [("test max support",)], (("val", None, None, None, None, None, None),)
        if "FN_LISTEXTENDEDPROPERTY" in upper:
            return [(1,)], (("val", None, None, None, None, None, None),)
        if "SCHEMA_NAME()" in upper:
            return [("dbo",)], (("schema_name", None, None, None, None, None, None),)
        if "OUTPUT INSERTED." in upper or "OUTPUT DELETED." in upper:
            output_set = ([(42,)], (("id", None, None, None, None, None, None),))
            if self.spin_forever:
                return [], None
            if self.leading_empty_sets > 0:
                self._pending_sets = [([], None) for _ in range(self.leading_empty_sets - 1)]
                self._pending_sets.append(output_set)
                return [], None
            return output_set
        return [], None


class _FakeMSSQLAsyncProtocolDriver:
    """Protocol-fake async SQL Server driver delegating to _FakeMSSQLSyncProtocolDriver."""

    def __init__(self, sync_driver: _FakeMSSQLSyncProtocolDriver) -> None:
        self._sync = sync_driver
        if hasattr(sync_driver, "server_version_info"):
            self.server_version_info = sync_driver.server_version_info

    async def begin(self, isolation_level: str | None = None) -> None:
        self._sync.begin(isolation_level)

    async def commit(self) -> None:
        self._sync.commit()

    async def rollback(self) -> None:
        self._sync.rollback()

    async def close(self) -> None:
        self._sync.close()

    async def nextset(self) -> Any:
        return self._sync.nextset()

    async def execute_with_description(
        self,
        sql: str,
        parameters: Any = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None]:
        return self._sync.execute_with_description(sql, parameters)


@pytest.mark.parametrize(
    ("fail_serverproperty", "preset_version", "expected_version"),
    [
        (False, None, (16, 0, 1000, 6)),
        (True, None, (16, 0, 4115, 5)),
        (False, (15, 0, 2000, 5), (15, 0, 2000, 5)),
    ],
)
@pytest.mark.anyio
async def test_mssql_get_server_version_info_queries_and_falls_back_to_version(
    fail_serverproperty: bool,
    preset_version: tuple[int, ...] | None,
    expected_version: tuple[int, ...],
) -> None:
    """Verify SERVERPROPERTY parsing, @@VERSION fallback, and raw server_version_info on sync and async MSSQL."""
    sync_driver = _FakeMSSQLSyncProtocolDriver(
        fail_serverproperty=fail_serverproperty,
        server_version_info=preset_version,
    )
    sync_engine = create_engine(
        "mssql+vtb://localhost:1433/master",
        creator=lambda: SyncDBAPIConnection(
            "mssql+vtb://localhost:1433/master",
            driver=sync_driver,
            share_pool=False,
        ),
    )
    try:
        with sync_engine.connect():
            assert sync_engine.dialect.server_version_info == expected_version
            assert sync_engine.dialect.execution_ctx_cls is MSExecutionContext_vtb
    finally:
        sync_engine.dispose()

    async_inner = _FakeMSSQLSyncProtocolDriver(
        fail_serverproperty=fail_serverproperty,
        server_version_info=preset_version,
    )
    async_driver = _FakeMSSQLAsyncProtocolDriver(async_inner)
    async_engine = create_async_engine(
        "mssql+vtb_async://localhost:1433/master",
        async_creator=lambda: AsyncDBAPIConnection(
            "mssql+vtb_async://localhost:1433/master",
            driver=async_driver,
            share_pool=False,
        ),
    )
    try:
        async with async_engine.connect():
            assert async_engine.dialect.server_version_info == expected_version
            assert async_engine.dialect.execution_ctx_cls is MSExecutionContext_vtb
    finally:
        await async_engine.dispose()


@pytest.mark.anyio
async def test_mssql_execution_context_output_and_nextset() -> None:
    """Verify OUTPUT inserted/deleted advance cursor.nextset() across sync and async MSSQL engines."""
    metadata = MetaData()
    tbl = Table(
        "mssql_items",
        metadata,
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )

    sync_driver = _FakeMSSQLSyncProtocolDriver(leading_empty_sets=2)
    sync_engine = create_engine(
        "mssql+vtb://localhost:1433/master",
        creator=lambda: SyncDBAPIConnection(
            "mssql+vtb://localhost:1433/master",
            driver=sync_driver,
            share_pool=False,
        ),
    )
    try:
        with sync_engine.begin() as conn:
            inserted_rows = conn.execute(insert(tbl).values(name="alpha").returning(tbl.c.id)).fetchall()
            assert inserted_rows == [(42,)]
            deleted_rows = conn.execute(delete(tbl).where(tbl.c.id == 42).returning(tbl.c.id)).fetchall()
            assert deleted_rows == [(42,)]
    finally:
        sync_engine.dispose()

    async_driver = _FakeMSSQLAsyncProtocolDriver(_FakeMSSQLSyncProtocolDriver(leading_empty_sets=2))
    async_engine = create_async_engine(
        "mssql+vtb_async://localhost:1433/master",
        async_creator=lambda: AsyncDBAPIConnection(
            "mssql+vtb_async://localhost:1433/master",
            driver=async_driver,
            share_pool=False,
        ),
    )
    try:
        async with async_engine.begin() as aconn:
            inserted_rows = (await aconn.execute(insert(tbl).values(name="beta").returning(tbl.c.id))).fetchall()
            assert inserted_rows == [(42,)]
            deleted_rows = (await aconn.execute(delete(tbl).where(tbl.c.id == 42).returning(tbl.c.id))).fetchall()
            assert deleted_rows == [(42,)]
    finally:
        await async_engine.dispose()

    spinning_driver = _FakeMSSQLSyncProtocolDriver(spin_forever=True)
    spinning_engine = create_engine(
        "mssql+vtb://localhost:1433/master",
        creator=lambda: SyncDBAPIConnection(
            "mssql+vtb://localhost:1433/master",
            driver=spinning_driver,
            share_pool=False,
        ),
    )
    try:
        with spinning_engine.begin() as conn:
            res = conn.execute(insert(tbl).values(name="spin").returning(tbl.c.id))
            assert res.returns_rows is False
            assert 1 <= spinning_driver.nextset_calls <= 32
    finally:
        spinning_engine.dispose()


@pytest.mark.parametrize(
    "dialect_cls",
    [SyncMSSQLDialect, AsyncMSSQLDialect],
)
@pytest.mark.parametrize(
    ("sqlstate", "vendor_code", "expected"),
    [
        ("08S01", None, True),
        ("08001", None, True),
        ("HYT00", None, True),
        ("HY010", None, True),
        (None, 10054, True),
        ("23505", None, False),
        ("40001", None, False),
        (None, 2627, False),
    ],
)
def test_mssql_is_disconnect_codes(
    dialect_cls: type[SyncMSSQLDialect | AsyncMSSQLDialect],
    sqlstate: str | None,
    vendor_code: int | None,
    expected: bool,
) -> None:
    """Verify SyncMSSQLDialect and AsyncMSSQLDialect classify SQLSTATE and vendor disconnect codes."""
    dialect = dialect_cls()
    err = _FakeMSSQLProtocolError("mssql failure", sqlstate=sqlstate, vendor_code=vendor_code)
    assert dialect.is_disconnect(err) is expected


@pytest.mark.anyio
async def test_duckdb_sqlalchemy21_real_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify live sync and async DuckDB engines run DDL, DML, and json_serializer on vertebra._duckdb."""
    pytest.importorskip("vertebra._duckdb", reason=_DUCKDB_SKIP_REASON)
    serializer = msgspec.json.encode
    deserializer = msgspec.json.decode

    import duckdb_engine

    orig_init = duckdb_engine.Dialect.__init__

    def _reject_json_kwargs(self: Any, *args: Any, **kwargs: Any) -> None:
        if "json_serializer" in kwargs or "json_deserializer" in kwargs:
            msg = "unexpected keyword argument 'json_serializer'"
            raise TypeError(msg)
        orig_init(self, *args, **kwargs)

    monkeypatch.setattr(duckdb_engine.Dialect, "__init__", _reject_json_kwargs)
    guarded_dialect = SyncDuckDBDialect(json_serializer=serializer, json_deserializer=deserializer)
    assert guarded_dialect._json_serializer is serializer
    assert guarded_dialect._json_deserializer is deserializer
    monkeypatch.undo()

    sync_engine = create_engine(
        "duckdb+vtb:///:memory:",
        json_serializer=serializer,
        json_deserializer=deserializer,
    )
    metadata = MetaData()
    sync_items = Table(
        "duck_sync_items",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(64)),
    )
    try:
        assert sync_engine.dialect._json_serializer is serializer
        assert sync_engine.dialect._json_deserializer is deserializer
        with sync_engine.begin() as conn:
            metadata.create_all(conn)
            conn.execute(insert(sync_items).values(id=1, name="alpha"))
            rows = conn.execute(select(sync_items)).fetchall()
            assert rows == [(1, "alpha")]
            assert sync_engine.dialect.server_version_info == (8, 0)
            dbapi_conn = conn.connection.dbapi_connection
            assert dbapi_conn is not None
            assert sync_engine.dialect.do_ping(dbapi_conn) is True
    finally:
        sync_engine.dispose()

    async_items = Table(
        "duck_async_items",
        MetaData(),
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("name", String(64)),
    )
    async_engine = create_async_engine("duckdb+vtb_async:///:memory:")
    try:
        async with async_engine.begin() as aconn:
            await aconn.run_sync(async_items.metadata.create_all)
            await aconn.execute(insert(async_items).values(id=2, name="beta"))
            rows = (await aconn.execute(select(async_items))).fetchall()
            assert rows == [(2, "beta")]
            assert async_engine.dialect.server_version_info == (8, 0)
            ping_ok = await aconn.run_sync(
                lambda sc: (
                    sc.connection.dbapi_connection is not None
                    and async_engine.dialect.do_ping(sc.connection.dbapi_connection)
                )
            )
            assert ping_ok is True
    finally:
        await async_engine.dispose()


def test_duckdb_missing_dependency_error_when_uninstalled(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify importing vertebra.extensions.sqlalchemy.dialects.duckdb without duckdb_engine names sqlalchemy-duckdb."""
    import vertebra.extensions.sqlalchemy.dialects.duckdb as duckdb_mod

    monkeypatch.setitem(sys.modules, "duckdb_engine", None)
    reset_dependency_cache("duckdb_engine")
    try:
        with pytest.raises(MissingDependencyError, match=r"sqlalchemy-duckdb"):
            importlib.reload(duckdb_mod)
    finally:
        monkeypatch.undo()
        reset_dependency_cache("duckdb_engine")
        importlib.reload(duckdb_mod)


@pytest.mark.parametrize(
    "dialect_cls",
    [SyncDuckDBDialect, AsyncDuckDBDialect],
)
@pytest.mark.parametrize(
    ("sqlstate", "message", "closed", "expected"),
    [
        ("08006", "socket error", False, True),
        ("08S01", "comm link failure", False, True),
        (None, "database operation failed", True, True),
        (None, "connection closed unexpectedly", False, True),
        ("40001", "serialization failure", False, False),
        ("23505", "unique violation", False, False),
    ],
)
def test_duckdb_is_disconnect_codes(
    dialect_cls: type[SyncDuckDBDialect | AsyncDuckDBDialect],
    sqlstate: str | None,
    message: str,
    closed: bool,
    expected: bool,
) -> None:
    """Verify DuckDB dialects classify SQLSTATE 08*, closed connections, and non-disconnect codes."""

    class _ConnStub:
        def __init__(self, is_closed: bool) -> None:
            self.closed = is_closed

    dialect = dialect_cls()
    err = _FakeMSSQLProtocolError(message, sqlstate=sqlstate)
    assert dialect.is_disconnect(err, _ConnStub(closed)) is expected
