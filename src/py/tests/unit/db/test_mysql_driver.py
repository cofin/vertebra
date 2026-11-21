"""Unit tests for native MySQL sync/async drivers, Cloud SQL dialer, pipelines, Arrow SHM, and mysql+vtb dialect."""

from __future__ import annotations

import asyncio
import datetime
import decimal
import gc
import hashlib
import importlib
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import msgspec
import pyarrow as pa
import pytest
from sqlalchemy import Column, MetaData, Table, create_engine, select, text
from sqlalchemy.ext.asyncio import create_async_engine

import vertebra.db.adapters.mysql.driver as mysql_driver_mod
from vertebra._mysql import (
    AsyncMySQLClient,
    AsyncMySQLCursor,
    MySQLWireTestServer,
    SyncMySQLClient,
    SyncMySQLCursor,
    compile_binds,
    map_mysql_error,
)
from vertebra.db import (
    ArrowCursor,
    AsyncPipelineCapable,
    ConnectionFailure,
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    NotFoundError,
    OperationalError,
    ProgrammingError,
    QueryCancelledError,
    SerializationFailure,
    SyncPipelineCapable,
)
from vertebra.db.adapters.mysql import (
    AsyncMySQLDriver,
    AsyncMySQLPipeline,
    MySQLConfig,
    MySQLCore,
    MySQLPoolConfig,
    SyncMySQLDriver,
    SyncMySQLPipeline,
    build_mysql_connect_kwargs,
    map_mysql_exception,
)
from vertebra.db.adapters.mysql.config import build_mysql_client_kwargs
from vertebra.db.dbapi import AsyncDBAPIConnection, SyncDBAPIConnection
from vertebra.db.dialer import aws_rds_root_ca_pem
from vertebra.db.dialer.cloudsql import AsyncCloudSQLDialer, CloudSQLConfig, SyncCloudSQLDialer
from vertebra.db.driver import AZURE_MYSQL_SCOPE, AwsRdsIamTokenProvider, AzureEntraTokenProvider
from vertebra.extensions.sqlalchemy import (
    AsyncMySQLDialect,
    SyncMySQLDialect,
    Vector,
)
from vertebra.typing import ImproperConfigurationError, MissingDependencyError, reset_dependency_cache


@pytest.fixture
def mysql_server() -> Iterator[MySQLWireTestServer]:
    """Start an ephemeral MySQL binary wire protocol server for deterministic unit testing."""
    srv = MySQLWireTestServer()
    yield srv
    srv.close()


def test_mysql_config_dsn_parsing_and_redaction() -> None:
    """Verify MySQLConfig parses URL parameters, validates inputs, and redacts secrets in repr."""
    cfg = MySQLConfig.from_url(
        "mysql+vtb://usr:super-secret@db.internal:3307/appdb?charset=utf8mb4&autocommit=true&ssl_mode=verify-ca&statement_cache_capacity=64",
        min_size=2,
        max_size=5,
        iam_auth_options={"access_token": "oauth-secret-token"},
    )
    params = cfg.parse_dsn_params()
    assert params["host"] == "db.internal"
    assert params["port"] == 3307
    assert params["user"] == "usr"
    assert params["password"] == "super-secret"
    assert params["database"] == "appdb"
    assert params["charset"] == "utf8mb4"
    assert params["autocommit"] is True
    assert params["ssl_mode"] == "verify-ca"
    assert params["statement_cache_size"] == 64

    rendered = repr(cfg)
    assert "super-secret" not in rendered
    assert "oauth-secret-token" not in rendered
    assert "***" in rendered

    plain_cfg = MySQLConfig(dsn="mysql://localhost/testdb")
    plain_params = plain_cfg.parse_dsn_params()
    assert plain_params["host"] == "localhost"
    assert plain_params["port"] == 3306
    assert plain_params["user"] == "root"
    assert plain_params["password"] == ""
    assert plain_params["database"] == "testdb"
    assert plain_params["autocommit"] is True


@pytest.mark.parametrize(
    ("kwargs", "match_pattern"),
    [
        ({"port": 0}, "port"),
        ({"port": 70000}, "port"),
        ({"host": "bad\x00host"}, "NUL"),
        ({"user": "bad\x00user"}, "NUL"),
        ({"database": "bad\x00db"}, "NUL"),
        ({"ssl_mode": "invalid-mode"}, "ssl_mode"),
        ({"min_size": 0}, "min_size"),
        ({"min_size": 5, "max_size": 2}, "max_size"),
        ({"statement_cache_size": -1}, "statement_cache_size"),
    ],
)
def test_mysql_config_validation_rejects_invalid_inputs(
    kwargs: dict[str, Any],
    match_pattern: str,
) -> None:
    """Verify MySQLConfig and SyncMySQLClient validate port bounds, NUL bytes, SSL mode, and pool bounds."""
    with pytest.raises(ValueError, match=match_pattern):
        MySQLConfig(**kwargs)


def test_native_mysql_client_validation_and_redaction(mysql_server: MySQLWireTestServer) -> None:
    """Verify Rust SyncMySQLClient enforces bounds and redacts passwords in repr."""
    client = SyncMySQLClient(dsn=mysql_server.dsn, password="topsecret-password")
    try:
        assert "topsecret-password" not in repr(client)
        assert "***" in repr(client)
    finally:
        client.close()

    with pytest.raises(ValueError, match="port"):
        SyncMySQLClient(host="127.0.0.1", port=0)

    with pytest.raises(ValueError, match="NUL"):
        SyncMySQLClient(host="127.0.0.1\x00bad")

    with pytest.raises(ValueError, match="ssl_mode"):
        SyncMySQLClient(host="127.0.0.1", ssl_mode="bogus")

    with pytest.raises(ValueError, match="pool_max_size"):
        SyncMySQLClient(host="127.0.0.1", pool_min_size=8, pool_max_size=2)

    pool_cfg = MySQLPoolConfig(min_size=2, max_size=8)
    assert pool_cfg.min_size == 2
    assert pool_cfg.max_size == 8


@pytest.mark.parametrize(
    ("sql", "params", "expected_sql", "expected_params"),
    [
        (
            "SELECT * FROM items WHERE id = :id AND status = ':literal' -- :comment\nAND tag = :tag",
            {"id": 7, "tag": "active"},
            "SELECT * FROM items WHERE id = ? AND status = ':literal' -- :comment\nAND tag = ?",
            (7, "active"),
        ),
        (
            "INSERT INTO items (`col:1`, name) VALUES (%(id)s, %(name)s) /* %(ignored)s */",
            {"id": 11, "name": "alice"},
            "INSERT INTO items (`col:1`, name) VALUES (?, ?) /* %(ignored)s */",
            (11, "alice"),
        ),
        (
            "SELECT * FROM items WHERE a = %s AND b = '?literal' -- %s comment\nAND c = %s",
            (10, 20),
            "SELECT * FROM items WHERE a = ? AND b = '?literal' -- %s comment\nAND c = ?",
            (10, 20),
        ),
    ],
)
def test_compile_for_driver_preserves_literals_comments_and_backticks(
    sql: str,
    params: Any,
    expected_sql: str,
    expected_params: tuple[Any, ...],
) -> None:
    """Verify MySQLCore.compile_for_driver rewrites placeholders outside literals, comments, and backticks."""
    compiled_sql, bound = MySQLCore.compile_for_driver(sql, params)
    assert compiled_sql == expected_sql
    assert bound == expected_params
    assert MySQLCore.compile_for_driver("SELECT 1", None) == ("SELECT 1", ())
    assert MySQLCore.compile_for_driver("SELECT 1", ()) == ("SELECT 1", ())


@pytest.mark.parametrize(
    ("errno", "sqlstate", "expected_cls", "expected_transient"),
    [
        (1062, "23000", IntegrityError, False),
        (1452, "23000", IntegrityError, False),
        (1205, "HY000", SerializationFailure, True),
        (1213, "40001", SerializationFailure, True),
        (1317, "70100", QueryCancelledError, False),
        (3024, "HY000", QueryCancelledError, False),
        (1040, "08004", ConnectionFailure, True),
        (2006, "HY000", ConnectionFailure, True),
        (2013, "HY000", ConnectionFailure, True),
        (1406, "22001", DataError, False),
        (1064, "42000", ProgrammingError, False),
        (1146, "42S02", ProgrammingError, False),
    ],
)
def test_map_mysql_exception_hierarchy_and_transience(
    errno: int,
    sqlstate: str,
    expected_cls: type[DatabaseError],
    expected_transient: bool,
) -> None:
    """Verify MySQL vendor errnos map to the canonical DatabaseError hierarchy in both Rust and Python."""
    raw = RuntimeError(f"ERROR {errno} ({sqlstate}): simulated failure")
    mapped = map_mysql_exception(raw)
    assert isinstance(mapped, expected_cls)
    assert mapped.vendor_code == errno
    assert mapped.sqlstate == sqlstate
    assert mapped.is_transient is expected_transient

    rust_mapped = map_mysql_error(errno, sqlstate, "simulated failure")
    assert isinstance(rust_mapped, expected_cls)


def test_sync_mysql_driver_crud_types_and_statement_cache(mysql_server: MySQLWireTestServer) -> None:
    """Verify SyncMySQLDriver CRUD, binary prepared statement LRU cache, and scalar/vector/JSON/temporal codecs."""
    cfg = MySQLConfig.from_url(mysql_server.dsn, statement_cache_size=32)
    driver = SyncMySQLDriver(cfg)
    try:
        assert driver.ping() is True
        assert driver.connection_id is not None and driver.connection_id >= 1

        driver.execute(
            "CREATE TABLE unit_items ("
            "id INT PRIMARY KEY AUTO_INCREMENT, "
            "name VARCHAR(64) NOT NULL, "
            "amount DECIMAL(10, 2), "
            "payload JSON, "
            "raw_bytes BLOB, "
            "embedding VECTOR(3), "
            "created_on DATE, "
            "created_at DATETIME(6)"
            ")"
        )

        vec_bytes = struct.pack("<3f", 0.25, 0.5, 0.75)
        d_val = datetime.date(2026, 11, 9)
        dt_val = datetime.datetime(2026, 11, 9, 14, 25, 36, 123456)
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")

        affected = driver.execute(
            "INSERT INTO unit_items (id, name, amount, payload, raw_bytes, embedding, created_on, created_at) "
            "VALUES (:id, :name, :amount, :payload, :raw_bytes, :embedding, :created_on, :created_at)",
            {
                "id": 1,
                "name": str(uid),
                "amount": decimal.Decimal("123.45"),
                "payload": {"tier": "pro", "active": True},
                "raw_bytes": b"\x00\x01\xfe\xff",
                "embedding": vec_bytes,
                "created_on": d_val,
                "created_at": dt_val,
            },
        )
        assert affected == 1
        assert driver.statement_cache_len() >= 1

        row = driver.select_one("SELECT * FROM unit_items WHERE id = %s", (1,))
        assert row["id"] == 1
        assert row["name"] == str(uid)
        assert decimal.Decimal(str(row["amount"])) == decimal.Decimal("123.45")
        assert bytes(row["raw_bytes"]) == b"\x00\x01\xfe\xff"
        assert bytes(row["embedding"]) == vec_bytes

        rows, cols = driver.execute_with_description("SELECT id, name FROM unit_items WHERE id = %s", (999,))
        assert rows == []
        assert cols == ("id", "name")

        assert driver.select_one_or_none("SELECT id FROM unit_items WHERE id = %s", (999,)) is None
        with pytest.raises(NotFoundError):
            driver.select_one("SELECT id FROM unit_items WHERE id = %s", (999,))
        with pytest.raises(NotFoundError):
            driver.select_value("SELECT id FROM unit_items WHERE id = %s", (999,))

        batch_count = driver.execute_many(
            "INSERT INTO unit_items (id, name, amount, payload, raw_bytes, embedding, created_on, created_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            [
                (2, "second", decimal.Decimal("10.00"), {"k": 2}, b"ab", vec_bytes, d_val, dt_val),
                (3, "third", decimal.Decimal("20.00"), {"k": 3}, b"cd", vec_bytes, d_val, dt_val),
            ],
        )
        assert batch_count == 2

        items, total = driver.select_with_total(
            "SELECT id, name FROM unit_items ORDER BY id",
            count_sql="SELECT COUNT(*) FROM unit_items",
        )
        assert total == 3
        assert [r["id"] for r in items] == [1, 2, 3]
    finally:
        driver.close()

    with pytest.raises(OperationalError, match="SyncMySQLDriver is closed"):
        driver.select("SELECT 1")


def test_sync_mysql_transactions_savepoints_and_pipeline(mysql_server: MySQLWireTestServer) -> None:
    """Verify SyncMySQLDriver transactions, nested savepoints, and SyncMySQLPipeline batching."""
    driver = SyncMySQLDriver(MySQLConfig.from_url(mysql_server.dsn))
    try:
        driver.execute("CREATE TABLE tx_items (id INT PRIMARY KEY, label VARCHAR(64) NOT NULL)")
        assert isinstance(driver, SyncPipelineCapable)

        with driver.transaction():
            driver.execute("INSERT INTO tx_items (id, label) VALUES (%s, %s)", (1, "outer"))
            with pytest.raises(RuntimeError, match="abort_inner"), driver.transaction():
                driver.execute("INSERT INTO tx_items (id, label) VALUES (%s, %s)", (2, "inner_aborted"))
                raise RuntimeError("abort_inner")
            driver.execute("INSERT INTO tx_items (id, label) VALUES (%s, %s)", (3, "inner_kept"))

        rows = driver.select("SELECT id, label FROM tx_items ORDER BY id")
        assert rows == [{"id": 1, "label": "outer"}, {"id": 3, "label": "inner_kept"}]

        pipe = driver.pipeline()
        assert isinstance(pipe, SyncMySQLPipeline)
        with pipe:
            pipe.execute("INSERT INTO tx_items (id, label) VALUES (%s, %s)", (4, "piped_four"))
            pipe.execute("UPDATE tx_items SET label = %s WHERE id = %s", ("piped_updated", 4))
            pipe.select("SELECT id, label FROM tx_items WHERE id = %s", (4,))
            res = pipe.results()
        assert res == [1, 1, [{"id": 4, "label": "piped_updated"}]]
    finally:
        driver.close()


def test_sync_mysql_arrow_cursor_stream_and_bulk_load(mysql_server: MySQLWireTestServer) -> None:
    """Verify SyncMySQLDriver select_to_arrow, stream_arrow_batches, and load_from_arrow over VTBARROW SHM."""
    driver = SyncMySQLDriver(MySQLConfig.from_url(mysql_server.dsn))
    try:
        driver.execute("CREATE TABLE arrow_items (id INT PRIMARY KEY, name VARCHAR(64), score DOUBLE)")
        source_table = pa.table(
            {
                "id": [1, 2, 3, 4],
                "name": ["a", "b", "c", "d"],
                "score": [1.5, 2.5, 3.5, 4.5],
            }
        )
        loaded = driver.load_from_arrow("arrow_items", source_table)
        assert loaded == 4

        cursor = driver.select_to_arrow("SELECT id, name, score FROM arrow_items ORDER BY id", threshold_rows=1)
        assert isinstance(cursor, ArrowCursor)
        assert cursor.row_count == 4
        assert hasattr(cursor, "__arrow_c_stream__")
        assert cursor.to_pylist() == [
            {"id": 1, "name": "a", "score": 1.5},
            {"id": 2, "name": "b", "score": 2.5},
            {"id": 3, "name": "c", "score": 3.5},
            {"id": 4, "name": "d", "score": 4.5},
        ]

        batches = list(driver.stream_arrow_batches("SELECT id, name FROM arrow_items ORDER BY id", batch_size=2))
        assert len(batches) == 2
        assert sum(b.row_count for b in batches) == 4
    finally:
        driver.close()


@pytest.mark.anyio
async def test_async_mysql_driver_lifecycle_pipeline_arrow_and_cancel(
    mysql_server: MySQLWireTestServer,
) -> None:
    """Verify AsyncMySQLDriver native execution, pipeline, Arrow streaming, and KILL QUERY cancellation."""
    driver = AsyncMySQLDriver(MySQLConfig.from_url(mysql_server.dsn))
    try:
        assert isinstance(driver, AsyncPipelineCapable)
        assert await driver.ping() is True
        await driver.execute("CREATE TABLE async_items (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)")

        pipe = driver.pipeline()
        assert isinstance(pipe, AsyncMySQLPipeline)
        async with pipe:
            pipe.execute("INSERT INTO async_items (id, name) VALUES (%s, %s)", (10, "async_one"))
            pipe.execute("INSERT INTO async_items (id, name) VALUES (%s, %s)", (20, "async_two"))
            pipe.select("SELECT id, name FROM async_items ORDER BY id")
            results = await pipe.results()
        assert results == [
            1,
            1,
            [{"id": 10, "name": "async_one"}, {"id": 20, "name": "async_two"}],
        ]

        rows, desc = await driver.execute_with_description("SELECT id, name FROM async_items ORDER BY id")
        assert rows == [(10, "async_one"), (20, "async_two")]
        assert desc == ("id", "name")

        empty_rows, empty_desc = await driver.execute_with_description(
            "SELECT id, name FROM async_items WHERE id = %s",
            (999,),
        )
        assert empty_rows == []
        assert empty_desc == ("id", "name")

        with pytest.raises(NotFoundError):
            await driver.select_one("SELECT id FROM async_items WHERE id = %s", (999,))
        with pytest.raises(NotFoundError):
            await driver.select_value("SELECT id FROM async_items WHERE id = %s", (999,))

        arrow_cur = await driver.select_to_arrow("SELECT id, name FROM async_items ORDER BY id")
        assert isinstance(arrow_cur, ArrowCursor)
        assert arrow_cur.row_count == 2

        await driver.cancel()
        assert mysql_server.kill_query_count >= 1
    finally:
        await driver.close()

    with pytest.raises(OperationalError, match="AsyncMySQLDriver is closed"):
        await driver.select("SELECT 1")


def test_mysql_cloudsql_and_iam_connect_kwargs_wiring() -> None:
    """Verify MySQLConfig wires Cloud SQL (:3307 mTLS) and IAM auth (:3306 token) into native connect kwargs."""
    cfg = MySQLConfig(
        dsn="mysql://iam-user@10.0.0.8:3307/prod_db",
        cloud_sql_instance="myproj:us-central1:myinst",
        enable_iam_auth=True,
        iam_auth_options={"access_token": "ephemeral-iam-token"},
        min_size=2,
        max_size=6,
    )
    kwargs = build_mysql_connect_kwargs(cfg)
    assert kwargs["host"] == "10.0.0.8"
    assert kwargs["port"] == 3307
    assert kwargs["user"] == "iam-user"
    assert kwargs["database"] == "prod_db"
    assert kwargs["cloud_sql_instance"] == "myproj:us-central1:myinst"
    assert kwargs["enable_iam_auth"] is True
    assert kwargs["pool_min_size"] == 2
    assert kwargs["pool_max_size"] == 6

    sync_dialer = SyncCloudSQLDialer(CloudSQLConfig(instance_connection_name="myproj:us-central1:myinst"))
    async_dialer = AsyncCloudSQLDialer(CloudSQLConfig(instance_connection_name="myproj:us-central1:myinst"))
    sync_drv = SyncMySQLDriver(cfg, dialer=sync_dialer)
    async_drv = AsyncMySQLDriver(cfg, dialer=async_dialer)
    assert sync_drv.dialer is sync_dialer
    assert async_drv.dialer is async_dialer


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_mysql_missing_extension_raises_missing_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
    asynchronous: bool,
) -> None:
    """Verify SyncMySQLDriver and AsyncMySQLDriver raise MissingDependencyError when vertebra._mysql is absent."""
    assert not hasattr(mysql_driver_mod, "_mysql_ext")
    monkeypatch.setitem(sys.modules, "vertebra._mysql", None)
    reset_dependency_cache("vertebra._mysql")
    try:
        cfg = MySQLConfig(dsn="mysql://localhost:3306/app")
        if asynchronous:
            async_driver = AsyncMySQLDriver(cfg)
            with pytest.raises(MissingDependencyError, match="vertebra-core\\[mysql\\]"):
                await async_driver.select("SELECT 1")
        else:
            driver = SyncMySQLDriver(cfg)
            with pytest.raises(MissingDependencyError, match="vertebra-core\\[mysql\\]"):
                driver.select("SELECT 1")
    finally:
        reset_dependency_cache("vertebra._mysql")


@pytest.mark.anyio
async def test_async_mysql_cancelled_error_not_swallowed() -> None:
    """Verify AsyncMySQLDriver propagates asyncio.CancelledError without wrapping into OperationalError."""
    client = MagicMock()
    client.is_closed = False
    client.cursor.return_value.execute = AsyncMock(side_effect=asyncio.CancelledError())
    driver = AsyncMySQLDriver(MySQLConfig(dsn="mysql://localhost:3306/app"), client=client)
    with pytest.raises(asyncio.CancelledError):
        await driver.select("SELECT 1")


@pytest.mark.anyio
async def test_mysql_parse_dsn_params_async_avoids_sync_iam_resolution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify parse_dsn_params_async awaits resolve_iam_password_async without blocking on sync resolution."""
    cfg = MySQLConfig(dsn="mysql://localhost:3306/app", enable_iam_auth=True)

    def _fail_sync(_self: MySQLConfig, *, force_refresh: bool = False) -> str | None:
        msg = f"sync IAM resolution called during async parse_dsn_params_async ({force_refresh})"
        raise AssertionError(msg)

    monkeypatch.setattr(MySQLConfig, "resolve_iam_password_sync", _fail_sync)
    monkeypatch.setattr(MySQLConfig, "resolve_iam_password_async", AsyncMock(return_value="async-iam-token"))
    params = await cfg.parse_dsn_params_async()
    assert params["password"] == "async-iam-token"


def test_dbapi_connection_routes_mysql_and_dialect_compilation(
    mysql_server: MySQLWireTestServer,
) -> None:
    """Verify SyncDBAPIConnection routes mysql+vtb URLs to SyncMySQLDriver and compiles MySQL vector types."""
    driver = SyncMySQLDriver(MySQLConfig.from_url(mysql_server.dsn))
    driver.execute("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)")
    driver.execute("INSERT INTO users (id, name) VALUES (1, 'alpha'), (2, 'beta')")

    conn = SyncDBAPIConnection(mysql_server.dsn.replace("mysql://", "mysql+vtb://"), driver=driver, share_pool=False)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, name FROM users WHERE id = %s", (1,))
        rows = cur.fetchall()
        assert rows == [(1, "alpha")]
        assert cur.description is not None
        assert cur.description[0][0] == "id"
        conn.commit()
        conn.rollback()
    finally:
        conn.close()

    assert SyncMySQLDialect.paramstyle == "qmark"
    assert SyncMySQLDialect.default_paramstyle == "qmark"
    assert SyncMySQLDialect().insert_executemany_returning is False
    assert SyncMySQLDialect.supports_statement_cache is True

    vec = Vector(512)
    assert vec.get_col_spec("mysql+vtb") == "VECTOR(512)"
    dialect_instance: Any = SyncMySQLDialect()
    tbl = Table("items", MetaData(), Column("embedding", vec))
    compiled_dist = (
        select(tbl).where(tbl.c.embedding.cosine_distance([0.1, 0.2]) < 0.5).compile(dialect=dialect_instance)
    )
    assert "DISTANCE_COSINE(items.embedding, ?)" in str(compiled_dist)
    assert dialect_instance.type_descriptor(vec) is vec
    assert SyncMySQLClient is not None
    assert AsyncMySQLClient is not None


@pytest.mark.parametrize(
    ("symbol_name", "target_module", "expected_module"),
    [
        ("SyncMySQLDriver", "vertebra.db.adapters.mysql", "vertebra.db.adapters.mysql._sync"),
        ("SyncMySQLDriver", "vertebra.db.adapters.mysql.driver", "vertebra.db.adapters.mysql._sync"),
        ("SyncMySQLDriver", "vertebra.db.adapters.mysql._sync", "vertebra.db.adapters.mysql._sync"),
        ("AsyncMySQLDriver", "vertebra.db.adapters.mysql", "vertebra.db.adapters.mysql._async"),
        ("AsyncMySQLDriver", "vertebra.db.adapters.mysql.driver", "vertebra.db.adapters.mysql._async"),
        ("AsyncMySQLDriver", "vertebra.db.adapters.mysql._async", "vertebra.db.adapters.mysql._async"),
        ("SyncMySQLPipeline", "vertebra.db.adapters.mysql", "vertebra.db.adapters.mysql.pipeline"),
        ("SyncMySQLPipeline", "vertebra.db.adapters.mysql.driver", "vertebra.db.adapters.mysql.pipeline"),
        ("SyncMySQLPipeline", "vertebra.db.adapters.mysql.pipeline", "vertebra.db.adapters.mysql.pipeline"),
        ("AsyncMySQLPipeline", "vertebra.db.adapters.mysql", "vertebra.db.adapters.mysql.pipeline"),
        ("AsyncMySQLPipeline", "vertebra.db.adapters.mysql.driver", "vertebra.db.adapters.mysql.pipeline"),
        ("AsyncMySQLPipeline", "vertebra.db.adapters.mysql.pipeline", "vertebra.db.adapters.mysql.pipeline"),
    ],
)
def test_mysql_driver_and_pipeline_resolution_identity(
    symbol_name: str,
    target_module: str,
    expected_module: str,
) -> None:
    """Verify MySQL sync/async driver and pipeline classes resolve identically across import paths."""
    mod = importlib.import_module(target_module)
    exp_mod = importlib.import_module(expected_module)
    assert getattr(mod, symbol_name) is getattr(exp_mod, symbol_name)


@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
def test_sync_mysql_pipeline_exit_contract(mysql_server: MySQLWireTestServer, scenario: str) -> None:
    """SyncMySQLPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    driver = SyncMySQLDriver(MySQLConfig.from_url(mysql_server.dsn))
    try:
        driver.execute("CREATE TABLE pipe_r114 (id INT PRIMARY KEY, label VARCHAR(64) NOT NULL)")
        if scenario == "auto_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "auto"))
            assert driver.in_transaction is False
            assert driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [{"id": 1, "label": "auto"}]
        elif scenario == "tail_flush":
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "first"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "tail"))
            assert driver.in_transaction is False
            assert driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [
                {"id": 1, "label": "first"},
                {"id": 2, "label": "tail"},
            ]
        elif scenario == "caller_tx_rollback":
            driver.begin()
            driver.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "outer"))
            with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "inner"))
            assert driver.in_transaction is True
            assert driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [
                {"id": 1, "label": "outer"},
                {"id": 2, "label": "inner"},
            ]
            driver.rollback()
            assert driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "first"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "discarded"))
                raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == []
        else:
            driver.begin()
            driver.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "outer_kept"))
            with pytest.raises(RuntimeError, match="abort_inner"), driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "inner_flushed"))
                assert pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (3, "inner_discarded"))
                raise RuntimeError("abort_inner")
            assert driver.in_transaction is True
            driver.commit()
            assert driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [
                {"id": 1, "label": "outer_kept"},
            ]
    finally:
        driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "scenario",
    ["auto_flush", "tail_flush", "caller_tx_rollback", "exception_discard", "caller_tx_exception"],
)
async def test_async_mysql_pipeline_exit_contract(mysql_server: MySQLWireTestServer, scenario: str) -> None:
    """AsyncMySQLPipeline honors unified R1.14 flush, commit, discard, and caller tx ownership."""
    driver = AsyncMySQLDriver(MySQLConfig.from_url(mysql_server.dsn))
    try:
        await driver.execute("CREATE TABLE pipe_r114 (id INT PRIMARY KEY, label VARCHAR(64) NOT NULL)")
        if scenario == "auto_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "auto"))
            assert driver.in_transaction is False
            assert await driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [
                {"id": 1, "label": "auto"},
            ]
        elif scenario == "tail_flush":
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "first"))
                assert await pipe.results() == [1]
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "tail"))
            assert driver.in_transaction is False
            assert await driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [
                {"id": 1, "label": "first"},
                {"id": 2, "label": "tail"},
            ]
        elif scenario == "caller_tx_rollback":
            await driver.begin()
            await driver.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "outer"))
            async with driver.pipeline() as pipe:
                pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "inner"))
            assert driver.in_transaction is True
            assert await driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [
                {"id": 1, "label": "outer"},
                {"id": 2, "label": "inner"},
            ]
            await driver.rollback()
            assert await driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == []
        elif scenario == "exception_discard":
            with pytest.raises(RuntimeError, match="abort"):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "first"))
                    assert await pipe.results() == [1]
                    pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "discarded"))
                    raise RuntimeError("abort")
            assert driver.in_transaction is False
            assert await driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == []
        else:
            await driver.begin()
            await driver.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (1, "outer_kept"))
            with pytest.raises(RuntimeError, match="abort_inner"):
                async with driver.pipeline() as pipe:
                    pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (2, "inner_flushed"))
                    assert await pipe.results() == [1]
                    pipe.execute("INSERT INTO pipe_r114 (id, label) VALUES (%s, %s)", (3, "inner_discarded"))
                    raise RuntimeError("abort_inner")
            assert driver.in_transaction is True
            await driver.commit()
            assert await driver.select("SELECT id, label FROM pipe_r114 ORDER BY id") == [
                {"id": 1, "label": "outer_kept"},
            ]
    finally:
        await driver.close()


def _mysql_packet(seq: int, payload: bytes) -> bytes:
    length = len(payload)
    header = bytes((length & 0xFF, (length >> 8) & 0xFF, (length >> 16) & 0xFF, seq & 0xFF))
    return header + payload


def _read_mysql_packet(conn: socket.socket) -> tuple[int, bytes]:
    header = b""
    while len(header) < 4:
        chunk = conn.recv(4 - len(header))
        if not chunk:
            raise EOFError("unexpected EOF reading MySQL header")
        header += chunk
    length = header[0] | (header[1] << 8) | (header[2] << 16)
    seq = header[3]
    payload = b""
    while len(payload) < length:
        chunk = conn.recv(length - len(payload))
        if not chunk:
            raise EOFError("unexpected EOF reading MySQL payload")
        payload += chunk
    return seq, payload


def _mysql_native_scramble(password: str, nonce: bytes) -> bytes:
    if not password:
        return b""
    stage1 = hashlib.sha1(password.encode("utf-8")).digest()
    stage2 = hashlib.sha1(stage1).digest()
    challenge = hashlib.sha1(nonce + stage2).digest()
    return bytes(a ^ b for a, b in zip(stage1, challenge, strict=True))


def _extract_handshake_auth_data(payload: bytes) -> bytes:
    caps = struct.unpack_from("<I", payload, 0)[0]
    pos = 32
    nul = payload.find(b"\x00", pos)
    pos = nul + 1
    if caps & 0x0020_0000:
        first = payload[pos]
        pos += 1
        auth_len = first if first < 251 else struct.unpack_from("<H", payload, pos)[0]
        if first >= 251:
            pos += 2
    else:
        auth_len = payload[pos]
        pos += 1
    return payload[pos : pos + auth_len]


def _mysql_handshake_v10(conn_id: int, nonce: bytes, extra_caps: int = 0) -> bytes:
    caps = (
        extra_caps
        | 0x0000_0001
        | 0x0000_0002
        | 0x0000_0004
        | 0x0000_0008
        | 0x0000_0200
        | 0x0000_2000
        | 0x0000_8000
        | 0x0008_0000
        | 0x0020_0000
    )
    return (
        b"\x0a"
        + b"8.4.0-Vertebra\x00"
        + struct.pack("<I", conn_id)
        + nonce[:8]
        + b"\x00"
        + struct.pack("<H", caps & 0xFFFF)
        + b"\x2d"
        + struct.pack("<H", 0x0002)
        + struct.pack("<H", (caps >> 16) & 0xFFFF)
        + b"\x15"
        + (b"\x00" * 10)
        + nonce[8:20]
        + b"\x00"
        + b"mysql_native_password\x00"
    )


def _mysql_ok_packet() -> bytes:
    return b"\x00\x00\x00\x02\x00\x00\x00"


def _mysql_err_packet(errno: int, sqlstate: str, message: str) -> bytes:
    return b"\xff" + struct.pack("<H", errno) + b"#" + sqlstate.encode("ascii") + message.encode("utf-8")


_MYSQL_CLIENT_SSL_CAPABILITY = 0x0000_0800


def _generate_mysql_tls_credentials(subject_alt_name: str) -> tuple[str, str]:
    """Generate a self-signed leaf certificate and private key carrying one SubjectAltName entry."""
    with tempfile.TemporaryDirectory(prefix="vtb-mysql-tls-") as tmpdir:
        key_path = Path(tmpdir) / "key.pem"
        cert_path = Path(tmpdir) / "cert.pem"
        cmd = [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key_path),
            "-out",
            str(cert_path),
            "-days",
            "1",
            "-subj",
            "/CN=vtb-mysql-loopback",
            "-addext",
            f"subjectAltName={subject_alt_name}",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
        ]
        subprocess.run(cmd, check=True, capture_output=True, timeout=30)
        return cert_path.read_text(encoding="utf-8"), key_path.read_text(encoding="utf-8")


class _MySQLTlsLoopbackServer:
    """Loopback MySQL wire fake that upgrades to TLS after SSLRequest and rejects the login with error 1045.

    The fake records `ssl_request`, `tls_established`, `tls_rejected`, and `login:<user>` events so tests can
    tell a completed TLS handshake (the client reached authentication) from a certificate rejection.
    """

    def __init__(self, cert_pem: str, key_pem: str) -> None:
        self.events: list[str] = []
        self._ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        with tempfile.TemporaryDirectory(prefix="vtb-mysql-srv-") as tmpdir:
            cert_path = Path(tmpdir) / "cert.pem"
            key_path = Path(tmpdir) / "key.pem"
            cert_path.write_text(cert_pem, encoding="utf-8")
            key_path.write_text(key_pem, encoding="utf-8")
            self._ctx.load_cert_chain(str(cert_path), str(key_path))
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(8)
        self._listener.settimeout(0.2)
        self.port: int = self._listener.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, name="vtb-mysql-tls-fake", daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        conn_id = 0
        while not self._stop.is_set():
            try:
                conn, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            conn_id += 1
            with conn:
                conn.settimeout(5.0)
                try:
                    self._handle(conn, conn_id)
                except (OSError, EOFError):
                    self.events.append("io_closed")

    def _handle(self, conn: socket.socket, conn_id: int) -> None:
        nonce = hashlib.sha1(f"vtb-tls-{conn_id}".encode()).digest()
        conn.sendall(_mysql_packet(0, _mysql_handshake_v10(conn_id, nonce, _MYSQL_CLIENT_SSL_CAPABILITY)))
        _, ssl_request = _read_mysql_packet(conn)
        client_caps = struct.unpack_from("<I", ssl_request, 0)[0]
        if len(ssl_request) != 32 or not client_caps & _MYSQL_CLIENT_SSL_CAPABILITY:
            self.events.append("plaintext_login")
            return
        self.events.append("ssl_request")
        try:
            tls_conn = self._ctx.wrap_socket(conn, server_side=True)
        except ssl.SSLError:
            self.events.append("tls_rejected")
            return
        with tls_conn:
            self.events.append("tls_established")
            try:
                seq, login = _read_mysql_packet(tls_conn)
            except (ssl.SSLError, EOFError):
                self.events.append("tls_rejected")
                return
            user = login[32 : login.index(b"\x00", 32)].decode("utf-8")
            self.events.append(f"login:{user}")
            tls_conn.sendall(_mysql_packet(seq + 1, _mysql_err_packet(1045, "28000", "Access denied for user")))

    def close(self) -> None:
        self._stop.set()
        self._listener.close()
        self._thread.join(timeout=5.0)


@pytest.fixture(scope="module")
def mysql_tls_credentials() -> dict[str, tuple[str, str]]:
    """Generate loopback TLS credentials once: a 127.0.0.1 leaf, an unrelated leaf, and a wrong-name leaf."""
    return {
        "loopback": _generate_mysql_tls_credentials("IP:127.0.0.1"),
        "foreign": _generate_mysql_tls_credentials("IP:127.0.0.1"),
        "wrong_name": _generate_mysql_tls_credentials("DNS:other.mysql.example"),
    }


@pytest.mark.parametrize(
    ("ssl_mode", "ca_form", "server_identity", "trusted_identity", "handshake_completes"),
    [
        ("verify-full", "inline", "loopback", "loopback", True),
        ("verify-full", "inline_padded", "loopback", "loopback", True),
        ("verify-full", "path", "loopback", "loopback", True),
        ("verify-ca", "inline", "loopback", "loopback", True),
        ("require", "inline", "loopback", "loopback", True),
        ("require", "inline", "loopback", "rds_bundle", True),
        ("verify-full", "inline", "loopback", "foreign", False),
        ("verify-full", "path", "loopback", "foreign", False),
        ("verify-ca", "inline", "loopback", "foreign", False),
        ("verify-full", "inline", "wrong_name", "wrong_name", False),
        ("verify-full", "inline", "loopback", "rds_bundle", False),
    ],
)
def test_mysql_tls_handshake_with_inline_and_path_ssl_ca(
    mysql_tls_credentials: dict[str, tuple[str, str]],
    tmp_path: Path,
    ssl_mode: str,
    ca_form: str,
    server_identity: str,
    trusted_identity: str,
    handshake_completes: bool,
) -> None:
    """Verify inline PEM and file-path `ssl_ca` values complete or reject a real loopback TLS handshake."""
    server_cert, server_key = mysql_tls_credentials[server_identity]
    trusted_cert = (
        aws_rds_root_ca_pem() if trusted_identity == "rds_bundle" else mysql_tls_credentials[trusted_identity][0]
    )
    if ca_form == "path":
        ca_file = tmp_path / "ca.pem"
        ca_file.write_text(trusted_cert, encoding="utf-8")
        ssl_ca = str(ca_file)
    elif ca_form == "inline_padded":
        ssl_ca = f"\n  {trusted_cert}"
    else:
        ssl_ca = trusted_cert

    srv = _MySQLTlsLoopbackServer(server_cert, server_key)
    try:
        cfg = MySQLConfig(
            host="127.0.0.1",
            port=srv.port,
            user="tls_user",
            password="test-token-placeholder",
            database="vtb",
            ssl_mode=ssl_mode,
            ssl_ca=ssl_ca,
            connect_timeout=5.0,
        )
        kwargs = build_mysql_client_kwargs(cfg)
        assert kwargs["ssl_mode"] == ssl_mode
        assert kwargs["ssl_ca"] == ssl_ca
        driver = SyncMySQLDriver(cfg)
        try:
            with pytest.raises(DatabaseError) as exc_info:
                driver.connect()
                driver.ping()
        finally:
            driver.close()
    finally:
        srv.close()

    detail = str(exc_info.value)
    assert "File name too long" not in detail
    assert "ssl_request" in srv.events
    if handshake_completes:
        assert "tls_established" in srv.events
        assert "login:tls_user" in srv.events
        assert "Access denied" in detail
    else:
        assert not any(event.startswith("login:") for event in srv.events)
        assert "Access denied" not in detail


def test_mysql_config_token_provider_redaction_and_ssl_defaults() -> None:
    """Verify MySQLConfig redacts RDS credentials, defaults TLS to verify-full, and preserves explicit ssl_mode."""
    provider = AwsRdsIamTokenProvider(
        endpoint_host="db.example.us-east-1.rds.amazonaws.com",
        port=3306,
        region="us-east-1",
        username="iam_user",
        access_key_id="AKIAEXAMPLEKEY1234",
        secret_access_key="test-secret-key-placeholder",
        session_token="test-session-placeholder",
        clock=lambda: 1_794_873_600.0,
    )
    cfg = MySQLConfig(
        dsn="mysql://iam_user:inline-secret@db.example.us-east-1.rds.amazonaws.com:3306/appdb",
        token_provider=provider,
    )
    assert cfg.ssl_mode is None
    assert cfg.canonical_ssl_mode == "verify-full"
    params = cfg.parse_dsn_params()
    assert params["ssl_mode"] == "verify-full"
    expected_token = provider.resolve_sync()
    assert params["password"] == expected_token
    assert "X-Amz-Signature=" in expected_token

    for rendered in (repr(cfg), repr(SyncMySQLDriver(cfg)), repr(AsyncMySQLDriver(cfg))):
        assert "inline-secret" not in rendered
        assert "AKIAEXAMPLEKEY1234" not in rendered
        assert "test-secret-key-placeholder" not in rendered
        assert "test-session-placeholder" not in rendered
        assert "X-Amz-Signature" not in rendered
        assert "AwsRdsIamTokenProvider" in rendered

    explicit_verify = MySQLConfig.from_url(
        "mysql://iam_user@db.example.us-east-1.rds.amazonaws.com:3306/appdb?ssl_mode=verify-ca",
        token_provider=provider,
    )
    assert explicit_verify.canonical_ssl_mode == "verify-ca"
    assert explicit_verify.parse_dsn_params()["ssl_mode"] == "verify-ca"

    explicit_disable = MySQLConfig(
        dsn="mysql://iam_user@127.0.0.1:3306/appdb?ssl_mode=disable",
        token_provider=provider,
    )
    assert explicit_disable.parse_dsn_params()["ssl_mode"] == "disable"

    other_provider = AwsRdsIamTokenProvider(
        endpoint_host="db2.example.us-east-1.rds.amazonaws.com",
        port=3306,
        region="us-east-1",
        username="iam_user",
        access_key_id="AKIAEXAMPLEKEY1234",
        secret_access_key="test-secret-key-placeholder",
        clock=lambda: 1_794_873_600.0,
    )
    assert cfg.ssl_ca == aws_rds_root_ca_pem()
    assert build_mysql_client_kwargs(cfg)["ssl_ca"] == aws_rds_root_ca_pem()
    assert explicit_disable.ssl_ca is None
    assert cfg.pool_key() != MySQLConfig(dsn=cfg.dsn, token_provider=other_provider).pool_key()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("explicit_scope", "expected_scope"),
    [
        (None, AZURE_MYSQL_SCOPE),
        ("https://custom.mysql.example/.default", "https://custom.mysql.example/.default"),
    ],
)
async def test_mysql_config_azure_entra_default_scope_injection(
    explicit_scope: str | None,
    expected_scope: str,
) -> None:
    """Verify MySQLConfig auto-injects AZURE_MYSQL_SCOPE directly on unscoped AzureEntraTokenProvider."""
    seen_scopes: list[str] = []

    def _fetch(scope: str) -> dict[str, Any]:
        seen_scopes.append(scope)
        return {"access_token": f"test-token-placeholder-{scope}", "expires_in": 600}

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="mysql-client",
        client_secret="test-token-placeholder-secret",
        scope=explicit_scope,
        fetcher=_fetch,
        clock=lambda: 1000.0,
    )
    cfg = MySQLConfig(
        dsn="mysql://entra_user@contoso.mysql.database.azure.com:3306/appdb",
        token_provider=provider,
    )
    assert isinstance(cfg.token_provider, AzureEntraTokenProvider)
    assert cfg.token_provider.scope == expected_scope
    assert cfg.canonical_ssl_mode == "verify-full"
    assert cfg.ssl_ca is None
    assert cfg.resolve_iam_password_sync() == f"test-token-placeholder-{expected_scope}"
    assert await cfg.resolve_iam_password_async(force_refresh=True) == f"test-token-placeholder-{expected_scope}"
    assert cfg.parse_dsn_params()["password"] == f"test-token-placeholder-{expected_scope}"
    assert seen_scopes == [expected_scope, expected_scope]


@pytest.mark.parametrize("provider_kind", ["aws_rds", "azure_entra"])
@pytest.mark.parametrize(
    ("host_literal", "is_loopback"),
    [
        ("db.example.us-east-1.rds.amazonaws.com", False),
        ("10.0.0.15", False),
        ("127.0.0.1", True),
        ("localhost", True),
        ("[::1]", True),
    ],
)
def test_mysql_cloud_auth_rds_ca_attachment_and_remote_tls_enforcement(
    provider_kind: str,
    host_literal: str,
    is_loopback: bool,
) -> None:
    """Verify MySQLConfig blocks remote ssl_mode='disable', attaches RDS Root CAs, and allows loopback plaintext."""
    provider: Any
    if provider_kind == "aws_rds":
        provider = AwsRdsIamTokenProvider(
            endpoint_host="db.example.us-east-1.rds.amazonaws.com",
            port=3306,
            region="us-east-1",
            username="iam_user",
            access_key_id="test-token-placeholder-akid",
            secret_access_key="test-token-placeholder-secret",
            clock=lambda: 1_794_873_600.0,
        )
    else:
        provider = AzureEntraTokenProvider(
            fetcher=lambda scope: {"access_token": f"test-token-placeholder-{scope}", "expires_in": 600},
            clock=lambda: 1000.0,
        )

    disabled_dsn = f"mysql://iam_user@{host_literal}:3306/appdb?ssl_mode=disable"
    default_dsn = f"mysql://iam_user@{host_literal}:3306/appdb"

    if not is_loopback:
        with pytest.raises(ImproperConfigurationError, match="ssl_mode='disable'"):
            MySQLConfig(dsn=disabled_dsn, token_provider=provider)
        with pytest.raises(ImproperConfigurationError, match="ssl_mode='disable'"):
            MySQLConfig(host=host_literal, ssl_mode="disable", token_provider=provider)
        with pytest.raises(ImproperConfigurationError, match="ssl_mode='disable'"):
            MySQLConfig.from_url(disabled_dsn, token_provider=provider)

        remote_cfg = MySQLConfig(dsn=default_dsn, token_provider=provider)
        assert remote_cfg.canonical_ssl_mode == "verify-full"
        assert build_mysql_connect_kwargs(remote_cfg)["ssl_mode"] == "verify-full"
        if provider_kind == "aws_rds":
            assert remote_cfg.ssl_ca == aws_rds_root_ca_pem()
            assert build_mysql_connect_kwargs(remote_cfg)["ssl_ca"] == aws_rds_root_ca_pem()
            custom_ca = "-----BEGIN CERTIFICATE-----\nMIIB+TCCAWACCQD=\n-----END CERTIFICATE-----\n"
            custom_cfg = MySQLConfig(dsn=default_dsn, ssl_ca=custom_ca, token_provider=provider)
            assert custom_cfg.ssl_ca == custom_ca
        else:
            assert remote_cfg.ssl_ca is None
    else:
        local_cfg = MySQLConfig(dsn=disabled_dsn, token_provider=provider)
        assert local_cfg.parse_dsn_params()["ssl_mode"] == "disable"
        assert local_cfg.ssl_ca is None


class _StaticMySQLTokenProvider:
    """Non-cloud `DatabaseTokenProvider` fixture returning a static placeholder token."""

    def resolve_sync(self, *, force_refresh: bool = False) -> str:
        _ = force_refresh
        return "test-token-placeholder"

    async def resolve_async(self, *, force_refresh: bool = False) -> str:
        _ = force_refresh
        return "test-token-placeholder"

    def invalidate(self) -> None:
        return None


def _mysql_token_provider(provider_kind: str) -> Any:
    """Build a deterministic AWS RDS, Azure Entra, or static token provider for SSL-mode tests."""
    if provider_kind == "aws_rds":
        return AwsRdsIamTokenProvider(
            endpoint_host="db.example.us-east-1.rds.amazonaws.com",
            port=3306,
            region="us-east-1",
            username="iam_user",
            access_key_id="test-token-placeholder-akid",
            secret_access_key="test-token-placeholder-secret",
            clock=lambda: 1_794_873_600.0,
        )
    if provider_kind == "azure_entra":
        return AzureEntraTokenProvider(
            fetcher=lambda scope: {"access_token": f"test-token-placeholder-{scope}", "expires_in": 600},
            clock=lambda: 1000.0,
        )
    if provider_kind == "static":
        return _StaticMySQLTokenProvider()
    return None


@pytest.mark.parametrize("provider_kind", ["aws_rds", "azure_entra", "static", "none"])
@pytest.mark.parametrize(
    ("config_kwargs", "target_kind"),
    [
        ({"dsn": "mysql://iam_user@db.example.us-east-1.rds.amazonaws.com:3306/appdb"}, "remote"),
        ({"dsn": "mysql://iam_user@10.0.0.15:3306/appdb"}, "remote"),
        ({"host": "db.example.us-east-1.rds.amazonaws.com", "user": "iam_user"}, "remote"),
        ({"dsn": "mysql://iam_user@127.0.0.1:3306/appdb"}, "local"),
        ({"dsn": "mysql://iam_user@localhost:3306/appdb"}, "local"),
        ({"dsn": "mysql://iam_user@[::1]:3306/appdb"}, "local"),
        ({"host": "127.0.0.1", "user": "iam_user"}, "local"),
        ({"host": "db.example.internal", "unix_socket": "/var/run/mysqld/mysqld.sock"}, "local"),
        ({"dsn": "mysql://iam_user@db.example.internal/appdb?unix_socket=/var/run/mysqld/mysqld.sock"}, "local"),
        ({"host": "db.example.internal", "cloud_sql_instance": "proj:us-central1:inst"}, "local"),
    ],
)
def test_mysql_default_ssl_mode_by_provider_and_target(
    provider_kind: str,
    config_kwargs: dict[str, Any],
    target_kind: str,
) -> None:
    """Verify the unset `ssl_mode` resolves to verify-full only for cloud IAM providers on remote targets."""
    provider = _mysql_token_provider(provider_kind)
    is_cloud = provider_kind in {"aws_rds", "azure_entra"}
    if provider is None:
        expected_mode = "prefer"
    elif is_cloud and target_kind == "remote":
        expected_mode = "verify-full"
    else:
        expected_mode = "require"

    cfg = MySQLConfig(token_provider=provider, **config_kwargs)

    assert cfg.ssl_mode is None
    assert cfg.explicit_ssl_mode is None
    assert cfg.canonical_ssl_mode == expected_mode
    assert cfg.parse_dsn_params()["ssl_mode"] == expected_mode
    assert build_mysql_connect_kwargs(cfg)["ssl_mode"] == expected_mode
    assert build_mysql_client_kwargs(cfg)["ssl_mode"] == expected_mode
    assert build_mysql_client_kwargs(cfg, "test-token-placeholder")["ssl_mode"] == expected_mode
    assert f"ssl_mode={expected_mode!r}" in repr(cfg)
    assert expected_mode in cfg.pool_key()
    expected_ca = aws_rds_root_ca_pem() if provider_kind == "aws_rds" and target_kind == "remote" else None
    assert cfg.ssl_ca == expected_ca
    assert build_mysql_client_kwargs(cfg)["ssl_ca"] == expected_ca


@pytest.mark.parametrize("provider_kind", ["aws_rds", "azure_entra"])
@pytest.mark.parametrize(
    ("dsn", "expected_mode"),
    [
        ("cloudsql://proj:us-central1:inst/appdb", "require"),
        ("cloudsql://proj:us-central1:inst/appdb?ssl_mode=disable", "disable"),
    ],
)
def test_mysql_cloud_iam_cloudsql_dsn_keeps_local_ssl_mode_rules(
    provider_kind: str,
    dsn: str,
    expected_mode: str,
) -> None:
    """Verify `cloudsql://` DSNs are never treated as remote cloud IAM targets."""
    cfg = MySQLConfig(dsn=dsn, token_provider=_mysql_token_provider(provider_kind))
    assert cfg.canonical_ssl_mode == expected_mode
    assert cfg.ssl_ca is None
    assert cfg.cloud_sql_instance == "proj:us-central1:inst"
    assert expected_mode in cfg.pool_key()
    assert "proj:us-central1:inst" in cfg.pool_key()
    params = cfg.parse_dsn_params()
    assert params["ssl_mode"] == expected_mode
    assert params["cloud_sql_instance"] == "proj:us-central1:inst"
    core_kwargs = MySQLCore.build_connect_kwargs(cfg)
    assert core_kwargs["ssl_mode"] == expected_mode
    assert core_kwargs["ssl_ca"] is None
    assert core_kwargs["cloud_sql_instance"] == "proj:us-central1:inst"
    client_kwargs = build_mysql_client_kwargs(cfg)
    assert client_kwargs["ssl_mode"] == expected_mode
    assert client_kwargs["ssl_ca"] is None
    assert client_kwargs["cloud_sql_instance"] == "proj:us-central1:inst"


@pytest.mark.parametrize("use_from_url", [False, True])
@pytest.mark.parametrize(
    (
        "dsn",
        "extra_kwargs",
        "expected_instance",
        "expected_user",
        "expected_password",
        "expected_database",
        "expected_ssl_mode",
        "expected_from_url_iam",
    ),
    [
        (
            "cloudsql://proj:us-central1:inst/appdb",
            {},
            "proj:us-central1:inst",
            "root",
            "",
            "appdb",
            "prefer",
            False,
        ),
        (
            "cloudsql://example.com:proj:us-central1:inst/appdb",
            {},
            "example.com:proj:us-central1:inst",
            "root",
            "",
            "appdb",
            "prefer",
            False,
        ),
        (
            "CLOUDSQL://proj:us-central1:inst/appdb",
            {},
            "proj:us-central1:inst",
            "root",
            "",
            "appdb",
            "prefer",
            False,
        ),
        (
            "cloudsql://proj:us-central1:inst?enable_iam_auth=true&ssl_mode=verify-ca",
            {"iam_auth_options": {"access_token": "test-token-placeholder"}},
            "proj:us-central1:inst",
            "root",
            "",
            "vertebra",
            "verify-ca",
            True,
        ),
        (
            "cloudsql://proj:us-central1:inst/",
            {},
            "proj:us-central1:inst",
            "root",
            "",
            "vertebra",
            "prefer",
            False,
        ),
        (
            "cloudsql://alice%40corp.test:p%3As%40w%2Bd%2F1@proj:us-central1:inst/my%20db#frag",
            {},
            "proj:us-central1:inst",
            "alice@corp.test",
            "p:s@w+d/1",
            "my db",
            "prefer",
            False,
        ),
        (
            "cloudsql://alice:@proj:us-central1:inst/appdb",
            {"password": "fallback"},
            "proj:us-central1:inst",
            "alice",
            "",
            "appdb",
            "prefer",
            False,
        ),
        (
            "cloudsql://alice@proj:us-central1:inst/appdb",
            {"password": "fallback"},
            "proj:us-central1:inst",
            "alice",
            "fallback",
            "appdb",
            "prefer",
            False,
        ),
    ],
)
def test_mysql_cloudsql_dsn_valid_matrix(
    use_from_url: bool,
    dsn: str,
    extra_kwargs: dict[str, Any],
    expected_instance: str,
    expected_user: str,
    expected_password: str,
    expected_database: str,
    expected_ssl_mode: str,
    expected_from_url_iam: bool,
) -> None:
    """Verify valid `cloudsql://` DSNs work across construction, pool keys, dialer config, and connect kwargs."""
    cfg = MySQLConfig.from_url(dsn, **extra_kwargs) if use_from_url else MySQLConfig(dsn=dsn, **extra_kwargs)
    expected_iam = expected_from_url_iam if use_from_url else False

    assert cfg.cloud_sql_instance == expected_instance
    assert cfg.enable_iam_auth is expected_iam
    assert cfg.canonical_ssl_mode == expected_ssl_mode
    assert cfg.pool_key() == (
        "mysql",
        "localhost",
        3306,
        expected_database,
        expected_user,
        expected_ssl_mode,
        expected_instance,
    )
    assert cfg.to_cloud_dialer_config() == CloudSQLConfig(
        instance_connection_name=expected_instance,
        enable_iam_auth=expected_iam,
    )

    params = cfg.parse_dsn_params()
    assert params["host"] == "localhost"
    assert params["port"] == 3306
    assert params["user"] == expected_user
    assert params["password"] == ("test-token-placeholder" if expected_iam else expected_password)
    assert params["database"] == expected_database
    assert params["ssl_mode"] == expected_ssl_mode
    assert params["cloud_sql_instance"] == expected_instance

    core_kwargs = MySQLCore.build_connect_kwargs(cfg)
    assert core_kwargs["host"] == "localhost"
    assert core_kwargs["port"] == 3306
    assert core_kwargs["user"] == expected_user
    assert core_kwargs["database"] == expected_database
    assert core_kwargs["ssl_mode"] == expected_ssl_mode
    assert core_kwargs["cloud_sql_instance"] == expected_instance
    assert core_kwargs["enable_iam_auth"] is expected_iam

    client_kwargs = build_mysql_client_kwargs(cfg)
    assert client_kwargs["host"] == "localhost"
    assert client_kwargs["port"] == 3306
    assert client_kwargs["user"] == expected_user
    assert client_kwargs["database"] == expected_database
    assert client_kwargs["ssl_mode"] == expected_ssl_mode
    assert client_kwargs["cloud_sql_instance"] == expected_instance
    assert client_kwargs["enable_iam_auth"] is expected_iam


def test_mysql_cloudsql_explicit_4part_and_matching_instance() -> None:
    """Verify explicit 4-part `cloud_sql_instance` and matching explicit/DSN instances succeed with stable pool keys."""
    tcp_cfg = MySQLConfig(
        dsn="mysql://root@localhost:3306/appdb",
        cloud_sql_instance="example.com:proj:us-central1:inst",
    )
    assert tcp_cfg.cloud_sql_instance == "example.com:proj:us-central1:inst"
    dialer_cfg = tcp_cfg.to_cloud_dialer_config()
    assert dialer_cfg is not None
    assert dialer_cfg.instance_connection_name == "example.com:proj:us-central1:inst"

    matching_cfg = MySQLConfig(
        dsn="cloudsql://proj:us-central1:inst/appdb",
        cloud_sql_instance="proj:us-central1:inst",
    )
    upper_cfg = MySQLConfig(dsn="CLOUDSQL://proj:us-central1:inst/appdb")
    query_match_cfg = MySQLConfig.from_url(
        "cloudsql://proj:us-central1:inst/appdb?cloud_sql_instance=proj:us-central1:inst"
    )
    assert matching_cfg.pool_key() == upper_cfg.pool_key() == query_match_cfg.pool_key()


@pytest.mark.parametrize(
    ("use_from_url", "dsn", "extra_kwargs", "match_pattern"),
    [
        (
            False,
            "cloudsql://proj:us-central1:inst/appdb",
            {"cloud_sql_instance": "other:us-central1:inst"},
            "Conflicting Cloud SQL instance",
        ),
        (
            False,
            "cloudsql://proj:us-central1:inst/appdb?cloud_sql_instance=other:us-central1:inst",
            {},
            "Conflicting Cloud SQL instance",
        ),
        (
            True,
            "cloudsql://proj:us-central1:inst/appdb?cloud_sql_instance=other:us-central1:inst",
            {},
            "Conflicting Cloud SQL instance",
        ),
        (
            False,
            "cloudsql://proj:us-central1:inst/appdb?cloudsql_instance=other:us-central1:inst",
            {},
            "Conflicting Cloud SQL instance",
        ),
        (
            False,
            "cloudsql://proj:us-central1:inst/appdb?cloud_sql_instance=proj:us-central1:inst&cloud_sql_instance=other:us-central1:inst",
            {},
            "Conflicting Cloud SQL instance",
        ),
        (
            False,
            "cloudsql://proj:us-central1:inst/appdb?ssl_mode=bogus",
            {},
            "Unsupported MySQL ssl_mode",
        ),
        (
            False,
            "cloudsql://proj:us-central1:inst/appdb?ssl_mode=require&ssl_mode=bogus",
            {},
            "Unsupported MySQL ssl_mode",
        ),
        (False, "cloudsql://", {}, "Invalid Cloud SQL instance"),
        (False, "cloudsql:///appdb", {}, "Invalid Cloud SQL instance"),
        (False, "cloudsql://alice@", {}, "Invalid Cloud SQL instance"),
        (False, "cloudsql://a:b/db", {}, "Invalid Cloud SQL instance"),
        (False, "cloudsql://a:b:c:d:e/db", {}, "Invalid Cloud SQL instance"),
        (False, "cloudsql://a::c/db", {}, "Invalid Cloud SQL instance"),
        (False, "cloudsql://proj:us-central1:inst/memory", {}, "in-memory"),
        (False, "cloudsql://proj:us-central1:inst/%3Amemory%3A", {}, "in-memory"),
        (False, "cloudsql://user%00bad@proj:us-central1:inst/appdb", {}, "NUL"),
        (False, "cloudsql://user:pass%00bad@proj:us-central1:inst/appdb", {}, "NUL"),
        (False, "cloudsql://proj:us-central1:inst/db%00bad", {}, "NUL"),
    ],
)
def test_mysql_cloudsql_dsn_rejects_invalid_and_conflicting_inputs(
    use_from_url: bool,
    dsn: str,
    extra_kwargs: dict[str, Any],
    match_pattern: str,
) -> None:
    """Verify invalid or conflicting `cloudsql://` DSNs raise `ImproperConfigurationError`."""
    with pytest.raises(ImproperConfigurationError, match=match_pattern):
        if use_from_url:
            MySQLConfig.from_url(dsn, **extra_kwargs)
        else:
            MySQLConfig(dsn=dsn, **extra_kwargs)


@pytest.mark.parametrize("provider_kind", ["aws_rds", "azure_entra"])
@pytest.mark.parametrize("source", ["field", "dsn", "from_url", "field_and_dsn"])
@pytest.mark.parametrize(
    ("mode_literal", "expected_mode"),
    [
        ("disable", None),
        ("disabled", None),
        ("off", None),
        ("prefer", None),
        ("preferred", None),
        ("PREFERRED", None),
        ("require", "require"),
        ("required", "require"),
        ("verify-ca", "verify-ca"),
        ("verify_ca", "verify-ca"),
        ("verify-full", "verify-full"),
        ("verify_identity", "verify-full"),
    ],
)
def test_mysql_cloud_iam_explicit_ssl_mode_on_remote_target(
    provider_kind: str,
    source: str,
    mode_literal: str,
    expected_mode: str | None,
) -> None:
    """Verify remote cloud IAM configs reject plaintext-capable explicit modes and keep deliberate TLS modes."""
    provider = _mysql_token_provider(provider_kind)
    base_dsn = "mysql://iam_user@db.example.us-east-1.rds.amazonaws.com:3306/appdb"
    query_dsn = f"{base_dsn}?ssl_mode={mode_literal}"

    def _build() -> MySQLConfig:
        if source == "field":
            return MySQLConfig(dsn=base_dsn, ssl_mode=mode_literal, token_provider=provider)
        if source == "dsn":
            return MySQLConfig(dsn=query_dsn, token_provider=provider)
        if source == "from_url":
            return MySQLConfig.from_url(query_dsn, token_provider=provider)
        return MySQLConfig(dsn=query_dsn, ssl_mode="verify-full", token_provider=provider)

    if expected_mode is None:
        with pytest.raises(ImproperConfigurationError, match="requires TLS on remote hosts"):
            _build()
        return

    cfg = _build()
    assert cfg.explicit_ssl_mode == expected_mode
    assert cfg.canonical_ssl_mode == expected_mode
    assert cfg.parse_dsn_params()["ssl_mode"] == expected_mode
    assert build_mysql_connect_kwargs(cfg)["ssl_mode"] == expected_mode
    assert build_mysql_client_kwargs(cfg)["ssl_mode"] == expected_mode


@pytest.mark.parametrize("provider_kind", ["aws_rds", "azure_entra"])
@pytest.mark.parametrize(
    ("config_kwargs", "expected_mode"),
    [
        ({"dsn": "mysql://iam_user@127.0.0.1:3306/appdb?ssl_mode=disable"}, "disable"),
        ({"dsn": "mysql://iam_user@127.0.0.1:3306/appdb?ssl_mode=preferred"}, "prefer"),
        ({"host": "localhost", "ssl_mode": "disable"}, "disable"),
        ({"host": "localhost", "ssl_mode": "prefer"}, "prefer"),
        ({"host": "db.example.internal", "unix_socket": "/tmp/mysql.sock", "ssl_mode": "disable"}, "disable"),
        (
            {"host": "db.example.internal", "cloud_sql_instance": "proj:us-central1:inst", "ssl_mode": "prefer"},
            "prefer",
        ),
    ],
)
def test_mysql_cloud_iam_explicit_ssl_mode_on_local_target(
    provider_kind: str,
    config_kwargs: dict[str, Any],
    expected_mode: str,
) -> None:
    """Verify loopback, unix-socket, and Cloud SQL targets keep explicit plaintext-capable modes."""
    cfg = MySQLConfig(token_provider=_mysql_token_provider(provider_kind), **config_kwargs)
    assert cfg.canonical_ssl_mode == expected_mode
    assert build_mysql_client_kwargs(cfg)["ssl_mode"] == expected_mode
    assert cfg.ssl_ca is None


@pytest.mark.parametrize("provider_kind", ["aws_rds", "azure_entra"])
def test_mysql_cloud_iam_default_ssl_mode_is_recomputed_for_new_target(provider_kind: str) -> None:
    """Verify a loopback default never follows a config rebuilt for a remote host, and the reverse."""
    provider = _mysql_token_provider(provider_kind)
    local_cfg = MySQLConfig(dsn="mysql://iam_user@127.0.0.1:3306/appdb", token_provider=provider)
    assert local_cfg.canonical_ssl_mode == "require"

    remote_cfg = msgspec.structs.replace(local_cfg, dsn="mysql://iam_user@db.example.internal:3306/appdb")
    assert remote_cfg.canonical_ssl_mode == "verify-full"
    assert build_mysql_client_kwargs(remote_cfg)["ssl_mode"] == "verify-full"

    sync_driver = SyncMySQLDriver(local_cfg, dsn="mysql://iam_user@db.example.internal:3306/appdb")
    async_driver = AsyncMySQLDriver(local_cfg, dsn="mysql://iam_user@db.example.internal:3306/appdb")
    assert sync_driver.config.canonical_ssl_mode == "verify-full"
    assert async_driver.config.canonical_ssl_mode == "verify-full"

    back_local = msgspec.structs.replace(remote_cfg, dsn="mysql://iam_user@localhost:3306/appdb")
    assert back_local.canonical_ssl_mode == "require"

    with pytest.raises(ImproperConfigurationError, match="requires TLS on remote hosts"):
        msgspec.structs.replace(
            MySQLConfig(host="127.0.0.1", ssl_mode="prefer", token_provider=provider),
            host="db.example.internal",
        )


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysql_drivers_rds_iam_wire_auth_and_close_isolation(mode: str) -> None:
    """Verify Sync and Async MySQL drivers authenticate with SigV4 RDS tokens and isolate close state."""
    provider_a = AwsRdsIamTokenProvider(
        endpoint_host="db.example.us-east-1.rds.amazonaws.com",
        port=3306,
        region="us-east-1",
        username="vtb",
        access_key_id="AKIAEXAMPLEKEY1234",
        secret_access_key="test-secret-key-placeholder",
        session_token="test-session-placeholder",
        clock=lambda: 1_794_873_600.0,
    )
    expected_token = provider_a.resolve_sync()
    srv = MySQLWireTestServer(expected_password=expected_token)
    try:
        cfg_a = MySQLConfig(
            dsn=f"mysql://vtb@127.0.0.1:{srv.port}/vtb?ssl_mode=disable",
            token_provider=provider_a,
        )
        provider_b = AwsRdsIamTokenProvider(
            endpoint_host="db.example.us-east-1.rds.amazonaws.com",
            port=3306,
            region="us-east-1",
            username="vtb",
            access_key_id="AKIAEXAMPLEKEY1234",
            secret_access_key="test-secret-key-placeholder",
            clock=lambda: 1_794_873_600.0,
            signer=lambda *, now_ts: f"wrong-token-{int(now_ts)}",
        )
        cfg_b = MySQLConfig(
            dsn=f"mysql://vtb@127.0.0.1:{srv.port}/vtb?ssl_mode=disable",
            token_provider=provider_b,
        )

        if mode == "sync":
            drv_a = SyncMySQLDriver(cfg_a)
            drv_b = SyncMySQLDriver(cfg_b)
            try:
                drv_a.connect()
                assert drv_a.ping() is True
                drv_a.execute("CREATE TABLE rds_iam_items (id INT PRIMARY KEY, label VARCHAR(32) NOT NULL)")
                drv_a.execute("INSERT INTO rds_iam_items (id, label) VALUES (%s, %s)", (1, "ok"))
                assert drv_a.select("SELECT id, label FROM rds_iam_items") == [{"id": 1, "label": "ok"}]
                drv_a.close()
                assert drv_a.ping() is False
                with pytest.raises(OperationalError, match="closed"):
                    drv_a.execute("SELECT 1")
                drv_a.connect()
                assert drv_a.ping() is True
                with pytest.raises(ProgrammingError):
                    drv_b.ping()
            finally:
                drv_a.close()
                drv_b.close()
        else:
            async_drv_a = AsyncMySQLDriver(cfg_a)
            async_drv_b = AsyncMySQLDriver(cfg_b)
            try:
                await async_drv_a.connect()
                assert await async_drv_a.ping() is True
                await async_drv_a.execute("CREATE TABLE rds_iam_items (id INT PRIMARY KEY, label VARCHAR(32) NOT NULL)")
                await async_drv_a.execute("INSERT INTO rds_iam_items (id, label) VALUES (%s, %s)", (1, "ok"))
                assert await async_drv_a.select("SELECT id, label FROM rds_iam_items") == [{"id": 1, "label": "ok"}]
                await async_drv_a.close()
                assert await async_drv_a.ping() is False
                with pytest.raises(OperationalError, match="closed"):
                    await async_drv_a.execute("SELECT 1")
                await async_drv_a.connect()
                assert await async_drv_a.ping() is True
                with pytest.raises(ProgrammingError):
                    await async_drv_b.ping()
            finally:
                await async_drv_a.close()
                await async_drv_b.close()
    finally:
        srv.close()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysql_drivers_token_provider_refresh_and_auth_retry(mode: str) -> None:
    """Verify Sync and Async MySQL drivers rotate expiring IAM tokens and force-refresh once on 1045."""
    now = [1000.0]
    issued: list[str] = []

    def _sync_mint(*, now_ts: float) -> str:
        token = f"test-token-placeholder-{len(issued) + 1}-{int(now_ts)}"
        issued.append(token)
        return token

    async def _async_mint(*, now_ts: float) -> str:
        await asyncio.sleep(0)
        return _sync_mint(now_ts=now_ts)

    provider = AwsRdsIamTokenProvider(
        endpoint_host="127.0.0.1",
        port=3306,
        region="us-east-1",
        username="iam_user",
        refresh_margin_seconds=60.0,
        token_ttl_seconds=900,
        clock=lambda: now[0],
        signer=_async_mint if mode == "async" else _sync_mint,
    )

    nonce = b"12345678901234567890"
    auth_results: list[tuple[int, bool]] = []
    reject_conn_indices = {2, 4, 5}
    stop_event = threading.Event()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(16)
        srv.settimeout(0.2)
        port = srv.getsockname()[1]

        def _handle_conn(conn: socket.socket, conn_idx: int) -> None:
            with conn:
                conn.sendall(_mysql_packet(0, _mysql_handshake_v10(conn_idx + 1, nonce)))
                try:
                    seq, payload = _read_mysql_packet(conn)
                except EOFError:
                    return
                auth_data = _extract_handshake_auth_data(payload)
                latest_token = issued[-1] if issued else ""
                expected_scramble = _mysql_native_scramble(latest_token, nonce)
                matched = auth_data == expected_scramble
                auth_results.append((conn_idx, matched))
                if conn_idx in reject_conn_indices or not matched:
                    conn.sendall(_mysql_packet(seq + 1, _mysql_err_packet(1045, "28000", "Access denied for user")))
                    return
                conn.sendall(_mysql_packet(seq + 1, _mysql_ok_packet()))
                while True:
                    try:
                        cmd_seq, cmd_payload = _read_mysql_packet(conn)
                    except EOFError:
                        break
                    if not cmd_payload or cmd_payload[0] == 0x01:
                        break
                    if cmd_payload[0] == 0x0E:
                        conn.sendall(_mysql_packet(cmd_seq + 1, _mysql_ok_packet()))
                    else:
                        conn.sendall(_mysql_packet(cmd_seq + 1, _mysql_ok_packet()))

        def _server() -> None:
            idx = 0
            while not stop_event.is_set():
                try:
                    conn, _ = srv.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                t = threading.Thread(target=_handle_conn, args=(conn, idx), daemon=True)
                t.start()
                idx += 1

        srv_thread = threading.Thread(target=_server, daemon=True)
        srv_thread.start()

        cfg = MySQLConfig(
            dsn=f"mysql://iam_user@127.0.0.1:{port}/appdb?ssl_mode=disable",
            token_provider=provider,
        )
        try:
            if mode == "sync":
                driver = SyncMySQLDriver(cfg)
                try:
                    driver.connect()
                    assert driver.ping() is True
                    assert len(issued) == 1

                    now[0] = 1850.0
                    assert driver.ping() is True
                    assert len(issued) == 2

                    driver.close()
                    driver.connect()
                    assert driver.ping() is True
                    assert len(issued) == 3

                    driver.close()
                    driver.connect()
                    with pytest.raises(ProgrammingError) as exc_info:
                        driver.ping()
                    assert exc_info.value.vendor_code == 1045
                    assert exc_info.value.sqlstate == "28000"
                    assert len(issued) == 4
                    for tok in issued:
                        assert tok not in repr(driver)
                        assert tok not in str(exc_info.value)
                finally:
                    driver.close()
            else:
                async_driver = AsyncMySQLDriver(cfg)
                try:
                    await async_driver.connect()
                    assert await async_driver.ping() is True
                    assert len(issued) == 1

                    now[0] = 1850.0
                    assert await async_driver.ping() is True
                    assert len(issued) == 2

                    await async_driver.close()
                    await async_driver.connect()
                    assert await async_driver.ping() is True
                    assert len(issued) == 3

                    await async_driver.close()
                    await async_driver.connect()
                    with pytest.raises(ProgrammingError) as exc_info:
                        await async_driver.ping()
                    assert exc_info.value.vendor_code == 1045
                    assert exc_info.value.sqlstate == "28000"
                    assert len(issued) == 4
                    for tok in issued:
                        assert tok not in repr(async_driver)
                        assert tok not in str(exc_info.value)
                finally:
                    await async_driver.close()
        finally:
            stop_event.set()
            srv_thread.join(timeout=2.0)

    assert len(auth_results) == 6
    assert all(matched for _, matched in auth_results)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("explicit_scope", "expected_scope"),
    [
        (None, AZURE_MYSQL_SCOPE),
        ("https://custom.mysql.example/.default", "https://custom.mysql.example/.default"),
    ],
)
async def test_mysql_drivers_azure_entra_default_scope_wire_auth_and_refresh(
    mode: str,
    explicit_scope: str | None,
    expected_scope: str,
) -> None:
    """Verify Sync and Async MySQL drivers inject AZURE_MYSQL_SCOPE, pass bearer tokens, and refresh pre-expiry."""
    now = [1000.0]
    seen_scopes: list[str] = []
    issued: list[str] = []

    def _sync_fetch(scope: str) -> dict[str, Any]:
        seen_scopes.append(scope)
        tok = f"test-token-placeholder-mysql-{len(issued) + 1}"
        issued.append(tok)
        return {"access_token": tok, "expires_in": 600}

    async def _async_fetch(scope: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return _sync_fetch(scope)

    provider = AzureEntraTokenProvider(
        tenant_id="contoso.onmicrosoft.com",
        client_id="mysql-client",
        client_secret="test-client-secret-placeholder",
        scope=explicit_scope,
        refresh_margin_seconds=120.0,
        clock=lambda: now[0],
        fetcher=_async_fetch if mode == "async" else _sync_fetch,
    )

    nonce = b"12345678901234567890"
    auth_results: list[bool] = []
    stop_event = threading.Event()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(8)
        srv.settimeout(0.2)
        port = srv.getsockname()[1]

        def _handle_conn(conn: socket.socket, conn_idx: int) -> None:
            with conn:
                conn.sendall(_mysql_packet(0, _mysql_handshake_v10(conn_idx + 1, nonce)))
                try:
                    seq, payload = _read_mysql_packet(conn)
                except EOFError:
                    return
                auth_data = _extract_handshake_auth_data(payload)
                latest_token = issued[-1] if issued else ""
                matched = auth_data == _mysql_native_scramble(latest_token, nonce)
                auth_results.append(matched)
                if not matched:
                    conn.sendall(_mysql_packet(seq + 1, _mysql_err_packet(1045, "28000", "Access denied for user")))
                    return
                conn.sendall(_mysql_packet(seq + 1, _mysql_ok_packet()))
                while True:
                    try:
                        cmd_seq, cmd_payload = _read_mysql_packet(conn)
                    except EOFError:
                        break
                    if not cmd_payload or cmd_payload[0] == 0x01:
                        break
                    if cmd_payload[0] == 0x16:
                        prep_ok = b"\x00" + struct.pack("<IHHBH", 1, 0, 0, 0, 0)
                        conn.sendall(_mysql_packet(cmd_seq + 1, prep_ok))
                    else:
                        conn.sendall(_mysql_packet(cmd_seq + 1, _mysql_ok_packet()))

        def _server() -> None:
            idx = 0
            while not stop_event.is_set():
                try:
                    conn, _ = srv.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                t = threading.Thread(target=_handle_conn, args=(conn, idx), daemon=True)
                t.start()
                idx += 1

        srv_thread = threading.Thread(target=_server, daemon=True)
        srv_thread.start()

        cfg = MySQLConfig(
            dsn=f"mysql://entra_user@127.0.0.1:{port}/appdb?ssl_mode=disable",
            token_provider=provider,
        )
        try:
            if mode == "sync":
                driver = SyncMySQLDriver(cfg)
                try:
                    assert driver.config.token_provider.scope == expected_scope
                    driver.connect()
                    assert driver.ping() is True
                    assert len(issued) == 1

                    now[0] = 1480.0
                    assert driver.ping() is True
                    assert len(issued) == 2

                    for rendered in (repr(cfg), repr(driver)):
                        assert "test-client-secret-placeholder" not in rendered
                        assert all(tok not in rendered for tok in issued)
                finally:
                    driver.close()
            else:
                async_driver = AsyncMySQLDriver(cfg)
                try:
                    assert async_driver.config.token_provider.scope == expected_scope
                    await async_driver.connect()
                    assert await async_driver.ping() is True
                    assert len(issued) == 1

                    now[0] = 1480.0
                    assert await async_driver.ping() is True
                    assert len(issued) == 2

                    for rendered in (repr(cfg), repr(async_driver)):
                        assert "test-client-secret-placeholder" not in rendered
                        assert all(tok not in rendered for tok in issued)
                finally:
                    await async_driver.close()
        finally:
            stop_event.set()
            srv_thread.join(timeout=2.0)

    assert seen_scopes == [expected_scope, expected_scope]
    assert auth_results == [True, True]


class _FakeMySQLError(Exception):
    """Exception double carrying vendor_code and sqlstate for MySQL dialect tests."""

    def __init__(self, *args: Any, vendor_code: int | None = None, sqlstate: str | None = None) -> None:
        super().__init__(*args)
        self.vendor_code = vendor_code
        self.sqlstate = sqlstate


@pytest.mark.parametrize(
    "dialect_cls",
    [SyncMySQLDialect, AsyncMySQLDialect],
)
@pytest.mark.parametrize(
    ("exc_factory", "expected_code", "expected_disconnect"),
    [
        (lambda: _FakeMySQLError("MySQL server has gone away", vendor_code=2006), 2006, True),
        (lambda: _FakeMySQLError("Lost connection during query", vendor_code=2013), 2013, True),
        (lambda: _FakeMySQLError(2013, "Lost connection"), 2013, True),
        (
            lambda: (
                RuntimeError("outer")
                if False
                else _raise_with_cause(
                    _FakeMySQLError("wrapped"),
                    _FakeMySQLError("inner gone away", vendor_code=2006),
                )
            ),
            2006,
            True,
        ),
        (lambda: _FakeMySQLError("comm link", sqlstate="08S01"), None, True),
        (lambda: _FakeMySQLError("duplicate entry", vendor_code=1062), 1062, False),
        (lambda: _FakeMySQLError("deadlock", sqlstate="40001"), None, False),
    ],
)
def test_mysql_extract_error_code_and_disconnect(
    dialect_cls: type[SyncMySQLDialect | AsyncMySQLDialect],
    exc_factory: Any,
    expected_code: int | None,
    expected_disconnect: bool,
) -> None:
    """Verify MySQL _extract_error_code and is_disconnect across vendor codes, causes, and SQLSTATE 08*."""
    dialect = dialect_cls()
    err = exc_factory()
    assert dialect._extract_error_code(err) == expected_code
    assert dialect.is_disconnect(err) is expected_disconnect


def _raise_with_cause(outer: Exception, cause: Exception) -> Exception:
    outer.__cause__ = cause
    return outer


class _FakeMySQLProtocolCursor:
    """Minimal PEP 249 sync cursor backed by _FakeMySQLSyncProtocolDriver."""

    description: tuple[tuple[Any, ...], ...] | None
    rowcount: int
    lastrowid: int | None
    arraysize: int

    def __init__(self, driver: _FakeMySQLSyncProtocolDriver) -> None:
        self._driver = driver
        self._rows: list[tuple[Any, ...]] = []
        self._pos = 0
        self.description = None
        self.rowcount = -1
        self.lastrowid = None
        self.arraysize = 1

    def execute(self, sql: str, parameters: Any = ()) -> _FakeMySQLProtocolCursor:
        """Execute a statement through the backing driver and buffer returned rows."""
        self._rows, self.description = self._driver.execute_with_description(sql, parameters)
        self._pos = 0
        self.rowcount = len(self._rows)
        return self

    def executemany(self, sql: str, seq_of_parameters: Any) -> _FakeMySQLProtocolCursor:
        """Execute a statement once per parameter set and record the batch size."""
        count = 0
        for params in seq_of_parameters:
            self.execute(sql, params)
            count += 1
        self._rows = []
        self._pos = 0
        self.description = None
        self.rowcount = count
        return self

    def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next buffered row, or `None` when exhausted."""
        if self._pos >= len(self._rows):
            return None
        row = self._rows[self._pos]
        self._pos += 1
        return row

    def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` buffered rows."""
        n = self.arraysize if size is None else size
        batch = self._rows[self._pos : self._pos + n]
        self._pos += len(batch)
        return batch

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining buffered rows."""
        rest = self._rows[self._pos :]
        self._pos = len(self._rows)
        return rest

    def close(self) -> None:
        """Release any buffered rows."""
        self._rows = []


class _FakeMySQLAsyncProtocolCursor:
    """Minimal PEP 249 async cursor delegating to _FakeMySQLProtocolCursor."""

    def __init__(self, sync_cursor: _FakeMySQLProtocolCursor) -> None:
        self._sync = sync_cursor

    @property
    def description(self) -> tuple[tuple[Any, ...], ...] | None:
        """Return PEP 249 column metadata tuples from the backing cursor."""
        return self._sync.description

    @property
    def rowcount(self) -> int:
        """Return the affected or buffered row count from the backing cursor."""
        return self._sync.rowcount

    @property
    def lastrowid(self) -> int | None:
        """Return the last inserted row identifier from the backing cursor."""
        return self._sync.lastrowid

    @property
    def arraysize(self) -> int:
        """Return the default batch size from the backing cursor."""
        return self._sync.arraysize

    async def execute(self, sql: str, parameters: Any = ()) -> _FakeMySQLAsyncProtocolCursor:
        """Execute a statement asynchronously via the backing sync cursor."""
        self._sync.execute(sql, parameters)
        return self

    async def executemany(self, sql: str, seq_of_parameters: Any) -> _FakeMySQLAsyncProtocolCursor:
        """Execute a statement across parameter sets asynchronously via the backing cursor."""
        self._sync.executemany(sql, seq_of_parameters)
        return self

    async def fetchone(self) -> tuple[Any, ...] | None:
        """Return the next buffered row asynchronously."""
        return self._sync.fetchone()

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        """Return up to `size` buffered rows asynchronously."""
        return self._sync.fetchmany(size)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Return all remaining buffered rows asynchronously."""
        return self._sync.fetchall()

    async def close(self) -> None:
        """Release any buffered rows asynchronously."""
        self._sync.close()


class _FakeMySQLSyncProtocolDriver:
    """Protocol-fake sync MySQL driver supporting initialize(), do_ping(), and isolation levels."""

    def __init__(self) -> None:
        self.isolation_level = "REPEATABLE-READ"
        self.statements: list[str] = []

    def cursor(self, *, lazy_begin: bool = False) -> _FakeMySQLProtocolCursor:
        """Create a sync cursor backed by this driver double."""
        _ = lazy_begin
        return _FakeMySQLProtocolCursor(self)

    def begin(self, isolation_level: str | None = None) -> None:
        _ = isolation_level

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None

    def close(self) -> None:
        return None

    def execute_with_description(
        self,
        sql: str,
        parameters: Any = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None]:
        _ = parameters
        self.statements.append(sql)
        upper = sql.strip().upper()
        if upper == "SELECT VERSION()":
            return [("8.0.36",)], (("version", None, None, None, None, None, None),)
        if upper == "SELECT DATABASE()":
            return [("appdb",)], (("database", None, None, None, None, None, None),)
        if upper == "SELECT @@CHARACTER_SET_CLIENT":
            return [("utf8mb4",)], (("charset", None, None, None, None, None, None),)
        if upper == "SELECT @@TRANSACTION_ISOLATION":
            return [(self.isolation_level,)], (("isolation", None, None, None, None, None, None),)
        if upper == "SELECT @@SQL_MODE":
            return [("STRICT_TRANS_TABLES",)], (("sql_mode", None, None, None, None, None, None),)
        if upper == "SELECT @@LOWER_CASE_TABLE_NAMES":
            return [(0,)], (("lctn", None, None, None, None, None, None),)
        if upper.startswith("SET SESSION TRANSACTION ISOLATION LEVEL "):
            self.isolation_level = upper.removeprefix("SET SESSION TRANSACTION ISOLATION LEVEL ").replace(" ", "-")
            return [], None
        if upper == "SELECT 1":
            return [(1,)], (("1", None, None, None, None, None, None),)
        return [], None


class _FakeMySQLAsyncProtocolDriver:
    """Protocol-fake async MySQL driver delegating to _FakeMySQLSyncProtocolDriver."""

    def __init__(self, sync_driver: _FakeMySQLSyncProtocolDriver) -> None:
        self._sync = sync_driver

    def cursor(self, *, lazy_begin: bool = False) -> _FakeMySQLAsyncProtocolCursor:
        """Create an async cursor backed by this driver double."""
        _ = lazy_begin
        return _FakeMySQLAsyncProtocolCursor(_FakeMySQLProtocolCursor(self._sync))

    async def begin(self, isolation_level: str | None = None) -> None:
        self._sync.begin(isolation_level)

    async def commit(self) -> None:
        self._sync.commit()

    async def rollback(self) -> None:
        self._sync.rollback()

    async def close(self) -> None:
        self._sync.close()

    async def execute_with_description(
        self,
        sql: str,
        parameters: Any = (),
    ) -> tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None]:
        return self._sync.execute_with_description(sql, parameters)


@pytest.mark.anyio
async def test_mysql_sync_and_async_engine_initialize_and_isolation() -> None:
    """Verify create_engine('mysql+vtb://...') and create_async_engine('mysql+vtb_async://...') lifecycle."""
    sync_driver = _FakeMySQLSyncProtocolDriver()
    sync_engine = create_engine(
        "mysql+vtb://localhost:3306/appdb",
        creator=lambda: SyncDBAPIConnection(
            "mysql+vtb://localhost:3306/appdb",
            driver=sync_driver,
            share_pool=False,
        ),
    )
    try:
        with sync_engine.connect() as conn:
            assert sync_engine.dialect.server_version_info == (8, 0, 36)
            assert sync_engine.dialect.default_schema_name == "appdb"
            dbapi_conn = conn.connection.dbapi_connection
            assert dbapi_conn is not None
            assert sync_engine.dialect.do_ping(dbapi_conn) is True
            assert sync_engine.dialect.get_isolation_level(dbapi_conn) == "REPEATABLE READ"
            sync_engine.dialect.set_isolation_level(dbapi_conn, "READ COMMITTED")
            assert sync_engine.dialect.get_isolation_level(dbapi_conn) == "READ COMMITTED"
            assert conn.execute(text("SELECT 1")).scalar() == 1
    finally:
        sync_engine.dispose()

    async_inner = _FakeMySQLSyncProtocolDriver()
    async_driver = _FakeMySQLAsyncProtocolDriver(async_inner)
    async_engine = create_async_engine(
        "mysql+vtb_async://localhost:3306/appdb",
        async_creator=lambda: AsyncDBAPIConnection(
            "mysql+vtb_async://localhost:3306/appdb",
            driver=async_driver,
            share_pool=False,
        ),
    )
    try:
        async with async_engine.connect() as aconn:
            assert async_engine.dialect.server_version_info == (8, 0, 36)
            assert async_engine.dialect.default_schema_name == "appdb"

            def _exercise_async_dbapi(sc: Any) -> tuple[bool, str, str]:
                raw = sc.connection.dbapi_connection
                assert raw is not None
                ping = async_engine.dialect.do_ping(raw)
                before = async_engine.dialect.get_isolation_level(raw)
                async_engine.dialect.set_isolation_level(raw, "READ COMMITTED")
                after = async_engine.dialect.get_isolation_level(raw)
                return ping, before, after

            ping_ok, level, updated_level = await aconn.run_sync(_exercise_async_dbapi)
            assert ping_ok is True
            assert level == "REPEATABLE READ"
            assert updated_level == "READ COMMITTED"
            assert (await aconn.execute(text("SELECT 1"))).scalar() == 1
    finally:
        await async_engine.dispose()


@pytest.mark.parametrize(
    ("sql", "params", "expected_sql", "expected_params"),
    [
        ("SELECT :a", {"a": 1}, "SELECT ?", (1,)),
        ("SELECT %(a)s, :b", {"a": 1, "b": "x"}, "SELECT ?, ?", (1, "x")),
        ("SELECT ? + ?", (1, 2), "SELECT ? + ?", (1, 2)),
        ("SELECT 1", None, "SELECT 1", ()),
        (
            "SELECT :u",
            {"u": uuid.UUID("12345678-1234-5678-1234-567812345678")},
            "SELECT ?",
            ("12345678-1234-5678-1234-567812345678",),
        ),
        ("SELECT :b", {"b": bytearray(b"hi")}, "SELECT ?", (b"hi",)),
    ],
)
def test_sync_cursor_compile_binds(
    sql: str,
    params: Any,
    expected_sql: str,
    expected_params: tuple[Any, ...],
) -> None:
    """Verify compile_binds rewrites named placeholders and coerces bind values."""
    out_sql, out_params = compile_binds(sql, params)
    assert out_sql == expected_sql
    assert out_params == expected_params


def test_sync_cursor_matrix_and_fetch_controls(mysql_server: MySQLWireTestServer) -> None:
    """Verify SyncMySQLCursor select, DML, positional/named binds, executemany, errors, and fetch APIs."""
    client = SyncMySQLClient(mysql_server.dsn)
    try:
        cur = client.cursor()
        assert isinstance(cur, SyncMySQLCursor)
        initial_desc = cur.description
        assert initial_desc is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        assert cur.arraysize == 1

        with pytest.raises(ProgrammingError):
            cur.fetchone()

        assert cur.execute("SELECT 2 AS a, 'x' AS b") is cur
        assert cur.fetchall() == [(2, "x")]
        desc = cur.description
        assert desc is not None
        assert [col[0] for col in desc] == ["a", "b"]
        assert all(len(col) == 7 and col[2:] == (None, None, None, None, None) for col in desc)

        cur.execute("CREATE TEMPORARY TABLE t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        assert cur.description is None
        with pytest.raises(ProgrammingError):
            cur.fetchall()

        cur.execute("INSERT INTO t (v) VALUES (?)", (10,))
        assert cur.rowcount == 1
        assert cur.lastrowid is not None and cur.lastrowid >= 1

        cur.execute("INSERT INTO t (v) VALUES (?)", (20,))
        assert cur.rowcount == 1

        cur.execute("UPDATE t SET v = ? WHERE v >= ?", (99, 10))
        assert cur.rowcount == 2
        assert cur.lastrowid is None

        cur.execute("SELECT v FROM t WHERE v = -1")
        assert cur.description is not None
        assert cur.fetchall() == []

        cur.execute("SELECT ? AS a, ? AS b", (1, 2))
        assert cur.fetchall() == [(1, 2)]

        cur.execute("SELECT :a AS a, :b AS b", {"a": 1, "b": 2})
        assert cur.fetchall() == [(1, 2)]

        cur.executemany("INSERT INTO t (v) VALUES (?)", [(101,), (102,), (103,)])
        assert cur.rowcount == 3
        assert cur.description is None

        before_exec = mysql_server.execute_count
        cur.executemany("INSERT INTO t (v) VALUES (?)", [])
        assert cur.rowcount == 0
        assert cur.description is None
        assert mysql_server.execute_count == before_exec

        cur.execute("SELECT v FROM t ORDER BY id")
        cur.arraysize = 2
        assert cur.arraysize == 2
        assert cur.fetchmany(0) == []
        first_batch = cur.fetchmany()
        assert len(first_batch) == 2
        assert cur.fetchone() is not None
        remaining = cur.fetchall()
        assert len(first_batch) + 1 + len(remaining) == 5
        assert cur.fetchone() is None
        assert cur.fetchall() == []

        with pytest.raises(ProgrammingError):
            cur.arraysize = 0
        with pytest.raises(ProgrammingError):
            cur.fetchmany(-1)

        cur.execute("CREATE TEMPORARY TABLE pk_t (id INT PRIMARY KEY, v INT)")
        cur.execute("INSERT INTO pk_t (id, v) VALUES (?, ?)", (1, 10))
        cur.execute("SELECT 1 AS ok")
        assert cur.description is not None
        with pytest.raises(IntegrityError):
            cur.execute("INSERT INTO pk_t (id, v) VALUES (?, ?)", (1, 20))
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None

        cur.execute("SELECT 1 AS ok")
        with pytest.raises(ProgrammingError):
            cur.execute("SELEC 1")
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None

        with pytest.raises(InterfaceError):
            cur.execute("SELECT ?", "not-a-sequence")

        cur.close()
        cur.close()
        with pytest.raises(InterfaceError):
            cur.execute("SELECT 1")
        with pytest.raises(InterfaceError):
            cur.fetchone()
        with pytest.raises(InterfaceError):
            cur.fetchmany()
        with pytest.raises(InterfaceError):
            cur.fetchall()

        with client.cursor() as ctx_cur:
            ctx_cur.execute("SELECT 1 AS v")
            assert ctx_cur.fetchone() == (1,)
        with pytest.raises(InterfaceError):
            ctx_cur.execute("SELECT 1")
    finally:
        client.close()


def test_sync_cursor_lazy_begin_and_lease_lifecycle(mysql_server: MySQLWireTestServer) -> None:
    """Verify SyncMySQLClient lazy_begin cursor transactions and lease attachment/release."""
    client = SyncMySQLClient(mysql_server.dsn)
    try:
        default_cur = client.cursor()
        default_cur.execute("CREATE TEMPORARY TABLE lb_t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        assert client.in_transaction is False
        default_cur.execute("INSERT INTO lb_t (v) VALUES (?)", (1,))
        assert client.in_transaction is False
        default_cur.close()

        tx_cur = client.cursor(lazy_begin=True)
        tx_cur.execute("INSERT INTO lb_t (v) VALUES (?)", (2,))
        assert client.in_transaction is True
        client.rollback()
        assert client.in_transaction is False

        check_cur = client.cursor()
        check_cur.execute("SELECT v FROM lb_t ORDER BY id")
        assert check_cur.fetchall() == [(1,)]
        check_cur.close()

        tx_cur.execute("INSERT INTO lb_t (v) VALUES (?)", (3,))
        assert client.in_transaction is True
        client.commit()
        assert client.in_transaction is False

        tx_cur.execute("INSERT INTO lb_t (v) VALUES (?)", (4,))
        assert client.in_transaction is True
        client.commit()
        assert client.in_transaction is False

        tx_cur.execute("SELECT v FROM lb_t ORDER BY id")
        assert tx_cur.fetchall() == [(1,), (3,), (4,)]
        client.rollback()

        calls: list[str] = []
        client.attach_lease(lambda: calls.append("first"))
        client.release_lease()
        client.release_lease()
        assert calls == ["first"]

        client.attach_lease(lambda: calls.append("second"))
        client.attach_lease(lambda: calls.append("third"))
        assert calls == ["first", "second"]

        client.close()
        assert calls == ["first", "second", "third"]

        with pytest.raises(ConnectionFailure):
            tx_cur.execute("SELECT 1")
        assert tx_cur.description is None
        assert tx_cur.rowcount == -1
        assert tx_cur.lastrowid is None
    finally:
        client.close()

    drop_calls: list[int] = []
    ephemeral = SyncMySQLClient(mysql_server.dsn)
    ephemeral.attach_lease(lambda: drop_calls.append(1))
    del ephemeral
    gc.collect()
    assert drop_calls == [1]


@pytest.mark.anyio
async def test_async_cursor_matrix_and_fetch_controls(mysql_server: MySQLWireTestServer) -> None:
    """Verify AsyncMySQLCursor select, DML, positional/named binds, executemany, errors, and fetch APIs."""
    client = AsyncMySQLClient(mysql_server.dsn)
    try:
        cur = client.cursor()
        assert isinstance(cur, AsyncMySQLCursor)
        initial_desc = cur.description
        assert initial_desc is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None
        assert cur.arraysize == 1

        with pytest.raises(ProgrammingError):
            await cur.fetchone()

        assert await cur.execute("SELECT 2 AS a, 'x' AS b") is cur
        assert await cur.fetchall() == [(2, "x")]
        desc = cur.description
        assert desc is not None
        assert [col[0] for col in desc] == ["a", "b"]
        assert all(len(col) == 7 and col[2:] == (None, None, None, None, None) for col in desc)

        await cur.execute("CREATE TEMPORARY TABLE at_t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        assert cur.description is None
        with pytest.raises(ProgrammingError):
            await cur.fetchall()

        await cur.execute("INSERT INTO at_t (v) VALUES (?)", (10,))
        assert cur.rowcount == 1
        assert cur.lastrowid is not None and cur.lastrowid >= 1

        await cur.execute("INSERT INTO at_t (v) VALUES (?)", (20,))
        assert cur.rowcount == 1

        await cur.execute("UPDATE at_t SET v = ? WHERE v >= ?", (99, 10))
        assert cur.rowcount == 2
        assert cur.lastrowid is None

        await cur.execute("SELECT v FROM at_t WHERE v = -1")
        assert cur.description is not None
        assert await cur.fetchall() == []

        await cur.execute("SELECT ? AS a, ? AS b", (1, 2))
        assert await cur.fetchall() == [(1, 2)]

        await cur.execute("SELECT :a AS a, :b AS b", {"a": 1, "b": 2})
        assert await cur.fetchall() == [(1, 2)]

        await cur.executemany("INSERT INTO at_t (v) VALUES (?)", [(101,), (102,), (103,)])
        assert cur.rowcount == 3
        assert cur.description is None

        before_exec = mysql_server.execute_count
        await cur.executemany("INSERT INTO at_t (v) VALUES (?)", [])
        assert cur.rowcount == 0
        assert cur.description is None
        assert mysql_server.execute_count == before_exec

        await cur.execute("SELECT v FROM at_t ORDER BY id")
        cur.arraysize = 2
        assert cur.arraysize == 2
        assert await cur.fetchmany(0) == []
        first_batch = await cur.fetchmany()
        assert len(first_batch) == 2
        assert await cur.fetchone() is not None
        remaining = await cur.fetchall()
        assert len(first_batch) + 1 + len(remaining) == 5
        assert await cur.fetchone() is None
        assert await cur.fetchall() == []

        with pytest.raises(ProgrammingError):
            cur.arraysize = 0
        with pytest.raises(ProgrammingError):
            await cur.fetchmany(-1)

        await cur.execute("CREATE TEMPORARY TABLE apk_t (id INT PRIMARY KEY, v INT)")
        await cur.execute("INSERT INTO apk_t (id, v) VALUES (?, ?)", (1, 10))
        await cur.execute("SELECT 1 AS ok")
        with pytest.raises(IntegrityError):
            await cur.execute("INSERT INTO apk_t (id, v) VALUES (?, ?)", (1, 20))
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None

        await cur.execute("SELECT 1 AS ok")
        with pytest.raises(ProgrammingError):
            await cur.execute("SELEC 1")
        assert cur.description is None
        assert cur.rowcount == -1
        assert cur.lastrowid is None

        with pytest.raises(InterfaceError):
            await cur.execute("SELECT ?", "not-a-sequence")

        pending = cur.execute("SELECT 1 AS a")
        with pytest.raises(InterfaceError, match="cursor is already executing"):
            cur.execute("SELECT 2 AS b")
        await pending
        assert await cur.fetchall() == [(1,)]

        await cur.close()
        await cur.close()
        with pytest.raises(InterfaceError):
            await cur.execute("SELECT 1")
        with pytest.raises(InterfaceError):
            await cur.fetchone()
        with pytest.raises(InterfaceError):
            await cur.fetchmany()
        with pytest.raises(InterfaceError):
            await cur.fetchall()
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_fetch_returns_completed_awaitable(mysql_server: MySQLWireTestServer) -> None:
    """Verify AsyncMySQLCursor.fetchall returns an immediately completed awaitable."""
    client = AsyncMySQLClient(mysql_server.dsn)
    try:
        cur = client.cursor()
        await cur.execute("SELECT 2 AS a, 'x' AS b")
        step = cur.fetchall().__await__()
        with pytest.raises(StopIteration) as exc_info:
            next(step)
        assert exc_info.value.value == [(2, "x")]
    finally:
        await client.close()


@pytest.mark.parametrize("method", ["fetchone", "fetchmany", "close"])
def test_async_cursor_ready_methods_complete_without_loop(
    mysql_server: MySQLWireTestServer,
    method: str,
) -> None:
    """Verify AsyncMySQLCursor fetchone, fetchmany, and close complete synchronously without a running loop."""
    client = AsyncMySQLClient(mysql_server.dsn)
    cur = client.cursor()

    async def _execute() -> None:
        await cur.execute("SELECT 2 AS a, 'x' AS b")

    async def _close_client() -> None:
        await client.close()

    try:
        asyncio.run(_execute())
        step = getattr(cur, method)().__await__()
        with pytest.raises(StopIteration) as exc_info:
            next(step)
        expected = {
            "fetchone": (2, "x"),
            "fetchmany": [(2, "x")],
            "close": None,
        }[method]
        assert exc_info.value.value == expected
    finally:
        asyncio.run(_close_client())


@pytest.mark.anyio
async def test_async_cursor_state_errors_raise_at_call(mysql_server: MySQLWireTestServer) -> None:
    """Verify AsyncMySQLCursor state errors raise immediately at the call site without awaiting."""
    client = AsyncMySQLClient(mysql_server.dsn)
    try:
        cur = client.cursor()
        with pytest.raises(ProgrammingError):
            cur.fetchall()

        await cur.execute("SELECT 1 AS a")
        with pytest.raises(ProgrammingError):
            cur.fetchmany(-1)

        await cur.close()
        with pytest.raises(InterfaceError):
            cur.fetchone()
    finally:
        await client.close()


@pytest.mark.anyio
@pytest.mark.parametrize("params", [(), [], {}])
async def test_async_cursor_empty_container_matches_no_parameters(
    mysql_server: MySQLWireTestServer,
    params: tuple[()] | list[Any] | dict[str, Any],
) -> None:
    """Verify empty exact parameter containers bind identically to omitted parameters."""
    client = AsyncMySQLClient(mysql_server.dsn)
    try:
        cur = client.cursor()
        await cur.execute("SELECT 1 AS a", params)
        assert await cur.fetchall() == [(1,)]
    finally:
        await client.close()


@pytest.mark.anyio
async def test_async_cursor_lazy_begin_and_lease_lifecycle(mysql_server: MySQLWireTestServer) -> None:
    """Verify AsyncMySQLClient lazy_begin cursor transactions and lease attachment/release."""
    client = AsyncMySQLClient(mysql_server.dsn)
    try:
        default_cur = client.cursor()
        await default_cur.execute("CREATE TEMPORARY TABLE alb_t (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
        assert client.in_transaction is False
        await default_cur.execute("INSERT INTO alb_t (v) VALUES (?)", (1,))
        assert client.in_transaction is False
        await default_cur.close()

        tx_cur = client.cursor(lazy_begin=True)
        await tx_cur.execute("INSERT INTO alb_t (v) VALUES (?)", (2,))
        assert client.in_transaction is True
        await client.rollback()
        assert client.in_transaction is False

        check_cur = client.cursor()
        await check_cur.execute("SELECT v FROM alb_t ORDER BY id")
        assert await check_cur.fetchall() == [(1,)]
        await check_cur.close()

        await tx_cur.execute("INSERT INTO alb_t (v) VALUES (?)", (3,))
        assert client.in_transaction is True
        await client.commit()
        assert client.in_transaction is False

        await tx_cur.execute("INSERT INTO alb_t (v) VALUES (?)", (4,))
        assert client.in_transaction is True
        await client.commit()
        assert client.in_transaction is False

        await tx_cur.execute("SELECT v FROM alb_t ORDER BY id")
        assert await tx_cur.fetchall() == [(1,), (3,), (4,)]
        await client.rollback()

        calls: list[str] = []
        client.attach_lease(lambda: calls.append("first"))
        client.release_lease()
        client.release_lease()
        assert calls == ["first"]

        client.attach_lease(lambda: calls.append("second"))
        client.attach_lease(lambda: calls.append("third"))
        assert calls == ["first", "second"]

        await client.close()
        assert calls == ["first", "second", "third"]

        with pytest.raises(ConnectionFailure):
            await tx_cur.execute("SELECT 1")
        assert tx_cur.description is None
        assert tx_cur.rowcount == -1
        assert tx_cur.lastrowid is None
    finally:
        await client.close()

    drop_calls: list[int] = []
    ephemeral = AsyncMySQLClient(mysql_server.dsn)
    ephemeral.attach_lease(lambda: drop_calls.append(1))
    del ephemeral
    gc.collect()
    assert drop_calls == [1]


class _MySQLSchemaRow(msgspec.Struct, frozen=True):
    a: int
    b: str = ""


class _RecordingLease:
    def __init__(self, broker: _RecordingBroker | _AsyncRecordingBroker) -> None:
        self._broker = broker

    def release(self) -> None:
        self._broker.release_calls += 1
        self._broker.events.append("release")


class _RecordingBroker:
    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.acquire_calls = 0
        self.release_calls = 0
        self.events: list[str] = []

    def acquire(self, pid: int, *, timeout_ms: int = 5000) -> _RecordingLease:
        _ = (pid, timeout_ms)
        self.acquire_calls += 1
        self.events.append("acquire")
        if self.refuse:
            msg = "broker lease refused"
            raise RuntimeError(msg)
        return _RecordingLease(self)


class _AsyncRecordingBroker:
    def __init__(self, *, refuse: bool = False) -> None:
        self.refuse = refuse
        self.acquire_calls = 0
        self.release_calls = 0
        self.events: list[str] = []

    async def acquire(self, pid: int, *, timeout_ms: int = 5000) -> _RecordingLease:
        _ = (pid, timeout_ms)
        await asyncio.sleep(0)
        self.acquire_calls += 1
        self.events.append("acquire")
        if self.refuse:
            msg = "broker lease refused"
            raise RuntimeError(msg)
        return _RecordingLease(self)


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysql_driver_native_cursor_query_matrix(
    mysql_server: MySQLWireTestServer,
    mode: str,
) -> None:
    """Verify SyncMySQLDriver and AsyncMySQLDriver execute all query methods through the native cursor."""
    cfg = MySQLConfig.from_url(mysql_server.dsn)
    if mode == "sync":
        driver = SyncMySQLDriver(cfg)
        try:
            cur = driver.cursor()
            assert isinstance(cur, SyncMySQLCursor)
            cur.close()

            lazy_cur = driver.cursor(lazy_begin=True)
            assert isinstance(lazy_cur, SyncMySQLCursor)
            lazy_cur.close()

            assert driver.select("SELECT 1 AS a") == [{"a": 1}]
            assert driver.select("SELECT 1 AS a", schema_type=_MySQLSchemaRow) == [_MySQLSchemaRow(a=1, b="")]
            assert driver.select("SELECT :a AS a", {"a": 5}) == [{"a": 5}]

            driver.execute("CREATE TABLE t13_items (id INT AUTO_INCREMENT PRIMARY KEY, label VARCHAR(64) NOT NULL)")
            assert driver.execute("INSERT INTO t13_items (label) VALUES (?)", ("first",)) == 1
            first_id = driver.lastrowid
            assert first_id is not None and first_id >= 1

            assert driver.execute("INSERT INTO t13_items (label) VALUES (?)", ("second",)) == 1
            second_id = driver.lastrowid
            assert second_id is not None and second_id > first_id

            assert driver.execute("UPDATE t13_items SET label = ?", ("updated",)) == 2
            assert driver.execute_with_description("SELECT id AS a, label AS b FROM t13_items ORDER BY id") == (
                [(first_id, "updated"), (second_id, "updated")],
                ("a", "b"),
            )
            assert driver.execute_tuples("SELECT id AS a, label AS b FROM t13_items ORDER BY id") == [
                (first_id, "updated"),
                (second_id, "updated"),
            ]
            assert driver.select(
                "SELECT id AS a, label AS b FROM t13_items ORDER BY id",
                schema_type=_MySQLSchemaRow,
            ) == [
                _MySQLSchemaRow(a=first_id, b="updated"),
                _MySQLSchemaRow(a=second_id, b="updated"),
            ]
            assert driver.execute_with_description(
                "SELECT id AS a, label AS b FROM t13_items WHERE id = ?", (999,)
            ) == (
                [],
                ("a", "b"),
            )
            assert (
                driver.execute_many(
                    "INSERT INTO t13_items (label) VALUES (?)",
                    [("m1",), ("m2",), ("m3",)],
                )
                == 3
            )

            assert driver.select_one("SELECT id AS a, label AS b FROM t13_items ORDER BY id LIMIT 1") == {
                "a": first_id,
                "b": "updated",
            }
            assert driver.select_one(
                "SELECT id AS a, label AS b FROM t13_items ORDER BY id LIMIT 1",
                schema_type=_MySQLSchemaRow,
            ) == _MySQLSchemaRow(a=first_id, b="updated")
            assert driver.select_one_or_none("SELECT id FROM t13_items WHERE id = ?", (999,)) is None
            assert len(driver.select_many("SELECT id, label FROM t13_items ORDER BY id", size=2)) == 2
            assert driver.select_value("SELECT COUNT(*) FROM t13_items", value_type=int) == 5
            assert driver.select_value_or_none("SELECT id FROM t13_items WHERE id = ?", (999,)) is None

            limited_rows, total = driver.select_with_total("SELECT id, label FROM t13_items ORDER BY id LIMIT 2")
            assert len(limited_rows) == 2
            assert total == 5

            with pytest.raises(IntegrityError):
                driver.execute("INSERT INTO t13_items (id, label) VALUES (?, ?)", (first_id, "dup"))
        finally:
            driver.close()
    else:
        async_driver = AsyncMySQLDriver(cfg)
        try:
            async_cur = async_driver.cursor()
            assert isinstance(async_cur, AsyncMySQLCursor)
            await async_cur.close()

            async_lazy_cur = async_driver.cursor(lazy_begin=True)
            assert isinstance(async_lazy_cur, AsyncMySQLCursor)
            await async_lazy_cur.close()

            assert await async_driver.select("SELECT 1 AS a") == [{"a": 1}]
            assert await async_driver.select("SELECT 1 AS a", schema_type=_MySQLSchemaRow) == [
                _MySQLSchemaRow(a=1, b="")
            ]
            assert await async_driver.select("SELECT :a AS a", {"a": 5}) == [{"a": 5}]

            await async_driver.execute(
                "CREATE TABLE t13_items (id INT AUTO_INCREMENT PRIMARY KEY, label VARCHAR(64) NOT NULL)"
            )
            assert await async_driver.execute("INSERT INTO t13_items (label) VALUES (?)", ("first",)) == 1
            first_id = async_driver.lastrowid
            assert first_id is not None and first_id >= 1

            assert await async_driver.execute("INSERT INTO t13_items (label) VALUES (?)", ("second",)) == 1
            second_id = async_driver.lastrowid
            assert second_id is not None and second_id > first_id

            assert await async_driver.execute("UPDATE t13_items SET label = ?", ("updated",)) == 2
            assert await async_driver.execute_with_description(
                "SELECT id AS a, label AS b FROM t13_items ORDER BY id"
            ) == ([(first_id, "updated"), (second_id, "updated")], ("a", "b"))
            assert await async_driver.execute_tuples("SELECT id AS a, label AS b FROM t13_items ORDER BY id") == [
                (first_id, "updated"),
                (second_id, "updated"),
            ]
            assert await async_driver.select(
                "SELECT id AS a, label AS b FROM t13_items ORDER BY id",
                schema_type=_MySQLSchemaRow,
            ) == [
                _MySQLSchemaRow(a=first_id, b="updated"),
                _MySQLSchemaRow(a=second_id, b="updated"),
            ]
            assert await async_driver.execute_with_description(
                "SELECT id AS a, label AS b FROM t13_items WHERE id = ?",
                (999,),
            ) == ([], ("a", "b"))
            assert (
                await async_driver.execute_many(
                    "INSERT INTO t13_items (label) VALUES (?)",
                    [("m1",), ("m2",), ("m3",)],
                )
                == 3
            )

            assert await async_driver.select_one("SELECT id AS a, label AS b FROM t13_items ORDER BY id LIMIT 1") == {
                "a": first_id,
                "b": "updated",
            }
            assert await async_driver.select_one(
                "SELECT id AS a, label AS b FROM t13_items ORDER BY id LIMIT 1",
                schema_type=_MySQLSchemaRow,
            ) == _MySQLSchemaRow(a=first_id, b="updated")
            assert await async_driver.select_one_or_none("SELECT id FROM t13_items WHERE id = ?", (999,)) is None
            assert len(await async_driver.select_many("SELECT id, label FROM t13_items ORDER BY id", size=2)) == 2
            assert await async_driver.select_value("SELECT COUNT(*) FROM t13_items", value_type=int) == 5
            assert await async_driver.select_value_or_none("SELECT id FROM t13_items WHERE id = ?", (999,)) is None

            limited_rows, total = await async_driver.select_with_total(
                "SELECT id, label FROM t13_items ORDER BY id LIMIT 2"
            )
            assert len(limited_rows) == 2
            assert total == 5

            with pytest.raises(IntegrityError):
                await async_driver.execute("INSERT INTO t13_items (id, label) VALUES (?, ?)", (first_id, "dup"))
        finally:
            await async_driver.close()


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysql_driver_broker_lease_lifecycle(
    mysql_server: MySQLWireTestServer,
    mode: str,
) -> None:
    """Verify broker leases are acquired once per client, released on close or drop, and skipped when refused or absent."""
    cfg = MySQLConfig.from_url(mysql_server.dsn)
    unused_broker = _RecordingBroker()

    if mode == "sync":
        no_broker_drv = SyncMySQLDriver(cfg, broker=None)
        try:
            for i in range(10):
                assert no_broker_drv.select("SELECT :a AS a", {"a": i}) == [{"a": i}]
        finally:
            no_broker_drv.close()
        assert unused_broker.acquire_calls == 0
        assert unused_broker.release_calls == 0

        broker = _RecordingBroker()
        drv = SyncMySQLDriver(cfg, broker=broker)
        try:
            for i in range(10):
                assert drv.select("SELECT :a AS a", {"a": i}) == [{"a": i}]
            assert broker.acquire_calls == 1
            assert broker.release_calls == 0
        finally:
            drv.close()
        assert broker.acquire_calls == 1
        assert broker.release_calls == 1

        refused_broker = _RecordingBroker(refuse=True)
        refused_drv = SyncMySQLDriver(cfg, broker=refused_broker)
        try:
            refused_drv.connect()
            assert refused_drv.select("SELECT 1 AS a") == [{"a": 1}]
            assert refused_broker.acquire_calls == 1
            assert refused_broker.release_calls == 0
        finally:
            refused_drv.close()
        assert refused_broker.release_calls == 0

        drop_broker = _RecordingBroker()
        ephemeral = SyncMySQLDriver(cfg, broker=drop_broker)
        ephemeral.connect()
        assert ephemeral.select("SELECT 1 AS a") == [{"a": 1}]
        assert drop_broker.acquire_calls == 1
        assert drop_broker.release_calls == 0
        del ephemeral
        gc.collect()
        assert drop_broker.release_calls == 1
    else:
        no_broker_async = AsyncMySQLDriver(cfg, broker=None)
        try:
            for i in range(10):
                assert await no_broker_async.select("SELECT :a AS a", {"a": i}) == [{"a": i}]
        finally:
            await no_broker_async.close()
        assert unused_broker.acquire_calls == 0
        assert unused_broker.release_calls == 0

        async_broker = _AsyncRecordingBroker()
        async_drv = AsyncMySQLDriver(cfg, broker=async_broker)
        try:
            for i in range(10):
                assert await async_drv.select("SELECT :a AS a", {"a": i}) == [{"a": i}]
            assert async_broker.acquire_calls == 1
            assert async_broker.release_calls == 0
        finally:
            await async_drv.close()
        assert async_broker.acquire_calls == 1
        assert async_broker.release_calls == 1

        refused_async_broker = _AsyncRecordingBroker(refuse=True)
        refused_async_drv = AsyncMySQLDriver(cfg, broker=refused_async_broker)
        try:
            await refused_async_drv.connect()
            assert await refused_async_drv.select("SELECT 1 AS a") == [{"a": 1}]
            assert refused_async_broker.acquire_calls == 1
            assert refused_async_broker.release_calls == 0
        finally:
            await refused_async_drv.close()
        assert refused_async_broker.release_calls == 0

        drop_async_broker = _AsyncRecordingBroker()
        ephemeral_async = AsyncMySQLDriver(cfg, broker=drop_async_broker)
        await ephemeral_async.connect()
        assert await ephemeral_async.select("SELECT 1 AS a") == [{"a": 1}]
        assert drop_async_broker.acquire_calls == 1
        assert drop_async_broker.release_calls == 0
        del ephemeral_async
        gc.collect()
        assert drop_async_broker.release_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_mysql_driver_lease_across_auth_refresh(mode: str) -> None:
    """Verify broker lease is released and re-acquired around an authentication refresh retry."""
    issued: list[str] = []

    def _sync_mint(*, now_ts: float) -> str:
        token = f"test-token-placeholder-{len(issued) + 1}-{int(now_ts)}"
        issued.append(token)
        return token

    async def _async_mint(*, now_ts: float) -> str:
        await asyncio.sleep(0)
        return _sync_mint(now_ts=now_ts)

    provider = AwsRdsIamTokenProvider(
        endpoint_host="127.0.0.1",
        port=3306,
        region="us-east-1",
        username="iam_user",
        refresh_margin_seconds=60.0,
        token_ttl_seconds=900,
        clock=lambda: 1000.0,
        signer=_async_mint if mode == "async" else _sync_mint,
    )

    nonce = b"12345678901234567890"
    stop_event = threading.Event()

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 0))
        srv.listen(16)
        srv.settimeout(0.2)
        port = srv.getsockname()[1]

        def _handle_conn(conn: socket.socket, conn_idx: int) -> None:
            with conn:
                conn.sendall(_mysql_packet(0, _mysql_handshake_v10(conn_idx + 1, nonce)))
                try:
                    seq, _payload = _read_mysql_packet(conn)
                except EOFError:
                    return
                if conn_idx == 0:
                    conn.sendall(_mysql_packet(seq + 1, _mysql_err_packet(1045, "28000", "Access denied for user")))
                    return
                conn.sendall(_mysql_packet(seq + 1, _mysql_ok_packet()))
                while True:
                    try:
                        cmd_seq, cmd_payload = _read_mysql_packet(conn)
                    except EOFError:
                        break
                    if not cmd_payload or cmd_payload[0] == 0x01:
                        break
                    if cmd_payload[0] == 0x16:
                        prep_ok = b"\x00" + struct.pack("<IHHBH", 1, 0, 0, 0, 0)
                        conn.sendall(_mysql_packet(cmd_seq + 1, prep_ok))
                    else:
                        conn.sendall(_mysql_packet(cmd_seq + 1, _mysql_ok_packet()))

        def _server() -> None:
            idx = 0
            while not stop_event.is_set():
                try:
                    conn, _ = srv.accept()
                except TimeoutError:
                    continue
                except OSError:
                    break
                t = threading.Thread(target=_handle_conn, args=(conn, idx), daemon=True)
                t.start()
                idx += 1

        srv_thread = threading.Thread(target=_server, daemon=True)
        srv_thread.start()

        cfg = MySQLConfig(
            dsn=f"mysql://iam_user@127.0.0.1:{port}/appdb?ssl_mode=disable",
            token_provider=provider,
        )
        try:
            if mode == "sync":
                broker = _RecordingBroker()
                drv = SyncMySQLDriver(cfg, broker=broker)
                try:
                    assert drv.execute("DO 1") == 0
                    assert broker.events == ["acquire", "release", "acquire"]
                finally:
                    drv.close()
                assert broker.events == ["acquire", "release", "acquire", "release"]
            else:
                async_broker = _AsyncRecordingBroker()
                async_drv = AsyncMySQLDriver(cfg, broker=async_broker)
                try:
                    assert await async_drv.execute("DO 1") == 0
                    assert async_broker.events == ["acquire", "release", "acquire"]
                finally:
                    await async_drv.close()
                assert async_broker.events == ["acquire", "release", "acquire", "release"]
        finally:
            stop_event.set()
            srv_thread.join(timeout=2.0)
