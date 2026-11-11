"""Unit tests for native MySQL sync/async drivers, Cloud SQL dialer, pipelines, Arrow SHM, and mysql+vtb dialect."""

from __future__ import annotations

import datetime
import decimal
import struct
import sys
import uuid
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

import vertebra.db.adapters.mysql.driver as mysql_driver_mod
from vertebra._mysql import (
    AsyncMySQLClient,
    MySQLWireTestServer,
    SyncMySQLClient,
    map_mysql_error,
)
from vertebra.db import (
    ArrowCursor,
    AsyncPipelineCapable,
    ConnectionFailure,
    DatabaseError,
    DataError,
    IntegrityError,
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
    MySQLPoolConfig,
    SyncMySQLDriver,
    SyncMySQLPipeline,
    build_mysql_connect_kwargs,
    compile_mysql_statement,
    map_mysql_exception,
)
from vertebra.db.dialer.cloudsql import AsyncCloudSQLDialer, CloudSQLConfig, SyncCloudSQLDialer
from vertebra.extensions.sqlalchemy import (
    SyncDBAPIConnection,
    SyncMySQLDialect,
    Vector,
)
from vertebra.typing import MissingDependencyError, reset_dependency_cache


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
def test_compile_mysql_statement_preserves_literals_comments_and_backticks(
    sql: str,
    params: Any,
    expected_sql: str,
    expected_params: tuple[Any, ...],
) -> None:
    """Verify compile_mysql_statement rewrites placeholders outside string literals, comments, and backticks."""
    compiled_sql, bound = compile_mysql_statement(sql, params, target_style="qmark")
    assert compiled_sql == expected_sql
    assert bound == expected_params


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


def test_mysql_missing_extension_raises_missing_dependency_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify SyncMySQLDriver and AsyncMySQLDriver raise MissingDependencyError when vertebra._mysql is absent."""
    monkeypatch.setattr(mysql_driver_mod, "_mysql_ext", None)
    monkeypatch.setitem(sys.modules, "vertebra._mysql", None)
    reset_dependency_cache("vertebra._mysql")
    try:
        driver = SyncMySQLDriver(MySQLConfig(dsn="mysql://localhost:3306/app"))
        with pytest.raises(MissingDependencyError, match="vertebra-core\\[mysql\\]"):
            driver.select("SELECT 1")
    finally:
        reset_dependency_cache("vertebra._mysql")


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

    assert SyncMySQLDialect.paramstyle == "format"
    assert SyncMySQLDialect.use_insertmanyvalues is True
    assert SyncMySQLDialect.insert_executemany_returning is False
    assert SyncMySQLDialect.supports_native_jsonb is False

    vec = Vector(512)
    assert vec.get_col_spec("mysql+vtb") == "VECTOR(512)"
    dist_expr = vec.cosine_distance([0.1, 0.2], param_name="q")
    assert dist_expr.compile_sql("mysql+vtb") == "DISTANCE_COSINE(embedding, :q)"
    assert SyncMySQLDialect.type_descriptor(vec) is vec
    assert SyncMySQLClient is not None
    assert AsyncMySQLClient is not None
