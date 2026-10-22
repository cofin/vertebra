"""Unit tests for Oracle sync/async drivers, OracleWallet mTLS, DBMS_CLOUD staging, and RRF execution."""

from __future__ import annotations

import asyncio
import io
import zipfile
from typing import Any

import pytest

from vertebra._typing import MissingDependencyError
from vertebra.db.oracle import (
    AsyncOracleDriver,
    OracleConfig,
    OracleContainer,
    OracleDataFrame,
    OracleWallet,
    SyncOracleDriver,
)
from vertebra.extensions.sqlalchemy import (
    AlembicImpl,
    CreateVectorIndexOp,
    DBAPIConnection,
    DropVectorIndexOp,
    InsertSentinelSpec,
    OracleVtbDialect,
)
from vertebra.extensions.sqlspec import OracleHybridSearch
from vertebra.storage import ObjectStore


class FakeOracleCursor:
    """Mock Oracle database cursor."""

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or [(101, "Espresso Roast", 0.95)]
        self.description = (
            ("ID", 2, None, None, None, None, None),
            ("NAME", 1, None, None, None, None, None),
            ("RRF_SCORE", 2, None, None, None, None, None),
        )
        self.executed_statements: list[tuple[str, Any]] = []
        self.closed = False

    def __enter__(self) -> FakeOracleCursor:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)
        self.close()

    def execute(self, sql: str, params: Any = None) -> None:
        """Record executed SQL statement and bind parameters."""
        self.executed_statements.append((sql, params))

    def fetchall(self) -> list[tuple[Any, ...]]:
        """Return canned tabular rows."""
        return list(self._rows)

    def close(self) -> None:
        """Mark cursor closed."""
        self.closed = True


class FakeOracleConnection:
    """Mock synchronous Oracle connection."""

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.cursor_instance = FakeOracleCursor(rows)
        self.committed = False
        self.rolled_back = False
        self.closed = False

    def cursor(self) -> FakeOracleCursor:
        """Return the mock cursor."""
        return self.cursor_instance

    def commit(self) -> None:
        """Record commit."""
        self.committed = True

    def rollback(self) -> None:
        """Record rollback."""
        self.rolled_back = True

    def close(self) -> None:
        """Record close."""
        self.closed = True


class FakeOracleConnectionContext:
    """Context manager for acquiring mock Oracle connection."""

    def __init__(self, conn: FakeOracleConnection) -> None:
        self._conn = conn

    def __enter__(self) -> FakeOracleConnection:
        return self._conn

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)


class FakeOraclePool:
    """Mock oracledb connection pool."""

    def __init__(self, conn: FakeOracleConnection | None = None) -> None:
        self.conn = conn or FakeOracleConnection()
        self.closed = False

    def acquire(self) -> FakeOracleConnectionContext:
        """Return an active connection context."""
        return FakeOracleConnectionContext(self.conn)

    def commit(self) -> None:
        """Commit through connection."""
        self.conn.commit()

    def rollback(self) -> None:
        """Rollback through connection."""
        self.conn.rollback()

    def close(self) -> None:
        """Mark pool closed."""
        self.closed = True
        self.conn.close()


class FakeAsyncOracleCursor:
    """Mock asynchronous Oracle cursor."""

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows or [(202, "Cold Brew", 0.88)]
        self.description = (
            ("ID", 2, None, None, None, None, None),
            ("NAME", 1, None, None, None, None, None),
            ("RRF_SCORE", 2, None, None, None, None, None),
        )
        self.executed_statements: list[tuple[str, Any]] = []

    async def __aenter__(self) -> FakeAsyncOracleCursor:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)

    async def execute(self, sql: str, params: Any = None) -> None:
        """Record async SQL execution."""
        self.executed_statements.append((sql, params))

    async def fetchall(self) -> list[tuple[Any, ...]]:
        """Return canned tabular rows."""
        return list(self._rows)


class FakeAsyncOracleConnection:
    """Mock asynchronous Oracle connection context."""

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.cur = FakeAsyncOracleCursor(rows)

    async def __aenter__(self) -> FakeAsyncOracleConnection:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        _ = (exc_type, exc_val, exc_tb)

    def cursor(self) -> FakeAsyncOracleCursor:
        """Return cursor."""
        return self.cur


class FakeAsyncOraclePool:
    """Mock asynchronous oracledb pool."""

    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self._rows = rows
        self.closed = False

    def acquire(self) -> FakeAsyncOracleConnection:
        """Acquire mock async connection."""
        return FakeAsyncOracleConnection(self._rows)

    def close(self) -> None:
        """Mark pool closed."""
        self.closed = True


def test_oracle_dataframe_strict_zip_validation() -> None:
    """Verify OracleDataFrame.to_dicts raises ValueError on row length mismatch."""
    df = OracleDataFrame(("id", "name"), [(1, "alpha"), (2, "beta")])
    dicts = df.to_dicts()
    assert dicts == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]

    mismatched = OracleDataFrame(("id", "name"), [(1, "alpha", "extra")])
    with pytest.raises(ValueError):
        mismatched.to_dicts()


def test_oracle_container_admin_password_validation() -> None:
    """Verify OracleContainer requires admin_password before generating env vars."""
    container = OracleContainer()
    assert container.admin_password == ""
    with pytest.raises(ValueError, match="OracleContainer.admin_password must be set"):
        container.env_vars()

    configured = OracleContainer(admin_password="SecretPass123!")
    env = configured.env_vars()
    assert env["ADMIN_PASSWORD"] == "SecretPass123!"
    assert env["WALLET_PASSWORD"] == "SecretPass123!"


def test_oracle_wallet_sqlnet_parsing_and_zip_slip_refusal(tmp_path: Any) -> None:
    """Verify OracleWallet parses sqlnet.ora and rejects zip-slip path traversal."""
    sqlnet_body = (
        "# Oracle Net Configuration\n"
        "WALLET_LOCATION = (SOURCE=(METHOD=FILE)(METHOD_DATA=(DIRECTORY=/opt/wallet)))\n"
        "SSL_SERVER_DN_MATCH = ON\n"
        "SSL_VERSION = 1.2\n"
    )
    tns_body = "MYDB_HIGH = (DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=db.oracle)(PORT=1522))(CONNECT_DATA=(SERVICE_NAME=mydb_high)))"

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("sqlnet.ora", sqlnet_body)
        zf.writestr("tnsnames.ora", tns_body)
    wallet_zip_bytes = buf.getvalue()

    wallet = OracleWallet.extract(wallet_zip_bytes, target_dir=tmp_path / "wallets")
    assert wallet.sqlnet_params["SSL_SERVER_DN_MATCH"] == "ON"
    assert wallet.sqlnet_params["SSL_VERSION"] == "1.2"
    assert "MYDB_HIGH" in wallet.aliases

    ssl_ctx = wallet.build_ssl_context(ssl_check_hostname=True)
    assert ssl_ctx is not None

    with pytest.raises(TypeError):
        legacy_kwargs: dict[str, Any] = {"cache_root": tmp_path / "legacy"}
        OracleWallet.extract(wallet_zip_bytes, **legacy_kwargs)

    evil_buf = io.BytesIO()
    with zipfile.ZipFile(evil_buf, "w") as zf:
        zf.writestr("../../etc/passwd", "root:x:0:0::/root:/bin/bash\n")
    evil_bytes = evil_buf.getvalue()

    with pytest.raises(ValueError, match="Rejected zip member outside target wallet directory"):
        OracleWallet.extract(evil_bytes, target_dir=tmp_path / "evil")


def test_oracle_hybrid_search_parameterized_rrf_and_execution() -> None:
    """Verify OracleHybridSearch emits parameterized bind placeholders and executes live RRF queries."""
    sql, binds = OracleHybridSearch.build_rrf_sql(
        "products",
        select_columns=("id", "name"),
        rrf_k=60,
        candidate_limit=25,
        query_vec=[0.1, 0.2, 0.3],
        text_query="espresso",
        top_k=5,
    )
    assert "FETCH FIRST :candidate_limit ROWS ONLY" in sql
    assert "COALESCE(1.0 / (:rrf_k + v.vec_rank), 0.0)" in sql
    assert "COALESCE(1.0 / (:rrf_k + t.text_rank), 0.0)" in sql
    assert ":candidate_limit" in sql
    assert ":rrf_k" in sql
    assert binds["candidate_limit"] == 25
    assert binds["rrf_k"] == 60
    assert binds["top_k"] == 5

    fake_conn = FakeOracleConnection([(1, "Espresso Roast", 0.95), (2, "Dark Roast", 0.85)])
    pool = FakeOraclePool(fake_conn)
    driver = SyncOracleDriver(OracleConfig(), pool=pool)

    results = OracleHybridSearch.execute_rrf(
        driver,
        "products",
        query_vec=[0.1, 0.2, 0.3],
        text_query="espresso",
        candidate_limit=20,
        rrf_k=60,
    )
    assert len(results) == 2
    assert results[0]["NAME"] == "Espresso Roast"
    assert results[0]["RRF_SCORE"] == 0.95


@pytest.mark.anyio
async def test_async_oracle_driver_and_rrf_execution() -> None:
    """Verify AsyncOracleDriver executes queries and RRF searches asynchronously."""
    async_pool = FakeAsyncOraclePool([(10, "Ethiopian Yirgacheffe", 0.98)])
    driver = AsyncOracleDriver(OracleConfig(), pool=async_pool)

    rows, desc = await driver.execute_with_description("SELECT ID, NAME FROM products")
    assert rows == [(10, "Ethiopian Yirgacheffe", 0.98)]
    assert desc is not None
    assert desc[0][0] == "ID"

    results = await OracleHybridSearch.execute_rrf_async(
        driver,
        "products",
        query_vec=[0.5, 0.5],
        text_query="yirgacheffe",
    )
    assert len(results) == 1
    assert results[0]["NAME"] == "Ethiopian Yirgacheffe"
    assert results[0]["RRF_SCORE"] == 0.98

    await driver.close()
    assert async_pool.closed is True


def test_oracle_dbms_cloud_staging_execution() -> None:
    """Verify SyncOracleDriver.stage_to_dbms_cloud executes DBMS_CLOUD.COPY_DATA."""
    fake_conn = FakeOracleConnection([])
    pool = FakeOraclePool(fake_conn)
    driver = SyncOracleDriver(OracleConfig(), pool=pool)
    store = ObjectStore("memory://data_lake")

    driver.stage_to_dbms_cloud(
        store,
        "telemetry/metrics.parquet",
        "raw_telemetry",
        credential_name="MY_OCI_CRED",
    )
    assert len(fake_conn.cursor_instance.executed_statements) == 1
    stmt, binds = fake_conn.cursor_instance.executed_statements[0]
    assert "DBMS_CLOUD.COPY_DATA" in stmt
    assert binds["table_name"] == "raw_telemetry"
    assert binds["credential_name"] == "MY_OCI_CRED"
    assert "telemetry/metrics.parquet" in binds["file_uri_list"]
    assert binds["format"] == '{"type": "parquet"}'


def test_oracle_missing_dependency_refusal() -> None:
    """Verify unmocked SyncOracleDriver and AsyncOracleDriver raise MissingDependencyError when oracledb is absent."""
    driver = SyncOracleDriver(OracleConfig())
    with pytest.raises(MissingDependencyError) as exc_info:
        driver.execute_tuples("SELECT 1 FROM DUAL")
    assert "oracledb" in str(exc_info.value)

    async_driver = AsyncOracleDriver(OracleConfig())
    with pytest.raises(MissingDependencyError) as async_exc_info:
        asyncio.run(async_driver.execute_tuples("SELECT 1 FROM DUAL"))
    assert "oracledb" in str(async_exc_info.value)


def test_oracle_vtb_dialect_dbapi_and_alembic_ddl_ops() -> None:
    """Verify OracleVtbDialect, DBAPIConnection routing, and Alembic vector index DDL compilation."""
    mock_conn = FakeOracleConnection([(42, "item42")])
    pool = FakeOraclePool(mock_conn)
    driver = SyncOracleDriver(OracleConfig(), pool=pool)

    conn = DBAPIConnection("oracle+vtb://system:secret@localhost:1521/FREEPDB1", driver=driver)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM items WHERE id = :id", {"id": 42})
    rows = cur.fetchall()
    assert rows == [(42, "item42")]

    conn.commit()
    assert mock_conn.committed is True
    conn.rollback()
    assert mock_conn.rolled_back is True
    assert OracleVtbDialect.paramstyle == "named"
    sentinel_spec = InsertSentinelSpec.create(table_name="orders", sentinel_column="_insert_sentinel")
    rendered_insert = OracleVtbDialect.compile_insert_returning_sentinel(
        "orders",
        ("id", "total"),
        sentinel_spec,
        pk_column="id",
    )
    assert rendered_insert == (
        "INSERT INTO orders (id, total) VALUES (:id, :total) "
        "RETURNING id, _insert_sentinel INTO :ret_id, :ret__insert_sentinel"
    )

    vec_op = CreateVectorIndexOp(
        index_name="idx_items_vec",
        table_name="items",
        column_name="embedding",
        metric="cosine",
    )
    assert vec_op.to_sql("oracle+vtb") == (
        "CREATE VECTOR INDEX idx_items_vec ON items (embedding) ORGANIZATION INMEMORY NEIGHBOR GRAPH DISTANCE COSINE"
    )
    assert vec_op.to_sql("postgresql+vtb") == (
        "CREATE INDEX idx_items_vec ON items USING hnsw (embedding vector_cosine_ops)"
    )

    drop_op = vec_op.reverse()
    assert isinstance(drop_op, DropVectorIndexOp)
    assert drop_op.to_sql("oracle+vtb") == "DROP INDEX idx_items_vec"
    assert drop_op.to_sql("postgresql+vtb") == "DROP INDEX IF EXISTS idx_items_vec"

    ora_url = AlembicImpl.resolve_migration_url(OracleConfig(host="db.internal", port=1521, service_name="FREEPDB1"))
    assert ora_url == "oracle+vtb://system:@db.internal:1521/FREEPDB1"

    AlembicImpl.execute_ddl(conn, vec_op, dialect_name="oracle+vtb")
    executed_sql = mock_conn.cursor_instance.executed_statements[-1][0]
    assert "CREATE VECTOR INDEX idx_items_vec" in executed_sql

    conn.close()
    assert mock_conn.closed is True
