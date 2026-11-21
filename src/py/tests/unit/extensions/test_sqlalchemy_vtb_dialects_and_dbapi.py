"""Unit and integration tests for SQLAlchemy 2.1 +vtb dialects, types, Alembic, and PEP 249 DBAPI."""

from __future__ import annotations

import array
import importlib.util
import subprocess
import sys
import warnings
from pathlib import Path
from typing import Any, cast

import pytest
from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.repository import SQLAlchemyAsyncRepository
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService
from advanced_alchemy.types import ORA_JSONB, JsonB
from advanced_alchemy.types.file_object import FileObject, StoredObject
from alembic.autogenerate import produce_migrations, render_python_code
from alembic.ddl.impl import DefaultImpl
from alembic.operations import Operations
from alembic.operations.ops import ModifyTableOps, UpgradeOps
from alembic.runtime.migration import MigrationContext as AlembicMigrationContext
from sqlalchemy import JSON as SAJSON
from sqlalchemy import Column, Integer, MetaData, String, Table, create_engine, delete, insert, select, text, update
from sqlalchemy import Uuid as SAUuid
from sqlalchemy import exc as sa_exc
from sqlalchemy.dialects import oracle as sa_oracle
from sqlalchemy.dialects import postgresql as sa_postgresql
from sqlalchemy.dialects import sqlite as sa_sqlite
from sqlalchemy.engine.url import make_url
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool
from sqlalchemy.schema import CreateTable
from sqlalchemy.util.concurrency import greenlet_spawn

import vertebra._oracle as _oracle_native
import vertebra.extensions.sqlalchemy as sqlalchemy_pkg
import vertebra.extensions.sqlalchemy.types as sqlalchemy_types_mod
from tests.unit.db.oracle_test_doubles import _StubAsyncOracleClient, _StubSyncOracleClient
from tests.unit.db.postgres_ports import FakePostgresCluster, FakeSyncPostgresPool
from tests.unit.extensions.test_sqlspec_vtb_stacks import _NativeBindConnection
from vertebra._sqlite import AsyncSQLiteCursor, SyncSQLiteCursor
from vertebra.db import DatabaseRegistry
from vertebra.db.adapters.mysql import MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.adapters.postgres import PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
from vertebra.db.dbapi import (
    BFILE,
    BLOB,
    CLOB,
    FIXED_CHAR,
    NCLOB,
    NUMBER,
    AsyncDBAPIConnection,
    AsyncDBAPIModule,
    InterfaceError,
    OperationalError,
    ProgrammingError,
    SyncDBAPIConnection,
    SyncDBAPIModule,
)
from vertebra.extensions.alchemy import (
    ObjectStoreBackend,
    create_alchemy_plugin,
    create_async_alchemy_config,
    create_sync_alchemy_config,
)
from vertebra.extensions.broker.transport import BrokerConfig, SyncSHMBrokerTransport
from vertebra.extensions.sqlalchemy import (
    CreateVectorIndexOp,
    CreateViewOp,
    DropVectorIndexOp,
    HalfVector,
    MsgspecJSON,
    OracleVector,
    PgVector,
    SpannerVector,
    SparseVector,
    SyncMySQLDialect,
    SyncOracleDialect,
    SyncPostgreSQLDialect,
    SyncSQLiteDialect,
    Vector,
)
from vertebra.extensions.sqlalchemy.alembic import (
    BigQueryAlembicImpl,
    DuckDBAlembicImpl,
    SpangresAlembicImpl,
    SpannerAlembicImpl,
    render_item,
)
from vertebra.extensions.sqlalchemy.async_adapter import (
    AsyncAdapt_vertebra_connection,
    AsyncAdapt_vertebra_cursor,
    AsyncAdapt_vertebra_ss_cursor,
    SQLAlchemyAsyncDBAPI,
)
from vertebra.extensions.sqlalchemy.dialects import (
    AsyncMSSQLDialect,
    AsyncMySQLDialect,
    AsyncOracleDialect,
    AsyncPostgreSQLDialect,
    AsyncSQLiteDialect,
    SyncMSSQLDialect,
)
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, BrokerPool
from vertebra.storage import SyncObjectStore

_HAS_DUCKDB_ENGINE = importlib.util.find_spec("duckdb_engine") is not None
_HAS_SQLALCHEMY_GCP = (
    importlib.util.find_spec("sqlalchemy_bigquery") is not None
    and importlib.util.find_spec("google.cloud.sqlalchemy_spanner") is not None
)
SyncDuckDBDialect: Any = None
if _HAS_DUCKDB_ENGINE:
    from vertebra.extensions.sqlalchemy.dialects import (
        SyncDuckDBDialect as _SyncDuckDBDialect,
    )

    SyncDuckDBDialect = _SyncDuckDBDialect

AsyncSpangresDialect: Any = None
SyncBigQueryDialect: Any = None
SyncSpangresDialect: Any = None
SyncSpannerDialect: Any = None
if _HAS_SQLALCHEMY_GCP:
    from vertebra.extensions.sqlalchemy.dialects import (
        AsyncSpangresDialect as _AsyncSpangresDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects import (
        SyncBigQueryDialect as _SyncBigQueryDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects import (
        SyncSpangresDialect as _SyncSpangresDialect,
    )
    from vertebra.extensions.sqlalchemy.dialects import (
        SyncSpannerDialect as _SyncSpannerDialect,
    )

    AsyncSpangresDialect = _AsyncSpangresDialect
    SyncBigQueryDialect = _SyncBigQueryDialect
    SyncSpangresDialect = _SyncSpangresDialect
    SyncSpannerDialect = _SyncSpannerDialect


@pytest.mark.parametrize("autocommit", [False, True])
def test_sync_dialect_sqlite_transaction_ownership(autocommit: bool) -> None:
    """SyncDBAPIConnection owns transactions for SQLite dialects and respects commit and rollback."""
    driver = SyncSQLiteDriver()
    driver.execute("CREATE TABLE transaction_items (id INTEGER)")
    conn = SyncDBAPIConnection(driver=driver, share_pool=False, autocommit=autocommit)
    try:
        conn.cursor().execute("INSERT INTO transaction_items VALUES (?)", (1,))
        if autocommit:
            assert driver.select_value("SELECT COUNT(*) FROM transaction_items") == 1
        else:
            conn.rollback()
            assert driver.select_value("SELECT COUNT(*) FROM transaction_items") == 0
    finally:
        conn.close()


@pytest.mark.anyio
async def test_async_dialect_sqlite_transaction_ownership() -> None:
    """AsyncAdapt_vertebra_connection owns transactions through SQLAlchemy's async adapter."""
    driver = AsyncSQLiteDriver()
    await driver.execute("CREATE TABLE transaction_items (id INTEGER)")
    module = SQLAlchemyAsyncDBAPI(dialect="sqlite")
    adapted = await greenlet_spawn(module.connect, driver=driver, share_pool=False)
    try:
        cursor = await greenlet_spawn(adapted.cursor)
        await greenlet_spawn(cursor.execute, "INSERT INTO transaction_items VALUES (?)", (1,))
        await greenlet_spawn(adapted.rollback)
        assert await driver.select_value("SELECT COUNT(*) FROM transaction_items") == 0
    finally:
        await greenlet_spawn(adapted.close)


@pytest.mark.parametrize("autocommit", [False, True])
def test_sync_dbapi_native_dml_transaction_ownership(autocommit: bool) -> None:
    """DBAPI cursors pin MySQL connections and respect commit, rollback, and autocommit."""
    port = _NativeBindConnection()
    port.raw.execute("CREATE TABLE transaction_items (id INTEGER)")
    driver = SyncMySQLDriver(MySQLConfig(dsn="mysql://localhost/db"), client=port)
    conn = SyncDBAPIConnection("mysql+vtb://localhost/db", driver=driver, share_pool=False)
    conn.autocommit = autocommit
    try:
        cursor = conn.cursor()
        cursor.execute("INSERT INTO transaction_items VALUES (?)", (1,))
        conn.commit()
        cursor.executemany("INSERT INTO transaction_items VALUES (?)", [(2,), (3,)])
        conn.rollback()
        assert port.raw.execute("SELECT id FROM transaction_items ORDER BY id").fetchall() == (
            [(1,), (2,), (3,)] if autocommit else [(1,)]
        )
        cursor.execute("INSERT INTO transaction_items VALUES (?) RETURNING id", (4,))
        assert cursor.fetchall() == [(4,)]
        conn.rollback()
        assert port.raw.execute("SELECT COUNT(*) FROM transaction_items WHERE id = 4").fetchone() == (
            (1,) if autocommit else (0,)
        )
    finally:
        conn.close()


@pytest.mark.anyio
@pytest.mark.parametrize("autocommit", [False, True])
async def test_async_dbapi_native_dml_transaction_ownership(autocommit: bool) -> None:
    """Async cursors honor implicit transaction boundaries for single and batched DML."""
    driver = AsyncSQLiteDriver()
    await driver.execute("CREATE TABLE transaction_items (id INTEGER)")
    conn = AsyncDBAPIConnection(driver=driver, share_pool=False)
    conn.autocommit = autocommit
    try:
        cursor = conn.cursor()
        await cursor.execute("INSERT INTO transaction_items VALUES (?)", (1,))
        await conn.commit()
        await cursor.executemany("INSERT INTO transaction_items VALUES (?)", [(2,), (3,)])
        await conn.rollback()
        assert await driver.execute_tuples("SELECT id FROM transaction_items ORDER BY id") == (
            [(1,), (2,), (3,)] if autocommit else [(1,)]
        )
        await cursor.execute("INSERT INTO transaction_items VALUES (?) RETURNING id", (4,))
        assert await cursor.fetchall() == [(4,)]
        await conn.rollback()
        assert await driver.select_value("SELECT COUNT(*) FROM transaction_items WHERE id = 4") == (
            1 if autocommit else 0
        )
    finally:
        await conn.close()


class BaseTestModel(DeclarativeBase):
    """Base declarative class for unit test models."""


class ItemRecord(BaseTestModel):
    """Declarative item model for engine verification."""

    __tablename__ = "test_items"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(64))


class DocumentAuditModel(UUIDAuditBase):
    """Advanced Alchemy UUIDAuditBase model with StoredObject column."""

    __tablename__ = "audit_docs"

    title: Mapped[str] = mapped_column(String(128))
    file: Mapped[FileObject] = mapped_column(StoredObject(backend="docs_store"))


class DocumentRepository(SQLAlchemyAsyncRepository[DocumentAuditModel]):
    """Advanced Alchemy asynchronous repository for DocumentAuditModel."""

    model_type = DocumentAuditModel


class DocumentService(SQLAlchemyAsyncRepositoryService[DocumentAuditModel]):
    """Advanced Alchemy asynchronous service for DocumentAuditModel."""

    repository_type = DocumentRepository


def test_sqlite_vtb_sync_engine_ddl_dml_and_rollback() -> None:
    """Verify create_engine with sqlite+vtb:///:memory: persists schema and executes transactions."""
    engine = create_engine("sqlite+vtb:///:memory:")

    with engine.begin() as conn:
        BaseTestModel.metadata.create_all(conn)

    with engine.connect() as conn:
        conn.execute(insert(ItemRecord).values(id=1, name="first"))
        conn.commit()

    with engine.connect() as conn:
        rows = conn.execute(select(ItemRecord).where(ItemRecord.id == 1)).fetchall()
        assert len(rows) == 1
        assert rows[0]._mapping["name"] == "first"

    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(insert(ItemRecord).values(id=2, name="rolled_back"))
        trans.rollback()

    with engine.connect() as conn:
        rows_after = conn.execute(select(ItemRecord)).fetchall()
        assert len(rows_after) == 1

    with engine.begin() as conn:
        upd_res = conn.execute(update(ItemRecord).where(ItemRecord.id == 1).values(name="updated"))
        assert upd_res.rowcount == 1
        del_res = conn.execute(delete(ItemRecord).where(ItemRecord.id == 1))
        assert del_res.rowcount == 1

    engine.dispose()


@pytest.mark.anyio
async def test_sqlite_vtb_async_engine_ddl_dml_and_rollback() -> None:
    """Verify create_async_engine with sqlite+vtb_async:///:memory: persists across sessions."""
    engine = create_async_engine("sqlite+vtb_async:///:memory:")

    async with engine.begin() as conn:
        await conn.run_sync(BaseTestModel.metadata.create_all)

    async with AsyncSession(engine) as session:
        session.add(ItemRecord(id=10, name="async_item"))
        await session.commit()

    async with AsyncSession(engine) as session:
        result = await session.execute(select(ItemRecord).where(ItemRecord.id == 10))
        item = result.scalar_one_or_none()
        assert item is not None
        assert item.name == "async_item"

    async with AsyncSession(engine) as session:
        session.add(ItemRecord(id=20, name="temp_item"))
        await session.flush()
        await session.rollback()

    async with AsyncSession(engine) as session:
        result_final = await session.execute(select(ItemRecord))
        items = result_final.scalars().all()
        assert len(items) == 1
        assert items[0].id == 10

    async with AsyncSession(engine) as session:
        upd_result = await session.execute(update(ItemRecord).where(ItemRecord.id == 10).values(name="async_updated"))
        assert getattr(upd_result, "rowcount", None) == 1
        del_result = await session.execute(delete(ItemRecord).where(ItemRecord.id == 10))
        assert getattr(del_result, "rowcount", None) == 1
        await session.commit()

    await engine.dispose()


@pytest.mark.anyio
async def test_async_adapter_never_calls_cursor_aenter(monkeypatch: pytest.MonkeyPatch) -> None:
    """AsyncAdapt_vertebra_cursor executes statements without awaiting cursor.__aenter__."""
    orig_cursor = AsyncSQLiteDriver.cursor

    class _NoAenterCursorProxy:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        async def __aenter__(self) -> Any:
            msg = "cursor.__aenter__ must not be called"
            raise AssertionError(msg)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    monkeypatch.setattr(
        AsyncSQLiteDriver,
        "cursor",
        lambda self, *args, **kwargs: _NoAenterCursorProxy(orig_cursor(self, *args, **kwargs)),
    )
    engine = create_async_engine("sqlite+vtb_async:///:memory:")
    try:
        async with engine.connect() as conn:
            row = (await conn.execute(text("SELECT 1"))).one()
            assert row == (1,)
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_async_adapter_transaction_opens_on_first_statement() -> None:
    """Driver reports in_transaction after the first statement and rollback discards uncommitted rows."""
    engine = create_async_engine("sqlite+vtb_async:///:memory:")
    try:
        async with engine.begin() as conn:
            await conn.execute(text("CREATE TABLE tx_items (id INTEGER)"))

        async with engine.connect() as conn:
            raw_conn = await conn.get_raw_connection()
            driver = cast("Any", raw_conn.driver_connection).driver
            await conn.execute(text("INSERT INTO tx_items VALUES (1)"))
            assert driver.in_transaction is True
            await conn.rollback()
            count = (await conn.execute(text("SELECT COUNT(*) FROM tx_items"))).scalar_one()
            assert count == 0
    finally:
        await engine.dispose()


@pytest.mark.anyio
async def test_async_adapter_autocommit_isolation_level_opens_no_transaction(tmp_path: Path) -> None:
    """Autocommit mode leaves driver.in_transaction False and exposes writes immediately."""
    db_path = tmp_path / "autocommit.db"
    url = f"sqlite+vtb_async:///{db_path}"
    engine_a = create_async_engine(url, connect_args={"autocommit": True})
    engine_b = create_async_engine(url)
    try:
        async with engine_a.connect() as conn_a:
            raw_conn = await conn_a.get_raw_connection()
            driver = cast("Any", raw_conn.driver_connection).driver
            await conn_a.execute(text("CREATE TABLE ac_items (id INTEGER)"))
            await conn_a.execute(text("INSERT INTO ac_items VALUES (42)"))
            assert driver.in_transaction is False
            async with engine_b.connect() as conn_b:
                visible = (await conn_b.execute(text("SELECT id FROM ac_items"))).scalars().all()
                assert visible == [42]
    finally:
        await engine_a.dispose()
        await engine_b.dispose()


def test_vector_and_msgspec_json_types_across_dialects() -> None:
    """Verify Vector and MsgspecJSON DDL compilation, bind processing, and Core distance expressions."""
    vec_type = Vector(768, precision="float32", column_name="embedding")
    assert vec_type.get_col_spec("postgresql+vtb") == "VECTOR(768)"
    assert vec_type.get_col_spec("oracle+vtb") == "VECTOR(768, FLOAT32)"
    assert vec_type.get_col_spec("mysql+vtb") == "VECTOR(768)"
    assert vec_type.get_col_spec("sqlite+vtb") == "BLOB"

    ora_bind = vec_type.process_bind_param([0.25, 0.5], "oracle+vtb")
    assert isinstance(ora_bind, array.array)
    pg_bind = vec_type.process_bind_param([0.25, 0.5], "postgresql+vtb")
    assert pg_bind == "[0.25,0.5]"
    assert vec_type.process_result_value(pg_bind, "postgresql+vtb") == [0.25, 0.5]

    tbl = Table("items", MetaData(), Column("embedding", vec_type))
    ora_sql = str(
        select(tbl).where(tbl.c.embedding.cosine_distance([1.0, 0.0]) < 0.5).compile(dialect=SyncOracleDialect())
    )
    assert "VECTOR_DISTANCE(items.embedding," in ora_sql and "COSINE" in ora_sql
    pg_sql = str(
        select(tbl)
        .where(tbl.c.embedding.cosine_distance([1.0, 0.0]) < 0.5)
        .compile(dialect=cast(Any, SyncPostgreSQLDialect)())
    )
    assert "items.embedding <=>" in pg_sql
    mysql_sql = str(
        select(tbl).where(tbl.c.embedding.cosine_distance([1.0, 0.0]) < 0.5).compile(dialect=SyncMySQLDialect())
    )
    assert "DISTANCE_COSINE(items.embedding," in mysql_sql

    mjson = MsgspecJSON()
    assert mjson.get_col_spec("postgresql+vtb") == "JSONB"
    assert mjson.get_col_spec("sqlite+vtb") == "JSONB"
    assert mjson.get_col_spec("oracle+vtb") == "JSON"
    assert mjson.process_result_value(memoryview(b'\x01{"a":1}'), "postgresql+vtb") == {"a": 1}


@pytest.mark.anyio
async def test_advanced_alchemy_audit_and_stored_object_lifecycle() -> None:
    """Verify real UUIDAuditBase, SQLAlchemyAsyncRepositoryService CRUD, and StoredObject rollback cleanup."""
    object_store = SyncObjectStore("memory://")
    async_store = object_store.to_async()
    config = create_async_alchemy_config(
        "sqlite+vtb_async:///:memory:",
        storage_backend=async_store,
        storage_key="docs_store",
        enable_file_object_listener=True,
    )
    plugin = create_alchemy_plugin(config)
    assert plugin is not None

    engine = config.get_engine()
    async with engine.begin() as conn:
        await conn.run_sync(DocumentAuditModel.metadata.create_all)

    session_maker = config.create_session_maker()

    async with session_maker() as session:
        service = DocumentService(session=session)
        file_payload = b"Sample specification text"
        file_obj = FileObject(content=file_payload, filename="arch.txt", backend="docs_store")
        doc = await service.create(DocumentAuditModel(title="Architecture", file=file_obj), auto_commit=True)
        assert doc.id is not None
        assert doc.file.path == "arch.txt"

        stored_bytes = await async_store.get("arch.txt")
        assert stored_bytes == file_payload

        loaded = await service.get(doc.id)
        assert loaded.title == "Architecture"
        assert loaded.file.path == "arch.txt"

    async with session_maker() as session:
        service = DocumentService(session=session)
        temp_file = FileObject(content=b"temporary bytes", filename="temporary.bin", backend="docs_store")
        temp_doc = await service.create(DocumentAuditModel(title="Temp", file=temp_file))
        await session.flush()
        temp_path = temp_doc.file.path
        await session.rollback()

        with pytest.raises(FileNotFoundError):
            await async_store.get(temp_path)

    await engine.dispose()


def test_sqlalchemy_dialects_dbapi_and_alembic_integration() -> None:
    """Verify +vtb dialect registration, DBAPI cursor, and Alembic 1.18+ DefaultImpl integrations."""
    assert DefaultImpl.get_by_dialect(cast(Any, SyncPostgreSQLDialect)()).__name__ == "PostgresqlImpl"
    assert DefaultImpl.get_by_dialect(SyncSQLiteDialect()).__name__ == "SQLiteImpl"
    assert DefaultImpl.get_by_dialect(SyncOracleDialect()).__name__ == "OracleImpl"
    assert DefaultImpl.get_by_dialect(SyncMySQLDialect()).__name__ == "MySQLImpl"
    assert DefaultImpl.get_by_dialect(cast(Any, SyncMSSQLDialect)()).__name__ == "MSSQLImpl"

    dbapi = SyncDBAPIModule()
    conn = dbapi.connect("sqlite+vtb:///:memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    cur.execute("INSERT INTO t (id, val) VALUES (?, ?)", (10, "ten"))
    cur.execute("SELECT id, val FROM t")
    row = cur.fetchone()
    assert row == (10, "ten")
    assert type(row) is tuple
    conn.close()

    class DummyAutogenContext:
        __slots__ = ("imports",)

        def __init__(self) -> None:
            self.imports: set[str] = set()

    ctx = DummyAutogenContext()
    rendered_vec = render_item("type", Vector(1536), ctx)
    assert rendered_vec == "Vector(1536, storage_format='float32')"
    assert "from vertebra.extensions.sqlalchemy import Vector" in ctx.imports
    rendered_json = render_item("type", MsgspecJSON(), ctx)
    assert rendered_json == "MsgspecJSON()"
    assert "from vertebra.extensions.sqlalchemy import MsgspecJSON" in ctx.imports

    view_op = CreateViewOp("active_users_v", "SELECT id FROM users WHERE active = 1", or_replace=True)
    assert (
        view_op.to_sql("postgresql+vtb")
        == "CREATE OR REPLACE VIEW active_users_v AS SELECT id FROM users WHERE active = 1"
    )
    assert view_op.reverse().to_sql("postgresql+vtb") == "DROP VIEW IF EXISTS active_users_v"

    pg_cfg = PostgreSQLConfig(
        dsn="postgresql://u:p@pgbouncer:6432/app",
        direct_dsn="postgresql://u:p@pg-primary:5432/app",
        pooler_mode="transaction",
    )
    assert pg_cfg.effective_direct_dsn == "postgresql://u:p@pg-primary:5432/app"

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

    engine = create_engine("sqlite+vtb:///:memory:")
    with engine.begin() as sa_conn:
        sa_conn.execute(text("CREATE TABLE users (id INTEGER PRIMARY KEY, active INTEGER)"))
        mc = AlembicMigrationContext.configure(sa_conn)
        ops: Any = Operations(mc)
        ops.create_view("active_users_v", "SELECT id FROM users WHERE active = 1", or_replace=True)
        ops.drop_view("active_users_v")
    engine.dispose()


def test_postgres_vtb_dialect_dbapi_and_compiler_features() -> None:
    """Verify SyncPostgreSQLDialect, SyncDBAPIModule paramstyles, compiler hooks, and cursor descriptions."""
    pg_dbapi = SyncPostgreSQLDialect.import_dbapi()
    assert not hasattr(SyncPostgreSQLDialect, "dbapi")
    assert SyncPostgreSQLDialect.default_paramstyle == "numeric_dollar"
    assert SyncPostgreSQLDialect.paramstyle == "numeric_dollar"
    assert pg_dbapi.paramstyle == "numeric_dollar"
    assert SyncSQLiteDialect.import_dbapi().paramstyle == "qmark"
    assert SyncMySQLDialect.import_dbapi().paramstyle == "qmark"
    assert SyncOracleDialect.import_dbapi().paramstyle == "named"

    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    cluster._conn.execute("INSERT INTO items (id, name) VALUES (1, 'item1'), (2, 'item2')")
    driver = SyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost:5432/app"),
        pool=FakeSyncPostgresPool(cluster),
    )

    conn = pg_dbapi.connect("postgresql+vtb://localhost:5432/app", driver=driver)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM items")
    assert cur.description is not None
    assert [d[0] for d in cur.description] == ["id", "name"]
    assert cur.fetchall() == [(1, "item1"), (2, "item2")]
    conn.commit()
    conn.rollback()
    conn.close()
    assert conn.closed is True

    vec = Vector(768)
    mjson = MsgspecJSON()
    pg_dialect = cast(Any, SyncPostgreSQLDialect)()
    assert pg_dialect.type_descriptor(vec) is vec
    assert pg_dialect.type_descriptor(mjson) is mjson


@pytest.mark.parametrize(
    ("dialect_cls", "expected_where_token", "expected_order_token"),
    [
        (SyncPostgreSQLDialect, "<=>", "<->"),
        (SyncSpangresDialect, "<=>", "<->"),
        (SyncOracleDialect, "VECTOR_DISTANCE", "VECTOR_DISTANCE"),
        (SyncMySQLDialect, "DISTANCE_COSINE", "DISTANCE_EUCLIDEAN"),
        (SyncSpannerDialect, "COSINE_DISTANCE", "EUCLIDEAN_DISTANCE"),
        (SyncBigQueryDialect, "COSINE_DISTANCE", "EUCLIDEAN_DISTANCE"),
    ],
)
def test_vector_comparator_compiles_in_select_where_and_order_by(
    dialect_cls: type[Any],
    expected_where_token: str,
    expected_order_token: str,
) -> None:
    """Verify Vector.Comparator expressions compile in where() and order_by() across all vector dialects."""
    if dialect_cls is None:
        pytest.skip("optional SQLAlchemy dialect package not installed")
    for vector_type in (
        Vector(3),
        PgVector(3),
        HalfVector(3),
        SparseVector(3),
        OracleVector(3),
        SpannerVector(3),
    ):
        tbl = Table("docs", MetaData(), Column("id", Integer, primary_key=True), Column("embedding", vector_type))
        stmt = (
            select(tbl)
            .where(tbl.c.embedding.cosine_distance([0.1, 0.2, 0.3]) < 0.5)
            .order_by(tbl.c.embedding.l2_distance([0.1, 0.2, 0.3]))
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            compiled = stmt.compile(dialect=dialect_cls())
        assert not [w for w in caught if issubclass(w.category, DeprecationWarning)]
        sql_text = str(compiled)
        assert expected_where_token in sql_text
        assert expected_order_token in sql_text
        assert compiled.params

    unsupported_tbl = Table("docs", MetaData(), Column("embedding", Vector(3)))
    unsupported_stmt = select(unsupported_tbl).order_by(unsupported_tbl.c.embedding.cosine_distance([0.1, 0.2, 0.3]))
    unsupported_dialects: tuple[Any, ...] = (
        (SyncDuckDBDialect, SyncSQLiteDialect) if _HAS_DUCKDB_ENGINE else (SyncSQLiteDialect,)
    )
    for unsupported_cls in unsupported_dialects:
        with pytest.raises(NotImplementedError):
            unsupported_stmt.compile(dialect=unsupported_cls())


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.parametrize("dialect_cls", [SyncSpangresDialect, AsyncSpangresDialect])
def test_vector_spangres_ddl_compilation(dialect_cls: type[Any]) -> None:
    """Verify CreateTable compiles Vector(768) and Vector() cleanly on sync and async Spangres dialects."""
    meta = MetaData()
    tbl_dim = Table("docs_dim", meta, Column("embedding", Vector(768)))
    tbl_unbounded = Table("docs_any", meta, Column("embedding", Vector()))
    sp_dialect = dialect_cls()
    assert "VECTOR(768)" in str(CreateTable(tbl_dim).compile(dialect=sp_dialect))
    assert "VECTOR" in str(CreateTable(tbl_unbounded).compile(dialect=sp_dialect))


def test_msgspec_json_dialect_impls_and_sqlite_jsonb_roundtrip() -> None:
    """Verify MsgspecJSON dialect impl mapping, str bind encoding, and live SQLite JSONB round-trips."""
    mjson = MsgspecJSON()
    assert isinstance(mjson.load_dialect_impl(cast(Any, SyncPostgreSQLDialect)()), sa_postgresql.JSONB)
    assert isinstance(mjson.load_dialect_impl(SyncSQLiteDialect()), sa_sqlite.JSONB)
    mssql_dialect = cast(Any, SyncMSSQLDialect)()
    mssql_impl = mjson.load_dialect_impl(mssql_dialect)
    assert mssql_impl.compile(dialect=mssql_dialect) == "NVARCHAR(max)"
    assert isinstance(mjson.load_dialect_impl(SyncOracleDialect()), sa_oracle.JSON)
    assert isinstance(mjson.load_dialect_impl(SyncMySQLDialect()), SAJSON)
    if _HAS_SQLALCHEMY_GCP:
        assert isinstance(mjson.load_dialect_impl(cast(Any, SyncSpangresDialect)()), sa_postgresql.JSONB)
        assert type(mjson.load_dialect_impl(SyncBigQueryDialect())) is SAJSON
        assert type(mjson.load_dialect_impl(SyncSpannerDialect())) is SAJSON
    if _HAS_DUCKDB_ENGINE:
        assert type(mjson.load_dialect_impl(SyncDuckDBDialect())) is SAJSON

    assert mjson.process_bind_param("hello", SyncSQLiteDialect()) == '"hello"'
    assert mjson.process_result_value(memoryview(b'{"k": 2}'), SyncSQLiteDialect()) == {"k": 2}

    engine = create_engine("sqlite+vtb:///:memory:")
    meta = MetaData()
    items = Table("json_items", meta, Column("id", Integer, primary_key=True), Column("data", MsgspecJSON()))
    ddl_sql = str(CreateTable(items).compile(engine))
    assert "JSONB" in ddl_sql
    with engine.begin() as conn:
        meta.create_all(conn)
        conn.execute(
            insert(items),
            [
                {"id": 1, "data": {"nested": [1, 2]}},
                {"id": 2, "data": ["alpha", "beta"]},
                {"id": 3, "data": "hello"},
                {"id": 4, "data": None},
            ],
        )
        rows = conn.execute(select(items).order_by(items.c.id)).fetchall()
        assert rows == [
            (1, {"nested": [1, 2]}),
            (2, ["alpha", "beta"]),
            (3, "hello"),
            (4, None),
        ]
    engine.dispose()


def test_deleted_type_helpers_absent() -> None:
    """Verify legacy BinaryJSON and dead SQLAlchemy 2.1 helper classes are removed from public surfaces."""
    removed_names = (
        "BinaryJSON",
        "VectorDistanceExpr",
        "SelectInLoadSpec",
        "InsertSentinelSpec",
        "DeleteUsingClause",
        "ParameterizedSQL",
    )
    for name in removed_names:
        assert not hasattr(sqlalchemy_pkg, name)
        assert not hasattr(sqlalchemy_types_mod, name)
        assert name not in sqlalchemy_pkg.__all__
        assert name not in sqlalchemy_types_mod.__all__


def test_alembic_column_comparator_real_produce_migrations() -> None:
    """Verify Alembic produce_migrations invokes 7-arg column comparator for matching and mismatched schemas."""
    engine = create_engine("sqlite+vtb:///:memory:")
    base_meta = MetaData()
    Table(
        "docs",
        base_meta,
        Column("id", Integer, primary_key=True),
        Column("embedding", Vector(3)),
        Column("payload", MsgspecJSON()),
    )
    with engine.begin() as conn:
        base_meta.create_all(conn)
        mc = AlembicMigrationContext.configure(conn)
        matching_diff = produce_migrations(mc, base_meta)
        assert matching_diff.upgrade_ops is not None
        assert matching_diff.upgrade_ops.is_empty()

        mismatched_meta = MetaData()
        Table(
            "docs",
            mismatched_meta,
            Column("id", Integer, primary_key=True),
            Column("embedding", Integer),
            Column("payload", Integer),
        )
        mismatch_diff = produce_migrations(mc, mismatched_meta)
        assert mismatch_diff.upgrade_ops is not None
        assert not mismatch_diff.upgrade_ops.is_empty()
        modify_ops = [op for op in mismatch_diff.upgrade_ops.ops if isinstance(op, ModifyTableOps)]
        assert modify_ops
    engine.dispose()


def test_alembic_operations_and_render_python_code() -> None:
    """Verify MigrateOperation subclasses execute via Operations and render op.* Python code."""
    engine = create_engine("sqlite+vtb:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)"))
        mc = AlembicMigrationContext.configure(conn)
        ops: Any = Operations(mc)
        ops.create_view("items_v", "SELECT id, name FROM items")
        rows = conn.execute(text("SELECT id, name FROM items_v")).fetchall()
        assert rows == []
        ops.drop_view("items_v")
    engine.dispose()

    upgrade_ops = UpgradeOps(
        [
            CreateViewOp("items_v", "SELECT id FROM items"),
            CreateVectorIndexOp("idx_items_vec", "items", "embedding", metric="cosine"),
            DropVectorIndexOp("idx_items_vec", "items"),
            CreateViewOp("items_v", "SELECT id FROM items").reverse(),
        ]
    )
    rendered = render_python_code(upgrade_ops)
    assert "op.create_view('items_v', 'SELECT id FROM items')" in rendered
    assert "op.create_vector_index('idx_items_vec', 'items', 'embedding', metric='cosine')" in rendered
    assert "op.drop_vector_index('idx_items_vec', 'items')" in rendered
    assert "op.drop_view('items_v')" in rendered


def test_alembic_impl_dialect_resolution_by_name() -> None:
    """Verify DefaultImpl.get_by_dialect resolves custom __dialect__ impls and drops AlembicImpl."""
    import vertebra.extensions.sqlalchemy.alembic as alembic_pkg
    import vertebra.extensions.sqlalchemy.alembic.impl as alembic_impl_mod

    if _HAS_DUCKDB_ENGINE:
        assert DefaultImpl.get_by_dialect(SyncDuckDBDialect()) is DuckDBAlembicImpl
    if _HAS_SQLALCHEMY_GCP:
        assert DefaultImpl.get_by_dialect(SyncBigQueryDialect()) is BigQueryAlembicImpl
        assert DefaultImpl.get_by_dialect(SyncSpannerDialect()) is SpannerAlembicImpl
        assert DefaultImpl.get_by_dialect(cast(Any, SyncSpangresDialect)()) is SpangresAlembicImpl

    for removed in (
        "AlembicImpl",
        "PostgreSQLAlembicImpl",
        "MySQLAlembicImpl",
        "SQLiteAlembicImpl",
        "OracleAlembicImpl",
        "MSSQLAlembicImpl",
    ):
        assert not hasattr(sqlalchemy_pkg, removed)
        assert not hasattr(alembic_pkg, removed)
        assert not hasattr(alembic_impl_mod, removed)


def test_alchemy_sync_config_factory() -> None:
    """Verify create_sync_alchemy_config and storage backend integration."""
    store = SyncObjectStore("memory://")
    backend = ObjectStoreBackend(store=store, key="sync_docs")
    assert backend.store is store

    sync_cfg = create_sync_alchemy_config(
        "sqlite+vtb:///:memory:",
        storage_backend=store,
        storage_key="sync_docs",
    )
    assert sync_cfg.connection_string == "sqlite+vtb:///:memory:"


@pytest.mark.anyio
async def test_pep249_sync_and_async_dbapi_dml_rowcount_routing() -> None:
    """Verify non-row-returning UPDATE and DELETE statements report affected rowcount == 1 across sync and async cursors."""
    raw_sync = SyncDBAPIConnection("sqlite+vtb:///:memory:")
    try:
        raw_cur = raw_sync.cursor()
        raw_cur.execute("CREATE TABLE dml_items (id INTEGER PRIMARY KEY, label TEXT)")
        raw_cur.execute("INSERT INTO dml_items (id, label) VALUES (?, ?)", (1, "v1"))
        raw_cur.execute("UPDATE dml_items SET label = 'RETURNING' WHERE id = ?", (1,))
        assert raw_cur.rowcount == 1
        raw_cur.execute("DELETE FROM dml_items WHERE id = ?", (1,))
        assert raw_cur.rowcount == 1
    finally:
        raw_sync.close()

    raw_async = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:")
    try:
        async_cur = raw_async.cursor()
        await async_cur.execute("CREATE TABLE dml_items (id INTEGER PRIMARY KEY, label TEXT)")
        await async_cur.execute("INSERT INTO dml_items (id, label) VALUES (?, ?)", (1, "v1"))
        await async_cur.execute("UPDATE dml_items SET label = 'RETURNING' WHERE id = ?", (1,))
        assert async_cur.rowcount == 1
        await async_cur.execute("DELETE FROM dml_items WHERE id = ?", (1,))
        assert async_cur.rowcount == 1
    finally:
        await raw_async.close()


@pytest.mark.parametrize(
    ("is_async", "plain_url", "vtb_url"),
    [
        (False, "sqlite+pysqlite:///plain.db", "sqlite+vtb:///managed.db"),
        (True, "sqlite+aiosqlite:///plain.db", "sqlite+vtb_async:///managed.db"),
    ],
    ids=["sync", "async"],
)
def test_alchemy_config_injects_broker_connect_args_only_for_vtb_urls(
    is_async: bool,
    plain_url: str,
    vtb_url: str,
) -> None:
    """Verify standard SQLAlchemy dialects exclude Vertebra-specific connect_args while +vtb dialects inject them."""
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0))
    factory = create_async_alchemy_config if is_async else create_sync_alchemy_config
    try:
        plain_cfg = factory(plain_url, broker=transport)
        plain_connect_args = plain_cfg.engine_config.connect_args
        assert not isinstance(plain_connect_args, dict) or not any(
            key in plain_connect_args for key in ("url", "broker", "lease_managed")
        )

        vtb_cfg = factory(vtb_url, broker=transport)
        vtb_connect_args = vtb_cfg.engine_config.connect_args
        assert isinstance(vtb_connect_args, dict)
        assert vtb_connect_args["url"] == vtb_url
        assert vtb_connect_args["broker"] is transport
        assert vtb_connect_args["lease_managed"] is True
    finally:
        transport.close()


@pytest.mark.parametrize(
    "module_name",
    [
        "vertebra.extensions.sqlalchemy",
        "vertebra.extensions.sqlalchemy.pool",
        "vertebra.extensions.sqlalchemy.dialects",
        "vertebra.extensions.alchemy",
    ],
)
def test_sqlalchemy_and_alchemy_extensions_import_without_sqlspec(module_name: str) -> None:
    """Verify SQLAlchemy and Advanced Alchemy extensions operate cleanly with sqlspec blocked."""
    script = (
        "import importlib, sys; "
        "sys.modules['sqlspec'] = None; "
        f"importlib.import_module({module_name!r}); "
        "from vertebra.extensions.alchemy import create_sync_alchemy_config; "
        "from vertebra.extensions.sqlalchemy.pool import BrokerPool; "
        "from vertebra.extensions.broker import BrokerConfig, SyncSHMBrokerTransport; "
        "t = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0)); "
        "cfg = create_sync_alchemy_config('sqlite+vtb:///:memory:', broker=t); "
        "p = BrokerPool(broker=t, url='sqlite+vtb:///%3Amemory%3A'); "
        "p.dispose(); t.close(); "
        "assert 'sqlspec' not in sys.modules or sys.modules['sqlspec'] is None"
    )
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, f"stderr={proc.stderr!r} stdout={proc.stdout!r}"


@pytest.mark.parametrize(
    "dialect_cls",
    [
        SyncPostgreSQLDialect,
        AsyncPostgreSQLDialect,
        SyncOracleDialect,
        AsyncOracleDialect,
        SyncMySQLDialect,
        AsyncMySQLDialect,
        SyncMSSQLDialect,
        AsyncMSSQLDialect,
    ],
)
def test_sqlalchemy_dialects_type_descriptor(dialect_cls: type[Any]) -> None:
    """SQLAlchemy dialects adapt colspecs via instance type_descriptor."""
    vec = Vector(4)
    mjson = MsgspecJSON()
    generic_json = SAJSON()
    generic_uuid = SAUuid()
    inst = dialect_cls()
    assert inst.type_descriptor(vec) is vec
    assert inst.type_descriptor(mjson) is mjson
    assert inst.type_descriptor(generic_json) is not None
    assert inst.type_descriptor(generic_uuid) is not None
    if dialect_cls is SyncPostgreSQLDialect:
        assert type(inst.type_descriptor(generic_json)) is not SAJSON


@pytest.mark.skipif(not _HAS_SQLALCHEMY_GCP, reason="sqlalchemy GCP dialects require Python < 3.14")
@pytest.mark.parametrize("spangres_cls", [SyncSpangresDialect, AsyncSpangresDialect])
def test_sqlalchemy_dialects_spangres_numeric_dollar_compilation(spangres_cls: type[Any]) -> None:
    """Spangres dialects compile positional numeric_dollar ($1..$N) placeholders."""
    meta = MetaData()
    tbl = Table("accounts", meta, Column("id", Integer), Column("email", String))
    sp_dialect = cast(Any, spangres_cls)()
    assert sp_dialect.paramstyle == "numeric_dollar"
    assert sp_dialect.default_paramstyle == "numeric_dollar"
    assert sp_dialect.positional is True
    compiled_sel = select(tbl).where(tbl.c.id == 42, tbl.c.email == "a@b.c").compile(dialect=sp_dialect)
    sql_text = str(compiled_sel)
    assert "$1" in sql_text and "$2" in sql_text
    assert "%(" not in sql_text
    compiled_ins = insert(tbl).values(id=1, email="x@y.z").compile(dialect=sp_dialect)
    ins_text = str(compiled_ins)
    assert "$1" in ins_text and "$2" in ins_text


@pytest.mark.skipif(
    not (_HAS_DUCKDB_ENGINE and _HAS_SQLALCHEMY_GCP),
    reason="optional DuckDB/GCP SQLAlchemy dialects require Python < 3.14",
)
def test_sqlalchemy_dialects_alembic_bare_names() -> None:
    """Alembic DefaultImpl resolves custom DuckDB, BigQuery, Spanner, and Spangres dialects."""
    assert DefaultImpl.get_by_dialect(SyncDuckDBDialect()) is DuckDBAlembicImpl
    assert DefaultImpl.get_by_dialect(SyncBigQueryDialect()) is BigQueryAlembicImpl
    assert DefaultImpl.get_by_dialect(SyncSpannerDialect()) is SpannerAlembicImpl
    assert DefaultImpl.get_by_dialect(cast(Any, SyncSpangresDialect)()) is SpangresAlembicImpl


@pytest.mark.anyio
async def test_async_adapter_setinputsizes_var_nextset_and_terminate() -> None:
    """Verify synchronous setinputsizes, var, nextset, executemany RETURNING, autocommit, and terminate."""
    seen_inputsizes: list[Any] = []
    closed_graceful = False
    forced_closed = False

    class _FakeAsyncCursor:
        def __init__(self) -> None:
            self.description: tuple[tuple[Any, ...], ...] | None = None
            self.rowcount: int = 0
            self.lastrowid: int | None = None
            self.arraysize: int = 1
            self._sets: list[tuple[list[tuple[Any, ...]], tuple[tuple[Any, ...], ...] | None]] = []
            self._rows: list[tuple[Any, ...]] = []
            self._closed = False

        async def __aenter__(self) -> _FakeAsyncCursor:
            return self

        async def __aexit__(self, *args: Any) -> None:
            await self.close()

        def setinputsizes(self, *sizes: Any) -> None:
            seen_inputsizes.append(sizes)

        def var(self, type_code: Any, size: int = 0) -> tuple[Any, int]:
            return (type_code, size)

        async def execute(self, operation: str, parameters: Any = None) -> None:
            _ = (operation, parameters)
            self._rows = [(1, "first")]
            self.description = (
                ("id", None, None, None, None, None, None),
                ("name", None, None, None, None, None, None),
            )
            self.rowcount = 1
            self.lastrowid = 1
            self._sets = [([(2, "second")], self.description)]

        async def executemany(self, operation: str, seq_of_parameters: Any) -> None:
            _ = operation
            self._rows = [(idx + 1, item["name"]) for idx, item in enumerate(seq_of_parameters)]
            self.description = (
                ("id", None, None, None, None, None, None),
                ("name", None, None, None, None, None, None),
            )
            self.rowcount = len(self._rows)
            self.lastrowid = len(self._rows)

        async def nextset(self) -> bool | None:
            if not self._sets:
                self._rows = []
                self.description = None
                return None
            rows, desc = self._sets.pop(0)
            self._rows = rows
            self.description = desc
            self.rowcount = len(rows)
            return True

        async def fetchall(self) -> list[tuple[Any, ...]]:
            rows = list(self._rows)
            self._rows = []
            return rows

        async def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        async def close(self) -> None:
            self._closed = True

    class _FakeAsyncConnection:
        def __init__(self) -> None:
            self.autocommit = False
            self.closed = False

        def cursor(self) -> _FakeAsyncCursor:
            return _FakeAsyncCursor()

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            nonlocal closed_graceful
            closed_graceful = True
            self.closed = True

        def terminate(self) -> None:
            nonlocal forced_closed
            forced_closed = True
            self.closed = True

    dbapi = SQLAlchemyAsyncDBAPI(dialect="sqlite")
    raw_graceful = _FakeAsyncConnection()
    adapted = AsyncAdapt_vertebra_connection(dbapi, cast("Any", raw_graceful))
    assert adapted.autocommit is False
    adapted.autocommit = True
    assert raw_graceful.autocommit is True

    cur = await greenlet_spawn(adapted.cursor)
    assert isinstance(cur, AsyncAdapt_vertebra_cursor)
    assert cur._awaitable_cursor_close is True
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        cur.setinputsizes(10, 20)
    assert [w for w in caught if issubclass(w.category, RuntimeWarning)] == []
    assert seen_inputsizes == [(10, 20)]
    assert cur.var("VARCHAR", size=64) == ("VARCHAR", 64)

    await greenlet_spawn(cur.execute, "SELECT id, name FROM t")
    assert cur.rowcount == 1
    assert cur.lastrowid == 1
    assert cur.fetchall() == [(1, "first")]
    assert await greenlet_spawn(cur.nextset) is True
    assert cur.fetchall() == [(2, "second")]
    assert await greenlet_spawn(cur.nextset) is None

    await greenlet_spawn(
        cur.executemany, "INSERT INTO t (name) VALUES (:name) RETURNING id, name", [{"name": "a"}, {"name": "b"}]
    )
    assert cur.rowcount == 2
    assert cur.lastrowid == 2
    assert cur.fetchall() == [(1, "a"), (2, "b")]
    await greenlet_spawn(cur.close)

    ss_cur = await greenlet_spawn(adapted.cursor, True)
    assert isinstance(ss_cur, AsyncAdapt_vertebra_ss_cursor)
    await greenlet_spawn(ss_cur.close)

    await greenlet_spawn(adapted.terminate)
    assert closed_graceful is True
    assert raw_graceful.closed is True

    raw_force = _FakeAsyncConnection()
    adapted_force = AsyncAdapt_vertebra_connection(dbapi, cast("Any", raw_force))
    with warnings.catch_warnings(record=True) as caught_force:
        warnings.simplefilter("always", RuntimeWarning)
        adapted_force.terminate()
    assert [w for w in caught_force if issubclass(w.category, RuntimeWarning)] == []
    assert forced_closed is True
    assert raw_force.closed is True


@pytest.mark.anyio
async def test_async_adapter_async_creator_fn_and_error_hierarchy() -> None:
    """Verify async_creator_fn, create_async_engine(..., async_creator=...), and DBAPI metadata."""
    created_drivers: list[AsyncSQLiteDriver] = []

    async def _make_conn() -> AsyncDBAPIConnection:
        driver = AsyncSQLiteDriver()
        created_drivers.append(driver)
        return AsyncDBAPIConnection(driver=driver, share_pool=False)

    pg_dbapi = SQLAlchemyAsyncDBAPI(paramstyle="numeric_dollar", dialect="postgresql")
    assert not hasattr(pg_dbapi, "sqlite_version")
    assert not hasattr(pg_dbapi, "sqlite_version_info")
    assert pg_dbapi.paramstyle == "numeric_dollar"
    assert pg_dbapi.Date(2026, 11, 17).year == 2026
    assert pg_dbapi.Time(1, 2, 3).minute == 2
    assert pg_dbapi.Timestamp(2026, 11, 17, 1, 2, 3).second == 3
    assert pg_dbapi.Binary(b"abc") == b"abc"
    assert issubclass(pg_dbapi.OperationalError, pg_dbapi.DatabaseError)
    assert issubclass(pg_dbapi.DatabaseError, pg_dbapi.Error)

    sqlite_dbapi = SQLAlchemyAsyncDBAPI(dialect="sqlite")
    assert isinstance(sqlite_dbapi.sqlite_version, str)
    assert isinstance(sqlite_dbapi.sqlite_version_info, tuple)

    adapted = await greenlet_spawn(sqlite_dbapi.connect, async_creator_fn=_make_conn)
    try:
        assert isinstance(adapted, AsyncAdapt_vertebra_connection)
        assert len(created_drivers) == 1
    finally:
        await greenlet_spawn(adapted.close)

    async_engine = create_async_engine("sqlite+vtb_async:///:memory:", async_creator=_make_conn)
    try:
        async with async_engine.connect() as conn:
            res = await conn.exec_driver_sql("SELECT 41")
            assert res.fetchone() == (41,)
        assert len(created_drivers) == 2
    finally:
        await async_engine.dispose()


@pytest.mark.parametrize(
    ("sqlstate", "vendor_code", "is_transient", "expected"),
    [
        ("08006", None, True, True),
        ("08S01", None, False, True),
        ("57P01", None, False, True),
        ("57P02", None, False, True),
        ("57P03", None, False, True),
        (None, 3113, False, True),
        (None, 2006, False, True),
        ("40001", None, True, False),
        ("40P01", None, True, False),
    ],
)
def test_base_dialect_driver_connection_terminate_and_disconnect(
    sqlstate: str | None,
    vendor_code: int | None,
    is_transient: bool,
    expected: bool,
) -> None:
    """Verify get_driver_connection, do_terminate, import_dbapi metadata, and SQLSTATE disconnect matching."""

    class _FakeError(Exception):
        sqlstate: str | None
        vendor_code: int | None
        is_transient: bool

    err = _FakeError("transient database status")
    err.sqlstate = sqlstate
    err.vendor_code = vendor_code
    err.is_transient = is_transient

    sync_pg = cast("Any", SyncPostgreSQLDialect)()
    async_pg = cast("Any", AsyncPostgreSQLDialect)()
    sync_mysql = cast("Any", SyncMySQLDialect)()
    assert sync_pg.is_disconnect(err) is expected
    assert async_pg.is_disconnect(err) is expected
    assert sync_mysql.is_disconnect(err) is expected

    class _BrokenStrError(Exception):
        def __str__(self) -> str:
            raise RuntimeError("broken str")

    assert sync_mysql.is_disconnect(_BrokenStrError()) is False

    class _TerminableConn:
        def __init__(self) -> None:
            self.terminated = False
            self.closed = False

        def terminate(self) -> None:
            self.terminated = True

        def close(self) -> None:
            self.closed = True

    sync_conn = _TerminableConn()
    assert sync_pg.get_driver_connection(sync_conn) is sync_conn
    sync_pg.do_terminate(sync_conn)
    assert sync_conn.terminated is True

    raw_async = _TerminableConn()
    adapted_stub = type("_AdaptedStub", (), {"_connection": raw_async, "terminate": raw_async.terminate})()
    assert async_pg.get_driver_connection(adapted_stub) is raw_async
    raw_async.terminated = False
    async_pg.do_terminate(adapted_stub)
    assert raw_async.terminated is True

    assert not hasattr(SyncPostgreSQLDialect, "dbapi")
    assert not hasattr(AsyncPostgreSQLDialect, "dbapi")
    assert not hasattr(SyncPostgreSQLDialect.import_dbapi(), "sqlite_version")
    assert not hasattr(AsyncPostgreSQLDialect.import_dbapi(), "sqlite_version")
    assert hasattr(SyncSQLiteDialect.import_dbapi(), "sqlite_version")
    assert hasattr(AsyncSQLiteDialect.import_dbapi(), "sqlite_version")


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("asynchronous", "version_text", "expected_tuple", "expected_native_bool"),
    [
        (False, "23.4.0.24.05", (23, 4, 0, 24, 5), True),
        (False, "19.3.0.0.0", (19, 3, 0, 0, 0), False),
        (True, "23.4.0.24.05", (23, 4, 0, 24, 5), True),
        (True, "19.3.0.0.0", (19, 3, 0, 0, 0), False),
    ],
)
async def test_oracle_get_server_version_info_queries_and_initializes_capabilities(
    asynchronous: bool,
    version_text: str,
    expected_tuple: tuple[int, ...],
    expected_native_bool: bool,
) -> None:
    """Verify sync and async Oracle dialects query product_component_version and initialize capabilities."""

    class _SyncOracleVersionCursor:
        _rows: list[tuple[Any, ...]]
        description: tuple[tuple[Any, ...], ...] | None
        rowcount: int

        def __init__(self, driver: _SyncOracleVersionDriver) -> None:
            self._driver = driver
            self._rows = []
            self.description = None
            self.rowcount = -1

        def execute(self, sql: str, parameters: Any = ()) -> _SyncOracleVersionCursor:
            rows, cols = self._driver.execute_with_description(sql, parameters)
            self._rows = list(rows)
            self.description = tuple((c, None, None, None, None, None, None) for c in cols) if cols else None
            self.rowcount = len(self._rows)
            return self

        def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        def fetchall(self) -> list[tuple[Any, ...]]:
            rows, self._rows = self._rows, []
            return rows

        def setinputsizes(self, *sizes: Any, **kw: Any) -> None:
            _ = (sizes, kw)

        def close(self) -> None:
            self._rows.clear()

    class _SyncOracleVersionDriver:
        def __init__(self, version_value: str | None) -> None:
            self._version_value = version_value
            self.queries: list[str] = []

        def cursor(self, *, lazy_begin: bool = False) -> _SyncOracleVersionCursor:
            _ = lazy_begin
            return _SyncOracleVersionCursor(self)

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = parameters
            self.queries.append(sql)
            if "product_component_version" in sql:
                rows = [(self._version_value,)] if self._version_value is not None else []
                return rows, ("VERSION",)
            if "current_schema" in sql:
                return [("APP_OWNER",)], ("SCHEMA",)
            if "v$parameter" in sql:
                rows = [(self._version_value,)] if self._version_value is not None else []
                return rows, ("VALUE",)
            return [], None

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    class _AsyncOracleVersionCursor:
        _rows: list[tuple[Any, ...]]
        description: tuple[tuple[Any, ...], ...] | None
        rowcount: int

        def __init__(self, driver: _AsyncOracleVersionDriver) -> None:
            self._driver = driver
            self._rows = []
            self.description = None
            self.rowcount = -1

        async def execute(self, sql: str, parameters: Any = ()) -> _AsyncOracleVersionCursor:
            rows, cols = await self._driver.execute_with_description(sql, parameters)
            self._rows = list(rows)
            self.description = tuple((c, None, None, None, None, None, None) for c in cols) if cols else None
            self.rowcount = len(self._rows)
            return self

        async def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        async def fetchall(self) -> list[tuple[Any, ...]]:
            rows, self._rows = self._rows, []
            return rows

        def setinputsizes(self, *sizes: Any, **kw: Any) -> None:
            _ = (sizes, kw)

        async def close(self) -> None:
            self._rows.clear()

    class _AsyncOracleVersionDriver:
        def __init__(self, version_value: str | None) -> None:
            self._version_value = version_value
            self.queries: list[str] = []

        def cursor(self, *, lazy_begin: bool = False) -> _AsyncOracleVersionCursor:
            _ = lazy_begin
            return _AsyncOracleVersionCursor(self)

        async def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = parameters
            self.queries.append(sql)
            if "product_component_version" in sql:
                rows = [(self._version_value,)] if self._version_value is not None else []
                return rows, ("VERSION",)
            if "current_schema" in sql:
                return [("APP_OWNER",)], ("SCHEMA",)
            if "v$parameter" in sql:
                rows = [(self._version_value,)] if self._version_value is not None else []
                return rows, ("VALUE",)
            return [], None

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            return None

    if asynchronous:
        async_driver = _AsyncOracleVersionDriver(version_text)
        async_engine = create_async_engine(
            "oracle+vtb_async://scott:tiger@localhost:1521/freepdb1",
            async_creator=lambda: AsyncDBAPIConnection("oracle+vtb_async://localhost", driver=async_driver),
        )
        try:
            async with async_engine.connect():
                assert async_engine.dialect.server_version_info == expected_tuple
                assert async_engine.dialect.supports_native_boolean is expected_native_bool
                assert any("product_component_version" in q for q in async_driver.queries)
        finally:
            await async_engine.dispose()

        bad_driver = _AsyncOracleVersionDriver("not-a-version")
        bad_engine = create_async_engine(
            "oracle+vtb_async://scott:tiger@localhost:1521/freepdb1",
            async_creator=lambda: AsyncDBAPIConnection("oracle+vtb_async://localhost", driver=bad_driver),
        )
        try:
            with pytest.raises(sa_exc.DBAPIError):
                async with bad_engine.connect():
                    pass
        finally:
            await bad_engine.dispose()
        return

    sync_driver = _SyncOracleVersionDriver(version_text)
    sync_engine = create_engine(
        "oracle+vtb://scott:tiger@localhost:1521/freepdb1",
        creator=lambda: SyncDBAPIConnection("oracle+vtb://localhost", driver=sync_driver),
    )
    try:
        with sync_engine.connect():
            assert sync_engine.dialect.server_version_info == expected_tuple
            assert sync_engine.dialect.supports_native_boolean is expected_native_bool
            assert any("product_component_version" in q for q in sync_driver.queries)
    finally:
        sync_engine.dispose()

    bad_sync_driver = _SyncOracleVersionDriver(None)
    bad_sync_engine = create_engine(
        "oracle+vtb://scott:tiger@localhost:1521/freepdb1",
        creator=lambda: SyncDBAPIConnection("oracle+vtb://localhost", driver=bad_sync_driver),
    )
    try:
        with pytest.raises(sa_exc.DBAPIError), bad_sync_engine.connect():
            pass
    finally:
        bad_sync_engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_oracle_execution_context_returning_into_cursor_var(asynchronous: bool) -> None:
    """Verify INSERT ... RETURNING ... INTO executes via cursor.var() and out_binds on sync and async Oracle."""
    meta = MetaData()
    items = Table(
        "items",
        meta,
        Column("id", Integer, primary_key=True),
        Column("name", String(64)),
    )

    class _FakeOutVar:
        def __init__(self, type_code: Any, size: int = 0, arraysize: int = 1, outconverter: Any = None) -> None:
            self.type_code = type_code
            self.size = size
            self.arraysize = arraysize
            self.outconverter = outconverter
            self.values: list[list[Any]] = [[]]

        def setvalue(self, pos: int, value: Any) -> None:
            while len(self.values) <= pos:
                self.values.append([])
            self.values[pos] = [value]

        def getvalue(self, pos: int = 0) -> Any:
            vals = self.values[pos]
            raw = vals[0] if vals else None
            if self.outconverter is not None and raw is not None:
                return self.outconverter(raw)
            return raw

    class _SyncOracleReturningCursor:
        _rows: list[tuple[Any, ...]]
        description: tuple[tuple[Any, ...], ...] | None
        rowcount: int

        def __init__(self, driver: _SyncOracleReturningDriver) -> None:
            self._driver = driver
            self._rows = []
            self.description = None
            self.rowcount = -1

        def var(self, type_code: Any, size: int = 0, arraysize: int = 1, outconverter: Any = None) -> _FakeOutVar:
            return self._driver.var(type_code, size=size, arraysize=arraysize, outconverter=outconverter)

        def execute(self, sql: str, parameters: Any = ()) -> _SyncOracleReturningCursor:
            rows, cols = self._driver.execute_with_description(sql, parameters)
            self._rows = list(rows)
            self.description = tuple((c, None, None, None, None, None, None) for c in cols) if cols else None
            self.rowcount = len(self._rows)
            return self

        def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        def fetchall(self) -> list[tuple[Any, ...]]:
            rows, self._rows = self._rows, []
            return rows

        def setinputsizes(self, *sizes: Any, **kw: Any) -> None:
            _ = (sizes, kw)

        def close(self) -> None:
            self._rows.clear()

    class _SyncOracleReturningDriver:
        server_version_info = (23, 4, 0, 0, 0)

        def __init__(self) -> None:
            self.executed: list[tuple[str, Any]] = []
            self.var_calls: list[tuple[Any, int]] = []
            self.out_binds: dict[str, Any] = {}

        def cursor(self, *, lazy_begin: bool = False) -> _SyncOracleReturningCursor:
            _ = lazy_begin
            return _SyncOracleReturningCursor(self)

        def var(self, type_code: Any, size: int = 0, arraysize: int = 1, outconverter: Any = None) -> _FakeOutVar:
            self.var_calls.append((type_code, arraysize))
            return _FakeOutVar(type_code, size=size, arraysize=arraysize, outconverter=outconverter)

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            self.executed.append((sql, parameters))
            if "current_schema" in sql:
                return [("APP_OWNER",)], ("SCHEMA",)
            if "v$parameter" in sql:
                return [("23.4.0.0.0",)], ("VALUE",)
            if isinstance(parameters, dict):
                self.out_binds = {"ret_0": 101, "ret_1": parameters.get("name", "widget")}
                for key in ("ret_0", "ret_1"):
                    if key in parameters and hasattr(parameters[key], "setvalue"):
                        parameters[key].setvalue(0, self.out_binds[key])
            return [], None

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    class _AsyncOracleReturningCursor:
        _rows: list[tuple[Any, ...]]
        description: tuple[tuple[Any, ...], ...] | None
        rowcount: int

        def __init__(self, driver: _AsyncOracleReturningDriver) -> None:
            self._driver = driver
            self._rows = []
            self.description = None
            self.rowcount = -1

        def var(self, type_code: Any, size: int = 0, arraysize: int = 1, outconverter: Any = None) -> _FakeOutVar:
            return self._driver.var(type_code, size=size, arraysize=arraysize, outconverter=outconverter)

        async def execute(self, sql: str, parameters: Any = ()) -> _AsyncOracleReturningCursor:
            rows, cols = await self._driver.execute_with_description(sql, parameters)
            self._rows = list(rows)
            self.description = tuple((c, None, None, None, None, None, None) for c in cols) if cols else None
            self.rowcount = len(self._rows)
            return self

        async def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        async def fetchall(self) -> list[tuple[Any, ...]]:
            rows, self._rows = self._rows, []
            return rows

        def setinputsizes(self, *sizes: Any, **kw: Any) -> None:
            _ = (sizes, kw)

        async def close(self) -> None:
            self._rows.clear()

    class _AsyncOracleReturningDriver:
        server_version_info = (23, 4, 0, 0, 0)

        def __init__(self) -> None:
            self.executed: list[tuple[str, Any]] = []
            self.var_calls: list[tuple[Any, int]] = []
            self.out_binds: dict[str, Any] = {}

        def cursor(self, *, lazy_begin: bool = False) -> _AsyncOracleReturningCursor:
            _ = lazy_begin
            return _AsyncOracleReturningCursor(self)

        def var(self, type_code: Any, size: int = 0, arraysize: int = 1, outconverter: Any = None) -> _FakeOutVar:
            self.var_calls.append((type_code, arraysize))
            return _FakeOutVar(type_code, size=size, arraysize=arraysize, outconverter=outconverter)

        async def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            self.executed.append((sql, parameters))
            if "current_schema" in sql:
                return [("APP_OWNER",)], ("SCHEMA",)
            if "v$parameter" in sql:
                return [("23.4.0.0.0",)], ("VALUE",)
            if isinstance(parameters, dict):
                self.out_binds = {"ret_0": 101, "ret_1": parameters.get("name", "widget")}
                for key in ("ret_0", "ret_1"):
                    if key in parameters and hasattr(parameters[key], "setvalue"):
                        parameters[key].setvalue(0, self.out_binds[key])
            return [], None

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            return None

    stmt_one = insert(items).values(name="widget").returning(items.c.id)
    stmt_two = insert(items).values(name="gadget").returning(items.c.id, items.c.name)

    if asynchronous:
        async_driver = _AsyncOracleReturningDriver()
        async_engine = create_async_engine(
            "oracle+vtb_async://scott:tiger@localhost:1521/freepdb1",
            async_creator=lambda: AsyncDBAPIConnection("oracle+vtb_async://localhost", driver=async_driver),
        )
        try:
            async with async_engine.begin() as async_conn:
                async_res_one = await async_conn.execute(stmt_one)
                assert async_res_one.scalar_one() == 101
                async_res_two = await async_conn.execute(stmt_two)
                assert async_res_two.one() == (101, "gadget")
            returning_sqls = [sql for sql, _ in async_driver.executed if "INSERT INTO items" in sql]
            assert len(returning_sqls) == 2
            assert "RETURNING items.id INTO :ret_0" in returning_sqls[0]
            assert "RETURNING items.id, items.name INTO :ret_0, :ret_1" in returning_sqls[1]
            assert all(arraysize == 1 for _, arraysize in async_driver.var_calls)
        finally:
            await async_engine.dispose()
        return

    sync_driver = _SyncOracleReturningDriver()
    sync_engine = create_engine(
        "oracle+vtb://scott:tiger@localhost:1521/freepdb1",
        creator=lambda: SyncDBAPIConnection("oracle+vtb://localhost", driver=sync_driver),
    )
    try:
        with sync_engine.begin() as sync_conn:
            sync_res_one = sync_conn.execute(stmt_one)
            assert sync_res_one.scalar_one() == 101
            sync_res_two = sync_conn.execute(stmt_two)
            assert sync_res_two.one() == (101, "gadget")
        returning_sqls = [sql for sql, _ in sync_driver.executed if "INSERT INTO items" in sql]
        assert len(returning_sqls) == 2
        assert "RETURNING items.id INTO :ret_0" in returning_sqls[0]
        assert "RETURNING items.id, items.name INTO :ret_0, :ret_1" in returning_sqls[1]
        assert all(arraysize == 1 for _, arraysize in sync_driver.var_calls)
    finally:
        sync_engine.dispose()


@pytest.mark.parametrize("dialect_cls", [SyncOracleDialect, AsyncOracleDialect])
@pytest.mark.parametrize(
    ("message", "vendor_code", "sqlstate", "expected"),
    [
        ("ORA-03113: end-of-file on communication channel", None, None, True),
        ("ORA-03114: not connected to ORACLE", None, None, True),
        ("ORA-01012: not logged on", None, None, True),
        ("not connected", None, None, True),
        ("session killed", 28, None, True),
        ("shutdown in progress", 1033, None, True),
        ("immediate shutdown", 1089, None, True),
        ("lost contact", 3135, None, True),
        ("listener closed connection", 12537, None, True),
        ("transport failure", None, "08006", True),
        ("ORA-00001: unique constraint violated", 1, "23000", False),
        ("duplicate key value", None, "23505", False),
        ("serialization failure", None, "40001", False),
    ],
)
def test_oracle_is_disconnect_codes(
    dialect_cls: type[Any],
    message: str,
    vendor_code: int | None,
    sqlstate: str | None,
    expected: bool,
) -> None:
    """Verify Oracle disconnect detection matches ORA codes and 08* SQLSTATEs without false positives."""

    class _OracleErr(Exception):
        vendor_code: int | None
        sqlstate: str | None

    err = _OracleErr(message)
    err.vendor_code = vendor_code
    err.sqlstate = sqlstate
    dialect = dialect_cls()
    assert dialect.is_disconnect(err) is expected


@pytest.mark.parametrize(
    ("is_async", "url_str", "expected_is_file", "expected_pool_name"),
    [
        (False, "sqlite+vtb:///:memory:", False, "StaticPool"),
        (False, "sqlite+vtb:///%3Amemory%3A", False, "StaticPool"),
        (False, "sqlite+vtb:///file:memdb1?mode=memory&cache=shared", False, "StaticPool"),
        (False, "sqlite+vtb://", False, "StaticPool"),
        (False, "sqlite+vtb:///app_data.db", True, "QueuePool"),
        (False, "sqlite+vtb:///app_data.db?shm_broker=true", True, "NullPool"),
        (True, "sqlite+vtb_async:///:memory:", False, "StaticPool"),
        (True, "sqlite+vtb_async:///%3Amemory%3A", False, "StaticPool"),
        (True, "sqlite+vtb_async:///file:memdb1?mode=memory&cache=shared", False, "StaticPool"),
        (True, "sqlite+vtb_async:///app_data.db", True, "AsyncAdaptedQueuePool"),
        (True, "sqlite+vtb_async:///app_data.db?shm_broker=true", True, "NullPool"),
    ],
)
def test_sqlite_is_url_file_db_and_pool_selection(
    is_async: bool,
    url_str: str,
    expected_is_file: bool,
    expected_pool_name: str,
) -> None:
    """Verify SQLite _is_url_file_db and get_pool_class match SQLAlchemy memory and broker rules."""
    dialect_cls: Any = AsyncSQLiteDialect if is_async else SyncSQLiteDialect
    url = make_url(url_str)
    assert dialect_cls._is_url_file_db(url) is expected_is_file
    assert dialect_cls.get_pool_class(url).__name__ == expected_pool_name

    if not is_async and expected_pool_name == "StaticPool" and "mode=memory" in url_str:
        engine = create_engine(url_str)
        try:
            assert isinstance(engine.pool, StaticPool)
        finally:
            engine.dispose()


def test_sqlite_get_pool_class_broker_routes_to_bound_broker_pool(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Verify SQLite ?broker=true returns a bound BrokerPool / AsyncBrokerPool from DatabaseRegistry."""
    fake_broker = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    db_path = tmp_path / "brokered.db"
    try:
        monkeypatch.setattr(DatabaseRegistry, "default_broker", classmethod(lambda cls: fake_broker))
        sync_pool_cls = SyncSQLiteDialect.get_pool_class(make_url(f"sqlite+vtb:///{db_path}?broker=true"))
        async_pool_cls = AsyncSQLiteDialect.get_pool_class(make_url(f"sqlite+vtb_async:///{db_path}?broker=true"))
        assert issubclass(sync_pool_cls, BrokerPool)
        assert issubclass(async_pool_cls, AsyncBrokerPool)
    finally:
        fake_broker.close()


@pytest.mark.anyio
async def test_sqlite_do_ping_leaves_no_open_transaction() -> None:
    """Verify SQLite do_ping with pool_pre_ping=True executes SELECT 1 without leaving an open transaction."""
    sync_dialect = cast(Any, SyncSQLiteDialect)()
    sync_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", autocommit=False, share_pool=False)
    try:
        assert sync_conn.autocommit is False
        assert sync_dialect.do_ping(sync_conn) is True
        assert sync_conn.autocommit is False
        assert sync_conn.driver is not None
        assert getattr(sync_conn.driver, "in_transaction", None) is False
    finally:
        sync_conn.close()

    sync_engine = create_engine("sqlite+vtb:///:memory:", pool_pre_ping=True)
    try:
        with sync_engine.connect() as conn:
            assert conn.exec_driver_sql("SELECT 1").scalar() == 1
        with sync_engine.connect() as conn:
            raw_conn = conn.connection.dbapi_connection
            assert raw_conn is not None
            assert raw_conn.driver.in_transaction is False
            assert conn.exec_driver_sql("SELECT 2").scalar() == 2
    finally:
        sync_engine.dispose()

    async_dialect = cast(Any, AsyncSQLiteDialect)()
    async_driver = AsyncSQLiteDriver()
    async_raw_conn = AsyncDBAPIConnection(driver=async_driver, autocommit=False, share_pool=False)
    async_dbapi = SQLAlchemyAsyncDBAPI(dialect="sqlite")
    adapted = AsyncAdapt_vertebra_connection(async_dbapi, cast(Any, async_raw_conn))
    try:
        assert await greenlet_spawn(async_dialect.do_ping, adapted) is True
        assert async_raw_conn.autocommit is False
        assert async_driver.in_transaction is False
    finally:
        await greenlet_spawn(adapted.close)

    async_engine = create_async_engine("sqlite+vtb_async:///:memory:", pool_pre_ping=True)
    try:
        async with async_engine.connect() as aconn:
            res = await aconn.exec_driver_sql("SELECT 1")
            assert res.scalar() == 1
        async with async_engine.connect() as aconn:
            res = await aconn.exec_driver_sql("SELECT 2")
            assert res.scalar() == 2
    finally:
        await async_engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("dialect_cls", [SyncSQLiteDialect, AsyncSQLiteDialect])
async def test_sqlite_is_disconnect_closed_connection(dialect_cls: type[Any]) -> None:
    """Verify SQLite is_disconnect matches closed-database errors and 08* SQLSTATEs cleanly."""
    dialect = dialect_cls()
    assert dialect.is_disconnect(ProgrammingError("Cannot operate on a closed database.")) is True
    assert dialect.is_disconnect(OperationalError("Cannot operate on a closed database.")) is True
    assert dialect.is_disconnect(InterfaceError("Cannot operate on a closed connection.")) is True
    assert dialect.is_disconnect(ProgrammingError("near syntax error")) is False

    class _SqlstateErr(Exception):
        sqlstate: str

    err_08 = _SqlstateErr("broken")
    err_08.sqlstate = "08006"
    assert dialect.is_disconnect(err_08) is True

    err_constraint = _SqlstateErr("unique")
    err_constraint.sqlstate = "23505"
    assert dialect.is_disconnect(err_constraint) is False

    if dialect_cls is SyncSQLiteDialect:
        conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)
        conn.close()
        with pytest.raises(Exception) as exc_info:
            conn.cursor().execute("SELECT 1")
        assert dialect.is_disconnect(exc_info.value) is True
    else:
        aconn = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:", share_pool=False)
        await aconn.close()
        with pytest.raises(Exception) as aexc_info:
            await aconn.cursor().execute("SELECT 1")
        assert dialect.is_disconnect(aexc_info.value) is True


class _OracleJsonBlobClient(_StubSyncOracleClient):
    """In-memory Oracle native client double servicing SQLAlchemy init and BLOB JSON round-trips."""

    def __init__(self) -> None:
        super().__init__()
        self.rows_by_id: dict[int, tuple[bytes | None, bytes | None]] = {}

    def query(
        self,
        sql: str,
        params: Any = None,
        *,
        out_binds: Any = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> _oracle_native.OracleQueryResult:
        super().query(
            sql,
            params,
            out_binds=out_binds,
            fetch_decimals=fetch_decimals,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        lower_sql = sql.lower()
        if "product_component_version" in lower_sql:
            col = _oracle_native.OracleColumn("version", "VARCHAR2")
            return _oracle_native.OracleQueryResult([col], [("23.4.0.0.0",)], rowcount=1)
        if "current_schema" in lower_sql:
            col = _oracle_native.OracleColumn("schema", "VARCHAR2")
            return _oracle_native.OracleQueryResult([col], [("APP_OWNER",)], rowcount=1)
        if "v$parameter" in lower_sql:
            col = _oracle_native.OracleColumn("value", "VARCHAR2")
            return _oracle_native.OracleQueryResult([col], [("23.4.0.0.0",)], rowcount=1)
        row_id = int(params["id_1"]) if isinstance(params, dict) and "id_1" in params else 1
        stored = self.rows_by_id.get(row_id)
        cols = [
            _oracle_native.OracleColumn("native_data", "BLOB"),
            _oracle_native.OracleColumn("legacy_data", "BLOB"),
        ]
        rows = [stored] if stored is not None else []
        return _oracle_native.OracleQueryResult(cols, rows, rowcount=len(rows))

    def execute(
        self,
        sql: str,
        params: Any = None,
        *,
        out_binds: Any = None,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
    ) -> _oracle_native.OracleQueryResult:
        super().execute(
            sql,
            params,
            out_binds=out_binds,
            fetch_decimals=fetch_decimals,
            enable_lowercase_column_names=enable_lowercase_column_names,
        )
        assert isinstance(params, dict)
        assert "ret_0" not in params
        assert "ret_1" not in params
        row_id = int(params["id"])
        native_val = params.get("native_data")
        legacy_val = params.get("legacy_data")
        assert native_val is None or isinstance(native_val, bytes)
        assert legacy_val is None or isinstance(legacy_val, bytes)
        self.rows_by_id[row_id] = (native_val, legacy_val)
        return _oracle_native.OracleQueryResult(
            [],
            [],
            rowcount=1,
            out_binds={"ret_0": native_val, "ret_1": legacy_val},
        )


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    ("row_id", "payload"),
    [
        (1, {"key": "value"}),
        (2, {"text": "é" * 33_000, "numbers": [7, 1.25]}),
        (3, '{"already_json": 1}'),
        (4, None),
    ],
)
async def test_advanced_alchemy_1130b2_oracle_jsonb_blob_transport_and_returning(
    asynchronous: bool,
    row_id: int,
    payload: Any,
) -> None:
    """Verify Advanced Alchemy 1.13.0b2 ORA_JSONB/JsonB DDL, DBAPI BLOB types, and RETURNING round-trips."""
    sync_dbapi = SyncDBAPIModule(dialect="oracle")
    async_dbapi = AsyncDBAPIModule(dialect="oracle")
    sa_async_dbapi = SQLAlchemyAsyncDBAPI(dialect="oracle")
    for mod in (sync_dbapi, async_dbapi, sa_async_dbapi):
        assert mod.BLOB is BLOB
        assert mod.BFILE is BFILE
        assert mod.CLOB is CLOB
        assert mod.NCLOB is NCLOB
        assert mod.FIXED_CHAR is FIXED_CHAR
        assert mod.BLOB == "BLOB"

    meta = MetaData()
    docs = Table(
        "aa_ora_json_docs",
        meta,
        Column("id", Integer, primary_key=True, autoincrement=False),
        Column("native_data", JsonB),
        Column("legacy_data", ORA_JSONB(use_blob=True)),
    )

    dialect_23 = SyncOracleDialect()
    dialect_23.server_version_info = (23, 4)
    ddl_23 = str(CreateTable(docs).compile(dialect=dialect_23)).upper()
    select_23 = str(select(docs.c.native_data, docs.c.legacy_data).compile(dialect=dialect_23)).upper()
    assert "NATIVE_DATA JSON" in ddl_23
    assert "LEGACY_DATA BLOB" in ddl_23
    assert "LEGACY_DATA IS JSON" in ddl_23
    assert "JSON_SERIALIZE(AA_ORA_JSON_DOCS.NATIVE_DATA RETURNING BLOB)" in select_23

    dialect_19 = SyncOracleDialect()
    dialect_19.server_version_info = (19, 0)
    ddl_19 = str(CreateTable(docs).compile(dialect=dialect_19)).upper()
    assert "NATIVE_DATA BLOB" in ddl_19
    assert "NATIVE_DATA IS JSON" in ddl_19

    ins_stmt = (
        docs.insert()
        .values(id=row_id, native_data=payload, legacy_data=payload)
        .returning(docs.c.native_data, docs.c.legacy_data)
    )
    sel_stmt = select(docs.c.native_data, docs.c.legacy_data).where(docs.c.id == row_id)

    if not asynchronous:
        sync_client = _OracleJsonBlobClient()
        sync_driver = SyncOracleDriver(OracleConfig(), client=sync_client)
        sync_engine = create_engine(
            "oracle+vtb://scott:tiger@localhost:1521/freepdb1",
            creator=lambda: SyncDBAPIConnection("oracle+vtb://localhost", driver=sync_driver),
        )
        try:
            with sync_engine.begin() as conn:
                ret_row = conn.execute(ins_stmt).one()
                assert ret_row.native_data == payload
                assert ret_row.legacy_data == payload
                fetched_row = conn.execute(sel_stmt).one()
                assert fetched_row.native_data == payload
                assert fetched_row.legacy_data == payload
            exec_calls = [data for kind, data in sync_client.calls if kind == "execute"]
            assert len(exec_calls) == 1
            assert exec_calls[0]["out_binds"] == {"ret_0": "BLOB", "ret_1": "BLOB"}
        finally:
            sync_engine.dispose()
        return

    async_stub = _StubAsyncOracleClient()
    async_stub._sync = _OracleJsonBlobClient()
    async_driver = AsyncOracleDriver(OracleConfig(), client=async_stub)
    async_engine = create_async_engine(
        "oracle+vtb_async://scott:tiger@localhost:1521/freepdb1",
        async_creator=lambda: AsyncDBAPIConnection("oracle+vtb_async://localhost", driver=async_driver),
    )
    try:
        async with async_engine.begin() as aconn:
            aret_row = (await aconn.execute(ins_stmt)).one()
            assert aret_row.native_data == payload
            assert aret_row.legacy_data == payload
            afetched_row = (await aconn.execute(sel_stmt)).one()
            assert afetched_row.native_data == payload
            assert afetched_row.legacy_data == payload
        aexec_calls = [data for kind, data in async_stub.calls if kind == "execute"]
        assert len(aexec_calls) == 1
        assert aexec_calls[0]["out_binds"] == {"ret_0": "BLOB", "ret_1": "BLOB"}
    finally:
        await async_engine.dispose()


@pytest.mark.parametrize(
    "url",
    ["sqlite+vtb:///:memory:", "duckdb+vtb:///:memory:"],
    ids=["sqlite", "duckdb"],
)
def test_sqlalchemy_sync_engines_run_on_native_cursor(url: str) -> None:
    """Verify sync sqlite+vtb and duckdb+vtb engines execute on native cursors with rollback support."""
    engine = create_engine(url)
    try:
        raw_conn = engine.raw_connection()
        try:
            raw_cursor = raw_conn.cursor()
            raw_cursor.close()
        finally:
            raw_conn.close()

        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE native_items (id INTEGER, name VARCHAR(64))"))
            conn.execute(text("INSERT INTO native_items (id, name) VALUES (1, 'alpha')"))
            conn.execute(text("INSERT INTO native_items (id, name) VALUES (2, 'beta')"))

        with engine.connect() as conn:
            rows = conn.execute(text("SELECT id, name FROM native_items ORDER BY id")).fetchall()
            assert rows == [(1, "alpha"), (2, "beta")]

            conn.execute(text("INSERT INTO native_items (id, name) VALUES (3, 'rolled_back')"))
            conn.rollback()

            after_rollback = conn.execute(text("SELECT id, name FROM native_items ORDER BY id")).fetchall()
            assert after_rollback == [(1, "alpha"), (2, "beta")]
    finally:
        engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "url",
    ["sqlite+vtb_async:///:memory:", "duckdb+vtb_async:///:memory:"],
    ids=["sqlite", "duckdb"],
)
async def test_sqlalchemy_async_engines_run_on_native_cursor(url: str) -> None:
    """Verify asyncio sqlite+vtb_async and duckdb+vtb_async engines execute on native cursors with rollback support."""
    engine = create_async_engine(url)
    try:
        async with engine.connect() as conn:
            raw_conn = await conn.get_raw_connection()
            driver_conn: Any = raw_conn.driver_connection
            assert driver_conn is not None
            native_cursor = driver_conn.cursor()
            assert type(native_cursor).__name__ in {"AsyncSQLiteCursor", "AsyncDuckDBCursor"}
            await native_cursor.close()

        async with engine.begin() as conn:
            await conn.execute(text("CREATE TABLE native_async_items (id INTEGER, name VARCHAR(64))"))
            await conn.execute(text("INSERT INTO native_async_items (id, name) VALUES (1, 'alpha')"))
            await conn.execute(text("INSERT INTO native_async_items (id, name) VALUES (2, 'beta')"))

        async with engine.connect() as conn:
            rows = (await conn.execute(text("SELECT id, name FROM native_async_items ORDER BY id"))).fetchall()
            assert rows == [(1, "alpha"), (2, "beta")]

            await conn.execute(text("INSERT INTO native_async_items (id, name) VALUES (3, 'rolled_back')"))
            await conn.rollback()

            after_rollback = (
                await conn.execute(text("SELECT id, name FROM native_async_items ORDER BY id"))
            ).fetchall()
            assert after_rollback == [(1, "alpha"), (2, "beta")]
    finally:
        await engine.dispose()


def test_sqlalchemy_native_cursor_integrity_error_and_lastrowid() -> None:
    """Verify sqlite+vtb surfaces sqlalchemy.exc.IntegrityError and populated inserted_primary_key."""
    metadata = MetaData()
    items = Table(
        "native_pk_items",
        metadata,
        Column("id", Integer, primary_key=True, autoincrement=True),
        Column("name", String(64), nullable=False),
    )

    engine = create_engine("sqlite+vtb:///:memory:")
    try:
        with engine.begin() as conn:
            metadata.create_all(conn)

        with engine.begin() as conn:
            first_res = conn.execute(items.insert().values(name="alpha"))
            assert first_res.inserted_primary_key == (1,)
            second_res = conn.execute(items.insert().values(name="beta"))
            assert second_res.inserted_primary_key == (2,)

        with engine.connect() as conn:
            with pytest.raises(sa_exc.IntegrityError):
                conn.execute(items.insert().values(id=1, name="duplicate"))
            conn.rollback()
    finally:
        engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True], ids=["sync", "async"])
async def test_oracle_dbapi_native_cursor_type_and_out_bind_var(asynchronous: bool) -> None:
    """Verify Oracle DB-API connections return native cursors and cursor.var(NUMBER) yields OracleOutBindVar."""

    class _OutBindBlockClient(_StubSyncOracleClient):
        def execute(
            self,
            sql: str,
            params: Any = None,
            *,
            out_binds: Any = None,
            fetch_decimals: bool = False,
            enable_lowercase_column_names: bool | None = None,
        ) -> _oracle_native.OracleQueryResult:
            _ = (fetch_decimals, enable_lowercase_column_names)
            self.calls.append(("execute", {"sql": sql, "params": params, "out_binds": out_binds}))
            return _oracle_native.OracleQueryResult([], [], rowcount=1, out_binds={"out_val": 99})

    if not asynchronous:
        sync_driver = SyncOracleDriver(OracleConfig(), client=_OutBindBlockClient())
        conn = SyncDBAPIConnection(
            "oracle+vtb://scott:test-token-placeholder@localhost:1521/freepdb1",
            driver=sync_driver,
            share_pool=False,
        )
        try:
            cur = conn.cursor()
            out_var = cur.var(NUMBER)
            assert isinstance(out_var, _oracle_native.OracleOutBindVar)
            cur.execute("BEGIN :out_val := 99; END;", {"out_val": out_var})
            assert out_var.getvalue() == 99
        finally:
            conn.close()
        return

    async_stub = _StubAsyncOracleClient()
    async_stub._sync = _OutBindBlockClient()
    async_driver = AsyncOracleDriver(OracleConfig(), client=async_stub)
    async_conn = AsyncDBAPIConnection(
        "oracle+vtb_async://scott:test-token-placeholder@localhost:1521/freepdb1",
        driver=async_driver,
        share_pool=False,
    )
    try:
        acur = async_conn.cursor()
        aout_var = acur.var(NUMBER)
        assert isinstance(aout_var, _oracle_native.OracleOutBindVar)
        await acur.execute("BEGIN :out_val := 99; END;", {"out_val": aout_var})
        assert aout_var.getvalue() == 99
    finally:
        await async_conn.close()


def test_sqlite_vtb_engine_cursor_is_native_cursor() -> None:
    """Verify sqlite+vtb raw_connection().cursor() returns SyncSQLiteCursor and execute scalar works."""
    engine = create_engine("sqlite+vtb:///:memory:")
    try:
        raw_conn = engine.raw_connection()
        try:
            raw_cur = raw_conn.cursor()
            try:
                assert isinstance(raw_cur, SyncSQLiteCursor)
            finally:
                raw_cur.close()
        finally:
            raw_conn.close()

        with engine.connect() as connection:
            assert connection.execute(text("select 1")).scalar() == 1
    finally:
        engine.dispose()


@pytest.mark.anyio
async def test_asyncio_adapter_receives_native_async_cursor() -> None:
    """Verify SQLAlchemy asyncio adapter holds an exact AsyncSQLiteCursor and executes queries."""
    driver = AsyncSQLiteDriver()
    module = SQLAlchemyAsyncDBAPI(dialect="sqlite")
    adapted = await greenlet_spawn(module.connect, driver=driver, share_pool=False)
    try:
        cursor = await greenlet_spawn(adapted.cursor)
        try:
            assert type(cast("Any", cursor._cursor)) is AsyncSQLiteCursor
            await greenlet_spawn(cursor.execute, "select 1")
            assert await greenlet_spawn(cursor.fetchall) == [(1,)]
        finally:
            await greenlet_spawn(cursor.close)
    finally:
        await greenlet_spawn(adapted.close)
