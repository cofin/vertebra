"""Unit and integration tests for SQLAlchemy 2.1 +vtb dialects, types, Alembic, and Advanced Alchemy."""

from __future__ import annotations

import array
import importlib
import sqlite3
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.repository import SQLAlchemyAsyncRepository
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService
from advanced_alchemy.types.file_object import FileObject, StoredObject
from sqlalchemy import String, create_engine, delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.util.concurrency import greenlet_spawn
from sqlspec import SQL, StatementStack, sql
from sqlspec.core.filters import LimitOffsetFilter, NullFilter
from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.core.statement import StatementConfig
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import StackExecutionError

import vertebra.extensions.sqlspec as sqlspec_extension
from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra._vertebra import LeaseBroker
from vertebra.db import DatabaseRegistry
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
from vertebra.db.dbapi import AsyncDBAPIConnection, InterfaceError, SyncDBAPIConnection, SyncDBAPIModule
from vertebra.extensions.alchemy import (
    ObjectStoreBackend,
    create_alchemy_plugin,
    create_async_alchemy_config,
    create_sync_alchemy_config,
)
from vertebra.extensions.broker.errors import LeaseQuarantinedError
from vertebra.extensions.broker.transport import AsyncSHMBrokerTransport, BrokerConfig, SyncSHMBrokerTransport
from vertebra.extensions.sqlalchemy import (
    AlembicImpl,
    BinaryJSON,
    CreateVectorIndexOp,
    CreateViewOp,
    DeleteUsingClause,
    DropVectorIndexOp,
    InsertSentinelSpec,
    MySQLAlembicImpl,
    OracleAlembicImpl,
    ParameterizedSQL,
    PostgreSQLAlembicImpl,
    SelectInLoadSpec,
    SQLiteAlembicImpl,
    SyncEngineAdapter,
    SyncMySQLDialect,
    SyncOracleDialect,
    SyncPostgreSQLDialect,
    SyncSQLiteDialect,
    Vector,
    dialects,
)
from vertebra.extensions.sqlalchemy.async_adapter import SQLAlchemyAsyncDBAPI
from vertebra.extensions.sqlalchemy.pool import AsyncBrokerPool, BrokerPool, broker_creator
from vertebra.extensions.sqlspec.adapters import (
    MySQLAsyncConfig,
    MySQLSyncConfig,
    PostgreSQLAsyncConfig,
    PostgreSQLSyncConfig,
    SQLiteAsyncDriver,
    SQLiteSyncDriver,
)
from vertebra.extensions.sqlspec.adapters import (
    MySQLAsyncDriver as SQLSpecMySQLAsyncDriver,
)
from vertebra.extensions.sqlspec.adapters import (
    MySQLSyncDriver as SQLSpecMySQLSyncDriver,
)
from vertebra.extensions.sqlspec.adapters import (
    PostgreSQLAsyncDriver as SQLSpecPostgreSQLAsyncDriver,
)
from vertebra.extensions.sqlspec.adapters import (
    PostgreSQLSyncDriver as SQLSpecPostgreSQLSyncDriver,
)
from vertebra.extensions.sqlspec.plugin import ProxyAsyncConfig, ProxySyncConfig
from vertebra.extensions.sqlspec.provider import (
    AsyncBrokerLeaseSource,
    BrokerRpcRequest,
    BrokerRpcResponse,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
    SyncBrokerLeaseSource,
)
from vertebra.extensions.sqlspec.proxy import ProxyAsyncDriver, ProxySyncDriver
from vertebra.storage import SyncObjectStore
from vertebra.typing import ImproperConfigurationError


@pytest.mark.parametrize("dialect_name", ["PostgreSQL", "SQLite", "MySQL", "Oracle", "MSSQL", "DuckDB"])
def test_sync_dialect_begins_native_transaction(dialect_name: str) -> None:
    """Dialect begin reaches the native driver before writes and permits rollback."""
    driver = SyncSQLiteDriver()
    driver.execute("CREATE TABLE transaction_items (id INTEGER)")
    conn = SyncDBAPIConnection(driver=driver, share_pool=False)
    try:
        getattr(dialects, f"Sync{dialect_name}Dialect")().do_begin(conn)
        conn.cursor().execute("INSERT INTO transaction_items VALUES (?)", (1,))
        conn.rollback()
        assert driver.select_value("SELECT COUNT(*) FROM transaction_items") == 0
    finally:
        conn.close()


@pytest.mark.anyio
@pytest.mark.parametrize("dialect_name", ["PostgreSQL", "SQLite", "MySQL", "Oracle", "MSSQL", "DuckDB"])
async def test_async_dialect_begins_native_transaction(dialect_name: str) -> None:
    """Async dialect begin forwards through SQLAlchemy's real adapted DBAPI connection."""
    driver = AsyncSQLiteDriver()
    await driver.execute("CREATE TABLE transaction_items (id INTEGER)")
    module = SQLAlchemyAsyncDBAPI()
    adapted = await greenlet_spawn(module.connect, driver=driver, share_pool=False)
    try:
        await greenlet_spawn(getattr(dialects, f"Async{dialect_name}Dialect")().do_begin, adapted)
        cursor = adapted.driver_connection.cursor()
        await cursor.execute("INSERT INTO transaction_items VALUES (?)", (1,))
        await adapted.driver_connection.rollback()
        assert await driver.select_value("SELECT COUNT(*) FROM transaction_items") == 0
    finally:
        await adapted.driver_connection.close()


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
        conn.execute(ItemRecord.__table__.insert().values(id=1, name="first"))
        conn.commit()

    with engine.connect() as conn:
        rows = conn.execute(select(ItemRecord).where(ItemRecord.id == 1)).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "first"

    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(ItemRecord.__table__.insert().values(id=2, name="rolled_back"))
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


def test_vector_and_binary_json_types_across_dialects() -> None:
    """Verify Vector and BinaryJSON DDL compilation, bind processing, and distance expressions."""
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

    cos_expr = vec_type.cosine_distance([1.0, 0.0], param_name="qvec")
    assert cos_expr.compile_sql("oracle+vtb") == "VECTOR_DISTANCE(embedding, :qvec, COSINE)"
    assert cos_expr.compile_sql("postgresql+vtb") == "embedding <=> :qvec"
    assert cos_expr.compile_sql("mysql+vtb") == "DISTANCE_COSINE(embedding, :qvec)"

    bjson = BinaryJSON()
    assert bjson.get_col_spec("postgresql+vtb") == "JSONB"
    assert bjson.get_col_spec("sqlite+vtb") == "JSONB"
    assert bjson.get_col_spec("oracle+vtb") == "JSON"
    assert bjson.process_result_value(memoryview(b'\x01{"a":1}'), "postgresql+vtb") == {"a": 1}


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
    """Verify +vtb dialect registration, DBAPI cursor, and Alembic DefaultImpl integrations."""
    impls = AlembicImpl.register_all()
    assert "postgresql+vtb" in impls
    assert "oracle+vtb" in impls
    assert "sqlite+vtb" in impls
    assert issubclass(PostgreSQLAlembicImpl, AlembicImpl.__bases__[0])
    assert issubclass(SQLiteAlembicImpl, AlembicImpl.__bases__[0])
    assert issubclass(OracleAlembicImpl, AlembicImpl.__bases__[0])
    assert issubclass(MySQLAlembicImpl, AlembicImpl.__bases__[0])

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
    rendered_vec = AlembicImpl.render_item("type", Vector(1536), ctx)
    assert rendered_vec == "Vector(1536, storage_format='float32')"
    assert "from vertebra.extensions.sqlalchemy import Vector" in ctx.imports
    assert AlembicImpl.compare_type(None, None, None, "VECTOR(1536)", Vector(1536)) is False
    assert AlembicImpl.compare_type(None, None, None, "JSONB", BinaryJSON()) is False

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
    assert AlembicImpl.resolve_migration_url(pg_cfg) == "postgresql://u:p@pg-primary:5432/app"

    sentinel_spec = InsertSentinelSpec.create(table_name="orders", sentinel_column="_insert_sentinel")
    rendered_insert = SyncOracleDialect.compile_insert_returning_sentinel(
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

    ddl_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:")
    AlembicImpl.execute_ddl(ddl_conn, view_op, dialect_name="sqlite+vtb")
    ddl_conn.close()


def test_postgres_vtb_dialect_dbapi_and_compiler_features() -> None:
    """Verify SyncPostgreSQLDialect, SyncDBAPIModule paramstyles, compiler hooks, and cursor descriptions."""
    pg_dbapi = SyncPostgreSQLDialect.dbapi()
    assert SyncPostgreSQLDialect.default_paramstyle == "numeric_dollar"
    assert SyncPostgreSQLDialect.paramstyle == "numeric_dollar"
    assert pg_dbapi.paramstyle == "numeric_dollar"
    assert SyncPostgreSQLDialect.import_dbapi().paramstyle == "numeric_dollar"
    assert SyncSQLiteDialect.dbapi().paramstyle == "qmark"
    assert SyncMySQLDialect.dbapi().paramstyle == "format"
    assert SyncOracleDialect.dbapi().paramstyle == "named"

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

    sentinel_spec = InsertSentinelSpec.create(table_name="orders", sentinel_column="_vtb_sentinel")
    rendered_insert = SyncPostgreSQLDialect.compile_insert_returning_sentinel(
        "orders",
        ["id", "name"],
        sentinel_spec,
        pk_column="id",
    )
    assert rendered_insert == "INSERT INTO orders (id, name) VALUES (:id, :name) RETURNING id, _vtb_sentinel"

    del_clause = DeleteUsingClause.create(
        "orders",
        "archived_customers",
        where_sql="orders.customer_id = archived_customers.id",
    )
    assert SyncPostgreSQLDialect.compile_delete_using(del_clause) == (
        "DELETE FROM orders USING archived_customers WHERE orders.customer_id = archived_customers.id"
    )

    vec = Vector(768)
    bjson = BinaryJSON()
    assert SyncPostgreSQLDialect.type_descriptor(vec) is vec
    assert SyncPostgreSQLDialect.type_descriptor(bjson) is bjson


@pytest.mark.anyio
async def test_sqlalchemy_21_orm_optimizations_and_sync_engine_adapter() -> None:
    """Verify SelectInLoadSpec, InsertSentinelSpec, DeleteUsingClause, ParameterizedSQL, and SyncEngineAdapter."""
    load_spec = SelectInLoadSpec.create("items", chunksize=500, omit_join=True)
    assert load_spec.chunksize == 500
    assert load_spec.omit_join is True
    assert load_spec.num_batches(1200) == 3

    sentinel = InsertSentinelSpec.create()
    assert sentinel.use_insertmanyvalues is True
    assert sentinel.insert_executemany_returning is True

    del_clause = DeleteUsingClause.create(
        "orders",
        "archived_customers",
        where_sql="orders.customer_id = archived_customers.id",
    )
    assert del_clause.to_sql() == (
        "DELETE FROM orders USING archived_customers WHERE orders.customer_id = archived_customers.id"
    )

    compiled = ParameterizedSQL.from_template(
        ["SELECT * FROM items WHERE category = ", " AND price <= ", ""],
        ["coffee", 25.0],
    )
    assert compiled.sql == "SELECT * FROM items WHERE category = :p_1 AND price <= :p_2"
    assert compiled.params == {"p_1": "coffee", "p_2": 25.0}

    adapter = SyncEngineAdapter(max_workers=2)
    result = await adapter.run_sync(lambda x, y: x + y, 20, 22)
    assert result == 42


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
async def test_pep249_sync_and_async_dbapi_cursor_seven_element_descriptions() -> None:
    """Verify SyncDBAPICursor and AsyncDBAPICursor synthesize 7-item PEP 249 descriptions from column names."""

    class _TupleColSyncDriver:
        def execute_with_description(
            self, sql: str, parameters: object = None
        ) -> tuple[list[tuple[object, ...]], tuple[str, ...]]:
            _ = (sql, parameters)
            return [(1, "alpha")], ("id", "name")

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    class _TupleColAsyncDriver:
        async def execute_with_description(
            self,
            sql: str,
            parameters: object = None,
        ) -> tuple[list[tuple[object, ...]], tuple[str, ...]]:
            _ = (sql, parameters)
            return [(2, "beta")], ("id", "name")

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            return None

    sync_conn = SyncDBAPIConnection("sqlite+vtb:///:memory:", driver=_TupleColSyncDriver())
    sync_cur = sync_conn.cursor()
    sync_cur.execute("SELECT id, name FROM items")
    assert sync_cur.description == (
        ("id", None, None, None, None, None, None),
        ("name", None, None, None, None, None, None),
    )
    assert sync_cur.fetchall() == [(1, "alpha")]
    sync_conn.close()

    async_conn = AsyncDBAPIConnection("sqlite+vtb_async:///:memory:", driver=_TupleColAsyncDriver())
    async_cur = async_conn.cursor()
    await async_cur.execute("SELECT id, name FROM items")
    assert async_cur.description == (
        ("id", None, None, None, None, None, None),
        ("name", None, None, None, None, None, None),
    )
    assert await async_cur.fetchall() == [(2, "beta")]
    await async_conn.close()


@pytest.mark.anyio
async def test_pep249_sync_and_async_dbapi_dml_rowcount_routing() -> None:
    """Verify non-row-returning UPDATE and DELETE statements report affected rowcount == 1 across sync and async cursors."""

    class _DmlTrackingSyncDriver:
        def __init__(self) -> None:
            self.executed: list[str] = []

        def execute(self, sql: str, parameters: object = None) -> int:
            _ = parameters
            self.executed.append(sql)
            return 1

        def execute_many(self, sql: str, seq_of_parameters: object) -> int:
            self.executed.append(sql)
            return len(list(seq_of_parameters)) if isinstance(seq_of_parameters, (list, tuple)) else 0

        def execute_with_description(
            self,
            sql: str,
            parameters: object = None,
        ) -> tuple[list[tuple[object, ...]], tuple[str, ...]]:
            _ = (sql, parameters)
            return [(1,)], ("id",)

        def close(self) -> None:
            return None

    class _DmlTrackingAsyncDriver:
        def __init__(self) -> None:
            self.executed: list[str] = []

        async def execute(self, sql: str, parameters: object = None) -> int:
            _ = parameters
            self.executed.append(sql)
            return 1

        async def execute_many(self, sql: str, seq_of_parameters: object) -> int:
            self.executed.append(sql)
            return len(list(seq_of_parameters)) if isinstance(seq_of_parameters, (list, tuple)) else 0

        async def execute_with_description(
            self,
            sql: str,
            parameters: object = None,
        ) -> tuple[list[tuple[object, ...]], tuple[str, ...]]:
            _ = (sql, parameters)
            return [(1,)], ("id",)

        async def close(self) -> None:
            return None

    raw_sync = SyncDBAPIConnection("sqlite+vtb:///:memory:")
    raw_cur = raw_sync.cursor()
    raw_cur.execute("CREATE TABLE dml_items (id INTEGER PRIMARY KEY, label TEXT)")
    raw_cur.execute("INSERT INTO dml_items (id, label) VALUES (?, ?)", (1, "v1"))
    raw_cur.execute("UPDATE dml_items SET label = 'RETURNING' WHERE id = ?", (1,))
    assert raw_cur.rowcount == 1
    raw_cur.execute("DELETE FROM dml_items WHERE id = ?", (1,))
    assert raw_cur.rowcount == 1
    raw_sync.close()

    sync_drv = _DmlTrackingSyncDriver()
    drv_sync_conn = SyncDBAPIConnection("postgresql+vtb://localhost:5432/app", driver=sync_drv)
    drv_sync_cur = drv_sync_conn.cursor()
    drv_sync_cur.execute("UPDATE items SET name = 'RETURNING' WHERE id = 1")
    assert drv_sync_cur.rowcount == 1
    assert drv_sync_cur.description is None
    drv_sync_cur.execute("DELETE FROM items WHERE id = 1")
    assert drv_sync_cur.rowcount == 1
    drv_sync_cur.execute("INSERT INTO items (name) VALUES ('a') RETURNING id")
    assert drv_sync_cur.rowcount == 1
    assert drv_sync_cur.fetchall() == [(1,)]
    assert len(sync_drv.executed) == 2
    drv_sync_conn.close()

    async_drv = _DmlTrackingAsyncDriver()
    drv_async_conn = AsyncDBAPIConnection("postgresql+vtb_async://localhost:5432/app", driver=async_drv)
    drv_async_cur = drv_async_conn.cursor()
    await drv_async_cur.execute("UPDATE items SET name = 'RETURNING' WHERE id = 1")
    assert drv_async_cur.rowcount == 1
    assert drv_async_cur.description is None
    await drv_async_cur.execute("DELETE FROM items WHERE id = 1")
    assert drv_async_cur.rowcount == 1
    await drv_async_cur.executemany("UPDATE items SET name = ? WHERE id = ?", [("a", 1), ("b", 2)])
    assert drv_async_cur.rowcount == 2
    assert len(async_drv.executed) == 3
    await drv_async_conn.close()


class _SyncPipelinePort(SyncSQLiteDriver):
    """SQLite database port exposing the native pipeline protocol and queued operations."""

    def __init__(self, *, result_count: int | None = None) -> None:
        super().__init__()
        self.pipeline_calls = 0
        self.result_count = result_count
        self.queued: list[tuple[str, str, object]] = []

    def pipeline(self) -> Any:
        self.pipeline_calls += 1
        port = self

        class Pipeline:
            def __enter__(self) -> Any:
                port.begin()
                return self

            def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                if exc_type:
                    port.rollback()
                else:
                    port.commit()

            def select(self, sql: str, parameters: Any = None) -> None:
                port.queued.append(("select", sql, parameters))

            def execute(self, sql: str, parameters: Any = None) -> None:
                port.queued.append(("execute", sql, parameters))

            def results(self) -> list[Any]:
                results = [getattr(port, method)(sql, params) for method, sql, params in port.queued]
                return results[: port.result_count]

        return Pipeline()


class _AsyncPipelinePort(AsyncSQLiteDriver):
    """Async SQLite database port exposing native pipeline ordering and transactions."""

    def __init__(self, *, result_count: int | None = None) -> None:
        super().__init__()
        self.pipeline_calls = 0
        self.result_count = result_count
        self.queued: list[tuple[str, str, object]] = []

    def pipeline(self) -> Any:
        self.pipeline_calls += 1
        port = self

        class Pipeline:
            async def __aenter__(self) -> Any:
                await port.begin()
                return self

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                if exc_type:
                    await port.rollback()
                else:
                    await port.commit()

            def select(self, sql: str, parameters: Any = None) -> None:
                port.queued.append(("select", sql, parameters))

            def execute(self, sql: str, parameters: Any = None) -> None:
                port.queued.append(("execute", sql, parameters))

            async def results(self) -> list[Any]:
                results = [await getattr(port, method)(sql, params) for method, sql, params in port.queued]
                return results[: port.result_count]

        return Pipeline()


@pytest.mark.parametrize("pipeline_capable", [False, True])
def test_sqlspec_stack_ordered_results_and_binds(pipeline_capable: bool) -> None:
    """The SQLSpec adapter preserves independent rows, DML counts, and SQL-bound parameters."""
    native = _SyncPipelinePort() if pipeline_capable else SyncSQLiteDriver()
    native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteSyncDriver(native)
    try:
        result = driver.execute_stack(
            StatementStack()
            .push_execute("INSERT INTO stack_items VALUES (:id)", id=7)
            .push_execute(SQL("SELECT id FROM stack_items WHERE id = :id", id=7))
            .push_execute("WITH total AS (SELECT COUNT(*) AS n FROM stack_items) SELECT n FROM total")
        )
        assert [item.rows_affected for item in result] == [1, 1, 1]
        assert result[1].get_result().get_data() == [{"id": 7}]
        assert result[2].get_result().get_data() == [{"n": 1}]
        if pipeline_capable:
            assert native.pipeline_calls == 1
            assert [item[0] for item in native.queued] == ["execute", "select", "select"]
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("pipeline_capable", [False, True])
async def test_sqlspec_async_stack_ordered_results_and_binds(pipeline_capable: bool) -> None:
    """Async SQLSpec stack results retain every row set and bound SQL value in order."""
    native = _AsyncPipelinePort() if pipeline_capable else AsyncSQLiteDriver()
    await native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteAsyncDriver(native)
    try:
        result = await driver.execute_stack(
            StatementStack()
            .push_execute("INSERT INTO stack_items VALUES (:id)", id=7)
            .push_execute(SQL("SELECT id FROM stack_items WHERE id = :id", id=7))
            .push_execute("WITH total AS (SELECT COUNT(*) AS n FROM stack_items) SELECT n FROM total")
        )
        assert [item.rows_affected for item in result] == [1, 1, 1]
        assert result[1].get_result().get_data() == [{"id": 7}]
        assert result[2].get_result().get_data() == [{"n": 1}]
        if pipeline_capable:
            assert native.pipeline_calls == 1
            assert [item[0] for item in native.queued] == ["execute", "select", "select"]
    finally:
        await native.close()


@pytest.mark.parametrize("pipeline_capable", [False, True])
@pytest.mark.parametrize("continue_on_error", [False, True])
def test_sqlspec_stack_errors_roll_back_or_continue(continue_on_error: bool, pipeline_capable: bool) -> None:
    """Failed statements roll back fail-fast work or preserve subsequent independent operations."""
    native = _SyncPipelinePort() if pipeline_capable else SyncSQLiteDriver()
    native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteSyncDriver(native)
    stack = (
        StatementStack()
        .push_execute("INSERT INTO stack_items VALUES (1)")
        .push_execute("INSERT INTO missing_table VALUES (2)")
        .push_execute("INSERT INTO stack_items VALUES (3)")
    )
    try:
        if continue_on_error:
            result = driver.execute_stack(stack, continue_on_error=True)
            assert len(result) == 3
            assert result[1].is_error()
            assert result[0].rows_affected == result[2].rows_affected == 1
            assert native.select("SELECT id FROM stack_items ORDER BY id") == [{"id": 1}, {"id": 3}]
            if pipeline_capable:
                assert native.pipeline_calls == 0
        else:
            with pytest.raises(Exception, match="missing_table"):
                driver.execute_stack(stack)
            assert native.select("SELECT id FROM stack_items") == []
            if pipeline_capable:
                assert native.pipeline_calls == 1
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("pipeline_capable", [False, True])
@pytest.mark.parametrize("continue_on_error", [False, True])
async def test_sqlspec_async_stack_errors_roll_back_or_continue(
    continue_on_error: bool, pipeline_capable: bool
) -> None:
    """Async failures propagate or occupy their ordered error slot while later work completes."""
    native = _AsyncPipelinePort() if pipeline_capable else AsyncSQLiteDriver()
    await native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteAsyncDriver(native)
    stack = (
        StatementStack()
        .push_execute("INSERT INTO stack_items VALUES (1)")
        .push_execute("INSERT INTO missing_table VALUES (2)")
        .push_execute("INSERT INTO stack_items VALUES (3)")
    )
    try:
        if continue_on_error:
            result = await driver.execute_stack(stack, continue_on_error=True)
            assert len(result) == 3
            assert result[1].is_error()
            assert result[0].rows_affected == result[2].rows_affected == 1
            assert await native.select("SELECT id FROM stack_items ORDER BY id") == [{"id": 1}, {"id": 3}]
            if pipeline_capable:
                assert native.pipeline_calls == 0
        else:
            with pytest.raises(Exception, match="missing_table"):
                await driver.execute_stack(stack)
            assert await native.select("SELECT id FROM stack_items") == []
            if pipeline_capable:
                assert native.pipeline_calls == 1
    finally:
        await native.close()


def test_query_pipeline_public_surface_removed() -> None:
    """The removed CTE compiler has no import path or SQLSpec re-export."""
    with pytest.raises(ModuleNotFoundError, match="vertebra.db.pipeline"):
        importlib.import_module("vertebra.db.pipeline")
    assert not hasattr(sqlspec_extension, "SyncQueryPipeline")
    assert not hasattr(sqlspec_extension, "AsyncQueryPipeline")


@pytest.mark.parametrize("pipeline_capable", [False, True])
def test_sqlspec_stack_uses_processed_row_metadata(pipeline_capable: bool) -> None:
    """Comments, WITH DML, and RETURNING retain upstream row-producing classification."""
    native = _SyncPipelinePort() if pipeline_capable else SyncSQLiteDriver()
    native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteSyncDriver(native)
    try:
        result = driver.execute_stack(
            StatementStack()
            .push_execute("/* insert result */ INSERT INTO stack_items VALUES (7) RETURNING id")
            .push_execute("WITH x AS (SELECT 1) UPDATE stack_items SET id = 9")
            .push_execute("/* independent read */ SELECT id FROM stack_items")
        )
        assert result[0].get_result().get_data() == [{"id": 7}]
        assert result[0].result_type == "INSERT"
        assert result[0].get_result().statement.operation_type == "INSERT"
        assert result[1].rows_affected == 0
        assert result[2].get_result().get_data() == [{"id": 9}]
        assert result[2].result_type == "SELECT"
        assert result[2].get_result().statement.operation_type == "SELECT"
        if pipeline_capable:
            assert [item[0] for item in native.queued] == ["select", "execute", "select"]
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("pipeline_capable", [False, True])
async def test_sqlspec_async_stack_uses_processed_row_metadata(pipeline_capable: bool) -> None:
    """Async dispatch follows SQLSpec row metadata through comments and mutations."""
    native = _AsyncPipelinePort() if pipeline_capable else AsyncSQLiteDriver()
    await native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteAsyncDriver(native)
    try:
        result = await driver.execute_stack(
            StatementStack()
            .push_execute("/* insert result */ INSERT INTO stack_items VALUES (7) RETURNING id")
            .push_execute("WITH x AS (SELECT 1) UPDATE stack_items SET id = 9")
            .push_execute("/* independent read */ SELECT id FROM stack_items")
        )
        assert result[0].get_result().get_data() == [{"id": 7}]
        assert result[0].result_type == "INSERT"
        assert result[0].get_result().statement.operation_type == "INSERT"
        assert result[1].rows_affected == 0
        assert result[2].get_result().get_data() == [{"id": 9}]
        assert result[2].result_type == "SELECT"
        assert result[2].get_result().statement.operation_type == "SELECT"
        if pipeline_capable:
            assert [item[0] for item in native.queued] == ["select", "execute", "select"]
    finally:
        await native.close()


def test_sqlspec_stack_non_execute_methods_use_sequential_fallback() -> None:
    """Batch, script, Arrow, and single-operation stacks retain their adapter result types."""
    native = _SyncPipelinePort()
    native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteSyncDriver(native)
    try:
        result = driver.execute_stack(
            StatementStack()
            .push_execute_many("INSERT INTO stack_items VALUES (?)", [(1,), (2,)])
            .push_execute_script("INSERT INTO stack_items VALUES (3); INSERT INTO stack_items VALUES (4)")
            .push_execute_arrow("SELECT id FROM stack_items ORDER BY id")
        )
        assert result[0].rows_affected == 2
        assert result[1].result_type == "SCRIPT"
        assert result[2].is_arrow_result()
        assert result[2].get_result().data.to_pylist() == [{"id": i} for i in range(1, 5)]
        single = driver.execute_stack(StatementStack().push_execute("SELECT COUNT(*) AS n FROM stack_items"))
        assert single[0].get_result().get_data() == [{"n": 4}]
        assert native.pipeline_calls == 0
    finally:
        native.close()


@pytest.mark.anyio
async def test_sqlspec_async_stack_non_execute_methods_use_sequential_fallback() -> None:
    """Async batch, script, Arrow, and single-operation stacks bypass native pipelines."""
    native = _AsyncPipelinePort()
    await native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteAsyncDriver(native)
    try:
        result = await driver.execute_stack(
            StatementStack()
            .push_execute_many("INSERT INTO stack_items VALUES (?)", [(1,), (2,)])
            .push_execute_script("INSERT INTO stack_items VALUES (3); INSERT INTO stack_items VALUES (4)")
            .push_execute_arrow("SELECT id FROM stack_items ORDER BY id")
        )
        assert result[0].rows_affected == 2
        assert result[1].result_type == "SCRIPT"
        assert result[2].is_arrow_result()
        assert result[2].get_result().data.to_pylist() == [{"id": i} for i in range(1, 5)]
        single = await driver.execute_stack(StatementStack().push_execute("SELECT COUNT(*) AS n FROM stack_items"))
        assert single[0].get_result().get_data() == [{"n": 4}]
        assert native.pipeline_calls == 0
    finally:
        await native.close()


def test_sqlspec_pipeline_result_count_mismatch_rolls_back() -> None:
    """Missing native results propagate before the pipeline transaction commits."""
    native = _SyncPipelinePort(result_count=1)
    native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteSyncDriver(native)
    try:
        with pytest.raises(ValueError, match="zip"):
            driver.execute_stack(
                StatementStack()
                .push_execute("INSERT INTO stack_items VALUES (1)")
                .push_execute("SELECT id FROM stack_items")
            )
        assert native.select("SELECT id FROM stack_items") == []
    finally:
        native.close()


@pytest.mark.anyio
async def test_sqlspec_async_pipeline_result_count_mismatch_rolls_back() -> None:
    """Async native result omissions propagate and roll back queued writes."""
    native = _AsyncPipelinePort(result_count=1)
    await native.execute("CREATE TABLE stack_items (id INTEGER)")
    driver = SQLiteAsyncDriver(native)
    try:
        with pytest.raises(ValueError, match="zip"):
            await driver.execute_stack(
                StatementStack()
                .push_execute("INSERT INTO stack_items VALUES (1)")
                .push_execute("SELECT id FROM stack_items")
            )
        assert await native.select("SELECT id FROM stack_items") == []
    finally:
        await native.close()


class _NativeBindConnection:
    """DBAPI database port recording native placeholders before SQLite adaptation."""

    def __init__(self) -> None:
        self.raw = sqlite3.connect(":memory:")
        self.raw.execute("CREATE TABLE stack_items (id INTEGER)")
        self.calls: list[tuple[str, Any]] = []
        self._in_tx = False

    @property
    def in_transaction(self) -> bool:
        return self._in_tx

    def connection(self) -> Any:
        return self

    def cursor(self) -> Any:
        port = self
        cursor = self.raw.cursor()

        class Cursor:
            @property
            def description(self) -> Any:
                return cursor.description

            @property
            def rowcount(self) -> int:
                return cursor.rowcount

            def execute(self, sql: str, params: Any = None) -> None:
                port.calls.append((sql, params))
                adapted_sql, bound = port._adapt_sqlite(sql, params)
                cursor.execute(adapted_sql, bound)

            def fetchall(self) -> list[Any]:
                return cursor.fetchall()

            def close(self) -> None:
                cursor.close()

        return Cursor()

    @staticmethod
    def _adapt_sqlite(sql: str, params: Any = None) -> tuple[str, Any]:
        if "$1" in sql and params is not None:
            return sql, {str(index): value for index, value in enumerate(params, 1)}
        return sql.replace("%s", "?"), list(params) if isinstance(params, (list, tuple)) else (params or ())

    def execute(self, sql: str, params: Any = None) -> int:
        bound = list(params) if params is not None else []
        self.calls.append((sql, bound))
        adapted_sql, adapted_bound = self._adapt_sqlite(sql, bound)
        cur = self.raw.cursor()
        try:
            cur.execute(adapted_sql, adapted_bound)
            return int(cur.rowcount) if cur.rowcount >= 0 else 0
        finally:
            cur.close()

    def execute_many(self, sql: str, param_sets: Any) -> int:
        total = 0
        for params in param_sets:
            total += self.execute(sql, params)
        return total

    def execute_with_description(self, sql: str, params: Any = None) -> tuple[list[tuple[Any, ...]], list[str]]:
        bound = list(params) if params is not None else []
        self.calls.append((sql, bound))
        adapted_sql, adapted_bound = self._adapt_sqlite(sql, bound)
        cur = self.raw.cursor()
        try:
            cur.execute(adapted_sql, adapted_bound)
            rows = [tuple(row) for row in cur.fetchall()]
            cols = [str(col[0]) for col in (cur.description or ())]
            return rows, cols
        finally:
            cur.close()

    def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        rows, cols = _NativeBindConnection.execute_with_description(self, sql, params)
        return [dict(zip(cols, row, strict=False)) for row in rows]

    def begin(self) -> None:
        self._in_tx = True

    def savepoint(self, name: str) -> None:
        self.raw.execute(f"SAVEPOINT {name}")

    def release_savepoint(self, name: str) -> None:
        self.raw.execute(f"RELEASE SAVEPOINT {name}")

    def rollback_to_savepoint(self, name: str) -> None:
        self.raw.execute(f"ROLLBACK TO SAVEPOINT {name}")

    def commit(self) -> None:
        self._in_tx = False
        self.raw.commit()

    def rollback(self) -> None:
        self._in_tx = False
        self.raw.rollback()

    def close(self) -> None:
        self._in_tx = False
        self.raw.close()

    def __enter__(self) -> Any:
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


class _AsyncNativeBindConnection(_NativeBindConnection):
    """Async DBAPI port retaining actual SQL and bind observations."""

    def acquire(self) -> Any:
        return self

    def cursor(self) -> Any:
        cursor = super().cursor()

        class Cursor:
            @property
            def description(self) -> Any:
                return cursor.description

            @property
            def rowcount(self) -> int:
                return cursor.rowcount

            async def execute(self, sql: str, params: Any = None) -> None:
                cursor.execute(sql, params)

            async def fetchall(self) -> list[Any]:
                return cursor.fetchall()

            async def close(self) -> None:
                cursor.close()

            async def __aenter__(self) -> Any:
                return self

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                cursor.close()

        return Cursor()

    async def execute(self, sql: str, params: Any = None) -> int:
        return super().execute(sql, params)

    async def execute_many(self, sql: str, param_sets: Any) -> int:
        total = 0
        for params in param_sets:
            total += await self.execute(sql, params)
        return total

    async def execute_with_description(self, sql: str, params: Any = None) -> tuple[list[tuple[Any, ...]], list[str]]:
        return super().execute_with_description(sql, params)

    async def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        return super().fetch_all(sql, params)

    async def begin(self) -> None:
        super().begin()

    async def savepoint(self, name: str) -> None:
        super().savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        super().release_savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        super().rollback_to_savepoint(name)

    async def commit(self) -> None:
        super().commit()

    async def rollback(self) -> None:
        super().rollback()

    async def close(self) -> None:
        super().close()

    async def __aenter__(self) -> Any:
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        return None


@pytest.mark.parametrize("engine", ["postgres", "mysql"])
@pytest.mark.parametrize("config_defaults", [False, True])
def test_sqlspec_stack_preserves_native_dbapi_binds_and_rows(engine: str, config_defaults: bool) -> None:
    """Native driver execution retains placeholders, binds, row counts, and independent reads."""
    if engine == "postgres":
        cluster = FakePostgresCluster()
        cluster._conn.execute("CREATE TABLE stack_items (id INTEGER)")
        native: Any = SyncPostgreSQLDriver(
            PostgreSQLConfig(dsn="postgresql://localhost/db"),
            pool=FakeSyncPostgresPool(cluster),
        )
        config: Any = PostgreSQLSyncConfig(native.config)
        driver: Any = SQLSpecPostgreSQLSyncDriver(
            native, statement_config=config.statement_config if config_defaults else None
        )
        insert_sql = "INSERT INTO stack_items VALUES ($1)"
        update_sql = "UPDATE stack_items SET id = $1 WHERE id = $2"
        select_sql = "SELECT id FROM stack_items WHERE id = $1"
        try:
            result = driver.execute_stack(
                StatementStack().push_execute(insert_sql, 7).push_execute(update_sql, 9, 7).push_execute(select_sql, 9)
            )
            assert [item.rows_affected for item in result] == [1, 1, 1]
            assert result[2].get_result().get_data() == [{"id": 9}]
            assert [(sql, list(params)) for sql, params, _ in cluster.recorded_queries] == [
                (insert_sql, [7]),
                (update_sql, [9, 7]),
                (select_sql, [9]),
            ]
            assert cluster._conn.execute("SELECT id FROM stack_items").fetchall() == [(9,)]
        finally:
            native.close()
        return

    port = _NativeBindConnection()
    native = SyncMySQLDriver(MySQLConfig(dsn="mysql://localhost/db"), client=port)
    config = MySQLSyncConfig(native.config)
    driver = SQLSpecMySQLSyncDriver(native, statement_config=config.statement_config if config_defaults else None)
    try:
        result = driver.execute_stack(
            StatementStack()
            .push_execute("INSERT INTO stack_items VALUES (%s)", 7)
            .push_execute("UPDATE stack_items SET id = %s WHERE id = %s", 9, 7)
            .push_execute("SELECT id FROM stack_items WHERE id = %s", 9)
        )
        assert [item.rows_affected for item in result] == [1, 1, 1]
        assert result[2].get_result().get_data() == [{"id": 9}]
        assert port.calls == [
            ("INSERT INTO stack_items VALUES (%s)", [7]),
            ("UPDATE stack_items SET id = %s WHERE id = %s", [9, 7]),
            ("SELECT id FROM stack_items WHERE id = %s", [9]),
        ]
        assert port.raw.execute("SELECT id FROM stack_items").fetchall() == [(9,)]
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("engine", ["postgres", "mysql"])
@pytest.mark.parametrize("config_defaults", [False, True])
async def test_sqlspec_async_stack_preserves_native_dbapi_binds_and_rows(engine: str, config_defaults: bool) -> None:
    """Async native client ports observe engine placeholder syntax and complete ordered results."""
    if engine == "postgres":
        cluster = FakePostgresCluster()
        cluster._conn.execute("CREATE TABLE stack_items (id INTEGER)")
        native: Any = AsyncPostgreSQLDriver(
            PostgreSQLConfig(dsn="postgresql://localhost/db"),
            pool=FakeAsyncPostgresPool(cluster),
        )
        config: Any = PostgreSQLAsyncConfig(native.config)
        driver: Any = SQLSpecPostgreSQLAsyncDriver(
            native, statement_config=config.statement_config if config_defaults else None
        )
        insert_sql = "INSERT INTO stack_items VALUES ($1)"
        update_sql = "UPDATE stack_items SET id = $1 WHERE id = $2"
        select_sql = "SELECT id FROM stack_items WHERE id = $1"
        try:
            result = await driver.execute_stack(
                StatementStack().push_execute(insert_sql, 7).push_execute(update_sql, 9, 7).push_execute(select_sql, 9)
            )
            assert [item.rows_affected for item in result] == [1, 1, 1]
            assert result[2].get_result().get_data() == [{"id": 9}]
            assert [(sql, list(params)) for sql, params, _ in cluster.recorded_queries] == [
                (insert_sql, [7]),
                (update_sql, [9, 7]),
                (select_sql, [9]),
            ]
            assert all("$" in sql for sql, _, _ in cluster.recorded_queries)
            assert cluster._conn.execute("SELECT id FROM stack_items").fetchall() == [(9,)]
        finally:
            await native.close()
        return

    port = _AsyncNativeBindConnection()
    native = AsyncMySQLDriver(MySQLConfig(dsn="mysql://localhost/db"), client=port)
    config = MySQLAsyncConfig(native.config)
    driver = SQLSpecMySQLAsyncDriver(native, statement_config=config.statement_config if config_defaults else None)
    insert_sql = "INSERT INTO stack_items VALUES (%s)"
    update_sql = "UPDATE stack_items SET id = %s WHERE id = %s"
    select_sql = "SELECT id FROM stack_items WHERE id = %s"
    try:
        result = await driver.execute_stack(
            StatementStack().push_execute(insert_sql, 7).push_execute(update_sql, 9, 7).push_execute(select_sql, 9)
        )
        assert [item.rows_affected for item in result] == [1, 1, 1]
        assert result[2].get_result().get_data() == [{"id": 9}]
        assert [sql for sql, _ in port.calls] == [insert_sql, update_sql, select_sql]
        assert [list(params) for _, params in port.calls] == [[7], [9, 7], [9]]
        assert all("%s" in sql for sql, _ in port.calls)
        assert port.raw.execute("SELECT id FROM stack_items").fetchall() == [(9,)]
    finally:
        await native.close()


def test_sqlspec_explicit_statement_config_remains_authoritative() -> None:
    """Caller-owned statement configuration controls compilation and retains object identity."""
    port = _NativeBindConnection()
    statement_config = SQL("SELECT 1").statement_config
    config = PostgreSQLSyncConfig(PostgreSQLConfig(dsn="postgresql://localhost/db"), statement_config=statement_config)
    driver = SQLSpecPostgreSQLSyncDriver(port, statement_config=config.statement_config)
    try:
        result = driver.execute_stack(StatementStack().push_execute("INSERT INTO stack_items VALUES (:id)", id=7))
        assert config.statement_config is statement_config
        assert driver.statement_config is statement_config
        assert result[0].rows_affected == 1
        assert port.calls[0][0] == "INSERT INTO stack_items VALUES (?)"
        assert list(port.calls[0][1]) == [7]
    finally:
        port.close()


@pytest.mark.anyio
async def test_sqlspec_async_explicit_statement_config_remains_authoritative() -> None:
    """Async native defaults yield to the caller's SQLSpec parameter configuration."""
    port = _AsyncNativeBindConnection()
    statement_config = SQL("SELECT 1").statement_config
    config = PostgreSQLAsyncConfig(PostgreSQLConfig(dsn="postgresql://localhost/db"), statement_config=statement_config)
    driver = SQLSpecPostgreSQLAsyncDriver(port, statement_config=config.statement_config)
    try:
        result = await driver.execute_stack(StatementStack().push_execute("INSERT INTO stack_items VALUES (:id)", id=7))
        assert config.statement_config is statement_config
        assert driver.statement_config is statement_config
        assert result[0].rows_affected == 1
        assert port.calls[0][0] == "INSERT INTO stack_items VALUES (?)"
        assert list(port.calls[0][1]) == [7]
    finally:
        await port.close()


@pytest.mark.parametrize("prebound", [False, True])
@pytest.mark.parametrize("override_config", [False, True])
def test_sqlspec_arrow_stack_prepares_binds_and_retains_statement(prebound: bool, override_config: bool) -> None:
    """Arrow stack binds use prepared SQL and keep execution options out of named parameters."""
    native = _SyncPipelinePort()
    driver = SQLiteSyncDriver(native)
    statement = SQL("SELECT :value AS value", value=7) if prebound else "SELECT :value AS value"
    options = {
        "return_format": "table",
        "native_only": True,
        "batch_size": 1,
        "arrow_schema": pa.schema([("value", pa.int64())]),
    }
    if override_config:
        options["statement_config"] = StatementConfig(dialect="sqlite")
    if not prebound:
        options["value"] = 7
    try:
        result = driver.execute_stack(StatementStack().push_execute_arrow(statement, **options))[0]
        assert result.is_arrow_result()
        assert result.get_result().data.to_pylist() == [{"value": 7}]
        prepared = result.get_result().statement
        assert prepared.named_parameters == {"value": 7}
        assert prepared.operation_type == "SELECT"
        if override_config:
            assert prepared.statement_config is options["statement_config"]
        assert native.pipeline_calls == 0
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("prebound", [False, True])
@pytest.mark.parametrize("override_config", [False, True])
async def test_sqlspec_async_arrow_stack_prepares_binds_and_retains_statement(
    prebound: bool, override_config: bool
) -> None:
    """Async Arrow stack execution retains prebound and keyword values with prepared metadata."""
    native = _AsyncPipelinePort()
    driver = SQLiteAsyncDriver(native)
    statement = SQL("SELECT :value AS value", value=7) if prebound else "SELECT :value AS value"
    options = {
        "return_format": "table",
        "native_only": True,
        "batch_size": 1,
        "arrow_schema": pa.schema([("value", pa.int64())]),
    }
    if override_config:
        options["statement_config"] = StatementConfig(dialect="sqlite")
    if not prebound:
        options["value"] = 7
    try:
        result = (await driver.execute_stack(StatementStack().push_execute_arrow(statement, **options)))[0]
        assert result.is_arrow_result()
        assert result.get_result().data.to_pylist() == [{"value": 7}]
        prepared = result.get_result().statement
        assert prepared.named_parameters == {"value": 7}
        assert prepared.operation_type == "SELECT"
        if override_config:
            assert prepared.statement_config is options["statement_config"]
        assert native.pipeline_calls == 0
    finally:
        await native.close()


@pytest.mark.parametrize("override_config", [False, True])
def test_sqlspec_batch_stack_compiles_native_binds(override_config: bool) -> None:
    """Batch stack processing applies the same native parameter profile as single execution."""
    if override_config:
        port = _NativeBindConnection()
        driver = SQLSpecPostgreSQLSyncDriver(port)
        config: StatementConfig | None = StatementConfig(dialect="postgres")
        try:
            result = driver.execute_stack(
                StatementStack().push_execute_many(
                    "INSERT INTO stack_items VALUES (?)", [(7,), (9,)], statement_config=config
                )
            )
            assert result[0].rows_affected == 2
            assert [sql for sql, _ in port.calls] == ["INSERT INTO stack_items VALUES (?)"] * 2
            assert [list(params) for _, params in port.calls] == [[7], [9]]
            assert port.raw.execute("SELECT id FROM stack_items ORDER BY id").fetchall() == [(7,), (9,)]
        finally:
            port.close()
        return

    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE stack_items (id INTEGER)")
    native = SyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost/db"),
        pool=FakeSyncPostgresPool(cluster),
    )
    driver = SQLSpecPostgreSQLSyncDriver(native)
    try:
        result = driver.execute_stack(
            StatementStack().push_execute_many("INSERT INTO stack_items VALUES (?)", [(7,), (9,)])
        )
        assert result[0].rows_affected == 2
        assert [sql for sql, _, _ in cluster.recorded_queries] == ["INSERT INTO stack_items VALUES ($1)"] * 2
        assert [list(params) for _, params, _ in cluster.recorded_queries] == [[7], [9]]
        assert cluster._conn.execute("SELECT id FROM stack_items ORDER BY id").fetchall() == [(7,), (9,)]
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("override_config", [False, True])
async def test_sqlspec_async_batch_stack_compiles_native_binds(override_config: bool) -> None:
    """Async batch stack parameters compile to native numeric binds or the explicit override."""
    if override_config:
        port = _AsyncNativeBindConnection()
        driver = SQLSpecPostgreSQLAsyncDriver(port)
        config: StatementConfig | None = StatementConfig(dialect="postgres")
        try:
            result = await driver.execute_stack(
                StatementStack().push_execute_many(
                    "INSERT INTO stack_items VALUES (?)", [(7,), (9,)], statement_config=config
                )
            )
            assert result[0].rows_affected == 2
            assert [sql for sql, _ in port.calls] == ["INSERT INTO stack_items VALUES (?)"] * 2
            assert [list(params) for _, params in port.calls] == [[7], [9]]
            assert port.raw.execute("SELECT id FROM stack_items ORDER BY id").fetchall() == [(7,), (9,)]
        finally:
            await port.close()
        return

    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE stack_items (id INTEGER)")
    native = AsyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost/db"),
        pool=FakeAsyncPostgresPool(cluster),
    )
    driver = SQLSpecPostgreSQLAsyncDriver(native)
    try:
        result = await driver.execute_stack(
            StatementStack().push_execute_many("INSERT INTO stack_items VALUES (?)", [(7,), (9,)])
        )
        assert result[0].rows_affected == 2
        assert [sql for sql, _, _ in cluster.recorded_queries] == ["INSERT INTO stack_items VALUES ($1)"] * 2
        assert [list(params) for _, params, _ in cluster.recorded_queries] == [[7], [9]]
        assert cluster._conn.execute("SELECT id FROM stack_items ORDER BY id").fetchall() == [(7,), (9,)]
    finally:
        await native.close()


@pytest.mark.parametrize("sql_object", [False, True])
def test_sqlspec_batch_stack_applies_filters(sql_object: bool) -> None:
    """Batch filters constrain actual database updates for strings and existing SQL objects."""
    native = _SyncPipelinePort()
    native.execute("CREATE TABLE stack_items (id INTEGER)")
    native.execute_many("INSERT INTO stack_items VALUES (?)", [(None,), (1,)])
    driver = SQLiteSyncDriver(native)
    statement = SQL("UPDATE stack_items SET id = ?") if sql_object else "UPDATE stack_items SET id = ?"
    try:
        result = driver.execute_stack(StatementStack().push_execute_many(statement, [(7,), (9,)], NullFilter("id")))
        assert result[0].rows_affected == 1
        assert native.select("SELECT id FROM stack_items ORDER BY id") == [{"id": 1}, {"id": 7}]
        assert native.pipeline_calls == 0
    finally:
        native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("sql_object", [False, True])
async def test_sqlspec_async_batch_stack_applies_filters(sql_object: bool) -> None:
    """Async batch filters preserve row scope when normalizing batched parameters."""
    native = _AsyncPipelinePort()
    await native.execute("CREATE TABLE stack_items (id INTEGER)")
    await native.execute_many("INSERT INTO stack_items VALUES (?)", [(None,), (1,)])
    driver = SQLiteAsyncDriver(native)
    statement = SQL("UPDATE stack_items SET id = ?") if sql_object else "UPDATE stack_items SET id = ?"
    try:
        result = await driver.execute_stack(
            StatementStack().push_execute_many(statement, [(7,), (9,)], NullFilter("id"))
        )
        assert result[0].rows_affected == 1
        assert await native.select("SELECT id FROM stack_items ORDER BY id") == [{"id": 1}, {"id": 7}]
        assert native.pipeline_calls == 0
    finally:
        await native.close()


def test_sync_dbapi_connection_driver_ownership_and_broker_isolation() -> None:
    """Shared registry drivers survive connection close while dedicated and custom-broker drivers close."""
    url = "postgresql+vtb://localhost:5432/ownership_sync_db"
    shared_conn = SyncDBAPIConnection(url, share_pool=True)
    shared_driver = shared_conn.driver
    assert shared_driver is DatabaseRegistry.default().get_shared_sync_driver(shared_conn.config)
    shared_conn.close()
    assert shared_driver._closed is False

    owned_conn = SyncDBAPIConnection(url, share_pool=False)
    owned_driver = owned_conn.driver
    assert owned_driver is not shared_driver
    owned_conn.close()
    assert owned_driver._closed is True
    assert shared_driver._closed is False

    custom_broker = LeaseBroker(pool_size=4, max_overflow=2)
    broker_conn = SyncDBAPIConnection(url, broker=custom_broker, share_pool=True)
    broker_driver = broker_conn.driver
    assert broker_driver is not shared_driver
    assert broker_driver.config.broker is custom_broker
    broker_conn.close()
    assert broker_driver._closed is True
    assert shared_driver._closed is False


@pytest.mark.anyio
async def test_async_dbapi_connection_driver_ownership_broker_isolation_and_close_settlement(
    tmp_path: Path,
) -> None:
    """Async DBAPI connections keep shared drivers open, isolate custom brokers, and settle active transactions."""
    url = "postgresql+vtb_async://localhost:5432/ownership_async_db"
    shared_conn = AsyncDBAPIConnection(url, share_pool=True)
    shared_driver = shared_conn.driver
    assert shared_driver is DatabaseRegistry.default().get_shared_driver(shared_conn.config)
    await shared_conn.close()
    assert shared_driver._closed is False

    owned_conn = AsyncDBAPIConnection(url, share_pool=False)
    owned_driver = owned_conn.driver
    assert owned_driver is not shared_driver
    await owned_conn.close()
    assert owned_driver._closed is True
    assert shared_driver._closed is False

    custom_broker = LeaseBroker(pool_size=4, max_overflow=2)
    broker_conn = AsyncDBAPIConnection(url, broker=custom_broker, share_pool=True)
    broker_driver = broker_conn.driver
    assert broker_driver is not shared_driver
    assert broker_driver.config.broker is custom_broker
    await broker_conn.close()
    assert broker_driver._closed is True
    assert shared_driver._closed is False

    sqlite_url = f"sqlite+vtb_async:///{tmp_path / 'close_tx.db'}"
    setup_conn = AsyncDBAPIConnection(sqlite_url, share_pool=True)
    sqlite_drv = setup_conn.driver
    await setup_conn.cursor().execute("CREATE TABLE close_tx_items (id INTEGER)")
    await setup_conn.commit()
    await setup_conn.close()

    tx_conn = AsyncDBAPIConnection(sqlite_url, share_pool=True)
    assert tx_conn.driver is sqlite_drv
    await tx_conn.cursor().execute("INSERT INTO close_tx_items VALUES (?)", (1,))
    await tx_conn.close()
    assert sqlite_drv._sync._closed is False
    assert await sqlite_drv.select_value("SELECT COUNT(*) FROM close_tx_items") == 0
    await sqlite_drv.close()


def test_sync_dbapi_connection_rejects_unparsable_url() -> None:
    """SyncDBAPIConnection and SyncDBAPIModule.connect raise PEP 249 InterfaceError on unparsable URLs."""
    with pytest.raises(
        InterfaceError,
        match=r"Cannot parse Vertebra database URL: Unsupported database URL scheme: 'nonsense\+vtb'",
    ) as exc_info:
        SyncDBAPIConnection("nonsense+vtb://host/db", share_pool=False)
    assert isinstance(exc_info.value.__cause__, ValueError)

    assert SyncDBAPIModule.InterfaceError is InterfaceError
    with pytest.raises(InterfaceError):
        SyncDBAPIModule().connect("nonsense+vtb://host/db")

    with pytest.raises(InterfaceError) as empty_exc:
        SyncDBAPIConnection("", share_pool=False)
    assert isinstance(empty_exc.value.__cause__, ValueError)
    assert str(empty_exc.value.__cause__) == "Unsupported database URL scheme: ''"

    driver = SyncSQLiteDriver()
    try:
        with pytest.raises(InterfaceError):
            SyncDBAPIConnection("nonsense+vtb://host/db", driver=driver, share_pool=False)
    finally:
        driver.close()


def test_async_dbapi_connection_rejects_unparsable_url() -> None:
    """AsyncDBAPIConnection and AsyncDBAPIModule.connect raise PEP 249 InterfaceError on unparsable URLs."""
    with pytest.raises(
        InterfaceError,
        match=r"'nonsense\+vtb_async'",
    ) as exc_info:
        AsyncDBAPIConnection("nonsense+vtb_async://host/db", share_pool=False)
    assert isinstance(exc_info.value.__cause__, ValueError)

    assert SQLAlchemyAsyncDBAPI.InterfaceError is InterfaceError
    with pytest.raises(InterfaceError):
        SQLAlchemyAsyncDBAPI().connect("nonsense+vtb_async://host/db")


@pytest.mark.parametrize("failure_mode", ["rollback_error", "exit_error", "quarantine_error"])
@pytest.mark.anyio
async def test_sqlspec_provider_quarantines_without_release_on_sync_and_async_failures(failure_mode: str) -> None:
    """Verify sync/async provider and proxy session release quarantines without release and preserves context."""

    class RecordingLease:
        def __init__(self, slot: int, gen: int) -> None:
            self.slot_index = slot
            self.generation = gen
            self.released = 0
            self.quarantined = False

        def release(self) -> None:
            self.released += 1

    class SyncBrokerDouble:
        def __init__(self) -> None:
            self.leases: list[RecordingLease] = []
            self.quarantined_calls = 0

        def acquire(self, *, pool_name: str, worker_pid: int) -> RecordingLease:
            _ = (pool_name, worker_pid)
            lease = RecordingLease(2, len(self.leases) + 1)
            self.leases.append(lease)
            return lease

        def quarantine(self, lease: RecordingLease) -> None:
            self.quarantined_calls += 1
            lease.quarantined = True
            if failure_mode == "quarantine_error":
                raise OSError("quarantine backend unreachable")

    class SyncFailingSession:
        def __init__(self) -> None:
            self.exit_called = False

        def __enter__(self) -> SyncFailingSession:
            return self

        def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.exit_called = True
            if failure_mode == "exit_error":
                raise RuntimeError("session context exit failure")

        def execute(self, sql: str, *parameters: Any) -> int:
            _ = (sql, parameters)
            return 1

        def rollback(self) -> None:
            if failure_mode in ("rollback_error", "quarantine_error"):
                raise ValueError("dirty rollback failure")

    sync_broker = SyncBrokerDouble()
    assert isinstance(sync_broker, SyncBrokerLeaseSource)
    sync_sessions: list[SyncFailingSession] = []

    def _build_sync_session() -> SyncFailingSession:
        sess = SyncFailingSession()
        sync_sessions.append(sess)
        return sess

    sync_provider = SQLSpecSyncProvider(_build_sync_session, broker=sync_broker)
    sync_driver = ProxySyncDriver(sync_provider, broker=sync_broker)
    sync_driver.begin()

    with pytest.raises(LeaseQuarantinedError) as exc_info:
        sync_driver.release_lease()
    assert sync_sessions[0].exit_called is True
    if failure_mode == "quarantine_error":
        assert "quarantine backend unreachable" in str(exc_info.value.__cause__)
    assert sync_broker.quarantined_calls == 1
    assert sync_broker.leases[0].released == 0

    class AsyncBrokerDouble:
        def __init__(self) -> None:
            self.leases: list[RecordingLease] = []
            self.quarantined_calls = 0

        async def acquire(self, *, pool_name: str, worker_pid: int) -> RecordingLease:
            _ = (pool_name, worker_pid)
            lease = RecordingLease(3, len(self.leases) + 1)
            self.leases.append(lease)
            return lease

        async def quarantine(self, lease: RecordingLease) -> None:
            self.quarantined_calls += 1
            lease.quarantined = True
            if failure_mode == "quarantine_error":
                raise OSError("async quarantine failure")

    class AsyncFailingSession:
        def __init__(self) -> None:
            self.exit_called = False

        async def __aenter__(self) -> AsyncFailingSession:
            return self

        async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
            self.exit_called = True
            if failure_mode == "exit_error":
                raise RuntimeError("async context exit failure")

        async def execute(self, sql: str, *parameters: Any) -> int:
            _ = (sql, parameters)
            return 1

        async def rollback(self) -> None:
            if failure_mode in ("rollback_error", "quarantine_error"):
                raise ValueError("async rollback failure")

    async_broker = AsyncBrokerDouble()
    assert isinstance(async_broker, AsyncBrokerLeaseSource)
    async_sessions: list[AsyncFailingSession] = []

    def _build_async_session() -> AsyncFailingSession:
        sess = AsyncFailingSession()
        async_sessions.append(sess)
        return sess

    async_provider = SQLSpecAsyncProvider(_build_async_session, broker=async_broker)
    async_driver = ProxyAsyncDriver(async_provider, broker=async_broker)
    await async_driver.begin()

    with pytest.raises(LeaseQuarantinedError) as a_exc_info:
        await async_driver.release_lease()
    assert async_sessions[0].exit_called is True
    if failure_mode == "quarantine_error":
        assert "async quarantine failure" in str(a_exc_info.value.__cause__)
    assert async_broker.quarantined_calls == 1
    assert async_broker.leases[0].released == 0


@pytest.mark.parametrize(
    ("dialect_name", "style"),
    [
        ("postgres", ParameterStyle.NUMERIC),
        ("oracle", ParameterStyle.NAMED_COLON),
        ("sqlite", ParameterStyle.QMARK),
    ],
)
def test_proxy_drivers_subclass_sqlspec_bases_and_preserve_statement_config(
    dialect_name: str,
    style: ParameterStyle,
) -> None:
    """Verify ProxySyncDriver/ProxyAsyncDriver inherit adapter bases, preserve statement_config, and stay reflect-free."""
    stmt_cfg = StatementConfig(
        dialect=dialect_name,
        parameter_config=ParameterStyleConfig(default_parameter_style=style, supported_parameter_styles={style}),
    )

    class WrappedCfg:
        dialect = dialect_name
        bind_key = "primary"
        statement_config = stmt_cfg
        driver_features: dict[str, Any] = {}

        def provide_session(self) -> Any:
            return self

    proxy_sync_cfg = ProxySyncConfig(WrappedCfg())
    proxy_async_cfg = ProxyAsyncConfig(WrappedCfg())
    sync_driver = ProxySyncDriver(SQLSpecSyncProvider(proxy_sync_cfg), statement_config=proxy_sync_cfg.statement_config)
    async_driver = ProxyAsyncDriver(
        SQLSpecAsyncProvider(proxy_async_cfg), statement_config=proxy_async_cfg.statement_config
    )
    assert isinstance(sync_driver, SyncDriverAdapterBase)
    assert isinstance(async_driver, AsyncDriverAdapterBase)
    assert sync_driver.statement_config is stmt_cfg
    assert async_driver.statement_config is stmt_cfg
    assert sync_driver.dialect == dialect_name
    assert async_driver.dialect == dialect_name

    assert BrokerRpcRequest.__struct_config__.gc is True
    assert BrokerRpcResponse.__struct_config__.gc is True
    assert sync_driver.data_dictionary is None
    assert async_driver.data_dictionary is None
    assert sync_driver.provider is not None
    assert sync_driver.provider.resolve_upstream_optional("primary") is not None


@pytest.mark.parametrize(
    "factory",
    [
        lambda: BrokerPool(),
        lambda: AsyncBrokerPool(),
        lambda: BrokerPool(broker=SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))),
        lambda: broker_creator("sqlite+vtb:///:memory:", broker=None),
        lambda: broker_creator("", broker=SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))),
    ],
)
def test_broker_pool_and_creator_reject_unconfigured_inputs(factory: Any) -> None:
    """Verify BrokerPool, AsyncBrokerPool, and broker_creator raise ImproperConfigurationError when unconfigured."""
    with pytest.raises(ImproperConfigurationError):
        factory()


def test_sync_broker_pool_checkout_rollback_quarantine_and_recreate(tmp_path: Path) -> None:
    """Verify BrokerPool acquires partition leases, releases healthy checkins, and quarantines rollback errors."""
    url = f"sqlite+vtb:///{tmp_path / 'broker_pool.db'}"
    transport = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    pool = BrokerPool(broker=transport, url=url)
    pool_key_tuple = DatabaseRegistry.parse_url(url, broker=transport).pool_key()
    assert pool_key_tuple is not None
    expected_partition = "|".join(str(part) for part in pool_key_tuple)
    assert pool.partition_key == expected_partition

    engine = create_engine(url, pool=pool)
    try:
        with engine.connect() as conn:
            row = conn.exec_driver_sql("SELECT 7").fetchone()
            assert row == (7,)
            assert transport.metrics().active_leases == 1
        assert transport.metrics().active_leases == 0
        assert transport.metrics().total_released == 1
        assert transport.quarantined_count() == 0

        recreated = pool.recreate()
        assert isinstance(recreated, BrokerPool)
        assert recreated.partition_key == expected_partition
        recreated.dispose()
    finally:
        engine.dispose()

    def _boom_rollback(_conn: Any) -> None:
        raise RuntimeError("dirty connection reset error")

    fail_pool = BrokerPool(broker=transport, url=url, on_rollback=_boom_rollback)
    fail_engine = create_engine(url, pool=fail_pool)
    try:
        with pytest.raises(LeaseQuarantinedError) as exc_info, fail_engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1").fetchone()
        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert transport.quarantined_count() == 1
        assert transport.metrics().total_quarantined == 1
        assert transport.metrics().total_released == 1
        transport.dispose_quarantined(exc_info.value.slot_index, exc_info.value.generation)
        assert transport.quarantined_count() == 0
    finally:
        fail_engine.dispose()


@pytest.mark.anyio
async def test_async_broker_pool_checkout_rollback_and_quarantine() -> None:
    """Verify AsyncBrokerPool coordinates greenlet checkouts with AsyncSHMBrokerTransport and quarantines failures."""
    url = "sqlite+vtb_async:///:memory:"
    transport = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    pool = AsyncBrokerPool(broker=transport, url=url)
    async_engine = create_async_engine(url, pool=pool)
    try:
        async with async_engine.connect() as conn:
            res = await conn.exec_driver_sql("SELECT 9")
            assert res.fetchone() == (9,)
            assert transport.metrics().active_leases == 1
        assert transport.metrics().active_leases == 0
        assert transport.metrics().total_released == 1
        assert transport.quarantined_count() == 0
    finally:
        await async_engine.dispose()

    def _boom_async_rollback(_conn: Any) -> None:
        raise RuntimeError("async dirty rollback error")

    fail_pool = AsyncBrokerPool(broker=transport, url=url, on_rollback=_boom_async_rollback)
    fail_engine = create_async_engine(url, pool=fail_pool)
    try:
        with pytest.raises(LeaseQuarantinedError) as exc_info:
            async with fail_engine.connect() as conn:
                await conn.exec_driver_sql("SELECT 1")
        assert isinstance(exc_info.value.__cause__, RuntimeError)
        assert transport.quarantined_count() == 1
        assert transport.metrics().total_quarantined == 1
        await transport.dispose_quarantined(exc_info.value.slot_index, exc_info.value.generation)
    finally:
        await fail_engine.dispose()


@pytest.mark.parametrize("is_async", [False, True])
def test_alchemy_plugin_wires_broker_pool_classes(is_async: bool) -> None:
    """Verify create_sync_alchemy_config and create_async_alchemy_config wire BrokerPool/AsyncBrokerPool."""
    broker = (
        AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
        if is_async
        else SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0))
    )
    if is_async:
        cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:", broker=broker)
        assert issubclass(cfg.engine_config.poolclass, AsyncBrokerPool)
    else:
        cfg = create_sync_alchemy_config("sqlite+vtb:///:memory:", broker=broker)
        assert issubclass(cfg.engine_config.poolclass, BrokerPool)


@pytest.mark.anyio
@pytest.mark.parametrize("count_with_window", [False, True])
async def test_proxy_drivers_compile_sql_filters_stacks_and_common_driver_params(count_with_window: bool) -> None:
    """Verify proxy drivers compile SQL/filters/styles, dispatch stacks/totals, bind single strings, and unwrap configs."""
    pg_cfg = StatementConfig(
        dialect="postgres",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.NUMERIC,
            supported_parameter_styles={ParameterStyle.NUMERIC},
        ),
    )
    pg_proxy = ProxySyncDriver(SQLSpecSyncProvider(SyncSQLiteDriver()), statement_config=pg_cfg)
    compiled_sql, compiled_params, _ = pg_proxy.compile_with_broker(
        pg_proxy,
        pg_proxy.broker,
        "postgres",
        SQL("SELECT id FROM items WHERE id = :id", id=42),
    )
    assert compiled_sql.strip() == "SELECT id FROM items WHERE id = $1"
    assert compiled_params == [42]

    sqlite_cfg = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
        ),
    )
    sync_driver = SyncSQLiteDriver()
    sync_provider = SQLSpecSyncProvider(sync_driver)
    proxy_sync = ProxySyncDriver(sync_provider, statement_config=sqlite_cfg)
    proxy_sync.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    proxy_sync.execute("INSERT INTO items VALUES (?, ?)", 1, "a")
    proxy_sync.execute("INSERT INTO items VALUES (?, ?)", 2, "b")
    proxy_sync.execute("INSERT INTO items VALUES (?, ?)", 3, "c")

    qb_sql, qb_params, _ = proxy_sync.compile_with_broker(
        proxy_sync,
        proxy_sync.broker,
        "sqlite",
        sql.select("id", "name").from_("items").where_eq("id", 2),
        (LimitOffsetFilter(limit=2, offset=1),),
    )
    assert "WHERE" in qb_sql.upper()
    assert "LIMIT" in qb_sql.upper()
    assert qb_params == [2, 2, 1]

    assert proxy_sync.select_value("SELECT ?", "hello") == "hello"
    assert proxy_sync.select_value_or_none("SELECT name FROM items WHERE id = ?", 2) == "b"
    assert proxy_sync.select_value_or_none("SELECT name FROM items WHERE id = ?", 999) is None

    rows, total = proxy_sync.select_with_total(
        "SELECT id, name FROM items ORDER BY id",
        LimitOffsetFilter(limit=2, offset=1),
        count_with_window=count_with_window,
    )
    assert total == 3
    assert [row["id"] for row in rows] == [2, 3]

    ok_stack = (
        StatementStack()
        .push_execute("INSERT INTO items VALUES (?, ?)", 4, "d")
        .push_execute("INSERT INTO items VALUES (?, ?)", 5, "e")
    )
    ok_results = proxy_sync.execute_stack(ok_stack)
    assert len(ok_results) == 2
    assert proxy_sync.select_value("SELECT COUNT(*) FROM items") == 5

    fail_stack = (
        StatementStack()
        .push_execute("INSERT INTO items VALUES (?, ?)", 6, "f")
        .push_execute("INSERT INTO items VALUES (?, ?)", 1, "dup")
    )
    with pytest.raises(StackExecutionError):
        proxy_sync.execute_stack(fail_stack)
    assert proxy_sync.select_value("SELECT COUNT(*) FROM items") == 5

    cont_stack = (
        StatementStack()
        .push_execute("INSERT INTO items VALUES (?, ?)", 1, "dup")
        .push_execute("INSERT INTO items VALUES (?, ?)", 7, "g")
    )
    cont_results = proxy_sync.execute_stack(cont_stack, continue_on_error=True)
    assert len(cont_results) == 2
    assert isinstance(cont_results[0].error, StackExecutionError)
    assert cont_results[1].error is None
    assert proxy_sync.select_value("SELECT COUNT(*) FROM items") == 6

    async_driver = AsyncSQLiteDriver()
    async_provider = SQLSpecAsyncProvider(async_driver)
    proxy_async = ProxyAsyncDriver(async_provider, statement_config=sqlite_cfg)
    await proxy_async.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
    await proxy_async.execute("INSERT INTO items VALUES (?, ?)", 1, "a")
    await proxy_async.execute("INSERT INTO items VALUES (?, ?)", 2, "b")
    await proxy_async.execute("INSERT INTO items VALUES (?, ?)", 3, "c")

    assert await proxy_async.select_value("SELECT ?", "hello") == "hello"
    assert await proxy_async.select_value_or_none("SELECT name FROM items WHERE id = ?", 2) == "b"
    assert await proxy_async.select_value_or_none("SELECT name FROM items WHERE id = ?", 999) is None

    arows, atotal = await proxy_async.select_with_total(
        "SELECT id, name FROM items ORDER BY id",
        LimitOffsetFilter(limit=2, offset=1),
        count_with_window=count_with_window,
    )
    assert atotal == 3
    assert [row["id"] for row in arows] == [2, 3]

    aok_results = await proxy_async.execute_stack(ok_stack)
    assert len(aok_results) == 2
    assert await proxy_async.select_value("SELECT COUNT(*) FROM items") == 5

    with pytest.raises(StackExecutionError):
        await proxy_async.execute_stack(fail_stack)
    assert await proxy_async.select_value("SELECT COUNT(*) FROM items") == 5

    acont_results = await proxy_async.execute_stack(cont_stack, continue_on_error=True)
    assert len(acont_results) == 2
    assert isinstance(acont_results[0].error, StackExecutionError)
    assert acont_results[1].error is None
    assert await proxy_async.select_value("SELECT COUNT(*) FROM items") == 6

    class WrappedCfg:
        dialect = "sqlite"
        bind_key = "primary"
        statement_config = sqlite_cfg
        driver_features: dict[str, Any] = {}

        def provide_session(self) -> Any:
            return self

    proxy_sync_cfg = ProxySyncConfig(WrappedCfg(), provider=sync_provider)
    proxy_async_cfg = ProxyAsyncConfig(WrappedCfg(), provider=async_provider)
    with proxy_sync_cfg.provide_proxied_session() as sess:
        assert isinstance(sess, ProxySyncDriver)
        assert sess.select_value("SELECT ?", 11) == 11
    async with proxy_async_cfg.provide_proxied_session() as asess:
        assert isinstance(asess, ProxyAsyncDriver)
        assert await asess.select_value("SELECT ?", 12) == 12


@pytest.mark.anyio
async def test_broker_pool_partitions_timeouts_real_rollback_invalidate_and_alchemy_execution(tmp_path: Path) -> None:
    """Verify partition isolation, pool_size=1 timeouts, real rollback quarantine, recreate/invalidate, and Alchemy SQL."""
    db1 = tmp_path / "one.db"
    db2 = tmp_path / "two.db"
    sync_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    p1 = BrokerPool(broker=sync_t, url=f"sqlite+vtb:///{db1}", pool_size=1, max_overflow=0, acquire_timeout_ms=50)
    p2 = BrokerPool(broker=sync_t, url=f"sqlite+vtb:///{db2}", pool_size=1, max_overflow=0, acquire_timeout_ms=50)
    mem_pool = BrokerPool(broker=sync_t, url="sqlite+vtb:///:memory:", pool_size=1, max_overflow=0)
    try:
        assert p1.partition_key != p2.partition_key
        assert mem_pool.partition_key is not None
        assert "None" not in mem_pool.partition_key
        assert mem_pool.partition_key.endswith(f"#pool-{id(mem_pool)}")

        c1 = p1.connect()
        c2 = p2.connect()
        try:
            with pytest.raises(TimeoutError):
                p1.connect()
        finally:
            c2.close()
        try:
            with pytest.raises(TimeoutError):
                p1.connect()
        finally:
            c1.close()

        recreated = p1.recreate()
        assert isinstance(recreated, BrokerPool)
        inv_conn = recreated.connect()
        inv_conn.invalidate()
        assert sync_t.metrics().active_leases == 0
        recreated.dispose()
    finally:
        mem_pool.dispose()
        p2.dispose()
        p1.dispose()

    rollback_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=1, max_overflow=0, acquire_timeout_ms=50))
    rollback_db = tmp_path / "rollback.db"
    should_fail_rollback = True

    class FailingRollbackConnection(SyncDBAPIConnection):
        def rollback(self) -> None:
            if should_fail_rollback:
                raise RuntimeError("real rollback failed")
            super().rollback()

    rollback_pool = BrokerPool(
        creator=lambda: FailingRollbackConnection(driver=SyncSQLiteDriver(), broker=rollback_t, lease_managed=True),
        broker=rollback_t,
        pool_key=f"sqlite://{rollback_db}",
        pool_size=1,
        max_overflow=0,
        acquire_timeout_ms=50,
        reset_on_return="rollback",
    )
    try:
        conn = rollback_pool.connect()
        with pytest.raises(LeaseQuarantinedError) as q_exc:
            conn.close()
        assert isinstance(q_exc.value.__cause__, RuntimeError)
        assert rollback_t.quarantined_count() == 1
        with pytest.raises(TimeoutError):
            rollback_pool.connect()
        should_fail_rollback = False
        rollback_t.dispose_quarantined(q_exc.value.slot_index, q_exc.value.generation)
        assert rollback_t.quarantined_count() == 0
        recovered = rollback_pool.connect()
        recovered.close()
    finally:
        rollback_pool.dispose()

    creator_sync = broker_creator("sqlite+vtb:///:memory:", broker=sync_t)
    raw_sync = creator_sync()
    try:
        assert raw_sync.broker is sync_t
    finally:
        raw_sync.close()

    async_t = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    creator_async = broker_creator("sqlite+vtb_async:///:memory:", broker=async_t, is_async=True)
    raw_async = await greenlet_spawn(creator_async)
    try:
        assert raw_async._connection.broker is async_t
    finally:
        await greenlet_spawn(raw_async.close)

    alchemy_sync_t = SyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    sync_cfg = create_sync_alchemy_config("sqlite+vtb:///:memory:", broker=alchemy_sync_t)
    sync_engine = sync_cfg.get_engine()
    try:
        with sync_engine.connect() as sa_conn:
            assert sa_conn.exec_driver_sql("SELECT 1").fetchone() == (1,)
            raw_sync_fairy = sa_conn.connection
            assert raw_sync_fairy.driver_connection.broker is alchemy_sync_t
        assert alchemy_sync_t.metrics().active_leases == 0
        assert alchemy_sync_t.metrics().total_released == 1
    finally:
        sync_engine.dispose()

    alchemy_async_t = AsyncSHMBrokerTransport(BrokerConfig(pool_size=2, max_overflow=0, acquire_timeout_ms=50))
    async_cfg = create_async_alchemy_config("sqlite+vtb_async:///:memory:", broker=alchemy_async_t)
    async_engine = async_cfg.get_engine()
    try:
        async with async_engine.connect() as asa_conn:
            res = await asa_conn.exec_driver_sql("SELECT 1")
            assert res.fetchone() == (1,)
            raw_async_fairy = await asa_conn.get_raw_connection()
            assert raw_async_fairy.driver_connection._connection.broker is alchemy_async_t
        assert alchemy_async_t.metrics().active_leases == 0
        assert alchemy_async_t.metrics().total_released == 1
    finally:
        await async_engine.dispose()
