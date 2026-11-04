"""Unit and integration tests for SQLAlchemy 2.1 +vtb dialects, types, Alembic, and Advanced Alchemy."""

from __future__ import annotations

import array

import pytest
from advanced_alchemy.base import UUIDAuditBase
from advanced_alchemy.repository import SQLAlchemyAsyncRepository
from advanced_alchemy.service import SQLAlchemyAsyncRepositoryService
from advanced_alchemy.types.file_object import FileObject, StoredObject
from sqlalchemy import String, create_engine, select
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from vertebra.db import PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.extensions.alchemy import (
    ObjectStoreBackend,
    create_alchemy_plugin,
    create_async_alchemy_config,
    create_sync_alchemy_config,
)
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
    SyncDBAPIConnection,
    SyncDBAPIModule,
    SyncEngineAdapter,
    SyncMySQLDialect,
    SyncOracleDialect,
    SyncPostgreSQLDialect,
    SyncSQLiteDialect,
    Vector,
)
from vertebra.storage import SyncObjectStore


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
    assert pg_dbapi.paramstyle == "pyformat"
    assert SyncPostgreSQLDialect.import_dbapi().paramstyle == "pyformat"
    assert SyncSQLiteDialect.dbapi().paramstyle == "qmark"
    assert SyncMySQLDialect.dbapi().paramstyle == "format"
    assert SyncOracleDialect.dbapi().paramstyle == "named"

    driver = SyncPostgreSQLDriver(PostgreSQLConfig(dsn="memory://"))
    driver._in_memory_rows = [(1, "item1"), (2, "item2")]

    conn = pg_dbapi.connect("postgresql+vtb://localhost:5432/app", driver=driver)
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM items")
    assert cur.description is not None
    assert len(cur.description) == 2
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
