"""Unit tests for SQLSpec storage/plugin helpers, SQLAlchemy 2.1 +vtb dialects, Alembic, and ORM optimizations."""

from __future__ import annotations

import array

import pytest

from vertebra.db import PostgresConfig, SyncPostgresDriver
from vertebra.extensions.sqlalchemy import (
    AlembicImpl,
    BinaryJson,
    CreateVectorIndexOp,
    CreateViewOp,
    DBAPIConnection,
    DBAPIModule,
    DeleteUsingClause,
    DialectDescriptor,
    DropVectorIndexOp,
    InsertSentinelSpec,
    MysqlVtbDialect,
    OracleDialect,
    OracleVtbDialect,
    ParameterizedSql,
    PostgresDialect,
    PostgresVtbDialect,
    SelectInLoadSpec,
    SqliteDialect,
    SqliteVtbDialect,
    SyncEngineAdapter,
    Vector,
)
from vertebra.extensions.sqlspec import (
    SQLSpecPlugin,
    SQLSpecStore,
    SyncDatabaseConfig,
)
from vertebra.storage import ObjectStore


@pytest.mark.anyio
async def test_sqlspec_arrow_capsule_export_and_multipool_shutdown() -> None:
    """Verify SQLSpecStore.export_arrow_capsule and SQLSpecPlugin multi-pool shutdown."""
    store = ObjectStore("memory://")
    bridge = SQLSpecStore(store, prefix="exports")

    class TabularProvider:
        __slots__ = ()

        def to_dicts(self) -> list[dict[str, object]]:
            return [{"id": 1, "name": "Alpha"}, {"id": 2, "name": "Beta"}]

    res = bridge.export_arrow_capsule("items.arrow", TabularProvider())
    assert res.size > 0
    assert len(bridge.read_arrow_ipc("items.arrow")) == res.size

    closed_pools: list[str] = []

    class DummyPool:
        __slots__ = ("name",)

        def __init__(self, name: str) -> None:
            self.name = name

        async def close(self) -> None:
            closed_pools.append(self.name)

    class DummyDriver:
        __slots__ = ()

        def execute(self, statement: str, *args: object, **kwargs: object) -> object:
            _ = (statement, args, kwargs)
            return None

    cfg1 = SyncDatabaseConfig[object, DummyPool, DummyDriver](
        bind_key="primary",
        pool_instance=DummyPool("primary"),
    )
    cfg2 = SyncDatabaseConfig[object, DummyPool, DummyDriver](
        bind_key="events",
        pool_instance=DummyPool("events"),
    )
    plugin: SQLSpecPlugin[SyncDatabaseConfig[object, DummyPool, DummyDriver]] = SQLSpecPlugin(
        config=[cfg1, cfg2],
        store=store,
    )
    state_dict: dict[str, object] = {}
    plugin.install_state(state_dict)
    await plugin.close_pools()
    assert closed_pools == ["primary", "events"]


def test_vector_and_binary_json_types_across_dialects() -> None:
    """Verify Vector and BinaryJson DDL compilation, bind processing, and distance expressions."""
    vec_type = Vector(768, storage_format="float32", column_name="embedding")
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

    bjson = BinaryJson()
    assert bjson.get_col_spec("postgresql+vtb") == "JSONB"
    assert bjson.get_col_spec("sqlite+vtb") == "JSONB"
    assert bjson.get_col_spec("oracle+vtb") == "JSON"
    assert bjson.process_result_value(memoryview(b'\x01{"a":1}'), "postgresql+vtb") == {"a": 1}


def test_sqlalchemy_dialects_dbapi_and_alembic_integration() -> None:
    """Verify +vertebra and +vtb dialect registration, plain-tuple DBAPI cursor, and Alembic hooks."""
    dialects = DialectDescriptor.register_all()
    assert len(dialects) == 8
    assert dialects["postgresql.vtb"] is PostgresDialect
    assert dialects["oracle.vtb"] is OracleDialect
    assert dialects["sqlite.vtb"] is SqliteDialect
    assert OracleDialect.use_insertmanyvalues is True
    assert OracleDialect.insert_executemany_returning is True

    dbapi = DBAPIModule()
    conn = dbapi.connect("sqlite+vtb:///:memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    cur.execute("INSERT INTO t (id, val) VALUES (?, ?)", (10, "ten"))
    cur.execute("SELECT id, val FROM t")
    row = cur.fetchone()
    assert row == (10, "ten")
    assert type(row) is tuple
    conn.close()

    impls = AlembicImpl.register_all()
    assert len(impls) == 8
    assert impls["oracle+vtb"].transactional_ddl is False
    assert impls["postgresql+vtb"].transactional_ddl is True

    class DummyAutogenContext:
        __slots__ = ("imports",)

        def __init__(self) -> None:
            self.imports: set[str] = set()

    ctx = DummyAutogenContext()
    rendered_vec = AlembicImpl.render_item("type", Vector(1536), ctx)
    assert rendered_vec == "Vector(1536, storage_format='float32')"
    assert "from vertebra.extensions.sqlalchemy import Vector" in ctx.imports
    assert AlembicImpl.compare_type(None, None, None, "VECTOR(1536)", Vector(1536)) is False
    assert AlembicImpl.compare_type(None, None, None, "JSONB", BinaryJson()) is False

    view_op = CreateViewOp("active_users_v", "SELECT id FROM users WHERE active = 1", or_replace=True)
    assert (
        view_op.to_sql("postgresql+vtb")
        == "CREATE OR REPLACE VIEW active_users_v AS SELECT id FROM users WHERE active = 1"
    )
    assert view_op.reverse().to_sql("postgresql+vtb") == "DROP VIEW IF EXISTS active_users_v"

    pg_cfg = PostgresConfig(
        dsn="postgresql://u:p@pgbouncer:6432/app",
        direct_dsn="postgresql://u:p@pg-primary:5432/app",
        pooler_mode="transaction",
    )
    assert AlembicImpl.resolve_migration_url(pg_cfg) == "postgresql://u:p@pg-primary:5432/app"

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

    ddl_conn = DBAPIConnection("sqlite+vtb:///:memory:")
    AlembicImpl.execute_ddl(ddl_conn, view_op, dialect_name="sqlite+vtb")
    ddl_conn.close()


def test_postgres_vtb_dialect_dbapi_and_compiler_features() -> None:
    """Verify PostgresVtbDialect, DBAPIModule paramstyles, compiler hooks, and cursor descriptions."""
    pg_dbapi = PostgresVtbDialect.dbapi()
    assert pg_dbapi.paramstyle == "pyformat"
    assert PostgresVtbDialect.import_dbapi().paramstyle == "pyformat"
    assert SqliteVtbDialect.dbapi().paramstyle == "qmark"
    assert MysqlVtbDialect.dbapi().paramstyle == "format"
    assert OracleVtbDialect.dbapi().paramstyle == "named"

    driver = SyncPostgresDriver(PostgresConfig(dsn="memory://"))
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
    rendered_insert = PostgresVtbDialect.compile_insert_returning_sentinel(
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
    assert PostgresVtbDialect.compile_delete_using(del_clause) == (
        "DELETE FROM orders USING archived_customers WHERE orders.customer_id = archived_customers.id"
    )

    vec = Vector(768)
    bjson = BinaryJson()
    assert PostgresVtbDialect.type_descriptor(vec) is vec
    assert PostgresVtbDialect.type_descriptor(bjson) is bjson


@pytest.mark.anyio
async def test_sqlalchemy_21_orm_optimizations_and_sync_engine_adapter() -> None:
    """Verify SelectInLoadSpec, InsertSentinelSpec, DeleteUsingClause, ParameterizedSql, and SyncEngineAdapter."""
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

    compiled = ParameterizedSql.from_template(
        ["SELECT * FROM items WHERE category = ", " AND price <= ", ""],
        ["coffee", 25.0],
    )
    assert compiled.sql == "SELECT * FROM items WHERE category = :p_1 AND price <= :p_2"
    assert compiled.params == {"p_1": "coffee", "p_2": 25.0}

    adapter = SyncEngineAdapter(max_workers=2)
    result = await adapter.run_sync(lambda x, y: x + y, 20, 22)
    assert result == 42
