"""Unit tests for SQLSpec Oracle 26ai helpers, SQLAlchemy 2.1 +vtb dialects, Alembic, and ORM optimizations."""

from __future__ import annotations

import array
import datetime
import decimal
import uuid

import pytest

from vertebra.db import OracleDataFrame, PostgresConfig
from vertebra.extensions.sqlalchemy import (
    ALEMBIC_DIALECT_IMPLS,
    DIALECT_SCHEMES,
    BinaryJson,
    CreateViewOp,
    FreeThreadedEngineAdapter,
    OracleVertebraDialect,
    PostgresVertebraDialect,
    SqliteVertebraDialect,
    Vector,
    VertebraDBAPIModule,
    build_delete_using,
    chunked_selectinload,
    compare_vertebra_type,
    compile_tstring_sql,
    configure_insertmanyvalues_sentinel,
    register_alembic_impls,
    register_sqlalchemy_dialects,
    render_vertebra_item,
    resolve_alembic_migration_url,
)
from vertebra.extensions.sqlspec import (
    SQLSpec,
    SQLSpecStorageBridge,
    SyncDatabaseConfig,
    build_oracle_hybrid_rrf_sql,
    sanitize_for_json,
    sanitize_oracle_text_query,
)
from vertebra.storage import ObjectStore


def test_oracle_json_text_and_hybrid_rrf_builders() -> None:
    """Verify sanitize_for_json, sanitize_oracle_text_query, and build_oracle_hybrid_rrf_sql."""
    payload = {
        "ts": datetime.datetime(2026, 10, 21, 12, 0, tzinfo=datetime.UTC),
        "price": decimal.Decimal("19.99"),
        "count": decimal.Decimal("5"),
        "id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "vec": array.array("f", [1.0, 2.0]),
        "raw": memoryview(b"hello"),
    }
    cleaned = sanitize_for_json(payload)
    assert cleaned["price"] == 19.99
    assert cleaned["count"] == 5
    assert cleaned["vec"] == [1.0, 2.0]
    assert cleaned["raw"] == "hello"

    assert sanitize_oracle_text_query("dark roast AND espresso; DROP TABLE!") == (
        "{dark} , {roast} , {AND} , {espresso} , {DROP} , {TABLE}"
    )
    assert sanitize_oracle_text_query("arabica beans", mode="and") == "{arabica} & {beans}"
    assert sanitize_oracle_text_query("cold brew", mode="near") == "NEAR(({cold}, {brew}))"

    rrf_sql = build_oracle_hybrid_rrf_sql(
        "products",
        select_columns=("id", "name"),
        rrf_k=60,
        candidate_limit=25,
    )
    assert "VECTOR_DISTANCE(embedding, :query_vec, COSINE)" in rrf_sql
    assert "CONTAINS(content, :text_query, 1) > 0" in rrf_sql
    assert "FULL OUTER JOIN text_candidates" in rrf_sql
    assert "FETCH FIRST :top_k ROWS ONLY" in rrf_sql


@pytest.mark.anyio
async def test_sqlspec_arrow_capsule_export_and_multipool_shutdown() -> None:
    """Verify SQLSpecStorageBridge.export_arrow_capsule and SQLSpec multi-pool shutdown."""
    store = ObjectStore("memory://")
    bridge = SQLSpecStorageBridge(store, prefix="exports")
    df = OracleDataFrame(("id", "name"), [(1, "Alpha"), (2, "Beta")])
    res = bridge.export_arrow_capsule("items.arrow", df)
    assert res.size > 0
    assert len(bridge.read_arrow_ipc("items.arrow")) == res.size

    closed_pools: list[str] = []

    class DummyPool:
        def __init__(self, name: str) -> None:
            self.name = name

        async def close(self) -> None:
            closed_pools.append(self.name)

    class DummyDriver:
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
    plugin: SQLSpec[SyncDatabaseConfig[object, DummyPool, DummyDriver]] = SQLSpec(config=[cfg1, cfg2], store=store)
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
    dialects = register_sqlalchemy_dialects()
    assert len(DIALECT_SCHEMES) == 8
    assert dialects["postgresql.vtb"] is PostgresVertebraDialect
    assert dialects["oracle.vtb"] is OracleVertebraDialect
    assert dialects["sqlite.vtb"] is SqliteVertebraDialect
    assert OracleVertebraDialect.use_insertmanyvalues is True
    assert OracleVertebraDialect.insert_executemany_returning is True

    dbapi = VertebraDBAPIModule()
    conn = dbapi.connect("sqlite+vtb:///:memory:")
    cur = conn.cursor()
    cur.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    cur.execute("INSERT INTO t (id, val) VALUES (?, ?)", (10, "ten"))
    cur.execute("SELECT id, val FROM t")
    row = cur.fetchone()
    assert row == (10, "ten")
    assert type(row) is tuple
    conn.close()

    impls = register_alembic_impls()
    assert set(ALEMBIC_DIALECT_IMPLS) == set(impls)
    assert impls["oracle+vtb"].transactional_ddl is False
    assert impls["postgresql+vtb"].transactional_ddl is True

    class DummyAutogenContext:
        def __init__(self) -> None:
            self.imports: set[str] = set()

    ctx = DummyAutogenContext()
    rendered_vec = render_vertebra_item("type", Vector(1536), ctx)
    assert rendered_vec == "Vector(1536, storage_format='float32')"
    assert "from vertebra.extensions.sqlalchemy import Vector" in ctx.imports
    assert compare_vertebra_type(None, None, None, "VECTOR(1536)", Vector(1536)) is False
    assert compare_vertebra_type(None, None, None, "JSONB", BinaryJson()) is False

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
    assert resolve_alembic_migration_url(pg_cfg) == "postgresql://u:p@pg-primary:5432/app"


@pytest.mark.anyio
async def test_sqlalchemy_21_orm_optimizations_and_free_threaded_adapter() -> None:
    """Verify chunked_selectinload, orm_insert_sentinel, Delete.using, tstring, and 3.14t adapter."""
    load_spec = chunked_selectinload("items", chunksize=500, omit_join=True)
    assert load_spec.chunksize == 500
    assert load_spec.omit_join is True
    assert load_spec.num_batches(1200) == 3

    sentinel = configure_insertmanyvalues_sentinel()
    assert sentinel.use_insertmanyvalues is True
    assert sentinel.insert_executemany_returning is True

    del_clause = build_delete_using(
        "orders", "archived_customers", where_sql="orders.customer_id = archived_customers.id"
    )
    assert del_clause.to_sql() == (
        "DELETE FROM orders USING archived_customers WHERE orders.customer_id = archived_customers.id"
    )

    compiled = compile_tstring_sql(["SELECT * FROM items WHERE category = ", " AND price <= ", ""], ["coffee", 25.0])
    assert compiled.sql == "SELECT * FROM items WHERE category = :p_1 AND price <= :p_2"
    assert compiled.params == {"p_1": "coffee", "p_2": 25.0}

    adapter = FreeThreadedEngineAdapter(max_workers=2)
    result = await adapter.run_sync(lambda x, y: x + y, 20, 22)
    assert result == 42
