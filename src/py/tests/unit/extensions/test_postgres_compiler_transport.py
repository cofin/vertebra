"""Unit tests verifying PostgreSQL SQLSpec numeric ($N) and SQLAlchemy numeric_dollar cutover."""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import bindparam, column, select, table
from sqlspec import StatementStack
from sqlspec.core.filters import LimitOffsetFilter, NullFilter
from sqlspec.core.parameters import ParameterStyle, TypedParameter
from sqlspec.core.statement import SQL

from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlalchemy.dialects import AsyncPostgreSQLDialect, SyncPostgreSQLDialect
from vertebra.extensions.sqlspec.adapters import (
    PostgreSQLAsyncConfig,
    PostgreSQLAsyncDriver,
    PostgreSQLSyncConfig,
    PostgreSQLSyncDriver,
    _compile_postgres_numeric_transport,
)


def _seed_cluster() -> FakePostgresCluster:
    """Create a FakePostgresCluster pre-seeded with the test ``items`` table."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    cluster._conn.execute("INSERT INTO items (id, name) VALUES (7, 'alpha')")
    return cluster


def test_postgres_sqlspec_execution_style_is_numeric() -> None:
    """Both sync and async PostgreSQL SQLSpec adapters default directly to NUMERIC ($N) binds."""
    assert PostgreSQLSyncDriver._execution_parameter_style is ParameterStyle.NUMERIC
    assert PostgreSQLAsyncDriver._execution_parameter_style is ParameterStyle.NUMERIC
    sync_cfg = PostgreSQLSyncDriver._default_statement_config().parameter_config
    async_cfg = PostgreSQLAsyncDriver._default_statement_config().parameter_config
    assert sync_cfg.default_execution_parameter_style is ParameterStyle.NUMERIC
    assert async_cfg.default_execution_parameter_style is ParameterStyle.NUMERIC


def test_compile_postgres_numeric_transport_preserves_transformers_filters_and_unwraps_typed() -> None:
    """Helper preserves statement_transformers, applies StatementFilters once, and unwraps TypedParameter."""
    cluster = _seed_cluster()
    pool = FakeSyncPostgresPool(cluster)
    native = SyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://localhost:5432/app"), pool=pool)
    seen_transforms: list[tuple[str, Any]] = []

    def transformer(expression: Any, parameters: Any) -> tuple[Any, Any]:
        seen_transforms.append((type(expression).__name__, parameters))
        return expression, parameters

    cfg = PostgreSQLSyncDriver._default_statement_config().replace(
        enable_parsing=True,
        statement_transformers=[transformer],
    )
    adapter = PostgreSQLSyncDriver(native, statement_config=cfg)
    try:
        prepared, compiled_sql, bound = _compile_postgres_numeric_transport(
            adapter,
            SQL("SELECT id, name FROM items WHERE id = ?", TypedParameter(7, int, "item_id"), statement_config=cfg),
            (LimitOffsetFilter(limit=10, offset=5),),
        )
        assert prepared.statement_config is cfg
        assert compiled_sql == "SELECT id, name FROM items WHERE id = $1 LIMIT $2 OFFSET $3"
        assert tuple(bound) == (7, 10, 5)
        assert len(seen_transforms) >= 1

        rows = adapter.select(
            "SELECT id, name FROM items WHERE id = ?",
            TypedParameter(7, int, "item_id"),
            LimitOffsetFilter(limit=5, offset=0),
        )
        assert rows == [{"id": 7, "name": "alpha"}]
        assert cluster.recorded_queries[-1][:2] == (
            "SELECT id, name FROM items WHERE id = $1 LIMIT $2 OFFSET $3",
            (7, 5, 0),
        )
    finally:
        native.close()


def test_postgres_sync_stack_and_rls_emit_numeric_dollar_binds() -> None:
    """Sync SQLSpec pipeline stack and tenant RLS setter execute with $N binds and unwrapped params."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    native = SyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost:5432/app"),
        pool=FakeSyncPostgresPool(cluster),
    )
    try:
        adapter = PostgreSQLSyncDriver(
            native,
            statement_config=PostgreSQLSyncConfig(native.config).statement_config,
        )
        results = adapter.execute_stack(
            StatementStack()
            .push_execute("INSERT INTO items (id, name) VALUES (?, ?)", TypedParameter(7, int, "id"), "alpha")
            .push_execute("SELECT id, name FROM items WHERE id = ?", 7, LimitOffsetFilter(limit=1, offset=0))
        )
        assert results[0].rows_affected == 1
        assert results[1].get_result().get_data() == [{"id": 7, "name": "alpha"}]
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries[:2]] == [
            ("INSERT INTO items (id, name) VALUES ($1, $2)", (7, "alpha")),
            ("SELECT id, name FROM items WHERE id = $1 LIMIT $2 OFFSET $3", (7, 1, 0)),
        ]

        cluster.recorded_queries.clear()
        predicate_sql = adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))
        assert predicate_sql == "o.tenant_id = $1"
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries] == [
            ("SELECT set_config($1, $2, $3)", ("app.tenant_id", "acme", True))
        ]
    finally:
        native.close()


@pytest.mark.anyio
async def test_postgres_async_stack_and_rls_emit_numeric_dollar_binds() -> None:
    """Async SQLSpec pipeline stack and tenant RLS setter execute with $N binds and unwrapped params."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    native = AsyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost:5432/app"),
        pool=FakeAsyncPostgresPool(cluster),
    )
    try:
        adapter = PostgreSQLAsyncDriver(
            native,
            statement_config=PostgreSQLAsyncConfig(native.config).statement_config,
        )
        results = await adapter.execute_stack(
            StatementStack()
            .push_execute("INSERT INTO items (id, name) VALUES (?, ?)", TypedParameter(7, int, "id"), "alpha")
            .push_execute("SELECT id, name FROM items WHERE id = ?", 7, LimitOffsetFilter(limit=1, offset=0))
        )
        assert results[0].rows_affected == 1
        assert results[1].get_result().get_data() == [{"id": 7, "name": "alpha"}]
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries[:2]] == [
            ("INSERT INTO items (id, name) VALUES ($1, $2)", (7, "alpha")),
            ("SELECT id, name FROM items WHERE id = $1 LIMIT $2 OFFSET $3", (7, 1, 0)),
        ]

        cluster.recorded_queries.clear()
        predicate_sql = await adapter.apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))
        assert predicate_sql == "o.tenant_id = $1"
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries] == [
            ("SELECT set_config($1, $2, $3)", ("app.tenant_id", "acme", True))
        ]
    finally:
        await native.close()


@pytest.mark.parametrize("dialect_cls", [SyncPostgreSQLDialect, AsyncPostgreSQLDialect])
def test_sqlalchemy_postgres_dialects_compile_numeric_dollar(dialect_cls: type[Any]) -> None:
    """Both sync and async SQLAlchemy PostgreSQL dialects use numeric_dollar and compile $1..$N binds."""
    assert dialect_cls.default_paramstyle == "numeric_dollar"
    assert dialect_cls.paramstyle == "numeric_dollar"
    assert dialect_cls.dbapi().paramstyle == "numeric_dollar"
    assert dialect_cls.import_dbapi().paramstyle == "numeric_dollar"

    dialect = dialect_cls(dbapi=dialect_cls.import_dbapi())
    assert dialect.paramstyle == "numeric_dollar"
    assert dialect.positional is True

    items = table("items", column("id"), column("name"))
    stmt = select(items.c.id).where(
        items.c.id == bindparam("id", 7),
        items.c.name == bindparam("name", "alpha"),
    )
    compiled = stmt.compile(dialect=dialect)
    assert "$1" in compiled.string
    assert "$2" in compiled.string
    assert compiled.positiontup == ["id", "name"]


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_postgres_execute_many_and_stack_many_compile_numeric_filters_and_typed_params(
    asynchronous: bool,
) -> None:
    """Sync and async execute_many and StatementStack.push_execute_many compile $N binds, filters, and TypedParameter."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER, name TEXT, deleted_at TEXT)")
    cfg = PostgreSQLConfig(dsn="postgresql://localhost:5432/app")
    seen_transforms: list[str] = []

    def transformer(expression: Any, parameters: Any) -> tuple[Any, Any]:
        seen_transforms.append(type(expression).__name__)
        return expression, parameters

    if asynchronous:
        native_async = AsyncPostgreSQLDriver(cfg, pool=FakeAsyncPostgresPool(cluster))
        adapter_async = PostgreSQLAsyncDriver(
            native_async,
            statement_config=PostgreSQLAsyncConfig(cfg).statement_config,
        )
        custom_cfg = adapter_async.statement_config.replace(
            enable_parsing=True,
            statement_transformers=[transformer],
        )
        try:
            inserted = await adapter_async.execute_many(
                "INSERT INTO items (id, name, deleted_at) VALUES (?, ?, ?)",
                [
                    (TypedParameter(1, int, "id"), TypedParameter("alpha", str, "name"), None),
                    (TypedParameter(2, int, "id"), "beta", "2026-01-01"),
                ],
                statement_config=custom_cfg,
            )
            assert inserted == 2
            assert seen_transforms == ["Insert"]
            assert [(sql, params) for sql, params, _ in cluster.recorded_queries] == [
                ("INSERT INTO items (id, name, deleted_at) VALUES ($1, $2, $3)", (1, "alpha", None)),
                ("INSERT INTO items (id, name, deleted_at) VALUES ($1, $2, $3)", (2, "beta", "2026-01-01")),
            ]

            cluster.recorded_queries.clear()
            updated = await adapter_async.execute_many(
                "UPDATE items SET name = ? WHERE id = ?",
                [
                    (TypedParameter("alpha-v2", str, "name"), TypedParameter(1, int, "id")),
                    (TypedParameter("beta-v2", str, "name"), TypedParameter(2, int, "id")),
                ],
                NullFilter("deleted_at"),
            )
            assert updated == 1
            assert [(sql, params) for sql, params, _ in cluster.recorded_queries] == [
                (
                    "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                    ("alpha-v2", 1),
                ),
                (
                    "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                    ("beta-v2", 2),
                ),
            ]

            cluster.recorded_queries.clear()
            stack_results = await adapter_async.execute_stack(
                StatementStack().push_execute_many(
                    "UPDATE items SET name = ? WHERE id = ?",
                    [
                        (TypedParameter("alpha-v3", str, "name"), TypedParameter(1, int, "id")),
                        (TypedParameter("beta-v3", str, "name"), TypedParameter(2, int, "id")),
                    ],
                    NullFilter("deleted_at"),
                )
            )
            assert stack_results[0].rows_affected == 1
            assert [(sql, params) for sql, params, _ in cluster.recorded_queries if sql.startswith("UPDATE")] == [
                (
                    "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                    ("alpha-v3", 1),
                ),
                (
                    "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                    ("beta-v3", 2),
                ),
            ]
            assert cluster._conn.execute("SELECT id, name FROM items ORDER BY id").fetchall() == [
                (1, "alpha-v3"),
                (2, "beta"),
            ]
        finally:
            await native_async.close()
        return

    native_sync = SyncPostgreSQLDriver(cfg, pool=FakeSyncPostgresPool(cluster))
    adapter_sync = PostgreSQLSyncDriver(
        native_sync,
        statement_config=PostgreSQLSyncConfig(cfg).statement_config,
    )
    custom_cfg = adapter_sync.statement_config.replace(
        enable_parsing=True,
        statement_transformers=[transformer],
    )
    try:
        inserted = adapter_sync.execute_many(
            "INSERT INTO items (id, name, deleted_at) VALUES (?, ?, ?)",
            [
                (TypedParameter(1, int, "id"), TypedParameter("alpha", str, "name"), None),
                (TypedParameter(2, int, "id"), "beta", "2026-01-01"),
            ],
            statement_config=custom_cfg,
        )
        assert inserted == 2
        assert seen_transforms == ["Insert"]
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries] == [
            ("INSERT INTO items (id, name, deleted_at) VALUES ($1, $2, $3)", (1, "alpha", None)),
            ("INSERT INTO items (id, name, deleted_at) VALUES ($1, $2, $3)", (2, "beta", "2026-01-01")),
        ]

        cluster.recorded_queries.clear()
        updated = adapter_sync.execute_many(
            "UPDATE items SET name = ? WHERE id = ?",
            [
                (TypedParameter("alpha-v2", str, "name"), TypedParameter(1, int, "id")),
                (TypedParameter("beta-v2", str, "name"), TypedParameter(2, int, "id")),
            ],
            NullFilter("deleted_at"),
        )
        assert updated == 1
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries] == [
            (
                "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                ("alpha-v2", 1),
            ),
            (
                "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                ("beta-v2", 2),
            ),
        ]

        cluster.recorded_queries.clear()
        stack_results = adapter_sync.execute_stack(
            StatementStack().push_execute_many(
                "UPDATE items SET name = ? WHERE id = ?",
                [
                    (TypedParameter("alpha-v3", str, "name"), TypedParameter(1, int, "id")),
                    (TypedParameter("beta-v3", str, "name"), TypedParameter(2, int, "id")),
                ],
                NullFilter("deleted_at"),
            )
        )
        assert stack_results[0].rows_affected == 1
        assert [(sql, params) for sql, params, _ in cluster.recorded_queries if sql.startswith("UPDATE")] == [
            (
                "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                ("alpha-v3", 1),
            ),
            (
                "UPDATE items SET name = $1 WHERE id = $2 AND deleted_at IS NULL",
                ("beta-v3", 2),
            ),
        ]
        assert cluster._conn.execute("SELECT id, name FROM items ORDER BY id").fetchall() == [
            (1, "alpha-v3"),
            (2, "beta"),
        ]
    finally:
        native_sync.close()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("method_name", ["select_one", "select_one_or_none", "select_value"])
async def test_postgres_select_one_family_forwards_statement_config(
    asynchronous: bool,
    method_name: str,
) -> None:
    """select_one, select_one_or_none, and select_value forward statement_config to select."""
    cluster = _seed_cluster()
    cfg = PostgreSQLConfig(dsn="postgresql://localhost:5432/app")
    seen_transforms: list[str] = []

    def transformer(expression: Any, parameters: Any) -> tuple[Any, Any]:
        seen_transforms.append(type(expression).__name__)
        return expression, parameters

    if asynchronous:
        native_async = AsyncPostgreSQLDriver(cfg, pool=FakeAsyncPostgresPool(cluster))
        adapter_async = PostgreSQLAsyncDriver(
            native_async,
            statement_config=PostgreSQLAsyncConfig(cfg).statement_config,
        )
        override_cfg = adapter_async.statement_config.replace(
            enable_parsing=True,
            statement_transformers=[transformer],
        )
        try:
            fn_async = getattr(adapter_async, method_name)
            out = await fn_async(
                "SELECT name FROM items WHERE id = ?",
                TypedParameter(7, int, "id"),
                statement_config=override_cfg,
            )
            expected = "alpha" if method_name == "select_value" else {"name": "alpha"}
            assert out == expected
            assert seen_transforms == ["Select"]
            assert cluster.recorded_queries[-1][:2] == ("SELECT name FROM items WHERE id = $1", (7,))
        finally:
            await native_async.close()
        return

    native_sync = SyncPostgreSQLDriver(cfg, pool=FakeSyncPostgresPool(cluster))
    adapter_sync = PostgreSQLSyncDriver(
        native_sync,
        statement_config=PostgreSQLSyncConfig(cfg).statement_config,
    )
    override_cfg = adapter_sync.statement_config.replace(
        enable_parsing=True,
        statement_transformers=[transformer],
    )
    try:
        fn_sync = getattr(adapter_sync, method_name)
        out = fn_sync(
            "SELECT name FROM items WHERE id = ?",
            TypedParameter(7, int, "id"),
            statement_config=override_cfg,
        )
        expected = "alpha" if method_name == "select_value" else {"name": "alpha"}
        assert out == expected
        assert seen_transforms == ["Select"]
        assert cluster.recorded_queries[-1][:2] == ("SELECT name FROM items WHERE id = $1", (7,))
    finally:
        native_sync.close()
