"""Unit tests verifying PostgreSQL SQLSpec numeric ($N) and SQLAlchemy numeric_dollar cutover."""

from __future__ import annotations

import importlib
from typing import Any

import pytest
from sqlalchemy import Column, MetaData, String, Table, bindparam, column, create_engine, delete, insert, select, table
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.engine.interfaces import BindTyping
from sqlalchemy.ext.asyncio import create_async_engine
from sqlspec import StatementStack
from sqlspec.core.filters import LimitOffsetFilter, NullFilter
from sqlspec.core.parameters import ParameterStyle, TypedParameter
from sqlspec.core.statement import SQL
from sqlspec.exceptions import UniqueViolationError

from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra._vertebra import IntegrityError as NativeIntegrityError
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.dbapi import AsyncDBAPIConnection, SyncDBAPIConnection
from vertebra.db.rls import TenantPredicate
from vertebra.extensions.sqlalchemy.dialects import AsyncPostgreSQLDialect, SyncPostgreSQLDialect
from vertebra.extensions.sqlalchemy.types import PgVector
from vertebra.extensions.sqlspec.adapters.postgres import (
    PostgresAsyncConfig,
    PostgresAsyncDriver,
    PostgresSyncConfig,
    PostgresSyncDriver,
)
from vertebra.extensions.sqlspec.adapters.postgres.core import compile_postgres_numeric_transport


def _seed_cluster() -> FakePostgresCluster:
    """Create a FakePostgresCluster pre-seeded with the test ``items`` table."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER, name TEXT)")
    cluster._conn.execute("INSERT INTO items (id, name) VALUES (7, 'alpha')")
    return cluster


def test_postgres_sqlspec_execution_style_is_numeric() -> None:
    """Both sync and async PostgreSQL SQLSpec adapters default directly to NUMERIC ($N) binds."""
    assert PostgresSyncDriver._execution_parameter_style is ParameterStyle.NUMERIC
    assert PostgresAsyncDriver._execution_parameter_style is ParameterStyle.NUMERIC
    sync_cfg = PostgresSyncDriver._default_statement_config().parameter_config
    async_cfg = PostgresAsyncDriver._default_statement_config().parameter_config
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

    cfg = PostgresSyncDriver._default_statement_config().replace(
        enable_parsing=True,
        statement_transformers=[transformer],
    )
    adapter = PostgresSyncDriver(native, statement_config=cfg)
    try:
        prepared, compiled_sql, bound = compile_postgres_numeric_transport(
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
        adapter = PostgresSyncDriver(
            native,
            statement_config=PostgresSyncConfig(native.config).statement_config,
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
        adapter = PostgresAsyncDriver(
            native,
            statement_config=PostgresAsyncConfig(native.config).statement_config,
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
    assert not hasattr(dialect_cls, "dbapi")
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
        adapter_async = PostgresAsyncDriver(
            native_async,
            statement_config=PostgresAsyncConfig(cfg).statement_config,
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
            assert inserted.rows_affected == 2
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
            assert updated.rows_affected == 1
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
    adapter_sync = PostgresSyncDriver(
        native_sync,
        statement_config=PostgresSyncConfig(cfg).statement_config,
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
        assert inserted.rows_affected == 2
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
        assert updated.rows_affected == 1
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
        adapter_async = PostgresAsyncDriver(
            native_async,
            statement_config=PostgresAsyncConfig(cfg).statement_config,
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
    adapter_sync = PostgresSyncDriver(
        native_sync,
        statement_config=PostgresSyncConfig(cfg).statement_config,
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


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_postgres_autocommit_isolation_and_bind_casts(asynchronous: bool) -> None:
    """Verify PostgreSQL AUTOCOMMIT isolation, BindTyping.RENDER_CASTS, and Core DML compilation."""
    dialect_cls: Any = AsyncPostgreSQLDialect if asynchronous else SyncPostgreSQLDialect
    dialect = dialect_cls(dbapi=dialect_cls.import_dbapi())
    assert dialect.bind_typing is BindTyping.RENDER_CASTS
    assert "AUTOCOMMIT" in dialect.get_isolation_level_values(None)

    meta = MetaData()
    tbl = Table(
        "records",
        meta,
        Column("id", UUID()),
        Column("payload", JSONB()),
        Column("tags", ARRAY(String())),
        Column("vec", PgVector(3)),
    )
    sel_sql = str(
        select(tbl)
        .where(
            tbl.c.id == bindparam("id", "00000000-0000-0000-0000-000000000001", type_=UUID()),
            tbl.c.payload == bindparam("payload", {"k": "v"}, type_=JSONB()),
            tbl.c.tags == bindparam("tags", ["a"], type_=ARRAY(String())),
            tbl.c.vec == bindparam("vec", [1.0, 2.0, 3.0], type_=PgVector(3)),
        )
        .compile(dialect=dialect)
    )
    assert "::UUID" in sel_sql
    assert "::JSONB" in sel_sql
    assert "::VARCHAR[]" in sel_sql
    assert "::VECTOR" in sel_sql

    del_sql = str(delete(tbl).where(tbl.c.id == bindparam("id", type_=UUID())).compile(dialect=dialect))
    assert del_sql == "DELETE FROM records WHERE records.id = $1::UUID"

    ins_sql = str(
        insert(tbl)
        .values(id="00000000-0000-0000-0000-000000000001", payload={"k": "v"})
        .returning(tbl.c.id)
        .compile(dialect=dialect)
    )
    assert "INSERT INTO records" in ins_sql
    assert "RETURNING records.id" in ins_sql

    class _SyncFakeCursor:
        def __init__(self, driver: Any) -> None:
            self._driver = driver
            self._rows: list[tuple[Any, ...]] = []
            self.description: tuple[tuple[Any, ...], ...] | None = None
            self.rowcount: int = -1
            self.arraysize: int = 1
            self.lastrowid: Any = None

        def execute(self, sql: str, parameters: Any = ()) -> None:
            rows, cols = self._driver.execute_with_description(sql, parameters)
            if cols is not None:
                self._rows = list(rows)
                self.description = tuple((str(col), None, None, None, None, None, None) for col in cols)
                self.rowcount = len(self._rows)
            else:
                self._rows = []
                self.description = None
                self.rowcount = 0

        def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        def fetchall(self) -> list[tuple[Any, ...]]:
            out = list(self._rows)
            self._rows.clear()
            return out

        def close(self) -> None:
            self._rows.clear()

    class _AsyncFakeCursor:
        def __init__(self, driver: Any) -> None:
            self._driver = driver
            self._rows: list[tuple[Any, ...]] = []
            self.description: tuple[tuple[Any, ...], ...] | None = None
            self.rowcount: int = -1
            self.arraysize: int = 1
            self.lastrowid: Any = None

        async def execute(self, sql: str, parameters: Any = ()) -> None:
            rows, cols = await self._driver.execute_with_description(sql, parameters)
            if cols is not None:
                self._rows = list(rows)
                self.description = tuple((str(col), None, None, None, None, None, None) for col in cols)
                self.rowcount = len(self._rows)
            else:
                self._rows = []
                self.description = None
                self.rowcount = 0

        async def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        async def fetchall(self) -> list[tuple[Any, ...]]:
            out = list(self._rows)
            self._rows.clear()
            return out

        async def close(self) -> None:
            self._rows.clear()

    class _SyncFakePGDriver:
        in_transaction: bool = False

        def __init__(self) -> None:
            self.queries: list[str] = []

        def cursor(self, *, lazy_begin: bool = False) -> _SyncFakeCursor:
            _ = lazy_begin
            return _SyncFakeCursor(self)

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = parameters
            self.queries.append(sql)
            lowered = sql.lower()
            if "pg_catalog.version" in lowered:
                return [("PostgreSQL 16.4 on x86_64",)], ("version",)
            if "current_schema" in lowered:
                return [("public",)], ("schema",)
            if "standard_conforming_strings" in lowered:
                return [("on",)], ("standard_conforming_strings",)
            if "show transaction isolation level" in lowered:
                return [("read committed",)], ("transaction_isolation",)
            return [], None

        def execute(self, sql: str, parameters: Any = ()) -> int:
            _ = parameters
            self.queries.append(sql)
            return 0

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    class _AsyncFakePGDriver:
        in_transaction: bool = False

        def __init__(self) -> None:
            self.queries: list[str] = []

        def cursor(self, *, lazy_begin: bool = False) -> _AsyncFakeCursor:
            _ = lazy_begin
            return _AsyncFakeCursor(self)

        async def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = parameters
            self.queries.append(sql)
            lowered = sql.lower()
            if "pg_catalog.version" in lowered:
                return [("PostgreSQL 16.4 on x86_64",)], ("version",)
            if "current_schema" in lowered:
                return [("public",)], ("schema",)
            if "standard_conforming_strings" in lowered:
                return [("on",)], ("standard_conforming_strings",)
            if "show transaction isolation level" in lowered:
                return [("read committed",)], ("transaction_isolation",)
            return [], None

        async def execute(self, sql: str, parameters: Any = ()) -> int:
            _ = parameters
            self.queries.append(sql)
            return 0

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            return None

    if asynchronous:
        async_driver = _AsyncFakePGDriver()
        async_raw = AsyncDBAPIConnection("postgresql+vtb_async://localhost/app", driver=async_driver)
        async_engine = create_async_engine(
            "postgresql+vtb_async://localhost/app",
            isolation_level="AUTOCOMMIT",
            async_creator=lambda: async_raw,
        )

        def _set_read_committed(sync_proxy: Any) -> None:
            dbapi_conn = sync_proxy.connection.dbapi_connection
            assert dbapi_conn is not None
            async_engine.dialect.set_isolation_level(dbapi_conn, "READ COMMITTED")

        try:
            async with async_engine.connect() as async_conn:
                proxied = await async_conn.get_raw_connection()
                raw_conn = proxied.dbapi_connection
                assert raw_conn is not None
                assert async_engine.dialect.detect_autocommit_setting(raw_conn) is True
                assert async_raw.autocommit is True
                assert not any("SET SESSION CHARACTERISTICS" in q for q in async_driver.queries)
                await async_conn.run_sync(_set_read_committed)
                assert async_raw.autocommit is False
                assert any(
                    "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED" in q
                    for q in async_driver.queries
                )
        finally:
            await async_engine.dispose()
        return

    sync_driver = _SyncFakePGDriver()
    sync_raw = SyncDBAPIConnection("postgresql+vtb://localhost/app", driver=sync_driver)
    sync_engine = create_engine(
        "postgresql+vtb://localhost/app",
        isolation_level="AUTOCOMMIT",
        creator=lambda: sync_raw,
    )
    try:
        with sync_engine.connect() as sync_conn:
            raw_conn = sync_conn.connection.dbapi_connection
            assert raw_conn is not None
            assert sync_engine.dialect.detect_autocommit_setting(raw_conn) is True
            assert sync_raw.autocommit is True
            assert not any("SET SESSION CHARACTERISTICS" in q for q in sync_driver.queries)
            sync_engine.dialect.set_isolation_level(raw_conn, "READ COMMITTED")
            assert sync_raw.autocommit is False
            assert any(
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED" in q
                for q in sync_driver.queries
            )
    finally:
        sync_engine.dispose()


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_postgres_server_version_and_is_disconnect(asynchronous: bool) -> None:
    """Verify PostgreSQL 57P01-57P03 and 08* disconnect detection and dynamic insert_executemany_returning."""
    dialect_cls: Any = AsyncPostgreSQLDialect if asynchronous else SyncPostgreSQLDialect
    assert "insert_executemany_returning" not in dialect_cls.__dict__

    dialect = dialect_cls()
    assert dialect.insert_executemany_returning is True
    dynamic_off = dialect_cls(use_insertmanyvalues=False)
    assert dynamic_off.insert_executemany_returning is False

    class _PGStateErr(Exception):
        sqlstate: str

    for code in ("57P01", "57P02", "57P03", "08006"):
        err = _PGStateErr("pg disconnect")
        err.sqlstate = code
        assert dialect.is_disconnect(err) is True

    for code in ("40001", "40P01", "23505"):
        err = _PGStateErr("pg non-disconnect")
        err.sqlstate = code
        assert dialect.is_disconnect(err) is False

    class _BrokenStrErr(Exception):
        def __str__(self) -> str:
            raise RuntimeError("boom")

    assert dialect.is_disconnect(_BrokenStrErr()) is False

    class _SyncVersionCursor:
        def __init__(self, driver: Any) -> None:
            self._driver = driver
            self._rows: list[tuple[Any, ...]] = []
            self.description: tuple[tuple[Any, ...], ...] | None = None
            self.rowcount: int = -1
            self.arraysize: int = 1
            self.lastrowid: Any = None

        def execute(self, sql: str, parameters: Any = ()) -> None:
            rows, cols = self._driver.execute_with_description(sql, parameters)
            if cols is not None:
                self._rows = list(rows)
                self.description = tuple((str(col), None, None, None, None, None, None) for col in cols)
                self.rowcount = len(self._rows)
            else:
                self._rows = []
                self.description = None
                self.rowcount = 0

        def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        def fetchall(self) -> list[tuple[Any, ...]]:
            out = list(self._rows)
            self._rows.clear()
            return out

        def close(self) -> None:
            self._rows.clear()

    class _AsyncVersionCursor:
        def __init__(self, driver: Any) -> None:
            self._driver = driver
            self._rows: list[tuple[Any, ...]] = []
            self.description: tuple[tuple[Any, ...], ...] | None = None
            self.rowcount: int = -1
            self.arraysize: int = 1
            self.lastrowid: Any = None

        async def execute(self, sql: str, parameters: Any = ()) -> None:
            rows, cols = await self._driver.execute_with_description(sql, parameters)
            if cols is not None:
                self._rows = list(rows)
                self.description = tuple((str(col), None, None, None, None, None, None) for col in cols)
                self.rowcount = len(self._rows)
            else:
                self._rows = []
                self.description = None
                self.rowcount = 0

        async def fetchone(self) -> tuple[Any, ...] | None:
            return self._rows.pop(0) if self._rows else None

        async def fetchall(self) -> list[tuple[Any, ...]]:
            out = list(self._rows)
            self._rows.clear()
            return out

        async def close(self) -> None:
            self._rows.clear()

    class _SyncVersionDriver:
        in_transaction: bool = False

        def cursor(self, *, lazy_begin: bool = False) -> _SyncVersionCursor:
            _ = lazy_begin
            return _SyncVersionCursor(self)

        def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = parameters
            lowered = sql.lower()
            if "pg_catalog.version" in lowered:
                return [("PostgreSQL 16.4 on x86_64",)], ("version",)
            if "current_schema" in lowered:
                return [("public",)], ("schema",)
            if "standard_conforming_strings" in lowered:
                return [("on",)], ("standard_conforming_strings",)
            if "show transaction isolation level" in lowered:
                return [("read committed",)], ("transaction_isolation",)
            return [], None

        def commit(self) -> None:
            return None

        def rollback(self) -> None:
            return None

        def close(self) -> None:
            return None

    class _AsyncVersionDriver:
        in_transaction: bool = False

        def cursor(self, *, lazy_begin: bool = False) -> _AsyncVersionCursor:
            _ = lazy_begin
            return _AsyncVersionCursor(self)

        async def execute_with_description(self, sql: str, parameters: Any = ()) -> tuple[list[tuple[Any, ...]], Any]:
            _ = parameters
            lowered = sql.lower()
            if "pg_catalog.version" in lowered:
                return [("PostgreSQL 16.4 on x86_64",)], ("version",)
            if "current_schema" in lowered:
                return [("public",)], ("schema",)
            if "standard_conforming_strings" in lowered:
                return [("on",)], ("standard_conforming_strings",)
            if "show transaction isolation level" in lowered:
                return [("read committed",)], ("transaction_isolation",)
            return [], None

        async def commit(self) -> None:
            return None

        async def rollback(self) -> None:
            return None

        async def close(self) -> None:
            return None

    if asynchronous:
        async_engine = create_async_engine(
            "postgresql+vtb_async://localhost/app",
            async_creator=lambda: AsyncDBAPIConnection(
                "postgresql+vtb_async://localhost/app", driver=_AsyncVersionDriver()
            ),
        )
        try:
            async with async_engine.connect():
                assert async_engine.dialect.server_version_info == (16, 4)
                assert async_engine.dialect.insert_executemany_returning is True
        finally:
            await async_engine.dispose()
        return

    sync_engine = create_engine(
        "postgresql+vtb://localhost/app",
        creator=lambda: SyncDBAPIConnection("postgresql+vtb://localhost/app", driver=_SyncVersionDriver()),
    )
    try:
        with sync_engine.connect():
            assert sync_engine.dialect.server_version_info == (16, 4)
            assert sync_engine.dialect.insert_executemany_returning is True
    finally:
        sync_engine.dispose()


def test_postgres_adapter_holds_no_parameter_normalizers() -> None:
    """Verify PostgreSQL SQLSpec core removes parameter normalizers and drivers omit _prepare_native_parameters."""
    pg_core = importlib.import_module("vertebra.extensions.sqlspec.adapters.postgres.core")
    deleted_names = (
        "unwrap_typed_parameters",
        "_normalize_bound_params",
        "_unwrap_scalar",
        "normalize_pg_parameters",
        "normalize_pg_many_parameters",
    )
    for name in deleted_names:
        assert not hasattr(pg_core, name)
        assert name not in pg_core.__all__

    for driver_cls in (PostgresSyncDriver, PostgresAsyncDriver):
        assert "_prepare_native_parameters" not in driver_cls.__dict__


@pytest.mark.anyio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_postgres_native_cursor_cache_hit_crud_typed_param_and_dbapi(asynchronous: bool) -> None:
    """Verify PostgreSQL SQLSpec cache hit performs one native execute, CRUD/many/TypedParameter/errors, and DB-API cursor."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, payload TEXT)")
    cluster._conn.execute("INSERT INTO items (id, name, payload) VALUES (7, 'alpha', '{\"k\": 1}')")
    cfg = PostgreSQLConfig(dsn="postgresql://localhost:5432/app")

    counts = {"execute": 0, "fetchall": 0}

    class _SyncCountingCursor:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        @property
        def description(self) -> Any:
            return self._inner.description

        @property
        def rowcount(self) -> int:
            return int(self._inner.rowcount)

        @property
        def lastrowid(self) -> Any:
            return self._inner.lastrowid

        def execute(
            self,
            sql: str,
            params: Any = None,
            *,
            parameter_oids: Any = None,
            timeout: Any = None,
        ) -> None:
            counts["execute"] += 1
            if "DUPLICATE_TRIGGER" in sql:
                err = NativeIntegrityError("duplicate key value violates unique constraint")
                err.sqlstate = "23505"
                raise err
            self._inner.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)

        def executemany(self, sql: str, param_sets: Any) -> None:
            self._inner.executemany(sql, param_sets)

        def fetchall(self) -> list[tuple[Any, ...]]:
            counts["fetchall"] += 1
            return list(self._inner.fetchall())

        def close(self) -> None:
            self._inner.close()

    class _AsyncCountingCursor:
        def __init__(self, inner: Any) -> None:
            self._inner = inner

        @property
        def description(self) -> Any:
            return self._inner.description

        @property
        def rowcount(self) -> int:
            return int(self._inner.rowcount)

        @property
        def lastrowid(self) -> Any:
            return self._inner.lastrowid

        async def execute(
            self,
            sql: str,
            params: Any = None,
            *,
            parameter_oids: Any = None,
            timeout: Any = None,
        ) -> None:
            counts["execute"] += 1
            if "DUPLICATE_TRIGGER" in sql:
                err = NativeIntegrityError("duplicate key value violates unique constraint")
                err.sqlstate = "23505"
                raise err
            await self._inner.execute(sql, params, parameter_oids=parameter_oids, timeout=timeout)

        async def executemany(self, sql: str, param_sets: Any) -> None:
            await self._inner.executemany(sql, param_sets)

        async def fetchall(self) -> list[tuple[Any, ...]]:
            counts["fetchall"] += 1
            return list(await self._inner.fetchall())

        async def close(self) -> None:
            await self._inner.close()

    if asynchronous:

        class _CountingAsyncPool(FakeAsyncPostgresPool):
            def cursor(self, *, lazy_begin: bool = False) -> Any:
                return _AsyncCountingCursor(super().cursor(lazy_begin=lazy_begin))

        native_async = AsyncPostgreSQLDriver(cfg, pool=_CountingAsyncPool(cluster))
        adapter_async = PostgresAsyncDriver(
            native_async,
            statement_config=PostgresAsyncConfig(cfg).statement_config,
        )
        try:
            assert await adapter_async.select("SELECT 1 AS a") == [{"a": 1}]
            assert await adapter_async.select("SELECT id, name FROM items WHERE id = ?", 7) == [
                {"id": 7, "name": "alpha"}
            ]
            counts["execute"] = 0
            counts["fetchall"] = 0
            assert await adapter_async.select("SELECT id, name FROM items WHERE id = ?", 7) == [
                {"id": 7, "name": "alpha"}
            ]
            assert counts == {"execute": 1, "fetchall": 1}

            upd = await adapter_async.execute("UPDATE items SET name = ? WHERE id = ?", ("beta", 7))
            assert upd.rows_affected == 1

            many = await adapter_async.execute_many(
                "INSERT INTO items (id, name, payload) VALUES (?, ?, ?)",
                [(10, "x", "{}"), (11, "y", "{}"), (12, "z", "{}")],
            )
            assert many.rows_affected == 3

            await adapter_async.execute(
                "INSERT INTO items (id, name, payload) VALUES (?, ?, ?)",
                (20, "json-row", TypedParameter('{"nested": true}', str, "payload")),
            )
            assert await adapter_async.select_value("SELECT payload FROM items WHERE id = ?", 20) == '{"nested": true}'

            with pytest.raises(UniqueViolationError):
                await adapter_async.execute("INSERT INTO items (id) VALUES (7) /* DUPLICATE_TRIGGER */")

            dbapi_async = AsyncDBAPIConnection(
                "postgres+vtb_async://localhost/app", driver=native_async, share_pool=False
            )
            try:
                cur = dbapi_async.cursor()
                assert isinstance(cur, _AsyncCountingCursor)
            finally:
                await dbapi_async.close()
        finally:
            await native_async.close()
        return

    class _CountingSyncPool(FakeSyncPostgresPool):
        def cursor(self, *, lazy_begin: bool = False) -> Any:
            return _SyncCountingCursor(super().cursor(lazy_begin=lazy_begin))

    native_sync = SyncPostgreSQLDriver(cfg, pool=_CountingSyncPool(cluster))
    adapter_sync = PostgresSyncDriver(
        native_sync,
        statement_config=PostgresSyncConfig(cfg).statement_config,
    )
    try:
        assert adapter_sync.select("SELECT 1 AS a") == [{"a": 1}]
        assert adapter_sync.select("SELECT id, name FROM items WHERE id = ?", 7) == [{"id": 7, "name": "alpha"}]
        counts["execute"] = 0
        counts["fetchall"] = 0
        assert adapter_sync.select("SELECT id, name FROM items WHERE id = ?", 7) == [{"id": 7, "name": "alpha"}]
        assert counts == {"execute": 1, "fetchall": 1}

        upd = adapter_sync.execute("UPDATE items SET name = ? WHERE id = ?", ("beta", 7))
        assert upd.rows_affected == 1

        many = adapter_sync.execute_many(
            "INSERT INTO items (id, name, payload) VALUES (?, ?, ?)",
            [(10, "x", "{}"), (11, "y", "{}"), (12, "z", "{}")],
        )
        assert many.rows_affected == 3

        adapter_sync.execute(
            "INSERT INTO items (id, name, payload) VALUES (?, ?, ?)",
            (20, "json-row", TypedParameter('{"nested": true}', str, "payload")),
        )
        assert adapter_sync.select_value("SELECT payload FROM items WHERE id = ?", 20) == '{"nested": true}'

        with pytest.raises(UniqueViolationError):
            adapter_sync.execute("INSERT INTO items (id) VALUES (7) /* DUPLICATE_TRIGGER */")

        dbapi_sync = SyncDBAPIConnection("postgres+vtb://localhost/app", driver=native_sync, share_pool=False)
        try:
            cur = dbapi_sync.cursor()
            assert isinstance(cur, _SyncCountingCursor)
        finally:
            dbapi_sync.close()
    finally:
        native_sync.close()
