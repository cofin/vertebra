"""Unit tests for SQLSpec StatementStack execution, native binds, and PostgreSQL/Oracle adapter stacks."""

from __future__ import annotations

import contextlib
import importlib
import sqlite3
import subprocess
import sys
from collections.abc import AsyncIterator, Callable, Iterator, Mapping
from typing import Any, ClassVar

import pyarrow as pa
import pytest
from sqlspec import SQL, StatementStack
from sqlspec.core.filters import NullFilter
from sqlspec.core.parameters import TypedParameter
from sqlspec.core.statement import StatementConfig
from sqlspec.exceptions import DialectNotSupportedError as SQLSpecDialectNotSupportedError
from sqlspec.exceptions import UniqueViolationError

import vertebra._oracle as _oracle_native
import vertebra.extensions.sqlspec as sqlspec_extension
from tests.unit.db.oracle_test_doubles import (
    _StubAsyncOracleClient,
    _StubAsyncOraclePool,
    _StubSyncOracleClient,
    _StubSyncOraclePool,
)
from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra._vertebra import IntegrityError as NativeIntegrityError
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
from vertebra.extensions.sqlspec._driver import BaseAsyncDriver, BaseSyncDriver
from vertebra.extensions.sqlspec.adapters import (
    MySQLAsyncConfig,
    MySQLSyncConfig,
    PostgresAsyncConfig,
    PostgresSyncConfig,
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
    PostgresAsyncDriver as SQLSpecPostgresAsyncDriver,
)
from vertebra.extensions.sqlspec.adapters import (
    PostgresSyncDriver as SQLSpecPostgresSyncDriver,
)
from vertebra.extensions.sqlspec.adapters.oracle import (
    OracleAsyncConfig,
    OracleAsyncConnectionContext,
    OracleAsyncSessionContext,
    OracleAsyncSessionlessTransactionContext,
    OracleConnectionParams,
    OracleDriverFeatures,
    OraclePoolParams,
    OracleSessionContextConfig,
    OracleSyncConfig,
    OracleSyncConnectionContext,
    OracleSyncSessionContext,
    OracleSyncSessionlessTransactionContext,
    build_duality_etag_update_sql,
    extract_duality_view_etag,
    inject_duality_view_etag,
)
from vertebra.extensions.sqlspec.adapters.oracle import (
    OracleAsyncDriver as SQLSpecOracleAsyncDriver,
)
from vertebra.extensions.sqlspec.adapters.oracle import (
    OracleSyncDriver as SQLSpecOracleSyncDriver,
)
from vertebra.extensions.sqlspec.adapters.oracle.core import build_oracle_statement_config
from vertebra.extensions.sqlspec.adapters.postgres import (
    PostgresAsyncConnectionContext,
    PostgresAsyncSessionContext,
    PostgresConnectionParams,
    PostgresDriverFeatures,
    PostgresPoolParams,
    PostgresSyncConnectionContext,
    PostgresSyncSessionContext,
)
from vertebra.typing import ImproperConfigurationError


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
        if isinstance(native, _SyncPipelinePort):
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
        if isinstance(native, _AsyncPipelinePort):
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
            if isinstance(native, _SyncPipelinePort):
                assert native.pipeline_calls == 0
        else:
            with pytest.raises(Exception, match="missing_table"):
                driver.execute_stack(stack)
            assert native.select("SELECT id FROM stack_items") == []
            if isinstance(native, _SyncPipelinePort):
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
            if isinstance(native, _AsyncPipelinePort):
                assert native.pipeline_calls == 0
        else:
            with pytest.raises(Exception, match="missing_table"):
                await driver.execute_stack(stack)
            assert await native.select("SELECT id FROM stack_items") == []
            if isinstance(native, _AsyncPipelinePort):
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
        assert result[1].rows_affected == 1
        assert result[2].get_result().get_data() == [{"id": 9}]
        assert result[2].result_type == "SELECT"
        assert result[2].get_result().statement.operation_type == "SELECT"
        if isinstance(native, _SyncPipelinePort):
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
        assert result[1].rows_affected == 1
        assert result[2].get_result().get_data() == [{"id": 9}]
        assert result[2].result_type == "SELECT"
        assert result[2].get_result().statement.operation_type == "SELECT"
        if isinstance(native, _AsyncPipelinePort):
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

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        port = self
        cursor = self.raw.cursor()

        class Cursor:
            def __init__(self) -> None:
                self._rows: list[Any] | None = None

            @property
            def description(self) -> Any:
                return cursor.description

            @property
            def rowcount(self) -> int:
                return cursor.rowcount

            @property
            def lastrowid(self) -> Any:
                return cursor.lastrowid

            def execute(self, sql: str, params: Any = None) -> Cursor:
                if lazy_begin and not port._in_tx:
                    port.begin()
                port.calls.append((sql, params))
                adapted_sql, bound = port._adapt_sqlite(sql, params)
                cursor.execute(adapted_sql, bound)
                self._rows = list(cursor.fetchall()) if cursor.description is not None else None
                if not lazy_begin and not port._in_tx:
                    port.raw.commit()
                return self

            def executemany(self, sql: str, param_sets: Any) -> Cursor:
                if lazy_begin and not port._in_tx:
                    port.begin()
                adapted_rows: list[Any] = []
                adapted_sql = sql
                for params in param_sets:
                    port.calls.append((sql, params))
                    adapted_sql, bound = port._adapt_sqlite(sql, params)
                    adapted_rows.append(bound)
                cursor.executemany(adapted_sql, adapted_rows)
                self._rows = None
                if not lazy_begin and not port._in_tx:
                    port.raw.commit()
                return self

            def fetchone(self) -> Any:
                if self._rows is not None:
                    return self._rows.pop(0) if self._rows else None
                return cursor.fetchone()

            def fetchmany(self, size: int = 1) -> list[Any]:
                if self._rows is not None:
                    out = self._rows[:size]
                    del self._rows[:size]
                    return out
                return cursor.fetchmany(size)

            def fetchall(self) -> list[Any]:
                if self._rows is not None:
                    out = list(self._rows)
                    self._rows.clear()
                    return out
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


class _AsyncNativeBindConnection:
    """Async DBAPI port retaining actual SQL and bind observations."""

    def __init__(self) -> None:
        self._sync = _NativeBindConnection()
        self.raw = self._sync.raw
        self.calls = self._sync.calls

    @property
    def in_transaction(self) -> bool:
        return self._sync.in_transaction

    def connection(self) -> Any:
        return self

    def acquire(self) -> Any:
        return self

    def cursor(self, *, lazy_begin: bool = False) -> Any:
        cursor = self._sync.cursor(lazy_begin=lazy_begin)

        class Cursor:
            @property
            def description(self) -> Any:
                return cursor.description

            @property
            def rowcount(self) -> int:
                return int(cursor.rowcount)

            @property
            def lastrowid(self) -> Any:
                return cursor.lastrowid

            async def execute(self, sql: str, params: Any = None) -> Cursor:
                cursor.execute(sql, params)
                return self

            async def executemany(self, sql: str, param_sets: Any) -> Cursor:
                cursor.executemany(sql, param_sets)
                return self

            async def fetchone(self) -> Any:
                return cursor.fetchone()

            async def fetchmany(self, size: int = 1) -> list[Any]:
                return list(cursor.fetchmany(size))

            async def fetchall(self) -> list[Any]:
                return list(cursor.fetchall())

            async def close(self) -> None:
                cursor.close()

            async def __aenter__(self) -> Any:
                return self

            async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
                cursor.close()

        return Cursor()

    async def execute(self, sql: str, params: Any = None) -> int:
        return self._sync.execute(sql, params)

    async def execute_many(self, sql: str, param_sets: Any) -> int:
        total = 0
        for params in param_sets:
            total += await self.execute(sql, params)
        return total

    async def execute_with_description(self, sql: str, params: Any = None) -> tuple[list[tuple[Any, ...]], list[str]]:
        return self._sync.execute_with_description(sql, params)

    async def fetch_all(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        return self._sync.fetch_all(sql, params)

    async def begin(self) -> None:
        self._sync.begin()

    async def savepoint(self, name: str) -> None:
        self._sync.savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        self._sync.release_savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        self._sync.rollback_to_savepoint(name)

    async def commit(self) -> None:
        self._sync.commit()

    async def rollback(self) -> None:
        self._sync.rollback()

    async def close(self) -> None:
        self._sync.close()

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
        config: Any = PostgresSyncConfig(native.config)
        driver: Any = SQLSpecPostgresSyncDriver(
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
            ("INSERT INTO stack_items VALUES (?)", [7]),
            ("UPDATE stack_items SET id = ? WHERE id = ?", [9, 7]),
            ("SELECT id FROM stack_items WHERE id = ?", [9]),
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
        config: Any = PostgresAsyncConfig(native.config)
        driver: Any = SQLSpecPostgresAsyncDriver(
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
        assert [sql for sql, _ in port.calls] == [
            "INSERT INTO stack_items VALUES (?)",
            "UPDATE stack_items SET id = ? WHERE id = ?",
            "SELECT id FROM stack_items WHERE id = ?",
        ]
        assert [list(params) for _, params in port.calls] == [[7], [9, 7], [9]]
        assert all("?" in sql for sql, _ in port.calls)
        assert port.raw.execute("SELECT id FROM stack_items").fetchall() == [(9,)]
    finally:
        await native.close()


def test_sqlspec_explicit_statement_config_remains_authoritative() -> None:
    """Caller-owned statement configuration controls compilation and retains object identity."""
    port = _NativeBindConnection()
    statement_config = SQL("SELECT 1").statement_config
    config = PostgresSyncConfig(PostgreSQLConfig(dsn="postgresql://localhost/db"), statement_config=statement_config)
    driver = SQLSpecPostgresSyncDriver(port, statement_config=config.statement_config)
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
    config = PostgresAsyncConfig(PostgreSQLConfig(dsn="postgresql://localhost/db"), statement_config=statement_config)
    driver = SQLSpecPostgresAsyncDriver(port, statement_config=config.statement_config)
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
        driver = SQLSpecPostgresSyncDriver(port)
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
    driver = SQLSpecPostgresSyncDriver(native)
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
        driver = SQLSpecPostgresAsyncDriver(port)
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
    driver = SQLSpecPostgresAsyncDriver(native)
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


class _PostgresTrackedStream:
    """Minimal row stream double tracking sync and async session-exit closure."""

    def __init__(self) -> None:
        self.closed_errors: list[bool] = []

    def _close_for_session_exit(self, *, error: bool = False) -> None:
        self.closed_errors.append(error)

    async def _aclose_for_session_exit(self, *, error: bool = False) -> None:
        self.closed_errors.append(error)

    def _mark_session_closed(self) -> None:
        return None


class _PostgresForwardingSpySyncContext:
    """Context double recording forwarded positional and keyword arguments."""

    __slots__ = ()

    recorded: ClassVar[list[tuple[tuple[Any, ...], dict[str, Any]]]] = []

    def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
        type(self).recorded.append((args, {"config": config, **kwargs}))

    def __enter__(self) -> str:
        return "forwarded-sync"

    def __exit__(self, *_exc: Any) -> None:
        return None


class _PostgresForwardingSpyAsyncContext:
    """Async context double recording forwarded positional and keyword arguments."""

    __slots__ = ()

    recorded: ClassVar[list[tuple[tuple[Any, ...], dict[str, Any]]]] = []

    def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
        type(self).recorded.append((args, {"config": config, **kwargs}))

    async def __aenter__(self) -> str:
        return "forwarded-async"

    async def __aexit__(self, *_exc: Any) -> None:
        return None


@pytest.mark.parametrize(
    ("slotted_cls",),
    [
        (PostgresSyncConfig,),
        (PostgresAsyncConfig,),
        (SQLSpecPostgresSyncDriver,),
        (SQLSpecPostgresAsyncDriver,),
        (PostgresSyncConnectionContext,),
        (PostgresAsyncConnectionContext,),
        (PostgresSyncSessionContext,),
        (PostgresAsyncSessionContext,),
    ],
)
def test_postgres_adapter_types_declare_slots_and_typed_surface(slotted_cls: type[Any]) -> None:
    """PostgreSQL SQLSpec configs, drivers, and context managers declare __slots__ and typed exports."""
    assert "__slots__" in slotted_cls.__dict__
    assert "dsn" in PostgresConnectionParams.__annotations__
    assert "direct_dsn" in PostgresConnectionParams.__annotations__
    assert "max_size" in PostgresPoolParams.__annotations__
    assert "reset_session_on_release" in PostgresDriverFeatures.__annotations__


def test_postgres_config_lazy_adk_store_attr_defers_google_adk_import() -> None:
    """Importing PostgresSyncConfig and PostgresAsyncConfig never loads google.adk or postgres.adk eagerly."""
    script = (
        "import sys; "
        "from vertebra.extensions.sqlspec.adapters.postgres import PostgresSyncConfig, PostgresAsyncConfig; "
        "assert 'google.adk' not in sys.modules; "
        "assert 'vertebra.extensions.sqlspec.adapters.postgres.adk' not in sys.modules; "
        "cls = PostgresSyncConfig.adk_store_class; "
        "assert cls.__name__ == 'PostgresSyncADKStore'; "
        "assert 'vertebra.extensions.sqlspec.adapters.postgres.adk' in sys.modules"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("raise_on_exit", [False, True], ids=["clean-exit", "error-exit"])
async def test_postgres_configs_distinct_sessions_stream_close_and_reset_on_release(
    is_async_mode: bool,
    raise_on_exit: bool,
) -> None:
    """PostgresSyncConfig and PostgresAsyncConfig create distinct drivers, close streams, rollback, reset, and mark closed."""
    cluster = FakePostgresCluster()
    cluster._conn.execute("CREATE TABLE pg_items (id INTEGER)")
    features: PostgresDriverFeatures = {"reset_session_on_release": True, "pooler_mode": "session"}
    conn_params: PostgresConnectionParams = {
        "host": "db.internal",
        "port": 5432,
        "user": "alice",
        "password": "test-token-placeholder",
        "database": "app",
        "statement_cache_size": 64,
        "server_settings": {"TimeZone": "UTC"},
    }
    pool_params: PostgresPoolParams = {"min_size": 1, "max_size": 4}

    if not is_async_mode:
        native_sync = SyncPostgreSQLDriver(
            PostgreSQLConfig(dsn="postgresql://localhost:5432/app"),
            pool=FakeSyncPostgresPool(cluster),
        )
        cfg = PostgresSyncConfig(
            conn_params,
            pool_config=pool_params,
            connection_instance=native_sync,
            share_pool=False,
            driver_features=features,
        )
        assert not hasattr(cfg, "__dict__")
        assert cfg.driver_config.max_prepared_statements == 64
        assert cfg.driver_config.max_size == 4
        assert cfg.driver_config.session_gucs.get("TimeZone") == "UTC"

        stream = _PostgresTrackedStream()
        try:
            with cfg.provide_session() as d1, cfg.provide_session() as d2:
                assert d1 is not d2
                assert isinstance(d1, SQLSpecPostgresSyncDriver)
                assert not hasattr(d1, "__dict__")
                d1._open_streams.append(stream)
                d1.begin()
                assert d1.connection.in_transaction is True
                if raise_on_exit:
                    raise RuntimeError("session failure")
        except RuntimeError:
            assert raise_on_exit is True
        assert stream.closed_errors == [raise_on_exit]
        assert d1.connection is not native_sync
        assert d2.connection is not native_sync
        assert native_sync.in_transaction is False
        assert sum(1 for sql, _, _ in cluster.recorded_queries if sql == "RESET ALL") == 2

        with cfg.provide_connection() as conn:
            assert conn is native_sync
            conn.begin()
            assert conn.in_transaction is True
        assert native_sync.in_transaction is False
        assert sum(1 for sql, _, _ in cluster.recorded_queries if sql == "RESET ALL") == 3
        cfg.close()
        return

    native_async = AsyncPostgreSQLDriver(
        PostgreSQLConfig(dsn="postgresql://localhost:5432/app"),
        pool=FakeAsyncPostgresPool(cluster),
    )
    async_cfg = PostgresAsyncConfig(
        conn_params,
        pool_config=pool_params,
        connection_instance=native_async,
        share_pool=False,
        driver_features=features,
    )
    assert not hasattr(async_cfg, "__dict__")
    assert async_cfg.driver_config.max_prepared_statements == 64
    assert async_cfg.driver_config.max_size == 4

    async_stream = _PostgresTrackedStream()
    try:
        async with async_cfg.provide_session() as a1, async_cfg.provide_session() as a2:
            assert a1 is not a2
            assert isinstance(a1, SQLSpecPostgresAsyncDriver)
            assert not hasattr(a1, "__dict__")
            a1._open_streams.append(async_stream)
            await a1.begin()
            assert a1.connection.in_transaction is True
            if raise_on_exit:
                raise RuntimeError("async session failure")
    except RuntimeError:
        assert raise_on_exit is True
    assert async_stream.closed_errors == [raise_on_exit]
    assert a1.connection is not native_async
    assert a2.connection is not native_async
    assert native_async.in_transaction is False
    assert sum(1 for sql, _, _ in cluster.recorded_queries if sql == "RESET ALL") == 2

    async with async_cfg.provide_connection() as async_conn:
        assert async_conn is native_async
        await async_conn.begin()
        assert async_conn.in_transaction is True
    assert native_async.in_transaction is False
    assert sum(1 for sql, _, _ in cluster.recorded_queries if sql == "RESET ALL") == 3
    await async_cfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_postgres_provide_session_and_connection_forward_args_and_route_replicas(
    is_async_mode: bool,
) -> None:
    """PostgresSyncConfig and PostgresAsyncConfig forward session/connection kwargs, route replicas, and preserve direct state."""
    primary_cluster = FakePostgresCluster()
    replica_cluster = FakePostgresCluster()
    primary_cluster._conn.execute("CREATE TABLE pg_routes (id INTEGER)")
    replica_cluster._conn.execute("CREATE TABLE pg_routes (id INTEGER)")
    replica_cluster._conn.execute("INSERT INTO pg_routes VALUES (99)")
    replica_cluster._conn.commit()

    features: PostgresDriverFeatures = {"reset_session_on_release": True, "pooler_mode": "transaction"}

    if not is_async_mode:
        _PostgresForwardingSpySyncContext.recorded = []

        class _SpyPostgresSyncConfig(PostgresSyncConfig):
            __slots__ = ()
            _session_context_class: ClassVar[type[Any]] = _PostgresForwardingSpySyncContext
            _connection_context_class: ClassVar[type[Any]] = _PostgresForwardingSpySyncContext

        spy_sync = _SpyPostgresSyncConfig(
            dsn="postgresql://primary.internal:6432/app",
            share_pool=False,
        )
        with spy_sync.provide_session("read-only", timeout=12) as sess_val:
            assert str(sess_val) == "forwarded-sync"
        with spy_sync.provide_connection("direct", isolation="REPEATABLE READ") as conn_val:
            assert conn_val == "forwarded-sync"
        assert _PostgresForwardingSpySyncContext.recorded[0][0] == ("read-only",)
        assert _PostgresForwardingSpySyncContext.recorded[0][1]["timeout"] == 12
        assert _PostgresForwardingSpySyncContext.recorded[1][0] == ("direct",)
        assert _PostgresForwardingSpySyncContext.recorded[1][1]["isolation"] == "REPEATABLE READ"

        primary_native = SyncPostgreSQLDriver(
            PostgreSQLConfig(
                dsn="postgresql://pgbouncer.internal:6432/app",
                direct_dsn="postgresql://primary-direct.internal:5432/app",
                pooler_mode="transaction",
            ),
            pool=FakeSyncPostgresPool(primary_cluster),
        )
        replica_native = SyncPostgreSQLDriver(
            PostgreSQLConfig(
                dsn="postgresql://replica-pooler.internal:6432/app",
                direct_dsn="postgresql://replica-direct.internal:5432/app",
                pooler_mode="transaction",
            ),
            pool=FakeSyncPostgresPool(replica_cluster),
        )
        replica_cfg = PostgresSyncConfig(
            replica_native.config,
            connection_instance=replica_native,
            share_pool=False,
            is_replica=True,
            driver_features=features,
        )
        primary_cfg = PostgresSyncConfig(
            primary_native.config,
            connection_instance=primary_native,
            share_pool=False,
            replicas=[replica_cfg],
            driver_features=features,
        )
        with primary_cfg.provide_replica_session() as rep_driver:
            rows = rep_driver.select("SELECT id FROM pg_routes")
            assert rows == [{"id": 99}]
        assert len(primary_cluster.recorded_queries) == 0

        routing = primary_cfg.provide_routing_session()
        assert routing.select("SELECT id FROM pg_routes") == [{"id": 99}]
        routing.execute("INSERT INTO pg_routes VALUES ($1)", 7)
        assert any("INSERT INTO pg_routes" in sql for sql, _, _ in primary_cluster.recorded_queries)

        direct_cfg = primary_cfg.derive_direct_config()
        assert direct_cfg.driver_config.dsn == "postgresql://primary-direct.internal:5432/app"
        assert direct_cfg.driver_config.pooler_mode == "session"
        assert direct_cfg.driver_features.get("reset_session_on_release") is True
        assert direct_cfg.has_replicas is True
        direct_replica = direct_cfg.replica_groups["default"].specs[0].config
        assert direct_replica.is_replica is True
        assert direct_replica.driver_config.dsn == "postgresql://replica-direct.internal:5432/app"
        assert direct_replica.driver_features.get("reset_session_on_release") is True
        primary_cfg.close()
        return

    _PostgresForwardingSpyAsyncContext.recorded = []

    class _SpyPostgresAsyncConfig(PostgresAsyncConfig):
        __slots__ = ()
        _session_context_class: ClassVar[type[Any]] = _PostgresForwardingSpyAsyncContext
        _connection_context_class: ClassVar[type[Any]] = _PostgresForwardingSpyAsyncContext

    spy_async = _SpyPostgresAsyncConfig(
        dsn="postgresql://primary.internal:6432/app",
        share_pool=False,
    )
    async with spy_async.provide_session("read-only", timeout=12) as async_sess_val:
        assert str(async_sess_val) == "forwarded-async"
    async with spy_async.provide_connection("direct", isolation="SERIALIZABLE") as async_conn_val:
        assert async_conn_val == "forwarded-async"
    assert _PostgresForwardingSpyAsyncContext.recorded[0][0] == ("read-only",)
    assert _PostgresForwardingSpyAsyncContext.recorded[0][1]["timeout"] == 12
    assert _PostgresForwardingSpyAsyncContext.recorded[1][0] == ("direct",)
    assert _PostgresForwardingSpyAsyncContext.recorded[1][1]["isolation"] == "SERIALIZABLE"

    primary_async_native = AsyncPostgreSQLDriver(
        PostgreSQLConfig(
            dsn="postgresql://pgbouncer.internal:6432/app",
            direct_dsn="postgresql://primary-direct.internal:5432/app",
            pooler_mode="transaction",
        ),
        pool=FakeAsyncPostgresPool(primary_cluster),
    )
    replica_async_native = AsyncPostgreSQLDriver(
        PostgreSQLConfig(
            dsn="postgresql://replica-pooler.internal:6432/app",
            direct_dsn="postgresql://replica-direct.internal:5432/app",
            pooler_mode="transaction",
        ),
        pool=FakeAsyncPostgresPool(replica_cluster),
    )
    replica_async_cfg = PostgresAsyncConfig(
        replica_async_native.config,
        connection_instance=replica_async_native,
        share_pool=False,
        is_replica=True,
        driver_features=features,
    )
    primary_async_cfg = PostgresAsyncConfig(
        primary_async_native.config,
        connection_instance=primary_async_native,
        share_pool=False,
        replicas=[replica_async_cfg],
        driver_features=features,
    )
    async with primary_async_cfg.provide_replica_session() as async_rep_driver:
        async_rows = await async_rep_driver.select("SELECT id FROM pg_routes")
        assert async_rows == [{"id": 99}]
    assert len(primary_cluster.recorded_queries) == 0

    async_routing = primary_async_cfg.provide_routing_session()
    assert await async_routing.select("SELECT id FROM pg_routes") == [{"id": 99}]
    await async_routing.execute("INSERT INTO pg_routes VALUES ($1)", 7)
    assert any("INSERT INTO pg_routes" in sql for sql, _, _ in primary_cluster.recorded_queries)

    direct_async_cfg = primary_async_cfg.derive_direct_config()
    assert direct_async_cfg.driver_config.dsn == "postgresql://primary-direct.internal:5432/app"
    assert direct_async_cfg.driver_config.pooler_mode == "session"
    assert direct_async_cfg.driver_features.get("reset_session_on_release") is True
    assert direct_async_cfg.has_replicas is True
    direct_async_replica = direct_async_cfg.replica_groups["default"].specs[0].config
    assert direct_async_replica.is_replica is True
    assert direct_async_replica.driver_config.dsn == "postgresql://replica-direct.internal:5432/app"
    assert direct_async_replica.driver_features.get("reset_session_on_release") is True
    await primary_async_cfg.close()


class _FakeOracleCursor:
    """Minimal sync Oracle cursor double backed by `_FakeOracleClient`."""

    def __init__(self, client: _FakeOracleClient) -> None:
        self._client = client
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: Any = None
        self.out_binds: dict[str, Any] = {}
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: Any = None, *, out_binds: Any = None) -> int:
        _ = out_binds
        upper = sql.lstrip().upper()
        if upper.startswith(("SELECT", "WITH")):
            self._client.executed.append((sql, params, {"select": True}))
            cols = list(self._client.rows[0].keys()) if self._client.rows else []
            self.description = tuple((col, None, None, None, None, None, None) for col in cols)
            self._rows = [tuple(row.get(col) for col in cols) for row in self._client.rows]
            self.rowcount = -1
            return 0
        self._client.executed.append((sql, params, {}))
        self.description = None
        self._rows = []
        self.rowcount = 1
        return 1

    def executemany(self, sql: str, parameter_sets: Any) -> int:
        batch = list(parameter_sets)
        self._client.executed.append((sql, batch, {"many": True}))
        self.description = None
        self._rows = []
        self.rowcount = len(batch)
        return len(batch)

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self._rows
        self._rows = []
        return rows

    def close(self) -> None:
        return None


class _FakeOracleSessionlessCursor(_FakeOracleCursor):
    """Fake Oracle cursor accepting `suspend_on_success` for 26ai sessionless tests."""

    def execute(
        self,
        sql: str,
        params: Any = None,
        *,
        suspend_on_success: bool = False,
        out_binds: Any = None,
    ) -> int:
        if suspend_on_success:
            _ = out_binds
            self._client.executed.append((sql, params, {"suspend_on_success": True}))
            self.description = None
            self._rows = []
            self.rowcount = 1
            return 1
        return super().execute(sql, params, out_binds=out_binds)


class _FakeAsyncOracleCursor:
    """Minimal async Oracle cursor double backed by `_FakeAsyncOracleClient`."""

    def __init__(self, client: _FakeAsyncOracleClient) -> None:
        self._client = client
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: Any = None
        self.out_binds: dict[str, Any] = {}
        self._rows: list[tuple[Any, ...]] = []

    async def execute(self, sql: str, params: Any = None, *, out_binds: Any = None) -> int:
        _ = out_binds
        upper = sql.lstrip().upper()
        if upper.startswith(("SELECT", "WITH")):
            self._client.executed.append((sql, params, {"select": True}))
            cols = list(self._client.rows[0].keys()) if self._client.rows else []
            self.description = tuple((col, None, None, None, None, None, None) for col in cols)
            self._rows = [tuple(row.get(col) for col in cols) for row in self._client.rows]
            self.rowcount = -1
            return 0
        self._client.executed.append((sql, params, {}))
        self.description = None
        self._rows = []
        self.rowcount = 1
        return 1

    async def executemany(self, sql: str, parameter_sets: Any) -> int:
        batch = list(parameter_sets)
        self._client.executed.append((sql, batch, {"many": True}))
        self.description = None
        self._rows = []
        self.rowcount = len(batch)
        return len(batch)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self._rows
        self._rows = []
        return rows

    async def close(self) -> None:
        return None


class _FakeAsyncOracleSessionlessCursor(_FakeAsyncOracleCursor):
    """Async fake Oracle cursor accepting `suspend_on_success` for 26ai sessionless tests."""

    async def execute(
        self,
        sql: str,
        params: Any = None,
        *,
        suspend_on_success: bool = False,
        out_binds: Any = None,
    ) -> int:
        if suspend_on_success:
            _ = out_binds
            self._client.executed.append((sql, params, {"suspend_on_success": True}))
            self.description = None
            self._rows = []
            self.rowcount = 1
            return 1
        return await super().execute(sql, params, out_binds=out_binds)


class _FakeOracleClient:
    """In-memory fake Oracle connection double for SQLSpec adapter session and 26ai lifecycle tests."""

    def __init__(self, *, rows: list[dict[str, Any]] | None = None, supports_sessionless: bool = True) -> None:
        self.executed: list[tuple[str, Any, dict[str, Any]]] = []
        self.rows: list[dict[str, Any]] = list(rows) if rows is not None else []
        self.in_transaction: bool = False
        self.rollbacks: int = 0
        self.commits: int = 0
        self.closed: bool = False
        self.transaction_priority: Any = None
        self.client_identifier: str | None = None
        self.sessionless_calls: list[tuple[str, Any]] = []
        self._supports_sessionless = supports_sessionless

    def begin(self) -> None:
        self.in_transaction = True

    def commit(self) -> None:
        self.in_transaction = False
        self.commits += 1

    def rollback(self) -> None:
        self.in_transaction = False
        self.rollbacks += 1

    def execute(self, sql: str, params: Any = None) -> int:
        self.executed.append((sql, params, {}))
        return 1

    def select(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        self.executed.append((sql, params, {"select": True}))
        return list(self.rows)

    def query_with_columns(self, sql: str, params: Any = None) -> tuple[list[dict[str, Any]], list[str]]:
        self.executed.append((sql, params, {"select": True}))
        cols = list(self.rows[0].keys()) if self.rows else []
        return list(self.rows), cols

    @contextlib.contextmanager
    def checkout_cursor(self) -> Any:
        cursor = _FakeOracleCursor(self)
        try:
            yield cursor
        finally:
            cursor.close()

    def close(self) -> None:
        self.closed = True


class _FakeOracleSessionlessClient(_FakeOracleClient):
    """Fake Oracle connection exposing native 26ai sessionless transaction methods."""

    @contextlib.contextmanager
    def checkout_cursor(self) -> Any:
        cursor = _FakeOracleSessionlessCursor(self)
        try:
            yield cursor
        finally:
            cursor.close()

    def execute(self, sql: str, params: Any = None, *, suspend_on_success: bool = False) -> int:
        self.executed.append((sql, params, {"suspend_on_success": suspend_on_success}))
        return 1

    def begin_sessionless_transaction(
        self,
        transaction_id: bytes | None = None,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        txid = transaction_id if transaction_id is not None else b"generated-txid"
        self.in_transaction = True
        self.sessionless_calls.append(("begin", (txid, timeout, defer_round_trip)))
        return txid

    def resume_sessionless_transaction(
        self,
        transaction_id: bytes,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        self.in_transaction = True
        self.sessionless_calls.append(("resume", (transaction_id, timeout, defer_round_trip)))
        return transaction_id

    def suspend_sessionless_transaction(self) -> None:
        self.in_transaction = False
        self.sessionless_calls.append(("suspend", None))


class _FakeAsyncOracleClient:
    """In-memory async fake Oracle connection double for SQLSpec async lifecycle tests."""

    def __init__(self, *, rows: list[dict[str, Any]] | None = None) -> None:
        self.executed: list[tuple[str, Any, dict[str, Any]]] = []
        self.rows: list[dict[str, Any]] = list(rows) if rows is not None else []
        self.in_transaction: bool = False
        self.rollbacks: int = 0
        self.commits: int = 0
        self.closed: bool = False
        self.transaction_priority: Any = None
        self.client_identifier: str | None = None
        self.sessionless_calls: list[tuple[str, Any]] = []

    async def begin(self) -> None:
        self.in_transaction = True

    async def commit(self) -> None:
        self.in_transaction = False
        self.commits += 1

    async def rollback(self) -> None:
        self.in_transaction = False
        self.rollbacks += 1

    async def execute(self, sql: str, params: Any = None) -> int:
        self.executed.append((sql, params, {}))
        return 1

    async def select(self, sql: str, params: Any = None) -> list[dict[str, Any]]:
        self.executed.append((sql, params, {"select": True}))
        return list(self.rows)

    async def query_with_columns(self, sql: str, params: Any = None) -> tuple[list[dict[str, Any]], list[str]]:
        self.executed.append((sql, params, {"select": True}))
        cols = list(self.rows[0].keys()) if self.rows else []
        return list(self.rows), cols

    @contextlib.asynccontextmanager
    async def checkout_cursor(self) -> Any:
        cursor = _FakeAsyncOracleCursor(self)
        try:
            yield cursor
        finally:
            await cursor.close()

    async def close(self) -> None:
        self.closed = True


class _FakeAsyncOracleSessionlessClient(_FakeAsyncOracleClient):
    """Async fake Oracle connection exposing native 26ai sessionless transaction methods."""

    @contextlib.asynccontextmanager
    async def checkout_cursor(self) -> Any:
        cursor = _FakeAsyncOracleSessionlessCursor(self)
        try:
            yield cursor
        finally:
            await cursor.close()

    async def execute(self, sql: str, params: Any = None, *, suspend_on_success: bool = False) -> int:
        self.executed.append((sql, params, {"suspend_on_success": suspend_on_success}))
        return 1

    async def begin_sessionless_transaction(
        self,
        transaction_id: bytes | None = None,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        txid = transaction_id if transaction_id is not None else b"async-generated-txid"
        self.in_transaction = True
        self.sessionless_calls.append(("begin", (txid, timeout, defer_round_trip)))
        return txid

    async def resume_sessionless_transaction(
        self,
        transaction_id: bytes,
        *,
        timeout: int = 60,
        defer_round_trip: bool = True,
    ) -> bytes:
        self.in_transaction = True
        self.sessionless_calls.append(("resume", (transaction_id, timeout, defer_round_trip)))
        return transaction_id

    async def suspend_sessionless_transaction(self) -> None:
        self.in_transaction = False
        self.sessionless_calls.append(("suspend", None))


@pytest.mark.parametrize(
    ("slotted_cls",),
    [
        (OracleSyncConfig,),
        (OracleAsyncConfig,),
        (SQLSpecOracleSyncDriver,),
        (SQLSpecOracleAsyncDriver,),
        (OracleSyncConnectionContext,),
        (OracleAsyncConnectionContext,),
        (OracleSyncSessionContext,),
        (OracleAsyncSessionContext,),
        (OracleSyncSessionlessTransactionContext,),
        (OracleAsyncSessionlessTransactionContext,),
    ],
)
def test_oracle_adapter_types_declare_slots_and_typed_surface(slotted_cls: type[Any]) -> None:
    """Oracle SQLSpec configs, drivers, and context managers declare __slots__ and typed exports."""
    assert "__slots__" in slotted_cls.__dict__
    assert "dsn" in OracleConnectionParams.__annotations__
    assert "transaction_priority" in OracleConnectionParams.__annotations__
    assert "min_size" in OraclePoolParams.__annotations__
    assert "session_context_hook" in OracleDriverFeatures.__annotations__
    assert "reset_session_on_release" in OracleDriverFeatures.__annotations__


def test_oracle_config_lazy_adk_store_attr_defers_google_adk_import() -> None:
    """Importing OracleSyncConfig and OracleAsyncConfig never loads google.adk or oracle.adk eagerly."""
    script = (
        "import sys; "
        "from vertebra.extensions.sqlspec.adapters.oracle import OracleSyncConfig, OracleAsyncConfig; "
        "assert 'google.adk' not in sys.modules; "
        "assert 'vertebra.extensions.sqlspec.adapters.oracle.adk' not in sys.modules; "
        "cls = OracleSyncConfig.adk_store_class; "
        "assert cls.__name__ == 'OracleSyncADKStore'; "
        "assert 'vertebra.extensions.sqlspec.adapters.oracle.adk' in sys.modules"
    )
    subprocess.run([sys.executable, "-c", script], check=True)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
@pytest.mark.parametrize("raise_on_exit", [False, True], ids=["clean-exit", "error-exit"])
async def test_oracle_session_isolation_cleanup_hook_and_reset_on_release(
    is_async_mode: bool,
    raise_on_exit: bool,
) -> None:
    """OracleSyncConfig and OracleAsyncConfig isolate sessions, close streams, apply hooks, rollback, reset, and mark closed."""
    hook = OracleSessionContextConfig(
        client_identifier="tenant-user-42",
        contexts={"APP_CTX": {"TENANT_ID": "acme"}},
        reset_package=True,
        reset_on_release=True,
    )
    features: OracleDriverFeatures = {
        "reset_session_on_release": True,
        "session_context_hook": hook,
    }
    conn_params: OracleConnectionParams = {
        "host": "ora.internal",
        "port": 1521,
        "service_name": "FREEPDB1",
        "user": "app_user",
        "password": "test-token-placeholder",
        "stmtcachesize": 128,
    }
    pool_params: OraclePoolParams = {"min_size": 1, "max_size": 5}

    if not is_async_mode:
        conn = _FakeOracleClient()
        cfg = OracleSyncConfig(
            conn_params,
            pool_config=pool_params,
            connection_instance=conn,
            share_pool=False,
            driver_features=features,
        )
        assert not hasattr(cfg, "__dict__")
        assert cfg.driver_config.statement_cache_size == 128
        assert cfg.driver_config.pool_max_size == 5

        stream = _PostgresTrackedStream()
        try:
            with cfg.provide_session() as d1, cfg.provide_session() as d2:
                assert d1 is not d2
                assert isinstance(d1, SQLSpecOracleSyncDriver)
                assert not hasattr(d1, "__dict__")
                d1._open_streams.append(stream)
                d1.begin()
                assert conn.in_transaction is True
                if raise_on_exit:
                    raise RuntimeError("oracle session error")
        except RuntimeError:
            assert raise_on_exit is True

        assert stream.closed_errors == [raise_on_exit]
        assert d1.connection is not conn
        assert d2.connection is not conn
        assert conn.in_transaction is False
        assert conn.rollbacks >= 1
        sql_log = [sql for sql, _, _ in conn.executed]
        assert any("DBMS_SESSION.SET_IDENTIFIER" in sql for sql in sql_log)
        assert any("DBMS_SESSION.SET_CONTEXT" in sql for sql in sql_log)
        assert any("DBMS_SESSION.CLEAR_IDENTIFIER" in sql for sql in sql_log)
        assert any("DBMS_SESSION.CLEAR_CONTEXT" in sql for sql in sql_log)
        assert any("DBMS_SESSION.RESET_PACKAGE" in sql for sql in sql_log)

        with cfg.provide_connection() as leased:
            assert leased is conn
            leased.begin()
        assert conn.in_transaction is False
        assert conn.client_identifier is None
        cfg.close()
        return

    async_conn = _FakeAsyncOracleClient()
    async_cfg = OracleAsyncConfig(
        conn_params,
        pool_config=pool_params,
        connection_instance=async_conn,
        share_pool=False,
        driver_features=features,
    )
    assert not hasattr(async_cfg, "__dict__")
    assert async_cfg.driver_config.statement_cache_size == 128
    assert async_cfg.driver_config.pool_max_size == 5

    async_stream = _PostgresTrackedStream()
    try:
        async with async_cfg.provide_session() as a1, async_cfg.provide_session() as a2:
            assert a1 is not a2
            assert isinstance(a1, SQLSpecOracleAsyncDriver)
            assert not hasattr(a1, "__dict__")
            a1._open_streams.append(async_stream)
            await a1.begin()
            assert async_conn.in_transaction is True
            if raise_on_exit:
                raise RuntimeError("async oracle session error")
    except RuntimeError:
        assert raise_on_exit is True

    assert async_stream.closed_errors == [raise_on_exit]
    assert a1.connection is not async_conn
    assert a2.connection is not async_conn
    assert async_conn.in_transaction is False
    assert async_conn.rollbacks >= 1
    async_sql_log = [sql for sql, _, _ in async_conn.executed]
    assert any("DBMS_SESSION.SET_IDENTIFIER" in sql for sql in async_sql_log)
    assert any("DBMS_SESSION.SET_CONTEXT" in sql for sql in async_sql_log)
    assert any("DBMS_SESSION.CLEAR_IDENTIFIER" in sql for sql in async_sql_log)
    assert any("DBMS_SESSION.CLEAR_CONTEXT" in sql for sql in async_sql_log)
    assert any("DBMS_SESSION.RESET_PACKAGE" in sql for sql in async_sql_log)

    async with async_cfg.provide_connection() as async_leased:
        assert async_leased is async_conn
        await async_leased.begin()
    assert async_conn.in_transaction is False
    assert async_conn.client_identifier is None
    await async_cfg.close()


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_oracle_replicas_routing_session_and_fallback(is_async_mode: bool) -> None:
    """OracleSyncConfig and OracleAsyncConfig route replica reads and primary writes cleanly."""
    if not is_async_mode:
        primary_conn = _FakeOracleClient()
        replica_conn = _FakeOracleClient(rows=[{"id": 42}])
        replica_cfg = OracleSyncConfig(
            "oracle://scott:test-token-placeholder@replica.internal:1521/FREEPDB1",
            connection_instance=replica_conn,
            share_pool=False,
            is_replica=True,
        )
        primary_cfg = OracleSyncConfig(
            "oracle://scott:test-token-placeholder@primary.internal:1521/FREEPDB1",
            connection_instance=primary_conn,
            share_pool=False,
            replicas=[replica_cfg],
        )
        assert primary_cfg.has_replicas is True
        with primary_cfg.provide_replica_session() as rep_driver:
            assert rep_driver.select("SELECT id FROM orders_view") == [{"id": 42}]
        assert len(primary_conn.executed) == 0

        routing = primary_cfg.provide_routing_session()
        assert routing.select("SELECT id FROM orders_view") == [{"id": 42}]
        routing.execute("INSERT INTO orders_view (id) VALUES (:1)", 7)
        assert any("INSERT INTO orders_view" in sql for sql, _, _ in primary_conn.executed)
        primary_cfg.close()
        return

    primary_async = _FakeAsyncOracleClient()
    replica_async = _FakeAsyncOracleClient(rows=[{"id": 42}])
    replica_async_cfg = OracleAsyncConfig(
        "oracle://scott:test-token-placeholder@replica.internal:1521/FREEPDB1",
        connection_instance=replica_async,
        share_pool=False,
        is_replica=True,
    )
    primary_async_cfg = OracleAsyncConfig(
        "oracle://scott:test-token-placeholder@primary.internal:1521/FREEPDB1",
        connection_instance=primary_async,
        share_pool=False,
        replicas=[replica_async_cfg],
    )
    assert primary_async_cfg.has_replicas is True
    async with primary_async_cfg.provide_replica_session() as async_rep_driver:
        assert await async_rep_driver.select("SELECT id FROM orders_view") == [{"id": 42}]
    assert len(primary_async.executed) == 0

    async_routing = primary_async_cfg.provide_routing_session()
    assert await async_routing.select("SELECT id FROM orders_view") == [{"id": 42}]
    await async_routing.execute("INSERT INTO orders_view (id) VALUES (:1)", 7)
    assert any("INSERT INTO orders_view" in sql for sql, _, _ in primary_async.executed)
    await primary_async_cfg.close()


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"_metadata": {"etag": "ABC123"}}, "ABC123"),
        ({"DATA": {"_metadata": {"etag": "DEF456"}, "name": "order"}}, "DEF456"),
        ({"data": '{"_metadata": {"etag": "GHI789"}}'}, "GHI789"),
        ('{"_metadata": {"etag": "JKL012"}}', "JKL012"),
        (b'{"data": {"_metadata": {"etag": "MNO345"}}}', "MNO345"),
        ({"id": 1}, None),
        ("not-json", None),
    ],
)
def test_oracle_duality_view_etag_extract_inject_and_update_sql(
    payload: Mapping[str, Any] | str | bytes,
    expected: str | None,
) -> None:
    """Oracle 26ai Duality View helpers extract, inject, and validate optimistic-concurrency UPDATE SQL."""
    assert extract_duality_view_etag(payload) == expected

    doc: dict[str, Any] = {"orderId": 10, "_metadata": {"asof": "123"}}
    injected = inject_duality_view_etag(doc, "ETAG99")
    assert injected["_metadata"] == {"asof": "123", "etag": "ETAG99"}
    assert "_metadata" in doc and "etag" not in doc["_metadata"]

    sql = build_duality_etag_update_sql("APP.ORDERS_DV", id_column="ORDER_ID", data_column="DOC", etag_param="etag")
    assert sql == (
        "UPDATE APP.ORDERS_DV SET DOC = :data WHERE ORDER_ID = :id AND JSON_VALUE(DOC, '$._metadata.etag') = :etag"
    )
    with pytest.raises(ImproperConfigurationError):
        build_duality_etag_update_sql("ORDERS_DV; DROP TABLE users")
    with pytest.raises(ImproperConfigurationError):
        build_duality_etag_update_sql("ORDERS_DV", id_column="ID = 1 OR 1=1")


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_oracle_26ai_sessionless_transaction_priority_and_unsupported_error(is_async_mode: bool) -> None:
    """Sessionless transactions, suspend_on_success, transaction priority, and DialectNotSupportedError behave cleanly."""
    if not is_async_mode:
        conn = _FakeOracleSessionlessClient()
        driver = SQLSpecOracleSyncDriver(conn)
        driver.set_transaction_priority("HIGH")
        assert conn.transaction_priority == "HIGH"
        with pytest.raises(ImproperConfigurationError):
            driver.set_transaction_priority("CRITICAL")

        with driver.sessionless_transaction("tx-order-1", timeout=30) as txid:
            assert txid == b"tx-order-1"
            assert driver.sessionless_transaction_id == b"tx-order-1"
            driver.execute("INSERT INTO orders VALUES (:id)", id=1)
        assert driver.sessionless_transaction_id is None
        assert [op for op, _ in conn.sessionless_calls] == ["begin", "suspend"]

        with (
            pytest.raises(RuntimeError, match="boom"),
            driver.sessionless_transaction("tx-order-1", resume=True, rollback_on_error=True),
        ):
            raise RuntimeError("boom")
        assert conn.rollbacks == 1
        assert driver.sessionless_transaction_id is None

        driver.begin_sessionless_transaction("tx-order-2")
        driver.execute("UPDATE orders SET status = :s", s="done", suspend_on_success=True)
        assert conn.executed[-1][2]["suspend_on_success"] is True
        assert driver.sessionless_transaction_id is None

        with pytest.raises(ImproperConfigurationError):
            driver.begin_sessionless_transaction("x" * 65)
        with pytest.raises(ImproperConfigurationError):
            driver.begin_sessionless_transaction("tx-ok", timeout=0)

        unsupported_driver = SQLSpecOracleSyncDriver(_FakeOracleClient())
        with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
            unsupported_driver.begin_sessionless_transaction("tx-unsupported")
        with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
            unsupported_driver.resume_sessionless_transaction("tx-unsupported")
        with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
            unsupported_driver.suspend_sessionless_transaction()
        with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
            unsupported_driver.execute("UPDATE orders SET status = :s", s="done", suspend_on_success=True)
        return

    async_conn = _FakeAsyncOracleSessionlessClient()
    async_driver = SQLSpecOracleAsyncDriver(async_conn)
    async_driver.set_transaction_priority("MEDIUM")
    assert async_conn.transaction_priority == "MEDIUM"

    async with async_driver.sessionless_transaction("async-tx-1", timeout=30) as txid:
        assert txid == b"async-tx-1"
        assert async_driver.sessionless_transaction_id == b"async-tx-1"
        await async_driver.execute("INSERT INTO orders VALUES (:id)", id=2)
    assert async_driver.sessionless_transaction_id is None
    assert [op for op, _ in async_conn.sessionless_calls] == ["begin", "suspend"]

    with pytest.raises(RuntimeError, match="async-boom"):
        async with async_driver.sessionless_transaction("async-tx-1", resume=True, rollback_on_error=True):
            raise RuntimeError("async-boom")
    assert async_conn.rollbacks == 1
    assert async_driver.sessionless_transaction_id is None

    await async_driver.begin_sessionless_transaction("async-tx-2")
    await async_driver.execute("UPDATE orders SET status = :s", s="done", suspend_on_success=True)
    assert async_conn.executed[-1][2]["suspend_on_success"] is True
    assert async_driver.sessionless_transaction_id is None

    unsupported_async = SQLSpecOracleAsyncDriver(_FakeAsyncOracleClient())
    with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
        await unsupported_async.begin_sessionless_transaction("tx-unsupported")
    with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
        await unsupported_async.resume_sessionless_transaction("tx-unsupported")
    with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
        await unsupported_async.suspend_sessionless_transaction()
    with pytest.raises(SQLSpecDialectNotSupportedError, match="sessionless"):
        await unsupported_async.execute("UPDATE orders SET status = :s", s="done", suspend_on_success=True)


class _CountingOracleSyncCursor:
    """Proxy around a sync Oracle cursor counting native execute and fetchall calls."""

    def __init__(self, inner: Any, counts: dict[str, int]) -> None:
        self._inner = inner
        self._counts = counts

    def execute(self, sql: str, params: Any = (), *, out_binds: Any = None) -> Any:
        self._counts["execute"] = self._counts.get("execute", 0) + 1
        if out_binds is not None:
            return self._inner.execute(sql, params, out_binds=out_binds)
        return self._inner.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Any) -> Any:
        self._counts["executemany"] = self._counts.get("executemany", 0) + 1
        return self._inner.executemany(sql, seq_of_params)

    def fetchall(self) -> Any:
        self._counts["fetchall"] = self._counts.get("fetchall", 0) + 1
        return self._inner.fetchall()

    @property
    def description(self) -> Any:
        return self._inner.description

    @property
    def rowcount(self) -> int:
        return int(self._inner.rowcount)

    @property
    def lastrowid(self) -> Any:
        return self._inner.lastrowid

    @property
    def out_binds(self) -> Any:
        return self._inner.out_binds


class _CountingOracleAsyncCursor:
    """Proxy around an async Oracle cursor counting native execute and fetchall calls."""

    def __init__(self, inner: Any, counts: dict[str, int]) -> None:
        self._inner = inner
        self._counts = counts

    async def execute(self, sql: str, params: Any = (), *, out_binds: Any = None) -> Any:
        self._counts["execute"] = self._counts.get("execute", 0) + 1
        if out_binds is not None:
            return await self._inner.execute(sql, params, out_binds=out_binds)
        return await self._inner.execute(sql, params)

    async def executemany(self, sql: str, seq_of_params: Any) -> Any:
        self._counts["executemany"] = self._counts.get("executemany", 0) + 1
        return await self._inner.executemany(sql, seq_of_params)

    async def fetchall(self) -> Any:
        self._counts["fetchall"] = self._counts.get("fetchall", 0) + 1
        return await self._inner.fetchall()

    @property
    def description(self) -> Any:
        return self._inner.description

    @property
    def rowcount(self) -> int:
        return int(self._inner.rowcount)

    @property
    def lastrowid(self) -> Any:
        return self._inner.lastrowid

    @property
    def out_binds(self) -> Any:
        return self._inner.out_binds


class _CountingOracleSyncDriverProxy:
    """Proxy around `SyncOracleDriver` wrapping `checkout_cursor()` with call counting."""

    def __init__(self, inner: SyncOracleDriver) -> None:
        self._inner = inner
        self.config = inner.config
        self.counts: dict[str, int] = {}
        self.checkout_calls: int = 0
        self._last_out_binds: Any = None

    @contextlib.contextmanager
    def checkout_cursor(self, *, lazy_begin: bool = False) -> Iterator[_CountingOracleSyncCursor]:
        self.checkout_calls += 1
        with self._inner.checkout_cursor(lazy_begin=lazy_begin) as cur:
            yield _CountingOracleSyncCursor(cur, self.counts)

    @property
    def out_binds(self) -> Any:
        return self._last_out_binds


class _CountingOracleAsyncDriverProxy:
    """Proxy around `AsyncOracleDriver` wrapping `checkout_cursor()` with call counting."""

    def __init__(self, inner: AsyncOracleDriver) -> None:
        self._inner = inner
        self.config = inner.config
        self.counts: dict[str, int] = {}
        self.checkout_calls: int = 0
        self._last_out_binds: Any = None

    @contextlib.asynccontextmanager
    async def checkout_cursor(self, *, lazy_begin: bool = False) -> AsyncIterator[_CountingOracleAsyncCursor]:
        self.checkout_calls += 1
        async with self._inner.checkout_cursor(lazy_begin=lazy_begin) as cur:
            yield _CountingOracleAsyncCursor(cur, self.counts)

    @property
    def out_binds(self) -> Any:
        return self._last_out_binds


class _OracleMatrixStubClient(_StubSyncOracleClient):
    """Stub Oracle client servicing SELECT, UPDATE, execute_many, RETURNING INTO, and ORA-00001."""

    def query(
        self,
        sql: str,
        params: Any = None,
        *,
        fetch_decimals: bool = False,
        enable_lowercase_column_names: bool | None = None,
        out_binds: Any = None,
    ) -> _oracle_native.OracleQueryResult:
        _ = (fetch_decimals, enable_lowercase_column_names, out_binds)
        self.calls.append(("query", {"sql": sql, "params": params}))
        if "FROM dual" in sql:
            return _oracle_native.OracleQueryResult(
                [_oracle_native.OracleColumn("A", "NUMBER")],
                [(1,)],
                rowcount=1,
            )
        return _oracle_native.OracleQueryResult([], [], rowcount=0)

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
        if "DUP_FAIL" in sql:
            raise NativeIntegrityError("ORA-00001: unique constraint violated")
        if "RETURNING" in sql.upper():
            return _oracle_native.OracleQueryResult([], [], rowcount=1, out_binds={"id": 42})
        if sql.strip().upper().startswith("UPDATE"):
            return _oracle_native.OracleQueryResult([], [], rowcount=2)
        return _oracle_native.OracleQueryResult([], [], rowcount=1)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_oracle_sqlspec_native_cursor_cache_hit_rows_dml_many_returning_and_errors(
    is_async_mode: bool,
) -> None:
    """Verify Oracle SQLSpec adapter uses native cursors for cache-hit select, DML, many, RETURNING, and errors."""

    if not is_async_mode:
        stub_client = _OracleMatrixStubClient()
        native_driver = SyncOracleDriver(OracleConfig(), client=stub_client)
        proxy = _CountingOracleSyncDriverProxy(native_driver)
        session = SQLSpecOracleSyncDriver(proxy)

        first = session.select("SELECT 1 AS a FROM dual")
        assert first == [{"a": 1}]
        proxy.counts.clear()

        second = session.select("SELECT 1 AS a FROM dual")
        assert second == [{"a": 1}]
        assert proxy.counts.get("execute") == 1
        assert proxy.counts.get("fetchall") == 1

        typed_res = session.execute("UPDATE items SET name = :name", name=TypedParameter("updated", str))
        assert typed_res.rows_affected == 2

        many_res = session.execute_many(
            "INSERT INTO items (id, name) VALUES (:id, :name)",
            [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}],
        )
        assert many_res.rows_affected == 3

        out_var = _oracle_native.OracleOutBindVar("NUMBER")
        ret_res = session.execute(
            "INSERT INTO items (name) VALUES (:name) RETURNING id INTO :id",
            {"name": "alpha", "id": out_var},
        )
        assert ret_res.all() == [{"id": 42}]
        assert out_var.getvalue() == 42
        assert proxy.out_binds == {"id": 42}

        with pytest.raises(UniqueViolationError):
            session.execute("INSERT INTO DUP_FAIL (id) VALUES (:id)", id=1)
        return

    async_stub = _StubAsyncOracleClient()
    async_stub._sync = _OracleMatrixStubClient()
    async_native = AsyncOracleDriver(OracleConfig(), client=async_stub)
    async_proxy = _CountingOracleAsyncDriverProxy(async_native)
    async_session = SQLSpecOracleAsyncDriver(async_proxy)

    first = await async_session.select("SELECT 1 AS a FROM dual")
    assert first == [{"a": 1}]
    async_proxy.counts.clear()

    second = await async_session.select("SELECT 1 AS a FROM dual")
    assert second == [{"a": 1}]
    assert async_proxy.counts.get("execute") == 1
    assert async_proxy.counts.get("fetchall") == 1

    typed_res = await async_session.execute("UPDATE items SET name = :name", name=TypedParameter("updated", str))
    assert typed_res.rows_affected == 2

    many_res = await async_session.execute_many(
        "INSERT INTO items (id, name) VALUES (:id, :name)",
        [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}],
    )
    assert many_res.rows_affected == 3

    aout_var = _oracle_native.OracleOutBindVar("NUMBER")
    ret_res = await async_session.execute(
        "INSERT INTO items (name) VALUES (:name) RETURNING id INTO :id",
        {"name": "alpha", "id": aout_var},
    )
    assert ret_res.all() == [{"id": 42}]
    assert aout_var.getvalue() == 42
    assert async_proxy.out_binds == {"id": 42}

    with pytest.raises(UniqueViolationError):
        await async_session.execute("INSERT INTO DUP_FAIL (id) VALUES (:id)", id=1)


@pytest.mark.anyio
@pytest.mark.parametrize("is_async_mode", [False, True], ids=["sync", "async"])
async def test_oracle_sqlspec_pooled_session_checks_out_and_returns_one_client_per_statement(
    is_async_mode: bool,
) -> None:
    """Verify two statements on a pooled Oracle SQLSpec session each check out and return one client."""

    class _CountingSyncPool(_StubSyncOraclePool):
        def __init__(self) -> None:
            super().__init__(min_size=1, max_size=4)
            self.acquire_count = 0

        def acquire(self, **kwargs: Any) -> Any:
            self.acquire_count += 1
            return super().acquire(**kwargs)

    class _CountingAsyncPool(_StubAsyncOraclePool):
        def __init__(self) -> None:
            super().__init__(min_size=1, max_size=4)
            self.acquire_count = 0

        async def acquire(self, **kwargs: Any) -> Any:
            self.acquire_count += 1
            return await super().acquire(**kwargs)

    if not is_async_mode:
        pool = _CountingSyncPool()
        client = _OracleMatrixStubClient()
        pool._idle = [client]
        pool.created_clients = [client]
        native_driver = SyncOracleDriver(OracleConfig(), pool=pool)
        session = SQLSpecOracleSyncDriver(native_driver)

        assert session.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert pool.borrowed_size == 0
        assert pool.acquire_count == 1

        assert session.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
        assert pool.borrowed_size == 0
        assert pool.acquire_count == 2
        return

    async_pool = _CountingAsyncPool()
    async_client = _StubAsyncOracleClient()
    async_client._sync = _OracleMatrixStubClient()
    async_pool._idle = [async_client]
    async_pool.created_clients = [async_client]
    async_native = AsyncOracleDriver(OracleConfig(), pool=async_pool)
    async_session = SQLSpecOracleAsyncDriver(async_native)

    assert await async_session.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert async_pool.borrowed_size == 0
    assert async_pool.acquire_count == 1

    assert await async_session.select("SELECT 1 AS a FROM dual") == [{"a": 1}]
    assert async_pool.borrowed_size == 0
    assert async_pool.acquire_count == 2


class _NoCheckoutCursorOracleConnection:
    """Minimal connection double omitting `checkout_cursor`."""


def _build_oracle_cache_hit_fixture(
    mode: str,
    *,
    client_factory: Callable[[], _StubSyncOracleClient] | None = None,
    statement_config: StatementConfig | None = None,
) -> tuple[Any, Any, Any]:
    """Build `(adapter, proxy, stub_client)` for sync or async Oracle cache-hit tests."""
    if mode == "sync":
        client = client_factory() if client_factory is not None else _StubSyncOracleClient()
        native = SyncOracleDriver(OracleConfig(), client=client)
        proxy: Any = _CountingOracleSyncDriverProxy(native)
        adapter: Any = SQLSpecOracleSyncDriver(proxy, statement_config=statement_config)
        return adapter, proxy, client
    async_client = _StubAsyncOracleClient()
    if client_factory is not None:
        async_client._sync = client_factory()
    async_native = AsyncOracleDriver(OracleConfig(), client=async_client)
    async_proxy: Any = _CountingOracleAsyncDriverProxy(async_native)
    async_adapter: Any = SQLSpecOracleAsyncDriver(async_proxy, statement_config=statement_config)
    return async_adapter, async_proxy, async_client


def _wrap_oracle_cache_hit_state(adapter: Any, mode: str) -> dict[str, int]:
    """Wrap the first two callables in `adapter._cache_hit_state` to count cached invocations."""
    counts = {"fetch": 0, "dml": 0}
    orig_fetch, orig_dml, mapper = adapter._cache_hit_state
    if mode == "sync":

        def counted_fetch(*args: Any, **kwargs: Any) -> Any:
            counts["fetch"] += 1
            return orig_fetch(*args, **kwargs)

        def counted_dml(*args: Any, **kwargs: Any) -> Any:
            counts["dml"] += 1
            return orig_dml(*args, **kwargs)

        adapter._cache_hit_state = (counted_fetch, counted_dml, mapper)
        return counts

    async def counted_async_fetch(*args: Any, **kwargs: Any) -> Any:
        counts["fetch"] += 1
        return await orig_fetch(*args, **kwargs)

    async def counted_async_dml(*args: Any, **kwargs: Any) -> Any:
        counts["dml"] += 1
        return await orig_dml(*args, **kwargs)

    adapter._cache_hit_state = (counted_async_fetch, counted_async_dml, mapper)
    return counts


async def _run_oracle_execute(adapter: Any, mode: str, sql: str, *parameters: Any, **kwargs: Any) -> Any:
    """Execute `sql` against either a sync or async Oracle SQLSpec adapter."""
    if mode == "sync":
        return adapter.execute(sql, *parameters, **kwargs)
    return await adapter.execute(sql, *parameters, **kwargs)


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_oracle_cache_hit_executor_is_registered(mode: str) -> None:
    """Verify both Oracle SQLSpec adapters register the base cache-hit executor and 3-item state."""
    adapter, _, _ = _build_oracle_cache_hit_fixture(mode)
    expected_executor = (
        BaseSyncDriver._run_cached_statement if mode == "sync" else BaseAsyncDriver._run_cached_statement
    )
    assert adapter._cache_hit_executor is expected_executor
    assert adapter._cache_hit_state is not None
    assert len(adapter._cache_hit_state) == 3


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("sql", "expected_key", "other_key"),
    [
        ("SELECT 1 AS a FROM dual", "fetch", "dml"),
        ("UPDATE t SET a = 2", "dml", "fetch"),
    ],
    ids=["select", "update"],
)
async def test_oracle_cache_hit_makes_one_adapter_call_and_one_checkout(
    mode: str,
    sql: str,
    expected_key: str,
    other_key: str,
) -> None:
    """Verify a cached statement calls one adapter callable and checks out one cursor."""
    adapter, proxy, _ = _build_oracle_cache_hit_fixture(mode, client_factory=_OracleMatrixStubClient)
    counts = _wrap_oracle_cache_hit_state(adapter, mode)

    await _run_oracle_execute(adapter, mode, sql)
    before_checkouts = proxy.checkout_calls
    counts["fetch"] = 0
    counts["dml"] = 0

    await _run_oracle_execute(adapter, mode, sql)
    assert counts[expected_key] == 1
    assert counts[other_key] == 0
    assert proxy.checkout_calls == before_checkouts + 1


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
@pytest.mark.parametrize(
    ("sql", "result_factory"),
    [
        (
            "SELECT 1 AS a FROM dual",
            lambda: _oracle_native.OracleQueryResult(
                [_oracle_native.OracleColumn("A", "NUMBER")],
                [(1,)],
                rowcount=1,
            ),
        ),
        (
            "SELECT a FROM empty_items",
            lambda: _oracle_native.OracleQueryResult(
                [_oracle_native.OracleColumn("A", "NUMBER")],
                [],
                rowcount=0,
            ),
        ),
        (
            "UPDATE t SET a = 2",
            lambda: _oracle_native.OracleQueryResult([], [], rowcount=2),
        ),
        (
            "INSERT INTO t (a) VALUES (1)",
            lambda: _oracle_native.OracleQueryResult([], [], rowcount=1, lastrowid="AAAR3sAAEAAAACXAAB"),
        ),
    ],
    ids=["select", "empty_select", "update", "insert"],
)
async def test_oracle_cache_hit_result_matches_method_path(
    mode: str,
    sql: str,
    result_factory: Callable[[], _oracle_native.OracleQueryResult],
) -> None:
    """Verify cached executor results match the method path across select, empty select, update, and insert."""
    exec_adapter, _, exec_client = _build_oracle_cache_hit_fixture(mode)
    method_adapter, _, method_client = _build_oracle_cache_hit_fixture(mode)
    method_adapter._cache_hit_executor = None
    counts = _wrap_oracle_cache_hit_state(exec_adapter, mode)

    exec_client.queued_results.extend([result_factory(), result_factory()])
    method_client.queued_results.extend([result_factory(), result_factory()])

    await _run_oracle_execute(exec_adapter, mode, sql)
    await _run_oracle_execute(method_adapter, mode, sql)
    counts["fetch"] = 0
    counts["dml"] = 0

    exec_res = await _run_oracle_execute(exec_adapter, mode, sql)
    method_res = await _run_oracle_execute(method_adapter, mode, sql)

    assert counts["fetch"] + counts["dml"] == 1
    assert exec_res.all() == method_res.all()
    assert exec_res.column_names == method_res.column_names
    assert exec_res.rows_affected == method_res.rows_affected


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_oracle_cache_hit_records_out_binds(mode: str) -> None:
    """Verify cached runs update `connection._last_out_binds` to cursor `out_binds` and clear it to `None`."""
    adapter, proxy, client = _build_oracle_cache_hit_fixture(mode)
    counts = _wrap_oracle_cache_hit_state(adapter, mode)
    first_sql = "UPDATE out_bind_tbl SET a = 1"
    second_sql = "UPDATE out_bind_tbl SET a = 2"

    client.queued_results.extend(
        [
            _oracle_native.OracleQueryResult([], [], rowcount=1, out_binds={"out": 99}),
            _oracle_native.OracleQueryResult([], [], rowcount=1, out_binds=None),
            _oracle_native.OracleQueryResult([], [], rowcount=1, out_binds={"out": 99}),
            _oracle_native.OracleQueryResult([], [], rowcount=1, out_binds=None),
        ]
    )

    await _run_oracle_execute(adapter, mode, first_sql)
    await _run_oracle_execute(adapter, mode, second_sql)
    counts["fetch"] = 0
    counts["dml"] = 0

    await _run_oracle_execute(adapter, mode, first_sql)
    assert counts["dml"] == 1
    assert proxy._last_out_binds == {"out": 99}

    await _run_oracle_execute(adapter, mode, second_sql)
    assert counts["dml"] == 2
    assert proxy._last_out_binds is None


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_oracle_cache_hit_not_registered_with_execution_args(mode: str) -> None:
    """Verify Oracle adapters skip executor registration when `statement_config.execution_args` is non-empty."""
    config = build_oracle_statement_config().replace(execution_args={"out_binds": {"out": "NUMBER"}})
    adapter, _, _ = _build_oracle_cache_hit_fixture(
        mode,
        client_factory=_OracleMatrixStubClient,
        statement_config=config,
    )
    assert adapter._cache_hit_executor is None

    first = await _run_oracle_execute(adapter, mode, "SELECT 1 AS a FROM dual")
    second = await _run_oracle_execute(adapter, mode, "SELECT 1 AS a FROM dual")
    assert first.all() == [{"a": 1}]
    assert second.all() == [{"a": 1}]


@pytest.mark.parametrize("mode", ["sync", "async"])
def test_oracle_cache_hit_not_registered_without_checkout_cursor(mode: str) -> None:
    """Verify Oracle adapters register no cache-hit executor when the connection lacks `checkout_cursor`."""
    conn = _NoCheckoutCursorOracleConnection()
    adapter: Any = SQLSpecOracleSyncDriver(conn) if mode == "sync" else SQLSpecOracleAsyncDriver(conn)
    assert adapter._cache_hit_executor is None


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_oracle_cache_hit_errors_match_method_path(mode: str) -> None:
    """Verify native errors on a cached statement map identically to the method path and preserve `__cause__`."""
    exec_adapter, exec_proxy, exec_client = _build_oracle_cache_hit_fixture(mode)
    method_adapter, _, method_client = _build_oracle_cache_hit_fixture(mode)
    method_adapter._cache_hit_executor = None
    counts = _wrap_oracle_cache_hit_state(exec_adapter, mode)

    exec_native_err = NativeIntegrityError("ORA-00001: unique constraint violated")
    method_native_err = NativeIntegrityError("ORA-00001: unique constraint violated")
    sql = "INSERT INTO err_tbl (id) VALUES (1)"

    exec_client.queued_results.extend([_oracle_native.OracleQueryResult([], [], rowcount=1), exec_native_err])
    method_client.queued_results.extend([_oracle_native.OracleQueryResult([], [], rowcount=1), method_native_err])

    await _run_oracle_execute(exec_adapter, mode, sql)
    await _run_oracle_execute(method_adapter, mode, sql)
    before_checkouts = exec_proxy.checkout_calls
    counts["fetch"] = 0
    counts["dml"] = 0

    with pytest.raises(UniqueViolationError) as exec_exc_info:
        await _run_oracle_execute(exec_adapter, mode, sql)
    with pytest.raises(UniqueViolationError) as method_exc_info:
        await _run_oracle_execute(method_adapter, mode, sql)

    assert type(exec_exc_info.value) is type(method_exc_info.value)
    assert exec_exc_info.value.__cause__ is exec_native_err
    assert counts["dml"] == 1
    assert exec_proxy.checkout_calls == before_checkouts + 1


@pytest.mark.anyio
@pytest.mark.parametrize("mode", ["sync", "async"])
async def test_oracle_suspend_on_success_skips_executor(mode: str) -> None:
    """Verify `execute(..., suspend_on_success=True)` bypasses the cache-hit executor."""
    adapter: Any = (
        SQLSpecOracleSyncDriver(_FakeOracleSessionlessClient())
        if mode == "sync"
        else SQLSpecOracleAsyncDriver(_FakeAsyncOracleSessionlessClient())
    )
    assert adapter._cache_hit_executor is not None
    counts = _wrap_oracle_cache_hit_state(adapter, mode)
    sql = "UPDATE sessionless_orders SET status = 'done'"

    await _run_oracle_execute(adapter, mode, sql, suspend_on_success=True)
    await _run_oracle_execute(adapter, mode, sql, suspend_on_success=True)

    assert counts["fetch"] == 0
    assert counts["dml"] == 0
