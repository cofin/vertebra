"""Unit tests for SQLSpec StatementStack execution, native binds, and PostgreSQL/Oracle adapter stacks."""

from __future__ import annotations

import importlib
import sqlite3
import subprocess
import sys
from collections.abc import Mapping
from typing import Any, ClassVar

import pyarrow as pa
import pytest
from sqlspec import SQL, StatementStack
from sqlspec.core.filters import NullFilter
from sqlspec.core.statement import StatementConfig
from sqlspec.exceptions import DialectNotSupportedError as SQLSpecDialectNotSupportedError

import vertebra.extensions.sqlspec as sqlspec_extension
from tests.unit.db.postgres_ports import FakeAsyncPostgresPool, FakePostgresCluster, FakeSyncPostgresPool
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
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

    def cursor(self) -> Any:
        cursor = self._sync.cursor()

        class Cursor:
            @property
            def description(self) -> Any:
                return cursor.description

            @property
            def rowcount(self) -> int:
                return int(cursor.rowcount)

            async def execute(self, sql: str, params: Any = None) -> None:
                cursor.execute(sql, params)

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

    def close(self) -> None:
        self.closed = True


class _FakeOracleSessionlessClient(_FakeOracleClient):
    """Fake Oracle connection exposing native 26ai sessionless transaction methods."""

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

    async def close(self) -> None:
        self.closed = True


class _FakeAsyncOracleSessionlessClient(_FakeAsyncOracleClient):
    """Async fake Oracle connection exposing native 26ai sessionless transaction methods."""

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
