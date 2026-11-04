"""First-class SQLSpec adapter configurations and drivers backed by Vertebra Rust-native SHM drivers."""

from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager, suppress
from types import TracebackType
from typing import Any, ClassVar, Self, cast

import msgspec

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.db._registry import DatabaseRegistry
from vertebra.db.dialer.alloydb import AsyncAlloyDBDialer, SyncAlloyDBDialer
from vertebra.db.dialer.cloudsql import AsyncCloudSQLDialer, SyncCloudSQLDialer
from vertebra.db.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.pipeline import AsyncQueryPipeline, PipelineDialect, SyncQueryPipeline
from vertebra.db.postgres import (
    AsyncPostgreSQLDriver,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.rls import TenantPredicate
from vertebra.db.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import ZeroCopyCodecRegistry

try:
    from sqlspec import StackResult, StatementStack
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.core.result._base import ArrowResult
    from sqlspec.core.statement import SQL, StatementConfig
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
except ImportError as exc:
    raise MissingDependencyError("sqlspec", install_package="sqlspec", extra="sqlspec") from exc

__all__ = (
    "AsyncAlloyDBDialer",
    "AsyncCloudSQLDialer",
    "AsyncQueryPipeline",
    "ArrowCursor",
    "AsyncAdapterConfig",
    "DuckDBAsyncConfig",
    "DuckDBAsyncDriver",
    "DuckDBSyncConfig",
    "DuckDBSyncDriver",
    "MSSQLAsyncConfig",
    "MSSQLAsyncDriver",
    "MSSQLSyncConfig",
    "MSSQLSyncDriver",
    "MySQLAsyncConfig",
    "MySQLAsyncDriver",
    "MySQLSyncConfig",
    "MySQLSyncDriver",
    "OracleAsyncConfig",
    "OracleAsyncDriver",
    "OracleSyncConfig",
    "OracleSyncDriver",
    "PostgreSQLAsyncConfig",
    "PostgreSQLAsyncDriver",
    "PostgreSQLSyncConfig",
    "PostgreSQLSyncDriver",
    "SyncAlloyDBDialer",
    "SyncCloudSQLDialer",
    "SyncQueryPipeline",
    "SQLiteAsyncConfig",
    "SQLiteAsyncDriver",
    "SQLiteSyncConfig",
    "SQLiteSyncDriver",
    "SyncAdapterConfig",
    "TenantPredicate",
)


class DeferredSyncExceptionHandler:
    """Synchronous exception handler storing deferred exceptions for caller handling."""

    __slots__ = ("_pending_exception",)

    def __init__(self) -> None:
        """Initialize exception handler with no pending exception."""
        self._pending_exception: Exception | None = None

    @property
    def pending_exception(self) -> Exception | None:
        """Return pending exception deferred during exit."""
        return self._pending_exception

    def __enter__(self) -> Self:
        """Enter synchronous context manager."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """Exit context manager and defer mapped exception."""
        _ = (exc_type, exc_tb)
        if exc_val is not None and isinstance(exc_val, Exception):
            self._pending_exception = exc_val
            return True
        return False


class DeferredAsyncExceptionHandler:
    """Asynchronous exception handler storing deferred exceptions for caller handling."""

    __slots__ = ("_pending_exception",)

    def __init__(self) -> None:
        """Initialize exception handler with no pending exception."""
        self._pending_exception: Exception | None = None

    @property
    def pending_exception(self) -> Exception | None:
        """Return pending exception deferred during exit."""
        return self._pending_exception

    async def __aenter__(self) -> Self:
        """Enter asynchronous context manager."""
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        """Exit asynchronous context manager and defer mapped exception."""
        _ = (exc_type, exc_tb)
        if exc_val is not None and isinstance(exc_val, Exception):
            self._pending_exception = exc_val
            return True
        return False


class BaseSyncDriver(SyncDriverAdapterBase):
    """Base synchronous driver adapter implementing standard SQLSpec execution methods."""

    dialect: str = "generic"

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
    ) -> None:
        """Initialize driver with native connection and statement configuration."""
        stmt_cfg = statement_config or StatementConfig(dialect=self.dialect)
        super().__init__(connection, stmt_cfg, driver_features, observability)
        self.native_driver: Any = connection

    @property
    def data_dictionary(self) -> Any:
        """Return data dictionary instance for metadata queries."""
        return None

    def execute(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a DML or DDL statement and return the affected row count."""
        _ = (statement_config, kwargs)
        if isinstance(statement, SQL):
            sql, p = statement.compile()
            res = self.native_driver.execute(sql, p) if p else self.native_driver.execute(sql)
        else:
            sql = str(statement)
            params = self._normalize_params(parameters)
            res = self.native_driver.execute(sql, params) if params is not None else self.native_driver.execute(sql)
        return len(res) if isinstance(res, list) else int(res)

    def select(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return rows as dictionaries or converted schema instances."""
        _ = (statement_config, kwargs)
        if isinstance(statement, SQL):
            sql, p = statement.compile()
            params = p
        else:
            sql = str(statement)
            params = self._normalize_params(parameters)

        if schema_type is not None and schema_type is not dict:
            try:
                res = (
                    self.native_driver.select(sql, params, schema_type=schema_type)
                    if params is not None
                    else self.native_driver.select(sql, schema_type=schema_type)
                )
                return list(res)
            except TypeError:
                pass

        res = self.native_driver.select(sql, params) if params is not None else self.native_driver.select(sql)
        rows: list[Any] = list(res)
        if not rows or schema_type is None or schema_type is dict:
            return rows
        if isinstance(rows[0], schema_type):
            return rows
        plan = ZeroCopyCodecRegistry.compile(schema_type)
        if isinstance(rows[0], dict):
            cols = tuple(rows[0].keys())
            tuple_rows = [tuple(r.get(c) for c in cols) for r in rows]
            return plan.hydrate_rows(tuple_rows, cols)
        return [msgspec.convert(row, type=schema_type) for row in rows]

    def select_one(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first row or raise ValueError."""
        _ = statement_config
        rows = self.select(statement, *parameters, schema_type=schema_type, **kwargs)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    def select_one_or_none(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query and return the first row or None."""
        _ = statement_config
        rows = self.select(statement, *parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    def select_value(
        self,
        statement: Any,
        /,
        *parameters: Any,
        value_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query and return the first scalar value."""
        _ = (value_type, statement_config)
        row = self.select_one_or_none(statement, *parameters, **kwargs)
        if row is None:
            return None
        return next(iter(row.values())) if isinstance(row, dict) else row[0]

    def execute_many(
        self,
        statement: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a batch statement across multiple parameter sets."""
        _ = (filters, statement_config, kwargs)
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        exec_many_fn = getattr(self.native_driver, "execute_many", None)
        if callable(exec_many_fn):
            return int(exec_many_fn(sql, list(parameters)))
        return sum(self.execute(sql, item) for item in parameters)

    def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any | None = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a query and return an ArrowResult containing a PyArrow Table."""
        _ = (statement_config, return_format, native_only, batch_size, arrow_schema)
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = self._normalize_params(parameters)
        arrow_fn = getattr(self.native_driver, "select_to_arrow", None)
        if callable(arrow_fn):
            raw = arrow_fn(sql, params) if params is not None else arrow_fn(sql)
            if hasattr(raw, "to_arrow_table"):
                table = raw.to_arrow_table()
            elif hasattr(raw, "read_all"):
                table = raw.read_all()
            else:
                table = raw
        else:
            rows = self.select(sql, *parameters)
            pa = import_optional("pyarrow")
            table = pa.Table.from_pylist(rows) if pa is not None else rows
        num_rows = len(table) if hasattr(table, "__len__") else getattr(table, "num_rows", 0)
        return ArrowResult(statement=SQL(sql), data=table, rows_affected=num_rows)

    def execute_stack(self, stack: StatementStack, *, continue_on_error: bool = False) -> tuple[StackResult, ...]:
        """Execute a StatementStack using SyncQueryPipeline CTE compilation when applicable or sequential execution."""
        operations = getattr(stack, "operations", ())
        all_read = bool(operations)
        for op in operations:
            stmt_raw = str(getattr(op, "statement", "")).strip().upper()
            if not (stmt_raw.startswith("SELECT") or stmt_raw.startswith("WITH")):
                all_read = False
                break

        if all_read and len(operations) > 1:
            pipeline_dialect: PipelineDialect = (
                "googlesql" if self.dialect in {"bigquery", "spanner", "googlesql"} else "postgres"
            )
            pipeline = SyncQueryPipeline(dialect=pipeline_dialect)
            for idx, op in enumerate(operations):
                stmt_sql = str(getattr(op, "statement", ""))
                args = getattr(op, "arguments", ())
                p = dict(args[0]) if args and isinstance(args[0], dict) else {}
                pipeline.add_step(f"step_{idx}", stmt_sql, params=p)
            with suppress(Exception):
                compiled_sql, merged_params = pipeline.compile()
                rows = self.select(compiled_sql, merged_params)
                return tuple(StackResult(rows_affected=len(rows)) for _ in operations)

        results: list[StackResult] = []
        self.begin()
        try:
            for op in operations:
                stmt = getattr(op, "statement", "")
                args = getattr(op, "arguments", ())
                rc = self.execute(str(stmt), *args)
                results.append(StackResult(rows_affected=rc))
            self.commit()
        except Exception as exc:
            self.rollback()
            if not continue_on_error:
                raise
            results.append(StackResult.from_error(exc))
        return tuple(results)

    def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Apply the transaction-local tenant GUC and return the SQL filter fragment.

        Raises:
            Exception: Any error raised by the driver while executing the GUC statement.
        """
        sql, params = TenantPredicate.build_session_guc_sql(
            str(predicate.tenant_value),
            predicate.config,
            is_local=True,
        )
        self.execute(sql, params)
        return predicate.to_sql_fragment()

    def dispatch_execute(self, cursor: Any, statement: Any) -> Any:
        """Execute a single SQL statement on the cursor."""
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = getattr(statement, "parameters", None)
        res = cursor.execute(sql, params) if params is not None else cursor.execute(sql)
        rowcount = len(res) if isinstance(res, list) else int(res)
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    def dispatch_execute_many(self, cursor: Any, statement: Any) -> Any:
        """Execute a SQL statement with multiple parameter sets on the cursor."""
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = getattr(statement, "parameters", ())
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            res = exec_many_fn(sql, list(params))
            rowcount = int(res)
        else:
            rowcount = sum(cursor.execute(sql, item) for item in params)
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

    def handle_database_exceptions(self) -> Any:
        """Return sync exception handler for driver operations."""
        return DeferredSyncExceptionHandler()

    @contextmanager
    def with_cursor(self, connection: Any) -> Iterator[Any]:
        """Provide a context manager yielding an active database cursor."""
        cursor_fn = getattr(connection, "cursor", None)
        cursor = cursor_fn() if callable(cursor_fn) else connection
        try:
            yield cursor
        finally:
            close_fn = getattr(cursor, "close", None)
            if callable(close_fn) and cursor is not connection:
                close_fn()

    def begin(self) -> None:
        """Begin a transaction on the underlying connection."""
        begin_fn = getattr(self.native_driver, "begin", None)
        if callable(begin_fn):
            begin_fn()

    def commit(self) -> None:
        """Commit the active transaction on the underlying connection."""
        commit_fn = getattr(self.native_driver, "commit", None)
        if callable(commit_fn):
            commit_fn()

    def rollback(self) -> None:
        """Roll back the active transaction on the underlying connection."""
        rollback_fn = getattr(self.native_driver, "rollback", None)
        if callable(rollback_fn):
            rollback_fn()

    def close(self) -> None:
        """Close the underlying connection."""
        close_fn = getattr(self.native_driver, "close", None)
        if callable(close_fn):
            close_fn()

    @staticmethod
    def _normalize_params(parameters: tuple[Any, ...]) -> Any:
        """Normalize parameter tuple into positional or keyword parameter structures."""
        if not parameters:
            return None
        if len(parameters) == 1 and isinstance(parameters[0], (dict, list, tuple)):
            return parameters[0]
        return parameters


class BaseAsyncDriver(AsyncDriverAdapterBase):
    """Base asynchronous driver adapter implementing standard SQLSpec execution methods."""

    dialect: str = "generic"

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
    ) -> None:
        """Initialize driver with native connection and statement configuration."""
        stmt_cfg = statement_config or StatementConfig(dialect=self.dialect)
        super().__init__(connection, stmt_cfg, driver_features, observability)
        self.native_driver: Any = connection

    @property
    def data_dictionary(self) -> Any:
        """Return data dictionary instance for metadata queries."""
        return None

    async def execute(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a DML or DDL statement asynchronously and return the affected row count."""
        _ = (statement_config, kwargs)
        if isinstance(statement, SQL):
            sql, p = statement.compile()
            res = await self.native_driver.execute(sql, p) if p else await self.native_driver.execute(sql)
        else:
            sql = str(statement)
            params = BaseSyncDriver._normalize_params(parameters)
            res = (
                await self.native_driver.execute(sql, params)
                if params is not None
                else await self.native_driver.execute(sql)
            )
        return len(res) if isinstance(res, list) else int(res)

    async def select(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return rows as dictionaries or schema instances."""
        _ = (statement_config, kwargs)
        if isinstance(statement, SQL):
            sql, p = statement.compile()
            params = p
        else:
            sql = str(statement)
            params = BaseSyncDriver._normalize_params(parameters)

        if schema_type is not None and schema_type is not dict:
            try:
                res = (
                    await self.native_driver.select(sql, params, schema_type=schema_type)
                    if params is not None
                    else await self.native_driver.select(sql, schema_type=schema_type)
                )
                return list(res)
            except TypeError:
                pass

        res = (
            await self.native_driver.select(sql, params) if params is not None else await self.native_driver.select(sql)
        )
        rows: list[Any] = list(res)
        if not rows or schema_type is None or schema_type is dict:
            return rows
        if isinstance(rows[0], schema_type):
            return rows
        plan = ZeroCopyCodecRegistry.compile(schema_type)
        if isinstance(rows[0], dict):
            cols = tuple(rows[0].keys())
            tuple_rows = [tuple(r.get(c) for c in cols) for r in rows]
            return plan.hydrate_rows(tuple_rows, cols)
        return [msgspec.convert(row, type=schema_type) for row in rows]

    async def select_one(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first row or raise ValueError."""
        _ = statement_config
        rows = await self.select(statement, *parameters, schema_type=schema_type, **kwargs)
        if not rows:
            msg = "No row found for select_one"
            raise ValueError(msg)
        return rows[0]

    async def select_one_or_none(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any | None:
        """Execute a query asynchronously and return the first row or None."""
        _ = statement_config
        rows = await self.select(statement, *parameters, schema_type=schema_type, **kwargs)
        return rows[0] if rows else None

    async def select_value(
        self,
        statement: Any,
        /,
        *parameters: Any,
        value_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a query asynchronously and return the first scalar value."""
        _ = (value_type, statement_config)
        row = await self.select_one_or_none(statement, *parameters, **kwargs)
        if row is None:
            return None
        return next(iter(row.values())) if isinstance(row, dict) else row[0]

    async def execute_many(
        self,
        statement: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a batch statement asynchronously across multiple parameter sets."""
        _ = (filters, statement_config, kwargs)
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        exec_many_fn = getattr(self.native_driver, "execute_many", None)
        if callable(exec_many_fn):
            res = exec_many_fn(sql, list(parameters))
            if hasattr(res, "__await__"):
                res = await res
            return int(res)
        total = 0
        for item in parameters:
            total += await self.execute(sql, item)
        return total

    async def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        return_format: str = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any | None = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a query asynchronously and return an ArrowResult containing a PyArrow Table."""
        _ = (statement_config, return_format, native_only, batch_size, arrow_schema)
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = BaseSyncDriver._normalize_params(parameters)
        arrow_fn = getattr(self.native_driver, "select_to_arrow", None)
        if callable(arrow_fn):
            raw = await arrow_fn(sql, params) if params is not None else await arrow_fn(sql)
            if hasattr(raw, "to_arrow_table"):
                table = raw.to_arrow_table()
            elif hasattr(raw, "read_all"):
                table = raw.read_all()
            else:
                table = raw
        else:
            rows = await self.select(sql, *parameters)
            pa = import_optional("pyarrow")
            table = pa.Table.from_pylist(rows) if pa is not None else rows
        num_rows = len(table) if hasattr(table, "__len__") else getattr(table, "num_rows", 0)
        return ArrowResult(statement=SQL(sql), data=table, rows_affected=num_rows)

    async def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute a StatementStack asynchronously using AsyncQueryPipeline or sequential execution."""
        operations = getattr(stack, "operations", ())
        all_read = bool(operations)
        for op in operations:
            stmt_raw = str(getattr(op, "statement", "")).strip().upper()
            if not (stmt_raw.startswith("SELECT") or stmt_raw.startswith("WITH")):
                all_read = False
                break

        if all_read and len(operations) > 1:
            pipeline_dialect: PipelineDialect = (
                "googlesql" if self.dialect in {"bigquery", "spanner", "googlesql"} else "postgres"
            )
            pipeline = AsyncQueryPipeline(dialect=pipeline_dialect)
            for idx, op in enumerate(operations):
                stmt_sql = str(getattr(op, "statement", ""))
                args = getattr(op, "arguments", ())
                p = dict(args[0]) if args and isinstance(args[0], dict) else {}
                pipeline.add_step(f"step_{idx}", stmt_sql, params=p)
            with suppress(Exception):
                compiled_sql, merged_params = pipeline.compile()
                rows = await self.select(compiled_sql, merged_params)
                return tuple(StackResult(rows_affected=len(rows)) for _ in operations)

        results: list[StackResult] = []
        await self.begin()
        try:
            for op in operations:
                stmt = getattr(op, "statement", "")
                args = getattr(op, "arguments", ())
                rc = await self.execute(str(stmt), *args)
                results.append(StackResult(rows_affected=rc))
            await self.commit()
        except Exception as exc:
            await self.rollback()
            if not continue_on_error:
                raise
            results.append(StackResult.from_error(exc))
        return tuple(results)

    async def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Apply the transaction-local tenant GUC and return the SQL filter fragment.

        Raises:
            Exception: Any error raised by the driver while executing the GUC statement.
        """
        sql, params = TenantPredicate.build_session_guc_sql(
            str(predicate.tenant_value),
            predicate.config,
            is_local=True,
        )
        await self.execute(sql, params)
        return predicate.to_sql_fragment()

    async def dispatch_execute(self, cursor: Any, statement: Any) -> Any:
        """Execute a single SQL statement asynchronously on the cursor."""
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = getattr(statement, "parameters", None)
        res = await cursor.execute(sql, params) if params is not None else await cursor.execute(sql)
        rowcount = len(res) if isinstance(res, list) else int(res)
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    async def dispatch_execute_many(self, cursor: Any, statement: Any) -> Any:
        """Execute a SQL statement with multiple parameter sets asynchronously on the cursor."""
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = getattr(statement, "parameters", ())
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            res = exec_many_fn(sql, list(params))
            if hasattr(res, "__await__"):
                res = await res
            rowcount = int(res)
        else:
            rowcount = 0
            for item in params:
                rowcount += await cursor.execute(sql, item)
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

    def handle_database_exceptions(self) -> Any:
        """Return async exception handler for driver operations."""
        return DeferredAsyncExceptionHandler()

    @asynccontextmanager
    async def with_cursor(self, connection: Any) -> AsyncIterator[Any]:
        """Provide an async context manager yielding an active database cursor."""
        cursor_fn = getattr(connection, "cursor", None)
        cursor = cursor_fn() if callable(cursor_fn) else connection
        if hasattr(cursor, "__await__"):
            cursor = await cursor
        try:
            yield cursor
        finally:
            close_fn = getattr(cursor, "close", None)
            if callable(close_fn) and cursor is not connection:
                res = close_fn()
                if hasattr(res, "__await__"):
                    await res

    async def begin(self) -> None:
        """Begin a transaction asynchronously on the underlying connection."""
        begin_fn = getattr(self.native_driver, "begin", None)
        if callable(begin_fn):
            res = begin_fn()
            if hasattr(res, "__await__"):
                await res

    async def commit(self) -> None:
        """Commit the active transaction asynchronously on the underlying connection."""
        commit_fn = getattr(self.native_driver, "commit", None)
        if callable(commit_fn):
            res = commit_fn()
            if hasattr(res, "__await__"):
                await res

    async def rollback(self) -> None:
        """Roll back the active transaction asynchronously on the underlying connection."""
        rollback_fn = getattr(self.native_driver, "rollback", None)
        if callable(rollback_fn):
            res = rollback_fn()
            if hasattr(res, "__await__"):
                await res

    async def close(self) -> None:
        """Close the underlying connection asynchronously."""
        close_fn = getattr(self.native_driver, "close", None)
        if callable(close_fn):
            res = close_fn()
            if hasattr(res, "__await__"):
                await res


class SQLiteSyncDriver(BaseSyncDriver):
    """Synchronous SQLite driver adapter wrapping vertebra.db.sqlite.SyncSQLiteDriver."""

    dialect: str = "sqlite"


class SQLiteAsyncDriver(BaseAsyncDriver):
    """Asynchronous SQLite driver adapter wrapping vertebra.db.sqlite.AsyncSQLiteDriver."""

    dialect: str = "sqlite"


class PostgreSQLSyncDriver(BaseSyncDriver):
    """Synchronous PostgreSQL driver adapter wrapping vertebra.db.postgres.SyncPostgreSQLDriver."""

    dialect: str = "postgres"


class PostgreSQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous PostgreSQL driver adapter wrapping vertebra.db.postgres.AsyncPostgreSQLDriver."""

    dialect: str = "postgres"


class MySQLSyncDriver(BaseSyncDriver):
    """Synchronous MySQL driver adapter wrapping vertebra.db.mysql.SyncMySQLDriver."""

    dialect: str = "mysql"


class MySQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous MySQL driver adapter wrapping vertebra.db.mysql.AsyncMySQLDriver."""

    dialect: str = "mysql"


class OracleSyncDriver(BaseSyncDriver):
    """Synchronous Oracle Thin driver adapter wrapping vertebra.db.oracle.SyncOracleDriver."""

    dialect: str = "oracle"


class OracleAsyncDriver(BaseAsyncDriver):
    """Asynchronous Oracle Thin driver adapter wrapping vertebra.db.oracle.AsyncOracleDriver."""

    dialect: str = "oracle"


class MSSQLSyncDriver(BaseSyncDriver):
    """Synchronous MSSQL driver adapter wrapping vertebra.db.mssql.SyncMSSQLDriver."""

    dialect: str = "mssql"


class MSSQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous MSSQL driver adapter wrapping vertebra.db.mssql.AsyncMSSQLDriver."""

    dialect: str = "mssql"


class DuckDBSyncDriver(BaseSyncDriver):
    """Synchronous DuckDB driver adapter wrapping vertebra.db.duckdb.SyncDuckDBDriver."""

    dialect: str = "duckdb"


class DuckDBAsyncDriver(BaseAsyncDriver):
    """Asynchronous DuckDB driver adapter wrapping vertebra.db.duckdb.AsyncDuckDBDriver."""

    dialect: str = "duckdb"


class SyncAdapterConfig[ConfigT, DriverT: SyncDriverAdapterBase](SyncDatabaseConfig[Any, Any, DriverT]):
    """Base synchronous SQLSpec adapter configuration backed by a Vertebra native driver."""

    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = False
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]
    driver_type: ClassVar[type[Any]]

    def __init__(
        self,
        config: ConfigT | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous configuration."""
        self.bind_key = bind_key
        self.share_pool: bool = bool(share_pool)
        config_broker = getattr(config, "broker", None) if config is not None else None
        if broker is not None:
            self.broker = broker
        elif config_broker is not None:
            self.broker = config_broker
        elif self.share_pool:
            self.broker = DatabaseRegistry.default_broker()
        else:
            self.broker = None
        self.arrow_threshold_rows = arrow_threshold_rows
        if config is not None:
            self.driver_config: ConfigT = config
        else:
            if self.broker is not None and "broker" not in kwargs:
                kwargs["broker"] = self.broker
            self.driver_config = self._config_cls(**kwargs)
        self._driver_instance: DriverT | None = None
        super().__init__(
            bind_key=bind_key,
            statement_config=statement_config or StatementConfig(dialect=self.dialect),
        )

    def create_driver(self) -> DriverT:
        """Instantiate or return the cached synchronous Vertebra database driver adapter."""
        if self._driver_instance is None:
            if self.share_pool and self.broker is DatabaseRegistry.default_broker():
                native = DatabaseRegistry.default().get_shared_sync_driver(cast("Any", self.driver_config))
            else:
                native = self._driver_cls(self.driver_config, broker=self.broker)
            adapter = self.driver_type(native, statement_config=self.statement_config)
            self._driver_instance = cast("DriverT", adapter)
        return self._driver_instance

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Iterator[DriverT]:
        """Yield a synchronous SQLSpec adapter session."""
        _ = (args, kwargs)
        driver = self.create_driver()
        yield driver

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        """Yield the underlying raw database connection."""
        _ = (args, kwargs)
        driver = self.create_driver()
        yield getattr(driver, "connection", driver)

    def close(self) -> None:
        """Close the cached synchronous driver if initialized."""
        if self._driver_instance is not None and hasattr(self._driver_instance, "close"):
            self._driver_instance.close()
            self._driver_instance = None

    def _create_pool(self) -> Any:
        """Return the instantiated driver as pool."""
        return self.create_driver()

    def _close_pool(self) -> None:
        """Close the cached driver instance."""
        self.close()


class AsyncAdapterConfig[ConfigT, DriverT: AsyncDriverAdapterBase](AsyncDatabaseConfig[Any, Any, DriverT]):
    """Base asynchronous SQLSpec adapter configuration backed by a Vertebra native driver."""

    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = True
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]
    driver_type: ClassVar[type[Any]]

    def __init__(
        self,
        config: ConfigT | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous configuration."""
        self.bind_key = bind_key
        self.share_pool: bool = bool(share_pool)
        config_broker = getattr(config, "broker", None) if config is not None else None
        if broker is not None:
            self.broker = broker
        elif config_broker is not None:
            self.broker = config_broker
        elif self.share_pool:
            self.broker = DatabaseRegistry.default_broker()
        else:
            self.broker = None
        self.arrow_threshold_rows = arrow_threshold_rows
        if config is not None:
            self.driver_config: ConfigT = config
        else:
            if self.broker is not None and "broker" not in kwargs:
                kwargs["broker"] = self.broker
            self.driver_config = self._config_cls(**kwargs)
        self._driver_instance: DriverT | None = None
        super().__init__(
            bind_key=bind_key,
            statement_config=statement_config or StatementConfig(dialect=self.dialect),
        )

    def create_driver(self) -> DriverT:
        """Instantiate or return the cached asynchronous Vertebra database driver adapter."""
        if self._driver_instance is None:
            if self.share_pool and self.broker is DatabaseRegistry.default_broker():
                native = DatabaseRegistry.default().get_shared_driver(cast("Any", self.driver_config))
            else:
                native = self._driver_cls(self.driver_config, broker=self.broker)
            adapter = self.driver_type(native, statement_config=self.statement_config)
            self._driver_instance = cast("DriverT", adapter)
        return self._driver_instance

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncIterator[DriverT]:
        """Yield an asynchronous SQLSpec adapter session."""
        _ = (args, kwargs)
        driver = self.create_driver()
        yield driver

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        """Yield the underlying raw database connection asynchronously."""
        _ = (args, kwargs)
        driver = self.create_driver()
        yield getattr(driver, "connection", driver)

    async def close(self) -> None:
        """Close the cached asynchronous driver if initialized."""
        if self._driver_instance is not None and hasattr(self._driver_instance, "close"):
            res = self._driver_instance.close()
            if hasattr(res, "__await__"):
                await res
            self._driver_instance = None

    def _create_pool(self) -> Any:
        """Return the instantiated driver as pool."""
        return self.create_driver()

    async def _close_pool(self) -> None:
        """Close the cached driver instance asynchronously."""
        await self.close()


class PostgreSQLSyncConfig(SyncAdapterConfig[PostgreSQLConfig, PostgreSQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration for PostgreSQL (postgres+vtb)."""

    dialect: ClassVar[str] = "postgres"
    _config_cls: ClassVar[type[PostgreSQLConfig]] = PostgreSQLConfig
    _driver_cls: ClassVar[type[SyncPostgreSQLDriver]] = SyncPostgreSQLDriver
    driver_type: ClassVar[type[PostgreSQLSyncDriver]] = PostgreSQLSyncDriver

    def derive_direct_config(self) -> Self:
        """Derive a session-mode direct configuration for DDL, migrations, and `LISTEN/NOTIFY`."""
        direct_cfg = self.driver_config.derive_direct_config()
        return type(self)(
            direct_cfg,
            bind_key=f"{self.bind_key}_direct",
            broker=self.broker,
            arrow_threshold_rows=self.arrow_threshold_rows,
            statement_config=self.statement_config,
        )


class PostgreSQLAsyncConfig(AsyncAdapterConfig[PostgreSQLConfig, PostgreSQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for PostgreSQL (postgres+vtb)."""

    dialect: ClassVar[str] = "postgres"
    _config_cls: ClassVar[type[PostgreSQLConfig]] = PostgreSQLConfig
    _driver_cls: ClassVar[type[AsyncPostgreSQLDriver]] = AsyncPostgreSQLDriver
    driver_type: ClassVar[type[PostgreSQLAsyncDriver]] = PostgreSQLAsyncDriver

    def derive_direct_config(self) -> Self:
        """Derive a session-mode direct configuration for DDL, migrations, and `LISTEN/NOTIFY`."""
        direct_cfg = self.driver_config.derive_direct_config()
        return type(self)(
            direct_cfg,
            bind_key=f"{self.bind_key}_direct",
            broker=self.broker,
            arrow_threshold_rows=self.arrow_threshold_rows,
            statement_config=self.statement_config,
        )


class MySQLSyncConfig(SyncAdapterConfig[MySQLConfig, MySQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration for MySQL/MariaDB (mysql+vtb)."""

    dialect: ClassVar[str] = "mysql"
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[SyncMySQLDriver]] = SyncMySQLDriver
    driver_type: ClassVar[type[MySQLSyncDriver]] = MySQLSyncDriver


class MySQLAsyncConfig(AsyncAdapterConfig[MySQLConfig, MySQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for MySQL/MariaDB (mysql+vtb)."""

    dialect: ClassVar[str] = "mysql"
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[AsyncMySQLDriver]] = AsyncMySQLDriver
    driver_type: ClassVar[type[MySQLAsyncDriver]] = MySQLAsyncDriver


class SQLiteSyncConfig(SyncAdapterConfig[SQLiteConfig, SQLiteSyncDriver]):
    """Synchronous SQLSpec adapter configuration for SQLite (sqlite+vtb)."""

    dialect: ClassVar[str] = "sqlite"
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[SyncSQLiteDriver]] = SyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteSyncDriver]] = SQLiteSyncDriver


class SQLiteAsyncConfig(AsyncAdapterConfig[SQLiteConfig, SQLiteAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for SQLite (sqlite+vtb)."""

    dialect: ClassVar[str] = "sqlite"
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[AsyncSQLiteDriver]] = AsyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteAsyncDriver]] = SQLiteAsyncDriver


class OracleSyncConfig(SyncAdapterConfig[OracleConfig, OracleSyncDriver]):
    """Synchronous SQLSpec adapter configuration for Oracle Thin (oracle+vtb)."""

    dialect: ClassVar[str] = "oracle"
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[SyncOracleDriver]] = SyncOracleDriver
    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver


class OracleAsyncConfig(AsyncAdapterConfig[OracleConfig, OracleAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for Oracle Thin (oracle+vtb)."""

    dialect: ClassVar[str] = "oracle"
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[AsyncOracleDriver]] = AsyncOracleDriver
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver


class MSSQLSyncConfig(SyncAdapterConfig[MSSQLConfig, MSSQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration for SQL Server (mssql+vtb)."""

    dialect: ClassVar[str] = "mssql"
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[SyncMSSQLDriver]] = SyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLSyncDriver]] = MSSQLSyncDriver


class MSSQLAsyncConfig(AsyncAdapterConfig[MSSQLConfig, MSSQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for SQL Server (mssql+vtb)."""

    dialect: ClassVar[str] = "mssql"
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[AsyncMSSQLDriver]] = AsyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLAsyncDriver]] = MSSQLAsyncDriver


class DuckDBSyncConfig(SyncAdapterConfig[DuckDBConfig, DuckDBSyncDriver]):
    """Synchronous SQLSpec adapter configuration for DuckDB (duckdb+vtb)."""

    dialect: ClassVar[str] = "duckdb"
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[SyncDuckDBDriver]] = SyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBSyncDriver]] = DuckDBSyncDriver


class DuckDBAsyncConfig(AsyncAdapterConfig[DuckDBConfig, DuckDBAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for DuckDB (duckdb+vtb)."""

    dialect: ClassVar[str] = "duckdb"
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[AsyncDuckDBDriver]] = AsyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBAsyncDriver]] = DuckDBAsyncDriver
