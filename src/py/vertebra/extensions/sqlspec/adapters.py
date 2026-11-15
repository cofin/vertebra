"""First-class SQLSpec adapter configurations and drivers backed by Vertebra Rust-native SHM drivers."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Sequence
from contextlib import asynccontextmanager, contextmanager
from types import TracebackType
from typing import Any, ClassVar, Self, cast

import msgspec

from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.adapters.oracle import AsyncOracleDriver, OracleConfig, SyncOracleDriver
from vertebra.db.adapters.oracle.core import OracleCore
from vertebra.db.adapters.postgres import (
    AsyncPostgreSQLDriver,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.adapters.postgres.core import PostgreSQLCore
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.db.dialer.alloydb import AsyncAlloyDBDialer, SyncAlloyDBDialer
from vertebra.db.dialer.cloudsql import AsyncCloudSQLDialer, SyncCloudSQLDialer
from vertebra.db.driver import NotSupportedError
from vertebra.db.driver.capabilities import AsyncPipelineCapable, SyncPipelineCapable
from vertebra.db.registry import DatabaseRegistry
from vertebra.db.rls import TenantPredicate
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import ZeroCopyCodecRegistry

try:
    from sqlspec import SQLResult, StackResult, StatementStack
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig, TypedParameter
    from sqlspec.core.result._base import ArrowResult
    from sqlspec.core.statement import SQL, StatementConfig
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
except ImportError as exc:
    raise MissingDependencyError("sqlspec", install_package="sqlspec", extra="sqlspec") from exc

__all__ = (
    "AsyncAlloyDBDialer",
    "AsyncCloudSQLDialer",
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
    _execution_parameter_style: ClassVar[ParameterStyle | None] = None
    _arrow_execution_options: ClassVar[tuple[str, ...]] = (
        "return_format",
        "native_only",
        "batch_size",
        "arrow_schema",
    )

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
    ) -> None:
        """Initialize driver with native connection and statement configuration."""
        stmt_cfg = statement_config or self._default_statement_config()
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
            return self._execute_compiled(sql, p if p else None)
        sql = str(statement)
        params = self._normalize_params(parameters)
        return self._execute_compiled(sql, params)

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
        return self._select_compiled(sql, params, schema_type=schema_type)

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
        rows = self.select(
            statement,
            *parameters,
            schema_type=schema_type,
            statement_config=statement_config,
            **kwargs,
        )
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
        rows = self.select(
            statement,
            *parameters,
            schema_type=schema_type,
            statement_config=statement_config,
            **kwargs,
        )
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
        _ = value_type
        row = self.select_one_or_none(
            statement,
            *parameters,
            statement_config=statement_config,
            **kwargs,
        )
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
        return self._execute_many_compiled(sql, parameters)

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
        _ = (statement_config, return_format, native_only, batch_size, arrow_schema, kwargs)
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = self._normalize_params(parameters)
        return self._select_to_arrow_compiled(sql, params, parameters)

    def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute ordered operations through a native pipeline or transactional primitives."""
        if not isinstance(stack, StatementStack):
            raise TypeError("execute_stack expects a StatementStack instance")
        if not stack:
            raise ValueError("Cannot execute an empty StatementStack")
        operations = stack.operations
        if (
            isinstance(self.native_driver, SyncPipelineCapable)
            and not continue_on_error
            and len(operations) > 1
            and all(op.method == "execute" for op in operations)
        ):
            prepared = []
            for op in operations:
                kwargs = dict(op.keyword_arguments or {})
                prepared.append(
                    self._compile_stack_entry(
                        op.statement,
                        op.arguments,
                        statement_config=kwargs.pop("statement_config", None),
                        kwargs=kwargs,
                    )
                )
            with self.native_driver.pipeline() as pipe:
                for statement, sql, params in prepared:
                    if statement.returns_rows():
                        pipe.select(sql, params)
                    else:
                        pipe.execute(sql, params)
                raw_results = pipe.results()
                return tuple(
                    StackResult.from_sql_result(
                        SQLResult(
                            statement=statement,
                            data=result,
                            rows_affected=len(result),
                            operation_type=statement.operation_type,
                        )
                    )
                    if isinstance(result, list)
                    else StackResult(rows_affected=int(result))
                    for (statement, _, _), result in zip(prepared, raw_results, strict=True)
                )

        results: list[StackResult] = []
        if not continue_on_error:
            self.begin()
        try:
            for op in operations:
                if continue_on_error:
                    self.begin()
                try:
                    kwargs = dict(op.keyword_arguments or {})
                    if op.method == "execute":
                        statement, sql, params = self._compile_stack_entry(
                            op.statement,
                            op.arguments,
                            statement_config=kwargs.pop("statement_config", None),
                            kwargs=kwargs,
                        )
                        normalized = params if params is not None else None
                        if statement.returns_rows():
                            rows = self._select_compiled(sql, normalized)
                            result = StackResult.from_sql_result(
                                SQLResult(
                                    statement=statement,
                                    data=rows,
                                    rows_affected=len(rows),
                                    operation_type=statement.operation_type,
                                )
                            )
                        else:
                            result = StackResult(rows_affected=self._execute_compiled(sql, normalized))
                    elif op.method == "execute_many":
                        _statement, sql, parameter_sets = _compile_many_transport(
                            self,
                            op.statement,
                            op.arguments[0],
                            op.arguments[1:],
                            statement_config=kwargs.pop("statement_config", None),
                            kwargs=kwargs,
                        )
                        result = StackResult(rows_affected=self._execute_many_compiled(sql, parameter_sets))
                    elif op.method == "execute_script":
                        script_result = self.execute_script(op.statement, *op.arguments, **kwargs)
                        result = (
                            StackResult.from_sql_result(script_result)
                            if isinstance(script_result, SQLResult)
                            else StackResult(rows_affected=int(script_result))
                        )
                    else:
                        arrow_options = {key: kwargs.pop(key) for key in self._arrow_execution_options if key in kwargs}
                        statement, sql, params = self._compile_stack_entry(
                            op.statement,
                            op.arguments,
                            statement_config=kwargs.pop("statement_config", None),
                            kwargs=kwargs,
                        )
                        if isinstance(self, PostgreSQLSyncDriver):
                            arrow_result = self._select_to_arrow_compiled(
                                sql,
                                params if params else None,
                                prepared=statement,
                            )
                        else:
                            args = (params,) if params is not None else ()
                            arrow_result = self.select_to_arrow(sql, *args, **arrow_options)
                            if isinstance(arrow_result, ArrowCursor):
                                arrow_result = ArrowResult(
                                    statement=statement,
                                    data=arrow_result.to_arrow_table(),
                                    rows_affected=arrow_result.row_count,
                                )
                            else:
                                arrow_result.statement = statement
                        result = StackResult.from_arrow_result(arrow_result)
                    if continue_on_error:
                        self.commit()
                except Exception as exc:
                    if not continue_on_error:
                        raise
                    self.rollback()
                    result = StackResult.from_error(exc)
                results.append(result)
            if not continue_on_error:
                self.commit()
        except BaseException:
            self.rollback()
            raise
        return tuple(results)

    def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Refuse unsupported session-level tenant enforcement."""
        msg = f"Tenant session enforcement is unsupported for {self.dialect}"
        raise NotSupportedError(msg)

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

    def _execute_compiled(self, sql: str, params: Any) -> int:
        """Execute pre-compiled SQL and parameters on the native driver."""
        res = self.native_driver.execute(sql, params) if params is not None else self.native_driver.execute(sql)
        return len(res) if isinstance(res, list) else int(res)

    def _execute_many_compiled(self, sql: str, parameters: Sequence[Any]) -> int:
        """Execute pre-compiled batch SQL and parameter sets on the native driver."""
        exec_many_fn = getattr(self.native_driver, "execute_many", None)
        if callable(exec_many_fn):
            return int(exec_many_fn(sql, list(parameters)))
        return sum(self._execute_compiled(sql, self._normalize_params((item,))) for item in parameters)

    def _select_compiled(
        self,
        sql: str,
        params: Any,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a pre-compiled query and return hydrated or dict rows."""
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

    def _select_to_arrow_compiled(
        self,
        sql: str,
        params: Any,
        fallback_parameters: tuple[Any, ...] = (),
        *,
        prepared: SQL | None = None,
    ) -> ArrowResult:
        """Execute a pre-compiled query and wrap its output as an ArrowResult."""
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
            rows = (
                self._select_compiled(sql, params) if prepared is not None else self.select(sql, *fallback_parameters)
            )
            pa = import_optional("pyarrow")
            table = pa.Table.from_pylist(rows) if pa is not None else rows
        num_rows = len(table) if hasattr(table, "__len__") else getattr(table, "num_rows", 0)
        return ArrowResult(statement=prepared or SQL(sql), data=table, rows_affected=num_rows)

    def _compile_stack_entry(
        self,
        raw_statement: Any,
        arguments: tuple[Any, ...],
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> tuple[SQL, str, Any]:
        """Compile a stack entry once according to the active adapter transport profile."""
        if isinstance(self, PostgreSQLSyncDriver):
            return _compile_postgres_numeric_transport(
                self,
                raw_statement,
                arguments,
                statement_config=statement_config,
                kwargs=kwargs,
            )
        statement = self.prepare_statement(
            raw_statement,
            arguments,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        sql, params = statement.compile()
        return statement, sql, params

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build the owning driver's default statement-processing configuration."""
        return _native_statement_config(cls.dialect, cls._execution_parameter_style)

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
    _execution_parameter_style: ClassVar[ParameterStyle | None] = None
    _arrow_execution_options: ClassVar[tuple[str, ...]] = (
        "return_format",
        "native_only",
        "batch_size",
        "arrow_schema",
    )

    def __init__(
        self,
        connection: Any,
        statement_config: StatementConfig | None = None,
        driver_features: dict[str, Any] | None = None,
        observability: Any = None,
    ) -> None:
        """Initialize driver with native connection and statement configuration."""
        stmt_cfg = statement_config or self._default_statement_config()
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
            return await self._execute_compiled(sql, p if p else None)
        sql = str(statement)
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._execute_compiled(sql, params)

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
        return await self._select_compiled(sql, params, schema_type=schema_type)

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
        rows = await self.select(
            statement,
            *parameters,
            schema_type=schema_type,
            statement_config=statement_config,
            **kwargs,
        )
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
        rows = await self.select(
            statement,
            *parameters,
            schema_type=schema_type,
            statement_config=statement_config,
            **kwargs,
        )
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
        _ = value_type
        row = await self.select_one_or_none(
            statement,
            *parameters,
            statement_config=statement_config,
            **kwargs,
        )
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
        return await self._execute_many_compiled(sql, parameters)

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
        _ = (statement_config, return_format, native_only, batch_size, arrow_schema, kwargs)
        sql = statement.sql if hasattr(statement, "sql") else str(statement)
        params = BaseSyncDriver._normalize_params(parameters)
        return await self._select_to_arrow_compiled(sql, params, parameters)

    async def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute ordered operations through a native pipeline or transactional primitives."""
        if not isinstance(stack, StatementStack):
            raise TypeError("execute_stack expects a StatementStack instance")
        if not stack:
            raise ValueError("Cannot execute an empty StatementStack")
        operations = stack.operations
        if (
            isinstance(self.native_driver, AsyncPipelineCapable)
            and not continue_on_error
            and len(operations) > 1
            and all(op.method == "execute" for op in operations)
        ):
            prepared = []
            for op in operations:
                kwargs = dict(op.keyword_arguments or {})
                prepared.append(
                    self._compile_stack_entry(
                        op.statement,
                        op.arguments,
                        statement_config=kwargs.pop("statement_config", None),
                        kwargs=kwargs,
                    )
                )
            async with self.native_driver.pipeline() as pipe:
                for statement, sql, params in prepared:
                    if statement.returns_rows():
                        pipe.select(sql, params)
                    else:
                        pipe.execute(sql, params)
                raw_results = await pipe.results()
                return tuple(
                    StackResult.from_sql_result(
                        SQLResult(
                            statement=statement,
                            data=result,
                            rows_affected=len(result),
                            operation_type=statement.operation_type,
                        )
                    )
                    if isinstance(result, list)
                    else StackResult(rows_affected=int(result))
                    for (statement, _, _), result in zip(prepared, raw_results, strict=True)
                )

        results: list[StackResult] = []
        if not continue_on_error:
            await self.begin()
        try:
            for op in operations:
                if continue_on_error:
                    await self.begin()
                try:
                    kwargs = dict(op.keyword_arguments or {})
                    if op.method == "execute":
                        statement, sql, params = self._compile_stack_entry(
                            op.statement,
                            op.arguments,
                            statement_config=kwargs.pop("statement_config", None),
                            kwargs=kwargs,
                        )
                        normalized = params if params is not None else None
                        if statement.returns_rows():
                            rows = await self._select_compiled(sql, normalized)
                            result = StackResult.from_sql_result(
                                SQLResult(
                                    statement=statement,
                                    data=rows,
                                    rows_affected=len(rows),
                                    operation_type=statement.operation_type,
                                )
                            )
                        else:
                            result = StackResult(rows_affected=await self._execute_compiled(sql, normalized))
                    elif op.method == "execute_many":
                        _statement, sql, parameter_sets = _compile_many_transport(
                            self,
                            op.statement,
                            op.arguments[0],
                            op.arguments[1:],
                            statement_config=kwargs.pop("statement_config", None),
                            kwargs=kwargs,
                        )
                        result = StackResult(rows_affected=await self._execute_many_compiled(sql, parameter_sets))
                    elif op.method == "execute_script":
                        script_result = await self.execute_script(op.statement, *op.arguments, **kwargs)
                        result = (
                            StackResult.from_sql_result(script_result)
                            if isinstance(script_result, SQLResult)
                            else StackResult(rows_affected=int(script_result))
                        )
                    else:
                        arrow_options = {key: kwargs.pop(key) for key in self._arrow_execution_options if key in kwargs}
                        statement, sql, params = self._compile_stack_entry(
                            op.statement,
                            op.arguments,
                            statement_config=kwargs.pop("statement_config", None),
                            kwargs=kwargs,
                        )
                        if isinstance(self, PostgreSQLAsyncDriver):
                            arrow_result = await self._select_to_arrow_compiled(
                                sql,
                                params if params else None,
                                prepared=statement,
                            )
                        else:
                            args = (params,) if params is not None else ()
                            arrow_result = await self.select_to_arrow(sql, *args, **arrow_options)
                            if isinstance(arrow_result, ArrowCursor):
                                arrow_result = ArrowResult(
                                    statement=statement,
                                    data=arrow_result.to_arrow_table(),
                                    rows_affected=arrow_result.row_count,
                                )
                            else:
                                arrow_result.statement = statement
                        result = StackResult.from_arrow_result(arrow_result)
                    if continue_on_error:
                        await self.commit()
                except Exception as exc:
                    if not continue_on_error:
                        raise
                    await self.rollback()
                    result = StackResult.from_error(exc)
                results.append(result)
            if not continue_on_error:
                await self.commit()
        except BaseException:
            await self.rollback()
            raise
        return tuple(results)

    async def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Refuse unsupported asynchronous session-level tenant enforcement."""
        msg = f"Tenant session enforcement is unsupported for {self.dialect}"
        raise NotSupportedError(msg)

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

    async def _execute_compiled(self, sql: str, params: Any) -> int:
        """Execute pre-compiled SQL and parameters asynchronously on the native driver."""
        res = (
            await self.native_driver.execute(sql, params)
            if params is not None
            else await self.native_driver.execute(sql)
        )
        return len(res) if isinstance(res, list) else int(res)

    async def _execute_many_compiled(self, sql: str, parameters: Sequence[Any]) -> int:
        """Execute pre-compiled batch SQL and parameter sets asynchronously on the native driver."""
        exec_many_fn = getattr(self.native_driver, "execute_many", None)
        if callable(exec_many_fn):
            res = exec_many_fn(sql, list(parameters))
            if hasattr(res, "__await__"):
                res = await res
            return int(res)
        total = 0
        for item in parameters:
            total += await self._execute_compiled(sql, BaseSyncDriver._normalize_params((item,)))
        return total

    async def _select_compiled(
        self,
        sql: str,
        params: Any,
        *,
        schema_type: type[Any] | None = None,
    ) -> list[Any]:
        """Execute a pre-compiled query asynchronously and hydrate rows."""
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

    async def _select_to_arrow_compiled(
        self,
        sql: str,
        params: Any,
        fallback_parameters: tuple[Any, ...] = (),
        *,
        prepared: SQL | None = None,
    ) -> ArrowResult:
        """Execute a pre-compiled query asynchronously and wrap its output as an ArrowResult."""
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
            rows = (
                await self._select_compiled(sql, params)
                if prepared is not None
                else await self.select(sql, *fallback_parameters)
            )
            pa = import_optional("pyarrow")
            table = pa.Table.from_pylist(rows) if pa is not None else rows
        num_rows = len(table) if hasattr(table, "__len__") else getattr(table, "num_rows", 0)
        return ArrowResult(statement=prepared or SQL(sql), data=table, rows_affected=num_rows)

    def _compile_stack_entry(
        self,
        raw_statement: Any,
        arguments: tuple[Any, ...],
        *,
        statement_config: StatementConfig | None = None,
        kwargs: dict[str, Any] | None = None,
    ) -> tuple[SQL, str, Any]:
        """Compile a stack entry once according to the active adapter transport profile."""
        if isinstance(self, PostgreSQLAsyncDriver):
            return _compile_postgres_numeric_transport(
                self,
                raw_statement,
                arguments,
                statement_config=statement_config,
                kwargs=kwargs,
            )
        statement = self.prepare_statement(
            raw_statement,
            arguments,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        sql, params = statement.compile()
        return statement, sql, params

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build the owning driver's default statement-processing configuration."""
        return _native_statement_config(cls.dialect, cls._execution_parameter_style)


class SQLiteSyncDriver(BaseSyncDriver):
    """Synchronous SQLite driver adapter wrapping vertebra.db.adapters.sqlite.SyncSQLiteDriver."""

    dialect: str = "sqlite"


class SQLiteAsyncDriver(BaseAsyncDriver):
    """Asynchronous SQLite driver adapter wrapping vertebra.db.adapters.sqlite.AsyncSQLiteDriver."""

    dialect: str = "sqlite"


class PostgreSQLSyncDriver(BaseSyncDriver):
    """Synchronous PostgreSQL driver adapter wrapping vertebra.db.adapters.postgres.SyncPostgreSQLDriver."""

    dialect: str = "postgres"

    _execution_parameter_style = ParameterStyle.NUMERIC

    def execute(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a PostgreSQL DML/DDL statement using compiled $N numeric binds."""
        _prepared, sql, params = _compile_postgres_numeric_transport(
            self,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return self._execute_compiled(sql, params if params else None)

    def select(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a PostgreSQL query using compiled $N numeric binds."""
        _prepared, sql, params = _compile_postgres_numeric_transport(
            self,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return self._select_compiled(sql, params if params else None, schema_type=schema_type)

    def execute_many(
        self,
        statement: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute a PostgreSQL batch statement using compiled $N numeric binds."""
        _prepared, sql, bound_sets = _compile_many_transport(
            self,
            statement,
            parameters,
            filters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return self._execute_many_compiled(sql, bound_sets)

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
        """Execute a PostgreSQL query to Arrow using compiled $N numeric binds."""
        _ = (return_format, native_only, batch_size, arrow_schema)
        prepared, sql, params = _compile_postgres_numeric_transport(
            self,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return self._select_to_arrow_compiled(sql, params if params else None, prepared=prepared)

    def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Set the owning engine's tenant session value and return its predicate."""
        sql, parameters = _prepare_postgres_tenant_session(self, predicate)
        self._execute_compiled(sql, parameters if parameters else None)
        return PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)


class PostgreSQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous PostgreSQL driver adapter wrapping vertebra.db.adapters.postgres.AsyncPostgreSQLDriver."""

    dialect: str = "postgres"

    _execution_parameter_style = ParameterStyle.NUMERIC

    async def execute(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an async PostgreSQL DML/DDL statement using compiled $N numeric binds."""
        _prepared, sql, params = _compile_postgres_numeric_transport(
            self,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return await self._execute_compiled(sql, params if params else None)

    async def select(
        self,
        statement: Any,
        /,
        *parameters: Any,
        schema_type: type[Any] | None = None,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an async PostgreSQL query using compiled $N numeric binds."""
        _prepared, sql, params = _compile_postgres_numeric_transport(
            self,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return await self._select_compiled(sql, params if params else None, schema_type=schema_type)

    async def execute_many(
        self,
        statement: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: Any | None = None,
        **kwargs: Any,
    ) -> Any:
        """Execute an async PostgreSQL batch statement using compiled $N numeric binds."""
        _prepared, sql, bound_sets = _compile_many_transport(
            self,
            statement,
            parameters,
            filters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return await self._execute_many_compiled(sql, bound_sets)

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
        """Execute an async PostgreSQL query to Arrow using compiled $N numeric binds."""
        _ = (return_format, native_only, batch_size, arrow_schema)
        prepared, sql, params = _compile_postgres_numeric_transport(
            self,
            statement,
            parameters,
            statement_config=statement_config,
            kwargs=kwargs or None,
        )
        return await self._select_to_arrow_compiled(sql, params if params else None, prepared=prepared)

    async def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Set the owning engine's tenant session value and return its predicate."""
        sql, parameters = _prepare_postgres_tenant_session(self, predicate)
        await self._execute_compiled(sql, parameters if parameters else None)
        return PostgreSQLCore.build_tenant_predicate_sql(predicate, param_index=1)


class MySQLSyncDriver(BaseSyncDriver):
    """Synchronous MySQL driver adapter wrapping vertebra.db.adapters.mysql.SyncMySQLDriver."""

    dialect: str = "mysql"

    _execution_parameter_style = ParameterStyle.POSITIONAL_PYFORMAT


class MySQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous MySQL driver adapter wrapping vertebra.db.adapters.mysql.AsyncMySQLDriver."""

    dialect: str = "mysql"

    _execution_parameter_style = ParameterStyle.POSITIONAL_PYFORMAT


class OracleSyncDriver(BaseSyncDriver):
    """Synchronous Oracle Thin driver adapter wrapping vertebra.db.adapters.oracle.SyncOracleDriver."""

    dialect: str = "oracle"

    _execution_parameter_style = ParameterStyle.NAMED_COLON

    def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Set the owning engine's tenant session value and return its predicate."""
        cfg = getattr(self.native_driver, "config", None)
        namespace = str(getattr(cfg, "context_namespace", "USERENV"))
        context_package_proc = getattr(cfg, "context_package_proc", None)
        sql, params = OracleCore.build_session_context_sql(
            str(predicate.tenant_value),
            predicate.config,
            namespace=namespace,
            context_package_proc=context_package_proc,
        )
        self.execute(sql, params)
        return predicate.to_sql_fragment(":tenant_id")


class OracleAsyncDriver(BaseAsyncDriver):
    """Asynchronous Oracle Thin driver adapter wrapping vertebra.db.adapters.oracle.AsyncOracleDriver."""

    dialect: str = "oracle"

    _execution_parameter_style = ParameterStyle.NAMED_COLON

    async def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Set the owning engine's tenant session value and return its predicate."""
        cfg = getattr(self.native_driver, "config", None)
        namespace = str(getattr(cfg, "context_namespace", "USERENV"))
        context_package_proc = getattr(cfg, "context_package_proc", None)
        sql, params = OracleCore.build_session_context_sql(
            str(predicate.tenant_value),
            predicate.config,
            namespace=namespace,
            context_package_proc=context_package_proc,
        )
        await self.execute(sql, params)
        return predicate.to_sql_fragment(":tenant_id")


class MSSQLSyncDriver(BaseSyncDriver):
    """Synchronous MSSQL driver adapter wrapping vertebra.db.adapters.mssql.SyncMSSQLDriver."""

    dialect: str = "mssql"


class MSSQLAsyncDriver(BaseAsyncDriver):
    """Asynchronous MSSQL driver adapter wrapping vertebra.db.adapters.mssql.AsyncMSSQLDriver."""

    dialect: str = "mssql"


class DuckDBSyncDriver(BaseSyncDriver):
    """Synchronous DuckDB driver adapter wrapping vertebra.db.adapters.duckdb.SyncDuckDBDriver."""

    dialect: str = "duckdb"


class DuckDBAsyncDriver(BaseAsyncDriver):
    """Asynchronous DuckDB driver adapter wrapping vertebra.db.adapters.duckdb.AsyncDuckDBDriver."""

    dialect: str = "duckdb"


class SyncAdapterConfig[ConfigT, DriverT: SyncDriverAdapterBase](SyncDatabaseConfig[Any, Any, DriverT]):
    """Base synchronous SQLSpec adapter configuration backed by a Vertebra native driver.

    Concrete subclasses declare ``connection_type`` until full
    ``DatabaseConfigProtocol`` parity (https://github.com/cofin/vertebra/issues/131)
    replaces these shim base classes.
    """

    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = False
    connection_type: ClassVar[type[Any]]
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
            statement_config=statement_config or self.driver_type._default_statement_config(),
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
    """Base asynchronous SQLSpec adapter configuration backed by a Vertebra native driver.

    Concrete subclasses declare ``connection_type`` until full
    ``DatabaseConfigProtocol`` parity (https://github.com/cofin/vertebra/issues/131)
    replaces these shim base classes.
    """

    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = True
    connection_type: ClassVar[type[Any]]
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
            statement_config=statement_config or self.driver_type._default_statement_config(),
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
    connection_type: ClassVar[type[Any]] = SyncPostgreSQLDriver
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
    connection_type: ClassVar[type[Any]] = AsyncPostgreSQLDriver
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
    connection_type: ClassVar[type[Any]] = SyncMySQLDriver
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[SyncMySQLDriver]] = SyncMySQLDriver
    driver_type: ClassVar[type[MySQLSyncDriver]] = MySQLSyncDriver


class MySQLAsyncConfig(AsyncAdapterConfig[MySQLConfig, MySQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for MySQL/MariaDB (mysql+vtb)."""

    dialect: ClassVar[str] = "mysql"
    connection_type: ClassVar[type[Any]] = AsyncMySQLDriver
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[AsyncMySQLDriver]] = AsyncMySQLDriver
    driver_type: ClassVar[type[MySQLAsyncDriver]] = MySQLAsyncDriver


class SQLiteSyncConfig(SyncAdapterConfig[SQLiteConfig, SQLiteSyncDriver]):
    """Synchronous SQLSpec adapter configuration for SQLite (sqlite+vtb)."""

    dialect: ClassVar[str] = "sqlite"
    connection_type: ClassVar[type[Any]] = SyncSQLiteDriver
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[SyncSQLiteDriver]] = SyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteSyncDriver]] = SQLiteSyncDriver


class SQLiteAsyncConfig(AsyncAdapterConfig[SQLiteConfig, SQLiteAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for SQLite (sqlite+vtb)."""

    dialect: ClassVar[str] = "sqlite"
    connection_type: ClassVar[type[Any]] = AsyncSQLiteDriver
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[AsyncSQLiteDriver]] = AsyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteAsyncDriver]] = SQLiteAsyncDriver


class OracleSyncConfig(SyncAdapterConfig[OracleConfig, OracleSyncDriver]):
    """Synchronous SQLSpec adapter configuration for Oracle Thin (oracle+vtb)."""

    dialect: ClassVar[str] = "oracle"
    connection_type: ClassVar[type[Any]] = SyncOracleDriver
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[SyncOracleDriver]] = SyncOracleDriver
    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver


class OracleAsyncConfig(AsyncAdapterConfig[OracleConfig, OracleAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for Oracle Thin (oracle+vtb)."""

    dialect: ClassVar[str] = "oracle"
    connection_type: ClassVar[type[Any]] = AsyncOracleDriver
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[AsyncOracleDriver]] = AsyncOracleDriver
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver


class MSSQLSyncConfig(SyncAdapterConfig[MSSQLConfig, MSSQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration for SQL Server (mssql+vtb)."""

    dialect: ClassVar[str] = "mssql"
    connection_type: ClassVar[type[Any]] = SyncMSSQLDriver
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[SyncMSSQLDriver]] = SyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLSyncDriver]] = MSSQLSyncDriver


class MSSQLAsyncConfig(AsyncAdapterConfig[MSSQLConfig, MSSQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for SQL Server (mssql+vtb)."""

    dialect: ClassVar[str] = "mssql"
    connection_type: ClassVar[type[Any]] = AsyncMSSQLDriver
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[AsyncMSSQLDriver]] = AsyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLAsyncDriver]] = MSSQLAsyncDriver


class DuckDBSyncConfig(SyncAdapterConfig[DuckDBConfig, DuckDBSyncDriver]):
    """Synchronous SQLSpec adapter configuration for DuckDB (duckdb+vtb)."""

    dialect: ClassVar[str] = "duckdb"
    connection_type: ClassVar[type[Any]] = SyncDuckDBDriver
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[SyncDuckDBDriver]] = SyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBSyncDriver]] = DuckDBSyncDriver


class DuckDBAsyncConfig(AsyncAdapterConfig[DuckDBConfig, DuckDBAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration for DuckDB (duckdb+vtb)."""

    dialect: ClassVar[str] = "duckdb"
    connection_type: ClassVar[type[Any]] = AsyncDuckDBDriver
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[AsyncDuckDBDriver]] = AsyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBAsyncDriver]] = DuckDBAsyncDriver


def _unwrap_typed_parameters(bound: Any) -> Any:
    """Unwrap TypedParameter instances recursively while preserving dict, sequence, or scalar shapes."""
    if isinstance(bound, dict):
        return {key: _unwrap_typed_parameters(value) for key, value in bound.items()}
    if isinstance(bound, tuple):
        return tuple(_unwrap_typed_parameters(value) for value in bound)
    if isinstance(bound, list):
        return [_unwrap_typed_parameters(value) for value in bound]
    if isinstance(bound, TypedParameter):
        return bound.value
    return bound


def _compile_postgres_numeric_transport(
    driver: BaseSyncDriver | BaseAsyncDriver,
    statement: Any,
    parameters: tuple[Any, ...] = (),
    *,
    statement_config: StatementConfig | None = None,
    kwargs: dict[str, Any] | None = None,
) -> tuple[SQL, str, Any]:
    """Compile a PostgreSQL statement once to $N binds preserving transformers and filters."""
    effective_config = statement_config or driver.statement_config
    prepared = driver.prepare_statement(
        statement,
        parameters,
        statement_config=effective_config,
        kwargs=kwargs or None,
    )
    sql, bound = prepared.compile()
    bound = driver.prepare_driver_parameters(bound, prepared.statement_config, prepared_statement=prepared)
    return prepared, sql, _unwrap_typed_parameters(bound)


def _prepare_many_statement(
    driver: SyncDriverAdapterBase | AsyncDriverAdapterBase,
    statement: Any,
    parameters: Sequence[Any],
    filters: tuple[Any, ...] = (),
    *,
    statement_config: StatementConfig | None = None,
    kwargs: dict[str, Any] | None = None,
) -> SQL:
    """Prepare batched binds through SQLSpec's execute-many statement contract."""
    extra_kwargs = dict(kwargs) if kwargs else {}
    config = statement_config or extra_kwargs.pop("statement_config", None) or driver.statement_config
    if isinstance(statement, str) and not filters and not extra_kwargs:
        return SQL(statement, parameters, statement_config=config, is_many=True)
    if isinstance(statement, SQL) and not filters:
        seed = statement.raw_expression or statement.raw_sql
    else:
        prepared = driver.prepare_statement(
            statement,
            filters,
            statement_config=config,
            kwargs=extra_kwargs or None,
        )
        seed = prepared.raw_expression or prepared.raw_sql
    return SQL(seed, parameters, statement_config=config, is_many=True, **extra_kwargs)


def _compile_many_transport(
    driver: SyncDriverAdapterBase | AsyncDriverAdapterBase,
    statement: Any,
    parameters: Sequence[Any],
    filters: tuple[Any, ...] = (),
    *,
    statement_config: StatementConfig | None = None,
    kwargs: dict[str, Any] | None = None,
) -> tuple[SQL, str, list[Any]]:
    """Compile a batched statement and unwrap TypedParameter values across every parameter set."""
    prepared = _prepare_many_statement(
        driver,
        statement,
        parameters,
        filters,
        statement_config=statement_config,
        kwargs=kwargs,
    )
    sql, parameter_sets = prepared.compile()
    bound_sets = driver.prepare_driver_parameters(
        parameter_sets,
        prepared.statement_config,
        is_many=True,
        prepared_statement=prepared,
    )
    return prepared, sql, list(_unwrap_typed_parameters(bound_sets))


def _prepare_postgres_tenant_session(
    driver: BaseSyncDriver | BaseAsyncDriver, predicate: TenantPredicate
) -> tuple[str, Any]:
    """Compile session SQL through its transport profile and unwrap typed bind values."""
    sql, parameters = PostgreSQLCore.build_session_guc_sql(str(predicate.tenant_value), predicate.config)
    _prepared, compiled_sql, bound = _compile_postgres_numeric_transport(driver, sql, (parameters,))
    if isinstance(bound, dict):
        return compiled_sql, bound
    return compiled_sql, tuple(cast("Sequence[Any]", bound))


def _native_statement_config(dialect: str, execution_style: ParameterStyle | None) -> StatementConfig:
    """Prepare native SQL with the owning driver's execution parameter format."""
    styles = tuple(ParameterStyle)
    supported_input_styles = (
        {ParameterStyle.QMARK, execution_style}
        if execution_style == ParameterStyle.NAMED_COLON
        else ({ParameterStyle.QMARK} if execution_style is not None else styles)
    )
    return StatementConfig(
        dialect=dialect,
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles=supported_input_styles,
            default_execution_parameter_style=execution_style,
            supported_execution_parameter_styles={execution_style} if execution_style is not None else styles,
            preserve_parameter_format=True,
        ),
    )
