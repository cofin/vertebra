"""Shared SQLSpec driver adapters for the broker-backed transport."""

import contextlib
from typing import TYPE_CHECKING, Any

from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    ExecutionResult,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
from sqlspec.exceptions import DatabaseError, ImproperConfigurationError, SQLSpecError
from sqlspec.utils.correlation import CorrelationContext

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.core import SQL

    from ._shared import AsyncBrokerConnection, SyncBrokerConnection


class BrokerExceptionHandler:
    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> "BrokerExceptionHandler":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, SQLSpecError):
            self.pending_exception = exc_val
            return True
        self.pending_exception = DatabaseError(str(exc_val))
        return True


class BrokerAsyncExceptionHandler:
    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> "BrokerAsyncExceptionHandler":
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, SQLSpecError):
            self.pending_exception = exc_val
            return True
        self.pending_exception = DatabaseError(str(exc_val))
        return True


def _collect_rows(result: "dict[str, Any]") -> "tuple[list[dict[str, Any]], list[str], int]":
    if "arrow" in result:
        table = result.get("arrow")
        if table is not None:
            data = table.to_pylist()
            columns = list(table.schema.names)
            return data, columns, int(table.num_rows)
    columns = list(result.get("columns") or [])
    rows = result.get("rows") or []
    data = [dict(zip(columns, row, strict=False)) for row in rows]
    rowcount = int(result.get("rowcount") or len(rows))
    return data, columns, rowcount


def _correlation_id() -> str | None:
    try:
        return CorrelationContext.get()
    except Exception:
        return None


class BrokerSyncDriverBase(SyncDriverAdapterBase):
    __slots__ = ("_data_dictionary", "_data_dictionary_factory")

    data_dictionary_factory: "Callable[[], SyncDataDictionaryBase]"

    def __init__(
        self,
        connection: "SyncBrokerConnection",
        statement_config: "Any" = None,
        driver_features: "dict[str, Any] | None" = None,
        *,
        data_dictionary_factory: "Callable[[], SyncDataDictionaryBase] | None" = None,
    ) -> None:
        features = driver_features or {}
        factory = data_dictionary_factory or features.get("data_dictionary_factory")
        if factory is None:
            msg = "data_dictionary_factory is required for broker-backed SQLSpec drivers"
            raise ImproperConfigurationError(msg)
        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)
        self._data_dictionary_factory = factory
        self._data_dictionary: "SyncDataDictionaryBase | None" = None

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        if self._data_dictionary is None:
            self._data_dictionary = self._data_dictionary_factory()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "SyncBrokerConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = cursor._execute(sql, prepared_parameters, correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(result)
            return self.create_execution_result(
                result, selected_data=data, column_names=columns, data_row_count=rowcount, is_select_result=True
            )
        return self.create_execution_result(result, rowcount_override=int(result.get("rowcount") or 0))

    def dispatch_execute_many(self, cursor: "SyncBrokerConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = cursor._execute_many(sql, prepared_parameters, correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(result)
            return self.create_execution_result(
                result,
                selected_data=data,
                column_names=columns,
                data_row_count=rowcount,
                is_select_result=True,
                is_many_result=True,
            )
        return self.create_execution_result(
            result, rowcount_override=int(result.get("rowcount") or 0), is_many_result=True
        )

    def begin(self) -> None:
        self.connection.begin()

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def with_cursor(self, connection: "Any") -> "Any":
        return contextlib.nullcontext(connection)

    def handle_database_exceptions(self) -> "BrokerExceptionHandler":
        return BrokerExceptionHandler()

    def _connection_in_transaction(self) -> bool:
        return False


class BrokerAsyncDriverBase(AsyncDriverAdapterBase):
    __slots__ = ("_data_dictionary", "_data_dictionary_factory")

    data_dictionary_factory: "Callable[[], AsyncDataDictionaryBase]"

    def __init__(
        self,
        connection: "AsyncBrokerConnection",
        statement_config: "Any" = None,
        driver_features: "dict[str, Any] | None" = None,
        *,
        data_dictionary_factory: "Callable[[], AsyncDataDictionaryBase] | None" = None,
    ) -> None:
        features = driver_features or {}
        factory = data_dictionary_factory or features.get("data_dictionary_factory")
        if factory is None:
            msg = "data_dictionary_factory is required for broker-backed SQLSpec drivers"
            raise ImproperConfigurationError(msg)
        super().__init__(connection=connection, statement_config=statement_config, driver_features=features)
        self._data_dictionary_factory = factory
        self._data_dictionary: "AsyncDataDictionaryBase | None" = None

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        if self._data_dictionary is None:
            self._data_dictionary = self._data_dictionary_factory()
        return self._data_dictionary

    async def dispatch_execute(self, cursor: "AsyncBrokerConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = await cursor._execute(sql, prepared_parameters, correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(result)
            return self.create_execution_result(
                result, selected_data=data, column_names=columns, data_row_count=rowcount, is_select_result=True
            )
        return self.create_execution_result(result, rowcount_override=int(result.get("rowcount") or 0))

    async def dispatch_execute_many(self, cursor: "AsyncBrokerConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = await cursor._execute_many(sql, prepared_parameters, correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(result)
            return self.create_execution_result(
                result,
                selected_data=data,
                column_names=columns,
                data_row_count=rowcount,
                is_select_result=True,
                is_many_result=True,
            )
        return self.create_execution_result(
            result, rowcount_override=int(result.get("rowcount") or 0), is_many_result=True
        )

    async def begin(self) -> None:
        await self.connection.begin()

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()

    def with_cursor(self, connection: "Any") -> "Any":
        @contextlib.asynccontextmanager
        async def _context() -> "Any":
            yield connection

        return _context()

    def handle_database_exceptions(self) -> "BrokerAsyncExceptionHandler":
        return BrokerAsyncExceptionHandler()

    def _connection_in_transaction(self) -> bool:
        return False


__all__ = ("BrokerAsyncDriverBase", "BrokerAsyncExceptionHandler", "BrokerExceptionHandler", "BrokerSyncDriverBase")
