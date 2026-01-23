"""Shared SQLSpec driver adapters for the broker-backed transport."""

import contextlib
import uuid
from types import TracebackType
from typing import TYPE_CHECKING, Any, Self, cast

from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    ExecutionResult,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
from sqlspec.exceptions import ImproperConfigurationError, OperationalError, SQLSpecError
from sqlspec.utils.correlation import CorrelationContext

from vertebra._vertebra import ShmRegion
from vertebra.typing import PYARROW_INSTALLED

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Callable, Iterable

    from sqlspec.core import SQL, StatementConfig

    from ._shared import VertebraAsyncConnection, VertebraSyncConnection

VertebraResult = dict[str, Any]


class VertebraExceptionHandler:
    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, SQLSpecError):
            self.pending_exception = exc_val
            return True
        self.pending_exception = OperationalError(str(exc_val))
        return True


class VertebraAsyncExceptionHandler:
    __slots__ = ("pending_exception",)

    def __init__(self) -> None:
        self.pending_exception: Exception | None = None

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None
    ) -> bool:
        if exc_type is None:
            return False
        if isinstance(exc_val, SQLSpecError):
            self.pending_exception = exc_val
            return True
        self.pending_exception = OperationalError(str(exc_val))
        return True


def _collect_rows(result: "VertebraResult") -> "tuple[list[dict[str, Any]], list[str], int]":
    if "arrow" in result:
        table = cast("Any", result.get("arrow"))
        if table is not None:
            data = cast("list[dict[str, Any]]", table.to_pylist())
            columns = list(cast("Any", table.schema).names)
            return data, columns, int(cast("Any", table).num_rows)
    columns = list(cast("Iterable[str]", result.get("columns") or []))
    rows = cast("Iterable[Iterable[Any]]", result.get("rows") or [])
    data = [dict(zip(columns, row, strict=False)) for row in rows]
    rowcount = int(cast("Any", result.get("rowcount") or len(data)))
    return data, columns, rowcount


def _correlation_id() -> str | None:
    try:
        return CorrelationContext.get()
    except LookupError:
        return None


class VertebraSyncDriverBase(SyncDriverAdapterBase):
    __slots__ = (
        "_arrow_regions",
        "_arrow_threshold_bytes",
        "_arrow_threshold_rows",
        "_data_dictionary",
        "_data_dictionary_factory",
    )

    data_dictionary_factory: "Callable[[], SyncDataDictionaryBase]"

    def __init__(
        self,
        connection: "VertebraSyncConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
        *,
        data_dictionary_factory: "Callable[[], SyncDataDictionaryBase] | None" = None,
    ) -> None:
        features = driver_features or {}
        factory = data_dictionary_factory or features.get("data_dictionary_factory")
        if factory is None:
            msg = "data_dictionary_factory is required for broker-backed SQLSpec drivers"
            raise ImproperConfigurationError(msg)
        super().__init__(
            connection=connection,
            statement_config=cast("Any", statement_config),  # Base class handles None but typed as required
            driver_features=features,
        )
        self._data_dictionary_factory = cast("Callable[[], SyncDataDictionaryBase]", factory)
        self._data_dictionary: "SyncDataDictionaryBase | None" = None
        self._arrow_regions: dict[str, ShmRegion] = {}
        self._arrow_threshold_rows = int(cast("int", features.get("arrow_threshold_rows", 1000)))
        self._arrow_threshold_bytes = int(cast("int", features.get("arrow_threshold_bytes", 1_048_576)))

    def _should_use_arrow(self, rows: "list[list[Any]]", columns: "list[str]") -> bool:
        if not PYARROW_INSTALLED:
            return False
        rowcount = len(rows)
        estimated_bytes = rowcount * max(1, len(columns)) * 8
        return rowcount >= self._arrow_threshold_rows or estimated_bytes >= self._arrow_threshold_bytes

    def _encode_arrow(self, rows: "list[list[Any]]", columns: "list[str]") -> "dict[str, Any]":
        import pyarrow as pa  # pyright: ignore[reportMissingImports]

        records = [dict(zip(columns, row, strict=False)) for row in rows]
        table = pa.Table.from_pylist(records)
        return self._write_arrow_region(table)

    def _write_arrow_region(self, table: "Any") -> "dict[str, Any]":
        import pyarrow as pa  # pyright: ignore[reportMissingImports]

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        buffer = sink.getvalue().to_pybytes()
        name = f"/vertebra.client.arrow.{uuid.uuid4().hex}"
        region = ShmRegion.create_named(name, len(buffer))
        memoryview(region)[: len(buffer)] = buffer
        self._arrow_regions[name] = region
        return {"arrow": {"name": name, "size": len(buffer)}}

    @property
    def data_dictionary(self) -> "SyncDataDictionaryBase":
        if self._data_dictionary is None:
            self._data_dictionary = self._data_dictionary_factory()
        return self._data_dictionary

    def dispatch_execute(self, cursor: "VertebraSyncConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = cursor._execute(sql, cast("Any", prepared_parameters), correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(cast("VertebraResult", result))
            return self.create_execution_result(
                result, selected_data=data, column_names=columns, data_row_count=rowcount, is_select_result=True
            )
        return self.create_execution_result(result, rowcount_override=int(cast("Any", result).get("rowcount") or 0))

    def dispatch_execute_many(self, cursor: "VertebraSyncConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = cursor._execute_many(sql, cast("Any", prepared_parameters), correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(cast("VertebraResult", result))
            return self.create_execution_result(
                result,
                selected_data=data,
                column_names=columns,
                data_row_count=rowcount,
                is_select_result=True,
                is_many_result=True,
            )
        return self.create_execution_result(
            result, rowcount_override=int(cast("Any", result).get("rowcount") or 0), is_many_result=True
        )

    def begin(self) -> None:
        self.connection.begin()

    def commit(self) -> None:
        self.connection.commit()

    def rollback(self) -> None:
        self.connection.rollback()

    def with_cursor(
        self, connection: "VertebraSyncConnection"
    ) -> "contextlib.AbstractContextManager[VertebraSyncConnection]":
        return contextlib.nullcontext(connection)

    def handle_database_exceptions(self) -> "VertebraExceptionHandler":
        return VertebraExceptionHandler()

    def _connection_in_transaction(self) -> bool:
        return False


class VertebraAsyncDriverBase(AsyncDriverAdapterBase):
    __slots__ = (
        "_arrow_regions",
        "_arrow_threshold_bytes",
        "_arrow_threshold_rows",
        "_data_dictionary",
        "_data_dictionary_factory",
    )

    data_dictionary_factory: "Callable[[], AsyncDataDictionaryBase]"

    def __init__(
        self,
        connection: "VertebraAsyncConnection",
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
        *,
        data_dictionary_factory: "Callable[[], AsyncDataDictionaryBase] | None" = None,
    ) -> None:
        features = driver_features or {}
        factory = data_dictionary_factory or features.get("data_dictionary_factory")
        if factory is None:
            msg = "data_dictionary_factory is required for broker-backed SQLSpec drivers"
            raise ImproperConfigurationError(msg)
        super().__init__(
            connection=connection, statement_config=cast("Any", statement_config), driver_features=features
        )
        self._data_dictionary_factory = cast("Callable[[], AsyncDataDictionaryBase]", factory)
        self._data_dictionary: "AsyncDataDictionaryBase | None" = None
        self._arrow_regions: dict[str, ShmRegion] = {}
        self._arrow_threshold_rows = int(cast("int", features.get("arrow_threshold_rows", 1000)))
        self._arrow_threshold_bytes = int(cast("int", features.get("arrow_threshold_bytes", 1_048_576)))

    def _should_use_arrow(self, rows: "list[list[Any]]", columns: "list[str]") -> bool:
        if not PYARROW_INSTALLED:
            return False
        rowcount = len(rows)
        estimated_bytes = rowcount * max(1, len(columns)) * 8
        return rowcount >= self._arrow_threshold_rows or estimated_bytes >= self._arrow_threshold_bytes

    def _encode_arrow(self, rows: "list[list[Any]]", columns: "list[str]") -> "dict[str, Any]":
        import pyarrow as pa  # pyright: ignore[reportMissingImports]

        records = [dict(zip(columns, row, strict=False)) for row in rows]
        table = pa.Table.from_pylist(records)
        return self._write_arrow_region(table)

    def _write_arrow_region(self, table: "Any") -> "dict[str, Any]":
        import pyarrow as pa  # pyright: ignore[reportMissingImports]

        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        buffer = sink.getvalue().to_pybytes()
        name = f"/vertebra.client.arrow.{uuid.uuid4().hex}"
        region = ShmRegion.create_named(name, len(buffer))
        memoryview(region)[: len(buffer)] = buffer
        self._arrow_regions[name] = region
        return {"arrow": {"name": name, "size": len(buffer)}}

    @property
    def data_dictionary(self) -> "AsyncDataDictionaryBase":
        if self._data_dictionary is None:
            self._data_dictionary = self._data_dictionary_factory()
        return self._data_dictionary

    async def dispatch_execute(self, cursor: "VertebraAsyncConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = await cursor._execute(sql, cast("Any", prepared_parameters), correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(cast("VertebraResult", result))
            return self.create_execution_result(
                result, selected_data=data, column_names=columns, data_row_count=rowcount, is_select_result=True
            )
        return self.create_execution_result(result, rowcount_override=int(cast("Any", result).get("rowcount") or 0))

    async def dispatch_execute_many(self, cursor: "VertebraAsyncConnection", statement: "SQL") -> ExecutionResult:
        sql, prepared_parameters = self._get_compiled_sql(statement, self.statement_config)
        correlation_id = _correlation_id()
        result = await cursor._execute_many(sql, cast("Any", prepared_parameters), correlation_id)
        if statement.returns_rows():
            data, columns, rowcount = _collect_rows(cast("VertebraResult", result))
            return self.create_execution_result(
                result,
                selected_data=data,
                column_names=columns,
                data_row_count=rowcount,
                is_select_result=True,
                is_many_result=True,
            )
        return self.create_execution_result(
            result, rowcount_override=int(cast("Any", result).get("rowcount") or 0), is_many_result=True
        )

    async def begin(self) -> None:
        await self.connection.begin()

    async def commit(self) -> None:
        await self.connection.commit()

    async def rollback(self) -> None:
        await self.connection.rollback()

    def with_cursor(
        self, connection: "VertebraAsyncConnection"
    ) -> "contextlib.AbstractAsyncContextManager[VertebraAsyncConnection]":
        @contextlib.asynccontextmanager
        async def _context() -> "AsyncGenerator[VertebraAsyncConnection, None]":
            yield connection

        return cast("contextlib.AbstractAsyncContextManager[VertebraAsyncConnection]", _context())

    def handle_database_exceptions(self) -> "VertebraAsyncExceptionHandler":
        return VertebraAsyncExceptionHandler()

    def _connection_in_transaction(self) -> bool:
        return False


__all__ = (
    "VertebraAsyncDriverBase",
    "VertebraAsyncExceptionHandler",
    "VertebraExceptionHandler",
    "VertebraSyncDriverBase",
)
