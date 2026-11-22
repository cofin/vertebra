"""Template-method SQLSpec dispatch bases and shared configuration wrappers over Vertebra drivers."""

from __future__ import annotations

import contextlib
import inspect
import threading
from collections.abc import AsyncIterator, Awaitable, Callable, Collection, Iterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, ClassVar, Literal, Self, cast

import msgspec
from anyio import CancelScope

from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    ConnectionFailure,
    DatabaseError,
    GoogleCloudAuthError,
    GoogleCloudError,
    InterfaceError,
    InternalError,
    NotSupportedError,
    PoolExhaustedError,
    ProgrammingError,
    QueryCancelledError,
    SerializationFailure,
    SpannerAbortedError,
    SpannerError,
)
from vertebra._vertebra import DataError as NativeDataError
from vertebra._vertebra import IntegrityError as NativeIntegrityError
from vertebra._vertebra import NotFoundError as NativeNotFoundError
from vertebra._vertebra import OperationalError as NativeOperationalError
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver.capabilities import AsyncPipelineCapable, SyncPipelineCapable
from vertebra.db.registry import DatabaseRegistry
from vertebra.db.rls import TenantPredicate
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import ensure_module, import_optional
from vertebra.utils.serializers import ZeroCopyCodecRegistry

try:
    from sqlspec import SQLResult, StackResult, StatementStack
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig
    from sqlspec.core.cache import get_cache_config
    from sqlspec.core.result import ArrowResult, DMLResult, create_arrow_result
    from sqlspec.driver import (
        AsyncDriverAdapterBase,
        AsyncRowStream,
        BaseAsyncExceptionHandler,
        BaseSyncExceptionHandler,
        ExecutionResult,
        SyncDriverAdapterBase,
        SyncRowStream,
        validate_savepoint_name,
    )
    from sqlspec.exceptions import (
        CheckViolationError,
        ConnectionTimeoutError,
        DatabaseConnectionError,
        DataError,
        DeadlockError,
        DialectNotSupportedError,
        ForeignKeyViolationError,
        ImproperConfigurationError,
        IntegrityError,
        NotNullViolationError,
        OperationalError,
        OperationCancelledError,
        PermissionDeniedError,
        QueryTimeoutError,
        SerializationConflictError,
        SQLParsingError,
        SQLSpecError,
        UniqueViolationError,
    )
except ImportError as exc:
    raise MissingDependencyError("sqlspec", install_package="sqlspec", extra="sqlspec") from exc

if TYPE_CHECKING:
    from types import TracebackType
    from uuid import UUID

    from sqlspec.core import StatementFilter
    from sqlspec.core.cache import CachedQuery
    from sqlspec.driver import AsyncRoutingSession, SyncRoutingSession
    from sqlspec.storage import StorageBridgeJob, StorageDestination, StorageFormat, StorageTelemetry
    from sqlspec.storage.pipeline import AsyncStoragePipeline, SyncStoragePipeline
    from sqlspec.typing import StatementParameters

__all__ = (
    "AsyncAdapterConfig",
    "BaseAsyncDriver",
    "BaseSyncDriver",
    "LazyStoreAttr",
    "SyncAdapterConfig",
)

type ExceptionMapper = Callable[[BaseException], Exception | None]
type _CacheHitState = tuple[Callable[..., Any], Callable[..., Any], ExceptionMapper]


class LazyStoreAttr:
    """Descriptor lazily resolving adapter store classes on attribute access without eager imports."""

    __slots__ = ("_attr", "_module")

    def __init__(self, module: str, attr: str) -> None:
        self._module = module
        self._attr = attr

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> type[Any]:
        _ = (instance, owner)
        return cast("type[Any]", getattr(ensure_module(self._module), self._attr))


class _DeferredSyncExceptionHandler(BaseSyncExceptionHandler):
    """Synchronous exception handler deferring mapped SQLSpec exceptions to caller dispatch flow."""

    __slots__ = ("_custom_mapper",)

    def __init__(self, custom_mapper: ExceptionMapper | None = None) -> None:
        super().__init__()
        self._custom_mapper = custom_mapper

    def _handle_exception(self, exc_type: type[BaseException] | None, exc_val: BaseException) -> bool:
        _ = exc_type
        mapped: Exception | None = None
        if self._custom_mapper is not None:
            mapped = self._custom_mapper(exc_val)
        if mapped is None:
            mapped = BaseSyncDriver._map_native_database_exception(exc_val)
        if mapped is None:
            return False
        self.pending_exception = mapped
        return True


class _DeferredAsyncExceptionHandler(BaseAsyncExceptionHandler):
    """Asynchronous exception handler deferring mapped SQLSpec exceptions to caller dispatch flow."""

    __slots__ = ("_custom_mapper",)

    def __init__(self, custom_mapper: ExceptionMapper | None = None) -> None:
        super().__init__()
        self._custom_mapper = custom_mapper

    def _handle_exception(self, exc_type: type[BaseException] | None, exc_val: BaseException) -> bool:
        _ = exc_type
        mapped: Exception | None = None
        if self._custom_mapper is not None:
            mapped = self._custom_mapper(exc_val)
        if mapped is None:
            mapped = BaseSyncDriver._map_native_database_exception(exc_val)
        if mapped is None:
            return False
        self.pending_exception = mapped
        return True


class _VertebraSyncRowSource:
    """Chunked row source consuming batches from a synchronous Vertebra driver."""

    __slots__ = ("_chunk_size", "_driver", "_iterator", "_params", "_sql")

    def __init__(self, driver: Any, sql: str, params: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._params = params
        self._chunk_size = chunk_size
        self._iterator: Iterator[Any] | None = None

    def start(self) -> None:
        stream_fn = self._driver.stream_arrow_batches
        self._iterator = iter(stream_fn(self._sql, self._params, batch_size=self._chunk_size))

    def fetch_chunk(self) -> list[dict[str, Any]]:
        if self._iterator is None:
            return []
        try:
            batch = next(self._iterator)
        except StopIteration:
            return []
        if hasattr(batch, "to_pylist"):
            return list(batch.to_pylist())
        if isinstance(batch, list):
            return batch
        return list(batch)

    def close(self, error: bool = False) -> None:
        _ = error
        close_fn = getattr(self._iterator, "close", None)
        if callable(close_fn):
            close_fn()
        self._iterator = None


class _VertebraAsyncRowSource:
    """Chunked row source consuming batches from an asynchronous Vertebra driver."""

    __slots__ = ("_chunk_size", "_driver", "_iterator", "_params", "_sql")

    def __init__(self, driver: Any, sql: str, params: Any, chunk_size: int) -> None:
        self._driver = driver
        self._sql = sql
        self._params = params
        self._chunk_size = chunk_size
        self._iterator: AsyncIterator[Any] | None = None

    async def start(self) -> None:
        stream_fn = self._driver.stream_arrow_batches
        self._iterator = stream_fn(self._sql, self._params, batch_size=self._chunk_size).__aiter__()

    async def fetch_chunk(self) -> list[dict[str, Any]]:
        if self._iterator is None:
            return []
        try:
            batch = await self._iterator.__anext__()
        except StopAsyncIteration:
            return []
        if hasattr(batch, "to_pylist"):
            return list(batch.to_pylist())
        if isinstance(batch, list):
            return batch
        return list(batch)

    async def close(self, error: bool = False) -> None:
        _ = error
        aclose_fn = getattr(self._iterator, "aclose", None)
        if callable(aclose_fn):
            res = aclose_fn()
            if isinstance(res, Awaitable):
                await res
        self._iterator = None


class _NativeSyncCursorContext(AbstractContextManager[Any]):
    """Context manager opening one native cursor on a driver and closing it on exit."""

    __slots__ = ("_cursor", "_driver")

    def __init__(self, driver: Any) -> None:
        self._driver = driver
        self._cursor: Any = None

    def __enter__(self) -> Any:
        self._cursor = self._driver.cursor()
        return self._cursor

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        cursor = self._cursor
        if cursor is not None:
            self._cursor = None
            cursor.close()


class _NativeAsyncCursorContext:
    """Async context manager opening one native async cursor on a driver and closing it on exit."""

    __slots__ = ("_cursor", "_driver")

    def __init__(self, driver: Any) -> None:
        self._driver = driver
        self._cursor: Any = None

    async def __aenter__(self) -> Any:
        self._cursor = self._driver.cursor()
        return self._cursor

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        cursor = self._cursor
        if cursor is not None:
            self._cursor = None
            await cursor.close()


class BaseSyncDriver(SyncDriverAdapterBase):
    """Base synchronous SQLSpec adapter implementing template-method execution over Vertebra drivers."""

    __slots__ = ("_driver_closed", "_exception_handler", "_shared_pool", "native_driver")

    dialect: Any = "generic"
    _execution_parameter_style: ClassVar[ParameterStyle | None] = None
    _prefer_row_arrow_fallback: ClassVar[bool] = False
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
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize adapter over a native Vertebra driver preserving explicit statement configs."""
        resolved_cfg = statement_config if statement_config is not None else self._default_statement_config()
        super().__init__(connection, resolved_cfg, driver_features, observability)
        self.native_driver: Any = connection
        self._shared_pool: bool = shared_pool
        self._driver_closed: bool = False
        self._exception_handler: _DeferredSyncExceptionHandler = _DeferredSyncExceptionHandler(self._map_exception)
        callables = self._cache_hit_callables()
        if callables is not None:
            self.set_cached_statement_executor(
                BaseSyncDriver._run_cached_statement,
                state=(callables[0], callables[1], self._map_exception),
                owner=self._cache_hit_owner(),
            )

    @staticmethod
    def _merge_statement_config(
        base: StatementConfig,
        override: StatementConfig | None = None,
        **updates: Any,
    ) -> StatementConfig:
        """Return a merged `StatementConfig` via non-mutating `.replace(...)` while respecting caller overrides."""
        if override is not None and not updates:
            return override
        target = override if override is not None else base
        if not updates:
            return target
        return target.replace(**updates)

    @staticmethod
    def _build_statement_config(
        dialect: str,
        execution_style: ParameterStyle | None = None,
        *,
        default_style: ParameterStyle = ParameterStyle.QMARK,
        supported_styles: Collection[ParameterStyle] | None = None,
        supported_execution_styles: Collection[ParameterStyle] | None = None,
        preserve_parameter_format: bool = True,
        enable_caching: bool | None = None,
    ) -> StatementConfig:
        """Construct a default `StatementConfig` configured for the adapter's native bind syntax."""
        all_styles = tuple(ParameterStyle)
        if supported_styles is not None:
            input_styles: Collection[ParameterStyle] = set(supported_styles)
        elif execution_style == ParameterStyle.NAMED_COLON:
            input_styles = {ParameterStyle.QMARK, ParameterStyle.NAMED_COLON}
        elif execution_style is not None:
            input_styles = {default_style, execution_style}
        else:
            input_styles = all_styles

        if supported_execution_styles is not None:
            exec_styles: Collection[ParameterStyle] = set(supported_execution_styles)
        elif execution_style is not None:
            exec_styles = {execution_style}
        else:
            exec_styles = all_styles

        caching = get_cache_config().compiled_cache_enabled if enable_caching is None else enable_caching
        return StatementConfig(
            dialect=dialect,
            enable_caching=caching,
            parameter_config=ParameterStyleConfig(
                default_parameter_style=default_style,
                supported_parameter_styles=set(input_styles),
                default_execution_parameter_style=execution_style,
                supported_execution_parameter_styles=set(exec_styles),
                preserve_parameter_format=preserve_parameter_format,
            ),
        )

    @staticmethod
    def _is_google_api_exception(exc: BaseException) -> bool:
        """Check whether `exc` originates from `google.api_core` or `google.cloud` without importing them."""
        return any(cls.__module__.startswith(("google.api_core", "google.cloud")) for cls in type(exc).__mro__)

    @staticmethod
    def _map_native_database_exception(exc: BaseException) -> Exception | None:
        """Map Vertebra native `DatabaseError` / `GoogleCloudError` hierarchy exceptions to `sqlspec.exceptions.*`."""
        if isinstance(exc, SQLSpecError):
            return None
        if not isinstance(exc, (DatabaseError, GoogleCloudError)) and not BaseSyncDriver._is_google_api_exception(exc):
            return None

        message = str(exc)
        lower = message.lower()
        sqlstate = str(getattr(exc, "sqlstate", "") or "").upper()
        code_attr = getattr(exc, "code", None)
        code_str = str(code_attr if code_attr is not None else "").upper()

        if (
            isinstance(exc, GoogleCloudAuthError)
            or sqlstate in {"42501", "28000", "28P01"}
            or any(
                token in lower
                for token in ("permission denied", "unauthenticated", "forbidden", "access denied", "ora-01031")
            )
        ):
            return PermissionDeniedError(message)

        if isinstance(exc, QueryCancelledError) or any(
            token in lower for token in ("cancelled", "canceled", "query was interrupted", "57014")
        ):
            return OperationCancelledError(message)

        if (
            isinstance(exc, (SpannerAbortedError, SerializationFailure))
            or sqlstate in {"40001", "40P01"}
            or any(token in lower for token in ("deadlock", "serialization", "ora-00060", "ora-08177", "aborted"))
        ):
            if "deadlock" in lower or sqlstate == "40P01" or "ora-00060" in lower or "1213" in lower:
                return DeadlockError(message)
            return SerializationConflictError(message)

        if isinstance(exc, NativeIntegrityError) or sqlstate.startswith("23"):
            if (
                "unique" in lower
                or "duplicate" in lower
                or sqlstate == "23505"
                or "ora-00001" in lower
                or "1062" in lower
            ):
                return UniqueViolationError(message)
            if (
                "foreign key" in lower
                or sqlstate == "23503"
                or "ora-02291" in lower
                or "ora-02292" in lower
                or "1451" in lower
                or "1452" in lower
            ):
                return ForeignKeyViolationError(message)
            if "check constraint" in lower or sqlstate == "23514":
                return CheckViolationError(message)
            if "not null" in lower or "not-null" in lower or sqlstate == "23502":
                return NotNullViolationError(message)
            return IntegrityError(message)

        if isinstance(exc, (ConnectionFailure, PoolExhaustedError)) or sqlstate.startswith("08"):
            if "timeout" in lower or "timed out" in lower or "ora-12170" in lower:
                return ConnectionTimeoutError(message)
            return DatabaseConnectionError(message)

        if isinstance(exc, (NativeDataError, NativeNotFoundError, BigQueryRowError)) or sqlstate.startswith("22"):
            return DataError(message)

        if isinstance(exc, NotSupportedError):
            return DialectNotSupportedError(message)

        if isinstance(exc, NativeOperationalError) or "timeout" in lower or "deadline" in lower:
            if any(token in lower for token in ("timeout", "timed out", "deadline", "1205", "57014")):
                return QueryTimeoutError(message)
            if "deadlock" in lower:
                return DeadlockError(message)
            if any(token in lower for token in ("connection", "refused", "unreachable", "reset by peer")):
                return DatabaseConnectionError(message)
            return OperationalError(message)

        if isinstance(exc, ProgrammingError) or any(
            token in lower for token in ("syntax", "parse error", "no such table", "does not exist", "undefined")
        ):
            return SQLParsingError(message)

        if isinstance(exc, (BigQueryError, SpannerError, GoogleCloudError)) or BaseSyncDriver._is_google_api_exception(
            exc
        ):
            if "ALREADY_EXISTS" in code_str or "already exists" in lower:
                return UniqueViolationError(message)
            if "DEADLINE_EXCEEDED" in code_str or "timeout" in lower:
                return QueryTimeoutError(message)
            if "PERMISSION_DENIED" in code_str or "UNAUTHENTICATED" in code_str:
                return PermissionDeniedError(message)
            if "ABORTED" in code_str:
                return SerializationConflictError(message)
            if "CANCELLED" in code_str:
                return OperationCancelledError(message)
            return OperationalError(message)

        if isinstance(exc, (InterfaceError, InternalError, DatabaseError)):
            return OperationalError(message)

        return OperationalError(message)

    @staticmethod
    def _coerce_arrow_payload(
        arrow_table: Any,
        *,
        return_format: str,
        batch_size: int | None,
        arrow_schema: Any,
    ) -> Any:
        """Format an Arrow table into the caller's requested `return_format`."""
        pa = import_optional("pyarrow")
        table = arrow_table
        if hasattr(table, "to_arrow_table"):
            table = table.to_arrow_table()
        elif hasattr(table, "read_all"):
            table = table.read_all()

        if arrow_schema is not None and pa is not None and hasattr(table, "cast"):
            table = table.cast(arrow_schema)

        if return_format == "table":
            return table
        if pa is None or not hasattr(table, "to_batches"):
            return table

        batches = table.to_batches(max_chunksize=batch_size) if batch_size is not None else table.to_batches()
        if return_format == "batch":
            return (
                batches[0]
                if batches
                else pa.RecordBatch.from_pydict({name: [] for name in table.column_names}, schema=table.schema)
            )
        if return_format == "batches":
            return iter(batches)
        if return_format == "reader":
            return pa.RecordBatchReader.from_batches(table.schema, batches)
        return table

    @staticmethod
    def _table_row_count(table: Any) -> int:
        """Extract row count from an Arrow table, batch, or sequence."""
        if hasattr(table, "num_rows"):
            return int(table.num_rows)
        if hasattr(table, "__len__"):
            return len(table)
        return 0

    @property
    def data_dictionary(self) -> Any:
        """Return adapter metadata dictionary or `None` when generic."""
        return None

    def _map_exception(self, exc: BaseException) -> Exception | None:
        """Override in engine drivers to apply engine-specific error code mappings before fallback."""
        _ = exc
        return None

    @staticmethod
    def _single_call_pair(
        target: Any, fetch_name: str, dml_name: str
    ) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the two bound single-call methods of `target`, or `None` when either is missing."""
        fetch_rows = getattr(target, fetch_name, None)
        run_dml = getattr(target, dml_name, None)
        if not callable(fetch_rows) or not callable(run_dml):
            return None
        return fetch_rows, run_dml

    @staticmethod
    def _normalize_rowcount(value: Any) -> int:
        """Return a non-negative integer row count, rejecting `bool` and non-integer sentinels."""
        if isinstance(value, bool) or not isinstance(value, int):
            return 0
        return value if value > 0 else 0

    @staticmethod
    def _normalize_last_inserted_id(value: Any) -> int | str | None:
        """Return a normalized `int` or non-empty `str` last inserted identifier, rejecting `bool`."""
        if isinstance(value, bool):
            return None
        if isinstance(value, str):
            return value or None
        if isinstance(value, int):
            return value
        return None

    def _cache_hit_owner(self) -> type[SyncDriverAdapterBase]:
        """Return the driver class whose cache-hit hook identities guard executor registration."""
        return BaseSyncDriver

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the row fetch and statement run the cache-hit executor calls.

        Returns:
            The native driver's `execute_with_description` and `execute_dml`, or `None` to register no executor.
        """
        return self._single_call_pair(self.native_driver, "execute_with_description", "execute_dml")

    @staticmethod
    def _run_cached_statement(
        connection: Any,
        sql: str,
        params: Any,
        cached: CachedQuery,
        statement_config: StatementConfig,
        state: _CacheHitState,
    ) -> SQLResult:
        """Run a cached statement with one driver call and build its result.

        Args:
            connection: The native driver. Unused; the callables in `state` are bound to it.
            sql: Raw SQL text the caller passed.
            params: Parameters the caller passed, already valid for the compiled SQL.
            cached: Cached compilation of `sql`.
            statement_config: Statement configuration the result uses to rebuild its statement.
            state: Row fetch, statement run, and error mapper chosen at construction.

        Returns:
            The statement result.

        Raises:
            SQLSpecError: Mapped from a native error, with the native error as its cause.
        """
        _ = connection
        compiled_sql = cached.compiled_sql
        try:
            if cached.operation_profile.returns_rows:
                rows, names = state[0](compiled_sql, params or ())
                return SQLResult.create_cached_select(
                    sql,
                    statement_config,
                    cached.processed_state,
                    compiled_sql,
                    params,
                    rows,
                    list(names),
                    len(rows),
                    "tuple",
                    None,
                )
            rowcount, lastrowid = state[1](compiled_sql, params or ())
        except Exception as exc:
            if isinstance(exc, SQLSpecError):
                raise
            mapped = state[2](exc)
            if mapped is None:
                mapped = BaseSyncDriver._map_native_database_exception(exc)
            if mapped is None:
                raise
            raise mapped from exc
        affected_rows = BaseSyncDriver._normalize_rowcount(rowcount)
        operation_type = cached.operation_type
        if operation_type != "INSERT" or affected_rows <= 0:
            return DMLResult(operation_type, affected_rows, None)
        return DMLResult(operation_type, affected_rows, BaseSyncDriver._normalize_last_inserted_id(lastrowid))

    def handle_database_exceptions(self) -> _DeferredSyncExceptionHandler:
        """Return synchronous deferred exception handler compatible with SQLSpec's dispatcher."""
        self._exception_handler.pending_exception = None
        return self._exception_handler

    def with_cursor(self, connection: Any) -> AbstractContextManager[Any]:
        """Return a native cursor context opening one cursor on `connection`."""
        return _NativeSyncCursorContext(connection)

    def _connection_in_transaction(self) -> bool:
        """Report whether the wrapped native driver currently has an active transaction."""
        return bool(getattr(self.connection, "in_transaction", False))

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single compiled statement against the Vertebra native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, params = self._compiled_sql(statement, effective_config)
        cursor.execute(sql, params or ())
        description = cursor.description
        if description is not None:
            rows = cursor.fetchall()
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=[column[0] for column in description],
                data_row_count=len(rows),
                is_select_result=True,
                row_format="tuple",
                last_inserted_id=BaseSyncDriver._normalize_last_inserted_id(cursor.lastrowid),
            )
        return self.create_execution_result(
            cursor,
            rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount),
            last_inserted_id=BaseSyncDriver._normalize_last_inserted_id(cursor.lastrowid),
        )

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single statement across multiple compiled parameter sets."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor,
            rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount),
            is_many_result=True,
        )

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Split a SQL script via SQLGlot dialect rules and dispatch each statement sequentially."""
        return super().dispatch_execute_script(cursor, statement)

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> SyncRowStream[dict[str, Any]] | None:
        """Return a native chunked `SyncRowStream` when the underlying driver provides `stream_arrow_batches`."""
        if self._prefer_row_arrow_fallback or not callable(getattr(self.connection, "stream_arrow_batches", None)):
            return None
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, params = self._compiled_sql(statement, effective_config)
        return SyncRowStream(_VertebraSyncRowSource(self.connection, sql, params or None, chunk_size))

    def execute_many(
        self,
        statement: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SQLResult:
        """Execute a batched statement preserving filters attached alongside prebuilt `SQL` objects."""
        if isinstance(statement, SQL) and filters:
            config = statement_config or self.statement_config
            base_statement = self.prepare_statement(statement, filters, statement_config=config, kwargs=kwargs)
            filtered_statement = SQL(
                base_statement.raw_expression or base_statement.raw_sql,
                statement_config=config,
            )
            return super().execute_many(
                filtered_statement,
                parameters,
                statement_config=config,
                **kwargs,
            )
        return super().execute_many(
            statement,
            parameters,
            *filters,
            statement_config=statement_config,
            **kwargs,
        )

    def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        return_format: Literal["table", "reader", "batch", "batches"] = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any | None = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute a query returning zero-copy `ArrowCursor` / PyArrow structures wrapped in `ArrowResult`."""
        arrow_fn = getattr(self.connection, "select_to_arrow", None)
        if not self._prefer_row_arrow_fallback and callable(arrow_fn):
            sql_statement = self.prepare_statement(
                statement,
                parameters,
                statement_config=statement_config or self.statement_config,
                kwargs=kwargs,
            )
            if not sql_statement.is_processed:
                sql_statement.compile()
            effective_config = getattr(sql_statement, "statement_config", None) or self.statement_config
            sql, compiled_params = self._compiled_sql(sql_statement, effective_config)
            params = compiled_params or None
            raw = arrow_fn(sql, params) if params is not None else arrow_fn(sql)
            arrow_data = self._coerce_arrow_payload(
                raw,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )
            rows_affected = raw.row_count if isinstance(raw, ArrowCursor) else self._table_row_count(arrow_data)
            return create_arrow_result(
                statement=sql_statement,
                data=arrow_data,
                rows_affected=rows_affected,
            )
        if native_only:
            msg = f"Adapter '{type(self).__name__}' does not support native Arrow results"
            raise ImproperConfigurationError(msg)
        return super().select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=return_format,
            native_only=False,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
        )

    def select_to_storage(
        self,
        statement: SQL | str,
        destination: StorageDestination,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        partitioner: dict[str, object] | None = None,
        format_hint: StorageFormat | None = None,
        telemetry: StorageTelemetry | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Stream a SELECT query directly into object storage via `_select_for_storage` and `_write_storage_result`."""
        arrow_result = self._select_for_storage(statement, *parameters, statement_config=statement_config, **kwargs)
        sync_pipeline = self._storage_pipeline()
        telemetry_payload = self._write_storage_result(
            arrow_result,
            destination,
            format_hint=format_hint,
            pipeline=sync_pipeline,
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def _storage_pipeline(self) -> SyncStoragePipeline:
        """Build a synchronous storage pipeline backed by `ObjectStoreRegistry`."""
        factory = self.storage_pipeline_factory
        if factory is not None:
            return cast("SyncStoragePipeline", factory())
        pipeline_mod = ensure_module("sqlspec.storage.pipeline", package="sqlspec", extra="sqlspec")
        storage_mod = ensure_module("vertebra.extensions.sqlspec.storage")
        return cast(
            "SyncStoragePipeline",
            pipeline_mod.SyncStoragePipeline(registry=storage_mod.get_object_store_registry()),
        )

    def stream_arrow_batches(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> Iterator[Any]:
        """Yield Arrow record batches or dict chunks from the wrapped synchronous Vertebra driver."""
        sql_statement = self.prepare_statement(
            statement,
            parameters,
            statement_config=statement_config or self.statement_config,
            kwargs=kwargs,
        )
        effective_config = getattr(sql_statement, "statement_config", None) or self.statement_config
        sql, compiled_params = self._compiled_sql(sql_statement, effective_config)
        params = compiled_params or None
        stream_fn = getattr(self.connection, "stream_arrow_batches", None)
        if callable(stream_fn):
            yield from cast("Iterator[Any]", stream_fn(sql, params, batch_size=batch_size))
            return
        arrow_res = self.select_to_arrow(sql_statement, statement_config=sql_statement.statement_config)
        data = arrow_res.data
        if hasattr(data, "to_batches"):
            yield from data.to_batches(max_chunksize=batch_size)
        elif data:
            yield data

    def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute a `StatementStack` using native pipelining when eligible, else `SyncDriverAdapterBase` fallback."""
        if not isinstance(stack, StatementStack):
            raise TypeError("execute_stack expects a StatementStack instance")
        if not stack:
            raise ValueError("Cannot execute an empty StatementStack")
        operations = stack.operations
        if (
            isinstance(self.connection, SyncPipelineCapable)
            and not continue_on_error
            and len(operations) > 1
            and all(op.method == "execute" for op in operations)
        ):
            prepared: list[tuple[SQL, str, Any]] = []
            for op in operations:
                op_kwargs = dict(op.keyword_arguments or {})
                stmt_cfg = op_kwargs.pop("statement_config", None)
                prepared_stmt = self.prepare_statement(
                    op.statement,
                    op.arguments,
                    statement_config=stmt_cfg or self.statement_config,
                    kwargs=op_kwargs,
                )
                effective_config = getattr(prepared_stmt, "statement_config", None) or self.statement_config
                sql, compiled_params = self._compiled_sql(prepared_stmt, effective_config)
                prepared.append((prepared_stmt, sql, compiled_params or None))
            exc_handler = self.handle_database_exceptions()
            with exc_handler, self.connection.pipeline() as pipe:
                for prepared_stmt, sql, params in prepared:
                    if prepared_stmt.returns_rows():
                        pipe.select(sql, params)
                    else:
                        pipe.execute(sql, params)
                raw_results = pipe.results()
                return tuple(
                    StackResult.from_sql_result(
                        SQLResult(
                            statement=prepared_stmt,
                            data=result,
                            rows_affected=len(result),
                            operation_type=prepared_stmt.operation_type,
                        )
                    )
                    if isinstance(result, list)
                    else StackResult(rows_affected=int(result))
                    for (prepared_stmt, _, _), result in zip(prepared, raw_results, strict=True)
                )
            self._check_pending_exception(exc_handler)
        return super().execute_stack(stack, continue_on_error=continue_on_error)

    def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Refuse session-level RLS predicates unless implemented by the owning engine adapter."""
        _ = predicate
        msg = f"Tenant session enforcement is unsupported for {self.dialect}"
        raise NotSupportedError(msg)

    def begin(self) -> None:
        """Begin a transaction on the native Vertebra driver."""
        begin_fn = getattr(self.connection, "begin", None)
        if callable(begin_fn):
            begin_fn()

    def commit(self) -> None:
        """Commit the active transaction on the native Vertebra driver."""
        commit_fn = getattr(self.connection, "commit", None)
        if callable(commit_fn):
            commit_fn()

    def rollback(self) -> None:
        """Roll back the active transaction on the native Vertebra driver."""
        rollback_fn = getattr(self.connection, "rollback", None)
        if callable(rollback_fn):
            rollback_fn()

    def create_savepoint(self, name: str) -> None:
        """Create a named savepoint via the native driver or ANSI `SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "create_savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"SAVEPOINT {valid}")

    def release_savepoint(self, name: str) -> None:
        """Release a named savepoint via the native driver or ANSI `RELEASE SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "release_savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"RELEASE SAVEPOINT {valid}")

    def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a named savepoint via the native driver or ANSI `ROLLBACK TO SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(fn):
            fn(valid)
            return
        self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    def close(self) -> None:
        """Release the underlying native driver, respecting shared-pool reference counts."""
        if self._driver_closed:
            return
        self._driver_closed = True
        if self._shared_pool or SyncAdapterConfig._shared_native_refcount(self.connection) > 0:
            SyncAdapterConfig._release_shared_native_driver(self.connection, close_when_unshared=True)
            return
        close_fn = getattr(self.connection, "close", None)
        if callable(close_fn):
            close_fn()

    def collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        """Collect rows and column names from a native cursor for direct cache-hit paths."""
        description = cursor.description
        if description is None:
            return [], [], 0
        return fetched, [column[0] for column in description], len(fetched)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve affected row count from native driver cursor attributes."""
        return BaseSyncDriver._normalize_rowcount(cursor.rowcount)

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build a default `StatementConfig` for this synchronous adapter."""
        return cls._build_statement_config(str(cls.dialect or "generic"), cls._execution_parameter_style)

    @staticmethod
    def _hydrate_rows_schema(rows: list[Any], schema_type: type[Any] | None) -> list[Any]:
        """Optionally convert hydrated dict rows into a target `schema_type`."""
        if not rows or schema_type is None or schema_type is dict:
            return rows
        if isinstance(rows[0], schema_type):
            return rows
        plan = ZeroCopyCodecRegistry.compile(schema_type)
        if isinstance(rows[0], dict):
            cols = tuple(rows[0].keys())
            tuple_rows = [tuple(row.get(col) for col in cols) for row in rows]
            return plan.hydrate_rows(tuple_rows, cols)
        return [msgspec.convert(row, type=schema_type) for row in rows]


class BaseAsyncDriver(AsyncDriverAdapterBase):
    """Base asynchronous SQLSpec adapter implementing template-method execution over Vertebra drivers."""

    __slots__ = ("_driver_closed", "_exception_handler", "_shared_pool", "native_driver")

    dialect: Any = "generic"
    _execution_parameter_style: ClassVar[ParameterStyle | None] = None
    _prefer_row_arrow_fallback: ClassVar[bool] = False
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
        *,
        shared_pool: bool = False,
    ) -> None:
        """Initialize async adapter over a native Vertebra driver preserving explicit statement configs."""
        resolved_cfg = statement_config if statement_config is not None else self._default_statement_config()
        super().__init__(connection, resolved_cfg, driver_features, observability)
        self.native_driver: Any = connection
        self._shared_pool: bool = shared_pool
        self._driver_closed: bool = False
        self._exception_handler: _DeferredAsyncExceptionHandler = _DeferredAsyncExceptionHandler(self._map_exception)
        callables = self._cache_hit_callables()
        if callables is not None:
            self.set_cached_statement_executor(
                BaseAsyncDriver._run_cached_statement,
                state=(callables[0], callables[1], self._map_exception),
                owner=self._cache_hit_owner(),
            )

    @property
    def data_dictionary(self) -> Any:
        """Return async adapter metadata dictionary or `None` when generic."""
        return None

    def _map_exception(self, exc: BaseException) -> Exception | None:
        """Override in engine drivers to apply engine-specific error code mappings before fallback."""
        _ = exc
        return None

    def _cache_hit_owner(self) -> type[AsyncDriverAdapterBase]:
        """Return the driver class whose cache-hit hook identities guard executor registration."""
        return BaseAsyncDriver

    def _cache_hit_callables(self) -> tuple[Callable[..., Any], Callable[..., Any]] | None:
        """Return the row fetch and statement run the cache-hit executor awaits.

        Returns:
            The native driver's `execute_with_description` and `execute_dml`, or `None` to register no executor.
        """
        return BaseSyncDriver._single_call_pair(self.native_driver, "execute_with_description", "execute_dml")

    @staticmethod
    async def _run_cached_statement(
        connection: Any,
        sql: str,
        params: Any,
        cached: CachedQuery,
        statement_config: StatementConfig,
        state: _CacheHitState,
    ) -> SQLResult:
        """Run a cached statement by awaiting one driver call and build its result.

        Args:
            connection: The native driver. Unused; the callables in `state` are bound to it.
            sql: Raw SQL text the caller passed.
            params: Parameters the caller passed, already valid for the compiled SQL.
            cached: Cached compilation of `sql`.
            statement_config: Statement configuration the result uses to rebuild its statement.
            state: Row fetch, statement run, and error mapper chosen at construction.

        Returns:
            The statement result.

        Raises:
            SQLSpecError: Mapped from a native error, with the native error as its cause.
        """
        _ = connection
        compiled_sql = cached.compiled_sql
        try:
            if cached.operation_profile.returns_rows:
                rows, names = await state[0](compiled_sql, params or ())
                return SQLResult.create_cached_select(
                    sql,
                    statement_config,
                    cached.processed_state,
                    compiled_sql,
                    params,
                    rows,
                    list(names),
                    len(rows),
                    "tuple",
                    None,
                )
            rowcount, lastrowid = await state[1](compiled_sql, params or ())
        except Exception as exc:
            if isinstance(exc, SQLSpecError):
                raise
            mapped = state[2](exc)
            if mapped is None:
                mapped = BaseSyncDriver._map_native_database_exception(exc)
            if mapped is None:
                raise
            raise mapped from exc
        affected_rows = BaseSyncDriver._normalize_rowcount(rowcount)
        operation_type = cached.operation_type
        if operation_type != "INSERT" or affected_rows <= 0:
            return DMLResult(operation_type, affected_rows, None)
        return DMLResult(operation_type, affected_rows, BaseSyncDriver._normalize_last_inserted_id(lastrowid))

    def handle_database_exceptions(self) -> _DeferredAsyncExceptionHandler:
        """Return asynchronous deferred exception handler compatible with SQLSpec's dispatcher."""
        self._exception_handler.pending_exception = None
        return self._exception_handler

    def with_cursor(self, connection: Any) -> AbstractAsyncContextManager[Any]:
        """Return a native async cursor context opening one cursor on `connection`."""
        return _NativeAsyncCursorContext(connection)

    def _connection_in_transaction(self) -> bool:
        """Report whether the wrapped async native driver currently has an active transaction."""
        return bool(getattr(self.connection, "in_transaction", False))

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single compiled statement asynchronously against the Vertebra native cursor."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, params = self._compiled_sql(statement, effective_config)
        await cursor.execute(sql, params or ())
        description = cursor.description
        if description is not None:
            rows = await cursor.fetchall()
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=[column[0] for column in description],
                data_row_count=len(rows),
                is_select_result=True,
                row_format="tuple",
                last_inserted_id=BaseSyncDriver._normalize_last_inserted_id(cursor.lastrowid),
            )
        return self.create_execution_result(
            cursor,
            rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount),
            last_inserted_id=BaseSyncDriver._normalize_last_inserted_id(cursor.lastrowid),
        )

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single statement asynchronously across multiple compiled parameter sets."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        if not prepared:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        await cursor.executemany(sql, prepared)
        return self.create_execution_result(
            cursor,
            rowcount_override=BaseSyncDriver._normalize_rowcount(cursor.rowcount),
            is_many_result=True,
        )

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Split a SQL script via SQLGlot dialect rules and dispatch each statement sequentially."""
        return await super().dispatch_execute_script(cursor, statement)

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> AsyncRowStream[dict[str, Any]] | None:
        """Return a native chunked `AsyncRowStream` when the underlying driver provides `stream_arrow_batches`."""
        if self._prefer_row_arrow_fallback or not callable(getattr(self.connection, "stream_arrow_batches", None)):
            return None
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, params = self._compiled_sql(statement, effective_config)
        return AsyncRowStream(_VertebraAsyncRowSource(self.connection, sql, params or None, chunk_size))

    async def execute_many(
        self,
        statement: Any,
        /,
        parameters: Sequence[Any],
        *filters: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> SQLResult:
        """Execute a batched async statement preserving filters attached alongside prebuilt `SQL` objects."""
        if isinstance(statement, SQL) and filters:
            config = statement_config or self.statement_config
            base_statement = self.prepare_statement(statement, filters, statement_config=config, kwargs=kwargs)
            filtered_statement = SQL(
                base_statement.raw_expression or base_statement.raw_sql,
                statement_config=config,
            )
            return await super().execute_many(
                filtered_statement,
                parameters,
                statement_config=config,
                **kwargs,
            )
        return await super().execute_many(
            statement,
            parameters,
            *filters,
            statement_config=statement_config,
            **kwargs,
        )

    async def select_to_arrow(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        return_format: Literal["table", "reader", "batch", "batches"] = "table",
        native_only: bool = False,
        batch_size: int | None = None,
        arrow_schema: Any | None = None,
        **kwargs: Any,
    ) -> ArrowResult:
        """Execute an async query returning zero-copy `ArrowCursor` / PyArrow structures wrapped in `ArrowResult`."""
        arrow_fn = getattr(self.connection, "select_to_arrow", None)
        if not self._prefer_row_arrow_fallback and callable(arrow_fn):
            sql_statement = self.prepare_statement(
                statement,
                parameters,
                statement_config=statement_config or self.statement_config,
                kwargs=kwargs,
            )
            if not sql_statement.is_processed:
                sql_statement.compile()
            effective_config = getattr(sql_statement, "statement_config", None) or self.statement_config
            sql, compiled_params = self._compiled_sql(sql_statement, effective_config)
            params = compiled_params or None
            raw = await cast("Awaitable[Any]", arrow_fn(sql, params) if params is not None else arrow_fn(sql))
            arrow_data = BaseSyncDriver._coerce_arrow_payload(
                raw,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )
            rows_affected = (
                raw.row_count if isinstance(raw, ArrowCursor) else BaseSyncDriver._table_row_count(arrow_data)
            )
            return create_arrow_result(
                statement=sql_statement,
                data=arrow_data,
                rows_affected=rows_affected,
            )
        if native_only:
            msg = f"Adapter '{type(self).__name__}' does not support native Arrow results"
            raise ImproperConfigurationError(msg)
        return await super().select_to_arrow(
            statement,
            *parameters,
            statement_config=statement_config,
            return_format=return_format,
            native_only=False,
            batch_size=batch_size,
            arrow_schema=arrow_schema,
            **kwargs,
        )

    async def select_to_storage(
        self,
        statement: SQL | str,
        destination: StorageDestination,
        /,
        *parameters: StatementParameters | StatementFilter,
        statement_config: StatementConfig | None = None,
        partitioner: dict[str, object] | None = None,
        format_hint: StorageFormat | None = None,
        telemetry: StorageTelemetry | None = None,
        **kwargs: Any,
    ) -> StorageBridgeJob:
        """Stream a SELECT query asynchronously into object storage via `_select_for_storage` and `_write_storage_result`."""
        arrow_result = await self._select_for_storage(
            statement, *parameters, statement_config=statement_config, **kwargs
        )
        async_pipeline = self._storage_pipeline()
        telemetry_payload = await self._write_storage_result(
            arrow_result,
            destination,
            format_hint=format_hint,
            pipeline=async_pipeline,
        )
        self._attach_partition_telemetry(telemetry_payload, partitioner)
        return self._storage_job(telemetry_payload, telemetry)

    def _storage_pipeline(self) -> AsyncStoragePipeline:
        """Build an asynchronous storage pipeline backed by `ObjectStoreRegistry`."""
        factory = self.storage_pipeline_factory
        if factory is not None:
            return cast("AsyncStoragePipeline", factory())
        pipeline_mod = ensure_module("sqlspec.storage.pipeline", package="sqlspec", extra="sqlspec")
        storage_mod = ensure_module("vertebra.extensions.sqlspec.storage")
        return cast(
            "AsyncStoragePipeline",
            pipeline_mod.AsyncStoragePipeline(registry=storage_mod.get_object_store_registry()),
        )

    async def stream_arrow_batches(
        self,
        statement: Any,
        /,
        *parameters: Any,
        statement_config: StatementConfig | None = None,
        batch_size: int = 65_536,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Yield Arrow record batches or dict chunks from the wrapped async Vertebra driver."""
        sql_statement = self.prepare_statement(
            statement,
            parameters,
            statement_config=statement_config or self.statement_config,
            kwargs=kwargs,
        )
        effective_config = getattr(sql_statement, "statement_config", None) or self.statement_config
        sql, compiled_params = self._compiled_sql(sql_statement, effective_config)
        params = compiled_params or None
        stream_fn = getattr(self.connection, "stream_arrow_batches", None)
        if callable(stream_fn):
            async for batch in cast("AsyncIterator[Any]", stream_fn(sql, params, batch_size=batch_size)):
                yield batch
            return
        arrow_res = await self.select_to_arrow(sql_statement, statement_config=sql_statement.statement_config)
        data = arrow_res.data
        if hasattr(data, "to_batches"):
            for batch in data.to_batches(max_chunksize=batch_size):
                yield batch
        elif data:
            yield data

    async def execute_stack(
        self,
        stack: StatementStack,
        *,
        continue_on_error: bool = False,
    ) -> tuple[StackResult, ...]:
        """Execute an async `StatementStack` using native pipelining when eligible, else `AsyncDriverAdapterBase` fallback."""
        if not isinstance(stack, StatementStack):
            raise TypeError("execute_stack expects a StatementStack instance")
        if not stack:
            raise ValueError("Cannot execute an empty StatementStack")
        operations = stack.operations
        if (
            isinstance(self.connection, AsyncPipelineCapable)
            and not continue_on_error
            and len(operations) > 1
            and all(op.method == "execute" for op in operations)
        ):
            prepared: list[tuple[SQL, str, Any]] = []
            for op in operations:
                op_kwargs = dict(op.keyword_arguments or {})
                stmt_cfg = op_kwargs.pop("statement_config", None)
                prepared_stmt = self.prepare_statement(
                    op.statement,
                    op.arguments,
                    statement_config=stmt_cfg or self.statement_config,
                    kwargs=op_kwargs,
                )
                effective_config = getattr(prepared_stmt, "statement_config", None) or self.statement_config
                sql, compiled_params = self._compiled_sql(prepared_stmt, effective_config)
                prepared.append((prepared_stmt, sql, compiled_params or None))
            exc_handler = self.handle_database_exceptions()
            async with exc_handler, self.connection.pipeline() as pipe:
                for prepared_stmt, sql, params in prepared:
                    if prepared_stmt.returns_rows():
                        pipe.select(sql, params)
                    else:
                        pipe.execute(sql, params)
                raw_results = await pipe.results()
                return tuple(
                    StackResult.from_sql_result(
                        SQLResult(
                            statement=prepared_stmt,
                            data=result,
                            rows_affected=len(result),
                            operation_type=prepared_stmt.operation_type,
                        )
                    )
                    if isinstance(result, list)
                    else StackResult(rows_affected=int(result))
                    for (prepared_stmt, _, _), result in zip(prepared, raw_results, strict=True)
                )
            self._check_pending_exception(exc_handler)
        return await super().execute_stack(stack, continue_on_error=continue_on_error)

    async def apply_tenant_predicate(self, predicate: TenantPredicate) -> str:
        """Refuse session-level RLS predicates unless implemented by the owning engine adapter."""
        _ = predicate
        msg = f"Tenant session enforcement is unsupported for {self.dialect}"
        raise NotSupportedError(msg)

    async def begin(self) -> None:
        """Begin a transaction on the native async Vertebra driver."""
        begin_fn = getattr(self.connection, "begin", None)
        if callable(begin_fn):
            await cast("Awaitable[Any]", begin_fn())

    async def commit(self) -> None:
        """Commit the active transaction on the native async Vertebra driver."""
        commit_fn = getattr(self.connection, "commit", None)
        if callable(commit_fn):
            await cast("Awaitable[Any]", commit_fn())

    async def rollback(self) -> None:
        """Roll back the active transaction on the native async Vertebra driver."""
        rollback_fn = getattr(self.connection, "rollback", None)
        if callable(rollback_fn):
            await cast("Awaitable[Any]", rollback_fn())

    async def create_savepoint(self, name: str) -> None:
        """Create a named savepoint via the async native driver or ANSI `SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "create_savepoint", None)
        if callable(fn):
            await cast("Awaitable[Any]", fn(valid))
            return
        await self.connection.execute(f"SAVEPOINT {valid}")

    async def release_savepoint(self, name: str) -> None:
        """Release a named savepoint via the async native driver or ANSI `RELEASE SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "release_savepoint", None)
        if callable(fn):
            await cast("Awaitable[Any]", fn(valid))
            return
        await self.connection.execute(f"RELEASE SAVEPOINT {valid}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a named savepoint via the async native driver or ANSI `ROLLBACK TO SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(fn):
            await cast("Awaitable[Any]", fn(valid))
            return
        await self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    async def close(self) -> None:
        """Release the underlying async native driver, respecting shared-pool reference counts."""
        if self._driver_closed:
            return
        self._driver_closed = True
        if self._shared_pool or AsyncAdapterConfig._shared_native_refcount(self.connection) > 0:
            await AsyncAdapterConfig._release_shared_native_driver_async(self.connection, close_when_unshared=True)
            return
        close_fn = getattr(self.connection, "close", None)
        if callable(close_fn):
            res = close_fn()
            if isinstance(res, Awaitable):
                await res

    def collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        """Collect rows and column names from a native cursor for direct cache-hit paths."""
        description = cursor.description
        if description is None:
            return [], [], 0
        return fetched, [column[0] for column in description], len(fetched)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve affected row count from native driver cursor attributes."""
        return BaseSyncDriver._normalize_rowcount(cursor.rowcount)

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build a default `StatementConfig` for this asynchronous adapter."""
        return BaseSyncDriver._build_statement_config(str(cls.dialect or "generic"), cls._execution_parameter_style)

    @staticmethod
    def _hydrate_rows_schema(rows: list[Any], schema_type: type[Any] | None) -> list[Any]:
        """Optionally convert hydrated dict rows into a target `schema_type`."""
        return BaseSyncDriver._hydrate_rows_schema(rows, schema_type)


class _AdapterSyncConnectionContext:
    """Default synchronous connection context manager for `SyncAdapterConfig`."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: SyncAdapterConfig[Any, Any, Any, Any], *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._tenant_lease: Any = None

    def __enter__(self) -> Any:
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = self._tenant_lease.acquire()
            return self._connection
        pool = self._config.provide_pool()
        get_conn = getattr(pool, "get_connection", None)
        if callable(get_conn):
            self._conn_ctx = get_conn()
            self._connection = self._conn_ctx.__enter__()
        else:
            self._connection = pool
        return self._connection

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                if getattr(conn, "in_transaction", False):
                    rollback_fn = getattr(conn, "rollback", None)
                    if callable(rollback_fn):
                        with contextlib.suppress(Exception):
                            rollback_fn()
                if reset_on_release:
                    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
                    if callable(reset_fn):
                        with contextlib.suppress(Exception):
                            reset_fn()
            if self._tenant_lease is not None:
                self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", self._conn_ctx.__exit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class _AdapterSyncSessionContext:
    """Default synchronous session context manager creating a fresh driver per session entry."""

    __slots__ = (
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_driver",
        "_kwargs",
        "_statement_config",
        "_tenant_lease",
    )

    def __init__(
        self,
        config: SyncAdapterConfig[Any, Any, Any, Any],
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._driver: Any = None
        self._tenant_lease: Any = None

    def __enter__(self) -> Any:
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = self._tenant_lease.acquire()
        else:
            pool = self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            if callable(get_conn):
                self._conn_ctx = get_conn()
                self._connection = self._conn_ctx.__enter__()
            else:
                self._connection = pool
        try:
            stmt_cfg = self._statement_config if self._statement_config is not None else self._config.statement_config
            driver = self._config.driver_type(
                self._connection,
                statement_config=stmt_cfg,
                driver_features=dict(self._config.driver_features),
                shared_pool=self._config._pool_is_shared,
            )
            self._driver = self._config._prepare_driver(driver)
            return self._driver
        except BaseException as exc:
            self.__exit__(type(exc), exc, exc.__traceback__)
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        driver = self._driver
        conn = self._connection
        try:
            if driver is not None:
                close_streams_fn = getattr(driver, "_close_open_streams", None)
                if callable(close_streams_fn):
                    close_streams_fn(error=exc_type is not None)
        except BaseException as close_exc:
            if exc_type is None:
                exc_type, exc_val, exc_tb = type(close_exc), close_exc, close_exc.__traceback__
                raise
        finally:
            try:
                reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
                pooled_state = bool(
                    self._config.share_pool or self._tenant_lease is not None or self._conn_ctx is not None
                )
                if driver is not None and (exc_type is not None or pooled_state or reset_on_release):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        with contextlib.suppress(Exception):
                            driver.rollback()
                if conn is not None and reset_on_release:
                    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
                    if callable(reset_fn):
                        with contextlib.suppress(Exception):
                            reset_fn()
                if self._tenant_lease is not None:
                    self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
                elif self._conn_ctx is not None:
                    self._conn_ctx.__exit__(exc_type, exc_val, exc_tb)
            finally:
                self._connection = None
                self._conn_ctx = None
                self._tenant_lease = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None


class _AdapterAsyncConnectionContext:
    """Default asynchronous connection context manager for `AsyncAdapterConfig`."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: AsyncAdapterConfig[Any, Any, Any, Any], *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> Any:
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = await self._tenant_lease.acquire()
            return self._connection
        pool = await self._config.provide_pool()
        get_conn = getattr(pool, "get_connection", None)
        if callable(get_conn):
            self._conn_ctx = get_conn()
            self._connection = await self._conn_ctx.__aenter__()
        else:
            self._connection = pool
        return self._connection

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                if getattr(conn, "in_transaction", False):
                    rollback_fn = getattr(conn, "rollback", None)
                    if callable(rollback_fn):
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            res = rollback_fn()
                            if isinstance(res, Awaitable):
                                await res
                if reset_on_release:
                    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
                    if callable(reset_fn):
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            res = reset_fn()
                            if isinstance(res, Awaitable):
                                await res
            if self._tenant_lease is not None:
                await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class _AdapterAsyncSessionContext:
    """Default asynchronous session context manager creating a fresh driver per session entry."""

    __slots__ = (
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_driver",
        "_kwargs",
        "_statement_config",
        "_tenant_lease",
    )

    def __init__(
        self,
        config: AsyncAdapterConfig[Any, Any, Any, Any],
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._driver: Any = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> Any:
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = await self._tenant_lease.acquire()
        else:
            pool = await self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            if callable(get_conn):
                self._conn_ctx = get_conn()
                self._connection = await self._conn_ctx.__aenter__()
            else:
                self._connection = pool
        try:
            stmt_cfg = self._statement_config if self._statement_config is not None else self._config.statement_config
            driver = self._config.driver_type(
                self._connection,
                statement_config=stmt_cfg,
                driver_features=dict(self._config.driver_features),
                shared_pool=self._config._pool_is_shared,
            )
            self._driver = self._config._prepare_driver(driver)
            return self._driver
        except BaseException as exc:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        driver = self._driver
        conn = self._connection
        try:
            if driver is not None:
                close_streams_fn = getattr(driver, "_close_open_streams", None)
                if callable(close_streams_fn):
                    res = close_streams_fn(error=exc_type is not None)
                    if isinstance(res, Awaitable):
                        await res
        except BaseException as close_exc:
            if exc_type is None:
                exc_type, exc_val, exc_tb = type(close_exc), close_exc, close_exc.__traceback__
                raise
        finally:
            try:
                reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
                pooled_state = bool(
                    self._config.share_pool or self._tenant_lease is not None or self._conn_ctx is not None
                )
                if driver is not None and (exc_type is not None or pooled_state or reset_on_release):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            res = driver.rollback()
                            if isinstance(res, Awaitable):
                                await res
                if conn is not None and reset_on_release:
                    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
                    if callable(reset_fn):
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            res = reset_fn()
                            if isinstance(res, Awaitable):
                                await res
                if self._tenant_lease is not None:
                    await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
                elif self._conn_ctx is not None:
                    await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb)
            finally:
                self._connection = None
                self._conn_ctx = None
                self._tenant_lease = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None


class _AdapterLifecycleMixin:
    """Shared internal config resolution, pool derivation, and refcounted shared-pool lifecycle helpers."""

    __slots__ = ()

    _SHARED_POOL_LOCK: ClassVar[threading.Lock] = threading.Lock()
    _SHARED_POOL_REFS: ClassVar[dict[int, tuple[Any, int]]] = {}
    _ADAPTER_META_KEYS: ClassVar[frozenset[str]] = frozenset(
        {
            "bind_key",
            "broker",
            "share_pool",
            "arrow_threshold_rows",
            "statement_config",
            "migration_config",
            "driver_features",
            "driver_features_overrides",
            "extension_config",
            "observability_config",
            "connection_config",
            "connection_config_overrides",
            "connection_instance",
            "replicas",
            "is_replica",
        }
    )
    _URL_ALIAS_KEYS: ClassVar[tuple[str, ...]] = ("url", "dsn", "conninfo", "connection_string")

    @classmethod
    def _retain_shared_native_driver(cls, native_driver: Any) -> int:
        """Increment and return the active reference count for a shared native driver."""
        if native_driver is None:
            return 0
        key = id(native_driver)
        with cls._SHARED_POOL_LOCK:
            entry = cls._SHARED_POOL_REFS.get(key)
            next_count = (entry[1] if entry is not None else 0) + 1
            cls._SHARED_POOL_REFS[key] = (native_driver, next_count)
            return next_count

    @classmethod
    def _shared_native_refcount(cls, native_driver: Any) -> int:
        """Return the active shared-pool reference count for `native_driver`."""
        if native_driver is None:
            return 0
        with cls._SHARED_POOL_LOCK:
            entry = cls._SHARED_POOL_REFS.get(id(native_driver))
            return entry[1] if entry is not None else 0

    @classmethod
    def _decrement_shared_native_refcount(cls, native_driver: Any) -> bool:
        """Decrement a shared driver's reference count and report whether its underlying pool should close."""
        if native_driver is None:
            return False
        key = id(native_driver)
        with cls._SHARED_POOL_LOCK:
            entry = cls._SHARED_POOL_REFS.get(key)
            if entry is None:
                return True
            next_count = entry[1] - 1
            if next_count > 0:
                cls._SHARED_POOL_REFS[key] = (native_driver, next_count)
                return False
            cls._SHARED_POOL_REFS.pop(key, None)
            return True

    @staticmethod
    def _evict_closed_from_default_registry(native_driver: Any) -> None:
        """Remove a closed shared driver entry from the default `DatabaseRegistry` caches."""
        reg = DatabaseRegistry.default()
        with reg._lock:
            for cache in (reg._shared_sync_drivers, reg._shared_async_drivers):
                stale_keys = [cache_key for cache_key, cached in cache.items() if cached is native_driver]
                for cache_key in stale_keys:
                    cache.pop(cache_key, None)

    @classmethod
    def _acquire_shared_native_driver(
        cls,
        driver_config: Any,
        driver_cls: type[Any],
        *,
        share_pool: bool,
        broker: Any,
        is_async: bool,
    ) -> tuple[Any, bool]:
        """Acquire a shared native driver from `DatabaseRegistry` or build an owned instance."""
        if share_pool and broker is DatabaseRegistry.default_broker():
            reg = DatabaseRegistry.default()
            native = reg.get_shared_driver(driver_config) if is_async else reg.get_shared_sync_driver(driver_config)
            cls._retain_shared_native_driver(native)
            return native, True
        return driver_cls(driver_config, broker=broker), False

    @classmethod
    def _release_shared_native_driver(cls, native_driver: Any, *, close_when_unshared: bool = True) -> bool:
        """Release one reference to a synchronous native driver, closing it when the last reference exits."""
        should_close = cls._decrement_shared_native_refcount(native_driver)
        if not should_close or not close_when_unshared or native_driver is None:
            return False
        close_fn = getattr(native_driver, "close", None)
        if callable(close_fn):
            close_fn()
        cls._evict_closed_from_default_registry(native_driver)
        return True

    @classmethod
    async def _release_shared_native_driver_async(cls, native_driver: Any, *, close_when_unshared: bool = True) -> bool:
        """Release one reference to an asynchronous native driver, closing it when the last reference exits."""
        should_close = cls._decrement_shared_native_refcount(native_driver)
        if not should_close or not close_when_unshared or native_driver is None:
            return False
        close_fn = getattr(native_driver, "close", None)
        if callable(close_fn):
            result = close_fn()
            if isinstance(result, Awaitable):
                await result
        cls._evict_closed_from_default_registry(native_driver)
        return True

    @classmethod
    def _resolve_adapter_driver_config(
        cls,
        adapter: Any,
        config: Any,
        connection_config: Mapping[str, Any] | None,
        kwargs: Mapping[str, Any],
        *,
        broker: Any,
    ) -> Any:
        """Resolve `driver_config` invoking subclass `build_connection_config` when defined."""
        if config is not None and not isinstance(config, (str, Mapping)):
            return config

        merged_kwargs: dict[str, Any] = dict(connection_config) if isinstance(connection_config, Mapping) else {}
        if isinstance(config, Mapping):
            merged_kwargs.update(config)
        merged_kwargs.update(kwargs)

        build_fn = getattr(adapter, "build_connection_config", None)
        config_cls = getattr(adapter, "_config_cls", None)
        struct_fields = set(getattr(config_cls, "__struct_fields__", ())) if config_cls is not None else set()

        if callable(build_fn):
            raw_url: str | None = config if isinstance(config, str) and config.strip() else None
            if raw_url is None:
                for source in (kwargs, connection_config or {}):
                    if isinstance(source, Mapping):
                        for alias in cls._URL_ALIAS_KEYS:
                            candidate = source.get(alias)
                            if isinstance(candidate, str) and candidate.strip():
                                raw_url = candidate
                                break
                    if raw_url is not None:
                        break

            if raw_url is not None:
                extra_fields = {k: v for k, v in merged_kwargs.items() if k not in cls._URL_ALIAS_KEYS}
                base_cfg = build_fn(raw_url, None, {})
                if broker is not None and "broker" not in extra_fields and hasattr(base_cfg, "broker"):
                    extra_fields["broker"] = broker
                if extra_fields:
                    if isinstance(base_cfg, msgspec.Struct):
                        return msgspec.structs.replace(base_cfg, **extra_fields)
                    return build_fn(raw_url, None, extra_fields)
                return base_cfg

            if (
                broker is not None
                and "broker" not in merged_kwargs
                and (not struct_fields or "broker" in struct_fields)
            ):
                merged_kwargs["broker"] = broker
            return build_fn(None, None, merged_kwargs)

        if isinstance(config, str) and config.strip():
            merged_kwargs.setdefault("dsn" if "dsn" in struct_fields else "url", config)

        if struct_fields and "url" in merged_kwargs and "url" not in struct_fields:
            url_val = merged_kwargs.pop("url")
            if isinstance(url_val, str) and url_val.strip():
                from_url_fn = getattr(config_cls, "from_url", None)
                if callable(from_url_fn) and "dsn" not in merged_kwargs:
                    base_from_url = from_url_fn(url_val)
                    if broker is not None and "broker" not in merged_kwargs and "broker" in struct_fields:
                        merged_kwargs["broker"] = broker
                    if merged_kwargs and isinstance(base_from_url, msgspec.Struct):
                        return msgspec.structs.replace(base_from_url, **merged_kwargs)
                    return base_from_url
                if "dsn" in struct_fields and "dsn" not in merged_kwargs:
                    merged_kwargs["dsn"] = url_val

        if broker is not None and "broker" not in merged_kwargs and (not struct_fields or "broker" in struct_fields):
            merged_kwargs["broker"] = broker
        if config_cls is None:
            msg = f"{type(adapter).__name__} must define _config_cls or build_connection_config"
            raise TypeError(msg)
        return config_cls(**merged_kwargs)

    @classmethod
    def _derive_adapter_pool(cls, adapter: Any, overrides: Mapping[str, Any]) -> Any:
        """Clone `SyncAdapterConfig` or `AsyncAdapterConfig` with non-mutating field overrides."""
        merged_features = dict(overrides.get("driver_features", adapter.driver_features))
        merged_features.pop("_statement_cache_holder", None)
        feature_overrides = overrides.get("driver_features_overrides")
        if isinstance(feature_overrides, Mapping):
            merged_features.update(feature_overrides)

        adapter_kwargs: dict[str, Any] = {
            "bind_key": str(overrides.get("bind_key", adapter.bind_key)),
            "broker": overrides.get("broker", adapter.broker),
            "share_pool": bool(overrides.get("share_pool", adapter.share_pool)),
            "arrow_threshold_rows": int(overrides.get("arrow_threshold_rows", adapter.arrow_threshold_rows)),
            "statement_config": overrides.get("statement_config", adapter.statement_config),
            "migration_config": (
                dict(overrides["migration_config"])
                if isinstance(overrides.get("migration_config"), Mapping)
                else dict(adapter.migration_config)
                if isinstance(adapter.migration_config, Mapping)
                else adapter.migration_config
            ),
            "driver_features": merged_features,
            "extension_config": dict(overrides.get("extension_config", adapter.extension_config)),
            "observability_config": overrides.get("observability_config", adapter.observability_config),
        }

        init_params = inspect.signature(type(adapter).__init__).parameters
        if "is_replica" in init_params:
            adapter_kwargs["is_replica"] = bool(overrides.get("is_replica", False))
        if "replicas" in init_params:
            adapter_kwargs["replicas"] = None

        override_conn = overrides.get("connection_config")
        cfg_overrides: dict[str, Any] = dict(override_conn) if isinstance(override_conn, Mapping) else {}
        conn_overrides = overrides.get("connection_config_overrides")
        if isinstance(conn_overrides, Mapping):
            cfg_overrides.update(conn_overrides)
        cfg_overrides.update({k: v for k, v in overrides.items() if k not in cls._ADAPTER_META_KEYS})

        has_url_override = any(alias in cfg_overrides for alias in cls._URL_ALIAS_KEYS)
        if not has_url_override and isinstance(adapter.driver_config, msgspec.Struct):
            struct_fields = set(adapter.driver_config.__struct_fields__)
            struct_updates = {k: v for k, v in cfg_overrides.items() if k in struct_fields}
            extra_kwargs = {k: v for k, v in cfg_overrides.items() if k not in struct_fields}
            if "broker" in overrides and "broker" in struct_fields:
                struct_updates["broker"] = overrides["broker"]
            derived_cfg = (
                msgspec.structs.replace(adapter.driver_config, **struct_updates)
                if struct_updates
                else adapter.driver_config
            )
            merged_conn = dict(adapter.connection_config)
            merged_conn.update(cfg_overrides)
            return type(adapter)(
                derived_cfg,
                connection_config=merged_conn,
                **adapter_kwargs,
                **extra_kwargs,
            )

        merged_connection = dict(adapter.connection_config)
        if has_url_override:
            for alias in cls._URL_ALIAS_KEYS:
                merged_connection.pop(alias, None)
        elif not any(alias in merged_connection for alias in cls._URL_ALIAS_KEYS):
            struct_fields = set(getattr(adapter.driver_config, "__struct_fields__", ()))
            if "dsn" in struct_fields and getattr(adapter.driver_config, "dsn", None):
                merged_connection["dsn"] = adapter.driver_config.dsn
            elif "url" in struct_fields and getattr(adapter.driver_config, "url", None):
                merged_connection["url"] = adapter.driver_config.url
        merged_connection.update(cfg_overrides)
        return type(adapter)(
            **adapter_kwargs,
            **{k: v for k, v in merged_connection.items() if k not in cls._ADAPTER_META_KEYS},
        )


class SyncAdapterConfig[ConfigT, ConnectionT, PoolT, DriverT: SyncDriverAdapterBase](
    SyncDatabaseConfig[ConnectionT, PoolT, DriverT],
    _AdapterLifecycleMixin,
):
    """Synchronous SQLSpec database configuration backed by a Vertebra native driver."""

    __slots__ = ("_pool_is_shared", "arrow_threshold_rows", "broker", "driver_config", "share_pool")

    adapter_name: ClassVar[str | None] = None
    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = False
    connection_type: ClassVar[type[Any]]
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]
    driver_type: ClassVar[type[Any]]
    _connection_context_class: ClassVar[type[Any]] = _AdapterSyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = _AdapterSyncSessionContext

    def __init__(
        self,
        config: ConfigT | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous adapter configuration without mutating caller-supplied configs."""
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
        self._pool_is_shared: bool = False

        self.driver_config: ConfigT = cast(
            "ConfigT",
            self._resolve_adapter_driver_config(
                self,
                config,
                connection_config,
                kwargs,
                broker=self.broker,
            ),
        )
        resolved_stmt_config = (
            statement_config if statement_config is not None else self.driver_type._default_statement_config()
        )
        super().__init__(
            bind_key=bind_key,
            connection_config=dict(connection_config) if isinstance(connection_config, Mapping) else {},
            connection_instance=connection_instance,
            migration_config=cast("Any", migration_config),
            statement_config=resolved_stmt_config,
            driver_features=driver_features,
            extension_config=cast("Any", extension_config),
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )

    def _build_adapter_driver(self, native: Any, *, shared_pool: bool) -> DriverT:
        """Instantiate `driver_type` wrapping `native`."""
        return cast(
            "DriverT",
            self.driver_type(
                native,
                statement_config=self.statement_config,
                driver_features=dict(self.driver_features),
                shared_pool=shared_pool,
            ),
        )

    def create_driver(self) -> DriverT:
        """Construct a fresh synchronous adapter driver over the active pool."""
        native = self.provide_pool()
        return self._prepare_driver(self._build_adapter_driver(native, shared_pool=self._pool_is_shared))

    def create_connection(self) -> Any:
        """Create or return the underlying native synchronous Vertebra driver pool."""
        return self.provide_pool()

    def _provide_session_impl(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        """Build the synchronous session context manager forwarding `*args` and `**kwargs`."""
        factory_cls = getattr(self, "_session_factory_class", None)
        if factory_cls is not None:
            handler = factory_cls(self)
            return self._session_context_class(
                *args,
                acquire_connection=handler.acquire_connection,
                release_connection=handler.release_connection,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
                prepare_driver=self._prepare_driver,
                **kwargs,
            )
        if statement_config is not None:
            kwargs["statement_config"] = statement_config
        return self._session_context_class(self, *args, **kwargs)

    def _provide_connection_impl(self, *args: Any, **kwargs: Any) -> Any:
        """Build the synchronous connection context manager forwarding `*args` and `**kwargs`."""
        return self._connection_context_class(self, *args, **kwargs)

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractContextManager[DriverT]:
        """Yield a fresh synchronous SQLSpec adapter session, forwarding arguments to `_session_context_class`."""
        if bypass_tenant or tenant is not None:
            return super().provide_session(
                *args,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            )
        return cast(
            "AbstractContextManager[DriverT]",
            self._provide_session_impl(*args, statement_config=statement_config, **kwargs),
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractContextManager[Any]:
        """Yield a synchronous connection context, forwarding arguments to `_connection_context_class`."""
        return cast("AbstractContextManager[Any]", self._provide_connection_impl(*args, **kwargs))

    def _allow_replica_fallback(self, _error: Exception) -> bool:
        """Return whether a failed reader checkout may fall back to this primary."""
        return super()._allow_replica_fallback(_error)

    def provide_replica_session(
        self,
        *args: Any,
        replica_group: str | None = None,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractContextManager[DriverT]:
        """Provide a synchronous replica session with primary fallback support."""
        return super().provide_replica_session(
            *args,
            replica_group=replica_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_routing_session(
        self,
        *args: Any,
        replica_group: str | None = None,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> SyncRoutingSession[DriverT]:
        """Construct a lazy synchronous session routing reads to replicas and writes to primary."""
        return super().provide_routing_session(
            *args,
            replica_group=replica_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_replica_connection(
        self,
        *args: Any,
        replica_group: str | None = None,
        **kwargs: Any,
    ) -> AbstractContextManager[Any]:
        """Provide a synchronous replica connection with primary fallback support."""
        return super().provide_replica_connection(*args, replica_group=replica_group, **kwargs)

    def _create_pool(self) -> Any:
        """Acquire or instantiate the synchronous native pool."""
        native, is_shared = self._acquire_shared_native_driver(
            self.driver_config,
            self._driver_cls,
            share_pool=self.share_pool,
            broker=self.broker,
            is_async=False,
        )
        self._pool_is_shared = is_shared
        return native

    def _close_pool(self) -> None:
        """Release the synchronous pool reference and close owned or unreferenced native drivers."""
        native = self.connection_instance
        is_shared = self._pool_is_shared
        self.connection_instance = None
        self._pool_is_shared = False
        if native is None:
            return
        if is_shared or self._shared_native_refcount(native) > 0:
            self._release_shared_native_driver(native, close_when_unshared=True)
            return
        close_fn = getattr(native, "close", None)
        if callable(close_fn):
            close_fn()

    def close(self) -> None:
        """Close the primary pool and every configured replica pool."""
        self.close_pool()

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous adapter config with non-mutating field overrides."""
        return cast("Self", self._derive_adapter_pool(self, overrides))


class AsyncAdapterConfig[ConfigT, ConnectionT, PoolT, DriverT: AsyncDriverAdapterBase](
    AsyncDatabaseConfig[ConnectionT, PoolT, DriverT],
    _AdapterLifecycleMixin,
):
    """Asynchronous SQLSpec database configuration backed by a Vertebra native driver."""

    __slots__ = ("_pool_is_shared", "arrow_threshold_rows", "broker", "driver_config", "share_pool")

    adapter_name: ClassVar[str | None] = None
    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = True
    connection_type: ClassVar[type[Any]]
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]
    driver_type: ClassVar[type[Any]]
    _connection_context_class: ClassVar[type[Any]] = _AdapterAsyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = _AdapterAsyncSessionContext

    def __init__(
        self,
        config: ConfigT | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous adapter configuration without mutating caller-supplied configs."""
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
        self._pool_is_shared: bool = False

        self.driver_config: ConfigT = cast(
            "ConfigT",
            self._resolve_adapter_driver_config(
                self,
                config,
                connection_config,
                kwargs,
                broker=self.broker,
            ),
        )
        resolved_stmt_config = (
            statement_config if statement_config is not None else self.driver_type._default_statement_config()
        )
        super().__init__(
            bind_key=bind_key,
            connection_config=dict(connection_config) if isinstance(connection_config, Mapping) else {},
            connection_instance=connection_instance,
            migration_config=cast("Any", migration_config),
            statement_config=resolved_stmt_config,
            driver_features=driver_features,
            extension_config=cast("Any", extension_config),
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )

    def _build_adapter_driver(self, native: Any, *, shared_pool: bool) -> DriverT:
        """Instantiate `driver_type` wrapping `native`."""
        return cast(
            "DriverT",
            self.driver_type(
                native,
                statement_config=self.statement_config,
                driver_features=dict(self.driver_features),
                shared_pool=shared_pool,
            ),
        )

    def _acquire_pool_sync(self) -> Any:
        """Acquire and cache the underlying native async driver pool synchronously."""
        if self.connection_instance is not None:
            return self.connection_instance
        native, is_shared = self._acquire_shared_native_driver(
            self.driver_config,
            self._driver_cls,
            share_pool=self.share_pool,
            broker=self.broker,
            is_async=True,
        )
        self._pool_is_shared = is_shared
        self.connection_instance = native
        return native

    def create_driver(self) -> DriverT:
        """Construct a fresh asynchronous adapter driver over the active pool."""
        native = self._acquire_pool_sync()
        return self._prepare_driver(self._build_adapter_driver(native, shared_pool=self._pool_is_shared))

    async def create_connection(self) -> Any:
        """Create or return the underlying native asynchronous Vertebra driver pool."""
        return await self.provide_pool()

    def _provide_session_impl(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> Any:
        """Build the asynchronous session context manager forwarding `*args` and `**kwargs`."""
        factory_cls = getattr(self, "_session_factory_class", None)
        if factory_cls is not None:
            handler = factory_cls(self)
            return self._session_context_class(
                *args,
                acquire_connection=handler.acquire_connection,
                release_connection=handler.release_connection,
                statement_config=statement_config or self.statement_config,
                driver_features=self.driver_features,
                prepare_driver=self._prepare_driver,
                **kwargs,
            )
        if statement_config is not None:
            kwargs["statement_config"] = statement_config
        return self._session_context_class(self, *args, **kwargs)

    def _provide_connection_impl(self, *args: Any, **kwargs: Any) -> Any:
        """Build the asynchronous connection context manager forwarding `*args` and `**kwargs`."""
        return self._connection_context_class(self, *args, **kwargs)

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[DriverT]:
        """Yield a fresh asynchronous SQLSpec adapter session, forwarding arguments to `_session_context_class`."""
        if bypass_tenant or tenant is not None:
            return super().provide_session(
                *args,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            )
        return cast(
            "AbstractAsyncContextManager[DriverT]",
            self._provide_session_impl(*args, statement_config=statement_config, **kwargs),
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractAsyncContextManager[Any]:
        """Yield an asynchronous connection context, forwarding arguments to `_connection_context_class`."""
        return cast("AbstractAsyncContextManager[Any]", self._provide_connection_impl(*args, **kwargs))

    def _allow_replica_fallback(self, _error: Exception) -> bool:
        """Return whether a failed reader checkout may fall back to this primary."""
        return super()._allow_replica_fallback(_error)

    def provide_replica_session(
        self,
        *args: Any,
        replica_group: str | None = None,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[DriverT]:
        """Provide an asynchronous replica session with primary fallback support."""
        return super().provide_replica_session(
            *args,
            replica_group=replica_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_routing_session(
        self,
        *args: Any,
        replica_group: str | None = None,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AsyncRoutingSession[DriverT]:
        """Construct a lazy asynchronous session routing reads to replicas and writes to primary."""
        return super().provide_routing_session(
            *args,
            replica_group=replica_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_replica_connection(
        self,
        *args: Any,
        replica_group: str | None = None,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[Any]:
        """Provide an asynchronous replica connection with primary fallback support."""
        return super().provide_replica_connection(*args, replica_group=replica_group, **kwargs)

    async def _create_pool(self) -> Any:
        """Acquire or instantiate the asynchronous native pool."""
        native, is_shared = self._acquire_shared_native_driver(
            self.driver_config,
            self._driver_cls,
            share_pool=self.share_pool,
            broker=self.broker,
            is_async=True,
        )
        self._pool_is_shared = is_shared
        return native

    async def _close_pool(self) -> None:
        """Release the async pool reference and close owned or unreferenced native drivers."""
        native = self.connection_instance
        is_shared = self._pool_is_shared
        self.connection_instance = None
        self._pool_is_shared = False
        if native is None:
            return
        if is_shared or self._shared_native_refcount(native) > 0:
            await self._release_shared_native_driver_async(native, close_when_unshared=True)
            return
        close_fn = getattr(native, "close", None)
        if callable(close_fn):
            res = close_fn()
            if isinstance(res, Awaitable):
                await res

    async def close(self) -> None:
        """Close the primary async pool and every configured replica pool."""
        await self.close_pool()

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous adapter config with non-mutating field overrides."""
        return cast("Self", self._derive_adapter_pool(self, overrides))
