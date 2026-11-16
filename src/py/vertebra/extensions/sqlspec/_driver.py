"""Template-method SQLSpec dispatch bases and shared configuration wrappers over Vertebra drivers."""

import contextlib
import threading
from collections.abc import AsyncIterator, Callable, Collection, Iterator, Mapping, Sequence
from contextlib import asynccontextmanager, contextmanager
from typing import Any, ClassVar, Literal, Self, cast

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
from vertebra._vertebra import (
    IntegrityError as NativeIntegrityError,
)
from vertebra._vertebra import (
    NotFoundError as NativeNotFoundError,
)
from vertebra._vertebra import OperationalError as NativeOperationalError
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS, ArrowCursor
from vertebra.db.driver.capabilities import AsyncPipelineCapable, SyncPipelineCapable
from vertebra.db.registry import DatabaseRegistry
from vertebra.db.rls import TenantPredicate
from vertebra.typing import MissingDependencyError
from vertebra.utils.module_loader import import_optional
from vertebra.utils.serializers import ZeroCopyCodecRegistry

try:
    from sqlspec import SQLResult, StackResult, StatementStack
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.core import SQL, ParameterStyle, ParameterStyleConfig, StatementConfig, TypedParameter
    from sqlspec.core.cache import get_cache_config
    from sqlspec.core.result._base import ArrowResult, create_arrow_result
    from sqlspec.driver import (
        AsyncDriverAdapterBase,
        AsyncRowStream,
        ExecutionResult,
        SyncDriverAdapterBase,
        SyncRowStream,
    )
    from sqlspec.driver._common import validate_savepoint_name
    from sqlspec.driver._exception_handler import BaseAsyncExceptionHandler, BaseSyncExceptionHandler
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

__all__ = (
    "AsyncAdapterConfig",
    "BaseAsyncDriver",
    "BaseSyncDriver",
    "DeferredAsyncExceptionHandler",
    "DeferredSyncExceptionHandler",
    "SyncAdapterConfig",
    "acquire_shared_native_driver",
    "compile_many_transport",
    "merge_statement_config",
    "native_statement_config",
    "prepare_many_statement",
    "release_shared_native_driver",
    "release_shared_native_driver_async",
    "retain_shared_native_driver",
    "shared_native_refcount",
    "unwrap_typed_parameters",
)

type ExceptionMapper = Callable[[BaseException], Exception | None]

_SHARED_POOL_LOCK = threading.Lock()
_SHARED_POOL_REFS: dict[int, tuple[Any, int]] = {}


def retain_shared_native_driver(native_driver: Any) -> int:
    """Increment and return the active reference count for a shared native driver."""
    if native_driver is None:
        return 0
    key = id(native_driver)
    with _SHARED_POOL_LOCK:
        entry = _SHARED_POOL_REFS.get(key)
        next_count = (entry[1] if entry is not None else 0) + 1
        _SHARED_POOL_REFS[key] = (native_driver, next_count)
        return next_count


def shared_native_refcount(native_driver: Any) -> int:
    """Return the active shared-pool reference count for `native_driver`."""
    if native_driver is None:
        return 0
    with _SHARED_POOL_LOCK:
        entry = _SHARED_POOL_REFS.get(id(native_driver))
        return entry[1] if entry is not None else 0


def _decrement_shared_native_refcount(native_driver: Any) -> bool:
    """Decrement a shared driver's reference count and report whether its underlying pool should close."""
    if native_driver is None:
        return False
    key = id(native_driver)
    with _SHARED_POOL_LOCK:
        entry = _SHARED_POOL_REFS.get(key)
        if entry is None:
            return True
        next_count = entry[1] - 1
        if next_count > 0:
            _SHARED_POOL_REFS[key] = (native_driver, next_count)
            return False
        _SHARED_POOL_REFS.pop(key, None)
        return True


def _evict_closed_from_default_registry(native_driver: Any) -> None:
    """Remove a closed shared driver entry from the default `DatabaseRegistry` caches."""
    reg = DatabaseRegistry.default()
    with reg._lock:
        for cache in (reg._shared_sync_drivers, reg._shared_async_drivers):
            stale_keys = [cache_key for cache_key, cached in cache.items() if cached is native_driver]
            for cache_key in stale_keys:
                cache.pop(cache_key, None)


def acquire_shared_native_driver(
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
        retain_shared_native_driver(native)
        return native, True
    return driver_cls(driver_config, broker=broker), False


def release_shared_native_driver(native_driver: Any, *, close_when_unshared: bool = True) -> bool:
    """Release one reference to a synchronous native driver, closing it when the last reference exits."""
    should_close = _decrement_shared_native_refcount(native_driver)
    if not should_close or not close_when_unshared or native_driver is None:
        return False
    close_fn = getattr(native_driver, "close", None)
    if callable(close_fn):
        close_fn()
    _evict_closed_from_default_registry(native_driver)
    return True


async def release_shared_native_driver_async(native_driver: Any, *, close_when_unshared: bool = True) -> bool:
    """Release one reference to an asynchronous native driver, closing it when the last reference exits."""
    should_close = _decrement_shared_native_refcount(native_driver)
    if not should_close or not close_when_unshared or native_driver is None:
        return False
    close_fn = getattr(native_driver, "close", None)
    if callable(close_fn):
        result = close_fn()
        if hasattr(result, "__await__"):
            await result
    _evict_closed_from_default_registry(native_driver)
    return True


def unwrap_typed_parameters(bound: Any) -> Any:
    """Unwrap `TypedParameter` wrappers recursively across dicts, sequences, and scalars."""
    if isinstance(bound, dict):
        return {key: unwrap_typed_parameters(value) for key, value in bound.items()}
    if isinstance(bound, tuple):
        return tuple(unwrap_typed_parameters(value) for value in bound)
    if isinstance(bound, list):
        return [unwrap_typed_parameters(value) for value in bound]
    if isinstance(bound, TypedParameter):
        return bound.value
    return bound


def _normalize_bound_params(params: Any) -> Any:
    """Normalize empty bound containers to `None` while keeping populated payloads intact."""
    if params is None:
        return None
    if isinstance(params, (dict, list, tuple)) and len(params) == 0:
        return None
    return params


def _normalize_statement_params(parameters: tuple[Any, ...]) -> Any:
    """Normalize positional vararg tuples into single dicts/sequences or tuples."""
    if not parameters:
        return None
    if len(parameters) == 1 and isinstance(parameters[0], (dict, list, tuple)):
        return parameters[0]
    return parameters


def merge_statement_config(
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


def native_statement_config(
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


def _is_google_api_exception(exc: BaseException) -> bool:
    """Check whether `exc` originates from `google.api_core` or `google.cloud` without importing them."""
    return any(cls.__module__.startswith(("google.api_core", "google.cloud")) for cls in type(exc).__mro__)


def map_native_database_exception(exc: BaseException) -> Exception | None:
    """Map Vertebra native `DatabaseError` / `GoogleCloudError` hierarchy exceptions to `sqlspec.exceptions.*`."""
    if isinstance(exc, SQLSpecError):
        return None
    if not isinstance(exc, (DatabaseError, GoogleCloudError)) and not _is_google_api_exception(exc):
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
        if "unique" in lower or "duplicate" in lower or sqlstate == "23505" or "ora-00001" in lower or "1062" in lower:
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

    if isinstance(exc, (BigQueryError, SpannerError, GoogleCloudError)) or _is_google_api_exception(exc):
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


class DeferredSyncExceptionHandler(BaseSyncExceptionHandler):
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
            mapped = map_native_database_exception(exc_val)
        if mapped is None:
            return False
        self.pending_exception = mapped
        return True


class DeferredAsyncExceptionHandler(BaseAsyncExceptionHandler):
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
            mapped = map_native_database_exception(exc_val)
        if mapped is None:
            return False
        self.pending_exception = mapped
        return True


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


def _table_row_count(table: Any) -> int:
    """Extract row count from an Arrow table, batch, or sequence."""
    if hasattr(table, "num_rows"):
        return int(table.num_rows)
    if hasattr(table, "__len__"):
        return len(table)
    return 0


class _VertebraSyncRowSource:
    """Chunked row source consuming batches from a synchronous Vertebra driver."""

    __slots__ = ("_driver", "_iterator", "_params", "_sql", "_chunk_size")

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

    __slots__ = ("_driver", "_iterator", "_params", "_sql", "_chunk_size")

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
            await aclose_fn()
        self._iterator = None


class BaseSyncDriver(SyncDriverAdapterBase):
    """Base synchronous SQLSpec adapter implementing template-method execution over Vertebra drivers."""

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

    @property
    def data_dictionary(self) -> Any:
        """Return adapter metadata dictionary or `None` when generic."""
        return None

    def _map_exception(self, exc: BaseException) -> Exception | None:
        """Override in engine drivers to apply engine-specific error code mappings before fallback."""
        _ = exc
        return None

    def handle_database_exceptions(self) -> DeferredSyncExceptionHandler:
        """Return synchronous deferred exception handler compatible with SQLSpec's dispatcher."""
        return DeferredSyncExceptionHandler(self._map_exception)

    @contextmanager
    def with_cursor(self, connection: Any) -> Iterator[Any]:
        """Yield `connection` directly so `cursor is self.connection` routes cache hits through `dispatch_execute`."""
        yield connection

    def _connection_in_transaction(self) -> bool:
        """Report whether the wrapped native driver currently has an active transaction."""
        return bool(getattr(self.connection, "in_transaction", False))

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        """Allow subclasses to adjust final compiled SQL and unwrapped bind parameters before driver dispatch."""
        _ = statement
        return sql, params

    def _compile_for_native(self, statement: SQL) -> tuple[str, Any]:
        """Compile `statement` honoring its effective `StatementConfig` and unwrap `TypedParameter` nodes."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        params = _normalize_bound_params(unwrap_typed_parameters(prepared))
        return self._prepare_native_parameters(sql, params, statement)

    def _fetch_rows_with_columns(self, cursor: Any, sql: str, params: Any) -> tuple[list[dict[str, Any]], list[str]]:
        """Execute a row-returning query preserving ordered column names even on empty result sets."""
        query_fn = getattr(cursor, "query_with_columns", None)
        if callable(query_fn):
            rows, cols = query_fn(sql, params) if params is not None else query_fn(sql)
            return list(rows), list(cols)
        desc_fn = getattr(cursor, "execute_with_description", None)
        if callable(desc_fn):
            raw_rows, raw_cols = desc_fn(sql, params) if params is not None else desc_fn(sql)
            cols = [str(col) for col in raw_cols]
            hydrate_fn = getattr(cursor, "hydrate_rows", None)
            rows = (
                hydrate_fn(raw_rows, cols, None)
                if callable(hydrate_fn)
                else [dict(zip(cols, row, strict=False)) for row in raw_rows]
            )
            return list(rows), cols
        raw_rows = cursor.select(sql, params) if params is not None else cursor.select(sql)
        rows = list(raw_rows)
        cols = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
        return rows, cols

    def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single compiled statement against the Vertebra native driver."""
        sql, params = self._compile_for_native(statement)
        if statement.returns_rows():
            rows, cols = self._fetch_rows_with_columns(cursor, sql, params)
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=cols,
                data_row_count=len(rows),
                is_select_result=True,
                row_format="dict",
            )
        raw = cursor.execute(sql, params) if params is not None else cursor.execute(sql)
        rowcount = len(raw) if isinstance(raw, list) else (int(raw) if raw is not None else 0)
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single statement across multiple compiled parameter sets."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        unwrapped = unwrap_typed_parameters(prepared)
        param_sets = list(unwrapped) if unwrapped is not None else []
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            res = exec_many_fn(sql, param_sets)
            rowcount = int(res) if res is not None else 0
        else:
            rowcount = 0
            for item in param_sets:
                res = cursor.execute(sql, item)
                rowcount += len(res) if isinstance(res, list) else (int(res) if res is not None else 0)
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

    def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Split a SQL script via SQLGlot dialect rules and dispatch each statement sequentially."""
        return super().dispatch_execute_script(cursor, statement)

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> SyncRowStream[dict[str, Any]] | None:
        """Return a native chunked `SyncRowStream` when the underlying driver provides `stream_arrow_batches`."""
        if self._prefer_row_arrow_fallback or not callable(getattr(self.connection, "stream_arrow_batches", None)):
            return None
        sql, params = self._compile_for_native(statement)
        return SyncRowStream(_VertebraSyncRowSource(self.connection, sql, params, chunk_size))

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
            sql, params = self._compile_for_native(sql_statement)
            raw = arrow_fn(sql, params) if params is not None else arrow_fn(sql)
            arrow_data = _coerce_arrow_payload(
                raw,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )
            rows_affected = raw.row_count if isinstance(raw, ArrowCursor) else _table_row_count(arrow_data)
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
        sql, params = self._compile_for_native(sql_statement)
        stream_fn = getattr(self.connection, "stream_arrow_batches", None)
        if callable(stream_fn):
            yield from stream_fn(sql, params, batch_size=batch_size)
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
                sql, params = self._compile_for_native(prepared_stmt)
                prepared.append((prepared_stmt, sql, params))
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
        if self._shared_pool:
            release_shared_native_driver(self.connection, close_when_unshared=True)
            return
        if shared_native_refcount(self.connection) > 0:
            release_shared_native_driver(self.connection, close_when_unshared=True)
            return
        close_fn = getattr(self.connection, "close", None)
        if callable(close_fn):
            close_fn()

    def collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        """Collect already-hydrated rows for direct cache-hit paths."""
        _ = cursor
        if not fetched:
            return [], [], 0
        if isinstance(fetched[0], dict):
            return fetched, list(fetched[0].keys()), len(fetched)
        return fetched, [], len(fetched)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve affected row count from native driver cursor attributes."""
        raw = getattr(cursor, "rowcount", 0)
        return int(raw) if isinstance(raw, int) and raw >= 0 else 0

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build a default `StatementConfig` for this synchronous adapter."""
        return native_statement_config(str(cls.dialect or "generic"), cls._execution_parameter_style)

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

    @property
    def data_dictionary(self) -> Any:
        """Return async adapter metadata dictionary or `None` when generic."""
        return None

    def _map_exception(self, exc: BaseException) -> Exception | None:
        """Override in engine drivers to apply engine-specific error code mappings before fallback."""
        _ = exc
        return None

    def handle_database_exceptions(self) -> DeferredAsyncExceptionHandler:
        """Return asynchronous deferred exception handler compatible with SQLSpec's dispatcher."""
        return DeferredAsyncExceptionHandler(self._map_exception)

    @asynccontextmanager
    async def with_cursor(self, connection: Any) -> AsyncIterator[Any]:
        """Yield `connection` directly so `cursor is self.connection` routes cache hits through `dispatch_execute`."""
        yield connection

    def _connection_in_transaction(self) -> bool:
        """Report whether the wrapped async native driver currently has an active transaction."""
        return bool(getattr(self.connection, "in_transaction", False))

    def _prepare_native_parameters(self, sql: str, params: Any, statement: SQL) -> tuple[str, Any]:
        """Allow subclasses to adjust final compiled SQL and unwrapped bind parameters before driver dispatch."""
        _ = statement
        return sql, params

    def _compile_for_native(self, statement: SQL) -> tuple[str, Any]:
        """Compile `statement` honoring its effective `StatementConfig` and unwrap `TypedParameter` nodes."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        params = _normalize_bound_params(unwrap_typed_parameters(prepared))
        return self._prepare_native_parameters(sql, params, statement)

    async def _fetch_rows_with_columns(
        self, cursor: Any, sql: str, params: Any
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Execute an async row-returning query preserving ordered column names on empty result sets."""
        query_fn = getattr(cursor, "query_with_columns", None)
        if callable(query_fn):
            rows, cols = await (query_fn(sql, params) if params is not None else query_fn(sql))
            return list(rows), list(cols)
        desc_fn = getattr(cursor, "execute_with_description", None)
        if callable(desc_fn):
            raw_rows, raw_cols = await (desc_fn(sql, params) if params is not None else desc_fn(sql))
            cols = [str(col) for col in raw_cols]
            hydrate_fn = getattr(cursor, "hydrate_rows", None)
            rows = (
                hydrate_fn(raw_rows, cols, None)
                if callable(hydrate_fn)
                else [dict(zip(cols, row, strict=False)) for row in raw_rows]
            )
            return list(rows), cols
        raw_rows = await (cursor.select(sql, params) if params is not None else cursor.select(sql))
        rows = list(raw_rows)
        cols = list(rows[0].keys()) if rows and isinstance(rows[0], dict) else []
        return rows, cols

    async def dispatch_execute(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single compiled statement asynchronously against the Vertebra native driver."""
        sql, params = self._compile_for_native(statement)
        if statement.returns_rows():
            rows, cols = await self._fetch_rows_with_columns(cursor, sql, params)
            return self.create_execution_result(
                cursor,
                selected_data=rows,
                column_names=cols,
                data_row_count=len(rows),
                is_select_result=True,
                row_format="dict",
            )
        raw = await (cursor.execute(sql, params) if params is not None else cursor.execute(sql))
        rowcount = len(raw) if isinstance(raw, list) else (int(raw) if raw is not None else 0)
        return self.create_execution_result(cursor, rowcount_override=rowcount)

    async def dispatch_execute_many(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Execute a single statement asynchronously across multiple compiled parameter sets."""
        effective_config = getattr(statement, "statement_config", None) or self.statement_config
        sql, prepared = self._compiled_sql(statement, effective_config)
        unwrapped = unwrap_typed_parameters(prepared)
        param_sets = list(unwrapped) if unwrapped is not None else []
        if not param_sets:
            return self.create_execution_result(cursor, rowcount_override=0, is_many_result=True)
        exec_many_fn = getattr(cursor, "execute_many", getattr(cursor, "executemany", None))
        if callable(exec_many_fn):
            res = await exec_many_fn(sql, param_sets)
            rowcount = int(res) if res is not None else 0
        else:
            rowcount = 0
            for item in param_sets:
                res = await cursor.execute(sql, item)
                rowcount += len(res) if isinstance(res, list) else (int(res) if res is not None else 0)
        return self.create_execution_result(cursor, rowcount_override=rowcount, is_many_result=True)

    async def dispatch_execute_script(self, cursor: Any, statement: SQL) -> ExecutionResult:
        """Split a SQL script via SQLGlot dialect rules and dispatch each statement sequentially."""
        return await super().dispatch_execute_script(cursor, statement)

    def dispatch_select_stream(self, statement: SQL, chunk_size: int) -> AsyncRowStream[dict[str, Any]] | None:
        """Return a native chunked `AsyncRowStream` when the underlying driver provides `stream_arrow_batches`."""
        if self._prefer_row_arrow_fallback or not callable(getattr(self.connection, "stream_arrow_batches", None)):
            return None
        sql, params = self._compile_for_native(statement)
        return AsyncRowStream(_VertebraAsyncRowSource(self.connection, sql, params, chunk_size))

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
            sql, params = self._compile_for_native(sql_statement)
            raw = await (arrow_fn(sql, params) if params is not None else arrow_fn(sql))
            arrow_data = _coerce_arrow_payload(
                raw,
                return_format=return_format,
                batch_size=batch_size,
                arrow_schema=arrow_schema,
            )
            rows_affected = raw.row_count if isinstance(raw, ArrowCursor) else _table_row_count(arrow_data)
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
        sql, params = self._compile_for_native(sql_statement)
        stream_fn = getattr(self.connection, "stream_arrow_batches", None)
        if callable(stream_fn):
            async for batch in stream_fn(sql, params, batch_size=batch_size):
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
                sql, params = self._compile_for_native(prepared_stmt)
                prepared.append((prepared_stmt, sql, params))
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
            await begin_fn()

    async def commit(self) -> None:
        """Commit the active transaction on the native async Vertebra driver."""
        commit_fn = getattr(self.connection, "commit", None)
        if callable(commit_fn):
            await commit_fn()

    async def rollback(self) -> None:
        """Roll back the active transaction on the native async Vertebra driver."""
        rollback_fn = getattr(self.connection, "rollback", None)
        if callable(rollback_fn):
            await rollback_fn()

    async def create_savepoint(self, name: str) -> None:
        """Create a named savepoint via the async native driver or ANSI `SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "create_savepoint", None)
        if callable(fn):
            await fn(valid)
            return
        await self.connection.execute(f"SAVEPOINT {valid}")

    async def release_savepoint(self, name: str) -> None:
        """Release a named savepoint via the async native driver or ANSI `RELEASE SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "release_savepoint", None)
        if callable(fn):
            await fn(valid)
            return
        await self.connection.execute(f"RELEASE SAVEPOINT {valid}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Roll back to a named savepoint via the async native driver or ANSI `ROLLBACK TO SAVEPOINT` SQL."""
        valid = validate_savepoint_name(name)
        fn = getattr(self.connection, "rollback_to_savepoint", None)
        if callable(fn):
            await fn(valid)
            return
        await self.connection.execute(f"ROLLBACK TO SAVEPOINT {valid}")

    async def close(self) -> None:
        """Release the underlying async native driver, respecting shared-pool reference counts."""
        if self._driver_closed:
            return
        self._driver_closed = True
        if self._shared_pool or shared_native_refcount(self.connection) > 0:
            await release_shared_native_driver_async(self.connection, close_when_unshared=True)
            return
        close_fn = getattr(self.connection, "close", None)
        if callable(close_fn):
            res = close_fn()
            if hasattr(res, "__await__"):
                await res

    def collect_rows(self, cursor: Any, fetched: list[Any]) -> tuple[list[Any], list[str], int]:
        """Collect already-hydrated rows for direct cache-hit paths."""
        _ = cursor
        if not fetched:
            return [], [], 0
        if isinstance(fetched[0], dict):
            return fetched, list(fetched[0].keys()), len(fetched)
        return fetched, [], len(fetched)

    def resolve_rowcount(self, cursor: Any) -> int:
        """Resolve affected row count from native driver cursor attributes."""
        raw = getattr(cursor, "rowcount", 0)
        return int(raw) if isinstance(raw, int) and raw >= 0 else 0

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        """Build a default `StatementConfig` for this asynchronous adapter."""
        return native_statement_config(str(cls.dialect or "generic"), cls._execution_parameter_style)

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


_ADAPTER_META_KEYS: frozenset[str] = frozenset(
    {
        "bind_key",
        "broker",
        "share_pool",
        "arrow_threshold_rows",
        "statement_config",
        "migration_config",
        "driver_features",
        "extension_config",
        "observability_config",
        "connection_config",
    }
)
_URL_ALIAS_KEYS: tuple[str, ...] = ("url", "dsn", "conninfo", "connection_string")


def _resolve_adapter_driver_config[ConfigT](
    adapter: Any,
    config: ConfigT | Any | None,
    connection_config: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
    *,
    broker: Any,
) -> ConfigT:
    """Resolve `driver_config` invoking subclass `build_connection_config` when defined."""
    if config is not None and not isinstance(config, (str, Mapping)):
        return cast("ConfigT", config)

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
                    for alias in _URL_ALIAS_KEYS:
                        candidate = source.get(alias)
                        if isinstance(candidate, str) and candidate.strip():
                            raw_url = candidate
                            break
                if raw_url is not None:
                    break

        if raw_url is not None:
            extra_fields = {k: v for k, v in merged_kwargs.items() if k not in _URL_ALIAS_KEYS}
            base_cfg = build_fn(raw_url, None, {})
            if broker is not None and "broker" not in extra_fields and hasattr(base_cfg, "broker"):
                extra_fields["broker"] = broker
            if extra_fields:
                if isinstance(base_cfg, msgspec.Struct):
                    return cast("ConfigT", msgspec.structs.replace(base_cfg, **extra_fields))
                return cast("ConfigT", build_fn(raw_url, None, extra_fields))
            return cast("ConfigT", base_cfg)

        if broker is not None and "broker" not in merged_kwargs and (not struct_fields or "broker" in struct_fields):
            merged_kwargs["broker"] = broker
        return cast("ConfigT", build_fn(None, None, merged_kwargs))

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
                    return cast("ConfigT", msgspec.structs.replace(base_from_url, **merged_kwargs))
                return cast("ConfigT", base_from_url)
            if "dsn" in struct_fields and "dsn" not in merged_kwargs:
                merged_kwargs["dsn"] = url_val

    if broker is not None and "broker" not in merged_kwargs and (not struct_fields or "broker" in struct_fields):
        merged_kwargs["broker"] = broker
    if config_cls is None:
        msg = f"{type(adapter).__name__} must define _config_cls or build_connection_config"
        raise TypeError(msg)
    return cast("ConfigT", config_cls(**merged_kwargs))


def _derive_adapter_pool(adapter: Any, overrides: Mapping[str, Any]) -> Any:
    """Clone `SyncAdapterConfig` or `AsyncAdapterConfig` with non-mutating field overrides."""
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
        "driver_features": dict(overrides.get("driver_features", adapter.driver_features)),
        "extension_config": dict(overrides.get("extension_config", adapter.extension_config)),
        "observability_config": overrides.get("observability_config", adapter.observability_config),
    }

    override_conn = overrides.get("connection_config")
    cfg_overrides: dict[str, Any] = dict(override_conn) if isinstance(override_conn, Mapping) else {}
    cfg_overrides.update({k: v for k, v in overrides.items() if k not in _ADAPTER_META_KEYS})

    has_url_override = any(alias in cfg_overrides for alias in _URL_ALIAS_KEYS)
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
        for alias in _URL_ALIAS_KEYS:
            merged_connection.pop(alias, None)
    elif not any(alias in merged_connection for alias in _URL_ALIAS_KEYS):
        struct_fields = set(getattr(adapter.driver_config, "__struct_fields__", ()))
        if "dsn" in struct_fields and getattr(adapter.driver_config, "dsn", None):
            merged_connection["dsn"] = adapter.driver_config.dsn
        elif "url" in struct_fields and getattr(adapter.driver_config, "url", None):
            merged_connection["url"] = adapter.driver_config.url
    merged_connection.update(cfg_overrides)
    return type(adapter)(
        **adapter_kwargs,
        **{k: v for k, v in merged_connection.items() if k not in _ADAPTER_META_KEYS},
    )


class SyncAdapterConfig[ConfigT, DriverT: SyncDriverAdapterBase](SyncDatabaseConfig[Any, Any, DriverT]):
    """Synchronous SQLSpec database configuration backed by a Vertebra native driver."""

    adapter_name: ClassVar[str | None] = None
    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = False
    connection_type: ClassVar[type[Any]]
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]
    driver_type: ClassVar[type[Any]]

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
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous adapter configuration without mutating caller-supplied configs."""
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

        self.driver_config: ConfigT = _resolve_adapter_driver_config(
            self,
            config,
            connection_config,
            kwargs,
            broker=self.broker,
        )
        self._driver_instance: DriverT | None = None
        resolved_stmt_config = (
            statement_config if statement_config is not None else self.driver_type._default_statement_config()
        )
        super().__init__(
            bind_key=bind_key,
            connection_config=dict(connection_config) if isinstance(connection_config, Mapping) else {},
            migration_config=cast("Any", migration_config),
            statement_config=resolved_stmt_config,
            driver_features=driver_features,
            extension_config=cast("Any", extension_config),
            observability_config=observability_config,
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
        """Instantiate or return the cached synchronous adapter driver with shared-pool refcounting."""
        if self._driver_instance is None or getattr(self._driver_instance, "_driver_closed", False):
            native, is_shared = acquire_shared_native_driver(
                self.driver_config,
                self._driver_cls,
                share_pool=self.share_pool,
                broker=self.broker,
                is_async=False,
            )
            self._driver_instance = self._build_adapter_driver(native, shared_pool=is_shared)
        return self._driver_instance

    def create_connection(self) -> Any:
        """Create or return the underlying native synchronous Vertebra driver."""
        return self.create_driver().connection

    @contextmanager
    def provide_session(self, *args: Any, **kwargs: Any) -> Iterator[DriverT]:
        """Yield a synchronous SQLSpec adapter session, rolling back active transactions on exception."""
        _ = (args, kwargs)
        driver = self.create_driver()
        try:
            yield driver
        except Exception:
            in_tx_fn = getattr(driver, "_connection_in_transaction", None)
            in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
            if in_tx:
                with contextlib.suppress(Exception):
                    driver.rollback()
            raise

    @contextmanager
    def provide_connection(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        """Yield the underlying native driver connection."""
        _ = (args, kwargs)
        driver = self.create_driver()
        yield getattr(driver, "connection", driver)

    def create_pool(self) -> Any:
        """Create and return the shared native driver pool."""
        return self.create_driver()

    def close(self) -> None:
        """Release one reference on the cached driver and close shared pools when unreferenced."""
        if self._driver_instance is not None:
            close_fn = getattr(self._driver_instance, "close", None)
            if callable(close_fn):
                close_fn()
            self._driver_instance = None

    def close_pool(self) -> None:
        """Release the adapter pool reference."""
        self.close()

    def provide_pool(self, *args: Any, **kwargs: Any) -> Any:
        """Return the active pool driver for SQLSpec lifecycle hooks."""
        _ = (args, kwargs)
        return self.create_driver()

    def _create_pool(self) -> Any:
        """Return the instantiated driver as pool."""
        return self.create_driver()

    def _close_pool(self) -> None:
        """Close the cached driver reference."""
        self.close()

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous adapter config with non-mutating field overrides."""
        return cast("Self", _derive_adapter_pool(self, overrides))


class AsyncAdapterConfig[ConfigT, DriverT: AsyncDriverAdapterBase](AsyncDatabaseConfig[Any, Any, DriverT]):
    """Asynchronous SQLSpec database configuration backed by a Vertebra native driver."""

    adapter_name: ClassVar[str | None] = None
    dialect: ClassVar[str] = "generic"
    is_async: ClassVar[bool] = True
    connection_type: ClassVar[type[Any]]
    _config_cls: ClassVar[type[Any]]
    _driver_cls: ClassVar[type[Any]]
    driver_type: ClassVar[type[Any]]

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
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous adapter configuration without mutating caller-supplied configs."""
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

        self.driver_config: ConfigT = _resolve_adapter_driver_config(
            self,
            config,
            connection_config,
            kwargs,
            broker=self.broker,
        )
        self._driver_instance: DriverT | None = None
        resolved_stmt_config = (
            statement_config if statement_config is not None else self.driver_type._default_statement_config()
        )
        super().__init__(
            bind_key=bind_key,
            connection_config=dict(connection_config) if isinstance(connection_config, Mapping) else {},
            migration_config=cast("Any", migration_config),
            statement_config=resolved_stmt_config,
            driver_features=driver_features,
            extension_config=cast("Any", extension_config),
            observability_config=observability_config,
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
        """Instantiate or return the cached asynchronous adapter driver with shared-pool refcounting."""
        if self._driver_instance is None or getattr(self._driver_instance, "_driver_closed", False):
            native, is_shared = acquire_shared_native_driver(
                self.driver_config,
                self._driver_cls,
                share_pool=self.share_pool,
                broker=self.broker,
                is_async=True,
            )
            self._driver_instance = self._build_adapter_driver(native, shared_pool=is_shared)
        return self._driver_instance

    async def create_connection(self) -> Any:
        """Create or return the underlying native asynchronous Vertebra driver."""
        return self.create_driver().connection

    @asynccontextmanager
    async def provide_session(self, *args: Any, **kwargs: Any) -> AsyncIterator[DriverT]:
        """Yield an asynchronous SQLSpec adapter session, rolling back active transactions on exception."""
        _ = (args, kwargs)
        driver = self.create_driver()
        try:
            yield driver
        except Exception:
            in_tx_fn = getattr(driver, "_connection_in_transaction", None)
            in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
            if in_tx:
                with CancelScope(shield=True), contextlib.suppress(Exception):
                    res = driver.rollback()
                    if hasattr(res, "__await__"):
                        await res
            raise

    @asynccontextmanager
    async def provide_connection(self, *args: Any, **kwargs: Any) -> AsyncIterator[Any]:
        """Yield the underlying native driver connection asynchronously."""
        _ = (args, kwargs)
        driver = self.create_driver()
        yield getattr(driver, "connection", driver)

    async def create_pool(self) -> Any:
        """Create and return the shared async native driver pool."""
        return self.create_driver()

    async def close(self) -> None:
        """Release one reference on the cached async driver and close shared pools when unreferenced."""
        if self._driver_instance is not None:
            close_fn = getattr(self._driver_instance, "close", None)
            if callable(close_fn):
                res = close_fn()
                if hasattr(res, "__await__"):
                    await res
            self._driver_instance = None

    async def close_pool(self) -> None:
        """Release the async adapter pool reference."""
        await self.close()

    async def provide_pool(self, *args: Any, **kwargs: Any) -> Any:
        """Return the active async pool driver for SQLSpec lifecycle hooks."""
        _ = (args, kwargs)
        return self.create_driver()

    async def _create_pool(self) -> Any:
        """Return the instantiated async driver as pool."""
        return self.create_driver()

    async def _close_pool(self) -> None:
        """Close the cached async driver reference."""
        await self.close()

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous adapter config with non-mutating field overrides."""
        return cast("Self", _derive_adapter_pool(self, overrides))


def prepare_many_statement(
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


def compile_many_transport(
    driver: SyncDriverAdapterBase | AsyncDriverAdapterBase,
    statement: Any,
    parameters: Sequence[Any],
    filters: tuple[Any, ...] = (),
    *,
    statement_config: StatementConfig | None = None,
    kwargs: dict[str, Any] | None = None,
) -> tuple[SQL, str, list[Any]]:
    """Compile a batched statement and unwrap TypedParameter values across every parameter set."""
    prepared = prepare_many_statement(
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
    return prepared, sql, list(unwrap_typed_parameters(bound_sets))
