"""Unit tests for SQLSpec BaseSyncDriver/BaseAsyncDriver template-method dispatch and error mapping."""

from collections.abc import Mapping, Sequence
from typing import Any

import pytest
from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig, TypedParameter
from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    DialectNotSupportedError,
    ForeignKeyViolationError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    SerializationConflictError,
    SQLParsingError,
    UniqueViolationError,
)

from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    ConnectionFailure,
    GoogleCloudAuthError,
    GoogleCloudError,
    NotFoundError,
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
from vertebra._vertebra import OperationalError as NativeOperationalError
from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.db.driver import AsyncDatabaseDriver, SyncDatabaseDriver
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import (
    BaseAsyncDriver,
    BaseSyncDriver,
    DeferredAsyncExceptionHandler,
    DeferredSyncExceptionHandler,
    acquire_shared_native_driver,
    merge_statement_config,
    native_statement_config,
    release_shared_native_driver_async,
    retain_shared_native_driver,
    shared_native_refcount,
    unwrap_typed_parameters,
)


class _RecordingSyncNativeDriver(SyncDatabaseDriver[Any]):
    """Minimal native sync driver double recording calls for BaseSyncDriver verification."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, columns: list[str] | None = None) -> None:
        self._in_tx = False
        self._closed = False
        self.calls: list[tuple[str, str, Any]] = []
        self._rows = list(rows or [])
        self._columns = list(columns or [])

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_transaction(self) -> bool:
        return self._in_tx

    def open(self) -> None:
        self._closed = False

    def close(self) -> None:
        self._closed = True

    def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        self.calls.append(("execute", sql, parameters))
        return 1

    def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]] = (),
        **kwargs: Any,
    ) -> int:
        _ = kwargs
        batch = list(parameter_sets)
        self.calls.append(("execute_many", sql, batch))
        return len(batch)

    def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        tuple_rows = [tuple(r.get(c) for c in self._columns) for r in self._rows]
        return tuple_rows, tuple(self._columns)

    def begin(self) -> None:
        self._in_tx = True

    def commit(self) -> None:
        self._in_tx = False

    def rollback(self) -> None:
        self._in_tx = False


class _RecordingAsyncNativeDriver(AsyncDatabaseDriver[Any]):
    """Minimal native async driver double recording calls for BaseAsyncDriver verification."""

    def __init__(self, rows: list[dict[str, Any]] | None = None, columns: list[str] | None = None) -> None:
        self._in_tx = False
        self._closed = False
        self.calls: list[tuple[str, str, Any]] = []
        self._rows = list(rows or [])
        self._columns = list(columns or [])

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def in_transaction(self) -> bool:
        return self._in_tx

    async def open(self) -> None:
        self._closed = False

    async def close(self) -> None:
        self._closed = True

    async def execute(self, sql: str, parameters: Any = None, **kwargs: Any) -> int:
        _ = kwargs
        self.calls.append(("execute", sql, parameters))
        return 1

    async def execute_many(
        self,
        sql: str,
        parameter_sets: Sequence[Sequence[Any] | Mapping[str, Any]] = (),
        **kwargs: Any,
    ) -> int:
        _ = kwargs
        batch = list(parameter_sets)
        self.calls.append(("execute_many", sql, batch))
        return len(batch)

    async def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        _ = kwargs
        self.calls.append(("execute_with_description", sql, parameters))
        tuple_rows = [tuple(r.get(c) for c in self._columns) for r in self._rows]
        return tuple_rows, tuple(self._columns)

    async def begin(self) -> None:
        self._in_tx = True

    async def commit(self) -> None:
        self._in_tx = False

    async def rollback(self) -> None:
        self._in_tx = False


class _SampleSyncAdapter(BaseSyncDriver):
    """Concrete BaseSyncDriver subclass for sqlite qmark execution."""

    dialect = "sqlite"

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        return native_statement_config("sqlite", ParameterStyle.QMARK)


class _SampleAsyncAdapter(BaseAsyncDriver):
    """Concrete BaseAsyncDriver subclass for sqlite qmark execution."""

    dialect = "sqlite"

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        return native_statement_config("sqlite", ParameterStyle.QMARK)


def test_base_drivers_inherit_template_dispatch_and_with_cursor_cache_contract() -> None:
    """BaseSyncDriver and BaseAsyncDriver inherit upstream execute/select and yield connection in with_cursor."""
    sync_native = _RecordingSyncNativeDriver(rows=[], columns=["id", "name"])
    sync_adapter = _SampleSyncAdapter(sync_native)
    assert isinstance(sync_adapter, SyncDriverAdapterBase)
    with sync_adapter.with_cursor(sync_native) as cursor:
        assert cursor is sync_native

    empty_res = sync_adapter.execute("SELECT id, name FROM users WHERE id = ?", 99)
    assert empty_res.all() == []
    assert empty_res.column_names == ["id", "name"]
    assert empty_res.num_rows == 0


@pytest.mark.anyio
async def test_base_async_driver_preserves_zero_row_columns_and_cursor_contract() -> None:
    """BaseAsyncDriver preserves zero-row ordered column names via query_with_columns."""
    async_native = _RecordingAsyncNativeDriver(rows=[], columns=["alpha", "beta"])
    async_adapter = _SampleAsyncAdapter(async_native)
    assert isinstance(async_adapter, AsyncDriverAdapterBase)
    async with async_adapter.with_cursor(async_native) as cursor:
        assert cursor is async_native

    empty_res = await async_adapter.execute("SELECT alpha, beta FROM items WHERE 1 = 0")
    assert empty_res.all() == []
    assert empty_res.column_names == ["alpha", "beta"]
    assert empty_res.num_rows == 0


def test_unwrap_typed_parameters_and_non_mutating_statement_config_merge() -> None:
    """TypedParameter wrappers unwrap cleanly and merge_statement_config never mutates its base."""
    assert unwrap_typed_parameters([TypedParameter(42, int), {"k": TypedParameter("v", str)}]) == [42, {"k": "v"}]
    assert unwrap_typed_parameters({"a": TypedParameter(None, Any)}) == {"a": None}

    base = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
        ),
    )
    updated = merge_statement_config(base, dialect="mysql", enable_caching=False)
    assert base.dialect == "sqlite"
    assert updated.dialect == "mysql"
    assert updated.enable_caching is False


@pytest.mark.parametrize(
    ("exc_factory", "expected_type"),
    [
        (lambda: NativeIntegrityError("UNIQUE constraint failed"), UniqueViolationError),
        (lambda: NativeIntegrityError("FOREIGN KEY constraint failed"), ForeignKeyViolationError),
        (lambda: NativeIntegrityError("CHECK constraint failed"), CheckViolationError),
        (lambda: NativeIntegrityError("NOT NULL constraint failed"), NotNullViolationError),
        (lambda: NativeDataError("invalid value"), DataError),
        (lambda: ConnectionFailure("connection refused"), DatabaseConnectionError),
        (lambda: PoolExhaustedError("acquire timed out"), ConnectionTimeoutError),
        (lambda: QueryCancelledError("query cancelled by caller"), OperationCancelledError),
        (lambda: SerializationFailure("deadlock detected"), DeadlockError),
        (lambda: SpannerAbortedError("Aborted: transaction aborted"), SerializationConflictError),
        (lambda: GoogleCloudAuthError("PermissionDenied"), PermissionDeniedError),
        (lambda: GoogleCloudError("InvalidArgument invalid parameter"), OperationalError),
        (lambda: BigQueryError("Permission denied on dataset"), PermissionDeniedError),
        (lambda: BigQueryRowError("invalid row payload"), DataError),
        (lambda: NotFoundError("table not found"), DataError),
        (lambda: NotSupportedError("feature unsupported"), DialectNotSupportedError),
        (lambda: ProgrammingError("syntax error near FROM"), SQLParsingError),
        (lambda: SpannerError("Unavailable transient error"), OperationalError),
        (lambda: NativeOperationalError("statement timeout expired"), OperationalError),
    ],
)
@pytest.mark.anyio
async def test_deferred_exception_handlers_map_native_and_gcp_errors(
    exc_factory: Any,
    expected_type: type[Exception],
) -> None:
    """Deferred sync and async handlers map Vertebra native and GCP errors onto SQLSpec exceptions."""
    with DeferredSyncExceptionHandler() as sync_handler:
        raise exc_factory()
    assert isinstance(sync_handler.pending_exception, expected_type)

    async with DeferredAsyncExceptionHandler() as async_handler:
        raise exc_factory()
    assert isinstance(async_handler.pending_exception, expected_type)


@pytest.mark.anyio
async def test_shared_pool_refcounted_close_defers_until_final_release(tmp_path: Any) -> None:
    """Shared-pool adapter close() holds native driver open until final refcount release."""
    sync_native = _RecordingSyncNativeDriver()
    retain_shared_native_driver(sync_native)
    retain_shared_native_driver(sync_native)
    assert shared_native_refcount(sync_native) == 2

    adapter_one = _SampleSyncAdapter(sync_native, shared_pool=True)
    adapter_two = _SampleSyncAdapter(sync_native, shared_pool=True)
    adapter_one.close()
    assert sync_native.closed is False
    assert shared_native_refcount(sync_native) == 1

    adapter_two.close()
    assert sync_native.closed is True
    assert shared_native_refcount(sync_native) == 0

    cfg = SQLiteConfig(path=str(tmp_path / "shared_pool_test.db"))
    a1, owner1 = acquire_shared_native_driver(
        cfg,
        _RecordingAsyncNativeDriver,
        share_pool=True,
        broker=DatabaseRegistry.default_broker(),
        is_async=True,
    )
    a2, owner2 = acquire_shared_native_driver(
        cfg,
        _RecordingAsyncNativeDriver,
        share_pool=True,
        broker=DatabaseRegistry.default_broker(),
        is_async=True,
    )
    try:
        assert a1 is a2
        assert owner1 is True
        assert owner2 is True
        assert shared_native_refcount(a1) == 2

        await release_shared_native_driver_async(a1)
        assert a1._closed is False
        assert shared_native_refcount(a1) == 1

        await release_shared_native_driver_async(a2)
        assert a1._closed is True
        assert shared_native_refcount(a1) == 0
    finally:
        with DatabaseRegistry.default()._lock:
            DatabaseRegistry.default()._shared_async_drivers.clear()
    assert shared_native_refcount(sync_native) == 0
