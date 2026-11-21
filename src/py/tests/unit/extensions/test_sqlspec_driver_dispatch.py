"""Unit tests for SQLSpec BaseSyncDriver/BaseAsyncDriver template-method dispatch and error mapping."""

import ast
import importlib
import io
import sys
import tokenize
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, ClassVar, cast

import pytest
from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig
from sqlspec.data_dictionary import get_dialect_config
from sqlspec.data_dictionary.dialects.mysql.dictionary import (
    MySQLAsyncDataDictionary as UpstreamMySQLAsyncDataDictionary,
)
from sqlspec.data_dictionary.dialects.mysql.dictionary import (
    MySQLSyncDataDictionary as UpstreamMySQLSyncDataDictionary,
)
from sqlspec.driver import (
    AsyncDataDictionaryBase,
    AsyncDriverAdapterBase,
    SyncDataDictionaryBase,
    SyncDriverAdapterBase,
)
from sqlspec.driver._common import cache_hit_hooks_unchanged
from sqlspec.exceptions import (
    CheckViolationError,
    ConnectionTimeoutError,
    DatabaseConnectionError,
    DataError,
    DeadlockError,
    DialectNotSupportedError,
    ForeignKeyViolationError,
    IntegrityError,
    NotNullViolationError,
    OperationalError,
    OperationCancelledError,
    PermissionDeniedError,
    SerializationConflictError,
    SQLParsingError,
    UniqueViolationError,
)
from sqlspec.extensions.events import AsyncEventBackendProtocol, SyncEventBackendProtocol
from sqlspec.observability import ObservabilityConfig, ObservabilityRuntime

from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    ConnectionFailure,
    DatabaseError,
    GoogleCloudAuthError,
    GoogleCloudError,
    InterfaceError,
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
from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
from vertebra.db.adapters.sqlite.config import SQLiteConfig
from vertebra.db.driver import AsyncDatabaseDriver, SyncDatabaseDriver, ThreadOffloadAsyncDatabaseDriver
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import (
    AsyncAdapterConfig,
    BaseAsyncDriver,
    BaseSyncDriver,
    LazyStoreAttr,
    SyncAdapterConfig,
)
from vertebra.extensions.sqlspec.adapters.bigquery.driver import BigQueryAsyncDriver, BigQuerySyncDriver
from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBAsyncDriver, DuckDBSyncDriver
from vertebra.extensions.sqlspec.adapters.mssql.driver import MSSQLAsyncDriver, MSSQLSyncDriver
from vertebra.extensions.sqlspec.adapters.mysql.driver import MySQLAsyncDriver, MySQLSyncDriver
from vertebra.extensions.sqlspec.adapters.oracle import OracleAsyncConfig, OracleSyncConfig
from vertebra.extensions.sqlspec.adapters.oracle.driver import OracleAsyncDriver, OracleSyncDriver
from vertebra.extensions.sqlspec.adapters.postgres import PostgresAsyncConfig, PostgresSyncConfig
from vertebra.extensions.sqlspec.adapters.postgres.driver import PostgresAsyncDriver, PostgresSyncDriver
from vertebra.extensions.sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver
from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver, SQLiteSyncDriver
from vertebra.extensions.sqlspec.events import ChangeFeedAsyncEventBackend, ChangeFeedSyncEventBackend


class _RecordingSyncCursor:
    """Minimal sync DB-API cursor double backed by `_RecordingSyncNativeDriver`."""

    def __init__(self, driver: "_RecordingSyncNativeDriver") -> None:
        self._driver = driver
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.closed: bool = False
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, parameters: Any = None) -> None:
        self._driver.calls.append(("execute", sql, parameters))
        upper = sql.lstrip().upper()
        if upper.startswith(("SELECT", "WITH", "PRAGMA", "VALUES")) or "RETURNING" in upper:
            cols = self._driver._columns or (list(self._driver._rows[0].keys()) if self._driver._rows else ["column_0"])
            self.description = tuple((col, None, None, None, None, None, None) for col in cols)
            self._rows = [tuple(row.get(col) for col in cols) for row in self._driver._rows]
            self.rowcount = -1
        else:
            self.description = None
            self._rows = []
            self.rowcount = 1

    def executemany(self, sql: str, parameter_sets: Sequence[Any]) -> None:
        batch = list(parameter_sets)
        self._driver.calls.append(("executemany", sql, batch))
        self.description = None
        self._rows = []
        self.rowcount = len(batch)

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self._rows
        self._rows = []
        return rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows.pop(0) if self._rows else None

    def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        chunk = self._rows[:size]
        self._rows = self._rows[size:]
        return chunk

    def close(self) -> None:
        self.closed = True


class _RecordingAsyncCursor:
    """Minimal async DB-API cursor double backed by `_RecordingAsyncNativeDriver`."""

    def __init__(self, driver: "_RecordingAsyncNativeDriver") -> None:
        self._driver = driver
        self.description: tuple[tuple[Any, ...], ...] | None = None
        self.rowcount: int = -1
        self.lastrowid: int | None = None
        self.closed: bool = False
        self._rows: list[tuple[Any, ...]] = []

    async def execute(self, sql: str, parameters: Any = None) -> None:
        self._driver.calls.append(("execute", sql, parameters))
        upper = sql.lstrip().upper()
        if upper.startswith(("SELECT", "WITH", "PRAGMA", "VALUES")) or "RETURNING" in upper:
            cols = self._driver._columns or (list(self._driver._rows[0].keys()) if self._driver._rows else ["column_0"])
            self.description = tuple((col, None, None, None, None, None, None) for col in cols)
            self._rows = [tuple(row.get(col) for col in cols) for row in self._driver._rows]
            self.rowcount = -1
        else:
            self.description = None
            self._rows = []
            self.rowcount = 1

    async def executemany(self, sql: str, parameter_sets: Sequence[Any]) -> None:
        batch = list(parameter_sets)
        self._driver.calls.append(("executemany", sql, batch))
        self.description = None
        self._rows = []
        self.rowcount = len(batch)

    async def fetchall(self) -> list[tuple[Any, ...]]:
        rows = self._rows
        self._rows = []
        return rows

    async def fetchone(self) -> tuple[Any, ...] | None:
        return self._rows.pop(0) if self._rows else None

    async def fetchmany(self, size: int = 1) -> list[tuple[Any, ...]]:
        chunk = self._rows[:size]
        self._rows = self._rows[size:]
        return chunk

    async def close(self) -> None:
        self.closed = True


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

    def cursor(self, *, lazy_begin: bool = False) -> _RecordingSyncCursor:
        _ = lazy_begin
        return _RecordingSyncCursor(self)


class _SingleCallSyncCursor(_RecordingSyncCursor):
    """Sync cursor double propagating configured errors, rowcount, and lastrowid."""

    _driver: "_SingleCallSyncNative"

    def __init__(self, driver: "_SingleCallSyncNative") -> None:
        super().__init__(driver)

    def execute(self, sql: str, parameters: Any = None) -> None:
        if self._driver.fail_with is not None:
            raise self._driver.fail_with
        super().execute(sql, parameters)
        self.lastrowid = self._driver.lastrowid
        if self.description is None:
            self.rowcount = self._driver.rowcount


class _SingleCallSyncClient:
    """Sync client double recording single-call fetch_rows and execute_dml invocations."""

    def __init__(self, driver: "_SingleCallSyncNative") -> None:
        self._driver = driver

    def fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        self._driver.calls.append(("fetch_rows", sql, params))
        if self._driver.fail_with is not None:
            raise self._driver.fail_with
        cols = self._driver._columns or (list(self._driver._rows[0].keys()) if self._driver._rows else [])
        tuple_rows = [tuple(r.get(c) for c in cols) for r in self._driver._rows]
        return tuple_rows, tuple(cols)

    def execute_dml(self, sql: str, params: Any = ()) -> tuple[int, int | None]:
        self._driver.calls.append(("client_execute_dml", sql, params))
        if self._driver.fail_with is not None:
            raise self._driver.fail_with
        return self._driver.rowcount, self._driver.lastrowid


class _SingleCallSyncNative(_RecordingSyncNativeDriver):
    """Sync native driver double supporting single-call methods and optional bound client methods."""

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        columns: list[str] | None = None,
        *,
        rowcount: int = 1,
        lastrowid: int | None = 7,
        with_client: bool = False,
        fail_with: BaseException | None = None,
    ) -> None:
        super().__init__(rows=rows, columns=columns)
        self.rowcount = rowcount
        self.lastrowid = lastrowid
        self.fail_with = fail_with
        if with_client:
            self._client = _SingleCallSyncClient(self)
            self._fetch_rows = self._client.fetch_rows
            self._run_dml = self._client.execute_dml

    def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        if self.fail_with is not None:
            raise self.fail_with
        return super().execute_with_description(sql, parameters, **kwargs)

    def execute_dml(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        _ = kwargs
        self.calls.append(("execute_dml", sql, parameters))
        if self.fail_with is not None:
            raise self.fail_with
        return self.rowcount, self.lastrowid

    def cursor(self, *, lazy_begin: bool = False) -> _SingleCallSyncCursor:
        _ = lazy_begin
        self.calls.append(("cursor", "", None))
        return _SingleCallSyncCursor(self)


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

    def cursor(self, *, lazy_begin: bool = False) -> _RecordingAsyncCursor:
        _ = lazy_begin
        return _RecordingAsyncCursor(self)


class _SingleCallAsyncCursor(_RecordingAsyncCursor):
    """Async cursor double propagating configured errors, rowcount, and lastrowid."""

    _driver: "_SingleCallAsyncNative"

    def __init__(self, driver: "_SingleCallAsyncNative") -> None:
        super().__init__(driver)

    async def execute(self, sql: str, parameters: Any = None) -> None:
        self._driver.awaited += 1
        if self._driver.fail_with is not None:
            raise self._driver.fail_with
        await super().execute(sql, parameters)
        self.lastrowid = self._driver.lastrowid
        if self.description is None:
            self.rowcount = self._driver.rowcount


class _SingleCallAsyncClient:
    """Async client double recording single-call fetch_rows and execute_dml invocations."""

    def __init__(self, driver: "_SingleCallAsyncNative") -> None:
        self._driver = driver

    async def fetch_rows(self, sql: str, params: Any = ()) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        self._driver.awaited += 1
        self._driver.calls.append(("fetch_rows", sql, params))
        if self._driver.fail_with is not None:
            raise self._driver.fail_with
        cols = self._driver._columns or (list(self._driver._rows[0].keys()) if self._driver._rows else [])
        tuple_rows = [tuple(r.get(c) for c in cols) for r in self._driver._rows]
        return tuple_rows, tuple(cols)

    async def execute_dml(self, sql: str, params: Any = ()) -> tuple[int, int | None]:
        self._driver.awaited += 1
        self._driver.calls.append(("client_execute_dml", sql, params))
        if self._driver.fail_with is not None:
            raise self._driver.fail_with
        return self._driver.rowcount, self._driver.lastrowid


class _SingleCallAsyncNative(_RecordingAsyncNativeDriver):
    """Async native driver double supporting single-call methods and optional bound client methods."""

    def __init__(
        self,
        rows: list[dict[str, Any]] | None = None,
        columns: list[str] | None = None,
        *,
        rowcount: int = 1,
        lastrowid: int | None = 7,
        with_client: bool = False,
        fail_with: BaseException | None = None,
    ) -> None:
        super().__init__(rows=rows, columns=columns)
        self.rowcount = rowcount
        self.lastrowid = lastrowid
        self.fail_with = fail_with
        self.awaited: int = 0
        if with_client:
            self._client = _SingleCallAsyncClient(self)
            self._fetch_rows = self._client.fetch_rows
            self._run_dml = self._client.execute_dml

    async def execute_with_description(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]:
        self.awaited += 1
        if self.fail_with is not None:
            raise self.fail_with
        return await super().execute_with_description(sql, parameters, **kwargs)

    async def execute_dml(
        self,
        sql: str,
        parameters: Any = None,
        **kwargs: Any,
    ) -> tuple[int, int | None]:
        _ = kwargs
        self.awaited += 1
        self.calls.append(("execute_dml", sql, parameters))
        if self.fail_with is not None:
            raise self.fail_with
        return self.rowcount, self.lastrowid

    def cursor(self, *, lazy_begin: bool = False) -> _SingleCallAsyncCursor:
        _ = lazy_begin
        self.calls.append(("cursor", "", None))
        return _SingleCallAsyncCursor(self)


class _SampleSyncAdapter(BaseSyncDriver):
    """Concrete BaseSyncDriver subclass for sqlite qmark execution."""

    dialect = "sqlite"

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        return cls._build_statement_config("sqlite", ParameterStyle.QMARK)


class _SampleAsyncAdapter(BaseAsyncDriver):
    """Concrete BaseAsyncDriver subclass for sqlite qmark execution."""

    dialect = "sqlite"

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        return BaseSyncDriver._build_statement_config("sqlite", ParameterStyle.QMARK)


def test_base_drivers_inherit_template_dispatch_and_with_cursor_cache_contract() -> None:
    """BaseSyncDriver inherits upstream execute/select and yields a closed-on-exit native cursor in with_cursor."""
    sync_native = _RecordingSyncNativeDriver(rows=[], columns=["id", "name"])
    sync_adapter = _SampleSyncAdapter(sync_native)
    assert isinstance(sync_adapter, SyncDriverAdapterBase)
    with sync_adapter.with_cursor(sync_native) as cursor:
        assert isinstance(cursor, _RecordingSyncCursor)
        assert cursor.closed is False
    assert cursor.closed is True

    empty_res = sync_adapter.execute("SELECT id, name FROM users WHERE id = ?", 99)
    assert empty_res.all() == []
    assert empty_res.column_names == ["id", "name"]
    assert empty_res.num_rows == 0


@pytest.mark.anyio
async def test_base_async_driver_preserves_zero_row_columns_and_cursor_contract() -> None:
    """BaseAsyncDriver preserves zero-row ordered column names via native cursor description."""
    async_native = _RecordingAsyncNativeDriver(rows=[], columns=["alpha", "beta"])
    async_adapter = _SampleAsyncAdapter(async_native)
    assert isinstance(async_adapter, AsyncDriverAdapterBase)
    async with async_adapter.with_cursor(async_native) as cursor:
        assert isinstance(cursor, _RecordingAsyncCursor)
        assert cursor.closed is False
    assert cursor.closed is True

    empty_res = await async_adapter.execute("SELECT alpha, beta FROM items WHERE 1 = 0")
    assert empty_res.all() == []
    assert empty_res.column_names == ["alpha", "beta"]
    assert empty_res.num_rows == 0


def test_non_mutating_statement_config_merge() -> None:
    """_merge_statement_config never mutates its base StatementConfig."""
    base = StatementConfig(
        dialect="sqlite",
        parameter_config=ParameterStyleConfig(
            default_parameter_style=ParameterStyle.QMARK,
            supported_parameter_styles={ParameterStyle.QMARK},
        ),
    )
    updated = BaseSyncDriver._merge_statement_config(base, dialect="mysql", enable_caching=False)
    assert base.dialect == "sqlite"
    assert updated.dialect == "mysql"
    assert updated.enable_caching is False


@pytest.mark.parametrize(
    "hook_name",
    [
        "_unwrap_typed_parameters",
        "_normalize_bound_params",
        "_normalize_statement_params",
        "_prepare_native_parameters",
        "_compile_for_native",
        "_fetch_rows_with_columns",
    ],
)
@pytest.mark.parametrize("driver_cls", [BaseSyncDriver, BaseAsyncDriver])
def test_base_drivers_omit_removed_legacy_hooks(driver_cls: type[Any], hook_name: str) -> None:
    """BaseSyncDriver and BaseAsyncDriver expose no legacy parameter, compile, or row-probe hooks."""
    with pytest.raises(AttributeError):
        getattr(driver_cls, hook_name)


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
    """Driver handle_database_exceptions maps Vertebra native and GCP errors onto SQLSpec exceptions."""
    sync_adapter = _SampleSyncAdapter(_RecordingSyncNativeDriver())
    with sync_adapter.handle_database_exceptions() as sync_handler:
        raise exc_factory()
    assert isinstance(sync_handler.pending_exception, expected_type)

    async_adapter = _SampleAsyncAdapter(_RecordingAsyncNativeDriver())
    async with async_adapter.handle_database_exceptions() as async_handler:
        raise exc_factory()
    assert isinstance(async_handler.pending_exception, expected_type)


@pytest.mark.anyio
async def test_shared_pool_refcounted_close_defers_until_final_release(tmp_path: Any) -> None:
    """Shared-pool adapter close() holds native driver open until final refcount release."""
    sync_native = _RecordingSyncNativeDriver()
    SyncAdapterConfig._retain_shared_native_driver(sync_native)
    SyncAdapterConfig._retain_shared_native_driver(sync_native)
    assert SyncAdapterConfig._shared_native_refcount(sync_native) == 2

    adapter_one = _SampleSyncAdapter(sync_native, shared_pool=True)
    adapter_two = _SampleSyncAdapter(sync_native, shared_pool=True)
    adapter_one.close()
    assert sync_native.closed is False
    assert SyncAdapterConfig._shared_native_refcount(sync_native) == 1

    adapter_two.close()
    assert sync_native.closed is True
    assert SyncAdapterConfig._shared_native_refcount(sync_native) == 0

    cfg = SQLiteConfig(path=str(tmp_path / "shared_pool_test.db"))
    a1, owner1 = AsyncAdapterConfig._acquire_shared_native_driver(
        cfg,
        _RecordingAsyncNativeDriver,
        share_pool=True,
        broker=DatabaseRegistry.default_broker(),
        is_async=True,
    )
    a2, owner2 = AsyncAdapterConfig._acquire_shared_native_driver(
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
        assert AsyncAdapterConfig._shared_native_refcount(a1) == 2

        await AsyncAdapterConfig._release_shared_native_driver_async(a1)
        assert a1._closed is False
        assert AsyncAdapterConfig._shared_native_refcount(a1) == 1

        await AsyncAdapterConfig._release_shared_native_driver_async(a2)
        assert a1._closed is True
        assert AsyncAdapterConfig._shared_native_refcount(a1) == 0
    finally:
        with DatabaseRegistry.default()._lock:
            DatabaseRegistry.default()._shared_async_drivers.clear()
    assert SyncAdapterConfig._shared_native_refcount(sync_native) == 0


class _TxRecordingSyncNative(_RecordingSyncNativeDriver):
    """Sync native driver recording transaction lifecycle calls for SyncAdapterConfig tests."""

    def __init__(self, config: Any = None, *, broker: Any = None) -> None:
        super().__init__()
        self.config = config
        self._broker = broker
        self.rollbacks = 0
        self.commits = 0

    def commit(self) -> None:
        self.commits += 1
        super().commit()

    def rollback(self) -> None:
        self.rollbacks += 1
        super().rollback()


class _TxRecordingAsyncNative(_RecordingAsyncNativeDriver):
    """Async native driver recording transaction lifecycle calls for AsyncAdapterConfig tests."""

    def __init__(self, config: Any = None, *, broker: Any = None) -> None:
        super().__init__()
        self.config = config
        self._broker = broker
        self.rollbacks = 0
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1
        await super().commit()

    async def rollback(self) -> None:
        self.rollbacks += 1
        await super().rollback()


class _CustomBuildSyncConfig(SyncAdapterConfig[dict[str, Any], Any, Any, _SampleSyncAdapter]):
    """SyncAdapterConfig subclass exercising build_connection_config hook."""

    dialect: ClassVar[str] = "sqlite"
    is_async: ClassVar[bool] = False
    connection_type: ClassVar[type[Any]] = _TxRecordingSyncNative
    _config_cls: ClassVar[type[Any]] = dict
    _driver_cls: ClassVar[type[Any]] = _TxRecordingSyncNative
    driver_type: ClassVar[type[Any]] = _SampleSyncAdapter

    def build_connection_config(
        self,
        config: dict[str, Any] | str | None,
        connection_config: dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(connection_config or {})
        merged.update(kwargs)
        if isinstance(config, str):
            merged["resolved_dsn"] = config
        elif isinstance(config, Mapping):
            merged.update(config)
        merged["built_via_hook"] = True
        return merged


class _CustomBuildAsyncConfig(AsyncAdapterConfig[dict[str, Any], Any, Any, _SampleAsyncAdapter]):
    """AsyncAdapterConfig subclass exercising build_connection_config and async provide_pool."""

    dialect: ClassVar[str] = "sqlite"
    is_async: ClassVar[bool] = True
    connection_type: ClassVar[type[Any]] = _TxRecordingAsyncNative
    _config_cls: ClassVar[type[Any]] = dict
    _driver_cls: ClassVar[type[Any]] = _TxRecordingAsyncNative
    driver_type: ClassVar[type[Any]] = _SampleAsyncAdapter

    def build_connection_config(
        self,
        config: dict[str, Any] | str | None,
        connection_config: dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> dict[str, Any]:
        merged = dict(connection_config or {})
        merged.update(kwargs)
        if isinstance(config, str):
            merged["resolved_dsn"] = config
        elif isinstance(config, Mapping):
            merged.update(config)
        merged["built_via_hook"] = True
        return merged


@pytest.mark.anyio
async def test_adapter_config_build_hook_derive_pool_session_rollback_and_async_pool() -> None:
    """Adapter configs invoke build_connection_config, derive dsn/url pools cleanly, roll back on error, and await provide_pool."""
    sync_cfg = _CustomBuildSyncConfig("sqlite:///custom.db", share_pool=False, pool_tag="primary")
    assert sync_cfg.driver_config["built_via_hook"] is True
    assert sync_cfg.driver_config["resolved_dsn"] == "sqlite:///custom.db"
    assert sync_cfg.driver_config["pool_tag"] == "primary"

    async_cfg = _CustomBuildAsyncConfig("sqlite:///async_custom.db", share_pool=False, pool_tag="replica")
    assert async_cfg.driver_config["built_via_hook"] is True
    assert async_cfg.driver_config["resolved_dsn"] == "sqlite:///async_custom.db"
    assert async_cfg.driver_config["pool_tag"] == "replica"

    sync_pool = sync_cfg.provide_pool()
    assert isinstance(sync_pool, _TxRecordingSyncNative)
    async_pool = await async_cfg.provide_pool()
    assert isinstance(async_pool, _TxRecordingAsyncNative)

    with pytest.raises(RuntimeError, match="sync boom"), sync_cfg.provide_session() as sync_session:
        sync_session.begin()
        assert sync_session.connection.in_transaction is True
        raise RuntimeError("sync boom")
    assert sync_pool.in_transaction is False
    assert sync_pool.rollbacks == 1

    with pytest.raises(RuntimeError, match="async boom"):
        async with async_cfg.provide_session() as async_session:
            await async_session.begin()
            assert async_session.connection.in_transaction is True
            raise RuntimeError("async boom")
    assert async_pool.in_transaction is False
    assert async_pool.rollbacks == 1

    pg_sync = PostgresSyncConfig(
        url="postgresql://alice:secret@db.internal:5432/analytics",
        share_pool=False,
        max_size=12,
    )
    assert pg_sync.driver_config.max_size == 12
    assert "analytics" in pg_sync.driver_config.dsn
    pg_sync_derived = pg_sync.derive_pool(
        url="postgresql://alice:secret@replica.internal:5432/analytics_ro",
        max_size=24,
    )
    assert pg_sync_derived.driver_config.max_size == 24
    assert "replica.internal" in pg_sync_derived.driver_config.dsn
    assert pg_sync.driver_config.max_size == 12

    pg_async = PostgresAsyncConfig(
        dsn="postgresql://bob:pw@db.internal:5432/core",
        share_pool=False,
    )
    pg_async_derived = pg_async.derive_pool(max_size=16, application_name="worker-2")
    assert pg_async_derived.driver_config.max_size == 16
    assert pg_async_derived.driver_config.application_name == "worker-2"

    ora_sync = OracleSyncConfig(
        url="oracle://scott:tiger@ora.internal:1521/FREEPDB1",
        share_pool=False,
    )
    assert ora_sync.driver_config.host == "ora.internal"
    ora_sync_derived = ora_sync.derive_pool(
        url="oracle://scott:tiger@ora-ro.internal:1521/FREEPDB2",
    )
    assert ora_sync_derived.driver_config.host == "ora-ro.internal"
    assert ora_sync_derived.driver_config.service_name == "FREEPDB2"

    ora_async = OracleAsyncConfig(
        dsn="oracle://scott:tiger@ora.internal:1521/FREEPDB1",
        share_pool=False,
    )
    ora_async_derived = ora_async.derive_pool(module="batch-worker")
    assert ora_async_derived.driver_config.module == "batch-worker"


_SQLSPEC_PACKAGE_ROOT = Path(__file__).resolve().parents[3] / "vertebra" / "extensions" / "sqlspec"
_SQLSPEC_ADAPTERS_ROOT = _SQLSPEC_PACKAGE_ROOT / "adapters"
_SQLSPEC_MODULE_PATHS = tuple(sorted(_SQLSPEC_PACKAGE_ROOT.rglob("*.py")))


@pytest.mark.parametrize(
    "adapter_name",
    ["postgres", "oracle", "sqlite", "mysql", "mssql", "duckdb", "spanner", "bigquery"],
)
def test_isolated_adapter_core_has_no_driver_or_sibling_imports(adapter_name: str) -> None:
    """Adapter core modules import neither _driver nor sibling adapters, and isolated cores use future annotations."""
    core_path = _SQLSPEC_ADAPTERS_ROOT / adapter_name / "core.py"
    tree = ast.parse(core_path.read_text(encoding="utf-8"), filename=str(core_path))

    has_future_annotations = False
    forbidden_modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == "__future__" and any(alias.name == "annotations" for alias in node.names):
                has_future_annotations = True
            if "_driver" in mod:
                forbidden_modules.append(mod)
            if mod.startswith("vertebra.extensions.sqlspec.adapters."):
                target_adapter = mod.removeprefix("vertebra.extensions.sqlspec.adapters.").split(".")[0]
                if target_adapter != adapter_name:
                    forbidden_modules.append(mod)
            if adapter_name == "spanner" and (
                mod.startswith(("google.", "sqlspec.adapters.")) or mod == "google" or "bigquery" in mod
            ):
                forbidden_modules.append(mod)
            if node.level > 1 and mod:
                forbidden_modules.append(f"{'.' * node.level}{mod}")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if "_driver" in alias.name or (
                    alias.name.startswith("vertebra.extensions.sqlspec.adapters.")
                    and alias.name.removeprefix("vertebra.extensions.sqlspec.adapters.").split(".")[0] != adapter_name
                ):
                    forbidden_modules.append(alias.name)
                if adapter_name == "spanner" and (
                    alias.name.startswith(("google.", "sqlspec.adapters."))
                    or alias.name == "google"
                    or "bigquery" in alias.name
                ):
                    forbidden_modules.append(alias.name)

    assert has_future_annotations is True
    assert forbidden_modules == []


@pytest.mark.parametrize(
    "module_path",
    _SQLSPEC_MODULE_PATHS,
    ids=lambda path: str(path.relative_to(_SQLSPEC_PACKAGE_ROOT)),
)
def test_sqlspec_package_ast_headers_and_zero_comments(module_path: Path) -> None:
    """Every module under extensions/sqlspec begins with docstring + future annotations, defines __all__, and has zero # comments."""
    source = module_path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(module_path))
    assert ast.get_docstring(tree), f"{module_path} missing module docstring"
    assert len(tree.body) >= 2
    future_node = tree.body[1]
    assert isinstance(future_node, ast.ImportFrom)
    assert future_node.module == "__future__"
    assert any(alias.name == "annotations" for alias in future_node.names)
    has_all = any(
        (isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets))
        or (isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name) and node.target.id == "__all__")
        for node in tree.body
    )
    assert has_all, f"{module_path} missing module-level __all__"
    comments = [
        (tok.start[0], tok.string)
        for tok in tokenize.generate_tokens(io.StringIO(source).readline)
        if tok.type == tokenize.COMMENT
    ]
    assert comments == [], f"{module_path} contains inline # comments: {comments}"


@pytest.mark.parametrize(
    ("adapter_name", "dialect", "parameter_style"),
    [
        ("postgres", "postgres", ParameterStyle.NUMERIC),
        ("oracle", "oracle", ParameterStyle.NAMED_COLON),
        ("sqlite", "sqlite", ParameterStyle.QMARK),
        ("spanner", "spanner", ParameterStyle.NAMED_AT),
    ],
)
def test_isolated_adapter_native_statement_config_returns_fresh_instances(
    adapter_name: str,
    dialect: str,
    parameter_style: ParameterStyle,
) -> None:
    """Isolated native_statement_config builds a fresh StatementConfig per call without aliasing."""
    core_mod = importlib.import_module(f"vertebra.extensions.sqlspec.adapters.{adapter_name}.core")
    local_builder = core_mod.native_statement_config
    assert local_builder is not BaseSyncDriver._build_statement_config

    first = local_builder(dialect, parameter_style)
    second = local_builder(dialect, parameter_style, enable_caching=False)
    assert isinstance(first, StatementConfig)
    assert isinstance(second, StatementConfig)
    assert first is not second
    assert first.dialect == dialect
    assert first.parameter_config.default_execution_parameter_style == parameter_style
    assert second.enable_caching is False


@pytest.mark.parametrize("adapter_name", ["postgres", "oracle", "spanner"])
@pytest.mark.parametrize(
    ("exc_factory", "expected_type"),
    [
        (lambda: NativeIntegrityError("UNIQUE constraint failed"), UniqueViolationError),
        (lambda: NativeIntegrityError("FOREIGN KEY constraint failed"), ForeignKeyViolationError),
        (lambda: NativeIntegrityError("CHECK constraint failed"), CheckViolationError),
        (lambda: NativeIntegrityError("NOT NULL constraint failed"), NotNullViolationError),
        (lambda: NativeIntegrityError("generic integrity error"), IntegrityError),
        (lambda: SerializationFailure("deadlock detected"), DeadlockError),
        (lambda: SerializationFailure("could not serialize access"), SerializationConflictError),
        (lambda: QueryCancelledError("query cancelled"), OperationCancelledError),
        (lambda: PoolExhaustedError("connection timed out"), ConnectionTimeoutError),
        (lambda: ConnectionFailure("connection refused"), DatabaseConnectionError),
        (lambda: NativeDataError("numeric overflow"), DataError),
        (lambda: NotSupportedError("unsupported feature"), DialectNotSupportedError),
        (lambda: ProgrammingError("syntax error"), SQLParsingError),
        (lambda: NativeOperationalError("unexpected backend failure"), OperationalError),
        (lambda: DatabaseError("generic database error"), OperationalError),
    ],
)
def test_isolated_adapter_map_native_database_exception_specificity(
    adapter_name: str,
    exc_factory: Any,
    expected_type: type[Exception],
) -> None:
    """Isolated postgres, oracle, and spanner map_native_database_exception map native errors in specificity order."""
    core_mod = importlib.import_module(f"vertebra.extensions.sqlspec.adapters.{adapter_name}.core")
    mapped = core_mod.map_native_database_exception(exc_factory())
    assert isinstance(mapped, expected_type)
    assert core_mod.map_native_database_exception(ValueError("unrelated")) is None
    assert core_mod.map_native_database_exception(OperationalError("already mapped")) is None


def test_spanner_core_isolated_helpers_handle_spanner_errors_and_dialects() -> None:
    """Spanner core helpers build fresh dialect StatementConfigs and map Spanner/GoogleCloud exceptions."""
    spanner_core = importlib.import_module("vertebra.extensions.sqlspec.adapters.spanner.core")
    assert spanner_core.native_statement_config is not BaseSyncDriver._build_statement_config

    googlesql_cfg = spanner_core.default_statement_config("googlesql")
    spangres_cfg = spanner_core.default_statement_config("spangres")
    assert googlesql_cfg.dialect == "spanner"
    assert googlesql_cfg.parameter_config.default_execution_parameter_style == ParameterStyle.NAMED_AT
    assert spangres_cfg.dialect == "spangres"
    assert spangres_cfg.parameter_config.default_execution_parameter_style == ParameterStyle.NUMERIC

    assert isinstance(
        spanner_core.map_native_database_exception(SpannerAbortedError("tx aborted")), SerializationConflictError
    )
    assert isinstance(
        spanner_core.map_native_database_exception(SpannerError("Resource already exists")), UniqueViolationError
    )
    assert isinstance(
        spanner_core.map_native_database_exception(SpannerError("Unavailable transient error")), OperationalError
    )
    assert isinstance(spanner_core.map_native_database_exception(GoogleCloudAuthError("denied")), PermissionDeniedError)


def test_base_driver_module_surface_slots_and_lazy_store_attr() -> None:
    """_driver exports only base classes and descriptors, declares __slots__, and LazyStoreAttr defers imports."""
    driver_mod = importlib.import_module("vertebra.extensions.sqlspec._driver")
    assert set(driver_mod.__all__) == {
        "AsyncAdapterConfig",
        "BaseAsyncDriver",
        "BaseSyncDriver",
        "LazyStoreAttr",
        "SyncAdapterConfig",
    }
    for deleted_name in (
        "DeferredAsyncExceptionHandler",
        "DeferredSyncExceptionHandler",
        "acquire_shared_native_driver",
        "compile_many_transport",
        "map_native_database_exception",
        "merge_statement_config",
        "native_statement_config",
        "prepare_many_statement",
        "release_shared_native_driver",
        "release_shared_native_driver_async",
        "retain_shared_native_driver",
        "shared_native_refcount",
        "unwrap_typed_parameters",
    ):
        assert not hasattr(driver_mod, deleted_name)

    for slotted_cls in (BaseSyncDriver, BaseAsyncDriver, SyncAdapterConfig, AsyncAdapterConfig, LazyStoreAttr):
        assert "__slots__" in slotted_cls.__dict__
    assert "_exception_handler" in BaseSyncDriver.__slots__
    assert "_native_cursor" not in BaseSyncDriver.__slots__
    assert "_exception_handler" in BaseAsyncDriver.__slots__
    assert "_native_cursor" not in BaseAsyncDriver.__slots__

    sync_cfg = _CustomBuildSyncConfig("sqlite:///slots_check.db", share_pool=False)
    assert not hasattr(sync_cfg, "_driver_instance")

    target_mod = "vertebra.extensions.sqlspec.adapters.mysql.adk"
    previous_module = sys.modules.pop(target_mod, None)

    class _LazyConfigHolder:
        adk_store_class = LazyStoreAttr(target_mod, "MySQLSyncADKStore")

    try:
        assert target_mod not in sys.modules
        resolved = _LazyConfigHolder.adk_store_class
        assert resolved.__name__ == "MySQLSyncADKStore"
        assert target_mod in sys.modules
    finally:
        if previous_module is not None:
            sys.modules[target_mod] = previous_module
        else:
            sys.modules.pop(target_mod, None)


class _ResettableSyncNative(_TxRecordingSyncNative):
    """Sync native double tracking reset_session calls."""

    def __init__(self, config: Any = None, *, broker: Any = None) -> None:
        super().__init__(config, broker=broker)
        self.reset_calls = 0

    def reset_session(self) -> None:
        self.reset_calls += 1


class _ResettableAsyncNative(_TxRecordingAsyncNative):
    """Async native double tracking async reset_session calls."""

    def __init__(self, config: Any = None, *, broker: Any = None) -> None:
        super().__init__(config, broker=broker)
        self.reset_calls = 0

    async def reset_session(self) -> None:
        self.reset_calls += 1


class _ResetSyncConfig(_CustomBuildSyncConfig):
    connection_type: ClassVar[type[Any]] = _ResettableSyncNative
    _driver_cls: ClassVar[type[Any]] = _ResettableSyncNative


class _ResetAsyncConfig(_CustomBuildAsyncConfig):
    connection_type: ClassVar[type[Any]] = _ResettableAsyncNative
    _driver_cls: ClassVar[type[Any]] = _ResettableAsyncNative


class _TrackingStream:
    """Minimal row stream double recording session-exit stream closure."""

    def __init__(self) -> None:
        self.closed_errors: list[bool] = []

    def _close_for_session_exit(self, *, error: bool = False) -> None:
        self.closed_errors.append(error)

    async def _aclose_for_session_exit(self, *, error: bool = False) -> None:
        self.closed_errors.append(error)

    def _mark_session_closed(self) -> None:
        return None


@pytest.mark.anyio
async def test_concurrent_provide_session_distinct_drivers_exit_cleanup_and_arg_forwarding() -> None:
    """Simultaneous sessions yield distinct drivers, close streams, reset on release, mark closed, and forward args."""
    sync_cfg = _ResetSyncConfig(
        "sqlite:///concurrent.db",
        share_pool=False,
        driver_features={"reset_session_on_release": True},
    )
    stream_sync = _TrackingStream()
    with sync_cfg.provide_session() as d1, sync_cfg.provide_session() as d2:
        assert d1 is not d2
        d1._open_streams.append(stream_sync)
        d1.begin()
        assert d1.connection.in_transaction is True
    sync_conn = sync_cfg.connection_instance
    assert sync_conn is not None
    assert stream_sync.closed_errors == [False]
    assert d1.connection is not sync_conn
    assert d2.connection is not sync_conn
    assert sync_conn.in_transaction is False
    assert sync_conn.rollbacks == 1
    assert sync_conn.reset_calls == 2
    sync_cfg.close()

    async_cfg = _ResetAsyncConfig(
        "sqlite:///concurrent_async.db",
        share_pool=False,
        driver_features={"reset_session_on_release": True},
    )
    stream_async = _TrackingStream()
    async with async_cfg.provide_session() as a1, async_cfg.provide_session() as a2:
        assert a1 is not a2
        a1._open_streams.append(stream_async)
        await a1.begin()
        assert a1.connection.in_transaction is True
    async_conn = async_cfg.connection_instance
    assert async_conn is not None
    assert stream_async.closed_errors == [False]
    assert a1.connection is not async_conn
    assert a2.connection is not async_conn
    assert async_conn.in_transaction is False
    assert async_conn.rollbacks == 1
    assert async_conn.reset_calls == 2
    await async_cfg.close()

    recorded: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    class _ArgSpySessionContext:
        def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
            recorded.append(("session", args, {"config": config, **kwargs}))

        def __enter__(self) -> str:
            return "spy-driver"

        def __exit__(self, *_: Any) -> None:
            return None

    class _ArgSpyConnectionContext:
        def __init__(self, config: Any, *args: Any, **kwargs: Any) -> None:
            recorded.append(("connection", args, {"config": config, **kwargs}))

        def __enter__(self) -> str:
            return "spy-conn"

        def __exit__(self, *_: Any) -> None:
            return None

    class _ArgSpyConfig(_CustomBuildSyncConfig):
        _session_context_class: ClassVar[type[Any]] = _ArgSpySessionContext
        _connection_context_class: ClassVar[type[Any]] = _ArgSpyConnectionContext

    spy_cfg = _ArgSpyConfig("sqlite:///spy.db", share_pool=False)
    with spy_cfg.provide_session("read-only", timeout=15) as session_val:
        assert str(session_val) == "spy-driver"
    with spy_cfg.provide_connection("direct", isolation="SERIALIZABLE") as conn_val:
        assert conn_val == "spy-conn"

    assert recorded[0][0] == "session"
    assert recorded[0][1] == ("read-only",)
    assert recorded[0][2]["timeout"] == 15
    assert recorded[1][0] == "connection"
    assert recorded[1][1] == ("direct",)
    assert recorded[1][2]["isolation"] == "SERIALIZABLE"


@pytest.mark.anyio
async def test_select_to_storage_streams_chunks_sync_and_async(tmp_path: Path) -> None:
    """BaseSyncDriver and BaseAsyncDriver stream query results to storage via _select_for_storage."""
    rows = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
    cols = ["id", "name"]

    sync_native = _RecordingSyncNativeDriver(rows=rows, columns=cols)
    sync_adapter = _SampleSyncAdapter(sync_native)
    sync_dest = tmp_path / "sync_export.parquet"
    sync_job = sync_adapter.select_to_storage("SELECT id, name FROM items", sync_dest, format_hint="parquet")
    assert sync_dest.exists()
    assert sync_job.telemetry.get("rows_processed") == 2

    async_native = _RecordingAsyncNativeDriver(rows=rows, columns=cols)
    async_adapter = _SampleAsyncAdapter(async_native)
    async_dest = tmp_path / "async_export.parquet"
    async_job = await async_adapter.select_to_storage(
        "SELECT id, name FROM items",
        async_dest,
        format_hint="parquet",
    )
    assert async_dest.exists()
    assert async_job.telemetry.get("rows_processed") == 2


@pytest.mark.anyio
async def test_replicas_route_read_sessions_and_connections_sync_and_async() -> None:
    """SyncAdapterConfig and AsyncAdapterConfig configure replicas and route read sessions cleanly."""
    sync_cfg = _CustomBuildSyncConfig(
        "sqlite:///primary.db",
        share_pool=False,
        replicas=["sqlite:///replica.db"],
    )
    assert sync_cfg.has_replicas is True
    assert sync_cfg.is_replica is False
    replica_sync = sync_cfg.replica_groups["default"].specs[0].config
    assert replica_sync.is_replica is True
    assert replica_sync.driver_config["resolved_dsn"] == "sqlite:///replica.db"

    with sync_cfg.provide_replica_session() as replica_driver:
        assert isinstance(replica_driver, _SampleSyncAdapter)
        assert replica_driver.connection.config["resolved_dsn"] == "sqlite:///replica.db"
    with sync_cfg.provide_replica_connection() as replica_conn:
        assert replica_conn.config["resolved_dsn"] == "sqlite:///replica.db"
    routing_sync = sync_cfg.provide_routing_session()
    routing_sync.select("SELECT 1")
    routing_sync.execute("INSERT INTO items VALUES (1)")
    assert replica_sync.connection_instance is not None
    assert sync_cfg.connection_instance is not None
    assert replica_sync.connection_instance.calls[-1][0] == "execute"
    assert "SELECT 1" in replica_sync.connection_instance.calls[-1][1]
    assert sync_cfg.connection_instance.calls[-1][0] == "execute"
    assert "INSERT INTO items" in sync_cfg.connection_instance.calls[-1][1]
    sync_cfg.close_pool()

    async_cfg = _CustomBuildAsyncConfig(
        "sqlite:///async_primary.db",
        share_pool=False,
        replicas=["sqlite:///async_replica.db"],
    )
    assert async_cfg.has_replicas is True
    replica_async = async_cfg.replica_groups["default"].specs[0].config
    assert replica_async.is_replica is True
    assert replica_async.driver_config["resolved_dsn"] == "sqlite:///async_replica.db"

    async with async_cfg.provide_replica_session() as async_replica_driver:
        assert isinstance(async_replica_driver, _SampleAsyncAdapter)
        assert async_replica_driver.connection.config["resolved_dsn"] == "sqlite:///async_replica.db"
    async with async_cfg.provide_replica_connection() as async_replica_conn:
        assert async_replica_conn.config["resolved_dsn"] == "sqlite:///async_replica.db"
    routing_async = async_cfg.provide_routing_session()
    await routing_async.select("SELECT 1")
    await routing_async.execute("INSERT INTO items VALUES (1)")
    assert replica_async.connection_instance is not None
    assert async_cfg.connection_instance is not None
    assert replica_async.connection_instance.calls[-1][0] == "execute"
    assert "SELECT 1" in replica_async.connection_instance.calls[-1][1]
    assert async_cfg.connection_instance.calls[-1][0] == "execute"
    assert "INSERT INTO items" in async_cfg.connection_instance.calls[-1][1]
    await async_cfg.close_pool()


@pytest.mark.parametrize(
    ("adapter_name", "sync_name", "async_name"),
    [
        ("postgres", "PostgresSyncDataDictionary", "PostgresAsyncDataDictionary"),
        ("oracle", "OracleSyncDataDictionary", "OracleAsyncDataDictionary"),
        ("sqlite", "SQLiteSyncDataDictionary", "SQLiteAsyncDataDictionary"),
        ("mysql", "MySQLSyncDataDictionary", "MySQLAsyncDataDictionary"),
        ("mssql", "MSSQLSyncDataDictionary", "MSSQLAsyncDataDictionary"),
        ("duckdb", "DuckDBSyncDataDictionary", "DuckDBAsyncDataDictionary"),
        ("bigquery", "BigQuerySyncDataDictionary", "BigQueryAsyncDataDictionary"),
        ("spanner", "SpannerSyncDataDictionary", "SpannerAsyncDataDictionary"),
    ],
)
def test_core_adapter_data_dictionaries_slots_bases_and_import_hygiene(
    adapter_name: str,
    sync_name: str,
    async_name: str,
) -> None:
    """Data dictionaries declare future annotations, `__slots__`, upstream bases, and zero `sqlspec.adapters` imports."""
    dd_path = _SQLSPEC_ADAPTERS_ROOT / adapter_name / "data_dictionary.py"
    tree = ast.parse(dd_path.read_text(encoding="utf-8"), filename=str(dd_path))

    has_future_annotations = False
    forbidden_imports: list[str] = []
    classes_missing_slots: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod == "__future__" and any(alias.name == "annotations" for alias in node.names):
                has_future_annotations = True
            if mod.startswith("sqlspec.adapters"):
                forbidden_imports.append(mod)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("sqlspec.adapters"):
                    forbidden_imports.append(alias.name)
        elif isinstance(node, ast.ClassDef):
            has_slots = any(
                (
                    isinstance(stmt, ast.Assign)
                    and any(isinstance(target, ast.Name) and target.id == "__slots__" for target in stmt.targets)
                )
                or (
                    isinstance(stmt, ast.AnnAssign)
                    and isinstance(stmt.target, ast.Name)
                    and stmt.target.id == "__slots__"
                )
                for stmt in node.body
            )
            if not has_slots:
                classes_missing_slots.append(node.name)

    assert has_future_annotations is True
    assert forbidden_imports == []
    assert classes_missing_slots == []

    dd_module = importlib.import_module(f"vertebra.extensions.sqlspec.adapters.{adapter_name}.data_dictionary")
    sync_cls = getattr(dd_module, sync_name)
    async_cls = getattr(dd_module, async_name)
    assert "__slots__" in sync_cls.__dict__
    assert "__slots__" in async_cls.__dict__

    if adapter_name == "mysql":
        assert issubclass(sync_cls, UpstreamMySQLSyncDataDictionary)
        assert issubclass(async_cls, UpstreamMySQLAsyncDataDictionary)
    else:
        assert issubclass(sync_cls, SyncDataDictionaryBase)
        assert issubclass(async_cls, AsyncDataDictionaryBase)
        sync_dd = sync_cls()
        async_dd = async_cls()
        assert sync_dd.get_dialect_config() is get_dialect_config(adapter_name)
        assert async_dd.get_dialect_config() is get_dialect_config(adapter_name)
        assert len(sync_dd.list_available_features()) > 0


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("schema", "expected_table", "expected_schema_bind"),
    [
        (None, "user_json_duality_views", None),
        ("hr", "all_json_duality_views", "HR"),
    ],
)
async def test_oracle_get_json_duality_views_sync_and_async(
    schema: str | None,
    expected_table: str,
    expected_schema_bind: str | None,
) -> None:
    """Oracle `get_json_duality_views` dispatches expected catalog queries and handles missing 26ai views."""
    from vertebra.extensions.sqlspec.adapters.oracle.data_dictionary import (
        OracleAsyncDataDictionary,
        OracleSyncDataDictionary,
    )

    sample_row = {
        "VIEW_NAME": "ORDERS_DV",
        "ROOT_TABLE_NAME": "ORDERS",
        "ROOT_TABLE_OWNER": "HR",
        "ALLOW_INSERT": "1",
        "ALLOW_UPDATE": "1",
        "ALLOW_DELETE": "0",
    }

    class _SyncDualityDriverSpy:
        def __init__(self, rows: list[dict[str, Any]] | None = None, error: Exception | None = None) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []
            self._rows = rows or []
            self._error = error

        def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
            raw_sql = statement.raw_sql if hasattr(statement, "raw_sql") else str(statement)
            self.calls.append((raw_sql, dict(kwargs)))
            if self._error is not None:
                raise self._error
            return list(self._rows)

    class _AsyncDualityDriverSpy:
        def __init__(self, rows: list[dict[str, Any]] | None = None, error: Exception | None = None) -> None:
            self.calls: list[tuple[str, dict[str, Any]]] = []
            self._rows = rows or []
            self._error = error

        async def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
            raw_sql = statement.raw_sql if hasattr(statement, "raw_sql") else str(statement)
            self.calls.append((raw_sql, dict(kwargs)))
            if self._error is not None:
                raise self._error
            return list(self._rows)

    sync_dd = OracleSyncDataDictionary()
    sync_driver: Any = _SyncDualityDriverSpy(rows=[sample_row])
    sync_views = sync_dd.get_json_duality_views(sync_driver, schema=schema)
    assert len(sync_views) == 1
    assert sync_views[0]["view_name"] == "ORDERS_DV"
    assert sync_views[0]["root_table_name"] == "ORDERS"
    assert expected_table in sync_driver.calls[0][0]
    if expected_schema_bind is None:
        assert "schema_name" not in sync_driver.calls[0][1]
    else:
        assert sync_driver.calls[0][1]["schema_name"] == expected_schema_bind

    ora942_sync: Any = _SyncDualityDriverSpy(error=RuntimeError("ORA-00942: table or view does not exist"))
    assert sync_dd.get_json_duality_views(ora942_sync, schema=schema) == []

    async_dd = OracleAsyncDataDictionary()
    async_driver: Any = _AsyncDualityDriverSpy(rows=[sample_row])
    async_views = await async_dd.get_json_duality_views(async_driver, schema=schema)
    assert len(async_views) == 1
    assert async_views[0]["view_name"] == "ORDERS_DV"
    assert expected_table in async_driver.calls[0][0]
    if expected_schema_bind is None:
        assert "schema_name" not in async_driver.calls[0][1]
    else:
        assert async_driver.calls[0][1]["schema_name"] == expected_schema_bind

    ora942_async: Any = _AsyncDualityDriverSpy(error=RuntimeError("ORA-00942: table or view does not exist"))
    assert await async_dd.get_json_duality_views(ora942_async, schema=schema) == []


@pytest.mark.anyio
async def test_postgres_and_oracle_data_dictionary_version_cache_and_bindings() -> None:
    """Postgres and Oracle data dictionaries cache driver versions cleanly across `__slots__`."""
    from vertebra.extensions.sqlspec.adapters.oracle.data_dictionary import (
        OracleAsyncDataDictionary,
        OracleSyncDataDictionary,
    )
    from vertebra.extensions.sqlspec.adapters.postgres.data_dictionary import (
        PostgresAsyncDataDictionary,
        PostgresSyncDataDictionary,
    )

    class _SyncVersionSpy:
        def __init__(self, version_row: dict[str, Any]) -> None:
            self.version_calls = 0
            self.select_calls: list[dict[str, Any]] = []
            self._version_row = version_row

        def select_one_or_none(self, statement: Any) -> dict[str, Any]:
            _ = statement
            self.version_calls += 1
            return self._version_row

        def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
            _ = statement
            self.select_calls.append(dict(kwargs))
            return []

    class _AsyncVersionSpy:
        def __init__(self, version_row: dict[str, Any]) -> None:
            self.version_calls = 0
            self.select_calls: list[dict[str, Any]] = []
            self._version_row = version_row

        async def select_one_or_none(self, statement: Any) -> dict[str, Any]:
            _ = statement
            self.version_calls += 1
            return self._version_row

        async def select(self, statement: Any, **kwargs: Any) -> list[dict[str, Any]]:
            _ = statement
            self.select_calls.append(dict(kwargs))
            return []

    pg_sync = PostgresSyncDataDictionary()
    pg_sync_drv: Any = _SyncVersionSpy({"version": "PostgreSQL 16.4"})
    assert pg_sync.get_optimal_type(pg_sync_drv, "jsonb") == "JSONB"
    assert pg_sync.get_feature_flag(pg_sync_drv, "supports_partitioning") is True
    assert pg_sync_drv.version_calls == 1
    pg_sync.get_tables(pg_sync_drv)
    assert pg_sync_drv.select_calls[-1] == {"schema_name": "public", "table_name": None}

    pg_async = PostgresAsyncDataDictionary()
    pg_async_drv: Any = _AsyncVersionSpy({"version": "PostgreSQL 16.4"})
    assert await pg_async.get_optimal_type(pg_async_drv, "json") == "JSONB"
    assert await pg_async.get_feature_flag(pg_async_drv, "supports_jsonb") is True
    assert pg_async_drv.version_calls == 1

    ora_sync = OracleSyncDataDictionary()
    ora_sync_drv: Any = _SyncVersionSpy({"version": "23.4.0.0.0"})
    assert ora_sync.get_optimal_type(ora_sync_drv, "json") == "BLOB"
    assert ora_sync.get_feature_flag(ora_sync_drv, "supports_vectors") is True
    assert ora_sync_drv.version_calls == 1

    ora_async = OracleAsyncDataDictionary()
    ora_async_drv: Any = _AsyncVersionSpy({"version": "19.18.0.0.0"})
    assert await ora_async.get_optimal_type(ora_async_drv, "json") == "BLOB"
    assert await ora_async.get_feature_flag(ora_async_drv, "supports_vectors") is False
    assert ora_async_drv.version_calls == 1


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_name", "sync_name", "async_name", "version_row", "type_category", "expected_type", "expected_domain"),
    [
        (
            "mssql",
            "MSSQLSyncDataDictionary",
            "MSSQLAsyncDataDictionary",
            {"product_version": "17.0.1000.1"},
            "json",
            "JSON",
            "tables",
        ),
        (
            "mssql",
            "MSSQLSyncDataDictionary",
            "MSSQLAsyncDataDictionary",
            {"product_version": "16.0.1000.1"},
            "uuid",
            "UNIQUEIDENTIFIER",
            "system",
        ),
        ("duckdb", "DuckDBSyncDataDictionary", "DuckDBAsyncDataDictionary", "v1.2.0", "json", "JSON", "dependencies"),
        ("duckdb", "DuckDBSyncDataDictionary", "DuckDBAsyncDataDictionary", "v1.2.0", "blob", "BLOB", "extensions"),
        ("bigquery", "BigQuerySyncDataDictionary", "BigQueryAsyncDataDictionary", None, "json", "JSON", "tables"),
        ("bigquery", "BigQuerySyncDataDictionary", "BigQueryAsyncDataDictionary", None, "boolean", "BOOL", "system"),
        (
            "spanner",
            "SpannerSyncDataDictionary",
            "SpannerAsyncDataDictionary",
            None,
            "text",
            "STRING(MAX)",
            "change_streams",
        ),
        (
            "spanner",
            "SpannerSyncDataDictionary",
            "SpannerAsyncDataDictionary",
            None,
            "blob",
            "BYTES(MAX)",
            "property_graphs",
        ),
    ],
)
async def test_extended_data_dictionary_capabilities_and_optimal_types(
    adapter_name: str,
    sync_name: str,
    async_name: str,
    version_row: Any,
    type_category: str,
    expected_type: str,
    expected_domain: str,
) -> None:
    """MSSQL, DuckDB, BigQuery, and Spanner sync/async data dictionaries align capabilities and optimal types."""
    dd_module = importlib.import_module(f"vertebra.extensions.sqlspec.adapters.{adapter_name}.data_dictionary")
    sync_dd = getattr(dd_module, sync_name)()
    async_dd = getattr(dd_module, async_name)()

    class _SyncProbeDriver:
        def select_one_or_none(self, statement: Any) -> Any:
            _ = statement
            return version_row

        def select_value_or_none(self, statement: Any) -> Any:
            _ = statement
            return version_row

    class _AsyncProbeDriver:
        async def select_one_or_none(self, statement: Any) -> Any:
            _ = statement
            return version_row

        async def select_value_or_none(self, statement: Any) -> Any:
            _ = statement
            return version_row

    sync_drv: Any = _SyncProbeDriver()
    async_drv: Any = _AsyncProbeDriver()

    sync_caps = sync_dd.get_metadata_capabilities(sync_drv)
    async_caps = await async_dd.get_metadata_capabilities(async_drv)
    assert sync_caps.dialect == adapter_name
    assert async_caps.dialect == adapter_name
    assert sync_caps.get(expected_domain) is not None
    assert async_caps.get(expected_domain) is not None
    assert sync_dd.get_optimal_type(sync_drv, type_category) == expected_type
    assert await async_dd.get_optimal_type(async_drv, type_category) == expected_type


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("dialect", "expected_profile_dialect", "expected_schema", "expected_json_type", "supports_graphs"),
    [
        ("googlesql", "spanner", "", "JSON", True),
        ("spangres", "spangres", "public", "JSONB", False),
    ],
)
async def test_spanner_data_dictionary_dual_dialect_dispatch(
    dialect: str,
    expected_profile_dialect: str,
    expected_schema: str,
    expected_json_type: str,
    supports_graphs: bool,
) -> None:
    """Spanner sync/async data dictionaries dispatch cleanly across GoogleSQL and Spangres dialects."""
    from sqlspec.data_dictionary import MetadataSupport

    from vertebra.extensions.sqlspec.adapters.spanner.data_dictionary import (
        SpannerAsyncDataDictionary,
        SpannerSyncDataDictionary,
    )

    class _SyncSpannerSpy:
        def __init__(self, dialect_name: str) -> None:
            self.dialect = dialect_name
            self.queries: list[str] = []

        def select(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
            _ = params
            self.queries.append(sql)
            return [
                {
                    "table_name": "accounts",
                    "column_name": "id",
                    "data_type": "INT64",
                    "index_name": "PRIMARY_KEY",
                    "columns": ["id"],
                }
            ]

    class _AsyncSpannerSpy:
        def __init__(self, dialect_name: str) -> None:
            self.dialect = dialect_name
            self.queries: list[str] = []

        async def select(self, sql: str, params: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
            _ = params
            self.queries.append(sql)
            return [
                {
                    "table_name": "accounts",
                    "column_name": "id",
                    "data_type": "INT64",
                    "index_name": "PRIMARY_KEY",
                    "columns": ["id"],
                }
            ]

    sync_dd = SpannerSyncDataDictionary(dialect=dialect)
    async_dd = SpannerAsyncDataDictionary(dialect=dialect)
    sync_drv: Any = _SyncSpannerSpy(dialect)
    async_drv: Any = _AsyncSpannerSpy(dialect)

    assert sync_dd.dialect == expected_profile_dialect
    assert async_dd.dialect == expected_profile_dialect
    assert sync_dd.get_dialect_config() is get_dialect_config("spanner")
    assert sync_dd.get_optimal_type(sync_drv, "json") == expected_json_type
    assert await async_dd.get_optimal_type(async_drv, "json") == expected_json_type
    assert sync_dd.get_feature_flag(sync_drv, "supports_interleaved_tables") is True
    assert await async_dd.get_feature_flag(async_drv, "supports_interleaved_tables") is True

    sync_caps = sync_dd.get_metadata_capabilities(sync_drv)
    async_caps = await async_dd.get_metadata_capabilities(async_drv)
    assert sync_caps.dialect == expected_profile_dialect
    assert async_caps.dialect == expected_profile_dialect
    expected_graph_support = MetadataSupport.SUPPORTED if supports_graphs else MetadataSupport.UNSUPPORTED
    sync_graph_cap = sync_dd.get_metadata_capabilities(sync_drv, domains=("property_graphs",)).get("property_graphs")
    async_graph_cap = (await async_dd.get_metadata_capabilities(async_drv, domains=("property_graphs",))).get(
        "property_graphs"
    )
    assert sync_graph_cap is not None and sync_graph_cap.support == expected_graph_support
    assert async_graph_cap is not None and async_graph_cap.support == expected_graph_support

    tables = sync_dd.get_tables(sync_drv)
    cols = sync_dd.get_columns(sync_drv, table="accounts")
    idxs = sync_dd.get_indexes(sync_drv, table="accounts")
    assert tables[0]["schema_name"] == expected_schema
    assert cols[0]["column_name"] == "id"
    assert idxs[0]["index_name"] == "PRIMARY_KEY"

    async_tables = await async_dd.get_tables(async_drv)
    async_cols = await async_dd.get_columns(async_drv, table="accounts")
    async_idxs = await async_dd.get_indexes(async_drv, table="accounts")
    assert async_tables[0]["schema_name"] == expected_schema
    assert async_cols[0]["column_name"] == "id"
    assert async_idxs[0]["index_name"] == "PRIMARY_KEY"


@pytest.mark.parametrize(
    ("kwargs", "valid"),
    [
        ({"project": "proj-test", "dataset": "ds_test", "region": "us"}, True),
        ({"dataset": "analytics_prod"}, True),
        ({"region": "europe-west1"}, True),
        ({"project": ""}, False),
        ({"project": "   "}, False),
        ({"project": "bad project"}, False),
        ({"project": "proj;DROP"}, False),
        ({"project": "proj..name"}, False),
        ({"dataset": ""}, False),
        ({"dataset": "bad dataset"}, False),
        ({"dataset": "ds`bad"}, False),
        ({"dataset": "proj.ds"}, False),
        ({"region": ""}, False),
        ({"region": "us central1"}, False),
    ],
)
def test_bigquery_metadata_scope_validation(kwargs: dict[str, Any], valid: bool) -> None:
    """BigQueryMetadataScope validates project, dataset, and region scopes with ImproperConfigurationError."""
    from vertebra.extensions.sqlspec.adapters.bigquery.data_dictionary import BigQueryMetadataScope
    from vertebra.typing import ImproperConfigurationError

    if valid:
        scope = BigQueryMetadataScope(**kwargs)
        assert scope.project == kwargs.get("project")
        assert scope.dataset == kwargs.get("dataset")
        assert scope.region == kwargs.get("region")
    else:
        with pytest.raises(ImproperConfigurationError):
            BigQueryMetadataScope(**kwargs)


class _LoopbackSyncNotify:
    """In-memory SyncNotificationSource double for ChangeFeed event backend testing."""

    def __init__(self, initial_frames: list[tuple[str, bytes]] | None = None) -> None:
        from vertebra.db.driver.capabilities import NotificationEvent

        self.published: list[tuple[str, bytes]] = []
        self.subscribed: list[str] = []
        self._frames = [NotificationEvent(channel=ch, payload=data) for ch, data in (initial_frames or [])]

    def publish(self, channel: str, payload: bytes | str) -> None:
        from vertebra.db.driver.capabilities import NotificationEvent

        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        self.published.append((channel, raw))
        self._frames.append(NotificationEvent(channel=channel, payload=raw))

    def subscribe(self, channels: Sequence[str]) -> None:
        self.subscribed.extend(channels)

    def unsubscribe(self, channels: Sequence[str]) -> None:
        for ch in channels:
            if ch in self.subscribed:
                self.subscribed.remove(ch)

    def listen(self) -> Any:
        while self._frames:
            yield self._frames.pop(0)


class _LoopbackAsyncNotify:
    """In-memory AsyncNotificationSource double for ChangeFeed event backend testing."""

    def __init__(self, initial_frames: list[tuple[str, bytes]] | None = None) -> None:
        from vertebra.db.driver.capabilities import NotificationEvent

        self.published: list[tuple[str, bytes]] = []
        self.subscribed: list[str] = []
        self._frames = [NotificationEvent(channel=ch, payload=data) for ch, data in (initial_frames or [])]

    async def publish(self, channel: str, payload: bytes | str) -> None:
        from vertebra.db.driver.capabilities import NotificationEvent

        raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
        self.published.append((channel, raw))
        self._frames.append(NotificationEvent(channel=channel, payload=raw))

    async def subscribe(self, channels: Sequence[str]) -> None:
        self.subscribed.extend(channels)

    async def unsubscribe(self, channels: Sequence[str]) -> None:
        for ch in channels:
            if ch in self.subscribed:
                self.subscribed.remove(ch)

    async def listen(self) -> Any:
        while self._frames:
            yield self._frames.pop(0)


@pytest.mark.parametrize(
    ("backend_cls", "protocol_cls", "expected_sync", "expected_async"),
    [
        (ChangeFeedSyncEventBackend, SyncEventBackendProtocol, True, False),
        (ChangeFeedAsyncEventBackend, AsyncEventBackendProtocol, False, True),
    ],
)
def test_changefeed_event_backends_protocol_and_slots(
    backend_cls: type[Any],
    protocol_cls: type[Any],
    expected_sync: bool,
    expected_async: bool,
) -> None:
    """ChangeFeed event backends satisfy SQLSpec 0.72.0 event protocols and enforce __slots__."""
    backend = backend_cls()
    assert isinstance(backend, protocol_cls)
    assert backend.backend_name == "changefeed"
    assert backend.supports_sync is expected_sync
    assert backend.supports_async is expected_async
    assert "__slots__" in backend_cls.__dict__
    assert not hasattr(backend, "__dict__")


@pytest.mark.parametrize(
    ("backend_cls", "ceiling"),
    [
        (ChangeFeedSyncEventBackend, 0),
        (ChangeFeedSyncEventBackend, -16),
        (ChangeFeedAsyncEventBackend, 0),
        (ChangeFeedAsyncEventBackend, -16),
    ],
)
def test_changefeed_event_backend_rejects_invalid_notify_ceiling(backend_cls: type[Any], ceiling: int) -> None:
    """Non-positive notify_ceiling_bytes fails fast with ValueError."""
    with pytest.raises(ValueError, match="notify_ceiling_bytes must be positive"):
        backend_cls(notify_ceiling_bytes=ceiling)


def test_changefeed_event_backend_rejects_mismatched_queue_store_mode(tmp_path: Path) -> None:
    """Passing an async store to sync backend or sync store to async backend fails fast."""
    from vertebra.extensions.sqlspec.adapters.sqlite.config import SQLiteAsyncConfig, SQLiteSyncConfig
    from vertebra.extensions.sqlspec.adapters.sqlite.events import SQLiteEventQueueStore
    from vertebra.typing import ImproperConfigurationError

    sync_store = SQLiteEventQueueStore(SQLiteSyncConfig(connection_config={"database": str(tmp_path / "sync.db")}))
    async_store = SQLiteEventQueueStore(SQLiteAsyncConfig(connection_config={"database": str(tmp_path / "async.db")}))

    with pytest.raises(ImproperConfigurationError, match="synchronous"):
        ChangeFeedSyncEventBackend(queue_store=async_store)
    with pytest.raises(ImproperConfigurationError, match="asynchronous"):
        ChangeFeedAsyncEventBackend(queue_store=sync_store)


@pytest.mark.anyio
async def test_changefeed_spilled_wakeup_without_queue_store_raises() -> None:
    """Receiving a spilled wakeup envelope without a configured queue store raises ImproperConfigurationError."""
    from vertebra.typing import ImproperConfigurationError

    wakeup_frame = b'{"event_id": "spilled-1", "channel": "orders", "wakeup": true, "spilled_to_queue": true}'
    sync_backend = ChangeFeedSyncEventBackend(
        notify_source=cast(Any, _LoopbackSyncNotify([("orders", wakeup_frame)])),
    )
    try:
        with pytest.raises(ImproperConfigurationError, match="BaseEventQueueStore"):
            sync_backend.dequeue("orders", 0.0)
    finally:
        sync_backend.shutdown()

    async_backend = ChangeFeedAsyncEventBackend(
        notify_source=cast(Any, _LoopbackAsyncNotify([("orders", wakeup_frame)])),
    )
    try:
        with pytest.raises(ImproperConfigurationError, match="BaseEventQueueStore"):
            await async_backend.dequeue("orders", 0.0)
    finally:
        await async_backend.shutdown()


def test_changefeed_sync_schema_reconciliation_and_publish_many_order(tmp_path: Path) -> None:
    """Sync backend reconciles queue schema via BaseEventQueueStore hooks and preserves batch order."""
    from vertebra.extensions.sqlspec.adapters.sqlite.config import SQLiteSyncConfig
    from vertebra.extensions.sqlspec.adapters.sqlite.events import SQLiteEventQueueStore

    class _SpySQLiteEventQueueStore(SQLiteEventQueueStore):
        def __init__(self, config: SQLiteSyncConfig) -> None:
            super().__init__(config)
            self.reconcile_calls: list[str] = []

        def prepare_schema_sync(self, driver: Any) -> None:
            self.reconcile_calls.append("prepare_schema_sync")
            super().prepare_schema_sync(driver)

        def get_schema_targets(self) -> Any:
            self.reconcile_calls.append("get_schema_targets")
            return super().get_schema_targets()

        def reconcile_companion_objects_sync(self, driver: Any) -> None:
            self.reconcile_calls.append("reconcile_companion_objects_sync")
            super().reconcile_companion_objects_sync(driver)

        def reconcile_native_backend_sync(
            self,
            driver: Any,
            backend_name: str,
            targets: tuple[str, ...],
            *,
            create_schema: bool,
        ) -> None:
            self.reconcile_calls.append(f"reconcile_native_backend_sync:{backend_name}:{create_schema}")
            super().reconcile_native_backend_sync(driver, backend_name, targets, create_schema=create_schema)

    db_path = tmp_path / "sync_batch.db"
    cfg = SQLiteSyncConfig(
        connection_config={"database": str(db_path)},
        extension_config={"events": {"queue_table": "vtb_cdc_queue"}},
    )
    queue_store = _SpySQLiteEventQueueStore(cfg)
    notify = _LoopbackSyncNotify()
    backend = ChangeFeedSyncEventBackend(
        queue_store=queue_store,
        notify_source=cast(Any, notify),
        checkpoint_key="sync-batch",
    )
    try:
        events: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = [
            ("ch_a", {"seq": 0}, {"kind": "inline"}),
            ("ch_b", {"blob": "x" * 9200}, {"kind": "spilled"}),
            ("ch_a", {"seq": 2}, {"kind": "inline"}),
        ]
        ids = backend.publish_many(events)
        assert len(ids) == 3
        assert len(set(ids)) == 3
        assert queue_store.reconcile_calls == [
            "prepare_schema_sync",
            "get_schema_targets",
            "reconcile_companion_objects_sync",
            "reconcile_native_backend_sync:changefeed:True",
        ]
        assert [ch for ch, _ in notify.published] == ["ch_a", "ch_b", "ch_a"]
        assert len(notify.published[0][1]) <= 7999
        assert b"spilled_to_queue" in notify.published[1][1]
        assert len(notify.published[2][1]) <= 7999

        with cfg.provide_session() as drv:
            indexes = drv.data_dictionary.get_indexes(drv, table="vtb_cdc_queue")
        assert any(row.get("index_name") == "idx_vtb_cdc_queue_channel_status" for row in indexes)

        msg0 = backend.dequeue("ch_a", 0.0)
        msg2 = backend.dequeue("ch_a", 0.0)
        msg1 = backend.dequeue("ch_b", 0.0)
        assert msg0 is not None and msg0.event_id == ids[0] and msg0.payload == {"seq": 0}
        assert msg2 is not None and msg2.event_id == ids[2] and msg2.payload == {"seq": 2}
        assert msg1 is not None and msg1.event_id == ids[1] and msg1.payload == {"blob": "x" * 9200}
        assert msg1.metadata == {"kind": "spilled"}
        backend.ack(msg0.event_id)
        backend.ack(msg1.event_id)
        backend.ack(msg2.event_id)
        assert backend.load_checkpoint("ch_a") == ids[2]
        assert backend.load_checkpoint("ch_b") == ids[1]
    finally:
        backend.shutdown()


@pytest.mark.anyio
async def test_changefeed_async_schema_reconciliation_and_publish_many_order(tmp_path: Path) -> None:
    """Async backend reconciles queue schema via BaseEventQueueStore hooks and preserves batch order."""
    from vertebra.extensions.sqlspec.adapters.sqlite.config import SQLiteAsyncConfig
    from vertebra.extensions.sqlspec.adapters.sqlite.events import SQLiteEventQueueStore

    class _SpyAsyncSQLiteEventQueueStore(SQLiteEventQueueStore):
        def __init__(self, config: SQLiteAsyncConfig) -> None:
            super().__init__(config)
            self.reconcile_calls: list[str] = []

        async def prepare_schema_async(self, driver: Any) -> None:
            self.reconcile_calls.append("prepare_schema_async")
            await super().prepare_schema_async(driver)

        def get_schema_targets(self) -> Any:
            self.reconcile_calls.append("get_schema_targets")
            return super().get_schema_targets()

        async def reconcile_companion_objects_async(self, driver: Any) -> None:
            self.reconcile_calls.append("reconcile_companion_objects_async")
            await super().reconcile_companion_objects_async(driver)

        async def reconcile_native_backend_async(
            self,
            driver: Any,
            backend_name: str,
            targets: tuple[str, ...],
            *,
            create_schema: bool,
        ) -> None:
            self.reconcile_calls.append(f"reconcile_native_backend_async:{backend_name}:{create_schema}")
            await super().reconcile_native_backend_async(driver, backend_name, targets, create_schema=create_schema)

    db_path = tmp_path / "async_batch.db"
    cfg = SQLiteAsyncConfig(
        connection_config={"database": str(db_path)},
        extension_config={"events": {"queue_table": "vtb_cdc_async_queue"}},
    )
    queue_store = _SpyAsyncSQLiteEventQueueStore(cfg)
    notify = _LoopbackAsyncNotify()
    backend = ChangeFeedAsyncEventBackend(
        queue_store=queue_store,
        notify_source=cast(Any, notify),
        checkpoint_key="async-batch",
    )
    try:
        events: list[tuple[str, dict[str, Any], dict[str, Any] | None]] = [
            ("ch_a", {"seq": 0}, {"kind": "inline"}),
            ("ch_b", {"blob": "y" * 9200}, {"kind": "spilled"}),
            ("ch_a", {"seq": 2}, {"kind": "inline"}),
        ]
        ids = await backend.publish_many(events)
        assert len(ids) == 3
        assert len(set(ids)) == 3
        assert queue_store.reconcile_calls == [
            "prepare_schema_async",
            "get_schema_targets",
            "reconcile_companion_objects_async",
            "reconcile_native_backend_async:changefeed:True",
        ]
        assert [ch for ch, _ in notify.published] == ["ch_a", "ch_b", "ch_a"]
        assert b"spilled_to_queue" in notify.published[1][1]

        async with cfg.provide_session() as drv:
            indexes = await drv.data_dictionary.get_indexes(drv, table="vtb_cdc_async_queue")
        assert any(row.get("index_name") == "idx_vtb_cdc_async_queue_channel_status" for row in indexes)

        msg0 = await backend.dequeue("ch_a", 0.0)
        msg2 = await backend.dequeue("ch_a", 0.0)
        msg1 = await backend.dequeue("ch_b", 0.0)
        assert msg0 is not None and msg0.event_id == ids[0] and msg0.payload == {"seq": 0}
        assert msg2 is not None and msg2.event_id == ids[2] and msg2.payload == {"seq": 2}
        assert msg1 is not None and msg1.event_id == ids[1] and msg1.payload == {"blob": "y" * 9200}
        assert msg1.metadata == {"kind": "spilled"}
        await backend.ack(msg0.event_id)
        await backend.ack(msg1.event_id)
        await backend.ack(msg2.event_id)
        assert await backend.load_checkpoint("ch_a") == ids[2]
        assert await backend.load_checkpoint("ch_b") == ids[1]
    finally:
        await backend.shutdown()


@pytest.mark.anyio
async def test_changefeed_table_queue_publish_many_preserves_order(tmp_path: Path) -> None:
    """Table-queue-only publish_many batches through native TableEventQueue in input order."""
    from vertebra.extensions.sqlspec.adapters.sqlite.config import SQLiteAsyncConfig, SQLiteSyncConfig
    from vertebra.extensions.sqlspec.adapters.sqlite.events import SQLiteEventQueueStore

    sync_cfg = SQLiteSyncConfig(connection_config={"database": str(tmp_path / "tbl_sync.db")})
    sync_backend = ChangeFeedSyncEventBackend(queue_store=SQLiteEventQueueStore(sync_cfg))
    try:
        sync_ids = sync_backend.publish_many([("jobs", {"i": 1}, None), ("jobs", {"i": 2}, None)])
        assert len(sync_ids) == 2
        m1 = sync_backend.dequeue("jobs", 0.0)
        m2 = sync_backend.dequeue("jobs", 0.0)
        assert m1 is not None and m1.event_id == sync_ids[0] and m1.payload == {"i": 1}
        assert m2 is not None and m2.event_id == sync_ids[1] and m2.payload == {"i": 2}
    finally:
        sync_backend.shutdown()

    async_cfg = SQLiteAsyncConfig(connection_config={"database": str(tmp_path / "tbl_async.db")})
    async_backend = ChangeFeedAsyncEventBackend(queue_store=SQLiteEventQueueStore(async_cfg))
    try:
        async_ids = await async_backend.publish_many([("jobs", {"i": 10}, None), ("jobs", {"i": 20}, None)])
        assert len(async_ids) == 2
        am1 = await async_backend.dequeue("jobs", 0.0)
        am2 = await async_backend.dequeue("jobs", 0.0)
        assert am1 is not None and am1.event_id == async_ids[0] and am1.payload == {"i": 10}
        assert am2 is not None and am2.event_id == async_ids[1] and am2.payload == {"i": 20}
    finally:
        await async_backend.shutdown()


class _CountingSyncCursor:
    """Proxy around a native sync cursor counting DB-API method and property accesses."""

    def __init__(self, inner: Any, counter: dict[str, int]) -> None:
        self._inner = inner
        self._counter = counter

    def execute(self, sql: str, params: Any = None) -> Any:
        self._counter["execute"] = self._counter.get("execute", 0) + 1
        if params is None:
            return self._inner.execute(sql)
        return self._inner.execute(sql, params)

    def executemany(self, sql: str, params_seq: Any) -> Any:
        self._counter["executemany"] = self._counter.get("executemany", 0) + 1
        return self._inner.executemany(sql, params_seq)

    def fetchall(self) -> Any:
        self._counter["fetchall"] = self._counter.get("fetchall", 0) + 1
        return self._inner.fetchall()

    def fetchone(self) -> Any:
        self._counter["fetchone"] = self._counter.get("fetchone", 0) + 1
        return self._inner.fetchone()

    def fetchmany(self, size: int | None = None) -> Any:
        self._counter["fetchmany"] = self._counter.get("fetchmany", 0) + 1
        if size is None:
            return self._inner.fetchmany()
        return self._inner.fetchmany(size)

    def close(self) -> None:
        self._counter["close"] = self._counter.get("close", 0) + 1
        self._inner.close()

    @property
    def description(self) -> Any:
        self._counter["description"] = self._counter.get("description", 0) + 1
        return self._inner.description

    @property
    def rowcount(self) -> int:
        self._counter["rowcount"] = self._counter.get("rowcount", 0) + 1
        return int(self._inner.rowcount)

    @property
    def lastrowid(self) -> Any:
        self._counter["lastrowid"] = self._counter.get("lastrowid", 0) + 1
        return self._inner.lastrowid


class _CountingSyncDriver:
    """Proxy around a real native sync driver counting cursor openings and forwarded attributes."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.config = inner.config
        self.cursor_calls = 0
        self.cursor_counts: dict[str, int] = {}
        self.forwarded_names: list[str] = []

    def cursor(self) -> _CountingSyncCursor:
        self.cursor_calls += 1
        return _CountingSyncCursor(self._inner.cursor(), self.cursor_counts)

    def __getattr__(self, name: str) -> Any:
        self.forwarded_names.append(name)
        return getattr(self._inner, name)


def test_native_sync_cache_hit_opens_no_cursor() -> None:
    """A cached sync SQLSpec query calls the bound client directly without opening a cursor."""
    native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    counting = _CountingSyncDriver(native)
    adapter = SQLiteSyncDriver(counting)
    try:
        assert counting.forwarded_names.count("_fetch_rows") == 1
        assert counting.forwarded_names.count("_run_dml") == 1

        first = adapter.execute("SELECT ? AS a", 10)
        assert first.all() == [{"a": 10}]

        cursor_calls_before = counting.cursor_calls
        execute_calls_before = counting.cursor_counts.get("execute", 0)
        counting.forwarded_names.clear()

        second = adapter.execute("SELECT ? AS a", 25)
        assert second.all() == [{"a": 25}]
        assert counting.cursor_calls == cursor_calls_before
        assert counting.cursor_counts.get("execute", 0) == execute_calls_before
        assert counting.forwarded_names == []
    finally:
        native.close()


@pytest.mark.parametrize("engine", ["sqlite", "duckdb"])
def test_native_sync_with_cursor_yields_closed_on_exit_native_cursor(engine: str) -> None:
    """with_cursor yields a native cursor distinct from connection and closes it on block exit."""
    from vertebra._vertebra import InterfaceError as NativeInterfaceError
    from vertebra.db import dbapi
    from vertebra.db.adapters.duckdb import DuckDBConfig, SyncDuckDBDriver
    from vertebra.db.adapters.sqlite import SyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBSyncDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteSyncDriver

    assert dbapi.InterfaceError is NativeInterfaceError
    native: Any
    adapter: BaseSyncDriver
    if engine == "sqlite":
        native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
        adapter = SQLiteSyncDriver(native)
    else:
        native = SyncDuckDBDriver(DuckDBConfig(path=":memory:"))
        adapter = DuckDBSyncDriver(native)
    try:
        with adapter.with_cursor(adapter.connection) as cursor:
            assert cursor is not adapter.connection
        with pytest.raises(dbapi.InterfaceError):
            cursor.fetchall()
    finally:
        native.close()


def test_native_sync_dispatch_reads_description_rowcount_and_lastrowid() -> None:
    """Native sync dispatch populates rows_affected, RETURNING data, and zero-row column_names from cursor state."""
    from vertebra.db.adapters.sqlite import SyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteSyncDriver

    native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    adapter = SQLiteSyncDriver(native)
    try:
        adapter.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)")
        ins = adapter.execute("INSERT INTO items (name) VALUES (?)", "alpha")
        assert ins.rows_affected == 1

        ret = adapter.execute("INSERT INTO items (name) VALUES (?) RETURNING id", "beta")
        assert ret.all() == [{"id": 2}]

        empty = adapter.execute("SELECT id, name FROM items WHERE id = ?", 999)
        assert empty.all() == []
        assert empty.column_names == ["id", "name"]
    finally:
        native.close()


@pytest.mark.parametrize("engine", ["sqlite", "duckdb"])
def test_native_sync_execute_many_reports_engine_rowcount(engine: str) -> None:
    """Native sync execute_many reports affected row counts for non-empty batches and skips native calls on empty."""
    from vertebra.db.adapters.duckdb import DuckDBConfig, SyncDuckDBDriver
    from vertebra.db.adapters.sqlite import SyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBSyncDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteSyncDriver

    native: Any
    adapter: BaseSyncDriver
    if engine == "sqlite":
        native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
        counting = _CountingSyncDriver(native)
        adapter = SQLiteSyncDriver(counting)
    else:
        native = SyncDuckDBDriver(DuckDBConfig(path=":memory:"))
        counting = _CountingSyncDriver(native)
        adapter = DuckDBSyncDriver(counting)
    try:
        adapter.execute("CREATE TABLE batch_items (id INTEGER, name TEXT)")
        res = adapter.execute_many(
            "INSERT INTO batch_items (id, name) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        assert res.rows_affected == 3

        many_calls_before = counting.cursor_counts.get("executemany", 0)
        empty_res = adapter.execute_many("INSERT INTO batch_items (id, name) VALUES (?, ?)", [])
        assert empty_res.rows_affected == 0
        assert counting.cursor_counts.get("executemany", 0) == many_calls_before
    finally:
        native.close()


def test_native_sync_cursor_errors_map_to_sqlspec_exceptions() -> None:
    """Native cursor errors map to sqlspec.exceptions preserving the native exception as __cause__."""
    from vertebra.db.adapters.sqlite import SyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteSyncDriver

    native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    adapter = SQLiteSyncDriver(native)
    try:
        adapter.execute("CREATE TABLE uniq_items (id INTEGER PRIMARY KEY)")
        adapter.execute("INSERT INTO uniq_items (id) VALUES (?)", 1)

        with pytest.raises(UniqueViolationError) as dup_info:
            adapter.execute("INSERT INTO uniq_items (id) VALUES (?)", 1)
        assert type(dup_info.value.__cause__) is NativeIntegrityError

        with pytest.raises(SQLParsingError):
            adapter.execute("SELECT * FROM missing_table_xyz")
    finally:
        native.close()


def test_native_sync_driver_reuses_one_exception_handler() -> None:
    """Every BaseSyncDriver reuses one exception handler clearing pending errors across calls."""
    from vertebra.db.adapters.sqlite import SyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteSyncDriver

    native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    adapter = SQLiteSyncDriver(native)
    try:
        assert adapter.handle_database_exceptions() is adapter.handle_database_exceptions()
        with pytest.raises(SQLParsingError):
            adapter.execute("SELECT * FROM nonexistent_table_abc")
        ok = adapter.execute("SELECT 1 AS v")
        assert ok.all() == [{"v": 1}]
    finally:
        native.close()

    sample = _SampleSyncAdapter(_RecordingSyncNativeDriver())
    assert sample.handle_database_exceptions() is sample.handle_database_exceptions()


@pytest.mark.parametrize(
    ("exc_factory", "expected_type"),
    [
        (lambda dbapi: dbapi.IntegrityError("UNIQUE constraint failed"), UniqueViolationError),
        (lambda dbapi: dbapi.DataError("invalid value"), DataError),
        (lambda dbapi: dbapi.NotSupportedError("unsupported feature"), DialectNotSupportedError),
        (lambda dbapi: dbapi.OperationalError("database is locked"), OperationalError),
        (lambda dbapi: dbapi.ProgrammingError("syntax error"), SQLParsingError),
        (lambda dbapi: dbapi.InterfaceError("closed"), OperationalError),
        (lambda _dbapi: QueryCancelledError("stopped"), OperationCancelledError),
    ],
)
def test_map_native_database_exception_matches_cursor_error_classes(
    exc_factory: Any,
    expected_type: type[Exception],
) -> None:
    """_map_native_database_exception maps DB-API native cursor errors and QueryCancelledError to SQLSpec types."""
    from vertebra.db import dbapi

    mapped = BaseSyncDriver._map_native_database_exception(exc_factory(dbapi))
    assert isinstance(mapped, expected_type)


class _CountingAsyncCursor:
    """Proxy around a native async cursor counting DB-API method and property accesses."""

    def __init__(self, inner: Any, counter: dict[str, int]) -> None:
        self._inner = inner
        self._counter = counter

    async def execute(self, sql: str, params: Any = None) -> Any:
        self._counter["execute"] = self._counter.get("execute", 0) + 1
        if params is None:
            return await self._inner.execute(sql)
        return await self._inner.execute(sql, params)

    async def executemany(self, sql: str, params_seq: Any) -> Any:
        self._counter["executemany"] = self._counter.get("executemany", 0) + 1
        return await self._inner.executemany(sql, params_seq)

    async def fetchall(self) -> Any:
        self._counter["fetchall"] = self._counter.get("fetchall", 0) + 1
        return await self._inner.fetchall()

    async def fetchone(self) -> Any:
        self._counter["fetchone"] = self._counter.get("fetchone", 0) + 1
        return await self._inner.fetchone()

    async def fetchmany(self, size: int | None = None) -> Any:
        self._counter["fetchmany"] = self._counter.get("fetchmany", 0) + 1
        if size is None:
            return await self._inner.fetchmany()
        return await self._inner.fetchmany(size)

    async def close(self) -> None:
        self._counter["close"] = self._counter.get("close", 0) + 1
        await self._inner.close()

    @property
    def description(self) -> Any:
        self._counter["description"] = self._counter.get("description", 0) + 1
        return self._inner.description

    @property
    def rowcount(self) -> int:
        self._counter["rowcount"] = self._counter.get("rowcount", 0) + 1
        return int(self._inner.rowcount)

    @property
    def lastrowid(self) -> Any:
        self._counter["lastrowid"] = self._counter.get("lastrowid", 0) + 1
        return self._inner.lastrowid


class _CountingAsyncDriver:
    """Proxy around a real native async driver counting cursor openings and forwarded attributes."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.config = inner.config
        self.cursor_calls = 0
        self.cursor_counts: dict[str, int] = {}
        self.forwarded_names: list[str] = []

    def cursor(self) -> _CountingAsyncCursor:
        self.cursor_calls += 1
        return _CountingAsyncCursor(self._inner.cursor(), self.cursor_counts)

    def __getattr__(self, name: str) -> Any:
        self.forwarded_names.append(name)
        return getattr(self._inner, name)


@pytest.mark.anyio
async def test_native_async_cache_hit_opens_no_cursor() -> None:
    """A cached async SQLSpec query calls the bound client directly without opening a cursor."""
    native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    counting = _CountingAsyncDriver(native)
    adapter = SQLiteAsyncDriver(counting)
    try:
        assert counting.forwarded_names.count("_fetch_rows") == 1
        assert counting.forwarded_names.count("_run_dml") == 1

        first = await adapter.execute("SELECT ? AS a", 10)
        assert first.all() == [{"a": 10}]

        cursor_calls_before = counting.cursor_calls
        execute_calls_before = counting.cursor_counts.get("execute", 0)
        counting.forwarded_names.clear()

        second = await adapter.execute("SELECT ? AS a", 25)
        assert second.all() == [{"a": 25}]
        assert counting.cursor_calls == cursor_calls_before
        assert counting.cursor_counts.get("execute", 0) == execute_calls_before
        assert counting.forwarded_names == []
    finally:
        await native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("engine", ["sqlite", "duckdb"])
async def test_native_async_with_cursor_yields_closed_on_exit_native_cursor(engine: str) -> None:
    """with_cursor yields a native async cursor distinct from connection and closes it on block exit."""
    from vertebra._vertebra import InterfaceError as NativeInterfaceError
    from vertebra.db import dbapi
    from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig
    from vertebra.db.adapters.sqlite import AsyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBAsyncDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver

    assert dbapi.InterfaceError is NativeInterfaceError
    native: Any
    adapter: BaseAsyncDriver
    if engine == "sqlite":
        native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
        adapter = SQLiteAsyncDriver(native)
    else:
        native = AsyncDuckDBDriver(DuckDBConfig(path=":memory:"))
        adapter = DuckDBAsyncDriver(native)
    try:
        async with adapter.with_cursor(adapter.connection) as cursor:
            assert cursor is not adapter.connection
        with pytest.raises(dbapi.InterfaceError):
            await cursor.fetchall()
    finally:
        await native.close()


@pytest.mark.anyio
async def test_native_async_dispatch_reads_description_rowcount_and_lastrowid() -> None:
    """Native async dispatch populates rows_affected, RETURNING data, and zero-row column_names from cursor state."""
    from vertebra.db.adapters.sqlite import AsyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver

    native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    adapter = SQLiteAsyncDriver(native)
    try:
        await adapter.execute("CREATE TABLE items (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)")
        ins = await adapter.execute("INSERT INTO items (name) VALUES (?)", "alpha")
        assert ins.rows_affected == 1

        ret = await adapter.execute("INSERT INTO items (name) VALUES (?) RETURNING id", "beta")
        assert ret.all() == [{"id": 2}]

        empty = await adapter.execute("SELECT id, name FROM items WHERE id = ?", 999)
        assert empty.all() == []
        assert empty.column_names == ["id", "name"]
    finally:
        await native.close()


@pytest.mark.anyio
@pytest.mark.parametrize("engine", ["sqlite", "duckdb"])
async def test_native_async_execute_many_reports_engine_rowcount(engine: str) -> None:
    """Native async execute_many reports affected row counts for non-empty batches and skips native calls on empty."""
    from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig
    from vertebra.db.adapters.sqlite import AsyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBAsyncDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver

    native: Any
    adapter: BaseAsyncDriver
    if engine == "sqlite":
        native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
        counting = _CountingAsyncDriver(native)
        adapter = SQLiteAsyncDriver(counting)
    else:
        native = AsyncDuckDBDriver(DuckDBConfig(path=":memory:"))
        counting = _CountingAsyncDriver(native)
        adapter = DuckDBAsyncDriver(counting)
    try:
        await adapter.execute("CREATE TABLE batch_items (id INTEGER, name TEXT)")
        res = await adapter.execute_many(
            "INSERT INTO batch_items (id, name) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c")],
        )
        assert res.rows_affected == 3

        many_calls_before = counting.cursor_counts.get("executemany", 0)
        empty_res = await adapter.execute_many("INSERT INTO batch_items (id, name) VALUES (?, ?)", [])
        assert empty_res.rows_affected == 0
        assert counting.cursor_counts.get("executemany", 0) == many_calls_before
    finally:
        await native.close()


@pytest.mark.anyio
async def test_native_async_cursor_errors_map_to_sqlspec_exceptions() -> None:
    """Native async cursor errors map to sqlspec.exceptions preserving the native exception as __cause__."""
    from vertebra.db.adapters.sqlite import AsyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver

    native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    adapter = SQLiteAsyncDriver(native)
    try:
        await adapter.execute("CREATE TABLE uniq_items (id INTEGER PRIMARY KEY)")
        await adapter.execute("INSERT INTO uniq_items (id) VALUES (?)", 1)

        with pytest.raises(UniqueViolationError) as dup_info:
            await adapter.execute("INSERT INTO uniq_items (id) VALUES (?)", 1)
        assert type(dup_info.value.__cause__) is NativeIntegrityError

        with pytest.raises(SQLParsingError):
            await adapter.execute("SELECT * FROM missing_table_xyz")
    finally:
        await native.close()


@pytest.mark.anyio
async def test_native_async_driver_reuses_one_exception_handler() -> None:
    """Every BaseAsyncDriver reuses one exception handler clearing pending errors across calls."""
    from vertebra.db.adapters.sqlite import AsyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver

    native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    adapter = SQLiteAsyncDriver(native)
    try:
        assert adapter.handle_database_exceptions() is adapter.handle_database_exceptions()
        with pytest.raises(SQLParsingError):
            await adapter.execute("SELECT * FROM nonexistent_table_abc")
        ok = await adapter.execute("SELECT 1 AS v")
        assert ok.all() == [{"v": 1}]
    finally:
        await native.close()

    sample = _SampleAsyncAdapter(_RecordingAsyncNativeDriver())
    assert sample.handle_database_exceptions() is sample.handle_database_exceptions()


def test_sqlite_select_stream_yields_expected_chunks() -> None:
    """select_stream on SQLite with chunk_size=2 over five rows yields chunks of two, two, and one."""
    from vertebra.db.adapters.sqlite import SyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteSyncDriver

    native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
    adapter = SQLiteSyncDriver(native)
    try:
        adapter.execute("CREATE TABLE stream_items (id INTEGER PRIMARY KEY, name TEXT)")
        adapter.execute_many(
            "INSERT INTO stream_items (id, name) VALUES (?, ?)",
            [(1, "a"), (2, "b"), (3, "c"), (4, "d"), (5, "e")],
        )
        batches = list(adapter.stream_arrow_batches("SELECT id, name FROM stream_items ORDER BY id", batch_size=2))
        assert [batch.num_rows for batch in batches] == [2, 2, 1]

        with adapter.select_stream("SELECT id, name FROM stream_items ORDER BY id", chunk_size=2) as stream:
            rows = list(stream)
        assert [row["id"] for row in rows] == [1, 2, 3, 4, 5]
    finally:
        native.close()


_CACHE_HIT_SYNC_ADAPTERS = [
    pytest.param(SQLiteSyncDriver, True, True, True, "fetch_rows", "client_execute_dml", id="sqlite"),
    pytest.param(DuckDBSyncDriver, True, True, True, "fetch_rows", "client_execute_dml", id="duckdb"),
    pytest.param(PostgresSyncDriver, True, True, False, "execute_with_description", "execute_dml", id="postgres"),
    pytest.param(MySQLSyncDriver, True, True, False, "execute_with_description", "execute_dml", id="mysql"),
    pytest.param(MSSQLSyncDriver, True, True, False, "execute_with_description", "execute_dml", id="mssql"),
    pytest.param(OracleSyncDriver, False, False, False, "", "", id="oracle"),
    pytest.param(SpannerSyncDriver, False, True, False, "", "", id="spanner"),
    pytest.param(BigQuerySyncDriver, False, True, False, "", "", id="bigquery"),
]

_REGISTERED_CACHE_HIT_SYNC_ADAPTERS = [
    pytest.param(SQLiteSyncDriver, True, "fetch_rows", "client_execute_dml", id="sqlite"),
    pytest.param(DuckDBSyncDriver, True, "fetch_rows", "client_execute_dml", id="duckdb"),
    pytest.param(PostgresSyncDriver, False, "execute_with_description", "execute_dml", id="postgres"),
    pytest.param(MySQLSyncDriver, False, "execute_with_description", "execute_dml", id="mysql"),
    pytest.param(MSSQLSyncDriver, False, "execute_with_description", "execute_dml", id="mssql"),
]


@pytest.mark.parametrize(
    ("adapter_cls", "base_hooks_unchanged", "registered", "with_client", "fetch_call", "dml_call"),
    _CACHE_HIT_SYNC_ADAPTERS,
)
def test_sync_cache_hit_executor_registration_follows_hook_overrides(
    adapter_cls: type[BaseSyncDriver],
    base_hooks_unchanged: bool,
    registered: bool,
    with_client: bool,
    fetch_call: str,
    dml_call: str,
) -> None:
    """Sync cache-hit executor registers when base hooks match or the adapter defines its own callables."""
    _ = (fetch_call, dml_call)
    assert cache_hit_hooks_unchanged(adapter_cls, BaseSyncDriver) is base_hooks_unchanged
    adapter = adapter_cls(_SingleCallSyncNative(with_client=with_client))
    if registered:
        assert adapter._cache_hit_executor is BaseSyncDriver._run_cached_statement
        assert isinstance(adapter._cache_hit_state, tuple)
        assert len(adapter._cache_hit_state) == 3
    else:
        assert adapter._cache_hit_executor is None
        assert adapter._cache_hit_state is None


@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_SYNC_ADAPTERS,
)
@pytest.mark.parametrize(
    ("sql", "kind"),
    [
        pytest.param("SELECT 1 AS a", "fetch", id="select"),
        pytest.param("UPDATE t SET a = 2", "dml", id="update"),
    ],
)
def test_sync_cache_hit_makes_one_driver_call(
    adapter_cls: type[BaseSyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
    sql: str,
    kind: str,
) -> None:
    """A cached statement makes exactly one single-call driver invocation with no cursor opened."""
    native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    adapter = adapter_cls(native)
    assert adapter._stmt_cache_enabled

    adapter.execute(sql)
    native.calls.clear()
    adapter.execute(sql)

    expected_call = fetch_call if kind == "fetch" else dml_call
    cached = adapter._stmt_cache.get(sql)
    assert cached is not None
    assert len(native.calls) == 1
    assert native.calls[0][0] == expected_call
    assert native.calls[0][1] == cached.compiled_sql
    assert all(entry[0] != "cursor" for entry in native.calls)


@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_SYNC_ADAPTERS,
)
def test_sync_cache_miss_runs_through_cursor(
    adapter_cls: type[BaseSyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
) -> None:
    """The first execution of a statement misses the cache and runs through a native cursor."""
    _ = (fetch_call, dml_call)
    native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    adapter = adapter_cls(native)
    assert adapter._stmt_cache_enabled

    result = adapter.execute("SELECT 1 AS a")

    call_names = [entry[0] for entry in native.calls]
    assert "cursor" in call_names
    assert "execute" in call_names
    assert result.all() == [{"a": 1}]


@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_SYNC_ADAPTERS,
)
@pytest.mark.parametrize(
    ("case_name", "sql", "rows", "columns", "rowcount", "lastrowid"),
    [
        pytest.param("select", "SELECT 1 AS a", [{"a": 1}], ["a"], 1, 7, id="select"),
        pytest.param("empty_select", "SELECT 1 AS a", [], ["a"], 0, None, id="empty-select"),
        pytest.param("insert", "INSERT INTO t (a) VALUES (1)", [], [], 1, 7, id="insert"),
        pytest.param("update", "UPDATE t SET a = 2", [], [], 3, 7, id="update"),
        pytest.param("update_no_match", "UPDATE t SET a = 2", [], [], 0, 7, id="update-no-match"),
    ],
)
def test_sync_cache_hit_result_matches_method_path(
    adapter_cls: type[BaseSyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
    case_name: str,
    sql: str,
    rows: list[dict[str, Any]],
    columns: list[str],
    rowcount: int,
    lastrowid: int | None,
) -> None:
    """Cached execution results match the method path across SELECT and DML shapes."""
    _ = (fetch_call, dml_call)
    exec_native = _SingleCallSyncNative(
        rows=rows,
        columns=columns,
        rowcount=rowcount,
        lastrowid=lastrowid,
        with_client=with_client,
    )
    method_native = _SingleCallSyncNative(
        rows=rows,
        columns=columns,
        rowcount=rowcount,
        lastrowid=lastrowid,
        with_client=with_client,
    )
    exec_adapter = adapter_cls(exec_native)
    method_adapter = adapter_cls(method_native)
    assert exec_adapter._stmt_cache_enabled
    assert method_adapter._stmt_cache_enabled
    method_adapter._cache_hit_executor = None

    exec_adapter.execute(sql)
    method_adapter.execute(sql)
    exec_result = exec_adapter.execute(sql)
    method_result = method_adapter.execute(sql)

    assert exec_result.all() == method_result.all()
    assert exec_result.column_names == method_result.column_names
    assert exec_result.rows_affected == method_result.rows_affected

    if case_name == "select":
        assert exec_result.all() == [{"a": 1}]
    elif case_name == "empty_select":
        assert exec_result.all() == []
        assert exec_result.column_names == ["a"]
    elif case_name == "insert":
        assert exec_result.rows_affected == 1
        assert exec_result.last_inserted_id == 7
    elif case_name == "update":
        assert exec_result.rows_affected == 3
        assert exec_result.last_inserted_id is None
    elif case_name == "update_no_match":
        assert exec_result.rows_affected == 0
        assert exec_result.last_inserted_id is None


@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call"),
    [
        pytest.param(SQLiteSyncDriver, True, "fetch_rows", id="sqlite"),
        pytest.param(DuckDBSyncDriver, True, "fetch_rows", id="duckdb"),
        pytest.param(PostgresSyncDriver, False, "execute_with_description", id="postgres"),
    ],
)
def test_sync_cache_hit_returning_clause_builds_row_result(
    adapter_cls: type[BaseSyncDriver],
    with_client: bool,
    fetch_call: str,
) -> None:
    """INSERT ... RETURNING routes through the fetch callable and matches the method path."""
    sql = "INSERT INTO t (a) VALUES (1) RETURNING a"
    exec_native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    method_native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    exec_adapter = adapter_cls(exec_native)
    method_adapter = adapter_cls(method_native)
    assert exec_adapter._stmt_cache_enabled
    method_adapter._cache_hit_executor = None

    exec_adapter.execute(sql)
    method_adapter.execute(sql)
    exec_native.calls.clear()

    exec_result = exec_adapter.execute(sql)
    method_result = method_adapter.execute(sql)

    assert len(exec_native.calls) == 1
    assert exec_native.calls[0][0] == fetch_call
    assert exec_result.all() == [{"a": 1}]
    assert exec_result.all() == method_result.all()
    assert exec_result.column_names == method_result.column_names
    assert exec_result.rows_affected == method_result.rows_affected


_SYNC_CACHE_HIT_MAPPED_ERRORS = [
    pytest.param(
        lambda: NativeIntegrityError("UNIQUE constraint failed: t.id"),
        UniqueViolationError,
        id="integrity",
    ),
    pytest.param(
        lambda: ProgrammingError("syntax error"),
        SQLParsingError,
        id="programming",
    ),
    pytest.param(
        lambda: NativeOperationalError("database is locked"),
        OperationalError,
        id="operational",
    ),
    pytest.param(
        lambda: InterfaceError("closed"),
        OperationalError,
        id="interface",
    ),
    pytest.param(
        lambda: QueryCancelledError("stopped"),
        OperationCancelledError,
        id="cancelled",
    ),
]


@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_SYNC_ADAPTERS,
)
@pytest.mark.parametrize(
    ("exc_factory", "mapped"),
    [
        pytest.param(lambda: NativeIntegrityError("UNIQUE constraint failed: t.id"), True, id="integrity"),
        pytest.param(lambda: ProgrammingError("syntax error"), True, id="programming"),
        pytest.param(lambda: NativeOperationalError("database is locked"), True, id="operational"),
        pytest.param(lambda: InterfaceError("closed"), True, id="interface"),
        pytest.param(lambda: QueryCancelledError("stopped"), True, id="cancelled"),
        pytest.param(lambda: ValueError("boom"), False, id="unmapped"),
    ],
)
def test_sync_cache_hit_errors_match_method_path(
    adapter_cls: type[BaseSyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
    exc_factory: Any,
    mapped: bool,
) -> None:
    """Errors raised on the cache-hit executor match the method path class and preserve __cause__."""
    _ = (fetch_call, dml_call)
    exec_native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    method_native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    exec_adapter = adapter_cls(exec_native)
    method_adapter = adapter_cls(method_native)
    assert exec_adapter._stmt_cache_enabled
    method_adapter._cache_hit_executor = None

    exec_adapter.execute("SELECT 1 AS a")
    method_adapter.execute("SELECT 1 AS a")

    exec_error = exc_factory()
    method_error = exc_factory()
    exec_native.fail_with = exec_error
    method_native.fail_with = method_error

    with pytest.raises(Exception) as exec_info:
        exec_adapter.execute("SELECT 1 AS a")
    with pytest.raises(Exception) as method_info:
        method_adapter.execute("SELECT 1 AS a")

    assert type(exec_info.value) is type(method_info.value)
    if mapped:
        assert exec_info.value.__cause__ is exec_error
    else:
        assert exec_info.value is exec_error


@pytest.mark.parametrize(("exc_factory", "expected_cls"), _SYNC_CACHE_HIT_MAPPED_ERRORS)
def test_sync_cache_hit_error_classes_on_base_adapter(
    exc_factory: Any,
    expected_cls: type[Exception],
) -> None:
    """BaseSyncDriver cache-hit executor maps native errors to expected SQLSpec exception classes."""
    native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"])
    adapter = _SampleSyncAdapter(native)
    assert adapter._stmt_cache_enabled

    adapter.execute("SELECT 1 AS a")
    native_error = exc_factory()
    native.fail_with = native_error

    with pytest.raises(expected_cls) as exc_info:
        adapter.execute("SELECT 1 AS a")
    assert exc_info.value.__cause__ is native_error


def test_sync_cache_hit_with_active_observability_uses_observed_path() -> None:
    """Active statement observers route cached statements through the observed cursor path."""
    events: list[Any] = []
    native = _SingleCallSyncNative(rows=[{"a": 1}], columns=["a"])
    adapter = _SampleSyncAdapter(
        native,
        observability=ObservabilityRuntime(ObservabilityConfig(statement_observers=(events.append,))),
    )
    assert adapter._stmt_cache_enabled

    adapter.execute("SELECT 1 AS a")
    native.calls.clear()
    adapter.execute("SELECT 1 AS a")

    call_names = [entry[0] for entry in native.calls]
    assert "cursor" in call_names
    assert "execute_with_description" not in call_names
    assert len(events) == 2


@pytest.mark.parametrize("engine", ["sqlite", "duckdb"])
def test_sync_cache_hit_on_real_embedded_engines(engine: str) -> None:
    """Real SQLite and DuckDB sync drivers execute cached DML, SELECT, duplicate-key, and closed-client paths."""
    native: Any
    exec_adapter: BaseSyncDriver
    method_adapter: BaseSyncDriver
    if engine == "sqlite":
        native = SyncSQLiteDriver(SQLiteConfig(path=":memory:"))
        exec_adapter = SQLiteSyncDriver(native)
        method_adapter = SQLiteSyncDriver(native)
    else:
        native = SyncDuckDBDriver(DuckDBConfig(path=":memory:"))
        exec_adapter = DuckDBSyncDriver(native)
        method_adapter = DuckDBSyncDriver(native)
    method_adapter._cache_hit_executor = None

    try:
        assert exec_adapter._stmt_cache_enabled
        exec_adapter.execute("CREATE TABLE cache_hit_items (id INTEGER PRIMARY KEY, name TEXT)")

        first_insert = exec_adapter.execute("INSERT INTO cache_hit_items (id, name) VALUES (?, ?)", (1, "alpha"))
        assert first_insert.rows_affected == 1

        second_insert = exec_adapter.execute("INSERT INTO cache_hit_items (id, name) VALUES (?, ?)", (2, "beta"))
        assert second_insert.rows_affected == 1
        if engine == "sqlite":
            assert second_insert.last_inserted_id == 2

        first_select = exec_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
        second_select = exec_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
        expected_rows = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
        assert first_select.all() == expected_rows
        assert second_select.all() == expected_rows

        with pytest.raises(UniqueViolationError) as dup_info:
            exec_adapter.execute("INSERT INTO cache_hit_items (id, name) VALUES (?, ?)", (1, "dup"))
        assert type(dup_info.value.__cause__) is NativeIntegrityError

        method_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
    finally:
        native.close()

    with pytest.raises(Exception) as exec_closed:
        exec_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
    with pytest.raises(Exception) as method_closed:
        method_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")

    assert type(exec_closed.value.__cause__) is InterfaceError
    assert type(exec_closed.value) is type(method_closed.value)


def test_sync_base_execute_dml_runs_one_cursor_statement() -> None:
    """SyncDatabaseDriver.execute_dml executes once on a cursor, normalizes empty params, and closes the cursor."""
    native = _RecordingSyncNativeDriver()
    result = native.execute_dml("UPDATE t SET a = 2")
    assert result == (1, None)
    assert native.calls == [("execute", "UPDATE t SET a = 2", ())]

    class _CapturingCursorNative(_RecordingSyncNativeDriver):
        def __init__(self) -> None:
            super().__init__()
            self.last_cursor: _RecordingSyncCursor | None = None

        def cursor(self, *, lazy_begin: bool = False) -> _RecordingSyncCursor:
            self.last_cursor = super().cursor(lazy_begin=lazy_begin)
            return self.last_cursor

    capturing = _CapturingCursorNative()
    assert capturing.execute_dml("UPDATE t SET a = 2") == (1, None)
    assert capturing.last_cursor is not None
    assert capturing.last_cursor.closed is True


_CACHE_HIT_ASYNC_ADAPTERS = [
    pytest.param(SQLiteAsyncDriver, True, True, True, "fetch_rows", "client_execute_dml", id="sqlite"),
    pytest.param(DuckDBAsyncDriver, True, True, True, "fetch_rows", "client_execute_dml", id="duckdb"),
    pytest.param(PostgresAsyncDriver, True, True, False, "execute_with_description", "execute_dml", id="postgres"),
    pytest.param(MySQLAsyncDriver, True, True, False, "execute_with_description", "execute_dml", id="mysql"),
    pytest.param(MSSQLAsyncDriver, True, True, False, "execute_with_description", "execute_dml", id="mssql"),
    pytest.param(OracleAsyncDriver, False, False, False, "", "", id="oracle"),
    pytest.param(SpannerAsyncDriver, False, True, False, "", "", id="spanner"),
    pytest.param(BigQueryAsyncDriver, False, True, False, "", "", id="bigquery"),
]

_REGISTERED_CACHE_HIT_ASYNC_ADAPTERS = [
    pytest.param(SQLiteAsyncDriver, True, "fetch_rows", "client_execute_dml", id="sqlite"),
    pytest.param(DuckDBAsyncDriver, True, "fetch_rows", "client_execute_dml", id="duckdb"),
    pytest.param(PostgresAsyncDriver, False, "execute_with_description", "execute_dml", id="postgres"),
    pytest.param(MySQLAsyncDriver, False, "execute_with_description", "execute_dml", id="mysql"),
    pytest.param(MSSQLAsyncDriver, False, "execute_with_description", "execute_dml", id="mssql"),
]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_cls", "base_hooks_unchanged", "registered", "with_client", "fetch_call", "dml_call"),
    _CACHE_HIT_ASYNC_ADAPTERS,
)
async def test_async_cache_hit_executor_registration_follows_hook_overrides(
    adapter_cls: type[BaseAsyncDriver],
    base_hooks_unchanged: bool,
    registered: bool,
    with_client: bool,
    fetch_call: str,
    dml_call: str,
) -> None:
    """Async cache-hit executor registers when base hooks match or the adapter defines its own callables."""
    _ = (fetch_call, dml_call)
    assert cache_hit_hooks_unchanged(adapter_cls, BaseAsyncDriver) is base_hooks_unchanged
    adapter = adapter_cls(_SingleCallAsyncNative(with_client=with_client))
    if registered:
        assert adapter._cache_hit_executor is BaseAsyncDriver._run_cached_statement
        assert isinstance(adapter._cache_hit_state, tuple)
        assert len(adapter._cache_hit_state) == 3
    else:
        assert adapter._cache_hit_executor is None
        assert adapter._cache_hit_state is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_ASYNC_ADAPTERS,
)
@pytest.mark.parametrize(
    ("sql", "kind"),
    [
        pytest.param("SELECT 1 AS a", "fetch", id="select"),
        pytest.param("UPDATE t SET a = 2", "dml", id="update"),
    ],
)
async def test_async_cache_hit_awaits_one_driver_awaitable(
    adapter_cls: type[BaseAsyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
    sql: str,
    kind: str,
) -> None:
    """A cached async statement awaits exactly one single-call driver awaitable with no cursor opened."""
    native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    adapter = adapter_cls(native)
    assert adapter._stmt_cache_enabled

    await adapter.execute(sql)
    native.calls.clear()
    native.awaited = 0
    await adapter.execute(sql)

    expected_call = fetch_call if kind == "fetch" else dml_call
    cached = adapter._stmt_cache.get(sql)
    assert cached is not None
    assert len(native.calls) == 1
    assert native.calls[0][0] == expected_call
    assert native.calls[0][1] == cached.compiled_sql
    assert native.awaited == 1
    assert all(entry[0] != "cursor" for entry in native.calls)


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_ASYNC_ADAPTERS,
)
async def test_async_cache_miss_runs_through_cursor(
    adapter_cls: type[BaseAsyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
) -> None:
    """The first async execution of a statement misses the cache and runs through a native cursor."""
    _ = (fetch_call, dml_call)
    native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    adapter = adapter_cls(native)
    assert adapter._stmt_cache_enabled

    result = await adapter.execute("SELECT 1 AS a")

    call_names = [entry[0] for entry in native.calls]
    assert "cursor" in call_names
    assert "execute" in call_names
    assert result.all() == [{"a": 1}]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_ASYNC_ADAPTERS,
)
@pytest.mark.parametrize(
    ("case_name", "sql", "rows", "columns", "rowcount", "lastrowid"),
    [
        pytest.param("select", "SELECT 1 AS a", [{"a": 1}], ["a"], 1, 7, id="select"),
        pytest.param("empty_select", "SELECT 1 AS a", [], ["a"], 0, None, id="empty-select"),
        pytest.param("insert", "INSERT INTO t (a) VALUES (1)", [], [], 1, 7, id="insert"),
        pytest.param("update", "UPDATE t SET a = 2", [], [], 3, 7, id="update"),
        pytest.param("update_no_match", "UPDATE t SET a = 2", [], [], 0, 7, id="update-no-match"),
    ],
)
async def test_async_cache_hit_result_matches_method_path(
    adapter_cls: type[BaseAsyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
    case_name: str,
    sql: str,
    rows: list[dict[str, Any]],
    columns: list[str],
    rowcount: int,
    lastrowid: int | None,
) -> None:
    """Async cached execution results match the method path across SELECT and DML shapes."""
    _ = (fetch_call, dml_call)
    exec_native = _SingleCallAsyncNative(
        rows=rows,
        columns=columns,
        rowcount=rowcount,
        lastrowid=lastrowid,
        with_client=with_client,
    )
    method_native = _SingleCallAsyncNative(
        rows=rows,
        columns=columns,
        rowcount=rowcount,
        lastrowid=lastrowid,
        with_client=with_client,
    )
    exec_adapter = adapter_cls(exec_native)
    method_adapter = adapter_cls(method_native)
    assert exec_adapter._stmt_cache_enabled
    assert method_adapter._stmt_cache_enabled
    method_adapter._cache_hit_executor = None

    await exec_adapter.execute(sql)
    await method_adapter.execute(sql)
    exec_result = await exec_adapter.execute(sql)
    method_result = await method_adapter.execute(sql)

    assert exec_result.all() == method_result.all()
    assert exec_result.column_names == method_result.column_names
    assert exec_result.rows_affected == method_result.rows_affected

    if case_name == "select":
        assert exec_result.all() == [{"a": 1}]
    elif case_name == "empty_select":
        assert exec_result.all() == []
        assert exec_result.column_names == ["a"]
    elif case_name == "insert":
        assert exec_result.rows_affected == 1
        assert exec_result.last_inserted_id == 7
    elif case_name == "update":
        assert exec_result.rows_affected == 3
        assert exec_result.last_inserted_id is None
    elif case_name == "update_no_match":
        assert exec_result.rows_affected == 0
        assert exec_result.last_inserted_id is None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call"),
    [
        pytest.param(SQLiteAsyncDriver, True, "fetch_rows", id="sqlite"),
        pytest.param(DuckDBAsyncDriver, True, "fetch_rows", id="duckdb"),
        pytest.param(PostgresAsyncDriver, False, "execute_with_description", id="postgres"),
    ],
)
async def test_async_cache_hit_returning_clause_builds_row_result(
    adapter_cls: type[BaseAsyncDriver],
    with_client: bool,
    fetch_call: str,
) -> None:
    """Async INSERT ... RETURNING routes through the fetch callable and matches the method path."""
    sql = "INSERT INTO t (a) VALUES (1) RETURNING a"
    exec_native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    method_native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    exec_adapter = adapter_cls(exec_native)
    method_adapter = adapter_cls(method_native)
    assert exec_adapter._stmt_cache_enabled
    method_adapter._cache_hit_executor = None

    await exec_adapter.execute(sql)
    await method_adapter.execute(sql)
    exec_native.calls.clear()
    exec_native.awaited = 0

    exec_result = await exec_adapter.execute(sql)
    method_result = await method_adapter.execute(sql)

    assert len(exec_native.calls) == 1
    assert exec_native.calls[0][0] == fetch_call
    assert exec_native.awaited == 1
    assert exec_result.all() == [{"a": 1}]
    assert exec_result.all() == method_result.all()
    assert exec_result.column_names == method_result.column_names
    assert exec_result.rows_affected == method_result.rows_affected


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("adapter_cls", "with_client", "fetch_call", "dml_call"),
    _REGISTERED_CACHE_HIT_ASYNC_ADAPTERS,
)
@pytest.mark.parametrize(
    ("exc_factory", "mapped"),
    [
        pytest.param(lambda: NativeIntegrityError("UNIQUE constraint failed: t.id"), True, id="integrity"),
        pytest.param(lambda: ProgrammingError("syntax error"), True, id="programming"),
        pytest.param(lambda: NativeOperationalError("database is locked"), True, id="operational"),
        pytest.param(lambda: InterfaceError("closed"), True, id="interface"),
        pytest.param(lambda: QueryCancelledError("stopped"), True, id="cancelled"),
        pytest.param(lambda: ValueError("boom"), False, id="unmapped"),
    ],
)
async def test_async_cache_hit_errors_match_method_path(
    adapter_cls: type[BaseAsyncDriver],
    with_client: bool,
    fetch_call: str,
    dml_call: str,
    exc_factory: Any,
    mapped: bool,
) -> None:
    """Async errors raised on the cache-hit executor match the method path class and preserve __cause__."""
    _ = (fetch_call, dml_call)
    exec_native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    method_native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"], with_client=with_client)
    exec_adapter = adapter_cls(exec_native)
    method_adapter = adapter_cls(method_native)
    assert exec_adapter._stmt_cache_enabled
    method_adapter._cache_hit_executor = None

    await exec_adapter.execute("SELECT 1 AS a")
    await method_adapter.execute("SELECT 1 AS a")

    exec_error = exc_factory()
    method_error = exc_factory()
    exec_native.fail_with = exec_error
    method_native.fail_with = method_error

    with pytest.raises(Exception) as exec_info:
        await exec_adapter.execute("SELECT 1 AS a")
    with pytest.raises(Exception) as method_info:
        await method_adapter.execute("SELECT 1 AS a")

    assert type(exec_info.value) is type(method_info.value)
    if mapped:
        assert exec_info.value.__cause__ is exec_error
    else:
        assert exec_info.value is exec_error


@pytest.mark.anyio
@pytest.mark.parametrize(("exc_factory", "expected_cls"), _SYNC_CACHE_HIT_MAPPED_ERRORS)
async def test_async_cache_hit_error_classes_on_base_adapter(
    exc_factory: Any,
    expected_cls: type[Exception],
) -> None:
    """BaseAsyncDriver cache-hit executor maps native errors to expected SQLSpec exception classes."""
    native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"])
    adapter = _SampleAsyncAdapter(native)
    assert adapter._stmt_cache_enabled

    await adapter.execute("SELECT 1 AS a")
    native_error = exc_factory()
    native.fail_with = native_error

    with pytest.raises(expected_cls) as exc_info:
        await adapter.execute("SELECT 1 AS a")
    assert exc_info.value.__cause__ is native_error


@pytest.mark.anyio
async def test_async_cache_hit_with_active_observability_uses_observed_path() -> None:
    """Active statement observers route async cached statements through the observed cursor path."""
    events: list[Any] = []
    native = _SingleCallAsyncNative(rows=[{"a": 1}], columns=["a"])
    adapter = _SampleAsyncAdapter(
        native,
        observability=ObservabilityRuntime(ObservabilityConfig(statement_observers=(events.append,))),
    )
    assert adapter._stmt_cache_enabled

    await adapter.execute("SELECT 1 AS a")
    native.calls.clear()
    await adapter.execute("SELECT 1 AS a")

    call_names = [entry[0] for entry in native.calls]
    assert "cursor" in call_names
    assert "execute_with_description" not in call_names
    assert len(events) == 2


@pytest.mark.anyio
@pytest.mark.parametrize("engine", ["sqlite", "duckdb"])
async def test_async_cache_hit_on_real_embedded_engines(engine: str) -> None:
    """Real SQLite and DuckDB async drivers execute cached DML, SELECT, duplicate-key, and closed-client paths."""
    native: Any
    exec_adapter: BaseAsyncDriver
    method_adapter: BaseAsyncDriver
    if engine == "sqlite":
        native = AsyncSQLiteDriver(SQLiteConfig(path=":memory:"))
        exec_adapter = SQLiteAsyncDriver(native)
        method_adapter = SQLiteAsyncDriver(native)
    else:
        native = AsyncDuckDBDriver(DuckDBConfig(path=":memory:"))
        exec_adapter = DuckDBAsyncDriver(native)
        method_adapter = DuckDBAsyncDriver(native)
    method_adapter._cache_hit_executor = None

    try:
        assert exec_adapter._stmt_cache_enabled
        await exec_adapter.execute("CREATE TABLE cache_hit_items (id INTEGER PRIMARY KEY, name TEXT)")

        first_insert = await exec_adapter.execute("INSERT INTO cache_hit_items (id, name) VALUES (?, ?)", (1, "alpha"))
        assert first_insert.rows_affected == 1

        second_insert = await exec_adapter.execute("INSERT INTO cache_hit_items (id, name) VALUES (?, ?)", (2, "beta"))
        assert second_insert.rows_affected == 1
        if engine == "sqlite":
            assert second_insert.last_inserted_id == 2

        first_select = await exec_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
        second_select = await exec_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
        expected_rows = [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]
        assert first_select.all() == expected_rows
        assert second_select.all() == expected_rows

        with pytest.raises(UniqueViolationError) as dup_info:
            await exec_adapter.execute("INSERT INTO cache_hit_items (id, name) VALUES (?, ?)", (1, "dup"))
        assert type(dup_info.value.__cause__) is NativeIntegrityError

        await method_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
    finally:
        await native.close()

    with pytest.raises(Exception) as exec_closed:
        await exec_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")
    with pytest.raises(Exception) as method_closed:
        await method_adapter.execute("SELECT id, name FROM cache_hit_items ORDER BY id")

    assert type(exec_closed.value.__cause__) is InterfaceError
    assert type(exec_closed.value) is type(method_closed.value)


@pytest.mark.anyio
async def test_async_base_execute_dml_awaits_one_cursor_statement() -> None:
    """AsyncDatabaseDriver.execute_dml executes once on a cursor, normalizes empty params, and closes the cursor."""
    native = _RecordingAsyncNativeDriver()
    result = await native.execute_dml("UPDATE t SET a = 2")
    assert result == (1, None)
    assert native.calls == [("execute", "UPDATE t SET a = 2", ())]

    class _CapturingCursorNative(_RecordingAsyncNativeDriver):
        def __init__(self) -> None:
            super().__init__()
            self.last_cursor: _RecordingAsyncCursor | None = None

        def cursor(self, *, lazy_begin: bool = False) -> _RecordingAsyncCursor:
            self.last_cursor = super().cursor(lazy_begin=lazy_begin)
            return self.last_cursor

    capturing = _CapturingCursorNative()
    assert await capturing.execute_dml("UPDATE t SET a = 2") == (1, None)
    assert capturing.last_cursor is not None
    assert capturing.last_cursor.closed is True


@pytest.mark.anyio
async def test_thread_offload_execute_dml_delegates_to_sync_driver() -> None:
    """ThreadOffloadAsyncDatabaseDriver.execute_dml runs the paired sync driver's execute_dml on a worker thread."""
    sync_native = _RecordingSyncNativeDriver()
    driver = ThreadOffloadAsyncDatabaseDriver(SQLiteConfig(path=":memory:"), sync_native)
    result = await driver.execute_dml("UPDATE t SET a = 2")
    assert result == (1, None)
    assert sync_native.calls == [("execute", "UPDATE t SET a = 2", ())]
