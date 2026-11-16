"""Unit tests for SQLSpec BaseSyncDriver/BaseAsyncDriver template-method dispatch and error mapping."""

from collections.abc import Mapping, Sequence
from typing import Any, ClassVar

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
    AsyncAdapterConfig,
    BaseAsyncDriver,
    BaseSyncDriver,
    DeferredAsyncExceptionHandler,
    DeferredSyncExceptionHandler,
    SyncAdapterConfig,
    acquire_shared_native_driver,
    merge_statement_config,
    native_statement_config,
    release_shared_native_driver_async,
    retain_shared_native_driver,
    shared_native_refcount,
    unwrap_typed_parameters,
)
from vertebra.extensions.sqlspec.adapters.oracle import OracleAsyncConfig, OracleSyncConfig
from vertebra.extensions.sqlspec.adapters.postgres import PostgresAsyncConfig, PostgresSyncConfig


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


class _CustomBuildSyncConfig(SyncAdapterConfig[dict[str, Any], _SampleSyncAdapter]):
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


class _CustomBuildAsyncConfig(AsyncAdapterConfig[dict[str, Any], _SampleAsyncAdapter]):
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
    assert isinstance(sync_pool, _SampleSyncAdapter)
    async_pool = await async_cfg.provide_pool()
    assert isinstance(async_pool, _SampleAsyncAdapter)

    with pytest.raises(RuntimeError, match="sync boom"), sync_cfg.provide_session() as sync_session:
        sync_session.begin()
        assert sync_session.connection.in_transaction is True
        raise RuntimeError("sync boom")
    assert sync_pool.connection.in_transaction is False
    assert sync_pool.connection.rollbacks == 1

    with pytest.raises(RuntimeError, match="async boom"):
        async with async_cfg.provide_session() as async_session:
            await async_session.begin()
            assert async_session.connection.in_transaction is True
            raise RuntimeError("async boom")
    assert async_pool.connection.in_transaction is False
    assert async_pool.connection.rollbacks == 1

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
