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
from sqlspec.core import ParameterStyle, ParameterStyleConfig, StatementConfig, TypedParameter
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

from vertebra._vertebra import (
    BigQueryError,
    BigQueryRowError,
    ConnectionFailure,
    DatabaseError,
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
    LazyStoreAttr,
    SyncAdapterConfig,
)
from vertebra.extensions.sqlspec.adapters.oracle import OracleAsyncConfig, OracleSyncConfig
from vertebra.extensions.sqlspec.adapters.postgres import PostgresAsyncConfig, PostgresSyncConfig
from vertebra.extensions.sqlspec.events import ChangeFeedAsyncEventBackend, ChangeFeedSyncEventBackend


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
        return cls._build_statement_config("sqlite", ParameterStyle.QMARK)


class _SampleAsyncAdapter(BaseAsyncDriver):
    """Concrete BaseAsyncDriver subclass for sqlite qmark execution."""

    dialect = "sqlite"

    @classmethod
    def _default_statement_config(cls) -> StatementConfig:
        return BaseSyncDriver._build_statement_config("sqlite", ParameterStyle.QMARK)


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
    """TypedParameter wrappers unwrap cleanly and _merge_statement_config never mutates its base."""
    assert BaseSyncDriver._unwrap_typed_parameters([TypedParameter(42, int), {"k": TypedParameter("v", str)}]) == [
        42,
        {"k": "v"},
    ]
    assert BaseSyncDriver._unwrap_typed_parameters({"a": TypedParameter(None, Any)}) == {"a": None}

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


def test_sqlite_driver_imports_unwrap_typed_parameters_from_sqlite_core() -> None:
    """SQLite driver module imports unwrap_typed_parameters from sqlite.core rather than _driver."""
    driver_path = _SQLSPEC_ADAPTERS_ROOT / "sqlite" / "driver.py"
    tree = ast.parse(driver_path.read_text(encoding="utf-8"), filename=str(driver_path))

    imported_sources: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                imported_sources[alias.name] = node.module

    assert imported_sources.get("unwrap_typed_parameters") == "vertebra.extensions.sqlspec.adapters.sqlite.core"


@pytest.mark.parametrize("adapter_name", ["postgres", "oracle", "sqlite", "mysql", "mssql", "duckdb"])
def test_isolated_adapter_unwrap_typed_parameters(adapter_name: str) -> None:
    """Each isolated adapter core unwraps TypedParameter recursively without aliasing input containers."""
    core_mod = importlib.import_module(f"vertebra.extensions.sqlspec.adapters.{adapter_name}.core")
    local_unwrap = core_mod.unwrap_typed_parameters
    assert local_unwrap is not BaseSyncDriver._unwrap_typed_parameters

    inner_list = [TypedParameter(None, Any), True, 3.5]
    payload = {
        "scalar": TypedParameter(7, int),
        "nested": TypedParameter(TypedParameter("ok", str), str),
        "tuple": (TypedParameter(1, int), "plain"),
        "list": inner_list,
    }
    unwrapped = local_unwrap(payload)
    assert unwrapped == {
        "scalar": 7,
        "nested": "ok",
        "tuple": (1, "plain"),
        "list": [None, True, 3.5],
    }
    assert unwrapped is not payload
    assert unwrapped["list"] is not inner_list
    assert isinstance(unwrapped["tuple"], tuple)
    assert isinstance(unwrapped["list"], list)
    assert local_unwrap("raw") == "raw"
    assert local_unwrap(None) is None


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
    assert replica_sync.connection_instance.calls[-1][0] == "execute_with_description"
    assert sync_cfg.connection_instance.calls[-1][0] == "execute"
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
    assert replica_async.connection_instance.calls[-1][0] == "execute_with_description"
    assert async_cfg.connection_instance.calls[-1][0] == "execute"
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
