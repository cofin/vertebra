"""SQLite SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from os import PathLike
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    apply_pragmas_async,
    apply_pragmas_sync,
    render_pragmas,
)

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

    from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SyncSQLiteDriver
    from vertebra.extensions.sqlspec.adapters.sqlite.config import SQLiteAsyncConfig, SQLiteSyncConfig
    from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver, SQLiteSyncDriver

__all__ = (
    "SQLiteAsyncConnectionContext",
    "SQLiteAsyncSessionContext",
    "SQLiteConnectionParams",
    "SQLiteDriverFeatures",
    "SQLitePoolParams",
    "SQLiteSyncConnectionContext",
    "SQLiteSyncSessionContext",
)

_TEMP_OBJECT_DROP_ORDER: tuple[str, ...] = ("view", "trigger", "index", "table")


class SQLiteConnectionParams(TypedDict, total=False):
    """Typed connection parameters accepted by `SQLiteSyncConfig` and `SQLiteAsyncConfig`."""

    database: NotRequired[str | PathLike[str]]
    """Target database name, catalog, or file path."""
    uri: NotRequired[bool]
    """Interpret the SQLite path as an RFC 3986 URI."""
    timeout: NotRequired[float]
    """Connection or lock acquisition timeout in seconds."""
    detect_types: NotRequired[int]
    """SQLite declared/column type parsing flags."""
    isolation_level: NotRequired[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"] | None]
    """SQLite transaction isolation level."""
    check_same_thread: NotRequired[bool]
    """Enforce single-thread connection affinity checks."""
    cached_statements: NotRequired[int]
    """Maximum prepared statements retained per SQLite connection."""
    path: NotRequired[str | PathLike[str]]
    """Filesystem path to the local database file."""
    url: NotRequired[str]
    """SQLite connection URL string."""
    dsn: NotRequired[str]
    """SQLite connection DSN string."""
    query_only: NotRequired[bool]
    """Issue ``PRAGMA query_only = ON`` on connection checkout."""
    enable_optimizations: NotRequired[bool]
    """Apply performance tuning PRAGMAs on SQLite startup."""
    enable_foreign_keys: NotRequired[bool]
    """Issue ``PRAGMA foreign_keys = ON`` on SQLite connections."""
    wal_mode: NotRequired[bool]
    """Configure SQLite WAL journal mode on connection checkout."""
    synchronous: NotRequired[str]
    """SQLite ``PRAGMA synchronous`` durability mode."""
    mmap_size: NotRequired[int]
    """SQLite memory-mapped I/O window size in bytes."""
    busy_timeout_ms: NotRequired[int]
    """SQLite lock busy timeout in milliseconds."""
    max_readers: NotRequired[int]
    """Maximum concurrent SQLite reader leases permitted."""
    foreign_keys: NotRequired[bool]
    """Default SQLite foreign key enforcement toggle."""
    enable_jsonb: NotRequired[bool]
    """Enable SQLite JSONB binary serialization helpers."""
    extra: NotRequired[dict[str, Any]]
    """Additional backend-specific keyword parameters."""


class SQLitePoolParams(SQLiteConnectionParams, total=False):
    """Typed pool and registry-broker parameters extending `SQLiteConnectionParams`."""

    min_size: NotRequired[int]
    """Minimum warm connections retained in the pool."""
    max_size: NotRequired[int]
    """Maximum concurrent connections allowed in the pool."""
    pool_recycle: NotRequired[int]
    """Maximum connection lifetime in seconds before recycling."""
    share_pool: NotRequired[bool]
    """Share connection leases through the Vertebra SHM registry broker."""


class SQLiteDriverFeatures(TypedDict, total=False):
    """Typed feature flags, callbacks, and PRAGMA settings for SQLite SQLSpec adapters."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Custom JSON serializer callable for parameter encoding."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Custom JSON deserializer callable for result decoding."""
    enable_custom_adapters: NotRequired[bool]
    """Register custom SQLite value adapters."""
    custom_functions: NotRequired[Sequence[Any]]
    """User-defined scalar SQL functions to register in SQLite."""
    collations: NotRequired[Sequence[Any]]
    """Custom collation callables to register in SQLite."""
    aggregates: NotRequired[Sequence[Any]]
    """User-defined aggregate classes to register in SQLite."""
    window_functions: NotRequired[Sequence[Any]]
    """User-defined window function classes to register in SQLite."""
    pragmas: NotRequired[Mapping[str, str | int | bool]]
    """Explicit SQLite PRAGMA key-value overrides."""
    on_connection_create: NotRequired[Callable[[Any], Any]]
    """Hook invoked with each newly established connection."""
    reset_session_on_release: NotRequired[bool]
    """Reset session state automatically when returning a connection."""
    default_transaction_mode: NotRequired[Literal["DEFERRED", "IMMEDIATE", "EXCLUSIVE"]]
    """Default SQLite ``BEGIN`` transaction mode."""


def _build_connection_pragmas(config: Any) -> list[str]:
    """Build ordered SQLite connection PRAGMA statements from config state and `driver_features`."""
    statements: list[str] = []
    if getattr(config, "_is_read_only", False):
        statements.append("PRAGMA query_only = ON")
    pragmas = config.driver_features.get("pragmas")
    if isinstance(pragmas, Mapping) and pragmas:
        for name, val in render_pragmas(pragmas):
            statements.append(f"PRAGMA {name} = {val}")
    return statements


def _build_baseline_reset_pragmas(config: Any) -> list[str]:
    """Build baseline session PRAGMA statements restored during `reset_session_on_release`."""
    driver_cfg = getattr(config, "driver_config", None)
    foreign_keys = bool(getattr(driver_cfg, "foreign_keys", True))
    busy_timeout_ms = int(getattr(driver_cfg, "busy_timeout_ms", 5000))
    query_only = bool(getattr(config, "_is_read_only", False))
    statements = [
        f"PRAGMA foreign_keys = {'ON' if foreign_keys else 'OFF'}",
        f"PRAGMA busy_timeout = {busy_timeout_ms}",
        f"PRAGMA query_only = {'ON' if query_only else 'OFF'}",
    ]
    pragmas = config.driver_features.get("pragmas")
    if isinstance(pragmas, Mapping) and pragmas:
        for name, val in render_pragmas(pragmas):
            statements.append(f"PRAGMA {name} = {val}")
    return statements


def _reset_sqlite_connection_sync(conn: Any, config: Any) -> None:
    """Roll back open transactions, drop temp objects, detach extra DBs, optimize, and restore PRAGMAs."""
    if getattr(conn, "in_transaction", False):
        rollback_fn = getattr(conn, "rollback", None)
        if callable(rollback_fn):
            with contextlib.suppress(Exception):
                rollback_fn()
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with contextlib.suppress(Exception):
            reset_fn()
    if not callable(getattr(conn, "execute", None)):
        return
    if callable(getattr(conn, "select", None)):
        with contextlib.suppress(Exception):
            for row in conn.select("PRAGMA database_list"):
                db_name = row.get("name") if isinstance(row, Mapping) else None
                if isinstance(db_name, str) and db_name not in {"main", "temp"}:
                    escaped = db_name.replace('"', '""')
                    conn.execute(f'DETACH DATABASE "{escaped}"')
        with contextlib.suppress(Exception):
            temp_rows = conn.select("SELECT type, name FROM temp.sqlite_master WHERE name NOT LIKE 'sqlite_%'")
            for obj_type in _TEMP_OBJECT_DROP_ORDER:
                for row in temp_rows:
                    if not isinstance(row, Mapping):
                        continue
                    if str(row.get("type", "")).lower() != obj_type:
                        continue
                    obj_name = row.get("name")
                    if isinstance(obj_name, str) and obj_name:
                        escaped = obj_name.replace('"', '""')
                        conn.execute(f'DROP {obj_type.upper()} IF EXISTS temp."{escaped}"')
    with contextlib.suppress(Exception):
        conn.execute("PRAGMA optimize")
    for stmt in _build_baseline_reset_pragmas(config):
        with contextlib.suppress(Exception):
            conn.execute(stmt)


async def _reset_sqlite_connection_async(conn: Any, config: Any) -> None:
    """Asynchronously roll back, drop temp objects, detach extra DBs, optimize, and restore PRAGMAs."""
    if getattr(conn, "in_transaction", False):
        rollback_fn = getattr(conn, "rollback", None)
        if callable(rollback_fn):
            with CancelScope(shield=True), contextlib.suppress(Exception):
                res = rollback_fn()
                if inspect.isawaitable(res):
                    await res
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = reset_fn()
            if inspect.isawaitable(res):
                await res
    if not callable(getattr(conn, "execute", None)):
        return
    if callable(getattr(conn, "select", None)):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            db_rows = conn.select("PRAGMA database_list")
            if inspect.isawaitable(db_rows):
                db_rows = await db_rows
            for row in db_rows:
                db_name = row.get("name") if isinstance(row, Mapping) else None
                if isinstance(db_name, str) and db_name not in {"main", "temp"}:
                    escaped = db_name.replace('"', '""')
                    exec_res = conn.execute(f'DETACH DATABASE "{escaped}"')
                    if inspect.isawaitable(exec_res):
                        await exec_res
        with CancelScope(shield=True), contextlib.suppress(Exception):
            temp_rows = conn.select("SELECT type, name FROM temp.sqlite_master WHERE name NOT LIKE 'sqlite_%'")
            if inspect.isawaitable(temp_rows):
                temp_rows = await temp_rows
            for obj_type in _TEMP_OBJECT_DROP_ORDER:
                for row in temp_rows:
                    if not isinstance(row, Mapping):
                        continue
                    if str(row.get("type", "")).lower() != obj_type:
                        continue
                    obj_name = row.get("name")
                    if isinstance(obj_name, str) and obj_name:
                        escaped = obj_name.replace('"', '""')
                        exec_res = conn.execute(f'DROP {obj_type.upper()} IF EXISTS temp."{escaped}"')
                        if inspect.isawaitable(exec_res):
                            await exec_res
    with CancelScope(shield=True), contextlib.suppress(Exception):
        opt_res = conn.execute("PRAGMA optimize")
        if inspect.isawaitable(opt_res):
            await opt_res
    for stmt in _build_baseline_reset_pragmas(config):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            stmt_res = conn.execute(stmt)
            if inspect.isawaitable(stmt_res):
                await stmt_res


class SQLiteSyncConnectionContext:
    """Synchronous connection context manager acquiring native SQLite drivers/leases and applying PRAGMAs."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: SQLiteSyncConfig, *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: SyncSQLiteDriver | None = None
        self._tenant_lease: Any = None

    def __enter__(self) -> SyncSQLiteDriver:
        """Acquire a native SQLite driver or tenant lease, apply PRAGMAs, and invoke `on_connection_create`."""
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
            initialized_ids: set[int] | None = getattr(self._config, "_initialized_conn_ids", None)
            conn_id = id(self._connection)
            if initialized_ids is None or conn_id not in initialized_ids:
                statements = _build_connection_pragmas(self._config)
                if statements and callable(getattr(self._connection, "execute", None)):
                    apply_pragmas_sync(self._connection, statements)
                hook = self._config.driver_features.get("on_connection_create")
                if callable(hook):
                    hook(self._connection)
                if initialized_ids is not None:
                    initialized_ids.add(conn_id)
            return cast("SyncSQLiteDriver", self._connection)
        except BaseException as exc:
            self.__exit__(type(exc), exc, exc.__traceback__)
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back on error, optionally reset session PRAGMAs/temp tables, and release the lease."""
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
                    _reset_sqlite_connection_sync(conn, self._config)
            if self._tenant_lease is not None:
                self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", self._conn_ctx.__exit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class SQLiteAsyncConnectionContext:
    """Asynchronous connection context manager acquiring native SQLite drivers/leases and applying PRAGMAs."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: SQLiteAsyncConfig, *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: AsyncSQLiteDriver | None = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> AsyncSQLiteDriver:
        """Asynchronously acquire a native SQLite driver or lease, apply PRAGMAs, and invoke `on_connection_create`."""
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
            initialized_ids: set[int] | None = getattr(self._config, "_initialized_conn_ids", None)
            conn_id = id(self._connection)
            if initialized_ids is None or conn_id not in initialized_ids:
                statements = _build_connection_pragmas(self._config)
                if statements and callable(getattr(self._connection, "execute", None)):
                    await apply_pragmas_async(self._connection, statements)
                hook = self._config.driver_features.get("on_connection_create")
                if callable(hook):
                    hook_res = hook(self._connection)
                    if inspect.isawaitable(hook_res):
                        await hook_res
                if initialized_ids is not None:
                    initialized_ids.add(conn_id)
            return cast("AsyncSQLiteDriver", self._connection)
        except BaseException as exc:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back on error, optionally reset session PRAGMAs/temp tables, and release the lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                if getattr(conn, "in_transaction", False):
                    rollback_fn = getattr(conn, "rollback", None)
                    if callable(rollback_fn):
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            res = rollback_fn()
                            if inspect.isawaitable(res):
                                await res
                if reset_on_release:
                    await _reset_sqlite_connection_async(conn, self._config)
            if self._tenant_lease is not None:
                await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class SQLiteSyncSessionContext:
    """Synchronous session context manager building a fresh `SQLiteSyncDriver` per session entry."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_driver", "_kwargs", "_statement_config")

    def __init__(
        self,
        config: SQLiteSyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: SQLiteSyncConnectionContext | None = None
        self._connection: SyncSQLiteDriver | None = None
        self._driver: SQLiteSyncDriver | None = None

    def __enter__(self) -> SQLiteSyncDriver:
        """Acquire a leased connection and instantiate a fresh `SQLiteSyncDriver`."""
        self._conn_ctx = SQLiteSyncConnectionContext(self._config, *self._args, **self._kwargs)
        self._connection = self._conn_ctx.__enter__()
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
        """Close open streams, roll back uncommitted work, release lease/reset, and mark driver closed."""
        driver = self._driver
        conn_ctx = self._conn_ctx
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
                    self._config.share_pool
                    or (conn_ctx is not None and (conn_ctx._tenant_lease is not None or conn_ctx._conn_ctx is not None))
                )
                if driver is not None and (exc_type is not None or pooled_state or reset_on_release):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        with contextlib.suppress(Exception):
                            driver.rollback()
                if conn_ctx is not None:
                    conn_ctx.__exit__(exc_type, exc_val, exc_tb)
            finally:
                self._connection = None
                self._conn_ctx = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None


class SQLiteAsyncSessionContext:
    """Asynchronous session context manager building a fresh `SQLiteAsyncDriver` per session entry."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_driver", "_kwargs", "_statement_config")

    def __init__(
        self,
        config: SQLiteAsyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: SQLiteAsyncConnectionContext | None = None
        self._connection: AsyncSQLiteDriver | None = None
        self._driver: SQLiteAsyncDriver | None = None

    async def __aenter__(self) -> SQLiteAsyncDriver:
        """Asynchronously acquire a leased connection and instantiate a fresh `SQLiteAsyncDriver`."""
        self._conn_ctx = SQLiteAsyncConnectionContext(self._config, *self._args, **self._kwargs)
        self._connection = await self._conn_ctx.__aenter__()
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
        """Close open streams, roll back uncommitted work, release lease/reset, and mark driver closed."""
        driver = self._driver
        conn_ctx = self._conn_ctx
        try:
            if driver is not None:
                close_streams_fn = getattr(driver, "_close_open_streams", None)
                if callable(close_streams_fn):
                    res = close_streams_fn(error=exc_type is not None)
                    if inspect.isawaitable(res):
                        await res
        except BaseException as close_exc:
            if exc_type is None:
                exc_type, exc_val, exc_tb = type(close_exc), close_exc, close_exc.__traceback__
                raise
        finally:
            try:
                reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
                pooled_state = bool(
                    self._config.share_pool
                    or (conn_ctx is not None and (conn_ctx._tenant_lease is not None or conn_ctx._conn_ctx is not None))
                )
                if driver is not None and (exc_type is not None or pooled_state or reset_on_release):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            await driver.rollback()
                if conn_ctx is not None:
                    await conn_ctx.__aexit__(exc_type, exc_val, exc_tb)
            finally:
                self._connection = None
                self._conn_ctx = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None
