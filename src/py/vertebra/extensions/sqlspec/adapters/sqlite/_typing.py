"""SQLite SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from os import PathLike
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec._driver import (
    _AdapterAsyncConnectionContext,
    _AdapterAsyncSessionContext,
    _AdapterSyncConnectionContext,
    _AdapterSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.sqlite.core import (
    apply_pragmas_async,
    apply_pragmas_sync,
    render_pragmas,
)

if TYPE_CHECKING:
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


class SQLiteSyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring native SQLite drivers/leases and applying PRAGMAs."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "SQLite"

    def __init__(self, config: SQLiteSyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _on_first_acquire(self, conn: Any) -> None:
        statements = _build_connection_pragmas(self._config)
        if statements and callable(getattr(conn, "execute", None)):
            apply_pragmas_sync(conn, statements)

    def _reset_conn(self, conn: Any) -> None:
        _reset_sqlite_connection_sync(conn, self._config)

    def __enter__(self) -> SyncSQLiteDriver:
        return cast("SyncSQLiteDriver", super().__enter__())


class SQLiteAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring native SQLite drivers/leases and applying PRAGMAs."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "SQLite"

    def __init__(self, config: SQLiteAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    async def _on_first_acquire(self, conn: Any) -> None:
        statements = _build_connection_pragmas(self._config)
        if statements and callable(getattr(conn, "execute", None)):
            await apply_pragmas_async(conn, statements)

    async def _reset_conn(self, conn: Any) -> None:
        await _reset_sqlite_connection_async(conn, self._config)

    async def __aenter__(self) -> AsyncSQLiteDriver:
        return cast("AsyncSQLiteDriver", await super().__aenter__())


class SQLiteSyncSessionContext(_AdapterSyncSessionContext):
    """Synchronous session context manager building a fresh `SQLiteSyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterSyncConnectionContext]] = SQLiteSyncConnectionContext
    _engine_label: ClassVar[str] = "SQLite"

    def __enter__(self) -> SQLiteSyncDriver:
        return cast("SQLiteSyncDriver", super().__enter__())


class SQLiteAsyncSessionContext(_AdapterAsyncSessionContext):
    """Asynchronous session context manager building a fresh `SQLiteAsyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterAsyncConnectionContext]] = SQLiteAsyncConnectionContext
    _engine_label: ClassVar[str] = "SQLite"

    async def __aenter__(self) -> SQLiteAsyncDriver:
        return cast("SQLiteAsyncDriver", await super().__aenter__())
