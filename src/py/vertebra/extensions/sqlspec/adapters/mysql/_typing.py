"""MySQL/MariaDB SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec._driver import (
    _AdapterAsyncConnectionContext,
    _AdapterAsyncSessionContext,
    _AdapterSyncConnectionContext,
    _AdapterSyncSessionContext,
)

if TYPE_CHECKING:
    from vertebra.db.adapters.mysql import AsyncMySQLDriver, SyncMySQLDriver
    from vertebra.db.driver._cloud_auth import DatabaseTokenProvider
    from vertebra.extensions.sqlspec.adapters.mysql.config import MySQLAsyncConfig, MySQLSyncConfig
    from vertebra.extensions.sqlspec.adapters.mysql.driver import MySQLAsyncDriver, MySQLSyncDriver

__all__ = (
    "MySQLAsyncConnectionContext",
    "MySQLAsyncSessionContext",
    "MySQLConnectionParams",
    "MySQLDriverFeatures",
    "MySQLPoolParams",
    "MySQLSyncConnectionContext",
    "MySQLSyncSessionContext",
)

_MISSING = object()


class MySQLConnectionParams(TypedDict, total=False):
    """Typed connection parameters accepted by `MySQLSyncConfig` and `MySQLAsyncConfig`."""

    dsn: NotRequired[str]
    """MySQL connection DSN string."""
    url: NotRequired[str]
    """MySQL connection URL string."""
    connection_string: NotRequired[str]
    """Alternative MySQL connection string alias."""
    host: NotRequired[str]
    """Database server hostname or address sequence."""
    port: NotRequired[int]
    """Database server TCP port or port sequence."""
    user: NotRequired[str]
    """Authenticating database username."""
    password: NotRequired[str]
    """Password credential for database authentication."""
    database: NotRequired[str]
    """Target database name, catalog, or file path."""
    unix_socket: NotRequired[str | None]
    """Local Unix domain socket path for transport."""
    charset: NotRequired[str]
    """Wire character set encoding name."""
    ssl: NotRequired[Any]
    """TLS/SSL configuration mapping or flag."""
    ssl_mode: NotRequired[str]
    """TLS verification mode."""
    ssl_ca: NotRequired[str | None]
    """Path or PEM bundle for TLS certificate authority validation."""
    ssl_cert: NotRequired[str | None]
    """Client TLS certificate path or PEM payload."""
    ssl_key: NotRequired[str | None]
    """Client TLS private key path or PEM payload."""
    connect_timeout: NotRequired[float]
    """TCP connect timeout in seconds."""
    read_timeout: NotRequired[float]
    """Socket read timeout in seconds."""
    write_timeout: NotRequired[float]
    """Socket write timeout in seconds."""
    autocommit: NotRequired[bool]
    """Enable autocommit mode on new connections."""
    init_command: NotRequired[str | None]
    """Initial SQL statement executed on connection startup."""
    cloud_sql_instance: NotRequired[str | None]
    """Cloud SQL instance connection name (``project:region:instance``)."""
    cloud_sql_ip_type: NotRequired[str]
    """Preferred IP type for Cloud SQL Dialer connections."""
    enable_iam_auth: NotRequired[bool]
    """Authenticate database connections with cloud IAM tokens."""
    token_provider: NotRequired[DatabaseTokenProvider | None]
    """Cloud database token provider supplying dynamic IAM passwords."""
    extra: NotRequired[dict[str, Any]]
    """Additional backend-specific keyword parameters."""


class MySQLPoolParams(MySQLConnectionParams, total=False):
    """Typed pool and registry-broker parameters extending `MySQLConnectionParams`."""

    min_size: NotRequired[int]
    """Minimum warm connections retained in the pool."""
    max_size: NotRequired[int]
    """Maximum concurrent connections allowed in the pool."""
    pool_recycle: NotRequired[int]
    """Maximum connection lifetime in seconds before recycling."""
    share_pool: NotRequired[bool]
    """Share connection leases through the Vertebra SHM registry broker."""


class MySQLDriverFeatures(TypedDict, total=False):
    """Typed feature flags and lifecycle callbacks for MySQL/MariaDB SQLSpec adapters."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Custom JSON serializer callable for parameter encoding."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Custom JSON deserializer callable for result decoding."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channel storage and dispatch."""
    events_backend: NotRequired[Literal["poll_queue"]]
    """Event channel backend implementation identifier."""
    on_connection_create: NotRequired[Callable[[Any], Any]]
    """Hook invoked with each newly established connection."""
    reset_session_on_release: NotRequired[bool]
    """Reset session state automatically when returning a connection."""
    use_pure_python_driver: NotRequired[bool]
    """Fallback toggle disabling pure-Rust acceleration."""
    cloud_sql_instance: NotRequired[str]
    """Cloud SQL instance connection name (``project:region:instance``)."""
    enable_iam_auth: NotRequired[bool]
    """Authenticate database connections with cloud IAM tokens."""
    connector_retry_count: NotRequired[int]
    """Maximum retry attempts on transient Cloud SQL dialer errors (defaults to ``3``; ``0`` disables retries)."""
    connector_retry_delay: NotRequired[float]
    """Initial backoff delay in seconds before jittered exponential dialer retries (defaults to ``0.5``)."""
    connector_retry_max_delay: NotRequired[float]
    """Upper bound in seconds on exponential backoff between dialer retry attempts (defaults to ``5.0``)."""
    tenant_strategy: NotRequired[Any]
    """Multi-tenant schema or row isolation strategy."""


def _is_unopened_native_driver(conn: Any) -> bool:
    """Return whether `conn` is a lazy native MySQL driver with no open client socket."""
    return getattr(conn, "_client", _MISSING) is None


def _rollback_mysql_connection_sync(conn: Any) -> None:
    """Roll back any active synchronous transaction on `conn`."""
    in_tx = getattr(conn, "in_transaction", False)
    if callable(in_tx):
        in_tx = in_tx()
    if not in_tx:
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        rollback_fn()
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn) and not _is_unopened_native_driver(conn):
        execute_fn("ROLLBACK")


def _reset_mysql_connection_sync(conn: Any) -> None:
    """Reset MySQL session state synchronously via driver reset hook or `RESET CONNECTION`."""
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        reset_fn()
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn) and not _is_unopened_native_driver(conn):
        execute_fn("RESET CONNECTION")


async def _rollback_mysql_connection_async(conn: Any) -> None:
    """Roll back any active asynchronous transaction on `conn`."""
    in_tx = getattr(conn, "in_transaction", False)
    if callable(in_tx):
        in_tx = in_tx()
    if not in_tx:
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with CancelScope(shield=True):
            res = rollback_fn()
            if inspect.isawaitable(res):
                await res
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn) and not _is_unopened_native_driver(conn):
        with CancelScope(shield=True):
            res = execute_fn("ROLLBACK")
            if inspect.isawaitable(res):
                await res


async def _reset_mysql_connection_async(conn: Any) -> None:
    """Reset MySQL session state asynchronously via driver reset hook or `RESET CONNECTION`."""
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with CancelScope(shield=True):
            res = reset_fn()
            if inspect.isawaitable(res):
                await res
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn) and not _is_unopened_native_driver(conn):
        with CancelScope(shield=True):
            res = execute_fn("RESET CONNECTION")
            if inspect.isawaitable(res):
                await res


class MySQLSyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring native MySQL drivers/leases and running hooks."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "MySQL"
    _strict_cleanup: ClassVar[bool] = True

    def __init__(self, config: MySQLSyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _rollback_conn(self, conn: Any) -> None:
        _rollback_mysql_connection_sync(conn)

    def _reset_conn(self, conn: Any) -> None:
        _reset_mysql_connection_sync(conn)

    def __enter__(self) -> SyncMySQLDriver:
        return cast("SyncMySQLDriver", super().__enter__())


class MySQLAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring native MySQL drivers/leases and running hooks."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "MySQL"
    _strict_cleanup: ClassVar[bool] = True

    def __init__(self, config: MySQLAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    async def _rollback_conn(self, conn: Any) -> None:
        await _rollback_mysql_connection_async(conn)

    async def _reset_conn(self, conn: Any) -> None:
        await _reset_mysql_connection_async(conn)

    async def __aenter__(self) -> AsyncMySQLDriver:
        return cast("AsyncMySQLDriver", await super().__aenter__())


class MySQLSyncSessionContext(_AdapterSyncSessionContext):
    """Synchronous session context manager building a fresh `MySQLSyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterSyncConnectionContext]] = MySQLSyncConnectionContext
    _engine_label: ClassVar[str] = "MySQL"
    _strict_cleanup: ClassVar[bool] = True

    def __enter__(self) -> MySQLSyncDriver:
        return cast("MySQLSyncDriver", super().__enter__())


class MySQLAsyncSessionContext(_AdapterAsyncSessionContext):
    """Asynchronous session context manager building a fresh `MySQLAsyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterAsyncConnectionContext]] = MySQLAsyncConnectionContext
    _engine_label: ClassVar[str] = "MySQL"
    _strict_cleanup: ClassVar[bool] = True

    async def __aenter__(self) -> MySQLAsyncDriver:
        return cast("MySQLAsyncDriver", await super().__aenter__())
