"""SQL Server (MSSQL) SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

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
    from vertebra.db.adapters.mssql import AsyncMSSQLDriver, SyncMSSQLDriver
    from vertebra.extensions.sqlspec.adapters.mssql.config import MSSQLAsyncConfig, MSSQLSyncConfig
    from vertebra.extensions.sqlspec.adapters.mssql.driver import MSSQLAsyncDriver, MSSQLSyncDriver

__all__ = (
    "MSSQLAsyncConnectionContext",
    "MSSQLAsyncSessionContext",
    "MSSQLConnectionParams",
    "MSSQLDriverFeatures",
    "MSSQLPoolParams",
    "MSSQLSyncConnectionContext",
    "MSSQLSyncSessionContext",
)

_MISSING = object()


class MSSQLConnectionParams(TypedDict, total=False):
    """Typed connection parameters accepted by `MSSQLSyncConfig` and `MSSQLAsyncConfig`."""

    dsn: NotRequired[str]
    """MSSQL connection DSN string."""
    url: NotRequired[str]
    """MSSQL connection URL string."""
    connection_string: NotRequired[str]
    """Alternative MSSQL connection string alias."""
    driver: NotRequired[str]
    """Driver backend identifier."""
    host: NotRequired[str]
    """Database server hostname or address sequence."""
    server: NotRequired[str]
    """Database server host or instance name."""
    address: NotRequired[str]
    """Network address override for server connection."""
    addr: NotRequired[str]
    """Short alias for server network address."""
    port: NotRequired[int]
    """Database server TCP port or port sequence."""
    database: NotRequired[str]
    """Target database name, catalog, or file path."""
    db: NotRequired[str]
    """Alternative database name alias."""
    user: NotRequired[str]
    """Authenticating database username."""
    uid: NotRequired[str]
    """ODBC/TDS username alias."""
    username: NotRequired[str]
    """Alternative database username alias."""
    password: NotRequired[str]
    """Password credential for database authentication."""
    pwd: NotRequired[str]
    """Alternative password credential alias."""
    encrypt: NotRequired[bool | str]
    """Enable transport encryption on the TDS connection."""
    encryption: NotRequired[str]
    """Transport encryption mode string."""
    trust_server_certificate: NotRequired[bool | str]
    """Accept server TLS certificates without CA verification."""
    trust: NotRequired[bool | str]
    """Alias for ``trust_server_certificate``."""
    server_name: NotRequired[str | None]
    """TLS SNI server name override."""
    hostname_in_certificate: NotRequired[str]
    """Expected hostname inside server TLS certificate."""
    hostnameincertificate: NotRequired[str]
    """Case-insensitive alias for ``hostname_in_certificate``."""
    application_name: NotRequired[str]
    """Client application identifier reported on connect."""
    appname: NotRequired[str]
    """Short alias for ``application_name``."""
    application_intent: NotRequired[str]
    """Read-write or read-only workload routing intent."""
    applicationintent: NotRequired[str]
    """Case-insensitive alias for ``application_intent``."""
    read_only_intent: NotRequired[bool]
    """Route connection attempts toward read-only replicas."""
    timeout: NotRequired[float | int]
    """Connection or lock acquisition timeout in seconds."""
    connect_timeout: NotRequired[float | int]
    """TCP connect timeout in seconds."""
    login_timeout: NotRequired[float | int]
    """Authentication handshake timeout in seconds."""
    connection_timeout: NotRequired[float | int]
    """Connection acquisition timeout in seconds."""
    connect_timeout_ms: NotRequired[int]
    """Connection establishment timeout in milliseconds."""
    command_timeout: NotRequired[float | int]
    """Default statement execution timeout in seconds."""
    query_timeout_ms: NotRequired[int]
    """Default query execution timeout in milliseconds."""
    cloud_sql_instance: NotRequired[str | None]
    """Cloud SQL instance connection name (``project:region:instance``)."""
    cloudsql_ip_type: NotRequired[str]
    """Preferred IP type alias for Cloud SQL Dialer connections."""
    cloudsql_auth_type: NotRequired[str]
    """Authentication mode for Cloud SQL Dialer connections."""
    cloudsql_proxy_addr: NotRequired[str | None]
    """Override address for Cloud SQL proxy endpoint."""
    token_provider: NotRequired[Any]
    """Callable or provider supplying OAuth/Entra access tokens."""
    extra: NotRequired[dict[str, Any]]
    """Additional backend-specific keyword parameters."""


class MSSQLPoolParams(MSSQLConnectionParams, total=False):
    """Typed pool and registry-broker parameters extending `MSSQLConnectionParams`."""

    min_size: NotRequired[int]
    """Minimum warm connections retained in the pool."""
    max_size: NotRequired[int]
    """Maximum concurrent connections allowed in the pool."""
    pool_min_size: NotRequired[int]
    """Alias for minimum warm pool connections."""
    pool_max_size: NotRequired[int]
    """Alias for maximum pool connections."""
    pool_recycle: NotRequired[int]
    """Maximum connection lifetime in seconds before recycling."""
    share_pool: NotRequired[bool]
    """Share connection leases through the Vertebra SHM registry broker."""
    statement_cache_size: NotRequired[int]
    """Maximum cached prepared statements per connection."""


class MSSQLDriverFeatures(TypedDict, total=False):
    """Typed feature flags and lifecycle callbacks for SQL Server SQLSpec adapters."""

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
    cloud_sql_instance: NotRequired[str]
    """Cloud SQL instance connection name (``project:region:instance``)."""
    cloudsql_ip_type: NotRequired[str]
    """Preferred IP type alias for Cloud SQL Dialer connections."""
    cloudsql_auth_type: NotRequired[str]
    """Authentication mode for Cloud SQL Dialer connections."""
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
    """Return whether `conn` is an unopened or closed native MSSQL driver."""
    return getattr(conn, "_client", _MISSING) is None or bool(getattr(conn, "closed", False))


def _rollback_mssql_connection_sync(conn: Any) -> None:
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
        execute_fn("ROLLBACK TRANSACTION")


def _reset_mssql_connection_sync(conn: Any) -> None:
    """Reset SQL Server session state synchronously via driver reset hook or `EXEC sp_reset_connection`."""
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        reset_fn()
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn) and not _is_unopened_native_driver(conn):
        execute_fn("EXEC sp_reset_connection")


async def _rollback_mssql_connection_async(conn: Any) -> None:
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
            res = execute_fn("ROLLBACK TRANSACTION")
            if inspect.isawaitable(res):
                await res


async def _reset_mssql_connection_async(conn: Any) -> None:
    """Reset SQL Server session state asynchronously via driver reset hook or `EXEC sp_reset_connection`."""
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
            res = execute_fn("EXEC sp_reset_connection")
            if inspect.isawaitable(res):
                await res


class MSSQLSyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring native MSSQL drivers/leases and running hooks."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "MSSQL"
    _strict_cleanup: ClassVar[bool] = True

    def __init__(self, config: MSSQLSyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _rollback_conn(self, conn: Any) -> None:
        _rollback_mssql_connection_sync(conn)

    def _reset_conn(self, conn: Any) -> None:
        _reset_mssql_connection_sync(conn)

    def __enter__(self) -> SyncMSSQLDriver:
        return cast("SyncMSSQLDriver", super().__enter__())


class MSSQLAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring native MSSQL drivers/leases and running hooks."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "MSSQL"
    _strict_cleanup: ClassVar[bool] = True

    def __init__(self, config: MSSQLAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    async def _rollback_conn(self, conn: Any) -> None:
        await _rollback_mssql_connection_async(conn)

    async def _reset_conn(self, conn: Any) -> None:
        await _reset_mssql_connection_async(conn)

    async def __aenter__(self) -> AsyncMSSQLDriver:
        return cast("AsyncMSSQLDriver", await super().__aenter__())


class MSSQLSyncSessionContext(_AdapterSyncSessionContext):
    """Synchronous session context manager building a fresh `MSSQLSyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterSyncConnectionContext]] = MSSQLSyncConnectionContext
    _engine_label: ClassVar[str] = "MSSQL"
    _strict_cleanup: ClassVar[bool] = True

    def __enter__(self) -> MSSQLSyncDriver:
        return cast("MSSQLSyncDriver", super().__enter__())


class MSSQLAsyncSessionContext(_AdapterAsyncSessionContext):
    """Asynchronous session context manager building a fresh `MSSQLAsyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterAsyncConnectionContext]] = MSSQLAsyncConnectionContext
    _engine_label: ClassVar[str] = "MSSQL"
    _strict_cleanup: ClassVar[bool] = True

    async def __aenter__(self) -> MSSQLAsyncDriver:
        return cast("MSSQLAsyncDriver", await super().__aenter__())
