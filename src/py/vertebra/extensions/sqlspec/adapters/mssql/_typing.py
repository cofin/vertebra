"""SQL Server (MSSQL) SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

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


def _record_exit_exception(primary: BaseException | None, secondary: BaseException, label: str) -> BaseException:
    """Promote `secondary` or chain it onto `primary` via `add_note`."""
    if primary is None:
        return secondary
    primary.add_note(f"{label}: {secondary!r}")
    return primary


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


class MSSQLSyncConnectionContext:
    """Synchronous connection context manager acquiring native MSSQL drivers/leases and running hooks."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_created_unshared", "_kwargs", "_tenant_lease")

    def __init__(self, config: MSSQLSyncConfig, *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: SyncMSSQLDriver | None = None
        self._created_unshared: bool = False
        self._tenant_lease: Any = None

    def __enter__(self) -> SyncMSSQLDriver:
        """Acquire a native MSSQL driver or tenant lease and invoke `on_connection_create`."""
        lease = self._config._create_tenant_lease()
        if lease is not None:
            self._tenant_lease = lease
            try:
                self._connection = lease.acquire()
            except BaseException:
                self._tenant_lease = None
                raise
        else:
            had_instance = self._config.connection_instance is not None
            pool = self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            if callable(get_conn):
                conn_ctx: Any = cast("Any", get_conn)()
                self._connection = conn_ctx.__enter__()
                self._conn_ctx = conn_ctx
            else:
                self._connection = pool
                self._created_unshared = not had_instance and not self._config.share_pool
        try:
            initialized_ids: set[int] | None = getattr(self._config, "_initialized_conn_ids", None)
            conn_id = id(self._connection)
            if initialized_ids is None or conn_id not in initialized_ids:
                hook = self._config.driver_features.get("on_connection_create")
                if callable(hook):
                    hook(self._connection)
                if initialized_ids is not None:
                    initialized_ids.add(conn_id)
            return cast("SyncMSSQLDriver", self._connection)
        except BaseException as exc:
            self._cleanup_enter_failure(exc)
            raise

    def _cleanup_enter_failure(self, exc: BaseException) -> None:
        conn = self._connection
        created_unshared = self._created_unshared
        try:
            self.__exit__(type(exc), exc, exc.__traceback__)
        except BaseException as cleanup_exc:
            if cleanup_exc is not exc:
                exc.add_note(f"MSSQL connection enter cleanup failed: {cleanup_exc!r}")
        if created_unshared and conn is not None:
            close_fn = getattr(conn, "close", None)
            if callable(close_fn):
                with contextlib.suppress(Exception):
                    close_fn()
            if self._config.connection_instance is conn:
                self._config.connection_instance = None

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back uncommitted work, optionally reset session state, and release the lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        primary_exc: BaseException | None = exc_val
        suppressed: bool | None = None
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                try:
                    _rollback_mssql_connection_sync(conn)
                except BaseException as rb_exc:
                    primary_exc = _record_exit_exception(primary_exc, rb_exc, "MSSQL connection rollback failed")
                if reset_on_release:
                    try:
                        _reset_mssql_connection_sync(conn)
                    except BaseException as reset_exc:
                        primary_exc = _record_exit_exception(primary_exc, reset_exc, "MSSQL session reset failed")
            rel_type = type(primary_exc) if primary_exc is not None else exc_type
            rel_val = primary_exc if primary_exc is not None else exc_val
            rel_tb = primary_exc.__traceback__ if primary_exc is not None else exc_tb
            if self._tenant_lease is not None:
                try:
                    self._tenant_lease.release(exc_type=rel_type, exc_val=rel_val, exc_tb=rel_tb)
                except BaseException as rel_exc:
                    primary_exc = _record_exit_exception(primary_exc, rel_exc, "MSSQL tenant lease release failed")
            elif self._conn_ctx is not None:
                try:
                    suppressed = cast("bool | None", self._conn_ctx.__exit__(rel_type, rel_val, rel_tb))
                except BaseException as ctx_exc:
                    primary_exc = _record_exit_exception(primary_exc, ctx_exc, "MSSQL connection release failed")
        finally:
            self._connection = None
            self._conn_ctx = None
            self._created_unshared = False
            self._tenant_lease = None
        if primary_exc is not None and primary_exc is not exc_val:
            raise primary_exc
        return suppressed


class MSSQLAsyncConnectionContext:
    """Asynchronous connection context manager acquiring native MSSQL drivers/leases and running hooks."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_created_unshared", "_kwargs", "_tenant_lease")

    def __init__(self, config: MSSQLAsyncConfig, *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: AsyncMSSQLDriver | None = None
        self._created_unshared: bool = False
        self._tenant_lease: Any = None

    async def __aenter__(self) -> AsyncMSSQLDriver:
        """Asynchronously acquire a native MSSQL driver or lease and invoke `on_connection_create`."""
        lease = self._config._create_tenant_lease()
        if lease is not None:
            self._tenant_lease = lease
            try:
                self._connection = await lease.acquire()
            except BaseException:
                self._tenant_lease = None
                raise
        else:
            had_instance = self._config.connection_instance is not None
            pool = await self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            if callable(get_conn):
                conn_ctx: Any = cast("Any", get_conn)()
                self._connection = await conn_ctx.__aenter__()
                self._conn_ctx = conn_ctx
            else:
                self._connection = pool
                self._created_unshared = not had_instance and not self._config.share_pool
        try:
            initialized_ids: set[int] | None = getattr(self._config, "_initialized_conn_ids", None)
            conn_id = id(self._connection)
            if initialized_ids is None or conn_id not in initialized_ids:
                hook = self._config.driver_features.get("on_connection_create")
                if callable(hook):
                    hook_res = hook(self._connection)
                    if inspect.isawaitable(hook_res):
                        await hook_res
                if initialized_ids is not None:
                    initialized_ids.add(conn_id)
            return cast("AsyncMSSQLDriver", self._connection)
        except BaseException as exc:
            await self._cleanup_aenter_failure(exc)
            raise

    async def _cleanup_aenter_failure(self, exc: BaseException) -> None:
        conn = self._connection
        created_unshared = self._created_unshared
        try:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
        except BaseException as cleanup_exc:
            if cleanup_exc is not exc:
                exc.add_note(f"MSSQL async connection enter cleanup failed: {cleanup_exc!r}")
        if created_unshared and conn is not None:
            close_fn = getattr(conn, "close", None)
            if callable(close_fn):
                with CancelScope(shield=True), contextlib.suppress(Exception):
                    res = close_fn()
                    if inspect.isawaitable(res):
                        await res
            if self._config.connection_instance is conn:
                self._config.connection_instance = None

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back uncommitted work, optionally reset session state, and release the lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        primary_exc: BaseException | None = exc_val
        suppressed: bool | None = None
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                try:
                    await _rollback_mssql_connection_async(conn)
                except BaseException as rb_exc:
                    primary_exc = _record_exit_exception(primary_exc, rb_exc, "MSSQL async connection rollback failed")
                if reset_on_release:
                    try:
                        await _reset_mssql_connection_async(conn)
                    except BaseException as reset_exc:
                        primary_exc = _record_exit_exception(primary_exc, reset_exc, "MSSQL async session reset failed")
            rel_type = type(primary_exc) if primary_exc is not None else exc_type
            rel_val = primary_exc if primary_exc is not None else exc_val
            rel_tb = primary_exc.__traceback__ if primary_exc is not None else exc_tb
            if self._tenant_lease is not None:
                try:
                    with CancelScope(shield=True):
                        await self._tenant_lease.release(exc_type=rel_type, exc_val=rel_val, exc_tb=rel_tb)
                except BaseException as rel_exc:
                    primary_exc = _record_exit_exception(
                        primary_exc, rel_exc, "MSSQL async tenant lease release failed"
                    )
            elif self._conn_ctx is not None:
                try:
                    with CancelScope(shield=True):
                        suppressed = cast(
                            "bool | None",
                            await self._conn_ctx.__aexit__(rel_type, rel_val, rel_tb),
                        )
                except BaseException as ctx_exc:
                    primary_exc = _record_exit_exception(primary_exc, ctx_exc, "MSSQL async connection release failed")
        finally:
            self._connection = None
            self._conn_ctx = None
            self._created_unshared = False
            self._tenant_lease = None
        if primary_exc is not None and primary_exc is not exc_val:
            raise primary_exc
        return suppressed


class MSSQLSyncSessionContext:
    """Synchronous session context manager building a fresh `MSSQLSyncDriver` per session entry."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_driver", "_kwargs", "_statement_config")

    def __init__(
        self,
        config: MSSQLSyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: MSSQLSyncConnectionContext | None = None
        self._connection: SyncMSSQLDriver | None = None
        self._driver: MSSQLSyncDriver | None = None

    def __enter__(self) -> MSSQLSyncDriver:
        """Acquire a leased connection and instantiate a fresh `MSSQLSyncDriver`."""
        conn_ctx = MSSQLSyncConnectionContext(self._config, *self._args, **self._kwargs)
        self._connection = conn_ctx.__enter__()
        self._conn_ctx = conn_ctx
        try:
            stmt_cfg = self._statement_config if self._statement_config is not None else self._config.statement_config
            driver = self._config.driver_type(
                self._connection,
                statement_config=stmt_cfg,
                driver_features=dict(self._config.driver_features),
                shared_pool=self._config._pool_is_shared,
            )
            self._driver = driver
            self._driver = self._config._prepare_driver(driver)
            return self._driver
        except BaseException as exc:
            try:
                self.__exit__(type(exc), exc, exc.__traceback__)
            except BaseException as cleanup_exc:
                if cleanup_exc is not exc:
                    exc.add_note(f"MSSQL session enter cleanup failed: {cleanup_exc!r}")
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Close open streams, roll back uncommitted work, release/reset lease, and mark driver closed."""
        driver = self._driver
        conn_ctx = self._conn_ctx
        primary_exc: BaseException | None = exc_val
        suppressed: bool | None = None
        try:
            if driver is not None:
                close_streams_fn = getattr(driver, "_close_open_streams", None)
                if callable(close_streams_fn):
                    try:
                        close_streams_fn(error=exc_type is not None)
                    except BaseException as close_exc:
                        primary_exc = _record_exit_exception(primary_exc, close_exc, "MSSQL stream close failed")
        finally:
            try:
                reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
                pooled_state = bool(
                    self._config.share_pool
                    or (conn_ctx is not None and (conn_ctx._tenant_lease is not None or conn_ctx._conn_ctx is not None))
                )
                if driver is not None and (
                    exc_type is not None or primary_exc is not None or pooled_state or reset_on_release
                ):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        try:
                            driver.rollback()
                        except BaseException as rb_exc:
                            primary_exc = _record_exit_exception(primary_exc, rb_exc, "MSSQL session rollback failed")
                if conn_ctx is not None:
                    rel_type = type(primary_exc) if primary_exc is not None else exc_type
                    rel_val = primary_exc if primary_exc is not None else exc_val
                    rel_tb = primary_exc.__traceback__ if primary_exc is not None else exc_tb
                    try:
                        suppressed = conn_ctx.__exit__(rel_type, rel_val, rel_tb)
                    except BaseException as rel_exc:
                        primary_exc = _record_exit_exception(
                            primary_exc, rel_exc, "MSSQL connection context exit failed"
                        )
            finally:
                self._connection = None
                self._conn_ctx = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        if primary_exc is not None and primary_exc is not exc_val:
            raise primary_exc
        return suppressed


class MSSQLAsyncSessionContext:
    """Asynchronous session context manager building a fresh `MSSQLAsyncDriver` per session entry."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_driver", "_kwargs", "_statement_config")

    def __init__(
        self,
        config: MSSQLAsyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: MSSQLAsyncConnectionContext | None = None
        self._connection: AsyncMSSQLDriver | None = None
        self._driver: MSSQLAsyncDriver | None = None

    async def __aenter__(self) -> MSSQLAsyncDriver:
        """Asynchronously acquire a leased connection and instantiate a fresh `MSSQLAsyncDriver`."""
        conn_ctx = MSSQLAsyncConnectionContext(self._config, *self._args, **self._kwargs)
        self._connection = await conn_ctx.__aenter__()
        self._conn_ctx = conn_ctx
        try:
            stmt_cfg = self._statement_config if self._statement_config is not None else self._config.statement_config
            driver = self._config.driver_type(
                self._connection,
                statement_config=stmt_cfg,
                driver_features=dict(self._config.driver_features),
                shared_pool=self._config._pool_is_shared,
            )
            self._driver = driver
            self._driver = self._config._prepare_driver(driver)
            return self._driver
        except BaseException as exc:
            try:
                await self.__aexit__(type(exc), exc, exc.__traceback__)
            except BaseException as cleanup_exc:
                if cleanup_exc is not exc:
                    exc.add_note(f"MSSQL async session enter cleanup failed: {cleanup_exc!r}")
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Close open streams, roll back uncommitted work, release/reset lease, and mark driver closed."""
        driver = self._driver
        conn_ctx = self._conn_ctx
        primary_exc: BaseException | None = exc_val
        suppressed: bool | None = None
        try:
            if driver is not None:
                close_streams_fn = getattr(driver, "_close_open_streams", None)
                if callable(close_streams_fn):
                    try:
                        res = close_streams_fn(error=exc_type is not None)
                        if inspect.isawaitable(res):
                            await res
                    except BaseException as close_exc:
                        primary_exc = _record_exit_exception(primary_exc, close_exc, "MSSQL async stream close failed")
        finally:
            try:
                reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
                pooled_state = bool(
                    self._config.share_pool
                    or (conn_ctx is not None and (conn_ctx._tenant_lease is not None or conn_ctx._conn_ctx is not None))
                )
                if driver is not None and (
                    exc_type is not None or primary_exc is not None or pooled_state or reset_on_release
                ):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        try:
                            with CancelScope(shield=True):
                                await driver.rollback()
                        except BaseException as rb_exc:
                            primary_exc = _record_exit_exception(
                                primary_exc, rb_exc, "MSSQL async session rollback failed"
                            )
                if conn_ctx is not None:
                    rel_type = type(primary_exc) if primary_exc is not None else exc_type
                    rel_val = primary_exc if primary_exc is not None else exc_val
                    rel_tb = primary_exc.__traceback__ if primary_exc is not None else exc_tb
                    try:
                        suppressed = await conn_ctx.__aexit__(rel_type, rel_val, rel_tb)
                    except BaseException as rel_exc:
                        primary_exc = _record_exit_exception(
                            primary_exc, rel_exc, "MSSQL async connection context exit failed"
                        )
            finally:
                self._connection = None
                self._conn_ctx = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        if primary_exc is not None and primary_exc is not exc_val:
            raise primary_exc
        return suppressed
