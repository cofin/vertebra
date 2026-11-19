"""PostgreSQL SQLSpec adapter TypedDict schemas and session/connection context managers."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.core import StatementConfig

    from vertebra.db.driver._cloud_auth import DatabaseTokenProvider
    from vertebra.extensions.sqlspec.adapters.postgres.config import PostgresAsyncConfig, PostgresSyncConfig
    from vertebra.extensions.sqlspec.adapters.postgres.driver import PostgresAsyncDriver, PostgresSyncDriver

__all__ = (
    "PostgresAsyncConnectionContext",
    "PostgresAsyncSessionContext",
    "PostgresConnectionParams",
    "PostgresDriverFeatures",
    "PostgresPoolParams",
    "PostgresSyncConnectionContext",
    "PostgresSyncSessionContext",
)


class PostgresConnectionParams(TypedDict):
    """Typed connection parameters accepted by PostgreSQL SQLSpec adapter configurations."""

    dsn: NotRequired[str]
    """Postgres connection DSN string."""
    host: NotRequired[str | Sequence[str]]
    """Database server hostname or address sequence."""
    port: NotRequired[int | Sequence[int]]
    """Database server TCP port or port sequence."""
    user: NotRequired[str]
    """Authenticating database username."""
    password: NotRequired[str]
    """Password credential for database authentication."""
    database: NotRequired[str]
    """Target database name, catalog, or file path."""
    ssl: NotRequired[Any]
    """TLS/SSL configuration mapping or flag."""
    timeout: NotRequired[float]
    """Connection or lock acquisition timeout in seconds."""
    command_timeout: NotRequired[float]
    """Default statement execution timeout in seconds."""
    server_settings: NotRequired[dict[str, str]]
    """PostgreSQL GUC runtime parameters sent at startup."""
    statement_cache_size: NotRequired[int]
    """Maximum cached prepared statements per connection."""
    direct_dsn: NotRequired[str | Mapping[str, Any] | None]
    """Direct database DSN bypassing pooler endpoints when needed."""
    token_provider: NotRequired[DatabaseTokenProvider | None]
    """Cloud database token provider supplying dynamic IAM passwords."""
    ca_pem: NotRequired[str | None]
    """Inline Root CA PEM bundle used for PostgreSQL TLS verification."""


class PostgresPoolParams(PostgresConnectionParams):
    """Typed connection and pool parameters accepted by PostgreSQL SQLSpec adapter configurations."""

    min_size: NotRequired[int]
    """Minimum warm connections retained in the pool."""
    max_size: NotRequired[int]
    """Maximum concurrent connections allowed in the pool."""
    max_queries: NotRequired[int]
    """Maximum queries executed before rotating an async pool connection."""
    max_inactive_connection_lifetime: NotRequired[float]
    """Idle lifetime in seconds before closing a pooled connection."""
    setup: NotRequired[Callable[[Any], Any]]
    """Callable executed after acquiring each connection."""
    init: NotRequired[Callable[[Any], Any]]
    """Callable executed once per newly opened connection."""
    reset: NotRequired[Callable[[Any], Any]]
    """Callable invoked when resetting a connection before release."""


class PostgresDriverFeatures(TypedDict):
    """Typed feature flags supported by PostgreSQL SQLSpec adapter configurations and drivers."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Custom JSON serializer callable for parameter encoding."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Custom JSON deserializer callable for result decoding."""
    enable_pgvector: NotRequired[bool]
    """Enable PostgreSQL pgvector binary/text codec registration."""
    enable_json_bson: NotRequired[bool]
    """Enable BSON/JSONB codec acceleration helpers."""
    enable_cloud_sql: NotRequired[bool]
    """Enable Cloud SQL Dialer transport."""
    enable_alloydb: NotRequired[bool]
    """Enable AlloyDB Dialer transport."""
    pooler_mode: NotRequired[Literal["direct", "session", "transaction", "statement", "auto"] | None]
    """PgBouncer or connection pooler multiplexing mode."""
    reset_session_on_release: NotRequired[bool]
    """Reset session state automatically when returning a connection."""
    tenant_strategy: NotRequired[Any]
    """Multi-tenant schema or row isolation strategy."""
    stack_native_disabled: NotRequired[bool]
    """Disable native Rust transport optimizations for compatibility fallback."""
    on_connection_create: NotRequired[Callable[[Any], Any]]
    """Hook invoked with each newly established connection."""


def _rollback_connection_sync(conn: Any) -> None:
    if not getattr(conn, "in_transaction", False):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with contextlib.suppress(Exception):
            rollback_fn()
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn):
        with contextlib.suppress(Exception):
            execute_fn("ROLLBACK")


def _reset_connection_sync(conn: Any) -> None:
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with contextlib.suppress(Exception):
            reset_fn()
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn):
        with contextlib.suppress(Exception):
            execute_fn("RESET ALL")


async def _rollback_connection_async(conn: Any) -> None:
    if not getattr(conn, "in_transaction", False):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res: Any = rollback_fn()
            if inspect.isawaitable(res):
                await res
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = execute_fn("ROLLBACK")
            if inspect.isawaitable(res):
                await res


async def _reset_connection_async(conn: Any) -> None:
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res: Any = reset_fn()
            if inspect.isawaitable(res):
                await res
        return
    execute_fn = getattr(conn, "execute", None)
    if callable(execute_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = execute_fn("RESET ALL")
            if inspect.isawaitable(res):
                await res


class PostgresSyncConnectionContext:
    """Synchronous connection context manager acquiring and releasing PostgreSQL leases."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: PostgresSyncConfig, *args: Any, **kwargs: Any) -> None:
        """Store configuration and forwarded connection arguments."""
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._tenant_lease: Any = None

    def __enter__(self) -> Any:
        """Acquire a native PostgreSQL connection or tenant lease."""
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = self._tenant_lease.acquire()
            return self._connection
        pool = self._config.provide_pool()
        get_conn = getattr(pool, "get_connection", None)
        if callable(get_conn):
            self._conn_ctx = get_conn()
            self._connection = self._conn_ctx.__enter__()
        else:
            self._connection = pool
        return self._connection

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back open transactions, optionally reset session state, and release the lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                _rollback_connection_sync(conn)
                if reset_on_release:
                    _reset_connection_sync(conn)
            if self._tenant_lease is not None:
                self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", self._conn_ctx.__exit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class PostgresAsyncConnectionContext:
    """Asynchronous connection context manager acquiring and releasing PostgreSQL leases."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: PostgresAsyncConfig, *args: Any, **kwargs: Any) -> None:
        """Store configuration and forwarded connection arguments."""
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> Any:
        """Acquire an asynchronous native PostgreSQL connection or tenant lease."""
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = await self._tenant_lease.acquire()
            return self._connection
        pool = await self._config.provide_pool()
        get_conn = getattr(pool, "get_connection", None)
        if callable(get_conn):
            self._conn_ctx = get_conn()
            self._connection = await self._conn_ctx.__aenter__()
        else:
            self._connection = pool
        return self._connection

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back open transactions, optionally reset session state, and release the lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                await _rollback_connection_async(conn)
                if reset_on_release:
                    await _reset_connection_async(conn)
            if self._tenant_lease is not None:
                await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class PostgresSyncSessionContext:
    """Synchronous session context manager yielding a fresh `PostgresSyncDriver` per entry."""

    __slots__ = (
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_driver",
        "_kwargs",
        "_statement_config",
        "_tenant_lease",
    )

    def __init__(
        self,
        config: PostgresSyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """Store configuration, optional statement override, and forwarded session arguments."""
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._driver: PostgresSyncDriver | None = None
        self._tenant_lease: Any = None

    def __enter__(self) -> PostgresSyncDriver:
        """Acquire a lease, instantiate a fresh `PostgresSyncDriver`, and prepare observability."""
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
        """Close open streams, roll back open transactions, reset session state, release lease, and mark closed."""
        driver = self._driver
        conn = self._connection
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
                    self._config.share_pool or self._tenant_lease is not None or self._conn_ctx is not None
                )
                if driver is not None and (exc_type is not None or pooled_state or reset_on_release):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        with contextlib.suppress(Exception):
                            driver.rollback()
                if conn is not None and reset_on_release:
                    _reset_connection_sync(conn)
                if self._tenant_lease is not None:
                    self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
                elif self._conn_ctx is not None:
                    self._conn_ctx.__exit__(exc_type, exc_val, exc_tb)
            finally:
                self._connection = None
                self._conn_ctx = None
                self._tenant_lease = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None


class PostgresAsyncSessionContext:
    """Asynchronous session context manager yielding a fresh `PostgresAsyncDriver` per entry."""

    __slots__ = (
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_driver",
        "_kwargs",
        "_statement_config",
        "_tenant_lease",
    )

    def __init__(
        self,
        config: PostgresAsyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        """Store configuration, optional statement override, and forwarded session arguments."""
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: Any = None
        self._driver: PostgresAsyncDriver | None = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> PostgresAsyncDriver:
        """Acquire a lease, instantiate a fresh `PostgresAsyncDriver`, and prepare observability."""
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
        """Close open streams, roll back open transactions, reset session state, release lease, and mark closed."""
        driver = self._driver
        conn = self._connection
        try:
            if driver is not None:
                close_streams_fn = getattr(driver, "_close_open_streams", None)
                if callable(close_streams_fn):
                    res: Any = close_streams_fn(error=exc_type is not None)
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
                    self._config.share_pool or self._tenant_lease is not None or self._conn_ctx is not None
                )
                if driver is not None and (exc_type is not None or pooled_state or reset_on_release):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx:
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            res = driver.rollback()
                            if hasattr(res, "__await__"):
                                await res
                if conn is not None and reset_on_release:
                    await _reset_connection_async(conn)
                if self._tenant_lease is not None:
                    await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
                elif self._conn_ctx is not None:
                    await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb)
            finally:
                self._connection = None
                self._conn_ctx = None
                self._tenant_lease = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None
