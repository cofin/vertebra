"""PostgreSQL SQLSpec adapter TypedDict schemas and session/connection context managers."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec._driver import (
    _AdapterAsyncConnectionContext,
    _AdapterAsyncSessionContext,
    _AdapterSyncConnectionContext,
    _AdapterSyncSessionContext,
)

if TYPE_CHECKING:
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
    connector_retry_count: NotRequired[int]
    """Maximum retry attempts on transient Cloud SQL or AlloyDB dialer errors (defaults to ``3``; ``0`` disables retries)."""
    connector_retry_delay: NotRequired[float]
    """Initial backoff delay in seconds before jittered exponential dialer retries (defaults to ``0.5``)."""
    connector_retry_max_delay: NotRequired[float]
    """Upper bound in seconds on exponential backoff between dialer retry attempts (defaults to ``5.0``)."""
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


class PostgresSyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring and releasing PostgreSQL leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "PostgreSQL"
    _run_on_connection_create: ClassVar[bool] = False

    def __init__(self, config: PostgresSyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _rollback_conn(self, conn: Any) -> None:
        _rollback_connection_sync(conn)

    def _reset_conn(self, conn: Any) -> None:
        _reset_connection_sync(conn)


class PostgresAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring and releasing PostgreSQL leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "PostgreSQL"
    _run_on_connection_create: ClassVar[bool] = False

    def __init__(self, config: PostgresAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    async def _rollback_conn(self, conn: Any) -> None:
        await _rollback_connection_async(conn)

    async def _reset_conn(self, conn: Any) -> None:
        await _reset_connection_async(conn)


class PostgresSyncSessionContext(_AdapterSyncSessionContext):
    """Synchronous session context manager yielding a fresh `PostgresSyncDriver` per entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterSyncConnectionContext]] = PostgresSyncConnectionContext
    _engine_label: ClassVar[str] = "PostgreSQL"

    def __enter__(self) -> PostgresSyncDriver:
        return cast("PostgresSyncDriver", super().__enter__())


class PostgresAsyncSessionContext(_AdapterAsyncSessionContext):
    """Asynchronous session context manager yielding a fresh `PostgresAsyncDriver` per entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterAsyncConnectionContext]] = PostgresAsyncConnectionContext
    _engine_label: ClassVar[str] = "PostgreSQL"

    async def __aenter__(self) -> PostgresAsyncDriver:
        return cast("PostgresAsyncDriver", await super().__aenter__())
