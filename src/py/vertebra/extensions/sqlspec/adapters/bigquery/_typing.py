"""BigQuery SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec._driver import (
    _AdapterAsyncConnectionContext,
    _AdapterAsyncSessionContext,
    _AdapterSyncConnectionContext,
    _AdapterSyncSessionContext,
)

if TYPE_CHECKING:
    from vertebra.db.adapters.bigquery import AsyncBigQueryDriver, SyncBigQueryDriver
    from vertebra.extensions.sqlspec.adapters.bigquery.config import BigQueryAsyncConfig, BigQuerySyncConfig
    from vertebra.extensions.sqlspec.adapters.bigquery.driver import BigQueryAsyncDriver, BigQuerySyncDriver

__all__ = (
    "BigQueryAsyncConnectionContext",
    "BigQueryAsyncSessionContext",
    "BigQueryConnectionParams",
    "BigQueryDriverFeatures",
    "BigQueryPoolParams",
    "BigQuerySyncConnectionContext",
    "BigQuerySyncSessionContext",
)


class BigQueryConnectionParams(TypedDict):
    """Typed connection parameters accepted by `BigQuerySyncConfig` and `BigQueryAsyncConfig`."""

    project: NotRequired[str]
    """Google Cloud project identifier."""
    project_id: NotRequired[str]
    """Google Cloud project identifier alias."""
    dataset_id: NotRequired[str]
    """Default BigQuery dataset identifier alias."""
    dataset: NotRequired[str]
    """Default BigQuery dataset identifier."""
    dsn: NotRequired[str]
    """BigQuery connection DSN string."""
    url: NotRequired[str]
    """BigQuery connection URL string."""
    location: NotRequired[str]
    """Default geographic processing location or region."""
    credentials_path: NotRequired[str | None]
    """Path to service account JSON credentials file."""
    scopes: NotRequired[Sequence[str]]
    """OAuth2 scope URIs requested for cloud credentials."""
    rest_endpoint: NotRequired[str | None]
    """Custom BigQuery REST API endpoint URL override."""
    storage_endpoint: NotRequired[str | None]
    """Custom BigQuery Storage Read/Write API endpoint URL."""
    maximum_bytes_billed: NotRequired[int | None]
    """Maximum bytes billed per BigQuery query job."""
    session_enabled: NotRequired[bool]
    """Enable stateful multi-statement BigQuery sessions."""
    job_timeout_ms: NotRequired[int | None]
    """BigQuery query job execution timeout in milliseconds."""
    priority: NotRequired[str]
    """BigQuery query job priority (``INTERACTIVE`` or ``BATCH``)."""
    labels: NotRequired[Mapping[str, str]]
    """Key-value billing and audit labels attached to query jobs."""
    quota_project_id: NotRequired[str | None]
    """Billing and quota attribution Google Cloud project ID."""
    access_token: NotRequired[str | None]
    """Static or dynamic bearer access token credential."""
    use_gcloud_cli: NotRequired[bool]
    """Allow fallback to active ``gcloud`` CLI credentials."""
    pool_size: NotRequired[int]
    """Target number of pooled sessions or connections."""
    statement_cache_capacity: NotRequired[int]
    """Maximum cached prepared statements per connection."""
    use_shm_cache: NotRequired[bool]
    """Enable shared-memory Arrow result caching."""
    arrow_compression: NotRequired[str]
    """Arrow IPC stream compression codec (``lz4`` or ``zstd``)."""
    max_read_streams: NotRequired[int]
    """Maximum parallel BigQuery Storage Read API streams."""
    extra: NotRequired[dict[str, Any]]
    """Additional backend-specific keyword parameters."""


class BigQueryPoolParams(BigQueryConnectionParams):
    """Typed pool and connection-instance parameters extending `BigQueryConnectionParams`."""

    share_pool: NotRequired[bool]
    """Share connection leases through the Vertebra SHM registry broker."""
    connection_instance: NotRequired[Any]
    """Pre-constructed driver or pool instance override."""


class BigQueryDriverFeatures(TypedDict):
    """Typed feature flags and callbacks supported by BigQuery SQLSpec adapters."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Custom JSON serializer callable for parameter encoding."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Custom JSON deserializer callable for result decoding."""
    enable_native_storage: NotRequired[bool]
    """Enable Storage Read/Write API acceleration paths."""
    native_export_connection: NotRequired[str | None]
    """Connection or bucket URI for BigQuery export stages."""
    job_retry_deadline: NotRequired[float]
    """Maximum retry deadline in seconds for transient job errors."""
    reset_session_on_release: NotRequired[bool]
    """Reset session state automatically when returning a connection."""
    storage_backend: NotRequired[Any]
    """Default object storage backend handle for external exports."""
    on_connection_create: NotRequired[Callable[[Any], Any]]
    """Hook invoked with each newly established connection."""
    connection_instance: NotRequired[Any]
    """Pre-constructed driver or pool instance override."""


def _needs_connect(conn: Any) -> bool:
    """Return whether a native BigQuery driver or shim requires connection initialization."""
    is_connected = getattr(conn, "is_connected", None)
    if is_connected is not None:
        return not bool(is_connected)
    if getattr(conn, "closed", False):
        return True
    return hasattr(conn, "_client") and getattr(conn, "_client", None) is None


def _ensure_connected_sync(conn: Any) -> None:
    """Connect a synchronous BigQuery native driver if not yet connected."""
    if conn is None:
        return
    connect_fn = getattr(conn, "connect", None)
    if callable(connect_fn) and _needs_connect(conn):
        connect_fn()


async def _ensure_connected_async(conn: Any) -> None:
    """Connect an asynchronous BigQuery native driver if not yet connected."""
    if conn is None:
        return
    connect_fn = getattr(conn, "connect", None)
    if callable(connect_fn) and _needs_connect(conn):
        res = connect_fn()
        if inspect.isawaitable(res):
            await res


def _rollback_connection_sync(conn: Any) -> None:
    """Roll back any active BigQuery session transaction synchronously."""
    if not getattr(conn, "in_transaction", False):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with contextlib.suppress(Exception):
            rollback_fn()


async def _rollback_connection_async(conn: Any) -> None:
    """Roll back any active BigQuery session transaction asynchronously under cancellation shielding."""
    if not getattr(conn, "in_transaction", False):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = rollback_fn()
            if inspect.isawaitable(res):
                await res


def _reset_session_sync(conn: Any) -> None:
    """Reset active BigQuery session state safely; no-op on stateless REST connections without a session."""
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with contextlib.suppress(Exception):
            reset_fn()
        return
    session_id = getattr(conn, "session_id", None) or getattr(conn, "_session_id", None)
    if not session_id:
        return
    try:
        if getattr(conn, "_client", None) is not None and callable(getattr(conn, "execute", None)):
            with contextlib.suppress(Exception):
                conn.execute("CALL BQ.ABORT_SESSION(@session_id)", {"session_id": str(session_id)})
    finally:
        if hasattr(conn, "_session_id"):
            conn._session_id = None
        if hasattr(conn, "_in_transaction"):
            conn._in_transaction = False


async def _reset_session_async(conn: Any) -> None:
    """Asynchronously reset active BigQuery session state safely; no-op on stateless REST connections."""
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = reset_fn()
            if inspect.isawaitable(res):
                await res
        return
    session_id = getattr(conn, "session_id", None) or getattr(conn, "_session_id", None)
    if not session_id:
        return
    try:
        if getattr(conn, "_client", None) is not None and callable(getattr(conn, "execute", None)):
            with CancelScope(shield=True), contextlib.suppress(Exception):
                res = conn.execute("CALL BQ.ABORT_SESSION(@session_id)", {"session_id": str(session_id)})
                if inspect.isawaitable(res):
                    await res
    finally:
        if hasattr(conn, "_session_id"):
            conn._session_id = None
        if hasattr(conn, "_in_transaction"):
            conn._in_transaction = False


class BigQuerySyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring native BigQuery drivers or tenant leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "BigQuery"

    def __init__(self, config: BigQuerySyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _on_acquire(self, conn: Any) -> Any:
        _ensure_connected_sync(conn)
        return super()._on_acquire(conn)

    def _rollback_conn(self, conn: Any) -> None:
        _rollback_connection_sync(conn)

    def _reset_conn(self, conn: Any) -> None:
        _reset_session_sync(conn)

    def __enter__(self) -> SyncBigQueryDriver:
        return cast("SyncBigQueryDriver", super().__enter__())


class BigQueryAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring native BigQuery drivers or tenant leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "BigQuery"

    def __init__(self, config: BigQueryAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    async def _on_acquire(self, conn: Any) -> Any:
        await _ensure_connected_async(conn)
        return await super()._on_acquire(conn)

    async def _rollback_conn(self, conn: Any) -> None:
        await _rollback_connection_async(conn)

    async def _reset_conn(self, conn: Any) -> None:
        await _reset_session_async(conn)

    async def __aenter__(self) -> AsyncBigQueryDriver:
        return cast("AsyncBigQueryDriver", await super().__aenter__())


class BigQuerySyncSessionContext(_AdapterSyncSessionContext):
    """Synchronous session context manager building a fresh `BigQuerySyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterSyncConnectionContext]] = BigQuerySyncConnectionContext
    _engine_label: ClassVar[str] = "BigQuery"

    def __enter__(self) -> BigQuerySyncDriver:
        return cast("BigQuerySyncDriver", super().__enter__())


class BigQueryAsyncSessionContext(_AdapterAsyncSessionContext):
    """Asynchronous session context manager building a fresh `BigQueryAsyncDriver` per session entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterAsyncConnectionContext]] = BigQueryAsyncConnectionContext
    _engine_label: ClassVar[str] = "BigQuery"

    async def __aenter__(self) -> BigQueryAsyncDriver:
        return cast("BigQueryAsyncDriver", await super().__aenter__())
