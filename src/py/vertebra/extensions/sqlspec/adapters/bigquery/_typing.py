"""BigQuery SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict, cast

from anyio import CancelScope

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

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


class BigQuerySyncConnectionContext:
    """Synchronous connection context manager acquiring native BigQuery drivers or tenant leases."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: BigQuerySyncConfig, *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: SyncBigQueryDriver | None = None
        self._tenant_lease: Any = None

    def __enter__(self) -> SyncBigQueryDriver:
        """Acquire a native BigQuery driver or tenant lease, connect if needed, and run creation hooks."""
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
            _ensure_connected_sync(self._connection)
            initialized_ids: set[int] | None = getattr(self._config, "_initialized_conn_ids", None)
            conn_id = id(self._connection)
            if initialized_ids is None or conn_id not in initialized_ids:
                hook = self._config.driver_features.get("on_connection_create")
                if callable(hook):
                    hook(self._connection)
                if initialized_ids is not None:
                    initialized_ids.add(conn_id)
            return cast("SyncBigQueryDriver", self._connection)
        except BaseException as exc:
            self.__exit__(type(exc), exc, exc.__traceback__)
            raise

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back on error, optionally reset active session state, and release the lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                _rollback_connection_sync(conn)
                if reset_on_release:
                    _reset_session_sync(conn)
            if self._tenant_lease is not None:
                self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", self._conn_ctx.__exit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class BigQueryAsyncConnectionContext:
    """Asynchronous connection context manager acquiring native BigQuery drivers or tenant leases."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_kwargs", "_tenant_lease")

    def __init__(self, config: BigQueryAsyncConfig, *args: Any, **kwargs: Any) -> None:
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._connection: AsyncBigQueryDriver | None = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> AsyncBigQueryDriver:
        """Asynchronously acquire a native BigQuery driver or lease, connect if needed, and run hooks."""
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
            await _ensure_connected_async(self._connection)
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
            return cast("AsyncBigQueryDriver", self._connection)
        except BaseException as exc:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
            raise

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back on error, optionally reset active session state, and release the lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        try:
            if conn is not None and (exc_type is not None or reset_on_release):
                await _rollback_connection_async(conn)
                if reset_on_release:
                    await _reset_session_async(conn)
            if self._tenant_lease is not None:
                await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb))
        finally:
            self._connection = None
            self._conn_ctx = None
            self._tenant_lease = None
        return None


class BigQuerySyncSessionContext:
    """Synchronous session context manager building a fresh `BigQuerySyncDriver` per session entry."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_driver", "_kwargs", "_statement_config")

    def __init__(
        self,
        config: BigQuerySyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: BigQuerySyncConnectionContext | None = None
        self._connection: SyncBigQueryDriver | None = None
        self._driver: BigQuerySyncDriver | None = None

    def __enter__(self) -> BigQuerySyncDriver:
        """Acquire a leased BigQuery connection and build a fresh `BigQuerySyncDriver`."""
        self._conn_ctx = BigQuerySyncConnectionContext(self._config, *self._args, **self._kwargs)
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
        """Close open streams, roll back uncommitted work, reset session, release lease, and mark closed."""
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


class BigQueryAsyncSessionContext:
    """Asynchronous session context manager building a fresh `BigQueryAsyncDriver` per session entry."""

    __slots__ = ("_args", "_config", "_conn_ctx", "_connection", "_driver", "_kwargs", "_statement_config")

    def __init__(
        self,
        config: BigQueryAsyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._kwargs = kwargs
        self._conn_ctx: BigQueryAsyncConnectionContext | None = None
        self._connection: AsyncBigQueryDriver | None = None
        self._driver: BigQueryAsyncDriver | None = None

    async def __aenter__(self) -> BigQueryAsyncDriver:
        """Asynchronously acquire a leased BigQuery connection and build a fresh `BigQueryAsyncDriver`."""
        self._conn_ctx = BigQueryAsyncConnectionContext(self._config, *self._args, **self._kwargs)
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
        """Close open streams, roll back uncommitted work, reset session, release lease, and mark closed."""
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
