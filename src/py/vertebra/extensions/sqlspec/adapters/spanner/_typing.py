"""Cloud Spanner SQLSpec adapter typed schemas and per-session/connection lifecycle contexts."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec._driver import _AdapterAsyncConnectionContext, _AdapterSyncConnectionContext
from vertebra.extensions.sqlspec.adapters.spanner.core import SpannerConnectionShim

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

    from vertebra.db.adapters.spanner import AsyncSpannerDriver, StalenessBound, SyncSpannerDriver
    from vertebra.extensions.sqlspec.adapters.spanner.config import SpannerAsyncConfig, SpannerSyncConfig
    from vertebra.extensions.sqlspec.adapters.spanner.driver import SpannerAsyncDriver, SpannerSyncDriver

__all__ = (
    "SpannerAsyncConnectionContext",
    "SpannerAsyncSessionContext",
    "SpannerConnectionParams",
    "SpannerDriverFeatures",
    "SpannerPoolParams",
    "SpannerSyncConnectionContext",
    "SpannerSyncSessionContext",
)


class SpannerConnectionParams(TypedDict):
    """Typed connection parameters accepted by ``SpannerSyncConfig`` and ``SpannerAsyncConfig``."""

    instance_id: NotRequired[str]
    """Cloud Spanner instance identifier."""
    database_id: NotRequired[str]
    """Cloud Spanner database identifier."""
    project: NotRequired[str]
    """Google Cloud project identifier."""
    project_id: NotRequired[str]
    """Google Cloud project identifier alias."""
    credentials_path: NotRequired[str | None]
    """Path to service account JSON credentials file."""
    emulator_host: NotRequired[str | None]
    """Host and port of local Cloud Spanner emulator."""
    dialect: NotRequired[str]
    """Cloud Spanner SQL dialect (``GOOGLE_STANDARD_SQL`` or ``POSTGRESQL``)."""
    staleness: NotRequired[StalenessBound | Mapping[str, Any] | None]
    """Read timestamp or staleness bound for snapshot reads."""
    dsn: NotRequired[str]
    """Spanner connection DSN string."""
    url: NotRequired[str]
    """Spanner connection URL string."""
    database_role: NotRequired[str | None]
    """Fine-grained Cloud Spanner database role name."""
    endpoint: NotRequired[str | None]
    """Custom Cloud Spanner gRPC API endpoint."""
    admin_endpoint: NotRequired[str | None]
    """Custom Cloud Spanner admin API endpoint."""
    pool_size: NotRequired[int]
    """Target number of pooled sessions or connections."""
    route_to_leader_enabled: NotRequired[bool]
    """Route read-write and read operations toward region leaders."""
    max_commit_delay_ms: NotRequired[int | None]
    """Spanner server-side commit batching delay target in milliseconds."""
    data_boost_enabled: NotRequired[bool]
    """Execute partitioned reads through Spanner Data Boost compute."""
    max_partitions: NotRequired[int]
    """Maximum read partitions requested for partitioned scans."""
    service_account_key: NotRequired[str | dict[str, Any] | None]
    """Service account JSON key path or parsed mapping."""
    quota_project_id: NotRequired[str | None]
    """Billing and quota attribution Google Cloud project ID."""
    impersonate_service_account: NotRequired[str | None]
    """Target service account email for IAM impersonation."""
    use_gcloud_cli: NotRequired[bool]
    """Allow fallback to active ``gcloud`` CLI credentials."""
    scopes: NotRequired[Sequence[str]]
    """OAuth2 scope URIs requested for cloud credentials."""
    access_token: NotRequired[str | None]
    """Static or dynamic bearer access token credential."""
    use_shm_cache: NotRequired[bool]
    """Enable shared-memory Arrow result caching."""
    statement_cache_capacity: NotRequired[int]
    """Maximum cached prepared statements per connection."""
    ca_pem: NotRequired[bytes | None]
    """Custom PEM-encoded CA trust bundle bytes."""
    extra: NotRequired[dict[str, Any]]
    """Additional backend-specific keyword parameters."""


class SpannerPoolParams(SpannerConnectionParams):
    """Typed pool and session-lease parameters extending ``SpannerConnectionParams``."""

    min_sessions: NotRequired[int]
    """Minimum warm Spanner sessions maintained in the pool."""
    max_sessions: NotRequired[int]
    """Maximum Spanner sessions permitted in the pool."""
    share_pool: NotRequired[bool]
    """Share connection leases through the Vertebra SHM registry broker."""
    connection_instance: NotRequired[Any]
    """Pre-constructed driver or pool instance override."""


class SpannerDriverFeatures(TypedDict):
    """Typed feature flags and callbacks supported by Cloud Spanner SQLSpec adapters."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Custom JSON serializer callable for parameter encoding."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Custom JSON deserializer callable for result decoding."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channel storage and dispatch."""
    events_backend: NotRequired[Literal["poll_queue"] | str]
    """Event channel backend implementation identifier."""
    storage_backend: NotRequired[Any]
    """Default object storage backend handle for external exports."""
    statement_cache_size: NotRequired[int]
    """Maximum cached prepared statements per connection."""
    enable_sqlcommenter: NotRequired[bool]
    """Append SQLCommenter observability metadata comments to queries."""
    reset_session_on_release: NotRequired[bool]
    """Reset session state automatically when returning a connection."""
    on_connection_create: NotRequired[Callable[[Any], Any]]
    """Hook invoked with each newly established connection."""
    connection_instance: NotRequired[Any]
    """Pre-constructed driver or pool instance override."""
    default_staleness: NotRequired[StalenessBound | Mapping[str, Any] | None]
    """Default staleness bound applied to read-only transactions."""
    enable_uuid_conversion: NotRequired[bool]
    """Convert UUID strings/bytes into Python ``uuid.UUID`` objects."""
    enable_batch_write_api: NotRequired[bool]
    """Enable Spanner BatchWrite RPC support for mutation batches."""


def _unwrap_sync_connection(conn: Any) -> Any:
    """Unwrap ``SpannerConnectionShim`` instances to their underlying synchronous driver."""
    return conn.driver if isinstance(conn, SpannerConnectionShim) else conn


def _needs_connect(conn: Any) -> bool:
    """Return whether a native Spanner driver requires explicit client initialization."""
    is_connected = getattr(conn, "is_connected", None)
    if is_connected is not None:
        return not bool(is_connected)
    if getattr(conn, "closed", False):
        return True
    return hasattr(conn, "_client") and getattr(conn, "_client", None) is None


def _ensure_connected_sync(conn: Any) -> None:
    """Connect a synchronous native Spanner driver when not yet connected."""
    if conn is None:
        return
    connect_fn = getattr(conn, "connect", None)
    if callable(connect_fn) and _needs_connect(conn):
        connect_fn()


async def _ensure_connected_async(conn: Any) -> None:
    """Connect an asynchronous native Spanner driver when not yet connected."""
    if conn is None:
        return
    connect_fn = getattr(conn, "connect", None)
    if callable(connect_fn) and _needs_connect(conn):
        res = connect_fn()
        if inspect.isawaitable(res):
            await res


def _has_active_tx(conn: Any) -> bool:
    """Return whether a native Spanner driver holds an open interactive transaction."""
    return bool(getattr(conn, "in_transaction", False) or getattr(conn, "active_transaction", None) is not None)


def _rollback_connection_sync(conn: Any, *, force: bool = False) -> None:
    """Roll back an active Spanner write transaction synchronously without raising."""
    if conn is None:
        return
    if not force and not _has_active_tx(conn):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with contextlib.suppress(Exception):
            rollback_fn()


async def _rollback_connection_async(conn: Any, *, force: bool = False) -> None:
    """Roll back an active Spanner write transaction asynchronously under cancellation shielding."""
    if conn is None:
        return
    if not force and not _has_active_tx(conn):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = rollback_fn()
            if inspect.isawaitable(res):
                await res


def _clear_driver_tx_state(driver: Any) -> None:
    """Clear active transaction flags on an exited SQLSpec Spanner driver."""
    if driver is None:
        return
    if hasattr(driver, "_in_transaction"):
        driver._in_transaction = False
    if hasattr(driver, "_session_managed_tx"):
        driver._session_managed_tx = False


def _reset_session_sync(conn: Any) -> None:
    """Reset session state synchronously on pool release."""
    if conn is None:
        return
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with contextlib.suppress(Exception):
            reset_fn()
    if hasattr(conn, "_active_transaction"):
        conn._active_transaction = None
    if hasattr(conn, "_in_transaction"):
        conn._in_transaction = False


async def _reset_session_async(conn: Any) -> None:
    """Reset session state asynchronously on pool release under cancellation shielding."""
    if conn is None:
        return
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if callable(reset_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = reset_fn()
            if inspect.isawaitable(res):
                await res
    if hasattr(conn, "_active_transaction"):
        conn._active_transaction = None
    if hasattr(conn, "_in_transaction"):
        conn._in_transaction = False


class SpannerSyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring native Spanner drivers or tenant leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "Spanner"

    def __init__(self, config: SpannerSyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _unwrap_connection(self, raw_conn: Any) -> Any:
        return _unwrap_sync_connection(raw_conn)

    def _on_acquire(self, conn: Any) -> Any:
        _ensure_connected_sync(conn)
        return super()._on_acquire(conn)

    def _rollback_conn(self, conn: Any) -> None:
        _rollback_connection_sync(conn)

    def _reset_conn(self, conn: Any) -> None:
        _reset_session_sync(conn)

    def __enter__(self) -> SyncSpannerDriver:
        return cast("SyncSpannerDriver", super().__enter__())


class SpannerAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring native Spanner drivers or tenant leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "Spanner"

    def __init__(self, config: SpannerAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _unwrap_connection(self, raw_conn: Any) -> Any:
        adapt_fn = getattr(self._config, "_adapt_async_connection", None)
        return adapt_fn(raw_conn) if callable(adapt_fn) else raw_conn

    async def _on_acquire(self, conn: Any) -> Any:
        await _ensure_connected_async(conn)
        return await super()._on_acquire(conn)

    async def _rollback_conn(self, conn: Any) -> None:
        await _rollback_connection_async(conn)

    async def _reset_conn(self, conn: Any) -> None:
        await _reset_session_async(conn)

    async def __aenter__(self) -> AsyncSpannerDriver:
        return cast("AsyncSpannerDriver", await super().__aenter__())


class SpannerSyncSessionContext:
    """Synchronous session context manager handling Spanner snapshots, transactions, and fresh driver lifecycle."""

    __slots__ = (
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_driver",
        "_kwargs",
        "_read_only",
        "_snapshot_ctx",
        "_staleness",
        "_statement_config",
        "_transaction",
    )

    def __init__(
        self,
        config: SpannerSyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._transaction = bool(transaction)
        self._staleness = staleness
        self._read_only = read_only
        self._kwargs = kwargs
        self._conn_ctx: SpannerSyncConnectionContext | None = None
        self._connection: Any = None
        self._snapshot_ctx: Any = None
        self._driver: SpannerSyncDriver | None = None

    def __enter__(self) -> SpannerSyncDriver:
        """Enter snapshot, write transaction, or autocommit mode and construct a fresh ``SpannerSyncDriver``."""
        self._conn_ctx = SpannerSyncConnectionContext(self._config, *self._args, **self._kwargs)
        self._connection = self._conn_ctx.__enter__()
        try:
            stmt_cfg = self._statement_config if self._statement_config is not None else self._config.statement_config
            features = dict(self._config.driver_features)
            storage = self._config.resolve_storage_backend()
            effective_staleness = self._staleness if self._staleness is not None else features.get("default_staleness")
            read_only_flag = False
            in_tx_flag = False
            active_snapshot: Any = None

            if not self._transaction:
                use_snapshot = (
                    bool(self._read_only) if self._read_only is not None else (effective_staleness is not None)
                )
                if use_snapshot:
                    read_only_flag = True
                    snapshot_fn = getattr(self._connection, "snapshot", None)
                    if callable(snapshot_fn):
                        snap_ctx: Any = cast("Any", snapshot_fn)(effective_staleness, multi_use=True)
                        if hasattr(snap_ctx, "__enter__"):
                            self._snapshot_ctx = snap_ctx
                            active_snapshot = snap_ctx.__enter__()
                        else:
                            active_snapshot = snap_ctx
                else:
                    effective_staleness = None
            else:
                begin_fn = getattr(self._connection, "begin", None)
                if callable(begin_fn):
                    begin_fn()
                in_tx_flag = True
                effective_staleness = None

            driver = self._config.driver_type(
                self._connection,
                statement_config=stmt_cfg,
                driver_features=features,
                observability=self._config.observability_config,
                shared_pool=bool(self._config._pool_is_shared),
                tenant_predicate=self._config.tenant_predicate,
                tenant_id=self._config.tenant_id,
                read_only=read_only_flag,
                in_transaction=in_tx_flag,
                staleness=effective_staleness,
                active_snapshot=active_snapshot,
                storage_backend=storage,
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
        """Close streams, commit or roll back active transactions, clear state, release lease, and mark closed."""
        driver = self._driver
        conn_ctx = self._conn_ctx
        conn = self._connection
        snap_ctx = self._snapshot_ctx
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
                if driver is not None and driver.in_transaction:
                    if self._transaction:
                        try:
                            if exc_type is not None:
                                _rollback_connection_sync(conn, force=True)
                            else:
                                commit_fn = getattr(conn, "commit", None)
                                if callable(commit_fn):
                                    commit_fn()
                        finally:
                            _clear_driver_tx_state(driver)
                    elif exc_type is not None or pooled_state or reset_on_release:
                        try:
                            with contextlib.suppress(Exception):
                                driver.rollback()
                        finally:
                            _clear_driver_tx_state(driver)
            except BaseException as tx_exc:
                if exc_type is None:
                    exc_type, exc_val, exc_tb = type(tx_exc), tx_exc, tx_exc.__traceback__
                    raise
            finally:
                try:
                    if snap_ctx is not None:
                        snap_ctx.__exit__(exc_type, exc_val, exc_tb)
                finally:
                    try:
                        if conn_ctx is not None:
                            conn_ctx.__exit__(exc_type, exc_val, exc_tb)
                    finally:
                        self._snapshot_ctx = None
                        self._connection = None
                        self._conn_ctx = None
                        self._driver = None
                        if driver is not None:
                            mark_closed_fn = getattr(driver, "_mark_closed", None)
                            if callable(mark_closed_fn):
                                mark_closed_fn()
        return None


class SpannerAsyncSessionContext:
    """Asynchronous session context manager handling Spanner snapshots, transactions, and fresh driver lifecycle."""

    __slots__ = (
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_driver",
        "_kwargs",
        "_read_only",
        "_snapshot_ctx",
        "_snapshot_is_async",
        "_staleness",
        "_statement_config",
        "_transaction",
    )

    def __init__(
        self,
        config: SpannerAsyncConfig,
        *args: Any,
        statement_config: StatementConfig | None = None,
        transaction: bool = False,
        staleness: StalenessBound | None = None,
        read_only: bool | None = None,
        **kwargs: Any,
    ) -> None:
        self._config = config
        self._args = args
        self._statement_config = statement_config
        self._transaction = bool(transaction)
        self._staleness = staleness
        self._read_only = read_only
        self._kwargs = kwargs
        self._conn_ctx: SpannerAsyncConnectionContext | None = None
        self._connection: Any = None
        self._snapshot_ctx: Any = None
        self._snapshot_is_async: bool = False
        self._driver: SpannerAsyncDriver | None = None

    async def __aenter__(self) -> SpannerAsyncDriver:
        """Enter snapshot, write transaction, or autocommit mode and construct a fresh ``SpannerAsyncDriver``."""
        self._conn_ctx = SpannerAsyncConnectionContext(self._config, *self._args, **self._kwargs)
        self._connection = await self._conn_ctx.__aenter__()
        try:
            stmt_cfg = self._statement_config if self._statement_config is not None else self._config.statement_config
            features = dict(self._config.driver_features)
            storage = self._config.resolve_storage_backend()
            effective_staleness = self._staleness if self._staleness is not None else features.get("default_staleness")
            read_only_flag = False
            in_tx_flag = False
            active_snapshot: Any = None

            if not self._transaction:
                use_snapshot = (
                    bool(self._read_only) if self._read_only is not None else (effective_staleness is not None)
                )
                if use_snapshot:
                    read_only_flag = True
                    snapshot_fn = getattr(self._connection, "snapshot", None)
                    if callable(snapshot_fn):
                        snap_ctx: Any = cast("Any", snapshot_fn)(effective_staleness, multi_use=True)
                        if hasattr(snap_ctx, "__aenter__"):
                            self._snapshot_ctx = snap_ctx
                            self._snapshot_is_async = True
                            active_snapshot = await snap_ctx.__aenter__()
                        elif hasattr(snap_ctx, "__enter__"):
                            self._snapshot_ctx = snap_ctx
                            self._snapshot_is_async = False
                            active_snapshot = snap_ctx.__enter__()
                        else:
                            active_snapshot = snap_ctx
                else:
                    effective_staleness = None
            else:
                begin_fn = getattr(self._connection, "begin", None)
                if callable(begin_fn):
                    res = begin_fn()
                    if inspect.isawaitable(res):
                        await res
                in_tx_flag = True
                effective_staleness = None

            driver = self._config.driver_type(
                self._connection,
                statement_config=stmt_cfg,
                driver_features=features,
                observability=self._config.observability_config,
                shared_pool=bool(self._config._pool_is_shared),
                tenant_predicate=self._config.tenant_predicate,
                tenant_id=self._config.tenant_id,
                read_only=read_only_flag,
                in_transaction=in_tx_flag,
                staleness=effective_staleness,
                active_snapshot=active_snapshot,
                storage_backend=storage,
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
        """Close streams, commit or roll back active transactions, clear state, release lease, and mark closed."""
        driver = self._driver
        conn_ctx = self._conn_ctx
        conn = self._connection
        snap_ctx = self._snapshot_ctx
        snap_is_async = self._snapshot_is_async
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
                if driver is not None and driver.in_transaction:
                    if self._transaction:
                        try:
                            if exc_type is not None:
                                await _rollback_connection_async(conn, force=True)
                            else:
                                commit_fn = getattr(conn, "commit", None)
                                if callable(commit_fn):
                                    cm_res = commit_fn()
                                    if inspect.isawaitable(cm_res):
                                        await cm_res
                        finally:
                            _clear_driver_tx_state(driver)
                    elif exc_type is not None or pooled_state or reset_on_release:
                        try:
                            with CancelScope(shield=True), contextlib.suppress(Exception):
                                await driver.rollback()
                        finally:
                            _clear_driver_tx_state(driver)
            except BaseException as tx_exc:
                if exc_type is None:
                    exc_type, exc_val, exc_tb = type(tx_exc), tx_exc, tx_exc.__traceback__
                    raise
            finally:
                try:
                    if snap_ctx is not None:
                        if snap_is_async:
                            await snap_ctx.__aexit__(exc_type, exc_val, exc_tb)
                        else:
                            snap_ctx.__exit__(exc_type, exc_val, exc_tb)
                finally:
                    try:
                        if conn_ctx is not None:
                            await conn_ctx.__aexit__(exc_type, exc_val, exc_tb)
                    finally:
                        self._snapshot_ctx = None
                        self._snapshot_is_async = False
                        self._connection = None
                        self._conn_ctx = None
                        self._driver = None
                        if driver is not None:
                            mark_closed_fn = getattr(driver, "_mark_closed", None)
                            if callable(mark_closed_fn):
                                mark_closed_fn()
        return None
