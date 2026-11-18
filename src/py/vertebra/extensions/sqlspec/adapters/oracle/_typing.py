"""Oracle SQLSpec adapter TypedDict schemas and session/connection/sessionless context managers."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec.adapters.oracle.core import (
    OracleSessionContextConfig,
    apply_oracle_session_attrs,
    apply_oracle_transaction_priority,
    build_oracle_checkout_operations,
    build_oracle_release_operations,
    validate_sessionless_transaction_args,
)
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from types import TracebackType

    from sqlspec.core import StatementConfig

    from vertebra.extensions.sqlspec.adapters.oracle.config import OracleAsyncConfig, OracleSyncConfig
    from vertebra.extensions.sqlspec.adapters.oracle.driver import OracleAsyncDriver, OracleSyncDriver

__all__ = (
    "OracleAccessToken",
    "OracleAppContext",
    "OracleAsyncConnectionContext",
    "OracleAsyncSessionContext",
    "OracleAsyncSessionlessTransactionContext",
    "OracleConnectionParams",
    "OracleDriverFeatures",
    "OracleEventsBackend",
    "OraclePoolBoundary",
    "OraclePoolParams",
    "OracleProtocol",
    "OracleServerType",
    "OracleSyncConnectionContext",
    "OracleSyncSessionContext",
    "OracleSyncSessionlessTransactionContext",
    "OracleTransactionPriority",
    "OracleVectorReturnFormat",
)

type OracleAccessToken = str | tuple[str, str] | Callable[..., str | tuple[str, str]]
type OracleAppContext = tuple[str, str, str]
type OracleProtocol = Literal["tcp", "tcps"]
type OracleServerType = Literal["dedicated", "shared", "pooled"]
type OraclePoolBoundary = Literal["statement", "transaction"]
type OracleVectorReturnFormat = Literal["array", "list", "numpy"]
type OracleEventsBackend = Literal["aq", "poll_queue", "txeventq"]
type OracleTransactionPriority = Literal["DEFAULT", "LOW", "MEDIUM", "HIGH"]


class OracleConnectionParams(TypedDict):
    """Typed Oracle connection parameters aligned with `sqlspec.adapters.oracledb.config`."""

    dsn: NotRequired[str]
    """Oracle connection DSN string."""
    url: NotRequired[str]
    """Oracle connection URL string."""
    connection_string: NotRequired[str]
    """Alternative Oracle connection string alias."""
    pool_alias: NotRequired[str]
    """Named Oracle connection pool alias."""
    user: NotRequired[str]
    """Authenticating database username."""
    proxy_user: NotRequired[str]
    """Oracle proxy session target username."""
    password: NotRequired[str]
    """Password credential for database authentication."""
    newpassword: NotRequired[str]
    """New password applied during password rotation."""
    wallet_password: NotRequired[str]
    """Oracle wallet decryption password."""
    access_token: NotRequired[OracleAccessToken]
    """Static or dynamic bearer access token credential."""
    host: NotRequired[str]
    """Database server hostname or address sequence."""
    port: NotRequired[int]
    """Database server TCP port or port sequence."""
    protocol: NotRequired[OracleProtocol]
    """Oracle Net transport protocol (``tcp`` or ``tcps``)."""
    https_proxy: NotRequired[str]
    """HTTPS proxy hostname for Oracle TCPS tunneling."""
    https_proxy_port: NotRequired[int]
    """HTTPS proxy TCP port."""
    service_name: NotRequired[str]
    """Oracle database service name."""
    instance_name: NotRequired[str]
    """Target Oracle RAC instance name."""
    sid: NotRequired[str]
    """Legacy Oracle System Identifier (SID)."""
    server_type: NotRequired[OracleServerType]
    """Oracle server handler type (``dedicated``, ``shared``, or ``pooled``)."""
    cclass: NotRequired[str]
    """Oracle DRCP connection class identifier."""
    purity: NotRequired[Any]
    """Oracle DRCP session purity mode."""
    expire_time: NotRequired[int]
    """SQLNET idle probe interval in minutes."""
    externalauth: NotRequired[bool]
    """Enable external OS/wallet/Kerberos Oracle authentication."""
    mode: NotRequired[Any]
    """Oracle privilege mode (SYSDBA, SYSOPER, etc.)."""
    wallet_location: NotRequired[str]
    """Filesystem directory containing Oracle ewallet/cwallet."""
    wallet_path: NotRequired[str]
    """Alias for Oracle wallet directory path."""
    config_dir: NotRequired[str]
    """Directory containing ``tnsnames.ora`` and ``sqlnet.ora``."""
    tcp_connect_timeout: NotRequired[float]
    """Oracle TCP connect timeout in seconds."""
    retry_count: NotRequired[int]
    """Oracle transparent connect retry count."""
    retry_delay: NotRequired[int]
    """Delay in seconds between connect retries."""
    ssl_server_dn_match: NotRequired[bool]
    """Verify server certificate distinguished name against descriptor."""
    ssl_server_cert_dn: NotRequired[str]
    """Expected server certificate distinguished name."""
    events: NotRequired[bool]
    """Enable Oracle AQ/CQN notification events on pool connections."""
    disable_oob: NotRequired[bool]
    """Disable Oracle out-of-band break packets."""
    stmtcachesize: NotRequired[int]
    """Oracle client statement cache size."""
    edition: NotRequired[str]
    """Oracle Edition-Based Redefinition edition name."""
    tag: NotRequired[str]
    """DRCP/pool connection session tag."""
    matchanytag: NotRequired[bool]
    """Allow pool checkout of connections with any matching tag."""
    appcontext: NotRequired[Sequence[OracleAppContext] | Mapping[str, Mapping[str, str]]]
    """Oracle application context triples applied at checkout."""
    sharding_key: NotRequired[Sequence[Any]]
    """Oracle Sharding key tuple for shard routing."""
    super_sharding_key: NotRequired[Sequence[Any]]
    """Oracle super-sharding key tuple for composite sharding."""
    transaction_priority: NotRequired[OracleTransactionPriority]
    """Oracle 23ai/26ai transaction priority level."""
    shardingkey: NotRequired[list[Any]]
    """Alias for ``sharding_key``."""
    supershardingkey: NotRequired[list[Any]]
    """Alias for ``super_sharding_key``."""
    debug_jdwp: NotRequired[str]
    """Oracle PL/SQL JDWP debugger address."""
    connection_id_prefix: NotRequired[str]
    """Prefix attached to Oracle connection audit IDs."""
    ssl_context: NotRequired[Any]
    """Pre-configured Python SSLContext instance."""
    sdu: NotRequired[int]
    """Oracle Net Session Data Unit buffer size in bytes."""
    pool_boundary: NotRequired[OraclePoolBoundary]
    """DRCP session state boundary (``statement`` or ``transaction``)."""
    use_tcp_fast_open: NotRequired[bool]
    """Enable TCP Fast Open sockets where supported."""
    ssl_version: NotRequired[Any]
    """Minimum allowed TLS protocol version."""
    program: NotRequired[str]
    """Client program name recorded in ``V$SESSION``."""
    machine: NotRequired[str]
    """Client hostname recorded in ``V$SESSION``."""
    terminal: NotRequired[str]
    """Client terminal identifier recorded in ``V$SESSION``."""
    osuser: NotRequired[str]
    """Client OS user recorded in ``V$SESSION``."""
    driver_name: NotRequired[str]
    """Driver identifier string reported to Oracle server."""
    use_sni: NotRequired[bool]
    """Send TLS Server Name Indication during TCPS handshake."""
    thick_mode_dsn_passthrough: NotRequired[bool]
    """Forward DSN verbatim to Oracle Instant Client thick mode."""
    extra_auth_params: NotRequired[dict[str, Any]]
    """Extra provider parameters for IAM/token Oracle plugins."""
    pool_name: NotRequired[str]
    """Unique pool name registered in Oracle driver."""
    on_connect_callback: NotRequired[Callable[..., Any]]
    """Callback invoked when Oracle establishes a physical session."""
    handle: NotRequired[int]
    """Pre-allocated external connection handle."""
    extra: NotRequired[dict[str, Any]]
    """Additional backend-specific keyword parameters."""
    topology: NotRequired[Any]
    """Oracle Global Data Services or RAC topology hints."""
    wallet_zip: NotRequired[str | Path | bytes]
    """Path or raw bytes of compressed Oracle wallet archive."""
    tns_alias: NotRequired[str]
    """TNS alias entry resolved via ``tnsnames.ora``."""


class OraclePoolParams(OracleConnectionParams):
    """Typed Oracle connection and pool parameters aligned with `sqlspec.adapters.oracledb.config`."""

    thick_mode: NotRequired[bool]
    """Enable Oracle thick mode client initialization."""
    lib_dir: NotRequired[str]
    """Directory containing Oracle Instant Client shared libraries."""
    pool_class: NotRequired[type[Any]]
    """Pool class override used by Oracle config."""
    params: NotRequired[Any]
    """Pre-built Oracle pool parameter object."""
    min: NotRequired[int]
    """Minimum warm Oracle pool sessions."""
    max: NotRequired[int]
    """Maximum Oracle pool sessions."""
    min_size: NotRequired[int]
    """Minimum warm connections retained in the pool."""
    max_size: NotRequired[int]
    """Maximum concurrent connections allowed in the pool."""
    increment: NotRequired[int]
    """Session growth increment when scaling Oracle pools."""
    connectiontype: NotRequired[type[Any]]
    """Custom Connection subclass factory for Oracle sessions."""
    getmode: NotRequired[Any]
    """Oracle pool borrow wait mode."""
    homogeneous: NotRequired[bool]
    """Require identical credentials across all pooled Oracle sessions."""
    timeout: NotRequired[int]
    """Connection or lock acquisition timeout in seconds."""
    wait_timeout: NotRequired[int]
    """Maximum wait time in milliseconds to borrow a pooled session."""
    max_lifetime_session: NotRequired[int]
    """Maximum pooled Oracle session lifetime in seconds."""
    session_callback: NotRequired[Callable[..., Any] | str]
    """Session initialization callback name or callable."""
    max_sessions_per_shard: NotRequired[int]
    """Maximum sessions per Oracle shard pool."""
    soda_metadata_cache: NotRequired[bool]
    """Enable Oracle SODA collection metadata caching."""
    ping_interval: NotRequired[int]
    """Idle time in seconds before liveness ping on checkout."""
    ping_timeout: NotRequired[int]
    """Timeout in milliseconds for Oracle checkout liveness ping."""


class OracleDriverFeatures(TypedDict):
    """Typed feature flags controlling Oracle codecs, hooks, session reset, and 26ai workflows."""

    json_serializer: NotRequired[Callable[[Any], str]]
    """Custom JSON serializer callable for parameter encoding."""
    json_deserializer: NotRequired[Callable[[str], Any]]
    """Custom JSON deserializer callable for result decoding."""
    retry_on_standby_lag: NotRequired[bool]
    """Retry read queries briefly on Oracle standby replication lag."""
    plugins: NotRequired[Sequence[str]]
    """Oracle authentication plugin module names."""
    params_hooks: NotRequired[Sequence[Callable[[Any], None]]]
    """Parameter transformation hooks invoked before connect."""
    enable_auto_state_tagging: NotRequired[bool]
    """Automatically tag session state changes for DRCP affinity."""
    enable_numpy_vectors: NotRequired[bool]
    """Convert Oracle ``VECTOR`` results into NumPy arrays."""
    enable_lowercase_column_names: NotRequired[bool]
    """Lowercase returned Oracle column identifiers automatically."""
    enable_uuid_binary: NotRequired[bool]
    """Bind and decode ``RAW(16)`` columns as Python UUIDs."""
    vector_return_format: NotRequired[OracleVectorReturnFormat]
    """Return format for Oracle vector columns."""
    oracle_varchar2_byte_limit: NotRequired[int]
    """Maximum VARCHAR2 byte threshold before LOB promotion."""
    oracle_raw_byte_limit: NotRequired[int]
    """Maximum RAW byte threshold before BLOB promotion."""
    arraysize: NotRequired[int]
    """Oracle fetch row batch size per round-trip."""
    prefetchrows: NotRequired[int]
    """Oracle driver prefetch row buffer count."""
    fetch_lobs: NotRequired[bool]
    """Materialize LOB locators automatically instead of streaming."""
    fetch_decimals: NotRequired[bool]
    """Return NUMBER columns as ``decimal.Decimal`` instances."""
    on_connection_create: NotRequired[Callable[..., Any]]
    """Hook invoked with each newly established connection."""
    enable_events: NotRequired[bool]
    """Enable SQLSpec event channel storage and dispatch."""
    events_backend: NotRequired[OracleEventsBackend]
    """Event channel backend implementation identifier."""
    enable_direct_path_load: NotRequired[bool]
    """Enable high-throughput direct-path bulk ingestion."""
    tenant_strategy: NotRequired[Any]
    """Multi-tenant schema or row isolation strategy."""
    session_context_hook: NotRequired[OracleSessionContextConfig | Mapping[str, Any] | Callable[[Any], Any]]
    """Session context configuration or callback applied at checkout."""
    reset_session_on_release: NotRequired[bool]
    """Reset session state automatically when returning a connection."""
    sharding_key_variable: NotRequired[str]
    """Session variable name supplying Oracle sharding key."""
    super_sharding_key_variable: NotRequired[str]
    """Session variable name supplying Oracle super-sharding key."""
    shard_resolver: NotRequired[Any]
    """Callable deriving Oracle sharding keys per operation."""
    use_tenant_id_as_sharding_key: NotRequired[bool]
    """Derive Oracle sharding key from active tenant ID."""
    enable_otel_trace_context: NotRequired[bool]
    """Propagate W3C traceparent via Oracle client identifier/action."""
    enable_session_tracing: NotRequired[bool]
    """Emit Oracle SQL trace tags for active sessions."""
    dbms_cloud_credential: NotRequired[str | None]
    """Oracle Cloud DBMS_CLOUD credential object name."""
    stack_native_disabled: NotRequired[bool]
    """Disable native Rust transport optimizations for compatibility fallback."""


def _exec_sync(conn: Any, sql: str, params: dict[str, str] | None) -> None:
    """Execute a synchronous checkout or release SQL block against a native connection."""
    execute_fn = getattr(conn, "execute", None)
    if not callable(execute_fn):
        return
    if params is not None:
        execute_fn(sql, params)
    else:
        execute_fn(sql)


async def _exec_async(conn: Any, sql: str, params: dict[str, str] | None) -> None:
    """Execute an asynchronous checkout or release SQL block against a native connection."""
    execute_fn = getattr(conn, "execute", None)
    if not callable(execute_fn):
        return
    res = execute_fn(sql, params) if params is not None else execute_fn(sql)
    if inspect.isawaitable(res):
        await res


def _rollback_connection_sync(conn: Any) -> None:
    """Roll back any active synchronous transaction on a native Oracle connection."""
    if not getattr(conn, "in_transaction", False):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with contextlib.suppress(Exception):
            rollback_fn()


async def _rollback_connection_async(conn: Any) -> None:
    """Roll back any active asynchronous transaction on a native Oracle connection."""
    if not getattr(conn, "in_transaction", False):
        return
    rollback_fn = getattr(conn, "rollback", None)
    if callable(rollback_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = rollback_fn()
            if inspect.isawaitable(res):
                await res


def _apply_checkout_sync(conn: Any, config: OracleSyncConfig, kwargs: Mapping[str, Any]) -> None:
    """Apply `session_context_hook` and configured transaction priority on synchronous checkout."""
    priority = kwargs.get("transaction_priority")
    if priority is None:
        priority = config.connection_config.get("transaction_priority")
    if priority is not None:
        apply_oracle_transaction_priority(conn, priority)
    hook = config.driver_features.get("session_context_hook")
    if hook is None:
        return
    if callable(hook):
        hook(conn)
        return
    attrs, sql_ops = build_oracle_checkout_operations(hook)
    apply_oracle_session_attrs(conn, attrs)
    for sql, params in sql_ops:
        _exec_sync(conn, sql, params)


async def _apply_checkout_async(conn: Any, config: OracleAsyncConfig, kwargs: Mapping[str, Any]) -> None:
    """Apply `session_context_hook` and configured transaction priority on asynchronous checkout."""
    priority = kwargs.get("transaction_priority")
    if priority is None:
        priority = config.connection_config.get("transaction_priority")
    if priority is not None:
        apply_oracle_transaction_priority(conn, priority)
    hook = config.driver_features.get("session_context_hook")
    if hook is None:
        return
    if callable(hook):
        res = hook(conn)
        if inspect.isawaitable(res):
            await res
        return
    attrs, sql_ops = build_oracle_checkout_operations(hook)
    apply_oracle_session_attrs(conn, attrs)
    for sql, params in sql_ops:
        await _exec_async(conn, sql, params)


def _apply_release_sync(conn: Any, config: OracleSyncConfig) -> None:
    """Clear Oracle session context and run `DBMS_SESSION.RESET_PACKAGE` on synchronous release."""
    hook = config.driver_features.get("session_context_hook")
    reset_on_release = bool(config.driver_features.get("reset_session_on_release"))
    if not reset_on_release and hook is None:
        return
    clear_attr, sql_ops = build_oracle_release_operations(hook, reset_session_on_release=reset_on_release)
    if clear_attr:
        with contextlib.suppress(AttributeError):
            conn.client_identifier = None
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if reset_on_release and callable(reset_fn):
        with contextlib.suppress(Exception):
            reset_fn()
    for sql, params in sql_ops:
        with contextlib.suppress(Exception):
            _exec_sync(conn, sql, params)


async def _apply_release_async(conn: Any, config: OracleAsyncConfig) -> None:
    """Clear Oracle session context and run `DBMS_SESSION.RESET_PACKAGE` on asynchronous release."""
    hook = config.driver_features.get("session_context_hook")
    reset_on_release = bool(config.driver_features.get("reset_session_on_release"))
    if not reset_on_release and hook is None:
        return
    clear_attr, sql_ops = build_oracle_release_operations(hook, reset_session_on_release=reset_on_release)
    if clear_attr:
        with contextlib.suppress(AttributeError):
            conn.client_identifier = None
    reset_fn = getattr(conn, "reset_session", None) or getattr(conn, "reset", None)
    if reset_on_release and callable(reset_fn):
        with CancelScope(shield=True), contextlib.suppress(Exception):
            res = reset_fn()
            if inspect.isawaitable(res):
                await res
    for sql, params in sql_ops:
        with CancelScope(shield=True), contextlib.suppress(Exception):
            await _exec_async(conn, sql, params)


class OracleSyncConnectionContext:
    """Synchronous connection context manager acquiring, configuring, and releasing Oracle leases."""

    __slots__ = (
        "_acquired_pool",
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_kwargs",
        "_tenant_lease",
    )

    def __init__(self, config: OracleSyncConfig, *args: Any, **kwargs: Any) -> None:
        """Store configuration and forwarded connection arguments."""
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._acquired_pool: Any = None
        self._connection: Any = None
        self._tenant_lease: Any = None

    def __enter__(self) -> Any:
        """Acquire a native Oracle connection or tenant lease and apply checkout session hooks."""
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = self._tenant_lease.acquire()
        else:
            pool = self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            lease_fn = getattr(pool, "lease", None)
            acquire_fn = getattr(pool, "acquire", None)
            release_fn = getattr(pool, "release", None)
            if callable(get_conn):
                self._conn_ctx = get_conn()
                self._connection = self._conn_ctx.__enter__()
            elif callable(lease_fn):
                self._conn_ctx = lease_fn()
                self._connection = self._conn_ctx.__enter__()
            elif callable(acquire_fn) and callable(release_fn):
                self._acquired_pool = pool
                self._connection = acquire_fn()
            else:
                self._connection = pool
        try:
            _apply_checkout_sync(self._connection, self._config, self._kwargs)
        except BaseException as exc:
            self.__exit__(type(exc), exc, exc.__traceback__)
            raise
        return self._connection

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back open transactions, clear/reset session state, and release the Oracle lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        hook = self._config.driver_features.get("session_context_hook")
        try:
            if conn is not None and (exc_type is not None or reset_on_release or hook is not None):
                _rollback_connection_sync(conn)
                _apply_release_sync(conn, self._config)
            if self._tenant_lease is not None:
                self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", self._conn_ctx.__exit__(exc_type, exc_val, exc_tb))
            elif self._acquired_pool is not None and conn is not None:
                self._acquired_pool.release(conn)
        finally:
            self._connection = None
            self._conn_ctx = None
            self._acquired_pool = None
            self._tenant_lease = None
        return None


class OracleAsyncConnectionContext:
    """Asynchronous connection context manager acquiring, configuring, and releasing Oracle leases."""

    __slots__ = (
        "_acquired_pool",
        "_args",
        "_config",
        "_conn_ctx",
        "_connection",
        "_kwargs",
        "_tenant_lease",
    )

    def __init__(self, config: OracleAsyncConfig, *args: Any, **kwargs: Any) -> None:
        """Store configuration and forwarded connection arguments."""
        self._config = config
        self._args = args
        self._kwargs = kwargs
        self._conn_ctx: Any = None
        self._acquired_pool: Any = None
        self._connection: Any = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> Any:
        """Acquire an asynchronous native Oracle connection or tenant lease and apply checkout hooks."""
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = await self._tenant_lease.acquire()
        else:
            pool = await self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            lease_fn = getattr(pool, "lease", None)
            acquire_fn = getattr(pool, "acquire", None)
            release_fn = getattr(pool, "release", None)
            if callable(get_conn):
                self._conn_ctx = get_conn()
                self._connection = await self._conn_ctx.__aenter__()
            elif callable(lease_fn):
                self._conn_ctx = lease_fn()
                self._connection = await self._conn_ctx.__aenter__()
            elif callable(acquire_fn) and callable(release_fn):
                self._acquired_pool = pool
                conn_res = acquire_fn()
                self._connection = await conn_res if inspect.isawaitable(conn_res) else conn_res
            else:
                self._connection = pool
        try:
            await _apply_checkout_async(self._connection, self._config, self._kwargs)
        except BaseException as exc:
            await self.__aexit__(type(exc), exc, exc.__traceback__)
            raise
        return self._connection

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool | None:
        """Roll back open transactions, clear/reset session state, and release the Oracle lease."""
        conn = self._connection
        reset_on_release = bool(self._config.driver_features.get("reset_session_on_release"))
        hook = self._config.driver_features.get("session_context_hook")
        try:
            if conn is not None and (exc_type is not None or reset_on_release or hook is not None):
                await _rollback_connection_async(conn)
                await _apply_release_async(conn, self._config)
            if self._tenant_lease is not None:
                await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
            elif self._conn_ctx is not None:
                return cast("bool | None", await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb))
            elif self._acquired_pool is not None and conn is not None:
                rel_res = self._acquired_pool.release(conn)
                if inspect.isawaitable(rel_res):
                    await rel_res
        finally:
            self._connection = None
            self._conn_ctx = None
            self._acquired_pool = None
            self._tenant_lease = None
        return None


class OracleSyncSessionContext:
    """Synchronous session context manager yielding a fresh `OracleSyncDriver` per entry."""

    __slots__ = (
        "_acquired_pool",
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
        config: OracleSyncConfig,
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
        self._acquired_pool: Any = None
        self._connection: Any = None
        self._driver: OracleSyncDriver | None = None
        self._tenant_lease: Any = None

    def __enter__(self) -> OracleSyncDriver:
        """Acquire a lease, apply checkout hooks, and instantiate a fresh `OracleSyncDriver`."""
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = self._tenant_lease.acquire()
        else:
            pool = self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            lease_fn = getattr(pool, "lease", None)
            acquire_fn = getattr(pool, "acquire", None)
            release_fn = getattr(pool, "release", None)
            if callable(get_conn):
                self._conn_ctx = get_conn()
                self._connection = self._conn_ctx.__enter__()
            elif callable(lease_fn):
                self._conn_ctx = lease_fn()
                self._connection = self._conn_ctx.__enter__()
            elif callable(acquire_fn) and callable(release_fn):
                self._acquired_pool = pool
                self._connection = acquire_fn()
            else:
                self._connection = pool
        try:
            _apply_checkout_sync(self._connection, self._config, self._kwargs)
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
                hook = self._config.driver_features.get("session_context_hook")
                pooled_state = bool(
                    self._config.share_pool
                    or self._tenant_lease is not None
                    or self._conn_ctx is not None
                    or self._acquired_pool is not None
                )
                if driver is not None and (
                    exc_type is not None or pooled_state or reset_on_release or hook is not None
                ):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx or getattr(driver, "sessionless_transaction_id", None) is not None:
                        with contextlib.suppress(Exception):
                            driver.rollback()
                if conn is not None and (reset_on_release or hook is not None):
                    _apply_release_sync(conn, self._config)
                if self._tenant_lease is not None:
                    self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
                elif self._conn_ctx is not None:
                    self._conn_ctx.__exit__(exc_type, exc_val, exc_tb)
                elif self._acquired_pool is not None and conn is not None:
                    self._acquired_pool.release(conn)
            finally:
                self._connection = None
                self._conn_ctx = None
                self._acquired_pool = None
                self._tenant_lease = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None


class OracleAsyncSessionContext:
    """Asynchronous session context manager yielding a fresh `OracleAsyncDriver` per entry."""

    __slots__ = (
        "_acquired_pool",
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
        config: OracleAsyncConfig,
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
        self._acquired_pool: Any = None
        self._connection: Any = None
        self._driver: OracleAsyncDriver | None = None
        self._tenant_lease: Any = None

    async def __aenter__(self) -> OracleAsyncDriver:
        """Acquire a lease, apply checkout hooks, and instantiate a fresh `OracleAsyncDriver`."""
        self._tenant_lease = self._config._create_tenant_lease()
        if self._tenant_lease is not None:
            self._connection = await self._tenant_lease.acquire()
        else:
            pool = await self._config.provide_pool()
            get_conn = getattr(pool, "get_connection", None)
            lease_fn = getattr(pool, "lease", None)
            acquire_fn = getattr(pool, "acquire", None)
            release_fn = getattr(pool, "release", None)
            if callable(get_conn):
                self._conn_ctx = get_conn()
                self._connection = await self._conn_ctx.__aenter__()
            elif callable(lease_fn):
                self._conn_ctx = lease_fn()
                self._connection = await self._conn_ctx.__aenter__()
            elif callable(acquire_fn) and callable(release_fn):
                self._acquired_pool = pool
                conn_res = acquire_fn()
                self._connection = await conn_res if inspect.isawaitable(conn_res) else conn_res
            else:
                self._connection = pool
        try:
            await _apply_checkout_async(self._connection, self._config, self._kwargs)
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
        conn = self._connection
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
                hook = self._config.driver_features.get("session_context_hook")
                pooled_state = bool(
                    self._config.share_pool
                    or self._tenant_lease is not None
                    or self._conn_ctx is not None
                    or self._acquired_pool is not None
                )
                if driver is not None and (
                    exc_type is not None or pooled_state or reset_on_release or hook is not None
                ):
                    in_tx_fn = getattr(driver, "_connection_in_transaction", None)
                    in_tx = bool(in_tx_fn()) if callable(in_tx_fn) else bool(getattr(driver, "in_transaction", False))
                    if in_tx or getattr(driver, "sessionless_transaction_id", None) is not None:
                        with CancelScope(shield=True), contextlib.suppress(Exception):
                            res = driver.rollback()
                            if inspect.isawaitable(res):
                                await res
                if conn is not None and (reset_on_release or hook is not None):
                    await _apply_release_async(conn, self._config)
                if self._tenant_lease is not None:
                    await self._tenant_lease.release(exc_type=exc_type, exc_val=exc_val, exc_tb=exc_tb)
                elif self._conn_ctx is not None:
                    await self._conn_ctx.__aexit__(exc_type, exc_val, exc_tb)
                elif self._acquired_pool is not None and conn is not None:
                    rel_res = self._acquired_pool.release(conn)
                    if inspect.isawaitable(rel_res):
                        await rel_res
            finally:
                self._connection = None
                self._conn_ctx = None
                self._acquired_pool = None
                self._tenant_lease = None
                self._driver = None
                if driver is not None:
                    mark_closed_fn = getattr(driver, "_mark_closed", None)
                    if callable(mark_closed_fn):
                        mark_closed_fn()
        return None


class OracleSyncSessionlessTransactionContext:
    """Begin or resume an Oracle 26ai sessionless transaction and suspend on clean exit."""

    __slots__ = ("_defer_round_trip", "_driver", "_resume", "_rollback_on_error", "_timeout", "_transaction_id")

    def __init__(
        self,
        driver: OracleSyncDriver,
        *,
        transaction_id: str | bytes | None = None,
        timeout: int = 60,
        defer_round_trip: bool = True,
        resume: bool = False,
        rollback_on_error: bool = True,
    ) -> None:
        """Validate sessionless transaction parameters and bind target driver."""
        if not isinstance(resume, bool) or not isinstance(rollback_on_error, bool):
            msg = "resume and rollback_on_error must be bool"
            raise ImproperConfigurationError(msg)
        self._transaction_id = validate_sessionless_transaction_args(
            transaction_id, timeout=timeout, is_resume=resume, defer_round_trip=defer_round_trip
        )
        self._driver = driver
        self._timeout = timeout
        self._defer_round_trip = defer_round_trip
        self._resume = resume
        self._rollback_on_error = rollback_on_error

    def __enter__(self) -> bytes:
        """Begin a new sessionless transaction or resume a suspended one."""
        if self._resume:
            return self._driver.resume_sessionless_transaction(
                cast("str | bytes", self._transaction_id),
                timeout=self._timeout,
                defer_round_trip=self._defer_round_trip,
            )
        return self._driver.begin_sessionless_transaction(
            self._transaction_id,
            timeout=self._timeout,
            defer_round_trip=self._defer_round_trip,
        )

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> Literal[False]:
        """Suspend on clean exit or roll back / suspend when exiting with an error."""
        _ = exc_tb
        if self._driver.sessionless_transaction_id is None:
            return False
        if exc_type is None:
            self._driver.suspend_sessionless_transaction()
        else:
            try:
                if self._rollback_on_error:
                    self._driver.rollback()
                else:
                    self._driver.suspend_sessionless_transaction()
            except BaseException as cleanup_error:
                if exc_val is not None:
                    exc_val.add_note(f"Oracle sessionless transaction cleanup failed: {cleanup_error}")
        return False


class OracleAsyncSessionlessTransactionContext:
    """Asynchronously begin or resume an Oracle 26ai sessionless transaction and suspend on clean exit."""

    __slots__ = ("_defer_round_trip", "_driver", "_resume", "_rollback_on_error", "_timeout", "_transaction_id")

    def __init__(
        self,
        driver: OracleAsyncDriver,
        *,
        transaction_id: str | bytes | None = None,
        timeout: int = 60,
        defer_round_trip: bool = True,
        resume: bool = False,
        rollback_on_error: bool = True,
    ) -> None:
        """Validate sessionless transaction parameters and bind target async driver."""
        if not isinstance(resume, bool) or not isinstance(rollback_on_error, bool):
            msg = "resume and rollback_on_error must be bool"
            raise ImproperConfigurationError(msg)
        self._transaction_id = validate_sessionless_transaction_args(
            transaction_id, timeout=timeout, is_resume=resume, defer_round_trip=defer_round_trip
        )
        self._driver = driver
        self._timeout = timeout
        self._defer_round_trip = defer_round_trip
        self._resume = resume
        self._rollback_on_error = rollback_on_error

    async def __aenter__(self) -> bytes:
        """Asynchronously begin a new sessionless transaction or resume a suspended one."""
        if self._resume:
            return await self._driver.resume_sessionless_transaction(
                cast("str | bytes", self._transaction_id),
                timeout=self._timeout,
                defer_round_trip=self._defer_round_trip,
            )
        return await self._driver.begin_sessionless_transaction(
            self._transaction_id,
            timeout=self._timeout,
            defer_round_trip=self._defer_round_trip,
        )

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> Literal[False]:
        """Suspend on clean exit or roll back / suspend when exiting with an error."""
        _ = exc_tb
        if self._driver.sessionless_transaction_id is None:
            return False
        if exc_type is None:
            await self._driver.suspend_sessionless_transaction()
        else:
            try:
                if self._rollback_on_error:
                    await self._driver.rollback()
                else:
                    await self._driver.suspend_sessionless_transaction()
            except BaseException as cleanup_error:
                if exc_val is not None:
                    exc_val.add_note(f"Oracle sessionless transaction cleanup failed: {cleanup_error}")
        return False
