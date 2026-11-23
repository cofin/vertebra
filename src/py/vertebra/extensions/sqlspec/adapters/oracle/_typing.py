"""Oracle SQLSpec adapter TypedDict schemas and session/connection/sessionless context managers."""

from __future__ import annotations

import contextlib
import inspect
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, ClassVar, Literal, NotRequired, TypedDict, cast

from anyio import CancelScope

from vertebra.extensions.sqlspec._driver import (
    _AdapterAsyncConnectionContext,
    _AdapterAsyncSessionContext,
    _AdapterSyncConnectionContext,
    _AdapterSyncSessionContext,
)
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


class OracleSyncConnectionContext(_AdapterSyncConnectionContext):
    """Synchronous connection context manager acquiring, configuring, and releasing Oracle leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "Oracle"
    _run_on_connection_create: ClassVar[bool] = False

    def __init__(self, config: OracleSyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    def _on_acquire(self, conn: Any) -> Any:
        _apply_checkout_sync(conn, self._config, self._kwargs)
        return conn

    def _needs_cleanup(self, exc_type: type[BaseException] | None, reset_on_release: bool) -> bool:
        return (
            exc_type is not None
            or reset_on_release
            or self._config.driver_features.get("session_context_hook") is not None
        )

    def _should_reset(self, reset_on_release: bool) -> bool:
        return reset_on_release or self._config.driver_features.get("session_context_hook") is not None

    def _rollback_conn(self, conn: Any) -> None:
        _rollback_connection_sync(conn)

    def _reset_conn(self, conn: Any) -> None:
        _apply_release_sync(conn, self._config)


class OracleAsyncConnectionContext(_AdapterAsyncConnectionContext):
    """Asynchronous connection context manager acquiring, configuring, and releasing Oracle leases."""

    __slots__ = ()
    _engine_label: ClassVar[str] = "Oracle"
    _run_on_connection_create: ClassVar[bool] = False

    def __init__(self, config: OracleAsyncConfig, *args: Any, **kwargs: Any) -> None:
        super().__init__(config, *args, **kwargs)

    async def _on_acquire(self, conn: Any) -> Any:
        await _apply_checkout_async(conn, self._config, self._kwargs)
        return conn

    def _needs_cleanup(self, exc_type: type[BaseException] | None, reset_on_release: bool) -> bool:
        return (
            exc_type is not None
            or reset_on_release
            or self._config.driver_features.get("session_context_hook") is not None
        )

    def _should_reset(self, reset_on_release: bool) -> bool:
        return reset_on_release or self._config.driver_features.get("session_context_hook") is not None

    async def _rollback_conn(self, conn: Any) -> None:
        await _rollback_connection_async(conn)

    async def _reset_conn(self, conn: Any) -> None:
        await _apply_release_async(conn, self._config)


class OracleSyncSessionContext(_AdapterSyncSessionContext):
    """Synchronous session context manager yielding a fresh `OracleSyncDriver` per entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterSyncConnectionContext]] = OracleSyncConnectionContext
    _engine_label: ClassVar[str] = "Oracle"

    def _driver_in_transaction(self, driver: Any) -> bool:
        return super()._driver_in_transaction(driver) or getattr(driver, "sessionless_transaction_id", None) is not None

    def _needs_tx_rollback(
        self,
        exc_type: type[BaseException] | None,
        primary_exc: BaseException | None,
        pooled_state: bool,
        reset_on_release: bool,
    ) -> bool:
        return (
            super()._needs_tx_rollback(exc_type, primary_exc, pooled_state, reset_on_release)
            or self._config.driver_features.get("session_context_hook") is not None
        )

    def __enter__(self) -> OracleSyncDriver:
        return cast("OracleSyncDriver", super().__enter__())


class OracleAsyncSessionContext(_AdapterAsyncSessionContext):
    """Asynchronous session context manager yielding a fresh `OracleAsyncDriver` per entry."""

    __slots__ = ()
    _connection_context_type: ClassVar[type[_AdapterAsyncConnectionContext]] = OracleAsyncConnectionContext
    _engine_label: ClassVar[str] = "Oracle"

    def _driver_in_transaction(self, driver: Any) -> bool:
        return super()._driver_in_transaction(driver) or getattr(driver, "sessionless_transaction_id", None) is not None

    def _needs_tx_rollback(
        self,
        exc_type: type[BaseException] | None,
        primary_exc: BaseException | None,
        pooled_state: bool,
        reset_on_release: bool,
    ) -> bool:
        return (
            super()._needs_tx_rollback(exc_type, primary_exc, pooled_state, reset_on_release)
            or self._config.driver_features.get("session_context_hook") is not None
        )

    async def __aenter__(self) -> OracleAsyncDriver:
        return cast("OracleAsyncDriver", await super().__aenter__())


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
