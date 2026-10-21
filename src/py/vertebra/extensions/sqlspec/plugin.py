"""Application plugin and PEP 695 generic configurations for SQLSpec integration."""

from __future__ import annotations

import contextlib
import importlib
import inspect
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, cast, overload, runtime_checkable

from vertebra.extensions.alchemy.broker import BrokerPlugin, BrokerTransport, ChannelMessage
from vertebra.extensions.alchemy.plugin import synthesize_skip_validation_annotation
from vertebra.extensions.sqlspec.storage import SQLSpecStorageBridge
from vertebra.storage import ObjectStore as BaseObjectStore

__all__ = (
    "SQLSPEC_PLUGIN_PRIORITY",
    "SQLSPEC_STATE_KEY",
    "AsyncDatabaseConfig",
    "AsyncDriverAdapterBase",
    "BrokerPlugin",
    "ChannelMessage",
    "DatabaseConfigProtocol",
    "ProxyAsyncConfig",
    "ProxySyncConfig",
    "SQLSpec",
    "SQLSpecConfig",
    "SQLSpecPlugin",
    "SQLSpecState",
    "SQLSpecStorePlugin",
    "SyncDatabaseConfig",
    "SyncDriverAdapterBase",
    "VertebraProxyAsyncConfig",
    "VertebraProxySyncConfig",
    "VertebraSQLSpecPlugin",
    "wrap_config",
)

SQLSPEC_PLUGIN_PRIORITY: int = -5800
SQLSPEC_STATE_KEY: str = "sqlspec"


@runtime_checkable
class SyncDriverAdapterBase(Protocol):
    """Protocol representing a synchronous SQLSpec driver adapter."""

    def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a SQL statement synchronously."""
        ...


@runtime_checkable
class AsyncDriverAdapterBase(Protocol):
    """Protocol representing an asynchronous SQLSpec driver adapter."""

    async def execute(self, statement: str, *args: Any, **kwargs: Any) -> Any:
        """Execute a SQL statement asynchronously."""
        ...


@runtime_checkable
class DatabaseConfigProtocol[
    ConnectionT,
    PoolT,
    DriverT,
](Protocol):
    """Structural protocol satisfied by all 18 SQLSpec database configuration classes."""

    is_async: bool
    supports_connection_pooling: bool

    def provide_session(self) -> DriverT:
        """Provide a typed driver session."""
        ...

    def provide_connection(self) -> ConnectionT:
        """Provide a raw database connection."""
        ...

    def create_pool(self) -> PoolT:
        """Create or return the connection pool."""
        ...


class SyncDatabaseConfig[
    ConnectionT,
    PoolT,
    DriverT: SyncDriverAdapterBase,
]:
    """Base synchronous SQLSpec database configuration."""

    is_async: bool = False
    supports_connection_pooling: bool = True
    bind_key: str = "default"
    session_key: str = "db_session"
    connection_key: str = "db_connection"
    pool_config: dict[str, Any]
    driver_features: dict[str, Any]

    def __init__(
        self,
        *,
        dsn: str = "sqlite:///:memory:",
        bind_key: str = "default",
        session_key: str = "db_session",
        connection_key: str = "db_connection",
        supports_connection_pooling: bool = True,
        pool_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        driver_instance: DriverT | None = None,
        connection_instance: ConnectionT | None = None,
        pool_instance: PoolT | None = None,
    ) -> None:
        self.dsn = dsn
        self.bind_key = bind_key
        self.session_key = session_key
        self.connection_key = connection_key
        self.supports_connection_pooling = supports_connection_pooling
        self.pool_config = dict(pool_config or {})
        self.driver_features = dict(driver_features or {})
        self._driver_instance = driver_instance
        self._connection_instance = connection_instance
        self._pool_instance = pool_instance
        _inject_sqlspec_pool_defaults(self)

    def provide_session(self) -> DriverT:
        """Return the configured synchronous driver session."""
        return cast("DriverT", self._driver_instance)

    def provide_connection(self) -> ConnectionT:
        """Return the configured synchronous connection."""
        return cast("ConnectionT", self._connection_instance)

    def create_pool(self) -> PoolT:
        """Return the configured synchronous connection pool."""
        return cast("PoolT", self._pool_instance)


class AsyncDatabaseConfig[
    ConnectionT,
    PoolT,
    DriverT: AsyncDriverAdapterBase,
]:
    """Base asynchronous SQLSpec database configuration."""

    is_async: bool = True
    supports_connection_pooling: bool = True
    bind_key: str = "default"
    session_key: str = "db_session"
    connection_key: str = "db_connection"
    pool_config: dict[str, Any]
    driver_features: dict[str, Any]

    def __init__(
        self,
        *,
        dsn: str = "sqlite+aiosqlite:///:memory:",
        bind_key: str = "default",
        session_key: str = "db_session",
        connection_key: str = "db_connection",
        supports_connection_pooling: bool = True,
        pool_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        driver_instance: DriverT | None = None,
        connection_instance: ConnectionT | None = None,
        pool_instance: PoolT | None = None,
    ) -> None:
        self.dsn = dsn
        self.bind_key = bind_key
        self.session_key = session_key
        self.connection_key = connection_key
        self.supports_connection_pooling = supports_connection_pooling
        self.pool_config = dict(pool_config or {})
        self.driver_features = dict(driver_features or {})
        self._driver_instance = driver_instance
        self._connection_instance = connection_instance
        self._pool_instance = pool_instance
        _inject_sqlspec_pool_defaults(self)

    def provide_session(self) -> DriverT:
        """Return the configured asynchronous driver session."""
        return cast("DriverT", self._driver_instance)

    def provide_connection(self) -> ConnectionT:
        """Return the configured asynchronous connection."""
        return cast("ConnectionT", self._connection_instance)

    def create_pool(self) -> PoolT:
        """Return the configured asynchronous connection pool."""
        return cast("PoolT", self._pool_instance)


def _is_in_memory_dsn(dsn: str | None) -> bool:
    if not dsn:
        return False
    lowered = dsn.lower()
    return ":memory:" in lowered or "mode=memory" in lowered


def _inject_sqlspec_pool_defaults(config: Any) -> None:
    supports_pooling = bool(getattr(config, "supports_connection_pooling", True))
    dsn = getattr(config, "dsn", None) or getattr(config, "connection_string", None)
    if not supports_pooling or _is_in_memory_dsn(dsn):
        return
    pool_cfg = getattr(config, "pool_config", None)
    if isinstance(pool_cfg, dict):
        pool_cfg.setdefault("pool_pre_ping", True)
        pool_cfg.setdefault("pool_recycle", 1800)


class SQLSpecConfig[ConfigT]:
    """Generic SQLSpec configuration wrapper preserving concrete ConfigT types across all SQLSpec drivers."""

    __slots__ = ("config",)

    def __init__(self, config: ConfigT) -> None:
        _inject_sqlspec_pool_defaults(config)
        self.config = config

    @property
    def is_async(self) -> bool:
        """Return True when the wrapped configuration is asynchronous."""
        return bool(getattr(self.config, "is_async", False))

    @property
    def supports_connection_pooling(self) -> bool:
        """Return True when the wrapped configuration supports connection pooling."""
        return bool(getattr(self.config, "supports_connection_pooling", True))

    def provide_session(self) -> Any:
        """Delegate session provision to the wrapped configuration."""
        fn = getattr(self.config, "provide_session", None)
        return fn() if callable(fn) else None

    def provide_connection(self) -> Any:
        """Delegate connection provision to the wrapped configuration."""
        fn = getattr(self.config, "provide_connection", None)
        return fn() if callable(fn) else None

    def create_pool(self) -> Any:
        """Delegate pool creation to the wrapped configuration."""
        fn = getattr(self.config, "create_pool", None)
        return fn() if callable(fn) else None

    def __getattr__(self, name: str) -> Any:
        """Forward attribute lookups to the underlying ConfigT instance."""
        return getattr(self.config, name)


class ProxySyncConfig[
    SyncConfigT: SyncDatabaseConfig[Any, Any, Any],
](SyncDatabaseConfig[Any, Any, Any]):
    """Generic synchronous SQLSpec proxy configuration preserving inner SyncConfigT types."""

    def __init__(
        self,
        config: SyncConfigT,
        *,
        broker: BrokerTransport | None = None,
    ) -> None:
        _inject_sqlspec_pool_defaults(config)
        super().__init__(
            dsn=getattr(config, "dsn", "sqlite:///:memory:"),
            bind_key=getattr(config, "bind_key", "default"),
            session_key=getattr(config, "session_key", "db_session"),
            connection_key=getattr(config, "connection_key", "db_connection"),
            supports_connection_pooling=getattr(config, "supports_connection_pooling", True),
            pool_config=getattr(config, "pool_config", None),
            driver_features=getattr(config, "driver_features", None),
        )
        self.config: SyncConfigT = config
        self.broker = broker

    def provide_session(self) -> Any:
        """Delegate session provision to the wrapped SyncConfigT."""
        return self.config.provide_session()

    def provide_connection(self) -> Any:
        """Delegate connection provision to the wrapped SyncConfigT."""
        return self.config.provide_connection()

    def create_pool(self) -> Any:
        """Delegate pool creation to the wrapped SyncConfigT."""
        return self.config.create_pool()


class ProxyAsyncConfig[
    AsyncConfigT: AsyncDatabaseConfig[Any, Any, Any],
](AsyncDatabaseConfig[Any, Any, Any]):
    """Generic asynchronous SQLSpec proxy configuration preserving inner AsyncConfigT types."""

    def __init__(
        self,
        config: AsyncConfigT,
        *,
        broker: BrokerTransport | None = None,
    ) -> None:
        _inject_sqlspec_pool_defaults(config)
        super().__init__(
            dsn=getattr(config, "dsn", "sqlite+aiosqlite:///:memory:"),
            bind_key=getattr(config, "bind_key", "default"),
            session_key=getattr(config, "session_key", "db_session"),
            connection_key=getattr(config, "connection_key", "db_connection"),
            supports_connection_pooling=getattr(config, "supports_connection_pooling", True),
            pool_config=getattr(config, "pool_config", None),
            driver_features=getattr(config, "driver_features", None),
        )
        self.config: AsyncConfigT = config
        self.broker = broker

    def provide_session(self) -> Any:
        """Delegate session provision to the wrapped AsyncConfigT."""
        return self.config.provide_session()

    def provide_connection(self) -> Any:
        """Delegate connection provision to the wrapped AsyncConfigT."""
        return self.config.provide_connection()

    def create_pool(self) -> Any:
        """Delegate pool creation to the wrapped AsyncConfigT."""
        return self.config.create_pool()


def wrap_config[ConfigT](config: ConfigT) -> SQLSpecConfig[ConfigT]:
    """Wrap any SQLSpec configuration in SQLSpecConfig[ConfigT] with production pool defaults."""
    return SQLSpecConfig(config)


@dataclass(slots=True)
class SQLSpecState:
    """Runtime state container registered under state['sqlspec']."""

    pools: dict[str | None, Any] = field(default_factory=dict)
    configs: dict[str | None, Any] = field(default_factory=dict)
    object_store: SQLSpecStorageBridge | None = None
    is_available: bool = True


@dataclass(slots=True)
class SQLSpec[ConfigT]:
    """Litestar and Vertebra SQLSpec plugin parameterized by the wrapped database configuration."""

    config: ConfigT | Sequence[ConfigT] | None = None
    store: BaseObjectStore | None = None
    storage_prefix: str = ""
    storage_key: str = "vertebra"
    loader: Any = None
    priority: int = SQLSPEC_PLUGIN_PRIORITY
    _state: SQLSpecState | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        """Apply pool health defaults to wrapped config object(s)."""
        for item in self.configs:
            _inject_sqlspec_pool_defaults(item)

    @property
    def configs(self) -> tuple[ConfigT, ...]:
        """Return all registered SQLSpec configuration objects as a tuple."""
        if self.config is None:
            return ()
        if isinstance(self.config, Sequence) and not isinstance(self.config, (str, bytes)):
            return tuple(self.config)
        return (self.config,)

    @overload
    def get_session[SyncDriverT: SyncDriverAdapterBase](
        self,
        config: SyncDatabaseConfig[Any, Any, SyncDriverT] | ProxySyncConfig[SyncDatabaseConfig[Any, Any, SyncDriverT]],
    ) -> SyncDriverT: ...

    @overload
    def get_session[AsyncDriverT: AsyncDriverAdapterBase](
        self,
        config: AsyncDatabaseConfig[Any, Any, AsyncDriverT]
        | ProxyAsyncConfig[AsyncDatabaseConfig[Any, Any, AsyncDriverT]],
    ) -> AsyncDriverT: ...

    def get_session(self, config: Any = None) -> Any:
        """Return a typed driver session from config or the primary registered configuration."""
        target = config if config is not None else (self.configs[0] if self.configs else None)
        if target is None:
            msg = "No SQLSpec configuration registered on plugin"
            raise RuntimeError(msg)
        fn = getattr(target, "provide_session", None)
        return fn() if callable(fn) else None

    def install_state(self, state_mapping: dict[str, Any]) -> SQLSpecState:
        """Populate and return the SQLSpecState in a mutable state dictionary."""
        adapter = SQLSpecStorageBridge(self.store, prefix=self.storage_prefix) if self.store is not None else None
        configs_map: dict[str | None, Any] = {}
        pools_map: dict[str | None, Any] = {}
        for idx, cfg in enumerate(self.configs):
            key = getattr(cfg, "bind_key", None) or (self.storage_key if idx == 0 else f"{self.storage_key}_{idx}")
            configs_map[key] = cfg
            create_pool_fn = getattr(cfg, "create_pool", None)
            if callable(create_pool_fn):
                with contextlib.suppress(Exception):
                    pool_obj = create_pool_fn()
                    if pool_obj is not None:
                        pools_map[key] = pool_obj
        if self.config is not None and self.storage_key not in configs_map:
            configs_map[self.storage_key] = self.config
        state = SQLSpecState(pools=pools_map, configs=configs_map, object_store=adapter, is_available=True)
        self._state = state
        state_mapping[SQLSPEC_STATE_KEY] = state
        return state

    async def close_pools(self) -> None:
        """Close all registered SQLSpec connection pools across primary and secondary bind keys."""
        if self._state is None:
            return
        for pool in list(self._state.pools.values()):
            close_fn = getattr(pool, "close", None)
            if callable(close_fn):
                res = close_fn()
                if inspect.isawaitable(res):
                    await res
        for cfg in self.configs:
            close_pool_fn = getattr(cfg, "close_pool", None)
            if callable(close_pool_fn):
                res = close_pool_fn()
                if inspect.isawaitable(res):
                    await res

    def on_app_init(self, app_config: Any) -> Any:
        """Litestar InitPluginProtocol hook registering SQLSpecState and SkipValidation DI providers."""
        target_state = getattr(app_config, "state", None)
        if isinstance(target_state, dict):
            self.install_state(target_state)
        elif target_state is not None and hasattr(target_state, "update"):
            holder: dict[str, Any] = {}
            self.install_state(holder)
            target_state.update(holder)

        dependencies = getattr(app_config, "dependencies", None)
        if isinstance(dependencies, dict):
            for cfg in self.configs:
                self._register_config_dependencies(dependencies, cfg)

        on_shutdown = getattr(app_config, "on_shutdown", None)
        if isinstance(on_shutdown, list):
            on_shutdown.append(self.close_pools)
        return app_config

    def on_cli_init(self, cli: Any) -> None:
        """Litestar CLIPluginProtocol hook for CLI extension registration."""
        _ = cli

    @staticmethod
    def _register_config_dependencies(dependencies: dict[str, Any], cfg: Any) -> None:
        session_key = getattr(cfg, "session_key", "db_session")
        connection_key = getattr(cfg, "connection_key", "db_connection")
        provide_session_fn = getattr(cfg, "provide_session", None)
        provide_connection_fn = getattr(cfg, "provide_connection", None)

        provide_cls: type[Any] | None = None
        try:
            provide_cls = importlib.import_module("litestar.di").Provide
        except ImportError:
            provide_cls = None

        if callable(provide_session_fn) and session_key not in dependencies:
            wrapped_session = SQLSpec._wrap_skip_validation_provider(provide_session_fn)
            dependencies[session_key] = (
                provide_cls(wrapped_session, sync_to_thread=False) if provide_cls is not None else wrapped_session
            )
        if callable(provide_connection_fn) and connection_key not in dependencies:
            wrapped_conn = SQLSpec._wrap_skip_validation_provider(provide_connection_fn)
            dependencies[connection_key] = (
                provide_cls(wrapped_conn, sync_to_thread=False) if provide_cls is not None else wrapped_conn
            )

    @staticmethod
    def _wrap_skip_validation_provider(fn: Any) -> Any:
        sig = inspect.signature(fn)
        ret_ann = sig.return_annotation if sig.return_annotation is not inspect.Signature.empty else Any
        skip_ann = synthesize_skip_validation_annotation(ret_ann)

        def provider() -> Any:
            return fn()

        provider_obj: Any = provider
        provider_obj.__signature__ = sig.replace(return_annotation=skip_ann)
        provider_obj.__annotations__ = {"return": skip_ann}
        return provider


SQLSpecPlugin = SQLSpec
SQLSpecStorePlugin = SQLSpec
VertebraSQLSpecPlugin = SQLSpec
VertebraProxySyncConfig = ProxySyncConfig
VertebraProxyAsyncConfig = ProxyAsyncConfig
