"""Application plugin and PEP 695 generic configurations for SQLSpec integration."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Protocol, Self, cast, overload, runtime_checkable

import msgspec

from vertebra.extensions.alchemy.broker import BrokerPlugin, BrokerTransport, ChannelMessage
from vertebra.extensions.alchemy.plugin import AlchemyPlugin
from vertebra.extensions.sqlspec.provider import SqlSpecProvider
from vertebra.extensions.sqlspec.proxy import (
    ProxyAsyncSessionContext,
    ProxySyncSessionContext,
)
from vertebra.extensions.sqlspec.storage import SQLSpecStore
from vertebra.storage import ObjectStore as BaseObjectStore
from vertebra.utils.module_loader import import_optional
from vertebra.utils.sync_tools import SyncPool

logger = logging.getLogger(__name__)

__all__ = (
    "SQLSPEC_PLUGIN_PRIORITY",
    "SQLSPEC_STATE_KEY",
    "AsyncDriverAdapterBase",
    "BrokerPlugin",
    "ChannelMessage",
    "DatabaseConfigProtocol",
    "ProxyAsyncConfig",
    "ProxySyncConfig",
    "SQLSpecAsyncConfig",
    "SQLSpecConfig",
    "SQLSpecPlugin",
    "SQLSpecState",
    "SQLSpecSyncConfig",
    "SyncDriverAdapterBase",
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


class _BaseSQLSpecConfig(msgspec.Struct, frozen=True, gc=False, kw_only=True):
    """Shared base configuration for SQLSpec synchronous and asynchronous database engines."""

    bind_key: str = "default"
    dsn: str = "sqlite:///:memory:"
    direct_dsn: str | None = None
    pooler_mode: Literal["session", "transaction", "statement", "direct", "auto"] | None = None
    min_size: int = 1
    max_size: int = 10
    statement_cache_size: int = 256
    enable_arrow: bool = False
    driver_features: dict[str, Any] = msgspec.field(default_factory=dict)
    session_key: str = "db_session"
    connection_key: str = "db_connection"
    supports_connection_pooling: bool = True
    pool_config: dict[str, Any] = msgspec.field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.pooler_mode in ("transaction", "statement") and self.statement_cache_size == 256:
            msgspec.structs.force_setattr(self, "statement_cache_size", 0)
        if self.supports_connection_pooling and not _is_in_memory_dsn(self.dsn):
            self.pool_config.setdefault("pool_pre_ping", True)
            self.pool_config.setdefault("pool_recycle", 1800)

    def effective_pool_config(self) -> dict[str, Any]:
        """Return merged effective connection pool options."""
        cfg: dict[str, Any] = {
            "min_size": self.min_size,
            "max_size": self.max_size,
            "statement_cache_size": self.statement_cache_size,
            "enable_arrow": self.enable_arrow,
            **self.pool_config,
        }
        if self.supports_connection_pooling and not _is_in_memory_dsn(self.dsn):
            cfg.setdefault("pool_pre_ping", True)
            cfg.setdefault("pool_recycle", 1800)
        return cfg

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone configuration with per-pool overrides and deep-copied mutable dictionaries (`sqlspec>=0.68.0`)."""
        base = msgspec.structs.asdict(self)
        base["pool_config"] = dict(self.pool_config)
        base["driver_features"] = dict(self.driver_features)
        if "pool_config" in overrides and isinstance(overrides["pool_config"], dict):
            merged_pool = dict(self.pool_config)
            merged_pool.update(overrides["pool_config"])
            overrides = {**overrides, "pool_config": merged_pool}
        if "driver_features" in overrides and isinstance(overrides["driver_features"], dict):
            merged_features = dict(self.driver_features)
            merged_features.update(overrides["driver_features"])
            overrides = {**overrides, "driver_features": merged_features}
        base.update(overrides)
        return type(self)(**base)

    def derive_direct_config(self) -> Self:
        """Return a session-mode configuration targeting ``direct_dsn`` for DDL/migrations/LISTEN (`sqlspec>=0.68.0`)."""
        if not self.direct_dsn:
            return self
        return self.derive_pool(
            dsn=self.direct_dsn,
            pooler_mode="session",
            min_size=min(1, self.max_size),
            max_size=min(2, self.max_size),
        )


class SQLSpecSyncConfig[
    ConnectionT,
    PoolT,
    DriverT: SyncDriverAdapterBase,
](_BaseSQLSpecConfig, frozen=True, gc=False, kw_only=True):
    """Synchronous SQLSpec database configuration inheriting from _BaseSQLSpecConfig."""

    driver_instance: Any = None
    connection_instance: Any = None
    pool_instance: Any = None

    @property
    def is_async(self) -> bool:
        """Return False for synchronous configuration."""
        return False

    def provide_session(self) -> DriverT:
        """Return the configured synchronous driver session."""
        return cast("DriverT", self.driver_instance)

    def provide_connection(self) -> ConnectionT:
        """Return the configured synchronous connection."""
        return cast("ConnectionT", self.connection_instance)

    def create_pool(self) -> PoolT:
        """Return the configured synchronous connection pool."""
        return cast("PoolT", self.pool_instance)


class SQLSpecAsyncConfig[
    ConnectionT,
    PoolT,
    DriverT: AsyncDriverAdapterBase,
](_BaseSQLSpecConfig, frozen=True, gc=False, kw_only=True):
    """Asynchronous SQLSpec database configuration inheriting from _BaseSQLSpecConfig."""

    driver_instance: Any = None
    connection_instance: Any = None
    pool_instance: Any = None

    @property
    def is_async(self) -> bool:
        """Return True for asynchronous configuration."""
        return True

    def provide_session(self) -> DriverT:
        """Return the configured asynchronous driver session."""
        return cast("DriverT", self.driver_instance)

    def provide_connection(self) -> ConnectionT:
        """Return the configured asynchronous connection."""
        return cast("ConnectionT", self.connection_instance)

    def create_pool(self) -> PoolT:
        """Return the configured asynchronous connection pool."""
        return cast("PoolT", self.pool_instance)


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

    def derive_pool(self, **overrides: Any) -> SQLSpecConfig[ConfigT]:
        """Delegate per-pool configuration cloning to the wrapped configuration (`sqlspec>=0.68.0`)."""
        fn = getattr(self.config, "derive_pool", None)
        if callable(fn):
            return SQLSpecConfig(cast("ConfigT", fn(**overrides)))
        return self

    def derive_direct_config(self) -> SQLSpecConfig[ConfigT]:
        """Delegate direct-session configuration derivation to the wrapped configuration (`sqlspec>=0.68.0`)."""
        fn = getattr(self.config, "derive_direct_config", None)
        if callable(fn):
            return SQLSpecConfig(cast("ConfigT", fn()))
        return self

    def __getattr__(self, name: str) -> Any:
        """Forward attribute lookups to the underlying ConfigT instance."""
        return getattr(self.config, name)

    @classmethod
    def wrap[T](cls, config: T) -> SQLSpecConfig[T]:
        """Wrap any SQLSpec configuration in SQLSpecConfig[T] with production pool defaults."""
        return SQLSpecConfig(config)


class ProxySyncConfig[
    SyncConfigT,
]:
    """Generic synchronous SQLSpec proxy configuration preserving inner SyncConfigT types."""

    __slots__ = ("broker", "config", "provider")

    def __init__(
        self,
        config: SyncConfigT,
        *,
        broker: BrokerTransport | SqlSpecProvider | None = None,
        provider: SqlSpecProvider | None = None,
    ) -> None:
        _inject_sqlspec_pool_defaults(config)
        self.config: SyncConfigT = config
        if isinstance(broker, SqlSpecProvider):
            self.provider: SqlSpecProvider | None = provider or broker
            self.broker: Any = broker.broker
        else:
            self.provider = provider
            self.broker = broker

    @property
    def is_async(self) -> bool:
        """Return False for synchronous proxy."""
        return False

    @property
    def dsn(self) -> str:
        """Return configured DSN."""
        return str(getattr(self.config, "dsn", "sqlite:///:memory:"))

    @property
    def bind_key(self) -> str:
        """Return configured bind key."""
        return str(getattr(self.config, "bind_key", "default"))

    @property
    def session_key(self) -> str:
        """Return configured session key."""
        return str(getattr(self.config, "session_key", "db_session"))

    @property
    def connection_key(self) -> str:
        """Return configured connection key."""
        return str(getattr(self.config, "connection_key", "db_connection"))

    @property
    def supports_connection_pooling(self) -> bool:
        """Return True when connection pooling is supported."""
        return bool(getattr(self.config, "supports_connection_pooling", True))

    @property
    def pool_config(self) -> dict[str, Any]:
        """Return pool configuration dictionary."""
        return getattr(self.config, "pool_config", {})

    @property
    def driver_features(self) -> dict[str, Any]:
        """Return driver features dictionary."""
        return getattr(self.config, "driver_features", {})

    def provide_proxied_session(self, *, worker_pid: int | None = None) -> ProxySyncSessionContext:
        """Return a synchronous session context manager backed by SqlSpecProvider and SHM leases."""
        if self.provider is None:
            self.provider = SqlSpecProvider(
                self.config,
                broker=self.broker,
                pool_name=self.bind_key,
            )
        return ProxySyncSessionContext(
            self.provider,
            broker=self.broker,
            pool_name=self.bind_key,
            worker_pid=worker_pid,
        )

    def provide_session(self) -> Any:
        """Delegate session provision to ProxySyncSessionContext when provider is attached, or inner SyncConfigT."""
        if self.provider is not None:
            return ProxySyncSessionContext(
                self.provider,
                broker=self.broker,
                pool_name=self.bind_key,
            )
        fn = getattr(self.config, "provide_session", None)
        return fn() if callable(fn) else None

    def provide_connection(self) -> Any:
        """Delegate connection provision to the wrapped SyncConfigT."""
        fn = getattr(self.config, "provide_connection", None)
        return fn() if callable(fn) else None

    def create_pool(self) -> Any:
        """Delegate pool creation to the wrapped SyncConfigT."""
        fn = getattr(self.config, "create_pool", None)
        return fn() if callable(fn) else None

    def derive_pool(self, **overrides: Any) -> ProxySyncConfig[SyncConfigT]:
        """Delegate per-pool configuration cloning to the wrapped SyncConfigT (`sqlspec>=0.68.0`)."""
        fn = getattr(self.config, "derive_pool", None)
        if callable(fn):
            return ProxySyncConfig(
                cast("SyncConfigT", fn(**overrides)),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    def derive_direct_config(self) -> ProxySyncConfig[SyncConfigT]:
        """Delegate direct-session configuration derivation to the wrapped SyncConfigT (`sqlspec>=0.68.0`)."""
        fn = getattr(self.config, "derive_direct_config", None)
        if callable(fn):
            return ProxySyncConfig(
                cast("SyncConfigT", fn()),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    def __getattr__(self, name: str) -> Any:
        """Forward attribute lookups to the underlying SyncConfigT instance."""
        return getattr(self.config, name)


class ProxyAsyncConfig[
    AsyncConfigT,
]:
    """Generic asynchronous SQLSpec proxy configuration preserving inner AsyncConfigT types."""

    __slots__ = ("broker", "config", "provider")

    def __init__(
        self,
        config: AsyncConfigT,
        *,
        broker: BrokerTransport | SqlSpecProvider | None = None,
        provider: SqlSpecProvider | None = None,
    ) -> None:
        _inject_sqlspec_pool_defaults(config)
        self.config: AsyncConfigT = config
        if isinstance(broker, SqlSpecProvider):
            self.provider: SqlSpecProvider | None = provider or broker
            self.broker: Any = broker.broker
        else:
            self.provider = provider
            self.broker = broker

    @property
    def is_async(self) -> bool:
        """Return True for asynchronous proxy."""
        return True

    @property
    def dsn(self) -> str:
        """Return configured DSN."""
        return str(getattr(self.config, "dsn", "sqlite+aiosqlite:///:memory:"))

    @property
    def bind_key(self) -> str:
        """Return configured bind key."""
        return str(getattr(self.config, "bind_key", "default"))

    @property
    def session_key(self) -> str:
        """Return configured session key."""
        return str(getattr(self.config, "session_key", "db_session"))

    @property
    def connection_key(self) -> str:
        """Return configured connection key."""
        return str(getattr(self.config, "connection_key", "db_connection"))

    @property
    def supports_connection_pooling(self) -> bool:
        """Return True when connection pooling is supported."""
        return bool(getattr(self.config, "supports_connection_pooling", True))

    @property
    def pool_config(self) -> dict[str, Any]:
        """Return pool configuration dictionary."""
        return getattr(self.config, "pool_config", {})

    @property
    def driver_features(self) -> dict[str, Any]:
        """Return driver features dictionary."""
        return getattr(self.config, "driver_features", {})

    def provide_proxied_session(self, *, worker_pid: int | None = None) -> ProxyAsyncSessionContext:
        """Return an asynchronous session context manager backed by SqlSpecProvider and SHM leases."""
        if self.provider is None:
            self.provider = SqlSpecProvider(
                self.config,
                broker=self.broker,
                pool_name=self.bind_key,
            )
        return ProxyAsyncSessionContext(
            self.provider,
            broker=self.broker,
            pool_name=self.bind_key,
            worker_pid=worker_pid,
        )

    def provide_session(self) -> Any:
        """Delegate session provision to ProxyAsyncSessionContext when provider is attached, or inner AsyncConfigT."""
        if self.provider is not None:
            return ProxyAsyncSessionContext(
                self.provider,
                broker=self.broker,
                pool_name=self.bind_key,
            )
        fn = getattr(self.config, "provide_session", None)
        return fn() if callable(fn) else None

    def provide_connection(self) -> Any:
        """Delegate connection provision to the wrapped AsyncConfigT."""
        fn = getattr(self.config, "provide_connection", None)
        return fn() if callable(fn) else None

    def create_pool(self) -> Any:
        """Delegate pool creation to the wrapped AsyncConfigT."""
        fn = getattr(self.config, "create_pool", None)
        return fn() if callable(fn) else None

    def derive_pool(self, **overrides: Any) -> ProxyAsyncConfig[AsyncConfigT]:
        """Delegate per-pool configuration cloning to the wrapped AsyncConfigT (`sqlspec>=0.68.0`)."""
        fn = getattr(self.config, "derive_pool", None)
        if callable(fn):
            return ProxyAsyncConfig(
                cast("AsyncConfigT", fn(**overrides)),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    def derive_direct_config(self) -> ProxyAsyncConfig[AsyncConfigT]:
        """Delegate direct-session configuration derivation to the wrapped AsyncConfigT (`sqlspec>=0.68.0`)."""
        fn = getattr(self.config, "derive_direct_config", None)
        if callable(fn):
            return ProxyAsyncConfig(
                cast("AsyncConfigT", fn()),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    def __getattr__(self, name: str) -> Any:
        """Forward attribute lookups to the underlying AsyncConfigT instance."""
        return getattr(self.config, name)


class SQLSpecState:
    """Runtime state container registered under state['sqlspec']."""

    __slots__ = ("configs", "is_available", "object_store", "pools")

    def __init__(
        self,
        pools: dict[str | None, Any] | None = None,
        configs: dict[str | None, Any] | None = None,
        object_store: SQLSpecStore | None = None,
        is_available: bool = True,
    ) -> None:
        self.pools: dict[str | None, Any] = dict(pools or {})
        self.configs: dict[str | None, Any] = dict(configs or {})
        self.object_store = object_store
        self.is_available = is_available


class SQLSpecPlugin[ConfigT]:
    """Litestar and Vertebra SQLSpec plugin parameterized by the wrapped database configuration."""

    PLUGIN_PRIORITY: ClassVar[int] = SQLSPEC_PLUGIN_PRIORITY
    STATE_KEY: ClassVar[str] = SQLSPEC_STATE_KEY
    __slots__ = (
        "_state",
        "broker",
        "config",
        "loader",
        "priority",
        "storage_key",
        "storage_prefix",
        "store",
    )

    def __init__(
        self,
        config: ConfigT | Sequence[ConfigT] | None = None,
        store: BaseObjectStore | None = None,
        storage_prefix: str = "",
        storage_key: str = "vertebra",
        loader: Any = None,
        broker: Any = None,
        priority: int = SQLSPEC_PLUGIN_PRIORITY,
    ) -> None:
        self.config = config
        self.store = store
        self.storage_prefix = storage_prefix
        self.storage_key = storage_key
        self.loader = loader
        self.broker = broker
        self.priority = priority
        self._state: SQLSpecState | None = None
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

    @staticmethod
    def wrap_config[T](config: T) -> SQLSpecConfig[T]:
        """Wrap any SQLSpec configuration in SQLSpecConfig[T] with production pool defaults."""
        return SQLSpecConfig(config)

    @overload
    def get_session[SyncDriverT: SyncDriverAdapterBase](
        self,
        config: SQLSpecSyncConfig[Any, Any, SyncDriverT] | ProxySyncConfig[SQLSpecSyncConfig[Any, Any, SyncDriverT]],
    ) -> SyncDriverT: ...

    @overload
    def get_session[AsyncDriverT: AsyncDriverAdapterBase](
        self,
        config: SQLSpecAsyncConfig[Any, Any, AsyncDriverT]
        | ProxyAsyncConfig[SQLSpecAsyncConfig[Any, Any, AsyncDriverT]],
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
        adapter = SQLSpecStore(self.store, prefix=self.storage_prefix) if self.store is not None else None
        configs_map: dict[str | None, Any] = {}
        pools_map: dict[str | None, Any] = {}
        for idx, cfg in enumerate(self.configs):
            key = getattr(cfg, "bind_key", None) or (self.storage_key if idx == 0 else f"{self.storage_key}_{idx}")
            configs_map[key] = cfg
            create_pool_fn = getattr(cfg, "create_pool", None)
            if callable(create_pool_fn):
                try:
                    pool_obj = create_pool_fn()
                    if pool_obj is not None:
                        pools_map[key] = pool_obj
                except (AttributeError, RuntimeError, OSError, ValueError) as exc:
                    logger.debug("Failed creating pool for %s", key, exc_info=exc)
        if self.config is not None and self.storage_key not in configs_map:
            configs_map[self.storage_key] = self.config
        state = SQLSpecState(pools=pools_map, configs=configs_map, object_store=adapter, is_available=True)
        self._state = state
        state_mapping[SQLSPEC_STATE_KEY] = state
        return state

    async def _close_target(self, target: Any) -> None:
        """Close or dispose a target pool or configuration object, offloading synchronous closes to SyncPool."""
        for attr in ("close", "dispose", "close_pool"):
            close_fn = getattr(target, attr, None)
            if callable(close_fn):
                try:
                    if inspect.iscoroutinefunction(close_fn):
                        await close_fn()
                    else:
                        res = await SyncPool.default().submit_async(close_fn)
                        if inspect.isawaitable(res):
                            await res
                except (AttributeError, RuntimeError, OSError, ValueError) as exc:
                    logger.debug("Error closing SQLSpec target %s", target, exc_info=exc)

    async def close_pools(self) -> None:
        """Close all registered SQLSpec connection pools across primary and secondary bind keys."""
        if self._state is None:
            return
        closed: set[int] = set()
        for pool in list(self._state.pools.values()):
            if id(pool) not in closed:
                closed.add(id(pool))
                await self._close_target(pool)
        for cfg in self.configs:
            pool_inst = getattr(cfg, "pool_instance", None)
            if pool_inst is not None:
                closed.add(id(pool_inst))
            if id(cfg) not in closed:
                closed.add(id(cfg))
                await self._close_target(cfg)

    async def shutdown(self) -> None:
        """Shut down and close all active SQLSpec connection pools."""
        await self.close_pools()

    def on_app_init(self, app_config: Any) -> Any:
        """Litestar InitPluginProtocol hook registering SQLSpecState and SkipValidation DI providers."""
        target_state = getattr(app_config, "state", None)
        if isinstance(target_state, dict):
            self.install_state(target_state)
            if self.broker is not None:
                broker_transport = getattr(self.broker, "transport", self.broker)
                target_state["db_broker"] = broker_transport
                if "zpages" in target_state and hasattr(self.broker, "_register_zpages"):
                    self.broker._register_zpages(target_state["zpages"])
        elif target_state is not None and hasattr(target_state, "update"):
            holder: dict[str, Any] = {}
            self.install_state(holder)
            if self.broker is not None:
                broker_transport = getattr(self.broker, "transport", self.broker)
                holder["db_broker"] = broker_transport
                if hasattr(target_state, "zpages") and hasattr(self.broker, "_register_zpages"):
                    self.broker._register_zpages(target_state.zpages)
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

    @classmethod
    def _register_config_dependencies(cls, dependencies: dict[str, Any], cfg: Any) -> None:
        session_key = getattr(cfg, "session_key", "db_session")
        connection_key = getattr(cfg, "connection_key", "db_connection")
        provide_session_fn = getattr(cfg, "provide_session", None)
        provide_connection_fn = getattr(cfg, "provide_connection", None)

        di_mod = import_optional("litestar.di")
        provide_cls: type[Any] | None = getattr(di_mod, "Provide", None) if di_mod is not None else None

        if callable(provide_session_fn) and session_key not in dependencies:
            wrapped_session = cls._wrap_skip_validation_provider(provide_session_fn)
            dependencies[session_key] = (
                provide_cls(wrapped_session, sync_to_thread=False) if provide_cls is not None else wrapped_session
            )
        if callable(provide_connection_fn) and connection_key not in dependencies:
            wrapped_conn = cls._wrap_skip_validation_provider(provide_connection_fn)
            dependencies[connection_key] = (
                provide_cls(wrapped_conn, sync_to_thread=False) if provide_cls is not None else wrapped_conn
            )

    @staticmethod
    def _wrap_skip_validation_provider(fn: Any) -> Any:
        sig = inspect.signature(fn)
        ret_ann = sig.return_annotation if sig.return_annotation is not inspect.Signature.empty else Any
        skip_ann = AlchemyPlugin.synthesize_skip_validation_annotation(ret_ann)

        def provider() -> Any:
            return fn()

        provider_obj: Any = provider
        provider_obj.__signature__ = sig.replace(return_annotation=skip_ann)
        provider_obj.__annotations__ = {"return": skip_ann}
        return provider


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
