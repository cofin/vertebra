"""Application plugin and PEP 695 generic configurations for SQLSpec integration."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Awaitable
from typing import Any, ClassVar, cast

from vertebra.extensions.broker import (
    AsyncSHMBrokerTransport,
    BrokerPlugin,
    ChannelMessage,
    SyncSHMBrokerTransport,
)
from vertebra.extensions.sqlspec.provider import (
    AsyncBrokerLeaseSource,
    SQLSpecAsyncProvider,
    SQLSpecSyncProvider,
    SyncBrokerLeaseSource,
    _BaseSQLSpecProvider,
)
from vertebra.extensions.sqlspec.proxy import (
    ProxyAsyncSessionContext,
    ProxySyncSessionContext,
)
from vertebra.extensions.sqlspec.storage import register_storage_alias
from vertebra.storage import AsyncObjectStore, SyncObjectStore
from vertebra.typing import MissingDependencyError

try:
    from sqlspec import SQLSpec
    from sqlspec.config import AsyncDatabaseConfig, SyncDatabaseConfig
    from sqlspec.driver import AsyncDriverAdapterBase, SyncDriverAdapterBase
    from sqlspec.extensions.litestar import SQLSpecPlugin
except ImportError as exc:
    raise MissingDependencyError("sqlspec", install_package="sqlspec", extra="sqlspec") from exc

type SyncBrokerTransport = SyncSHMBrokerTransport | SyncBrokerLeaseSource
type AsyncBrokerTransport = AsyncSHMBrokerTransport | AsyncBrokerLeaseSource

logger = logging.getLogger(__name__)

__all__ = (
    "SQLSPEC_PLUGIN_PRIORITY",
    "SQLSPEC_STATE_KEY",
    "AsyncDriverAdapterBase",
    "BrokerPlugin",
    "ChannelMessage",
    "ProxyAsyncConfig",
    "ProxySyncConfig",
    "SQLSpecPlugin",
    "SyncDriverAdapterBase",
    "create_sqlspec_plugin",
)

SQLSPEC_PLUGIN_PRIORITY: int = -5800
SQLSPEC_STATE_KEY: str = "sqlspec"


class _BaseProxyConfig:
    """Shared Litestar extension key normalization and forwarding properties for SQLSpec proxy configurations."""

    _DEFAULT_DSN: ClassVar[str] = "sqlite:///:memory:"
    config: Any
    extension_config: dict[str, Any]

    @property
    def dsn(self) -> str:
        """Return configured DSN."""
        return str(getattr(self.config, "dsn", self._DEFAULT_DSN))

    @property
    def session_key(self) -> str:
        """Return configured session key."""
        return str(
            self.extension_config.get("litestar", {}).get(
                "session_key",
                getattr(self.config, "session_key", "db_session"),
            )
        )

    @property
    def connection_key(self) -> str:
        """Return configured connection key."""
        return str(
            self.extension_config.get("litestar", {}).get(
                "connection_key",
                getattr(self.config, "connection_key", "db_connection"),
            )
        )

    @property
    def connection_type(self) -> type[Any]:
        """Return connection type annotation for DI registration."""
        return getattr(self.config, "connection_type", Any)

    @connection_type.setter
    def connection_type(self, value: type[Any]) -> None:
        """Set connection type annotation."""
        with contextlib.suppress(AttributeError):
            setattr(self.config, _CONN_TYPE_ATTR, value)

    @property
    def driver_type(self) -> type[Any]:
        """Return driver type annotation for DI registration."""
        return getattr(self.config, "driver_type", Any)

    @driver_type.setter
    def driver_type(self, value: type[Any]) -> None:
        """Set driver type annotation."""
        with contextlib.suppress(AttributeError):
            setattr(self.config, _DRIVER_TYPE_ATTR, value)

    @property
    def pool_config(self) -> dict[str, Any]:
        """Return pool configuration dictionary."""
        return getattr(self.config, "pool_config", {})

    def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate connection provision to the wrapped configuration."""
        _ = (args, kwargs)
        fn = getattr(self.config, "provide_connection", None)
        return fn() if callable(fn) else None

    def create_pool(self) -> Any:
        """Delegate pool creation to the wrapped configuration."""
        fn = getattr(self.config, "create_pool", None)
        return fn() if callable(fn) else None

    def __getattr__(self, name: str) -> Any:
        """Forward attribute lookups to the underlying configuration instance."""
        return getattr(self.config, name)

    def _configure_litestar_extension_keys(self, config: Any, resolved_bind_key: str | None) -> None:
        """Populate default Litestar DI keys on self.extension_config."""
        ext_cfg = dict(getattr(config, "extension_config", {}))
        litestar_cfg = dict(ext_cfg.get("litestar", {}))
        if "connection_key" not in litestar_cfg:
            conn_k = getattr(config, "connection_key", None)
            if conn_k:
                litestar_cfg["connection_key"] = str(conn_k)
            elif resolved_bind_key and resolved_bind_key not in ("default", "primary"):
                litestar_cfg["connection_key"] = f"{resolved_bind_key}_connection"
            else:
                litestar_cfg["connection_key"] = "db_connection"
        if "session_key" not in litestar_cfg:
            sess_k = getattr(config, "session_key", None)
            if sess_k:
                litestar_cfg["session_key"] = str(sess_k)
            elif resolved_bind_key and resolved_bind_key not in ("default", "primary"):
                litestar_cfg["session_key"] = f"{resolved_bind_key}_session"
            else:
                litestar_cfg["session_key"] = "db_session"
        if "pool_key" not in litestar_cfg:
            p_k = getattr(config, "pool_key", None)
            if p_k:
                litestar_cfg["pool_key"] = str(p_k)
            elif resolved_bind_key and resolved_bind_key not in ("default", "primary"):
                litestar_cfg["pool_key"] = f"{resolved_bind_key}_pool"
            else:
                litestar_cfg["pool_key"] = "db_pool"
        ext_cfg["litestar"] = litestar_cfg
        self.extension_config = ext_cfg

    def _create_pool(self) -> Any:
        """Delegate pool creation to wrapped configuration."""
        fn = getattr(self.config, "_create_pool", None) or getattr(self.config, "create_pool", None)
        return fn() if callable(fn) else None


class ProxySyncConfig[SyncConfigT](_BaseProxyConfig, SyncDatabaseConfig[Any, Any, Any]):
    """Generic synchronous SQLSpec proxy configuration preserving inner SyncConfigT types."""

    __slots__ = ("_statement_cache", "broker", "config", "provider")
    _DEFAULT_DSN: ClassVar[str] = "sqlite:///:memory:"
    is_async: ClassVar[bool] = False
    supports_connection_pooling: ClassVar[bool] = True

    def __init__(
        self,
        config: SyncConfigT,
        *,
        broker: SyncBrokerTransport | SQLSpecSyncProvider | None = None,
        provider: SQLSpecSyncProvider | None = None,
        statement_config: Any = None,
        bind_key: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize ProxySyncConfig wrapping a synchronous database configuration."""
        _inject_sqlspec_pool_defaults(config)
        self.config: SyncConfigT = config
        self.provider: SQLSpecSyncProvider | None
        if isinstance(broker, _BaseSQLSpecProvider):
            self.provider = provider or broker
            self.broker: Any = broker.broker
        else:
            self.provider = provider
            self.broker = broker

        resolved_bind_key = bind_key or getattr(config, "bind_key", "default")
        stmt_cfg = statement_config or getattr(config, "statement_config", None)
        driver_feats = getattr(config, "driver_features", None)
        super().__init__(
            bind_key=resolved_bind_key,
            statement_config=stmt_cfg,
            driver_features=driver_feats,
            **kwargs,
        )
        self._configure_litestar_extension_keys(config, resolved_bind_key)

    def provide_proxied_session(self, *, worker_pid: int | None = None) -> ProxySyncSessionContext:
        """Return a synchronous session context manager backed by SQLSpecSyncProvider and SHM leases."""
        resolved_bind_key = self.bind_key or "default"
        if self.provider is None:
            self.provider = SQLSpecSyncProvider(
                self.config,
                broker=self.broker,
                pool_name=resolved_bind_key,
            )
        return ProxySyncSessionContext(
            self.provider,
            broker=self.broker,
            pool_name=resolved_bind_key,
            worker_pid=worker_pid,
            statement_config=self.statement_config,
            dialect=getattr(self.config, "dialect", None),
        )

    def provide_session(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate session provision to ProxySyncSessionContext when provider is attached, or inner SyncConfigT."""
        _ = (args, kwargs)
        resolved_bind_key = self.bind_key or "default"
        if self.provider is not None:
            return ProxySyncSessionContext(
                self.provider,
                broker=self.broker,
                pool_name=resolved_bind_key,
                statement_config=self.statement_config,
                dialect=getattr(self.config, "dialect", None),
            )
        fn = getattr(self.config, "provide_session", None)
        return fn() if callable(fn) else None

    def derive_pool(self, **overrides: Any) -> ProxySyncConfig[SyncConfigT]:
        """Delegate per-pool configuration cloning to the wrapped SyncConfigT."""
        fn = getattr(self.config, "derive_pool", None)
        if callable(fn):
            return ProxySyncConfig(
                cast("SyncConfigT", fn(**overrides)),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    def derive_direct_config(self) -> ProxySyncConfig[SyncConfigT]:
        """Delegate direct-session configuration derivation to the wrapped SyncConfigT."""
        fn = getattr(self.config, "derive_direct_config", None)
        if callable(fn):
            return ProxySyncConfig(
                cast("SyncConfigT", fn()),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    def _close_pool(self) -> None:
        """Delegate pool close to wrapped configuration."""
        fn = getattr(self.config, "_close_pool", None) or getattr(self.config, "close_pool", None)
        if callable(fn):
            fn()


class ProxyAsyncConfig[AsyncConfigT](_BaseProxyConfig, AsyncDatabaseConfig[Any, Any, Any]):
    """Generic asynchronous SQLSpec proxy configuration preserving inner AsyncConfigT types."""

    __slots__ = ("_statement_cache", "broker", "config", "provider")
    _DEFAULT_DSN: ClassVar[str] = "sqlite+aiosqlite:///:memory:"
    is_async: ClassVar[bool] = True
    supports_connection_pooling: ClassVar[bool] = True

    def __init__(
        self,
        config: AsyncConfigT,
        *,
        broker: AsyncBrokerTransport | SQLSpecAsyncProvider | None = None,
        provider: SQLSpecAsyncProvider | None = None,
        statement_config: Any = None,
        bind_key: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize ProxyAsyncConfig wrapping an asynchronous database configuration."""
        _inject_sqlspec_pool_defaults(config)
        self.config: AsyncConfigT = config
        self.provider: SQLSpecAsyncProvider | None
        if isinstance(broker, _BaseSQLSpecProvider):
            self.provider = provider or broker
            self.broker: Any = broker.broker
        else:
            self.provider = provider
            self.broker = broker

        resolved_bind_key = bind_key or getattr(config, "bind_key", "default")
        stmt_cfg = statement_config or getattr(config, "statement_config", None)
        driver_feats = getattr(config, "driver_features", None)
        super().__init__(
            bind_key=resolved_bind_key,
            statement_config=stmt_cfg,
            driver_features=driver_feats,
            **kwargs,
        )
        self._configure_litestar_extension_keys(config, resolved_bind_key)

    def provide_proxied_session(self, *, worker_pid: int | None = None) -> ProxyAsyncSessionContext:
        """Return an asynchronous session context manager backed by SQLSpecAsyncProvider and SHM leases."""
        resolved_bind_key = self.bind_key or "default"
        if self.provider is None:
            self.provider = SQLSpecAsyncProvider(
                self.config,
                broker=self.broker,
                pool_name=resolved_bind_key,
            )
        return ProxyAsyncSessionContext(
            self.provider,
            broker=self.broker,
            pool_name=resolved_bind_key,
            worker_pid=worker_pid,
            statement_config=self.statement_config,
            dialect=getattr(self.config, "dialect", None),
        )

    def provide_session(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate session provision to ProxyAsyncSessionContext when provider is attached, or inner AsyncConfigT."""
        _ = (args, kwargs)
        resolved_bind_key = self.bind_key or "default"
        if self.provider is not None:
            return ProxyAsyncSessionContext(
                self.provider,
                broker=self.broker,
                pool_name=resolved_bind_key,
                statement_config=self.statement_config,
                dialect=getattr(self.config, "dialect", None),
            )
        fn = getattr(self.config, "provide_session", None)
        return fn() if callable(fn) else None

    def derive_pool(self, **overrides: Any) -> ProxyAsyncConfig[AsyncConfigT]:
        """Delegate per-pool configuration cloning to the wrapped AsyncConfigT."""
        fn = getattr(self.config, "derive_pool", None)
        if callable(fn):
            return ProxyAsyncConfig(
                cast("AsyncConfigT", fn(**overrides)),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    def derive_direct_config(self) -> ProxyAsyncConfig[AsyncConfigT]:
        """Delegate direct-session configuration derivation to the wrapped AsyncConfigT."""
        fn = getattr(self.config, "derive_direct_config", None)
        if callable(fn):
            return ProxyAsyncConfig(
                cast("AsyncConfigT", fn()),
                broker=self.broker,
                provider=self.provider,
            )
        return self

    async def _close_pool(self) -> None:
        """Delegate pool close to wrapped configuration asynchronously."""
        fn = getattr(self.config, "_close_pool", None) or getattr(self.config, "close_pool", None)
        if callable(fn):
            res = fn()
            if isinstance(res, Awaitable):
                await res


def create_sqlspec_plugin(
    *configs: Any,
    config: Any = None,
    store: SyncObjectStore | AsyncObjectStore | None = None,
    storage_alias: str = "vertebra",
    storage_base_path: str = "",
    loader: Any = None,
    observability_config: Any = None,
    **kwargs: Any,
) -> SQLSpecPlugin:
    """Create a real sqlspec.extensions.litestar.SQLSpecPlugin instance with registered configs."""
    _ = kwargs
    if store is not None:
        register_storage_alias(storage_alias, store, base_path=storage_base_path)
    sqlspec_instance = SQLSpec(
        loader=loader,
        observability_config=observability_config,
    )
    all_configs: list[Any] = []
    if configs:
        all_configs.extend(configs)
    if config is not None:
        if isinstance(config, (list, tuple)):
            all_configs.extend(config)
        else:
            all_configs.append(config)
    for cfg in all_configs:
        _inject_sqlspec_pool_defaults(cfg)
        sqlspec_instance.add_config(cfg)
    return SQLSpecPlugin(sqlspec_instance, loader=loader)


def _is_in_memory_dsn(dsn: str | None) -> bool:
    """Return True when DSN targets an in-memory SQLite database."""
    if not dsn:
        return False
    lowered = dsn.lower()
    return ":memory:" in lowered or "mode=memory" in lowered


_CONN_TYPE_ATTR = "connection_type"
_DRIVER_TYPE_ATTR = "driver_type"


def _inject_sqlspec_pool_defaults(config: Any) -> None:
    """Inject production connection pool defaults for persistent databases."""
    supports_pooling = bool(getattr(config, "supports_connection_pooling", True))
    dsn = getattr(config, "dsn", None) or getattr(config, "connection_string", None)
    if not supports_pooling or _is_in_memory_dsn(dsn):
        return
    pool_cfg = getattr(config, "pool_config", None)
    if isinstance(pool_cfg, dict):
        pool_cfg.setdefault("pool_pre_ping", True)
        pool_cfg.setdefault("pool_recycle", 1800)
