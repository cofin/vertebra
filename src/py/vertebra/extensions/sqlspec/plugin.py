"""Application plugin and PEP 695 generic configurations for SQLSpec integration."""

from __future__ import annotations

import contextlib
import copy
import logging
from collections.abc import AsyncIterator, Awaitable, Iterator, Mapping
from contextlib import asynccontextmanager, contextmanager
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

type SyncBrokerTransport = SyncSHMBrokerTransport | SyncBrokerLeaseSource
type AsyncBrokerTransport = AsyncSHMBrokerTransport | AsyncBrokerLeaseSource

logger = logging.getLogger(__name__)

SQLSPEC_PLUGIN_PRIORITY: int = -5800
SQLSPEC_STATE_KEY: str = "sqlspec"


class _ForwardedConfigTypeAttr:
    """Descriptor forwarding class or instance type attributes to a wrapped configuration."""

    __slots__ = ("_attr",)

    def __init__(self, attr: str) -> None:
        self._attr = attr

    def __get__(self, instance: Any, owner: type[Any] | None = None) -> type[Any]:
        _ = owner
        if instance is None:
            return cast("type[Any]", Any)
        return cast("type[Any]", getattr(instance.config, self._attr, Any))

    def __set__(self, instance: Any, value: type[Any]) -> None:
        with contextlib.suppress(AttributeError):
            setattr(instance.config, self._attr, value)


def _deep_merge_dicts(base: Mapping[str, Any], override: Mapping[str, Any]) -> dict[str, Any]:
    """Recursively deep-merge two mappings without mutating either input."""
    merged: dict[str, Any] = {}
    for key, value in base.items():
        merged[key] = copy.deepcopy(value)
    for key, value in override.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _deep_merge_dicts(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


class _BaseProxyConfig:
    """Shared Litestar extension key normalization and forwarding properties for SQLSpec proxy configurations."""

    __slots__ = ()
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
    def pool_key(self) -> str:
        """Return configured pool key."""
        return str(
            self.extension_config.get("litestar", {}).get(
                "pool_key",
                getattr(self.config, "pool_key", "db_pool"),
            )
        )

    connection_type = _ForwardedConfigTypeAttr("connection_type")
    driver_type = _ForwardedConfigTypeAttr("driver_type")

    @property
    def pool_config(self) -> dict[str, Any]:
        """Return pool configuration dictionary."""
        return getattr(self.config, "pool_config", {})

    @property
    def has_replicas(self) -> bool:
        """Return whether this proxy or its wrapped configuration owns replica groups."""
        return bool(getattr(self, "_replica_groups", None)) or bool(getattr(self.config, "has_replicas", False))

    @property
    def replica_groups(self) -> Mapping[str, Any]:
        """Return named replica groups from this proxy or the wrapped configuration."""
        if getattr(self, "_replica_groups", None):
            return cast("Mapping[str, Any]", cast("Any", super()).replica_groups)
        wrapped_groups = getattr(self.config, "replica_groups", None)
        if isinstance(wrapped_groups, Mapping):
            return wrapped_groups
        return cast("Mapping[str, Any]", cast("Any", super()).replica_groups)

    @property
    def supports_read_routing(self) -> bool:
        """Return whether replica read routing is available on this proxy or wrapped configuration."""
        if getattr(self, "_replica_groups", None):
            return True
        return bool(getattr(self.config, "supports_read_routing", False))

    def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
        """Delegate connection provision to the wrapped configuration."""
        fn = getattr(self.config, "provide_connection", None)
        return fn(*args, **kwargs) if callable(fn) else None

    def provide_replica_session(
        self,
        *args: Any,
        replica_group: str | None = None,
        statement_config: Any = None,
        tenant: Any = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Provide a replica session from local replica groups or delegate to the wrapped configuration."""
        if getattr(self, "_replica_groups", None):
            return cast("Any", super()).provide_replica_session(
                *args,
                replica_group=replica_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            )
        fn = getattr(self.config, "provide_replica_session", None)
        if callable(fn):
            return fn(
                *args,
                replica_group=replica_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            )
        return cast("Any", super()).provide_replica_session(
            *args,
            replica_group=replica_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_replica_connection(self, *args: Any, replica_group: str | None = None, **kwargs: Any) -> Any:
        """Provide a replica connection from local replica groups or delegate to the wrapped configuration."""
        if getattr(self, "_replica_groups", None):
            return cast("Any", super()).provide_replica_connection(*args, replica_group=replica_group, **kwargs)
        fn = getattr(self.config, "provide_replica_connection", None)
        if callable(fn):
            return fn(*args, replica_group=replica_group, **kwargs)
        return cast("Any", super()).provide_replica_connection(*args, replica_group=replica_group, **kwargs)

    def provide_routing_session(
        self,
        *args: Any,
        replica_group: str | None = None,
        statement_config: Any = None,
        tenant: Any = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> Any:
        """Provide a read/write routing session using proxied replica groups or wrapped configuration."""
        if (
            getattr(self, "_replica_groups", None)
            or getattr(self, "provider", None) is not None
            or getattr(self, "broker", None) is not None
        ):
            return cast("Any", super()).provide_routing_session(
                *args,
                replica_group=replica_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            )
        fn = getattr(self.config, "provide_routing_session", None)
        if callable(fn):
            return fn(
                *args,
                replica_group=replica_group,
                statement_config=statement_config,
                tenant=tenant,
                bypass_tenant=bypass_tenant,
                **kwargs,
            )
        return cast("Any", super()).provide_routing_session(
            *args,
            replica_group=replica_group,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def create_pool(self) -> Any:
        """Delegate pool creation to the wrapped configuration."""
        fn = getattr(self.config, "create_pool", None)
        return fn() if callable(fn) else None

    def __getattr__(self, name: str) -> Any:
        """Forward attribute lookups to the underlying configuration instance."""
        return getattr(self.config, name)

    @staticmethod
    def _build_merged_extension_config(
        config: Any,
        caller_extension_config: Mapping[str, Any] | None,
        resolved_bind_key: str | None,
    ) -> dict[str, Any]:
        """Deep-merge wrapped and caller extension_config without mutating inputs, filling missing Litestar defaults."""
        base_raw = getattr(config, "extension_config", None)
        base_cfg: Mapping[str, Any] = base_raw if isinstance(base_raw, Mapping) else {}
        caller_cfg: Mapping[str, Any] = caller_extension_config if isinstance(caller_extension_config, Mapping) else {}
        ext_cfg = _deep_merge_dicts(base_cfg, caller_cfg)
        litestar_raw = ext_cfg.get("litestar")
        litestar_cfg: dict[str, Any] = dict(litestar_raw) if isinstance(litestar_raw, Mapping) else {}
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
        return ext_cfg

    def _create_pool(self) -> Any:
        """Delegate pool creation to wrapped configuration."""
        fn = getattr(self.config, "_create_pool", None) or getattr(self.config, "create_pool", None)
        return fn() if callable(fn) else None


class ProxySyncConfig[SyncConfigT](_BaseProxyConfig, SyncDatabaseConfig[Any, Any, Any]):
    """Generic synchronous SQLSpec proxy configuration preserving inner SyncConfigT types."""

    __slots__ = ("broker", "config", "provider")
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
        extension_config: Mapping[str, Any] | None = None,
        replicas: Any = None,
        is_replica: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize ProxySyncConfig wrapping a synchronous database configuration."""
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
        driver_feats = kwargs.pop("driver_features", None)
        if driver_feats is None:
            driver_feats = getattr(config, "driver_features", None)
        merged_ext_cfg = self._build_merged_extension_config(config, extension_config, resolved_bind_key)
        resolved_is_replica = bool(getattr(config, "is_replica", False)) if is_replica is None else bool(is_replica)
        super().__init__(
            bind_key=resolved_bind_key,
            statement_config=stmt_cfg,
            driver_features=driver_feats,
            extension_config=cast("Any", merged_ext_cfg),
            replicas=replicas,
            is_replica=resolved_is_replica,
            **kwargs,
        )

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
        """Delegate session provision to ProxySyncSessionContext when proxied, or inner SyncConfigT."""
        resolved_bind_key = self.bind_key or "default"
        if self.provider is not None or self.broker is not None:
            if self.provider is None:
                self.provider = SQLSpecSyncProvider(
                    self.config,
                    broker=self.broker,
                    pool_name=resolved_bind_key,
                )
            worker_pid = kwargs.get("worker_pid")
            stmt_cfg = kwargs.get("statement_config") or self.statement_config
            return ProxySyncSessionContext(
                self.provider,
                broker=self.broker,
                pool_name=resolved_bind_key,
                worker_pid=worker_pid,
                statement_config=stmt_cfg,
                dialect=getattr(self.config, "dialect", None),
            )
        fn = getattr(self.config, "provide_session", None)
        return fn(*args, **kwargs) if callable(fn) else None

    def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
        """Acquire a broker-leased connection via provide_session when proxied, or delegate to inner SyncConfigT."""
        if self.provider is not None or self.broker is not None:

            @contextmanager
            def _leased_connection() -> Iterator[Any]:
                with self.provide_session(*args, **kwargs) as driver:
                    yield getattr(driver, "connection", driver)

            return _leased_connection()
        fn = getattr(self.config, "provide_connection", None)
        return fn(*args, **kwargs) if callable(fn) else None

    def derive_pool(self, **overrides: Any) -> ProxySyncConfig[SyncConfigT]:
        """Delegate per-pool configuration cloning to the wrapped SyncConfigT."""
        fn = getattr(self.config, "derive_pool", None)
        if callable(fn):
            derived = cast("SyncConfigT", fn(**overrides))
            return ProxySyncConfig(
                derived,
                broker=self.broker,
                provider=self.provider,
                extension_config=overrides.get("extension_config", self.extension_config),
                is_replica=bool(overrides.get("is_replica", getattr(derived, "is_replica", False))),
            )
        if overrides:
            return ProxySyncConfig(
                self.config,
                broker=self.broker,
                provider=self.provider,
                bind_key=overrides.get("bind_key", self.bind_key),
                extension_config=overrides.get("extension_config", self.extension_config),
                is_replica=bool(overrides.get("is_replica", self.is_replica)),
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
                extension_config=self.extension_config,
            )
        return self

    def _close_pool(self) -> None:
        """Delegate pool close to wrapped configuration."""
        fn = getattr(self.config, "_close_pool", None) or getattr(self.config, "close_pool", None)
        if callable(fn):
            fn()


class ProxyAsyncConfig[AsyncConfigT](_BaseProxyConfig, AsyncDatabaseConfig[Any, Any, Any]):
    """Generic asynchronous SQLSpec proxy configuration preserving inner AsyncConfigT types."""

    __slots__ = ("broker", "config", "provider")
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
        extension_config: Mapping[str, Any] | None = None,
        replicas: Any = None,
        is_replica: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize ProxyAsyncConfig wrapping an asynchronous database configuration."""
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
        driver_feats = kwargs.pop("driver_features", None)
        if driver_feats is None:
            driver_feats = getattr(config, "driver_features", None)
        merged_ext_cfg = self._build_merged_extension_config(config, extension_config, resolved_bind_key)
        resolved_is_replica = bool(getattr(config, "is_replica", False)) if is_replica is None else bool(is_replica)
        super().__init__(
            bind_key=resolved_bind_key,
            statement_config=stmt_cfg,
            driver_features=driver_feats,
            extension_config=cast("Any", merged_ext_cfg),
            replicas=replicas,
            is_replica=resolved_is_replica,
            **kwargs,
        )

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
        """Delegate session provision to ProxyAsyncSessionContext when proxied, or inner AsyncConfigT."""
        resolved_bind_key = self.bind_key or "default"
        if self.provider is not None or self.broker is not None:
            if self.provider is None:
                self.provider = SQLSpecAsyncProvider(
                    self.config,
                    broker=self.broker,
                    pool_name=resolved_bind_key,
                )
            worker_pid = kwargs.get("worker_pid")
            stmt_cfg = kwargs.get("statement_config") or self.statement_config
            return ProxyAsyncSessionContext(
                self.provider,
                broker=self.broker,
                pool_name=resolved_bind_key,
                worker_pid=worker_pid,
                statement_config=stmt_cfg,
                dialect=getattr(self.config, "dialect", None),
            )
        fn = getattr(self.config, "provide_session", None)
        return fn(*args, **kwargs) if callable(fn) else None

    def provide_connection(self, *args: Any, **kwargs: Any) -> Any:
        """Acquire a broker-leased async connection via provide_session when proxied, or delegate to inner AsyncConfigT."""
        if self.provider is not None or self.broker is not None:

            @asynccontextmanager
            async def _leased_connection() -> AsyncIterator[Any]:
                async with self.provide_session(*args, **kwargs) as driver:
                    yield getattr(driver, "connection", driver)

            return _leased_connection()
        fn = getattr(self.config, "provide_connection", None)
        return fn(*args, **kwargs) if callable(fn) else None

    def derive_pool(self, **overrides: Any) -> ProxyAsyncConfig[AsyncConfigT]:
        """Delegate per-pool configuration cloning to the wrapped AsyncConfigT."""
        fn = getattr(self.config, "derive_pool", None)
        if callable(fn):
            derived = cast("AsyncConfigT", fn(**overrides))
            return ProxyAsyncConfig(
                derived,
                broker=self.broker,
                provider=self.provider,
                extension_config=overrides.get("extension_config", self.extension_config),
                is_replica=bool(overrides.get("is_replica", getattr(derived, "is_replica", False))),
            )
        if overrides:
            return ProxyAsyncConfig(
                self.config,
                broker=self.broker,
                provider=self.provider,
                bind_key=overrides.get("bind_key", self.bind_key),
                extension_config=overrides.get("extension_config", self.extension_config),
                is_replica=bool(overrides.get("is_replica", self.is_replica)),
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
                extension_config=self.extension_config,
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
        sqlspec_instance.add_config(cfg)
    return SQLSpecPlugin(sqlspec_instance, loader=loader)
