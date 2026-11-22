"""MySQL and MariaDB synchronous and asynchronous SQLSpec adapter configurations."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from urllib.parse import parse_qs, urlsplit

from sqlspec.core import StatementConfig

from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, LazyStoreAttr, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.mysql._typing import (
    MySQLAsyncConnectionContext,
    MySQLAsyncSessionContext,
    MySQLConnectionParams,
    MySQLDriverFeatures,
    MySQLPoolParams,
    MySQLSyncConnectionContext,
    MySQLSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.mysql.core import build_connection_config
from vertebra.extensions.sqlspec.adapters.mysql.driver import MySQLAsyncDriver, MySQLSyncDriver
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from uuid import UUID

    from vertebra.db.driver._cloud_auth import DatabaseTokenProvider

__all__ = ("MySQLAsyncConfig", "MySQLSyncConfig")

_UNSET: Any = object()

_POOL_ONLY_KEYS: frozenset[str] = frozenset({"pool_recycle", "share_pool"})
_URL_KEYS: tuple[str, ...] = ("url", "dsn", "conninfo", "connection_string")
_ENDPOINT_KEYS: frozenset[str] = frozenset(
    {"host", "port", "user", "password", "database", "unix_socket", "charset", "ssl_mode"}
)
_DERIVE_META_KEYS: frozenset[str] = frozenset(
    {
        "arrow_threshold_rows",
        "bind_key",
        "broker",
        "connection_config",
        "connection_config_overrides",
        "connection_instance",
        "driver_features",
        "driver_features_overrides",
        "extension_config",
        "is_replica",
        "migration_config",
        "observability_config",
        "replicas",
        "reset_session_on_release",
        "share_pool",
        "statement_config",
    }
)


def _resolve_mysql_config(
    config: MySQLConfig | None,
    *,
    share_pool: bool,
    broker: Any,
    connection_config: Mapping[str, Any] | None,
    driver_features: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
) -> tuple[MySQLConfig, Any]:
    config_broker = getattr(config, "broker", None) if config is not None else None
    if broker is not None:
        resolved_broker = broker
    elif config_broker is not None:
        resolved_broker = config_broker
    elif share_pool:
        resolved_broker = DatabaseRegistry.default_broker()
    else:
        resolved_broker = None
    if config is not None:
        has_retry_features = isinstance(driver_features, Mapping) and any(
            key in driver_features
            for key in ("connector_retry_count", "connector_retry_delay", "connector_retry_max_delay")
        )
        if not connection_config and not kwargs and not has_retry_features:
            return config, resolved_broker
        seed_conn: dict[str, Any] = {f: getattr(config, f) for f in MySQLConfig.__struct_fields__ if f != "dsn"}
        if config.dsn != "mysql://localhost:3306/vertebra":
            seed_conn["url"] = config.dsn
        if has_retry_features and isinstance(driver_features, Mapping):
            for retry_key in ("connector_retry_count", "connector_retry_delay", "connector_retry_max_delay"):
                if retry_key in driver_features:
                    seed_conn[retry_key] = driver_features[retry_key]
        if isinstance(connection_config, Mapping):
            seed_conn.update(connection_config)
        rebuilt = build_connection_config(
            connection_config=seed_conn,
            driver_features=driver_features,
            broker=resolved_broker,
            overrides=kwargs,
        )
        return rebuilt, resolved_broker
    built = build_connection_config(
        connection_config=connection_config,
        driver_features=driver_features,
        broker=resolved_broker,
        overrides=kwargs,
    )
    return built, resolved_broker


def _normalize_url_aliases(source: dict[str, Any]) -> None:
    """Normalize `connection_string` / `conninfo` aliases onto `url` for `build_connection_config`."""
    for alias in ("connection_string", "conninfo"):
        alias_val = source.pop(alias, None)
        if isinstance(alias_val, str) and alias_val.strip() and "url" not in source and "dsn" not in source:
            source["url"] = alias_val


def _prepare_mysql_setup(
    config: MySQLConfig | MySQLConnectionParams | str | None,
    *,
    share_pool: bool | None,
    broker: Any,
    connection_config: MySQLConnectionParams | MySQLPoolParams | Mapping[str, Any] | None,
    driver_features: MySQLDriverFeatures | Mapping[str, Any] | None,
    reset_session_on_release: bool | None,
    kwargs: Mapping[str, Any],
) -> tuple[MySQLConfig, Any, bool, dict[str, Any], dict[str, Any]]:
    """Normalize MySQL connection parameters, pool options, and driver feature flags."""
    raw_conn: dict[str, Any] = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    if isinstance(config, Mapping):
        raw_conn.update(config)
    elif isinstance(config, str) and config.strip():
        raw_conn.setdefault("url", config.strip())

    if share_pool is not None:
        resolved_share_pool = bool(share_pool)
    elif "share_pool" in raw_conn:
        resolved_share_pool = bool(raw_conn["share_pool"])
    else:
        resolved_share_pool = True

    features: dict[str, Any] = dict(driver_features) if isinstance(driver_features, Mapping) else {}
    if reset_session_on_release is not None:
        features["reset_session_on_release"] = bool(reset_session_on_release)

    filtered_kwargs: dict[str, Any] = {}
    for key, value in kwargs.items():
        if key == "reset_session_on_release":
            features.setdefault("reset_session_on_release", bool(value))
        elif key == "use_pure_python_driver":
            features.setdefault("use_pure_python_driver", bool(value))
        elif key in _POOL_ONLY_KEYS or key in {"min_size", "max_size"}:
            raw_conn[key] = value
        else:
            filtered_kwargs[key] = value
            raw_conn[key] = value

    build_conn = {k: v for k, v in raw_conn.items() if k not in _POOL_ONLY_KEYS}
    build_overrides = {k: v for k, v in filtered_kwargs.items() if k not in _POOL_ONLY_KEYS}
    _normalize_url_aliases(build_conn)
    _normalize_url_aliases(build_overrides)

    explicit_cfg = config if isinstance(config, MySQLConfig) else None
    resolved_cfg, resolved_broker = _resolve_mysql_config(
        explicit_cfg,
        share_pool=resolved_share_pool,
        broker=broker,
        connection_config=build_conn,
        driver_features=features,
        kwargs=build_overrides,
    )
    return resolved_cfg, resolved_broker, resolved_share_pool, raw_conn, features


def _build_mysql_replica_overrides(adapter: Any, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
    """Build MySQL replica `derive_pool` overrides from endpoint URLs, host shorthand, or mappings."""
    if isinstance(spec_input, bool):
        msg = f"{type(adapter).__name__} requires explicit replica endpoints or overrides"
        raise ImproperConfigurationError(msg)
    if isinstance(spec_input, str):
        endpoint = spec_input.strip()
        if not endpoint:
            msg = "MySQL replica endpoint must not be empty"
            raise ImproperConfigurationError(msg)
        if "://" not in endpoint:
            endpoint = f"mysql://{endpoint}"
        if not endpoint.startswith(("mysql://", "mariadb://")):
            msg = "MySQL replica endpoint must use mysql:// or mariadb://"
            raise ImproperConfigurationError(msg)
        return {"connection_config_overrides": {"url": endpoint}}

    data = dict(spec_input)
    if "connection_config_overrides" in data or "driver_features_overrides" in data:
        return data
    meta: dict[str, Any] = {}
    conn_overrides: dict[str, Any] = {}
    for key, value in data.items():
        if key in _DERIVE_META_KEYS:
            meta[key] = value
        else:
            conn_overrides[key] = value
    if conn_overrides:
        meta["connection_config_overrides"] = conn_overrides
    return meta


def _inherited_endpoint_fields(driver_config: MySQLConfig) -> dict[str, Any]:
    """Return the endpoint fields a derived pool inherits from its parent `MySQLConfig`.

    Only an SSL mode the caller set explicitly is inherited. An unset mode is left out so the derived
    `MySQLConfig` resolves its own default for the new target and token provider.
    """
    parsed = driver_config._parse_dsn_fields()
    inherited = {
        field_key: parsed[field_key]
        for field_key in ("host", "port", "user", "password", "database", "charset", "unix_socket")
        if parsed.get(field_key) is not None
    }
    explicit_ssl_mode = driver_config.explicit_ssl_mode
    if explicit_ssl_mode is not None:
        inherited["ssl_mode"] = explicit_ssl_mode
    return inherited


def _derive_mysql_adapter(adapter: Any, overrides: Mapping[str, Any]) -> Any:
    """Clone `MySQLSyncConfig` or `MySQLAsyncConfig` with non-mutating connection and feature overrides."""
    merged_features = dict(overrides.get("driver_features", adapter.driver_features))
    merged_features.pop("_statement_cache_holder", None)
    user_hook = getattr(adapter, "_user_connection_hook", None)
    if user_hook is not None and "on_connection_create" not in merged_features:
        merged_features["on_connection_create"] = user_hook
    feature_overrides = overrides.get("driver_features_overrides")
    if isinstance(feature_overrides, Mapping):
        merged_features.update(feature_overrides)
    if "reset_session_on_release" in overrides and overrides["reset_session_on_release"] is not None:
        merged_features["reset_session_on_release"] = bool(overrides["reset_session_on_release"])

    merged_conn: dict[str, Any] = dict(adapter.connection_config)
    override_conn = overrides.get("connection_config")
    conn_overrides = overrides.get("connection_config_overrides")
    extra_conn_updates: dict[str, Any] = {}
    for key, value in overrides.items():
        if key in _DERIVE_META_KEYS:
            continue
        if key in adapter.driver_features or key == "use_pure_python_driver":
            merged_features[key] = value
        else:
            extra_conn_updates[key] = value

    explicit_conn: dict[str, Any] = {}
    if isinstance(override_conn, Mapping):
        explicit_conn.update(override_conn)
    if isinstance(conn_overrides, Mapping):
        explicit_conn.update(conn_overrides)
    explicit_conn.update(extra_conn_updates)

    override_url: str | None = None
    for alias in _URL_KEYS:
        candidate = explicit_conn.get(alias)
        if isinstance(candidate, str) and candidate.strip():
            override_url = candidate.strip()
            break
    has_url_override = override_url is not None
    if has_url_override:
        assert override_url is not None
        for alias in _URL_KEYS:
            merged_conn.pop(alias, None)
        for endpoint_key in _ENDPOINT_KEYS:
            if endpoint_key not in explicit_conn:
                merged_conn.pop(endpoint_key, None)
        override_query = parse_qs(urlsplit(override_url).query)
        url_sets_ssl_ca = any(ca_key in override_query for ca_key in ("ssl_ca", "ssl-ca", "sslca"))
        if "ssl_ca" not in explicit_conn:
            if url_sets_ssl_ca:
                merged_conn.pop("ssl_ca", None)
            elif adapter.driver_config.ssl_ca is not None:
                merged_conn["ssl_ca"] = adapter.driver_config.ssl_ca
        if "token_provider" not in explicit_conn and adapter.driver_config.token_provider is not None:
            merged_conn["token_provider"] = adapter.driver_config.token_provider
    elif any(k in _ENDPOINT_KEYS for k in explicit_conn):
        for alias in _URL_KEYS:
            merged_conn.pop(alias, None)
        base_fields = _inherited_endpoint_fields(adapter.driver_config)
        for field_key, field_value in base_fields.items():
            if field_key not in merged_conn and field_key not in explicit_conn:
                merged_conn[field_key] = field_value
        if "ssl_ca" not in explicit_conn and adapter.driver_config.ssl_ca is not None:
            merged_conn["ssl_ca"] = adapter.driver_config.ssl_ca
        if "token_provider" not in explicit_conn and adapter.driver_config.token_provider is not None:
            merged_conn["token_provider"] = adapter.driver_config.token_provider
    else:
        if not any(k in merged_conn for k in _URL_KEYS) and not any(k in merged_conn for k in _ENDPOINT_KEYS):
            if adapter.driver_config.dsn != "mysql://localhost:3306/vertebra":
                merged_conn["dsn"] = adapter.driver_config.dsn
            else:
                for field_key, field_value in _inherited_endpoint_fields(adapter.driver_config).items():
                    if field_key not in merged_conn:
                        merged_conn[field_key] = field_value
        if "ssl_ca" not in explicit_conn and adapter.driver_config.ssl_ca is not None:
            merged_conn["ssl_ca"] = adapter.driver_config.ssl_ca
        if "token_provider" not in explicit_conn and adapter.driver_config.token_provider is not None:
            merged_conn["token_provider"] = adapter.driver_config.token_provider
    merged_conn.update(explicit_conn)
    if isinstance(feature_overrides, Mapping):
        for retry_key in ("connector_retry_count", "connector_retry_delay", "connector_retry_max_delay"):
            if retry_key in feature_overrides and retry_key not in explicit_conn:
                merged_conn.pop(retry_key, None)
    for retry_key in ("connector_retry_count", "connector_retry_delay", "connector_retry_max_delay"):
        if retry_key not in explicit_conn and retry_key not in merged_features and retry_key not in merged_conn:
            merged_conn[retry_key] = getattr(adapter.driver_config, retry_key)

    return type(adapter)(
        None,
        bind_key=str(overrides.get("bind_key", adapter.bind_key)),
        broker=overrides.get("broker", adapter.broker),
        share_pool=bool(overrides.get("share_pool", adapter.share_pool)),
        arrow_threshold_rows=int(overrides.get("arrow_threshold_rows", adapter.arrow_threshold_rows)),
        statement_config=overrides.get("statement_config", adapter.statement_config),
        connection_config=merged_conn,
        migration_config=(
            dict(overrides["migration_config"])
            if isinstance(overrides.get("migration_config"), Mapping)
            else dict(adapter.migration_config)
            if isinstance(adapter.migration_config, Mapping)
            else adapter.migration_config
        ),
        driver_features=merged_features,
        extension_config=dict(overrides.get("extension_config", adapter.extension_config)),
        observability_config=overrides.get("observability_config", adapter.observability_config),
        replicas=None,
        is_replica=bool(overrides.get("is_replica", False)),
    )


class MySQLSyncConfig(SyncAdapterConfig[MySQLConfig, SyncMySQLDriver, SyncMySQLDriver, MySQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncMySQLDriver`."""

    __slots__ = ("_initialized_conn_ids", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_mysql"
    dialect: ClassVar[str] = "mysql"
    connection_type: ClassVar[type[Any]] = SyncMySQLDriver
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[SyncMySQLDriver]] = SyncMySQLDriver
    driver_type: ClassVar[type[MySQLSyncDriver]] = MySQLSyncDriver
    _connection_context_class: ClassVar[type[Any]] = MySQLSyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = MySQLSyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.adk",
        "MySQLSyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.adk",
        "MySQLSyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.adk",
        "MySQLSyncADKArtifactStore",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.events",
        "MySQLEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.litestar",
        "MySQLLitestarStore",
    )

    def __init__(
        self,
        config: MySQLConfig | MySQLConnectionParams | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: MySQLConnectionParams | MySQLPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: MySQLDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        token_provider: DatabaseTokenProvider | None = _UNSET,
        ssl_ca: str | None = _UNSET,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous MySQL SQLSpec configuration supporting URLs, replicas, and session reset."""
        if token_provider is not _UNSET:
            kwargs["token_provider"] = token_provider
        if ssl_ca is not _UNSET:
            kwargs["ssl_ca"] = ssl_ca
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features = _prepare_mysql_setup(
            config,
            share_pool=share_pool,
            broker=broker,
            connection_config=connection_config,
            driver_features=driver_features,
            reset_session_on_release=reset_session_on_release,
            kwargs=kwargs,
        )
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = features.pop("on_connection_create", None)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=normalized_conn,
            connection_instance=connection_instance,
            migration_config=migration_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractContextManager[MySQLSyncDriver]:
        """Yield a fresh synchronous MySQL SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractContextManager[SyncMySQLDriver]:
        """Yield a synchronous native MySQL driver context forwarding positional and keyword options."""
        return cast("AbstractContextManager[SyncMySQLDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build MySQL replica overrides from endpoint URLs, host shorthand, or explicit dictionaries."""
        return _build_mysql_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous MySQL configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_mysql_adapter(self, overrides))

    def _close_pool(self) -> None:
        """Close the synchronous MySQL pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return MySQL adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "MySQLConfig": MySQLConfig,
                "MySQLConnectionParams": MySQLConnectionParams,
                "MySQLDriverFeatures": MySQLDriverFeatures,
                "MySQLPoolParams": MySQLPoolParams,
                "MySQLSyncConnectionContext": MySQLSyncConnectionContext,
                "MySQLSyncDriver": MySQLSyncDriver,
                "MySQLSyncSessionContext": MySQLSyncSessionContext,
                "SyncMySQLDriver": SyncMySQLDriver,
            }
        )
        return namespace


class MySQLAsyncConfig(AsyncAdapterConfig[MySQLConfig, AsyncMySQLDriver, AsyncMySQLDriver, MySQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncMySQLDriver`."""

    __slots__ = ("_initialized_conn_ids", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_mysql"
    dialect: ClassVar[str] = "mysql"
    connection_type: ClassVar[type[Any]] = AsyncMySQLDriver
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[AsyncMySQLDriver]] = AsyncMySQLDriver
    driver_type: ClassVar[type[MySQLAsyncDriver]] = MySQLAsyncDriver
    _connection_context_class: ClassVar[type[Any]] = MySQLAsyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = MySQLAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.adk",
        "MySQLAsyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.adk",
        "MySQLAsyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.adk",
        "MySQLAsyncADKArtifactStore",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.events",
        "MySQLEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mysql.litestar",
        "MySQLLitestarStore",
    )

    def __init__(
        self,
        config: MySQLConfig | MySQLConnectionParams | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: MySQLConnectionParams | MySQLPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: MySQLDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        token_provider: DatabaseTokenProvider | None = _UNSET,
        ssl_ca: str | None = _UNSET,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous MySQL SQLSpec configuration supporting URLs, replicas, and session reset."""
        if token_provider is not _UNSET:
            kwargs["token_provider"] = token_provider
        if ssl_ca is not _UNSET:
            kwargs["ssl_ca"] = ssl_ca
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features = _prepare_mysql_setup(
            config,
            share_pool=share_pool,
            broker=broker,
            connection_config=connection_config,
            driver_features=driver_features,
            reset_session_on_release=reset_session_on_release,
            kwargs=kwargs,
        )
        self._initialized_conn_ids: set[int] = set()
        self._user_connection_hook: Any = features.pop("on_connection_create", None)
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=resolved_share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=normalized_conn,
            connection_instance=connection_instance,
            migration_config=migration_config,
            driver_features=features,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )
        if self._user_connection_hook is not None:
            self.driver_features["on_connection_create"] = self._user_connection_hook

    def provide_session(
        self,
        *args: Any,
        statement_config: StatementConfig | None = None,
        tenant: str | UUID | Any | None = None,
        bypass_tenant: bool = False,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[MySQLAsyncDriver]:
        """Yield a fresh asynchronous MySQL SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractAsyncContextManager[AsyncMySQLDriver]:
        """Yield an asynchronous native MySQL driver context forwarding positional and keyword options."""
        return cast("AbstractAsyncContextManager[AsyncMySQLDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build MySQL replica overrides from endpoint URLs, host shorthand, or explicit dictionaries."""
        return _build_mysql_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous MySQL configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_mysql_adapter(self, overrides))

    async def _close_pool(self) -> None:
        """Close the asynchronous MySQL pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        await super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return MySQL adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "AsyncMySQLDriver": AsyncMySQLDriver,
                "MySQLAsyncConnectionContext": MySQLAsyncConnectionContext,
                "MySQLAsyncDriver": MySQLAsyncDriver,
                "MySQLAsyncSessionContext": MySQLAsyncSessionContext,
                "MySQLConfig": MySQLConfig,
                "MySQLConnectionParams": MySQLConnectionParams,
                "MySQLDriverFeatures": MySQLDriverFeatures,
                "MySQLPoolParams": MySQLPoolParams,
            }
        )
        return namespace
