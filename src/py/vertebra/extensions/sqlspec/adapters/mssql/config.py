"""SQL Server (MSSQL) synchronous and asynchronous SQLSpec adapter configurations."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager, AbstractContextManager
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast

from sqlspec.core import StatementConfig

from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, LazyStoreAttr, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.mssql._typing import (
    MSSQLAsyncConnectionContext,
    MSSQLAsyncSessionContext,
    MSSQLConnectionParams,
    MSSQLDriverFeatures,
    MSSQLPoolParams,
    MSSQLSyncConnectionContext,
    MSSQLSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.mssql.core import build_connection_config
from vertebra.extensions.sqlspec.adapters.mssql.driver import MSSQLAsyncDriver, MSSQLSyncDriver
from vertebra.typing import ImproperConfigurationError

if TYPE_CHECKING:
    from uuid import UUID

__all__ = ("MSSQLAsyncConfig", "MSSQLSyncConfig")

_POOL_ONLY_KEYS: frozenset[str] = frozenset({"pool_recycle", "share_pool", "min_size", "max_size"})
_URL_KEYS: tuple[str, ...] = ("url", "dsn", "conninfo", "connection_string")
_ENDPOINT_KEYS: frozenset[str] = frozenset(
    {
        "addr",
        "address",
        "application_intent",
        "application_name",
        "applicationintent",
        "appname",
        "database",
        "db",
        "encrypt",
        "encryption",
        "host",
        "hostname_in_certificate",
        "hostnameincertificate",
        "password",
        "port",
        "pwd",
        "read_only_intent",
        "server",
        "server_name",
        "trust",
        "trust_server_certificate",
        "uid",
        "user",
        "username",
    }
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


def _resolve_mssql_config(
    config: MSSQLConfig | None,
    *,
    share_pool: bool,
    broker: Any,
    connection_config: Mapping[str, Any] | None,
    driver_features: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
) -> tuple[MSSQLConfig, Any]:
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
        return config, resolved_broker
    built = build_connection_config(
        connection_config=connection_config,
        driver_features=driver_features,
        broker=resolved_broker,
        overrides=kwargs,
    )
    return built, resolved_broker


def _is_odbc_connection_string(value: str) -> bool:
    """Return whether `value` looks like a semicolon-delimited ODBC connection string."""
    stripped = value.strip()
    return ";" in stripped and "=" in stripped and "://" not in stripped


def _normalize_url_aliases(source: dict[str, Any]) -> None:
    """Normalize `conninfo` and URL-formatted `connection_string` onto `url` while preserving ODBC strings."""
    conninfo_val = source.pop("conninfo", None)
    if isinstance(conninfo_val, str) and conninfo_val.strip() and "url" not in source and "dsn" not in source:
        if _is_odbc_connection_string(conninfo_val):
            source.setdefault("connection_string", conninfo_val.strip())
        else:
            source["url"] = conninfo_val.strip()

    conn_str = source.get("connection_string")
    if (
        isinstance(conn_str, str)
        and conn_str.strip()
        and not _is_odbc_connection_string(conn_str)
        and "://" in conn_str
        and "url" not in source
        and "dsn" not in source
    ):
        source["url"] = source.pop("connection_string").strip()


def _prepare_mssql_setup(
    config: MSSQLConfig | MSSQLConnectionParams | str | None,
    *,
    share_pool: bool | None,
    broker: Any,
    connection_config: MSSQLConnectionParams | MSSQLPoolParams | Mapping[str, Any] | None,
    driver_features: MSSQLDriverFeatures | Mapping[str, Any] | None,
    reset_session_on_release: bool | None,
    kwargs: Mapping[str, Any],
) -> tuple[MSSQLConfig, Any, bool, dict[str, Any], dict[str, Any]]:
    """Normalize SQL Server connection parameters, pool options, and driver feature flags."""
    raw_conn: dict[str, Any] = dict(connection_config) if isinstance(connection_config, Mapping) else {}
    if isinstance(config, Mapping):
        raw_conn.update(config)
    elif isinstance(config, str) and config.strip():
        stripped = config.strip()
        if _is_odbc_connection_string(stripped):
            raw_conn.setdefault("connection_string", stripped)
        else:
            raw_conn.setdefault("url", stripped)

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
        elif key in _POOL_ONLY_KEYS:
            raw_conn[key] = value
        else:
            filtered_kwargs[key] = value
            raw_conn[key] = value

    build_conn = {k: v for k, v in raw_conn.items() if k not in _POOL_ONLY_KEYS}
    build_overrides = {k: v for k, v in filtered_kwargs.items() if k not in _POOL_ONLY_KEYS}
    if "min_size" in raw_conn and "pool_min_size" not in build_conn:
        build_conn["pool_min_size"] = int(raw_conn["min_size"])
    if "max_size" in raw_conn and "pool_max_size" not in build_conn:
        build_conn["pool_max_size"] = int(raw_conn["max_size"])
    build_conn.pop("driver", None)
    build_overrides.pop("driver", None)
    _normalize_url_aliases(build_conn)
    _normalize_url_aliases(build_overrides)

    explicit_cfg = config if isinstance(config, MSSQLConfig) else None
    resolved_cfg, resolved_broker = _resolve_mssql_config(
        explicit_cfg,
        share_pool=resolved_share_pool,
        broker=broker,
        connection_config=build_conn,
        driver_features=features,
        kwargs=build_overrides,
    )
    return resolved_cfg, resolved_broker, resolved_share_pool, raw_conn, features


def _build_mssql_replica_overrides(adapter: Any, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
    """Build SQL Server replica `derive_pool` overrides from endpoint URLs, ODBC strings, or mappings."""
    if isinstance(spec_input, bool):
        msg = f"{type(adapter).__name__} requires explicit replica endpoints or overrides"
        raise ImproperConfigurationError(msg)
    if isinstance(spec_input, str):
        endpoint = spec_input.strip()
        if not endpoint:
            msg = "MSSQL replica endpoint must not be empty"
            raise ImproperConfigurationError(msg)
        if _is_odbc_connection_string(endpoint):
            return {"connection_config_overrides": {"connection_string": endpoint}}
        if "://" not in endpoint:
            endpoint = f"mssql://{endpoint}"
        if not endpoint.startswith(("mssql://", "sqlserver://")):
            msg = "MSSQL replica endpoint must use mssql://, sqlserver://, or an ODBC connection string"
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


def _derive_mssql_adapter(adapter: Any, overrides: Mapping[str, Any]) -> Any:
    """Clone `MSSQLSyncConfig` or `MSSQLAsyncConfig` with non-mutating connection and feature overrides."""
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
    if isinstance(override_conn, Mapping):
        merged_conn.update(override_conn)

    conn_overrides = overrides.get("connection_config_overrides")
    extra_conn_updates: dict[str, Any] = dict(conn_overrides) if isinstance(conn_overrides, Mapping) else {}
    for key, value in overrides.items():
        if key in _DERIVE_META_KEYS:
            continue
        if key in adapter.driver_features:
            merged_features[key] = value
        else:
            extra_conn_updates[key] = value

    has_url_override = any(
        isinstance(extra_conn_updates.get(alias), str) and extra_conn_updates[alias].strip() for alias in _URL_KEYS
    )
    if has_url_override:
        for alias in _URL_KEYS:
            merged_conn.pop(alias, None)
        for endpoint_key in _ENDPOINT_KEYS:
            if endpoint_key not in extra_conn_updates:
                merged_conn.pop(endpoint_key, None)
    elif any(k in _ENDPOINT_KEYS for k in extra_conn_updates):
        for alias in _URL_KEYS:
            merged_conn.pop(alias, None)
        base_cfg = adapter.driver_config
        for field_key in (
            "host",
            "port",
            "user",
            "password",
            "database",
            "encryption",
            "trust_server_certificate",
            "server_name",
            "read_only_intent",
            "application_name",
            "connect_timeout_ms",
            "query_timeout_ms",
            "cloud_sql_instance",
            "cloudsql_ip_type",
            "cloudsql_auth_type",
            "cloudsql_proxy_addr",
            "token_provider",
        ):
            if field_key not in merged_conn and field_key not in extra_conn_updates:
                base_val = getattr(base_cfg, field_key, None)
                if base_val is not None:
                    merged_conn[field_key] = base_val
    merged_conn.update(extra_conn_updates)

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


class MSSQLSyncConfig(SyncAdapterConfig[MSSQLConfig, SyncMSSQLDriver, SyncMSSQLDriver, MSSQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncMSSQLDriver`."""

    __slots__ = ("_initialized_conn_ids", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_mssql"
    dialect: ClassVar[str] = "tsql"
    connection_type: ClassVar[type[Any]] = SyncMSSQLDriver
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[SyncMSSQLDriver]] = SyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLSyncDriver]] = MSSQLSyncDriver
    _connection_context_class: ClassVar[type[Any]] = MSSQLSyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = MSSQLSyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.adk",
        "MSSQLSyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.adk",
        "MSSQLSyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.adk",
        "MSSQLSyncADKArtifactStore",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.events",
        "MSSQLEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.litestar",
        "MSSQLLitestarStore",
    )

    def __init__(
        self,
        config: MSSQLConfig | MSSQLConnectionParams | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: MSSQLConnectionParams | MSSQLPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: MSSQLDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous SQL Server SQLSpec configuration supporting URLs, ODBC strings, and replicas."""
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features = _prepare_mssql_setup(
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
    ) -> AbstractContextManager[MSSQLSyncDriver]:
        """Yield a fresh synchronous SQL Server SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractContextManager[SyncMSSQLDriver]:
        """Yield a synchronous native SQL Server driver context forwarding positional and keyword options."""
        return cast("AbstractContextManager[SyncMSSQLDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build SQL Server replica overrides from endpoint URLs, ODBC strings, or explicit dictionaries."""
        return _build_mssql_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous SQL Server configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_mssql_adapter(self, overrides))

    def _close_pool(self) -> None:
        """Close the synchronous SQL Server pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return SQL Server adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "MSSQLConfig": MSSQLConfig,
                "MSSQLConnectionParams": MSSQLConnectionParams,
                "MSSQLDriverFeatures": MSSQLDriverFeatures,
                "MSSQLPoolParams": MSSQLPoolParams,
                "MSSQLSyncConnectionContext": MSSQLSyncConnectionContext,
                "MSSQLSyncDriver": MSSQLSyncDriver,
                "MSSQLSyncSessionContext": MSSQLSyncSessionContext,
                "SyncMSSQLDriver": SyncMSSQLDriver,
            }
        )
        return namespace


class MSSQLAsyncConfig(AsyncAdapterConfig[MSSQLConfig, AsyncMSSQLDriver, AsyncMSSQLDriver, MSSQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncMSSQLDriver`."""

    __slots__ = ("_initialized_conn_ids", "_user_connection_hook")

    adapter_name: ClassVar[str | None] = "vertebra_mssql"
    dialect: ClassVar[str] = "tsql"
    connection_type: ClassVar[type[Any]] = AsyncMSSQLDriver
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[AsyncMSSQLDriver]] = AsyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLAsyncDriver]] = MSSQLAsyncDriver
    _connection_context_class: ClassVar[type[Any]] = MSSQLAsyncConnectionContext
    _session_context_class: ClassVar[type[Any]] = MSSQLAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.adk",
        "MSSQLAsyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.adk",
        "MSSQLAsyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.adk",
        "MSSQLAsyncADKArtifactStore",
    )
    events_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.events",
        "MSSQLEventQueueStore",
    )
    litestar_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.mssql.litestar",
        "MSSQLLitestarStore",
    )

    def __init__(
        self,
        config: MSSQLConfig | MSSQLConnectionParams | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool | None = None,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: MSSQLConnectionParams | MSSQLPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: MSSQLDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        reset_session_on_release: bool | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous SQL Server SQLSpec configuration supporting URLs, ODBC strings, and replicas."""
        resolved_cfg, resolved_broker, resolved_share_pool, normalized_conn, features = _prepare_mssql_setup(
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
    ) -> AbstractAsyncContextManager[MSSQLAsyncDriver]:
        """Yield a fresh asynchronous SQL Server SQLSpec session forwarding positional and keyword options."""
        return super().provide_session(
            *args,
            statement_config=statement_config,
            tenant=tenant,
            bypass_tenant=bypass_tenant,
            **kwargs,
        )

    def provide_connection(self, *args: Any, **kwargs: Any) -> AbstractAsyncContextManager[AsyncMSSQLDriver]:
        """Yield an asynchronous native SQL Server driver context forwarding positional and keyword options."""
        return cast("AbstractAsyncContextManager[AsyncMSSQLDriver]", super().provide_connection(*args, **kwargs))

    def _build_replica_spec_overrides(self, spec_input: str | bool | Mapping[str, Any]) -> dict[str, Any]:
        """Build SQL Server replica overrides from endpoint URLs, ODBC strings, or explicit dictionaries."""
        return _build_mssql_replica_overrides(self, spec_input)

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous SQL Server configuration with non-mutating connection or feature overrides."""
        return cast("Self", _derive_mssql_adapter(self, overrides))

    async def _close_pool(self) -> None:
        """Close the asynchronous SQL Server pool and clear tracked connection initialization IDs."""
        self._initialized_conn_ids.clear()
        await super()._close_pool()

    def get_signature_namespace(self) -> dict[str, Any]:
        """Return SQL Server adapter types registered in Litestar's DI signature namespace."""
        namespace = super().get_signature_namespace()
        namespace.update(
            {
                "AsyncMSSQLDriver": AsyncMSSQLDriver,
                "MSSQLAsyncConnectionContext": MSSQLAsyncConnectionContext,
                "MSSQLAsyncDriver": MSSQLAsyncDriver,
                "MSSQLAsyncSessionContext": MSSQLAsyncSessionContext,
                "MSSQLConfig": MSSQLConfig,
                "MSSQLConnectionParams": MSSQLConnectionParams,
                "MSSQLDriverFeatures": MSSQLDriverFeatures,
                "MSSQLPoolParams": MSSQLPoolParams,
            }
        )
        return namespace
