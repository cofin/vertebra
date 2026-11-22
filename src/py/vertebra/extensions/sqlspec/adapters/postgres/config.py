"""PostgreSQL synchronous and asynchronous SQLSpec adapter configuration classes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar, Self, cast
from urllib.parse import quote

import msgspec
from sqlspec.config import ReplicaGroup
from sqlspec.core.routing import ReplicaSpec

from vertebra.db.adapters.postgres import AsyncPostgreSQLDriver, PostgreSQLConfig, SyncPostgreSQLDriver
from vertebra.db.adapters.postgres.options import parse_postgres_dsn
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.dialer._base import (
    DEFAULT_CONNECTOR_RETRY_COUNT,
    DEFAULT_CONNECTOR_RETRY_DELAY,
    DEFAULT_CONNECTOR_RETRY_MAX_DELAY,
    validate_connector_retry_config,
)
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, LazyStoreAttr, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.postgres._typing import (
    PostgresAsyncConnectionContext,
    PostgresAsyncSessionContext,
    PostgresConnectionParams,
    PostgresDriverFeatures,
    PostgresPoolParams,
    PostgresSyncConnectionContext,
    PostgresSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.postgres.driver import PostgresAsyncDriver, PostgresSyncDriver
from vertebra.extensions.sqlspec.adapters.postgres.events import (
    PostgresAsyncEventsBackend,
    PostgresEventQueueStore,
    PostgresSyncEventsBackend,
)
from vertebra.extensions.sqlspec.adapters.postgres.litestar import PostgresLitestarStore

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

    from vertebra.db.driver._cloud_auth import DatabaseTokenProvider

__all__ = ("PostgresAsyncConfig", "PostgresSyncConfig")

_UNSET: Any = object()
_URL_ALIAS_KEYS = ("url", "dsn", "conninfo", "connection_string")
_PG_ENDPOINT_KEYS = ("host", "port", "user", "password", "database", "ssl")
_PG_POOL_ONLY_KEYS = (
    "init",
    "max_inactive_connection_lifetime",
    "max_queries",
    "reset",
    "setup",
)


def _extract_pg_url(connection_config: Mapping[str, Any], kwargs: Mapping[str, Any]) -> str | None:
    """Resolve URL/DSN aliases cleanly from `connection_config` or keyword arguments."""
    for source in (kwargs, connection_config):
        for key in _URL_ALIAS_KEYS:
            val = source.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return None


def _build_pg_dsn_from_parts(parts: Mapping[str, Any], base_dsn: str | None = None) -> str:
    """Construct a PostgreSQL URI from structured endpoint fields and an optional base DSN."""
    parsed = parse_postgres_dsn(base_dsn) if base_dsn else None
    scheme = parsed.scheme if parsed and parsed.scheme else "postgresql"
    raw_host = parts.get("host")
    raw_port = parts.get("port")
    user = parts.get("user", parsed.user if parsed else None)
    password = parts.get("password", parsed.password if parsed else None)
    database = parts.get("database", parsed.database if parsed else None)

    if raw_host is None:
        hosts = list(parsed.hosts) if parsed and parsed.hosts else ["localhost"]
    elif isinstance(raw_host, Sequence) and not isinstance(raw_host, (str, bytes)):
        hosts = [str(item) for item in raw_host] or ["localhost"]
    else:
        hosts = [str(raw_host)]

    if raw_port is None:
        ports: list[int | None] = list(parsed.ports) if parsed and parsed.ports else []
    elif isinstance(raw_port, Sequence) and not isinstance(raw_port, (str, bytes)):
        ports = [int(item) for item in raw_port]
    else:
        ports = [int(raw_port)]

    auth = ""
    if user is not None:
        auth = quote(str(user), safe="")
        if password is not None:
            auth = f"{auth}:{quote(str(password), safe='')}"
        auth = f"{auth}@"

    host_entries: list[str] = []
    for idx, host_item in enumerate(hosts):
        port_item = ports[idx] if idx < len(ports) else (ports[0] if len(ports) == 1 else None)
        host_entries.append(f"{host_item}:{int(port_item)}" if port_item is not None else host_item)
    authority = ",".join(host_entries)
    db_suffix = f"/{quote(str(database), safe='')}" if database else ""

    query_params = dict(parsed.params) if parsed else {}
    ssl_val = parts.get("ssl")
    if isinstance(ssl_val, str) and ssl_val.strip() and "sslmode" not in query_params:
        query_params["sslmode"] = ssl_val.strip()
    query_suffix = (
        "?" + "&".join(f"{quote(str(k), safe='')}={quote(str(v), safe='')}" for k, v in query_params.items())
        if query_params
        else ""
    )
    return f"{scheme}://{auth}{authority}{db_suffix}{query_suffix}"


def _normalize_pg_params(
    merged: dict[str, Any],
    *,
    explicit_url: str | None = None,
    base_dsn: str | None = None,
) -> tuple[str | None, dict[str, Any]]:
    """Translate `PostgresConnectionParams` and `PostgresPoolParams` into `PostgreSQLConfig` kwargs."""
    url = explicit_url if explicit_url is not None else _extract_pg_url(merged, {})
    for alias in _URL_ALIAS_KEYS:
        merged.pop(alias, None)
    for pool_only_key in _PG_POOL_ONLY_KEYS:
        merged.pop(pool_only_key, None)

    direct_dsn_val = merged.get("direct_dsn")
    if isinstance(direct_dsn_val, Mapping):
        merged["direct_dsn"] = _build_pg_dsn_from_parts(direct_dsn_val)

    endpoint_parts: dict[str, Any] = {}
    for key in ("host", "port", "user", "password", "database"):
        if key in merged:
            val = merged.pop(key)
            if val is not None:
                endpoint_parts[key] = val

    ssl_val = merged.pop("ssl", None)
    if ssl_val is not None:
        endpoint_parts["ssl"] = ssl_val

    merged.pop("timeout", None)
    command_timeout = merged.pop("command_timeout", None)
    if command_timeout is not None and "statement_timeout_ms" not in merged:
        merged["statement_timeout_ms"] = int(float(command_timeout) * 1000)

    server_settings = merged.pop("server_settings", None)
    if isinstance(server_settings, Mapping):
        existing_gucs = dict(merged.get("session_gucs") or {})
        merged_gucs = {str(k): str(v) for k, v in server_settings.items()}
        merged_gucs.update(existing_gucs)
        merged["session_gucs"] = merged_gucs

    statement_cache_size = merged.pop("statement_cache_size", None)
    if statement_cache_size is not None and "max_prepared_statements" not in merged:
        merged["max_prepared_statements"] = int(statement_cache_size)

    if endpoint_parts:
        seed_dsn = url if url is not None else base_dsn
        url = _build_pg_dsn_from_parts(endpoint_parts, base_dsn=seed_dsn)

    return url, merged


def _build_pg_connection_config(
    config: PostgresConnectionParams | PostgreSQLConfig | str | None,
    connection_config: PostgresPoolParams | dict[str, Any] | None,
    kwargs: dict[str, Any],
) -> PostgreSQLConfig:
    """Build a native `PostgreSQLConfig` from explicit config, URL/DSN, or typed param mappings."""
    if isinstance(config, PostgreSQLConfig):
        if not connection_config and not kwargs:
            return config
        override_map: dict[str, Any] = dict(connection_config or {})
        override_map.update(kwargs)
        url, updates = _normalize_pg_params(override_map, base_dsn=config.dsn)
        if url is not None:
            updates["dsn"] = url
        return msgspec.structs.replace(config, **updates) if updates else config

    merged: dict[str, Any] = dict(connection_config or {})
    if isinstance(config, Mapping):
        merged.update(config)
    merged.update(kwargs)
    explicit_url = config if isinstance(config, str) else None
    url, normalized = _normalize_pg_params(merged, explicit_url=explicit_url)
    if url is not None:
        base = PostgreSQLConfig.from_url(url)
        return msgspec.structs.replace(base, **normalized) if normalized else base
    return PostgreSQLConfig(**normalized) if normalized else PostgreSQLConfig()


def _resolve_pg_init_inputs(
    config: PostgresConnectionParams | PostgreSQLConfig | str | None,
    connection_config: PostgresPoolParams | PostgresConnectionParams | dict[str, Any] | None,
    pool_config: PostgresPoolParams | dict[str, Any] | None,
    driver_features: PostgresDriverFeatures | dict[str, Any] | None,
    broker: Any,
    share_pool: bool,
    kwargs: dict[str, Any],
) -> tuple[PostgreSQLConfig, dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    """Normalize config, pool options, broker resolution, and driver features for adapter initialization."""
    pool_cfg = dict(pool_config) if pool_config is not None else {}
    merged_conn: dict[str, Any] = {}
    if isinstance(connection_config, Mapping):
        merged_conn.update(connection_config)
    if pool_cfg:
        merged_conn.update(pool_cfg)
    if isinstance(config, Mapping):
        merged_conn.update(config)
    merged_conn.update(kwargs)

    features_dict = dict(driver_features) if driver_features is not None else None
    retry_feature_keys = ("connector_retry_count", "connector_retry_delay", "connector_retry_max_delay")
    if isinstance(config, PostgreSQLConfig):
        cfg_updates = dict(kwargs)
        if features_dict is not None:
            for retry_key in retry_feature_keys:
                if retry_key in features_dict and retry_key not in cfg_updates:
                    cfg_updates[retry_key] = features_dict[retry_key]
            if any(key in features_dict for key in retry_feature_keys):
                validate_connector_retry_config(
                    cfg_updates.get("connector_retry_count", config.connector_retry_count),
                    cfg_updates.get("connector_retry_delay", config.connector_retry_delay),
                    cfg_updates.get("connector_retry_max_delay", config.connector_retry_max_delay),
                )
        resolved_cfg = _build_pg_connection_config(config, None, cfg_updates) if cfg_updates else config
        return resolved_cfg, merged_conn, pool_cfg, features_dict

    if broker is not None:
        resolved_broker = broker
    elif share_pool:
        resolved_broker = DatabaseRegistry.default_broker()
    else:
        resolved_broker = None

    build_kwargs = dict(merged_conn)
    if resolved_broker is not None and "broker" not in build_kwargs:
        build_kwargs["broker"] = resolved_broker
    if features_dict is not None:
        if (
            "pooler_mode" in features_dict
            and features_dict["pooler_mode"] is not None
            and "pooler_mode" not in build_kwargs
        ):
            build_kwargs["pooler_mode"] = features_dict["pooler_mode"]
        for retry_key in retry_feature_keys:
            if retry_key in features_dict and retry_key not in build_kwargs:
                build_kwargs[retry_key] = features_dict[retry_key]
        if any(key in features_dict for key in retry_feature_keys):
            validate_connector_retry_config(
                features_dict.get(
                    "connector_retry_count",
                    build_kwargs.get("connector_retry_count", DEFAULT_CONNECTOR_RETRY_COUNT),
                ),
                features_dict.get(
                    "connector_retry_delay",
                    build_kwargs.get("connector_retry_delay", DEFAULT_CONNECTOR_RETRY_DELAY),
                ),
                features_dict.get(
                    "connector_retry_max_delay",
                    build_kwargs.get("connector_retry_max_delay", DEFAULT_CONNECTOR_RETRY_MAX_DELAY),
                ),
            )
    resolved_cfg = _build_pg_connection_config(config if isinstance(config, str) else None, None, build_kwargs)
    return resolved_cfg, merged_conn, pool_cfg, features_dict


def _derive_direct_replicas(config: PostgresSyncConfig | PostgresAsyncConfig) -> dict[str, ReplicaGroup[Any]] | None:
    """Derive direct-mode replica groups preserving group structure, weights, and staleness bounds."""
    if config.is_replica or not config.replica_groups:
        return None
    derived: dict[str, ReplicaGroup[Any]] = {}
    for group_name, group in config.replica_groups.items():
        specs: list[ReplicaSpec] = []
        for spec in group.specs:
            replica_cfg = spec.config
            direct_replica = (
                replica_cfg.derive_direct_config() if hasattr(replica_cfg, "derive_direct_config") else replica_cfg
            )
            specs.append(
                ReplicaSpec(
                    direct_replica,
                    weight=spec.weight,
                    name=spec.name,
                    max_staleness_seconds=spec.max_staleness_seconds,
                )
            )
        derived[group_name] = ReplicaGroup(
            specs,
            fallback_to_primary=group.fallback_to_primary,
            name=group.name,
        )
    return derived


def _direct_connection_metadata(
    config: PostgresSyncConfig | PostgresAsyncConfig, direct_pg: PostgreSQLConfig
) -> dict[str, Any]:
    """Copy connection metadata updating DSN to the derived direct endpoint."""
    metadata = dict(config.connection_config)
    for alias in ("url", "conninfo", "connection_string", "direct_dsn"):
        metadata.pop(alias, None)
    metadata["dsn"] = direct_pg.dsn
    return metadata


def _derive_pg_pool(
    adapter: PostgresSyncConfig | PostgresAsyncConfig,
    overrides: Mapping[str, Any],
) -> Any:
    """Clone `PostgresSyncConfig` or `PostgresAsyncConfig` preserving `token_provider` and `ca_pem` across URL overrides."""
    explicit_conn: dict[str, Any] = {}
    override_conn = overrides.get("connection_config")
    if isinstance(override_conn, Mapping):
        explicit_conn.update(override_conn)
    conn_overrides = overrides.get("connection_config_overrides")
    if isinstance(conn_overrides, Mapping):
        explicit_conn.update(conn_overrides)
    for key, value in overrides.items():
        if key not in adapter._ADAPTER_META_KEYS and key != "pool_config":
            explicit_conn[key] = value

    has_url_override = any(
        isinstance(explicit_conn.get(alias), str) and explicit_conn[alias].strip() for alias in _URL_ALIAS_KEYS
    )
    if not has_url_override:
        return adapter._derive_adapter_pool(adapter, overrides)

    merged_connection = dict(adapter.connection_config)
    for alias in _URL_ALIAS_KEYS:
        merged_connection.pop(alias, None)
    for endpoint_key in _PG_ENDPOINT_KEYS:
        if endpoint_key not in explicit_conn:
            merged_connection.pop(endpoint_key, None)
    if "token_provider" not in explicit_conn and adapter.driver_config.token_provider is not None:
        merged_connection["token_provider"] = adapter.driver_config.token_provider
    if "ca_pem" not in explicit_conn and adapter.driver_config.ca_pem is not None:
        merged_connection["ca_pem"] = adapter.driver_config.ca_pem
    merged_connection.update(explicit_conn)

    merged_features = dict(overrides.get("driver_features", adapter.driver_features))
    merged_features.pop("_statement_cache_holder", None)
    feature_overrides = overrides.get("driver_features_overrides")
    if isinstance(feature_overrides, Mapping):
        merged_features.update(feature_overrides)
        for retry_key in ("connector_retry_count", "connector_retry_delay", "connector_retry_max_delay"):
            if retry_key in feature_overrides and retry_key not in explicit_conn:
                merged_connection.pop(retry_key, None)
    for retry_key in ("connector_retry_count", "connector_retry_delay", "connector_retry_max_delay"):
        if retry_key in explicit_conn or retry_key in merged_features:
            continue
        if retry_key not in merged_connection:
            merged_connection[retry_key] = getattr(adapter.driver_config, retry_key)

    pool_cfg_override = overrides.get("pool_config")
    resolved_pool_cfg = (
        dict(pool_cfg_override)
        if isinstance(pool_cfg_override, Mapping)
        else (dict(adapter.pool_config) if adapter.pool_config else None)
    )
    if resolved_pool_cfg is not None:
        for alias in _URL_ALIAS_KEYS:
            resolved_pool_cfg.pop(alias, None)
        for endpoint_key in _PG_ENDPOINT_KEYS:
            if endpoint_key not in explicit_conn:
                resolved_pool_cfg.pop(endpoint_key, None)

    return type(adapter)(
        None,
        bind_key=str(overrides.get("bind_key", adapter.bind_key or "default")),
        broker=overrides.get("broker", adapter.broker),
        share_pool=bool(overrides.get("share_pool", adapter.share_pool)),
        arrow_threshold_rows=int(overrides.get("arrow_threshold_rows", adapter.arrow_threshold_rows)),
        statement_config=overrides.get("statement_config", adapter.statement_config),
        connection_config={k: v for k, v in merged_connection.items() if k not in adapter._ADAPTER_META_KEYS},
        pool_config=resolved_pool_cfg,
        migration_config=(
            dict(overrides["migration_config"])
            if isinstance(overrides.get("migration_config"), Mapping)
            else dict(adapter.migration_config)
            if isinstance(adapter.migration_config, Mapping)
            else cast("Any", adapter.migration_config)
        ),
        driver_features=merged_features,
        extension_config=dict(overrides.get("extension_config", adapter.extension_config)),
        observability_config=overrides.get("observability_config", adapter.observability_config),
        replicas=None,
        is_replica=bool(overrides.get("is_replica", False)),
    )


class PostgresSyncConfig(
    SyncAdapterConfig[PostgreSQLConfig, SyncPostgreSQLDriver, SyncPostgreSQLDriver, PostgresSyncDriver]
):
    """Synchronous PostgreSQL SQLSpec adapter configuration over `SyncPostgreSQLDriver`."""

    __slots__ = ("pool_config",)

    adapter_name: ClassVar[str | None] = "vertebra_postgres"
    dialect: ClassVar[str] = "postgres"
    is_async: ClassVar[bool] = False
    supports_transactional_ddl: ClassVar[bool] = True
    _config_cls: ClassVar[type[PostgreSQLConfig]] = PostgreSQLConfig
    _driver_cls: ClassVar[type[SyncPostgreSQLDriver]] = SyncPostgreSQLDriver
    connection_type: ClassVar[type[SyncPostgreSQLDriver]] = SyncPostgreSQLDriver
    driver_type: ClassVar[type[PostgresSyncDriver]] = PostgresSyncDriver
    _connection_context_class: ClassVar[type[PostgresSyncConnectionContext]] = PostgresSyncConnectionContext
    _session_context_class: ClassVar[type[PostgresSyncSessionContext]] = PostgresSyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresSyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresSyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresSyncADKArtifactStore",
    )
    events_store_class: ClassVar[type[Any] | None] = PostgresEventQueueStore
    events_backend_class: ClassVar[type[PostgresSyncEventsBackend]] = PostgresSyncEventsBackend
    litestar_store_class: ClassVar[type[Any] | None] = PostgresLitestarStore

    def __init__(
        self,
        config: PostgresConnectionParams | PostgreSQLConfig | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: PostgresPoolParams | PostgresConnectionParams | dict[str, Any] | None = None,
        pool_config: PostgresPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: PostgresDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        token_provider: DatabaseTokenProvider | None = _UNSET,
        ca_pem: str | None = _UNSET,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous PostgreSQL SQLSpec adapter configuration."""
        if token_provider is not _UNSET:
            kwargs["token_provider"] = token_provider
        if ca_pem is not _UNSET:
            kwargs["ca_pem"] = ca_pem
        resolved_cfg, merged_conn, pool_cfg, features_dict = _resolve_pg_init_inputs(
            config,
            connection_config,
            pool_config,
            driver_features,
            broker,
            share_pool,
            kwargs,
        )
        self.pool_config: dict[str, Any] = pool_cfg
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=merged_conn,
            connection_instance=connection_instance,
            migration_config=migration_config,
            driver_features=features_dict,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )

    def build_connection_config(
        self,
        config: PostgresConnectionParams | PostgreSQLConfig | str | None,
        connection_config: PostgresPoolParams | dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> PostgreSQLConfig:
        """Build `PostgreSQLConfig` supporting `url`/`dsn`/`conninfo`/`connection_string` and typed params."""
        return _build_pg_connection_config(config, connection_config, kwargs)

    def derive_direct_config(self) -> PostgresSyncConfig:
        """Return a direct-session `PostgresSyncConfig` bypassing transaction poolers."""
        direct_pg = self.driver_config.derive_direct_config()
        features = dict(self.driver_features)
        features.pop("_statement_cache_holder", None)
        return PostgresSyncConfig(
            direct_pg,
            bind_key=self.bind_key or "default",
            broker=self.broker,
            share_pool=False,
            arrow_threshold_rows=self.arrow_threshold_rows,
            statement_config=self.statement_config,
            connection_config=_direct_connection_metadata(self, direct_pg),
            pool_config=dict(self.pool_config) if self.pool_config else None,
            migration_config=dict(self.migration_config)
            if isinstance(self.migration_config, Mapping)
            else cast("Any", self.migration_config),
            driver_features=features,
            extension_config=dict(self.extension_config),
            observability_config=self.observability_config,
            replicas=_derive_direct_replicas(self),
            is_replica=self.is_replica,
        )

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this synchronous PostgreSQL configuration preserving cloud auth and Root CA settings."""
        return cast("Self", _derive_pg_pool(self, overrides))


class PostgresAsyncConfig(
    AsyncAdapterConfig[PostgreSQLConfig, AsyncPostgreSQLDriver, AsyncPostgreSQLDriver, PostgresAsyncDriver]
):
    """Asynchronous PostgreSQL SQLSpec adapter configuration over `AsyncPostgreSQLDriver`."""

    __slots__ = ("pool_config",)

    adapter_name: ClassVar[str | None] = "vertebra_postgres"
    dialect: ClassVar[str] = "postgres"
    is_async: ClassVar[bool] = True
    supports_transactional_ddl: ClassVar[bool] = True
    _config_cls: ClassVar[type[PostgreSQLConfig]] = PostgreSQLConfig
    _driver_cls: ClassVar[type[AsyncPostgreSQLDriver]] = AsyncPostgreSQLDriver
    connection_type: ClassVar[type[AsyncPostgreSQLDriver]] = AsyncPostgreSQLDriver
    driver_type: ClassVar[type[PostgresAsyncDriver]] = PostgresAsyncDriver
    _connection_context_class: ClassVar[type[PostgresAsyncConnectionContext]] = PostgresAsyncConnectionContext
    _session_context_class: ClassVar[type[PostgresAsyncSessionContext]] = PostgresAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresAsyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresAsyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.postgres.adk",
        "PostgresAsyncADKArtifactStore",
    )
    events_store_class: ClassVar[type[Any] | None] = PostgresEventQueueStore
    events_backend_class: ClassVar[type[PostgresAsyncEventsBackend]] = PostgresAsyncEventsBackend
    litestar_store_class: ClassVar[type[Any] | None] = PostgresLitestarStore

    def __init__(
        self,
        config: PostgresConnectionParams | PostgreSQLConfig | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: PostgresPoolParams | PostgresConnectionParams | dict[str, Any] | None = None,
        pool_config: PostgresPoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: PostgresDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        token_provider: DatabaseTokenProvider | None = _UNSET,
        ca_pem: str | None = _UNSET,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous PostgreSQL SQLSpec adapter configuration."""
        if token_provider is not _UNSET:
            kwargs["token_provider"] = token_provider
        if ca_pem is not _UNSET:
            kwargs["ca_pem"] = ca_pem
        resolved_cfg, merged_conn, pool_cfg, features_dict = _resolve_pg_init_inputs(
            config,
            connection_config,
            pool_config,
            driver_features,
            broker,
            share_pool,
            kwargs,
        )
        self.pool_config: dict[str, Any] = pool_cfg
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=merged_conn,
            connection_instance=connection_instance,
            migration_config=migration_config,
            driver_features=features_dict,
            extension_config=extension_config,
            observability_config=observability_config,
            replicas=replicas,
            is_replica=is_replica,
        )

    def build_connection_config(
        self,
        config: PostgresConnectionParams | PostgreSQLConfig | str | None,
        connection_config: PostgresPoolParams | dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> PostgreSQLConfig:
        """Build `PostgreSQLConfig` supporting `url`/`dsn`/`conninfo`/`connection_string` and typed params."""
        return _build_pg_connection_config(config, connection_config, kwargs)

    def derive_direct_config(self) -> PostgresAsyncConfig:
        """Return a direct-session `PostgresAsyncConfig` bypassing transaction poolers."""
        direct_pg = self.driver_config.derive_direct_config()
        features = dict(self.driver_features)
        features.pop("_statement_cache_holder", None)
        return PostgresAsyncConfig(
            direct_pg,
            bind_key=self.bind_key or "default",
            broker=self.broker,
            share_pool=False,
            arrow_threshold_rows=self.arrow_threshold_rows,
            statement_config=self.statement_config,
            connection_config=_direct_connection_metadata(self, direct_pg),
            pool_config=dict(self.pool_config) if self.pool_config else None,
            migration_config=dict(self.migration_config)
            if isinstance(self.migration_config, Mapping)
            else cast("Any", self.migration_config),
            driver_features=features,
            extension_config=dict(self.extension_config),
            observability_config=self.observability_config,
            replicas=_derive_direct_replicas(self),
            is_replica=self.is_replica,
        )

    def derive_pool(self, **overrides: Any) -> Self:
        """Clone this asynchronous PostgreSQL configuration preserving cloud auth and Root CA settings."""
        return cast("Self", _derive_pg_pool(self, overrides))
