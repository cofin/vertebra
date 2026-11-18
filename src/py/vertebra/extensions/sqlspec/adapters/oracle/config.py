"""Oracle synchronous and asynchronous SQLSpec adapter configuration classes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, ClassVar

import msgspec

from vertebra.db.adapters.oracle import (
    AsyncOracleDriver,
    AsyncOraclePool,
    OracleConfig,
    SyncOracleDriver,
    SyncOraclePool,
)
from vertebra.db.adapters.oracle.config import OracleDrcpConfig
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, LazyStoreAttr, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.oracle._typing import (
    OracleAsyncConnectionContext,
    OracleAsyncSessionContext,
    OracleConnectionParams,
    OracleDriverFeatures,
    OraclePoolParams,
    OracleSyncConnectionContext,
    OracleSyncSessionContext,
)
from vertebra.extensions.sqlspec.adapters.oracle.core import (
    OracleVersionCache,
    normalize_oracle_transaction_priority,
)
from vertebra.extensions.sqlspec.adapters.oracle.driver import OracleAsyncDriver, OracleSyncDriver
from vertebra.extensions.sqlspec.adapters.oracle.events import (
    OracleAsyncEventsBackend,
    OracleEventQueueStore,
    OracleSyncEventsBackend,
)
from vertebra.extensions.sqlspec.adapters.oracle.litestar import OracleLitestarStore

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

__all__ = ("OracleAsyncConfig", "OracleSyncConfig")

_URL_ALIAS_KEYS: tuple[str, ...] = ("url", "dsn", "connection_string")
_ORACLE_CONFIG_FIELDS: frozenset[str] = frozenset(OracleConfig.__struct_fields__)


def _extract_oracle_url(connection_config: Mapping[str, Any], kwargs: Mapping[str, Any]) -> str | None:
    """Resolve Oracle URL/DSN aliases cleanly from `connection_config` or kwargs."""
    for source in (kwargs, connection_config):
        for key in _URL_ALIAS_KEYS:
            val = source.get(key)
            if isinstance(val, str) and val.strip():
                return val
    return None


def _normalize_oracle_params(
    merged: dict[str, Any],
    *,
    explicit_url: str | None = None,
) -> tuple[str | None, dict[str, Any]]:
    """Translate `OracleConnectionParams` and `OraclePoolParams` into `OracleConfig` kwargs."""
    url = explicit_url if explicit_url is not None else _extract_oracle_url(merged, {})
    for alias in _URL_ALIAS_KEYS:
        merged.pop(alias, None)

    if "transaction_priority" in merged:
        normalize_oracle_transaction_priority(merged.pop("transaction_priority"))

    if "min_size" in merged and "pool_min_size" not in merged:
        merged["pool_min_size"] = int(merged.pop("min_size"))
    elif "min" in merged and "pool_min_size" not in merged:
        merged["pool_min_size"] = int(merged.pop("min"))

    if "max_size" in merged and "pool_max_size" not in merged:
        merged["pool_max_size"] = int(merged.pop("max_size"))
    elif "max" in merged and "pool_max_size" not in merged:
        merged["pool_max_size"] = int(merged.pop("max"))

    if "stmtcachesize" in merged and "statement_cache_size" not in merged:
        merged["statement_cache_size"] = int(merged.pop("stmtcachesize"))

    if "wallet_location" in merged and "wallet_path" not in merged:
        merged["wallet_path"] = merged.pop("wallet_location")

    cclass = merged.pop("cclass", None)
    purity = merged.pop("purity", None)
    tag = merged.pop("tag", None)
    if (cclass is not None or purity is not None or tag is not None) and "drcp" not in merged:
        merged["drcp"] = OracleDrcpConfig(
            cclass=cclass,
            purity=str(purity) if purity is not None else "SELF",
            connection_tag=tag,
        )

    filtered = {k: v for k, v in merged.items() if k in _ORACLE_CONFIG_FIELDS}
    return url, filtered


def _build_oracle_connection_config(
    config: OracleConnectionParams | OracleConfig | str | None,
    connection_config: OraclePoolParams | dict[str, Any] | None,
    kwargs: dict[str, Any],
) -> OracleConfig:
    """Build native `OracleConfig` from explicit config, URL/DSN, or typed parameter mappings."""
    if isinstance(config, OracleConfig):
        if not connection_config and not kwargs:
            return config
        override_map: dict[str, Any] = dict(connection_config or {})
        override_map.update(kwargs)
        url, updates = _normalize_oracle_params(override_map)
        if url is not None:
            updates["dsn"] = url
        return msgspec.structs.replace(config, **updates) if updates else config

    merged: dict[str, Any] = dict(connection_config or {})
    if isinstance(config, Mapping):
        merged.update(config)
    merged.update(kwargs)
    explicit_url = config if isinstance(config, str) else None
    url, normalized = _normalize_oracle_params(merged, explicit_url=explicit_url)
    if url is not None:
        base = OracleConfig.from_url(url)
        return msgspec.structs.replace(base, **normalized) if normalized else base
    return OracleConfig(**normalized) if normalized else OracleConfig()


def _resolve_oracle_init_inputs(
    config: OracleConnectionParams | OracleConfig | str | None,
    connection_config: OraclePoolParams | OracleConnectionParams | dict[str, Any] | None,
    pool_config: OraclePoolParams | dict[str, Any] | None,
    driver_features: OracleDriverFeatures | dict[str, Any] | None,
    broker: Any,
    share_pool: bool,
    kwargs: dict[str, Any],
) -> tuple[OracleConfig, dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    """Normalize config, pool options, broker resolution, and driver features for Oracle initialization."""
    pool_cfg = dict(pool_config) if pool_config is not None else {}
    merged_conn: dict[str, Any] = {}
    if isinstance(connection_config, Mapping):
        merged_conn.update(connection_config)
    if pool_cfg:
        merged_conn.update(pool_cfg)
    if isinstance(config, Mapping):
        merged_conn.update(config)

    remaining_kwargs = dict(kwargs)
    features_dict = dict(driver_features) if driver_features is not None else None
    if "reset_session_on_release" in remaining_kwargs:
        reset_val = bool(remaining_kwargs.pop("reset_session_on_release"))
        if features_dict is None:
            features_dict = {"reset_session_on_release": reset_val}
        else:
            features_dict.setdefault("reset_session_on_release", reset_val)

    merged_conn.update(remaining_kwargs)
    if "transaction_priority" in merged_conn:
        merged_conn["transaction_priority"] = normalize_oracle_transaction_priority(merged_conn["transaction_priority"])

    if isinstance(config, OracleConfig):
        resolved_cfg = _build_oracle_connection_config(config, None, remaining_kwargs) if remaining_kwargs else config
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
    resolved_cfg = _build_oracle_connection_config(config if isinstance(config, str) else None, None, build_kwargs)
    return resolved_cfg, merged_conn, pool_cfg, features_dict


class OracleSyncConfig(
    SyncAdapterConfig[OracleConfig, SyncOracleDriver, SyncOraclePool | SyncOracleDriver, OracleSyncDriver]
):
    """Synchronous Oracle SQLSpec adapter configuration over `SyncOracleDriver`."""

    __slots__ = ("_oracle_version_cache", "pool_config")

    adapter_name: ClassVar[str | None] = "vertebra_oracle"
    dialect: ClassVar[str] = "oracle"
    is_async: ClassVar[bool] = False
    supports_transactional_ddl: ClassVar[bool] = False
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[SyncOracleDriver]] = SyncOracleDriver
    connection_type: ClassVar[type[SyncOracleDriver]] = SyncOracleDriver
    driver_type: ClassVar[type[OracleSyncDriver]] = OracleSyncDriver
    _connection_context_class: ClassVar[type[OracleSyncConnectionContext]] = OracleSyncConnectionContext
    _session_context_class: ClassVar[type[OracleSyncSessionContext]] = OracleSyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.oracle.adk",
        "OracleSyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.oracle.adk",
        "OracleSyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.oracle.adk",
        "OracleSyncADKArtifactStore",
    )
    events_store_class: ClassVar[type[Any] | None] = OracleEventQueueStore
    events_backend_class: ClassVar[type[OracleSyncEventsBackend]] = OracleSyncEventsBackend
    litestar_store_class: ClassVar[type[Any] | None] = OracleLitestarStore

    def __init__(
        self,
        config: OracleConnectionParams | OracleConfig | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: OraclePoolParams | OracleConnectionParams | dict[str, Any] | None = None,
        pool_config: OraclePoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: OracleDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous Oracle SQLSpec adapter configuration."""
        resolved_cfg, merged_conn, pool_cfg, features_dict = _resolve_oracle_init_inputs(
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
        self._oracle_version_cache = OracleVersionCache()

    def build_connection_config(
        self,
        config: OracleConnectionParams | OracleConfig | str | None,
        connection_config: OraclePoolParams | dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> OracleConfig:
        """Build `OracleConfig` supporting `url`/`dsn`/`connection_string` aliases and typed params."""
        return _build_oracle_connection_config(config, connection_config, kwargs)


class OracleAsyncConfig(
    AsyncAdapterConfig[OracleConfig, AsyncOracleDriver, AsyncOraclePool | AsyncOracleDriver, OracleAsyncDriver]
):
    """Asynchronous Oracle SQLSpec adapter configuration over `AsyncOracleDriver`."""

    __slots__ = ("_oracle_version_cache", "pool_config")

    adapter_name: ClassVar[str | None] = "vertebra_oracle"
    dialect: ClassVar[str] = "oracle"
    is_async: ClassVar[bool] = True
    supports_transactional_ddl: ClassVar[bool] = False
    _config_cls: ClassVar[type[OracleConfig]] = OracleConfig
    _driver_cls: ClassVar[type[AsyncOracleDriver]] = AsyncOracleDriver
    connection_type: ClassVar[type[AsyncOracleDriver]] = AsyncOracleDriver
    driver_type: ClassVar[type[OracleAsyncDriver]] = OracleAsyncDriver
    _connection_context_class: ClassVar[type[OracleAsyncConnectionContext]] = OracleAsyncConnectionContext
    _session_context_class: ClassVar[type[OracleAsyncSessionContext]] = OracleAsyncSessionContext
    adk_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.oracle.adk",
        "OracleAsyncADKStore",
    )
    adk_memory_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.oracle.adk",
        "OracleAsyncADKMemoryStore",
    )
    adk_artifact_store_class: ClassVar[Any] = LazyStoreAttr(
        "vertebra.extensions.sqlspec.adapters.oracle.adk",
        "OracleAsyncADKArtifactStore",
    )
    events_store_class: ClassVar[type[Any] | None] = OracleEventQueueStore
    events_backend_class: ClassVar[type[OracleAsyncEventsBackend]] = OracleAsyncEventsBackend
    litestar_store_class: ClassVar[type[Any] | None] = OracleLitestarStore

    def __init__(
        self,
        config: OracleConnectionParams | OracleConfig | str | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: OraclePoolParams | OracleConnectionParams | dict[str, Any] | None = None,
        pool_config: OraclePoolParams | dict[str, Any] | None = None,
        connection_instance: Any = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: OracleDriverFeatures | dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        replicas: Any | None = None,
        is_replica: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous Oracle SQLSpec adapter configuration."""
        resolved_cfg, merged_conn, pool_cfg, features_dict = _resolve_oracle_init_inputs(
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
        self._oracle_version_cache = OracleVersionCache()

    def build_connection_config(
        self,
        config: OracleConnectionParams | OracleConfig | str | None,
        connection_config: OraclePoolParams | dict[str, Any] | None,
        kwargs: dict[str, Any],
    ) -> OracleConfig:
        """Build `OracleConfig` supporting `url`/`dsn`/`connection_string` aliases and typed params."""
        return _build_oracle_connection_config(config, connection_config, kwargs)
