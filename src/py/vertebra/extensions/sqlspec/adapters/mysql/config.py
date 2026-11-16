"""MySQL and MariaDB synchronous and asynchronous SQLSpec adapter configurations."""

from collections.abc import Mapping
from typing import Any, ClassVar

from sqlspec.core import StatementConfig

from vertebra.db.adapters.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.mysql.adk import (
    MySQLAsyncADKMemoryStore,
    MySQLAsyncADKStore,
    MySQLSyncADKMemoryStore,
    MySQLSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.mysql.core import build_connection_config
from vertebra.extensions.sqlspec.adapters.mysql.driver import MySQLAsyncDriver, MySQLSyncDriver
from vertebra.extensions.sqlspec.adapters.mysql.events import MySQLEventQueueStore
from vertebra.extensions.sqlspec.adapters.mysql.litestar import MySQLLitestarStore

__all__ = ("MySQLAsyncConfig", "MySQLSyncConfig")


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
        return config, resolved_broker
    built = build_connection_config(
        connection_config=connection_config,
        driver_features=driver_features,
        broker=resolved_broker,
        overrides=kwargs,
    )
    return built, resolved_broker


class MySQLSyncConfig(SyncAdapterConfig[MySQLConfig, MySQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncMySQLDriver`."""

    adapter_name: ClassVar[str] = "vertebra_mysql"
    dialect: ClassVar[str] = "mysql"
    connection_type: ClassVar[type[Any]] = SyncMySQLDriver
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[SyncMySQLDriver]] = SyncMySQLDriver
    driver_type: ClassVar[type[MySQLSyncDriver]] = MySQLSyncDriver
    adk_store_class: ClassVar[type[Any]] = MySQLSyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = MySQLSyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = MySQLEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = MySQLLitestarStore

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize synchronous MySQL SQLSpec configuration supporting URLs and Cloud SQL options."""
        resolved_cfg, resolved_broker = _resolve_mysql_config(
            config,
            share_pool=bool(share_pool),
            broker=broker,
            connection_config=connection_config,
            driver_features=driver_features,
            kwargs=kwargs,
        )
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=connection_config,
            migration_config=migration_config,
            driver_features=driver_features,
            extension_config=extension_config,
            observability_config=observability_config,
        )


class MySQLAsyncConfig(AsyncAdapterConfig[MySQLConfig, MySQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncMySQLDriver`."""

    adapter_name: ClassVar[str] = "vertebra_mysql"
    dialect: ClassVar[str] = "mysql"
    connection_type: ClassVar[type[Any]] = AsyncMySQLDriver
    _config_cls: ClassVar[type[MySQLConfig]] = MySQLConfig
    _driver_cls: ClassVar[type[AsyncMySQLDriver]] = AsyncMySQLDriver
    driver_type: ClassVar[type[MySQLAsyncDriver]] = MySQLAsyncDriver
    adk_store_class: ClassVar[type[Any]] = MySQLAsyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = MySQLAsyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = MySQLEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = MySQLLitestarStore

    def __init__(
        self,
        config: MySQLConfig | None = None,
        *,
        bind_key: str = "default",
        broker: Any = None,
        share_pool: bool = True,
        arrow_threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS,
        statement_config: StatementConfig | None = None,
        connection_config: dict[str, Any] | None = None,
        migration_config: dict[str, Any] | None = None,
        driver_features: dict[str, Any] | None = None,
        extension_config: dict[str, Any] | None = None,
        observability_config: Any | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize asynchronous MySQL SQLSpec configuration supporting URLs and Cloud SQL options."""
        resolved_cfg, resolved_broker = _resolve_mysql_config(
            config,
            share_pool=bool(share_pool),
            broker=broker,
            connection_config=connection_config,
            driver_features=driver_features,
            kwargs=kwargs,
        )
        super().__init__(
            resolved_cfg,
            bind_key=bind_key,
            broker=resolved_broker,
            share_pool=share_pool,
            arrow_threshold_rows=arrow_threshold_rows,
            statement_config=statement_config,
            connection_config=connection_config,
            migration_config=migration_config,
            driver_features=driver_features,
            extension_config=extension_config,
            observability_config=observability_config,
        )
