"""SQL Server (MSSQL) synchronous and asynchronous SQLSpec adapter configurations."""

from collections.abc import Mapping
from typing import Any, ClassVar

from sqlspec.core import StatementConfig

from vertebra.db.adapters.mssql import AsyncMSSQLDriver, MSSQLConfig, SyncMSSQLDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.mssql.adk import (
    MSSQLAsyncADKMemoryStore,
    MSSQLAsyncADKStore,
    MSSQLSyncADKMemoryStore,
    MSSQLSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.mssql.core import build_connection_config
from vertebra.extensions.sqlspec.adapters.mssql.driver import MSSQLAsyncDriver, MSSQLSyncDriver
from vertebra.extensions.sqlspec.adapters.mssql.events import MSSQLEventQueueStore
from vertebra.extensions.sqlspec.adapters.mssql.litestar import MSSQLLitestarStore

__all__ = ("MSSQLAsyncConfig", "MSSQLSyncConfig")


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


class MSSQLSyncConfig(SyncAdapterConfig[MSSQLConfig, MSSQLSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncMSSQLDriver`."""

    adapter_name: ClassVar[str] = "vertebra_mssql"
    dialect: ClassVar[str] = "tsql"
    connection_type: ClassVar[type[Any]] = SyncMSSQLDriver
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[SyncMSSQLDriver]] = SyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLSyncDriver]] = MSSQLSyncDriver
    adk_store_class: ClassVar[type[Any]] = MSSQLSyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = MSSQLSyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = MSSQLEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = MSSQLLitestarStore

    def __init__(
        self,
        config: MSSQLConfig | None = None,
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
        """Initialize synchronous SQL Server SQLSpec configuration supporting URLs, ODBC strings, and Cloud SQL."""
        resolved_cfg, resolved_broker = _resolve_mssql_config(
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


class MSSQLAsyncConfig(AsyncAdapterConfig[MSSQLConfig, MSSQLAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncMSSQLDriver`."""

    adapter_name: ClassVar[str] = "vertebra_mssql"
    dialect: ClassVar[str] = "tsql"
    connection_type: ClassVar[type[Any]] = AsyncMSSQLDriver
    _config_cls: ClassVar[type[MSSQLConfig]] = MSSQLConfig
    _driver_cls: ClassVar[type[AsyncMSSQLDriver]] = AsyncMSSQLDriver
    driver_type: ClassVar[type[MSSQLAsyncDriver]] = MSSQLAsyncDriver
    adk_store_class: ClassVar[type[Any]] = MSSQLAsyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = MSSQLAsyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = MSSQLEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = MSSQLLitestarStore

    def __init__(
        self,
        config: MSSQLConfig | None = None,
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
        """Initialize asynchronous SQL Server SQLSpec configuration supporting URLs, ODBC strings, and Cloud SQL."""
        resolved_cfg, resolved_broker = _resolve_mssql_config(
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
