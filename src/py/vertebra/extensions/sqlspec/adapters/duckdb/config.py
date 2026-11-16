"""DuckDB synchronous and asynchronous SQLSpec adapter configurations."""

from collections.abc import Mapping
from typing import Any, ClassVar

from sqlspec.core import StatementConfig

from vertebra.db.adapters.duckdb import AsyncDuckDBDriver, DuckDBConfig, SyncDuckDBDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.duckdb.adk import (
    DuckDBAsyncADKMemoryStore,
    DuckDBAsyncADKStore,
    DuckDBSyncADKMemoryStore,
    DuckDBSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.duckdb.core import build_connection_config
from vertebra.extensions.sqlspec.adapters.duckdb.driver import DuckDBAsyncDriver, DuckDBSyncDriver
from vertebra.extensions.sqlspec.adapters.duckdb.events import DuckDBEventQueueStore
from vertebra.extensions.sqlspec.adapters.duckdb.litestar import DuckDBLitestarStore

__all__ = ("DuckDBAsyncConfig", "DuckDBSyncConfig")


def _resolve_duckdb_config(
    config: DuckDBConfig | None,
    *,
    share_pool: bool,
    broker: Any,
    connection_config: Mapping[str, Any] | None,
    driver_features: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
) -> tuple[DuckDBConfig, Any]:
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


class DuckDBSyncConfig(SyncAdapterConfig[DuckDBConfig, DuckDBSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncDuckDBDriver`."""

    adapter_name: ClassVar[str] = "vertebra_duckdb"
    dialect: ClassVar[str] = "duckdb"
    connection_type: ClassVar[type[Any]] = SyncDuckDBDriver
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[SyncDuckDBDriver]] = SyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBSyncDriver]] = DuckDBSyncDriver
    adk_store_class: ClassVar[type[Any]] = DuckDBSyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = DuckDBSyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = DuckDBEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = DuckDBLitestarStore

    def __init__(
        self,
        config: DuckDBConfig | None = None,
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
        """Initialize synchronous DuckDB SQLSpec configuration preserving isolated `:memory:<name>` DBs."""
        resolved_cfg, resolved_broker = _resolve_duckdb_config(
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


class DuckDBAsyncConfig(AsyncAdapterConfig[DuckDBConfig, DuckDBAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncDuckDBDriver`."""

    adapter_name: ClassVar[str] = "vertebra_duckdb"
    dialect: ClassVar[str] = "duckdb"
    connection_type: ClassVar[type[Any]] = AsyncDuckDBDriver
    _config_cls: ClassVar[type[DuckDBConfig]] = DuckDBConfig
    _driver_cls: ClassVar[type[AsyncDuckDBDriver]] = AsyncDuckDBDriver
    driver_type: ClassVar[type[DuckDBAsyncDriver]] = DuckDBAsyncDriver
    adk_store_class: ClassVar[type[Any]] = DuckDBAsyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = DuckDBAsyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = DuckDBEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = DuckDBLitestarStore

    def __init__(
        self,
        config: DuckDBConfig | None = None,
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
        """Initialize asynchronous DuckDB SQLSpec configuration preserving isolated `:memory:<name>` DBs."""
        resolved_cfg, resolved_broker = _resolve_duckdb_config(
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
