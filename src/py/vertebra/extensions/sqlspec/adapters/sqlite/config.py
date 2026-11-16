"""SQLite synchronous and asynchronous SQLSpec adapter configurations."""

from collections.abc import Mapping
from typing import Any, ClassVar

from sqlspec.core import StatementConfig

from vertebra.db.adapters.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.arrow import ARROW_SHM_THRESHOLD_ROWS
from vertebra.db.registry import DatabaseRegistry
from vertebra.extensions.sqlspec._driver import AsyncAdapterConfig, SyncAdapterConfig
from vertebra.extensions.sqlspec.adapters.sqlite.adk import (
    SQLiteAsyncADKMemoryStore,
    SQLiteAsyncADKStore,
    SQLiteSyncADKMemoryStore,
    SQLiteSyncADKStore,
)
from vertebra.extensions.sqlspec.adapters.sqlite.core import build_connection_config
from vertebra.extensions.sqlspec.adapters.sqlite.driver import SQLiteAsyncDriver, SQLiteSyncDriver
from vertebra.extensions.sqlspec.adapters.sqlite.events import SQLiteEventQueueStore
from vertebra.extensions.sqlspec.adapters.sqlite.litestar import SQLiteLitestarStore

__all__ = ("SQLiteAsyncConfig", "SQLiteSyncConfig")


def _resolve_sqlite_config(
    config: SQLiteConfig | None,
    *,
    share_pool: bool,
    broker: Any,
    connection_config: Mapping[str, Any] | None,
    driver_features: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
) -> tuple[SQLiteConfig, Any]:
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


class SQLiteSyncConfig(SyncAdapterConfig[SQLiteConfig, SQLiteSyncDriver]):
    """Synchronous SQLSpec adapter configuration backed by Vertebra `SyncSQLiteDriver`."""

    adapter_name: ClassVar[str] = "vertebra_sqlite"
    dialect: ClassVar[str] = "sqlite"
    connection_type: ClassVar[type[Any]] = SyncSQLiteDriver
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[SyncSQLiteDriver]] = SyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteSyncDriver]] = SQLiteSyncDriver
    adk_store_class: ClassVar[type[Any]] = SQLiteSyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = SQLiteSyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = SQLiteEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = SQLiteLitestarStore

    def __init__(
        self,
        config: SQLiteConfig | None = None,
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
        """Initialize synchronous SQLite SQLSpec configuration supporting URLs and shared `:memory:` URIs."""
        resolved_cfg, resolved_broker = _resolve_sqlite_config(
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


class SQLiteAsyncConfig(AsyncAdapterConfig[SQLiteConfig, SQLiteAsyncDriver]):
    """Asynchronous SQLSpec adapter configuration backed by Vertebra `AsyncSQLiteDriver`."""

    adapter_name: ClassVar[str] = "vertebra_sqlite"
    dialect: ClassVar[str] = "sqlite"
    connection_type: ClassVar[type[Any]] = AsyncSQLiteDriver
    _config_cls: ClassVar[type[SQLiteConfig]] = SQLiteConfig
    _driver_cls: ClassVar[type[AsyncSQLiteDriver]] = AsyncSQLiteDriver
    driver_type: ClassVar[type[SQLiteAsyncDriver]] = SQLiteAsyncDriver
    adk_store_class: ClassVar[type[Any]] = SQLiteAsyncADKStore
    adk_memory_store_class: ClassVar[type[Any]] = SQLiteAsyncADKMemoryStore
    events_store_class: ClassVar[type[Any]] = SQLiteEventQueueStore
    litestar_store_class: ClassVar[type[Any]] = SQLiteLitestarStore

    def __init__(
        self,
        config: SQLiteConfig | None = None,
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
        """Initialize asynchronous SQLite SQLSpec configuration supporting URLs and shared `:memory:` URIs."""
        resolved_cfg, resolved_broker = _resolve_sqlite_config(
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
