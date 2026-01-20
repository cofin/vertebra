"""Generic broker proxy configs for external SQLSpec adapters."""

from typing import TYPE_CHECKING, Any, ClassVar, TypeVar

from sqlspec.config import AsyncDatabaseConfig, ExtensionConfigs, SyncDatabaseConfig
from sqlspec.core import get_default_config

from vertebra.extensions.sqlspec._config import VertebraAsyncConfigBase, VertebraSyncConfigBase
from vertebra.extensions.sqlspec._dictionary import resolve_async_data_dictionary, resolve_sync_data_dictionary
from vertebra.extensions.sqlspec._driver import VertebraAsyncDriverBase, VertebraSyncDriverBase

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

    from vertebra.resources.broker import BrokerClientConfig

SyncConfigT = TypeVar("SyncConfigT", bound=SyncDatabaseConfig[Any, Any, Any])
AsyncConfigT = TypeVar("AsyncConfigT", bound=AsyncDatabaseConfig[Any, Any, Any])


class VertebraProxySyncDriver(VertebraSyncDriverBase):
    """Sync Vertebra broker proxy driver for external SQLSpec adapters."""

    dialect: str = "proxy"


class VertebraProxyAsyncDriver(VertebraAsyncDriverBase):
    """Async Vertebra broker proxy driver for external SQLSpec adapters."""

    dialect: str = "proxy"


class VertebraProxySyncConfig[SyncConfigT: SyncDatabaseConfig[Any, Any, Any]](VertebraSyncConfigBase):
    """Vertebra broker proxy config wrapping a sync SQLSpec adapter."""

    driver_type: ClassVar[type[Any]] = VertebraProxySyncDriver
    default_statement_config: ClassVar["StatementConfig"] = get_default_config()
    default_pool_name: ClassVar[str] = "provider"

    def __init__(
        self,
        config: SyncConfigT,
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        max_clients: int = 5,
        migration_config: "dict[str, object] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: object,
    ) -> None:
        self.target_config = config
        effective_statement = statement_config or config.statement_config
        dialect = str(effective_statement.dialect or "")
        features = dict(driver_features or config.driver_features or {})
        features.setdefault("data_dictionary_factory", resolve_sync_data_dictionary(dialect))
        pool_name = pool or config.bind_key or type(self).default_pool_name
        super().__init__(
            pool=pool_name,
            client_config=client_config,
            max_clients=max_clients,
            connection_instance=None,
            migration_config=migration_config or getattr(config, "migration_config", None),
            statement_config=effective_statement,
            driver_features=features,
            bind_key=bind_key or config.bind_key,
            extension_config=extension_config or getattr(config, "extension_config", None),
            observability_config=observability_config,
            **kwargs,
        )


class VertebraProxyAsyncConfig[AsyncConfigT: AsyncDatabaseConfig[Any, Any, Any]](VertebraAsyncConfigBase):
    """Vertebra broker proxy config wrapping an async SQLSpec adapter."""

    driver_type: ClassVar[type[Any]] = VertebraProxyAsyncDriver
    default_statement_config: ClassVar["StatementConfig"] = get_default_config()
    default_pool_name: ClassVar[str] = "provider"

    def __init__(
        self,
        config: AsyncConfigT,
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        max_clients: int = 5,
        migration_config: "dict[str, object] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: object,
    ) -> None:
        self.target_config = config
        effective_statement = statement_config or config.statement_config
        dialect = str(effective_statement.dialect or "")
        features = dict(driver_features or config.driver_features or {})
        features.setdefault("data_dictionary_factory", resolve_async_data_dictionary(dialect))
        pool_name = pool or config.bind_key or type(self).default_pool_name
        super().__init__(
            pool=pool_name,
            client_config=client_config,
            max_clients=max_clients,
            connection_instance=None,
            migration_config=migration_config or getattr(config, "migration_config", None),
            statement_config=effective_statement,
            driver_features=features,
            bind_key=bind_key or config.bind_key,
            extension_config=extension_config or getattr(config, "extension_config", None),
            observability_config=observability_config,
            **kwargs,
        )


__all__ = ("VertebraProxyAsyncConfig", "VertebraProxyAsyncDriver", "VertebraProxySyncConfig", "VertebraProxySyncDriver")
