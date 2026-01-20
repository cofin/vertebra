"""Broker-backed PostgreSQL SQLSpec configuration."""

from typing import TYPE_CHECKING, ClassVar, NotRequired, TypedDict

from vertebra.extensions.sqlspec._config import (
    VertebraAsyncConfigBase,
    VertebraSyncConfigBase,
    _AsyncSessionHandler,
    _SyncSessionHandler,
)
from vertebra.extensions.sqlspec.postgres._typing import (
    VertebraPostgresAsyncSessionContext,
    VertebraPostgresSyncSessionContext,
)
from vertebra.extensions.sqlspec.postgres.core import apply_driver_features, default_statement_config
from vertebra.extensions.sqlspec.postgres.data_dictionary import (
    VertebraPostgresAsyncDataDictionary,
    VertebraPostgresSyncDataDictionary,
)
from vertebra.extensions.sqlspec.postgres.driver import VertebraPostgresAsyncDriver, VertebraPostgresSyncDriver

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlspec.config import ExtensionConfigs
    from sqlspec.core import StatementConfig
    from sqlspec.driver import AsyncDataDictionaryBase, SyncDataDictionaryBase
    from sqlspec.observability import ObservabilityConfig

    from vertebra.drivers.broker import VertebraAsyncPool, VertebraSyncPool
    from vertebra.resources.broker import BrokerClientConfig


class PostgresDriverFeatures(TypedDict, total=False):
    """Driver features for broker-backed PostgreSQL."""

    data_dictionary_factory: NotRequired["Callable[[], SyncDataDictionaryBase | AsyncDataDictionaryBase]"]


class VertebraPostgresSyncConfig(VertebraSyncConfigBase):
    """Synchronous Vertebra broker-backed PostgreSQL configuration."""

    driver_type: ClassVar[type[VertebraPostgresSyncDriver]] = VertebraPostgresSyncDriver
    default_statement_config: ClassVar["StatementConfig"] = default_statement_config
    default_pool_name: ClassVar[str] = "postgres"
    supports_transactional_ddl: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = False
    supports_native_arrow_import: ClassVar[bool] = False
    supports_native_parquet_export: ClassVar[bool] = False
    supports_native_parquet_import: ClassVar[bool] = False

    def __init__(
        self,
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        max_clients: int = 5,
        connection_instance: "VertebraSyncPool | None" = None,
        migration_config: "dict[str, object] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PostgresDriverFeatures | dict[str, object] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: object,
    ) -> None:
        statement_config = statement_config or default_statement_config
        statement_config, features = apply_driver_features(statement_config, driver_features)
        features.setdefault("data_dictionary_factory", VertebraPostgresSyncDataDictionary)
        super().__init__(
            pool=pool,
            client_config=client_config,
            max_clients=max_clients,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def provide_session(
        self, *_args: object, statement_config: "StatementConfig | None" = None, **_kwargs: object
    ) -> "VertebraPostgresSyncSessionContext":
        handler = _SyncSessionHandler(self)
        return VertebraPostgresSyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )


class VertebraPostgresAsyncConfig(VertebraAsyncConfigBase):
    """Asynchronous Vertebra broker-backed PostgreSQL configuration."""

    driver_type: ClassVar[type[VertebraPostgresAsyncDriver]] = VertebraPostgresAsyncDriver
    default_statement_config: ClassVar["StatementConfig"] = default_statement_config
    default_pool_name: ClassVar[str] = "postgres"
    supports_transactional_ddl: ClassVar[bool] = True
    supports_native_arrow_export: ClassVar[bool] = False
    supports_native_arrow_import: ClassVar[bool] = False
    supports_native_parquet_export: ClassVar[bool] = False
    supports_native_parquet_import: ClassVar[bool] = False

    def __init__(
        self,
        *,
        pool: "str | None" = None,
        client_config: "BrokerClientConfig | None" = None,
        max_clients: int = 5,
        connection_instance: "VertebraAsyncPool | None" = None,
        migration_config: "dict[str, object] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "PostgresDriverFeatures | dict[str, object] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: object,
    ) -> None:
        statement_config = statement_config or default_statement_config
        statement_config, features = apply_driver_features(statement_config, driver_features)
        features.setdefault("data_dictionary_factory", VertebraPostgresAsyncDataDictionary)
        super().__init__(
            pool=pool,
            client_config=client_config,
            max_clients=max_clients,
            connection_instance=connection_instance,
            migration_config=migration_config,
            statement_config=statement_config,
            driver_features=features,
            bind_key=bind_key,
            extension_config=extension_config,
            observability_config=observability_config,
            **kwargs,
        )

    def provide_session(
        self, *_args: object, statement_config: "StatementConfig | None" = None, **_kwargs: object
    ) -> "VertebraPostgresAsyncSessionContext":
        handler = _AsyncSessionHandler(self)
        return VertebraPostgresAsyncSessionContext(
            acquire_connection=handler.acquire_connection,
            release_connection=handler.release_connection,
            statement_config=statement_config or self.statement_config,
            driver_features=self.driver_features,
            prepare_driver=self._prepare_driver,
        )


__all__ = (
    "PostgresDriverFeatures",
    "VertebraPostgresAsyncConfig",
    "VertebraPostgresSyncConfig",
    "default_statement_config",
)
