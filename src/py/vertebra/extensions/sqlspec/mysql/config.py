"""Broker-backed MySQL SQLSpec configuration."""

from typing import TYPE_CHECKING, Any, ClassVar, NotRequired, TypedDict

from sqlspec.core import ParameterStyle

from vertebra.extensions.sqlspec._config import VertebraAsyncConfigBase, VertebraSyncConfigBase
from vertebra.extensions.sqlspec._core import build_broker_profile, build_statement_config
from vertebra.extensions.sqlspec._dictionary import resolve_async_data_dictionary, resolve_sync_data_dictionary
from vertebra.extensions.sqlspec.mysql.driver import VertebraMysqlAsyncDriver, VertebraMysqlSyncDriver

if TYPE_CHECKING:
    from sqlspec.config import ExtensionConfigs
    from sqlspec.core import StatementConfig
    from sqlspec.observability import ObservabilityConfig

    from vertebra.resources.broker import BrokerClientConfig


class MysqlDriverFeatures(TypedDict, total=False):
    """Driver features for broker-backed MySQL."""

    data_dictionary_factory: NotRequired["Any"]


_MYSQL_PROFILE = build_broker_profile("VertebraMysql", default_style=ParameterStyle.QMARK, dialect="mysql")


default_statement_config = build_statement_config(_MYSQL_PROFILE, "mysql")


class VertebraMysqlSyncConfig(VertebraSyncConfigBase):
    """Synchronous Vertebra broker-backed MySQL configuration."""

    driver_type: ClassVar[type[Any]] = VertebraMysqlSyncDriver
    default_statement_config: ClassVar["StatementConfig"] = default_statement_config
    default_pool_name: ClassVar[str] = "mysql"
    supports_transactional_ddl: ClassVar[bool] = False
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
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MysqlDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        features = dict(driver_features or {})
        features.setdefault("data_dictionary_factory", resolve_sync_data_dictionary("mysql"))
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


class VertebraMysqlAsyncConfig(VertebraAsyncConfigBase):
    """Asynchronous Vertebra broker-backed MySQL configuration."""

    driver_type: ClassVar[type[Any]] = VertebraMysqlAsyncDriver
    default_statement_config: ClassVar["StatementConfig"] = default_statement_config
    default_pool_name: ClassVar[str] = "mysql"
    supports_transactional_ddl: ClassVar[bool] = False
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
        connection_instance: "Any" = None,
        migration_config: "dict[str, Any] | None" = None,
        statement_config: "StatementConfig | None" = None,
        driver_features: "MysqlDriverFeatures | dict[str, Any] | None" = None,
        bind_key: "str | None" = None,
        extension_config: "ExtensionConfigs | None" = None,
        observability_config: "ObservabilityConfig | None" = None,
        **kwargs: Any,
    ) -> None:
        features = dict(driver_features or {})
        features.setdefault("data_dictionary_factory", resolve_async_data_dictionary("mysql"))
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


__all__ = ("VertebraMysqlAsyncConfig", "VertebraMysqlSyncConfig", "MysqlDriverFeatures", "default_statement_config")
