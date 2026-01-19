"""Broker-backed PostgreSQL SQLSpec drivers."""

from typing import Any, ClassVar

from vertebra.extensions.sqlspec._dictionary import resolve_async_data_dictionary, resolve_sync_data_dictionary
from vertebra.extensions.sqlspec._driver import BrokerAsyncDriverBase, BrokerSyncDriverBase

_SYNC_DICTIONARY_FACTORY = resolve_sync_data_dictionary("postgres")
_ASYNC_DICTIONARY_FACTORY = resolve_async_data_dictionary("postgres")


class BrokerPostgresSyncDriver(BrokerSyncDriverBase):
    """Synchronous broker-backed PostgreSQL driver."""

    dialect: ClassVar[str] = "postgres"

    def __init__(
        self,
        connection: Any,
        statement_config: Any = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_SYNC_DICTIONARY_FACTORY,
        )


class BrokerPostgresAsyncDriver(BrokerAsyncDriverBase):
    """Asynchronous broker-backed PostgreSQL driver."""

    dialect: ClassVar[str] = "postgres"

    def __init__(
        self,
        connection: Any,
        statement_config: Any = None,
        driver_features: "dict[str, Any] | None" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_ASYNC_DICTIONARY_FACTORY,
        )


__all__ = ("BrokerPostgresAsyncDriver", "BrokerPostgresSyncDriver")
