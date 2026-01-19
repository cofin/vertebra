"""Broker-backed MySQL SQLSpec drivers."""

from typing import Any, ClassVar

from vertebra.extensions.sqlspec._dictionary import resolve_async_data_dictionary, resolve_sync_data_dictionary
from vertebra.extensions.sqlspec._driver import BrokerAsyncDriverBase, BrokerSyncDriverBase

_SYNC_DICTIONARY_FACTORY = resolve_sync_data_dictionary("mysql")
_ASYNC_DICTIONARY_FACTORY = resolve_async_data_dictionary("mysql")


class VertebraMysqlSyncDriver(BrokerSyncDriverBase):
    """Synchronous Vertebra broker-backed MySQL driver."""

    dialect: ClassVar[str] = "mysql"

    def __init__(
        self, connection: Any, statement_config: Any = None, driver_features: "dict[str, Any] | None" = None
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_SYNC_DICTIONARY_FACTORY,
        )


class VertebraMysqlAsyncDriver(BrokerAsyncDriverBase):
    """Asynchronous Vertebra broker-backed MySQL driver."""

    dialect: ClassVar[str] = "mysql"

    def __init__(
        self, connection: Any, statement_config: Any = None, driver_features: "dict[str, Any] | None" = None
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_ASYNC_DICTIONARY_FACTORY,
        )


__all__ = ("VertebraMysqlAsyncDriver", "VertebraMysqlSyncDriver")
