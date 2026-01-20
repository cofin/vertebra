"""Broker-backed MySQL SQLSpec drivers."""

from typing import TYPE_CHECKING, ClassVar

from vertebra.drivers.broker import AsyncBrokerCursor, SyncBrokerCursor
from vertebra.extensions.sqlspec._driver import (
    BrokerAsyncDriverBase,
    BrokerAsyncExceptionHandler,
    BrokerExceptionHandler,
    BrokerSyncDriverBase,
)
from vertebra.extensions.sqlspec.mysql._typing import (
    VertebraMysqlAsyncConnection,
    VertebraMysqlAsyncSessionContext,
    VertebraMysqlSyncConnection,
    VertebraMysqlSyncSessionContext,
)
from vertebra.extensions.sqlspec.mysql.data_dictionary import (
    VertebraMysqlAsyncDataDictionary,
    VertebraMysqlSyncDataDictionary,
)

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

_SYNC_DICTIONARY_FACTORY = VertebraMysqlSyncDataDictionary
_ASYNC_DICTIONARY_FACTORY = VertebraMysqlAsyncDataDictionary

__all__ = (
    "VertebraMysqlAsyncCursor",
    "VertebraMysqlAsyncDriver",
    "VertebraMysqlAsyncExceptionHandler",
    "VertebraMysqlAsyncSessionContext",
    "VertebraMysqlSyncCursor",
    "VertebraMysqlSyncDriver",
    "VertebraMysqlSyncExceptionHandler",
    "VertebraMysqlSyncSessionContext",
)


class VertebraMysqlSyncCursor:
    """Context manager for broker-backed sync cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: VertebraMysqlSyncConnection) -> None:
        self.connection = connection
        self.cursor: SyncBrokerCursor | None = None

    def __enter__(self) -> SyncBrokerCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            self.cursor.close()


class VertebraMysqlAsyncCursor:
    """Async context manager for broker-backed cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: VertebraMysqlAsyncConnection) -> None:
        self.connection = connection
        self.cursor: AsyncBrokerCursor | None = None

    async def __aenter__(self) -> AsyncBrokerCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            await self.cursor.close()


class VertebraMysqlSyncExceptionHandler(BrokerExceptionHandler):
    """Sync exception handler for broker-backed MySQL."""


class VertebraMysqlAsyncExceptionHandler(BrokerAsyncExceptionHandler):
    """Async exception handler for broker-backed MySQL."""


class VertebraMysqlSyncDriver(BrokerSyncDriverBase):
    """Synchronous Vertebra broker-backed MySQL driver."""

    dialect: ClassVar[str] = "mysql"

    def __init__(
        self,
        connection: VertebraMysqlSyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
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
        self,
        connection: VertebraMysqlAsyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_ASYNC_DICTIONARY_FACTORY,
        )
