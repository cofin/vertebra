"""Broker-backed SQLite SQLSpec drivers."""

from typing import TYPE_CHECKING, ClassVar

from vertebra.drivers.broker import AsyncBrokerCursor, SyncBrokerCursor
from vertebra.extensions.sqlspec._driver import (
    BrokerAsyncDriverBase,
    BrokerAsyncExceptionHandler,
    BrokerExceptionHandler,
    BrokerSyncDriverBase,
)
from vertebra.extensions.sqlspec.sqlite._typing import (
    VertebraSqliteAsyncConnection,
    VertebraSqliteAsyncSessionContext,
    VertebraSqliteSyncConnection,
    VertebraSqliteSyncSessionContext,
)
from vertebra.extensions.sqlspec.sqlite.data_dictionary import (
    VertebraSqliteAsyncDataDictionary,
    VertebraSqliteSyncDataDictionary,
)

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

_SYNC_DICTIONARY_FACTORY = VertebraSqliteSyncDataDictionary
_ASYNC_DICTIONARY_FACTORY = VertebraSqliteAsyncDataDictionary

__all__ = (
    "VertebraSqliteAsyncCursor",
    "VertebraSqliteAsyncDriver",
    "VertebraSqliteAsyncExceptionHandler",
    "VertebraSqliteAsyncSessionContext",
    "VertebraSqliteSyncCursor",
    "VertebraSqliteSyncDriver",
    "VertebraSqliteSyncExceptionHandler",
    "VertebraSqliteSyncSessionContext",
)


class VertebraSqliteSyncCursor:
    """Context manager for broker-backed sync cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: VertebraSqliteSyncConnection) -> None:
        self.connection = connection
        self.cursor: SyncBrokerCursor | None = None

    def __enter__(self) -> SyncBrokerCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            self.cursor.close()


class VertebraSqliteAsyncCursor:
    """Async context manager for broker-backed cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: VertebraSqliteAsyncConnection) -> None:
        self.connection = connection
        self.cursor: AsyncBrokerCursor | None = None

    async def __aenter__(self) -> AsyncBrokerCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            await self.cursor.close()


class VertebraSqliteSyncExceptionHandler(BrokerExceptionHandler):
    """Sync exception handler for broker-backed SQLite."""


class VertebraSqliteAsyncExceptionHandler(BrokerAsyncExceptionHandler):
    """Async exception handler for broker-backed SQLite."""


class VertebraSqliteSyncDriver(BrokerSyncDriverBase):
    """Synchronous Vertebra broker-backed SQLite driver."""

    dialect: ClassVar[str] = "sqlite"

    def __init__(
        self,
        connection: VertebraSqliteSyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_SYNC_DICTIONARY_FACTORY,
        )


class VertebraSqliteAsyncDriver(BrokerAsyncDriverBase):
    """Asynchronous Vertebra broker-backed SQLite driver."""

    dialect: ClassVar[str] = "sqlite"

    def __init__(
        self,
        connection: VertebraSqliteAsyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_ASYNC_DICTIONARY_FACTORY,
        )
