"""Broker-backed PostgreSQL SQLSpec drivers."""

from typing import TYPE_CHECKING

from vertebra.database.backends.broker import VertebraAsyncCursor, VertebraSyncCursor
from vertebra.extensions.sqlspec._driver import (
    VertebraAsyncDriverBase,
    VertebraAsyncExceptionHandler,
    VertebraExceptionHandler,
    VertebraSyncDriverBase,
)
from vertebra.extensions.sqlspec.postgres._typing import (
    VertebraPostgresAsyncConnection,
    VertebraPostgresAsyncSessionContext,
    VertebraPostgresSyncConnection,
    VertebraPostgresSyncSessionContext,
)
from vertebra.extensions.sqlspec.postgres.data_dictionary import (
    VertebraPostgresAsyncDataDictionary,
    VertebraPostgresSyncDataDictionary,
)

if TYPE_CHECKING:
    from sqlspec.core import StatementConfig

_SYNC_DICTIONARY_FACTORY = VertebraPostgresSyncDataDictionary
_ASYNC_DICTIONARY_FACTORY = VertebraPostgresAsyncDataDictionary

__all__ = (
    "VertebraPostgresAsyncCursor",
    "VertebraPostgresAsyncDriver",
    "VertebraPostgresAsyncExceptionHandler",
    "VertebraPostgresAsyncSessionContext",
    "VertebraPostgresSyncCursor",
    "VertebraPostgresSyncDriver",
    "VertebraPostgresSyncExceptionHandler",
    "VertebraPostgresSyncSessionContext",
)


class VertebraPostgresSyncCursor:
    """Context manager for broker-backed sync cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: VertebraPostgresSyncConnection) -> None:
        self.connection = connection
        self.cursor: VertebraSyncCursor | None = None

    def __enter__(self) -> VertebraSyncCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, *_: object) -> None:
        if self.cursor is not None:
            self.cursor.close()


class VertebraPostgresAsyncCursor:
    """Async context manager for broker-backed cursors."""

    __slots__ = ("connection", "cursor")

    def __init__(self, connection: VertebraPostgresAsyncConnection) -> None:
        self.connection = connection
        self.cursor: VertebraAsyncCursor | None = None

    async def __aenter__(self) -> VertebraAsyncCursor:
        self.cursor = self.connection.cursor()
        return self.cursor

    async def __aexit__(self, *_: object) -> None:
        if self.cursor is not None:
            await self.cursor.close()


class VertebraPostgresSyncExceptionHandler(VertebraExceptionHandler):
    """Sync exception handler for broker-backed PostgreSQL."""


class VertebraPostgresAsyncExceptionHandler(VertebraAsyncExceptionHandler):
    """Async exception handler for broker-backed PostgreSQL."""


class VertebraPostgresSyncDriver(VertebraSyncDriverBase):
    """Synchronous Vertebra broker-backed PostgreSQL driver."""

    dialect: str = "postgres"

    def __init__(
        self,
        connection: VertebraPostgresSyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_SYNC_DICTIONARY_FACTORY,
        )


class VertebraPostgresAsyncDriver(VertebraAsyncDriverBase):
    """Asynchronous Vertebra broker-backed PostgreSQL driver."""

    dialect: str = "postgres"

    def __init__(
        self,
        connection: VertebraPostgresAsyncConnection,
        statement_config: "StatementConfig | None" = None,
        driver_features: "dict[str, object] | None" = None,
    ) -> None:
        super().__init__(
            connection=connection,
            statement_config=statement_config,
            driver_features=driver_features,
            data_dictionary_factory=_ASYNC_DICTIONARY_FACTORY,
        )
