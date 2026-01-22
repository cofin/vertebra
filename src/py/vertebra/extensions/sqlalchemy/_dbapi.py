"""PEP 249 DBAPI implementation for Vertebra broker."""

import contextlib
from collections.abc import Sequence
from typing import Any

from vertebra.drivers.broker import VertebraSyncConnection, VertebraSyncCursor
from vertebra.resources.broker import SyncBrokerClient

# DBAPI Globals
apilevel = "2.0"
threadsafety = 1  # Threads can share the module, but not connections
paramstyle = "qmark"  # Can be swapped to 'numeric' via config


# DBAPI Exceptions
class Error(Exception):
    """Base class for all other error exceptions."""


class Warning(Exception):  # noqa: A001
    """Exception raised for important warnings like data truncations."""


class InterfaceError(Error):
    """Exception raised for errors that are related to the database interface rather than the database itself."""


class DatabaseError(Error):
    """Exception raised for errors that are related to the database."""


class InternalError(DatabaseError):
    """Exception raised when the database encounters an internal error."""


class OperationalError(DatabaseError):
    """Exception raised for errors that are related to the database's operation."""


class ProgrammingError(DatabaseError):
    """Exception raised for programming errors."""


class IntegrityError(DatabaseError):
    """Exception raised when the relational integrity of the database is affected."""


class DataError(DatabaseError):
    """Exception raised for errors that are due to problems with the processed data."""


class NotSupportedError(DatabaseError):
    """Exception raised in case a method or database API was used which is not supported by the database."""


class Connection:
    """DBAPI Connection."""

    def __init__(self, pool_name: str, client: SyncBrokerClient | None = None) -> None:
        self.pool_name = pool_name
        self._client = client or SyncBrokerClient()
        self._token: str | None = None
        self._connection: VertebraSyncConnection | None = None
        self._transaction_in_progress = False

    @property
    def connection(self) -> VertebraSyncConnection:
        if self._connection is None:
            # Lazy connect
            try:
                self._token = self._client.acquire(self.pool_name)
                self._connection = VertebraSyncConnection(self.pool_name, self._client, self._token)
            except Exception as e:
                raise OperationalError(str(e)) from e
        return self._connection

    def cursor(self) -> "Cursor":
        return Cursor(self)

    def commit(self) -> None:
        if self._transaction_in_progress:
            try:
                self.connection.commit()
            except Exception as e:
                raise DatabaseError(str(e)) from e
            self._transaction_in_progress = False

    def rollback(self) -> None:
        if self._transaction_in_progress:
            try:
                self.connection.rollback()
            except Exception as e:
                raise DatabaseError(str(e)) from e
            self._transaction_in_progress = False

    def close(self) -> None:
        if self._connection:
            with contextlib.suppress(Exception):
                self._connection.close()
            self._connection = None
            self._token = None


class Cursor:
    """DBAPI Cursor."""

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self._cursor: VertebraSyncCursor | None = None
        self.description: Sequence[Any] | None = None
        self.rowcount = -1

    @property
    def _inner_cursor(self) -> VertebraSyncCursor:
        if self._cursor is None:
            self._cursor = self.connection.connection.cursor()
        return self._cursor

    def execute(self, operation: str, parameters: Sequence[Any] | None = None) -> "Cursor":
        try:
            if not self.connection._transaction_in_progress:
                self.connection.connection.begin()
                self.connection._transaction_in_progress = True

            self.rowcount = self._inner_cursor.execute(operation, parameters) or -1
            # Update description based on columns
            if self._inner_cursor._columns:
                # name, type_code, display_size, internal_size, precision, scale, null_ok
                self.description = [(col, None, None, None, None, None, None) for col in self._inner_cursor._columns]
            else:
                self.description = None
        except Exception as e:
            raise DatabaseError(str(e)) from e
        return self

    def executemany(self, operation: str, seq_of_parameters: Sequence[Sequence[Any]]) -> "Cursor":
        try:
            if not self.connection._transaction_in_progress:
                self.connection.connection.begin()
                self.connection._transaction_in_progress = True

            self.rowcount = self._inner_cursor.executemany(operation, seq_of_parameters)
        except Exception as e:
            raise DatabaseError(str(e)) from e
        return self

    def fetchone(self) -> Any | None:
        return self._inner_cursor.fetchone()

    def fetchmany(self, size: int | None = None) -> Sequence[Any]:
        return self._inner_cursor.fetchmany(size)

    def fetchall(self) -> Sequence[Any]:
        return self._inner_cursor.fetchall()

    def close(self) -> None:
        if self._cursor:
            self._inner_cursor.close()
            self._cursor = None


def connect(dsn: str, **kwargs: Any) -> Connection:
    """Connect to the database.

    dsn: The pool name.
    """
    return Connection(dsn)
