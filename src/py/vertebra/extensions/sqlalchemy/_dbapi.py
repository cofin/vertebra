"""PEP 249 DBAPI implementation for Vertebra broker."""

# ruff: noqa: A001
import contextlib
from collections.abc import Sequence
from typing import Any

from vertebra.database.backends.broker import VertebraSyncConnection, VertebraSyncCursor
from vertebra.messaging.broker import SyncBrokerClient

# DBAPI Globals
apilevel = "2.0"
threadsafety = 1  # Threads can share the module, but not connections
paramstyle = "qmark"  # Can be swapped to 'numeric' via config


# DBAPI Exceptions
class Error(Exception):
    """Base class for all other error exceptions."""


class DBAPIError(Exception):
    """Exception raised for important warnings like data truncations (PEP 249)."""


Warning = DBAPIError  # PEP 249 requires this name


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

    def __init__(self, pool_name: str, client: SyncBrokerClient | None = None, paramstyle: str = paramstyle) -> None:
        self.pool_name = pool_name
        self._client = client or SyncBrokerClient()
        self.paramstyle = paramstyle
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

    def begin(self) -> None:
        if not self._transaction_in_progress:
            try:
                self.connection.begin()
                self._transaction_in_progress = True
            except Exception as e:
                raise DatabaseError(str(e)) from e

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

    def savepoint(self, name: str) -> None:
        try:
            self.connection.savepoint(name)
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def release_savepoint(self, name: str) -> None:
        try:
            self.connection.release_savepoint(name)
        except Exception as e:
            raise DatabaseError(str(e)) from e

    def rollback_savepoint(self, name: str) -> None:
        try:
            self.connection.rollback_savepoint(name)
        except Exception as e:
            raise DatabaseError(str(e)) from e

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

            # Handle parameter style conversion if needed
            # Postgres (numeric) uses :1, :2 in SQLAlchemy, but many PG drivers expect $1, $2
            # We check the module level paramstyle of the calling module or just this one.
            # Actually, we should probably check if it was set to 'numeric'.
            import re

            # This is a bit hacky but if paramstyle is 'numeric', we expect :1, :2
            # We'll convert them to $1, $2 which is standard for Postgres.
            if getattr(self.connection, "paramstyle", paramstyle) == "numeric" and parameters:
                operation = re.sub(r":(\d+)", r"$\1", operation)

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
    # Use global paramstyle from the module where connect is called if possible,
    # but since it's imported, we'll just use the one from this module unless passed.
    # Actually, the most robust way is for the sub-dbapi to pass it or for us to
    # detect the calling module's global.
    style = kwargs.pop("paramstyle", paramstyle)
    return Connection(dsn, paramstyle=style, **kwargs)
