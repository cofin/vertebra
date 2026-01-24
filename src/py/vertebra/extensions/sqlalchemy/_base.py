"""Base classes for Vertebra SQLAlchemy dialects."""

from typing import Any

from sqlalchemy.engine import URL
from sqlalchemy.engine.default import DefaultDialect


class VertebraDialectBase(DefaultDialect):
    """Base dialect for Vertebra broker-backed connections."""

    driver = "vertebra"
    supports_statement_cache = True
    supports_multivalues_insert = True
    supports_is_distinct_from = True
    supports_native_boolean = True
    supports_savepoints = True

    @classmethod
    def import_dbapi(cls) -> Any:
        """Import the DBAPI module."""
        from . import _dbapi

        return _dbapi

    def create_connect_args(self, url: URL) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Create connection arguments from URL.

        The URL host is treated as the pool name.
        """
        return (url.host,), {}

    def is_disconnect(self, e: Exception, connection: Any, cursor: Any) -> bool:
        """Check if the error is a disconnect error."""
        from vertebra.exceptions import BrokerError, IPCError

        from . import _dbapi

        if isinstance(e, (BrokerError, IPCError)):
            return True
        if isinstance(e, (ConnectionError, OSError, EOFError, BrokenPipeError)):
            return True
        if isinstance(e, (_dbapi.InterfaceError, _dbapi.OperationalError)):
            return True
        message = str(e).lower()
        return bool(
            "connection" in message and any(token in message for token in ("closed", "broken", "reset", "lost"))
        )

    def do_savepoint(self, connection: Any, name: str) -> None:
        """Create a savepoint using native connection method."""
        connection.connection.savepoint(name)

    def do_release_savepoint(self, connection: Any, name: str) -> None:
        """Release a savepoint using native connection method."""
        connection.connection.release_savepoint(name)

    def do_rollback_to_savepoint(self, connection: Any, name: str) -> None:
        """Rollback to a savepoint using native connection method."""
        connection.connection.rollback_savepoint(name)
