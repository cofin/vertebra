"""Base classes for Vertebra SQLAlchemy dialects."""

from typing import Any

from sqlalchemy.engine import URL
from sqlalchemy.engine.default import DefaultDialect

from ._dbapi import connect


class VertebraDialectBase(DefaultDialect):
    """Base dialect for Vertebra broker-backed connections."""

    driver = "vertebra"
    supports_statement_cache = True
    supports_multivalues_insert = True
    supports_is_distinct_from = True
    supports_native_boolean = True

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
        # TODO: Implement proper disconnect check based on broker errors
        return False
