"""Standalone PEP 249 DB-API 2.0 interfaces backed by Vertebra drivers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.dbapi._async import (
        AsyncDBAPIConnection,
        AsyncDBAPICursor,
        AsyncDBAPIModule,
    )
    from vertebra.db.dbapi._sync import (
        Binary,
        DatabaseError,
        DataError,
        Error,
        IntegrityError,
        InterfaceError,
        InternalError,
        NotSupportedError,
        OperationalError,
        ProgrammingError,
        SyncDBAPIConnection,
        SyncDBAPICursor,
        SyncDBAPIModule,
        Warning,
        is_row_returning_sql,
    )

__all__ = (
    "AsyncDBAPIConnection",
    "AsyncDBAPICursor",
    "AsyncDBAPIModule",
    "Binary",
    "DataError",
    "DatabaseError",
    "Error",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "ProgrammingError",
    "SyncDBAPIConnection",
    "SyncDBAPICursor",
    "SyncDBAPIModule",
    "Warning",
    "is_row_returning_sql",
)

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AsyncDBAPIConnection": ("vertebra.db.dbapi._async", "AsyncDBAPIConnection"),
    "AsyncDBAPICursor": ("vertebra.db.dbapi._async", "AsyncDBAPICursor"),
    "AsyncDBAPIModule": ("vertebra.db.dbapi._async", "AsyncDBAPIModule"),
    "Binary": ("vertebra.db.dbapi._sync", "Binary"),
    "DataError": ("vertebra.db.dbapi._sync", "DataError"),
    "DatabaseError": ("vertebra.db.dbapi._sync", "DatabaseError"),
    "Error": ("vertebra.db.dbapi._sync", "Error"),
    "IntegrityError": ("vertebra.db.dbapi._sync", "IntegrityError"),
    "InterfaceError": ("vertebra.db.dbapi._sync", "InterfaceError"),
    "InternalError": ("vertebra.db.dbapi._sync", "InternalError"),
    "NotSupportedError": ("vertebra.db.dbapi._sync", "NotSupportedError"),
    "OperationalError": ("vertebra.db.dbapi._sync", "OperationalError"),
    "ProgrammingError": ("vertebra.db.dbapi._sync", "ProgrammingError"),
    "SyncDBAPIConnection": ("vertebra.db.dbapi._sync", "SyncDBAPIConnection"),
    "SyncDBAPICursor": ("vertebra.db.dbapi._sync", "SyncDBAPICursor"),
    "SyncDBAPIModule": ("vertebra.db.dbapi._sync", "SyncDBAPIModule"),
    "Warning": ("vertebra.db.dbapi._sync", "Warning"),
    "is_row_returning_sql": ("vertebra.db.dbapi._sync", "is_row_returning_sql"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
