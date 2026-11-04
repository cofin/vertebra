"""Base configuration, typed database exceptions, and driver classes for Vertebra database drivers."""

from __future__ import annotations

from vertebra._vertebra import (
    ConnectionFailure,
    DatabaseError,
    DataError,
    IntegrityError,
    InterfaceError,
    InternalError,
    NotFoundError,
    NotSupportedError,
    OperationalError,
    PoolExhaustedError,
    ProgrammingError,
    QueryCancelledError,
    SerializationFailure,
)
from vertebra.db.driver._async import (
    AsyncDatabaseDriver,
    ThreadOffloadAsyncDatabaseDriver,
)
from vertebra.db.driver._common import (
    BaseDatabaseConfig,
    CommonDriverMixin,
    _normalize_driver_params,
)
from vertebra.db.driver._sync import SyncDatabaseDriver

__all__ = (
    "AsyncDatabaseDriver",
    "BaseDatabaseConfig",
    "CommonDriverMixin",
    "ConnectionFailure",
    "DataError",
    "DatabaseError",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotFoundError",
    "NotSupportedError",
    "OperationalError",
    "PoolExhaustedError",
    "ProgrammingError",
    "QueryCancelledError",
    "SerializationFailure",
    "SyncDatabaseDriver",
    "ThreadOffloadAsyncDatabaseDriver",
    "_normalize_driver_params",
)
