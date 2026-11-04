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
from vertebra.db.driver.capabilities import (
    AsyncChangeSource,
    AsyncPipeline,
    AsyncPipelineCapable,
    ClaimedJob,
    EmbeddingPushdown,
    NotificationEvent,
    NotificationSource,
    QueueStore,
    SyncChangeSource,
    SyncPipeline,
    SyncPipelineCapable,
)

__all__ = (
    "AsyncChangeSource",
    "AsyncDatabaseDriver",
    "AsyncPipeline",
    "AsyncPipelineCapable",
    "BaseDatabaseConfig",
    "ClaimedJob",
    "CommonDriverMixin",
    "ConnectionFailure",
    "DataError",
    "DatabaseError",
    "EmbeddingPushdown",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "NotFoundError",
    "NotSupportedError",
    "NotificationEvent",
    "NotificationSource",
    "OperationalError",
    "PoolExhaustedError",
    "ProgrammingError",
    "QueryCancelledError",
    "QueueStore",
    "SerializationFailure",
    "SyncChangeSource",
    "SyncDatabaseDriver",
    "SyncPipeline",
    "SyncPipelineCapable",
    "ThreadOffloadAsyncDatabaseDriver",
    "_normalize_driver_params",
)
