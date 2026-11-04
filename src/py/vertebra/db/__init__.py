"""Database drivers, Cloud SQL and AlloyDB dialers, CDC, RLS, and Arrow IPC cursors."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.arrow import (
        ARROW_SHM_THRESHOLD_ROWS,
        ArrowCursor,
    )
    from vertebra.db.cdc import (
        CDCConfig,
        ChangeEvent,
        ChangeFeed,
    )
    from vertebra.db.dialer import (
        AlloyDBAuthType,
        AlloyDBClusterURI,
        AlloyDBConfig,
        AsyncAlloyDBDialer,
        AsyncCloudSQLDialer,
        CloudSQLConfig,
        CloudSQLInstanceURI,
        SyncAlloyDBDialer,
        SyncCloudSQLDialer,
    )
    from vertebra.db.driver import (
        AsyncDatabaseDriver,
        BaseDatabaseConfig,
        CommonDriverMixin,
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
        SyncDatabaseDriver,
        ThreadOffloadAsyncDatabaseDriver,
    )
    from vertebra.db.registry import (
        DatabaseRegistry,
        provide_bigquery_driver,
        provide_spanner_driver,
    )
    from vertebra.db.rls import (
        TenantPredicate,
        TenantScopeConfig,
    )

__all__ = (
    "ARROW_SHM_THRESHOLD_ROWS",
    "AlloyDBAuthType",
    "AlloyDBClusterURI",
    "AlloyDBConfig",
    "ArrowCursor",
    "AsyncAlloyDBDialer",
    "AsyncCloudSQLDialer",
    "AsyncDatabaseDriver",
    "BaseDatabaseConfig",
    "CDCConfig",
    "ChangeEvent",
    "ChangeFeed",
    "CloudSQLConfig",
    "CloudSQLInstanceURI",
    "CommonDriverMixin",
    "ConnectionFailure",
    "DataError",
    "DatabaseError",
    "DatabaseRegistry",
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
    "SyncAlloyDBDialer",
    "SyncCloudSQLDialer",
    "SyncDatabaseDriver",
    "TenantPredicate",
    "TenantScopeConfig",
    "ThreadOffloadAsyncDatabaseDriver",
    "provide_bigquery_driver",
    "provide_spanner_driver",
)

__lazy_modules__ = [
    "vertebra.db.arrow",
    "vertebra.db.cdc",
    "vertebra.db.dialer",
    "vertebra.db.driver",
    "vertebra.db.registry",
    "vertebra.db.rls",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "ARROW_SHM_THRESHOLD_ROWS": ("vertebra.db.arrow", "ARROW_SHM_THRESHOLD_ROWS"),
    "AlloyDBAuthType": ("vertebra.db.dialer", "AlloyDBAuthType"),
    "AlloyDBClusterURI": ("vertebra.db.dialer", "AlloyDBClusterURI"),
    "AlloyDBConfig": ("vertebra.db.dialer", "AlloyDBConfig"),
    "ArrowCursor": ("vertebra.db.arrow", "ArrowCursor"),
    "AsyncAlloyDBDialer": ("vertebra.db.dialer", "AsyncAlloyDBDialer"),
    "AsyncCloudSQLDialer": ("vertebra.db.dialer", "AsyncCloudSQLDialer"),
    "AsyncDatabaseDriver": ("vertebra.db.driver", "AsyncDatabaseDriver"),
    "BaseDatabaseConfig": ("vertebra.db.driver", "BaseDatabaseConfig"),
    "CDCConfig": ("vertebra.db.cdc", "CDCConfig"),
    "ChangeEvent": ("vertebra.db.cdc", "ChangeEvent"),
    "ChangeFeed": ("vertebra.db.cdc", "ChangeFeed"),
    "CloudSQLConfig": ("vertebra.db.dialer", "CloudSQLConfig"),
    "CloudSQLInstanceURI": ("vertebra.db.dialer", "CloudSQLInstanceURI"),
    "CommonDriverMixin": ("vertebra.db.driver", "CommonDriverMixin"),
    "ConnectionFailure": ("vertebra.db.driver", "ConnectionFailure"),
    "DataError": ("vertebra.db.driver", "DataError"),
    "DatabaseError": ("vertebra.db.driver", "DatabaseError"),
    "DatabaseRegistry": ("vertebra.db.registry", "DatabaseRegistry"),
    "IntegrityError": ("vertebra.db.driver", "IntegrityError"),
    "InterfaceError": ("vertebra.db.driver", "InterfaceError"),
    "InternalError": ("vertebra.db.driver", "InternalError"),
    "NotFoundError": ("vertebra.db.driver", "NotFoundError"),
    "NotSupportedError": ("vertebra.db.driver", "NotSupportedError"),
    "OperationalError": ("vertebra.db.driver", "OperationalError"),
    "PoolExhaustedError": ("vertebra.db.driver", "PoolExhaustedError"),
    "ProgrammingError": ("vertebra.db.driver", "ProgrammingError"),
    "QueryCancelledError": ("vertebra.db.driver", "QueryCancelledError"),
    "SerializationFailure": ("vertebra.db.driver", "SerializationFailure"),
    "SyncAlloyDBDialer": ("vertebra.db.dialer", "SyncAlloyDBDialer"),
    "SyncCloudSQLDialer": ("vertebra.db.dialer", "SyncCloudSQLDialer"),
    "SyncDatabaseDriver": ("vertebra.db.driver", "SyncDatabaseDriver"),
    "TenantPredicate": ("vertebra.db.rls", "TenantPredicate"),
    "TenantScopeConfig": ("vertebra.db.rls", "TenantScopeConfig"),
    "ThreadOffloadAsyncDatabaseDriver": ("vertebra.db.driver", "ThreadOffloadAsyncDatabaseDriver"),
    "provide_bigquery_driver": ("vertebra.db.registry", "provide_bigquery_driver"),
    "provide_spanner_driver": ("vertebra.db.registry", "provide_spanner_driver"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
