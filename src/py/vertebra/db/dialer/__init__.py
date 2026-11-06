"""Native zero-sidecar Cloud SQL (:3307) and AlloyDB (:5433) dialer package."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.dialer.alloydb import (
        AlloyDBAuthType,
        AlloyDBClusterURI,
        AlloyDBConfig,
        AsyncAlloyDBDialer,
        SyncAlloyDBDialer,
    )
    from vertebra.db.dialer.cloudsql import (
        AsyncCloudSQLDialer,
        CloudSQLConfig,
        CloudSQLInstanceURI,
        EphemeralCertBundle,
        SyncCloudSQLDialer,
        generate_ephemeral_keypair,
    )

__all__ = (
    "AlloyDBAuthType",
    "AlloyDBClusterURI",
    "AlloyDBConfig",
    "AsyncAlloyDBDialer",
    "AsyncCloudSQLDialer",
    "CloudSQLConfig",
    "CloudSQLInstanceURI",
    "EphemeralCertBundle",
    "SyncAlloyDBDialer",
    "SyncCloudSQLDialer",
    "generate_ephemeral_keypair",
)

__lazy_modules__ = [
    "vertebra.db.dialer.alloydb",
    "vertebra.db.dialer.cloudsql",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AlloyDBAuthType": ("vertebra.db.dialer.alloydb", "AlloyDBAuthType"),
    "AlloyDBClusterURI": ("vertebra.db.dialer.alloydb", "AlloyDBClusterURI"),
    "AlloyDBConfig": ("vertebra.db.dialer.alloydb", "AlloyDBConfig"),
    "AsyncAlloyDBDialer": ("vertebra.db.dialer.alloydb", "AsyncAlloyDBDialer"),
    "AsyncCloudSQLDialer": ("vertebra.db.dialer.cloudsql", "AsyncCloudSQLDialer"),
    "CloudSQLConfig": ("vertebra.db.dialer.cloudsql", "CloudSQLConfig"),
    "CloudSQLInstanceURI": ("vertebra.db.dialer.cloudsql", "CloudSQLInstanceURI"),
    "EphemeralCertBundle": ("vertebra.db.dialer.cloudsql", "EphemeralCertBundle"),
    "SyncAlloyDBDialer": ("vertebra.db.dialer.alloydb", "SyncAlloyDBDialer"),
    "SyncCloudSQLDialer": ("vertebra.db.dialer.cloudsql", "SyncCloudSQLDialer"),
    "generate_ephemeral_keypair": ("vertebra.db.dialer.cloudsql", "generate_ephemeral_keypair"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
