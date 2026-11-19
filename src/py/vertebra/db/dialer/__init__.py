"""Native zero-sidecar Cloud SQL, AlloyDB, AWS RDS/Aurora/DSQL, and Azure Entra ID dialer package."""

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
    from vertebra.db.dialer.aws import (
        AsyncAwsAuroraDsqlDialer,
        AsyncAwsRdsDialer,
        SyncAwsAuroraDsqlDialer,
        SyncAwsRdsDialer,
        aws_rds_root_ca_pem,
    )
    from vertebra.db.dialer.azure import (
        AsyncAzureEntraDialer,
        SyncAzureEntraDialer,
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
    "AsyncAwsAuroraDsqlDialer",
    "AsyncAwsRdsDialer",
    "AsyncAzureEntraDialer",
    "AsyncCloudSQLDialer",
    "CloudSQLConfig",
    "CloudSQLInstanceURI",
    "EphemeralCertBundle",
    "SyncAlloyDBDialer",
    "SyncAwsAuroraDsqlDialer",
    "SyncAwsRdsDialer",
    "SyncAzureEntraDialer",
    "SyncCloudSQLDialer",
    "aws_rds_root_ca_pem",
    "generate_ephemeral_keypair",
)

__lazy_modules__ = [
    "vertebra.db.dialer.alloydb",
    "vertebra.db.dialer.aws",
    "vertebra.db.dialer.azure",
    "vertebra.db.dialer.cloudsql",
]

_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AlloyDBAuthType": ("vertebra.db.dialer.alloydb", "AlloyDBAuthType"),
    "AlloyDBClusterURI": ("vertebra.db.dialer.alloydb", "AlloyDBClusterURI"),
    "AlloyDBConfig": ("vertebra.db.dialer.alloydb", "AlloyDBConfig"),
    "AsyncAlloyDBDialer": ("vertebra.db.dialer.alloydb", "AsyncAlloyDBDialer"),
    "AsyncAwsAuroraDsqlDialer": ("vertebra.db.dialer.aws", "AsyncAwsAuroraDsqlDialer"),
    "AsyncAwsRdsDialer": ("vertebra.db.dialer.aws", "AsyncAwsRdsDialer"),
    "AsyncAzureEntraDialer": ("vertebra.db.dialer.azure", "AsyncAzureEntraDialer"),
    "AsyncCloudSQLDialer": ("vertebra.db.dialer.cloudsql", "AsyncCloudSQLDialer"),
    "CloudSQLConfig": ("vertebra.db.dialer.cloudsql", "CloudSQLConfig"),
    "CloudSQLInstanceURI": ("vertebra.db.dialer.cloudsql", "CloudSQLInstanceURI"),
    "EphemeralCertBundle": ("vertebra.db.dialer.cloudsql", "EphemeralCertBundle"),
    "SyncAlloyDBDialer": ("vertebra.db.dialer.alloydb", "SyncAlloyDBDialer"),
    "SyncAwsAuroraDsqlDialer": ("vertebra.db.dialer.aws", "SyncAwsAuroraDsqlDialer"),
    "SyncAwsRdsDialer": ("vertebra.db.dialer.aws", "SyncAwsRdsDialer"),
    "SyncAzureEntraDialer": ("vertebra.db.dialer.azure", "SyncAzureEntraDialer"),
    "SyncCloudSQLDialer": ("vertebra.db.dialer.cloudsql", "SyncCloudSQLDialer"),
    "aws_rds_root_ca_pem": ("vertebra.db.dialer.aws", "aws_rds_root_ca_pem"),
    "generate_ephemeral_keypair": ("vertebra.db.dialer.cloudsql", "generate_ephemeral_keypair"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
