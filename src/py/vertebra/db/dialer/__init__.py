"""Native zero-sidecar Cloud SQL, AlloyDB, AWS RDS/Aurora/DSQL, and Azure Entra ID dialer package."""

from __future__ import annotations

from typing import TYPE_CHECKING

from vertebra.utils.module_loader import lazy_exports

if TYPE_CHECKING:
    from vertebra.db.dialer._base import (
        DEFAULT_CONNECTOR_RETRY_COUNT,
        DEFAULT_CONNECTOR_RETRY_DELAY,
        DEFAULT_CONNECTOR_RETRY_MAX_DELAY,
        calculate_connector_backoff_seconds,
        is_transient_connector_error,
        validate_connector_retry_config,
    )
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
    "DEFAULT_CONNECTOR_RETRY_COUNT",
    "DEFAULT_CONNECTOR_RETRY_DELAY",
    "DEFAULT_CONNECTOR_RETRY_MAX_DELAY",
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
    "calculate_connector_backoff_seconds",
    "generate_ephemeral_keypair",
    "is_transient_connector_error",
    "validate_connector_retry_config",
)

__lazy_modules__ = [
    "vertebra.db.dialer._base",
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
    "DEFAULT_CONNECTOR_RETRY_COUNT": ("vertebra.db.dialer._base", "DEFAULT_CONNECTOR_RETRY_COUNT"),
    "DEFAULT_CONNECTOR_RETRY_DELAY": ("vertebra.db.dialer._base", "DEFAULT_CONNECTOR_RETRY_DELAY"),
    "DEFAULT_CONNECTOR_RETRY_MAX_DELAY": ("vertebra.db.dialer._base", "DEFAULT_CONNECTOR_RETRY_MAX_DELAY"),
    "EphemeralCertBundle": ("vertebra.db.dialer.cloudsql", "EphemeralCertBundle"),
    "SyncAlloyDBDialer": ("vertebra.db.dialer.alloydb", "SyncAlloyDBDialer"),
    "SyncAwsAuroraDsqlDialer": ("vertebra.db.dialer.aws", "SyncAwsAuroraDsqlDialer"),
    "SyncAwsRdsDialer": ("vertebra.db.dialer.aws", "SyncAwsRdsDialer"),
    "SyncAzureEntraDialer": ("vertebra.db.dialer.azure", "SyncAzureEntraDialer"),
    "SyncCloudSQLDialer": ("vertebra.db.dialer.cloudsql", "SyncCloudSQLDialer"),
    "aws_rds_root_ca_pem": ("vertebra.db.dialer.aws", "aws_rds_root_ca_pem"),
    "calculate_connector_backoff_seconds": ("vertebra.db.dialer._base", "calculate_connector_backoff_seconds"),
    "generate_ephemeral_keypair": ("vertebra.db.dialer.cloudsql", "generate_ephemeral_keypair"),
    "is_transient_connector_error": ("vertebra.db.dialer._base", "is_transient_connector_error"),
    "validate_connector_retry_config": ("vertebra.db.dialer._base", "validate_connector_retry_config"),
}

__getattr__, __dir__ = lazy_exports(__name__, __all__, _EXPORT_MAP)
