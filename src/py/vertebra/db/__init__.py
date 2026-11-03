"""Native database drivers, Cloud SQL / AlloyDB dialers, Valkey/Redis, and DataFusion subsystem."""

from __future__ import annotations

from vertebra.db._arrow import (
    ARROW_SHM_THRESHOLD_ROWS,
    ArrowCursor,
)
from vertebra.db._registry import DatabaseRegistry
from vertebra.db.cdc import (
    CDCConfig,
    ChangeEvent,
    ChangeFeed,
)
from vertebra.db.channels import DatabaseChannelBackend
from vertebra.db.datafusion import (
    ARROW_STREAM_MEDIA_TYPE,
    DataFusionConfig,
    DataFusionEngine,
    DataFusionMode,
    TableRegistration,
)
from vertebra.db.dialer import (
    AlloyDBAuthType,
    AlloyDBClusterURI,
    AlloyDBConfig,
    AlloyDBDialer,
    CloudSQLConfig,
    CloudSQLDialer,
    CloudSQLInstanceURI,
)
from vertebra.db.duckdb import (
    AsyncDuckDBDriver,
    DuckDBConfig,
    DuckDBSecretConfig,
    SyncDuckDBDriver,
    duckdb_bootstrap_statements,
)
from vertebra.db.mssql import (
    AsyncMSSQLDriver,
    MSSQLConfig,
    SyncMSSQLDriver,
    encode_tds_sql_batch,
    rewrite_mssql_placeholders,
)
from vertebra.db.mysql import AsyncMySQLDriver, MySQLConfig, SyncMySQLDriver
from vertebra.db.oracle import (
    AsyncOracleDriver,
    OracleConfig,
    OracleConnectDescriptor,
    OracleDriverFeatures,
    OracleSessionAttrs,
    OracleTopologyConfig,
    OracleWallet,
    SyncOracleDriver,
    decode_oson,
    decode_vector_binary,
    decode_vector_f32,
    decode_vector_f64,
    decode_vector_i8,
    encode_oson,
    encode_ttc_session_attrs,
    encode_vector_binary,
    encode_vector_f32,
    encode_vector_f64,
    encode_vector_i8,
    parse_tnsnames_ora,
    resolve_tns_alias,
)
from vertebra.db.pipeline import (
    CTEStep,
    QueryPipeline,
    RelOverlay,
)
from vertebra.db.postgres import (
    AsyncPostgreSQLDriver,
    PoolerMode,
    PostgreSQLConfig,
    SyncPostgreSQLDriver,
)
from vertebra.db.redis import (
    AsyncRedisDriver,
    RedisConfig,
    SyncRedisDriver,
)
from vertebra.db.rls import (
    TenantPredicate,
    TenantScopeConfig,
)
from vertebra.db.sqlite import AsyncSQLiteDriver, SQLiteConfig, SyncSQLiteDriver
from vertebra.db.valkey import (
    AsyncValkeyDriver,
    SyncValkeyDriver,
    ValkeyConfig,
)

__all__ = (
    "ARROW_SHM_THRESHOLD_ROWS",
    "ARROW_STREAM_MEDIA_TYPE",
    "AlloyDBAuthType",
    "AlloyDBClusterURI",
    "AlloyDBConfig",
    "AlloyDBDialer",
    "ArrowCursor",
    "AsyncDuckDBDriver",
    "AsyncMSSQLDriver",
    "AsyncMySQLDriver",
    "AsyncOracleDriver",
    "AsyncPostgreSQLDriver",
    "AsyncRedisDriver",
    "AsyncSQLiteDriver",
    "AsyncValkeyDriver",
    "CDCConfig",
    "CTEStep",
    "ChangeEvent",
    "ChangeFeed",
    "CloudSQLConfig",
    "CloudSQLDialer",
    "CloudSQLInstanceURI",
    "DataFusionConfig",
    "DataFusionEngine",
    "DataFusionMode",
    "DatabaseChannelBackend",
    "DatabaseRegistry",
    "DuckDBConfig",
    "DuckDBSecretConfig",
    "MSSQLConfig",
    "MySQLConfig",
    "OracleConfig",
    "OracleConnectDescriptor",
    "OracleDriverFeatures",
    "OracleSessionAttrs",
    "OracleTopologyConfig",
    "OracleWallet",
    "PoolerMode",
    "PostgreSQLConfig",
    "QueryPipeline",
    "RedisConfig",
    "RelOverlay",
    "SQLiteConfig",
    "SyncDuckDBDriver",
    "SyncMSSQLDriver",
    "SyncMySQLDriver",
    "SyncOracleDriver",
    "SyncPostgreSQLDriver",
    "SyncRedisDriver",
    "SyncSQLiteDriver",
    "SyncValkeyDriver",
    "TableRegistration",
    "TenantPredicate",
    "TenantScopeConfig",
    "ValkeyConfig",
    "decode_oson",
    "decode_vector_binary",
    "decode_vector_f32",
    "decode_vector_f64",
    "decode_vector_i8",
    "duckdb_bootstrap_statements",
    "encode_oson",
    "encode_tds_sql_batch",
    "encode_ttc_session_attrs",
    "encode_vector_binary",
    "encode_vector_f32",
    "encode_vector_f64",
    "encode_vector_i8",
    "parse_tnsnames_ora",
    "resolve_tns_alias",
    "rewrite_mssql_placeholders",
)
