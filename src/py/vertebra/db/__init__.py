"""Native database drivers, Cloud SQL / AlloyDB dialers, Valkey/Redis, and DataFusion subsystem."""

from __future__ import annotations

from vertebra.db._registry import DatabaseRegistry
from vertebra.db.cdc import (
    CdcConfig,
    ChangeEvent,
    ChangeFeed,
)
from vertebra.db.datafusion import (
    ARROW_STREAM_MEDIA_TYPE,
    DataFusionConfig,
    DataFusionEngine,
    DataFusionMode,
    TableRegistration,
)
from vertebra.db.dialer import (
    AlloyDbAuthType,
    AlloyDbClusterUri,
    AlloyDbConfig,
    AlloyDbDialer,
    CloudSqlConfig,
    CloudSqlDialer,
    CloudSqlInstanceUri,
)
from vertebra.db.duckdb import (
    AsyncDuckdbDriver,
    DuckdbConfig,
    SyncDuckdbDriver,
    duckdb_bootstrap_statements,
)
from vertebra.db.mssql import (
    AsyncMssqlDriver,
    MssqlConfig,
    SyncMssqlDriver,
    encode_tds_sql_batch,
    rewrite_mssql_placeholders,
)
from vertebra.db.mysql import AsyncMysqlDriver, MysqlConfig, SyncMysqlDriver
from vertebra.db.oracle import (
    AsyncOracleDriver,
    OracleConfig,
    OracleConnectDescriptor,
    OracleSessionAttrs,
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
    CteStep,
    QueryPipeline,
    RelOverlay,
)
from vertebra.db.postgres import (
    ARROW_SHM_THRESHOLD_ROWS,
    AsyncPostgresDriver,
    PoolerMode,
    PostgresConfig,
    SyncPostgresDriver,
    VertebraArrowCursor,
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
from vertebra.db.sqlite import AsyncSqliteDriver, SqliteConfig, SyncSqliteDriver
from vertebra.db.valkey import (
    AsyncValkeyDriver,
    SyncValkeyDriver,
    ValkeyConfig,
)

__all__ = (
    "ARROW_SHM_THRESHOLD_ROWS",
    "ARROW_STREAM_MEDIA_TYPE",
    "AlloyDbAuthType",
    "AlloyDbClusterUri",
    "AlloyDbConfig",
    "AlloyDbDialer",
    "AsyncDuckdbDriver",
    "AsyncMssqlDriver",
    "AsyncMysqlDriver",
    "AsyncOracleDriver",
    "AsyncPostgresDriver",
    "AsyncRedisDriver",
    "AsyncSqliteDriver",
    "AsyncValkeyDriver",
    "CdcConfig",
    "ChangeEvent",
    "ChangeFeed",
    "CloudSqlConfig",
    "CloudSqlDialer",
    "CloudSqlInstanceUri",
    "CteStep",
    "DataFusionConfig",
    "DataFusionEngine",
    "DataFusionMode",
    "DatabaseRegistry",
    "DuckdbConfig",
    "MssqlConfig",
    "MysqlConfig",
    "OracleConfig",
    "OracleConnectDescriptor",
    "OracleSessionAttrs",
    "OracleWallet",
    "PoolerMode",
    "PostgresConfig",
    "QueryPipeline",
    "RedisConfig",
    "RelOverlay",
    "SqliteConfig",
    "SyncDuckdbDriver",
    "SyncMssqlDriver",
    "SyncMysqlDriver",
    "SyncOracleDriver",
    "SyncPostgresDriver",
    "SyncRedisDriver",
    "SyncSqliteDriver",
    "SyncValkeyDriver",
    "TableRegistration",
    "TenantPredicate",
    "TenantScopeConfig",
    "ValkeyConfig",
    "VertebraArrowCursor",
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
