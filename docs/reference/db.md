# `vertebra.db` — Database Drivers, Analytics Engines & Cloud Dialers

Native database configurations, synchronous and asynchronous drivers (`SyncDatabaseDriver`, `AsyncDatabaseDriver`), capability protocols (`SyncPipelineCapable`, `AsyncPipelineCapable`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, `EmbeddingPushdown`), typed `DatabaseError` / `NotFoundError` exceptions, Apache Arrow shared-memory cursors (`ArrowCursor`), DataFusion federated analytics (`DataFusionConfig`, `DataFusionEngine`), Change Data Capture (`ChangeFeed`), dialect-agnostic tenant predicates (`TenantPredicate`), and zero-sidecar Cloud SQL / AlloyDB dialers. See the [Databases & Native Drivers Usage Guide](../usage/databases.md) for architectural patterns and examples.

| Category | Classes | Highlights |
| --- | --- | --- |
| **SQLSpec Adapters (Litestar)** | `PostgreSQLAsyncConfig`, `MySQLAsyncConfig`, `SQLiteAsyncConfig`, `OracleAsyncConfig`, `MSSQLAsyncConfig`, `DuckDBAsyncConfig`, `BigQueryAsyncConfig`, `SpannerAsyncConfig` | Canonical Litestar adapter configurations (`vertebra.extensions.sqlspec`) backed by engine drivers and connection leasing (Oracle sessions raise `MissingDependencyError` because the native Oracle driver package is not yet published) |
| **Registry, Contracts & Native Pipelines** | `DatabaseRegistry`, `provide_driver`, `BaseDatabaseConfig`, `SyncDatabaseDriver`, `AsyncDatabaseDriver`, `DatabaseError`, `NotFoundError`, `ArrowCursor` | Polymorphic URL scheme resolution, shared endpoint pool keys, uniform sync/async execution contracts, capability-based `driver.pipeline()` batching, and zero-copy Arrow IPC cursors |
| **Capability Protocols** | `SyncPipelineCapable`, `AsyncPipelineCapable`, `NotificationSource`, `SyncChangeSource`, `AsyncChangeSource`, `QueueStore`, `EmbeddingPushdown` | `@runtime_checkable` structural protocols for query pipelining, pub/sub notifications, CDC streams, `SKIP LOCKED` job queues, and in-database embedding pushdown |
| **Relational & OLAP Configs** | `PostgreSQLConfig`, `MySQLConfig`, `SQLiteConfig`, `OracleConfig`, `MSSQLConfig`, `DuckDBConfig`, `BigQueryConfig`, `SpannerConfig` | Pooler-aware statement caching, WAL/mmap pragmas, Oracle 26ai `VECTOR`/`OSON` & Cloud Wallets, SQL Server `@P1` placeholder rewriting, DuckDB Arrow streaming (`vertebra-core[duckdb]`), BigQuery Storage Read/Write v1 + Object Tables, and Cloud Spanner Multiplexed Sessions + `BatchWrite` |
| **RESP3 & Federated SQL** | `RedisConfig`, `ValkeyConfig`, `DataFusionConfig`, `DataFusionEngine` | RESP3 `HELLO 3` client-side cache invalidation, Cluster/Sentinel routing, and cross-engine SQL federation |
| **Cloud Dialers, CDC & RLS** | `AlloyDBConfig`, `SyncAlloyDBDialer`, `AsyncAlloyDBDialer`, `CloudSQLConfig`, `SyncCloudSQLDialer`, `AsyncCloudSQLDialer`, `CDCConfig`, `ChangeFeed`, `PostgresNotifyConfig`, `PostgresNotificationSource`, `SyncSpannerChangeStreamSource`, `AsyncSpannerChangeStreamSource`, `TenantScopeConfig`, `TenantPredicate` | Direct TCP/TLS IAM OAuth2 token password resolution (`:5432`, `:3306`), zero-sidecar `:5433` AlloyDB and `:3307` Cloud SQL mTLS connectors, logical replication / Spanner change-stream streaming, and index-friendly tenant isolation |

## Database Registry, Driver Contracts & Native Pipelines

`DatabaseRegistry` and `provide_driver` resolve polymorphic configurations and shared drivers from URL schemes, while `SyncDatabaseDriver` / `AsyncDatabaseDriver` define the uniform execution contract and `SyncPipelineCapable` / `AsyncPipelineCapable` expose native `driver.pipeline()` batching with ordered results.

::: vertebra.db.registry.DatabaseRegistry
    options:
      heading_level: 3
      members:
        - register
        - parse_url
        - get_driver
        - get_sync_driver
        - get_dialer

::: vertebra.db.registry.provide_driver
    options:
      heading_level: 4

::: vertebra.db.driver.BaseDatabaseConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.SyncDatabaseDriver
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.AsyncDatabaseDriver
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.DatabaseError
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.driver.capabilities.SyncPipelineCapable
    options:
      heading_level: 3
      members:
        - pipeline

::: vertebra.db.driver.capabilities.AsyncPipelineCapable
    options:
      heading_level: 3
      members:
        - pipeline

::: vertebra.db.arrow.ArrowCursor
    options:
      heading_level: 4
      members: false
      summary: false

## Relational, Cloud & Analytical Drivers (`vertebra.db.adapters`)

::: vertebra.db.adapters.postgres.PostgreSQLConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.adapters.mysql.MySQLConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.sqlite.SQLiteConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.adapters.oracle.OracleConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.adapters.mssql.MSSQLConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.duckdb.DuckDBConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.adapters.bigquery.BigQueryConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.adapters.spanner.SpannerConfig
    options:
      heading_level: 4
      members: false

## RESP3 Key-Value Stores & DataFusion (`redis`, `valkey`, `datafusion`)

::: vertebra.db.adapters.redis.RedisConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.valkey.ValkeyConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.datafusion.DataFusionConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.adapters.datafusion.DataFusionEngine
    options:
      heading_level: 3
      members:
        - register_table
        - register_parquet_from_store
        - execute_arrow
        - execute_sql

## Cloud Dialers, CDC & Row-Level Security

::: vertebra.db.dialer.alloydb.AlloyDBConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.dialer.cloudsql.CloudSQLConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.cdc.ChangeFeed
    options:
      heading_level: 3
      members:
        - publish_event
        - replay_since
        - flush_to_parquet

::: vertebra.db.rls.TenantPredicate
    options:
      heading_level: 4
      members: false
      summary: false


`TenantScopeConfig.setting_name` is adapter-neutral. `TenantPredicate` renders ANSI
placeholders and optional `CAST`, while PostgreSQL and Oracle cores own session
SQL. SQLSpec supports the declared PostgreSQL/Oracle setters and refuses other
engines. BigQuery/Spanner constructors reject non-None `tenant_predicate` before
initialization. `OracleCore.build_session_context_sql()` returns the
`DBMS_SESSION.SET_CONTEXT` block and named binds; Oracle drivers raise
`MissingDependencyError`, so no Vertebra session executes it.

See [Database Capabilities](database-capabilities.md) for notification, change-stream,
tenant-scope, and tool-provider APIs.
