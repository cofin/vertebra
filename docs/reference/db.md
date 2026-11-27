# Database Drivers, Analytics & Cloud Dialers

`vertebra.db` provides database configs, sync and async drivers, capability protocols, DB-API 2.0 adapters, Apache Arrow cursors, DataFusion SQL federation, change data capture, tenant scoping, and direct Cloud SQL and AlloyDB dialers. See the [Databases & Native Drivers Usage Guide](../usage/databases.md) for patterns and examples.

| Category | Coverage |
| --- | --- |
| **SQLSpec Adapters** | Litestar adapter configs under `vertebra.extensions.sqlspec` backed by Vertebra drivers and connection leasing. |
| **PEP 249 DB-API** | Sync and async DB-API 2.0 interfaces under `vertebra.db.dbapi` with no SQLAlchemy dependency. |
| **Registry & Driver Contracts** | URL-based driver resolution, shared pool keys, uniform sync and async contracts, query pipelining, and zero-copy Arrow cursors. |
| **Capability Protocols** | Runtime-checkable protocols for pipelining, pub/sub notifications, CDC streams, `SKIP LOCKED` queues, and in-database embedding pushdown. |
| **Relational & Analytical Drivers** | Configs for PostgreSQL, MySQL, SQLite, Oracle 26ai, SQL Server, DuckDB, BigQuery, and Spanner. |
| **Key-Value & Federated SQL** | RESP3 Redis and Valkey clients with client-side caching, Sentinel, and Cluster routing, plus cross-engine DataFusion analytics. |
| **Cloud Dialers, CDC & RLS** | Direct IAM token auth, `:5433` AlloyDB and `:3307` Cloud SQL mTLS connectors, logical replication and change streams, and tenant isolation. |

## Registry, Driver Contracts & Pipelines

`DatabaseRegistry` and `provide_driver` resolve configs and shared drivers from URL schemes. `SyncDatabaseDriver` and `AsyncDatabaseDriver` define the execution contract, while `SyncPipelineCapable` and `AsyncPipelineCapable` add batched `driver.pipeline()` execution.

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

## PEP 249 DB-API

::: vertebra.db.dbapi.SyncDBAPIConnection
    options:
      heading_level: 3
      members: false
      summary: false

::: vertebra.db.dbapi.AsyncDBAPIConnection
    options:
      heading_level: 3
      members: false
      summary: false

::: vertebra.db.dbapi.SyncDBAPIModule
    options:
      heading_level: 3
      members: false
      summary: false

::: vertebra.db.dbapi.AsyncDBAPIModule
    options:
      heading_level: 3
      members: false
      summary: false

## Relational, Cloud & Analytical Drivers

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
      summary: false

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

## Key-Value Stores & DataFusion

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
        - register_arrow_table
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

`TenantScopeConfig.setting_name` is adapter-neutral. `TenantPredicate` renders ANSI placeholders with an optional `CAST`, while the PostgreSQL and Oracle adapters own session SQL. SQLSpec accepts the declared PostgreSQL and Oracle setters and rejects other engines. BigQuery and Spanner constructors reject a non-`None` `tenant_predicate` before initialization. Oracle VPD scopes support trusted context package procedures alongside `DBMS_SESSION.SET_IDENTIFIER` and reset or evict connections on pool release.

See [Database Capabilities](database-capabilities.md) for notification, change-stream, tenant-scope, and tool-provider APIs.
