# `vertebra.db` — Database Drivers, Analytics Engines & Cloud Dialers

Native database configurations, synchronous and asynchronous drivers, single-RTT CTE query pipelines (`SyncQueryPipeline`, `AsyncQueryPipeline`, `RelOverlay`), Apache Arrow shared-memory cursors (`ArrowCursor`), DataFusion federated analytics (`DataFusionEngine`), Change Data Capture (`ChangeFeed`), planner-unfenced Row-Level Security (`TenantPredicate`), and zero-sidecar Cloud SQL / AlloyDB dialers. See the [Databases & Analytics Usage Guide](../usage/databases.md) for architectural patterns and examples.

| Category | Classes | Highlights |
| --- | --- | --- |
| **SQLSpec Adapters (Litestar)** | `PostgreSQLAsyncConfig`, `MySQLAsyncConfig`, `SQLiteAsyncConfig`, `OracleAsyncConfig`, `MSSQLAsyncConfig`, `DuckDBAsyncConfig`, `BigQueryAsyncConfig`, `SpannerAsyncConfig` | Canonical Litestar adapter configurations (`vertebra.extensions.sqlspec`) backed by native Rust drivers and connection leasing |
| **Registry & CTE Pipelines** | `DatabaseRegistry`, `SyncQueryPipeline`, `AsyncQueryPipeline`, `CTEStep`, `RelOverlay`, `ArrowCursor` | Named driver management, single-RTT read-your-own-writes CTE compilation (`postgresql` and `googlesql`), and zero-copy Arrow IPC cursors |
| **Relational & OLAP Configs** | `PostgreSQLConfig`, `MySQLConfig`, `SQLiteConfig`, `OracleConfig`, `MSSQLConfig`, `DuckDBConfig`, `BigQueryConfig`, `SpannerConfig` | Pooler-aware statement caching, WAL/mmap pragmas, Oracle 26ai `VECTOR`/`OSON` & Cloud Wallets, TDS 7.4 `@P1` rewriting, DuckDB Arrow streaming, BigQuery Storage Read/Write v1 + Object Tables, and Cloud Spanner Multiplexed Sessions + `BatchWrite` |
| **RESP3 & Federated SQL** | `RedisConfig`, `ValkeyConfig`, `DataFusionEngine` | RESP3 `HELLO 3` client-side cache invalidation, Cluster/Sentinel routing, and cross-engine SQL federation |
| **Cloud Dialers, CDC & RLS** | `AlloyDBConfig`, `SyncAlloyDBDialer`, `AsyncAlloyDBDialer`, `CloudSQLConfig`, `SyncCloudSQLDialer`, `AsyncCloudSQLDialer`, `ChangeFeed`, `SyncSpannerChangeStreamSource`, `AsyncSpannerChangeStreamSource`, `TenantScopeConfig`, `TenantPredicate` | Direct TCP/TLS IAM OAuth2 token password resolution (`:5432`, `:3306`), zero-sidecar `:5433` AlloyDB and `:3307` Cloud SQL mTLS connectors, logical replication / Spanner change-stream streaming, and index-friendly tenant isolation |

## Database Registry & Query Pipeline

`DatabaseRegistry` manages named driver configurations across an application, while `SyncQueryPipeline` / `AsyncQueryPipeline` and `RelOverlay` compile multi-step reads and mutations into a single PostgreSQL round-trip.

::: vertebra.db._registry.DatabaseRegistry
    options:
      heading_level: 3
      members:
        - register
        - get_sync_driver
        - get_async_driver
        - close_all

::: vertebra.db.pipeline.SyncQueryPipeline
    options:
      heading_level: 3
      members:
        - add_step
        - add_overlay
        - compile
        - execute

::: vertebra.db.pipeline.AsyncQueryPipeline
    options:
      heading_level: 3
      members:
        - add_step
        - add_overlay
        - compile
        - execute

::: vertebra._vertebra.ArrowCursor
    options:
      heading_level: 4
      members: false

## Relational, Cloud & Analytical Drivers (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`)

::: vertebra.db.postgres.PostgreSQLConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.mysql.MySQLConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.sqlite.SQLiteConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.oracle.OracleConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.mssql.MSSQLConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.duckdb.DuckDBConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.bigquery.BigQueryConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.spanner.SpannerConfig
    options:
      heading_level: 4
      members: false

## RESP3 Key-Value Stores & DataFusion (`redis`, `valkey`, `datafusion`)

::: vertebra.db.redis.RedisConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.valkey.ValkeyConfig
    options:
      heading_level: 4
      members: false
      summary: false

::: vertebra.db.datafusion.DataFusionEngine
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

::: vertebra.db.dialer.cloudsql.CloudSQLConfig
    options:
      heading_level: 4
      members: false

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
