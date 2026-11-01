# `vertebra.db` — Database Drivers, Analytics Engines & Cloud Dialers

Native database configurations, synchronous and asynchronous drivers, single-RTT CTE query pipelines (`QueryPipeline`, `RelOverlay`), Apache Arrow shared-memory cursors (`ArrowCursor`), DataFusion federated analytics (`DataFusionEngine`), Change Data Capture (`ChangeFeed`), planner-unfenced Row-Level Security (`TenantPredicate`), and zero-sidecar Cloud SQL / AlloyDB dialers. See the [Databases & Analytics Usage Guide](../usage/databases.md) for architectural patterns and examples.

| Category | Classes | Highlights |
| --- | --- | --- |
| **SQLSpec Adapters (Litestar)** | `PostgreSQLAsyncConfig`, `MySQLAsyncConfig`, `SQLiteAsyncConfig`, `OracleAsyncConfig`, `MSSQLAsyncConfig`, `DuckDBAsyncConfig` | Canonical Litestar adapter configurations (`vertebra.extensions.sqlspec.adapters`) backed by native Rust drivers and connection leasing |
| **Registry & CTE Pipelines** | `DatabaseRegistry`, `QueryPipeline`, `CTEStep`, `RelOverlay`, `ArrowCursor` | Named driver management, single-RTT read-your-own-writes CTE compilation, and zero-copy Arrow IPC cursors |
| **Relational & OLAP Configs** | `PostgreSQLConfig`, `MySQLConfig`, `SQLiteConfig`, `OracleConfig`, `MSSQLConfig`, `DuckDBConfig` | Pooler-aware statement caching, WAL/mmap pragmas, Oracle 26ai `VECTOR`/`OSON` & Cloud Wallets, TDS 7.4 `@P1` rewriting, and DuckDB Arrow streaming |
| **RESP3 & Federated SQL** | `RedisConfig`, `ValkeyConfig`, `DataFusionEngine` | RESP3 `HELLO 3` client-side cache invalidation, Cluster/Sentinel routing, and cross-engine SQL federation |
| **Cloud Dialers, CDC & RLS** | `AlloyDBConfig`, `AlloyDBDialer`, `CloudSQLConfig`, `CloudSQLDialer`, `ChangeFeed`, `TenantScopeConfig`, `TenantPredicate` | Zero-sidecar `:5433` AlloyDB and `:3307` Cloud SQL mTLS connectors, logical replication streaming, and index-friendly tenant isolation |

## Database Registry & Query Pipeline

`DatabaseRegistry` manages named driver configurations across an application, while `QueryPipeline` and `RelOverlay` compile multi-step reads and mutations into a single PostgreSQL round-trip.

::: vertebra.db._registry.DatabaseRegistry
    options:
      heading_level: 3
      members:
        - register
        - get
        - close_all

::: vertebra.db.pipeline.QueryPipeline
    options:
      heading_level: 3
      members:
        - add_cte
        - add_overlay
        - compile
        - execute
        - execute_async

::: vertebra.db.pipeline.CTEStep
    options:
      heading_level: 4
      members: false

::: vertebra.db.pipeline.RelOverlay
    options:
      heading_level: 4
      members: false

::: vertebra._vertebra.ArrowCursor
    options:
      heading_level: 4
      members: false

## SQLSpec Adapters (`vertebra.extensions.sqlspec.adapters`)

First-class SQLSpec adapter configurations connecting Litestar applications to Vertebra's native Rust drivers.

::: vertebra.extensions.sqlspec.adapters.PostgreSQLAsyncConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.adapters.MySQLAsyncConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.adapters.SQLiteAsyncConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.adapters.OracleAsyncConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.adapters.MSSQLAsyncConfig
    options:
      heading_level: 4
      members: false

::: vertebra.extensions.sqlspec.adapters.DuckDBAsyncConfig
    options:
      heading_level: 4
      members: false

## Relational & Analytical Drivers (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`)

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

## RESP3 Key-Value Stores & DataFusion (`redis`, `valkey`, `datafusion`)

::: vertebra.db.redis.RedisConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.valkey.ValkeyConfig
    options:
      heading_level: 4
      members: false

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

::: vertebra.db.dialer.alloydb.AlloyDBDialer
    options:
      heading_level: 4
      members: false

::: vertebra.db.dialer.cloudsql.CloudSQLConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.dialer.cloudsql.CloudSQLDialer
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

::: vertebra.db.rls.TenantScopeConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.rls.TenantPredicate
    options:
      heading_level: 4
      members: false
