# `vertebra.db` — Database Drivers, Analytics Engines & Cloud Dialers

Native database configurations, synchronous and asynchronous drivers, single-RTT CTE query pipelines (`QueryPipeline`, `RelOverlay`), Apache Arrow shared-memory cursors (`ArrowCursor`), DataFusion federated analytics (`DataFusionEngine`), Change Data Capture (`ChangeFeed`), planner-unfenced Row-Level Security (`TenantPredicate`), and zero-sidecar Cloud SQL / AlloyDB dialers. See the [Databases & Analytics Usage Guide](../usage/databases.md) for architectural patterns and examples.

| Category | Classes | Highlights |
| --- | --- | --- |
| **Registry & CTE Pipelines** | `DatabaseRegistry`, `QueryPipeline`, `CteStep`, `RelOverlay`, `ArrowCursor` | Named driver management, single-RTT read-your-own-writes CTE compilation, and zero-copy Arrow IPC cursors |
| **Relational & OLAP Configs** | `PostgresConfig`, `MysqlConfig`, `SqliteConfig`, `OracleConfig`, `MssqlConfig`, `DuckdbConfig` | Pooler-aware statement caching, WAL/mmap pragmas, Oracle 26ai `VECTOR`/`OSON` & Cloud Wallets, TDS 7.4 `@P1` rewriting, and DuckDB Arrow streaming |
| **RESP3 & Federated SQL** | `RedisConfig`, `ValkeyConfig`, `DataFusionEngine` | RESP3 `HELLO 3` client-side cache invalidation, Cluster/Sentinel routing, and cross-engine SQL federation |
| **Cloud Dialers, CDC & RLS** | `AlloyDbConfig`, `AlloyDbDialer`, `CloudSqlConfig`, `CloudSqlDialer`, `ChangeFeed`, `TenantScopeConfig`, `TenantPredicate` | Zero-sidecar `:5433` AlloyDB and `:3307` Cloud SQL mTLS connectors, logical replication streaming, and index-friendly tenant isolation |

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

::: vertebra.db.pipeline.CteStep
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

## Relational & Analytical Drivers (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`)

::: vertebra.db.postgres.PostgresConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.mysql.MysqlConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.sqlite.SqliteConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.oracle.OracleConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.mssql.MssqlConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.duckdb.DuckdbConfig
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

::: vertebra.db.dialer.alloydb.AlloyDbConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.dialer.alloydb.AlloyDbDialer
    options:
      heading_level: 4
      members: false

::: vertebra.db.dialer.cloudsql.CloudSqlConfig
    options:
      heading_level: 4
      members: false

::: vertebra.db.dialer.cloudsql.CloudSqlDialer
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
