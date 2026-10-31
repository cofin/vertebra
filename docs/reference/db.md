# `vertebra.db` — Database Drivers, Analytics Engines & Cloud Dialers

Native database configurations, synchronous and asynchronous drivers, DataFusion analytics, change data capture (`ChangeFeed`), and Cloud SQL / AlloyDB dialers.

## Database Registry & Query Pipeline

::: vertebra.db._registry.DatabaseRegistry
    options:
      heading_level: 3

::: vertebra.db.pipeline.QueryPipeline
    options:
      heading_level: 3

## Relational & Analytical Drivers (`postgres`, `mysql`, `sqlite`, `oracle`, `mssql`, `duckdb`)

::: vertebra.db.postgres.PostgresConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.mysql.MysqlConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.sqlite.SqliteConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.oracle.OracleConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.mssql.MssqlConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.duckdb.DuckdbConfig
    options:
      heading_level: 3
      members: false

## RESP3 Key-Value Stores & DataFusion (`redis`, `valkey`, `datafusion`)

::: vertebra.db.redis.RedisConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.valkey.ValkeyConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.datafusion.DataFusionEngine
    options:
      heading_level: 3

## Cloud Dialers, CDC & Row-Level Security

::: vertebra.db.dialer.alloydb.AlloyDbConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.dialer.cloudsql.CloudSqlConfig
    options:
      heading_level: 3
      members: false

::: vertebra.db.cdc.ChangeFeed
    options:
      heading_level: 3

::: vertebra.db.rls.TenantScopeConfig
    options:
      heading_level: 3
      members: false
