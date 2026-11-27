# PyO3 Native Extension Stubs

Low-level Rust classes and functions declared in PEP 561 `.pyi` stub files across Vertebra's compiled extension modules. Each extension is built with `gil_used = false` for free-threaded Python. Most applications use these through the higher-level [`vertebra`](vertebra.md), [`vertebra.server`](server.md), [`vertebra.db`](db.md), and [`vertebra.storage`](storage.md) APIs.

| Native Module | Backing Crate | Scope |
| --- | --- | --- |
| **`vertebra._vertebra`** | `vertebra-py`, `vertebra-core`, `vertebra-http`, `vertebra-db` | Request and response views, protocol scopes, shared-memory cache, and core IPC bridge. |
| **`vertebra._bundler`** | `vertebra-bundler` | Oxc and Rolldown asset bundling, QuickJS SSR pool, and `.d.ts` declaration generation. |
| **`vertebra._storage`** | `vertebra-storage` | Sync and async object storage backed by Rust `object_store` and Tokio. |
| **`vertebra._bigquery`** | `vertebra-bigquery` | BigQuery REST v2 queries and Storage Read/Write gRPC streams. |
| **`vertebra._duckdb`** | `vertebra-duckdb` | Bundled DuckDB 2.0 client with shared-memory Arrow output. |
| **`vertebra._mssql`** | `vertebra-mssql` | SQL Server TDS 7.4 client with bulk load and Arrow output. |
| **`vertebra._mysql`** | `vertebra-mysql` | MySQL binary protocol client with Arrow output. |
| **`vertebra._oracle`** | `vertebra-oracle` | Oracle 26ai Thin client, connection pools, and notification listeners. |
| **`vertebra._postgres`** | `vertebra-postgres` | PostgreSQL Protocol 3.0 client, pools, `COPY`, `pgoutput` replication, and Arrow output. |
| **`vertebra._spanner`** | `vertebra-spanner` | Cloud Spanner data and admin client. |
| **`vertebra._sqlite`** | `vertebra-sqlite` | Bundled SQLite 3.45+ client with WAL pooling and Arrow output. |
| **`vertebra._redis`** | `vertebra-redis` | RESP3 Redis client with client-side caching, Sentinel, and Cluster support. |
| **`vertebra._valkey`** | `vertebra-valkey` | RESP3 Valkey client with hash-field expiration, client-side caching, Sentinel, and Cluster support. |

## Core Runtime & Protocol Views

::: vertebra._vertebra.Request
    options:
      heading_level: 3

::: vertebra._vertebra.Headers
    options:
      heading_level: 3

::: vertebra._vertebra.Response
    options:
      heading_level: 3

::: vertebra._vertebra.RequestBatch
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._vertebra.AsgiScope
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._vertebra.RsgiScope
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._vertebra.WsgiEnviron
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._vertebra.ShmCache
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._vertebra.ShmCacheStats
    options:
      heading_level: 4
      members: false
      show_signature: false

## Bundler & QuickJS SSR

::: vertebra._bundler.BundleConfig
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._bundler.BuildResult
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._bundler.Bundler
    options:
      heading_level: 3

::: vertebra._bundler.SSRPool
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._bundler.IsolatedDeclarationsGenerator
    options:
      heading_level: 4
      members: false
      show_signature: false

## Object Storage

::: vertebra._storage.StorageBuffer
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._storage.ObjectMeta
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._storage.SyncObjectStore
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._storage.AsyncObjectStore
    options:
      heading_level: 3
      separate_signature: false
      members:
        - put
        - get
        - head
        - delete
        - list
        - copy

## BigQuery & Spanner

::: vertebra._bigquery.SyncBigQueryClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._bigquery.AsyncBigQueryClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._spanner.SyncSpannerClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._spanner.AsyncSpannerClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

## DuckDB

::: vertebra._duckdb.SyncDuckDBClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._duckdb.AsyncDuckDBClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

## SQL Database Drivers

Each SQL database extension exports `GIL_USED = False` and `FREE_THREADED` alongside its sync and async clients, connection pools, listeners, and replication types.

::: vertebra._postgres.AsyncPostgresClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._postgres.AsyncPostgresReplication
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._oracle.SyncOracleClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._mysql.SyncMySQLClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._mssql.SyncMSSQLClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._sqlite.SyncSQLiteClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._sqlite.AsyncSQLiteClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

## Redis & Valkey Drivers

`vertebra._redis` and `vertebra._valkey` expose sync and async clients, pub/sub listeners, and pipelines with RESP3 framing, client-side caching invalidations, Sentinel, and Cluster routing. `vertebra._valkey` adds Valkey 9.0 hash-field expiration and `CLIENT CAPA redirect`.

::: vertebra._redis.AsyncRedisClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._redis.SyncRedisClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._valkey.AsyncValkeyClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false

::: vertebra._valkey.SyncValkeyClient
    options:
      heading_level: 4
      members: false
      summary: false
      show_signature: false
