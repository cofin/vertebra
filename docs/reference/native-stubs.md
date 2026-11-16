# PyO3 Native Extension Stubs (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_duckdb`, `_mssql`, `_mysql`, `_oracle`, `_postgres`, `_spanner`, `_sqlite`, `_redis`)

Low-level Rust/PyO3 classes and functions declared in PEP 561 `.pyi` stub files across all 12 native extensions (`_vertebra.pyi`, `_bundler.pyi`, `_storage.pyi`, `_bigquery.pyi`, `_duckdb.pyi`, `_mssql.pyi`, `_mysql.pyi`, `_oracle.pyi`, `_postgres.pyi`, `_spanner.pyi`, `_sqlite.pyi`, `_redis.pyi`) and compiled with `gil_used = false` for free-threaded Python `3.14t`/`3.15t`. Most applications interact with these primitives through the higher-level [`vertebra`](vertebra.md), [`vertebra.server`](server.md), [`vertebra.db`](db.md), and [`vertebra.storage`](storage.md) facades.

| Native Module | Key Stub Classes | Backing Rust Crate |
| --- | --- | --- |
| **`vertebra._vertebra`** | `Request`, `Headers`, `Response`, `RequestBatch`, `AsgiScope`, `RsgiScope`, `WsgiEnviron`, `ShmCache`, `ShmCacheStats` | `vertebra-py`, `vertebra-core`, `vertebra-http`, `vertebra-db` |
| **`vertebra._bundler`** | `BundleConfig`, `BuildResult`, `Bundler`, `SSRPool`, `IsolatedDeclarationsGenerator` | `vertebra-bundler` (Oxc, Rolldown, QuickJS) |
| **`vertebra._storage`** | `StorageBuffer`, `ObjectMeta`, `SyncObjectStore`, `AsyncObjectStore` | `vertebra-storage` (Rust `object_store` + Tokio) |
| **`vertebra._bigquery`** | `SyncBigQueryClient`, `AsyncBigQueryClient` | `vertebra-bigquery` (REST v2 + Storage Read/Write gRPC) |
| **`vertebra._duckdb`** | `SyncDuckDBClient`, `AsyncDuckDBClient` | `vertebra-duckdb` (Bundled DuckDB 2.0 + `VTBARROW` SHM) |
| **`vertebra._mssql`** | `SyncMSSQLClient`, `AsyncMSSQLClient` | `vertebra-mssql` (TDS 7.4/8.0 + `BulkLoad` + `VTBARROW` SHM) |
| **`vertebra._mysql`** | `SyncMySQLClient`, `AsyncMySQLClient`, `MySQLWireTestServer` | `vertebra-mysql` (MySQL Binary Protocol + `VTBARROW` SHM) |
| **`vertebra._oracle`** | `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, `AsyncOraclePool`, `SyncOracleListener`, `AsyncOracleListener` | `vertebra-oracle` (Oracle 26ai Thin TNS/TTC + Arrow SHM) |
| **`vertebra._postgres`** | `SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `SyncPostgresListener`, `AsyncPostgresListener`, `AsyncPostgresReplication` | `vertebra-postgres` (PostgreSQL Protocol 3.0 + `PGCOPY` + `pgoutput` + `VTBARROW` SHM) |
| **`vertebra._spanner`** | `SyncSpannerClient`, `AsyncSpannerClient` | `vertebra-spanner` (Cloud Spanner Data + DatabaseAdmin) |
| **`vertebra._sqlite`** | `SyncSQLiteClient`, `AsyncSQLiteClient` | `vertebra-sqlite` (Bundled SQLite 3.45+ + `VTBARROW` SHM) |
| **`vertebra._redis`** | `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisListener`, `AsyncRedisListener` | `vertebra-redis` (RESP3 + Client Tracking + Sentinel + Cluster) |

## `vertebra._vertebra` — HTTP/IPC Transport, Request Batches & Protocol Drivers

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

## `vertebra._bundler` — Embedded Rolldown/Oxc Bundler & QuickJS SSR Pool

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

## `vertebra._storage` — Rust `object_store` & Cloud Presigning Primitives

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

## `vertebra._bigquery` & `vertebra._spanner` — Google Cloud Data-Plane Extensions

::: vertebra._bigquery.SyncBigQueryClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._bigquery.AsyncBigQueryClient
    options:
      heading_level: 3
      members: false
      show_signature: false

::: vertebra._spanner.SyncSpannerClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._spanner.AsyncSpannerClient
    options:
      heading_level: 3
      members: false
      show_signature: false

## `vertebra._duckdb` — Embedded DuckDB 2.0 Analytical Client

::: vertebra._duckdb.SyncDuckDBClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._duckdb.AsyncDuckDBClient
    options:
      heading_level: 4
      members: false
      show_signature: false

## Native SQL Drivers (`_postgres`, `_oracle`, `_mysql`, `_mssql`, `_sqlite`)

Each companion database extension exposes `GIL_USED = False` and `FREE_THREADED` module constants alongside its synchronous/asynchronous native clients (`Sync*`, `Async*`) and extension-specific pooling or replication types listed in the summary table above (`SyncPostgresClient`, `AsyncPostgresClient`, `SyncPostgresPool`, `AsyncPostgresPool`, `AsyncPostgresReplication`, `SyncOracleClient`, `AsyncOracleClient`, `SyncOraclePool`, `AsyncOraclePool`, `SyncMySQLClient`, `AsyncMySQLClient`, `SyncMSSQLClient`, `AsyncMSSQLClient`, `SyncSQLiteClient`, `AsyncSQLiteClient`).

::: vertebra._postgres.AsyncPostgresClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._postgres.AsyncPostgresReplication
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._oracle.SyncOracleClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._mysql.SyncMySQLClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._mssql.SyncMSSQLClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._sqlite.SyncSQLiteClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._sqlite.AsyncSQLiteClient
    options:
      heading_level: 4
      members: false
      show_signature: false

## `vertebra._redis` — Native RESP3 Client & Cache Invalidation

The standalone `vertebra._redis` native extension provides `SyncRedisClient`, `AsyncRedisClient`, `SyncRedisListener`, `AsyncRedisListener`, `SyncRedisPipeline`, and `AsyncRedisPipeline` with RESP3 wire protocol support, client-side caching invalidations, Sentinel, and Cluster topology routing.

::: vertebra._redis.AsyncRedisClient
    options:
      heading_level: 4
      members: false
      show_signature: false

::: vertebra._redis.SyncRedisClient
    options:
      heading_level: 4
      members: false
      show_signature: false
