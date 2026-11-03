---
type: Guide
title: "SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions"
description: "The `vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`, and `vertebra.extensions.sqlalchemy` packages: Tier-1 native SHM adapters (`SyncDatabaseConfig`, `AsyncDatabaseConfig`), Tier-2 universal SHM broker proxy (`ProxySyncConfig`, `ProxyAsyncConfig`), real `+vtb` dialect implementations, column types, and CDC event backends."
status: stable
tags: [domains, extensions, sqlspec, sqlalchemy, alembic, shm, arrow]
sources:
  - resource: src/py/vertebra/extensions/sqlspec/__init__.py
  - resource: src/py/vertebra/extensions/sqlspec/plugin.py
  - resource: src/py/vertebra/extensions/sqlspec/adapters.py
  - resource: src/py/vertebra/extensions/sqlspec/proxy.py
  - resource: src/py/vertebra/extensions/sqlspec/provider.py
  - resource: src/py/vertebra/extensions/sqlspec/storage.py
  - resource: src/py/vertebra/extensions/sqlspec/events.py
  - resource: src/py/vertebra/extensions/alchemy/__init__.py
  - resource: src/py/vertebra/extensions/alchemy/plugin.py
  - resource: src/py/vertebra/extensions/alchemy/broker.py
  - resource: src/py/vertebra/extensions/alchemy/storage.py
  - resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  - resource: src/py/vertebra/extensions/sqlalchemy/types.py
  - resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
  - resource: pyproject.toml
---

# SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions

These packages provide Vertebra's two-tier shared-memory integration for SQLSpec, Advanced Alchemy, SQLAlchemy 2.1, and Alembic without hard-requiring those libraries at import time. Each module imports `litestar.di`, `litestar.typing`, `sqlalchemy.dialects`, or `alembic.ddl.impl` lazily through [`vertebra.utils.module_loader`](../../../../../src/py/vertebra/utils/module_loader.py) and still works when they are missing. Unit coverage: `test_drop_in_names_and_generics.py`, `test_data_storage_integrations.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, and `test_shm_broker_driver_proxy.py` under `src/py/tests/unit/extensions/`.

## 1. Plugins, Tier-1 Native Configs & Tier-2 Universal Broker Proxy

Shared plugin behavior:

- `on_app_init(app_config)` installs a state object into `app_config.state` (a dict, or anything with `update`).
- It registers session and connection/engine providers in `app_config.dependencies`, wrapped in `litestar.di.Provide(..., sync_to_thread=False)` when Litestar is importable. Existing keys are never overwritten.
- `on_cli_init` is a no-op.

| Symbol | Source | Responsibility |
|---|---|---|
| `create_sqlspec_plugin(config, *, broker=None, store=None)` | `sqlspec/plugin.py` | Thin factory returning real `sqlspec.extensions.litestar.SQLSpecPlugin`. Injects Vertebra `LeaseBroker` and `ObjectStoreBackend` into the plugin configuration without shadow wrapper classes. |
| `create_alchemy_plugin(config, *, broker=None, store=None)` | `alchemy/plugin.py` | Thin factory returning real `advanced_alchemy.extensions.litestar.SQLAlchemyPlugin`. Injects Vertebra `LeaseBroker` and `ObjectStoreBackend` into the plugin configuration without shadow wrapper classes. |
| `create_async_alchemy_config` / `create_sync_alchemy_config` / `normalize_alchemy_url` | `alchemy/plugin.py` | Config helpers returning real `advanced_alchemy.config.SQLAlchemyAsyncConfig` / `SQLAlchemySyncConfig`, normalizing `bigquery://`, `spanner://`, and `spangres://` URLs to their `+vtb` SQLAlchemy dialects with `NullPool` when a `LeaseBroker` is attached. |
| [`SyncDatabaseConfig` / `AsyncDatabaseConfig`](../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) | `sqlspec/adapters.py` | **Tier-1 native SHM adapters**: concrete subclasses for PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, Google BigQuery, and Google Cloud Spanner (`PostgreSQLSyncConfig`, `BigQuerySyncConfig`, `BigQueryAsyncConfig`, `SpannerSyncConfig`, `SpannerAsyncConfig`, etc.) backed directly by `vertebra.db` drivers, `LeaseBroker`, `StatementCache`, `ArrowCursor`, and `ObjectStoreBackend` (`load_from_object_store*`, `export_to_object_store*`, `create_object_table*`, `list_object_table_refs*`, `read_arrow_from_object_store*`, `write_arrow_to_object_store*`). `BaseSyncDriver.apply_tenant_predicate` and `BaseAsyncDriver.apply_tenant_predicate` propagate driver exceptions unchanged when `SET LOCAL` fails so unscoped sessions never execute. Resolves native drivers via `DatabaseRegistry.default().get_shared_sync_driver` / `get_shared_driver` (`share_pool=True` default) so stacked Litestar, Django, and SQLAlchemy apps targeting the same database endpoint share one underlying connection pool, `CloudSQLDialer` / `AlloyDBDialer`, and process-wide `LeaseBroker`. |
| [`ProxySyncConfig` / `ProxyAsyncConfig`](../../../../../src/py/vertebra/extensions/sqlspec/proxy.py) | `sqlspec/proxy.py` | **Tier-2 universal SHM broker proxy**: wraps *any* existing SQLSpec or Advanced Alchemy driver (`asyncpg`, `psycopg`, `oracledb`, `duckdb`, `adbc`, etc.) with `LeaseBroker` slot acquisition, `StatementCache` plan lookup, and zero-copy `ShmRegion` Arrow IPC transfer. |
| [`ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py) | `sqlspec/events.py` | SQLSpec event backends bridging database transactions to Vertebra CDC `ChangeFeed`. |

Pool defaults: when a broker is attached to a `+vtb` dialect (`poolclass="NullPool"`), SQLAlchemy's per-process `QueuePool` is bypassed so the cross-process `LeaseBroker` governs connection concurrency.

## 2. Connection Broker (`alchemy/broker.py` & `broker/transport.py`)

| Symbol | Responsibility |
|---|---|
| [`BrokerConfig`](../../../../../src/py/vertebra/extensions/alchemy/broker.py) / `BrokerMetrics` / `ChannelMessage` | Config (`pool_size`, `max_overflow`, `acquire_timeout`, `lease_ttl`, state keys), counters, and an array-like pub/sub envelope. |
| `BrokerTransport` | Runtime-checkable protocol: `acquire`, `acquire_async`, `release`, `publish`, `drain_channel`, `metrics`, `close`. |
| `ConnectionLease` | TTL-bound slot lease usable as a sync or async context manager. `release` is idempotent. |
| [`ShmBrokerTransport`](../../../../../src/py/vertebra/extensions/broker/transport.py) / `InMemoryBrokerTransport` | `ShmBrokerTransport` binds to `LeaseBroker` and its co-located `StatementCache` + `ShmChannelSlot` ring in POSIX shared memory; `InMemoryBrokerTransport` provides an in-process `threading.Condition` fallback. |
| `BrokerPlugin` | Installs the broker transport and metrics snapshot under `db_broker` / `db_broker_metrics`, registers DI providers, and closes the transport in `lifespan` / `server_lifespan`. |

## 3. Object-Storage & Caching Adapters

- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py) (`alchemy/storage.py`) implements `advanced_alchemy.types.file_object.StorageBackend` over `ObjectStore` with native sync and async methods (`save_object[_async]`, `get_content[_async]`, `get_range[_async]`, `get_metadata[_async]`, `exists[_async]`, `copy_object[_async]`, `list_objects[_async]`, `delete_object[_async]`, and `sign[_async]`) plus `FileObjectSessionTracker` transaction rollback safety.
- [`SQLAlchemyCacheBackend`](../../../../../src/py/vertebra/extensions/alchemy/cache.py), [`AsyncCacheManager`](../../../../../src/py/vertebra/extensions/alchemy/cache.py), and [`ZeroCopyServiceMixin`](../../../../../src/py/vertebra/extensions/alchemy/cache.py) (`alchemy/cache.py`) provide two-tier L1 (`ShmRegion` / LRU) + L2 (`SyncRedisDriver` / `AsyncRedisDriver` with Valkey RESP3 `CLIENT TRACKING` invalidation, prefix-scoped `SCAN` + `UNLINK` on `clear()` / `clear_async()` under `key_prefix="vtb:aa:"`, and millisecond `px` TTLs) caching, event-loop-native `CacheManager` async methods (`get_async`, `set_async`, `delete_async`, `get_schema_async` without `asyncio.to_thread`), and C-API vectorcall `to_schema` projection (`orm_to_struct_vectorcall`).
- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py) (`sqlspec/storage.py`) implements `sqlspec.protocols.ObjectStoreProtocol` over `ObjectStore` (`read_bytes[_async]`, `write_bytes[_async]`, `read_arrow[_async]`, `write_arrow[_async]`, `stream_arrow[_async]`, `sign[_async]`, `stream_response`, and `ArrowFlightSerializer`) and registers directly with `SyncStoragePipeline` and `AsyncStoragePipeline`.

## 4. `+vtb` Dialects, Types, Alembic (`sqlalchemy/dialects.py`, `sqlalchemy/alembic.py`, `sqlalchemy/types.py`)

| Symbol | Responsibility |
|---|---|
| `DIALECT_SCHEMES` | `{postgresql,oracle,mysql,sqlite,mssql,duckdb,bigquery,spanner,spangres}+{vertebra,vtb}`. |
| Real Dialects (`sqlalchemy/dialects.py`) | Subclasses of SQLAlchemy 2.1 base dialects (`PostgreSQLDialect_vtb`, `MySQLDialect_vtb`, `SQLiteDialect_vtb`, `OracleDialect_vtb`, `MSSQLDialect_vtb`, `DuckDBDialect_vtb`, `BigQueryDialect`, `BigQueryAsyncDialect`, `SpannerDialect`, `SpannerAsyncDialect`, `SpangresDialect`, `SpangresAsyncDialect`) with `NullPool` + `LeaseBroker` connection leasing, Cloud SQL / AlloyDB dialer integration, and `DBAPIModule` binding. |
| `DBAPIModule` / `DBAPIConnection` / `DBAPICursor` | PEP 249 surface over `vertebra.db` drivers (`postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`) resolved through `DatabaseRegistry.default().get_shared_sync_driver` / `get_shared_driver` (`share_pool=True` default), plus `execute_arrow_shm()` returning `ArrowCursor`. Non-supported URLs raise [`UnsupportedBackendError`](../../../../../src/py/vertebra/typing.py). |
| `SyncEngineAdapter` | `run_sync(fn, ...)` submits to `SyncPool.submit_async`. `requires_greenlet` is `not is_free_threaded()`. |
| [`Vector`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py) / `VectorDistanceExpr` | `get_col_spec` renders Oracle `VECTOR(dim\|*, FLOAT32\|...)`, PG/MySQL/`spangres` `VECTOR(dim)`, BigQuery `ARRAY<FLOAT64>`, Spanner GoogleSQL `ARRAY<FLOAT32>(vector_length=>dim)` (or `ARRAY<FLOAT64>`), otherwise `BLOB`. `cosine_distance` / `l2_distance` / `inner_product` compile to Oracle `VECTOR_DISTANCE`, pgvector `<=>`/`<->`/`<#>`, BigQuery/Spanner `COSINE_DISTANCE`/`EUCLIDEAN_DISTANCE`/`DOT_PRODUCT`, MySQL `DISTANCE_*`, or `vec_distance_*`. |
| [`BinaryJSON`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py) | `JSONB` on PG/SQLite/`spangres`, `JSON` on BigQuery/Spanner/MySQL/Oracle/MSSQL/DuckDB. Encodes with `encode_json`. |
| [`AlembicImpl`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic.py) / `BigQueryAlembicImpl` / `SpannerAlembicImpl` | `register_all` registers each `+vtb` key in `DefaultImpl._impls`. `BigQueryAlembicImpl` sets `transactional_ddl = False`, while `SpannerAlembicImpl` buffers DDL statements emitted during a migration (`buffer_ddl` / `flush_ddl_batch`) and submits them in a single `UpdateDatabaseDdl` LRO batch instead of one 10–60s schema change per statement. Static `render_item` and `compare_type` hooks handle `Vector`/`BinaryJSON` and treat `JSON`/`JSONB`/`OSON` as equal. `resolve_migration_url` returns `PostgresConfig.effective_direct_dsn`. |
| `CreateViewOp` / `DropViewOp` / `CreateVectorIndexOp` / `DropVectorIndexOp` | DDL strings. `OR REPLACE` is dropped on SQLite and `IF EXISTS` is dropped on Oracle. `CreateVectorIndexOp` compiles `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR GRAPH` on Oracle, `USING hnsw` on PostgreSQL/`spangres`, `USING IVF` on BigQuery, and `USING ScaNN` on Spanner GoogleSQL. `reverse()` gives the inverse op. |
| [`SelectInLoadSpec`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py), `InsertSentinelSpec`, `DeleteUsingClause`, `ParameterizedSql` | Chunk size 1–1000 (Oracle `ORA-01795` limit), `num_batches`; sentinel-column settings; `DELETE ... USING ...` rendering with identifier validation checks on table names; conversion of PEP 750 template strings (`strings`/`interpolations`) or a static-parts sequence into `:p_N` named binds. |
