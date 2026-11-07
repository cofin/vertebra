---
type: Guide
title: "SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions"
description: "The `vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`, and `vertebra.extensions.sqlalchemy` packages providing native SHM adapters (`SyncAdapterConfig`, `AsyncAdapterConfig`), universal SHM broker proxies (`ProxySyncConfig`, `ProxyAsyncConfig`), `+vtb` dialect implementations, column types, and CDC event backends."
status: stable
tags:
  - domains
  - extensions
  - sqlspec
  - sqlalchemy
  - alembic
  - shm
  - arrow
generated:
  by: flow/0.26.0
  at: "2026-11-04T04:33:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T03:55:00Z"
    commit: cdb978d3
  - by: agent:codex
    at: "2026-11-05T06:09:26Z"
    commit: 80ff8a65e43a2bcef9e48a7763e89a7352118232
  - by: agent:flow-setup
    at: "2026-11-04T04:33:00Z"
    commit: f741701
sources:
- id: src-extensions-sqlspec-init-py
  resource: src/py/vertebra/extensions/sqlspec/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py)
- id: src-extensions-sqlspec-plugin-py
  resource: src/py/vertebra/extensions/sqlspec/plugin.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/plugin.py)
- id: src-extensions-sqlspec-adapters-py
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/adapters.py)
- id: src-extensions-sqlspec-proxy-py
  resource: src/py/vertebra/extensions/sqlspec/proxy.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/proxy.py)
- id: src-extensions-sqlspec-provider-py
  resource: src/py/vertebra/extensions/sqlspec/provider.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/provider.py)
- id: src-extensions-sqlspec-storage-py
  resource: src/py/vertebra/extensions/sqlspec/storage.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/storage.py)
- id: src-extensions-sqlspec-events-py
  resource: src/py/vertebra/extensions/sqlspec/events.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/events.py)
- id: src-extensions-alchemy-init-py
  resource: src/py/vertebra/extensions/alchemy/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py)
- id: src-extensions-alchemy-plugin-py
  resource: src/py/vertebra/extensions/alchemy/plugin.py
  title: Python implementation module (src/py/vertebra/extensions/alchemy/plugin.py)
- id: src-extensions-alchemy-broker-py
  resource: src/py/vertebra/extensions/alchemy/broker.py
  title: Python implementation module (src/py/vertebra/extensions/alchemy/broker.py)
- id: src-extensions-alchemy-storage-py
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Python implementation module (src/py/vertebra/extensions/alchemy/storage.py)
- id: src-extensions-sqlalchemy-dialects-py
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py)
- id: src-extensions-sqlalchemy-types-py
  resource: src/py/vertebra/extensions/sqlalchemy/types.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/types.py)
- id: src-extensions-sqlalchemy-alembic-py
  resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/alembic.py)
- id: src-pyproject-toml
  resource: pyproject.toml
  title: Python package metadata, dependencies, and tool configuration
---

# SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions

<!-- truth: start -->
- `create_sqlspec_plugin` and `create_alchemy_plugin` return real upstream `SQLSpecPlugin` and `SQLAlchemyPlugin` instances wired to Vertebra `LeaseBroker` (`NullPool` when brokered) and `ObjectStoreBackend`.
- Native `<Engine>{Sync,Async}Config` adapters, `ProxySyncConfig` / `ProxyAsyncConfig` wrappers, and 36 SQLAlchemy 2.1 `+vertebra`/`+vtb` sync/async dialect entry points share pools via `DatabaseRegistry.default()`.
<!-- truth: end -->

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
| [`SyncAdapterConfig` / `AsyncAdapterConfig`](../../../../../src/py/vertebra/extensions/sqlspec/adapters.py)[^src-extensions-sqlspec-adapters-py] | `sqlspec/adapters.py` | **Tier-1 native SHM adapters**: concrete subclasses for PostgreSQL, MySQL, SQLite, Oracle, MSSQL, DuckDB, Google BigQuery, and Google Cloud Spanner (`PostgreSQLSyncConfig`, `BigQuerySyncConfig`, `BigQueryAsyncConfig`, `SpannerSyncConfig`, `SpannerAsyncConfig`, etc.) backed directly by `vertebra.db` drivers, `LeaseBroker`, `StatementCache`, `ArrowCursor`, and `ObjectStoreBackend` (`load_from_object_store*`, `export_to_object_store*`, `create_object_table*`, `list_object_table_refs*`, `read_arrow_from_object_store*`, `write_arrow_to_object_store*`). `BaseSyncDriver.apply_tenant_predicate` and `BaseAsyncDriver.apply_tenant_predicate` raise native `NotSupportedError`. `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` prepare sync `%s` and async `$N` tenant session binds through the configured SQLSpec profile and unwrap typed values, while `OracleSyncDriver` and `OracleAsyncDriver` execute `OracleCore.build_session_context_sql` and return `predicate.to_sql_fragment(":tenant_id")`. Resolves native drivers via `DatabaseRegistry.default().get_shared_sync_driver` / `get_shared_async_driver` (`share_pool=True` default when `driver` is omitted; explicit `driver=` instances are preserved verbatim without pool sharing) so stacked Litestar, Django, and SQLAlchemy apps targeting the same database endpoint share one underlying connection pool, `SyncCloudSQLDialer` / `AsyncCloudSQLDialer` / `SyncAlloyDBDialer` / `AsyncAlloyDBDialer`, and process-wide `LeaseBroker`. |
| [`ProxySyncConfig` / `ProxyAsyncConfig`](../../../../../src/py/vertebra/extensions/sqlspec/proxy.py)[^src-extensions-sqlspec-proxy-py] | `sqlspec/proxy.py` | **Tier-2 universal SHM broker proxy**: wraps *any* existing SQLSpec or Advanced Alchemy driver (`asyncpg`, `psycopg`, `oracledb`, `duckdb`, `adbc`, etc.) with `LeaseBroker` slot acquisition, `StatementCache` plan lookup, and zero-copy `ShmRegion` Arrow IPC transfer. |
| [`ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py)[^src-extensions-sqlspec-events-py] | `sqlspec/events.py` | SQLSpec event backends bridging database transactions to Vertebra CDC `ChangeFeed`. |

BigQuery and Spanner configs and drivers reject non-None `tenant_predicate` before base initialization, supplied-core access, resolver/default-broker access, or native effects. These eight constructor guards prevent unsupported isolation sessions from existing. Ordinary None-predicate queries, scripts, Arrow, batch, and snapshots retain their previous preparation and results.

Pool defaults: when a broker is attached to a `+vtb` dialect (`poolclass="NullPool"`), SQLAlchemy's per-process `QueuePool` is bypassed so the cross-process `LeaseBroker` governs connection concurrency.

## 2. Connection Broker (`alchemy/broker.py` & `broker/transport.py`)

| Symbol | Responsibility |
|---|---|
| [`BrokerConfig`](../../../../../src/py/vertebra/extensions/alchemy/broker.py)[^src-extensions-alchemy-broker-py] / `BrokerMetrics` / `ChannelMessage` | Config (`pool_size`, `max_overflow`, `acquire_timeout`, `lease_ttl`, state keys), counters, and an array-like pub/sub envelope. |
| `SyncBrokerTransport` / `AsyncBrokerTransport` | Runtime-checkable protocol: `acquire` / `async def acquire`, `release`, `publish`, `drain_channel`, `metrics`, `close`. |
| `SyncConnectionLease` / `AsyncConnectionLease` | TTL-bound slot lease usable as a sync or async context manager. `release` is idempotent. |
| `SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport` & `SyncInMemoryBrokerTransport` / `AsyncInMemoryBrokerTransport` | Bind to `LeaseBroker` and its co-located `StatementCache` + `ShmChannelSlot` ring in POSIX shared memory, with in-process thread-safe fallback. |
| `BrokerPlugin` | Installs the broker transport and metrics snapshot under `db_broker` / `db_broker_metrics`, registers DI providers, and closes the transport in `lifespan` / `server_lifespan`. |

## 3. Object-Storage & Caching Adapters

- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py)[^src-extensions-alchemy-storage-py] (`alchemy/storage.py`) implements `advanced_alchemy.types.file_object.StorageBackend` over `SyncObjectStore` / `AsyncObjectStore` with native sync and async methods (`save_object[_async]`, `get_content[_async]`, `get_range[_async]`, `get_metadata[_async]`, `exists[_async]`, `copy_object[_async]`, `list_objects[_async]`, `delete_object[_async]`, and `sign[_async]`) plus `FileObjectSessionTracker` transaction rollback safety.
- [`SQLAlchemyCacheBackend`](../../../../../src/py/vertebra/extensions/alchemy/cache.py), [`AsyncCacheManager`](../../../../../src/py/vertebra/extensions/alchemy/cache.py), and [`ZeroCopyServiceMixin`](../../../../../src/py/vertebra/extensions/alchemy/cache.py) (`alchemy/cache.py`) provide two-tier L1 (`ShmRegion` / LRU) + L2 (`SyncRedisDriver` / `AsyncRedisDriver` with Valkey RESP3 `CLIENT TRACKING` invalidation, prefix-scoped `SCAN` + `UNLINK` on `clear()` / `clear_async()` under `key_prefix="vtb:aa:"`, and millisecond `px` TTLs) caching, event-loop-native `CacheManager` async methods (`get_async`, `set_async`, `delete_async`, `get_schema_async` without `asyncio.to_thread`), and C-API vectorcall `to_schema` projection (`orm_to_struct_vectorcall`).
- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-extensions-sqlspec-storage-py] (`sqlspec/storage.py`) implements `sqlspec.protocols.ObjectStoreProtocol` over `SyncObjectStore` / `AsyncObjectStore` (`read_bytes[_async]`, `write_bytes[_async]`, `read_arrow[_async]`, `write_arrow[_async]`, `stream_arrow[_async]`, `sign[_async]`, `stream_response`, and `ArrowFlightSerializer`).

## 4. `+vtb` Dialects, Types, Alembic (`sqlalchemy/dialects.py`, `sqlalchemy/alembic.py`, `sqlalchemy/types.py`)

| Symbol | Responsibility |
|---|---|
| `DIALECT_SCHEMES` | `{postgresql,oracle,mysql,sqlite,mssql,duckdb,bigquery,spanner,spangres}+{vertebra,vtb,vertebra_async,vtb_async}`. |
| Real Dialects (`sqlalchemy/dialects.py`) | Subclasses of SQLAlchemy 2.1 base dialects (`SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect`, `SyncMySQLDialect` / `AsyncMySQLDialect`, `SyncSQLiteDialect` / `AsyncSQLiteDialect`, `SyncOracleDialect` / `AsyncOracleDialect`, `SyncMSSQLDialect` / `AsyncMSSQLDialect`, `SyncDuckDBDialect` / `AsyncDuckDBDialect`, `SyncBigQueryDialect` / `AsyncBigQueryDialect`, `SyncSpannerDialect` / `AsyncSpannerDialect`, `SyncSpangresDialect` / `AsyncSpangresDialect`) with `NullPool` + `LeaseBroker` connection leasing, Cloud SQL / AlloyDB dialer integration, and `SyncDBAPIModule` / `AsyncDBAPIModule` binding. |
| `SyncDBAPIModule` / `SyncDBAPIConnection` / `SyncDBAPICursor` (and `Async*` counterparts) | PEP 249 surface over `vertebra.db` drivers (`postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`) resolved through `DatabaseRegistry.default().get_shared_sync_driver` / `get_shared_async_driver` (`share_pool=True` default, acquiring `LeaseBroker` slots around SQLite queries when `broker` is attached), plus `execute_arrow_shm()` returning `ArrowCursor`. Non-supported URLs raise [`UnsupportedBackendError`](../../../../../src/py/vertebra/typing.py). |
| `SyncEngineAdapter` | `run_sync(fn, ...)` submits to `SyncPool.submit_async`. `requires_greenlet` is `not is_free_threaded()`. |
| [`Vector`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^src-extensions-sqlalchemy-types-py] / `VectorDistanceExpr` | `get_col_spec` renders Oracle `VECTOR(dim\|*, FLOAT32\|...)`, PG/MySQL/`spangres` `VECTOR(dim)`, BigQuery `ARRAY<FLOAT64>`, Spanner GoogleSQL `ARRAY<FLOAT32>(vector_length=>dim)` (or `ARRAY<FLOAT64>`), otherwise `BLOB`. `cosine_distance` / `l2_distance` / `inner_product` compile to Oracle `VECTOR_DISTANCE`, pgvector `<=>`/`<->`/`<#>`, BigQuery/Spanner `COSINE_DISTANCE`/`EUCLIDEAN_DISTANCE`/`DOT_PRODUCT`, MySQL `DISTANCE_*`, or `vec_distance_*`. |
| [`BinaryJSON`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py) | `JSONB` on PG/SQLite/`spangres`, `JSON` on BigQuery/Spanner/MySQL/Oracle/MSSQL/DuckDB. Encodes with `encode_json`. |
| [`AlembicImpl`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic.py)[^src-extensions-sqlalchemy-alembic-py] / `BigQueryAlembicImpl` / `SpannerAlembicImpl` | `register_all` registers each `+vtb` key in `DefaultImpl._impls`. `BigQueryAlembicImpl` sets `transactional_ddl = False`, while `SpannerAlembicImpl` buffers DDL statements emitted during a migration (`buffer_ddl` / `flush_ddl_batch`) and submits them in a single `UpdateDatabaseDdl` LRO batch instead of one 10–60s schema change per statement. Static `render_item` and `compare_type` hooks handle `Vector`/`BinaryJSON` and treat `JSON`/`JSONB`/`OSON` as equal. `resolve_migration_url` returns `PostgreSQLConfig.effective_direct_dsn`. |
| `CreateViewOp` / `DropViewOp` / `CreateVectorIndexOp` / `DropVectorIndexOp` | DDL strings. `OR REPLACE` is dropped on SQLite and `IF EXISTS` is dropped on Oracle. `CreateVectorIndexOp` compiles `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR GRAPH` on Oracle, `USING hnsw` on PostgreSQL/`spangres`, `USING IVF` on BigQuery, and `USING ScaNN` on Spanner GoogleSQL. `reverse()` gives the inverse op. |
| [`SelectInLoadSpec`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py)[^src-extensions-sqlalchemy-dialects-py], `InsertSentinelSpec`, `DeleteUsingClause`, `ParameterizedSql` | Chunk size 1–1000 (Oracle `ORA-01795` limit), `num_batches`; sentinel-column settings; `DELETE ... USING ...` rendering with identifier validation checks on table names; conversion of PEP 750 template strings (`strings`/`interpolations`) or a static-parts sequence into `:p_N` named binds. |

Provenance sources: [^src-extensions-sqlspec-init-py], [^src-extensions-sqlspec-plugin-py], [^src-extensions-sqlspec-provider-py], [^src-extensions-alchemy-init-py], [^src-extensions-alchemy-plugin-py], [^src-pyproject-toml].

[^src-extensions-sqlspec-init-py]: `src/py/vertebra/extensions/sqlspec/__init__.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py).
[^src-extensions-sqlspec-plugin-py]: `src/py/vertebra/extensions/sqlspec/plugin.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/plugin.py).
[^src-extensions-sqlspec-adapters-py]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/adapters.py).
[^src-extensions-sqlspec-proxy-py]: `src/py/vertebra/extensions/sqlspec/proxy.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/proxy.py).
[^src-extensions-sqlspec-provider-py]: `src/py/vertebra/extensions/sqlspec/provider.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/provider.py).
[^src-extensions-sqlspec-storage-py]: `src/py/vertebra/extensions/sqlspec/storage.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/storage.py).
[^src-extensions-sqlspec-events-py]: `src/py/vertebra/extensions/sqlspec/events.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/events.py).
[^src-extensions-alchemy-init-py]: `src/py/vertebra/extensions/alchemy/__init__.py` — Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py).
[^src-extensions-alchemy-plugin-py]: `src/py/vertebra/extensions/alchemy/plugin.py` — Python implementation module (src/py/vertebra/extensions/alchemy/plugin.py).
[^src-extensions-alchemy-broker-py]: `src/py/vertebra/extensions/alchemy/broker.py` — Python implementation module (src/py/vertebra/extensions/alchemy/broker.py).
[^src-extensions-alchemy-storage-py]: `src/py/vertebra/extensions/alchemy/storage.py` — Python implementation module (src/py/vertebra/extensions/alchemy/storage.py).
[^src-extensions-sqlalchemy-dialects-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py).
[^src-extensions-sqlalchemy-types-py]: `src/py/vertebra/extensions/sqlalchemy/types.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/types.py).
[^src-extensions-sqlalchemy-alembic-py]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/alembic.py).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
