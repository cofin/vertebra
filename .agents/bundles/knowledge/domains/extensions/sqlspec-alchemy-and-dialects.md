---
type: Guide
title: SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions
description: The `vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`, and
  `vertebra.extensions.sqlalchemy` packages providing native SHM adapters (`SyncAdapterConfig`,
  `AsyncAdapterConfig`), universal SHM broker proxies (`ProxySyncConfig`, `ProxyAsyncConfig`),
  `+vtb` dialect implementations, column types, and CDC event backends.
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
  at: '2026-11-04T04:33:00Z'
verified:
- by: agent:flow-archive
  at: '2026-11-15T17:48:00Z'
  commit: 28415b3f
- by: agent:flow-executor
  at: '2026-11-14T01:18:00Z'
  commit: 7116ba13
- by: agent:flow-executor
  at: '2026-11-09T15:23:00Z'
  commit: af1b3d30
- by: agent:codex
  at: '2026-11-08T18:27:30.456016Z'
  commit: 067fa901
sources:
- id: src-oracle-driver-py
  resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
  title: Oracle drivers over the standalone vertebra._oracle module
- id: src-extensions-sqlspec-init-py
  resource: src/py/vertebra/extensions/sqlspec/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py)
- id: src-extensions-sqlspec-plugin-py
  resource: src/py/vertebra/extensions/sqlspec/plugin.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/plugin.py)
- id: src-extensions-sqlspec-adapters-py
  resource: src/py/vertebra/extensions/sqlspec/adapters.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/adapters.py)
- id: src-extensions-sqlspec-bigquery-py
  resource: src/py/vertebra/extensions/sqlspec/bigquery.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/bigquery.py)
- id: src-extensions-sqlspec-spanner-py
  resource: src/py/vertebra/extensions/sqlspec/spanner.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/spanner.py)
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
- id: src-extensions-broker-transport-py
  resource: src/py/vertebra/extensions/broker/transport.py
  title: Python implementation module (src/py/vertebra/extensions/broker/transport.py)
- id: src-extensions-alchemy-broker-py
  resource: src/py/vertebra/extensions/alchemy/broker.py
  title: Python implementation module (src/py/vertebra/extensions/alchemy/broker.py)
- id: src-extensions-alchemy-storage-py
  resource: src/py/vertebra/extensions/alchemy/storage.py
  title: Python implementation module (src/py/vertebra/extensions/alchemy/storage.py)
- id: src-extensions-sqlalchemy-dialects-py
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py)
- id: src-extensions-sqlalchemy-pool-py
  resource: src/py/vertebra/extensions/sqlalchemy/pool.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/pool.py)
- id: src-db-dbapi-init-py
  resource: src/py/vertebra/db/dbapi/__init__.py
  title: Python implementation module (src/py/vertebra/db/dbapi/__init__.py)
- id: src-extensions-sqlalchemy-types-py
  resource: src/py/vertebra/extensions/sqlalchemy/types.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/types.py)
- id: src-extensions-sqlalchemy-alembic-py
  resource: src/py/vertebra/extensions/sqlalchemy/alembic.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/alembic.py)
- id: src-pyproject-toml
  resource: pyproject.toml
  title: Python package metadata, dependencies, and tool configuration
- id: src-storage-arrow-codecs
  resource: src/py/vertebra/storage/_arrow.py
  title: Bounded PyArrow codec scheduling and native I/O adapters
---

# SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions

<!-- truth: start -->
- `create_sqlspec_plugin` and `create_alchemy_plugin` return real upstream `SQLSpecPlugin` and `SQLAlchemyPlugin` instances wired to Vertebra `LeaseBroker` (`BrokerPool` / `AsyncBrokerPool` for config factories, `NullPool` when dialect URLs use broker leasing directly) and `ObjectStoreBackend`.
- Native `<Engine>{Sync,Async}Config` adapters, `ProxySyncConfig` / `ProxyAsyncConfig` wrappers, and 36 SQLAlchemy 2.1 `+vertebra`/`+vtb` sync/async dialect entry points share pools via `DatabaseRegistry.default()`.
- Object-storage adapters preserve upstream method/return contracts while sharing native stores; asynchronous Arrow streams preserve demand and cleanup ownership [^src-extensions-sqlspec-storage-py] [^src-extensions-alchemy-storage-py].
<!-- truth: end -->

These packages provide Vertebra's two-tier shared-memory integration for SQLSpec, Advanced Alchemy, SQLAlchemy 2.1, and Alembic without hard-requiring those libraries at import time. Each module imports `litestar.di`, `litestar.typing`, `sqlalchemy.dialects`, or `alembic.ddl.impl` lazily through [`vertebra.utils.module_loader`](../../../../../src/py/vertebra/utils/module_loader.py) and loads without them when absent. Unit coverage under `src/py/tests/unit/extensions/` includes `test_drop_in_names_and_generics.py`, `test_data_storage_integrations.py`, and `test_sqlalchemy_and_sqlspec_vtb.py`.

## 1. Plugins, Tier-1 Native Configs & Tier-2 Universal Broker Proxy

Shared plugin behavior:

- `on_app_init(app_config)` installs a state object into `app_config.state` (a dict, or anything with `update`).
- It registers session and connection/engine providers in `app_config.dependencies`, wrapped in `litestar.di.Provide(..., sync_to_thread=False)` when Litestar is importable. Existing keys are never overwritten.
- `on_cli_init` is a no-op.

| Symbol | Source | Responsibility |
|---|---|---|
| `create_sqlspec_plugin(*configs, config=None, store=None, storage_alias="vertebra", ...)` | `sqlspec/plugin.py` | Thin factory returning real `sqlspec.extensions.litestar.SQLSpecPlugin`. Registers optional `ObjectStoreBackend` under `storage_alias` and binds all provided configs without shadow wrapper classes. |
| `create_alchemy_plugin(*configs, storage_backend=None, storage_key="default", **kwargs)` | `alchemy/plugin.py` | Thin factory returning real `advanced_alchemy.extensions.litestar.SQLAlchemyPlugin`. Registers optional storage backend under `storage_key` (`create_sync_alchemy_config` / `create_async_alchemy_config` wire `broker`, `storage_backend`, and `cache_config`). |
| `create_async_alchemy_config` / `create_sync_alchemy_config` / `normalize_alchemy_url` | `alchemy/plugin.py` | Config helpers returning real `advanced_alchemy.config.SQLAlchemyAsyncConfig` / `SQLAlchemySyncConfig`, normalizing `bigquery://`, `spanner://`, and `spangres://` URLs to their `+vtb` SQLAlchemy dialects and wiring `BrokerPool` / `AsyncBrokerPool` when a broker is attached. |
| [`SyncAdapterConfig` / `AsyncAdapterConfig`](../../../../../src/py/vertebra/extensions/sqlspec/adapters.py)[^src-extensions-sqlspec-adapters-py] | `sqlspec/adapters.py`, [`sqlspec/bigquery.py`](../../../../../src/py/vertebra/extensions/sqlspec/bigquery.py)[^src-extensions-sqlspec-bigquery-py], [`sqlspec/spanner.py`](../../../../../src/py/vertebra/extensions/sqlspec/spanner.py)[^src-extensions-sqlspec-spanner-py] | **Tier-1 native SHM adapters**: `sqlspec/adapters.py` defines `{PostgreSQL,MySQL,SQLite,Oracle,MSSQL,DuckDB}{Sync,Async}Config`, while `sqlspec/bigquery.py` defines `BigQuery{Sync,Async}Config` and `sqlspec/spanner.py` defines `Spanner{Sync,Async}Config`. Each concrete subclass sets explicit `connection_type` and `driver_type` attributes (`SyncPostgreSQLDriver`/`PostgreSQLSyncDriver`, `AsyncPostgreSQLDriver`/`PostgreSQLAsyncDriver`, etc.), while `SyncAdapterConfig` / `AsyncAdapterConfig` declare `ClassVar[type[Any]]` annotations pending upstream `DatabaseConfigProtocol` parity (GitHub issue `#131`). Backed by `vertebra.db` drivers, `LeaseBroker`, `StatementCache`, `ArrowCursor`, and `ObjectStoreBackend` (`load_from_object_store*`, `export_to_object_store*`, `create_object_table*`, `list_object_table_refs*`, `read_arrow_from_object_store*`, `write_arrow_to_object_store*`). `BaseSyncDriver.apply_tenant_predicate` and `BaseAsyncDriver.apply_tenant_predicate` raise native `NotSupportedError`. `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` compile statements in a single pass via `ParameterStyle.NUMERIC` (`$1..$N`), unwrap `TypedParameter` values, and prepare PostgreSQL tenant session binds (`select set_config($1, $2, $3)`) over `vertebra._postgres`, while `OracleSyncDriver` and `OracleAsyncDriver` execute `OracleCore.build_session_context_sql` against `vertebra._oracle` and return `predicate.to_sql_fragment(":tenant_id")`.[^src-oracle-driver-py] Native drivers resolve through `DatabaseRegistry.default().get_shared_sync_driver` / `get_shared_driver` (`share_pool=True` default when `broker` is omitted) so stacked Litestar, Django, and SQLAlchemy apps share pools, Cloud SQL/AlloyDB dialers, and `LeaseBroker`. |
| [`ProxySyncConfig` / `ProxyAsyncConfig`](../../../../../src/py/vertebra/extensions/sqlspec/plugin.py)[^src-extensions-sqlspec-plugin-py] & [`ProxySyncDriver` / `ProxyAsyncDriver`](../../../../../src/py/vertebra/extensions/sqlspec/proxy.py)[^src-extensions-sqlspec-proxy-py] | `sqlspec/plugin.py`, `sqlspec/proxy.py` | **Tier-2 universal SHM broker proxy**: `ProxySyncConfig` / `ProxyAsyncConfig` wrap any existing SQLSpec or Advanced Alchemy config with `LeaseBroker` slot acquisition and `ProxySyncDriver` / `ProxyAsyncDriver` (`ProxySyncSessionContext` / `ProxyAsyncSessionContext`) zero-copy `ShmRegion` Arrow IPC transfer. |
| [`ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py)[^src-extensions-sqlspec-events-py] | `sqlspec/events.py` | SQLSpec event backends bridging database transactions to Vertebra CDC `ChangeFeed`. |

BigQuery and Spanner configs and drivers reject non-None `tenant_predicate` before base initialization, supplied-core access, resolver/default-broker access, or native effects. These eight constructor guards prevent unsupported isolation sessions from existing. Unconfigured sessions preserve query, script, Arrow, batch, and snapshot behavior.

Pool defaults: `create_sync_alchemy_config` and `create_async_alchemy_config` wire `BrokerPool` / `AsyncBrokerPool` (`pool_key()`-partitioned SHM leasing with rollback-or-quarantine on check-in) when a broker is supplied, while direct `+vtb` dialects default to `NullPool` under explicit `shm_broker` URL parameters so per-process `QueuePool` does not multiply connections.

## 2. Connection Broker (`extensions/broker/{transport,host,plugin}.py`, `alchemy/broker.py`, & `sqlalchemy/pool.py`)

| Symbol | Responsibility |
|---|---|
| [`BrokerConfig`](../../../../../src/py/vertebra/extensions/broker/transport.py)[^src-extensions-broker-transport-py] / `BrokerMetrics` / `ChannelMessage` | Config (`pool_size`, `max_overflow`, `acquire_timeout`, `lease_ttl`, state keys), counters, and an array-like pub/sub envelope (re-exported alongside `SyncSHMSessionBroker` / `AsyncSHMSessionBroker` in [`alchemy/broker.py`](../../../../../src/py/vertebra/extensions/alchemy/broker.py)[^src-extensions-alchemy-broker-py]). |
| `SyncBrokerTransport` / `AsyncBrokerTransport` | Runtime-checkable protocol: `acquire` / `async def acquire`, `release`, and `metrics`. |
| `SyncConnectionLease` / `AsyncConnectionLease` | TTL-bound slot lease usable as a sync or async context manager. `release` is idempotent. |
| `SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport` | Bind to `LeaseBroker` and its co-located `StatementCache` + `ShmChannelSlot` ring in POSIX shared memory (via named segment or UDS `SCM_RIGHTS` attach) with `BrokerHost` owner supervision and `LeaseQuarantinedError` crash/failure isolation. |
| `BrokerPlugin` / `BrokerHost` / `BrokerPool` | Installs the broker transport and metrics snapshot under `db_broker` / `db_broker_metrics`, manages worker reclamation (`BrokerHost`), and coordinates SQLAlchemy / Advanced Alchemy SHM lease pools (`BrokerPool` / `AsyncBrokerPool`). |

## 3. Object-Storage & Caching Adapters

- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py)[^src-extensions-alchemy-storage-py] (`alchemy/storage.py`) implements `advanced_alchemy.types.file_object.StorageBackend` over `SyncObjectStore` / `AsyncObjectStore` with native sync and async methods (`save_object[_async]`, `get_content[_async]`, `get_range[_async]`, `get_metadata[_async]`, `exists[_async]`, `copy_object[_async]`, `list_objects[_async]`, `delete_object[_async]`, and `sign[_async]`) plus `FileObjectSessionTracker` transaction rollback safety.
- [`SQLAlchemyCacheBackend`](../../../../../src/py/vertebra/extensions/alchemy/cache.py), [`AsyncCacheManager`](../../../../../src/py/vertebra/extensions/alchemy/cache.py), and [`ZeroCopyServiceMixin`](../../../../../src/py/vertebra/extensions/alchemy/cache.py) (`alchemy/cache.py`) provide two-tier L1 (`ShmRegion` / LRU) + L2 (`SyncRedisDriver` / `AsyncRedisDriver` with Valkey RESP3 `CLIENT TRACKING` invalidation, prefix-scoped `SCAN` + `UNLINK` on `clear()` / `clear_async()` under `key_prefix="vtb:aa:"`, and millisecond `px` TTLs) caching, event-loop-native `CacheManager` async methods (`get_async`, `set_async`, `delete_async`, `get_schema_async` without `asyncio.to_thread`), and C-API vectorcall `to_schema` projection (`orm_to_struct_vectorcall`).
- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-extensions-sqlspec-storage-py] (`sqlspec/storage.py`) implements `sqlspec.protocols.ObjectStoreProtocol` with explicit `_sync` / `_async` methods for byte, text, Arrow, listing, metadata, and signing operations. Native stream/page/reader extensions share paired store handles and preserve qualified keys, URI checks, and backend errors.

SQLSpec `read_arrow_sync` / `read_arrow_async` return complete Tables, preserving empty schemas and IPC-file fallback. `write_arrow_*` and `write_arrow_cursor_*` return `None`; cursor writes accept `buffer_view()` objects and raw IPC buffer-protocol inputs. Their private batch codec bridge keeps synchronous PyArrow work in `SyncPool` and native async I/O on the caller loop. Pattern streams open files sequentially and yield batches on demand; closing or cancelling the outer async iterator closes its current inner batch iterator [^src-extensions-sqlspec-storage-py] [^src-storage-arrow-codecs].

Parquet batch readers default to `pre_buffer=False`; explicit `pre_buffer=True` preserves SQLSpec compatibility and permits broader prefetch. Full-Table reads materialize their return. SQLSpec writer options preserve PyArrow's unspecified compression default independently of the public storage writer's `zstd` default, forwarding PyArrow constructor/batch options and unsupported-option errors directly [^src-extensions-sqlspec-storage-py] [^src-storage-arrow-codecs].

Advanced Alchemy registers concrete instances through `StorageRegistry.register_backend`: use `register_storage_backend(store, key="assets")` or `register_storage_backend("assets", store=store)`. A key alone fails closed, and `set_default=True` explicitly sets the default backend class. Upstream `StorageRegistry` lookup methods handle resolution. `FileObject` / `StoredObject` consumers use `get_content[_async]`; bounded source iterators and native multipart uploads preserve original failures, cancellation, and transaction-tracker cleanup [^src-extensions-alchemy-storage-py].

## 4. `+vtb` Dialects, Types, Alembic (`sqlalchemy/dialects.py`, `sqlalchemy/alembic.py`, `sqlalchemy/types.py`)

| Symbol | Responsibility |
|---|---|
| `DIALECT_SCHEMES` / `ALL_DIALECT_SCHEMES` | `DIALECT_SCHEMES` registers the 8 sync schemes (`{postgresql,oracle,mysql,sqlite}+{vertebra,vtb}`); `ALL_DIALECT_SCHEMES` registers all 36 sync and async schemes across `{postgresql,oracle,mysql,sqlite,mssql,duckdb,bigquery,spanner,spangres}+{vertebra,vtb,vertebra_async,vtb_async}`. |
| Real Dialects (`sqlalchemy/dialects.py`, `async_adapter.py`, `pool.py`[^src-extensions-sqlalchemy-pool-py], `vertebra.db.dbapi`[^src-db-dbapi-init-py])[^src-extensions-sqlalchemy-dialects-py] | Subclasses of SQLAlchemy 2.1 base dialects (`SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` with `numeric_dollar` `$1..$N` paramstyle and `+vtb`/`+vertebra`/`+vtb_async`/`+vertebra_async` URL normalization in `PostgreSQLConfig.from_url`, `SyncMySQLDialect` / `AsyncMySQLDialect`, `SyncSQLiteDialect` / `AsyncSQLiteDialect`, `SyncOracleDialect` / `AsyncOracleDialect`, `SyncMSSQLDialect` / `AsyncMSSQLDialect`, `SyncDuckDBDialect` / `AsyncDuckDBDialect`, `SyncBigQueryDialect` / `AsyncBigQueryDialect`, `SyncSpannerDialect` / `AsyncSpannerDialect`, `SyncSpangresDialect` / `AsyncSpangresDialect`) delegating PEP 249 cursors/connections to standalone `vertebra.db.dbapi` (`SyncDBAPIModule`) and `SQLAlchemyAsyncDBAPI`, plus partitioned `BrokerPool`, `AsyncBrokerPool`, and `broker_creator`. |
| `SyncDBAPIModule` / `SyncDBAPIConnection` / `SyncDBAPICursor` (and `Async*` counterparts) | PEP 249 surface (`connect`, `cursor`, `begin`, `commit`, `rollback`, `close`, `execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`, plus `execute_with_description`, `execute_dml`, `execute_many_dml`, and `execute_raw_tuples` on `SyncDBAPIConnection`) over `vertebra.db` drivers (`postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`) resolved through `DatabaseRegistry.default().get_shared_sync_driver` / `get_shared_driver` (`share_pool=True` default, preserving explicit `driver=` instances verbatim and acquiring `LeaseBroker` slots around SQLite queries when `broker` is attached). When `DatabaseRegistry.parse_url` raises `ValueError`, `SyncDBAPIConnection` / `AsyncDBAPIConnection` raise the module's PEP 249 `InterfaceError` (`Cannot parse Vertebra database URL: …`) chained from it, with no SQLite substitution; optional engine connections require their matching installed extension extra (`vertebra._postgres`, `vertebra._mysql`, `vertebra._oracle`, `vertebra._mssql`, `vertebra._duckdb`, `vertebra._bigquery`, `vertebra._spanner`) and raise `MissingDependencyError` when omitted. |
| `SyncEngineAdapter` | `run_sync(fn, ...)` submits to `SyncPool.submit_async`. `requires_greenlet` is `not is_free_threaded()`. |
| [`Vector`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^src-extensions-sqlalchemy-types-py] / `VectorDistanceExpr` | `get_col_spec` renders Oracle `VECTOR(dim\|*, FLOAT32\|...)`, BigQuery `ARRAY<FLOAT64>`, Spanner GoogleSQL `ARRAY<FLOAT32>(vector_length=>dim)` (or `ARRAY<FLOAT64>`), SQLite `BLOB`, and `VECTOR(dim)`/`VECTOR` on PostgreSQL, MySQL/MariaDB, DuckDB, MSSQL, `spangres`, and fallback dialects. `cosine_distance` / `l2_distance` / `inner_product` compile to Oracle `VECTOR_DISTANCE`, pgvector `<=>`/`<->`/`<#>`, BigQuery/Spanner `COSINE_DISTANCE`/`EUCLIDEAN_DISTANCE`/`DOT_PRODUCT`, MySQL `DISTANCE_*`, or `vec_distance_*`. |
| [`BinaryJSON`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py) | `JSONB` on PG/SQLite/`spangres`, `NVARCHAR(MAX)` (`NVARCHAR(None)` dialect impl) on MSSQL, `JSON` on BigQuery/Spanner/MySQL/Oracle/DuckDB. Encodes with `encode_json`. |
| [`AlembicImpl`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic.py)[^src-extensions-sqlalchemy-alembic-py] / `BigQueryAlembicImpl` / `SpannerAlembicImpl` | `register_all` registers each `+vtb` key in `DefaultImpl._impls`. `BigQueryAlembicImpl` sets `transactional_ddl = False`, while `SpannerAlembicImpl` buffers DDL statements emitted during a migration (`buffer_ddl`, `flush_ddl_batch_sync`, `flush_ddl_batch_async`) and submits them in a single `UpdateDatabaseDdl` LRO batch instead of one 10–60s schema change per statement. Static `render_item` and `compare_type` hooks handle `Vector`/`BinaryJSON` and treat `JSON`/`JSONB`/`OSON` as equal. `resolve_migration_url` returns `PostgreSQLConfig.effective_direct_dsn`. |
| `CreateViewOp` / `DropViewOp` / `CreateVectorIndexOp` / `DropVectorIndexOp` | DDL strings. `OR REPLACE` is dropped on SQLite and `IF EXISTS` is dropped on Oracle. `CreateVectorIndexOp` compiles `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR GRAPH` on Oracle, `USING hnsw` on PostgreSQL/`spangres`, `OPTIONS(index_type = 'IVF', ...)` on BigQuery, and `OPTIONS (tree_type = 'SCANN', ...)` on Spanner GoogleSQL. `reverse()` gives the inverse op. |
| [`SelectInLoadSpec`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^src-extensions-sqlalchemy-types-py], `InsertSentinelSpec`, `DeleteUsingClause`, `ParameterizedSQL` | Chunk size 1–1000 (Oracle `ORA-01795` limit), `num_batches`; sentinel-column settings; `DELETE ... USING ...` statement rendering (`DELETE FROM <target_table> USING <using_tables> [WHERE ...]`); conversion of PEP 750 template strings (`strings`/`interpolations`) or a static-parts sequence into `:p_N` named binds. |

Provenance sources: [^src-extensions-sqlspec-init-py], [^src-extensions-sqlspec-plugin-py], [^src-extensions-sqlspec-provider-py], [^src-extensions-alchemy-init-py], [^src-extensions-alchemy-plugin-py], [^src-pyproject-toml].

[^src-extensions-sqlspec-init-py]: `src/py/vertebra/extensions/sqlspec/__init__.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py).
[^src-extensions-sqlspec-plugin-py]: `src/py/vertebra/extensions/sqlspec/plugin.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/plugin.py).
[^src-extensions-sqlspec-adapters-py]: `src/py/vertebra/extensions/sqlspec/adapters.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/adapters.py).
[^src-extensions-sqlspec-bigquery-py]: `src/py/vertebra/extensions/sqlspec/bigquery.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/bigquery.py).
[^src-extensions-sqlspec-spanner-py]: `src/py/vertebra/extensions/sqlspec/spanner.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/spanner.py).
[^src-extensions-sqlspec-proxy-py]: `src/py/vertebra/extensions/sqlspec/proxy.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/proxy.py).
[^src-extensions-sqlspec-provider-py]: `src/py/vertebra/extensions/sqlspec/provider.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/provider.py).
[^src-extensions-sqlspec-storage-py]: `src/py/vertebra/extensions/sqlspec/storage.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/storage.py).
[^src-extensions-sqlspec-events-py]: `src/py/vertebra/extensions/sqlspec/events.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/events.py).
[^src-extensions-alchemy-init-py]: `src/py/vertebra/extensions/alchemy/__init__.py` — Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py).
[^src-extensions-alchemy-plugin-py]: `src/py/vertebra/extensions/alchemy/plugin.py` — Python implementation module (src/py/vertebra/extensions/alchemy/plugin.py).
[^src-extensions-broker-transport-py]: `src/py/vertebra/extensions/broker/transport.py` — Python implementation module (src/py/vertebra/extensions/broker/transport.py).
[^src-extensions-alchemy-broker-py]: `src/py/vertebra/extensions/alchemy/broker.py` — Python implementation module (src/py/vertebra/extensions/alchemy/broker.py).
[^src-extensions-alchemy-storage-py]: `src/py/vertebra/extensions/alchemy/storage.py` — Python implementation module (src/py/vertebra/extensions/alchemy/storage.py).
[^src-extensions-sqlalchemy-dialects-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects.py).
[^src-extensions-sqlalchemy-pool-py]: `src/py/vertebra/extensions/sqlalchemy/pool.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/pool.py).
[^src-db-dbapi-init-py]: `src/py/vertebra/db/dbapi/__init__.py` — Python implementation module (src/py/vertebra/db/dbapi/__init__.py).
[^src-extensions-sqlalchemy-types-py]: `src/py/vertebra/extensions/sqlalchemy/types.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/types.py).
[^src-extensions-sqlalchemy-alembic-py]: `src/py/vertebra/extensions/sqlalchemy/alembic.py` — Python implementation module (src/py/vertebra/extensions/sqlalchemy/alembic.py).
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.

[^src-storage-arrow-codecs]: `src/py/vertebra/storage/_arrow.py` — Bounded codecs, format options, and resource ownership.

[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle drivers over the standalone `vertebra._oracle` module.
