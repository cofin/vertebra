---
type: Guide
title: SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions
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
  at: '2026-11-04T04:33:00Z'
last_reviewed_commit: 9077dd3e
verified:
- by: agent:flow-executor
  at: '2026-11-22T01:02:00Z'
  commit: 9077dd3e
- by: agent:flow-executor
  at: '2026-11-21T21:08:00Z'
  commit: 06e99fbf
- by: agent:bb-executor
  at: '2026-11-21T01:17:00Z'
  commit: 06e99fbf
- by: agent:flow-executor
  at: '2026-11-17T08:15:00Z'
  commit: 5a833832
- by: agent:flow-archive
  at: '2026-11-15T17:48:00Z'
  commit: 246d7d0e
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
  resource: src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver
  title: Oracle drivers over the standalone vertebra._oracle module
- id: src-extensions-sqlspec-init-py
  resource: src/py/vertebra/extensions/sqlspec/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py)
- id: src-extensions-sqlspec-plugin-py
  resource: src/py/vertebra/extensions/sqlspec/plugin.py
  title: Python implementation module (src/py/vertebra/extensions/sqlspec/plugin.py)
- id: src-extensions-sqlspec-driver-py
  resource: src/py/vertebra/extensions/sqlspec/_driver.py
  title: Shared SQLSpec SyncAdapterConfig, AsyncAdapterConfig, BaseSyncDriver, and BaseAsyncDriver
- id: src-extensions-sqlspec-adapters-py
  resource: src/py/vertebra/extensions/sqlspec/adapters/__init__.py
  title: Per-engine SQLSpec 0.74.1+ adapter packages (postgres, sqlite, mysql, oracle, mssql, duckdb, bigquery, spanner)
- id: src-extensions-sqlspec-postgres-typing-py
  resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/_typing.py
  title: Per-session and connection contexts plus TypedDict schemas across adapter packages
- id: src-extensions-sqlspec-postgres-reconcile-py
  resource: src/py/vertebra/extensions/sqlspec/adapters/postgres/reconcile.py
  title: Per-adapter extension schema reconcilers (reconcile.py)
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
- id: src-cache-init-py
  resource: src/py/vertebra/cache/__init__.py
  title: Framework-neutral cache store package (src/py/vertebra/cache/__init__.py)
- id: src-extensions-alchemy-cache-py
  resource: src/py/vertebra/extensions/alchemy/cache.py
  title: Advanced Alchemy cache regions, serializers, and ZeroCopyServiceMixin (src/py/vertebra/extensions/alchemy/cache.py)
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
  resource: src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py)
- id: src-extensions-sqlalchemy-dialects-base-py
  resource: src/py/vertebra/extensions/sqlalchemy/dialects/_base.py
  title: Shared SQLAlchemy 2.1 dialect mixins and disconnect checks (src/py/vertebra/extensions/sqlalchemy/dialects/_base.py)
- id: src-extensions-sqlalchemy-async-adapter-py
  resource: src/py/vertebra/extensions/sqlalchemy/async_adapter.py
  title: AsyncAdapt_dbapi_* subclasses and SQLAlchemyAsyncDBAPI (src/py/vertebra/extensions/sqlalchemy/async_adapter.py)
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
  resource: src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py
  title: Python implementation module (src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py)
- id: src-extensions-sqlalchemy-alembic-hooks-py
  resource: src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py
  title: Alembic 1.18+ column comparator and item rendering hooks (src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py)
- id: src-extensions-sqlalchemy-alembic-impl-py
  resource: src/py/vertebra/extensions/sqlalchemy/alembic/impl.py
  title: Alembic DefaultImpl subclasses and Spanner DDL batching (src/py/vertebra/extensions/sqlalchemy/alembic/impl.py)
- id: src-extensions-sqlalchemy-alembic-ops-py
  resource: src/py/vertebra/extensions/sqlalchemy/alembic/ops.py
  title: Alembic MigrateOperation view and vector index operations (src/py/vertebra/extensions/sqlalchemy/alembic/ops.py)
- id: src-pyproject-toml
  resource: pyproject.toml
  title: Python package metadata, dependencies, and tool configuration
- id: src-storage-arrow-codecs
  resource: src/py/vertebra/storage/_arrow.py
  title: Bounded PyArrow codec scheduling and native I/O adapters
---

# SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions

<!-- truth: start -->
- Across all eight SQL engines, SQLSpec and SQLAlchemy share the engine's native cursor as their execution seam; cached SQLSpec statements execute in one native call via `set_cached_statement_executor` with no Python row-coercion, SQL-scanning, or error-translation layer.
- `create_sqlspec_plugin` and `create_alchemy_plugin` (plus FastAPI, Starlette, and Flask factories) return upstream plugin instances wired to `LeaseBroker` (`SyncBrokerPool` / `AsyncBrokerPool` for brokered configs, `NullPool` on direct `+vtb` URLs), `ObjectStoreBackend`, and `CacheConfig`.
- Per-engine `sqlspec>=0.74.1` adapters (`vertebra.extensions.sqlspec.adapters.*`), `ProxySyncConfig` / `ProxyAsyncConfig` wrappers, and 36 SQLAlchemy 2.1 `+vertebra`/`+vtb` dialect entry points share pools through `DatabaseRegistry.default()` with zero monkeypatching.
- Each SQLSpec adapter isolates `core.py` helpers, specializes 4-arg `SyncAdapterConfig` / `AsyncAdapterConfig`, builds fresh drivers per `_typing.py` context, declares store classes via `LazyStoreAttr`, and ships upstream-based ADK stores, data dictionaries, and `reconcile.py`.
- `ProxySyncDriver` and `ProxyAsyncDriver` implement SQLSpec dispatch hooks (mapping remote errors to `sqlspec.exceptions`), stream chunks via `stream_open`/`stream_fetch`/`stream_close` RPCs, and acquire leases in `provide_connection`.
- `vertebra.cache` (`SyncCacheStore`, `AsyncCacheStore`) provides framework-neutral `bytes` caching across `memory://`, `shm://`, `valkey(s)://`, `redis(s)://`, and object-store URLs; `SyncCacheRegion` / `AsyncCacheRegion` implement Advanced Alchemy regions returning `NO_VALUE` on miss.
- `alchemy/broker.py` imports `SyncBrokerLeaseSource` / `AsyncBrokerLeaseSource` from `vertebra.extensions.broker.transport`; `ObjectStoreBackend` preserves `content_type` and `metadata` across both SQLSpec and Advanced Alchemy [^src-extensions-sqlspec-storage-py] [^src-extensions-alchemy-storage-py].
<!-- truth: end -->

These packages integrate SQLSpec, Advanced Alchemy, SQLAlchemy 2.1, and Alembic without hard-importing optional dependencies at module load (using [`vertebra.utils.module_loader`](../../../../../src/py/vertebra/utils/module_loader.py)). Unit suites live under `src/py/tests/unit/extensions/` (`test_drop_in_names_and_generics.py`, `test_sqlalchemy_vtb_dialects_and_dbapi.py`, `test_sqlspec_vtb_stacks.py`, `test_sqlalchemy_and_sqlspec_broker_proxy.py`, `test_sqlspec_driver_dispatch.py`, `test_sqlspec_shm_proxy.py`, and per-engine adapter tests).

## 1. Plugins, Tier-1 Adapters & Tier-2 Broker Proxy

Shared plugin wiring (`on_app_init`) installs state into `app_config.state` and registers session/connection DI providers wrapped in `litestar.di.Provide(..., sync_to_thread=False)` when Litestar is installed, never overwriting existing keys (`on_cli_init` is a no-op).

| Component | Source | Responsibility |
|---|---|---|
| `create_sqlspec_plugin` | `sqlspec/plugin.py` | Returns upstream `SQLSpecPlugin`, registering optional `ObjectStoreBackend` under `storage_alias` and binding configs without shadow wrappers. |
| `create_alchemy_plugin` / `create_{fastapi,starlette,flask}_alchemy` | `alchemy/plugin.py`, `alchemy/__init__.py` | Returns upstream `SQLAlchemyPlugin` or `AdvancedAlchemy` with `storage_key` uniqueness validation and lazy exports (`ServiceGroup`, `FilterSet`, `RelationshipFilter`, `SchemaDumpConfig`). |
| `create_{sync,async}_alchemy_config` / `normalize_alchemy_url` | `alchemy/plugin.py` | Framework-aware PEP 695 helpers normalizing shorthand URLs to `+vtb`/`+vtb_async` and wiring `SyncBrokerPool`, `ObjectStoreBackend`, and `CacheConfig` without calling global `setup_cache_listeners()`. |
| [`SyncAdapterConfig` / `AsyncAdapterConfig`](../../../../../src/py/vertebra/extensions/sqlspec/_driver.py)[^src-extensions-sqlspec-driver-py] & [`adapters/`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/__init__.py)[^src-extensions-sqlspec-adapters-py] | `sqlspec/_driver.py`, `sqlspec/adapters/<engine>/` | **Tier-1 native adapters (`sqlspec>=0.74.1`)**: 4-arg engine configs (`adapter_name = "vertebra_<engine>"`), drivers, upstream data dictionaries, ADK/events/Litestar stores (`LazyStoreAttr`), [`reconcile.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/reconcile.py)[^src-extensions-sqlspec-postgres-reconcile-py], `replicas=`, routing sessions, and streaming `select_to_storage`. |
| Session & Connection Contexts | [`adapters/<engine>/_typing.py`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/_typing.py)[^src-extensions-sqlspec-postgres-typing-py] | `TypedDict` parameter schemas plus sync/async session and connection contexts. Entering acquires a lease and creates a fresh driver; exiting closes streams, rolls back open transactions, optionally resets session state (`reset_session_on_release`), and releases the lease. |
| [`Proxy*Config`](../../../../../src/py/vertebra/extensions/sqlspec/plugin.py)[^src-extensions-sqlspec-plugin-py], [`Proxy*Driver`](../../../../../src/py/vertebra/extensions/sqlspec/proxy.py)[^src-extensions-sqlspec-proxy-py], [`SQLSpec*Provider`](../../../../../src/py/vertebra/extensions/sqlspec/provider.py)[^src-extensions-sqlspec-provider-py] | `sqlspec/{plugin,proxy,provider}.py` | **Tier-2 universal SHM broker proxy**: wraps any SQLSpec/Alchemy config with SHM broker leasing, maps remote errors to `sqlspec.exceptions`, and streams chunks via `stream_open`/`stream_fetch`/`stream_close` RPCs. |
| [`ChangeFeed*EventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py)[^src-extensions-sqlspec-events-py] | `sqlspec/events.py` | Bridges SQLSpec transactions to Vertebra CDC `ChangeFeed` with durable checkpoints and `> 8,000` B payload spillover. |

### Adapter Internals

- **Single Native Cursor Seam**: SQLSpec (`with_cursor`) and SQLAlchemy (`SyncDBAPIConnection.cursor` / `AsyncDBAPIConnection.cursor`) both execute directly on the engine's native cursor.
- **Cached-Statement Executor**: `BaseSyncDriver` and `BaseAsyncDriver` register `_run_cached_statement` via `set_cached_statement_executor(..., owner=self._cache_hit_owner())` at construction [^src-extensions-sqlspec-driver-py]. SQLite and DuckDB (without dynamic secrets) bind client `fetch_rows`/`execute_dml` directly; network engines bind driver `execute_with_description`/`execute_dml`. `_run_cached_statement` executes once, builds `SQLResult.create_cached_select` or `DMLResult`, and chains native errors via `_map_exception`.
- **ADK, Data Dictionaries & Events**: ADK stores subclass upstream SQLSpec ADK bases via `ensure_module(..., package="google-adk", extra="adk")` with zero fallback stores. All eight adapters subclass upstream sync/async dialect dictionaries (Oracle adds 26ai Duality View helpers; BigQuery adds `get_partitions`; Postgres and Oracle ship native event backends).
- **Tenant Predicates & Pooling**: `apply_tenant_predicate` raises `NotSupportedError` by default. Postgres compiles `set_config($1, $2, $3)` over `vertebra._postgres`, Oracle runs `OracleCore.build_session_context_sql` over `vertebra._oracle` [^src-oracle-driver-py], and BigQuery/Spanner reject tenant predicates in constructors. Shared pools resolve via `DatabaseRegistry.default()`; brokered Alchemy configs use `SyncBrokerPool` / `AsyncBrokerPool`, while direct `+vtb` URLs with `?shm_broker=true` default to `NullPool`.

## 2. Connection Broker (`extensions/broker/`, `alchemy/broker.py`, `sqlalchemy/pool.py`)

| Symbol | Responsibility |
|---|---|
| [`BrokerConfig`](../../../../../src/py/vertebra/extensions/broker/transport.py)[^src-extensions-broker-transport-py], `BrokerMetrics`, `ChannelMessage` | Pool/TTL config (`acquire_timeout_ms`, `lease_ttl_ms`), metrics counters, and array-like pub/sub frame re-exported by [`alchemy/broker.py`](../../../../../src/py/vertebra/extensions/alchemy/broker.py)[^src-extensions-alchemy-broker-py]. |
| `SyncBrokerLeaseSource` / `AsyncBrokerLeaseSource` & `*SHMSessionBroker` | Imported into `alchemy/broker.py` from `extensions/broker/transport.py`; manage `acquire`/`release`/`in_use` with `_active_leases` tracking (`AsyncSHMSessionBroker` rejects sync SQLAlchemy pool hooks with `InterfaceError`). |
| `SyncConnectionLease` / `AsyncConnectionLease` | TTL-bound context managers; set `_released = True` only after native release succeeds (`AsyncConnectionLease.release_sync()` delegates or raises `InterfaceError`). |
| `SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport` & `AsyncBrokerStream` | Attach to `LeaseBroker`, `StatementCache`, and `ShmChannelSlot` via named SHM or UDS `SCM_RIGHTS`. `AsyncSHMBrokerTransport` has no `*_sync` methods, offloads blocking native ops onto `SyncPool.default().submit_async(...)`, and streams DBRPC frames via `AsyncBrokerStream`. |
| `BrokerPlugin` / `BrokerHost` / `SyncBrokerPool` | Installs broker state/metrics, tracks disposal failures in `ShutdownReport`, and backs SQLAlchemy pooling (`SyncBrokerPool` / `AsyncBrokerPool` with `reset_on_return` and `?broker=true` routing). |

## 3. Object-Storage & Caching Adapters

- **Framework-Neutral Cache (`vertebra.cache`)**: [`SyncCacheStore` / `AsyncCacheStore` / `CacheStats`](../../../../../src/py/vertebra/cache/__init__.py)[^src-cache-init-py] store raw `bytes` across `memory://`, `shm://`, `valkey(s)://`, `redis(s)://`, and object-store URLs (`VTBSTORE\x01`) with no ORM imports. Keys over 128 bytes hash with `blake2b`; SHM values over 856 bytes evict stale L1 entries.
- **Advanced Alchemy Cache & Zero-Copy Projection**: [`SyncCacheRegion` / `AsyncCacheRegion`](../../../../../src/py/vertebra/extensions/alchemy/cache.py)[^src-extensions-alchemy-cache-py] wrap `vertebra.cache`, returning `NO_VALUE` on miss. `create_cache_config(...)` pairs upstream `CacheManager` with `msgspec` serializers; `ZeroCopyServiceMixin.to_schema` projects rows via `ZeroCopyCodecRegistry` (`orm_to_struct`).
- **Advanced Alchemy Storage**: [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py)[^src-extensions-alchemy-storage-py] subclasses `advanced_alchemy.types.file_object.StorageBackend`, forwarding `content_type` and `metadata`. `register_storage_backend` registers concrete instances in `StorageRegistry`, rejecting conflicting keys or key-only registration [^src-extensions-alchemy-plugin-py].
- **SQLSpec Storage & Arrow Codecs**: [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-extensions-sqlspec-storage-py] implements `ObjectStoreProtocol` (`supports_direct_arrow_write` for `parquet`, `arrow`, `ipc`, `feather`, `csv`) and delegates `register_storage_alias` to `StorageRegistry.register_alias`. `read_arrow_*` returns complete Tables, `write_arrow_*` / `write_arrow_cursor_*` return `None` while keeping PyArrow work in `SyncPool` and async I/O on the caller loop, Parquet batch readers default to `pre_buffer=False`, and SQLSpec writers preserve PyArrow's unspecified compression default [^src-storage-arrow-codecs].

## 4. `+vtb` Dialects, Types & Alembic (`sqlalchemy/`)

| Component | Responsibility |
|---|---|
| `DIALECT_SCHEMES` / `ALL_DIALECT_SCHEMES` | Registers 36 sync/async entry points across 9 engines (`postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`, `bigquery`, `spanner`, `spangres`) using `+{vertebra,vtb,vertebra_async,vtb_async}`. |
| Dialects ([`dialects/`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py)[^src-extensions-sqlalchemy-dialects-py], [`_base.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects/_base.py)[^src-extensions-sqlalchemy-dialects-base-py], [`pool.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/pool.py)[^src-extensions-sqlalchemy-pool-py]) | Subclass SQLAlchemy 2.1 base dialects (and optional upstream DuckDB/BigQuery/Spanner dialects via `ensure_module`; `Spangres` subclasses `PGDialect` with `supports_twophase = False`). Mixins handle `is_disconnect` (SQLSTATE, MySQL/Oracle/MSSQL/gRPC codes) and pool selection (`NullPool` for `?shm_broker=true` or BigQuery/Spanner, `SyncBrokerPool` for `?broker=true`, `StaticPool` for in-memory SQLite, `QueuePool` otherwise). |
| Async DB-API Adapter ([`async_adapter.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/async_adapter.py)[^src-extensions-sqlalchemy-async-adapter-py]) | `SQLAlchemyAsyncDBAPI`, `AsyncAdapt_vertebra_connection`, and `AsyncAdapt_vertebra_cursor` (`_aenter_cursor` returns the native async cursor directly without `__aenter__`) exposing `setinputsizes`, `var()`, `nextset()`, and `async_creator_fn`. |
| Standalone PEP 249 (`vertebra.db.dbapi`[^src-db-dbapi-init-py]) | `SyncDBAPIConnection` / `AsyncDBAPIConnection` bind directly to the driver's native cursor factory, validate `connect_args` allowlists, re-export `vertebra._vertebra` exceptions without translation, and run without SQLAlchemy imports. |
| Column Types ([`types.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^src-extensions-sqlalchemy-types-py]) | `Vector`, dialect vector types (`PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`), `BigQueryGeography` with `_VectorDistance` distance operators, and `MsgspecJSON` (`JSONB` on PG/SQLite, `NVARCHAR(MAX)` on MSSQL, `JSON` on Oracle/others). |
| Alembic ([`alembic/`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py)[^src-extensions-sqlalchemy-alembic-py], [`impl.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/impl.py)[^src-extensions-sqlalchemy-alembic-impl-py], [`hooks.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py)[^src-extensions-sqlalchemy-alembic-hooks-py], [`ops.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/ops.py)[^src-extensions-sqlalchemy-alembic-ops-py]) | Registers `DuckDB`, `BigQuery` (`transactional_ddl = False`), `Spanner`, and `Spangres` `DefaultImpl`s (with online DDL batching via `update_ddl` on Spanner/Spangres), autogenerate comparators/renderers for `Vector`/`MsgspecJSON`, and reversible `CreateViewOp` / `DropViewOp` / `CreateVectorIndexOp` / `DropVectorIndexOp` operations. |

Provenance sources: [^src-extensions-sqlspec-init-py], [^src-extensions-sqlspec-plugin-py], [^src-extensions-sqlspec-provider-py], [^src-extensions-alchemy-init-py], [^src-extensions-alchemy-plugin-py], [^src-pyproject-toml].

[^src-extensions-sqlspec-init-py]: `src/py/vertebra/extensions/sqlspec/__init__.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py).
[^src-extensions-sqlspec-plugin-py]: `src/py/vertebra/extensions/sqlspec/plugin.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/plugin.py).
[^src-extensions-sqlspec-driver-py]: `src/py/vertebra/extensions/sqlspec/_driver.py` — Shared SQLSpec `SyncAdapterConfig`, `AsyncAdapterConfig`, `BaseSyncDriver`, and `BaseAsyncDriver`.
[^src-extensions-sqlspec-adapters-py]: `src/py/vertebra/extensions/sqlspec/adapters/__init__.py` — Per-engine SQLSpec 0.74.1+ adapter packages (`postgres`, `sqlite`, `mysql`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`).
[^src-extensions-sqlspec-postgres-typing-py]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/_typing.py` — Per-session and connection contexts plus `TypedDict` schemas across adapter packages.
[^src-extensions-sqlspec-postgres-reconcile-py]: `src/py/vertebra/extensions/sqlspec/adapters/postgres/reconcile.py` — Per-adapter extension schema reconcilers (`reconcile.py`).
[^src-extensions-sqlspec-proxy-py]: `src/py/vertebra/extensions/sqlspec/proxy.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/proxy.py).
[^src-extensions-sqlspec-provider-py]: `src/py/vertebra/extensions/sqlspec/provider.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/provider.py).
[^src-extensions-sqlspec-storage-py]: `src/py/vertebra/extensions/sqlspec/storage.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/storage.py).
[^src-extensions-sqlspec-events-py]: `src/py/vertebra/extensions/sqlspec/events.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/events.py).
[^src-extensions-alchemy-init-py]: `src/py/vertebra/extensions/alchemy/__init__.py` — Python implementation module (src/py/vertebra/extensions/alchemy/__init__.py).
[^src-cache-init-py]: `src/py/vertebra/cache/__init__.py` — Framework-neutral cache store package (`SyncCacheStore`, `AsyncCacheStore`, `CacheStats`).
[^src-extensions-alchemy-cache-py]: `src/py/vertebra/extensions/alchemy/cache.py` — Advanced Alchemy cache regions, serializers, and `ZeroCopyServiceMixin`.
[^src-extensions-alchemy-plugin-py]: `src/py/vertebra/extensions/alchemy/plugin.py` — Python implementation module (src/py/vertebra/extensions/alchemy/plugin.py).
[^src-extensions-broker-transport-py]: `src/py/vertebra/extensions/broker/transport.py` — Python implementation module (src/py/vertebra/extensions/broker/transport.py).
[^src-extensions-alchemy-broker-py]: `src/py/vertebra/extensions/alchemy/broker.py` — Python implementation module (src/py/vertebra/extensions/alchemy/broker.py).
[^src-extensions-alchemy-storage-py]: `src/py/vertebra/extensions/alchemy/storage.py` — Python implementation module (src/py/vertebra/extensions/alchemy/storage.py).
[^src-extensions-sqlalchemy-dialects-py]: `src/py/vertebra/extensions/sqlalchemy/dialects/__init__.py` — Per-engine SQLAlchemy 2.1 sync and async dialects.
[^src-extensions-sqlalchemy-dialects-base-py]: `src/py/vertebra/extensions/sqlalchemy/dialects/_base.py` — Shared SQLAlchemy 2.1 dialect mixins and disconnect checks.
[^src-extensions-sqlalchemy-async-adapter-py]: `src/py/vertebra/extensions/sqlalchemy/async_adapter.py` — `AsyncAdapt_dbapi_*` subclasses and `SQLAlchemyAsyncDBAPI`.
[^src-extensions-sqlalchemy-pool-py]: `src/py/vertebra/extensions/sqlalchemy/pool.py` — `SyncBrokerPool`, `AsyncBrokerPool`, and `broker_creator`.
[^src-db-dbapi-init-py]: `src/py/vertebra/db/dbapi/__init__.py` — Standalone PEP 249 DB-API 2.0 interfaces and typed SQL driver protocols.
[^src-extensions-sqlalchemy-types-py]: `src/py/vertebra/extensions/sqlalchemy/types.py` — `Vector`, dialect vector types, `_VectorDistance`, and `MsgspecJSON`.
[^src-extensions-sqlalchemy-alembic-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py` — Alembic `DefaultImpl` subclasses, custom DDL ops, and autogenerate hooks.
[^src-extensions-sqlalchemy-alembic-hooks-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py` — Alembic 1.18+ column comparator and item rendering hooks.
[^src-extensions-sqlalchemy-alembic-impl-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/impl.py` — `DuckDBAlembicImpl`, `BigQueryAlembicImpl`, `SpannerAlembicImpl`, and `SpangresAlembicImpl`.
[^src-extensions-sqlalchemy-alembic-ops-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/ops.py` — `MigrateOperation` view and vector index DDL operations.
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.
[^src-storage-arrow-codecs]: `src/py/vertebra/storage/_arrow.py` — Bounded codecs, format options, and resource ownership.
[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/_sync.py::SyncOracleDriver` — Oracle drivers over the standalone `vertebra._oracle` module.
