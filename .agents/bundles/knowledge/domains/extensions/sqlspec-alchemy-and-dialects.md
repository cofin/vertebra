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
last_reviewed_commit: 5a833832
verified:
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
  resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
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
  title: Per-engine SQLSpec 0.72.1+ adapter packages (postgres, sqlite, mysql, oracle, mssql, duckdb, bigquery, spanner)
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
- `create_sqlspec_plugin` and `create_alchemy_plugin` (plus `create_{fastapi,starlette,flask}_alchemy`) return upstream plugin or extension instances wired to `LeaseBroker` (`BrokerPool` / `AsyncBrokerPool` for brokered configs, `NullPool` on direct `+vtb` URLs), `ObjectStoreBackend`, and `CacheConfig(region_factory=...)`.
- Per-engine `sqlspec>=0.72.1` adapters (`vertebra.extensions.sqlspec.adapters.*`), `ProxySyncConfig` / `ProxyAsyncConfig` wrappers, and 36 SQLAlchemy 2.1 `+vertebra`/`+vtb` dialect entry points share pools via `DatabaseRegistry.default()` with zero `_compat.py` monkeypatching.
- Every SQLSpec adapter isolates `core.py` helpers with zero `_driver.py` or sibling imports, specializes 4-arg `SyncAdapterConfig` / `AsyncAdapterConfig` generics, constructs fresh drivers per `_typing.py` session/connection context (with optional `reset_session_on_release`), exposes `adk_artifact_store_class` across all eight engines via `LazyStoreAttr`, and ships upstream-based ADK stores, data dictionaries, and `reconcile.py` schema reconcilers.
- `ProxySyncDriver` and `ProxyAsyncDriver` execute via SQLSpec dispatch hooks (mapping remote errors to `sqlspec.exceptions`), stream rows in chunks via `stream_open` / `stream_fetch` / `stream_close` broker RPCs on `SQLSpecSyncProvider` / `SQLSpecAsyncProvider`, and lease through `ProxySyncConfig.provide_connection` / `ProxyAsyncConfig.provide_connection`.
- `vertebra.cache` (`SyncCacheStore`, `AsyncCacheStore`, `CacheStats`) provides framework-neutral `bytes` caching across `memory://`, `shm://`, `valkey(s)://`, `redis(s)://`, and object-store URLs with zero ORM imports; `SyncCacheRegion` and `AsyncCacheRegion` implement Advanced Alchemy region protocols returning `NO_VALUE` on miss.
- `alchemy/broker.py` imports `SyncBrokerLeaseSource` and `AsyncBrokerLeaseSource` from `vertebra.extensions.broker.transport`, and `ObjectStoreBackend` forwards `content_type` and `metadata` while preserving upstream contracts [^src-extensions-sqlspec-storage-py] [^src-extensions-alchemy-storage-py].
<!-- truth: end -->

These packages provide Vertebra's two-tier shared-memory integration for SQLSpec, Advanced Alchemy, SQLAlchemy 2.1, and Alembic without hard-requiring those libraries at import time. Each module imports `litestar.di`, `litestar.typing`, `sqlalchemy.dialects`, or `alembic.ddl.impl` lazily through [`vertebra.utils.module_loader`](../../../../../src/py/vertebra/utils/module_loader.py) and loads without them when absent. Unit coverage under `src/py/tests/unit/extensions/` includes `test_drop_in_names_and_generics.py`, `test_data_storage_integrations.py`, `test_sqlalchemy_vtb_dialects_and_dbapi.py`, `test_sqlspec_vtb_stacks.py`, `test_sqlalchemy_and_sqlspec_broker_proxy.py`, `test_extensions_cache_adk_and_events.py`, `test_sqlspec_driver_dispatch.py`, `test_sqlspec_shm_proxy.py`, `test_sqlspec_sqlite_mysql_mssql_duckdb.py`, `test_sqlspec_bigquery_adapter.py`, `test_sqlspec_bigquery.py`, `test_sqlspec_spanner_adapter.py`, and `test_sqlspec_spanner.py` (alongside `src/py/tests/unit/storage/test_storage_core_and_ranges.py`, `test_storage_arrow_and_streaming.py`, and `test_storage_bulk_checksums_and_backends.py`).

## 1. Plugins, Tier-1 Native Configs & Tier-2 Universal Broker Proxy

Shared plugin behavior:

- `on_app_init(app_config)` installs a state object into `app_config.state` (a dict, or anything with `update`).
- It registers session and connection/engine providers in `app_config.dependencies`, wrapped in `litestar.di.Provide(..., sync_to_thread=False)` when Litestar is importable. Existing keys are never overwritten.
- `on_cli_init` is a no-op.

| Symbol | Source | Responsibility |
|---|---|---|
| `create_sqlspec_plugin(*configs, config=None, store=None, storage_alias="vertebra", ...)` | `sqlspec/plugin.py` | Thin factory returning real `sqlspec.extensions.litestar.SQLSpecPlugin`. Registers optional `ObjectStoreBackend` under `storage_alias` and binds all provided configs without shadow wrapper classes. |
| `create_alchemy_plugin(*configs, storage_backend=None, storage_key="default", **kwargs)` / `create_{fastapi,starlette,flask}_alchemy` | `alchemy/plugin.py`, `alchemy/__init__.py` | Thin factories returning upstream `SQLAlchemyPlugin` (Litestar) or `AdvancedAlchemy` (FastAPI, Starlette, Flask). Validate `storage_key` uniqueness and bind all configs without shadow wrappers; `alchemy/__init__.py` exposes exports lazily via `lazy_exports` and exposes `ServiceGroup`, `FilterSet`, `RelationshipFilter`, and `SchemaDumpConfig`. |
| `create_async_alchemy_config` / `create_sync_alchemy_config` / `normalize_alchemy_url` | `alchemy/plugin.py` | Framework-aware PEP 695 generic helpers (`framework: Literal["litestar", "fastapi", "starlette", "flask"] = "litestar"`) accepting one of `connection_string`, `routing_config`, or `engine_instance`, preserving explicit pool options, normalizing shorthand URLs to `+vtb` / `+vtb_async`, and wiring `BrokerPool`, `ObjectStoreBackend`, and `CacheConfig` without calling global `setup_cache_listeners()`. |
| [`SyncAdapterConfig` / `AsyncAdapterConfig`](../../../../../src/py/vertebra/extensions/sqlspec/_driver.py)[^src-extensions-sqlspec-driver-py] & [`adapters.<engine>`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/__init__.py)[^src-extensions-sqlspec-adapters-py] | `sqlspec/_driver.py`, `sqlspec/adapters/{postgres,sqlite,mysql,oracle,mssql,duckdb,bigquery,spanner}/` | **Tier-1 per-engine native SHM adapters (`sqlspec>=0.72.1`)**: per-engine subpackages export 4-arg concrete generics `{Postgres,MySQL,SQLite,Oracle,MSSQL,DuckDB,BigQuery,Spanner,Spangres}{Sync,Async}Config` (`adapter_name = "vertebra_<engine>"`), `<Engine>{Sync,Async}Driver`, `<Engine>{Sync,Async}DataDictionary`, `<Engine>{Sync,Async}ADKStore`, `<Engine>{Sync,Async}ADKMemoryStore`, `<Engine>{Sync,Async}ADKArtifactStore` (via `ArtifactReference` and `google-adk`), `<Engine>EventQueueStore`, `<Engine>LitestarStore`, and `reconcile.py` (`reconcile(context: MigrationContext)`)[^src-extensions-sqlspec-postgres-reconcile-py]. All eight engines declare `adk_store_class`, `adk_memory_store_class`, and `adk_artifact_store_class` via `LazyStoreAttr` (`SQLite`, `MySQL`, `MSSQL`, `DuckDB`, `BigQuery`, and `Spanner` also use `LazyStoreAttr` for `events_store_class` and `litestar_store_class`). Each adapter isolates `unwrap_typed_parameters`, `native_statement_config`, and `map_native_database_exception` in its own `core.py` with zero `_driver.py` or cross-adapter imports, declares `__slots__`, and implements `replicas=`, `provide_replica_session`, `provide_routing_session`, and streaming `select_to_storage` (`_select_for_storage` / `_write_storage_result`). |
| [`<Engine>{Sync,Async}SessionContext` / `<Engine>{Sync,Async}ConnectionContext`](../../../../../src/py/vertebra/extensions/sqlspec/adapters/postgres/_typing.py)[^src-extensions-sqlspec-postgres-typing-py] | `sqlspec/adapters/<engine>/_typing.py` | Each adapter defines `<Engine>ConnectionParams`, `<Engine>PoolParams`, and `<Engine>DriverFeatures` `TypedDict` types alongside sync and async session and connection context managers (plus `OracleSyncSessionlessTransactionContext` / `OracleAsyncSessionlessTransactionContext`). Entering a context acquires a connection or tenant lease and constructs a fresh driver per session (with zero `_driver_instance` caching), forwarding `provide_session` and `provide_connection` arguments; exiting closes open row streams, rolls back open transactions, resets session state when `reset_session_on_release=True`, releases the lease, and marks the driver closed. |
| [`ProxySyncConfig` / `ProxyAsyncConfig`](../../../../../src/py/vertebra/extensions/sqlspec/plugin.py)[^src-extensions-sqlspec-plugin-py], [`ProxySyncDriver` / `ProxyAsyncDriver`](../../../../../src/py/vertebra/extensions/sqlspec/proxy.py)[^src-extensions-sqlspec-proxy-py] & [`SQLSpecSyncProvider` / `SQLSpecAsyncProvider`](../../../../../src/py/vertebra/extensions/sqlspec/provider.py)[^src-extensions-sqlspec-provider-py] | `sqlspec/plugin.py`, `sqlspec/proxy.py`, `sqlspec/provider.py` | **Tier-2 universal SHM broker proxy**: `ProxySyncConfig` / `ProxyAsyncConfig` wrap any SQLSpec or Advanced Alchemy config with broker leasing in both `provide_session` and `provide_connection` (forwarding arguments, merging caller `extension_config` over default Litestar keys with caller precedence, and exposing `replicas=`, `provide_replica_session`, and `provide_routing_session`). `ProxySyncDriver` / `ProxyAsyncDriver` (`ProxySyncSessionContext` / `ProxyAsyncSessionContext`) implement SQLSpec dispatch hooks (with zero `execute` / `execute_many` overrides), map remote error types (`_SQLSPEC_ERROR_MAP`) to `sqlspec.exceptions` subclasses (`NotFoundError`, `IntegrityError`, `OperationalError`, fallback `SQLSpecError`), raise `NotFoundError` on empty `select_one`, and stream chunks via `_BrokerChunkSyncRowSource` / `_BrokerChunkAsyncRowSource` backed by `stream_open`, `stream_fetch`, and `stream_close` RPC ops on `SQLSpecSyncProvider` / `SQLSpecAsyncProvider` (which uses native `SyncSQLiteDriver` / `AsyncSQLiteDriver` rather than stdlib `sqlite3`). |
| [`ChangeFeedSyncEventBackend` / `ChangeFeedAsyncEventBackend`](../../../../../src/py/vertebra/extensions/sqlspec/events.py)[^src-extensions-sqlspec-events-py] | `sqlspec/events.py` | SQLSpec event backends implementing `SyncEventBackendProtocol` / `AsyncEventBackendProtocol` (`__slots__`), bridging database transactions to Vertebra CDC `ChangeFeed` with durable checkpoint persistence and `> 8,000`B `EventQueueStore` / object-store payload spillover. |

Adapter specifics across the eight engines:

- **ADK, Data Dictionaries & Events**: ADK session, memory, and artifact stores subclass upstream SQLSpec ADK bases (loaded via `ensure_module(..., package="google-adk", extra="adk")` with zero fallback stores), support `app_state_delta` / `user_state_delta` merged-state returns and owner-scoped `delete_entries_by_session(session_id, app_name, user_id)`, and execute memory ranking in GoogleSQL / Spanner SQL on BigQuery and Spanner. `MySQLSyncDataDictionary` and `MySQLAsyncDataDictionary` subclass upstream `sqlspec.data_dictionary.dialects.mysql.dictionary`, while the other seven adapters subclass `SyncDataDictionaryBase` / `AsyncDataDictionaryBase` directly (`Oracle` adds 26ai JSON Relational Duality View catalog helpers; `Postgres` and `Oracle` also ship native `Postgres{Sync,Async}EventsBackend` and `Oracle{Sync,Async}EventsBackend`).
- **Tenant Predicates & Pool Routing**: `BaseSyncDriver.apply_tenant_predicate` and `BaseAsyncDriver.apply_tenant_predicate` raise `NotSupportedError` by default; `PostgresSyncDriver` / `PostgresAsyncDriver` compile statements via `ParameterStyle.NUMERIC` (`$1..$N`), unwrap `TypedParameter` values, and prepare PostgreSQL tenant session binds (`select set_config($1, $2, $3)`) over `vertebra._postgres`, while `OracleSyncDriver` / `OracleAsyncDriver` execute `OracleCore.build_session_context_sql` against `vertebra._oracle` and return `predicate.to_sql_fragment(":tenant_id")`.[^src-oracle-driver-py] Native drivers resolve through `DatabaseRegistry.default().get_shared_sync_driver` / `get_shared_driver` (`share_pool=True` default when `broker` is omitted). BigQuery and Spanner configs and drivers enforce eight constructor guards rejecting non-None `tenant_predicate` / `tenant_id` with `NotSupportedError` before base initialization, supplied-core access, resolver/default-broker access, or native effects, while unconfigured sessions preserve query, script, Arrow, batch, and snapshot behavior.

Pool defaults: `create_sync_alchemy_config` and `create_async_alchemy_config` wire `BrokerPool` / `AsyncBrokerPool` (`pool_key()`-partitioned SHM leasing with rollback-or-quarantine on check-in) when a broker is supplied, while direct `+vtb` dialects default to `NullPool` under explicit `shm_broker` URL parameters so per-process `QueuePool` does not multiply connections.

## 2. Connection Broker (`extensions/broker/{transport,host,plugin}.py`, `alchemy/broker.py`, & `sqlalchemy/pool.py`)

| Symbol | Responsibility |
|---|---|
| [`BrokerConfig`](../../../../../src/py/vertebra/extensions/broker/transport.py)[^src-extensions-broker-transport-py] / `BrokerMetrics` / `ChannelMessage` | Config (`pool_size`, `max_overflow`, `acquire_timeout_ms`, `lease_ttl_ms`, state keys), counters, and an array-like pub/sub envelope re-exported by [`alchemy/broker.py`](../../../../../src/py/vertebra/extensions/alchemy/broker.py)[^src-extensions-alchemy-broker-py]. |
| `SyncBrokerLeaseSource` / `AsyncBrokerLeaseSource` & `SyncSHMSessionBroker` / `AsyncSHMSessionBroker` | `alchemy/broker.py` imports `SyncBrokerLeaseSource` and `AsyncBrokerLeaseSource` from `vertebra.extensions.broker.transport`. Both session brokers run `acquire`, `release`, and `in_use` through the transport with single-index `_active_leases` tracking; `AsyncSHMSessionBroker` raises `InterfaceError` on synchronous SQLAlchemy `on_checkout` and `on_checkin` pool hooks. |
| `SyncBrokerTransport` / `AsyncBrokerTransport` | Runtime-checkable protocol: `acquire` / `async def acquire`, `release`, and `metrics`. |
| `SyncConnectionLease` / `AsyncConnectionLease` | TTL-bound slot lease usable as a sync or async context manager; `release()` marks `_released = True` only after native release succeeds, and `AsyncConnectionLease.release_sync()` releases synchronously or raises `InterfaceError` when the transport has none. |
| `SyncSHMBrokerTransport` / `AsyncSHMBrokerTransport` & `AsyncBrokerStream` | Bind to `LeaseBroker` and its co-located `StatementCache` + `ShmChannelSlot` ring in POSIX shared memory (via named segment or UDS `SCM_RIGHTS` attach) with `BrokerHost` owner supervision and `LeaseQuarantinedError` isolation. `AsyncSHMBrokerTransport` carries no `*_sync` methods, offloads blocking native operations via `SyncPool.default().submit_async(...)`, and returns `AsyncBrokerStream` (`__aiter__`, `__anext__`, `aclose()`, `DataError` on malformed frames) from `stream_dbrpc(...)`. |
| `BrokerPlugin` / `BrokerHost` / `BrokerPool` | Installs the broker transport and metrics snapshot under `db_broker` / `db_broker_metrics`, manages worker reclamation (`BrokerHost` tracking recovery failures in `ShutdownReport.disposal_errors` / `last_disposal_error`), and coordinates SQLAlchemy lease pools via `BrokerPool` / `AsyncBrokerPool` (and `broker_creator`) with `pool_size`/`max_overflow` bounds, SQLAlchemy `reset_on_return` (guarded by `in_greenlet()`), idle reuse on check-in, idle-only `dispose()`, and `?broker=true` routing through `DatabaseRegistry.default_broker()`. |

## 3. Object-Storage & Caching Adapters

- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py)[^src-extensions-alchemy-storage-py] (`alchemy/storage.py`) subclasses `advanced_alchemy.types.file_object.StorageBackend` over `SyncObjectStore` / `AsyncObjectStore`. `save_object` and `save_object_async` forward `FileObject.content_type` and `FileObject.metadata` as native attributes alongside `get_content[_async]`, `get_range[_async]`, `get_metadata[_async]`, `exists[_async]`, `copy_object[_async]`, `list_objects[_async]`, `delete_object[_async]`, and `sign[_async]`.
- [`SyncCacheStore` / `AsyncCacheStore` / `CacheStats`](../../../../../src/py/vertebra/cache/__init__.py)[^src-cache-init-py] (`vertebra.cache`) provide framework-neutral `bytes` caching returning `None` on miss with zero ORM or extension imports. Accepted schemes are `memory://`, `shm://` (`vertebra._vertebra.ShmCache`), `valkey(s)://`, `redis(s)://`, and `s3://`/`gs://`/`az://`/`r2://`/`oci://`/`file://` (`VTBSTORE\x01`). `clear()` and `delete_prefix()` stay namespace-scoped, keys over `128` UTF-8 bytes hash with `blake2b`, and SHM L1 values over `856` bytes evict stale L1 entries.
- [`SyncCacheRegion` / `AsyncCacheRegion`](../../../../../src/py/vertebra/extensions/alchemy/cache.py)[^src-extensions-alchemy-cache-py] (`alchemy/cache.py`) implement `SyncCacheRegionProtocol` and `AsyncCacheRegionProtocol` over `vertebra.cache`, returning upstream `NO_VALUE` on miss and clearing the namespace via parameterless `invalidate()`. `create_cache_config(...)` returns `CacheConfig(region_factory=...)` wired to `msgspec_model_serializer` / `msgspec_model_deserializer` and upstream `CacheManager` unmodified; `ZeroCopyServiceMixin.to_schema` projects sequences into `OffsetPagination` with `ResultConverter`-aligned overloads and `ZeroCopyCodecRegistry` (`orm_to_struct`).
- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py)[^src-extensions-sqlspec-storage-py] (`sqlspec/storage.py`) implements `sqlspec.protocols.ObjectStoreProtocol` with explicit `_sync` / `_async` methods for byte, text, Arrow, listing, metadata, and signing operations, forwarding `overwrite` on writes, `recursive` on deletes, and `supports_direct_arrow_write` (`parquet`, `arrow`, `ipc`, `arrow-ipc`, `feather`, `csv`), while `register_storage_alias` delegates strictly to public `StorageRegistry.register_alias`. Native stream/page/reader extensions share paired store handles and preserve qualified keys, URI checks, and backend errors.

SQLSpec `read_arrow_sync` / `read_arrow_async` return complete Tables, preserving empty schemas and IPC-file fallback. `write_arrow_*` and `write_arrow_cursor_*` return `None`; cursor writes accept `buffer_view()` objects and raw IPC buffer-protocol inputs. Their private batch codec bridge keeps synchronous PyArrow work in `SyncPool` and native async I/O on the caller loop. Pattern streams open files sequentially and yield batches on demand; closing or cancelling the outer async iterator closes its current inner batch iterator [^src-extensions-sqlspec-storage-py] [^src-storage-arrow-codecs].

Parquet batch readers default to `pre_buffer=False`; explicit `pre_buffer=True` preserves SQLSpec compatibility and permits broader prefetch. Full-Table reads materialize their return. SQLSpec writer options preserve PyArrow's unspecified compression default independently of the public storage writer's `zstd` default, forwarding PyArrow constructor/batch options and unsupported-option errors directly [^src-extensions-sqlspec-storage-py] [^src-storage-arrow-codecs].

Advanced Alchemy registers concrete instances through `StorageRegistry.register_backend` via `register_storage_backend(store, key="assets")` or `register_storage_backend("assets", store=store)`. Supplying `storage_backend` with `storage_key` applies that key and raises `ValueError` if the instance is already registered under a different key. A key alone fails closed, and `set_default=True` sets the default backend class. Upstream `StorageRegistry` handles resolution, and `FileObject` / `StoredObject` consumers use `get_content[_async]` with bounded source iterators and cleanup preserved [^src-extensions-alchemy-storage-py] [^src-extensions-alchemy-plugin-py].

## 4. `+vtb` Dialects, Types, Alembic (`sqlalchemy/dialects/`, `sqlalchemy/alembic/`, `sqlalchemy/types.py`)

| Symbol | Responsibility |
|---|---|
| `DIALECT_SCHEMES` / `ALL_DIALECT_SCHEMES` | `DIALECT_SCHEMES` registers the 8 sync schemes (`{postgresql,oracle,mysql,sqlite}+{vertebra,vtb}`); `ALL_DIALECT_SCHEMES` registers all 36 sync and async schemes across `{postgresql,oracle,mysql,sqlite,mssql,duckdb,bigquery,spanner,spangres}+{vertebra,vtb,vertebra_async,vtb_async}`. |
| Real Dialects (`sqlalchemy/dialects/`[^src-extensions-sqlalchemy-dialects-py], `_base.py`[^src-extensions-sqlalchemy-dialects-base-py], `async_adapter.py`[^src-extensions-sqlalchemy-async-adapter-py], `pool.py`[^src-extensions-sqlalchemy-pool-py]) | Subclass SQLAlchemy 2.1 base dialects (`PGDialect`, `BaseMySQLDialect`, `BaseSQLiteDialect`, `BaseOracleDialect`, `MSDialect`) and required upstream extras (`duckdb_engine.Dialect` via `vertebra-core[sqlalchemy-duckdb]`; `sqlalchemy_bigquery.BigQueryDialect` and `google.cloud.sqlalchemy_spanner.SpannerDialect` via `vertebra-core[sqlalchemy-gcp]`), raising `MissingDependencyError` via `ensure_module` when absent. `SyncSpangresDialect` / `AsyncSpangresDialect` subclass `PGDialect` with `supports_twophase = False`. `_SyncDialectMixin` / `_AsyncDialectMixin` implement `get_driver_connection`, `do_terminate`, `is_disconnect` (SQLSTATE `08*` and `57P01`–`57P03`, MySQL `_extract_error_code`, Oracle `ORA-03113`/`03114`/`01012`, MSSQL `08S01`, gRPC `UNAVAILABLE` before message text), and `_resolve_broker_pool_class` (`NullPool` for `?shm_broker=true` or BigQuery/Spanner/Spangres default, bound `BrokerPool` / `AsyncBrokerPool` for `?broker=true` via `DatabaseRegistry.default_broker()`, `StaticPool` for in-memory SQLite, `QueuePool` / `AsyncAdaptedQueuePool` otherwise). Oracle adds `_get_server_version_info` and `OracleExecutionContext_vtb` (`cursor.var()` out-binds); MSSQL adds `_get_server_version_info` and `MSExecutionContext_vtb` (`nextset()` buffering); PostgreSQL enables `BindTyping.RENDER_CASTS` and `AUTOCOMMIT`; SQLite runs transaction-free `do_ping`. |
| `SQLAlchemyAsyncDBAPI` / `AsyncAdapt_vertebra_connection` / `AsyncAdapt_vertebra_cursor` (`async_adapter.py`[^src-extensions-sqlalchemy-async-adapter-py]) | Subclasses SQLAlchemy `AsyncAdapt_dbapi_module`, `AsyncAdapt_terminate` + `AsyncAdapt_dbapi_connection`, `AsyncAdapt_dbapi_cursor`, and `AsyncAdapt_dbapi_ss_cursor`, exposing synchronous `setinputsizes`, `var()`, `nextset()`, graceful/forced termination, and `async_creator_fn` support on every async dialect. |
| `SyncDBAPIModule` / `SyncDBAPIConnection` / `SyncDBAPICursor` & `Async*` (`vertebra.db.dbapi`[^src-db-dbapi-init-py]) | PEP 249 surface behind typed `SyncSQLDriverProtocol` and `AsyncSQLDriverProtocol` (`connect`, `cursor`, `begin`, `commit`, `rollback`, `close`, `execute`, `executemany`, `fetchone`, `fetchmany`, `fetchall`, `var`, `nextset`, plus driver helpers). Validates `connect_args` against `_SUPPORTED_CONNECT_ARGS_BY_DIALECT` inside `_translate_errors()` (raising `InterfaceError` on invalid keys or URLs), accumulates rows across all parameter sets in `executemany()` on row-returning SQL, raises `ProgrammingError` on negative `fetchmany(size)`, propagates driver `type_code` descriptions, exports PEP 249 constructors and type singletons (`Date`, `Time`, `Timestamp`, `*FromTicks`, `Binary`, `STRING`, `BINARY`, `NUMBER`, `DATETIME`, `ROWID`), and sets `sqlite_version` / `sqlite_version_info` only for SQLite. |
| [`Vector`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^src-extensions-sqlalchemy-types-py], Dialect Vector Types & `_VectorDistance` | `Vector`, `PgVector`, `HalfVector`, `SparseVector`, `OracleVector`, `SpannerVector`, and `BigQueryGeography` define `comparator_factory = _VectorComparator` (`Vector.Comparator = _VectorComparator`) with `cosine_distance`, `l2_distance`, `l1_distance`, `inner_product`, `max_inner_product`, and `approx_cosine_distance` returning `_VectorDistance(ColumnElement[float])` expressions compiled via `@compiles(_VectorDistance, ...)` for `postgresql`, `spangres`, `oracle`, `mysql`, `bigquery`, and `spanner` (plus `@compiles(Vector, "spangres")`). |
| [`MsgspecJSON`](../../../../../src/py/vertebra/extensions/sqlalchemy/types.py)[^src-extensions-sqlalchemy-types-py] | `TypeDecorator` delegating via `load_dialect_impl` to `postgresql.JSONB()` (PostgreSQL, CockroachDB, Spangres), `sqlite.JSONB()` (SQLite), `mssql.NVARCHAR(None)` (`NVARCHAR(MAX)`), `oracle.JSON(use_blob=True)` (Oracle `< 21`), `oracle.JSON()` (Oracle `21+`), and `JSON()` elsewhere, encoding Python values including `str` with `encode_json` and decoding JSON/OSON payloads with `decode_json`. |
| [`alembic/impl.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/impl.py)[^src-extensions-sqlalchemy-alembic-impl-py] & [`alembic/hooks.py`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py)[^src-extensions-sqlalchemy-alembic-hooks-py] | Registers `DuckDBAlembicImpl`, `BigQueryAlembicImpl` (`transactional_ddl = False`), `SpannerAlembicImpl`, and `SpangresAlembicImpl` via `__dialect__` (while PostgreSQL, MySQL, SQLite, Oracle, and MSSQL use built-in Alembic `DefaultImpl` classes). `SpannerAlembicImpl._exec` and `SpangresAlembicImpl._exec` batch `DDLElement` statements in online mode and flush through `update_ddl` on `emit_commit()` or before non-DDL execution. `register_alembic_hooks()` registers 7-arg `compare_column_types` (`comparators.dispatch_for("column", subgroup="types", priority=DispatchPriority.FIRST)` returning `PriorityDispatchResult`), `compare_table_options`, and `render_item` for `Vector`, dialect vector types, and `MsgspecJSON`. |
| [`CreateViewOp` / `DropViewOp` / `CreateVectorIndexOp` / `DropVectorIndexOp`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic/ops.py)[^src-extensions-sqlalchemy-alembic-ops-py] | `MigrateOperation` subclasses registered via `@Operations.register_operation`, `@Operations.implementation_for`, and `@renderers.dispatch_for` (`alembic/__init__.py`[^src-extensions-sqlalchemy-alembic-py]). `OR REPLACE` is omitted on SQLite, `IF EXISTS` is omitted on Oracle `DropViewOp`, and `CreateVectorIndexOp` renders Oracle `ORGANIZATION INMEMORY NEIGHBOR GRAPH`, PostgreSQL/`spangres` `USING hnsw`, BigQuery `OPTIONS(index_type = 'IVF', ...)`, and Spanner `OPTIONS (tree_type = 'SCANN', ...)`; `reverse()` returns the inverse operation. |

Provenance sources: [^src-extensions-sqlspec-init-py], [^src-extensions-sqlspec-plugin-py], [^src-extensions-sqlspec-provider-py], [^src-extensions-alchemy-init-py], [^src-extensions-alchemy-plugin-py], [^src-pyproject-toml].

[^src-extensions-sqlspec-init-py]: `src/py/vertebra/extensions/sqlspec/__init__.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/__init__.py).
[^src-extensions-sqlspec-plugin-py]: `src/py/vertebra/extensions/sqlspec/plugin.py` — Python implementation module (src/py/vertebra/extensions/sqlspec/plugin.py).
[^src-extensions-sqlspec-driver-py]: `src/py/vertebra/extensions/sqlspec/_driver.py` — Shared SQLSpec `SyncAdapterConfig`, `AsyncAdapterConfig`, `BaseSyncDriver`, and `BaseAsyncDriver`.
[^src-extensions-sqlspec-adapters-py]: `src/py/vertebra/extensions/sqlspec/adapters/__init__.py` — Per-engine SQLSpec 0.72.1+ adapter packages (`postgres`, `sqlite`, `mysql`, `oracle`, `mssql`, `duckdb`, `bigquery`, `spanner`).
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
[^src-extensions-sqlalchemy-pool-py]: `src/py/vertebra/extensions/sqlalchemy/pool.py` — `BrokerPool`, `AsyncBrokerPool`, and `broker_creator`.
[^src-db-dbapi-init-py]: `src/py/vertebra/db/dbapi/__init__.py` — Standalone PEP 249 DB-API 2.0 interfaces and typed SQL driver protocols.
[^src-extensions-sqlalchemy-types-py]: `src/py/vertebra/extensions/sqlalchemy/types.py` — `Vector`, dialect vector types, `_VectorDistance`, and `MsgspecJSON`.
[^src-extensions-sqlalchemy-alembic-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/__init__.py` — Alembic `DefaultImpl` subclasses, custom DDL ops, and autogenerate hooks.
[^src-extensions-sqlalchemy-alembic-hooks-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/hooks.py` — Alembic 1.18+ column comparator and item rendering hooks.
[^src-extensions-sqlalchemy-alembic-impl-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/impl.py` — `DuckDBAlembicImpl`, `BigQueryAlembicImpl`, `SpannerAlembicImpl`, and `SpangresAlembicImpl`.
[^src-extensions-sqlalchemy-alembic-ops-py]: `src/py/vertebra/extensions/sqlalchemy/alembic/ops.py` — `MigrateOperation` view and vector index DDL operations.
[^src-pyproject-toml]: `pyproject.toml` — Python package metadata, dependencies, and tool configuration.

[^src-storage-arrow-codecs]: `src/py/vertebra/storage/_arrow.py` — Bounded codecs, format options, and resource ownership.

[^src-oracle-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle drivers over the standalone `vertebra._oracle` module.
