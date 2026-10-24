---
type: Guide
title: "SQLSpec, Advanced Alchemy & SQLAlchemy Dialect Extensions"
description: "The `vertebra.extensions.sqlspec`, `vertebra.extensions.alchemy`, and `vertebra.extensions.sqlalchemy` packages: Tier-1 native SHM adapters (`SyncAdapterConfig`, `AsyncAdapterConfig`), Tier-2 universal SHM broker proxy (`SqlSpecProvider`, `ProxySyncDriver`, `ProxyAsyncDriver`), `+vtb` dialect descriptors, column types, Alembic hooks, and SQL builders."
status: stable
tags: [domains, extensions, sqlspec, sqlalchemy, alembic, shm, arrow]
sources:
  - resource: src/py/vertebra/extensions/sqlspec/__init__.py
  - resource: src/py/vertebra/extensions/sqlspec/plugin.py
  - resource: src/py/vertebra/extensions/sqlspec/adapters.py
  - resource: src/py/vertebra/extensions/sqlspec/proxy.py
  - resource: src/py/vertebra/extensions/sqlspec/provider.py
  - resource: src/py/vertebra/extensions/sqlspec/storage.py
  - resource: src/py/vertebra/extensions/alchemy/__init__.py
  - resource: src/py/vertebra/extensions/alchemy/plugin.py
  - resource: src/py/vertebra/extensions/alchemy/broker.py
  - resource: src/py/vertebra/extensions/alchemy/storage.py
  - resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
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
| [`SQLSpecPlugin[ConfigT]`](../../../../../src/py/vertebra/extensions/sqlspec/plugin.py) | `sqlspec/plugin.py` | Priority `SQLSPEC_PLUGIN_PRIORITY = -5800`, state key `SQLSPEC_STATE_KEY = "sqlspec"`. `install_state` builds `SQLSpecState(pools, configs, object_store)` keyed by `bind_key` and calls each config's `create_pool`. `get_session(config=None)` is overloaded for sync and async driver types. `close_pools` is appended to `on_shutdown`. With a `store`, the state carries a `SQLSpecStore`. |
| [`SyncAdapterConfig[DriverT, ConfigT]` / `AsyncAdapterConfig[DriverT, ConfigT]`](../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) | `sqlspec/adapters.py` | **Tier-1 native SHM adapters**: generic base configs and per-engine concrete subclasses (`{Postgres,Mysql,Sqlite,Oracle,Mssql,Duckdb}{Sync,Async}Config`) backed directly by `vertebra.db` drivers, `LeaseBroker`, `StatementCache`, and `ArrowCursor`. |
| [`SqlSpecProvider` / `ProxySyncDriver` / `ProxyAsyncDriver`](../../../../../src/py/vertebra/extensions/sqlspec/provider.py) | `sqlspec/provider.py`, `sqlspec/proxy.py` | **Tier-2 universal SHM broker proxy**: wraps *any* existing SQLSpec or Advanced Alchemy driver (`asyncpg`, `psycopg`, `oracledb`, `duckdb`, `adbc`, `spanner`, `bigquery`, `cockroach`, `aiosqlite`, etc.) with `LeaseBroker` slot acquisition, `StatementCache` plan lookup (`fetch_cached_plan` / `store_cached_plan`), and threshold-based zero-copy `ShmRegion` Arrow IPC transfer (`SharedArrowBuffer`, `SharedArrowTable`, `arrow_threshold_rows=1000`, `arrow_threshold_bytes=65536`). |
| `ProxySyncConfig[SyncConfigT]` / `ProxyAsyncConfig[AsyncConfigT]` | `sqlspec/plugin.py` | Wrap any inner SQLSpec config, attach a `BrokerTransport`, and return `ProxySyncDriver` / `ProxyAsyncDriver` instances from `provide_session()`. |
| `SyncDatabaseConfig` / `AsyncDatabaseConfig` | `sqlspec/plugin.py` | PEP 695 generics over `ConnectionT, PoolT, DriverT` (bounded by `SyncDriverAdapterBase` / `AsyncDriverAdapterBase` protocols). |
| `SQLSpecConfig[ConfigT]`, `DatabaseConfigProtocol` | `sqlspec/plugin.py` | Generic wrapper (`wrap`, `__getattr__` forwarding) and runtime-checkable structural protocol. |
| [`AlchemyPlugin[ConfigT]`](../../../../../src/py/vertebra/extensions/alchemy/plugin.py) | `alchemy/plugin.py` | Priority `-5600`, state key `advanced_alchemy`. `install_state` builds `SQLAlchemyState(engines, session_makers, configs, storage_backend, broker)`. With a `store`, it creates an `ObjectStoreBackend`. When a `BrokerPlugin` is attached, `provide_broker_session` / `provide_async_broker_session` acquire an SHM `ConnectionLease` around each session lifecycle. |
| `SQLAlchemySyncConfig` / `SQLAlchemyAsyncConfig` | `alchemy/plugin.py` | Generics over `EngineT, SessionT, SessionMakerT`. `provide_session` calls `session_maker_instance()`. Defaults are SQLite in-memory URLs. |

Pool defaults: `SQLSpecConfig.inject_pool_defaults`, `AlchemyPlugin.wrap_config`, and `AlchemyPlugin._inject_engine_pool_defaults` set `pool_pre_ping=True` and `pool_recycle=1800` unless the DSN is in-memory (`:memory:` or `mode=memory`) or pooling is disabled. When a broker is attached to a `+vtb` dialect (`poolclass="NullPool"`), SQLAlchemy's per-process `QueuePool` is bypassed so the cross-process `LeaseBroker` governs connection concurrency.

`vertebra.extensions.sqlspec` re-exports `BrokerPlugin` and `ChannelMessage` from the alchemy package.

## 2. Connection Broker (`alchemy/broker.py` & `broker/transport.py`)

| Symbol | Responsibility |
|---|---|
| [`BrokerConfig`](../../../../../src/py/vertebra/extensions/alchemy/broker.py) / `BrokerMetrics` / `ChannelMessage` | Config (`pool_size`, `max_overflow`, `acquire_timeout`, `lease_ttl`, state keys), counters, and an array-like pub/sub envelope. |
| `BrokerTransport` | Runtime-checkable protocol: `acquire`, `acquire_async`, `release`, `publish`, `drain_channel`, `metrics`, `close`. |
| `ConnectionLease` | TTL-bound slot lease usable as a sync or async context manager. `release` is idempotent. |
| [`ShmBrokerTransport`](../../../../../src/py/vertebra/extensions/broker/transport.py) / `InMemoryBrokerTransport` | `ShmBrokerTransport` binds to `LeaseBroker` and its co-located `StatementCache` + `ShmChannelSlot` ring in POSIX shared memory; `InMemoryBrokerTransport` provides an in-process `threading.Condition` fallback. |
| `BrokerPlugin` | Installs the broker transport and metrics snapshot under `db_broker` / `db_broker_metrics`, registers DI providers, and closes the transport in `lifespan` / `server_lifespan`. |

## 3. Object-Storage Adapters

- [`ObjectStoreBackend`](../../../../../src/py/vertebra/extensions/alchemy/storage.py) (`alchemy/storage.py`) is an Advanced Alchemy-style backend over `ObjectStore` (wrapped via `PrefixedObjectStore`) with `save_object[_async]`, `get_content[_async]`, `get_metadata`, `delete_object[_async]`, and `sign_object_url`. It fills `size`/`etag` on `FileAttachment`, applies a key prefix, and accepts buffers, `Path`, file-likes, and (async) iterables. `tracker()` and `provide_tracker(store)` return an `UploadTracker` that rolls back on exception ([object-storage](../storage/object-storage.md)).
- [`SQLSpecStore`](../../../../../src/py/vertebra/extensions/sqlspec/storage.py) (`sqlspec/storage.py`) is a prefix-scoped adapter over `PrefixedObjectStore`: bytes, views, ranges, `move` (copy then delete), `list_objects`, `glob` (fnmatch on full path or basename), `get_metadata`, JSONL export/import through the shared serializers, `presign_download`, `stream_response` → `StreamingStorageResponse` (64 KiB `memoryview` chunks, `Content-Length`/`ETag`/`Content-Disposition`), and async variants.
- `export_arrow_ipc`, `export_parquet`, and `read_parquet` require `pyarrow` (`ensure_pyarrow()`) when serializing row sequences or reading Parquet tables, while passing raw `Buffer` payloads through directly.

## 4. `+vtb` Dialects, Types, Alembic (`sqlalchemy/dialects.py`, `sqlalchemy/alembic.py`)

| Symbol | Responsibility |
|---|---|
| `DIALECT_SCHEMES` | `{postgresql,oracle,mysql,sqlite,mssql,duckdb}+{vertebra,vtb}`. |
| [`DialectDescriptor`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) | Frozen metadata struct (`name`, `driver`, `paramstyle`, statement-cache and insertmanyvalues flags, `use_null_pool_with_broker`). Module-level instances: `PostgresDialect` (`pyformat`), `OracleDialect` (`named`), `MysqlDialect` (`format`, no RETURNING, no native JSONB), `SqliteDialect` (`qmark`), `MssqlDialect` (`pyformat`, no RETURNING), `DuckdbDialect` (`qmark`). `import_dbapi()` returns `DBAPIModule`. `register_all()` calls `sqlalchemy.dialects.registry.register` for each `backend.driver` name when SQLAlchemy is importable. |
| `DBAPIModule` / `DBAPIConnection` / `DBAPICursor` | PEP 249 surface over `vertebra.db` drivers (`postgresql`, `oracle`, `mysql`, `sqlite`, `mssql`, `duckdb`), plus `execute_arrow_shm()` returning `ArrowCursor`. Non-supported URLs raise [`UnsupportedBackendError`](../../../../../src/py/vertebra/typing.py). |
| `SyncEngineAdapter` | `run_sync(fn, ...)` submits to `SyncPool.submit_async`. `requires_greenlet` is `not is_free_threaded()`. |
| [`Vector`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) / `VectorDistanceExpr` | `get_col_spec` renders Oracle `VECTOR(dim\|*, FLOAT32\|...)`, PG/MySQL `VECTOR(dim)`, otherwise `BLOB`. `cosine_distance` / `l2_distance` / `inner_product` compile to Oracle `VECTOR_DISTANCE`, pgvector `<=>`/`<->`/`<#>`, MySQL `DISTANCE_*`, or `vec_distance_*`. |
| [`BinaryJson`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) | `JSONB` on PG/SQLite, `JSON` elsewhere. Encodes with `encode_json`. |
| [`AlembicImpl`](../../../../../src/py/vertebra/extensions/sqlalchemy/alembic.py) | `register_all` aliases each `+vtb` key to the base dialect impl in `DefaultImpl._impls`. Static `render_item` and `compare_type` hooks handle `Vector`/`BinaryJson` and treat `JSON`/`JSONB`/`OSON` as equal. `resolve_migration_url` returns `PostgresConfig.effective_direct_dsn`. |
| `CreateViewOp` / `DropViewOp` / `CreateVectorIndexOp` / `DropVectorIndexOp` | DDL strings. `OR REPLACE` is dropped on SQLite and `IF EXISTS` is dropped on Oracle. `CreateVectorIndexOp` compiles `CREATE VECTOR INDEX ... ORGANIZATION INMEMORY NEIGHBOR GRAPH` on Oracle and `USING hnsw` on PostgreSQL. `reverse()` gives the inverse op. |
| [`SelectInLoadSpec`](../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py), `InsertSentinelSpec`, `DeleteUsingClause`, `ParameterizedSql` | Chunk size 1–1000 (Oracle `ORA-01795` limit), `num_batches`; sentinel-column settings; `DELETE ... USING ...` rendering with identifier validation checks on table names; conversion of PEP 750 template strings (`strings`/`interpolations`) or a static-parts sequence into `:p_N` named binds. |
