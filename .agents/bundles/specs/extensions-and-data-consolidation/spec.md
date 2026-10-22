---
flow_id: "extensions-and-data-consolidation"
title: "Extensions and Data Consolidation"
state: closed
plan_revision: 2
plan_commit: ""
created_at: "2026-04-20T04:00:00Z"
updated_at: "2026-04-20T04:00:00Z"
parent_spec: "vertebra-design-quality"
chapter_index: 6
depends_on:
  - "framework-consolidation"
findings:
  - "FW-01"
  - "FW-02"
  - "FW-03"
  - "FW-04"
  - "FW-05"
  - "FW-06"
  - "FW-09"
  - "FW-10"
  - "FW-11"
  - "FW-12"
  - "FW-20"
  - "FW-21"
  - "FW-22"
  - "FW-23"
  - "FW-24"
  - "FW-39"
  - "FW-40"
  - "FW-42"
  - "FW-43"
  - "FW-44"
  - "FW-45"
  - "FW-46"
  - "FW-47"
  - "FW-48"
  - "FW-51"
  - "FW-52"
  - "FW-54"
  - "FW-55"
  - "FW-56"
  - "FW-57"
  - "FW-58"
  - "FW-59"
  - "FW-60"
  - "FW-61"
tasks:
  - id: "6.1"
    title: "SQLAlchemy, Advanced Alchemy, and ShmSessionBroker"
    state: "closed"
    strategy: "regression_tdd"
    files:
      - "src/py/vertebra/extensions/alchemy/plugin.py"
      - "src/py/vertebra/extensions/alchemy/broker.py"
      - "src/py/vertebra/extensions/alchemy/storage.py"
      - "src/py/vertebra/extensions/sqlalchemy/__init__.py"
      - "src/py/vertebra/extensions/sqlalchemy/dialects.py"
      - "src/py/vertebra/extensions/sqlalchemy/types.py"
      - "src/py/vertebra/extensions/sqlalchemy/optimizations.py"
      - "src/py/vertebra/extensions/sqlalchemy/alembic.py"
      - "src/py/tests/unit/extensions/test_drop_in_names_and_generics.py"
      - "src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py"
    depends_on: []
  - id: "6.2"
    title: "SQLSpec extension"
    state: "closed"
    strategy: "characterization"
    files:
      - "src/py/vertebra/extensions/sqlspec/__init__.py"
      - "src/py/vertebra/extensions/sqlspec/plugin.py"
      - "src/py/vertebra/extensions/sqlspec/storage.py"
      - "src/py/vertebra/extensions/sqlspec/oracle.py"
      - "src/py/vertebra/extensions/litestar/plugin.py"
      - "src/py/vertebra/extensions/litestar/fastpath.py"
      - "src/py/tests/unit/extensions/test_data_storage_integrations.py"
    depends_on:
      - "6.1"
  - id: "6.3"
    title: "Storage consolidation"
    state: "closed"
    strategy: "regression_tdd"
    files:
      - "src/py/vertebra/storage/__init__.py"
      - "src/py/vertebra/storage/_store.py"
      - "src/py/vertebra/storage/arrow.py"
      - "src/py/vertebra/storage/pipeline.py"
      - "src/py/vertebra/storage/registry.py"
      - "src/py/tests/unit/storage/test_multicloud_storage.py"
      - "src/py/tests/unit/storage/test_storage_store.py"
    depends_on: []
  - id: "6.4"
    title: "PostgreSQL, MySQL, Redis, Valkey, and SQLite drivers"
    state: "closed"
    strategy: "behavior_tdd"
    files:
      - "src/py/vertebra/db/_registry.py"
      - "src/py/vertebra/db/postgres.py"
      - "src/py/vertebra/db/mysql.py"
      - "src/py/vertebra/db/redis.py"
      - "src/py/vertebra/db/valkey.py"
      - "src/py/vertebra/db/sqlite.py"
      - "src/py/tests/unit/db/test_native_db_drivers.py"
    depends_on: []
  - id: "6.5"
    title: "Wasm plugin host"
    state: "closed"
    strategy: "behavior_tdd"
    files:
      - "pyproject.toml"
      - "src/py/vertebra/wasm/__init__.py"
      - "src/py/tests/unit/extensions/test_grpc_service.py"
    depends_on: []
  - id: "6.6"
    title: "Oracle, APEX/ORDS, DataFusion, CDC, pipeline, and RLS"
    state: "closed"
    strategy: "regression_tdd"
    files:
      - "src/py/vertebra/db/oracle/__init__.py"
      - "src/py/vertebra/db/oracle/config.py"
      - "src/py/vertebra/db/oracle/driver.py"
      - "src/py/vertebra/db/oracle/wallet.py"
      - "src/py/vertebra/db/oracle/containers.py"
      - "src/py/vertebra/db/dialer/cloudsql.py"
      - "src/py/vertebra/db/dialer/alloydb.py"
      - "src/py/vertebra/db/datafusion.py"
      - "src/py/vertebra/db/cdc.py"
      - "src/py/vertebra/db/pipeline.py"
      - "src/py/vertebra/db/rls.py"
      - "src/py/vertebra/extensions/apex/__init__.py"
      - "src/py/vertebra/extensions/apex/session.py"
      - "src/py/vertebra/extensions/apex/gateway.py"
      - "src/py/vertebra/extensions/apex/apexlang.py"
      - "src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py"
      - "src/py/tests/unit/extensions/test_oracle_apex_and_gateway.py"
    depends_on:
      - "6.4"
  - id: "6.7"
    title: "gRPC extension consolidation"
    state: "closed"
    strategy: "regression_tdd"
    files:
      - "src/py/vertebra/extensions/grpc/__init__.py"
      - "src/py/vertebra/extensions/grpc/_decorator.py"
      - "src/py/tests/unit/extensions/test_grpc_service.py"
    depends_on:
      - "6.5"
---

# Chapter 6 — Extensions and Data Consolidation

## Problem & Goal

`vertebra.extensions`, `vertebra.db`, `vertebra.storage`, and `vertebra.wasm` suffer from four structural defects: (1) silent no-op I/O methods in database/cache drivers (`PostgresDriver`, `MysqlDriver`, `RedisDriver`, `ValkeyDriver`, `OracleDriver`, `DataFusionEngine`) and `WasmPluginHost` that return fake empty results instead of executing or raising `UnsupportedBackendError` / `MissingDependencyError`, (2) duplicated config, storage-registry, and Oracle-pool abstractions across `alchemy`, `sqlspec`, `storage`, and `db/oracle`, (3) concurrency and security bugs in `ShmSessionBroker` (`release_async` lock race and `O(N)` waiter removal), `UploadTracker` (`pop(0)` and swallowed rollback errors), `ApexSessionBridge` (ignored `exp`), and `ApexSessionContext` / `RelOverlay` (unvalidated SQL identifiers), and (4) fragmented submodule layouts in `extensions/sqlalchemy`, `extensions/grpc`, and `storage`. This chapter resolves all 32 assigned findings and aligns every data, storage, Wasm, and gRPC module with Decisions D1 and D3.

## Scope

### In scope
- `vertebra.extensions.alchemy` and `vertebra.extensions.sqlalchemy`: deduplicating `SyncSessionConfig`/`AsyncSessionConfig`, fixing `ShmSessionBroker` lock safety and `deque` waiter queue, compiling real SQLAlchemy `ClauseElement` plans in `StatementCache`, executing real SQLAlchemy queries in `DatabaseService`, delegating `StoredObject` to `ObjectStore`, consolidating `types.py` and `optimizations.py`, and retaining `dialects.py` and `alembic.py` as clean submodules re-exported by `src/py/vertebra/extensions/sqlalchemy/__init__.py`.
- `vertebra.extensions.sqlspec` and `vertebra.extensions.litestar`: deduplicating `SQLSpecSyncConfig`/`SQLSpecAsyncConfig`, removing `SyncDatabaseConfig`/`AsyncDatabaseConfig` duplicate bodies and `wrap`/`wrap_config` wrappers, offloading sync `_close_target` calls to `SyncPool`, removing duplicate `SqlSpecStorageBridge` and `SqlSpecOraclePoolRegistry`, and wiring `LitestarFastPathBypass` in `LitestarPlugin`.
- `vertebra.storage`: removing `SyncStore`, `AsyncStore`, and `StorageRegistry.register_store`, merging `ReadableFile` into `_store.py` (deleting `storage/arrow.py`), extracting native metadata conversion helpers on `ObjectStore`, raising `NotImplementedError` for GCS in `verify_presigned_url`, and fixing `UploadTracker` (`collections.deque`, logged cleanup warnings, `after_flush` hook).
- `vertebra.db`: enforcing Decision D1 (`UnsupportedBackendError` on unconnected non-`memory://` driver execution across `postgres`, `mysql`, `redis`, `valkey`, `oracle`, `datafusion`, and cloud dialers), sharing RESP3 logic between `redis.py` and `valkey.py`, offloading `AsyncSqliteDriver` to `SyncPool`, and adding `DatabaseRegistry.get_sync_driver`.
- `vertebra.wasm`: adding `vertebra[wasm]` (`wasmtime>=27.0`), executing real Wasm binaries via `wasmtime` with fuel and memory limits when `wasmtime` is installed, and raising `MissingDependencyError` when `wasmtime` is absent (Decision D3).
- `vertebra.db.{oracle,dialer,cdc,pipeline,rls}` and `vertebra.extensions.apex`: enforcing `ApexSessionBridge` token expiration, validating SQL identifiers in `ApexSessionContext`, `RelOverlay`, `CteStep`, and `TenantPredicate`, deleting `ApexLangCompiler` (`extensions/apex/apexlang.py`), bounding `ChangeFeed._history`, and fixing `CloudSqlConfig.refresh_lead_seconds` and `CloudSqlDialer.build_tls_context`.
- `vertebra.extensions.grpc`: merging `_decorator.py` into `extensions/grpc/__init__.py`, injecting `GrpcContext` into 2-parameter handlers, rejecting trailing partial LPM header bytes, and preserving declaration-order field numbering in `ProtobufSchemaGenerator`.

### Out of scope
- Framework core, `LitestarMCP`/`LitestarA2A` wiring, and `OrdsOpenApiBridge` MCP/A2A tool projection (completed in Chapter 5).
- Removal of `[tool.ruff.lint.per-file-ignores]` from `pyproject.toml` (completed in Chapter 7 Task `7.9` after this chapter's code fixes land).

## Target Design

1. **Unified Alchemy & SQLSpec configs and thread-safe `ShmSessionBroker` (`6.1`, `6.2`)**: `_BaseSessionConfig` and `_BaseSQLSpecConfig` eliminate duplicated config fields and validation. `ShmSessionBroker` guards both sync and async release paths with `self._lock` and uses `collections.deque` for `_waiters`. `StoredObject` and `SqlSpecStorageBridge` delegate directly to `ObjectStore` and `StorageRegistry`, and `extensions/sqlspec/oracle.py` delegates pool management to `vertebra.db.oracle`.
2. **Consolidated `ObjectStore` and transactional `UploadTracker` (`6.3`)**: `ObjectStore` is the single storage facade (no `SyncStore`/`AsyncStore` wrappers), includes `ReadableFile` and `open_readable_file()`, shares `_native_to_put_result`/`_native_to_object_meta`, and raises `NotImplementedError` when `verify_presigned_url` is called with `provider="gcs"`. `UploadTracker` uses `collections.deque` and logs cleanup failures instead of suppressing them.
3. **Explicit driver execution contracts (`6.4`, `6.6`)**: `PostgresDriver`, `MysqlDriver`, `RedisDriver`, `ValkeyDriver`, `OracleDriver`, `DataFusionEngine`, `CloudSqlDialer`, and `AlloyDbDialer` retain pure wire/config helpers and `memory://` test modes, and raise `UnsupportedBackendError` when network/SQL execution methods are called without an underlying driver connection. `AsyncSqliteDriver` dispatches `sqlite3` operations on `SyncPool.default()`.
4. **Real `wasmtime` sandbox & hardened SQL/ORDS boundaries (`6.5`, `6.6`, `6.7`)**: `WasmPluginHost` executes `.wasm` modules through `wasmtime` (`vertebra[wasm]`) with fuel and memory limits. `ApexSessionBridge` enforces `exp`, `ApexSessionContext`/`RelOverlay`/`CteStep`/`TenantPredicate` validate SQL identifiers, `extensions/apex/apexlang.py` is deleted, and `extensions/grpc/__init__.py` passes `GrpcContext` to handlers and rejects truncated LPM headers.

## Task Index

| Task | Title | Strategy | Depends On | Findings |
|---|---|---|---|---|
| `6.1` | SQLAlchemy, Advanced Alchemy, and ShmSessionBroker | `regression_tdd` | — | `FW-05`, `FW-06`, `FW-20`, `FW-23`, `FW-39`, `FW-42`, `FW-43`, `FW-44`, `FW-48` |
| `6.2` | SQLSpec extension | `characterization` | `6.1` | `FW-20`, `FW-21`, `FW-22`, `FW-24`, `FW-40`, `FW-45`, `FW-46`, `FW-48` |
| `6.3` | Storage consolidation | `regression_tdd` | — | `FW-21`, `FW-48`, `FW-51`, `FW-52`, `FW-60`, `FW-61` |
| `6.4` | PostgreSQL, MySQL, Redis, Valkey, and SQLite drivers | `behavior_tdd` | — | `FW-01`, `FW-02`, `FW-55`, `FW-57`, `FW-58` |
| `6.5` | Wasm plugin host | `behavior_tdd` | — | `FW-09` |
| `6.6` | Oracle, APEX/ORDS, DataFusion, CDC, pipeline, and RLS | `regression_tdd` | `6.4` | `FW-03`, `FW-04`, `FW-10`, `FW-11`, `FW-12`, `FW-48`, `FW-54`, `FW-56`, `FW-59` |
| `6.7` | gRPC extension consolidation | `regression_tdd` | `6.5` | `FW-47` |

## Finding-to-task trace

| Finding | Task(s) | Resolution Summary |
|---|---|---|
| `FW-01` | `6.4` | Raise `UnsupportedBackendError` in `PostgresDriver.execute_tuples` and `MysqlDriver.execute_tuples` unless `dsn` uses `memory://` or an active DB connection is bound |
| `FW-02` | `6.4` | Share RESP3 logic between `RedisDriver` and `ValkeyDriver`; allow in-memory KV only for `memory://` URLs and raise `UnsupportedBackendError` for unconnected remote URLs |
| `FW-03` | `6.6` | Add `execute` on `SyncOracleDriver`/`AsyncOracleDriver` that delegates to `oracledb` or raises `UnsupportedBackendError` |
| `FW-04` | `6.6` | Load client cert/key PEMs in `CloudSqlDialer.build_tls_context` and add `dial()` raising `UnsupportedBackendError` when no live cloud endpoint is connected |
| `FW-05` | `6.1` | Compile SQLAlchemy `ClauseElement` objects in `StatementCache.compile_and_encode` to extract SQL and bind parameter order |
| `FW-06` | `6.1` | Build real `sqlalchemy.select` and `sqlalchemy.func.count` queries in `DatabaseService` when backed by a SQLAlchemy session |
| `FW-09` | `6.5` | Add `vertebra[wasm]` (`wasmtime>=27.0`), execute `.wasm` modules via `wasmtime` with fuel/memory limits, and raise `MissingDependencyError` when `wasmtime` is absent |
| `FW-10` | `6.6` | Add `DataFusionEngine.execute_sql` delegating to `datafusion.SessionContext` or raising `UnsupportedBackendError` |
| `FW-11` | `6.6` | Enforce `exp` in `ApexSessionBridge.verify_session_cookie` and validate/parameterize `ApexSessionContext` identifiers in `initialize_apps_context_sql` |
| `FW-12` | `6.6` | Delete `src/py/vertebra/extensions/apex/apexlang.py` (`ApexLangCompiler`, `ApexLangDocument`) and remove exports from `extensions/apex/__init__.py` |
| `FW-20` | `6.1`, `6.2` | Delegate `StoredObject.verify_presigned_url` to `ObjectStore.verify_presigned_url` (`6.1`) and delegate `SqlSpecStorageBridge` to `StorageRegistry` (`6.2`) |
| `FW-21` | `6.2`, `6.3` | Delete `SQLSpecConfig.wrap` and `SQLSpecPlugin.wrap_config` (`6.2`), and delete `SyncStore`, `AsyncStore`, and `StorageRegistry.register_store` (`6.3`) |
| `FW-22` | `6.2` | Remove `derive_isolated_oracle_config` and `SqlSpecOraclePoolRegistry` from `extensions/sqlspec/oracle.py`, delegating to `vertebra.db.oracle` |
| `FW-23` | `6.1` | Extract `_BaseSessionConfig` shared by `SyncSessionConfig`/`AsyncSessionConfig` in `extensions/alchemy/plugin.py` |
| `FW-24` | `6.2` | Extract `_BaseSQLSpecConfig` and alias `SyncDatabaseConfig`/`AsyncDatabaseConfig` in `extensions/sqlspec/plugin.py` |
| `FW-39` | `6.1` | Call `ObjectStore` methods (`put`, `get`, `delete`, `put_async`, `get_async`, `delete_async`) directly in `StoredObject` |
| `FW-40` | `6.2` | Remove unused `Route`/`Router` imports in `extensions/litestar/plugin.py` and wire `LitestarFastPathBypass` in `LitestarPlugin.on_app_init` |
| `FW-42` | `6.1` | Use `collections.deque` for `ShmSessionBroker._waiters` and hold `self._lock` in `release_async()` |
| `FW-43` | `6.1` | Support single-query window count (`func.count().over()`) in `DatabaseService.list_and_count` when backed by SQLAlchemy |
| `FW-44` | `6.1` | Consolidate `types.py`/`optimizations.py` and keep `dialects.py`/`alembic.py` as clean submodules re-exported by `extensions/sqlalchemy/__init__.py` |
| `FW-45` | `6.2` | Dispatch synchronous `close()`/`dispose()` calls in `SQLSpecPlugin._close_target` via `SyncPool.default().submit_async` |
| `FW-46` | `6.2` | Delegate `OracleVectorConverter.to_oracle_vector` to `SyncOracleDriver.encode_vector` and retain `extensions/sqlspec/oracle.py` as a clean submodule re-exported by `sqlspec/__init__.py` |
| `FW-47` | `6.7` | Merge `extensions/grpc/_decorator.py` into `extensions/grpc/__init__.py`, inject `GrpcContext`, reject trailing LPM bytes, and preserve declaration field order in `ProtobufSchemaGenerator` |
| `FW-48` | `6.1`, `6.2`, `6.3`, `6.6` | Fix `RUF022`, `BLE001`, `S110`, `S105`, `S310`, and `B905` violations across `extensions/`, `storage/`, and `db/` |
| `FW-51` | `6.3` | Delete `SyncStore`/`AsyncStore` and extract `_native_to_put_result` and `_native_to_object_meta` on `ObjectStore` |
| `FW-52` | `6.3` | Use `collections.deque` in `UploadTracker`, log delete errors in `after_commit`/`after_rollback`, and register `after_flush` on SQLAlchemy sessions |
| `FW-54` | `6.6` | Rename `CloudSqlConfig.refresh_Lead_seconds` to `refresh_lead_seconds` in `src/py/vertebra/db/dialer/cloudsql.py` |
| `FW-55` | `6.4` | Remove duplicate `reserved_super_connections` parameter from `PostgresConfig.compute_worker_pool_budget` |
| `FW-56` | `6.6` | Remove duplicate `dtype` from `SyncOracleDriver.encode_vector`, `cache_root` from `OracleWallet.extract`, and default `admin_password` from `OracleContainer` |
| `FW-57` | `6.4` | Share RESP3 driver implementation between `SyncRedisDriver` and `SyncValkeyDriver` and remove duplicate `auth_username`/`auth_password` parameters |
| `FW-58` | `6.4` | Dispatch `AsyncSqliteDriver.execute_tuples` and `close` on `SyncPool.default().submit_async` |
| `FW-59` | `6.6` | Cap `ChangeFeed._history` with `deque(maxlen=...)`, raise `KeyError` on missing spilled CDC payload, validate SQL identifiers in `RelOverlay`/`CteStep`/`TenantPredicate`, and reject colliding `QueryPipeline` params |
| `FW-60` | `6.3` | Raise `NotImplementedError` in `ObjectStore.verify_presigned_url` when `provider == "gcs"` |
| `FW-61` | `6.3` | Merge `ReadableFile` into `src/py/vertebra/storage/_store.py`, add `ObjectStore.open_readable_file`, and delete `src/py/vertebra/storage/arrow.py` |

## Verification

```bash
uv run pytest src/py/tests/unit/extensions/ src/py/tests/unit/storage/ src/py/tests/unit/db/ -q
make lint
```
