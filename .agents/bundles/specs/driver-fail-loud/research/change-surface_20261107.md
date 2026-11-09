---
type: Research
research_id: "change-surface-20261107"
title: "Codebase Audit: Fail-Loud Driver Change Surface"
description: "Symbol-level inventory of the Oracle, MSSQL, and DuckDB stand-ins, the SQLAlchemy SQLite fallback, memory:// fakes, dead vertebra-db pools and codecs, install hints, and conformance gating, with affected tests, docs, and sibling-spec overlap."
scope: architecture
status: stable
state: promoted
promoted_to: driver-fail-loud
tags: [research, database, drivers, cleanup]
generated:
  by: flow/0.28.0
  at: 2026-11-07T00:00:00Z
verified:
  - by: agent:researcher
    at: 2026-11-07T00:00:00Z
sources:
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error and install hint
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle sqlite3 stand-in driver
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: MSSQL sqlite3 stand-in driver
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB driver with sqlite3 fallback
  - id: sqla-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: SQLAlchemy DB-API connection with SQLite parse fallback
  - id: oracle-queue
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend with in-memory executor fallback
  - id: redis-config
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: Redis config accepting the memory scheme
  - id: db-lib
    resource: src/rs/vertebra-db/src/lib.rs
    title: vertebra-db re-exports
  - id: db-bindings
    resource: src/rs/vertebra-py/src/db.rs::register_db_bindings
    title: Python codec bindings
  - id: conformance
    resource: src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config
    title: Conformance live config resolution
  - id: packaging-test
    resource: src/py/tests/unit/build/test_packaging_and_pgo.py
    title: Packaging extras assertions
---

# Codebase Audit: Fail-Loud Driver Change Surface

<!-- truth: start -->
- `ensure_module(module_name, *, package=None, extra=None, install_package=None)` raises `MissingDependencyError`, an `ImportError` subclass exported from `vertebra.typing`; `reset_dependency_cache(name)` clears its cache.[^module-loader]
- `MissingDependencyError` renders `Install it with: uv add {install_package}` and defaults `install_package` to `vtb[{extra}]`; the distribution is `vertebra-core`.[^missing-dep]
- Oracle and MSSQL drivers open `sqlite3.connect(":memory:")` in `__init__`; the async drivers construct the sync driver in `__init__`.[^oracle-driver][^mssql-driver]
- Pre-remediation baseline (`d2b168dc`): DuckDB imported `duckdb` via `import_optional` with `_using_native_duckdb` branches and placed `duckdb` in `[dependency-groups].bench` rather than a dedicated GIL-only `duckdb` group.[^duckdb-driver]
- Pre-remediation baseline (`d2b168dc`): `SyncDBAPIConnection.__init__` and `AsyncDBAPIConnection.__init__` caught `DatabaseRegistry.parse_url` errors and returned `SQLiteConfig(":memory:")`.[^sqla-dbapi]
- Pre-remediation baseline (`d2b168dc`): `OracleQueueBackend._build_executor` returned `_InMemoryOracleQueueExecutor` when `oracledb` was absent.[^oracle-queue]
- `RedisConfig.url_schemes` includes `memory`, so `DatabaseRegistry.parse_url("memory://")` yields a Redis config backed by an in-process dict.[^redis-config]
- The six `*Pool` types in `vertebra-db` have no callers outside their modules, inline tests, and `lib.rs` re-exports.[^db-lib]
- `_resolve_live_config` calls `ensure_module` for the native module before BigQuery/Spanner credential skips; missing modules error or hide behind strict xfail.[^conformance]
<!-- truth: end -->

## Loader and Error Contracts

BigQuery and Spanner resolve their native module lazily inside `_ensure_client()` with `ensure_module("vertebra._bigquery", package="vertebra-core[bigquery]", extra="bigquery")`. `src/py/tests/unit/db/test_native_db_drivers.py::test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent` patches `sys.modules[name] = None` and calls `reset_dependency_cache` — the template for missing-module tests. `test_registry_parse_url_executes_zero_eager_driver_imports` requires that no driver module import occurs at registry import or URL parse time.[^module-loader]

## Oracle and MSSQL Stand-Ins

`SyncOracleDriver` methods using `self._conn`: `__init__`, `begin`, `commit`, `rollback`, `cancel`, `close`, `execute`, `execute_tuples`, `execute_with_description`, `select`, `load_from_arrow`. SQLite-only helpers: `OracleCore.normalize_sql` (`:1`/`:name` → `?`). Engine-independent pieces that live in config/core and stay: `OracleSessionAttrs.encode_ttc_piggyback`, `OracleCore` VECTOR/OSON codecs, TNS descriptor parsing.[^oracle-driver]

`SyncMSSQLDriver` has the same method set plus `dialer`. `AsyncMSSQLDriver` constructs the sync driver without forwarding `dialer`. SQLite-only helpers: `MSSQLCore.normalize_for_sqlite` and the `@P\d+` → `?` substitution in `load_from_arrow`. `MSSQLCore.rewrite_placeholders` (`@P1` rewriter) stays for `native-mssql-driver`. `_last_tds_batch` / `last_tds_batch` exist only to inspect the stand-in through `MSSQLCore.encode_tds_sql_batch`.[^mssql-driver]

Consumers: `DatabaseRegistry.get_shared_driver` / `get_shared_sync_driver` (lazy), `extensions/sqlspec/adapters.py` Oracle/MSSQL session providers (config construction stays safe), `extensions/django/db/backends/__init__.py::native_driver` (lazy).

## DuckDB and SQLAlchemy

DuckDB methods branching on `_using_native_duckdb`: `begin`, `commit`, `rollback`, `execute`, `execute_many`, `execute_tuples`, `execute_with_description`, `load_from_arrow`, `_sync_gcs_token_secret`. Retained: `_writer_lock`, bootstrap statements from `duckdb_bootstrap_statements`, GCS secret rotation (`applied_token_generation`, `last_gcs_secret_sql`, `_cached_gcs_snapshot`).[^duckdb-driver]

SQLAlchemy: the parse fallback sits in both DB-API connection constructors. The separate raw `sqlite3` connection (`_sqlite_conn`) opened for genuine SQLite URLs belongs to `sqlalchemy-and-django-integration`, which prescribes `InterfaceError` / `ProgrammingError` and no second SQLite connection.[^sqla-dbapi]

## memory:// Fakes

- `postgres/driver.py`: `_in_memory_rows` slot plus branches in sync/async drivers including `_ensure_pool` and `_ensure_async_pool`.
- `mysql/driver.py`: branches in sync and async drivers; no test uses MySQL `memory://`.
- `redis/driver.py`: about 40 branches including `_execute_memory_command` and `_eval_memory_script`; `valkey/driver.py` subclasses Redis drivers.
- Public reach: `RedisConfig.url_schemes`; the `from_valkey` / `from_redis` factories on `extensions/litestar/store.py::MemoryStore`, `extensions/starlette/store.py::CacheStore`, and `framework/stores.py::KeyValueStore`; `extensions/litestar/store.py` URL strings.[^redis-config]
- Tests: `unit/db/test_redis_valkey_driver.py` (about 11 constructions plus a parametrize), `unit/extensions/test_alchemy_cache_backend.py` (4), `unit/db/test_native_db_drivers.py` (Valkey driver, `MemoryStore.from_valkey`, registry `("memory://", RedisConfig)`), `unit/db/test_redis_valkey_resp3.py`, `unit/framework/test_framework_subdomains.py`, `unit/extensions/test_starlette_plugin.py`, `unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` (sets `_in_memory_rows`), conformance `_resolve_cache_config`, `test_cache_config_url_pool_key_and_provide_driver`, `test_sync_cache_memory_roundtrip_and_pipeline`, `test_async_cache_memory_roundtrip_and_pipeline`.
- Unaffected real backends: queue `MemoryQueueBackend` (`QueueConfig.backend_url="memory://"`), Advanced Alchemy local cache in `extensions/alchemy/cache.py`, object storage `memory://`, SQLite dialect `memory://` StaticPool alias (`get_pool_class`), SQLite `:memory:`.

## vertebra-db Dead Code

Delete: `PostgresPool`, `PostgresPoolConfig`, `MysqlPool`, `MysqlPoolConfig`, `MssqlPool`, `OraclePool`, `SqlitePool` (with `encode_msgpack_rows` / `decode_msgpack_rows`), `DuckdbBrokerPool`, their inline tests, and `lib.rs` re-exports; the whole `mysql` module; `mssql::{encode_tds_packet, decode_tds_packet, TdsPacketType, MSSQL_DIALECT_TAG}`; `oracle::{encode_tns_connect_packet, encode_tns_data_packet, decode_tns_packet, TnsPacketType, ORACLE_DIALECT_TAG}` and `tns::OracleWalletMaterial` (whole struct, including `from_directory`; `parse_tnsnames_ora` and `resolve_tns_alias` stay); unused `*_DIALECT_TAG` constants; Cargo dependencies `sha2` (mysql only) and `rmp-serde` (SqlitePool only). Delete the `encode_tds_sql_batch` binding and stub. Dead Python branches probing unexported natives: `oracle/core.py` (`encode_tns_connect_packet`, `encode_tns_data_packet`), `mssql/core.py` (`encode_tds_packet`), `redis/core.py` (`redis_hash_slot_native`).[^db-lib][^db-bindings]

Keep: TNS/wallet parsing (`parse_tnsnames_ora`, `resolve_tns_alias`), Oracle VECTOR and pgvector codecs, OSON, `encode_ttc_session_attrs`, `rewrite_mssql_placeholders`, `sqlite_initialization_pragmas`, `duckdb_bootstrap_statements`, all RESP3 bindings (the Python Redis driver uses them until `native-redis-driver`), `vertebra_core` statement-cache types.

`mypy.stubtest vertebra._vertebra` runs in `make py-test`, `make lint`, and `make lint-all`; a removed `#[pyfunction]` must lose its `_vertebra.pyi` definition. Review areas: `runtime-ipc` owns `src/rs/vertebra-db/**`, `pyo3-bridge` owns `src/rs/vertebra-py/**` and `_vertebra.pyi`.

## Install Hints and Extras

Undeclared extras referenced in code: `postgres` (`postgres/driver.py::_ensure_pool`, `_ensure_async_pool`, which also mismatches module `psycopg_pool` against package `asyncpg`), `mysql` (`pymysql`, `asyncmy`), `wasm` (`wasm/__init__.py` uses `vertebra[wasm]`), `numpy`, `pandas` (`utils/module_loader.py::ensure_numpy`, `ensure_pandas`). `extensions/litestar/queues/config.py` and `extensions/litestar/saq/plugin.py` pass sentences as `package=`. `test_packaging_and_pgo.py` asserts native wheel extras and is the home for an extras-declared gate. Per-engine extras (`postgres`, `mysql`, `redis`, `valkey`, `oracle`, `mssql`, `sqlite`, `duckdb`) are owned by the native driver specs.[^packaging-test][^missing-dep]

## Conformance

`EngineConformanceSpec` carries `native_extension_module`, `pending_flow_id`, `env_dsn_vars`, `container_fixture`. `_build_live_engine_params` wraps pending specs in `pytest.mark.xfail(strict=True)`. `test_engine_driver_resolution_and_provide_driver` is not xfail-wrapped and resolves Oracle, MSSQL, and DuckDB drivers. `make py-test` runs `src/py/tests` including integration. `src/py/tests/integration/conftest.py` uses the banned `from __future__ import annotations`.[^conformance]

## Documentation

`README.md` (driver list), `docs/usage/databases.md` (engine table), `docs/reference/db.md`, `docs/reference/index.md`, `docs/usage/extensions.md` ("all 8 engines"), `docs/usage/analytics-and-cdc.md` (Oracle stand-in narrative). OKF: `knowledge/domains/data/database-drivers-and-dialers.md` (adapter table, vector/DuckDB truth bullet, conformance section), `knowledge/domains/data/query-pipeline-cdc-and-tenancy.md` (SQLite stand-in), `product/product.md` (vertebra-db component row). Crate docs: `vertebra-db/src/lib.rs`, module `//!` docs in `mysql`, `mssql`, `oracle`, `sqlite`, `duckdb`, `resp3`, `vertebra-db/Cargo.toml` description. Python docstrings claiming native protocols: `oracle/{__init__,core,driver,config}.py`, `mssql/{__init__,core,driver,config}.py`, `duckdb/__init__.py`.

## Sibling Spec Overlap

- `native-postgres-driver` ch08:t02 owns the `postgres` extra; ch14 and ch27 assume `driver-fail-loud` has already removed `vertebra_db::postgres::PostgresPool` and the PostgreSQL `memory://` paths (`_in_memory_rows`); ch30 drops `memory://` doc claims.
- `native-mysql-driver` and `native-redis-driver` remove `memory://`; `native-redis-driver` describes a hand-built RESP3 client while the PRD selects `redis` 1.8.1.
- `native-oracle-driver`, `native-mssql-driver`, `native-duckdb-driver` remove the stand-ins / fallback at cutover.
- `broker-pool-sharing` t01 deletes the unused `*Pool` structs.
- `sqlalchemy-and-django-integration` owns DB-API error mapping and the second SQLite connection.

[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader.
[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error and install hint.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle sqlite3 stand-in driver.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — MSSQL sqlite3 stand-in driver.
[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver with sqlite3 fallback.
[^sqla-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — SQLAlchemy DB-API connection with SQLite parse fallback.
[^oracle-queue]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend with in-memory executor fallback.
[^redis-config]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — Redis config accepting the memory scheme.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db re-exports.
[^db-bindings]: `src/rs/vertebra-py/src/db.rs::register_db_bindings` — Python codec bindings.
[^conformance]: `src/py/tests/integration/db/test_driver_conformance.py::_resolve_live_config` — Conformance live config resolution.
[^packaging-test]: `src/py/tests/unit/build/test_packaging_and_pgo.py` — Packaging extras assertions.
