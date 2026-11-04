---
type: Task
id: db-layout-and-contract:t06
title: Make DatabaseRegistry polymorphic, add provide_driver, and enforce the vertebra/db upward-import gate
description: "Add `url_schemes`, lazy `driver_module`/`sync_driver_name`/`async_driver_name` descriptors, `from_url`, `pool_key`, and `build_sync_driver`/`build_async_driver` hooks to `BaseDatabaseConfig` and every adapter config (plus `url_schemes`/`from_url`/`pool_key` on `CloudSQLConfig` and `AlloyDBConfig`), replace `isinstance` chains in `DatabaseRegistry` with table-driven dispatch and `@overload`ed `get_dialer(config, *, sync=False)`, replace `provide_bigquery_driver` and `provide_spanner_driver` with `provide_driver`, and add an AST upward-import gate for `src/py/vertebra/db/`."
state: in_progress
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - db-layout-and-contract:t04
  - db-layout-and-contract:t05
files:
  - src/py/vertebra/db/__init__.py
  - src/py/vertebra/db/driver/_common.py
  - src/py/vertebra/db/registry.py
  - src/py/vertebra/db/mcp_tools.py
  - src/py/vertebra/db/dialer/alloydb.py
  - src/py/vertebra/db/dialer/cloudsql.py
  - src/py/vertebra/db/adapters/bigquery/config.py
  - src/py/vertebra/db/adapters/datafusion/config.py
  - src/py/vertebra/db/adapters/duckdb/config.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mysql/config.py
  - src/py/vertebra/db/adapters/oracle/config.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/redis/config.py
  - src/py/vertebra/db/adapters/spanner/config.py
  - src/py/vertebra/db/adapters/sqlite/config.py
  - src/py/vertebra/db/adapters/valkey/config.py
  - src/py/vertebra/extensions/fastapi/__init__.py
  - src/py/vertebra/extensions/flask/__init__.py
  - src/py/vertebra/extensions/litestar/__init__.py
  - src/py/vertebra/extensions/starlette/__init__.py
  - .agents/bundles/knowledge/domains/data/broker-and-shm-leases.md
tests:
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
plan_revision: 4
plan_commit: "f741701"
state_revision: 1
claimed_by: "flow-executor"
claimed_at: "2026-11-04T18:57:00Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: 2026-11-03T21:40:00Z
updated_at: "2026-11-04T18:57:00Z"
commit: null
tags: [db, registry, polymorphism, di, import-gate]
---

## Objective

Add `url_schemes`, lazy `driver_module`/`sync_driver_name`/`async_driver_name` descriptors, `from_url`, `pool_key`, and `build_sync_driver`/`build_async_driver` hooks to `BaseDatabaseConfig` and every adapter config (plus `url_schemes`/`from_url`/`pool_key` on `CloudSQLConfig` and `AlloyDBConfig`), replace `isinstance` chains in `DatabaseRegistry` with table-driven dispatch and `@overload`ed `get_dialer(config, *, sync=False)`, replace `provide_bigquery_driver` and `provide_spanner_driver` with `provide_driver`, and add an AST upward-import gate for `src/py/vertebra/db/`.

## Context

- **Verification strategy (`behavior_tdd`)**: URL parsing across every scheme, lazy `driver.py` loading, `pool_key()` determinism vs `:memory:` isolation (`None`), `provide_driver` DI resolution across frameworks, `DatabaseRegistry.get_dialer(..., sync=False/True)` resolution, removal of `provide_bigquery_driver`/`provide_spanner_driver`, and the AST import boundary are deterministic unit-testable behaviors.
- **Architectural invariants**:
  - Preserve every active URL scheme: `SpannerConfig.url_schemes = ("spanner", "spangres", "spanner+googlesql", "spanner+spangres")`, `BigQueryConfig.url_schemes = ("bigquery", "bq")`, `RedisConfig.url_schemes = ("redis", "rediss", "memory")` (until `native-embedded-and-cache-drivers` removes `memory://`), `ValkeyConfig.url_schemes = ("valkey", "valkeys")`, `PostgreSQLConfig.url_schemes = ("postgresql", "postgres")`, `MySQLConfig.url_schemes = ("mysql", "mariadb")`, `SQLiteConfig.url_schemes = ("sqlite",)`, `OracleConfig.url_schemes = ("oracle",)`, `MSSQLConfig.url_schemes = ("mssql", "sqlserver")`, `DuckDBConfig.url_schemes = ("duckdb",)`, `DataFusionConfig.url_schemes = ("datafusion",)`, plus dialer URL configs `CloudSQLConfig.url_schemes = ("cloudsql",)` and `AlloyDBConfig.url_schemes = ("alloydb",)`.
  - Lazy driver resolution without circular imports or eager `driver.py` imports: each `<Engine>Config` in `config.py` declares `driver_module = "vertebra.db.adapters.<engine>.driver"`, `sync_driver_name = "Sync<Engine>Driver"`, and `async_driver_name = "Async<Engine>Driver"` (`"DataFusionEngine"` on `DataFusionConfig`). `BaseDatabaseConfig.resolve_driver_classes()` loads `ensure_module(self.driver_module)` lazily on demand, and `BaseDatabaseConfig.build_sync_driver(registry)` / `build_async_driver(registry)` construct `(self, broker=...)` by default while `PostgreSQLConfig`, `MySQLConfig`, and `MSSQLConfig` (adding optional `dialer: SyncCloudSQLDialer | None = None` to `SyncMSSQLDriver` and `dialer: AsyncCloudSQLDialer | None = None` to `AsyncMSSQLDriver`) override `build_sync_driver` to pass `dialer=registry.get_dialer(dialer_cfg, sync=True)` and `build_async_driver` to pass `dialer=registry.get_dialer(dialer_cfg, sync=False)` whenever `(dialer_cfg := self.to_cloud_dialer_config()) is not None`, `RedisConfig`/`ValkeyConfig` construct `(self)`, and `DataFusionConfig` constructs `DataFusionEngine(self)`.
  - `CloudSQLConfig` and `AlloyDBConfig` (`FrozenBaseStruct`, not `BaseDatabaseConfig`) declare `url_schemes`, `from_url`, and `pool_key()` (`("cloudsql", self.instance_connection_name)` and `("alloydb", self.instance_uri)`). `@overload`ed `DatabaseRegistry.get_dialer(config: CloudSQLConfig | AlloyDBConfig, *, sync: bool = False) -> SyncCloudSQLDialer | AsyncCloudSQLDialer | SyncAlloyDBDialer | AsyncAlloyDBDialer` replaces `_get_or_create_dialer` and caches dialer instances keyed by `(*config.pool_key(), sync)`; `DatabaseRegistry.get_driver` / `get_shared_driver` delegate `CloudSQLConfig | AlloyDBConfig` to `self.get_dialer(config, sync=False)` and `get_sync_driver` / `get_shared_sync_driver` delegate to `self.get_dialer(config, sync=True)`; and `provide_driver` raises `TypeError` when given a `CloudSQLConfig` / `AlloyDBConfig` or `cloudsql://` / `alloydb://` URL directly (instructing callers to use `DatabaseRegistry.get_dialer()` or attach `cloudsql_instance` / `alloydb_instance_uri` on a database config).
  - `SQLiteConfig.pool_key()` returns `None` when `self.is_in_memory and "cache=shared" not in str(self.path)` and a deterministic tuple otherwise; `DuckDBConfig.pool_key()` returns `None` when `self.resolved_path() == ":memory:"` and `("duckdb", self.resolved_path(), self.read_only)` otherwise (so `DuckDBConfig(path="/tmp/x.duckdb").pool_key()` is non-`None`). When `config.pool_key()` returns `None`, `DatabaseRegistry.get_shared_driver` and `get_shared_sync_driver` construct a fresh driver rather than caching across callers.
  - The AST upward-import test in `test_native_db_drivers.py` defines `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST: frozenset[str] = frozenset({"vertebra.db.mcp_tools"})` and asserts both (a) no other `vertebra/db/**/*.py` file imports `vertebra.{agent,framework,extensions,server,embeddings}` and (b) every allowlisted module actually imports upward today so stale allowlist entries fail immediately once `db-engine-primitives:t05` relocates `mcp_tools.py`.

## Steps

1. **Write failing registry polymorphism, `provide_driver`, and import-gate tests (RED)**:
   - In `src/py/tests/unit/db/test_native_db_drivers.py`, add parameterized `@pytest.mark.parametrize` tests covering:
     - Every scheme (`postgresql://`, `postgres://`, `mysql://`, `mariadb://`, `sqlite:///tmp.db`, `oracle://`, `mssql://`, `sqlserver://`, `duckdb:///tmp.duckdb`, `bigquery://proj/ds`, `bq://proj/ds`, `spanner://projects/p/instances/i/databases/d`, `spangres://projects/p/instances/i/databases/d`, `redis://`, `rediss://`, `memory://`, `valkey://`, `valkeys://`, `datafusion://`, `cloudsql://`, `alloydb://`) round-trips through `DatabaseRegistry.parse_url` via `config_cls.from_url` without importing any `vertebra.db.adapters.*.driver` module during `parse_url`.
     - `SQLiteConfig(path=":memory:").pool_key() is None`, `SQLiteConfig(path="file:memdb1?mode=memory&cache=shared").pool_key() is not None`, `DuckDBConfig(database=":memory:").pool_key() is None`, and `DuckDBConfig(path="/tmp/x.duckdb").pool_key() is not None`, causing `provide_driver("sqlite:///:memory:", sync=True)` to return distinct driver instances while shared-cache/file/network configs share driver instances.
     - `DatabaseRegistry.get_dialer(cfg, sync=True)` caches `SyncCloudSQLDialer` / `SyncAlloyDBDialer` and `DatabaseRegistry.get_dialer(cfg, sync=False)` caches `AsyncCloudSQLDialer` / `AsyncAlloyDBDialer` separately, wiring `to_cloud_dialer_config()` across `PostgreSQLConfig`, `MySQLConfig`, and `MSSQLConfig`, while passing `cloudsql://...` or `CloudSQLConfig` to `provide_driver` raises `TypeError`.
     - `provide_driver` resolves registered names, URLs, and `BaseDatabaseConfig` objects (including `DataFusionConfig`) in both `sync=False` and `sync=True` modes, and `hasattr(vertebra.db, "provide_bigquery_driver")` / `hasattr(vertebra.db, "provide_spanner_driver")` are `False`.
     - AST scan over `src/py/vertebra/db/**/*.py` enforces zero imports from `vertebra.{agent,framework,extensions,server,embeddings}` outside `_TEMPORARY_UPWARD_IMPORT_ALLOWLIST`.
   - Update `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` to test `provide_driver` across `vertebra.db` and `vertebra.extensions.{litestar,fastapi,starlette,flask}` and assert `provide_bigquery_driver` / `provide_spanner_driver` are absent.

2. **Implement polymorphic `BaseDatabaseConfig`, table-driven `DatabaseRegistry`, and `provide_driver` (GREEN)**:
   - Add `url_schemes`, `driver_module`, `sync_driver_name`, `async_driver_name`, `from_url`, `pool_key`, `resolve_driver_classes`, `build_sync_driver`, and `build_async_driver` on `BaseDatabaseConfig` (`src/py/vertebra/db/driver/_common.py`) and implement them on all 11 adapter configs (`PostgreSQLConfig`, `MySQLConfig`, `SQLiteConfig`, `OracleConfig`, `MSSQLConfig`, `DuckDBConfig`, `BigQueryConfig`, `SpannerConfig`, `RedisConfig`, `ValkeyConfig`, `DataFusionConfig` inheriting `BaseDatabaseConfig`), plus `url_schemes`, `from_url`, and `pool_key` on `CloudSQLConfig` and `AlloyDBConfig` (and optional `dialer` kwarg on `SyncMSSQLDriver`/`AsyncMSSQLDriver`).
   - Rewrite `DatabaseRegistry.parse_url`, `pool_fingerprint`, `get_dialer`, `_build_async_driver`, `_build_sync_driver`, `get_shared_driver`, and `get_shared_sync_driver` in `src/py/vertebra/db/registry.py` using `REGISTERED_CONFIG_TYPES` (the declarative tuple of config classes) and `SCHEME_REGISTRY` built from `url_schemes`, `config.pool_key()`, and `config.build_sync_driver(self)` / `config.build_async_driver(self)` (delegating `CloudSQLConfig` / `AlloyDBConfig` via `self.get_dialer(config, sync=...)`).
   - Replace `provide_bigquery_driver` and `provide_spanner_driver` with `provide_driver` in `src/py/vertebra/db/registry.py`, `src/py/vertebra/db/__init__.py`, `src/py/vertebra/db/mcp_tools.py`, and `src/py/vertebra/extensions/{litestar,fastapi,starlette,flask}/__init__.py`, and update `pool_key()` / `DatabaseRegistry` documentation in `.agents/bundles/knowledge/domains/data/broker-and-shm-leases.md`.

3. **Refactor & verify**:
   - Confirm zero engine `isinstance` dispatch chains remain in `src/py/vertebra/db/registry.py` and run `make lint-all` + `uv run pytest src/py/tests/unit`.

## Verification

- **Initial (RED)**:
  - `uv run pytest src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` -> fails before `url_schemes`, `pool_key`, `provide_driver`, and the AST import gate exist.
- **Final (GREEN)**:
  - `uv run pytest src/py/tests/unit` -> exits `0`.
  - `make lint-all` -> exits `0`.

## Acceptance Criteria

- [ ] R8: All adapter configs declare `url_schemes`, `driver_module`, `sync_driver_name`, `async_driver_name`, `from_url`, `pool_key`, and `build_*_driver` (with `CloudSQLConfig` / `AlloyDBConfig` declaring `url_schemes` and `from_url`); `DatabaseRegistry` uses table lookups with lazy driver module loading, separate `sync=False/True` dialer caching, and `:memory:` isolation when `pool_key()` is `None`; and `provide_driver` replaces `provide_bigquery_driver` / `provide_spanner_driver` everywhere with zero compatibility aliases.
- [ ] R9 (import gate): AST upward-import test passes with sole temporary allowlist `{"vertebra.db.mcp_tools"}`.

## Notes & Discoveries

<!-- Append timestamped implementation findings here: - [YYYY-MM-DD HH:MM] ... -->
