---
type: Task
id: "driver-fail-loud:ch08:t02"
chapter_id: ch08-install-hints-and-extras-gate
title: Name pip packages in PostgreSQL, MySQL, and WASM dependency guards
description: Drop the undeclared postgres, mysql, and wasm extras from engine and WASM dependency guards, name psycopg[binary,pool], pymysql, asyncmy, and wasmtime directly, and make the async PostgreSQL guard check psycopg_pool.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/wasm/__init__.py
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/extensions/test_grpc_service.py
files:
  - src/py/vertebra/db/adapters/postgres/driver.py
  - src/py/vertebra/db/adapters/mysql/driver.py
  - src/py/vertebra/wasm/__init__.py
tests:
  - src/py/tests/unit/db/test_postgres_driver.py
  - src/py/tests/unit/db/test_mysql_driver.py
  - src/py/tests/unit/extensions/test_grpc_service.py
estimated_loc: 90
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T00:00:00Z"
last_verified_commit: 616de0f2
verification_evidence: "pytest test_postgres_driver.py test_mysql_driver.py test_grpc_service.py passed"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T00:00:00Z"
commit: 616de0f2
sources:
  - id: postgres-driver
    resource: src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver
    title: PostgreSQL _ensure_pool and _ensure_async_pool dependency guards
  - id: mysql-driver
    resource: src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver
    title: MySQL pymysql and asyncmy dependency guards
  - id: wasm-host
    resource: src/py/vertebra/wasm/__init__.py::WASMPluginHost
    title: WASM plugin host wasmtime guard
tags:
  - packaging
  - install-hints
  - database
  - python
---

# Task t02: Name Pip Packages in PostgreSQL, MySQL, and WASM Dependency Guards

## Objective & Scope Boundary

Remove the undeclared `postgres`, `mysql`, and `wasm` extras from five dependency guards so each hint names the pip package that provides the imported module, and make `AsyncPostgreSQLDriver._ensure_async_pool` check `psycopg_pool`, the module its guarded thread-offload path imports.[^postgres-driver][^mysql-driver][^wasm-host]

Out of scope: the `memory://` early returns in `_ensure_pool` / `_ensure_async_pool` (deleted by ch03 `t01` before this task runs; if one is still present, stop and route through `/flow:revise`); any other line of the PostgreSQL or MySQL drivers; adding engine extras to `pyproject.toml`; removing `from __future__ import annotations` from these modules.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver._ensure_pool`, `AsyncPostgreSQLDriver._ensure_async_pool`, `AsyncPostgreSQLDriver._run_async_query` (call site only).[^postgres-driver]
- `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver._create_connection`, `AsyncMySQLDriver._ensure_pool`.[^mysql-driver]
- `src/py/vertebra/wasm/__init__.py::WASMPluginHost.register_module`.[^wasm-host]
- `src/py/tests/unit/db/test_postgres_driver.py::test_postgres_missing_dependency_raises_on_unconnected_dsn`.
- `src/py/tests/unit/db/test_mysql_driver.py::test_sync_mysql_missing_dependency_refusal`, `test_async_mysql_missing_dependency_refusal`.
- `src/py/tests/unit/extensions/test_grpc_service.py::test_wasm_plugin_host_execution_and_missing_dependency`.

## Symbol & Signature Contracts

- `SyncPostgreSQLDriver._ensure_pool(self) -> Any` — the guard call becomes `psycopg_pool = ensure_module("psycopg_pool", install_package="psycopg[binary,pool]")`; the rest of the method is unchanged.
- `AsyncPostgreSQLDriver._ensure_async_pool(self) -> Any` — body becomes:

  ```python
  if self._pool is not None:
      return self._pool
  return ensure_module("psycopg_pool", install_package="psycopg[binary,pool]")
  ```

  The `import_optional("asyncpg")` probe is deleted. Reason: `_run_async_query` discards the return value and runs `self._sync._run_sync_query` through `asyncio.to_thread`, which calls `SyncPostgreSQLDriver._ensure_pool` and imports `psycopg_pool`. `import_optional` stays imported because `pyarrow` lookups in the same module use it. Update the docstring to a Google-style summary plus `Raises: MissingDependencyError: When psycopg_pool is not installed.`
- `SyncMySQLDriver._create_connection(self) -> Any` — `pymysql: Any = ensure_module("pymysql")`.
- `AsyncMySQLDriver._ensure_pool(self) -> Any` — `asyncmy: Any = ensure_module("asyncmy")`.
- `WASMPluginHost.register_module(...)` — `raise MissingDependencyError("wasmtime")`.

## Edge Cases & Error Semantics

- `asyncpg` importable, `psycopg_pool` absent, no injected pool or executor: `AsyncPostgreSQLDriver.execute_tuples` raises `MissingDependencyError` with `package == "psycopg_pool"` and `install_package == "psycopg[binary,pool]"` from `_ensure_async_pool`, before `asyncio.to_thread(self._sync._run_sync_query, ...)` runs. Today the guard returns the `asyncpg` module and the same error surfaces later from `SyncPostgreSQLDriver._ensure_pool` inside the thread, so only a patched `_run_sync_query` distinguishes the two paths.
- `asyncpg` and `psycopg_pool` both absent: the same `psycopg_pool` error. Today `_ensure_async_pool` raises with `package == "asyncpg"` and `install_package == "asyncpg"`.
- Injected `pool=` on either PostgreSQL driver: the guard returns the pool without importing anything.
- `connect_factory=` on `SyncMySQLDriver`: `_create_connection` returns before the `pymysql` guard; unchanged.
- WASM `register_module(..., exports={...})`: returns before the `wasmtime` check; unchanged.
- No hint produced by these guards contains `vertebra-core[`, `vertebra[`, or `vtb[`.

## Public-Seam Test Blueprint

- **Canonical test files**: the three existing modules above (in-process; missing modules simulated with `monkeypatch.setitem(sys.modules, name, None)` plus `reset_dependency_cache(name)` so results do not depend on what the venv has installed; in `test_postgres_driver.py` add `import sys`, `import types`, `NoReturn` to the `typing` import, and `reset_dependency_cache` to the `vertebra.typing` import; `SyncPostgreSQLDriver` is already imported for the thread-offload patch). Rename `test_postgres_missing_dependency_raises_on_unconnected_dsn` to `test_postgres_sync_and_async_guards_name_psycopg_pool` and extend it with the async case.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Sync PostgreSQL | `sys.modules["psycopg_pool"] = None`; `SyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://unreachable-host:5432/db")).execute_tuples("SELECT 1")` | `MissingDependencyError`; `package == "psycopg_pool"`; `install_package == "psycopg[binary,pool]"`; `hint == "Install it with: uv add psycopg[binary,pool]"` |
| Async PostgreSQL with asyncpg absent | `sys.modules["asyncpg"] = None`; `sys.modules["psycopg_pool"] = None`; `await AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://unreachable-host:5432/db")).execute_tuples("SELECT 1")` | `MissingDependencyError`; `package == "psycopg_pool"`; `install_package == "psycopg[binary,pool]"` (today `package == "asyncpg"`) |
| Async PostgreSQL with asyncpg present, raised before thread offload | `sys.modules["psycopg_pool"] = None`; `sys.modules["asyncpg"] = types.ModuleType("asyncpg")`; `monkeypatch.setattr(SyncPostgreSQLDriver, "_run_sync_query", _fail_thread_offload)` where `def _fail_thread_offload(*_args: object, **_kwargs: object) -> NoReturn: raise AssertionError("thread offload reached before the psycopg_pool guard")`; `await AsyncPostgreSQLDriver(PostgreSQLConfig(dsn="postgresql://unreachable-host:5432/db")).execute_tuples("SELECT 1")` | `MissingDependencyError`; `package == "psycopg_pool"`; `install_package == "psycopg[binary,pool]"`; no `AssertionError` (today the guard returns the `asyncpg` module and the patched `_run_sync_query` raises `AssertionError`) |
| Sync MySQL | `sys.modules["pymysql"] = None`; `SyncMySQLDriver(MySQLConfig(dsn="mysql://localhost:3306/app")).execute_tuples("SELECT 1")` | `MissingDependencyError`; `package == "pymysql"`; `install_package == "pymysql"` |
| Async MySQL | `sys.modules["asyncmy"] = None`; `await AsyncMySQLDriver(MySQLConfig(dsn="mysql://localhost:3306/app")).execute_tuples("SELECT 1")` | `MissingDependencyError`; `package == "asyncmy"`; `install_package == "asyncmy"` |
| WASM | `monkeypatch.setattr(wasm_mod, "_wasmtime", None)`; `host.register_module(WASMModuleConfig(name="guest_missing", wasm_source=wasm_add_bytes))` | `MissingDependencyError`; `package == "wasmtime"`; `install_package == "wasmtime"`; `hint == "Install it with: uv add wasmtime"` |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/extensions/test_grpc_service.py -q -k "missing_dependency or psycopg_pool or wasm"` fails on the "Async PostgreSQL with asyncpg absent" row (`package == "asyncpg"`, `install_package == "asyncpg"`) and the WASM row (`install_package == "vertebra[wasm]"`); the asyncpg-present row also fails, with the patched `_run_sync_query` raising `AssertionError` after the guard returns the `asyncpg` module. Without the `_run_sync_query` patch the asyncpg-present case passes today, because `SyncPostgreSQLDriver._ensure_pool` raises the same `psycopg_pool` error inside the thread.
- Final Green + lint + typecheck: `uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/driver.py src/py/vertebra/db/adapters/mysql/driver.py src/py/vertebra/wasm/__init__.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/extensions/test_grpc_service.py && uv run --no-sync ruff format --check src/py/vertebra/db/adapters/postgres/driver.py src/py/vertebra/db/adapters/mysql/driver.py src/py/vertebra/wasm/__init__.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/extensions/test_grpc_service.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/driver.py src/py/vertebra/db/adapters/mysql/driver.py src/py/vertebra/wasm/__init__.py && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_mysql_driver.py src/py/tests/unit/extensions/test_grpc_service.py src/py/tests/unit/db/test_native_db_drivers.py -q`

## Acceptance Criteria

- [x] No `extra="postgres"`, `extra="mysql"`, or `extra="wasm"` literal remains under `src/py/vertebra` (R10).
- [x] `_ensure_async_pool` checks `psycopg_pool` and names `psycopg[binary,pool]`; the `asyncpg` probe is gone (R10).
- [x] The WASM hint names `wasmtime`, not `vertebra[wasm]` (R10).
- [x] Missing-module tests are deterministic through `sys.modules` patching.
- [x] Zero inline `#` comments in touched files.

## Notes & Discoveries

- [2026-11-07 00:00] `AsyncPostgreSQLDriver._run_async_query` is the only caller of `_ensure_async_pool`, and it ignores the return value.

[^postgres-driver]: `src/py/vertebra/db/adapters/postgres/driver.py::AsyncPostgreSQLDriver` — PostgreSQL _ensure_pool and _ensure_async_pool dependency guards.
[^mysql-driver]: `src/py/vertebra/db/adapters/mysql/driver.py::SyncMySQLDriver` — MySQL pymysql and asyncmy dependency guards.
[^wasm-host]: `src/py/vertebra/wasm/__init__.py::WASMPluginHost` — WASM plugin host wasmtime guard.
