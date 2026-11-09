---
type: Task
id: "driver-fail-loud:ch02:t04"
chapter_id: ch02-duckdb-sqlalchemy-queue-fail-loud
title: DuckDB driver requires the duckdb package and DuckDB tests skip without it
description: Resolve duckdb through ensure_module in SyncDuckDBDriver, delete the sqlite3 fallback and every _using_native_duckdb branch, and guard DuckDB-executing tests with pytest.importorskip so free-threaded runs skip them.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-fail-loud:ch02:t01"
read_set:
  - src/py/vertebra/db/adapters/duckdb/driver.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/unit/extensions/test_django_plugin.py
files:
  - src/py/vertebra/db/adapters/duckdb/driver.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/storage/test_gcs_token_refresh.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 190
plan_revision: 2
plan_commit: fd571e3a
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
commit: null
sources:
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB driver with sqlite3 fallback
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: missing-template
    resource: src/py/tests/unit/db/test_native_db_drivers.py::test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent
    title: Missing-module test template using sys.modules and reset_dependency_cache
  - id: conformance-resolution
    resource: src/py/tests/integration/db/test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver
    title: Registry resolution test opening a duckdb file per engine id
tags:
  - database
  - duckdb
  - fail-loud
  - free-threading
---

# Task t04: DuckDB Driver Requires the `duckdb` Package

## Objective & Scope Boundary

`SyncDuckDBDriver.__init__` resolves `duckdb` with `ensure_module("duckdb", package="duckdb", install_package="duckdb")` and always opens a DuckDB connection.[^duckdb-driver][^module-loader] The module loses `import sqlite3`, `import contextlib`, the `import_optional` import, the `_using_native_duckdb` attribute, every SQLite branch, and the `contextlib.suppress(RuntimeError, ValueError, OSError)` wrappers (verified dead: `duckdb.Error` derives from `Exception` only, so they never catch a DuckDB failure).

Every test that constructs `SyncDuckDBDriver` / `AsyncDuckDBDriver` or opens a `duckdb+vtb` connection calls `pytest.importorskip("duckdb", reason=_DUCKDB_SKIP_REASON)` first, so free-threaded interpreters (where t01 installs no `duckdb`) skip those tests and GIL interpreters run them on real DuckDB. A missing-module test and a DuckDB-engine identity test land in `test_enterprise_olap_drivers.py`.

Out of scope: the dependency group and Makefile wiring (t01); `AsyncDuckDBDriver` method bodies (unchanged); `DuckDBConfig`; the `_acquire_duckdb_lease` broker logic; the GCS token cache logic in `_sync_gcs_token_secret` apart from removing its native branch; conformance native-module gating (ch09); docs (`ch11-docs-and-okf`); a `duckdb` optional-dependency extra (native DuckDB flow).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/duckdb/driver.py` — `SyncDuckDBDriver` (all methods), `AsyncDuckDBDriver.__init__`.[^duckdb-driver]
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — `test_duckdb_execute_many_reports_affected_rows`, `test_duckdb_post_init_bootstrap_and_arrow_shm_streaming`, `test_sqlspec_adapter_config_flags_and_async_driver_close`, `test_sqlspec_adapters_and_async_drivers`, `test_sqlalchemy_dbapi_broker_forwarding`, `test_enterprise_and_olap_sync_load_from_arrow_rejects_unsafe_identifiers`, `test_enterprise_and_olap_async_load_from_arrow_rejects_unsafe_identifiers`.
- `src/py/tests/unit/storage/test_gcs_token_refresh.py` — `test_duckdb_multi_session_token_generation_secret_rotation`, `test_async_duckdb_driver_token_generation_rotation`.
- `src/py/tests/unit/db/test_native_db_drivers.py` — `test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent` (template) and the `engine == "duckdb"` contract-driver branch.[^missing-template]
- `src/py/tests/integration/db/test_driver_conformance.py` — `test_engine_driver_resolution_and_provide_driver` (`duckdb` id).[^conformance-resolution]
- `src/py/tests/unit/extensions/test_django_plugin.py` — the `DuckDBDatabaseWrapper` assertions (read only; guard them only if the wrapper constructs a driver).

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/duckdb/driver.py` imports `from vertebra.utils.module_loader import ensure_module` and drops `import contextlib`, `import sqlite3`, and `import_optional`.
- `SyncDuckDBDriver.__init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None`:
  1. `duckdb_mod = ensure_module("duckdb", package="duckdb", install_package="duckdb")` runs first.
  2. Existing state (`_writer_lock`, `_in_transaction`, `applied_token_generation`, `last_gcs_secret_sql`, `_cached_gcs_snapshot`, `executed_bootstrap_statements`) is built as today.
  3. `self._conn = duckdb_mod.connect(self.config.resolved_path(), read_only=self.config.read_only)`.
  4. `for stmt in self.executed_bootstrap_statements: self._conn.execute(stmt)` with no suppression.
  5. Google docstring: summary line, `Args:` (`config`, `broker`), `Raises:` (`MissingDependencyError` when `duckdb` is not installed; `duckdb.Error` when a bootstrap statement fails).
- `begin(self) -> None`: under `_writer_lock`, `self._conn.execute("BEGIN TRANSACTION")`, then `self._in_transaction = True`.
- `commit(self) -> None` / `rollback(self) -> None`: under `_writer_lock`, `try: if not self._in_transaction: return; self._conn.commit()` (or `.rollback()`) `finally: self._in_transaction = False`.
- `cancel(self) -> None`: `if not self._closed: self._conn.interrupt()`.
- `execute(...) -> int`: keeps the `Count` row handling (`if cur.description and cur.description[0][0] == "Count"`) and the `rowcount` fallback; deletes the SQLite auto-commit line.
- `execute_many(...) -> int`: keeps only the per-parameter-set loop that sums `Count` rows; deletes the `executemany` + commit branch.
- `execute_tuples(...)`, `execute_with_description(...)`, `load_from_arrow(...)`: delete the SQLite auto-commit lines; `load_from_arrow` keeps `self._conn.executemany(sql, rows)`.
- `_sync_gcs_token_secret(self) -> None`: after setting `last_gcs_secret_sql` and `applied_token_generation`, runs `with self._writer_lock: self._conn.execute(secret_sql)` unconditionally.
- Test guard, defined once per touched test module beside its imports:

```python
_DUCKDB_SKIP_REASON = "duckdb publishes no free-threaded wheel; DuckDB tests run on GIL interpreters"
```

  Whole-DuckDB tests call `pytest.importorskip("duckdb", reason=_DUCKDB_SKIP_REASON)` as their first statement. Parametrized tests and helpers call it inside the DuckDB branch only (`if engine == "duckdb":`, the `duckdb+vtb:///:memory:` parameter, the `duckdb-sync` / `duckdb-async` identifier-rejection ids, and the `duckdb` id of `test_engine_driver_resolution_and_provide_driver`), so non-DuckDB parameters still run on free-threaded interpreters.

  Two tests in `test_enterprise_olap_drivers.py` construct DuckDB drivers alongside other engines and take these exact guards:

  - `test_sqlspec_adapter_config_flags_and_async_driver_close`: its `(DuckDBSyncConfig, DuckDBAsyncConfig, AsyncDuckDBDriver)` row becomes `pytest.param(DuckDBSyncConfig, DuckDBAsyncConfig, AsyncDuckDBDriver, marks=pytest.mark.skipif(not module_available("duckdb"), reason=_DUCKDB_SKIP_REASON))`. Reason: the row builds `AsyncDuckDBDriver()` in the test body, and a collection-time mark keeps the Postgres, MySQL, and SQLite rows running. `module_available` is `vertebra.typing::module_available` (re-exported from `vertebra._typing`); it returns `False` when `sys.modules["duckdb"] is None` and otherwise checks `importlib.util.find_spec`.
  - `test_sqlspec_adapters_and_async_drivers`: `pytest.importorskip("duckdb", reason=_DUCKDB_SKIP_REASON)` sits after `sqlite_sync.close()` and immediately before `duck_async = DuckDBAsyncConfig(path=":memory:")`, so the registry parsing and SQLite session half run on every interpreter.

## Edge Cases & Error Semantics

- `sys.modules["duckdb"] = None`: `SyncDuckDBDriver()` and `AsyncDuckDBDriver()` raise `MissingDependencyError` with `.package == "duckdb"`, `.install_package == "duckdb"`, `.hint == "Install it with: uv add duckdb"`; no connection or thread-offload state exists.
- The missing-module test needs no guard: it simulates absence and runs on every interpreter.
- `commit()` / `rollback()` with no active transaction return `None`; DuckDB `rollback` without a transaction raises `TransactionException`, so the `_in_transaction` guard is required.
- DDL through `execute` returns `0`: DuckDB reports a `Count` description with no row for `CREATE TABLE`.
- `execute_with_description` on DML returns `([(n,)], ("Count",))` from DuckDB; callers relying on `([], ())` for DML do not exist in the read set.
- A rejected bootstrap statement raises `duckdb.Error` from `__init__`.
- Offline CI risk: the bootstrap statements `SET http_keep_alive = ...` and `SET enable_http_metadata_cache = ...` (emitted by `DuckDBConfig` for every driver) and every GCS `CREATE SECRET` test (`test_duckdb_multi_session_token_generation_secret_rotation`, `test_async_duckdb_driver_token_generation_rotation`) need the DuckDB `httpfs` extension, which DuckDB downloads on first use and then caches under `~/.duckdb/extensions`. A runner with no network and no cached extension raises `duckdb.IOException` (or `duckdb.HTTPException`) from `SyncDuckDBDriver.__init__`, because the bootstrap no longer suppresses errors. CI `test-python` jobs have network access; record the outcome in Notes if an offline runner hits it.
- `test_sqlalchemy_dbapi_broker_forwarding[duckdb+vtb:///:memory:]` runs `CREATE TABLE items (id INTEGER, name TEXT)` on DuckDB through the mock broker and still returns `(1, "vtb")`.
- `test_engine_driver_resolution_and_provide_driver[duckdb]` opens two connections to one `reg.duckdb` file in one process; DuckDB 2.0.0 permits this (verified).
- Config-only DuckDB tests (`DuckDBConfig.from_url`, `pool_key`, registry parsing, Django wrapper `select_to_arrow(rows=)`) construct no driver and carry no guard.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py`. `import sys` and `MissingDependencyError, reset_dependency_cache` on the `from vertebra.typing import ...` line already exist after ch01/t01; add only `module_available` to that same line (`from vertebra.typing import ImproperConfigurationError, MissingDependencyError, VertebraError, module_available, reset_dependency_cache`). Reason: a second `from vertebra.typing import ...` line fails ruff `I001`.
- **Dependency category**: external boundary (`duckdb`), exercised for real on GIL interpreters; absence simulated with `monkeypatch.setitem(sys.modules, "duckdb", None)` plus `reset_dependency_cache("duckdb")` before and in `finally`.[^missing-template]
- **New tests**:
  - `test_duckdb_drivers_raise_missing_dependency_when_duckdb_absent(monkeypatch: pytest.MonkeyPatch) -> None`
  - `test_duckdb_driver_executes_on_duckdb_engine() -> None` (guarded)
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Sync driver, `duckdb` absent | `sys.modules["duckdb"] = None`; `SyncDuckDBDriver()` | `MissingDependencyError`; `err.package == "duckdb"`; `err.install_package == "duckdb"`; `err.hint == "Install it with: uv add duckdb"` |
| Async driver, `duckdb` absent | Same; `AsyncDuckDBDriver(DuckDBConfig(path=":memory:"))` | `MissingDependencyError` with `err.package == "duckdb"` |
| DuckDB-only SQL | `driver = SyncDuckDBDriver()`; `driver.select("SELECT list_value(1, 2, 3) AS xs")` | `[{"xs": [1, 2, 3]}]` |
| DML count | `driver.execute("CREATE TABLE tx_items (v INTEGER)")`, then `driver.execute("INSERT INTO tx_items VALUES (1), (2)")` | `0`, then `2` |
| Rollback discards | `driver.begin()`; `driver.execute("INSERT INTO tx_items VALUES (?)", [7])`; `driver.rollback()`; `driver.execute_tuples("SELECT count(*) FROM tx_items")` | `1` from the insert; `[(2,)]` after rollback |
| Commit persists | `driver.begin()`; `driver.execute("INSERT INTO tx_items VALUES (?)", [9])`; `driver.commit()`; `driver.execute_tuples("SELECT max(v) FROM tx_items")` | `[(9,)]` |
| No-transaction commit/rollback | `driver.commit()`; `driver.rollback()` with no `begin()` | Both return `None` |
| Existing batch counts on DuckDB | `test_duckdb_execute_many_reports_affected_rows` | `6`, `0`, `3` assertions pass on DuckDB |
| Existing GCS rotation on DuckDB | `test_duckdb_multi_session_token_generation_secret_rotation` | `applied_token_generation` `1` then `2`; `"mock-oauth-token-user-2" in last_gcs_secret_sql` |
| Mixed-engine SQLSpec flags | `test_sqlspec_adapter_config_flags_and_async_driver_close` with `duckdb` absent | DuckDB row SKIPPED with `_DUCKDB_SKIP_REASON`; Postgres, MySQL, SQLite rows pass |
| Mixed-engine SQLSpec sessions | `test_sqlspec_adapters_and_async_drivers` with `duckdb` absent | Registry parsing and SQLite session assertions run, then the test reports SKIPPED at the `importorskip` |
| Free-threaded simulation | Pytest run with `sys.modules["duckdb"] = None` set before collection | DuckDB tests report SKIPPED with `_DUCKDB_SKIP_REASON`; zero failures; non-DuckDB parameters pass |

## Verification

- Initial Red baseline (tests written, driver unchanged; the missing-module test fails because the driver falls back to SQLite): `uv run --group duckdb pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k "duckdb_absent or duckdb_engine" -q`
- Final Green:
  - `uv run --group duckdb pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/storage/test_gcs_token_refresh.py -q`
  - `uv run --group duckdb pytest src/py/tests/unit/db/test_native_db_drivers.py -k "duckdb or registry_parse_url_executes_zero_eager_driver_imports" -q`
  - `uv run --group duckdb pytest src/py/tests/integration/db/test_driver_conformance.py -k "test_engine_driver_resolution_and_provide_driver and duckdb" -q`
  - Free-threaded simulation (exit `0`, DuckDB tests skipped): `uv run python -c "import sys; sys.modules['duckdb'] = None; import pytest; raise SystemExit(pytest.main(['src/py/tests/unit/db/test_enterprise_olap_drivers.py', 'src/py/tests/unit/storage/test_gcs_token_refresh.py', 'src/py/tests/unit/db/test_native_db_drivers.py', 'src/py/tests/unit/extensions/test_django_plugin.py', 'src/py/tests/integration/db/test_driver_conformance.py', '-q', '-rs', '-p', 'no:cacheprovider']))"`
  - `uv run ruff check src/py/vertebra/db/adapters/duckdb/driver.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/storage/test_gcs_token_refresh.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/integration/db/test_driver_conformance.py`
  - `uv run mypy src/py/vertebra/db/adapters/duckdb/driver.py`
  - `! grep -nE "sqlite3|_using_native_duckdb|contextlib|import_optional" src/py/vertebra/db/adapters/duckdb/driver.py`

## Acceptance Criteria

- [ ] `SyncDuckDBDriver()` and `AsyncDuckDBDriver()` raise `MissingDependencyError` naming `duckdb` when it is absent (R3).
- [ ] `src/py/vertebra/db/adapters/duckdb/driver.py` has no `sqlite3`, `_using_native_duckdb`, `contextlib.suppress`, or `import_optional` (R3.1).
- [ ] DuckDB tests in `test_enterprise_olap_drivers.py`, `test_gcs_token_refresh.py`, `test_native_db_drivers.py`, and the conformance resolution `duckdb` case pass on DuckDB with `--group duckdb` and skip through `pytest.importorskip` when `duckdb` is absent (R3).
- [ ] `SyncDuckDBDriver.__init__` has a Google docstring with `Raises:`; no inline `#` comments in touched Python files.

## Notes & Discoveries

- [2026-11-07 00:00] Probed `duckdb` 2.0.0: bootstrap `SET` statements from `DuckDBConfig()` succeed; `CREATE TABLE` returns a `Count` description with no row; `INSERT`/`UPDATE` return `[(n,)]` under `Count`; `commit()` without a transaction succeeds and `rollback()` without one raises `TransactionException`.

[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver with sqlite3 fallback.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising `MissingDependencyError`.
[^missing-template]: `src/py/tests/unit/db/test_native_db_drivers.py::test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent` — Missing-module test template.
[^conformance-resolution]: `src/py/tests/integration/db/test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver` — Registry resolution test opening a duckdb file per engine id.
