---
type: Task
id: "driver-fail-loud:ch01:t02"
chapter_id: ch01-oracle-mssql-fail-loud
title: Guard MSSQL drivers on vertebra._mssql, delete the SQLite stand-in, and forward the sync dialer
description: SyncMSSQLDriver and AsyncMSSQLDriver resolve vertebra._mssql in __init__ and every public method, raise MissingDependencyError when it is absent, carry no sqlite3 connection, normalize_for_sqlite helper, or stand-in inspection state, and AsyncMSSQLDriver forwards a SyncCloudSQLDialer to its sync driver.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "driver-fail-loud:ch01:t01"
read_set:
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/integration/db/test_driver_conformance.py
files:
  - src/py/vertebra/db/adapters/mssql/driver.py
  - src/py/vertebra/db/adapters/mssql/core.py
  - src/py/vertebra/db/adapters/mssql/config.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 220
plan_revision: 1
plan_commit: 1e1f028a
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
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: MSSQL sync and async drivers backed by sqlite3
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore.normalize_for_sqlite
    title: SQLite-only MSSQL placeholder normalizer
  - id: mssql-config
    resource: src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.build_async_driver
    title: Async driver construction with a Cloud SQL dialer
  - id: offload-base
    resource: src/py/vertebra/db/driver/_async.py::ThreadOffloadAsyncDatabaseDriver
    title: Async driver delegating to a paired sync driver on a worker thread
  - id: registry-dialer-test
    resource: src/py/tests/unit/db/test_native_db_drivers.py::test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error
    title: Registry Cloud SQL dialer wiring test
tags:
  - database
  - mssql
  - fail-loud
  - dialer
  - tdd
---

# Task t02: Guard MSSQL Drivers on `vertebra._mssql`, Delete the SQLite Stand-In, and Forward the Sync Dialer

## Objective & Scope Boundary

Make `SyncMSSQLDriver` and `AsyncMSSQLDriver` raise `MissingDependencyError` on construction when `vertebra._mssql` is absent and guard every public method they define with `_require_mssql_native()`, mirroring t01[^mssql-driver]. Delete the SQLite stand-in, the `@P\d+` → `?` substitution, `MSSQLCore.normalize_for_sqlite`[^mssql-core], and the stand-in inspection state (`_prepare_and_lease`, `last_rewritten_sql`, `last_tds_batch`). Make `AsyncMSSQLDriver` accept a `SyncCloudSQLDialer`, forward it to its `SyncMSSQLDriver`, and expose it through `self._sync.dialer`; `MSSQLConfig.build_async_driver` requests the sync dialer, because `ThreadOffloadAsyncDatabaseDriver` performs I/O in the sync driver on a worker thread[^offload-base][^mssql-config]. Rewrite the driver module and class docstrings to state the native module requirement.

Out of scope: `MSSQLCore.encode_tds_sql_batch`, `MSSQLConfig.encode_sql_batch`, the `mssql/__init__.py` lazy export, the `encode_tds_sql_batch` Rust binding and stub, and TDS packet framing (ch05); `MSSQLCore.rewrite_placeholders` stays for `native-mssql-driver`; docstrings in `mssql/core.py`, `mssql/config.py`, `mssql/__init__.py` (ch05); conformance live-fixture policy (ch09).

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/mssql/driver.py` — `SyncMSSQLDriver`, `AsyncMSSQLDriver`[^mssql-driver].
- `src/py/vertebra/db/adapters/mssql/core.py` — `MSSQLCore.normalize_for_sqlite`, module imports[^mssql-core].
- `src/py/vertebra/db/adapters/mssql/config.py` — `MSSQLConfig.build_sync_driver`, `MSSQLConfig.build_async_driver`[^mssql-config].
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — `test_mssql_tds_framing_and_placeholder_rewriter`, `test_sqlspec_adapter_config_flags_and_async_driver_close`, `test_sqlalchemy_dbapi_broker_forwarding`, the two `load_from_arrow` identifier tests, and the Oracle missing-module tests t01 adds (template).
- `src/py/tests/unit/db/test_native_db_drivers.py` — `_SQL_CONTRACT_ENGINES`, `_make_contract_driver_pair`, `test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error`[^registry-dialer-test].
- `src/py/tests/integration/db/test_driver_conformance.py` — `_FAIL_LOUD_DRIVER_ENGINES`, `test_engine_driver_resolution_and_provide_driver`.

## Symbol & Signature Contracts

`src/py/vertebra/db/adapters/mssql/driver.py`:

- Module docstring: `"""SQL Server drivers that require the native ``vertebra._mssql`` module from ``vertebra-core[mssql]``."""` followed by one sentence stating that construction and every method raise `MissingDependencyError` when the module is absent.
- Imports after the change: `from types import ModuleType`; `from typing import TYPE_CHECKING, Any, ClassVar, Self, cast`; `MSSQLConfig`; `MSSQL_DIALECT_TAG` from `vertebra.db.adapters.mssql.core`; `ARROW_SHM_THRESHOLD_ROWS`, `ArrowCursor` from `vertebra.db.arrow`; `SyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver`; `ensure_module` from `vertebra.utils.module_loader`; under `TYPE_CHECKING`: `Mapping`, `Sequence`, `SyncCloudSQLDialer`. Removed: `os`, `re`, `sqlite3`, `MSSQLCore`, `_hydrate_rows_with_schema`, `_table_to_rows_and_cols`, `quote_dialect_identifier`, `validate_identifier`, `AsyncCloudSQLDialer`. `from types import ModuleType` is a runtime import, used by the `_require_mssql_native` return annotation and its `cast`. Reason: the repository ruff selection is `E`, `F`, `I`, `UP`, `B`, `SIM` with no `TC` rules, so nothing moves it under `TYPE_CHECKING`.
- Module-level resolver after `__all__`:

```python
def _require_mssql_native() -> ModuleType:
    """Resolve the native SQL Server extension module.

    Returns:
        The imported ``vertebra._mssql`` module.

    Raises:
        MissingDependencyError: When ``vertebra._mssql`` is not installed.
    """
    return cast(
        "ModuleType",
        ensure_module("vertebra._mssql", package="vertebra-core[mssql]", install_package="vertebra-core[mssql]"),
    )
```

- `class SyncMSSQLDriver(SyncDatabaseDriver[MSSQLConfig])` — docstring `"""Synchronous SQL Server driver backed by the native ``vertebra._mssql`` module from ``vertebra-core[mssql]``."""`. Keeps `dialect = "mssql"`, `dialect_tag = MSSQL_DIALECT_TAG`.
  - `__init__(self, config: MSSQLConfig | None = None, *, dialer: SyncCloudSQLDialer | None = None, broker: Any = None) -> None` → `_require_mssql_native()`; `super().__init__(config or MSSQLConfig(), broker=broker)`; `self._dialer = dialer`.
  - `dialer` property → `SyncCloudSQLDialer | None` returning `self._dialer` (properties return construction-time state and carry no guard).
  - Guarded methods, each `_require_mssql_native()` then `return super().<method>(...)` with every argument forwarded: `connect() -> Self`, `close() -> None`, `begin() -> None`, `commit() -> None`, `rollback() -> None`, `cancel() -> None`, `execute(sql, parameters=None, **kwargs) -> int`, `execute_tuples(...) -> list[tuple[Any, ...]]`, `execute_with_description(...) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`, `select(sql, parameters=None, *, schema_type: type[Any] | None = None, **kwargs) -> list[Any]`, `select_to_arrow(sql, parameters=None, *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, **kwargs) -> ArrowCursor`, `load_from_arrow(table_name: str, data: Any, *, columns: Sequence[str] | None = None, **kwargs: Any) -> int`. Parameter types match t01 (`Sequence[Any] | Mapping[str, Any] | None`). One-line Google docstrings name the operation and the `MissingDependencyError`.
- `class AsyncMSSQLDriver(ThreadOffloadAsyncDatabaseDriver[MSSQLConfig, SyncMSSQLDriver])` — docstring `"""Asynchronous SQL Server driver offloading to ``SyncMSSQLDriver``; requires ``vertebra._mssql`` from ``vertebra-core[mssql]``. The Cloud SQL dialer runs on the sync driver's worker thread."""`.

```python
def __init__(
    self,
    config: MSSQLConfig | None = None,
    *,
    dialer: SyncCloudSQLDialer | None = None,
    broker: Any = None,
) -> None:
    _require_mssql_native()
    resolved = config or MSSQLConfig()
    sync_driver = SyncMSSQLDriver(resolved, dialer=dialer, broker=broker)
    super().__init__(resolved, sync_driver, broker=sync_driver.broker)

@property
def dialer(self) -> SyncCloudSQLDialer | None:
    """Return the Cloud SQL dialer attached to the paired sync driver."""
    return self._sync.dialer
```

- Deleted: `SyncMSSQLDriver._conn`, `_in_transaction`, `_last_rewritten_sql`, `_last_tds_batch`, `last_rewritten_sql`, `last_tds_batch`, `_prepare_and_lease`, `_release_mssql_lease`; `AsyncMSSQLDriver._dialer`, `last_rewritten_sql`, `last_tds_batch`.

`src/py/vertebra/db/adapters/mssql/core.py`:

- Delete `MSSQLCore.normalize_for_sqlite`.
- Delete the now-unused imports `re`, `from collections.abc import Mapping, Sequence`, and `from typing import Any` (confirm with `uv run --no-sync ruff check`). Keep `struct`, `MSSQL_DIALECT_TAG`, `VertebraError`, `import_optional`, `encode_tds_sql_batch`, `rewrite_placeholders`, and every docstring.

`src/py/vertebra/db/adapters/mssql/config.py`:

- `MSSQLConfig.build_async_driver`: replace `registry.get_dialer(dialer_cfg, sync=False)` with `registry.get_dialer(dialer_cfg, sync=True)`. Leave the docstring and `build_sync_driver` unchanged.

## Edge Cases & Error Semantics

- `sys.modules["vertebra._mssql"] = None` → `MissingDependencyError`; `exc.package == "vertebra-core[mssql]"`; `exc.hint == "Install it with: uv add vertebra-core[mssql]"`; `isinstance(exc, ImportError)`.
- `AsyncMSSQLDriver(dialer=d)` with the module absent raises from its own resolver before `SyncMSSQLDriver` is built; `d` is never touched.
- `AsyncMSSQLDriver()` with no dialer and the stub present → `drv.dialer is None` and `drv.sync_driver.dialer is None`.
- Stub present: `SyncMSSQLDriver().select("SELECT 42 AS x")` raises `NotImplementedError`.
- Module removed after construction: every guarded method raises `MissingDependencyError`.
- `MSSQLConfig.build_sync_driver` and `build_async_driver` with a `cloud_sql_instance` both resolve `registry.get_dialer(cfg, sync=True)`, so the shared sync and async drivers hold the same cached `SyncCloudSQLDialer`.
- `MSSQLCore.encode_tds_sql_batch` keeps its callers (`MSSQLConfig.encode_sql_batch`, the `mssql/__init__.py` export, the codec test); the driver no longer calls it.
- Every test that installs or hides `vertebra._mssql` calls `reset_dependency_cache("vertebra._mssql")` in `finally`.

## Public-Seam Test Blueprint

Seam category: external boundary (native module) simulated through `sys.modules`, plus the registry/config collaborator for dialer wiring. No new test files.

**`src/py/tests/unit/db/test_enterprise_olap_drivers.py`** — t01 already adds `import sys`, `from types import ModuleType`, `Callable` under `TYPE_CHECKING`, and extends the existing `from vertebra.typing import ...` line with `MissingDependencyError, reset_dependency_cache`; this task adds no import lines there. Reason: a second `from vertebra.typing import ...` line fails ruff `I001`. `MSSQLConfig`, `MSSQLSyncConfig`, `MSSQLAsyncConfig`, `SyncCloudSQLDialer`, `CloudSQLConfig`, and `DatabaseRegistry` are already imported (the dialer and Cloud SQL names from the `from vertebra.db import (...)` block).

- `test_mssql_tds_framing_and_placeholder_rewriter`: delete the `broker = _MockBroker()` / `SyncMSSQLDriver(...)` block through `mssql_driver.close()`; keep the TDS packet, `SQLBatch`, and rewriter assertions.
- `test_sqlspec_adapter_config_flags_and_async_driver_close`: delete the `(MSSQLSyncConfig, MSSQLAsyncConfig, AsyncMSSQLDriver)` row.
- `test_sqlalchemy_dbapi_broker_forwarding`: delete the `"mssql+vtb://localhost:1433/master"` row.
- Identifier tests: delete `SyncMSSQLDriver` / `AsyncMSSQLDriver` and the `"mssql-sync"` / `"mssql-async"` ids; docstrings become `"""SyncDuckDBDriver rejects unsafe table and column identifiers."""` and `"""AsyncDuckDBDriver rejects unsafe table and column identifiers."""`.
- Add, with the same structure as the t01 Oracle tests: `def test_mssql_drivers_raise_missing_dependency_when_native_module_absent(monkeypatch: pytest.MonkeyPatch) -> None`, `@pytest.mark.anyio async def test_mssql_async_sqlspec_session_raises_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None`, `def test_mssql_driver_with_stub_native_module_raises_not_implemented(monkeypatch: pytest.MonkeyPatch) -> None`, `def test_mssql_driver_methods_require_native_module(monkeypatch: pytest.MonkeyPatch, call: Callable[[SyncMSSQLDriver], object]) -> None` (12 `pytest.param` ids, one per sync method), and `@pytest.mark.anyio async def test_async_mssql_driver_methods_require_native_module(monkeypatch: pytest.MonkeyPatch) -> None`.
- Add `def test_async_mssql_driver_forwards_sync_dialer_to_sync_driver(monkeypatch: pytest.MonkeyPatch) -> None` covering the dialer-forwarding, no-dialer, and module-absent dialer rows.

**`src/py/tests/unit/db/test_native_db_drivers.py`**:

- Remove `"mssql"` from `_SQL_CONTRACT_ENGINES` and delete the `if engine == "mssql":` branch of `_make_contract_driver_pair`.
- `test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error` gains `monkeypatch: pytest.MonkeyPatch`; add `from types import ModuleType` to the standard-library import group (the module already imports `sys` and `from vertebra.typing import MissingDependencyError, VertebraError, reset_dependency_cache`). Before `ms_sync = ...`, run `monkeypatch.setitem(sys.modules, "vertebra._mssql", ModuleType("vertebra._mssql"))` and `reset_dependency_cache("vertebra._mssql")`; wrap the MSSQL driver lines and assertions in `try` / `finally: reset_dependency_cache("vertebra._mssql")`. Replace `assert ms_async.dialer is async_csql` with `assert ms_async.dialer is sync_csql` and add `assert ms_async.sync_driver.dialer is sync_csql`. Docstring becomes `"""Verify get_dialer caching, PG/MySQL/MSSQL dialer wiring with MSSQL async drivers using the sync dialer, and provide_driver dialer rejection."""`.

**`src/py/tests/integration/db/test_driver_conformance.py`**:

- `_FAIL_LOUD_DRIVER_ENGINES = frozenset({"oracle", "mssql"})`.

**Worked Test Matrix**:

| Case | Host Test | Concrete Input | Expected Output / Error |
| --- | --- | --- | --- |
| Sync construction, module absent | `test_enterprise_olap_drivers.py::test_mssql_drivers_raise_missing_dependency_when_native_module_absent` | `SyncMSSQLDriver(MSSQLConfig.from_url("mssql://sa:secret@localhost:1433/master"))` | `MissingDependencyError`; `exc.package == "vertebra-core[mssql]"`; `exc.hint == "Install it with: uv add vertebra-core[mssql]"`; `isinstance(exc, ImportError)` |
| Async construction, module absent | `test_enterprise_olap_drivers.py::test_mssql_drivers_raise_missing_dependency_when_native_module_absent` | `AsyncMSSQLDriver()` | `MissingDependencyError` matching `r"vertebra-core\[mssql\]"`; no `TypeError` |
| Registry async | `test_enterprise_olap_drivers.py::test_mssql_drivers_raise_missing_dependency_when_native_module_absent` | `DatabaseRegistry().get_shared_driver(MSSQLConfig())` | `MissingDependencyError` |
| Registry sync | `test_enterprise_olap_drivers.py::test_mssql_drivers_raise_missing_dependency_when_native_module_absent` | `DatabaseRegistry().get_shared_sync_driver("sqlserver://sa:secret@localhost:1433/master")` | `MissingDependencyError` |
| SQLSpec sync session | `test_enterprise_olap_drivers.py::test_mssql_drivers_raise_missing_dependency_when_native_module_absent` | `cfg = MSSQLSyncConfig()`; `with cfg.provide_session(): ...` | `cfg.is_async is False`; `MissingDependencyError` |
| SQLSpec async session | `test_enterprise_olap_drivers.py::test_mssql_async_sqlspec_session_raises_missing_dependency` | `cfg = MSSQLAsyncConfig()`; `async with cfg.provide_session(): ...` | `cfg.is_async is True`; `MissingDependencyError` |
| SQLAlchemy DB-API | `test_enterprise_olap_drivers.py::test_mssql_drivers_raise_missing_dependency_when_native_module_absent` | `SyncDBAPIConnection("mssql+vtb://localhost:1433/master", broker=_MockBroker())` | `MissingDependencyError` |
| Stub present, no stand-in rows | `test_enterprise_olap_drivers.py::test_mssql_driver_with_stub_native_module_raises_not_implemented` | `sys.modules["vertebra._mssql"] = ModuleType("vertebra._mssql")`; `SyncMSSQLDriver().select("SELECT 42 AS x")` | `NotImplementedError` |
| Per-method guard (sync) | `test_enterprise_olap_drivers.py::test_mssql_driver_methods_require_native_module[<method>]` | Construct under `pytest.MonkeyPatch.context()` with the stub, then hide the module and reset the cache; the parametrized call is one of `connect()`, `close()`, `begin()`, `commit()`, `rollback()`, `cancel()`, `execute("SELECT 1")`, `execute_tuples("SELECT 1")`, `execute_with_description("SELECT 1")`, `select("SELECT 1")`, `select_to_arrow("SELECT 1")`, `load_from_arrow("items", [{"id": 1}])` | Each raises `MissingDependencyError` |
| Per-method guard (async) | `test_enterprise_olap_drivers.py::test_async_mssql_driver_methods_require_native_module` | Same setup with `AsyncMSSQLDriver()`; `await drv.execute("SELECT 1")` | `MissingDependencyError` |
| Dialer forwarding | `test_enterprise_olap_drivers.py::test_async_mssql_driver_forwards_sync_dialer_to_sync_driver` | Stub present; `d = SyncCloudSQLDialer(CloudSQLConfig(instance_connection_name="proj:us-central1:inst"))`; `drv = AsyncMSSQLDriver(MSSQLConfig(), dialer=d)` | `drv.sync_driver.dialer is d`; `drv.dialer is d` |
| No dialer | `test_enterprise_olap_drivers.py::test_async_mssql_driver_forwards_sync_dialer_to_sync_driver` | Stub present; `drv = AsyncMSSQLDriver(MSSQLConfig())` | `drv.dialer is None`; `drv.sync_driver.dialer is None` |
| Dialer forwarding, module absent | `test_enterprise_olap_drivers.py::test_async_mssql_driver_forwards_sync_dialer_to_sync_driver` | `AsyncMSSQLDriver(MSSQLConfig(), dialer=d)` with the module hidden | `MissingDependencyError` |
| Registry dialer wiring | `test_native_db_drivers.py::test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error` | Stub present; `ms_cfg = MSSQLConfig(host="localhost", port=1433, database="master", cloud_sql_instance="proj:us-central1:inst")`; `reg.get_shared_sync_driver(ms_cfg)`, `reg.get_shared_driver(ms_cfg)` | `ms_sync.dialer is sync_csql`; `ms_async.dialer is sync_csql`; `ms_async.sync_driver.dialer is sync_csql` where `sync_csql = reg.get_dialer(csql_cfg, sync=True)` |
| Conformance resolution (mssql) | `test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver[mssql]` | `provide_driver("mssql", sync=True, registry=registry)` and `sync=False` with `vertebra._mssql` hidden | Both raise `MissingDependencyError` matching `r"vertebra-core\[mssql\]"` |
| Contract suite | `test_native_db_drivers.py::test_unified_driver_contract_across_sql_engines` | Full parametrization | Runs for `sqlite`, `duckdb`, `postgres`, `mysql`, `bigquery`, `spanner` |

## Verification

- Initial Red baseline: the missing-module family fails because the drivers still construct on SQLite (`test_mssql_drivers_raise_missing_dependency_when_native_module_absent`, `test_mssql_async_sqlspec_session_raises_missing_dependency`, `test_mssql_driver_methods_require_native_module`, `test_async_mssql_driver_methods_require_native_module`, `test_mssql_driver_with_stub_native_module_raises_not_implemented`); `test_async_mssql_driver_forwards_sync_dialer_to_sync_driver` fails on `drv.sync_driver.dialer is d`, because `AsyncMSSQLDriver` builds its `SyncMSSQLDriver` without the dialer; the registry test fails on `assert ms_async.dialer is sync_csql`:
  `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py -k "mssql" -q`
- Final Green + lint + typecheck gate:
  `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_cloud_dialers.py src/py/tests/unit/extensions/test_django_plugin.py "src/py/tests/integration/db/test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver" -q`
  `uv run --no-sync ruff check src/py/vertebra/db/adapters/mssql src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/integration/db/test_driver_conformance.py`
  `uv run --no-sync ruff format --check src/py/vertebra/db/adapters/mssql src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/integration/db/test_driver_conformance.py`
  `uv run --no-sync mypy src/py/vertebra/db/adapters/mssql/driver.py src/py/vertebra/db/adapters/mssql/core.py src/py/vertebra/db/adapters/mssql/config.py`
- Static checks (each prints nothing):
  `grep -nE "sqlite3|\bnormalize_for_sqlite\b|last_tds_batch|last_rewritten_sql|_conn\b|AsyncCloudSQLDialer" src/py/vertebra/db/adapters/mssql/driver.py src/py/vertebra/db/adapters/mssql/core.py`
  `grep -rnE "\bnormalize_for_sqlite\b|\blast_tds_batch\b|\blast_rewritten_sql\b" src/py/vertebra src/py/tests` (word-bounded, matching t01)
  `grep -n "TDS 7.4\|SHM statement cache\|sqlite" src/py/vertebra/db/adapters/mssql/driver.py`
- Docstring check: `grep -c "vertebra-core\[mssql\]" src/py/vertebra/db/adapters/mssql/driver.py` prints `4` or more.
- Binding retained: `grep -n "def encode_tds_sql_batch" src/py/vertebra/db/adapters/mssql/core.py src/py/vertebra/_vertebra.pyi` prints two lines.
- Chapter checkpoint (both tasks): the command in [spec.md §5](../spec.md#5-implementation-plan).

## Acceptance Criteria

- [ ] `SyncMSSQLDriver()` and `AsyncMSSQLDriver()` raise `MissingDependencyError` naming `vertebra-core[mssql]` when `vertebra._mssql` is absent (R1).
- [ ] Every public method defined on `SyncMSSQLDriver` calls `_require_mssql_native()` first (R1).
- [ ] `mssql/driver.py` imports no `sqlite3` or `re`; `MSSQLCore.normalize_for_sqlite`, `last_tds_batch`, and `last_rewritten_sql` are deleted; `MSSQLCore.encode_tds_sql_batch` and the binding remain (R1).
- [ ] `AsyncMSSQLDriver` forwards its `SyncCloudSQLDialer` to `SyncMSSQLDriver`, and `MSSQLConfig.build_async_driver` supplies the sync dialer (R2).
- [ ] Registry, SQLSpec, SQLAlchemy DB-API, and conformance `provide_driver` paths surface `MissingDependencyError` for MSSQL (R1).
- [ ] Driver module and class docstrings state the `vertebra._mssql` / `vertebra-core[mssql]` requirement (ch01 doc plan).
- [ ] Zero inline `#` comments and zero new test files.

## Notes & Discoveries

- [2026-11-07 00:00] At `0acdf576` the async driver stores an `AsyncCloudSQLDialer` while its sync driver, which performs every call on a worker thread, holds none; the sync dialer is the only type that thread can drive.
- [2026-11-07 00:00] For ch05: once the driver stops calling it, `MSSQLCore.encode_tds_sql_batch` is reached only through `MSSQLConfig.encode_sql_batch`, the `vertebra.db.adapters.mssql.encode_tds_sql_batch` lazy export, and `test_mssql_tds_framing_and_placeholder_rewriter`.

[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — MSSQL sync and async drivers backed by sqlite3.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore.normalize_for_sqlite` — SQLite-only MSSQL placeholder normalizer.
[^mssql-config]: `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.build_async_driver` — Async driver construction with a Cloud SQL dialer.
[^offload-base]: `src/py/vertebra/db/driver/_async.py::ThreadOffloadAsyncDatabaseDriver` — Async driver delegating to a paired sync driver on a worker thread.
[^registry-dialer-test]: `src/py/tests/unit/db/test_native_db_drivers.py::test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error` — Registry Cloud SQL dialer wiring test.
