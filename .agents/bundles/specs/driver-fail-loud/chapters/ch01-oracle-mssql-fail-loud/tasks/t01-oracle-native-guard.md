---
type: Task
id: "driver-fail-loud:ch01:t01"
chapter_id: ch01-oracle-mssql-fail-loud
title: Guard Oracle drivers on vertebra._oracle and delete the SQLite stand-in
description: SyncOracleDriver and AsyncOracleDriver resolve vertebra._oracle in __init__ and every public method, raise MissingDependencyError when it is absent, and carry no sqlite3 connection, normalize_sql helper, piggyback state, or broker lease helpers.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/core.py
  - src/py/vertebra/db/driver/_sync.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/integration/db/test_driver_conformance.py
files:
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/db/adapters/oracle/core.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_native_db_drivers.py
  - src/py/tests/integration/db/test_driver_conformance.py
estimated_loc: 230
plan_revision: 2
plan_commit: c2b09b9b
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T02:53:00Z"
last_verified_commit: 06762e64
verification_evidence: "pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py -q (348 passed)"
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T02:53:00Z"
commit: 06762e64
sources:
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle sync and async drivers backed by sqlite3
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore.normalize_sql
    title: SQLite-only Oracle placeholder normalizer
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: sync-base
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: Base sync driver routing through execute_with_description
  - id: sqlspec-oracle
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver.apply_tenant_predicate
    title: SQLSpec Oracle tenant predicate over an injected native port
tags:
  - database
  - oracle
  - fail-loud
  - tdd
---

# Task t01: Guard Oracle Drivers on `vertebra._oracle` and Delete the SQLite Stand-In

## Objective & Scope Boundary

Make `SyncOracleDriver` and `AsyncOracleDriver` raise `MissingDependencyError` on construction when `vertebra._oracle` is absent, guard every public method they define with the same resolver, and delete the SQLite stand-in with every helper that exists only to feed it[^oracle-driver][^module-loader]. Delete `OracleCore.normalize_sql` and `_NAMED_PARAM_RE`, whose only caller is the driver[^oracle-core]. Rewrite the driver module and class docstrings to state the native module requirement. Update the three canonical test files so no test depends on Oracle SQLite execution, Oracle tenant tests run against an injected port, and missing-module behaviour is covered at every consumer seam.

Out of scope: MSSQL (t02); docstrings in `oracle/core.py`, `oracle/config.py`, `oracle/__init__.py` (ch06); TNS packet framing and its codec test assertions (ch06); conformance live-fixture policy (ch09); `from __future__ import annotations` removal.

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver`, `AsyncOracleDriver`[^oracle-driver].
- `src/py/vertebra/db/adapters/oracle/core.py` — `OracleCore.normalize_sql`, `_NAMED_PARAM_RE`, module imports[^oracle-core].
- `src/py/vertebra/db/driver/_sync.py` — `SyncDatabaseDriver` method signatures and routing[^sync-base].
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — `test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs`, `test_sqlspec_adapter_config_flags_and_async_driver_close`, `test_sqlalchemy_dbapi_broker_forwarding`, `test_sync_tenant_session_setting_preserves_failure_identity`, `test_async_tenant_session_setting_preserves_failure_identity`, `test_oracle_tenant_context_stand_in_sets_session_attribute`, `test_async_oracle_tenant_context_stand_in_sets_session_attribute`, `test_enterprise_and_olap_sync_load_from_arrow_rejects_unsafe_identifiers`, `test_enterprise_and_olap_async_load_from_arrow_rejects_unsafe_identifiers`.
- `src/py/tests/unit/db/test_native_db_drivers.py` — `_SQL_CONTRACT_ENGINES`, `_make_contract_driver_pair`, `test_unified_driver_contract_across_all_eight_engines`, `test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent` (template).
- `src/py/tests/integration/db/test_driver_conformance.py` — `test_engine_driver_resolution_and_provide_driver`, imports.

## Symbol & Signature Contracts

`src/py/vertebra/db/adapters/oracle/driver.py`:

- Module docstring: `"""Oracle drivers that require the native ``vertebra._oracle`` module from ``vertebra-core[oracle]``."""` followed by one sentence stating that construction and every method raise `MissingDependencyError` when the module is absent.
- Imports after the change: `asyncio`; `from types import ModuleType`; `from typing import TYPE_CHECKING, Any, ClassVar, Self, cast`; `OracleConfig` from `vertebra.db.adapters.oracle.config`; `ORACLE_DIALECT_TAG` from `vertebra.db.adapters.oracle.core`; `ARROW_SHM_THRESHOLD_ROWS`, `ArrowCursor` from `vertebra.db.arrow`; `SyncDatabaseDriver`, `ThreadOffloadAsyncDatabaseDriver` from `vertebra.db.driver`; `ensure_module` from `vertebra.utils.module_loader`; `Mapping`, `Sequence` under `TYPE_CHECKING`. Removed: `os`, `sqlite3`, runtime `Mapping`, `OracleSessionAttrs`, `OracleCore`, `_hydrate_rows_with_schema`, `_table_to_rows_and_cols`, `validate_identifier`. `from types import ModuleType` is a runtime import beside `asyncio`, used by the `_require_oracle_native` return annotation and its `cast`. Reason: the repository ruff selection is `E`, `F`, `I`, `UP`, `B`, `SIM` with no `TC` rules, so nothing moves it under `TYPE_CHECKING`.
- `__all__ = ("AsyncOracleDriver", "SyncOracleDriver")` unchanged.
- Module-level, placed after `__all__` and before the classes:

```python
def _require_oracle_native() -> ModuleType:
    """Resolve the native Oracle extension module.

    Returns:
        The imported ``vertebra._oracle`` module.

    Raises:
        MissingDependencyError: When ``vertebra._oracle`` is not installed.
    """
    return cast(
        "ModuleType",
        ensure_module("vertebra._oracle", package="vertebra-core[oracle]", install_package="vertebra-core[oracle]"),
    )
```

- `class SyncOracleDriver(SyncDatabaseDriver[OracleConfig])` — docstring: `"""Synchronous Oracle driver backed by the native ``vertebra._oracle`` module from ``vertebra-core[oracle]``."""`. Keeps `dialect: ClassVar[str] = "oracle"` and `dialect_tag: ClassVar[int] = ORACLE_DIALECT_TAG`. Defines exactly these methods, each calling `_require_oracle_native()` as its first statement and then returning the `super()` call with every argument forwarded:
  - `__init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None` → `super().__init__(config or OracleConfig(), broker=broker)`.
  - `connect(self) -> Self`, `close(self) -> None`, `begin(self) -> None`, `commit(self) -> None`, `rollback(self) -> None`, `cancel(self) -> None`.
  - `execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, **kwargs: Any) -> int`.
  - `execute_tuples(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, **kwargs: Any) -> list[tuple[Any, ...]]`.
  - `execute_with_description(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, **kwargs: Any) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`.
  - `select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, schema_type: type[Any] | None = None, **kwargs: Any) -> list[Any]`.
  - `select_to_arrow(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, **kwargs: Any) -> ArrowCursor`.
  - `load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, direct_path: bool | None = None, **kwargs: Any) -> int` → `_ = direct_path`, then `super().load_from_arrow(table_name, data, columns=columns, **kwargs)`.
  - Each method has a one-line Google docstring stating the operation and that it raises `MissingDependencyError` without the native module (for example `"""Execute a DML or DDL statement; raises ``MissingDependencyError`` without ``vertebra._oracle``."""`).
- `class AsyncOracleDriver(ThreadOffloadAsyncDatabaseDriver[OracleConfig, SyncOracleDriver])` — docstring: `"""Asynchronous Oracle driver offloading to ``SyncOracleDriver``; requires ``vertebra._oracle`` from ``vertebra-core[oracle]``."""`. Keeps `dialect` and `dialect_tag`. Defines:
  - `__init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None` → `_require_oracle_native()`; `resolved = config or OracleConfig()`; `sync_driver = SyncOracleDriver(resolved, broker=broker)`; `super().__init__(resolved, sync_driver, broker=sync_driver.broker)`.
  - `async def load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, direct_path: bool | None = None, **kwargs: Any) -> int` → `_require_oracle_native()`; `return await asyncio.to_thread(self._sync.load_from_arrow, table_name, data, columns=columns, direct_path=direct_path, **kwargs)`.
- Deleted: `SyncOracleDriver._conn`, `_in_transaction`, `_session_context`, `_pending_session_attrs`, `_last_piggyback_frame`, `last_piggyback_frame`, `set_session_attrs`, `_acquire_oracle_broker_lease`, `_release_oracle_broker_lease`, `_consume_piggyback`; `AsyncOracleDriver.last_piggyback_frame`, `set_session_attrs`.

`src/py/vertebra/db/adapters/oracle/core.py`:

- Delete `OracleCore.normalize_sql` and `_NAMED_PARAM_RE`.
- Change `from collections.abc import Mapping, Sequence` to `from collections.abc import Sequence` (no other `Mapping` use remains). Keep `re` (TNS regexes use it). Touch no docstrings.

Caller wiring is unchanged: `OracleConfig.build_sync_driver` / `build_async_driver`, `DatabaseRegistry`, `extensions/sqlspec/adapters.py` (`OracleSyncConfig._driver_cls`, `OracleAsyncConfig._driver_cls`), and `oracle/embeddings.py` type references keep importing the same class names.

## Edge Cases & Error Semantics

- `sys.modules["vertebra._oracle"] = None` → `import_optional` returns `None` → `MissingDependencyError("vertebra-core[oracle]", install_package="vertebra-core[oracle]")`; `str(exc) == "Optional dependency 'vertebra-core[oracle]' is required but not installed."`; `exc.hint == "Install it with: uv add vertebra-core[oracle]"`; `isinstance(exc, ImportError)`.
- `AsyncOracleDriver()` raises `MissingDependencyError` from its own resolver call before `SyncOracleDriver` is built; never `TypeError`.
- Stub module present (`ModuleType("vertebra._oracle")` in `sys.modules`): construction succeeds; `select("SELECT 42 AS x FROM DUAL")` raises `NotImplementedError` from `SyncDatabaseDriver.execute_with_description`.
- Module removed after construction: every guarded method raises `MissingDependencyError`.
- `import_optional` caches a stub in `_module_cache`; every test that installs or hides the module calls `reset_dependency_cache("vertebra._oracle")` in `finally`.
- `DatabaseRegistry.get_shared_driver(OracleConfig())` raises before the pool-key cache write, so nothing is cached.
- `SyncDBAPIConnection("oracle+vtb://localhost:1521/FREEPDB1", broker=_MockBroker())` raises `MissingDependencyError` from its constructor (it calls `OracleConfig.build_sync_driver`).
- Importing `vertebra.db.adapters.oracle.driver` and constructing `OracleConfig()`, `OracleSyncConfig()`, `OracleAsyncConfig()` never call the resolver.

## Public-Seam Test Blueprint

Seam category: external boundary (native module) simulated through `sys.modules`, following `test_bigquery_and_spanner_drivers_raise_missing_dependency_when_wheel_absent`. No new test files.

**`src/py/tests/unit/db/test_enterprise_olap_drivers.py`** — add `import sys` and `from types import ModuleType` to the standard-library import group, and extend the existing `from vertebra.typing import ImproperConfigurationError, VertebraError` line to `from vertebra.typing import ImproperConfigurationError, MissingDependencyError, VertebraError, reset_dependency_cache`. Reason: a second `from vertebra.typing import ...` line fails ruff `I001`. `DatabaseRegistry`, `SyncDBAPIConnection`, `OracleSyncDriver`, `OracleAsyncDriver`, `OracleSyncConfig`, `OracleAsyncConfig`, and `TenantPredicate` are already imported. Add module-level stubs:

```python
class _OracleSessionPort:
    """Native port stub recording Oracle session calls for SQLSpec adapter tests."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def execute(self, sql: str, parameters: Any = None) -> int:
        """Record the statement and its parameters and report zero affected rows."""
        self.calls.append((sql, parameters))
        return 0

    def close(self) -> None:
        """Release nothing; the stub holds no resources."""
```

and `_AsyncOracleSessionPort` with `async def execute` and `async def close` of the same shape.

Changes:

- `test_oracle_26ai_vector_oson_and_ttc_piggyback_codecs`: delete the `broker = _MockBroker()` / `SyncOracleDriver(...)` block through `ora_driver.close()`; keep every codec assertion.
- `test_sqlspec_adapter_config_flags_and_async_driver_close`: delete the `(OracleSyncConfig, OracleAsyncConfig, AsyncOracleDriver)` row; docstring becomes `"""Verify Sync and Async SQLSpec adapter flags and async driver teardown."""`.
- `test_sqlalchemy_dbapi_broker_forwarding`: delete the `"oracle+vtb://localhost:1521/FREEPDB1"` row.
- `test_sync_tenant_session_setting_preserves_failure_identity` / async twin: keep the `["postgres", "oracle"]` parametrization; the Oracle branch builds `_OracleSessionPort()` / `_AsyncOracleSessionPort()` instead of `SyncOracleDriver()` / `AsyncOracleDriver()`.
- Delete `test_oracle_tenant_context_stand_in_sets_session_attribute` and `test_async_oracle_tenant_context_stand_in_sets_session_attribute`; add `test_oracle_tenant_predicate_sets_session_context_through_native_port` and `test_async_oracle_tenant_predicate_sets_session_context_through_native_port`.
- `test_enterprise_and_olap_sync_load_from_arrow_rejects_unsafe_identifiers` / async twin: delete `SyncOracleDriver` / `AsyncOracleDriver` and the `"oracle-sync"` / `"oracle-async"` ids; docstrings name the remaining classes.
- Add `def test_oracle_drivers_raise_missing_dependency_when_native_module_absent(monkeypatch: pytest.MonkeyPatch) -> None`: `monkeypatch.setitem(sys.modules, "vertebra._oracle", None)`, `reset_dependency_cache("vertebra._oracle")`, then its matrix rows inside `try` with `reset_dependency_cache("vertebra._oracle")` in `finally`.
- Add `@pytest.mark.anyio async def test_oracle_async_sqlspec_session_raises_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None` with the same hide/reset/`finally` structure.
- Add `def test_oracle_driver_with_stub_native_module_raises_not_implemented(monkeypatch: pytest.MonkeyPatch) -> None`: `monkeypatch.setitem(sys.modules, "vertebra._oracle", ModuleType("vertebra._oracle"))`, `reset_dependency_cache("vertebra._oracle")`, the stub row inside `try`, reset in `finally`.
- Add `def test_oracle_driver_methods_require_native_module(monkeypatch: pytest.MonkeyPatch, call: Callable[[SyncOracleDriver], object]) -> None` parametrized over the 12 sync method calls (one `pytest.param(lambda drv: drv.<method>(...), id="<method>")` each).
- Add `@pytest.mark.anyio async def test_async_oracle_driver_methods_require_native_module(monkeypatch: pytest.MonkeyPatch) -> None` covering the async `load_from_arrow` and `execute` rows. Add `from collections.abc import Callable` to the existing `if TYPE_CHECKING:` block beside `from pathlib import Path`; the module keeps `from __future__ import annotations`, so the annotation never evaluates at runtime.

**`src/py/tests/unit/db/test_native_db_drivers.py`**:

- Remove `"oracle"` from `_SQL_CONTRACT_ENGINES` and delete the `if engine == "oracle":` branch of `_make_contract_driver_pair`.
- Rename `test_unified_driver_contract_across_all_eight_engines` to `test_unified_driver_contract_across_sql_engines`; docstring `"""Verify unified signatures, return types, NotFoundError, Arrow, transactions, and alias removal."""`.
- Drop `AsyncOracleDriver` and `SyncOracleDriver` from the `vertebra.db.adapters.oracle` import when ruff reports them unused; keep `OracleConfig`.

**`src/py/tests/integration/db/test_driver_conformance.py`**:

- Add `import sys`, `from vertebra.typing import reset_dependency_cache` (extend the existing `vertebra.typing` import), and module-level `_FAIL_LOUD_DRIVER_ENGINES: frozenset[str] = frozenset({"oracle"})`.
- `test_engine_driver_resolution_and_provide_driver` gains `monkeypatch: pytest.MonkeyPatch`. After `registry.register(spec.engine, config)`, when `spec.engine in _FAIL_LOUD_DRIVER_ENGINES`: assert `spec.native_extension_module is not None`, set `sys.modules[spec.native_extension_module] = None` via `monkeypatch.setitem`, `reset_dependency_cache(...)`, assert both `provide_driver(spec.engine, sync=True, registry=registry)` and `provide_driver(spec.engine, sync=False, registry=registry)` raise `MissingDependencyError` matching `rf"vertebra-core\[{spec.engine}\]"`, reset the cache in `finally`, and `return`.

**Worked Test Matrix**:

| Case | Host Test | Concrete Input | Expected Output / Error |
| --- | --- | --- | --- |
| Sync construction, module absent | `test_enterprise_olap_drivers.py::test_oracle_drivers_raise_missing_dependency_when_native_module_absent` | `SyncOracleDriver(OracleConfig.from_url("oracle://u:p@db:1521/svc"))` | `MissingDependencyError`; `exc.package == "vertebra-core[oracle]"`; `exc.hint == "Install it with: uv add vertebra-core[oracle]"`; `isinstance(exc, ImportError)` |
| Async construction, module absent | `test_enterprise_olap_drivers.py::test_oracle_drivers_raise_missing_dependency_when_native_module_absent` | `AsyncOracleDriver()` | `MissingDependencyError` matching `r"vertebra-core\[oracle\]"`; no `TypeError` |
| Registry async | `test_enterprise_olap_drivers.py::test_oracle_drivers_raise_missing_dependency_when_native_module_absent` | `DatabaseRegistry().get_shared_driver(OracleConfig())` | `MissingDependencyError` |
| Registry sync | `test_enterprise_olap_drivers.py::test_oracle_drivers_raise_missing_dependency_when_native_module_absent` | `DatabaseRegistry().get_shared_sync_driver("oracle://scott:tiger@localhost:1521/FREEPDB1")` | `MissingDependencyError` |
| SQLSpec sync session | `test_enterprise_olap_drivers.py::test_oracle_drivers_raise_missing_dependency_when_native_module_absent` | `cfg = OracleSyncConfig()`; `with cfg.provide_session(): ...` | `cfg.is_async is False`; `provide_session` raises `MissingDependencyError` |
| SQLSpec async session | `test_enterprise_olap_drivers.py::test_oracle_async_sqlspec_session_raises_missing_dependency` | `cfg = OracleAsyncConfig()`; `async with cfg.provide_session(): ...` | `cfg.is_async is True`; `MissingDependencyError` |
| SQLAlchemy DB-API | `test_enterprise_olap_drivers.py::test_oracle_drivers_raise_missing_dependency_when_native_module_absent` | `SyncDBAPIConnection("oracle+vtb://localhost:1521/FREEPDB1", broker=_MockBroker())` | `MissingDependencyError` |
| Stub present, no stand-in rows | `test_enterprise_olap_drivers.py::test_oracle_driver_with_stub_native_module_raises_not_implemented` | `sys.modules["vertebra._oracle"] = ModuleType("vertebra._oracle")`; `SyncOracleDriver().select("SELECT 42 AS x FROM DUAL")` | `NotImplementedError` |
| Per-method guard (sync) | `test_enterprise_olap_drivers.py::test_oracle_driver_methods_require_native_module[<method>]` | `with pytest.MonkeyPatch.context() as stub_ctx:` `stub_ctx.setitem(sys.modules, "vertebra._oracle", ModuleType("vertebra._oracle"))`, `reset_dependency_cache("vertebra._oracle")`, `drv = SyncOracleDriver()`; after the block, `monkeypatch.setitem(sys.modules, "vertebra._oracle", None)` and `reset_dependency_cache("vertebra._oracle")`; then the parametrized call, one of: `connect()`, `close()`, `begin()`, `commit()`, `rollback()`, `cancel()`, `execute("SELECT 1 FROM DUAL")`, `execute_tuples("SELECT 1 FROM DUAL")`, `execute_with_description("SELECT 1 FROM DUAL")`, `select("SELECT 1 FROM DUAL")`, `select_to_arrow("SELECT 1 FROM DUAL")`, `load_from_arrow("items", [{"id": 1}])` | Each raises `MissingDependencyError` |
| Per-method guard (async) | `test_enterprise_olap_drivers.py::test_async_oracle_driver_methods_require_native_module` | Same setup with `AsyncOracleDriver()`; `await drv.load_from_arrow("items", [{"id": 1}])` and `await drv.execute("SELECT 1 FROM DUAL")` | Each raises `MissingDependencyError` |
| Tenant context via port (sync) | `test_enterprise_olap_drivers.py::test_oracle_tenant_predicate_sets_session_context_through_native_port` | `port = _OracleSessionPort()`; `OracleSyncDriver(port).apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))` | Returns `"o.tenant_id = :tenant_id"`; `port.calls == [("BEGIN DBMS_SESSION.SET_CONTEXT(:namespace, :attribute, :value); END;", {"namespace": "VERTEBRA_CTX", "attribute": "tenant_id", "value": "acme"})]` |
| Tenant context via port (async) | `test_enterprise_olap_drivers.py::test_async_oracle_tenant_predicate_sets_session_context_through_native_port` | `port = _AsyncOracleSessionPort()`; `await OracleAsyncDriver(port).apply_tenant_predicate(TenantPredicate(table_alias="o", tenant_value="acme"))` | Same return value and `port.calls` |
| Tenant failure identity (oracle) | `test_enterprise_olap_drivers.py::test_sync_tenant_session_setting_preserves_failure_identity[oracle]`, `::test_async_tenant_session_setting_preserves_failure_identity[oracle]` | `_OracleSessionPort` / `_AsyncOracleSessionPort` with `execute` monkeypatched to raise `failure = RuntimeError("session setting refused")` | `raised.value is failure` |
| Conformance resolution (oracle) | `test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver[oracle]` | `provide_driver("oracle", sync=True, registry=registry)` and `sync=False` with `vertebra._oracle` hidden | Both raise `MissingDependencyError` matching `r"vertebra-core\[oracle\]"`; `config.resolve_driver_classes() == (SyncOracleDriver, AsyncOracleDriver)` still holds |
| Contract suite | `test_native_db_drivers.py::test_unified_driver_contract_across_sql_engines` | Full parametrization | Runs for `sqlite`, `duckdb`, `mssql`, `postgres`, `mysql`, `bigquery`, `spanner`; no `oracle` id |

## Verification

- Initial Red baseline: only the missing-module family fails, because the drivers still construct on SQLite: `test_oracle_drivers_raise_missing_dependency_when_native_module_absent`, `test_oracle_async_sqlspec_session_raises_missing_dependency`, `test_oracle_driver_methods_require_native_module`, `test_async_oracle_driver_methods_require_native_module`, and `test_oracle_driver_with_stub_native_module_raises_not_implemented` (the stand-in returns `[{"x": 42}]`). The port-based tenant tests (`test_oracle_tenant_predicate_sets_session_context_through_native_port`, its async twin, and the `[oracle]` failure-identity cases) pass at Red, because `OracleSyncDriver` / `OracleAsyncDriver` already accept any object as the native port:
  `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k "oracle" -q`
- Final Green + lint + typecheck gate:
  `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_django_plugin.py "src/py/tests/integration/db/test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver" -q`
  `uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle/driver.py src/py/vertebra/db/adapters/oracle/core.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/integration/db/test_driver_conformance.py`
  `uv run --no-sync ruff format --check src/py/vertebra/db/adapters/oracle/driver.py src/py/vertebra/db/adapters/oracle/core.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/integration/db/test_driver_conformance.py`
  `uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/driver.py src/py/vertebra/db/adapters/oracle/core.py`
- Static checks (each prints nothing):
  `grep -nE "sqlite3|\bnormalize_sql\b|_NAMED_PARAM_RE|_conn\b" src/py/vertebra/db/adapters/oracle/driver.py src/py/vertebra/db/adapters/oracle/core.py`
  `grep -rnE "\bnormalize_sql\b|last_piggyback_frame|set_session_attrs" src/py/vertebra src/py/tests` (word-bounded so `sqlite/driver.py::_normalize_sqlite_param` does not match)
  `grep -n "Thin\|SHM statement cache\|sqlite" src/py/vertebra/db/adapters/oracle/driver.py`
- Docstring check: `grep -c "vertebra-core\[oracle\]" src/py/vertebra/db/adapters/oracle/driver.py` prints `4` or more.

## Acceptance Criteria

- [x] `SyncOracleDriver()` and `AsyncOracleDriver()` raise `MissingDependencyError` naming `vertebra-core[oracle]` when `vertebra._oracle` is absent (R1).
- [x] Every public method defined on `SyncOracleDriver` and `AsyncOracleDriver.load_from_arrow` calls `_require_oracle_native()` first (R1).
- [x] `oracle/driver.py` imports no `sqlite3`; `OracleCore.normalize_sql` and `_NAMED_PARAM_RE` are deleted (R1).
- [x] Registry, SQLSpec sync/async session, SQLAlchemy DB-API, and conformance `provide_driver` paths surface `MissingDependencyError` for Oracle (R1).
- [x] Oracle tenant predicate tests run against injected ports and assert the exact `DBMS_SESSION.SET_CONTEXT` call (R1).
- [x] Driver module and class docstrings state the `vertebra._oracle` / `vertebra-core[oracle]` requirement (ch01 doc plan).
- [x] Zero inline `#` comments and zero new test files.

## Notes & Discoveries

- [2026-11-07 00:00] `import_optional` caches a found module in `_module_cache`, so a stub left in the cache leaks into later tests; reset it in `finally`.
- [2026-11-07 00:00] `OracleSyncDriver` (SQLSpec) stores any object as `native_driver`[^sqlspec-oracle]; `BaseSyncDriver._normalize_params` passes a single mapping through unchanged, so the port receives the dict from `OracleCore.build_session_context_sql`.
- [2026-11-07 00:00] `integration/conftest.py::vtb_oracle_driver` has no consumers; ch09/t01 deletes it, so this task leaves `integration/conftest.py` untouched.

[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle sync and async drivers backed by sqlite3.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore.normalize_sql` — SQLite-only Oracle placeholder normalizer.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising MissingDependencyError.
[^sync-base]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver` — Base sync driver routing through execute_with_description.
[^sqlspec-oracle]: `src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver.apply_tenant_predicate` — SQLSpec Oracle tenant predicate over an injected native port.
