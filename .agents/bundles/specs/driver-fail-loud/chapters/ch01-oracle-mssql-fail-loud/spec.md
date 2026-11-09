---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch01-oracle-mssql-fail-loud
title: "Chapter 01: Oracle and MSSQL Fail-Loud Drivers"
description: Oracle and MSSQL drivers resolve vertebra._oracle / vertebra._mssql on construction and on every public method, raise MissingDependencyError when the module is absent, carry no SQLite execution or SQLite-only helpers, and AsyncMSSQLDriver forwards its dialer to the paired sync driver.
status: stable
state: completed
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: null
plan_revision: 2
plan_commit: fd571e3a
state_revision: 1
last_verified_checkpoint: d7a629b1
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-09T02:53:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified: []
sources:
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: oracle-driver
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: Oracle sync and async drivers backed by sqlite3
  - id: mssql-driver
    resource: src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver
    title: MSSQL sync and async drivers backed by sqlite3
  - id: oracle-core
    resource: src/py/vertebra/db/adapters/oracle/core.py::OracleCore
    title: Oracle codecs and the SQLite-only normalize_sql helper
  - id: mssql-core
    resource: src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore
    title: TDS codecs, placeholder rewriter, and the SQLite-only normalize_for_sqlite helper
  - id: mssql-config
    resource: src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig
    title: MSSQL config building sync and async drivers with Cloud SQL dialers
  - id: sync-base
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: Base sync driver whose execute paths route through execute_with_description
  - id: offload-base
    resource: src/py/vertebra/db/driver/_async.py::ThreadOffloadAsyncDatabaseDriver
    title: Async driver delegating every call to a paired sync driver on a worker thread
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver
    title: SQLSpec Oracle and MSSQL adapters and session providers
  - id: olap-tests
    resource: src/py/tests/unit/db/test_enterprise_olap_drivers.py
    title: Oracle, MSSQL, and DuckDB driver and codec unit tests
  - id: native-tests
    resource: src/py/tests/unit/db/test_native_db_drivers.py::test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error
    title: Unified driver contract and registry dialer wiring tests
  - id: conformance-resolution
    resource: src/py/tests/integration/db/test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver
    title: Conformance driver class resolution and provide_driver test
tags:
  - database
  - drivers
  - fail-loud
  - oracle
  - mssql
---

# Chapter 01: Oracle and MSSQL Fail-Loud Drivers

<!-- truth: start -->
- **Construction guard**: `SyncOracleDriver.__init__` and `AsyncOracleDriver.__init__` call `_require_oracle_native()` before any state is built; `SyncMSSQLDriver.__init__` and `AsyncMSSQLDriver.__init__` call `_require_mssql_native()` the same way.[^oracle-driver][^mssql-driver]
- **Resolver shape**: each resolver is a module-level function in its `driver.py` returning `cast("ModuleType", ensure_module("vertebra._<engine>", package="vertebra-core[<engine>]", install_package="vertebra-core[<engine>]"))`; neither passes `extra=`.[^module-loader]
- **Per-method guard**: every public method a driver class defines calls its resolver first, then delegates to the base class implementation; no method executes SQL against any engine.[^sync-base][^offload-base]
- **No SQLite**: neither `driver.py` imports `sqlite3`, holds `_conn`, or rewrites placeholders to `?`; `OracleCore.normalize_sql`, `_NAMED_PARAM_RE`, and `MSSQLCore.normalize_for_sqlite` are deleted.[^oracle-core][^mssql-core]
- **Error contract**: a missing module raises `MissingDependencyError` (an `ImportError`) whose `package` is `vertebra-core[oracle]` / `vertebra-core[mssql]` and whose `hint` is `Install it with: uv add vertebra-core[<engine>]`; never `TypeError`.[^module-loader]
- **Import safety**: importing either `driver.py`, `vertebra.db.registry`, or `vertebra.extensions.sqlspec.adapters`, and constructing `OracleConfig`, `MSSQLConfig`, `OracleSyncConfig`, or `MSSQLSyncConfig`, never calls a resolver.[^sqlspec-adapters]
- **Dialer forwarding**: `AsyncMSSQLDriver` accepts `SyncCloudSQLDialer | None`, passes it to its `SyncMSSQLDriver`, and its `dialer` property returns `self._sync.dialer`; `MSSQLConfig.build_async_driver` requests `registry.get_dialer(cfg, sync=True)`.[^mssql-config][^offload-base]
- **Codecs stay**: TNS/TTC/VECTOR/OSON codecs, `OracleCore.build_session_context_sql`, `MSSQLCore.rewrite_placeholders`, `MSSQLCore.encode_tds_sql_batch`, and the `encode_tds_sql_batch` Rust binding are untouched in this chapter.[^oracle-core][^mssql-core]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`SyncOracleDriver` and `SyncMSSQLDriver` accept host, user, and password, then open `sqlite3.connect(":memory:")` in `__init__` and run every statement against it after rewriting `:1` / `:name` / `@P1` placeholders to `?`.[^oracle-driver][^mssql-driver] Callers that build an Oracle or SQL Server driver through `DatabaseRegistry`, a SQLSpec session, a Django backend, or a SQLAlchemy `+vtb` URL receive rows from SQLite. The async drivers wrap the same stand-in through `ThreadOffloadAsyncDatabaseDriver`.[^offload-base] `AsyncMSSQLDriver` stores its dialer on itself and builds its sync driver without one, so the thread that would perform I/O never sees the Cloud SQL dialer.[^mssql-driver]

This chapter makes both drivers resolve their native extension module (`vertebra._oracle`, `vertebra._mssql`) on construction and on every public method, deletes the SQLite stand-in and the helpers that exist only to feed it, and forwards the MSSQL dialer to the sync driver that owns I/O. Until the native wheels ship, constructing any of the four driver classes raises `MissingDependencyError`.

**Non-goals**:

- Native Oracle or SQL Server clients and their connection APIs (owned by `native-oracle-driver` and `native-mssql-driver`).
- Docstrings in `mssql/core.py`, `mssql/config.py`, `mssql/__init__.py` (owned by ch05) and in `oracle/core.py`, `oracle/config.py`, `oracle/__init__.py` (owned by ch06).
- Deleting `MSSQLCore.encode_tds_sql_batch`, the `encode_tds_sql_batch` binding, its `_vertebra.pyi` stub, TNS/TDS packet framing, or Python branches probing unexported natives (owned by ch05 for MSSQL and TDS, ch06 for Oracle, TNS, and Redis).
- `MissingDependencyError` default hint and the extras gate (owned by ch08-install-hints-and-extras-gate); this chapter passes `install_package` explicitly.
- Conformance live-fixture skip/fail policy and `src/py/tests/integration/conftest.py` (owned by ch09). The `vtb_oracle_driver` and `vtb_mssql_driver` fixtures there have no consumers; ch09/t01 deletes them, so ch01 leaves that file untouched.
- The SQLAlchemy DB-API parse fallback (owned by ch02); this chapter only drops the `oracle+vtb` / `mssql+vtb` rows from the broker-forwarding test because those URLs now raise.
- `from __future__ import annotations` in the touched modules; it is used across `src/py/vertebra/db/adapters/**` and its removal is a separate sweep.

### 1.2 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent roadmap [spec.md](../../spec.md); catalog [chapters/index.md](../index.md).
- `depends_on_chapters: []`; ch02 through ch13 start from this chapter's baseline.
- Research: [change-surface](../../research/change-surface_20261107.md).

### 1.3 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| Optional modules | `src/py/vertebra/utils/module_loader.py::ensure_module`, `::import_optional`[^module-loader] | In-process; `import_optional` returns `None` when `sys.modules[name] is None` and caches hits in `_module_cache`, which `reset_dependency_cache(name)` clears |
| Oracle driver | `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver`, `::AsyncOracleDriver`[^oracle-driver] | External boundary (native module); `_conn`, `_session_context`, piggyback state, and broker lease helpers serve only the SQLite path |
| MSSQL driver | `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver`, `::AsyncMSSQLDriver`[^mssql-driver] | External boundary (native module); `_conn`, `_prepare_and_lease`, `last_rewritten_sql`, `last_tds_batch` serve only the SQLite path |
| Oracle core | `src/py/vertebra/db/adapters/oracle/core.py::OracleCore.normalize_sql`, `::_NAMED_PARAM_RE`[^oracle-core] | Only caller is `oracle/driver.py`; deleted with it |
| MSSQL core | `src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore.normalize_for_sqlite`[^mssql-core] | Only caller is `mssql/driver.py`; deleted with it. `encode_tds_sql_batch` keeps callers in `MSSQLConfig.encode_sql_batch`, the `mssql/__init__.py` lazy export, and the codec test |
| MSSQL config | `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig.build_async_driver`[^mssql-config] | Requests the sync Cloud SQL dialer for the async driver |
| Driver bases | `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver`[^sync-base], `src/py/vertebra/db/driver/_async.py::ThreadOffloadAsyncDatabaseDriver`[^offload-base] | Concrete bases: `execute`, `execute_tuples`, `select`, `select_value`, `select_to_arrow`, `load_from_arrow` route through `execute_with_description`, which raises `NotImplementedError` |
| Consumers | `src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver`, `::SyncAdapterConfig.create_driver`[^sqlspec-adapters], `src/py/vertebra/db/registry.py::DatabaseRegistry.get_shared_driver`, `src/py/vertebra/extensions/django/db/backends/__init__.py::native_driver`, `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` | Lazy construction; each surfaces `MissingDependencyError` at driver build time |
| Tests | `test_enterprise_olap_drivers.py`[^olap-tests], `test_native_db_drivers.py`[^native-tests], `test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver`[^conformance-resolution] | Canonical module test files; no new test files |

### 1.4 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [ADR-004 optional driver wheels](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md)
- [Polyglot test matrix](../../../../knowledge/testing/polyglot-test-matrix.md)
- [python-framework skill](../../../../../skills/python-framework/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1 (`t01`, `t02`)**: Constructing `SyncOracleDriver`, `AsyncOracleDriver`, `SyncMSSQLDriver`, or `AsyncMSSQLDriver` raises `MissingDependencyError` for `vertebra._oracle` / `vertebra._mssql`; every public method the classes define calls the resolver first; no Oracle or MSSQL code path opens `sqlite3`; `OracleCore.normalize_sql` and `MSSQLCore.normalize_for_sqlite` are deleted.
- **R2 (`t02`)**: `AsyncMSSQLDriver` forwards `dialer` to its sync driver; `MSSQLConfig.build_async_driver` supplies the sync Cloud SQL dialer.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

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

`_require_mssql_native()` in `mssql/driver.py` mirrors it for `vertebra._mssql` and `vertebra-core[mssql]`. Both are module-private and excluded from `__all__`.

`SyncOracleDriver(SyncDatabaseDriver[OracleConfig])` keeps `dialect = "oracle"`, `dialect_tag = ORACLE_DIALECT_TAG`, and defines exactly:

- `__init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None` — resolver, then `super().__init__(config or OracleConfig(), broker=broker)`.
- `connect(self) -> Self`, `close(self) -> None`, `begin(self) -> None`, `commit(self) -> None`, `rollback(self) -> None`, `cancel(self) -> None`.
- `execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, **kwargs: Any) -> int`.
- `execute_tuples(...) -> list[tuple[Any, ...]]`, `execute_with_description(...) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]` with the same parameters as `execute`.
- `select(self, sql, parameters=None, *, schema_type: type[Any] | None = None, **kwargs: Any) -> list[Any]`.
- `select_to_arrow(self, sql, parameters=None, *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, **kwargs: Any) -> ArrowCursor`.
- `load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, direct_path: bool | None = None, **kwargs: Any) -> int`.

Each method body is `_require_oracle_native()` followed by `return super().<method>(...)` forwarding every argument (`load_from_arrow` discards `direct_path`). Inherited `execute_many`, `select_one`, `select_one_or_none`, `select_value`, `stream_arrow_batches`, and `transaction` route through the overridden methods.

`AsyncOracleDriver(ThreadOffloadAsyncDatabaseDriver[OracleConfig, SyncOracleDriver])` defines `__init__(self, config: OracleConfig | None = None, *, broker: Any = None) -> None` (resolver, then build `SyncOracleDriver(resolved, broker=broker)`, then `super().__init__(resolved, sync_driver, broker=sync_driver.broker)`) and `async def load_from_arrow(..., direct_path: bool | None = None, **kwargs: Any) -> int` (resolver, then `asyncio.to_thread(self._sync.load_from_arrow, ...)`). Every other async method delegates to the guarded sync driver.

`SyncMSSQLDriver(SyncDatabaseDriver[MSSQLConfig])` keeps `dialect = "mssql"`, `dialect_tag = MSSQL_DIALECT_TAG`, `__init__(self, config: MSSQLConfig | None = None, *, dialer: SyncCloudSQLDialer | None = None, broker: Any = None) -> None`, the `dialer -> SyncCloudSQLDialer | None` property, and the same guarded method set as Oracle with `load_from_arrow(self, table_name, data, *, columns=None, **kwargs) -> int`.

`AsyncMSSQLDriver(ThreadOffloadAsyncDatabaseDriver[MSSQLConfig, SyncMSSQLDriver])`:

```python
def __init__(
    self,
    config: MSSQLConfig | None = None,
    *,
    dialer: SyncCloudSQLDialer | None = None,
    broker: Any = None,
) -> None: ...

@property
def dialer(self) -> SyncCloudSQLDialer | None: ...
```

Deleted symbols: `SyncOracleDriver.{_conn, _in_transaction, _session_context, _pending_session_attrs, _last_piggyback_frame, last_piggyback_frame, set_session_attrs, _acquire_oracle_broker_lease, _release_oracle_broker_lease, _consume_piggyback}`, `AsyncOracleDriver.{last_piggyback_frame, set_session_attrs}`, `OracleCore.normalize_sql`, `oracle/core.py::_NAMED_PARAM_RE`, `SyncMSSQLDriver.{_conn, _in_transaction, _last_rewritten_sql, _last_tds_batch, last_rewritten_sql, last_tds_batch, _prepare_and_lease, _release_mssql_lease}`, `AsyncMSSQLDriver.{_dialer, last_rewritten_sql, last_tds_batch}`, `MSSQLCore.normalize_for_sqlite`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `DatabaseRegistry.get_shared_driver(OracleConfig)` → `OracleConfig.build_async_driver(registry)` → `AsyncOracleDriver.__init__` → `_require_oracle_native()` → `ensure_module("vertebra._oracle", ...)` → `import_optional` → `MissingDependencyError`.
- `DatabaseRegistry.get_shared_sync_driver(MSSQLConfig)` → `MSSQLConfig.build_sync_driver(registry)` → `registry.get_dialer(cfg, sync=True)` → `SyncMSSQLDriver(cfg, dialer=..., broker=...)` → `_require_mssql_native()`.
- `DatabaseRegistry.get_shared_driver(MSSQLConfig)` → `MSSQLConfig.build_async_driver(registry)` → `registry.get_dialer(cfg, sync=True)` → `AsyncMSSQLDriver(cfg, dialer=sync_dialer, broker=...)` → `SyncMSSQLDriver(cfg, dialer=sync_dialer, broker=...)`.
- `OracleSyncConfig().provide_session()` → `SyncAdapterConfig.create_driver()` → `DatabaseRegistry.default().get_shared_sync_driver(cfg)` → `SyncOracleDriver.__init__` → resolver.
- `OracleSyncDriver(port).apply_tenant_predicate(predicate)` → `OracleCore.build_session_context_sql(...)` → `BaseSyncDriver.execute` → `port.execute(sql, params)`; tenant behaviour is tested against an injected port, independent of `SyncOracleDriver`.
- `SyncDBAPIConnection("oracle+vtb://...", broker=b)` → `OracleConfig.build_sync_driver(DatabaseRegistry(broker=b))` → resolver.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| Module absent, sync | `SyncOracleDriver()` with `sys.modules["vertebra._oracle"] = None` | `MissingDependencyError`, `package == "vertebra-core[oracle]"` | No `CommonDriverMixin` state built |
| Module absent, async | `AsyncOracleDriver()` / `AsyncMSSQLDriver()` | `MissingDependencyError` before the sync driver is built | Never `TypeError` |
| Module present, query | Stub `ModuleType("vertebra._oracle")` in `sys.modules`; `SyncOracleDriver().select("SELECT 42 AS x FROM DUAL")` | `NotImplementedError` from `SyncDatabaseDriver.execute_with_description` | No rows from any stand-in |
| Module removed after construction | Construct under stub, then set `sys.modules[...] = None` and `reset_dependency_cache(...)`; call `execute("SELECT 1 FROM DUAL")` | `MissingDependencyError` | Per-method guard |
| Module cache leak | Stub cached by `import_optional` | Tests call `reset_dependency_cache(name)` in `finally` | Later tests see a cold cache |
| Driver module import | `import vertebra.db.adapters.oracle.driver` | No resolver call | `test_registry_parse_url_executes_zero_eager_driver_imports` stays green |
| SQLSpec config | `OracleSyncConfig()`; `MSSQLAsyncConfig()` | Construct; `is_async` flags correct; `provide_session()` raises `MissingDependencyError` | Config import safety |
| Registry caching | `get_shared_driver(OracleConfig())` raises | Nothing cached under the pool key | Retry after install builds a fresh driver |
| SQLAlchemy `+vtb` | `SyncDBAPIConnection("oracle+vtb://localhost:1521/FREEPDB1", broker=_MockBroker())` | `MissingDependencyError` from the constructor | No SQLite substitution for Oracle |
| MSSQL dialer | `AsyncMSSQLDriver(MSSQLConfig(), dialer=d)` under stub | `drv.sync_driver.dialer is d` and `drv.dialer is d` | I/O thread holds the dialer |
| Registry MSSQL dialer | `MSSQLConfig(cloud_sql_instance="proj:us-central1:inst")` under stub | Sync and async drivers both hold `reg.get_dialer(csql_cfg, sync=True)` | One dialer type per I/O thread |
| Tenant context | `OracleSyncDriver(port).apply_tenant_predicate(TenantPredicate("o", "acme"))` | Returns `"o.tenant_id = :tenant_id"`; port receives the `DBMS_SESSION.SET_CONTEXT` call | Behaviour owned by `OracleCore` and the SQLSpec adapter |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given no `vertebra._oracle`, when a caller runs `DatabaseRegistry().get_shared_driver(OracleConfig.from_url("oracle://u:p@db:1521/svc"))`, then `MissingDependencyError` is raised with `package == "vertebra-core[oracle]"` and `hint == "Install it with: uv add vertebra-core[oracle]"`.
- Given no `vertebra._mssql`, when `AsyncMSSQLDriver()` runs, then `MissingDependencyError` is raised and no `TypeError` occurs.
- Given a stub `vertebra._mssql` and `d = SyncCloudSQLDialer(CloudSQLConfig(instance_connection_name="proj:us-central1:inst"))`, when `AsyncMSSQLDriver(MSSQLConfig(), dialer=d)` runs, then `drv.sync_driver.dialer is d`.
- Given a stub `vertebra._oracle`, when `SyncOracleDriver().select("SELECT 42 AS x FROM DUAL")` runs, then `NotImplementedError` is raised.

### 2.6 Alternatives Considered

| Decision | Chosen Seam | Rejected Alternative | Reason |
| --- | --- | --- | --- |
| Post-guard method body | `_require_<engine>_native()` then `super().<method>(...)` | Raise a new error after the guard | Base `execute_with_description` already raises `NotImplementedError`; no new error type or message to maintain |
| Method set | Override the public methods the stand-in defined plus `connect` | Guard only `__init__` | Root §2.2 locks per-method guarding; inherited helpers route through overridden methods |
| Piggyback and broker statement-cache state | Delete from the drivers | Keep `set_session_attrs` / `last_rewritten_sql` with guards | With no execute path the state is never transmitted; `OracleSessionAttrs.encode_ttc_piggyback` and `MSSQLCore.rewrite_placeholders` keep codec coverage |
| MSSQL async dialer type | `SyncCloudSQLDialer` forwarded to the sync driver; config requests `sync=True` | Forward `AsyncCloudSQLDialer` and widen the sync parameter | `ThreadOffloadAsyncDatabaseDriver` runs I/O in the sync driver on a worker thread, which cannot await an async dialer |
| Missing-module simulation | `monkeypatch.setitem(sys.modules, name, None)` plus `reset_dependency_cache` | Patch the private resolver | Exercises the real `ensure_module` path, matching the BigQuery/Spanner test |
| Module-present simulation | `monkeypatch.setitem(sys.modules, name, ModuleType(name))` | Injectable connection port on the driver | Root §2.6 rejects a fake-only port; the stub only satisfies the import |
| Tenant tests | Inject a port into `OracleSyncDriver` / `OracleAsyncDriver` | Delete Oracle tenant coverage | The adapter's `apply_tenant_predicate` stays live code and keeps a behavioural test |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[2/2 files documented in ch01]` — driver module docstrings only; user docs and OKF chapters are documented in ch11.
- `[x]` `src/py/vertebra/db/adapters/oracle/driver.py` — Module and class docstrings state that the driver requires `vertebra._oracle` from `vertebra-core[oracle]` and raises `MissingDependencyError` without it (`t01`).
- `[x]` `src/py/vertebra/db/adapters/mssql/driver.py` — Module and class docstrings state the `vertebra._mssql` requirement; `AsyncMSSQLDriver` documents that its dialer runs on the sync driver (`t02`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R1 (Oracle) | [t01](tasks/t01-oracle-native-guard.md) | `behavior_tdd` | `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_oracle_drivers_raise_missing_dependency_when_native_module_absent` |
| R1 (MSSQL) | [t02](tasks/t02-mssql-native-guard.md) | `behavior_tdd` | `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_mssql_drivers_raise_missing_dependency_when_native_module_absent` |
| R2 | [t02](tasks/t02-mssql-native-guard.md) | `behavior_tdd` | `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_async_mssql_driver_forwards_sync_dialer_to_sync_driver`, `src/py/tests/unit/db/test_native_db_drivers.py::test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error` |
| Oracle driver docstrings | t01 | `documentation_validation` | `grep -c "vertebra-core\[oracle\]" src/py/vertebra/db/adapters/oracle/driver.py` prints `>= 4`; `grep -n "Thin\|SHM statement cache\|sqlite" src/py/vertebra/db/adapters/oracle/driver.py` prints nothing |
| MSSQL driver docstrings | t02 | `documentation_validation` | `grep -c "vertebra-core\[mssql\]" src/py/vertebra/db/adapters/mssql/driver.py` prints `>= 4`; `grep -n "TDS 7.4\|SHM statement cache\|sqlite" src/py/vertebra/db/adapters/mssql/driver.py` prints nothing |

## 5. Implementation Plan

- [x] [d7a629b1] driver-fail-loud:ch01:t01 — Oracle drivers resolve `vertebra._oracle` on every entry point with the SQLite stand-in and `normalize_sql` deleted (tasks/t01-oracle-native-guard.md)
- [x] [d7a629b1] driver-fail-loud:ch01:t02 — MSSQL drivers resolve `vertebra._mssql` on every entry point, `normalize_for_sqlite` deleted, and `AsyncMSSQLDriver` forwards its sync dialer (tasks/t02-mssql-native-guard.md)
- **Chapter Recovery Checkpoint**: `uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_pg_gateway.py src/py/tests/unit/extensions/test_django_plugin.py "src/py/tests/integration/db/test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver" -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/oracle src/py/vertebra/db/adapters/mssql && uv run --no-sync mypy src/py/vertebra/db/adapters/oracle src/py/vertebra/db/adapters/mssql` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ d7a629b1`; `state: completed`, `depends_on_chapters: []`.
- **Remaining Tasks in Chapter**: none (`2/2` completed).
- **Documentation & OKF Sync**: `[2/2 files documented in ch01]`.
- **Handoffs to ch05**: after `t02`, `MSSQLCore.encode_tds_sql_batch` keeps callers in `MSSQLConfig.encode_sql_batch`, the `mssql/__init__.py` lazy export, and `test_mssql_tds_framing_and_placeholder_rewriter`; ch05 removes them with the binding. `OracleCore.build_session_context_sql` still says "or the Oracle stand-in" in its docstring.
- **Active Invariants**: registry and SQLSpec adapter imports stay free of resolver calls; tests restore the dependency cache in `finally`; no new test files.

[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising MissingDependencyError.
[^oracle-driver]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — Oracle sync and async drivers backed by sqlite3.
[^mssql-driver]: `src/py/vertebra/db/adapters/mssql/driver.py::SyncMSSQLDriver` — MSSQL sync and async drivers backed by sqlite3.
[^oracle-core]: `src/py/vertebra/db/adapters/oracle/core.py::OracleCore` — Oracle codecs and the SQLite-only normalize_sql helper.
[^mssql-core]: `src/py/vertebra/db/adapters/mssql/core.py::MSSQLCore` — TDS codecs, placeholder rewriter, and the SQLite-only normalize_for_sqlite helper.
[^mssql-config]: `src/py/vertebra/db/adapters/mssql/config.py::MSSQLConfig` — MSSQL config building sync and async drivers with Cloud SQL dialers.
[^sync-base]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver` — Base sync driver whose execute paths route through execute_with_description.
[^offload-base]: `src/py/vertebra/db/driver/_async.py::ThreadOffloadAsyncDatabaseDriver` — Async driver delegating every call to a paired sync driver on a worker thread.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::OracleSyncDriver` — SQLSpec Oracle and MSSQL adapters and session providers.
[^olap-tests]: `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — Oracle, MSSQL, and DuckDB driver and codec unit tests.
[^native-tests]: `src/py/tests/unit/db/test_native_db_drivers.py::test_registry_get_dialer_sync_async_caching_mssql_wiring_and_provide_driver_type_error` — Unified driver contract and registry dialer wiring tests.
[^conformance-resolution]: `src/py/tests/integration/db/test_driver_conformance.py::test_engine_driver_resolution_and_provide_driver` — Conformance driver class resolution and provide_driver test.
