---
type: Spec
flow_id: driver-fail-loud
parent_flow: driver-fail-loud
chapter_id: ch02-duckdb-sqlalchemy-queue-fail-loud
title: "Chapter 02: DuckDB, SQLAlchemy, and Oracle Queue Fail-Loud Paths"
description: DuckDB requires the duckdb package, installed through a GIL-only duckdb dependency group with DuckDB tests skipping on free-threaded interpreters, the SQLAlchemy DB-API raises InterfaceError on unparsable URLs, and OracleQueueBackend raises instead of substituting an in-memory executor.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch01-oracle-mssql-fail-loud
replan_trigger: null
current_task: "driver-fail-loud:ch02:t01"
plan_revision: 1
plan_commit: 0acdf576
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-07T00:00:00Z"
updated_at: "2026-11-07T00:00:00Z"
generated:
  by: flow/0.28.0
  at: "2026-11-07T00:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T00:00:00Z"
    commit: 0acdf576
sources:
  - id: duckdb-driver
    resource: src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver
    title: DuckDB driver with sqlite3 fallback and _using_native_duckdb branches
  - id: duckdb-config
    resource: src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig.resolved_path
    title: DuckDB path resolution passed to duckdb.connect
  - id: module-loader
    resource: src/py/vertebra/utils/module_loader.py::ensure_module
    title: Optional module loader raising MissingDependencyError
  - id: missing-dep
    resource: src/py/vertebra/_typing.py::MissingDependencyError
    title: Missing dependency error, package, install_package, and hint
  - id: dep-groups
    resource: pyproject.toml::dependency-groups
    title: test and bench dependency groups; duckdb only in bench
  - id: makefile
    resource: Makefile::py-test
    title: install, py-test, coverage, and upgrade targets that sync and run the Python suite
  - id: ci-test-python
    resource: .github/workflows/ci.yml::test-python
    title: CI Python matrix including 3.14t and 3.15t running make py-test
  - id: sqla-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection
    title: Sync and async DB-API connections with SQLite parse fallback
  - id: sqla-errors
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::InterfaceError
    title: PEP 249 InterfaceError exposed as SyncDBAPIModule.InterfaceError and AsyncDBAPIModule.InterfaceError
  - id: registry-parse
    resource: src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url
    title: URL parser raising ValueError for unsupported schemes
  - id: oracle-queue
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: Oracle queue backend whose _build_executor always returns the in-memory executor
  - id: queue-factory
    resource: src/py/vertebra/framework/queues/base.py::QueueService._build_backend
    title: oracle:// queue URL routes to OracleQueueBackend(config) without an executor
  - id: queue-tests
    resource: src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url
    title: Queue contract tests that construct OracleQueueBackend on the in-memory executor
tags:
  - database
  - duckdb
  - sqlalchemy
  - queues
  - fail-loud
---

# Chapter 02: DuckDB, SQLAlchemy, and Oracle Queue Fail-Loud Paths

<!-- truth: start -->
- **DuckDB requires `duckdb`**: `SyncDuckDBDriver.__init__` resolves the module through `ensure_module("duckdb", package="duckdb", install_package="duckdb")`; the driver module imports no `sqlite3` and carries no `_using_native_duckdb` flag.[^duckdb-driver][^module-loader]
- **DuckDB has its own dependency group**: `pyproject.toml` declares `duckdb = ["duckdb>=1.0"]`, included by `bench` and not by `dev`; `make install` syncs it and `make py-test` / `make coverage` add `--group duckdb` only on GIL interpreters, so the CI `3.12`–`3.15` jobs install it and `3.14t` / `3.15t` do not.[^dep-groups][^makefile][^ci-test-python]
- **DuckDB tests skip without `duckdb`**: every test that constructs a DuckDB driver calls `pytest.importorskip("duckdb", reason=...)` and runs DuckDB-only SQL when the package is present.[^duckdb-driver]
- **DuckDB errors propagate**: bootstrap `SET` statements and the GCS `CREATE OR REPLACE SECRET` statement run without exception suppression, so `duckdb.Error` reaches the caller.[^duckdb-driver]
- **No SQLAlchemy SQLite substitution**: `SyncDBAPIConnection.__init__` and `AsyncDBAPIConnection.__init__` raise `InterfaceError` chained `from` the `ValueError` that `DatabaseRegistry.parse_url` raises; the raw `sqlite3` connection for genuine SQLite URLs is unchanged.[^sqla-dbapi][^sqla-errors][^registry-parse]
- **Oracle queue requires an executor**: `OracleQueueBackend` without `executor=` raises `MissingDependencyError` naming `oracledb` when the package is absent and `ImproperConfigurationError` when it is present; `_InMemoryOracleQueueExecutor` lives only in the queue test module.[^oracle-queue][^queue-factory][^queue-tests]
- **Registry import stays lazy**: no module-level `duckdb` or `oracledb` import is added; resolution happens in constructors.[^module-loader]
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

`SyncDuckDBDriver` imports `duckdb` through `import_optional` and opens `sqlite3.connect` when the package is absent; every write method branches on `_using_native_duckdb`.[^duckdb-driver] `duckdb` sits only in the `bench` dependency group, so every DuckDB unit test runs on SQLite today.[^dep-groups] `duckdb` publishes only `cp312-abi3` wheels, which free-threaded CPython cannot load, and the CI `test-python` matrix includes `3.14t` and `3.15t`.[^ci-test-python] `SyncDBAPIConnection` and `AsyncDBAPIConnection` catch any exception from `DatabaseRegistry.parse_url` and substitute `SQLiteConfig(":memory:")`, so a typo in a URL produces a working in-memory SQLite connection.[^sqla-dbapi] `OracleQueueBackend._build_executor` returns `_InMemoryOracleQueueExecutor` whether or not `oracledb` is installed, so `QueueConfig(backend_url="oracle://...")` silently runs an in-process dictionary queue.[^oracle-queue][^queue-factory]

This chapter makes all three paths raise: DuckDB raises `MissingDependencyError`, the DB-API raises its PEP 249 `InterfaceError`, and the Oracle queue raises `MissingDependencyError` or `ImproperConfigurationError`. A dedicated `duckdb` dependency group installs the package on GIL interpreters; DuckDB tests run on the real engine there and skip on free-threaded interpreters. Oracle queue tests move to a test-owned executor fake.

### 1.2 Non-Goals

- The raw `sqlite3` connection (`SyncDBAPIConnection._sqlite_conn`) opened for genuine SQLite URLs and DB-API error mapping beyond the parse fallback (owned by `sqlalchemy-and-django-integration`).
- A live Oracle queue executor for `OracleQueueBackend` (owned by `queues-tasks-and-channels`, which builds the durable `OracleQueueStore` and `PostgresQueueStore` queue stores).
- In-memory fallback executors in `framework/queues/postgres.py`, `framework/queues/redis.py`, and `framework/queues/spanner.py` (owned by [ch08-queue-backend-fail-loud](../ch08-queue-backend-fail-loud/spec.md)).
- A cached free-threaded `duckdb` source build (follow-up once DuckDB supports free-threading) and the existing `bench-ft` target, which syncs `--group bench` on a free-threaded interpreter.
- A native `vertebra._duckdb` engine and the `duckdb` optional-dependency extra (owned by `native-duckdb-driver` / `native-embedded-and-cache-drivers`).
- Conformance native-module gating (owned by `ch07-install-hints-and-conformance`).
- User docs and OKF knowledge prose (owned by `ch09-docs-and-okf`; queue docs by `ch10-queue-docs-and-knowledge`).

### 1.3 Upstream Execution Baseline

- Branch `feat/storage-updates` at `0acdf576`; parent flow [`driver-fail-loud`](../../spec.md).
- Depends on `ch01-oracle-mssql-fail-loud`: both chapters edit `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, whose identifier-rejection and DB-API broker tests parametrize Oracle, MSSQL, and DuckDB together.
- `uv.lock` already resolves `duckdb` `2.0.0` through the `bench` group; `oracledb` is not in `uv.lock`.

### 1.4 Code Analysis & Seam Summary

| Area | Files Examined (`path/to/file::Symbol`) | Public Seam, Dependency Category & Integration |
| --- | --- | --- |
| DuckDB driver | `duckdb/driver.py::SyncDuckDBDriver`[^duckdb-driver], `duckdb/config.py::DuckDBConfig.resolved_path`[^duckdb-config] | External boundary (`duckdb` package); built lazily by `DatabaseRegistry`, SQLSpec `DuckDB*Config`, and `AsyncDuckDBDriver` |
| Optional modules | `utils/module_loader.py::ensure_module`[^module-loader], `_typing.py::MissingDependencyError`[^missing-dep] | In-process; `sys.modules[name] = None` plus `reset_dependency_cache(name)` simulates absence |
| Dependency groups | `pyproject.toml` `[dependency-groups]`[^dep-groups], `Makefile::install`, `py-test`, `coverage`, `upgrade`[^makefile], `.github/workflows/ci.yml::test-python`[^ci-test-python] | `dev` includes `test`; `make py-test` runs `uv run pytest`, which syncs `dev`; CI runs `make py-test` per matrix interpreter |
| SQLAlchemy DB-API | `extensions/sqlalchemy/dialects.py::SyncDBAPIConnection`, `AsyncDBAPIConnection`[^sqla-dbapi], `InterfaceError`[^sqla-errors] | In-process over `DatabaseRegistry.parse_url`[^registry-parse]; reached through `SyncDBAPIModule.connect` / `AsyncDBAPIModule.connect` |
| Oracle queue | `framework/queues/oracle.py::OracleQueueBackend`, `_InMemoryOracleQueueExecutor`[^oracle-queue], `base.py::QueueService._build_backend`[^queue-factory] | External boundary (`oracledb`); `OracleQueueExecutor` protocol is the injected seam |
| Tests | `test_enterprise_olap_drivers.py`, `test_gcs_token_refresh.py`, `test_native_db_drivers.py`, `test_driver_conformance.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, `test_distributed_queues.py`[^queue-tests] | Canonical module test files; no new test modules |

### 1.5 Relevant OKF Knowledge Chapters & Skills

- [Database drivers and dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md)
- [SQLSpec, Advanced Alchemy, and dialects](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md)
- [Distributed queues and email](../../../../knowledge/domains/framework/distributed-queues-and-email.md)
- [Benchmarks and free-threaded testing](../../../../knowledge/testing/benchmarks-and-free-threaded.md)
- [Project workflow](../../../../knowledge/workflow.md)
- [`python-framework` skill](../../../../../skills/python-framework/SKILL.md)

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Functional Requirements

- **R3**: `SyncDuckDBDriver` and `AsyncDuckDBDriver` raise `MissingDependencyError` when `duckdb` is absent. `duckdb` lives in its own `[dependency-groups].duckdb` group, installed by `make install` and by `make py-test` / `make coverage` on GIL interpreters only; DuckDB tests run against it there and skip through `pytest.importorskip("duckdb", reason=...)` on free-threaded interpreters.
- **R3.1**: The DuckDB driver module contains no `sqlite3` import, no `_using_native_duckdb` attribute, and no exception suppression around DuckDB statements.
- **R4**: `SyncDBAPIConnection` and `AsyncDBAPIConnection` raise the DB-API `InterfaceError`, chained from the parse exception, when `DatabaseRegistry.parse_url` fails; an injected `driver=` does not bypass the check.
- **R5**: `OracleQueueBackend` raises `MissingDependencyError` (`package="oracledb"`, `install_package="oracledb"`) when `oracledb` is absent and no `executor` is supplied.
- **R5.1**: With `oracledb` installed and no `executor`, `OracleQueueBackend` raises `ImproperConfigurationError`; `_InMemoryOracleQueueExecutor` and `_build_executor` are deleted from production.

### 2.2 Full Public Type, Struct, Schema & Signature Contracts

```python
class SyncDuckDBDriver(SyncDatabaseDriver[DuckDBConfig]):
    def __init__(self, config: DuckDBConfig | None = None, *, broker: Any = None) -> None:
        """Open a DuckDB connection and run bootstrap statements.

        Raises:
            MissingDependencyError: When the ``duckdb`` package is not installed.
            duckdb.Error: When a bootstrap statement fails.
        """
```

`__init__` calls `ensure_module("duckdb", package="duckdb", install_package="duckdb")` before `super().__init__`. `AsyncDuckDBDriver.__init__` keeps its signature and inherits the error through its `SyncDuckDBDriver` construction.

```python
class SyncDBAPIConnection:
    def __init__(
        self,
        url: str = "sqlite+vtb:///:memory:",
        *,
        driver: Any = None,
        broker: Any = None,
        share_pool: bool = True,
        database: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Raises:
            InterfaceError: When ``url`` cannot be parsed into a Vertebra database configuration.
        """
```

`AsyncDBAPIConnection.__init__` keeps its signature (`url: str = "sqlite+vtb_async:///:memory:"`) and the same `Raises:` contract. Both wrap the parse in `except ValueError as exc: raise InterfaceError(f"Cannot parse Vertebra database URL: {exc}") from exc`.

```python
class OracleQueueBackend:
    def __init__(
        self,
        config: QueueConfig | None = None,
        *,
        oracle_config: Any = None,
        executor: OracleQueueExecutor | None = None,
    ) -> None:
        """Raises:
            MissingDependencyError: When ``executor`` is None and ``oracledb`` is not installed.
            ImproperConfigurationError: When ``executor`` is None and ``oracledb`` is installed.
        """
```

`pyproject.toml` `[dependency-groups]`: `duckdb = ["duckdb>=1.0"]`; `bench` replaces its `"duckdb>=1.0"` entry with `{ include-group = "duckdb" }`; `dev` is unchanged; `uv.lock` is regenerated with `uv lock`.

`Makefile`:

```make
PY_GIL_DISABLED = $(shell uv run --no-sync python -c "import sysconfig; print(int(bool(sysconfig.get_config_var('Py_GIL_DISABLED'))))" 2>/dev/null)
PY_TEST_GROUPS = $(if $(filter 1,$(PY_GIL_DISABLED)),,--group duckdb)
```

`install` runs `uv sync --all-extras --dev --group doc --group duckdb`; `py-test` and `coverage` run `uv run $(PY_TEST_GROUPS) pytest ...`; `upgrade` syncs with `$(PY_TEST_GROUPS)`. `.github/workflows/ci.yml` is unchanged: its `test-python` job runs `make py-test` on each interpreter.

Test guard in each module with DuckDB-executing tests: `_DUCKDB_SKIP_REASON = "duckdb publishes no free-threaded wheel; DuckDB tests run on GIL interpreters"`, passed to `pytest.importorskip("duckdb", reason=_DUCKDB_SKIP_REASON)`.

### 2.3 Caller -> Seam -> Collaborator Integration Map

- `make py-test` → `PY_TEST_GROUPS` (empty when `Py_GIL_DISABLED == 1`) → `uv run --group duckdb pytest` → DuckDB tests run; on free-threaded interpreters `pytest.importorskip("duckdb")` → SKIPPED.
- `DatabaseRegistry.get_shared_sync_driver(DuckDBConfig)` / SQLSpec `DuckDBSyncConfig.provide_session` → `SyncDuckDBDriver.__init__` → `ensure_module("duckdb")` → `duckdb.connect(config.resolved_path(), read_only=config.read_only)` or `MissingDependencyError`.
- `AsyncDuckDBDriver.__init__` → `SyncDuckDBDriver.__init__` → same guard.
- `SyncDBAPIModule.connect(url)` → `SyncDBAPIConnection.__init__` → `DatabaseRegistry.parse_url` → `ValueError` → `InterfaceError` `from` it.
- `AsyncDBAPIModule.connect(url)` → `AsyncDBAPIConnection.__init__` → same path.
- `QueueService(QueueConfig(backend_url="oracle://..."))` → `QueueService._build_backend` → `OracleQueueBackend(config)` → `ensure_module("oracledb")` → `MissingDependencyError`, else `ImproperConfigurationError`.
- `OracleQueueBackend(config, executor=fake)` → binds the injected `OracleQueueExecutor` with no module resolution.

### 2.4 Non-Functional Constraints & Edge-Case / Failure-Mode Matrix

| Scenario / Boundary | Trigger / Input | Expected Behavior / Error Contract | Invariant Preserved |
| --- | --- | --- | --- |
| DuckDB absent (sync) | `sys.modules["duckdb"] = None`; `SyncDuckDBDriver()` | `MissingDependencyError`; `.package == "duckdb"`, `.install_package == "duckdb"`, `.hint == "Install it with: uv add duckdb"` | No `sqlite3` connection opened |
| DuckDB absent (async) | Same; `AsyncDuckDBDriver()` | Same error before thread-offload state exists | No `TypeError` |
| DuckDB tests, free-threaded | `3.14t` / `3.15t` CI job; `PY_TEST_GROUPS` empty | DuckDB tests SKIPPED with `_DUCKDB_SKIP_REASON`; non-DuckDB parameters run | No sdist build in CI |
| DuckDB tests, GIL | `3.12`–`3.15` CI job or `make install` env | `duckdb` installed; DuckDB tests run on DuckDB | Real engine coverage |
| DuckDB engine identity | `SyncDuckDBDriver().select("SELECT list_value(1, 2, 3) AS xs")` | `[{"xs": [1, 2, 3]}]` | SQLite cannot execute `list_value` |
| DuckDB commit without transaction | `commit()` with no `begin()` | Returns without calling `self._conn.commit()` | `_in_transaction` resets to `False` |
| DuckDB rollback without transaction | `rollback()` with no `begin()` | Returns; no `TransactionException` | `_in_transaction` resets to `False` |
| DuckDB bad bootstrap | A bootstrap statement DuckDB rejects | `duckdb.Error` propagates from `__init__` | No silent half-configured session |
| GCS secret rotation | `DuckDBConfig(gcs_token_cache=cache)`, first `select` | Executes `CREATE OR REPLACE SECRET vertebra_gcs_adc (TYPE GCS, BEARER_TOKEN '...')` on DuckDB; `applied_token_generation == 1` | Requires the DuckDB `httpfs` extension (auto-loaded) |
| Registry import | `import vertebra.db.registry` | No `duckdb` import | `test_registry_parse_url_executes_zero_eager_driver_imports` stays green |
| Unsupported DB-API scheme | `SyncDBAPIConnection("nonsense+vtb://host/db", share_pool=False)` | `InterfaceError("Cannot parse Vertebra database URL: Unsupported database URL scheme: 'nonsense+vtb'")`; `__cause__` is `ValueError` | No SQLite substitution |
| Empty DB-API URL | `SyncDBAPIConnection("", share_pool=False)` | `InterfaceError`; `__cause__` is `ValueError` | No SQLite substitution |
| Injected driver with bad URL | `SyncDBAPIConnection("nonsense+vtb://host/db", driver=SyncSQLiteDriver(), share_pool=False)` | `InterfaceError` | Config resolution is mandatory |
| Genuine SQLite URL | `SyncDBAPIConnection("sqlite+vtb:///:memory:", share_pool=False)` | Unchanged: `_sqlite_conn` opened | Out-of-scope SQLite path untouched |
| Oracle queue, `oracledb` absent | `sys.modules["oracledb"] = None`; `OracleQueueBackend(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))` | `MissingDependencyError`; `.package == "oracledb"`, `.install_package == "oracledb"` | No in-memory executor |
| Oracle queue, `oracledb` present, no executor | `sys.modules["oracledb"] = types.ModuleType("oracledb")`; same constructor | `ImproperConfigurationError` matching `"requires an OracleQueueExecutor"` | No in-memory executor |
| Oracle queue via `QueueService` | `QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))` with `oracledb` absent | `MissingDependencyError` | `memory://` queue backend unchanged |
| Oracle queue with injected executor | `OracleQueueBackend(cfg, executor=_InMemoryOracleQueueExecutor())` (test module) | Constructs; queue contract tests pass | `OracleQueueExecutor` seam unchanged |

### 2.5 Acceptance Scenarios (Given / When / Then)

- Given `duckdb` installed, when `SyncDuckDBDriver(DuckDBConfig()).select("select 42 as x")` runs, then it returns `[{"x": 42}]` from DuckDB.
- Given `duckdb` absent, when `AsyncDuckDBDriver()` is constructed, then `MissingDependencyError` names `duckdb` and hints `uv add duckdb`.
- Given `SyncDBAPIModule()`, when `.connect("nonsense+vtb://host/db")` runs, then `InterfaceError` is raised with a `ValueError` cause.
- Given `oracledb` absent, when `QueueService(QueueConfig(backend_url="oracle://localhost:1521/FREEPDB1"))` is constructed, then `MissingDependencyError` names `oracledb`.

### 2.6 Alternatives Considered

| Alternative | Depth / Locality / Seam | Outcome |
| --- | --- | --- |
| DuckDB: raise lazily on first query (`_ensure_conn()`) | Defers failure; driver holds a half-built state and every method needs a guard | Rejected: construction is the earliest point and DuckDB has no non-DuckDB path |
| DuckDB in the `test` group unconditionally | Every interpreter installs `duckdb` | Rejected: `3.14t` / `3.15t` have no wheel and the sdist build fails the CI matrix |
| `duckdb` in `test` with a free-threading environment marker | Single group, no Makefile logic | Rejected: no PEP 508 marker distinguishes free-threaded builds in the supported uv versions |
| Dedicated `duckdb` group installed on GIL interpreters, `pytest.importorskip` elsewhere | Makefile detects `Py_GIL_DISABLED`; tests skip only where the package cannot install | Chosen |
| DuckDB: keep `contextlib.suppress(RuntimeError, ValueError, OSError)` | `duckdb.Error` derives from `Exception`, not those classes, so the suppression never catches DuckDB failures | Rejected: dead code that implies tolerance the driver does not have |
| SQLAlchemy: catch `Exception` and re-raise `InterfaceError` | Wraps `TypeError` / `KeyError` bugs as interface errors | Rejected: `parse_url` and `from_url` signal bad URLs with `ValueError` (including `ImproperConfigurationError`); other exceptions propagate unchanged |
| SQLAlchemy: raise `ProgrammingError` | PEP 249 reserves `ProgrammingError` for SQL and parameter errors | Rejected: a bad connection target is an interface error |
| Oracle queue: build a live `oracledb` executor now | ~150 lines of pool and cursor code with no Oracle test container in unit scope | Rejected: the durable Oracle queue store is owned by `queues-tasks-and-channels` (`OracleQueueStore`) |
| Oracle queue: keep the in-memory executor behind a `testing=True` flag | Preserves a production fake | Rejected: fakes live in tests; the `executor=` seam already supports injection |
| Oracle queue: `NotImplementedError` when `oracledb` is present | Callers' `except ValueError` configuration handling does not catch it | Rejected: missing `executor=` is a configuration error |

## 3. Documentation & OKF Knowledge Plan

- **Progress**: `[0/3 files documented]`
- `[ ]` `src/py/vertebra/db/adapters/duckdb/driver.py` — `SyncDuckDBDriver.__init__` Google docstring with `Raises:` (t04).
- `[ ]` `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncDBAPIConnection.__init__` / `AsyncDBAPIConnection.__init__` `Raises: InterfaceError` (t02).
- `[ ]` `src/py/vertebra/framework/queues/oracle.py` — `OracleQueueBackend.__init__` `Raises:` and class docstring naming the required `executor` (t03).
- User docs and OKF prose (`docs/usage/databases.md` DuckDB requirement, `docs/usage/queues-and-realtime.md` Oracle row, `knowledge/domains/framework/distributed-queues-and-email.md` Oracle executor requirement) are handed to `ch09-docs-and-okf` (DuckDB) and `ch10-queue-docs-and-knowledge` (queues).

## 4. Requirement-to-Task Traceability Matrix

| Requirement / Doc Target | Owning Chapter / Task | Verification Strategy | Canonical Test / Doc Gate |
| --- | --- | --- | --- |
| R3 (dependency group, Makefile) | `ch02 / t01` | `refactor_verification` | `uv lock --check`; `make -n py-test PY_GIL_DISABLED=0` and `PY_GIL_DISABLED=1` group checks |
| R3, R3.1 | `ch02 / t04` | `behavior_tdd` | `src/py/tests/unit/db/test_enterprise_olap_drivers.py::test_duckdb_drivers_raise_missing_dependency_when_duckdb_absent`, `::test_duckdb_driver_executes_on_duckdb_engine`; `src/py/tests/unit/storage/test_gcs_token_refresh.py::test_duckdb_multi_session_token_generation_secret_rotation` |
| R4 | `ch02 / t02` | `behavior_tdd` | `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py::test_sync_dbapi_connection_rejects_unparsable_url`, `::test_async_dbapi_connection_rejects_unparsable_url` |
| R5, R5.1 | `ch02 / t03` | `behavior_tdd` | `src/py/tests/unit/framework/test_distributed_queues.py::test_oracle_queue_backend_requires_oracledb_and_executor` |

## 5. Implementation Plan

- [ ] driver-fail-loud:ch02:t01 — `duckdb` dependency group installed on GIL interpreters by `make install`, `make py-test`, and `make coverage` (tasks/t01-duckdb-dependency-group.md)
- [ ] driver-fail-loud:ch02:t02 — DB-API connections raise `InterfaceError` on unparsable URLs (tasks/t02-sqlalchemy-parse-error.md)
- [ ] driver-fail-loud:ch02:t03 — `OracleQueueBackend` requires `oracledb` and an injected executor (tasks/t03-oracle-queue-requires-oracledb.md)
- [ ] driver-fail-loud:ch02:t04 — DuckDB driver requires `duckdb`; DuckDB tests skip through `pytest.importorskip` without it (tasks/t04-duckdb-requires-duckdb.md)
- **Chapter / Phase Recovery Checkpoint**: `uv run ruff check src/py/vertebra/db/adapters/duckdb/driver.py src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/vertebra/framework/queues/oracle.py && uv run mypy src/py/vertebra/db/adapters/duckdb/driver.py src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/vertebra/framework/queues/oracle.py && make py-test`

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline (`HEAD`)**: `0acdf576` — chapter planned; `0/4` tasks completed.
- **Active Task**: `driver-fail-loud:ch02:t01`.
- **Remaining Plan**: t01, t02, t03, t04; t04 depends on t01; t02 and t03 are independent of t01, t04, and each other.
- **Documentation & OKF Sync**: `[0/3 files documented]`; user docs and OKF prose go to `ch09-docs-and-okf` and queue docs to `ch10-queue-docs-and-knowledge`.
- **Active Invariants & Triggers**: registry import stays free of `duckdb` / `oracledb`; the SQLite `_sqlite_conn` path and `MemoryQueueBackend` are untouched; free-threaded interpreters never install `duckdb`.

[^duckdb-driver]: `src/py/vertebra/db/adapters/duckdb/driver.py::SyncDuckDBDriver` — DuckDB driver with sqlite3 fallback and `_using_native_duckdb` branches.
[^duckdb-config]: `src/py/vertebra/db/adapters/duckdb/config.py::DuckDBConfig.resolved_path` — DuckDB path resolution passed to `duckdb.connect`.
[^module-loader]: `src/py/vertebra/utils/module_loader.py::ensure_module` — Optional module loader raising `MissingDependencyError`.
[^missing-dep]: `src/py/vertebra/_typing.py::MissingDependencyError` — Missing dependency error, package, install_package, and hint.
[^dep-groups]: `pyproject.toml::dependency-groups` — `test` and `bench` dependency groups; `duckdb` only in `bench`.
[^makefile]: `Makefile::py-test` — install, py-test, coverage, and upgrade targets that sync and run the Python suite.
[^ci-test-python]: `.github/workflows/ci.yml::test-python` — CI Python matrix including 3.14t and 3.15t running `make py-test`.
[^sqla-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIConnection` — Sync and async DB-API connections with SQLite parse fallback.
[^sqla-errors]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::InterfaceError` — PEP 249 `InterfaceError` exposed as `SyncDBAPIModule.InterfaceError` and `AsyncDBAPIModule.InterfaceError`.
[^registry-parse]: `src/py/vertebra/db/registry.py::DatabaseRegistry.parse_url` — URL parser raising `ValueError` for unsupported schemes.
[^oracle-queue]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend whose `_build_executor` always returns the in-memory executor.
[^queue-factory]: `src/py/vertebra/framework/queues/base.py::QueueService._build_backend` — `oracle://` queue URL routes to `OracleQueueBackend(config)` without an executor.
[^queue-tests]: `src/py/tests/unit/framework/test_distributed_queues.py::_create_backend_by_url` — Queue contract tests that construct `OracleQueueBackend` on the in-memory executor.
