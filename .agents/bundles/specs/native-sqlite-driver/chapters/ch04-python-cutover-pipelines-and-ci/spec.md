---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch04-python-cutover-pipelines-and-ci
title: "Chapter 04: Python SQLite Adapter Cutover, Delayed-Commit Pipelines, Backup Hooks, and CI Gate"
description: "Fix SQLiteCore.initialization_pragmas, delete legacy _vertebra sqlite binding, add delayed-commit SyncSQLitePipeline/AsyncSQLitePipeline with savepoint nesting, cut SyncSQLiteDriver/AsyncSQLiteDriver over to vertebra._sqlite with zero stdlib sqlite3, and wire conformance + CI."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:45:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:45:00Z"
    commit: 18baadcd
tags:
  - sqlite
  - driver
  - pipeline
  - conformance
  - ci
sources:
  - id: sqlite-core
    resource: src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore
    title: "SQLiteCore pragma helper with 5-arg call defect"
  - id: vertebra-py-db
    resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
    title: "Legacy sqlite_initialization_pragmas binding in vertebra-py"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Core vertebra._vertebra stub declarations"
  - id: sqlite-drv
    resource: src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver
    title: "Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3"
  - id: sqlite-cfg
    resource: src/py/vertebra/db/adapters/sqlite/config.py::SQLiteConfig
    title: "Python SQLiteConfig struct"
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
    title: "SyncPipeline and AsyncPipeline capability protocols"
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect
    title: "SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects"
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: "Database driver conformance suite"
  - id: ci-wf
    resource: .github/workflows/ci.yml
    title: "Continuous integration test and wheel isolation workflow"
---

# Chapter 04: Python SQLite Adapter Cutover, Delayed-Commit Pipelines, Backup Hooks, and CI Gate

<!-- truth: start -->
- `SQLiteCore.initialization_pragmas` fixes the 5-arg `TypeError` bug (`src/py/vertebra/db/adapters/sqlite/core.py:33`) and `sqlite_initialization_pragmas` is removed from `vertebra-py` and `_vertebra.pyi` [^sqlite-core] [^vertebra-py-db] [^vertebra-pyi].
- `SyncSQLitePipeline` and `AsyncSQLitePipeline` implement `SyncPipeline` / `AsyncPipeline` (`capabilities.py`) using delayed-commit `BEGIN IMMEDIATE` (or nested `SAVEPOINT` inside an outer transaction) flushed in a single native worker batch [^driver-caps].
- `SyncSQLiteDriver` and `AsyncSQLiteDriver(AsyncDatabaseDriver[SQLiteConfig])` execute exclusively through `vertebra._sqlite` + `spawn_rust_db_task_with_attr_and_cancel`, exposing `pipeline()`, `backup_to()`, `restore_from()`, `cancel()`, and zero-copy `VTBARROW` `ArrowCursor` with zero `import sqlite3` in `src/py/vertebra/db/adapters/sqlite/` [^sqlite-drv] [^sqlite-cfg].
- `test_driver_conformance.py` adds `"sqlite"` to `_FAIL_LOUD_DRIVER_ENGINES` (`native_extension_module="vertebra._sqlite"`), `dialects.py` removes stdlib `sqlite3` shortcuts, and `.github/workflows/ci.yml` runs `make sqlite-test` and `make sqlite-wheel-check` [^sa-dialects] [^conformance-suite] [^ci-wf].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `18baadcd`, `SQLiteCore.initialization_pragmas` passes 5 arguments to 6-argument `_vertebra.sqlite_initialization_pragmas` and silently swallows `TypeError` [^sqlite-core] [^vertebra-py-db], `SyncSQLiteDriver` wraps stdlib `sqlite3`, and `AsyncSQLiteDriver` subclasses `ThreadOffloadAsyncDatabaseDriver` (`asyncio.to_thread` per query) [^sqlite-drv]. This chapter cuts both drivers cleanly onto `vertebra._sqlite`, adds delayed-commit `SyncSQLitePipeline` / `AsyncSQLitePipeline` with savepoint nesting plus `backup_to` / `restore_from`, and enforces fail-loud conformance in CI.

**Non-Goals:** Public documentation and OKF chapters (`ch05`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Core & `_vertebra` | `adapters/sqlite/core.py`, `vertebra-py/src/db.rs`, `_vertebra.pyi` [^sqlite-core] [^vertebra-py-db] | Clean pragma generation; legacy binding removed |
| Pipelines | `adapters/sqlite/{pipeline,__init__}.py` [^driver-caps] | `SyncSQLitePipeline(SyncPipeline)` & `AsyncSQLitePipeline(AsyncPipeline)` |
| Drivers | `adapters/sqlite/{driver,config}.py` [^sqlite-drv] [^sqlite-cfg] | `SyncSQLiteDriver` & `AsyncSQLiteDriver` (`SyncPipelineCapable`/`AsyncPipelineCapable`, `backup_to`/`restore_from`) |
| Conformance & CI | `extensions/sqlalchemy/dialects.py`, `test_driver_conformance.py`, `ci.yml` [^sa-dialects] [^conformance-suite] [^ci-wf] | Fail-loud `vertebra-core[sqlite]` + CI gates |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10**: Fix `SQLiteCore.initialization_pragmas` and delete `sqlite_initialization_pragmas` from `vertebra-py/src/db.rs` and `_vertebra.pyi` (`t01`).
- **R11**: Implement delayed-commit `SyncSQLitePipeline` and `AsyncSQLitePipeline` supporting nested savepoints when `driver.in_transaction` is true (`t02`).
- **R12**: Cut `SyncSQLiteDriver` and `AsyncSQLiteDriver` over to `vertebra._sqlite`, removing `import sqlite3` and `ThreadOffloadAsyncDatabaseDriver`, and exposing `pipeline()`, `backup_to()`, and `restore_from()` (`t03`).
- **R13**: Remove stdlib `sqlite3` shortcuts from `extensions/sqlalchemy/dialects.py`, register `sqlite` in `_FAIL_LOUD_DRIVER_ENGINES` in `test_driver_conformance.py`, and add `make sqlite-test` + `make sqlite-wheel-check` to `.github/workflows/ci.yml` (`t04`).

```python
class SyncSQLitePipeline(SyncPipeline):
    def __init__(self, driver: SyncSQLiteDriver) -> None: ...
    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    def results(self) -> list[Any]: ...

class AsyncSQLitePipeline(AsyncPipeline):
    def __init__(self, driver: AsyncSQLiteDriver) -> None: ...
    def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None: ...
    async def results(self) -> list[Any]: ...

class SyncSQLiteDriver(SyncDatabaseDriver[SQLiteConfig]):
    def pipeline(self) -> SyncSQLitePipeline: ...
    def backup_to(self, target_path: str | os.PathLike[str], *, pages_per_step: int = 256, sleep_ms: int = 0) -> None: ...
    def restore_from(self, source_path: str | os.PathLike[str], *, pages_per_step: int = 256, sleep_ms: int = 0) -> None: ...

class AsyncSQLiteDriver(AsyncDatabaseDriver[SQLiteConfig]):
    def pipeline(self) -> AsyncSQLitePipeline: ...
    async def backup_to(self, target_path: str | os.PathLike[str], *, pages_per_step: int = 256, sleep_ms: int = 0) -> None: ...
    async def restore_from(self, source_path: str | os.PathLike[str], *, pages_per_step: int = 256, sleep_ms: int = 0) -> None: ...
```

**Integration Flow:**
- `ensure_module("vertebra._sqlite", package="vertebra-core[sqlite]", extra="sqlite")` fails loudly with `MissingDependencyError` when `vertebra-sqlite` is absent.
- `AsyncSQLiteDriver` awaits `spawn_rust_db_task_with_attr_and_cancel(..., "_vertebra_sqlite_completion_drainer")` directly without `asyncio.to_thread`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Missing `_sqlite` wheel | `SyncSQLiteDriver()` / `AsyncSQLiteDriver()` | Raises `MissingDependencyError` naming `vertebra-core[sqlite]` | Zero stdlib `sqlite3` fallback |
| Nested pipeline error | Mid-batch statement fails inside outer transaction | Rolls back nested `SAVEPOINT` only; outer transaction remains valid | Savepoint atomicity |
| Active async `Task.cancel()` | `asyncio.Task.cancel()` during slow query | Fires `cancel_operation(op_id)` -> `InterruptHandle::interrupt()` | Immediate worker abort |

## 3. Documentation & OKF Knowledge Plan

Driver, pipeline, and backup usage docs execute in `ch05`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R10` | `t01` | `adapters/sqlite/core.py`, `vertebra-py/src/db.rs`, `_vertebra.pyi` | `mypy.stubtest vertebra._vertebra` |
| `R11` | `t02` | `adapters/sqlite/{pipeline,__init__}.py` | `pytest src/py/tests/unit/db/test_sqlite_driver.py` |
| `R12` | `t03` | `adapters/sqlite/{driver,config}.py` | `pytest src/py/tests/unit/db/test_sqlite_driver.py` |
| `R13` | `t04` | `extensions/sqlalchemy/dialects.py`, `.github/workflows/ci.yml` | `pytest src/py/tests/integration/db/test_driver_conformance.py -k sqlite` |

## 5. Implementation Plan

- [ ] [t01-sqlite-core-and-vertebra-cleanup.md](tasks/t01-sqlite-core-and-vertebra-cleanup.md) — Fix 5-arg TypeError in SQLiteCore.initialization_pragmas and delete sqlite_initialization_pragmas from vertebra-py and _vertebra.pyi.
- [ ] [t02-delayed-commit-sqlite-pipelines.md](tasks/t02-delayed-commit-sqlite-pipelines.md) — Implement SyncSQLitePipeline and AsyncSQLitePipeline batching queued statements with BEGIN IMMEDIATE or nested SAVEPOINT.
- [ ] [t03-sync-and-async-driver-cutover.md](tasks/t03-sync-and-async-driver-cutover.md) — Replace stdlib sqlite3 and ThreadOffloadAsyncDatabaseDriver with native SyncSQLiteClient/AsyncSQLiteClient, pipelines, and backup hooks.
- [ ] [t04-conformance-sqlalchemy-and-ci.md](tasks/t04-conformance-sqlalchemy-and-ci.md) — Remove stdlib sqlite3 shortcuts in dialects.py, add sqlite to _FAIL_LOUD_DRIVER_ENGINES in conformance, and wire CI.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `git grep 'import sqlite3' src/py/vertebra` returns zero matches in production SQLite and dialect modules.
- **Fast Chapter Gate (`< 60s`)**: `uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/integration/db/test_driver_conformance.py -k sqlite`

[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore` — SQLiteCore pragma helper with 5-arg call defect.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3.
[^sqlite-cfg]: `src/py/vertebra/db/adapters/sqlite/config.py::SQLiteConfig` — Python SQLiteConfig struct.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — SyncPipeline and AsyncPipeline capability protocols.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect` — SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — Continuous integration test and wheel isolation workflow.
