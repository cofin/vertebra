---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch03-python-cutover-pipelines-ci-and-docs
title: 'Chapter 03: Python SQLite Adapter Cutover, Delayed-Commit Pipelines, Conformance CI, and Docs/OKF'
description: Fix SQLiteCore.initialization_pragmas, delete legacy _vertebra sqlite binding, add Sync/AsyncSQLitePipeline, cut Sync/AsyncSQLiteDriver over to vertebra._sqlite, wire SQLAlchemy/conformance/CI, and synchronize docs and OKF knowledge.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
tags:
- sqlite
- driver
- pipeline
- conformance
- ci
- docs
- okf
- limitations
sources:
- id: sqlite-core
  resource: src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore
  title: SQLiteCore pragma helper with 5-arg call defect
- id: vertebra-py-db
  resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
  title: Legacy sqlite_initialization_pragmas binding in vertebra-py
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Core vertebra._vertebra stub declarations
- id: sqlite-drv
  resource: src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver
  title: Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3
- id: sqlite-cfg
  resource: src/py/vertebra/db/adapters/sqlite/config.py::SQLiteConfig
  title: Python SQLiteConfig struct
- id: driver-caps
  resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
  title: SyncPipeline and AsyncPipeline capability protocols
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect
  title: SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects
- id: conformance-suite
  resource: src/py/tests/integration/db/test_driver_conformance.py
  title: Database driver conformance suite
- id: ci-wf
  resource: .github/workflows/ci.yml
  title: Continuous integration test and wheel isolation workflow
- id: docs-db
  resource: docs/usage/databases.md
  title: Database drivers usage guide
- id: docs-stubs
  resource: docs/reference/native-stubs.md
  title: Native extension stub API reference
- id: docs-changelog
  resource: docs/changelog/index.md
  title: Release changelog
- id: okf-drivers
  resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
  title: Database drivers and dialers OKF chapter
- id: domains-idx
  resource: .agents/bundles/knowledge/domains/index.md
  title: OKF domains catalog index
- id: review-pyo3-skill
  resource: .agents/skills/review-pyo3-bridge/SKILL.md
  title: PyO3 bridge review skill
- id: agents-md
  resource: AGENTS.md
  title: Repository agent instructions and quick verification commands
- id: okf-workflow
  resource: .agents/bundles/knowledge/workflow.md
  title: Project Makefile and verification workflow chapter
---

# Chapter 03: Python SQLite Adapter Cutover, Delayed-Commit Pipelines, Conformance CI, and Docs/OKF

<!-- truth: start -->
- `SQLiteCore.initialization_pragmas` fixes the 5-arg `TypeError` bug (`src/py/vertebra/db/adapters/sqlite/core.py:33`) and `sqlite_initialization_pragmas` is removed from `vertebra-py` and `_vertebra.pyi` [^sqlite-core] [^vertebra-py-db] [^vertebra-pyi].
- `SyncSQLitePipeline` and `AsyncSQLitePipeline` implement `SyncPipeline` / `AsyncPipeline` (`capabilities.py`) using delayed-commit `BEGIN IMMEDIATE` (or nested `SAVEPOINT` inside an outer transaction) flushed in a single native worker batch [^driver-caps].
- `SyncSQLiteDriver` and `AsyncSQLiteDriver(AsyncDatabaseDriver[SQLiteConfig])` execute exclusively through `vertebra._sqlite` + `spawn_rust_db_task_with_attr_and_cancel`, exposing `pipeline()`, `backup_to()`, `restore_from()`, `cancel()`, and zero-copy `VTBARROW` `ArrowCursor` with zero `import sqlite3` in `src/py/vertebra/db/adapters/sqlite/` [^sqlite-drv] [^sqlite-cfg].
- `src/py/tests/conformance/db/registry.py` registers `"sqlite"` with `EngineConformanceSpec(status="implemented", native_extension_module="vertebra._sqlite")` under `ConformanceGate` (`src/py/tests/conformance/db/_gate.py`), `dialects.py` removes stdlib `sqlite3` shortcuts, and `.github/workflows/ci.yml` runs `make sqlite-test` and `make sqlite-wheel-check` [^sa-dialects] [^conformance-suite] [^ci-wf].
- `docs/usage/databases.md`, `docs/reference/native-stubs.md` (`heading_level: 4`, `<= 14` TOC items, `< 120_000` bytes), and `docs/changelog/index.md` document `vertebra-core[sqlite]`, `SyncSQLitePipeline`/`AsyncSQLitePipeline`, `backup_to`/`restore_from`, and `vertebra._sqlite` [^docs-db] [^docs-stubs] [^docs-changelog].
- `sqlite-limitations.md` (`<= 200` lines) records single-writer WAL serialization, `:memory:` single-worker affinity vs shared URI, dynamic SQLite expression affinity in `VTBARROW`, and disabled `load_extension` [^okf-drivers] [^domains-idx].
- `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` list `vertebra-sqlite`, `make sqlite-test`, and `make sqlite-wheel-check` [^review-pyo3-skill] [^agents-md] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `18baadcd`, `SQLiteCore.initialization_pragmas` passes 5 arguments to 6-argument `_vertebra.sqlite_initialization_pragmas` and silently swallows `TypeError` [^sqlite-core] [^vertebra-py-db], `SyncSQLiteDriver` wraps stdlib `sqlite3`, and `AsyncSQLiteDriver` subclasses `ThreadOffloadAsyncDatabaseDriver` (`asyncio.to_thread` per query) [^sqlite-drv]. This chapter cuts both drivers cleanly onto `vertebra._sqlite`, adds delayed-commit `SyncSQLitePipeline` / `AsyncSQLitePipeline` with savepoint nesting plus `backup_to` / `restore_from`, and enforces fail-loud conformance in CI.

**Non-Goals:** Public documentation and OKF chapters (`ch03:t04`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Core & `_vertebra` | `adapters/sqlite/core.py`, `vertebra-py/src/db.rs`, `_vertebra.pyi` [^sqlite-core] [^vertebra-py-db] | Clean pragma generation; legacy binding removed |
| Pipelines | `adapters/sqlite/{pipeline,__init__}.py` [^driver-caps] | `SyncSQLitePipeline(SyncPipeline)` & `AsyncSQLitePipeline(AsyncPipeline)` |
| Drivers | `adapters/sqlite/{driver,config}.py` [^sqlite-drv] [^sqlite-cfg] | `SyncSQLiteDriver` & `AsyncSQLiteDriver` (`SyncPipelineCapable`/`AsyncPipelineCapable`, `backup_to`/`restore_from`) |
| Conformance & CI | `extensions/sqlalchemy/dialects.py`, `test_driver_conformance.py`, `ci.yml` [^sa-dialects] [^conformance-suite] [^ci-wf] | Fail-loud `vertebra-core[sqlite]` + CI gates |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).

Shipping a new standalone native wheel requires synchronized installation tabs (`uv`, `pip`, `pdm`, `poetry`), `native-stubs.md` API documentation within Zensical TOC/byte limits, an OKF limitations record (`<= 200` lines), and updated `AGENTS.md` / `workflow.md` / `review-pyo3-bridge` inventories [^docs-stubs] [^agents-md].

**Non-Goals:** Redis/Valkey documentation (owned by `native-redis-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Public docs | `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md` [^docs-db] [^docs-stubs] [^docs-changelog] | User docs, stub API reference, and `Unreleased` changelog |
| OKF data chapters | `knowledge/domains/data/{sqlite-limitations,database-drivers-and-dialers}.md`, `domains/index.md` [^okf-drivers] [^domains-idx] | Architecture and limitations chapters (`<= 200` lines) |
| Workflow & skills | `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, `knowledge/workflow.md` [^review-pyo3-skill] [^agents-md] [^okf-workflow] | `DOC-08` `Makefile` target parity and review skill sync |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10**: Fix `SQLiteCore.initialization_pragmas` and delete `sqlite_initialization_pragmas` from `vertebra-py/src/db.rs` and `_vertebra.pyi` (`t01`).
- **R11**: Implement delayed-commit `SyncSQLitePipeline` and `AsyncSQLitePipeline` supporting nested savepoints when `driver.in_transaction` is true (`t02`).
- **R12**: Cut `SyncSQLiteDriver` and `AsyncSQLiteDriver` over to `vertebra._sqlite`, removing `import sqlite3` and `ThreadOffloadAsyncDatabaseDriver`, and exposing `pipeline()`, `backup_to()`, and `restore_from()` (`t03`).
- **R13**: Remove stdlib `sqlite3` shortcuts from `extensions/sqlalchemy/dialects.py`, register `sqlite` with `native_extension_module="vertebra._sqlite"` in `src/py/tests/conformance/db/registry.py` and `ConformanceGate` (`src/py/tests/conformance/db/_gate.py`), and add `make sqlite-test` + `make sqlite-wheel-check` to `.github/workflows/ci.yml` (`t04`).

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

- **R14a**: Update `docs/usage/databases.md`, `docs/reference/native-stubs.md` (`heading_level: 4`), and `docs/changelog/index.md` (`t01`).
- **R14b**: Create `.agents/bundles/knowledge/domains/data/sqlite-limitations.md` (`<= 200` lines), update `database-drivers-and-dialers.md`, and index in `domains/index.md` (`t02`).
- **R14c**: Update `.agents/skills/review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` (`t03`).

```text
sqlite-limitations.md required caveats (<= 200 lines):
1. Single-writer WAL serialization (cross-process writers exceeding busy_timeout_ms surface transient OperationalError SQLITE_BUSY).
2. Plain :memory: pins to 1 worker thread; multi-reader in-memory concurrency requires file:...?mode=memory&cache=shared.
3. Dynamic SQLite expression columns without declared affinity infer VTBARROW schema from the first non-null row.
4. Runtime load_extension is disabled by default for FFI safety.
```

**Integration Flow:**
- `make docs` runs `scan_public_docstrings`, Zensical build, and `verify_docs_artifacts.py` (`<= 14` TOC items, `< 120_000` bytes).
- `make review-check` verifies `DOC-01`..`DOC-09` across `AGENTS.md`, skills, and OKF chapters.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Reference TOC overflow | Adding top-level `##` per class in `native-stubs.md` | Group under embedded engines with `heading_level: 4` (`<= 14` TOC items) | Zensical artifact budget |
| `DOC-08` command drift | `Makefile` adds `sqlite-test` without `AGENTS.md`/`workflow.md` | Synchronized in `t03` | Workflow command parity |

## 3. Documentation & OKF Knowledge Plan

Driver, pipeline, and backup usage docs execute in `ch03:t04`.

This chapter completes `[9/9]` documentation, skill, and OKF deliverables for `native-sqlite-driver`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Fix SQLiteCore Pragma Bug, Clean Up _vertebra, and Implement Delayed-Commit Pipelines | `src/py/vertebra/db/adapters/sqlite/core.py`, `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi` (+2 more) | `src/py/tests/unit/db/test_sqlite_driver.py` |
| `t02` | Cut SyncSQLiteDriver and AsyncSQLiteDriver Over to vertebra._sqlite | `src/py/vertebra/db/adapters/sqlite/driver.py`, `src/py/vertebra/db/adapters/sqlite/config.py` | `src/py/tests/unit/db/test_sqlite_driver.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |
| `t03` | SQLAlchemy Native SQLite Dialect Alignment, Conformance Suite, and CI Gate | `src/py/vertebra/extensions/sqlalchemy/dialects.py`, `src/py/tests/conformance/db/registry.py`, `src/py/tests/conformance/db/_gate.py` (+1 more) | `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/unit/db/test_sqlite_driver.py` |
| `t04` | Synchronize Public Documentation, Changelog, OKF SQLite Chapters, and Review Skills | `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md` (+6 more) | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-sqlite-core-and-delayed-commit-pipelines.md](tasks/t01-sqlite-core-and-delayed-commit-pipelines.md) — Fix 5-arg TypeError in SQLiteCore.initialization_pragmas, delete sqlite_initialization_pragmas from vertebra-py/_vertebra.pyi, and implement SyncSQLitePipeline and AsyncSQLitePipeline.
- [ ] [t02-sync-and-async-driver-cutover.md](tasks/t02-sync-and-async-driver-cutover.md) — Replace stdlib sqlite3 and ThreadOffloadAsyncDatabaseDriver with native SyncSQLiteClient/AsyncSQLiteClient, pipelines, and backup hooks.
- [ ] [t03-conformance-sqlalchemy-and-ci.md](tasks/t03-conformance-sqlalchemy-and-ci.md) — Remove stdlib sqlite3 shortcuts in dialects.py, register sqlite native_extension_module in conformance registry/gate, and wire CI.
- [ ] [t04-docs-changelog-and-okf-sync.md](tasks/t04-docs-changelog-and-okf-sync.md) — Update databases/installation/analytics docs, changelog, AGENTS.md, test_native_stubs.py, OKF sqlite-limitations.md and data/packaging/testing chapters, and review-python-framework.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: All OKF and skill files stay `<= 200` lines; `make docs` and `make review-check` exit `0`.
- **Fast Chapter Gate (`< 60s`)**: `make review-check && make docs`

[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore` — SQLiteCore pragma helper with 5-arg call defect.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3.
[^sqlite-cfg]: `src/py/vertebra/db/adapters/sqlite/config.py::SQLiteConfig` — Python SQLiteConfig struct.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — SyncPipeline and AsyncPipeline capability protocols.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect` — SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — Continuous integration test and wheel isolation workflow.
[^docs-db]: `docs/usage/databases.md` — Database drivers usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^agents-md]: `AGENTS.md` — Repository agent instructions and quick verification commands.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
