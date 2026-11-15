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
depends_on_chapters:
  - ch02-rust-engine-arrow-and-pyo3-clients
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:10:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-14T23:10:00Z'
    commit: 1b191388
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
    title: SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect
    title: SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec
    title: Database driver conformance suite and ENGINE_SPECS
  - id: conformance-gate
    resource: src/py/tests/integration/conformance_gate.py::ConformanceGate
    title: ConformanceGate SUPPORTED_ENGINES lane filter
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
current_task: "native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t01"
---

# Chapter 03: Python SQLite Adapter Cutover, Delayed-Commit Pipelines, Conformance CI, and Docs/OKF

<!-- truth: start -->
- `SQLiteCore.initialization_pragmas` fixes the 5-of-7-argument call suppressed at `src/py/vertebra/db/adapters/sqlite/core.py:34-41` and `sqlite_initialization_pragmas` is removed from `vertebra-py` and `_vertebra.pyi` [^sqlite-core] [^vertebra-py-db] [^vertebra-pyi].
- `SyncSQLitePipeline` and `AsyncSQLitePipeline` implement `SyncPipeline` / `AsyncPipeline` using delayed-commit `BEGIN IMMEDIATE` (or nested `SAVEPOINT` inside an outer transaction) flushed in a single native worker batch [^driver-caps].
- `SyncSQLiteDriver` and `AsyncSQLiteDriver(AsyncDatabaseDriver[SQLiteConfig])` execute exclusively through `vertebra._sqlite` + `spawn_rust_db_task_with_attr_and_cancel`, exposing `pipeline()`, `backup_to()`, `restore_from()`, `cancel()`, and zero-copy `VTBARROW` `ArrowCursor` with zero `import sqlite3` in `src/py/vertebra/db/adapters/sqlite/` [^sqlite-drv] [^sqlite-cfg].
- `test_driver_conformance.py` updates `ENGINE_SPECS` for `"sqlite"` with `native_extension_module="vertebra._sqlite"`, and `supports_pipeline=True` (`pending_flow_id` already defaults to `None`); `ConformanceGate.SUPPORTED_ENGINES` adds `"sqlite"`; `.github/workflows/ci.yml` adds `sqlite` to the `test-db-conformance` matrix and runs `make sqlite-test` and `make sqlite-wheel-check` [^sa-dialects] [^conformance-suite] [^conformance-gate] [^ci-wf].
- `docs/usage/databases.md`, `docs/reference/native-stubs.md` (`heading_level: 4`, `<= 14` TOC items, `< 120_000` bytes), `docs/changelog/index.md`, `sqlite-limitations.md` (`<= 200` lines), `database-drivers-and-dialers.md`, `domains/index.md`, `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` are synchronized [^docs-db] [^docs-stubs] [^docs-changelog] [^okf-drivers] [^domains-idx] [^review-pyo3-skill] [^agents-md] [^okf-workflow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `473e1f2e`, `SQLiteCore.initialization_pragmas` passes 5 arguments to 7-parameter `_vertebra.sqlite_initialization_pragmas` while suppressing `TypeError`, `ValueError`, and `RuntimeError` [^sqlite-core] [^vertebra-py-db], `SyncSQLiteDriver` wraps stdlib `sqlite3`, and `AsyncSQLiteDriver` subclasses `ThreadOffloadAsyncDatabaseDriver` (`asyncio.to_thread` per query) [^sqlite-drv]. This chapter cuts both drivers cleanly onto `vertebra._sqlite`, adds delayed-commit `SyncSQLitePipeline` / `AsyncSQLitePipeline` with savepoint nesting plus `backup_to` / `restore_from`, wires fail-loud conformance and CI, and updates public docs, `AGENTS.md`, review skills, and OKF chapters (`sqlite-limitations.md <= 200` lines) [^docs-stubs] [^okf-drivers] [^okf-workflow].

**Non-Goals:** Redis/Valkey documentation (owned by `native-redis-driver`); Django SQLite backend subclassing (owned by `sqlalchemy-and-django-integration`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Core & `_vertebra` | `adapters/sqlite/core.py`, `vertebra-py/src/db.rs`, `_vertebra.pyi` [^sqlite-core] [^vertebra-py-db] [^vertebra-pyi] | Clean pragma generation; legacy binding removed |
| Pipelines & Drivers | `adapters/sqlite/{pipeline,driver,config,__init__}.py` [^sqlite-drv] [^sqlite-cfg] [^driver-caps] | `SyncSQLitePipeline`/`AsyncSQLitePipeline`, native `SyncSQLiteDriver`/`AsyncSQLiteDriver` |
| Conformance & CI | `extensions/sqlalchemy/dialects.py`, `test_driver_conformance.py`, `conformance_gate.py`, `ci.yml` [^sa-dialects] [^conformance-suite] [^conformance-gate] [^ci-wf] | Fail-loud `vertebra-core[sqlite]` + CI gates |
| Docs & OKF knowledge | `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `knowledge/domains/data/*`, `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, `knowledge/workflow.md` [^docs-db] [^docs-stubs] [^docs-changelog] [^okf-drivers] [^agents-md] [^okf-workflow] | Tabbed installs, stub API reference, changelog, present-tense OKF chapters (`<= 200` lines), `DOC-08` parity |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10 / R11**: Fix `SQLiteCore.initialization_pragmas` without swallowing errors, delete `sqlite_initialization_pragmas` from `vertebra-py/src/db.rs` and `_vertebra.pyi`, and implement delayed-commit `SyncSQLitePipeline` / `AsyncSQLitePipeline` with nested savepoints (`t01`).
- **R12**: Cut `SyncSQLiteDriver` and `AsyncSQLiteDriver` over to `vertebra._sqlite` (`ensure_module`), removing `import sqlite3` and `ThreadOffloadAsyncDatabaseDriver`, and exposing `pipeline()`, `backup_to()`, `restore_from()`, `select_to_arrow()`, `stream_arrow_batches()`, `load_from_arrow()`, `execute_script()`, and `cancel()` (`t02`).
- **R13**: Set `native_extension_module="vertebra._sqlite"` and `supports_pipeline=True` on `"sqlite"` (the `sqlite3` fallback in `src/py/vertebra/db/dbapi/` is removed by `sqlalchemy-and-django-integration:ch01:t01`; `extensions/sqlalchemy/dialects.py` has no `sqlite3` use) in `ENGINE_SPECS` (`test_driver_conformance.py`), add `"sqlite"` to `ConformanceGate.SUPPORTED_ENGINES` (`conformance_gate.py` + `test_conformance_gate.py`), and run `make sqlite-test` + `make sqlite-wheel-check` in `.github/workflows/ci.yml` (`t03`).
- **R14**: Update `docs/usage/databases.md` (`uv`/`pip`/`pdm`/`poetry` tabs), `docs/reference/native-stubs.md` (`heading_level: 4`, `<= 14` TOC items, `< 120_000` bytes), `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/data/{sqlite-limitations,database-drivers-and-dialers}.md` (`<= 200` lines), `.agents/bundles/knowledge/domains/index.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` (`t04`).

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

```text
sqlite-limitations.md required caveats (<= 200 lines):
1. Single-writer WAL serialization (cross-process writers exceeding busy_timeout_ms surface transient OperationalError SQLITE_BUSY).
2. Plain :memory: pins to 1 worker thread; multi-reader in-memory concurrency requires file:...?mode=memory&cache=shared.
3. Dynamic SQLite expression columns without declared affinity infer VTBARROW schema from the first non-null row.
4. Runtime load_extension is disabled by default for FFI safety.
```

**Integration Flow:**
- `ensure_module("vertebra._sqlite", package="vertebra-core[sqlite]", extra="sqlite")` fails loudly with `MissingDependencyError` when `vertebra-sqlite` is absent.
- `AsyncSQLiteDriver` awaits `AsyncSQLiteClient` awaitables that the Rust side produces through `spawn_rust_db_task_with_attr_and_cancel` (`_vertebra_sqlite_completion_drainer`), without `asyncio.to_thread`.
- `make docs` runs `scan_public_docstrings`, Zensical build, and `tools/docs/verify_docs_artifacts.py` (`<= 14` TOC items, `< 120_000` bytes); `make review-check` verifies `DOC-01`..`DOC-09`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Missing `_sqlite` wheel | `SyncSQLiteDriver()` / `AsyncSQLiteDriver()` | Raises `MissingDependencyError` naming `vertebra-core[sqlite]` | Zero stdlib `sqlite3` fallback |
| Nested pipeline error | Mid-batch statement fails inside outer transaction | Rolls back nested `SAVEPOINT` only; outer tx stays intact | Savepoint atomicity |
| Active async `Task.cancel()` | `asyncio.Task.cancel()` during slow query | Fires `cancel_operation(op_id)` -> `InterruptHandle::interrupt()` | Immediate worker abort |
| Reference TOC overflow | Adding top-level `##` per class in `native-stubs.md` | Group under `_sqlite` with `heading_level: 4` (`<= 14` TOC items) | Zensical artifact budget |
| `DOC-08` command drift | `Makefile` adds `sqlite-test` without `AGENTS.md`/`workflow.md` | Synchronized identically in `t04` | Workflow command parity |

## 3. Documentation & OKF Knowledge Plan

Task `t04` completes `[9/9]` public documentation, skill, workflow, and OKF deliverables for `native-sqlite-driver`: `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/data/sqlite-limitations.md`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, `.agents/bundles/knowledge/domains/index.md`, `.agents/skills/review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Fix SQLiteCore Pragma Bug, Clean Up _vertebra, and Implement Delayed-Commit Pipelines | `src/py/vertebra/db/adapters/sqlite/{core,pipeline,__init__}.py`, `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi` | `mypy.stubtest vertebra._vertebra`, `src/py/tests/unit/db/test_sqlite_driver.py` |
| `t02` | Cut SyncSQLiteDriver and AsyncSQLiteDriver Over to vertebra._sqlite | `src/py/vertebra/db/adapters/sqlite/{driver,config}.py` | `src/py/tests/unit/db/test_sqlite_driver.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py` |
| `t03` | SQLAlchemy Native SQLite Dialect Alignment, Conformance Suite, and CI Gate | `src/py/vertebra/extensions/sqlalchemy/dialects.py`, `src/py/tests/integration/conformance_gate.py`, `src/py/tests/unit/db/test_conformance_gate.py`, `src/py/tests/integration/db/test_driver_conformance.py`, `.github/workflows/ci.yml` | `src/py/tests/unit/db/test_conformance_gate.py`, `src/py/tests/integration/db/test_driver_conformance.py` |
| `t04` | Synchronize Public Documentation, Changelog, OKF SQLite Chapters, and Review Skills | `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `.agents/bundles/knowledge/domains/data/*`, `AGENTS.md`, `workflow.md`, `review-pyo3-bridge/SKILL.md` | `python -m tools.docs.scan_public_docstrings`, `make review-check` |

## 5. Implementation Plan

- [ ] [t01-sqlite-core-and-delayed-commit-pipelines.md](tasks/t01-sqlite-core-and-delayed-commit-pipelines.md) — Fix 5-arg `TypeError` in `SQLiteCore.initialization_pragmas`, delete `sqlite_initialization_pragmas` from `vertebra-py`/`_vertebra.pyi`, and implement `SyncSQLitePipeline` and `AsyncSQLitePipeline`.
- [ ] [t02-sync-and-async-driver-cutover.md](tasks/t02-sync-and-async-driver-cutover.md) — Replace stdlib `sqlite3` and `ThreadOffloadAsyncDatabaseDriver` with native `SyncSQLiteClient`/`AsyncSQLiteClient`, pipelines, and `backup_to`/`restore_from` hooks.
- [ ] [t03-conformance-sqlalchemy-and-ci.md](tasks/t03-conformance-sqlalchemy-and-ci.md) — Remove stdlib `sqlite3` shortcuts in `dialects.py`, wire `sqlite` in `test_driver_conformance.py` (`ENGINE_SPECS`) and `ConformanceGate`, and add `sqlite-test` + `sqlite-wheel-check` to `ci.yml`.
- [ ] [t04-docs-changelog-and-okf-sync.md](tasks/t04-docs-changelog-and-okf-sync.md) — Update `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `AGENTS.md`, `review-pyo3-bridge/SKILL.md`, and OKF chapters (`sqlite-limitations.md`, `database-drivers-and-dialers.md`, `workflow.md`).

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero `import sqlite3` in `src/py/vertebra/db/adapters/sqlite/`; all OKF and skill files stay `<= 200` lines; `make docs` and `make review-check` exit `0`.
- **Fast Chapter Gate (`< 60s`)**: `make sqlite-test && uv run --no-sync ruff check src/py/vertebra/db/adapters/sqlite src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/adapters/sqlite src/py/vertebra/extensions/sqlalchemy && uv run --no-sync pytest src/py/tests/unit/db/test_conformance_gate.py -q && uv run --no-sync python -m tools.docs.scan_public_docstrings && make review-check`

[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore` — SQLiteCore pragma helper with 5-arg call defect.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3.
[^sqlite-cfg]: `src/py/vertebra/db/adapters/sqlite/config.py::SQLiteConfig` — Python SQLiteConfig struct.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect` — SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec` — Database driver conformance suite and ENGINE_SPECS.
[^conformance-gate]: `src/py/tests/integration/conformance_gate.py::ConformanceGate` — ConformanceGate SUPPORTED_ENGINES lane filter.
[^ci-wf]: `.github/workflows/ci.yml` — Continuous integration test and wheel isolation workflow.
[^docs-db]: `docs/usage/databases.md` — Database drivers usage guide.
[^docs-stubs]: `docs/reference/native-stubs.md` — Native extension stub API reference.
[^docs-changelog]: `docs/changelog/index.md` — Release changelog.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
[^domains-idx]: `.agents/bundles/knowledge/domains/index.md` — OKF domains catalog index.
[^review-pyo3-skill]: `.agents/skills/review-pyo3-bridge/SKILL.md` — PyO3 bridge review skill.
[^agents-md]: `AGENTS.md` — Repository agent instructions and quick verification commands.
[^okf-workflow]: `.agents/bundles/knowledge/workflow.md` — Project Makefile and verification workflow chapter.
