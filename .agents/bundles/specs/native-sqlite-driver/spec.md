---
type: Spec
flow_id: native-sqlite-driver
title: "Standalone Rust SQLite Wheel (vertebra-sqlite)"
description: "Build standalone PyO3 extension src/rs/vertebra-sqlite (vertebra._sqlite, vertebra-core[sqlite], gil_used = false) over bundled rusqlite 0.41.1 with a 3.14t/3.15t-safe single-writer/multi-reader WAL pool, SQLite 3.45+ JSONB, InterruptHandle cancel(), online backup/snapshot hooks, delayed-commit savepoint pipelines, and zero-copy VTBARROW ShmCapsule results, removing stdlib sqlite3 from production drivers."
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-wheel-scaffold-and-packaging
  - ch02-wheel-isolation-and-core-config
  - ch03-worker-pool-arrow-and-pyo3-clients
  - ch04-python-cutover-pipelines-and-ci
  - ch05-docs-and-knowledge-sync
current_chapter: ch01-wheel-scaffold-and-packaging
current_task: "native-sqlite-driver:ch01:t01"
parent_prd: native-data-platform
depends_on_flows:
  - broker-pool-sharing
plan_revision: 1
plan_commit: 18baadcd
created_at: "2026-11-12T23:45:00Z"
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
  - rust
  - pyo3
  - wal
  - arrow
  - wheel
sources:
  - id: cargo-ws
    resource: src/rs/Cargo.toml
    title: "Rust workspace member manifest"
  - id: duckdb-cargo
    resource: src/rs/vertebra-duckdb/Cargo.toml
    title: "Standalone embedded PyO3 wheel Cargo precedent"
  - id: root-pyproject
    resource: pyproject.toml
    title: "Root vertebra-core extras, uv sources, maturin excludes, and bumpversion"
  - id: sqlite-rs
    resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
    title: "Shared SQLite pragma helper in vertebra-db"
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: "AsyncCompletionQueue and Notifier PyO3 bridge"
  - id: arrow-shm
    resource: src/rs/vertebra-db/src/arrow_shm.rs
    title: "ShmArrowStreamWriter and VTBARROW ShmCapsule constructor"
  - id: vertebra-py-db
    resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
    title: "Legacy sqlite_initialization_pragmas binding in vertebra-py"
  - id: sqlite-core
    resource: src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore
    title: "SQLiteCore pragma helper with 5-arg call defect"
  - id: sqlite-drv
    resource: src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver
    title: "Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3"
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
    title: "SyncPipeline and AsyncPipeline capability protocols"
  - id: conformance-suite
    resource: src/py/tests/integration/db/test_driver_conformance.py
    title: "Database driver conformance suite"
  - id: okf-drivers
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: "Database drivers and dialers OKF chapter"
---

# Specification: Standalone Rust SQLite Wheel (vertebra-sqlite)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncSQLiteDriver` wraps stdlib `sqlite3` behind a Python `RLock`, `AsyncSQLiteDriver` hops through `ThreadOffloadAsyncDatabaseDriver` (`asyncio.to_thread`), and `SQLiteCore.initialization_pragmas` calls 6-arg `_vertebra.sqlite_initialization_pragmas` with 5 args (`TypeError` suppressed) [^sqlite-drv] [^sqlite-core] [^vertebra-py-db].
- **Target Architecture**: Standalone `src/rs/vertebra-sqlite` (`vertebra._sqlite`, `vertebra-core[sqlite]`, `#[pymodule(gil_used = false)]`) over `rusqlite = "=0.41.1"` (`bundled` + `modern_sqlite`, zero system `libsqlite3`) with 1 writer + `max_readers` WAL OS threads, SQLite 3.45+ `JSONB`, `InterruptHandle` `cancel()`, online `backup_to`/`restore_from`, `VTBARROW` `ShmCapsule` `ArrowCursor` + `load_from_arrow`, `broker_engine_capsule()` (`DIALECT_TAG_SQLITE = 3`), and delayed-commit `SyncSQLitePipeline`/`AsyncSQLitePipeline` [^duckdb-cargo] [^py-async] [^arrow-shm] [^driver-caps].
- **Scope & Boundaries**: `5` chapters and `19` compact tasks cutting `SyncSQLiteDriver` and `AsyncSQLiteDriver` over to `vertebra._sqlite` with zero `import sqlite3` in production drivers and full CI/release/OKF integration [^conformance-suite] [^okf-drivers].
<!-- truth: end -->

## 1. Specification, Context & Provenance

SQLite is the remaining relational adapter still executing on Python stdlib `sqlite3` and `ThreadOffloadAsyncDatabaseDriver` at `18baadcd` [^sqlite-drv]. In addition, `SQLiteCore.initialization_pragmas` (`src/py/vertebra/db/adapters/sqlite/core.py:33`) invokes `_vertebra.sqlite_initialization_pragmas` without `path: str` and suppresses `TypeError` on every call [^sqlite-core] [^vertebra-py-db]. This flow delivers a standalone `src/rs/vertebra-sqlite` wheel (`vertebra._sqlite`, `vertebra-core[sqlite]`) compiling bundled SQLite 3.45+ via `rusqlite 0.41.1` (`bundled`, `modern_sqlite`, `hooks`, `serialize`, `blob`, `column_decltype`, `backup`), running a GIL-free single-writer + multi-reader WAL worker pool safe for Python `3.14t`/`3.15t`, streaming `VTBARROW` `ShmCapsule` Arrow batches directly from `sqlite3_column_*` values, exposing delayed-commit `SyncSQLitePipeline`/`AsyncSQLitePipeline` with nested savepoints plus online `backup_to`/`restore_from` hooks, and registering `DIALECT_TAG_SQLITE = 3` with `BrokerEngineCapsule`.

**Non-Goals:**
- System dynamic `libsqlite3` linking (bundled amalgamation is mandatory for wheel portability and SQLite 3.45+ `JSONB`).
- Enabling `sqlite3_load_extension` by default (kept disabled for FFI memory safety; documented in `sqlite-limitations.md`).
- Django SQLite backend subclassing (owned by `sqlalchemy-and-django-integration`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate & wheel | `src/rs/vertebra-sqlite/*`, `src/rs/Cargo.toml`, `pyproject.toml`, `Makefile` [^cargo-ws] [^root-pyproject] | `vertebra._sqlite` (`gil_used = false`), `vertebra-core[sqlite]`, `make sqlite-test`, `make sqlite-wheel-check` |
| WAL pool & Arrow | `src/rs/vertebra-sqlite/src/{worker,arrow_bridge,py_client}.rs` [^py-async] [^arrow-shm] | `SyncSQLiteClient`, `AsyncSQLiteClient`, `InterruptHandle`, `backup_to`/`restore_from`, `VTBARROW`, `broker_engine_capsule()` |
| Python adapter | `src/py/vertebra/db/adapters/sqlite/{core,pipeline,driver,config}.py` [^sqlite-drv] [^driver-caps] | `SyncSQLiteDriver`, `AsyncSQLiteDriver`, `SyncSQLitePipeline`, `AsyncSQLitePipeline` |
| Conformance & docs | `test_driver_conformance.py`, `.github/workflows/{ci.yml,release.yaml}`, `docs/`, `knowledge/` [^conformance-suite] [^okf-drivers] | `_FAIL_LOUD_DRIVER_ENGINES`, CI/release wheels, `sqlite-limitations.md` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [ADR-004 Optional Driver Wheels and SHM Ownership](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Query Pipeline, CDC, and Tenancy](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [Benchmarks and Free-Threaded Python](../../knowledge/testing/benchmarks-and-free-threaded.md), [Project Workflow](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1–R4**: Standalone `src/rs/vertebra-sqlite` crate (`rusqlite 0.41.1` `bundled` + `modern_sqlite`), `vertebra-core[sqlite]` extra, `_sqlite.pyi`, `Makefile` targets, `release.yaml`, and review manifest/checklist/skill entries (`ch01`).
- **R5**: Reciprocal `_sqlite.pyi` wheel exclusions across all 9 sibling PyO3 wheels, `SqliteClientConfig` WAL/pragma validation, and extended SQLite FFI error mapping (`ch02`).
- **R6–R9**: Single-writer + `max_readers` WAL worker pool (`3.14t`/`3.15t` safe), SQLite 3.45+ `JSONB`, `InterruptHandle` `cancel()`, online `backup_to`/`restore_from`, direct `VTBARROW` `ShmCapsule` `ArrowCursor` + `load_from_arrow`, `SyncSQLiteClient`/`AsyncSQLiteClient`, and `broker_engine_capsule()` (`ch03`).
- **R10–R13**: Fix `SQLiteCore.initialization_pragmas`, remove `_vertebra.sqlite_initialization_pragmas`, add delayed-commit `SyncSQLitePipeline`/`AsyncSQLitePipeline` with savepoint nesting, cut `SyncSQLiteDriver`/`AsyncSQLiteDriver` over with zero `import sqlite3`, and wire conformance + `ci.yml` (`ch04`).
- **R14**: Synchronize `docs/usage/databases.md`, `docs/reference/native-stubs.md`, `docs/changelog/index.md`, `sqlite-limitations.md`, `database-drivers-and-dialers.md`, `review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `workflow.md` (`ch05`).

```python
# vertebra._sqlite
GIL_USED: Final[bool]  # False
def broker_engine_capsule() -> object: ...
class SyncSQLiteClient: ...
class AsyncSQLiteClient: ...

# vertebra.db.adapters.sqlite
class SyncSQLitePipeline(SyncPipeline): ...
class AsyncSQLitePipeline(AsyncPipeline): ...
class SyncSQLiteDriver(SyncDatabaseDriver[SQLiteConfig]): ...
class AsyncSQLiteDriver(AsyncDatabaseDriver[SQLiteConfig]): ...
```

**Integration Flow:**
- `SyncSQLiteDriver` -> `vertebra._sqlite.SyncSQLiteClient` (`SyncCompletionQueue` inside `py.detach`).
- `AsyncSQLiteDriver` -> `vertebra._sqlite.AsyncSQLiteClient` (`AsyncCompletionQueue` + `Notifier` via `spawn_rust_db_task_with_attr_and_cancel`).
- `BrokerEngineDiscovery` -> `vertebra._sqlite.broker_engine_capsule()` -> `BrokerEngineRegistry` (`DIALECT_TAG_SQLITE = 3`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Wheel absent | `vertebra-sqlite` not installed | `MissingDependencyError` naming `vertebra-core[sqlite]` | Fail-loud driver contract |
| Constraint error | SQLite extended codes `2067`/`787`/`1299`/`275` | `UniqueViolationError` / `ForeignKeyViolationError` / `NotNullViolationError` / `CheckViolationError` | Typed exception parity |
| Query cancel | `driver.cancel()` or async `Task.cancel()` | `InterruptHandle::interrupt()` aborts execution within `< 50ms` | Responsive cancellation |
| Nested pipeline fail | Statement raises inside nested `driver.pipeline()` | Rolls back nested `SAVEPOINT` while preserving outer tx | Savepoint isolation |

## 3. Documentation & OKF Knowledge Plan

- `[ ]` `docs/usage/databases.md` — `vertebra-core[sqlite]` install tabs, WAL tuning, delayed-commit pipelines, and online backups (`ch05:t01`).
- `[ ]` `docs/reference/native-stubs.md` — `vertebra._sqlite` stub API reference with `heading_level: 4` (`ch05:t01`).
- `[ ]` `docs/changelog/index.md` — `Unreleased` entries (`ch05:t01`).
- `[ ]` `.agents/bundles/knowledge/domains/data/sqlite-limitations.md` — SQLite WAL, `:memory:`, `VTBARROW` affinity, and extension caveats (`<= 200` lines, `ch05:t02`).
- `[ ]` `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` & `domains/index.md` — native SQLite driver architecture (`ch05:t02`).
- `[ ]` `.agents/skills/review-pyo3-bridge/SKILL.md`, `AGENTS.md`, and `.agents/bundles/knowledge/workflow.md` — crate and `Makefile` target sync (`ch05:t03`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1`–`R4` | `ch01` (`t01`–`t04`) | `src/rs/vertebra-sqlite/*`, `pyproject.toml`, `Makefile`, `release.yaml`, `areas.toml` | `cargo test -p vertebra-sqlite && make review-check` |
| `R5` | `ch02` (`t01`–`t04`) | `src/rs/vertebra-*/pyproject.toml`, `vertebra-sqlite/src/{config,errors}.rs` | `make sqlite-wheel-check && pytest test_optional_driver_wheels.py` |
| `R6`–`R9` | `ch03` (`t01`–`t04`) | `vertebra-sqlite/src/{value,worker,arrow_bridge,py_client}.rs`, `_sqlite.pyi` | `make sqlite-test && pytest test_freethreaded_gil.py` |
| `R10`–`R13` | `ch04` (`t01`–`t04`) | `adapters/sqlite/{core,pipeline,driver,config}.py`, `dialects.py`, `ci.yml` | `pytest test_sqlite_driver.py test_driver_conformance.py -k sqlite` |
| `R14` | `ch05` (`t01`–`t03`) | `docs/**`, `.agents/bundles/knowledge/**`, `AGENTS.md`, `review-pyo3-bridge/SKILL.md` | `make docs && make review-check` |

## 5. Implementation Plan

- [ ] [ch01-wheel-scaffold-and-packaging](chapters/ch01-wheel-scaffold-and-packaging/spec.md) — Scaffold `src/rs/vertebra-sqlite` (`vertebra._sqlite`), `vertebra-core[sqlite]`, `Makefile` targets, `release.yaml`, and review manifests (Gate: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite && make review-check`)
- [ ] [ch02-wheel-isolation-and-core-config](chapters/ch02-wheel-isolation-and-core-config/spec.md) — Sibling `.pyi` exclusions, `test_optional_driver_wheels.py` gate, `SqliteClientConfig`, and extended SQLite error mapping (Gate: `make sqlite-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite`)
- [ ] [ch03-worker-pool-arrow-and-pyo3-clients](chapters/ch03-worker-pool-arrow-and-pyo3-clients/spec.md) — Single-writer + multi-reader WAL pool, `JSONB`, `InterruptHandle` `cancel()`, `backup_to`/`restore_from`, `VTBARROW`, `SyncSQLiteClient`/`AsyncSQLiteClient`, and broker capsule (Gate: `make sqlite-test && uv run --no-sync pytest src/py/tests/unit/test_freethreaded_gil.py`)
- [ ] [ch04-python-cutover-pipelines-and-ci](chapters/ch04-python-cutover-pipelines-and-ci/spec.md) — Delayed-commit `SyncSQLitePipeline`/`AsyncSQLitePipeline`, `SyncSQLiteDriver`/`AsyncSQLiteDriver` cutover (zero `sqlite3`), backup hooks, and CI (Gate: `uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/integration/db/test_driver_conformance.py -k sqlite`)
- [ ] [ch05-docs-and-knowledge-sync](chapters/ch05-docs-and-knowledge-sync/spec.md) — Public `docs/` + `native-stubs.md`, `sqlite-limitations.md` (`<= 200` lines), `AGENTS.md`, `workflow.md`, and `review-pyo3-bridge` (Gate: `make review-check && make docs`)

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `0/5` chapters and `0/19` tasks completed; frontier at `ch01-wheel-scaffold-and-packaging` (`native-sqlite-driver:ch01:t01`).
- **Upstream Flow Dependencies**: `broker-pool-sharing`.
- **Active Invariants**: Zero system `libsqlite3` dependency; `GIL_USED == False`; zero `import sqlite3` in production SQLite driver/dialect modules; reciprocal `.pyi` wheel isolation holds across every extension.
- **Full `/flow:finish` Gate**: `make sqlite-test && make sqlite-wheel-check && make lint-all && make py-test && make docs && make review-check`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^duckdb-cargo]: `src/rs/vertebra-duckdb/Cargo.toml` — Standalone embedded PyO3 wheel Cargo precedent.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — Shared SQLite pragma helper in vertebra-db.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^arrow-shm]: `src/rs/vertebra-db/src/arrow_shm.rs` — ShmArrowStreamWriter and VTBARROW ShmCapsule constructor.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore` — SQLiteCore pragma helper with 5-arg call defect.
[^sqlite-drv]: `src/py/vertebra/db/adapters/sqlite/driver.py::SyncSQLiteDriver` — Python SyncSQLiteDriver and AsyncSQLiteDriver over stdlib sqlite3.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — SyncPipeline and AsyncPipeline capability protocols.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database driver conformance suite.
[^okf-drivers]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Database drivers and dialers OKF chapter.
