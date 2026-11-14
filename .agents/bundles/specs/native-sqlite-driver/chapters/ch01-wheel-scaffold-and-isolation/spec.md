---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: 'Chapter 01: Standalone vertebra-sqlite Crate, Companion Wheel Isolation, and Review Manifests'
description: Scaffold src/rs/vertebra-sqlite (vertebra._sqlite, gil_used = false, bundled SQLCipher), wire [sqlite] extra and Makefile/CI release matrix, enforce companion wheel isolation across sibling extensions, and register review manifests.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- sqlite
- rust
- wheel
- packaging
- pyo3
- errors
- pragmas
sources:
- id: cargo-ws
  resource: src/rs/Cargo.toml
  title: Rust workspace member manifest
- id: duckdb-cargo
  resource: src/rs/vertebra-duckdb/Cargo.toml
  title: Standalone embedded PyO3 wheel Cargo precedent
- id: duckdb-pyproj
  resource: src/rs/vertebra-duckdb/pyproject.toml
  title: Maturin standalone wheel isolation manifest precedent
- id: root-pyproject
  resource: pyproject.toml
  title: Root vertebra-core extras, uv sources, maturin excludes, and bumpversion
- id: makefile
  resource: Makefile
  title: Polyglot build, lint, stubtest, and wheel-check targets
- id: release-wf
  resource: .github/workflows/release.yaml
  title: Release wheel and sdist build matrix
- id: review-areas
  resource: .agents/skills/review-core/references/areas.toml
  title: Review router area path ownership manifest
- id: review-checklist
  resource: .agents/skills/review-core/references/checklist.md
  title: Review checklist including FFI-09 stub list
- id: wheel-tests
  resource: src/py/tests/unit/db/test_optional_driver_wheels.py
  title: Sibling PyO3 wheel isolation test gate
- id: sqlite-rs
  resource: src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig
  title: Shared SQLite pragma helper in vertebra-db
- id: vertebra-py-db
  resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
  title: Legacy sqlite_initialization_pragmas binding in vertebra-py
---

# Chapter 01: Standalone vertebra-sqlite Crate, Companion Wheel Isolation, and Review Manifests

<!-- truth: start -->
- `src/rs/vertebra-sqlite` compiles `vertebra._sqlite` (`crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)]`) against `rusqlite = "=0.41.1"` with `bundled`, `modern_sqlite`, `hooks`, `serialize`, `blob`, `column_decltype`, and `backup` (zero system `libsqlite3`) [^duckdb-cargo] [^duckdb-pyproj].
- `pyproject.toml` exposes `vertebra-core[sqlite]` and adds `vertebra-sqlite` to `[tool.uv.sources]`, `[tool.maturin].exclude`, and `[[tool.bumpversion.files]]` (omitting `[project.optional-dependencies].all` per `test_optional_driver_wheels.py`) [^root-pyproject].
- `Makefile` adds `sqlite-test` and `sqlite-wheel-check`, and `.github/workflows/release.yaml` builds wheels + sdist for `vertebra-sqlite` [^makefile] [^release-wf].
- `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` register `vertebra-sqlite` and `_sqlite.pyi` [^review-areas] [^review-checklist].
- Every sibling maturin `pyproject.toml` (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`) excludes `"**/py/vertebra/_sqlite.pyi"` [^duckdb-pyproj] [^wheel-tests].
- `test_optional_driver_wheels.py` enforces reciprocal `.pyi` isolation across `_oracle.pyi`, `_postgres.pyi`, and `_sqlite.pyi` [^wheel-tests].
- `SqliteClientConfig` computes WAL, `synchronous`, `mmap_size`, `busy_timeout`, `temp_store = MEMORY`, and `foreign_keys` pragmas and detects `:memory:` vs shared-URI vs file paths [^sqlite-rs].
- `errors.rs` maps extended `rusqlite::ffi::Error` codes (`2067`, `1555`, `2579`, `787`, `1299`, `275`, `5`, `6`, `9`, `1`, `20`) to typed `vertebra._vertebra.DatabaseError` subclasses with `vendor_code`, `sqlstate`, and `is_transient` [^vertebra-py-db] [^cargo-ws].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Every database engine in Vertebra ships as an isolated optional PyO3 wheel (`vertebra-<engine>` -> `vertebra._<engine>`) with zero system C library dependencies [^duckdb-cargo] [^root-pyproject]. This chapter establishes the `vertebra-sqlite` workspace crate, `_sqlite.pyi` type stub, `Makefile` targets, release matrix entries, and review ownership entries.

**Non-Goals:** Sibling wheel exclusion edits (`ch02`); WAL worker pool and Python driver cutover (`ch03`–`ch04`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate | `src/rs/vertebra-sqlite/{Cargo.toml,pyproject.toml,src/lib.rs}` [^duckdb-cargo] | `vertebra._sqlite` with `GIL_USED = False` |
| Packaging | `src/rs/Cargo.toml`, `pyproject.toml`, `Makefile`, `release.yaml` [^cargo-ws] [^root-pyproject] [^makefile] [^release-wf] | `vertebra-core[sqlite]`, `make sqlite-test`, `make sqlite-wheel-check` |
| Review infra | `areas.toml`, `checklist.md`, `pyo3-bridge/SKILL.md` [^review-areas] [^review-checklist] | `FFI-09` stubtest gate and path routing |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Project Workflow](../../../../knowledge/workflow.md).

Maturin workspace wheels share `src/py` as `python-source`, so every sibling wheel must exclude `_sqlite.pyi` to avoid file collisions on installation [^duckdb-pyproj] [^wheel-tests]. Inside `vertebra-sqlite`, `config.rs` and `errors.rs` provide deterministic WAL pragma generation and extended SQLite constraint/transient error classification before worker threads land in `ch03`.

**Non-Goals:** Worker thread pool and `VTBARROW` builder (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^wheel-tests] | Disjoint `.pyi` wheel archives |
| SQLite config & errors | `src/rs/vertebra-sqlite/src/{config,errors,lib}.rs` [^sqlite-rs] | `SqliteClientConfig` + extended FFI code -> `DatabaseError` |

**Knowledge Anchors:** [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1**: Scaffold `src/rs/vertebra-sqlite` (`Cargo.toml`, `pyproject.toml`, `src/lib.rs`) over `rusqlite = "=0.41.1"` (`bundled`, `modern_sqlite`, `backup`, `hooks`, `serialize`, `blob`, `column_decltype`) with `#[pymodule(gil_used = false)]` (`t01`).
- **R2**: Register `vertebra-sqlite` in `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and root `pyproject.toml` (`[sqlite]` extra, `uv.sources`, `maturin.exclude`, `bumpversion`; never add `[project.optional-dependencies].all`) (`t02`).
- **R3**: Create `src/py/vertebra/_sqlite.pyi` and wire `Makefile` (`dev`, `build`, `lint`, `lint-all`, `py-test`, `sqlite-test`, `sqlite-wheel-check`) and `.github/workflows/release.yaml` (`t03`).
- **R4**: Register `src/rs/vertebra-sqlite/**` and `src/py/vertebra/_sqlite.pyi` in `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` (`t04`).

```toml
# src/rs/vertebra-sqlite/Cargo.toml
[package]
name = "vertebra-sqlite"
version.workspace = true
edition.workspace = true

[lib]
name = "_sqlite"
crate-type = ["cdylib", "rlib"]

[dependencies]
arrow-array = "=60.1.0"
arrow-buffer = "=60.1.0"
arrow-ipc = "=60.1.0"
arrow-schema = "=60.1.0"
parking_lot.workspace = true
pyo3.workspace = true
rusqlite = { version = "=0.41.1", features = ["bundled", "modern_sqlite", "hooks", "serialize", "blob", "column_decltype", "backup"] }
serde_json.workspace = true
vertebra-core = { workspace = true }
vertebra-db = { workspace = true, features = ["pyo3"] }

[build-dependencies]
pyo3-build-config.workspace = true
```

**Integration Flow:**
- `pip install vertebra-core[sqlite]` -> installs `vertebra-sqlite` wheel providing `vertebra._sqlite` and `vertebra/_sqlite.pyi`.
- `make review-check` routes `src/rs/vertebra-sqlite/**` and `src/py/vertebra/_sqlite.pyi` to `pyo3-bridge` (`FFI`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| System libsqlite3 absent | Clean container without `libsqlite3-dev` | Compiles bundled SQLite 3.45+ C amalgamation cleanly | Zero system C deps |
| Orphan source file | `make review-check` scans tracked files | Owned by `[areas.pyo3-bridge]` in `areas.toml` | Zero unrouted files |

- **R5a**: Exclude `_sqlite.pyi` in `bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, and `storage` `pyproject.toml` manifests and enforce in `test_optional_driver_wheels.py` (`t01`–`t03`).
- **R5b**: Implement `SqliteClientConfig` and extended `rusqlite::Error` -> `vertebra._vertebra` exception mapping (`UniqueViolationError`, `ForeignKeyViolationError`, `NotNullViolationError`, `CheckViolationError`, `OperationalError` `is_transient`, `ProgrammingError`, `DataError`) (`t04`).

```rust
pub struct SqliteClientConfig {
    pub path: String,
    pub wal_mode: bool,
    pub synchronous: SqliteSynchronous,
    pub mmap_size: u64,
    pub busy_timeout_ms: u32,
    pub max_readers: usize,
    pub foreign_keys: bool,
    pub enable_jsonb: bool,
}
// Extended SQLite error codes -> (Python exception class, sqlstate, is_transient):
// 2067 | 1555 | 2579 -> UniqueViolationError ("23505", false)
// 787                -> ForeignKeyViolationError ("23503", false)
// 1299               -> NotNullViolationError ("23502", false)
// 275                -> CheckViolationError ("23514", false)
// 5 | 261 | 517 | 6  -> OperationalError ("55P03", true)
// 9 (SQLITE_INTERRUPT)-> OperationalError ("57014", true)
```

**Integration Flow:**
- `make sqlite-wheel-check` + `pytest test_optional_driver_wheels.py` validate wheel payload boundaries.
- `worker.rs` (`ch03`) consumes `SqliteClientConfig::initialization_pragmas()` and `map_rusqlite_error(py, err)`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Invalid synchronous mode | `synchronous = "BOGUS"` | Raises `ValueError` immediately at client construction | SQL injection-free pragma validation |
| Constraint violation | Extended code `2067` (`SQLITE_CONSTRAINT_UNIQUE`) | `UniqueViolationError` (`vendor_code=2067`, `sqlstate="23505"`) | Dialect-neutral ORM error handling |
| Busy / interrupt | `SQLITE_BUSY` (`5`) or `SQLITE_INTERRUPT` (`9`) | `OperationalError` (`is_transient=True`) | Retryable lock/cancel classification |

## 3. Documentation & OKF Knowledge Plan

Skill and reference updates complete in `ch03:t04`.

Pragma defaults and error mappings are documented in `ch03:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Scaffold vertebra-sqlite Crate and Wire Cargo Workspace + [sqlite] Extra | `src/rs/vertebra-sqlite/Cargo.toml`, `src/rs/vertebra-sqlite/pyproject.toml`, `src/rs/vertebra-sqlite/src/lib.rs` (+3 more) | `make review-check` |
| `t02` | Author _sqlite.pyi Stub, Makefile Targets, and Release Matrix | `src/py/vertebra/_sqlite.pyi`, `Makefile`, `.github/workflows/release.yaml` | `make review-check` |
| `t03` | Enforce _sqlite Companion Wheel Exclusions Across All Sibling Extensions | `src/rs/vertebra-bigquery/pyproject.toml`, `src/rs/vertebra-bundler/pyproject.toml`, `src/rs/vertebra-duckdb/pyproject.toml` (+6 more) | `src/py/tests/unit/db/test_optional_driver_wheels.py` |
| `t04` | Register vertebra-sqlite in Review Manifests, Checklist, and Skills | `.agents/skills/review-core/references/areas.toml`, `.agents/skills/review-core/references/checklist.md`, `.agents/skills/pyo3-bridge/SKILL.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-crate-workspace-and-root-pyproject.md](tasks/t01-crate-workspace-and-root-pyproject.md) — Create src/rs/vertebra-sqlite with bundled SQLCipher and gil_used = false, and wire Cargo workspace and root pyproject.toml [sqlite], uv.sources, maturin.exclude, and bumpversion.
- [ ] [t02-stub-makefile-and-release-matrix.md](tasks/t02-stub-makefile-and-release-matrix.md) — Create src/py/vertebra/_sqlite.pyi and wire Makefile sqlite-test/sqlite-wheel-check and .github/workflows/release.yaml.
- [ ] [t03-sibling-wheel-exclusions-and-isolation-gate.md](tasks/t03-sibling-wheel-exclusions-and-isolation-gate.md) — Exclude _sqlite* and vertebra-sqlite from all 10 sibling extension pyproject.toml manifests and register sqlite in test_optional_driver_wheels.py.
- [ ] [t04-review-manifest-checklist-and-skills.md](tasks/t04-review-manifest-checklist-and-skills.md) — Add vertebra-sqlite paths and _sqlite.pyi to areas.toml, checklist.md (FFI-09), and pyo3-bridge/SKILL.md.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero sibling wheels ship `_sqlite.pyi`; every extended SQLite constraint code maps to its specific `IntegrityError` subclass.
- **Fast Chapter Gate (`< 60s`)**: `make sqlite-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py && cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^duckdb-cargo]: `src/rs/vertebra-duckdb/Cargo.toml` — Standalone embedded PyO3 wheel Cargo precedent.
[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area path ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
[^sqlite-rs]: `src/rs/vertebra-db/src/sqlite/mod.rs::SqliteConfig` — Shared SQLite pragma helper in vertebra-db.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
