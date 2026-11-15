---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: 'Chapter 01: Standalone vertebra-sqlite Crate, Companion Wheel Isolation, and Review Manifests'
description: Scaffold src/rs/vertebra-sqlite (vertebra._sqlite, gil_used = false, bundled SQLite 3.45+), wire [sqlite] extra and Makefile/CI release matrix, enforce companion wheel isolation across sibling extensions, and register review manifests.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
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
  - rust
  - wheel
  - packaging
  - pyo3
  - review
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
  - id: pyo3-skill
    resource: .agents/skills/pyo3-bridge/SKILL.md
    title: PyO3 native extension engineering skill
  - id: wheel-tests
    resource: src/py/tests/unit/db/test_optional_driver_wheels.py
    title: Sibling PyO3 wheel isolation test gate
current_task: "native-sqlite-driver:ch01-wheel-scaffold-and-isolation:t01"
---

# Chapter 01: Standalone vertebra-sqlite Crate, Companion Wheel Isolation, and Review Manifests

<!-- truth: start -->
- `src/rs/vertebra-sqlite` compiles `vertebra._sqlite` (`crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)]`) against `rusqlite = "=0.41.1"` with `bundled`, `modern_sqlite`, `hooks`, `serialize`, `blob`, `column_decltype`, and `backup` (zero system `libsqlite3`) [^duckdb-cargo] [^duckdb-pyproj].
- `pyproject.toml` exposes `vertebra-core[sqlite]` (`vertebra-sqlite==0.3.0`) and adds `vertebra-sqlite` to `[tool.uv.sources]`, `[tool.maturin].exclude`, and `[[tool.bumpversion.files]]` (omitting `[project.optional-dependencies].all` per `test_optional_driver_wheels.py`) [^root-pyproject] [^wheel-tests].
- `Makefile` adds `sqlite-test` and `sqlite-wheel-check`, and `.github/workflows/release.yaml` builds wheels + sdist for `vertebra-sqlite` [^makefile] [^release-wf].
- Every sibling maturin `pyproject.toml` (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`) excludes `"**/py/vertebra/_sqlite.pyi"`, enforced reciprocally in `test_optional_driver_wheels.py` [^duckdb-pyproj] [^wheel-tests].
- `areas.toml`, `checklist.md` (`FFI-09`), and `.agents/skills/pyo3-bridge/SKILL.md` register `vertebra-sqlite` and `_sqlite.pyi` [^review-areas] [^review-checklist] [^pyo3-skill].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Every optional database engine in Vertebra ships as an isolated PyO3 wheel (`vertebra-<engine>` -> `vertebra._<engine>`) with zero system C library dependencies [^duckdb-cargo] [^root-pyproject]. Because maturin workspace wheels share `src/py` as `python-source`, adding `src/py/vertebra/_sqlite.pyi` requires symmetric exclusion across all 9 sibling wheel manifests plus registration in build, release, and review routing files [^duckdb-pyproj] [^wheel-tests] [^review-areas].

**Non-Goals:** `SqliteClientConfig` and extended error mapping (`ch02:t01`); WAL worker pool and Python driver cutover (`ch02`–`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate & workspace | `src/rs/vertebra-sqlite/{Cargo.toml,pyproject.toml,src/lib.rs}`, `src/rs/Cargo.toml`, `pyproject.toml` [^cargo-ws] [^duckdb-cargo] [^root-pyproject] | `vertebra._sqlite` (`GIL_USED = False`), `vertebra-core[sqlite]` |
| Stubs & build matrix | `src/py/vertebra/_sqlite.pyi`, `Makefile`, `.github/workflows/release.yaml` [^makefile] [^release-wf] | `make sqlite-test`, `make sqlite-wheel-check`, wheel/sdist builds |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `test_optional_driver_wheels.py` [^duckdb-pyproj] [^wheel-tests] | Disjoint `.pyi` wheel archives across all 10 PyO3 crates |
| Review routing | `areas.toml`, `checklist.md`, `pyo3-bridge/SKILL.md` [^review-areas] [^review-checklist] [^pyo3-skill] | `FFI-09` stubtest gate and path ownership |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [ADR-004 Optional Driver Wheels and SHM Ownership](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1 / R2**: Scaffold `src/rs/vertebra-sqlite` (`Cargo.toml`, `pyproject.toml`, `src/lib.rs`) over `rusqlite = "=0.41.1"` (`bundled`, `modern_sqlite`, `backup`, `hooks`, `serialize`, `blob`, `column_decltype`) with `#[pymodule(gil_used = false)]`; register in `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and root `pyproject.toml` (`[sqlite]` extra, `uv.sources`, `maturin.exclude`, `bumpversion`; omit `[project.optional-dependencies].all`) (`t01`).
- **R3**: Create `src/py/vertebra/_sqlite.pyi` and wire `Makefile` (`dev`, `build`, `lint`, `lint-all`, `py-test`, `sqlite-test`, `sqlite-wheel-check`) and `.github/workflows/release.yaml` (`t02`).
- **R5a**: Exclude `"**/py/vertebra/_sqlite.pyi"` in all 9 sibling maturin `pyproject.toml` manifests (`bigquery`, `bundler`, `duckdb`, `mssql`, `mysql`, `oracle`, `postgres`, `spanner`, `storage`) and enforce reciprocal isolation in `test_optional_driver_wheels.py` (`t03`).
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
arrow-array = { version = "=60.1.0", default-features = false }
arrow-buffer = { version = "=60.1.0", default-features = false }
arrow-ipc = { version = "=60.1.0", default-features = false }
arrow-schema = { version = "=60.1.0", default-features = false }
parking_lot = "0.12"
pyo3 = { workspace = true }
rusqlite = { version = "=0.41.1", features = ["bundled", "modern_sqlite", "hooks", "serialize", "blob", "column_decltype", "backup"] }
serde_json = { workspace = true }
vertebra-core = { path = "../vertebra-core" }
vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }

[build-dependencies]
pyo3-build-config = { workspace = true }
```

**Integration Flow:**
- `pip install "vertebra-core[sqlite]"` -> installs `vertebra-sqlite` providing `vertebra._sqlite` and `vertebra/_sqlite.pyi`.
- `make sqlite-wheel-check` and `pytest src/py/tests/unit/db/test_optional_driver_wheels.py` verify disjoint wheel payloads across all 10 PyO3 extensions.
- `make review-check` routes `src/rs/vertebra-sqlite/**` and `src/py/vertebra/_sqlite.pyi` to `pyo3-bridge`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| System `libsqlite3` absent | Clean build without OS `libsqlite3-dev` | Compiles bundled SQLite 3.45+ C amalgamation (`rusqlite::version_number() >= 3_045_000`) | Zero system C deps |
| Sibling wheel collision | Missing exclusion in any sibling `pyproject.toml` | `test_optional_driver_wheels.py` fails immediately naming missing `.pyi` exclusion | Disjoint extension wheels |
| Orphan source file | `make review-check` scans tracked files | `src/rs/vertebra-sqlite/**` and `_sqlite.pyi` owned by `[areas.pyo3-bridge]` | Zero unrouted files |

## 3. Documentation & OKF Knowledge Plan

User documentation, `AGENTS.md`, and OKF chapters execute in `ch03:t04`; review skill registration completes here in `t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Scaffold vertebra-sqlite Crate and Wire Cargo Workspace + [sqlite] Extra | `src/rs/vertebra-sqlite/Cargo.toml`, `src/rs/vertebra-sqlite/pyproject.toml`, `src/rs/vertebra-sqlite/src/lib.rs` (+3 more) | `cargo test -p vertebra-sqlite` |
| `t02` | Author _sqlite.pyi Stub, Makefile Targets, and Release Matrix | `src/py/vertebra/_sqlite.pyi`, `Makefile`, `.github/workflows/release.yaml` | `make dev && mypy.stubtest vertebra._sqlite` |
| `t03` | Enforce _sqlite Companion Wheel Exclusions Across All Sibling Extensions | `src/rs/vertebra-*/pyproject.toml` (9 manifests) | `make sqlite-wheel-check`, `src/py/tests/unit/db/test_optional_driver_wheels.py` |
| `t04` | Register vertebra-sqlite in Review Manifests, Checklist, and Skills | `.agents/skills/review-core/references/areas.toml`, `.agents/skills/review-core/references/checklist.md`, `.agents/skills/pyo3-bridge/SKILL.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-crate-workspace-and-root-pyproject.md](tasks/t01-crate-workspace-and-root-pyproject.md) — Create `src/rs/vertebra-sqlite` with bundled SQLite 3.45+ (`rusqlite 0.41.1`) and `gil_used = false`, and wire `src/rs/Cargo.{toml,lock}` plus root `pyproject.toml` `[sqlite]`, `uv.sources`, `maturin.exclude`, and `bumpversion`.
- [ ] [t02-stub-makefile-and-release-matrix.md](tasks/t02-stub-makefile-and-release-matrix.md) — Create `src/py/vertebra/_sqlite.pyi` and wire `Makefile` `sqlite-test`/`sqlite-wheel-check` and `.github/workflows/release.yaml`.
- [ ] [t03-sibling-wheel-exclusions-and-isolation-gate.md](tasks/t03-sibling-wheel-exclusions-and-isolation-gate.md) — Exclude `_sqlite.pyi` from all 9 sibling extension `pyproject.toml` manifests and register `sqlite` in `test_optional_driver_wheels.py`.
- [ ] [t04-review-manifest-checklist-and-skills.md](tasks/t04-review-manifest-checklist-and-skills.md) — Add `vertebra-sqlite` paths and `_sqlite.pyi` to `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero sibling wheels ship `_sqlite.pyi`; `_sqlite` compiles against bundled SQLite 3.45+ without OS libraries.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite && make sqlite-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q && make review-check`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^duckdb-cargo]: `src/rs/vertebra-duckdb/Cargo.toml` — Standalone embedded PyO3 wheel Cargo precedent.
[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area path ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
[^pyo3-skill]: `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 native extension engineering skill.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
