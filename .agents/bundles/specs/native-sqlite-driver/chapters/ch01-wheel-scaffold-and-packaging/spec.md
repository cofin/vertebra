---
type: Spec
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-packaging
title: "Chapter 01: Standalone vertebra-sqlite Crate, Wheel Extras, Build Targets, and Review Manifests"
description: "Scaffold src/rs/vertebra-sqlite (vertebra._sqlite, gil_used = false) over bundled rusqlite 0.41.1, wire vertebra-core[sqlite] packaging, Makefile targets, release matrix, and review manifests."
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
  - rust
  - wheel
  - packaging
  - pyo3
sources:
  - id: cargo-ws
    resource: src/rs/Cargo.toml
    title: "Rust workspace member manifest"
  - id: duckdb-cargo
    resource: src/rs/vertebra-duckdb/Cargo.toml
    title: "Standalone embedded PyO3 wheel Cargo precedent"
  - id: duckdb-pyproj
    resource: src/rs/vertebra-duckdb/pyproject.toml
    title: "Maturin standalone wheel isolation manifest precedent"
  - id: root-pyproject
    resource: pyproject.toml
    title: "Root vertebra-core extras, uv sources, maturin excludes, and bumpversion"
  - id: makefile
    resource: Makefile
    title: "Polyglot build, lint, stubtest, and wheel-check targets"
  - id: release-wf
    resource: .github/workflows/release.yaml
    title: "Release wheel and sdist build matrix"
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: "Review router area path ownership manifest"
  - id: review-checklist
    resource: .agents/skills/review-core/references/checklist.md
    title: "Review checklist including FFI-09 stub list"
---

# Chapter 01: Standalone vertebra-sqlite Crate, Wheel Extras, Build Targets, and Review Manifests

<!-- truth: start -->
- `src/rs/vertebra-sqlite` compiles `vertebra._sqlite` (`crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)]`) against `rusqlite = "=0.41.1"` with `bundled`, `modern_sqlite`, `hooks`, `serialize`, `blob`, `column_decltype`, and `backup` (zero system `libsqlite3`) [^duckdb-cargo] [^duckdb-pyproj].
- `pyproject.toml` exposes `vertebra-core[sqlite]` and adds `vertebra-sqlite` to `[project.optional-dependencies].all`, `[tool.uv.sources]`, `[tool.maturin].exclude`, and `[[tool.bumpversion.files]]` [^root-pyproject].
- `Makefile` adds `sqlite-test` and `sqlite-wheel-check`, and `.github/workflows/release.yaml` builds wheels + sdist for `vertebra-sqlite` [^makefile] [^release-wf].
- `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` register `vertebra-sqlite` and `_sqlite.pyi` [^review-areas] [^review-checklist].
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

## 2. Requirements, Concrete Contracts & Architecture

- **R1**: Scaffold `src/rs/vertebra-sqlite` (`Cargo.toml`, `pyproject.toml`, `src/lib.rs`) over `rusqlite = "=0.41.1"` (`bundled`, `modern_sqlite`, `backup`, `hooks`, `serialize`, `blob`, `column_decltype`) with `#[pymodule(gil_used = false)]` (`t01`).
- **R2**: Register `vertebra-sqlite` in `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and root `pyproject.toml` (`[sqlite]` extra, `all`, `uv.sources`, `maturin.exclude`, `bumpversion`) (`t02`).
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
vertebra-core = { path = "../vertebra-core" }
vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }
```

**Integration Flow:**
- `pip install vertebra-core[sqlite]` -> installs `vertebra-sqlite` wheel providing `vertebra._sqlite` and `vertebra/_sqlite.pyi`.
- `make review-check` routes `src/rs/vertebra-sqlite/**` and `src/py/vertebra/_sqlite.pyi` to `pyo3-bridge` (`FFI`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| System libsqlite3 absent | Clean container without `libsqlite3-dev` | Compiles bundled SQLite 3.45+ C amalgamation cleanly | Zero system C deps |
| Orphan source file | `make review-check` scans tracked files | Owned by `[areas.pyo3-bridge]` in `areas.toml` | Zero unrouted files |

## 3. Documentation & OKF Knowledge Plan

Skill and reference updates complete in `ch05`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` | `t01` | `src/rs/vertebra-sqlite/{Cargo.toml,pyproject.toml,src/lib.rs}` | `cargo check -p vertebra-sqlite` |
| `R2` | `t02` | `src/rs/Cargo.{toml,lock}`, `pyproject.toml` | `cargo test -p vertebra-sqlite` |
| `R3` | `t03` | `src/py/vertebra/_sqlite.pyi`, `Makefile`, `.github/workflows/release.yaml` | `mypy.stubtest vertebra._sqlite` |
| `R4` | `t04` | `areas.toml`, `checklist.md`, `pyo3-bridge/SKILL.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-crate-and-pymodule-scaffold.md](tasks/t01-crate-and-pymodule-scaffold.md) — Create src/rs/vertebra-sqlite with bundled rusqlite 0.41.1, gil_used = false, and isolated maturin pyproject.toml.
- [ ] [t02-workspace-and-root-pyproject.md](tasks/t02-workspace-and-root-pyproject.md) — Add vertebra-sqlite to Cargo workspace, [project.optional-dependencies].sqlite/all, uv.sources, maturin.exclude, and bumpversion.
- [ ] [t03-stub-makefile-and-release-matrix.md](tasks/t03-stub-makefile-and-release-matrix.md) — Create src/py/vertebra/_sqlite.pyi and wire Makefile sqlite-test/sqlite-wheel-check and .github/workflows/release.yaml.
- [ ] [t04-review-manifest-checklist-and-skills.md](tasks/t04-review-manifest-checklist-and-skills.md) — Add vertebra-sqlite paths and _sqlite.pyi to areas.toml, checklist.md (FFI-09), and pyo3-bridge/SKILL.md.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `#[pymodule(gil_used = false)]` is set from commit 1; `vertebra-core` wheel excludes `_sqlite.pyi`.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite && make review-check`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^duckdb-cargo]: `src/rs/vertebra-duckdb/Cargo.toml` — Standalone embedded PyO3 wheel Cargo precedent.
[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^makefile]: `Makefile` — Polyglot build, lint, stubtest, and wheel-check targets.
[^release-wf]: `.github/workflows/release.yaml` — Release wheel and sdist build matrix.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review router area path ownership manifest.
[^review-checklist]: `.agents/skills/review-core/references/checklist.md` — Review checklist including FFI-09 stub list.
