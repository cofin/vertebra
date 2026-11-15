---
type: Spec
flow_id: native-valkey-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: 'Chapter 01: Crate, Wheel, Stubs, Isolation Gates, and Review Manifests'
description: Scaffold src/rs/vertebra-valkey (vertebra._valkey), add [valkey] to pyproject.toml, author _valkey.pyi, wire Makefile and release workflows, exclude _valkey.pyi from sibling wheels, and register vertebra-valkey in review manifests.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters: []
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-15T20:55:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:55:00Z'
    commit: 246d7d0e
tags:
  - rust
  - pyo3
  - valkey
  - packaging
  - stubtest
  - ci
sources:
  - id: rs-workspace
    resource: src/rs/Cargo.toml
    title: Rust workspace Cargo.toml
  - id: root-pyproject
    resource: pyproject.toml
    title: Root pyproject.toml extras and wheel exclusions
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: Maturin standalone wheel isolation precedent
  - id: makefile
    resource: Makefile
    title: Root Makefile targets
  - id: release-wf
    resource: .github/workflows/release.yaml
    title: GitHub release wheel build matrix
  - id: areas-toml
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area capability manifest
  - id: checklist-md
    resource: .agents/skills/review-core/references/checklist.md
    title: Review checklist items
  - id: pyo3-skill
    resource: .agents/skills/pyo3-bridge/SKILL.md
    title: PyO3 bridge engineering skill
  - id: wheel-tests
    resource: src/py/tests/unit/db/test_optional_driver_wheels.py
    title: Sibling PyO3 wheel isolation test gate
current_task: "native-valkey-driver:ch01-wheel-scaffold-and-isolation:t01"
---

# Chapter 01: Crate, Wheel, Stubs, Isolation Gates, and Review Manifests

<!-- truth: start -->
- `src/rs/vertebra-valkey` builds standalone wheel `vertebra-valkey` (`vertebra._valkey` + `src/py/vertebra/_valkey.pyi`, `#[pymodule(gil_used = false)]`), wired into `pyproject.toml` under `[valkey] = ["vertebra-valkey==0.3.0"]` [^rs-workspace] [^root-pyproject].
- `Makefile` (`dev`, `build`, `lint`, `lint-all`, `py-test`, `valkey-test`, `valkey-wheel-check`) and `.github/workflows/release.yaml` build, stubtest, and verify `vertebra-valkey` wheel isolation (`== 1` `.pyi` stub and `== 1` native library, `< 15 MiB`) [^makefile] [^release-wf].
- Every sibling extension wheel `pyproject.toml` and root `pyproject.toml` excludes `"**/py/vertebra/_valkey.pyi"`, enforced reciprocally in `test_optional_driver_wheels.py`, and `vertebra-valkey` is registered in `areas.toml`, `checklist.md` (`FFI-09`), and `pyo3-bridge/SKILL.md` [^pg-pyproj] [^wheel-tests] [^areas-toml] [^checklist-md] [^pyo3-skill].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`vertebra-valkey` follows the standalone PyO3 extension crate pattern established by `vertebra-redis` and `vertebra-postgres` [^rs-workspace] [^pg-pyproj]. This chapter scaffolds the crate, `.pyi` stub, `Makefile` targets, sibling wheel exclusions, and review manifest registrations so Chapter 02 can implement the Rust RESP3 + Valkey 9.0 wire engine cleanly.

**Non-Goals:** Full RESP3 client implementation (Chapter 02) and Python adapter cutover (Chapter 03).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Crate & workspace | `src/rs/vertebra-valkey/{Cargo.toml,pyproject.toml,src/lib.rs}`, `src/rs/Cargo.toml`, `pyproject.toml` [^rs-workspace] [^root-pyproject] | `vertebra._valkey`, `[valkey] = ["vertebra-valkey==0.3.0"]` |
| Stubs & build | `src/py/vertebra/_valkey.pyi`, `Makefile`, `.github/workflows/release.yaml` [^makefile] [^release-wf] | `mypy.stubtest vertebra._valkey`, `make valkey-test`, `make valkey-wheel-check` |
| Wheel isolation | `src/rs/vertebra-*/pyproject.toml`, `src/py/tests/unit/db/test_optional_driver_wheels.py` [^pg-pyproj] [^wheel-tests] | `"**/py/vertebra/_valkey.pyi"` excluded from sibling wheels |
| Review manifests | `.agents/skills/review-core/references/{areas.toml,checklist.md}`, `.agents/skills/pyo3-bridge/SKILL.md` [^areas-toml] [^checklist-md] [^pyo3-skill] | `FFI-09` and `pyo3-bridge` scope include `vertebra-valkey` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [PyO3 0.30.1 FFI & GIL Discipline](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md), [Project Workflow](../../../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1**: Create `src/rs/vertebra-valkey/{Cargo.toml,pyproject.toml,src/lib.rs}`, register `vertebra-valkey` in `src/rs/Cargo.toml`, and add `[valkey] = ["vertebra-valkey==0.3.0"]` and the root wheel exclusion `{ path = "src/py/vertebra/_valkey.pyi", format = "wheel" }` to root `pyproject.toml` (`t01`).
- **R2**: Create `src/py/vertebra/_valkey.pyi` (scaffold surface only: `__version__`, `FREE_THREADED`, `GIL_USED`, and hash-slot helpers; client classes land in Chapter 02) and add `vertebra-valkey` to `Makefile` (`dev`, `build`, `lint`, `lint-all`, `py-test`, `valkey-test`, `valkey-wheel-check`) and `.github/workflows/release.yaml` (`t02`).
- **R3a**: Add `"**/py/vertebra/_valkey.pyi"` to every sibling `src/rs/vertebra-*/pyproject.toml` exclusion list (the 9 current siblings plus `vertebra-redis` and `vertebra-sqlite`) and register `valkey` in `src/py/tests/unit/db/test_optional_driver_wheels.py` (`t03`).
- **R3b**: Register `src/rs/vertebra-valkey/**` and `src/py/vertebra/_valkey.pyi` in `.agents/skills/review-core/references/areas.toml`, `checklist.md` (`FFI-09`), and `.agents/skills/pyo3-bridge/SKILL.md` (`t04`).

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Free-threaded Python | `import vertebra._valkey` on Python 3.14t/3.15t | `#[pymodule(gil_used = false)]` keeps GIL disabled | `FFI-01` compliance |
| Sibling wheel build | `make valkey-wheel-check` and `test_optional_driver_wheels.py` | Each wheel ships `== 1` `.pyi` and `== 1` `.so`/`.dylib`/`.pyd` | Wheel isolation (`test_optional_driver_wheels.py`) and `FFI-09` stub parity |

## 3. Documentation & OKF Knowledge Plan

Task `t04` updates `.agents/skills/review-core/references/areas.toml`, `.agents/skills/review-core/references/checklist.md`, and `.agents/skills/pyo3-bridge/SKILL.md` (`<= 200` lines); public docs and OKF chapters are updated in Chapter 04.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Scaffold vertebra-valkey Crate, Workspace Member, and Root Extras | `src/rs/vertebra-valkey/{Cargo.toml,pyproject.toml,src/lib.rs}`, `src/rs/Cargo.toml`, `pyproject.toml` | `cargo check -p vertebra-valkey` |
| `t02` | Author _valkey.pyi Stub and Wire Makefile and Release Workflows | `src/py/vertebra/_valkey.pyi`, `Makefile`, `.github/workflows/release.yaml` | `mypy.stubtest vertebra._valkey` |
| `t03` | Exclude _valkey.pyi from Sibling Wheels and Add Wheel Isolation Gate | `src/rs/vertebra-*/pyproject.toml`, `src/py/tests/unit/db/test_optional_driver_wheels.py` | `make valkey-wheel-check` |
| `t04` | Register vertebra-valkey in Review Capability Manifests and PyO3 Skills | `.agents/skills/review-core/references/{areas.toml,checklist.md}`, `.agents/skills/pyo3-bridge/SKILL.md` | `make review-check` |

## 5. Implementation Plan

- [ ] [t01-crate-workspace-and-root-extras.md](tasks/t01-crate-workspace-and-root-extras.md) — Scaffold `src/rs/vertebra-valkey`, register workspace member, and wire `[valkey]` in `pyproject.toml`.
- [ ] [t02-stub-makefile-and-release-matrix.md](tasks/t02-stub-makefile-and-release-matrix.md) — Author `src/py/vertebra/_valkey.pyi` and wire `Makefile` (`valkey-test`, `valkey-wheel-check`) and `.github/workflows/release.yaml`.
- [ ] [t03-sibling-wheel-exclusions-and-isolation-gate.md](tasks/t03-sibling-wheel-exclusions-and-isolation-gate.md) — Exclude `"**/py/vertebra/_valkey.pyi"` across sibling extension `pyproject.toml` files and register `valkey` in `test_optional_driver_wheels.py`.
- [ ] [t04-review-manifests-and-skill-routing.md](tasks/t04-review-manifests-and-skill-routing.md) — Register `vertebra-valkey` and `_valkey.pyi` in `areas.toml`, `checklist.md`, and `pyo3-bridge/SKILL.md`.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `mypy.stubtest vertebra._valkey` and `make review-check` must pass with zero warnings.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-valkey && make valkey-wheel-check && uv run --no-sync pytest src/py/tests/unit/db/test_optional_driver_wheels.py -q && make review-check`

[^rs-workspace]: `src/rs/Cargo.toml` — Rust workspace Cargo.toml.
[^root-pyproject]: `pyproject.toml` — Root pyproject.toml extras and wheel exclusions.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^makefile]: `Makefile` — Root Makefile targets.
[^release-wf]: `.github/workflows/release.yaml` — GitHub release wheel build matrix.
[^areas-toml]: `.agents/skills/review-core/references/areas.toml` — Review area capability manifest.
[^checklist-md]: `.agents/skills/review-core/references/checklist.md` — Review checklist items.
[^pyo3-skill]: `.agents/skills/pyo3-bridge/SKILL.md` — PyO3 bridge engineering skill.
[^wheel-tests]: `src/py/tests/unit/db/test_optional_driver_wheels.py` — Sibling PyO3 wheel isolation test gate.
