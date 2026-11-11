---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch32-ffi-okf-and-review-routing-sync
title: "Chapter 32: PyO3 FFI OKF, Packaging ADR, Bridge Skills, and Review Routing Synchronization"
description: Synchronize all 9 PyO3 FFI pattern/gotcha/worker OKF chapters, packaging/PGO and ADR-004 chapters, pyo3-bridge skill, and review-core areas/checklist/review-pyo3-bridge routing for vertebra-postgres.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch31-data-and-testing-okf-sync
replan_trigger: null
current_task: "native-postgres-driver:ch32:t01"
plan_revision: 6
plan_commit: 043d8444
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-10T14:45:00Z"
updated_at: "2026-11-10T14:45:00Z"
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 043d8444
sources:
  - id: okf-ffi-pattern
    resource: .agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md
    title: PyO3 0.30.1 FFI and GIL discipline pattern chapter
  - id: okf-ffi-gotcha
    resource: .agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md
    title: PyO3 FFI, embedded globals, and Python runtime gotchas chapter
  - id: okf-worker-ffi
    resource: .agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md
    title: Worker PyO3 FFI surface and database extension boundary chapter
  - id: okf-packaging
    resource: .agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md
    title: Standalone packaging, companion wheel matrix, and PGO pipeline chapter
  - id: okf-adr-004
    resource: .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
    title: ADR-004 optional driver wheels and shared-memory capsule ownership
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area path globs, knowledge lists, and verification commands
tags:
  - postgres
  - pyo3
  - okf
  - skills
  - review
---

# Chapter 32: PyO3 FFI OKF, Packaging ADR, Bridge Skills, and Review Routing Synchronization

<!-- truth: start -->
- **Complete Six-Extension FFI & Packaging Alignment (`t01`–`t02`)**: Synchronizes `ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, `pyo3-ffi-and-batch-dispatch.md` (`212L -> <= 200L`), `packaging-and-pgo.md`, `adr-004-optional-driver-wheels-and-shm-ownership.md`, and `.agents/skills/pyo3-bridge/SKILL.md` from five to all six `gil_used = false` extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`, `_postgres`), covering cross-`cdylib` primitive kwargs, `VTBARROW` `ShmCapsule` ownership, `PendingDeliveryGuard` async drop restoration, and core-5 PGO vs release-LTO `_postgres` packaging [^okf-ffi-pattern] [^okf-ffi-gotcha] [^okf-worker-ffi] [^okf-packaging] [^okf-adr-004].
- **Review Routing & Checklist Synchronization (`t03`)**: Verifies and synchronizes `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` coverage, PostgreSQL OKF `knowledge` entries, and `focused_commands` in `areas.toml`, updates `FFI-09` and `PERF-06` in `checklist.md`, and aligns `review-pyo3-bridge/SKILL.md` (`<= 200L`) so `make review-check` and `make review-route BASE=88c30004` validate and route every PostgreSQL crate/stub change prior to `ch33` [^review-areas].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Across the repository's FFI architecture, packaging decisions, and project-local engineering/review skills, nine files still assume five compiled PyO3 extensions (`_vertebra`, `_bundler`, `_storage`, `_bigquery`, `_spanner`) and four optional companion wheels:
- `ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, and `pyo3-ffi-and-batch-dispatch.md` (`212` lines, over the `200`-line `DOC-08` budget) omit `_postgres` (`vertebra-postgres`), its unique `spawn_rust_db_task_with_attr` drainer attribute, cross-`cdylib` primitive-kwarg boundary, and `PendingDeliveryGuard` async drop delivery restoration [^okf-ffi-pattern] [^okf-ffi-gotcha] [^okf-worker-ffi].
- `packaging-and-pgo.md`, `adr-004-optional-driver-wheels-and-shm-ownership.md`, and `.agents/skills/pyo3-bridge/SKILL.md` omit `vertebra-postgres` (`vertebra-core[postgres]`) wheel/sdist packaging, core-5 `EXTENSION_MANIFESTS` PGO vs release-LTO `_postgres` build boundary, and six-module `mypy.stubtest` verification [^okf-packaging] [^okf-adr-004].
- `.agents/skills/review-core/references/areas.toml`, `checklist.md`, and `review-pyo3-bridge/SKILL.md` still enforce `FFI-09` over five `.pyi` stubs, omit `[dependency-groups].postgres-bench` in `PERF-06`, omit `postgres-native-driver.md` / `postgres-limitations.md` in `runtime-ipc` and `pyo3-bridge` `knowledge` lists, and omit `vertebra-postgres` / `_postgres.pyi` in `review-pyo3-bridge` [^review-areas].

This chapter updates all `9` files across three `static_validation` worksheets (`3` files per task) directly before final review in [ch33-final-review-and-release-gate](../ch33-final-review-and-release-gate/spec.md).

### 1.2 Non-Goals

- Modifying Rust/Python implementation code, `.agents/agents/*.md` subagent prompts (unless validated clean by `make review-check`), public `docs/` (`ch30`), or data/testing/product OKF chapters (`ch31`).
- Executing the final branch-wide correctness and quality review sign-off (`ch33:t01`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch31-data-and-testing-okf-sync](../ch31-data-and-testing-okf-sync/spec.md) (which materializes `postgres-native-driver.md` and `postgres-limitations.md` referenced by `areas.toml`). `areas.toml` already carries top-level `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` globs from `ch08`, which `ch32:t03` verifies alongside knowledge-list and focused-command synchronization so `make review-route BASE=88c30004` covers every touched area cleanly [^review-areas].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1.5 / R27.9 (`ch32:t01`)**: Synchronize `ffi-and-gil-discipline.md` (`<= 200L`), `pyo3-and-python-runtime.md` (`<= 200L`), and trimmed `pyo3-ffi-and-batch-dispatch.md` (`212L -> <= 200L`) to six `#[pymodule(gil_used = false)]` extensions (`_postgres`), distinct per-extension `spawn_rust_db_task_with_attr` drainer attributes, cross-`cdylib` primitive-kwargs (never sharing sibling `Arc`/`PyRef` handles), `VTBARROW` `ShmCapsule` single-claim adoption, and `PendingDeliveryGuard` async drop restoration for cancelled notification/replication `next()` futures.
- **R26.4 / R27.10 (`ch32:t02`)**: Synchronize `packaging-and-pgo.md` (`<= 200L`), `adr-004-optional-driver-wheels-and-shm-ownership.md` (`<= 200L`), and `.agents/skills/pyo3-bridge/SKILL.md` (`<= 200L`) for five standalone optional companion distributions including `vertebra-postgres` (`vertebra-core[postgres]`), core-5 `make build-pgo` vs workspace release-LTO `vertebra-postgres` compilation, `VTBARROW` producer-owned cleanup, and six-extension `pyo3-bridge` rules and `mypy.stubtest` commands.
- **R8.1 / R27.11 (`ch32:t03`)**: Update `.agents/skills/review-core/references/areas.toml` (verifying/preserving `src/rs/vertebra-postgres/**` in `runtime-ipc` and `pyo3-bridge` plus `src/py/vertebra/_postgres.pyi` in `pyo3-bridge`, registering `postgres-native-driver.md` and `postgres-limitations.md` in `knowledge`, and aligning `focused_commands`), update `.agents/skills/review-core/references/checklist.md` (`FFI-09` covering all six `.pyi` stubs including `_postgres.pyi` and `PERF-06` recording `[dependency-groups].postgres-bench`), and update `.agents/skills/review-pyo3-bridge/SKILL.md` (`<= 200L`) to cover `vertebra-postgres` and `_postgres.pyi`, passing `make review-check` and `make review-route BASE=88c30004`.

### 2.2 Validation & Routing Map

- `5` OKF chapters (`ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, `pyo3-ffi-and-batch-dispatch.md`, `packaging-and-pgo.md`, `adr-004-optional-driver-wheels-and-shm-ownership.md`) -> `wc -l <= 200`, truth blocks `<= 40L`, 1:1 `sources:` <-> footnotes, `make review-check`.
- `4` skill/routing files (`pyo3-bridge/SKILL.md`, `areas.toml`, `checklist.md`, `review-pyo3-bridge/SKILL.md`) -> `SKILL.md <= 200L`, `make review-check`, and `make review-route BASE=88c30004`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| `pyo3-ffi-and-batch-dispatch.md` (`212L`) exceeding the `200`-line `DOC-08` budget | Trim blank lines between footnote definitions and condense repetitive method prose so total line count settles strictly `<= 200` after adding `_postgres` and `PendingDeliveryGuard` coverage. |
| `areas.toml` referencing a non-existent `knowledge` path during `make review-check` | `ch32` depends strictly on `ch31` (`ch31:t01` creates `postgres-native-driver.md` and `postgres-limitations.md`), so every `knowledge` entry exists on disk when `ch32:t03` runs `make review-check`. |
| `review-pyo3-bridge/SKILL.md` exceeding `max_review_skill_lines = 200` or violating `reviewer_forbidden_commands` | Keep `Focused Commands` strictly read-only (`cargo test` and `uv run --no-sync` `stubtest`/`mypy`; never `make dev` inside reviewer skills) and well below `200` lines. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Review routing & checklist timing | Synchronize `areas.toml`, `checklist.md`, and `review-pyo3-bridge/SKILL.md` in `ch32:t03` right before `ch33:t01` | Running `ch33:t01` reviews with five-extension `FFI-09` and missing PostgreSQL OKF references in `areas.toml` — leaves reviewers checking an outdated five-stub command without `postgres-native-driver.md` / `postgres-limitations.md` preloaded. |
| PGO scope accuracy in `packaging-and-pgo.md` | Explicitly document `EXTENSION_MANIFESTS` covering the core five extensions while `vertebra-postgres` builds via release LTO `maturin` | Falsely claiming `make build-pgo` already instruments six extensions — contradicts `src/py/vertebra/build/packaging/pgo.py` (`DOC-02`). |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/9 files documented]`

- `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` (`ch32:t01`) — Six `gil_used = false` extensions (`_postgres`), cross-`cdylib` primitive-kwargs & `VTBARROW` `ShmCapsule` ownership, and `PendingDeliveryGuard` cancellation safety (`<= 200L`).
- `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` (`ch32:t01`) — Six extensions, `vertebra-postgres` `-m` `maturin` packaging, `[dependency-groups].postgres-bench` isolation, and async stream drop-guard gotchas (`<= 200L`).
- `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`ch32:t01`) — Trimmed `<= 200L` six-extension surface (`_postgres.pyi`) and updated database extension boundary covering `_bigquery`, `_spanner`, and `_postgres`.
- `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` (`ch32:t02`) — Five optional companion distributions including `vertebra-postgres` (`[postgres]`) in `release.yaml` and core-5 `make build-pgo` vs release-LTO `vertebra-postgres` (`<= 200L`).
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` (`ch32:t02`) — Optional driver wheel decision updated to cover `vertebra-postgres` (`vertebra._postgres`), primitive-kwarg cloud/session isolation, and `VTBARROW` `ShmCapsule` transfer (`<= 200L`).
- `.agents/skills/pyo3-bridge/SKILL.md` (`ch32:t02`) — Six-extension engineering skill (`vertebra-postgres`, `_postgres.pyi`, and six-module `mypy.stubtest`) (`<= 200L`).
- `.agents/skills/review-core/references/areas.toml` (`ch32:t03`) — Verified `src/rs/vertebra-postgres/**` and `src/py/vertebra/_postgres.pyi` routing globs plus PostgreSQL OKF `knowledge` and `focused_commands` registration.
- `.agents/skills/review-core/references/checklist.md` (`ch32:t03`) — Six-extension `FFI-09` `{_vertebra,_bundler,_storage,_bigquery,_postgres,_spanner}.pyi` parity and `[dependency-groups].postgres-bench` coverage in `PERF-06`.
- `.agents/skills/review-pyo3-bridge/SKILL.md` (`ch32:t03`) — Six-extension PyO3 review skill covering `vertebra-postgres` and `_postgres.pyi` with read-only focused verification commands (`<= 200L`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Gate |
| --- | --- | --- | --- |
| R1, R7, R20, R27 (FFI patterns, gotchas, & worker boundary OKF) | [t01](tasks/t01-ffi-gil-and-worker-okf-sync.md) | `static_validation` | `make review-check`, `wc -l <= 200` |
| R1, R20, R26, R27 (Packaging/PGO, ADR-004, & `pyo3-bridge` skill) | [t02](tasks/t02-packaging-adr-and-pyo3-skill-sync.md) | `static_validation` | `make review-check`, `wc -l <= 200` |
| R1, R8, R27 (`areas.toml`, `checklist.md`, & `review-pyo3-bridge` skill) | [t03](tasks/t03-review-areas-checklist-and-bridge-skill.md) | `static_validation` | `make review-check`, `make review-route BASE=88c30004` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch32:t01 — Synchronize FFI/GIL pattern, PyO3 runtime gotchas, and worker batch-dispatch OKF chapters (tasks/t01-ffi-gil-and-worker-okf-sync.md)
- [ ] native-postgres-driver:ch32:t02 — Synchronize packaging-and-pgo, ADR-004, and pyo3-bridge engineering skill for vertebra-postgres (tasks/t02-packaging-adr-and-pyo3-skill-sync.md)
- [ ] native-postgres-driver:ch32:t03 — Synchronize review areas.toml, checklist.md, and review-pyo3-bridge skill routing (tasks/t03-review-areas-checklist-and-bridge-skill.md)

**Chapter Recovery Checkpoint:** `make review-check && make review-route BASE=88c30004` exits `0` with all updated OKF and `SKILL.md` files strictly `<= 200` lines (`[33/33 files documented]` cumulative across `ch30`–`ch32`).

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `043d8444`; `depends_on_chapters: [ch31-data-and-testing-okf-sync]`, `state: planned`, `plan_revision: 6`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending); `[0/9 files documented]`.
- **Downstream Dependents**: Unblocks [ch33-final-review-and-release-gate](../ch33-final-review-and-release-gate/spec.md) (`ch33:t01`) for complete-driver verification, routed correctness review, and fresh zero-context quality review across `88c30004..HEAD`.

[^okf-ffi-pattern]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 0.30.1 FFI and GIL discipline pattern chapter.
[^okf-ffi-gotcha]: `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — PyO3 FFI, embedded globals, and Python runtime gotchas chapter.
[^okf-worker-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — Worker PyO3 FFI surface and database extension boundary chapter.
[^okf-packaging]: `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` — Standalone packaging, companion wheel matrix, and PGO pipeline chapter.
[^okf-adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — ADR-004 optional driver wheels and shared-memory capsule ownership.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review area path globs, knowledge lists, and verification commands.
