---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch32-ffi-okf-and-review-routing-sync
title: "Chapter 32: PyO3 FFI OKF, Packaging ADR, Bridge Skills, and Review Routing Synchronization"
description: Synchronize PyO3 FFI pattern/gotcha/worker OKF chapters, packaging-and-pgo and ADR-004 chapters, the pyo3-bridge skill, and review-core areas/checklist/review-pyo3-bridge routing for vertebra-postgres.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch31-data-and-testing-okf-sync
replan_trigger: null
current_task: "native-postgres-driver:ch32:t01"
plan_revision: 7
plan_commit: c8722c76
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-10T14:45:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
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
  - id: pgo-discovery
    resource: tools/pgo/rust_pgo.py::discover_extension_manifests
    title: Rust PGO extension discovery from crate-local maturin pyproject.toml files
tags:
  - postgres
  - pyo3
  - okf
  - skills
  - review
---

# Chapter 32: PyO3 FFI OKF, Packaging ADR, Bridge Skills, and Review Routing Synchronization

<!-- truth: start -->
- **FFI & Packaging Alignment (`t01`–`t02`)**: Synchronizes `ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, `pyo3-ffi-and-batch-dispatch.md` (`212L` at `c8722c76` -> `<= 200L`), `packaging-and-pgo.md`, `adr-004-optional-driver-wheels-and-shm-ownership.md`, and `.agents/skills/pyo3-bridge/SKILL.md` by adding `_postgres` / `vertebra-postgres` to their extension, crate, and companion-wheel enumerations (each reconciled against the `Makefile` `lint` stubtest list and `src/rs/*/pyproject.toml` at execution time, never a fixed count), covering cross-`cdylib` primitive kwargs, `VTBARROW` `ShmCapsule` ownership, and `PendingDeliveryGuard` async drop restoration; `packaging-and-pgo.md` already documents discovery-based PGO through `tools/pgo/rust_pgo.py::discover_extension_manifests`, which includes `vertebra-postgres` [^okf-ffi-pattern] [^okf-ffi-gotcha] [^okf-worker-ffi] [^okf-packaging] [^okf-adr-004] [^pgo-discovery].
- **Review Routing & Checklist Synchronization (`t03`)**: `areas.toml` already routes `src/rs/vertebra-postgres/**` (`runtime-ipc`, `pyo3-bridge`) and `src/py/vertebra/_postgres.pyi` (`pyo3-bridge`); `t03` adds PostgreSQL OKF `knowledge` entries, a `database-and-postgres` `runtime-ipc` shard, and `-p vertebra-postgres` focused commands, updates checklist rules `FFI-09` (stub parity) and `PERF-06` (benchmark dependency groups), and aligns `review-pyo3-bridge/SKILL.md` (`<= 200L`) so `make review-check` and `make review-route BASE=c8722c76` validate and route every PostgreSQL crate/stub change before `ch33` [^review-areas].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Across the repository's FFI architecture, packaging decisions, and project-local engineering/review skills, nine files omit `vertebra-postgres` at `c8722c76`, and several carry stale extension counts (for example "all five extensions" or "all 6 PyO3 extensions") that no longer match the `Makefile` `lint` stubtest list:
- `ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, and `pyo3-ffi-and-batch-dispatch.md` (`212` lines, over the `200`-line budget) omit `_postgres` (`vertebra-postgres`), its distinct `spawn_rust_db_task_with_attr` drainer attribute, the cross-`cdylib` primitive-kwarg boundary, and `PendingDeliveryGuard` async drop delivery restoration [^okf-ffi-pattern] [^okf-ffi-gotcha] [^okf-worker-ffi].
- `packaging-and-pgo.md` (whose `Release Wheel Matrix` names "the four companion extension manifests" and whose `Optional extension distributions` section lists `vertebra-bundler`, `vertebra-bigquery`, `vertebra-spanner`, and `vertebra-storage`), `adr-004-optional-driver-wheels-and-shm-ownership.md`, and `.agents/skills/pyo3-bridge/SKILL.md` omit `vertebra-postgres` (`vertebra-core[postgres]`) wheel/sdist packaging and `vertebra._postgres` `mypy.stubtest` verification [^okf-packaging] [^okf-adr-004].
- `.agents/skills/review-core/references/checklist.md` `FFI-09` enumerates `{_vertebra,_bundler,_storage,_bigquery,_spanner,_duckdb}.pyi` as "all 6 PyO3 extensions", `PERF-06` names only `[dependency-groups].bench`, `areas.toml` `knowledge` lists omit PostgreSQL OKF chapters, and `review-pyo3-bridge/SKILL.md` omits `vertebra-postgres` / `_postgres.pyi` [^review-areas].

This chapter updates all `9` files across three `static_validation` worksheets (`3` files per task) directly before final review in [ch33-final-review-and-release-gate](../ch33-final-review-and-release-gate/spec.md).

### 1.2 Non-Goals

- Modifying Rust/Python implementation code, `tools/pgo/`, `.agents/agents/*.md` subagent prompts (unless validated clean by `make review-check`), public `docs/` (`ch30`), or data/testing/product OKF chapters (`ch31`).
- Executing the final branch-wide correctness and quality review sign-off (`ch33:t01`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch31-data-and-testing-okf-sync](../ch31-data-and-testing-okf-sync/spec.md) (which materializes `postgres-native-driver.md`; `postgres-limitations.md` exists from `ch22:t04`). At `c8722c76`, `areas.toml` carries `src/rs/vertebra-postgres/**` in `[areas.runtime-ipc].paths` and `[areas.pyo3-bridge].paths` and `src/py/vertebra/_postgres.pyi` in `[areas.pyo3-bridge].paths`, with `rings` and `platform-and-transport` as the only `runtime-ipc` shards [^review-areas]. `tools/pgo/rust_pgo.py::discover_extension_manifests` returns `vertebra-py` plus every crate with a crate-local maturin `pyproject.toml`, and `EXTENSION_MANIFESTS` is that call evaluated at import, so `make build-pgo` profiles `vertebra-postgres` without code changes [^pgo-discovery].

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1.5 / R27.9 (`ch32:t01`)**: Synchronize `ffi-and-gil-discipline.md` (`<= 200L`), `pyo3-and-python-runtime.md` (`<= 200L`), and trimmed `pyo3-ffi-and-batch-dispatch.md` (`212L -> <= 200L`) so every `#[pymodule(gil_used = false)]` extension enumeration matches the `Makefile` `lint` stubtest list (including `_postgres`), documenting distinct per-extension `spawn_rust_db_task_with_attr` drainer attributes, cross-`cdylib` primitive kwargs (never sharing sibling `Arc`/`PyRef` handles), `VTBARROW` `ShmCapsule` single-claim adoption, and `PendingDeliveryGuard` async drop restoration for cancelled notification/replication `next()` futures.
- **R26.4 / R27.10 (`ch32:t02`)**: Synchronize `packaging-and-pgo.md` (`<= 200L`), `adr-004-optional-driver-wheels-and-shm-ownership.md` (`<= 200L`), and `.agents/skills/pyo3-bridge/SKILL.md` (`<= 200L`) by adding `vertebra-postgres` (`vertebra-core[postgres]`) to the companion wheel inventory (matching `src/rs/*/pyproject.toml`), `make postgres-wheel-check` isolation verification, `VTBARROW` producer-owned cleanup, and `vertebra._postgres` in the `pyo3-bridge` crate/stub enumerations and `mypy.stubtest` command; the discovery-based PGO paragraph stays as written apart from naming `vertebra-postgres` as a discovered crate where the inventory needs it.
- **R8.1 / R27.11 (`ch32:t03`)**: Update `.agents/skills/review-core/references/areas.toml` (preserving the existing PostgreSQL globs, adding a `database-and-postgres` `runtime-ipc` shard, registering `postgres-native-driver.md` and `postgres-limitations.md` in `knowledge`, and adding `-p vertebra-postgres` to `focused_commands`), update `.agents/skills/review-core/references/checklist.md` (`FFI-09` enumerating every `.pyi` stub in the `Makefile` `lint` stubtest list including `_postgres.pyi`; `PERF-06` recording `[dependency-groups].postgres-bench`), and update `.agents/skills/review-pyo3-bridge/SKILL.md` (`<= 200L`) to cover `vertebra-postgres` and `_postgres.pyi`, passing `make review-check` and `make review-route BASE=c8722c76`.

### 2.2 Validation & Routing Map

- `5` OKF chapters (`ffi-and-gil-discipline.md`, `pyo3-and-python-runtime.md`, `pyo3-ffi-and-batch-dispatch.md`, `packaging-and-pgo.md`, `adr-004-optional-driver-wheels-and-shm-ownership.md`) -> `wc -l <= 200`, truth blocks `<= 40L`, 1:1 `sources:` <-> footnotes, `make review-check`.
- `4` skill/routing files (`pyo3-bridge/SKILL.md`, `areas.toml`, `checklist.md`, `review-pyo3-bridge/SKILL.md`) -> `SKILL.md <= 200L`, `make review-check`, and `make review-route BASE=c8722c76`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| `pyo3-ffi-and-batch-dispatch.md` (`212L`) exceeding the `200`-line budget | Trim blank lines between footnote definitions and condense repetitive method prose so the total lands `<= 200` after adding `_postgres` and `PendingDeliveryGuard` coverage. |
| `areas.toml` referencing a non-existent `knowledge` path during `make review-check` | `ch32` depends on `ch31` (`ch31:t01` creates `postgres-native-driver.md`; `ch22:t04` creates `postgres-limitations.md`), so every `knowledge` entry exists on disk when `ch32:t03` runs `make review-check`. |
| Sibling driver flows editing the same enumerations | Every extension, crate, and wheel list is compared against the `Makefile` `lint` stubtest list, `src/rs/*/Cargo.toml`, and `src/rs/*/pyproject.toml` at execution time; count words ("five", "six", "all 6") are replaced with plain enumerations. |
| `review-pyo3-bridge/SKILL.md` exceeding `max_review_skill_lines = 200` or violating `reviewer_forbidden_commands` | Keep `Focused Commands` read-only (`cargo test` and `uv run --no-sync` `stubtest`/`mypy`; never `make dev` inside reviewer skills) and well below `200` lines. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Review routing & checklist timing | Synchronize `areas.toml`, `checklist.md`, and `review-pyo3-bridge/SKILL.md` in `ch32:t03` right before `ch33:t01` | Running `ch33:t01` reviews with an `FFI-09` stub list lacking `_postgres.pyi` and without PostgreSQL OKF references in `areas.toml` — leaves reviewers checking an incomplete stubtest command without `postgres-native-driver.md` / `postgres-limitations.md` preloaded. |
| PGO documentation for `vertebra-postgres` | Keep the existing discovery-based description in `packaging-and-pgo.md` (`discover_extension_manifests` over crate-local `pyproject.toml` files) and name `vertebra-postgres` only in the wheel inventory | Adding a separate PostgreSQL PGO boundary or a static manifest list — contradicts `tools/pgo/rust_pgo.py` (checklist rule `DOC-02`) and drifts every time a driver wheel is added. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/9 files documented]`

- `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` (`ch32:t01`) — `_postgres` in the `gil_used = false` extension enumeration, cross-`cdylib` primitive kwargs & `VTBARROW` `ShmCapsule` ownership, and `PendingDeliveryGuard` cancellation safety (`<= 200L`).
- `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` (`ch32:t01`) — `_postgres` in the extension enumeration, `vertebra-postgres` `-m` `maturin` packaging, `[dependency-groups].postgres-bench` isolation, and async stream drop-guard gotchas (`<= 200L`).
- `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` (`ch32:t01`) — Trimmed `<= 200L` extension surface including `_postgres.pyi` and a database extension boundary covering `_postgres` beside the existing optional database modules.
- `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` (`ch32:t02`) — `vertebra-postgres` (`[postgres]`) in the `release.yaml` companion wheel inventory, `make postgres-wheel-check`, and unchanged discovery-based `make build-pgo` (`<= 200L`).
- `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` (`ch32:t02`) — Optional driver wheel decision covering `vertebra-postgres` (`vertebra._postgres`), primitive-kwarg cloud/session isolation, and `VTBARROW` `ShmCapsule` transfer (`<= 200L`).
- `.agents/skills/pyo3-bridge/SKILL.md` (`ch32:t02`) — Engineering skill crate/stub enumerations and `mypy.stubtest` command including `vertebra-postgres` / `_postgres.pyi` (`<= 200L`).
- `.agents/skills/review-core/references/areas.toml` (`ch32:t03`) — Preserved PostgreSQL routing globs plus `database-and-postgres` shard, PostgreSQL OKF `knowledge`, and `focused_commands` registration.
- `.agents/skills/review-core/references/checklist.md` (`ch32:t03`) — `FFI-09` stub parity over every module in the `Makefile` `lint` stubtest list including `_postgres.pyi`, and `[dependency-groups].postgres-bench` coverage in `PERF-06`.
- `.agents/skills/review-pyo3-bridge/SKILL.md` (`ch32:t03`) — PyO3 review skill covering `vertebra-postgres` and `_postgres.pyi` with read-only focused verification commands (`<= 200L`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Gate |
| --- | --- | --- | --- |
| R1, R7, R20, R27 (FFI patterns, gotchas, & worker boundary OKF) | [t01](tasks/t01-ffi-gil-and-worker-okf-sync.md) | `static_validation` | `make review-check`, `wc -l <= 200` |
| R1, R20, R26, R27 (Packaging/PGO, ADR-004, & `pyo3-bridge` skill) | [t02](tasks/t02-packaging-adr-and-pyo3-skill-sync.md) | `static_validation` | `make review-check`, `wc -l <= 200` |
| R1, R8, R27 (`areas.toml`, `checklist.md`, & `review-pyo3-bridge` skill) | [t03](tasks/t03-review-areas-checklist-and-bridge-skill.md) | `static_validation` | `make review-check`, `make review-route BASE=c8722c76` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch32:t01 — Synchronize FFI/GIL pattern, PyO3 runtime gotchas, and worker batch-dispatch OKF chapters (tasks/t01-ffi-gil-and-worker-okf-sync.md)
- [ ] native-postgres-driver:ch32:t02 — Synchronize packaging-and-pgo, ADR-004, and pyo3-bridge engineering skill for vertebra-postgres (tasks/t02-packaging-adr-and-pyo3-skill-sync.md)
- [ ] native-postgres-driver:ch32:t03 — Synchronize review areas.toml, checklist.md, and review-pyo3-bridge skill routing (tasks/t03-review-areas-checklist-and-bridge-skill.md)

**Chapter Recovery Checkpoint:** `make review-check && make review-route BASE=c8722c76` exits `0` with all updated OKF and `SKILL.md` files `<= 200` lines (`[33/33 files documented]` cumulative across `ch30`–`ch32`).

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_commit: c8722c76`, `plan_revision: 7`; `depends_on_chapters: [ch31-data-and-testing-okf-sync]`, `state: planned`. The flow spans `33` chapters / `119` tasks; `ch01`–`ch21` plus `ch22:t01`–`t02` (`80` tasks) landed on `main` in `c8722c76`, and the remaining chapters ship in a follow-up pull request.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending); `[0/9 files documented]`.
- **Downstream Dependents**: Unblocks [ch33-final-review-and-release-gate](../ch33-final-review-and-release-gate/spec.md) (`ch33:t01`) for complete-driver verification, routed correctness review, and fresh zero-context quality review across `c8722c76..HEAD`.

[^okf-ffi-pattern]: `.agents/bundles/knowledge/patterns/rust/ffi-and-gil-discipline.md` — PyO3 0.30.1 FFI and GIL discipline pattern chapter.
[^okf-ffi-gotcha]: `.agents/bundles/knowledge/gotchas/pyo3-and-python-runtime.md` — PyO3 FFI, embedded globals, and Python runtime gotchas chapter.
[^okf-worker-ffi]: `.agents/bundles/knowledge/architecture/worker/pyo3-ffi-and-batch-dispatch.md` — Worker PyO3 FFI surface and database extension boundary chapter.
[^okf-packaging]: `.agents/bundles/knowledge/domains/frontend/packaging-and-pgo.md` — Standalone packaging, companion wheel matrix, and PGO pipeline chapter.
[^okf-adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — ADR-004 optional driver wheels and shared-memory capsule ownership.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review area path globs, knowledge lists, and verification commands.
[^pgo-discovery]: `tools/pgo/rust_pgo.py::discover_extension_manifests` — Rust PGO extension discovery from crate-local maturin `pyproject.toml` files.
