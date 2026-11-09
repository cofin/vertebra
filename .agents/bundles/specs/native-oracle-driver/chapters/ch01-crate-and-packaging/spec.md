---
type: Spec
parent_flow: native-oracle-driver
chapter_id: ch01-crate-and-packaging
title: "Chapter 01: Rust Crate Scaffold, Build Matrix, and Optional Wheel Wiring"
description: Scaffold the standalone src/rs/vertebra-oracle PyO3 crate wrapping official pure-Rust oracledb =26.0.0-beta.5 and crate-local arrow 59, register the workspace member and vertebra._oracle wheel/stub, and wire root optional dependencies, Makefile targets, and review area manifests.
status: stable
state: in_progress
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: "native-oracle-driver:ch01:t03"
plan_revision: 3
plan_commit: 769302bc
state_revision: 2
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-09T01:28:30Z"
generated:
  by: flow-plan
  at: "2026-11-09T01:15:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-09T01:28:30Z"
    commit: 769302bc
sources:
  - id: ws-cargo
    resource: src/rs/Cargo.toml
    title: Rust workspace members and shared dependency pins
  - id: spanner-wheel
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: Reference optional PyO3 Maturin wheel layout in vertebra namespace
  - id: spanner-lib
    resource: src/rs/vertebra-spanner/src/lib.rs::_spanner
    title: Reference gil_used=false PyO3 module declaration and FREE_THREADED constant
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public connection constructor options, exports, and wheel isolation contracts
  - id: crate-research
    resource: .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
    title: Primary-source evaluation of official pure-Rust oracledb =26.0.0-beta.5 crate
  - id: root-pyproject
    resource: pyproject.toml
    title: Root vertebra-core optional dependency extras, uv.sources, and bumpversion config
  - id: root-makefile
    resource: Makefile
    title: Workspace dev, build, lint, lint-all, py-test, and wheel verification targets
  - id: review-areas
    resource: .agents/skills/review-core/references/areas.toml
    title: Review area path routing manifest for pyo3-bridge and runtime-ipc
tags:
  - oracle
  - rust
  - pyo3
  - wheel
  - packaging
---

# Chapter 01: Rust Crate Scaffold, Build Matrix, and Optional Wheel Wiring

<!-- truth: start -->
- **Green-Every-Commit Crate Scaffold & Crate-Local Arrow 59**: `ch01:t01` creates `src/rs/vertebra-oracle` (`Cargo.toml`, `build.rs`, `src/lib.rs`) pinning `oracledb = { version = "=26.0.0-beta.5", features = ["arrow"] }`, crate-local `arrow-{array,buffer,ipc,schema} = { version = "59", default-features = false }`, and `zip = { version = "2", default-features = false, features = ["deflate"] }` before `ch01:t02` registers `"vertebra-oracle"` in `src/rs/Cargo.toml` and locks `src/rs/Cargo.lock` [^ws-cargo] [^crate-research].
- **Truthful Free-Threaded `vertebra._oracle` Module & Incremental Stub**: `#[pymodule(gil_used = false)] fn _oracle` exports `__version__`, `FREE_THREADED`, and `GIL_USED = false`, packaged via `src/rs/vertebra-oracle/pyproject.toml` and `src/py/vertebra/_oracle.pyi` (declaring only the symbols registered in `lib.rs` at `ch01:t02` so `mypy.stubtest vertebra._oracle` passes at every commit) [^spanner-wheel] [^spanner-lib] [^ora-contracts].
- **Workspace Build Matrix & Review Routing**: `ch01:t03` wires `pyproject.toml` (`[project.optional-dependencies].oracle = ["vertebra-oracle==0.3.0"]`, `[tool.uv.sources]`, `[[tool.bumpversion.files]]`), `Makefile` (`dev`, `build`, `lint`, `lint-all`, `py-test`, `oracle-test`, `oracle-wheel-check`), and `.agents/skills/review-core/references/areas.toml` (`src/rs/vertebra-oracle/**` and `src/py/vertebra/_oracle.pyi` under `[areas.pyo3-bridge]`) [^root-pyproject] [^root-makefile] [^review-areas].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The native Oracle Thin driver ships as an optional standalone wheel (`vertebra-oracle` -> `vertebra._oracle`, `vertebra-core[oracle]`) sharing the `vertebra` Python namespace without linking C/C++ Oracle Instant Client (`libclntsh` / ODPI-C) or leaking `arrow 59` into `vertebra-core` [^ws-cargo] [^spanner-wheel] [^crate-research]. Chapter 01 scaffolds `src/rs/vertebra-oracle` (`Cargo.toml`, `build.rs`, `src/lib.rs`) with all downstream `oracledb`, `arrow-* = "59"`, TLS, and `.zip` wallet dependencies declared upfront, registers `"vertebra-oracle"` in `src/rs/Cargo.toml` (resolving `src/rs/Cargo.lock` once so downstream chapters compile with `--locked`), authors `src/rs/vertebra-oracle/pyproject.toml` and `src/py/vertebra/_oracle.pyi` (`__version__`, `FREE_THREADED`, `GIL_USED`), and wires root `pyproject.toml`, `Makefile`, and `.agents/skills/review-core/references/areas.toml` [^ora-contracts] [^root-pyproject] [^root-makefile] [^review-areas].

### 1.2 Non-Goals

- Connection configuration validation, `ORA-xxxxx` to SQLSTATE `DbExceptionTypes` error hierarchy, TNS/Wallet/IAM dialers, or worker-thread completion bridges (owned by [ch02-connect-auth-and-error-hierarchy](../ch02-connect-auth-and-error-hierarchy/spec.md)).
- Sibling wheel `_oracle.pyi` exclusions, `tools/scripts/verify_oracle_wheel.py`, and `.github/workflows/release.yaml` CI matrix lanes (owned by `ch07:t02`).
- Public user documentation (`docs/usage/databases.md`, `README.md`, `AGENTS.md`) and OKF knowledge synthesis (`[0/6 files documented]`, owned by `ch07:t03`–`t04`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public interface contracts: [contracts.md](../../contracts.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).

| Existing Seam | Verified Consequence |
| :--- | :--- |
| `src/rs/Cargo.toml` & `src/rs/Cargo.lock` [^ws-cargo] | Declaring `oracledb = { version = "=26.0.0-beta.5", features = ["arrow"] }`, `arrow-{array,buffer,ipc,schema} = { version = "59", default-features = false }`, and `zip = { version = "2", default-features = false, features = ["deflate"] }` in `ch01:t01` before registering the workspace member in `ch01:t02` locks `Cargo.lock` once for all downstream chapters (`ch02`–`ch07`). |
| `src/rs/vertebra-spanner/pyproject.toml` [^spanner-wheel] | Maturin `python-source = "../../py"` with explicit `exclude` globs packages only `vertebra/_oracle.<abi>.*` and `vertebra/_oracle.pyi` inside `.whl` while retaining workspace Cargo sources inside `.tar.gz` sdists. |
| `pyproject.toml`, `Makefile`, & `areas.toml` [^root-pyproject] [^root-makefile] [^review-areas] | Adding `[project.optional-dependencies].oracle`, `Makefile` (`dev`, `build`, `lint`, `lint-all`, `py-test`, `oracle-test`, `oracle-wheel-check`), and `areas.toml` `[areas.pyo3-bridge]` paths (`src/rs/vertebra-oracle/**` and `src/py/vertebra/_oracle.pyi`) integrates `vertebra-oracle` into local and CI gates immediately. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1.1 (`ch01:t01`)**: Create `src/rs/vertebra-oracle/Cargo.toml`, `src/rs/vertebra-oracle/build.rs`, and `src/rs/vertebra-oracle/src/lib.rs` (`[lib] name = "_oracle"`, `crate-type = ["cdylib", "rlib"]`, `#[pymodule(gil_used = false)] fn _oracle`) pinning `oracledb = "=26.0.0-beta.5"` (`features = ["arrow"]`), crate-local `arrow-{array,buffer,ipc,schema} = "59"`, `zip = "2"` (`features = ["deflate"]`), and workspace dependencies without touching `src/rs/Cargo.toml` yet.
- **R1.2 (`ch01:t02`)**: Register `"vertebra-oracle"` in `src/rs/Cargo.toml` (resolving `src/rs/Cargo.lock` so `--locked` workspace checks pass), author `src/rs/vertebra-oracle/pyproject.toml` (`module-name = "vertebra._oracle"`), and author `src/py/vertebra/_oracle.pyi` declaring only `__version__: str`, `FREE_THREADED: bool`, and `GIL_USED: bool` to match `src/lib.rs` at `ch01:t02`.
- **R1.3 (`ch01:t03`)**: Wire `vertebra-oracle==0.3.0` into root `pyproject.toml` (`[project.optional-dependencies].oracle`, `all`, `[tool.uv.sources]`, `[[tool.bumpversion.files]]`), add `vertebra-oracle` to `Makefile` (`dev`, `build`, `py-test`, `lint`, `lint-all`, `oracle-test`, `oracle-wheel-check`), and register `src/rs/vertebra-oracle/**` alongside `src/py/vertebra/_oracle.pyi` under `[areas.pyo3-bridge]` in `.agents/skills/review-core/references/areas.toml`.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-oracle/src/lib.rs`):
  - `#[cfg(Py_GIL_DISABLED)] pub const FREE_THREADED: bool = true;`
  - `#[cfg(not(Py_GIL_DISABLED))] pub const FREE_THREADED: bool = false;`
  - `pub const GIL_USED: bool = false;`
  - `#[pymodule(gil_used = false)] fn _oracle(m: &Bound<'_, PyModule>) -> PyResult<()>` (exports `__version__`, `FREE_THREADED`, `GIL_USED`)
- Python stub (`src/py/vertebra/_oracle.pyi` at `ch01:t02`):
  - `__version__: str`
  - `FREE_THREADED: bool`
  - `GIL_USED: bool`
  - `__all__ = ["FREE_THREADED", "GIL_USED", "__version__"]`
- Root packaging (`pyproject.toml` & `Makefile`):
  - `[project.optional-dependencies] oracle = ["vertebra-oracle==0.3.0"]`
  - `[tool.uv.sources] vertebra-oracle = { path = "src/rs/vertebra-oracle", editable = true }`
  - `Makefile` targets `oracle-test` and `oracle-wheel-check`

### 2.3 Caller -> Seam -> Collaborator Integration Map

```text
Makefile (dev / build / py-test / lint / lint-all / oracle-test / oracle-wheel-check)
  -> maturin (src/rs/vertebra-oracle/pyproject.toml + src/rs/vertebra-oracle/Cargo.toml)
  -> vertebra._oracle (src/rs/vertebra-oracle/src/lib.rs + src/py/vertebra/_oracle.pyi)
  -> mypy.stubtest vertebra._oracle & review_setup.py (.agents/skills/review-core/references/areas.toml)
```

### 2.4 Edge-Case & Failure-Mode Matrix

| Edge Case / Failure Mode | Required Behavior |
| :--- | :--- |
| Crate created before workspace membership (`ch01:t01` vs `ch01:t02`) | `ch01:t01` authors `Cargo.toml`, `build.rs`, and `src/lib.rs` together so `ch01:t02` adds `"vertebra-oracle"` to `src/rs/Cargo.toml` and locks `src/rs/Cargo.lock` in one `--locked`-green commit. |
| Arrow version isolation (`arrow 59` vs workspace `arrow 58`/`60`) | `arrow-array`, `arrow-buffer`, `arrow-ipc`, and `arrow-schema` (`version = "59"`, `default-features = false`) remain strictly inside `src/rs/vertebra-oracle/Cargo.toml` and never enter `[workspace.dependencies]`, `vertebra-core`, or `vertebra-db`. |
| Incremental `mypy.stubtest vertebra._oracle` parity | `src/py/vertebra/_oracle.pyi` in `ch01:t02` declares only `__version__`, `FREE_THREADED`, and `GIL_USED` so `mypy.stubtest vertebra._oracle` succeeds before `SyncOracleClient` / `AsyncOracleClient` are registered in `ch02:t03`. |
| Non-Maturin `cargo build --workspace` link failure on `cdylib` | `Makefile` `dev` and `build` pass `--exclude vertebra-oracle` to `cargo build --workspace` and build `_oracle` via `uv run maturin develop --uv -m src/rs/vertebra-oracle/Cargo.toml` while `[features] default = []` keeps `cargo test --workspace` green. |
| Unrouted files in `make review-route` | Adding `src/rs/vertebra-oracle/**` and `src/py/vertebra/_oracle.pyi` to `[areas.pyo3-bridge].paths` in `areas.toml` ensures `make review-check` and `make review-route` map all Oracle Rust/FFI files cleanly. |

### 2.5 Mandatory Alternatives Comparison

| Seam | Chosen Design | Rejected Alternative | Rationale |
| :--- | :--- | :--- | :--- |
| `arrow 59` dependency placement | Crate-local `arrow-{array,buffer,ipc,schema} = "59"` inside `src/rs/vertebra-oracle/Cargo.toml` | Hoisting `arrow 59` into `src/rs/Cargo.toml` `[workspace.dependencies]` or `vertebra-db` | `oracledb = "=26.0.0-beta.5"` (`features = ["arrow"]`) depends on Arrow 59, whereas sibling crates use Arrow 58/60; crate-local pinning + IPC byte handoff via `ShmArrowStreamWriter` prevents semver conflicts. |
| Initial `_oracle` module exports in `ch01` | Export only `__version__`, `FREE_THREADED`, and `GIL_USED = false` in `ch01`, adding `SyncOracleClient`/`AsyncOracleClient` in `ch02:t03` | Declaring unimplemented stub classes in `_oracle.pyi` during `ch01:t02` | `mypy.stubtest vertebra._oracle` runs in `ch01:t02` and fails if `.pyi` declares classes not yet registered in `src/lib.rs`. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/6 files documented]` (`src/py/vertebra/_oracle.pyi` module docstring and stub constants land in `ch01:t02`; all `6` user documentation and OKF v0.2 knowledge files in the root roadmap are owned by `ch07:t03`–`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| :--- | :--- | :--- | :--- |
| R1 (`Cargo.toml`, `build.rs`, `gil_used=false` `src/lib.rs`) | [t01](tasks/t01-crate-and-pymodule-scaffold.md) | `static_validation` | `tomllib` manifest check + `rustfmt --edition 2024 --check` + `cargo check --workspace --locked` |
| R1 (workspace registration, `pyproject.toml`, `_oracle.pyi` stub) | [t02](tasks/t02-workspace-lock-and-wheel-stub.md) | `static_validation` | `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` + `cargo clippy --locked` + `mypy.stubtest vertebra._oracle` |
| R1 (root `[oracle]` extra, `Makefile` targets, `areas.toml` routing) | [t03](tasks/t03-makefile-extra-and-review-areas.md) | `static_validation` | `make dev`, `make oracle-test`, `make oracle-wheel-check`, `make review-check` |

## 5. Implementation Plan

- [x] [bcfb9a8c] native-oracle-driver:ch01:t01 — Scaffold vertebra-oracle Cargo manifest, build script, and free-threaded PyO3 module (tasks/t01-crate-and-pymodule-scaffold.md)
- [x] native-oracle-driver:ch01:t02 — Register workspace member, Maturin wheel manifest, and _oracle.pyi type stub (tasks/t02-workspace-lock-and-wheel-stub.md)
- [ ] native-oracle-driver:ch01:t03 — Wire root [oracle] extra, Makefile build/test targets, and review area paths (tasks/t03-makefile-extra-and-review-areas.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-oracle --all-targets --locked -- -D warnings && make dev && make oracle-test && make oracle-wheel-check && make review-check && uv run --no-sync python -m mypy.stubtest vertebra._oracle` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Plan Baseline**: `HEAD @ 769302bc`; chapter `state: planned`, `planning_status: complete`, `plan_revision: 3`, `plan_commit: 769302bc`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03` (`3/3` pending).
- **Documentation & OKF Status**: `[0/6 files documented]`; `_oracle.pyi` wired in `t02`, `areas.toml` wired in `t03`, and full docs/OKF scheduled in `ch07:t03`–`t04`.
- **Downstream Dependents**: Unblocks [ch02-connect-auth-and-error-hierarchy](../ch02-connect-auth-and-error-hierarchy/spec.md) (`ch02:t01` depends on `ch01:t03`).

[^ws-cargo]: `src/rs/Cargo.toml` — Rust workspace members and shared dependency pins.
[^spanner-wheel]: `src/rs/vertebra-spanner/pyproject.toml` — Reference optional PyO3 Maturin wheel layout in `vertebra` namespace.
[^spanner-lib]: `src/rs/vertebra-spanner/src/lib.rs::_spanner` — Reference `gil_used=false` PyO3 module declaration and `FREE_THREADED` constant.
[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public connection constructor options, exports, and wheel isolation contracts.
[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Primary-source evaluation of official pure-Rust `oracledb =26.0.0-beta.5` crate.
[^root-pyproject]: `pyproject.toml` — Root `vertebra-core` optional dependency extras, `uv.sources`, and `bumpversion` config.
[^root-makefile]: `Makefile` — Workspace `dev`, `build`, `lint`, `lint-all`, `py-test`, and wheel verification targets.
[^review-areas]: `.agents/skills/review-core/references/areas.toml` — Review area path routing manifest for `pyo3-bridge` and `runtime-ipc`.
