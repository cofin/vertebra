---
type: Task
id: "native-sqlite-driver:ch01:t01"
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-packaging
title: "Scaffold vertebra-sqlite Cargo, Maturin Manifest, and PyO3 Module"
description: "Create src/rs/vertebra-sqlite with bundled rusqlite 0.41.1, gil_used = false, and isolated maturin pyproject.toml."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 18baadcd
depends_on: []
files:
  - src/rs/vertebra-sqlite/Cargo.toml
  - src/rs/vertebra-sqlite/pyproject.toml
  - src/rs/vertebra-sqlite/src/lib.rs
tests: []
read_set:
  - src/rs/vertebra-duckdb/Cargo.toml
  - src/rs/vertebra-duckdb/pyproject.toml
  - src/rs/vertebra-duckdb/src/lib.rs
estimated_loc: 145
tags:
  - sqlite
  - rust
  - pyo3
sources:
  - id: duckdb-cargo
    resource: src/rs/vertebra-duckdb/Cargo.toml
    title: "Standalone embedded PyO3 wheel Cargo precedent"
  - id: duckdb-pyproj
    resource: src/rs/vertebra-duckdb/pyproject.toml
    title: "Maturin standalone wheel isolation manifest precedent"
  - id: duckdb-lib
    resource: src/rs/vertebra-duckdb/src/lib.rs
    title: "Embedded worker-thread and VTBARROW PyO3 extension precedent"
---

# Task `t01`: Scaffold vertebra-sqlite Cargo, Maturin Manifest, and PyO3 Module

## Objective & Scope Boundary

Create `src/rs/vertebra-sqlite/{Cargo.toml,pyproject.toml,src/lib.rs}` exporting `__version__`, `FREE_THREADED`, and `GIL_USED = False` per [`../spec.md` §2](../spec.md) [^duckdb-cargo] [^duckdb-pyproj] [^duckdb-lib].

## Bounded Read Manifest

- `src/rs/vertebra-duckdb/Cargo.toml` — Arrow 60.1.0 and workspace dependency conventions.
- `src/rs/vertebra-duckdb/pyproject.toml` — Maturin `module-name`, `include`, and `exclude` globs.
- `src/rs/vertebra-duckdb/src/lib.rs` — `#[pymodule(gil_used = false)]` pattern.

## Contracts & Edge Cases

- Enable `rusqlite` features `bundled`, `modern_sqlite`, `hooks`, `serialize`, `blob`, `column_decltype`, and `backup`.
- Include `vertebra/_sqlite.pyi` in `vertebra-sqlite/pyproject.toml` and exclude `vertebra/**/*.py` plus all sibling `_*.pyi` stubs.
- **Edge Case**: Module initializes without acquiring the GIL on free-threaded Python `3.14t`/`3.15t` (`FFI-01`).

## Test Matrix

- Unit test in `src/lib.rs` asserts bundled SQLite version `>= 3.45.0` (`rusqlite::version_number() >= 3_045_000`) and `GIL_USED == false`.

## Verification

```bash
cargo test --manifest-path src/rs/vertebra-sqlite/Cargo.toml || true
```

## Acceptance Criteria

- [ ] `src/rs/vertebra-sqlite` defines `cdylib` + `rlib` with `#[pymodule(gil_used = false)]`.
- [ ] Zero regressions in the chapter fast verification gate.

[^duckdb-cargo]: `src/rs/vertebra-duckdb/Cargo.toml` — Standalone embedded PyO3 wheel Cargo precedent.
[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
