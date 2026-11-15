---
type: Task
id: native-valkey-driver:ch01-wheel-scaffold-and-isolation:t01
flow_id: native-valkey-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Scaffold vertebra-valkey Crate, Workspace Member, and Root Extras
description: Create src/rs/vertebra-valkey/{Cargo.toml,pyproject.toml,src/lib.rs}, register in src/rs/Cargo.toml, and add [valkey] = ["vertebra-valkey==0.3.0"] to root pyproject.toml.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 473e1f2e
depends_on: []
files:
  - src/rs/vertebra-valkey/Cargo.toml
  - src/rs/vertebra-valkey/pyproject.toml
  - src/rs/vertebra-valkey/src/lib.rs
  - src/rs/Cargo.toml
  - src/rs/Cargo.lock
  - pyproject.toml
tests: []
read_set:
  - src/rs/Cargo.toml
  - src/rs/vertebra-oracle/Cargo.toml
  - src/rs/vertebra-postgres/pyproject.toml
  - pyproject.toml
estimated_loc: 180
tags:
  - rust
  - pyo3
  - valkey
  - packaging
sources:
  - id: rs-workspace
    resource: src/rs/Cargo.toml
    title: Rust workspace Cargo.toml
  - id: pg-pyproj
    resource: src/rs/vertebra-postgres/pyproject.toml
    title: Maturin standalone wheel isolation precedent
  - id: root-pyproject
    resource: pyproject.toml
    title: Root pyproject.toml extras and wheel exclusions
verification_strategy: implementation_first
---

# Task `t01`: Scaffold vertebra-valkey Crate, Workspace Member, and Root Extras

## Objective & Scope Boundary

Create `src/rs/vertebra-valkey/{Cargo.toml,pyproject.toml,src/lib.rs}` (`#[pymodule(gil_used = false)] fn _valkey`), register `"vertebra-valkey"` in `src/rs/Cargo.toml` and `src/rs/Cargo.lock`, and wire `valkey = ["vertebra-valkey==0.3.0"]`, `[tool.uv.sources]`, `[tool.maturin].exclude` (`{ path = "src/py/vertebra/_valkey.pyi", format = "wheel" }`, the root table form), and `bumpversion` in root `pyproject.toml` per [`../spec.md` §2](../spec.md) [^rs-workspace] [^pg-pyproj] [^root-pyproject].

## Bounded Read Manifest

- `src/rs/Cargo.toml` — Workspace members and shared dependencies.
- `src/rs/vertebra-oracle/Cargo.toml` — Canonical PyO3 extension crate manifest.
- `src/rs/vertebra-postgres/pyproject.toml` — Canonical maturin wheel manifest.
- `pyproject.toml` — Optional dependencies (`[valkey]`), `uv.sources`, and wheel exclusions.

## Contracts & Edge Cases

- `src/rs/vertebra-valkey/Cargo.toml` sets `name = "vertebra-valkey"`, `[lib] name = "_valkey"`, `crate-type = ["cdylib", "rlib"]`, and depends on `vertebra-core` and `vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }`.
- Root `pyproject.toml` sets `valkey = ["vertebra-valkey==0.3.0"]` (omitting `[project.optional-dependencies].all` per `test_optional_driver_wheels.py`), maps `vertebra-valkey` in `[tool.uv.sources]`, and excludes `{ path = "src/py/vertebra/_valkey.pyi", format = "wheel" }` from the `vertebra-core` wheel (the `"**/py/vertebra/…"` glob form belongs to sibling crate manifests).
- **Edge Case**: Declare `#[pymodule(gil_used = false)]` on `_valkey` so free-threaded Python 3.14t/3.15t never re-enables the GIL.

## Test Matrix

- `cargo check --manifest-path src/rs/Cargo.toml -p vertebra-valkey` succeeds with zero warnings.

## Verification

```bash
cargo check --manifest-path src/rs/Cargo.toml -p vertebra-valkey
```

## Acceptance Criteria

- [ ] `src/rs/vertebra-valkey` compiles cleanly as part of the Rust workspace.
- [ ] Root `pyproject.toml` defines `valkey = ["vertebra-valkey==0.3.0"]` and excludes `_valkey.pyi`.
- [ ] Zero regressions in the fast verification gate.

[^rs-workspace]: `src/rs/Cargo.toml` — Rust workspace Cargo.toml.
[^pg-pyproj]: `src/rs/vertebra-postgres/pyproject.toml` — Maturin standalone wheel isolation precedent.
[^root-pyproject]: `pyproject.toml` — Root pyproject.toml extras and wheel exclusions.
