---
type: Task
id: native-sqlite-driver:ch01-wheel-scaffold-and-isolation:t01
flow_id: native-sqlite-driver
chapter_id: ch01-wheel-scaffold-and-isolation
title: Scaffold vertebra-sqlite Crate and Wire Cargo Workspace + [sqlite] Extra
description: Create src/rs/vertebra-sqlite with bundled SQLCipher and gil_used = false, and wire Cargo workspace and root pyproject.toml [sqlite], uv.sources, maturin.exclude, and bumpversion.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: implementation_first
plan_commit: 104fd6fa
depends_on: []
files:
- src/rs/vertebra-sqlite/Cargo.toml
- src/rs/vertebra-sqlite/pyproject.toml
- src/rs/vertebra-sqlite/src/lib.rs
- src/rs/Cargo.toml
- src/rs/Cargo.lock
- pyproject.toml
tests: []
read_set:
- src/rs/vertebra-duckdb/Cargo.toml
- src/rs/vertebra-duckdb/pyproject.toml
- src/rs/vertebra-duckdb/src/lib.rs
- src/rs/Cargo.toml
- pyproject.toml
estimated_loc: 220
tags:
- sqlite
- rust
- pyo3
- packaging
- cargo
sources:
- id: duckdb-cargo
  resource: src/rs/vertebra-duckdb/Cargo.toml
  title: Standalone embedded PyO3 wheel Cargo precedent
- id: duckdb-pyproj
  resource: src/rs/vertebra-duckdb/pyproject.toml
  title: Maturin standalone wheel isolation manifest precedent
- id: duckdb-lib
  resource: src/rs/vertebra-duckdb/src/lib.rs
  title: Embedded worker-thread and VTBARROW PyO3 extension precedent
- id: cargo-ws
  resource: src/rs/Cargo.toml
  title: Rust workspace member manifest
- id: root-pyproject
  resource: pyproject.toml
  title: Root vertebra-core extras, uv sources, maturin excludes, and bumpversion
---

# Task `t01`: Scaffold vertebra-sqlite Crate and Wire Cargo Workspace + [sqlite] Extra

## Objective & Scope Boundary

Create `src/rs/vertebra-sqlite/{Cargo.toml,pyproject.toml,src/lib.rs}` exporting `__version__`, `FREE_THREADED`, and `GIL_USED = False` per [`../spec.md` §2](../spec.md) [^duckdb-cargo] [^duckdb-pyproj] [^duckdb-lib].

Wire `vertebra-sqlite` into `src/rs/Cargo.toml`, `src/rs/Cargo.lock`, and `pyproject.toml` per [`../spec.md` §2](../spec.md) [^cargo-ws] [^root-pyproject].

## Bounded Read Manifest

- `src/rs/vertebra-duckdb/Cargo.toml` — Arrow 60.1.0 and workspace dependency conventions.
- `src/rs/vertebra-duckdb/pyproject.toml` — Maturin `module-name`, `include`, and `exclude` globs.
- `src/rs/vertebra-duckdb/src/lib.rs` — `#[pymodule(gil_used = false)]` pattern.

- `src/rs/Cargo.toml` — Workspace `members` list.
- `pyproject.toml` — `optional-dependencies`, `uv.sources`, `maturin.exclude`, and `bumpversion`.

## Contracts & Edge Cases

- Enable `rusqlite` features `bundled`, `modern_sqlite`, `hooks`, `serialize`, `blob`, `column_decltype`, and `backup`.
- Include `vertebra/_sqlite.pyi` in `vertebra-sqlite/pyproject.toml` and exclude `vertebra/**/*.py` plus all sibling `_*.pyi` stubs.
- **Edge Case**: Module initializes without acquiring the GIL on free-threaded Python `3.14t`/`3.15t` (`FFI-01`).

- Add `"vertebra-sqlite"` to `src/rs/Cargo.toml` workspace members and `[workspace.dependencies]`, and generate lockfile entries.
- Add `sqlite = ["vertebra-sqlite==0.3.0"]` in `[project.optional-dependencies]` (never add `[project.optional-dependencies].all`, which is forbidden by `test_optional_driver_wheels.py`), add `vertebra-sqlite = { path = "src/rs/vertebra-sqlite", editable = true }` in `[tool.uv.sources]`, exclude `src/py/vertebra/_sqlite.pyi` from the root wheel, and add `bumpversion` entries.
- **Edge Case**: Keep `bench` dependencies untouched so `3.14t` test environments stay lightweight.

## Test Matrix

- Unit test in `src/lib.rs` asserts bundled SQLite version `>= 3.45.0` (`rusqlite::version_number() >= 3_045_000`) and `GIL_USED == false`.

- `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite` compiles and passes.

## Verification

```bash
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-sqlite
```

## Acceptance Criteria

- [ ] `src/rs/vertebra-sqlite` defines `cdylib` + `rlib` with `#[pymodule(gil_used = false)]`.
- [ ] Workspace Cargo build and root `pyproject.toml` extras include `vertebra-sqlite` cleanly.
- [ ] Zero regressions in the fast verification gate.

[^duckdb-cargo]: `src/rs/vertebra-duckdb/Cargo.toml` — Standalone embedded PyO3 wheel Cargo precedent.
[^duckdb-pyproj]: `src/rs/vertebra-duckdb/pyproject.toml` — Maturin standalone wheel isolation manifest precedent.
[^duckdb-lib]: `src/rs/vertebra-duckdb/src/lib.rs` — Embedded worker-thread and VTBARROW PyO3 extension precedent.
[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
