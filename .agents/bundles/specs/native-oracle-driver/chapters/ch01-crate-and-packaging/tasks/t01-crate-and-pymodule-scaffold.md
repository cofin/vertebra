---
type: Task
id: "native-oracle-driver:ch01:t01"
chapter_id: ch01-crate-and-packaging
title: Scaffold vertebra-oracle Cargo manifest, build script, and free-threaded PyO3 module
description: Create src/rs/vertebra-oracle/Cargo.toml with oracledb =26.0.0-beta.5 (arrow feature), crate-local arrow-* =59, and zip =2 pins, build.rs with PyO3 cfg/rpath setup, and src/lib.rs exporting gil_used=false vertebra._oracle metadata constants prior to workspace registration.
status: stable
state: pending
priority: P1
verification_strategy: static_validation
depends_on: []
read_set:
  - src/rs/Cargo.toml
  - src/rs/vertebra-spanner/Cargo.toml
  - src/rs/vertebra-spanner/build.rs
  - src/rs/vertebra-spanner/src/lib.rs
  - .agents/skills/pyo3-bridge/SKILL.md
  - .agents/bundles/specs/native-oracle-driver/contracts.md
files:
  - src/rs/vertebra-oracle/Cargo.toml
  - src/rs/vertebra-oracle/build.rs
  - src/rs/vertebra-oracle/src/lib.rs
tests: []
estimated_loc: 110
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T01:28:30Z"
commit: null
sources:
  - id: ws-cargo
    resource: src/rs/Cargo.toml
    title: Rust workspace shared dependency versions
  - id: spanner-cargo
    resource: src/rs/vertebra-spanner/Cargo.toml
    title: Sibling optional PyO3 extension Cargo manifest
  - id: spanner-build
    resource: src/rs/vertebra-spanner/build.rs
    title: Sibling PyO3 build script with Unix rpath configuration
  - id: spanner-lib
    resource: src/rs/vertebra-spanner/src/lib.rs::_spanner
    title: Reference gil_used=false PyO3 module declaration and FREE_THREADED constant
  - id: crate-research
    resource: .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
    title: Official pure-Rust oracledb =26.0.0-beta.5 and crate-local arrow 59 constraints
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Standalone vertebra._oracle wheel boundary and module exports
tags:
  - oracle
  - rust
  - cargo
  - pyo3
---

# Task t01: Scaffold `vertebra-oracle` Cargo Manifest, Build Script, and Free-Threaded PyO3 Module

## Objective & Scope Boundary

Create the complete `src/rs/vertebra-oracle` crate scaffold (`Cargo.toml`, `build.rs`, and `src/lib.rs`) with `oracledb = { version = "=26.0.0-beta.5", features = ["arrow"] }`, crate-local `arrow-{array,buffer,ipc,schema} = { version = "59", default-features = false }`, `zip = { version = "2", default-features = false, features = ["deflate"] }`, and workspace TLS/PyO3 dependencies pinned upfront, plus `#[pymodule(gil_used = false)] fn _oracle` exporting truthful metadata constants before registering the crate in `src/rs/Cargo.toml` [^ws-cargo] [^spanner-cargo] [^spanner-lib] [^crate-research] [^ora-contracts]. Registering `"vertebra-oracle"` in `src/rs/Cargo.toml`, locking `src/rs/Cargo.lock`, and authoring `pyproject.toml` + `_oracle.pyi` belong to `native-oracle-driver:ch01:t02` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/Cargo.toml` — `[workspace.package]` and `[workspace.dependencies]` keys [^ws-cargo].
- `src/rs/vertebra-spanner/Cargo.toml` — Reference `cdylib` + `rlib` optional extension manifest and `extension-module` feature wiring [^spanner-cargo].
- `src/rs/vertebra-spanner/build.rs` — Reference `pyo3_build_config::use_pyo3_cfgs()` and Unix `-Wl,-rpath` linker setup [^spanner-build].
- `src/rs/vertebra-spanner/src/lib.rs` — `FREE_THREADED`, `GIL_USED`, and `#[pymodule(gil_used = false)]` module registration pattern [^spanner-lib].
- `.agents/skills/pyo3-bridge/SKILL.md` — Free-threaded Python 3.14t/3.15t `gil_used = false` rules.
- `.agents/bundles/specs/native-oracle-driver/contracts.md` — Standalone wheel boundary and truthful export rules ([../../../contracts.md](../../../contracts.md)) [^ora-contracts].

## Symbol & Signature Contracts

- `src/rs/vertebra-oracle/Cargo.toml`:
  - `[package]`: `name = "vertebra-oracle"`, `version.workspace = true`, `edition.workspace = true`, `authors.workspace = true`, `license.workspace = true`, `repository.workspace = true`, `description = "Standalone native Oracle Thin 26ai client and pool extension for Vertebra"`.
  - `[lib]`: `name = "_oracle"`, `crate-type = ["cdylib", "rlib"]`.
  - `[features]`: `default = []`, `extension-module = ["pyo3/extension-module"]`.
  - `[dependencies]`:
    - `arrow-array = { version = "59", default-features = false }`
    - `arrow-buffer = { version = "59", default-features = false }`
    - `arrow-ipc = { version = "59", default-features = false }`
    - `arrow-schema = { version = "59", default-features = false }`
    - `oracledb = { version = "=26.0.0-beta.5", features = ["arrow"] }`
    - `pyo3 = { workspace = true }`
    - `rustls = { workspace = true }`
    - `rustls-pemfile = { workspace = true }`
    - `thiserror = { workspace = true }`
    - `tokio = { workspace = true }`
    - `url = { workspace = true }`
    - `vertebra-core = { path = "../vertebra-core" }`
    - `vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }`
    - `zip = { version = "2", default-features = false, features = ["deflate"] }`
  - `[build-dependencies]`:
    - `pyo3-build-config = { workspace = true }`
  - `[dev-dependencies]`:
    - `pyo3 = { workspace = true, features = ["auto-initialize"] }`
    - `tokio = { workspace = true, features = ["test-util"] }`
- `src/rs/vertebra-oracle/build.rs`:
  - `fn main()` invoking `pyo3_build_config::use_pyo3_cfgs();` and under `#[cfg(unix)]` emitting `println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}")` when `pyo3_build_config::get().lib_dir()` returns `Some(lib_dir)` [^spanner-build].
- `src/rs/vertebra-oracle/src/lib.rs`:
  - `#[cfg(Py_GIL_DISABLED)] pub const FREE_THREADED: bool = true;`
  - `#[cfg(not(Py_GIL_DISABLED))] pub const FREE_THREADED: bool = false;`
  - `pub const GIL_USED: bool = false;`
  - `#[pymodule(gil_used = false)] fn _oracle(m: &Bound<'_, PyModule>) -> PyResult<()>` registering `__version__` (`env!("CARGO_PKG_VERSION")`), `FREE_THREADED`, and `GIL_USED` (zero placeholder client classes) [^spanner-lib] [^ora-contracts].

## Edge Cases & Error Semantics

- Keep `arrow-array`, `arrow-buffer`, `arrow-ipc`, and `arrow-schema` (`version = "59"`, `default-features = false`) strictly crate-local in `src/rs/vertebra-oracle/Cargo.toml` so `arrow 59` never leaks into `vertebra-core` or `vertebra-db` [^crate-research].
- Declare `zip = { version = "2", default-features = false, features = ["deflate"] }` and `oracledb = { version = "=26.0.0-beta.5", features = ["arrow"] }` upfront so `ch02` (wallet `.zip` extraction) and `ch03` (`query_arrow`) never modify `Cargo.toml` or dirty `src/rs/Cargo.lock`.
- Leaving `src/rs/Cargo.toml` untouched in `t01` keeps the existing workspace `--locked`-green while `src/lib.rs` is already on disk before `t02` adds `"vertebra-oracle"` to `[workspace].members`.

## Public-Seam Test Blueprint

- **Canonical gate**: Static TOML pin validation, `rustfmt --edition 2024 --check` on `build.rs` and `src/lib.rs`, and existing workspace `--locked` check (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| :--- | :--- | :--- |
| Library target & feature isolation | Parse `src/rs/vertebra-oracle/Cargo.toml` with `tomllib` | `lib == {"name": "_oracle", "crate-type": ["cdylib", "rlib"]}`, `features == {"default": [], "extension-module": ["pyo3/extension-module"]}`. |
| Upfront `oracledb`, `arrow 59`, and `zip` pins | Inspect `dependencies` table | `oracledb == {"version": "=26.0.0-beta.5", "features": ["arrow"]}`, `zip == {"version": "2", "default-features": False, "features": ["deflate"]}`, and all four `arrow-*` crates equal `{"version": "59", "default-features": False}`. |
| `build.rs` & `lib.rs` formatting and module exports | `rustfmt --edition 2024 --check` + source inspection of `src/lib.rs` | Exits `0`; `#[pymodule(gil_used = false)] fn _oracle` exports `FREE_THREADED`, `GIL_USED`, and `__version__`. |
| Existing workspace `--locked` health | `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` | Exits `0` with `src/rs/Cargo.lock` completely clean prior to `t02`. |

## Verification

Verify all manifest pins, formatting, and workspace health exit `0`:

```bash
uv run --no-sync python -c '
import pathlib, tomllib
pkg = tomllib.loads(pathlib.Path("src/rs/vertebra-oracle/Cargo.toml").read_text())
deps = pkg["dependencies"]
assert pkg["lib"] == {"name": "_oracle", "crate-type": ["cdylib", "rlib"]}
assert pkg["features"] == {"default": [], "extension-module": ["pyo3/extension-module"]}
assert deps["oracledb"] == {"version": "=26.0.0-beta.5", "features": ["arrow"]}
assert deps["zip"] == {"version": "2", "default-features": False, "features": ["deflate"]}
for k in ("arrow-array", "arrow-buffer", "arrow-ipc", "arrow-schema"):
    assert deps[k] == {"version": "59", "default-features": False}, k
assert deps["vertebra-db"] == {"path": "../vertebra-db", "features": ["pyo3"]}
'
rustfmt --edition 2024 --check src/rs/vertebra-oracle/build.rs src/rs/vertebra-oracle/src/lib.rs
cargo check --manifest-path src/rs/Cargo.toml --workspace --locked
```

Commit as `build(oracle): scaffold vertebra-oracle crate and pymodule`.

## Acceptance Criteria

- [ ] `src/rs/vertebra-oracle/Cargo.toml` declares `_oracle` (`cdylib` + `rlib`), `oracledb = { version = "=26.0.0-beta.5", features = ["arrow"] }`, crate-local `arrow-{array,buffer,ipc,schema} = { version = "59", default-features = false }`, `zip = { version = "2", default-features = false, features = ["deflate"] }`, and `vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }` (R1).
- [ ] `src/rs/vertebra-oracle/build.rs` and `src/rs/vertebra-oracle/src/lib.rs` (`#[pymodule(gil_used = false)] fn _oracle` exporting `FREE_THREADED`, `GIL_USED`, `__version__`) pass `rustfmt --edition 2024 --check` (R1).
- [ ] Existing workspace passes `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` with zero dirty lockfile changes (R1).

## Notes & Discoveries

- [2026-11-09 01:15] Authoring `Cargo.toml`, `build.rs`, and `src/lib.rs` together in `t01` before `t02` adds `"vertebra-oracle"` to `src/rs/Cargo.toml` ensures Cargo never encounters a workspace member missing `src/lib.rs` while keeping `t01` strictly within the 3-file bound.

[^ws-cargo]: `src/rs/Cargo.toml` — Rust workspace shared dependency versions.
[^spanner-cargo]: `src/rs/vertebra-spanner/Cargo.toml` — Sibling optional PyO3 extension Cargo manifest.
[^spanner-build]: `src/rs/vertebra-spanner/build.rs` — Sibling PyO3 build script with Unix rpath configuration.
[^spanner-lib]: `src/rs/vertebra-spanner/src/lib.rs::_spanner` — Reference `gil_used=false` PyO3 module declaration and `FREE_THREADED` constant.
[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Official pure-Rust `oracledb =26.0.0-beta.5` and crate-local `arrow 59` constraints.
[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Standalone `vertebra._oracle` wheel boundary and module exports.
