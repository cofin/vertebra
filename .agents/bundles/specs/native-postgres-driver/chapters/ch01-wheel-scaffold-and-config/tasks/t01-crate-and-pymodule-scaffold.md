---
type: Task
id: "native-postgres-driver:ch01:t01"
chapter_id: ch01-wheel-scaffold-and-config
title: Scaffold vertebra-postgres Cargo manifest, build script, and free-threaded PyO3 module
description: Create src/rs/vertebra-postgres/Cargo.toml with complete locked dependency pins, build.rs with PyO3 cfg/rpath setup, and src/lib.rs exporting gil_used=false vertebra._postgres metadata constants prior to workspace registration.
status: stable
state: completed
priority: P1
verification_strategy: static_validation
depends_on: []
read_set:
  - src/rs/Cargo.toml
  - src/rs/vertebra-spanner/Cargo.toml
  - src/rs/vertebra-spanner/build.rs
  - src/rs/vertebra-spanner/src/lib.rs
  - .agents/skills/pyo3-bridge/SKILL.md
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/rs/vertebra-postgres/Cargo.toml
  - src/rs/vertebra-postgres/build.rs
  - src/rs/vertebra-postgres/src/lib.rs
tests: []
estimated_loc: 120
plan_revision: 3
plan_commit: dd432f18
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T20:05:56Z"
last_verified_commit: 5f25a4a0
verification_evidence: "Declared TOML assertions, rustfmt --edition 2024 --check, cargo check --workspace --locked (baseline and final), git diff --check: exit 0; Cargo workspace manifest and lock unchanged."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T16:40:00Z"
commit: 5f25a4a0
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
  - id: proto-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: Pinned postgres-protocol, tokio-postgres dev oracle, and x509-parser constraints
tags:
  - postgres
  - rust
  - cargo
  - pyo3
---

# Task t01: Scaffold `vertebra-postgres` Cargo Manifest, Build Script, and Free-Threaded PyO3 Module

## Objective & Scope Boundary

Create the complete `src/rs/vertebra-postgres` crate scaffold (`Cargo.toml`, `build.rs`, and `src/lib.rs`) with all protocol, TLS, X.509, `half`, and Arrow dependencies pinned upfront and `#[pymodule(gil_used = false)] fn _postgres` exporting truthful metadata constants before registering the crate in `src/rs/Cargo.toml` [^ws-cargo] [^spanner-cargo] [^spanner-lib] [^proto-research]. Registering `"vertebra-postgres"` in `src/rs/Cargo.toml` and updating `src/rs/Cargo.lock` belong to `native-postgres-driver:ch01:t02` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `src/rs/Cargo.toml` — `[workspace.package]` and `[workspace.dependencies]` keys [^ws-cargo].
- `src/rs/vertebra-spanner/Cargo.toml` — Reference `cdylib` + `rlib` optional extension manifest and `extension-module` feature wiring [^spanner-cargo].
- `src/rs/vertebra-spanner/build.rs` — Reference `pyo3_build_config::use_pyo3_cfgs()` and Unix `-Wl,-rpath` linker setup [^spanner-build].
- `src/rs/vertebra-spanner/src/lib.rs` — `FREE_THREADED`, `GIL_USED`, and `#[pymodule(gil_used = false)]` module registration pattern [^spanner-lib].
- `.agents/skills/pyo3-bridge/SKILL.md` — Free-threaded Python 3.14t/3.15t `gil_used = false` rules.
- `.agents/bundles/specs/native-postgres-driver/contracts.md` — Standalone wheel boundary and truthful export rules ([../../../contracts.md](../../../contracts.md)).

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/Cargo.toml`:
  - `[package]`: `name = "vertebra-postgres"`, `version.workspace = true`, `edition.workspace = true`, `authors.workspace = true`, `license.workspace = true`, `repository.workspace = true`, `description = "Standalone native PostgreSQL Protocol 3.0 client and pool extension for Vertebra"`.
  - `[lib]`: `name = "_postgres"`, `crate-type = ["cdylib", "rlib"]`.
  - `[features]`: `default = []`, `extension-module = ["pyo3/extension-module"]`.
  - `[dependencies]`:
    - `arrow-array = { version = "=60.1.0", default-features = false }`
    - `arrow-buffer = { version = "=60.1.0", default-features = false }`
    - `arrow-ipc = { version = "=60.1.0", default-features = false }`
    - `arrow-schema = { version = "=60.1.0", default-features = false }`
    - `bytes = { workspace = true }`
    - `half = "=2.7.1"`
    - `postgres-protocol = "=0.6.12"`
    - `pyo3 = { workspace = true }`
    - `reqwest = { workspace = true }`
    - `rsa = { version = "0.9", features = ["pem", "sha2"] }`
    - `rustls = { workspace = true }`
    - `rustls-pemfile = { workspace = true }`
    - `serde = { workspace = true }`
    - `serde_json = { workspace = true }`
    - `sha2 = { workspace = true }`
    - `thiserror = { workspace = true }`
    - `tokio = { workspace = true }`
    - `tokio-rustls = { workspace = true }`
    - `url = { workspace = true }`
    - `vertebra-core = { path = "../vertebra-core" }`
    - `vertebra-db = { path = "../vertebra-db", features = ["pyo3"] }`
    - `x509-parser = { version = "=0.18.1", default-features = false }`
  - `[build-dependencies]`:
    - `pyo3-build-config = { workspace = true }`
  - `[dev-dependencies]`:
    - `base64 = { workspace = true }`
    - `hex = { workspace = true }`
    - `hmac = { workspace = true }`
    - `pyo3 = { workspace = true, features = ["auto-initialize"] }`
    - `rcgen = { workspace = true }`
    - `tokio = { workspace = true, features = ["test-util"] }`
    - `tokio-postgres = "=0.7.18"`
- `src/rs/vertebra-postgres/build.rs`:
  - `fn main()` invoking `pyo3_build_config::use_pyo3_cfgs();` and under `#[cfg(unix)]` emitting `println!("cargo:rustc-link-arg=-Wl,-rpath,{lib_dir}")` when `pyo3_build_config::get().lib_dir()` returns `Some(lib_dir)` [^spanner-build].
- `src/rs/vertebra-postgres/src/lib.rs`:
  - `#[cfg(Py_GIL_DISABLED)] pub const FREE_THREADED: bool = true;`
  - `#[cfg(not(Py_GIL_DISABLED))] pub const FREE_THREADED: bool = false;`
  - `pub const GIL_USED: bool = false;`
  - `#[pymodule(gil_used = false)] fn _postgres(m: &Bound<'_, PyModule>) -> PyResult<()>` registering `__version__` (`env!("CARGO_PKG_VERSION")`), `FREE_THREADED`, and `GIL_USED` (zero placeholder client classes) [^spanner-lib].

## Edge Cases & Error Semantics

- Pin `x509-parser = { version = "=0.18.1", default-features = false }` to match the workspace `Cargo.lock` resolution without OpenSSL/native TLS features, enable `"pem"` on `rsa = { version = "0.9", features = ["pem", "sha2"] }` alongside `rustls-pemfile = { workspace = true }`, and declare `half = "=2.7.1"` plus `arrow-{array,buffer,ipc,schema} = { version = "=60.1.0", default-features = false }` upfront so `ch04`, `ch12:t01`, and `ch19:t01` never dirty `src/rs/Cargo.lock`.
- Keep `tokio-postgres = "=0.7.18"` strictly inside `[dev-dependencies]` and `[features] default = []` empty so workspace `cargo test` links `_postgres` cleanly with `pyo3` `auto-initialize` [^proto-research].
- Leaving `src/rs/Cargo.toml` untouched in `t01` guarantees the existing workspace remains `--locked`-green while `src/lib.rs` is already present before `t02` adds `"vertebra-postgres"` to `[workspace].members`.

## Public-Seam Test Blueprint

- **Canonical gate**: Static TOML pin validation, `rustfmt --edition 2024 --check` on `build.rs` and `src/lib.rs`, and existing workspace `--locked` check (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Library target & feature isolation | Parse `src/rs/vertebra-postgres/Cargo.toml` with `tomllib` | `lib == {"name": "_postgres", "crate-type": ["cdylib", "rlib"]}`, `features == {"default": [], "extension-module": ["pyo3/extension-module"]}`. |
| Complete upfront dependency pins | Inspect `dependencies` and `dev-dependencies` tables | `postgres-protocol == "=0.6.12"`, `x509-parser == {"version": "=0.18.1", "default-features": False}`, `rsa["features"] == ["pem", "sha2"]`, `rustls-pemfile == {"workspace": True}`, `half == "=2.7.1"`, all four `arrow-*` crates `{"version": "=60.1.0", "default-features": False}`, `"tokio-postgres" not in dependencies`, `dev-dependencies["tokio-postgres"] == "=0.7.18"`. |
| `build.rs` & `lib.rs` formatting and module exports | `rustfmt --edition 2024 --check` + source inspection of `src/lib.rs` | Exits `0`; `#[pymodule(gil_used = false)] fn _postgres` exports `FREE_THREADED`, `GIL_USED`, and `__version__`. |
| Existing workspace `--locked` health | `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` | Exits `0` with `src/rs/Cargo.lock` completely clean prior to `t02`. |

## Verification

Verify all manifest pins, formatting, and workspace health exit `0`:

```bash
uv run --no-sync python -c '
import pathlib, tomllib
pkg = tomllib.loads(pathlib.Path("src/rs/vertebra-postgres/Cargo.toml").read_text())
deps = pkg["dependencies"]
assert pkg["lib"] == {"name": "_postgres", "crate-type": ["cdylib", "rlib"]}
assert pkg["features"] == {"default": [], "extension-module": ["pyo3/extension-module"]}
assert deps["postgres-protocol"] == "=0.6.12"
assert deps["x509-parser"] == {"version": "=0.18.1", "default-features": False}
assert deps["rsa"] == {"version": "0.9", "features": ["pem", "sha2"]}
assert deps["rustls-pemfile"] == {"workspace": True}
assert deps["half"] == "=2.7.1"
for k in ("arrow-array", "arrow-buffer", "arrow-ipc", "arrow-schema"):
    assert deps[k] == {"version": "=60.1.0", "default-features": False}, k
assert "tokio-postgres" not in deps
assert pkg["dev-dependencies"]["tokio-postgres"] == "=0.7.18"
'
rustfmt --edition 2024 --check src/rs/vertebra-postgres/build.rs src/rs/vertebra-postgres/src/lib.rs
cargo check --manifest-path src/rs/Cargo.toml --workspace --locked
```

Commit as `build(postgres): scaffold vertebra-postgres crate and pymodule`.

## Acceptance Criteria

- [x] `src/rs/vertebra-postgres/Cargo.toml` declares `_postgres` (`cdylib` + `rlib`), `postgres-protocol = "=0.6.12"`, `x509-parser = "=0.18.1"` (`default-features = false`), `rsa` `["pem", "sha2"]`, `rustls-pemfile`, `half = "=2.7.1"`, `arrow-{array,buffer,ipc,schema} = "=60.1.0"`, and dev-only `tokio-postgres = "=0.7.18"` (R1).
- [x] `src/rs/vertebra-postgres/build.rs` and `src/rs/vertebra-postgres/src/lib.rs` (`#[pymodule(gil_used = false)] fn _postgres` exporting `FREE_THREADED`, `GIL_USED`, `__version__`) pass `rustfmt --edition 2024 --check` (R1).
- [x] Existing workspace passes `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` with zero dirty lockfile changes (R1).

## Notes & Discoveries

- [2026-11-07 16:40] Authoring `Cargo.toml`, `build.rs`, and `src/lib.rs` together in `t01` before `t02` adds `"vertebra-postgres"` to `src/rs/Cargo.toml` ensures Cargo never encounters a workspace member missing `src/lib.rs` while keeping `t01` strictly within the 3-file bound.

[^ws-cargo]: `src/rs/Cargo.toml` — Rust workspace shared crate versions.
[^spanner-cargo]: `src/rs/vertebra-spanner/Cargo.toml` — Reference optional PyO3 extension manifest.
[^spanner-build]: `src/rs/vertebra-spanner/build.rs` — Reference PyO3 extension `build.rs` script.
[^spanner-lib]: `src/rs/vertebra-spanner/src/lib.rs::_spanner` — Free-threaded PyO3 module entry point.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Pinned protocol, SCRAM, and X.509 dependency analysis.
