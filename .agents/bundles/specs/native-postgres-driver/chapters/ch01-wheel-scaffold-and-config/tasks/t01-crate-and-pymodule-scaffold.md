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
plan_revision: 7
plan_commit: 1c75af65
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-07T22:40:44Z"
last_verified_commit: d809be01
verification_evidence:
  - "Direct fallible-iterator =0.2.0 manifest and lock-edge assertions: exit 0; all existing package/version/source/checksum identities unchanged."
  - "UV_NO_SYNC=1; PYO3_PYTHON=${PWD}/.venv/bin/python; CARGO_TARGET_DIR=/tmp/vtb-cargo-target."
  - "Workspace cargo check --locked: exit 0; workspace all-target Clippy --features tls,compression,gcp --locked -- -D warnings: exit 0."
  - "flock /tmp/vertebra-flow-shm-tests.lock cargo test -p vertebra-postgres --locked: exit 0; 9 codec and 7 configuration tests."
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T22:40:44Z"
commit: d809be01
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

The native crate, build script, metadata exports, and workspace registration satisfy the scaffold contracts below. The task-01 manifest declares `fallible-iterator = "=0.2.0"` directly in the existing crate manifest, paired atomically with task 02's existing-version lock edge. No library/build-script changes or new Python exports belong to this prerequisite repair. All complete dependency, feature-isolation, and free-threaded scaffold contracts remain required.

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
    - `fallible-iterator = "=0.2.0"`
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

- Direct diagnostic iteration uses the same `fallible-iterator 0.2.0` trait implemented by `postgres-protocol 0.6.12`; a different trait version is incompatible.
- Manifest and lock changes form one coherent source delta; neither owner commits an intermediate unresolved lock state.
- Existing package/version/source/checksum identities and all workspace membership, dependency pins, metadata exports, and module visibility stay intact.

## Public-Seam Test Blueprint

- **Canonical gate**: Static TOML pin validation, `rustfmt --edition 2024 --check` on `build.rs` and `src/lib.rs`, and existing workspace `--locked` check (`tests: []`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Library target & feature isolation | Parse `src/rs/vertebra-postgres/Cargo.toml` with `tomllib` | `lib == {"name": "_postgres", "crate-type": ["cdylib", "rlib"]}`, `features == {"default": [], "extension-module": ["pyo3/extension-module"]}`. |
| Complete upfront dependency pins | Inspect `dependencies` and `dev-dependencies` tables | `postgres-protocol == "=0.6.12"`, `x509-parser == {"version": "=0.18.1", "default-features": False}`, `rsa["features"] == ["pem", "sha2"]`, `rustls-pemfile == {"workspace": True}`, `half == "=2.7.1"`, all four `arrow-*` crates `{"version": "=60.1.0", "default-features": False}`, `"tokio-postgres" not in dependencies`, `dev-dependencies["tokio-postgres"] == "=0.7.18"`. |
| `build.rs` & `lib.rs` formatting and module exports | `rustfmt --edition 2024 --check` + source inspection of `src/lib.rs` | Exits `0`; `#[pymodule(gil_used = false)] fn _postgres` exports `FREE_THREADED`, `GIL_USED`, and `__version__`. |
| Coupled prerequisite `--locked` health | `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` | Exits `0` after the coupled delta; final validation makes no further lock edits. |

## Verification

Baseline and final native gates select the project interpreter with `PYO3_PYTHON=${PWD}/.venv/bin/python`, `UV_NO_SYNC=1`, and the PostgreSQL Cargo target directory. The current change is the coupled direct-dependency/lock-edge delta; scaffold files and workspace membership retain their verified contracts.

```bash
uv run --no-sync python - <<'PY'
from pathlib import Path
import subprocess
import tomllib
manifest = tomllib.loads(Path("src/rs/vertebra-postgres/Cargo.toml").read_text())
assert manifest["dependencies"]["fallible-iterator"] == "=0.2.0"
previous = tomllib.loads(subprocess.check_output(["git", "show", "8833b064:src/rs/Cargo.lock"], text=True))
current = tomllib.loads(Path("src/rs/Cargo.lock").read_text())
def identities(data):
    return {(p["name"], p["version"], p.get("source"), p.get("checksum")) for p in data["package"]}
assert identities(previous) == identities(current)
package = next(p for p in current["package"] if p["name"] == "vertebra-postgres")
assert any(dep == "fallible-iterator" or dep == "fallible-iterator 0.2.0" for dep in package["dependencies"])
PY
```

```bash
cargo check --manifest-path src/rs/Cargo.toml --workspace --locked
cargo clippy --manifest-path src/rs/Cargo.toml --workspace --all-targets --features tls,compression,gcp --locked -- -D warnings
flock /tmp/vertebra-flow-shm-tests.lock cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked
```

Resolve the two-file delta with scoped/offline Cargo metadata before running the final locked gates. Compare `(name, version, source, checksum)` package identities against `git show 8833b064:src/rs/Cargo.lock`; all identities stay equal and the PostgreSQL package gains its direct `fallible-iterator` edge at existing version `0.2.0`.

Tasks 01 and 02 share signed source `d809be01` (`fix(postgres): declare diagnostic iterator prerequisite`), covering only the crate manifest and Cargo lockfile. Both worksheets bind their respective ownership to that source SHA and its passing gates. Chapter 01 is complete and reviewed at `3d68c853`; the downstream session uses the direct iterator dependency.

## Acceptance Criteria

- [x] The direct `fallible-iterator = "=0.2.0"` prerequisite is present and its existing-version lock edge passes the static native delta gates (R1).

- [x] `src/rs/vertebra-postgres/Cargo.toml` declares `_postgres` (`cdylib` + `rlib`), `postgres-protocol = "=0.6.12"`, `x509-parser = "=0.18.1"` (`default-features = false`), `rsa` `["pem", "sha2"]`, `rustls-pemfile`, `half = "=2.7.1"`, `arrow-{array,buffer,ipc,schema} = "=60.1.0"`, and dev-only `tokio-postgres = "=0.7.18"` (R1).
- [x] `src/rs/vertebra-postgres/build.rs` and `src/rs/vertebra-postgres/src/lib.rs` (`#[pymodule(gil_used = false)] fn _postgres` exporting `FREE_THREADED`, `GIL_USED`, `__version__`) pass `rustfmt --edition 2024 --check` (R1).
- [x] Existing workspace passes `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` with zero dirty lockfile changes (R1).

## Notes & Discoveries

- `fallible-iterator = "=0.2.0"` is a direct dependency because the pinned protocol crate exposes diagnostic fields through its non-reexported trait. This static prerequisite delta shares a signed commit with the task 02 lockfile edge; both worksheets keep their existing file ownership.

- The manifest, build script, and library target form a valid crate before workspace registration.

[^ws-cargo]: `src/rs/Cargo.toml` — Rust workspace shared crate versions.
[^spanner-cargo]: `src/rs/vertebra-spanner/Cargo.toml` — Reference optional PyO3 extension manifest.
[^spanner-build]: `src/rs/vertebra-spanner/build.rs` — Reference PyO3 extension `build.rs` script.
[^spanner-lib]: `src/rs/vertebra-spanner/src/lib.rs::_spanner` — Free-threaded PyO3 module entry point.
[^proto-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — Pinned protocol, SCRAM, and X.509 dependency analysis.
