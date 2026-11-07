---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch01-wheel-scaffold-and-config
title: "Chapter 01: Standalone Optional Wheel Scaffold and Native Connection Configuration"
description: Scaffold the standalone vertebra-postgres PyO3 crate, lock workspace and vertebra-db Cargo dependencies upfront, wire the vertebra._postgres wheel/stub/Makefile matrix, and implement pure PostgresConfig validation and ServerError categories.
status: stable
state: active
planning_status: complete
depends_on_chapters: []
replan_trigger: null
current_task: "native-postgres-driver:ch01:t04"
plan_revision: 4
plan_commit: 45ff9563
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T16:40:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:40:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:40:00Z"
    commit: dd432f18
sources:
  - id: ws-cargo
    resource: src/rs/Cargo.toml
    title: Rust workspace members and shared dependency pins
  - id: spanner-wheel
    resource: src/rs/vertebra-spanner/pyproject.toml
    title: Reference optional PyO3 Maturin wheel layout in vertebra namespace
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Public connection constructor options, defaults, and ownership contracts
  - id: rs-tls-mode
    resource: src/rs/vertebra-db/src/connection.rs::TlsMode
    title: Shared transport TLS modes and error SQLSTATE classifications
tags:
  - postgres
  - rust
  - pyo3
  - wheel
  - config
---

# Chapter 01: Standalone Optional Wheel Scaffold and Native Connection Configuration

<!-- truth: start -->
- **Green-Every-Commit Scaffold & Locked Dependencies**: `ch01:t01` creates the complete `src/rs/vertebra-postgres` crate (`Cargo.toml`, `build.rs`, `src/lib.rs`) with upfront pins (`postgres-protocol = "=0.6.12"`, `x509-parser = "=0.18.1"`, `rsa` `["pem", "sha2"]`, `rustls-pemfile`, `half = "=2.7.1"`, `arrow-* = "=60.1.0"`) before `ch01:t02` registers `"vertebra-postgres"` in `src/rs/Cargo.toml`, adds `socket2 = "=0.6.5"` + `half = "=2.7.1"` to `vertebra-db`, and locks `src/rs/Cargo.lock` [^ws-cargo].
- **Truthful Free-Threaded Wheel & Stub Matrix**: `#[pymodule(gil_used = false)] fn _postgres` exports only `__version__`, `FREE_THREADED`, and `GIL_USED = false`, packaged via `src/rs/vertebra-postgres/pyproject.toml`, `src/py/vertebra/_postgres.pyi`, and `Makefile` (`dev`, `build`, `py-test`, `lint`, `lint-all`, `postgres-test`, `postgres-wheel-check`) [^spanner-wheel] [^pg-contracts].
- **Pure Owned `PostgresConfig` & `ServerError`**: `PostgresConfig` validates scalar, port (`1..=65535`), timeout, NUL-byte, PEM-pair, and five-spelling `TlsMode` invariants without I/O, while `PostgresError::Server(ServerError)` preserves `severity`, `sqlstate`, `message`, and `(u8, String)` `diagnostics` upfront with secret redaction [^pg-contracts] [^rs-tls-mode].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

The native PostgreSQL driver ships as an optional standalone wheel (`vertebra-postgres`) sharing the `vertebra` Python namespace without linking PostgreSQL clients into `vertebra-core` [^ws-cargo] [^spanner-wheel]. Chapter 01 scaffolds `src/rs/vertebra-postgres` (`Cargo.toml`, `build.rs`, `src/lib.rs`) with all downstream protocol, TLS, X.509, `half`, and Arrow dependencies declared upfront, locks workspace membership plus `vertebra-db` `socket2`/`half` dependencies in `src/rs/Cargo.lock` so every commit builds cleanly with `--locked`, wires the Maturin wheel, `_postgres.pyi` stub, and `Makefile` targets, and implements pure `PostgresConfig` validation plus `ServerError` / `PostgresError` categories [^pg-contracts] [^rs-tls-mode].

### 1.2 Non-Goals

- Root `pyproject.toml` `[postgres]` optional extra, `uv.lock`, `.agents/skills/review-core/references/areas.toml` registration, sibling optional `pyproject.toml` `_postgres.pyi` exclusions, and `.github/workflows/release.yaml` wheel CI lanes (owned by `ch08:t02`–`t04` and `ch29:t01`).
- Live TCP/Unix socket dialing, `SSLRequest`, startup framing, or SASL/IAM handshakes (owned by Chapters 02–06).
- Python `SyncPostgresClient` and `AsyncPostgresClient` PyO3 class registration (owned by Chapter 07).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md). Public interface contracts: [contracts.md](../../contracts.md). Skills: [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md) and [runtime-ipc](../../../../../skills/runtime-ipc/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), and [ffi-and-gil-discipline.md](../../../../knowledge/patterns/rust/ffi-and-gil-discipline.md).

| Existing Seam | Verified Consequence |
| --- | --- |
| `src/rs/Cargo.toml` & `src/rs/Cargo.lock` [^ws-cargo] | Locking `x509-parser = "=0.18.1"`, `rsa` `["pem", "sha2"]`, `rustls-pemfile`, `half = "=2.7.1"`, `arrow-* = "=60.1.0"`, and `vertebra-db` `socket2 = "=0.6.5"` + `half = "=2.7.1"` in `ch01:t01`–`t02` prevents downstream tasks (`ch03`–`ch20`) from dirtying `Cargo.lock`. |
| `src/rs/vertebra-spanner/pyproject.toml` [^spanner-wheel] | Maturin `python-source = "../../py"` with explicit `exclude` globs packages only `vertebra/_postgres.<abi>.*` and `vertebra/_postgres.pyi` inside `.whl` while retaining Cargo workspace sources inside `.tar.gz` sdists. |
| `src/rs/vertebra-db/src/connection.rs::TlsMode` [^rs-tls-mode] | Shared `TlsMode` (`Disable`, `Prefer`, `Require`, `VerifyCa`, `VerifyFull`) is reused; native PostgreSQL config enforces the exact five canonical spellings (`disable`, `prefer`, `require`, `verify-ca`, `verify-full`). |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R1 (`ch01:t01`, `ch01:t02`, `ch01:t03`)**: Author `vertebra-postgres` (`lib.name = "_postgres"`, `cdylib` + `rlib`, `gil_used = false`), register workspace membership and lock `src/rs/Cargo.lock` alongside `vertebra-db`'s `socket2` and `half` pins, and wire `src/rs/vertebra-postgres/pyproject.toml`, `src/py/vertebra/_postgres.pyi`, and `Makefile` (`dev`, `build`, `py-test`, `lint`, `lint-all`, `postgres-test`, `postgres-wheel-check`).
- **R2 & R3 (`ch01:t04`)**: Define the complete owned `PostgresConfig` struct matching every constructor field and default in `contracts.md`, validate scalar/port/timeout/NUL/PEM/five-spelling `TlsMode` rules purely in memory, and define `ServerError` and `PostgresError` with SQLSTATE retention, diagnostic preservation, and secret redaction.

### 2.2 Concrete Type & Signature Contracts

- Rust (`src/rs/vertebra-postgres/src/lib.rs`):
  - `pub const FREE_THREADED: bool` (`true` under `#[cfg(Py_GIL_DISABLED)]`, `false` otherwise)
  - `pub const GIL_USED: bool = false;`
  - `#[pymodule(gil_used = false)] fn _postgres(m: &Bound<'_, PyModule>) -> PyResult<()>` (exports `__version__`, `FREE_THREADED`, `GIL_USED`)
- Rust (`src/rs/vertebra-postgres/src/config.rs`):
  - `#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)] pub enum ChannelBindingPolicy { Disable, #[default] Prefer, Require }`
  - `#[derive(Clone, PartialEq, Eq)] pub struct PostgresConfig` owning `user: String`, `database: String`, `host: String` (`"localhost"`), `port: u16` (`5432`), `password: Option<String>`, `unix_socket: Option<String>`, `sslmode: TlsMode` (`TlsMode::VerifyFull`), `ca_pem: Option<Vec<u8>>`, `client_cert_pem: Option<Vec<u8>>`, `client_key_pem: Option<Vec<u8>>`, `server_name: Option<String>`, `channel_binding: ChannelBindingPolicy` (`Prefer`), `connect_timeout: Duration` (`10s`), `application_name: String` (`"vertebra"`), `session_gucs: HashMap<String, String>`, `keepalives: bool` (`true`), `keepalives_idle: Option<i64>`, `keepalives_interval: Option<i64>`, `keepalives_count: Option<i64>`, `cloud_sql_instance: Option<String>`, `cloud_sql_ip_type: String` (`"PRIVATE"`), `alloydb_instance_uri: Option<String>`, `alloydb_ip_type: String` (`"PRIVATE"`), `enable_iam_auth: bool` (`false`), `cloud_auth_options: HashMap<String, String>`, `iam_auth_options: HashMap<String, String>`
  - `pub struct RawPostgresOptions` and `PostgresConfig::validate(raw: RawPostgresOptions) -> Result<Self, PostgresError>`
  - Custom `fmt::Debug for PostgresConfig` redacting `password`, `client_key_pem`, `cloud_auth_options`, and `iam_auth_options`
- Rust (`src/rs/vertebra-postgres/src/errors.rs`):
  - `#[derive(Debug, Clone, PartialEq, Eq)] pub struct ServerError { pub severity: String, pub sqlstate: String, pub message: String, pub diagnostics: Vec<(u8, String)> }`
  - `#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)] pub enum PostgresError { Config(String), Interface(String), Data(String), UnsupportedAuth(String), UnsupportedFeature(String), Transport(String), Protocol(String), Server(ServerError) }`
  - `PostgresError::sqlstate(&self) -> Option<&str>` and `PostgresError::server(severity: impl Into<String>, sqlstate: &str, message: impl Into<String>, diagnostics: Vec<(u8, String)>) -> Result<Self, Self>`
- Python stub (`src/py/vertebra/_postgres.pyi`):
  - `__version__: str`, `FREE_THREADED: bool`, `GIL_USED: bool`, `__all__ = ["FREE_THREADED", "GIL_USED", "__version__"]`

### 2.3 Caller -> Seam -> Collaborator Integration Map

```text
Makefile (dev / postgres-test / postgres-wheel-check)
  -> maturin (src/rs/vertebra-postgres/pyproject.toml)
  -> vertebra._postgres (src/rs/vertebra-postgres/src/lib.rs)
  -> _postgres::config::PostgresConfig::validate -> vertebra_db::connection::TlsMode
  -> _postgres::errors::{PostgresError, ServerError}
```

### 2.4 Edge-Case & Alternatives Summary

| Edge Case / Failure Mode | Required Behavior |
| --- | --- |
| Crate created before workspace membership (`ch01:t01` vs `ch01:t02`) | `ch01:t01` creates `Cargo.toml`, `build.rs`, and `src/lib.rs` together so `ch01:t02` adds `"vertebra-postgres"` to `src/rs/Cargo.toml` and locks `src/rs/Cargo.lock` in one green `--locked` commit. |
| Missing, empty, or NUL-containing `user`, `database`, `host`, `server_name`, `password`, or `application_name` | Reject with `PostgresError::Config` without performing DNS or socket I/O, never echoing secret values. |
| Signed `port` outside `1..=65535`, non-finite / `<= 0.0` `connect_timeout`, or mismatched mTLS PEM pair | Reject before `u16` / `Duration` conversion with `PostgresError::Config` and redacted key bytes. |
| Non-canonical `sslmode` (`"true"`, `"allow"`, `"PREFER"`) or URI/slash TCP `host` | Reject with `PostgresError::Config`; accept only exact `disable`, `prefer`, `require`, `verify-ca`, `verify-full`. |
| Server `ErrorResponse` carrying `D`/`H`/`W` diagnostic fields | `ServerError` retains `severity`, 5-byte ASCII `sqlstate`, `message`, and `diagnostics: Vec<(u8, String)>` for structured query consumers while `Display` formats only sanitized primary `message` + `sqlstate`. |

- **Alternatives Considered**: Deferring `ServerError` (`diagnostics`) or `arrow-*`/`half`/`socket2` pins to Chapters 06–19 was rejected because adding fields to `PostgresError::Server` in `ch09:t02` would break `ch01`–`ch08` callers, and adding crates later would dirty `src/rs/Cargo.lock` outside `files:` bounds.

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch01]` (`src/py/vertebra/_postgres.pyi` module docstring and stub exports land in `ch01:t03`; public user documentation in `docs/usage/databases.md`, `docs/reference/database-capabilities.md`, `README.md`, and OKF chapters in `.agents/bundles/knowledge/domains/data/` are consolidated in `ch30:t01`–`ch30:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test / Gate |
| --- | --- | --- | --- |
| R1 (crate manifest, `build.rs`, `gil_used=false` `lib.rs`) | [t01](tasks/t01-crate-and-pymodule-scaffold.md) | `static_validation` | `tomllib` pin check + `rustfmt --edition 2024 --check` + `cargo check --workspace --locked` |
| R1 (workspace registration & locked `Cargo.lock`) | [t02](tasks/t02-workspace-and-db-cargo-lock.md) | `static_validation` | `cargo check --manifest-path src/rs/Cargo.toml --workspace --locked` + `cargo clippy --locked` |
| R1 (wheel, sdist, `_postgres.pyi`, `Makefile` matrix) | [t03](tasks/t03-wheel-stub-and-makefile-matrix.md) | `static_validation` | `make dev`, `make postgres-wheel-check`, `make postgres-test`, `mypy.stubtest vertebra._postgres` |
| R2, R3 (`PostgresConfig`, `ServerError`, `PostgresError`) | [t04](tasks/t04-configuration-and-error-categories.md) | `behavior_tdd` | `src/rs/vertebra-postgres/tests/postgres_wire_tests.rs` |

## 5. Implementation Plan

- [x] [5f25a4a0] native-postgres-driver:ch01:t01 — Scaffold vertebra-postgres Cargo manifest, build script, and free-threaded PyO3 module (tasks/t01-crate-and-pymodule-scaffold.md)
- [x] [9e128425] native-postgres-driver:ch01:t02 — Register workspace member, add vertebra-db socket2/half pins, and lock Cargo.lock (tasks/t02-workspace-and-db-cargo-lock.md)
- [x] [05fff946] native-postgres-driver:ch01:t03 — Wire vertebra-postgres wheel, _postgres.pyi stub, and Makefile build/test targets (tasks/t03-wheel-stub-and-makefile-matrix.md)
- [~] native-postgres-driver:ch01:t04 — Implement pure PostgresConfig validation and sanitized PostgresError categories (tasks/t04-configuration-and-error-categories.md)

**Chapter Recovery Checkpoint:** `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --locked && cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets --locked -- -D warnings && make dev && make postgres-wheel-check && make postgres-test && uv run --no-sync python -m mypy.stubtest vertebra._postgres` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Baseline**: `HEAD @ 05fff946`; chapter `state: active`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t04` (`1/4` in progress; `t01` verified at `5f25a4a0`, `t02` at `9e128425`, `t03` at `05fff946`).
- **Documentation & OKF Status**: `[0/0 files documented in ch01]`; `_postgres.pyi` and strict wheel/sdist checks verified in `t03`, root `pyproject.toml`/`uv.lock`/`areas.toml` wired in `ch08:t02`–`t04`, and full docs/OKF scheduled in `ch30`.
- **Downstream Dependents**: Unblocks `ch02-wire-codec-and-session-startup` (`ch02:t01` depends on `ch01:t04`).

[^ws-cargo]: `src/rs/Cargo.toml` — Rust workspace members and shared crate dependency versions.
[^spanner-wheel]: `src/rs/vertebra-spanner/pyproject.toml` — Reference optional Maturin namespace wheel configuration.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Public constructor signature, defaults, and ownership invariants.
[^rs-tls-mode]: `src/rs/vertebra-db/src/connection.rs::TlsMode` — Shared transport TLS modes and SQLSTATE mappings.
